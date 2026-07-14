"""RepositoryLock operation scoping, bounded wait, and worktree-setup serialization."""

from __future__ import annotations

import asyncio
import os
import subprocess
import threading
import time
from pathlib import Path

import pytest

from worker_bridge.adapters.mock import MockWorkerAdapter
from worker_bridge.orchestrator import WorkerBridge
from worker_bridge.registry import WorkerRegistry
from worker_bridge.store import WorkerStore
from worker_bridge.workspace import RepositoryLock, WorkspaceError, WorkspaceManager


@pytest.fixture
def repository(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    repo.mkdir()

    def g(*args: str) -> None:
        subprocess.run(args, cwd=repo, capture_output=True, check=True)

    g("git", "init")
    g("git", "config", "user.email", "worker@example.test")
    g("git", "config", "user.name", "Worker Test")
    (repo / "README.md").write_text("base\n", encoding="utf-8")
    g("git", "add", ".")
    g("git", "commit", "-m", "base")
    return repo


def test_operation_scoped_lock_does_not_exclude_plain_lock(repository: Path):
    # A direct-mode worker holds the plain repo lock for its whole run; a
    # concurrent worktree-setup on the same repo must not contend with it.
    with RepositoryLock(repository):
        with RepositoryLock(repository, operation="worktree-setup"):
            pass


def test_same_operation_excludes_and_fail_fast_raises(repository: Path):
    lock = RepositoryLock(repository, operation="worktree-setup")
    with lock:
        # Simulate a foreign live holder: the file exists and names a live
        # pid, and we bypass the in-process thread lock by making a second
        # lock object target the same file via a fresh key... the thread lock
        # is shared per key, so use the lock FILE directly.
        pass
    # Foreign live holder: create the lock file with our own (live) pid.
    holder = RepositoryLock(repository, operation="worktree-setup")
    holder._path.parent.mkdir(parents=True, exist_ok=True)
    holder._path.write_text(str(os.getpid()), encoding="ascii")
    try:
        with pytest.raises(WorkspaceError, match="repository lock exists"):
            # thread lock is free (we never entered holder), so this exercises
            # the file-lock fail-fast path against a live foreign pid.
            with RepositoryLock(repository, operation="worktree-setup"):
                pass
    finally:
        holder._path.unlink(missing_ok=True)


def test_wait_seconds_outlasts_a_transient_holder(repository: Path):
    lock = RepositoryLock(repository, operation="worktree-setup", wait_seconds=10)
    lock._path.parent.mkdir(parents=True, exist_ok=True)
    lock._path.write_text(str(os.getpid()), encoding="ascii")  # live foreign holder

    def release_soon() -> None:
        time.sleep(0.5)
        lock._path.unlink(missing_ok=True)

    thread = threading.Thread(target=release_soon)
    thread.start()
    started = time.monotonic()
    try:
        with lock:
            waited = time.monotonic() - started
    finally:
        thread.join()
    assert waited >= 0.4  # actually waited for the holder, didn't raise


def test_stale_lock_from_dead_pid_is_swept(repository: Path):
    lock = RepositoryLock(repository, operation="worktree-setup")
    lock._path.parent.mkdir(parents=True, exist_ok=True)
    # A pid that cannot be alive (way beyond any real pid table on CI).
    lock._path.write_text("999999999", encoding="ascii")
    with lock:
        pass  # acquired despite the leftover file


def test_parallel_worktree_allocations_serialize(tmp_path: Path, repository: Path):
    # Regression for the CI flake: N tasks in one parallel job all run
    # `git worktree add` against the same repository. With worktree-setup
    # serialized the job must be fully green, never 'partial'.
    spec = lambda: {  # noqa: E731
        "objective": "write a deterministic result",
        "worker": "mock",
        "workspace": {"repository": str(repository.resolve()), "isolation": "git_worktree"},
        "permissions": {"profile": "workspace_write"},
        "verification": {"commands": []},
        "metadata": {"mock_write": "result.txt"},
    }
    bridge = WorkerBridge(
        store=WorkerStore(tmp_path / "stress.db"),
        registry=WorkerRegistry([MockWorkerAdapter()]),
        workspaces=WorkspaceManager(tmp_path / "stress-worktrees"),
        maximum_concurrency=16,
        per_repository_concurrency=16,
        per_job_concurrency=16,
    )
    job = bridge.create_job("parallel-stress", [spec() for _ in range(8)], strategy="parallel")
    result = asyncio.run(bridge.run_job(job["job_id"]))
    statuses = {tid: bridge.get_task(tid)["status"] for tid in result["tasks"]}
    assert result["status"] == "succeeded", statuses
    paths = {bridge.get_task(tid)["runtime"]["path"] for tid in result["tasks"]}
    assert len(paths) == 8
