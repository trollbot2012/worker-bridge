"""RepositoryLock operation scoping, stale recovery, and worktree-setup serialization.

Fail-fast against a live holder and bounded waiting are covered by real
cross-process holder tests in ``test_repository_lock_concurrency.py`` —
simulating a holder by writing a pid into the lock file no longer
represents holding now that exclusion is a kernel-mediated byte lock.
"""

from __future__ import annotations

import asyncio
import subprocess
from pathlib import Path

import pytest

from worker_bridge.adapters.mock import MockWorkerAdapter
from worker_bridge.orchestrator import WorkerBridge
from worker_bridge.registry import WorkerRegistry
from worker_bridge.store import WorkerStore
from worker_bridge.workspace import RepositoryLock, WorkspaceManager


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


# Fail-fast against a live foreign holder and bounded waiting under a
# transient holder moved to test_repository_lock_concurrency.py, where real
# subprocess holders exercise them (a lock file containing a live pid is no
# longer equivalent to holding the lock).


def test_stale_lock_file_from_dead_pid_is_recoverable(repository: Path):
    lock = RepositoryLock(repository, operation="worktree-setup")
    lock._path.parent.mkdir(parents=True, exist_ok=True)
    # A pid that cannot be alive (way beyond any real pid table on CI). With
    # kernel-mediated exclusion the leftover record claims nothing: the file
    # is only ever a claim point, so acquisition succeeds immediately.
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
