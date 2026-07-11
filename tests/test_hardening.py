"""Regression tests for the production-hardening audit fixes.

Each test pins a defect reproduced during the Fable audit:

  * recover_running clobbering live tasks across processes
  * absence of an atomic cross-process task claim / terminal guard
  * per-process-only scheduler caps
  * verification-gate filename corruption + forbidden-path bypass
  * secret redaction gaps (connection strings, PEM, cloud keys)
  * out-of-worktree symlink escapes
  * integration diff -> apply round-trip on tracked-file edits

They share a single MockWorkerAdapter instance between two independently
constructed WorkerBridge objects so that ``max_observed`` and ``starts`` measure
true behaviour across bridges (== separate runner processes for scheduler
purposes) rather than within one event loop.
"""
from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import time
from pathlib import Path

import psutil
import pytest

from worker_bridge.orchestrator import _terminate_runner_tree

from worker_bridge.adapters.mock import MockWorkerAdapter
from worker_bridge.orchestrator import WorkerBridge
from worker_bridge.redaction import redact, redact_text
from worker_bridge.registry import WorkerRegistry
from worker_bridge.store import WorkerStore
from worker_bridge.workspace import WorkspaceManager


def _run(*args: str, cwd: Path) -> None:
    subprocess.run(args, cwd=cwd, check=True, capture_output=True, text=True)


@pytest.fixture
def repository(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    repo.mkdir()
    _run("git", "init", cwd=repo)
    _run("git", "config", "user.email", "worker@example.test", cwd=repo)
    _run("git", "config", "user.name", "Worker Test", cwd=repo)
    (repo / "README.md").write_text("base\n", encoding="utf-8")
    _run("git", "add", ".", cwd=repo)
    _run("git", "commit", "-m", "base", cwd=repo)
    return repo


def _spec(repository: Path, **overrides) -> dict:
    value = {
        "objective": "write a deterministic result",
        "worker": "mock",
        "workspace": {"repository": str(repository.resolve()), "isolation": "git_worktree"},
        "permissions": {"profile": "workspace_write"},
        "verification": {"commands": []},
        "metadata": {"mock_write": "result.txt", "mock_content": "done\n"},
    }
    value.update(overrides)
    return value


def _bridge(store: WorkerStore, adapter: MockWorkerAdapter, root: Path, **kwargs) -> WorkerBridge:
    return WorkerBridge(
        store=store,
        registry=WorkerRegistry([adapter]),
        workspaces=WorkspaceManager(root),
        **kwargs,
    )


# ── P0-1: recovery never clobbers a live task ────────────────────────────────

def test_recover_running_leaves_live_pid_untouched(tmp_path: Path, repository: Path):
    store = WorkerStore(tmp_path / "s.db")
    task = store.create_task(_spec(repository, task_id="live"))
    # A task owned by THIS (alive) process must survive a peer's recovery sweep.
    store.update_task("live", status="running", runtime={"pid": os.getpid()})
    assert store.recover_running() == 0
    assert store.get_task("live")["status"] == "running"


def test_recover_running_reaps_dead_pid(tmp_path: Path, repository: Path):
    store = WorkerStore(tmp_path / "s.db")
    store.create_task(_spec(repository, task_id="dead"))
    store.update_task("dead", status="running", runtime={"pid": 2_000_000_000})
    assert store.recover_running() == 1
    assert store.get_task("dead")["status"] == "paused"


def test_constructing_a_second_bridge_does_not_pause_live_task(tmp_path: Path, repository: Path):
    store = WorkerStore(tmp_path / "s.db")
    adapter = MockWorkerAdapter()
    b1 = _bridge(store, adapter, tmp_path / "wt")
    task = b1.create_task(_spec(repository, task_id="t"))
    store.update_task("t", status="running", runtime={"pid": os.getpid()})
    # Any other `hermes worker ...` command / runner spawn builds a bridge:
    _bridge(WorkerStore(store.path), MockWorkerAdapter(), tmp_path / "wt2")
    assert store.get_task("t")["status"] == "running"


# ── P0-2: atomic claim + terminal guard ──────────────────────────────────────

def test_two_bridges_cannot_both_run_one_task(tmp_path: Path, repository: Path):
    store = WorkerStore(tmp_path / "s.db")
    adapter = MockWorkerAdapter(delay=0.3)  # shared instance
    b1 = _bridge(store, adapter, tmp_path / "wt")
    b2 = _bridge(WorkerStore(store.path), adapter, tmp_path / "wt")
    b1.create_task(_spec(repository, task_id="solo"))

    async def race():
        return await asyncio.gather(
            b1.start_task("solo"), b2.start_task("solo"), return_exceptions=True
        )

    results = asyncio.run(race())
    # Exactly one bridge executed the adapter; the loser was refused.
    assert adapter.starts == 1
    statuses = [r if isinstance(r, Exception) else r["status"] for r in results]
    assert statuses.count("succeeded") == 1


def test_terminal_task_is_not_reexecuted(tmp_path: Path, repository: Path):
    store = WorkerStore(tmp_path / "s.db")
    adapter = MockWorkerAdapter()
    b = _bridge(store, adapter, tmp_path / "wt")
    b.create_task(_spec(repository, task_id="once"))
    assert asyncio.run(b.start_task("once"))["status"] == "succeeded"
    assert adapter.starts == 1
    # Starting a finished task again is idempotent, not a re-run.
    assert asyncio.run(b.start_task("once"))["status"] == "succeeded"
    assert adapter.starts == 1


# ── P0-3: cross-process scheduler caps hold host-wide ────────────────────────

def test_global_cap_holds_across_two_bridges(tmp_path: Path, repository: Path):
    store = WorkerStore(tmp_path / "s.db")
    adapter = MockWorkerAdapter(delay=0.25)  # shared -> measures true concurrency
    b1 = _bridge(store, adapter, tmp_path / "wt", maximum_concurrency=1)
    b2 = _bridge(WorkerStore(store.path), adapter, tmp_path / "wt", maximum_concurrency=1)
    b1.create_task(_spec(repository, task_id="g1"))
    b2.create_task(_spec(repository, task_id="g2"))

    async def race():
        return await asyncio.gather(
            b1.start_task("g1"), b2.start_task("g2"), return_exceptions=True
        )

    asyncio.run(race())
    # Global cap of 1 must serialize the two runners despite separate semaphores.
    assert adapter.max_observed == 1


# ── P0-5: verification-gate filename integrity ───────────────────────────────

def test_forbidden_path_not_bypassed_by_leading_status_space(tmp_path: Path, repository: Path):
    wm = WorkspaceManager(tmp_path / "wt")
    runtime = wm.prepare("v", {"workspace": {"repository": str(repository), "isolation": "git_worktree", "base_ref": "HEAD"}})
    # Single unstaged modification -> porcelain " M README.md" (leading space).
    (Path(runtime["path"]) / "README.md").write_text("changed\n", encoding="utf-8")
    assert wm.changed_files(runtime) == ["README.md"]
    result = wm.verify(runtime, {"commands": [], "forbidden_paths": ["README.md"]}, 60)
    assert result["forbidden_files"] == ["README.md"]
    assert result["ok"] is False


def test_changed_files_handles_spaces_and_unicode(tmp_path: Path, repository: Path):
    wm = WorkspaceManager(tmp_path / "wt")
    runtime = wm.prepare("u", {"workspace": {"repository": str(repository), "isolation": "git_worktree", "base_ref": "HEAD"}})
    root = Path(runtime["path"])
    (root / "my file.txt").write_text("x", encoding="utf-8")
    (root / "naïve.txt").write_text("y", encoding="utf-8")
    changed = wm.changed_files(runtime)
    assert "my file.txt" in changed
    assert "naïve.txt" in changed
    assert not any(name.startswith('"') for name in changed)


# ── P1-6: redaction ──────────────────────────────────────────────────────────

@pytest.mark.parametrize(
    "secret",
    [
        "postgres://admin:SuperSecret123@db.internal:5432/prod",
        "-----BEGIN RSA PRIVATE KEY-----\nMIIEpAIBAAKCAQEA\n-----END RSA PRIVATE KEY-----",
        "AKIAIOSFODNN7EXAMPLE",
        "Authorization: Bearer sk-abc123def456ghijkl",
    ],
)
def test_redaction_masks_known_secret_shapes(secret: str):
    out = redact_text(secret)
    for leaked in ("SuperSecret123", "MIIEpAIBAAKCAQEA", "AKIAIOSFODNN7EXAMPLE", "sk-abc123def456ghijkl"):
        assert leaked not in out


def test_redaction_persists_nothing_secret_in_store(tmp_path: Path):
    store = WorkerStore(tmp_path / "s.db")
    store.append_event(
        "worker.completed",
        {"summary": "db postgres://u:SuperSecret123@h/db key AKIAIOSFODNN7EXAMPLE"},
        task_id="t",
    )
    blob = Path(store.path).read_bytes().decode("utf-8", "replace")
    assert "SuperSecret123" not in blob
    assert "AKIAIOSFODNN7EXAMPLE" not in blob


# ── P1-7: symlink escape detection ───────────────────────────────────────────

def test_symlink_escape_is_flagged(tmp_path: Path, repository: Path):
    wm = WorkspaceManager(tmp_path / "wt")
    runtime = wm.prepare("e", {"workspace": {"repository": str(repository), "isolation": "git_worktree", "base_ref": "HEAD"}})
    root = Path(runtime["path"])
    outside = tmp_path / "outside_secret"
    outside.write_text("secret", encoding="utf-8")
    link = root / "link"
    try:
        os.symlink(outside, link)
    except (OSError, NotImplementedError):
        pytest.skip("symlink creation not permitted on this host")
    escapes = wm.escaping_paths(runtime, ["link"])
    assert "link" in escapes


# ── P1-8: integration round-trip on a tracked-file edit ──────────────────────

def test_integration_applies_tracked_file_edit(tmp_path: Path, repository: Path):
    wm = WorkspaceManager(tmp_path / "wt")
    src = wm.prepare("src", {"workspace": {"repository": str(repository), "isolation": "git_worktree", "base_ref": "HEAD"}})
    (Path(src["path"]) / "README.md").write_text("changed\n", encoding="utf-8")
    diff_text = wm.diff(src, 2_000_000)
    artifact = tmp_path / "changes.diff"
    artifact.write_text(diff_text, encoding="utf-8")
    dst = wm.prepare("dst", {"workspace": {"repository": str(repository), "isolation": "git_worktree", "base_ref": "HEAD"}})
    applied = wm.apply_diff(dst, artifact)
    assert applied["ok"], applied
    assert (Path(dst["path"]) / "README.md").read_text().strip() == "changed"


# ── P0-4: process-tree cancellation + PID-owner validation ───────────────────

def _alive(pid: int) -> bool:
    try:
        return psutil.pid_exists(pid) and psutil.Process(pid).status() != psutil.STATUS_ZOMBIE
    except psutil.NoSuchProcess:
        return False


def test_cancel_terminates_the_whole_runner_tree(tmp_path: Path):
    marker_task = "task-killme"
    pidfile = tmp_path / "child.pid"
    child_code = "import time; time.sleep(60)"
    parent_code = (
        "import subprocess, sys, time;"
        f"c = subprocess.Popen([sys.executable, '-c', {child_code!r}]);"
        "open(sys.argv[3], 'w').write(str(c.pid));"
        "time.sleep(60)"
    )
    # argv 1..3 embed the runner marker + task id so cmdline validation passes.
    parent = subprocess.Popen(
        [sys.executable, "-c", parent_code, "worker_bridge.runner", marker_task, str(pidfile)]
    )
    try:
        for _ in range(200):
            if pidfile.exists() and pidfile.read_text().strip():
                break
            time.sleep(0.05)
        child_pid = int(pidfile.read_text().strip())
        assert _alive(child_pid)
        _terminate_runner_tree(parent.pid, marker_task)
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and (_alive(parent.pid) or _alive(child_pid)):
            time.sleep(0.05)
        assert not _alive(parent.pid)
        assert not _alive(child_pid), "worker child was orphaned by cancellation"
    finally:
        for pid in (parent.pid,):
            try:
                psutil.Process(pid).kill()
            except psutil.NoSuchProcess:
                pass


def test_cancel_refuses_reused_foreign_pid(tmp_path: Path):
    # A process whose cmdline lacks the runner marker (a recycled PID) must
    # never be killed by cancellation.
    victim = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
    try:
        _terminate_runner_tree(victim.pid, "task-unrelated")
        time.sleep(0.3)
        assert _alive(victim.pid), "cancellation killed an unrelated reused PID"
    finally:
        victim.kill()
