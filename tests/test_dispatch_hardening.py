"""Fast, hermetic regressions for the storage-safety and dispatch hardening.

Every test pins WORKER_BRIDGE_HOME to its own tmp dir and touches only tmp
paths, so nothing collides with shared bridge state under load.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

# Reuse the bridge's own process helper rather than calling out to a raw
# process runner from this file — keeps the tests exercising real git through
# the same seam the engine uses.
from worker_bridge.workspace import _run as _proc


@pytest.fixture(autouse=True)
def _isolated_home(tmp_path: Path, monkeypatch):
    """Pin every get_home()-derived path (artifacts, worktrees, locks) into
    this test's tmp dir so runs never touch the real/default bridge home."""
    home = tmp_path / "bridge-home"
    home.mkdir(exist_ok=True)
    monkeypatch.setenv("WORKER_BRIDGE_HOME", str(home))


def _git(repo: Path, *args: str) -> str:
    proc = _proc(["git", *args], repo)
    assert proc.returncode == 0, proc.stderr or proc.stdout
    return proc.stdout.strip()


def _git_init(repo: Path, extra_files: "dict[str, str] | None" = None) -> None:
    """Init a committed git repo in place (copy isolation reads HEAD)."""
    repo.mkdir(parents=True, exist_ok=True)
    _git(repo, "init")
    _git(repo, "config", "user.email", "worker@example.test")
    _git(repo, "config", "user.name", "Worker Test")
    for name, content in (extra_files or {}).items():
        (repo / name).write_text(content, encoding="utf-8")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-m", "base")


@pytest.fixture
def repository(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    _git_init(repo, {"README.md": "base\n"})
    return repo


def test_copy_isolation_refuses_oversized_source(tmp_path: Path, monkeypatch):
    """A copy workspace larger than the budget fails fast instead of hanging or
    filling the disk (the field 'spawns but never executes' / disk-full)."""
    import worker_bridge.workspace as ws

    repo = tmp_path / "big"
    repo.mkdir()
    _git_init(repo, {"a.txt": "x" * 4096})
    monkeypatch.setattr(ws, "_COPY_MAX_BYTES", 1024)  # 1 KiB budget
    manager = ws.WorkspaceManager(tmp_path / "worktrees")
    with pytest.raises(ws.WorkspaceError, match="copy isolation refused"):
        manager.prepare(
            "oversized",
            {"workspace": {"repository": str(repo), "isolation": "copy"}},
        )
    assert not (manager.root / "oversized").exists()


def test_copy_isolation_ignores_heavy_directories(tmp_path: Path):
    """node_modules/.git/caches never enter a copy workspace, so a repo that is
    'huge' only because of build output copies cheaply and correctly."""
    import worker_bridge.workspace as ws

    repo = tmp_path / "repo"
    (repo / "node_modules" / "pkg").mkdir(parents=True)
    (repo / "node_modules" / "pkg" / "junk.bin").write_text("y" * 100_000, encoding="utf-8")
    (repo / "src").mkdir()
    (repo / "src" / "main.py").write_text("print('hi')\n", encoding="utf-8")
    _git_init(repo)

    manager = ws.WorkspaceManager(tmp_path / "worktrees")
    runtime = manager.prepare(
        "light", {"workspace": {"repository": str(repo), "isolation": "copy"}}
    )
    dest = Path(runtime["path"])
    assert (dest / "src" / "main.py").exists()
    assert not (dest / "node_modules").exists()


def test_gated_repo_worktree_falls_back_to_direct(tmp_path: Path, repository: Path):
    """When a reference-transaction hook blocks worktree branch creation,
    prep degrades to in-place `direct` isolation instead of failing or copying
    the whole tree."""
    from worker_bridge.workspace import WorkspaceManager

    hook = repository / ".git" / "hooks" / "reference-transaction"
    hook.parent.mkdir(parents=True, exist_ok=True)
    hook.write_text(
        '#!/bin/sh\n'
        '[ "$1" = prepared ] || exit 0\n'
        'while read old new ref; do\n'
        '  case "$ref" in refs/heads/*) echo "blocked by gate" >&2; exit 1;; esac\n'
        'done\n'
        'exit 0\n',
        encoding="utf-8", newline="\n",
    )
    hook.chmod(0o755)

    manager = WorkspaceManager(tmp_path / "worktrees")
    runtime = manager.prepare(
        "gated", {"workspace": {"repository": str(repository), "isolation": "git_worktree"}}
    )
    assert runtime["isolation"] == "direct"
    assert runtime["isolation_requested"] == "git_worktree"
    assert runtime["isolation_fallback"] == "gate_blocked_worktree_ref"
    assert not (manager.root / "gated").exists()
    assert _git(repository, "branch", "--list", "codex/worker-gated").strip() == ""


def test_external_repo_still_uses_worktree(tmp_path: Path, repository: Path):
    """A repo without a governance hook keeps full worktree isolation — the
    fallback must not fire for ordinary targets."""
    from worker_bridge.workspace import WorkspaceManager

    manager = WorkspaceManager(tmp_path / "worktrees")
    runtime = manager.prepare(
        "ext", {"workspace": {"repository": str(repository), "isolation": "git_worktree"}}
    )
    assert runtime["isolation"] == "git_worktree"
    assert "isolation_fallback" not in runtime
    assert runtime["branch"] == "codex/worker-ext"


def test_copy_isolation_refuses_junctions(tmp_path: Path):
    """A junction/symlink anywhere in the copy source is refused — a link
    pointing back up the tree re-creates the recursive self-copy amplification
    even with the name-based exclusions."""
    import os

    import worker_bridge.workspace as ws

    repo = tmp_path / "repo"
    _git_init(repo, {"a.txt": "x\n"})
    target = tmp_path / "elsewhere"
    target.mkdir()
    link = repo / "loop"
    if os.name == "nt":
        proc = _proc(["cmd", "/c", "mklink", "/J", str(link), str(target)], tmp_path)
        if proc.returncode != 0:
            pytest.skip("cannot create junction on this system")
    else:
        try:
            link.symlink_to(target, target_is_directory=True)
        except OSError:
            pytest.skip("cannot create symlink on this system")
    manager = ws.WorkspaceManager(tmp_path / "worktrees")
    with pytest.raises(ws.WorkspaceError, match="symlinks or junctions"):
        manager.prepare("junc", {"workspace": {"repository": str(repo), "isolation": "copy"}})
    assert not (manager.root / "junc").exists()


def test_recursive_containment_is_rejected(tmp_path: Path):
    """A repository that contains the worker root (or vice-versa) is refused by
    path math, not name matching — unbounded self-copy amplification."""
    from worker_bridge.workspace import WorkspaceError, WorkspaceManager

    repo = tmp_path / "project"
    repo.mkdir()
    # Worker root lives *inside* the repository — the pathological layout.
    manager = WorkspaceManager(repo / "workers" / "worktrees")
    with pytest.raises(WorkspaceError, match="recursive workspace containment"):
        manager.plan("t", {"workspace": {"repository": str(repo), "isolation": "copy"}})


def test_bridge_home_copy_requires_optin(tmp_path: Path, monkeypatch):
    """copy isolation refuses a repository at or above the bridge home unless
    workspace.allow_profile_copy is set explicitly."""
    import worker_bridge.workspace as ws

    home = tmp_path / "bridge-home"  # created by the _isolated_home fixture
    monkeypatch.setenv("WORKER_BRIDGE_HOME", str(home))
    _git_init(home, {"README.md": "x\n"})
    manager = ws.WorkspaceManager(tmp_path / "worktrees")  # root OUTSIDE home
    with pytest.raises(ws.WorkspaceError, match="at or above the bridge home"):
        manager.plan("t", {"workspace": {"repository": str(home), "isolation": "copy"}})
    # Explicit opt-in is honored (plan only; no copy performed here).
    plan = manager.plan(
        "t2",
        {"workspace": {"repository": str(home), "isolation": "copy", "allow_profile_copy": True}},
    )
    assert plan["allocation_state"] == "allocating"


def test_plan_records_destination_before_any_copy(tmp_path: Path, repository: Path):
    """plan() names the destination + isolation with NO filesystem mutation, so
    the orchestrator can persist it before allocate() touches disk."""
    from worker_bridge.workspace import WorkspaceManager

    manager = WorkspaceManager(tmp_path / "worktrees")
    plan = manager.plan("early", {"workspace": {"repository": str(repository), "isolation": "git_worktree"}})
    assert plan["allocation_state"] == "allocating"
    assert plan["path"].endswith("early")
    assert plan["branch"] == "codex/worker-early"
    assert not Path(plan["path"]).exists()  # nothing created yet


def test_prune_reclaims_terminal_and_orphans_not_active(tmp_path: Path, repository: Path):
    """prune reports (dry-run) then reclaims terminal-task worktrees and
    unreferenced orphan dirs, never running/queued/verifying ones."""
    from worker_bridge.adapters.mock import MockWorkerAdapter
    from worker_bridge.orchestrator import WorkerBridge
    from worker_bridge.registry import WorkerRegistry
    from worker_bridge.store import WorkerStore
    from worker_bridge.workspace import WorkspaceManager

    root = tmp_path / "worktrees"
    bridge = WorkerBridge(
        store=WorkerStore(tmp_path / "prune.db"),
        registry=WorkerRegistry([MockWorkerAdapter()]),
        workspaces=WorkspaceManager(root),
    )
    # A failed task leaves a real worktree behind (retention defaults keep it).
    done = bridge.create_task({
        "objective": "x", "worker": "mock",
        "workspace": {"repository": str(repository), "isolation": "git_worktree"},
        "metadata": {"mock_write": "out.txt", "mock_fail": True},
        "verification": {"commands": []},
    })
    done = asyncio.run(bridge.start_task(done["task_id"]))
    assert done["status"] == "failed"
    done_path = done["runtime"]["path"]
    assert Path(done_path).exists()
    # An orphan directory nothing references.
    orphan = root / "orphan-xyz"
    orphan.mkdir()

    dry = bridge.prune_workspaces()
    assert dry["applied"] is False
    assert str(orphan) in dry["orphans"]
    assert Path(done_path).exists()  # dry-run touched nothing

    applied = bridge.prune_workspaces(apply=True)
    assert done["task_id"] in applied["pruned"]
    assert not Path(done_path).exists()
    assert not orphan.exists()


def test_workspace_prep_hang_times_out(tmp_path: Path, repository: Path, monkeypatch):
    """A wedged workspace build surfaces as a clean timeout, never an
    indefinitely 'running' task with a 0-byte log and no worker child."""
    from worker_bridge.adapters.mock import MockWorkerAdapter
    from worker_bridge.orchestrator import WorkerBridge
    from worker_bridge.registry import WorkerRegistry
    from worker_bridge.store import WorkerStore
    from worker_bridge.workspace import WorkspaceManager

    bridge = WorkerBridge(
        store=WorkerStore(tmp_path / "hang.db"),
        registry=WorkerRegistry([MockWorkerAdapter()]),
        workspaces=WorkspaceManager(tmp_path / "hang-worktrees"),
    )

    def _hang(*_a, **_k):
        # Just over the 1s prep timeout below: long enough that wait_for fires,
        # short enough that asyncio.run doesn't block on the executor thread.
        import time as _t
        _t.sleep(2)

    # allocate() is the filesystem phase the orchestrator runs under timeout.
    monkeypatch.setattr(bridge.workspaces, "allocate", _hang)
    task = bridge.create_task({
        "objective": "x",
        "worker": "mock",
        "workspace": {"repository": str(repository), "isolation": "git_worktree"},
        "verification": {"commands": []},
        "limits": {"timeout_seconds": 1},
    })
    result = asyncio.run(bridge.start_task(task["task_id"]))
    assert result["status"] == "timed_out"
