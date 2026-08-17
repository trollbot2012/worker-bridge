"""Subprocess contender used by RepositoryLock concurrency tests.

Driven by a JSON config file so the parent test can orchestrate real
cross-process contention deterministically:

    python _lock_child.py <config.json>

Config keys:
    repo, operation, wait_seconds  -- lock identity and wait budget
    signaldir                      -- directory for markers/outcomes
    id                             -- this child's marker suffix ("0"/"1")

Optional deterministic seams (inert unless the implementation calls the
patched functions during acquisition):
    barrier_pid_exists  -- synchronize BOTH children at the stale-pid
                           observation point before psutil.pid_exists returns
    wait_peer_created_then_unlink -- before unlinking the lock path, block
                           until the peer reports holding the lock (forces
                           the stale-sweep to run against a *live* lock)

Outcome protocol: writes ``outcome-<id>.json`` ({acquired, error}); when
acquired, also writes ``holding-<id>`` and keeps the lock until a
``release-<id>`` file appears (polling with hard caps so nothing hangs).
"""
from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path


def _await_file(path: Path, timeout: float = 15.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            return True
        time.sleep(0.02)
    return False


def _patch_pid_exists_barrier(signaldir: Path, child_id: str, peer_id: str) -> None:
    """Both children meet here before agreeing the pid is dead."""
    import psutil

    mine = signaldir / f"saw-stale-{child_id}"
    peers = signaldir / f"saw-stale-{peer_id}"

    def synchronized(pid: int) -> bool:
        mine.write_text(str(pid), encoding="ascii")
        if not _await_file(peers):
            raise RuntimeError("pid_exists barrier timed out")
        return False  # both contenders agree: stale

    psutil.pid_exists = synchronized


def _patch_unlink_waits_for_peer(signaldir: Path, child_id: str, peer_id: str, lock_path: Path) -> None:
    """Force the stale-sweep unlink to run after the peer recreated the lock."""
    real_unlink = os.unlink

    def ordered_unlink(path, *args, **kwargs):
        try:
            if Path(path).resolve() == lock_path.resolve():
                if not _await_file(signaldir / f"holding-{peer_id}"):
                    raise RuntimeError("peer-created barrier timed out")
        except OSError:
            pass  # not the lock path, or already gone: behave like unlink
        return real_unlink(path, *args, **kwargs)

    os.unlink = ordered_unlink


def main() -> int:
    config = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    signaldir = Path(config["signaldir"])
    child_id = str(config["id"])
    peer_id = str(config["peer"])
    outcome_path = signaldir / f"outcome-{child_id}.json"

    from worker_bridge.workspace import RepositoryLock

    lock = RepositoryLock(
        config["repo"], operation=config["operation"], wait_seconds=config["wait_seconds"]
    )

    if config.get("barrier_pid_exists"):
        _patch_pid_exists_barrier(signaldir, child_id, peer_id)
    if config.get("wait_peer_created_then_unlink"):
        _patch_unlink_waits_for_peer(signaldir, child_id, peer_id, lock._path)

    try:
        with lock:
            (signaldir / f"holding-{child_id}").write_text("", encoding="ascii")
            outcome_path.write_text(
                json.dumps({"id": child_id, "acquired": True, "error": None}), encoding="utf-8"
            )
            _await_file(signaldir / f"release-{child_id}", timeout=30.0)
        return 0
    except Exception as exc:  # noqa: BLE001 - report every failure shape to the parent
        outcome_path.write_text(
            json.dumps({"id": child_id, "acquired": False, "error": f"{type(exc).__name__}: {exc}"}),
            encoding="utf-8",
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
