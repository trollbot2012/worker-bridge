"""Shared helpers for cross-process RepositoryLock tests.

Spawns real contender subprocesses (``_lock_child.py``) coordinated through
marker files, so mutual exclusion is tested against actual OS-level
processes rather than simulated lock-file contents.
"""
from __future__ import annotations

import json
import subprocess
import sys
import time
from pathlib import Path

CHILD = Path(__file__).parent / "_lock_child.py"


def child_config(tmp: Path, *, repo: Path, operation: str, wait_seconds: float,
                 id: str, peer: str, **overrides) -> Path:  # noqa: A002 - test fixture ids
    config = {
        "repo": str(repo),
        "operation": operation,
        "wait_seconds": wait_seconds,
        "signaldir": str(tmp / "signals"),
        "id": id,
        "peer": peer,
    }
    config.update(overrides)
    (tmp / "signals").mkdir(parents=True, exist_ok=True)
    path = tmp / f"config-{id}.json"
    path.write_text(json.dumps(config), encoding="utf-8")
    return path


def spawn(config: Path) -> subprocess.Popen:
    return subprocess.Popen(
        [sys.executable, str(CHILD), str(config)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def await_file(path: Path, timeout: float = 30.0) -> str:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            return path.read_text(encoding="utf-8")
        time.sleep(0.02)
    raise AssertionError(f"timed out waiting for {path.name}")


def outcome(tmp: Path, child_id: str) -> dict:
    return json.loads(await_file(tmp / "signals" / f"outcome-{child_id}.json"))


def release_and_join(tmp: Path, children: list[subprocess.Popen]) -> None:
    for child_id in ("0", "1"):
        (tmp / "signals" / f"release-{child_id}").write_text("", encoding="ascii")
    for proc in children:
        proc.wait(timeout=30)
