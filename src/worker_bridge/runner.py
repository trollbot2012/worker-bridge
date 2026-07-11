"""Detached task runner used by the operator CLI."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys

from worker_bridge.orchestrator import WorkerBridge
from worker_bridge.store import WorkerStore
from worker_bridge.redaction import redact_text


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("task_id")
    parser.add_argument("--store", required=True)
    parser.add_argument("--continue-message")
    args = parser.parse_args(argv)
    bridge = WorkerBridge(store=WorkerStore(args.store))
    try:
        if args.continue_message is not None:
            result = asyncio.run(bridge.continue_task(args.task_id, args.continue_message))
        else:
            result = asyncio.run(bridge.start_task(args.task_id))
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0 if result["status"] in {"succeeded", "accepted"} else 1
    except Exception as exc:
        # Only record a failure for a task THIS process owns. After losing a
        # cross-process claim race the task belongs to the winning runner; its
        # status must never be clobbered by the loser's benign exit.
        try:
            current = bridge.store.get_task(args.task_id)
            owner = (current.get("runtime") or {}).get("pid") if current else None
            if current and owner == os.getpid():
                bridge.store.update_task(
                    args.task_id,
                    status="failed",
                    result={"status": "failed", "error": f"runner crashed: {type(exc).__name__}: {exc}"},
                )
        except Exception:
            pass
        print(redact_text(f"worker runner failed: {type(exc).__name__}: {exc}"), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
