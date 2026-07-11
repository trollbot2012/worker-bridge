"""MCP server exposing worker-bridge to any MCP-capable agent.

Run it as ``worker-bridge-mcp`` (or ``python -m worker_bridge.mcp_server``) and
add it to any MCP client — Claude Code, Cursor, Windsurf, Cline, Continue. The
tools let the host agent delegate a scoped coding task to an external worker
(Codex / Claude Code / OpenCode / any configured CLI) in an isolated git
worktree, then have worker-bridge independently verify the diff.
"""
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

from mcp.server.fastmcp import FastMCP

from worker_bridge.cli import _bridge, _spawn

mcp = FastMCP("worker-bridge")


def _make_bridge():
    return _bridge(SimpleNamespace(store=""))


def _summarize(task: dict[str, Any]) -> dict[str, Any]:
    result = task.get("result") or {}
    verification = (result.get("metadata") or {}).get("verification") or {}
    return {
        "task_id": task.get("task_id"),
        "worker": task.get("worker"),
        "status": task.get("status"),
        "summary": (result.get("summary") or "")[:4000],
        "changed_files": result.get("changed_files", []),
        "verified": verification.get("ok"),
        "error": result.get("error"),
        "artifacts": result.get("artifacts", []),
    }


@mcp.tool()
async def worker_delegate(
    objective: str,
    repository: str,
    worker: str = "codex",
    permission: str = "workspace_write",
    verify: list[str] | None = None,
    base_ref: str = "HEAD",
    wait: bool = False,
) -> dict:
    """Delegate a scoped coding task to an external AI coding worker in an
    isolated git worktree, then independently verify the result.

    USE for: implementing a feature, fixing a bug across files, a refactor or
    migration — work you'd otherwise do inline. Delegating keeps your context
    clean and gets an independent verification pass on the diff. The worker runs
    in the background; poll with worker_status(task_id). DO NOT USE for a trivial
    one-line edit, or non-coding work.

    Args:
        objective: What the worker must accomplish. Be specific and scoped.
        repository: Absolute path to the target git repository.
        worker: codex | claude-code | opencode | zcode-glm | vscode | mock (or a configured worker).
        permission: read_only | workspace_write | full_access | custom.
        verify: Shell commands worker-bridge runs itself to verify, e.g. ["pytest -q"].
        base_ref: Git ref to branch the worktree from (default HEAD).
        wait: Block until finished and return the full result (default false).
    """
    repo = Path(repository).expanduser()
    if not objective.strip():
        return {"error": "objective is required"}
    if not repo.is_absolute() or not repo.exists():
        return {"error": f"repository must be an existing absolute path: {repository}"}
    spec = {
        "objective": objective,
        "worker": worker or "codex",
        "workspace": {
            "repository": str(repo.resolve()),
            "isolation": "git_worktree",
            "base_ref": base_ref or "HEAD",
        },
        "permissions": {"profile": permission or "workspace_write"},
        "verification": {"commands": [str(c) for c in (verify or [])]},
    }
    try:
        bridge = _make_bridge()
        task = bridge.create_task(spec)
        task_id = task["task_id"]
        if wait:
            result = await bridge.start_task(task_id)
            return _summarize(result)
        spawned = _spawn(bridge, task_id)
        return {
            "task_id": task_id,
            "status": spawned["status"],
            "worker": spec["worker"],
            "note": "Worker running in the background. Poll with worker_status(task_id).",
        }
    except Exception as exc:
        return {"error": f"{type(exc).__name__}: {exc}"}


@mcp.tool()
async def worker_status(task_id: str = "") -> dict:
    """Check delegated worker tasks.

    With task_id: returns status, summary, changed files, the independent
    verification result, and artifact paths (diff + manifest). Without task_id:
    lists recent tasks. Use to poll a task started by worker_delegate.
    """
    try:
        bridge = _make_bridge()
        task_id = (task_id or "").strip()
        if not task_id:
            tasks = bridge.list_tasks(limit=20)
            return {"tasks": [
                {"task_id": t["task_id"], "worker": t["worker"], "status": t["status"]}
                for t in tasks
            ]}
        return _summarize(bridge.get_task(task_id))
    except Exception as exc:
        return {"error": f"{type(exc).__name__}: {exc}"}


@mcp.tool()
async def list_workers() -> dict:
    """List available coding workers on this machine (installed and healthy),
    with their capabilities. Call this to see which `worker` values worker_delegate
    can use before delegating.
    """
    try:
        bridge = _make_bridge()
        workers = await bridge.list_workers()
        return {"workers": [
            {
                "worker": w["worker"],
                "installed": w["availability"]["installed"],
                "version": w["availability"].get("version"),
                "sessions": w["capabilities"].get("sessions"),
                "max_concurrency": w["capabilities"].get("maximum_concurrency"),
            }
            for w in workers
        ]}
    except Exception as exc:
        return {"error": f"{type(exc).__name__}: {exc}"}


@mcp.tool()
async def worker_cancel(task_id: str) -> dict:
    """Cancel a running or queued delegated task and terminate its worker
    process tree. Returns the task's terminal state.
    """
    try:
        bridge = _make_bridge()
        return _summarize(await bridge.cancel_task(task_id))
    except Exception as exc:
        return {"error": f"{type(exc).__name__}: {exc}"}


@mcp.tool()
async def worker_logs(task_id: str, after: int = 0) -> dict:
    """Return the normalized event log for a delegated task (worker.started,
    progress, worker.completed, verification, ...), for streaming-style progress.
    Pass `after` (an event_id) to page.
    """
    try:
        bridge = _make_bridge()
        events = bridge.store.events(task_id=task_id, after=after)
        return {"events": events[-200:]}
    except Exception as exc:
        return {"error": f"{type(exc).__name__}: {exc}"}


@mcp.tool()
async def worker_prune(apply: bool = False, include_paused: bool = False) -> dict:
    """Reclaim disk from isolated worktrees that no longer back active work.

    Dry-run by default: reports which terminal-task worktrees and orphan
    directories (left by interrupted allocations) would be removed. Pass
    apply=true to actually delete them. Never touches running/queued/verifying
    tasks or direct (in-place) workspaces; include_paused=true also reclaims
    paused-task worktrees.
    """
    try:
        bridge = _make_bridge()
        report = bridge.prune_workspaces(apply=apply, include_paused=include_paused)
        return {
            "applied": report["applied"],
            "candidates": [
                {"task_id": c["task_id"], "status": c["status"], "path": c["path"]}
                for c in report["candidates"]
            ],
            "orphans": report["orphans"],
            "pruned": report["pruned"],
            "failed": report["failed"],
        }
    except Exception as exc:
        return {"error": f"{type(exc).__name__}: {exc}"}


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
