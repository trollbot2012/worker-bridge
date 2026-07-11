"""worker-bridge — delegate coding tasks to external AI coding agents.

Public API for embedding the bridge in your own Python agent or framework::

    from worker_bridge import WorkerBridge, TaskSpec

    bridge = WorkerBridge()
    task = bridge.create_task({
        "objective": "Add a --json flag to the CLI",
        "worker": "codex",
        "workspace": {"repository": "/abs/path/to/repo", "isolation": "git_worktree"},
        "verification": {"commands": ["pytest -q"]},
    })
    result = await bridge.start_task(task["task_id"])

For a host-neutral tool surface, run the MCP server (``worker-bridge-mcp``) or
the CLI (``worker-bridge``).
"""

from worker_bridge.models import (
    JobStatus,
    PermissionSpec,
    TaskLimits,
    TaskSpec,
    TaskStatus,
    VerificationSpec,
    WorkerResult,
    WorkspaceSpec,
)
from worker_bridge.orchestrator import OrchestrationError, WorkerBridge
from worker_bridge.registry import WorkerRegistry
from worker_bridge.store import WorkerStore
from worker_bridge.workspace import WorkspaceManager

__version__ = "0.1.0"

__all__ = [
    "WorkerBridge",
    "OrchestrationError",
    "WorkerStore",
    "WorkerRegistry",
    "WorkspaceManager",
    "TaskSpec",
    "TaskStatus",
    "JobStatus",
    "PermissionSpec",
    "TaskLimits",
    "VerificationSpec",
    "WorkspaceSpec",
    "WorkerResult",
    "__version__",
]
