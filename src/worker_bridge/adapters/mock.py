"""Deterministic worker used by normal tests and orchestration demos."""

from __future__ import annotations

import asyncio
import uuid
from pathlib import Path

from worker_bridge.adapters.base import WorkerAdapter
from worker_bridge.models import (
    RuntimeContext,
    TaskSpec,
    WorkerAvailability,
    WorkerCapabilities,
    WorkerResult,
)


class MockWorkerAdapter(WorkerAdapter):
    name = "mock"

    def __init__(self, *, delay: float = 0.0, fail: bool = False, maximum_concurrency: int = 100) -> None:
        self.delay = delay
        self.fail = fail
        self.maximum_concurrency = maximum_concurrency
        self.cancelled: set[str] = set()
        self.active = 0
        self.max_observed = 0
        # Total adapter entries. Regression tests read this to prove a task is
        # never executed twice across independently launched bridges, and that
        # ``max_observed`` reflects true simultaneous concurrency.
        self.starts = 0

    async def detect(self) -> WorkerAvailability:
        return WorkerAvailability(True, True, "1.0-test", "in-process")

    async def capabilities(self) -> WorkerCapabilities:
        return WorkerCapabilities(
            sessions=True,
            streaming=True,
            structured_output=True,
            approvals=True,
            pause=False,
            sandbox_modes=["read_only", "workspace_write", "full_access", "custom"],
            maximum_concurrency=self.maximum_concurrency,
        )

    async def start(self, task: TaskSpec, runtime: RuntimeContext) -> WorkerResult:
        session_id = f"mock-{uuid.uuid4().hex[:10]}"
        runtime.emit("worker.started", {"session_id": session_id})
        self.starts += 1
        self.active += 1
        self.max_observed = max(self.max_observed, self.active)
        try:
            delay = self.delay or float(task.metadata.get("mock_delay", 0.0))
            if delay:
                await asyncio.sleep(delay)
            if task.metadata.get("mock_write"):
                target = Path(runtime.workspace) / str(task.metadata["mock_write"])
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text(str(task.metadata.get("mock_content", task.objective)), encoding="utf-8")
            if self.fail or task.metadata.get("mock_fail"):
                return WorkerResult("failed", session_id=session_id, error="controlled mock failure")
            return WorkerResult(
                "succeeded",
                str(task.metadata.get("mock_summary", f"Mock completed: {task.objective}")),
                session_id=session_id,
            )
        finally:
            self.active -= 1

    async def continue_task(
        self, session_id: str, message: str, task: TaskSpec, runtime: RuntimeContext
    ) -> WorkerResult:
        runtime.emit("worker.continued", {"session_id": session_id})
        if task.metadata.get("mock_follow_up_write"):
            target = Path(runtime.workspace) / str(task.metadata["mock_follow_up_write"])
            target.write_text(message, encoding="utf-8")
        return WorkerResult("succeeded", f"Mock continued: {message}", session_id=session_id)

    async def cancel(self, execution_id: str) -> None:
        self.cancelled.add(execution_id)
