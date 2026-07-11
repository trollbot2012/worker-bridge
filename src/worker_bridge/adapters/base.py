"""Adapter contract and registration primitives."""

from __future__ import annotations

from abc import ABC, abstractmethod

from worker_bridge.models import (
    RuntimeContext,
    TaskSpec,
    WorkerAvailability,
    WorkerCapabilities,
    WorkerHealth,
    WorkerResult,
)


class WorkerAdapter(ABC):
    name: str

    @abstractmethod
    async def detect(self) -> WorkerAvailability: ...

    @abstractmethod
    async def capabilities(self) -> WorkerCapabilities: ...

    @abstractmethod
    async def start(self, task: TaskSpec, runtime: RuntimeContext) -> WorkerResult: ...

    @abstractmethod
    async def continue_task(
        self, session_id: str, message: str, task: TaskSpec, runtime: RuntimeContext
    ) -> WorkerResult: ...

    async def submit_input(self, execution_id: str, message: str) -> None:
        raise NotImplementedError(f"{self.name} does not accept mid-turn input")

    async def approve(self, request_id: str, decision: dict) -> None:
        raise NotImplementedError(f"{self.name} does not support live approvals")

    async def pause(self, execution_id: str) -> None:
        raise NotImplementedError(f"{self.name} cannot pause an active execution")

    async def resume(self, execution_id: str) -> None:
        raise NotImplementedError(f"{self.name} cannot resume an active execution")

    @abstractmethod
    async def cancel(self, execution_id: str) -> None: ...

    async def health_check(self) -> WorkerHealth:
        availability = await self.detect()
        return WorkerHealth(availability.installed, availability.reason or "available")

    async def collect_result(self, execution_id: str) -> WorkerResult:
        raise NotImplementedError("results are returned by start/continue_task")

