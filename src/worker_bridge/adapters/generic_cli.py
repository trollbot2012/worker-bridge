"""Configuration-driven adapter for future noninteractive coding CLIs."""

from __future__ import annotations

import asyncio
import shlex
import shutil

from worker_bridge.environ import subprocess_env as hermes_subprocess_env

from worker_bridge.adapters.base import WorkerAdapter
from worker_bridge.models import (
    RuntimeContext,
    TaskSpec,
    WorkerAvailability,
    WorkerCapabilities,
    WorkerResult,
)


class GenericCliAdapter(WorkerAdapter):
    def __init__(
        self,
        name: str,
        command: list[str],
        *,
        resume_command: list[str] | None = None,
        maximum_concurrency: int = 1,
    ) -> None:
        self.name = name
        self.command = command
        self.resume_command = resume_command
        self.maximum_concurrency = maximum_concurrency
        self._processes: dict[str, asyncio.subprocess.Process] = {}

    async def detect(self) -> WorkerAvailability:
        executable = shutil.which(self.command[0])
        return WorkerAvailability(bool(executable), None, executable=executable, reason=None if executable else "executable not found")

    async def capabilities(self) -> WorkerCapabilities:
        return WorkerCapabilities(
            sessions=self.resume_command is not None,
            streaming=True,
            structured_output=False,
            maximum_concurrency=self.maximum_concurrency,
        )

    async def _run(self, argv: list[str], prompt: str, runtime: RuntimeContext) -> WorkerResult:
        args = [part.replace("{prompt}", prompt) for part in argv]
        proc = await asyncio.create_subprocess_exec(
            *args,
            cwd=runtime.workspace,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=hermes_subprocess_env(inherit_credentials=True),
        )
        execution_id = str(proc.pid)
        self._processes[runtime.task_id] = proc
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), runtime.timeout_seconds)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            return WorkerResult("timed_out", error="worker timed out")
        finally:
            self._processes.pop(runtime.task_id, None)
        output = stdout[: runtime.maximum_output_bytes].decode("utf-8", "replace")
        error = stderr[: runtime.maximum_output_bytes].decode("utf-8", "replace")
        return WorkerResult(
            "succeeded" if proc.returncode == 0 else "failed",
            summary=output,
            session_id=execution_id,
            error=error or None if proc.returncode else None,
            metadata={"exit_code": proc.returncode, "command": shlex.join(args)},
        )

    async def start(self, task: TaskSpec, runtime: RuntimeContext) -> WorkerResult:
        return await self._run(self.command, task.objective, runtime)

    async def continue_task(
        self, session_id: str, message: str, task: TaskSpec, runtime: RuntimeContext
    ) -> WorkerResult:
        if not self.resume_command:
            raise NotImplementedError(f"{self.name} has no resume command")
        argv = [part.replace("{session_id}", session_id) for part in self.resume_command]
        return await self._run(argv, message, runtime)

    async def cancel(self, execution_id: str) -> None:
        process = self._processes.get(execution_id)
        if process and process.returncode is None:
            process.terminate()
