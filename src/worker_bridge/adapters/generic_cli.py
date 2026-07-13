"""Configuration-driven adapter for future noninteractive coding CLIs."""

from __future__ import annotations

import asyncio
import json
import shlex
import shutil
from pathlib import Path

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
            session_id=self._extract_session_id(output) or execution_id,
            error=error or None if proc.returncode else None,
            metadata={"exit_code": proc.returncode, "command": shlex.join(args)},
        )

    @staticmethod
    def _extract_session_id(output: str) -> str | None:
        """Best-effort native session id from a structured CLI's stdout.

        Config workers built on ``claude -p --output-format json`` emit their
        session id in the JSON payload. Without this, the fallback session id
        is the child PID, which the resume_command's ``--resume {session_id}``
        can never resolve — so follow-ups and verification auto-repair
        silently could not work for any config-defined worker."""
        text = output.strip()
        if not text.startswith(("{", "[")):
            return None
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            return None
        candidates = payload if isinstance(payload, list) else [payload]
        for item in candidates:
            if isinstance(item, dict):
                for key in ("session_id", "sessionId", "sessionID"):
                    value = item.get(key)
                    if value:
                        return str(value)
        return None

    async def start(self, task: TaskSpec, runtime: RuntimeContext) -> WorkerResult:
        return await self._run(self.command, task.objective, runtime)

    async def continue_task(
        self, session_id: str, message: str, task: TaskSpec, runtime: RuntimeContext
    ) -> WorkerResult:
        if not self.resume_command:
            raise NotImplementedError(f"{self.name} has no resume command")
        argv = [part.replace("{session_id}", session_id) for part in self.resume_command]
        # Follow-up messages (e.g. a verification auto-repair transcript) are
        # multi-line; passed inline through ``{prompt}`` argv they get mangled
        # by Windows .CMD-shim parsing. Write the message to a brief file in
        # the workspace and hand the CLI a one-line pointer instead.
        brief = Path(runtime.workspace) / f".worker-bridge-followup-{runtime.task_id}.md"
        brief.write_text(
            "Follow-up for the existing task:\n\n" + message, encoding="utf-8"
        )
        pointer = (
            f"Read the file {brief.name} in your current directory and carry out "
            "its follow-up now. Do not just describe the work."
        )
        try:
            return await self._run(argv, pointer, runtime)
        finally:
            brief.unlink(missing_ok=True)

    async def cancel(self, execution_id: str) -> None:
        process = self._processes.get(execution_id)
        if process and process.returncode is None:
            process.terminate()
