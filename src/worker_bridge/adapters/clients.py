"""Supported structured CLI workers discovered from their installed help."""

from __future__ import annotations

import asyncio
import json
import shutil
import uuid
from pathlib import Path
from typing import Any

from worker_bridge.adapters.base import WorkerAdapter
from worker_bridge.models import (
    RuntimeContext,
    TaskSpec,
    WorkerAvailability,
    WorkerCapabilities,
    WorkerResult,
)
from worker_bridge.prompt import build_worker_prompt
from worker_bridge.environ import subprocess_env as hermes_subprocess_env


def _find_value(value: Any, keys: set[str]) -> Any:
    if isinstance(value, dict):
        for key, item in value.items():
            if key in keys and item:
                return item
        for item in value.values():
            found = _find_value(item, keys)
            if found:
                return found
    elif isinstance(value, list):
        for item in value:
            found = _find_value(item, keys)
            if found:
                return found
    return None


class _StructuredCliWorker(WorkerAdapter):
    executable_name: str
    maximum_concurrency = 2

    def __init__(self, executable: str | None = None) -> None:
        self.executable = executable or self.executable_name
        self._processes: dict[str, asyncio.subprocess.Process] = {}

    def _subprocess_env(self) -> dict[str, str]:
        """Environment for every child process this worker spawns.

        A single overlay seam so subclasses can redirect the CLI (e.g. at an
        alternate Anthropic-compatible endpoint) without touching the transport.
        """
        return hermes_subprocess_env(inherit_credentials=True)

    async def detect(self) -> WorkerAvailability:
        executable = shutil.which(self.executable)
        if not executable:
            return WorkerAvailability(False, reason=f"{self.executable_name} executable not found")
        try:
            proc = await asyncio.create_subprocess_exec(
                executable,
                "--version",
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=self._subprocess_env(),
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), 10)
        except (OSError, asyncio.TimeoutError) as exc:
            return WorkerAvailability(False, executable=executable, reason=str(exc))
        output = (stdout or stderr).decode("utf-8", "replace").strip()
        return WorkerAvailability(proc.returncode == 0, None, output or None, executable, None if proc.returncode == 0 else output)

    async def capabilities(self) -> WorkerCapabilities:
        return WorkerCapabilities(
            sessions=True,
            streaming=False,
            structured_output=True,
            approvals=False,
            pause=False,
            sandbox_modes=["read_only", "workspace_write", "full_access", "custom"],
            models=True,
            maximum_concurrency=self.maximum_concurrency,
        )

    async def _execute(
        self,
        task: TaskSpec,
        runtime: RuntimeContext,
        argv: list[str],
        fallback_session_id: str | None,
        instruction_path: Path | None = None,
    ) -> WorkerResult:
        proc = await asyncio.create_subprocess_exec(
            *argv,
            cwd=runtime.workspace,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=self._subprocess_env(),
        )
        self._processes[runtime.task_id] = proc
        runtime.emit("worker.process", {"pid": proc.pid, "client": self.name})
        try:
            stdout, stderr = await proc.communicate()
        finally:
            self._processes.pop(runtime.task_id, None)
            if instruction_path is not None:
                instruction_path.unlink(missing_ok=True)
        stdout = stdout[: runtime.maximum_output_bytes]
        stderr = stderr[: runtime.maximum_output_bytes]
        return self._parse(stdout.decode("utf-8", "replace"), stderr.decode("utf-8", "replace"), proc.returncode, fallback_session_id)

    @staticmethod
    def _instruction_file(runtime: RuntimeContext, content: str) -> Path:
        path = Path(runtime.workspace) / f".worker-bridge-instruction-{runtime.task_id}.md"
        path.write_text(content, encoding="utf-8")
        return path

    async def cancel(self, execution_id: str) -> None:
        process = self._processes.get(execution_id)
        if process and process.returncode is None:
            process.terminate()

    def _parse(self, stdout: str, stderr: str, returncode: int, fallback_session_id: str | None) -> WorkerResult:
        raise NotImplementedError


class ClaudeCodeAdapter(_StructuredCliWorker):
    """Claude Code print/JSON adapter (locally verified against 2.1.186)."""

    name = "claude-code"
    executable_name = "claude"

    @staticmethod
    def _permission_args(task: TaskSpec) -> list[str]:
        if task.permissions.profile == "read_only":
            return ["--permission-mode", "plan"]
        if task.permissions.profile == "full_access":
            return ["--dangerously-skip-permissions"]
        return ["--permission-mode", "acceptEdits"]

    async def start(self, task: TaskSpec, runtime: RuntimeContext) -> WorkerResult:
        session_id = str(uuid.uuid4())
        instruction = self._instruction_file(runtime, build_worker_prompt(task, runtime.workspace))
        short_prompt = f"Read {instruction.name} and execute its Objective now. Do not merely acknowledge it."
        argv = [
            shutil.which(self.executable) or self.executable,
            "--print",
            short_prompt,
            "--output-format",
            "json",
            "--session-id",
            session_id,
            *self._permission_args(task),
        ]
        return await self._execute(task, runtime, argv, session_id, instruction)

    async def continue_task(self, session_id: str, message: str, task: TaskSpec, runtime: RuntimeContext) -> WorkerResult:
        instruction = self._instruction_file(
            runtime,
            "Follow-up for the existing task:\n\n"
            + message
            + "\n\nExecute this follow-up now, inspect the resulting diff, and rerun relevant verification.",
        )
        short_prompt = f"Read {instruction.name} and execute the follow-up now."
        argv = [
            shutil.which(self.executable) or self.executable,
            "--print",
            short_prompt,
            "--output-format",
            "json",
            "--resume",
            session_id,
            *self._permission_args(task),
        ]
        return await self._execute(task, runtime, argv, session_id, instruction)

    def _parse(self, stdout: str, stderr: str, returncode: int, fallback_session_id: str | None) -> WorkerResult:
        try:
            payload = json.loads(stdout)
        except json.JSONDecodeError:
            payload = {}
        malformed = not isinstance(payload, dict) or "result" not in payload
        summary = str(payload.get("result") or stdout)
        session_id = str(payload.get("session_id") or fallback_session_id or "") or None
        failed = returncode != 0 or bool(payload.get("is_error")) or malformed
        return WorkerResult(
            "failed" if failed else "succeeded",
            summary=summary,
            session_id=session_id,
            error=(
                "malformed Claude Code JSON output"
                if malformed and returncode == 0
                else str(payload.get("error") or stderr or "Claude Code failed")
            ) if failed else None,
            metadata={"transport": "claude_print_json", "exit_code": returncode},
        )


class OpenCodeAdapter(_StructuredCliWorker):
    """OpenCode run/JSON adapter (locally verified against 1.17.11)."""

    name = "opencode"
    executable_name = "opencode"

    def _base_args(self, task: TaskSpec, runtime: RuntimeContext) -> list[str]:
        args = [
            shutil.which(self.executable) or self.executable,
            "run",
            "--format",
            "json",
            "--dir",
            runtime.workspace,
        ]
        if task.permissions.profile == "full_access":
            args.append("--dangerously-skip-permissions")
        return args

    async def start(self, task: TaskSpec, runtime: RuntimeContext) -> WorkerResult:
        instruction = self._instruction_file(runtime, build_worker_prompt(task, runtime.workspace))
        short_prompt = f"Read {instruction.name} and execute its Objective now. Do not merely acknowledge it."
        return await self._execute(
            task,
            runtime,
            [
                shutil.which(self.executable) or self.executable,
                "run",
                short_prompt,
                *self._base_args(task, runtime)[2:],
            ],
            None,
            instruction,
        )

    async def continue_task(self, session_id: str, message: str, task: TaskSpec, runtime: RuntimeContext) -> WorkerResult:
        instruction = self._instruction_file(
            runtime,
            "Follow-up for the existing task:\n\n"
            + message
            + "\n\nExecute this follow-up now and rerun relevant verification.",
        )
        short_prompt = f"Read {instruction.name} and execute the follow-up now."
        return await self._execute(
            task,
            runtime,
            [
                shutil.which(self.executable) or self.executable,
                "run",
                short_prompt,
                *self._base_args(task, runtime)[2:],
                "--session",
                session_id,
            ],
            session_id,
            instruction,
        )

    def _parse(self, stdout: str, stderr: str, returncode: int, fallback_session_id: str | None) -> WorkerResult:
        events = []
        for line in stdout.splitlines():
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError:
                continue
        session = _find_value(events, {"sessionID", "sessionId", "session_id"}) or fallback_session_id
        summary = _find_value(list(reversed(events)), {"text", "content", "result", "message"}) or stdout
        malformed = not events
        failed = returncode != 0 or malformed
        return WorkerResult(
            "failed" if failed else "succeeded",
            summary=str(summary),
            session_id=str(session) if session else None,
            error=("malformed OpenCode JSON event stream" if malformed and returncode == 0 else (stderr or "OpenCode failed")) if failed else None,
            metadata={"transport": "opencode_run_json", "exit_code": returncode, "event_count": len(events)},
        )


class DiscoveryOnlyAdapter(WorkerAdapter):
    """Reports an installed client without claiming an unverified execution API."""

    def __init__(self, name: str, executable: str) -> None:
        self.name = name
        self.executable = executable

    async def detect(self) -> WorkerAvailability:
        path = shutil.which(self.executable)
        return WorkerAvailability(bool(path), executable=path, reason="execution adapter not configured" if path else "executable not found")

    async def capabilities(self) -> WorkerCapabilities:
        return WorkerCapabilities()

    async def start(self, task: TaskSpec, runtime: RuntimeContext) -> WorkerResult:
        raise NotImplementedError(f"{self.name} is discovery-only")

    async def continue_task(self, session_id: str, message: str, task: TaskSpec, runtime: RuntimeContext) -> WorkerResult:
        raise NotImplementedError(f"{self.name} is discovery-only")

    async def cancel(self, execution_id: str) -> None:
        return None
