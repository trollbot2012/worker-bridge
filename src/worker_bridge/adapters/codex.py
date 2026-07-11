"""Codex worker adapter (non-interactive `codex exec`).

The standalone bridge talks to Codex the same way it talks to Claude Code and
OpenCode: through the client's documented non-interactive CLI, not a private
app-server transport. Sandbox selection uses Codex's stable ``-c sandbox_mode``
config overrides.
"""

from __future__ import annotations

import shutil

from worker_bridge.adapters.clients import _StructuredCliWorker
from worker_bridge.models import RuntimeContext, TaskSpec, WorkerCapabilities, WorkerResult
from worker_bridge.prompt import build_worker_prompt


class CodexAdapter(_StructuredCliWorker):
    """OpenAI Codex CLI adapter."""

    name = "codex"
    executable_name = "codex"
    maximum_concurrency = 3

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

    @staticmethod
    def _sandbox_args(task: TaskSpec) -> list[str]:
        profile = task.permissions.profile
        sandbox = {
            "read_only": "read-only",
            "workspace_write": "workspace-write",
            "full_access": "danger-full-access",
            "custom": "workspace-write",
        }[profile]
        args = ["-c", f'sandbox_mode="{sandbox}"']
        if profile == "workspace_write":
            network = "true" if task.permissions.network == "allow" else "false"
            args.extend(["-c", f"sandbox_workspace_write.network_access={network}"])
        return args

    def _argv(self, task: TaskSpec, prompt: str) -> list[str]:
        executable = shutil.which(self.executable) or self.executable
        return [executable, "exec", *self._sandbox_args(task), prompt]

    async def start(self, task: TaskSpec, runtime: RuntimeContext) -> WorkerResult:
        instruction = self._instruction_file(runtime, build_worker_prompt(task, runtime.workspace))
        prompt = f"Read {instruction.name} and execute its Objective now. Do not merely acknowledge it."
        return await self._execute(task, runtime, self._argv(task, prompt), None, instruction)

    async def continue_task(
        self, session_id: str, message: str, task: TaskSpec, runtime: RuntimeContext
    ) -> WorkerResult:
        instruction = self._instruction_file(
            runtime,
            "Follow-up for the same task and workspace:\n\n"
            + message
            + "\n\nInspect the current workspace state, make the requested correction, "
            "rerun relevant verification, and report exact evidence.",
        )
        prompt = f"Read {instruction.name} and execute the follow-up now."
        # Codex `exec` continuation semantics vary by version; the bridge re-runs
        # in the same worktree (state persists) rather than depending on a
        # native resume flag that may be absent.
        return await self._execute(task, runtime, self._argv(task, prompt), session_id, instruction)

    def _parse(self, stdout: str, stderr: str, returncode: int, fallback_session_id: str | None) -> WorkerResult:
        summary = stdout.strip() or stderr.strip()
        failed = returncode != 0
        return WorkerResult(
            "failed" if failed else "succeeded",
            summary=summary,
            session_id=fallback_session_id,
            error=(stderr.strip() or "codex exec failed") if failed else None,
            metadata={"transport": "codex_exec", "exit_code": returncode},
        )
