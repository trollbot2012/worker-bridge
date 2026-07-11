"""VS Code bridge worker adapter (experimental).

Delegates a task into a running VS Code window through the companion
``worker-bridge-vscode`` extension (see ``vscode-extension/``), which exposes a
loopback HTTP API on ``127.0.0.1:9394``. ``_submit_turn`` POSTs the worker prompt
to ``/task`` and returns the structured result; ``detect`` probes ``/health`` so
the worker only reports installed when a VS Code window is actually hosting the
bridge — otherwise it fails closed cleanly.

The bearer token is read from ``~/.worker-bridge-vscode-token`` at call time and
sent in the Authorization header only. It is never stored on the adapter
instance, persisted in a task spec, or echoed into any :class:`WorkerResult`.
"""

from __future__ import annotations

import asyncio
import json
import urllib.error
import urllib.request
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

_DEFAULT_HOST = "127.0.0.1"
_DEFAULT_PORT = 9394
_TOKEN_FILE = Path.home() / ".worker-bridge-vscode-token"
_APPROVE_PROFILES = {"workspace_write", "full_access"}
_TRANSPORT_ERRORS = (urllib.error.URLError, OSError, json.JSONDecodeError, TimeoutError)


class VSCodeBridgeAdapter(WorkerAdapter):
    """Drives coding tasks into VS Code via the loopback bridge extension."""

    name = "vscode"
    maximum_concurrency = 1

    def __init__(
        self,
        host: str = _DEFAULT_HOST,
        port: int = _DEFAULT_PORT,
        token_file: str | Path = _TOKEN_FILE,
        request_timeout_seconds: float | None = None,
    ) -> None:
        self.host = host
        self.port = port
        self.token_file = Path(token_file)
        self.request_timeout_seconds = request_timeout_seconds
        # task_id -> session_id, so cancel() can address the right terminal.
        self._sessions: dict[str, str] = {}

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"

    def _read_token(self) -> str | None:
        try:
            token = self.token_file.read_text(encoding="utf-8").strip()
        except OSError:
            return None
        return token or None

    def _http(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
        timeout: float = 10.0,
        authorize: bool = True,
    ) -> dict[str, Any]:
        """Blocking JSON request; always invoked through ``asyncio.to_thread``."""
        data = json.dumps(payload).encode("utf-8") if payload is not None else None
        headers: dict[str, str] = {"Content-Type": "application/json"} if data is not None else {}
        if authorize:
            token = self._read_token()
            if token:
                headers["Authorization"] = f"Bearer {token}"
        request = urllib.request.Request(f"{self.base_url}{path}", data=data, headers=headers, method=method)
        with urllib.request.urlopen(request, timeout=timeout) as response:  # noqa: S310 (loopback only)
            raw = response.read()
        return json.loads(raw.decode("utf-8", "replace")) if raw else {}

    async def detect(self) -> WorkerAvailability:
        try:
            payload = await asyncio.to_thread(self._http, "GET", "/health", None, 5.0, False)
        except _TRANSPORT_ERRORS as exc:
            return WorkerAvailability(
                False,
                reason=f"VS Code bridge unreachable at {self.base_url} ({exc}); "
                "install and run the worker-bridge-vscode extension",
            )
        if not isinstance(payload, dict) or payload.get("status") != "ok":
            return WorkerAvailability(False, reason="unexpected /health response from VS Code bridge")
        agents = [str(agent) for agent in (payload.get("agents") or [])]
        reason = f"VS Code bridge on {self.host}:{self.port}"
        if agents:
            reason += f"; agents: {', '.join(agents)}"
        return WorkerAvailability(
            True,
            authenticated=self._read_token() is not None,
            version=str(payload.get("version") or "") or None,
            executable=self.base_url,
            reason=reason,
        )

    async def capabilities(self) -> WorkerCapabilities:
        return WorkerCapabilities(
            sessions=True,
            streaming=False,
            structured_output=True,
            approvals=False,
            pause=False,
            sandbox_modes=["workspace_write", "full_access"],
            models=False,
            maximum_concurrency=self.maximum_concurrency,
        )

    async def _submit_turn(
        self,
        prompt: str,
        runtime: RuntimeContext,
        *,
        session_id: str | None = None,
        approve_all: bool = True,
    ) -> WorkerResult:
        payload: dict[str, Any] = {"prompt": prompt, "cwd": runtime.workspace, "approve_all": approve_all}
        if session_id:
            payload["session_id"] = session_id
        timeout = float(self.request_timeout_seconds or runtime.timeout_seconds)
        runtime.emit("worker.process", {"client": self.name, "endpoint": self.base_url})
        try:
            response = await asyncio.to_thread(self._http, "POST", "/task", payload, timeout, True)
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", "replace")[:500]
            return self._failure(f"VS Code bridge HTTP {exc.code}: {detail}")
        except _TRANSPORT_ERRORS as exc:
            return self._failure(f"VS Code bridge request failed: {exc}")

        returned_session = str(response.get("session_id") or session_id or "") or None
        if returned_session:
            self._sessions[runtime.task_id] = returned_session
        success = bool(response.get("success"))
        return WorkerResult(
            "succeeded" if success else "failed",
            summary=str(response.get("output") or ""),
            session_id=returned_session,
            changed_files=[str(item) for item in (response.get("changed_files") or [])],
            error=None if success else str(response.get("error") or response.get("output") or "VS Code bridge task failed"),
            metadata={"transport": "vscode_http", "endpoint": self.base_url},
        )

    @staticmethod
    def _failure(error: str) -> WorkerResult:
        return WorkerResult("failed", error=error, metadata={"transport": "vscode_http"})

    async def start(self, task: TaskSpec, runtime: RuntimeContext) -> WorkerResult:
        prompt = build_worker_prompt(task, runtime.workspace)
        approve_all = task.permissions.profile in _APPROVE_PROFILES
        return await self._submit_turn(prompt, runtime, session_id=None, approve_all=approve_all)

    async def continue_task(
        self, session_id: str, message: str, task: TaskSpec, runtime: RuntimeContext
    ) -> WorkerResult:
        follow_up = (
            "Follow-up for the same task and workspace:\n"
            f"{message}\n\nInspect the current workspace state, make the requested correction, "
            "rerun relevant verification, and report exact evidence."
        )
        approve_all = task.permissions.profile in _APPROVE_PROFILES
        return await self._submit_turn(follow_up, runtime, session_id=session_id, approve_all=approve_all)

    async def cancel(self, execution_id: str) -> None:
        session_id = self._sessions.get(execution_id, execution_id)
        if not session_id:
            return
        try:
            await asyncio.to_thread(self._http, "POST", "/cancel", {"session_id": session_id}, 10.0, True)
        except _TRANSPORT_ERRORS:
            pass
        finally:
            self._sessions.pop(execution_id, None)
