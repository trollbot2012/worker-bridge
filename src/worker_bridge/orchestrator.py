"""Transport-independent worker orchestration service."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any

import psutil

from worker_bridge.environ import get_home as get_hermes_home
from worker_bridge.models import (
    JobStatus,
    RuntimeContext,
    TaskSpec,
    TaskStatus,
    TERMINAL_TASK_STATUSES,
    WorkerResult,
)
from worker_bridge.registry import WorkerRegistry
from worker_bridge.store import WorkerStore
from worker_bridge.workspace import RepositoryLock, WorkspaceManager


class OrchestrationError(RuntimeError):
    pass


logger = logging.getLogger(__name__)


def _terminate_runner_tree(pid: int, task_id: str) -> None:
    """Kill a detached runner and every descendant it spawned.

    ``os.kill(pid, SIGTERM)`` on Windows terminates only the named process,
    orphaning the actual worker (codex/claude/opencode) child that keeps
    editing the worktree after the orchestrator believes the task is cancelled. We instead
    walk the process tree. The runner is identified by its command line before
    anything is killed so a recycled PID belonging to an unrelated process is
    never terminated.
    """
    try:
        proc = psutil.Process(pid)
        cmdline = " ".join(proc.cmdline())
    except (psutil.NoSuchProcess, psutil.AccessDenied, ValueError):
        return
    if "worker_bridge.runner" not in cmdline or task_id not in cmdline:
        # PID was reused by an unrelated process; refuse to kill it.
        logger.warning("refusing to cancel pid %s: not the runner for task %s", pid, task_id)
        return
    try:
        children = proc.children(recursive=True)
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        children = []
    for victim in [*children, proc]:
        try:
            victim.kill()
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    psutil.wait_procs([*children, proc], timeout=5)


class WorkerBridge:
    def __init__(
        self,
        *,
        store: WorkerStore | None = None,
        registry: WorkerRegistry | None = None,
        workspaces: WorkspaceManager | None = None,
        maximum_concurrency: int = 4,
        per_repository_concurrency: int = 3,
        per_job_concurrency: int = 3,
        retention: dict[str, bool] | None = None,
    ) -> None:
        self.store = store or WorkerStore()
        self.registry = registry or WorkerRegistry()
        self.workspaces = workspaces or WorkspaceManager()
        # Capacity is enforced with DB-backed leases (see store.acquire_slot) so
        # limits hold host-wide across independently launched runner processes,
        # not just within one event loop. The integers below are the ceilings.
        self._maximum_concurrency = max(1, maximum_concurrency)
        self._per_repository_concurrency = max(1, per_repository_concurrency)
        self._per_job_concurrency = max(1, per_job_concurrency)
        self._worker_capacity: dict[str, int] = {}
        self._retention = {
            "retain_failed_tasks": True,
            "retain_cancelled_tasks": True,
            "retain_unaccepted_tasks": True,
            **(retention or {}),
        }
        self._failures: dict[str, list[float]] = {}
        self.store.recover_running()

    def create_task(self, task: TaskSpec | dict[str, Any]) -> dict[str, Any]:
        spec = task if isinstance(task, TaskSpec) else TaskSpec.from_dict(task)
        spec.validate()
        return self.store.create_task(spec.to_dict())

    def get_task(self, task_id: str) -> dict[str, Any]:
        task = self.store.get_task(task_id)
        if not task:
            raise KeyError(task_id)
        return task

    def list_tasks(self, **kwargs: Any) -> list[dict[str, Any]]:
        return self.store.list_tasks(**kwargs)

    async def list_workers(self) -> list[dict[str, Any]]:
        return await self.registry.list()

    async def get_worker_status(self, worker: str) -> dict[str, Any]:
        return await self.registry.inspect(worker)

    async def get_worker_capabilities(self, worker: str) -> dict[str, Any]:
        return (await self.registry.inspect(worker))["capabilities"]

    def get_job(self, job_id: str) -> dict[str, Any]:
        job = self.store.get_job(job_id)
        if not job:
            raise KeyError(job_id)
        return job

    def list_jobs(self, limit: int = 100) -> list[dict[str, Any]]:
        return self.store.list_jobs(limit)

    async def stream_task_events(
        self,
        task_id: str,
        *,
        after: int = 0,
        poll_interval: float = 0.25,
    ):
        """Replay then tail task events with cursor-based reconnection."""
        cursor = after
        while True:
            events = self.store.events(task_id=task_id, after=cursor)
            for event in events:
                cursor = event["event_id"]
                yield event
            task = self.get_task(task_id)
            if task["status"] in TERMINAL_TASK_STATUSES and not events:
                return
            await asyncio.sleep(poll_interval)

    async def _worker_limit(self, name: str) -> int:
        if name not in self._worker_capacity:
            capabilities = await self.registry.get(name).capabilities()
            self._worker_capacity[name] = max(1, capabilities.maximum_concurrency)
        return self._worker_capacity[name]

    async def _acquire_slots(
        self,
        scopes: list[tuple[str, int]],
        task_id: str,
        pid: int,
        timeout_seconds: float,
    ) -> list[str]:
        """Acquire every capacity slot atomically, releasing partials on failure.

        Blocks (polling) until all slots are free or ``timeout_seconds`` elapses,
        so concurrent runners queue on shared capacity instead of overrunning it.
        """
        deadline = time.monotonic() + max(1.0, float(timeout_seconds))
        while True:
            held: list[str] = []
            for scope, limit in scopes:
                if self.store.acquire_slot(scope, limit, task_id, pid):
                    held.append(scope)
                else:
                    break
            if len(held) == len(scopes):
                return held
            for scope in held:
                self.store.release_slot(scope, task_id)
            if time.monotonic() >= deadline:
                raise OrchestrationError(
                    f"timed out waiting for scheduler capacity: {[s for s, _ in scopes]}"
                )
            await asyncio.sleep(0.05)

    def _release_slots(self, scopes: list[str], task_id: str) -> None:
        for scope in scopes:
            self.store.release_slot(scope, task_id)

    def _circuit_open(self, worker: str) -> bool:
        cutoff = time.time() - 300
        recent = [ts for ts in self._failures.get(worker, []) if ts >= cutoff]
        self._failures[worker] = recent
        persisted = self.store.recent_worker_failures(worker, cutoff)
        return max(len(recent), persisted) >= 3

    def _record_failure(self, worker: str) -> None:
        self._failures.setdefault(worker, []).append(time.time())

    @staticmethod
    def _extract_permission_request(summary: str) -> dict[str, Any] | None:
        text = str(summary or "").strip()
        candidates = [text]
        if "```" in text:
            candidates.extend(part.strip().removeprefix("json").strip() for part in text.split("```")[1::2])
        for candidate in candidates:
            try:
                payload = json.loads(candidate)
            except (json.JSONDecodeError, TypeError):
                continue
            if isinstance(payload, dict) and isinstance(payload.get("permission_request"), dict):
                return payload["permission_request"]
        return None

    @staticmethod
    def _extract_clarification_request(summary: str) -> dict[str, Any] | None:
        text = str(summary or "").strip()
        candidates = [text]
        if "```" in text:
            candidates.extend(part.strip().removeprefix("json").strip() for part in text.split("```")[1::2])
        for candidate in candidates:
            try:
                payload = json.loads(candidate)
            except (json.JSONDecodeError, TypeError):
                continue
            if isinstance(payload, dict) and isinstance(payload.get("clarification_request"), dict):
                return payload["clarification_request"]
        return None

    def _emit(self, task_id: str, kind: str, payload: dict[str, Any]) -> None:
        task = self.get_task(task_id)
        self.store.append_event(kind, payload, task_id=task_id, job_id=task.get("job_id"))

    # States a task may legitimately be started from. A fresh start refuses to
    # re-run finished work; a follow-up may resume a completed native session.
    _FRESH_CLAIMABLE = ("created", "queued", "paused")
    _FOLLOWUP_CLAIMABLE = (
        "created", "queued", "paused", "waiting_input", "succeeded", "failed", "timed_out",
    )

    async def start_task(self, task_id: str, *, follow_up: str | None = None) -> dict[str, Any]:
        task = self.get_task(task_id)
        if task["status"] in {TaskStatus.RUNNING.value, TaskStatus.VERIFYING.value}:
            raise OrchestrationError(f"task is already {task['status']}")
        if not follow_up and task["status"] in {TaskStatus.SUCCEEDED.value, TaskStatus.ACCEPTED.value}:
            # Idempotent: never silently re-execute an already-finished task.
            return task
        spec = TaskSpec.from_dict(task["spec"])
        adapter = self.registry.get(spec.worker)
        if self._circuit_open(spec.worker):
            raise OrchestrationError(f"worker circuit is open: {spec.worker}")
        availability = await adapter.detect()
        if not availability.installed:
            raise OrchestrationError(availability.reason or f"worker unavailable: {spec.worker}")

        pid = os.getpid()
        scopes = [
            ("global", self._maximum_concurrency),
            (f"worker:{spec.worker}", await self._worker_limit(spec.worker)),
            (f"repo:{Path(spec.workspace.repository).resolve()}", self._per_repository_concurrency),
        ]
        acquired = await self._acquire_slots(scopes, task_id, pid, spec.limits.timeout_seconds)
        try:
            # Claim BEFORE preparing the workspace. git worktree/branch creation
            # is not atomic across processes; if two runners both prepared, one
            # would crash with "reference already exists" and its crash handler
            # would clobber the winner's task. Only the single claim winner
            # prepares, so the loser exits cleanly without touching shared state.
            allowed = self._FOLLOWUP_CLAIMABLE if follow_up else self._FRESH_CLAIMABLE
            if not self.store.claim_task(task_id, allowed_from=allowed, pid=pid):
                current = self.get_task(task_id)
                if current["status"] in {TaskStatus.RUNNING.value, TaskStatus.VERIFYING.value}:
                    raise OrchestrationError(f"task is already {current['status']}")
                return current
            runtime = dict(self.get_task(task_id).get("runtime") or {})
            if not runtime.get("path"):
                runtime = self.workspaces.prepare(task_id, spec.to_dict())
            runtime["pid"] = pid
            runtime["started_at"] = time.time()
            self.store.update_task(task_id, runtime=runtime)
            context = RuntimeContext(
                task_id=task_id,
                workspace=runtime["path"],
                emit=lambda kind, payload: self._emit(task_id, kind, payload),
                timeout_seconds=spec.limits.timeout_seconds,
                maximum_output_bytes=spec.limits.maximum_output_bytes,
            )
            self._emit(task_id, "worker.started", {"worker": spec.worker, "pid": pid})
            lock = RepositoryLock(spec.workspace.repository) if runtime["isolation"] == "direct" else None
            lock_acquired = False
            try:
                if lock:
                    lock.__enter__()
                    lock_acquired = True
                if follow_up:
                    session_id = (task.get("result") or {}).get("session_id")
                    if not session_id:
                        raise OrchestrationError("task has no resumable native session")
                    result = await asyncio.wait_for(
                        adapter.continue_task(session_id, follow_up, spec, context),
                        timeout=spec.limits.timeout_seconds,
                    )
                else:
                    result = await asyncio.wait_for(
                        adapter.start(spec, context), timeout=spec.limits.timeout_seconds
                    )
            except asyncio.TimeoutError:
                await adapter.cancel(task_id)
                result = WorkerResult(TaskStatus.TIMED_OUT.value, error="worker timed out")
            except asyncio.CancelledError:
                await adapter.cancel(task_id)
                result = WorkerResult(TaskStatus.CANCELLED.value, error="execution cancelled")
            except Exception as exc:
                result = WorkerResult(TaskStatus.FAILED.value, error=f"{type(exc).__name__}: {exc}")
            finally:
                if lock and lock_acquired:
                    lock.__exit__(None, None, None)
        finally:
            self._release_slots(acquired, task_id)

        result.changed_files = self.workspaces.changed_files(runtime)
        summary_bytes = result.summary.encode("utf-8", "replace")
        if len(summary_bytes) > spec.limits.maximum_output_bytes:
            result.summary = summary_bytes[: spec.limits.maximum_output_bytes].decode(
                "utf-8", "replace"
            ) + "\n[worker output truncated]"
            result.metadata["output_truncated"] = True
            result.metadata["original_output_bytes"] = len(summary_bytes)
        artifact_dir = get_hermes_home() / "workers" / "artifacts" / task_id
        artifact_dir.mkdir(parents=True, exist_ok=True)
        diff_path = artifact_dir / "changes.diff"
        manifest_path = artifact_dir / "manifest.json"
        diff_path.write_text(
            self.workspaces.diff(runtime, spec.limits.maximum_output_bytes), encoding="utf-8"
        )
        manifest_path.write_text(
            json.dumps({"changed_files": result.changed_files}, indent=2), encoding="utf-8"
        )
        result.artifacts.extend([str(diff_path), str(manifest_path)])

        permission_request = self._extract_permission_request(result.summary)
        if permission_request and spec.permissions.allow_escalation:
            stored_request = self.create_permission_request(
                {
                    "task_id": task_id,
                    "worker": spec.worker,
                    "requested_capability": permission_request.get("requested_capability", "unspecified"),
                    "requested_scope": permission_request.get("requested_scope", []),
                    "reason": permission_request.get("reason", "worker requested additional authority"),
                    "proposed_duration": permission_request.get("proposed_duration", "once"),
                    "proposed_command": permission_request.get("proposed_command"),
                    "risk_summary": permission_request.get("risk_summary", ""),
                    "alternatives_considered": permission_request.get("alternatives_considered", []),
                }
            )
            result.status = TaskStatus.WAITING_INPUT.value
            result.metadata["permission_request_id"] = stored_request["request_id"]
        clarification = self._extract_clarification_request(result.summary)
        if clarification:
            stored_input = self.store.create_input_request(task_id, clarification)
            result.status = TaskStatus.WAITING_INPUT.value
            result.metadata["clarification_request_id"] = stored_input["request_id"]

        escapes = self.workspaces.escaping_paths(runtime, result.changed_files)
        if escapes:
            result.metadata["workspace_escapes"] = escapes

        permission_violations: list[str] = []
        if spec.permissions.profile == "read_only" and result.changed_files:
            permission_violations = list(result.changed_files)
        elif spec.permissions.profile == "custom" and result.changed_files:
            allowed = [path.replace("\\", "/").rstrip("/") for path in spec.permissions.paths]
            permission_violations = [
                name for name in result.changed_files
                if not any(name == path or name.startswith(path + "/") for path in allowed)
            ]
        # A symlink/junction escape is a violation under every profile except
        # full_access, even when the in-tree link name itself is permitted.
        if spec.permissions.profile != "full_access":
            permission_violations = sorted(set(permission_violations) | set(escapes))
        if permission_violations:
            result.status = TaskStatus.FAILED.value
            result.error = "permission profile forbids changed files"
            result.metadata["permission_violations"] = permission_violations

        if result.status == TaskStatus.SUCCEEDED.value:
            self.store.update_task(task_id, status=TaskStatus.VERIFYING.value, result=asdict(result))
            verification = self.workspaces.verify(
                runtime, asdict(spec.verification), spec.limits.timeout_seconds
            )
            result.commands.extend(verification["commands"])
            result.metadata["verification"] = verification
            result.status = (
                TaskStatus.SUCCEEDED.value if verification["ok"] else TaskStatus.FAILED.value
            )
            if not verification["ok"] and not result.error:
                result.error = "independent verification failed"
        if result.status == TaskStatus.FAILED.value:
            self._record_failure(spec.worker)
        runtime["completed_at"] = time.time()
        runtime.pop("pid", None)
        self.store.update_task(task_id, status=result.status, runtime=runtime, result=asdict(result))
        self._emit(
            task_id,
            "worker.completed",
            {"status": result.status, "session_id": result.session_id, "error": result.error},
        )
        self._update_parent_job(task.get("job_id"))
        if result.status in {TaskStatus.FAILED.value, TaskStatus.TIMED_OUT.value} and not self._retention["retain_failed_tasks"]:
            self.workspaces.cleanup(runtime)
            runtime["retained"] = False
            self.store.update_task(task_id, runtime=runtime)
        return self.get_task(task_id)

    async def continue_task(self, task_id: str, message: str) -> dict[str, Any]:
        task = self.get_task(task_id)
        spec = TaskSpec.from_dict(task["spec"])
        follow_ups = int((task.get("runtime") or {}).get("follow_up_turns", 0))
        if follow_ups >= spec.limits.maximum_follow_up_turns:
            raise OrchestrationError("maximum follow-up turns reached")
        runtime = dict(task.get("runtime") or {})
        runtime["follow_up_turns"] = follow_ups + 1
        self.store.update_task(task_id, runtime=runtime)
        self._emit(task_id, "task.follow_up", {"message": message})
        return await self.start_task(task_id, follow_up=message)

    async def submit_input(self, task_id: str, message: str) -> dict[str, Any]:
        return await self.continue_task(task_id, message)

    async def attach_session(self, task_id: str, session_id: str) -> dict[str, Any]:
        """Link an idle externally-created native client session to a task."""
        task = self.get_task(task_id)
        spec = TaskSpec.from_dict(task["spec"])
        capabilities = await self.registry.get(spec.worker).capabilities()
        if not capabilities.sessions:
            raise OrchestrationError(f"worker does not support native sessions: {spec.worker}")
        if spec.workspace.isolation != "direct":
            raise OrchestrationError(
                "external session attach requires direct isolation pointed at the session's existing repository"
            )
        if task["status"] in {"running", "verifying"}:
            raise OrchestrationError("cannot attach over an active task")
        result = {
            "status": "paused",
            "summary": "Externally-created worker session attached; waiting for orchestrator input.",
            "session_id": session_id,
            "changed_files": [],
            "commands": [],
            "artifacts": [],
            "error": None,
            "metadata": {"externally_attached": True, "native_session_continuation": True},
        }
        self.store.update_task(task_id, status="paused", result=result)
        self._emit(task_id, "worker.session_attached", {"worker": spec.worker, "session_id": session_id})
        return self.get_task(task_id)

    async def redirect_task(self, task_id: str, message: str) -> dict[str, Any]:
        self._emit(task_id, "task.redirected", {"instruction": message})
        return await self.continue_task(task_id, f"Direction changed by the orchestrator: {message}")

    async def cancel_task(self, task_id: str) -> dict[str, Any]:
        task = self.get_task(task_id)
        if task["status"] in TERMINAL_TASK_STATUSES:
            # Cancellation is a terminal state; do not overwrite a finished task.
            return task
        adapter = self.registry.get(task["worker"])
        await adapter.cancel(task_id)
        pid = (task.get("runtime") or {}).get("pid")
        if pid and int(pid) != os.getpid():
            _terminate_runner_tree(int(pid), task_id)
        self.store.release_all_slots(task_id)
        self.store.update_task(task_id, status=TaskStatus.CANCELLED.value)
        self._emit(task_id, "task.cancelled", {})
        runtime = task.get("runtime") or {}
        if runtime.get("path") and not self._retention["retain_cancelled_tasks"]:
            self.workspaces.cleanup(runtime)
            runtime["retained"] = False
            self.store.update_task(task_id, runtime=runtime)
        return self.get_task(task_id)

    async def cancel_job(self, job_id: str) -> dict[str, Any]:
        job = self.get_job(job_id)
        for task_id in job["tasks"]:
            task = self.get_task(task_id)
            if task["status"] not in TERMINAL_TASK_STATUSES:
                await self.cancel_task(task_id)
        self.store.update_job(job_id, status=JobStatus.CANCELLED.value)
        return self.get_job(job_id)

    def pause_task(self, task_id: str) -> dict[str, Any]:
        task = self.get_task(task_id)
        if task["status"] == "running":
            raise OrchestrationError(
                "the active worker has no safe process pause; cancel it or wait for the turn, then resume the native session"
            )
        if task["status"] not in {"created", "queued", "waiting_input"}:
            raise OrchestrationError(f"cannot pause task in state {task['status']}")
        self.store.update_task(task_id, status=TaskStatus.PAUSED.value)
        return self.get_task(task_id)

    def resume_task(self, task_id: str) -> dict[str, Any]:
        task = self.get_task(task_id)
        if task["status"] != TaskStatus.PAUSED.value:
            raise OrchestrationError("only paused tasks can be resumed")
        self.store.update_task(task_id, status=TaskStatus.CREATED.value)
        return self.get_task(task_id)

    async def retry_task(self, task_id: str) -> dict[str, Any]:
        self.queue_retry(task_id)
        return await self.start_task(task_id)

    def queue_retry(self, task_id: str) -> dict[str, Any]:
        task = self.get_task(task_id)
        spec = TaskSpec.from_dict(task["spec"])
        runtime = dict(task.get("runtime") or {})
        retries = int(runtime.get("retries", 0))
        if retries >= spec.limits.maximum_retries:
            raise OrchestrationError("maximum retries reached")
        runtime["retries"] = retries + 1
        self.store.update_task(task_id, status=TaskStatus.CREATED.value, runtime=runtime)
        self._emit(task_id, "task.retry_queued", {"retry": retries + 1})
        return self.get_task(task_id)

    def replace_worker(self, task_id: str, worker: str) -> dict[str, Any]:
        original = self.get_task(task_id)
        spec = dict(original["spec"])
        spec["task_id"] = None
        spec["idempotency_key"] = None
        spec["parent_task_id"] = task_id
        spec["worker"] = worker
        runtime = original.get("runtime") or {}
        if runtime.get("path"):
            spec["workspace"] = {
                "repository": spec["workspace"]["repository"],
                "working_directory": runtime["path"],
                "isolation": "direct",
                "base_ref": runtime.get("base_commit", "HEAD"),
            }
        replacement = self.create_task(spec)
        self._emit(task_id, "task.handoff", {"replacement_task_id": replacement["task_id"], "worker": worker})
        return replacement

    def verify_task(self, task_id: str) -> dict[str, Any]:
        task = self.get_task(task_id)
        spec = TaskSpec.from_dict(task["spec"])
        runtime = task.get("runtime") or {}
        if not runtime.get("path"):
            raise OrchestrationError("task has no prepared workspace")
        result = self.workspaces.verify(runtime, asdict(spec.verification), spec.limits.timeout_seconds)
        self._emit(task_id, "verification.completed", result)
        return result

    def accept_task(self, task_id: str) -> dict[str, Any]:
        task = self.get_task(task_id)
        verification = (task.get("result") or {}).get("metadata", {}).get("verification", {})
        if task["status"] != TaskStatus.SUCCEEDED.value or not verification.get("ok", False):
            raise OrchestrationError("only independently verified successful tasks can be accepted")
        self.store.update_task(task_id, status=TaskStatus.ACCEPTED.value)
        self._emit(task_id, "task.accepted", {})
        return self.get_task(task_id)

    def reject_task(self, task_id: str, reason: str) -> dict[str, Any]:
        task = self.get_task(task_id)
        self.store.update_task(task_id, status=TaskStatus.REJECTED.value)
        self._emit(task_id, "task.rejected", {"reason": reason})
        runtime = task.get("runtime") or {}
        if runtime.get("path") and not self._retention["retain_unaccepted_tasks"]:
            self.workspaces.cleanup(runtime)
            runtime["retained"] = False
            self.store.update_task(task_id, runtime=runtime)
        return self.get_task(task_id)

    def get_task_artifacts(self, task_id: str) -> list[str]:
        return list((self.get_task(task_id).get("result") or {}).get("artifacts", []))

    def create_permission_request(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self.store.create_permission_request(payload)

    async def answer_input_request(self, request_id: str, answer: str) -> dict[str, Any]:
        request = self.store.answer_input_request(request_id, answer)
        return await self.continue_task(request["task_id"], answer)

    def decide_permission(
        self, request_id: str, decision: str, *, scope: list[str] | None = None
    ) -> dict[str, Any]:
        if decision not in {"approved_once", "approved_task", "approved_narrowed", "denied"}:
            raise ValueError("invalid permission decision")
        decided = self.store.decide_permission(request_id, {"decision": decision, "scope": scope or []})
        if decision.startswith("approved"):
            task = self.get_task(decided["task_id"])
            spec = dict(task["spec"])
            permissions = dict(spec.get("permissions") or {})
            request = decided["request"]
            granted_scope = list(scope or request.get("requested_scope") or [])
            capability = str(request.get("requested_capability") or "")
            if decision == "approved_narrowed" or granted_scope:
                permissions["profile"] = "custom"
                permissions["paths"] = granted_scope
            elif capability in {"full_access", "host_filesystem_full", "danger_full_access"}:
                permissions["profile"] = "full_access"
            if capability == "network":
                permissions["network"] = "allow"
                permissions["domains"] = granted_scope
            spec["permissions"] = permissions
            self.store.update_task_spec(task["task_id"], spec)
            self.store.update_task(task["task_id"], status=TaskStatus.PAUSED.value)
        return decided

    def approve_request(self, request_id: str, *, scope: list[str] | None = None) -> dict[str, Any]:
        return self.decide_permission(
            request_id, "approved_narrowed" if scope else "approved_task", scope=scope
        )

    def deny_request(self, request_id: str) -> dict[str, Any]:
        return self.decide_permission(request_id, "denied")

    def modify_permission_request(self, request_id: str, scope: list[str]) -> dict[str, Any]:
        return self.decide_permission(request_id, "approved_narrowed", scope=scope)

    def create_job(
        self,
        objective: str,
        task_specs: list[TaskSpec | dict[str, Any]],
        *,
        strategy: str = "parallel",
    ) -> dict[str, Any]:
        if strategy not in {"single", "parallel", "implement_review", "competing", "map_reduce", "debate"}:
            raise ValueError(f"unsupported job strategy: {strategy}")
        job = self.store.create_job(objective=objective, strategy=strategy)
        for item in task_specs:
            spec = item.to_dict() if isinstance(item, TaskSpec) else dict(item)
            spec["job_id"] = job["job_id"]
            self.create_task(spec)
        return self.store.get_job(job["job_id"])  # type: ignore[return-value]

    async def run_job(self, job_id: str) -> dict[str, Any]:
        job = self.store.get_job(job_id)
        if not job:
            raise KeyError(job_id)
        self.store.update_job(job_id, status=JobStatus.RUNNING.value)
        task_ids = sorted(
            job["tasks"], key=lambda task_id: self.get_task(task_id)["priority"], reverse=True
        )
        if job["strategy"] == "implement_review":
            implementers = [tid for tid in task_ids if self.get_task(tid)["spec"].get("role") != "reviewer"]
            reviewers = [tid for tid in task_ids if tid not in implementers]
            for task_id in implementers:
                await self.start_task(task_id)
            upstream = []
            for task_id in implementers:
                item = self.get_task(task_id)
                upstream.append({
                    "task_id": task_id,
                    "objective": item["spec"]["objective"],
                    "summary": (item.get("result") or {}).get("summary", ""),
                    "diff": self.workspaces.diff(item["runtime"], 500_000),
                    "verification": (item.get("result") or {}).get("metadata", {}).get("verification", {}),
                })
            for task_id in reviewers:
                reviewer = self.get_task(task_id)
                reviewer_spec = dict(reviewer["spec"])
                reviewer_context = dict(reviewer_spec.get("context") or {})
                reviewer_context["upstream_results"] = upstream
                reviewer_spec["context"] = reviewer_context
                self.store.update_task_spec(task_id, reviewer_spec)
                await self.start_task(task_id)
            if implementers and reviewers:
                findings = "\n\n".join(
                    (self.get_task(task_id).get("result") or {}).get("summary", "")
                    for task_id in reviewers
                ).strip()
                if findings:
                    await self.continue_task(
                        implementers[0],
                        "An independent reviewer produced the findings below. Address findings that are valid, explain any rejected finding, and rerun verification.\n\n" + findings,
                    )
        elif job["strategy"] in {"map_reduce", "debate"}:
            reducers = [
                task_id for task_id in task_ids
                if self.get_task(task_id)["spec"].get("role") in {"synthesizer", "integrator"}
            ]
            contributors = [task_id for task_id in task_ids if task_id not in reducers]
            job_limit = asyncio.Semaphore(self._per_job_concurrency)

            async def run_contributor(task_id: str) -> Any:
                async with job_limit:
                    return await self.start_task(task_id)

            await asyncio.gather(
                *(run_contributor(task_id) for task_id in contributors), return_exceptions=True
            )
            upstream = []
            for task_id in contributors:
                item = self.get_task(task_id)
                upstream.append({
                    "task_id": task_id,
                    "worker": item["worker"],
                    "role": item["spec"].get("role"),
                    "status": item["status"],
                    "summary": (item.get("result") or {}).get("summary", ""),
                    "verification": (item.get("result") or {}).get("metadata", {}).get("verification", {}),
                })
            for task_id in reducers:
                reducer = self.get_task(task_id)
                reducer_spec = dict(reducer["spec"])
                context = dict(reducer_spec.get("context") or {})
                context["upstream_results"] = upstream
                reducer_spec["context"] = context
                self.store.update_task_spec(task_id, reducer_spec)
                await self.start_task(task_id)
        else:
            job_limit = asyncio.Semaphore(self._per_job_concurrency)

            async def run_one(task_id: str) -> Any:
                async with job_limit:
                    return await self.start_task(task_id)

            await asyncio.gather(*(run_one(task_id) for task_id in task_ids), return_exceptions=True)
        self._update_parent_job(job_id)
        finished = self.store.get_job(job_id)
        payload = dict((finished or {}).get("payload") or {})
        payload["task_results"] = [
            {
                "task_id": task_id,
                "worker": self.get_task(task_id)["worker"],
                "status": self.get_task(task_id)["status"],
                "summary": (self.get_task(task_id).get("result") or {}).get("summary", ""),
            }
            for task_id in task_ids
        ]
        self.store.update_job(job_id, status=(finished or {}).get("status", "failed"), payload=payload)
        return self.store.get_job(job_id)  # type: ignore[return-value]

    def _update_parent_job(self, job_id: str | None) -> None:
        if not job_id:
            return
        job = self.store.get_job(job_id)
        if not job:
            return
        statuses = [self.get_task(task_id)["status"] for task_id in job["tasks"]]
        if statuses and all(status in {"succeeded", "accepted"} for status in statuses):
            status = JobStatus.SUCCEEDED.value
        elif any(status in {"running", "queued", "created", "verifying"} for status in statuses):
            status = JobStatus.RUNNING.value
        elif any(status in {"succeeded", "accepted"} for status in statuses):
            status = JobStatus.PARTIAL.value
        else:
            status = JobStatus.FAILED.value
        self.store.update_job(job_id, status=status)

    def compare_results(self, job_id: str) -> dict[str, Any]:
        job = self.store.get_job(job_id)
        if not job:
            raise KeyError(job_id)
        candidates = []
        for task_id in job["tasks"]:
            task = self.get_task(task_id)
            result = task.get("result") or {}
            verification = result.get("metadata", {}).get("verification", {})
            candidates.append(
                {
                    "task_id": task_id,
                    "worker": task["worker"],
                    "status": task["status"],
                    "verified": verification.get("ok", False),
                    "changed_file_count": len(result.get("changed_files", [])),
                    "summary": result.get("summary", ""),
                    "error": result.get("error"),
                }
            )
        candidates.sort(key=lambda item: (not item["verified"], item["status"] != "succeeded", item["changed_file_count"]))
        return {"job_id": job_id, "strategy": job["strategy"], "auto_merged": False, "candidates": candidates}

    def integrate_results(
        self,
        job_id: str,
        selected_task_ids: list[str],
        *,
        verification_commands: list[str] | None = None,
    ) -> dict[str, Any]:
        """Apply explicitly selected verified diffs in a dedicated worktree.

        This never merges or commits to the primary branch. Any failed 3-way
        application is preserved as an integration artifact and returned as a
        structured conflict instead of being silently resolved.
        """
        job = self.store.get_job(job_id)
        if not job:
            raise KeyError(job_id)
        if not selected_task_ids:
            raise ValueError("at least one selected task is required")
        if any(task_id not in job["tasks"] for task_id in selected_task_ids):
            raise ValueError("selected task does not belong to the job")
        selected = [self.get_task(task_id) for task_id in selected_task_ids]
        for task in selected:
            verified = (task.get("result") or {}).get("metadata", {}).get("verification", {}).get("ok")
            if task["status"] not in {"succeeded", "accepted"} or not verified:
                raise OrchestrationError(f"task is not independently verified: {task['task_id']}")
        repositories = {task["spec"]["workspace"]["repository"] for task in selected}
        if len(repositories) != 1:
            raise OrchestrationError("selected results target different repositories")
        integration = self.create_task(
            {
                "objective": f"Integrate selected results for {job_id}",
                "job_id": job_id,
                "worker": "mock",
                "role": "integrator",
                "workspace": {
                    "repository": next(iter(repositories)),
                    "isolation": "git_worktree",
                    "base_ref": selected[0]["runtime"]["base_commit"],
                },
                "permissions": {"profile": "workspace_write"},
                "verification": {"commands": verification_commands or []},
                "context": {"upstream_results": selected_task_ids},
            }
        )
        task_id = integration["task_id"]
        runtime = self.workspaces.prepare(task_id, integration["spec"])
        self.store.update_task(task_id, status="running", runtime=runtime)
        applications = []
        for task in selected:
            artifacts = (task.get("result") or {}).get("artifacts", [])
            diff_path = next((item for item in artifacts if item.endswith("changes.diff")), None)
            if not diff_path:
                applications.append({"ok": False, "task_id": task["task_id"], "error": "missing diff artifact"})
                break
            applied = self.workspaces.apply_diff(runtime, diff_path)
            applied["task_id"] = task["task_id"]
            applications.append(applied)
            if not applied["ok"]:
                break
        all_applied = all(item.get("ok") for item in applications) and len(applications) == len(selected)
        verification = (
            self.workspaces.verify(runtime, {"commands": verification_commands or [], "forbidden_paths": []}, 3600)
            if all_applied
            else {"ok": False, "commands": [], "changed_files": self.workspaces.changed_files(runtime), "forbidden_files": []}
        )
        status = "succeeded" if all_applied and verification["ok"] else "failed"
        result = {
            "status": status,
            "summary": "Selected results integrated in an isolated worktree" if status == "succeeded" else "Integration requires conflict resolution",
            "changed_files": self.workspaces.changed_files(runtime),
            "commands": verification["commands"],
            "artifacts": [item.get("diff") for item in applications if item.get("diff")],
            "error": None if status == "succeeded" else "one or more selected diffs conflicted or verification failed",
            "metadata": {"applications": applications, "verification": verification, "explicit_selection": selected_task_ids},
        }
        self.store.update_task(task_id, status=status, runtime=runtime, result=result)
        job_payload = dict(job.get("payload") or {})
        job_payload["final_result"] = {
            "integration_task_id": task_id,
            "status": status,
            "workspace": runtime["path"],
            "selected_tasks": selected_task_ids,
        }
        self.store.update_job(job_id, status="succeeded" if status == "succeeded" else "partial", payload=job_payload)
        self._emit(task_id, "integration.completed", result["metadata"])
        logger.info("worker integration %s completed with status=%s", task_id, status)
        return self.get_task(task_id)
