"""Typed contracts shared by every worker adapter and transport."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any


class TaskStatus(str, Enum):
    CREATED = "created"
    QUEUED = "queued"
    RUNNING = "running"
    WAITING_INPUT = "waiting_input"
    PAUSED = "paused"
    VERIFYING = "verifying"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMED_OUT = "timed_out"
    ACCEPTED = "accepted"
    REJECTED = "rejected"


class JobStatus(str, Enum):
    CREATED = "created"
    RUNNING = "running"
    PARTIAL = "partial"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


TERMINAL_TASK_STATUSES = {
    TaskStatus.SUCCEEDED.value,
    TaskStatus.FAILED.value,
    TaskStatus.CANCELLED.value,
    TaskStatus.TIMED_OUT.value,
    TaskStatus.ACCEPTED.value,
    TaskStatus.REJECTED.value,
}


@dataclass(slots=True)
class WorkerAvailability:
    installed: bool
    authenticated: bool | None = None
    version: str | None = None
    executable: str | None = None
    reason: str | None = None


@dataclass(slots=True)
class WorkerCapabilities:
    sessions: bool = False
    streaming: bool = False
    structured_output: bool = False
    approvals: bool = False
    pause: bool = False
    sandbox_modes: list[str] = field(default_factory=list)
    models: bool = False
    maximum_concurrency: int = 1


@dataclass(slots=True)
class WorkerHealth:
    healthy: bool
    detail: str = ""


@dataclass(slots=True)
class PermissionSpec:
    profile: str = "workspace_write"
    network: str = "request"
    host_filesystem: str = "request"
    secrets: str = "brokered"
    allow_escalation: bool = True
    paths: list[str] = field(default_factory=list)
    commands: list[str] = field(default_factory=list)
    domains: list[str] = field(default_factory=list)

    def validate(self) -> None:
        if self.profile not in {"read_only", "workspace_write", "full_access", "custom"}:
            raise ValueError(f"unsupported permission profile: {self.profile}")


@dataclass(slots=True)
class WorkspaceSpec:
    repository: str
    working_directory: str | None = None
    isolation: str = "git_worktree"
    base_ref: str = "HEAD"

    def validate(self) -> None:
        repository = Path(self.repository).expanduser()
        if not repository.is_absolute():
            raise ValueError("workspace.repository must be an absolute path")
        if not repository.exists():
            raise ValueError(f"repository does not exist: {repository}")
        if self.isolation not in {"git_worktree", "direct", "copy"}:
            raise ValueError(f"unsupported workspace isolation: {self.isolation}")


@dataclass(slots=True)
class VerificationSpec:
    commands: list[str] = field(default_factory=list)
    require_clean_test_run: bool = True
    require_diff_review: bool = True
    forbidden_paths: list[str] = field(default_factory=list)


@dataclass(slots=True)
class TaskLimits:
    timeout_seconds: int = 3600
    maximum_retries: int = 2
    maximum_follow_up_turns: int = 20
    maximum_output_bytes: int = 10_000_000

    def validate(self) -> None:
        if self.timeout_seconds < 1:
            raise ValueError("timeout_seconds must be positive")
        if self.maximum_retries < 0 or self.maximum_follow_up_turns < 0:
            raise ValueError("retry and follow-up limits cannot be negative")
        if self.maximum_output_bytes < 1024:
            raise ValueError("maximum_output_bytes must be at least 1024")


@dataclass(slots=True)
class TaskSpec:
    objective: str
    workspace: WorkspaceSpec
    worker: str = "codex"
    role: str = "implementer"
    task_id: str | None = None
    job_id: str | None = None
    parent_task_id: str | None = None
    idempotency_key: str | None = None
    priority: int = 50
    context: dict[str, Any] = field(default_factory=dict)
    permissions: PermissionSpec = field(default_factory=PermissionSpec)
    constraints: list[str] = field(default_factory=list)
    acceptance_criteria: list[str] = field(default_factory=list)
    verification: VerificationSpec = field(default_factory=VerificationSpec)
    limits: TaskLimits = field(default_factory=TaskLimits)
    metadata: dict[str, Any] = field(default_factory=dict)

    def validate(self) -> None:
        if not self.objective.strip():
            raise ValueError("objective is required")
        if not self.worker.strip():
            raise ValueError("worker is required")
        if not 0 <= self.priority <= 100:
            raise ValueError("priority must be between 0 and 100")
        self.workspace.validate()
        self.permissions.validate()
        self.limits.validate()

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "TaskSpec":
        data = dict(payload)
        data["workspace"] = WorkspaceSpec(**data["workspace"])
        data["permissions"] = PermissionSpec(**data.get("permissions", {}))
        data["verification"] = VerificationSpec(**data.get("verification", {}))
        data["limits"] = TaskLimits(**data.get("limits", {}))
        spec = cls(**data)
        spec.validate()
        return spec


@dataclass(slots=True)
class WorkerExecution:
    execution_id: str
    session_id: str | None = None
    status: str = TaskStatus.RUNNING.value


@dataclass(slots=True)
class WorkerResult:
    status: str
    summary: str = ""
    session_id: str | None = None
    changed_files: list[str] = field(default_factory=list)
    commands: list[dict[str, Any]] = field(default_factory=list)
    artifacts: list[str] = field(default_factory=list)
    error: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class PermissionRequest:
    task_id: str
    worker: str
    requested_capability: str
    requested_scope: list[str]
    reason: str
    request_id: str | None = None
    proposed_duration: str = "once"
    proposed_command: str | None = None
    risk_summary: str = ""
    alternatives_considered: list[str] = field(default_factory=list)


@dataclass(slots=True)
class RuntimeContext:
    task_id: str
    workspace: str
    emit: Any
    timeout_seconds: int
    maximum_output_bytes: int

