"""Workflow-typed dispatch and verification auto-repair."""

from __future__ import annotations

import argparse
import asyncio
import subprocess
import sys
from pathlib import Path

import pytest

from worker_bridge.adapters.generic_cli import GenericCliAdapter
from worker_bridge.adapters.mock import MockWorkerAdapter
from worker_bridge.cli import _load_task_spec
from worker_bridge.models import TaskSpec
from worker_bridge.orchestrator import WorkerBridge
from worker_bridge.registry import WorkerRegistry
from worker_bridge.store import WorkerStore
from worker_bridge.workflows import TASK_TYPES, apply_task_type
from worker_bridge.workspace import WorkspaceManager


def _git(*args: str, cwd: Path) -> None:
    subprocess.run(args, cwd=cwd, text=True, capture_output=True, check=True)


@pytest.fixture
def repository(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git("git", "init", cwd=repo)
    _git("git", "config", "user.email", "worker@example.test", cwd=repo)
    _git("git", "config", "user.name", "Worker Test", cwd=repo)
    (repo / "README.md").write_text("base\n", encoding="utf-8")
    _git("git", "add", ".", cwd=repo)
    _git("git", "commit", "-m", "base", cwd=repo)
    return repo


def spec(repository: Path, **overrides) -> dict:
    value = {
        "objective": "write a deterministic result",
        "worker": "mock",
        "workspace": {"repository": str(repository.resolve()), "isolation": "git_worktree"},
        "permissions": {"profile": "workspace_write"},
        "acceptance_criteria": ["result exists"],
        "verification": {"commands": []},
        "metadata": {"mock_write": "result.txt", "mock_content": "done\n"},
    }
    value.update(overrides)
    return value


# ---------------------------------------------------------------- type profiles


def test_apply_task_type_fills_only_absent_fields(repository: Path):
    payload = spec(repository)
    payload.pop("metadata")
    payload.pop("worker")
    apply_task_type(payload, "chore", available_workers=["codex", "mock"])
    assert payload["metadata"]["type"] == "chore"
    assert payload["metadata"]["auto_repair"] == 1
    assert payload["priority"] == 30
    assert payload["limits"]["timeout_seconds"] == 900
    # chore prefers haiku > codex > claude-code; haiku unavailable here.
    assert payload["worker"] == "codex"
    task = TaskSpec.from_dict(payload)
    assert task.priority == 30


def test_apply_task_type_never_overrides_explicit_values(repository: Path):
    payload = spec(repository)  # spec() sets worker="mock"
    payload["priority"] = 77
    payload["limits"] = {"timeout_seconds": 120}
    payload["metadata"]["auto_repair"] = 0
    apply_task_type(payload, "chore", available_workers=["codex", "mock"])
    assert payload["worker"] == "mock"
    assert payload["priority"] == 77
    assert payload["limits"]["timeout_seconds"] == 120
    assert payload["metadata"]["auto_repair"] == 0
    assert payload["metadata"]["type"] == "chore"


def test_apply_task_type_rejects_unknown_type(repository: Path):
    with pytest.raises(ValueError, match="unknown task type"):
        apply_task_type(spec(repository), "bogus")


def test_hotfix_jumps_the_queue(repository: Path):
    hotfix = apply_task_type(spec(repository), "hotfix")
    feature = apply_task_type(spec(repository), "feature")
    assert hotfix["priority"] > feature["priority"]


def test_all_declared_types_produce_valid_specs(repository: Path):
    for task_type in TASK_TYPES:
        payload = apply_task_type(spec(repository), task_type)
        TaskSpec.from_dict(payload).validate()


def test_cli_create_applies_type(repository: Path):
    args = argparse.Namespace(
        spec=None,
        objective="fix a typo",
        repo=str(repository),
        task_type="chore",
        worker=None,
        priority=None,
        role="implementer",
        base_ref="HEAD",
        isolation="git_worktree",
        permission="workspace_write",
        verify=[],
        acceptance=[],
        constraint=[],
        forbid=[],
        idempotency_key=None,
        job_id=None,
        timeout=None,
    )
    payload = _load_task_spec(args, available_workers=["claude-code", "codex"])
    assert payload["metadata"]["type"] == "chore"
    assert payload["worker"] == "codex"
    assert payload["priority"] == 30
    # Explicit flags beat the profile.
    args.worker, args.timeout, args.priority = "claude-code", 60, 95
    payload = _load_task_spec(args, available_workers=["claude-code", "codex"])
    assert payload["worker"] == "claude-code"
    assert payload["limits"]["timeout_seconds"] == 60
    assert payload["priority"] == 95


# ------------------------------------------------------- verification auto-repair


class RepairingMockAdapter(MockWorkerAdapter):
    """Mock worker whose follow-up turn actually fixes the verification failure."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.repair_messages: list[str] = []

    async def continue_task(self, session_id, message, task, runtime):
        self.repair_messages.append(message)
        fix = task.metadata.get("mock_repair_write")
        if fix and len(self.repair_messages) >= int(task.metadata.get("mock_repairs_needed", 1)):
            (Path(runtime.workspace) / str(fix)).write_text("repaired\n", encoding="utf-8")
        return await super().continue_task(session_id, message, task, runtime)


def _repair_bridge(tmp_path: Path, adapter: MockWorkerAdapter, **bridge_kwargs) -> WorkerBridge:
    return WorkerBridge(
        store=WorkerStore(tmp_path / "repair-bridge.db"),
        registry=WorkerRegistry([adapter]),
        workspaces=WorkspaceManager(tmp_path / "repair-worktrees"),
        **bridge_kwargs,
    )


def _needs_fix_verification() -> dict:
    check = (
        f'"{sys.executable}" -c "from pathlib import Path; '
        "assert Path('fixed.txt').exists(), 'fixed.txt missing'\""
    )
    return {"commands": [check]}


def test_auto_repair_recovers_verification_failure(tmp_path: Path, repository: Path):
    adapter = RepairingMockAdapter()
    bridge = _repair_bridge(tmp_path, adapter, verification_auto_repair=1)
    task = bridge.create_task({
        **spec(repository),
        "verification": _needs_fix_verification(),
        "metadata": {"mock_write": "broken.txt", "mock_repair_write": "fixed.txt"},
    })
    result = asyncio.run(bridge.start_task(task["task_id"]))
    assert result["status"] == "succeeded"
    assert result["result"]["metadata"]["verification"]["ok"] is True
    assert len(adapter.repair_messages) == 1
    message = adapter.repair_messages[0]
    assert "Independent verification failed" in message
    assert "fixed.txt missing" in message  # the failing check's output made it back
    kinds = [e["kind"] for e in bridge.store.events(task_id=task["task_id"], after=0)]
    assert "verification.auto_repair" in kinds


def test_auto_repair_budget_is_terminal(tmp_path: Path, repository: Path):
    adapter = RepairingMockAdapter()
    bridge = _repair_bridge(tmp_path, adapter, verification_auto_repair=2)
    task = bridge.create_task({
        **spec(repository),
        "verification": _needs_fix_verification(),
        # Never repaired: mock_repair_write absent, so every retry re-fails.
        "metadata": {"mock_write": "broken.txt"},
    })
    result = asyncio.run(bridge.start_task(task["task_id"]))
    assert result["status"] == "failed"
    assert result["result"]["error"] == "independent verification failed"
    assert len(adapter.repair_messages) == 2  # budget spent, then terminal


def test_auto_repair_disabled_by_metadata_override(tmp_path: Path, repository: Path):
    adapter = RepairingMockAdapter()
    bridge = _repair_bridge(tmp_path, adapter, verification_auto_repair=2)
    task = bridge.create_task({
        **spec(repository),
        "verification": _needs_fix_verification(),
        "metadata": {"mock_write": "broken.txt", "mock_repair_write": "fixed.txt", "auto_repair": 0},
    })
    result = asyncio.run(bridge.start_task(task["task_id"]))
    assert result["status"] == "failed"
    assert adapter.repair_messages == []


def test_auto_repair_needing_two_rounds(tmp_path: Path, repository: Path):
    adapter = RepairingMockAdapter()
    bridge = _repair_bridge(tmp_path, adapter, verification_auto_repair=3)
    task = bridge.create_task({
        **spec(repository),
        "verification": _needs_fix_verification(),
        "metadata": {
            "mock_write": "broken.txt",
            "mock_repair_write": "fixed.txt",
            "mock_repairs_needed": 2,
        },
    })
    result = asyncio.run(bridge.start_task(task["task_id"]))
    assert result["status"] == "succeeded"
    assert len(adapter.repair_messages) == 2


# -------------------------------------------------- generic adapter session ids


def test_generic_adapter_extracts_claude_json_session_id():
    payload = '{"result": "done", "session_id": "abc-123", "is_error": false}'
    assert GenericCliAdapter._extract_session_id(payload) == "abc-123"
    assert GenericCliAdapter._extract_session_id("plain text output") is None
    assert GenericCliAdapter._extract_session_id('[{"sessionId": "xyz"}]') == "xyz"
    assert GenericCliAdapter._extract_session_id("{broken json") is None
