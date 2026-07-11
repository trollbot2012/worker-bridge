from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from worker_bridge.adapters.generic_cli import GenericCliAdapter
from worker_bridge.adapters.clients import ClaudeCodeAdapter, OpenCodeAdapter
from worker_bridge.adapters.mock import MockWorkerAdapter
from worker_bridge.cli import register_cli
from worker_bridge.models import TaskSpec
from worker_bridge.orchestrator import OrchestrationError, WorkerBridge
from worker_bridge.redaction import redact_text
from worker_bridge.registry import WorkerRegistry
from worker_bridge.store import WorkerStore
from worker_bridge.workspace import WorkspaceManager
from worker_bridge.discovery import _extensions_for_editor, discover_sessions


def run(*args: str, cwd: Path) -> str:
    proc = subprocess.run(args, cwd=cwd, text=True, capture_output=True, check=True)
    return proc.stdout.strip()


@pytest.fixture
def repository(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    repo.mkdir()
    run("git", "init", cwd=repo)
    run("git", "config", "user.email", "worker@example.test", cwd=repo)
    run("git", "config", "user.name", "Worker Test", cwd=repo)
    (repo / "README.md").write_text("base\n", encoding="utf-8")
    run("git", "add", ".", cwd=repo)
    run("git", "commit", "-m", "base", cwd=repo)
    return repo


@pytest.fixture
def bridge(tmp_path: Path) -> WorkerBridge:
    store = WorkerStore(tmp_path / "bridge.db")
    registry = WorkerRegistry([MockWorkerAdapter()])
    return WorkerBridge(
        store=store,
        registry=registry,
        workspaces=WorkspaceManager(tmp_path / "worktrees"),
    )


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


def test_task_contract_validates_repository(repository: Path):
    task = TaskSpec.from_dict(spec(repository))
    assert task.worker == "mock"
    with pytest.raises(ValueError, match="objective"):
        TaskSpec.from_dict({**spec(repository), "objective": ""})


def test_task_persistence_and_idempotency(bridge: WorkerBridge, repository: Path):
    first = bridge.create_task({**spec(repository), "idempotency_key": "stable"})
    second = bridge.create_task({**spec(repository), "idempotency_key": "stable"})
    assert first["task_id"] == second["task_id"]
    assert bridge.get_task(first["task_id"])["spec"]["objective"] == first["spec"]["objective"]


def test_job_persistence(bridge: WorkerBridge, repository: Path):
    job = bridge.create_job("parallel demo", [spec(repository), spec(repository)], strategy="parallel")
    restored = bridge.store.get_job(job["job_id"])
    assert restored and len(restored["tasks"]) == 2


def test_event_replay_and_secret_redaction(bridge: WorkerBridge, repository: Path):
    task = bridge.create_task(spec(repository))
    bridge.store.append_event("test", {"token": "token=super-secret-value"}, task_id=task["task_id"])
    events = bridge.store.events(task_id=task["task_id"], after=0)
    assert events[-1]["payload"]["token"] == "token=[REDACTED]"
    assert bridge.store.events(task_id=task["task_id"], after=events[-1]["event_id"]) == []


def test_redacts_common_key_shapes():
    text = redact_text("Authorization: Bearer abcdef password=hunter2 sk-test_123456789012345")
    assert "abcdef" not in text and "hunter2" not in text and "123456789" not in text


def test_redacts_sensitive_dictionary_values(bridge: WorkerBridge, repository: Path):
    task = bridge.create_task({
        **spec(repository),
        "metadata": {"api_key": "plain-secret", "token_usage": {"input": 12}},
    })
    assert task["spec"]["metadata"]["api_key"] == "[REDACTED]"
    assert task["spec"]["metadata"]["token_usage"] == {"input": 12}


def test_mock_worker_execution_and_changed_files(bridge: WorkerBridge, repository: Path):
    task = bridge.create_task(spec(repository))
    result = asyncio.run(bridge.start_task(task["task_id"]))
    assert result["status"] == "succeeded"
    assert result["result"]["changed_files"] == ["result.txt"]
    assert all(Path(path).exists() for path in result["result"]["artifacts"])


def test_native_session_continuation_contract(bridge: WorkerBridge, repository: Path):
    task = bridge.create_task({
        **spec(repository),
        "metadata": {
            "mock_write": "result.txt",
            "mock_follow_up_write": "result.txt",
        },
    })
    first = asyncio.run(bridge.start_task(task["task_id"]))
    session_id = first["result"]["session_id"]
    second = asyncio.run(bridge.continue_task(task["task_id"], "corrected"))
    assert second["result"]["session_id"] == session_id
    assert (Path(second["runtime"]["path"]) / "result.txt").read_text() == "corrected"


def test_attach_existing_native_session(tmp_path: Path, repository: Path):
    bridge = WorkerBridge(
        store=WorkerStore(tmp_path / "attach.db"),
        registry=WorkerRegistry([MockWorkerAdapter()]),
        workspaces=WorkspaceManager(tmp_path / "attach-worktrees"),
    )
    task = bridge.create_task({
        **spec(repository),
        "workspace": {
            "repository": str(repository.resolve()),
            "working_directory": str(repository.resolve()),
            "isolation": "direct",
        },
        "metadata": {},
    })
    attached = asyncio.run(bridge.attach_session(task["task_id"], "existing-session"))
    assert attached["result"]["metadata"]["externally_attached"] is True
    continued = asyncio.run(bridge.continue_task(task["task_id"], "take this next task"))
    assert continued["result"]["session_id"] == "existing-session"


def test_verification_success(bridge: WorkerBridge, repository: Path):
    task = bridge.create_task({
        **spec(repository),
        "verification": {"commands": [f'"{sys.executable}" -c "from pathlib import Path; assert Path(\'result.txt\').read_text() == \'done\\n\'"']},
    })
    result = asyncio.run(bridge.start_task(task["task_id"]))
    assert result["result"]["metadata"]["verification"]["ok"] is True


def test_verification_failure(bridge: WorkerBridge, repository: Path):
    task = bridge.create_task({**spec(repository), "verification": {"commands": [f'"{sys.executable}" -c "raise SystemExit(7)"']}})
    result = asyncio.run(bridge.start_task(task["task_id"]))
    assert result["status"] == "failed"
    assert result["result"]["error"] == "independent verification failed"


def test_forbidden_path_detection(bridge: WorkerBridge, repository: Path):
    task = bridge.create_task({**spec(repository), "verification": {"forbidden_paths": ["result.txt"]}})
    result = asyncio.run(bridge.start_task(task["task_id"]))
    assert result["status"] == "failed"
    assert result["result"]["metadata"]["verification"]["forbidden_files"] == ["result.txt"]


def test_read_only_permission_rejects_writes(bridge: WorkerBridge, repository: Path):
    task = bridge.create_task({**spec(repository), "permissions": {"profile": "read_only"}})
    result = asyncio.run(bridge.start_task(task["task_id"]))
    assert result["status"] == "failed"
    assert result["result"]["metadata"]["permission_violations"] == ["result.txt"]


def test_custom_permission_scope_is_enforced(bridge: WorkerBridge, repository: Path):
    task = bridge.create_task({
        **spec(repository),
        "permissions": {"profile": "custom", "paths": ["allowed"]},
        "metadata": {"mock_write": "outside.txt"},
    })
    result = asyncio.run(bridge.start_task(task["task_id"]))
    assert result["status"] == "failed"


def test_full_access_permission_allows_workspace_write(bridge: WorkerBridge, repository: Path):
    task = bridge.create_task({**spec(repository), "permissions": {"profile": "full_access"}})
    result = asyncio.run(bridge.start_task(task["task_id"]))
    assert result["status"] == "succeeded"


def test_accept_requires_independent_verification(bridge: WorkerBridge, repository: Path):
    task = bridge.create_task(spec(repository))
    with pytest.raises(OrchestrationError):
        bridge.accept_task(task["task_id"])
    completed = asyncio.run(bridge.start_task(task["task_id"]))
    accepted = bridge.accept_task(completed["task_id"])
    assert accepted["status"] == "accepted"


def test_permission_approval_denial_and_narrowing(bridge: WorkerBridge, repository: Path):
    task = bridge.create_task(spec(repository))
    first = bridge.create_permission_request({
        "task_id": task["task_id"], "worker": "mock", "requested_capability": "network",
        "requested_scope": ["example.com"], "reason": "fetch docs",
    })
    approved = bridge.decide_permission(first["request_id"], "approved_narrowed", scope=["docs.example.com"])
    assert approved["decision"]["scope"] == ["docs.example.com"]
    assert bridge.decide_permission(first["request_id"], "denied")["status"] == "approved_narrowed"
    second = bridge.create_permission_request({
        "task_id": task["task_id"], "worker": "mock", "requested_capability": "host_write",
        "requested_scope": ["/tmp"], "reason": "write",
    })
    assert bridge.decide_permission(second["request_id"], "denied")["status"] == "denied"


def test_worker_permission_request_is_persisted_and_applied(bridge: WorkerBridge, repository: Path):
    summary = json.dumps({"permission_request": {
        "requested_capability": "network", "requested_scope": ["docs.example.com"],
        "reason": "read upstream docs", "risk_summary": "outbound HTTPS",
    }})
    task = bridge.create_task({**spec(repository), "metadata": {"mock_summary": summary}})
    waiting = asyncio.run(bridge.start_task(task["task_id"]))
    assert waiting["status"] == "waiting_input"
    request_id = waiting["result"]["metadata"]["permission_request_id"]
    bridge.decide_permission(request_id, "approved_narrowed", scope=["docs.example.com"])
    updated = bridge.get_task(task["task_id"])
    assert updated["spec"]["permissions"]["domains"] == ["docs.example.com"]


def test_clarification_answer_resumes_session(bridge: WorkerBridge, repository: Path):
    summary = json.dumps({"clarification_request": {
        "question": "Which format?", "context": "two are valid", "options": ["json", "yaml"]
    }})
    task = bridge.create_task({**spec(repository), "metadata": {"mock_summary": summary}})
    waiting = asyncio.run(bridge.start_task(task["task_id"]))
    request_id = waiting["result"]["metadata"]["clarification_request_id"]
    completed = asyncio.run(bridge.answer_input_request(request_id, "json"))
    assert completed["status"] == "succeeded"
    assert bridge.store.get_input_request(request_id)["status"] == "answered"


def test_pause_resume_and_cancel(bridge: WorkerBridge, repository: Path):
    task = bridge.create_task(spec(repository))
    assert bridge.pause_task(task["task_id"])["status"] == "paused"
    assert bridge.resume_task(task["task_id"])["status"] == "created"
    assert asyncio.run(bridge.cancel_task(task["task_id"]))["status"] == "cancelled"


def test_recovery_marks_interrupted_task_paused(tmp_path: Path, repository: Path):
    store = WorkerStore(tmp_path / "state.db")
    task = store.create_task(TaskSpec.from_dict(spec(repository)).to_dict())
    store.update_task(task["task_id"], status="running", runtime={"pid": 999999})
    assert store.recover_running() == 1
    recovered = store.get_task(task["task_id"])
    assert recovered["status"] == "paused"
    assert recovered["runtime"]["recovery_required"] is True


def test_circuit_breaker_uses_persisted_failures(tmp_path: Path, repository: Path):
    store = WorkerStore(tmp_path / "circuit.db")
    bridge = WorkerBridge(
        store=store,
        registry=WorkerRegistry([MockWorkerAdapter(fail=True)]),
        workspaces=WorkspaceManager(tmp_path / "circuit-worktrees"),
    )
    for _ in range(3):
        task = bridge.create_task({**spec(repository), "metadata": {"mock_fail": True}})
        asyncio.run(bridge.start_task(task["task_id"]))
    restarted = WorkerBridge(
        store=WorkerStore(store.path),
        registry=WorkerRegistry([MockWorkerAdapter()]),
        workspaces=WorkspaceManager(tmp_path / "circuit-worktrees-2"),
    )
    task = restarted.create_task(spec(repository))
    with pytest.raises(OrchestrationError, match="circuit"):
        asyncio.run(restarted.start_task(task["task_id"]))


def test_task_timeout_is_persisted(tmp_path: Path, repository: Path):
    bridge = WorkerBridge(
        store=WorkerStore(tmp_path / "timeout.db"),
        registry=WorkerRegistry([MockWorkerAdapter(delay=1.1)]),
        workspaces=WorkspaceManager(tmp_path / "timeout-worktrees"),
    )
    task = bridge.create_task({
        **spec(repository),
        "limits": {"timeout_seconds": 1, "maximum_output_bytes": 1024},
    })
    result = asyncio.run(bridge.start_task(task["task_id"]))
    assert result["status"] == "timed_out"


def test_worker_output_size_limit(bridge: WorkerBridge, repository: Path):
    task = bridge.create_task({
        **spec(repository),
        "metadata": {"mock_summary": "x" * 5000},
        "limits": {"maximum_output_bytes": 1024},
    })
    result = asyncio.run(bridge.start_task(task["task_id"]))
    assert result["result"]["metadata"]["output_truncated"] is True
    assert len(result["result"]["summary"]) < 1200


def test_parallel_tasks_get_independent_worktrees(bridge: WorkerBridge, repository: Path):
    job = bridge.create_job("parallel", [spec(repository), spec(repository)], strategy="parallel")
    result = asyncio.run(bridge.run_job(job["job_id"]))
    tasks = [bridge.get_task(task_id) for task_id in result["tasks"]]
    assert result["status"] == "succeeded"
    assert len({task["runtime"]["path"] for task in tasks}) == 2


def test_per_worker_concurrency_limit(tmp_path: Path, repository: Path):
    adapter = MockWorkerAdapter(delay=0.15, maximum_concurrency=1)
    bridge = WorkerBridge(
        store=WorkerStore(tmp_path / "limits.db"),
        registry=WorkerRegistry([adapter]),
        workspaces=WorkspaceManager(tmp_path / "limit-worktrees"),
        per_job_concurrency=3,
    )
    job = bridge.create_job("limited", [spec(repository), spec(repository), spec(repository)], strategy="parallel")
    asyncio.run(bridge.run_job(job["job_id"]))
    assert adapter.max_observed == 1


def test_partial_worker_failure_does_not_cancel_sibling(bridge: WorkerBridge, repository: Path):
    failing = {**spec(repository), "metadata": {"mock_fail": True}}
    job = bridge.create_job("partial", [failing, spec(repository)], strategy="parallel")
    result = asyncio.run(bridge.run_job(job["job_id"]))
    statuses = {bridge.get_task(task_id)["status"] for task_id in result["tasks"]}
    assert statuses == {"failed", "succeeded"}
    assert result["status"] == "partial"


def test_competing_results_are_compared_without_merge(bridge: WorkerBridge, repository: Path):
    job = bridge.create_job("compete", [spec(repository), spec(repository)], strategy="competing")
    asyncio.run(bridge.run_job(job["job_id"]))
    comparison = bridge.compare_results(job["job_id"])
    assert comparison["auto_merged"] is False
    assert len(comparison["candidates"]) == 2


def test_explicit_integration_applies_selected_verified_result(bridge: WorkerBridge, repository: Path):
    job = bridge.create_job("integrate", [spec(repository)], strategy="competing")
    asyncio.run(bridge.run_job(job["job_id"]))
    integrated = bridge.integrate_results(job["job_id"], [job["tasks"][0]])
    assert integrated["status"] == "succeeded"
    assert (Path(integrated["runtime"]["path"]) / "result.txt").read_text() == "done\n"


def test_explicit_integration_reports_conflict(bridge: WorkerBridge, repository: Path):
    left = {**spec(repository), "metadata": {"mock_write": "same.txt", "mock_content": "left\n"}}
    right = {**spec(repository), "metadata": {"mock_write": "same.txt", "mock_content": "right\n"}}
    job = bridge.create_job("conflict", [left, right], strategy="competing")
    asyncio.run(bridge.run_job(job["job_id"]))
    integrated = bridge.integrate_results(job["job_id"], job["tasks"])
    assert integrated["status"] == "failed"
    assert integrated["result"]["metadata"]["applications"][-1]["ok"] is False


def test_implement_review_passes_diff_to_reviewer_and_returns_to_implementer(bridge: WorkerBridge, repository: Path):
    implementer = spec(repository)
    reviewer = {
        **spec(repository),
        "role": "reviewer",
        "permissions": {"profile": "read_only"},
        "metadata": {"mock_summary": "Finding: add coverage."},
    }
    job = bridge.create_job("review", [implementer, reviewer], strategy="implement_review")
    asyncio.run(bridge.run_job(job["job_id"]))
    reviewer_task = bridge.get_task(job["tasks"][1])
    implementer_task = bridge.get_task(job["tasks"][0])
    assert reviewer_task["spec"]["context"]["upstream_results"][0]["diff"]
    assert implementer_task["runtime"]["follow_up_turns"] == 1


def test_map_reduce_synthesizer_receives_upstream_results(bridge: WorkerBridge, repository: Path):
    mapper_a = {**spec(repository), "role": "researcher", "metadata": {"mock_summary": "A"}}
    mapper_b = {**spec(repository), "role": "researcher", "metadata": {"mock_summary": "B"}}
    reducer = {**spec(repository), "role": "synthesizer", "metadata": {"mock_summary": "A+B"}}
    job = bridge.create_job("map reduce", [mapper_a, mapper_b, reducer], strategy="map_reduce")
    asyncio.run(bridge.run_job(job["job_id"]))
    reducer_task = bridge.get_task(job["tasks"][2])
    upstream = reducer_task["spec"]["context"]["upstream_results"]
    assert [item["summary"] for item in upstream] == ["A", "B"]


def test_failed_worker_handoff_preserves_workspace(bridge: WorkerBridge, repository: Path):
    failed = bridge.create_task({**spec(repository), "metadata": {"mock_fail": True, "mock_write": "partial.txt"}})
    failed = asyncio.run(bridge.start_task(failed["task_id"]))
    replacement = bridge.replace_worker(failed["task_id"], "mock")
    assert replacement["spec"]["parent_task_id"] == failed["task_id"]
    assert replacement["spec"]["workspace"]["working_directory"] == failed["runtime"]["path"]


def test_failed_workspace_retention_can_be_disabled(tmp_path: Path, repository: Path):
    bridge = WorkerBridge(
        store=WorkerStore(tmp_path / "retention.db"),
        registry=WorkerRegistry([MockWorkerAdapter(fail=True)]),
        workspaces=WorkspaceManager(tmp_path / "retention-worktrees"),
        retention={"retain_failed_tasks": False},
    )
    task = bridge.create_task({**spec(repository), "metadata": {"mock_fail": True}})
    failed = asyncio.run(bridge.start_task(task["task_id"]))
    assert failed["runtime"]["retained"] is False
    assert not Path(failed["runtime"]["path"]).exists()


def test_worker_registry_discovery():
    registry = WorkerRegistry([MockWorkerAdapter()])
    result = asyncio.run(registry.list())
    assert result[0]["availability"]["installed"] is True
    assert result[0]["capabilities"]["sessions"] is True


def test_generic_cli_adapter(repository: Path, tmp_path: Path):
    adapter = GenericCliAdapter(
        "python-worker",
        [sys.executable, "-c", "print('structured worker result')"],
    )
    registry = WorkerRegistry([adapter])
    bridge = WorkerBridge(
        store=WorkerStore(tmp_path / "cli.db"),
        registry=registry,
        workspaces=WorkspaceManager(tmp_path / "cli-worktrees"),
    )
    task = bridge.create_task({**spec(repository), "worker": "python-worker", "metadata": {}})
    result = asyncio.run(bridge.start_task(task["task_id"]))
    assert result["status"] == "succeeded"
    assert "structured worker result" in result["result"]["summary"]


def test_supported_cli_parsers_capture_sessions():
    claude = ClaudeCodeAdapter()
    claude_result = claude._parse(
        json.dumps({"result": "done", "session_id": "claude-session", "is_error": False}), "", 0, None
    )
    assert claude_result.session_id == "claude-session" and claude_result.status == "succeeded"
    opencode = OpenCodeAdapter()
    opencode_result = opencode._parse(
        '{"type":"step_start","sessionID":"ses-1"}\n{"type":"text","text":"done"}\n', "", 0, None
    )
    assert opencode_result.session_id == "ses-1" and opencode_result.summary == "done"


def test_structured_clients_fail_closed_on_malformed_output():
    assert ClaudeCodeAdapter()._parse("not-json", "", 0, "fallback").status == "failed"
    assert OpenCodeAdapter()._parse("not-json", "", 0, "fallback").status == "failed"


def test_configured_generic_worker_registration():
    registry = WorkerRegistry.from_config({"worker_bridge": {"workers": {
        "custom": {"command": [sys.executable, "-c", "print('x')"], "maximum_concurrency": 2}
    }}})
    assert registry.get("custom").name == "custom"


def test_ai_extension_discovery_links_backing_worker(monkeypatch):
    class Result:
        stdout = "anthropic.claude-code@2.1.206\ncontinue.continue@2.0.0\n"
        stderr = ""
        returncode = 0

    monkeypatch.setattr("worker_bridge.discovery.subprocess.run", lambda *a, **kw: Result())
    editor = {"id": "visual-studio-code", "path": "code"}
    status = {"claude-code": {"availability": {"installed": True}}}
    records = _extensions_for_editor(editor, status)
    by_id = {item["id"]: item for item in records}
    assert by_id["anthropic.claude-code"]["worker_ready"] is True
    assert by_id["anthropic.claude-code"]["worker"] == "claude-code"
    assert by_id["continue.continue"]["worker_ready"] is False


def test_cli_parses_ecosystem_discovery_and_link():
    parser = argparse.ArgumentParser()
    register_cli(parser)
    discover = parser.parse_args(["workers", "discover", "--kind", "extensions"])
    assert discover.worker_action == "discover" and discover.kind == "extensions"
    link = parser.parse_args([
        "workers", "link", "zcode", "--name", "junior-zcode",
        "--command-json", '["zcode-cli","run","{prompt}"]',
    ])
    assert link.discovery_id == "zcode" and link.name == "junior-zcode"


def test_session_discovery_does_not_read_prompt_content(tmp_path: Path, monkeypatch):
    home = tmp_path
    session_dir = home / ".claude" / "projects" / "project-hint"
    session_dir.mkdir(parents=True)
    (session_dir / "00000000-0000-0000-0000-000000000001.jsonl").write_text(
        '{"private_prompt":"must not be returned"}\n', encoding="utf-8"
    )
    monkeypatch.setattr(Path, "home", lambda: home)
    sessions = discover_sessions("claude-code")
    assert sessions[0]["session_id"] == "00000000-0000-0000-0000-000000000001"
    assert "private_prompt" not in json.dumps(sessions)


def test_cli_surface_parses_required_operations():
    parser = argparse.ArgumentParser()
    register_cli(parser)
    args = parser.parse_args(["tasks", "create", "--objective", "x", "--repo", str(Path.cwd())])
    assert args.worker_area == "tasks" and args.worker_action == "create"
    args = parser.parse_args(["results", "compare", "job-1"])
    assert args.worker_area == "results" and args.worker_action == "compare"
