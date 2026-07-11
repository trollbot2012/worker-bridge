"""Tests for the ZCode GLM worker adapter.

Pins the four contracts the adapter must hold:

  * the subprocess env overlay sets ANTHROPIC_BASE_URL / _MODEL / _AUTH_TOKEN
  * the concrete auth token never leaks into a parsed WorkerResult
  * the Claude Code print/JSON parser is inherited unchanged
  * the adapter registers in a WorkerRegistry and round-trips its config
"""
from __future__ import annotations

import json
from dataclasses import asdict

from worker_bridge.adapters.clients import ClaudeCodeAdapter
from worker_bridge.adapters.mock import MockWorkerAdapter
from worker_bridge.adapters.zcode import ZCodeGlmAdapter
from worker_bridge.registry import WorkerRegistry


_SENTINEL_TOKEN = "zcode-secret-token-abc123"


def test_env_overlay_sets_the_three_anthropic_vars(monkeypatch):
    monkeypatch.setenv("ZCODE_AUTH_TOKEN", _SENTINEL_TOKEN)
    adapter = ZCodeGlmAdapter()
    env = adapter._subprocess_env()
    assert env["ANTHROPIC_BASE_URL"] == "https://api.z.ai/api/anthropic"
    assert env["ANTHROPIC_MODEL"] == "GLM-5.2"
    assert env["ANTHROPIC_AUTH_TOKEN"] == _SENTINEL_TOKEN


def test_env_overlay_reads_token_by_name_not_value(monkeypatch):
    # The constructor is given the NAME of the env var; the value is resolved
    # from the process environment at spawn time.
    monkeypatch.setenv("CUSTOM_ZCODE_KEY", _SENTINEL_TOKEN)
    adapter = ZCodeGlmAdapter(auth_token_env="CUSTOM_ZCODE_KEY")
    env = adapter._subprocess_env()
    assert env["ANTHROPIC_AUTH_TOKEN"] == _SENTINEL_TOKEN


def test_env_overlay_omits_token_when_var_unset(monkeypatch):
    monkeypatch.delenv("ZCODE_AUTH_TOKEN", raising=False)
    # When the named var is unset the adapter must not synthesize a token; it
    # leaves ANTHROPIC_AUTH_TOKEN exactly as the inherited base env had it
    # (comparing to the base makes this robust to a credential the runner may
    # already export, rather than asserting bare absence).
    base = ClaudeCodeAdapter()._subprocess_env()
    env = ZCodeGlmAdapter()._subprocess_env()
    assert env.get("ANTHROPIC_AUTH_TOKEN") == base.get("ANTHROPIC_AUTH_TOKEN")
    assert env["ANTHROPIC_BASE_URL"] == "https://api.z.ai/api/anthropic"
    assert env["ANTHROPIC_MODEL"] == "GLM-5.2"


def test_token_not_logged_in_results(monkeypatch):
    # Even with the secret live in the environment, nothing the adapter returns
    # as a WorkerResult may carry the concrete token.
    monkeypatch.setenv("ZCODE_AUTH_TOKEN", _SENTINEL_TOKEN)
    adapter = ZCodeGlmAdapter()
    assert adapter._subprocess_env()["ANTHROPIC_AUTH_TOKEN"] == _SENTINEL_TOKEN

    result = adapter._parse(
        json.dumps({"result": "done", "session_id": "s-1", "is_error": False}), "", 0, None
    )
    assert _SENTINEL_TOKEN not in json.dumps(asdict(result), default=str)


def test_inherited_parser_matches_claude_code(monkeypatch):
    monkeypatch.setenv("ZCODE_AUTH_TOKEN", _SENTINEL_TOKEN)
    payload = json.dumps({"result": "done", "session_id": "zcode-session", "is_error": False})
    zcode = ZCodeGlmAdapter()._parse(payload, "", 0, None)
    claude = ClaudeCodeAdapter()._parse(payload, "", 0, None)

    assert zcode.status == "succeeded"
    assert zcode.session_id == "zcode-session"
    assert zcode.metadata["transport"] == "claude_print_json"
    # Identical transport => identical structured parse.
    assert asdict(zcode) == asdict(claude)


def test_inherited_parser_fails_closed_on_malformed_output():
    assert ZCodeGlmAdapter()._parse("not-json", "", 0, "fallback").status == "failed"


def test_config_registration_round_trips():
    adapter = ZCodeGlmAdapter()
    registry = WorkerRegistry([adapter])
    resolved = registry.get("zcode-glm")
    assert resolved is adapter
    assert resolved.name == "zcode-glm"
    assert resolved.base_url == "https://api.z.ai/api/anthropic"
    assert resolved.model == "GLM-5.2"
    assert resolved.auth_token_env == "ZCODE_AUTH_TOKEN"


def test_config_registration_via_register_accepts_custom_config():
    registry = WorkerRegistry([MockWorkerAdapter()])
    registry.register(
        ZCodeGlmAdapter(
            base_url="https://api.z.ai/api/anthropic",
            model="GLM-5.2",
            auth_token_env="ZCODE_AUTH_TOKEN",
        )
    )
    assert registry.get("zcode-glm").model == "GLM-5.2"


def test_default_registry_wires_zcode_glm():
    # The default worker set must resolve "zcode-glm" so worker_delegate's enum
    # value is functional end-to-end, not just accepted by input validation.
    resolved = WorkerRegistry().get("zcode-glm")
    assert isinstance(resolved, ZCodeGlmAdapter)
    assert resolved.base_url == "https://api.z.ai/api/anthropic"
    assert resolved.model == "GLM-5.2"
    assert resolved.auth_token_env == "ZCODE_AUTH_TOKEN"
