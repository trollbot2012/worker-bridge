"""Claude Code CLI pointed at an alternate Anthropic-compatible endpoint.

Drives the same ``claude`` CLI transport as :class:`ClaudeCodeAdapter` but
redirects it at an Anthropic-compatible API (default: Z.ai's GLM) so a task runs
against a different model. Only the subprocess environment differs: the
print/JSON transport, session resume, permission mapping, and result parser are
all inherited unchanged. Use it as a template for any Anthropic-compatible
endpoint by subclassing or constructing with different ``base_url``/``model``.

The auth token is supplied by the *name* of an environment variable, never by
value. The concrete secret is read from that variable at spawn time inside
``_subprocess_env`` and handed straight to the child process env, so it is never
stored on the adapter instance, persisted in the task spec, or echoed into any
``WorkerResult`` the parser produces.
"""

from __future__ import annotations

import os

from worker_bridge.adapters.clients import ClaudeCodeAdapter


class ZCodeGlmAdapter(ClaudeCodeAdapter):
    """Claude Code CLI pointed at Z.ai's GLM (Anthropic-compatible) API."""

    name = "zcode-glm"

    def __init__(
        self,
        base_url: str = "https://api.z.ai/api/anthropic",
        model: str = "GLM-5.2",
        auth_token_env: str = "ZCODE_AUTH_TOKEN",
        executable: str | None = None,
    ) -> None:
        super().__init__(executable=executable)
        self.base_url = base_url
        self.model = model
        self.auth_token_env = auth_token_env

    def _subprocess_env(self) -> dict[str, str]:
        env = super()._subprocess_env()
        env["ANTHROPIC_BASE_URL"] = self.base_url
        env["ANTHROPIC_MODEL"] = self.model
        token = os.environ.get(self.auth_token_env)
        if token:
            env["ANTHROPIC_AUTH_TOKEN"] = token
        return env
