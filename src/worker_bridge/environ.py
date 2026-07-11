"""Host-neutral environment seams.

These are the only things the engine needed from its original host (Hermes):
a home directory for state, a sanitized subprocess environment, and a small
config store. Providing them here makes the bridge runnable standalone or
embedded in any agent.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

# Substrings that mark an environment variable as credential-bearing. When a
# child process does not need credentials (e.g. a `--version` probe), these are
# stripped so a version check never leaks secrets into logs or argv.
_SENSITIVE_ENV_SUBSTRINGS = (
    "SECRET", "TOKEN", "PASSWORD", "PASSWD", "APIKEY", "API_KEY",
    "ACCESS_KEY", "PRIVATE_KEY", "CREDENTIAL", "SESSION_KEY",
)


def get_home() -> Path:
    """Root directory for bridge state (DB, worktrees, artifacts, config).

    Override with ``WORKER_BRIDGE_HOME``; defaults to ``~/.worker-bridge``.
    """
    root = os.environ.get("WORKER_BRIDGE_HOME")
    home = Path(root).expanduser() if root else Path.home() / ".worker-bridge"
    home.mkdir(parents=True, exist_ok=True)
    return home


def subprocess_env(*, inherit_credentials: bool = False) -> dict[str, str]:
    """A copy of the current environment for a worker/probe subprocess.

    ``inherit_credentials=True`` passes everything through — real workers need
    their provider API keys. ``False`` strips credential-bearing variables, used
    for capability probes that must not carry secrets.
    """
    env = dict(os.environ)
    if not inherit_credentials:
        for key in list(env):
            upper = key.upper()
            if any(marker in upper for marker in _SENSITIVE_ENV_SUBSTRINGS):
                env.pop(key, None)
    return env


# --- minimal config store (optional PyYAML) ---------------------------------

def config_path() -> Path:
    return get_home() / "config.yaml"


def read_raw_config() -> dict[str, Any]:
    path = config_path()
    if not path.exists():
        return {}
    try:
        import yaml  # optional dependency
    except ImportError:
        return {}
    try:
        return yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception:
        return {}


def load_config() -> dict[str, Any]:
    return read_raw_config()


def save_config(data: dict[str, Any]) -> None:
    try:
        import yaml
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("saving config requires PyYAML (pip install worker-bridge-mcp[yaml])") from exc
    config_path().write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")
