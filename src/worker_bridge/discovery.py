"""Discovery of coding workers, AI-first environments, and editor assistants."""

from __future__ import annotations

import os
import shutil
import sqlite3
import subprocess
from pathlib import Path
from typing import Any

from worker_bridge.environ import subprocess_env as hermes_subprocess_env


_AI_EXTENSION_MARKERS = {
    "anthropic.claude-code": ("Claude Code", "claude-code"),
    "sst-dev.opencode": ("OpenCode", "opencode"),
    "github.copilot": ("GitHub Copilot", None),
    "github.copilot-chat": ("GitHub Copilot Chat", None),
    "continue.continue": ("Continue", None),
    "saoudrizwan.claude-dev": ("Cline", None),
    "rooveterinaryinc.roo-cline": ("Roo Code", None),
    "sourcegraph.cody-ai": ("Cody", None),
    "tabnine.tabnine-vscode": ("Tabnine", None),
    "codeium.codeium": ("Codeium", None),
    "windsurf.windsurf": ("Windsurf", None),
    "supermaven.supermaven": ("Supermaven", None),
    "apertia.vscode-aider": ("Aider", "aider"),
    "mattflower.aider": ("Aider", "aider"),
    "openai.chatgpt": ("OpenAI Codex", "codex"),
    "google.geminicodeassist": ("Gemini Code Assist", "gemini-cli"),
    "amazonwebservices.amazon-q-vscode": ("Amazon Q Developer", None),
}


def _version(command: str) -> str | None:
    try:
        proc = subprocess.run(
            [command, "--version"],
            text=True,
            capture_output=True,
            timeout=8,
            stdin=subprocess.DEVNULL,
            env=hermes_subprocess_env(inherit_credentials=False),
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if proc.returncode:
        return None
    return (proc.stdout or proc.stderr).splitlines()[0].strip() or None


def _environment_candidates() -> list[dict[str, Any]]:
    candidates = [
        ("visual-studio-code", "Visual Studio Code", "code", None),
        ("cursor", "Cursor", "cursor", None),
        ("windsurf", "Windsurf", "windsurf", None),
        ("void", "Void", "void", None),
        ("trae", "Trae", "trae", None),
        ("zed", "Zed", "zed", None),
    ]
    if os.name == "nt":
        local = Path(os.environ.get("LOCALAPPDATA", ""))
        candidates.append(
            ("zcode", "ZCode", "", local / "Programs" / "ZCode" / "ZCode.exe")
        )
    records = []
    for identifier, name, command, fallback in candidates:
        path = shutil.which(command) if command else None
        if not path and fallback and fallback.exists():
            path = str(fallback)
        if not path:
            continue
        # ZCode's GUI executable launches the application for --version, so do
        # not probe it. Presence and its session store are enough for discovery;
        # linking still requires a documented CLI/ACP/MCP task interface.
        version = _version(path) if identifier != "zcode" else None
        evidence = [f"executable: {path}"]
        if identifier == "zcode":
            session_root = Path.home() / ".zcode" / "cli" / "agents"
            if session_root.exists():
                evidence.append(f"agent session store: {session_root}")
        records.append(
            {
                "id": identifier,
                "name": name,
                "kind": "environment",
                "installed": True,
                "path": path,
                "version": version,
                "host": None,
                "worker_ready": False,
                "worker": None,
                "link_method": "discovery_only",
                "link_options": ["configured_cli", "acp", "mcp"],
                "evidence": evidence,
                "reason": "No verified unattended task/result interface is registered for the environment itself.",
            }
        )
    return records


def _extensions_for_editor(editor: dict[str, Any], worker_status: dict[str, dict]) -> list[dict[str, Any]]:
    if editor["id"] != "visual-studio-code":
        return []
    try:
        proc = subprocess.run(
            [editor["path"], "--list-extensions", "--show-versions"],
            text=True,
            capture_output=True,
            timeout=15,
            stdin=subprocess.DEVNULL,
            env=hermes_subprocess_env(inherit_credentials=False),
        )
    except (OSError, subprocess.TimeoutExpired):
        return []
    records = []
    for raw in proc.stdout.splitlines():
        extension, _, version = raw.strip().partition("@")
        key = extension.lower()
        marker = _AI_EXTENSION_MARKERS.get(key)
        if marker is None and not any(
            token in key
            for token in ("copilot", "codex", "claude", "cline", "continue", "aider", "opencode", "gemini", "cody", "tabnine", "codeium", "supermaven")
        ):
            continue
        name, worker = marker or (extension, None)
        status = worker_status.get(worker or "", {})
        ready = bool(status.get("availability", {}).get("installed")) if worker else False
        records.append(
            {
                "id": extension,
                "name": name,
                "kind": "extension",
                "installed": True,
                "path": None,
                "version": version or None,
                "host": editor["id"],
                "worker_ready": ready,
                "worker": worker if ready else None,
                "link_method": "backing_cli" if ready else "discovery_only",
                "link_options": ["configured_cli", "acp", "mcp"],
                "evidence": [f"{editor['id']} extension: {raw.strip()}"],
                "reason": None if ready else "Extension found, but no verified unattended worker adapter is linked.",
            }
        )
    return records


async def discover_ecosystem(registry) -> list[dict[str, Any]]:
    worker_items = await registry.list()
    worker_status = {item["worker"]: item for item in worker_items}
    records = []
    for item in worker_items:
        records.append(
            {
                "id": item["worker"],
                "name": item["worker"],
                "kind": "assistant",
                "installed": item["availability"]["installed"],
                "path": item["availability"]["executable"],
                "version": item["availability"]["version"],
                "host": None,
                "worker_ready": item["availability"]["installed"],
                "worker": item["worker"] if item["availability"]["installed"] else None,
                "link_method": "native_adapter" if item["availability"]["installed"] else "unavailable",
                "link_options": [],
                "evidence": [item["availability"].get("reason") or "registered worker adapter"],
                "reason": item["availability"].get("reason"),
            }
        )
    environments = _environment_candidates()
    records.extend(environments)
    for editor in environments:
        records.extend(_extensions_for_editor(editor, worker_status))
    return sorted(records, key=lambda item: (item["kind"], item["name"].lower(), item["id"]))


def discover_sessions(worker: str | None = None, limit: int = 20) -> list[dict[str, Any]]:
    """Discover recent native session ids without reading prompt content."""
    records: list[dict[str, Any]] = []
    wanted = (worker or "").lower()
    if wanted in {"", "claude", "claude-code"}:
        root = Path.home() / ".claude" / "projects"
        if root.exists():
            files = sorted(root.glob("*/*.jsonl"), key=lambda path: path.stat().st_mtime, reverse=True)
            for path in files[:limit]:
                if path.parent.name == "subagents" or path.stem.startswith("agent-"):
                    continue
                records.append({
                    "worker": "claude-code",
                    "session_id": path.stem,
                    "workspace": None,
                    "workspace_hint": path.parent.name,
                    "updated_at": path.stat().st_mtime,
                    "attach_supported": True,
                    "source": "native session index",
                })
    if wanted in {"", "opencode"}:
        database = Path.home() / ".local" / "share" / "opencode" / "opencode.db"
        if database.exists():
            try:
                conn = sqlite3.connect(f"file:{database.as_posix()}?mode=ro", uri=True, timeout=2)
                rows = conn.execute(
                    "SELECT id, directory, time_updated FROM session "
                    "WHERE time_archived IS NULL ORDER BY time_updated DESC LIMIT ?",
                    (limit,),
                ).fetchall()
                conn.close()
                for session_id, directory, updated in rows:
                    normalized_updated = (float(updated) / 1000.0) if float(updated or 0) > 10_000_000_000 else float(updated or 0)
                    records.append({
                        "worker": "opencode",
                        "session_id": session_id,
                        "workspace": directory,
                        "workspace_hint": None,
                        "updated_at": normalized_updated,
                        "attach_supported": True,
                        "source": "read-only native SQLite index",
                    })
            except sqlite3.Error:
                pass
    if wanted in {"", "zcode"}:
        root = Path.home() / ".zcode" / "cli" / "agents"
        if root.exists():
            for path in sorted(root.glob("sess_*"), key=lambda item: item.stat().st_mtime, reverse=True)[:limit]:
                records.append({
                    "worker": "zcode",
                    "session_id": path.name,
                    "workspace": None,
                    "workspace_hint": None,
                    "updated_at": path.stat().st_mtime,
                    "attach_supported": False,
                    "source": "native session store; no verified task adapter",
                })
    return sorted(records, key=lambda item: item["updated_at"], reverse=True)[:limit]
