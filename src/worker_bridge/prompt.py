"""Stable prompt construction from the typed task contract."""

from __future__ import annotations

import json
import re
from pathlib import Path

from worker_bridge.models import TaskSpec


_CONTEXT_LIMIT = 1000
_MATCH_WORDS = re.compile(r"[a-z0-9][a-z0-9_-]{2,}")
_STOP_WORDS = {
    "add", "and", "any", "build", "change", "from", "into", "objective",
    "the", "this", "with",
}


def _repository_context(objective: str, workspace_path: str) -> str:
    """Return bounded repository guidance relevant to the worker objective.

    Surfaces the repo's own conventions (AGENTS.md / CLAUDE.md) and any
    objective-relevant SKILL.md descriptions, so the worker follows local
    conventions without the orchestrator hand-writing them.
    """
    root = Path(workspace_path)
    entries: list[str] = []

    for name in ("AGENTS.md", "CLAUDE.md"):
        path = root / name
        try:
            if path.is_file():
                entries.append(f"Coding conventions ({name}):\n{path.read_text(encoding='utf-8')[:500]}")
        except (OSError, UnicodeError):
            continue

    objective_words = set(_MATCH_WORDS.findall(objective.lower())) - _STOP_WORDS
    try:
        skill_paths = sorted(root.rglob("SKILL.md"))
    except OSError:
        skill_paths = []
    for path in skill_paths:
        try:
            text = path.read_text(encoding="utf-8")
        except (OSError, UnicodeError):
            continue
        match = re.search(r"^description:\s*[\"']?(.+?)[\"']?\s*$", text, re.MULTILINE | re.IGNORECASE)
        if not match:
            continue
        description = match.group(1)
        skill_words = set(_MATCH_WORDS.findall(f"{path.parent.name} {description}".lower())) - _STOP_WORDS
        if objective_words & skill_words:
            entries.append(f"Matching skill ({path.parent.name}): {description}")

    if not entries:
        return ""
    return ("CONTEXT:\n" + "\n\n".join(entries))[:_CONTEXT_LIMIT]


_ROLE_INSTRUCTIONS = {
    "implementer": "Implement the requested change and keep the diff narrowly scoped.",
    "junior_developer": "Work as a supervised junior developer: implement the scoped task, show evidence, and escalate uncertainty instead of broadening scope.",
    "junior-dev": "Work as a supervised junior developer: implement the scoped task, show evidence, and escalate uncertainty instead of broadening scope.",
    "reviewer": "Review independently. Report concrete findings with paths and evidence; do not edit unless asked.",
    "test_writer": "Add tests that express behavior contracts, then run them.",
    "researcher": "Inspect and report evidence. Do not modify the workspace unless explicitly required.",
    "debugger": "Reproduce, minimize, diagnose, then fix and add a regression test.",
    "integrator": "Integrate only accepted inputs, surface conflicts, and verify the combined result.",
    "synthesizer": "Compare upstream evidence and produce a concise normalized conclusion.",
}


def build_worker_prompt(task: TaskSpec, workspace_path: str) -> str:
    repository_context = _repository_context(task.objective, workspace_path)
    objective = f"{repository_context}\n\n{task.objective}" if repository_context else task.objective
    context = json.dumps(task.context, indent=2, sort_keys=True)
    permissions = json.dumps(task.permissions.__dict__ if hasattr(task.permissions, "__dict__") else {
        name: getattr(task.permissions, name)
        for name in task.permissions.__slots__
    }, indent=2, sort_keys=True)
    criteria = "\n".join(f"- {item}" for item in task.acceptance_criteria) or "- Complete the objective and provide evidence."
    constraints = "\n".join(f"- {item}" for item in task.constraints) or "- Do not modify unrelated files."
    verification = "\n".join(f"- Run: {item}" for item in task.verification.commands) or "- Inspect the final diff and run the repository's relevant checks."
    role_text = _ROLE_INSTRUCTIONS.get(task.role, _ROLE_INSTRUCTIONS["implementer"])
    return f"""You are an execution sub-agent working under an orchestrator.

The orchestrator owns task state, permissions, acceptance, integration, and user communication. {role_text}

Objective:
{objective}

Assigned workspace (operate only here):
{workspace_path}

Context:
{context}

Current permissions:
{permissions}

Constraints:
{constraints}

Acceptance criteria:
{criteria}

Verification requirements:
{verification}

Complete the objective directly. Inspect the repository before making assumptions. Follow existing project conventions. Make the smallest coherent changes that fully satisfy the acceptance criteria. Do not commit, merge, push, deploy, purchase, message external people, or modify orchestrator task state unless the objective explicitly and safely requires it.

Use only the authority granted above. Never read or print secrets. Do not claim to have run a command, test, build, or check unless it actually ran. Do not conceal failures. If additional authority is required, stop and return a JSON object under `permission_request` containing: requested_capability, requested_scope, reason, proposed_duration, proposed_command, risk_summary, and alternatives_considered.

If one genuinely unknowable product decision blocks progress, stop and return a JSON object under `clarification_request` containing `question`, `context`, and `options`. Do not use this for routine implementation choices.

At completion report: summary, changed files, commands executed, tests performed, evidence for every acceptance criterion, remaining risks, unresolved issues, and whether you are blocked. The orchestrator will independently inspect the diff and rerun verification.

EXECUTION DIRECTIVE: The Objective section above is your task assignment, not a role-setup preamble. Begin executing it now in the assigned workspace. Do not reply that you are ready, ask the orchestrator to provide a task, or stop after restating the instructions.
"""
