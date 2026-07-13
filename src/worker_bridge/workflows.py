"""Workflow-typed dispatch: per-type defaults applied at task creation.

A task's *type* (chore/feature/hotfix/refactor) shapes how much machinery it
gets: queue priority, time budget, follow-up allowance, automatic verification
repair attempts, and a preferred worker tier. The profile only fills fields the
caller left unset — an explicit ``--worker``/``--timeout``/``--priority`` (or a
key already present in a ``--spec`` JSON contract) always wins, so typing a
task can never silently override an operator's decision.

Downstream consumers read ``spec.metadata.type``: schedulers can order by the
type-shaped ``priority``, the orchestrator reads ``metadata.auto_repair`` for
the verification-repair loop, and review tooling can pick its depth from the
type tag.
"""

from __future__ import annotations

from typing import Any, Iterable

# Worker preferences are ordered cheapest-adequate-first and resolved against
# the registered workers at create time; an unknown name is skipped rather
# than failing task creation on a machine without that worker. "haiku" matches
# only if a config-defined worker of that name exists (e.g. a claude CLI
# worker pinned to a small model).
TASK_TYPE_PROFILES: dict[str, dict[str, Any]] = {
    "chore": {
        # Mechanical, low-blast-radius work (typo fixes, single-file tweaks).
        # Cheap model tier, short leash, one shot at self-repair.
        "priority": 30,
        "timeout_seconds": 900,
        "maximum_follow_up_turns": 4,
        "auto_repair": 1,
        "workers": ("haiku", "codex", "claude-code"),
    },
    "feature": {
        # The default pipeline shape; the tag mostly informs review depth.
        "priority": 50,
        "auto_repair": 2,
        "workers": (),
    },
    "hotfix": {
        # Production-down surge work: jump the queue, keep the loop tight so a
        # human sees a result (good or bad) quickly instead of a long retry
        # spiral.
        "priority": 90,
        "timeout_seconds": 1800,
        "auto_repair": 1,
        "workers": (),
    },
    "refactor": {
        # Wide but reversible; worth more self-repair before a human looks.
        "priority": 60,
        "auto_repair": 2,
        "workers": (),
    },
}

TASK_TYPES = tuple(sorted(TASK_TYPE_PROFILES))


def apply_task_type(
    spec: dict[str, Any],
    task_type: str,
    *,
    available_workers: Iterable[str] | None = None,
) -> dict[str, Any]:
    """Fill type-profile defaults into a raw task-spec dict, in place.

    Only absent keys are filled (``setdefault`` semantics throughout), so a
    spec loaded from JSON or built from explicit CLI flags keeps every value
    the caller stated.
    """
    profile = TASK_TYPE_PROFILES.get(task_type)
    if profile is None:
        raise ValueError(
            f"unknown task type: {task_type!r} (expected one of {', '.join(TASK_TYPES)})"
        )
    metadata = spec.setdefault("metadata", {})
    if isinstance(metadata, dict):
        metadata.setdefault("type", task_type)
        if "auto_repair" in profile:
            metadata.setdefault("auto_repair", profile["auto_repair"])
    if "priority" in profile:
        spec.setdefault("priority", profile["priority"])
    limits = spec.setdefault("limits", {})
    if isinstance(limits, dict):
        for key in ("timeout_seconds", "maximum_follow_up_turns"):
            if key in profile:
                limits.setdefault(key, profile[key])
    if not spec.get("worker") and profile.get("workers"):
        known = set(available_workers or ())
        for candidate in profile["workers"]:
            if candidate in known:
                spec["worker"] = candidate
                break
    return spec
