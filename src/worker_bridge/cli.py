"""Operator CLI for the external worker bridge."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

from worker_bridge.environ import get_home as get_hermes_home
from worker_bridge.orchestrator import WorkerBridge
from worker_bridge.registry import WorkerRegistry
from worker_bridge.store import WorkerStore
from worker_bridge.workflows import TASK_TYPES, apply_task_type
from worker_bridge.workspace import WorkspaceManager


def _json(value: Any) -> None:
    print(json.dumps(value, indent=2, sort_keys=True, default=str))


def _add_store(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--store", default="", help=argparse.SUPPRESS)


def register_cli(parser: argparse.ArgumentParser) -> None:
    subs = parser.add_subparsers(dest="worker_area")

    workers = subs.add_parser("workers", help="Inspect worker availability")
    workers_sub = workers.add_subparsers(dest="worker_action")
    for action in ("list", "status", "discover"):
        p = workers_sub.add_parser(action)
        if action == "status":
            p.add_argument("worker", nargs="?")
        if action in {"list", "discover"}:
            p.add_argument(
                "--kind",
                choices=["workers", "all", "assistants", "environments", "extensions"],
                default="workers" if action == "list" else "all",
            )
        _add_store(p)
    link = workers_sub.add_parser("link", help="Link a discovered assistant through a verified command adapter")
    link.add_argument("discovery_id")
    link.add_argument("--name")
    link.add_argument("--command-json", help='argv JSON, e.g. ["agent","run","{prompt}"]')
    link.add_argument("--resume-command-json", help='argv JSON containing {session_id} and {prompt}')
    link.add_argument("--maximum-concurrency", type=int, default=1)
    _add_store(link)
    unlink = workers_sub.add_parser("unlink")
    unlink.add_argument("worker")
    _add_store(unlink)
    sessions = workers_sub.add_parser("sessions", help="Discover recent native client session ids")
    sessions.add_argument("worker", nargs="?")
    sessions.add_argument("--limit", type=int, default=20)
    _add_store(sessions)

    tasks = subs.add_parser("tasks", help="Create and operate tasks")
    task_sub = tasks.add_subparsers(dest="worker_action")
    create = task_sub.add_parser("create")
    create.add_argument("--spec", help="JSON task contract")
    create.add_argument("--objective")
    create.add_argument("--repo")
    # Workflow-typed dispatch: the type shapes priority, budgets, auto-repair
    # and (when the caller names no worker) the worker tier. Explicit flags
    # always win over the type profile.
    create.add_argument("--type", choices=list(TASK_TYPES), dest="task_type",
                        help="workflow type; fills priority/limits/worker defaults")
    create.add_argument("--worker", default=None, help="worker name (default: type profile, then codex)")
    create.add_argument("--priority", type=int, default=None)
    create.add_argument("--role", default="implementer")
    create.add_argument("--base-ref", default="HEAD")
    create.add_argument("--isolation", choices=["git_worktree", "direct", "copy"], default="git_worktree")
    create.add_argument("--permission", choices=["read_only", "workspace_write", "full_access", "custom"], default="workspace_write")
    create.add_argument("--verify", action="append", default=[])
    create.add_argument("--acceptance", action="append", default=[])
    create.add_argument("--constraint", action="append", default=[])
    create.add_argument("--forbid", action="append", default=[])
    create.add_argument("--idempotency-key")
    create.add_argument("--job-id")
    create.add_argument("--timeout", type=int, default=None)
    _add_store(create)
    for action in ("list", "show", "logs", "artifacts", "start", "attach", "continue", "submit-input", "redirect", "cancel", "pause", "resume", "retry", "verify", "accept", "reject", "handoff"):
        p = task_sub.add_parser(action)
        if action != "list":
            p.add_argument("task_id")
        if action in {"continue", "submit-input", "redirect"}:
            p.add_argument("message")
        if action == "reject":
            p.add_argument("--reason", required=True)
        if action == "handoff":
            p.add_argument("--worker", required=True)
        if action == "attach":
            p.add_argument("--session-id", required=True)
        if action in {"start", "continue", "submit-input", "redirect", "retry"}:
            p.add_argument("--wait", action="store_true")
        if action == "list":
            p.add_argument("--status")
        if action == "logs":
            p.add_argument("--after", type=int, default=0)
        _add_store(p)

    jobs = subs.add_parser("jobs", help="Create and operate jobs")
    job_sub = jobs.add_subparsers(dest="worker_action")
    create_job = job_sub.add_parser("create")
    create_job.add_argument("spec", help="JSON file with objective, strategy, and tasks")
    _add_store(create_job)
    for action in ("list", "show", "run", "cancel"):
        p = job_sub.add_parser(action)
        if action != "list":
            p.add_argument("job_id")
        _add_store(p)

    requests = subs.add_parser("requests", help="Operate permission requests")
    request_sub = requests.add_subparsers(dest="worker_action")
    req_list = request_sub.add_parser("list")
    req_list.add_argument("--status")
    _add_store(req_list)
    for action in ("approve", "deny", "modify", "answer"):
        p = request_sub.add_parser(action)
        p.add_argument("request_id")
        if action in {"approve", "modify"}:
            p.add_argument("--scope", action="append", default=[])
        if action == "answer":
            p.add_argument("message")
            p.add_argument("--wait", action="store_true")
        _add_store(p)

    results = subs.add_parser("results", help="Compare job results")
    result_sub = results.add_subparsers(dest="worker_action")
    compare = result_sub.add_parser("compare")
    compare.add_argument("job_id")
    _add_store(compare)
    integrate = result_sub.add_parser("integrate")
    integrate.add_argument("job_id")
    integrate.add_argument("--task", action="append", required=True)
    integrate.add_argument("--verify", action="append", default=[])
    _add_store(integrate)

    health = subs.add_parser("health", help="Show bridge and worker health")
    _add_store(health)

    workspaces = subs.add_parser("workspaces", help="Inspect and reclaim worker worktrees")
    ws_sub = workspaces.add_subparsers(dest="worker_action")
    prune = ws_sub.add_parser("prune", help="Reclaim worktrees no active task backs (dry-run by default)")
    prune.add_argument("--apply", action="store_true", help="actually delete (default: dry-run report)")
    prune.add_argument("--include-paused", action="store_true", help="also reclaim paused-task worktrees")
    _add_store(prune)

    parser.set_defaults(func=worker_command)


def _bridge(args: argparse.Namespace) -> WorkerBridge:
    from worker_bridge.environ import load_config

    config = load_config() or {}
    # Standalone config is flat; a nested `worker_bridge:` section is also
    # honored for anyone who prefers to namespace it.
    section = config.get("worker_bridge") or config or {}
    path = (
        getattr(args, "store", "")
        or os.environ.get("WORKER_BRIDGE_STORE")
        or section.get("store_path")
        or None
    )
    worktree_root = (
        os.environ.get("WORKER_BRIDGE_WORKTREE_ROOT")
        or section.get("worktree_root")
        or None
    )
    maximum = int(
        os.environ.get("WORKER_BRIDGE_MAX_CONCURRENCY")
        or section.get("maximum_concurrency", 4)
    )
    per_repo = int(
        os.environ.get("WORKER_BRIDGE_REPO_CONCURRENCY")
        or section.get("per_repository_concurrency", 3)
    )
    per_job = int(
        os.environ.get("WORKER_BRIDGE_JOB_CONCURRENCY")
        or section.get("per_job_concurrency", 3)
    )
    return WorkerBridge(
        store=WorkerStore(path),
        registry=WorkerRegistry.from_config(config),
        workspaces=WorkspaceManager(worktree_root),
        maximum_concurrency=maximum,
        per_repository_concurrency=per_repo,
        per_job_concurrency=per_job,
        retention={
            "retain_failed_tasks": bool(section.get("retain_failed_tasks", True)),
            "retain_cancelled_tasks": bool(section.get("retain_cancelled_tasks", True)),
            "retain_unaccepted_tasks": bool(section.get("retain_unaccepted_tasks", True)),
        },
        verification_auto_repair=int(section.get("verification_auto_repair", 1)),
    )


def _load_task_spec(
    args: argparse.Namespace, available_workers: list[str] | None = None
) -> dict[str, Any]:
    if args.spec:
        payload = json.loads(Path(args.spec).read_text(encoding="utf-8"))
    else:
        if not args.objective or not args.repo:
            raise ValueError("--objective and --repo are required without --spec")
        payload = {
            "objective": args.objective,
            "role": args.role,
            "job_id": args.job_id,
            "idempotency_key": args.idempotency_key,
            "workspace": {
                "repository": str(Path(args.repo).expanduser().resolve()),
                "isolation": args.isolation,
                "base_ref": args.base_ref,
            },
            "permissions": {"profile": args.permission},
            "constraints": args.constraint,
            "acceptance_criteria": args.acceptance,
            "verification": {"commands": args.verify, "forbidden_paths": args.forbid},
        }
        # Absent-when-defaulted keys let a --type profile fill them; the
        # TaskSpec dataclass supplies the final fallbacks (worker=codex,
        # timeout=3600, priority=50).
        if args.worker:
            payload["worker"] = args.worker
        if args.priority is not None:
            payload["priority"] = args.priority
        if args.timeout is not None:
            payload["limits"] = {"timeout_seconds": args.timeout}
    task_type = getattr(args, "task_type", None)
    if task_type:
        apply_task_type(payload, task_type, available_workers=available_workers)
    return payload


def _spawn(bridge: WorkerBridge, task_id: str, message: str | None = None) -> dict[str, Any]:
    artifact_dir = get_hermes_home() / "workers" / "artifacts" / task_id
    artifact_dir.mkdir(parents=True, exist_ok=True)
    log_path = artifact_dir / "runner.log"
    command = [
        sys.executable,
        "-m",
        "worker_bridge.runner",
        task_id,
        "--store",
        str(bridge.store.path),
    ]
    if message is not None:
        command.extend(["--continue-message", message])
    flags = 0
    if os.name == "nt":
        flags = subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.DETACHED_PROCESS
    # worker_bridge is an installed package, so ``-m worker_bridge.runner``
    # resolves from site-packages regardless of cwd.
    with log_path.open("ab") as log:
        proc = subprocess.Popen(
            command,
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=subprocess.STDOUT,
            creationflags=flags,
            start_new_session=os.name != "nt",
            close_fds=True,
        )
    task = bridge.get_task(task_id)
    runtime = dict(task.get("runtime") or {})
    runtime.update({"pid": proc.pid, "runner_log": str(log_path)})
    bridge.store.update_task(task_id, status="queued", runtime=runtime)
    bridge.store.append_event("task.spawned", {"pid": proc.pid}, task_id=task_id)
    return bridge.get_task(task_id)


def worker_command(args: argparse.Namespace) -> int:
    area = getattr(args, "worker_area", None)
    action = getattr(args, "worker_action", None)
    if not area:
        print("Usage: worker-bridge {workers|tasks|jobs|requests|results|workspaces|health} ...")
        return 2
    bridge = _bridge(args)
    try:
        if area == "workers":
            if action in {"discover", "list"} and args.kind != "workers":
                items = asyncio.run(bridge.registry.discover(args.kind))
            elif action in {"discover", "list", "status"}:
                items = asyncio.run(bridge.registry.list())
            elif action == "link":
                _json(_link_discovery(bridge, args))
                return 0
            elif action == "unlink":
                _json(_unlink_worker(args.worker))
                return 0
            elif action == "sessions":
                from worker_bridge.discovery import discover_sessions

                _json(discover_sessions(args.worker, max(1, min(args.limit, 100))))
                return 0
            else:
                return 2
            if action == "status" and args.worker:
                items = [item for item in items if item["worker"] == args.worker]
            _json(items)
        elif area == "health":
            _json({"store": str(bridge.store.path), "workers": asyncio.run(bridge.registry.list())})
        elif area == "workspaces" and action == "prune":
            _json(bridge.prune_workspaces(apply=args.apply, include_paused=args.include_paused))
        elif area == "tasks":
            return _task_command(bridge, args, action)
        elif area == "jobs":
            return _job_command(bridge, args, action)
        elif area == "requests":
            if action == "list":
                _json({
                    "permissions": bridge.store.list_permission_requests(args.status),
                    "clarifications": bridge.store.list_input_requests(args.status),
                })
            elif action == "approve":
                decision = "approved_narrowed" if args.scope else "approved_task"
                _json(bridge.decide_permission(args.request_id, decision, scope=args.scope))
            elif action == "modify":
                _json(bridge.decide_permission(args.request_id, "approved_narrowed", scope=args.scope))
            elif action == "deny":
                _json(bridge.decide_permission(args.request_id, "denied"))
            elif action == "answer":
                if args.wait:
                    _json(asyncio.run(bridge.answer_input_request(args.request_id, args.message)))
                else:
                    request = bridge.store.answer_input_request(args.request_id, args.message)
                    _json(_spawn(bridge, request["task_id"], args.message))
            else:
                return 2
        elif area == "results" and action == "compare":
            _json(bridge.compare_results(args.job_id))
        elif area == "results" and action == "integrate":
            _json(bridge.integrate_results(args.job_id, args.task, verification_commands=args.verify))
        else:
            return 2
        return 0
    except (KeyError, ValueError, RuntimeError) as exc:
        print(f"worker bridge error: {exc}", file=sys.stderr)
        return 1


def _link_discovery(bridge: WorkerBridge, args: argparse.Namespace) -> dict[str, Any]:
    records = asyncio.run(bridge.registry.discover("all"))
    record = next((item for item in records if item["id"].lower() == args.discovery_id.lower()), None)
    if not record:
        raise ValueError(f"discovery item not found: {args.discovery_id}")
    if record.get("worker_ready") and record.get("worker") and not args.command_json:
        return {
            "linked": True,
            "discovery_id": record["id"],
            "worker": record["worker"],
            "link_method": record["link_method"],
            "persisted": False,
        }
    if not args.command_json:
        raise ValueError(
            f"{record['id']} is discovery-only. Supply --command-json for a documented "
            "noninteractive CLI, or install an ACP/MCP adapter; GUI presence alone is not a safe worker link."
        )
    command = json.loads(args.command_json)
    resume = json.loads(args.resume_command_json) if args.resume_command_json else None
    if not isinstance(command, list) or not command or not all(isinstance(item, str) for item in command):
        raise ValueError("--command-json must be a non-empty JSON argv array")
    if "{prompt}" not in " ".join(command):
        raise ValueError("linked command must contain {prompt}")
    if resume is not None:
        if not isinstance(resume, list) or not all(isinstance(item, str) for item in resume):
            raise ValueError("--resume-command-json must be a JSON argv array")
        joined = " ".join(resume)
        if "{session_id}" not in joined or "{prompt}" not in joined:
            raise ValueError("resume command must contain {session_id} and {prompt}")
    name = args.name or record["id"].lower().replace(".", "-")
    from worker_bridge.environ import read_raw_config, save_config

    raw = read_raw_config() or {}
    section = raw.setdefault("worker_bridge", {})
    workers = section.setdefault("workers", {})
    workers[name] = {
        "source_discovery_id": record["id"],
        "command": command,
        "maximum_concurrency": max(1, args.maximum_concurrency),
    }
    if resume:
        workers[name]["resume_command"] = resume
    save_config(raw)
    return {
        "linked": True,
        "discovery_id": record["id"],
        "worker": name,
        "link_method": "configured_cli",
        "persisted": True,
    }


def _unlink_worker(worker: str) -> dict[str, Any]:
    from worker_bridge.environ import read_raw_config, save_config

    raw = read_raw_config() or {}
    workers = ((raw.get("worker_bridge") or {}).get("workers") or {})
    if worker not in workers:
        raise KeyError(f"configured worker not found: {worker}")
    del workers[worker]
    save_config(raw)
    return {"unlinked": True, "worker": worker}


def _task_command(bridge: WorkerBridge, args: argparse.Namespace, action: str | None) -> int:
    if action == "create":
        _json(bridge.create_task(_load_task_spec(args, available_workers=bridge.registry.names())))
    elif action == "list":
        _json(bridge.list_tasks(status=args.status))
    elif action == "show":
        _json(bridge.get_task(args.task_id))
    elif action == "logs":
        _json(bridge.store.events(task_id=args.task_id, after=args.after))
    elif action == "artifacts":
        _json(bridge.get_task_artifacts(args.task_id))
    elif action == "start":
        _json(asyncio.run(bridge.start_task(args.task_id)) if args.wait else _spawn(bridge, args.task_id))
    elif action == "attach":
        _json(asyncio.run(bridge.attach_session(args.task_id, args.session_id)))
    elif action in {"continue", "submit-input"}:
        _json(asyncio.run(bridge.continue_task(args.task_id, args.message)) if args.wait else _spawn(bridge, args.task_id, args.message))
    elif action == "redirect":
        if args.wait:
            _json(asyncio.run(bridge.redirect_task(args.task_id, args.message)))
        else:
            bridge.store.append_event("task.redirected", {"instruction": args.message}, task_id=args.task_id)
            _json(_spawn(bridge, args.task_id, f"Direction changed by the orchestrator: {args.message}"))
    elif action == "cancel":
        _json(asyncio.run(bridge.cancel_task(args.task_id)))
    elif action == "pause":
        _json(bridge.pause_task(args.task_id))
    elif action == "resume":
        _json(bridge.resume_task(args.task_id))
    elif action == "retry":
        if args.wait:
            _json(asyncio.run(bridge.retry_task(args.task_id)))
        else:
            bridge.queue_retry(args.task_id)
            _json(_spawn(bridge, args.task_id))
    elif action == "verify":
        _json(bridge.verify_task(args.task_id))
    elif action == "accept":
        _json(bridge.accept_task(args.task_id))
    elif action == "reject":
        _json(bridge.reject_task(args.task_id, args.reason))
    elif action == "handoff":
        _json(bridge.replace_worker(args.task_id, args.worker))
    else:
        return 2
    return 0


def _job_command(bridge: WorkerBridge, args: argparse.Namespace, action: str | None) -> int:
    if action == "create":
        payload = json.loads(Path(args.spec).read_text(encoding="utf-8"))
        _json(bridge.create_job(payload["objective"], payload["tasks"], strategy=payload.get("strategy", "parallel")))
    elif action == "list":
        _json(bridge.store.list_jobs())
    elif action == "show":
        _json(bridge.store.get_job(args.job_id))
    elif action == "run":
        _json(asyncio.run(bridge.run_job(args.job_id)))
    elif action == "cancel":
        _json(asyncio.run(bridge.cancel_job(args.job_id)))
    else:
        return 2
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="worker-bridge",
        description="Delegate coding tasks to external AI coding agents in isolated "
        "git worktrees, then independently verify the result.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    register_cli(parser)
    args = parser.parse_args(argv)
    func = getattr(args, "func", None)
    if func is None:
        parser.print_help()
        return 2
    return func(args)


if __name__ == "__main__":
    raise SystemExit(main())
