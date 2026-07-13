# worker-bridge

**Delegate coding tasks to external AI coding agents — in isolated git worktrees, with independent verification — from any agent, over MCP.**

worker-bridge lets one AI agent hand a scoped coding job (implement a feature, fix a cross-file bug, run a migration) to an external coding CLI — **Codex, Claude Code, OpenCode, or any command-line agent** — inside an isolated git worktree, then **independently verifies the diff itself** before handing it back. It exposes this as an **MCP server**, so any MCP-capable client (Claude Code, Cursor, Windsurf, Cline, Continue, …) can use it with zero code.

Dispatching agents is commodity. worker-bridge is about dispatching with a **provable chain of custody**: isolated execution, host-wide concurrency limits, an independent verification gate, and secret-safe event logs.

## Why

- **Isolation by default** — each task runs in its own `git worktree` on a fresh branch. Workers never touch your working tree.
- **Independent verification** — the orchestrator runs your verification commands (`pytest -q`, `npm test`, …) itself, separately from the worker, and records the result. A worker can't mark its own homework.
- **Cross-process safety** — task ownership is an atomic SQLite claim and concurrency is enforced by DB-backed leases, so many independently-launched runners can share one host without double-executing a task or overrunning limits.
- **Windows-aware** — cancellation kills the whole worker process tree (validated by command line, so a recycled PID is never killed).
- **Secret-safe** — connection strings, PEM keys, and cloud key IDs are redacted before anything is persisted.
- **Multi-worker** — run the same task on several workers and compare, or run an implementer + a reviewer.

> These guarantees come from an independent production audit of the engine; see [`docs/audit.md`](docs/audit.md) for the findings.

## Install

```bash
pip install worker-bridge-mcp
```

You also need at least one worker CLI on your `PATH` — e.g. [Codex](https://github.com/openai/codex), [Claude Code](https://docs.claude.com/claude-code), or [OpenCode](https://opencode.ai). Run `worker-bridge workers list` to see what's detected.

## Use it as an MCP server

The server runs over stdio. Add it to your MCP client:

**Claude Code**
```bash
claude mcp add worker-bridge -- worker-bridge-mcp
```

**Cursor / Windsurf / Cline / Continue** — add to the client's MCP config (`mcp.json` / settings):
```json
{
  "mcpServers": {
    "worker-bridge": {
      "command": "worker-bridge-mcp"
    }
  }
}
```

Optional environment:
```json
{
  "mcpServers": {
    "worker-bridge": {
      "command": "worker-bridge-mcp",
      "env": { "WORKER_BRIDGE_HOME": "/path/to/state", "WORKER_BRIDGE_MAX_CONCURRENCY": "4" }
    }
  }
}
```

### Tools

| Tool | What it does |
|---|---|
| `worker_delegate` | Start a scoped coding task on a worker in an isolated worktree; returns a `task_id`. |
| `worker_status` | Poll a task: status, summary, changed files, verification result, artifact paths. |
| `list_workers` | Which coding workers are installed and healthy on this machine. |
| `worker_cancel` | Cancel a task and terminate its worker process tree. |
| `worker_logs` | Normalized event stream for a task (progress, completion, verification). |

Typical flow, from the host agent's side: call `worker_delegate(objective=…, repository=…, verify=["pytest -q"])`, then poll `worker_status(task_id)` until it's `succeeded`/`failed`. The changed files land in an isolated worktree plus a diff artifact; nothing is merged into your branch automatically.

## Use it as a CLI

```bash
worker-bridge workers list
worker-bridge tasks create --objective "Add a --json flag" --repo /abs/path/repo --worker codex --verify "pytest -q"
worker-bridge tasks start <task_id> --wait
worker-bridge tasks show <task_id>
```

### Workflow-typed dispatch

`tasks create --type {chore,feature,hotfix,refactor}` shapes the task at
creation time. The profile fills only fields you left unset — explicit
`--worker`/`--priority`/`--timeout` (or keys in a `--spec` contract) always
win. `chore` → priority 30, 900s budget, cheapest adequate worker; `hotfix` →
priority 90 with a tight 1800s budget. The type lands in `spec.metadata.type`
for downstream tooling. Profiles live in `worker_bridge/workflows.py`.

### Verification auto-repair

When independent verification fails, the failing commands' exit codes and
output tails are piped back into the worker's native session as a follow-up,
and the follow-up run re-verifies — bounded by `verification_auto_repair` in
config (default 1 attempt), per-task `metadata.auto_repair` (0 disables), and
`limits.maximum_follow_up_turns`. Repair attempts emit
`verification.auto_repair` events; only an unrepairable failure counts toward
the worker's circuit breaker. Deterministic checks stay outside the agent
loop — tokens are spent only when a check fails and its output carries
information the worker needs.

## Use it as a Python library

```python
import asyncio
from worker_bridge import WorkerBridge

bridge = WorkerBridge()
task = bridge.create_task({
    "objective": "Add a --json flag to the CLI",
    "worker": "codex",
    "workspace": {"repository": "/abs/path/to/repo", "isolation": "git_worktree"},
    "verification": {"commands": ["pytest -q"]},
})
result = asyncio.run(bridge.start_task(task["task_id"]))
print(result["status"], result["result"]["metadata"]["verification"]["ok"])
```

## Workers

Built-in adapters:

- **codex**, **claude-code**, **opencode** — the mainstream coding CLIs.
- **zcode-glm** — Claude Code pointed at an Anthropic-compatible endpoint (default Z.ai GLM). A template for any alternate endpoint: subclass or construct with a different `base_url`/`model`; the auth token is read from an env var *by name* (`ZCODE_AUTH_TOKEN`) so it never lands in a task spec or result.
- **vscode** *(experimental)* — delegates into a running VS Code window via the companion `vscode-extension/` (a loopback HTTP bridge on `127.0.0.1:9394`). Install/run the extension first; without it the worker fails closed cleanly. See [`vscode-extension/README.md`](vscode-extension/README.md).
- **mock** — deterministic, for tests.

Any other non-interactive coding CLI can be linked without code:

```bash
worker-bridge workers link my-agent --command-json '["my-agent","run","{prompt}"]'
```

### Accepting work

A task's changes live in an isolated worktree and are never merged automatically. When you're satisfied, `worker-bridge tasks accept <task_id>` copies the verified changes back into the source repository (under a repository lock, refusing symlink escapes). Only independently-verified successful tasks can be accepted.

## Configuration

State lives under `WORKER_BRIDGE_HOME` (default `~/.worker-bridge/`): the SQLite store, worktrees, and artifacts. Tunables via env or `~/.worker-bridge/config.yaml`:

| Env | Default | Meaning |
|---|---|---|
| `WORKER_BRIDGE_HOME` | `~/.worker-bridge` | State root |
| `WORKER_BRIDGE_MAX_CONCURRENCY` | `4` | Global concurrent workers (host-wide) |
| `WORKER_BRIDGE_REPO_CONCURRENCY` | `3` | Concurrent workers per repository |
| `WORKER_BRIDGE_STORE` | — | Override the SQLite path |

## Storage safety

Workspace allocation is designed so a delegation can never quietly eat the disk:

- **Containment** — the worker root and the target repository must never contain each other (checked by path math, not names), so a task can never recursively copy earlier tasks' workspaces into itself. Copying a repo at or above the bridge home requires an explicit `allow_profile_copy` opt-in, and copy sources containing symlinks/junctions are refused outright.
- **Copy budgets + disk reserve** — `copy` isolation measures the source first (honoring cache/VCS exclusions like `.git`, `node_modules`, `__pycache__`) and refuses anything over 2 GiB / 50k files; the copy must also leave at least max(10 GiB, 5% of capacity) free on disk.
- **Two-phase allocation** — the destination is planned and persisted (`allocation_state: "allocating"`) *before* any filesystem mutation, and the mutation runs under a timeout. A crash, kill, or hang always leaves a task record naming the directory it was building, a clean `failed`/`timed_out` state, and a swept partial tree — never an unfindable half-copied giant.
- **Prune** — `worker-bridge workspaces prune` (dry-run by default; `--apply` to delete, `--include-paused` to widen) and the `worker_prune` MCP tool reclaim worktrees of terminal tasks plus orphan directories no task record references. Accepted tasks reclaim their worktree automatically.

## Security

Custom/read-only permission profiles are checked after execution and are **not** an OS sandbox — the real filesystem boundary is the worker client's own sandbox (Codex `workspace-write`, etc.), which the bridge selects per profile. Symlink escapes are detected and fail the task; a raw path-traversal write by a sandbox-less worker cannot be seen post-hoc. Do not point a `full_access` worker at hostile input without a container. Secrets are redacted from the event log and store.

## License

MIT — see [LICENSE](LICENSE).
