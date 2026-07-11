# Production audit

The orchestration engine underwent an independent production-readiness audit focused on cross-process correctness, security, Windows behavior, and the verification gate. Nine defects were reproduced with runnable evidence and fixed; all are covered by the deterministic regression tests in `tests/test_hardening.py`.

The theme: dispatching workers was solid, but the layers that make multi-client orchestration *trustworthy across processes* — task ownership, scheduling limits, cancellation, and the verification gate — were where the release blockers lived. A single-process test suite never exercised two OS processes sharing the store, which is exactly where the failures were.

| # | Severity | Defect | Fix |
|---|---|---|---|
| 1 | P0 | Restart recovery reset every `running` task to `paused` on any bridge construction (e.g. a status query), corrupting live peers | PID-aware recovery: reap only tasks whose owner process is dead |
| 2 | P0 | No atomic task claim / no terminal guard — two runners could execute one task; a finished task could re-run | Atomic `BEGIN IMMEDIATE` compare-and-set claim + terminal-state guard |
| 3 | P0 | Concurrency caps were in-process semaphores — two runners doubled the global limit | DB-backed leases for global / per-worker / per-repo scopes, reclaimed from dead PIDs |
| 4 | P0 | Windows cancellation killed only the runner, orphaning the worker child that kept editing the worktree | Process-tree kill with command-line PID validation; durable terminal state |
| 5 | P0 | `git status` parsing stripped the first filename's leading space, corrupting names and **bypassing forbidden-path checks** | NUL-framed (`-z`) status parsing |
| 6 | P1 | Redaction missed connection-string passwords, PEM keys, and cloud key IDs — they persisted in the store | Extended redaction patterns; verified nothing sensitive reaches the DB |
| 7 | P1 | Symlink/junction escapes were invisible to permission checks | Resolve changed paths; fail closed on real targets outside the workspace |
| 8 | P1 | Integration `git apply` failed on ordinary tracked-file edits under CRLF | Apply with `--ignore-whitespace` for line-ending robustness |
| 9 | P0 | Workspace preparation raced across processes; the loser clobbered the winner's task | Claim ownership before preparing the workspace; ownership-aware runner failure handling |

**Residual, by design:** custom/read-only permission profiles are post-hoc checks over what `git status` surfaces, not an OS sandbox. A sandbox-less worker performing a raw path-traversal write outside the repo produces no git-visible change and cannot be caught after the fact — that boundary belongs to the worker client's native sandbox, which the bridge selects per permission profile. Native Codex thread-resume is best-effort (re-run in the same worktree) rather than depending on a private transport.
