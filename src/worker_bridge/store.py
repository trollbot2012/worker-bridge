"""SQLite persistence and replayable event log for worker orchestration."""

from __future__ import annotations

import json
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

import psutil

from worker_bridge.environ import get_home as get_hermes_home
from worker_bridge.redaction import redact


def _pid_alive(pid: Any) -> bool:
    """True only if pid names a live, non-zombie process.

    A pid owned by another user (AccessDenied) is treated as alive so the
    scheduler never reaps or double-claims a task it merely cannot inspect.
    """
    if not pid:
        return False
    try:
        proc = psutil.Process(int(pid))
        return proc.is_running() and proc.status() != psutil.STATUS_ZOMBIE
    except psutil.AccessDenied:
        return True
    except (psutil.NoSuchProcess, ValueError, OverflowError, TypeError):
        return False


def default_store_path() -> Path:
    return get_hermes_home() / "workers" / "bridge.db"


class WorkerStore:
    def __init__(self, path: str | Path | None = None) -> None:
        self.path = Path(path or default_store_path()).expanduser().resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._initialize()

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=30000")
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _initialize(self) -> None:
        with self._lock, self._connect() as conn:
            conn.executescript(
                """
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS jobs (
                    job_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    objective TEXT NOT NULL,
                    strategy TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS tasks (
                    task_id TEXT PRIMARY KEY,
                    job_id TEXT,
                    idempotency_key TEXT UNIQUE,
                    worker TEXT NOT NULL,
                    status TEXT NOT NULL,
                    priority INTEGER NOT NULL,
                    spec TEXT NOT NULL,
                    runtime TEXT NOT NULL,
                    result TEXT,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    FOREIGN KEY(job_id) REFERENCES jobs(job_id)
                );
                CREATE TABLE IF NOT EXISTS events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id TEXT,
                    job_id TEXT,
                    kind TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    created_at REAL NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_events_task ON events(task_id, event_id);
                CREATE TABLE IF NOT EXISTS permission_requests (
                    request_id TEXT PRIMARY KEY,
                    task_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    request TEXT NOT NULL,
                    decision TEXT,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    FOREIGN KEY(task_id) REFERENCES tasks(task_id)
                );
                CREATE TABLE IF NOT EXISTS input_requests (
                    request_id TEXT PRIMARY KEY,
                    task_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    request TEXT NOT NULL,
                    answer TEXT,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    FOREIGN KEY(task_id) REFERENCES tasks(task_id)
                );
                CREATE TABLE IF NOT EXISTS leases (
                    scope TEXT NOT NULL,
                    task_id TEXT NOT NULL,
                    pid INTEGER NOT NULL,
                    acquired_at REAL NOT NULL,
                    PRIMARY KEY(scope, task_id)
                );
                CREATE INDEX IF NOT EXISTS idx_leases_scope ON leases(scope);
                """
            )

    @staticmethod
    def _loads(value: str | None) -> Any:
        return json.loads(value) if value else None

    @staticmethod
    def _dumps(value: Any) -> str:
        return json.dumps(redact(value), sort_keys=True, separators=(",", ":"))

    def create_job(
        self,
        *,
        objective: str,
        strategy: str = "single",
        payload: dict[str, Any] | None = None,
        job_id: str | None = None,
    ) -> dict[str, Any]:
        now = time.time()
        job_id = job_id or f"job-{uuid.uuid4().hex[:12]}"
        body = dict(payload or {})
        with self._lock, self._connect() as conn:
            conn.execute(
                "INSERT INTO jobs VALUES (?, ?, ?, ?, ?, ?, ?)",
                (job_id, "created", objective, strategy, self._dumps(body), now, now),
            )
        self.append_event("job.created", body, job_id=job_id)
        return self.get_job(job_id)  # type: ignore[return-value]

    def get_job(self, job_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone()
            if not row:
                return None
            result = dict(row)
            result["payload"] = self._loads(result["payload"])
            result["tasks"] = [
                item["task_id"]
                for item in conn.execute(
                    "SELECT task_id FROM tasks WHERE job_id=? ORDER BY created_at", (job_id,)
                ).fetchall()
            ]
            return result

    def list_jobs(self, limit: int = 100) -> list[dict[str, Any]]:
        with self._connect() as conn:
            ids = conn.execute(
                "SELECT job_id FROM jobs ORDER BY created_at DESC LIMIT ?", (limit,)
            ).fetchall()
        return [job for row in ids if (job := self.get_job(row["job_id"]))]

    def update_job(self, job_id: str, *, status: str, payload: dict[str, Any] | None = None) -> None:
        with self._lock, self._connect() as conn:
            if payload is None:
                conn.execute(
                    "UPDATE jobs SET status=?, updated_at=? WHERE job_id=?",
                    (status, time.time(), job_id),
                )
            else:
                conn.execute(
                    "UPDATE jobs SET status=?, payload=?, updated_at=? WHERE job_id=?",
                    (status, self._dumps(payload), time.time(), job_id),
                )
        self.append_event("job.status", {"status": status}, job_id=job_id)

    def create_task(self, spec: dict[str, Any], runtime: dict[str, Any] | None = None) -> dict[str, Any]:
        now = time.time()
        task_id = spec.get("task_id") or f"task-{uuid.uuid4().hex[:12]}"
        spec = dict(spec)
        spec["task_id"] = task_id
        idempotency_key = spec.get("idempotency_key")
        with self._lock, self._connect() as conn:
            if idempotency_key:
                existing = conn.execute(
                    "SELECT task_id FROM tasks WHERE idempotency_key=?", (idempotency_key,)
                ).fetchone()
                if existing:
                    return self.get_task(existing["task_id"])  # type: ignore[return-value]
            conn.execute(
                "INSERT INTO tasks VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?)",
                (
                    task_id,
                    spec.get("job_id"),
                    idempotency_key,
                    spec["worker"],
                    "created",
                    int(spec.get("priority", 50)),
                    self._dumps(spec),
                    self._dumps(runtime or {}),
                    now,
                    now,
                ),
            )
        self.append_event("task.created", {"worker": spec["worker"]}, task_id=task_id, job_id=spec.get("job_id"))
        return self.get_task(task_id)  # type: ignore[return-value]

    def get_task(self, task_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
        if not row:
            return None
        result = dict(row)
        for key in ("spec", "runtime", "result"):
            result[key] = self._loads(result[key])
        return result

    def list_tasks(self, *, status: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        sql = "SELECT task_id FROM tasks"
        params: list[Any] = []
        if status:
            sql += " WHERE status=?"
            params.append(status)
        sql += " ORDER BY priority DESC, created_at ASC LIMIT ?"
        params.append(limit)
        with self._connect() as conn:
            ids = conn.execute(sql, params).fetchall()
        return [task for row in ids if (task := self.get_task(row["task_id"]))]

    def update_task(
        self,
        task_id: str,
        *,
        status: str | None = None,
        runtime: dict[str, Any] | None = None,
        result: dict[str, Any] | None = None,
    ) -> None:
        updates = ["updated_at=?"]
        values: list[Any] = [time.time()]
        for key, value in (("status", status), ("runtime", runtime), ("result", result)):
            if value is not None:
                updates.append(f"{key}=?")
                values.append(self._dumps(value) if key != "status" else value)
        values.append(task_id)
        with self._lock, self._connect() as conn:
            cursor = conn.execute(f"UPDATE tasks SET {', '.join(updates)} WHERE task_id=?", values)
            if not cursor.rowcount:
                raise KeyError(task_id)
        if status:
            self.append_event("task.status", {"status": status}, task_id=task_id)

    def update_task_spec(self, task_id: str, spec: dict[str, Any]) -> None:
        with self._lock, self._connect() as conn:
            cursor = conn.execute(
                "UPDATE tasks SET spec=?, updated_at=? WHERE task_id=?",
                (self._dumps(spec), time.time(), task_id),
            )
            if not cursor.rowcount:
                raise KeyError(task_id)

    def append_event(
        self,
        kind: str,
        payload: dict[str, Any],
        *,
        task_id: str | None = None,
        job_id: str | None = None,
    ) -> int:
        encoded = self._dumps(payload)
        raw = encoded.encode("utf-8", "replace")
        if len(raw) > 1_000_000:
            encoded = self._dumps(
                {
                    "truncated": True,
                    "original_bytes": len(raw),
                    "preview": raw[:950_000].decode("utf-8", "replace"),
                }
            )
        with self._lock, self._connect() as conn:
            cursor = conn.execute(
                "INSERT INTO events(task_id, job_id, kind, payload, created_at) VALUES (?, ?, ?, ?, ?)",
                (task_id, job_id, kind, encoded, time.time()),
            )
            return int(cursor.lastrowid)

    def events(self, *, task_id: str | None = None, job_id: str | None = None, after: int = 0) -> list[dict[str, Any]]:
        clauses = ["event_id>?"]
        params: list[Any] = [after]
        if task_id:
            clauses.append("task_id=?")
            params.append(task_id)
        if job_id:
            clauses.append("job_id=?")
            params.append(job_id)
        with self._connect() as conn:
            rows = conn.execute(
                f"SELECT * FROM events WHERE {' AND '.join(clauses)} ORDER BY event_id", params
            ).fetchall()
        results = []
        for row in rows:
            item = dict(row)
            item["payload"] = self._loads(item["payload"])
            results.append(item)
        return results

    def create_permission_request(self, request: dict[str, Any]) -> dict[str, Any]:
        now = time.time()
        request_id = request.get("request_id") or f"req-{uuid.uuid4().hex[:12]}"
        request = dict(request)
        request["request_id"] = request_id
        with self._lock, self._connect() as conn:
            conn.execute(
                "INSERT INTO permission_requests VALUES (?, ?, 'pending', ?, NULL, ?, ?)",
                (request_id, request["task_id"], self._dumps(request), now, now),
            )
        self.append_event("permission.requested", request, task_id=request["task_id"])
        return self.get_permission_request(request_id)  # type: ignore[return-value]

    def get_permission_request(self, request_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM permission_requests WHERE request_id=?", (request_id,)
            ).fetchone()
        if not row:
            return None
        result = dict(row)
        result["request"] = self._loads(result["request"])
        result["decision"] = self._loads(result["decision"])
        return result

    def list_permission_requests(self, status: str | None = None) -> list[dict[str, Any]]:
        sql = "SELECT request_id FROM permission_requests"
        params: tuple[Any, ...] = ()
        if status:
            sql += " WHERE status=?"
            params = (status,)
        sql += " ORDER BY created_at"
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [item for row in rows if (item := self.get_permission_request(row["request_id"]))]

    def decide_permission(self, request_id: str, decision: dict[str, Any]) -> dict[str, Any]:
        existing = self.get_permission_request(request_id)
        if not existing:
            raise KeyError(request_id)
        if existing["status"] != "pending":
            return existing
        status = str(decision.get("decision") or "denied")
        with self._lock, self._connect() as conn:
            conn.execute(
                "UPDATE permission_requests SET status=?, decision=?, updated_at=? WHERE request_id=?",
                (status, self._dumps(decision), time.time(), request_id),
            )
        self.append_event(
            "permission.decided", {"request_id": request_id, **decision}, task_id=existing["task_id"]
        )
        return self.get_permission_request(request_id)  # type: ignore[return-value]

    def recover_running(self) -> int:
        """Reap only tasks whose owning process is dead.

        Safe to call from any process at any time: a task whose recorded pid is
        still alive is left untouched, so merely constructing a WorkerBridge (as
        every CLI command and detached runner does) can never clobber a peer's
        live task. Only genuinely orphaned work is surfaced as ``paused`` for an
        operator to resume.
        """
        recovered: list[str] = []
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                "SELECT task_id, runtime FROM tasks WHERE status IN ('running','verifying')"
            ).fetchall()
            for row in rows:
                runtime = self._loads(row["runtime"]) or {}
                if _pid_alive(runtime.get("pid")):
                    continue
                runtime["recovery_required"] = True
                conn.execute(
                    "UPDATE tasks SET status='paused', runtime=?, updated_at=? WHERE task_id=?",
                    (self._dumps(runtime), time.time(), row["task_id"]),
                )
                # Release any leases the dead owner still holds.
                conn.execute("DELETE FROM leases WHERE task_id=?", (row["task_id"],))
                recovered.append(row["task_id"])
        for task_id in recovered:
            self.append_event("task.recovered", {"status": "paused"}, task_id=task_id)
        return len(recovered)

    def claim_task(
        self,
        task_id: str,
        *,
        allowed_from: tuple[str, ...],
        pid: int,
        runtime: dict[str, Any] | None = None,
    ) -> bool:
        """Atomically transition a task to ``running`` iff no peer holds it.

        Returns True only for the single caller that wins the compare-and-set,
        so two independently launched runner processes can never both drive the
        same task. A task already ``running``/``verifying`` under a live pid is
        never stolen; if its owner is dead the row is first reaped.
        """
        with self._lock, self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT status, runtime FROM tasks WHERE task_id=?", (task_id,)
            ).fetchone()
            if row is None:
                conn.execute("ROLLBACK")
                raise KeyError(task_id)
            status = row["status"]
            if status in {"running", "verifying"}:
                owner = (self._loads(row["runtime"]) or {}).get("pid")
                if _pid_alive(owner) and owner != pid:
                    conn.execute("ROLLBACK")
                    return False
                # dead owner — fall through and reclaim
            elif status not in allowed_from:
                conn.execute("ROLLBACK")
                return False
            merged = self._loads(row["runtime"]) or {}
            if runtime:
                merged.update(runtime)
            merged["pid"] = pid
            conn.execute(
                "UPDATE tasks SET status='running', runtime=?, updated_at=? WHERE task_id=?",
                (self._dumps(merged), time.time(), task_id),
            )
            conn.execute("COMMIT")
        self.append_event("task.status", {"status": "running"}, task_id=task_id)
        return True

    def acquire_slot(self, scope: str, limit: int, task_id: str, pid: int) -> bool:
        """Cross-process concurrency slot for ``scope`` (global/worker/repo).

        Stale slots held by dead pids are reclaimed inside the same transaction,
        so a crashed runner never permanently consumes capacity.
        """
        with self._lock, self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute(
                "SELECT task_id, pid FROM leases WHERE scope=?", (scope,)
            ).fetchall()
            held = 0
            for entry in existing:
                if entry["task_id"] == task_id:
                    conn.execute("COMMIT")  # already own this slot (re-entrant)
                    return True
                if _pid_alive(entry["pid"]):
                    held += 1
                else:
                    conn.execute(
                        "DELETE FROM leases WHERE scope=? AND task_id=?",
                        (scope, entry["task_id"]),
                    )
            if held >= max(1, limit):
                conn.execute("ROLLBACK")
                return False
            conn.execute(
                "INSERT OR REPLACE INTO leases(scope, task_id, pid, acquired_at) VALUES (?, ?, ?, ?)",
                (scope, task_id, int(pid), time.time()),
            )
            conn.execute("COMMIT")
        return True

    def release_slot(self, scope: str, task_id: str) -> None:
        with self._lock, self._connect() as conn:
            conn.execute("DELETE FROM leases WHERE scope=? AND task_id=?", (scope, task_id))

    def release_all_slots(self, task_id: str) -> None:
        with self._lock, self._connect() as conn:
            conn.execute("DELETE FROM leases WHERE task_id=?", (task_id,))

    def recent_worker_failures(self, worker: str, since: float) -> int:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT e.payload FROM events e JOIN tasks t ON t.task_id=e.task_id "
                "WHERE t.worker=? AND e.kind='worker.completed' AND e.created_at>=?",
                (worker, since),
            ).fetchall()
        count = 0
        for row in rows:
            payload = self._loads(row["payload"]) or {}
            if payload.get("status") in {"failed", "timed_out"}:
                count += 1
        return count

    def create_input_request(self, task_id: str, request: dict[str, Any]) -> dict[str, Any]:
        now = time.time()
        request_id = f"input-{uuid.uuid4().hex[:12]}"
        body = {"request_id": request_id, "task_id": task_id, **request}
        with self._lock, self._connect() as conn:
            conn.execute(
                "INSERT INTO input_requests VALUES (?, ?, 'pending', ?, NULL, ?, ?)",
                (request_id, task_id, self._dumps(body), now, now),
            )
        self.append_event("input.requested", body, task_id=task_id)
        return self.get_input_request(request_id)  # type: ignore[return-value]

    def get_input_request(self, request_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM input_requests WHERE request_id=?", (request_id,)).fetchone()
        if not row:
            return None
        result = dict(row)
        result["request"] = self._loads(result["request"])
        return result

    def list_input_requests(self, status: str | None = None) -> list[dict[str, Any]]:
        sql = "SELECT request_id FROM input_requests"
        params: tuple[Any, ...] = ()
        if status:
            sql += " WHERE status=?"
            params = (status,)
        sql += " ORDER BY created_at"
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [item for row in rows if (item := self.get_input_request(row["request_id"]))]

    def answer_input_request(self, request_id: str, answer: str) -> dict[str, Any]:
        existing = self.get_input_request(request_id)
        if not existing:
            raise KeyError(request_id)
        if existing["status"] == "pending":
            with self._lock, self._connect() as conn:
                conn.execute(
                    "UPDATE input_requests SET status='answered', answer=?, updated_at=? WHERE request_id=?",
                    (self._dumps(answer), time.time(), request_id),
                )
            self.append_event("input.answered", {"request_id": request_id}, task_id=existing["task_id"])
        return self.get_input_request(request_id)  # type: ignore[return-value]
