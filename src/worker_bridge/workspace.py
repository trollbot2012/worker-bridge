"""Isolated workspace creation, diff capture, locking, and verification."""

from __future__ import annotations

import fnmatch
import hashlib
import os
import shutil
import subprocess
import threading
import time
from pathlib import Path
from typing import Any

import psutil

from worker_bridge.environ import get_home as get_hermes_home


class WorkspaceError(RuntimeError):
    pass


_PROCESS_LOCKS: dict[str, threading.Lock] = {}
_PROCESS_LOCKS_GUARD = threading.Lock()

# Names never worth copying into a task workspace: version-control internals,
# dependency/build caches, and the bridge's own runtime state. Copying these is
# what turns a 2 GB repo into a 2 GB-per-task disk bomb and makes copytree hang
# for minutes.
_COPY_EXCLUDED_DIRECTORIES = frozenset({
    ".git", ".gradle", ".mypy_cache", ".next", ".pytest_cache", ".ruff_cache",
    ".turbo", ".venv", "__pycache__", "artifacts", "backups", "build", "cache",
    "caches", "checkpoints", "dist", "logs", "node_modules", "sessions",
    "target", "temp", "tmp", "venv", "workers", "worktrees",
})
_COPY_EXCLUDED_STATE_FILES = frozenset({
    "bridge.db", "kanban.db", "receipts.db", "response_store.db",
    "sessions.db", "state.db", "verification_evidence.db",
})


def _copy_ignore(directory: str, names: list[str]) -> set[str]:
    """shutil.copytree ignore callback: drop runtime state at every depth."""
    ignored: set[str] = set()
    for name in names:
        lowered = name.casefold()
        if lowered in _COPY_EXCLUDED_DIRECTORIES:
            ignored.add(name)
        elif lowered in _COPY_EXCLUDED_STATE_FILES or lowered.endswith((".db-wal", ".db-shm", ".pyc")):
            ignored.add(name)
    return ignored


# A copy workspace larger than this almost certainly means the wrong
# isolation mode (whole-home or a repo full of build output). Refuse
# early with an actionable error instead of silently filling the disk.
_COPY_MAX_BYTES = 2 * 1024**3           # 2 GiB
_COPY_MAX_FILES = 50_000
_COPY_MIN_FREE_BYTES = 10 * 1024**3     # never leave the disk below this…
_COPY_MIN_FREE_RATIO = 0.05             # …or below this fraction of capacity


def _measure_copy_source(source: Path) -> int:
    """Sum the byte size of what a guarded copy would actually copy.

    Honors the exclusion list, refuses symlinks/junctions anywhere in the
    tree (a link pointing back up the hierarchy re-creates the recursive
    self-copy amplification even with the name filters), and aborts early
    once the budget is blown so a pathological tree cannot make measurement
    itself take forever.
    """
    import stat as _stat
    reparse_flag = getattr(_stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    total = 0
    files = 0
    pending = [source]
    while pending:
        directory = pending.pop()
        try:
            entries = list(os.scandir(directory))
        except OSError as exc:
            raise WorkspaceError(f"cannot inspect copy source {directory}: {exc}") from exc
        ignored = _copy_ignore(str(directory), [entry.name for entry in entries])
        for entry in entries:
            if entry.name in ignored:
                continue
            try:
                info = entry.stat(follow_symlinks=False)
            except OSError as exc:
                raise WorkspaceError(f"cannot inspect copy source {entry.path}: {exc}") from exc
            if entry.is_symlink() or (
                reparse_flag and getattr(info, "st_file_attributes", 0) & reparse_flag
            ):
                raise WorkspaceError(
                    f"copy isolation refuses symlinks or junctions: {entry.path}"
                )
            if entry.is_dir(follow_symlinks=False):
                pending.append(Path(entry.path))
                continue
            files += 1
            total += max(0, int(info.st_size))
            if total > _COPY_MAX_BYTES or files > _COPY_MAX_FILES:
                raise WorkspaceError(
                    f"copy isolation refused: source {source} exceeds "
                    f"{_COPY_MAX_BYTES // 1024**3} GiB / {_COPY_MAX_FILES} files "
                    "even after ignoring caches. Use --isolation git_worktree "
                    "(cheap, shares git objects) or --isolation direct "
                    "(edit in place under a repository lock) instead."
                )
    return total


def _guarded_copytree(source: Path, destination: Path) -> None:
    size = _measure_copy_source(source)
    try:
        usage = shutil.disk_usage(destination.parent)
    except OSError:
        usage = None
    if usage is not None:
        # Reserve semantics: the copy must leave at least
        # max(10 GiB, 5% of capacity) free — a delegation can never be the
        # thing that runs the system drive to the wall.
        reserve = max(_COPY_MIN_FREE_BYTES, int(usage.total * _COPY_MIN_FREE_RATIO))
        if usage.free - size < reserve:
            raise WorkspaceError(
                "copy isolation would violate the disk free-space reserve: "
                f"free={usage.free}, input={size}, required_reserve={reserve}"
            )
    shutil.copytree(source, destination, ignore=_copy_ignore)


def _run(args: list[str], cwd: Path, timeout: int = 60) -> subprocess.CompletedProcess[str]:
    # Decode git output as UTF-8 explicitly. `text=True` alone uses the locale
    # encoding, which on a non-UTF-8 Windows console (cp1252) mangles non-ASCII
    # filenames git emits as UTF-8 bytes — corrupting changed_files/diff/verify.
    return subprocess.run(
        args,
        cwd=cwd,
        encoding="utf-8",
        errors="replace",
        capture_output=True,
        timeout=timeout,
        stdin=subprocess.DEVNULL,
        check=False,
    )


def _git(repository: Path, *args: str, timeout: int = 60) -> str:
    proc = _run(["git", *args], repository, timeout)
    if proc.returncode:
        raise WorkspaceError(proc.stderr.strip() or proc.stdout.strip() or "git command failed")
    return proc.stdout.strip()


def _reftx_hook_present(repository: Path) -> bool:
    """True if a reference-transaction hook governs *repository*.

    The hook lives in the git *common* dir, which is not ``repository/.git``
    when the target is itself a worktree or a subdir of a larger repo. Resolve
    it via git so the gate is detected regardless of layout.
    """
    proc = _run(["git", "rev-parse", "--git-common-dir"], repository)
    if proc.returncode:
        return False
    common = Path(proc.stdout.strip())
    if not common.is_absolute():
        common = (repository / common).resolve()
    return (common / "hooks" / "reference-transaction").exists()


class RepositoryLock:
    """Process and cross-process lock used by direct-workspace mode.

    ``operation`` scopes the lock to one kind of work: concurrent operations
    of *different* kinds on the same repository (e.g. a short worktree-setup
    vs. a direct-mode worker holding the plain lock for its whole run) must
    not serialize on one global per-repo lock. Same operation on the same
    repository still excludes.

    ``wait_seconds`` bounds how long acquisition waits for a live holder to
    release before raising. The default (0) keeps the historical fail-fast
    behavior direct mode relies on.
    """

    def __init__(
        self,
        repository: str | Path,
        *,
        operation: str | None = None,
        wait_seconds: float = 0.0,
    ) -> None:
        resolved = str(Path(repository).resolve())
        key = f"{resolved}::{operation}" if operation else resolved
        with _PROCESS_LOCKS_GUARD:
            self._thread_lock = _PROCESS_LOCKS.setdefault(key, threading.Lock())
        safe = hashlib.sha256(key.encode("utf-8", "replace")).hexdigest()[:24]
        self._path = get_hermes_home() / "workers" / "locks" / f"{safe}.lock"
        self._wait_seconds = max(0.0, float(wait_seconds))
        self._fd: int | None = None

    def __enter__(self) -> "RepositoryLock":
        if not self._thread_lock.acquire(timeout=max(30.0, self._wait_seconds)):
            raise WorkspaceError("repository is busy")
        self._path.parent.mkdir(parents=True, exist_ok=True)
        deadline = time.monotonic() + self._wait_seconds
        swept_stale = False
        while True:
            try:
                self._fd = os.open(self._path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.write(self._fd, str(os.getpid()).encode())
                return self
            except FileExistsError as exc:
                stale = False
                try:
                    pid = int(self._path.read_text(encoding="ascii").strip())
                    stale = not psutil.pid_exists(pid)
                except (OSError, ValueError):
                    stale = True
                if stale and not swept_stale:
                    swept_stale = True
                    self._path.unlink(missing_ok=True)
                    continue
                if time.monotonic() < deadline:
                    time.sleep(0.2)
                    # The holder may die while we wait; allow another sweep.
                    swept_stale = False
                    continue
                self._thread_lock.release()
                raise WorkspaceError(f"repository lock exists: {self._path}") from exc

    def __exit__(self, *_exc: Any) -> None:
        if self._fd is not None:
            os.close(self._fd)
            self._fd = None
        self._path.unlink(missing_ok=True)
        self._thread_lock.release()


class WorkspaceManager:
    def __init__(self, root: str | Path | None = None) -> None:
        self.root = Path(root or (get_hermes_home() / "workers" / "worktrees")).resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def plan(self, task_id: str, spec: dict[str, Any]) -> dict[str, Any]:
        """Validate and describe an allocation WITHOUT touching the filesystem.

        Everything the orchestrator needs to persist (repository, destination,
        isolation, branch) is decided here, so a crash or kill during the
        subsequent allocate() always leaves a task record that names the
        directory it was building — an interrupted copy is otherwise
        unfindable because nothing is recorded until after copytree finishes.
        allocation_state: 'allocating' -> allocate() -> 'ready'.
        """
        workspace = spec["workspace"]
        repository = Path(workspace["repository"]).expanduser().resolve()
        isolation = workspace.get("isolation", "git_worktree")
        base_ref = workspace.get("base_ref") or "HEAD"
        if isolation == "direct":
            return self._direct_runtime(repository, workspace, base_ref)

        # Recursive-containment rule (path math, not name matching): the
        # worker root must never sit inside the source being copied, and the
        # source must never sit inside the worker root. A tree whose
        # workers/worktrees lives under it otherwise copies every earlier
        # task's copy into each new task — unbounded disk amplification.
        if self.root == repository or self.root.is_relative_to(repository) or repository.is_relative_to(self.root):
            raise WorkspaceError(
                "recursive workspace containment is forbidden: repository "
                f"{repository} and worker root {self.root} overlap. Choose a "
                "narrow project root or direct isolation."
            )
        if isolation == "copy":
            home = get_hermes_home().resolve()
            home_scoped = home == repository or home.is_relative_to(repository)
            if home_scoped and not workspace.get("allow_profile_copy"):
                raise WorkspaceError(
                    "copy isolation refused: repository is at or above the "
                    f"bridge home ({home}). The bridge home is state, not a "
                    "project. Use direct isolation, a narrow project "
                    "subdirectory, or set workspace.allow_profile_copy "
                    "explicitly."
                )
        destination = self.root / task_id
        if destination.exists():
            raise WorkspaceError(f"workspace already exists: {destination}")
        base_commit = _git(repository, "rev-parse", base_ref)
        return {
            "repository": str(repository),
            "path": str(destination),
            "isolation": isolation,
            "base_commit": base_commit,
            "branch": f"codex/worker-{task_id}" if isolation == "git_worktree" else None,
            "allocation_state": "allocating",
        }

    def allocate(self, runtime: dict[str, Any]) -> dict[str, Any]:
        """Perform the filesystem mutation described by a plan() runtime."""
        if runtime.get("allocation_state") != "allocating":
            return runtime
        repository = Path(runtime["repository"])
        destination = Path(runtime["path"])
        runtime = dict(runtime)
        if runtime["isolation"] == "copy":
            _guarded_copytree(repository, destination)
            runtime["allocation_state"] = "ready"
            return runtime
        branch = runtime["branch"]
        try:
            _git(repository, "worktree", "add", "-b", branch, str(destination),
                 runtime["base_commit"], timeout=120)
        except WorkspaceError:
            # A governance hook (e.g. a reference-transaction hook enforcing a
            # pipeline gate) can abort branch-ref creation when the target repo
            # is the gated repo itself. Rather than fail the task or silently
            # fall back to a whole-tree copy, degrade to in-place `direct`
            # isolation — the orchestrator serializes direct work with a
            # RepositoryLock, and no branch ref is created so the gate is not
            # tripped. External repos (no such hook) never reach this path.
            if not _reftx_hook_present(repository):
                raise
            self._discard_partial_worktree(repository, destination, branch)
            fallback = self._direct_runtime(
                repository, {"working_directory": None}, runtime["base_commit"]
            )
            fallback["isolation_requested"] = "git_worktree"
            fallback["isolation_fallback"] = "gate_blocked_worktree_ref"
            return fallback
        runtime["allocation_state"] = "ready"
        return runtime

    def prepare(self, task_id: str, spec: dict[str, Any]) -> dict[str, Any]:
        return self.allocate(self.plan(task_id, spec))

    def cleanup_partial(self, runtime: dict[str, Any]) -> None:
        """Remove whatever a failed/interrupted allocate() left on disk."""
        path = Path(runtime.get("path") or "")
        if not str(path) or runtime.get("isolation") == "direct":
            return
        if not path.resolve().is_relative_to(self.root):
            return
        if runtime.get("isolation") == "git_worktree":
            self._discard_partial_worktree(
                Path(runtime["repository"]), path, runtime.get("branch") or ""
            )
        elif path.exists():
            self.cleanup({**runtime, "isolation": "copy"})

    def discover_orphans(self, known_paths: set[str]) -> list[str]:
        """Directories under the worker root that no task record references."""
        known = {str(Path(p).resolve()) for p in known_paths if p}
        orphans = []
        for entry in self.root.iterdir():
            if entry.is_dir() and str(entry.resolve()) not in known:
                orphans.append(str(entry))
        return sorted(orphans)

    def cleanup_orphan(self, path: str | Path) -> None:
        """Delete an orphan directory strictly under the worker root.

        Refuses symlinks/junctions (never follow a link out of the root) and
        anything that resolves outside root — a defense against a planted link
        turning cleanup into deletion of an unrelated tree."""
        target = Path(path)
        if target.is_symlink() or (os.name == "nt" and target.exists()
                                   and os.path.islink(str(target))):
            raise WorkspaceError(f"refusing to remove a symlink/junction: {target}")
        resolved = target.resolve()
        if resolved == self.root or not resolved.is_relative_to(self.root):
            raise WorkspaceError(f"refusing orphan cleanup outside worker root: {resolved}")
        # Prefer git's own worktree removal when it is a registered worktree.
        git_file = resolved / ".git"
        if git_file.exists():
            for repo_hint in (resolved,):
                try:
                    _git(repo_hint, "worktree", "remove", "--force", str(resolved))
                    return
                except WorkspaceError:
                    break
        shutil.rmtree(resolved, ignore_errors=True)

    @staticmethod
    def _direct_runtime(repository: Path, workspace: dict[str, Any], base_ref: str) -> dict[str, Any]:
        return {
            "repository": str(repository),
            "path": str(Path(workspace.get("working_directory") or repository).resolve()),
            "isolation": "direct",
            "base_commit": _git(repository, "rev-parse", base_ref),
            "branch": _git(repository, "branch", "--show-current"),
        }

    @staticmethod
    def _discard_partial_worktree(repository: Path, destination: Path, branch: str) -> None:
        """Best-effort cleanup after an aborted `worktree add` so the retry and
        future tasks are not blocked by a half-registered worktree or branch.

        `worktree remove --force` deletes the working directory git created;
        `prune` clears any dangling registration. Task ids are unique, so a
        stray directory (if git left one) never collides with a later task."""
        for args in (
            ["worktree", "remove", "--force", str(destination)],
            ["worktree", "prune"],
            ["branch", "-D", branch],
        ):
            try:
                _git(repository, *args)
            except WorkspaceError:
                pass

    @staticmethod
    def _status_entries(path: Path) -> list[tuple[str, str]]:
        """Parse ``git status`` as (XY, path) pairs using NUL framing.

        NUL framing (``-z``) is mandatory here: the human-readable porcelain
        C-quotes paths containing spaces/unicode, and ``_git`` strips the
        leading space of the first ``␣M`` line, which silently corrupted the
        first changed filename and let a worker evade lexical forbidden-path
        checks by arranging for its file to sort first.
        """
        proc = _run(
            ["git", "status", "--porcelain=v1", "-z", "--untracked-files=all"], path
        )
        if proc.returncode:
            raise WorkspaceError(proc.stderr.strip() or "git status failed")
        tokens = proc.stdout.split("\0")
        entries: list[tuple[str, str]] = []
        index = 0
        while index < len(tokens):
            token = tokens[index]
            index += 1
            if len(token) < 4:
                continue
            xy, name = token[:2], token[3:]
            if xy[:1] in {"R", "C"}:
                # Rename/copy: the source path is the next NUL-framed field.
                index += 1
            entries.append((xy, name))
        return entries

    def changed_files(self, runtime: dict[str, Any]) -> list[str]:
        path = Path(runtime["path"])
        if runtime.get("isolation") == "copy":
            return sorted(str(p.relative_to(path)).replace("\\", "/") for p in path.rglob("*") if p.is_file())
        return sorted({name.replace("\\", "/") for _xy, name in self._status_entries(path)})

    def escaping_paths(self, runtime: dict[str, Any], changed_files: list[str]) -> list[str]:
        """Return changed paths whose real location is outside the workspace.

        Catches symlink/junction escapes: a worker can create an in-tree link
        whose target resolves outside the assigned worktree, which ``git status``
        reports as an ordinary in-tree change. This is best-effort defence, not a
        sandbox — a hostile worker with real write authority still needs a
        client-native sandbox or container (see docs/worker-bridge.md).
        """
        root = Path(runtime["path"]).resolve()
        prefix = str(root) + os.sep
        escapes: list[str] = []
        for name in changed_files:
            candidate = root / name
            try:
                real = Path(os.path.realpath(candidate))
            except OSError:
                continue
            if real != root and not str(real).startswith(prefix):
                escapes.append(name)
        return sorted(set(escapes))

    def diff(self, runtime: dict[str, Any], maximum_bytes: int = 2_000_000) -> str:
        if runtime.get("isolation") == "copy":
            return ""
        path = Path(runtime["path"])
        tracked = _git(path, "diff", "--binary", runtime["base_commit"], timeout=120)
        untracked = [name for xy, name in self._status_entries(path) if xy == "??"]
        untracked_parts = []
        for name in untracked:
            proc = _run(["git", "diff", "--no-index", "--binary", "--", os.devnull, name], path)
            if proc.returncode in (0, 1) and "new file mode" in proc.stdout:
                untracked_parts.append(proc.stdout)
        value = tracked + "\n" + "\n".join(untracked_parts)
        encoded = value.encode("utf-8", "replace")
        if len(encoded) > maximum_bytes:
            return encoded[:maximum_bytes].decode("utf-8", "replace") + "\n[diff truncated]"
        return value

    def verify(self, runtime: dict[str, Any], verification: dict[str, Any], timeout: int) -> dict[str, Any]:
        path = Path(runtime["path"])
        changed = self.changed_files(runtime)
        forbidden = [
            name
            for name in changed
            if any(fnmatch.fnmatch(name, pattern) or name.startswith(pattern.rstrip("/") + "/") for pattern in verification.get("forbidden_paths", []))
        ]
        commands = []
        ok = not forbidden
        for command in verification.get("commands", []):
            try:
                proc = subprocess.run(
                    command,
                    cwd=path,
                    shell=True,
                    text=True,
                    capture_output=True,
                    timeout=timeout,
                    stdin=subprocess.DEVNULL,
                )
                record = {
                    "command": command,
                    "exit_code": proc.returncode,
                    "stdout": proc.stdout[-20_000:],
                    "stderr": proc.stderr[-20_000:],
                    "owner": "worker-bridge",
                }
            except subprocess.TimeoutExpired as exc:
                record = {
                    "command": command,
                    "exit_code": None,
                    "stdout": str(exc.stdout or "")[-20_000:],
                    "stderr": "verification timed out",
                    "owner": "worker-bridge",
                }
            commands.append(record)
            ok = ok and record["exit_code"] == 0
        return {"ok": ok, "commands": commands, "changed_files": changed, "forbidden_files": forbidden}

    def integrate(self, runtime: dict[str, Any], repository: str | Path) -> list[str]:
        """Copy an isolated git worktree's changes into its source repository."""
        if runtime.get("isolation") == "direct":
            return []
        if runtime.get("isolation") != "git_worktree":
            raise WorkspaceError("only git worktrees can be integrated on acceptance")

        worktree = Path(runtime["path"]).resolve()
        destination = Path(repository).expanduser().resolve()
        expected = Path(runtime["repository"]).resolve()
        if destination != expected:
            raise WorkspaceError("task repository does not match the prepared workspace")

        proc = _run(
            ["git", "diff", "--name-only", "--no-renames", "-z", runtime["base_commit"]],
            worktree,
            timeout=120,
        )
        if proc.returncode:
            raise WorkspaceError(proc.stderr.strip() or "git diff failed")
        changed = {name for name in proc.stdout.split("\0") if name}
        changed.update(name for xy, name in self._status_entries(worktree) if xy == "??")

        for name in sorted(changed):
            source = worktree / name
            target = destination / name
            if source.is_symlink():
                raise WorkspaceError(f"refusing to integrate symlink: {name}")
            if source.is_file():
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source, target)
            else:
                target.unlink(missing_ok=True)
        return sorted(name.replace("\\", "/") for name in changed)

    def apply_diff(self, runtime: dict[str, Any], diff_path: str | Path) -> dict[str, Any]:
        path = Path(runtime["path"])
        patch = Path(diff_path).resolve()
        # --ignore-whitespace: diffs are captured LF-normalized by `git diff`,
        # but worktrees on Windows may be checked out CRLF via core.autocrlf.
        # Without this, `git apply` searches the CRLF working file for an
        # LF-context line and spuriously reports "patch does not apply" on
        # perfectly ordinary tracked-file edits, breaking integration. Trailing
        # CR is ignorable whitespace, so the 3-way apply matches correctly.
        proc = _run(
            ["git", "apply", "--3way", "--ignore-whitespace", "--whitespace=nowarn", str(patch)],
            path,
            timeout=120,
        )
        conflicts_proc = _run(["git", "diff", "--name-only", "--diff-filter=U"], path)
        conflicts = [line for line in conflicts_proc.stdout.splitlines() if line]
        return {
            "ok": proc.returncode == 0 and not conflicts,
            "diff": str(patch),
            "exit_code": proc.returncode,
            "stdout": proc.stdout[-20_000:],
            "stderr": proc.stderr[-20_000:],
            "conflicts": conflicts,
        }

    def cleanup(self, runtime: dict[str, Any]) -> None:
        """Remove only a bridge-owned isolated workspace; never direct mode."""
        isolation = runtime.get("isolation")
        path = Path(runtime["path"]).resolve()
        if isolation == "direct":
            raise WorkspaceError("direct workspaces are never removed by the bridge")
        if not path.is_relative_to(self.root):
            raise WorkspaceError(f"refusing cleanup outside worktree root: {path}")
        if isolation == "git_worktree":
            repository = Path(runtime["repository"])
            _git(repository, "worktree", "remove", "--force", str(path), timeout=120)
            branch = runtime.get("branch")
            if branch:
                _git(repository, "branch", "-D", branch)
        elif isolation == "copy":
            shutil.rmtree(path)
