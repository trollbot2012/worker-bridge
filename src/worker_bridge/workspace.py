"""Isolated workspace creation, diff capture, locking, and verification."""

from __future__ import annotations

import fnmatch
import hashlib
import os
import shutil
import subprocess
import threading
from pathlib import Path
from typing import Any

import psutil

from worker_bridge.environ import get_home as get_hermes_home


class WorkspaceError(RuntimeError):
    pass


_PROCESS_LOCKS: dict[str, threading.Lock] = {}
_PROCESS_LOCKS_GUARD = threading.Lock()


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


class RepositoryLock:
    """Process and cross-process lock used by direct-workspace mode."""

    def __init__(self, repository: str | Path) -> None:
        resolved = str(Path(repository).resolve())
        with _PROCESS_LOCKS_GUARD:
            self._thread_lock = _PROCESS_LOCKS.setdefault(resolved, threading.Lock())
        safe = hashlib.sha256(resolved.encode("utf-8", "replace")).hexdigest()[:24]
        self._path = get_hermes_home() / "workers" / "locks" / f"{safe}.lock"
        self._fd: int | None = None

    def __enter__(self) -> "RepositoryLock":
        if not self._thread_lock.acquire(timeout=30):
            raise WorkspaceError("repository is busy")
        self._path.parent.mkdir(parents=True, exist_ok=True)
        for attempt in range(2):
            try:
                self._fd = os.open(self._path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.write(self._fd, str(os.getpid()).encode())
                break
            except FileExistsError as exc:
                stale = False
                try:
                    pid = int(self._path.read_text(encoding="ascii").strip())
                    stale = not psutil.pid_exists(pid)
                except (OSError, ValueError):
                    stale = True
                if stale and attempt == 0:
                    self._path.unlink(missing_ok=True)
                    continue
                self._thread_lock.release()
                raise WorkspaceError(f"repository lock exists: {self._path}") from exc
        return self

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

    def prepare(self, task_id: str, spec: dict[str, Any]) -> dict[str, Any]:
        workspace = spec["workspace"]
        repository = Path(workspace["repository"]).expanduser().resolve()
        isolation = workspace.get("isolation", "git_worktree")
        base_ref = workspace.get("base_ref") or "HEAD"
        if isolation == "direct":
            return {
                "repository": str(repository),
                "path": str(Path(workspace.get("working_directory") or repository).resolve()),
                "isolation": "direct",
                "base_commit": _git(repository, "rev-parse", base_ref),
                "branch": _git(repository, "branch", "--show-current"),
            }
        destination = self.root / task_id
        if destination.exists():
            raise WorkspaceError(f"workspace already exists: {destination}")
        base_commit = _git(repository, "rev-parse", base_ref)
        if isolation == "copy":
            shutil.copytree(repository, destination, ignore=shutil.ignore_patterns(".git"))
            return {
                "repository": str(repository),
                "path": str(destination),
                "isolation": "copy",
                "base_commit": base_commit,
                "branch": None,
            }
        branch = f"codex/worker-{task_id}"
        _git(repository, "worktree", "add", "-b", branch, str(destination), base_commit, timeout=120)
        return {
            "repository": str(repository),
            "path": str(destination),
            "isolation": "git_worktree",
            "base_commit": base_commit,
            "branch": branch,
        }

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
