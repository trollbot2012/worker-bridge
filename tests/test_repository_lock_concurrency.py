"""RepositoryLock concurrency regression tests (cross-process, deterministic).

Reproductions for the two reported defects, plus the invariants they must
keep after the fix:

1. Stale-sweep race (cross-process, forced interleaving):
   two contenders both observe a planted stale lock; the losing contender's
   sweep is forced to run *after* the winner recreated the lock, so a
   check-then-unlink implementation deletes a live lock and both processes
   end up inside the critical section.
2. Leaked in-process thread lock: exceptions between thread-lock acquisition
   and cross-process ownership (os.open / os.write failures) used to escape
   without releasing the thread lock.

Determinism: all cross-process coordination uses marker files and barriers
injected at the exact code boundary under test — never sleep-and-hope.
"""

from __future__ import annotations

import errno
import os
import threading
import time
from pathlib import Path

import pytest

from worker_bridge import workspace
from worker_bridge.workspace import RepositoryLock, WorkspaceError

from lock_test_helpers import await_file, child_config, outcome, release_and_join, spawn

OPERATION = "concurrency-test"


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    repo.mkdir()
    return repo


def _plant_stale_lock(repo: Path) -> None:
    probe = RepositoryLock(repo, operation=OPERATION)
    probe._path.parent.mkdir(parents=True, exist_ok=True)
    probe._path.write_text("999999999", encoding="ascii")


# --- Finding 1: the stale-sweep race -----------------------------------------


def test_two_contenders_never_both_hold_after_stale_sweep(tmp_path: Path, repo: Path) -> None:
    # Both contenders synchronize at the stale-pid observation; contender 1's
    # sweep is then forced to run only after contender 0 holds the lock, so a
    # check-then-unlink implementation necessarily deletes a live lock.
    _plant_stale_lock(repo)
    child0 = spawn(child_config(
        tmp_path, repo=repo, operation=OPERATION, wait_seconds=0,
        id="0", peer="1", barrier_pid_exists=True))
    child1 = spawn(child_config(
        tmp_path, repo=repo, operation=OPERATION, wait_seconds=0,
        id="1", peer="0", barrier_pid_exists=True, wait_peer_created_then_unlink=True))
    try:
        outcomes = [outcome(tmp_path, "0"), outcome(tmp_path, "1")]
    finally:
        release_and_join(tmp_path, [child0, child1])

    acquired = [o["acquired"] for o in outcomes]
    assert acquired.count(True) == 1, f"mutual exclusion violated, both contenders hold: {outcomes}"
    loser = next(o for o in outcomes if not o["acquired"])
    assert "repository lock exists" in (loser["error"] or "")
    holding = [p for p in (tmp_path / "signals").glob("holding-*") if p.exists()]
    assert len(holding) == 1, "exactly one contender may enter the critical section"


def test_waiter_does_not_delete_replacement_lock(tmp_path: Path, repo: Path) -> None:
    # Symmetric variant of the race: both contenders carry a wait budget, so
    # the loser genuinely retries while the winner holds — and whichever
    # child wins, the winner must stay undisturbed until released. The test
    # must not assume which contender wins the (fair) initial claim.
    _plant_stale_lock(repo)
    child0 = spawn(child_config(
        tmp_path, repo=repo, operation=OPERATION, wait_seconds=1,
        id="0", peer="1", barrier_pid_exists=True))
    child1 = spawn(child_config(
        tmp_path, repo=repo, operation=OPERATION, wait_seconds=1,
        id="1", peer="0", barrier_pid_exists=True, wait_peer_created_then_unlink=True))
    try:
        outcomes = [outcome(tmp_path, "0"), outcome(tmp_path, "1")]
    finally:
        release_and_join(tmp_path, [child0, child1])

    acquired = [o for o in outcomes if o["acquired"]]
    failed = [o for o in outcomes if not o["acquired"]]
    assert len(acquired) == 1 and len(failed) == 1, outcomes
    assert "repository lock exists" in (failed[0]["error"] or "")
    # The winner survived the loser's failed attempts: it was still holding
    # when released (its outcome was written from inside the critical section).
    assert (tmp_path / "signals" / f"holding-{acquired[0]['id']}").exists()


# --- Finding 2: leaked in-process thread lock ---------------------------------


def _thread_lock_for(repo: Path, operation: str = "leak-test") -> threading.Lock:
    return RepositoryLock(repo, operation=operation)._thread_lock


def test_failed_open_releases_thread_lock(repo: Path, monkeypatch) -> None:
    def injected_open(*_args, **_kwargs):
        raise OSError(13, "injected os.open failure")

    monkeypatch.setattr(workspace.os, "open", injected_open)
    with pytest.raises(OSError, match="injected"):
        with RepositoryLock(repo, operation="leak-test"):
            pass
    monkeypatch.undo()

    thread_lock = _thread_lock_for(repo)
    assert not thread_lock.locked(), "thread lock leaked by a failed acquisition"
    assert thread_lock.acquire(blocking=False), "same key must be immediately re-lockable"
    thread_lock.release()
    with RepositoryLock(repo, operation="leak-test"):
        pass  # full acquisition works right after the failure


def test_failed_write_does_not_break_acquisition_or_leak(repo: Path, monkeypatch) -> None:
    def injected_write(*_args, **_kwargs):
        raise OSError(5, "injected os.write failure")

    monkeypatch.setattr(workspace.os, "write", injected_write)
    # Ownership must not depend on the diagnostic pid write: acquisition
    # succeeds, the failure is contained, and nothing leaks.
    with RepositoryLock(repo, operation="leak-test"):
        pass
    monkeypatch.undo()

    assert not _thread_lock_for(repo).locked(), "thread lock leaked despite successful ownership"
    with RepositoryLock(repo, operation="leak-test"):
        pass


# --- Preserved invariants ------------------------------------------------------


def test_live_foreign_holder_blocks_fail_fast_and_survives(tmp_path: Path, repo: Path) -> None:
    child = spawn(child_config(
        tmp_path, repo=repo, operation=OPERATION, wait_seconds=5, id="0", peer="1"))
    try:
        await_file(tmp_path / "signals" / "holding-0")
        with pytest.raises(WorkspaceError, match="repository lock exists"):
            with RepositoryLock(repo, operation=OPERATION):
                pass
        assert (tmp_path / "signals" / "holding-0").exists(), "waiter disturbed a live holder"
    finally:
        release_and_join(tmp_path, [child])
    assert outcome(tmp_path, "0")["acquired"] is True
    with RepositoryLock(repo, operation=OPERATION):
        pass  # lock is reusable immediately after the holder exits


def test_wait_seconds_outlasts_a_transient_real_holder(tmp_path: Path, repo: Path) -> None:
    child = spawn(child_config(
        tmp_path, repo=repo, operation=OPERATION, wait_seconds=5, id="0", peer="1"))
    try:
        await_file(tmp_path / "signals" / "holding-0")
        started = time.monotonic()
        timer = threading.Timer(0.4, lambda: (tmp_path / "signals" / "release-0").write_text(""))
        timer.start()
        with RepositoryLock(repo, operation=OPERATION, wait_seconds=10):
            waited = time.monotonic() - started
        timer.join()
    finally:
        release_and_join(tmp_path, [child])
    assert waited >= 0.3, "waiter did not actually wait for the live holder"


def test_exit_releases_after_exception_in_block(repo: Path) -> None:
    lock = RepositoryLock(repo, operation="cleanup-test")
    with pytest.raises(ValueError, match="boom"):
        with lock:
            raise ValueError("boom")
    assert not lock._thread_lock.locked()
    with RepositoryLock(repo, operation="cleanup-test"):
        pass


def test_uncontended_acquisition_is_reusable(repo: Path) -> None:
    lock = RepositoryLock(repo, operation="uncontended-test")
    with lock:
        assert lock._fd is not None
    assert lock._fd is None
    assert not lock._thread_lock.locked()
    with RepositoryLock(repo, operation="uncontended-test"):
        pass


def test_lock_records_owner_pid_diagnostic(repo: Path) -> None:
    lock = RepositoryLock(repo, operation="diagnostic-test")
    with lock:
        pass
    # The file persists after release (it is a claim point, not a marker),
    # and on Windows the locked byte range is unreadable from other handles
    # while held — so diagnostics are read after release, when free.
    recorded = lock._path.read_text(encoding="ascii").strip()
    assert recorded == str(os.getpid()), "lock file should carry the owning pid for diagnostics"


# --- Cleanup failures must not leak the thread lock ---------------------------


def test_exit_unlock_failure_still_releases_thread_lock(repo: Path, monkeypatch) -> None:
    def failing_unlock(_fd: int) -> None:
        raise OSError(errno.EIO, "injected unlock failure")

    monkeypatch.setattr(workspace, "_release_lock_file", failing_unlock)
    lock = RepositoryLock(repo, operation="cleanup-test")
    with pytest.raises(OSError, match="injected unlock"):
        with lock:
            pass
    monkeypatch.undo()

    assert not lock._thread_lock.locked(), "unlock failure leaked the thread lock"
    with RepositoryLock(repo, operation="cleanup-test"):
        pass  # key remains usable


def test_exit_close_failure_still_releases_thread_lock(repo: Path, monkeypatch) -> None:
    real_close = workspace.os.close
    leaked: list[int] = []

    def failing_close(fd: int) -> None:
        leaked.append(fd)
        raise OSError(errno.EIO, "injected close failure")

    monkeypatch.setattr(workspace.os, "close", failing_close)
    lock = RepositoryLock(repo, operation="cleanup-test")
    with pytest.raises(OSError, match="injected close"):
        with lock:
            pass
    monkeypatch.undo()
    real_close(leaked[0])  # finish the cleanup the injection interrupted

    assert not lock._thread_lock.locked(), "close failure leaked the thread lock"
    with RepositoryLock(repo, operation="cleanup-test"):
        pass


def test_failed_acquisition_with_failing_cleanup_preserves_primary_error(
    repo: Path, monkeypatch
) -> None:
    # Partial initialization followed by cleanup failure: the claim error is
    # the real cause and must survive; the thread lock must still be freed.
    real_close = workspace.os.close
    leaked: list[int] = []

    def bad_claim(_fd: int) -> None:
        raise OSError(errno.EBADF, "injected invalid descriptor")

    def failing_close(fd: int) -> None:
        leaked.append(fd)
        raise OSError(errno.EIO, "injected close failure")

    monkeypatch.setattr(workspace, "_claim_lock_file", bad_claim)
    monkeypatch.setattr(workspace.os, "close", failing_close)
    with pytest.raises(OSError) as caught:
        with RepositoryLock(repo, operation="cleanup-test"):
            pass
    monkeypatch.undo()
    real_close(leaked[0])

    assert caught.value.errno == errno.EBADF, "primary claim error was replaced by cleanup noise"
    assert not _thread_lock_for(repo, "cleanup-test").locked()


# --- Claim-error classification -------------------------------------------------


def test_genuine_claim_failure_keeps_cause(repo: Path, monkeypatch) -> None:
    def bad_claim(_fd: int) -> None:
        raise OSError(errno.EBADF, "injected invalid descriptor")

    monkeypatch.setattr(workspace, "_claim_lock_file", bad_claim)
    with pytest.raises(OSError) as caught:
        with RepositoryLock(repo, operation="claim-test"):
            pass
    monkeypatch.undo()

    assert caught.value.errno == errno.EBADF, "genuine claim failure must not masquerade as busy"
    assert not _thread_lock_for(repo, "claim-test").locked()
    with RepositoryLock(repo, operation="claim-test"):
        pass


def test_contention_claim_failure_maps_to_busy(repo: Path, monkeypatch) -> None:
    def contended_claim(_fd: int) -> None:
        raise BlockingIOError(errno.EAGAIN, "injected contention")

    monkeypatch.setattr(workspace, "_claim_lock_file", contended_claim)
    with pytest.raises(WorkspaceError, match="repository lock exists"):
        with RepositoryLock(repo, operation="claim-test", wait_seconds=0):
            pass
    monkeypatch.undo()

    assert not _thread_lock_for(repo, "claim-test").locked()
    with RepositoryLock(repo, operation="claim-test"):
        pass


def test_claim_error_classification_by_platform(monkeypatch) -> None:
    # POSIX: EAGAIN (BlockingIOError) and EACCES are contention; EBADF/EINVAL
    # are not. Windows: EACCES/EDEADLK are contention; EBADF/EINVAL are not.
    monkeypatch.setattr(workspace.os, "name", "posix")
    assert workspace._is_lock_contention(BlockingIOError(errno.EAGAIN, "x"))
    assert workspace._is_lock_contention(OSError(errno.EACCES, "x"))
    assert not workspace._is_lock_contention(OSError(errno.EBADF, "x"))
    assert not workspace._is_lock_contention(OSError(errno.EINVAL, "x"))
    monkeypatch.setattr(workspace.os, "name", "nt")
    assert workspace._is_lock_contention(OSError(errno.EACCES, "x"))
    assert not workspace._is_lock_contention(OSError(errno.EBADF, "x"))
    assert not workspace._is_lock_contention(OSError(errno.EINVAL, "x"))


# --- Documented in-process waiting floor ---------------------------------------


def test_in_process_contention_waits_for_holder_then_acquires(repo: Path) -> None:
    # Same key, same process: the second acquisition waits on the thread lock
    # (documented >=30s budget) instead of failing fast, and succeeds once
    # the holder releases — verified without waiting anywhere near 30s.
    holding, release = threading.Event(), threading.Event()
    result: dict[str, object] = {}

    def holder() -> None:
        with RepositoryLock(repo, operation="thread-test"):
            holding.set()
            release.wait(10)

    def waiter() -> None:
        try:
            with RepositoryLock(repo, operation="thread-test"):
                result["waited"] = time.monotonic() - result["started"]  # type: ignore[operator]
        except Exception as exc:  # noqa: BLE001 - surface any failure shape
            result["error"] = exc

    t_holder = threading.Thread(target=holder)
    t_holder.start()
    assert holding.wait(5), "holder never acquired"
    result["started"] = time.monotonic()
    t_waiter = threading.Thread(target=waiter)
    t_waiter.start()
    time.sleep(0.3)  # let the waiter actually contend, not race the holder
    release.set()
    t_holder.join(10)
    t_waiter.join(10)

    assert "error" not in result, f"in-process contention must wait, not fail: {result}"
    assert result["waited"] >= 0.25, f"waiter did not wait for the holder: {result}"
