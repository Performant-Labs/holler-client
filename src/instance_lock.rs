//! Single-instance guard for `holler run` (issue #52).
//!
//! Two `holler run` processes against the same `HOLLER_STATE_DIR` would
//! both try to authenticate with the same persisted credential and race
//! writes to `connection_state.json` — `holler status`/`holler detach`
//! then have no reliable way to know which process (if either) they're
//! actually observing. This module makes a second `run` refuse to start
//! instead.
//!
//! # Why `flock`, not a "does a PID file exist" check
//!
//! A PID file alone can't distinguish "another `run` is live" from "a
//! prior `run` crashed (`kill -9`, power loss) and left its PID file
//! behind" — the file looks the same either way, and a naive check would
//! treat the second case as the first, permanently blocking future runs
//! after any unclean shutdown. An advisory `flock` (`LOCK_EX | LOCK_NB`)
//! sidesteps this: the kernel releases the lock the instant the holding
//! process's file descriptors are closed, including on a crash — so a
//! stale lock from an unclean exit is never observably different from no
//! lock at all. This is the standard, uncontested pattern for exactly
//! this problem.
//!
//! # Doesn't affect `detach`
//!
//! `holler detach` never opens this lock — it only reads/writes
//! [`crate::connection::ConnectionStateStore`]'s own files (the detach
//! marker, the live-state file), so it keeps working against a live
//! `run` process exactly as before.

use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};

use crate::credential::{resolve_state_dir, CredentialError, STATE_DIR_ENV};

const LOCK_FILE: &str = "run.lock";

/// Held for the lifetime of a `holler run` process. Dropping it (including
/// on process exit or crash — the kernel does this for us) releases the
/// advisory lock.
pub struct InstanceLock {
    // Never read, but must stay alive: closing this file descriptor is
    // what releases the `flock`.
    _file: File,
    path: PathBuf,
}

/// Why [`InstanceLock::acquire`] (or [`InstanceLock::acquire_default`])
/// failed.
#[derive(Debug)]
pub enum InstanceLockError {
    /// Another `holler run` already holds the lock for this state dir.
    AlreadyRunning,
    /// Couldn't resolve which state dir to lock (see
    /// [`crate::credential::CredentialError::NoStateDir`]).
    StateDir(CredentialError),
    Io(io::Error),
}

impl std::fmt::Display for InstanceLockError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InstanceLockError::AlreadyRunning => write!(
                f,
                "another `holler run` is already active against this state dir"
            ),
            InstanceLockError::StateDir(e) => write!(f, "{e}"),
            InstanceLockError::Io(e) => write!(f, "instance lock I/O error: {e}"),
        }
    }
}

impl std::error::Error for InstanceLockError {}

impl From<io::Error> for InstanceLockError {
    fn from(e: io::Error) -> Self {
        InstanceLockError::Io(e)
    }
}

impl InstanceLock {
    /// Tries to acquire the single-instance lock for the state directory
    /// `dir` (the same directory `credential.json`/`connection_state.json`
    /// live in). Non-blocking: returns
    /// [`InstanceLockError::AlreadyRunning`] immediately if another live
    /// `run` holds it, rather than waiting.
    pub fn acquire(dir: &Path) -> Result<Self, InstanceLockError> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join(LOCK_FILE);
        // Never truncate: this file's only job is to be an `flock` target,
        // and truncating it on every `acquire` would race a concurrent
        // reader for no reason (there's no content to preserve or clear).
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&path)?;

        // SAFETY: `file`'s raw fd is valid for the duration of this call,
        // and `flock` only ever reads/writes kernel lock state for that
        // fd — it can't violate any of `file`'s own invariants.
        let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if rc != 0 {
            let err = io::Error::last_os_error();
            return match err.raw_os_error() {
                Some(libc::EWOULDBLOCK) => Err(InstanceLockError::AlreadyRunning),
                _ => Err(InstanceLockError::Io(err)),
            };
        }

        Ok(InstanceLock { _file: file, path })
    }

    /// [`Self::acquire`] against the same state dir
    /// [`crate::credential::CredentialStore::open`]/
    /// [`crate::connection::ConnectionStateStore::open`] resolve
    /// (`$HOLLER_STATE_DIR`, or `$HOME/.holler`) — what `holler run`
    /// actually calls.
    pub fn acquire_default() -> Result<Self, InstanceLockError> {
        let dir = resolve_state_dir(
            std::env::var(STATE_DIR_ENV).ok(),
            std::env::var("HOME").ok(),
        )
        .map_err(InstanceLockError::StateDir)?;
        Self::acquire(&dir)
    }
}

impl std::fmt::Debug for InstanceLock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InstanceLock")
            .field("path", &self.path)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn acquire_succeeds_when_uncontended() {
        let dir = tempfile::tempdir().unwrap();
        let lock = InstanceLock::acquire(dir.path());
        assert!(lock.is_ok());
    }

    #[test]
    fn second_acquire_in_same_process_fails_while_first_is_held() {
        let dir = tempfile::tempdir().unwrap();
        let first = InstanceLock::acquire(dir.path()).unwrap();
        let second = InstanceLock::acquire(dir.path());
        assert!(matches!(second, Err(InstanceLockError::AlreadyRunning)));
        drop(first);
    }

    #[test]
    fn acquire_succeeds_again_after_the_first_lock_is_dropped() {
        let dir = tempfile::tempdir().unwrap();
        let first = InstanceLock::acquire(dir.path()).unwrap();
        drop(first);
        let second = InstanceLock::acquire(dir.path());
        assert!(second.is_ok());
    }

    #[test]
    fn acquire_creates_the_state_dir_if_missing() {
        let dir = tempfile::tempdir().unwrap();
        let nested = dir.path().join("nested");
        assert!(!nested.exists());
        let lock = InstanceLock::acquire(&nested);
        assert!(lock.is_ok());
        assert!(nested.join(LOCK_FILE).exists());
    }
}
