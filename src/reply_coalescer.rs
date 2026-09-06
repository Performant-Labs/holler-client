//! Coalescing buffer for streamed `reply` chunks (issue #83).
//!
//! # Why
//!
//! Without buffering, one ACP `DriverEvent::Update` becomes one wire
//! frame. Measured against a real OpenCode turn, that is ~200 bytes of
//! frame per ~10-20 bytes of text — roughly 130 of those bytes (`v`,
//! `type`, `id`, `from`, `session`) are byte-identical on every chunk of
//! the same turn, so the preamble is re-sent once per chunk.
//!
//! `ReplyBody` already carries a `chunks: Vec<String>` field for exactly
//! this, and holler-server already joins `text` + `chunks` in arrival
//! order (`wire/registry.rs`, spec §10). So batching is purely a client
//! -side change: fill `chunks`, send fewer frames, and the far side
//! reassembles identically.
//!
//! # Policy
//!
//! Updates for a session accumulate until the first of:
//!
//! - the debounce window elapses since the *first* buffered chunk
//!   ([`ReplyCoalescer::due`]),
//! - the turn ends ([`ReplyCoalescer::take`] — called on `StopReason`),
//! - the buffer exceeds a byte cap ([`ReplyCoalescer::push`] returns the
//!   batch immediately).
//!
//! The window is measured from the first buffered chunk, not the most
//! recent one: a steady stream must not defer its flush indefinitely.
//!
//! # Time is a parameter, not a side effect
//!
//! Every method that needs "now" takes it as an argument, so the whole
//! policy is unit-testable without sleeping or a clock abstraction. The
//! caller (the session loop) supplies a real [`Instant`].

use std::collections::HashMap;
use std::time::{Duration, Instant};

/// How long chunks accumulate before being flushed as one frame.
///
/// 50ms is short enough to stay imperceptible in a streamed response and
/// long enough to batch meaningfully: on a measured OpenCode turn (chunks
/// ~21ms apart) it halves the frame count. A harness streaming faster
/// batches proportionally harder.
pub const DEFAULT_WINDOW: Duration = Duration::from_millis(50);

/// Flush early once a session's buffered text exceeds this, so a fast
/// stream produces several reasonable frames rather than one huge one.
/// Two orders of magnitude under holler-server's 2 MiB inbound frame cap.
pub const DEFAULT_MAX_BYTES: usize = 64 * 1024;

#[derive(Debug)]
struct Pending {
    chunks: Vec<String>,
    bytes: usize,
    /// When the *first* chunk of this batch arrived — the window is
    /// measured from here.
    opened_at: Instant,
}

/// Per-session batching of outbound reply chunks.
///
/// Holds no timer of its own: the caller asks [`next_deadline`] when to
/// wake up and calls [`due`] when it does.
///
/// [`next_deadline`]: ReplyCoalescer::next_deadline
/// [`due`]: ReplyCoalescer::due
#[derive(Debug)]
pub struct ReplyCoalescer {
    window: Duration,
    max_bytes: usize,
    pending: HashMap<String, Pending>,
}

impl Default for ReplyCoalescer {
    fn default() -> Self {
        Self::new(DEFAULT_WINDOW, DEFAULT_MAX_BYTES)
    }
}

impl ReplyCoalescer {
    pub fn new(window: Duration, max_bytes: usize) -> Self {
        ReplyCoalescer {
            window,
            max_bytes,
            pending: HashMap::new(),
        }
    }

    /// Buffers one streamed update for `session`.
    ///
    /// Returns `Some(chunks)` when the byte cap is reached and the batch
    /// must be sent immediately; `None` when it was buffered and will be
    /// released by [`due`] or [`take`]. The returned batch always
    /// includes `text` itself, so the caller never has to merge.
    #[must_use = "a returned batch must be sent, or its text is dropped"]
    pub fn push(&mut self, session: &str, text: String, now: Instant) -> Option<Vec<String>> {
        let entry = self.pending.entry(session.to_string()).or_insert(Pending {
            chunks: Vec::new(),
            bytes: 0,
            opened_at: now,
        });
        entry.bytes += text.len();
        entry.chunks.push(text);

        if entry.bytes >= self.max_bytes {
            // `remove` rather than draining in place: a flushed session
            // has no open window, so the next chunk must start a fresh
            // one from its own arrival time.
            return self.pending.remove(session).map(|p| p.chunks);
        }
        None
    }

    /// Takes everything buffered for `session`, whether or not its window
    /// has elapsed. Called when a turn ends, so the final frame carries
    /// any straggling chunks rather than dropping them.
    ///
    /// Returns an empty `Vec` if nothing is pending — an idle session
    /// ending its turn is normal, not an error.
    pub fn take(&mut self, session: &str) -> Vec<String> {
        self.pending
            .remove(session)
            .map(|p| p.chunks)
            .unwrap_or_default()
    }

    /// Releases every session whose window has elapsed as of `now`.
    ///
    /// Sessions are independent: one session's batch never merges with
    /// another's, preserving the sibling-isolation guarantee.
    pub fn due(&mut self, now: Instant) -> Vec<(String, Vec<String>)> {
        let expired: Vec<String> = self
            .pending
            .iter()
            .filter(|(_, p)| now.duration_since(p.opened_at) >= self.window)
            .map(|(session, _)| session.clone())
            .collect();

        expired
            .into_iter()
            .filter_map(|session| {
                self.pending
                    .remove(&session)
                    .map(|p| (session, p.chunks))
            })
            .collect()
    }

    /// When the earliest open window expires, or `None` if nothing is
    /// buffered. The caller sleeps until this before calling [`due`].
    ///
    /// [`due`]: ReplyCoalescer::due
    pub fn next_deadline(&self) -> Option<Instant> {
        self.pending
            .values()
            .map(|p| p.opened_at + self.window)
            .min()
    }

    /// Whether anything at all is buffered.
    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(base: Instant, millis: u64) -> Instant {
        base + Duration::from_millis(millis)
    }

    #[test]
    fn a_single_chunk_is_buffered_not_sent() {
        let mut c = ReplyCoalescer::default();
        let t0 = Instant::now();
        assert_eq!(c.push("m1", "hello".into(), t0), None);
        assert!(!c.is_empty());
    }

    #[test]
    fn chunks_release_once_the_window_elapses() {
        let mut c = ReplyCoalescer::new(Duration::from_millis(50), DEFAULT_MAX_BYTES);
        let t0 = Instant::now();
        assert_eq!(c.push("m1", "a".into(), t0), None);
        assert_eq!(c.push("m1", "b".into(), at(t0, 20)), None);

        assert!(c.due(at(t0, 49)).is_empty(), "must not flush early");

        let flushed = c.due(at(t0, 50));
        assert_eq!(flushed.len(), 1);
        assert_eq!(flushed[0].0, "m1");
        assert_eq!(flushed[0].1, vec!["a".to_string(), "b".to_string()]);
        assert!(c.is_empty());
    }

    #[test]
    fn window_runs_from_the_first_chunk_not_the_latest() {
        // A steady stream must still flush on schedule; if the window
        // reset on every push it would never fire.
        let mut c = ReplyCoalescer::new(Duration::from_millis(50), DEFAULT_MAX_BYTES);
        let t0 = Instant::now();
        for ms in [0, 20, 40, 45] {
            assert_eq!(c.push("m1", "x".into(), at(t0, ms)), None);
        }
        let flushed = c.due(at(t0, 50));
        assert_eq!(flushed.len(), 1, "window should be measured from t0");
        assert_eq!(flushed[0].1.len(), 4);
    }

    #[test]
    fn order_is_preserved_within_a_session() {
        let mut c = ReplyCoalescer::new(Duration::from_millis(10), DEFAULT_MAX_BYTES);
        let t0 = Instant::now();
        for piece in ["first", "second", "third"] {
            let _ = c.push("m1", piece.into(), t0);
        }
        let flushed = c.due(at(t0, 10));
        assert_eq!(
            flushed[0].1,
            vec![
                "first".to_string(),
                "second".to_string(),
                "third".to_string()
            ]
        );
    }

    #[test]
    fn sessions_never_merge_into_one_batch() {
        let mut c = ReplyCoalescer::new(Duration::from_millis(10), DEFAULT_MAX_BYTES);
        let t0 = Instant::now();
        let _ = c.push("alpha", "a1".into(), t0);
        let _ = c.push("beta", "b1".into(), t0);
        let _ = c.push("alpha", "a2".into(), t0);

        let mut flushed = c.due(at(t0, 10));
        flushed.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(flushed.len(), 2);
        assert_eq!(flushed[0].0, "alpha");
        assert_eq!(flushed[0].1, vec!["a1".to_string(), "a2".to_string()]);
        assert_eq!(flushed[1].0, "beta");
        assert_eq!(flushed[1].1, vec!["b1".to_string()]);
    }

    #[test]
    fn take_returns_everything_pending_for_the_turn_end() {
        let mut c = ReplyCoalescer::default();
        let t0 = Instant::now();
        let _ = c.push("m1", "tail".into(), t0);
        // Well inside the window — take must not wait for it.
        assert_eq!(c.take("m1"), vec!["tail".to_string()]);
        assert!(c.is_empty(), "take must clear the session");
    }

    #[test]
    fn take_on_an_idle_session_is_empty_not_a_panic() {
        let mut c = ReplyCoalescer::default();
        assert!(c.take("never-seen").is_empty());
    }

    #[test]
    fn take_does_not_disturb_a_sibling_session() {
        let mut c = ReplyCoalescer::default();
        let t0 = Instant::now();
        let _ = c.push("alpha", "a".into(), t0);
        let _ = c.push("beta", "b".into(), t0);
        assert_eq!(c.take("alpha"), vec!["a".to_string()]);
        assert_eq!(c.take("beta"), vec!["b".to_string()]);
    }

    #[test]
    fn byte_cap_flushes_immediately_including_the_triggering_chunk() {
        let mut c = ReplyCoalescer::new(Duration::from_secs(60), 10);
        let t0 = Instant::now();
        assert_eq!(c.push("m1", "12345".into(), t0), None);
        let flushed = c
            .push("m1", "67890".into(), t0)
            .expect("cap reached, should flush");
        assert_eq!(flushed, vec!["12345".to_string(), "67890".to_string()]);
        assert!(c.is_empty(), "a capped flush clears the session");
    }

    #[test]
    fn a_capped_flush_opens_a_fresh_window_for_the_next_chunk() {
        let mut c = ReplyCoalescer::new(Duration::from_millis(50), 4);
        let t0 = Instant::now();
        let _ = c.push("m1", "abcd".into(), t0); // hits cap, flushes
        assert!(c.is_empty());

        assert_eq!(c.push("m1", "e".into(), at(t0, 100)), None);
        // The new window opened at t0+100, so t0+120 is too early.
        assert!(c.due(at(t0, 120)).is_empty());
        assert_eq!(c.due(at(t0, 150)).len(), 1);
    }

    #[test]
    fn next_deadline_is_none_when_idle_and_earliest_when_buffered() {
        let mut c = ReplyCoalescer::new(Duration::from_millis(50), DEFAULT_MAX_BYTES);
        assert_eq!(c.next_deadline(), None);

        let t0 = Instant::now();
        let _ = c.push("late", "x".into(), at(t0, 30));
        let _ = c.push("early", "y".into(), t0);

        assert_eq!(
            c.next_deadline(),
            Some(at(t0, 50)),
            "earliest open window wins"
        );
    }

    #[test]
    fn due_leaves_sessions_whose_window_is_still_open() {
        let mut c = ReplyCoalescer::new(Duration::from_millis(50), DEFAULT_MAX_BYTES);
        let t0 = Instant::now();
        let _ = c.push("ready", "a".into(), t0);
        let _ = c.push("waiting", "b".into(), at(t0, 40));

        let flushed = c.due(at(t0, 50));
        assert_eq!(flushed.len(), 1);
        assert_eq!(flushed[0].0, "ready");
        assert!(!c.is_empty(), "the still-open session stays buffered");
        assert_eq!(c.next_deadline(), Some(at(t0, 90)));
    }

    #[test]
    fn no_text_is_lost_across_a_full_turn() {
        // The property that matters most: every pushed piece comes back
        // exactly once, in order, via some combination of due/take.
        let mut c = ReplyCoalescer::new(Duration::from_millis(50), DEFAULT_MAX_BYTES);
        let t0 = Instant::now();
        let sent = ["one", "two", "three", "four", "five"];

        let mut got: Vec<String> = Vec::new();
        for (i, piece) in sent.iter().enumerate() {
            let now = at(t0, (i as u64) * 30);
            if let Some(batch) = c.push("m1", (*piece).into(), now) {
                got.extend(batch);
            }
            for (_, batch) in c.due(now) {
                got.extend(batch);
            }
        }
        got.extend(c.take("m1"));

        assert_eq!(got, sent.iter().map(|s| s.to_string()).collect::<Vec<_>>());
        assert!(c.is_empty());
    }
}
