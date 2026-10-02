//! Panic-tolerant supervision for long-lived background threads (audit P1-5).
//!
//! A bare `std::thread::spawn` + infinite `loop` thread dies permanently on
//! its first panic (a poisoned-lock `unwrap`, an IO assertion, ...): the
//! function it performed then silently stops until process restart, because
//! the `JoinHandle` was dropped and nothing observes the exit.
//!
//! [`spawn_supervised`] wraps ONE round of work ("tick") in `catch_unwind`:
//! a panicking round is logged with `tracing::error!` and retried after an
//! exponential backoff (100 ms start, doubling, capped at 60 s), so a
//! transient fault degrades the cadence instead of killing the subsystem.
//! The round body must be one bounded unit of work — one tick, or one
//! received-and-processed message — not the original infinite loop, so the
//! supervisor regains control between rounds.
//!
//! Cancellation: the supervisor polls a stop flag between rounds, and
//! [`SupervisorHandle::shutdown`] sets that flag and joins the thread with a
//! bounded timeout — a wedged round is warned about and abandoned, never
//! deadlocking shutdown. Dropping the handle detaches the thread (it keeps
//! running until its stop flag is set), matching the pre-supervision
//! behavior of detached background threads.
//!
//! Channel-consumer threads that already own a stop/join protocol
//! (`BatchWriter`'s workers) do not need the full supervisor:
//! [`run_panic_gated`] wraps a single work item so a panicking message is
//! logged and skipped while the thread itself survives.

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// Delay before the first retry after a panic.
const BACKOFF_START: Duration = Duration::from_millis(100);
/// Ceiling of the exponential retry delay.
const BACKOFF_MAX: Duration = Duration::from_secs(60);
/// How long `shutdown` waits for the thread to exit before abandoning.
const SHUTDOWN_JOIN_TIMEOUT: Duration = Duration::from_secs(10);
/// Granularity of interruptible sleeps (stop-flag poll interval).
const STOP_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Handle to a supervised background thread. Dropping it detaches the
/// thread; [`SupervisorHandle::shutdown`] stops and joins it.
pub struct SupervisorHandle {
    name: String,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

/// Spawn `body` under a dedicated supervisor thread.
///
/// `body` is ONE round of work. For a timer thread that is one tick
/// (cadence sleep included, e.g. via [`sleep_interruptibly`]); for a channel
/// consumer it is "block for one message and process it" (returning once a
/// shutdown message was seen). The supervisor repeats
/// `catch_unwind(body)` until the stop flag is set, retrying a panicking
/// round after an exponential backoff instead of letting the thread die.
pub fn spawn_supervised(name: &str, body: impl Fn() + Send + 'static) -> SupervisorHandle {
    spawn_supervised_with_stop(name, Arc::new(AtomicBool::new(false)), body)
}

/// [`spawn_supervised`] with a caller-owned stop flag, for threads that
/// already share one (e.g. `SegmentManager::flush_stop`): setting that flag
/// stops the supervisor as well, so pre-existing shutdown paths keep
/// working unchanged.
pub fn spawn_supervised_with_stop(
    name: &str,
    stop: Arc<AtomicBool>,
    body: impl Fn() + Send + 'static,
) -> SupervisorHandle {
    let thread_name = name.to_string();
    let loop_stop = Arc::clone(&stop);
    let loop_name = thread_name.clone();
    let handle = std::thread::Builder::new()
        .name(thread_name.clone())
        .spawn(move || {
            let mut backoff = BACKOFF_START;
            while !loop_stop.load(Ordering::Acquire) {
                let round = catch_unwind(AssertUnwindSafe(&body));
                // Re-check before acting on the outcome: a round that
                // returned because the stop flag flipped must not be
                // mistaken for success or failure, just exit.
                if loop_stop.load(Ordering::Acquire) {
                    break;
                }
                match round {
                    Ok(()) => backoff = BACKOFF_START,
                    Err(payload) => {
                        tracing::error!(
                            thread = %loop_name,
                            panic = %panic_message(&payload),
                            backoff_ms = backoff.as_millis() as u64,
                            "supervised background thread panicked; retrying round after backoff"
                        );
                        sleep_interruptibly(&loop_stop, backoff);
                        backoff = std::cmp::min(backoff.saturating_mul(2), BACKOFF_MAX);
                    }
                }
            }
        })
        .expect("failed to spawn supervised thread");
    SupervisorHandle {
        name: thread_name,
        stop,
        handle: Some(handle),
    }
}

impl SupervisorHandle {
    /// Ask the thread to stop and join it, bounded by an internal timeout:
    /// a round still in flight after the timeout is warned about and the
    /// join is abandoned (the thread exits on its own once its current
    /// round observes the stop flag). Never blocks shutdown indefinitely.
    pub fn shutdown(mut self) {
        self.stop.store(true, Ordering::Release);
        let Some(handle) = self.handle.take() else {
            return;
        };
        if !handle.is_finished() {
            let deadline = Instant::now()
                .checked_add(SHUTDOWN_JOIN_TIMEOUT)
                .unwrap_or_else(Instant::now);
            while !handle.is_finished() {
                if Instant::now() >= deadline {
                    tracing::warn!(
                        thread = %self.name,
                        timeout_ms = SHUTDOWN_JOIN_TIMEOUT.as_millis() as u64,
                        "supervised thread did not stop within the shutdown timeout; abandoning join"
                    );
                    return;
                }
                std::thread::sleep(STOP_POLL_INTERVAL);
            }
        }
        let _ = handle.join();
    }

    /// Set the stop flag without waiting for the thread to exit.
    pub fn request_stop(&self) {
        self.stop.store(true, Ordering::Release);
    }
}

/// Sleep for `total`, returning early once `stop` is set. Supervised round
/// bodies use this for their cadence sleep so `shutdown` does not have to
/// wait out a full interval.
pub fn sleep_interruptibly(stop: &AtomicBool, total: Duration) {
    let Some(deadline) = Instant::now().checked_add(total) else {
        std::thread::sleep(total);
        return;
    };
    while !stop.load(Ordering::Acquire) {
        let now = Instant::now();
        if now >= deadline {
            return;
        }
        std::thread::sleep(STOP_POLL_INTERVAL.min(deadline - now));
    }
}

/// Run one unit of background work, converting a panic into `None` with an
/// `error!` log. Channel-consumer loops that must keep their thread (and
/// its existing stop/join protocol) intact use this around each message
/// instead of full supervision: a panicking message is skipped and the next
/// one is processed on the same thread.
pub fn run_panic_gated<T>(name: &str, body: impl FnOnce() -> T) -> Option<T> {
    match catch_unwind(AssertUnwindSafe(body)) {
        Ok(value) => Some(value),
        Err(payload) => {
            tracing::error!(
                thread = name,
                panic = %panic_message(&payload),
                "background work item panicked; skipping it and continuing"
            );
            None
        }
    }
}

fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "non-string panic payload".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    /// Block until `condition` holds, failing the test after 10 s.
    fn wait_until(description: &str, mut condition: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while !condition() {
            if Instant::now() > deadline {
                panic!("timed out waiting for {description}");
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    #[test]
    fn panicking_rounds_are_retried_until_success() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let successes = Arc::new(AtomicUsize::new(0));
        let attempts_body = Arc::clone(&attempts);
        let successes_body = Arc::clone(&successes);
        let handle = spawn_supervised("test-panic-retry", move || {
            let attempt = attempts_body.fetch_add(1, Ordering::SeqCst);
            if attempt < 2 {
                panic!("controlled panic #{attempt}");
            }
            successes_body.fetch_add(1, Ordering::SeqCst);
        });
        // Two panicking rounds (backoffs ~100 ms + ~200 ms) then a
        // successful third round prove the supervisor retried the body.
        wait_until("third round to run successfully", || {
            successes.load(Ordering::SeqCst) >= 1
        });
        handle.shutdown();
        assert!(
            attempts.load(Ordering::SeqCst) >= 3,
            "body must have been invoked at least three times"
        );
    }

    #[test]
    fn shutdown_stops_rounds_and_join_returns() {
        let rounds = Arc::new(AtomicUsize::new(0));
        let rounds_body = Arc::clone(&rounds);
        let handle = spawn_supervised("test-shutdown", move || {
            rounds_body.fetch_add(1, Ordering::SeqCst);
            std::thread::sleep(Duration::from_millis(5));
        });
        wait_until("first round to run", || rounds.load(Ordering::SeqCst) >= 1);
        // Must return promptly (the round sleeps 5 ms, far below the join
        // timeout) and no further rounds may run afterwards.
        handle.shutdown();
        let after_shutdown = rounds.load(Ordering::SeqCst);
        std::thread::sleep(Duration::from_millis(200));
        assert_eq!(
            rounds.load(Ordering::SeqCst),
            after_shutdown,
            "no rounds may run after shutdown"
        );
    }

    #[test]
    fn externally_set_shared_stop_flag_stops_the_thread() {
        let stop = Arc::new(AtomicBool::new(false));
        let rounds = Arc::new(AtomicUsize::new(0));
        let rounds_body = Arc::clone(&rounds);
        let stop_body = Arc::clone(&stop);
        let handle = spawn_supervised_with_stop(
            "test-shared-stop",
            Arc::clone(&stop),
            move || {
                rounds_body.fetch_add(1, Ordering::SeqCst);
                if stop_body.load(Ordering::Acquire) {
                    // Simulate a round body that exits without doing work
                    // once its owner asked for shutdown.
                    return;
                }
            },
        );
        wait_until("first round to run", || rounds.load(Ordering::SeqCst) >= 1);
        stop.store(true, Ordering::Release);
        // shutdown() sees the same flag already set and joins quickly.
        handle.shutdown();
        let after_stop = rounds.load(Ordering::SeqCst);
        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(rounds.load(Ordering::SeqCst), after_stop);
    }

    #[test]
    fn run_panic_gated_skips_panicking_item() {
        assert_eq!(run_panic_gated("test-gated", || 7u32), Some(7));
        let skipped: Option<u32> = run_panic_gated("test-gated", || {
            panic!("controlled gated panic")
        });
        assert_eq!(skipped, None, "a panicking item must yield None, not propagate");
    }
}
