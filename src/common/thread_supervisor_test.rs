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
    let handle = spawn_supervised_with_stop("test-shared-stop", Arc::clone(&stop), move || {
        rounds_body.fetch_add(1, Ordering::SeqCst);
        if stop_body.load(Ordering::Acquire) {
            // Simulate a round body that exits without doing work
            // once its owner asked for shutdown.
        }
    });
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
    let skipped: Option<u32> = run_panic_gated("test-gated", || panic!("controlled gated panic"));
    assert_eq!(
        skipped, None,
        "a panicking item must yield None, not propagate"
    );
}
