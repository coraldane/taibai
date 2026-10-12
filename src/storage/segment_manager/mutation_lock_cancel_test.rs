//! A queued mutation-lock waiter whose client disconnected (or whose
//! statement deadline expired) must abort at the next poll instead of
//! occupying the queue until the lock timeout. Regression for the 2026-10-12
//! `t_stock_minute` incident: dead writers piled up behind a 15-minute lock
//! backlog while their JDBC clients had already timed out, so every live
//! writer queued behind corpses and lock waits reached 992-1113 seconds.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::*;
use crate::common::error::TaibaiError;
use crate::common::query_cancel::{QueryCancellationGuard, QueryContext};

fn is_cancellation<T>(result: &crate::common::error::Result<T>) -> bool {
    match result {
        Ok(_) => false,
        Err(TaibaiError::Sql(message)) => message.contains("cancelled"),
        Err(_) => false,
    }
}

fn disconnect_flag() -> Arc<AtomicBool> {
    Arc::new(AtomicBool::new(false))
}

#[test]
fn table_mutation_lock_waiter_aborts_when_client_disconnects() {
    let state = Arc::new(TableSegmentState::new(1));
    let held = state.mutation_lock.lock().unwrap();

    let flag = disconnect_flag();
    let waiter_flag = Arc::clone(&flag);
    let waiter_state = Arc::clone(&state);
    let handle = std::thread::spawn(move || {
        let context = Arc::new(QueryContext::new(waiter_flag));
        let _guard = QueryCancellationGuard::enter(context);
        let started = Instant::now();
        let result = manager_core::manager_core_test_support::acquire_mutation_lock_for_test(
            &waiter_state,
            "main.t_stock_minute",
        )
        .map(|_| ());
        (result, started.elapsed())
    });

    std::thread::sleep(Duration::from_millis(200));
    flag.store(true, Ordering::Release);
    let (result, elapsed) = handle.join().unwrap();

    assert!(
        is_cancellation(&result),
        "queued waiter must surface the cancellation error, got {result:?}"
    );
    assert!(
        elapsed < Duration::from_secs(5),
        "disconnected waiter must abort at the next poll, waited {elapsed:?}"
    );
    drop(held);
}

#[test]
fn partition_mutation_lock_waiter_aborts_when_client_disconnects() {
    let state = Arc::new(TableSegmentState::new(1));
    let lock = manager_core::partition_mutation_lock_arc(&state, "p0");
    let held = lock.lock().unwrap();

    let flag = disconnect_flag();
    let waiter_flag = Arc::clone(&flag);
    let waiter_state = Arc::clone(&state);
    let waiter_lock = Arc::clone(&lock);
    let handle = std::thread::spawn(move || {
        let context = Arc::new(QueryContext::new(waiter_flag));
        let _guard = QueryCancellationGuard::enter(context);
        let started = Instant::now();
        let result = manager_core::manager_core_test_support::acquire_partition_mutation_lock_for_test(
                &waiter_state, &waiter_lock, "main.t", "p0",
            )
            .map(|_| ());
        (result, started.elapsed())
    });

    std::thread::sleep(Duration::from_millis(200));
    flag.store(true, Ordering::Release);
    let (result, elapsed) = handle.join().unwrap();

    assert!(
        is_cancellation(&result),
        "queued partition waiter must surface the cancellation error, got {result:?}"
    );
    assert!(
        elapsed < Duration::from_secs(5),
        "disconnected partition waiter must abort at the next poll, waited {elapsed:?}"
    );
    drop(held);
}

#[test]
fn waiter_without_context_still_acquires_once_released() {
    let state = Arc::new(TableSegmentState::new(1));
    let held = state.mutation_lock.lock().unwrap();

    let waiter_state = Arc::clone(&state);
    let handle = std::thread::spawn(move || {
        let started = Instant::now();
        let result = manager_core::manager_core_test_support::acquire_mutation_lock_for_test(
            &waiter_state,
            "main.t_stock_minute",
        )
        .map(|_| ());
        (result.is_ok(), started.elapsed())
    });

    std::thread::sleep(Duration::from_millis(100));
    drop(held);
    let (acquired, elapsed) = handle.join().unwrap();

    assert!(
        acquired,
        "background callers without an ambient context must queue as before"
    );
    assert!(elapsed < Duration::from_secs(5), "waited {elapsed:?}");
}
