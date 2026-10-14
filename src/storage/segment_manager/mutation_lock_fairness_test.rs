//! Regression tests for the FIFO admission queue on the table and partition
//! mutation locks (`FairMutex`): a registered waiter must be served ahead of
//! later arrivals no matter the polling luck, an abandoned wait (cancel)
//! must hand its place to the successor instead of blocking it, and a queued
//! partition writer must park while the wide gate is raised so the gate
//! collector — which bypasses the queue — can always take the mutex.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::*;
use crate::common::query_cancel::{QueryCancellationGuard, QueryContext};

use manager_core::manager_core_test_support::{
    acquire_mutation_lock_for_test, acquire_partition_mutation_lock_for_test,
};
use manager_core::{acquire_partition_lock_for_wide_gate, partition_mutation_lock_arc};

/// Spawn a waiter, give it time to register its ticket (the queue head
/// polls every <=16ms, so 150ms leaves no ordering ambiguity), and return
/// the handle.
fn spawn_waiter<T, F>(body: F) -> std::thread::JoinHandle<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    let handle = std::thread::spawn(body);
    std::thread::sleep(Duration::from_millis(150));
    handle
}

#[test]
fn table_mutation_lock_wakes_waiters_in_arrival_order() {
    let state = Arc::new(TableSegmentState::new(1));
    let held = state.mutation_lock.lock_for_test();

    let (tx, rx) = mpsc::channel::<usize>();
    let mut handles = Vec::new();
    for id in 1..=3 {
        let state = Arc::clone(&state);
        let tx = tx.clone();
        handles.push(spawn_waiter(move || {
            // Background caller: no ambient cancellation context, the
            // wait must still queue fairly and complete once released.
            let result = acquire_mutation_lock_for_test(&state, "main.t");
            tx.send(id).unwrap();
            drop(result);
        }));
    }
    drop(tx);
    // Every waiter has registered by now and none can acquire while the
    // holder keeps the mutex; releasing must wake them in ticket order.
    std::thread::sleep(Duration::from_millis(100));
    drop(held);

    let mut order = Vec::new();
    for _ in 0..3 {
        order.push(
            rx.recv_timeout(Duration::from_secs(5))
                .expect("every queued waiter must acquire eventually"),
        );
    }
    for handle in handles {
        handle.join().unwrap();
    }
    assert_eq!(
        order,
        vec![1, 2, 3],
        "waiters must acquire in arrival order"
    );
}

#[test]
fn cancelled_table_waiter_hands_its_queue_position_to_the_next() {
    let state = Arc::new(TableSegmentState::new(1));
    let held = state.mutation_lock.lock_for_test();

    let flag = Arc::new(AtomicBool::new(false));
    let cancelled = {
        let state = Arc::clone(&state);
        let flag = Arc::clone(&flag);
        spawn_waiter(move || {
            let context = Arc::new(QueryContext::new(flag));
            let _guard = QueryCancellationGuard::enter(context);
            acquire_mutation_lock_for_test(&state, "main.t").is_err()
        })
    };
    let successor = {
        let state = Arc::clone(&state);
        spawn_waiter(move || acquire_mutation_lock_for_test(&state, "main.t").is_ok())
    };

    // The cancelled waiter must leave the queue at the next poll; the
    // successor behind it must then acquire without waiting for the
    // abandoned ticket to be served.
    flag.store(true, Ordering::Release);
    assert!(cancelled.join().unwrap(), "cancelled waiter must abort");
    drop(held);
    let started = Instant::now();
    assert!(
        successor.join().unwrap(),
        "the successor must not be blocked by the abandoned ticket"
    );
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "successor waited {:?} after the holder released",
        started.elapsed()
    );
}

#[test]
fn queued_partition_writer_parks_while_wide_gate_is_raised() {
    let state = Arc::new(TableSegmentState::new(1));
    let lock = partition_mutation_lock_arc(&state, "p0");
    let held = lock.lock_for_test();

    let waiter = {
        let state = Arc::clone(&state);
        let lock = Arc::clone(&lock);
        spawn_waiter(move || {
            acquire_partition_mutation_lock_for_test(&state, &lock, "main.t", "p0").is_ok()
        })
    };

    // Raise the wide gate: the queued writer must stop polling the mutex
    // entirely, so the gate collector — which never queues — can win the
    // mutex instead of deadlocking against the head of the queue.
    state.partition_gate.store(true, Ordering::Release);
    std::thread::sleep(Duration::from_millis(100));
    drop(held);

    let collector = {
        let lock = Arc::clone(&lock);
        std::thread::spawn(move || {
            acquire_partition_lock_for_wide_gate(&lock, "main.t", "p0", Duration::from_secs(5))
                .is_ok()
        })
    };
    assert!(
        collector.join().unwrap(),
        "wide-gate collector must take the mutex while the gate parks the queue"
    );
    // The collector's guard dropped when its thread ended; the parked
    // writer must still stay parked until the gate itself clears.
    std::thread::sleep(Duration::from_millis(100));
    state.partition_gate.store(false, Ordering::Release);

    assert!(
        waiter.join().unwrap(),
        "the queued writer must acquire once the gate clears"
    );
}
