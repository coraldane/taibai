use super::*;
use std::time::Duration;

#[test]
fn cancellation_is_scoped_and_observed() {
    assert!(check().is_ok());
    let cancelled = Arc::new(AtomicBool::new(false));
    {
        let _guard = QueryCancellationGuard::enter_flag(Arc::clone(&cancelled));
        assert!(check().is_ok());
        cancelled.store(true, Ordering::Release);
        assert!(check().is_err());
    }
    assert!(check().is_ok());
}

#[test]
fn committing_phase_ignores_cancellation() {
    let cancelled = Arc::new(AtomicBool::new(false));
    let context = Arc::new(QueryContext::new(Arc::clone(&cancelled)));
    let _guard = QueryCancellationGuard::enter(Arc::clone(&context));
    context.enter_phase(QueryPhase::Committing);
    cancelled.store(true, Ordering::Release);
    assert!(context.check().is_ok());
    assert!(!context.is_cancelled());
    context.enter_phase(QueryPhase::Executing);
    assert!(context.check().is_err());
}

#[test]
fn deadline_expires_with_distinct_error() {
    let cancelled = Arc::new(AtomicBool::new(false));
    let context = Arc::new(
        QueryContext::new(Arc::clone(&cancelled))
            .with_deadline(Some(Instant::now() - Duration::from_millis(1))),
    );
    let _guard = QueryCancellationGuard::enter(context);
    assert!(!cancelled.load(Ordering::Acquire));
    let error = check().unwrap_err().to_string();
    assert!(error.contains("statement timeout"), "got: {error}");
}

#[test]
fn deadline_respected_before_expiry() {
    let cancelled = Arc::new(AtomicBool::new(false));
    let context = Arc::new(
        QueryContext::new(Arc::clone(&cancelled))
            .with_deadline(Some(Instant::now() + Duration::from_secs(3600))),
    );
    let _guard = QueryCancellationGuard::enter(context);
    assert!(check().is_ok());
}

#[test]
fn flag_is_capturable_for_parallel_lanes() {
    let cancelled = Arc::new(AtomicBool::new(false));
    let context = Arc::new(QueryContext::new(Arc::clone(&cancelled)));
    let _guard = QueryCancellationGuard::enter(Arc::clone(&context));

    // Simulate a rayon lane: no ambient guard, only the captured flag.
    let captured = current_flag().expect("ambient context present");
    std::thread::spawn(move || {
        assert!(!check_on(&captured));
        cancelled.store(true, Ordering::Release);
        assert!(check_on(&captured));
    })
    .join()
    .unwrap();

    // The ambient thread is unaffected by the lane-side store until it
    // observes the same shared flag.
    assert!(check().is_err());
}

fn check_on(flag: &AtomicBool) -> bool {
    flag.load(Ordering::Acquire)
}

#[test]
fn query_ids_are_unique() {
    let a = QueryContext::new(Arc::new(AtomicBool::new(false)));
    let b = QueryContext::new(Arc::new(AtomicBool::new(false)));
    assert_ne!(a.query_id(), b.query_id());
}
