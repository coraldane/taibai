use super::*;

#[test]
fn cancellation_is_scoped_and_observed() {
    assert!(check().is_ok());
    let cancelled = Arc::new(AtomicBool::new(false));
    {
        let _guard = QueryCancellationGuard::enter(Arc::clone(&cancelled));
        assert!(check().is_ok());
        cancelled.store(true, Ordering::Release);
        assert!(check().is_err());
    }
    assert!(check().is_ok());
}
