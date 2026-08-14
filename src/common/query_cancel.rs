use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

thread_local! {
    static CURRENT: RefCell<Option<Arc<AtomicBool>>> = const { RefCell::new(None) };
}

pub struct QueryCancellationGuard {
    previous: Option<Arc<AtomicBool>>,
}

impl QueryCancellationGuard {
    pub fn enter(cancelled: Arc<AtomicBool>) -> Self {
        let previous = CURRENT.with(|current| current.replace(Some(cancelled)));
        Self { previous }
    }
}

impl Drop for QueryCancellationGuard {
    fn drop(&mut self) {
        CURRENT.with(|current| {
            current.replace(self.previous.take());
        });
    }
}

pub fn is_cancelled() -> bool {
    CURRENT.with(|current| {
        current
            .borrow()
            .as_ref()
            .is_some_and(|cancelled| cancelled.load(Ordering::Acquire))
    })
}

pub fn check() -> crate::common::error::Result<()> {
    if is_cancelled() {
        Err(crate::common::error::TaibaiError::Sql(
            "query cancelled because the client disconnected".into(),
        ))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
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
}
