use std::sync::{Condvar, Mutex, OnceLock};

struct Semaphore {
    available: Mutex<usize>,
    wake: Condvar,
}

impl Semaphore {
    fn new(permits: usize) -> Self {
        Self {
            available: Mutex::new(permits.max(1)),
            wake: Condvar::new(),
        }
    }

    fn acquire(&'static self) -> Permit {
        let mut available = self.available.lock().unwrap_or_else(|e| e.into_inner());
        while *available == 0 {
            available = self.wake.wait(available).unwrap_or_else(|e| e.into_inner());
        }
        *available -= 1;
        Permit { semaphore: self }
    }
}

/// RAII permit shared by every query in the process. This prevents each query
/// from independently saturating the Rayon pool and storage device.
pub struct Permit {
    semaphore: &'static Semaphore,
}
impl Drop for Permit {
    fn drop(&mut self) {
        let mut available = self
            .semaphore
            .available
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        *available += 1;
        self.semaphore.wake.notify_one();
    }
}

static CPU: OnceLock<Semaphore> = OnceLock::new();
static IO: OnceLock<Semaphore> = OnceLock::new();

pub fn acquire_cpu() -> Permit {
    CPU.get_or_init(|| Semaphore::new(crate::common::config::query_limits().global_cpu_permits))
        .acquire()
}

pub fn acquire_io() -> Permit {
    IO.get_or_init(|| Semaphore::new(crate::common::config::query_limits().global_io_permits))
        .acquire()
}
