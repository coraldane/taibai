//! Process self-healing: a canary watchdog plus systemd `sd_notify` pings.
//!
//! A wedged-but-alive process (pool starvation, a future lock deadlock) is
//! invisible to `Restart=on-failure`: the 2026-11-05 hz-dev-204 incident
//! needed four manual restarts in forty minutes. The watchdog runs a real
//! end-to-end canary through the general scan path on a fixed cadence;
//! after N consecutive failures it hands a report to the wedge hook (the
//! default hook logs the running-query registry and aborts, letting
//! systemd restart the service within seconds). `sd_notify` WATCHDOG pings
//! additionally cover total runtime stalls the canary thread itself cannot
//! observe, via systemd's `WatchdogSec`.

use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::common::error::Result;

/// Canary health for the last round, exposed for `SHOW WORKLOAD`.
pub const CANARY_OK: u8 = 0;
pub const CANARY_FAILED: u8 = 1;
pub const CANARY_TIMED_OUT: u8 = 2;
pub const CANARY_NEVER_RAN: u8 = 3;

/// Canary health counters, injectable so tests run against a private
/// instance instead of the process-wide one.
pub struct CanaryMetrics {
    last_latency_ms: AtomicU64,
    last_status: AtomicU8,
    consecutive_failures: AtomicU32,
}

impl CanaryMetrics {
    pub fn new() -> Self {
        Self {
            last_latency_ms: AtomicU64::new(u64::MAX),
            last_status: AtomicU8::new(CANARY_NEVER_RAN),
            consecutive_failures: AtomicU32::new(0),
        }
    }
}

impl Default for CanaryMetrics {
    fn default() -> Self {
        Self::new()
    }
}

static GLOBAL_METRICS: CanaryMetrics = CanaryMetrics {
    last_latency_ms: AtomicU64::new(u64::MAX),
    last_status: AtomicU8::new(CANARY_NEVER_RAN),
    consecutive_failures: AtomicU32::new(0),
};

/// Snapshot of canary health: (last latency ms, status, consecutive
/// failures). `u64::MAX` latency means the canary has not run yet.
pub fn canary_snapshot() -> (u64, u8, u32) {
    (
        GLOBAL_METRICS.last_latency_ms.load(Ordering::Relaxed),
        GLOBAL_METRICS.last_status.load(Ordering::Relaxed),
        GLOBAL_METRICS.consecutive_failures.load(Ordering::Relaxed),
    )
}

/// Send an sd_notify state frame to the socket named by `$NOTIFY_SOCKET`.
/// Returns false (and does nothing) when the variable is unset — manual
/// runs, Darwin and containers simply have no notify socket. Hand-rolled
/// on purpose: the protocol is one unix datagram, not a dependency.
pub fn sd_notify(state: &str) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::net::UnixDatagram;

        let Some(raw) = std::env::var_os("NOTIFY_SOCKET") else {
            return false;
        };
        // An "@" prefix marks systemd's abstract namespace; the kernel
        // spelling replaces it with NUL.
        #[cfg(target_os = "linux")]
        let target: std::path::PathBuf = {
            use std::os::unix::ffi::{OsStrExt, OsStringExt};
            let mut bytes = raw.clone().into_vec();
            if bytes.first() == Some(&b'@') {
                bytes[0] = 0;
            }
            std::ffi::OsStr::from_bytes(&bytes).to_os_string().into()
        };
        #[cfg(not(target_os = "linux"))]
        let target: std::path::PathBuf = std::path::PathBuf::from(raw);

        let Ok(socket) = UnixDatagram::unbound() else {
            return false;
        };
        socket.send_to(state.as_bytes(), target).is_ok()
    }
    #[cfg(not(unix))]
    {
        let _ = state;
        false
    }
}

pub struct WatchdogConfig {
    /// Sleep between canary rounds (also the sd_notify ping cadence).
    pub interval: Duration,
    /// A canary slower than this counts as a failure; the round's thread is
    /// deliberately left wedged as incident evidence when it times out.
    pub timeout: Duration,
    /// Consecutive failures before the wedge hook fires. With the default
    /// interval/timeout the worst-case time-to-abort stays well under the
    /// unit's WatchdogSec, so the two safety nets never race.
    pub failure_threshold: u32,
}

impl Default for WatchdogConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(15),
            timeout: Duration::from_secs(10),
            failure_threshold: 3,
        }
    }
}

/// Everything the wedge hook needs to log before the process goes down.
pub struct WatchdogReport {
    pub consecutive_failures: u32,
    pub last_latency: Option<Duration>,
    pub timeout: Duration,
    pub message: &'static str,
}

pub type Canary = Arc<dyn Fn() -> Result<()> + Send + Sync + 'static>;
pub type WedgeHook = Arc<dyn Fn(&WatchdogReport) + Send + Sync + 'static>;

/// Run one watchdog round: ping systemd, sleep, then canary under a
/// timeout. Returns true when the wedge hook fired (the caller decides
/// whether the process survives — the production hook aborts inside).
pub fn run_round(
    config: &WatchdogConfig,
    stop: &AtomicBool,
    canary: &Canary,
    on_wedge: &WedgeHook,
    metrics: &CanaryMetrics,
) -> bool {
    if sd_notify("WATCHDOG=1\n") {
        // Deliberately silent on failure: pings are best-effort extras on
        // top of the canary, not a health signal of their own.
    }
    crate::common::thread_supervisor::sleep_interruptibly(stop, config.interval);
    if stop.load(Ordering::Acquire) {
        return false;
    }

    let started = Instant::now();
    let (tx, rx) = std::sync::mpsc::channel();
    let canary = Arc::clone(canary);
    // A fresh short-lived thread per round keeps a wedged canary from
    // wedging the watchdog itself; leaked threads are bounded by the
    // failure threshold (the hook aborts the process) and their stacks are
    // post-mortem evidence.
    std::thread::Builder::new()
        .name("taibai-watchdog-canary".into())
        .spawn(move || {
            let _ = tx.send(canary());
        })
        .ok();

    let mut status = CANARY_OK;
    let mut latency = started.elapsed();
    match rx.recv_timeout(config.timeout) {
        Ok(Ok(())) => {
            latency = started.elapsed();
        }
        Ok(Err(error)) => {
            status = CANARY_FAILED;
            tracing::warn!(%error, "watchdog canary failed");
        }
        Err(_) => {
            status = CANARY_TIMED_OUT;
            tracing::error!(
                timeout_secs = config.timeout.as_secs(),
                "watchdog canary timed out; thread left wedged as evidence"
            );
        }
    }
    metrics
        .last_latency_ms
        .store(latency.as_millis() as u64, Ordering::Relaxed);
    metrics.last_status.store(status, Ordering::Relaxed);
    let failures = if status == CANARY_OK {
        metrics.consecutive_failures.store(0, Ordering::Relaxed);
        0
    } else {
        metrics.consecutive_failures.fetch_add(1, Ordering::Relaxed) + 1
    };

    if failures >= config.failure_threshold {
        let report = WatchdogReport {
            consecutive_failures: failures,
            last_latency: (status != CANARY_NEVER_RAN).then_some(latency),
            timeout: config.timeout,
            message: "watchdog canary failed repeatedly; aborting for systemd restart",
        };
        on_wedge(&report);
        return true;
    }
    false
}

/// Spawn the watchdog under the thread supervisor: each supervised body is
/// one round, so a panicking round retries with backoff instead of leaving
/// the watchdog silently dead.
pub fn spawn_supervised(
    config: WatchdogConfig,
    stop: Arc<AtomicBool>,
    canary: Canary,
    on_wedge: WedgeHook,
) {
    let body_stop = Arc::clone(&stop);
    crate::common::thread_supervisor::spawn_supervised_with_stop(
        "taibai-watchdog",
        stop,
        move || {
            if run_round(&config, &body_stop, &canary, &on_wedge, &GLOBAL_METRICS) {
                // The production hook already aborted; an injected test hook
                // asks to stop looping instead.
                body_stop.store(true, Ordering::Release);
            }
        },
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sd_notify_sends_frames_to_a_local_datagram_socket() {
        use std::os::unix::net::UnixDatagram;

        let dir = tempfile::tempdir().expect("tempdir");
        let socket_path = dir.path().join("notify.sock");
        let receiver = UnixDatagram::bind(&socket_path).expect("bind notify socket");
        // Guard against the environment leaking a real NOTIFY_SOCKET into
        // the test process.
        std::env::set_var("NOTIFY_SOCKET", &socket_path);

        let sent = sd_notify("READY=1\n");
        std::env::remove_var("NOTIFY_SOCKET");

        assert!(sent, "frame should be delivered to the bound socket");
        let mut buffer = [0u8; 64];
        let (len, _) = receiver.recv_from(&mut buffer).expect("receive frame");
        assert_eq!(&buffer[..len], b"READY=1\n");
    }

    #[test]
    fn sd_notify_without_socket_reports_false() {
        std::env::remove_var("NOTIFY_SOCKET");
        assert!(!sd_notify("WATCHDOG=1\n"));
    }

    #[test]
    fn consecutive_failures_fire_the_wedge_hook_at_the_threshold() {
        let config = WatchdogConfig {
            interval: Duration::from_millis(1),
            timeout: Duration::from_millis(200),
            failure_threshold: 3,
        };
        let stop = Arc::new(AtomicBool::new(false));
        let fired = Arc::new(AtomicBool::new(false));
        let hook_seen = Arc::new(std::sync::Mutex::new(None::<WatchdogReportSnapshot>));
        let metrics = CanaryMetrics::new();

        let canary: Canary = Arc::new(|| {
            Err(crate::common::error::TaibaiError::Internal("canary down".into()))
        });
        let fired_flag = Arc::clone(&fired);
        let seen = Arc::clone(&hook_seen);
        let on_wedge: WedgeHook = Arc::new(move |report: &WatchdogReport| {
            *seen.lock().unwrap() = Some(WatchdogReportSnapshot {
                failures: report.consecutive_failures,
                message: report.message,
            });
            fired_flag.store(true, Ordering::Release);
        });

        assert!(!run_round(&config, &stop, &canary, &on_wedge, &metrics));
        assert!(!run_round(&config, &stop, &canary, &on_wedge, &metrics));
        // Third failure crosses the threshold.
        assert!(run_round(&config, &stop, &canary, &on_wedge, &metrics));

        assert!(fired.load(Ordering::Acquire));
        let seen = hook_seen.lock().unwrap().take().expect("hook fired");
        assert_eq!(seen.failures, 3);
        assert!(seen.message.contains("aborting"));

        assert_eq!(metrics.last_status.load(Ordering::Relaxed), CANARY_FAILED);
        assert_eq!(
            metrics.consecutive_failures.load(Ordering::Relaxed),
            3
        );
    }

    #[test]
    fn a_success_round_resets_the_failure_streak() {
        let config = WatchdogConfig {
            interval: Duration::from_millis(1),
            timeout: Duration::from_millis(200),
            failure_threshold: 2,
        };
        let stop = Arc::new(AtomicBool::new(false));
        let metrics = CanaryMetrics::new();
        let failing: Canary = Arc::new(|| {
            Err(crate::common::error::TaibaiError::Internal("canary down".into()))
        });
        let ok: Canary = Arc::new(|| Ok(()));
        let never: WedgeHook = Arc::new(|_| panic!("wedge hook must not fire"));

        assert!(!run_round(&config, &stop, &failing, &never, &metrics));
        assert!(!run_round(&config, &stop, &ok, &never, &metrics));
        // Streak was reset by the success: this failure is the first again.
        assert!(!run_round(&config, &stop, &failing, &never, &metrics));
        assert_eq!(metrics.consecutive_failures.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn a_hung_canary_counts_as_timeout_failure() {
        let config = WatchdogConfig {
            interval: Duration::from_millis(1),
            timeout: Duration::from_millis(50),
            failure_threshold: 1,
        };
        let stop = Arc::new(AtomicBool::new(false));
        // The canary sleeps far past the round timeout; the round thread is
        // deliberately left running (and rejoined here to keep the test
        // harness clean).
        let (hung_tx, hung_rx) = std::sync::mpsc::channel::<()>();
        let hung_rx = Arc::new(std::sync::Mutex::new(hung_rx));
        let signal = Arc::clone(&hung_rx);
        let canary: Canary = Arc::new(move || {
            let _ = signal.lock().unwrap().recv();
            Ok(())
        });
        let metrics = CanaryMetrics::new();
        let fired = Arc::new(AtomicBool::new(false));
        let fired_flag = Arc::clone(&fired);
        let on_wedge: WedgeHook = Arc::new(move |_| {
            fired_flag.store(true, Ordering::Release);
        });

        let wedged = run_round(&config, &stop, &canary, &on_wedge, &metrics);
        drop(hung_tx);
        assert!(wedged, "single-round threshold must fire on a timeout");
        assert!(fired.load(Ordering::Acquire));
        assert_eq!(metrics.last_status.load(Ordering::Relaxed), CANARY_TIMED_OUT);
    }

    struct WatchdogReportSnapshot {
        failures: u32,
        message: &'static str,
    }
}
