//! In-process publish/subscribe for committed table changes.
//!
//! The design borrows from two systems:
//!
//! * kdb+ tickerplant: the change feed is insert-driven, carries per-topic
//!   monotonically increasing sequence numbers, and every subscriber drains
//!   its own queue so one slow consumer never delays another.
//! * NATS: topics are dot-separated subjects with `*` / `>` wildcard
//!   matching, delivery is at-most-once, and a slow consumer hits a bounded
//!   queue where overflow drops messages (counted) instead of blocking the
//!   publisher.
//!
//! Non-goals that keep the database unaffected: the write path never blocks,
//! locks, allocates rows, or waits on subscribers. Publishing is an
//! `AtomicU64` load when nobody listens and a lock-free `try_send` when
//! someone does; the fan-out work (batch encoding, compression, per-subscriber
//! queueing) happens on a dedicated thread. All queues are bounded so memory
//! stays proportional to the configured capacities, never to write volume.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use bytes::Bytes;
use crossbeam_channel::{RecvTimeoutError, Sender};
use tokio::sync::mpsc;

use crate::common::config::PubSubSettings;
use crate::protocol::compress;
use crate::protocol::message::{write_len_prefixed_string, Message, MSG_TYPE_NOTIFICATION};
use crate::protocol::result::encode_binary_result;
use crate::types::batch::RecordBatch;

/// The fan-out thread parks on the event channel; the stop flag is polled
/// with this cadence when the channel is idle.
const FANOUT_POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Log at most one warning per this many dropped events so a saturated feed
/// cannot turn into a log flood on the write path.
const DROP_WARN_EVERY: u64 = 100_000;

/// A subscription filter over `db.table` subjects.
///
/// Tokens: an exact name, `*` (exactly one token), or a trailing `>` (every
/// remaining token). Examples: `main.ticks`, `main.*`, `>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicPattern {
    tokens: Vec<String>,
    raw: String,
}

impl TopicPattern {
    pub fn parse(pattern: &str) -> Result<Self, String> {
        let raw = pattern.trim();
        if raw.is_empty() {
            return Err("empty subscription topic".to_string());
        }
        let tokens: Vec<String> = raw.split('.').map(str::trim).map(str::to_string).collect();
        if tokens.len() > 2 {
            return Err(format!(
                "invalid subscription topic '{raw}': expected 'database.table', 'database.*' or '>'"
            ));
        }
        if tokens.iter().any(String::is_empty) {
            return Err(format!("invalid subscription topic '{raw}': empty token"));
        }
        for token in &tokens {
            let is_wildcard = token == "*" || token == ">";
            let has_wildcard = token.contains('*') || token.contains('>');
            if has_wildcard && !is_wildcard {
                return Err(format!(
                    "invalid subscription topic '{raw}': wildcards must fill a whole token"
                ));
            }
        }
        if let Some(non_final) = tokens.iter().take(tokens.len() - 1).find(|t| *t == ">") {
            let _ = non_final;
            return Err(format!(
                "invalid subscription topic '{raw}': '>' is only allowed as the last token"
            ));
        }
        Ok(Self {
            tokens,
            raw: raw.to_string(),
        })
    }

    pub fn as_str(&self) -> &str {
        &self.raw
    }

    pub fn matches(&self, db: &str, table: &str) -> bool {
        let subject = [db, table];
        for (idx, token) in self.tokens.iter().enumerate() {
            if token == ">" {
                return true;
            }
            let Some(candidate) = subject.get(idx) else {
                return false;
            };
            if token != "*" && !token.eq_ignore_ascii_case(candidate) {
                return false;
            }
        }
        self.tokens.len() == 2
    }
}

/// The per-connection sink notifications are queued into. The server owns the
/// receiving half (a bounded tokio mpsc channel drained by the connection
/// loop); the hub only ever `try_send`s, so a stalled client can never block
/// the fan-out thread.
pub struct ConnHandle {
    push_tx: mpsc::Sender<Message>,
    conn_id: u64,
}

impl ConnHandle {
    pub fn conn_id(&self) -> u64 {
        self.conn_id
    }
}

/// One registered subscription: a pattern plus the connection it delivers to.
pub struct Subscription {
    id: u64,
    pattern: TopicPattern,
    conn: Arc<ConnHandle>,
    sent: AtomicU64,
    dropped: AtomicU64,
}

impl Subscription {
    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn topic(&self) -> &str {
        self.pattern.as_str()
    }

    pub fn sent(&self) -> u64 {
        self.sent.load(Ordering::Relaxed)
    }

    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

#[derive(Debug)]
enum PubEvent {
    Change {
        db: String,
        table: String,
        batch: Arc<RecordBatch>,
        /// Assigned at publish time so events dropped by a full channel
        /// still consume their sequence: subscribers observe the gap.
        seq: u64,
    },
    Shutdown,
}

/// Snapshot of per-subscription counters, for introspection and tests.
#[derive(Debug, Clone)]
pub struct SubscriptionStats {
    pub id: u64,
    pub topic: String,
    pub sent: u64,
    pub dropped: u64,
}

struct FanoutState {
    subs: RwLock<Vec<Arc<Subscription>>>,
    subscriber_count: AtomicU64,
    compress_threshold: usize,
    stop: Arc<AtomicBool>,
}

pub struct PubSubHub {
    settings: PubSubSettings,
    event_tx: Sender<PubEvent>,
    event_rx: Mutex<Option<crossbeam_channel::Receiver<PubEvent>>>,
    subs: Arc<FanoutState>,
    next_sub_id: AtomicU64,
    next_conn_id: AtomicU64,
    dropped_events: AtomicU64,
    /// Per-topic notification sequence, assigned at publish time (not in the
    /// fan-out thread) so dropped events consume a seq and stay detectable.
    topic_seqs: Mutex<HashMap<String, u64>>,
    worker: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl PubSubHub {
    pub fn new(settings: PubSubSettings) -> Arc<Self> {
        let (event_tx, event_rx) =
            crossbeam_channel::bounded(settings.event_channel_capacity.clamp(1, 1_048_576));
        let fanout = Arc::new(FanoutState {
            subs: RwLock::new(Vec::new()),
            subscriber_count: AtomicU64::new(0),
            compress_threshold: settings.compress_threshold,
            stop: Arc::new(AtomicBool::new(false)),
        });
        let hub = Arc::new(Self {
            event_tx,
            event_rx: Mutex::new(Some(event_rx)),
            subs: fanout,
            settings,
            next_sub_id: AtomicU64::new(1),
            next_conn_id: AtomicU64::new(1),
            dropped_events: AtomicU64::new(0),
            topic_seqs: Mutex::new(HashMap::new()),
            worker: Mutex::new(None),
        });
        hub.start_worker();
        hub
    }

    fn start_worker(&self) {
        let Some(rx) = self.event_rx.lock().unwrap().take() else {
            return;
        };
        let fanout = Arc::clone(&self.subs);
        let worker = std::thread::Builder::new()
            .name("taibai-pubsub".into())
            .spawn(move || Self::fanout_loop(&rx, &fanout))
            .expect("failed to spawn pubsub fan-out thread");
        *self.worker.lock().unwrap() = Some(worker);
    }

    pub fn settings(&self) -> &PubSubSettings {
        &self.settings
    }

    /// Cheap gate for the write path: true only when publishing can reach at
    /// least one subscriber. A single relaxed atomic load.
    pub fn has_subscribers(&self) -> bool {
        self.settings.enabled && self.subs.subscriber_count.load(Ordering::Relaxed) > 0
    }

    pub fn subscriber_count(&self) -> u64 {
        self.subs.subscriber_count.load(Ordering::Relaxed)
    }

    pub fn dropped_events(&self) -> u64 {
        self.dropped_events.load(Ordering::Relaxed)
    }

    /// Enqueue a committed change for fan-out. Never blocks: when the event
    /// channel is full the event is dropped and counted, matching NATS
    /// at-most-once semantics. Sequence continuity lets subscribers detect
    /// the gap and re-sync with a query.
    pub fn publish(&self, db: &str, table: &str, batch: RecordBatch) {
        self.publish_shared(db, table, Arc::new(batch));
    }

    /// Same as [`publish`](Self::publish) for a batch that is already shared
    /// (e.g. a memtable snapshot), avoiding a column copy on the caller.
    pub fn publish_shared(&self, db: &str, table: &str, batch: Arc<RecordBatch>) {
        if !self.has_subscribers() || batch.num_rows() == 0 {
            return;
        }
        // Assign the sequence here, before the channel accepts the event: an
        // event dropped by a full channel still consumes its seq, so the
        // sequence gap subscribers observe covers hub-level drops too.
        let seq = {
            let mut seqs = self.topic_seqs.lock().unwrap();
            let seq = seqs
                .entry(format!("{db}.{table}"))
                .or_insert(0);
            *seq = seq.wrapping_add(1);
            *seq
        };
        let event = PubEvent::Change {
            db: db.to_string(),
            table: table.to_string(),
            batch,
            seq,
        };
        if self.event_tx.try_send(event).is_err() {
            self.record_dropped_event();
        }
    }

    fn record_dropped_event(&self) {
        let dropped = self.dropped_events.fetch_add(1, Ordering::Relaxed) + 1;
        if dropped % DROP_WARN_EVERY == 1 {
            tracing::warn!(
                dropped_events = dropped,
                "pubsub event channel full; dropping change events (subscribers will see sequence gaps)"
            );
        }
    }

    /// Create the per-connection delivery handle. The caller (the connection
    /// task) keeps the handle for the connection lifetime and unregisters it
    /// on disconnect.
    pub fn create_conn(&self, push_tx: mpsc::Sender<Message>) -> Arc<ConnHandle> {
        Arc::new(ConnHandle {
            push_tx,
            conn_id: self.next_conn_id.fetch_add(1, Ordering::Relaxed),
        })
    }

    /// Register a subscription and return its id.
    pub fn subscribe(&self, conn: &Arc<ConnHandle>, topic: &str) -> Result<u64, String> {
        if !self.settings.enabled {
            return Err("pubsub is disabled".to_string());
        }
        let pattern = TopicPattern::parse(topic)?;
        let mut subs = self.subs.subs.write().unwrap();
        if subs.len() >= self.settings.max_subscriptions.max(1) {
            return Err(format!(
                "subscription limit reached ({})",
                self.settings.max_subscriptions
            ));
        }
        let id = self.next_sub_id.fetch_add(1, Ordering::Relaxed);
        subs.push(Arc::new(Subscription {
            id,
            pattern,
            conn: Arc::clone(conn),
            sent: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
        }));
        self.subs.subscriber_count.fetch_add(1, Ordering::Relaxed);
        Ok(id)
    }

    /// Remove one subscription of this connection. Returns false when the id
    /// is unknown or belongs to another connection.
    pub fn unsubscribe(&self, conn: &Arc<ConnHandle>, sub_id: u64) -> bool {
        let mut subs = self.subs.subs.write().unwrap();
        let before = subs.len();
        subs.retain(|sub| !(sub.id == sub_id && Arc::ptr_eq(&sub.conn, conn)));
        let removed = before - subs.len();
        if removed > 0 {
            self.subs
                .subscriber_count
                .fetch_sub(removed as u64, Ordering::Relaxed);
        }
        removed > 0
    }

    /// Drop every subscription of a disconnecting connection.
    pub fn drop_connection(&self, conn: &Arc<ConnHandle>) {
        let mut subs = self.subs.subs.write().unwrap();
        let before = subs.len();
        subs.retain(|sub| !Arc::ptr_eq(&sub.conn, conn));
        let removed = before - subs.len();
        if removed > 0 {
            self.subs
                .subscriber_count
                .fetch_sub(removed as u64, Ordering::Relaxed);
            tracing::debug!(
                conn_id = conn.conn_id(),
                removed,
                "pubsub subscriptions removed on disconnect"
            );
        }
    }

    pub fn subscription_stats(&self) -> Vec<SubscriptionStats> {
        self.subs
            .subs
            .read()
            .unwrap()
            .iter()
            .map(|sub| SubscriptionStats {
                id: sub.id,
                topic: sub.pattern.as_str().to_string(),
                sent: sub.sent(),
                dropped: sub.dropped(),
            })
            .collect()
    }

    /// Stop the fan-out thread. Pending events in the channel are dropped;
    /// callers invoke this during server shutdown.
    pub fn shutdown(&self) {
        self.subs.stop.store(true, Ordering::Release);
        let _ = self.event_tx.try_send(PubEvent::Shutdown);
        if let Some(worker) = self.worker.lock().unwrap().take() {
            let _ = worker.join();
        }
    }

    fn fanout_loop(rx: &crossbeam_channel::Receiver<PubEvent>, state: &FanoutState) {
        loop {
            match rx.recv_timeout(FANOUT_POLL_INTERVAL) {
                Ok(PubEvent::Change {
                    db,
                    table,
                    batch,
                    seq,
                }) => {
                    Self::fanout_change(state, &db, &table, batch, seq);
                }
                Ok(PubEvent::Shutdown) => break,
                Err(RecvTimeoutError::Timeout) => {
                    if state.stop.load(Ordering::Acquire) {
                        break;
                    }
                }
                Err(RecvTimeoutError::Disconnected) => break,
            }
        }
    }

    fn fanout_change(
        state: &FanoutState,
        db: &str,
        table: &str,
        batch: Arc<RecordBatch>,
        seq: u64,
    ) {
        let topic = format!("{db}.{table}");
        // Encode and compress once; every subscriber shares the same buffer.
        let mut encoded = encode_binary_result(&batch);
        if encoded.len() > state.compress_threshold {
            if let Ok(compressed) = compress::zlib_compress(&encoded) {
                encoded = compressed;
            }
        }
        let encoded = Bytes::from(encoded);

        let mut stale: Vec<Arc<Subscription>> = Vec::new();
        {
            let subs = state.subs.read().unwrap();
            if subs.is_empty() {
                return;
            }
            for sub in subs.iter() {
                if !sub.pattern.matches(db, table) {
                    continue;
                }
                let mut payload = Vec::with_capacity(16 + topic.len() + 4 + encoded.len());
                payload.extend_from_slice(&sub.id.to_le_bytes());
                payload.extend_from_slice(&seq.to_le_bytes());
                write_len_prefixed_string(&mut payload, &topic);
                payload.extend_from_slice(&encoded);
                let msg = Message::new(MSG_TYPE_NOTIFICATION, Bytes::from(payload));
                match sub.conn.push_tx.try_send(msg) {
                    Ok(()) => {
                        sub.sent.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                        // NATS-style slow consumer: drop and count, never block.
                        sub.dropped.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                        stale.push(Arc::clone(sub));
                    }
                }
            }
        }
        if !stale.is_empty() {
            let mut subs = state.subs.write().unwrap();
            let before = subs.len();
            subs.retain(|sub| !stale.iter().any(|dead| Arc::ptr_eq(dead, sub)));
            let removed = before - subs.len();
            if removed > 0 {
                state
                    .subscriber_count
                    .fetch_sub(removed as u64, Ordering::Relaxed);
            }
        }
    }
}

impl Drop for PubSubHub {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(test)]
#[path = "pubsub_test.rs"]
mod tests;
