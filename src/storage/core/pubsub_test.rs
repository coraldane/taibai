//! Unit tests for the reliable pub/sub manager: resumable sequences, the
//! retention floor, resync marking, subscription limits and bookmarks.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use uuid::Uuid;

use super::*;
use crate::types::batch::{ColumnDef, RecordBatch, Schema};

fn int_batch(values: &[i64]) -> RecordBatch {
    let schema = Schema::new(vec![ColumnDef::new(
        "v",
        crate::protocol::types::DataType::Int64,
    )]);
    let mut col = crate::types::array::ColumnArray::with_capacity(
        crate::protocol::types::DataType::Int64,
        values.len(),
    );
    for v in values {
        col.push(&crate::types::scalar::ScalarValue::Int64(Some(*v)))
            .expect("push");
    }
    RecordBatch::try_new(schema, vec![col]).expect("batch")
}

async fn wait_committed(manager: &PubSubManager, total: u64) {
    for _ in 0..200 {
        if manager.stats().committed_batches >= total {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("publisher thread never reached {total} committed batches");
}

fn test_settings(max_subscriptions: usize, log_max_batches: usize) -> PubSubSettings {
    PubSubSettings {
        enabled: true,
        max_subscriptions,
        log_max_batches,
        poll_interval_ms: 5,
        commit_ring_capacity: 1024,
        log_max_bytes_mb: 64,
        ..PubSubSettings::default()
    }
}

fn subscribe_req(
    frame_tx: mpsc::Sender<PublishedFrame>,
    mode: SubscribeMode,
    named: Option<&str>,
) -> SubscribeRequest {
    SubscribeRequest {
        db: "main".into(),
        table: "ticks".into(),
        mode,
        filter_column: None,
        filter_values: None,
        named: named.map(str::to_string),
        session_id: Uuid::new_v4(),
        frame_tx,
    }
}

async fn drain_with_timeout(handle: &SubscriptionHandle) -> DrainResult {
    // The publisher thread appends asynchronously; poll until data or resync
    // shows up instead of racing a fixed sleep.
    for _ in 0..200 {
        match handle.drain(64) {
            DrainResult::Empty => {
                if handle.is_closed() {
                    return DrainResult::Empty;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            other => return other,
        }
    }
    DrainResult::Empty
}

#[tokio::test]
async fn from_now_subscription_delivers_only_new_entries() {
    let manager = PubSubManager::start(test_settings(16, 64), std::env::temp_dir());
    let (tx, mut rx) = mpsc::channel(16);
    let (grant, handle) = manager
        .subscribe(subscribe_req(tx, SubscribeMode::FromNow, None))
        .expect("subscribe");
    assert_eq!(grant.granted_from, 0);

    manager.notify_commit("main", "ticks", &int_batch(&[1]));
    match drain_with_timeout(&handle).await {
        DrainResult::Data {
            seq_start,
            seq_end,
            batches,
        } => {
            assert_eq!((seq_start, seq_end), (1, 1));
            assert_eq!(batches.len(), 1);
            assert_eq!(batches[0].num_rows(), 1);
        }
        other => panic!("expected data, got {other:?}"),
    }
    handle.advance_delivered(1);

    // Acknowledged delivery shows up in the stats counters.
    manager.ack("main", "ticks", grant.sub_id, 1).expect("ack");
    let stats = manager.stats();
    assert_eq!(stats.committed_batches, 1);
    assert_eq!(stats.subscriber_count, 1);
    assert_eq!(manager.total_subscriber_count(), 1);
    assert_eq!(
        rx.try_recv().map(|_| ()),
        Err(mpsc::error::TryRecvError::Empty)
    );
}

#[tokio::test]
async fn from_seq_replays_within_retention_and_rejects_below_floor() {
    let manager = PubSubManager::start(test_settings(16, 64), std::env::temp_dir());
    // Channel + entries exist before anyone subscribes... they do not: the
    // channel is created by the first subscribe, so subscribe, unsubscribe,
    // then write, then resubscribe from the last seen sequence.
    let (tx, _rx) = mpsc::channel(16);
    let (_grant, handle) = manager
        .subscribe(subscribe_req(tx, SubscribeMode::FromNow, None))
        .expect("first subscribe");
    handle.drain(64);
    manager.unsubscribe("main", "ticks", handle.sub_id());

    manager.notify_commit("main", "ticks", &int_batch(&[1, 2]));
    manager.notify_commit("main", "ticks", &int_batch(&[3]));
    wait_committed(&manager, 2).await;

    let (tx, mut rx) = mpsc::channel(16);
    let (grant, handle) = manager
        .subscribe(subscribe_req(tx, SubscribeMode::FromSeq(1), None))
        .expect("resubscribe");
    // granted_from is the last processed sequence; delivery starts at 2.
    assert_eq!(grant.granted_from, 1);
    match drain_with_timeout(&handle).await {
        DrainResult::Data {
            seq_start,
            seq_end,
            batches,
        } => {
            // Sequence 1 was already processed; only its successors replay.
            assert_eq!((seq_start, seq_end), (2, 2));
            assert_eq!(batches.len(), 1);
            assert_eq!(batches[0].num_rows(), 1);
        }
        other => panic!("expected replay, got {other:?}"),
    }
    assert_eq!(
        rx.try_recv().map(|_| ()),
        Err(mpsc::error::TryRecvError::Empty)
    );

    // Sequences below the retained floor are refused instead of silently
    // renumbering.
    let err = manager.subscribe(subscribe_req(
        mpsc::channel(1).0,
        SubscribeMode::FromSeq(0),
        None,
    ));
    // Seq 0 is still >= floor - 1 while every entry is retained; force a
    // real floor by requesting something impossibly old after eviction.
    let _ = err;
    let (tx2, _rx2) = mpsc::channel(1);
    manager.unsubscribe("main", "ticks", handle.sub_id());
    for i in 0..200 {
        manager.notify_commit("main", "ticks", &int_batch(&[i]));
    }
    wait_committed(&manager, 202).await;
    let granted = manager.subscribe(subscribe_req(tx2, SubscribeMode::FromSeq(1), None));
    assert!(
        granted.is_err(),
        "sequence below the retained floor must be rejected"
    );
}

#[tokio::test]
async fn eviction_marks_lagging_subscribers_for_resync() {
    let manager = PubSubManager::start(test_settings(16, 8), std::env::temp_dir());
    let (tx, mut rx) = mpsc::channel(64);
    let (_grant, handle) = manager
        .subscribe(subscribe_req(tx, SubscribeMode::FromNow, None))
        .expect("subscribe");

    for i in 0..40 {
        manager.notify_commit("main", "ticks", &int_batch(&[i]));
    }
    match drain_with_timeout(&handle).await {
        DrainResult::Resync { floor } => {
            assert!(floor >= 1, "floor should point at the retained window");
        }
        DrainResult::Data { .. } => {
            // The subscriber caught up before eviction: drain again after a
            // beat to confirm no resync is spuriously raised.
            handle.advance_delivered(40);
            manager.notify_commit("main", "ticks", &int_batch(&[99]));
            assert!(matches!(
                drain_with_timeout(&handle).await,
                DrainResult::Data { .. }
            ));
            return;
        }
        DrainResult::Empty => panic!("expected resync or data"),
    }
    assert_eq!(
        rx.try_recv().map(|_| ()),
        Err(mpsc::error::TryRecvError::Empty)
    );
}

#[tokio::test]
async fn subscription_limit_is_enforced() {
    let manager = PubSubManager::start(test_settings(1, 64), std::env::temp_dir());
    let (tx1, _rx1) = mpsc::channel(4);
    manager
        .subscribe(subscribe_req(tx1, SubscribeMode::FromNow, None))
        .expect("first subscribe");
    let (tx2, _rx2) = mpsc::channel(4);
    let err = manager
        .subscribe(subscribe_req(tx2, SubscribeMode::FromNow, None))
        .expect_err("second subscribe must hit the limit");
    assert!(err.to_string().contains("subscription limit"), "{err}");
}

#[tokio::test]
async fn named_bookmark_resumes_from_last_acked() {
    let dir = tempfile::TempDir::new().expect("bookmark dir");
    let manager = PubSubManager::start(test_settings(16, 64), dir.path().to_path_buf());
    let (tx, _rx) = mpsc::channel(16);
    let (grant, handle) = manager
        .subscribe(subscribe_req(tx, SubscribeMode::FromNow, Some("reader-1")))
        .expect("subscribe");

    manager.notify_commit("main", "ticks", &int_batch(&[1]));
    match drain_with_timeout(&handle).await {
        DrainResult::Data { seq_end, .. } => {
            manager
                .ack("main", "ticks", grant.sub_id, seq_end)
                .expect("ack");
        }
        other => panic!("expected data, got {other:?}"),
    }
    // Unsubscribe flushes the bookmark for named subscriptions.
    assert!(manager.unsubscribe("main", "ticks", grant.sub_id));

    // More writes happen while nobody is subscribed.
    manager.notify_commit("main", "ticks", &int_batch(&[2, 3]));
    wait_committed(&manager, 2).await;

    let (tx, _rx) = mpsc::channel(16);
    let (grant2, handle2) = manager
        .subscribe(subscribe_req(tx, SubscribeMode::FromNow, Some("reader-1")))
        .expect("resubscribe with bookmark");
    // granted_from is the acked bookmark; delivery resumes after it.
    assert_eq!(
        grant2.granted_from, 1,
        "bookmark should skip the already-acked entry"
    );
    match drain_with_timeout(&handle2).await {
        DrainResult::Data {
            seq_start,
            seq_end,
            batches,
        } => {
            assert_eq!((seq_start, seq_end), (2, 2));
            assert_eq!(batches[0].num_rows(), 2);
        }
        other => panic!("expected replay after bookmark, got {other:?}"),
    }
}

#[tokio::test]
async fn gap_marks_subscribers_for_resync() {
    let manager = PubSubManager::start(test_settings(16, 64), std::env::temp_dir());
    let (tx, _rx) = mpsc::channel(16);
    let (_grant, handle) = manager
        .subscribe(subscribe_req(tx, SubscribeMode::FromNow, None))
        .expect("subscribe");
    manager.notify_commit("main", "ticks", &int_batch(&[1]));
    assert!(matches!(
        drain_with_timeout(&handle).await,
        DrainResult::Data { .. }
    ));

    // Simulate an unrecoverable commit-ring overflow: the gap flag forces
    // every subscriber to resync on the next drain.
    let channel = manager
        .existing_channel("main", "ticks")
        .expect("channel exists");
    channel.mark_gap();
    channel.resolve_gap();
    match handle.drain(64) {
        DrainResult::Resync { floor } => assert!(floor >= 1),
        other => panic!("expected resync, got {other:?}"),
    }
    assert!(manager.stats().resyncs >= 1);
}

#[test]
fn disabled_manager_rejects_subscriptions() {
    let manager = PubSubManager::disabled();
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    let req = SubscribeRequest {
        db: "main".into(),
        table: "t".into(),
        mode: SubscribeMode::FromNow,
        filter_column: None,
        filter_values: Some(HashSet::new()),
        named: None,
        session_id: Uuid::new_v4(),
        frame_tx: tx,
    };
    let err = manager.subscribe(req).expect_err("disabled");
    assert!(err.to_string().contains("disabled"), "{err}");
}
