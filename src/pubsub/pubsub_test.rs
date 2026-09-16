use std::time::Duration;

use super::{PubSubHub, TopicPattern};
use crate::common::config::PubSubSettings;
use crate::protocol::message::{read_len_prefixed_string, MSG_TYPE_NOTIFICATION};
use crate::protocol::types::DataType;
use crate::types::array::ColumnArray;
use crate::types::batch::{ColumnDef, RecordBatch, Schema};
use crate::types::bitmap::NullBitmap;

fn test_settings(event_capacity: usize, notification_capacity: usize) -> PubSubSettings {
    PubSubSettings {
        enabled: true,
        event_channel_capacity: event_capacity,
        max_subscriptions: 16,
        notification_channel_capacity: notification_capacity,
        compress_threshold: usize::MAX,
    }
}

fn int_batch(values: Vec<i64>) -> RecordBatch {
    let schema = Schema::new(vec![ColumnDef::new("id", DataType::Int64)]);
    RecordBatch::try_new(
        schema,
        vec![ColumnArray::Int64 {
            bitmap: NullBitmap::new_valid(values.len()),
            data: values,
        }],
    )
    .expect("valid batch")
}

#[test]
fn topic_pattern_parse_and_match() {
    let exact = TopicPattern::parse("main.ticks").unwrap();
    assert!(exact.matches("main", "ticks"));
    assert!(!exact.matches("main", "trades"));
    assert!(!exact.matches("other", "ticks"));

    let db_wild = TopicPattern::parse("main.*").unwrap();
    assert!(db_wild.matches("main", "ticks"));
    assert!(!db_wild.matches("other", "ticks"));

    let all = TopicPattern::parse(">").unwrap();
    assert!(all.matches("main", "ticks"));
    assert!(all.matches("a", "b"));

    assert!(TopicPattern::parse("").is_err());
    assert!(TopicPattern::parse("a.b.c").is_err());
    assert!(TopicPattern::parse("a.").is_err());
    assert!(TopicPattern::parse("a.b>").is_err());
    assert!(TopicPattern::parse(">.*").is_err());
}

#[test]
fn publish_delivers_notification_with_seq_and_topic() {
    let hub = PubSubHub::new(test_settings(64, 8));
    let (push_tx, mut push_rx) = tokio::sync::mpsc::channel(8);
    let conn = hub.create_conn(push_tx);
    assert!(!hub.has_subscribers());
    let sub_id = hub.subscribe(&conn, "main.ticks").expect("subscribe");
    assert!(hub.has_subscribers());

    hub.publish("main", "ticks", int_batch(vec![1, 2, 3]));
    let msg = recv_with_timeout(&mut push_rx, Duration::from_secs(5)).expect("notification");
    assert_eq!(msg.msg_type, MSG_TYPE_NOTIFICATION);

    let (sub_id_read, rest) = (
        u64::from_le_bytes(msg.payload[..8].try_into().unwrap()),
        &msg.payload[8..],
    );
    let (seq, rest) = (
        u64::from_le_bytes(rest[..8].try_into().unwrap()),
        &rest[8..],
    );
    let (topic, rest) = read_len_prefixed_string(rest).expect("topic");
    assert_eq!(sub_id_read, sub_id);
    assert_eq!(seq, 1);
    assert_eq!(topic, "main.ticks");
    // encoded batch body present
    assert!(rest.len() > 4);

    // The fan-out worker bumps the sent counter right after try_send; under
    // load the notification can be observed before the counter lands. Wait
    // for the stats to catch up instead of asserting immediately.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let stats = hub.subscription_stats();
        if stats.len() == 1 && stats[0].sent == 1 && stats[0].dropped == 0 {
            break;
        }
        if std::time::Instant::now() > deadline {
            panic!("expected sent=1 dropped=0, got {stats:?}");
        }
        std::thread::sleep(Duration::from_millis(5));
    }

    // second publish bumps the per-topic sequence
    hub.publish("main", "ticks", int_batch(vec![4]));
    let msg2 =
        recv_with_timeout(&mut push_rx, Duration::from_secs(5)).expect("second notification");
    let seq2 = u64::from_le_bytes(msg2.payload[8..16].try_into().unwrap());
    assert_eq!(seq2, 2);

    hub.shutdown();
}

#[test]
fn publish_is_filtered_by_pattern() {
    let hub = PubSubHub::new(test_settings(64, 8));
    let (push_tx, mut push_rx) = tokio::sync::mpsc::channel(8);
    let conn = hub.create_conn(push_tx);
    hub.subscribe(&conn, "main.ticks").expect("subscribe");

    hub.publish("main", "trades", int_batch(vec![1]));
    hub.publish("other", "ticks", int_batch(vec![1]));
    assert!(
        recv_with_timeout(&mut push_rx, Duration::from_millis(200)).is_none(),
        "non-matching publishes must not deliver"
    );

    hub.publish("main", "ticks", int_batch(vec![1]));
    assert!(recv_with_timeout(&mut push_rx, Duration::from_secs(5)).is_some());
    hub.shutdown();
}

#[test]
fn slow_consumer_drops_without_blocking() {
    let hub = PubSubHub::new(test_settings(64, 1));
    let (push_tx, _push_rx_kept) = tokio::sync::mpsc::channel(1);
    let conn = hub.create_conn(push_tx);
    hub.subscribe(&conn, ">").expect("subscribe");

    // Fill the channel, then overflow it several times. try_send must never
    // block and the subscription must account the drops.
    for i in 0..16 {
        hub.publish("db", "t", int_batch(vec![i as i64]));
    }
    // The fan-out worker drains the event channel asynchronously: wait until
    // every event has been accounted as either sent or dropped instead of
    // racing the first drop.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let stats = hub.subscription_stats();
        if stats[0].sent + stats[0].dropped == 16 {
            assert!(stats[0].dropped >= 1);
            break;
        }
        if std::time::Instant::now() > deadline {
            panic!(
                "expected all 16 events to be accounted, got sent={} dropped={}",
                stats[0].sent, stats[0].dropped
            );
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    hub.shutdown();
}

#[test]
fn unsubscribe_and_disconnect_stop_delivery() {
    let hub = PubSubHub::new(test_settings(64, 8));
    let (push_tx, mut push_rx) = tokio::sync::mpsc::channel(8);
    let conn = hub.create_conn(push_tx);
    let sub_id = hub.subscribe(&conn, "main.ticks").expect("subscribe");

    assert!(!hub.unsubscribe(&conn, 9999), "unknown sub id");
    assert!(hub.unsubscribe(&conn, sub_id));
    assert_eq!(hub.subscriber_count(), 0);

    hub.publish("main", "ticks", int_batch(vec![1]));
    assert!(recv_with_timeout(&mut push_rx, Duration::from_millis(200)).is_none());

    // A closed connection channel is pruned by the fan-out thread.
    let _sub_id2 = {
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        let conn2 = hub.create_conn(tx);
        let sid = hub.subscribe(&conn2, "main.ticks").expect("subscribe2");
        drop(rx);
        sid
    };
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while hub.subscriber_count() > 0 {
        hub.publish("main", "ticks", int_batch(vec![1]));
        if std::time::Instant::now() > deadline {
            panic!("stale connection was not pruned");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(hub.subscriber_count(), 0);
    hub.shutdown();
}

#[test]
fn event_channel_overflow_drops_and_counts() {
    let hub = PubSubHub::new(test_settings(2, 64));
    let (push_tx, push_rx) = tokio::sync::mpsc::channel(64);
    let conn = hub.create_conn(push_tx);
    hub.subscribe(&conn, ">").expect("subscribe");
    // The receiver is never drained by nobody... keep receiver alive unused.
    let _keep = push_rx;

    for i in 0..256 {
        hub.publish("db", "t", int_batch(vec![i]));
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while hub.dropped_events() == 0 {
        if std::time::Instant::now() > deadline {
            panic!("expected hub-level dropped events");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    hub.shutdown();
}

#[test]
fn publish_with_no_subscribers_is_free() {
    let hub = PubSubHub::new(test_settings(64, 8));
    hub.publish("main", "ticks", int_batch(vec![1]));
    assert_eq!(hub.subscriber_count(), 0);
    hub.shutdown();
}

#[test]
fn disabled_hub_rejects_subscriptions() {
    let settings = PubSubSettings {
        enabled: false,
        ..test_settings(64, 8)
    };
    let hub = PubSubHub::new(settings);
    let (push_tx, _rx) = tokio::sync::mpsc::channel(8);
    let conn = hub.create_conn(push_tx);
    assert!(hub.subscribe(&conn, "main.ticks").is_err());
    assert!(!hub.has_subscribers());
    hub.shutdown();
}

fn recv_with_timeout(
    rx: &mut tokio::sync::mpsc::Receiver<crate::protocol::message::Message>,
    dur: Duration,
) -> Option<crate::protocol::message::Message> {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("test runtime")
        .block_on(async { tokio::time::timeout(dur, rx.recv()).await.ok().flatten() })
}
