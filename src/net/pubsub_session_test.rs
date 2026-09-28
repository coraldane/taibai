//! Round-trip tests for the reliable-subscription wire payloads and the
//! subscriber-side row filter.

use std::collections::HashSet;
use std::sync::Arc;

use super::*;
use crate::net::bulk_importer::encode_columnar_bulk_insert;
use crate::protocol::message::{MSG_TYPE_SUB_CONFIRMED, MSG_TYPE_SUB_DATA, MSG_TYPE_SUB_ERROR};
use crate::types::array::ColumnArray;
use crate::types::batch::{ColumnDef, RecordBatch, Schema};

fn ticks_batch(rows: &[(&str, f64)]) -> RecordBatch {
    let schema = Schema::new(vec![
        ColumnDef::new("symbol", crate::protocol::types::DataType::VarChar),
        ColumnDef::new("price", crate::protocol::types::DataType::Float64),
    ]);
    let mut symbols =
        ColumnArray::with_capacity(crate::protocol::types::DataType::VarChar, rows.len());
    let mut prices =
        ColumnArray::with_capacity(crate::protocol::types::DataType::Float64, rows.len());
    for (symbol, price) in rows {
        symbols
            .push(&crate::types::scalar::ScalarValue::VarChar(Some(
                (*symbol).to_string(),
            )))
            .expect("push symbol");
        prices
            .push(&crate::types::scalar::ScalarValue::Float64(Some(*price)))
            .expect("push price");
    }
    RecordBatch::try_new(schema, vec![symbols, prices]).expect("batch")
}

#[test]
fn subscribe_payload_round_trip() {
    let values: HashSet<String> = ["AAPL".to_string(), "MSFT".to_string()].into();
    let wire = encode_subscribe(
        "main",
        "ticks",
        Some(42),
        Some("reader-1"),
        Some("symbol"),
        Some(&values),
    );
    let decoded = decode_subscribe(&wire).expect("decode");
    assert_eq!(decoded.db, "main");
    assert_eq!(decoded.table, "ticks");
    assert_eq!(decoded.from_seq, Some(42));

    let plain = decode_subscribe(&encode_subscribe("main", "ticks", None, None, None, None))
        .expect("decode plain");
    assert_eq!(plain.from_seq, None);
    assert!(plain.named.is_none());
    assert!(plain.filter_column.is_none());
    assert!(plain.filter_values.is_none());

    let named = decode_subscribe(&encode_subscribe(
        "main",
        "ticks",
        None,
        Some("r"),
        None,
        None,
    ))
    .expect("decode named");
    assert_eq!(named.named.as_deref(), Some("r"));

    assert!(decode_subscribe(&wire).is_ok());
    // Trailing garbage is rejected instead of silently ignored.
    let mut corrupt = wire.to_vec();
    corrupt.push(0xFF);
    assert!(decode_subscribe(&corrupt).is_err());
}

#[test]
fn confirmed_and_error_payloads_round_trip() {
    let grant = SubscribeGrant {
        sub_id: 7,
        epoch: 123,
        granted_from: 5,
        floor: 2,
    };
    let decoded = decode_sub_confirmed(&encode_sub_confirmed(&grant)).expect("decode grant");
    assert_eq!(decoded.sub_id, 7);
    assert_eq!(decoded.epoch, 123);
    assert_eq!(decoded.granted_from, 5);
    assert_eq!(decoded.floor, 2);

    let err = decode_sub_error(&encode_sub_error(9, SUB_ERROR_RESYNC_REQUIRED, 4, "gap"))
        .expect("decode error");
    assert_eq!(err.sub_id, 9);
    assert_eq!(err.code, SUB_ERROR_RESYNC_REQUIRED);
    assert_eq!(err.aux, 4);
    assert_eq!(err.message, "gap");

    let confirmed = confirmed_message(&grant);
    assert_eq!(confirmed.msg_type, MSG_TYPE_SUB_CONFIRMED);
    let data_error = error_message(1, SUB_ERROR_NOT_FOUND, 0, "missing");
    assert_eq!(data_error.msg_type, MSG_TYPE_SUB_ERROR);
}

#[test]
fn data_frame_round_trips_through_wire_encoding() {
    let batches = vec![
        Arc::new(ticks_batch(&[("AAPL", 191.2), ("MSFT", 420.1)])),
        Arc::new(ticks_batch(&[("AAPL", 192.0)])),
    ];
    let msg = frame_to_message(PublishedFrame {
        db: "main".into(),
        table: "ticks".into(),
        sub_id: 3,
        kind: FrameKind::Data {
            seq_start: 4,
            seq_end: 5,
            batches,
        },
    })
    .expect("encode frame");
    assert_eq!(msg.msg_type, MSG_TYPE_SUB_DATA);

    let frame = decode_sub_data(&msg.payload).expect("decode frame");
    assert_eq!(frame.sub_id, 3);
    assert_eq!(frame.seq_start, 4);
    assert_eq!(frame.seq_end, 5);
    assert_eq!(frame.table, "ticks");
    assert_eq!(frame.batches.len(), 2);
    assert_eq!(frame.batches[0].num_rows(), 2);
    assert_eq!(frame.batches[1].num_rows(), 1);
    assert_eq!(frame.batches[0].schema.columns[0].name, "symbol");
}

#[test]
fn resync_frame_becomes_sub_error() {
    let msg = frame_to_message(PublishedFrame {
        db: "main".into(),
        table: "ticks".into(),
        sub_id: 11,
        kind: FrameKind::ResyncRequired { floor: 9 },
    })
    .expect("encode frame");
    assert_eq!(msg.msg_type, MSG_TYPE_SUB_ERROR);
    let err = decode_sub_error(&msg.payload).expect("decode");
    assert_eq!(err.code, SUB_ERROR_RESYNC_REQUIRED);
    assert_eq!(err.aux, 9);
}

#[test]
fn apply_filter_keeps_only_matching_rows() {
    let batch = ticks_batch(&[("AAPL", 1.0), ("MSFT", 2.0), ("AAPL", 3.0)]);
    let values: HashSet<String> = ["AAPL".to_string()].into();

    let filtered = apply_filter(&batch, "symbol", &values)
        .expect("filter")
        .expect("rows kept");
    assert_eq!(filtered.num_rows(), 2);
    assert_eq!(
        filtered.scalar(0, 0).expect("scalar"),
        crate::types::scalar::ScalarValue::VarChar(Some("AAPL".into()))
    );
    assert_eq!(
        filtered.scalar(1, 1),
        Some(crate::types::scalar::ScalarValue::Float64(Some(3.0)))
    );

    // No match: nothing is delivered.
    let none_values: HashSet<String> = ["TSLA".to_string()].into();
    assert!(apply_filter(&batch, "symbol", &none_values)
        .expect("filter")
        .is_none());

    // All rows match: the batch passes through unchanged.
    let all_values: HashSet<String> = ["AAPL".to_string(), "MSFT".to_string()].into();
    let kept = apply_filter(&batch, "symbol", &all_values)
        .expect("filter")
        .expect("rows kept");
    assert_eq!(kept.num_rows(), 3);

    // NULLs never match; unknown columns are a protocol error.
    assert!(apply_filter(&batch, "nope", &values).is_err());
}

#[test]
fn filtered_batches_survive_wire_encoding() {
    // The delivery task re-encodes filtered batches as bulk bodies; the
    // gather must produce encodable columns.
    let batch = ticks_batch(&[("AAPL", 1.0), ("MSFT", 2.0), ("AAPL", 3.0)]);
    let values: HashSet<String> = ["AAPL".to_string()].into();
    let filtered = apply_filter(&batch, "symbol", &values)
        .expect("filter")
        .expect("rows");
    let body = encode_columnar_bulk_insert("ticks", &filtered).expect("encode");
    let decoded = crate::net::bulk_importer::decode_columnar_bulk_insert(&body).expect("decode");
    assert_eq!(decoded.num_rows(), 2);
    assert_eq!(decoded.schema.columns[1].name, "price");
}
