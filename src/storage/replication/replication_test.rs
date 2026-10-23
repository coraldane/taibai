use super::*;

fn tail_event(table: &str) -> ReplicationEvent {
    ReplicationEvent::TailAppend {
        db: "main".into(),
        table: table.into(),
        partition: "2026".into(),
        memtable_id: Uuid::new_v4(),
        first_lsn: 1,
        last_lsn: 2,
        frames: vec![1, 2, 3, 4],
    }
}

#[test]
fn event_codec_round_trips() {
    for event in [
        tail_event("t"),
        ReplicationEvent::TailMutation {
            db: "main".into(),
            table: "t".into(),
            memtable_id: Uuid::new_v4(),
            is_update: false,
            sequence: 42,
            payload: vec![0x52, 0x47, 0x42, 0x54, 1, 2, 3],
        },
        ReplicationEvent::TailMutation {
            db: "main".into(),
            table: "t".into(),
            memtable_id: Uuid::new_v4(),
            is_update: true,
            sequence: 43,
            payload: Vec::new(),
        },
        ReplicationEvent::SegmentUpdate {
            db: "main".into(),
            table: "t".into(),
            segment: "seg_00001.dat".into(),
            kind: SegmentUpdateKind::Prefix,
            memtable_ids: vec![Uuid::new_v4(), Uuid::new_v4()],
        },
        ReplicationEvent::SegmentUpdate {
            db: "main".into(),
            table: "t".into(),
            segment: "p2026_0001.dat".into(),
            kind: SegmentUpdateKind::Final,
            memtable_ids: Vec::new(),
        },
    ] {
        let decoded = ReplicationEvent::decode(&event.encode()).unwrap();
        assert_eq!(decoded, event);
    }
}

#[test]
fn event_decode_rejects_truncated_and_unknown() {
    let event = tail_event("t");
    let bytes = event.encode();
    assert!(ReplicationEvent::decode(&bytes[..bytes.len() - 1]).is_err());
    let mut unknown = bytes.clone();
    unknown[0..2].copy_from_slice(&999u16.to_le_bytes());
    assert!(ReplicationEvent::decode(&unknown).is_err());
    assert!(ReplicationEvent::decode(&[]).is_err());
}

#[test]
fn rep_log_sequences_ring_and_fans_out() {
    let dir = tempfile::tempdir().unwrap();
    // Capacity is floored at 16, so push past that to exercise eviction.
    let log = RepLog::new(16, u64::MAX, dir.path()).with_persist_every(u64::MAX);
    let (tx, rx) = crossbeam_channel::bounded(64);
    log.register_session(&tx);

    let mut seqs = Vec::new();
    for i in 0..20 {
        seqs.push(log.publish(tail_event(&format!("t{i}"))));
    }
    assert_eq!(
        seqs,
        (1..=20).collect::<Vec<_>>(),
        "seqs are dense and monotonic"
    );
    // Ring capacity 16: only seqs 5..=20 remain.
    assert_eq!(log.events_after(4).unwrap().len(), 16);

    // Resume inside the ring window works; beyond it errors.
    assert_eq!(log.events_after(4).unwrap()[0].0, 5);
    assert!(log.events_after(0).is_err(), "gap older than ring");

    // Session removal, then close the channel: `rx.iter()` blocks until
    // every sender is gone.
    log.remove_session(&tx);
    assert_eq!(log.session_count(), 0);
    drop(tx);
    let received: Vec<u64> = rx.iter().map(|(seq, _)| seq).collect();
    assert_eq!(received, seqs);
}

#[test]
fn applied_seq_state_persists_and_advances_monotonically() {
    let dir = tempfile::tempdir().unwrap();
    {
        let state = AppliedSeqState::load(dir.path());
        assert_eq!(state.applied(), 0);
        state.advance(5, false).unwrap();
        assert_eq!(state.applied(), 5, "force on first advance below threshold");
        state.advance(7, false).unwrap();
        assert_eq!(state.applied(), 5, "below persist-every threshold");
        state.advance(9, true).unwrap();
        assert_eq!(state.applied(), 9);
    }
    let reloaded = AppliedSeqState::load(dir.path());
    assert_eq!(reloaded.applied(), 9);
}

#[test]
fn rep_log_resumes_after_persisted_seq() {
    let dir = tempfile::tempdir().unwrap();
    {
        let log = RepLog::new(64, u64::MAX, dir.path()).with_persist_every(1);
        log.publish(tail_event("t"));
        log.publish(tail_event("t"));
        assert_eq!(log.persisted_seq(), 2);
        log.flush_seq();
    }
    let reopened = RepLog::new(64, u64::MAX, dir.path()).with_persist_every(u64::MAX);
    assert_eq!(reopened.current_seq(), 2, "seq resumes after restart");
    assert_eq!(reopened.persisted_seq(), 2);
    assert_eq!(reopened.publish(tail_event("t")), 3);
}
/// P0: a primary that restarts on a stale seq file (the file lags by up
/// to `persist_every` events) stamps fresh events with seqs a standby
/// already applied. A reconnecting standby's watermark must re-stamp the
/// ring above itself, or every resume filter silently drops the new
/// events and the pair diverges without any error.
#[test]
fn standby_watermark_re_stamps_a_lagging_ring() {
    let temp = tempfile::tempdir().unwrap();
    let log = RepLog::new(64, u64::MAX, temp.path());
    // Simulate a crash + restart on a stale file: the file says 0 (only
    // persisted every 256), a standby out there applied seq 3.
    let reused = log.publish(tail_event("t")); // stamped with a reused seq
    assert_eq!(reused, 1, "control: seq resumed from the stale file");
    log.note_standby_applied(3);
    let history = log.events_after(3).unwrap();
    assert_eq!(
        history.len(),
        1,
        "the causally-new event must survive the resume filter"
    );
    assert_eq!(history[0].0, 4, "re-stamped above the standby watermark");
    assert_eq!(log.current_seq(), 4);
    // Subsequent publishes continue above the watermark.
    assert_eq!(log.publish(tail_event("t")), 5);
    // A normal resume (watermark at or below current) re-stamps nothing.
    log.note_standby_applied(2);
    assert_eq!(log.current_seq(), 5);
}

#[test]
fn ring_stats_tracks_bytes_and_capacity_eviction() {
    let dir = tempfile::TempDir::new().unwrap();
    let log = RepLog::new(16, u64::MAX, dir.path()).with_persist_every(u64::MAX);

    let big = ReplicationEvent::TailAppend {
        db: "main".into(),
        table: "t".into(),
        partition: "2026".into(),
        memtable_id: Uuid::new_v4(),
        first_lsn: 1,
        last_lsn: 2,
        frames: vec![0u8; 4096],
    };
    log.publish(big);
    let (len, bytes) = log.ring_stats();
    assert_eq!(len, 1);
    assert!(bytes >= 4096, "ring bytes must cover the frame payload");

    for _ in 0..64 {
        log.publish(tail_event("t"));
    }
    let (len, _) = log.ring_stats();
    assert_eq!(len, 16, "ring must stay entry-bounded at capacity");
}

#[test]
fn ring_respects_byte_budget_in_addition_to_entry_cap() {
    let dir = tempfile::TempDir::new().unwrap();
    // 1.5 MiB budget; each event carries a ~600 KiB frame payload.
    let budget = 1536 * 1024;
    let log = RepLog::new(64, budget, dir.path()).with_persist_every(u64::MAX);

    let frame_event = |n: usize| ReplicationEvent::TailAppend {
        db: "main".into(),
        table: "t".into(),
        partition: "2026".into(),
        memtable_id: Uuid::new_v4(),
        first_lsn: 1,
        last_lsn: 2,
        frames: vec![0u8; n],
    };
    for _ in 0..8 {
        log.publish(frame_event(600 * 1024));
    }
    let (len, bytes) = log.ring_stats();
    assert!(
        bytes <= budget,
        "ring bytes {bytes} must stay under {budget}"
    );
    assert!(
        len < 8,
        "oldest events must be evicted past the byte budget"
    );
}

#[test]
fn ring_keeps_the_newest_event_even_when_it_alone_exceeds_the_budget() {
    let dir = tempfile::TempDir::new().unwrap();
    let log = RepLog::new(64, 1024 * 1024, dir.path()).with_persist_every(u64::MAX);

    let frame_event = |n: usize| ReplicationEvent::TailAppend {
        db: "main".into(),
        table: "t".into(),
        partition: "2026".into(),
        memtable_id: Uuid::new_v4(),
        first_lsn: 1,
        last_lsn: 2,
        frames: vec![0u8; n],
    };
    log.publish(frame_event(2 * 1024 * 1024));
    let (len, bytes) = log.ring_stats();
    assert_eq!(len, 1);
    assert!(
        bytes > 1024 * 1024,
        "oversized newest event must be retained"
    );

    // The next publish evicts the oversized predecessor, never the newest.
    log.publish(frame_event(64));
    let (len, _) = log.ring_stats();
    assert_eq!(len, 1);
}
