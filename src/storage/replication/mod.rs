//! Primary-standby replication: physical operation stream (R1).
//!
//! The primary's single tail-commit thread is the only globally serialized
//! mutation point, so replication events flow through the same commit
//! channel: every event is seq-stamped when the commit thread dequeues it,
//! which makes `rep_seq` order identical to the primary's durable-mutation
//! order. The standby replays events in seq order and converges to the same
//! physical state.
//!
//! R1 event set:
//! - [`ReplicationEvent::TailAppend`]: durable-tail V3 frames (all INSERT
//!   deltas and committed transaction deltas).
//! - [`ReplicationEvent::SegmentUpdate`]: a segment file's published state
//!   grew (`Prefix`, after a flush landed row groups in the active segment)
//!   or is final (`Final`, after a seal). Segment bytes are fetched by the
//!   standby on demand (`REPL_SEGMENT_FETCH`), so the in-memory event ring
//!   holds metadata only.
//!
//! R1 scope cuts (documented in docs/operations/replication.md):
//! memtable DELETE/UPDATE sidecars ship as [`ReplicationEvent::TailMutation`]
//! (segment-level tombstones as `DeleteFooterRewrite`, replacements as
//! `SegmentsReplaced`); TTL and DDL events land in R2; a primary with
//! `replication.enabled` rejects bulk/RELAXED direct writes (the only WAL
//! bypass) and a standby engine is read-only.

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use uuid::Uuid;

use crate::common::error::{Result, TaibaiError};

pub mod standby;

/// R1 event kinds. Binary layout is self-describing (tag + length-prefixed
/// fields) so newer primaries cannot silently desync older standbys: an
/// unknown tag fails the decode loudly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicationEvent {
    /// Durable-tail frames appended on the primary (already V3-encoded and
    /// LSN-stamped). The standby appends the same bytes to the same
    /// memtable's tail file.
    TailAppend {
        db: String,
        table: String,
        partition: String,
        memtable_id: Uuid,
        first_lsn: u64,
        last_lsn: u64,
        /// Concatenated V3 frames, exactly as written to the primary's file.
        frames: Vec<u8>,
    },
    /// A segment's readable content advanced. `Prefix(data_end)` after a
    /// flush landed row groups in the active segment; `Final` after a seal.
    /// The `memtable_ids` are the tails the segment now supersedes; the
    /// standby clears them once the bytes are in place.
    SegmentUpdate {
        db: String,
        table: String,
        segment: String,
        kind: SegmentUpdateKind,
        memtable_ids: Vec<Uuid>,
    },
    /// A memtable-level mutation shipped as the EXACT sidecar bytes the
    /// primary wrote (`.del` positions or `.upd` update set). The standby's
    /// tail file holds the identical frame history, so replaying the same
    /// sidecar generation applies the mutation to the same rows. Without
    /// this event a standby kept serving deleted rows from its tail until
    /// the next flush superseded it.
    TailMutation {
        db: String,
        table: String,
        memtable_id: Uuid,
        /// `false` = delete positions, `true` = update set.
        is_update: bool,
        /// The primary's sidecar GENERATION SEQUENCE: the standby names its
        /// copy identically, so a replayed event (crash before the seq
        /// watermark persists) replaces the file instead of duplicating the
        /// mutation — delete positions are not idempotent on their own.
        sequence: u64,
        /// The primary's encoded sidecar payload, verbatim.
        payload: Vec<u8>,
    },
    /// DELETE tombstones materialized into a segment footer. The bitmap is
    /// the segment's ABSOLUTE delete state (not a delta), so standby apply
    /// is idempotent.
    DeleteFooterRewrite {
        db: String,
        table: String,
        segment: String,
        bitmap: Vec<u8>,
        deleted_count: u64,
    },
    /// Compaction / UPDATE segment replacement: `new_segments` replace
    /// `retired`. Standby fetches each new segment, writes it, removes the
    /// retired files.
    SegmentsReplaced {
        db: String,
        table: String,
        new_segments: Vec<String>,
        retired: Vec<String>,
    },
    /// Whole segment files removed (TTL partition purge, DROP cleanup).
    SegmentsRemoved {
        db: String,
        table: String,
        segments: Vec<String>,
    },
    /// Full catalog snapshot for one database (DDL replication). Standby
    /// replaces its `<data>/meta/catalogs/<db>.json` and reloads.
    CatalogSnapshot { db: String, json: Vec<u8> },
    /// Full auth store snapshot (user/role/GRANT statements).
    AuthSnapshot { json: Vec<u8> },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentUpdateKind {
    Prefix,
    Final,
}

impl SegmentUpdateKind {
    fn tag(self) -> u8 {
        match self {
            Self::Prefix => 0,
            Self::Final => 1,
        }
    }

    fn from_tag(tag: u8) -> Result<Self> {
        match tag {
            0 => Ok(Self::Prefix),
            1 => Ok(Self::Final),
            other => Err(TaibaiError::Storage(format!(
                "unknown replication segment-update kind tag {other}"
            ))),
        }
    }
}

const TAG_TAIL_APPEND: u16 = 1;
const TAG_SEGMENT_UPDATE: u16 = 2;
const TAG_DELETE_FOOTER_REWRITE: u16 = 3;
const TAG_SEGMENTS_REPLACED: u16 = 4;
const TAG_SEGMENTS_REMOVED: u16 = 5;
const TAG_CATALOG_SNAPSHOT: u16 = 6;
const TAG_AUTH_SNAPSHOT: u16 = 7;
const TAG_TAIL_MUTATION: u16 = 8;

impl ReplicationEvent {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(128);
        match self {
            Self::TailAppend {
                db,
                table,
                partition,
                memtable_id,
                first_lsn,
                last_lsn,
                frames,
            } => {
                buf.extend_from_slice(&TAG_TAIL_APPEND.to_le_bytes());
                put_str(&mut buf, db);
                put_str(&mut buf, table);
                put_str(&mut buf, partition);
                buf.extend_from_slice(memtable_id.as_bytes());
                buf.extend_from_slice(&first_lsn.to_le_bytes());
                buf.extend_from_slice(&last_lsn.to_le_bytes());
                buf.extend_from_slice(&(frames.len() as u32).to_le_bytes());
                buf.extend_from_slice(frames);
            }
            Self::TailMutation {
                db,
                table,
                memtable_id,
                is_update,
                sequence,
                payload,
            } => {
                buf.extend_from_slice(&TAG_TAIL_MUTATION.to_le_bytes());
                put_str(&mut buf, db);
                put_str(&mut buf, table);
                buf.extend_from_slice(memtable_id.as_bytes());
                buf.push(u8::from(*is_update));
                buf.extend_from_slice(&sequence.to_le_bytes());
                buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
                buf.extend_from_slice(payload);
            }
            Self::SegmentUpdate {
                db,
                table,
                segment,
                kind,
                memtable_ids,
            } => {
                buf.extend_from_slice(&TAG_SEGMENT_UPDATE.to_le_bytes());
                put_str(&mut buf, db);
                put_str(&mut buf, table);
                put_str(&mut buf, segment);
                buf.push(kind.tag());
                buf.extend_from_slice(&(memtable_ids.len() as u32).to_le_bytes());
                for id in memtable_ids {
                    buf.extend_from_slice(id.as_bytes());
                }
            }
            Self::DeleteFooterRewrite {
                db,
                table,
                segment,
                bitmap,
                deleted_count,
            } => {
                buf.extend_from_slice(&TAG_DELETE_FOOTER_REWRITE.to_le_bytes());
                put_str(&mut buf, db);
                put_str(&mut buf, table);
                put_str(&mut buf, segment);
                buf.extend_from_slice(deleted_count.to_le_bytes().as_slice());
                buf.extend_from_slice(&(bitmap.len() as u32).to_le_bytes());
                buf.extend_from_slice(bitmap);
            }
            Self::SegmentsReplaced {
                db,
                table,
                new_segments,
                retired,
            } => {
                buf.extend_from_slice(&TAG_SEGMENTS_REPLACED.to_le_bytes());
                put_str(&mut buf, db);
                put_str(&mut buf, table);
                buf.extend_from_slice(&(new_segments.len() as u32).to_le_bytes());
                for name in new_segments {
                    put_str(&mut buf, name);
                }
                buf.extend_from_slice(&(retired.len() as u32).to_le_bytes());
                for name in retired {
                    put_str(&mut buf, name);
                }
            }
            Self::SegmentsRemoved {
                db,
                table,
                segments,
            } => {
                buf.extend_from_slice(&TAG_SEGMENTS_REMOVED.to_le_bytes());
                put_str(&mut buf, db);
                put_str(&mut buf, table);
                buf.extend_from_slice(&(segments.len() as u32).to_le_bytes());
                for name in segments {
                    put_str(&mut buf, name);
                }
            }
            Self::CatalogSnapshot { db, json } => {
                buf.extend_from_slice(&TAG_CATALOG_SNAPSHOT.to_le_bytes());
                put_str(&mut buf, db);
                buf.extend_from_slice(&(json.len() as u32).to_le_bytes());
                buf.extend_from_slice(json);
            }
            Self::AuthSnapshot { json } => {
                buf.extend_from_slice(&TAG_AUTH_SNAPSHOT.to_le_bytes());
                buf.extend_from_slice(&(json.len() as u32).to_le_bytes());
                buf.extend_from_slice(json);
            }
        }
        buf
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut decoder = Decoder { bytes, cursor: 0 };
        match decoder.read_u16()? {
            TAG_TAIL_APPEND => {
                let db = decoder.read_str("db")?;
                let table = decoder.read_str("table")?;
                let partition = decoder.read_str("partition")?;
                let memtable_id =
                    Uuid::from_slice(decoder.take(16, "memtable id")?).map_err(|error| {
                        TaibaiError::Storage(format!("invalid memtable id: {error}"))
                    })?;
                let first_lsn = decoder.read_u64()?;
                let last_lsn = decoder.read_u64()?;
                let frames_len = decoder.read_u32()? as usize;
                let frames = decoder.take(frames_len, "frames")?.to_vec();
                Ok(Self::TailAppend {
                    db,
                    table,
                    partition,
                    memtable_id,
                    first_lsn,
                    last_lsn,
                    frames,
                })
            }
            TAG_SEGMENT_UPDATE => {
                let db = decoder.read_str("db")?;
                let table = decoder.read_str("table")?;
                let segment = decoder.read_str("segment")?;
                let kind = SegmentUpdateKind::from_tag(decoder.take(1, "kind")?[0])?;
                let count = decoder.read_u32()? as usize;
                let mut memtable_ids = Vec::with_capacity(count.min(4096));
                for _ in 0..count {
                    memtable_ids.push(Uuid::from_slice(decoder.take(16, "memtable id")?).map_err(
                        |error| TaibaiError::Storage(format!("invalid memtable id: {error}")),
                    )?);
                }
                Ok(Self::SegmentUpdate {
                    db,
                    table,
                    segment,
                    kind,
                    memtable_ids,
                })
            }
            TAG_TAIL_MUTATION => {
                let db = decoder.read_str("db")?;
                let table = decoder.read_str("table")?;
                let memtable_id =
                    Uuid::from_slice(decoder.take(16, "memtable id")?).map_err(|error| {
                        TaibaiError::Storage(format!("invalid memtable id: {error}"))
                    })?;
                let is_update = decoder.take(1, "is_update")?[0] != 0;
                let sequence = decoder.read_u64()?;
                let payload_len = decoder.read_u32()? as usize;
                let payload = decoder.take(payload_len, "tail mutation payload")?.to_vec();
                Ok(Self::TailMutation {
                    db,
                    table,
                    memtable_id,
                    is_update,
                    sequence,
                    payload,
                })
            }
            TAG_DELETE_FOOTER_REWRITE => {
                let db = decoder.read_str("db")?;
                let table = decoder.read_str("table")?;
                let segment = decoder.read_str("segment")?;
                let deleted_count = decoder.read_u64()?;
                let bitmap_len = decoder.read_u32()? as usize;
                let bitmap = decoder.take(bitmap_len, "bitmap")?.to_vec();
                Ok(Self::DeleteFooterRewrite {
                    db,
                    table,
                    segment,
                    bitmap,
                    deleted_count,
                })
            }
            TAG_SEGMENTS_REPLACED => {
                let db = decoder.read_str("db")?;
                let table = decoder.read_str("table")?;
                let new_segments = decode_str_vec(&mut decoder)?;
                let retired = decode_str_vec(&mut decoder)?;
                Ok(Self::SegmentsReplaced {
                    db,
                    table,
                    new_segments,
                    retired,
                })
            }
            TAG_SEGMENTS_REMOVED => {
                let db = decoder.read_str("db")?;
                let table = decoder.read_str("table")?;
                let segments = decode_str_vec(&mut decoder)?;
                Ok(Self::SegmentsRemoved {
                    db,
                    table,
                    segments,
                })
            }
            TAG_CATALOG_SNAPSHOT => {
                let db = decoder.read_str("db")?;
                let json_len = decoder.read_u32()? as usize;
                let json = decoder.take(json_len, "catalog json")?.to_vec();
                Ok(Self::CatalogSnapshot { db, json })
            }
            TAG_AUTH_SNAPSHOT => {
                let json_len = decoder.read_u32()? as usize;
                let json = decoder.take(json_len, "auth json")?.to_vec();
                Ok(Self::AuthSnapshot { json })
            }
            tag => Err(TaibaiError::Storage(format!(
                "unknown replication event tag {tag}"
            ))),
        }
    }

    /// `(db, table)` the event touches; used for standby-side routing.
    pub fn topic(&self) -> Option<(&str, &str)> {
        match self {
            Self::TailAppend { db, table, .. }
            | Self::TailMutation { db, table, .. }
            | Self::SegmentUpdate { db, table, .. }
            | Self::DeleteFooterRewrite { db, table, .. }
            | Self::SegmentsReplaced { db, table, .. }
            | Self::SegmentsRemoved { db, table, .. } => Some((db, table)),
            Self::CatalogSnapshot { .. } | Self::AuthSnapshot { .. } => None,
        }
    }
}

fn put_str(buf: &mut Vec<u8>, value: &str) {
    buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
    buf.extend_from_slice(value.as_bytes());
}
/// Cursor over an encoded event payload.
struct Decoder<'a> {
    bytes: &'a [u8],
    cursor: usize,
}

impl<'a> Decoder<'a> {
    fn take(&mut self, n: usize, what: &str) -> Result<&'a [u8]> {
        if self.cursor + n > self.bytes.len() {
            return Err(TaibaiError::Storage(format!(
                "replication event truncated while reading {what}"
            )));
        }
        let slice = &self.bytes[self.cursor..self.cursor + n];
        self.cursor += n;
        Ok(slice)
    }

    fn read_u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.take(2, "tag")?.try_into().unwrap()))
    }

    fn read_u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4, "u32")?.try_into().unwrap()))
    }

    fn read_u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8, "u64")?.try_into().unwrap()))
    }

    fn read_str(&mut self, what: &str) -> Result<String> {
        let len = self.read_u32()? as usize;
        let raw = self.take(len, what)?;
        String::from_utf8(raw.to_vec())
            .map_err(|_| TaibaiError::Storage(format!("invalid UTF-8 in {what}")))
    }

    fn read_str_vec(&mut self, what: &str) -> Result<Vec<String>> {
        let count = self.read_u32()? as usize;
        let mut out = Vec::with_capacity(count.min(4096));
        for _ in 0..count {
            out.push(self.read_str(what)?);
        }
        Ok(out)
    }
}

fn decode_str_vec(decoder: &mut Decoder<'_>) -> Result<Vec<String>> {
    decoder.read_str_vec("names")
}

// ---------------------------------------------------------------------------
// RepLog: sequence assignment, ring buffer, session fan-out
// ---------------------------------------------------------------------------

/// One connected standby's event pipe. Bounded: a stuck consumer is dropped
/// after the send timeout rather than stalling the primary's commit thread.
pub type SessionSink = crossbeam_channel::Sender<(u64, Arc<ReplicationEvent>)>;

struct RepLogState {
    next_seq: u64,
    ring: VecDeque<(u64, Arc<ReplicationEvent>)>,
    /// Approximate heap bytes held by `ring`. The ring is bounded by entry
    /// count, not bytes, so this is the only visibility into how much memory
    /// the retained events actually pin (exposed via `ring_stats`).
    ring_bytes: u64,
    sessions: Vec<SessionSink>,
    unpersisted: u64,
}

/// Rough heap footprint of a buffered event — the payload Vec dominates for
/// the frame-carrying variants; the rest is fixed fields plus strings. Used
/// only for accounting/reporting, never for correctness.
fn estimated_event_bytes(event: &ReplicationEvent) -> u64 {
    const WORDS: u64 = 192;
    match event {
        ReplicationEvent::TailAppend {
            db,
            table,
            partition,
            frames,
            ..
        } => WORDS + (db.len() + table.len() + partition.len() + frames.len()) as u64,
        ReplicationEvent::TailMutation {
            db, table, payload, ..
        } => WORDS + (db.len() + table.len() + payload.len()) as u64,
        ReplicationEvent::DeleteFooterRewrite {
            db,
            table,
            segment,
            bitmap,
            ..
        } => WORDS + (db.len() + table.len() + segment.len() + bitmap.len()) as u64,
        ReplicationEvent::SegmentUpdate {
            db,
            table,
            segment,
            memtable_ids,
            ..
        } => {
            WORDS
                + (db.len() + table.len() + segment.len()) as u64
                + (memtable_ids.len() as u64) * 16
        }
        ReplicationEvent::SegmentsReplaced {
            db,
            table,
            new_segments,
            retired,
        } => {
            let names: usize = new_segments.iter().map(String::len).sum::<usize>()
                + retired.iter().map(String::len).sum::<usize>();
            WORDS + (db.len() + table.len()) as u64 + names as u64
        }
        ReplicationEvent::SegmentsRemoved {
            db,
            table,
            segments,
        } => {
            let names: usize = segments.iter().map(String::len).sum();
            WORDS + (db.len() + table.len()) as u64 + names as u64
        }
        ReplicationEvent::CatalogSnapshot { db, json } => WORDS + (db.len() + json.len()) as u64,
        ReplicationEvent::AuthSnapshot { json } => WORDS + json.len() as u64,
    }
}

/// Sequence source + ring + fan-out. All mutation happens on the tail-commit
/// thread (see `CommitCommand::Replicate`), so `next_seq` needs no lock of
/// its own; the Mutex guards the ring against the session-registration path
/// running on connection threads.
pub struct RepLog {
    state: Mutex<RepLogState>,
    capacity: usize,
    /// Byte ceiling for the ring; the entry cap alone lets write-heavy
    /// primaries retain hundreds of GB (each event carries a whole
    /// compressed write group, up to 16 MiB). At least the newest event is
    /// always kept, even when it alone exceeds the ceiling.
    ring_max_bytes: u64,
    /// `<data>/wal/replication_seq` — durable high-water for bootstrap.
    seq_file: Mutex<Option<PathBuf>>,
    persisted_seq: AtomicU64,
    /// Events persisted per flush of the seq file.
    persist_every: u64,
    /// Commit-thread handle for structural events (`SegmentUpdate`, and
    /// R2's DML/DDL events): they queue through the commit channel and are
    /// seq-stamped at dequeue, after the tail appends they causally follow.
    tail_committer: OnceLock<Arc<crate::storage::core::durable_tail::TailCommitter>>,
}

const DEFAULT_PERSIST_EVERY: u64 = 256;

impl RepLog {
    pub fn new(capacity: usize, ring_max_bytes: u64, data_dir: &Path) -> Self {
        let seq_file = data_dir.join("wal").join("replication_seq");
        let persisted = read_seq_file(&seq_file).unwrap_or(0);
        Self {
            state: Mutex::new(RepLogState {
                // Resume after the last durable seq so a primary restart never
                // re-stamps events a standby may have already applied.
                next_seq: persisted + 1,
                ring: VecDeque::new(),
                ring_bytes: 0,
                sessions: Vec::new(),
                unpersisted: 0,
            }),
            capacity: capacity.max(16),
            ring_max_bytes: ring_max_bytes.max(1024 * 1024),
            seq_file: Mutex::new(Some(seq_file)),
            persisted_seq: AtomicU64::new(persisted),
            persist_every: DEFAULT_PERSIST_EVERY,
            tail_committer: OnceLock::new(),
        }
    }

    /// Wire the commit-thread handle (engine startup, after the batch writer
    /// exists).
    pub(crate) fn set_tail_committer(
        &self,
        committer: Arc<crate::storage::core::durable_tail::TailCommitter>,
    ) {
        let _ = self.tail_committer.set(committer);
    }

    /// Enqueue a structural event through the commit channel.
    pub fn enqueue(&self, event: ReplicationEvent) {
        match self.tail_committer.get() {
            Some(committer) => committer.submit_event(event),
            None => tracing::warn!("replication event dropped: no commit thread wired"),
        }
    }

    /// Whether the ring records a compaction/TTL retirement of `segment`.
    /// The fetch path consults this to answer a standby racing a compaction:
    /// the segment's rows live on in the replacement event later in the same
    /// stream, so the stale `SegmentUpdate` can be skipped. A file missing
    /// without a provable retirement still fails loudly (bootstrap drift).
    pub fn segment_retired(&self, db: &str, table: &str, segment: &str) -> bool {
        let state = self.state.lock().unwrap();
        state
            .ring
            .iter()
            .rev()
            .any(|(_, event)| match event.as_ref() {
                ReplicationEvent::SegmentsReplaced {
                    db: d,
                    table: t,
                    retired,
                    ..
                } => d == db && t == table && retired.iter().any(|name| name == segment),
                ReplicationEvent::SegmentsRemoved {
                    db: d,
                    table: t,
                    segments,
                } => d == db && t == table && segments.iter().any(|name| name == segment),
                _ => false,
            })
    }

    /// Test-only injection of the seq file location.
    #[cfg(test)]
    fn with_persist_every(mut self, every: u64) -> Self {
        self.persist_every = every;
        self
    }

    /// Called only from the tail-commit thread while processing
    /// `CommitCommand::Replicate`: stamp the next seq, buffer, fan out.
    pub fn publish(&self, event: ReplicationEvent) -> u64 {
        let mut state = self.state.lock().unwrap();
        let seq = state.next_seq;
        state.next_seq += 1;
        let event = Arc::new(event);
        state.ring.push_back((seq, Arc::clone(&event)));
        state.ring_bytes += estimated_event_bytes(&event);
        while state.ring.len() > self.capacity
            || (state.ring.len() > 1 && state.ring_bytes > self.ring_max_bytes)
        {
            match state.ring.pop_front() {
                Some((_, evicted)) => {
                    state.ring_bytes = state
                        .ring_bytes
                        .saturating_sub(estimated_event_bytes(&evicted));
                }
                None => break,
            }
        }
        // Fan out. A session that cannot keep up is dropped: it will
        // reconnect, resume from its persisted applied seq, and re-pull the
        // ring history. Losing the session is far better than stalling the
        // commit thread — this runs on the only group-commit/fsync thread,
        // so a slow standby used to freeze every table's writes for up to
        // the full timeout per event. The bounded per-session queue absorbs
        // normal jitter; only a genuinely wedged consumer trips the drop.
        state.sessions.retain(|sink| {
            match sink.send_timeout(
                (seq, Arc::clone(&event)),
                std::time::Duration::from_millis(500),
            ) {
                Ok(()) => true,
                Err(crossbeam_channel::SendTimeoutError::Timeout(_)) => {
                    tracing::warn!("replication session fell behind and was dropped");
                    false
                }
                Err(crossbeam_channel::SendTimeoutError::Disconnected(_)) => false,
            }
        });
        state.unpersisted += 1;
        if state.unpersisted >= self.persist_every {
            let target = seq;
            drop(state);
            self.persist_seq(target);
        }
        seq
    }

    /// Durable seq high-water (tmp + rename). Called from the commit thread.
    fn persist_seq(&self, seq: u64) {
        // A stale caller (a late ack for a seq another session already drove
        // past) must not downgrade the durable high-water: a primary restart
        // resumes from this file, and resuming below a seq a standby already
        // applied re-stamps consumed seqs — the standby then skips the
        // re-delivered events as stale and silently loses them.
        if seq <= self.persisted_seq.load(Ordering::Acquire) {
            return;
        }
        let guard = self.seq_file.lock().unwrap();
        let Some(path) = guard.as_ref() else {
            return;
        };
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let tmp = path.with_extension("seq.tmp");
        let write = std::fs::File::create(&tmp).and_then(|mut file| {
            use std::io::Write;
            file.write_all(seq.to_string().as_bytes())?;
            file.sync_all()
        });
        match write {
            Ok(()) => {
                if let Err(error) = std::fs::rename(&tmp, path) {
                    tracing::warn!(error = %error, "replication seq rename failed");
                } else {
                    self.persisted_seq.store(seq, Ordering::Release);
                }
            }
            Err(error) => {
                tracing::warn!(error = %error, "replication seq persist failed");
            }
        }
        if let Ok(mut guard) = self.state.lock() {
            guard.unpersisted = 0;
        }
    }

    /// Flush the seq file on shutdown.
    pub fn flush_seq(&self) {
        self.persist_seq(self.current_seq());
    }

    /// A standby reported `seq` durably applied (REPL_ACK, or its last_seq at
    /// REPL_HELLO). Advance the persisted high-water to it: the periodic
    /// `persist_every` cadence alone leaves the seq file up to 255 events
    /// behind, and a primary crash then reuses seqs the standby already
    /// consumed — the standby skips the re-delivered events as stale and
    /// silently loses them. Monotonic; acks for already-persisted seqs are
    /// no-ops, so the steady state adds no writes.
    pub fn note_standby_applied(&self, seq: u64) {
        if seq > self.persisted_seq.load(Ordering::Acquire) {
            self.persist_seq(seq);
        }
        // A restart resumes next_seq from the durable file, which can lag a
        // standby's applied watermark by up to `persist_every` events. Any
        // events published in that gap carry seqs the standby already
        // consumed, and every resume filter (events_after, the bridge, the
        // standby's own dedup) silently drops them — the new data they carry
        // is lost with no error. Re-stamp the surviving ring entries above
        // the watermark: they were published after the restart, so they are
        // causally new even though numerically stale. A standby that was
        // behind the watermark now fails events_after explicitly and
        // re-bootstraps instead of silently diverging.
        let mut state = self.state.lock().unwrap();
        if seq + 1 > state.next_seq {
            let mut next = seq + 1;
            for slot in state.ring.iter_mut() {
                slot.0 = next;
                next += 1;
            }
            tracing::warn!(
                standby_applied = seq,
                renumbered = state.ring.len(),
                new_next_seq = next,
                "replication seq fell behind a standby watermark (primary restarted on a stale seq file); ring re-stamped"
            );
            state.next_seq = next;
        }
    }

    pub fn current_seq(&self) -> u64 {
        self.state.lock().unwrap().next_seq - 1
    }

    pub fn persisted_seq(&self) -> u64 {
        self.persisted_seq.load(Ordering::Acquire)
    }

    /// Snapshot for a (re)connecting standby: events strictly after
    /// `after_seq`. `Err(())` when the gap is older than the ring
    /// (re-bootstrap required).
    pub fn events_after(&self, after_seq: u64) -> Result<Vec<(u64, Arc<ReplicationEvent>)>> {
        let state = self.state.lock().unwrap();
        let oldest = state
            .ring
            .front()
            .map(|(seq, _)| *seq)
            .unwrap_or(state.next_seq);
        if after_seq + 1 < oldest {
            return Err(TaibaiError::Storage(format!(
                "standby at seq {after_seq} is behind the replication ring (oldest {}); \
                 re-bootstrap from a backup is required",
                oldest.saturating_sub(1)
            )));
        }
        Ok(state
            .ring
            .iter()
            .filter(|(seq, _)| *seq > after_seq)
            .map(|(seq, event)| (*seq, Arc::clone(event)))
            .collect())
    }

    /// Register a live standby session; returns `(oldest_available,
    /// next_seq)` for the resume decision.
    pub fn register_session(&self, sink: &SessionSink) -> (u64, u64) {
        let mut state = self.state.lock().unwrap();
        let oldest = state
            .ring
            .front()
            .map(|(seq, _)| *seq)
            .unwrap_or(state.next_seq);
        state.sessions.push(sink.clone());
        (oldest.saturating_sub(1), state.next_seq - 1)
    }

    pub fn remove_session(&self, sink: &SessionSink) {
        let mut state = self.state.lock().unwrap();
        state.sessions.retain(|sender| !sender.same_channel(sink));
    }

    pub fn session_count(&self) -> usize {
        self.state.lock().unwrap().sessions.len()
    }

    /// `(retained events, approximate heap bytes)` for the ring. The ring is
    /// entry-bounded only, so bytes are what `SHOW MEMORY` needs to report.
    pub fn ring_stats(&self) -> (usize, u64) {
        let state = self.state.lock().unwrap();
        (state.ring.len(), state.ring_bytes)
    }

    /// Configured byte ceiling for the ring (`SHOW MEMORY`).
    pub fn ring_byte_limit(&self) -> u64 {
        self.ring_max_bytes
    }
}

fn read_seq_file(path: &Path) -> Option<u64> {
    let raw = std::fs::read_to_string(path).ok()?;
    raw.trim().parse::<u64>().ok()
}

/// Read the durable replication seq for a data directory (used by the backup
/// tool to stamp manifests).
pub fn durable_replication_seq(data_dir: &Path) -> u64 {
    read_seq_file(&data_dir.join("wal").join("replication_seq")).unwrap_or(0)
}

/// Standby-side durable applied-seq state (`<data>/wal/replication_applied`).
pub struct AppliedSeqState {
    path: PathBuf,
    pending: Mutex<u64>,
    persisted: AtomicU64,
    every: u64,
}

impl AppliedSeqState {
    pub fn load(data_dir: &Path) -> Self {
        let path = data_dir.join("wal").join("replication_applied");
        let persisted = read_seq_file(&path).unwrap_or(0);
        Self {
            path,
            pending: Mutex::new(persisted),
            persisted: AtomicU64::new(persisted),
            every: DEFAULT_PERSIST_EVERY,
        }
    }

    pub fn applied(&self) -> u64 {
        self.persisted.load(Ordering::Acquire)
    }

    /// Record an applied seq; persists every `every` updates. `force`
    /// flushes immediately (shutdown, before answering a fetch).
    pub fn advance(&self, seq: u64, force: bool) -> Result<()> {
        let mut pending = self.pending.lock().unwrap();
        *pending = (*pending).max(seq);
        let target = *pending;
        let persisted = self.persisted.load(Ordering::Acquire);
        // First advance always persists: a standby that died before its
        // baseline must not replay the whole bootstrap again.
        if !force && persisted > 0 && target.saturating_sub(persisted) < self.every {
            return Ok(());
        }
        drop(pending);
        self.persist(target)
    }

    pub fn persist(&self, seq: u64) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let tmp = self.path.with_extension("tmp");
        let mut file = std::fs::File::create(&tmp)?;
        use std::io::Write;
        file.write_all(seq.to_string().as_bytes())?;
        file.sync_all()?;
        std::fs::rename(&tmp, &self.path)?;
        self.persisted.store(seq, Ordering::Release);
        Ok(())
    }
}

#[cfg(test)]
#[path = "replication_test.rs"]
mod tests;
