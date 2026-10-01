//! Standby replication applier: connects to the primary, replays the
//! physical operation stream, and serves reads from the converged data.
//!
//! Apply model (R1):
//! - `TailAppend` → fsync the V3 frames into the standby's local tail file
//!   and register/refresh a spilled memtable (lazy decode on first read).
//! - `SegmentUpdate` → fetch the segment bytes from the primary (atomic
//!   tmp+rename locally), then retire the memtables the segment supersedes.
//!
//! Durability: every applied seq is persisted (`<data>/wal/replication_applied`,
//! fsync + rename) at `ACK_EVERY` intervals; on restart the standby resumes
//! from that seq. Events behind the primary's ring require re-bootstrap.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio_util::codec::Framed;

use crate::common::error::{Result, TaibaiError};
use crate::net::handler::build_error;
use crate::protocol::codec::MessageCodec;
use crate::protocol::message::{
    Message, MSG_TYPE_AUTH_FAIL, MSG_TYPE_AUTH_OK, MSG_TYPE_ERROR, MSG_TYPE_LOGIN, MSG_TYPE_OK,
    MSG_TYPE_REPL_ACK, MSG_TYPE_REPL_EVENT, MSG_TYPE_REPL_HELLO, MSG_TYPE_REPL_SEGMENT_CHUNK,
    MSG_TYPE_REPL_SEGMENT_FETCH, MSG_TYPE_REPL_SEGMENT_GONE,
};
use crate::storage::core::durable_tail::{append_raw_frames, count_v3_frame_rows};
use crate::storage::replication::{AppliedSeqState, ReplicationEvent};

use super::SegmentUpdateKind;

const MAX_MESSAGE_SIZE: usize = 256 * 1024 * 1024;
const ACK_EVERY: u64 = 256;

/// Byte cap on replication events deferred while a segment fetch is in flight.
const DEFERRED_EVENT_BUDGET_BYTES: u64 = 256 * 1024 * 1024;
const MAX_BACKOFF: Duration = Duration::from_secs(30);

#[derive(Debug, Clone)]
pub struct StandbyConfig {
    pub primary_addr: String,
    pub user: String,
    pub password: String,
    pub database: String,
}

/// Run the applier forever (reconnecting with exponential backoff). Spawned
/// on a dedicated thread by the server startup.
/// Handle for tests and graceful shutdown: `stop` ends the applier thread.
#[derive(Clone)]
pub struct ApplierHandle {
    pub stop: Arc<std::sync::atomic::AtomicBool>,
}

impl ApplierHandle {
    pub fn stop(&self) {
        use std::sync::atomic::Ordering;
        self.stop.store(true, Ordering::Release);
    }
}

pub fn spawn(engine: Arc<crate::sql::execution::Engine>, config: StandbyConfig) -> ApplierHandle {
    spawn_with_stop(
        engine,
        config,
        Arc::new(std::sync::atomic::AtomicBool::new(false)),
    )
}

pub fn spawn_with_stop(
    engine: Arc<crate::sql::execution::Engine>,
    config: StandbyConfig,
    stop: Arc<std::sync::atomic::AtomicBool>,
) -> ApplierHandle {
    let handle = ApplierHandle {
        stop: Arc::clone(&stop),
    };
    std::thread::Builder::new()
        .name("taibai-standby".into())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("standby tokio runtime must build");
            runtime.block_on(run_forever(engine, config, stop));
        })
        .expect("failed to spawn standby replication thread");
    handle
}

async fn run_forever(
    engine: Arc<crate::sql::execution::Engine>,
    config: StandbyConfig,
    stop: Arc<std::sync::atomic::AtomicBool>,
) {
    use std::sync::atomic::Ordering;
    let mut backoff = Duration::from_secs(1);
    loop {
        if stop.load(Ordering::Acquire) {
            return;
        }
        let started = std::time::Instant::now();
        match connect_and_stream(&engine, &config, &stop).await {
            Ok(()) => unreachable!("replication stream only ends on error"),
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    backoff_ms = backoff.as_millis() as u64,
                    "replication stream ended; reconnecting"
                );
            }
        }
        if stop.load(Ordering::Acquire) {
            return;
        }
        // A connection that lasted a while resets the backoff.
        if started.elapsed() > MAX_BACKOFF {
            backoff = Duration::from_secs(1);
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}

async fn connect_and_stream(
    engine: &Arc<crate::sql::execution::Engine>,
    config: &StandbyConfig,
    stop: &Arc<std::sync::atomic::AtomicBool>,
) -> Result<()> {
    let tcp = tokio::net::TcpStream::connect(&config.primary_addr)
        .await
        .map_err(|error| TaibaiError::Storage(format!("connect primary: {error}")))?;
    let mut framed = Framed::new(tcp, MessageCodec::new(MAX_MESSAGE_SIZE));

    // LOGIN (payload mirrors net/client.rs: user, password, database, tz, streaming flag).
    let mut login = Vec::new();
    put_str(&mut login, &config.user);
    put_str(&mut login, &config.password);
    put_str(&mut login, &config.database);
    login.extend_from_slice(&0i32.to_le_bytes());
    login.push(0);
    framed
        .send(Message::new(MSG_TYPE_LOGIN, Bytes::from(login)))
        .await?;
    expect(&mut framed, &[MSG_TYPE_AUTH_OK, MSG_TYPE_AUTH_FAIL]).await?;

    let applied = AppliedSeqState::load(engine.data_dir());
    let mut hello = Vec::new();
    put_str(&mut hello, &config.user);
    put_str(&mut hello, &config.password);
    hello.extend_from_slice(&applied.applied().to_le_bytes());
    framed
        .send(Message::new(MSG_TYPE_REPL_HELLO, Bytes::from(hello)))
        .await?;

    tracing::info!(
        primary = %config.primary_addr,
        from_seq = applied.applied(),
        "replication stream connected"
    );

    // Deferred events buffered while a segment fetch is in flight, bounded
    // by DEFERRED_EVENT_BUDGET_BYTES: a GB-scale fetch under heavy write
    // traffic used to buffer every incoming TailAppend (up to ~16MB of
    // frames each) without limit. Blowing the budget fails the applier on
    // purpose — it reconnects and resumes from the persisted applied seq,
    // losing nothing.
    let mut deferred: VecDeque<(u64, ReplicationEvent, u64)> = VecDeque::new();
    let mut deferred_bytes: u64 = 0;
    // Cumulative live-row counts per tail path. Tails are append-only, so
    // each event adds exactly its own frames' rows; re-reading the whole
    // tail file per event (the old behavior) cost O(N²) bytes of I/O over
    // a replication session. Entries are re-seeded from the file whenever
    // a path is first seen (or after the conservative clear below).
    let mut tail_rows_cache: std::collections::HashMap<String, u64> =
        std::collections::HashMap::new();
    // Delete tombstones for segments the standby has not received yet: the
    // primary can tombstone an ACTIVE segment before any SegmentUpdate
    // shipped it. Queued rewrites are applied when the segment materializes.
    let mut pending_rewrites: std::collections::HashMap<String, Vec<(Vec<u8>, u64)>> =
        std::collections::HashMap::new();
    let mut since_ack = 0u64;
    loop {
        if stop.load(std::sync::atomic::Ordering::Acquire) {
            return Err(TaibaiError::Storage("applier stopped".into()));
        }
        if let Some((seq, event, event_bytes)) = deferred.pop_front() {
            deferred_bytes -= event_bytes;
            apply(
                engine,
                &applied,
                seq,
                event,
                &mut framed,
                &mut deferred,
                &mut deferred_bytes,
                &mut tail_rows_cache,
                &mut pending_rewrites,
            )
            .await?;
            since_ack += 1;
            if since_ack >= ACK_EVERY {
                send_ack(&mut framed, applied.applied()).await?;
                since_ack = 0;
            }
            continue;
        }
        let message = framed
            .next()
            .await
            .ok_or_else(|| TaibaiError::Storage("primary closed the stream".into()))??;
        match message.msg_type {
            MSG_TYPE_OK => {}
            MSG_TYPE_ERROR => {
                return Err(TaibaiError::Storage(format!(
                    "primary rejected the replication handshake: {}",
                    String::from_utf8_lossy(&message.payload)
                )));
            }
            MSG_TYPE_REPL_EVENT => {
                if message.payload.len() < 12 {
                    return Err(TaibaiError::Storage("REPL_EVENT truncated".into()));
                }
                let seq = u64::from_le_bytes(message.payload[..8].try_into().unwrap());
                let len = u32::from_le_bytes(message.payload[8..12].try_into().unwrap()) as usize;
                if message.payload.len() < 12 + len {
                    return Err(TaibaiError::Storage("REPL_EVENT payload truncated".into()));
                }
                let event = ReplicationEvent::decode(&message.payload[12..12 + len])?;
                if seq <= applied.applied() {
                    continue;
                }
                // Stop check AFTER the frame arrives but BEFORE applying: a
                // dropped event is replayed by the next applier from the
                // persisted seq (exactly-once via the seq watermark).
                if stop.load(std::sync::atomic::Ordering::Acquire) {
                    return Err(TaibaiError::Storage("applier stopped".into()));
                }
                apply(
                    engine,
                    &applied,
                    seq,
                    event,
                    &mut framed,
                    &mut deferred,
                    &mut deferred_bytes,
                    &mut tail_rows_cache,
                    &mut pending_rewrites,
                )
                .await?;
                since_ack += 1;
                if since_ack >= ACK_EVERY {
                    send_ack(&mut framed, applied.applied()).await?;
                    since_ack = 0;
                }
            }
            other => {
                tracing::debug!(msg_type = other, "ignoring frame on replication stream");
            }
        }
    }
}

async fn apply(
    engine: &Arc<crate::sql::execution::Engine>,
    applied: &AppliedSeqState,
    seq: u64,
    event: ReplicationEvent,
    framed: &mut Framed<tokio::net::TcpStream, MessageCodec>,
    deferred: &mut VecDeque<(u64, ReplicationEvent, u64)>,
    deferred_bytes: &mut u64,
    tail_rows_cache: &mut std::collections::HashMap<String, u64>,
    pending_rewrites: &mut std::collections::HashMap<String, Vec<(Vec<u8>, u64)>>,
) -> Result<()> {
    match event {
        ReplicationEvent::TailAppend {
            db,
            table,
            partition,
            memtable_id,
            first_lsn,
            last_lsn,
            frames,
        } => {
            let table_dir = engine.data_dir().join(&db).join(&table);
            // Replay dedup: the applied watermark persists only AFTER the
            // tail append is fsynced, so a standby crash in that window
            // replays this event from the persisted (stale) seq. Re-appending
            // blindly would double the tail's rows. Skip when the tail
            // already covers the event's LSN range; fail loudly on a partial
            // overlap (cannot be patched safely).
            let replayed = match crate::storage::core::durable_tail::live_tail_path(
                &table_dir,
                memtable_id,
            )? {
                Some(path) => {
                    match crate::storage::core::durable_tail::tail_last_lsn(&path)? {
                        Some(tail_lsn) if tail_lsn >= last_lsn => true,
                        Some(tail_lsn) if tail_lsn >= first_lsn => {
                            return Err(TaibaiError::Storage(format!(
                                "replication replay for memtable {memtable_id} partially applied \
                                 (tail ends at lsn {tail_lsn}, event spans {first_lsn}..{last_lsn}); \
                                 the tail needs manual inspection"
                            )));
                        }
                        _ => false,
                    }
                }
                None => false,
            };
            if !replayed {
                append_raw_frames(&table_dir, memtable_id, &partition, &frames)?;
            }
            // The spilled row count must reflect the WHOLE tail file (delta
            // frames from several events accumulate in one file).
            let live_path =
                crate::storage::core::durable_tail::live_tail_path(&table_dir, memtable_id)?;
            let rows = match &live_path {
                Some(path) => {
                    if replayed {
                        // The per-path cache assumed append-only progression
                        // from events this session applied; a replay invalidates
                        // it, so re-read the file once.
                        tail_rows_cache.remove(&path.to_string_lossy().into_owned());
                    }
                    if tail_rows_cache.len() > 1024 {
                        // Conservative bound on paths from tails that were
                        // replaced by compaction; the next event re-seeds.
                        tail_rows_cache.clear();
                    }
                    let key = path.to_string_lossy().into_owned();
                    let rows = match tail_rows_cache.get(&key) {
                        // Append-only tail: previous total plus this event's
                        // frames. The first sight reads the file once (it
                        // already contains this event's frames).
                        Some(cached) => cached.saturating_add(count_v3_frame_rows(&frames)),
                        None => {
                            crate::storage::core::durable_tail::tail_log_rows(path)?.unwrap_or(0)
                        }
                    };
                    tail_rows_cache.insert(key, rows);
                    rows
                }
                None => count_v3_frame_rows(&frames),
            };
            match table_schema(engine, &db, &table) {
                Some(schema) => {
                    // Register on first sight; refresh the count afterwards.
                    let tail_path = crate::storage::core::durable_tail::live_tail_path(
                        &table_dir,
                        memtable_id,
                    )?
                    .ok_or_else(|| TaibaiError::Storage("tail vanished after append".into()))?;
                    engine.memtable_manager().restore_spilled_tail(
                        memtable_id,
                        &db,
                        &table,
                        &partition,
                        schema,
                        rows,
                        tail_path,
                    );
                    engine.memtable_manager().set_tail_rows(memtable_id, rows);
                }
                None => {
                    // DDL events are R2: the table must exist via bootstrap.
                    return Err(TaibaiError::Storage(format!(
                        "replicated table '{db}.{table}' is missing from the standby catalog; \
                         re-bootstrap from a fresh backup"
                    )));
                }
            }
            tracing::info!(seq, db = %db, table = %table, rows, "standby applied tail append");
            applied.advance(seq, true)?;
        }
        ReplicationEvent::SegmentUpdate {
            db,
            table,
            segment,
            kind,
            memtable_ids,
        } => {
            let bytes = match fetch_segment(
                engine,
                framed,
                deferred,
                deferred_bytes,
                &db,
                &table,
                &segment,
            )
            .await?
            {
                Some(bytes) => bytes,
                None => {
                    // SEGMENT_GONE: the primary compacted/TTL-removed the
                    // segment between this update's commit and the fetch.
                    // The retirement event later in this same stream ships
                    // the replacement (or deletes the local copy), so the
                    // skipped update cannot lose rows.
                    tracing::info!(
                        seq,
                        db = %db,
                        table = %table,
                        segment = %segment,
                        "segment superseded on primary; skipping stale update"
                    );
                    applied.advance(seq, true)?;
                    return Ok(());
                }
            };
            let dest = engine.data_dir().join(&db).join(&table).join(&segment);
            if let Some(parent) = dest.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let tmp = dest.with_extension("repl_tmp");
            {
                use std::io::Write;
                let mut file = std::fs::File::create(&tmp)?;
                file.write_all(&bytes)?;
                file.sync_all()?;
            }
            tracing::info!(seq, db = %db, table = %table, segment = %segment, bytes = bytes.len(), "standby fetched segment");
            crate::storage::segment::meta::atomic_replace(&tmp, &dest)?;
            // Retire the spilled memtables AND remove their tail files, so a
            // standby restart cannot recover the same rows a second time
            // (the segment now durably holds them).
            let table_dir = engine.data_dir().join(&db).join(&table);
            for id in &memtable_ids {
                crate::storage::core::durable_tail::clear(&table_dir, *id)?;
            }
            if kind == SegmentUpdateKind::Final || !memtable_ids.is_empty() {
                engine
                    .memtable_manager()
                    .retire_replicated_memtables(&memtable_ids);
            }
            // The fetched bytes carry the primary's CURRENT tombstone state,
            // which includes any queued rewrites for this segment.
            pending_rewrites.remove(&segment);
            applied.advance(seq, true)?;
        }
        ReplicationEvent::DeleteFooterRewrite {
            db,
            table,
            segment,
            bitmap,
            deleted_count,
        } => {
            let path = engine.data_dir().join(&db).join(&table).join(&segment);
            if path.exists() {
                crate::storage::segment::SegmentReader::rewrite_footer_with_deletes(
                    &path,
                    &bitmap,
                    deleted_count,
                )?;
            } else {
                // Segment not shipped yet: queue; applied when it materializes.
                pending_rewrites
                    .entry(segment)
                    .or_default()
                    .push((bitmap, deleted_count));
            }
            applied.advance(seq, true)?;
        }
        ReplicationEvent::SegmentsReplaced {
            db,
            table,
            new_segments,
            retired,
        } => {
            for segment in &new_segments {
                let bytes = match fetch_segment(
                    engine,
                    framed,
                    deferred,
                    deferred_bytes,
                    &db,
                    &table,
                    segment,
                )
                .await?
                {
                    Some(bytes) => bytes,
                    // The primary proved (via its replication ring) that this
                    // replacement was itself retired by a later compaction/TTL
                    // whose events follow this one in the stream. Skipping the
                    // write converges: the later event ships the newer segment
                    // and this event's retired-file removals still apply.
                    // (A hard fetch error below still fails the applier, so a
                    // missing file without provable retirement cannot silently
                    // drop rows.)
                    None => {
                        tracing::info!(
                            db = %db,
                            table = %table,
                            segment = %segment,
                            "replacement segment superseded on primary; skipping write"
                        );
                        continue;
                    }
                };
                let dest = engine.data_dir().join(&db).join(&table).join(segment);
                if let Some(parent) = dest.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                let tmp = dest.with_extension("repl_tmp");
                {
                    use std::io::Write;
                    let mut file = std::fs::File::create(&tmp)?;
                    file.write_all(&bytes)?;
                    file.sync_all()?;
                }
                crate::storage::segment::meta::atomic_replace(&tmp, &dest)?;
            }
            for segment in retired.iter().chain(new_segments.iter()) {
                pending_rewrites.remove(segment);
            }
            for segment in &retired {
                let path = engine.data_dir().join(&db).join(&table).join(segment);
                if path.exists() {
                    let _ = std::fs::remove_file(&path);
                    let _ = std::fs::remove_file(crate::storage::segment::meta::sidecar_meta_path(
                        &path,
                    ));
                }
            }
            applied.advance(seq, true)?;
        }
        ReplicationEvent::SegmentsRemoved {
            db,
            table,
            segments,
        } => {
            for segment in &segments {
                pending_rewrites.remove(segment);
                let path = engine.data_dir().join(&db).join(&table).join(segment);
                if path.exists() {
                    let _ = std::fs::remove_file(&path);
                    let _ = std::fs::remove_file(crate::storage::segment::meta::sidecar_meta_path(
                        &path,
                    ));
                }
            }
            applied.advance(seq, true)?;
        }
        ReplicationEvent::CatalogSnapshot { db, json } => {
            let dest = engine
                .data_dir()
                .join("meta")
                .join("catalogs")
                .join(format!("{db}.json"));
            if json.is_empty() {
                // DROP DATABASE: the snapshot is absent on the primary.
                let _ = std::fs::remove_file(&dest);
                engine.remove_catalog(&db);
            } else {
                if let Some(parent) = dest.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                let tmp = dest.with_extension("repl_tmp");
                {
                    use std::io::Write;
                    let mut file = std::fs::File::create(&tmp)?;
                    file.write_all(&json)?;
                    file.sync_all()?;
                }
                crate::storage::segment::meta::atomic_replace(&tmp, &dest)?;
                engine.reload_catalog(&db)?;
            }
            applied.advance(seq, true)?;
        }
        ReplicationEvent::AuthSnapshot { json } => {
            let dest = engine.data_dir().join("meta").join("auth.json");
            if let Some(parent) = dest.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let tmp = dest.with_extension("repl_tmp");
            {
                use std::io::Write;
                let mut file = std::fs::File::create(&tmp)?;
                file.write_all(&json)?;
                file.sync_all()?;
            }
            crate::storage::segment::meta::atomic_replace(&tmp, &dest)?;
            engine.auth_manager().reload()?;
            applied.advance(seq, true)?;
        }
    }
    Ok(())
}

fn table_schema(
    engine: &crate::sql::execution::Engine,
    db: &str,
    table: &str,
) -> Option<crate::types::batch::Schema> {
    let meta = engine.get_table_meta(db, table)?;
    Some(crate::types::batch::Schema::new(meta.columns.clone()))
}

/// Stream a segment file from the primary. Frames that arrive while the
/// fetch is in flight (later replication events) are buffered in `deferred`
/// so apply order is preserved. `Ok(None)` means the primary answered
/// SEGMENT_GONE: the segment was retired by a compaction/TTL event later in
/// this stream, so the triggering update is stale and must be skipped.
#[allow(clippy::type_complexity)]
async fn fetch_segment(
    engine: &crate::sql::execution::Engine,
    framed: &mut Framed<tokio::net::TcpStream, MessageCodec>,
    deferred: &mut VecDeque<(u64, ReplicationEvent, u64)>,
    deferred_bytes: &mut u64,
    db: &str,
    table: &str,
    segment: &str,
) -> Result<Option<Bytes>> {
    let _ = engine;
    let mut request = Vec::new();
    put_str(&mut request, db);
    put_str(&mut request, table);
    put_str(&mut request, segment);
    request.extend_from_slice(&0u64.to_le_bytes());
    request.extend_from_slice(&(4 * 1024 * 1024u32).to_le_bytes());
    framed
        .send(Message::new(
            MSG_TYPE_REPL_SEGMENT_FETCH,
            Bytes::from(request),
        ))
        .await?;

    let mut bytes: Option<Bytes> = None;
    loop {
        let message = framed.next().await.ok_or_else(|| {
            TaibaiError::Storage("primary closed the stream during fetch".into())
        })??;
        match message.msg_type {
            MSG_TYPE_REPL_SEGMENT_GONE => {
                return Ok(None);
            }
            MSG_TYPE_REPL_SEGMENT_CHUNK => {
                if message.payload.len() < 16 {
                    return Err(TaibaiError::Storage("REPL_SEGMENT_CHUNK truncated".into()));
                }
                let total_len =
                    u64::from_le_bytes(message.payload[..8].try_into().unwrap()) as usize;
                let offset =
                    u64::from_le_bytes(message.payload[8..16].try_into().unwrap()) as usize;
                let chunk = message.payload.slice(16..);
                let merged = match bytes.take() {
                    Some(existing) => {
                        let mut buf = Vec::with_capacity(total_len);
                        buf.extend_from_slice(&existing);
                        buf.extend_from_slice(&chunk);
                        Bytes::from(buf)
                    }
                    None => {
                        if offset != 0 {
                            return Err(TaibaiError::Storage(
                                "REPL_SEGMENT_CHUNK started at a non-zero offset".into(),
                            ));
                        }
                        let mut buf = Vec::with_capacity(total_len);
                        buf.extend_from_slice(&chunk);
                        Bytes::from(buf)
                    }
                };
                if merged.len() == total_len {
                    return Ok(Some(merged));
                }
                bytes = Some(merged);
            }
            MSG_TYPE_REPL_EVENT => {
                if message.payload.len() < 12 {
                    return Err(TaibaiError::Storage("REPL_EVENT truncated".into()));
                }
                let seq = u64::from_le_bytes(message.payload[..8].try_into().unwrap());
                let len = u32::from_le_bytes(message.payload[8..12].try_into().unwrap()) as usize;
                if message.payload.len() < 12 + len {
                    return Err(TaibaiError::Storage("REPL_EVENT payload truncated".into()));
                }
                let event = ReplicationEvent::decode(&message.payload[12..12 + len])?;
                let event_bytes = len as u64;
                if *deferred_bytes + event_bytes > DEFERRED_EVENT_BUDGET_BYTES {
                    return Err(TaibaiError::Storage(format!(
                        "deferred replication events exceeded the {} byte budget                          during a segment fetch; reconnecting to resume from the                          applied sequence",
                        DEFERRED_EVENT_BUDGET_BYTES
                    )));
                }
                *deferred_bytes += event_bytes;
                deferred.push_back((seq, event, event_bytes));
            }
            MSG_TYPE_ERROR => {
                return Err(TaibaiError::Storage(format!(
                    "segment fetch rejected: {}",
                    String::from_utf8_lossy(&message.payload)
                )));
            }
            _ => {}
        }
    }
}

async fn send_ack(
    framed: &mut Framed<tokio::net::TcpStream, MessageCodec>,
    applied_seq: u64,
) -> Result<()> {
    framed
        .send(Message::new(
            MSG_TYPE_REPL_ACK,
            Bytes::from(applied_seq.to_le_bytes().to_vec()),
        ))
        .await
}

/// Read one frame and require it to be one of `ok_or_fail`: AUTH_OK/OK pass,
/// AUTH_FAIL/ERROR fail with the message text.
async fn expect(
    framed: &mut Framed<tokio::net::TcpStream, MessageCodec>,
    ok_or_fail: &[u8],
) -> Result<()> {
    let message = framed
        .next()
        .await
        .ok_or_else(|| TaibaiError::Storage("connection closed during handshake".into()))??;
    let text = String::from_utf8_lossy(&message.payload).to_string();
    if ok_or_fail.contains(&message.msg_type) && message.msg_type != MSG_TYPE_AUTH_FAIL {
        return Ok(());
    }
    Err(TaibaiError::Auth(if text.is_empty() {
        format!("handshake failed (frame {:#04X})", message.msg_type)
    } else {
        text
    }))
}

fn put_str(buf: &mut Vec<u8>, value: &str) {
    buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
    buf.extend_from_slice(value.as_bytes());
}

// Silence unused import when build_error is only used in future variants.
#[allow(dead_code)]
fn _assert_build_error_in_scope(_: fn(&str) -> Message) {
    let _ = build_error;
}
