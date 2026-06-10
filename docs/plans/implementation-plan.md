# taibai: Implementation Plan

## Context

taibai is a greenfield Rust columnar database server, wire-compatible with the existing taibai-go client. The repo initially contained only `docs/design/columnar-database-design.md`. This plan implements all 4 phases incrementally, where each phase produces a working, testable binary.

## Protocol Foundation (from taibai source)

- **7-byte header**: `[0xDB01 LE (2B)] [msgType (1B)] [payloadLen LE (4B)]`
- **Max message**: 64MB
- **All encoding**: Little-Endian
- **Key wire formats**:
  - Login: `[u32 len][user][u32 len][pass][u32 len][db]`
  - Query: `[u32 len][db][u32 len][sql]`
  - OK: `[u64 LE rows_affected]` (8 bytes)
  - Error: UTF-8 string (entire payload)
  - Text Result: `u32 num_cols | per col: u32 name_len+name+u8 dtype+u8 scale+u8 precision | u32 num_rows | per cell: u8 null_flag + value`

---

## Phase 1: Basic Skeleton (Protocol + Network Shell)

**Goal**: TCP server on 5151; taibai-go client can connect, authenticate, ping/pong.

### Step 1.1 - Cargo.toml + module skeleton

**Create**: `Cargo.toml`, `src/main.rs`, all `src/**/mod.rs` stubs

Dependencies: `tokio` (full), `tokio-util` (codec), `bytes`, `uuid` (v4), `tracing`, `tracing-subscriber`, `clap` (derive), `serde` + `serde_json`, `crc32fast`

Module tree (only Phase 1 sub-modules declared):
```
src/main.rs          → mod common; mod types; mod protocol; mod storage; mod sql; mod net;
src/common/mod.rs    → pub mod error; pub mod config;
src/types/mod.rs     → (empty)
src/protocol/mod.rs  → pub mod message; pub mod types; pub mod codec;
src/storage/mod.rs   → (empty)
src/sql/mod.rs       → (empty)
src/net/mod.rs       → pub mod server; pub mod session; pub mod handler;
```

**Verify**: `cargo check`

### Step 1.2 - `common/error.rs`

Unified error type:
```rust
pub enum TaibaiError {
    Io(std::io::Error),
    Protocol(String),
    Auth(String),
    Config(String),
    Storage(String),
    Sql(String),
    Internal(String),
}
pub type Result<T> = std::result::Result<T, TaibaiError>;
```

### Step 1.3 - `common/config.rs`

```rust
pub struct ServerConfig {
    pub listen_addr: String,       // "0.0.0.0:5151"
    pub data_dir: String,          // "./data"
    pub default_database: String,  // "default"
    pub max_message_size: usize,   // 64MB
    pub flush_threshold_rows: u32, // 65536
    pub batch_size: usize,         // 4096
}
```
With `clap` CLI parsing for `--addr`, `--data-dir`.

### Step 1.4 - `protocol/message.rs`

Constants: `MAGIC = [0xDB, 0x01]`, `HEADER_SIZE = 7`, all `MsgType` as `pub const u8`.

```rust
pub struct MessageHeader { pub msg_type: u8, pub payload_len: u32 }
impl MessageHeader {
    pub fn encode(&self) -> [u8; 7];
    pub fn decode(buf: &[u8; 7]) -> Result<Self>;
}
pub struct Message { pub msg_type: u8, pub payload: Bytes }
```

### Step 1.5 - `protocol/types.rs`

```rust
#[repr(u8)] pub enum DataType { Bool=1, Int8=2, ..., Vector=19 }
#[repr(u8)] pub enum StorageType { Int64=0, Int32=1, ..., Decimal=18 }
```
With `fixed_size()`, `from_u8()`, `display_name()`.

### Step 1.6 - `protocol/codec.rs` (critical)

```rust
pub struct MessageCodec { max_message_size: usize }
impl Decoder for MessageCodec { type Item = Message; type Error = TaibaiError; }
impl Encoder<Message> for MessageCodec { type Error = TaibaiError; }
```
- Decoder: read 7-byte header, validate magic, check size, read payload
- Encoder: write header + payload

### Step 1.7 - `net/session.rs`

```rust
pub struct Session {
    id: Uuid, database: String, username: String,
    created_at: Instant, last_activity: Instant,
}
```

### Step 1.8 - `net/handler.rs`

```rust
pub struct Handler { session: Option<Session>, config: Arc<ServerConfig> }
impl Handler {
    pub fn new(config: Arc<ServerConfig>) -> Self;
    pub fn handle_message(&mut self, msg: Message) -> Result<Vec<Message>>;
}
```
Dispatch: LOGIN→AuthOK, PING→Pong, LOGOUT→close, QUERY→dummy result, other→Error.

Response builders: `build_auth_ok()`, `build_ok()`, `build_error()`, `build_pong()`, `build_text_result()`.

### Step 1.9 - `net/server.rs`

```rust
pub struct Server { config: Arc<ServerConfig> }
impl Server {
    pub async fn run(&self) -> Result<()>;
}
```
Accept loop + `handle_connection()` with `Framed<TcpStream, MessageCodec>`.

### Step 1.10 - `main.rs`

Parse CLI → init tracing → ensure data_dir → `Server::new(config).run().await`

**Phase 1 checkpoint**: taibai-go client connects, Login→AuthOK, Ping→Pong, Query("SHOW TABLES")→Result.

---

## Phase 2: Types + Storage + WAL + Basic SQL

**Goal**: CREATE TABLE, INSERT, BulkInsert, SELECT *, WAL recovery.

### Step 2.1 - `types/` module

| File | Key types |
|------|-----------|
| `scalar.rs` | `enum ScalarValue` — one variant per DataType |
| `bitmap.rs` | `struct NullBitmap` — Arrow-style bit=1=NULL |
| `array.rs` | `enum ColumnArray` — typed Vec<T> + NullBitmap |
| `batch.rs` | `struct RecordBatch` — Vec<ColumnArray> + schema |

### Step 2.2 - `storage/schema.rs`

`struct TableSchema`, `struct DatabaseSchema` with JSON persistence (`schema.json`).
`create_table()`, `drop_table()`, `get_table()`.

### Step 2.3 - `storage/wal.rs`

WAL entry: `CRC(4B) + entry_len(4B) + table_id + segment_id + row_count + column_count + [columns]`.
`WalWriter` (append + fsync), `WalReader` (read all for recovery).

### Step 2.4 - `storage/segment.rs`

Segment format: Header(64B) + ColumnChunks(Blocks) + Footer.
- `SegmentWriter`: create → write_block → seal (write footer, backfill header, fsync)
- `SegmentReader`: open (prefer mmap) → read_header → read_footer → read_column

### Step 2.5 - Write entry point (now merged into sql/execution/engine.rs)

> Note: the old storage/writer.rs::TableWriter (which combined WAL + memory buffer +
> segment sealing) has been removed. Its responsibilities now live entirely in Engine:
> - WAL writes: sql/execution/wal_helper.rs::write_wal
> - Segment append / seal: Engine::exec_bulk_insert_with_wal_control,
>   Engine::exec_bulk_insert_payload_with_wal_control, Engine::seal_segment
> - Thresholds: flush row group at SQL_INSERT_ROW_GROUP_TARGET_ROWS, seal segment when
>   the file exceeds 2 GB
>
> The following modules have been removed as dead code:
> storage/writer.rs, storage/memtable.rs, storage/async_segment.rs,
> storage/column_encoder.rs, storage/reader.rs, and the legacy sql/executor.rs.

### Step 2.6 - Read entry point

SegmentReader::open plus sql/execution/helpers.rs::discover_segments are used by
sql/execution/query.rs (SELECT) and the UPDATE/DELETE rewrite paths in dml_update.rs /
dml_delete.rs to enumerate and scan .dat files.

### Step 2.7 - `sql/parser/`

Hand-written recursive descent: `Lexer` -> tokens, `Parser` -> AST.
Statements: CREATE TABLE, INSERT INTO, DROP TABLE, SHOW TABLES, SELECT.

### Step 2.8 - `sql/execution/engine.rs` (Engine facade)

`struct Engine`:
- `catalogs: RwLock<HashMap<String, DatabaseCatalog>>`
- `wal_writers: Mutex<HashMap<String, WalWriter>>`
- `active_segments: Mutex<HashMap<String, ActiveSegment>>`
- `segment_seqs: Mutex<HashMap<String, u32>>`

`execute(session, sql)` -> parse -> dispatch to `dml_insert` / `dml_update` /
`dml_delete` / `query` / `ddl`. On startup it scans each table directory for
`seg_*.dat` to seed `segment_seqs` and uses `WalRecovery::replay` to recover
unflushed batches.

### Step 2.9 - Wire into handler

`Handler` holds `engine: Arc<Engine>`. `MSG_TYPE_QUERY` -> `Engine::execute`;
`MSG_TYPE_BULK_INSERT_COLUMNAR` -> `Engine::exec_bulk_insert_payload_with_wal_control`
(bypasses SQL parsing and writes columnar payloads directly to the active segment).

### Step 2.10 - `protocol/bulk.rs`

Parses the BulkInsert binary payload; data is then handed to
`Engine::exec_bulk_insert_payload_with_wal_control` which uses rayon-parallel zstd
compression before appending row groups to the current active segment.
### Step 3.1 - `sql/planner/`

AST → `LogicalPlan` tree: Scan, Filter, Project, Aggregate, Sort, Limit.

### Step 3.2 - `sql/optimizer/`

Rules: predicate pushdown, column pruning, constant folding, limit pushdown.

### Step 3.3 - `sql/executor/` pull-model operators

Trait: `fn next_batch(&mut self) -> Result<Option<RecordBatch>>`.
Operators: ScanExec, FilterExec, ProjectExec, HashAggregateExec, SortExec, LimitExec.
Expression evaluator for vectorized batch evaluation.

### Step 3.4 - Wire full query pipeline

Engine::execute for Select → plan → optimize → build pipeline → pull batches → serialize.

### Step 3.5 - `storage/sort_key.rs`

Sorted insertion: binary-search position, sorted blocks, min/max index for range pruning.

### Step 3.6 - `storage/partition.rs`

PARTITION BY DAY/YEAR: partition key in filename (`p{key}_XXXX.dat`), partition pruning on WHERE.

**Phase 3 checkpoint**: Full SQL queries with WHERE/GROUP BY/ORDER BY/LIMIT, sorted tables, partitioned tables.

---

## Phase 4: Push Execution + Enhancements

### Step 4.1 - Push pipeline (`push.rs`)

`trait PushOp { fn push(&mut self, batch) -> Result<()>; fn finalize() -> Result<Option<RecordBatch>>; }`
Enum-based monomorphic pipelines for cache efficiency.

### Step 4.2 - Morsel parallelism (`morsel.rs`)

Per-task morsel fetching, work stealing, concurrent hash aggregate.

### Step 4.3 - Prepared statements (`protocol/encoding.rs`)

KV sub-protocol: Prepare/Execute/CloseStmt. Session holds prepared plan cache.

### Step 4.4 - Advanced encodings (`storage/encoding.rs`)

Dictionary, Delta-of-delta, RLE, Bit-packed. Auto-selection heuristic at block flush.

### Step 4.5 - Compression (`protocol/compress.rs`)

gzip/zlib message decompression wrapper.

### Step 4.6 - TLS (`net/tls.rs`)

Optional `tokio-rustls` + `rustls` with cert/key from config.

### Step 4.7 - Binary result format (`protocol/result.rs`)

MsgTypeBinaryQueryResult (0x24) + ZCPY format.

### Step 4.8 - Benchmarks (`benches/scan_bench.rs`)

Criterion: scan, filter, aggregate, WAL write, BulkInsert throughput.

---

## Dependency Order

```
Phase 1: error → config → message → types → codec → session → handler → server → main
Phase 2: types/* → schema → wal → segment → writer → reader → parser → executor → handler update → bulk
Phase 3: planner → optimizer → executor/* → Engine update → sort_key → partition
Phase 4: push → morsel → encoding → compress → kv-encoding → tls → binary-result → bench
```

## Verification

1. **Phase 1**: `cargo run` → taibai-go client: Login→AuthOK, Ping→Pong
2. **Phase 2**: `cargo run` → CREATE TABLE → INSERT → SELECT * → kill → restart → SELECT * (WAL recovery)
3. **Phase 3**: Full SQL queries via taibai-go client, sorted/partitioned table tests
4. **Phase 4**: Benchmarks (`cargo bench`), prepared statements, TLS connections
