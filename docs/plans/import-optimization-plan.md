# 导入性能优化方案

## 1. 现状分析

### 1.1 当前性能指标

| 指标 | 当前值 | 目标值 |
|------|--------|--------|
| SQL INSERT 吞吐 | ~11万行/秒 | 30万行/秒 |
| WAL 文件总大小 | 2.8GB (44×64MB) | 与 segment 一致 (~1.1GB) |
| Segment 文件大小 | 2GB | 正常 |
| 批量导入 Segment | 1.1GB | 正常参考值 |

### 1.2 数据流路径分析

当前 SQL INSERT 路径（`multi_insert` 示例）：

```
CSV 解析 → build_multi_insert_sql() → TCP 发送 SQL 文本
→ 服务端 SQL Parser 解析 → AST
→ exec_insert(): eval_expr_to_scalar_with_scale_timezone() 逐行/逐列
→ RecordBatch 构建
→ serialize_batch_for_wal() 序列化为行式字节
→ WAL write (Begin + Insert + Commit + sync_all)
→ encode_column_direct() 列编码
→ zstd 压缩（rayon 并行）
→ 写入 active_segment (BufWriter)
```

批量二进制导入路径（`batch_import` 示例）：

```
客户端列式编码 → TCP 发送二进制
→ parse_direct_bulk_payload() 零拷贝解析
→ zstd 压缩（rayon 并行）
→ 写入 active_segment (BufWriter)
```

### 1.3 瓶颈定位

通过代码分析，识别出以下主要瓶颈：

| # | 瓶颈 | 位置 | 影响 |
|---|------|------|------|
| B1 | SQL 文本解析 + 表达式求值 | `dml_insert.rs` + `helpers.rs` | 每个 INSERT 语句需要完整解析，5000 行的 multi-INSERT 生成巨大 SQL 文本 |
| B2 | 逐行 ScalarValue 转换 | `eval_expr_to_scalar_with_scale_timezone()` | 对每个值创建 `ScalarValue` 枚举，分配开销大 |
| B3 | WAL 行式序列化 | `serialize_batch_for_wal()` | 每个值逐个序列化，null 标志 + 类型匹配，无批量优化 |
| B4 | WAL sync_all() | `WalWriter::write_batch()` | 每个 INSERT batch 都 fsync，即使是 bulk mode 也只免了 sync |
| B5 | WAL 25字节头开销 | `WalRecord::encode()` | 每条记录 25 字节头（seq+type+txn_id+len+crc），加上 Begin/Commit 包裹 |
| B6 | 全局 Mutex 锁竞争 | `wal_writers`, `active_segments`, `catalogs` | 多个锁串行化写入路径 |
| B7 | CRC32 计算 | `WalRecord::encode()` | 每个 WAL record 计算 CRC32 |

---

## 2. 优化策略

### 策略 1: SQL INSERT 批量预编译路径（预期 +3-5万/秒）

**原理**: 绕过 SQL 文本解析，为 multi-INSERT 添加快速路径。

**方案**:
- 在 `exec_insert` 中，当检测到 batch 模式且是纯值插入（无表达式、无函数调用）时，跳过 `eval_expr_to_scalar_with_scale_timezone()` 的完整表达式求值
- 直接从 AST 的 `Expr::Literal` 提取值，按类型批量转换
- 预估节省: 减少 30-40% 的 CPU 时间在解析和求值阶段

**预期收益**: 11万 → 14-16万/秒（+27-45%）

### 策略 2: WAL 列式序列化（预期 +2-3万/秒 + WAL 大小减半）

**原理**: 当前 `serialize_batch_for_wal()` 是逐值序列化（行式），改为列式批量序列化。

**当前 WAL 序列化格式**（每值 1+ 字节开销）:
```
for each column:
    data_type: u8
    for each row:
        null_flag: u8
        if not null: serialize_scalar(value)   // 逐值匹配类型
    data_len: u32 + data
```

**优化后格式**:
```
num_cols: u32, num_rows: u32
for each column:
    data_type: u8
    null_bitmap: ceil(num_rows/8) bytes    // 位图，非逐字节
    values: 批量 memcpy                     // 直接内存拷贝
```

**优势**:
- null 标志从每行 1 字节 → 每 8 行 1 字节（~8x 节省）
- 固定宽度类型（INT32/INT64/FLOAT64/DECIMAL/TIMESTAMP）直接 `memcpy`，跳过逐值序列化
- 与 segment 编码对齐，恢复时可直接复用

**预期收益**:
- WAL 写入速度 +15-25%（减少序列化 CPU）
- WAL 文件大小减少 40-50%（从 2.8GB → ~1.5GB）

### 策略 3: WAL 写入优化（预期 +3-5万/秒）

**3a: 批量合并提交**

当前每个 INSERT batch 都产生 3 条 WAL 记录（Begin + Insert + Commit），25 字节头 × 3 = 75 字节开销/batch。

**方案**: 在 bulk mode 下，累积多个 insert batch 后一次性写 WAL：
- 引入 `WalBatchBuffer`，按时间（如 50ms）或大小（如 4MB）阈值批量提交
- 一次 Begin → N 个 Insert → 一次 Commit → 一次 sync
- 减少 Begin/Commit 记录数量 N 倍
- 减少 sync_all 调用次数

**3b: 去除 CRC32 计算（bulk mode）**

CRC32 计算在 bulk mode 下是不必要的性能消耗。bulk mode 已经假设可控环境。

**方案**: bulk mode 下跳过 CRC32 计算，写入 0 作为占位符。或改用更轻量的校验（如 Adler32）。

**3c: WAL 文件预分配**

当前每次 rotate 都重新打开文件。改为预分配文件空间（`set_len()`），减少文件系统元数据开销。

**预期收益**: 11万 → 14-16万/秒（减少 fsync 次数和序列化开销）

### 策略 4: 锁优化 - 无锁/细粒度锁（预期 +2-4万/秒）

**问题**: 当前写入路径持有多个全局 Mutex：

```
wal_writers.lock()      // 全局 WAL 写入锁
active_segments.lock()  // 全局 segment 写入锁
catalogs.write()        // 全局 catalog 写入锁
```

**方案**:
- 按 table 分片锁: 使用 `HashMap<String, Arc<TableSlot>>` (TableWriter has been removed; this is the proposed target structure) 替代全局锁，每个表独立锁
- WAL 写入: 每个表独立的 `WalWriter`（当前已部分实现，但 HashMap 的全局 Mutex 仍序列化所有表的 WAL 操作）
- Segment 写入: 每个表独立的 `ActiveSegment`，移除全局 `active_segments` Mutex
- Catalog 更新: 使用 `AtomicU64` 存储 `row_count`，避免每次 insert 都获取 catalog 写锁

**预期收益**: 单表场景不明显，多表并发场景显著。单表预计 +10-20%。

### 策略 5: Segment 写入流水线化（预期 +3-5万/秒）

**原理**: 将 WAL 写入和 Segment 编码压缩并行化。

**当前流程（串行）**:
```
INSERT → WAL write → encode + compress → segment write → return OK
```

**优化后流程（流水线）**:
```
INSERT → WAL write → return OK
         └→ async: encode + compress → segment write（后台线程）
```

**方案**:
- 使用 `crossbeam::channel` 或类似机制，将 batch 发送到后台压缩线程
- 后台线程负责列编码 + zstd 压缩 + 写入 segment
- 前端线程只做 WAL 写入，立即返回
- 引入背压机制：当后台积压超过阈值时，前端阻塞等待

**预期收益**: 11万 → 15-17万/秒（WAL 写入和压缩并行）

### 策略 6: zstd 压缩级别调优（预期 +1-2万/秒）

**当前**: 默认 level 3，通过环境变量 `TAIBAI_BULK_ZSTD_LEVEL` 可调。

**分析**: zstd level 3 的压缩速度约 400MB/s（单核），对于批量导入场景：
- Level 1: ~500MB/s，压缩比略低
- Level 3: ~400MB/s，压缩比适中
- 导入场景对压缩比不敏感（后续查询才需要），建议 level 1

**方案**: 将默认压缩级别从 3 降为 1，在 bulk mode 下甚至可以 level 0（不压缩）。

**预期收益**: 减少 20-30% 的压缩 CPU 时间，吞吐 +5-10%。

### 策略 7: 客户端批量大小和 Pipeline 优化

**当前 `multi_insert`**: batch_size=5000, pipeline_depth=4

**分析**: 
- 5000 行的 SQL INSERT 文本约 1.5-2MB，网络传输开销大
- SQL 文本格式比二进制格式大 3-5 倍
- pipeline_depth=4 可能不够充分利用网络带宽

**建议**: 
- 增加 batch_size 到 10000-20000（减少 SQL 解析次数）
- 增加 pipeline_depth 到 8-16
- 或切换到列式二进制导入路径（`bulk_insert_columnar`），完全绕过 SQL

---

## 3. WAL 大小优化方案

### 3.1 问题根因

WAL 2.8GB vs Segment 1.1GB（批量导入）/ 2GB（SQL INSERT），差异来源：

| 因素 | 额外开销 |
|------|----------|
| WAL 记录头（25 字节 × 3 × batch 数） | ~5-10% |
| null 标志（每值 1 字节 vs 位图） | ~10-15% |
| 行式序列化无压缩 | ~50-60% |
| Begin/Commit 空记录开销 | ~2-3% |
| 数据未压缩（WAL 是原始数据，segment 是 zstd 压缩后） | 最大因素 |

**核心问题**: WAL 写入的是未压缩的原始数据，而 segment 写入的是 zstd 压缩后的数据。对于 tick 数据（大量 DECIMAL、INT32），压缩比约 2:1，所以 WAL 约为 segment 的 2 倍大小。

### 3.2 优化方案

#### 方案 A: WAL 压缩写入（推荐）

对 WAL 数据使用轻量级压缩（lz4 或 zstd level 1）：

```
WAL entry:
    header (25 bytes)
    compression_type: u8 (0=none, 1=lz4, 2=zstd)
    uncompressed_len: u32
    compressed_data: bytes
```

- 预期 WAL 大小: 2.8GB → ~0.8-1.0GB（与 segment 接近）
- CPU 开销: lz4 压缩速度 >1GB/s，对吞吐影响 <5%
- 恢复时自动检测压缩类型解压

#### 方案 B: WAL 截断优化

当前 WAL 只在 `clear()` 时删除（整个 WAL 目录清空）。优化为：
- 每当一个 segment 被 seal（写入 footer + sync），立即截断该 segment 对应的 WAL 数据
- 实现: 记录每个 WAL 文件中哪些数据已被持久化到 segment，定期清理

#### 方案 C: 列式 WAL 格式 + 位图 null（见策略 2）

将 WAL 从行式改为列式格式，null 标志改为位图：
- 预期减少 WAL 大小 10-15%
- 与方案 A 叠加效果更好

#### 方案 D: bulk mode 下可选禁用 WAL

当前 `BulkImportConfig.disable_wal` 已支持但默认 false。对于可重放的批量导入场景：
- 设置 `disable_wal: true`
- WAL 大小直接降为 0
- 风险: 导入过程中崩溃会丢失未持久化的数据

### 3.3 推荐组合

**短期**（方案 C + D）: 列式 WAL + 可选禁用 WAL
- 实现简单，风险低
- 预期 WAL: 2.8GB → 1.2-1.5GB（不禁用 WAL）或 0（禁用 WAL）

**中期**（方案 A + B + C）: WAL 压缩 + 截断优化 + 列式格式
- 完整 WAL 优化
- 预期 WAL: 2.8GB → 0.5-0.8GB

---

## 4. 实施优先级和预期效果汇总

| 优先级 | 策略 | 预期吞吐提升 | 预期 WAL 大小 | 实现复杂度 | 风险 |
|--------|------|-------------|--------------|-----------|------|
| P0 | 策略 7: 客户端批次调优 | 11→13万 | 不变 | 低 | 无 |
| P0 | 策略 6: zstd level 降为 1 | +5-10% | 不变 | 极低 | 无 |
| P1 | 策略 2: WAL 列式序列化 | +15-25% | 2.8→1.5GB | 中 | 中 |
| P1 | 策略 3: WAL 批量合并提交 | +20-30% | 2.8→2.5GB | 中 | 低 |
| P2 | 策略 5: Segment 写入流水线化 | +30-45% | 不变 | 高 | 中 |
| P2 | 策略 4: 锁优化 | +10-20% | 不变 | 中 | 低 |
| P3 | 策略 1: SQL INSERT 快速路径 | +27-45% | 不变 | 高 | 低 |
| P3 | WAL 压缩写入 | 不变 | 2.8→0.8GB | 中 | 低 |

### 组合预期

**Phase 1（P0，1-2天）**:
- 策略 6 + 7: 11万 → 13-14万/秒
- 改动: 环境变量配置 + 客户端参数调优

**Phase 2（P1，3-5天）**:
- 策略 2 + 3: 13万 → 18-22万/秒
- WAL 大小: 2.8GB → 1.2-1.5GB
- 改动: `wal_helper.rs` 序列化重写 + `WalWriter` 批量提交逻辑

**Phase 3（P2，5-7天）**:
- 策略 4 + 5: 18万 → 25-30万/秒
- 改动: `engine.rs` 锁重构 + 后台压缩线程

**达到 30万/秒的最低组合**: 策略 2 + 3 + 5 + 6 + 7

---

## 5. 技术细节

### 5.1 WAL 列式序列化新格式

```rust
pub fn serialize_batch_for_wal_v2(batch: &RecordBatch) -> Vec<u8> {
    let mut buf = Vec::with_capacity(estimate_wal_size(batch));
    let num_cols = batch.num_columns() as u32;
    let num_rows = batch.num_rows() as u32;

    buf.extend_from_slice(&num_cols.to_le_bytes());
    buf.extend_from_slice(&num_rows.to_le_bytes());

    for col_idx in 0..batch.num_columns() {
        let col = batch.column(col_idx).unwrap();
        buf.push(col.data_type() as u8);

        // Null bitmap (compact)
        let bitmap = col.bitmap();
        let null_count = bitmap.null_count();
        buf.extend_from_slice(&(null_count as u32).to_le_bytes());
        if null_count > 0 {
            let bytes = bitmap.to_bytes();
            buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(&bytes);
        } else {
            buf.extend_from_slice(&0u32.to_le_bytes());
        }

        // Values: bulk memcpy for fixed-width types
        match col {
            ColumnArray::Int32 { data, .. } => {
                let bytes = unsafe {
                    std::slice::from_raw_parts(data.as_ptr() as *const u8, data.len() * 4)
                };
                buf.extend_from_slice(bytes);
            }
            ColumnArray::Int64 { data, .. } => {
                let bytes = unsafe {
                    std::slice::from_raw_parts(data.as_ptr() as *const u8, data.len() * 8)
                };
                buf.extend_from_slice(bytes);
            }
            // ... 类似处理其他固定宽度类型
            ColumnArray::VarChar { data, .. } => {
                for s in data {
                    buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
                    buf.extend_from_slice(s.as_bytes());
                }
            }
            // ...
        }
    }
    buf
}
```

### 5.2 WAL 批量合并提交

```rust
pub struct WalBatchBuffer {
    buffer: Vec<u8>,
    last_flush: Instant,
    max_buffer_size: usize,
    flush_interval: Duration,
}

impl WalBatchBuffer {
    pub fn append(&mut self, data: &[u8]) -> bool {
        self.buffer.extend_from_slice(data);
        self.buffer.len() >= self.max_buffer_size
            || self.last_flush.elapsed() >= self.flush_interval
    }

    pub fn flush(&mut self, writer: &mut WalWriter) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        // Write as single large WAL record
        writer.write_batch_no_sync(0, &[(RecordType::Insert, &self.buffer)])?;
        writer.sync()?;
        self.buffer.clear();
        self.last_flush = Instant::now();
        Ok(())
    }
}
```

### 5.3 Segment 后台写入线程

```rust
pub struct SegmentWritePipeline {
    sender: crossbeam::channel::Sender<PipelineBatch>,
    handle: Option<std::thread::JoinHandle<()>>,
}

struct PipelineBatch {
    db: String,
    table: String,
    batch: RecordBatch,
}

impl SegmentWritePipeline {
    pub fn new(engine: Arc<Engine>) -> Self {
        let (tx, rx) = crossbeam::channel::bounded(16);
        let handle = std::thread::spawn(move || {
            while let Ok(item) = rx.recv() {
                let _ = engine.process_segment_write(&item);
            }
        });
        Self { sender: tx, handle: Some(handle) }
    }
}
```

---

## 6. 验证方法

### 6.1 性能测试

使用现有 `multi_insert` 示例导入相同的 tick CSV 数据：
- 记录: 总行数、总耗时、吞吐（行/秒）
- 监控: CPU 使用率、磁盘 I/O、内存使用

### 6.2 WAL 大小验证

- 导入前后检查 `data/{db}/{table}/wal/` 目录大小
- 对比 WAL 文件数和总大小
- 验证 WAL 恢复正确性（kill -9 后重启）

### 6.3 正确性验证

- 每个 phase 完成后，对比 segment 文件内容与优化前一致
- WAL 恢复后数据完整性检查
- 使用 `segment_checker` 验证 segment 文件格式
