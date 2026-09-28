# taibai: Rust 列式数据库架构方案

> 状态注记：经典 WAL（`table/wal/`、`storage/wal.rs`）已移除，现行持久化由 durable tail（`memtable_tail/`）与 active segment 自身承担，见 `TAIBAI_DESIGN_ZH.md`。本文保留为历史设计记录。

## Context

基于 taibai-go 客户端驱动已有的自定义TCP二进制协议，用 Rust 构建全新的列式数据库服务端。目标是通用数据库（非业务绑定），面向 AI 时代大数据场景，支持自研SQL引擎。

## 核心决策

| 决策 | 选择 | 原因 |
|------|------|------|
| 语言 | Rust | 无GC尾延迟可预测、SIMD友好、零成本抽象 |
| 协议 | 沿用 taibai 7字节头+自定义消息类型 | 与现有客户端兼容 |
| 存储 | 分段文件 `.dat`，序列号递增，单文件≤2GB | 避免大量文件句柄，列数据集中管理，顺序写友好 |
| SQL | 手写递归下降解析器 | 精简依赖、完全控制错误信息 |
| 执行 | 向量化批量(4096行) | 缓存友好、摊薄虚函数开销 |
| 写入 | Append-only | 简化并发、避免写放大 |
| I/O | mmap 读密封段 + 直接 I/O 写 | 读路径零拷贝，写路径精确控制 |
| 排序 | 可选 SORT KEY | 时序场景 10-100x 加速，通用场景不强制 |
| 执行 | 先 Pull 后 Push | Pull 快速交付 MVP，Push 后续迁移热路径 |

---

## 0. 从 kdb+ / DuckDB 借鉴的关键设计

### 0.1 可选排序表 (SORT KEY)

借鉴 kdb+ 的核心优化：物理排序的表在范围扫描和压缩上碾压无序列。

```sql
-- 有序表：时序场景性能拉满
CREATE TABLE trades (
    ts    TIMESTAMP,
    sym   VARCHAR,
    price FLOAT64
) SORT KEY (ts, sym);

-- 无序列：普通场景，简单追加
CREATE TABLE logs (
    msg VARCHAR
);  -- 无 SORT KEY，纯追加
```

- 有序行的 .dat 文件：范围查询退化为顺序读 + Delta编码压缩率极高
- 插入时二分查找位置（O(log n)），对于 < 10万行/秒的写入速率可接受
- 可选，不强制，通用表不受影响

### 0.2 分阶段执行模型：先 Pull 后 Push

DuckDB 的 Push 模型 CPU 效率极高（无虚函数调用、I-cache 常驻、SIMD友好），但调试困难。

| 阶段 | 执行模型 | 说明 |
|------|---------|------|
| Phase 1-3 (MVP) | **Pull 模型** | `fn next_batch() -> Option<Batch>`，每个算子独立，易于调试 |
| Phase 4+ | **Push 模型（热路径）** | Scan→Filter→Project 管道化，模板展开（非 LLVM JIT），保持可调试 |

两套 trait 共存，查询计划时按子计划选择执行方式：
```rust
trait PullOp  { fn next(&mut self) -> Result<Option<Batch>>; }
trait PushOp  { fn push(&mut self, batch: Batch) -> Result<()>; }
```

**不用 LLVM JIT**：模板化的 Push 管道（Rust 编译期单态化）可达 JIT 80% 的性能，无 LLVM 依赖和调试困难。

### 0.3 mmap 读 + 直接 I/O 写

借鉴 kdb+ 的 mmap 策略：

| 操作 | I/O 方式 | 原因 |
|------|---------|------|
| 读已密封 .dat 文件 | **mmap** | 不可变、顺序访问、OS page cache 自动共享、零拷贝 |
| 写 WAL | **write() + fsync()** | 精确控制持久化时机 |
| 写 .dat 文件 | **write()** | 控制写入顺序和时机 |
| 读小元数据 | **pread()** | 64字节不值得建 mmap 映射 |

```rust
enum SegReader {
    Mmap(memmap2::Mmap),   // 首选，密封段不可变
    Direct(File),           // 回退（32位地址空间不足等）
}
```

### 0.4 可选时间分区 (PARTITION BY)

kdb+ 强制按日期分区，对通用数据库太死板。做成可选的：

```sql
CREATE TABLE trades (...) 
PARTITION BY DAY(ts)     -- 按天分区，可选
SORT KEY (ts, sym);      -- 分区内排序

-- 分区文件命名（对齐 taibai）
-- 所有分段文件平铺在同一表目录下，分区键编码在文件名中
trades/
  seg_00001.dat           -- 非分区表使用 seg_ 前缀 + 5位序号
  seg_00002.dat
trades/                   -- 分区表使用 p{partitionKey}_ 前缀 + 4位序号
  p20260501_0001.dat      -- 2026-05-01 分区的第1段
  p20260501_0002.dat      -- 2026-05-01 分区的第2段
  p20260502_0001.dat      -- 2026-05-02 分区的第1段
```

**文件命名规则**（对齐 taibai 实现）:

| 类型 | 命名模式 | 示例 |
|------|---------|------|
| 非分区 | `seg_XXXXX.dat` (5位零填充序号，从1开始) | `seg_00001.dat`, `seg_00002.dat` |
| 分区 | `p{partitionKey}_XXXX.dat` (4位零填充序号，从1开始) | `p20260501_0001.dat`, `p2024_0003.dat` |
| 临时(压缩) | `seg_{ms}.dat.tmp` / `p{key}_{ms}.dat.tmp` | `seg_1712345678.dat.tmp` |
| 临时(删除) | `{original}.delete.tmp` | `seg_00001.dat.delete.tmp` |

- 分区键由分区函数计算得出（如 `DAY(ts)` → `20260501`，`YEAR(ts)` → `2026`）
- 所有分段文件平铺在同一表目录下，不使用日期层级子目录
- 按时间范围的查询通过文件名前缀匹配修剪分区
- 旧分区可独立压缩、备份、删除
- 不指定 PARTITION BY 的表使用 `seg_` 前缀的平铺文件

### 0.5 Morsel 并行 (Phase 3+)

借鉴 DuckDB 的 morsel-driven 模型而不是 kdb+ 的分区间并行：

- 每线程取一个 morsel（小块数据），跑完整管道，取下一个
- 工作窃取自然负载均衡
- 单表查询也能并行（不是必须跨分区）

---

## 1. 项目结构

单 crate 模块化组织，按功能分模块目录，简单直接：

```
taibai/
├── Cargo.toml
├── src/
│   ├── main.rs              # 入口: 组装所有子系统
│   ├── common/               # 基础: error, config, arena
│   │   ├── mod.rs
│   │   ├── error.rs
│   │   ├── config.rs
│   │   └── arena.rs
│   ├── types/                # 类型系统: ColumnArray, RecordBatch, NullBitmap
│   │   ├── mod.rs
│   │   ├── scalar.rs
│   │   ├── array.rs
│   │   ├── batch.rs
│   │   └── bitmap.rs
│   ├── protocol/             # 协议层: 帧编解码, 消息类型, 结果格式化
│   │   ├── mod.rs
│   │   ├── codec.rs           # 7字节帧 Framed codec (Encoder/Decoder)
│   │   ├── message.rs         # 所有 MsgType 常量, MessageHeader 结构
│   │   ├── types.rs           # DataType + StorageType 枚举定义
│   │   ├── result.rs          # Text / Binary / ZCPY 三种结果集序列化
│   │   ├── encoding.rs        # Key-Value 子协议 (Prepare/Execute/CloseStmt)
│   │   ├── bulk.rs            # BulkInsert 系列消息编解码
│   │   └── compress.rs        # 压缩协议 (gzip/zlib 检测与解压)
│   ├── storage/              # 存储引擎: Segment, WAL, Schema
│   │   ├── mod.rs
│   │   ├── schema.rs
│   │   ├── segment.rs        # SegmentWriter/SegmentReader + .dat
│   │   ├── wal.rs
│   │   ├── encoding.rs
│   │   ├── sort_key.rs
│   │   ├── partition.rs
│   │   └── buffer_pool.rs
│   ├── sql/                  # SQL引擎: Parser, Planner, Optimizer, Executor
│   │   ├── mod.rs
│   │   ├── parser/
│   │   ├── planner/
│   │   ├── optimizer/
│   │   └── executor/
│   └── net/                  # 网络层: TcpListener, Session, Handler, TLS
│       ├── mod.rs
│       ├── server.rs
│       ├── session.rs
│       ├── handler.rs
│       └── tls.rs
└── benches/
    └── scan_bench.rs
```

---

## 2. 存储引擎

### 磁盘布局

每张表一个目录，数据按分段文件组织。全局一个 `schema.json` 管理所有表定义：

```
data/mydb/
├── schema.json              # 所有表定义（唯一）
├── wal/
│   ├── 000000.wal           # 当前WAL (顺序写)
│   └── 000001.wal           # 轮转后新文件
├── users/                   # 非分区表
│   ├── seg_00001.dat        # 第1段 (< 2GB)
│   ├── seg_00002.dat        # 第2段 (< 2GB)
│   └── ...
├── trades/                  # 分区表 (PARTITION BY DAY(ts))
│   ├── p20260501_0001.dat   # 2026-05-01 分区第1段
│   ├── p20260501_0002.dat   # 2026-05-01 分区第2段
│   ├── p20260502_0001.dat   # 2026-05-02 分区第1段
│   └── ...
└── ...
```

**schema.json 格式**:
```json
{
  "tables": {
    "users": {
      "columns": [
        {"name": "id",       "type": "INT64"},
        {"name": "name",     "type": "VARCHAR"},
        {"name": "score",    "type": "FLOAT64"},
        {"name": "amount",   "type": "DECIMAL", "precision": 12, "scale": 4}
      ],
      "created_at": 1712345678000000
    },
    "trades": { ... }
  }
}
```

- 添加表时写入 schema.json，删除表时移除，修改表时更新
- 启动时一次性加载到内存，运行时保持内存缓存
- WAL 和 schema.json 在同一级，表数据在子目录

### WAL 预写日志 (Write-Ahead Log)

所有写入先落 WAL，再进内存缓冲。保证崩溃恢复时数据不丢失。

**WAL 文件格式**：顺序追加的条目流，每个条目包含一批原始列数据：

```
┌─ WAL Entry ──────────────────────────────────┐
│  crc: u32 LE (CRC32，从 table_id 开始)        │
│  entry_len: u32 LE (本条目总字节数)            │
│  table_id: u64 LE                            │
│  segment_id: u64 LE (目标段序号)              │
│  row_count: u32 LE                           │
│  column_count: u16 LE                        │
│  columns: [                                  │
│    data_type: u8                             │
│    data_len: u32 LE                          │
│    raw_bytes: [u8; data_len]                 │
│  ] × column_count                            │
└──────────────────────────────────────────────┘
```

**写入顺序**（严格）：
1. 将列数据序列化为 WAL entry → `fsync` WAL 文件
2. 将数据追加到内存中的列缓冲区
3. 当缓冲区满块（65536行）→ 编码 → Zstd → 写入 .dat 文件

**崩溃恢复**：
1. 启动时检查每个表的最后密封段序号
2. 扫描 WAL 文件中 `segment_id > 最后密封段` 的所有条目
3. 按序重放到 SegmentWriter，重建内存缓冲区和未完成块
4. 截断已密封段对应的旧 WAL 文件

**WAL 清理**：当某个段完成密封（Footer 写入 + fsync + Header 回填），该段对应的 WAL 文件即可删除。可定期轮转 WAL 并清理旧文件。

### 分段文件格式 (.dat)

每段包含**所有列**的列式数据，元数据集中在 Footer。写入时顺序追加列块，密封时写 Footer + 回填 Header。

```
┌─ File Header (64B, offset 0) ────────────────────┐
│  magic: [u8;4] = b"SEG\x01"                      │
│  segment_id: u64 LE (等于文件序号)                 │
│  row_start: u64 LE (全局行偏移)                    │
│  row_count: u32 LE                                │
│  column_count: u16 LE                             │
│  footer_offset: u64 LE (Footer起始位置)            │
│  created_at: i64 LE (epoch微秒)                    │
│  reserved: [u8; 26]                               │
├─ ColumnChunk 0 (列0的全部数据块) ──────────────────┤
│  ┌─ Block 0 (默认65536行) ────────────────────┐   │
│  │  null_bitmap: ceil(rows/8) bytes           │   │
│  │  encoded + zstd compressed values          │   │
│  ├─ Block 1 ──────────────────────────────────┤   │
│  │  ...                                       │   │
│  └────────────────────────────────────────────┘   │
├─ ColumnChunk 1 ───────────────────────────────────┤
│  [列1的全部数据块...]                               │
├─ ...                                              │
├─ Segment Footer (最后写入, 含所有索引信息) ─────────┤
│  magic: [u8;4] = b"FOOT"                          │
│  column_offsets: [u64 LE; N] (每列Chunk的文件偏移)  │
│  per-column block index:                          │
│    data_type: u8                                  │
│    compression: u8                                │
│    encoding: u8                                   │
│    decimal_meta: (precision: u8, scale: u8)       │
│    block_count: u16 LE                            │
│    blocks: [(offset: u32 LE, row_count: u16,      │
│              null_count: u16, min_val, max_val)]  │
│  checksum: u32 LE (CRC32)                         │
│  footer_len: u32 LE                               │
└──────────────────────────────────────────────────┘
```

**关键设计点**:
- Footer 后写，包含每列的文件偏移和块级 min/max 索引
- Header 的 `footer_offset` 在密封时回填
- 读的时候：读Header(64B)→跳到Footer→拿到列偏移和块索引→按需读列
- 列内按块组织（默认 65536 行/块），块内有 null bitmap + 编码压缩数据
- 每段行数 = 该段所有列的行数（必须一致）

### 编码策略（按类型自动选择）

| 数据类型 | 默认编码 | 备选 |
|---------|---------|------|
| INT64 (单调/时序) | Delta-of-delta | Delta / RLE |
| INT64 (低基数) | Dictionary | Bit-packed |
| FLOAT32/64 | Gorilla XOR（与 zstd(Plain) 按块竞争） | Plain + Zstd |
| VARCHAR | Dictionary + Zstd | Plain + Zstd |
| BOOL | Bit-packed | — |
| TIMESTAMP | Delta-of-delta | Delta / RLE |
| DECIMAL (i64) | Gorilla XOR / Delta（与 zstd(Plain) 按块竞争） | Plain + Zstd |
| VECTOR | Plain | — |

**专用编码与 zstd 的联动（按块实测决策）**:

- `MXE1` 自适应块中，GorillaXor（Float32/64/Decimal，Gorilla 位级 XOR，前导/尾随零窗口复用）和
  DeltaOfDelta（单调整数，zigzag varint 的 dod 流）在写入时与 `zstd(Plain)` 候选按实际字节数竞争，
  取更小者存储——不串联（位级紧凑流之上 zstd 几乎无收益且白付 CPU）。
- 实测基准（10 万值/块，见 `column_codec_test.rs::adaptive_encoding_compression_bench`）：
  两位小数 tick 价格 f64/Decimal 上 zstd(Plain) 更优（约 55x/143x），XOR 落选；
  100ms 固定间隔时间戳 dod 裸存更优（约 7.1x vs 5.4x，解码约 2 倍快）。
- 每块以明文首值开头，编码状态（前值、前导/尾随窗口）不跨块——保随机访问、崩溃恢复和
  按行投影语义。
- XOR/dod 适用于 ≥128 个非空值的块；谓词下推路径（predicate_scan）对两种编码提供
  count/eq 快路径，行级过滤先解压为列数组后再走 SIMD 布局（fastscan 不感知编码）。

**磁盘格式兼容性**:

- 列块头部 `MXE1` 后的类型字节新增实际写入值：`4` = DeltaOfDelta，`6` = GorillaXor
  （枚举早已预留，此前从不写出）。旧版本二进制无法解码这两种块——升级部署时需整体换代。
- 读取端按块探测 zstd frame magic（`28 B5 2F FD`）：命中则解压，否则视为未压缩的自适应块。
  旧数据全部为 zstd 帧，读取零回退；行组内混存两种块也安全（块级自描述）。

### 写路径 (WAL + Append-Only)

```
INSERT INTO users VALUES (1, 'Alice', 3.14)
         │
         ▼
┌──────────────────────────────────────────────┐
│ 1. 解析值，按列拆分                           │
│ 2. 序列化为列式数据                           │
│ 3. ═══ 写 WAL ═══                           │
│    a. 构造 WAL entry (table_id + raw cols)   │
│    b. 追加写入当前 .wal 文件                  │
│    c. fsync WAL  → 数据已持久化              │
│ 4. 追加到当前活跃段的内存缓冲区 (per-column)    │
│ 5. 缓冲区达到 65536 行时:                     │
│    a. 按列独立编码 (dod/Delta/RLE/Dict/XOR)   │
│    b. 专用编码与 zstd(Plain) 按块竞争取小存储  │
│    c. 顺序追加写入当前 .dat 文件              │
│ 6. 当前 .dat 达到 2GB 时:                    │
│    a. 写 Footer (列偏移 + 块索引 + checksum)  │
│    b. 回填 Header.footer_offset              │
│    c. fsync .dat → 密封完成                   │
│    d. 创建新段 (序号+1)                       │
│    e. 标记旧段对应的 WAL 可清理               │
│ 7. 也可手动 FLUSH TABLE 密封当前段             │
└──────────────────────────────────────────────┘
```

### 读路径 (列裁剪 + 谓词下推)

```
SELECT id, name FROM users WHERE score > 0.5
         │
         ▼
┌──────────────────────────────────────────────┐
│ 1. 读 Schema → 知道 score 是 FLOAT64         │
│ 2. 遍历所有 .dat 文件:                        │
│    a. 读 Header → 拿到 footer_offset          │
│    b. 读 Footer → 拿到 id/name/score 列偏移   │
│    c. 检查 score 的块级 min/max 索引:         │
│       - 跳过 max(score) ≤ 0.5 的块           │
│       - 只读符合条件的块                      │
│    d. 对 score 块: 解压→解码→过滤→选择向量    │
│    e. 用选择向量从 id/name 列读对应行          │
│    f. 填充 RecordBatch (4096行) → yield       │
│ 3. 不需要的列 (如 email) 完全不读             │
└──────────────────────────────────────────────┘
```

**零拷贝**: 解压后的 `Bytes` 直接作为 RecordBatch 的底层存储，不复制。

### 二进制批量导入

绕过 SQL 解析，客户端直接发送列式二进制数据，实现极限写入速度。新增协议消息 `MsgTypeBulkInsert = 0x20`。

**消息格式**：

```
table_name_len: u32 LE
table_name: UTF-8
column_count: u16 LE
for each column:
  name_len: u16 LE
  name: UTF-8
row_count: u32 LE
for each column (same order):
  data_type: u8
  null_bitmap: ceil(row_count/8) bytes
  values: typed array (如 INT64 = [i64 LE; non_null_count])
```

**处理流程**（极速路径，零解析开销）：

```
Client                                   Server
  │                                       │
  │── MsgTypeBulkInsert ────────────────▶│
  │   (列式二进制数据)                    │ 1. 校验 Schema（列名+类型匹配）
  │                                       │ 2. 写入 WAL（整批落盘，一次 fsync）
  │                                       │ 3. 按列直接移入 SegmentWriter 缓冲区
  │                                       │ 4. 返回 OK(affected_rows)
  │◀── OK(n) ───────────────────────────│
```

对比 SQL INSERT 路径的优势：
- 无需 SQL 文本解析（省 CPU）
- 无需逐行类型转换验证（列已预拆分）
- 列缓冲区可直接 move 而非逐行拷贝
- 大批次可一次 WAL fsync（而非每行或每批）

**批量写入优化**：
- 大批量（1万+ 行）直接触发整批 WAL 写入 + 一次 fsync
- 如果一批数据足够填满存储块（65536行），跳过内存缓冲直接编码压缩写入
- 支持多连接并发批量导入到不同表（表级锁）

---

## 3. SQL 引擎

### Parser

- 手写递归下降解析器（不用 parser generator）
- Token: SELECT, FROM, WHERE, AND, OR, NOT, GROUP BY, ORDER BY, ASC, DESC, LIMIT, OFFSET, INSERT, INTO, VALUES, CREATE, TABLE, DROP, SHOW, DATABASES, TABLES, DISTINCT, HAVING, LIKE, BETWEEN, IN, IS, NULL, JOIN, LEFT, RIGHT, ON, BEGIN, COMMIT, ROLLBACK, SORT, KEY, PARTITION, BY, DAY
- 表达式: 比较(=,<,>,!=,>=,<=)、算术(+,-,*,/)、IS NULL、IS NOT NULL、BETWEEN、IN、NOT IN、LIKE、聚合函数(COUNT/SUM/AVG/MIN/MAX)

### Planner & Optimizer

LogicalPlan 节点: Scan → Filter → Project → Aggregate → Sort → Limit → Insert

优化规则（顺序执行）:
1. **谓词下推**: WHERE → Scan 节点
2. **列裁剪**: 移除不需要的列
3. **常量折叠**: 编译时计算常量子表达式
4. **Limit下推**: LIMIT → Scan（提前终止读取）

### Executor

向量化批量执行：
```rust
trait Executor {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>>;
}
```

| 算子 | 策略 |
|------|------|
| Scan | 读 .dat 分段文件，Footer索引做谓词裁剪和列裁剪，生产批次 |
| Filter | 逐行求值表达式，生成选择向量 |
| Project | 对选中行求值表达式 |
| HashAggregate | 单趟哈希表聚集 |
| Sort | 外部排序（超内存预算时溢写到临时文件） |
| Limit | 计数截断 |

### MVP SQL 支持

```
✅ SELECT col1, col2 FROM table WHERE ...
✅ SELECT expr                    -- 无 FROM 表达式求值 (如 SELECT 1)
✅ WHERE ... AND/OR/NOT, IS NULL, IS NOT NULL
✅ WHERE ... BETWEEN, IN, NOT IN, LIKE
✅ GROUP BY + COUNT/SUM/AVG/MIN/MAX
✅ HAVING                         -- GROUP BY 后过滤
✅ ORDER BY ASC/DESC
✅ LIMIT n OFFSET m
✅ DISTINCT                       -- SELECT DISTINCT col FROM t
✅ INSERT INTO table VALUES (...)
✅ CREATE TABLE name (col TYPE, ...)              -- 普通表
✅ CREATE TABLE ... SORT KEY (col, ...)           -- 有序表 (Phase 2)
✅ CREATE TABLE ... PARTITION BY DAY(col)         -- 时间分区 (Phase 2)
✅ DROP TABLE
✅ SHOW DATABASES / SHOW TABLES                   -- 元数据查询
✅ BEGIN / COMMIT / ROLLBACK                      -- 返回 OK (暂时无 ACID)
❌ JOIN / LEFT JOIN / RIGHT JOIN (Phase 3)
❌ UPDATE / DELETE (Phase 4)
❌ 真正的事务 MVCC (Phase 4)
```

---

## 4. 网络层

### TCP Server

- `tokio::net::TcpListener` 监听 5151 端口
- `tokio-util::Framed` + 自定义 `MessageCodec` 实现 7字节头编解码
- 每个连接 spawn 独立 task

### 协议消息流

```
Client                              Server
  │── Login(user,pass,db) ────────▶ 验证→AuthOK(session_id)
  │── Query(SQL text) ───────────▶  SQL引擎→Result
  │── BulkInsert(cols,rows) ─────▶  WAL→SegmentWriter→OK(n)
  │── Ping ──────────────────────▶  Pong
  │── Prepare(sql) ──────────────▶  编译→stmt_id
  │── Execute(stmt_id, params) ──▶  执行→Result
  │── CloseStmt(stmt_id) ────────▶  释放
  │── Logout ────────────────────▶  清理→关闭TCP
```

### Auth & Session

```rust
struct Session {
    id: Uuid,
    database: String,
    username: String,
    created_at: Instant,
    last_activity: Instant,
    prepared_stmts: HashMap<u64, PreparedStatement>,
}
```

心跳: 客户端每 15s 发送 Ping（ReadTimeout/2），服务端及时响应 Pong。服务端不主动断开空闲连接，容忍度 > 60s。

### 完整协议消息类型 (对齐 taibai-go)

| 类型 | 值 | 方向 | 说明 |
|------|-----|------|------|
| `MsgTypeQuery` | 0x01 | C→S | SQL 文本查询 |
| `MsgTypeResult` | 0x02 | S→C | 查询结果（文本或二进制） |
| `MsgTypeError` | 0x03 | S→C | 错误响应 |
| `MsgTypePing` | 0x04 | C→S | 心跳请求 |
| `MsgTypePong` | 0x05 | S→C | 心跳响应 |
| `MsgTypeInsert` | 0x06 | C→S | 保留 |
| `MsgTypeOK` | 0x07 | S→C | 成功响应(8字节 u64 LE rows_affected) |
| `MsgTypeLogin` | 0x08 | C→S | 登录请求 |
| `MsgTypeLogout` | 0x09 | C→S | 登出请求 |
| `MsgTypeAuthOK` | 0x0A | S→C | 认证成功(session_id) |
| `MsgTypeAuthFail` | 0x0B | S→C | 认证失败 |
| `MsgTypeBulkInsert` | 0x20 | C→S | 批量插入(binary) |
| `MsgTypeBulkInsertAsync` | 0x21 | C→S | 异步批量插入 |
| `MsgTypeBulkInsertAck` | 0x22 | S→C | 批量插入确认 |
| `MsgTypeBulkInsertColumnar` | 0x23 | C→S | 列式批量插入 |
| `MsgTypeBinaryQueryResult` | 0x24 | S→C | 二进制查询结果 |
| `MsgTypeBeginTxn` | 0x30 | C→S | 开启事务(保留) |
| `MsgTypeCommitTxn` | 0x31 | C→S | 提交事务(保留) |
| `MsgTypeRollbackTxn` | 0x32 | C→S | 回滚事务(保留) |
| `MsgTypeCursor` | 0x40 | C→S | 查询执行别名（历史兼容；游标语义从未实现） |
| `MsgTypeCursorFetch` | 0x41 | C→S | 已取消预留：发送方收到 unsupported message type 错误 |
| `MsgTypeCursorClose` | 0x42 | C→S | 已取消预留：发送方收到 unsupported message type 错误 |
| `MsgTypePrepare` | 0x50 | C→S | 预编译语句 |
| `MsgTypeExecute` | 0x51 | C→S | 执行预编译 |
| `MsgTypeCloseStmt` | 0x52 | C→S | 关闭预编译 |

Phase 1 实现: Login/AuthOK/AuthFail, Ping/Pong, Query/Result/Error/OK, Logout
Phase 2 实现: BulkInsert 系列
Phase 4 实现: Prepare/Execute/CloseStmt, BinaryQueryResult
Phase 5 实现: Transaction 系列
0x40 为查询执行别名;0x41/0x42 游标消息已取消预留(2026-09)

### 关键消息 Wire Format（与 taibai-go 精确对齐）

**Login 请求** (MsgTypeLogin = 0x08):
```
username_len: u32 LE
username: UTF-8 bytes
password_len: u32 LE
password: UTF-8 bytes
database_len: u32 LE
database: UTF-8 bytes
```

**AuthOK 响应** (MsgTypeAuthOK = 0x0A):
```
payload: 任意字节 (客户端当作 session_id 字符串)
```

**OK 响应** (MsgTypeOK = 0x07):
```
rows_affected: u64 LE (8 bytes)  ← 必须精确
```

**Error 响应** (MsgTypeError = 0x03):
```
error_message: UTF-8 string (payload 整体)
```
后续可扩展为结构化错误: `code: u32 LE + message`, 参考错误码: Syntax=1001, TableNotFound=1002, ColumnNotFound=1003, DuplicateKey=1004, Constraint=1005, TypeMismatch=1006, PermissionDenied=1007, TransactionError=1008, InternalError=1009

**Ping/Pong** (0x04/0x05): 空 payload

**Text 结果集** (MsgTypeResult = 0x02) — **Phase 1 必须精确实现**:
```
num_columns: u32 LE
for each column:
    name_len: u32 LE
    name: UTF-8 bytes
    data_type: u8       (DataType 枚举值 1-19)
    scale: u8           (DECIMAL 的小数位，其他为 0)
    precision: u8       (DECIMAL 的总位数，其他为 0)
num_rows: u32 LE
for each row:
    for each column:
        null_flag: u8   (0=not null, 1=null)
        if not null: value_bytes (类型相关，见下表)
```

**各类型 Value Wire Format**:
| DataType | Value 编码 |
|----------|-----------|
| BOOL | 1 byte (0/1) |
| INT8 | 1 byte signed |
| INT16 | 2 bytes LE signed |
| INT32 | 4 bytes LE signed |
| INT64 | 8 bytes LE signed |
| UINT8 | 1 byte |
| UINT16 | 2 bytes LE |
| UINT32 | 4 bytes LE |
| UINT64 | 8 bytes LE |
| FLOAT32 | 4 bytes IEEE 754 LE |
| FLOAT64 | 8 bytes IEEE 754 LE |
| DECIMAL | 8 bytes LE (scaled i64) |
| DATE | 4 bytes LE (epoch 起天数, i32) |
| TIME | 8 bytes LE (午夜起微秒, i64) |
| TIMESTAMP | 8 bytes LE (epoch 起微秒, i64) |
| INTERVAL | 8 bytes LE (微秒, i64) |
| VARCHAR | len:u32 LE + UTF-8 data |
| BLOB | len:u32 LE + raw bytes |
| VECTOR | dim:u32 LE + dim*4 bytes (float32 LE[]) |

### Key-Value 编码子协议 (用于 Prepare/Execute/CloseStmt)

Phase 4 实现。Prepared Statement 消息体用键值编码：

```
num_fields: u16 LE
for each field:
    key_len: u16 LE
    key: UTF-8 bytes
    value_type: u8   (0=null, 1=bool, 2=i32, 3=i64, 4=u8, 5=string, 6=bytes, 7=map[string]string, 8=nested_object, 9=object_array)
    value: (按 type 编码)
```

**Prepare 请求**: `{stmt_id: u64, query: string}`
**Prepare 响应**: `{param_count: i32, columns: [{name: string, type: u8}]}`
**Execute 请求**: `{stmt_id: u64, params: [...]}`
**Execute 响应**: MsgTypeBinaryQueryResult 或 MsgTypeOK(rows_affected: i64)
**CloseStmt 请求**: `{stmt_id: u64}`

### 压缩协议 (可选)

客户端可发送压缩消息，服务端 Phase 2 实现解压：

```
comp_type: u8      (0=none, 1=gzip, 2=zlib)
original_size: u32 LE
compressed_data: bytes
```

服务端检测到压缩后先解压再解析内容。

### DSN 连接参数

客户端通过 DSN 传入参数，服务端需处理：
- `taibai://user:password@host:port/database?param=value`
- `database`: 默认 "default"，决定 Login 后的命名空间
- `binary`: 是否请求二进制结果（Phase 4）
- `tls=true/false`: TLS 开关
- `tls_cert`, `tls_key`, `tls_ca`: mTLS 证书路径
- ReadTimeout/WriteTimeout: 客户端侧超时（服务端无需处理）

---

## 5. 类型系统

### 物理类型（19种，对齐 taibai 协议）

| DataType | Wire存储 |
|----------|---------|
| BOOL | 1 byte |
| INT8/16/32/64 | 1/2/4/8 bytes LE |
| UINT8/16/32/64 | 1/2/4/8 bytes LE |
| FLOAT32/64 | 4/8 bytes IEEE 754 |
| DECIMAL(p,s) | 8 或 16 bytes LE (缩放后整数，精度在Schema定义) |
| DATE | 4 bytes LE (距epoch天数) |
| TIME | 8 bytes LE (午夜起微秒数) |
| TIMESTAMP | 8 bytes LE (epoch 起微秒数, UTC) |
| INTERVAL | 8 bytes LE (微秒) |
| VARCHAR | 4 bytes LE 长度 + UTF-8数据 |
| BLOB | 4 bytes LE 长度 + 字节数据 |
| VECTOR | 4 bytes LE 维度 + float32数组 |

### DECIMAL 存储语义

DECIMAL 在 Schema 中定义为 `DECIMAL(precision, scale)`，如 `DECIMAL(12,4)`。底层存储为缩放后的整数：

```rust
// Schema 元数据
struct DecimalMeta {
    precision: u8,   // 总位数, 如12
    scale: u8,       // 小数位数, 如4
}

// 存储: 1234.5678 → i64 = 12345678 (值 × 10^scale)
// 读取: 12345678 ÷ 10^4 → 1234.5678
```

- precision ≤ 18 用 `i64` 存储（8 bytes）
- precision > 18 用 `i128` 存储（16 bytes）
- 算术运算时按 scale 对齐后按整数计算

### Null 处理

Arrow 风格位图：每行1 bit，bit=1 表示 NULL。
注意：Text 协议用 1-byte null_flag (0/1)，与内存位图不同。编解码时需转换。

### StorageType (二进制/ZCPY结果编码)

**与 DataType 是两套独立编号**，二进制结果和 ZCPY 格式使用 StorageType：

| StorageType | 值 | 对应 DataType | Rust 类型 |
|------------|-----|---------------|-----------|
| Int64 | 0 | INT64(5) | `[i64]` |
| Int32 | 1 | INT32(4) | `[i32]` |
| Float64 | 2 | FLOAT64(11) | `[f64]` |
| Float32 | 3 | FLOAT32(10) | `[f32]` |
| String | 4 | VARCHAR(17) | `[u32 LE len + UTF-8]` |
| Bool | 5 | BOOL(1) | `[u8]` |
| Uint64 | 6 | UINT64(9) | `[u64]` |
| Uint32 | 7 | UINT32(8) | `[u32]` |
| Timestamp | 8 | TIMESTAMP(15) | `[i64]` (微秒) |
| Decimal | 18 | DECIMAL(12) | `[i64]` (缩放) |

### ZCPY 零拷贝结果格式 (Phase 3)

用于极限性能场景，客户端直接用 `unsafe` 将字节数组 reinterpret 为类型化数组：

```
Header (13 bytes):
    magic: u32 LE = 0x5A435059 ("ZCPY")
    flags: u8
    row_count: u32 LE
    col_count: u32 LE

Column Metadata × col_count (每列 10 bytes):
    storage_type: u8      (StorageType 枚举值 0-18)
    decimal_scale: u8
    data_offset: u32 LE   (在 data region 内的偏移)
    data_length: u32 LE

Data Region (紧接 metadata):
    每列: [typed values]  (根据 storage_type 解释)
    顺序: Int64=8B/值, Float64=8B/值, String=len+data, 等等
```

**服务端实现要点**: 将 `RecordBatch` 各列直接序列化为连续字节区域，填充 ZCPY header + column meta + data region。

---

## 6. 内存管理

- **BumpAllocator**: `bumpalo` crate，查询级 arena，批次间 reset
- **Batch大小**: 4096 行（L1 cache 友好，~16批/存储块）
- **零拷贝路径**: Protocol→Storage→Execution 全程用 `Bytes` 引用计数

---

## 7. 关键 Rust 依赖

| 用途 | Crate |
|------|-------|
| 异步运行时 | `tokio` + `tokio-util` |
| 内存映射 | `memmap2` |
| 压缩 | `zstd` |
| CRC校验 | `crc32fast` |
| Arena | `bumpalo` |
| 字节缓冲 | `bytes` |
| UUID | `uuid` |
| 日志 | `tracing` + `tracing-subscriber` |
| CLI参数 | `clap` |
| 序列化 | `serde` + `serde_json` |
| 基准测试 | `criterion` |
| TLS (可选) | `tokio-rustls` + `rustls` |

**明确不使用**: `arrow-rs`、`datafusion`、`sqlparser-rs`、`parquet` — 自己从零构建，保持精简和完全控制。

---

## 8. 实现阶段

### Phase 1: 基础骨架
- `common/`: Error, Config, Arena
- `protocol/`: 7字节头编解码, 所有消息类型和数据类型常量
- `net/`: TCP server, session/auth, handler (Ping→Pong, Login→AuthOK)
- `main.rs`: 引导启动
- **验证**: taibai-go 客户端连接、认证、Ping/Pong

### Phase 2: 类型+存储+WAL+普通表
- `types/`: ColumnArray, RecordBatch, NullBitmap, ScalarValue
- `storage/segment.rs`: Segment格式(.dat), SegmentWriter, SegmentReader(优先 mmap)
- `storage/wal.rs`: WAL 写入+fsync, 恢复回放, 清理
- `storage/schema.rs`: 全局 schema.json 管理
- `sql/parser/`: CREATE TABLE, INSERT INTO 语法
- `net/handler.rs`: INSERT, BulkInsert 路由
- `protocol/bulk.rs`: MsgTypeBulkInsert=0x20 消息编解码
- **验证**: CREATE TABLE → INSERT/BulkInsert → WAL恢复 → SELECT * 端到端

### Phase 3: SQL查询 + 有序表 + 时间分区
- `sql/parser/`: 完整 SELECT + SORT KEY + PARTITION BY 语法
- `sql/planner/`: AST → LogicalPlan
- `sql/optimizer/`: 谓词下推、列裁剪
- `sql/executor/`: Pull 模型 Scan, Filter, Project, HashAggregate, Sort, Limit
- `sql/executor/expr.rs`: 表达式求值引擎
- `storage/sort_key.rs`: 排序插入、有序段密封、分区文件管理
- `protocol/result.rs`: Text协议 + ZCPY二进制格式
- **验证**: CREATE TABLE ... SORT KEY → INSERT → 有序范围扫描 → GROUP BY/ORDER BY

### Phase 4: Push执行 + Morsel并行 + 增强
- `sql/executor/push.rs`: Push 模型管道 (Scan→Filter→Project 模板展开)
- `sql/executor/morsel.rs`: Morsel驱动并行 + 工作窃取
- 预编译语句 (MsgTypePrepare/Execute/CloseStmt)
- 高级编码 (Dictionary, Delta-of-Delta)
- TLS 支持, 基准测试, 文档

---

## 9. 验证方式

1. **单元测试**: 每个 crate 内部 #[test]
2. **集成测试**: 用 taibai-go 客户端连接 taibai server，执行完整 CRUD 流程
3. **WAL 恢复测试**: 写入数据中途 kill 进程，重启验证数据完整恢复
4. **批量导入测试**: 100万行二进制 BulkInsert，验证吞吐量和数据正确性
5. **基准测试**: Criterion 测试 Scan 吞吐量、聚合性能、WAL 写入延迟
6. **端到端**: `CREATE TABLE → BulkInsert 100万行 → SELECT WHERE/GROUP BY/ORDER BY` 验证正确性和性能

---

## 10. 关键文件

| 文件 | 职责 |
|------|------|
| `src/protocol/codec.rs` | 7字节帧编解码，所有I/O基础 |
| `src/storage/segment.rs` | .dat 格式定义 + SegReader(mmap优先) + SegWriter |
| `src/storage/wal.rs` | WAL 预写日志：写入/fsync/恢复回放/清理 |
| `src/storage/schema.rs` | 全局 schema.json 管理与缓存 |
| `src/storage/sort_key.rs` | 排序键：二分插入、有序段密封 |
| `src/storage/partition.rs` | 可选时间分区：分区键计算、文件名前缀匹配、分区裁剪 |
| `src/protocol/bulk.rs` | BulkInsert 消息: 列式二进制编解码 |
| `src/types/array.rs` | ColumnArray/RecordBatch，内存表示 |
| `src/sql/executor/mod.rs` | Pull Executor trait + 所有算子 |
| `src/sql/executor/push.rs` | Push 管道执行 (Phase 4) |
| `src/net/handler.rs` | 消息分发: Query/BulkInsert/Prepare 路由 |
