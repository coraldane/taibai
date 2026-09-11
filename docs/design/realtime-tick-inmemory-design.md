# 实时行情当日内存存储设计 (Realtime Tick In-Memory Design)

> 状态注记：经典 WAL 已移除（见 `TAIBAI_DESIGN_ZH.md`「持久化 tail」）。本文中 WAL 阈值/fsync 相关表述为历史设计，现行持久化由 durable tail 与 active segment 承担。
>
> 实现进度（2026-09）：Step 1 性能核心已落地——`ColumnArray::take(&[u32])` 随机行 gather（`src/types/array/analytics.rs`，含 VarChar 无堆克隆路径）、`StringPool` sym 字典化与 `HotStore`/`HotStoreManager`（per-sym 倒排索引、resident coverage、内存软限与 `compact_prefix` 兜底）已实现于 `src/storage/core/hot_store.rs`，查询接线见 `src/sql/execution/scan/memtable.rs`，端到端验证见 `tests/hot_store_fast_path_test.rs`。本文其余部分保留为设计记录，以代码为准。

> 场景：股票 tick 行情，每日约 2000 万行，按时序流式写入。
> 目标：**当天单股票全量 tick 查询（约 5000 行/票）P50 < 1ms、P99 进 1ms**。
> 历史数据查询 < 10ms（已由 PARTITION BY DAY + CLUSTER BY + mmap 覆盖，不在本文重点）。

本设计对齐行业内 kdb+/DolphinDB（时序分区派）与 Pinot/Druid（实时分析派）的成熟实践，并基于 taibai 现有存储引擎做了最小改动落地。

---

## 0. 设计基线（已核实的现状）

下表是动工前必须明确的事实，避免设计建立在错误前提上：

| 事项 | 现状 | 对设计的影响 |
|------|------|-------------|
| MemTable 事务可见性 | `src/storage/core/memtable.rs` 已有 MVCC（`TxnVisibility`/`is_visible_to`） | watermark 一致性可直接复用 |
| MemTable 三态 | `Pending → Flushing → Flushed`，flush 后 `remove_flushed()` 删除 | ⚠️ 当天常驻需新增 `Archived` 态 |
| 自动 flush | `batch_writer.rs::flush_memtable` 用 `take_batch()` 把数据 move 走 | ⚠️ 会清空内存，checkpoint flush 必须改 copy |
| 自动 flush 排序 | **不排序**，走 `bulk_insert_batch_ref`，产出时序 segment | ✅ 盘中 checkpoint 省 CPU，符合预期 |
| CLUSTER BY 排序 | 仅 `OPTIMIZE TABLE`（`compactor.rs`，手动触发）路径排序 | ✅ 收盘归档 = flush all + OPTIMIZE TABLE |
| 列存 `take`（按行号取行） | `array.rs` 仅有 `slice_fast`（连续区间），**无 `take(&[u32])`** | ⚠️ 必须新增，是 gather 路径命脉 |
| VarChar 存储形式 | `Vec<String>` | ⚠️ sym 列需字典化，否则 gather 慢 |
| 结果序列化 | `result.rs` null_bitmap 逐行 push | ⚠️ 需批量化 memcpy |

**核心原则（贯穿全文）**：

> **「刷盘」与「当天数据是否在内存」是正交的两件事。**
> 刷盘解决持久化/恢复/归档；当天查询路径 100% 走内存，对刷盘无感。
> 只有收盘那一刻，内存才整体清场、交棒给历史层。

---

## 1. 总体架构

三层数据组织，对应 kdb+/DolphinDB 的"内存常驻表 + 收盘归档"与 Pinot 的"consuming segment + handoff"：

```
                    当天实时写入（WAL fsync）
                            │
                            ▼
        ┌─── 常驻内存表（TodayHotStore）──────────────────────────┐
        │   · 单张长期存活的 MVCC 内存表（仿 DolphinDB）            │
        │   · per-sym 倒排索引（sym_id → Vec<row_offset>）         │
        │   · 当天查询【只读这一处】← 1ms 保证所在                  │
        │   · 支持双 buffer：flush 期间写不停                       │
        │   · 内存兜底阀门：超上限强制 flush                         │
        └──────────────────────────────────────────────────────────┘
                            │
        ┌─── 三段式刷盘机制 ────────────────────────────────────────┐
        │                                                            │
        │  ① 盘中 checkpoint flush（低频）                           │
        │     触发：WAL 阈值（500MB~1GB）                             │
        │     目的：截断 WAL、缩短崩溃恢复                            │
        │     产物：时序 sealed segment（pre-archive，对查询不可见）  │
        │     ⚠️ 不释放内存，不改查询路径                             │
        │                                                            │
        │  ② 收盘归档 flush（每天一次）                               │
        │     触发：交易日 15:30 调度信号                              │
        │     目的：当天数据正式归档到历史层，释放内存                 │
        │     动作：flush all + OPTIMIZE TABLE（按 sym,ts 排序）      │
        │     产物：CLUSTER BY 排序的历史 segment（查询可见）          │
        │                                                            │
        │  ③ 优雅停机 flush                                          │
        │     触发：SIGTERM / shutdown hook                          │
        │     目的：重启不丢数据                                       │
        └────────────────────────────────────────────────────────────┘
```

### 与行业方案的对应

| taibai 概念 | kdb+ | DolphinDB | Pinot/Druid |
|-------------|------|-----------|-------------|
| TodayHotStore 常驻内存表 | 日内内存表 | MVCC 内存表 | consuming segment |
| per-sym 索引 | sortColumns 加速 | sortColumns | inverted index |
| 盘中 checkpoint flush | 实时日志 -11! | enableTablePersistence | appenderator spill |
| 收盘归档 flush | `.Q.dpft` 收盘批处理 | flush 到 DFS | realtime segment handoff |
| 历史 segment | splayed 分区 | DFS OLAP | historical segment |

---

## 2. 常驻内存表：TodayHotStore

### 2.1 数据布局

当天数据组织成**一张连续的列存表 + per-sym 倒排索引**，而不是"按 sym 分桶成 5000 个小缓冲区"。理由：连续列存 cache 局部性好、写入路径简单；倒排索引解决"找哪 5000 行"的查找问题。

```
TodayHotStore {
    // 主数据：时序追加的连续列存（cache 友好，写入只 append）
    columns: Vec<ColumnArray>,        // ts, sym_id, price, vol, ...（sym 已字典化为 u32）

    // 倒排索引：sym_id → 全局行号列表（append 友好）
    symbol_index: HashMap<u32, Vec<u32>>,

    // sym 字符串驻留（A 股约 5000 个 symbol，命中率 100%）
    interner: StringPool,             // "000001" → u32 id

    // MVCC（复用现有 MemTable 的可见性）
    visibility: TxnVisibility,
}
```

**为什么 sym 列字典化**：原 `ColumnArray::VarChar` 是 `Vec<String>`，gather 5000 行要做 5000 次 `String::clone`（堆访问，cache 差，~100µs）。字典化后 sym 列变成 `Vec<u32>`，gather 退化为定长 index，~5µs。

### 2.2 写入路径（增量维护索引，写放大极小）

```
append(batch):
    base = columns.num_rows()                 // 追加前的基础行号
    1. 将 batch 各列 append 到 columns（含 sym 列，已字典化）
    2. 只扫描【新增行】的 sym 列：
         for (i, sym_id) in batch.sym_col.enumerate():
             symbol_index[sym_id].push(base + i as u32)
```

- 每秒 ~1 万行写入，sym 列扫描 + HashMap 更新 ≈ 几十 µs，对写入吞吐无感。
- 索引只针对【新增行】，不重扫全表——O(新增行数) 而非 O(总行数)。
- 字典驻留：相同 sym 字符串只存一份，比较退化为 u32 比较。

### 2.3 查询路径（1ms 核心）

```
query(sym='000001', [列投影]):
    1. sym_id = interner.lookup("000001")              // ~1µs
    2. rows = symbol_index[sym_id]                      // ~1µs，直接拿到 ~5000 行号
    3. result = columns.take(&rows, projection)         // ~15µs（定长列 SIMD gather）
    4. 返回 RecordBatch（零拷贝引用 columns 的 Bytes）
```

- 没有 clone 整批，没有全列扫描。
- 当天查询**完全不碰磁盘、不碰 mmap**，避开 page fault 尾延迟。

---

## 3. 三大工程点（行业实践必做，设计阶段易漏）

### 3.1 内存兜底阀门（防异常爆内存）

「纯内存」不能真的无上限。异常场景（收盘任务卡住、跨天没切成功、写入速率暴涨）会导致内存持续上涨。

```
TodayHotStore {
    memory_soft_limit: 6GB,        // 软限：单表
    memory_hard_limit: 8GB,        // 硬限：强保护
}

append 时检查:
    if estimated_mem > hard_limit:
        // 安全阀：强制触发 checkpoint flush 并释放最老的已归档区间
        force_checkpoint_flush()
        // 仍超限则拒绝写入，返回压力错误（backpressure）
    elif estimated_mem > soft_limit:
        // 软告警：提前触发 checkpoint flush，压缩 WAL
        schedule_checkpoint_flush()
```

- 借鉴 DolphinDB `enableTablePersistence(cacheSize)` 的 cacheSize 阈值。
- 软限触发低频 checkpoint，硬限触发强保护（拒绝写/强制 flush），二选一避免雪崩。

### 3.2 watermark 一致性（flush 瞬间查询不丢不重）

收盘归档 flush 那一刻，查询正在进来。需要保证：
- flush 开始前的查询 → 只读内存表（照常）
- flush 期间的查询 → 同时读"冻结快照 + 刚落地的历史段"，不丢不重
- flush 完成后的查询 → 只读历史段（内存表已下线）

```
watermark: AtomicU64                  // 当前已稳定归档到的 row_offset

收盘 flush 流程:
    1. freeze_at = current_row_offset             // 冻结点
    2. 将 [0, freeze_at) 的数据 flush 成历史段 + OPTIMIZE TABLE
    3. flush 完成后: watermark.store(freeze_at)
    4. 从内存表移除 [0, freeze_at) 区间

查询合并读:
    读 watermark 当前值 W
    if W < freeze_at && query_range 跨越 W:
        合并读 [内存表 W..freeze_at] ∪ [历史段 0..W]   // 跨界区间双重覆盖，靠 row_offset 去重
    else:
        只读单侧
```

- 复用现有 `MemTable::is_visible_to(reader_txn_id)` 的 MVCC 基础，补一个"冻结快照 row_offset"语义。
- 借鉴 Pinot realtime segment handoff 的 watermark 机制。

### 3.3 双 buffer（flush 期间写不停）

收盘 flush 时要冻结旧数据，但新 tick 仍在写入。若只有一个 buffer，flush 期间写入被阻塞。

```
TodayHotStore {
    active_buffer: ColumnArray buffer,     // 写入中
    frozen_buffer: Option<...>,            // 冻结的待 flush 副本
}

append 路径（始终写 active_buffer）:
    正常: rows → active_buffer
    flush 期间: rows → active_buffer（继续写）

触发收盘归档:
    1. frozen_buffer = active_buffer 的 [0, freeze_at) 冻结快照（引用计数，不 copy）
    2. active_buffer 保留 [freeze_at, ..) 继续接收新写入（写不停）
    3. 后台线程 flush frozen_buffer → 历史 segment + OPTIMIZE
    4. flush 完成后丢弃 frozen_buffer
```

- 借鉴 TDengine vnode 的滚动双 buffer：一块在写、一块在 flush，互不阻塞。
- 关键：frozen_buffer 用引用计数而非深拷贝，避免 flush 触发大内存复制。

---

## 4. 三段式刷盘机制

### 4.1 盘中 checkpoint flush（低频，为恢复打快照）

| 项 | 设定 |
|----|------|
| 触发 | WAL 累积 ≥ 阈值（建议 500MB~1GB，可配）|
| 目的 | 截断 WAL、缩短崩溃恢复时间 |
| 排序 | **不排序**（复用现有 `bulk_insert_batch_ref`，省 CPU）|
| 产物 | 时序 sealed segment，标记 `pre-archive` |
| 查询可见性 | **对当天查询不可见**（当天查询只读 TodayHotStore）|
| 内存影响 | **不释放** TodayHotStore 内存（与现状 take_batch 不同）|

⚠️ **现状改造点**：`flush_memtable` 当前用 `take_batch()` 会清空内存表，对常驻场景致命。checkpoint flush 必须改成 **copy 而非 move**：

```
// 现状（batch_writer.rs:307）—— 清空内存，不可用于 checkpoint
let batch = mt.take_batch();   // move，mt 变空

// 改造后 checkpoint 路径
let batch = mt.read_snapshot();   // clone 副本，mt 数据保留
mt.set_state(MemTableState::Archived);   // 新增态：已持久化但当天仍驻留
// remove_flushed() 当天不删 Archived 态
```

### 4.2 收盘归档 flush（每天一次，正式归档）

| 项 | 设定 |
|----|------|
| 触发 | 交易日 15:30 外部调度信号（收盘后半小时，避尾盘压力）|
| 动作 | flush all + `OPTIMIZE TABLE`（按 `sym, ts` 排序）|
| 排序 | ✅ CLUSTER BY 排序（复用 `compactor.rs` 现有路径）|
| 产物 | CLUSTER BY 排序的历史 segment，标记可查询、移除 `pre-archive` |
| 内存影响 | TodayHotStore [0, freeze_at) 区间释放，整体清场 |

```
close_of_day_flush(table):
    1. freeze_at = current_row_offset
    2. flush [0, freeze_at) 全部数据为历史 segment（时序）
    3. OPTIMIZE TABLE 触发 compactor 按 (sym, ts) 重排 → CLUSTER BY segment
    4. watermark.store(freeze_at)，历史段查询可见
    5. 移除 pre-archive 标记
    6. TodayHotStore 移除 [0, freeze_at)，释放内存
    7. 次日开盘新建 TodayHotStore（或本表 row_offset 重置）
```

- 收盘归档 = 现有 `flush all` + 现有 `OPTIMIZE TABLE` 的组合，**无需新写排序逻辑**。
- 归档后的历史段由 PARTITION BY DAY + CLUSTER BY + mmap 承接，历史查询 < 10ms。

### 4.3 优雅停机 flush

```
shutdown hook (SIGTERM):
    遍历所有 TodayHotStore，强制 flush all 为 pre-archive segment
    WAL fsync
    保证重启后从 checkpoint + WAL 重放，RPO 最小
```

### 4.4 状态机扩展

现有 `MemTableState`：`Pending → Flushing → Flushed`（flush 后 remove）。
新增 `Archived` 态承接"已持久化但当天仍驻留查询"：

```
Pending      活跃可写，当天查询可读
   │ flush（checkpoint，copy 不清空）
   ▼
Archived     已持久化但当天仍驻留，当天查询【仍可读】
   │ 收盘 close_of_day_flush（move + OPTIMIZE）
   ▼
Flushed      历史归档完成，从内存移除，查询转历史段
```

- `get_readable_by_table` 需把 `Archived` 也算进可读集合。
- `remove_flushed()` 当天不删 `Archived`，只在收盘清场后删。

---

## 5. 微秒级预算（1ms 目标分解）

当天单股票 5000 行查询，优化后预算：

| 阶段 | 优化前 | 优化后 | 路径 |
|------|--------|--------|------|
| tokio 调度 + 帧解码 | 100~300µs | 100~150µs | 连接复用 + prepared statement（省 SQL 解析）|
| per-sym 索引查找 | 无 | ~1µs | `HashMap<sym_id, Vec<u32>>` |
| 定长列 gather（ts/price/vol，i64×3）| 全批 clone+过滤 ms 级 | ~15µs | 新增 `ColumnArray::take(&[u32])`，SIMD index gather |
| VarChar gather（sym）| 5000×String clone ~100µs | ~5µs | 字典化，gather 只取 u32 id |
| null bitmap 序列化 | 5 万次 push ~50µs | ~1µs | 批量 memcpy 字节块 |
| 结果序列化 | 逐列 memcpy ~30µs | ~30µs | 已是零拷贝 memcpy，近极限 |
| 网络写回 | 100~300µs | 100~200µs | 二进制协议，避免 Text |
| **合计** | 数 ms~数十 ms | **服务端计算 ~50µs，含网络 ~250~400µs** | P50 进 1ms，P99 靠无 GC 抖动 |

**信心评估**：
- 服务端纯计算 1ms：90% 把握（~50µs，余量巨大）。
- 同机全栈 P50 < 1ms：很有把握（~300µs）。
- 同机全栈 P99 < 1ms：有难度但可行（Rust 无 GC 是天然优势；必要时热查询连接开专属线程避开 tokio 调度抖动）。
- 跨机 1ms：放弃（网络 RTT 是硬墙）。

---

## 6. 代码集成点（最小改动清单）

| 模块 | 改动 | 复用 |
|------|------|------|
| `src/storage/core/memtable.rs` | 新增 `Archived` 态；checkpoint flush 用 `read_snapshot()`（copy）而非 `take_batch()`（move）| MVCC 可见性 |
| 新增 `src/storage/core/today_hot_store.rs` | TodayHotStore + per-sym 倒排索引 + 字典驻留 + 双 buffer + 兜底阀门 | `ColumnArray`、`RecordBatch` |
| `src/types/array.rs` | 新增 `ColumnArray::take(&[u32])`（定长列 SIMD gather）；sym 列字典化 | `slice_fast` |
| `src/sql/execution/scan/memtable.rs` | `scan_memtables` 检测 cluster key eq 谓词时走 TodayHotStore 索引 gather，否则回退现有路径 | — |
| `src/storage/core/batch_writer.rs` | flush 触发改 WAL 阈值驱动（非 30s 定时）；新增 `close_of_day_flush()` | `flush_memtable` |
| `src/storage/core/compactor.rs` | 收盘归档复用现有 CLUSTER BY 排序路径（无改动，仅组合调用）| `get_cluster_key_indices`、`sort_batch_by_key_indices` |
| `src/protocol/result.rs` | null_bitmap 批量化 memcpy；encode 路径 `with_capacity` 预分配 | `append_column_data` 零拷贝 memcpy |
| `src/storage/segment_manager.rs` | 盘中 flush 产物标记 `pre-archive`，当天扫描器跳过；收盘移除标记 | `bulk_insert_batch_ref` |

---

## 7. 落地优先级

分三步走，每步可独立验证：

**Step 1（性能核心，达 1ms）**
- `ColumnArray::take(&[u32])` + sym 列字典化
- TodayHotStore + per-sym 倒排索引
- 改 `scan_memtables` 走索引 gather 路径
- null_bitmap 批量化
- 验证：当天单股票 5000 行查询 < 1ms

**Step 2（持久化正确性）**
- `Archived` 态 + checkpoint flush 改 copy
- WAL 阈值驱动 flush（替代 30s 定时）
- pre-archive 标记 + 当天扫描器跳过
- 验证：盘中 checkpoint 后当天查询仍 < 1ms，崩溃恢复时间可控

**Step 3（健壮性）**
- 内存兜底阀门（软/硬限）
- watermark 一致性（收盘 flush 瞬间查询不丢不重）
- 双 buffer（收盘 flush 期间写不停）
- 收盘归档调度信号（cron / 集合竞价结束）
- 验证：收盘 flush 全程查询不中断、不丢数据、内存按预期释放

---

## 8. 一句话总结

> 当天数据查询路径 100% 走内存（TodayHotStore + per-sym 倒排索引），刷盘与查询解耦：
> 盘中低频 checkpoint flush 只为持久化与恢复（不释放内存、不进查询路径），
> 收盘整体归档 flush all + OPTIMIZE TABLE 交棒历史层。
> 三大工程点（内存兜底阀门、watermark 一致性、双 buffer）保证异常不爆内存、
> flush 瞬间查询不丢不重、flush 期间写不停。
