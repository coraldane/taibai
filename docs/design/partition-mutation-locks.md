# 分区级 mutation lock 设计

本文记录存储层写入互斥从单一表级锁演进为"表级宽锁 + 分区级锁"两级模型的设计动机、协议与边界。行为描述与源码、测试一一对应，可在文末"代码索引"查证。

## 动机

在此之前，同一张表上的所有变更操作（INSERT / UPDATE / DELETE / REPLACE / 事务提交 / flush / compaction / DDL）都串行在 `TableSegmentState::mutation_lock` 这把 **(数据库, 表) 粒度**的锁上。持锁方经常包含慢 I/O：durable tail 组提交 fsync、memtable 落盘、compaction 重写 segment。对按时间分区的大表（如分钟级行情表 `t_stock_minute`），历史分区的一次 compaction 或 flush 会让当前分区的实时写入排队数秒，服务端日志出现大量：

```text
WARN taibai::storage::segment_manager: table mutation lock acquisition was slow table="quant_base:t_stock_minute" wait_ms=4358
```

而实际上这些操作作用于**互不重叠的分区**，本可并行。

## 两级锁模型

### 分区锁（窄锁）

`TableSegmentState` 新增 `partition_locks: RwLock<HashMap<String, Arc<Mutex<()>>>>`，按分区键惰性登记每分区一把互斥锁。单分区操作只获取自己分区的锁：

- 普通 INSERT（`write_columns_with_txn` → `with_partition_mutation_lock`）；
- 非事务提交的 durable tail 发布（`commit_txn_submit_inner`，逐分区加锁）；
- 分区 memtable 的 flush / spill（`BatchWriter` 后台 flush 路径）;
- bulk 导入的单分区追加（`buffered_write.rs` / `direct_bulk.rs` 的分区 owned 路径）。

**不同分区的上述操作互不阻塞**；表级慢操作（compaction、历史分区 flush）不再拖住当前分区的实时写入。

分区锁的获取复用表级锁的超时语义：等待 ≥ `storage.mutation_lock_warn_ms`（默认 1s）打 WARN，≥ `storage.mutation_lock_timeout_ms`（默认 60s）返回 `ResourceBusy`，锁排队同样在日志可见。

### 表级宽锁（gate 协议）

`with_table_mutation_lock` / `with_database_mutation_locks` 保持表级（跨表时按表名排序）互斥，供必须看全表一致状态的操作使用：REPLACE、UPDATE / DELETE 的 segment 重写、compaction 的分区发现与提交、DDL、跨分区 DML。宽锁的获取协议：

1. 获取表 `mutation_lock`（排除其他宽锁持有者）；
2. 置原子标志 `partition_gate = true`；
3. 短暂持有 `partition_locks` **写闸**，对已登记分区按分区名**排序**快照出锁的 `Arc` 列表，随即释放写闸；
4. 依序以**有界等待**锁定全部快照锁（`acquire_partition_lock_for_wide_gate`，复用 `storage.mutation_lock_timeout_ms` 超时语义）：超时返回 `ResourceBusy` 并释放已收集的分区锁与 gate，绝不无限制阻塞；
5. 执行临界区 `f`；
6. 释放分区锁、`partition_gate = false`。

gate 复位由 drop 守卫（`PartitionGateReset` / `DatabaseGatesReset`）完成：无论 `f` 正常返回、返回错误还是 panic 展开，gate 都会被清零，分区写者不会因一次被线程层捕获的 panic 而永久饥饿。

分区写路径在拿到分区锁后**复查 gate 标志**：若宽锁正在收集中（本分区锁已被快照进宽锁持有者的集合，或自己晚于快照创建），写者释放锁并退避重试，直到 gate 清除。该协议保证：

- **宽锁等待者不会饿死**：gate 置位后新到达的分区写者全部自旋等待；
- **写闸不长时间持有**：注册表读路径（新分区创建）不会被宽锁收集中卡死，新分区的写者通过 gate 标志自冻结；
- **无死锁、宽锁收集有界**：分区锁只在快照后按全局排序获取，且收集等待复用 `mutation_lock_timeout_ms` 上限——一个被长持的分区锁（如慢速 bulk 追加）最多让宽锁等待者超时报错，而不是把 gate 顶住同样久、拖垮全表所有分区的写入。宽锁持有者在临界区内不反向等待分区写者持有的其他资源。

### 分区锁内禁止慢操作

分区锁的临界区必须是**内存级或元数据级**操作。任何慢 I/O（decode/sort/segment 重写/fsync）要么移出分区锁，要么像 memtable flush 一样拆成"锁内 freeze、锁外 I/O"两阶段。已落地的两阶段拆分：

- **memtable flush**：锁内只翻转 `Flushing` 并共享 batch（`freeze_memtable_under_partition_lock`），segment 写入在 flush I/O gate 下进行；
- **compaction**：heavy 阶段（解码全部行组、TTL 清理、聚簇排序、写替换 tmp 文件）**不持分区锁**运行，CPU/IO permit 也在取锁**之前**获取（持分区锁睡在 permit 队列上会白白冻结该分区的写入）。提交阶段在分区锁内完成：先以输入文件身份（长度+行数+删除数）校验 heavy 期间无人改动输入，再原子 rename 替换；输入有变则丢弃本次产物，留待下轮收敛。

### 分区子集锁

第二阶段把"实际只触碰部分分区"的宽锁持有方收窄为 `with_partition_set_mutation_lock(db, table, &[partitions], f)`：对显式分区列表排序去重后逐一获取分区锁（复用 gate 感知的 `acquire_partition_mutation_lock`，因此宽锁收集中同样会把子集写者排除在外），集合外分区完全并行。空集合直接执行 `f` 不加锁。

收窄后的调用点：

| 操作 | 锁范围 |
|------|--------|
| `compact_table` 全表 compaction | 宽锁只保护分区发现（毫秒级目录列举）；heavy 重写无锁运行（CPU/IO permit 先于取锁），提交（身份校验+原子替换）只持该分区锁，毫秒级 |
| `optimize_partition` | 分区匹配留在宽锁内（毫秒级），重写同上两阶段 |
| 后台健康统计 `table_physical_health` | 纯读统计，不再持任何 mutation 锁 |
| `ANALYZE TABLE` | 统计收集为只读的 advisory 操作，仅持 flush 可见性读锁，不取宽锁 |
| `DELETE`（分区键等值裁剪命中时） | 单分区子集锁；未命中裁剪仍走宽锁 |
| `UPDATE`（分区键等值裁剪命中时） | 行重写走单分区子集锁；tail 持久化失败的回退 flush 也只冲目标分区（`flush_partition_under_partition_lock`）；锁后的全表唯一索引快照重建仍走宽锁 |
| TTL 过期分区清除 | 过期分区的 memtable drain、active 段 flush/seal 与文件删除都在过期分区集合锁内，文件列表在 drain 之后重新扫描 |
| 内存压力 spill / bulk 组提交 | 按涉事分区集合加子集锁，`active_segments` 注册按组分段持有 |

与宽锁的互斥关系保持不变：宽锁通过 gate + 全量快照仍然排除子集写者；子集与子集之间通过"排序获取 + 成员去重"保证重叠集合串行、无死锁。

### 锁排序

- 跨表：`with_database_mutation_locks` 按 `db:table` 键排序去重后依次获取（原有行为不变）；
- 表内：分区锁按分区名字典序获取；
- 禁止在持有分区锁时获取另一张表的任何锁（与既有 `docs/plans/direct-bulk-unique-index-maintenance-plan.md` 的锁序约束一致）。

## 正确性边界

- **唯一约束原子性不受影响**：check-then-write 的原子性由 SQL 层 `constraint_insert_lock` 覆盖（校验 → 写入 → 索引发布全程），与存储层锁解耦。
- **REPLACE 路径不重入**：SQL 层已持宽锁时，`write_columns_with_txn_under_table_lock` 与 `commit_txn_submit_under_table_lock` 不再获取分区锁（宽锁 gate 已冻结全部分区）；`std::sync::Mutex` 不可重入，重入即自锁，因此该约定由命名后缀 `_under_table_lock` 显式表达。
- **可见性与 flush 语义不变**：memtable 行数 watermark、`MemTableState` 状态机、durable tail 组提交契约均未改动；分区锁只改变"何时允许物理变更"，不改变"何时可见"。
- **retired 表兜底**：分区锁获取前仍执行 `ensure_state_not_retired`，drop database 等路径退役表状态后写者快速失败。

## 已知代价

- 表级一致操作（REPLACE、未命中分区裁剪的 DELETE / UPDATE、DDL、唯一索引快照重建）持有宽锁期间，该表**所有**分区写入停顿——这是全表一致语义的既定代价；命中分区裁剪的 DELETE / UPDATE、compaction 与 ANALYZE 已收窄，不再停顿全表。宽锁收集的等待以 `mutation_lock_timeout_ms` 为上限，单个慢分区不会再把 gate（连同全表写入）顶住无限期；
- 分区数极多的表上，宽锁收集需逐一锁定全部分区锁，代价与分区数线性相关；compaction 的全量快照阶段（毫秒级）也有同样量级的登记成本；`partition_locks` 注册表只增不减，随分区数缓慢增长；
- 同分区并发写者仍然互斥，分区键设计差（如全部写入落入同一分区）无法从该模型获益；
- compaction heavy 阶段不持分区锁后，同一分区的两个并发 compaction 轮次可能都完成重写，提交阶段的输入身份校验会让后到者丢弃产物（正确性不受影响，浪费一轮重写）。

## 测试

并发回归位于 `src/storage/partition_lock_test.rs`（挂载为 `segment_manager::partition_lock_tests`）：

| 测试 | 锁定行为 |
|------|----------|
| `partition_locks_are_independent` | 两个分区可同时各持分区锁 |
| `writes_to_other_partitions_proceed_while_partition_locked` | 分区 A 写入持锁期间，分区 B 的 INSERT 正常完成 |
| `flush_table_avoids_other_partitions_locks` | 表级 flush 不再等待无关分区的分区锁 |
| `wide_gate_waits_for_partition_writers` | 宽锁等待在途分区写者完成后才进入 |
| `wide_gate_collection_is_bounded_while_partition_lock_held` | 宽锁收集在分区锁被长持时按超时返回 `ResourceBusy`，失败收集不影响其他分区 |
| `partition_writers_blocked_while_wide_gate_held` | 宽锁临界区内分区写者被排除 |
| `wide_gate_resets_after_panic_inside_critical_section` | 宽锁临界区内 panic 展开后 gate 被复位，分区写可立即继续 |
| `partition_set_lock_leaves_other_partitions_free` | 子集锁持有期间，集合外分区可立即获取自己的分区锁 |
| `partition_set_lock_waits_for_held_member_partition` | 子集锁等待集合内已被持有的成员分区 |
| `overlapping_partition_sets_serialize_on_shared_members` | 重叠子集通过共享成员分区串行，无死锁 |
| `wide_lock_excludes_partition_set_writers` | 宽锁（gate）排除子集写者，两者互斥 |

引擎级收窄回归位于 `src/sql/execution/engine_test/lifecycle.rs`：

| 测试 | 锁定行为 |
|------|----------|
| `optimize_heavy_phase_keeps_target_partition_writable` | 分区压缩 heavy 阶段（测试注入延迟模拟长重写）期间，**目标分区自身**的 INSERT 不等待，压缩仍正确提交 |
| `optimize_partition_does_not_freeze_other_partitions` | 目标分区被锁住时 OPTIMIZE 该分区阻塞，其他分区 INSERT 不受影响，释放后完成 |
| `delete_partition_eq_does_not_freeze_other_partitions` | 分区键等值 DELETE 只阻塞目标分区，兄弟分区写入并行 |

TTL 回收回归位于 `tests/ttl_purge_test.rs`：

| 测试 | 锁定行为 |
|------|----------|
| `ttl_purge_drains_expired_partition_memtables_before_deleting_files` | 过期分区尚驻留 memtable 的行在分区锁内被 drain 并连同 flush 产物一起删除，后续 flush 不再复活该分区 |

既有 `lock_tests::blocked_table_state_does_not_block_global_registry` 继续覆盖全局注册表不被阻塞表拖垮的行为。

## 代码索引

| 行为 | 位置 |
|------|------|
| 分区锁注册表与 gate 标志 | `src/storage/segment_manager/state.rs`（`TableSegmentState`） |
| 宽锁 gate 协议、分区锁 API | `src/storage/segment_manager/manager_core.rs`（`with_table_mutation_lock` / `with_partition_mutation_lock` / `with_partition_set_mutation_lock` / `with_state_table_mutation_lock`） |
| 有界等待、慢锁 WARN、超时 | 同上（`acquire_mutation_lock` / `acquire_partition_mutation_lock`） |
| 写路径迁移 | `src/storage/core/batch_writer.rs`、`src/storage/segment_manager/buffered_write.rs`、`src/storage/segment_manager/direct_bulk.rs` |
| 宽锁调用点收窄 | `src/storage/core/compactor.rs`（`compact_table` / `optimize_partition`）、`src/sql/execution/dml_delete.rs`、`src/sql/execution/dml_update.rs`、`src/sql/execution/ttl_purge.rs`、`src/sql/execution/engine.rs`（健康统计） |
| 并发回归 | `src/storage/partition_lock_test.rs`、`src/sql/execution/engine_test/lifecycle.rs` |
