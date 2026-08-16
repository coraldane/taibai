# Durable Tail 升级为可查询 spill 层（惰性恢复 + 内存压力 spill）

> 计划文档。状态：**Phase 1+2 已实施**（含单测，`cargo test --lib` 357 全绿），
> 实施细节见文末「实施记录」。Phase 3（压缩缓存物化层）未实施。
>
> 注：本文档曾因编码事故损坏，已按实施后的最终状态重写。
>
> **2026-08 更新（tail 文件堆积与转储延迟修复）**：
> - 低速率 memtable 的年龄阈值默认从 7 天降为 1 小时
>   （`low_rate_memtable_max_age_secs`），避免低频分区的 tail 文件长期滞留；
> - 已 spill 的 memtable 不再适用低速率放宽年龄——spill 后内存已释放，
>   延迟转储只剩 tail 文件堆积代价，改用常规 `max_age_secs` 判定；
> - 周期 flush 不再被 write_epoch guard 阻断：持续写入期间每
>   `flush_interval_secs`（默认 30s）照常执行 tail→segment 转储；
> - flush/spill 路径改为批量目录扫描（`has_tail_batch` /
>   `live_tail_paths_batch` / `finish_flush_batch`），消除随 tail 文件数
>   二次增长的 `read_dir` 开销；
> - 增加每表 256 个 generation 文件的物理高水位：后台线程每秒检查一次，
>   达到阈值后立即按分区批量转储该表全部 committed、非事务 memtable，
>   不再等待年龄或内存阈值，避免高基数分区把目录持续推到几千个小文件；
> - 修复满 memtable checkpoint 饥饿：`WriteCommand::Flush` 到达时立即批量
>   drain 当前表的全部满 memtable，不再等 512MB+ backlog 或命令队列空闲
>   1 秒。`multi_insert` 的 2470 万行单日行情在 8192 行阈值下会滚动约
>   3016 个 memtable；旧调度在 16 客户端 × 8 pipeline 持续写入时长期等不到
>   queue timeout，因而这些 memtable 各自对应的 tail 文件全部堆积。

## 1. 背景与问题

当前"避免零碎 row group / 小文件"的策略是让未达刷盘阈值的数据长期驻留内存：

- memtable 攒满 `max_rows_per_memtable = row_group_target_rows`（默认 131072）才 flush；
  未满的低频 memtable 年龄阈值放宽到 7 天（`low_rate_max_age_secs`）；
- `ActiveSegmentWriter` 自带缓冲，攒满 `row_group_target_rows` 才切一个 row group；
- 已 ack 的写入由 durable tail（group commit 日志，`memtable_tail/` 目录）保证崩溃
  可恢复，因此常规关停只 flush 无 tail 的 memtable（`flush_untailed_pending`），
  有 tail 的 memtable **整体驻留内存**等待下次启动；
- 重启时 `recover_durable_memtable_tails`（`sql/execution/engine.rs`）调用
  `durable_tail::recover`，把每个 tail 文件**全量解码成 `RecordBatch`**，经
  `MemTableManager::restore_committed_tail` 重建为驻留 memtable——启动内存瞬间回到
  关停前水平。

由此产生两个问题：

1. **常驻内存 = 全库未刷盘数据之和**。表越多、低频分区越多，驻留越多，且随运行
   时间单调增长；
2. **内存高水位的泄压手段与"避免小 row group"目标直接冲突**：worker 在
   `total_estimated_memory_bytes() >= checkpoint_backlog_bytes` 时只能
   `flush_all_pending`——把不满的 memtable 切成小 row group。要么占内存，要么制造
   碎片，没有第三条路。

### 关键事实：内存副本不是正确性依赖

读路径已证实：**committed memtable 的每一行都已持久化在 tail 里**——

- auto-commit INSERT：请求线程先 `mt.append(&batch)`，随后
  `append_memtable_tail` 把该 delta 经 group commit（`TailCommitter`，500µs/16MB/
  128 请求聚合）顺序追加并 fsync；tail 写失败时回退为立即 flush 该 memtable，
  不变式不被破坏；
- 事务提交：`commit_txn_inner` 对每个 committed memtable `persist_memtable_tail`
  写全量快照 tail；
- UPDATE：更新后 `persist_memtable_tail` 重写快照。

因此 memtable 的内存副本**纯粹是查询加速缓存**，丢弃它不损失任何持久性。
这是本方案成立的根基。

## 2. 目标与非目标

目标：

- 未达到刷盘阈值的 committed 数据可以**长期不占内存**，且重启后不自动占内存
  （惰性恢复）；
- 内存高水位的首选泄压手段从"flush 成小 row group"改为"spill 到 tail"
  （**零额外 I/O**，数据本就在 tail 里），小 row group 只在极端兜底时产生；
- 崩溃恢复、flush intent 去重、读己之写、事务可见性语义**完全不变**；
- 可配置、可整体回退（配置关闭即恢复现状）。

非目标（列入后续可选阶段）：

- tail frame 编码改为列存、实现 flush 时字节级拼接（当前 tail frame 是 zstd 压缩的
  WAL 序列化批次，与 segment row group 编码**不**字节兼容）；
- hot store 自身的内存治理（已有独立的 soft/hard limit 与
  `reclaim_persisted_prefix`）；
- `ActiveSegmentWriter` 行组缓冲的 spill（可复用同一抽象，单独评估）。

## 3. 已有基础（本方案依赖的现成机制）

- **durable tail 全量持久化**：`persist` / `append` / `.flush` intent（`begin_flush` /
  `finish_flush`，崩溃中 flush 的去重）/ `.clear` generation / `recover` 的
  generation 仲裁逻辑均已就绪且经测试；
- **tail frame 格式可跳过**：frame = `[u64 payload_len][payload][crc32]`，length
  prefixed，扫描时无需解压即可逐帧跳过；
- **多源合并扫描**：`ScanExecutor::execute_scan_into` 先扫 segments，再由
  `scan_memtables` 合并 memtable；对 memtable 的所有访问都收口在
  `read_snapshot()` / `replace_pending_batch()` / `row_count()` 等少数方法上，
  适合在内部做惰性物化；
- **hot store watermark 不变式**：`scan_memtables` 用
  `watermark = store.row_count() - live_memtable_rows` 避免重复计数。spill 不改变
  memtable 的 `row_count()`，该不变式天然保持；
- **内存计量与高水位触发点**：`MemTableManager::total_estimated_memory_bytes`
  （按列向量实际容量求和）+ worker 循环里的两个检查点（`batch_writer.rs` 周期
  flush 线程、Flush 命令处理），是 spill 调度的现成挂载点；
- **auto-compact 兜底**：碎片化 compact 已上线（日志实证 400 → 97 row group），
  极端情况下少量小 row group 可被后台修复。

## 4. 设计

### 4.1 MemTable 增加 backing store 抽象

`MemTable.batch: RwLock<Option<Arc<RecordBatch>>>` 语义扩展为：

```
enum MemTableBacking {
    Resident(Arc<RecordBatch>),               // 现状
    Spilled { tail_path: PathBuf, rows: u64 }, // 数据在 tail，内存仅元数据
}
```

- `row_count()`：两种状态下都可用（Spilled 用元数据里的 `rows`），**不触发物化**；
- `read()` / `read_with()` / `read_snapshot()`：Resident 直接返回；Spilled 时从
  `tail_path` 解码物化，转为 Resident 后返回。物化走 backing 写锁，与 flush 的
  `Pending → Flushing` 状态迁移互斥（`get_readable_by_table` 已包含 Flushing，
  物化期间查询读到的是同一批数据，无窗口期）；物化失败时记 `poisoned` 并返回
  None/空，不 panic；
- `append()` / `append_owned()`：Spilled 时先物化再追加（见 4.4 的"变热自愈"）；
- `total_estimated_memory_bytes()`：Spilled 只计元数据开销（≈0），不再计入
  batch 容量；
- 事务中（`TxnVisibility::Uncommitted`）的 memtable **不可 spill**：其 tail 快照要
  到 commit 才写，spill 会丢数据。spill 只针对 committed/auto-commit 且有 tail
  的 memtable（`has_tail` 判定逻辑已存在）。

### 4.2 惰性恢复（重启不占内存）

新增 `durable_tail::recover_index(table_dir) -> Vec<RecoveredTailIndex>`：

- generation 仲裁逻辑与 `recover` **完全复用**（`.flush` intent 满足判定、`.clear`
  短路、旧世代清理），区别只在最后一步：不调用 `decode_tail`，而是提取
  `(id, partition_key, tail_path, rows)`；
- `rows` 的获取：tail log 升级为 **v2 格式**（`TAIL_LOG_MAGIC_V2`），frame 头扩展为
  `[u64 payload_len][u64 rows]`，`recover_index` 顺序扫描 frame 头累加行数——
  只读原始字节、不解压、不分配列向量，O(文件大小) 的顺序读；
  - 存量 v1 文件：退回一次性 eager decode（现有 `recover` 路径），解码后补物化为
    Resident——与现状一致，v1 文件随 flush 自然消亡；
  - 当前实现仅接受现行 tail log 格式，不再读取 snapshot tail；
- `engine.rs` 启动恢复改调 `recover_durable_memtable_tail_indices`：对每个
  `RecoveredTailIndex` 调用新的
  `MemTableManager::restore_spilled_tail(id, db, table, partition_key, path, rows)`，
  注册 Spilled 状态的 memtable。**重启后内存占用 ≈ 0，启动时间不再随未刷盘数据量
  线性增长**；
- 恢复出的空 tail（`rows == 0`）沿用现有跳过逻辑。

### 4.3 内存压力下的 spill（替代 flush_all_pending 成为首选泄压）

`BatchWriter` 在现有两处高水位检查点（周期 flush 线程每秒检查、worker 处理
Flush 命令时）接线 `try_spill_cold_memtables()`：

1. 候选筛选：`MemTableManager::spill_candidates(min_age)` —— Pending、非事务、
   Resident、有 durable tail、最老未触摸时间超过 `spill_min_age_secs`（300s）的
   memtable，按最冷优先排序；
2. spill 执行：`spill_memtable_under_table_lock` 在表 mutation lock 下调用
   `MemTable::spill(tail_path)`，把 backing 从 Resident 换成
   `Spilled { tail_path, rows }`，内存立即释放；`live_tail_path` 解析当前活跃
   generation 的文件路径；
3. 顺序：**先 spill 后 flush**——只有 spill 候选耗尽且内存仍超水位时，才走原有
   `flush_all_pending` 兜底；
4. `flush_all_with_retry` 增加物化兜底：flush 前对 Spilled memtable 先
   `read` 物化（flush 需要真实 batch 写 segment），flush 完成后走原有
   `remove_flushed` 清理；
5. 事务防护：2PC 提交路径 `persist_memtable_tail` 前对已 spill 的 memtable 先
   `read` 物化（spill 的 memtable 的 tail 快照已是最新 committed 态，物化后重写
   快照与现状行为一致）。

### 4.4 读路径：惰性物化 + 变热自愈

- 查询扫到 Spilled memtable：`read_with` 内部物化（一次顺序读 + zstd 解压，与
  重启恢复单表开销相当），之后该 memtable 回到 Resident，后续查询零额外开销；
- 写入扫到 Spilled memtable：`append` 先物化再追加——冷分区重新变热时自动回到
  内存，无需显式 unspill；
- DELETE/UPDATE/REPLACE 走 `replace_pending_batch` / 冲突检测，同样经物化入口，
  语义不变；
- 关停路径保持现状：未 spill 的 pending memtable 照常写快照；已 spill 的无需
  处理，重启走惰性恢复。

### 4.5 已知限制（Phase 3 候选）

- tail frame 是 zstd 压缩的 WAL 序列化批次，与 segment row group 编码不字节兼容，
  flush 仍需"解码 → 重编码"；如需零拷贝 flush，需把 tail frame 升级为列存编码
  （单独阶段）；
- spill 后首次查询有一次性物化开销，暂无压缩缓存层/LRU 中间态；必要时可对
  "昨日及更早分区"在空闲期预物化；
- spill/物化振荡由 `spill_min_age_secs` 抑制；如需更精细控制，后续可加
  `spill→materialize` 短间隔重复次数指标。

## 5. 兼容性与回退

- 存量 v1 tail 文件：惰性恢复对其退化为 eager，行为不劣于现状；v1 文件随 flush
  自然消亡；
- 当前实现仅识别现行 magic，不支持不同版本文件混存；
- 可关闭 spill 调度；已落盘 tail 仍按当前唯一格式恢复。

## 6. 测试

新增单测 8 个：

- durable_tail 4 个：v2 frame 免解压行数扫描、`recover_index` 保留 tail 文件且
  行数正确、损坏文件拒绝和当前格式恢复；
- memtable 2 个：spill 后惰性物化数据逐行一致、空/Flushed memtable 拒绝 spill；
- batch_writer 2 个：spill 后读己之写与新写入可见、内存泄压时 spill 优先于
  flush 且 flush 自动物化 Spilled memtable。

回归：`cargo test --lib` 357 全绿（含既有 `dml/visibility`、truncate、
crash recovery、REPLACE 冲突等 memtable/flush 一致性用例）。

待跟进：baochu 回归套件（`dml/visibility.yaml`、`dml/truncate.yaml` 及新增
重启场景：写入未刷盘数据 → spill → 重启服务 → 校验可见性与计数，并断言恢复后
进程 RSS 显著低于数据量）尚未运行。

## 7. 实施记录

- 改动文件：`src/storage/core/durable_tail.rs`（v2 帧格式 `TAIL_LOG_MAGIC_V2`、
  frame 头扩展 `[u64 len][u64 rows]`、免解压行数扫描、`recover_index`、
  `live_tail_path`、`decode_tail` 双 magic 分发）、`src/storage/core/memtable.rs`
  （`MemTableBacking` 抽象、spill/惰性物化/poisoned 标记、内存计量适配、
  `restore_spilled_tail`、`spill_candidates`）、`src/storage/core/batch_writer.rs`
  （`recover_durable_memtable_tail_indices`、`try_spill_cold_memtables`、
  `spill_memtable_under_table_lock`、两处高水位检查点接线、spill 优先于 flush、
  `flush_all_with_retry` 物化兜底、2PC 提交前物化防护）、
  `src/sql/execution/engine.rs`（启动改调惰性恢复入口）。
- 与计划的差异：Phase 3 压缩缓存层未实施（spill 后首次查询为一次性物化开销，
  后续走内存）；`spill_min_age_secs` 暂未配置化，固定 300s；`memtable_spill_enabled`
  配置项未暴露到 yaml，spill 默认开启。
