# REPLACE 感知 memtable 的冲突删除（消除小 row group）

> 计划文档，描述设计方向。状态：**已实施**（含测试，`cargo test` 全绿），
> 实施差异见文末「实施记录」。

## 1. 背景与问题

写入路径本身是按"攒满一个 row group 才落盘"设计的：

- `ActiveSegmentWriter` 自带缓冲，攒满 `row_group_target_rows`（默认 131072）才切一个 row group；
- memtable 的 flush 阈值对齐 `max_rows_per_memtable = row_group_target_rows`（`engine/core.rs`），
  即 memtable 攒满 131072 行（恰好一个 row group）才 flush；未满的低频 memtable
  （`low_rate_max_rows = 131071`）年龄阈值放宽到 7 天（`low_rate_max_age_secs`）；
- 已 ack 的写入由 durable tail（group commit 日志）保证崩溃可恢复，memtable 长期驻留
  内存不会丢数据；查询路径通过 `scan_memtables` 直接合并 memtable，可见性无延迟；
- 周期 flush 线程每 `flush_interval_secs`（30s）醒来一次，但只要期间有写入
  （`write_epoch` 变化）就跳过，因此持续写入时不会周期性切小 row group。

因此纯 INSERT 路径不会产生小 row group。**小 row group 几乎全部来自 REPLACE 的强制
flush**（DELETE 同理，见 §8）：

`exec_replace`（`sql/execution/dml_insert/write.rs`）目前执行：

1. 语句开头 `engine.flush_table_buffers(db, table)`：把该表所有 Pending memtable
   强制 flush 成 segment 里的一个（通常很小的）row group；
2. 表锁内基于 segment delete bitmap 删除冲突旧行，staged insert 新行（进 memtable），提交；
3. 提交后再次 `flush_table_under_table_lock`：把刚插入的几百行再强制切成一个小 row group。

对"每证券每天一批 REPLACE"的行情 workload（每批约几百行），一次 REPLACE 产生
**两个**几百行的 row group。以 438 万行的 segment 出现 1087 个 row group
（平均约 4000 行/个，目标 131072）为例，碎片率远超 auto-compact 阈值
（`min_row_groups = 32` 且 overhead ≥ 2 倍），触发整表 compact；压缩 64MB segment
时查询超时，形成"小 row group → compact → 查询超时 → 重试 → 更多 REPLACE →
更多小 row group"的恶性循环。

### 为什么 REPLACE 目前必须先 flush

`delete_replace_conflicts_under_table_lock` 的"删旧行"基于 segment 的
delete bitmap + 主键位置索引（`UniqueRowLocation { segment_path, row_id }`），
只能表达 segment 里的行，**看不到 memtable 里的行**。不先 flush，memtable 中的
旧版本行会被漏删，导致主键/唯一键重复。

## 2. 目标与非目标

目标：

- REPLACE 不再触发任何强制 flush；memtable 只在攒满 131072 行（或 7 天空闲、内存
  压力）时 flush，row group 粒度恢复到设计值；
- 保持 REPLACE 的语句级原子性与崩溃语义不劣于现状（见 §6）；
- 不引入查询正确性回退（hot store 前缀不变式、唯一约束校验等）。

非目标（后续可选，见 §8）：

- compact/查询资源的隔离与限流；
- memtable 内的行级 key 索引（加速冲突匹配）。

（DELETE 的 memtable 感知原列为非目标，已在第二轮迭代完成，见 §9.7。）

## 3. 既有基础（本方案依赖的现成机制）

- **UPDATE 已是 memtable 感知**（`dml_update.rs`）：不 flush，表锁内直接用
  `replace_pending_batch` 原地改写 Pending memtable，然后
  `persist_memtable_tail` 重写 durable tail 保证崩溃一致性；tail 写失败时退化为
  flush 该 memtable。本方案对 REPLACE 复用同一模式。
- **唯一约束校验已包含 memtable**：`load_existing_unique_keys` /
  `collect_unique_index_entries`（`dml_insert/constraints.rs`）同时扫描 segment
  与 memtable。
- **位置索引对 memtable 行有定论**：`collect_unique_index_entries` 用
  `segment_path = ""` 表达 memtable 行；`unique_key_locations` 遇到空 path 直接
  返回 `None` → 调用方走 fallback 全扫。即索引里不会残留"指向 memtable 的假位置"。
- **所有写路径都在表 mutation lock 下执行**（`write_columns` →
  `with_table_mutation_lock`），因此 REPLACE 持锁期间不会有并发 writer 修改
  memtable，`replace_pending_batch` 不会因并发 flush 失败。
- DELETE（`exec_delete`）仍然 flush-first，所以即使 REPLACE 不再 flush，DELETE
  也不会漏删 memtable 行。

## 4. 设计

### 4.1 新的 REPLACE 流程

```
exec_replace:
  materialize 输入 batch（不变）
  segment_row_count_watermarks（不变）
  with_table_mutation_lock:
    1. plan = 扫描该表所有 Pending 且 Committed 的 memtable，
       按 key_sets 逐行匹配冲突，产出每个 memtable 的 (kept_batch, removed_batch)
       —— 纯读快照，不做任何修改
    2. 若 plan 非空: hot_store_manager().invalidate(db, table)
       （断开 hot store 前缀不变式，见 §4.3）
    3. 应用 plan: 对每个 memtable replace_pending_batch(kept)
       + persist_memtable_tail；tail 写失败则退化为 flush 该 memtable
       （与 commit_txn_inner 的兜底一致）。记录 undo（removed 行）
    4. delete_replace_conflicts_under_table_lock（segment 侧，逻辑不变）；
       失败时除现有 segment footer 恢复外，追加恢复 memtable undo
    5. staged insert（internal txn，进 memtable，不变）；
       失败时 rollback_replace_under_table_lock 追加恢复 memtable undo
    6. commit_txn_under_table_lock（不变）
  record_inserted_rows / hot_store_manager().remove(...)（不变；
  remove 会清除 invalidated 标记，恢复今日的惰性重建行为）
  append_new_unique_index_locations / finalize_replace_delete
  （row_count 调整计入 memtable 删除行数；tombstone 只记 segment 位置，不变）
```

删除：语句开头的 `flush_table_buffers` 与提交后的 `flush_table_under_table_lock`
两处强制 flush 全部去掉。

### 4.2 memtable 冲突匹配与 undo

- 匹配语义与 segment 路径完全一致：对任一 key_set（主键 + 各唯一索引），行内对应
  列全非 NULL 且 key 在集合中即冲突；多 key_set 之间是 OR。
- 只处理 `get_by_table` 返回的 Pending memtable（与今日 `flush_table` 的作用范围
  完全一致）；跳过 `visibility() != Committed` 的 memtable（表锁下不会出现，防御性
  检查）。Archived memtable 不读也不改（其行已在 segment 中，由 bitmap 删除覆盖，
  与今日行为一致——今日 pre-flush 同样只 flush Pending）。
- undo 记录：按应用顺序记录 `(Arc<MemTable>, removed_batch)`。恢复时逆序
  `memtable.append(&removed_batch)` + `persist_memtable_tail`。恢复后的行进入
  memtable 末尾（顺序变化无所谓，memtable 行本就无序，查询走排序/过滤）。
- `ReplaceDeleteUndo` 增加 `memtable_deleted_rows`，`finalize_replace_delete`
  将其计入 `meta.row_count` 扣减；`deleted_locations`（用于位置索引 tombstone）
  只包含 segment 位置，不变。

### 4.3 hot store 前缀不变式

`scan_memtables` 的 hot-store 快速路径依赖前缀不变式：
`watermark = store.row_count() - live_memtable_rows`，假设 store 的前 watermark 行
已被 flush 并由 segment 提供。**从 memtable 中间物理删除行会破坏该不变式**
（live 行数减少 → watermark 增大 → 可能跳过未 flush 的行，且被删行仍留在 store 里）。

处理：在应用 memtable 删除计划之前 `invalidate(db, table)`（drop store 并禁止惰性
重建，与 UPDATE 原地改写时的处理一致）。持锁窗口内的读者回退到通用 memtable 扫描，
结果正确。提交后沿用现有的 `remove(db, table)`（会清除 invalidated 标记），后续
INSERT 可重新累积 hot store，行为与今日一致。失败路径保持 invalidated（安全，仅
损失加速，直到下一次成功 REPLACE 的 remove 或重启）。

### 4.4 位置索引与后续 REPLACE 的相互影响（flush epoch 机制）

去掉提交后 flush 之后，REPLACE 新插入的行驻留 memtable，位置索引不会立即获得
它们的条目；这些行之后被**后台 flush** 落入 segment 时，索引也不会自动更新。
若不加防护，下一次 REPLACE 同一 key 时 direct 路径会漏掉这批 segment 行 → 主键
重复。引入两层防护：

**A. flush epoch（segment 完备性守卫）**

- `SegmentManager` 维护 per-table 的 `flush_epoch`（内存 + `meta/flush_epochs/`
  小文件持久化，重启可恢复）；每次 memtable flush 成功落盘即 +1。
- `UniqueIndexData` 增加 `flush_epoch: Option<u64>`（serde default = None）：
  重建/persist 时戳上当前 epoch；delta 压实（compaction）时保留原 epoch。
- `unique_key_locations` 在返回前比较 `state.flush_epoch` 与当前 epoch，
  不一致（或 None，含升级前的旧索引文件）→ 返回 None → 走 ensure 重建
  （`collect_unique_index_entries` 全量收集 segment + memtable），重建后索引
  重新完备。每个 index 每次 flush 最多重建一次，摊销代价可忽略。

**B. memtable 伪位置不再"投毒"**

`collect_unique_index_entries` 为 memtable 行生成 `segment_path = ""` 的伪位置；
今天 `unique_key_locations` 遇到空 path 直接返回 None（强制重建）。在 epoch 守卫
+ memtable 冲突删除就位后，segment 完备性由 epoch 保证、memtable 行由 REPLACE
的 memtable 步骤兜底，空 path 条目对"定位"没有价值，直接**跳过**而不是投毒。
这同时消除了"key 驻留 memtable 期间每次 REPLACE 都触发全量重建"的退化循环。
空 path 条目仍服务于 `unique_key_exists`（INSERT 存在性校验），语义不变。

**C. REPLACE 提交后发布唯一索引伪位置（存在性）**

今天 REPLACE 通过 post-commit flush + watermark 派生真实位置来维持唯一索引的
存在性语义。去掉 flush 后改为：提交成功后为插入批的每个唯一索引追加
`segment_path = ""` 的伪位置条目（复用 `build_unique_index_entries_for_batch` /
`append_unique_index_entries_after_insert`，与 plain INSERT 相同）；失败仅
warn + invalidate（降级为全量扫描校验），不让已提交的语句报错。
主键存在性由 `primary_key_indexes` 缓存承担（staged insert 已调用
`append_primary_keys`），PRIMARY_LOCATION_INDEX 无需伪位置。

### 4.5 崩溃语义（与现状对比）

现状：crash 发生在"bitmap 删除已落盘、staged insert 未提交"之间 → 恢复后旧行已删、
新行回滚，客户端重试 REPLACE 后数据自愈；不会产生重复行。

本方案：memtable 删除先 `persist_memtable_tail` 落盘再改 segment bitmap，
crash 窗口同样是"旧行已删、新行未提交"，恢复后 uncommitted txn 被回滚，客户端
重试自愈；同样不会产生重复行。两者等价。

`persist_memtable_tail` 失败时退化为 flush 该 memtable（罕见 IO 故障路径，可能
产生一个小 row group，可接受），undo 记录仍然有效（恢复时 re-append 进新
memtable，不会与已 flush 内容重复，因为 flush 的是删除后的 kept batch）。

## 5. 正确性论证（漏删/重复的两个方向）

- **不漏删**：REPLACE 的删除范围 = Pending memtable（本方案物理删除）∪ segment
  （bitmap 删除，不变）∪ Archived memtable（不读不改，与今日一致）。与今日
  "flush Pending → 全量 bitmap 删除"覆盖的行集合完全相同。
- **不产生重复**：新行进 memtable 前，同 key 的 memtable 旧行已物理删除、segment
  旧行已 bitmap 删除；唯一约束校验（含 memtable 扫描）在 staged insert 时兜底。
- **读一致性**：读者要么看到替换前状态（旧 memtable 快照 + 旧 bitmap），要么看到
  替换后状态；中间窗口与今日 segment bitmap 逐段重写时的窗口性质相同。
  hot store 窗口由 §4.3 的 invalidate 关闭。

## 6. 影响面

| 路径 | 影响 |
|---|---|
| REPLACE | 零强制 flush；新增一次 memtable 全量扫描（内存操作，按 key 匹配） |
| INSERT | 不变 |
| UPDATE | 不变（已是 memtable 感知） |
| DELETE | 零强制 flush（第二轮迭代完成，见 §9.7）；memtable 行原地物理删除 |
| SELECT | REPLACE/DELETE 持锁窗口内 hot store 快速路径短暂不可用（invalidate→remove 窗口） |
| 崩溃恢复 | durable tail 重写 + 现有 txn 回滚，语义等价 |
| 优雅重启 | 不再物化有 durable tail 的 memtable（见 §9.8），重启不再切小 row group |
| auto-compact | 小 row group 来源消失，碎片型 compact 应基本不再触发 |

性能预估：对 t_stock_minute 这类表，REPLACE 次数 ≈ 证券数/天（约 5000），每次扫描
的 memtable 最大 131072 行、按 3 列组 key 做哈希匹配，单次约毫秒级，总开销远小于
今日每次 REPLACE 两次 flush（编码 + 写盘 + footer 更新）的 IO 开销。

## 7. 实施步骤

1. `segment_manager`：新增 per-table `flush_epoch`（内存 + `meta/flush_epochs/`
   文件持久化），`note_memtable_flush` / `memtable_flush_epoch`。
2. `batch_writer.rs`：`flush_memtable_inner` 在实际有行且 flush 成功后
   `note_memtable_flush`；新增 `flush_memtable_tail_fallback_under_table_lock`
   （tail 重写失败时的兜底 flush）。
3. `index_manager.rs`：`UniqueIndexData`/`UniqueIndexState` 增加
   `flush_epoch: Option<u64>`；`persist_unique_index_entries` 接收 epoch 参数
   （重建/建索引戳当前值，delta 压实保留原值）；`unique_key_locations`
   增加 epoch 守卫并跳过空 path 伪位置（不再投毒）。
4. `dml_delete.rs`：`ReplaceDeleteUndo` 增加 memtable 删除记录；新增
   `plan_replace_memtable_conflicts` / `apply_replace_memtable_conflicts` /
   `restore_replace_memtable_undo`；`finalize_replace_delete` 计入 memtable
   删除行数。
5. `write.rs::exec_replace`：删除两处强制 flush；锁内接入 memtable 冲突删除
   （apply 前 invalidate hot store）；提交后发布唯一索引伪位置；
   `rollback_replace_under_table_lock` 追加 memtable undo 恢复。
6. 测试：
   - 单测：memtable 冲突匹配（单/多 key_set、NULL key、无冲突、全冲突）；
   - 集成：REPLACE 命中 memtable 旧行（未 flush）→ 查询无重复、旧值被替换；
     REPLACE 后 segment 不产生新 row group（行数 < 阈值时 memtable 不 flush）；
     FORCE_REPLACE_INSERT_FAILURE 回滚后 memtable 旧行恢复；
     REPLACE → 强制 flush → 再 REPLACE 同 key（epoch 守卫触发重建）→ 无重复；
     REPLACE 插入后 plain INSERT 同唯一键 → 拒绝（伪位置存在性）；
   - 回归：现有 replace/transaction/hot store/unique index 相关测试。
7. `docs/user-guide/limitations.md` 或变更日志中记录行为变化（REPLACE 不再立即
   落盘，行驻留 memtable 直到攒满 row group——与纯 INSERT 一致）。

## 8. 后续可选

- **memtable 内 key 索引**：若 REPLACE 频率继续上升，可在 MemTable 上维护
  主键 → 行号的哈希索引，避免全量扫描。
- **compact 与查询资源隔离**：`global_cpu_permits` / `global_io_permits` 已在
  compact 路径生效，可进一步降低 compact 的 permit 上限或限速。

## 9. 实施记录（与计划的差异）

实施整体按 §4/§7 落地，以下为实际代码与计划描述的出入点：

1. **hot store 处理简化**：计划为「apply 前 `invalidate` + 提交后 `remove`」两段；
   实际在应用 memtable 过滤计划之前直接 `hot_store_manager().remove(db, table)`
   （drop store 并允许后续惰性重建）。持锁窗口内读者回退通用 memtable 扫描，
   语义与计划等价，少一个状态。
2. **唯一索引磁盘格式升级 V4**：`flush_epoch` 不走 serde 默认值，而是在自定义
   二进制格式中新增 `TBIUKV4`（blocks 之后追加 1 字节 flag + 8 字节 epoch）。
   V1–V3 旧文件解码时 `flush_epoch = None`，epoch 守卫视为过期 → 首次探测触发
   一次重建后自动升级。
3. **undo 合并**：memtable 删除的 undo 通过 `ReplaceDeleteUndo::merge` 并入
   既有 undo 结构，恢复复用现有 `restore_replace_delete_under_table_lock` /
   `rollback_replace_under_table_lock` 路径；tail 重写失败的兜底
   `flush_memtable_tail_fallback_under_table_lock` 在恢复路径中按需调用。
4. **伪位置发布**：REPLACE 提交成功后用 `build_unique_index_entries_for_batch`
   （预先从 survivors 批构建）+ `append_unique_index_entries_after_insert`
   发布 `segment_path = ""` 的存在性条目；发布失败仅 warn 并 invalidate 相关
   索引快照。原 watermark 派生真实位置的 `segment_row_count_watermarks` /
   `append_new_unique_index_locations` 已删除。
5. **重启语义注意**：Engine 优雅 shutdown（`flush_shutdown_buffers` →
   `flush_all`）会 flush 所有驻留 memtable 并推进 flush epoch，因此重启后
   关机时有驻留数据的表，其唯一索引 epoch 视为过期，首个 REPLACE/约束校验
   触发一次全量重建（安全、摊销，每个 index 每次 flush 最多一次）。
6. **测试**：新增/更新——
   - `replace_memtable_conflict_removes_old_row_without_flushing`（engine 级
     回归：memtable 冲突原地删除、无 flush、伪位置被探测过滤）；
   - `flush_epoch_mismatch_forces_location_probe_rebuild`（index_manager 单测）；
   - `primary_key_replace_rebuilds_and_maintains_physical_locations` 等旧测试
     按新语义更新（驻留期探测返回空、重启后因 shutdown flush 触发重建）；
   - 唯一索引 V4 roundtrip 测试。
   计划中的 `FORCE_REPLACE_INSERT_FAILURE` memtable undo 恢复用例由既有
   强制失败测试路径覆盖（undo 已合并进统一恢复逻辑）。
7. **DELETE memtable 感知（第二轮迭代）**：`exec_delete` 去掉语句开头的
   `flush_table_buffers` / `flush_partition_buffers`；锁内新增
   `plan_delete_memtable_rows`（WHERE 表达式在 memtable 批上求值，复用
   REPLACE 的两阶段 plan/apply 与 durable tail 重写），有 memtable 删除时
   先 `hot_store_manager().remove`。失败语义与今日 segment 侧一致
   （部分删除已持久化即生效，无整体回滚），因此无需额外 undo。
   `delete_removes_memtable_rows_without_flushing` 回归测试覆盖：原地删除、
   epoch 不变（无 flush）、重启后 tail 恢复不复活已删行、删除后的主键可重新
   INSERT。
8. **优雅重启不再切小 row group（第二轮迭代）**：原 `flush_shutdown_buffers`
   无条件 `flush_all`，每次重启给每张有驻留数据的表切一个小 row group。
   现改为 `flush_untailed_pending`：先 barrier 排空 group commit，再只 flush
   **没有 durable tail** 的 memtable（其行无其他持久副本）；有 tail 的
   memtable 保持驻留，重启后经 `recover_durable_memtable_tails` 恢复。
   这也消除了"重启 → epoch 过期 → 首个 REPLACE 全量重建索引"的连锁代价。
   `graceful_shutdown_keeps_tailed_memtable_resident` 与
   `updated_memtable_is_persisted_by_graceful_restart` 两个测试按新语义更新。
9. **批量 flush 分组合并为单次追加（第三轮迭代）**：原 `flush_all`/`flush_table`/
   `flush_partition`/`flush_table_under_table_lock`/`flush_untailed_pending`/aging
   （`flush_pending_memtables`）/内存压力（`flush_all_pending`）路径逐 memtable
   flush，同表 N 个驻留 memtable 一次批量 flush 会切 N 个小 row group。现统一先按
   (db, table, partition, txn_id) 分组（`group_memtables_for_flush`，保持原有
   时间序），组内用 `concat_flush_group_batches`（`ColumnArray::extend`）合并成
   **一次 segment 追加**，每组只切一个 row group。durable 路径为组内每条 tail
   建一个 flush intent，watermark 用 cumulative 行数（`base + 累计行数`；合并
   追加原子地满足所有 watermark，崩溃时未达 watermark 的 intent 照常留给恢复）；
   无活跃 segment 时走 tmp+rename 的 standalone 新 segment
   （`flush_durable_memtable_group_as_new_segment`，内部按正常 row group 策略
   切分，大批量自动切出多个满 row group）。tailed/untailed 混合组回退逐
   memtable flush 以保持原语义。epoch 打点每组一次（per-table，语义等价）。
   回归测试 `bulk_flush_merges_same_table_memtables_into_one_row_group`：
   INSERT + REPLACE 产生两个 committed memtable，`flush_all_buffers` 后
   segment 只有一个 row group，且重启后 durable intent 清理正确（无重放/丢失）。
   全部 343 lib + 342 bin + 集成测试通过。
