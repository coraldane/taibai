# 事务与并发语义

本文描述 Taibai 当前版本实际实现的事务与并发行为。文中每一条都与源码和测试对应，可在"代码索引"一节查证；行为如有变更会同步更新本文。

## 事务基本用法

```sql
BEGIN;
INSERT INTO ticks VALUES (...);
UPDATE ticks SET price = 100.5 WHERE symbol = 'AAPL';
COMMIT;   -- 或 ROLLBACK;
```

- 事务绑定在**会话**上：`BEGIN` 后该会话进入事务状态，`COMMIT` / `ROLLBACK` 结束它。
- 不允许嵌套 `BEGIN`；无活动事务时 `COMMIT` / `ROLLBACK` 返回错误。
- 隔离级别只有 **ReadCommitted** 一种，且为默认值。`RepeatableRead` 与 `Serializable` 尚未实现。

## 可见性规则

Taibai 的可见性以 **memtable 为粒度**，不是行级多版本（MVCC）：

- 事务内的 INSERT 会写入该事务专属的事务 memtable（`TxnVisibility::Uncommitted(txn_id)`）。
- 未提交的行**只有发起事务的会话自己能看到**；其他会话（包括非事务查询）完全不可见。
- `COMMIT` 后，事务 memtable 标记为 `Committed`，对所有读者立即可见。
- `ROLLBACK` 后，事务 memtable 标记为 `Aborted`，对所有人（包括原会话）不可见，数据被丢弃。
- 非事务查询的读者标识为 `0`，只能看到已提交数据。

推论：**同一事务内两次相同的 SELECT 可能返回不同结果**（其他会话在两次读取之间提交了新数据）——这是 ReadCommitted 的正常行为，不是缺陷。Taibai 目前不提供事务内快照隔离。

## 提交与持久化

- `COMMIT` 返回成功时，事务数据已写入该表目录下的 durable tail 文件（组提交 + 按配置 fsync），崩溃重启后可恢复。
- 提交流程：标记 memtable 已提交 → 提交 durable tail 追加 → 等待落盘确认。若 tail 写入失败，服务端回退为直接 flush memtable 到 segment，保证提交的数据不丢。
- 非事务写入（自动提交）同样逐批进入 durable tail，语义相同。
- 更细的崩溃恢复行为（CRC 校验、代际仲裁、watermark 防重）见整体设计文档「持久化 tail (durable tail)」一节。

### `tail_async_fsync` 模式下的持久化边界

配置 `tail_async_fsync: true` 后，提交确认与磁盘屏障解耦，换取实时小批次吞吐：

- **ACK 语义变化**：语句返回成功时数据已进入 durable tail 文件的**页缓存**（`write` 完成），但不保证已 fsync。
- **惰性刷盘条件**（满足任一即落盘）：距上次 fsync ≥ 200ms；未刷字节 ≥ 8MB；提交通道持续静默 ≥ `max(5ms, 2×实测 fsync 均价)`（自适应：快盘保持敏捷、慢盘批量更狠，上限 200ms）；收到显式 flush 或有序 shutdown（`Engine::shutdown()`，关停前全量落盘）。
- **崩溃丢失窗口**：进程崩溃（非有序关停）最多丢失最近 200ms 或 8MB 的已确认写入。需要"提交即落盘"保证的场景（资金成交、账务）应保持默认 strict 模式。
- 有序关停（`Engine::shutdown()`，服务端正常退出走此路径）会先同步全部 dirty tail 再退出，此时已确认数据零丢失（回归测试锁定：`concurrent_async_fsync_inserts_are_visible_and_durable_after_shutdown`）。注意：仅调用底层 `SegmentManager::shutdown()` **不构成**有序关停——它不触碰 memtable 与 durable tail，未刷盘的已确认数据依赖后台惰性刷盘兜底。

## 并发写入

- 存储层写入互斥为**两级锁**：单分区操作（普通 INSERT、事务提交、分区 flush、bulk 追加）按 **(数据库, 表, 分区) 粒度互斥**；REPLACE、未命中分区裁剪的 UPDATE / DELETE、DDL 等全表一致操作获取表级宽锁执行。
- **分区裁剪命中时锁更细**：分区键等值的 UPDATE / DELETE 只锁目标分区；compaction 按分区并行执行、每个分区重写只锁自己；后台健康统计为纯读，不再加锁。集合外分区的写入不受影响。
- 同一分区上的写入仍然严格串行；**不同分区之间互不阻塞**。对按时间分区的大表（如分钟级行情表），历史分区的 flush / compaction 不再阻塞当前分区的实时写入。
- **没有行级锁**。两个会话修改同一行时，后执行者覆盖先执行者（last-writer-wins），不报错也不等待。
- 主键与唯一约束的冲突检查在写入前完成：违反约束的批会在**写入任何行之前**整批报错（语句级原子性）。
- 跨多张表的操作按表名排序后依次加锁，同一张表内多个分区按分区名排序加锁，避免相互等待造成死锁。
- 长事务提交时会依次持有其涉及分区的分区锁；大事务提交只停顿它写过的分区，同表其他分区的写入不受影响。

## 并发读取

- **读写互不阻塞**：查询不获取表写锁，读取的是已提交 memtable 与不可变 segment。
- flush（Pending → Flushing → Flushed/Archived）期间查询不丢不重：行数 watermark 保证跨 flush 边界的 COUNT/SELECT 一致，相关回归测试覆盖了 point lookup、COUNT 与并发重写场景。
- 对启用常驻热存（`ALTER TABLE ... SET HOT`）的表，当天分区的点查走内存倒排索引，不触碰 segment；flush 不影响该路径。

## UPDATE 与 DELETE 的并发行为

- UPDATE 按谓词重写受影响的 row group，DELETE 以删除位图做逻辑删除。谓词命中分区键等值条件时，两者只在目标分区的分区锁内执行，与该分区并发写串行化、不影响其他分区；其余情况在表级宽锁内执行。
- UPDATE 拒绝修改分区键，避免行在分区之间"移动"造成不一致。
- 删除后的可见性（memtable 内、flush 后、混合状态）由 `delete_visibility_regression_test` 覆盖。

## 当前边界与不保证

以下是当前版本**不提供**的行为，使用时应显式规避：

1. **RepeatableRead / Serializable**：未实现。需要稳定快照的应用应在单条查询内完成读取，或自行在应用层加版本戳。
2. **行级多版本（MVCC）**：不存在版本链；写冲突靠表级串行 + 约束检查解决。
3. **事务内 DDL**：DDL 语句不检查活动事务，在事务内执行会**立即生效，且 `ROLLBACK` 不会回滚它**。事务应只包含 DML。
4. **跨语句快照**：ReadCommitted 语义下，事务内每条语句都读取最新已提交数据。

## 代码索引

| 行为 | 位置 |
|------|------|
| 事务状态机与 ReadCommitted 默认值 | `src/storage/txn/txn.rs` |
| memtable 可见性（`TxnVisibility` / `is_visible_to`） | `src/storage/core/memtable.rs` |
| 表级宽锁、分区锁、分区子集锁与跨表锁排序 | `src/storage/segment_manager/manager_core.rs`（`with_table_mutation_lock` / `with_partition_mutation_lock` / `with_partition_set_mutation_lock` / `with_database_mutation_locks`） |
| UPDATE / DELETE 与 compaction 的分区收窄 | `src/sql/execution/dml_update.rs`、`src/sql/execution/dml_delete.rs`、`src/storage/core/compactor.rs` |
| 写入与提交流程 | `src/storage/core/batch_writer.rs`（`write_columns_with_txn` / `commit_txn` / `rollback_txn`） |
| 读路径可见性过滤 | `src/sql/execution/scan/memtable.rs` |
| 事务端到端行为 | `tests/transaction_integration_test.rs` |
| flush 期间读写一致性 | `tests/concurrency_stability_test.rs` |
| 约束与冲突原子性 | `tests/constraints_test.rs` |
| durable tail 组提交与惰性刷盘 | `src/storage/core/durable_tail.rs`（`tail_commit_loop` / 自适应窗口 / `sync_dirty_tails_for_table`） |
| 实时写入扩展性与恢复契约 | `tests/realtime_write_scaling_test.rs` |
