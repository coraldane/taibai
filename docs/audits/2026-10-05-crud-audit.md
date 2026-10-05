# CRUD 内存、操作放大与正确性审计

审计日期：2026-10-05。代码基线：`e43da30`。此次只增加审计文档和临时数据库复现程序，未修改生产实现。

结论：发现 10 项问题或性能风险。前 8 项有执行复现（第 7 项分别验证两个算子）；后 2 项由代码路径确认，未进行规模压测。没有在本轮证明永久性内存泄漏；已确认的是预算遗漏、数据复制和计算/扫描放大。现有测试通过不能覆盖这些反例。

## 检查范围

| 操作 | 检查链路 | 主要发现 |
| --- | --- | --- |
| INSERT | SQL 快速/普通物化、约束验证、分区路由、批写入、尾部持久化、索引发布 | UPDATE 后快照过期导致错误约束判断；索引失效时全表键物化 |
| SELECT | 扫描与投影、物化/流式管线、Sort/Join/Aggregate 的预算、Window/TopN、结果收集 | Window AVG 错误；Window/TopN 缺预算；非默认窗口帧二次复杂度 |
| UPDATE | 自动提交/显式事务、memtable/segment、约束验证、镜像 staging、sidecar、索引维护 | 内存表索引未同步、失败语句残留、DECIMAL scale 丢失、NOT NULL 漏检、整批复制与预算遗漏 |
| DELETE | 内存表计划与应用、segment 匹配/位图持久化、错误处理、索引/热缓存/行数清理 | segment 读取错误绕过清理；内存表按整批复制幸存行 |

检查涉及持久化与锁的调用顺序，但本轮未穷举所有崩溃时机、磁盘故障和并发交错；不能据此宣称其余路径没有缺陷。

## 1. P1：内存表 UPDATE 唯一键后，索引快照仍保留旧键

位置：[dml_update.rs](/Users/coral/workspace/taibai/src/sql/execution/dml_update.rs:587)、[constraints.rs](/Users/coral/workspace/taibai/src/sql/execution/dml_insert/constraints.rs:94)。

自动提交 UPDATE 的增量索引退休/追加覆盖 segment 镜像；后面的 `memtable_updates` 只持久化 sidecar 并替换 batch，没有同步该批旧键、新键或使相关唯一键快照失效。INSERT 直接信任快照，memtable 内容版本变化也不会自动触发该快照刷新。

复现：表有 `UNIQUE KEY uk_val (uk)`，先 INSERT `(id=1, uk=100)`，不 FLUSH，UPDATE `uk=200`。随后 INSERT `(id=2, uk=200)` 成功，查询得到两个 `uk=200`；INSERT 已释放的 `uk=100` 却返回 duplicate key。

影响：约束被破坏，也会错误拒绝合法 INSERT。

修复方向：在持有约束锁/变更锁期间，为内存表匹配行退休旧键并追加新键；无法精确维护时，统一失效唯一键和主键快照。回归必须覆盖未 FLUSH 的数据，不能只测 segment UPDATE。

## 2. P1：显式事务内失败的 UPDATE 会留下已修改的数据

位置：[txn_dml.rs](/Users/coral/workspace/taibai/src/sql/execution/txn_dml.rs:637)。

事务自己的 memtable 在规划扫描阶段立即 `replace_pending_batch`。未知 SET 列的完整验证要到后面的 `precompute_assigned_values` 才执行；后续错误没有恢复这个提前发生的替换。

复现：`BEGIN; INSERT INTO x VALUES (1,10); UPDATE x SET v=99,nosuch=5 WHERE id=1;`。UPDATE 返回未知列错误，但同事务 SELECT 已经看到 `v=99`，COMMIT 后仍为 99。

影响：客户端收到失败，却可以提交该失败语句的部分效果。这与整笔事务的 ROLLBACK 是否有效是两个不同问题。

修复方向：所有赋值先验证；事务自己的 memtable 替换也进入语句级 staging，扫描、预算和验证全部成功后一起应用，并具备失败补偿。

## 3. P1：UPDATE DECIMAL 忽略列 scale，静默写入错误数值

位置：[dml_update.rs](/Users/coral/workspace/taibai/src/sql/execution/dml_update.rs:765)、[dml_update.rs](/Users/coral/workspace/taibai/src/sql/execution/dml_update.rs:883)、[scalar_utils.rs](/Users/coral/workspace/taibai/src/sql/execution/helpers/scalar_utils.rs:279)。

两条 UPDATE 赋值路径都调用 `eval_expr_to_scalar`，它固定传入 `scale=0, timezone=0`，没有使用目标列 scale。INSERT 已使用带 scale 的转换。

复现：`DECIMAL(10,2)` 列 INSERT 1.23 后，内存表 UPDATE 4.56 写入原始值 `Decimal(5)`，应为 456；FLUSH 后 UPDATE 7.89 写入 `Decimal(8)`，应为 789。按 scale=2 解码分别为 0.05 和 0.08。

影响：内存表和 segment 路径均静默损坏数值。

修复方向：赋值解析统一使用目标 ColumnDef 的 scale 和会话时区；当前 UPDATE 接口未携带会话时区，时区行为还需独立回归验证。

## 4. P1：UPDATE 未检查 NOT NULL

位置：[dml_update.rs](/Users/coral/workspace/taibai/src/sql/execution/dml_update.rs:760)、[dml_update.rs](/Users/coral/workspace/taibai/src/sql/execution/dml_update.rs:883)。

赋值预计算只做类型转换，没有执行 INSERT 的 `validate_nullable` 等价检查。列数组本身允许 NULL，并不替代 SQL 约束验证。

复现：`v INT64 NOT NULL` 的行执行 `UPDATE u SET v=NULL WHERE id=1` 返回成功，SELECT 得到 NULL。

修复方向：预计算赋值后、任何 staging 或替换前，统一检查 nullable/primary_key 等约束。不能仅依赖唯一键检查间接拦截。

## 5. P1：DELETE 的读取错误绕过部分应用后的索引清理

位置：[dml_delete.rs](/Users/coral/workspace/taibai/src/sql/execution/dml_delete.rs:929)、[dml_delete.rs](/Users/coral/workspace/taibai/src/sql/execution/dml_delete.rs:986)、[dml_delete.rs](/Users/coral/workspace/taibai/src/sql/execution/dml_delete.rs:1081)。

DELETE 先永久应用内存表删除，再扫描 segments。footer 写入失败有专门清理分支，但 `discover_segments`、`open_reader_cached`、行组解码和谓词计算的 `?` 会直接退出，绕过该分支及正常结束时的索引维护、热缓存失效和 catalog 行数修正。

复现：临时表 d 带唯一键 id，INSERT id=1；在该临时表目录放入不可读的 `seg_99999.dat`。`DELETE FROM d` 返回 storage error，但 memtable 已有 0 行。移除测试损坏文件后重新 INSERT id=1，仍被旧唯一键快照拒绝。

影响：失败语句部分删除，并留下与物理数据不一致的派生状态。复现已验证唯一键快照陈旧；热缓存和 catalog 问题是同一退出路径的代码推导，未单独故障复现。

修复方向：优先在应用前完成可失败的规划；即使保留部分应用语义，也必须让所有退出路径经过统一的变更后清理，而不是只处理 footer 写入错误。

## 6. P1：默认窗口 AVG 始终返回 NULL

位置：[window.rs](/Users/coral/workspace/taibai/src/sql/execution/operators/window.rs:639)、[window.rs](/Users/coral/workspace/taibai/src/sql/execution/operators/window.rs:730)。

运行前缀聚合只在 `AggFunc::Count` 分支递增 count；AVG 分支累加总和，却不递增 count。finalize 检查 `count == 0`，因此输出 NULL。

复现：v 为 10、20、30，`SELECT id, AVG(v) OVER (ORDER BY id) FROM w ORDER BY id` 实际为 NULL、NULL、NULL，应为 10、15、20。

修复方向：所有非 NULL AVG 输入都要增加计数，覆盖整数、浮点、DECIMAL、全 NULL 和分区重置。

## 7. P1：Window 和 TopN 未执行查询内存预算

位置：[window.rs](/Users/coral/workspace/taibai/src/sql/execution/operators/window.rs:73)、[topn.rs](/Users/coral/workspace/taibai/src/sql/execution/operators/topn.rs:47)。

Window 无限制收集全部输入，随后复制输入列并物化排序键/结果值；TopN 保留 capacity 行及键、heap、chunks，但没有使用 MemoryReservation 或本地字节预算。大 OFFSET 会增大 TopN 的 capacity，即使最终 LIMIT 很小也会保留大量行。

算子复现：设查询预算为 1 MiB，以 200,000 行 INT64 输入运行。

- Window ROW_NUMBER 正常返回，输出估算大小 3,754,920 bytes。
- TopN capacity=190,000 正常返回，输出估算大小 1,543,752 bytes。

这里只统计输出 batch 的估算大小，不是 RSS 测量；内部排序键、临时数组和 heap 会增加峰值。普通执行的最终结果限制可能稍后拒绝结果，但它发生在这些分配之后，不能防止中间状态耗尽内存。

修复方向：为两个算子的所有保留状态执行字节记账；Window 支持按分区处理/溢写或提前报预算错误；TopN 对大 capacity 采用预算约束或外部排序。

## 8. P2：内存表 UPDATE/DELETE 整批复制，UPDATE 还绕过 mutation budget

位置：[dml_update.rs](/Users/coral/workspace/taibai/src/sql/execution/dml_update.rs:127)、[dml_update.rs](/Users/coral/workspace/taibai/src/sql/execution/dml_update.rs:792)、[dml_delete.rs](/Users/coral/workspace/taibai/src/sql/execution/dml_delete.rs:460)。

普通 resident memtable 的 batch Arc 仍由 memtable 持有，共享 walk 又保留 original，所以 `Arc::try_unwrap` 通常进入 shared 分支。未赋值的普通平坦列 `clone()` 深拷贝数据，赋值列也重建全部行。DELETE 对所有列复制幸存行。修改/删除一行的复制量因此与整批大小相关，所有命中 memtable 的计划又同时保留到应用阶段。

UPDATE 的 `image_bytes` 预算只计 segment 镜像，不计前面保存的 `memtable_updates`。

预算复现：将 `TAIBAI_TXN_MUTATION_BUDGET_BYTES` 设为 1024，更新 1000 行 resident memtable 成功；FLUSH 后对同表同规模 UPDATE 返回 `32896 > 1024 bytes` 的预算错误。

影响：宽表、小更新、跨多个 memtable 更新时，CPU 和峰值内存远大于匹配行规模；小 mutation budget 对 resident 路径不生效。此项是复制/预算问题，不是已证实的永久泄漏。

修复方向：所有 staging 都记账；以列共享、行级增量镜像或有界分批计划降低整批复制。DELETE 同样需要对幸存行复制成本进行预算检查。

## 9. P2：INSERT 的索引回退仍会全表物化键，并深复制内存表宽列

位置：[constraints.rs](/Users/coral/workspace/taibai/src/sql/execution/dml_insert/constraints.rs:252)、[constraints.rs](/Users/coral/workspace/taibai/src/sql/execution/dml_insert/constraints.rs:191)、[memtable.rs](/Users/coral/workspace/taibai/src/storage/core/memtable.rs:292)。

唯一键快照不存在或失效时，单行 INSERT 通过 `collect_unique_index_entries` 收集所有现存键到 Vec 后才检查冲突并安装快照。没有对该全表 Vec 执行预算限制；多个唯一约束可分别触发回退。扫描 memtable 还调用 `read()`，深复制全部列，而不是只读取键列的 Arc 快照。

这是代码确认的放大路径，未做大表 RSS 压测。回退成功后的快照能避免紧接着每次 INSERT 都重复扫描，但不能消除首次恢复索引时的峰值，也不能消除失效后反复重建的成本。

修复方向：先用新增键集合对现存键做流式探测；需要重建时使用有界/分块索引构建，避免整个键集合常驻；memtable 扫描改用 Arc 快照和窄投影。

## 10. P2：非默认窗口帧仍存在 O(N²) 计算和分配

位置：[window.rs](/Users/coral/workspace/taibai/src/sql/execution/operators/window.rs:305)。

例如 `SUM(v) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)` 无法进入当前 RANGE 前缀优化，每行重新构造完整 frame Vec 并聚合。N 行分区会读取/复制约 N(N+1)/2 个元素。显式整分区 frame 也可能对每一行重复计算同一个聚合。内层没有取消检查，长计算期间不能及时响应查询取消。

这是代码确认的复杂度风险，未进行规模基准测试。

修复方向：ROWS 前缀使用运行累计；整分区聚合每分区计算一次；滑动帧使用增量/前缀状态，并在计算阶段周期检查取消标记。

## 复现与验证

复现程序只操作 `tempfile::tempdir` 创建的测试库。DELETE 的损坏 segment 也只写入该临时库。

```sh
cargo run --example audit_probe
cargo run --example audit_memory_probe
```

第一份程序输出正确性反例；第二份包含验证当前预算遗漏行为的断言。它们是诊断程序，不是修复后应继续通过的正确性回归测试。修复时应将预期反转并转换成标准测试。

已执行 `cargo test --lib -- --test-threads=1`：779 passed、0 failed、4 ignored。

另执行 CRUD/查询相关集成测试：`constraints_test`（23）、`delete_test`（13）、`query_predicate_test`（8）、`transaction_dml_test`（14）、`update_test`（17）、`window_function_test`（12）。共 87 passed、0 failed。

建议先修复 1–6 的数据正确性，再补齐 7–9 的预算和复制路径，最后优化 10；每个修复都应覆盖 resident/FLUSH 后的数据形态，以及失败语句后继续 INSERT/SELECT/COMMIT 的行为。
