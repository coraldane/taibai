# 代码重构设计：消除重复、拆分超大文件、引入设计模式

> 诊断日期：2026-10-04 · 基线：dev 分支 44cadff
> 诊断方法：滑动窗口重复代码扫描（9 行显著代码窗口，12 行起报）+ git 修改热区统计（2025-01 以来 336 次提交）+ 三路子系统逐文件勘察（storage / sql 执行 / net·common·types）。

---

## 1. 诊断结论总览

| 指标 | 数值 |
|---|---|
| 总代码量（src，含测试） | 142,054 行 Rust |
| 检出重复块 | 416 块，约 6,300 行重复（双计），其中非测试代码约 2,500 行为逐字重复 |
| ≥2000 行的文件 | 12 个（最大 durable_tail.rs 3,326 行） |
| ≥300 行的单函数 | ≥15 个（最大 compute_folded_aggregate 923 行） |
| `#[allow(clippy::too_many_arguments)]` | 22 处（集中在 DML 与存储写路径） |
| "used to / old / previously" 考古注释 | ~30 处（说明大量代码是修 bug 叠加的产物） |
| git 修改热区 TOP5 | engine.rs(73) · segment_manager.rs(69) · batch_writer.rs(65) · net/handler.rs(64) · dml_delete.rs(61) |

**核心判断：病根不是"文件太大"，而是三类结构性问题。**

### 病灶 A：`include!` 假模块拆分（一切问题的放大器）

历史上做过"拆文件"，但用的是文本拼接而非真模块，子文件共享父模块全部私有项，没有独立编译边界：

| 拼接宿主 | 被拼文件 | 位置 |
|---|---|---|
| `engine.rs` (717 行) | engine/core.rs (1,429) + runtime.rs (1,285) + bulk.rs (820) → **逻辑翻译单元 4,251 行** | engine.rs:563-565 |
| `segment_manager.rs` | manager_core.rs (2,181) + buffered_write.rs + direct_bulk.rs + lifecycle.rs → **约 3,800 行** | segment_manager.rs:115-118 |
| `ddl.rs` | ddl/table.rs (1,749) + database_auth.rs + introspection.rs (1,135) | ddl.rs:724-726 |
| `dml_insert.rs` | fast.rs (1,152) + write.rs (1,910) + constraints.rs | dml_insert.rs:70-72 |
| `array.rs` (2,004) | array/analytics.rs (1,433) | array.rs:2004 |

后果：core.rs 里一个 1,411 行的连续 `impl Engine` 块、方法缩进错乱（core.rs:2 vs core.rs:676）、测试只能挂在拼接宿主上（segment_manager.rs:170-188）。git 热区显示改动的放大效应——engine.rs 与 segment_manager.rs 恰好是修复最频繁的两个文件。

### 病灶 B：巨型函数与多职责文件

最大函数（行号为起始位置）：

| 函数 | 位置 | 行数 |
|---|---|---|
| `compute_folded_aggregate` | scan/aggregate_meta.rs:60 | 923 |
| `execute_stmt` | engine/core.rs:676 | 736 |
| `exec_update` | dml_update.rs:36 | 671 |
| `lookup_filtered` | hot_store.rs:1396 | 336 |
| `restore_replace_memtable_undo` | dml_delete.rs:505 | 345 |
| `replace_keys_may_match_row_group_from_refs` | dml_delete.rs:902 | 387 |
| `build_operator_with_limit` | physical/planner.rs:160 | ~360 |
| `merge_accumulator` | aggregate/aggregate_fast.rs:863 | 339 |
| `bind_correlated_subquery` | query.rs:2121 | 368 |
| `handle_framed` | net/server.rs:173 | 284 |
| `estimate_plan_profile` | physical/planner.rs:1433 | 289 |
| `gather_into` | types/array/analytics.rs:705 | 248 |
| `read_scalar_value` | net/client.rs:1334 | 193 |
| `decode_param_value` | net/handler.rs:880 | 175 |
| `tail_commit_loop` | durable_tail.rs:615 | 209 |

多职责混装典型：durable_tail.rs 一个文件混了 **13 类职责**（格式/组提交线程/sync 策略/多代仲裁/flush intent/sidecar/恢复/损伤统计/复制对接/文件清理/标量编解码/守卫/对外 API）；dml_delete.rs 里 **REPLACE 语义机器（约 970 行）放错了文件**；helpers.rs 混入 pubsub 的 ChangeEventBuffer；physical/planner.rs 里藏着一个 ~900 行的 join-order 优化器。

### 病灶 C：已漂移的重复（不是坏味道，是潜伏 bug）

以下重复**已经语义分歧**，属于第一批必须处理的高危项：

1. **标量比较有 5 份实现，行为已分叉**：aggregate.rs:1807、storage/index/sort_key.rs:48、helpers.rs:802、operators/sort.rs:456、query.rs:331。aggregate 版 null 排序与 sort_key 版**相反**，且**缺 Bool/Time/Interval/Blob 臂**（sort_key.rs:76-88 注释自证"没有这些臂会静默比较为 Equal"——aggregate 版现在就带着这个问题）。
2. **ScalarValue wire 编解码 3 套编码器 + 3 套解码器**：client.rs `encode_scalar_param`(1597)/`read_scalar_value`(1334)、handler.rs `decode_param_value`(880)、storage/codec/scalar_tag.rs、protocol/result.rs。已分歧：Decimal（handler 只支持 8 字节 i64，client 额外支持 scale>0 文本形式）、VarChar（handler 严格 UTF-8 校验，client `from_utf8_lossy` 静默替换）、错误文案、null 表示互不相同。
3. **占位符 AST walker 残缺复制品**：query.rs:2533-2630 自己重写了一份 `contains_placeholder`，而 prepared.rs:622-727 已有完整版；query.rs 版**不遍历 FromClause 派生表与 Insert/Update/Delete**——语义已漂移且偏弱。
4. **跨文件逐字节复制、注定再漂移**：`run_fold_type_ok`(aggregate_meta.rs:1574) 与 `column_sum_supported`(segment/mod.rs:42) 连"UInt64 excluded"注释都一样；`compute_segment_zone_maps`(writer.rs:997) 与 `compute_segment_zone_maps_from_row_groups`(partition/pruning.rs:207) 53 行逐字节相同；`null_for_dtype`(scalar_tag.rs:8) 与 `ScalarValue::null_for_type`(scalar.rs:192) 是同一个 19 臂 match 的两份拷贝。

### 病灶 D：补丁叠层的固定模式

- **锁内二次检查（in-lock re-check）至少 6 处手写变体**：DML 三份逐字相同（dml_delete.rs:1342、dml_update.rs:123、dml_insert/write.rs:92，连注释都一样"the fn-top gate races a transaction registering…"），存储侧 pin re-check 三处（batch_writer.rs:1759、compactor.rs:1316/1390、manager_core.rs:1436/1901/1960）。
- **txn 手写 unwind 补丁**：txn_dml.rs:410-465 与 803-858 是逐字相同的"staged tombstone 注册失败手动逆注册"循环——这段重复本身就是修"中途失败留下已注册 tombstone"bug 的补丁产物。
- **fast 路径三兄弟，两种世界观**：dml_insert/fast.rs 是一个 **1,152 行的手写字节级 SQL 迷你解析器**（重复实现 parser/lexer 与 decimal/时间戳解析，靠 core.rs:279 的 `b'I'` 裸字节嗅探钩进主路径，另配 `FAST_INSERT_DISABLED` 测试开关——路径间不等价的直接证据）；aggregate_fast.rs 的 `merge_accumulator`(339 行) 把每个累加器变体在第二套类型化表示里再实现一遍；而 **scan/fast_path.rs 是唯一健康的范本**——`ScanFastPath` trait + 链式节点 + decline 原因记录（commit 44cadff 的产物）。
- **字符串当错误码**：handler.rs:1997-2014 用 `contains("memory budget")` 区分确定性/瞬时 ResourceBusy。
- **配置双轨**：config.rs 里 82 个 `default_*` 函数 + `default_*_settings()` 内联重写 + `impl Default` 三写一默认值；另有 8 个 `OnceLock` 进程级全局与 serde 配置并行（main.rs:183-248 逐个注入）。
- **错误处理形态**：handler.rs 中 82 处 `build_error`，约 60 处是 `Ok(vec![build_error(...)])` 形态——调用方无法区分协议错误与业务错误。

### 病灶 E：类型穷举税

`ColumnArray` 19 变体 + 15 处以上穷举 match（array.rs `empty`/`with_capacity`/`len`/`data_type`/`bitmap`/`push`/`extend`/`scalar`…），每新增一个变体要改 array.rs 约 10 处 + analytics.rs 6 处 + protocol/result.rs、client.rs 各 1 处。`Utf8Buffer`/`BinaryBuffer`(array.rs:11-319) 是手工复制的双胞胎；analytics.rs 的 `col_min`/`col_max`(187-374)、`cmp_mask_*`(1010-1092) 是 4-6 份同模板。analytics.rs 自己的 `impl_eq_counts!` 宏(92-163)已示范了解法并注释"Type coverage MUST stay identical"。

---

## 2. 设计模式映射（为消除重复服务，不为模式而模式）

Rust 中的设计模式应惯用化：trait 即 Strategy、闭包注入即 Template Method、泛型 fold 即 Visitor、声明宏即表驱动。以下每一条都对应第 1 节的具体病灶，注明落点。

### 2.1 FastPath 链（Chain of Responsibility / Strategy）——已验证的范本，推广它

scan/fast_path.rs:137-247 的 `ScanFastPath` trait（`try_decline/try_execute` + 链式节点 + decline 原因记录）是仓库内唯一收敛成功的 fast 路径。推广：

- `dml_insert/fast.rs`：删除字节级迷你解析器。若量出确有瓶颈，以"结构化 `Statement` 上的轻量执行节点"重建（实现同一 FastPath trait 族），而不是绕过 parser 重解析字节。
- `aggregate_fast.rs`：给 `Accumulator` 增加"类型化视图"trait（`as_i64_sum()/as_f64_avg()/…`），`merge_accumulator` 改为对该视图的泛型合并，消灭第二套实现。

### 2.2 泛型 fold（Visitor 的 Rust 形态）——收益最大的一项

prepared.rs 有 4 个平行 AST walker（`bind_params` 129、`substitute_placeholders` 266、`count_placeholders` 385、`has_placeholders` 622，约 450 行），query.rs:2533-2630 又有一份残缺复制品。定义：

```rust
trait StmtFolder: Sized {
    type Out;
    fn fold_expr(&mut self, e: &Expr) -> Self::Out;
    fn fold_stmt(&mut self, s: &mut Statement) -> ...;
    // 框架提供默认的全 AST 遍历（含 FromClause 派生表、DML、Explain），
    // 各 walker 只覆写关心的变体
}
```

一次实现覆盖全部 5 份，并顺带修复 query.rs 版不遍历派生表的漂移。同理合并 planner/expr.rs 的三个平行转换器（`ast_expr_to_logical[_with_context|_join]`，约 80% match 臂相同）为一个 `convert(expr, ctx: &impl Resolver)`——递归策略由 `Resolver` 提供，预计净删 ~350 行。

### 2.3 Template Method（闭包注入骨架）

- **join.rs 四个 probe 函数**（613-696、512-531 vs 815-834 逐字相同）：合并为一个 probe 骨架，"侧向取行 + emit"以闭包注入，外侧枚举 `ProbeFlavor`（已有 `ScalarProbeKind` 可参考）。
- **batch_writer.rs 三份 flush 流水线**（1864-1922、2036-2108、2140-2209，共享 89 行模板）：提取 `run_group_flush(&self, group, protocol: impl FnOnce(...) -> FlushOutcome)`。注释 1604-1605 自证"group protocol is the single protocol generalized"——只差把三协议收进一个函数。
- **compactor.rs commit 守卫**（1304-1339 vs 1376-1411）：提取 `ensure_commit_safe(&self, prepared) -> Result<Option<CompactStats>>`，并成为全库唯一 pin re-check 实现（替代 6 处手写变体，见 2.6）。
- **ddl/table.rs 三处锁重试循环**（600-616、797-813、988-1004）：`with_table_mutation_lock_retried(catalogs, db, name, label, op)`。

### 2.4 唯一事实源（Single Source of Truth，配 Facade 迁移）

- **标量比较**：以 sort_key.rs:48 为准下沉 `types/scalar.rs`，`null_order: Ordering` 参数化；删除其余 5 份（aggregate.rs:1807、sort.rs:456、helpers.rs:802、query.rs:331）。注意这是行为修复（补齐缺失臂），必须有针对性测试先行。
- **ScalarValue wire codec**：在 `protocol/scalar_codec.rs` 建唯一 `encode_param/decode_param/null_for(DataType)`；client/handler/scalar_tag/protocol-result 四处改为调用方（Adapter 角色）。wire 语义分歧（Decimal scale、UTF-8 严格性）需要显式决策：建议以 handler 的严格语义为准，client 宽松侧加告警。
- **DataType 19 臂样板**：null_for_type（scalar.rs:192 与 scalar_tag.rs:8 合一）、DataType tag 表（scalar_tag.rs:73-127 改用 protocol/types.rs:8-74）。`ColumnArray` 穷举族用声明宏表驱动（analytics.rs `impl_eq_counts!` 已示范），`Utf8Buffer`/`BinaryBuffer` 用 `OffsetBuffer<T>` 泛型合一，`col_min/col_max`/`cmp_mask_*` 用泛型 + NaN 分派合一。

### 2.5 Command 分发（execute_stmt 瘦身）

engine/core.rs:676-1411 的 736 行 match 拆为 `engine/dispatch.rs`：每个 Statement 变体一个 `fn exec_xxx(&mut self, ...)` 私有方法，match 只留一行映射；同时 `ensure_permissions`(core.rs:1139-1336) 独立为 `engine/permissions.rs`。durable_tail 的 `CommitCommand/AppendCommand` 已是 Command 雏形，保持。

### 2.6 RAII Guard 统一（Null Object / 现有惯用法收敛）

`HotFeedGuard`(runtime.rs:5) 与 `HotBulkCoverageGuard`(bulk.rs:175) 是同一"喂热存覆盖记账"的两个平行 guard，合一。`FlushGroupGuard`(batch_writer.rs:130) 是好实践，把 in-lock re-check 收进 `DmlGate`（一个类型，三个 DML 文件共用），re-check 逻辑只写一次。

### 2.7 参数对象（Builder 的轻量形态）

22 处 `too_many_arguments` 是参数对象缺失的直接信号。优先级最高的三个：flush 协议族（batch_writer 8 处）、compactor 6 处、dml_insert/write.rs 9 处——统一收敛为 `FlushRequest`、`CompactionRequest`、`InsertBatch` 等结构体，allow 属性随之删除。

### 2.8 错误建模（替代字符串嗅探）

`TaibaiError` 增加类别枚举（`ResourceBusy { deterministic: bool }` 等），handler 的消息族处理改为返回 `Err(TaibaiError)`，由分发层一次 `build_query_error`。`build_query_error` 的 `contains("memory budget")`(handler.rs:1997-2014) 与 82 处 `Ok(vec![build_error(...)])` 形态随之消除，KILL/cancel 与指标可按错误类别统计。

---

## 3. 模块边界重划

### storage

| 现文件 | 拆为 | 依据 |
|---|---|---|
| durable_tail.rs (3,326) | `durable_tail/{format, committer, generations, sidecar, flush_intent, recovery, replication, api}.rs` | 13 类职责，tail_commit_loop 209 行 |
| batch_writer.rs (2,216) | `batch_writer/{config, worker, freeze, protocol, pressure, gate}.rs` | impl 2,000 行，8 处 allow |
| compactor.rs (2,616) | `compactor/{plan, unsorted, clustered, commit, gather}.rs` | gather（跨类型行 gather 约 430 行）与压缩语义无关 |
| segment_manager.rs + 4 include | 去 include! 改真 mod；manager_core 再拆 `{readers, paths, locks, replace_journal, epochs}` | include! 是 69 次修改热区的放大器 |
| hot_store.rs (2,491) | `hot_store/{keys, predicate, grouper, chunk, store, manager}.rs` | lookup_filtered 336 行归 store，其余独立关注点 |
| segment/reader.rs (1,993) | 打开+列读取保留；delete journal + footer 回滚补偿（约 470 行）移 `segment/journal.rs` | journal 生命周期不依赖 SegmentReader 实例 |
| codec/column_codec.rs (2,052) | `codec/encodings/{rle, delta, dod, bitpack, gorilla, dictionary}.rs + selected.rs + choose.rs` | decode_column_with_runs 185 行 match 可下沉 |
| memtable.rs (1,577) | `memtable/{mod, replace_key, manager}.rs` | 三类型边界清晰 |

### sql

| 现文件 | 拆为 | 依据 |
|---|---|---|
| engine.rs + 3 include (4,251) | `engine/{mod, dispatch, permissions, session_txn, hot_feed, flush, auto_compact, bulk}.rs` | runtime.rs 是 grab-bag；日终调度与日终 flush 分居两文件 |
| query.rs (2,775) | `select_exec.rs`（入口+物化）+ `derived.rs`（派生表+表引用提取）+ `correlated.rs`（**合并 query.rs 子查询机制、decorrelate.rs、subquery_runtime.rs 三个相关子查询模块**） | bind_correlated_subquery 368 行；三个模块同一关注点 |
| physical/planner.rs (2,334) | join-order 优化器（545-1432，约 900 行）迁 `sql/optimizer/join_order.rs`；算子构建留在 physical | 优化职责放错层 |
| dml_delete.rs (1,989) | REPLACE 机器（43-1014）迁 `dml/replace.rs`；delete 逻辑留下 | REPLACE 语义放错文件（dml_delete.rs:84-89 注释自证） |
| helpers.rs (1,635) | `eval/`（三套 WHERE 求值器）+ `scalar.rs` + `change_events.rs`（迁 pubsub 侧） | 求值器分裂调用是语义漂移温床 |
| scan/aggregate_meta.rs (2,287) | fold 规划器（KeyZone/plan_run_fold）+ fold 执行（AggAccum）+ `raw_i64_scalar`（400 行标量转换下沉 types/scalar） | compute_folded_aggregate 923 行 |
| txn_dml.rs (1,143) | 提取 `apply_staged<T>(staged, register, unregister)` 消灭两份 unwind；delete/update 两路径共享 plan 骨架 | 410-465 vs 803-858 逐字相同 |
| ddl/table.rs (1,749) | 10 个 exec_* + `alter_column_catalog_only` 独立 + 锁重试 helper | 见 2.3 |

### net / common / types

| 现文件 | 拆为 | 依据 |
|---|---|---|
| net/handler.rs (2,057) | 按消息族：`{session, query, prepared, pubsub, replication}.rs` + `respond.rs`（ExecResult→Message 唯一映射） | 7 类职责；17 处 "not logged in" 样板、3 份 timings 构造 |
| net/client.rs (1,851) | `client/{connection, commands, decode}.rs`；`read_scalar_value` 直接删除改用 protocol/scalar_codec | 13 处 "unexpected response type" 三臂样板 → `request() -> Result<Message>` + `expect_ok/expect_u64` 应答器 |
| main.rs (973) | CLI REPL 迁 `src/bin/taibai-cli.rs`（两份 QueryResult 打印合一）；Windows 服务拆 `service.rs`；8 个全局 setter 改为构造参数注入 | main 混 4 类职责 |
| bin/backup.rs / migrate.rs | `collect_table_dirs` 移入已存在的 `tools::segment` | 整函数 44 行复制 |
| common/config.rs (1,346) | `#[serde(default)]` 单一来源替代三写；clap Args/RunMode 移出；`PerformanceBaselineSettings/Config` 合一；长期消灭 8 个 OnceLock | 82 个 default_* 双写三写 |
| types/array.rs + analytics | 去 include! 改真模块；穷举族宏表驱动；Buffer 泛型合一 | 见 2.4 |

---

## 4. 实施路线（五阶段，风险从低到高）

**总原则：每一步都是可独立回滚的小 PR；先修"已漂移的重复"（潜伏 bug），再修"未漂移的重复"（坏味道），最后动结构。每阶段收尾跑全量测试 + 重复扫描对比（防回潮基线）。**

### Phase 0：安全网（半天）
1. 重复扫描脚本入库（本设计的 `target/dup_scan.py` → `scripts/dup_scan.py`），CI 里跑并对比基线，禁止重复行数回涨。
2. clippy 门禁：新代码禁增 `too_many_arguments`/`type_complexity` allow。
3. 确认 `cargo test` 全绿作为重构门禁（现有 `*_test.rs` 经 `#[cfg(test)] #[path=...]` 组织，保持不动）。

### Phase 1：零风险机械去重（逐字节相同，纯搬移）
| 项 | 动作 | 预计消除 |
|---|---|---|
| zone map 聚合 | 删 writer.rs:997-1049，复用 partition/pruning.rs:207 | 53 行 |
| `null_for_dtype`/`null_for_type` | 合一到 types/scalar.rs | ~45 行 |
| `decimal_multiplier` | 删 write.rs:1811，用 helpers.rs:1257 | 30 行 |
| `arg_column_type` | 删 aggregate_fast.rs:1255 | 18 行 |
| `metadata_float32/64` | trait `FloatMeta` 合一（writer.rs:854/897） | 45 行 |
| ChunkView 双份（hot_store.rs:892/968） | 提取共享内部 struct 或宏 | 34 行 |
| `SpillCursor` 两份（aggregate.rs:1128/1632） | `SpillCursor<K>` 泛型 | 50 行 |
| DDL 锁重试 ×3（ddl/table.rs） | `with_table_mutation_lock_retried` | 60 行 |
| in-lock re-check ×3（DML）+ pin re-check ×3（storage） | `DmlGate` + `ensure_commit_safe` | ~90 行 |
| `collect_table_dirs`（backup/migrate） | 移 tools::segment | 44 行 |
| prepared 查找失败块 ×2（handler.rs:814/1076） | `lookup_prepared` | 35 行 |

小计 ~500 行，全部无行为变化。

### Phase 2：消灭已漂移的重复（行为统一，测试先行）
按危险度排序，每项先写锁定当前正确行为的测试，再统一：

1. **标量比较 5→1**（以 sort_key.rs 为准，参数化 null_order，补 aggregate 缺失臂）——这是修 bug，不只是去重。
2. **ScalarValue codec 3+3→1**（protocol/scalar_codec.rs；wire 语义决策点：Decimal scale 支持、UTF-8 严格性——建议统一为严格语义）。
3. **占位符 walker**：StmtFolder 落地（2.2），删除 query.rs 残缺版，同时修复其漏遍历。
4. **planner/expr.rs 三转换器合一**（Resolver 注入）。
5. **txn_dml unwind → `apply_staged`**。
6. **三个 DML 公共前置**（partition-key 拒绝 + assignments 解析 + in-lock re-check → `DmlGate`）。

小计 ~800-1,000 行，附带消除 3 处潜伏 bug。

### Phase 3：include! → 真模块（纯结构，无行为变化）
engine（4 文件）、segment_manager（5 文件）、ddl（4 文件）、dml_insert（4 文件）、array/analytics 五处，全部改为 `mod` 声明 + 可见性修正。机械但量大；好处是此后每个子文件可独立编译、测试可挂真模块、clippy 按模块生效。

### Phase 4：巨型函数拆解 + 模块边界重划
按第 3 节表格逐文件执行，优先序按 git churn（修复频率）：
1. engine：execute_stmt → dispatch（736 行 → 每分支一方法）；日终调度/flush/warmup 各归其位。
2. query.rs 拆三 + 三个相关子查询模块合一（correlated.rs）。
3. dml_delete REPLACE 机器归位 dml/replace.rs。
4. planner join-order 迁 optimizer。
5. durable_tail 八模块、batch_writer/compactor/hot_store/reader/codec/memtable。
6. handler/client 按消息族；respond() 唯一映射 + 错误 Err 化（2.8）。

### Phase 5：长期收敛
- fast 路径全面统一为 FastPath trait 链（删 dml_insert/fast.rs 迷你解析器、aggregate_fast 二套实现）。
- config 单一来源 + 消灭 OnceLock 双轨。
- `ColumnArray` 穷举族宏表驱动全覆盖。
- WHERE 求值器三套 → 统一向量化入口 + 可选行级降级策略。

---

## 5. 量化预期与验收

| 维度 | 现状 | 目标 |
|---|---|---|
| 逐字重复（非测试） | ~2,500 行 | <300 行（测试夹具重复另计） |
| ≥2,000 行文件 | 12 | 0（单文件目标 <800 行，多数 <600） |
| ≥300 行函数 | ≥15 | 0（dispatch/fold 类匹配函数除外） |
| `too_many_arguments` allow | 22 | ≤5 |
| include! | 5 处 | 0 |
| ScalarValue codec 实现 | 3 编 + 3 解 | 各 1 |
| 标量比较实现 | 5 | 1 |
| AST walker 实现 | 5（1 份残缺） | 1 fold 框架 |

**验收信号**：`scripts/dup_scan.py` 基线对比不回涨；Phase 2 的三个行为修复有专门回归测试；git churn 热区在 Phase 4 后重新统计应显著分散。

---

## 6. 风险与对策

| 风险 | 对策 |
|---|---|
| Phase 2 行为统一改变既有语义（标量比较、codec） | 测试先行锁定行为；wire 语义决策点显式记录到 docs/design/ |
| Rust 可见性改造牵连过广（Phase 3） | 每次 include!→mod 一个 PR；`pub(crate)` 就地替换，不做接口重设计 |
| 重构期间新功能并行开发 | 按 churn 热区排序，先拆改动最频繁的；每 PR 只碰一个子系统 |
| 巨型函数拆解引入顺序依赖回归 | 拆函数不改逻辑顺序；保留原函数为薄壳转发一个 PR 周期后再删壳 |

---

## 7. 实施记录

### Phase 0：安全网（已完成，2026-10-04）

- `scripts/dup_scan.py`：滑动窗口重复扫描，支持 `scan` / `baseline` / `check` 三种模式；`make dup-check` 对比 `scripts/dup_baseline.json`，非测试重复行新增超过容差或 clippy allow 预算上涨即失败。
- 基线：非测试重复 **4,157 行**；`too_many_arguments` **62** 处、`type_complexity` **5** 处（只降不升）。
- 编译门禁：`cargo check --workspace --all-targets` 干净。

### Phase 1：零风险机械去重（已完成，2026-10-04）

实测与设计表的差异：

| 项 | 实际情况 |
|---|---|
| zone map 聚合 | writer.rs 私有副本删除，改调 partition/pruning.rs 公开版（-53 行） |
| null_for_dtype | scalar_tag.rs 委托 `ScalarValue::null_for_type`（-22 行） |
| decimal_multiplier | write.rs 副本删除，helpers.rs 版提为 `pub(super)` |
| arg_column_type | aggregate_fast.rs 副本删除，aggregate.rs 版提为 `pub(super)` |
| metadata_float32/64 | `FloatMeta` trait（含 `Bits` 关联类型保 bits 语义）合一 |
| ChunkView 双份 | `impl_chunk_view!` 宏；结构性合并留给 hot_store 拆分 |
| SpillCursor | 合一为 `SpillCursor`，两组 payload 解码保留为 `current_group_payload` / `current_distinct_key` |
| DDL 锁重试 | **实为 5 处**（drop/rename/partition/set hot/truncate），非设计时的 3 处。因各处 op 闭包借用 catalog guard 内容，guard 按值传 helper 会触发 E0505，故提取 `try_or_block_table_mutation_lock`（升级尝试）+ `warn_table_mutation_lock_busy`（告警节奏），guard 的 drop/sleep/重取留在调用点；整循环结构化留 Phase 4 |
| in-lock re-check | **DML 三处代码早已收敛**到 `txn_dml::refuse_auto_commit_dml_while_pinned`（仅注释相似），无需改动。compactor 两臂提取 `commit_guards_block`（7 参数避免新增 too_many_arguments，清理动作留调用点） |
| collect_table_dirs | backup.rs 与 migrate.rs 的整函数副本移入 `tools::segment` |
| handler prepared 查找 | 提取 `take_prepared_statement`（参数化 warn 上下文）与 `reject_if_schema_changed`（`std::result::Result` 区分通过/拒绝响应） |
| 顺手修复 | examples/batch_import 的 clamp-like clippy 告警 |

**验证结果**：`cargo test --workspace` 65 套件 **1,149 测试全部通过**；`cargo clippy --workspace --all-targets` **零告警**；重复扫描 **4,157 → 3,744 行（-413）**；allow 预算不变；diff **+345/-557（净 -212 行）**。

### Phase 2：消灭已漂移的重复（已完成，2026-10-04）

| 项 | 实施 |
|---|---|
| 标量比较 5→1 | `ScalarValue::compare_ordered(other, null_order)` 落在 types/scalar.rs（numeric 提升 → 同类型臂全覆盖 → Display 兜底保证全序，NaN 排在任意数之后）；`aggregate::compare_scalars`（null 在前）、`sort_key::compare_scalars_for_sort` / `sort::compare_scalars_null_last` / `query::union_scalar_cmp`（null 在后）全部改为薄委托。helpers.rs 的 `Option<Ordering>` 版是 SQL 谓词语义（None → 谓词假），**契约不同，保留** |
| 修复的潜伏 bug | aggregate 版缺 Bool/Time/Interval/Blob 臂 → spill 合并路径的 MIN/MAX 对这些类型返回 first-seen 值。新增 `spilled_min_max_merge_orders_bool_states` 单元测试**双向验证**（旧实现下 MIN 断言失败，新实现通过）；另加 `compare_ordered` 单元测试（null 位置、数值提升、u64 不回绕、NaN、非数值类型）与 SQL 级回归 |
| ScalarValue codec 3+3→1 | 新建 `protocol/scalar_codec.rs`（`encode_scalar_param` + `decode_param_value`），client/handler 均委托。调查确认：client 的文本 Decimal 分支是死代码（唯一调用点 scale 恒为 0 且 Decimal 走定宽路径），已删；wire 语义统一为严格 UTF-8（client 结果路径从 lossy 收紧，编码端只产生合法 UTF-8，故仅影响损坏数据的报错方式）。`scalar_tag` 的 19 值 tag 表改为 `DataType as u8` 判别值 + `from_u8` 回程校验（拒绝 ASCII 别名渗入存储格式） |
| 占位符 walker | 删除 query.rs 的残缺版（不遍历 FROM 派生表、Union 顶层子句、DML/Explain），入口检查与相关性标记分类均改用 `prepared::has_placeholders` / `expr_contains_placeholder`。入口检查变严格：派生表中的 `?` 现在会被正确拒绝 |
| txn_dml unwind | 两段逐字相同的 staged tombstone 注册-撤销循环提取为 `apply_staged_tombstones` |
| DML 公共前置 | partition-key 拒绝 + SET 子查询解析提取为 `txn_dml::plan_update_assignments`（auto-commit 与 in-txn UPDATE 共用） |
| footer 类型锁步 | `run_fold_type_ok` 委托 `column_sum_supported`（读写两侧类型集合无法再漂移） |
| 顺带 | `dml_insert::constraint_insert_lock` 调用点不变；修复 client.rs 脚本误删造成的 30 行重复后全量重验 |

**未做（推迟到 Phase 4 前的独立批次）**：planner/expr.rs 三个 AST→LogicalExpr 转换器合一（~350 行，需要 Resolver 抽象设计，风险高于本批，单独成批）。

**验证结果**：`cargo test --workspace` 65 套件 **1,150 测试全部通过**（+1 为新锁定测试）；`cargo clippy` **零告警**；重复扫描 **3,744 → 3,608 行（本阶段 -136，两阶段累计 -549）**，基线已收紧至 3,608；本阶段 diff **+399/-1,004（净 -605 行）**。

下一阶段入口：Phase 3（include! → 真模块，五处拼接宿主：engine、segment_manager、ddl、dml_insert、array/analytics）。

### Phase 3：include! → 真模块（已完成，2026-10-04）

五处拼接宿主全部改为真 `mod` 声明（version.rs 的 `include!(concat!(env!("OUT_DIR")...))` 是 build 脚本生成代码的合法用法，保留）：

| 宿主 | 改造 | 备注 |
|---|---|---|
| types/array.rs | `mod analytics;` + `pub use analytics::{ArithOp, CompareOp};` | 最简单：纯 impl 块，`use super::*` 即无缝 |
| dml_insert.rs | `mod {fast, write, constraints};` + 显式 pub/pub(crate) re-export | 外部约 15 个 `dml_insert::X` 路径保持不变 |
| ddl.rs | `mod {table, database_auth, introspection};` + `pub use xxx::*` glob | glob re-export 取 min(pub, 原项) 可见性，与 include! 时代完全一致；外部约 30 个 `ddl::exec_*` 路径不变 |
| engine.rs | `mod {core, runtime, bulk};` + `pub use runtime::HotFeedGuard;` | 混缩进的 impl Engine 块原样保留；68 处 `super::X` 改为 `crate::sql::execution::X` |
| segment_manager.rs | `mod {manager_core, buffered_write, direct_bulk, lifecycle};` + glob | 子文件全是 impl 方法 + crate 路径，最干净；测试模块挪到文件尾 |

**通用手法**（后续 Phase 4 拆文件可复用）：
- 子文件 prelude `use super::*;` 继承宿主命名空间（私有项对后代模块天然可见）；
- 子文件间共享：直接 `use super::兄弟::*`（比经宿主 glob 中转可靠——实测 glob 链传递在某些方向上不生效）；
- 跨文件共享的私有项提升 `pub(super)`（不泄漏到 crate 外）；
- 宿主对子模块 glob `pub use`（原可见性保持）供外部路径兼容。

**可见性提升清单**（全部 `pub(super)` 或测试需要的 `pub(crate)`）：dml_insert 17 个跨文件 fn、ddl 的 `get_existing_catalog`/`exec_partition_change`/Analyze 直方图项、engine 的 `begin_operation`/`apply_hot_resident_mark`、segment_manager 的锁辅助三件套 + `PendingSeal`/`SealingToken` 及其字段 + 11 个跨文件方法。

**验证结果**：`cargo test --workspace` 65 套件 **1,150 测试全部通过**（第一次跑有一个时序敏感测试 `optimize_heavy_phase_keeps_target_partition_writable` 在高负载下超时，单独重跑与全量重跑均通过——该测试断言真实墙钟 1.5s 时限，与本次纯结构改造无关）；`cargo clippy --workspace --all-targets` **零告警**；重复扫描 3,608 → 3,595 行，基线收紧至 3,595；三阶段累计 diff **+639/-1,162**。

**收益**：每个子文件从此是独立编译单元（IDE 导航、增量编译、按模块 clippy 生效）；测试可挂在真子模块上；后续 Phase 4 的文件拆分不再受"共享父模块私有项"牵制。

下一阶段入口：Phase 4（巨型函数拆解 + 模块边界重划，按 git churn 排序：execute_stmt 分发 → query.rs 拆三 → dml_delete 的 REPLACE 机器归位 → planner join-order 迁 optimizer → durable_tail 八模块）。

### Phase 4：巨型函数拆解 + 模块归位（进行中，2026-10-04）

| 项 | 实施 | 提交 |
|---|---|---|
| execute_stmt 分发瘦身 | InsertSelect(79行)/ReplaceSelect(44行)/Insert/Replace/Update/Delete/Begin/Commit/Rollback/Use 提取为 `exec_xxx_stmt` 方法；SHOW PARTITIONS 与 FLUSH TABLE 共享 `resolve_optional_table_target`。分发体 ~464 → 246 行，全部 match 臂单行化 | d761776 |
| REPLACE 机器归位 | dml_delete.rs 中 ~470 行 REPLACE 语义机器迁 `dml_insert/replace.rs`。**边界修正**：侦察发现 exec_delete 共用 ReplaceDeleteUndo/plan/apply_replace_memtable_conflicts，这三项及其 helper 留在 dml_delete（字段提升 pub(crate)）；`replace_pruning_test` 挂载点随迁。dml_delete 1,989 → 1,518 行 | 13fe64a |
| join-order 归位 | physical/planner.rs 约 1,250 行优化职责（内连接树重排 DP/greedy、基数/NDV 估计、表达式重映射家族、289 行计划画像估计）迁 `sql/optimizer/join_order.rs`；planner 经 `reorder_inner_join_trees`/`estimate_plan_rows` 两入口调用，explain.rs 改路径，join_order_test.rs 随迁（git mv）。planner 2,334 → 1,067 行 | 43bf3a3 |
| helpers 拆四 | 1,635 行拆 `helpers/{eval, scalar_utils, scan_predicates, change_events}.rs` + glob re-export（`helpers::` 路径不变，调用方零改动）。子文件自持 use，跨文件项显式导入 | 24557cf |

**剩余项**：query.rs 拆三（select_exec/derived/correlated，含三个子查询模块合一）、durable_tail 八模块拆分、scan/aggregate_meta 的 923 行 `compute_folded_aggregate` 拆解。
