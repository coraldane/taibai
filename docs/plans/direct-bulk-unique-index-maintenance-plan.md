# Direct Bulk 主键与唯一索引增量维护方案

## 1. 问题

当前 direct bulk 写入在落盘前执行主键和唯一约束检查，但写入成功后会删除或失效相关索引。下一批写入无法命中索引，只能重新遍历全部历史 segment 和 row group。

对于每天导入固定行数的表，第 N 批需要处理当前批次并扫描前 N-1 批历史数据，单批成本随总数据量线性增长，连续导入的累计成本趋近 O(N²)。

现有实现还存在并发正确性风险：direct bulk 的“约束校验”和“segment 写入”没有统一由 `constraint_insert_lock` 覆盖。两个并发写入可能同时确认同一个键不存在，然后分别写入重复键。

## 2. 目标

1. 主键和唯一约束不能因性能优化而弱化。
2. 稳态 direct bulk 每批成本只与当前批次规模和索引探测成本相关，不与历史总行数线性相关。
3. 写入成功后增量维护索引，不删除完整快照。
4. 进程崩溃、索引更新失败或旧版本升级后能够自动恢复，不漏报历史冲突。
5. 普通 INSERT、REPLACE 和 direct bulk 使用相同的约束串行化与索引生命周期语义。
6. 不改变 segment 数据格式和 bulk 协议。

## 3. 非目标

1. 第一阶段不引入全新的 LSM/B+Tree 索引引擎。
2. 第一阶段不改变 SQL 约束语义。
3. 第一阶段不允许通过 bulk 模式跳过主键或唯一约束检查。
4. 第一阶段不依赖后台任务才能恢复正确性。

## 4. 最佳方案

复用现有唯一索引快照、delta、segment watermark 和 flush epoch 基础设施，将 direct bulk 改造成完整的增量索引生命周期：

```text
同表约束写锁
  → 解析并编码当前批次键
  → 检查批次内重复
  → 加载或增量刷新持久化索引
  → 批量探测历史冲突
  → 写入 segment
  → 推进 segment-content epoch
  → 追加本批索引 delta
  → 发布新 watermark/epoch
  → 释放锁
```

正常路径不再删除索引文件。索引不可用、损坏或版本不兼容时才执行一次全量重建；重建完成后后续批次继续增量维护。

## 5. 索引模型

### 5.1 唯一的持久化模型

主键和显式唯一索引统一使用现有 `UniqueIndexState` 模型：

- base 快照保存已编码键和位置；
- delta 保存后续追加和 tombstone；
- `segment_watermarks` 记录每个 segment 已被索引覆盖的行数；
- `flush_epoch` 标识索引覆盖的 segment 内容版本；
- delta 达到阈值后在后台线程压实回 base（阈值按条目数对 base 大小摊还并设绝对上限，导入路径只追加 delta）。

内存 `primary_key_indexes: HashSet` 只作为热点缓存，不能作为主键正确性的唯一索引。重启后应从持久化的 `PRIMARY_LOCATION_INDEX_NAME` 状态加载或探测，而不是回退为全表扫描后只安装内存 HashSet。

### 5.2 键编码

当前批次应直接构建 `Vec<PreparedUniqueIndexEntry>` 或等价的编码键集合，避免在多个阶段重复创建和克隆 `Vec<ScalarValue>`。

每个约束一次遍历完成：

1. NULL 和主键完整性验证；
2. 键编码；
3. 批次内去重；
4. 生成索引增量条目。

主键和具有相同列集合的显式唯一索引可以共享本批编码结果。

## 6. 一致性与故障恢复

### 6.1 锁顺序

所有带主键或唯一约束的 INSERT、REPLACE 和 direct bulk 必须遵循统一锁顺序：

```text
constraint_insert_lock(db, table)
  → SegmentManager table mutation lock
  → index state mutation lock
```

约束检查到数据写入完成之间必须持有 `constraint_insert_lock`。否则两个并发请求可能产生 check-then-write 竞态。

不得在持有 index state mutation lock 时反向获取表 mutation lock。

### 6.2 写入与索引不是单文件原子提交

segment 追加和索引 delta 不能通过普通文件操作形成一个原子事务，因此采用“数据权威、索引可验证并自愈”的顺序：

1. 在旧索引状态上完成冲突检查；
2. 写入并使 segment 数据可恢复；
3. 持久化并推进 segment-content epoch；
4. 追加索引 delta；
5. 将索引状态更新到新 epoch 和 watermark。

如果在步骤 2 后、步骤 5 前崩溃：

- segment 数据已经存在；
- segment-content epoch 或实际 watermark 与索引不一致；
- 下次探测必须先执行增量刷新；
- 刷新只扫描 watermark 之后的新行，而不是扫描全部历史行。

如果 epoch 文件持久化失败，当前代码只记录日志并继续是不够安全的。约束表 direct bulk 应将 epoch 持久化失败视为写入后索引状态不确定：

- 保留内存 epoch 前进；
- 持久化一个表级 `index-recovery-required` 标记；
- 当前请求返回包含可操作上下文的错误；
- 后续约束写入必须先根据 segment watermark 恢复索引。

更稳妥的后续演进是把 epoch 与索引发布记录写入带 checksum 的原子 replace 文件，但不要求第一阶段改变数据格式。

### 6.3 索引增量追加失败

segment 已成功写入后，索引 delta 追加失败不能回滚为“数据不存在”。处理规则：

1. 标记对应索引 stale，而不是删除 base 和 delta；
2. 保留旧 watermark；
3. 返回错误并说明数据已经持久化、索引将在下次访问时恢复；
4. 下一次写入在约束检查前调用增量刷新；
5. 增量刷新成功后再接受新写入。

旧 base 快照必须保留，因为它能把恢复成本限制为未覆盖的 segment 尾部。

## 7. Direct Bulk 改造

### 7.1 写入前

`Engine::exec_bulk_insert_payload_with_options` 应在发现主键或唯一约束后：

1. 获取 `constraint_insert_lock`；
2. 将 payload 转换或投影为约束列；
3. 构建一次编码键集合；
4. 确保对应持久化索引存在且已刷新到当前 epoch；
5. 使用批量探测接口检查冲突。

当前 `direct_bulk_request_to_batch` 会物化全部列。后续应增加约束列投影解析，只物化主键、唯一键和自增列；第一阶段可先保留全 batch 物化以降低改动风险。

### 7.2 写入后

删除以下正常路径行为：

- `remove_table_unique_index_snapshots`；
- direct bulk 后无条件 `invalidate_primary_key_index`。

替换为：

1. 从 segment append 返回新行的真实位置范围，或者依赖新 watermark 增量刷新；
2. 向主键和每个唯一索引追加准备好的编码条目；
3. 更新索引的 segment watermark 和 flush epoch；
4. 内存主键缓存存在时同步追加，不存在时不强制构建第二份全量 HashSet。

最佳实现是让 segment append 返回：

```text
Vec<SegmentAppendRange> {
    segment_path,
    start_row,
    row_count
}
```

这样索引 delta 可以直接记录真实 `UniqueRowLocation`，无需使用空路径伪位置，也无需随后扫描刚写入的 row group。

分区表一次请求可能写入多个 segment，因此返回值必须是范围列表，不能只返回单个起始行。

### 7.3 Bulk 会话

`START BULK IMPORT` 不需要维护另一套会话级权威索引。持久化索引状态和同表约束锁已经足以保证多连接正确性。

可选优化是在会话中缓存：

- schema 到约束列索引的映射；
- 编码器；
- 已加载的索引 state 引用。

不能把跨批次新增键只留在 session 私有 HashSet 中，因为其他连接无法看到，且连接中断会丢失索引状态。

## 8. Segment 枚举优化

主问题修复后，仍应消除 segment 数增长带来的目录扫描：

1. 在 `TableSegmentState` 中维护已排序 segment 清单和最大 sequence；
2. 创建、seal、compaction、删除 segment 时增量更新；
3. 启动或首次访问时扫描一次目录恢复；
4. `next_segment_path` 使用持久化/内存 sequence，不再扫描目录；
5. 唯一索引增量刷新优先比较索引 watermark 与 segment registry。

该项属于 P1，不能替代索引生命周期修复。

## 9. 实施阶段

### Phase 0：可观测性与回归基线

为 direct bulk 增加阶段 profiling：

- payload parse；
- constraint materialize/encode；
- batch duplicate check；
- index load/refresh；
- index probe；
- historical fallback scan；
- compression；
- segment append；
- index delta append；
- total。

增加计数器：

- full index rebuild count；
- incremental refresh rows；
- segment files visited；
- index delta bytes；
- stale recovery count。

### Phase 1：停止重复全量重建

1. direct bulk 获取与普通 INSERT 相同的约束锁；
2. 写入成功后调用 `append_primary_keys` 和 `append_unique_index_entries`；
3. 不再删除索引快照；
4. stale 时保留 base 并使用 watermark 增量刷新；
5. 为主键建立并持久化 `PRIMARY_LOCATION_INDEX_NAME` 索引；
6. 补充崩溃和并发测试。

该阶段应立即把稳态单批复杂度从历史行数相关降为当前批次相关。

### Phase 2：真实位置和统一索引路径

1. segment append 返回 `SegmentAppendRange`；
2. direct bulk 为新增键发布真实位置；
3. 主键探测统一走持久化唯一索引；
4. 内存主键 HashSet 降级为可选缓存；
5. 避免主键索引与 primary location index 双份全量状态。

### Phase 3：CPU 和内存优化

1. direct payload 只投影约束列；
2. 一次编码同时完成去重和 delta 构造；
3. 批量有序探测索引 block，避免每个 key 独立随机查找；
4. 对编码键排序并去重后执行 merge probe；
5. segment registry 消除重复目录枚举。

## 10. 测试

### 10.1 正确性

1. 单批主键内部重复必须失败且不写入。
2. 与历史 segment 主键重复必须失败。
3. 连续多个 bulk 批次之间重复必须失败。
4. 两个连接并发写入相同主键，只能一个成功。
5. 显式唯一索引覆盖以上同类场景。
6. 复合主键、NULL 唯一键语义与普通 INSERT 一致。
7. 分区表跨 partition 重复仍必须失败。
8. REPLACE、DELETE、UPDATE 后索引位置和 tombstone 正确。

### 10.2 故障注入

在以下位置模拟进程终止或 I/O 错误：

1. segment append 前；
2. segment append 后、epoch 前；
3. epoch 后、delta 前；
4. delta 追加中；
5. delta 后、watermark 发布前；
6. delta 压实过程中。

重启后必须满足：

- 已持久化数据不会丢失；
- 不会漏报重复键；
- 恢复只扫描 watermark 未覆盖范围；
- base 索引不会因单次失败被无条件删除。

### 10.3 性能

固定每日约 124 万行，连续导入至少 60 批：

- 第 60 批约束检查耗时不得随前 59 批总行数线性增长；
- 稳态单批耗时相对第 2 批漂移目标不超过 15%；
- `full index rebuild count` 在首次构建后保持为 0；
- 每批 `incremental refresh rows` 不超过本批新增行数加未完成恢复尾部；
- 内存峰值不随历史总行数线性增长；
- 重启后的首批不得全表扫描。

同时对无约束表做回归，确保 direct bulk 写入吞吐不下降。

## 11. 验收标准

1. 删除 direct bulk 正常成功路径中的索引快照删除操作。
2. direct bulk 的校验、写入和索引发布受同一表约束锁保护。
3. 稳态每批不调用 `collect_unique_index_entries` 或 `load_existing_unique_keys` 全量扫描。
4. 索引落后时只扫描 watermark 之后的数据。
5. 服务重启后索引仍可直接加载或增量恢复。
6. 60 批固定规模导入无随历史行数线性退化。
7. 并发、故障注入和现有 INSERT/REPLACE/DELETE/UPDATE 测试全部通过。

## 12. 结论

最佳方案不是关闭约束，也不是在 bulk 会话内临时缓存全部历史主键，而是把 direct bulk 接入现有持久化唯一索引的完整生命周期：写前批量探测、写后追加 delta、用 epoch 和 watermark 处理崩溃窗口，并用统一的同表约束锁消除并发竞态。

该方案保留现有 segment 格式和协议，复用已经存在的索引增量与刷新能力，改动风险可控，并能同时解决性能退化、重启首批全扫和并发重复键三个问题。

## 13. 实施补充：增量刷新的 delta 追加快路径（已落地）

问题：接入增量生命周期后，direct bulk 每批仍触发一次 `refresh_stale_unique_index_state` 的完整物化与快照重写（内存物化 base+delta、编码、写临时文件、fsync、rename），单批成本随表内唯一键总数线性增长，导致连续导入逐批变慢。

落地的改动：

1. `UniqueIndexState` 的刷新进度从 base 快照中分离为独立的 `UniqueIndexProgress`（flush epoch + 各 segment watermark），持久化为 `{name}.progress.tbi` sidecar，重启后与 base+delta 合并加载，崩溃或截断时回退为从 watermark 0 全量重扫，保证不漏报。
2. `refresh_stale_unique_index_state` 增加 `allow_delta_append` 门控：
   - direct bulk 写后通过 `IndexManager::refresh_unique_index_state` 走快路径：只扫描 watermark 之后的新 segment 尾部，把新增条目追加到 `{name}.delta.tbi` 并发布新 progress，不再重写 base 快照；delta 超过按条目数摊还的压实阈值时派发后台压实，合并计算不持锁，发布阶段仅短暂持锁，合并窗口内的新增追加以文件后缀形式保留在新 delta 中，不在导入路径上同步重写 base。
   - 普通探测路径（`unique_keys_exist` / `unique_key_locations`）保持完整物化行为，继续折叠同一键的重复 memtable 占位条目，维持原有清理不变量。
3. direct bulk 写后不再用批量 `unique_keys_exist` 探测来顺带触发刷新，而是直接调用 `refresh_unique_index_state`，消除每批对百万级键的无谓探测。

效果：稳态 bulk 批次不再重写与表内键总数成正比的快照文件，索引维护成本只与本批新增行数相关。结构性回归测试 `bulk_payload_refresh_appends_delta_without_rewriting_base_snapshot` 断言多批导入期间 base 快照字节不变、delta/progress 持续追加、唯一约束冲突仍可检出。
