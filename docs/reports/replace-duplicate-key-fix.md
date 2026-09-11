# REPLACE 撞历史重复主键报错修复报告

## 现象

业务端使用 `REPLACE INTO` 写入主键表时,报错:

```
duplicate unique key '__taibai_primary_location'
```

且表内已存在历史重复主键行(bulk 导入时代绕过校验写入)。

## 根因

位置索引快照的"新鲜度"判断依赖 **flush epoch**——只在 memtable flush 落段时递增。
但直写段路径(direct bulk、RELAXED 直写)不经过 memtable,epoch 不变:

1. `CREATE TABLE` 时持久化一个**空但有效**的 `__taibai_primary_location` 快照;
2. 直写段写入行,epoch 不动,快照内容与段内行数脱节;
3. `REPLACE` 的 `ensure_replace_location_indexes` 只检查"快照是否存在",存在即跳过重建
   → 查不到任何冲突行位置 → 删除 0 行、照常插入新行;
4. 同一空快照使 `unique_key_exists` 对 INSERT 校验返回"不存在"
   → 重复主键被静默放行,持续产生新脏数据;
5. 旧版 `collect_unique_index_entries` 遇到段内重复 key 直接报
   `duplicate unique key '__taibai_primary_location'`——即客户看到的错误。

## 修复

将 flush epoch 泛化为**段内容 epoch**:任何绕过 memtable 的直写落段都递增同一计数器,
使现有 `refresh_stale_unique_index_state` 增量追补生效(重复 key 的多个位置全部保留)。

| 文件 | 改动 |
|---|---|
| `storage/segment_manager/manager_core.rs` | 新增 `note_direct_segment_append`,复用 epoch 持久化 |
| `storage/segment_manager/buffered_write.rs` | 两条 bulk 写入路径落段后递增 epoch |
| `storage/segment_manager/direct_bulk.rs` | 直写 payload(含 legacy 入口)落段后递增 epoch |
| `storage/index/index_manager.rs` | `unique_key_exists` 补 staleness 守卫(与 `unique_key_locations` 一致) |
| `sql/execution/dml_insert/constraints.rs` | INSERT 快路径改为批量 `unique_keys_exist`(带刷新) |
| `sql/execution/dml_insert/write.rs`、`dml_delete.rs`、`ddl/table.rs`、`engine/bulk.rs` | `collect_unique_index_entries` 容忍历史重复;REBUILD 重建保留全部位置;bulk 校验主键并失效快照 |

### 行为保证

- **REPLACE 永不再因历史重复键报错**,且替换范围=全部冲突行(重复历史行全部删除,同批次重复保留最后一条);
- INSERT 对历史脏数据中的重复键**恢复拒绝**,不再产生新脏数据;
- 压缩(`OPTIMIZE TABLE`/自动 compact)与重命名等路径原有的快照失效逻辑不受影响。

### 防线纵深

1. 段内容 epoch(本修复):一切直写可被快照感知;
2. 引擎级 bulk 完成后主动失效快照(本工单既有改动);
3. INSERT 校验 staleness 守卫(本修复):即使前两层失效也不放过重复键。

## 回归测试

- `replace_replaces_all_preexisting_duplicate_primary_key_rows`
- `replace_replaces_all_preexisting_duplicate_unique_index_rows`
- `insert_rejects_duplicate_primary_key_after_unvalidated_bulk_write`

## 性能影响

每个 bulk 批次新增一次数字节 epoch 文件写;查找路径仅多一次内存比较,无新增目录扫描。
