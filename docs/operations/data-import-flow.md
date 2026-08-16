# Taibai 数据导入流程说明

本文基于当前已实现代码整理，重点说明 Taibai 现有数据导入入口、服务端处理链路、落盘流程，以及检查时发现的实现限制。

## 导入方式总览

当前工程里主要有三类导入方式：

| 方式 | 入口 | 协议/语句 | 主要用途 |
| --- | --- | --- | --- |
| 普通 SQL INSERT | `Client::query` / `Client::execute` | `MSG_TYPE_QUERY` + `INSERT INTO ... VALUES ...` | 小批量写入、功能验证、兼容 SQL |
| Pipeline SQL INSERT | `examples/multi_insert` | 多个 `MSG_TYPE_QUERY` 请求并发在途 | CSV 到多值 INSERT，适合中等批量 |
| 列式二进制 Bulk Import | `Client::bulk_insert_columnar` / `pipeline_bulk_insert_columnar` / `examples/batch_import` | `MSG_TYPE_BULK_INSERT_COLUMNAR` | 当前最高性能路径，绕过 SQL 解析 |

此外，`START BULK IMPORT` / `STOP BULK IMPORT` 是一种会话级模式开关：

- `START BULK IMPORT` 会设置服务端 bulk mode。
- `STOP BULK IMPORT` 会关闭 bulk mode，并调用 `flush_all_buffers()` 刷新 MemTable 与 active segment。
- SQL INSERT 和列式二进制 bulk 都可能在 bulk mode 下运行，但两者走的内部路径不同。

## 端到端导入链路

```text
客户端 CSV/RecordBatch/SQL
  -> Client 编码为 QUERY 或 BULK_INSERT_COLUMNAR 消息
  -> protocol::codec 写入消息头和 payload
  -> net::handler 分发消息
  -> Engine 权限、catalog、bulk mode 检查
  -> SQL INSERT 路径或二进制 bulk 路径
  -> SegmentManager 写 active segment / MemTable
  -> 查询时从 MemTable + segment 合并读取
```

## SQL INSERT 路径

### 客户端侧

`examples/multi_insert` 使用 CSV 文件生成多值 SQL：

```sql
INSERT INTO stock_tick_multi VALUES (...), (...), ...
```

它通过 `Client::send_execute()` 连续发送 SQL 请求，并用 `pipeline_depth` 控制在途请求数。开启 `--bulk` 时，会先执行：

```sql
START BULK IMPORT
```

导入结束后再执行：

```sql
STOP BULK IMPORT
```

### 服务端侧

普通 SQL INSERT 的服务端路径如下：

```text
MSG_TYPE_QUERY
  -> Handler::handle_query
  -> Engine::execute
  -> Parser::parse 或 fast_insert::try_fast_parse_insert
  -> Engine::execute_stmt
  -> dml_insert::exec_insert
```

`exec_insert` 的主要工作：

1. 从 catalog 中读取表元数据和 schema。
2. 解析插入列并检查列数。
3. 处理 `AUTO_INCREMENT`、默认值、NULL 约束。
4. 将 AST 表达式逐值转为 `ScalarValue`，构造 `RecordBatch`。
5. 若表定义了 `CLUSTER BY`，会按聚簇键排序 batch。
6. 若表有主键，会检查本批与已有 segment 中的主键唯一性。
7. 根据是否处于 bulk mode 选择写入路径。

### SQL INSERT 非 bulk mode

非 bulk mode 下，写入走 `BatchWriter`：

```text
exec_insert
  -> write_batch_by_partition
  -> BatchWriter::write_columns_with_txn
  -> MemTableManager
  -> 到达阈值或定时 flush
  -> BatchWriter::flush_memtable
  -> SegmentManager::bulk_insert_batch[_with_partition]
```

特点：

- 数据先进入 MemTable，查询时会合并 MemTable 和 segment。
- 非事务写入达到 MemTable 阈值后异步 flush。
- 事务写入提交时 flush；回滚时丢弃对应 MemTable。
- catalog 的 `row_count` 会立即增加，并在非 bulk mode 下保存到元数据文件。

### SQL INSERT bulk mode

开启 `START BULK IMPORT` 后，`exec_insert` 直接调用：

```text
Engine::exec_bulk_insert
  -> SegmentManager::bulk_insert_batch
  -> SegmentManager::bulk_insert_batch_with_partition
```

特点：

- 绕过 MemTable，直接追加到 active segment。
- 仍然需要经过 SQL 解析、表达式求值、RecordBatch 构建、主键检查等 SQL 层成本。
- `multi_insert --bulk` 属于这种路径。
- catalog `row_count` 会在内存中增加，但 bulk mode 下不会每批保存 catalog；最终 `STOP BULK IMPORT` / `flush_all_buffers()` 会保存 catalog。

## 列式二进制 Bulk Import 路径

### 客户端侧

`examples/batch_import` 是当前主要的列式二进制导入示例。流程是：

```text
CSV 文件
  -> produce_batches 读取大块文件并手动切行
  -> parse_tick_csv_line_interned 解析字段
  -> ColumnarTickBatch 列式缓存
  -> encode_columnar_tick_batch_precise
  -> Client::pipeline_bulk_insert_columnar
```

默认行为：

- 表名：`stock_tick_batch`。
- 默认 batch size：`200000` 行。
- pipeline depth：`8`。
- 使用 `START BULK IMPORT`。
- 使用 `MSG_TYPE_BULK_INSERT_COLUMNAR` 发送列式 payload。
- 导入结束后先 `drain_pipeline()`，再 `stop_pipeline()`，最后 `STOP BULK IMPORT`。

### payload 格式

服务端由 `storage::bulk_payload::parse_direct_bulk_payload()` 解析 payload。当前格式如下：

```text
table_len: u16
table_name: [u8; table_len]
row_count: u32
column_count: u32

repeat column_count:
  name_len: u16
  name: [u8; name_len]
  data_type: u8
  scale: u8
  precision: u8
  elem_count: u32
  nulls_size: u32
  data: [u8; computed_data_len]
  nulls: [u8; nulls_size]
```

其中：

- 固定宽度列的 `computed_data_len = elem_count * type_width`。
- 变长列按每行 `len: u32 + bytes` 顺序编码。
- `data` 为 dense slot 布局：即使某行是 `NULL`，也保留该行的固定宽度占位值或变长值槽。
- `elem_count` 必须等于 payload 头部的 `row_count`。
- `nulls_size == 0` 表示整列无 NULL；否则 bitmap 使用小端 `u64` 字组，长度必须为 `ceil(row_count / 64) * 8`，第 K 位为 1 表示第 K 行是 NULL。
- 最后一个字组中超出 `row_count` 的 padding bit 必须为 0。
- 服务端解析变长列时会一次性建立每行的 `(offset, len)` 索引，后续分区/聚簇转换和统计信息计算可以按行 O(1) 访问。

### 服务端侧

列式二进制 bulk 的服务端路径：

```text
MSG_TYPE_BULK_INSERT_COLUMNAR
  -> Handler::handle_bulk_insert
  -> Engine::exec_bulk_insert_payload
  -> SegmentManager::bulk_insert_payload_with_catalog
```

`bulk_insert_payload_with_catalog` 的主要工作：

1. 解析二进制 payload。
2. 根据 catalog 找到表 schema。
3. 校验列数量、列名、列类型和 null bitmap；`NOT NULL`/主键列出现 NULL 时拒绝导入。
4. 分区或 row group 切片时同步重建每个子组的 null bitmap。
5. 落盘前把 wire 层的 dense slot 数据转换为 segment codec 所需的“bitmap + 仅非 NULL 值”布局。
6. 走直接列式写入：
   - 每列用 Rayon 并行 zstd 压缩。
   - 生成 row group 元数据。
   - 计算 Zone Map。
   - 追加到 active segment。
   - 写 meta footer 或达到 segment 大小后 seal。

## active segment 写入与落盘

`SegmentManager` 维护每张表的 active segment 状态。核心行为：

- 数据目录为 `{data_dir}/{db}/{table}`。
- 分区表会写入带 partition key 的 segment 文件。
- 写入前会尝试恢复未 seal 的 segment。
- 新 segment 先写 header。
- 每次批量写入生成一个或多个 row group。
- row group 记录：
  - 数据 offset。
  - 压缩后 length。
  - row_count。
  - 每列压缩大小。
  - 每列未压缩大小。
  - Zone Map。
  - Bloom Filter 占位，目前 bulk 直接路径写入 `None`。
- segment 大小达到阈值时 seal，写入 footer 和 footer offset。
- 未达到 seal 阈值时会刷新 meta，便于后续恢复未 seal segment。

bulk 压缩级别由配置文件 `compression.bulk_zstd_level` 控制，默认值为 `3`。

## 分区与聚簇处理

导入时的分区/聚簇逻辑分两类：

### SQL INSERT

- `exec_insert` 会先按 `CLUSTER BY` 排序当前 batch。
- `write_batch_by_partition` 根据表分区列生成分区 mask。
- 每个分区单独写入对应 active segment。

### 列式二进制 bulk

- 若表没有分区也没有聚簇键，则保持 DirectBulkRequest 的列式形态，直接压缩列数据写 segment。
- 若表存在分区或聚簇键，则先转成 `RecordBatch`：
  - 聚簇键排序需要逐行读取 DirectBulkColumn。
  - 分区拆分也需要逐行计算分区 key。
  - 因此这类表会退回 RecordBatch 写入路径，性能低于无分区/无聚簇的直接列式路径。

当前 `examples/batch_import` 创建的表包含：

```sql
partition by deal_date cluster by security_id
```

所以它会触发 DirectBulkRequest -> RecordBatch -> 排序/分区 -> batch 写入路径，而不是最短的直接列式压缩路径。

## WAL 与持久化语义

经典 WAL（`WalWriter`/`WalRecovery`，`table/wal/` 目录）已删除：它只写不读，启动恢复从不回放，没有恢复价值。当前持久化完全由 durable tail 与 active segment 自身承担：

- SQL INSERT 非 bulk mode：每批先写入 `memtable_tail/{memtable_id}.tail`（组提交 fsync），flush 时物化进未 seal 的 active segment（首次建段，之后原地追加 row group），完成后 tail 删除；崩溃后由 durable tail 重放。
- 显式事务：COMMIT 时物化 txn MemTable 进 active segment（数据与 meta 均 fsync），依赖 segment 自身持久化，不再写事务 WAL。
- SQL INSERT bulk mode / 列式二进制 bulk：直接写 active segment，当前无独立 WAL 写入。

因此 bulk 导入的可靠性依赖 active segment flush/meta/seal。批量导入采用单一持久化语义，不提供绕过持久化的协议开关。

## 查询可见性

导入后的查询可见性：

- SQL INSERT 非 bulk mode：数据先在 MemTable 中可见，扫描时会合并 MemTable 与 segment。
- SQL INSERT bulk mode：数据直接写 active segment，可通过 segment 扫描读取。
- 列式二进制 bulk：数据直接进入 active segment。
- `COUNT(*)` 不依赖 catalog 的 `row_count`，而是扫描 segment footer 和 MemTable 行数。

## 检查发现与注意事项

### 1. bulk payload 已支持 nullable columns

direct bulk 会保留固定宽度和变长列的 NULL，并在分区/row group 拆分后重新映射 bitmap。Zone Map 的 `null_count` 也随写入结果生成。协议要求使用上文定义的 canonical `u64` 字组 bitmap；畸形长度、越界 padding bit，以及向 `NOT NULL`/主键列写 NULL 都会返回协议错误。

### 2. `net::bulk_importer::encode_columnar_bulk_insert` 协议字段语义已修复

服务端期望 column header 中的 `elem_count` 是行数。`src/net/bulk_importer.rs` 的通用 helper 已改为写入 `batch.num_rows()`，并增加了解析回归测试，避免固定宽度列把字节数误写成行数导致：

```text
column N row count mismatch
```

`examples/batch_import` 自己实现的编码函数传入的也是行数，因此示例路径和通用 `BulkImporter` 路径现在保持一致。

### 3. bulk mode 下 catalog 行数更新

SQL INSERT bulk mode 会在内存 catalog 中增加 `row_count`，最终由 `flush_all_buffers()` 保存。列式二进制 bulk 也按当前统一语义更新 catalog 行数。

这不影响 `SELECT COUNT(*)`，因为 count 会读 segment footer；但会影响 `SHOW STATS` 这类读取 catalog `row_count` 的管理信息。

### 4. 分区/聚簇表无法使用最短直接列式路径

只要表定义了 `PARTITION BY` 或 `CLUSTER BY`，列式 bulk 会先转换成 `RecordBatch` 并进行排序/分区拆分。这保证语义正确，但会牺牲一部分 bulk 直接写入的性能优势。

### 5. bulk 直接路径的 Bloom Filter 目前为空

bulk 写入 row group 时会计算 Zone Map，但 `bloom_filters` 写入为 `None`。因此等值查询仍可受益于 Zone Map 和后续过滤，但不能依赖 bulk 写入时生成的 Bloom Filter。

## 推荐使用方式

开发调试：

```text
普通 SQL INSERT 或 examples/multi_insert
```

需要 SQL 兼容、可读性和中等吞吐：

```text
examples/multi_insert --bulk --pipeline=<n>
```

生产型可重放大批量导入：

```text
examples/batch_import
```

对最高吞吐更敏感时，建议：

- 尽量使用列式二进制 bulk。
- 调大 batch size 和 pipeline depth，但不要超过 `Client::start_pipeline` 当前上限 16。
- 对无需查询裁剪的导入阶段，可通过配置文件调低 `compression.bulk_zstd_level`，减少 CPU 消耗；默认值为 `3`。
- 若希望直接列式路径收益最大，优先导入无分区/无聚簇表，或后续再执行整理/优化。

## 后续优化建议

1. 在列式 bulk 直接路径中为适合的列生成 Bloom Filter。
2. 对分区/聚簇 bulk 做流式分桶和局部排序，减少重排开销。
3. 在 `STOP BULK IMPORT` 或 bulk 写入结束时同步更新 catalog `row_count`，避免 `SHOW STATS` 与实际 segment 行数不一致。
