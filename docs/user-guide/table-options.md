# 建表选项 SQL 参考

本文描述 `CREATE TABLE` 支持的表选项，以及对应的 `ALTER TABLE` 维护语句。语法以当前实现为准（`src/sql/parser/ddl.rs`）。列定义、数据类型与基础用法见[快速开始](../getting-started/quick-start.md)。

## 语法总览

```sql
CREATE TABLE [IF NOT EXISTS] 表名 (
  列名 类型 [列约束],
  ...
)
[CLUSTER BY (列, ...)]
[PARTITION BY ...]
[TTL 列, 秒数]
[HOT BY (时间列) [INDEX(列)] [CLOSE AT 'HH:MM']]
[DURABILITY = STRICT | RELAXED]
[COMPRESSION_LEVEL = 级别]
```

表选项写在列定义之后，顺序不限，每个选项最多出现一次。`=` 在 `DURABILITY` 和 `COMPRESSION_LEVEL` 中均可省略。

## COMPRESSION_LEVEL（按表 zstd 压缩级别）

控制该表列数据落盘时的 zstd 压缩级别，取值范围 `1..=22`：

- 未设置时使用全局默认 `compression.bulk_zstd_level`（默认 `3`，见 `conf/example.yaml`）。
- 归档类表建议调高（如 `19`）换取更小体积；高频写入的热表建议调低（如 `1`）降低 CPU 消耗。

```sql
CREATE TABLE archive_ticks (
  symbol VARCHAR,
  ts TIMESTAMP,
  price FLOAT64,
  volume INT64
) COMPRESSION_LEVEL = 19;
```

事后调整与恢复全局默认：

```sql
ALTER TABLE archive_ticks SET COMPRESSION_LEVEL = 22;
ALTER TABLE archive_ticks SET COMPRESSION_LEVEL = DEFAULT;
```

行为说明：

- 级别越界（小于 1 或大于 22）会在建表和修改时报错：`COMPRESSION_LEVEL must be between 1 and 22, got n`。
- 修改只影响之后写入以及 `OPTIMIZE TABLE` 重建的 segment；已落盘数据不会立即重压缩。
- `SHOW CREATE TABLE` 会回显该选项；`RENAME TABLE` 保留该设置。
- 普通列式 bulk 与二进制直写协议均按表级别生效。

## CLUSTER BY

按指定列排序存储，加速范围扫描与过滤裁剪：

```sql
CREATE TABLE trades (
  symbol VARCHAR,
  ts TIMESTAMP,
  price FLOAT64
) CLUSTER BY (symbol, ts);
```

## PARTITION BY

按分区表达式切分 segment，支持 `DAY(col)`、`MONTH(col)`、`YEAR(col)`、`MOD(col, n)`、`HASH(col[, n])`（省略桶数时默认 10）以及按列值分区：

```sql
CREATE TABLE ticks (
  ts TIMESTAMP,
  price FLOAT64
) PARTITION BY DAY(ts);

CREATE TABLE users (
  id INT64,
  name VARCHAR
) PARTITION BY HASH(id, 8);
```

## TTL

基于时间列的过期清理，时长为正整数秒。以下写法等价：

```sql
CREATE TABLE events (
  ts TIMESTAMP,
  payload VARCHAR
) TTL ts, 86400;

CREATE TABLE events (
  ts TIMESTAMP,
  payload VARCHAR
) TTL(ts, 86400);
```

修改与删除：

```sql
ALTER TABLE events SET TTL ts, 3600;
ALTER TABLE events DROP TTL;
```

## HOT

将指定时间列附近的数据常驻热存，可选二级索引列与收盘时间：

```sql
CREATE TABLE md_snapshot (
  ts TIMESTAMP,
  symbol VARCHAR,
  bid FLOAT64
) HOT BY (ts) INDEX(symbol) CLOSE AT '15:00';
```

`HOT KEEP N DAYS` 语法已不再支持，解析器会明确报错。热存数据在重启后走惰性恢复（只登记索引不物化数据）。

## DURABILITY

控制写入持久化级别：

```sql
CREATE TABLE metrics (
  ts TIMESTAMP,
  v FLOAT64
) DURABILITY = RELAXED;
```

`STRICT` 为默认严格落盘语义；`RELAXED` 放宽落盘时机以换取吞吐。持久化边界见[事务与并发语义](transactions-and-concurrency.md)。

## ALTER TABLE 维护语句汇总

| 语句 | 作用 |
| --- | --- |
| `ALTER TABLE t SET COMPRESSION_LEVEL = n` | 按表调整 zstd 压缩级别（1..=22） |
| `ALTER TABLE t SET COMPRESSION_LEVEL = DEFAULT` | 恢复全局默认级别 |
| `ALTER TABLE t SET TTL 列, 秒数` | 设置或调整 TTL |
| `ALTER TABLE t DROP TTL` | 移除 TTL |
| `ALTER TABLE t SET HOT BY (时间列) [INDEX(列)] [CLOSE AT 'HH:MM']` | 设置热存 |

## 相关文档

- [数据导入流程说明](../operations/data-import-flow.md)：压缩级别在 bulk 导入路径中的生效方式。
- [示例工具使用指南](../operations/examples-guide.md)：批量导入工具用法。
