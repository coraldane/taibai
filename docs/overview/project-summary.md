# Taibai 工程主要功能总结

本文基于当前代码结构、配置、测试与示例整理，用于快速理解 Taibai 的定位、核心能力和主要模块。

## 项目定位

Taibai 是一个使用 Rust 实现的列式数据库服务器，目标是提供面向分析型场景的高吞吐写入、列式查询和可优化的 SQL 执行能力，同时保留事务、权限、网络协议和客户端交互等数据库服务端基础能力。

从 `Cargo.toml` 与主入口可以看出，工程提供：

- `taibai` 服务端二进制：启动数据库服务。
- `taibai client` 命令行客户端：连接服务端、登录、执行 SQL 或进入交互模式。
- `segment` 工具：用于检查和调试 segment 存储文件。
- 示例工程：批量导入、多行插入、预处理语句、CSV 导出/导入、热数据写入、变更订阅等使用场景，详见[示例工具使用指南](../operations/examples-guide.md)。

## 整体架构

工程按数据库系统的典型链路分层：

- `src/net/`：异步 TCP/TLS 网络层，负责连接、登录、会话、消息分发和客户端实现。
- `src/protocol/`：二进制协议层，定义消息类型、帧格式、压缩与查询结果编码。
- `src/sql/`：SQL 解析、绑定、逻辑计划、优化和执行。
- `src/storage/`：目录元数据、列式 segment、持久化 tail、MemTable、事务、索引、分区与压缩合并。
- `src/types/`：标量值、列数组、RecordBatch 和 NULL 位图等内部数据表示。
- `src/common/`：配置、错误类型等公共设施。
- `src/tools/` 与 `src/bin/`：segment 相关辅助工具。

请求的大致路径是：

```text
TCP/TLS 连接
  -> protocol message 解码
  -> Session 权限与状态检查
  -> SQL Parser / Fast Insert Parser
  -> Planner / Optimizer
  -> Execution Engine
  -> Storage Engine
  -> RecordBatch / OK / Error 编码返回
```

## 服务端与客户端能力

Taibai 可以读取 YAML 配置并启动数据库服务，主要配置包括监听地址、最大消息大小、数据目录、默认数据库、查询批大小、协议压缩和 TLS 证书。

服务端支持：

- TCP 异步连接处理。
- 可选 TLS。
- 登录认证与会话管理。
- 普通 SQL 查询。
- 二进制查询结果。
- Ping/Pong。
- Flush。
- 事务消息。
- Cursor 打开、拉取和关闭。
- Prepare / Execute / Close Statement。
- Batch Execute。
- 列式批量导入消息。

客户端支持连接、登录、登出、Ping、执行查询、交互式 SQL、二进制结果接收、批量导入和 pipeline 模式。

## SQL 功能

SQL 层包含手写 lexer/parser、AST、binder、planner、optimizer 和执行器。当前语句覆盖面较广：

- DDL：`CREATE/DROP DATABASE`、`CREATE/DROP/ALTER/TRUNCATE TABLE`。
- 表结构能力：主键、默认值、`AUTO_INCREMENT`、`CLUSTER BY`、`PARTITION BY`、`TTL`、`COMPRESSION_LEVEL`（按表设置 zstd 级别 1..=22）。
- 分区策略：`VALUE`、`DAY`、`MONTH`、`YEAR`、`HASH`、`MOD`。
- DML：`INSERT`、多行插入、`INSERT INTO ... SELECT`（库内 ETL：过滤、聚合、投影后落表）、`REPLACE INTO`（含 `REPLACE INTO ... SELECT`）、`UPDATE`、`DELETE`；`UPDATE` / `DELETE` 支持 MySQL 风格的表别名（`DELETE FROM t AS a WHERE a.x = 1`）。
- 查询：`SELECT`、`WHERE`、`GROUP BY`、`HAVING`、`ORDER BY`、`LIMIT/OFFSET`、`DISTINCT`。
- 表达式：`CASE`、`BETWEEN`、`[NOT] IN/LIKE`、`CAST(expr AS type)`（支持 `DECIMAL(p,s)` 精度）、参数占位符 `?` 与 `$1/$2/...`。
- 词法：`--` 行注释、`/* */` 块注释、双引号 `"col"` 与反引号 `` `col` `` 引用标识符（可为保留字或含空格的列名）。
- Join：Inner、Left、Right、Full、Cross、Semi、Anti,以及 ASOF JOIN / LEFT ASOF JOIN（按等值键分区、时间键二分的行情最近匹配连接）。
- 子查询：`IN`、`EXISTS`、标量子查询等。
- 集合查询：`UNION` / `UNION ALL`。
- 窗口函数：`ROW_NUMBER`、`RANK`、`DENSE_RANK`、`LEAD`、`LAG`，以及窗口聚合。
- 管理语句：`USE`、`SHOW TABLES/DATABASES/COLUMNS/INDEXES/CREATE TABLE/USERS/ROLES/GRANTS/PARTITIONS/STATS/ENGINES/PROCESSLIST`、`KILL QUERY`（终止运行中的查询）。
- 诊断语句：`EXPLAIN`、`EXPLAIN ANALYZE`。
- 运维语句：`FLUSH TABLE`、`OPTIMIZE TABLE`、`ANALYZE TABLE`（重建统计信息）、`PAUSE FLUSH`、`RESUME FLUSH`；`ALTER TABLE ... SET HOT` 控制常驻热存，`ALTER TABLE ... SET COMPRESSION_LEVEL = n | DEFAULT` 调整压缩级别。
- 事务语句：`BEGIN`、`COMMIT`、`ROLLBACK`。

内置函数注册表支持数学、字符串、日期时间、控制流、类型转换和向量函数，例如 `abs`、`sqrt`、`upper`、`substring`、`now`、`coalesce`、`to_int`、`vector_distance`、`vector_norm`、`vector_dot` 等。

## 查询优化与执行

优化器是规则驱动的迭代式优化器，当前包含：

- 表达式化简。
- 谓词下推。
- 列裁剪。
- Zone Map 裁剪。
- Bloom Filter 优化。
- Limit 下推。
- Order + Limit 下推。
- TopN 优化。
- 过滤条件重排。
- 索引选择。
- 分区裁剪。
- Join 重排。
- Semi Join 转换与列裁剪。
- 子查询重写。

执行层以批处理的 `RecordBatch` 为核心数据单位，包含扫描、过滤、投影、聚合、排序、Limit、Join、Semi Join、窗口函数、DML、DDL、Explain 和 prepared statement 执行路径。

针对扫描与分析场景，工程还提供：

- `COUNT(*)` 快速路径；单列无谓词 `SELECT DISTINCT` 由 row group zone map 元数据直接应答。
- 向量化表达式求值（`expr_vec`）：过滤比较与投影优先走列式内核。
- MemTable 与 segment 合并扫描。
- 分区裁剪（含 `BETWEEN` / `IN` 字面量按列类型转换后的裁剪）。
- 行组裁剪。
- 行过滤。
- 基于聚簇键和统计信息的扫描优化（前缀等值 + 范围复合二分裁剪）。
- 字典直通解码：低基数 VARCHAR 以字典编码形式直接参与分组聚合，免展开往返。

查询执行带阶段跟踪（Planning / Executing / Committing）与取消机制：`SHOW PROCESSLIST` 列出真实运行中的查询，`KILL QUERY` 可终止排队或执行中的请求，`statement_timeout_secs` 提供语句级超时。详见[查询管理与语句超时](../user-guide/query-management.md)。

## 存储引擎

存储层是 Taibai 的核心。它使用自定义列式 segment 文件格式，并围绕写入缓冲、持久化 tail、目录元数据和优化合并构建。

主要能力包括：

- 多数据库 catalog，元数据以 JSON 形式保存在 `meta/catalogs` 下。
- 表元数据：列定义、行数、segment 数、创建时间、聚簇键、分区信息、TTL 信息。
- MemTable 写入缓冲。
- BatchWriter 批写入队列。
- 持久化 tail（durable tail）承担写路径持久化与崩溃恢复，经典 WAL 已移除。
- SegmentManager 管理表数据文件、活跃 segment、flush 与读取。
- Segment 文件包含 header、压缩列数据、row group 元数据、footer 和 footer offset。
- Row Group 默认大小为 8192 行。
- 列数据按形态选择编码：定宽列支持 Raw、BitPacked、DeltaRle，低基数 VARCHAR 走字典编码，编码后统一 zstd 压缩；压缩级别可按表用 `COMPRESSION_LEVEL` 覆盖全局默认。
- Footer 记录 schema、row group offset/length、列压缩大小、Zone Map、Bloom Filter、删除位图等。
- 支持删除标记和逻辑删除。
- 唯一索引按需加载并以 base + delta 增量维护，direct bulk 批量导入只追加增量，不随历史数据量线性变慢。
- 支持 `OPTIMIZE TABLE` 触发 segment 合并、排序和清理；compaction 的重写阶段在分区锁外运行，压缩期间目标分区仍可写入。
- 写入互斥按 (库, 表, 分区) 粒度加锁，分区间的写入、flush 与 compaction 互不阻塞；全表操作取表级宽锁，等待受 `mutation_lock_timeout_ms` 有界限制。
- 常驻热存（`ALTER TABLE ... SET HOT`）：当日分区的点查走内存倒排索引；重启预热只加载日结之后写入的段（`storage.hot_warmup_enabled` 控制），批量写覆盖全表后自动标记全表常驻。
- Reader cache、buffer pool、列编码和快速扫描相关模块用于提升读取性能。

## 数据类型

协议层定义 19 种数据类型：

- Bool。
- Int8、Int16、Int32、Int64。
- UInt8、UInt16、UInt32、UInt64。
- Float32、Float64。
- Decimal。
- Date、Time、Timestamp、Interval。
- VarChar。
- Blob。
- Vector。

内部类型系统围绕 `ScalarValue`、`ColumnArray`、`RecordBatch` 和 `NullBitmap` 构建，适合列式批处理和二进制结果编码。

## 事务、权限与变更流

事务相关能力：

- SQL 事务：`BEGIN`、`COMMIT`、`ROLLBACK`。
- 协议事务消息：Begin、Commit、Rollback。
- Session 维护当前事务 ID 与写入状态。
- 写入事务在提交时通过 BatchWriter 提交，回滚时清理未提交写入。
- 存储层包含 MVCC 与事务管理模块。

权限相关能力：

- 用户：`CREATE USER`、`DROP USER`、`ALTER USER`。
- 角色：`CREATE ROLE`、`DROP ROLE`。
- 授权：`GRANT`、`REVOKE`、`SHOW GRANTS`。
- 权限类型：Select、Insert、Update、Delete、Create、Drop、Alter、Truncate、All。
- 权限对象：全局、数据库、表。
- 默认 root 用户使用 `taibai` 口令。

变更流能力：

- DML 成功后发布 Insert、Update、Delete 变更事件。
- 最近事件保存在内存队列中。
- 事务内插入在提交后才发布变更事件。
- 可按数据库和表过滤最近变更。
- 变更订阅（Pub/Sub）：客户端通过 `SUBSCRIBE` 协议消息订阅表的插入变更，服务端带序号异步推送，主题支持通配符；另有可靠订阅（`SUBSCRIBE_V2`，支持序列回放、ack 确认、具名书签续传与列过滤）；详见[变更订阅文档](../user-guide/pubsub.md)。

## 批量导入与性能方向

工程对高吞吐写入做了专门设计：

- 协议支持列式批量插入消息，包括 direct bulk 二进制列载荷（定宽列整段对齐拷贝、VarChar 走连续缓冲）。
- 服务端支持 `START BULK IMPORT` / `STOP BULK IMPORT` 模式。
- `START BULK IMPORT` 启用统一的批量导入模式；当前无独立 WAL 写入。
- 客户端支持 pipeline，在多个批次请求未完成时继续发送后续批次。
- `examples/batch_import` 针对股票 tick CSV 数据实现了高性能导入示例，包括块读取、字符串池化、列式编码、批大小控制和进度输出。
- `examples/export_csv` / `insert_csv` / `load_csv` 覆盖 CSV 导出、普通 INSERT 与列式 bulk 两种导入路径。
- 持久化由 durable tail（组提交 + 自适应 fsync 窗口）承担，实时小批次吞吐与崩溃安全可按 `tail_async_fsync` 权衡。

## 测试覆盖重点

`tests/` 目录覆盖了多个关键功能：

- 数据类型读写。
- 约束、主键、默认值、自增列。
- 删除、更新。
- 多数据库隔离。
- 分区与聚簇。
- 事务集成。
- TTL 与变更流。
- 并发稳定性。

这些测试说明工程关注的不只是 SQL 语法，而是端到端的数据写入、查询、元数据、事务和存储行为。

## 运行方式

构建：

```bash
cargo build --release
```

启动服务端：

```bash
cargo run -- --config conf/example.yaml
```

使用默认配置启动：

```bash
cargo run
```

启动客户端并执行查询：

```bash
cargo run -- client --host 127.0.0.1:5151 --user root --password taibai --database main --query "SHOW TABLES"
```

运行测试：

```bash
cargo test
```

## 一句话总结

Taibai 是一个面向列式存储和分析查询的 Rust 数据库服务端工程，已经具备网络协议、SQL 引擎、规则优化器、列式 segment 存储、事务、权限、分区、TTL、变更流和批量导入等完整数据库系统雏形。
