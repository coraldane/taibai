# Taibai

**为行情与 tick 数据准备的开源列式数据库。** 单二进制部署、SQL 即用、中文文档齐备，面向「大量写入 + 按标的与时间聚合」的分析场景。

<sub>An open-source columnar database in Rust, built for market-data / tick workloads.</sub>

![license](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue)
![rust](https://img.shields.io/badge/rust-stable-dea584)

---

## 为什么会有 Taibai

做行情数据的人大多在这几个选项里打转：

- **kdb+** 的 `aj` 语义和 tick 架构确实是行业标杆，但授权费按年计、以万美元为单位，q 语言也不是团队里每个人都能上手。
- **ClickHouse** 足够强，但要为一份日内 tick 数据拉起整套集群、调好分区键和物化视图，往往比数据本身更费劲。
- **QuestDB / InfluxDB** 面向时序指标很顺手，但「逐笔成交匹配最近一条报价」这类关系型时序语义写起来别扭。
- 自己用 Parquet + DuckDB 拼一套，能查，但没有服务端、没有权限、没有订阅推送，每个项目重写一遍。

Taibai 想填的是这个缝：**一个单二进制、说 SQL、原生懂 tick 数据的列式库**。写入侧按列式直写，查询侧用 `ASOF JOIN` 和窗口函数直接表达行情逻辑，运维侧只有进程、目录和配置文件。

## 和现成方案的差别

| | Taibai | kdb+ | ClickHouse | QuestDB | DuckDB |
|---|---|---|---|---|---|
| 形态 | 单二进制服务端 + 内置客户端 | 单二进制 | 服务端（集群需配套组件） | JVM 服务端 | 嵌入式库，无服务端 |
| 授权 | MIT / Apache-2.0 | 商业授权 | Apache-2.0 | Apache-2.0 | MIT |
| `ASOF JOIN`（最近匹配） | 原生，支持 4 种方向 | 原生（`aj`） | 支持 | 支持 | 支持 |
| 内置变更订阅推送 | 支持，可回放与断点续传 | 原生（tick 架构） | 无（需外接消息队列） | 无 | 无 |
| 常驻热分区内存索引 | 内置，按日自动切换 | 原生 | 需自行调优 | 部分 | 不适用 |
| 按表 TTL 过期 | 支持 | 需自建 | 支持 | 支持 | 不支持 |
| 中文文档 | 完整 | 稀少 | 部分 | 无 | 部分 |

> 对比只描述各项目的能力形态，不构成性能结论；同类能力的具体表现请以各自官方文档与实测为准。这里不列性能数字，是因为诚实的数字必须能在你的机器上复现，见[性能数据说明](docs/user-guide/limitations.md)。

**它不适合的场合**（先写在前头，省得你白试）：需要 BI 工具用 MySQL/PostgreSQL 驱动直连、需要分布式多副本强一致、需要厂商 SLA 的生产系统。

## 30 秒上手

需要 Rust stable 工具链。

```bash
git clone https://github.com/coraldane/taibai.git && cd taibai
cargo build --release

# 终端 1：起服务（默认 0.0.0.0:5151，数据落在 ./data）
cargo run --release -- --config conf/example.yaml

# 终端 2：进交互客户端（默认 root/taibai，仅限本地开发）
cargo run --release -- client --host 127.0.0.1:5151 --user root --password taibai --database main
```

然后在客户端里跑一段**能看出差别**的 SQL —— 逐笔成交匹配它之前最近的一条报价：

```sql
CREATE DATABASE demo;
USE demo;

-- 按 (标的, 时间) 聚簇：等值前缀 + 时间范围的扫描走复合二分裁剪
CREATE TABLE trades (symbol VARCHAR, ts TIMESTAMP, qty INT64) CLUSTER BY (symbol, ts);
CREATE TABLE quotes (symbol VARCHAR, ts TIMESTAMP, bid FLOAT64) CLUSTER BY (symbol, ts);

INSERT INTO quotes VALUES
  ('AAPL', 1717200000000, 191.10),
  ('AAPL', 1717200060000, 191.30),
  ('AAPL', 1717200120000, 191.25);

INSERT INTO trades VALUES
  ('AAPL', 1717200030000, 100),
  ('AAPL', 1717200090000, 200),
  ('AAPL', 1717200150000, 300);

-- kdb+ aj 语义：每个左行取右表中时间不晚于它的最后一行
SELECT t.ts, t.qty, q.bid
FROM trades t ASOF JOIN quotes q
  ON t.symbol = q.symbol AND t.ts >= q.ts
ORDER BY t.ts;
```

结果里每笔成交都带上了成交时刻的有效报价。`LEFT ASOF JOIN` 会保留匹配不到的左行（价格为空）。四种不等式方向（`>=` `>` `<=` `<`）的语义见 [SQL 语法参考](docs/user-guide/sql-reference.md)。

## 为行情数据专门做的事

- **`ASOF JOIN`**：`ON` 条件由若干等值键 + 恰好一个时间不等式组成，按等值键分区、时间键二分匹配；`ASOF LEFT JOIN` 拼写同样接受。
- **`CLUSTER BY (symbol, ts)`**：写入即按聚簇键有序，查询走「前缀等值 + 范围」复合裁剪，不扫全表。
- **常驻热存（HOT）**：`ALTER TABLE ... SET HOT BY (ts)` 让当日分区驻留内存倒排索引，点查不落盘；重启只预热日结之后写入的段。
- **可靠变更订阅**：`SUBSCRIBE_V2` 支持序列回放、ack 确认、具名书签续传与列过滤——下游策略进程断线重连不丢数据。
- **列式直写导入**：`DIRECT BULK` 二进制列载荷跳过 SQL 解析与逐值求值，配合 pipeline 多批并发；CSV 导入示例见 `examples/batch_import`。
- **按表 TTL 与压缩级别**：`TTL ts, 86400` 自动清理过期分区，`COMPRESSION_LEVEL = 19` 让归档表单独吃满压缩率。
- **分区级写锁**：写入、flush 与 compaction 按 `(库, 表, 分区)` 互不阻塞，压缩期间目标分区仍可写入。
- **查询可管可控**：`SHOW PROCESSLIST` 看真实运行中的查询，`KILL QUERY` 终止失控语句，`statement_timeout_secs` 兜底。

## 能力清单

- **SQL**：DDL / DML / `INSERT INTO ... SELECT` / `REPLACE INTO`；Inner、Left、Right、Full、Cross、Semi、Anti、ASOF 各类 Join；子查询、CTE、`UNION`/`INTERSECT`/`EXCEPT`、窗口函数（`ROW_NUMBER`、`RANK`、`LEAD`、`LAG` 与窗口聚合）；`EXPLAIN` / `EXPLAIN ANALYZE`。
- **存储**：自定义列式 segment 格式，Row Group 默认 8192 行，定宽列 Raw / BitPacked / DeltaRle 编码，低基数 `VARCHAR` 走字典编码，统一 zstd 压缩；durable tail 承担写路径持久化与崩溃恢复。
- **优化器**：谓词下推、列裁剪、Zone Map 与 Bloom Filter 裁剪、分区裁剪、Limit / TopN / Order 下推、索引选择、Join 重排、子查询重写；`COUNT(*)` 与单列 `DISTINCT` 由段元数据直接应答。
- **服务端**：异步 TCP 与可选 TLS、用户/角色/`GRANT` 权限、事务、多数据库、主备流复制、备份与 `migrate` 工具、`segment` 段文件检查器、Windows MSI 安装包与 systemd 服务。
- **数据类型**：19 种，含 `Decimal`、`Timestamp`、`Blob`、`Vector`（配套 `vector_distance` / `vector_dot` 等函数）。

## 已知边界

- SQL 是自有实现，**不兼容** MySQL / PostgreSQL 方言，现有 BI 与 ORM 驱动不能直接对接。
- 相关子查询仅支持直接外层引用，更深嵌套会明确报错（不会静默给错结果）；`JOIN ON` 内的子查询不支持。
- 单机为主：有 primary/standby 流复制，没有分片与多副本一致性协议。
- `0.x` 期间段格式与协议仍会变；升级前请按[兼容性指引](docs/user-guide/limitations.md)备份并跑 `migrate`。

## 项目现状

约 15 万行 Rust（`src/` 下 373 个文件），54 组集成测试覆盖类型、约束、事务、分区、索引、Join、窗口、聚合、Pub/Sub、复制与并发稳定性；CI 在每次推送执行 `cargo fmt --check`、`cargo clippy --workspace --all-targets`、`cargo test --workspace`。

## 文档

- [五分钟快速开始](docs/getting-started/quick-start.md)
- [SQL 语法参考](docs/user-guide/sql-reference.md)：全部支持的语法，以及明确不支持的清单
- [建表选项](docs/user-guide/table-options.md)：`CLUSTER BY`、`PARTITION BY`、`TTL`、`HOT`、`DURABILITY`、`COMPRESSION_LEVEL`
- [事务与并发语义](docs/user-guide/transactions-and-concurrency.md)
- [变更订阅 Pub/Sub](docs/user-guide/pubsub.md)｜[查询管理](docs/user-guide/query-management.md)｜[主备流复制](docs/operations/replication.md)
- [架构设计](docs/design/TAIBAI_DESIGN_ZH.md)｜[二进制协议](docs/design/wire-protocol.md)｜[工程功能总览](docs/overview/project-summary.md)
- [兼容性与部署指引](docs/user-guide/limitations.md)｜[备份与迁移](docs/operations/backup-and-migrate.md)｜[示例工具指南](docs/operations/examples-guide.md)
- 完整目录见 [docs/README.md](docs/README.md)

## 开发与贡献

- 代码入口：`src/main.rs`（启动流程）、`src/sql/`（解析/计划/优化/执行）、`src/storage/`（段格式/事务/分区）、`src/net/` 与 `src/protocol/`（连接与协议）。
- 提交改动前请读 [CONTRIBUTING.md](CONTRIBUTING.md) 与 [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)。
- 安全漏洞请按 [SECURITY.md](SECURITY.md) 私密报告，不要开公开 Issue。
- 欢迎的贡献方向：更全的函数覆盖、文档与示例、真实数据集上的性能调优、客户端语言绑定。

## 许可证

按 [Apache-2.0](LICENSE-APACHE) 或 [MIT](LICENSE-MIT) 双许可提供，贡献内容同样按 `MIT OR Apache-2.0` 提供。
