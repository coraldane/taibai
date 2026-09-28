# Taibai 文档

如果你第一次接触项目，请按以下顺序阅读：

1. [五分钟快速开始](getting-started/quick-start.md)：构建、启动、连接并执行第一组 SQL。
2. [工程主要功能总结](overview/project-summary.md)：了解项目目标、架构和主要能力。
3. [兼容性与部署指引](user-guide/limitations.md)：了解兼容性、安全边界和生产部署检查。
4. [参与贡献](../CONTRIBUTING.md)：搭建开发环境并提交改动。

## 使用指南

面向日常使用，描述当前版本的实际行为：

- [事务与并发语义](user-guide/transactions-and-concurrency.md)：隔离级别、可见性、锁与持久化边界。
- [查询管理与语句超时](user-guide/query-management.md)：`SHOW PROCESSLIST`、`KILL QUERY` 与语句超时配置。
- [变更订阅（Pub/Sub）](user-guide/pubsub.md)：订阅表插入变更、主题通配符与慢消费者策略，以及可恢复的可靠订阅（序列确认、具名书签、列过滤）。
- [建表选项 SQL 参考](user-guide/table-options.md)：`CLUSTER BY`、`PARTITION BY`、`TTL`、`HOT`、`DURABILITY`、`COMPRESSION_LEVEL` 的语法与 `ALTER TABLE` 维护语句。

## 运维

- [示例工具使用指南](operations/examples-guide.md)：导入、导出、订阅与性能测试示例的用法。
- [数据导入流程说明](operations/data-import-flow.md)
- [备份、迁移与恢复](operations/backup-and-migrate.md)：backup/migrate 工具、tail 归档与恢复流程。
- [Windows 安装说明](operations/windows-installation.md)
- [金融行情场景验证与测试清单](operations/financial-market-data-validation-checklist.md)

## 架构与设计

- [Taibai 列式数据库服务器](design/TAIBAI_DESIGN_ZH.md)：整体架构与各子系统设计。
- [Rust 列式数据库架构方案](design/columnar-database-design.md)：最初的架构设计文档。
- [分区级 mutation lock 设计](design/partition-mutation-locks.md)：写入互斥的锁粒度与锁序。
- [Transaction State Model and Session Management](design/transaction-state-model.md)
- [SQL Transaction Support Analysis](design/transaction-hooks-analysis.md)
- [CLUSTER BY Binary Search Integration Design](design/cluster-by-binary-search-design.md)
- [实时 tick 内存态设计](design/realtime-tick-inmemory-design.md)
- [Direct Bulk 二进制协议](design/direct-bulk-binary-protocol.md)
- [通用性能基础](general-performance-foundations.md)

## 计划

计划文档描述尚未完成的设计方向，不代表功能已经发布或稳定支持；已实施完成的计划会从本目录清理（历史版本见 git 记录）。

- [向量化执行 → Parquet/Arrow → 时序金融深化路线图](plans/vectorization-parquet-timeseries-roadmap.md)：当前主线路线图。
- [行情数据实时落库改造方案](plans/realtime-market-data-ingestion-plan.md)：实时摄取链路（ingest profile、二进制列式直写）。
- [ScalarValue 演进与聚合执行器路线图](plans/scalar-value-evolution-roadmap.md)
- [普通 INSERT 持久化路径磁盘 I/O 优化方案](plans/ordinary-insert-disk-io-optimization-plan.md)
- [Durable Tail 升级为可查询 spill 层](plans/memtable-tail-spill-layer-plan.md)：Phase 1+2 已实施，Phase 3 未实施。

## 报告

报告中的数据只适用于文档记录的版本、配置和测试环境。

- [Taibai vs DuckDB vs kdb+ 性能对比报告](reports/benchmark-vs-duckdb-kdb-2026-09.md)：当前版本的对照基准。
- [性能测试与验证报告](reports/performance-verification-report.md)：写入路径性能与瓶颈分析。
- [SQL 查询语义支持度审计报告](reports/sql-semantics-audit-report.md)

## 项目治理

- [贡献指南](../CONTRIBUTING.md)
- [安全策略](../SECURITY.md)
- [社区行为准则](../CODE_OF_CONDUCT.md)
- [MIT 许可证](../LICENSE-MIT)
- [Apache 2.0 许可证](../LICENSE-APACHE)
