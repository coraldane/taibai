# Taibai 文档

如果你第一次接触项目，请按以下顺序阅读：

1. [五分钟快速开始](getting-started/quick-start.md)：构建、启动、连接并执行第一组 SQL。
2. [工程主要功能总结](overview/project-summary.md)：了解项目目标、架构和主要能力。
3. [兼容性与部署指引](user-guide/limitations.md)：了解兼容性、安全边界和生产部署检查。
4. [参与贡献](../CONTRIBUTING.md)：搭建开发环境并提交改动。

## 使用指南

面向日常使用，描述当前版本的实际行为：

- [SQL 语法参考](user-guide/sql-reference.md)：**全部**支持的 SQL 语法——DDL、DML、查询、表达式、内置函数、管理语句，以及明确不支持的能力清单。taibai 方言不兼容主流数据库，请以此为准。
- [事务与并发语义](user-guide/transactions-and-concurrency.md)：隔离级别、可见性、锁与持久化边界。
- [查询管理与语句超时](user-guide/query-management.md)：`SHOW PROCESSLIST`、`KILL QUERY` 与语句超时配置。
- [变更订阅（Pub/Sub）](user-guide/pubsub.md)：订阅表插入变更、主题通配符与慢消费者策略，以及可恢复的可靠订阅（序列确认、具名书签、列过滤）。
- [建表选项 SQL 参考](user-guide/table-options.md)：`CLUSTER BY`、`PARTITION BY`、`TTL`、`HOT`、`DURABILITY`、`COMPRESSION_LEVEL` 的语法与 `ALTER TABLE` 维护语句。

## 运维

- [示例工具使用指南](operations/examples-guide.md)：导入、导出、订阅与性能测试示例的用法。
- [数据导入流程说明](operations/data-import-flow.md)
- [备份、迁移与恢复](operations/backup-and-migrate.md)
- [主备流复制](operations/replication.md)：primary/standby 配置、事件流与引导流程。
- [Windows 安装说明](operations/windows-installation.md)
- [金融行情场景验证与测试清单](operations/financial-market-data-validation-checklist.md)

## 架构与设计

- [Taibai 列式数据库服务器](design/TAIBAI_DESIGN_ZH.md)：整体架构与各子系统设计。
- [二进制协议（线上格式参考）](design/wire-protocol.md)：帧格式、消息类型表、登录能力字节、流式结果的 `0x26`/`0x27` 语义与客户端接入检查清单。
- [分区级 mutation lock 设计](design/partition-mutation-locks.md)：写入互斥的锁粒度与锁序。
- [元数据折叠聚合执行](design/folded-aggregate-execution.md)：COUNT(*)/分组聚合从段 footer 元数据直接作答的设计。
- [Direct Bulk 二进制协议](design/direct-bulk-binary-protocol.md)
- [通用性能基础](general-performance-foundations.md)
- [暂缓的设计决策](deferred-design-decisions.md)：当前版本有意保留的架构折衷与备选方案。

## 计划

计划文档描述尚未完成的设计方向，不代表功能已经发布或稳定支持；已实施完成的计划会从本目录清理（历史版本见 git 记录）。

- [向量化执行 → Parquet/Arrow → 时序金融深化路线图](plans/vectorization-parquet-timeseries-roadmap.md)：当前主线路线图。
- [行情数据实时落库改造方案](plans/realtime-market-data-ingestion-plan.md)：实时摄取链路（ingest profile、二进制列式直写）。
- [普通 INSERT 持久化路径磁盘 I/O 优化方案](plans/ordinary-insert-disk-io-optimization-plan.md)

## 报告

报告中的数据只适用于文档记录的版本、配置和测试环境。

- [Taibai vs DuckDB vs kdb+ 性能对比报告](reports/benchmark-vs-duckdb-kdb-2026-09.md)：当前版本的对照基准与后续优化方向。

## 项目治理

- [贡献指南](../CONTRIBUTING.md)
- [安全策略](../SECURITY.md)
- [社区行为准则](../CODE_OF_CONDUCT.md)
- [MIT 许可证](../LICENSE-MIT)
- [Apache 2.0 许可证](../LICENSE-APACHE)
