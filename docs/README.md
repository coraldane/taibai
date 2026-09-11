# Taibai 文档

如果你第一次接触项目，请按以下顺序阅读：

1. [五分钟快速开始](getting-started/quick-start.md)：构建、启动、连接并执行第一组 SQL。
2. [工程主要功能总结](overview/project-summary.md)：了解项目目标、架构和主要能力。
3. [项目状态与限制](user-guide/limitations.md)：了解实验性状态、兼容性与生产边界。
4. [参与贡献](../CONTRIBUTING.md)：搭建开发环境并提交改动。

## 使用与运维

- [事务与并发语义](user-guide/transactions-and-concurrency.md)
- [示例工具使用指南](operations/examples-guide.md)
- [数据导入流程说明](operations/data-import-flow.md)
- [Windows 安装说明](operations/windows-installation.md)
- [金融行情场景验证与测试清单](operations/financial-market-data-validation-checklist.md)

## 架构与设计

- [Taibai 列式数据库服务器](design/TAIBAI_DESIGN_ZH.md)
- [Rust 列式数据库架构方案](design/columnar-database-design.md)
- [CLUSTER BY Binary Search Integration Design](design/cluster-by-binary-search-design.md)
- [SQL Transaction Support Analysis](design/transaction-hooks-analysis.md)
- [Transaction State Model and Session Management](design/transaction-state-model.md)
- [通用性能基础](general-performance-foundations.md)

## 计划

计划文档描述设计方向，不代表功能已经发布或稳定支持。

- [Implementation Plan](plans/implementation-plan.md)
- [导入性能优化方案](plans/import-optimization-plan.md)
- [查询性能架构重构计划](plans/query-performance-architecture-refactor-plan.md)

## 报告

报告中的数据只适用于文档记录的版本、配置和测试环境。

- [性能测试与验证报告](reports/performance-verification-report.md)
- [Quest Completion Report](reports/quest-completion-report.md)

## 项目治理

- [贡献指南](../CONTRIBUTING.md)
- [安全策略](../SECURITY.md)
- [社区行为准则](../CODE_OF_CONDUCT.md)
- [更新日志](../CHANGELOG.md)
- [MIT 许可证](../LICENSE-MIT)
- [Apache 2.0 许可证](../LICENSE-APACHE)
