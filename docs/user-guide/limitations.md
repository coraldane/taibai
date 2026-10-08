# 兼容性与部署指引

Taibai 是用 Rust 编写的偏分析场景的列式数据库服务端：适合写入大量数据，然后按列扫描、统计、聚合和查询。

## 兼容性说明

- SQL 语法是项目自有实现，不代表完整兼容 MySQL、PostgreSQL、ClickHouse 或其他数据库。
- 升级前应备份数据，并在隔离环境验证读取、写入和恢复流程。
- 配置项、命令行参数、SQL 行为或公共接口的不兼容变更会在 Pull Request 描述中突出说明。
## 列变更（ALTER TABLE ADD/DROP COLUMN）与数据布局

- 列的增删是 catalog 元数据操作；**不可变的段**保留写入时刻的布局，读取时按列名对齐，
  早于 ADD COLUMN 的段按列的 DEFAULT（无 DEFAULT 则 NULL）参与谓词与投影——SELECT、
  COUNT、聚合、UPDATE、DELETE 全部同一语义。
- **热状态（常驻 memtable、durable tail、hot mirror）在 ADD/DROP COLUMN 时于表突变锁内
  先收敛（flush 落段）再翻转 catalog**，因此这些 ALTER 会强制未落盘数据落盘，耗时与
  未刷数据量成正比；执行期间有打开事务写过该表时 ALTER 会明确拒绝（先 COMMIT/ROLLBACK）。
- 由**旧版本**二进制产生、未经收敛的 durable tail 在新版本恢复时会被隔离：文件保留在磁盘，
  该 memtable 的行不可用，`tail_damage_snapshot().tails_schema_drift` 计数，日志给出
  remedy——此类表需从 ALTER 前的备份重载。

## 段格式版本

数据目录中的 segment 文件带格式版本号。升级二进制时：

- **新二进制读旧数据**：读取端支持版本区间（当前 `{1, 2}`），直接启动即可，无需迁移。
- **旧二进制读新数据**：会在 header 版本门处**明确报错**（不会静默错解），错误信息会提示运行 `migrate` 工具。
- **升级流程**：停止服务（优雅关闭会 seal 全部 active writer）→ 备份数据目录 → `migrate --data <dir>`（dry run 校验）→ `migrate --data <dir> --write`（原地升级，tmp+rename 原子替换）→ 启动新二进制。带 `.meta` sidecar 的 active 段会被拒绝迁移，先正常关服再执行。
- footer 扩展块（value sets / column sums / column CRCs / TLV 尾块 / 整体校验和）的编解码统一在 `src/storage/segment/footer_ext.rs` 一处维护；新增扩展不再需要同步修改多个解析点。

## 功能边界

项目包含事务、权限、分区、TTL、索引、Join、子查询、窗口函数和查询优化等实现。使用具体能力前，建议阅读对应文档并针对业务数据做端到端验证；事务与并发的实际行为见 [事务与并发语义](transactions-and-concurrency.md)。

子查询与语句覆盖的当前边界（不支持时均明确报错，不会静默出错）：

- 相关引用仅支持**直接外层**查询块，更深层嵌套的相关子查询报 `nested correlated subqueries are not supported`。
- 相关子查询内不支持参数占位符（`?` / `$1`）。
- `JOIN ON` 条件中的子查询不支持。
- `UPDATE` / `DELETE` 语句中的相关子查询不支持。
- 窗口函数支持 `ROW_NUMBER` / `RANK` / `DENSE_RANK` / `LEAD` / `LAG` 与窗口聚合；窗口 + `GROUP BY`、嵌套窗口等形态明确报错。

## 安全边界

- 默认 `root/taibai` 凭据只用于本地开发，部署前必须修改。
- TLS 默认关闭，跨主机或不可信网络通信时应启用。
- 使用者负责网络隔离、凭据管理、文件权限、备份和日志脱敏。
- 发现漏洞时请遵循仓库根目录的 [安全策略](../../SECURITY.md)。

## 性能数据

仓库中的性能数据只代表文档所述环境和测试方法，不构成对其他硬件、数据分布或负载的性能承诺。性能对比应记录版本、配置、硬件、数据集、预热方式和完整命令。

## 生产部署检查

将 Taibai 部署到生产环境前，至少应完成：

1. 修改默认凭据并限制网络访问。
2. 启用和验证 TLS。
3. 使用独立持久化目录并设置最小文件权限。
4. 建立备份和恢复演练。
5. 对实际 SQL、并发量和数据规模执行压力测试。
6. 验证升级和回滚流程。
7. 评估当前版本的已知问题和变更记录。
