# 查询管理与语句超时

本文说明 Taibai 的运行中查询观察、手动终止（`KILL QUERY`）与语句超时配置。这些能力用于治理长查询：发现失控语句、及时止损，并避免队列被慢请求占满。

## 查看运行中的查询

```sql
SHOW PROCESSLIST;
SHOW FULL PROCESSLIST;
```

每条正在执行的语句在注册表中占一行，包含以下列：

| 列 | 说明 |
|------|------|
| `Id` | 查询 id，`KILL QUERY` 使用它定位目标 |
| `User` / `Database` | 发起会话的用户与数据库 |
| `Command` | 固定为 `Query` |
| `Time` | 已执行秒数 |
| `State` | 查询阶段：`Planning`（规划）、`Executing`（执行）、`Committing`（提交）或 `Killed`（已请求终止） |
| `Info` | SQL 文本；超过 100 字符时默认截断，`SHOW FULL PROCESSLIST` 显示完整语句 |

查询在开始执行前（排队等待执行槽位时）不会出现在列表中；语句结束（完成、报错或被终止）后立即从列表移除。

## 终止查询

```sql
KILL QUERY <query_id>;
```

- 目标可以是任意会话的查询；`query_id` 取自 `SHOW PROCESSLIST` 的 `Id` 列。
- 终止请求只是**置位取消标志**：正在排队或处于 `Planning` / `Executing` 阶段的查询会在下一个安全点检查到标志并中止，返回错误 `query <id> was killed by user '<user>'`。
- 已经进入 `Committing` 阶段的查询**不可中断**——此阶段包含 durable tail 追加、memtable 安装与索引维护，中途放弃会造成半提交状态，因此取消检查变为空操作，语句会运行到自然结束。
- `query_id` 不存在或已结束时返回错误 `query <id> is not running`。
- 被 `KILL` 的查询在消失前于 `SHOW PROCESSLIST` 中显示 `State = Killed`。

## 语句超时

`server.statement_timeout_secs` 为单条语句设置最长执行时间（墙钟），超时的语句在下一个安全点被取消，返回错误 `query exceeded the statement timeout`：

```yaml
server:
  # 0 表示关闭（默认）。生产环境建议设置一个大于最慢合法查询的值
  statement_timeout_secs: 300
```

- 超时与 `KILL QUERY` 走同一取消机制：`Committing` 阶段不受超时打断，保证已开始提交的数据要么完整落盘、要么整体不生效。
- 计时覆盖语句的规划与执行全程，从语句开始执行时起算。

## 相关的服务端限流配置

查询管理相关的其余配置都在 `server` 段：

| 配置 | 默认值 | 说明 |
|------|------|------|
| `request_queue_timeout_secs` | 30 | 重请求等待执行槽位的最长时间（秒），超时返回过载错误而不是静默排队 |
| `connection_idle_timeout_secs` | — | 连接无任何协议帧的最长时间，超时断开（0 关闭） |
| `bulk_import_idle_timeout_secs` | 3600 | bulk 导入会话存续期间的空闲断连超时：导入客户端会长时间静默（解码输入文件、攒批），仅靠 keepalive PING 续命，间隔与常规空闲打平时会误杀进行中的导入（0 = bulk 会话完全不受空闲断连） |
| `max_concurrent_requests` | 8 | CPU/磁盘重请求的最大并发数 |
| `reserved_bulk_slots` | 2 | 为 bulk 写入保留的重请求槽位数：查询类请求（`MSG_TYPE_QUERY`）最多占用 `max_concurrent_requests - reserved_bulk_slots` 个槽，bulk 写入始终使用完整池。查询风暴期间 bulk 导入的提交不会再被排队超时拒绝（0 关闭保留） |

过载与超时错误会返回给客户端，应用侧应将其视为可重试信号。

## 代码索引

| 行为 | 位置 |
|------|------|
| 查询阶段定义与取消标志 | `src/common/query_cancel.rs`（`QueryPhase` / `QueryContext`） |
| 运行查询注册表 | `src/sql/execution/query_registry.rs` |
| `SHOW PROCESSLIST` / `KILL QUERY` 执行 | `src/sql/execution/ddl/introspection.rs`（`exec_show_processlist` / `exec_kill_query`） |
| 语句超时配置 | `src/common/config.rs`（`statement_timeout_secs`） |
| 取消与超时行为测试 | `src/sql/execution/engine_test/cancellation.rs` |
