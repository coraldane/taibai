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

已登录连接的重请求在排队等待执行槽位时就会出现在列表中；语句结束（完成、报错或被终止）后立即从列表移除。

## 终止查询

```sql
KILL QUERY <query_id>;
```

- 目标可以是任意会话的查询；`query_id` 取自 `SHOW PROCESSLIST` 的 `Id` 列。
- 终止请求只是**置位取消标志**：正在排队或处于 `Planning` / `Executing` 阶段的查询会在下一个安全点检查到标志并中止，返回错误 `query <id> was killed by user '<user>'`。
- 已经进入 `Committing` 阶段的查询**不可中断**——此阶段包含 durable tail 追加、memtable 安装与索引维护，中途放弃会造成半提交状态，因此取消检查变为空操作，语句会运行到自然结束。
- `query_id` 不存在或已结束时返回错误 `query <id> is not running`。
- 被 `KILL` 的查询在消失前于 `SHOW PROCESSLIST` 中显示 `State = Killed`。

单独发送的 `KILL QUERY` 和 `SHOW [FULL] PROCESSLIST` 使用独立的 2 个管理请求槽位，
普通请求池满时仍可执行；权限检查保持不变。为限制分类开销，仅对载荷不超过 4096 字节、
且只包含一条上述语句的 QUERY 请求使用该通道。请使用另一条可用连接发送管理命令。
此通道不绕过 TCP 连接数上限，也不能中断已经进入提交阶段的写入。

约束锁等待（INSERT/REPLACE/UPDATE/DELETE/bulk）会轮询取消标志。
普通 INSERT 完成约束检查后才进入提交阶段，排队和校验期间仍可取消。

## 语句超时

`server.statement_timeout_secs` 为单条语句设置最长执行时间（墙钟），超时的语句在下一个安全点被取消，返回错误 `query exceeded the statement timeout`：

```yaml
server:
  # 0 表示关闭（默认）。生产环境建议设置一个大于最慢合法查询的值
  statement_timeout_secs: 300
```

- 超时与 `KILL QUERY` 走同一取消机制：`Committing` 阶段不受超时打断，保证已开始提交的数据要么完整落盘、要么整体不生效。
- 计时覆盖语句的规划与执行全程，从收到请求并创建取消上下文时起算，包含请求槽位排队。

## 相关的服务端限流配置

查询管理相关的其余配置都在 `server` 段：

| 配置 | 默认值 | 说明 |
|------|------|------|
| `request_queue_timeout_secs` | 30 | 重请求等待执行槽位的最长时间（秒），超时返回过载错误而不是静默排队 |
| `connection_idle_timeout_secs` | — | 连接无任何协议帧的最长时间，超时断开（0 关闭） |
| `bulk_import_idle_timeout_secs` | 3600 | bulk 导入会话存续期间的空闲断连超时：导入客户端会长时间静默（解码输入文件、攒批），仅靠 keepalive PING 续命，间隔与常规空闲打平时会误杀进行中的导入（0 = bulk 会话完全不受空闲断连） |
| `max_concurrent_requests` | 8 | CPU/磁盘重请求的最大并发数 |
| `reserved_bulk_slots` | 2 | 为 bulk 写入保留的重请求槽位数：查询类请求（`MSG_TYPE_QUERY`）最多占用 `max_concurrent_requests - reserved_bulk_slots` 个槽，bulk 写入始终使用完整池。查询风暴期间 bulk 导入的提交不会再被排队超时拒绝（0 关闭保留） |

过载与超时错误会返回给客户端。重试应有退避和次数上限；写入遇到网络断开时，应先核对提交结果或使用幂等键，避免盲目重试重复写入。

## 查询结果内存上限

结果集在服务端缓冲后以单条消息发回。缓冲上限为：

```text
min(buffer.query_memory_limit_mb / 2, server.max_message_size / 2)
```

默认配置（4096 MB / 64 MB）下即 **32 MB**，由 `server.max_message_size` 决定——即使把 `query_memory_limit_mb` 调到再大也不会放宽结果集上限。

超限报 `query result exceeded memory limit of ... (bounded by <配置键> = ...)`。这是**确定性错误**：同样的查询对同样的数据重试必然复现，服务端不会标记 `[RETRYABLE]`，内置客户端也不会自动重试。处理方式按优先级：

1. 加 WHERE 过滤或 LIMIT 分页，减少单次返回的数据量；
2. 会话内执行 `SET streaming_results = true` 切换为**分块流式结果**：结果按批次以 `RESULT_CHUNK` 消息逐块发送、以 `RESULT_END` 收尾，单批超过帧上限一半时自动按行切片。结果集不再受 64 MB 单消息上限约束，只受查询内存预算（聚合/连接/排序算子内部预算与 `buffer.query_memory_limit_mb`）约束。内置客户端已支持聚合分块；`SET streaming_results = false`（或重连）恢复默认单消息模式。流式路径仅覆盖无子查询/派生表的普通 SELECT，其余语句自动回落单消息路径；
3. 确需更大的单消息结果时，同步调大 `server.max_message_size` **和所有客户端的接收上限**（内置客户端与 JDBC 客户端均硬编码 64 MB），缺一会导致大结果在客户端解码阶段报协议错误；`max_bulk_message_size` 是 bulk 导入独立上限，与结果集无关。

注意：JDBC / Go 等未改造的客户端不会发送 `SET streaming_results`，行为完全不变；流式模式是会话级显式选择。

## 代码索引

| 行为 | 位置 |
|------|------|
| 查询阶段定义与取消标志 | `src/common/query_cancel.rs`（`QueryPhase` / `QueryContext`） |
| 运行查询注册表 | `src/sql/execution/query_registry.rs` |
| `SHOW PROCESSLIST` / `KILL QUERY` 执行 | `src/sql/execution/ddl/introspection.rs`（`exec_show_processlist` / `exec_kill_query`） |
| 语句超时配置 | `src/common/config.rs`（`statement_timeout_secs`） |
| 取消与超时行为测试 | `src/sql/execution/engine_test/cancellation.rs` |
