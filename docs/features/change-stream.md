# 变更流

Taibai 通过现有的 TCP 消息协议暴露表级 DML 变更通知，因此订阅者可以用任何语言实现。

面向消费者的使用指南，请参阅 `change-stream-quickstart.md`。

## 线路协议

每条消息使用标准的 Taibai 帧：

| 字段 | 大小 | 编码 |
| --- | ---: | --- |
| magic | 2 字节 | `0x01 0xDB` |
| 消息类型 | 1 字节 | 无符号整数 |
| 负载长度 | 4 字节 | 小端序 `u32` |
| 负载 | 可变 | 消息特定 |

## 消息类型

| 名称 | 类型 | 方向 | 负载 |
| --- | ---: | --- | --- |
| `MSG_TYPE_CHANGE_SUBSCRIBE` | `0x60` | 客户端 -> 服务端 | JSON 订阅请求 |
| `MSG_TYPE_CHANGE_EVENT` | `0x61` | 服务端 -> 客户端 | JSON 变更事件 |
| `MSG_TYPE_CHANGE_ACK` | `0x62` | 客户端 -> 服务端 | JSON 消费者组确认 |
| `MSG_TYPE_CHANGE_ACK_STATE` | `0x63` | 服务端 -> 客户端 | JSON 消费者组状态 |
| `MSG_TYPE_ERROR` | `0x03` | 服务端 -> 客户端 | UTF-8 错误文本 |

客户端必须先使用常规的 `MSG_TYPE_LOGIN` 流程登录，然后在该连接上发送 `MSG_TYPE_CHANGE_SUBSCRIBE`。订阅后，该连接专门用于流式传输变更事件。

订阅授权使用 `SUBSCRIBE` 权限：

| 请求范围 | 所需权限 |
| --- | --- |
| `database` + `table` | 表 `database.table` 上的 `SUBSCRIBE`，或匹配的数据库/全局授权 |
| 仅 `database` | 该数据库上的 `SUBSCRIBE`，或全局授权 |
| 无数据库/表过滤器 | `*` 上的全局 `SUBSCRIBE`/`ALL` |
| 仅 `table` | 视为当前登录数据库 + 表 |

## 订阅请求

负载为 UTF-8 JSON：

```json
{
  "database": "market",
  "table": "ticks",
  "include_recent": true,
  "from_sequence": 1200,
  "consumer_group": "etl-worker"
}
```

字段：

| 字段 | 类型 | 必填 | 描述 |
| --- | --- | --- | --- |
| `database` | string/null | 否 | 数据库过滤器。空或缺失表示所有数据库。 |
| `table` | string/null | 否 | 表过滤器。空或缺失表示所有表。 |
| `include_recent` | boolean | 否 | 如果为 true，服务端在实时事件之前发送匹配的近期事件。当提供 `from_sequence` 或可恢复的 `consumer_group` 状态时忽略。 |
| `from_sequence` | integer/null | 否 | 重放 `sequence >= from_sequence` 的持久化匹配事件，然后继续实时事件。 |
| `consumer_group` | string/null | 否 | 使用该组的持久化 ACK 状态。如果省略 `from_sequence`，则从 `last_acked_sequence + 1` 开始重放。 |

空负载是有效的，表示订阅所有未来变更而不重放近期事件。

## 变更事件

负载是从 `ChangeEvent` 序列化的 UTF-8 JSON：

```json
{
  "version": 1,
  "sequence": 1,
  "timestamp_micros": 1760000000000000,
  "database": "market",
  "table": "ticks",
  "operation": "Insert",
  "rows_affected": 1000,
  "rows": [
    {
      "primary_key": { "id": { "Int64": 42 } },
      "after": {
        "id": { "Int64": 42 },
        "symbol": { "VarChar": "688256.SH" }
      }
    }
  ],
  "row_images_truncated": false
}
```

`operation` 为以下之一：

- `Insert`
- `Update`
- `Delete`
- `Truncate`
- `CreateTable`
- `DropTable`
- `AlterTable`

流是表级的：DDL 和 `TRUNCATE` 事件的 `rows_affected = 0`。

DML 事件在 SQL 执行路径可以捕获时包含行级映像：

- `Insert`：`after`
- `Update`：`before` 和 `after`
- `Delete`：`before`

`primary_key` 包含声明的主键列。如果表没有主键，行映像仍然存在但 `primary_key` 为空。为防止日志条目过大，每个事件最多携带 1,024 个行映像，当影响的行数更多时设置 `row_images_truncated = true`。

## 消费者组确认

消费者组偏移量持久化在 `meta/change_stream_consumers.json` 中。

`MSG_TYPE_CHANGE_ACK` 的 ACK 请求负载：

```json
{
  "group": "etl-worker",
  "sequence": 1288
}
```

`MSG_TYPE_CHANGE_ACK_STATE` 的 ACK 响应负载：

```json
{
  "group": "etl-worker",
  "sequence": 1288
}
```

ACK 更新是单调的：确认比存储值更小的序列号不会让组向后移动。流连接专门用于事件，因此消费者应该在单独的普通客户端连接上发送 ACK，或在确认后重新连接。

ACK 也可以参与普通的 SQL 事务：

```sql
BEGIN;
-- 在 Taibai 中应用下游/幂等写入
ACK CHANGE CONSUMER 'etl-worker' TO 1288;
COMMIT;
```

在事务内部，`ACK CHANGE CONSUMER` 在会话中暂存，仅在 `COMMIT` 成功时持久化。`ROLLBACK` 会丢弃暂存的 ACK。当消费者将其处理结果或幂等键存储在 Taibai 中，并希望结果写入和消费者偏移量一起推进时，这是推荐的模式。

消费者偏移量可以通过 SQL 查看：

```sql
SHOW CHANGE CONSUMERS;
```

结果包含 `consumer_group` 和 `sequence` 列，按消费者组名称排序。

消费者偏移量可以重置或删除：

```sql
RESET CHANGE CONSUMER 'etl-worker' TO 1288;
DROP CHANGE CONSUMER 'etl-worker';
```

变更流状态和保留事件可以查看：

```sql
SHOW CHANGE STATUS;
SHOW CHANGE EVENTS FROM 1200 LIMIT 100;
SHOW CHANGE EVENTS FOR market.ticks FROM 1200 LIMIT 100;
```

这些管理语句需要全局管理员权限。

## 消费者指南

下游消费者应将当前流视为带有有限行映像的表级通知，而不是完整的 CDC 日志。事件表明表发生了变更，影响了多少行，并包含采样到配置的每事件上限的行映像。

如果服务端发送带有 `change stream lagged, skipped N events` 等文本的 `MSG_TYPE_ERROR`，说明订阅者太慢，应重新同步，例如重新查询受影响的表或使用 `include_recent=true` 重新连接。

变更事件以 JSON 行的形式追加到配置的数据目录下的 `meta/change_stream.log`。启动时，Taibai 重新加载日志，恢复下一个序列号，从日志尾部重建内存中的近期缓存，并保留最新的 `change_stream.retention_events` 个事件。默认为 1,000,000 个事件。

新的日志条目以校验和保护的 JSON 记录形式写入。较旧的纯 `ChangeEvent` JSON 行仍然可读。启动时，损坏的尾部条目会被检测到，日志通过截断到最后一个有效事件来修复。

变更发布通过 `meta/change_stream_outbox.json` 的持久化发件箱协调。SQL 事务提交在提交存储写入之前将待处理的变更发布请求暂存到发件箱，在存储提交后将发件箱条目标记为已提交，然后将其排入 `change_stream.log`。启动时，已提交的发件箱条目会自动排空。`SHOW CHANGE STATUS` 暴露 `outbox_prepared` 和 `outbox_committed` 计数供运维检查。

对于可恢复的消费，在下游服务中持久化最后处理的 `sequence`，并使用设置为下一个要消费序列的 `from_sequence` 重新连接。如果请求的序列早于保留的日志，服务端返回带有最早可用序列的 `MSG_TYPE_ERROR`，消费者必须从全表快照重新同步。

投递语义是至少一次。消费者应仅在持久化下游处理后 ACK，并使用 `sequence` 加上主键行映像使处理幂等。事务性 ACK 为副作用和 ACK 状态都存储在 Taibai 中的消费者提供有效一次的行为；外部系统中的副作用仍需要自己的幂等性或事务性偏移存储。