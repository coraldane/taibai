# 变更订阅（Pub/Sub）

Taibai 支持客户端订阅表的插入变更：数据提交后，服务端把变更批次异步推送给所有匹配的订阅者，无需轮询。设计借鉴了 kdb+ tickerplant（表级订阅、每主题单调序号、每订阅者独立队列）和 NATS（主题通配符、at-most-once 投递、慢消费者限额丢弃）。

## 能力范围

- **发布内容**：INSERT 提交的行批次（autocommit 单条/批量 INSERT、预处理语句批量执行）以及显式事务 `COMMIT` 的全部写入。通知体复用二进制查询结果编码，列式、零拷贝共享。
- **投递语义**：at-most-once。每个主题维护单调递增序号（`seq`）；客户端发现 `seq` 跳号即说明有事件被丢弃（见下面的慢消费者策略），应重新查询表数据补齐。
- **已知限制**：
  - `UPDATE` / `DELETE` 目前不产生通知（与 kdb tickerplant 的 insert-only 模型一致）。
  - `DURABILITY = RELAXED` 直写路径与 `START BULK IMPORT` 延迟批量导入不发布通知（数据延迟到 flush 才可见）。

## 协议消息

| 消息类型 | 值 | 方向 | 载荷 |
| --- | --- | --- | --- |
| `SUBSCRIBE` | `0x60` | 客户端 → 服务端 | 长度前缀字符串：主题 |
| `UNSUBSCRIBE` | `0x61` | 客户端 → 服务端 | u64：订阅 id |
| `NOTIFICATION` | `0x62` | 服务端 → 客户端（推送） | `[u64 sub_id][u64 seq][string topic][binary result batches]` |

- `SUBSCRIBE` 成功后服务端返回 `OK`，载荷为 u64 订阅 id；失败返回 `ERROR`。
- 主题支持 `table`（等价 `当前库.table`）、`database.table`、`database.*`、`>`（全库所有表）。
- `NOTIFICATION` 由服务端扇出线程预压缩一次（超过压缩阈值时），所有订阅者共享同一份字节，编解码器对该消息类型不再二次压缩。

## 权限

订阅要求 `SUBSCRIBE` 权限（或 root/admin 管理员）：

```sql
GRANT SUBSCRIBE ON TABLE main.ticks TO alice;   -- 表级
GRANT SUBSCRIBE ON DATABASE main TO alice;      -- 覆盖 main.* 与 main.>
-- 全库订阅（>）需要管理员身份
```

## 客户端 API（Rust）

```rust
let mut client = Client::connect("127.0.0.1:5151").await?;
client.login("root", "taibai", "main").await?;

let sub_id = client.subscribe("main.ticks").await?;

// 通知可能在任何请求响应之间到达；客户端库会自动缓冲，不会干扰请求/响应流。
if let Some(n) = client.recv_notification_timeout(Duration::from_secs(5)).await? {
    println!("seq={} topic={} rows={}", n.seq, n.topic, n.num_rows());
}

client.unsubscribe(sub_id).await?;
```

完整示例见 `examples/pubsub_demo`。订阅连接若长时间不发送任何协议帧，仍受 `server.connection_idle_timeout_secs` 空闲超时约束——纯订阅客户端应定期 `PING`（推送活动本身也会重置空闲窗口）。

## 实现与资源约束

- **写入路径零阻塞**：发布只做一次 relaxed 原子读（无订阅者时直接返回）加一次无锁 `try_send`；行数据以 `Arc` 共享，提交路径不做列拷贝。没有订阅者时开销接近零。
- **扇出线程**：单独的 `taibai-pubsub` 线程负责序号分配、一次性编码/压缩、按主题匹配和分发；编码结果以共享字节推给各订阅者，避免 N 份重复编码。
- **内存可控**：写入事件通道（`event_channel_capacity`）与每连接通知队列（`notification_channel_capacity`）全部有界。慢消费者的超额通知被丢弃并按订阅计数（`dropped`），绝不会拖垮写入或其他订阅者——这正是 NATS 的慢消费者隔离策略。
- **断线自愈**：连接断开时，连接任务显式注销全部订阅；即使清理被跳过，扇出线程向已关闭队列投递失败后也会自动回收订阅。

## 配置

```yaml
pubsub:
  enabled: true                        # 总开关；关闭时 SUBSCRIBE 被拒绝
  event_channel_capacity: 65536        # 写入 -> 扇出线程事件通道容量
  max_subscriptions: 1024              # 全局最大订阅数
  notification_channel_capacity: 1024  # 每连接通知队列容量
```


## 变更事件类型

通知消息按类型区分事件语义（负载布局完全一致：`[sub_id][seq][topic][行数据]`）：

| 消息类型 | 含义 | 载荷 |
|---|---|---|
| `MSG_TYPE_NOTIFICATION` (0x62) | 插入 | 新插入的行 |
| `MSG_TYPE_NOTIFICATION_DELETE` (0x64) | 删除 | 被删除的行（删除提交后发布） |
| `MSG_TYPE_NOTIFICATION_UPDATE` (0x65) | 更新 | 更新后的行（新值，重写提交后发布） |

说明：
- DELETE/UPDATE 事件只在存在订阅者时才产生（否则零开销，DELETE 保持纯 tombstone 路径）。
- 有订阅者时，DELETE 需要读取被删行的全部列来构造事件（无订阅者时仍使用仅键列投影的优化路径）。
- 旧版本客户端会把 0x64/0x65 当作未知消息类型；客户端与服务端需一同升级。

## 可靠订阅（SUBSCRIBE_V2）

在一的通知模型之上，taibai 还提供**可恢复**的表级订阅：服务端把已提交的插入批次写入每表的有界保留日志，客户端按连续序号接收、确认（ack），断线后可以从上次处理到的序号继续，也可以用**具名书签**跨连接续传。

### 协议消息

| 消息类型 | 值 | 方向 | 载荷 |
| --- | --- | --- | --- |
| `SUBSCRIBE_V2` | `0x66` | 客户端 → 服务端 | `[string db][string table][u8 mode][u64 from_seq][string named][u8 has_filter][过滤]` |
| `SUB_CONFIRMED` | `0x67` | 服务端 → 客户端 | `[u64 sub_id][u64 epoch][u64 granted_from][u64 floor]` |
| `SUB_DATA` | `0x68` | 服务端 → 客户端（推送） | `[u64 sub_id][u64 seq_start][u64 seq_end][u32 batch_count][u32 len][批量插入编码]...` |
| `SUB_ERROR` | `0x69` | 服务端 → 客户端（推送） | `[u64 sub_id][u8 code][u64 aux][string message]` |
| `SUB_ACK` | `0x6A` | 客户端 → 服务端 | `[string db][string table][u64 sub_id][u64 seq]` |
| `UNSUBSCRIBE_V2` | `0x6B` | 客户端 → 服务端 | `[string db][string table][u64 sub_id]` |

- `mode`：`0` = 从当前头部开始；`1` = 从 `from_seq` 之后开始（服务端回放保留窗口内的历史，`granted_from` 会被钳制到当前头部）。
- `named`：具名订阅。每次 ack 定期把序号写入 `<数据目录>/pubsub/` 下的书签文件，之后用同名订阅自动从书签处续传。
- `has_filter`：列过滤。只投递该列文本值命中的行（NULL 不匹配）；订阅时校验列存在。
- 订阅失败（无权限、表/列不存在、pubsub 关闭）以普通 `ERROR` 应答；`SUB_ERROR` 只用于异步推送，`code = 1` 表示**需要重新同步**（`aux` 为保留下限 `floor`）：保留窗口已覆盖不到该订阅，客户端应从 `floor` 重新订阅并自行补齐缺口。
- `SUB_DATA` 的载荷与 `BULK_INSERT_COLUMNAR` 的批次编码一致，客户端无需访问目录即可解码。

### 投递与保留语义

- 每表通道维护单调序号；`seq_start..=seq_end` 在单帧内连续，服务端会把多个批次合并进一帧。
- 保留日志按 `log_max_batches` / `log_max_bytes_mb` 有界；被逐出的序号会让落后订阅者收到重同步通知。
- 断开连接（或显式退订）期间的提交仍会进入保留日志，所以重连后能补齐——这正是书签续传的前提。
- 通道在无订阅者且空闲超过 60 秒后被**退役**：保留日志释放、写入路径停止镜像；再次订阅会自动重新激活。序号永不重置（`epoch` 仅在服务重启后变化）。
- 事务提交、autocommit 插入、批量导入可见化都会进日志；`UPDATE` / `DELETE` 变更仍只走一代通知。

### 配置

```yaml
pubsub:
  # ... 一代配置之外，新增：
  commit_ring_capacity: 65536   # 写入 -> publisher 线程的提交环形队列
  poll_interval_ms: 50          # 投递任务无唤醒时的兜底轮询间隔
  log_max_batches: 1024         # 每表保留日志的批次数上限
  log_max_bytes_mb: 64          # 每表保留日志的字节数上限
  max_batches_per_frame: 32     # 单个 SUB_DATA 帧合并的批次上限
```

### 客户端 API（Rust）

```rust
let grant = client
    .subscribe_v2("main", "ticks", None, Some("worker-a"), Some(("symbol", &["AAPL"][..])))
    .await?;

loop {
    match client.recv_sub_event_timeout(Duration::from_secs(5)).await? {
        Some(SubEvent::Data(frame)) => {
            // frame.seq_start..=frame.seq_end, frame.batches
            client
                .ack_subscription("main", "ticks", grant.sub_id, frame.seq_end)
                .await?;
        }
        Some(SubEvent::ResyncRequired { sub_id, floor }) => {
            // 保留窗口覆盖不到了：从 floor 重新订阅并重查表数据
            break;
        }
        None => { /* 暂无事件 */ }
    }
}

client.unsubscribe_v2("main", "ticks", grant.sub_id).await?;
```
