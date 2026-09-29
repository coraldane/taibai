# 主备流复制（R1）

taibai 支持单主多备的物理操作流复制：主库把每一次持久化变更作为带序号的
事件发布，备机按序重放，收敛到与主库一致的物理状态。备机只读、可承载查询。

## 配置

```yaml
# 主库
replication:
  role: primary
  ring_capacity: 65536

# 备机
replication:
  role: standby
  primary_addr: 127.0.0.1:5151
  user: root
  password: taibai
```

- `role = none`（默认）：不参与复制。
- `role = primary`：引擎创建事件日志（`RepLog`）。**bulk/RELAXED 直写会被拒绝**——
  它们绕过 durable tail，复制流无法覆盖；复制主库只接受 DURABLE INSERT。
- `role = standby`：引擎只读（写入返回明确错误）；本地 close-of-day 调度、
  TTL 清理与自动 compaction 全部关闭（数据布局由主库的事件流决定）；
  后台 applier 连接主库持续重放。

## 事件流与序

唯一的全局串行点是 durable tail 提交线程（`TailCommitter`）。所有复制事件
都经过它或它的通道，`rep_seq` 在出队时分配，因此 **rep_seq 序 = 主库持久化
变更序**：

- `TailAppend`：durable tail V3 帧（全部 INSERT 与已提交事务增量）。提交线程
  组写盘后内联发布——帧落盘序即事件序。备机把同样的帧 fsync 进本地 tail
  文件并注册 spilled memtable（惰性解码，读路径零改动）。
- `SegmentUpdate`（`Prefix`/`Final`）：flush 后活动段的可见前缀推进、seal 后
  段定稿。事件只带元数据；备机按需用 `REPL_SEGMENT_FETCH` 拉取段字节，
  tmp+rename 原子落盘，然后清理被覆盖的 tail 与 memtable。

seq 高水位持久化在 `<data>/wal/replication_seq`（主）与
`<data>/wal/replication_applied`（备），崩溃/重启后从持久化位点续传。

## 引导与续传

1. 停主库（优雅关闭会 seal 全部 active writer）；
2. `backup` 工具备份（manifest 之后版本会记录 replication_seq）；
3. 备库以备份目录为 `data_dir` 启动，`role: standby` 指向主库；
4. applier 从持久化 applied seq 续传。

断线重连：主库在内存环形缓冲（`ring_capacity` 个事件）内自动补齐；落后
超出环形窗口时主库返回明确错误——需要重新执行 bootstrap。备机指数退避
（1s→30s）重连。

## 网络协议

复用 taibai 二进制协议（帧头 7 字节）新增消息：

| 类型 | 值 | 方向 | 载荷 |
|---|---|---|---|
| REPL_HELLO | 0x70 | 备→主 | `[user][password][last_applied_seq u64]` |
| REPL_EVENT | 0x71 | 主→备 | `[rep_seq u64][len u32][事件字节]`（不再压缩） |
| REPL_SEGMENT_FETCH | 0x72 | 备→主 | `[db][table][segment][offset u64][max_chunk u32]` |
| REPL_SEGMENT_CHUNK | 0x73 | 主→备 | `[total_len u64][offset u64][chunk]` |
| REPL_ACK | 0x74 | 备→主 | `[applied_seq u64]` |

REPL_HELLO 要求登录会话与 hello 用户一致，且用户持有 `REPLICATION` 权限
（`GRANT REPLICATION ON *.* TO ...`；root 的 ALL 覆盖）。

## R1 已知边界

- **DDL 不复制**：备机的 catalog 来自 bootstrap 备份；主库建表后需要重新
  引导（R2 的 CatalogSnapshot 事件解决）。
- **DELETE/UPDATE/compaction/TTL 事件未实现**（R2）：这些操作在主库执行后
  备机不会看到效果；R1 的事件集覆盖 INSERT 主链路与 flush/seal。
- **hot 表**：备机仅在启动时 warmup，无实时镜像。
- **故障转移为手动**：停备机 applier、去掉 `replication.role` 配置、以普通
  引擎重启即可提升；主库侧无 fencing（R1 假设单写者纪律）。
- **TLS**：复制通道当前为明文 TCP，跨不可信网络部署请在 R2 前使用隧道或
  等待 TLS 客户端支持。

## 代码索引

| 行为 | 位置 |
|---|---|
| 事件编解码 / RepLog（seq、环形缓冲、fan-out） | `src/storage/replication/mod.rs` |
| 提交线程内联发布 + 结构事件入队 | `src/storage/core/durable_tail.rs`（`TailCommitter::submit_event`、`write_append_group`） |
| flush/seal 发射点 | `src/storage/core/batch_writer.rs`、`src/storage/segment_manager/lifecycle.rs`、`buffered_write.rs` |
| 主库会话（鉴权、历史续传、段拉取） | `src/net/handler.rs`（`handle_repl_*`） |
| 备机 applier | `src/storage/replication/standby.rs` |
| 端到端测试 | `tests/replication_test.rs` |
