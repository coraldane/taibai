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
- `DeleteFooterRewrite`：DELETE/REPLACE 的段级墓碑。位图是段的绝对删除
  状态（非增量），备机幂等重写本地 footer；段尚未送达时先排队，段到位后
  补应用。
- `SegmentsReplaced`：compaction/UPDATE 的段替换。备机拉取新段、删除退役段。
- `SegmentsRemoved`：TTL 分区清理删除的段文件。
- `CatalogSnapshot`：DDL 后整库 catalog JSON 快照（空载荷 = DROP DATABASE）。
  备机原子替换并重载内存 catalog——**主库建表后备机立即可查询**。
- `AuthSnapshot`：用户/角色/授权语句后的 auth.json 快照。

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
| REPL_SEGMENT_GONE | 0x75 | 主→备 | 空。请求的段已被 compaction/TTL 淘汰（事件环可证明）；备机跳过触发它的 `SegmentUpdate`，行数据由流中稍后的 `SegmentsReplaced`/`SegmentsRemoved` 交付。无退休记录的缺失文件仍返回 ERROR |

REPL_HELLO 要求登录会话与 hello 用户一致，且用户持有 `REPLICATION` 权限。
`REPLICATION` 是全局权限：`GRANT REPLICATION TO repl_user;`（省略 ON，或用
`*.*` 通配对象；带 `ON TABLE`/`ON DATABASE` 会报错）。root 的 `ALL` 覆盖该检查。

## 已知边界

- **hot 表**：备机仅在启动时 warmup，无实时镜像。
- **故障转移为手动**：停备机 applier、去掉 `replication.role` 配置、以普通
  引擎重启即可提升；主库侧无 fencing（假设单写者纪律）。
- **TLS**：复制通道当前为明文 TCP，跨不可信网络部署请使用隧道。
- **DROP TABLE 的段文件清理**：catalog 快照使表在备机立即不可见，遗留的
  段文件待未来 GC 事件清理。
- **引导仍需短暂停主库**：在线快照（不停机 bootstrap）是后续项。

## 代码索引

| 行为 | 位置 |
|---|---|
| 事件编解码 / RepLog（seq、环形缓冲、fan-out） | `src/storage/replication/mod.rs` |
| 提交线程内联发布 + 结构事件入队 | `src/storage/core/durable_tail/committer.rs`（`TailCommitter::submit_event`、`write_append_group`） |
| flush/seal 发射点 | `src/storage/core/batch_writer.rs`、`src/storage/segment_manager/lifecycle.rs`、`buffered_write.rs` |
| 主库会话（鉴权、历史续传、段拉取） | `src/net/handler.rs`（`handle_repl_*`） |
| 备机 applier | `src/storage/replication/standby.rs` |
| 端到端测试 | `tests/replication_test.rs` |
