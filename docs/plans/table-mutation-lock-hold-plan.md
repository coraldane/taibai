# 表变更锁长期持有的根治方案

## 背景：2026-10-13 生产表现

- `base_increment:t_stock_minute` 全天出现 162 次 slow-acquisition 告警，平均等待
  37.3s、最长 41.2s，从 15:31 持续到 20:51（日志末尾），直接导致 data-synchro
  PADDING_MINUTE 的 JDBC 写（30s read timeout）连环超时，149 只证券分钟线未补齐。
- `base_realtime:stock_minute` 在 close-of-day 归档（15:30 收盘触发）期间：
  556 万行刷盘封段 → 分区重写 → `unique index snapshot rebuild`（entries=9,736,427，
  29.1s 全表扫描），全程在 `with_table_mutation_lock` 临界区内
  （`src/sql/execution/engine.rs` 的 `rebuild_compacted_location_indexes`），
  同表写入被阻塞 4.8s+。
- `base_increment:t_stock_minute` 的 30-40s 级持有者**无法从现有日志归因**：
  持有侧没有任何告警（只有等待侧的 slow-acquisition），写入又走 prepared/binary
  通道，文本日志里没有对应语句。

## 已落地（本次代码改动）

1. **持有侧告警**：`src/storage/segment_manager/manager_core.rs`
   `with_state_table_mutation_lock` 现在在持锁超过
   `mutation_lock_warn_secs`（默认 1s）时输出
   `table mutation lock held for a long critical section (table, hold_ms)`。
   覆盖所有走该包装的后台持有者（tail flush、压缩、重建、归档）。
   下次再出现 30s 级等待，按时间戳对齐即可直接锁定持有者。

## 待落地（按优先级）

### P0：close-of-day / 压缩重建移出锁临界区 —— **已落地（两阶段重建）**

实现：`dml_insert/write.rs` 的 `rebuild_table_location_indexes_two_phase`，
接入 `engine.rs::rebuild_compacted_location_indexes`（后台压缩）与
`ddl/introspection.rs::exec_optimize_table`（OPTIMIZE TABLE 全表维护）：

1. **锁外阶段**：用 `discover_inactive_segments` 枚举全部不可变段并记录
   指纹 `(row_count, deleted_row_count)`，把唯一索引条目流式写入
   `Runs` 有界外部排序 scratch（即历史上 29s 的主体）。
2. **锁内阶段**：复核指纹与 `memtable_flush_epoch`；一切未变则只补扫
   锁外阶段之后新出现的不可变段、活跃段与全部 memtable，然后原子发布
   base（`unique_index_stream_publish`，重复键保留后者）。
3. **回退**：删除/重写/压缩/刷盘 epoch 任一在锁外阶段插手
   （指纹变化、段消失、epoch 漂移），整体回退为传统的锁内全扫
   （`rebuild_table_location_indexes_from`），正确性从不依赖快路径。
   发布时 base 携带锁内读取的 flush epoch 与段水位，与既有
   "快照过期→惰性全量重建"兜底机制完全兼容。

安全性论证（相对旧实现的并发模型）：
- 不可变段 reader 是打开时快照（删除走 `invalidate_reader` 重开，
  不原地改位图），锁外读无数据竞争；
- append-only 负载（分钟线/日线主场景）不触发任何回退条件，
  热表在持续写入下仍走快路径；
- DELETE（表锁或分区锁）改不可变段 footer → 指纹 deleted_count 变化 → 回退；
  UPDATE/压缩重写 → 旧段消失 → 回退；memtable flush → epoch 变化 → 回退。

验收标准：
- 1000 万行表触发 close-of-day 重写期间，并发 50ms 间隔单条 INSERT 的
  变更锁等待 p99 < 500ms；
- 重写前后唯一索引快照与表内容一致性校验通过（可复用
  `segment_checker` / 现有 property test 扩展）；
- 已有回归：`dml_insert/write_two_phase_test.rs` 3 个用例
  （两阶段覆盖不可变段+memtable、OPTIMIZE 接入、删除竞态指纹检测）。

### P1：大批量回填错峰与限流

- 已核实并落地：ricequant 分钟线抓取的调度方是 data-synchro
  `VendorFetchTask`（cron 可配），生产实际在跑（10/13 日志：15:20 抓取
  STOCK_MINUTE 1,250,880 行，写入 `t_stock_minute_ricequant` 的行重写
  15:22–15:35 与盘后 padding 窗口完全重叠）。已把权益分钟抓取默认 cron
  从 `0 20 15` 改为 `0 55 15`：padding 日线批 15:43 收尾后、16:25 期货
  抓取前、16:40 归并前完成。
- 注意：仓库内 `application-prod.yml` 无 `project.vendor` 配置（默认
  `enabled:false`），但生产实际已启用——**生产服务器的生效配置与仓库
  不一致**，发布时需核对该环境的 application.yml，确保新 cron 真正生效。
- close-of-day 错峰不再需要：29s 锁持有的根因已由两阶段重建根治
  （见上文 P0）。
- bulk 写入单次 apply 行数维持 ≤25 万行（`PipelineColumnarWriter` 已有注释
  说明 25 万行 apply 可能超过服务端 60s 锁超时），回填路径如有更大批次需要收敛。

### P2：客户端超时对齐

- data-synchro PADDING 任务的 JDBC read timeout（30s）小于服务端慢锁等待
  常态（37s+），导致客户端超时放弃但服务端继续执行（OutcomeUnknown），
  队列越积越多。在锁饥饿根治前，把 padding 写超时提到 ≥60s，
  并在任务层面对 `OutcomeUnknown` 做"查实后重试"而不是盲目重发。

## 观测验收

- 压测复现：对同一张分钟表并发跑"close-of-day 重写 + 持续单条写入"，
  确认 slow-acquisition 告警消失或 wait_ms < 500；
- 新的 held-for-a-long-critical-section 告警在压测中能准确指出持有阶段
  （刷盘/重写/重建）与时长。
