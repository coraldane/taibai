# 普通 INSERT 持久化路径磁盘 I/O 优化方案

> 计划文档。状态：**提案，未实施**。
> 背景：tail 文件堆积问题已由三道防线消除（Flush 即时 drain / 周期转储 / 每表 256
> 文件高水位），memtable spill 与重启惰性恢复已实施（见 `../design/TAIBAI_DESIGN_ZH.md`
> MemTable 一节）；本方案只针对**残余的磁盘 I/O 开销**。

## 1. 现状与开销定位

auto-commit INSERT 的完整持久化路径（`batch_writer.rs:392-411`）：

```
INSERT batch
  ├─ memtable.append（内存）
  ├─ append_memtable_tail：group commit（500µs / 16MB / 128 请求聚合，durable_tail.rs:29-31）
  │    └─ zstd WAL 帧追加 + fsync，ack 前完成            ← 第 1 次写 + 第 1 类 barrier
  └─ 后台 flush（满 8192 行 / 30s 年龄 / 256 文件高水位）
       ├─ 读 resident batch → 列编码 → zstd → 写 segment row group
       │    （tail 帧是 WAL 行式序列化，与行组编码不兼容 → 必须 解码→重编码，仅 CPU）
       └─ 行组落盘 sync_all + finish_flush 删 tail       ← 第 2 次写 + 第 2 类 barrier
```

对照 bulk deferred 路径（`exec_bulk_insert_partitioned_with_wal_control`，
`bulk_insert_batch_partitioned_deferred`）：**单写**——直接进
ActiveSegmentWriter 行组缓冲，攒满 `row_group_target_rows` 切组并 sync，无 tail、
无 WAL；崩溃窗口 = 缓冲中未成组的行。

| # | 开销 | 量级 | 代码锚点 |
|---|------|------|----------|
| C1 | **双写**：每行先 tail 后 segment，字节数 2×，两套 zstd | 与导入量成正比 | `write_batch_by_partition` vs `exec_bulk_insert_partitioned_with_wal_control` |
| C2 | **双 barrier**：tail group-commit fsync + 行组 sync_all | fsync 次数 ≈ 2× 批次数/聚合比 | `durable_tail.rs:283`、`active_writer.rs:315,321` |
| C3 | **小行组**：`row_group_target_rows` 默认/示例均 8192（≈1MB，zstd-1） | 元数据占比、扫描放大 | `config.rs:22`、`example.yaml:14` |
| C4 | flush 时 解码→重编码（tail 帧 ≠ 行组编码） | CPU，非磁盘 | 本方案 P1 提案消除 |

已确认**不是**问题的：tail 文件数（三道防线：Flush 即时 drain / 30s 周期 / 每表 256
文件高水位，`batch_writer.rs:680,197,782`）；小行组碎片（ActiveSegmentWriter 会把
不满的 memtable flush 合并缓冲后再切组，`active_writer.rs:64,239`）。

## 2. 优化提案（按投入产出排序）

### P0 — 行组阈值调参（改配置，即时收益）

`row_group_target_rows` 8192 → 65536（tick 类大批量场景）。

- 收益：行组数/元数据占比降 8×；memtable 滚动次数、tail generation 更替次数同比例
  下降；flush 周期数下降。
- 代价：在写 memtable 常驻内存上限升高（= 阈值 × 活跃分区数）；spill 机制（已实施）
  会把冷 committed memtable 释放为元数据，内存可控。
- 注意：`max_rows_per_memtable` 与该值绑定（`engine/core.rs:103`），
  `low_rate_max_rows = 阈值 - 1`（`core.rs:106`）；低频表由 `get_flushable` 的年龄
  阈值兜底，不受影响。
- 回退：纯配置，改回即回退。

### P1 — tail 帧列存同构 + flush 字节拼接（消除 C4，软化 C1/C2 的写放大）

tail v3 帧 = 与 segment 行组**同构**的列式 zstd chunk。flush 从"解码→重编码"变为
"tail 字节顺序拼接进 segment + footer + 一次 sync"，随后删 tail。

- 收益：flush 的 CPU 解码/重编码归零；写 I/O 变为纯顺序大块拷贝（macOS
  `clonefile` / Linux `copy_file_range` 可进一步降拷贝开销）。
- 局限：瞬态字节仍是 2×（tail 在 flush 前存在）——只把"双写"变廉价，未消除。
- 改动面：`durable_tail.rs` 帧编码与当前 magic；`flush_memtable_group`
  拼接路径；恢复路径复用 generation 仲裁不变。
- 格式切换不保留旧版解码路径，已有数据需重新导入。

### P2 — 多值 INSERT 自动直写（结构性消除 C1，需解决恢复语义）

识别"大批量多值 INSERT"（如 ≥1024 行/语句）自动走 bulk deferred 直写路径，无需
客户端显式 `START BULK IMPORT`。两个子方案：

- **P2a — relaxed durability 表选项**（低风险）：建表时声明
  `DURABILITY = RELAXED`（文档化崩溃丢失窗口 = 行组缓冲），此类表的大批量 INSERT
  自动直写。默认表语义完全不变。适合行情/日志类可再生数据。
- **P2b — 逐批 fsync 直写**（中等复杂度，收益最大）：直写路径每批对 active segment
  `sync_all` 后 ack——barrier 数与 tail 路径相同、字节数减半。难点是崩溃恢复：无
  tail 可回放，需要"扫描未 footer 的已提交行组 vs catalog 行数"的 segment 侧恢复
  （现有 `.flush` intent/watermark 机制依赖 tail，不能直接复用）。

### P3 — durability 分级 / 异步 ack（软化 C2）

`synchronous_commit` 风格的会话/表选项：tail 写入 page cache 即 ack，fsync 由
committer 周期 barrier（窗口 ≤ 500µs~2ms）兜底。group commit 窗口本就 500µs，实际
丢失窗口极小；对 fsync 敏感盘（非 NVMe）吞吐收益显著。实现小，风险低。

### P4 — 自适应 group-commit 窗口（低优先级）

`GROUP_COMMIT_MAX_DELAY` 按观测 fsync 延迟动态调整（延迟高→窗口放大聚合更多）。
NVMe 上通常不是瓶颈，仅在 P3 之后仍有 fsync 瓶颈时考虑。

### P5 — 研究方向：log-is-the-segment

活动 segment 文件本身即 WAL（append-only 行组 chunk + 提交水位 footer），彻底单写、
单 barrier。与 2PC、REPLACE 冲突检测、DELETE/UPDATE tombstone 的交互面大，仅在
P1/P2 验证后评估。

## 3. 明确不做

- 不放宽每表 256 tail 文件高水位——它是目录膨胀的最后防线；
- 不让普通路径按批切小行组——ActiveSegmentWriter 的合并缓冲是正确设计；
- 不为省 fsync 而去掉 tail ack 语义的默认值——默认表必须保持现有崩溃不变式。

## 4. 建议落地顺序与验证

| 步骤 | 内容 | 验证 |
|------|------|------|
| 1 | P0 调参 | `multi_insert --no-bulk` A/B：iostat 写字节/fsync 数、`memtable_tail/` 文件数曲线、`check_segment` 行组数与大小分布、导入行/秒 |
| 2 | P3 异步 ack（若 fsync 敏感） | 同上 + 崩溃注入：kill -9 后重放丢失行数 ≤ 窗口 |
| 3 | P2a relaxed 表选项 | 导入表 A/B 吞吐 + kill -9 丢失窗口实测 |
| 4 | P1 tail 列存拼接 | flush 延迟剖析（`insert_profile` 日志）前后对比 |
| 5 | P2b / P5 | 单独立项设计评审 |
