# Taibai 性能测试与验证报告

> **状态注记（2026-09）**：本报告写于经典 WAL 时代，现行持久化已由 durable tail（组提交 + 帧级 CRC + zstd）替代经典 WAL，报告中 WAL 文件大小、WAL fsync、`wal_writers().lock()` 等讨论为历史架构记录。后续进展：
>
> - **瓶颈 #2（行式序列化）已解决**：`prepare_batch_for_wal` 现按列编码——定宽列整列 memcpy（`encode_column_raw_into`），压缩后写入 durable tail。
> - **瓶颈 #1（解析与表达式求值）已解决**：`try_exec_fast_insert_sql`（`src/sql/execution/dml_insert/fast.rs`）实现报告"方案 A"的等价物——字节级快速解析、`FastInsertPlan` 计划缓存、字面量按列类型直推（绕过 `ScalarValue` 中转）、批量预分配；不支持的形态回落完整解析路径。
> - **恢复正确性修复（2026-09）**：`decode_column_raw` 此前对 NULL 行不消费编码槽位，导致含 NULL 的列崩溃恢复后错位；已修复并新增 round-trip 契约测试（`wal_helper_test.rs`、`durable_tail_test.rs::recovered_tail_preserves_null_values_in_mixed_columns`）。
> - **重新实测（2026-09，见 §1.1）**：普通 SQL INSERT 已从 ~110k 提升到 **~192k rows/sec**（10000 行/批），实时形态（100 行/批）~14.9k rows/sec，bulk 模式 ~286k rows/sec。复现工具见 [perf_verify](../../examples/perf_verify/README.md)。
> - **实时小批次组提交修复（2026-09，见 §1.2）**：诊断发现组提交平均组大小仅 ~2，并发小批次吞吐被单语句独占 fsync 串行化；已实现自适应收集窗口与 async 模式惰性刷盘，本机（慢 fsync 环境）8 并发 strict fsync 吞吐 +57%、async 模式 +16%。

## 1. 当前状态

| 指标 | 当前值 | 目标值 | 状态 |
|------|--------|--------|------|
| SQL INSERT 导入速率 | ~110k rows/sec | 300k rows/sec | ❌ 未达标 |
| 批量导入速率 | ~300k+ rows/sec | 300k rows/sec | ✅ 已达标 |
| WAL 文件大小 | 2.8GB (64MB×44文件) | - | ⚠️ 远大于segment |
| Segment 文件大小 (SQL) | 2GB | - | ⚠️ 大于批量导入 |
| Segment 文件大小 (批量) | 1.1GB | - | ✅ 合理 |

### 1.1 重新实测（2026-09，durable tail 架构 + fast insert 路径）

环境：Windows，release 构建（`cargo build --release --workspace`），单机服务端与客户端本机回环，8 客户端并发；数据为合成的 34 列行情 tick（100 只股票轮流交替）；服务端/客户端均无调优。命令：`examples\perf_verify\perf_verify.bat 100000`。

| 场景 | 每 INSERT 行数 | 吞吐（客户端观测端到端） |
|------|--------------|------------------------|
| realtime（贴近实时写入） | 100 | ~14,900 rows/sec（~149 语句/sec） |
| import（批量导入形态） | 10,000 | ~192,000 rows/sec |
| bulk 直写（对照） | 10,000 | ~286,000 rows/sec |

结论：

- 历史值 ~110k 已过时，当前普通 SQL INSERT 在 10000 行/批下达 ~192k rows/sec（+75%）。
- realtime 与 import 形态约 13 倍差距，主要来自语句数与每语句固定开销（解析入口、tail 组提交次数）——单条 INSERT 的行数对吞吐影响远大于其他因素；实时采集侧若需更高吞吐应聚合小批次（如 5–50ms 窗口）后再提交。
- bulk 模式（~286k）与普通 INSERT（~192k）之间仍有约 1.5 倍差距，后续优化空间在 tail 提交与批量路径的合并。

### 1.2 实时小批次组提交修复（2026-09）

`tests/realtime_write_scaling_test.rs::realtime_group_commit_scaling_diagnostic`（`--ignored`，需 `--nocapture` 查看 profile 输出）以 100 行/INSERT 的实时形态做进程内测量。

**修复前根因**（insert-profile 证据）：单提交线程上组提交平均组大小仅 ~2.04（8 并发下 max=8 罕见）——固定 500µs 收集窗口抓不住错峰到达的提交请求；每条语句基本独享一次 fsync。叠加本机受安全软件干扰 fsync 高达 30–55ms，串行 fsync 链把 8 并发 strict 模式钉在 83 stmt/s。

**修复**（`src/storage/core/durable_tail.rs`）：

1. **自适应收集窗口**：提交线程记录组大小历史；检测到并发负载（前组 ≥ 2 请求）时窗口从 500µs 放大到 2ms、组上限 8，单客户端串行流维持 500µs 不受影响。
2. **async 模式惰性刷盘**：`tail_async_fsync` 下 ACK 不再等待每次 fsync——提交线程批量写入页缓存即 ACK；dirty tail 满足以下任一条件才落盘：距上次 fsync ≥ 200ms、未刷字节 ≥ 8MB、持续静默 ≥ `max(5ms, 2×实测 fsync 均价)`（自适应，上限 200ms）、或收到 Flush/Shutdown 命令。acknowledged-but-not-synced 的窗口由这些阈值界定，崩溃时最多丢最近 200ms/8MB。
3. **自适应静默阈值**：初版固定 5ms 静默即刷在慢盘上得不偿失（为 5ms 空闲付出 30–100ms 停顿，async 相位实测跌至 95 stmt/s）；改为随 fsync 实测成本缩放后，同环境恢复到 257–331 stmt/s。commit 通道断开（未经 `stop()` 的退出）也会兜底 sync 一次，避免嵌入式用法静默丢数据。

**修复后本机实测**（release，同一诊断；本机 fsync 受安全软件干扰且逐次波动 30–100ms，绝对值仅供方向参考）：

| 相位 | 修复前 stmt/s | 修复后 stmt/s（两次运行区间） | 变化 |
|------|--------------|------------------------------|------|
| 单客户端 strict fsync | 25 | 20–31 | 不变（串行流无组可攒） |
| 8 并发 strict fsync | 83 | 94–143 | +13% ~ +72% |
| 8 并发 async fsync | 187 | **257–331** | **+37% ~ +77%** |

机制证据：并发相位组平均从 ~2 提升到 ~3.8（上限 8）；async 相对 strict 的吞吐比恢复到 ~2.7 倍（惰性刷盘的预期形态）。正确性由 `concurrent_autocommit_inserts_are_visible_and_durable`（strict + async 两变体，均以 `Engine::shutdown()` 有序关停并重启校验）锁定。

## 2. 性能瓶颈分析

### 2.1 SQL INSERT 路径 (multi_insert) — 主要瓶颈

SQL INSERT 路径的数据流：
```
CSV → 解析SQL → 构建RecordBatch → WAL序列化 → WAL写入 → 列编码 → ZSTD压缩 → Segment写入
```

**瓶颈 #1: SQL 解析和表达式求值** (`dml_insert.rs:exec_insert`)
- 每个 INSERT 语句需要完整的 Parser 解析
- 每个值通过 `eval_expr_to_scalar_with_scale_timezone` 转换
- 对于5000行的批次和34列，= 170,000次表达式求值
- 估计开销：~20-30% 的总时间

**瓶颈 #2: WAL 行式序列化** (`wal_helper.rs:serialize_batch_for_wal`)
- 按行按列遍历，每次调用 `col.scalar(i)` 提取 ScalarValue
- ScalarValue 是枚举类型，每次提取都有分支判断开销
- VarChar 列会产生堆分配
- 50k行34列 = 1,700,000次 scalar 提取
- 估计开销：~25-35% 的总时间

**瓶颈 #3: WAL fsync** (非bulk模式下)
- `write_batch` 每个批次调用 `sync_all()` = fsync
- 即使在 bulk 模式下使用 `write_batch_no_sync`，WAL 仍需写入磁盘
- fsync 通常耗时 1-10ms，在 HDD 上更慢
- bulk 模式已优化此项 ✅

**瓶颈 #4: 锁竞争**
- `engine.wal_writers().lock()` — 整个 WAL 写入期间持有
- `engine.active_segments.lock()` — 整个 segment 写入期间持有
- `engine.next_txn_id().lock()` — 每批次
- `engine.catalogs().write()` — 每批次更新 row_count（bulk模式已跳过 ✅）

### 2.2 批量导入路径 (batch_import) — 已优化

```
CSV → 二进制列式编码 → Pipeline发送 → 服务端直接解析payload → ZSTD压缩 → Segment写入
```

- ✅ 跳过 SQL 解析
- ✅ 使用当前统一的 active segment 持久化路径
- ✅ Pipeline 模式（8个并发请求）
- ✅ 生产者-消费者双线程架构
- ✅ 字符串池化（security_id 零拷贝）
- ✅ 列式二进制编码

## 3. WAL 大小分析

### 3.1 WAL 数据格式（行式，未压缩）

每个 WAL 记录：
- Header: 25 bytes（seq:8 + type:1 + txn_id:8 + data_len:4 + crc:4）
- 每批次: Begin(25) + Insert(25+data) + Commit(25) = 75 bytes overhead

每行 WAL 数据（34列 tick 数据）估算：
| 列类型 | 数量 | 每行字节 |
|--------|------|----------|
| VarChar (security_id, ~6字节) | 1 | 1+4+6 = 11 |
| Int32 | 11 | 11 × (1+4) = 55 |
| Int64 | 1 | 1+8 = 9 |
| Decimal (Int64) | 17 | 17 × (1+8) = 153 |
| Timestamp | 1 | 1+8 = 9 |
| 列元数据 (per col) | 34 | 34 × (1+4) = 170 |
| **合计每行** | | **~407 bytes** |

WAL 额外开销：
- 每列：null标志(1) + 数据长度前缀(4)
- 每批次：schema 头部(8) + 列类型(34×1) + 数据长度(34×4)

### 3.2 Segment 数据格式（列式，ZSTD压缩）

- 列式编码：同类型数据连续存储，压缩率极高
- ZSTD level 3：对 Int32/Int64/Decimal 等固定宽度列可达 3-5x 压缩
- 无行级开销（无 null 标志，无行元数据）

### 3.3 大小对比

| 路径 | WAL | Segment | 总计 |
|------|-----|---------|------|
| SQL INSERT | 2.8GB | 2GB | 4.8GB |
| 批量导入 | 0 | 1.1GB | 1.1GB |
| WAL/Segment比 | 1.4x | 1.8x | 4.4x |

**WAL 是 segment 的 1.4 倍**：因为 WAL 是未压缩的行式数据
**SQL segment 是批量 segment 的 1.8 倍**：可能是数据排列差异或缺少字符串池化

## 4. 数据完整性验证

### 4.1 WAL 完整性 ✅
- CRC32 校验每个 WAL 记录
- 仅重放已提交事务（Begin+Insert+Commit 三元组）
- 截断记录被安全忽略

### 4.2 Segment 完整性 ✅
- 魔数校验（0x54414948）
- 版本校验（v3）
- seal_segment 调用 `sync_all()` 确保持久化
- flush_all_buffers 正确关闭所有活跃 segment

### 4.3 潜在风险点
1. **bulk 模式 crash 安全性**：`write_batch_no_sync` 不保证 WAL 持久化，如果 segment 也未 seal，数据可能丢失
2. **Segment 大小限制**：`MAX_SEGMENT_SIZE = 2GB`，超过后自动 seal，但 seal 前的数据在 BufWriter 中
3. **WAL 清理时机**：`WalWriter::clear()` 在 segment 持久化后调用，但当前代码中未看到自动清理逻辑

## 5. 达到 30万/秒 的优化建议

### 5.1 SQL INSERT 路径优化（如果需要）

| 优化 | 预期提升 | 复杂度 |
|------|----------|--------|
| 使用批量导入路径替代 SQL INSERT | 3-5x | 低（已实现） |
| WAL 列式序列化（替代行式） | 2-3x WAL 写入 | 中 |
| WAL 压缩 | 3-5x WAL 大小减小 | 中 |
| 增大 BufWriter 缓冲 | 10-20% | 低 |
| 减少 lock 持有时间 | 10-15% | 中 |
| 预分配 WAL buffer | 5-10% | 低 |

### 5.2 推荐方案

**方案 A（推荐）**: SQL INSERT 场景也切换到批量导入路径
- 当前批量导入已达到 300k+/sec
- 将 multi_insert 工具改为使用 `bulk_insert_columnar` API
- 或者添加一个新的 "prepared columnar insert" 协议

**方案 B**: 优化现有 SQL INSERT 路径
1. 实现 `serialize_batch_for_wal` 的列式版本
2. 在 WAL 层添加 zstd 压缩
3. 使用 `parking_lot::Mutex` 替代 `std::sync::Mutex`
4. 合并 next_txn_id 为原子操作

### 5.3 WAL 大小优化

| 优化 | 效果 |
|------|------|
| WAL 列式序列化 | 减少 20-30% |
| WAL zstd 压缩 | 减少 60-70% |
| 定期 WAL 清理（segment seal 后） | 防止无限增长 |
| 增大 segment flush 阈值 | 减少 WAL 记录数 |

## 6. 验证工具

已创建 `examples/perf_verify.rs`，运行方式：
```bash
cargo run --example perf_verify
```

该工具包含：
- 列编码性能基准
- ZSTD 压缩基准
- WAL 序列化基准
- WAL 写入基准（有/无 sync）
- Segment 写入基准
- 数据完整性验证
- WAL 完整性验证

## 7. 结论

1. **批量导入路径已达到 300k rows/sec 目标** ✅
2. **SQL INSERT 路径需要切换到批量导入或进行深度优化** ❌
3. **WAL 大小问题通过禁用 WAL（批量模式）或压缩解决**
4. **数据完整性有保障** — CRC32 校验 + 事务恢复 + sync_all 持久化
5. **关键建议：将 SQL INSERT 场景迁移到批量导入路径**
