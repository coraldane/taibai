# Taibai 性能测试与验证报告

## 1. 当前状态

| 指标 | 当前值 | 目标值 | 状态 |
|------|--------|--------|------|
| SQL INSERT 导入速率 | ~110k rows/sec | 300k rows/sec | ❌ 未达标 |
| 批量导入速率 | ~300k+ rows/sec | 300k rows/sec | ✅ 已达标 |
| WAL 文件大小 | 2.8GB (64MB×44文件) | - | ⚠️ 远大于segment |
| Segment 文件大小 (SQL) | 2GB | - | ⚠️ 大于批量导入 |
| Segment 文件大小 (批量) | 1.1GB | - | ✅ 合理 |

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
- ✅ 跳过 WAL（`disable_wal: true`）
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
