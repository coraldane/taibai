# Monax 示例工具使用指南

本文档介绍 Monax 数据库 `examples` 目录下的各类工具的使用方法。

## 目录

- [导入工具](#导入工具)
  - [multi_insert](#multi_insert)
  - [prepare_test](#prepare_test)
  - [batch_import](#batch_import)
- [调试工具](#调试工具)
  - [check_segment](#check_segment)
- [性能测试](#性能测试)
  - [minimal_test](#minimal_test)
- [性能对比](#性能对比)

---

## 导入工具

### multi_insert

**用途**: 使用批量 INSERT SQL 语句导入股票 Tick 数据。

**特点**:
- 使用 `INSERT INTO table VALUES (...), (...), ...` 多值 SQL 语句
- 支持 Pipeline 模式异步发送
- 可选 Bulk Import 模式优化写入
- 字符串参数在 SQL 中需要转义

**编译**:
```bash
cargo build --release --package multi_insert
```

**使用**:
```bash
# 基本使用
multi_insert 127.0.0.1:5151 /path/to/tick.csv

# 指定批量大小和数据库
multi_insert 127.0.0.1:5151 tick.csv --batch-size=5000 --database=main

# 启用 Bulk Import 模式（更高性能）
multi_insert 127.0.0.1:5151 tick.csv --bulk

# 限制最大导入行数
multi_insert 127.0.0.1:5151 tick.csv --max=100000

# 全部参数
multi_insert <host> <csv_path> \
  [--batch-size=<n>] \      # 每批次行数，默认 5000
  [--no-header] \           # 跳过 CSV 第一行
  [--database=<db>] \       # 数据库名，默认 main
  [--drop] \                # 导入前删除表（默认）
  [--no-drop] \             # 不删除表
  [--max=<n>] \             # 最大导入行数
  [--pipeline=<n>] \        # Pipeline 深度，默认 4
  [--bulk]                  # 启用 Bulk Import 模式
```

**输出示例**:
```
[2024-01-15 10:30:00] Connecting to 127.0.0.1:5151 ...
[2024-01-15 10:30:00] Logged in to database 'main'.
[2024-01-15 10:30:00] Dropped table stock_tick_multi.
[2024-01-15 10:30:00] Created table stock_tick_multi.
[2024-01-15 10:30:05] Progress: read=50000 parsed=50000 sent=50000 (10000 rows/sec)
[2024-01-15 10:30:15] Import completed: 100000 rows in 10.00s (10000 rows/sec)
```

**表结构**: `stock_tick_multi` (34列股票行情数据)

---

### prepare_test

**用途**: 使用 Prepared Statement + 二进制参数绑定导入股票 Tick 数据。

**特点**:
- 预编译 INSERT 语句，绑定二进制参数
- 支持 `execute_prepared_batch` 批量执行
- 二进制参数无需 SQL 转义，更安全
- 适合测试 Prepared Statement 性能

**编译**:
```bash
cargo build --release --package prepare_test
```

**使用**:
```bash
# 基本使用
prepare_test 127.0.0.1:5151 /path/to/tick.csv

# 指定批量大小
prepare_test 127.0.0.1:5151 tick.csv --batch-size=1000

# 全部参数
prepare_test <host> <csv_path> \
  [--batch-size=<n>] \      # 每批次行数，默认 1000
  [--no-header] \           # 跳过 CSV 第一行
  [--database=<db>] \       # 数据库名，默认 main
  [--drop] \                # 导入前删除表（默认）
  [--no-drop] \             # 不删除表
  [--max=<n>]               # 最大导入行数
```

**输出示例**:
```
[2024-01-15 10:30:00] Connecting to 127.0.0.1:5151 ...
[2024-01-15 10:30:00] Logged in to database 'main'.
[2024-01-15 10:30:00] Created table stock_tick_prep.
[2024-01-15 10:30:00] Prepared INSERT statement: stmt_id=1, param_count=34
[2024-01-15 10:30:05] Progress: read=10000 parsed=10000 sent=10000 (2000 rows/sec)
[2024-01-15 10:30:15] Import completed: 100000 rows in 50.00s (2000 rows/sec)
```

**表结构**: `stock_tick_prep` (与 multi_insert 相同的 34列结构)

---

### batch_import

**用途**: 使用列式二进制协议导入股票 Tick 数据（最高性能）。

**特点**:
- 使用 `MSG_TYPE_BULK_INSERT_COLUMNAR` 列式二进制协议
- Producer-Consumer 双任务架构
- String Interning 优化（security_id 零重复分配）
- Pipeline + Bulk Import 双重优化
- Block-level I/O 读取

**编译**:
```bash
cargo build --release --package batch_import
```

**使用**:
```bash
# 基本使用
batch_import 127.0.0.1:5151 /path/to/tick.csv

# 指定批量大小
batch_import 127.0.0.1:5151 tick.csv --batch-size=200000

# 全部参数
batch_import <host> <csv_path> \
  [--batch-size=<n>] \      # 每批次行数，默认 200000
  [--no-header] \           # 跳过 CSV 第一行
  [--database=<db>]         # 数据库名，默认 main
```

**输出示例**:
```
[2024-01-15 10:30:00] Connecting to 127.0.0.1:5151 ...
[2024-01-15 10:30:00] Logged in to database 'main'.
[2024-01-15 10:30:00] Table 'stock_tick_batch' ensured.
[2024-01-15 10:30:00] Starting bulk import mode...
[2024-01-15 10:30:00] Starting pipeline with depth=8...
[2024-01-15 10:30:05] Progress: 200000 rows sent (40000 rows/sec)
[2024-01-15 10:30:15] Import completed: 1000000 rows in 25.00s (40000 rows/sec)
[2024-01-15 10:30:15] All pipeline responses received.
[2024-01-15 10:30:15] Stopping bulk import...
[2024-01-15 10:30:15] Flushed 0 buffered rows.
```

**表结构**: `stock_tick_batch` (34列股票行情数据)

---

## 调试工具

### check_segment

**用途**: 检查 Monax Segment 文件的元数据和结构。

**特点**:
- 显示文件基本信息（路径、大小）
- 解析文件头 32 字节
- 定位并解析 Footer 偏移量
- 校验 Magic Number (0x54414948)

**编译**:
```bash
cargo build --release --example check_segment
```

**使用**:
```bash
# 检查指定 Segment 文件
check_segment main/my_table/seg_00001.dat

# 默认路径
check_segment  # 默认: main/benchmark_test/seg_00001.dat
```

**输出示例**:
```
File: main/my_table/seg_00001.dat (1048576 bytes)
First 32 bytes: 01 DB 23 00 ...
Footer offset: 1048000
Footer first 32 bytes: 48 41 49 54 ...
Magic: 0x54414948 (expected: 0x54414948)
Version: 1
```

---

## 性能测试

### minimal_test

**用途**: 基础性能测试，验证 Zstd 压缩性能。

**特点**:
- 生成 100,000 个 i64 值
- 测试 Zstd 压缩速度和压缩率

**编译**:
```bash
cargo build --release --example minimal_test
```

**使用**:
```bash
minimal_test
```

**输出示例**:
```
Starting benchmark...
Created 100000 i64 values in 1.5ms
Compressed to 1200 bytes in 5.2ms
Benchmark complete!
```

---

## 性能对比

三种导入工具的性能特点对比：

| 工具 | 协议 | 优点 | 适用场景 |
|------|------|------|----------|
| `multi_insert` | SQL 文本 | 简单易懂，兼容性好 | 小批量导入、调试 |
| `prepare_test` | Prepared Statement | 参数绑定安全，减少解析开销 | 中等批量、参数化场景 |
| `batch_import` | 列式二进制 | 最高性能，零拷贝优化 | 大批量生产导入 |

**典型性能对比** (100万行 Tick 数据):

| 工具 | 默认配置 | 启用优化后 |
|------|---------|-----------|
| `multi_insert` | ~10,000 rows/sec | ~20,000 rows/sec (--bulk) |
| `prepare_test` | ~5,000 rows/sec | - |
| `batch_import` | ~40,000 rows/sec | - |

**选择建议**:

1. **开发调试**: 使用 `multi_insert`，SQL 可读性好
2. **参数化导入**: 使用 `prepare_test`，适合需要动态参数的场景
3. **生产大批量**: 使用 `batch_import`，性能最优

---

## CSV 数据格式

股票 Tick 数据 CSV 格式要求：

```csv
security_id,deal_date,deal_time,pre_close,open_px,high_px,low_px,close_px,high_limit,low_limit,total_num_trade,trans_volume,trans_value,ask_px1,...,local_time
000001,20240115,93000000,10.50,10.60,10.80,10.55,10.75,11.50,9.50,1000,50000,537500,10.70,...,2024-01-15 09:30:00.000
```

**字段说明**:
- `security_id`: 股票代码 (VARCHAR)
- `deal_date`: 交易日期 (INT32)
- `deal_time`: 交易时间 (INT32)
- `pre_close` ~ `close_px`: 价格字段 (DECIMAL(9,2))
- `trans_volume`: 成交量 (INT64)
- `trans_value`: 成交额 (DECIMAL(18,2))
- `ask_px1` ~ `bid_vol5`: 五档行情
- `local_time`: 本地时间戳 (TIMESTAMP)

---

## 前置条件

所有导入工具需要：

1. **Monax 服务已启动**: 默认监听 `127.0.0.1:5151`
2. **认证信息**: 默认用户 `root`，密码 `taibai`
3. **数据库存在**: 默认数据库 `main`

启动 Monax 服务：
```bash
monax-server --config conf/example.yaml
```