# Taibai 示例工具使用指南

本文档介绍 Taibai 数据库 `examples` 目录下的各类工具的使用方法。

## 目录

- [导入工具](#导入工具)
  - [multi_insert](#multi_insert)
  - [prepare_test](#prepare_test)
  - [batch_import](#batch_import)
  - [insert_csv](#insert_csv)
  - [load_csv](#load_csv)
  - [hot_insert](#hot_insert)
- [导出工具](#导出工具)
  - [export_csv](#export_csv)
- [订阅示例](#订阅示例)
  - [pubsub_demo](#pubsub_demo)
- [调试工具](#调试工具)
  - [check_segment](#check_segment)
  - [count_rows](#count_rows)
- [性能测试](#性能测试)
  - [minimal_test](#minimal_test)
  - [query_bench](#query_bench)
  - [perf_verify](#perf_verify)
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

### insert_csv

**用途**: 通过普通 pipeline INSERT 语句导入 CSV（与 `export_csv` 配套），走 memtable + 热存镜像写路径。

**特点**:
- 读取 `export_csv` 导出的 CSV（表头 + 明文值），按列类型解析
- 多值 INSERT 批量提交，pipeline 异步发送
- 与 `hot_insert` 相同的写入路径，适合验证实时写入形态

**编译**:
```bash
cargo build --release --package insert_csv
```

**使用**:
```bash
insert_csv <host> <database> <table> <csv_path> [batch_rows] [max_rows] [--no-header]
# batch_rows 默认 2000
```

**说明**: CSV 需与目标表列结构一致（默认读取表头行）。演示三列形态（id, gmt_create, security_id 等）之外的表时需调整源码中的列映射。

---

### load_csv

**用途**: 通过列式 bulk 二进制协议批量导入 CSV（与 `export_csv` 配套），走直写 segment 的高吞吐路径。

**特点**:
- 先 `SHOW COLUMNS` 拉取目标表结构，按实际类型解析 CSV
- 使用 `encode_columnar_bulk_insert` 编码列式批量消息
- 大批次（默认 200000 行/批）+ bulk 模式

**编译**:
```bash
cargo build --release --package load_csv
```

**使用**:
```bash
load_csv <host> <database> <table> <csv_path> [batch_rows]
# batch_rows 默认 200000
```

---

### hot_insert

**用途**: 将股票 tick CSV 持续写入常驻热存表（`HOT` 表），演示实时行情落库形态。

**特点**:
- 使用普通 INSERT 多值语句 + pipeline，多客户端并发（默认 8 连接 × 深度 8）
- 建表自动带 `HOT` 属性与收盘时间（`--close-at`），当日分区点查走内存索引
- 默认批次 20000 行，贴近 NVMe 组提交的最优吞吐形态

**编译**:
```bash
cargo build --release --package hot_insert
```

**使用**:
```bash
hot_insert <host> <csv_path> \
  [--batch-size=<n>] \      # 每批次行数，默认 20000
  [--no-header] \           # 跳过 CSV 第一行
  [--database=<db>] \       # 数据库名，默认 main
  [--pipeline=<n>] \        # 每客户端 pipeline 深度，默认 8
  [--clients=<n>] \         # 并发客户端数，默认 8
  [--close-at=HH:MM] \      # HOT 表收盘时间，默认 15:30
  [--drop] \                # 导入前删除表
  [--max=<n>]               # 最大导入行数，0 为不限
```

**表结构**: `hot_tick` (34列股票行情数据，HOT 属性)

---

## 导出工具

### export_csv

**用途**: 把任意查询结果导出为 CSV 文件。

**特点**:
- 直接接收 SQL，输出带表头的明文 CSV
- 流式写文件（BufWriter，默认每 200000 行刷一次盘）
- 含逗号、引号、换行的值自动转义

**编译**:
```bash
cargo build --release --package export_csv
```

**使用**:
```bash
export_csv <host> <database> "<sql>" <output.csv> [flush_every_rows]
```

**示例**:
```bash
export_csv 127.0.0.1:5151 main "SELECT * FROM stock_tick_multi" out.csv
```

导出的 CSV 可用 `insert_csv`（普通写入路径）或 `load_csv`（bulk 路径）重新导入。

---

## 订阅示例

### pubsub_demo

**用途**: 演示变更订阅：一个连接订阅表，另一个连接持续 INSERT，订阅端实时收到服务端推送。

**编译**:
```bash
cargo build --release --package pubsub_demo
```

**使用**:
```bash
pubsub_demo [host:port]    # 默认 127.0.0.1:5151
```

示例自动建表 `demo_ticks`、订阅并写入 5 批数据，然后打印收到的通知。协议与权限细节见[变更订阅文档](../user-guide/pubsub.md)。

---

## 调试工具

### check_segment

**用途**: 检查 Taibai Segment 文件的元数据和结构。

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

### count_rows

**用途**: 快速执行一条查询并打印结果行，常用于导入后核对行数。

**使用**:
```bash
cargo run --release --example count_rows -- [host:port] ["sql"]
# 默认查询 SELECT count(*) FROM stock_tick_multi
# SQL 中的空格可用 + 代替
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

### query_bench

**用途**: 对单条 SQL 反复执行并输出耗时统计，用于查询性能对比。

**使用**:
```bash
cargo run --release --package query_bench -- [host] [database] [sql|@sql文件] [repeat] [--print-rows]
# repeat 默认 2 次；@path 从文件读取 SQL；--print-rows 额外打印每行结果
```

**示例**:
```bash
query_bench 127.0.0.1:5151 main "SELECT count(*) FROM stock_tick_multi" 5
```

---

### perf_verify

**用途**: SQL INSERT 吞吐端到端实测工具，覆盖实时（100 行/批）、导入（10000 行/批）与 bulk 三种写入形态，是[性能验证报告](../reports/performance-verification-report.md)的数据来源。

**使用**: 该工具运行在 Windows + PowerShell 环境，通过 `examples/perf_verify/perf_verify.bat [行数]` 启动，服务端配置由脚本生成到临时目录。详细说明见 [perf_verify README](../../examples/perf_verify/README.md)。

---

## 性能对比

各导入工具的性能特点对比：

| 工具 | 协议 | 优点 | 适用场景 |
|------|------|------|----------|
| `multi_insert` | SQL 文本 | 简单易懂，兼容性好 | 小批量导入、调试 |
| `prepare_test` | Prepared Statement | 参数绑定安全，减少解析开销 | 中等批量、参数化场景 |
| `batch_import` | 列式二进制 | 最高性能，零拷贝优化 | 大批量生产导入 |
| `insert_csv` | SQL 文本 | 与 `export_csv` 配套，走实时写路径 | 数据回放、验证 memtable/热存路径 |
| `load_csv` | 列式二进制 | 与 `export_csv` 配套，直写 segment | 大批量 CSV 迁移 |
| `hot_insert` | SQL 文本 | 多客户端并发，演示 HOT 表实时写入 | 实时行情落库形态验证 |

**典型性能对比** (100万行 Tick 数据):

| 工具 | 默认配置 | 启用优化后 |
|------|---------|-----------|
| `multi_insert` | ~10,000 rows/sec | ~20,000 rows/sec (--bulk) |
| `prepare_test` | ~5,000 rows/sec | - |
| `batch_import` | ~40,000 rows/sec | - |

以上数字来自历史测量环境，仅作量级参考；当前版本的实测数据见[性能验证报告](../reports/performance-verification-report.md)。

**选择建议**:

1. **开发调试**: 使用 `multi_insert`，SQL 可读性好
2. **参数化导入**: 使用 `prepare_test`，适合需要动态参数的场景
3. **生产大批量**: 使用 `batch_import` 或 `load_csv`，性能最优
4. **CSV 导出/回灌**: `export_csv` + `insert_csv`/`load_csv` 组合
5. **实时写入验证**: 使用 `hot_insert`

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

1. **Taibai 服务已启动**: 默认监听 `127.0.0.1:5151`
2. **认证信息**: 默认用户 `root`，密码 `taibai`
3. **数据库存在**: 默认数据库 `main`

启动 Taibai 服务：
```bash
taibai --config conf/example.yaml
```