# Taibai — 列式数据库服务器

> **Taibai** 是一个高性能列式数据库服务器，使用自有二进制协议与自有 SQL 方言。  
> 使用 Rust 构建，旨在提供高效的 OLAP 查询性能，同时支持 OLTP 场景。

---

## 目录

- [架构概览](#架构概览)
- [SQL 支持](#sql-支持)
- [存储引擎](#存储引擎)
- [查询优化器](#查询优化器)
- [执行引擎](#执行引擎)
- [安全与权限](#安全与权限)
- [网络协议](#网络协议)
- [类型系统](#类型系统)
- [构建与运行](#构建与运行)

---

## 架构概览

Taibai 采用分层架构：

```
┌─────────────────────────────────┐
│         网络层 (net/)            │  TCP + TLS，自有二进制协议
├─────────────────────────────────┤
│       协议层 (protocol/)         │  编解码、类型映射
├─────────────────────────────────┤
│         SQL 层 (sql/)            │  解析 → 规划 → 优化 → 执行
├─────────────────────────────────┤
│       存储层 (storage/)          │  段存储、durable tail、目录、事务
├─────────────────────────────────┤
│       类型层 (types/)            │  标量值、列数组、记录批
└─────────────────────────────────┘
```

### 核心模块

| 模块 | 路径 | 说明 |
|------|------|------|
| SQL 解析器 | `src/sql/parser/` | 手写递归下降解析器 |
| 查询规划器 | `src/sql/planner/` | SQL AST → 逻辑计划 |
| 查询优化器 | `src/sql/optimizer/` | 基于规则的优化管线 |
| 执行引擎 | `src/sql/execution/` | Volcano 模型算子 |
| 存储引擎 | `src/storage/` | 列式段存储 + durable tail |
| 网络层 | `src/net/` | 异步 TCP/TLS 服务 |
| 协议层 | `src/protocol/` | 自有二进制协议编解码 |

---

## SQL 支持

完整的 SQL 语法参考见 [SQL 语法参考](../user-guide/sql-reference.md)。以下为代表性示例：

### DDL (数据定义)

```sql
-- 数据库管理
CREATE DATABASE db_name;
DROP DATABASE db_name;
SHOW DATABASES;

-- 表管理
CREATE TABLE t (
    id      INT PRIMARY KEY,
    name    VARCHAR(255),
    age     INT,
    salary  FLOAT64,
    data    VECTOR
) CLUSTER BY (id)
  PARTITION BY HASH(id, 4);

ALTER TABLE t ADD COLUMN new_col VARCHAR(100);
ALTER TABLE t DROP COLUMN old_col;
ALTER TABLE t RENAME COLUMN old_name TO new_name;
ALTER TABLE t MODIFY COLUMN col_name VARCHAR(500);

DROP TABLE t;
TRUNCATE TABLE t;

-- 索引
CREATE INDEX idx_name ON t(col1, col2);
DROP INDEX idx_name ON t;

-- 表优化（段压缩）
OPTIMIZE TABLE t;
```

### DML (数据操作)

```sql
-- 插入
INSERT INTO t VALUES (1, 'Alice', 30, 50000.0);
INSERT INTO t (id, name) VALUES (1, 'Bob'), (2, 'Carol');

-- 更新
UPDATE t SET salary = 60000.0 WHERE id = 1;
UPDATE t AS a SET a.salary = 60000.0 WHERE a.id = 1;

-- 删除
DELETE FROM t WHERE id = 1;
DELETE FROM t AS a WHERE a.id = 1;

-- 插入查询结果（库内 ETL）
INSERT INTO archive SELECT * FROM t WHERE ts < '2026-01-01';
```

### DQL (数据查询)

```sql
-- 基础查询
SELECT * FROM t WHERE age > 25 ORDER BY salary DESC LIMIT 10;

-- 聚合查询
SELECT department, COUNT(*), AVG(salary), SUM(bonus)
FROM employees
GROUP BY department
HAVING COUNT(*) > 5;

-- JOIN 查询
SELECT t1.id, t1.name, t2.order_id
FROM users t1
INNER JOIN orders t2 ON t1.id = t2.user_id;

LEFT JOIN / RIGHT JOIN / FULL JOIN / CROSS JOIN / ASOF JOIN 均支持。

-- 子查询
SELECT * FROM t WHERE id IN (SELECT id FROM t2 WHERE active = true);
SELECT * FROM t WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.id = t.id);
SELECT (SELECT MAX(salary) FROM employees) AS max_sal;

-- UNION 查询
SELECT id FROM t1 UNION SELECT id FROM t2;

-- 窗口函数
SELECT
    id,
    ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC),
    RANK()       OVER (ORDER BY score DESC),
    DENSE_RANK() OVER (ORDER BY score DESC),
    LEAD(score, 1) OVER (ORDER BY id),
    LAG(score, 1)  OVER (ORDER BY id)
FROM scores;

-- DISTINCT
SELECT DISTINCT department FROM employees;

-- BETWEEN / IN / LIKE
SELECT * FROM t WHERE age BETWEEN 20 AND 40;
SELECT * FROM t WHERE name LIKE '%Alice%';
SELECT * FROM t WHERE id IN (1, 2, 3);

-- CASE 表达式
SELECT CASE WHEN score >= 90 THEN 'A' ELSE 'B' END FROM scores;
```

### 事务支持

```sql
BEGIN [TRANSACTION];
COMMIT;
ROLLBACK;
```

### 其他语句

```sql
EXPLAIN SELECT ...;
EXPLAIN ANALYZE SELECT ...;
USE db_name;
SHOW TABLES;
SHOW COLUMNS FROM t;
FLUSH;
```

---

## 存储引擎

### 段式列存 (Segment Storage)

Taibai 使用自定义的列式段存储格式：

- **段 (Segment)**：磁盘上的基本存储单元，包含多个行组 (Row Group)
- **行组 (Row Group)**：固定行数（默认 8192 行）的列数据块
- **列编码**：每列独立存储，支持多种编码策略
- **布隆过滤器**：为 CLUSTER KEY 列自动构建布隆过滤器，加速等值查询
- **Zone Map**：每个行组维护 min/max 统计信息，用于行组裁剪

### 持久化 tail (durable tail)

- 经典 WAL 已移除：恢复从不回放，纯写路径开销
- 活动写入先落 `memtable_tail/`（组提交 + fsync，帧含 CRC 与行数索引）
- flush 时物化进未 seal 的 active segment（原地追加 row group，sidecar 维护元数据），完成后 tail 清理
- 崩溃恢复按 tail/clear/flush intent generation 仲裁重放，行数 watermark 防止重复
- 恢复契约：已提交帧 CRC 损坏时整体报错（fail-loud，不静默丢行）；不足完整 header 的 tail 文件视为不存在（可安全跳过并新建）；损坏或撕裂的 flush intent 标记视为未满足，回退到更早的 tail 代际。以上行为由 `durable_tail_test.rs` 的崩溃注入测试锁定。

### 分区 (Partitioning)

```sql
CREATE TABLE t (...) PARTITION BY HASH(col, N);
```

- 支持 HASH 分区
- 查询时自动分区裁剪 (Partition Pruning)
- 每个分区独立的段文件

### 段压缩 (Compaction)

```sql
OPTIMIZE TABLE t;
```

`Compactor` 执行以下操作：
1. 按分区分组段文件
2. 过滤已删除行
3. 按 CLUSTER KEY 排序
4. 合并为新的优化段
5. 使用行组缓存减少 I/O
6. 利用 Rayon 并行处理

### MemTable

- 写入首先进入内存表 (MemTable)
- 查询时合并段数据与 MemTable 数据
- 支持行级删除标记
- 内存泄压首选 **spill** 而非提前刷盘：committed 且已有 durable tail 的 memtable 在内存高水位时切换为 Spilled 态（数据留在 tail，内存仅元数据），避免为泄压切成小 row group；spill 候选耗尽后才走 flush 兜底
- 查询或写入扫到 Spilled memtable 时惰性物化回内存（冷分区变热自愈）；事务中未提交的 memtable 不参与 spill
- 重启走**惰性恢复**：只登记 tail 索引不物化数据，启动内存占用 ≈ 0，启动时间不随未刷盘数据量线性增长

### 事务管理

- 会话级事务：`BEGIN / COMMIT / ROLLBACK` 语法（协议层有等价消息）
- ReadCommitted 隔离，memtable 粒度可见性；两级分区锁保证写入互斥
- 实际并发行为与持久化边界见[事务与并发语义](../user-guide/transactions-and-concurrency.md)

---

## 查询优化器

优化器采用基于规则的管线 (Rule-based Pipeline)，依次应用以下规则：

| 规则 | 文件 | 说明 |
|------|------|------|
| **表达式简化** | `expr_rules.rs` | 常量折叠、等式传播 |
| **谓词下推** | `predicate_pushdown.rs` | 将过滤条件下推至 Scan 节点 |
| **列裁剪** | `column_pruning.rs` | 仅读取查询需要的列 |
| **TopN 优化** | `topn.rs` | 将 `ORDER BY + LIMIT` 转换为 TopN 算子 |
| **排序 LIMIT 下推** | `order_limit_pushdown.rs` | 排序 + LIMIT 下推到 Scan 层 |
| **子查询优化** | `subquery.rs` | 标量子查询、EXISTS、IN 子查询重写 |
| **Semi-Join 转换** | `semi_join.rs` | 子查询转 Semi-Join / Anti-Join |
| **Join 重排序** | `join_reorder.rs` / `join_order.rs` | 逻辑层 Join 重排；DP/贪心的代价搜索与行数估算在 `join_order.rs` |
| **Join 投影下推** | `join_projection.rs` | Join 侧投影裁剪 |
| **索引选择** | `index_selection.rs` | 自动选择最优索引 |
| **过滤器重排序** | `filter_reorder.rs` | 选择性高的谓词优先执行 |
| **分区裁剪** | `partition_pruning.rs` | 跳过无关分区 |
| **Zone Map 裁剪** | `zone_map_pruning.rs` | 跳过无关行组 |
| **Bloom Filter** | `bloom_filter.rs` | 布隆过滤器加速等值查询 |
| **Limit 下推** | `limit_pushdown.rs` | 将 LIMIT 下推到子计划 |
| **Cost 估算** | `cost.rs` | 查询计划代价估算 |

### Scan 优化流程

```
查询谓词 → ScanSpec 编译
    ├── 分区裁剪：跳过无关分区
    ├── Zone Map 裁剪：跳过无关行组
    ├── Bloom Filter：快速判定列值是否存在
    ├── Cluster Key Range Scan：利用排序键范围读取
    └── Row Filter：谓词过滤剩余行
```

---

## 执行引擎

采用 Volcano 模型 (Pull-based Pipeline)：

```
ExecutionOp::next_batch() → Option<RecordBatch>
```

### 算子列表

| 算子 | 文件 | 说明 |
|------|------|------|
| **Scan** | `scan.rs` | 段扫描，支持谓词下推 |
| **Filter** | `filter.rs` | 行过滤，支持复杂表达式 |
| **Project** | `project.rs` | 列投影与表达式计算 |
| **Aggregate** | `aggregate.rs` | 分组聚合 (COUNT/SUM/AVG/MIN/MAX) |
| **Sort** | `sort.rs` | 排序 (多键、混合 ASC/DESC) |
| **Limit** | `limit.rs` | LIMIT / OFFSET |
| **Join** | `join.rs` | Hash Join (INNER/LEFT/RIGHT/FULL/CROSS/ASOF/SEMI/ANTI) |
| **SemiJoin** | `semi_join.rs` | Semi-Join / Anti-Join |
| **Window** | `window.rs` | 窗口函数 (ROW_NUMBER/RANK/DENSE_RANK/LEAD/LAG) |

### 特殊执行路径

- **Count(*) 优化**：直接统计行数，无需读取列数据
- **TopN 执行**：堆排序优化的 ORDER BY + LIMIT
- **MemTable 扫描**：合并内存数据与磁盘段数据
- **Cluster Key 快速扫描**：利用排序键直接范围读取

---

## 安全与权限

### 用户管理

```sql
CREATE USER username IDENTIFIED BY 'password';
DROP USER username;
ALTER USER username IDENTIFIED BY 'new_password';
```

### 角色管理

```sql
CREATE ROLE role_name;
DROP ROLE role_name;
```

### 权限控制

```sql
GRANT SELECT, INSERT ON db.table TO user_or_role;
GRANT ALL PRIVILEGES ON db.* TO user_or_role;
REVOKE SELECT ON db.table FROM user_or_role;
SHOW GRANTS FOR user_or_role;
```

支持细粒度的权限检查，包括：
- 数据库级权限
- 表级权限
- 操作类型权限 (SELECT / INSERT / UPDATE / DELETE / CREATE / DROP / ALTER)

---

## 网络协议

- **协议**：taibai-go 兼容协议
- **传输**：异步 TCP，支持 TLS 加密 (rustls)
- **编解码**：自定义二进制帧格式
- **连接处理**：基于 tokio 的异步连接池

### 网络层架构

```
TCP Listener (tokio)
  └── Handler
       ├── 协议解码 (protocol::codec)
       ├── SQL 解析与执行
       └── 结果编码与返回
```

---

## 类型系统

### 支持的数据类型

| 类型 | 说明 |
|------|------|
| `INT8` / `INT16` / `INT32` / `INT64` | 有符号整数（`INT`/`INTEGER` 为 `INT32` 别名，`BIGINT` 为 `INT64` 别名） |
| `UINT8` / `UINT16` / `UINT32` / `UINT64` | 无符号整数 |
| `FLOAT32` / `FLOAT64` | 浮点数（`FLOAT`/`REAL` 为 `FLOAT32` 别名，`DOUBLE` 为 `FLOAT64` 别名） |
| `DECIMAL` | 定点小数，scale 取值 0–18 |
| `DATE` / `TIME` / `TIMESTAMP` / `INTERVAL` | 时间类型（`TIMESTAMP` 为 epoch 毫秒整数，`DATETIME` 别名） |
| `VARCHAR` | 变长字符串（`TEXT`/`STRING` 别名，长度参数接受但不强制） |
| `BLOB` | 二进制（`BYTES` 别名） |
| `BOOL` | 布尔值（`BOOLEAN` 别名） |
| `VECTOR` | 浮点向量 (AI/ML 场景) |

完整类型清单见 [SQL 语法参考](../user-guide/sql-reference.md)。

### 向量类型函数

```sql
-- 向量距离（默认 l2 度量；metric 可选 l2/euclidean/cosine/dot/manhattan）
SELECT vector_distance(vec1, vec2) FROM t;
SELECT vector_distance(vec1, vec2, 'cosine') FROM t;

-- 向量范数
SELECT vector_norm(vec) FROM t;

-- 归一化向量
SELECT vector_normalize(vec) FROM t;

-- 逐元素运算与点积
SELECT vector_add(vec1, vec2), vector_sub(vec1, vec2),
       vector_scale(vec, 2.0), vector_dot(vec1, vec2) FROM t;
```

### 内部类型

- **ScalarValue**：统一标量值表示，支持 NULL
- **ColumnArray**：类型化的列数组，零拷贝读取
- **RecordBatch**：列式记录批次
- **NullBitmap**：NULL 值位图

---

## 构建与运行

### 构建

```bash
cargo build --release
```

### 运行

```bash
# 默认读取 conf/config.yaml
./taibai

# 指定配置文件
./taibai --config conf/example.yaml
```

内置命令行客户端：

```bash
./taibai client --host 127.0.0.1:5151 --user root --password taibai --database main --query "SELECT 1"
```

### 命令行参数

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `--config` | 配置文件路径（YAML，监听地址/数据目录等均在其中配置） | `conf/config.yaml` |
| `--addr` | 覆盖监听地址 | 取配置文件 |
| `--data-dir` | 覆盖数据目录 | 取配置文件 |
| `--database` | 覆盖默认数据库 | 取配置文件 |
| `--tls-cert` / `--tls-key` | TLS 证书与私钥路径 | 取配置文件 |
| `--no-compression` | 关闭传输压缩 | 关闭 |
| `--service` / `--install-service` / `--uninstall-service` | Windows 服务模式 | 关闭 |

默认端口为 `5151`（见 `conf/example.yaml` 的 `server.port`）。Windows 图形化安装见 [Windows 安装说明](../operations/windows-installation.md)。

### 工具

- `segment_checker`：段文件检查与调试工具 (`src/bin/segment_checker.rs`)
- `backup`：离线备份工具 (`src/bin/backup.rs`)
- `migrate`：段格式版本迁移工具 (`src/bin/migrate.rs`)
- 批量导入工具 (`examples/batch_import/`)
- 多行插入示例 (`examples/multi_insert/`)

---

## 目录结构

```
src/
├── main.rs                    # 入口，服务器启动
├── lib.rs                     # 库入口
├── bin/                       # 独立工具
│   ├── backup.rs              # 离线备份
│   ├── migrate.rs             # 段格式版本迁移
│   └── segment_checker.rs     # 段文件检查
├── common/                    # 配置、错误、取消、性能基线等公共设施
├── net/                       # 网络层
│   ├── server.rs / handler.rs / session.rs
│   ├── client.rs              # 内置 CLI 客户端
│   ├── bulk_importer.rs       # 列式批量导入客户端
│   ├── pubsub_session.rs      # 订阅会话
│   └── tls.rs                 # TLS 配置
├── protocol/                  # 协议层
│   ├── codec.rs / message.rs  # 帧编解码与消息类型
│   ├── compress.rs            # zlib 载荷压缩
│   ├── scalar_codec.rs        # 标量值编解码
│   ├── result.rs              # 查询结果编码
│   └── types.rs               # 协议类型
├── pubsub/                    # 变更订阅核心逻辑
├── sql/
│   ├── parser/                # 手写递归下降解析器
│   │   ├── lexer.rs / parser.rs / ast.rs
│   │   ├── ddl.rs / dml.rs / query.rs / expr.rs / show.rs
│   ├── binder/                # 名称与类型绑定
│   ├── planner/               # AST → 逻辑计划
│   ├── optimizer/             # 基于规则的优化管线（谓词下推、列裁剪、Join 重排、分区裁剪等）
│   └── execution/             # 执行引擎
│       ├── engine/            # 引擎主入口（core/runtime/bulk）
│       ├── ddl/               # DDL 执行（建表、鉴权、内省）
│       ├── dml_insert/        # INSERT 快路径、约束、替换与写入
│       ├── dml_update.rs / dml_delete.rs / txn_dml.rs
│       ├── query/             # SELECT 执行、相关子查询
│       ├── scan/              # 扫描执行（分区/行组裁剪、memtable 合并、元数据折叠聚合）
│       ├── operators/         # 拉取式批量算子（filter/project/aggregate/join/sort/topn/window）
│       ├── functions/         # 内置函数注册表
│       ├── physical/          # 物理计划
│       ├── prepared.rs        # 预编译语句
│       ├── spill.rs           # 内存泄压（spill）
│       └── explain.rs         # EXPLAIN / EXPLAIN ANALYZE
├── storage/
│   ├── catalog/               # 库/表元数据、用户权限、自增列
│   ├── core/                  # memtable、batch writer、compactor、hot store、事务变更
│   │   └── durable_tail/      # 持久化 tail（format/committer/recovery/…）
│   ├── segment/               # 段读写、footer、删除位图
│   ├── segment_manager/       # 段管理器生命周期、bulk 直写、缓存
│   ├── partition/             # 分区函数、裁剪、TTL
│   ├── index/                 # bloom/minmax/set 索引、sort key
│   ├── codec/                 # 列编码与谓词扫描
│   ├── replication/           # 主备流复制
│   ├── txn/                   # 事务
│   └── bulk_payload.rs        # 批量载荷解析
├── types/                     # ScalarValue、ColumnArray、RecordBatch、位图
└── tools/                     # 辅助工具
```

---

## 性能特征

- **列式存储**：仅读取查询需要的列，减少 I/O
- **Zone Map 裁剪**：跳过不满足条件的行组
- **Bloom Filter**：快速排除不匹配的行组
- **Cluster Key 排序**：利用数据排序进行范围读取
- **分区裁剪**：跳过无关分区
- **TopN 堆排序**：避免全量排序
- **并行压缩**：利用 Rayon 并行处理段合并
- **Count(*) 优化**：直接统计行数
