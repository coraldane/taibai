# Taibai — 列式数据库服务器

> **Taibai** 是一个高性能列式数据库服务器，兼容 taibai-go 协议。  
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
│         网络层 (net/)            │  TCP + TLS，taibai-go 协议
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
| 协议层 | `src/protocol/` | taibai-go 协议编解码 |

---

## SQL 支持

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
    salary  FLOAT,
    data    VECTOR(128)
) CLUSTER BY (id)
  HASH PARTITION BY (id) PARTITIONS 4;

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

-- 删除
DELETE FROM t WHERE id = 1;
DELETE FROM t AS a WHERE a.id = 1;

-- 批量插入（高性能）
INSERT_BATCH t VALUES (...);
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

LEFT JOIN / RIGHT JOIN / CROSS JOIN 均支持。

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
SHOW TABLE STATUS;
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
CREATE TABLE t (...) HASH PARTITION BY (col) PARTITIONS N;
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

### 事务管理

- MVCC 事务支持
- `BEGIN / COMMIT / ROLLBACK` 语法
- 事务 ID 分配与隔离

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
| **Join 重排序** | `join_reorder.rs` | 基于 cost 的 Join 顺序优化 |
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
| **Join** | `join.rs` | Hash Join (INNER/LEFT/RIGHT/CROSS) |
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
| `INT` / `INTEGER` | 64 位整数 |
| `FLOAT` | 64 位浮点数 |
| `VARCHAR(N)` | 变长字符串 |
| `BOOLEAN` / `BOOL` | 布尔值 |
| `VECTOR(N)` | N 维浮点向量 (AI/ML 场景) |

### 向量类型函数

```sql
-- 余弦相似度
SELECT COSINE_SIMILARITY(vec1, vec2) FROM t;

-- L2 距离
SELECT L2_DISTANCE(vec1, vec2) FROM t;

-- 向量范数
SELECT VECTOR_NORM(vec) FROM t;

-- 归一化向量
SELECT VECTOR_NORMALIZE(vec) FROM t;
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
./taibai --data-dir ./data --listen 0.0.0.0:7654
```

### 命令行参数

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `--data-dir` | 数据存储目录 | `./data` |
| `--listen` | 监听地址 | `0.0.0.0:7654` |
| `--tls-cert` | TLS 证书路径 | 无 |
| `--tls-key` | TLS 密钥路径 | 无 |

### 工具

- `segment_checker`：段文件检查与调试工具 (`src/bin/segment_checker.rs`)
- 批量导入工具 (`examples/batch_import/`)
- 多行插入示例 (`examples/multi_insert/`)

---

## 目录结构

```
src/
├── main.rs                    # 入口，服务器启动
├── lib.rs                     # 库入口
├── bin/
│   └── segment_checker.rs     # 段文件检查工具
├── common/                    # 公共工具
│   └── error.rs               # 错误类型定义
├── net/                       # 网络层
│   ├── handler.rs             # 连接处理器
│   └── tls.rs                 # TLS 配置
├── protocol/                  # 协议层
│   ├── codec.rs               # 编解码器
│   ├── types.rs               # 协议类型
│   ├── request.rs             # 请求定义
│   └── response.rs            # 响应定义
├── sql/                       # SQL 引擎
│   ├── parser/                # SQL 解析器
│   │   ├── lexer.rs           # 词法分析
│   │   ├── parser.rs          # 语法分析
│   │   ├── ast.rs             # AST 定义
│   │   ├── expr.rs            # 表达式解析
│   │   ├── query.rs           # 查询解析
│   │   ├── ddl.rs             # DDL 解析
│   │   ├── dml.rs             # DML 解析
│   │   ├── show.rs            # SHOW/OPTIMIZE 解析
│   │   └── fast_insert.rs     # 批量插入解析
│   ├── planner/               # 查询规划器
│   │   ├── mod.rs             # 逻辑计划定义
│   │   └── types.rs           # 计划类型
│   ├── optimizer/             # 查询优化器 (15+ 规则)
│   │   ├── mod.rs             # 优化器管线
│   │   ├── expr_rules.rs      # 表达式规则
│   │   ├── predicate_pushdown.rs
│   │   ├── column_pruning.rs
│   │   ├── topn.rs
│   │   ├── order_limit_pushdown.rs
│   │   ├── subquery.rs
│   │   ├── semi_join.rs
│   │   ├── join_reorder.rs
│   │   ├── index_selection.rs
│   │   ├── filter_reorder.rs
│   │   ├── partition_pruning.rs
│   │   ├── zone_map_pruning.rs
│   │   ├── bloom_filter.rs
│   │   ├── limit_pushdown.rs
│   │   └── cost.rs
│   └── execution/             # 执行引擎
│       ├── engine.rs          # 执行引擎主入口
│       ├── ddl.rs             # DDL/DML 执行
│       ├── query.rs           # 查询执行
│       ├── explain.rs         # EXPLAIN 执行
│       ├── push.rs            # 批量写入
│       ├── operators/         # 执行算子
│       │   ├── scan.rs        # 段扫描
│       │   ├── filter.rs      # 过滤
│       │   ├── project.rs     # 投影
│       │   ├── aggregate.rs   # 聚合
│       │   ├── sort.rs        # 排序
│       │   ├── limit.rs       # LIMIT
│       │   ├── join.rs        # JOIN
│       │   ├── semi_join.rs   # SEMI/ANTI JOIN
│       │   └── window.rs      # 窗口函数
│       └── scan/              # 扫描优化
│           ├── executor.rs    # 扫描执行器
│           ├── spec.rs        # 扫描规范
│           ├── partition.rs   # 分区裁剪
│           ├── row_group.rs   # 行组裁剪
│           ├── row_filter.rs  # 行过滤
│           ├── memtable.rs    # 内存表扫描
│           └── count.rs       # COUNT(*) 优化
├── storage/                   # 存储引擎
│   ├── catalog/               # 目录管理
│   │   ├── schema.rs          # 表/库元数据
│   │   └── auth.rs            # 用户/权限
│   ├── core/                  # 核心存储
│   │   ├── compactor.rs       # 段压缩
│   │   ├── durable_tail.rs    # 持久化尾部日志
│   │   └── mod.rs             # 模块导出
│   ├── segment/               # 段读写
│   ├── partition/             # 分区管理
│   ├── index/                 # 索引
│   ├── codec/                 # 编解码
│   ├── txn/                   # 事务
│   ├── segment_manager.rs     # 段管理器
│   ├── reader_cache.rs        # 读取缓存
│   ├── column_encoding.rs     # 列编码
│   ├── metrics.rs             # 存储指标
│   └── bulk_payload.rs        # 批量载荷
├── types/                     # 类型系统
│   ├── scalar.rs              # 标量值
│   ├── array.rs               # 列数组
│   ├── batch.rs               # 记录批
│   └── bitmap.rs              # NULL 位图
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

---

## 最近完成的改进

1. **窗口函数修复**：RANK / DENSE_RANK 正确处理平局排序，窗口输出列正确追加到结果
2. **身份认证增强**：细粒度权限检查、角色支持
3. **ALTER TABLE 支持**：ADD / DROP / RENAME / MODIFY COLUMN
4. **VECTOR 类型**：向量存储与相似度计算函数
5. **段压缩**：OPTIMIZE TABLE 命令，支持段合并与排序优化
