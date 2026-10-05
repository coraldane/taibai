# SQL 语法参考

Taibai 使用**自有 SQL 方言与自有二进制协议**，不兼容 MySQL、PostgreSQL 等主流数据库的方言与线协议。本文档完整描述当前实现支持的全部 SQL 语法，内容与 `src/sql/parser/` 的解析器逐条核对；凡本文未列出的语法（即使主流数据库支持）一律视为不支持，服务端会明确报错而不是静默错解。

与其他文档的关系：

- 建表选项（`CLUSTER BY` / `PARTITION BY` / `TTL` / `HOT` / `DURABILITY` / `COMPRESSION_LEVEL`）的完整语义与维护语句见[建表选项 SQL 参考](table-options.md)，本文只给出语法骨架。
- 事务的实际并发语义（隔离级别、可见性、锁）见[事务与并发语义](transactions-and-concurrency.md)。
- 客户端如何通过二进制协议发送 SQL 见[工程主要功能总结](../overview/project-summary.md)。

---

## 1. 语法约定

### 1.1 语句与大小写

- 每次 QUERY 请求**只能包含一条语句**。解析器允许分号分隔的多语句文本，但执行层会直接报错：`multi-statement strings are not supported here (N statements); send them one at a time`。语句末尾的分号可省略。
- 关键字**不区分大小写**（`select` 与 `SELECT` 等价）。
- 标识符匹配**不区分大小写**，但显示时保留书写形式（`SELECT Symbol FROM t` 与 `symbol` 解析为同一列）。注意：列被 `RENAME COLUMN` 后，旧列名仍可用于匹配（schema 漂移兼容）。
- 注释：`-- 行注释` 与 `/* 块注释 */`。

### 1.2 标识符

```
标识符        := [A-Za-z_][A-Za-z0-9_]*
引用标识符    := "任意字符"   |  `任意字符`
```

- 裸标识符以字母或下划线开头；与关键字冲突的名字用双引号或反引号引用（如 `"select"`、`` `group` ``）。
- 引用标识符内部将引用字符双写转义（`"a""b"` 表示名字 `a"b`）。
- 对象名最多两级：`表名` 或 `库名.表名`；列名最多三级限定：`列名`、`表名.列名`、`库名.表名.列名`。

### 1.3 字面量

| 字面量 | 语法 | 说明 |
|---|---|---|
| 整数 | `123`、`-1` | 解析为 i64；`-` 紧跟数字时整体作为负数字面量 |
| 浮点数 | `1.5`、`-0.25` | 解析为 f64 |
| 字符串 | `'hello'` | 仅支持 `''` 转义单引号，**不支持反斜杠转义** |
| 布尔 | `TRUE`、`FALSE` | 关键字 |
| NULL | `NULL` | 无类型 null；需要类型时用 `CAST` |

- **没有**十六进制/二进制字面量、`DATE '...'` / `TIMESTAMP '...'` / `INTERVAL` 字面量、数组字面量。
- 日期与时间戳直接写整型毫秒值或字符串（如 `'2026-01-02 15:04:05'`），由列类型做隐式转换或用 `CAST` 显式转换。
- 参数占位符：`?`（按出现顺序编号，从 0 开始）与 `$n`（从 1 开始，`$0` 报错）。两种风格可在同一条语句混用，`?` 的编号接在已出现的最大 `$n` 之后。占位符通过**协议层的 PREPARE/EXECUTE 消息**（而非 SQL 文本）绑定参数值。
- `DEFAULT` 关键字仅可在 `INSERT` 的 VALUES 中使用，表示取列默认值。

### 1.4 数据类型

| 规范名 | 可接受的别名 | 参数 | 说明 |
|---|---|---|---|
| `BOOL` | `BOOLEAN` | — | |
| `INT8` | `TINYINT` | — | |
| `INT16` | `SMALLINT` | — | |
| `INT32` | `INT`、`INTEGER` | — | 32 位整数 |
| `INT64` | `BIGINT` | — | 64 位整数 |
| `UINT8` / `UINT16` / `UINT32` / `UINT64` | 无别名 | — | 无符号整数 |
| `FLOAT32` | `FLOAT`、`REAL` | — | 32 位浮点 |
| `FLOAT64` | `DOUBLE` | — | 64 位浮点 |
| `DECIMAL` | `DECIMAL64` | `(p, s)` | 第二参数为 scale，取值 0–18；仅 `DECIMAL(p)` 时 scale 为 0 |
| `DATE` | — | — | |
| `TIME` | — | — | |
| `TIMESTAMP` | `DATETIME` | — | epoch 毫秒整数 |
| `INTERVAL` | — | — | |
| `VARCHAR` | `TEXT`、`STRING` | `(n)` | 长度参数接受但**不强制** |
| `BLOB` | `BYTES` | — | |
| `VECTOR` | — | — | 向量类型，配合向量函数使用 |

建表时任何类型后都可写 `(p)` 或 `(p, s)`，但对 `DECIMAL`（scale）与 `VARCHAR`（接受并忽略长度）以外的类型这些数字没有实际语义。

---

## 2. DDL

### 2.1 CREATE DATABASE

```sql
CREATE DATABASE [IF NOT EXISTS] 名称
```

没有 `CREATE SCHEMA` 语法。

### 2.2 CREATE TABLE

```sql
CREATE TABLE [IF NOT EXISTS] 表名 (
    列定义 [, 列定义 | 表级约束]...
)
[表选项...]
```

**列定义**（约束按此顺序书写，不可调换）：

```sql
列名 类型[(p[, s])]
    [NOT NULL]
    [AUTO_INCREMENT]        -- AUTOINCREMENT 拼写同样接受
    [PRIMARY KEY]
    [UNIQUE [KEY | INDEX]]
    [DEFAULT 默认值]
```

`DEFAULT` 只接受常量：`CURRENT_TIMESTAMP`、字符串、整数、浮点数、`TRUE`、`FALSE`、`NULL`。不支持表达式默认值、`DEFAULT (expr)`。

**表级约束**：

```sql
PRIMARY KEY (列 [, 列]...)                  -- 支持复合主键；主键列自动 NOT NULL
UNIQUE [KEY | INDEX] [约束名] (列 [, 列]...)
```

**不支持**：`CHECK`、`FOREIGN KEY` / `REFERENCES`、生成列、`COLLATE`。

**表选项**（顺序任意，每项最多出现一次；`=` 可省略）。完整语义见[建表选项 SQL 参考](table-options.md)：

```sql
CLUSTER BY (列 [, 列]...)                   -- 聚簇键，括号可省略
PARTITION BY 列                             -- 按 值 分区
PARTITION BY DAY(列) | MONTH(列) | YEAR(列) -- 按时间分区分桶
PARTITION BY MOD(列, n)                     -- 按取模分桶
PARTITION BY HASH(列[, n])                  -- 哈希分桶，默认 10 桶
TTL (列, 秒数)                              -- 列必须为 TIMESTAMP；TTL 列, 秒数 同样接受
HOT BY (时间列) [INDEX(列)] [CLOSE AT 'HH:MM']
DURABILITY [=] STRICT | RELAXED
COMPRESSION_LEVEL [=] n                     -- 1..=22
```

> 注意：`HOT KEEP N DAYS` 已被移除，解析器会明确报错。

示例：

```sql
CREATE TABLE IF NOT EXISTS main.ticks (
    symbol   VARCHAR NOT NULL,
    ts       TIMESTAMP NOT NULL,
    price    FLOAT64,
    volume   INT64 DEFAULT 0,
    PRIMARY KEY (symbol, ts)
) CLUSTER BY (symbol, ts)
  PARTITION BY DAY(ts)
  TTL (ts, 86400)
  DURABILITY STRICT
  COMPRESSION_LEVEL = 6;

CREATE TABLE IF NOT EXISTS users (
    id       INT64 AUTO_INCREMENT PRIMARY KEY,
    name     VARCHAR NOT NULL,
    email    VARCHAR UNIQUE,
    active   BOOL DEFAULT TRUE,
    created  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### 2.3 CREATE INDEX

```sql
CREATE [UNIQUE] INDEX [IF NOT EXISTS] 索引名 ON 表名 (列 [, 列]...) [TYPE bloom | minmax | min_max | set]
```

- 索引类型默认 `bloom`。
- 二级索引是可选的扫描加速结构，不参与唯一性约束（唯一性由 `UNIQUE KEY` / 主键保证）。

```sql
CREATE INDEX idx_symbol ON ticks (symbol);
CREATE UNIQUE INDEX IF NOT EXISTS idx_code ON t (code) TYPE minmax;
DROP INDEX IF EXISTS idx_symbol ON ticks;
```

### 2.4 CREATE USER / CREATE ROLE

```sql
CREATE USER 用户名 [IDENTIFIED BY] '密码'    -- IDENTIFIED BY 可省略
CREATE ROLE 角色名
```

### 2.5 ALTER TABLE

```sql
ALTER TABLE 表名 操作 [, 操作]...
```

| 操作 | 语法 | 说明 |
|---|---|---|
| 加列 | `ADD [COLUMN] 列定义` | `COLUMN` 可省略 |
| 加唯一约束 | `ADD UNIQUE [KEY | INDEX] [名] (列, ...)` | |
| 删列 | `DROP [COLUMN] 列名` | |
| 删索引 | `DROP KEY 索引名`（`INDEX` 同义） | |
| 删分区 | `DROP PARTITION 分区键` | 必须单独执行 |
| 清空分区 | `TRUNCATE PARTITION 分区键` | 必须单独执行 |
| 删 TTL | `DROP TTL` | |
| 设 TTL | `SET TTL 列, 秒数` | |
| 设热存 | `SET HOT BY (时间列) [INDEX(列)] [CLOSE AT 'HH:MM']` | 必须单独执行 |
| 设压缩级别 | `SET COMPRESSION_LEVEL [=] n | DEFAULT` | |
| 改列 | `MODIFY COLUMN 列定义` | |
| 列重命名 | `RENAME COLUMN 旧名 TO 新名` | |
| 表重命名 | `RENAME TO 新表名` | 必须单独执行 |

加列/删列与热态收敛（memtable、durable tail、hot mirror）的交互语义见[兼容性与部署指引](limitations.md)。

### 2.6 ALTER USER

```sql
ALTER USER 用户名 IDENTIFIED BY '新密码'     -- 或 PASSWORD = '新密码'
ALTER USER 用户名 LOCK | UNLOCK              -- 三种操作可逗号/连续组合
```

### 2.7 DROP 语句

```sql
DROP TABLE   [IF EXISTS] 表名
DROP INDEX   [IF EXISTS] 索引名 ON 表名
DROP DATABASE [IF EXISTS] 库名
DROP USER    [IF EXISTS] 用户名
DROP ROLE    [IF EXISTS] 角色名
```

### 2.8 TRUNCATE

```sql
TRUNCATE [TABLE] 表名
```

### 2.9 GRANT / REVOKE

```sql
GRANT   权限 [, 权限]... [ON 对象] TO 用户
REVOKE  权限 [, 权限]... [ON 对象] FROM 用户
```

- 权限：`ALL [PRIVILEGES]`、`SELECT`、`INSERT`、`UPDATE`、`DELETE`、`CREATE`、`DROP`、`ALTER`、`TRUNCATE`、`SUBSCRIBE`、`REPLICATION`。
- 对象：`TABLE 库.表`、`DATABASE 库名`、`*`（全部表）、`库.*`、裸标识符（等价表名）。
- `REPLICATION` 是**全局权限**（授权备机读取复制事件流）：授予/回收时省略 ON 子句或使用通配对象（`*` / `*.*`），带 `ON TABLE` / `ON DATABASE` 会明确报错。

```sql
GRANT SELECT, INSERT ON TABLE main.ticks TO alice;
GRANT SUBSCRIBE ON DATABASE main TO bob;
GRANT ALL ON *.* TO alice;
GRANT REPLICATION TO repl_user;
REVOKE REPLICATION FROM repl_user;
```

---

## 3. DML

### 3.1 INSERT

```sql
INSERT INTO 表名 [(列 [, 列]...)] VALUES (表达式 [, 表达式]...) [, (表达式, ...)]...
INSERT INTO 表名 [(列 [, 列]...)] SELECT ...
```

- 支持一条语句多行 VALUES；`INSERT INTO` 的 `INTO` 不可省略。
- VALUES 中可用 `DEFAULT` 关键字取列默认值。
- `INSERT ... SELECT` 只接受单个 SELECT（可带 CTE 与集合操作）。
- 未出现在列清单中的列取默认值（无默认值则为 NULL）。

```sql
INSERT INTO ticks (symbol, ts, price, volume) VALUES
  ('AAPL', 1717200000000, 191.2, 1000),
  ('MSFT', 1717200000000, 420.1, DEFAULT);

INSERT INTO ticks_archive (symbol, ts, price)
SELECT symbol, ts, price FROM ticks WHERE ts < 1717200000000;

REPLACE INTO kv (k, v) VALUES ('a', 1);
```

### 3.2 REPLACE

```sql
REPLACE [INTO] 表名 [(列, ...)] VALUES (...), ...
REPLACE [INTO] 表名 [(列, ...)] SELECT ...
```

与 MySQL 语义一致：按主键/唯一键冲突时整行替换，否则插入。`INTO` 可省略。

### 3.3 UPDATE

```sql
UPDATE 表名 [[AS] 别名] SET 列 = 表达式 [, 列 = 表达式]... [WHERE 表达式]
```

- SET 目标可带限定符（`SET t.price = ...`），限定符仅被忽略，实际按裸列名解析。
- 不支持多表 UPDATE；**不支持 WHERE 中使用相关子查询**。

```sql
UPDATE ticks AS t SET t.price = 100.5 WHERE t.symbol = 'AAPL';
DELETE FROM ticks WHERE symbol = 'MSFT';
```

### 3.4 DELETE

```sql
DELETE FROM 表名 [[AS] 别名] [WHERE 表达式]
```

不支持多表 DELETE。

---

## 4. 查询（SELECT）

### 4.1 语法骨架

```
查询 := [WITH CTE, ...] SELECT 核心 (集合操作 SELECT 核心)* [ORDER BY ...] [LIMIT 表达式] [OFFSET 表达式]

SELECT 核心 :=
    SELECT [DISTINCT] 选择列表
    [FROM 表引用 [JOIN 子句]...]
    [WHERE 表达式]
    [GROUP BY 表达式 [, 表达式]...]
    [HAVING 表达式]
```

### 4.2 选择列表

- `*` 表示全部列；**不支持 `t.*` 形式的限定星号**。
- 别名：`表达式 [AS] 别名`；`AS` 可省略，裸标识符作别名（当其后是 `FROM`/`WHERE`/`GROUP` 等子句关键字时不会误判为别名）。
- `SELECT` 不带 `FROM` 也是合法的（如 `SELECT 1 + 1`）。

### 4.3 FROM 与 JOIN

```sql
FROM 表引用 [, 表引用]...                          -- 逗号等价 CROSS JOIN
表引用 := [库名.]表名 [[AS] 别名]
        | (SELECT ...) 别名                         -- 派生表，别名必须写
```

JOIN 类型：

```sql
[INNER] JOIN        右表 ON 表达式
LEFT  [OUTER] JOIN  右表 ON 表达式
RIGHT [OUTER] JOIN  右表 ON 表达式
FULL  [OUTER] JOIN  右表 ON 表达式
CROSS JOIN          右表                          -- 不带 ON
ASOF JOIN           右表 ON 表达式                -- 时序最近邻匹配
LEFT ASOF JOIN      右表 ON 表达式                -- ASOF LEFT JOIN 拼写同样接受
```

- 除 CROSS JOIN 外**必须写 `ON 表达式`**；不支持 `USING (...)` 与 `NATURAL JOIN`。
- JOIN 可以链式书写多个。
- **JOIN ON 条件中不支持子查询**。

**ASOF JOIN**（面向时序行情场景）：ON 条件由若干等值条件加**恰好一个**左右时间列之间的不等式构成，每个左表行匹配满足等值条件与不等式的右表中最近的一行：

- `l.ts >= r.ts`：取右表中时间不晚于左表时间的最后一行（kdb+ `aj` 风格）；
- `l.ts > r.ts`：严格早于；
- `l.ts <= r.ts`：取右表中时间不早于左表时间的最前一行；
- `l.ts < r.ts`：严格晚于。

```sql
SELECT t.ts, q.price
FROM trades t ASOF JOIN quotes q
  ON t.symbol = q.symbol AND t.ts >= q.ts;
```

### 4.4 WHERE / GROUP BY / HAVING

- `WHERE` 与 `HAVING` 均为任意表达式（见第 5 节）。
- `GROUP BY` 接受任意表达式列表（不限于列名）；聚合与 `HAVING` 的组合正常支持。
- 不支持 `GROUPING SETS` / `CUBE` / `ROLLUP`。

### 4.5 ORDER BY / LIMIT / OFFSET

```sql
ORDER BY 表达式 [ASC | DESC] [, 表达式 [ASC | DESC]]...
LIMIT 表达式
OFFSET 表达式
```

- 排序项为任意表达式，默认 `ASC`。**不支持 `NULLS FIRST/LAST`**。
- `LIMIT` 与 `OFFSET` 是独立子句（接受表达式），**不支持 `LIMIT n, m` 逗号形式**与 `FETCH FIRST`；`OFFSET` 可以不与 `LIMIT` 同用。
- ORDER BY / LIMIT / OFFSET 写在集合操作最后时，作用于整个集合结果。

```sql
SELECT symbol, price FROM ticks
ORDER BY ts DESC, price ASC
LIMIT 10 OFFSET 5;
```

### 4.6 CTE（WITH）

```sql
WITH 名称 AS (查询) [, 名称 AS (查询)]... 查询
```

- 仅支持**非递归** CTE，`WITH RECURSIVE` 明确报错。
- 后定义的 CTE 可以引用先定义的 CTE；CTE 名称不区分大小写，只能用不带库前缀的名字引用。
- 实现为派生表脱糖，可在 FROM / JOIN / 子查询位置引用。

```sql
WITH big AS (
  SELECT symbol, price FROM ticks WHERE price > 100.0
)
SELECT symbol, COUNT(*) AS cnt FROM big GROUP BY symbol;
```

### 4.7 集合操作

```sql
查询 UNION [ALL] 查询
查询 INTERSECT 查询        -- 不支持 INTERSECT ALL
查询 EXCEPT 查询           -- 不支持 EXCEPT ALL
```

- `INTERSECT` 优先级高于 `UNION` / `EXCEPT`；`UNION` 与 `EXCEPT` 同级、左结合（与 PostgreSQL 一致）。
- `UNION` 不带 `ALL` 时去重。
- 集合操作两端可用括号包裹：`(SELECT ...) UNION (SELECT ...)`。

```sql
SELECT symbol FROM ticks_ns
UNION ALL
SELECT symbol FROM ticks_ny
ORDER BY symbol;
```

### 4.8 子查询

- 标量子查询：`(SELECT ...)` 可出现在任何表达式位置。
- `IN (SELECT ...)` / `NOT IN (SELECT ...)`、`EXISTS (SELECT ...)`（`NOT EXISTS` 写作 `NOT EXISTS (...)`）。
- 派生表：`FROM (SELECT ...) 别名`。
- 相关子查询（引用外层列）**仅支持直接外层一层**；更深层嵌套报 `nested correlated subqueries are not supported`。相关子查询内不支持占位符；`UPDATE`/`DELETE` 的 WHERE 中不支持相关子查询。详见[兼容性与部署指引](limitations.md)。

```sql
-- 标量子查询
SELECT symbol, price FROM ticks
WHERE price = (SELECT MAX(price) FROM ticks);

-- IN / EXISTS 子查询
SELECT * FROM t WHERE id IN (SELECT id FROM t2 WHERE active = TRUE);
SELECT * FROM t WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.id = t.id);

-- 相关子查询（直接外层）
SELECT p.name,
       (SELECT SUM(o.qty) FROM orders o WHERE o.product_id = p.id) AS total
FROM products p;
```

### 4.9 聚合函数

聚合函数固定为 5 个：

```sql
COUNT(*)  COUNT(expr)  COUNT(DISTINCT expr)
SUM(expr)  AVG(expr)  MIN(expr)  MAX(expr)
```

不支持其他聚合函数（如 `STRING_AGG`、`ARRAY_AGG`、`STDDEV`）。

### 4.10 窗口函数

```sql
函数(...) OVER (
    [PARTITION BY 表达式 [, 表达式]...]
    [ORDER BY 表达式 [ASC | DESC] [, ...]]
    [ROWS | RANGE 框架]
)
```

- 专用窗口函数：`ROW_NUMBER()`、`RANK()`、`DENSE_RANK()`、`LEAD(值[, 偏移[, 默认值]])`、`LAG(值[, 偏移[, 默认值]])`。`LEAD`/`LAG` 的偏移必须是整数字面量，默认为 1。
- 任意聚合函数（`SUM` / `AVG` / `MIN` / `MAX` / `COUNT`）也可作为窗口聚合使用。
- 框架边界：`UNBOUNDED PRECEDING | FOLLOWING`、`n PRECEDING | FOLLOWING`、`CURRENT ROW`；支持 `BETWEEN ... AND ...` 与单边界两种形式。
- 限制：窗口函数与 `GROUP BY` 同用、嵌套窗口均明确报错。

```sql
SELECT symbol, ts, price,
       ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY ts) AS rn,
       SUM(price) OVER (ORDER BY ts ROWS BETWEEN 10 PRECEDING AND CURRENT ROW) AS ma
FROM ticks;
```

---

## 5. 表达式

### 5.1 运算符与优先级

从低到高：

| 优先级 | 运算符 | 说明 |
|---|---|---|
| 1 | `OR` | |
| 2 | `AND` | |
| 3 | `= != <> < <= > >=`、`IS [NOT] NULL`、`[NOT] BETWEEN`、`[NOT] IN`、`[NOT] LIKE` | 单个比较不链式（非结合） |
| 4 | `+` `-` | |
| 5 | `*` `/` `%` | |
| 6 | `-`（一元负号）、`NOT` | |

- `!=` 与 `<>` 等价。
- **不支持**：`||` 字符串连接（用 `CONCAT()`）、位运算符、`XOR`、`IS TRUE/FALSE`、`ANY/ALL/SOME`、`::` 类型转换。

### 5.2 谓词

```sql
expr IS NULL / expr IS NOT NULL
expr [NOT] BETWEEN 低 AND 高
expr [NOT] IN (表达式 [, 表达式]... | SELECT ...)
expr [NOT] LIKE 模式            -- % 与 _ 通配符；NULL 参与时结果为 NULL
EXISTS (SELECT ...)
```

### 5.3 CASE

```sql
CASE WHEN 条件 THEN 结果 [WHEN ...]... [ELSE 结果] END
CASE 操作数 WHEN 值 THEN 结果 [WHEN ...]... [ELSE 结果] END   -- 简单形式
```

### 5.4 CAST

```sql
CAST(表达式 AS 类型[(p[, s])])
```

类型参数仅对 `VARCHAR`（接受并忽略长度）与 `DECIMAL`（第二参数为 scale；仅 `DECIMAL(p)` 时 scale 取 0）有意义，其余类型带参数报错。没有 `CONVERT()` 函数与 `::` 转换语法。

---

## 6. 内置函数

函数名不区分大小写。以下为全部内置函数（`别名` 指向同一实现）。

### 6.1 数学

`abs` `ceil`（别名 `ceiling`）`floor` `round` `sqrt` `pow`（别名 `power`）`log` `ln` `log10` `log2` `exp` `sin` `cos` `tan` `asin` `acos` `atan` `atan2` `sign` `mod` `truncate` `pi`

### 6.2 字符串

`length`（别名 `len`）`upper`（别名 `ucase`）`lower`（别名 `lcase`）`trim` `ltrim` `rtrim` `substring`（别名 `substr`）`concat` `concat_ws` `replace` `reverse` `left` `right` `lpad` `rpad` `instr`（别名 `locate`）`repeat` `space`

### 6.3 日期与时间

`now` `current_date` `current_time` `current_timestamp` `year` `month` `day` `hour` `minute` `second` `dayofweek` `dayofyear` `week` `quarter` `date_format` `last_day`

### 6.4 控制与转换

- 控制流：`coalesce` `nullif` `ifnull` `isnull`
- 转换：`cast` `to_int` `to_float` `to_string` `to_bool`

### 6.5 向量

- `vector_distance(a, b[, metric])`：距离度量，默认 `"l2"`；可选 `l2`/`euclidean`、`cosine`（余弦距离）、`dot`（返回负点积，便于按距离排序）、`manhattan`
- `vector_norm(vec)`、`vector_normalize(vec)`、`vector_add(a, b)`、`vector_sub(a, b)`、`vector_scale(vec, 标量)`、`vector_dot(a, b)`

向量参数也接受逗号分隔的字符串形式（如 `'1.0,2.0,3.0'`）。

未注册的函数名报 `unknown function '<name>'`。

---

## 7. 事务语句

语法：`BEGIN [TRANSACTION]` / `COMMIT [TRANSACTION]` / `ROLLBACK [TRANSACTION]`（`TRANSACTION` 可省略）。

- 事务绑定在会话上；隔离级别为 ReadCommitted，行为边界见[事务与并发语义](transactions-and-concurrency.md)。
- 协议层也有等价的 BEGIN/COMMIT/ROLLBACK 消息（0x30–0x32），效果相同。

```sql
BEGIN;
INSERT INTO ticks VALUES ('AAPL', 1717200000000, 191.2, 1000);
UPDATE ticks SET price = 100.5 WHERE symbol = 'AAPL';
COMMIT;
```

---

## 8. 管理与运维语句

### 8.1 SHOW / DESCRIBE

```sql
SHOW TABLES [FROM | IN 库名]
SHOW DATABASES
SHOW COLUMNS FROM 表名           -- DESCRIBE TABLE 表名 等价
SHOW INDEXES FROM 表名           -- SHOW INDEX 同义
SHOW UNIQUE INDEX STATUS FROM 表名
SHOW CREATE TABLE 表名
SHOW USER
SHOW ROLE
SHOW GRANTS [FOR 用户名]
SHOW PARTITIONS [FROM 表名]
SHOW STATS
SHOW PERFORMANCE BASELINES
SHOW ENGINES
SHOW PROCESSLIST [FULL]
```

### 8.2 EXPLAIN

```sql
EXPLAIN [ANALYZE] 任意语句
```

`EXPLAIN` 输出执行计划；`EXPLAIN ANALYZE` 实际执行并给出各算子耗时剖析（见[通用性能基础](../general-performance-foundations.md)）。

### 8.3 运维语句

```sql
USE 库名
FLUSH [TABLE [表名]]                              -- 触发落盘
PAUSE                                             -- 暂停 flush
RESUME                                            -- 恢复 flush
OPTIMIZE TABLE 表名 [ON PARTITION 分区名]          -- 手动 compaction
ANALYZE TABLE 表名                                -- 采集统计信息
REBUILD UNIQUE INDEX[ES] [索引名] ON 表名          -- 流式重建唯一索引
KILL QUERY 查询ID                                 -- 取消运行中的查询（ID 见 SHOW PROCESSLIST）
RESET PERFORMANCE BASELINES
```

查询取消、超时与 `SHOW PROCESSLIST` 的阶段语义见[查询管理与语句超时](query-management.md)。

```sql
SHOW TABLES FROM main;
SHOW COLUMNS FROM ticks;
SHOW PROCESSLIST;
KILL QUERY 42;

EXPLAIN ANALYZE SELECT symbol, COUNT(*) FROM ticks GROUP BY symbol;

USE main;
FLUSH TABLE ticks;
OPTIMIZE TABLE ticks;
ANALYZE TABLE ticks;
REBUILD UNIQUE INDEXES ON kv;
```

---

## 9. 协议级会话命令（不是 SQL 语法）

以下命令通过 QUERY 通道发送，但由网络层**字符串前缀匹配**处理，不经过 SQL 解析器，语法上不接受变体：

```sql
START BULK IMPORT
STOP BULK IMPORT
```

用于开启/关闭当前会话的批量导入模式，配合列式二进制批量写消息（`BULK_INSERT_COLUMNAR`）使用，见[数据导入流程说明](../operations/data-import-flow.md)。

此外以下能力只存在于二进制协议层，**没有 SQL 语句形式**：

- 预编译语句：PREPARE / EXECUTE / BATCH_EXECUTE / CLOSE_STMT 消息（SQL 文本中的 `?` / `$n` 占位符在此绑定参数）。
- 变更订阅：SUBSCRIBE / UNSUBSCRIBE 及可靠订阅 V2 消息（`SUBSCRIBE` 关键字仅在 GRANT/REVOKE 权限列表中出现），见[变更订阅（Pub/Sub）](pubsub.md)。
- 登录、心跳：LOGIN / PING / LOGOUT 消息。

---

## 10. 明确不支持的语法一览

以下主流 SQL 特性在当前实现中**不存在**（多数会明确报错）：

- 语句级：独立 `SET` 会话变量语句、`PREPARE`/`EXECUTE` SQL 文本、`CALL` / 存储过程、`LOAD DATA`、`COPY`、`VALUES` 语句、多语句脚本执行。
- 查询：`t.*` 限定星号、`USING` / `NATURAL JOIN`、`JOIN ON` 中子查询、`NULLS FIRST/LAST`、`LIMIT n, m`、`FETCH FIRST`、`OFFSET n ROWS`、`GROUPING SETS`/`CUBE`/`ROLLUP`、`INTERSECT ALL` / `EXCEPT ALL`、递归 CTE、窗口函数与 `GROUP BY` 同用。
- 表达式：`||` 连接、位运算、`XOR`、`IS TRUE/FALSE`、`ANY/ALL/SOME`、`CONVERT()`、`::` 转换、日期/间隔字面量、十六进制字面量。
- DDL：`CHECK`、`FOREIGN KEY`/`REFERENCES`、生成列、表达式默认值、`CREATE SCHEMA`、`ALTER TABLE ... ADD PARTITION`（分区由 `PARTITION BY` 随写入自动创建）。
- DML：多表 UPDATE/DELETE、`UPDATE`/`DELETE` WHERE 中的相关子查询、`ON DUPLICATE KEY UPDATE`（用 `REPLACE` 代替）。
- 权限：`GRANT REPLICATION`（REPLICATION 权限存在但暂不能通过 SQL 授予，root 的 ALL 覆盖）。

## 11. 与主流数据库的差异速查

| 习惯写法（MySQL/PostgreSQL） | Taibai 中的写法 |
|---|---|
| `SET NAMES utf8mb4` 等会话变量 | 无 SET 语句；时区在登录消息中指定 |
| `a \|\| b` 字符串连接 | `CONCAT(a, b)` |
| `LIMIT 10, 20` | `LIMIT 20 OFFSET 10` |
| `INSERT ... ON DUPLICATE KEY UPDATE` | `REPLACE INTO` |
| `JOIN ... USING (col)` | `JOIN ... ON l.col = r.col` |
| `PREPARE stmt FROM ...` | 协议层 PREPARE/EXECUTE 消息 |
| `LISTEN/NOTIFY` | 协议层 SUBSCRIBE/UNSUBSCRIBE 消息 |
| `SHOW VARIABLES` | 无 |
| `GROUP_CONCAT` | 无 |
| `SELECT t.*` | `SELECT *`（配合表别名逐列列出） |
| `COUNT(DISTINCT a, b)` 多参去重 | 不支持（仅单表达式） |
