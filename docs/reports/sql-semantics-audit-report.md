# SQL 查询语义支持度审计报告

日期:2026-09-05(二期:子查询与 FROM 派生表完整实现;三期:SemiJoin 去相关;四期:标量去相关;五期:聚合去相关 + EXPLAIN ANALYZE)
范围:常用 SQL 查询语句(DQL)的解析、语义正确性与错误处理质量。
方法:基于 `tests/sql_semantics_regression_test.rs`、`tests/subquery_derived_table_test.rs`(28 个场景)+ `aggregate_streaming_test`、`having_clause_test`、`aggregate_regression_test`、`analyze_statistics_test` 等既有套件,全量 `cargo test --workspace` 验证。

## 五期:聚合 EXISTS/IN 去相关 + EXPLAIN ANALYZE 接入

- **聚合 EXISTS/IN 去相关**:内层带 HAVING 或聚合选择列的相关子查询,右侧改为"按相关键分组的聚合计划":
  - `EXISTS (SELECT 1 FROM b WHERE b.k = a.k HAVING COUNT(*) > 1)` → `SEMI JOIN (SELECT k FROM b GROUP BY k HAVING COUNT(*) > 1)`;
  - `x IN (SELECT MAX(c) FROM b WHERE b.k = a.k)` → `SEMI JOIN (SELECT k, MAX(c) FROM b GROUP BY k) ON x = max AND a.k = k`;NOT IN 走反连接。
  - 语义守卫:HAVING 不得引用外层列;裸聚合的 EXISTS(无 HAVING,逐行恒真)不重写;原 GROUP BY / LIMIT / OFFSET 仍回退逐行。
- **EXPLAIN ANALYZE 全面支持子查询特性**:预解析上下文(`PreResolvedExec`:resolved + 去相关伪表批次)穿透到分析执行路径;`PhysicalContext` 携带子查询注册表,使无法去相关的逐行标记也能在剖析管线下求值;物化路径的 Scan 不再强制要求存储元数据(伪表以计划自带 schema 构建)。此前此类查询直接报错。
- 随带修复:物化 Scan 构建的早退返回曾绕过剖析包装(`ProfiledExec`),导致普通 EXPLAIN ANALYZE 丢失算子指标——已恢复包装路径。

## 四期:标量相关子查询去相关(LEFT JOIN + 按相关键分组)

三期遗留的标量形状(`x > (SELECT AVG(y) FROM b WHERE b.k = a.k)`)仍走逐行执行,本期消除:

- **改写**:标记所在的谓词被重写为引用连接输出列,下方堆叠 `LEFT JOIN (SELECT k, AGG(v) FROM b WHERE <局部> GROUP BY k) ON <外层表达式> = k`。
- **语义关键点**:
  - LEFT JOIN(不滤行)保证可安全嵌套于 OR / NOT / 函数参数中——与半连接只能整取合取不同;
  - 相关合取必须是 `内层 = 外层` 等值对(内层侧成为分组键);无匹配组时 LEFT JOIN 产出 NULL,与"空子查询返回 NULL"一致;
  - **COUNT 特殊处理**:`COUNT(*)` 空组在原始语义下返回 0 而非 NULL,改写后以 `coalesce(agg, 0)` 包裹(专门测试区分 0 与 NULL);
  - 聚合仅支持非 DISTINCT 的 COUNT/SUM/AVG/MIN/MAX,参数不得引用外层。
- **基础设施**:`plan_output_schema` 增加 Aggregate 输出布局推导(分组键 + `aggregate_result_column_def`);`rename_scans` 增加 Aggregate/Window 分支,使伪表重命名可穿透聚合计划(修复右侧批次丢失)。
- 未命中形状(表达式相关、内层已带 GROUP BY、DISTINCT 聚合等)保持逐行执行,结果不变。

## 三期:SemiJoin 去相关(相关子查询性能优化)

二期留下的"逐行执行 + 参数缓存"开销,本期以计划改写消除:

- **改写**(`execution::decorrelate`,在 optimize 之后、管线构建之前执行):`WHERE EXISTS(SELECT ... WHERE b.k = a.k AND 局部谓词) AND 其它` 重写为 `SemiJoin(左=a, 右=SELECT k FROM b WHERE 局部谓词, 条件=a.k=b.k)`;`NOT EXISTS` → 反连接;`IN (SELECT c ...)` → 以选择列的第 0 列为值列的半连接。一次扫描 + 哈希探测替代外层行 × 内层扫描。
- **SemiJoinExec 升级**(原实现仅支持单等值对且实际无生产路径):支持 AND 树内多等值键、残留谓词(非等值合取在拼接行上逐候选求值)、NULL 等值键不可匹配的标准语义、无等值键时退化为嵌套循环。
- **隔离**:右侧扫描重命名为 `__corr_{id}__{表}` 伪表并挂载自己的快照批次,避免与外层同表扫描共享 `table_batches` 键和扫描谓词——自相关子查询(内外同表)因此正确。
- **保守回退**:标量子查询、内层带聚合/GROUP BY/HAVING/LIMIT/OFFSET、OR/NOT 包裹、表达式不可翻译等形状保持逐行执行(结果不变,仅少优化);EXPLAIN 可观察两种计划(SemiJoin vs correlated subquery)。
- 全部既有相关子查询结果语义经 21 场景回归验证不变。

## 二期:子查询与 FROM 派生表完整实现

一期审计将"相关子查询"与"FROM 派生表"列为最高频缺口,本期已完成:

### 1. FROM 派生表(`FROM (SELECT ...) alias`)

- **实现**:解析器原本已支持,本期打通执行层——子查询经完整执行管线物化为批次,外层改写为对合成表(`__derived__` 库下伪表)的扫描,批次直接注入 `table_batches`。
- **覆盖**:基础选择、外层再过滤/聚合、与真实表及派生表 JOIN、嵌套派生表、UNION 分支内派生表、无 FROM 子查询。
- 派生表自动获得别名绑定与列裁剪保护,`EXPLAIN` 可渲染其计划。

### 2. 相关子查询(引用外层列)

- **实现**:绑定阶段(`query::bind_correlated_subquery`)将子查询 WHERE 拆为"局部合取"与"相关合取",外层引用改写为 `Placeholder` 标记;执行层按外层行参数化执行内层查询(纯数据快照 + 管线复用,无需在求值栈中传递引擎句柄),并按参数值记忆缓存。
- **覆盖**:`EXISTS / NOT EXISTS / IN / NOT IN / 标量子查询`,支持位于 WHERE(含 OR/NOT 组合)、SELECT 列表、聚合参数;内层可带自己的 WHERE 局部谓词、GROUP BY、ORDER BY + LIMIT(逐行执行天然保持语义)。
- **快照一致性**:内层表在绑定阶段物化快照,与既有非相关子查询"查询开始时执行一次"的语义一致。

### 3. 随带修复的潜在缺陷

| # | 缺陷 | 修复 |
|---|------|------|
| 1 | `ast_expr_to_logical_join` 调用与 `resolve_qualified_column_index` 签名不匹配,工作区基线无法编译 | 补齐 `None` 参数 |
| 2 | `row_filter::apply_remaining_mask` 求值失败静默放行全部行(错误数据风险,一期报告同类问题的残留) | 改为传播 `Result`,5 处调用点相应处理 |
| 3 | `compile_scan_spec` 对"投影位于聚合之上"的计划误用顶层列索引切列,物化路径下丢失分组键列(普通聚合走物理管线未暴露) | 聚合之下跳过普通投影切片 |
| 4 | 含相关子查询谓词可能被下推进扫描/联接一侧导致错误求值 | `predicate_pushdown` 对含 `CorrelatedSubquery` 谓词禁止下推 |
| 5 | 无 FROM 的 SELECT(`SELECT (SELECT ...)`)不做子查询解析 | 执行前统一解析 |

### 4. 已知边界(均明确报错,不会静默出错)

- 相关引用仅支持**直接外层**查询块;更深嵌套报 "nested correlated subqueries are not supported"。
- 相关子查询内不支持用户参数占位符(`?`)。
- JOIN ON 条件中的子查询不支持。
- UPDATE / DELETE 语句的相关子查询不支持(保持一期行为)。
- EXPLAIN 支持渲染子查询/派生表计划;`EXPLAIN ANALYZE` 对此类查询暂不支持。
- 性能说明:相关子查询按外层行执行并按参数值缓存,同键重复行只执行一次;三期已将 EXISTS/IN 形状自动去相关为 SemiJoin,其余形状保持逐行。

## 一、一期审计发现并修复的缺陷

| # | 缺陷 | 现象 | 根因 | 修复 |
|---|------|------|------|------|
| 1 | WHERE 中计算表达式被静默忽略 | `WHERE COALESCE(price,0.0)<6.0`、`price+1<6.5`、`UPPER(...)='...'`、`ABS(...)<...`、`CASE...=1` 全部返回全表行数 | `scan/row_filter.rs` 的 `eval_logical_expr_batch` 对不认识的表达式形态返回全 true;`COUNT(*)` 快路径信任该结果,绕过了上层 FilterExec | `apply_remaining_mask` 委托给完备的 `operators::filter::eval_filter`,删除失效的临时求值器(约 230 行) |
| 2 | GROUP BY 表达式键投影为 NULL | `GROUP BY deal_time/100` 分组数正确但键全为 NULL | `rewrite_agg_refs` 只在投影项恰为 Column 时才映射到分组键列 | 函数入口增加整表达式与分组键的结构匹配 |
| 3 | 混合 CASE 聚合列错位(既有回归) | `SELECT volume, MAX(CASE...), SUM(volume), SUM(high_px) ... GROUP BY volume` 输出列串位、SUM 变 NULL、排序错乱 | `materialize_aggregate_inputs` 混用两个列空间:简单列保留原表索引、复杂表达式用投影索引,而下层 Project 只输出复杂表达式 | 进入物化分支时统一按投影布局重建全部分组键与聚合参数索引 |
| 4 | `%` 取模不支持 | 词法器无 `%`,解析报错 | — | lexer/parser 增加 `%` → `BinOp::Mod`,补齐常量折叠、命名与求值 |
| 5 | FULL JOIN 不支持 | `unexpected token: FULL`(执行器早已实现 probe_full) | parser 未分发 FULL | FROM/JOIN 解析增加 `FULL [OUTER] JOIN` |
| 6 | 常量除法与运行时语义不一致 | `SELECT 930/100` 折叠为 9,列上除法却是 9.3 | 常量折叠对 Int/Int 用整除 | 统一为浮点除法(与 MySQL 一致;推断类型本就是 Float64) |

## 二、支持度矩阵(验证通过)

| 类别 | 语句/特性 | 状态 |
|------|-----------|------|
| 投影 | 列、`*`、别名、算术表达式(`+ - * / %`) | ✅ |
| 选择 | `= <> < <= > >=`、`AND/OR/NOT`、`BETWEEN`、`IN/NOT IN`、`LIKE`(含 `_`/`%` 通配)、`IS [NOT] NULL`、函数调用谓词、CASE 谓词 | ✅ |
| NULL 语义 | 比较返回 NULL 并过滤、聚合跳过 NULL、`COUNT(*)` vs `COUNT(col)`、NULL 分组单独成组 | ✅ |
| 聚合 | `COUNT/SUM/MIN/MAX/AVG`、`DISTINCT`、聚合间算术、聚合参数为表达式或 CASE | ✅ |
| 分组 | 单列/多列/表达式键、`HAVING`(含聚合比较) | ✅ |
| 排序分页 | 多键混合方向 `ORDER BY`、`LIMIT/OFFSET`、按别名/聚合排序、TopN | ✅ |
| 连接 | `INNER`、`LEFT [OUTER]`、`RIGHT [OUTER]`、`FULL [OUTER]`、`CROSS`、逗号连接、自连接、`IS NULL` 反连接模式 | ✅ |
| 集合 | `UNION / UNION ALL`(含 ORDER BY/LIMIT) | ✅ |
| 子查询 | 标量子查询(含比较)、`IN (SELECT ...)` | ✅ |
| 子查询(二期) | 相关 `EXISTS / NOT EXISTS / IN / 标量`(WHERE/OR/NOT/SELECT 列表/聚合参数);派生表 `FROM (SELECT ...) alias`(含 JOIN、嵌套、UNION 分支) | ✅ |
| 标量函数 | UPPER/LOWER/ABS/COALESCE/ROUND 等(详见 scalar_functions_test) | ✅ |
| CASE | 简单式与搜索式、投影/WHERE/聚合参数中、NULL 落入 ELSE | ✅ |
| 无 FROM | `SELECT 1+1`、`SELECT 930/100`、`SELECT (SELECT ...)` | ✅ |

## 三、明确不支持(报错清晰,不会静默出错)

| 特性 | 错误信息 |
|------|----------|
| 跨多层的相关引用 / 相关子查询内再嵌相关子查询 | `nested correlated subqueries are not supported ...` |
| 相关子查询内使用 `?` 占位符 | `parameter markers (?) are not supported inside correlated subqueries` |
| JOIN ON 条件中的子查询 | join 上下文不支持的表达式错误 |
| UPDATE / DELETE 中的相关子查询 | 保持既有相关错误 |
| 窗口函数 | 明确报不支持(有专门测试守护) |

## 四、已确认的设计行为(非缺陷)

- `INT` 列存储与返回为 `Int32`(`SELECT *` 返回 Int32 标量)。
- `/` 为浮点除法:`930/100 = 9.3`(常量与列一致)。
- 搜索式 CASE 中 NULL 与任何值比较均为 NULL,落入 `ELSE` 分支。
- 相关标量子查询空结果返回 NULL;`SUM` 空集为 NULL(标准 SQL)。
- 相关子查询看到的是查询开始时的数据快照,与非相关子查询行为一致。

## 五、验证

- `cargo test --workspace`:618 个测试全部通过(含 `subquery_derived_table_test` 19 个场景与 `sql_semantics_regression_test` 24 个场景;其中相关 EXISTS/NOT EXISTS 与派生表用例由"必须报错"改为正向断言)。
- `cargo fmt` 已运行;clippy 仅余 1 个既有的 `too_many_arguments` 警告(内部溢出辅助构造器)。

## 六、后续建议

1. 去相关查询在确认右侧表均含于本次扫描集合后,可重新开放物理管线路径( pseudo 表需在 PhysicalContext 中补齐元数据映射)。
2. `eval_filter` 仍是 WHERE 谓词在物化路径的唯一权威实现;`apply_remaining_mask` 现已传播求值错误,新增表达式类型不再有静默放行风险。
