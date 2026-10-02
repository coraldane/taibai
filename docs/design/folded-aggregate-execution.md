# 聚合折算执行（metadata-folded aggregate execution）

## 问题

`Aggregate(单 Scan)` 形状的查询——含 `COUNT(*)` 与 `GROUP BY`——曾经由四条并行路径服务：

- `count(*)` 专用五级链（hybrid 镜像 → 常驻热存 → TTL → 纯 memtable → 通用元数据），由 **AST 级**检测触发；
- 无分组简单聚合的元数据快路径，显式拒绝 GROUP BY，靠跨层布尔协议回传；
- 物理 pipeline（流式 cursor + HashAggregateExec）——GROUP BY 全表解码键列逐行哈希；
- 物化路径（显式事务 / EXPLAIN ANALYZE / 派生表）——丢失列裁剪。

行组候选枚举（sidecar meta 剪枝 → reader 回退）在四处各抄一遍。按日期分区的表上，
"逐日 `count(*)` 快、整表 `GROUP BY date` 慢"的根因即在此：分区键的答案全部躺在
footer 元数据里，却要逐行解码去算。

## 目标架构

**一个折算引擎、一个接入点、一套候选枚举。**

```
exec_table_select
├─ 物理 pipeline（提交读、无派生/相关子查询）
│    └─ Aggregate 分支 ── try_folded_aggregate_exec
│         ├─ 命中 → FoldedAggregateExec
│         │    首次 next_batch 运行折算引擎（flush 读锁内）
│         │    ├─ Some(batch) → 直接产出最终行（含分组）
│         │    └─ None(decline) → 惰性构建 HashAggregateExec over ScanExec::deferred
│         └─ 形状不符 → 常规 HashAggregateExec 管线
└─ 物化路径（显式事务 / ANALYZE / 派生表）
     └─ spec.metadata_fold = extract_metadata_fold(plan)（与 pipeline 同一形状规则）
          └─ ScanExecutor::execute_folded_aggregate → 同一引擎；decline 则回退原始扫描
```

`COUNT(*)` 不再是特例：它就是"无分组的 `Aggregate(Count Wildcard)`"，与
`GROUP BY 分区键` 共用同一条路径。

## 折算引擎（`scan/aggregate_meta.rs::compute_folded_aggregate`）

输入 `ScanSpec + GROUP BY 表达式 + 聚合表达式`，输出最终结果批（多行，首见序）；
`Ok(None)` 表示拒绝，调用方回退常规管线。

### 折算单元与分类

逐段（分区裁剪 + 段级 zone map 之后）、逐行组分类：

| 单元状态 | 处理 |
|---|---|
| 全部键列 zone map 单值（`min==max` 非空、`null_count==0`）或全 NULL，无删除位图，聚合可由元数据回答 | **meta-fold**：零解码，整单元折入其唯一组。Day 分区表的分区键段恒属此类，整表 GROUP BY 退化为一次 footer 遍历 |
| 键列漂移为常量 DEFAULT（footer 早于该列） | 键恒为该常量，整单元折入一组（`fold_constant_default`） |
| 其余（键值分裂、有删除、zone map 未计算、SUM 无 footer 和） | **data-fold**：仅解码键列+聚合参数列（+谓词列），批内键单值则向量化折入一组，否则逐行归组；删除行先经位图过滤 |

段级捷径：无谓词、无删除、无 SUM/AVG 且全部键列在**段级** zone map 单值 → 整段一次折算。

### 部分态来源

- `COUNT(*)` → `row_count`（扣删除位图区间：`row_range_deleted_count`）
- `COUNT(col)` → `row_count − null_count`
- `MIN/MAX` → zone map 端点
- `SUM/AVG` → footer 逐行组 `col_sums`；无分组的 SUM 另有进程内段级部分和缓存（键不含分组值，分组查询禁用）
- 常驻热存 / hybrid 镜像 / memtable / hot 腿：与流式 cursor 的 initialize 同语义（常驻镜像整体作答；hybrid 镜像收上界以上行并以 `<= covers` 约束其余腿）

### 拒绝条件（内聚于引擎）

TTL 激活（读时过滤行相关）、表达式 GROUP BY 键、DISTINCT 聚合、HAVING（planner 侧拒绝）、
谓词列 footer 缺席、键列/参数列 `DEFAULT CURRENT_TIMESTAMP`、前导聚簇列等值点查形态、
SUM/AVG 参数类型不支持。拒绝 = 回退，绝不产生错误答案。

### 不变量依赖

- 写入路由保证 Day 分区段内分区键同值（`write_batch_by_partition`；UPDATE 改键重路由；compaction 不跨分区）；
- 行组落盘即写 zone map / col_sums / footer（或 sidecar）；删除走 footer 重写（缓存 reader 失效）或事务 overlay（`txn_has_segment_overlay` 降级）；
- schema 漂移按列名（现名+曾用名）对齐：`FooterColumnDrift`。

## 输出契约

组键列（源列名/类型）+ 聚合列，与 `HashAggregateExec` 的 `grouped_output_schema` 同布局；
pipeline 中折算算子位于 Project 之下，别名/列序由上层正常处理。物化路径的
`extract_metadata_fold` 要求 Project 为聚合输出的位置恒等映射，否则拒绝（旁路返回时列序即管线列序）。

## EXPLAIN ANALYZE

ANALYZE 不再强制走物化全扫——它测量真实执行路径。折算回答在 OPERATOR METRICS
中以 `AggregateFold <table>` 算子条目呈现（无 Scan 条目 = 零行解码）。

## 候选枚举（`scan/candidates.rs`）

`open_segment_with_candidates`：sidecar meta 剪枝优先（空结果不开段文件）、
reader 回退，供流式 cursor 与物化 executor 内联路径共用。并行扫描路径（需占位序号）
与折算引擎（需对齐检查本身）直接调用底层原语（`row_filter::meta_prune_row_groups` /
`strategy::prune_segment`）。

## 后续工作

- run 级折算（RLE 逐 run 发射分组）的分组变体——接口已预留（`run_folds_enabled`）；
- 流式 cursor 段间乱序并行解码（吞吐项，与本设计正交）；
- 多列 GROUP BY 的 meta-fold 目前要求全部键列单值；分裂单元已走 data-fold 逐行归组，正确但可进一步向量化。
