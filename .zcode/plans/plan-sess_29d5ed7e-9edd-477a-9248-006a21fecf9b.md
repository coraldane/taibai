# taibai 聚合查询 OOM 优化方案

## 根因
`HashAggregateExec::next_batch`（`aggregate.rs:54-119`）把全部输入行物化成 `Vec<Vec<ScalarValue>>` 再 `clone` 进分组表，内存 O(行数×列数×32B)。对 L2 逐笔成交这种上亿行宽表 = 几 GB，直接 OOM。同时 `COUNT(*),MIN(),MAX()` 组合查询无法命中 `detect_count_star`（只认单一 `COUNT(*)`），白白读了全表却没用上已持久化的 row group zone map。

## 改动清单（按依赖顺序）

### 改动 1：`src/types/array.rs` — ColumnArray 整列向量化聚合方法
在现有 `impl ColumnArray`（line 985 那块，已有 `count_eq` 的位置）新增公共方法，**在模块内部访问私有 `data`/`bitmap` 字段，避免逐元素装箱**：
- `pub fn col_min(&self) -> Option<ScalarValue>` — 整列最小值（跳过 null），支持数值/时间/字符串
- `pub fn col_max(&self) -> Option<ScalarValue>` — 整列最大值
- `pub fn col_count_non_null(&self) -> u64` — 非 null 计数
- `pub fn col_sum_i64(&self) -> Option<i64>` / `col_sum_f64(&self) -> Option<f64>`
- 这些方法内部 `match` ColumnArray 变体，对 `data: Vec<i64>` 等直接 `iter().min()/max()/sum()`，配合 `bitmap` 跳过 null

**目的**：给累加器提供零装箱的列式更新原语，同时方案 B 可直接复用。

### 改动 2：`src/sql/execution/operators/aggregate.rs` — 重写为流式累加器（方案 A，治本）
完全重写 `HashAggregateExec::next_batch`，引入 accumulator 模式：

```rust
enum Accumulator {
    Count(u64),                              // COUNT(*) 或 COUNT(col)
    Min(Option<ScalarValue>, DataType),      // MIN(col)
    Max(Option<ScalarValue>, DataType),      // MAX(col)
    SumI(Option<i64>),                       // SUM(整数列)
    SumF(Option<f64>),                       // SUM(浮点列)
    Avg { sum: Option<f64>, count: u64 },    // AVG = SUM+COUNT
}
impl Accumulator {
    fn update(&mut self, col: &ColumnArray) -> Result<()>;  // 调改动1的整列方法批量更新
    fn finalize(&self) -> ScalarValue;
}
```

新执行逻辑：
- **无 GROUP BY**：维护一组 `Vec<Accumulator>`，`while let Some(batch) = input.next_batch()` 逐批 `accumulator.update(batch.column(col_idx))`，内存恒定。finalize 成单行结果。
- **有 GROUP BY**：`HashMap<Vec<u8/groupkey>, Vec<Accumulator>>`，逐 batch 按 group key 更新对应累加器。内存 = O(分组数 × 聚合数)，对绝大多数查询仍是 KB-MB 级。
- 保留现有的 `having`/`distinct`/schema-remap（`remap_aggregate_for_batch`）后处理逻辑，保持对外行为不变。
- 保留 `compute_aggregate`、`format_aggregate_name`、`aggregate_result_type`、`compare_scalars` 等公共函数签名（`push_aggregate.rs` 仍引用 `compute_aggregate`，虽是次要路径但避免破坏）。
- `distinct` 聚合（如 `COUNT(DISTINCT col)`）仍需收集值集合，但对单分组也是单列去重，内存可控；保留现有行为。

**收益**：内存峰值从 GB 级降到 KB 级，彻底解决 OOM。性能因向量化（整列 min/max/sum 而非逐行）再提升数倍。

### 改动 3：`src/sql/execution/scan/aggregate_meta.rs`（新文件）— 元数据直答（方案 B）
实现 `compute_simple_aggregate(spec, aggs, table_meta, table_dir, segment_mgr, memtable_mgr, db) -> Result<Vec<ScalarValue>>`：

COUNT(*)：复用 `count::count_star`（已含分区/row group 剪裁 + 删除位图 + memtable 计数）。

COUNT(col) / MIN(col) / MAX(col)：遍历分区剪裁后的候选 segment + row group：
- 若 segment `deleted_row_count == 0`（无删除）：
  - COUNT(col) = Σ(row_count) − Σ(zone_map[col].null_count)
  - MIN(col) = min over candidate rg of `zone_map[col].min_value`
  - MAX(col) = max over candidate rg of `zone_map[col].max_value`
  - **零数据读取，纯 footer 元数据**
- 若有删除（保守处理，保证正确）：
  - MIN/MAX：先按 zone map 取「可能包含全局极值」的候选 row group 集合（zone map 与当前极值有交集的 rg），再 `reader.read_column(rg, col_idx)` 读该列，用 `delete_bitmap` 过滤后调改动 1 的 `col_min/col_max` 合并
  - COUNT(col)：退化为读列计数（复用 `count::count_with_predicates` 思路）

memtable 部分：COUNT 直接用 `memtable::count_*_rows`；MIN/MAX 遍历 memtable batch 调 `col_min/col_max` 合并。若命中 resident hot store，同样合并。

### 改动 4：`src/sql/execution/scan/executor.rs` — 方案 B 入口
加 `execute_simple_metadata_aggregate(&mut self, aggs: &[AggregateExpr]) -> Result<Vec<RecordBatch>>`，调改动 3，用 `count::build_count_star_result` 类似方式构造结果 batch。在 `execute()` 顶部增加分支：若 `spec.is_simple_metadata_aggregate` 则走此路径。

### 改动 5：`src/sql/execution/query.rs` — 方案 B 检测
新增 `detect_simple_metadata_aggregate(stmt, table_names) -> Option<Vec<AggregateExpr>>`，条件：
- 单表、无 `GROUP BY`、无 `HAVING`、无 `DISTINCT`
- `select_list` 全是 `COUNT/MIN/MAX`（`COUNT` 参数为 `*` 或普通列，`MIN/MAX` 参数为普通列），无其他表达式
- 保留 `detect_count_star` 优先（它是方案 B 的特例，更早短路）

在 `exec_select`（line 188 附近）调用检测，命中时在 `ScanExecutor` 上设 `simple_metadata_aggregates` 字段并走快路径。与现有 `can_use_physical_pipeline` 互斥优先。

### 改动 6：`src/sql/execution/scan/mod.rs` / `spec.rs`
- `mod.rs` 导出 `aggregate_meta`
- `ScanSpec` 加 `simple_metadata_aggregates: Option<Vec<AggregateExpr>>` 字段（默认 None），用于 cursor/deferred 路径透传

### 改动 7：`src/sql/execution/physical/planner.rs` — 聚合投影下推（方案 C）
在 `LogicalPlan::Aggregate` 分支（line 142）：
- 从 `agg.aggregates` 收集所有 `arg` 引用的列索引集合 `agg_cols`
- 若 input 是 `LogicalPlan::Scan`，给 Scan 设置 `projection = Some(agg_cols ∪ 谓词列)`（谓词列由现有 scan spec 决定）
- 依赖现有 `remap_aggregate_for_batch` 处理投影后的列索引重映射（已验证该逻辑存在且正确）
- **仅当聚合所需的列集 ⊂ 全表列时才下推**（避免空投影风险），保守取交集

**收益**：你的 SQL 只需读 `TradeDate`(谓词)+`TransactTime`+`LocalTime` 3 列，跳过买卖盘十档等几十个宽列，IO 降一个量级。

### 改动 8：`src/common/config.rs` + `conf/windows.yaml` — 内存熔断（方案 D）
- `BufferSettings` 加 `#[serde(default = "default_query_memory_limit_mb")] pub query_memory_limit_mb: u64`（默认 4096）和 `#[serde(default = "default_max_aggregate_groups")] pub max_aggregate_groups: usize`（默认 1_000_000）
- `default_` 函数 + `Default` impl 同步更新
- `conf/windows.yaml` 加注释示例项

### 改动 9：熔断接入
- 方案 A 的累加器在 GROUP BY 分组数超过 `max_aggregate_groups` 时返回 `TaibaiError::Sql("aggregate group count exceeds limit N; consider refining GROUP BY")`，而不是无限制吃内存
- 查询级内存软限：在聚合算子累积处理行数估算上超 `query_memory_limit_mb` 时报错（粗粒度估算，作为兜底，防未覆盖的坏查询拖垮进程）。具体用一个简单的「已处理行数 × 行宽估算」阈值检查，避免引入 jemalloc/分配器 hook 的复杂度。

### 改动 10：测试
- `tests/` 新增聚合大数据量测试：插入百万级行，验证 `COUNT(*),MIN(),MAX(),SUM(),AVG()` 内存稳定 + 结果正确
- 扩展 `query_predicate_test.rs`：加 `SELECT COUNT(*),MIN(x),MAX(x) FROM t WHERE ...` 用例，验证方案 B 路径结果与逐行计算一致（含删除行场景）
- `cargo test` 全量回归（含现有 COUNT(*) 测试不受影响）

## 风险与控制
- **方案 B 正确性**：删除行场景必须保守验证。实现里对 `deleted_row_count>0` 强制走读列路径，不信任 zone map 给的精确极值，保证正确。加专门测试覆盖「删除极值行后 MIN/MAX 仍正确」。
- **方案 C 列索引**：依赖现有 `remap_aggregate_for_batch`，已有成熟逻辑；加测试验证投影后聚合结果。
- **死代码不动**：`src/sql/operators.rs`、`operators_fix.rs`、`push.rs` 是未挂载遗留文件，不碰。
- **行为兼容**：方案 A 对外输出 schema/结果完全不变；方案 B 是短路快路径，结果必须与走聚合算子一致（测试保证）。

## 验证
- `cargo build` 编译通过
- `cargo test`（重点 query_predicate_test、新聚合测试、delete_test）
- 对 `sz_l2_transaction` 的崩溃 SQL 重跑，确认毫秒级返回且内存平稳