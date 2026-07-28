# Taibai 通用性能基础

本轮建设面向通用数据库执行与存储能力。行情数据只是首个验证负载，所有优化均不依赖行情表结构或时间序列语义。

## 已落地

### 1. 批量向量执行

- 谓词比较复用类型化批量运算。
- 过滤使用 `SelectionVector`，集中处理选择索引和列批量提取。
- `RecordBatch`、列和空值位图提供统一的内存估算。

### 2. 受控并行调度

- 查询线程数、单次扫描最大并行 Row Group 数和并行启用阈值均可配置。
- 扫描按有界窗口提交任务，避免单个查询一次性占满全局线程池。

### 3. 内存预算与可靠落盘

- 排序按查询内存预算生成有序 run，使用 Taibai segment 格式落盘后多路归并。
- 高基数 `GROUP BY` 将可合并中间状态写成有序 run，再按 key 流式归并。
- 单组及全局高基数 `DISTINCT` 使用类型安全键编码和外部有序去重。
- Inner Hash Join 超过预算后对 Build/Probe 两侧做一致哈希分区，再逐分区连接。
- 临时数据带压缩、校验和与 RAII 清理；排序和聚合归并使用最小堆。

### 4. 统计信息与成本基础

- `ANALYZE TABLE [database.]table` 扫描所有可见 segment 和删除位图，生成表级行数以及列级 NDV、空值数、min/max 和等深直方图。
- 单列与复合 NDV 使用固定 4096 寄存器的 HyperLogLog；直方图每列最多保留 2048 个确定性优先级样本并生成最多 32 个桶，采集内存不随表行数增长。
- 复合 NDV 仅采集显式声明的复合主键、唯一索引和聚簇键，去重后每表最多 32 组；因此不会按列数平方级枚举任意组合。
- 统计快照持久化到目录；只有行数与 schema version 同时匹配时才会进入成本估算。写入或结构变化后，旧快照自动失效。
- 选择率估算支持等值、范围、`IN`、`AND`、`OR` 等常见谓词；范围谓词使用直方图 CDF，单值高频桶也会修正等值选择率。
- 无序 `LIMIT` 会依据 zone map、value set 等 Row Group 元数据优先扫描预计命中率高的块。
- Inner Hash Join 根据目录行数与过滤选择率选择较小 build side，并保持 SQL 输出列顺序不变。
- Join 计划画像会传播单列与复合 NDV；普通等值连接使用 `left_rows × right_rows / max(left_NDV, right_NDV)`，命中已统计复合键时以整组 NDV 估算，不再假设组内列相互独立。
- 主键或唯一索引覆盖连接键时，估算器使用唯一侧约束限制输出上界；连接后的 NDV 会继续传播给上层 Join，但唯一性在可能发生行复制后保守清除。
- 3 至 8 张表的连通 Inner Join 树使用左深树动态规划搜索，以累计扫描行数和中间结果行数为成本；超过 8 张表时使用同一基数模型的连通贪心搜索。重排后添加投影恢复原列布局。
- Join 条件的限定列使用别名绑定的精确列区间解析，不再依赖表名或别名前缀猜测。
- `EXPLAIN` 复用物理计划准备路径，展示 CBO 重排后的 Join 树，并在每个节点输出 `estimated_rows`；`EXPLAIN ANALYZE` 继续附加实际扫描、裁剪、读取字节、返回行数和总耗时。
- `EXPLAIN ANALYZE` 额外输出逐算子的 `actual_rows`、估算/实际行数比、输出批次数、耗时、峰值内存估算，以及 spill 文件数、行数和数据文件字节数；所有指标来自同一次查询执行。
- 算子画像只在显式 `EXPLAIN ANALYZE` 时启用；spill 通过执行线程上的算子作用域自动归属，普通查询不承担锁和计时开销。
- 每次 `EXPLAIN ANALYZE` 会将总耗时、最大算子内存、总 spill 字节和最大基数误差写入查询性能基线。查询形状在物理重排前生成，常量统一替换为占位符，因此不同参数和不同物理 Join 顺序仍可比较。
- 性能基线持久化到 `meta/performance_baselines.json`；默认最多保留 1024 条、前三个样本用于预热，之后只有稳定样本才以 0.2 权重更新 EWMA，回归样本不会污染基线。这些参数均可在 `performance` 配置段调整。
- 默认回归条件包括：耗时同时增长超过 1.5 倍和 5 ms、内存同时增长超过 1.5 倍和 1 MiB、从无 spill 变为 spill 或 spill 超过 2 倍、基数误差显著扩大。可通过 `SHOW PERFORMANCE BASELINES` 查看，使用 `RESET PERFORMANCE BASELINES` 清理当前数据库、环境和版本的基线。

### 5. 可配置的数据块粒度

- 写入 Row Group 目标行数可配置，批量导入按目标粒度切块。
- 现有 segment 格式和历史数据保持兼容，读取侧继续使用 Row Group 级裁剪与并行解码。

## 配置项

```yaml
buffer:
  execution_threads: 8
  max_parallel_row_groups: 8
  parallel_scan_min_row_groups: 4
  spill_batch_rows: 4096
  join_spill_partitions: 16

storage:
  row_group_target_rows: 131072

performance:
  max_entries: 1024
  warmup_samples: 3
  ewma_alpha: 0.20
  elapsed_regression_ratio: 1.50
  elapsed_regression_min_ms: 5.0
  memory_regression_ratio: 1.50
  memory_regression_min_mb: 1
  spill_regression_ratio: 2.0
  row_error_regression_ratio: 2.0
  row_error_regression_min: 2.0
  environment: "default"
  # version_label: "release-2026.07"
```

这些值需要按目标机器和真实负载标定，示例值不是所有环境的统一最优值。

## 当前边界

- `SelectionVector` 已统一过滤路径；字符串和其他变长列的零拷贝视图仍需单独设计。
- 排序、分组聚合、`DISTINCT` 和 Inner Hash Join 已具备外部执行路径；Outer/Semi/Anti/Asof Join 的分区 spill 尚未实现。
- 当前 `ANALYZE TABLE` 会精确扫描全表并对直方图做有界抽样，适合显式维护；自动触发、增量统计和仅抽样扫描尚未实现。
- 多表 Join 已对最多 8 张表执行左深树动态规划，但尚未搜索 bushy tree；未显式声明组合的列相关性、外键包含关系和跨表达式统计仍是后续重点。
- 算子 `elapsed_ms` 包含其管线内的子算子调用，Scan 还会计入存储读取阶段；`peak_memory` 是 RecordBatch、Hash 状态和缓冲区的内部估算，不等同于进程 RSS。
- 当前性能基线仍由显式 `EXPLAIN ANALYZE` 样本驱动；环境和版本标签用于隔离不同部署的样本，但需要运维侧按硬件/发布批次正确配置。后台自动采样和自动识别硬件环境仍是后续能力。
- 当前页粒度使用兼容的 Row Group 分块；列内压缩 page、page index 和 page cache 仍是后续格式演进。

## 建议验收指标

- 吞吐：固定数据规模下扫描、过滤、聚合、排序和连接耗时。
- 延迟：冷缓存与热缓存的 P50/P95/P99。
- 资源：峰值 RSS、临时磁盘峰值、读取放大和 CPU 利用率。
- 并发：1、4、16、64 个并发查询下的吞吐与尾延迟。
- 优化器：固定数据集上比较原 Join 顺序与统计重排后的中间结果行数、峰值内存和总耗时。
- 正确性：与基准引擎结果对比，覆盖 spill、删除位图、空值、别名、自连接和事务可见性。
