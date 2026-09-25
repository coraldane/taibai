# 三步走路线图:向量化执行 → Parquet/Arrow 生态 → 时序金融深化

> **状态**:规划中。本文是 2026-09 制定的主线路线图,按"第一步 → 第二步 → 第三步"顺序推进;
> 每完成一个里程碑在本文勾销状态标记。P3.4 与第一步无依赖,可穿插实施。
>
> **与现有计划的关系**:
> - 延续 `docs/plans/query-performance-architecture-refactor-plan.md` 的方向(该计划 Phase 0-5 已完成),
>   本路线图的第一步是其 Phase 4"向量化谓词执行"的自然延伸:从"谓词归一"推进到"表达式与算子全链路类型化"。
> - 实时摄取侧(A2 ingest profile DDL、A3 symbol dictionary、C3 replay、M2-M4)仍归
>   `docs/plans/realtime-market-data-ingestion-plan.md` 管,不在本文范围。
> - **实施状态差异记录**:该摄取计划标记 M1(A1+B1,服务端 v2 bulk 编码)已完成(2026-09-11),
>   但 `dev` 分支当前代码中不存在 v2 解析(`bulk_payload.rs`/`direct_bulk.rs` 无 magic/encoding 分支)。
>   本文所有规划以当前分支实际代码为准;若 v2 在其他分支,合入后 P2.3 的导入入口可直接受益。

## 现状基线(2026-09,HEAD = 49d3d31)

优势(向量化改造的现成地基):

- `ColumnArray` 已是物理类型数组 + 位图空值(`src/types/array.rs:283`),VarChar/Blob 为
  offset 式缓冲(Arrow 风格);`NullBitmap` 为 u64 字位图(`src/types/bitmap.rs`)。
- 已有类型化内核:`eq_vec/lt_vec/le_vec/gt_vec/ge_vec`、`filter_by_mask_fast`、`slice_fast`、
  `take(&[u32])`、`col_min/col_max/col_sum_*`(`src/types/array/analytics.rs`)。
- 扫描侧已按类型化掩码过滤(`src/sql/execution/scan/row_filter.rs`),物理管线
  `build_physical_pipeline` 已去物化(`src/sql/execution/physical/planner.rs`)。

热点短板(第一步的目标):

| 热点 | 现状 | 位置 |
| --- | --- | --- |
| 通用表达式求值 | 每节点物化 `Vec<ScalarValue>`,逐行循环 | `filter.rs:468` `eval_expr_column_with` |
| 比较快路径 | 仅 `col op literal`;`col op col`、算术后比较都走逐行 | `filter.rs:331` `eval_column_literal_comparison` |
| Project 通用路径 | 逐值 `coerce_project_value` + `push` | `project.rs:46-49` |
| Filter remap | 每个 batch 克隆整棵表达式树 | `filter.rs:24` |
| 标量函数 | 每 batch 新建 `FunctionRegistry`,逐行调用 | `filter.rs:614-622` |
| Hash Join 探测 | 逐行 `Vec<ScalarValue>` 建键 + 逐匹配收集 | `join.rs:306-560` |
| GROUP BY | 逐行 `encode_key_at` 编键,`HashMap<Vec<u8>, …>` | `aggregate.rs:674-762` |
| DISTINCT | 每值 JSON 序列化进 `HashSet<String>` | `aggregate.rs:274-298` |

---

## 第一步:向量化执行

目标:消除执行层 `ScalarValue` 逐行往返,让谓词、投影、聚合、Join 的热路径全部在
物理类型数组上运行;导入路径零回归(遵循旧计划的 import isolation 规则)。

### P1.0 基线护栏 ✅(部分)

- [x] 现有性能基线:`tests/performance_baseline_test.rs` + 服务端 EWMA 基线
  (`meta/performance_baselines.json`)已具备。
- [x] 新增 in-proc 查询微基准 `tests/vectorized_expr_bench_test.rs`
  (`#[ignore]` 诊断风格),度量表达式求值 fast path vs fallback;
  后续 P1.2/P1.3 复用同一基准骨架。

### P1.1 类型化表达式求值层 ✅(2026-09)

- [x] `src/types/array/analytics.rs` 新增内核:
  - `compare_column_column(op, &ColumnArray, &ColumnArray) -> Option<Vec<bool>>`
    (同变体数值/时间/字符串列-列比较);
  - 算术内核(列-字面量、列-列):Add/Sub/Mul/Mod 整数→Int64(溢出→NULL、模零→NULL),
    Div→Float64(除零→NULL),语义与 `eval_binary_scalar`(`filter.rs:666`)完全一致;
  - `arith_column_literal` / `arith_column_column -> Option<ColumnArray>`。
- [x] 新建 `src/sql/execution/expr_vec.rs`:`eval_expr_to_column(expr, batch, subqueries)
  -> Result<Option<ColumnArray>>`。支持 Column / Literal / 比较(列-列、列-字面量、
  算术操作数比较)/ 算术 / IsNull / IsNotNull / Not;不支持的形状返回 `None`,
  调用方回落现有 `eval_expr_column_with` 路径,行为零变化。
- [x] `filter.rs`:`eval_filter_with` 比较分支插入列-列与算术操作数快路径;
  `FilterExec::next_batch` 的 `remap_expr_for_batch` 结果按批量 schema 指纹缓存。
- [x] `project.rs`:通用路径先尝试 `eval_expr_to_column`,命中则整列一次转换;
  `FunctionRegistry` 提升为复用实例(不再每 batch 新建)。
- [x] 修复发现的隐患:`lt/le/gt/ge_vec` 原先对 Timestamp/Date/Time/Interval/
  Float/Decimal 字面量无同变体快路径,且回退分支要求 `numeric_f64().is_some()`,
  时序字面量会静默落到全 false——现已补齐同变体内核,回归测试锁定。
- [x] 三值语义分野:过滤掩码走两值(NULL 行不匹配),列表达式走三值
  (NULL/NaN 行产出 `Bool(None)`),NaN 比较按 `numeric_cmp` 的不可比语义视同 NULL。
  `expr_vec_test.rs::typed_path_matches_scalar_path` 对拍锁定。

实测(2026-09,Windows release,100k 行 × 5 轮,`--ignored --nocapture`):

| 场景 | 标量基线(ms) | 类型化(ms) | 提速 |
| --- | --- | --- | --- |
| ts >= 字面量(时序) | 33 | 6 | 5.5x |
| price > 150.0(浮点) | 31 | 12 | 2.6x |
| volume < 2500(整数,含 NULL) | 35 | 6 | 5.8x |
| symbol = 'S0042'(字符串) | 85 | 10 | 8.5x |
| price*1.5 > 200(算术操作数) | 52 | 23 | 2.3x |
| volume > volume(列-列) | 43 | 9 | 4.8x |
| volume + 7(投影算术) | 40 | 12 | 3.3x |

### P1.2 聚合热路径重构 ✅(2026-09)

- [x] GROUP BY 热路径重构(所有分组形态受益,`aggregate.rs`):
  键缓冲跨行复用(消除每行堆分配)、`contains_key` + `get_mut` 双哈希改为
  单次 `get_mut`(仅新组走二次查找)、每行两次 `estimated_size_bytes` 深度估算
  (DISTINCT 时为 O(集合大小))改为每批一次深刷新、聚合参数列索引每批解析一次;
  内存核算粒度与无分组路径对齐(每批)。
  说明:原计划的"物理类型向量做 key(`HashMap<u64>`)"需要复制整套分组存储与
  落盘结构,而实测收益主要来自上述记账开销修复,故按本方案落地;
  类型化整列哈希归入 P1.3(Join build/probe 需要同一套键空间)。
- [x] DISTINCT 类型化(含潜伏 bug 修复):`HashSet<String>` + serde_json/hex
  改为 `HashSet<Vec<u8>>`(复用 `encode_key_at` 自描述格式:类型标签 + 空标志 +
  定长/带长度负载),新增 `decode_key_scalar` 反解;修复 **GROUP BY 路径下
  SUM/AVG/MIN/MAX(DISTINCT) 恒为 0/NULL 的潜伏 bug**(旧 update_row 存 hex 键、
  finalize 按 JSON 解析必然失败,仅 COUNT 侥幸正确);DISTINCT 溢出落盘由
  VarChar 改为 Blob 列;新增分组 DISTINCT 值回归测试与编解码往返测试。
- [ ] `COUNT(*)`/`COUNT(col)`/`MIN`/`MAX` 在扫描层已有元数据快路径,补齐
  分组版最小裁剪(仅当组数小时才值得)。
- [ ] `update_row` 的 MIN/MAX 对 VarChar 参数每行克隆候选 String,可改为
  先按字节比较、命中才物化(留待 P1.3 顺带处理)。

实测(2026-09,Windows release,200k 行 / 5000 组 / 3 轮,
`cargo test --test aggregate_group_bench_test -- --ignored --nocapture`):

| 场景 | 重构前(ms) | 重构后(ms) | 提升 |
| --- | --- | --- | --- |
| GROUP BY symbol:COUNT(*), SUM(volume) | 121 | 72 | 1.7x |
| GROUP BY symbol:AVG(price) | 116 | 70 | 1.7x |
| GROUP BY symbol:MIN/MAX(price) | 134 | 83 | 1.6x |
| GROUP BY symbol:COUNT(DISTINCT volume) | 251 | 133 | 1.9x |
| 无分组:COUNT(DISTINCT symbol) | 143 | 59 | 2.4x |

### P1.3 Hash Join 批量探测 ✅(2026-09)

- [x] 纯等值条件判定 `condition_is_pure_eq`:条件恰为 `col = col` 合取时,
  哈希命中即条件成立,跳过每对的残留条件求值(`join.rs`);Float/Blob/Vector
  键保守回退(位哈希/Null 映射的相等面宽于标量相等)。
- [x] 批量配对探测:`JoinHashTable::probe_batch_row` 直接从列存储构建类型化键
  (免物化整行 `Vec<ScalarValue>`),`scan_probe_batch` 把命中收集为
  `(probe_row, build_ptr)` 索引对。
- [x] take 式输出组装:`emit_fast_chunk` 每列一次 `ColumnArray::take` 拼出输出
  batch(消除逐标量 push 与每对两次行克隆);键缓冲/索引 scratch 跨批复用。
- [x] 覆盖 INNER(双侧 build)、LEFT、RIGHT、SEMI(命中标记/尾部 NULL 补齐语义
  与旧路径一致);FULL/CROSS/ASOF、含非等值条件与 spill 路径保持原实现。
- [ ] build 侧利用有序输入改排序合并(需要物理属性 output_ordering 贯通,
  与 P3.2 复合聚簇键一并做)。
- [ ] Asof 执行器残桩(匹配行输出全 NULL)修复归入 P3.1。
- [ ] 变长键(VarChar)探测仍每行克隆一次键字符串,后续可换编码字节键空间。

实测(2026-09,Windows release,50k probe × 5k build,2 轮,
`cargo test --release --test join_probe_bench_test -- --ignored --nocapture`):

| 场景 | 重构前(ms) | 重构后(ms) | 提升 |
| --- | --- | --- | --- |
| INNER int64 键 | 56 | 6 | 9.3x |
| INNER varchar 键 | 61 | 7 | 8.7x |
| LEFT varchar 键 | 60 | 7 | 8.6x |
| SEMI varchar 键 | 55 | 7 | 7.9x |

### P1.4(可选)掩码与变长列(未开始)

- [ ] 位图掩码(`Vec<bool>` → 位压缩)与 selection vector 统一评估。
- [ ] VarChar 零拷贝视图(扫描过滤期间不物化 String)。
- 对齐 `docs/general-performance-foundations.md` 已声明的边界项。

**第一步验收**:query_bench 场景时延下降且性能基线无回归;`cargo test` 全绿;
`examples/perf_verify` 导入吞吐不变(±5%)。

---

## 第二步:Parquet/Arrow 生态

目标:打通数据进出通道,让 Taibai 进入 Python/数据工程工具链。
策略:**边界转换**,不把 arrow-rs 引入执行内核;`ColumnArray` 与 Arrow 数组物理表示
几乎一致,转换成本 O(n) memcpy 级。

### P2.1 类型映射与转换模块(未开始)

- [ ] 新增 `src/types/arrow_convert.rs`:`to_arrow(&RecordBatch) -> arrow::record_batch::RecordBatch`
  与 `from_arrow(...) -> RecordBatch`。
- [ ] 映射表(全部物理表示一一对应):

  | Taibai | Arrow | 备注 |
  | --- | --- | --- |
  | Bool | Boolean | 位图极性翻转(taibai 1=NULL,Arrow 1=valid) |
  | Int8..Int64 / UInt8..UInt64 | Int8..Int64 / Uint8..Uint64 | 直通 |
  | Float32/Float64 | Float32/Float64 | 直通 |
  | Decimal(i64 未标度) | Decimal128(precision, scale) | scale 在 ColumnDef |
  | Date(i32 天) | Date32 | 直通 |
  | Time/Interval(i64) | Time64(Microsecond) / Duration | Interval 语义实施时确认 |
  | Timestamp(i64 微秒) | Timestamp(Microsecond, None) | 时区不引入 |
  | VarChar(Utf8Buffer) | Utf8 | offsets u32→i32 检查式转换 |
  | Blob(BinaryBuffer) | Binary | 同上 |
  | Vector(Vec<Vec<f32>>) | List(Float32) | 定长后可升级 FixedSizeList |

### P2.2 导出(未开始)

- [ ] 语法:`COPY <table> TO '<path>.parquet'`;按 segment/row group 流式转换写出,
  不整表物化。复用扫描层(投影/谓词可下推到 COPY)。
- [ ] Arrow IPC(Feather V2)同语句支持(P2.1 完成后近乎免费)。

### P2.3 导入(未开始)

- [ ] 语法:`COPY INTO <table> FROM '<path>.parquet'`;读取 → `from_arrow` →
  现有 bulk 导入路径(约束预检、聚簇排序、分区路由全部复用)。

### P2.4(可选)Parquet 外表查询(未开始)

- [ ] `SELECT ... FROM 'x.parquet'`:ParquetScanExec;row group 统计(min/max)
  映射到现有 `RowGroupReadStrategy` 裁剪框架。

### P2.5(可选)协议层 Arrow 结果(未开始)

- [ ] 新消息类型或协商开关:查询结果以 Arrow IPC 返回,消除客户端二次编解码。

**第二步验收**:COPY 往返类型保真测试(NULL、Decimal scale、微秒时间戳、变长列);
导出/导入吞吐显著优于 CSV 路径;Python(pyarrow)可直接读取导出文件。

---

## 第三步:时序金融深化

目标:把"kdb+/DolphinDB 特色线"做实——ASOF JOIN、复合聚簇键、TTL 分区级过期、窗口函数。

### P3.1 ASOF JOIN 端到端 ✅(2026-09)

原状:AST 有 `JoinType::Asof` 但解析器无法产生;执行器 `probe_asof` 匹配行输出
全 NULL 右行;仅支持单时间键;`AsofJoinIndex` 无条件排序。

- [x] 语法:`ASOF JOIN`(内连接语义)与 `LEFT ASOF JOIN` / `ASOF LEFT JOIN`
  (保留左行,kdb `aj` 惯例),新增 `JoinType::LeftAsof`;
  ON 子句 = 若干等值键 + 恰好一个时间键不等式,`op ∈ {>=, <=, >, <}`。
- [x] 执行器重写(`AsofJoinIndex`):按等值键分区(`HashMap<JoinKey, Vec<时间有序
  entries>>`),二分按方向取最近过去/最近未来/严格变体;构建侧 batch 保留在
  `build_data`,匹配行真实取回(修复全 NULL 残桩);时间键限
  TIMESTAMP/INT64/DOUBLE,其余形态响亮报错。
- [x] NULL 语义:等值键或时间键为 NULL 的行不匹配(与 hash join 一致);
  LEFT ASOF 无匹配输出 NULL 右行。
- [x] 端到端测试 `tests/asof_join_test.rs`(8 项):最近过去/最近未来方向、
  两种关键字顺序、等值键分区隔离、多列等值键、NULL 键/时间、错误条件响亮报错。
- [ ] `TOLERANCE <n>` 容差参数(语法已预留,执行器未实现)。
- [ ] 探测侧利用聚簇有序退化为归并(依赖 P3.2 的有序性贯通)。

### P3.2 复合聚簇键前缀下推 ✅(2026-09)

原状:`cluster_key_bounds()` 只对第一列建边界;`(symbol, ts)` 典型查询无法
利用第二列范围。

- [x] `ClusterKeyBounds` 泛化为"等值前缀 + 末列范围"
  (`prefix_eq: Vec<(col, value)>` + `range_column_index` + lower/upper);
  二分键元组 = 前缀值 + 范围值,单列旧路径是前缀为空的特例,完全兼容。
- [x] row group 内组合键二分(`cluster_key_range_read`):排序性检查改为
  "键元组前缀字典序非降"(全键字典序排序蕴含任意前缀有序,组合搜索恒合法);
  命中区间连续切片 + `slice_fast`,元组比较对 NULL 行保守放行、由常规谓词兜底。
- [x] 段级元组裁剪:每列 zone map 构造 min/max 元组(min 元组 ≤ 真元组 ≤ max
  元组,保守正确),元组单调性防护 + 元组二分替代单列版本;ORDER BY 守卫
  保留单列单调检查。
- [x] 多列 ORDER BY 有序早停:`ordered_cluster_order` 接受匹配聚簇键连续
  位置且方向一致的 ORDER BY 列表(如 `ORDER BY symbol, ts LIMIT n`),
  混合方向或键位缺口拒绝。
- [x] 测试:strategy 边界构建 4 项、row_filter 组合二分(含开闭边界)1 项、
  引擎端到端 `tests/cluster_composite_test.rs` 3 项(跨 row group 精确区间、
  全符号扫描对拍、多列 ORDER BY 早停)。既有 `topn_offset_test` 反向抓住并
  修复了"无下界分支误用上界元组收窄起始段"的实现缺陷。
- [ ] 复合 (symbol, ts) 段级收益依赖段与符号运行对齐,probe 侧有序归并
  (ASOF/JOIN build 侧)待 output_ordering 物理属性贯通后跟进。

### P3.3 有序早停与 memtable 共存(未开始)

现状:memtable 有数据即禁用 ordered LIMIT 早停(`executor.rs:230-232`)。

- [ ] memtable 批次虽非全局有序,但每批内有序:维护每批 [min,max] 区间,与 segment
  有序流做 k 路归并判优,早停条件改为"所有源的剩余区间都不可能更优"。
- [ ] hot store(`HOT(...)` 表)已有 symbol 倒排索引,参与最近-N 查询的有限归并。

### P3.4 TTL 分区级过期 ✅(2026-09)

原状:TTL 仅读时行级过滤,过期分区永远躺在盘上,compaction 不清过期行。

- [x] 过期判定(`storage/partition/ttl.rs`):DAY/MONTH/YEAR 分区键解析出
  时段末微秒(UTC,与写入侧 `temporal_partition_key` 算术一致),
  `时段末 + TTL <= now` 即整分区不可见;TTL 列 ≠ 分区列、非时序分区、
  键格式非法时一律判定"不可推断"(保守)。
- [x] 后台清理(`sql/execution/ttl_purge.rs`):幂等对账式清除——删过期分区
  文件(含 `.meta` sidecar,行数从 segment reader 本身统计)+ 收编 catalog
  中文件已消失的过期条目 + row_count 扣减,catalog 保存失败回滚元数据;
  表级变更锁内执行,先 `flush_table_active` 防止 memtable 复活已删分区;
  挂到 close-of-day 调度线程,每分钟至多跑一轮(`Engine::run_ttl_partition_purge`)。
- [x] compaction 物理清除:排序与非排序两条路径都按 TTL 过滤
  (`ts + duration <= now` 或非 Timestamp 值),与读时过滤完全同界,
  并入 `rows_dropped` 统计。
- [x] 测试:过期判定单测 4 项(时段末/月年边界/守卫/与读过滤边界一致);
  端到端 `tests/ttl_purge_test.rs` 2 项(整分区回收 + catalog 更新 + 幂等,
  compaction 物理行数 5→2)。
- [ ] TTL 表 COUNT(*) 元数据快路径(需"整活分区/整死分区/边界分区"三分类,
  边界分区仍要走过滤,收益依赖分区周期 ≫ TTL 的场景)。
- [ ] 含唯一索引/主键表的 purge(需先处理索引项清理,当前这类表不参与
  整分区回收,读过滤仍然生效)。

### P3.5 窗口函数执行器 ✅(2026-09)

原状:解析器完整支持但 planner 直接报错,`LogicalPlan::Window` 为死代码,
物理层丢弃窗口节点,回归测试锁定"必须响亮报错"。

- [x] 类型重构:`WindowPlan.funcs: Vec<WindowFuncPlan>`,每函数独立
  PARTITION BY / ORDER BY / 帧;`WindowFuncKind`(排名/偏移/聚合)参数解析为
  LogicalExpr;修复解析器 LEAD/LAG 丢弃 value 参数的缺陷(标准三参形式)。
- [x] `plan_select` 接线:SELECT 顶层窗口表达式 → 转换 + 去重 + Window 节点;
  投影引用窗口计算列;ORDER BY 窗口别名解析到投影列;窗口 + GROUP BY/聚合、
  嵌套窗口、非 SELECT 位置一律响亮报错。
- [x] `WindowExec` 排序式执行:物化输入 → 每函数按 (partition, order) 独立
  排序行索引(复用 `compare_scalars` NULL 语义)→ 值按原始行位置散射,
  保持输入行序。
- [x] 函数集:`ROW_NUMBER`/`RANK`/`DENSE_RANK`(对等组)、`LEAD`/`LAG`
  (含 offset 与默认值)、窗口聚合 SUM/AVG/MIN/MAX/COUNT;帧支持默认
  RANGE UNBOUNDED PRECEDING..CURRENT ROW(累计含对等组)、无 ORDER BY 整分区、
  ROWS BETWEEN(滚动均值)。
- [x] 测试:`tests/window_function_test.rs` 8 项正例;回归测试改为
  "窗口 + GROUP BY / 嵌套窗口必须报错"的反向断言。
- [ ] RANGE 帧的显式偏移量(值前/后移)、窗口函数作为表达式子项
  (如 `ROW_NUMBER() + 1`)、聚合输入上的窗口计算。

**第三步验收**:ASOF 对拍全绿;`(symbol, ts)` 典型查询时延较 P3.2 前下降;
TTL 过期空间回收可观测;窗口函数回归测试覆盖排名/偏移/滚动三类。

---

## 风险与边界

- 不兼容的格式/协议变更须在 Pull Request 中突出说明(见
  `docs/user-guide/limitations.md`)。
- 每步改造以"不支持的形状回落旧路径"为第一原则,保证语义零变化后再谈提速。
- 性能数据仅代表测试环境;每次提速合并前跑 `cargo test` + 基线对比。
