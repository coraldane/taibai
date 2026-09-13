# Taibai vs DuckDB vs kdb+ 性能对比报告(2026-09)

> 对象:taibai 0.2.4(dev 分支 ed3d7d5 + 本轮修复,本机构建)与 DuckDB 1.5.5、kdb+(公开数据,见 §6)。
> 场景:行情 tick 类时序负载 —— 批量导入、实时写入、按 symbol 点查、按日聚合。
> 性质:单机功能/性能对比,**非生产就绪性评估**;taibai 为实验性项目(见 README)。
> 修订:v2 —— 修正了 v1 中对"刷盘后热存失效"的误诊(真因是收盘归档调度器,见 §5),并补入本轮修复与插入优化的实测结果(§7、§8)。

## 1. 测试环境

| 项 | 值 |
|---|---|
| CPU | Intel Core Ultra 7 270K Plus(24 核 24 线程) |
| 内存 | 47.5 GB |
| 磁盘 | 2TB NVMe SSD(JZ-SSD5.0) |
| OS | Windows 11(win32 10.0.26200) |
| **本机 fsync 延迟** | **中位 27.6ms**(4KB 写 + fsync 循环实测;Windows FlushFileBuffers 特性,生产 Linux NVMe 通常为 0.1~2ms) |
| taibai | 0.2.4,release 构建,`tail_async_fsync=false`(默认严格 fsync),execution_threads=16 |
| DuckDB | 1.5.5(Python 嵌入模式,file 数据库) |

**数据集**:`ticks_10m.csv` —— 10,000,000 行 × 34 列(与 `examples/batch_import` 的 tick schema 一致),1000 只 symbol × 5 个交易日,CSV 2.35 GB。另取 day-1 的 200 万行作为"当日实时"数据集。每个查询重复 5~7 次取中位数,页缓存预热后测量。

**表结构**:冷表 `stock_tick_batch`(PARTITION BY deal_date,CLUSTER BY (security_id, deal_date, deal_time));热表 `hot_tick`(同上 + `HOT BY (local_time) INDEX (security_id) CLOSE AT 'HH:MM'`)。

## 2. 结论速览(TL;DR)

- **批量导入**:DuckDB 337 万行/s,taibai 165 万行/s(差 2 倍);taibai 压缩率更高(400MB vs 667MB / 千万行)。
- **实时写入(持久化)**:调优后 taibai **25.6 万行/s**(fsync ACK 语义),已超过 DuckDB appender 的 23.1 万行/s;高并发下组提交把 fsync 成本摊薄到接近免费(strict ≈ async)。
- **点查(taibai 的主场)**:盘中热存路径 **~1ms**(2000 行结果,刷盘前后一致);收盘归档后 1.5~4ms;DuckDB 无二级索引、未聚类,同查询 55ms。
- **扫描聚合(DuckDB 的主场)**:v3→v5 后差距大幅收敛(全表聚合 0.97ms vs ~38ms;千万行 GROUP BY 11ms vs 111ms——从 76× 收敛到 10×);剩余差距在字典直通解码与流水线重叠(见 §10)。
- **hotstore 进程内微基准**:append 1600万~1709万行/s,symbol 最新 N 条点查 1.2~2.0µs,整键 gather 1900万~2615万行/s。
- **本轮修复**(§7):修掉 3 个热存快路径上的真实正确性 bug(事务行丢失、reclaim 后丢行、REPLACE 残留),并澄清 v1 报告的"352ms 缺口"真因是收盘归档调度。

## 3. 插入性能

### 3.1 批量导入(10M 行 × 34 列,CSV → 持久化)

| 系统 | 路径 | 耗时 | 吞吐 | 磁盘占用 |
|---|---|---|---|---|
| taibai | `batch_import` 直连二进制 bulk 协议(客户端解析 + 网络 + 服务端 zstd 压缩直写 segment) | 6.05 s | **1,652,993 行/s** | 400 MB(10M 行) |
| DuckDB | `CREATE TABLE AS SELECT * FROM read_csv(...)` + CHECKPOINT(嵌入式,并行 CSV 解析) | 2.96 s | **3,373,802 行/s** | 667 MB(10M 行) |

### 3.2 实时写入(200 万行 × 34 列,小事务持久化)——v2 更新

| 形态(batch×clients) | 吞吐(严格 fsync ACK) |
|---|---|
| 5000 × 4(hot_insert 旧默认) | 76,599 行/s(冷)/ 133,027 行/s(热身) |
| 20000 × 4 | 142,741 行/s |
| **20000 × 8(新默认)+ 服务端槽位 48** | **256,449 行/s** |
| 20000 × 16 | 235,427 行/s(平台期) |
| 20000 × 8,`tail_async_fsync=true` | 229,256 行/s(≈ strict —— 组提交已把 fsync 摊薄) |
| 参照:DuckDB Appender,每 1 万行一个 COMMIT | 231,127 行/s |

要点:并发客户端 + 更大批次让每个 fsync 组携带更多行,是与 DuckDB 拉平并反超的关键;由于组提交的摊薄效应,严格持久化在该形态下几乎不再付出吞吐代价(async 不再更快)。hot_insert 示例默认值已更新为 batch=20000 / clients=8。

### 3.3 小语句(100 行/条 INSERT)——v2 剖析

insert-profile 阶段拆解(100 行/条,服务端):

| 阶段 | 中位耗时 |
|---|---|
| fast_materialize(SQL 解析 + 物化) | 25 µs |
| memtable_append | 7 µs |
| **write(含 durable tail 组提交 fsync 等待)** | **41.2 ms** |
| catalog 等 | < 5 µs |

结合本机 fsync 实测 27.6ms:**小语句 95% 以上的服务端时间在等 fsync,代码逻辑仅 ~0.4ms**。这是平台耐久性延迟地板,不是实现问题:

| 形态 | 吞吐 |
|---|---|
| 串行 1 客户端,严格 fsync | 2,540 行/s(25 stmt/s) |
| 8 并发,严格 fsync(组提交) | 13,908 行/s(5.5×) |
| 8 并发,`tail_async_fsync=true` | 26,420 行/s |

**使用建议**:小语句务必多客户端并发(组提交摊薄);容忍秒级丢失的行情流可开 `tail_async_fsync`;生产部署 Linux(NVMe fsync 通常快 1~2 个数量级)。

### 3.4 热存储进程内微基准(`tests/hot_store_bench_test.rs`)

4 列 schema,1000 symbol,4096 行/批:

| 规模 | append | 点查(最新 10 条/symbol) | 整键 gather | 内存驻留 |
|---|---|---|---|---|
| 100 万行 | 1,599 万行/s | **1.16 µs/次** | 2,615 万行/s | 45 MB(~45 B/行) |
| 1000 万行 | 1,709 万行/s | **1.98 µs/次** | 1,902 万行/s | 453 MB |

## 4. 查询性能(10M 行 × 34 列冷表)

| 查询 | taibai 原始导入后 | taibai OPTIMIZE 后 | taibai OPTIMIZE + 扫描优化(v3) | taibai + 分组快路径(v4) | taibai + 并行分组(v5) | DuckDB |
|---|---|---|---|---|---|---|
| Q1 点查:单 symbol 单日 → 2000 行×34 列 | 363 ms | **3.0 ms**(wall 5.1 ms) | ~5 ms | ~3.9 ms | **~3.9 ms** | 55.0 ms |
| Q2 单 symbol 最新 10 条 | 616 ms | **6.5 ms** | 不变 | ~6.0 ms | **~6.0 ms** | 7.0 ms |
| Q3 单 symbol 全期聚合 | 308 ms | **1.9 ms** | ~2.0 ms | ~1.9 ms | **~1.9 ms** | 2.5 ms |
| Q4 单日 2M 行 GROUP BY | 219 ms | 181 ms | 79 ms | 47 ms | **26 ms**(8.4×) | **7.2 ms** |
| Q5 全表 10M 行 GROUP BY | 1.07 s | 836 ms | 388 ms | 229 ms | **111 ms**(9.6×) | **11.0 ms** |
| Q6 单日 2M 行过滤聚合 | 49 ms | 28 ms | 12.7 ms | ~10 ms | **~10 ms**(4.9×) | **0.72 ms** |
| Q7 全表 10M 行 count+sum | 224 ms | 110 ms | 38 ms | ~45 ms | **~38 ms**(2.9×) | **0.97 ms** |

(`OPTIMIZE TABLE` 一次性成本 2.43 s/10M 行。公平性:DuckDB 嵌入式无网络;taibai wall 含 localhost TCP,server_elapsed 不含。)

### 4.1 热表(hot_tick,200 万行驻留热存)——v2 修正标签

| 场景 | Q1 点查(symbol+date) | Q2 latest10 | Q3 symbol 聚合 |
|---|---|---|---|
| 盘中,写入进行中 | 1.3 ms | — | 66~70 ms |
| **盘中,memtable 已刷盘(本轮修复验证)** | **~1.0 ms**(wall ~3ms) | 146 ms(未聚类段扫) | 130 ms |
| 收盘归档后(flush+seal+聚类压缩) | ~1.5 ms | **4.3 ms** | **0.9 ms** |

要点:热存快路径在整个交易时段保持 ~1ms 点查,不随刷盘退化;收盘归档(聚类压缩)把无 partition 谓词和 ORDER BY 类查询从 130~146ms 带到 0.9~4.3ms。

## 5. "352ms 慢查询"的真正原因(v1 误诊修正)

v1 报告把"写入结束后热表点查 352ms"归因于"刷盘使热存失去 resident 覆盖"。**本轮排查证明这是误诊**,真因链是:

1. `hot_insert` 默认 `CLOSE AT '15:30'`,而基准在 15:30 之后运行;
2. taibai 的**收盘归档调度器**(kdb+ 收盘清场模式,`Engine::close_of_day_flush_table`)在到达 CLOSE AT 后自动执行:flush memtable → seal segment → **移除热存**(设计行为,内存交棒给历史层)→ 对脏分区做聚类压缩;
3. 归档进行中/完成前的查询落在**未聚类段扫描**上 → 352ms;归档完成后同类查询为 1.5~4ms(§4.1)。

即:收盘归档本身是设计(kdb+ 同款生命周期),v1 测量恰好踩进归档竞态窗口。盘中的热存快路径(memtable 刷盘前后)在正确性保障下始终保持 ~1ms。

不过,追查过程发现并修复了热存快路径上 **3 个真实的正确性 bug**(详见 §7)——它们会在混合事务、内存压力回收、REPLACE 场景下导致查询漏行或残留旧行。

## 6. kdb+ 参照(公开数据,非本机实测)

- KX 官方性能页([code.kx.com/q/kb/performance-tips](https://code.kx.com/q/kb/performance-tips)):100 万行内存表 append,单行插入 264 万行/s,1 万行批量 **3.33 亿行/s**(12 核 Mac mini,kdb+ 4.1t);`g#` 属性使 1000 万行 trade 快照查询从 84ms 降至约 0.08ms/次。
- 学术基准(arXiv:2301.12561,i7-1065G7 笔记本):kdb+ 磁盘导入 47.35 MB/s;综合排名 kdb+ >> InfluxDB >> ClickHouse >> TimescaleDB。

## 7. 本轮修复实录(热存快路径正确性)

**机制**:新增 `MemTableManager` 按 `(db, table, partition)` 记账 **hot-unfed 行数**(所有绕过热存镜像的行:显式事务提交增量、bulk 模式非喂入写、relaxed/bulk 喂入失败、表级哨兵)。查询侧 `resident_hot_covers` 现在要求 **resident 标记 ∧ 分区未镜像行数为 0** 才允许热存独答。

修复的 bug:

1. **事务提交行丢失(数据正确性)**:`scan_memtables` 的水位线捷径会把热存 gather 结果**整体替换** memtable 扫描;当存在已提交但未镜像的事务行时,这些行不可见(实测 2 行表只返回 1 行)。修复:存在未镜像写时跳过捷径,回落完整 memtable 扫描。测试:`committed_transaction_disables_resident_fast_path_for_partition`。
2. **内存压力回收后查询丢行**:`reclaim_hot_store_pressure` 执行 `compact_prefix` 丢弃已落盘前缀,但不撤销 resident 标记 → resident 查询继续用截断后的热存独答。修复:回收后撤销 resident 覆盖并翻转镜像记账。测试覆盖于 fast path 套件。
3. **REPLACE 残留旧行**:`exec_replace` 走内部事务且从不镜像热存,修复前不失效热存 → resident 查询可能读到已被替换的旧行。修复:REPLACE 成功后 `invalidate_hot_store`。
4. 旁路写记账:bulk/relaxed 喂入失败、bulk 模式直写等路径全部落入 unfed 记账(可精确到分区时按分区,不可时表级哨兵),保证"热存独答 ⇒ 热存持有该分区全部行"恒成立。

验证:`cargo test --release` 全量 40 个测试目标、458+ 单测与集成测试全部通过(新增 2 个回归测试钉住事务行为与刷盘后快路径)。

## 8. 实时写入优化实录(小语句与实时小事务)

**小语句(100 行/条)**:剖析结论是 fsync 延迟地板(§3.3),代码层无值得动刀的空间;价值在用户指引——并发 + 组提交、async 模式、Linux 部署。

**实时小事务(批量 INSERT)**:路径已接近 fsync 摊薄极限,吞吐提升来自并发形态调优(§3.2 矩阵,最高 **3.3×**):

- 客户端:批次 5000→20000、并发 4→8(hot_insert 默认值已更新);
- 服务端:`max_concurrent_requests` 16→48(示例配置);
- 剩余服务端 CPU 大头是 SQL VALUES 文本解析(3.5ms/5000 行,占语句时间 ~8%)——后续可考虑二进制批量 INSERT 协议(复用 bulk payload 编码但走热存+tail 路径),预计再释放 5~15%;
- 高并发下 strict 与 async 吞吐持平(229 vs 256k 行/s),说明组提交设计已把持久化成本摊薄,**无需为吞吐牺牲 fsync 语义**。

## 9. 综合定位

| 维度 | 最强 | 说明 |
|---|---|---|
| 批量导入吞吐/压缩 | DuckDB 快 2×,taibai 压缩率优 40% | 同一梯队 |
| 实时持久化写入 | **taibai 25.6 万行/s > DuckDB 23.1 万行/s**(v2 调优后) | fsync ACK 语义更严 |
| 选择性点查/时序取数 | **taibai**(盘中 ~1ms、归档后 1.5~4ms) | 热存 + 分区/聚类有效 |
| 扫描聚合 | **DuckDB**(领先 1~2 个数量级) | taibai 执行引擎最大短板 |
| 内存时序原语 | kdb+ >> taibai(方向一致) | µs 级原语已达设计量级 |

**后续优先级**:① 字典直通解码(存储层字典编码直接产出 (ids, dict) 列,分组免字符串哈希,Q5 预计再 2×);② 二进制批量 INSERT 协议;③ ORDER BY + LIMIT 的热存反序 gather 端到端接线(H2 盘中仍走段扫);④ 聚合前全量物化改为流式消费(EXPLAIN ANALYZE 实测峰值 2.14GB)。

## 10. 扫描/聚合优化实录(v3,2026-09-13)

**根因链**(EXPLAIN ANALYZE + 分阶段计时探针定位):

1. 聚合查询的 ScanSpec 不含聚合标记 → `is_streamable` 放行 → 走 **ScanCursor** 串行逐 RG 扫描,完全绕开 ScanExecutor 的 rayon 并行;
2. 定宽列 plain 解码逐值 `Cursor::read_exact`(每 8 字节值一次带边界检查的游标读);
3. `col_sum_i64`/`col_count_non_null` 逐行做 `is_null` 位图判断(除法+移位),阻碍自动向量化;
4. `max_parallel_segments` 默认 ≤4、`global_io_permits` ≤4,段内 RG 串行。

**修复**:

| 修复 | 内容 |
|---|---|
| cursor 并行预解码窗口 | `ScanCursor::fill_prefetch`:按 `max_parallel_row_groups`(4~16)并行解码候选 RG,rayon 有序 collect 保留候选顺序,LIMIT 查询窗口缩到 4 控制超读 |
| 无空值批量解码 | `read_fixed!` 宏增加 null-free 快路径:整块 `chunks_exact + from_le_bytes`(LE 机器上等价 memcpy),否则保留逐值路径 |
| 向量化聚合核 | `col_sum_i64/col_sum_f64` 无空值时走无分支 wrapping-add fold(可自动向量化);有空值时按位图字(64 行)三态处理:全空跳过/全有效整段累加/混合逐行;`col_count_non_null` 改为 `len - popcnt(null_count)` |
| 并行默认值 | `max_parallel_segments` 4→8、`global_io_permits` 4→16;`scan_segments_parallel` 段内 RG 并行化 |

**结果**(10M 行×34 列,聚类后):Q7 110→38ms(2.9×)、Q5 836→388ms(2.2×)、Q4 181→79ms(2.3×)、Q6 28→12.7ms(2.2×);点查/单 symbol 聚合不变(本就走红存/剪枝)。结果正确性校验通过(count=10,000,000,sum 与数据分布一致)。全量测试 40 个目标全部通过。

**剩余差距**(vs DuckDB ~21×,Q5):聚合输入的 VARCHAR 列解码后仍以字符串形式参与 intern(存储字典未直通到执行层);下一阶段首选字典直通解码。

### 10.1 分组字典化快路径(v4,同日追加)

在 §10 基础上再落两层:

1. **单列 GROUP BY 快路径**(`operators/aggregate/aggregate_fast.rs`):组键 intern 为稠密 u32 id(FxHash 族),聚合参数解析为一次性类型化列视图,行循环直读切片;支持 VarChar/整型/Decimal/Timestamp 组列 × Count/Sum/Avg(非 DISTINCT),其余形态回落通用循环,spill/输出结构 1:1 兼容。
2. **字典解码零分配**:字典编码 VARCHAR 解码改为字典项借用 payload + 行展开单次 memcpy 直写列 UTF-8 arena(移除每行两次 String 克隆);plain VARCHAR 同样直写 Utf8Buffer。

**结果**:Q5 388→**229ms**(累计 3.7×)、Q4 79→**47ms**(累计 3.8×)、Q6 28→**~10ms**;组数/每组计数/avg 最值校验精确(1000 组 × 10000 行)。

### 10.2 并行分组聚合(v5,同日追加)

分组行循环是单线程的(10M 行 × ~11ns ≈ 110ms 串行)。参照 DuckDB 的并行哈希聚合:

- 输入 batch 按行切成 N 个连续区间(N = `max_parallel_row_groups`,≥8192 行才并行),每个槽位维护独立的 `FastGroups` 表,rayon 并行累加;
- 槽位表只增不缩(lanes 变小时保留既有状态,曾因 resize 收缩丢表导致少 7/8 行,已修复并加正确性校验);
- 收尾把 N 张表按"槽位序 + 各自插入序"合并(`merge_accumulator`:Count 相加、Sum 相加、Avg 累 sum/count),再走原有 spill/输出结构。

**结果**:Q5 229→**111ms**(vs 最初 836ms = **7.5×**)、Q4 47→**26ms**(7×)、Q7 38ms 持平;正确性校验精确(1000 组/200 万行/sum 10M)。全量 40 个测试目标通过。

**与 DuckDB 剩余差距**:Q5 111 vs 11ms(~10×)——集中在字典直通解码(VARCHAR 仍解码成字符串再 intern,存储字典未直通执行层)。

### 10.3 扫描解码流水线(v6,同日追加)

profiling(聚合 busy 时间 vs 墙钟)发现:并行分组落地后行循环只占 ~22ms(8 槽),**wall 的其余 ~85ms 是"窗口锯齿"**——游标预取窗口解码与聚合消费交替进行、互不重叠。

- `ScanCursor` 新增后台解码线程:候选行组 ≥4 时,工作清单交由 `taibai-cursor-decode` 线程,内部按 16 RG 分块 rayon 并行解码,经容量 64 的有界通道按序送达;消费端(LIMIT 早退/断连)自动停止生产者;
- 小扫描(<4 RG)保持内联解码,避免线程开销;点查类不受影响。

**结果**:Q7 38→**26ms**(1.5×)、Q5 111→**~112ms**(与聚合重叠后受限于聚合自身)、Q4 ~25ms、Q1 ~3.9ms;正确性校验精确。全量 40 个测试目标通过。

**累计轨迹**(vs 最初基线):Q5 836→112ms(**7.5×**)、Q4 219→25ms(**8.8×**)、Q7 110→26ms(**4.2×**)、Q1 363→3.9ms(**93×**)。与 DuckDB:Q5 剩 ~10×、Q7 剩 ~27×。

### 10.4 通道等待测量与下一步依据(v6 补充)

给解码通道两端加等待计时(consumer_wait=聚合等解码、producer_wait=解码等消费):
`batches=615 consumer_wait_ms≈97 producer_wait_ms=36~101`。

结论:Q5 的 ~110ms 中 **~97ms 是聚合在等解码**——字典展开虽已零分配,单 RG 解码成本(~0.4ms)在 16 RG 并行分块下只扩到 ~3× 有效并行。下一阶段的量化依据:
1. **字典直通解码**(ColumnArray 增加字典变体,跳过 expand + 跳过逐行字符串 intern,分组键直接消费 u32 数组)——同时削减解码成本与分组哈希成本,是剩余差距的主刀;
2. 解码线程的并行度扩不上去的原因(permit 粒度/内存带宽/rayon 任务粒度)需单独 profile。

## 附:复现命令

```bash
cargo build --release --workspace
# hotstore 微基准(默认 1M 行;HOT_BENCH_SCALE=10 → 10M 行)
HOT_BENCH_SCALE=10 cargo test --release --test hot_store_bench_test -- --ignored --nocapture
# 小语句组提交
cargo test --release --test realtime_write_scaling_test -- --ignored --nocapture
# 热存快路径回归(含本轮新增测试)
cargo test --release --test hot_store_fast_path_test
# 服务端(bench.yaml;注意 CLOSE AT 默认 15:30,盘中基准请设 --close-at=23:59)
target/release/taibai.exe --config E:/workspace/bench/bench.yaml
target/release/batch_import.exe 127.0.0.1:15151 E:/workspace/bench/ticks_10m.csv --no-header
target/release/hot_insert.exe 127.0.0.1:15151 E:/workspace/bench/ticks_10m.csv --no-header --drop --close-at=23:59 --max=2000000 --clients=8 --batch-size=20000
target/release/query_bench.exe 127.0.0.1:15151 main "<sql>" 5
python E:/workspace/bench/duckdb_bench.py load|query|appender
```

数据生成:`E:/workspace/bench/gen_ticks.py`(numpy+pandas,种子 42,可复现)。
