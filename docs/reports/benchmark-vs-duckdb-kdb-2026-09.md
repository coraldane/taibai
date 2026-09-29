# Taibai vs DuckDB vs kdb+ 性能对比报告(2026-09)

> 对象:taibai 0.2.x(dev 分支,本机构建)与 DuckDB 1.5.5、kdb+(公开数据,见 §6)。
> 场景:行情 tick 类时序负载 —— 批量导入、实时写入、按 symbol 点查、按日聚合。
> 性质:单机功能/性能对比。本文只记录当前版本的稳态结果;历史优化过程见 git 提交记录。

## 1. 测试环境

| 项 | 值 |
|---|---|
| CPU | Intel Core Ultra 7 270K Plus(24 核 24 线程) |
| 内存 | 47.5 GB |
| 磁盘 | 2TB NVMe SSD(JZ-SSD5.0) |
| OS | Windows 11(win32 10.0.26200) |
| **本机 fsync 延迟** | **中位 27.6ms**(4KB 写 + fsync 循环实测;Windows FlushFileBuffers 特性,生产 Linux NVMe 通常为 0.1~2ms) |
| taibai | release 构建,`tail_async_fsync=false`(默认严格 fsync),execution_threads=16 |
| DuckDB | 1.5.5(Python 嵌入模式,file 数据库) |

**数据集**:`ticks_10m.csv` —— 10,000,000 行 × 34 列(与 `examples/batch_import` 的 tick schema 一致),1000 只 symbol × 5 个交易日,CSV 2.35 GB。另取 day-1 的 200 万行作为"当日实时"数据集。每个查询重复 5~7 次取中位数,页缓存预热后测量。

**表结构**:冷表 `stock_tick_batch`(PARTITION BY deal_date,CLUSTER BY (security_id, deal_date, deal_time));热表 `hot_tick`(同上 + `HOT BY (local_time) INDEX (security_id) CLOSE AT 'HH:MM'`)。

## 2. 结论速览(TL;DR)

- **批量导入**:DuckDB 337 万行/s,taibai 165 万行/s(差 2 倍);taibai 压缩率更高(400MB vs 667MB / 千万行)。
- **实时写入(持久化)**:taibai **25.6 万行/s**(fsync ACK 语义),已超过 DuckDB appender 的 23.1 万行/s;高并发下组提交把 fsync 成本摊薄到接近免费(strict ≈ async)。
- **点查(taibai 的主场)**:盘中热存路径 **~1ms**(2000 行结果,刷盘前后一致);收盘归档后 1.5~4ms;DuckDB 无二级索引、未聚类,同查询 55ms。
- **扫描聚合(DuckDB 的主场)**:差距已收敛到 2~3 倍(全表 GROUP BY 24ms vs 11ms;过滤聚合 2.3ms vs 0.72ms);全表 count+sum 走段级/行组级部分和元数据直答,稳态 **0.23ms,反超 DuckDB**(冷首查 ~155ms 建缓存)。
- **hotstore 进程内微基准**:append 1600万~1709万行/s,symbol 最新 N 条点查 1.2~2.0µs,整键 gather 1900万~2615万行/s。
- 不同测量轮次间数字有 ±30% 级波动(段布局、缓存状态),以量级与倍数关系为准。

## 3. 插入性能

### 3.1 批量导入(10M 行 × 34 列,CSV → 持久化)

| 系统 | 路径 | 耗时 | 吞吐 | 磁盘占用 |
|---|---|---|---|---|
| taibai | `batch_import` 直连二进制 bulk 协议(客户端解析 + 网络 + 服务端 zstd 压缩直写 segment) | 6.05 s | **1,652,993 行/s** | 400 MB(10M 行) |
| DuckDB | `CREATE TABLE AS SELECT * FROM read_csv(...)` + CHECKPOINT(嵌入式,并行 CSV 解析) | 2.96 s | **3,373,802 行/s** | 667 MB(10M 行) |

### 3.2 实时写入(200 万行 × 34 列,小事务持久化)

| 形态(batch×clients) | 吞吐(严格 fsync ACK) |
|---|---|
| 5000 × 4(hot_insert 旧默认) | 76,599 行/s(冷)/ 133,027 行/s(热身) |
| 20000 × 4 | 142,741 行/s |
| **20000 × 8(新默认)+ 服务端槽位 48** | **256,449 行/s** |
| 20000 × 16 | 235,427 行/s(平台期) |
| 20000 × 8,`tail_async_fsync=true` | 229,256 行/s(≈ strict —— 组提交已把 fsync 摊薄) |
| 参照:DuckDB Appender,每 1 万行一个 COMMIT | 231,127 行/s |

要点:并发客户端 + 更大批次让每个 fsync 组携带更多行,是与 DuckDB 拉平并反超的关键;由于组提交的摊薄效应,严格持久化在该形态下几乎不再付出吞吐代价(async 不再更快)。hot_insert 示例默认值为 batch=20000 / clients=8。

### 3.3 小语句(100 行/条 INSERT)

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

| 查询 | taibai 当前 | DuckDB | 对比 |
|---|---|---|---|
| Q1 点查:单 symbol 单日 → 2000 行×34 列 | ~2.2 ms(wall ~4ms) | 55.0 ms | **taibai 领先 ~25×** |
| Q2 单 symbol 最新 10 条 | ~3.7 ms | 7.0 ms | taibai 领先 ~2× |
| Q3 单 symbol 全期聚合 | ~1.0 ms | 2.5 ms | taibai 领先 ~2.4× |
| Q4 单日 2M 行 GROUP BY | ~12.7 ms | 7.2 ms | DuckDB 领先 ~1.8× |
| Q5 全表 10M 行 GROUP BY | ~24 ms | 11.0 ms | DuckDB 领先 ~2.2× |
| Q6 单日 2M 行过滤聚合 | ~2.3 ms | 0.72 ms | DuckDB 领先 ~3.1× |
| Q7 全表 10M 行 count+sum | **~0.23 ms**(稳态) | 0.97 ms | **taibai 反超 ~4×** |

(点查/单 symbol 聚合走热存与聚簇二分;Q7 由 footer 行组级部分和元数据直答,冷首查 ~155ms 建缓存,此后每查亚毫秒。taibai wall 含 localhost TCP;DuckDB 嵌入式无网络。`OPTIMIZE TABLE` 一次性成本 2.43~3.5 s/10M 行。)

**累计轨迹(vs 项目最初基线)**:Q1 363ms→2.2ms(**~165×**)、Q2 616→3.7ms(**~165×**)、Q3 308→1.0ms(**~300×**)、Q4 219→12.7ms(**~17×**)、Q5 1.07s→24ms(**~45×**)、Q6 49→2.3ms(**~21×**)、Q7 224ms→0.23ms(**~1000×**)。

### 4.1 热表(hot_tick,200 万行驻留热存)

| 场景 | Q1 点查(symbol+date) | Q2 latest10 | Q3 symbol 聚合 |
|---|---|---|---|
| 盘中,写入进行中 | 1.3 ms | — | 66~70 ms |
| 盘中,memtable 已刷盘 | **~1.0 ms**(wall ~3ms) | 146 ms(未聚类段扫) | 130 ms |
| 收盘归档后(flush+seal+聚类压缩) | ~1.5 ms | **4.3 ms** | **0.9 ms** |

要点:热存快路径在整个交易时段保持 ~1ms 点查,不随刷盘退化;收盘归档(聚类压缩)把无 partition 谓词和 ORDER BY 类查询从 130~146ms 带到 0.9~4.3ms。

### 4.2 热存快路径的正确性边界

热存独答的成立条件是"**热存持有该分区全部行**":`MemTableManager` 按 (库, 表, 分区) 记账所有绕过热存镜像的写入(显式事务提交增量、bulk 非喂入写、relaxed/喂入失败等,可精确到分区时按分区、不可时表级哨兵);查询侧 `resident_hot_covers` 要求 resident 标记 ∧ 该分区未镜像行数为 0 才允许热存独答,否则回落完整扫描。REPLACE、内存压力回收(`compact_prefix`)等路径都会使覆盖失效并回落,保证快路径永远不会漏行或返回旧行。

## 5. 综合定位

| 维度 | 最强 | 说明 |
|---|---|---|
| 批量导入吞吐/压缩 | DuckDB 快 2×,taibai 压缩率优 40% | 同一梯队 |
| 实时持久化写入 | **taibai 25.6 万行/s > DuckDB 23.1 万行/s** | fsync ACK 语义更严 |
| 选择性点查/时序取数 | **taibai**(盘中 ~1ms、归档后 1.5~4ms) | 热存 + 分区/聚类有效 |
| 扫描聚合 | DuckDB(领先 2~3 倍) | taibai 执行引擎的主要差距,持续收敛中 |
| 内存时序原语 | kdb+ >> taibai(方向一致) | µs 级原语已达设计量级 |

## 6. kdb+ 参照(公开数据,非本机实测)

- KX 官方性能页([code.kx.com/q/kb/performance-tips](https://code.kx.com/q/kb/performance-tips)):100 万行内存表 append,单行插入 264 万行/s,1 万行批量 **3.33 亿行/s**(12 核 Mac mini,kdb+ 4.1t);`g#` 属性使 1000 万行 trade 快照查询从 84ms 降至约 0.08ms/次。
- 学术基准(arXiv:2301.12561,i7-1065G7 笔记本):kdb+ 磁盘导入 47.35 MB/s;综合排名 kdb+ >> InfluxDB >> ClickHouse >> TimescaleDB。

## 7. 当前瓶颈与后续方向

扫描/聚合侧(按投入产出排序):

1. **Q5 剩余 ~24ms 的构成**:解码侧 ~10ms(字典 ids_full 展开 O(n)、zstd 解压、`read_column_raw` 的 into_owned 拷贝)+ 聚合 ~13ms(616 批次 × ~15µs 固定开销为主,run 交叉聚合本身已近零)。方向:① 字典解码侧顺带产出 id-runs(消费端 ids 比较消失,ids_full 懒物化);② 批次合并(616→154)摊薄固定开销;③ 消除 `read_column_raw` 拷贝。
2. **聚合流式消费**:聚合输入的全量物化改为每批即算即弃(流式聚合),与后台解码管线全程重叠;`EXPLAIN ANALYZE` 曾实测聚合窗口峰值内存 2.14GB。
3. **footer 瘦身**:Q7 冷首查 ~155ms 的剩余成本是 footer 自身解析(value_sets 字符串解码);Q4 的剩余差距同样与段布局/元数据解析相关。
4. **二进制批量 INSERT 协议**:服务端剩余 CPU 大头是 SQL VALUES 文本解析(3.5ms/5000 行,占语句时间 ~8%),复用 bulk payload 编码走热存+tail 路径,预计释放 5~15%。
5. **盘中 ORDER BY + LIMIT**:热存反序 gather 的端到端接线(当前 H2 盘中仍走段扫)。

主线路线(向量化执行 → Parquet/Arrow 生态 → 时序金融深化)见
[三步走路线图](../plans/vectorization-parquet-timeseries-roadmap.md);实时摄取侧见
[行情数据实时落库改造方案](../plans/realtime-market-data-ingestion-plan.md)。

## 8. 复现命令

```bash
cargo build --release --workspace
# hotstore 微基准(默认 1M 行;HOT_BENCH_SCALE=10 → 10M 行)
HOT_BENCH_SCALE=10 cargo test --release --test hot_store_bench_test -- --ignored --nocapture
# 小语句组提交
cargo test --release --test realtime_write_scaling_test -- --ignored --nocapture
# 热存快路径回归
cargo test --release --test hot_store_fast_path_test
# 服务端(bench.yaml;注意 CLOSE AT 默认 15:30,盘中基准请设 --close-at=23:59)
target/release/taibai.exe --config E:/workspace/bench/bench.yaml
target/release/batch_import.exe 127.0.0.1:15151 E:/workspace/bench/ticks_10m.csv --no-header
target/release/hot_insert.exe 127.0.0.1:15151 E:/workspace/bench/ticks_10m.csv --no-header --drop --close-at=23:59 --max=2000000 --clients=8 --batch-size=20000
target/release/query_bench.exe 127.0.0.1:15151 main "<sql>" 5
python E:/workspace/bench/duckdb_bench.py load|query|appender
```

数据生成:`E:/workspace/bench/gen_ticks.py`(numpy+pandas,种子 42,可复现)。

## 11. 批量导入端到端优化实录(2026-09-29)

> 场景与 §3.1 相同(10M 行 × 34 列 CSV → 持久化)。本轮把测量口径修正为**端到端**(客户端发送 + 服务端 drain + STOP 刷盘全部完成),原 §3.1 的 6.05s/165 万行/s 止于最后一条 payload 写入 socket,漏计了约 1.9s 的服务端尾巴——按同一口径,优化前基线是 **8.64s / 116 万行/s**。

### 11.1 基线画像(log.insert_profile 分阶段探针)

| 阶段 | 每 200k 行请求 |
|---|---|
| 客户端 CSV 解析+编码(单线程) | 5.2s / 总 6.78s(第一瓶颈) |
| 服务端 write=partition+prepare+append | 92ms(其中 select 逐值拷贝 16.7ms、zstd 16.8ms、行组写入+元数据 36.8ms、分区路由 13.9ms) |

### 11.2 落地的优化

| 项 | 内容 | 效果 |
|---|---|---|
| 客户端并行解析 | 单 reader 按行边界切 64MB 段 → 8 parser 线程解析+编码+发送,段完成哨兵恢复严格文件序 | 客户端 CPU 5.2s → 墙钟 ~0.9s |
| reader 线性化 | 行界扫描只扫新块(原每 8MB 块全量重扫 pending,近似平方) | reader ~1s |
| 服务端零拷贝行组 | Borrowed 路径从"整请求"泛化为"连续行 run":定宽列指针子切片、varchar arena 重基切片、NULL 位图按 u64 word 对齐平移;Owned 逐值 gather 路径整体删除 | select 16.7ms → 0 |
| 分区路由均匀快路径 | 分区列原始槽位全等(memcmp)时一次键计算替代逐行 scalar+format! | partition 13.9ms → 1.3ms |
| col_sums 特化+并行 | 无 NULL 定宽列按宽度类型化 wrapping 求和(unaligned load),34 列 rayon 并行 | append 内部求和减半 |
| 定宽 zone map 特化 | 逐行 enum 匹配 → 类型化 min/max | append 36.8ms → 8-14ms |
| bulk 消息独立上限 | `server.max_bulk_message_size`(默认 128MB),bulk 0x23 不再受 64MB 通用上限约束 | 为大批次留空间 |

**结果(同机同数据,端到端口径):4.0-4.4s(228-248 万行/s),对比修正后基线 2.0-2.1x。** 服务端 per-request 92ms → 36ms。

### 11.3 过程中发现并修复的正确性问题

1. **零拷贝 varchar arena(自引入自修复)**:变长列 piece 最初把整个请求 arena 传给编码器,无 NULL 快路径逐字追加,导致每个行组重放第一片的值——partition_cluster_test 的 NULL/乱序用例抓住,round-trip 测试钉死。
2. **NULL 位图 word 对齐**:切片位图按字节粒度生成,而 `NullBitmap::from_bytes` 按 u64 word 拷贝(短尾被丢弃);已按格式不变量改为 word 对齐+零尾位。
3. **多轮导入 SUM=NULL(预先存在,本轮修复)**:bulk 直写只给键列算 zone map,非键列是默认值(min=NULL);SUM 元数据路由把"未计算"当"全 NULL 行组"跳过,多轮导入后 COUNT 正常而 SUM/AVG 返回 NULL。修复:SUM/AVG 参数列的 zone map 未计算(或真全空)时该行组走数据折叠;`tests/direct_bulk_multi_session_test.rs` 钉住(三轮导入后 SUM/AVG 精确)。

### 11.4 剩余差距与下一步

- 端到端 4.0-4.4s vs DuckDB 2.96s(337 万行/s):客户端单 socket 写(~800MB/s 回环)与串行 per-connection 处理是剩余约束。已验证无效的方向:双连接(客户端单 consumer 交替写,无收益且不稳)、500k 大批次(段对齐破坏批次结构,反而变慢)、线上 zstd-1(回环上解压成本超过传输收益;真实网络场景仍值得做,服务端入口已支持 0x28B52FFD magic 检测解压)。
- 下一步优先级:① 双 consumer 各持一个连接(重叠 socket 写与服务端处理);② 线上压缩采用定长前缀精确分配解压缓冲;③ bulk 编码 v2(连续 varchar 缓冲 + offsets,协议文档已定义,服务端解析待实现)。
- 全量回归 617+ 用例通过;新增 `direct_bulk_multi_session_test`(多轮导入 SUM/AVG 精确)与 partition_cluster 的 NULL/乱序用例共同钉住本轮改动。
