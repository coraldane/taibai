# 行情数据实时落库改造方案

> 状态：提案（2026-09）。基于对 tonglian-md（采集）、taibai-cpp（驱动）、taibai 服务端三方代码的实证分析。
> 目标：以 kdb+ 经典 tick 路径为对标，将行情 INSERT 能力从万级 rows/s 提升到数十万级，同时移除 NATS 中间件。

## 1. 现状与核心结论

| 环节 | 现状 | 断点 |
|------|------|------|
| tonglian-md | MDL 回调 → `serialize()` 拼 CSV → `SimpleQueue<string>` → NATS 逐条发送 | 二进制行情文本化，逐条发送 |
| taibai-cpp | `bulk_insert_columnar` + pipeline 已具备，但列值为 `vector<string>` | 伪列式：数值走 int→string→int 往返 |
| taibai 服务端 | 列式消息在非 bulk 会话即走 memtable + durable tail + 热存（`flush_immediately=false`） | 载荷按文本逐值解析 |

核心结论：链路骨架已齐，改造本质是**把三段"文本惯性"替换为原生二进制列式，并把 INSERT 从"执行语句"重新定义为"提交一批列数组"**——热路径只保留 memcpy 与组提交，其余（排序、压缩、校验、索引、fsync）延迟、可选或移出临界区。

## 2. 改造方案（四个工作流）

### A. taibai 服务端（改动最小，全部向后兼容）

- **A1 列载荷二进制编码 v2**：`DirectBulkRequest` 列载荷增加 `encoding` 版本字段（1=text 现状，2=binary）。binary 模式下定宽列（Int32/Int64/Float64/Decimal/Date/Time/Timestamp）整段对齐 memcpy 进 `ColumnArray`；VarChar 走连续缓冲 + StringPool。text 路径原样保留。
- **A2 按表 ingest profile**（新 DDL 子句，全部可选，默认值=现行为）：
  - `TAIL_CODEC = RAW | LZ4 | ZSTD`（默认 ZSTD；行情表用 RAW/LZ4，压缩延迟到 seal）
  - `CLUSTER_ORDER = SORT | VERIFY`（默认 SORT；VERIFY 为 O(n) 单调校验，乱序回退排序——tick feed 天然按时间有序）
  - `CONSTRAINT_MODE = FULL | DEFERRED`（默认 FULL；DEFERRED 将唯一性校验延迟到 flush/compact，供无主键行情表使用）
  - `DURABILITY = STRICT | ASYNC`（映射现有 `tail_async_fsync`，改为按表覆盖）
- **A3 观测**：insert-profile 增加按表聚合（批大小分布、组提交组大小、各阶段耗时），复用现有 `insert_profile` 开关。

### B. taibai-cpp 驱动

- **B1 类型化列缓冲**：`Column.values: vector<string>` → 类型化 union 缓冲 + null 位图；`add_*_column` 变为整段 memcpy。协议随 A1 升级。**价格列二进制编码**：Decimal 按定宽 `int64`（unscaled 值）+ scale/precision 元数据直传，服务端 memcpy 进 DecimalArray——二进制路径下 Decimal 与 Float64 编码成本相同，精确定点无额外代价。
- **B2 连接韧性**：自动重连；重连后按 `SELECT MAX(trans_time)` 对齐逐笔缺口，快照等下一帧覆盖。
- **B3 流控**：pipeline 深度 4–8 + in-flight 行数上限，防慢消费反压采集线程。

### C. tonglian-md 采集端

- **C1 taibaiWriter（新增）**：MDL 回调直接填充驱动列缓冲；`serialize()` 仅保留给 `market_data_file` 落盘旁路。落库不再经 NATS。
- **C2 微批器**：四类数据各自独立批窗口（快照 20ms/全量批；逐笔 5ms/500 行；可配），批满或超时即 `pipeline_bulk_insert_columnar`。
- **C3 背压语义（已确认：逐笔零丢失）**：所有行情类别均不允许丢弃。缓冲设计为三层：① 大容量有界内存缓冲（按高峰 10–30 秒突发量定容）；② 溢出即同步落盘（复用 `market_data_file` 格式），③ 恢复后回放补投。水位 >60% 告警、>80% 触发"加大批窗口"的自适应降速而不是丢帧。MDL 回调线程**永不阻塞在队列上**（提交失败即走落盘旁路），优先保障行情源连接安全——断线重连的代价比丢数据小。
- **C4 NATS 移除**：`NatsWriter` 退役；消费端改为水位轮询（见 D）。文件落地保留作灾备。

### D. 消费端

- **D1 拉取模板**：Python/JDBC 各提供一个水位轮询客户端示例（维护 per-consumer `trans_time` 水位；`WHERE security_id = ? AND trans_time > ? ORDER BY trans_time LIMIT n`，或全市场无 symbol 谓词的 memtable 扫描）。
- **D2 查询规范**：当日查询命中热存（per-symbol 倒排 + 时间范围下推 + `reverse`/`row_limit` 已实现）；历史查询走列式 segment。

### 实施顺序与验收

| 里程碑 | 内容 | 验收标准 |
|--------|------|---------|
| M1 | A1 + B1（二进制列式端到端） | 同形状小批次吞吐 ×2–5；text 路径回归全绿 |
| M2 | C1–C3（微批直写）+ D1 | 100 行批 ≥100k rows/s；写入余量 ≥ 峰值 ×3 |
| M3 | A2 profile 落地 + DDL 规范 + 端到端基准 | 读写并发基准：拉取 P99 < 5ms（按 symbol），写吞吐无退化 |
| M4（可选） | 按 security_id 哈希分片 ingest worker | 写入随核数近线性扩展 |

注：本机 fsync 受安全软件干扰（30–100ms），绝对值验收须在干净硬件复测；`examples/perf_verify` 与 `tests/realtime_write_scaling_test.rs` 为现成工具。

## 3. 数据插入流程（改造后）

以深交所 L2 快照一批 1000 行为例，标注线程与成本特征：

```
[MDL 回调线程]
 1. OnMDLSZL2Message：读消息结构体（零解析，字段即内存布局）
 2. taibaiWriter::append：逐字段 memcpy 进列缓冲
    - security_id → symbol 字典查 id（首见插入，后续 4 字节）
    - 定宽字段 → 直接写类型槽位；NULL → 置位图
 3. 批满（1000 行）或窗口超时（20ms）→ 封批

[taibai-cpp 驱动]
 4. encode：列缓冲原样拷入协议帧（二进制 v2，无转换）
 5. pipeline 发送（深度 4–8），不等响应继续攒下一批

[taibai 服务端 · 连接线程]
 6. handle_bulk_insert：校验列名/类型/行数（O(列数)，非 O(行数)）
 7. 分区路由：按 trans_date 归组（批内通常单分区）
 8. ingest profile 生效：VERIFY 单调校验（O(n)）、RAW 编码、DEFERRED 约束跳过
 9. memtable 插入：每列一次 memcpy 扩容          ← 唯一"数据搬运"
10. feed 热存：per-symbol 倒排追加（O(行)）

[taibai 服务端 · 提交线程（组提交，独立于表锁）]
11. 批帧顺序写入 durable tail（页缓存），ACK 返回       ← ASYNC 模式到此为止
12. 惰性落盘：200ms / 8MB / 静默自适应阈值触发 fsync；崩溃窗口 ≤200ms/8MB
13. （后台）memtable 达阈值 → seal 为 segment（此时做 zstd + zone map + 延迟约束校验）

[消费端]
14. 水位轮询：热存 lookup_filtered（倒排 + 时间下推），µs 级；已落盘前缀经
    min_offset 水位自动去重
```

关键不变量：**步骤 1–11 全程无文本、无逐值转换、无排序、无压缩、无磁盘 IO**；批量形态（10k 行）下步骤 9–10 的 memcpy 占比进一步扩大，预期进入 1M+ rows/s 区间。

## 4. taibai 服务端功能保留清单

原则：**按数据域分档，而不是为行情阉割引擎**。行情表与账务表共存于同一引擎，各自使用不同的 profile 档位。

### 4.1 全域保留（核心资产，任何改造不动）

| 功能 | 保留理由 |
|------|---------|
| 列式存储 + row group + zone map | 查询侧的根本；kdb+ 无此查询能力 |
| durable tail（组提交 + 帧级 CRC + 代际仲裁 + 水位防重） | 崩溃恢复语义的基座，本轮已修复三个恢复缺口并有 23 项崩溃注入测试 |
| 热存（HOT BY + per-symbol 倒排 + resident coverage） | 当日查询 µs 级的原语，也是 NATS 替代方案的查询底座 |
| 分区 + 聚簇 DDL | 日期分区与 symbol 聚簇是行情查询性能的结构性前提 |
| SQL 引擎 + 向量化执行（scan/aggregate/TopN/join） | 对用户的差异化价值，热路径优化不以牺牲 SQL 为代价 |
| 后台 flush + compaction | 与插入解耦的落盘通道，调阈值即可，机制不动 |

### 4.2 按表分档保留（新增 profile 控制，机制保留、策略可调）

| 功能 | 行情表（快照/逐笔） | 账务/配置表 | 说明 |
|------|--------------------|------------|------|
| DURABILITY | ASYNC（默认）¹ | STRICT（强制） | 崩溃窗口见注¹；资金数据不可接受 |
| CONSTRAINT_MODE | DEFERRED | FULL | 行情表无业务主键；唯一性延迟到 seal 校验 |
| CLUSTER_ORDER | VERIFY | SORT | tick feed 天然时间有序 |
| TAIL_CODEC | RAW / LZ4 | ZSTD | 压缩延迟到 seal |
| 事务（BEGIN/COMMIT/ROLLBACK） | 不建议使用（逐批自动提交） | 保留 | 行情表无需多语句原子性 |
| REPLACE/UPDATE/DELETE | 不建议使用 | 保留 | 行情为追加流；修正走补偿行 |
| Prepared 语句 | 可用但非必需（列式通道更快） | 保留 | 兼容 JDBC 生态 |

> ¹ **逐笔零丢失的持久化边界（已确认决策的工程含义）**：ASYNC 模式下**进程崩溃不丢已确认数据**（页缓存由 OS 兜底，重启经 durable tail 恢复）；仅整机断电/内核崩溃存在 ≤200ms/8MB 窗口。逐笔表结合 C3 的落盘旁路回放可将该窗口也补齐（重放文件按 `trans_time` 水位去重）。若业务进一步要求电力级零丢失，逐笔表可单独设 STRICT，代价是小批次吞吐回落到组提交上限（本机慢盘实测 94–143 语句/s，干净硬件显著更高），需复测评估。

### 4.3 维持现状、暂不扩展

- MVCC/多隔离级别：当前 ReadCommitted（memtable 粒度可见性）对两类场景均够用，不扩展 RR/Serializable。
- 复制/集群：单机写入能力达标前不引入；M4 分片解决的是单机扩展性。
- 经典 WAL：已由 durable tail 取代，不再回头。

## 5. 已确认决策与剩余风险

### 已确认决策（2026-09-11）

1. **逐笔高峰零丢失**：不允许计数丢弃。背压设计改为"内存缓冲 → 溢出落盘 → 回放补投"（见 C3）；回放按 `trans_time` 水位去重。
2. **价格列 DECIMAL，两位小数**：理由——差一分钱可能导致订单无法成交，价格必须精确定点；当前数据只有股票和指数，两位小数（0.01 tick）已完备。定档与现有 tick 表 schema 对齐：**价格列 `DECIMAL(9,2)`、金额列 `DECIMAL(18,2)`**（茅台两千元、指数万点位均在 `DECIMAL(9,2)` 上限内）。二进制编码不受影响：Decimal 按定宽 `int64`（unscaled）+ scale 元数据直传，服务端 memcpy；采集端 taibaiWriter 将 MDL 价格整数（×1000 或 double）规整为 ×100 的 unscaled int64（纯整数运算，无浮点）。驱动 `add_decimal_column` 已有 int64 接口，B1 改造为其原生二进制直传。

### 剩余风险

3. **<1ms 推送刚需**：若存在此类策略消费者，轮询模型不适用，需届时单独评估专用通道。
4. **验收硬件**：W/R 两项基准必须在干净 fsync 环境复测，本机数字仅供方向参考。

## 6. 实施进度

- **2026-09-11 M1（A1 + B1）已完成**：
  - 服务端：`DirectBulkRequest` 增加 encoding 版本（v2 magic `0xFFFF` + encoding=2），变长列支持连续缓冲 + offsets 数组；新增 `column_array_from_direct_bulk` 快速构建（定宽列整段 memcpy 进 `ColumnArray`，v2 VarChar/Blob 直接落地为 `Utf8Buffer`/`BinaryBuffer`）；v1 路径原样保留。协议细节见 [Direct Bulk 列式载荷协议](../design/direct-bulk-binary-protocol.md)。
  - taibai-cpp：`RecordBatch` 改为类型化 `ColumnBuffer`（`add_*_column` 整段 memcpy、原生 null 位图、Decimal 直传 unscaled int64、新增 `add_timestamp_column`），`RecordBatch` 路径发送 v2；`vector<Column>` 兼容重载保留 v1。
  - 验收：`tests/bulk_encoding_bench_test.rs`（ignored 基准，5000 行 × 6 列 × 60 批）服务端 v1/v2 吞吐持平（≈5.6 万 rows/s）——服务端瓶颈在 zstd 压缩与每批元数据 flush（属 A2 范畴），v2 的收益主要在驱动侧消除 int→string→int 往返；lib 429 测试全绿，v1 回归全绿。
- 待做：A2（realtime profile / TAIL_CODEC / zone map 延迟）、A3（symbol 字典）、C3（落盘旁路回放）、M2–M4。
- **2026-09-11 B2 + C1 已完成**：
  - taibai-cpp：新增 `include/taibai/batch_writer.hpp` 的 `ColumnarBatchWriter<Row>`（SPSC 环形队列 + 后台聚批线程，`try_push` 非阻塞、满则计数丢弃，`close()` 排空并等待全部批次服务端确认，管线生命周期由 writer 持有）；`RecordBatchBuilder` 新增 `append_decimal`；`Client::flush()` 文档明确为"有界检查点"语义（先 drain pipeline 再发 FLUSH）。
  - tonglian-md：新增 `src/TaibaiConfig.h`、`src/TaibaiWriter.h/.cpp`（与 NatsWriter 同级的双写通道：MDL 回调线程内将记录规整为定长 POD 行——价格 llround×100 转 unscaled int64、量额 clamp int32/int64、`local_time` 解析为 epoch µs（按天缓存 mktime）——`try_push` 入队；每表一个后台聚批线程 + 独立连接，懒建表 `CREATE TABLE IF NOT EXISTS`，快照表镜像 hot_insert 的 34 列 HOT DDL）。`MessageReceiver::init` 增加 `TaibaiConfig` 参数，三个 Push*Record 入口在 `TONGLIAN_MD_TAIBAI` 宏下双写；`main.cpp` 解析 `[taibai]` 段；`conf/example.ini` 新增 `[taibai]` 配置块（默认 `enabled=false`）；`CMakeLists.txt` 新增 `TAIBAI_CPP_ROOT` 选项（add_subdirectory taibai-cpp、定义 `TONGLIAN_MD_TAIBAI`、链接 `taibai_cpp`、目标升 C++17）。
  - 验证：taibai-cpp（batch.cpp/client.cpp）MSVC /W4 编译通过；TaibaiWriter.cpp 以 spdlog 桩头在 MSVC 下编译通过；tonglian-md 整链（mdl_api/spdlog 依赖）需在 Linux 构建机回归。
  - 遗留语义：IndexL2Snapshot 暂不写 taibai；orderflow `data_type` 按原始 INT32 落库；taibai 连接失败则该写入通道整体禁用（计数丢弃、记录日志），需重启进程恢复。
