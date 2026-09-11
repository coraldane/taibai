# Taibai 金融行情场景验证与测试清单

## 1. 目标与范围

本文档用于验证 Taibai 是否可以支撑金融行情数据的生产使用。第一阶段聚焦两个明确场景：

1. 实盘接收 tick 数据或逐笔成交数据，并提供低延迟查询与第三方订阅。
2. 存储每日盘后的股票日数据和分钟数据，支持 10 年历史数据的高速检索。

验证目标不是证明 Taibai 是通用数据库，而是证明它在金融行情数据的写入、存储、查询、订阅、恢复和运维场景下具备可上线的边界。

## 2. 上线准入标准

| 类别 | 准入要求 | 状态 |
|------|----------|------|
| 数据正确性 | 导入、查询、索引、分区、聚合结果可与基准系统一致 | 待验证 |
| 实时写入 | tick/逐笔数据持续写入时无丢失、无乱序不可恢复、无重复不可识别 | 待验证 |
| 历史查询 | 单股票最近 60 个交易日分钟线加载时间小于 1 秒 | 待验证 |
| 实时查询 | 单股票单交易日 tick 查询首包返回小于 10ms | 待验证 |
| 订阅能力 | 第三方订阅低延迟推送，支持断线续传或可验证重放 | 待验证 |
| 崩溃恢复 | 进程崩溃、机器重启、写入中断后数据可恢复或损失边界明确 | 待验证 |
| 长稳运行 | 连续写入、查询、订阅、压缩、备份 7 天无严重退化 | 待验证 |
| 备份恢复 | 全量备份、增量备份、指定日期恢复演练通过 | 待验证 |
| 升级回滚 | 数据格式、协议、索引兼容性验证通过 | 待验证 |
| 可观测性 | 关键指标、日志、慢查询、错误原因、恢复过程可观测 | 待验证 |
| Runbook | 常见故障有明确处理步骤 | 待验证 |

## 3. 数据模型与基准数据集

### 3.1 实时 tick/逐笔成交表

推荐验证表：

```sql
CREATE TABLE ticks (
    symbol VARCHAR,
    exchange VARCHAR,
    trading_day INT,
    event_time TIMESTAMP,
    sequence BIGINT,
    price DECIMAL,
    volume BIGINT,
    amount DECIMAL,
    bid_price_1 DECIMAL,
    bid_volume_1 BIGINT,
    ask_price_1 DECIMAL,
    ask_volume_1 BIGINT,
    trade_id VARCHAR
) CLUSTER BY (symbol, event_time, sequence)
  PARTITION BY (trading_day);
```

必须验证的逻辑主键或去重键：

```text
symbol + trading_day + sequence
```

### 3.2 分钟线历史表

推荐验证表：

```sql
CREATE TABLE minute_bars (
    symbol VARCHAR,
    trading_day INT,
    bar_time TIMESTAMP,
    open DECIMAL,
    high DECIMAL,
    low DECIMAL,
    close DECIMAL,
    volume BIGINT,
    amount DECIMAL,
    vwap DECIMAL
) CLUSTER BY (symbol, bar_time)
  PARTITION BY (year(bar_time));
```

### 3.3 日线历史表

推荐验证表：

```sql
CREATE TABLE daily_bars (
    symbol VARCHAR,
    trading_day INT,
    open DECIMAL,
    high DECIMAL,
    low DECIMAL,
    close DECIMAL,
    volume BIGINT,
    amount DECIMAL,
    adj_factor DECIMAL
) CLUSTER BY (symbol, trading_day)
  PARTITION BY (year(trading_day));
```

### 3.4 基准数据规模

| 数据集 | 规模 | 用途 |
|--------|------|------|
| Mini | 10 个股票，5 个交易日 | 本地开发、CI、正确性回归 |
| Small | 500 个股票，60 个交易日 | 查询路径、索引、分区剪枝验证 |
| Full-day | 全市场 1 个交易日 tick/分钟线 | 单日写入、查询、订阅验证 |
| Full-history | 分钟线约 124 万行/日，250 交易日/年，10 年约 31 亿行 | 历史库上线压测 |
| Replay | 真实或模拟实盘 tick 流 | 实时写入、订阅、聚合、故障恢复 |

数据集必须附带校验信息：

- 每个 symbol 的行数。
- 每个交易日的行数。
- 每列 min/max/null count。
- 全表 checksum。
- 每个分区 checksum。
- 可重复生成的随机种子或原始数据版本号。

## 4. 场景一：实时 tick/逐笔数据验证

### 4.1 实时写入

| 测试项 | 验证方法 | 验收标准 |
|--------|----------|----------|
| 单连接持续写入 | 单客户端按交易时序写入 tick | 无错误、无丢行、吞吐达到目标 |
| 多连接并发写入 | 多客户端按不同 symbol 或交易所写入 | 无死锁、无重复提交异常 |
| 高峰写入 | 模拟开盘、收盘、极端行情突发流量 | P99 写入延迟不超过目标 |
| 批量写入 | 使用批量导入协议写入当日数据 | 吞吐稳定，segment 文件可读 |
| 小批量写入 | 逐条或小批次写入 | 延迟可控，tail 不异常膨胀 |
| 乱序写入 | 部分 tick 延迟到达 | 查询结果按 event_time/sequence 正确排序 |
| 重复写入 | 重放同一批 tick | 可去重或重复边界明确 |
| 跨交易日写入 | 交易日切换时持续写入 | 正确进入新分区 |

### 4.2 实时查询

| 查询 | 验收标准 |
|------|----------|
| 单 symbol 单 trading_day tick 查询 | 首包 P95 < 10ms，完整返回吞吐达到目标 |
| 单 symbol 最近 N 分钟 tick 查询 | P95 < 10ms 到 50ms，取决于返回量 |
| 单 symbol 指定时间区间查询 | 正确命中分区和排序索引 |
| 多 symbol 同一时间区间查询 | P95/P99 随 symbol 数量可解释增长 |
| 全市场某一分钟截面查询 | 走时间过滤或专用索引，延迟和扫描量可观测 |
| 查询不存在的 symbol | 快速返回空结果，不扫描无关分区 |
| 查询非交易日 | 快速返回空结果 |

需要同时记录：

- 首包时间。
- 完整结果返回时间。
- 返回行数。
- 扫描行数。
- 扫描 segment 数。
- 分区剪枝数。
- 解压耗时。
- 序列化耗时。
- 网络发送耗时。

### 4.3 订阅与回放

| 测试项 | 验证方法 | 验收标准 |
|--------|----------|----------|
| 单订阅者实时推送 | 订阅指定 symbol 列表 | P95 推送延迟达到目标 |
| 多订阅者实时推送 | 10/100/1000 个订阅者并发 | 无明显阻塞写入路径 |
| 全市场订阅 | 订阅所有 symbol | 吞吐达到目标，慢消费者隔离 |
| 断线重连 | 消费者断线 30 秒后恢复 | 可从 last_sequence 续传 |
| 消费者落后 | 消费者处理速度低于写入速度 | 背压、丢弃或落盘策略明确 |
| 重复推送 | 服务端重试或客户端重连 | 下游可通过 sequence 去重 |
| 权限隔离 | 不同用户订阅不同 symbol 集合 | 无越权数据 |
| 订阅过滤 | symbol、exchange、trading_day、time range 过滤 | 过滤结果正确 |

推荐订阅语义：

```text
subscribe(symbols, from_sequence)
ack(sequence)
resume(last_ack_sequence)
```

### 4.4 tick 聚合分钟线

| 测试项 | 验证方法 | 验收标准 |
|--------|----------|----------|
| 实时聚合 | 订阅 tick 后在第三方应用聚合分钟线 | 与 Taibai 回放聚合结果一致 |
| 盘后批处理聚合 | 从当日 tick 批量生成分钟线 | 与实时聚合结果一致 |
| 乱序 tick 聚合 | 延迟 tick 进入已完成分钟窗口 | 修正策略明确，可重算 |
| 缺失 tick | 故意丢弃部分 tick | 校验能发现差异 |
| 重复 tick | 重放重复 tick | 聚合结果可去重或差异可解释 |
| 停牌/无成交分钟 | 无 tick 的分钟窗口 | 生成规则符合业务要求 |

## 5. 场景二：历史日线/分钟线验证

### 5.1 导入与存储

| 测试项 | 验证方法 | 验收标准 |
|--------|----------|----------|
| 单日分钟线导入 | 导入约 124 万行 | 行数、checksum、min/max 正确 |
| 10 年分钟线导入 | 导入约 31 亿行等价数据 | 无损坏，分区和索引可用 |
| 按年分区 | 验证 year 分区目录和元数据 | 跨年查询只扫描必要分区 |
| 按 symbol,time 聚簇 | 验证物理排序或稀疏索引 | 单股票查询不扫描大量无关股票 |
| 重复导入 | 同一天重复导入 | 可替换、幂等或拒绝策略明确 |
| 增量补数据 | 补充历史某一天数据 | 元数据、索引、checksum 更新正确 |
| 修正数据 | 替换某 symbol 某日分钟线 | 旧数据不可见，新数据可查 |
| 压缩率 | 统计原始数据、tail、segment、索引大小 | 符合容量规划 |

### 5.2 核心查询

| 查询 | 数据量估算 | 验收标准 |
|------|------------|----------|
| 单股票最近 60 个交易日分钟线 | 约 14,400 行 | warm cache P95 < 200ms，cold cache P95 < 1s |
| 单股票最近 1 年分钟线 | 约 60,000 行 | P95 < 1s |
| 单股票最近 10 年分钟线 | 约 600,000 行 | P95 可解释，流式返回稳定 |
| 单股票某一年分钟线 | 约 60,000 行 | 只扫描对应年份分区 |
| 多股票最近 60 天分钟线 | symbol 数量可变 | 延迟随 symbol 数量线性或次线性增长 |
| 全市场某一天分钟线 | 约 124 万行 | 流式吞吐达到目标 |
| 全市场某一分钟截面 | 约全市场股票数 | 查询路径明确，不全表扫 10 年 |
| 日线最近 10 年 | 每股票约 2,500 行 | P95 < 100ms 到 500ms |

### 5.3 查询正确性

| 测试项 | 验证方法 | 验收标准 |
|--------|----------|----------|
| 与 CSV/Parquet 基准比对 | 随机抽样 symbol/time range | 结果完全一致 |
| 与 PostgreSQL/ClickHouse 比对 | 同一数据集执行同一查询 | 行数、排序、数值一致 |
| 聚合查询 | open/high/low/close/volume/amount | 聚合结果一致 |
| 排序查询 | ORDER BY symbol,time | 稳定有序 |
| LIMIT 查询 | 小 limit、大 offset | 结果正确，limit pushdown 生效 |
| 分区边界查询 | 年初、年末、跨年 60 天 | 不漏行、不重复 |
| 非交易日查询 | 周末、节假日 | 返回符合业务日历规则 |
| Decimal 精度 | 价格、金额、复权因子 | 无精度丢失 |
| 时间类型 | 时区、毫秒/微秒精度 | 无偏移、无截断 |

## 6. 数据库基础能力验证

### 6.1 SQL 与执行引擎

- CREATE DATABASE、DROP DATABASE、SHOW DATABASES。
- CREATE TABLE、DROP TABLE、TRUNCATE TABLE。
- ALTER TABLE ADD/DROP/RENAME/MODIFY COLUMN。
- INSERT、INSERT_BATCH、批量导入。
- SELECT 基础投影、过滤、排序、LIMIT。
- BETWEEN、IN、LIKE、IS NULL、IS NOT NULL。
- COUNT、SUM、AVG、MIN、MAX。
- GROUP BY、HAVING。
- DISTINCT。
- JOIN、LEFT JOIN、SEMI JOIN。
- 子查询、EXISTS、IN subquery。
- 窗口函数。
- CASE 表达式。
- EXPLAIN 输出是否展示分区剪枝、列裁剪、索引选择、limit pushdown。

### 6.2 优化器

| 能力 | 验证点 |
|------|--------|
| 分区剪枝 | trading_day/year 条件只扫描目标分区 |
| zone map pruning | time range、price range 能跳过 row group |
| 稀疏索引 | symbol,time 定位准确 |
| bloom filter | symbol 等高选择性过滤有效 |
| 列裁剪 | 查询少量列时不读取无关列 |
| 谓词下推 | WHERE 条件尽早进入 scan |
| LIMIT 下推 | 小结果查询减少扫描 |
| ORDER BY/LIMIT 优化 | TopN 查询不做全量排序 |
| 聚合下推 | 可下推聚合减少数据搬运 |

### 6.3 事务与并发

- BEGIN、COMMIT、ROLLBACK 基本语义。
- 未提交数据对其他会话不可见。
- 已提交数据崩溃恢复后可见。
- 并发 INSERT 不丢行。
- 并发 SELECT 与 INSERT 互不阻塞或阻塞边界明确。
- 并发 UPDATE/DELETE 与 SELECT 可见性正确。
- 长事务不会无限阻塞 compaction 或 tail 清理。
- 事务 ID、快照、MVCC 版本回收正确。
- 死锁或锁等待有超时和日志。

### 6.4 UPDATE、DELETE 与修正数据

- 单行 DELETE。
- 范围 DELETE。
- 按 symbol/trading_day 删除。
- DELETE 后 count 和查询结果正确。
- UPDATE 单列、多列。
- UPDATE 主排序列或分区列时行为明确。
- delete bitmap 与 segment reader 结果一致。
- compaction 后删除数据不再可见。
- 修正某股票某日历史数据后，旧数据不可被查询路径读出。

### 6.5 约束与权限

- NOT NULL、PRIMARY KEY、UNIQUE 或等价约束行为明确。
- 重复 tick sequence 的处理策略明确。
- 用户认证。
- 数据库级、表级、列级权限。
- 订阅权限。
- TLS 连接。
- 错误信息不泄露敏感路径、密码、token。

## 7. 存储、持久化 tail 与恢复验证

### 7.1 持久化 tail (durable tail)

经典 WAL 已移除，持久化由 `memtable_tail/` 尾部日志与 active segment 自身承担。

| 测试项 | 验证方法 | 验收标准 |
|--------|----------|----------|
| tail 追加 | 写入后检查 tail 帧 | CRC、长度、行数索引正确 |
| tail replay | 停库后重启 | 已提交数据恢复 |
| 未提交事务 | crash 前未 commit | 重启后不可见 |
| 截断 tail | 人为截断最后一帧 | 可安全忽略损坏尾部 |
| tail 膨胀 | 长时间写入 | flush/seal 后及时清理，膨胀可告警 |
| tail 压缩 | zstd 压缩帧 | replay 正确，压缩率可观测 |
| 重复回放 | crash 于物化后、tail 清理前 | 行数 watermark 防止重复计入 |
| fsync 策略 | sync/no-sync/bulk 模式 | 数据损失边界明确 |

### 7.2 Segment

- segment header/footer 魔数、版本、schema 校验。
- 写入中崩溃后不读取半成品 segment。
- seal_segment 后数据持久化。
- segment 超过大小阈值后自动切换。
- segment checker 能发现损坏文件。
- column chunk checksum 校验。
- row group 元数据正确。
- delete bitmap 持久化正确。
- segment 文件移动、备份、恢复后可读。

### 7.3 Crash Recovery

| 场景 | 验证方法 | 验收标准 |
|------|----------|----------|
| INSERT 前崩溃 | kill -9 before write | 无半条数据 |
| tail 落盘后物化前崩溃 | 注入崩溃点 | replay 后数据可见 |
| segment 写入中崩溃 | 注入崩溃点 | 不读取损坏 segment，可恢复 |
| commit 前崩溃 | 注入崩溃点 | 事务不可见 |
| commit 后崩溃 | 注入崩溃点 | 事务可见 |
| compaction 中崩溃 | 注入崩溃点 | 旧 segment 或新 segment 至少一方可用 |
| schema 变更中崩溃 | ALTER TABLE 时 kill | 元数据一致 |
| 索引构建中崩溃 | CREATE INDEX 时 kill | 可回滚或继续构建 |

## 8. 性能测试清单

### 8.1 写入性能

| 测试 | 指标 |
|------|------|
| SQL INSERT 单行 | rows/s、P95/P99 latency、tail size |
| SQL INSERT 多行 | batch size 与吞吐曲线 |
| INSERT_BATCH | rows/s、CPU、内存、磁盘写入 |
| bulk import columnar | rows/s、压缩率、segment size |
| 实时 tick 流 | 写入延迟、订阅延迟、落盘延迟 |
| 并发写入 | 连接数、锁等待、吞吐扩展性 |

### 8.2 查询性能

每个查询都必须分别记录 cold cache 和 warm cache：

- P50/P95/P99 延迟。
- 首包时间。
- 完整返回时间。
- 结果行数。
- 扫描行数。
- 扫描字节数。
- 解压耗时。
- 执行算子耗时。
- 序列化耗时。
- 网络发送耗时。
- CPU、内存、磁盘 IO。

### 8.3 长稳测试

| 时长 | 内容 | 通过标准 |
|------|------|----------|
| 1 小时 | 开发 smoke soak | 无 panic、无明显泄漏 |
| 24 小时 | 写入 + 查询 + 订阅 | 错误率低于目标 |
| 72 小时 | 高压力混合负载 | P99 无持续恶化 |
| 7 天 | 生产模拟 | 数据正确、容量可控、可恢复 |

长稳测试必须覆盖：

- 持续 tick 写入。
- 历史分钟线查询。
- 第三方订阅。
- 定时 flush。
- segment seal。
- compaction。
- tail 清理。
- 备份。
- 随机 kill/restart。
- 定期 checksum 校验。

## 9. 故障注入清单

| 故障 | 验证目标 |
|------|----------|
| kill -9 服务进程 | tail/segment 恢复正确 |
| 机器重启 | 数据目录可恢复 |
| 网络断开 | 客户端重连、订阅恢复 |
| 网络延迟 | 写入和订阅延迟可观测 |
| 磁盘满 | 写入失败明确，不损坏已有数据 |
| 磁盘慢 | P99 延迟和背压策略可观测 |
| 文件损坏 | segment checker 能发现并隔离 |
| tail 损坏 | replay 可停止在安全位置 |
| schema 文件损坏 | 有备份或明确恢复方式 |
| 时钟跳变 | event_time 不受系统时间异常影响 |
| 内存压力 | 查询失败可控，不 OOM 崩溃 |
| 慢消费者 | 不拖垮写入路径 |
| 大查询 | 可取消、可限流、可观测 |

## 10. 可运维性验证

### 10.1 指标

必须暴露或可采集：

- 写入 QPS、rows/s、bytes/s。
- 查询 QPS。
- 查询 P50/P95/P99。
- tick 订阅推送延迟。
- 订阅 backlog。
- tail 文件数量和大小。
- segment 数量、大小、row group 数。
- compaction 次数、耗时、输入/输出大小。
- 分区数量和大小。
- cache hit ratio。
- 磁盘读写吞吐。
- 错误数和错误类型。
- 当前活跃连接数。
- 慢查询列表。

### 10.2 日志

- 启动配置。
- 数据目录和版本。
- schema 加载。
- tail replay 进度。
- segment seal。
- compaction 开始和结束。
- 查询计划和慢查询。
- 订阅连接、断线、续传。
- 权限失败。
- 磁盘、IO、checksum 错误。

### 10.3 备份与恢复

- 全量备份。
- 增量备份。
- 按 trading_day 恢复。
- 按 symbol 恢复或重建。
- 备份过程中持续写入。
- 从备份恢复到新实例。
- 恢复后 checksum 与源实例一致。
- 备份文件版本兼容。

### 10.4 升级与回滚

- 小版本升级。
- 数据格式升级。
- 协议兼容。
- 索引格式兼容。
- 升级中断恢复。
- 回滚到上一版本。
- 新版本写入数据后旧版本能否读取，边界必须明确。

## 11. 对比验证

建议选择至少一个成熟系统作为 reference：

- PostgreSQL：语义正确性和事务对照。
- ClickHouse：列式历史行情查询性能对照。
- DuckDB/Parquet：单机分析和文件格式对照。
- Kafka/Redpanda：订阅、offset、重放语义对照。

对比内容：

- 同数据集导入行数。
- 同查询结果集。
- 排序一致性。
- Decimal 精度。
- 聚合结果。
- 分区裁剪效果。
- 查询延迟。
- 存储大小。
- 故障恢复行为。

## 12. CI 与自动化测试分层

| 层级 | 数据规模 | 触发方式 | 内容 |
|------|----------|----------|------|
| Unit | 极小 | 每次提交 | parser、types、codec、tail、segment、optimizer |
| Integration | Mini | 每次提交 | DDL、DML、查询、事务、权限 |
| Correctness | Small | 每日 | 与 reference 系统比对 |
| Performance Smoke | Small | 每日 | 核心查询和写入性能趋势 |
| Full Benchmark | Full-day/Full-history | 每周或版本发布 | 历史库、tick、订阅、恢复 |
| Soak | Replay | 发布前 | 72 小时到 7 天长稳 |

## 13. 第一阶段建议验收矩阵

第一阶段建议先以历史分钟线场景作为主线，实时 tick 作为第二主线。

| 优先级 | 验收项 | 目标 |
|--------|--------|------|
| P0 | 10 年分钟线等价数据导入 | 完整导入，无损坏 |
| P0 | 单股票最近 60 交易日分钟线查询 | cold cache P95 < 1s |
| P0 | 分区剪枝 | 跨年查询只扫必要年份 |
| P0 | symbol,time 聚簇查询 | 不扫描大量无关 symbol |
| P0 | 与 reference 查询结果一致 | 行数、排序、数值一致 |
| P0 | crash recovery | 已提交数据恢复，未提交数据不可见 |
| P0 | checksum 工具 | 可验证分区、segment、查询结果 |
| P1 | 实时 tick 持续写入 | 吞吐和延迟达到目标 |
| P1 | 单 symbol 当日 tick 查询 | 首包 P95 < 10ms |
| P1 | 第三方订阅 | 支持 sequence、ack、resume |
| P1 | tick 聚合分钟线 | 与盘后批处理结果一致 |
| P1 | 24 小时混合长稳 | 无严重退化 |
| P2 | 7 天生产模拟 | 发布前准入 |
| P2 | 全量/增量备份恢复 | 恢复后 checksum 一致 |
| P2 | 升级回滚演练 | 版本边界明确 |

## 14. 测试产物要求

每次完整验证应产出：

- 测试数据版本。
- Taibai commit hash。
- 配置文件。
- 硬件和操作系统信息。
- 导入日志。
- 查询 benchmark 报告。
- 正确性比对报告。
- crash recovery 报告。
- 长稳测试报告。
- 存储容量报告。
- 已知问题和限制。
- 是否满足上线准入的结论。

## 15. 已知需要尽早明确的问题

- “一天 tick 数据 10ms 内完成”是指首包返回、完整结果传输，还是只查单个 symbol。
- tick/逐笔数据的目标峰值写入速率。
- 第三方订阅是否要求 exactly-once，还是 at-least-once 加 sequence 去重。
- 历史分钟线是否需要复权、多价格版本、多数据源版本。
- 修正历史数据时是覆盖、追加版本，还是保留审计历史。
- 交易日历和时区由 Taibai 内置，还是由上层应用传入。
- 是否需要跨市场、跨交易所统一 symbol 编码。
- 是否需要冷热分层存储。
- 是否要求单机上线，还是第一阶段就需要集群。
