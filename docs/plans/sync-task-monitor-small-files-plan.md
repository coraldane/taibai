# t_sync_task_monitor 小文件堆积治理方案

## 背景：2026-10-13 生产表现

- `t_sync_task_monitor` 尾文件数从 2,387 一路涨到 **3,449**（强制刷盘阈值 256），
  全天反复触发 forced memtable flush，是当天服务端最大的刷盘噪音源。
- 成因是写入模式：监控表被"每次任务尝试都写一条状态"的用法打穿——
  t_stock_daily 同步 30s 重试一次，当天产生 532×(RUNNING+FAILED) 状态写入；
  叠加 caravan 的 UPDATE→INSERT 回退与按天 DELETE 清理，每条语句在存储层
  留下一个极小的尾文件，压缩节奏（256 个文件才合并）远远追不上。
- `t_stock_profile`（逐股 REPLACE 巨文本）也 20 次顶到 256 上限。

## 根治措施

### 1. 消灭重试风暴（已随 bulk 根因修复消失，需保持）

t_stock_daily 的 bulk 根因修复后，532 次重试×2 次状态写入自然消失。
保持原则：**任务重试不等于状态变更**，同一 (deal_date, table_name) 的重试
不该反复产生新版本行。

### 2. data-synchro：状态写入去重（建议）

任务监控只写两类点：首次进入 RUNNING、最终态（COMPLETED/FAILED）。
重试期间不更新 start_time（保留首次启动时间，另加 last_attempt_time 可选），
或对状态写入做 ≥5 分钟的节流。改动点：任务监控的 publish/update 路径。

### 3. 运维兜底（立即可做）

盘后窗口结束后（如 21:30）对监控表执行一次物理合并：

```sql
OPTIMIZE TABLE t_sync_task_monitor;
```

服务端已支持 `OPTIMIZE TABLE`（`Statement::OptimizeTable`）。可挂在
caravan 收盘后的维护任务里，与 close-of-day 调度同批。

### 4. taibai：小尾文件自动合并（P2，按需）

当前压缩以文件数量（256）为触发条件，对"极小文件海"场景不敏感。
可增加启发式：当某表尾文件数 > 64 且平均单文件 < 256KB 时，
后台自动做一次轻量合并（监控表全天数据量本身只有几 MB，合并代价可忽略）。
实现位置：`storage/core/compactor` 的调度判断；需评估与
`table-mutation-lock-hold-plan.md` 的锁预算约束（合并也要走变更锁，
必须保持单次合并短临界区）。

**2026-10-13 补充取证（静态核查结论）**：

- `t_sync_task_monitor` 的堆积物是 **durable tail generation 文件**
  （`tails/` 目录下 `<uuid>.<seq>.<kind>`），不是 segment 文件——
  `discover_segments` 看不见它们，现有 auto-compact/OPTIMIZE 均不回收。
- 当天 `forced memtable flush to bound durable tail file count` 触发 560 次，
  但 generation 计数不降反升（2,383 → 3,449）：强制刷盘只是"把内存 pending
  落成更多 tail/segment 文件"，没有任何 coalesce（合并/回收）API 存在于
  `durable_tail` 模块（仅 persist/append/clear/replay）。
- generation 按 kind 分为 tail/clear/flush/del/upd；计数器
  `flushable_generation_file_count` 只排除 clear 墓碑。哪种 kind 在净增
  （活跃 memtable 追加 vs 已刷盘 memtable 的孤儿残留）当天日志无法区分。
- **已落地观测**：`flush_tail_file_pressure` 强制刷盘后若计数仍超阈值，
  现在会输出 WARN 及 generation kind 直方图
  （`storage/core/durable_tail/api.rs::generation_kind_histogram`），
  下一个生产日即可定位净增来源，再决定 coalesce 的设计切面。

## 验收

- 盘后窗口内 t_sync_task_monitor 的 tail file_count 稳定在 256 以内；
- OPTIMIZE 后 file_count 归 1～个位数，次日增长速率下降一个数量级；
- 服务端日志不再出现该表的 forced memtable flush。
