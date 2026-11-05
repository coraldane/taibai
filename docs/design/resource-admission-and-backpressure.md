# 资源准入与背压：消除查询死锁的结构性方案

状态：提案（待评审）
适用版本：v0.2.8
现场：hz-dev-204，2026-11-05

---

## 0. 结论

`query_limits` 中的 `global_cpu_permits` / `global_io_permits` 是一对**进程级阻塞信号量**，
在六处扫描路径上被**从 rayon worker 线程内部**获取。这把"并发度调节"实现成了
"不可中断的互斥等待"，同时满足了死锁的三个必要条件。

问题的性质不是"许可给少了"，而是：

> 用互斥原语表达调度意图，并且在不能阻塞的执行上下文里阻塞。

本文的处置原则只有一条：

> **有界资源只在一个地方被决策，决策完成后执行路径上不再出现第二次等待。**

---

## 1. 现场证据

### 1.1 现象

| 观测项 | 结果 |
| --- | --- |
| `SHOW PROCESSLIST` | 8 个查询挂起 620s，其中 2 个状态 `Killed` 但不退出 |
| 线程上下文切换 | 20s 内主动切换 0 次 |
| `taibai-exec-*` | 64 个线程全部空闲 |
| 客户端 | 全部 `request queue wait timeout`（30s 排队后判过载） |
| 查询日志 | 15:14:28 后无任何完成记录 |
| 重启节奏 | 22:29 / 22:43 / 22:45 / 23:10，均为人为 restart |

### 1.2 线程栈分布

| 阻塞函数 | 线程数 |
| --- | --- |
| `work_limiter::Semaphore::acquire`（Condvar） | 10 |
| `rayon::LockLatch::wait_and_reset` | 6 |

### 1.3 许可余量（读进程内存）

| 信号量 | 容量 | 空闲 |
| --- | --- | --- |
| CPU | 64 | 38 |
| IO | 16 | **0** |

同时，遍历全部线程栈**未发现任何线程停留在 `read_columns_filtered` / `SegmentReader` 等
实际解码帧内**——即 16 个 IO 许可被持有者握住，但持有者并不在做受保护的读工作。
许可被"持有并等待"了。

---

## 2. 为什么这是结构性问题

### 2.1 读路径没有磁盘请求队列

`SegmentReader.data` 是 `SegmentData`，生产环境走 `SegmentData::Mmap`
（`src/storage/segment/reader.rs:193`）。`read_columns_filtered`
（`reader.rs:940`）中只有 `self.data.as_slice()` 的切片与解压，没有 `read(2)`。

因此 `acquire_io()` **既观察不到 IO、也无法给 IO 排序**。它实际约束的是
"同时进行页命中 + 解压的线程数"，这与 `global_cpu_permits` 是同一件事。

### 2.2 解码路径已经有两层全局池

| 路径 | 执行池 | 全局线程上限 |
| --- | --- | --- |
| inline 扫描（`executor.rs:378/630`） | rayon 全局池 | 64 |
| 后台解码（`cursor.rs:496`） | `background_decode_pool()` | 8（`(nproc/2).clamp(2,8)`）|

两个池都是 `static` 全局共享的。也就是说**并发解码线程数本来就已经被池大小全局限定了**。
再加一个全局计数信号量，是在同一维度上表达第二遍约束；而第二遍约束的获取点位于池内，
于是把一个调度问题变成了阻塞问题。

### 2.3 六个获取点全部在 rayon 闭包内

| 位置 | 上下文 |
| --- | --- |
| `executor.rs:339-340` | `.par_iter()` 闭包（读行组） |
| `executor.rs:583` | `.par_iter()` 闭包（打开段 reader） |
| `executor.rs:639-640` | `.into_par_iter()` 闭包（读行组） |
| `cursor.rs:325-326` | `.into_par_iter()` 闭包（预取） |
| `cursor.rs:500-501` | `install(|| .par_iter())` 闭包（后台解码） |
| `compactor.rs:387-388/532-533` | 维护线程（**不在** rayon worker，安全） |

rayon 的核心不变式是：**worker 线程不得阻塞在非 rayon 原语上**，否则工作窃取失效、
依赖该 worker 的子任务无法推进。`Semaphore::acquire` 用的是裸 `Condvar::wait`
（`work_limiter.rs:18-20`），既阻塞 worker，又：

- 无超时；
- 不检查 `QueryContext` 取消位。

这直接解释了 `KILL QUERY` 无效（`Killed` 状态卡 10 分钟）。

### 2.4 嵌套并行放大了问题

`read_columns_filtered` 内部还有一层 `column_indices.par_iter()`（`reader.rs:991`），
运行在**同一个池**上。于是外层闭包持有许可 → 进入内层 `par_iter` → 在
`LockLatch` 上等待同池 worker。当外层 worker 已被 `acquire_io` 阻塞时，
这种嵌套等待无法被窃取机制化解。

### 2.5 现有默认值构成"必然复发"的配比

| 配置 | 默认值 | 代码位置 |
| --- | --- | --- |
| `execution_threads` | 64（nproc） | `config.rs:472` |
| `max_concurrent_requests` | `min(nproc,8)` = 8，保留 2 → **6 槽** | `config.rs` |
| `max_parallel_row_groups` | `clamp(nproc,1,8)` = **8** | `config.rs:449` |
| `global_io_permits` | `clamp(nproc,1,16)` = **16** | `config.rs:468` |
| `global_cpu_permits` | 64 | `config.rs:471` |
| `background_decode_pool` | **8 线程** | `cursor.rs:24` |

6 个查询槽 × 8 行组扇出 = 48 路并发解码，争夺 16 个不可中断许可，
而承载这些任务的池只有 8 个线程。这就是"重启后 3~4 分钟必复发"的算术来源。

### 2.6 同一个文件里已经有正确写法

`wait_for_byte_budget`（`cursor.rs:103`）是可借鉴的范本：

- 轮询而非阻塞等待；
- 同时检查 `cancel_flag` 与 `consumer_done`；
- 运行在生产线程而非 rayon worker；
- 注释中给出了死锁自由的论证。

内存背压（`byte_cap` + `query_memory_limit_mb`）已经是独立且正确的，
这说明 IO 许可并不是内存保护手段。

---

## 3. 目标模型

一条原则：**有界资源只在一个地方被决策，决策完成后执行路径上不再出现第二次等待。**

四条推论：

1. **并发度由池大小表达**，不由信号量表达。
2. **准入在入口做一次**：进入查询时按成本分配预算，查询生命周期内独占。
3. **执行路径内禁止阻塞等待**：需要等待时必须可取消、有 deadline，且不得发生在 rayon worker 上。
4. **读者拿快照，不拿锁**：`Arc<Snapshot>` 替代横跨扫描的表读锁。

---

## 4. 分阶段落地

### 阶段 0：自愈与可观测（当天，风险低）

| 项 | 内容 |
| --- | --- |
| 自愈 | `WatchdogSec` + 定时 canary 查询；连续失败主动 `abort` 让 systemd 拉起 |
| 指标 | 导出许可空闲数、队列深度、`request queue wait timeout` 计数 |
| 现场 | 看门狗线程：60s 无进展自动 dump 全栈 |

理由：`Restart=on-failure` 对"进程活着但卡死"完全无效，这是当晚人工重启 4 次的直接原因。

### 阶段 1：移除扫描路径的全局许可（本周，低风险）

**改动**：删除 `executor.rs:339-340/583/639-640`、`cursor.rs:325-326/500-501` 六处获取。

**保留**：`compactor.rs` 两处。维护线程不在 rayon worker 上，阻塞是安全的，
且其注释已明确"先许可后分区锁"的顺序，不应在本次一并改动。

**依据**：并发解码已由两个全局池限定（8 / 64）；内存由字节预算限定；
查询数由准入槽位限定。信号量没有提供池之外的任何保护。

**风险**：解码并发上限从 16 提升到"池大小"。这是吞吐/延迟的权衡，不是正确性风险。
若观察到大表扫描时页故障抖动，正确做法是**调小 `background_decode_pool` 或
`max_parallel_row_groups`**，而不是恢复一个在 worker 内阻塞的全局锁。

**验证**：
1. `cargo test` 全量通过；
2. 压测：6 并发 × 全表扫描，持续 30 分钟，观察是否出现 `request queue wait timeout`；
3. 复现用例：并发触发 `MAX(id)` 类全扫查询 + 后台解码路径，确认无 `Semaphore::acquire` 栈。

**回滚**：单 commit revert，无配置依赖。

### 阶段 2：入口准入与查询预算（本周~下周，中风险）

**改动**：
- `Admission` 在查询入口创建，持有该查询的并发预算；
- `Permit` 改为借用绑定 `&Admission`，**编译期禁止**在已持有预算处再申请全局预算；
- 所有等待改为 `try_acquire_for(deadline)` + 轮询 `QueryContext` 取消位。

**收益**：即使将来引入新的有界资源，"嵌套申请"也无法被表达，不依赖 review 记忆。

**验证**：`cargo test` + 注入式并发用例（人为耗尽预算，断言查询在 deadline 内以可识别错误退出，而非挂起）。

### 阶段 3：IO 异步化与快照读（排期，高风险）

| 子项 | 说明 |
| --- | --- |
| 3a 快照读 | 短锁取 manifest 版本 → `Arc<Snapshot>` → 释放；flush 只在切换瞬间拿写锁 |
| 3b 异步 IO | 若是 mmap 路径，先评估是否值得引入显式异步读；`madvise`/预取可能已足够 |

3a 同时解决"长查询阻塞 flush"。3b 需先量化页故障占比再决定，**不要为了架构美感引入 io_uring**。

---

## 5. 明确不做的事

| 不做 | 原因 |
| --- | --- |
| 把 `global_io_permits` 从 16 调到 64 | 只把死锁阈值从 8 并发推到 32 并发，性质不变 |
| 只加 `statement_timeout_secs` | `acquire` 不检查取消位，超时杀不掉它；只把"挂死"变成"查询全失败" |
| 长期使用 `TAIBAI_INLINE_DECODE=1` | 可作为诊断手段验证瓶颈，长期牺牲单查询延迟与并发 |
| 在 `work_limiter` 上加超时就算完 | 是安全网，不是消除结构 |

---

## 6. 阶段 1 落地记录

**状态**：已实现，未提交（工作区改动）。

| 文件 | 改动 |
| --- | --- |
| `scan/executor.rs` | 移除 3 处获取（行组读取 ×2、段 reader 打开 ×1） |
| `scan/cursor.rs` | 移除 2 处获取（预取、后台解码） |
| `common/work_limiter.rs` | 增加"禁止在 rayon worker 内获取"的契约说明 |
| `compactor.rs` | **未改动**，维护线程阻塞安全 |

**验证**：

- `cargo check --lib` 通过；
- `cargo test --lib`：893 passed / 0 failed / 4 ignored。

**部署后需在 hz-dev-204 复验**：

1. 6 并发全表扫描，持续 30 分钟，不得出现 `request queue wait timeout`；
2. 卡死复现窗口内抓栈，`work_limiter::Semaphore::acquire` 应只出现在 compactor 线程；
3. 对比改造前后大表扫描的 p99 与页故障率（`/proc/<pid>/stat` 的 minflt/majflt）。

---

## 7. 顺带确认的独立问题


`SELECT MAX(id) FROM t_stock_minute` 每天 15:48 左右稳定耗时 68~75s（7 次记录），
1.6G 表全量扫描，875MB 主键索引未被利用。这条查询每天制造一次高压窗口，
与死锁相互放大。建议单独立项（可走 `aggregate_meta` 的元数据快速路径）。

---

## 8. 实施记录（2026-11-06）

状态：**已实施**，分四个独立 commit（均可单独 revert）：

| Commit | 内容 |
| --- | --- |
| `移除全局许可信号量` | 阶段 1 扩大化：work_limiter 整个删除（含配置键），扫描 6 处 + compactor 2 处获取全部移除；cursor 后台解码闭包补齐取消检查 |
| `扫描读锁只护快照瞬间` | 阶段 3a 的 ScanExecutor/ANALYZE 部分：锁内只做原子快照（段租约 + memtable 物化），解码移到锁外；flush 写者不再被长扫描阻塞 |
| `看门狗自愈` | 阶段 0：canary 探活 + sd_notify（手写）+ systemd Type=notify/WatchdogSec=90；连续 3 轮失败 dump 运行中查询后 abort |
| `SHOW WORKLOAD` | 阶段 0 指标部分：准入池容量/累计/拒绝计数、canary 状态、运行查询数、折叠命中计数，全部一条 SQL 可查 |

### 与本提案的两处偏差

1. **compaction 不建专用池**（§4 阶段 1 原计划"保留 compactor 两处许可"，评审后进一步删除）：
   `compaction_memory_gate()` 本来就把全进程的重写 prepare 串行化为 1，
   许可对 compaction 的实际约束仅此而已——专用池不增加任何可达并发度，
   只会把编排逻辑搬到池线程（恰好破坏 thread-local 测试钩子，曾迫使
   引入测试串行化与全局原子改造，全部随池一起回滚）。内存闸门即并发上限。
2. **阶段 2（入口预算制）不再需要**：预算制要解决的问题是"嵌套申请全局
   有界资源无法在编译期禁止"。信号量原语删除后，进程内已不存在跨路径
   的计数信号量——并发度全部由池大小表达（入口槽位、全局池 64、
   后台解码池 8、compaction 闸门 1），内存由字节预算表达。没有第二处
   等待，就没有需要编译期禁止的对象。

### 遗留验证清单（hz-dev-204）

1. 30 分钟 6 并发全表扫描 + 夜间同步窗口：无 `request queue wait timeout`；
2. 任意时刻抓栈：`work_limiter::Semaphore::acquire` 不应出现（该符号已不存在）；
3. `SHOW WORKLOAD` 的 `admission.*.rejected_overload_total` 应保持 0；
4. `systemctl status taibai` 显示 `Type=notify` 心跳（`sd_notify WATCHDOG=1`）；
5. §7 的 `SELECT MAX(id) FROM t_stock_minute` 高压窗口与折叠命中
   （`fast_path.metadata_answers_total` 增速）另行立项。
