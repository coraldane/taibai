# 后台生命周期、缓存与聚合补充审计

## 覆盖清单

- [x] 追踪分段刷盘线程的持有关系、停止标志与引擎 shutdown。
- [x] 核查可靠订阅 publisher 的 Weak 关系，排除强引用环误报。
- [x] 核查 reader 缓存淘汰、并发 miss 及 owned 文件加载。
- [x] 检查 join/aggregate 的预算、取消与外排归并，并运行现有测试。
- [x] 明确静态证据、测试范围和未完成的负载复现。

本轮不修改业务实现。下面是源码审计发现，不代表所有问题已动态复现或修复。

## F9：引擎关闭未停止分段刷盘线程

优先级 P1；调用链静态确认，尚未用重复创建/关闭引擎验证线程及 RSS 增长。

证据：src/storage/segment_manager/manager_core.rs:405-440 创建刷盘闭包，持有管理器强引用；返回监督线程句柄被丢弃。:443-447 的 SegmentManager::shutdown 才设置 flush_stop。src/sql/execution/engine/runtime.rs:1305-1335 的 ExecutionEngine::shutdown 停止批写器、执行 flush_shutdown_buffers 并停止其他运行时句柄，但没有调用 segment_mgr.shutdown。:794-807 的 flush_shutdown_buffers 只调用 flush_all_active 和保存目录，不设置停止标志。src/storage/core/batch_writer.rs:1277-1287 的 stop 也不停止该分段管理器。源码未找到补偿性的 ExecutionEngine/SegmentManager Drop 实现或其他生产调用设置该标志。

影响：进程内引擎关闭/重建或嵌入式使用时，旧刷盘线程可能继续存活并持有分段管理器、缓存及状态。整个进程退出会由操作系统回收，因此不能把它描述为退出后系统内存永久泄漏。正常单实例长期运行的影响也与反复创建实例不同。

建议：引擎关闭显式停止分段刷盘管理器，保存并 join 监督句柄，设计停止与最后 flush 的顺序；用 Weak 检查对象销毁、线程退出确认及重复创建/关闭测试验证。仅设置标志而不 join，仍不能证明 shutdown 返回时所有资源已释放。

## F10：reader 缓存 miss 缺少同路径加载合并，可能重复整文件分配

优先级 P1；代码确认加载不在同一个受保护区间内，实际并发放大待测。

证据：src/storage/segment_manager/manager_core.rs:986-1030 先锁缓存查找，释放锁后打开文件，再锁缓存 put。中间未见按路径 single-flight 或加载前字节预留。mutable reader 以及部分 fallback 使用 owned 模式；src/storage/segment/reader.rs:139-150 为整个文件长度分配 Vec 并读取。

影响：同一冷路径多个并发请求可以各自打开和分配 reader；后续同路径替换会使缓存只保留一份，但此前返回给查询的 Arc 可以继续存活。LRU 预算控制缓存持有量，不等于限制所有查询和正在加载的 reader 总量。immutable mmap 不应直接算作等量私有堆内存，风险应按 owned/mmap 区分。

建议：同路径加载合并、加载前资源预留及过大文件准入；统计 in-flight、缓存及外部 Arc 的占用；增加并发 miss 测试核对实际打开次数和 owned 分配量。

## F11：聚合外排归并同样没有限制 run 扇入

优先级 P1；代码确认，未做大量 run 压力复现。属于前轮排序 F3 的同类问题在另一算子中的独立路径。

证据：src/sql/execution/operators/aggregate.rs:1254-1275 为全部 spills 创建 cursor 并加入集合/堆；相应 SpillCursor 会加载当前批次。归并开始时 run 数增加会带来更多 reader、批次和归并状态。

影响：有外排不等于全流程固定内存。大量小 run 可放大归并工作集与资源需求。文件句柄是否达到限制、实际内存倍率均未量测。

建议：有界扇入和多轮归并，统一核算所有 cursor 的驻留预算。DISTINCT 已有动态大小估算与组内保护，不将其误报为完全没有预算。

## 已有保护及排除项

- src/storage/core/pubsub.rs:550-553 使用 Arc::downgrade，publisher 不是直接持有 manager 强引用的永久环。
- reader 缓存有字节计费、LRU 淘汰及同路径重新插入的计费释放；F10 针对并发加载和外部引用，不是声称缓存没有预算。
- join 与 aggregate 有外排及内存限制实现；本轮未确认新的 join 缺陷，不把有限阅读写成无 bug 证明。
- 线程监督器本身能停止并 join 的测试通过，但不能代替检查调用方有没有保存句柄和触发停止。

## 验证结果

本轮相关测试均预期通过，实际结果：

- cargo test --lib storage::segment_manager::reader_cache::tests -- --test-threads=2：8 passed。
- cargo test --lib common::thread_supervisor::tests::shutdown -- --test-threads=2：1 passed。
- cargo test --lib sql::execution::engine::tests::lifecycle -- --test-threads=2：11 passed。
- cargo test --lib sql::execution::operators::aggregate -- --test-threads=2：9 passed。
- cargo test --lib sql::execution::operators::join -- --test-threads=2：9 passed。

合计 38 项通过。现有生命周期测试未专门验证旧刷盘线程销毁，测试通过不能否定 F9。

## 总体交付与限制

三份报告合计 F1-F11；其中包含条件性泄漏、预算盲区、延迟释放和待基准验证的性能风险，不应统称为 11 个已复现故障。本轮清单的源码检查和现有定向验证已完成，但没有完成生产内存主因归因、长时间压力测试或修复验收。全库测试上一轮曾超时，不能声称全库通过。建议首先处理 F1/F9，再统一设计 F2/F3/F6/F7/F10/F11 的预算及准入控制，并对负载建立可重复的内存曲线。
