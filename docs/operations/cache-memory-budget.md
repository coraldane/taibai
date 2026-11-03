# 缓存内存预算与上报规范

## 1. 背景

生产环境出现「空闲进程常驻 25 GiB、`SHOW MEMORY` 全 0」的问题。

第一层原因是 `SegmentMetaCache` 按**条数**限容（1024 条），单条是一个 2 GiB
segment 解析后的 footer 对象图（约 80 MiB）。这个上限来自一个看起来稳定的配置项，
没有任何字节口径的指标能暴露它，且淘汰只在 `put` 时触发 —— 空闲进程永远走不到淘汰。

第二层原因更本质：**footer 被当成常驻对象图来持有**。一个段的 23.5 MiB footer
字节解析后膨胀到 80 MiB 堆（每个行组 8 个 `Vec` + 每列一个内嵌两个 `ScalarValue`
的 `ZoneMap`），而同一份 footer 还被 `SegmentReader`（扫描）和
`SegmentMetaSnapshot`（裁剪）各解析一次，互不共享。这不是「某条缓存太贪」，
是「元数据的数据模型错了」。

同类问题此前已出现过一次（`mutable_reader_cache_mb` 的注释记录了
"without a byte ceiling … unbounded anon RSS growth"）。本文档记录规范与终态设计。

## 2. 硬性规则

新增或修改任何**进程内缓存**时，以下三条全部满足：

1. **按字节限容**，不按条数。允许在字节之外再加条数上限，但不允许只有条数。
   条数上限的失效模式是静默的：单条大小随数据规模变化，配置不变而内存线性增长。
2. **必须有预算上限的配置项**，单位 MB，默认值写进 `conf/example.yaml`。
3. **必须出现在 `SHOW MEMORY`**。没有上报的缓存等于没有上限 —— 事故里
   `SHOW MEMORY` 全 0 却占 25 GiB，就是因为这条没做到。

推荐直接复用 `storage/segment_manager/byte_lru.rs` 的 `ByteLru<K, V>`：
传入每条目的 cost，容器负责 LRU 淘汰、超预算拒收、失效释放。

**再加一条**：缓存条目应当是**紧凑表示**，不是解析后的对象图。上限只能限制
「装多少条」，压不住「一条有多重」—— 参见第 4 节。

## 3. 当前缓存清单

| 缓存 | 配置项 | 默认上限 | 上报位置 | 单条成本 |
|---|---|---|---|---|
| sealed reader cache | `storage.reader_cache_mb`（`reader_cache_capacity` 仍为条数上限） | 2 GiB | `segment_readers.*` | `SegmentReader::footer_heap_bytes()` = footer 索引 + 行数前缀；footer 字节走 mmap，不计堆 |
| mutable reader cache | `storage.mutable_reader_cache_mb` | 4 GiB（另受 4×capacity 条数限制） | `segment_readers.mutable_*` | 活跃段文件整份堆拷贝 |
| segment meta cache | `storage.segment_meta_cache_mb` | 2 GiB | `segment_meta.*` | `SegmentMetaSnapshot::estimated_heap_bytes()` = 索引 + footer 字节 |
| cluster meta cache | 无（常数 16384 条） | ≈ 数 MB | 未上报 | 单列 `ZoneMap`，条目小且定长为主 |

`SHOW MEMORY` 的 `caches.charged_heap_bytes_total` 是前三项之和，用于和进程 RSS 对照：
两者差额应稳定在 memtable + 执行期结构 + 分配器余量，若差额持续增长，说明有未纳入
预算的持有者。

## 4. 终态：footer 索引常驻，行组记录按需解码

这是 Parquet / ClickHouse 的模型 —— **footer 是磁盘上的字节，不是常驻对象图**。

### 4.1 结构

`SegmentFooterIndex`（`storage/segment/footer.rs`）只保留：

- 段级元数据：schema、段级 zone map、delete bitmap、footer tail；
- **每行组 12 字节**：一个 `u32` footer 内偏移 + 一个 `u64` 行数。

行组记录（每列三种 sizes、每列 zone map、CRC、预聚合 sum）不解析，需要时用
`decode_row_group` / `decode_row_group_into` 从 footer 字节现解码，用完即丢。
只读列数据时用 `decode_row_group_layout`，连 zone map 都不必跨过。

`SegmentReader` 的 footer 字节直接切片自**已经 mmap 的段文件**
（`data[data_end .. len-8]`）：零堆、零额外 IO，占的是可回收的 page cache。
只有 sidecar 来源的 footer 才持有一份 owned 缓冲。

`SegmentMetaSnapshot` 与 `SegmentReader` 走同一套索引与解码器，不再各解析一份。

### 4.2 实测收益

对生产 `md_trade` 一个真实段（`p20250825_0001`，2.0 GiB 压缩 / 14.9 GiB 原始，
210,193,861 行，28,315 行组）用 `examples/meta_report.rs` 实测：

| 项 | 数值 |
|---|---|
| footer 落盘 | 23.49 MiB |
| 解析后堆（旧） | **80.20 MiB**（zone map 54.4 + sizes/CRC/sums 13.6 + 向量头） |
| 解析后堆（新，索引） | **0.33 MiB**（12.1 B/行组） |
| 降幅 | **246×** |
| 布隆过滤器 / value set | 0 / 0（生产段本来就没有） |

### 4.3 为什么按需解码不会拖慢扫描

- 一个行组记录约 830 字节，而同组要读的列数据是 552 KiB（原始）/ 74 KiB（压缩），
  解码开销是 I/O 的 0.2% 量级；
- 裁剪与读取的**字段需求互补**：裁剪要 zone map、不要 sizes；读数据要 sizes、
  不要 zone map。两条路径各取所需，不存在「解码了用不上」；
- 遍历全段时复用同一个 `RowGroupMeta` 缓冲（`row_group_meta_into`），
  每行组零分配；
- `read_row_by_index` 从「遍历到目标行组」改为在常驻行数前缀上二分。

### 4.4 不改变磁盘格式

索引只是解析策略，footer 的二进制布局未动 —— 存量 14,500 个段不需要迁移，
升级后重启即生效（`SUPPORTED_SEGMENT_VERSIONS` 不变）。

## 5. 今后若还要降体积

索引已经把「常驻」压到可忽略，但**单段 footer 字节本身**仍是 23.5 MiB。若将来
footer 字节成为瓶颈（例如全表扫描反复 page-in），可继续：

| 手段 | footer 字节 23.5 MiB → | 代价 |
|---|---|---|
| 行组按字节成形（8192 行 → 8 MiB/组） | ≈1.8 MiB | 行组变粗，点查多读 8 MiB；memtable 阈值联动 |
| 行组级 zone map 只保留排序列（14 → 3 列） | ≈6 MiB | 非排序列失去行组级裁剪（段级仍在） |

两者都改写入侧语义，且只影响新段；存量要靠 compaction 重写。

## 6. 排查手法

```sql
SHOW MEMORY;              -- 各缓存条目数、已用字节、预算字节
```

对照进程 RSS：

```bash
grep -E 'VmRSS|RssAnon|RssFile' /proc/$(pgrep -f 'taibai server')/status
```

若 `charged_heap_bytes_total` 远小于 `RssAnon`，说明存在**未上报的持有者**，
优先怀疑：全局静态缓存、连接级缓冲、执行期结构、分配器滞留
（后者可通过 cgroup `inactive_anon`、`MALLOC_CONF` 的 decay 设置排除）。

单个段的 footer 剖面（不需要启动服务）：

```bash
cargo run --release --example meta_report -- /path/to/segment.dat
# 需要同目录同名 .meta 侧车文件
```

输出包含行组数、每行组体积、eager 与 lazy 两种解析的堆占用对比，并对每个行组
校验两种解析结果一致。
