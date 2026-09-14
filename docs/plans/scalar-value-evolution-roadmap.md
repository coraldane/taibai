# ScalarValue 演进与聚合执行器路线图(2026-09)

> 背景:v3 扫描优化后(见 reports/benchmark-vs-duckdb-kdb-2026-09.md §10),10M 行查询 taibai 与 DuckDB 差距:Q7 count+sum 38ms vs 0.97ms(~39×),Q5 GROUP BY 388ms vs 11ms(~35×)。
> 本文回答两个问题:ScalarValue 这层封装能否去掉;差距如何继续收敛。

## 1. ScalarValue 使用面取证

全库 **2752 处引用**。按成本分类:

| 场景 | 每查询发生次数 | 是否 ScalarValue 瓶颈 |
|---|---|---|
| 解析器字面量 / prepared 参数 / 协议解码 / 结果编码 | O(1)~O(列) | 否(边界,应保留) |
| 谓词边界 `RangeClause(ScalarValue, bool)`(row_filter.rs) | 每查询编译一次 | 否(边界,保留) |
| 聚合新组时 `array.scalar(row)` 构造 key | O(组数) | 否 |
| **分组键 `encode_key_at`**:每行 push 类型 tag + null tag + LE bytes → 对 `Vec<u8>` 做哈希 | **O(行数)** | **是** |
| **聚合累加 `numeric_i64_at/f64_at`**:每行×每累加器一次枚举 match + 位图判断 + 边界检查 | **O(行数×累加器)** | **是(无 ScalarValue 构造,但逐行三分支)** |
| **VARCHAR 解码**(column_codec.rs:676):每行 `vec![0u8;len]` + `read_exact` + `String::from_utf8_lossy().into_owned()` = **2 次堆分配**;字典解码 `build_varchar_column` 又物化回 `Vec<String>` 丢弃字典 | **O(行数)** | **是(布局问题)** |
| 表达式求值 / 行过滤谓词评估 | O(行数)(走向量化内核的部分已豁免) | 部分 |

**关键事实**:`ColumnArray` 本身已是列式(`Vec<T>` + 位图)——问题不是"行存",而是热路径通过**逐行访问器**消费列式数据,且 VARCHAR 的 `Vec<String>` 布局在解码/分组两处制造海量堆分配。

## 2. 结论:不是"去掉",而是"双表示体系"

DuckDB(Vector + Value)、DataFusion(ArrayRef + ScalarValue)、Polars 同款答案:**ScalarValue 保留为边界类型,热路径全面类型化**。全量删除 2752 处引用是数月级手术且伤害表达力;正确目标是最多 50~80 处热路径改造:

```
边界(保留 ScalarValue):解析字面量、协议进出、prepared 参数、
                        谓词边界编译、表达式退化路径、REPLACE/UPDATE 行语义
热路径(类型化直读):聚合累加、分组键、扫描过滤、向量表达式、
                    INSERT VALUES 物化(fast.rs 已类型化)、结果批量编码
```

## 3. 分层路线图(按投入产出排序)

### 第 1 层:分组键字典化(Q5 主攻,预计 2~3 天)
- 单列 GROUP BY 快路径:解码后的 VarChar 列按 batch intern(`HashMap<&str, u32>`,跨 batch 维护全局 `Vec<String>` 字典),组键从 `Vec<u8>` 换 **u32 id**,`HashMap` 换 `Vec<Option<Accumulator>>` 按 id 直取;
- numeric 单列分组同样类型化(i64/f64 键 + FxHash);
- 输出侧只在建组时构造一次 `ScalarValue` key(现有 group_keys 结构不变,兼容 spilling/输出);
- 预期:Q5 388ms → **~130-160ms**(分组 ~280ms → ~40ms)。

### 第 2 层:VARCHAR 列 Arrow 式布局(全库布局手术,1~2 周)
- `ColumnArray::VarChar`:`Vec<String>` → `offsets: Vec<i32> + data: Vec<u8>`(Arrow StringArray 语义),解码 0 per-row 分配,`take/gather` 走 offsets 拷贝;
- 字典编码列解码直出 `(ids: Vec<u32>, dict)` 的 Dictionary 形态,**不再物化 Vec<String>**;GROUP BY / DISTINCT / IN 直接消费 id;
- 涉及:types/array、codec、hot_store(已有 StringPool 字典,语义对齐)、protocol 编码、表达式字符串内核;
- 预期:Q5 再 -80~100ms;所有含字符串的扫描/分组/过滤受益。

### 第 3 层:聚合算子向量化 + 流水线化(与第 2 层并行)
- `Accumulator::update_slice(col, mask)`:按列批量累加(分组后按组 gather 成连续段再累加,消除逐行分支);
- 扫描-聚合重叠:当前窗口解码是"16 个 RG 并行解完→单线程消费→下一窗"的锯齿波(EXPLAIN ANALYZE 峰值 2.14GB 也源于此),改双缓冲流水线后解码与消费全程重叠,Q7 剩余 38ms 约可再减半;
- 多线程分组:per-thread 局部哈希表 + 按 key 分区合并(DuckDB 同款),受 `max_parallel_row_groups` 之外的独立并行度控制。

### 第 4 层:编码与执行器持续逼近(长线)
- 数值列位压缩(bitpacking,SIMD 解包)替代 zstd-plain,DuckDB 扫描优势的核心;zstd 只留给字符串列与高熵列;
- 谓词 → 选择向量 → 按命中行 gather 的延迟物化(宽表大过滤比场景);
- 段级部分和元数据(Sum/Count/Min/Max 存入 segment footer,delete bitmap 为空时直接元数据作答)——count+sum 类查询亚毫秒的点杀方案;
- 向量表达式内核补全(算术/时间函数),`expr_vec_test` 已有骨架。

### 预期轨迹

| 里程碑 | Q5 | Q7 | 与 DuckDB 差距 |
|---|---|---|---|
| 现状(v3) | 388ms | 38ms | ~35× |
| +第 1 层 字典分组 | ~150ms | 38ms | ~14× |
| +第 2 层 String 布局 | ~70ms | ~30ms | ~6× |
| +第 3 层 向量化+流水线 | ~35ms | ~15ms | ~3× |
| +第 4 层 编码/元数据 | ~25ms | **<1ms(元数据)** / ~8ms(实算) | 2~8× |

剩余差距是 DuckDB 十余年 SIMD/编码调优的护城河,属于健康追赶区间。

> **实际达成(2026-09-14,v10,见 reports/benchmark-vs-duckdb-kdb-2026-09.md §4/§10.5~§10.8)**:
> Q5 836→**44ms**(~4× vs DuckDB)、Q7 224ms→**0.3ms**(元数据直答,**反超** DuckDB 0.97ms)、
> Q4 219→12ms(1.7×)、Q6 49→7ms(~10×)、Q1/Q2/Q3 反超。
> 轨迹与预期表量级一致;第 1 层(字典分组+直通)、第 4 层(BitPacked/DeltaRle 编码、段级部分和元数据直答)已落地,
> 第 3 层的流式聚合消费与 Q6 谓词下推仍为下一步空间。

## 4. 原则

- **每层独立可测**:每层落地后跑 Q1~Q7 + 全量回归,收益不达预期即回滚;
- **不引入逐行 ScalarValue 的新消费方**:新增算子一律列式输入;
- **布局手术优先级高于算法微优化**:`Vec<String>` → offsets 是全库收益,单独的分组快路径只是 Q5 收益。
