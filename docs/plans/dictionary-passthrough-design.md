# 字典直通解码设计(ColumnArray 字典变体)

> 目标:消灭"字典编码 VARCHAR 解码展开成字符串、聚合再重新 intern"的往返。
> 量化依据(§10.4):Q5 的 110ms 中 ~97ms 是聚合等解码;分组循环仍有逐行字符串哈希。
> 预期:Q5 112ms → ~60-80ms;所有含低基数 VARCHAR 的扫描/分组/过滤受益。

## 1. 数据结构

```rust
// types/array.rs 新增变体
Dictionary {
    ids: Vec<u32>,          // 行 → 字典条目(UTF8 字典内下标)
    dict: Utf8Buffer,       // 去重值Arena(复用现有 Utf8Buffer)
    bitmap: NullBitmap,     // NULL 行:ids[row] 无效(用 bitmap 判)
}
```

语义约定:
- `scalar(row)` → `ScalarValue::VarChar(dict 值 / None)`(与 VarChar 一致);
- `encode_key_at` → tag + dict 字节(与 VarChar 完全同形,保证分组键/spill 兼容);
- `len()` = ids.len();`estimated_size_bytes` = ids 字节 + dict 字节;
- NULL 表示:沿用 bitmap(ids[row] 不指向哨兵)。

## 2. 产出处(解码层)

`decode_adaptive_column` 的 Dictionary 分支:不再 `decode_varchar_dictionary_payload → build_varchar_column`,
直接:
1. 解析字典 → Utf8Buffer(每 RG 仅 dict_count 次小分配);
2. 索引区(每行 u32)一次性 `chunks_exact(4)` 批量转 `Vec<u32>`(memcpy 级);
3. 产出 `ColumnArray::Dictionary`。
RLE/Plain VARCHAR 分支维持现状(见 §5 渐进策略)。

## 3. 消费处适配(按实际流向逐一处理,其余显式物化兜底)

扫描产出的 Dictionary 列只会流向以下消费方(逐个适配,**禁止** `_ =>` 静默吞):

| 消费方 | 处理 |
|---|---|
| 聚合快路径(aggregate_fast) | **主收益**:组键 = dict id;`FastGroupCol::Dict(&[u32], dict_len)`;slot 表用 `Vec<Option<u32>>`(dict 条目 → 组 id)直取,零哈希 |
| 通用分组循环 | `encode_key_at` 已天然支持(按 dict 值编码) |
| 行过滤(row_filter) | eq/in 谓词:先算值的 id,再按 ids 向量化比较 |
| LIMIT trim / filter_by_mask / take(gather) | ids 按选择向量 gather;dict 不变(共享) |
| 结果二进制编码 | 按行取 dict 值编码(客户端无字典感知,保持协议不变) |
| 热存 feed / 唯一索引 / 表达式求值 | **物化兜底**:进入这些路径前 `materialize_to_varchar()`(展开为 VarChar)——它们不在本次目标查询的热路径上 |

物化兜底实现为 `ColumnArray::materialize_varchar(&self) -> ColumnArray`,所有未适配的
`match` 臂调用它,保证语义正确优先、性能渐进。

## 4. 分组快路径的字典收益(核心)

```rust
FastGroupCol::Dict { ids, dict_len, bitmap } => {
    // group_of: Vec<Option<u32>>,长度 = dict_len,直取无哈希
    let gid = match group_of[ids[row] as usize] { Some(g) => g, None => new_group(...) };
}
```
分组循环每行成本:1 次数组索引 + 累加,字符串哈希完全消失。
合并(`merge_from`)需处理 dict 条目重映射:槽位表各自持有 dict 时,
合并阶段按值映射 dict 条目(低基数,代价 O(dict_len × slots))。
**更优**:槽位表共享同一 batch 的 dict(同批内必然相同),跨 batch 用
`dict_values → gid` 的全局 intern(每 batch 仅 dict_len 次,而非行数次)。

## 5. 渐进策略(降低手术风险)

1. **第一步只做"产生 + 聚合消费"**:解码产 Dictionary;聚合快路径/通用循环适配;
   所有其他消费点先走 `materialize_to_varchar` 兜底(保证全量测试绿);
2. 第二步再逐个把 row_filter、结果编码切到原生字典路径,删兜底;
3. 每步以 Q5/Q4 + 全量测试验收。

## 6. 前置未解问题(动手前先 profile)

解码并行度只扩到 ~3×(16 RG 分块理论 16×):需先定位是
permit 粒度、内存带宽(400MB 级 memcpy/zstd)还是 rayon 任务粒度。
若为内存带宽,字典直通的收益(少一次展开 memcpy)会打折,但不影响
分组端收益。

## 7. 验收

- Q5 ≤ 80ms(现 112ms);Q4 ≤ 20ms(现 25ms);
- 全量 40 测试目标通过;GROUP BY 结果与现实现逐组精确一致;
- `SELECT sym ... GROUP BY` 含 NULL 组的用例(手工构造)行为不变。
