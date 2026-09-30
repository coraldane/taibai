# Direct Bulk 列式载荷协议（v1/v2）

> 状态：已实现（2026-09）。描述 `MSG_TYPE_BULK_INSERT_COLUMNAR`（0x23）消息载荷的两种编码。
> 背景与动机见 [行情数据实时落库改造方案](../plans/realtime-market-data-ingestion-plan.md)。

## 版本识别

- **v1（legacy，encoding=1）**：载荷以 u16 表名长度开头，无版本字段。
- **v2（binary，encoding=2）**：载荷以 `u16 magic = 0xFFFF` + `u8 encoding = 2` 开头，其后结构与 v1 头部一致。

服务端通过前两个字节判定版本：v1 载荷的首字段是表名长度，只有表名长达 65535 字节才会与
magic 冲突，而标识符长度远小于此，因此无实际歧义。v1 路径原样保留，新旧客户端可混用。

## v1 载荷布局

```
u16 table_len, table bytes
u32 row_count
u32 column_count
per column:
    u16 name_len, name bytes
    u8  dtype
    u8  scale
    u8  precision
    u32 elem_count        // 必须等于 row_count
    u32 nulls_size        // 0 或 ceil(row_count/64)*8
    data bytes            // 定宽列：row_count * width 原始小端值
                          // 变长列：每值 u32 长度前缀 + 字节（NULL 行也占一个槽位）
    nulls bytes           // Arrow 风格位图，bit K = 1 表示第 K 行为 NULL
```

## v2 载荷布局

头部在 v1 之前增加 3 字节版本头；列块显式携带数据长度，变长列改用连续缓冲 + offsets 数组：

```
u16 magic = 0xFFFF
u8  encoding = 2
u16 table_len, table bytes
u32 row_count
u32 column_count
per column:
    u16 name_len, name bytes
    u8  dtype
    u8  scale
    u8  precision
    u32 elem_count        // 必须等于 row_count
    u32 nulls_size        // 0 或 ceil(row_count/64)*8
    u32 data_len
    u32 offsets_len       // 定宽列必须为 0；变长列必须为 (elem_count+1)*4
    data bytes            // 定宽列：row_count * width 原始小端值（客户端直接 memcpy）
                          // 变长列：所有值拼接的连续字节，无逐值长度前缀
    offsets bytes         // 仅变长列：u32 小端 offsets[row_count+1]，
                          // offsets[0]=0、单调不减、offsets[row_count]=data_len
    nulls bytes           // 同 v1
```

## 服务端校验

- 定宽列：`data_len == row_count * width`，且不得携带 offsets。
- 变长列：`offsets_len == (row_count+1)*4`，offsets 首项为 0、单调不减、末项等于 data_len。
- null 位图：长度必须是 `ceil(row_count/64)*8`，超出 row_count 的保留位必须为 0。
- 载荷不允许有尾部多余字节。

## 性能语义

v2 的设计目标是消除"文本惯性"：

- 定宽列在服务端一次 memcpy 进入 `ColumnArray` 的类型化 vector，不再逐值构造 `ScalarValue`。
- VarChar/Blob 列的连续缓冲与 offsets 数组直接构成 `Utf8Buffer`/`BinaryBuffer`（Arrow 兼容布局），
  服务端只做一次整体 UTF-8 校验。
- 客户端（taibai-cpp）侧 `RecordBatch` 持有类型化 `ColumnBuffer`，编码即整块拷贝，
  不再经过 `vector<string>` 中间态。

## 客户端支持

| 客户端 API | 编码 |
| --- | --- |
| taibai-cpp `bulk_insert_columnar(table, const RecordBatch&)` / `pipeline_bulk_insert_columnar` | v2 |
| taibai-cpp `bulk_insert_columnar(table, const std::vector<Column>&, row_count)`（兼容重载） | v1 |
| `examples/batch_import` 等存量 Rust 构造器 | v1 |

服务端对两种编码的处理路径完全一致（解析后统一为 `DirectBulkRequest`），查询、压缩、
zone map、热存喂给等行为与编码版本无关。
