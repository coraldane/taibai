# Taibai 二进制协议（线上格式参考）

本文描述 taibai 自有二进制协议的实际线上字节布局，面向**要实现第三方客户端、网关或代理**的人。常量定义的唯一权威是 [`src/protocol/message.rs`](../../src/protocol/message.rs)，本文与代码冲突时以代码为准并请提 issue。

协议是**单连接请求/响应串行**的：一条连接上同一时刻只有一个在途请求（流水线写入 `send_execute` 是唯一例外，见第 6 节）。

## 1. 帧格式

```
+--------+--------+-----------+--------------------+------------------+
| 0x01   | 0xDB   | type (u8) | payload_len (u32LE)| payload ...      |
+--------+--------+-----------+--------------------+------------------+
  <-- MAGIC -->   <--------- 7 字节帧头 ---------->
```

- `MAGIC = [0x01, 0xDB]`，不匹配立即判定协议错误。
- `payload_len` 上限为 `server.max_message_size`（默认 64 MiB）。超长帧被拒绝。
- 所有多字节整数一律小端。

## 2. 消息类型

| 类型 | 名称 | 方向 | 说明 |
|---|---|---|---|
| `0x01` | `QUERY` | C→S | 执行 SQL |
| `0x03` | `ERROR` | S→C | 错误，payload 是 UTF-8 文本 |
| `0x04` | `PING` | C→S | 心跳 |
| `0x05` | `PONG` | S→C | 心跳应答 |
| `0x07` | `OK` | S→C | 通用成功，payload 首 8 字节是 `rows_affected` |
| `0x08` | `LOGIN` | C→S | 登录握手 |
| `0x09` | `LOGOUT` | C→S | 登出（服务端不回复） |
| `0x0A` | `AUTH_OK` | S→C | 登录成功，payload 是 session_id 字符串 |
| `0x0B` | `AUTH_FAIL` | S→C | 登录失败，payload 是原因文本 |
| `0x23` | `BULK_INSERT_COLUMNAR` | C→S | 列式批量导入，见 [direct-bulk-binary-protocol](direct-bulk-binary-protocol.md) |
| `0x24` | `BINARY_QUERY_RESULT` | S→C | 单帧结果集 |
| `0x25` | `FLUSH` | C→S | 刷盘 |
| `0x26` | `RESULT_CHUNK` | S→C | **流式结果分块** |
| `0x27` | `RESULT_END` | S→C | **流式结果结束** |
| `0x30/0x31/0x32` | `BEGIN/COMMIT/ROLLBACK_TXN` | C→S | 会话级事务 |
| `0x40` | `CURSOR` | C→S | 历史别名，等价于 `QUERY`（无游标语义） |
| `0x50/0x51/0x52/0x53` | `PREPARE/EXECUTE/CLOSE_STMT/BATCH_EXECUTE` | C→S | 预编译语句 |
| `0x60`–`0x65` | `SUBSCRIBE`/`UNSUBSCRIBE`/`NOTIFICATION*` | 双向 | v1 变更订阅，见 [pubsub](../user-guide/pubsub.md) |
| `0x66`–`0x6B` | `SUBSCRIBE_V2`/`SUB_CONFIRMED`/`SUB_DATA`/`SUB_ERROR`/`SUB_ACK`/`UNSUBSCRIBE_V2` | 双向 | 可靠订阅 |
| `0x70`–`0x75` | `REPL_HELLO`/`REPL_EVENT`/`REPL_SEGMENT_FETCH`/`REPL_SEGMENT_CHUNK`/`REPL_ACK`/`REPL_SEGMENT_GONE` | 双向 | 主备流复制 |

`0x26`/`0x27` 只由服务端发出，**只在会话开启流式结果时出现**（第 5 节）。未识别的类型必须报错并断开，不能静默跳过——帧是对齐的，跳过一帧会让后续整条流错位。

## 3. 登录握手

`LOGIN` payload：

```
[string user][string password][string database][i32 LE timezone_offset_hours][u8 streaming]
```

- `string` 指 `u32 LE 长度 + UTF-8 字节`。
- 三段字符串之后必须**恰好 5 字节**，否则服务端返回 `invalid login capabilities`。
- `streaming != 0` 表示本会话的 SELECT 使用流式结果（`0x26`…`0x27`）；`0` 表示单帧 `0x24`。
- `timezone_offset_hours` 是整小时 UTC 偏移，影响 TIMESTAMP 的渲染。
- 数据库名留空时取 `storage.default_database`。
- 成功后返回 `0x0A`（payload=session_id）；失败返回 `0x0B`。**同一条连接重复 LOGIN 会被拒绝**（保留已建立的会话状态：预编译语句、活动事务）。

> 接入警示：内置 Rust 客户端默认 `streaming = 1`（见 `LoginOptions::default`）。照抄它的登录包，就必须同时实现流式结果，否则会在第一条 SELECT 上报“未识别的 0x26”。

## 4. 查询与结果

`QUERY` payload：

```
[string database][string sql]
```

`database` 留空表示使用会话当前库。回复有三种形态，**按顺序判断，缺一不可**：

| 回复 | payload |
|---|---|
| `0x07 OK` | `u64 LE rows_affected`（+ 可选 timings 尾巴） |
| `0x24 BINARY_QUERY_RESULT` | 二进制结果集（+ 可选 timings 尾巴） |
| `0x26`…`0x27` | 流式结果集（第 5 节） |
| `0x03 ERROR` | UTF-8 错误文本 |

错误文本可能以 `[RETRYABLE]` 开头，表示瞬态资源不足；只有只读语句（`SELECT`/`SHOW`/`EXPLAIN`）可以退避重试，写语句必须由应用决定（可能已提交）。

### 4.1 二进制结果集编码

`0x24` 与每一个 `0x26` 使用同一套布局：

```
u32  column_count
重复 column_count 次：
  u32  name_len
  [name_len] name (UTF-8)
  u8   data_type       // 见 src/protocol/types.rs
  u8   scale
  u8   precision
u32  row_count
重复 column_count 次：
  u32  bitmap_len      // 空值位图字节数
  [bitmap_len] bitmap  // 1 bit/行，1 = NULL
  u32  data_len
  [data_len] data      // 定长类型直接拼接；变长类型逐值 u32 长度前缀
```

### 4.2 服务端计时尾巴（可选）

`0x07`、`0x24`、`0x27` 的 payload 尾部可能追加 28 字节：

```
"MXTP" + u64 LE total_us + u64 LE execute_us + u64 LE encode_us
```

解析时先看固定偏移处是否出现 `MXTP`；不存在则整段 payload 都是业务数据。客户端必须容忍该尾巴缺失。

## 5. 流式结果（`0x26` / `0x27`）

会话 `streaming = 1` 时，一个 SELECT 的应答是：

```
0x26 (chunk 1)  0x26 (chunk 2)  ...  0x27 (END)
```

- **每个 chunk 都是一份完整自描述的结果集**：带列定义、带行数据。批次过大时服务端按行切分（估算预算为 `max_message_size / 2`，为编码膨胀和每块重复的 schema 留余量）。
- **零行结果仍然发一个 chunk**（列定义完整、`row_count = 0`）。这是刻意的：schema 只存在于 chunk 里，`0x27` 不带列信息，少这一块客户端就只剩零列可用。
- **`0x27` 是纯元数据帧**：payload 为空或仅含 timings 尾巴。它不是“又一条结果”。
- **错误可以出现在流中间**：`0x03` 直接终止流，已发出的 chunk 由客户端自行丢弃或保留（内置客户端整体丢弃）。
- 流式只放宽了**单帧**上限，不放宽查询内存预算；结果集上限仍由 `buffer.query_memory_limit_mb` 与算子预算决定。

客户端循环（伪代码）：

```
frames = 0; schema = None; rows = []
frame = recv()
if frame.type == 0x26:
    while true:
        if frame.type == 0x26:
            batch = decode(frame.payload)
            if schema != None and schema != batch.schema: fail("schema changed mid-stream")
            schema = batch.schema
            rows += batch.rows
            frames += 1
        elif frame.type == 0x27 or frame.type == 0x03: break
        else: fail("unexpected frame inside result stream")
        frame = recv()
```

### 5.1 覆盖范围

| 语句形态 | 结果形态 |
|---|---|
| 普通 `SELECT`（单表或多表 JOIN，无子查询、无派生表） | 流式 `0x26`…`0x27` |
| 含子查询 / 派生表 / `UNION` 的 `SELECT` | 回落单帧 `0x24` |
| 事务内（`BEGIN` 之后）的 `SELECT` | 回落单帧 `0x24` |
| 预编译 `EXECUTE` / `BATCH_EXECUTE` | **始终** `0x24`（流式尚未覆盖该路径） |
| `SHOW` / `EXPLAIN` / DDL / DML | `0x24` 或 `0x07` |

也就是说：**打开流式不等于所有结果都会分块**。客户端必须同时实现两条路径；只实现 `0x24` 的客户端在开启流式的会话上必然失败。

## 6. 流水线写入

`send_execute` / `recv_execute_result` 是写入专用流水线：连续发送多条 `QUERY`，再按发送顺序逐个收取 `0x07`。它只接受返回 `rows_affected` 的语句；发 `SELECT`/`SHOW`/`EXPLAIN` 会在发送侧直接拒绝（内置客户端），若绕过则接收侧会把整条结果流抽干后报错，保证连接不会错位。

## 7. 客户端接入检查清单

1. 校验 `MAGIC`，按 `payload_len` 精确切帧。
2. 登录能力字节显式写出（`streaming` 明确取 0 或 1），不要留未初始化字节。
3. 处理 `0x07` / `0x24` / `0x26`…`0x27` / `0x03` 四种回复，缺一不可。
4. 合并 chunk 前校验 schema 一致；不一致视为协议错误。
5. 零行结果使用 chunk 里的列定义，不要假设“没有行就没有列”。
6. 流中 `0x03` 视为终止，并丢弃已收批次（或按业务显式保留）。
7. 未识别的消息类型必须报错，不能跳过。
8. 不要依赖 `0x40 CURSOR` 的游标语义——它只是 `QUERY` 别名。

## 8. 兼容性说明

- `0x27` 的报文格式自流式上线以来未变；零行结果补发的 schema-only chunk 是**新增的 chunk**，对已有客户端透明（它本来就在处理 chunk）。
- 若某客户端历史上把“零行 → 零列”当作约定实现，升级到本版本后会开始收到列定义，属于修正而非破坏。

## 9. 代码索引

| 行为 | 位置 |
|---|---|
| 帧头、类型常量 | `src/protocol/message.rs` |
| 二进制结果编码 | `src/protocol/result.rs` |
| 登录、查询分发、流式分支 | `src/net/handler.rs`（`handle_login` / `handle_query`） |
| 计时尾巴 | `src/net/handler.rs`（`append_server_timing_trailer`） |
| 内置客户端（分块合并、登录选项） | `src/net/client.rs` |
| 输出 schema 推导（零行结果） | `src/sql/execution/operators/mod.rs`（`ExecutionOp::output_schema`） |
