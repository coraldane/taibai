# Change Stream

Monax exposes table-level DML change notifications over the existing TCP message protocol so subscribers can be implemented in any language.

For a consumer-focused walkthrough, see `docs/change-stream-quickstart.md`.

## Wire protocol

Each message uses the standard Monax frame:

| Field | Size | Encoding |
| --- | ---: | --- |
| magic | 2 bytes | `0x01 0xDB` |
| message type | 1 byte | unsigned integer |
| payload length | 4 bytes | little-endian `u32` |
| payload | variable | message-specific |

## Message types

| Name | Type | Direction | Payload |
| --- | ---: | --- | --- |
| `MSG_TYPE_CHANGE_SUBSCRIBE` | `0x60` | client -> server | JSON subscription request |
| `MSG_TYPE_CHANGE_EVENT` | `0x61` | server -> client | JSON change event |
| `MSG_TYPE_CHANGE_ACK` | `0x62` | client -> server | JSON consumer group ACK |
| `MSG_TYPE_CHANGE_ACK_STATE` | `0x63` | server -> client | JSON consumer group state |
| `MSG_TYPE_ERROR` | `0x03` | server -> client | UTF-8 error text |

A client must login first using the normal `MSG_TYPE_LOGIN` flow, then send `MSG_TYPE_CHANGE_SUBSCRIBE` on that connection. After subscribing, the connection is dedicated to streaming change events.

Subscription authorization uses `SUBSCRIBE` permissions:

| Request scope | Required permission |
| --- | --- |
| `database` + `table` | `SUBSCRIBE` on table `database.table`, or a matching database/global grant |
| `database` only | `SUBSCRIBE` on that database, or global grant |
| no database/table filter | global `SUBSCRIBE`/`ALL` on `*` |
| `table` only | treated as current login database + table |

## Subscription request

Payload is UTF-8 JSON:

```json
{
  "database": "market",
  "table": "ticks",
  "include_recent": true,
  "from_sequence": 1200,
  "consumer_group": "etl-worker"
}
```

Fields:

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `database` | string/null | no | Database filter. Empty or missing means all databases. |
| `table` | string/null | no | Table filter. Empty or missing means all tables. |
| `include_recent` | boolean | no | If true, server sends matching recent events before live events. Ignored when `from_sequence` or resumable `consumer_group` state is provided. |
| `from_sequence` | integer/null | no | Replays persisted matching events with `sequence >= from_sequence`, then continues with live events. |
| `consumer_group` | string/null | no | Uses persisted ACK state for the group. If `from_sequence` is omitted, replay starts at `last_acked_sequence + 1`. |

An empty payload is valid and means subscribe to all future changes without recent replay.

## Change event

Payload is UTF-8 JSON serialized from `ChangeEvent`:

```json
{
  "version": 1,
  "sequence": 1,
  "timestamp_micros": 1760000000000000,
  "database": "market",
  "table": "ticks",
  "operation": "Insert",
  "rows_affected": 1000,
  "rows": [
    {
      "primary_key": { "id": { "Int64": 42 } },
      "after": {
        "id": { "Int64": 42 },
        "symbol": { "VarChar": "688256.SH" }
      }
    }
  ],
  "row_images_truncated": false
}
```

`operation` is one of:

- `Insert`
- `Update`
- `Delete`
- `Truncate`
- `CreateTable`
- `DropTable`
- `AlterTable`

The stream is table-level: DDL and `TRUNCATE` events have `rows_affected = 0`.

DML events include row-level images when the SQL execution path can capture them:

- `Insert`: `after`
- `Update`: `before` and `after`
- `Delete`: `before`

`primary_key` contains declared primary-key columns. If a table has no primary key, row images are still present but `primary_key` is empty. To prevent very large log entries, each event carries up to 1,024 row images and sets `row_images_truncated = true` when more rows were affected.

## Consumer group ACK

Consumer group offsets are persisted in `meta/change_stream_consumers.json`.

ACK request payload for `MSG_TYPE_CHANGE_ACK`:

```json
{
  "group": "etl-worker",
  "sequence": 1288
}
```

ACK response payload for `MSG_TYPE_CHANGE_ACK_STATE`:

```json
{
  "group": "etl-worker",
  "sequence": 1288
}
```

ACK updates are monotonic: acknowledging a lower sequence than the stored value does not move the group backward. Streaming connections are dedicated to events, so consumers should send ACKs on a separate normal client connection or reconnect after acknowledging.

ACK can also participate in a normal SQL transaction:

```sql
BEGIN;
-- apply downstream/idempotency writes in Monax
ACK CHANGE CONSUMER 'etl-worker' TO 1288;
COMMIT;
```

Inside a transaction, `ACK CHANGE CONSUMER` is staged in the session and is persisted only when `COMMIT` succeeds. `ROLLBACK` discards the staged ACK. This is the recommended pattern when the consumer stores its processed result or idempotency keys in Monax and wants the result write and consumer offset to advance together.

Consumer offsets can be inspected over SQL:

```sql
SHOW CHANGE CONSUMERS;
```

The result contains `consumer_group` and `sequence` columns, sorted by consumer group name.

Consumer offsets can be reset or removed:

```sql
RESET CHANGE CONSUMER 'etl-worker' TO 1288;
DROP CHANGE CONSUMER 'etl-worker';
```

Change stream status and retained events can be inspected:

```sql
SHOW CHANGE STATUS;
SHOW CHANGE EVENTS FROM 1200 LIMIT 100;
SHOW CHANGE EVENTS FOR market.ticks FROM 1200 LIMIT 100;
```

These management statements require global admin privileges.

## Consumer guidance

Downstream consumers should treat the current stream as table-level notification with bounded row images, not a full CDC log. The event indicates that a table changed, how many rows were affected, and includes sampled row images up to the configured per-event cap.

If the server sends `MSG_TYPE_ERROR` with text like `change stream lagged, skipped N events`, the subscriber was too slow and should resynchronize, for example by querying the affected tables again or reconnecting with `include_recent=true`.

Change events are appended as JSON lines to `meta/change_stream.log` under the configured data directory. On startup, Monax reloads the log, restores the next sequence number, rebuilds the in-memory recent cache from the tail of the log, and retains the newest `change_stream.retention_events` events. The default is 1,000,000 events.

New log entries are written as checksum-protected JSON records. Older plain `ChangeEvent` JSON lines are still readable. On startup, a corrupt tail entry is detected and the log is repaired by truncating to the last valid event.

Change publishing is coordinated through a persistent outbox at `meta/change_stream_outbox.json`. SQL transaction commit stages pending change publish requests in the outbox before committing storage writes, marks the outbox entry committed after storage commit, then drains it into `change_stream.log`. On startup, committed outbox entries are drained automatically. `SHOW CHANGE STATUS` exposes `outbox_prepared` and `outbox_committed` counts for operational checks.

For resumable consumption, persist the last processed `sequence` in the downstream service and reconnect with `from_sequence` set to the next sequence to consume. If the requested sequence is older than the retained log, the server returns `MSG_TYPE_ERROR` with the earliest available sequence, and the consumer must resynchronize from a full table snapshot.

Delivery semantics are at-least-once. Consumers should ACK only after durable downstream processing and use `sequence` plus primary-key row images to make processing idempotent. Transactional ACK gives effectively-once behavior for consumers whose side effects and ACK state are both stored in Monax; side effects in external systems still need their own idempotency or transactional offset storage.
