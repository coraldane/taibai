# Change Stream Quickstart

This quickstart shows how an external consumer can subscribe to Monax change streams, process events durably, and ACK consumer offsets.

For protocol-level details, see `docs/change-stream.md`.

## 1. Start Monax

Use the normal server startup flow. The default listener is:

```text
127.0.0.1:5151
```

The examples below assume:

- user: `root`
- password: `taibai`
- database: `main`
- table: `orders`
- consumer group: `order-indexer`

Create a source table:

```sql
CREATE TABLE orders (
  id INT64 PRIMARY KEY,
  status VARCHAR,
  amount INT64
);
```

Insert a test row:

```sql
INSERT INTO orders VALUES (1, 'new', 100);
```

## 2. Grant Subscribe Permission

`root` can subscribe by default. For a non-admin user, grant `SUBSCRIBE`:

```sql
CREATE USER app_user IDENTIFIED BY 'app_pw';
GRANT SUBSCRIBE ON TABLE main.orders TO app_user;
```

Use database or global grants if the consumer needs a wider scope:

```sql
GRANT SUBSCRIBE ON DATABASE main TO app_user;
```

## 3. Consume With the Rust Client

Use one connection for streaming and another connection for ACKs. A stream connection becomes dedicated to change events after `subscribe_changes_*`.

```rust
use monax::net::client::Client;
use monax::storage::change_stream::ChangeOperation;

#[tokio::main]
async fn main() -> monax::common::error::Result<()> {
    let mut stream = Client::connect("127.0.0.1:5151").await?;
    stream.login("app_user", "app_pw", "main").await?;

    let mut ack = Client::connect("127.0.0.1:5151").await?;
    ack.login("app_user", "app_pw", "main").await?;

    stream
        .subscribe_changes_group(Some("main"), Some("orders"), "order-indexer")
        .await?;

    loop {
        let event = stream.recv_change_event().await?;

        match event.operation {
            ChangeOperation::Insert | ChangeOperation::Update | ChangeOperation::Delete => {
                for row in &event.rows {
                    println!(
                        "seq={} pk={:?} before={:?} after={:?}",
                        event.sequence, row.primary_key, row.before, row.after
                    );
                }
            }
            _ => {
                println!(
                    "seq={} {}.{} {:?}",
                    event.sequence, event.database, event.table, event.operation
                );
            }
        }

        // ACK only after the downstream side effect is durable.
        ack.ack_change_consumer("order-indexer", event.sequence).await?;
    }
}
```

On restart, `subscribe_changes_group` resumes from `last_acked_sequence + 1`.

## 4. Reliable Consumer Pattern

For an external sink such as Kafka, Redis, Elasticsearch, or another database:

1. Subscribe with a stable `consumer_group`.
2. Process each event idempotently.
3. Use `event.sequence` as the dedupe key.
4. Use `row.primary_key` when applying row-level upserts/deletes.
5. ACK only after the sink write is durable.

Pseudo-flow:

```text
event = recv_change_event()
if already_processed(event.sequence):
    ack(event.sequence)
    continue

write_to_sink(event)
record_processed_sequence(event.sequence)
ack(event.sequence)
```

Delivery is at-least-once. Consumers must tolerate duplicate events after reconnects, crashes, or ACK failures.

## 5. Transactional ACK Pattern

If the consumer stores its processed result or idempotency table in Monax, put the work and ACK in the same SQL transaction:

```sql
BEGIN;

INSERT INTO processed_change_events VALUES (1288, 'order-indexer');
INSERT INTO order_projection VALUES (1, 'new', 100);

ACK CHANGE CONSUMER 'order-indexer' TO 1288;

COMMIT;
```

If the transaction rolls back, the ACK is rolled back too. This gives effectively-once behavior for side effects stored in Monax.

For external systems, this is not a distributed transaction. Keep using sink-side idempotency.

## 6. Raw Protocol Summary

Any language can implement the protocol:

1. Login with `MSG_TYPE_LOGIN`.
2. Send `MSG_TYPE_CHANGE_SUBSCRIBE` (`0x60`) with a JSON payload:

```json
{
  "database": "main",
  "table": "orders",
  "consumer_group": "order-indexer"
}
```

3. Receive `MSG_TYPE_CHANGE_EVENT` (`0x61`) messages.
4. ACK on a normal non-streaming connection with `MSG_TYPE_CHANGE_ACK` (`0x62`):

```json
{
  "group": "order-indexer",
  "sequence": 1288
}
```

5. Expect `MSG_TYPE_CHANGE_ACK_STATE` (`0x63`):

```json
{
  "group": "order-indexer",
  "sequence": 1288
}
```

Frame format:

| Field | Size | Encoding |
| --- | ---: | --- |
| magic | 2 bytes | `0x01 0xDB` |
| message type | 1 byte | unsigned integer |
| payload length | 4 bytes | little-endian `u32` |
| payload | variable | message-specific |

## 7. Inspect and Operate

List consumer offsets:

```sql
SHOW CHANGE CONSUMERS;
```

Show stream status:

```sql
SHOW CHANGE STATUS;
```

Important status columns:

- `earliest_sequence`: oldest retained event
- `latest_sequence`: newest retained event
- `retained_events`: current persisted event count
- `outbox_prepared`: change publish requests prepared but not marked committed
- `outbox_committed`: committed outbox entries waiting to be drained

Replay recent persisted events:

```sql
SHOW CHANGE EVENTS FOR main.orders FROM 1200 LIMIT 100;
```

Reset or remove a consumer:

```sql
RESET CHANGE CONSUMER 'order-indexer' TO 1200;
DROP CHANGE CONSUMER 'order-indexer';
```

These management statements require global admin privileges.

## 8. Error Handling

If a subscriber receives an error like:

```text
change stream lagged, skipped N events
```

the consumer fell behind the live broadcast buffer. Reconnect with the same `consumer_group`; Monax will replay from the persisted change log when the ACKed sequence is still retained.

If the requested sequence is older than `earliest_sequence`, rebuild the consumer from a full table snapshot and then reset the consumer group to the snapshot boundary.

## 9. Production Checklist

- Use a stable consumer group name per logical sink.
- Keep one streaming connection and one normal ACK/SQL connection.
- ACK after durable processing, never before.
- Make sink writes idempotent with `event.sequence`.
- Use primary keys from `row.primary_key` for row-level upsert/delete logic.
- Monitor `SHOW CHANGE STATUS`, especially retention and outbox columns.
- Increase `change_stream.retention_events` if consumers may be offline for a long time.
- Treat `row_images_truncated = true` as a signal to resync or query the affected table.
