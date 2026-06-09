# 变更流快速入门

本快速入门展示外部消费者如何订阅 Monax 变更流、持久化处理事件以及 ACK 消费者偏移量。

协议级详情请参阅 `docs/change-stream.md`。

## 1. 启动 Monax

使用常规的服务器启动流程。默认监听地址为：

```text
127.0.0.1:5151
```

以下示例假设：

- 用户：`root`
- 数据库：`main`
- 表：`orders`
- 消费者组：`order-indexer`

创建源表：

```sql
CREATE TABLE orders (
  id INT64 PRIMARY KEY,
  status VARCHAR,
  amount INT64
);
```

插入测试行：

```sql
INSERT INTO orders VALUES (1, 'new', 100);
```

## 2. 授予订阅权限

`root` 默认可以订阅。对于非管理员用户，授予 `SUBSCRIBE`：

```sql
CREATE USER app_user IDENTIFIED BY 'app_pw';
GRANT SUBSCRIBE ON TABLE main.orders TO app_user;
```

如果消费者需要更广的范围，使用数据库或全局授权：

```sql
GRANT SUBSCRIBE ON DATABASE main TO app_user;
```

## 3. 使用 Rust 客户端消费

使用一个连接用于流式传输，另一个连接用于 ACK。流连接在调用 `subscribe_changes_*` 后专门用于变更事件。

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

        // 仅在下游副作用持久化后 ACK
        ack.ack_change_consumer("order-indexer", event.sequence).await?;
    }
}
```

重启时，`subscribe_changes_group` 从 `last_acked_sequence + 1` 恢复。

## 4. 使用 Golang 客户端消费

Golang 客户端需要实现原始 TCP 协议。以下是完整的消费者实现：

```go
package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"net"
)

const (
	MsgTypeLogin            = 0x01
	MsgTypeChangeSubscribe  = 0x60
	MsgTypeChangeEvent      = 0x61
	MsgTypeChangeAck        = 0x62
	MsgTypeChangeAckState   = 0x63
	MsgTypeError            = 0x03
)

type LoginRequest struct {
	User     string `json:"user"`
	Password string `json:"password"`
	Database string `json:"database"`
}

type SubscribeRequest struct {
	Database      string `json:"database,omitempty"`
	Table         string `json:"table,omitempty"`
	ConsumerGroup string `json:"consumer_group,omitempty"`
}

type ChangeEvent struct {
	Version            int64                  `json:"version"`
	Sequence           int64                  `json:"sequence"`
	TimestampMicros    int64                  `json:"timestamp_micros"`
	Database           string                 `json:"database"`
	Table              string                 `json:"table"`
	Operation          string                 `json:"operation"`
	RowsAffected       int64                  `json:"rows_affected"`
	Rows               []RowImage             `json:"rows"`
	RowImagesTruncated bool                   `json:"row_images_truncated"`
}

type RowImage struct {
	PrimaryKey map[string]interface{} `json:"primary_key"`
	Before     map[string]interface{} `json:"before"`
	After      map[string]interface{} `json:"after"`
}

type AckRequest struct {
	Group    string `json:"group"`
	Sequence int64  `json:"sequence"`
}

type Client struct {
	conn net.Conn
}

func Connect(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) sendMessage(msgType byte, payload []byte) error {
	header := make([]byte, 7)
	header[0] = 0x01
	header[1] = 0xDB
	header[2] = msgType
	binary.LittleEndian.PutUint32(header[3:7], uint32(len(payload)))

	_, err := c.conn.Write(header)
	if err != nil {
		return err
	}
	if len(payload) > 0 {
		_, err = c.conn.Write(payload)
	}
	return err
}

func (c *Client) recvMessage() (byte, []byte, error) {
	header := make([]byte, 7)
	_, err := c.conn.Read(header)
	if err != nil {
		return 0, nil, err
	}

	msgType := header[2]
	payloadLen := binary.LittleEndian.Uint32(header[3:7])

	payload := make([]byte, payloadLen)
	if payloadLen > 0 {
		_, err = c.conn.Read(payload)
	}
	return msgType, payload, err
}

func (c *Client) Login(user, password, database string) error {
	req := LoginRequest{
		User:     user,
		Password: password,
		Database: database,
	}
	payload, _ := json.Marshal(req)
	err := c.sendMessage(MsgTypeLogin, payload)
	if err != nil {
		return err
	}

	msgType, resp, err := c.recvMessage()
	if err != nil {
		return err
	}
	if msgType == MsgTypeError {
		return fmt.Errorf("login failed: %s", string(resp))
	}
	return nil
}

func (c *Client) SubscribeChanges(database, table, consumerGroup string) error {
	req := SubscribeRequest{
		Database:      database,
		Table:         table,
		ConsumerGroup: consumerGroup,
	}
	payload, _ := json.Marshal(req)
	return c.sendMessage(MsgTypeChangeSubscribe, payload)
}

func (c *Client) RecvChangeEvent() (*ChangeEvent, error) {
	msgType, payload, err := c.recvMessage()
	if err != nil {
		return nil, err
	}
	if msgType == MsgTypeError {
		return nil, fmt.Errorf("server error: %s", string(payload))
	}
	if msgType != MsgTypeChangeEvent {
		return nil, fmt.Errorf("unexpected message type: %d", msgType)
	}

	var event ChangeEvent
	err = json.Unmarshal(payload, &event)
	return &event, err
}

func (c *Client) AckConsumer(group string, sequence int64) error {
	req := AckRequest{
		Group:    group,
		Sequence: sequence,
	}
	payload, _ := json.Marshal(req)
	err := c.sendMessage(MsgTypeChangeAck, payload)
	if err != nil {
		return err
	}

	msgType, resp, err := c.recvMessage()
	if err != nil {
		return err
	}
	if msgType == MsgTypeError {
		return fmt.Errorf("ack failed: %s", string(resp))
	}
	return nil
}

func main() {
	stream, err := Connect("127.0.0.1:5151")
	if err != nil {
		log.Fatal(err)
	}
	defer stream.Close()

	err = stream.Login("app_user", "app_pw", "main")
	if err != nil {
		log.Fatal(err)
	}

	ack, err := Connect("127.0.0.1:5151")
	if err != nil {
		log.Fatal(err)
	}
	defer ack.Close()

	err = ack.Login("app_user", "app_pw", "main")
	if err != nil {
		log.Fatal(err)
	}

	err = stream.SubscribeChanges("main", "orders", "order-indexer")
	if err != nil {
		log.Fatal(err)
	}

	for {
		event, err := stream.RecvChangeEvent()
		if err != nil {
			log.Printf("receive error: %v", err)
			break
		}

		switch event.Operation {
		case "Insert", "Update", "Delete":
			for _, row := range event.Rows {
				log.Printf("seq=%d pk=%v before=%v after=%v",
					event.Sequence, row.PrimaryKey, row.Before, row.After)
			}
		default:
			log.Printf("seq=%d %s.%s %s",
				event.Sequence, event.Database, event.Table, event.Operation)
		}

		err = ack.AckConsumer("order-indexer", event.Sequence)
		if err != nil {
			log.Printf("ack error: %v", err)
		}
	}
}
```

## 5. 使用 Java 客户端消费

Java 客户端同样需要实现原始 TCP 协议。以下是完整的消费者实现：

```java
import java.io.*;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ChangeStreamConsumer {

    private static final byte MSG_TYPE_LOGIN = 0x01;
    private static final byte MSG_TYPE_CHANGE_SUBSCRIBE = 0x60;
    private static final byte MSG_TYPE_CHANGE_EVENT = 0x61;
    private static final byte MSG_TYPE_CHANGE_ACK = 0x62;
    private static final byte MSG_TYPE_CHANGE_ACK_STATE = 0x63;
    private static final byte MSG_TYPE_ERROR = 0x03;

    private static final byte[] MAGIC = {0x01, (byte) 0xDB};

    private static class LoginRequest {
        public String user;
        public String password;
        public String database;

        public LoginRequest(String user, String password, String database) {
            this.user = user;
            this.password = password;
            this.database = database;
        }
    }

    private static class SubscribeRequest {
        public String database;
        public String table;
        
        @JsonProperty("consumer_group")
        public String consumerGroup;

        public SubscribeRequest(String database, String table, String consumerGroup) {
            this.database = database;
            this.table = table;
            this.consumerGroup = consumerGroup;
        }
    }

    public static class ChangeEvent {
        public long version;
        public long sequence;
        
        @JsonProperty("timestamp_micros")
        public long timestampMicros;
        
        public String database;
        public String table;
        public String operation;
        
        @JsonProperty("rows_affected")
        public long rowsAffected;
        
        public List<RowImage> rows;
        
        @JsonProperty("row_images_truncated")
        public boolean rowImagesTruncated;
    }

    public static class RowImage {
        
        @JsonProperty("primary_key")
        public Map<String, Object> primaryKey;
        
        public Map<String, Object> before;
        public Map<String, Object> after;
    }

    private static class AckRequest {
        public String group;
        public long sequence;

        public AckRequest(String group, long sequence) {
            this.group = group;
            this.sequence = sequence;
        }
    }

    private final Socket socket;
    private final DataInputStream in;
    private final DataOutputStream out;
    private final ObjectMapper mapper;

    public ChangeStreamConsumer(String host, int port) throws IOException {
        this.socket = new Socket(host, port);
        this.in = new DataInputStream(socket.getInputStream());
        this.out = new DataOutputStream(socket.getOutputStream());
        this.mapper = new ObjectMapper();
    }

    public void close() throws IOException {
        socket.close();
    }

    private void sendMessage(byte msgType, byte[] payload) throws IOException {
        out.write(MAGIC);
        out.writeByte(msgType);
        out.writeInt(Integer.reverseBytes(payload.length));
        if (payload.length > 0) {
            out.write(payload);
        }
        out.flush();
    }

    private byte[] recvMessage() throws IOException {
        byte[] magic = new byte[2];
        in.readFully(magic);
        byte msgType = in.readByte();
        int payloadLen = Integer.reverseBytes(in.readInt());

        byte[] payload = new byte[payloadLen];
        if (payloadLen > 0) {
            in.readFully(payload);
        }

        if (msgType == MSG_TYPE_ERROR) {
            throw new IOException("server error: " + new String(payload));
        }

        return payload;
    }

    public void login(String user, String password, String database) throws IOException {
        byte[] payload = mapper.writeValueAsBytes(new LoginRequest(user, password, database));
        sendMessage(MSG_TYPE_LOGIN, payload);
        recvMessage();
    }

    public void subscribeChanges(String database, String table, String consumerGroup) throws IOException {
        byte[] payload = mapper.writeValueAsBytes(new SubscribeRequest(database, table, consumerGroup));
        sendMessage(MSG_TYPE_CHANGE_SUBSCRIBE, payload);
    }

    public ChangeEvent recvChangeEvent() throws IOException {
        byte[] payload = recvMessage();
        return mapper.readValue(payload, ChangeEvent.class);
    }

    public void ackConsumer(String group, long sequence) throws IOException {
        byte[] payload = mapper.writeValueAsBytes(new AckRequest(group, sequence));
        sendMessage(MSG_TYPE_CHANGE_ACK, payload);
        recvMessage();
    }

    public static void main(String[] args) {
        try {
            ChangeStreamConsumer stream = new ChangeStreamConsumer("127.0.0.1", 5151);
            stream.login("app_user", "app_pw", "main");

            ChangeStreamConsumer ack = new ChangeStreamConsumer("127.0.0.1", 5151);
            ack.login("app_user", "app_pw", "main");

            stream.subscribeChanges("main", "orders", "order-indexer");

            while (true) {
                ChangeEvent event = stream.recvChangeEvent();

                switch (event.operation) {
                    case "Insert":
                    case "Update":
                    case "Delete":
                        for (RowImage row : event.rows) {
                            System.out.printf("seq=%d pk=%s before=%s after=%s%n",
                                event.sequence, row.primaryKey, row.before, row.after);
                        }
                        break;
                    default:
                        System.out.printf("seq=%d %s.%s %s%n",
                            event.sequence, event.database, event.table, event.operation);
                }

                ack.ackConsumer("order-indexer", event.sequence);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
```

Maven 依赖（`pom.xml`）：

```xml
<dependency>
    <groupId>com.fasterxml.jackson.core</groupId>
    <artifactId>jackson-databind</artifactId>
    <version>2.15.0</version>
</dependency>
```

## 6. 可靠消费者模式

对于外部接收器如 Kafka、Redis、Elasticsearch 或其他数据库：

1. 使用稳定的 `consumer_group` 订阅。
2. 幂等地处理每个事件。
3. 使用 `event.sequence` 作为去重键。
4. 应用行级 upsert/delete 时使用 `row.primary_key`。
5. 仅在接收器写入持久化后 ACK。

伪流程：

```text
event = recv_change_event()
if already_processed(event.sequence):
    ack(event.sequence)
    continue

write_to_sink(event)
record_processed_sequence(event.sequence)
ack(event.sequence)
```

投递是至少一次。消费者必须能够容忍重连、崩溃或 ACK 失败后的重复事件。

## 7. 事务性 ACK 模式

如果消费者将其处理结果或幂等表存储在 Monax 中，将工作和 ACK 放在同一个 SQL 事务中：

```sql
BEGIN;

INSERT INTO processed_change_events VALUES (1288, 'order-indexer');
INSERT INTO order_projection VALUES (1, 'new', 100);

ACK CHANGE CONSUMER 'order-indexer' TO 1288;

COMMIT;
```

如果事务回滚，ACK 也会回滚。这为存储在 Monax 中的副作用提供有效一次的行为。

对于外部系统，这不是分布式事务。继续使用接收器端的幂等性。

## 8. 原始协议摘要

任何语言都可以实现该协议：

1. 使用 `MSG_TYPE_LOGIN` 登录。
2. 发送带有 JSON 负载的 `MSG_TYPE_CHANGE_SUBSCRIBE`（`0x60`）：

```json
{
  "database": "main",
  "table": "orders",
  "consumer_group": "order-indexer"
}
```

3. 接收 `MSG_TYPE_CHANGE_EVENT`（`0x61`）消息。
4. 在普通非流连接上使用 `MSG_TYPE_CHANGE_ACK`（`0x62`）ACK：

```json
{
  "group": "order-indexer",
  "sequence": 1288
}
```

5. 期望 `MSG_TYPE_CHANGE_ACK_STATE`（`0x63`）：

```json
{
  "group": "order-indexer",
  "sequence": 1288
}
```

帧格式：

| 字段 | 大小 | 编码 |
| --- | ---: | --- |
| magic | 2 字节 | `0x01 0xDB` |
| 消息类型 | 1 字节 | 无符号整数 |
| 负载长度 | 4 字节 | 小端序 `u32` |
| 负载 | 可变 | 消息特定 |

## 9. 查看和运维

列出消费者偏移量：

```sql
SHOW CHANGE CONSUMERS;
```

查看流状态：

```sql
SHOW CHANGE STATUS;
```

重要状态列：

- `earliest_sequence`：最早保留的事件
- `latest_sequence`：最新保留的事件
- `retained_events`：当前持久化的事件数
- `outbox_prepared`：已暂存但未标记为已提交的变更发布请求
- `outbox_committed`：等待排空的已提交发件箱条目

重放近期持久化事件：

```sql
SHOW CHANGE EVENTS FOR main.orders FROM 1200 LIMIT 100;
```

重置或删除消费者：

```sql
RESET CHANGE CONSUMER 'order-indexer' TO 1200;
DROP CHANGE CONSUMER 'order-indexer';
```

这些管理语句需要全局管理员权限。

## 10. 错误处理

如果订阅者收到如下错误：

```text
change stream lagged, skipped N events
```

说明消费者落后于实时广播缓冲区。使用相同的 `consumer_group` 重新连接；当 ACK 的序列号仍然保留时，Monax 将从持久化的变更日志重放。

如果请求的序列早于 `earliest_sequence`，从全表快照重建消费者，然后将消费者组重置到快照边界。

## 11. 生产检查清单

- 每个逻辑接收器使用稳定的消费者组名称。
- 保持一个流连接和一个普通 ACK/SQL 连接。
- 在持久化处理后 ACK，不要提前。
- 使用 `event.sequence` 使接收器写入幂等。
- 使用 `row.primary_key` 中的主键进行行级 upsert/delete 逻辑。
- 监控 `SHOW CHANGE STATUS`，特别是保留和发件箱列。
- 如果消费者可能长时间离线，增加 `change_stream.retention_events`。
- 将 `row_images_truncated = true` 视为重新同步或查询受影响表的信号。