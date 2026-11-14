# 多语言驱动

Taibai 服务端说自有二进制线协议（见[协议参考](../design/wire-protocol.md)），官方为常用语言各维护一套驱动，独立仓库发布。所有驱动连同一个端口（默认 `5151`），共享一致的认证、查询、预处理语句、事务与列式批量导入语义。

## 驱动一览

| 语言 | 仓库地址 | 引入方式 | 接口形态 |
|---|---|---|---|
| Go | https://github.com/coraldane/taibai-go | `go get` | 标准库 `database/sql` + 原生 API |
| Java | https://gitee.com/coraldane/taibai-jdbc | Maven | JDBC 4.2 |
| Python | https://gitee.com/coraldane/taibai-python | pip（源码安装） | `asyncio` 原生 |
| C++ | https://gitee.com/coraldane/taibai-cpp | CMake | C++17 静态库 |
| Rust | https://gitee.com/coraldane/taibai-rust | Cargo（git 依赖） | tokio 异步 |

连接约定与主仓一致：默认 `127.0.0.1:5151`，开发环境初始账号 `root` / `taibai`，数据库 `main`。示例里的凭据按本机实际配置替换。

## Go

```bash
go get github.com/coraldane/taibai-go
```

```go
package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/coraldane/taibai-go"
)

func main() {
	db, err := sql.Open("taibai", "taibai://root:taibai@127.0.0.1:5151/main")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT symbol, price FROM ticks LIMIT 10")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var symbol string
		var price float64
		if err := rows.Scan(&symbol, &price); err != nil {
			log.Fatal(err)
		}
		fmt.Println(symbol, price)
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
}
```

需要绕开文本结果的场景（大批量写入、二进制列式导入）用驱动自带原生 API：`BulkInsert` 生成多行 SQL，`BinaryBulkInsert` 直接发送列式载荷，详见仓库 README。

## Java

仓库未发布到中心仓库，先安装到本地 Maven 仓库：

```bash
git clone https://gitee.com/coraldane/taibai-jdbc
cd taibai-jdbc && mvn clean install
```

```xml
<dependency>
    <groupId>com.taibai</groupId>
    <artifactId>taibai-jdbc</artifactId>
    <version>1.0.0</version>
</dependency>
```

```java
import java.sql.*;

public class Example {
    public static void main(String[] args) throws SQLException {
        String url = "jdbc:taibai://127.0.0.1:5151/main";
        try (Connection conn = DriverManager.getConnection(url, "root", "taibai");
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT symbol, price FROM ticks LIMIT 10")) {
            while (rs.next()) {
                System.out.println(rs.getString("symbol") + " " + rs.getDouble("price"));
            }
        }
    }
}
```

驱动实现 JDBC 4.2，含 `PreparedStatement`、批量操作、`TaibaiDataSource` 连接池与 `TaibaiBulkInsert` 高速写入。

## Python

```bash
git clone https://gitee.com/coraldane/taibai-python
cd taibai-python && pip install -e .
```

```python
import asyncio
from taibai import Client

async def main():
    client = await Client.connect("127.0.0.1", 5151,
                                  user="root", password="taibai", database="main")
    result = await client.query("SELECT symbol, price FROM ticks LIMIT 10")
    for row in result.dicts():
        print(row)
    await client.close()

asyncio.run(main())
```

驱动零第三方依赖（只用 `asyncio`、`struct`、`ssl`、`zlib`、`json`），要求 Python 3.10+，覆盖文本/二进制结果集、预处理语句、事务、列式批量导入与 pipeline。

## C++

```bash
git clone https://gitee.com/coraldane/taibai-cpp
cd taibai-cpp
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build
```

```cpp
#include <taibai/client.hpp>
#include <taibai/batch.hpp>

int main() {
    taibai::Client client;
    client.connect("127.0.0.1", 5151);
    client.login("root", "taibai", "main");

    taibai::RecordBatch batch;
    batch.add_string_column("security_id", {"600000.SH", "000001.SZ"});
    batch.add_int32_column("trans_date", {20260610, 20260610});
    batch.add_float64_column("last_price", {12.34, 10.01});
    client.bulk_insert_columnar("stock_l2_snapshot", batch);
}
```

安装后下游 CMake 工程可直接链接：

```cmake
find_package(taibai_cpp CONFIG REQUIRED)
target_link_libraries(app PRIVATE taibai::taibai_cpp)
```

## Rust

```toml
[dependencies]
taibai-rust = { git = "https://gitee.com/coraldane/taibai-rust" }
tokio = { version = "1", features = ["full"] }
```

```rust
use taibai_rust::{Client, ClientOptions};

#[tokio::main]
async fn main() -> taibai_rust::Result<()> {
    let mut client = Client::connect_with_options(ClientOptions::default()).await?;
    client.execute("CREATE DATABASE IF NOT EXISTS demo").await?;
    let rows = client.query("SHOW TABLES").await?;
    println!("{rows:?}");
    Ok(())
}
```

`ClientOptions::default()` 指向 `127.0.0.1:5151`、`root` / `taibai`、库 `main`。驱动基于 tokio，支持预处理语句、事务、列式批量导入、pipeline 与变更订阅。

## 版本匹配

驱动与服务端共用同一套线协议，**升级服务端时请同步升级驱动**，跨版本组合需先做兼容性验证。协议帧格式与消息类型见[二进制协议参考](../design/wire-protocol.md)，各驱动的 Issue 请提到对应仓库。
