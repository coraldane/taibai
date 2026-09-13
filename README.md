# Taibai

Taibai 是一个用 Rust 编写的列式数据库服务端。

> **项目状态：实验性。** 当前 `0.x` 版本适合数据库内核研究、功能验证和性能测试，尚未声明生产就绪。数据格式、协议、配置和 SQL 行为可能发生不兼容变化，详见[项目状态与限制](docs/user-guide/limitations.md)。

第一次使用请从[五分钟快速开始](docs/getting-started/quick-start.md)开始。

简单说，它是一个偏分析场景的数据库：适合写入大量数据，然后按列扫描、统计、聚合和查询。项目目前还在开发和完善中，更适合用来做数据库内核实验、功能验证和性能测试。

## 它能做什么

- 启动一个数据库服务，客户端可以连接上来执行 SQL。
- 支持创建数据库、创建表、插入数据、查询数据、更新数据和删除数据。
- 数据按列存储，更适合统计分析类查询。
- 支持批量导入，适合一次性写入大量数据。
- 支持事务、用户权限、分区、数据过期清理和变更订阅等数据库能力。
- 支持压缩数据，减少存储空间和网络传输开销。
- 支持可选的 TLS 加密连接。
- 支持独立 SQL Query Log，可通过 `log.query_file` 配置查询日志文件。
- 提供命令行客户端和若干示例程序。

## 项目结构

```text
.
├── src/
│   ├── main.rs           # 服务端和命令行入口
│   ├── lib.rs            # 库入口
│   ├── common/           # 配置、错误处理等通用代码
│   ├── net/              # 网络连接、客户端、服务端会话
│   ├── protocol/         # 客户端和服务端通信时使用的数据格式
│   ├── sql/              # SQL 解析、查询计划、优化和执行
│   ├── storage/          # 数据存储、表元数据、写入日志、索引、分区
│   ├── types/            # 数据类型、列数据和查询结果批次
│   ├── tools/            # 辅助工具
│   └── bin/              # 独立工具入口
├── conf/                 # 配置文件示例
├── docs/                 # 设计、功能和运维文档
├── examples/             # 使用示例
├── scripts/              # 构建、安装和服务管理脚本
├── Cargo.toml
└── Makefile
```

## 快速开始

### 构建

```bash
cargo build --release
```

也可以使用 Makefile：

```bash
make build
```

### 启动服务端

```bash
cargo run --release -- --config conf/example.yaml
```

也可以直接启动：

```bash
cargo run --release
```

默认服务端口是 `5151`，默认数据库是 `main`。

启动前无需修改示例配置；`storage.data_dir` 默认使用仓库下的 `./data`。生产部署时应将它改为独立的持久化目录。

### 使用命令行客户端

启动服务端后，可以用内置客户端连接：

```bash
cargo run --release -- client \
  --host 127.0.0.1:5151 \
  --user root \
  --password taibai \
  --database main
```

执行单条 SQL：

```bash
cargo run --release -- client \
  --host 127.0.0.1:5151 \
  --user root \
  --password taibai \
  --database main \
  --query "SHOW TABLES"
```

默认 root 用户密码是 `taibai`，仅用于本地开发。部署到共享或生产环境前必须修改默认凭据，并限制服务端口的网络访问。

## SQL 示例

```sql
CREATE DATABASE demo;
USE demo;

CREATE TABLE ticks (
  symbol VARCHAR,
  ts TIMESTAMP,
  price FLOAT64,
  volume INT64
) CLUSTER BY (symbol, ts);

INSERT INTO ticks VALUES
  ('AAPL', 1717200000000, 191.2, 1000),
  ('AAPL', 1717200001000, 191.5, 800),
  ('MSFT', 1717200000000, 420.1, 600);

SELECT symbol, COUNT(*) AS cnt, AVG(price) AS avg_price
FROM ticks
GROUP BY symbol
ORDER BY cnt DESC;
```

更多功能说明见 `docs/overview/project-summary.md`。

## 配置文件

示例配置在 `conf/example.yaml`。

常用配置项：

```yaml
server:
  host: "0.0.0.0"
  port: 5151
  max_message_size: 67108864

storage:
  data_dir: "/path/to/taibai/data"
  default_database: "main"
  flush_threshold_rows: 65536
  min_segment_seal_size_mb: 500

buffer:
  batch_size: 4096

compression:
  enabled: true

tls:
  enabled: false
  cert_path: ""
  key_path: ""


log:
  level: "info"
```

说明：

- `server.host`：服务监听地址。
- `server.port`：服务监听端口。
- `storage.data_dir`：数据文件保存目录。
- `storage.default_database`：默认数据库。
- `compression.enabled`：是否启用通信压缩。
- `tls.enabled`：是否启用加密连接。
- `log.level`：日志级别。

## 常用命令

| 命令 | 说明 |
| --- | --- |
| `cargo build --release` | 构建 release 版本 |
| `cargo run --release -- --config conf/example.yaml` | 启动服务端 |
| `cargo test` | 运行测试 |
| `cargo fmt --check` | 检查代码格式 |
| `cargo clippy` | 运行代码检查 |
| `make build` | 使用脚本构建当前平台产物 |
| `make package` | 打包当前平台发行包 |
| `make install PREFIX=/opt/taibai` | 安装到指定目录 |
| `make start` | 启动已安装的服务 |
| `make stop` | 停止已安装的服务 |
| `make status` | 查看已安装服务的状态 |
| `make clean` | 清理构建产物 |

## 示例程序

| 目录 | 说明 |
| --- | --- |
| `examples/batch_import` | 批量导入示例，适合大批量 CSV 或行情数据写入 |
| `examples/multi_insert` | 多行插入示例 |
| `examples/prepare_test` | 预处理语句示例 |
| `examples/hot_insert` | 热数据持续写入示例 |
| `examples/pubsub_demo` | 变更订阅示例：订阅表插入变更并接收服务端推送 |

运行示例前，请先启动 Taibai 服务端，并根据示例源码调整连接参数。

## 更多文档

- `docs/getting-started/quick-start.md`：五分钟快速开始。
- `docs/overview/project-summary.md`：工程功能总览。
- `docs/user-guide/pubsub.md`：变更订阅（pub/sub）协议、权限与配置。
- `docs/user-guide/limitations.md`：项目状态、兼容性和使用限制。
- `docs/design/TAIBAI_DESIGN_ZH.md`：整体设计说明。
- `docs/operations/data-import-flow.md`：数据导入流程。
- `docs/operations/windows-installation.md`：Windows 图形安装包和系统服务安装说明。
- `docs/reports/performance-verification-report.md`：性能验证报告。
- `CONTRIBUTING.md`：开发环境、质量检查和贡献流程。
- `SECURITY.md`：漏洞报告和部署安全注意事项。

完整文档目录见 `docs/README.md`。

## 打包和安装

```bash
make build
make package
sudo make install PREFIX=/usr/local/taibai
```

Windows 平台生成 MSI 安装包：

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\installer\windows\build.ps1 -AcceptWixEula
```

生成的安装包位于 `target\release\Taibai-<version>.msi`，其中 `<version>` 与 `Cargo.toml` 中的包版本一致。

安装后的默认目录结构：

```text
/usr/local/taibai/
├── bin/                  # 可执行文件
├── conf/                 # 配置文件
├── data/                 # 数据文件
├── log/                  # 日志文件
└── run/                  # 运行时文件
```

服务管理：

```bash
make start
make status
make stop
```

## 开发入口

- 看启动流程：从 `src/main.rs` 开始。
- 看 SQL：从 `src/sql/` 开始。
- 看存储：从 `src/storage/` 开始。
- 看网络通信：从 `src/net/` 和 `src/protocol/` 开始。
- 看数据类型：从 `src/types/` 开始。

## 参与贡献

提交改动前请阅读 `CONTRIBUTING.md` 和 `CODE_OF_CONDUCT.md`。安全漏洞请按 `SECURITY.md` 私密报告，不要创建公开 Issue。

## 许可证

Taibai 按以下任一许可证提供：

- Apache License, Version 2.0（`LICENSE-APACHE`）
- MIT License（`LICENSE-MIT`）

贡献内容同样按 `MIT OR Apache-2.0` 双许可证提供。
