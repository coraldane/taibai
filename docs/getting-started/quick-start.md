# 五分钟快速开始

本指南用于在本地启动 Taibai、执行第一组 SQL，并确认读写链路正常。

## 1. 准备环境

需要 Git 和 Rust stable 工具链。确认工具可用：

```bash
rustc --version
cargo --version
```

Windows 用户可在 PowerShell 中执行相同命令；下面包含反斜杠续行的命令应改为单行执行。

## 2. 构建

在仓库根目录执行：

```bash
cargo build --release
```

首次构建需要下载依赖，耗时取决于网络和机器性能。

## 3. 启动服务端

打开第一个终端：

```bash
cargo run --release -- --config conf/example.yaml
```

示例配置将数据写入仓库下的 `data` 目录，服务默认监听 `0.0.0.0:5151`。模板保留了常用的连接、存储容量、查询内存和并行度参数；删除清理、碎片整理、TLS 和性能基线等高级参数未写出时使用内置默认值。看到包含 `starting taibai server` 的日志表示服务已开始启动。

如果端口已占用，可复制 `conf/example.yaml` 后修改 `server.port`，并在后续客户端命令中使用相同端口。

## 4. 验证连接

打开第二个终端：

```bash
cargo run --release -- client --host 127.0.0.1:5151 --user root --password taibai --database main --query "SHOW TABLES"
```

成功时客户端会显示连接、Ping 和查询结果，不应出现 `Failed to connect` 或 `Login failed`。

默认凭据仅用于本地开发，不要用于共享或生产环境。

## 5. 执行第一组 SQL

进入交互客户端：

```bash
cargo run --release -- client --host 127.0.0.1:5151 --user root --password taibai --database main
```

依次执行：

```sql
CREATE TABLE quick_start_ticks (
  symbol VARCHAR,
  ts TIMESTAMP,
  price FLOAT64,
  volume INT64
) CLUSTER BY (symbol, ts);

INSERT INTO quick_start_ticks VALUES
  ('AAPL', 1717200000000, 191.2, 1000),
  ('AAPL', 1717200001000, 191.5, 800),
  ('MSFT', 1717200000000, 420.1, 600);

SELECT symbol, COUNT(*) AS cnt, AVG(price) AS avg_price
FROM quick_start_ticks
GROUP BY symbol
ORDER BY cnt DESC;
```

查询应返回 `AAPL` 和 `MSFT` 两组结果，其中行数分别为 2 和 1。输入 `quit` 或 `\q` 退出客户端。

重复执行本流程前，可以先运行：

```sql
DROP TABLE quick_start_ticks;
```

## 6. 停止和清理

在服务端终端按 `Ctrl+C`，等待刷新和退出日志完成。确认不再需要本地数据后，可以删除仓库根目录下的 `data` 目录。

不要直接删除生产或仍在使用的数据目录。

## 常见问题

### 无法连接 5151 端口

确认服务端进程仍在运行，并检查服务端日志中是否有配置、目录或端口错误。

### 配置文件加载失败

确保命令从仓库根目录执行，或者给 `--config` 传入正确路径。

### 登录失败

本地默认用户为 `root`，密码为 `taibai`，默认数据库为 `main`。如果数据目录以前被其他配置使用过，请以该实例的实际用户配置为准。

### 构建失败

先确认正在使用受支持的 Rust stable 工具链，再执行 `cargo clean` 和 `cargo build`。若问题仍存在，请按 [贡献指南](../../CONTRIBUTING.md) 中的要求提交 Issue。

## 下一步

- [工程功能总览](../overview/project-summary.md)
- [示例工具使用指南](../operations/examples-guide.md)
- [兼容性与部署指引](../user-guide/limitations.md)
- [参与贡献](../../CONTRIBUTING.md)
