# Taibai

Taibai 是一个使用 Rust 实现的列式数据库服务器，线协议兼容 taibai-go。

## 功能特性

- 面向 OLAP 查询优化的列式存储
- 兼容 taibai-go 的线协议
- 高性能批量导入
- 支持 TLS 加密
- 支持 zstd 压缩
- 支持跨平台部署（Linux、macOS）

## 快速开始

### 构建

```bash
# 为当前平台构建
make build

# 或直接使用脚本构建
./scripts/build.sh build

# 为所有平台构建（linux/amd64、linux/arm64、darwin/amd64、darwin/arm64）
make build-all
```

### 打包

```bash
# 为当前平台创建发行包
make package

# 为所有平台创建发行包
make package-all
```

### 安装

```bash
# 安装到默认位置（/usr/local/taibai）
sudo make install

# 安装到自定义位置
sudo make install PREFIX=/opt/taibai
```

### 运行

```bash
# 启动服务
make start

# 或直接运行
./target/release/taibai --config conf/example.yaml

# 查看服务状态
make status

# 停止服务
make stop
```

## 项目结构

```
taibai/
├── Makefile              # 构建系统入口
├── Cargo.toml            # Rust 包清单
├── build.rs              # 构建脚本（嵌入版本信息）
├── conf/
│   └── example.yaml      # 示例配置
├── scripts/
│   ├── build.sh          # 跨平台构建脚本
│   ├── install.sh        # 系统安装脚本
│   ├── uninstall.sh      # 卸载脚本
│   └── service.sh        # 服务管理脚本
├── src/
│   ├── main.rs           # 主入口
│   ├── lib.rs            # 库入口
│   ├── version.rs        # 版本信息模块
│   ├── common/           # 通用工具
│   ├── net/              # 网络层
│   ├── protocol/         # 线协议
│   ├── sql/              # SQL 执行
│   ├── storage/          # 存储引擎
│   └── types/            # 类型定义
├── build/                # 构建产物
└── dist/                 # 发行包
```

## 构建命令

| 命令 | 说明 |
|------|------|
| `make build` | 为当前平台构建 |
| `make build-all` | 为所有平台构建 |
| `make package` | 为当前平台创建发行包 |
| `make package-all` | 为所有平台创建发行包 |
| `make install` | 安装到系统 |
| `make uninstall` | 卸载安装内容（保留数据） |
| `make uninstall-purge` | 删除所有内容，包括数据 |
| `make start` | 启动服务 |
| `make stop` | 停止服务 |
| `make restart` | 重启服务 |
| `make status` | 查看服务状态 |
| `make test` | 运行测试 |
| `make clean` | 清理构建产物 |
| `make help` | 显示所有命令 |

## 安装目录结构

安装完成后会创建以下目录：

```
/usr/local/taibai/
├── bin/                  # 二进制文件
│   └── taibai
├── conf/                 # 配置文件
│   └── taibai.yaml
├── data/                 # 数据库文件
├── log/                  # 日志文件
│   └── taibai.log
└── run/                  # PID 文件
    └── taibai.pid
```

## 服务管理

### Linux（systemd）

```bash
sudo systemctl start taibai    # 启动服务
sudo systemctl stop taibai     # 停止服务
sudo systemctl status taibai   # 查看状态
sudo systemctl enable taibai   # 设置开机自启
sudo journalctl -u taibai -f   # 查看日志
```

### macOS（launchd）

```bash
sudo launchctl load /Library/LaunchDaemons/com.taibai.server.plist    # 启动
sudo launchctl unload /Library/LaunchDaemons/com.taibai.server.plist  # 停止
sudo launchctl list | grep taibai                                      # 查看状态
tail -f /usr/local/taibai/log/stdout.log                               # 查看日志
```

## 配置

配置文件：`conf/taibai.yaml`（也可以使用 `conf/example.yaml` 作为模板）

```yaml
server:
  host: "0.0.0.0"
  port: 5151
  max_message_size: 67108864  # 64MB

storage:
  data_dir: "/usr/local/taibai/data"
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

## 交叉编译

进行交叉编译前，请确保已安装所需的 Rust target：

```bash
# 添加 target
rustup target add x86_64-unknown-linux-gnu
rustup target add aarch64-unknown-linux-gnu
rustup target add x86_64-apple-darwin
rustup target add aarch64-apple-darwin

# 为指定平台构建
./scripts/build.sh build -os=linux -arch=amd64
./scripts/build.sh build -os=darwin -arch=arm64
```

## 版本信息

版本信息会在构建时嵌入：

```bash
# 查看版本
./target/release/taibai --version

# 构建时指定自定义版本
VERSION=1.0.0 make build
```

构建版本包含：

- 版本号（来自 Cargo.toml 或 VERSION 环境变量）
- Git 提交哈希
- 构建时间戳
- 目标平台
- 构建配置

## 开发

```bash
# 以开发模式运行
cargo run --release -- --config conf/example.yaml

# 运行测试
cargo test

# 运行基准测试
cargo bench

# 检查代码
cargo clippy
cargo fmt --check
```

## 许可证

MIT 或 Apache-2.0