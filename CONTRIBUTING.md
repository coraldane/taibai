# 参与贡献

感谢参与 Taibai。项目仍处于实验性开发阶段，提交改动前请先了解 [项目限制](docs/user-guide/limitations.md) 和 [整体设计](docs/design/TAIBAI_DESIGN_ZH.md)。

## 开发环境

需要安装 Git 和 Rust stable 工具链。克隆仓库后运行：

```bash
cargo build
cargo test
```

服务端和客户端的本地使用方式见 [快速开始](docs/getting-started/quick-start.md)。

## 提交改动

1. 从最新代码创建独立分支。
2. 保持改动范围集中，并为行为变化增加或更新测试。
3. 若修改配置、命令、SQL 行为或公共接口，同步更新文档。
4. 提交前运行质量检查。
5. 创建 Pull Request，说明问题、方案、验证结果和兼容性影响。

```bash
cargo fmt --check
cargo clippy --workspace --all-targets --all-features
cargo test --workspace
```

`Cargo.toml` 设置了 `autotests = false`。新增根包集成测试时，需要添加对应的 `[[test]]` 条目；workspace 示例包中的测试按各自清单管理。

## 开发约定

- 遵循现有模块边界和 Rust 风格。
- 不在代码或配置中提交密码、令牌、证书、私有数据和机器专属路径。
- 不提交数据库文件、日志、构建产物或性能测试原始数据。
- 错误应包含可操作的上下文，服务端不得因普通客户端输入而 panic。
- 修复缺陷时优先添加能够复现问题的测试。
- 性能改动应说明测试数据、硬件、命令和优化前后结果。
- 数据格式、协议或配置的不兼容变更必须在 Pull Request 中突出说明。

## Issue 与安全问题

普通缺陷和功能建议可以通过仓库 Issue 提交。请提供版本、平台、复现步骤、预期行为、实际行为和必要日志。

安全漏洞不要提交公开 Issue，请遵循 [安全策略](SECURITY.md)。

## 许可证

提交贡献即表示你同意贡献内容按 `MIT OR Apache-2.0` 双许可证提供。
