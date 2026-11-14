# perf_verify — SQL INSERT 吞吐实测工具

对运行中的 taibai 服务端做端到端 SQL INSERT 吞吐测量，覆盖三种写入形态。它是[示例工具指南](../../docs/operations/examples-guide.md)中「典型性能对比」写入数字的数据来源。

## 前置

- Windows + PowerShell 5+（生成合成 tick CSV 与服务端配置）
- 先构建：`cargo build --release --workspace`
- 默认凭据 `root/taibai`，端口 `5153`（配置由脚本生成到 `%TEMP%\taibai_bench\`，不写入仓库）

## 用法

```bat
examples\perf_verify\perf_verify.bat          @rem 默认 100000 行
examples\perf_verify\perf_verify.bat 200000   @rem 指定行数
```

依次测量（每次重建同名表，保证互不干扰）：

| 场景 | 每 INSERT 行数 | 说明 |
|------|--------------|------|
| realtime | 100 | 语句数多、逐批提交，贴近实时写入形态 |
| import | 10000 | 多值大批次，贴近批量导入形态 |
| bulk | 10000 | 直写 segment 的批量导入模式（对照） |

## 合成数据

34 列行情 tick 表（`VARCHAR security_id` + `DECIMAL(9,2)` 价格 + `INT32/INT64` 量额 + `TIMESTAMP local_time`），`PARTITION BY VALUE(deal_date)` + `CLUSTER BY (security_id, deal_date, deal_time)`，行数由参数控制，100 只股票轮流交替——与真实 tick 流入顺序形态一致。

## 解读

- 输出中的 `rows/sec` 为客户端观测的端到端吞吐（含 CSV 解析、SQL 生成、网络、服务端写入与持久化）。
- realtime 与 import 的差距主要来自语句数（固定开销 × 语句数）与 tail 提交次数；如需服务端内部阶段计时，把生成的 `bench.yaml` 中 `log.insert_profile` 保持为 `true`，观察服务端 stderr 的 `[insert-profile]` 输出（`server.log`）。
- 结果受 CPU、磁盘与内存影响，报告数字时请附带硬件与环境说明（见 [金融行情数据验证清单](../../docs/operations/financial-market-data-validation-checklist.md)）。

## 故障排查

**服务端启动报 `os error 10048`（套接字地址只允许使用一次），但 `netstat` 显示端口空闲。**
已定位的一种本机环境形态：安全/网络组件按进程行为拦截新编译二进制的 socket `bind`（`.NET`/`tokio` 最小程序在同一端口 `bind` 成功，taibai 服务端任意端口、任意 `data_dir` 均失败；改名 exe 无效，Defender 无威胁记录）。处置建议：

1. 用管理员权限排查本机安全软件/EDR 的网络行为防护记录，将 `target\release\taibai.exe` 加入白名单后重试；
2. 或换一台无此类拦截的机器运行本工具（工具本身与代码无关，属环境问题）；
3. 排查时可先用最小程序对照确认是环境而非代码：

```powershell
$l=[System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback,<port>); $l.Start(); $l.Stop(); 'BIND_OK'
```

若上例成功而 taibai 失败，即为上述按进程拦截的形态。
