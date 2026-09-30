# 备份、迁移与恢复

本文描述 taibai 数据目录的备份、段格式迁移和恢复流程。相关工具随服务端一起构建（`cargo build --release`）。

## 备份（offline）

`backup` 工具把 `meta/`（catalog、账号、基线）与所有段 `.dat` 文件复制到目标目录，并写 `backup_manifest.json`。若主库开启了 `storage.tail_archive`，归档的 tail 日志（`wal/<db>/<table>/memtable_tail/`）会一并备份。

```bash
# 前置：先停止服务端（优雅关闭会 seal 全部 active writer，保证目录一致）
backup --data /var/lib/taibai --dest /backup/2026-09-28 --verify
```

- `--verify` 会用服务端同款 reader 打开每个已复制段（结构性校验 + 整体 CRC）。
- 发现 `.meta`/`.del`/`.tmp` 等活动段产物时会跳过并告警——说明服务端不是正常关闭，请重新执行关闭流程后再备份。
- 恢复 = 把备份目录拷回（或将 `storage.data_dir` 指向备份目录）。
- 要前滚到最新已提交状态，可用归档 tail 日志重放（PITR/复制阶段的能力，基于 V3 帧的 LSN 与提交时间戳）。

## 段格式迁移（migrate）

读取端支持版本区间，新二进制可直接读旧数据；旧二进制读新段会在 header 版本门明确报错。升级二进制后可执行：

```bash
# dry run：只校验并报告（magic、版本、footer CRC、完整解析）
migrate --data /var/lib/taibai
# 应用：header 版本补丁（tmp+rename 原子替换）
migrate --data /var/lib/taibai --write
# 校验读取：额外用 SegmentReader 打开每个段
migrate --data /var/lib/taibai --deep
```

- 仅迁移已 seal 的段；带 `.meta` sidecar 的 active 段会被拒绝——先正常关服。
- `--db`/`--table` 可缩小范围。
- 升级前照例先备份。

## 相关配置

| 配置 | 默认 | 说明 |
|---|---|---|
| `storage.tail_archive` | `false` | 清理 memtable tail 时归档到 `<data>/wal/<db>/<table>/memtable_tail/` 而非删除；PITR/复制引导的增量日志源 |

`wal` 顶级目录是保留名，SQL 与数据库扫描不会把它当作用户数据库。

## 代码索引

| 行为 | 位置 |
|---|---|
| V3 tail 帧（LSN + 提交时间戳）与 V2/V3 双解码 | `src/storage/core/durable_tail.rs` |
| tail 归档与清理路径 | `durable_tail.rs`（`archive_or_remove` / `dispose_tail_file`） |
| 段版本门与版本区间 | `src/storage/segment/constants.rs` |
| footer 扩展单一维护点 | `src/storage/segment/footer_ext.rs` |
| 段升级原语 | `src/storage/segment/migrate.rs` |
| 备份工具 | `src/bin/backup.rs` |
| 迁移工具 | `src/bin/migrate.rs` |
