# 大文件重构后的隐藏问题复查

复查范围：compactor、hot_store、aggregate_meta 及压缩读取旧段的接口。

## 已确认并修复

1. **旧段列位置与当前 schema 错位，可能持久化错误数据**
   - 普通压缩的缓存和内存排序直接使用当前 schema 的位置读取段文件。
   - 反例：旧段为 removed/id/payload，删除 removed、重命名 id 后压缩，原 removed 的 999 被写为 id。
   - 统一 `SegmentReader::read_catalog_column`：按当前名及历史名查找，缺失列补 DEFAULT 或类型化 NULL；缓存、内存排序、外部排序及 TTL 读取复用此规则。TTL 输入选择也对齐物理列位置，读取错误向上返回。
   - 回归：普通、内存排序、外部排序三条路径，覆盖 DROP、RENAME、ADD 后的值及 NULL。

2. **未知字符串分区被解释为没有分区条件**
   - `resolve_query_keys` 把“给定值但池中不存在”和“未给分区条件”都转成 None。
   - 反例：请求 partition='missing'，返回 partition='known' 的行。
   - 修正共同解析入口，未知分区返回无匹配；覆盖 lookup、count、filtered count，保留未传分区及 NULL 分区的正确含义，覆盖 sealed/open 两种数据。

3. **外部归并在输入结束时绕过缓冲刷新**
   - 输入耗尽分支提前 continue，跳过输出批次阈值检查。
   - 反例：7 个单行输入、输出批次上限 2，原实现一次写出 7 行。
   - 将刷新放在流推进之前，所有结束分支均服从阈值；回归结果为 2/2/2/1，行顺序及总数保持正确。

4. **字符串/二进制列压缩有平方级搬移工作**
   - 对紧凑变长缓冲逐行 set，会搬移已有 payload 并更新后续全部 offsets。
   - 改为按输出位置暂存变长值，完成后一次打包，消除逐行 relocation。
   - 回归覆盖跨源交错、长 UTF-8、空字符串、空二进制、NULL、小容量缓存淘汰后的结果。

5. **每次 gather 分配并遍历全部历史源行组**
   - 即使仅输出一个小批次，也为整个分区的每个源行组构造 Vec。
   - 改为只登记当前输出批次实际涉及的源行组，避免随着整个分区行组数增加而重复分配和扫描。

## 验证

新增四个有实际反例/边界输入的测试，放在独立测试文件中。前三个正确性问题均有修复前失败的日志。

- cargo test --lib：794 passed，4 ignored，0 failed。
- cargo check --all-targets：通过。
- cargo test --tests：52 个集成测试文件，369 passed，8 ignored，0 failed；其中同时运行的 lib 测试也全部通过。
- rustfmt 检查（涉及的修改文件）、git diff --check：通过。

aggregate_meta 本轮未确认新的错误。以上结论是针对本轮范围的验证，不表示整个数据库已无隐藏问题。
