# 迭代复盘：包边界、可测性与对外 SDK——把「能跑」整理成「可交付」

日期：2026-08-13　范围：全仓（`storage`、`service`、`cluster`、`client`、`internal`）

## 背景

功能与性能之外，仓库本身有一批「不影响运行、但决定他人能否使用与维护」的问题：包边界不清、构造靠全局配置、死代码与失真的桩混在测试里、对外只有裸 TCP 协议。逐项处理。

## 依赖注入取代全局配置

`storage` 原先在包内直接读 `config.G`。后果有三：同进程内无法并存两套配置的引擎（多节点集成测试正需要）、测试之间经全局变量互相影响（本包曾因此偶发失败）、生产代码被迫写防御性快照以避开「测试并发改配置」形成的数据竞争。

改为 `Options` 显式传参，`DefaultOptions()` 是全局配置进入存储层的**唯一入口**——调用方构造时读一次，此后引擎只认自己那份参数。

## 包边界

| 动作 | 理由 |
| --- | --- |
| `MemTable` → `Engine` | 它管着内存表 + SSTable + WAL 协作 + compaction，早已不是一张内存表 |
| 910 行 `sstable.go` 拆为 `_read` / `_write` / `_meta` | 单文件承载三种关注点，改一处要通读全文 |
| `SkipList` 独立成文件，层高/概率改为字段 | 原先从全局配置读，无法在测试里构造不同形态 |
| `service/cluster` 上提为顶层 `cluster/` | 它不依赖 service，反被 service 依赖；放在 service 下面制造了反向依赖的错觉 |
| 内部实现移入 `internal/` | 由编译器而非注释强制「外部不得导入」 |

`cluster` 一并删掉三段**零调用方**的代码（`BoundedRing`、`Forward`/`ForwardFunc`、`Rebalance`）。死代码不修、不测、直接删——留着只会让后来者以为它是设计的一部分。

## 对外 SDK（`client/`）

此前对外只有裸 TCP 帧格式：使用方要自己拼 `[dataLen u32 LE][idLen u16 LE][id][data]`、自己管连接、自己解析状态串。SDK 承担这些：

- **连接池 + 有界重试**：`MaxRetries`、`RetryBackoff`；重试仅对连接级失败，不对业务错误。
- **哨兵错误**：`ErrKeyNotFound`、`ErrOverloaded`、`ErrDropped`、`ErrServer`、`ErrClosed`、`ErrProtocol`，配 `errors.Is`。此前「远端故障」与「key 不存在」在客户端无法区分（`bannet.Client.Get` 把一切非 OK 状态都映射为「未找到」）——这是个真缺陷，一并修掉。
- **context 支持**：阻塞 I/O 期间由看门狗置 `SetDeadline(now)` 打断，使取消真正生效而不是等超时。
- **线程安全**：一个 client 可被多 goroutine 共用。该用例正是测出前述两处数据竞争的地方。

**协议一致性由测试守护**：`wire_compat_test.go` 逐字节对比 SDK 的 `encodeFrame` 与服务端 `bannet.DataPack.Pack` 的输出。SDK 与服务端各有一份编码实现（SDK 不能依赖服务端包），因此必须有测试钉住它们相等——否则协议一改就静默不兼容。

gRPC 相应降为内部接口，对外只以 SDK 呈现。

## 压测脚本此前根本跑不起来

`bench.sh` 有两个各自致命的问题：**无条件等待日志出现「becoming leader」**（单机模式永不出现，故必然超时退出），以及**用 GNU 语法的 `sed \+`**（BSD sed 静默不匹配，PORT 覆盖从未生效）。也就是说**一条压测都没能跑过**。修好之后本轮所有测量才成为可能。

## 仓库卫生

`.claude/`（含本地会话与工作树）、`bin/` 下 23MB 编译产物曾被提交上线。`git rm --cached` 移出并写进 `.gitignore`。根因是 `git add -A` 的惯性——`git mv` 会立刻把改名入暂存区，此后 `-A` 会把无关文件一并带走。此后一律按显式路径暂存。

## 后续

- `service` 层仍有 24 处直接读 `config.G`、34 处测试改全局，可按 `storage` 的同一手法参数化。
- `internal/metrics` 已留出扩展点，尚无 `/metrics` HTTP 端点。
