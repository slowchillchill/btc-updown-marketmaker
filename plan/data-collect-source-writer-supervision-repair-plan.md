# data-collect Binance aggTrade 最小恢复计划

> 生命周期：`HISTORICAL_EVIDENCE_ONLY`
>
> 最终快照：`FROZEN_AT_20260730_CONSOLIDATION`
>
> 当前执行 authority：`none`
>
> superseded_by：`DC-DOCS-20260730-CURRENT-AUTHORITY-CONSOLIDATION`
>
> 读取规则：本文仅用于审计历史事实；内部“当前、下一步、待执行、命令”均属于
> 当时检查点，不得驱动当前实现、测试、部署或远端操作。
>
> 2026-07-30：本计划只记录早期 aggTrade supervision 修复，后续代码与专项已接管，
> 不再是当前执行计划。
>
> 对应需求：`demand/data-collect-demand.md` 中 `DC-INCIDENT-20260709-AGGTRADE-T08`
>
> 对应 changelog：`changelog/data-collect-changelog.md`
>
> 事项编号：`DC-INCIDENT-20260709-AGGTRADE-T08`
>
> 状态：`DEPLOYED_VERIFIED`

## 目标

修复 Binance aggTrade 因 `serverShutdown` 或 WebSocket 断开而永久停止，以及 source/writer 已退出但主进程继续显示 active 的问题。修复必须保持最小范围，本地测试通过后才部署到数据采集服务器。

## 已确认事实

1. `binance_aggtrade_2026-07-09T08-running.parquet` 只有四字节 `PAR1`，没有 footer、row group 或可恢复行。
2. 远端采集主进程未退出，systemd 因此没有触发 `Restart=always`；六个 writer 线程只剩五个。
3. 当前代码会把所有 Binance text frame 送入 aggTrade writer；`serverShutdown` 会触发业务 parser 错误并结束 writer。
4. source task 只在整体 shutdown 后 join；writer channel disconnected 原本只记录 gap 并返回 `Ok(())`。
5. Binance 官方规定单个行情 WebSocket 连接最长不超过 24 小时，并要求在 `serverShutdown` 或连接断开后重新建立连接。

## 范围

- Binance aggTrade 的 raw/combined `serverShutdown` 识别。
- `serverShutdown`、`Close`、EOF、网络/协议错误和业务消息超时后的有界退避重连。
- 长期运行期间 source task 提前结束检测。
- writer channel disconnected 以及 writer parser/finalize 错误传播。
- 聚焦故障测试、全量本地回归、本地 release 构建、binary-only 远端部署和运行验证。

## 非范围

- 不修复或伪造四字节事故文件。
- 不在 live typed raw 中伪装历史缺口已补齐。
- 不新增通用 quarantine、manifest revision、归档协议、systemd StartLimit 分类或 26 小时阻塞 gate。
- 不修改 Polymarket SDK 路径、数据 schema、retention 或日归档逻辑。

## 阶段 1：本地修复与验证

### 任务 1.1：先固定失败行为

- 步骤 1.1.1：本地假 WebSocket 第一条连接发送 raw/combined `serverShutdown`，第二条连接发送合法 aggTrade。
  - 验证：修复前 raw 控制帧进入业务队列；修复后两种控制帧都不进入队列，第二条连接收到 aggTrade。
- 步骤 1.1.2：第一条连接发送 `Close` 或直接 EOF，第二条连接发送合法 aggTrade。
  - 验证：覆盖官方 24 小时断开和无 close handshake 路径。
- 步骤 1.1.3：第一条连接持续发送 Ping 但不发送业务消息。
  - 验证：Ping 不重置业务 freshness deadline，timeout 后第二条连接收到 aggTrade。
- 步骤 1.1.4：构造断开的 writer channel 和 writer parser 错误。
  - 验证：发送阶段返回 `BrokenPipe`；shutdown/finalize 阶段返回 writer error，均不得成功退出。
- 步骤 1.1.5：构造已完成的 source task。
  - 验证：运行期监督函数能返回对应 source kind。

### 任务 1.2：实现最小生产修改

- 步骤 1.2.1：业务 parser 前读取 raw/combined payload 的 `e`；`serverShutdown` 不进入业务队列并立即重连。
- 步骤 1.2.2：`Close`、EOF、错误和 freshness timeout 使用 `500ms` 起、最大 `5s` 的退避重连；freshness 只由合法业务消息刷新，Ping/Pong 不刷新。
- 步骤 1.2.3：主循环每次事件轮询后检查 source task 是否提前结束；异常时写 gap、完成受控收尾并返回非零。
- 步骤 1.2.4：writer channel disconnected 和 shutdown/finalize error 通过 `Result` 或 process failure 返回主循环，使现有 systemd 重启策略可接管。

### 任务 1.3：本地质量门

执行：

```bash
cargo fmt --all -- --check
CARGO_TARGET_DIR=target/data-collect-ci cargo test -p data-collect --lib --tests -- --nocapture
CARGO_TARGET_DIR=target/data-collect-ci cargo clippy -p data-collect --all-targets -- -D warnings
git diff --check
```

验收：所有命令退出码为 `0`；故障测试明确覆盖 raw/combined `serverShutdown`、24 小时 `Close`、EOF、持续 Ping 无业务消息、source task 完成、writer disconnected 和 writer parser/finalize error。

### 任务 1.4：阶段审查与发布源码

- 自审 diff、测试证据、需求/计划/changelog 对齐和无关文件排除。
- 派生一个 `gpt-5.6-sol`、`ultra` 子代理审查正确性、遗漏、过度设计和部署风险。
- 审查通过后只 stage 本事项文件，创建 commit 并 push 当前分支。

## 阶段 2：构建、部署与运行验收

### 任务 2.1：从已提交代码构建 release

- 在本地干净临时 worktree 使用阶段 1 commit 构建，禁止在远端 cargo 编译。
- 使用唯一 release id，记录 commit、构建时间、binary sha256 和 release 路径。
- 验证 release 内 `SHA256SUMS` 全部通过。

### 任务 2.2：上传与切换

- 记录远端旧 `current` symlink、旧 binary sha256、服务 PID 和启动时间。
- rsync 新 release 到 `~/btc-updown-marketmaker/data-collect/releases/<release-id>/`。
- 远端执行 `sha256sum -c SHA256SUMS`，通过后原子切换 `current`。
- `systemctl --user restart data-collect-training-collector.service`，不得重启整台服务器。

### 任务 2.3：运行验收

必须同时满足：

1. service 为 `active/running`，PID 与旧进程不同，`ExecMainStatus=0`。
2. `current` 指向新 release；运行 binary sha256 与本地发布值一致。
3. 进程存在六个 writer 线程，`binance_aggtrade` 不再停留在旧四字节文件。
4. 新 run 在当前 UTC 小时创建 aggTrade running artifact；通过短时观察或受控 finalize 证明产生可读、非空 aggTrade parquet。
5. journal 没有 writer parser error、source task 意外结束或重启循环。
6. 其他连续来源仍在推进；事故文件保持原状并从 finalized 数据发现路径排除。

### 任务 2.4：回滚

任一部署验收失败时：停止 service，切回任务 2.2 记录的旧 release，校验旧 `SHA256SUMS` 后重新启动；保留新 release、journal 和失败文件作为证据，不删除生产数据。

## 最终验收

- 本地测试与独立审查通过。
- 修复 commit 已 push。
- 数据采集服务器只接收本地构建二进制，没有远端编译。
- Binance 24 小时断开、`serverShutdown` 和普通网络断线均可继续采集。
- source/writer 静默退出不再让主进程伪装 healthy。
- 新 aggTrade 数据恢复落盘；历史事故文件和缺口没有被伪装为 complete。

## 执行结果

- 修复 commit `5c0d8907b54be9f8a9a1ef3ccf95fd4de98810c6` 已推送到 `origin/data-collect-pmxt-hourly-retention`。
- 本地从该 commit 的干净临时 worktree 构建 release `5c0d8907b54b-aggtrade-20260710T062005Z`；训练 binary sha256 为 `8279b338ffbb2f9bf5ec480df458cdea9569c0dad400a0320c63f62bbf53efff`。
- 远端 `sha256sum -c SHA256SUMS` 全部通过；旧 `current` 为 `train-data-collect-ver1-0`，已切换到新 release。远端没有运行 Cargo 命令。
- 首次重启后 PID 从 `922846` 变为 `942675`，六个 writer 全部存在；观察后再次受控重启完成短段 finalize。
- finalized `binance_aggtrade_2026-07-10T06.parquet` 为 `17191` bytes，头尾均为 `PAR1`；对应 **segment manifest** 为 `Complete`、`row_count=789`、`aggtrade_rows=789`、`source_gaps=[]`。独立解析确认 1 个 row group、789 行、无 null，aggregate trade id 从 `4010919603` 连续到 `4010920391`。
- 该段所属短 run `training-longrun-20260710T062226Z` 只运行约 `111s` 后为验证 finalize 而受控停止；其 **run manifest** 为 `SOURCE_GAP`，仅包含 `insufficient_polymarket_market_windows: observed=2 required=20` 和 `insufficient_rollover_edges: observed=0 required=1`。这是长跑覆盖门槛未满足，不是 aggTrade segment 缺口；本计划不把该短 run 表述为整体 PASS。
- 当前 PID `942838`，`ActiveState=active`、`SubState=running`、`ExecMainStatus=0`、`NRestarts=0`；30 秒稳定性检查 PID 未变化，六个 writer 仍存在，journal 无相关错误。
- 当前 aggTrade writer 已打开 `binance_aggtrade_2026-07-10T06--part=1-running.parquet` 继续采集；旧 T08 事故文件仍为 `4` bytes，未修改。

## 外部依据

- Binance WebSocket Streams：<https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md>
- Binance API Changelog：<https://github.com/binance/binance-spot-api-docs/blob/master/CHANGELOG.md>
