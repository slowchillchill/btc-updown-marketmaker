# data-collect 重复 typed 列删除执行计划

> 事项编号：`DC-REPAIR-20260728-REDUNDANT-TYPED-COLUMN-REMOVAL`
>
> 对应需求：`demand/data-collect-redundant-typed-column-removal-demand.md`
>
> 对应 changelog：`changelog/data-collect-redundant-typed-column-removal-changelog.md`
>
> 状态：`APPROVED_STAGE0_CLOSED_ON_PUBLISH_STAGE1_NEXT_ATTEMPT5_USER_TERMINATED_INCOMPLETE_8H`

## 阶段 0：停止旧测试并同步 authority

### 任务 0.1：受控停止 Attempt5

- 步骤 0.1.1：核实 collector PID、父脚本、进程组和 dataset root，再向精确进程组
  发送 `SIGINT`。
  - 验证：collector、wrapper 和 tee 均退出；不影响其他进程。
- 步骤 0.1.2：核实 wrapper result、manifest coverage、finalized 文件和临时文件。
  - 验证：记录真实 exit、起止、时长、文件数/bytes；`.running` 为零；旧 root
    保持只读。

### 任务 0.2：建立并同步文档 authority

- 步骤 0.2.1：创建本事项 demand/plan/changelog，冻结 v7/v5、legacy 和范围边界。
  - 验证：三份文件使用同一事项编号并互相引用；需求和每项验收都有计划步骤。
- 步骤 0.2.2：同步当前 data-collect、source-availability、resolution-followup、
  SDK070 umbrella 与 RTDS Attempt 游标。
  - 验证：旧 v5/v6、v3/v4 历史语义不被回写；新文档只精确取代 future
    current new-write；Attempt5 固定为用户终止，Attempt6 成为唯一长跑游标。
- 步骤 0.2.3：自审并派生独立 `gpt-5.6-sol`/Ultra 文档审查。
  - 验证：无上下文冲突、遗漏 authority、过度校验或范围扩张；问题闭环后
    commit/push。

阶段 0 完成条件：停止证据与新 authority 一致，文档审查通过，阶段 0 commit 已
push 到 `origin/fix-rtds-silents`。

## 阶段 1：实现 `binance_aggtrade_v7`

### 任务 1.1：先冻结失败/兼容测试

- 步骤 1.1.1：更新 schema contract，断言 v7 不含
  `websocket_timestamp_received`，其余 v6 字段和 nullability 不变。
  - 验证：旧代码下测试先失败于目标字段仍存在。
- 步骤 1.1.2：冻结 v5/v6 真实物理兼容 fixture，禁止用 current writer 加旧标签
  伪造 fixture。
  - 验证：v5 的 WS/REST/merged 语义及 v6 的 trade/invalid 语义可独立恢复。
- 步骤 1.1.3：增加 current writer 版本负例。
  - 验证：用 v5/v6 标签请求 current aggTrade writer 必须失败。

### 任务 1.2：手术式删除字段

- 步骤 1.2.1：把 current 版本升为 `binance_aggtrade_v7`，从 row、三个构造器、
  validator 和 Arrow schema 只删除目标字段。
  - 验证：`timestamp_received` 与其他时间/排序字段语义不变。
- 步骤 1.2.2：删除 output alias method、source raw-event 重复键、writer parser
  合成/读取和 Parquet batch array。
  - 验证：trade 与 invalid input 均不再生成目标字段，剩余 schema/array 顺序一致。
- 步骤 1.2.3：收紧 writer dispatch 为精确 current schema version，并让 footer、
  embedded metadata、published result 和 manifest fact 复用同一版本常量。
  - 验证：不存在“v7 物理布局 + v5/v6 标签”。

### 任务 1.3：验证与阶段审查

- 步骤 1.3.1：运行 aggTrade layout、writer、handoff、startup recovery 定向测试。
  - 验证：v7 trade/invalid round-trip 通过，v5/v6 recovery 通过。
- 步骤 1.3.2：运行受影响 package tests、check、严格 Clippy、fmt、diff check。
  - 验证：全部通过，无无关改动。
- 步骤 1.3.3：自审并派生独立 `gpt-5.6-sol`/Ultra 阶段审查。
  - 验证：问题闭环后同步 changelog、commit/push。

阶段 1 完成条件：需求验收 1、3、4、6、8 中 aggTrade 部分通过，阶段 1 commit
已 push。

## 阶段 2：实现 `polymarket_market_metadata_v5`

### 任务 2.1：先冻结失败/兼容测试

- 步骤 2.1.1：更新 schema contract，断言 v5 等于 v4 删除
  `resolution_evidence_raw_sha256`，claimant pair 保持末尾和原语义。
  - 验证：旧代码下测试先失败于目标字段仍存在。
- 步骤 2.1.2：冻结 v3/v4 exact schema、owned terminal digest 旧规则和 legacy
  recovery fixture。
  - 验证：旧版本仍要求并校验旧列；v5 不使用该列。
- 步骤 2.1.3：增加旧标签 new-write、unknown version 和 v5 raw-event 仍出现被删
  字段的负例。
  - 验证：current writer/version dispatch 精确 fail closed；不新增跨文件
    lifecycle 版本混用门。

### 任务 2.2：删除 current 重复 digest

- 步骤 2.2.1：新增 exact metadata v5 version/schema，物理字段顺序为 v4 删除目标
  字段，其余不变。
  - 验证：v3/v4 schema identity 不变。
- 步骤 2.2.2：从 current persisted/raw-event 合同、validator、writer batch 和
  memory-size accounting 删除目标字段及其 required/null/equality 校验；若复用
  crate-private legacy prefix carrier，其旧 slot 必须恒为 `None`且永不序列化。
  - 验证：source payload/source digest 校验仍在唯一 owner 执行；terminal
    ownership、winner/outcome 和 claimant 约束不变。
- 步骤 2.2.3：扩展精确 MetadataLayout/version dispatch、v5 reader/recovery 和
  current writer，保留 v3/v4 legacy 路径。
  - 验证：同一 v5 常量传播到 footer、embedded recovery metadata、published
    result 与 manifest per-file fact，并拒绝“v5 物理布局 + v3/v4 标签”；不复制
    整个通用框架、不增加第二 writer。
- 步骤 2.2.4：保留既有 AC-12 validation artifact 不变，新增 v5 repo-local
  contract validator/test 与受影响 fixtures。
  - 验证：v5 检查目标列不存在并用 source digest 验证 payload；冻结的 source v2
    + metadata v3 AC-12 artifact/validator 仍按旧规则且哈希不被修改，既有 v4
    fixture/Attempt5 文件仍可按 v4 exact schema 读取。

### 任务 2.3：验证与阶段审查

- 步骤 2.3.1：运行 metadata layout、writer、event adapter、source、resolution
  recovery、claimant matrix 和版本传播定向测试。
  - 验证：五类 row、Gamma/WS terminal、local reference、restart 均通过；footer、
    embedded metadata、published result 与 manifest per-file version 均为 v5。
- 步骤 2.3.2：运行受影响 package tests、check、严格 Clippy、fmt、diff check。
  - 验证：全部通过，无行为扩张。
- 步骤 2.3.3：自审并派生独立 `gpt-5.6-sol`/Ultra 阶段审查。
  - 验证：问题闭环后同步 changelog、commit/push。

阶段 2 完成条件：需求验收 2--6、8 中 metadata 部分通过，阶段 2 commit 已 push。

## 阶段 3：完整六源 Attempt6 与收口

### 任务 3.1：构建新验证入口

- 步骤 3.1.1：从 clean/pushed HEAD 在隔离 target root 构建 release training
  collector，记录 commit、命令、binary SHA-256 和空 dataset root。
  - 验证：HEAD、upstream、binary provenance 一致；不复用 Attempt5 root。
- 步骤 3.1.2：使用现有香港线路与冻结 RTDS 参数启动完整六源 Attempt6，
  `--max-runtime-secs 28800`。
  - 验证：启动时 root 为空，正式 Parquet/manifest writer 生效。

### 任务 3.2：运行与结果验证

- 步骤 3.2.1：持续运行至正常退出或真实错误。
  - 验证：若出现新 parser/schema/runtime 事实冲突，停止并按项目决策门报告；
    普通健康运行不请求额外授权。
- 步骤 3.2.2：验证六源 finalized 文件、schema/footer/version、manifest/file
  rows/bytes/SHA、PyArrow 可读性和零临时残留。
  - 验证：aggTrade v7 与 metadata v5 目标列均不存在，其余语义满足需求。
- 步骤 3.2.3：报告 RTDS 实际 replacement/自然重连/error 与最长 fresh 间隔，
  不把无事故窗口冒充因果效果。
  - 验证：Attempt1--5 历史终态与 Attempt6 结果并列记录。

### 任务 3.3：最终审查与发布

- 步骤 3.3.1：同步 demand/plan/changelog 状态和真实验证证据。
  - 验证：三件套、SDK070 umbrella 和 RTDS 游标一致。
- 步骤 3.3.2：自审并派生独立 `gpt-5.6-sol`/Ultra 全面终审。
  - 验证：需求、计划、代码、测试和 artifact 一致；问题闭环。
- 步骤 3.3.3：创建最终 commit、push 并核对本地 HEAD、upstream、远端 ref。
  - 验证：三方 SHA 一致。

阶段 3 完成条件：需求全部验收通过；若 Attempt6 暴露新的真实冲突，则按决策门
暂停而不伪称完成。
