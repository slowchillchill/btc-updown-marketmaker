# data-collect 当前主需求

> 事项编号：`DATA-COLLECT-CURRENT`
>
> 生命周期：`CURRENT_MASTER_DEMAND`
>
> 当前执行 authority：`data-collect/docs/CURRENT.md`
>
> 对应计划：`plan/data-collect-plan.md`
>
> 对应 changelog：`changelog/data-collect-changelog.md`
>
> 状态：`COMPLETE_CURRENT_CONTRACT`

## 1. 目标

提供一个 SDK-first、因果可审计、可恢复且有界的外部市场数据采集模块，为训练写入
typed raw，并为 future paper/live 在线接入提供隔离的 binary 与版本化 handoff
基础；不得侵入训练加工、交易执行或账本职责。

## 2. 模块范围

1. training binary 采集 Polymarket market/metadata/RTDS 和 Binance
   depth snapshot/update、aggTrade 六类数据。
2. paper/live binary 与 training 分进程、分输出根、分部署角色；当前只提供 startup
   receipt 与 Unix socket heartbeat/status。
3. `data-collect` 不负责 labels、features、训练切分、模型输入、订单、撤单、钱包、
   NOE、ledger、PnL 或策略。
4. 当前只有 training 连接真实外部源。paper/live 真实 adapter 不属于
   `model-train`，继续保持延期。等下一模块 `model-train`（模型训练模块，不是
   `data-collect-training-collector`）完成之后，只有在用户决定返回 `data-collect`
   时，才另立 demand/plan/changelog 开发；该延期不阻断当前模块完成。

## 3. SDK 与数据合同

1. Polymarket 必须使用官方 Rust SDK `0.7.0` 和仓库唯一 `0.7.0-p1` patch；SDK
   缺口必须先报告并取得用户决定，禁止未经批准的 raw REST、manual signing 或
   shadow SDK。patch 的四文件 production surface 及解析、诊断、连接和代际边界由
   `current/contracts.md` 与 patch/runtime contract tests 拥有，不得只凭版本名删除、
   缩小或扩大。
2. current new-write schema 固定为 orderbook v3、metadata v5、RTDS v4、Binance
   depth snapshot v3、depth update v4、aggTrade v7。
3. 旧 schema 只能用于代码明确保留的 exact legacy read/recovery，不得成为
   new-write authority。
4. UTC 日和小时必须按 collector receive time 分区；`*.running` 不得被当作
   finalized artifact。
5. 每个 UTC 日只有一个 daily manifest v3 和一个 owner，并直接包含单一
   collection-day coverage v1。

## 4. Runtime 合同

1. collector-owned data-bearing、admission、deferred 与 writer queue/batch 必须有
   memory hard bound；已接纳数据在可靠停止、rollover 或运行期容量退出时必须
   drain/finalize。vendored SDK transport-control 的 outbound request/heartbeat
   ping channel 使用 unbounded channel type；当前 wrapper 的
   latest-desired coalescing、固定小 desired set 和单 outstanding heartbeat 只构成
   逻辑调用约束，不等同于 channel hard capacity。
2. configured capacity limit 必须为正整数；CLI 默认值和当前 production unit 为
   `50_000_000_000` bytes，测试或另行批准配置可以覆盖。startup scan 已达到
   configured limit 时，必须在 source/typed raw/manifest/runtime artifact 启动前
   直接退出 `75`；运行期间每 `60s` scan 达到 configured limit 时，必须停止
   admission、drain/finalize、更新适用 manifest 后退出 `75`。collector 不自动删除
   finalized raw。
3. 普通 source 静默或暂时无数据不得退出整体进程；transport failure 由对应 owner
   恢复。
4. training RTDS 保留唯一业务静默窄例外：首次 fresh 前禁用 watchdog，随后
   steady 300s / first-fresh 60s 触发完整 runtime replacement；outer backoff base
   从 1s 起每级 `×2`、应用 `±20%` deterministic jitter、单次 cap `300s`，fresh reset，
   replacement episode 无独立 attempt/累计 elapsed exhaustion，严格
   break-before-make。
5. market fixed identity 必须在 current/next/restart/resolution 中保持不可变；多
   authority、immutable mismatch 或 schema contradiction 必须 fail closed。
6. resolution follow-up 固定 E+3 至 E+8 六个 slot；qualified terminal 只能关闭一次，
   exhausted 不得伪造 winner。不匹配 current/next/pending role 或超过 pending
   budget horizon 的 late `market_resolved` 被忽略；第二个 Gamma terminal claim、
   fixed-identity/schema contradiction 和已有 terminal winner 冲突必须 fail closed。
7. UTC rollover 必须先完成旧日 producer fence、writer finalize、manifest
   rendezvous 和 durable close，再 activation 新日。

## 5. 构建与权限边界

1. release binary 必须在本地从 clean、已 push commit 隔离构建，记录 commit 与
   SHA-256；远端不编译 Rust，不通过服务器 `git pull` 部署。
2. current training binary、dataset root 与唯一 systemd unit 路径由
   `data-collect/docs/current/operations.md` 固定。
3. current 文档只描述合同，不授权上传、部署、启停服务、替换 binary、删除数据或
   改变远端状态；这些动作需要独立批准的活动三件套。
4. release bundle、binary alias、retention admin、archive uploader 和自动 raw
   删除不是当前部署路径。

## 6. 完成与未声明事项

1. 当前模块已经实现、测试并由用户验收。
2. 2026-07-28 事故已最终验收；真实 UTC rollover 与完整健康 UTC 日证据不声明为
   已取得，也不再是 completion gate。
3. paper/live 真实 external-source adapter、runtime receive artifact、
   authoritative unit 与 production deployment 尚未实现。
4. 当前没有 Rust 实现或生产部署游标；未来变化必须新建事项并重新验证 current 代码、
   artifact、环境与外部依赖。

## 7. 验收标准

1. 三个 binary 的职责、CLI 和角色派生输出根与当前 Cargo/代码一致，只有 training
   连接真实源；hot run-id 尚无安全文件名组件校验的限制必须显式保留，不得冒充
   receipt confinement 已 fail-closed。
2. 六个 current schema、SDK/patch、hourly Parquet、daily manifest、capacity 退出、
   RTDS、identity、resolution、restart 和 rollover 合同均有测试证据。
3. paper/live current/future 能力不被夸大，延期不变成隐形执行计划。
4. `data-collect/docs/CURRENT.md` 是唯一 current 入口，六个主题 owner 无影子规范。
5. 45 份历史长文可由 archive tag/path/blob 恢复，但不在活动树，也不产生当前
   authority。
6. 文档 authority guard 能拒绝重复入口、坏三件套引用、坏 archive blob、恢复历史
   路径或 evidence-only banner 缺失。
7. 当前验收命令和实际结果记录在
   `data-collect/docs/current/acceptance.md`。
