# data-collect 当前限制与未声明事项

> 生命周期：`CURRENT_CONTRACT`
>
> 当前执行 authority：`data-collect/docs/CURRENT.md`
>
> 当前需求：`demand/data-collect-demand.md`
>
> 当前计划：`plan/data-collect-plan.md`
>
> 当前 changelog：`changelog/data-collect-changelog.md`
>
> 历史读取入口：`data-collect/docs/archive-manifest.tsv`
>
> 执行授权：`none`

以下内容是边界，不是隐含“下一步”或执行计划：

1. paper/live 真实 Polymarket、RTDS、Binance adapter 与 producer wiring 尚未实现。
2. paper/live 当前不输出真实 `external_market_event`；只输出 heartbeat/status。
3. paper/live runtime receive artifact、authoritative systemd unit 和 production
   deployment 尚未实现或验收。
4. paper/live frame/schema 与一次性 Unix socket helper 有 component test，但完整
   hot binary 的 startup receipt → 长运行 heartbeat server 路径尚无端到端测试。
5. hot `--run-id` 当前只校验 trim 后非空，并原样参与
   `<run-id>.startup_receipt.json` 路径拼接；含路径分隔符/parent-like 内容时 receipt
   可能离开角色派生的 handoff root。当前代码尚未提供对此的不可信输入
   fail-closed，任何 future 部署必须先另立事项修复并补负例，不能把 output-root
   confinement 写成已验收。
6. paper/live 真实 adapter 不属于 `model-train`，并继续保持延期。等下一模块
   `model-train`（模型训练模块，不是 `data-collect-training-collector`）完成之后，
   只有在用户决定返回 `data-collect` 时，才另立 demand/plan/changelog 开发，并
   重新核对 current 代码和外部环境。
7. 真实 UTC rollover 与完整健康 UTC 日运行证据不作已取得声明，但不再阻断当前模块
   完成；确定性 rollover 合同测试仍是 current 验收的一部分。
8. named throughput fixture 在 UTC 12:00 前有 wall-clock 限制；八小时 RTDS public
   probe 默认 ignored，详见 `current/acceptance.md`。
9. collector 不自动归档或删除 finalized raw；退出 75 后的任何迁移、归档或清理都
   需要独立授权。
10. current 文档描述代码合同，不证明任一远端服务的实时状态。需要远端事实时必须另行
    进行只读核实。
11. vendored SDK 的 outbound request 与 heartbeat ping transport-control channel
    使用 unbounded channel type。当前 wrapper 用 latest-desired coalescing、固定小
    desired set 和单 outstanding heartbeat 约束调用量，但没有 memory hard cap；
    不得把 collector data queue 的 hard bound 泛化到这些 SDK control channel。
12. 本轮没有修改 Rust runtime、schema、交易逻辑或远端状态；文档整理不能替代未来
    代码、数据、artifact 与运行环境验证。
