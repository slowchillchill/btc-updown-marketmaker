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

1. 模拟盘和实盘的真实 Polymarket、RTDS、Binance 适配器与生产者接线尚未实现。
2. 模拟盘和实盘当前不输出真实 `external_market_event`；只输出心跳、状态。
3. 模拟盘和实盘的运行时接收产物、权威 systemd 单元和生产部署尚未实现或验收。
4. 模拟盘和实盘的帧、schema 与一次性 Unix 套接字辅助函数有组件测试，但完整热路径
   二进制程序从启动回执到长运行心跳服务端的路径尚无端到端测试。
5. 热路径 `--run-id` 当前只校验去除首尾空白后非空，并原样参与
   `<run-id>.startup_receipt.json` 路径拼接；含路径分隔符或类似父目录的内容时，回执
   可能离开角色派生的交接根目录。当前代码尚未对这种不可信输入提供失败即关闭保证，
   任何后续部署必须先另立事项修复并补负例，不能把输出根目录限制写成已验收。
6. 模拟盘和实盘真实适配器不属于 `model-train`，并继续保持延期。必须先完成
   `model-train`（模型训练模块，不是 `data-collect-training-collector`）；之后仍
   只有在用户决定返回 `data-collect` 时，才另立需求、计划和变更记录开发，并重新核对
   当前代码和外部环境。
7. 真实 UTC 日切换与完整健康 UTC 日运行证据不作已取得声明，但不再阻断当前模块完成；
   确定性 UTC 日切换合同测试仍是当前验收的一部分。
8. 具名吞吐测试夹具在 UTC 12:00 前有实际时钟限制；八小时 RTDS 公开探测默认被忽略，
   详见 `current/acceptance.md`。
9. 采集器不自动归档或删除最终化原始数据；退出 75 后的任何迁移、归档或清理都
   需要独立授权。
10. 当前文档描述代码合同，不证明任一远端服务的实时状态。需要远端事实时必须另行
    进行只读核实。
11. 供应商版 SDK 的出站请求与 `heartbeat PING` 传输控制通道使用无界通道类型。
    当前封装器通过合并最新期望状态、固定且较小的期望状态集合，以及单个待处理心跳来
    约束调用量，但没有内存硬上限；不得把采集器数据队列的硬上限泛化到这些 SDK 控制通道。
12. 本轮没有修改 Rust 运行时、schema、交易逻辑或远端状态；文档整理不能替代未来代码、
    数据、产物与运行环境验证。
