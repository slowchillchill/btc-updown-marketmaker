# 有限前缀零更新诊断

`model_diagnostic` 比较已完成轨迹的原采集需求与原学习再生需求，不启动学习事务。
它复用原 Parquet 公共 core、统计、冻结规则、`market_projection`、token 编码，以及原
`Service.begin` 和 `LearningEngine.teacher`。它不创建 SIM 会话、账户、订单或新的经济事实。
诊断输入是按原合同再生成的输入，不能冒称当时没有保存的原始 token。

## 入口与授权

```bash
model_diagnostic print-config diagnostic.json
model_diagnostic identities diagnostic.json
model_diagnostic inputs diagnostic.json
model_diagnostic model diagnostic.json
```

`print-config` 只解析配置。`identities` 只读原完成采集、轨迹、初始模型文件及冻结规则身份，
不导入模型、不读取原行情。`inputs` 会启动两条正式公共流，但不启动 Python。
`model` 在合法输入准备完成后才显式启动诊断 Python。原行情和模型作业仍需准确签发；
工程编译和无模型夹具通过不构成执行授权。已有 `run_fixture_budget.py` 可监督任意准确子命令，
总作业预算须先扣准备、失败保留及封存的成本，不能在子命令上限之外叠加时间。

`examples/diagnostic.json` 是完整可解析示例。路径、哈希和停止文件均须显式填写；
占位哈希不能用于实际作业。`completed_collection` 必须是原完成记录；原消费者必须绑定
RECOVERY 最终规则，不接受 CURRENT_QUERY 或以更晚时间替代 Historical 检查。
`trajectory` 必须匹配所选原日/账户的采集路径，所有帧保持原账户、行为模型和规则身份。
只读产物的 SHA 用于已有制品身份核对，不添加逐帧运行哈希或第二套经济账本。
输出目录必须不存在，禁止覆盖原证据、规则、模型或检查点。

## 公共输入与停止

本反例固定比较 `FULL/source_progress=false` 与 `FULL/source_progress=true`。
新窄 typed visitor 接受显式需求与停止回调，旧 visitor/compact 入口继续原 FULL/on 默认行为。
没有通过 observer 或策略健康开关改变生产调度。两路各用一条原公共流，保留原 scope、
合法初始化和规则来源；没有重新经营账户。

先按原市场截止和可信公共组定位帧，再沿原 `market_ids` 顺序，组合原私有投影及实际处理年龄。
报告分别保存公共组首差、公开/私有组合投影首差和 token 首差，包含字段路径与左右原值。
原组不成立或必要市场缺失时，依赖它的模型比较为未知；不替换组、不删字段继续。
查询边界和供料成本仍由原公共实现决定，不声称仅读取2.6秒文件片段即可完成准入。
最后必要查询完成后通过同一停止回调结束公共消费，保留原scope身份；
报告区分计划前缀完成和预算停止，不把提前结束称为EOF或整scope完成。

范围只支持从 call1 起、最多到 call26、原冻结动作全部 NoTrade 且没有条件 RPC 的前缀。
它不支持全部 Pair/Repair 条件头，也不覆盖 call30 的最大差异或完整 PPO 学习正确性。
不支持的动作返回错误，不能转换成 NoTrade。

诊断配置的调用/流/墙钟上限和停止文件只约束本次诊断。每个模型请求在发送前计数并保存
`progress.json`，错误、失败和在途均保留，`report.json` 给未完成结果，不自动重试。
停止文件在阶段边界、顺序定位（`order_location`）控制回调、公共消费边界及模型协议响应接收等待中检查。
既有准入（`admission`）和元数据初始化同步阶段只通知进度，不在其中查询停止回调；
期间收到停止请求，可能要到后续检查点才能正常结束，不能承诺即时正常停止。
候选外部监督器可在300秒上限终止本次整个进程组。若在同步阶段强停，
`progress.json` 可保留已有的 `unfinished` 状态及已发起计数，但最终 `report.json` 可能尚未生成或未完整。
内层停止参数和外部监督都不是生产模型服务、100ms积压或交易政策。

## 三条独立记忆路径

推理路径沿原 `Service.begin` 的 eval/inference_mode；teacher 路径沿原 `teacher` 的
实际 enable_grad/train，不算 loss、不反向、不建立 optimizer、不调用 learn_open 或 commit。
共同输入控制只在两路实际 global/market token 不同时建立，使用推理输入并明确标为反事实。
三路从各自零 hidden 顺序递推，不能注入另一分支的 hidden。所有分支共享不变权重；没有参数更新。

Service.begin 仍真实采样。诊断比较原冻结 NoTrade 索引在完整原 options 中的概率，
不使用新样本动作的 logp。即使新样本为 Pair，也不扩展 RPC 或发送动作；只清除诊断账户
未使用的条件上下文，保留真实 begin 推进的 hidden、调用数和采样 RNG。

编码、市场池化、GRU 输入、前后 hidden、latent、value 和 options logits 来自本次实际调用的
包装/forward hook。trace 不补做前向。Python 专用入口使用原类，生产模型实现没有修改。
原 `audit_step` 的 no_grad/eval 不能替代该实际 teacher 分支。

只有最多26个必要成对输入在内存驻留，私有记录及相同的 global/market token 组件通过 Arc 共享。
报告的唯一 JSON 字节数是序列化负载大小，不等于 allocator 内存；进程 RSS 应以外部监督为准。
持久结果只保存首差及 call24—26 必要投影/token和逐层 trace，不保存完整前缀/日期特征副本。

概率记录以原 trajectory 的 `old_joint_logp` 为参考，诊断输出明确给原冻结动作概率。
原学习使用 frozen-labels 的 old_logp，两者已知约3.55e-15重序列化差分开处理，
不能把它称为新根因或接受数值容差。诊断没有新增零差/固定阈值的业务合同。

## 工程验证与后继

工程阶段只用合法合成 Parquet 与 stdlib 协议替身，真实 Python 仅作 AST/静态审查。
这验证公共需求、输入装配、顺序/组错误、独立分支协议和预算停止；不证明真实 CUDA 数值一致。
初始模型、依赖、程序/脚本版本、完整配置及准确命令在独立交付包中绑定。
后继最多两公共流、26+26+可选26前向、零反向/更新/提交的作业必须另获签发。
