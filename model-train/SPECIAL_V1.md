# D-0375 专项信用与冻结模型

专项信用只用于学习。它不改变现金、持仓、费用、返佣、动作域、Actor输入或SIM时序。
经营收益、信用数值与工程接线验收分别判断。

## 显式选择与兼容

`LearningParameters.special`及采集的`CollectionConfig.special`显式选择D-0375公式及完整生效参数。
`version=D-0375-special-v1`，`alpha=0或1`，`frame_age_threshold_ms=100`，
`safe_notrade_credit_pusd=0.01`，`repair_credit_per_share_pusd=0.01`，`credit_bound_pusd=0.10`。
这些参数是采用的首版数值，不是研究选优结果，也不是交易超时或宿主代选政策。
程序接受配置中的精确数值，保留公式版本、alpha取值及B非负的数学边界。
配置能力不授权其他数值作业；改变数值仍受具体作业授权约束。
无`special`时不写null字段，保持原base配置、`confirmed-finite-j-v1`及`learning-trajectory-v2`身份。
显式专项两臂均使用`confirmed-finite-j-special-v1`和`learning-trajectory-special-v1`；
完整参数进入原binding、checkpoint配置及consumed比较。旧base优化器不热迁移到专项。
检查点存放位置仍只沿PR355的三个准确投影边界排除。

参照配置见`examples/special-learning-parameters.json`与`examples/special-learning-job.json`。
配置中的路径、输入、程序身份与版本须由实际作业替换；示例不签发查询、训练或评价。
两臂必须从同一显式初模各自建立fresh Adam与独立提交目录，不把alpha当成同一学习器内可变参数。

## 原事实与实际受理

Ledger在首次合法应用FinalSuccess事实之前保存原净份额参照。
净份额来自原官方终局量索引，扣除原实际配对与孤腿赎回。它不等待经济成本池的`success_applied`旗标。
资产顺序用于保存有符号净份额和成交符号；两者同时翻转不改变绝对风险公式。
后继数量修正替换同一trade贡献，补偿使有效量为0，费用修正不重锚。
现有同版本历史记录若没有首锚，读取保持未知；不以当前净量猜测历史。

ModelConsumer通过原`dispatched`结果确认零意图NoTrade受理或Repair真实发送回执。
跨E未接受的决定不授信；`actual_intents`不能独自证明发送。
安全NoTrade覆盖整个账户的原净量、旧订单和可信在途义务，不限于当次观察市场。
全账户已知风险非零即可判定不安全；必要事实未知且没有已知非零风险时保留安全证明未知。
未知不阻止合法动作，但需要该证明的专项标签保持未就绪。每独立日账户只授一次安全NoTrade信用。

新定向事实只在显式专项learning读取中请求。
普通TradeEconomicRead、market_private和原Actor编码不增加锚点、贡献或归属身份。
未变化公共净量共享同一Arc；只按保留的专项request定向读取包括闭合订单在内的原成交。

## 前缀、尾部与冻结

每次真实调用只记录必要信用前缀、资格和原可信引用，不复制完整公共观察或逐帧成交历史。
原始累计A保留越界值，只裁剪C。使用`rlearn=rbase+alpha*(Cnext-Ccurrent)`；
最后一条真实转移使用`Cnext=0`，完整Critic目标是`Jend-Kcurrent-alpha*Ccurrent`。
金额先按原精确有理数计算，再投影神经网络浮点值。
N0不生成Actor样本；单转移不造末端动作。H只关闭学习信用，不撤单、赎回或清除经济义务。

`frozen-labels.json`只冻结一次，两轮PPO及恢复读取同一份标签。
`unit-objectives.json`的`special_h`保存H前原信用解释、终止信用0、原NOE/Ledger根、轨迹及最后真实转移引用。
`special-confirmation-changes.jsonl`按原Ledger提交顺序保存必要trade变化的原NOE持久引用和position。
原读取逐页处理提交段，不重跑SIM，也不将完整历史载入内存。
因此last到H之间成功、数量修正与补偿即使净抵消，仍可按原位置读取每条原事实。
这些原文件和引用属于采集材料，不能当临时构建缓存删除。
若真实P_i的信用/K及base J已知且正常到H，只有last到H新出现的raw解释未知不进入训练标量。
此时special_h保留raw/clipped为null及原未知原因，数学终端C_H仍为0；不宣称全部信用事实已算清。
真实P_i必要信用未知、base J未知、未正常到H或原来源读取失败仍阻止对应标签。

## 正式冻结导出

```bash
python export_frozen.py export-config.json --print-config
python export_frozen.py export-config.json
model_consumer print-config consumer-config.json
model_consumer run consumer-config.json frozen-summary.json
```

`examples/frozen-export.json`给出完整配置形状。
导出直接以`weights_only=True`在CPU读取完整已提交checkpoint的tensor字典。
它不构造Policy，不设置或恢复RNG，不运行前向、优化器或学习提交，也不修改源checkpoint。
输出消费者包保存原模型tensor，绑定源程序commit/源码身份、checkpoint SHA、提交序号、shape、编码、目标及轨迹版本。
消费者沿原strict state_dict装载，并在模型身份中交回`frozen_source`。
专项完整数值保存在原checkpoint的配置及group绑定中，通过源checkpoint SHA追溯；
消费者包不另存一份完整数值配置，不能只凭目标版本推断具体参数。
已存在输出不覆盖；实际来源和新输出须保留完整配置与SHA。

导出的协议/源码测试不证明真实tensor导出已执行。
实际Torch初始化、装载、tensor逐项核对、前向和学习都必须列入当轮明确工作量与预算。
