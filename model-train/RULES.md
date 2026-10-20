# 新B进程规则来源与恢复

本段按#181任务4前置工程接入D-0080/D-0047已采用的CURRENT_QUERY。D-0367明确角色、D-0371学习事务继续保持；没有改模型、动作、奖励或学习方法。

`model_consumer run`和`model_learner run`在启动worker、learn_open、SIM账户和业务前解析一次规则。CURRENT_QUERY复用原公共CLOB/Gamma加载器，原多个GET属于一次完整冷启动事务，有限重试及30秒初始化上限保持。失败直接返回，不回退历史规则。无凭据、端点和强类型响应仍由sim-system维护，DPT事件只提供历史市场事实。

新查询封存源规则后，`derive_public_maker_from_sealed_v3`在同一已验证句柄上显式派生所选角色，保留原平台值、来源/观测时间及源SHA。原历史派生包装及HistoricalNotApplicable检查保留。ModelTrain/历史比较/独立评价各用自己的真实角色，PAPER不变。

同一Arc最终规则进入所有日账户、学习采集和每轮公共再生；不按账户或epoch重新加载/查询。backtest增加接受同句柄的visitor，继续原Parquet流与公共统计实现；旧A和旧显式历史入口保持原路径。显式历史句柄仍核对原初始化前史截止；CURRENT_QUERY/RECOVERY不套历史来源门。

## 完整配置与实际产物

[消费者示例](examples/current-query-consumer.json)和[学习示例](examples/current-query-learning-job.json)是可由CLI完整解析的配置形状。reference_market含condition/clob_market_id/slug及两个outcome身份。示例使用既有合法平台形状，不声称是当前真实研究输入；运行前必须提供本轮显式参考市场、实际程序/输入/模型路径和准确实现哈希。零串实现身份是待替换占位，不是已核实版本。

rules接受以下明确模式：

- `CURRENT_QUERY`：指定reference_market、source_snapshot_path、最终snapshot_path、effective_config_path、implementation_identity和public_maker_roles。新进程查询一次；两个封存文件及有效配置不覆盖既有文件。输出目录须预先准备。
- `HISTORICAL_AS_OF`：指定snapshot_path、expected_file_sha256，保持原来源不晚于合法回放起点的判断。原无mode的`{snapshot_path, expected_file_sha256}`配置仍表示既有显式历史模式，不默认为当前研究规则。
- `RECOVERY`：binding直接采用原有效配置。只读取原最终快照，核对原SHA/实现及必要元数据，不联网、不重新派生。恢复不是给新运行绕过冷启动的方式。

CURRENT_QUERY成功后、worker启动前，effective_config_path落盘完整规范化ConsumerRunConfig。其rules为可恢复的RECOVERY表示，binding中source_mode仍是原CURRENT_QUERY。绑定包含最终path/SHA、源查询path/SHA、观测时刻、参考市场、静态配置版本、实现和实际角色政策。原源文件及最终快照都应保留。此文件是有效配置产物，不是第二份消费/经济账本。

消费者采集轨迹先写一次rules_binding记录，每条真实样本引用最终SHA；不逐帧重复整份规则、不新增运行时哈希。组绑定使用规范化原配置，原模式与本次打开方式分开。已完成采集及Python checkpoint的完整group/consumed协议继续使用同一身份，不因CURRENT_QUERY切RECOVERY制造新组。

公开规则上下文来自封存内容和文件SHA，不包含句柄本次access_mode。恢复打开方式不会变成模型特征。当前进程中调用多次原日作业/再生时复用同一已打开句柄；独立新进程的新运行重新查询。

## 新运行与恢复命令

以下仅说明调用方法，不签发实网、数据或模型作业。普通参数调整无需重编译。

```bash
model_consumer print-config current-consumer.json
model_learner print-config current-learning-job.json
model_consumer run current-consumer.json new-summary.json
model_learner run current-learning-job.json
```

CURRENT_QUERY已经写出有效配置后，消费者恢复规则使用该原文件：

```bash
model_consumer run runs/example/effective-consumer.json new-recovery-summary.json
```

这只复用规则，不提供准确日内账户续跑。学习恢复保留原job的组、参数、路径、账户及全部输入，仅替换为原规范化consumer并给本次新回执路径：

```bash
python3 - <<'PY'
import json
job = json.load(open('current-learning-job.json'))
job['consumer'] = json.load(open('runs/example/effective-consumer.json'))
job['attempt_report'] = 'runs/example/attempt-recovery.json'
with open('recovery-learning-job.json', 'w') as output:
    json.dump(job, output, ensure_ascii=False, indent=2)
PY
model_learner print-config recovery-learning-job.json
model_learner run recovery-learning-job.json
```

不要把completed-collection中的已注入采集路径/截止配置整体代替原consumer；使用业务前有效配置产物。采集未完成仍从原单元起点重做；完成采集复用、未提交整组重学、已消费只确认均按原事务。旧fixture、旧checkpoint及原成本记录保留原身份，不自动迁移成CURRENT_QUERY成果。

本段验证仅用原ScriptedServer、合法合成Parquet、CPU直接输入比较和无torch的受控worker协议。它验证启动顺序、同源再生、恢复及角色；不提供新GRU/PPO、真实经营、实网规则成功率或全日容量证据。原14次更新/6次提交上限及已结束预算未借用，后继30分钟仍是待签提案。
