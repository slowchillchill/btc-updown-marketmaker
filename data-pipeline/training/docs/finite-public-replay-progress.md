# 有限公共重放的阶段与工作量进度

本段按 [#214](https://github.com/slowchillchill/btc-updown-marketmaker/issues/214) 的正式进度分配，为原 `PublicReplayV1` 增加可选的单次重放进度。`RecordedPublicGroupSourceV1` 只转交配置，公共统计与两市场／首小时研究仍消费原完整组。没有新增读取入口、数据准入条件或业务选择规则。

## 配置与调用

旧 `open` 调用默认关闭进度。库使用者在消费前调用 `set_progress(Duration, output)`，其中 `output` 是该次重放拥有的 `Write + Send + 'static` 输出；零间隔关闭。计数和单调耗时随本次重放释放，没有线程、历史事实副本或跨运行共享。

实际工具默认每 30 秒输出到标准错误，可用前置参数调整或关闭：

```text
material_trial [--progress-interval-seconds <SECONDS>] <PREPARATION_JSON> <REPORT_JSON>
research_causal_input [--progress-interval-seconds <SECONDS>] <CONFIG_JSON> <REPORT_JSON>
research_causal_input [--progress-interval-seconds <SECONDS>] --first-fit-hour <CONFIG_JSON> <REPORT_JSON>
```

`SECONDS=0` 关闭。旧位置参数继续有效；结果报告记录 `progress_interval_seconds`。纯已保存窗口统计模式 `--retained-windows` 不读取公共记录，因此没有重放进度。

间隔限制普通重复检查点的格式化和写入。每种阶段第一次出现、首次依赖完整验证结束、EOF 或原错误可立即打印，不需要等待间隔；这不是每次切换阶段都打印。阶段内的检查点合作式执行，单次文件打开、解码、准备或 core 调用尚未返回时，不保证固定频率心跳。输出不刷新业务时钟，不影响同毫秒顺序。

## 输出口径

每行是 `schema=finite_public_replay_progress_v1` 的 JSON。`elapsed_seconds` 从显式启用开始用单调时钟计量，不是原始数据覆盖时长。阶段对应当前合作式检查点：

| stage | 含义 |
|---|---|
| `main_reading` | 原主记录 reader 读取和解析 |
| `dependency_validation` | 非空保存依赖需要的原稳定前缀验证 |
| `candidate_validation` | 临时候选按实际请求推进原验证前沿 |
| `dependency_location` / `candidate_location` | 从原文件有界定位相应参考正文 |
| `core_application` | 原输入准备之后的 core 应用及返回 |
| `eof` | 原 reader 已返回 EOF；是否已应用正常 End 另见标志 |
| `error` | 原重放调用失败；`failed_stage` 是最后到达的阶段检查点 |

阶段没有独立耗时归因，`failed_stage` 也不是内部函数栈定位。没有正常 End 的合法稳定前缀可在库重放中到 EOF，但公共组来源仍沿原合同拒绝把它当正常完成。

`work` 中的计数分开解释，不能混加为单一“已解码量”：

| 字段 | 真实计量单位 |
|---|---|
| `main_records_read` | 主 reader 成功返回的完整存储记录数 |
| `dependency_records_validated` / `candidate_records_validated` | 同一私有原校验前沿分别在两类用途下成功返回的记录数；复用后的已验证记录不再累计 |
| `dependency_location_rows_visited` / `candidate_location_rows_visited` | 定位游标实际访问的原 `SortRow` 数；不是 Arrow 提前解码的整批／行组行数 |
| `dependency_target_records_parsed` / `candidate_target_records_parsed` | 定位目标成功解析为 `PublicStoredRowV1` 的次数；同目标再次请求可再次累计 |
| `applied_inputs` | core 原应用前缀的 `next_input_position`，不等于主 reader 已读记录 |
| `generated_groups` | 启用后原 core 返回的闭合组数，含合法初始化组；不是消费者已交付／处理组数 |
| `current_input_position` | 已从主 reader 取出的当前／最近 Input；首次依赖长读时该 Input 尚未应用 |
| `end_input_applied` | 启用后原 EndOfInput 已实际应用成功 |

只有来自原 `normal_end=true` 稳定前缀的输入总量才写入 `known_total_inputs`，正总量时 `applied_input_fraction` 使用上述原应用位置除以该总量。无正常前缀或总量为零时比例为 `null`。该前缀仍由原消费边界校验；初期看到比例不是额外资格证明。验证／定位总量与 `eta_seconds` 始终为 `null`，不拿参考索引长度、记录字节或阶段编号推算百分比和倒计时。

## 输出故障与验证

输出失败后停止该次日志，不重试输出或业务调用。原成功、错误、EOF、候选选择和完整业务返回继续沿原实现；错误日志不含原业务 stderr 或本机路径。关闭时不创建内部进度计数、时钟或拥有输出的 Box。启用时仅在达到打印条件后构造 JSON，不逐行格式化或复制业务载荷。没有测量生产进度的精确性能开销。

有效小夹具经原 writer 构造，开／关对照完整 Input、完整应用结果与准备 PM 对象；既有完整候选／依赖和历史前缀代表继续覆盖原语义。新增断言核对已知／未知总量、依赖验证时尚未应用当前 Input、坏尾原错误和成功前缀相同、输出故障不能替代业务结果，以及长间隔下重复工作只计数而不逐行打印。固定工作区回归与实际非测试构建核对另见关联 PR 验证记录。

本段没有执行真实材料、prepare 或新十二分钟／小时试验。原始准入、源文件扫描、外排、公共写入和 hidden 派生尚未接入这一进度；它只在原有限重放，以及公共统计／研究读取中生效。旧 179.86 秒是前段有限重放结果，不能当作本次整套流程效果。准备 core 移交及后续单次完整流程测量由 #214 的后继准确分段承担。
