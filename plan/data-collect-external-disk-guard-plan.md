# data-collect 外部磁盘保护执行计划

> 事项编号：`DATA-COLLECT-EXTERNAL-DISK-GUARD-20260807`
>
> 生命周期：`APPROVED_ACTIVE_PLAN`
>
> 当前执行 authority：`plan/data-collect-external-disk-guard-plan.md`
>
> 对应需求：`demand/data-collect-external-disk-guard-demand.md`
>
> 对应 changelog：`changelog/data-collect-external-disk-guard-changelog.md`
>
> `data-collect` 当前权威入口：`data-collect/docs/CURRENT.md`
>
> 工作树：`/home/chillchill/data-collect-external-disk-guard`
>
> 创建基线：`main@de51c297882016aa9b68e8079d648508973cc2c4`
>
> 执行授权：`阶段 2 / 任务 2.7：最终 10 份文档提交、推送并向 main 创建草稿 PR`
>
> 当前游标：`阶段 2 / 任务 2.7：最终文档提交、推送和草稿 PR 创建`
>
> 阻断声明：`只允许提交推送当前 10 份最终证据文档并向 main 创建草稿 PR；不得合并、转为正式评审、执行其他 Git/PR、远端写入、手动清理或服务操作`

本文是已批准需求的技术规范和执行顺序。当前生产事实仍由 `plan/data-collect-plan.md` 拥有；
用户已单独授权并完成阶段 1 的实现、验证、独立复审、提交和推送，并随后授权完成阶段 2
任务 2.1 的本地发布候选构建、任务 2.2 的生产只读核查和任务 2.3 的维护窗方案制定。用户
现已用新的明确决定覆盖原候选维护窗，并授权任务 2.4 停止旧服务、永久删除当前 UTC 日目录、
部署冻结候选并恢复长期运行。任务已经完成停止、当前日删除和四产物安装，但首次启动被错误的
`NRestarts` 验收门主动停止。第二次启动按用户批准的最小门执行，但 180 秒时仍未完成启动恢复，
因此按门停止。用户随后授权取消启动恢复固定时限；同一候选已经完成启动恢复、180 秒稳定性和
实际字节增长验收，当前长期运行。生产事实文档同步与两路独立审查已经通过。用户随后明确
授权把 timer 调度更新为每日 UTC `12:00:00` 并首次启用。执行前发现新 timer 与当前冻结 SHA
不同。用户已选择先复审、提交并推送当前 10 份文档与 1 份 timer 源文件，再从该干净且已推送
提交冻结并部署 timer 单文件候选。该 timer 已完成原子替换和首次启用，当前等待首次自然调度；
生产证据文档同步与独立复审已经通过。用户现已授权提交并推送当前 10 份最终证据文档，再向
`main` 创建草稿 PR；不授权合并、转为正式评审、其他 Git/PR、手动清理或远端状态变更。

## 1. 目标、完成定义与已核实边界

### 1.1 仅有的两个修复目标

1. 以根文件系统 30GB 水位和每日 UTC `12:00:00` 的外部任务删除最早一个历史采集日；训练
   采集器只移除运行期间每 60 秒执行的递归容量扫描，启动时 50GB 检查及其现有合同保持不变。
2. 修复 `real_source_alignment_probe` 被普通 Cargo 测试误执行、默认产物相撞且写入器主错误
   被通道断开信息掩盖的问题。

启动清理协调锁、删除隔离状态机、进程身份持续性、专用发布切换和完整移除内部 50GB 合同
均不属于本事项。

### 1.2 可验证的完成定义

1. `data-collect-training-collector` 运行后不再创建 60 秒容量扫描定时状态，也不再进入周期
   容量扫描及其关闭分支。
2. 启动时的递归容量扫描、`--max-dataset-bytes`、50GB 默认值、配置摘要
   `data_collect_training_config_v2`、退出码 `75`、容量单元测试、启动容量合同测试和采集
   systemd 单元保持不变。
3. 固定外部脚本每天 UTC `12:00:00` 运行一次：低于 30GB 零删除，达到或超过 30GB 时直接
   删除严格早于 UTC 昨日且日期最早的一个采集日；不读取日期目录内容决定资格。
4. 清理脚本没有采集服务启停或重启动作；一次清理执行后，以
   `systemctl --user is-active data-collect-training-collector.service` 证明采集服务仍为
   `active`。
5. `real_source_alignment_probe` 只保留为操作员显式运行的本地二进制验证工具；普通回归不
   执行真实网络主程序，默认运行目录互不冲突，写入器实际主错误可以返回调用方。
6. 只保留直接证明上述合同的测试；仓库规定的普通回归、格式、静态检查和独立审查通过。

### 1.3 当前已核实事实

- 用户提供的当前业务量是每天约增加 2–3GB；外部触发线据此固定为 30GB。用户最新决定把
  调度固定为每日 UTC `12:00:00`。
- 当前已提交并推送的 `data-collect/src/live_runtime.rs` 只在启动时调用
  `training_dataset_size_bytes`；运行期周期容量定时状态、递归扫描分支、容量终态和孤立的
  `RuntimeCloseCause::CapacityLimit` 已移除。
- 当前启动扫描达到配置上限会产生 `dataset_capacity_reached`，再由
  `data-collect/src/runtime.rs` 映射为退出码 `75`。生产单元仍传入
  `--max-dataset-bytes 50000000000`，并配置 `RestartPreventExitStatus=75 76`。
- `data-collect/src/live_runtime/dataset_capacity.rs` 和对应单元测试同时服务启动扫描，不能随
  周期扫描一起删除。
- 当前采集器已有全生命周期数据集根锁。本事项不修改该锁，也不让外部脚本取得或等待它；
  不新增第二把协调锁。
- 当前仓库已提交并推送固定 30GB 水位的外部清理脚本、两个 Shell 夹具、oneshot 服务和每日
  timer；脚本与两个单元已经安装到生产，timer 当前为 `enabled/active/waiting`，首次自然调度
  尚未发生，清理脚本尚未运行。
- 当前主需求 `demand/data-collect-demand.md` 已按获批需求完成窄范围同步：继续禁止采集器
  静默删除原始数据，并明确经独立需求批准的外部历史数据保留任务不属于采集器静默删数。
- 基线事故与基线代码已经证明：`cargo test --workspace --all-targets` 会执行原
  `harness = false` bench 的真实网络主程序，并可能以二级通道断开错误掩盖写入器主因。当前
  已提交并推送的候选把 `real_source_alignment_probe` 迁为 `bench = false` 的显式二进制
  程序，并完成运行身份、产物目录归属和写入器主错误传播修复；其他 bench、三个生产角色
  二进制程序和生产写入器未修改。
- 唯一发布候选从干净且已推送的
  `94a3e0b37151ba2ee36aa7fbcf94fec0d3d8a9c9` 构建于
  `/tmp/data-collect-disk-guard-release.ISPMI9/release/data-collect-training-collector`。二进制、
  清理脚本、服务和 timer 的 SHA-256 依次为
  `016ec1049ebd2b608038fe257a1f940418a43f743b1b5c6ca850da4aa232f62f`、
  `7243c496451e57cfa5c2a5ff3b7282bdc509e3278598227de935f100a8e6706c`、
  `6fb4f1bf5326ce578ab4eaec17453d6205c59c1623c7e911427e57562e22882b`、
  `0c544042eb3d463662bdf20f09c4640e3c7a1393ff3c259d3e6c025c784e383e`；构建前后提交、上游和
  工作树不变，真实源验证工具不属于发布候选。
- `2026-08-08T02:19:05Z` 生产只读快照中，训练服务为 `active/running`，活动 PID
  `1298039` 前后不变，活动二进制 SHA-256 为
  `5359b4cfcd397f8fd83a648808950e635df54b7fff5ee7ab584238d2c5784b0e`。根文件系统已用
  `27,598,520,320` 字节，低于 30GB 水位 `2,401,479,680` 字节；直接日期目录为
  `utc_day=2026-07-29` 至 `utc_day=2026-08-08`。三个新清理文件均不存在，生产仍运行旧
  二进制；`NRestarts=5` 只作为观测事实保留，本次只读核查不推断原因。
- `2026-08-08T02:42:47Z` 进一步只读核实当前日清单为 `open`、覆盖为 `active`，身份绑定旧
  提交 `a3fed4050e5554bb2f24d567d182d4f3ce87aece` 和旧二进制 SHA-256
  `5359b4cfcd397f8fd83a648808950e635df54b7fff5ee7ab584238d2c5784b0e`；候选的提交和二进制
  SHA 均不同。`utc_day=2026-08-09` 尚不存在。当前代码要求当前日清单五项身份精确一致，
  因此候选同日不能成功接管；若通过更早执行的启动容量门并到达清单恢复，则会因身份不匹配
  退出 `76`。停止旧进程只结束覆盖，不会改写同日身份。
- `2026-08-08T02:59:30Z` 按启动门的普通文件逻辑长度语义只读统计生产 `datasets/training` 为
  `23,977,394,313` 字节，且没有符号链接或特殊文件，当前低于 50GB 启动上限。该快照不能代替
  维护窗前刷新；根文件系统 `df /` 水位也不能证明启动容量门会通过。
- `2026-08-08T03:37:48Z` 旧服务正常停止；`03:38:32Z` 精确永久删除当前日目录，当时其中普通
  文件逻辑长度为 `253,898,406` 字节，昨日及更早十个目录保持存在；`03:39:18Z` 四个冻结产物
  已按计划安装，timer 为 `disabled/inactive` 且清理脚本未运行。
- `03:39:41Z` 首次启动候选后，systemd 把新一轮人工启动的 `NRestarts` 从历史值 `5` 重置为
  `0`；执行脚本把该合法重置误判为漂移，在约 30 毫秒后主动停止服务。日志只有启动、停止中、
  已停止三条；候选没有创建当前日目录或清单。当前服务为 `inactive/dead`、`MainPID=0`，四产物
  SHA 正确，昨日及更早目录未变。
- `03:57:17Z` 第二次启动候选，活动进程 PID `1314688`、SHA-256 与冻结候选一致，至
  `04:00:18Z` 的 181 秒内 PID 不变且 `NRestarts=0`，但当前日目录尚未创建。验收门因此发出
  systemd 停止请求。
- `04:02:16Z` 程序完成退出并报告清单激活被预激活 `OperatorStop` 拒绝；这证明停止请求到达时
  启动恢复尚未完成，数据源和周期 flush 尚未获准运行。systemd 终态为 `failed`、`MainPID=0`、
  `NRestarts=0`。随后生成的当前日清单为候选身份、`open`、覆盖 `not_started`、文件列表为空，
  没有任何 Parquet 文件；timer 仍为 `disabled/inactive`。
- `04:15:56Z` 同一候选从 systemd `failed` 状态直接启动，没有单独执行 `reset-failed`，也没有
  删除或改写当前日清单。`04:20:59Z` 启动恢复在 302 秒后自然完成，覆盖进入 `active` 并出现
  运行中 Parquet。随后 182 秒内 PID `1315799` 不变、`NRestarts=0`，同一运行中文件从 `0`
  增长到 `39,839` 字节。
- `04:24:25Z` 最终只读快照中服务为 `active/running`，进程 SHA-256 与冻结候选一致；当前日有
  6 个运行中 Parquet，总计 `2,065,692` 字节。timer 为 `disabled/inactive`，清理服务为
  `inactive`，昨日及更早十个目录仍存在。
- 提交 `800bca07b98605339d0929eccbb2d9c8b156cb53` 已把 UTC `12:00:00` timer 与当前 10 份文档
  推送到远端分支；提交、上游和远端分支一致，工作树当时干净。该提交中的 timer SHA-256 为
  `b612164bc19f9df6f064b64cb03cedd8ff9a6c08464d2ca602b63a5cc9ea13ac`。
- `08:50:50Z` 生产只原子替换 timer 并执行 `daemon-reload`、`enable --now`。`08:51:35Z`
  复核为 `enabled/active/waiting`，下一次名义触发为 `2026-08-08T12:00:00Z`，`Persistent=no`、
  `AccuracyUSec=1min`，清理服务仍为 `inactive/dead` 且 journal 为零行。采集服务仍为
  `active/running`、PID `1315799`、`NRestarts=0`；11 个日期目录没有变化。根文件系统已用
  `27,703,296,000` 字节，低于 30GB 水位；首次自然调度结果尚未发生，不能提前声称低水位运行
  已通过。

### 1.4 已冻结的用户决定

1. 根文件系统水位固定为十进制 `30,000,000,000` 字节。
2. 外部定时器每天 UTC `12:00:00` 执行一次；每次达到水位最多删除一个最早历史日，当前 UTC
   日和 UTC 昨日永不选择。
3. 日期内容、清单、覆盖状态、备份回执和异常日登记不参与正常删除资格。
4. 采集器只删除运行期间每 60 秒的递归容量扫描，保留启动时 50GB 检查及其 CLI、配置、
   退出码和 systemd 合同。
5. 不新增清理协调锁、删除隔离区或残留状态机。
6. 清理脚本不得停止、启动或重启采集服务；执行后只验收服务仍为 `active`。
7. `real_source_alignment_probe` 的目标归类、默认产物隔离和写入器主错误传播继续作为第二个
   修复目标。
8. 用户确认 UTC 昨日及更早采集数据已经备份，接受当前 UTC 日从 `00:00:00Z` 至候选恢复
   写入前的全部数据缺口；本次换版直接停止旧服务，永久删除且只删除当前 UTC 日目录，再以
   冻结候选重新创建同日身份并长期运行。该一次性换版不改变每日清理的历史日选择规则。
9. 首次启动被错误验收门主动停止后，用户授权只再次启动一次，不再次删除目录或重装产物；
   本次只验收冻结候选二进制、180 秒无重启和同一当前日运行中 Parquet 文件字节数严格增加。
10. 第二次启动证明 180 秒早于启动恢复完成后，用户授权取消启动恢复阶段的固定停止时限；直接
    启动同一候选，等运行中 Parquet 出现后才开始 180 秒无重启和同一文件字节增长观察。不删除
    或改写已经生成的候选身份 `open/not_started` 清单。
11. 用户授权把已安装的外部清理 timer 调度改为每日 UTC `12:00:00` 并首次启用，同时授权
    复审、提交和推送当前 10 份文档与 1 份 timer 源文件。该次授权当时不包括 PR，也不包括
    手动启动清理服务、其他 Git 动作、其他产物重装或额外手动删除。
12. timer 启用和任务 2.6 独立复审完成后，用户最新授权提交并推送当前 10 份最终证据文档，
    随后以当前事项分支为 head、`main` 为 base 创建草稿 PR；不授权合并、转为正式评审、其他
    Git/PR、手动启动清理服务、其他产物重装或额外手动删除。

### 1.5 明确接受的残余风险

- 外部 30GB 使用根文件系统 `df /` 已用字节数，启动 50GB 使用 `datasets/training` 逻辑文件
  大小，两者不能相减为 20GB 余量。30GB 是根据每天约 2–3GB 业务增长选定的较早操作水位，
  但定时器失效、脚本失败、没有可删除历史日或其他进程快速占盘时，仍不能保证根文件系统
  不会占满。
- 采集器已经运行后不会再因周期容量扫描主动停止；若外部清理失效，且采集器随后启动时训练
  数据集逻辑大小已经达到 50GB，保留的启动门仍会退出 `75`，systemd 按现有合同不会自动
  重启。
- 本事项不协调清理与采集器恰好同时启动的罕见竞态，也不为人工并发执行同一脚本增加锁。
  正常调度只通过一个 systemd oneshot 单元运行；发生竞态或删除失败时按实际错误报告，不
  扩大删除范围。
- 直接递归删除若中断，原日期目录可能部分保留。下一次每日运行仍按活动树中最早日期重新
  选择；本事项不增加隔离区或恢复状态机。
- 本次一次性删除当前 UTC 日目录不可恢复；用户提供的备份确认只覆盖 UTC 昨日及更早数据，
  当前日旧数据明确放弃。候选一旦重新创建当前日身份，旧二进制不能同日接管；任务失败时
  停止并报告，不自行再次删除、改写身份或回滚旧版本。

## 2. 技术方案

### 2.1 成熟工具与最小自研边界

- 复用现有 Rust、Tokio 和容量统计函数，只删除周期调度及由它产生的孤立状态，不重写容量
  统计器。
- 复用 Bash、`df`、`find`、标准排序和 GNU `rm` 编写一个固定生产脚本；复用 systemd 用户级
  oneshot 服务和 timer 做每日调度，不新增守护进程、数据库或依赖。
- 复用 Cargo 标准二进制目标、默认 libtest、Rust 标准库原子新建文件和现有开发依赖
  `tempfile` 修复真实源验证工具，不新增第三方依赖。

### 2.2 只删除运行期间 60 秒容量扫描

计划修改范围固定如下：

| 路径 | 最小动作 | 明确保留 |
| --- | --- | --- |
| `data-collect/src/live_runtime.rs` | 删除 `DATASET_CAPACITY_SCAN_INTERVAL_SECS`、`next_capacity_scan`、`capacity_shutdown`、运行循环中的周期扫描块及周期容量终态返回 | 启动时 `max_dataset_bytes` 正数检查、`training_dataset_size_bytes` 调用和 `dataset_capacity_reached` 返回 |
| `data-collect/src/live_runtime/raw_day.rs` | 删除周期扫描移除后孤立的 `RuntimeCloseCause::CapacityLimit` | 其他关闭原因与日终语义 |
| `data-collect/src/live_runtime/tests/raw_day.rs` | 删除孤立的 `CapacityLimit` 测试分支 | 完整性失败和其他关闭测试 |

以下文件和合同不得修改：

- `data-collect/src/runtime.rs` 中的 `--max-dataset-bytes`、50GB 默认值、配置字段、摘要 schema
  `data_collect_training_config_v2` 和 `dataset_capacity_reached → 75` 映射；
- `data-collect/src/live_runtime/dataset_capacity.rs`、其模块登记和全部单元测试；
- `data-collect/tests/training_collector_runtime_contract.rs` 中的启动超限退出 `75` 合同；
- `data-collect/deploy/systemd/data-collect-training-collector.service` 中的容量参数和
  `RestartPreventExitStatus=75 76`；
- 根锁、启动恢复、UTC 日切换、数据源、队列、批次、写入器和退出码 `76`。

该修改不改变 `CliArgs` 序列化结构，因此不升级配置摘要 schema。实现验证采用编译、既有启动
容量测试、完整回归和精确残留搜索，不新增等待 60 秒或连续事件流测试。

### 2.3 30GB 每日外部清理

仓库候选路径：

```text
data-collect/deploy/scripts/prune-oldest-utc-day.sh
data-collect/deploy/tests/prune-oldest-utc-day-test.sh
data-collect/deploy/systemd/data-collect-prune-oldest-day.service
data-collect/deploy/systemd/data-collect-prune-oldest-day.timer
```

服务器安装路径：

```text
/home/ubuntu/btc-updown-marketmaker/data-collect/bin/prune-oldest-utc-day.sh
/home/ubuntu/.config/systemd/user/data-collect-prune-oldest-day.service
/home/ubuntu/.config/systemd/user/data-collect-prune-oldest-day.timer
```

生产固定值：

```text
根文件系统：/
触发条件：df 已用字节数 >= 30,000,000,000
活动数据根：/home/ubuntu/btc-updown-marketmaker/datasets/training/typed_raw
调度：每日 UTC 12:00:00
```

生产脚本不接受参数，也不从环境变量覆盖根、阈值、当前日期或删除日期。脚本内部把选择和
删除主流程封装为函数，测试以临时目录、固定日期和模拟水位调用同一函数；生产入口始终使用
上述常量，不另写测试影子实现。

单次执行顺序只有以下动作：

1. 使用 `df -B1 --output=used /` 读取根文件系统已用字节数；低于 30GB 时记录零动作并退出。
2. 使用 UTC 计算当前日 `D` 和昨日 `D-1`，只枚举活动数据根的直接目录。目录名必须完整匹配
   `utc_day=YYYY-MM-DD`；忽略其他名称，只保留日期严格小于 `D-1` 的目录，因此昨日、当前日
   和未来日期都不会成为候选。
3. 按日期升序选择第一个目录；没有历史日期时记录零动作并退出。不得读取该目录中的清单、
   Parquet、覆盖状态或任何业务内容。
4. 对已选择并保持引用的唯一绝对路径执行一次
   `rm -rf --one-file-system --preserve-root=all -- "$candidate"`；本轮不再选择第二个日期。
5. 记录 UTC 时间、删除前 `df` 已用量、所选日期、精确路径和结果。脚本不调用
   `systemctl start`、`stop`、`restart`、`try-restart` 或等价服务控制动作。

用户级服务使用 `Type=oneshot`、固定绝对 `ExecStart`、`Restart=no`，不声明对采集服务的
`Requires`、`Wants`、`PartOf`、`Conflicts` 或启停传播关系。timer 使用：

```ini
OnCalendar=*-*-* 12:00:00 UTC
```

systemd 负责同一 oneshot 单元不重入和 journal 记录；脚本不增加文件锁。生产首次启用、任何
真实删除及服务状态验收仍分别需要明确远端和数据删除授权。

### 2.4 修复 `real_source_alignment_probe` 的原 bench 事故

该修复只调整这一验证工具：

1. 把 `data-collect/benches/real_source_alignment_probe.rs` 迁移到
   `data-collect/src/bin/real_source_alignment_probe.rs`；Cargo 目标从 `[[bench]]`、
   `harness = false` 改为显式 `[[bin]]`，并设置 `bench = false`。真实网络入口固定为：

   ```bash
   cargo run --release --locked --offline -p data-collect --bin real_source_alignment_probe
   ```

   `--offline` 只约束 Cargo 依赖解析，不表示真实源运行无需网络。普通 `cargo test` 使用标准
   libtest，不执行该二进制的业务 `main`。
2. 配置解析只生成一次 `run_id`。无显式产物目录时使用
   `<cwd>/real-source-alignment-artifacts/<run_id>`；有 `DATASET_ROOT` 时使用
   `<root>/phase0-validation/<run_id>`；显式 `--artifact-dir` 或
   `REAL_SOURCE_ARTIFACT_DIR` 继续表示精确目录。
3. 默认 `run_id` 包含 UTC 毫秒、PID 和进程内标准库 `AtomicU64` 单调序号，确保同一进程同一
   毫秒内重复解析配置也不会复用目录。显式 `REAL_SOURCE_RUN_ID` 必须是单个安全路径组件；
   目录、运行归属回执和探针 `manifest.json` 使用同一个 `run_id`。
4. 在联网和业务写入前，要求显式目录为空，并使用 `OpenOptions::create_new(true)` 原子创建
   `run_owner.json`。非空目录或两个进程竞争同一目录时明确失败，不覆盖或删除既有文件。
5. 保留 `ParquetSegmentWriter::create` 的现有失败关闭保护。增加一次性启动结果通道，使写入器
   创建错误直接返回；工作线程后续写入或最终化失败时保存主错误，发送端遇到通道断开时优先
   返回该主错误。只有确实没有详情时才报告写入器意外结束。
6. 不迁移其他 `bench`，不修改三个生产二进制程序、六类数据源、schema、JSON、JSONL、
   Parquet 字段或生产写入器合同，也不把该工具部署到服务器。

最小离线测试只覆盖：Cargo 元数据中同名目标只有 `bin`；两次默认配置产生不同目录且单次配置
身份一致；一个显式非空目录保持原文件不变；注入的写入器启动错误和运行错误分别显示真实
主因。测试使用临时目录和注入错误，不连接真实外部数据源。

## 3. 执行计划

### 阶段 0：文档收敛与授权

- 任务 0.1：核对需求、计划、changelog、`data-collect/docs/CURRENT.md`、真实代码和当前权限。
  - 验证：当前事实与未来方案分开，两个目标之外没有实施项。
- 任务 0.2：按用户决定重写审查稿并独立复审。
  - 验证：30GB、每日、仅删周期扫描、保留启动 50GB、无新锁、服务 `active` 和 bench 修复在
    三件套中一致；`git diff --check` 与语言自检通过。
- 任务 0.3：独立复审通过后，由用户明确批准需求和计划。
  - 验证：批准只确认规范内容，不自动授权代码、测试、构建、提交、推送、远端、服务或数据
    操作。
- 任务 0.4：用户批准后、提交前，先把 `demand/data-collect-demand.md` 的绝对禁令收窄为禁止
  采集器静默删数并保留获批外部任务；同时把本事项三件套、`AGENTS.md` 和
  `data-collect/docs/CURRENT.md` 的生命周期、批准状态、当前游标和路由更新为真实的“规范已
  批准、阶段 1 尚未获授权”。
  - 验证：主需求与获批需求不再冲突，所有状态文本反映同一权限；自审后由独立
    `gpt-5.6-sol`、`ultra` 子代理复审这一最终阶段 0 文档集合。
- 任务 0.5：任务 0.4 复审通过且用户另行授权阶段 0 文档提交和推送后，才暂存、提交并推送
  主需求、本事项三件套、`AGENTS.md` 和 `data-collect/docs/CURRENT.md`。
  - 验证：提交前审查精确暂存差异并执行 `git diff --cached --check`；提交范围只包含本事项及
    其必要权威路由更新，工作树干净，本地分支与远端分支指向同一提交。未获得提交和推送授权
    时停留在阶段 0，不进入阶段 1。

阶段 0 只有在候选规范独立复审、用户批准、批准后权威与状态同步、最终独立复审、获授权提交
和推送全部完成后才结束；阶段 0 完成仍不授权阶段 1。

### 阶段 1：实现两个修复目标

- 任务 1.1：移除运行期周期容量扫描。
  - 步骤 1.1.1：按第 2.2 节删除周期定时状态、扫描分支、容量终态和孤立关闭原因。
    验证：精确搜索只剩启动容量路径；`runtime.rs`、容量模块、启动合同和采集 systemd 单元无
    差异。
  - 步骤 1.1.2：调整因枚举分支删除而必须同步的现有测试。
    验证：容量模块单元测试、启动超限退出 `75` 合同和相关运行时测试通过；不新增无业务意义
    的 60 秒等待测试。

- 任务 1.2：实现 30GB 每日外部清理。
  - 步骤 1.2.1：先为同一脚本建立两个夹具：低于 30GB 零删除；达到 30GB 时在同时存在多个
    历史日、昨日、当前日和非日期目录的情况下，实际只删除最早一天。
    验证：高水位夹具中最早日消失，其余历史日、昨日、当前日和非日期目录仍在；两个夹具
    直接覆盖阈值、日期范围和一次一天，不增加清单、锁、inode、跨日、异常日或中断状态机
    测试。未来日期排除由同一日期 `< D-1` 条件的代码审查确认，不新增无独立证明力的夹具。
  - 步骤 1.2.2：实现固定生产入口、直接删除和最小日志。
    验证：`bash -n`、真实脚本可执行位和脚本夹具通过；源代码审查确认没有参数覆盖、环境
    覆盖、文件锁、隔离目录或采集服务控制命令。
  - 步骤 1.2.3：新增 oneshot 服务和每日 timer。
    验证：在临时 `--root` 夹具中为远端绝对 `ExecStart` 创建同路径可执行占位，再执行
    `systemd-analyze verify`；不在本机创建真实 `/home/ubuntu/...` 路径，也不启动任何服务。

- 任务 1.3：修复真实源验证工具。
  - 步骤 1.3.1：迁移 Cargo 目标并加入第 2.4 节的最小离线回归。
    验证：实现前代表性断言按预期失败；Cargo 元数据不再存在同名 `bench`。
  - 步骤 1.3.2：实现单次 `run_id`、默认目录隔离、原子目录归属和写入器主错误传播。
    验证：

    ```bash
    cargo test -p data-collect --bin real_source_alignment_probe --locked --offline
    ```

    全部通过；不执行真实网络业务主程序，不在工作树创建运行产物。

- 任务 1.4：同步当前合同并完成本地验收。
  - 步骤 1.4.1：只同步受两个目标影响的 `plan/data-collect-plan.md`、本事项计划与 changelog、
    `data-collect/docs/current/` 派生参考、`data-collect/docs/CURRENT.md` 和 `AGENTS.md`；状态明确
    写成“本地实现已验证、生产尚未部署”，获批需求的业务语义不变。
    验证：生命周期、游标、权限与真实实现状态一致，不修改无关业务合同，不恢复读取 Markdown
    的 Rust 文档守卫。
  - 步骤 1.4.2：执行标准完成门：

    ```bash
    cargo metadata --no-deps --format-version 1 --locked --offline
    cargo fmt --all -- --check
    cargo check --workspace --all-targets --locked --offline
    cargo clippy --workspace --all-targets --locked --offline -- -D warnings
    cargo test --workspace --lib --bins --tests --examples --locked --offline -- --skip four_source_production_path_sustains_two_to_five_thousand_native_messages_per_second
    ```

    验证：具名吞吐测试明确记录为“跳过”；任何 `[[bench]]` 和真实网络探针均未执行，Shell 与
    systemd 验证另行通过。
  - 步骤 1.4.3：自审后由独立 `gpt-5.6-sol`、`ultra` 子代理全面复审；发现范围内问题后修复
    并重复相关验证和审查。
    验证：需求第 1 至 5 项的阶段 1 本地实现范围均有直接证据，无锁、切换、隔离或完整容量
    移除等范围扩张；第 6 项属于后续一次性生产部署验收。
  - 步骤 1.4.4：阶段审查通过后，只有用户另行授权提交和推送时才执行对应 Git 动作。
    验证：提交内容只包含获批范围，本地与远端分支指向同一提交；没有授权时保持本地工作树，
    阶段 1 停留在等待提交与推送，不自行创建 PR，也不进入阶段 2。

阶段 1 完成表示本地实现、验证、独立审查、提交和推送全部完成，不表示生产已部署。

### 阶段 2：生产部署与验收（必须另行授权）

- 任务 2.1：准备唯一发布候选。
  - 步骤 2.1.1：只有提交、推送和构建均获授权时，确认阶段 1 审查提交已经推送且工作树干净，
    再从该提交使用独立临时产物目录构建，并冻结二进制、脚本、服务和定时器四个发布产物：

    ```bash
    set -euo pipefail
    disk_guard_release_commit="$(git rev-parse HEAD)"
    test -z "$(git status --porcelain)"
    test "$disk_guard_release_commit" = "$(git rev-parse '@{upstream}')"
    test -x data-collect/deploy/scripts/prune-oldest-utc-day.sh
    disk_guard_release_target_dir="$(mktemp -d)"
    DATA_COLLECT_GIT_COMMIT="$disk_guard_release_commit" \
      CARGO_TARGET_DIR="$disk_guard_release_target_dir" \
      cargo build --release --locked --offline -p data-collect --bin data-collect-training-collector
    sha256sum \
      "$disk_guard_release_target_dir/release/data-collect-training-collector" \
      data-collect/deploy/scripts/prune-oldest-utc-day.sh \
      data-collect/deploy/systemd/data-collect-prune-oldest-day.service \
      data-collect/deploy/systemd/data-collect-prune-oldest-day.timer
    test "$disk_guard_release_commit" = "$(git rev-parse HEAD)"
    test "$disk_guard_release_commit" = "$(git rev-parse '@{upstream}')"
    test -z "$(git status --porcelain)"
    ```

    验证：记录提交、完整本地产物路径、四个 SHA-256 和脚本可执行位；构建前后提交不变且
    工作树干净。远端不编译，不从未推送或有未提交改动的工作树重建同名产物。
- 任务 2.2：只读刷新生产服务、活动二进制、配置、磁盘水位和现有日期目录证据。
  - 验证：证据带 UTC 时间；与计划或当前合同不一致时停止，不自行修复。
- 任务 2.3：同日身份冲突决策已经完成。用户确认 UTC 昨日及更早数据已经备份，否决跨日
  维护窗，接受当前 UTC 日从 `00:00:00Z` 至候选恢复写入前的数据缺口，并明确授权停止旧程序后
  永久删除当前 UTC 日采集目录，再立即部署和长期运行候选。该决定覆盖本任务此前的“不删除
  当前日”和候选跨日窗，不授权删除任何昨日或更早目录。
  - 验证：需求、计划、changelog 和当前路由都只把当前日删除写成一次性部署例外；每日清理仍
    只允许严格早于昨日的历史日。
- 任务 2.4：按已授权的一次性同日删除换版执行。
  - 步骤 2.4.1：只读刷新 UTC 日期、旧服务状态与活动二进制、冻结候选四个 SHA-256、训练目录
    逻辑大小及清理脚本和两个单元的计划安装路径。当前 UTC 日在整个删除前置检查中必须不变；
    `datasets/training` 必须没有符号链接、特殊文件或扫描错误且严格小于
    `50,000,000,000` 字节；距下一 UTC 午夜必须大于 600 秒。冻结 `typed_raw` 的全部直接条目；
    按现有启动清理函数的文件名和两层目录深度语义，确认当前日删除目标之外的每个真实直接
    子目录中都不存在任何 `.tmp`、`.running`、`-running.parquet`、`.running.parquet` 或
    `-digesting.parquet` 普通文件，避免候选启动时删除其他目录中的对象。任一事实漂移即停止。
  - 步骤 2.4.2：把唯一删除目标解析为
    `/home/ubuntu/btc-updown-marketmaker/datasets/training/typed_raw/utc_day=<当前 UTC 日>`；确认
    `typed_raw` 是预期真实父目录，目标是其直接子目录、不是符号链接且确实存在，并冻结 UTC
    昨日及更早直接日期目录清单。不得读取业务文件内容决定删除，也不得扩大到父目录。
  - 步骤 2.4.3：停止服务前，先把四个冻结产物上传到 `/home/ubuntu` 下唯一、不属于 systemd
    搜索路径的 `0700` 暂存目录。逐项确认是普通文件且不是符号链接，SHA-256 与冻结值一致；
    规范化二进制和脚本为 `0755`、两个单元为 `0644`。确认暂存目录和两个不同最终父目录都是
    规范路径精确匹配的非符号链接真实目录，且位于同一文件系统；确认现有训练二进制仍是普通
    非符号链接文件，其他三个最终路径及 timer 启用链接均完全不存在，包括悬空符号链接。任一
    检查失败时只允许清理本次新建的精确暂存目录并报告，不停止服务或删除数据。
  - 步骤 2.4.4：只通过
    `systemctl --user stop data-collect-training-collector.service` 停止旧服务；确认
    `inactive/dead`、`MainPID=0`、`Result=success`。停止失败时不删除、不安装。
  - 步骤 2.4.5：在同一个失败即停的远端 shell 中，紧邻删除再次断言当前 UTC 日仍等于冻结日、
    距下一 UTC 午夜仍大于 600 秒、采集服务仍为 `inactive/dead` 且 `MainPID=0`、父目录和目标
    规范路径精确相等、目标仍为非符号链接直接目录；然后只对该冻结绝对目标执行一次
    `rm -rf --one-file-system --preserve-root=all -- <精确当前日目录>`。验证目标不存在，且冻结的
    UTC 昨日及更早直接日期目录仍存在；再次确认冻结的其余直接条目没有漂移，且删除目标之外
    的全部真实直接子目录中没有会被候选启动清理的上述临时文件。删除失败、残留或检查失败时
    停止，不安装或启动候选，不继续删除。
  - 步骤 2.4.6：从已验证暂存目录以同文件系统 `rename` 原子安装训练二进制、清理脚本、oneshot
    服务和 timer；紧邻安装前重复两个最终父目录和四个最终路径类型检查，并对每个路径使用
    `mv -T --` 或等价的不跟随目标目录语义。执行 `systemctl --user daemon-reload`，但 timer
    必须保持 `disabled/inactive`，清理脚本不得运行。
  - 步骤 2.4.6 的首次执行已经证明“启动前后 `NRestarts` 数值相等”不成立：人工启动会把历史
    计数重置为 `0`，错误门因此主动停止了刚启动的候选。
  - 用户批准删除错误比较后，第二次启动在 181 秒内保持冻结候选 SHA、同一 PID 和
    `NRestarts=0`，但尚未创建当前日目录或产生 Parquet 文件，因而按获批门停止。程序随后明确
    报告启动清单激活被预激活 `OperatorStop` 拒绝；当前日仅留下候选身份的 `open/not_started`
    清单，文件列表为空。该事实证明 180 秒停止时仍处于启动恢复而非数据采集后的 flush 等待。
  - 用户已经批准直接从当前 systemd `failed` 且无进程的状态启动同一候选，不单独执行
    `reset-failed`，也不删除或改写当前日清单。启动恢复阶段不设置固定停止时限；在运行中
    Parquet 出现之前只观察服务是否仍由同一候选运行。出现运行中 Parquet 后，才记录当时 PID
    和 `NRestarts` 基线并开始 180 秒观察，要求 PID 不变、`NRestarts` 不增加且同一运行中
    Parquet 普通文件字节数严格增加。候选自行退出或验收失败时停止继续处置并报告，不自行
    删除、改写、回滚或启用 timer。
  - 本次执行已经通过：启动恢复耗时 302 秒；就绪后 182 秒内 PID `1315799` 不变、
    `NRestarts=0`，同一运行中 Parquet 从 `0` 增长到 `39,839` 字节；最终服务保持
    `active/running`。任务 2.4 至此完成。
- 任务 2.5：已经完成把 timer 更新为每日 UTC `12:00:00` 并首次启用。只替换 timer 单元，
  执行 `daemon-reload` 和 `enable --now`；不手动启动清理服务。timer 后续按既有 30GB 合同
  自动运行。
  - 步骤 2.5.1：先对当前 10 份事项与当前权威文档和 1 份 timer 源文件做独立复审；复审通过后
    提交并推送这 11 个路径，确认 `HEAD`、上游和远端分支一致且工作树干净，再记录提交和新
    timer SHA-256。该提交只形成 timer 单文件配置候选，不重建或重装训练二进制、清理脚本或
    oneshot 服务，也不改变它们已经冻结的提交和 SHA。
  - 步骤 2.5.2：确认生产仍为原 timer SHA、`disabled/inactive`，清理服务没有运行且采集服务
    为 `active`；只上传并原子替换新 timer，核对远端 SHA 后执行 `daemon-reload` 和
    `enable --now data-collect-prune-oldest-day.timer`。
  - 验证：timer 为 `enabled`、`active/waiting`，下一次触发时间精确对应 UTC `12:00:00`；启用
    动作没有立即运行清理服务，采集服务仍为 `active`。首次定时运行后再按实际日志核对低水位
    零删除或高水位最多删除一个最早历史日，以及采集服务仍为 `active`。
- 任务 2.6：把真实提交、产物 SHA、部署时间、脚本结果和服务状态同步到本事项三件套、
  `AGENTS.md`、`data-collect/docs/CURRENT.md` 及受影响的当前权威文档，再做独立验收。
  - 验证：只有生产证据完整后才把候选行为写成当前事实。
- 任务 2.7：阶段 2 独立验收通过且用户已经另行授权后，提交并推送当前 10 份最终证据文档，
  再以 `data-collect-external-disk-guard` 为 head、`main` 为 base 创建草稿 PR。
  - 验证：提交只包含获批实现的真实生产证据与当前权威同步；本地、上游和远端分支指向同一
    提交；PR 的仓库、head、base、草稿状态和 head 提交均与本次推送一致。不合并、不转为
    正式评审，也不执行其他 Git/PR 动作。

阶段 2 只有在生产验收、文档同步、独立复审、获授权提交和推送，以及本轮获授权的草稿 PR
创建全部完成后才结束。

## 4. 测试与需求验收映射

| 需求验收 | 本地证据 | 生产证据 |
| --- | --- | --- |
| 1. 只删除周期扫描并保留启动 50GB | 精确差异、残留搜索、容量模块与启动退出 `75` 既有测试、普通回归 | 新二进制身份；启动合同未变 |
| 2. 30GB、每日、最早一天 | 两个脚本夹具、`bash -n`、timer 静态验证 | 获批启用后的 timer 状态、下一次触发时间及一次获批运行日志 |
| 3. 脚本不控制服务且执行后仍为 `active` | 源码和 systemd 单元依赖关系静态审查 | 脚本执行后的 `systemctl --user is-active data-collect-training-collector.service` |
| 4–5. 探针显式运行、目录隔离和主错误 | Cargo 元数据、最小离线单元测试、标准完成门及独立复审 | 不部署；显式真实运行仅在用户另行批准时执行 |
| 6. 一次性同日删除换版 | 用户明确决定、精确路径预检和冻结目录清单 | 旧服务正常停止；只删除当前日；冻结候选二进制无重启运行并恢复实际写入；timer 未启用 |

## 5. 明确不实施

- 不删除启动时 50GB 检查及其 CLI、配置、退出码 `75` 或采集 systemd 合同。
- 不新增锁、清单资格、隔离删除、残留状态机、清理脚本执行前后的 PID 不变验收或专用发布
  切换组件，也不恢复用户已经否决的附加测试。
- 不修改其他 `bench`、生产数据合同或生产写入器，不让真实网络探针进入普通回归。
- 不在远端编译或执行 `git pull`。已授权的 10 份文档与 1 份 timer 源文件提交、推送已经完成；
  当前只继续执行用户最新授权的最终 10 份文档提交、推送和面向 `main` 的草稿 PR 创建；不
  合并、不转为正式评审、不执行其他 Git/PR，也不手动启动清理服务或执行额外手动删除。获批
  timer 只按每日 UTC `12:00:00` 和既有 30GB 合同自动运行。

## 6. 当前游标与下一次允许动作

用户已批准需求和计划；阶段 1、任务 2.1、任务 2.2 和任务 2.3 已完成。用户以最新明确决定
覆盖候选跨日维护窗，并授权任务 2.4 的停止旧服务、永久删除当前 UTC 日目录、安装冻结产物及
启动候选长期运行。

任务 2.4 已完成：旧当前日目录按授权删除，四个冻结产物已经安装；同一候选完成 302 秒启动恢复
和就绪后 182 秒稳定写入验收，当前服务为 `active/running`。该次生产事实文档同步和两路独立
审查已经通过。用户随后授权任务 2.5：把 timer 调度更新为每日 UTC `12:00:00` 并首次启用。
新 timer 候选 SHA-256 为
`b612164bc19f9df6f064b64cb03cedd8ff9a6c08464d2ca602b63a5cc9ea13ac`，与已部署冻结 SHA 不同；
任务 2.5 已完成：提交 `800bca07b98605339d0929eccbb2d9c8b156cb53` 已推送；生产 timer 已
更新为 SHA-256 `b612164bc19f9df6f064b64cb03cedd8ff9a6c08464d2ca602b63a5cc9ea13ac`，
当前为 `enabled/active/waiting`，下一次名义触发为 `2026-08-08T12:00:00Z`。启用没有立即运行
清理服务，采集器和日期目录均未变化。任务 2.6 的事实文档同步和独立复审已经完成；当前游标
为任务 2.7：提交并推送当前 10 份最终证据文档，再向 `main` 创建草稿 PR。首次自然调度结果
仍待 12:00 后按实际证据核对；不授权合并、转为正式评审、其他 Git/PR、手动启动清理服务、
其他产物重装或额外手动删除。
