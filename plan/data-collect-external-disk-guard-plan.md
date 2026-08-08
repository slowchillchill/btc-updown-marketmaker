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
> 执行授权：`STAGE_1_COMMIT_AND_PUSH_ONLY`
>
> 当前游标：`阶段 1 / 任务 1.4.4：提交和推送已授权、执行中`
>
> 阻断声明：`只允许阶段 1 提交和推送；阶段 2、PR、发布构建、远端、服务和数据操作未授权`

本文是已批准需求的技术规范和执行顺序。当前生产事实仍由 `plan/data-collect-plan.md` 拥有；
用户已单独授权阶段 1 的实现、验证和独立复审，并已授权任务 1.4.4 的提交和推送。本计划中
的阶段 2 未来式步骤不构成 PR、发布构建、远端、服务或数据操作授权。

## 1. 目标、完成定义与已核实边界

### 1.1 仅有的两个修复目标

1. 以根文件系统 30GB 水位和每日一次的外部任务删除最早一个历史采集日；训练采集器只移除
   运行期间每 60 秒执行的递归容量扫描，启动时 50GB 检查及其现有合同保持不变。
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
3. 固定外部脚本每天运行一次：低于 30GB 零删除，达到或超过 30GB 时直接删除严格早于 UTC
   昨日且日期最早的一个采集日；不读取日期目录内容决定资格。
4. 清理脚本没有采集服务启停或重启动作；一次清理执行后，以
   `systemctl --user is-active data-collect-training-collector.service` 证明采集服务仍为
   `active`。
5. `real_source_alignment_probe` 只保留为操作员显式运行的本地二进制验证工具；普通回归不
   执行真实网络主程序，默认运行目录互不冲突，写入器实际主错误可以返回调用方。
6. 只保留直接证明上述合同的测试；仓库规定的普通回归、格式、静态检查和独立审查通过。

### 1.3 当前已核实事实

- 用户提供的当前业务量是每天约增加 2–3GB；外部触发线据此固定为 30GB，调度固定为每日
  一次。
- 当前已验证候选中的 `data-collect/src/live_runtime.rs` 只在启动时调用
  `training_dataset_size_bytes`；运行期周期容量定时状态、递归扫描分支、容量终态和孤立的
  `RuntimeCloseCause::CapacityLimit` 已移除。
- 当前启动扫描达到配置上限会产生 `dataset_capacity_reached`，再由
  `data-collect/src/runtime.rs` 映射为退出码 `75`。生产单元仍传入
  `--max-dataset-bytes 50000000000`，并配置 `RestartPreventExitStatus=75 76`。
- `data-collect/src/live_runtime/dataset_capacity.rs` 和对应单元测试同时服务启动扫描，不能随
  周期扫描一起删除。
- 当前采集器已有全生命周期数据集根锁。本事项不修改该锁，也不让外部脚本取得或等待它；
  不新增第二把协调锁。
- 当前仓库已实现并验证固定 30GB 水位的外部清理脚本、两个 Shell 夹具、oneshot 服务和每日
  timer；尚未部署、启用或执行真实删除。
- 当前主需求 `demand/data-collect-demand.md` 已按获批需求完成窄范围同步：继续禁止采集器
  静默删除原始数据，并明确经独立需求批准的外部历史数据保留任务不属于采集器静默删数。
- 基线事故与基线代码已经证明：`cargo test --workspace --all-targets` 会执行原
  `harness = false` bench 的真实网络主程序，并可能以二级通道断开错误掩盖写入器主因。当前
  已验证候选把 `real_source_alignment_probe` 迁为 `bench = false` 的显式二进制程序，并
  完成运行身份、产物目录归属和写入器主错误传播修复；其他 bench、三个生产角色二进制程序
  和生产写入器未修改。

### 1.4 已冻结的用户决定

1. 根文件系统水位固定为十进制 `30,000,000,000` 字节。
2. 外部定时器每天执行一次；每次达到水位最多删除一个最早历史日，当前 UTC 日和 UTC 昨日
   永不选择。
3. 日期内容、清单、覆盖状态、备份回执和异常日登记不参与正常删除资格。
4. 采集器只删除运行期间每 60 秒的递归容量扫描，保留启动时 50GB 检查及其 CLI、配置、
   退出码和 systemd 合同。
5. 不新增清理协调锁、删除隔离区或残留状态机。
6. 清理脚本不得停止、启动或重启采集服务；执行后只验收服务仍为 `active`。
7. `real_source_alignment_probe` 的目标归类、默认产物隔离和写入器主错误传播继续作为第二个
   修复目标。

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
调度：每日一次
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
OnCalendar=daily
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
    验证：需求五项验收均有直接证据，无锁、切换、隔离或完整容量移除等范围扩张。
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
- 任务 2.3：按届时有效的采集身份和发布合同制定并由用户批准具体二进制维护窗。
  - 验证：本事项不预设专用切换，不删除当前日，也不迁移旧新身份；若同日清单身份阻止换版，
    停止并由用户决定，不自行扩展切换设计。
- 任务 2.4：在获得二进制部署、服务启停和文件安装授权后，部署已验证二进制、脚本及两个
  systemd 单元；脚本按 `0755` 安装，真实源验证工具不部署，timer 保持未启用且未启动。
  - 验证：活动二进制及三个文本文件的 SHA-256 与已审查产物一致，脚本可执行，采集服务恢复为
    `active`；静态确认 timer 为每日，并确认其仍为 `disabled`、`inactive`，不会触发删除。
- 任务 2.5：只有另获持续自动删除及其首次启用授权后，才启用并启动 timer，允许其执行可能
  达到 30GB 的生产运行。
  - 验证：timer 为 `enabled`、`active`，下一次触发时间符合每日调度；低水位零删除，真实删除
    时最多一个最早历史日；脚本执行后采集服务仍为 `active`。
- 任务 2.6：把真实提交、产物 SHA、部署时间、脚本结果和服务状态同步到本事项三件套、
  `AGENTS.md`、`data-collect/docs/CURRENT.md` 及受影响的当前权威文档，再做独立验收。
  - 验证：只有生产证据完整后才把候选行为写成当前事实。
- 任务 2.7：阶段 2 独立验收通过后，只有用户另行授权最终文档提交和推送时才执行对应 Git
  动作。
  - 验证：提交只包含获批实现的真实生产证据与当前权威同步，本地与远端分支指向同一提交；
    未获得授权时阶段 2 停留在等待提交和推送，不自行创建 PR。

阶段 2 只有在生产验收、文档同步、独立复审、获授权提交和推送全部完成后才结束。

## 4. 测试与需求验收映射

| 需求验收 | 本地证据 | 生产证据 |
| --- | --- | --- |
| 1. 只删除周期扫描并保留启动 50GB | 精确差异、残留搜索、容量模块与启动退出 `75` 既有测试、普通回归 | 新二进制身份；启动合同未变 |
| 2. 30GB、每日、最早一天 | 两个脚本夹具、`bash -n`、timer 静态验证 | 获批启用后的 timer 状态、下一次触发时间及一次获批运行日志 |
| 3. 脚本不控制服务且执行后仍为 `active` | 源码和 systemd 单元依赖关系静态审查 | 脚本执行后的 `systemctl --user is-active data-collect-training-collector.service` |
| 4–5. 探针显式运行、目录隔离和主错误 | Cargo 元数据、最小离线单元测试、标准完成门及独立复审 | 不部署；显式真实运行仅在用户另行批准时执行 |

## 5. 明确不实施

- 不删除启动时 50GB 检查及其 CLI、配置、退出码 `75` 或采集 systemd 合同。
- 不新增锁、清单资格、隔离删除、残留状态机、进程身份持续性验收或专用发布切换，也不恢复
  用户已经否决的附加测试。
- 不修改其他 `bench`、生产数据合同或生产写入器，不让真实网络探针进入普通回归。
- 不在远端编译或执行 `git pull`，也不在未授权时提交、推送、部署、启停服务或删除数据。

## 6. 当前游标与下一次允许动作

用户已批准需求和计划，并已单独授权阶段 1 的实现、必要测试和静态验证。任务 1.1 至 1.4.3
已完成并通过独立阶段复审；用户现已授权任务 1.4.4 的提交和推送，当前正在执行该任务。真实
网络探针未执行。

阶段 2、PR、发布构建、远端部署、服务操作和真实数据删除均未获授权。
