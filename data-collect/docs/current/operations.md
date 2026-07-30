# data-collect 当前运行与部署边界

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

本文只定义当前构建、路径、进程与观察合同。它不证明远端服务当前状态，也不授权部署、
上传、替换 binary、启停服务、删除数据或其他远端变更；这些动作必须由届时真实状态
支持，并有另行批准的活动需求、计划与 changelog。

## 1. 构建合同

- release binary 必须在本地从 clean、已 push 的完整 commit 隔离构建。
- 构建时注入 `DATA_COLLECT_GIT_COMMIT=<完整 commit>`，构建前后复核 commit 与 clean
  状态，记录 binary SHA-256。
- 数据采集、paper 和 live 服务器都不是 Rust 构建机，不执行 `cargo build/test/check`
  或服务器 `git pull`。
- paper/live binary 不进入 training 部署产物。

## 2. Training 固定路径与 unit

```text
/home/ubuntu/btc-updown-marketmaker/data-collect/bin/data-collect-training-collector
/home/ubuntu/btc-updown-marketmaker/datasets
/home/ubuntu/.config/systemd/user/data-collect-training-collector.service
```

仓库内唯一 unit 源：

```text
data-collect/deploy/systemd/data-collect-training-collector.service
```

固定 unit 行为包括 `Restart=on-failure`、`RestartSec=10`、
`RestartPreventExitStatus=75 76`、`StartLimitIntervalSec=600`、
`StartLimitBurst=5`、`KillSignal=SIGTERM` 和 `TimeoutStopSec=120`。

仓库当前没有 authoritative paper/live systemd unit，也没有已验收的 paper/live
production deployment。

## 3. Training 退出与容量

| exit code | 当前含义 |
| ---: | --- |
| `0` | 有界运行结束、操作员信号后的协调停止，或 training help/version 成功 |
| `1` | 普通运行错误或 runtime 创建失败 |
| `75` | startup 容量 gate 直接退出，或运行期容量 gate 协调停止后退出 |
| `76` | 配置、路径、恢复或数据合同要求 operator intervention |

paper/live binary 正常完成 heartbeat loop 时退出 `0`，任一错误固定退出 `1`；
`75/76` 映射只属于 training binary。

容量在启动前扫描一次，并在运行期间每 `60s` 扫描：

- 启动扫描已经达到上限：不启动 source、不创建 typed raw/manifest/runtime
  artifact，直接退出 `75`。
- 运行期间达到上限：停止新 admission，drain/finalize 已接纳数据并更新适用
  manifest，然后退出 `75`。

collector 不自动清理 finalized raw；退出 `75/76` 不得形成 systemd 自动重启循环。

## 4. 只读运行观察

在另行批准的部署或启动已经发生后，才可将下列项目作为只读观察合同：

- service、PID、binary SHA 与 restart 计数相互一致；
- current-day manifest 为真实 `open` 状态，identity 与 coverage 合法；
- 六个 source counter 存在；稀疏 source 零行或暂时没有文件可以合法；
- `.parquet.running` 不作为 finalized 数据读取；
- 自然小时边界正确 finalize 有数据的 source；
- 自然 UTC rollover 按旧日 fence/finalize/close 后新日 activation；
- 普通 source 静默或暂时无数据不等于整体进程失败；
- RSS、FD、线程、I/O 与磁盘趋势没有无界增长。

这些观察不是启动前 gate。真实 UTC rollover/full-day 证据当前不声明为已取得，也不是
模块 completion gate。

## 5. 失败与未来部署

发现完整性错误、restart storm 或容量退出时，必须保留真实数据、manifest、journal
和 digest 证据，并按另行批准的 incident/runbook 处置。本文件不授权停服或清理。

旧直接冷启动、同日 reset、release bundle、retention admin、archive uploader、
binary alias、服务器 source checkout 与自动 raw 删除均不是当前部署路径。未来部署
必须按届时 commit、binary、service、manifest、数据目录和环境重新建立并批准三件套。
