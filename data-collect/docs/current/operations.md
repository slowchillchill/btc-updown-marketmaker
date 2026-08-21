# data-collect 当前运行与部署边界

> 生命周期：`CURRENT_REFERENCE`
>
> 当前执行 authority：`none`
>
> 当前任务事实源：`https://github.com/qiuqiuaiweb3/btc-updown-marketmaker/issues/21`
>
> 历史读取入口：`data-collect/docs/archive-manifest.tsv`
>
> 执行授权：`none`

本文是从 GitHub 模块 Issue #21 和真实实现派生的非权威参考，不得新增或改变 Issue 语义；出现不一致时按
`data-collect/docs/CURRENT.md` 的冲突门处理。

本文只汇总当前构建、路径、进程与观察参考。它不证明远端服务当前状态，也不授权部署、
上传、替换二进制文件、启停服务、删除数据或其他远端变更；这些动作必须由届时真实状态
支持，并有另行批准的 GitHub Issue。

## 1. 构建合同

- 发布二进制文件必须在本地从干净且已推送的完整提交隔离构建。
- 构建时注入 `DATA_COLLECT_GIT_COMMIT=<完整 commit>`，构建前后复核提交与干净状态，
  记录二进制文件 SHA-256。
- 数据采集、模拟盘和实盘服务器都不是 Rust 构建机，不执行 `cargo build/test/check`
  或服务器 `git pull`。
- 模拟盘和实盘二进制文件不进入训练部署产物。

## 2. 训练程序固定路径与单元

```text
/home/ubuntu/btc-updown-marketmaker/data-collect/bin/data-collect-training-collector
/home/ubuntu/btc-updown-marketmaker/datasets
/home/ubuntu/.config/systemd/user/data-collect-training-collector.service
```

仓库内唯一单元源文件：

```text
data-collect/deploy/systemd/data-collect-training-collector.service
```

固定单元行为包括 `Restart=on-failure`、`RestartSec=10`、
`RestartPreventExitStatus=75 76`、`StartLimitIntervalSec=600`、
`StartLimitBurst=5`、`KillSignal=SIGTERM` 和 `TimeoutStopSec=120`。

仓库当前没有权威的模拟盘、实盘 systemd 单元，也没有已验收的模拟盘、实盘生产部署。

生产服务器已经安装以下独立外部清理产物：

```text
data-collect/deploy/scripts/prune-oldest-utc-day.sh
data-collect/deploy/systemd/data-collect-prune-oldest-day.service
data-collect/deploy/systemd/data-collect-prune-oldest-day.timer
```

安装路径为 `/home/ubuntu/btc-updown-marketmaker/data-collect/bin/prune-oldest-utc-day.sh` 及
`/home/ubuntu/.config/systemd/user/` 下的同名单元。版本化 timer 配置为每日 UTC
`12:00:00`；实际启用状态和下一次触发时间必须从目标主机只读核对，不由本文声明。

## 3. 训练程序退出与容量

| 退出码 | 当前含义 |
| ---: | --- |
| `0` | 有界运行结束、操作员信号后的协调停止，或训练程序帮助、版本命令成功 |
| `1` | 普通运行错误或运行时创建失败 |
| `75` | 当前生产的启动容量门直接退出 |
| `76` | 配置、路径、恢复或数据合同要求操作员介入 |

模拟盘和实盘二进制程序正常完成心跳循环时退出 `0`，任一错误固定退出 `1`；
`75/76` 映射只属于训练二进制程序。

当前生产二进制只在启动前扫描一次。启动扫描已经达到上限时，不启动数据源、不创建类型化
原始数据、清单或运行时产物，直接退出 `75`；运行期间不再递归扫描容量。退出 `75/76` 不得
形成 systemd 自动重启循环，采集器本身也不自动清理最终化原始数据。

独立外部任务每日 UTC `12:00:00` 读取根文件系统 `df /` 已用字节数：低于十进制 30GB 时不
删除；达到或超过 30GB 时，只删除活动数据根中严格早于 UTC 昨日的最早一个日期目录。脚本
不控制采集服务，不读取日期目录的业务内容，也不使用文件锁或隔离目录。脚本和单元已安装，
timer 已启用；首次自然调度尚未发生，脚本尚未在生产运行。

## 4. 只读运行观察

在另行批准的部署或启动已经发生后，才可将下列项目作为只读观察合同：

- 服务、PID、二进制文件 SHA 与重启计数相互一致；
- 当日清单为真实 `open` 状态，身份信息与覆盖情况合法；
- 六个数据源计数器存在；稀疏数据源零行或暂时没有文件可以合法；
- `.parquet.running` 不作为最终化数据读取；
- 自然小时边界正确最终化有数据的数据源；
- 自然 UTC 日切换按旧日围栏、最终化、关闭后再启用新日；
- 普通数据源静默或暂时无数据不等于整体进程失败；
- RSS、FD、线程、I/O 与磁盘趋势没有无界增长。

这些观察不是启动前门。真实 UTC 日切换和完整 UTC 日证据当前不声明为已取得，也不是
模块完成门。

## 5. 失败与未来部署

发现完整性错误、重启风暴或容量退出时，必须保留真实数据、清单、`systemd journal` 与
`digest` 证据，并按另行批准的事故处置手册处理。本文件不授权停服或清理。

旧直接冷启动、同日重置、发布包、保留策略管理器、归档上传器、二进制文件别名和服务器
源码检出目录均不是当前部署路径。每日外部清理 timer 已按 UTC `12:00:00` 启用；首次自然
调度结果尚未发生。其他未来变更或手动运行仍须按届时提交、脚本、服务、数据目录和环境重新
核实并取得授权。
