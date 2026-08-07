# data-collect 当前运行与部署边界

> 生命周期：`CURRENT_REFERENCE`
>
> 当前执行 authority：`none`
>
> 当前规范来源：`plan/data-collect-plan.md`
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

本文是从主计划和真实实现派生的非权威参考，不得新增或改变计划语义；出现不一致时按
`data-collect/docs/CURRENT.md` 的冲突门处理。

本文只汇总当前构建、路径、进程与观察参考。它不证明远端服务当前状态，也不授权部署、
上传、替换二进制文件、启停服务、删除数据或其他远端变更；这些动作必须由届时真实状态
支持，并有另行批准的活动需求、计划与变更记录。

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

## 3. 训练程序退出与容量

| 退出码 | 当前含义 |
| ---: | --- |
| `0` | 有界运行结束、操作员信号后的协调停止，或训练程序帮助、版本命令成功 |
| `1` | 普通运行错误或运行时创建失败 |
| `75` | 启动容量门直接退出，或运行期容量门协调停止后退出 |
| `76` | 配置、路径、恢复或数据合同要求操作员介入 |

模拟盘和实盘二进制程序正常完成心跳循环时退出 `0`，任一错误固定退出 `1`；
`75/76` 映射只属于训练二进制程序。

容量在启动前扫描一次，并在运行期间每 `60s` 扫描：

- 启动扫描已经达到上限：不启动数据源、不创建类型化原始数据、清单或运行时产物，
  直接退出 `75`。
- 运行期间达到上限：停止新准入，排空并最终化已接纳数据并更新适用清单，然后退出
  `75`。

采集器不自动清理最终化原始数据；退出 `75/76` 不得形成 systemd 自动重启循环。

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

旧直接冷启动、同日重置、发布包、保留策略管理器、归档上传器、二进制文件别名、
服务器源码检出目录与自动删除原始数据均不是当前部署路径。未来部署必须按届时提交、
二进制文件、服务、清单、数据目录和环境重新建立并批准三件套。
