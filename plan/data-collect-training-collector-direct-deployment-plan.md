# 训练数据采集器直接长期部署计划

> 事项编号：`DC-DEPLOY-20260718-DIRECT-LONG-RUN`
>
> 对应需求：`demand/data-collect-training-collector-direct-deployment-demand.md`
>
> 对应 changelog：`changelog/data-collect-training-collector-direct-deployment-changelog.md`
>
> 状态：`APPROVED_IN_PROGRESS`

## 阶段 1：冻结部署合同

- 任务 1.1：把任意时刻直接启动、唯一 `Restart=on-failure` unit、无 `Restart=no`/独立 gate/数据删除的用户决策同步到需求、计划、changelog、部署 README 和现行 authority 文档。
  - 验证：三件套同一事项编号且互相引用；旧 M8-A 只保留为历史决策并明确被本事项取代；README contract test 不再要求旧 gate 流程。
- 任务 1.2：只读核对本地 commit/worktree、远端 service/PID、当前日目录与全部collector产物、binary/unit/path identity、磁盘和 glibc。
  - 验证：已知旧数据不被修改；当前UTC日无任何collector产物，若出现任一产物则原样保留并停止。
- 任务 1.3：独立审查本阶段，修复成立问题，运行文档合同测试与 `git diff --check`，只提交本事项文件并 push。

## 阶段 2：构建与上传前兼容验证

- 任务 2.1：从 `c6b8bac3c5d3ce7ea2fa689bd431cf1a8b7070de` 创建 clean detached worktree，使用独立 target 运行 locked release build。
  - 验证：build 前后 HEAD 固定且 worktree clean；`--help`/`--version` 退出 0；记录 file/readelf/loader/build-id 与 SHA-256。
- 任务 2.2：从实际 binary 提取最高 GLIBC requirement。
  - 验证：不高于远端 GLIBC 2.39 才继续；高于 2.39 时停止并请求新的兼容构建决策，禁止远端编译。
- 任务 2.3：上传到正式 binary 同目录下唯一 SHA staging 路径。
  - 验证：本地/远端 SHA 一致；远端 `file`/`ldd`/`--help`/`--version` 通过；service 仍 inactive、无 PID/lease；当前UTC日无任何collector产物；unit SHA 未变化。若发现当前日产物，原样保留并停止。

## 阶段 3：原子安装并直接长期启动

- 任务 3.1：`sync` staging 后在同一文件系统原子替换固定 binary，同步destination所在目录；不替换内容已一致的 unit，不触碰历史数据。
  - 验证：目录同步已完成，正式 binary SHA 正确，路径/owner/mode 正确，staging 不残留。
- 任务 3.2：执行 `systemctl --user enable --now data-collect-training-collector.service`。
  - 验证：唯一正式 unit 为 enabled、active/running，MainPID 大于零，`/proc/<pid>/exe` SHA 正确；从第一次启动起就是 `Restart=on-failure`。

## 阶段 4：即时验收与长期观察交接

- 任务 4.1：有界重复采样 service、PID、`NRestarts`、journal、manifest、逐 source 文件、CPU/RSS/线程/FD/socket/IO 和磁盘。
  - 验证：满足需求 3.1；逐 source 分开报告“已有正向出数证据”或“暂时没有正向证据”，不得要求六路立即出数；同时确认已知2026-07-15 legacy open manifest被中性关闭且没有伪造coverage或unclean gap。
- 任务 4.2：把实现 commit、binary/unit SHA、远端启动时间/PID、manifest identity、初始数据与资源事实写入本 changelog。
  - 验证：独立生产/测试/文档审查无 Critical/Important；提交并 push。
- 任务 4.3：将跨小时、跨 UTC 日和约 24 小时的持续观察交给正在运行的正式 service 与用户人工观察，不停止进程等待报告。
  - 验证：后续按需求 3.2读取真实状态；异常时保留数据与证据，不自动清理或切回旧 binary。
