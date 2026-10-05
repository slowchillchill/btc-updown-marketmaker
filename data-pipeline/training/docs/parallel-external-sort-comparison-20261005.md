# #251 有界并行外排对照

同轮合并组并行处理彼此独立的有序段。它缩短可并行轮次的等待时间，并保留后续分组和业务输入顺序。本轮对应 [#251](https://github.com/slowchillchill/btc-updown-marketmaker/issues/251)，消费任务为 #185。

## 实现与边界

`SortConfig.merge_workers` 是显式线程上限。旧 JSON 配置缺省为 1。协调线程先编号各组，再按组号收集输出。工作完成先后不改变下一轮分组、键、载荷或逻辑摘要。工作线程只共享同一 sorter 的资源计数，不共享消费者的 `RefCell` 通知器。

正在写入和已封片输出，以及尚未删除的输入，共享原临时字节、物理文件和打开句柄额度。关闭文件只释放句柄；删除文件才释放磁盘额度。调度同时考虑每组输入读者和输出 writer。`fan_in=8/open_files=16` 最多运行一个完整组；请求 4 workers 不会扩额，也不会因此新增拒绝。四个完整组需要 36 个句柄。

失败后不再派发新组。协调线程收回全部活动工作，再沿原路径清理本次排序。不存在成功发布的部分出口。初始 run、不同来源、最终单组、连续 core 和研究统计仍顺序执行。Gate-A 与 hidden 的 V1 持久出口保持单文件，原 Parquet 容量错误继续传播。

合并进度包含来源、轮次、完成组/总组、实际活动 workers、实际处理行数和资源峰值。协调线程低频转发原通知器。研究例子沿显式进度间隔输出；backtest 沿原间隔输出。处理行数跨轮累计，不能当成去重输入行数。`merge_elapsed_ns` 是各合并轮墙钟耗时之和。缓冲额度按每个工作者适用；它不表示总 RSS 上限。

## 固定测量

源提交：`a82488a8cb438a8b02b9d818e775848181779b64`。使用同一 release 二进制，SHA-256 为 `1bc61f53d6f906a84bb51cc3cde166480cc20b88c648376e3dbc68ae2bbf70e9`，大小 14,686,664 B。工具链为 rustc 1.95.0 / cargo 1.95.0。UTC 2026-10-05，先运行 1 worker，再运行 4 workers，各一次。

输入固定为 1,048,576 行，每行载荷 256 B。键由固定可逆乱序产生。验证逐条读取完整输出，独立生成预期键和载荷，并独立计算逻辑摘要。输入与预期构造见 [`measure_parallel_external_sort.rs`](../examples/measure_parallel_external_sort.rs)。

两次都使用 run 16,384 行 / 64 MiB、batch 1,024 行、fan-in 8、open-files 40、临时文件 256、临时字节 2 GiB。均形成 64 个初始 run、2 轮合并。合并共处理 2,097,152 行。

| 实测项 | 1 worker | 4 workers |
|---|---:|---:|
| 排序墙钟（秒） | 10.841 | 8.658 |
| 合并墙钟（秒） | 7.429 | 5.278 |
| 完整程序墙钟，含独立验证（秒） | 11.157 | 8.968 |
| CPU 用户 / 系统时间（秒） | 2.13 / 1.37 | 2.25 / 1.26 |
| time CPU 使用率 | 31% | 39% |
| RSS 峰值（KiB） | 31,476 | 52,220 |
| 实际合并 workers 峰值 | 1 | 4 |
| 实际打开句柄峰值 | 9 | 36 |
| 临时物理文件峰值 | 65 | 68 |
| 临时磁盘字节峰值 | 554,287,310 | 554,287,310 |

两次完整对照通过，进程退出码均为 0。逻辑摘要一致：`0cd7d89952f9903c3ca1239182be6572e808b59e5a9f83fb92648aeb7020da8c`。详细配置、计数和结果见[机器摘要](parallel-external-sort-comparison-20261005.json)。

测量命令如下。两个输出目录必须尚不存在。

```bash
cargo build -p data-pipeline-training --example measure_parallel_external_sort --release --locked --offline -j8
/usr/bin/time -v -o workers-1.time target/release/examples/measure_parallel_external_sort 1 workers-1 > workers-1.json 2> workers-1.stderr
/usr/bin/time -v -o workers-4.time target/release/examples/measure_parallel_external_sort 4 workers-4 > workers-4.json 2> workers-4.stderr
```

每种配置只有一次测量。未隔离缓存状态或其他任务的存储活动。最终轮只有一个组，无法用组间并行加速。RSS 随实际 workers 增大。这些数字不证明稳定加速比例，也不是十八日研究的 ETA。本轮没有重跑十八日，没有修改其失败现场或正式采用健康阈值。

## 验证对应

- 强制后组先完成：全部独立预期键、载荷和摘要保持一致。旧 open16 配置实际并发为 1；旧配置反序列化缺省为 1。
- 两个并发 writer 共用 7 B 额度，各写 4 B：只能一个成功，实际磁盘与计数均为 4 B。真实合并重复键失败后，活动线程与句柄均为 0，原清理完成。
- 公共历史夹具对照 1/4 workers：连续 core 输出与前序 TWAP 重读结果一致。复用原真实 32768 行组分片、临时配额、V1 持久容量和公共录制回归。
- 固定 workspace 回归退出 0。`four_source_production_path_sustains_two_to_five_thousand_native_messages_per_second` 明确跳过，其夹具依赖实际 UTC 时钟。fmt 与相关 training/baseline/backtest 全目标 clippy 均退出 0；保留既有依赖告警。

本任务树没有可用代码索引。实现与交付核对读取本任务树当前源码，未使用主树索引代替。有限测量的原日志、time、源补丁、二进制及绑定保存在独立归档，私有位置由任务交接给出。
