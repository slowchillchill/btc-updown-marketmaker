# data-collect Hardware Tier Validation

事项编号：`data-collect-demand`

对应需求文档：`/home/chillchill/btc-updown-marketmaker/demand/data-collect-demand.md`

对应计划文档：`/home/chillchill/btc-updown-marketmaker/plan/data-collect-plan.md`

核对时间：`2026-07-06`

## 1. Scope

本次只做只读/低影响 hardware tier 核对：

- 未启动生产采集。
- 未终止远端进程。
- 未删除远端程序或数据。
- 未在 AWS live-trade 小主机运行 `cargo build`、`cargo test`、`cargo check`、`cargo fmt` 或任何 Rust 编译命令。

用户已授权后续在本地开发与测试通过后使用数据采集服务器做测试验证，并授权必要时终止旧采集程序、删除旧采集程序和旧采集数据以释放空间；本次核对尚未使用删除/终止权限。

## 2. 数据采集服务器

主机：`ssh ubuntu@3.255.156.183`

执行命令：

```bash
ssh -o BatchMode=yes -o ConnectTimeout=10 ubuntu@3.255.156.183 'uname -a; nproc; free -h; df -h; ulimit -n; cat /proc/pressure/cpu 2>/dev/null || true; cat /proc/pressure/memory 2>/dev/null || true; cat /proc/pressure/io 2>/dev/null || true'
```

结果摘要：

| 项目 | 值 |
| --- | --- |
| kernel | `Linux ip-172-26-10-0 6.17.0-1007-aws #7~24.04.1-Ubuntu SMP Thu Jan 22 21:04:49 UTC 2026 x86_64` |
| CPU | `2` vCPU |
| memory | `1.9Gi total`, `1.4Gi available` |
| swap | `0B` |
| root filesystem | `/dev/root 58G total, 52G used, 5.8G available, 90% used` |
| ulimit nofile | `1024` |
| PSI | cpu/memory/io available |

判断：

- 数据采集服务器当前硬件 tier 很小，CPU/RAM 与 live 小主机同级；不能假设可承受训练式重处理、历史全量 parquet 扫描或 Rust 编译。
- 根盘仅约 `5.8G` 可用，后续远端采集验证前需要先清理旧采集程序/旧采集数据或选择用户批准的测试 artifact 根；清理动作必须记录命令、影响和清理前后 `df -h`。
- 远端测试工作目录仍应写作 `~/btc-updown-marketmaker`，不得把展开后的 `/home/ubuntu/...` 当成本地开发路径。

## 3. AWS Ireland Live-Trade 服务器

主机：`ssh ubuntu@108.130.74.143`

执行命令：

```bash
ssh -o BatchMode=yes -o ConnectTimeout=10 ubuntu@108.130.74.143 'uname -a; nproc; free -h; df -h; ulimit -n; cat /proc/pressure/cpu 2>/dev/null || true; cat /proc/pressure/memory 2>/dev/null || true; cat /proc/pressure/io 2>/dev/null || true'
```

结果摘要：

| 项目 | 值 |
| --- | --- |
| kernel | `Linux ip-172-26-13-4 6.17.0-1017-aws #17~24.04.1-Ubuntu SMP Tue May 26 21:30:32 UTC 2026 x86_64` |
| CPU | `2` vCPU |
| memory | `1.9Gi total`, `1.4Gi available` |
| swap | `0B` |
| root filesystem | `/dev/root 58G total, 11G used, 47G available, 19% used` |
| ulimit nofile | `1024` |
| PSI | cpu/memory/io available |

判断：

- live-trade 服务器确认是小主机运行边界，不是构建机。
- 仍禁止在该服务器运行 `cargo build`、`cargo test`、`cargo check`、`cargo fmt`、训练式重处理、历史全量 parquet 扫描或归档压缩。
- `/home/ubuntu/live-trade` 仅是该服务器运行产物根；不代表本地开发路径，也不授权 data-collect 在 live 小主机上做训练式数据工作。

## 4. Phase 0 Implications

1. 本地 synthetic writer/throughput 结果不能直接外推到数据采集服务器；该服务器只有 `2` vCPU、`1.9Gi` 内存和低剩余磁盘。
2. 后续远端验证必须先记录清理/部署/运行根 receipt，消费者只能读取 finalized artifact。
3. live 小主机只允许低影响运行边界核对和已构建二进制运行，不承担编译、训练式重处理或历史数据扫描。
