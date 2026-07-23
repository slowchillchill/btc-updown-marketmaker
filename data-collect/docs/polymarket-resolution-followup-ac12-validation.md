# Polymarket 结算追踪 AC-12 本地香港线路真实数据验收报告

> 事项编号：`DC-REPAIR-20260721-POLYMARKET-RESOLUTION-FOLLOWUP`
>
> 对应需求：`demand/data-collect-polymarket-resolution-followup-demand.md`
>
> 对应计划：`plan/data-collect-polymarket-resolution-followup-plan.md`
>
> 对应 changelog：`changelog/data-collect-polymarket-resolution-followup-changelog.md`
>
> 报告状态：`PHASE_4_COMPLETE_LOCAL_HK_VPN_AC12_AC13_PASS_2026-07-23`

## 1. 结论

本地真实 source 验收满足 AC-12，阶段4全量命令满足 AC-13：

- 精确 release binary 连续采集 `1802` 秒并正常退出，collector 与线路监控退出码均为 `0`；
- 识别 `5` 个完整五分钟 market，其中 `4` 个在 `E+3m` 前取得真实
  `market_resolved` WS terminal，满足 AC-12 允许的真实分支；
- `31/31` 份 before/running/after 线路样本为同一
  `8.210.176.1 / HK / HKG` 出口，覆盖每个完整 market window；
- collector 进程树的 `22/22` 次 IPv4/IPv6 `connect` 都只到
  `127.0.0.1:20810`，非 loopback connect 为 `0`；
- `11/11` 个 manifest file facts与文件字节数、SHA-256、Parquet行数、footer和
  embedded fact一致；没有 `.running`、`.tmp`、`.partial` 或 digesting文件；
- metadata为精确 `68` 字段v3 schema，共 `25` 行；5条WS terminal与5条
  `polymarket_market` raw `market_resolved`逐条唯一对应；
- 全量test、check、Clippy、fmt和diff check全部退出 `0`。

因此 AC-12 与 AC-13 的实现/运行证据已经通过主代理自审；需求映射、文档/验收与
范围/简洁性三路独立Ultra终审最终均为PASS，Critical 0、Important 0、Minor 0。
阶段4已满足获授权的最终commit/push门；production部署仍是独立授权门。

## 2. 授权与执行边界

用户授权完整阶段1至阶段4、本地构建/全量测试、每阶段独立审查后的commit/push，以及
最终在本机通过既有香港线路执行 AC-12。以下操作未获授权，本次也没有执行：

- production部署或production service操作；
- 访问或改变AWS数据采集、paper或live主机；
- 修改系统VPN、默认路由、firewall或systemd service；
- 系统安装软件包；
- 删除或改写既有数据。

实际方法是只对本次精确 collector 进程使用既有
`127.0.0.1:20810` 香港线路和strict fail-closed包装器。主机默认路由及继承的通用代理
都不是验收依据；`collector_region=local-hk-vpn-ac12`也只是provenance标签。

## 3. Build与来源

| 项目 | 证据 |
|---|---|
| repository | `/home/chillchill/fix-rtds-silents` |
| branch | `fix-rtds-silents` |
| Phase 0 commit | `948941ea552a43eb6b2754188f35864c07e0b52b` |
| Phase 1 commit | `5db96e61e25d791ad8a5e31699f2fa4edf8e6a77` |
| Phase 2 commit | `b713d3ca98ae87fcefcda1d7094bd06bc7769c58` |
| Phase 3 / build commit | `a32226863428dcbd82b016538f36855240a41892` |
| source/upstream/remote | 三者均为 `a32226863428dcbd82b016538f36855240a41892` |
| release binary | `validation-artifacts/.../binary/data-collect-training-collector` |
| binary SHA-256 | `82aa27fb3f1dfdb425b9e9aeae97e4666ac9bfd352908afe95a2eee3ffdad0ca` |
| binary size | `29,430,920` bytes |
| ELF Build ID | `d0f9ed9fa35564844051948ae8a904f9d9e0500d` |
| build receipt SHA-256 | `1e9b7725f1ba64fbd4a4443a06aef64ad0d75d47f5332cbea8b79fbb92dfecfa` |

构建命令：

```bash
DATA_COLLECT_GIT_COMMIT=a32226863428dcbd82b016538f36855240a41892 \
CARGO_TARGET_DIR=target/data-collect-resolution-followup \
cargo build -p data-collect \
  --bin data-collect-training-collector --release --locked
```

构建前 tracked worktree为clean。以下3个与本事项无关的untracked RTDS文档始终排除：

- `changelog/data-collect-rtds-business-silence-full-reconnect-changelog.md`
- `demand/data-collect-rtds-business-silence-full-reconnect-demand.md`
- `plan/data-collect-rtds-business-silence-full-reconnect-plan.md`

## 4. 本地香港线路与fail-closed证据

### 4.1 最小包装边界

现有香港代理服务在整个验收期间保持：

- `ActiveState=active`、`SubState=running`；
- `MainPID=3706`；
- `NRestarts=0`；
- listener为 `127.0.0.1:20810`。

为了同时覆盖官方SDK的Gamma HTTP与market/RTDS WebSocket TCP路径，本次只把Ubuntu
`proxychains-ng 4.17-3build1`官方包解压到隔离artifact，使用绝对
`LD_PRELOAD`、`strict_chain`、`proxy_dns`和唯一
`socks5 127.0.0.1 20810`。没有执行系统安装，也没有修改Rust、SDK或production配置。

供应链哈希：

| artifact | SHA-256 |
|---|---|
| `proxychains4_4.17-3build1_amd64.deb` | `d71e5a4d6930d6489c88fef5066ec62620331bd7d4ec04fd987047b08297099b` |
| `libproxychains4_4.17-3build1_amd64.deb` | `dbd367704959fe68654588d85fda2318a7bc4d90ca8ddd80c2dae58edf017fbf` |
| loaded `libproxychains.so.4` | `3959be10c4ecd9c6655588c99c85d8116f10cca7fa60b8ae45f69f69e59c592a` |
| strict HK config | `fa60d528a8f2b2638f4507b0f34196b4f24be4710518c1b64ce6db434017e27e` |

该包装器是本地validation harness，不是Cargo依赖、SDK旁路或production网络实现。

### 4.2 正负控制与精确binary smoke

启动正式计时前完成：

1. IPInfo、Cloudflare、IPify、IPWho四源一致指向
   `8.210.176.1 / HK / HKG`；
2. 把同一strict配置指向无人监听的 `127.0.0.1:20812`，curl退出 `7`，
   syscall只尝试该本地端口，没有直连；
3. 现有官方SDK smoke通过Gamma discovery、CLOB market WS业务消息和RTDS业务消息，
   三条连接均只到 `127.0.0.1:20810`；
4. 精确release binary以独立throwaway dataset运行45秒并退出 `0`，strace记录的
   `6/6` 次connect均只到 `20810`，6类Parquet与manifest正常finalized且无临时文件；
   正式1800秒run的 `/proc/<pid>/maps` 另行证明加载上述绝对路径library。

### 4.3 正式运行线路结果

| 指标 | 结果 |
|---|---:|
| before/running/after route samples | `31` |
| route failures | `0` |
| IPWho第三源交叉样本 | `7` |
| 唯一出口IP | `8.210.176.1` |
| IPInfo country/city | `HK / Hong Kong` |
| Cloudflare loc/colo | `HK / HKG` |
| proxy PID集合 | `{3706}` |
| proxy restart集合 | `{0}` |
| collector IPv4/IPv6 connect | `22` |
| connect到 `127.0.0.1:20810` | `22` |
| 非loopback connect | `0` |
| proxychains成功链 | `22` |
| proxychains失败链 | `0` |

`strace`从exec开始覆盖collector进程树。`/proc` maps、非quiet chain日志、每分钟线路样本和
connect inventory分别证明library实际加载、目标逐条走代理、香港出口连续和不存在已知
TCP路径的静默直连回退。它们不被表述为通用内核级network namespace保证。

## 5. 正式采集运行

| 项目 | 结果 |
|---|---|
| validation root | `/home/chillchill/validation-artifacts/data-collect-resolution-followup-ac12-20260723T121902Z` |
| dataset root | 上述root下的 `dataset` |
| start | `2026-07-23T12:30:31.728772491Z` |
| finish | `2026-07-23T13:00:33.702730918Z` |
| elapsed | `1802` seconds |
| collector exit | `0` |
| route monitor exit | `0` |
| temporary marker count | `0` |
| manifest state | `open`，但 `collection_day_coverage.state=ended_known` |
| manifest gaps | `0` |

当前UTC日manifest保持 `open` 是现有daily manifest合同允许的状态；退出是否干净由
`ended_known` coverage、shutdown-finalized file facts和零临时文件共同证明。

manifest共记录11个Parquet：

| source kind | files | rows |
|---|---:|---:|
| `binance_aggtrade` | 2 | 30,515 |
| `binance_depth_snapshot` | 1 | 1 |
| `binance_depth_update` | 2 | 17,985 |
| `polymarket_market` | 2 | 1,706,280 |
| `polymarket_market_metadata` | 2 | 25 |
| `polymarket_rtds_chainlink_btc_usd` | 2 | 1,738 |

本事项目标文件的file fact全部通过；manifest SHA-256为
`62d782f4848d6ea2debc9e7216ee0f1cf7434f0f48d22782be0ecf47b3f6d045`。

## 6. 完整market与真实终态

以下5个market具备T前prefetched snapshot、T时
`prefetched_to_active + next_to_current`、E时
`current_to_resolution_pending`，且 `E-T=300s`。每个 `[T,E)` 内都有5份香港线路样本。

| slug | T | E | HK samples | 真实分支 |
|---|---|---|---:|---|
| `btc-updown-5m-1784810100` | 12:35Z | 12:40Z | 5 | WS terminal `Down` at 12:41:29.909Z |
| `btc-updown-5m-1784810400` | 12:40Z | 12:45Z | 5 | WS terminal `Down` at 12:47:18.169Z |
| `btc-updown-5m-1784810700` | 12:45Z | 12:50Z | 5 | WS terminal `Up` at 12:51:29.217Z |
| `btc-updown-5m-1784811000` | 12:50Z | 12:55Z | 5 | WS terminal `Up` at 12:56:29.927Z |
| `btc-updown-5m-1784811300` | 12:55Z | 13:00Z | 5 | 运行结束前进入pending，尚未到E+3 |

前4个完整market均在其 `E+3m` slot前取得唯一WS terminal，因此不再发Gamma probe符合
terminal-once与有界follow-up合同；`resolution_poll_attempt=0`不是漏测。启动时已处于current的
`btc-updown-5m-1784809800`不计入完整market，但也在12:37:40.149Z取得真实WS terminal。

最终：

- complete markets：`5`；
- complete-market WS terminal path：`4`；
- metadata WS terminal总数：`5`；
- raw `market_resolved`总数：`5`；
- 5条terminal的slug、receive、payload digest、winning asset与outcome均各匹配唯一raw row。

## 7. v3 schema与时间线复算

两个metadata文件均为 `polymarket_market_metadata_v3`，schema字段名、顺序、类型与
nullability精确匹配68字段合同；没有v2文件作为identity、pending、attempt或terminal authority。

实际row kinds：

| row kind | rows |
|---|---:|
| `metadata_snapshot` | 8 |
| `market_state_transition` | 12 |
| `resolution_terminal_ws` | 5 |
| `resolution_poll_attempt` | 0 |
| `resolution_followup_summary` | 0 |

AC-12只要求取得WS terminal、Gamma terminal或retry/exhausted之一，不要求真实运行把五类row
全部制造为非零。required/null和19类人工失败枚举已由确定性Rust合同测试覆盖；本地artifact
检查器只复算这次实际出现的schema、identity、reference、digest、ownership与时间线，避免形成
第二套生产validator authority。

时间字段保持分离：

- WS terminal保存 `ws_platform_timestamp_raw/utc`；
- `source_evidence_received_at_utc`与本地 `timestamp_received`分别保存并在本轮相等；
- validation completed、lifecycle/runtime-role applied与collector admission时间分别保存；
- 本轮terminal均由WS先取得，所以terminal rows的Gamma fetch、`closedTime`为null；
- Gamma discovery snapshot仍保存各自 `metadata_fetch_start_utc`和source receive；
- 没有用 `closedTime`推断历史可见时间，也没有恢复被用户拒绝的固定5秒future-skew门。

本地WS platform-to-receive差值只描述本机香港线路观测，不能冒充AWS爱尔兰paper/live延迟。

精简只读检查器：

```bash
python3 <validation-root>/tools/ac12_offline_validate.py \
  --dataset-root <validation-root>/dataset \
  --expected-commit a32226863428dcbd82b016538f36855240a41892 \
  --expected-binary-sha256 \
    82aa27fb3f1dfdb425b9e9aeae97e4666ac9bfd352908afe95a2eee3ffdad0ca \
  --expected-host-id chillchill-AMD128G \
  --expected-region local-hk-vpn-ac12 \
  --minimum-complete-markets 2
```

结果：`PASS`、`violations=[]`。脚本与结果SHA-256分别为：

- `f254dea3db0f6049018d5b22a2595e85ddbdb67e821503ecf7fbe395df2042b8`
- `920f472b22d4adc9356ed22f47a68f991d18edad1370a2f665511a5fb519fc59`

## 8. AC-13本地全量验证

独立target dir为 `target/data-collect-resolution-followup`。保存日志后再次执行：

```bash
CARGO_TARGET_DIR=target/data-collect-resolution-followup \
  cargo test -p data-collect --locked -- --test-threads=1
CARGO_TARGET_DIR=target/data-collect-resolution-followup \
  cargo check -p data-collect --tests --locked
CARGO_TARGET_DIR=target/data-collect-resolution-followup \
  cargo clippy -p data-collect --locked --all-targets -- -D warnings
cargo fmt --all -- --check
git diff --check
```

全部退出 `0`；完整lib为 `347/347`，所有integration targets与doc-tests通过。日志SHA-256：

| log | SHA-256 |
|---|---|
| cargo test | `da5444a58decdbdcb413b60cbe69b3ce440101cdd05413d744ad9653ad6d090f` |
| cargo check | `a41051563a4cb5fc1b1bcdd9aa0ba28ca80f7736d629d90cc138218756e8e3a9` |
| Clippy | `f0788aa36d1c86b6ccdfb7f4042a5574102d82061c046422a2d42e008a1e4710` |

Phase1只新增测试所需 `tokio test-util` dev feature；Phase3及Phase4没有新增manifest、lock、
vendor或production网络依赖。新增Polymarket production调用仍只有官方
`GammaClient`、`MarketBySlugRequest`和既有官方SDK actor；没有raw REST、manual WS/signing、
第二subscription owner、checkpoint或shadow implementation。

## 9. 证据索引与限制

隔离artifact中的主要authority：

| 证据 | SHA-256 |
|---|---|
| `evidence/build_receipt.json` | `1e9b7725f1ba64fbd4a4443a06aef64ad0d75d47f5332cbea8b79fbb92dfecfa` |
| `evidence/ac12-run-status.json` | `6258808e19a482c7ac07bab465f380dbe9dc31e8894fe40c226d325f822c0c93` |
| `evidence/hk-route-samples.jsonl` | `a09d15762b11e84ad280f8ab9e61316cb7efe079982ce08b329d82eba0bf3f44` |
| `evidence/hk-route-summary.json` | `f7bbdcc1eebcb5314d8eb4132837e1116d1486d74b58dca02fcfc130f72c3c55` |
| `evidence/connect-inventory.json` | `c58aded96fa515bc5b7a39555a147def91cd488f92a4680881d3fd43d54d77f1` |
| `evidence/route-market-coverage.json` | `3835993046446b796e2f4120fa1401de1d0633380ef05dbbfd7a8a2ddf6a3d12` |
| `evidence/ac12-offline-validation.json` | `920f472b22d4adc9356ed22f47a68f991d18edad1370a2f665511a5fb519fc59` |
| `evidence/artifact-inventory.sha256` | `d11219fe4e6e4cc14e7bec80a027e349a0daaf0934890d2c27f3a776338d94c6` |

`artifact-inventory.sha256`固定artifact根下其自身之外的223个普通文件，包括正式dataset与
throwaway smoke dataset。带
`ac12-offline-validation-pyc`前缀的JSON仅是检查器开发期间的诊断输出；本报告引用的唯一
离线复算authority是上表中的 `ac12-offline-validation.json`。

证据边界：

- 真实运行证明本次已知TCP路径在精确binary上没有静默直连；不扩大为对任意未知binary的
  内核级网络隔离保证；
- 本轮真实市场走WS-first路径，没有人为延迟terminal或制造Gamma retry/exhausted；
- v2/restart/crash cuts、失败precedence和所有row-kind required/null组合以已通过的确定性Rust
  测试为authority，不用本次live数据重复造景；
- 本报告不把本地香港线路的receive timing解释为AWS Ireland latency、市场历史首次可见时间
  或盈利证据。

## 10. RF/AC结果索引

| 需求/验收 | 主要证据 | 结果 |
|---|---|---|
| RF-01至RF-11、AC-01至AC-11 | Phase1至Phase3 commits、347/347 lib及integration/recovery/resource tests | PASS |
| AC-12 multiple complete markets | 5个完整market、每窗5份HK线路样本 | PASS |
| AC-12真实结算路径 | 4个完整market WS terminal，5/5 raw linkage | PASS |
| AC-12无静默直连 | 31份线路样本、22/22本地connect、0非loopback | PASS |
| AC-12时间语义 | v3 per-market timeline，WS/Gamma/receive/lifecycle/admission分离 | PASS |
| AC-13 full regression | test/check/Clippy/fmt/diff logs | PASS |
| Phase4独立终审 | 三路独立Ultra终审 | PASS（Critical 0 / Important 0 / Minor 0） |

## 11. 阶段4独立终审

artifact检查器第四轮专项复审为PASS，Critical 0、Important 0、Minor 2；两项Minor分别是
未来合法小写outcome可能被检查器偏严拒绝，以及未重复核对已由embedded facts交叉验证的
source/utc_day目录语义。当前冻结payload与路径均正确，专项审查明确建议不再扩张检查器，
主代理接受这一收敛判断。

全面终审期间另发现并修复：

1. 报告把正式run的`/proc/maps`证据误归到45秒smoke，并把6次connect误写为6个活动socket；
2. 首版inventory只固定216个文件，漏掉报告引用的smoke manifest与6个Parquet。

修复后，需求映射、文档/验收、范围/简洁性三路独立全面终审分别复核当前稳定artifact与四份
文档，最终均为PASS，Critical 0、Important 0、Minor 0。三路均确认本次香港线路包装、线路采样、
syscall审计和artifact-local检查器与AC-12风险相称，没有production SDK旁路、第二authority、
过度设计或过度校验。
