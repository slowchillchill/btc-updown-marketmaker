# SDK070 2-REST owner 模块化报告

> 生命周期：`HISTORICAL_EVIDENCE_ONLY`
>
> 最终快照：`FROZEN_AT_20260730_CONSOLIDATION`
>
> 当前执行 authority：`none`
>
> superseded_by：`DC-DOCS-20260730-CURRENT-AUTHORITY-CONSOLIDATION`
>
> 读取规则：本文仅用于审计历史事实；内部“当前、下一步、待执行、命令”均属于
> 当时检查点，不得驱动当前实现、测试、部署或远端操作。
>
> 2026-07-30：本文是 2-REST 完成报告；RTDS deferred 等当时状态不再是当前游标。
>
> 事项编号：`DC-REPAIR-20260724-SDK070-TYPED-LIVE-RUNTIME`
>
> 对应需求：
> `demand/data-collect-sdk070-typed-schema-live-runtime-refactor-demand.md`
>
> 对应计划：
> `plan/data-collect-sdk070-typed-schema-live-runtime-refactor-plan.md`
>
> 对应 changelog：
> `changelog/data-collect-sdk070-typed-schema-live-runtime-refactor-changelog.md`
>
> 入口 commit：
> `c0aff3a3e61583138b33eec44d847acaa8ebd682`
>
> 状态：`2_REST_CLOSED`
>
> 生效条件：承载本记录的closure commit成功push到
> `origin/fix-rtds-silents`，且本地HEAD、upstream与远端ref一致。
>
> 总事项：`CORE_COMPLETE_RTDS_PARAMETERS_DEFERRED`；RTDS行为专项未完成。

## 1. 结论与边界

2-REST 已把 `live_runtime.rs` 中除 Polymarket、RTDS 既有 owner 之外的有效行为，
按真实职责迁入私有 owner 模块。当前 root 在测试模块前只保留：

- 私有 module wiring、必要 import/re-export 与共享配置常量；
- `IngressControl` 共享控制合同；
- `RuntimeProvenance`、`CollectorIdentity`、`LiveRuntimeSourceSpecs` 配置载体；
- public config、receipt、finalized segment 与唯一 public runtime 入口；
- 唯一顶层 runtime composition；
- 共享 `Result` / `invalid_data` facade glue；
- 一个只供组合测试复用的 `#[cfg(test)]` discovery helper。

独立 root 残留审计结论为
`PASS / CLOSURE_UNBLOCKED`，剩余必做迁移为 0。没有新增 public API、generic
source/actor/plugin framework、第二 feed owner、shadow path 或第二校验框架。

本分支只做行为保持的 owner 搬家、必要 path/visibility 适配和测试归属调整。它没有
改变 schema、payload、fingerprint、admission、queue/resource hard bound、
rollover/manifest/shutdown 顺序、RTDS business-silence authority、paper/live
边界或 production/deployment 状态。

## 2. Owner 与 LOC

LOC 为 2026-07-26 最终 formatter 输出的物理行数，只用于报告，不是验收阈值。
“owner 文件”列包含文件内的测试条件代码；直接测试单独成文件时列在最后一列。

| owner | owner 文件 LOC | 直接测试文件 LOC |
|---|---:|---:|
| root `live_runtime.rs` | 4,966 | `mod tests`为3,151；其前root为1,815，含17行`#[cfg(test)]` discovery helper及最小test-only wiring |
| `binance_aggtrade_source` | 765 | 1,182 |
| `binance_depth_source` | 1,476 | 2,512 |
| `dataset_capacity` | 51 | 21 |
| `dataset_root_lock` | 12 | 12 |
| `ingress_control_sender` | 104 | 62 |
| `ingress_deferred` | 438 | 328 |
| `ingress_owned_bytes` | 109 | 0 |
| `manifest_owner` | 850 | 628 |
| `polymarket_event_adapter` | 907 | 451 |
| `polymarket_source` | 6,076 | 11,043 |
| `producer_day_barrier` | 791 | 494 |
| `raw_day` | 2,224 | 767 |
| `raw_ingress_admission` | 371 | 162 |
| `rollover_coordinator` | 775 | 415 |
| `rtds_source` | 365 | 1,019 |
| `runtime_coverage` | 88 | 0 |
| `runtime_gaps` | 78 | 23 |
| `runtime_ingress` | 295 | 61 |
| `runtime_rollover_driver` | 942 | 1,017 |
| `runtime_shutdown` | 102 | 225 |
| `runtime_time` | 51 | 0 |
| `source_ingress_state` | 464 | 18 |
| `source_recovery` | 219 | 两项直接测试保留在同一 owner 文件 |
| `source_task_supervision` | 572 | 428 |
| `source_topology` | 104 | 49 |
| `startup_coordinator` | 268 | 208 |
| `startup_recovery` | 388 | 973 |
| `writer_supervisor` | 2,717 | 1,716 |

所有 `live_runtime` owner 文件合计 26,568 行，直接测试文件合计 23,814 行。
root 从入口 31,904 行降至 4,966 行；该结果不被解释为净删除。

## 3. moved / deleted / rewritten / added / net

Git 不识别同文件内函数搬家，且 rustfmt 与 `pub(super)` 会改变纯文本行形，因此本
报告不虚构“精确语义移动行数”。终态分类为：

| 分类 | 结果 |
|---|---|
| `MOVE` | 有效 production item 与直接测试按上表迁入唯一 owner；测试完整限定名随模块路径变化 |
| semantic `DELETE` | 0；2-REST 未执行 Stage5 已闭合的死代码删除 |
| semantic `REWRITE` | 0；函数体、状态机、poll/order、错误类别与稳定事件语义不变 |
| behavioral `ADD` | 0；没有新增 runtime 能力、校验或 fallback |
| structural `ADD` | 新增 25 个 production owner 文件、21 个直接测试文件，以及必要 module wiring/visibility |
| Rust file-level churn | `+30,966 / -30,895`，净 `+71` 行 |

其中 46 个新 Rust 文件共 25,555 行；既有 root 的 Git file-level diff 为
`+3,957/-30,895`，既有 Polymarket source/test、RTDS source 与
`inflight.rs` 的必要归属适配合计增加 1,454 行。净增来自 module wiring、
visibility、标准格式和文件边界，不代表新行为。

## 4. 关键 KEEP / MOVE 决策

- `IngressControl` 保留 root。它同时连接 ingress sender、runtime consumer 与
  day-barrier release；移入 `source_ingress_state` 会制造
  `source_ingress_state <-> producer_day_barrier` 的语义依赖环。为消除该环再建
  protocol 模块属于过度设计。
- `RuntimeProvenance`、`CollectorIdentity`、`LiveRuntimeSourceSpecs` 保留 root。
  它们是由 public config 投影并由 composition 注入的纯配置载体，没有 I/O、状态机
  或业务方法。仅因单一 consumer 而迁移会扩大字段 visibility，没有 AC 收益。
- 两个 Gamma endpoint 常量有真实 Polymarket adapter 消费，属于必要私有配置；
  只为 lexical locality 重开已闭合 2-PM owner 不构成必要修改。
- 私有 owner 内的 `use super::*` 保持机械 wiring。收口时批量显式化 28 处 import
  只会扩大重构面，不改变外部 API 或 owner。
- `estimated_event_bytes` 保留 logical persisted-event byte 语义；
  `owned_event_bytes` 保留 inflight allocation contract 语义。两者不是重复校验，
  不合并。
- writer 的 required/optional JSON trust-boundary checks 迁入
  `writer_supervisor`，没有删除或复制。只有被 Polymarket 测试消费的
  `optional_i64` 使用最小 `pub(super)`。
- `global_inflight_budget_enforces_exact_production_boundaries_and_releases` 迁入
  `inflight.rs::tests`；跨 raw-day/generic/PM gate 的 missing-gate 测试保留 root。
- `RuntimeExitRequest`、runtime-exit apply 与 source-wrapper lifecycle 类型迁入
  `raw_day`；`binance_ws_payload` 迁入 depth owner；RTDS dedupe error wrapper迁入
  RTDS owner。

## 5. 测试库存与确定性门

入口自动库存为 574 个 named tests、37 个 package test/doc targets
（36 个可执行 test targets + 1 个 doctest target）；入口完整限定名排序清单
SHA-256 为
`135168b32b594884fed2bd1ad437b44f5d77c5eaf26b06beb63f6e4df444f78d`。

终态仍为 574 个 named tests、37 个 package test/doc targets：

- 终态完整限定名排序清单 SHA-256：
  `5ac43b7efc6f9840fb2ca723f87685b1dd95b16d123b78c5f842c765b17fc71d`；
- 去除 owner module 前缀后的 basename 排序清单 SHA-256：
  `76c2711cb908bde6cbfa2a5b9b9cb54b3416f1135f29cec5e7143fdef2d9184a`；
- 入口 basename 排序清单 SHA-256 同为
  `76c2711cb908bde6cbfa2a5b9b9cb54b3416f1135f29cec5e7143fdef2d9184a`。

完整限定名 hash 的变化来自测试随 owner 改变 module path；basename、数量和行为
合同未丢失。`global_inflight_budget...` 的路径从
`live_runtime::tests`变为`inflight::tests`，符合真实 owner。

最终代码冻结后，主执行方只运行一次分支完整门：

```bash
cargo test -p data-collect --locked -- --test-threads=1
cargo check -p data-collect --locked --all-targets
cargo clippy -p data-collect --locked --all-targets -- -D warnings
cargo fmt --all -- --check
git diff --check
```

结果：

- lib 348 项通过；
- integration 226 项通过；
- 三个 bin target 与 doctest 均为 0 项、退出 0；
- check、Clippy `-D warnings`、fmt 与 diff gate 全部退出 0。

独立证据终审者随后在只读审查中独立复跑同一完整suite及check/Clippy/fmt/diff，
结果同样全部退出0。该复跑属于独立审查证据，不计作第二次主执行方分支验收门。

各 owner 小步另已运行直接及受影响跨模块测试；这些只用于迁移绿点诊断，不被冒充
第二次分支完整验收门。

## 6. 偏差与清理

测试库存早期误用了包含 harness-free benchmark 的
`cargo test ... --all-targets ... -- --list`。Cargo 因此实际执行
`market_discovery_probe`，尝试公开 Gamma 只读请求并生成未跟踪
`data-collect/market-discovery-artifacts/market_discovery_receipt.json`。

该 receipt 已被检查后删除，artifact 目录当前为空；没有 tracked diff、远端写入、
production activation、deployment 或数据迁移。该网络请求不计入任何验收证据。
此后库存命令固定为：

```bash
cargo test -p data-collect --lib --bins --tests --locked -- --list
```

## 7. 受保护与范围外状态

三份未跟踪 RTDS 草案始终未修改、未暂存、未删除，SHA-256 仍为：

- changelog：
  `d8caddb53c9fbe259ce2f844845426580f614de47ea8bd79c5f110576dc84148`；
- demand：
  `911aa69bd6d086887f88c185f1b7dc20f6223e7d8637fc1848752966886fc1a7`；
- plan：
  `50d4a405a664baec39844ac1952af7be10de6b2f75d792f8abb57f62d36d4c1`。

本分支没有运行 public-network smoke、production binary、AWS 远端命令、release、
deployment、canonical-root cutover、migration 或数据删除。RTDS 专项继续保持其
真实 deferred 状态，不由 2-REST 自动闭合。

## 8. AC 映射与审查状态

| AC | 2-REST 证据 | 状态 |
|---|---|---|
| AC-10 | 测试前root 1,815行，只含facade/config/composition及最小test-only glue；28个唯一owner；测试随owner；LOC与line classification；残留审计 | `LOCAL_GATES_PASSED` |
| AC-11 | Stage5 已独立闭合；2-REST semantic delete 为 0，未重开 cleanup | `CONSUMED_STAGE5_CLOSED` |
| AC-13 | manifest/day/writer/shutdown 直接及跨 owner 回归、完整 package suite | `PASS` |
| AC-14 | raw/PM/global inflight exact-limit、timeout、closed-channel、release 回归 | `PASS` |
| AC-15 | 无 production、远端、部署、migration、删除或 public smoke | `PASS` |

代码、证据与文档三路 `gpt-5.6-sol` / Ultra 独立终审均为
`PASS / CLOSURE_UNBLOCKED`：

- 代码终审：Critical 0 / Important 0 / Minor 0；production函数、类型/常量、
  非函数item、状态机与测试均确认等价搬迁；
- 证据终审：Critical 0 / Important 0 / Minor 0；LOC/churn、测试库存/hash、
  完整门、保护草案hash与空artifact目录均复核一致；
- 文档终审：Critical 0 / Important 0 / Minor 0；三件套、AC、历史阶段事实、
  范围边界与方案A条件式闭环方式一致。

剩余必做事项为0。本文按方案A条件式记录`2-REST_CLOSED`；只有顶部生效条件
满足后，2-REST与核心总事项闭合才实际成立。RTDS行为专项继续保持
`RTDS_PARAMETERS_DEFERRED / 未完成`。
