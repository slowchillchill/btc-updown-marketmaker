# SDK070 typed schema / live runtime 阶段0执行基线

> 事项编号：`DC-REPAIR-20260724-SDK070-TYPED-LIVE-RUNTIME`
>
> 对应需求：`demand/data-collect-sdk070-typed-schema-live-runtime-refactor-demand.md`
>
> 对应计划：`plan/data-collect-sdk070-typed-schema-live-runtime-refactor-plan.md`
>
> 对应 changelog：`changelog/data-collect-sdk070-typed-schema-live-runtime-refactor-changelog.md`
>
> 快照日期：`2026-07-25`
>
> 状态：`STAGE_0_CLOSED`。本状态以承载本记录的commit成功推送到`origin/fix-rtds-silents`且远端分支SHA与本地一致为生效条件。

## 1. 执行边界与仓库身份

用户已批准按总需求与计划开始执行。当前执行起点为已推送的pre-execution
checkpoint：

```text
branch: fix-rtds-silents
HEAD: b65a2add5749d0931e91ef36d3bbadcb15a53014
upstream: origin/fix-rtds-silents
ahead/behind: 0/0
```

阶段0只读盘点与current-v2 probe开始前，以及probe结束但本事项尚未写入执行文档
时，原有worktree变化均只有以下三份既存未跟踪RTDS草案。它们不属于本次改动；
阶段0只读取其状态字段以消费专项游标，没有修改、暂存或删除：

```text
changelog/data-collect-rtds-business-silence-full-reconnect-changelog.md
demand/data-collect-rtds-business-silence-full-reconnect-demand.md
plan/data-collect-rtds-business-silence-full-reconnect-plan.md
```

当前阶段0文档diff精确限定为：

```text
demand/data-collect-reliability-repair-demand.md
plan/data-collect-reliability-repair-plan.md
changelog/data-collect-reliability-repair-changelog.md
demand/data-collect-sdk070-typed-schema-live-runtime-refactor-demand.md
plan/data-collect-sdk070-typed-schema-live-runtime-refactor-plan.md
changelog/data-collect-sdk070-typed-schema-live-runtime-refactor-changelog.md
data-collect/docs/sdk070-stage0-baseline.md
```

本次总批准不消解以下独立门：

| 门 | 当前状态 |
|---|---|
| SDK typed字段gap逐项决定 | `PENDING`，只阻断v3 |
| RTDS三项参数 | `PENDING` |
| RTDS 4A / 4B | `NOT_AUTHORIZED` |
| public CLOB / RTDS / Gamma smoke | `SEPARATE_AUTHORIZATION_REQUIRED` |
| 阶段commit / push | `SEPARATE_AUTHORIZATION_REQUIRED` |
| production activation / cutover / release / deployment | `NOT_AUTHORIZED` |
| 远端服务、历史migration、删除 | `NOT_AUTHORIZED` |

## 2. RTDS专项消费结果

umbrella只消费RTDS专项已有状态，没有重算参数、重建测试矩阵或维护第二游标：

| 项目 | 当前状态 |
|---|---|
| `steady_silence_threshold` | `PENDING` |
| `first_fresh_observation_window` duration | `PENDING` |
| `first_fresh_observation_window` anchor | `PENDING` |
| cold-start watchdog enable/disable | `PENDING` |
| umbrella结果 | `RTDS_PARAMETERS_DEFERRED` |
| wrapper路线 | `WRAPPER_FIRST_PROVISIONAL_REQUIRES_STAGE_1_CAPABILITY_GATE` |

因此4A authority同步和4B实现均未启动。

## 3. Toolchain、Cargo与SDK基线

### 3.1 Toolchain

```text
rustc 1.95.0 (59807616e 2026-04-14)
cargo 1.95.0
+1.88: installed, includes clippy/rustfmt/llvm-tools
+1.88.0: same compiler, but clippy/rustfmt are not installed
nightly-2025-11-24: rustfmt available
repository rust-toolchain override: none
```

不安装或恢复任何组件；后续vendor MSRV门如获确认，使用已有`+1.88`，不得把
缺组件的`+1.88.0`别名失败冒充SDK失败。

### 3.2 当前0.6.0身份

- root `exclude`与`[patch.crates-io]`均指向
  `third_party/polymarket_client_sdk_v2-0.6.0-p1`。
- `data-collect`精确依赖`=0.6.0`，features为`clob,gamma,rtds,ws`。
- locked feature tree只解析到该本地0.6.0 vendor。
- 当前vendor相对官方0.6.0恰好修改四个源码路径：
  `src/ws/connection.rs`、`src/ws/config.rs`、
  `src/clob/ws/types/response.rs`、`src/rtds/subscription.rs`。
- `third_party/polymarket_client_sdk_v2-0.7.0-p1`尚不存在。

关键SHA-256：

| 对象 | SHA-256 |
|---|---|
| root `Cargo.toml` | `88d3fc7530f209b724dc11213fac75445f9ea59aebd6bb405639baaed0af6cc9` |
| `data-collect/Cargo.toml` | `5330bc1d819eb6ee94050b107c21f59f6e1ef61c36c8795904691756f49e0d61` |
| root `Cargo.lock` | `7e4b4d0a4a4333c12640139570d5883f014442e412ed2708d7e184b8986267ce` |
| vendor `Cargo.toml` | `792f7c76c15f9ac8fc35e71672bf5ffaf0432fb8a493e068a3c220d514fe78e6` |
| vendor `Cargo.lock` | `f905a083c028e5e54f1cf799cd4ceab373cfd192a312950940ad52094179a3ae` |
| vendor Git tree | `f41f9e405c7a7f3069c363bda2e1afd9a945b33b` |

### 3.3 官方0.7.0 provenance

| 项目 | 值 |
|---|---|
| crate SHA-256 | `ba212e0641f178c274af266772de15962ac7e76da550a0f79f47b49349b1138a` |
| VCS commit | `222143d321eba97d5711a848265eb9aab3bc7ff4` |
| published | `2026-07-17T14:56:51Z` |
| MSRV | `1.88.0` |
| license | `MIT` |
| yanked | `false` |
| archive paths | `132` |
| sorted file-list SHA-256 | `55c093724c71c384cb485cc44bef1cb75d21123035cb8f253308b3d518e0dc74` |
| license SHA-256 | `2ef5f959b834b22b3e2798a5c0f20a4742a9c1bdb85a7171c72603c58a0a0137` |

官方0.6.0与0.7.0中四个当前patch底文件逐字节相同。该事实只证明patch可逐
hunk重放，不证明0.7.0 runtime或typed合同已经通过。

可复核命令：

```bash
cargo_cache_crate=/home/chillchill/.cargo/registry/cache/index.crates.io-1949cf8c6b5b557f/polymarket_client_sdk_v2-0.7.0.crate
sha256sum "$cargo_cache_crate"
tar -tf "$cargo_cache_crate" | LC_ALL=C sort | sha256sum
tar -xOf "$cargo_cache_crate" \
  polymarket_client_sdk_v2-0.7.0/LICENSE | sha256sum

cargo tree -p data-collect --locked -e features \
  -i polymarket_client_sdk_v2
```

## 4. Runtime与代码规模基线

### 4.1 `live_runtime.rs`

| 项目 | 当前值 |
|---|---:|
| 文件行数 | `50,382` |
| 文件bytes | `1,989,547` |
| 主tests模块起点 | `23,398` |
| 主tests模块行数 | `26,985` |
| `#[test]` | `83` |
| `#[tokio::test]` | `190` |
| 测试合计 | `273` |
| 顶层functions | `335` |
| structs / enums | `86 / 55` |
| type aliases / traits / impls | `3 / 0 / 51` |
| consts / statics | `62 / 2` |

无ignored test或重复名称。测试名称源序SHA-256为
`f2b63bc88ac9e4ade50c6219f8d238cca5b4c63cfdc6866d8637ca9b206425f9`，
排序集合SHA-256为
`2b32d67063e327043242fbc7ddd00bf90b3ba129aae97a553e9448a220627f6a`。
`273`只作为before/after名称集合快照，不是固定验收门。

### 4.2 First-party Rust physical LOC

复核命令：

```bash
rg --files data-collect/src data-collect/tests data-collect/benches \
  -g '*.rs' | sort | xargs wc -l
```

结果为82文件、91,940行：`src` 36文件/75,318行，`tests`
40文件/13,700行，`benches` 6文件/2,922行。该口径包含空行和注释，排除
vendor、target、文档和非Rust文件；`live_runtime.rs`占54.80%。

### 4.3 生产入口与artifact

training生产调用链：

```text
data_collect_training_collector
  -> runtime::run_training_collector
  -> live_runtime::run_live_training_runtime
  -> run_live_training_runtime_with_sources_and_stop
```

paper/live hot collector走`runtime.rs`的heartbeat handoff，不进入training
live runtime。

| logical producer | artifacts |
|---|---|
| `polymarket` | `polymarket_market`, `polymarket_market_metadata` |
| `polymarket_rtds` | `polymarket_rtds_chainlink_btc_usd` |
| `binance_depth` | `binance_depth_snapshot`, `binance_depth_update` |
| `binance_aggtrade` | `binance_aggtrade` |

该入口、四producer、六artifact与总需求基线一致。

## 5. 数据、storage与性能报告基线

输入文件：

```text
/home/chillchill/new-datasets/2026-07-22/typed_raw/utc_day=2026-07-22/source_kind=polymarket_market/polymarket_market_01.parquet
```

使用PyArrow `23.0.1`按Parquet metadata逐row group汇总compressed
column-chunk size：

| 项目 | 值 |
|---|---:|
| rows | `4,047,003` |
| row groups | `624` |
| columns | `28` |
| file bytes | `269,203,640` |
| `source_native_payload_json` compressed | `140,671,726` |
| `hash` compressed | `92,754,899` |
| 两列合计 | `233,426,625` / `86.7101%` |

同一UTC日24个Polymarket文件合计：

| 项目 | 值 |
|---|---:|
| rows | `93,481,048` |
| file bytes | `6,265,404,486` |
| payload compressed | `3,281,353,139` |
| hash compressed | `2,145,938,625` |
| 两列合计 | `5,427,291,764` / `86.6232%` |

上述单文件与完整日聚合的只读复现命令如下；输出artifact就是本文件：

```bash
python3 - <<'PY'
from pathlib import Path
import pyarrow
import pyarrow.parquet as pq

day = Path(
    "/home/chillchill/new-datasets/2026-07-22/typed_raw/"
    "utc_day=2026-07-22"
)
source = day / "source_kind=polymarket_market"
single = source / "polymarket_market_01.parquet"
columns = {"source_native_payload_json", "hash"}

def summarize(paths):
    result = {
        "files": 0,
        "rows": 0,
        "file_bytes": 0,
        **{name: 0 for name in columns},
    }
    for path in paths:
        parquet = pq.ParquetFile(path)
        result["files"] += 1
        result["rows"] += parquet.metadata.num_rows
        result["file_bytes"] += path.stat().st_size
        for row_group_index in range(parquet.metadata.num_row_groups):
            row_group = parquet.metadata.row_group(row_group_index)
            for column_index in range(row_group.num_columns):
                column = row_group.column(column_index)
                if column.path_in_schema in columns:
                    result[column.path_in_schema] += column.total_compressed_size
    return result

single_file = pq.ParquetFile(single)
print("pyarrow", pyarrow.__version__)
print("single", summarize([single]))
print(
    "single_shape",
    single_file.metadata.num_row_groups,
    single_file.metadata.num_columns,
)
print(
    "day",
    summarize(sorted(source.glob("polymarket_market_*.parquet"))),
)
PY
```

对应manifest：

```text
/home/chillchill/new-datasets/2026-07-22/typed_raw/utc_day=2026-07-22/manifest.json
```

其状态为`closed`，schema为`data_collect_daily_manifest_v3`，build commit为
`1d95823614ceed78288ce34cb95fbf23e58ef64d`，Polymarket rows为
93,481,048，24文件，`structural_dedupe_dropped_events=14,664`，
`reconnect_events=1`，writer ack存在。

manifest事实的只读复现命令：

```bash
jq '{
  schema_version,
  state,
  build_commit,
  closure_reason,
  polymarket_market: .source_counters.polymarket_market,
  polymarket_writer_ack:
    (.writer_finalize_acks | index("polymarket_market"))
}' \
  /home/chillchill/new-datasets/2026-07-22/typed_raw/utc_day=2026-07-22/manifest.json
```

当前缓存状态下，以65,536 rows/batch、all-column、threaded streaming读取用户
文件得到62 batches、4,047,003 rows、0.952598秒、4,248,382.92 rows/s，
max RSS 1,169,268 KiB。该结果不是受控cold-cache测量，只是报告项。

同一scan口径的只读复现命令如下；elapsed和RSS依赖复跑时的cache/host状态，只作
报告，不与上述快照做固定相等断言：

```bash
/usr/bin/time -v python3 - <<'PY'
from time import perf_counter
import pyarrow
import pyarrow.parquet as pq

path = (
    "/home/chillchill/new-datasets/2026-07-22/typed_raw/"
    "utc_day=2026-07-22/source_kind=polymarket_market/"
    "polymarket_market_01.parquet"
)
rows = 0
batches = 0
started = perf_counter()
for batch in pq.ParquetFile(path).iter_batches(
    batch_size=65_536,
    use_threads=True,
):
    rows += batch.num_rows
    batches += 1
elapsed = perf_counter() - started
print("pyarrow", pyarrow.__version__)
print("rows", rows, "batches", batches, "elapsed_s", elapsed)
print("rows_per_s", rows / elapsed)
PY
```

现有v2 writer基线命令：

```bash
cargo bench -p data-collect --locked \
  --bench source_hourly_layout_probe
```

20,000 rows结果：

| layout | elapsed | finalize | bytes |
|---|---:|---:|---:|
| receive / zstd3 | `25ms` | `10ms` | `220,343` |
| sorted / zstd3 | `19ms` | `7ms` | `237,053` |
| receive / zstd9 | `27ms` | `13ms` | `214,079` |
| sorted / zstd9 | `27ms` | `13ms` | `237,428` |

该probe只写并清理本地临时路径；运行后tracked diff仍为空。这些数值是current-v2
报告基线，不是v3必须达到的固定比例或耗时门。

## 6. 已裁决的不一致停止门

### D0-01：vendor patch三路径与四路径

- 修复前，`demand/data-collect-reliability-repair-demand.md`第3.1节第3项声明
  source-availability需求第4.7节为最终authority且不再以固定数量为合同，但同一句
  末尾只枚举三个路径，漏掉`src/ws/config.rs`。
- source-availability需求第4.7节、当前vendor事实和本需求AC-01均为四路径。
- 用户决定：批准修正旧句为四路径并包含`src/ws/config.rs`；不改变当前patch语义。

### D0-02：完整测试命令不等价

- source-availability需求AC-27要求
  `cargo test -p data-collect --locked -- --test-threads=1`。
- 修复前，本计划阶段1与阶段6使用
  `cargo test -p data-collect --locked --tests`。
- 后者默认并行且不运行doctest，不能替代前者。
- 用户决定：阶段1与阶段6各自收口时只运行一次既有串行完整命令，不再额外运行
  `--tests`，避免重复。

### D0-03：Gamma真实owner与2-PM / 2-REST冲突

- 修复前，文档要求`2-PM`独立完成Polymarket source、discovery、adapter和resolution，同时把
  Gamma放入`2-REST`。
- 真实`run_polymarket_source_with_spec`创建并持有唯一`GammaClient`，直接用于
  initial/current/next discovery和resolution polling；相关helpers也是该actor的直接
  依赖。
- 用户决定：直接参与Polymarket discovery、identity和resolution的Gamma transport/helpers
  归`2-PM`，从`2-REST`删除笼统的Gamma项；`2-PM`继续不依赖`2-REST`。

### D0-04：vendor身份变化后的验证矩阵

- 既有source-availability计划规定vendor SDK身份任一变化都重跑受影响矩阵；其已
  批准矩阵包含`+1.88` core/all-features tests、default/all-features all-targets
  Clippy和固定nightly vendor fmt。
- 0.7.0升级必然改变vendor身份；修复前，本计划阶段1只显式列出两组vendor tests及
  workspace Clippy/fmt。
- workspace命令不覆盖被root排除的vendor，因此两者不是重复验证。
- 用户决定：阶段1对新vendor只运行一次上述适用矩阵；不在每个小步骤重复，也不增加
  网络、bench或无关feature组合。

### D0-05：旧vendor终态遗漏

- 独立审查复核确认，修复前计划只写新建`0.7.0-p1`和切换workspace，没有规定
  tracked旧`0.6.0-p1`源码树的终态；按字面执行会在`third_party`保留两个完整SDK
  目录。
- 总需求定义的是从0.6升级到0.7，且禁止shadow路径；当前Git checkpoint已经提供
  回滚能力，不需要把旧源码树留作第二fallback。
- 修正：阶段1只有在新vendor provenance、四路径patch和workspace切换均已建立后，
  才在同一diff删除旧vendor；终态只有一个tracked SDK vendor目录，active
  Cargo/code/probe中的Polymarket SDK旧路径和其0.6精确版本引用为0，历史文档记录
  保留。

用户已于2026-07-25批准四项推荐，demand/plan/changelog已完成最小对齐；全部审查
修复经同一`gpt-5.6-sol` / `ultra`审查者最终复核为
`Critical 0 / Important 0 / Minor 0 / PASS`。用户已另行批准精确阶段0文档集
commit/push；以下闭合状态以承载本记录的commit成功推送且远端分支SHA与本地
一致为生效条件：

```text
Stage 0: CLOSED
Stage 1: NOT_STARTED
Stage 2: NOT_STARTED
Rust/Cargo/vendor/schema/data diff: NONE
network probe: NOT_RUN
```
