# data-collect training collector deploy README

对应需求：`demand/data-collect-demand.md`

本文件只描述训练数据采集服务器上的 binary-only 部署与验收。paper/live hot path 接入见 `data-collect/docs/paper-live-hot-path-integration.md`。paper/live 服务器不得运行 cargo；live 小主机不得承担训练重处理或全量 parquet 扫描。

## 本地构建与导出

在本地开发机执行：

```bash
cd /home/chillchill/btc-updown-marketmaker
DATA_COLLECT_RELEASE_ID=train-data-collect-ver1-0 \
  CARGO_TARGET_DIR=target/data-collect-ci \
  data-collect/scripts/export-release-artifacts.sh
RELEASE_ID=train-data-collect-ver1-0
```

release 包必须包含正式训练采集别名、原始训练采集二进制、retention admin、paper/live hot path 二进制、部署文档、systemd 示例、`RELEASE_INFO.env` 和 `SHA256SUMS`：

```text
bin/data-collect-training-collector
bin/train-data-collect-ver1-0
bin/data-collect-retention-admin
bin/data-collect-paper-hot
bin/data-collect-live-hot
deploy/systemd/data-collect-training-collector.service.example
deploy/systemd/data-collect-training-collector-gate.service.example
docs/training-collector-deploy-readme.md
docs/paper-live-hot-path-integration.md
RELEASE_INFO.env
SHA256SUMS
```

`SHA256SUMS` 是 binary sha256 和文档/service 示例完整性检查的唯一入口；启动 receipt 还必须记录 git commit、config digest、dataset root canonical path、collector host、collector region 和 binary role。

## 上传到数据采集服务器

```bash
ssh ubuntu@3.255.156.183 \
  'mkdir -p ~/btc-updown-marketmaker/data-collect/releases'
rsync -av "data-collect/releases/$RELEASE_ID/" \
  ubuntu@3.255.156.183:~/btc-updown-marketmaker/data-collect/releases/"$RELEASE_ID"/
ssh ubuntu@3.255.156.183 \
  'cd ~/btc-updown-marketmaker/data-collect/releases/'"$RELEASE_ID"' && sha256sum -c SHA256SUMS'
ssh ubuntu@3.255.156.183 \
  'ln -sfn ~/btc-updown-marketmaker/data-collect/releases/'"$RELEASE_ID"' ~/btc-updown-marketmaker/data-collect/current'
```

切换 `current` 前必须记录原 symlink：

```bash
ssh ubuntu@3.255.156.183 \
  'readlink -f ~/btc-updown-marketmaker/data-collect/current || true'
```

## 回滚方法

只允许回滚到仍保留完整 `SHA256SUMS` 且 `sha256sum -c SHA256SUMS` 通过的历史 release。回滚时先停止 service，再把 `current` symlink 切回上一 release，校验 sha256，最后按需重启：

```bash
ssh ubuntu@3.255.156.183 '
  set -euo pipefail
  PREVIOUS_RELEASE="$HOME/btc-updown-marketmaker/data-collect/releases/<previous-release-id>"
  systemctl --user stop data-collect-training-collector.service || true
  cd "$PREVIOUS_RELEASE"
  sha256sum -c SHA256SUMS
  ln -sfn "$PREVIOUS_RELEASE" "$HOME/btc-updown-marketmaker/data-collect/current"
  systemctl --user daemon-reload
  systemctl --user start data-collect-training-collector.service
'
```

如果回滚原因是 resource hard-threshold、retention fail-closed 或磁盘不可恢复，不得直接重启；必须先保留 failed run manifest、retention receipt、journal 和 cleanup inventory，再决定是否恢复旧 release。

训练数据根为：

```text
~/btc-updown-marketmaker/datasets
```

训练 cold path 使用 day-first hourly logical partition：

```text
~/btc-updown-marketmaker/datasets/training/typed_raw/utc_day=YYYY-MM-DD/source_kind=<source_kind>/<source_kind>_YYYY-MM-DDTHH.parquet
~/btc-updown-marketmaker/datasets/training/typed_raw/utc_day=YYYY-MM-DD/source_kind=<source_kind>/<source_kind>_YYYY-MM-DDTHH--part=N.parquet
~/btc-updown-marketmaker/datasets/training/run_manifests/<run_id>.manifest.json
~/btc-updown-marketmaker/datasets/training/runtime_artifacts/<run_id>/retention_policy_receipt.json
```

默认生产路径不再检查 source_coverage.json、不再检查 source_gaps.json、不再检查 rollover_edges.json；这些证据必须从 run manifest 内的 `source_coverage`、`source_gaps`、`rollover_edges` 读取。运行目录不得生成 write_receipts.jsonl、不得生成 queue_samples.jsonl、不得生成 retention_after_finalize_ 开头的 source-specific receipt；writer 证据看 run manifest 内的 `source_writer_stats`，queue 证据看 `queue_summary`。

`part=N` 只表示同一小时内 `max_rows` / `max_bytes` 保护性 soft rollover；不能把它解释为停机或缺数据。人工检查 Polymarket `book` 时，必须读取 manifest 中 `book_rows`、`book_rows_with_bids_asks`，并抽查 parquet/raw evidence 中 `bids` 和 `asks` 是完整 price-size 数组；只看到 `bids_len` / `asks_len` 不能通过。

retention 阈值是 `~/btc-updown-marketmaker/datasets/training` 的滚动容量上限，默认 `50_000_000_000` bytes。默认模式只写 dry-run/no-op policy receipt；真实删除必须显式启用 delete mode 或另获用户批准，且只允许删除 `training/typed_raw/utc_day=*` 下最老 finalized complete UTC day。current day、running/digesting/tmp/`.rsync-partial`、incomplete day、`manual-inspection`、`run_manifests`、startup receipts 不得删除。

正式长期采集 service 必须显式传入 `--run-forever --retention-delete-mode enabled --retention-threshold-bytes 50000000000`，并在 run manifest 中看到 `runtime_mode=run_forever`、`run_started_utc_ms`、`run_forever=true`、`retention_delete_mode=enabled`、`retention_threshold_bytes=50000000000`、`code_version`、`collector_binary_sha256`、`source_config_digest`、`systemd_unit_name=data-collect-training-collector.service` 和 `systemd_restart_policy`。Resource hard-threshold fail-closed 使用退出码 `75`，service 必须配置 `RestartPreventExitStatus=75`，避免磁盘/资源不可恢复时重启循环。

## 15 分钟 foreground smoke

```bash
ssh ubuntu@3.255.156.183 '
  set -euo pipefail
  BIN="$HOME/btc-updown-marketmaker/data-collect/current/bin/data-collect-training-collector"
  DATASET_ROOT="$HOME/btc-updown-marketmaker/datasets"
  cd "$HOME/btc-updown-marketmaker/data-collect/current"
  sha256sum -c SHA256SUMS
  DATA_COLLECT_RUNTIME_MODE=live "$BIN" \
    --dataset-root "$DATASET_ROOT" \
    --run-id "training-smoke-$(date -u +%Y%m%dT%H%M%SZ)" \
    --collector-region eu-west-1 \
    --max-runtime-secs 900 \
    --retention-delete-mode dry-run
'
```

smoke 后必须检查：

```text
binary sha256
git commit
config digest
dataset root canonical path
collector host
collector region
binary role
```

## 完整 5 分钟 market 人工审查运行

该模式只用于验收 `manual_review_complete_market` artifact，不是生产 hourly cadence。运行完成后必须看到 graceful shutdown，当前 hourly part 的 manifest `rollover_reason` 为 `manual_review_complete_market_shutdown`，run manifest 记录 `manual_review_complete_market`、目标 market 数量、选择的 market 信息字段、订阅 asset ids、planned/preload window 和 stop reason。

```bash
ssh ubuntu@3.255.156.183 '
  set -euo pipefail
  BIN="$HOME/btc-updown-marketmaker/data-collect/current/bin/data-collect-training-collector"
  DATASET_ROOT="$HOME/btc-updown-marketmaker/datasets"
  cd "$HOME/btc-updown-marketmaker/data-collect/current"
  sha256sum -c SHA256SUMS
  DATA_COLLECT_RUNTIME_MODE=live "$BIN" \
    --dataset-root "$DATASET_ROOT" \
    --run-id "manual-review-$(date -u +%Y%m%dT%H%M%SZ)" \
    --collector-region eu-west-1 \
    --stop-after-complete-polymarket-market 1
'
```

## 2 小时 systemd user service 验收

```bash
ssh ubuntu@3.255.156.183 '
	  set -euo pipefail
	  mkdir -p ~/.config/systemd/user
	  cp ~/btc-updown-marketmaker/data-collect/current/deploy/systemd/data-collect-training-collector-gate.service.example \
	    ~/.config/systemd/user/data-collect-training-collector-gate.service
	  systemctl --user daemon-reload
	  systemctl --user start data-collect-training-collector-gate.service
	  systemctl --user is-active --quiet data-collect-training-collector-gate.service
	  sleep 7300
	  systemctl --user show data-collect-training-collector-gate.service \
	    --property=ActiveState,SubState,Result,ExecMainStatus --no-pager
	  if systemctl --user is-active --quiet data-collect-training-collector-gate.service; then
	    echo "gate service still active after bounded 2h run"
	    exit 1
	  fi
	  journalctl --user -u data-collect-training-collector-gate.service \
	    --since "-130 minutes" --no-pager | tail -n 200
	'
```

不得 enable --now。是否执行 `loginctl enable-linger ubuntu` 或 `systemctl --user enable --now data-collect-training-collector.service` 必须单独获得用户批准。

2 小时验收必须再次检查 binary sha256、git commit、config digest、dataset root canonical path、collector host、collector region、binary role、run manifest 内的 source coverage/source gap/rollover evidence、`source_writer_stats` event/typed row 口径、`queue_summary`、`shutdown_stage_timings`、`shutdown_retention_scan_budget_ms`、`run_manifest_atomic_write`、ResourceWatermark、running/finalized artifact 边界、`run_forever=false`、`duration_secs=7200`、`retention_delete_mode=dry_run`、metadata source coverage 和 shutdown finalize 结果。shutdown stop 路径不再启动新的全盘 retention 扫描；`retention_shutdown_scan.status=skipped` 表示依赖 startup/periodic run-level `retention_policy_receipt.json`，不能判为漏验。
示例 service 已显式传入 `--collector-region eu-west-1`；如果未来迁移出 eu-west-1，必须先更新 service、README 和验收记录，不能让生产验收依赖隐式 region。

## 清理旧测试产物

正式长跑启动前必须生成并保存 `cleanup_allowlist.json`。只允许删除 allowlist 中逐项列出的路径，每项必须包含 path、type、bytes、digest 或目录清单 digest、reason、run_id_or_release_id、local_evidence_copy。默认保护：

- `~/btc-updown-marketmaker/data-collect/releases/train-data-collect-ver1-0`
- `~/btc-updown-marketmaker/data-collect/current`
- `~/btc-updown-marketmaker/datasets/training/run_manifests`
- current UTC day 下的 `training/typed_raw/utc_day=*`
- `manual-inspection`、phase8 evidence、历史 PASS/selected-window/retention validation evidence

允许删除的范围只限明确历史测试产物：`data-collect/phase0-runs/*`、`datasets/phase0-validation/*`、已知 validation run 的 `datasets-retention-validation/<run-id>`、未被 current 指向且非正式版本的旧 release、已有本地证据副本的旧 `training/runtime_artifacts/<test-run-id>`、满足阶段 8 计划条件的旧测试 `training/typed_raw/utc_day=<YYYY-MM-DD>`。删除前后都要记录 `du -sh`，清理证据拉回本地 `target/manual-inspection/train-data-collect-ver1-0-deploy-<ts>/cleanup/`。

## 启动长期采集

清理 gate 通过并获得用户批准后，安装正式 service：

```bash
ssh ubuntu@3.255.156.183 '
  set -euo pipefail
  mkdir -p ~/.config/systemd/user
  cp ~/btc-updown-marketmaker/data-collect/current/deploy/systemd/data-collect-training-collector.service.example \
    ~/.config/systemd/user/data-collect-training-collector.service
  systemctl --user daemon-reload
  systemctl --user enable --now data-collect-training-collector.service
  systemctl --user show data-collect-training-collector.service \
    --property=ActiveState,SubState,Result,ExecMainStatus,NRestarts --no-pager
'
```

启动后必须观察 service 为 active，最新 run manifest 写出上述 production provenance，`source_gaps=[]`，retention receipt 为 enabled 模式，且没有 `.running` / `.digesting` 长时间残留。
