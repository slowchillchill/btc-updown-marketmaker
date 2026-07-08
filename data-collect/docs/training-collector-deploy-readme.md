# data-collect training collector deploy README

对应需求：`demand/data-collect-demand.md`

本文件只描述训练数据采集服务器上的 binary-only 部署与验收。paper/live hot path 接入见 `data-collect/docs/paper-live-hot-path-integration.md`。paper/live 服务器不得运行 cargo；live 小主机不得承担训练重处理或全量 parquet 扫描。

## 本地构建与导出

在本地开发机执行：

```bash
cd /home/chillchill/btc-updown-marketmaker
CARGO_TARGET_DIR=target/data-collect-ci data-collect/scripts/export-release-artifacts.sh
COMMIT="$(git rev-parse --short=12 HEAD)"
```

release 包必须包含三个二进制、部署文档、systemd 示例、`RELEASE_INFO.env` 和 `SHA256SUMS`：

```text
bin/data-collect-training-collector
bin/data-collect-paper-hot
bin/data-collect-live-hot
deploy/systemd/data-collect-training-collector.service.example
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
rsync -av "target/data-collect-release/$COMMIT/" \
  ubuntu@3.255.156.183:~/btc-updown-marketmaker/data-collect/releases/"$COMMIT"/
ssh ubuntu@3.255.156.183 \
  'cd ~/btc-updown-marketmaker/data-collect/releases/'"$COMMIT"' && sha256sum -c SHA256SUMS'
ssh ubuntu@3.255.156.183 \
  'ln -sfn ~/btc-updown-marketmaker/data-collect/releases/'"$COMMIT"' ~/btc-updown-marketmaker/data-collect/current'
```

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
    --max-runtime-secs 900
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
  cp ~/btc-updown-marketmaker/data-collect/current/deploy/systemd/data-collect-training-collector.service.example \
    ~/.config/systemd/user/data-collect-training-collector.service
  systemctl --user daemon-reload
  systemctl --user start data-collect-training-collector.service
  systemctl --user is-active --quiet data-collect-training-collector.service
  sleep 7200
  systemctl --user stop data-collect-training-collector.service
  systemctl --user show data-collect-training-collector.service \
    --property=ActiveState,SubState,Result,ExecMainStatus --no-pager
  if systemctl --user is-active --quiet data-collect-training-collector.service; then
    echo "service still active after stop"
    exit 1
  fi
  journalctl --user -u data-collect-training-collector.service \
    --since "-130 minutes" --no-pager | tail -n 200
'
```

不得 enable --now。是否执行 `loginctl enable-linger ubuntu` 或 `systemctl --user enable --now data-collect-training-collector.service` 必须单独获得用户批准。

2 小时验收必须再次检查 binary sha256、git commit、config digest、dataset root canonical path、collector host、collector region、binary role、run manifest 内的 source coverage/source gap/rollover evidence、`source_writer_stats` event/typed row 口径、`queue_summary`、`shutdown_stage_timings`、`shutdown_retention_scan_budget_ms`、`run_manifest_atomic_write`、ResourceWatermark、running/finalized artifact 边界和 shutdown finalize 结果。shutdown stop 路径不再启动新的全盘 retention 扫描；`retention_shutdown_scan.status=skipped` 表示依赖 startup/periodic run-level `retention_policy_receipt.json`，不能判为漏验。
示例 service 已显式传入 `--collector-region eu-west-1`；如果未来迁移出 eu-west-1，必须先更新 service、README 和验收记录，不能让生产验收依赖隐式 region。
