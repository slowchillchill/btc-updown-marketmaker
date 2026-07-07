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

2 小时验收必须再次检查 binary sha256、git commit、config digest、dataset root canonical path、collector host、collector region、binary role、source coverage、source gap、ResourceWatermark、running/finalized artifact 边界和 shutdown finalize 结果。
