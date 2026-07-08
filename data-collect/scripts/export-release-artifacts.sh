#!/usr/bin/env bash
set -euo pipefail

cd /home/chillchill/btc-updown-marketmaker

COMMIT="$(git rev-parse --short=12 HEAD)"
FULL_COMMIT="$(git rev-parse HEAD)"
BUILD_GIT_COMMIT="$FULL_COMMIT"
BUILD_GIT_COMMIT_SHORT="$COMMIT"
if [[ -n "$(git status --porcelain -- Cargo.toml Cargo.lock data-collect demand plan changelog AGENTS.md)" ]]; then
  BUILD_GIT_COMMIT="${FULL_COMMIT}-dirty"
  BUILD_GIT_COMMIT_SHORT="${COMMIT}-dirty"
fi
TARGET_DIR="${CARGO_TARGET_DIR:-target/data-collect-ci}"
EXPORT_ROOT="target/data-collect-release/$COMMIT"

required_files=(
  "data-collect/deploy/systemd/data-collect-training-collector.service.example"
  "data-collect/docs/training-collector-deploy-readme.md"
  "data-collect/docs/paper-live-hot-path-integration.md"
)

for path in "${required_files[@]}"; do
  if [[ ! -f "$path" ]]; then
    echo "missing required release file: $path" >&2
    exit 1
  fi
done

rm -rf "$EXPORT_ROOT"
mkdir -p "$EXPORT_ROOT/bin" "$EXPORT_ROOT/deploy/systemd" "$EXPORT_ROOT/docs"

DATA_COLLECT_GIT_COMMIT="$BUILD_GIT_COMMIT" \
CARGO_TARGET_DIR="$TARGET_DIR" cargo build -p data-collect --release \
  --bin data-collect-training-collector \
  --bin data-collect-retention-admin \
  --bin data-collect-paper-hot \
  --bin data-collect-live-hot

install -Dm755 "$TARGET_DIR/release/data-collect-training-collector" "$EXPORT_ROOT/bin/data-collect-training-collector"
install -Dm755 "$TARGET_DIR/release/data-collect-retention-admin" "$EXPORT_ROOT/bin/data-collect-retention-admin"
install -Dm755 "$TARGET_DIR/release/data-collect-paper-hot" "$EXPORT_ROOT/bin/data-collect-paper-hot"
install -Dm755 "$TARGET_DIR/release/data-collect-live-hot" "$EXPORT_ROOT/bin/data-collect-live-hot"
install -Dm644 data-collect/deploy/systemd/data-collect-training-collector.service.example \
  "$EXPORT_ROOT/deploy/systemd/data-collect-training-collector.service.example"
install -Dm644 data-collect/docs/training-collector-deploy-readme.md \
  "$EXPORT_ROOT/docs/training-collector-deploy-readme.md"
install -Dm644 data-collect/docs/paper-live-hot-path-integration.md \
  "$EXPORT_ROOT/docs/paper-live-hot-path-integration.md"

{
  printf "git_commit=%s\n" "$BUILD_GIT_COMMIT"
  printf "git_commit_short=%s\n" "$BUILD_GIT_COMMIT_SHORT"
  NOW_NS="$(date -u +%s%N)"
  if [[ "$NOW_NS" =~ ^[0-9]{19}$ ]]; then
    printf "built_at_utc_ms=%s\n" "${NOW_NS:0:13}"
  else
    printf "built_at_utc_ms=%s000\n" "$(date -u +%s)"
  fi
} > "$EXPORT_ROOT/RELEASE_INFO.env"

(cd "$EXPORT_ROOT" && sha256sum \
  bin/data-collect-training-collector \
  bin/data-collect-retention-admin \
  bin/data-collect-paper-hot \
  bin/data-collect-live-hot \
  deploy/systemd/data-collect-training-collector.service.example \
  docs/training-collector-deploy-readme.md \
  docs/paper-live-hot-path-integration.md \
  RELEASE_INFO.env > SHA256SUMS)

printf "data_collect_release=%s\n" "$EXPORT_ROOT"
