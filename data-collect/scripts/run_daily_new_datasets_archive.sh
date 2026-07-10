#!/usr/bin/env bash
# Daily archive pipeline for data-collect typed_raw output.
#
# Default schedule:
#   - wake/preflight: 23:45 UTC, via systemd/RTC configuration outside this script
#   - archive run:   02:00 UTC, defaulting to yesterday's UTC day
#
# This script does not backfill Binance aggTrades. data-collect collects
# binance_aggtrade from the live websocket and this archive task only pulls
# finalized typed_raw parquet/manifest data from the data collection server.
#
# Usage:
#   data-collect/scripts/run_daily_new_datasets_archive.sh
#   data-collect/scripts/run_daily_new_datasets_archive.sh 2026-07-08
#   data-collect/scripts/run_daily_new_datasets_archive.sh 20260708
set -euo pipefail

DATE_INPUT="${1:-$(date -u -d 'yesterday' +%F)}"
LOCAL_ROOT="${DATA_COLLECT_NEW_DATASETS_ROOT:-/home/chillchill/new-datasets}"
REMOTE="${DATA_COLLECT_ARCHIVE_REMOTE:-ubuntu@3.255.156.183}"
REMOTE_DATASET_ROOT="${DATA_COLLECT_ARCHIVE_REMOTE_DATASET_ROOT:-/home/ubuntu/btc-updown-marketmaker/datasets}"
REMOTE_ALIYUNPAN_DIR="${DATA_COLLECT_ARCHIVE_ALIYUNPAN_DIR:-/new-datasets/}"

HK_PROXY_ENABLED="${DATA_COLLECT_ARCHIVE_HK_PROXY:-1}"
HK_PROXY_SOCKS_ADDR="${DATA_COLLECT_ARCHIVE_HK_SOCKS_ADDR:-127.0.0.1:20810}"
UPLOAD_VERIFY_ATTEMPTS="${DATA_COLLECT_ARCHIVE_UPLOAD_VERIFY_ATTEMPTS:-12}"
UPLOAD_VERIFY_SLEEP_SECONDS="${DATA_COLLECT_ARCHIVE_UPLOAD_VERIFY_SLEEP_SECONDS:-10}"
MAX_SOURCE_GAP_MS="${DATA_COLLECT_ARCHIVE_MAX_SOURCE_GAP_MS:-60000}"
VERIFY_SHA256="${DATA_COLLECT_ARCHIVE_VERIFY_SHA256:-1}"
SKIP_PULL="${DATA_COLLECT_ARCHIVE_SKIP_PULL:-0}"
SKIP_UPLOAD="${DATA_COLLECT_ARCHIVE_SKIP_UPLOAD:-0}"
SET_NEXT_RTC_WAKE="${DATA_COLLECT_ARCHIVE_SET_NEXT_RTC_WAKE:-0}"
GAMMA_BASE_URL="${DATA_COLLECT_ARCHIVE_GAMMA_BASE_URL:-https://gamma-api.polymarket.com/events/slug}"
GAMMA_TIMEOUT_SECONDS="${DATA_COLLECT_ARCHIVE_GAMMA_TIMEOUT_SECONDS:-10}"
GAMMA_WORKERS="${DATA_COLLECT_ARCHIVE_GAMMA_WORKERS:-4}"

THREADS="${DATA_COLLECT_ARCHIVE_THREADS:-$(nproc)}"

log() { printf '[%s] %s\n' "$(date -u +%H:%M:%S)" "$*"; }

normalize_date() {
  local raw="$1"
  if [[ "$raw" =~ ^[0-9]{8}$ ]]; then
    date -u -d "${raw:0:4}-${raw:4:2}-${raw:6:2}" +%F
  elif [[ "$raw" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    date -u -d "$raw" +%F
  else
    echo "error: date must be YYYY-MM-DD or YYYYMMDD, got '$raw'" >&2
    return 2
  fi
}

DATE_ISO="$(normalize_date "$DATE_INPUT")"
DATE_COMPACT="${DATE_ISO//-/}"
DAY_DIR="$LOCAL_ROOT/$DATE_ISO"
RAW_DAY_DIR="$DAY_DIR/typed_raw/utc_day=$DATE_ISO"
VALIDATED_DIR="$DAY_DIR/validated_markets"
ARCHIVE_DIR="$LOCAL_ROOT/archives"
REPORT_PATH="$DAY_DIR/validation_report.json"
P2B_PATH="$DAY_DIR/price_to_beat.parquet"
ARCHIVE_PATH="$ARCHIVE_DIR/new-datasets-$DATE_ISO.tar.gz"

check_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "error: required command not found: $1" >&2
    return 127
  fi
}

check_hk_proxy() {
  if [[ "$HK_PROXY_ENABLED" != "1" ]]; then
    return 0
  fi
  local host="${HK_PROXY_SOCKS_ADDR%:*}"
  local port="${HK_PROXY_SOCKS_ADDR##*:}"
  if [[ -z "$host" || -z "$port" || "$host" == "$port" ]]; then
    echo "error: invalid DATA_COLLECT_ARCHIVE_HK_SOCKS_ADDR=$HK_PROXY_SOCKS_ADDR" >&2
    return 2
  fi
  if ! nc -z -w 2 "$host" "$port"; then
    echo "error: Hong Kong proxy is enabled but not reachable at $HK_PROXY_SOCKS_ADDR" >&2
    return 1
  fi
}

ssh_rsh() {
  local ssh_cmd="ssh -o BatchMode=yes -o ConnectTimeout=20 -o ServerAliveInterval=15 -o ServerAliveCountMax=4"
  if [[ "$HK_PROXY_ENABLED" == "1" ]]; then
    ssh_cmd+=" -o 'ProxyCommand=nc -x ${HK_PROXY_SOCKS_ADDR} -X 5 %h %p'"
  fi
  printf '%s' "$ssh_cmd"
}

upload_and_verify() {
  local local_path="$1"
  local remote_dir="${2%/}"
  local remote_name
  local upload_rc=0
  local verify_attempt
  local local_sha1
  local remote_sha1
  remote_name="$(basename "$local_path")"
  local_sha1="$(sha1sum "$local_path" | awk '{print toupper($1)}')"

  remote_sha1_for_name() {
    env -u http_proxy -u https_proxy -u all_proxy -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY \
      aliyunpan ll "$remote_dir" 2>/dev/null \
      | awk -v name="$remote_name" '$NF == name {print toupper($4); exit}'
  }

  remote_sha1="$(remote_sha1_for_name || true)"
  if [[ -n "$remote_sha1" && "$remote_sha1" == "$local_sha1" ]]; then
    log "aliyunpan already has verified content $remote_dir/$remote_name sha1=$local_sha1"
    return 0
  fi

  set +e
  env -u http_proxy -u https_proxy -u all_proxy -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY \
    aliyunpan upload --ow "$local_path" "$remote_dir/"
  upload_rc=$?
  set -e
  if (( upload_rc != 0 )); then
    log "warning: aliyunpan upload returned rc=$upload_rc for $local_path; checking remote listing"
  fi

  for (( verify_attempt=1; verify_attempt<=UPLOAD_VERIFY_ATTEMPTS; verify_attempt++ )); do
    remote_sha1="$(remote_sha1_for_name || true)"
    if [[ -n "$remote_sha1" && "$remote_sha1" == "$local_sha1" ]]; then
      return 0
    fi
    if (( verify_attempt < UPLOAD_VERIFY_ATTEMPTS )); then
      log "warning: aliyunpan content verification mismatch for $remote_dir/$remote_name expected_sha1=$local_sha1 actual_sha1=${remote_sha1:-missing} (attempt $verify_attempt/$UPLOAD_VERIFY_ATTEMPTS); retrying in ${UPLOAD_VERIFY_SLEEP_SECONDS}s"
      sleep "$UPLOAD_VERIFY_SLEEP_SECONDS"
    fi
  done

  echo "error: aliyunpan upload verification failed: $remote_dir/$remote_name expected_sha1=$local_sha1 actual_sha1=${remote_sha1:-missing}" >&2
  return 1
}

schedule_next_rtc_wake() {
  if [[ "$SET_NEXT_RTC_WAKE" != "1" ]]; then
    return 0
  fi
  local next_wake_epoch
  next_wake_epoch="$(date -u -d 'today 23:45' +%s)"
  if (( next_wake_epoch <= $(date -u +%s) )); then
    next_wake_epoch="$(date -u -d 'tomorrow 23:45' +%s)"
  fi
  log "scheduling next RTC wake for $(date -u -d "@$next_wake_epoch" +%FT%TZ)"
  if [[ -w /sys/class/rtc/rtc0/wakealarm ]]; then
    printf '0\n' > /sys/class/rtc/rtc0/wakealarm
    printf '%s\n' "$next_wake_epoch" > /sys/class/rtc/rtc0/wakealarm
  else
    printf '0\n%s\n' "$next_wake_epoch" | sudo -n tee /sys/class/rtc/rtc0/wakealarm >/dev/null
  fi
}

pull_day() {
  if [[ "$SKIP_PULL" == "1" ]]; then
    log "skip pull because DATA_COLLECT_ARCHIVE_SKIP_PULL=1"
    return 0
  fi
  mkdir -p "$RAW_DAY_DIR"
  local rsh
  rsh="$(ssh_rsh)"
  log "pull typed_raw utc_day=$DATE_ISO from $REMOTE via Hong Kong route"
  rsync -az --delete --partial --partial-dir=.rsync-partial --prune-empty-dirs -e "$rsh" \
    "$REMOTE:$REMOTE_DATASET_ROOT/training/typed_raw/utc_day=$DATE_ISO/" \
    "$RAW_DAY_DIR/"
}

validate_and_select_markets() {
  mkdir -p "$VALIDATED_DIR"
  VALIDATION_DATE_ISO="$DATE_ISO" \
  RAW_DAY_DIR="$RAW_DAY_DIR" \
  VALIDATED_DIR="$VALIDATED_DIR" \
  REPORT_PATH="$REPORT_PATH" \
  P2B_PATH="$P2B_PATH" \
  MAX_SOURCE_GAP_MS="$MAX_SOURCE_GAP_MS" \
  VERIFY_SHA256="$VERIFY_SHA256" \
  GAMMA_BASE_URL="$GAMMA_BASE_URL" \
  GAMMA_TIMEOUT_SECONDS="$GAMMA_TIMEOUT_SECONDS" \
  GAMMA_WORKERS="$GAMMA_WORKERS" \
  python3 - <<'PY'
import concurrent.futures
import datetime as dt
import decimal
import hashlib
import json
import os
import pathlib
import re
import shutil
import sys
import urllib.error
import urllib.parse
import urllib.request

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

date_iso = os.environ["VALIDATION_DATE_ISO"]
raw_day_dir = pathlib.Path(os.environ["RAW_DAY_DIR"])
validated_dir = pathlib.Path(os.environ["VALIDATED_DIR"])
report_path = pathlib.Path(os.environ["REPORT_PATH"])
p2b_path = pathlib.Path(os.environ["P2B_PATH"])
max_gap_ms = int(os.environ.get("MAX_SOURCE_GAP_MS", "60000"))
verify_sha256 = os.environ.get("VERIFY_SHA256", "1") == "1"
gamma_base_url = os.environ["GAMMA_BASE_URL"].rstrip("/")
gamma_timeout_seconds = float(os.environ.get("GAMMA_TIMEOUT_SECONDS", "10"))
gamma_workers = max(1, int(os.environ.get("GAMMA_WORKERS", "4")))

source_kinds = [
    "polymarket_market",
    "polymarket_market_metadata",
    "binance_depth_snapshot",
    "binance_depth_update",
    "binance_aggtrade",
    "polymarket_rtds_chainlink_btc_usd",
]
continuous_sources = [
    "polymarket_market",
    "binance_depth_update",
    "binance_aggtrade",
    "polymarket_rtds_chainlink_btc_usd",
]
slug_re = re.compile(r"^btc-updown-5m-(\d+)$")

def utc_ms(value):
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=dt.timezone.utc)
        return int(value.timestamp() * 1000)
    if isinstance(value, (int, float)):
        return int(value)
    return None

def ms_dt(ms):
    return dt.datetime.fromtimestamp(ms / 1000, tz=dt.timezone.utc)

def list_parquets(source):
    source_dir = raw_day_dir / f"source_kind={source}"
    if not source_dir.exists():
        return []
    return sorted(
        p for p in source_dir.glob("*.parquet")
        if not any(token in p.name for token in ["running", "digesting", ".tmp", ".rsync-partial"])
    )

def manifest_for(parquet_path):
    return parquet_path.with_suffix(".manifest.json")

def sha256_file(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def validate_manifest(parquet_path, source):
    manifest_path = manifest_for(parquet_path)
    if not manifest_path.exists():
        return [f"missing_manifest:{manifest_path.name}"]
    issues = []
    try:
        manifest = json.loads(manifest_path.read_text())
    except Exception as exc:
        return [f"invalid_manifest_json:{manifest_path.name}:{exc}"]
    rows = pq.ParquetFile(parquet_path).metadata.num_rows
    if manifest.get("row_count") != rows:
        issues.append(f"row_count_mismatch:{parquet_path.name}:manifest={manifest.get('row_count')}:parquet={rows}")
    if manifest.get("required_typed_fields_present") is not True:
        issues.append(f"required_typed_fields_not_true:{manifest_path.name}")
    if manifest.get("validation_verdict") not in (None, "Complete"):
        issues.append(f"manifest_not_complete:{manifest_path.name}:{manifest.get('validation_verdict')}")
    expected_sha = manifest.get("sha256")
    for required_file in manifest.get("required_files") or []:
        if required_file.get("file_role") == "typed_raw_parquet":
            expected_sha = required_file.get("sha256") or expected_sha
            if required_file.get("row_count") not in (None, rows):
                issues.append(f"required_file_row_count_mismatch:{manifest_path.name}")
            if required_file.get("finalized") is not True:
                issues.append(f"required_file_not_finalized:{manifest_path.name}")
    if verify_sha256 and expected_sha:
        actual_sha = sha256_file(parquet_path)
        if actual_sha != expected_sha:
            issues.append(f"sha256_mismatch:{parquet_path.name}")
    path_source = parquet_path.parent.name.replace("source_kind=", "")
    if path_source != source:
        issues.append(f"source_path_mismatch:{parquet_path.name}:{path_source}!={source}")
    return issues

def read_source(source):
    files = list_parquets(source)
    if not files:
        return None
    return pa.concat_tables(
        [pq.read_table(path) for path in files],
        promote_options="default",
    )

def normalize_output_table(table):
    if table is None:
        return None
    legacy_replacements = [
        ("recv_time_utc", "timestamp_received"),
        ("snapshot_recv_time_utc", "timestamp_received"),
    ]
    drop_columns = {
        legacy
        for legacy, replacement in legacy_replacements
        if legacy in table.column_names and replacement in table.column_names
    }
    if drop_columns:
        table = table.select([name for name in table.column_names if name not in drop_columns])
    return table

def decimal_text(value):
    if value is None or isinstance(value, bool):
        return None
    try:
        number = value if isinstance(value, decimal.Decimal) else decimal.Decimal(str(value))
    except (decimal.InvalidOperation, ValueError):
        return None
    if not number.is_finite():
        return None
    return format(number, "f")

def diagnostic_bool(value):
    return value if isinstance(value, bool) else None

def fetch_gamma_record(item):
    slug, market = item
    source_url = f"{gamma_base_url}/{urllib.parse.quote(slug, safe='')}"
    fetched_at = None
    http_status = None
    payload = None
    error = None
    request = urllib.request.Request(
        source_url,
        headers={"Accept": "application/json", "User-Agent": "btc-updown-data-archive/1"},
    )
    try:
        with urllib.request.urlopen(request, timeout=gamma_timeout_seconds) as response:
            http_status = int(response.status)
            raw = response.read()
        payload = json.loads(raw.decode("utf-8"), parse_float=decimal.Decimal)
        if not isinstance(payload, dict):
            error = "response_not_object"
            payload = None
    except urllib.error.HTTPError as exc:
        http_status = int(exc.code)
        error = f"http_error:{exc.code}"
    except Exception as exc:
        error = f"request_error:{type(exc).__name__}:{exc}"
    finally:
        fetched_at = dt.datetime.now(tz=dt.timezone.utc)

    metadata = payload.get("eventMetadata") if isinstance(payload, dict) else None
    metadata = metadata if isinstance(metadata, dict) else {}
    price_to_beat = decimal_text(metadata.get("priceToBeat"))
    final_price = decimal_text(metadata.get("finalPrice"))
    fetch_succeeded = price_to_beat is not None and final_price is not None
    if error is None and not fetch_succeeded:
        error = "p2b_final_missing"

    event_id = payload.get("id") if isinstance(payload, dict) else None
    return {
        "schema_version": "price-to-beat-gamma-v1",
        "date_utc": date_iso,
        "slug": slug,
        "market_start_utc": ms_dt(market["start_ms"]),
        "market_end_utc": ms_dt(market["end_ms"]),
        "observed": slug in markets,
        "fetch_succeeded": fetch_succeeded,
        "polymarket_price_to_beat": price_to_beat,
        "polymarket_final_price": final_price,
        "source_url": source_url,
        "fetched_at_utc": fetched_at,
        "http_status": http_status,
        "gamma_event_id": str(event_id) if event_id is not None else None,
        "gamma_closed": diagnostic_bool(payload.get("closed")) if isinstance(payload, dict) else None,
        "gamma_active": diagnostic_bool(payload.get("active")) if isinstance(payload, dict) else None,
        "diagnostic_error": error,
    }

def fetch_gamma_price_to_beat(target_markets):
    ordered = sorted(target_markets.items(), key=lambda item: (item[1]["start_ms"], item[0]))
    expected_slugs = [slug for slug, _market in ordered]
    if p2b_path.exists():
        existing = pq.read_table(p2b_path)
        required_columns = {
            "slug",
            "fetch_succeeded",
            "polymarket_price_to_beat",
            "polymarket_final_price",
            "diagnostic_error",
        }
        missing_columns = sorted(required_columns - set(existing.column_names))
        existing_slugs = existing["slug"].to_pylist() if "slug" in existing.column_names else []
        if missing_columns or existing.num_rows != len(expected_slugs) or existing_slugs != expected_slugs:
            raise RuntimeError(
                "existing price_to_beat.parquet does not match the complete D-day slug contract: "
                f"missing_columns={missing_columns} rows={existing.num_rows} "
                f"expected_rows={len(expected_slugs)}"
            )
        return existing.to_pylist(), True, 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=min(gamma_workers, len(ordered))) as executor:
        rows = list(executor.map(fetch_gamma_record, ordered))
    schema = pa.schema([
        ("schema_version", pa.string()),
        ("date_utc", pa.string()),
        ("slug", pa.string()),
        ("market_start_utc", pa.timestamp("ms", tz="UTC")),
        ("market_end_utc", pa.timestamp("ms", tz="UTC")),
        ("observed", pa.bool_()),
        ("fetch_succeeded", pa.bool_()),
        ("polymarket_price_to_beat", pa.string()),
        ("polymarket_final_price", pa.string()),
        ("source_url", pa.string()),
        ("fetched_at_utc", pa.timestamp("ms", tz="UTC")),
        ("http_status", pa.int32()),
        ("gamma_event_id", pa.string()),
        ("gamma_closed", pa.bool_()),
        ("gamma_active", pa.bool_()),
        ("diagnostic_error", pa.string()),
    ])
    table = pa.Table.from_pylist(rows, schema=schema)
    p2b_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = p2b_path.with_name(f"{p2b_path.name}.tmp")
    temporary_path.unlink(missing_ok=True)
    pq.write_table(table, temporary_path, compression="zstd", compression_level=3)
    os.replace(temporary_path, p2b_path)
    return rows, False, len(rows)

def timestamp_list(table, timestamp_col="timestamp_received"):
    if table is None or timestamp_col not in table.column_names:
        return []
    return sorted(ms for ms in (utc_ms(v) for v in table[timestamp_col].to_pylist()) if ms is not None)

def time_filter(table, start_ms, end_ms):
    if table is None or table.num_rows == 0 or "timestamp_received" not in table.column_names:
        return table.slice(0, 0) if table is not None else None
    ts = table["timestamp_received"]
    start_scalar = pa.scalar(ms_dt(start_ms), type=ts.type)
    end_scalar = pa.scalar(ms_dt(end_ms), type=ts.type)
    mask = pc.and_(pc.greater_equal(ts, start_scalar), pc.less(ts, end_scalar))
    return table.filter(mask)

def slug_filter(table, slug):
    if table is None or table.num_rows == 0:
        return table
    masks = []
    for name in ("slug", "market"):
        if name in table.column_names:
            masks.append(pc.equal(table[name], pa.scalar(slug, type=table[name].type)))
    if not masks:
        return table.slice(0, 0)
    mask = masks[0]
    for extra in masks[1:]:
        mask = pc.or_(mask, extra)
    return table.filter(mask)

def write_table(table, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    if table is None:
        return
    table = normalize_output_table(table)
    pq.write_table(table, path, compression="zstd", compression_level=3)

def gap_report(times, start_ms, end_ms):
    scoped = [t for t in times if start_ms <= t < end_ms]
    if not scoped:
        return {"rows": 0, "max_gap_ms": end_ms - start_ms, "bad": True, "reason": "no_rows_in_market_window"}
    gaps = [scoped[0] - start_ms]
    gaps.extend(b - a for a, b in zip(scoped, scoped[1:]))
    gaps.append(end_ms - scoped[-1])
    max_gap = max(gaps)
    return {
        "rows": len(scoped),
        "first_timestamp_received_ms": scoped[0],
        "last_timestamp_received_ms": scoped[-1],
        "max_gap_ms": max_gap,
        "bad": max_gap >= max_gap_ms,
        "reason": "continuous_gap_at_least_threshold" if max_gap >= max_gap_ms else None,
    }

if not raw_day_dir.exists():
    print(f"error: missing raw day directory: {raw_day_dir}", file=sys.stderr)
    sys.exit(1)

partial_tokens = ["running", "digesting", ".tmp", ".rsync-partial"]
partial_files = sorted(
    str(p.relative_to(report_path.parent)) for p in raw_day_dir.rglob("*")
    if p.is_file() and any(token in str(p.relative_to(raw_day_dir)) for token in partial_tokens)
)

manifest_issues = {}
source_files = {}
for source in source_kinds:
    files = list_parquets(source)
    source_files[source] = [str(p) for p in files]
    source_issues = []
    for parquet_path in files:
        source_issues.extend(validate_manifest(parquet_path, source))
    if source_issues:
        manifest_issues[source] = source_issues
if manifest_issues:
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps({"date_utc": date_iso, "verdict": "FAIL_MANIFEST", "partial_artifacts": partial_files, "manifest_issues": manifest_issues}, indent=2, sort_keys=True))
    print(f"error: manifest validation failed; see {report_path}", file=sys.stderr)
    sys.exit(1)

tables = {source: read_source(source) for source in source_kinds}
day_start = int(dt.datetime.fromisoformat(date_iso).replace(tzinfo=dt.timezone.utc).timestamp() * 1000)
day_end = day_start + 24 * 60 * 60 * 1000

markets = {}
metadata_boundary_issues = {}
metadata_table = tables.get("polymarket_market_metadata")
if metadata_table is not None and {"slug", "market_start_utc", "market_end_utc"}.issubset(set(metadata_table.column_names)):
    for row in metadata_table.select(["slug", "market_start_utc", "market_end_utc"]).to_pylist():
        slug = row.get("slug")
        match = slug_re.match(str(slug)) if slug else None
        if match is None:
            continue
        slug = str(slug)
        start_ms = int(match.group(1)) * 1000
        end_ms = start_ms + 5 * 60 * 1000
        metadata_start_ms = utc_ms(row.get("market_start_utc"))
        metadata_end_ms = utc_ms(row.get("market_end_utc"))
        boundary_issues = []
        if metadata_start_ms is not None and metadata_start_ms != start_ms:
            boundary_issues.append(
                f"market_start_mismatch:metadata={metadata_start_ms}:slug={start_ms}"
            )
        if metadata_end_ms is not None and metadata_end_ms != end_ms:
            boundary_issues.append(
                f"market_end_mismatch:metadata={metadata_end_ms}:slug={end_ms}"
            )
        if boundary_issues:
            metadata_boundary_issues.setdefault(slug, []).extend(boundary_issues)
        if day_start <= start_ms < day_end:
            markets.setdefault(slug, {"slug": slug, "start_ms": start_ms, "end_ms": end_ms})

pm_table = tables.get("polymarket_market")
if pm_table is not None:
    for name in ("slug", "market"):
        if name not in pm_table.column_names:
            continue
        for value in pm_table[name].to_pylist():
            if not value:
                continue
            match = slug_re.match(str(value))
            if not match:
                continue
            start_ms = int(match.group(1)) * 1000
            if day_start <= start_ms < day_end:
                markets.setdefault(value, {"slug": value, "start_ms": start_ms, "end_ms": start_ms + 5 * 60 * 1000})

market_window_ms = 5 * 60 * 1000
nominal_markets = {
    f"btc-updown-5m-{start_ms // 1000}": {
        "slug": f"btc-updown-5m-{start_ms // 1000}",
        "start_ms": start_ms,
        "end_ms": start_ms + 5 * 60 * 1000,
    }
    for start_ms in range(day_start, day_end, market_window_ms)
}
off_grid_market_slugs = {
    slug for slug, market in markets.items()
    if (market["start_ms"] - day_start) % market_window_ms != 0
}
p2b_rows, p2b_artifact_reused, gamma_http_request_count = (
    fetch_gamma_price_to_beat(nominal_markets)
)
p2b_by_slug = {row["slug"]: row for row in p2b_rows}
p2b_summary = {
    "artifact": p2b_path.name,
    "source": "gamma_event_by_slug_post_close",
    "date_utc": date_iso,
    "nominal_market_count": len(nominal_markets),
    "observed_market_count": len(markets),
    "off_grid_observed_market_count": len(off_grid_market_slugs),
    "target_market_count": len(p2b_rows),
    "gamma_http_request_count": gamma_http_request_count,
    "artifact_reused": p2b_artifact_reused,
    "successful_market_count": sum(row["fetch_succeeded"] for row in p2b_rows),
    "successful_observed_market_count": sum(
        row["fetch_succeeded"] for row in p2b_rows if row["observed"]
    ),
    "required_fields": ["eventMetadata.priceToBeat", "eventMetadata.finalPrice"],
    "retry_policy": "one request per D market; an existing complete D artifact is reused; no retry and no D+2 fetch",
    "diagnostic_fields_do_not_gate": ["gamma_event_id", "gamma_closed", "gamma_active"],
}

if not markets:
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps({
        "date_utc": date_iso,
        "verdict": "FAIL_NO_MARKETS",
        "partial_artifacts": partial_files,
        "source_files": source_files,
        "price_to_beat": p2b_summary,
    }, indent=2, sort_keys=True))
    print(f"error: no btc-updown-5m markets found for {date_iso}", file=sys.stderr)
    sys.exit(1)

global_times = {
    source: timestamp_list(tables.get(source))
    for source in ("binance_depth_update", "binance_aggtrade", "polymarket_rtds_chainlink_btc_usd")
}
pm_times_by_slug = {}
if pm_table is not None and "timestamp_received" in pm_table.column_names:
    cols = [name for name in ("timestamp_received", "slug", "market") if name in pm_table.column_names]
    for row in pm_table.select(cols).to_pylist():
        ts = utc_ms(row.get("timestamp_received"))
        if ts is None:
            continue
        candidates = [row.get("slug"), row.get("market")]
        for candidate in candidates:
            if candidate and slug_re.match(str(candidate)):
                pm_times_by_slug.setdefault(str(candidate), []).append(ts)
for times in pm_times_by_slug.values():
    times.sort()

accepted = []
rejected = []
selected_root = validated_dir
if selected_root.exists():
    shutil.rmtree(selected_root)
selected_root.mkdir(parents=True, exist_ok=True)

metadata_slugs = set()
if metadata_table is not None and "slug" in metadata_table.column_names:
    metadata_slugs = {str(v) for v in metadata_table["slug"].to_pylist() if v}

for slug, market in sorted(markets.items(), key=lambda item: item[1]["start_ms"]):
    start_ms = market["start_ms"]
    end_ms = market["end_ms"]
    source_reports = {}
    reasons = []

    pm_report = gap_report(pm_times_by_slug.get(slug, []), start_ms, end_ms)
    source_reports["polymarket_market"] = pm_report
    if pm_report["bad"]:
        reasons.append(f"polymarket_market:{pm_report['reason']}:max_gap_ms={pm_report['max_gap_ms']}")

    for source in ("binance_depth_update", "binance_aggtrade", "polymarket_rtds_chainlink_btc_usd"):
        source_report = gap_report(global_times.get(source, []), start_ms, end_ms)
        source_reports[source] = source_report
        if source_report["bad"]:
            reasons.append(f"{source}:{source_report['reason']}:max_gap_ms={source_report['max_gap_ms']}")

    if slug not in metadata_slugs:
        reasons.append("polymarket_market_metadata:missing_market_metadata_row")
    for issue in metadata_boundary_issues.get(slug, []):
        reasons.append(f"polymarket_market_metadata:{issue}")

    if slug in off_grid_market_slugs:
        reasons.append("market_identity:slug_not_on_utc_5m_grid")

    p2b_row = p2b_by_slug.get(slug)
    if slug not in off_grid_market_slugs and (p2b_row is None or not p2b_row["fetch_succeeded"]):
        diagnostic_error = p2b_row["diagnostic_error"] if p2b_row is not None else "missing_target_record"
        reasons.append(f"price_to_beat:{diagnostic_error}")

    market_record = {
        "slug": slug,
        "start_utc_ms": start_ms,
        "end_utc_ms": end_ms,
        "start_utc": ms_dt(start_ms).isoformat(),
        "end_utc": ms_dt(end_ms).isoformat(),
        "source_reports": source_reports,
        "price_to_beat": {
            "fetch_succeeded": p2b_row["fetch_succeeded"] if p2b_row is not None else False,
            "polymarket_price_to_beat": p2b_row["polymarket_price_to_beat"] if p2b_row is not None else None,
            "polymarket_final_price": p2b_row["polymarket_final_price"] if p2b_row is not None else None,
            "fetched_at_utc": p2b_row["fetched_at_utc"].isoformat() if p2b_row is not None else None,
            "http_status": p2b_row["http_status"] if p2b_row is not None else None,
            "gamma_event_id": p2b_row["gamma_event_id"] if p2b_row is not None else None,
            "gamma_closed": p2b_row["gamma_closed"] if p2b_row is not None else None,
            "gamma_active": p2b_row["gamma_active"] if p2b_row is not None else None,
            "diagnostic_error": p2b_row["diagnostic_error"] if p2b_row is not None else "missing_target_record",
        },
    }
    if reasons:
        market_record["reasons"] = reasons
        rejected.append(market_record)
        continue

    market_dir = selected_root / slug
    market_dir.mkdir(parents=True, exist_ok=True)
    (market_dir / "market_window.json").write_text(json.dumps(market_record, indent=2, sort_keys=True))

    pm_selected = slug_filter(time_filter(pm_table, start_ms, end_ms), slug)
    write_table(pm_selected, market_dir / "polymarket_market_selected_window.parquet")

    if metadata_table is not None:
        metadata_selected = slug_filter(metadata_table, slug)
        write_table(metadata_selected, market_dir / "polymarket_market_metadata_selected_window.parquet")

    for source in ("binance_depth_update", "binance_aggtrade", "polymarket_rtds_chainlink_btc_usd"):
        selected = time_filter(tables.get(source), start_ms, end_ms)
        write_table(selected, market_dir / f"{source}_selected_window.parquet")

    snapshot_table = tables.get("binance_depth_snapshot")
    if snapshot_table is not None and "timestamp_received" in snapshot_table.column_names:
        rows = snapshot_table.to_pylist()
        eligible = [(utc_ms(row.get("timestamp_received")), idx) for idx, row in enumerate(rows)]
        eligible = [(ts, idx) for ts, idx in eligible if ts is not None and ts <= start_ms]
        if eligible:
            _, idx = max(eligible)
            write_table(snapshot_table.slice(idx, 1), market_dir / "binance_depth_snapshot_bootstrap.parquet")

    accepted.append(market_record)

report = {
    "date_utc": date_iso,
    "verdict": "PASS" if accepted else "FAIL_NO_ACCEPTED_MARKETS",
    "max_source_gap_ms": max_gap_ms,
    "continuous_gap_policy": {
        "continuous_sources": continuous_sources,
        "non_continuous_sources": ["binance_depth_snapshot", "polymarket_market_metadata"],
        "drop_scope": "drop only the failing 5-minute market window; retain other accepted markets",
    },
    "source_files": source_files,
    "partial_artifacts": partial_files,
    "price_to_beat": p2b_summary,
    "nominal_market_count": len(nominal_markets),
    "off_grid_observed_markets": sorted(off_grid_market_slugs),
    "market_count": len(markets),
    "accepted_market_count": len(accepted),
    "rejected_market_count": len(rejected),
    "accepted_markets": accepted,
    "rejected_markets": rejected,
}
report_path.parent.mkdir(parents=True, exist_ok=True)
report_path.write_text(json.dumps(report, indent=2, sort_keys=True))
(report_path.parent / "accepted_markets.json").write_text(json.dumps(accepted, indent=2, sort_keys=True))
(report_path.parent / "rejected_markets.json").write_text(json.dumps(rejected, indent=2, sort_keys=True))

if not accepted:
    print(f"error: no accepted markets for {date_iso}; see {report_path}", file=sys.stderr)
    sys.exit(1)

print(f"validation_pass date={date_iso} accepted={len(accepted)} rejected={len(rejected)} report={report_path}")
PY
}

build_archive() {
  mkdir -p "$ARCHIVE_DIR"
  rm -f "$ARCHIVE_PATH"
  log "package accepted markets -> $ARCHIVE_PATH"
  if command -v pigz >/dev/null 2>&1; then
    tar -C "$DAY_DIR" -cf - validation_report.json accepted_markets.json rejected_markets.json price_to_beat.parquet validated_markets \
      | pigz -p "$THREADS" > "$ARCHIVE_PATH"
  else
    tar -C "$DAY_DIR" -czf "$ARCHIVE_PATH" validation_report.json accepted_markets.json rejected_markets.json price_to_beat.parquet validated_markets
  fi
  sha256sum "$ARCHIVE_PATH" > "$ARCHIVE_PATH.sha256"
}

main() {
  check_command rsync
  check_command ssh
  check_command nc
  check_command python3
  check_command tar
  check_command sha1sum
  check_command sha256sum
  if [[ "$SKIP_UPLOAD" != "1" ]]; then
    check_command aliyunpan
  fi
  check_hk_proxy

  mkdir -p "$LOCAL_ROOT" "$DAY_DIR" "$ARCHIVE_DIR"
  log "archive date UTC $DATE_ISO ($DATE_COMPACT)"
  pull_day
  log "validate manifests, fetch Gamma P2B, and select healthy 5-minute markets"
  validate_and_select_markets
  build_archive
  if [[ "$SKIP_UPLOAD" == "1" ]]; then
    log "skip upload because DATA_COLLECT_ARCHIVE_SKIP_UPLOAD=1"
  else
    log "upload archive to aliyunpan $REMOTE_ALIYUNPAN_DIR"
    upload_and_verify "$ARCHIVE_PATH" "$REMOTE_ALIYUNPAN_DIR"
    upload_and_verify "$ARCHIVE_PATH.sha256" "$REMOTE_ALIYUNPAN_DIR"
  fi
  schedule_next_rtc_wake
  log "done archive=$ARCHIVE_PATH size=$(du -sh "$ARCHIVE_PATH" | awk '{print $1}') report=$REPORT_PATH"
}

main "$@"
