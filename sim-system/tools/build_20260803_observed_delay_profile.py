#!/usr/bin/env python3
"""从固定 2026-08-03 清单构建首版采集观测代理时延配置档。"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import stat
import sys
import tempfile
from collections import Counter
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


EXPECTED_MANIFEST_SHA256 = (
    "342d2fa2c55a1c4b9446090cffd57e31814554abb319ff412e72200a7e5aabdf"
)
EXPECTED_STATS = {
    "sample_count": 39_618_963,
    "missing_source_timestamp_count": 0,
    "negative_delay_count": 0,
    "min_delay_ms": 6,
    "p50_delay_ms": 10,
    "p85_delay_ms": 19,
    "p90_delay_ms": 30,
    "p95_delay_ms": 63,
    "p99_delay_ms": 187,
    "max_delay_ms": 29_697,
    "histogram_bin_count": 9_666,
}
PROFILE_ID = "btc_updown_5m_collector_observed_delay_20260803_v1"
PROFILE_VERSION = 1
SCHEMA_VERSION = "sim_proxy_latency_profile_v1"
SAMPLER_VERSION = "sha256_multiply_high_empirical_rank_v1"
DEFAULT_MANIFEST = Path(
    "/home/chillchill/new-datasets/2026-08-03/typed_raw/utc_day=2026-08-03/manifest.json"
)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def nearest_rank(histogram: Counter[int], sample_count: int, percentile: int) -> int:
    rank = (sample_count * percentile + 99) // 100
    cumulative = 0
    for delay_ms, count in sorted(histogram.items()):
        cumulative += count
        if cumulative >= rank:
            return delay_ms
    raise RuntimeError(f"无法解析 p{percentile} 的 nearest-rank")


def selected_members(manifest: dict) -> list[dict]:
    if manifest.get("schema_version") != "data_collect_daily_manifest_v3":
        raise RuntimeError("清单 schema_version 不符合固定输入")
    if manifest.get("utc_day") != "2026-08-03":
        raise RuntimeError("清单 utc_day 不符合固定输入")
    if manifest.get("state") != "closed" or manifest.get("gaps") != []:
        raise RuntimeError("清单未关闭或存在缺口")

    members = [
        member
        for member in manifest["files"]
        if member.get("source_kind") == "polymarket_market"
        and member.get("schema_version") == "polymarket_orderbook_v3"
    ]
    members.sort(key=lambda member: member["utc_hour"])
    expected_hours = [f"2026-08-03T{hour:02d}" for hour in range(24)]
    if len(members) != 24 or [member["utc_hour"] for member in members] != expected_hours:
        raise RuntimeError("固定输入必须恰好覆盖 24 个连续小时文件")
    return members


def scan_member(path: Path, expected: dict, histogram: Counter[int]) -> tuple[int, int]:
    stat = path.stat()
    if stat.st_size != expected["bytes"]:
        raise RuntimeError(f"文件字节数不匹配: {path}")
    if sha256_file(path) != expected["sha256"]:
        raise RuntimeError(f"文件 SHA-256 不匹配: {path}")

    parquet = pq.ParquetFile(path)
    if parquet.metadata.num_rows != expected["rows"]:
        raise RuntimeError(f"Parquet footer 行数不匹配: {path}")
    schema = parquet.schema_arrow
    if schema.field("timestamp_received").type != pa.timestamp("ms", tz="UTC"):
        raise RuntimeError(f"timestamp_received 类型不匹配: {path}")
    if schema.field("timestamp").type != pa.timestamp("ms", tz="UTC"):
        raise RuntimeError(f"timestamp 类型不匹配: {path}")

    scanned_rows = 0
    missing_source = 0
    for batch in parquet.iter_batches(
        batch_size=262_144,
        columns=["timestamp_received", "timestamp"],
        use_threads=False,
    ):
        scanned_rows += batch.num_rows
        received = pc.cast(batch.column(0), pa.int64())
        source = pc.cast(batch.column(1), pa.int64())
        missing_source += source.null_count
        delays = pc.drop_null(pc.subtract(received, source))
        value_counts = pc.value_counts(delays)
        values = value_counts.field("values").to_pylist()
        counts = value_counts.field("counts").to_pylist()
        histogram.update({int(value): int(count) for value, count in zip(values, counts)})

    if scanned_rows != expected["rows"]:
        raise RuntimeError(f"实际扫描行数不匹配: {path}")
    return scanned_rows, missing_source


def build_profile(manifest_path: Path) -> dict:
    manifest_bytes = manifest_path.read_bytes()
    if hashlib.sha256(manifest_bytes).hexdigest() != EXPECTED_MANIFEST_SHA256:
        raise RuntimeError("清单 SHA-256 不符合固定输入")
    manifest = json.loads(manifest_bytes)
    members = selected_members(manifest)

    histogram: Counter[int] = Counter()
    sample_rows = 0
    missing_source = 0
    market_slugs: set[str] = set()
    host_ids: set[str] = set()
    regions: set[str] = set()
    source_config_digests: set[str] = set()
    selected_files = []

    for member in members:
        member_path = manifest_path.parent / member["path"]
        rows, missing = scan_member(member_path, member, histogram)
        sample_rows += rows
        missing_source += missing
        facts = member["facts"]
        market_slugs.update(facts["covered_market_slugs"])
        host_ids.add(facts["collector_host_id"])
        regions.add(facts["collector_region"])
        source_config_digests.add(facts["source_config_digest"])
        selected_files.append(
            {
                "bytes": member["bytes"],
                "path": member["path"],
                "rows": member["rows"],
                "sha256": member["sha256"],
                "utc_hour": member["utc_hour"],
            }
        )

    negative_delay_count = sum(count for delay, count in histogram.items() if delay < 0)
    if not histogram:
        raise RuntimeError("固定输入没有可用延迟样本")
    observed_stats = {
        "sample_count": sample_rows - missing_source,
        "missing_source_timestamp_count": missing_source,
        "negative_delay_count": negative_delay_count,
        "min_delay_ms": min(histogram),
        "p50_delay_ms": nearest_rank(histogram, sample_rows - missing_source, 50),
        "p85_delay_ms": nearest_rank(histogram, sample_rows - missing_source, 85),
        "p90_delay_ms": nearest_rank(histogram, sample_rows - missing_source, 90),
        "p95_delay_ms": nearest_rank(histogram, sample_rows - missing_source, 95),
        "p99_delay_ms": nearest_rank(histogram, sample_rows - missing_source, 99),
        "max_delay_ms": max(histogram),
        "histogram_bin_count": len(histogram),
    }
    if observed_stats != EXPECTED_STATS:
        raise RuntimeError(
            "重算统计与已批准固定输入不一致: "
            + json.dumps(observed_stats, sort_keys=True)
        )
    if any(not slug.startswith("btc-updown-5m-") for slug in market_slugs):
        raise RuntimeError("固定输入包含目标系列之外的市场")
    if len(market_slugs) != 290:
        raise RuntimeError("固定输入的唯一市场数量不等于 290")

    script_path = Path(__file__).resolve()
    return {
        "application_policy": "PROJECT_ASSUMPTION",
        "evidence_kind": "COLLECTOR_OBSERVED_FEED_DELAY",
        "generator": {
            "pyarrow_version": pa.__version__,
            "source_path": "sim-system/tools/build_20260803_observed_delay_profile.py",
            "source_sha256": sha256_file(script_path),
        },
        "histogram": [
            {"count": count, "delay_ms": delay_ms}
            for delay_ms, count in sorted(histogram.items())
        ],
        "lineage": {
            "collector_binary_sha256": manifest["collector_binary_sha256"],
            "collector_host_ids": sorted(host_ids),
            "collector_regions": sorted(regions),
            "dataset_manifest_path_hint": (
                "2026-08-03/typed_raw/utc_day=2026-08-03/manifest.json"
            ),
            "dataset_manifest_sha256": EXPECTED_MANIFEST_SHA256,
            "file_count": len(members),
            "market_count": len(market_slugs),
            "market_slug_prefix": "btc-updown-5m-",
            "selected_files": selected_files,
            "source_config_digests": sorted(source_config_digests),
            "source_kind": "polymarket_market",
            "source_schema_version": "polymarket_orderbook_v3",
            "utc_day": "2026-08-03",
        },
        "limitations": [
            "NOT_EXACT_NETWORK_DELAY",
            "NO_SHARED_CLOCK_AUTHORITY_PROOF",
            "MARGINAL_DISTRIBUTION_ONLY",
            "BIDIRECTIONAL_REUSE_IS_PROJECT_ASSUMPTION",
            "HISTORICAL_PUBLIC_EVENTS_ARE_NOT_RESAMPLED",
            "ITODE_IS_EXCLUDED",
        ],
        "profile_id": PROFILE_ID,
        "profile_version": PROFILE_VERSION,
        "sample_definition": {
            "formula": "timestamp_received_utc_ms - source_event_timestamp_utc_ms",
            "percentile_method": "nearest_rank",
            "unit": "decoded_websocket_message",
        },
        "sampler": {
            "algorithm_version": SAMPLER_VERSION,
            "domains": [
                "REQUEST_OUTBOUND_V1",
                "DIRECT_RESPONSE_INBOUND_V1",
                "LIFECYCLE_FEEDBACK_INBOUND_V1",
            ],
            "historical_public_events_resampled": False,
            "itode_included": False,
            "physical_retry_resamples": False,
        },
        "schema_version": SCHEMA_VERSION,
        "statistics": observed_stats,
    }


def publish_profile(profile: dict, output_root: Path) -> tuple[Path, str, int]:
    profile_bytes = (
        json.dumps(profile, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        + "\n"
    ).encode("utf-8")
    profile_sha256 = hashlib.sha256(profile_bytes).hexdigest()
    output_root.mkdir(parents=True, exist_ok=True)

    candidate_path: Path | None = None
    target_dir = output_root / profile_sha256
    target_path = target_dir / "profile.json"
    if target_dir.exists():
        directory_stat = target_dir.lstat()
        if not stat.S_ISDIR(directory_stat.st_mode) or target_dir.is_symlink():
            raise FileExistsError(f"published target is not a real directory: {target_dir}")
        if sorted(path.name for path in target_dir.iterdir()) != ["profile.json"]:
            raise FileExistsError(f"published target layout is not exact: {target_dir}")
        file_fd = os.open(target_path, os.O_RDONLY | os.O_NOFOLLOW)
        try:
            with os.fdopen(file_fd, "rb", closefd=False) as published:
                if published.read() != profile_bytes:
                    raise FileExistsError(
                        f"published target bytes do not match content identity: {target_path}"
                    )
            os.fsync(file_fd)
        finally:
            os.close(file_fd)
        for directory in (target_dir, output_root):
            directory_fd = os.open(
                directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
            )
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        return target_path, profile_sha256, len(profile_bytes)

    published = False
    try:
        with tempfile.NamedTemporaryFile(
            dir=output_root,
            prefix=".profile.",
            suffix=".candidate",
            delete=False,
        ) as candidate:
            candidate_path = Path(candidate.name)
            candidate.write(profile_bytes)
            candidate.flush()
            os.fsync(candidate.fileno())
        target_dir.mkdir()
        os.link(candidate_path, target_path, follow_symlinks=False)
        published = True
        candidate_path.unlink()
        candidate_path = None
        with target_path.open("rb") as published:
            os.fsync(published.fileno())
        directory_fd = os.open(target_dir, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        root_fd = os.open(output_root, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(root_fd)
        finally:
            os.close(root_fd)
        return target_path, profile_sha256, len(profile_bytes)
    except Exception:
        if candidate_path is not None:
            candidate_path.unlink(missing_ok=True)
        if not published:
            try:
                target_dir.rmdir()
            except OSError:
                pass
        raise


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "config" / "latency-profiles",
    )
    args = parser.parse_args()

    profile = build_profile(args.manifest.resolve())
    target, profile_sha256, byte_count = publish_profile(profile, args.output_root.resolve())
    print(
        "proxy_latency_profile=PASS "
        f"path={target} sha256={profile_sha256} bytes={byte_count} "
        f"samples={EXPECTED_STATS['sample_count']} bins={EXPECTED_STATS['histogram_bin_count']}"
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"proxy_latency_profile=FAIL error={error}", file=sys.stderr)
        raise
