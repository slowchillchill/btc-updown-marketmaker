import collections
import datetime as dt
import hashlib
import json
import os
import pathlib
import subprocess
import tarfile
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pyarrow as pa
import pyarrow.parquet as pq


REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "data-collect/scripts/run_daily_new_datasets_archive.sh"
TIMER = REPO_ROOT / "data-collect/deploy/systemd/data-collect-new-datasets-archive.timer"
DATE_ISO = "2026-07-09"
DAY_START = int(
    dt.datetime.fromisoformat(DATE_ISO).replace(tzinfo=dt.timezone.utc).timestamp()
)
OBSERVED_SLUG = f"btc-updown-5m-{DAY_START}"


def utc_timestamp_array(epoch_seconds):
    values = [dt.datetime.fromtimestamp(value, tz=dt.timezone.utc) for value in epoch_seconds]
    return pa.array(values, type=pa.timestamp("ms", tz="UTC"))


def write_parquet_with_manifest(path, table):
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path)
    manifest = {
        "row_count": table.num_rows,
        "required_typed_fields_present": True,
        "validation_verdict": "Complete",
    }
    path.with_suffix(".manifest.json").write_text(json.dumps(manifest))


def build_day_fixture(local_root, metadata_start_offset=0, metadata_end_offset=300):
    raw_day = local_root / DATE_ISO / "typed_raw" / f"utc_day={DATE_ISO}"
    offsets = list(range(0, 300, 30))
    event_seconds = [DAY_START + offset for offset in offsets]
    received = utc_timestamp_array(event_seconds)

    polymarket = pa.table(
        {
            "timestamp_received": received,
            "slug": pa.array([OBSERVED_SLUG] * len(offsets)),
        }
    )
    metadata = pa.table(
        {
            "slug": pa.array([OBSERVED_SLUG]),
            "market_start_utc": utc_timestamp_array([DAY_START + metadata_start_offset]),
            "market_end_utc": utc_timestamp_array([DAY_START + metadata_end_offset]),
        }
    )
    continuous = pa.table({"timestamp_received": received})
    aggtrade = pa.table(
        {
            "timestamp_received": received,
            "event_time": utc_timestamp_array([value - 1 for value in event_seconds]),
            "trade_time": utc_timestamp_array([value - 2 for value in event_seconds]),
        }
    )
    snapshot = pa.table({"timestamp_received": utc_timestamp_array([DAY_START])})

    tables = {
        "polymarket_market": polymarket,
        "polymarket_market_metadata": metadata,
        "binance_depth_snapshot": snapshot,
        "binance_depth_update": continuous,
        "binance_aggtrade": aggtrade,
        "polymarket_rtds_chainlink_btc_usd": continuous,
    }
    for source, table in tables.items():
        path = raw_day / f"source_kind={source}" / f"{source}_{DATE_ISO}T00.parquet"
        write_parquet_with_manifest(path, table)

    partial = (
        raw_day
        / "source_kind=binance_aggtrade"
        / f"binance_aggtrade_{DATE_ISO}T08-running.parquet"
    )
    partial.write_bytes(b"not-a-parquet-file")
    return partial


def append_off_grid_market(local_root, offset_seconds=1):
    raw_day = local_root / DATE_ISO / "typed_raw" / f"utc_day={DATE_ISO}"
    slug = f"btc-updown-5m-{DAY_START + offset_seconds}"
    offsets = list(range(0, 300, 30))

    polymarket_path = (
        raw_day
        / "source_kind=polymarket_market"
        / f"polymarket_market_{DATE_ISO}T00.parquet"
    )
    polymarket = pq.ParquetFile(polymarket_path).read()
    extra_polymarket = pa.table(
        {
            "timestamp_received": utc_timestamp_array(
                [DAY_START + offset_seconds + value for value in offsets]
            ),
            "slug": pa.array([slug] * len(offsets)),
        }
    )
    write_parquet_with_manifest(
        polymarket_path,
        pa.concat_tables([polymarket, extra_polymarket]),
    )

    metadata_path = (
        raw_day
        / "source_kind=polymarket_market_metadata"
        / f"polymarket_market_metadata_{DATE_ISO}T00.parquet"
    )
    metadata = pq.ParquetFile(metadata_path).read()
    extra_metadata = pa.table(
        {
            "slug": pa.array([slug]),
            "market_start_utc": utc_timestamp_array([DAY_START + offset_seconds]),
            "market_end_utc": utc_timestamp_array([DAY_START + offset_seconds + 300]),
        }
    )
    write_parquet_with_manifest(
        metadata_path,
        pa.concat_tables([metadata, extra_metadata]),
    )
    return slug


def make_aggtrade_day_mixed_v2_v3(local_root):
    source_dir = (
        local_root
        / DATE_ISO
        / "typed_raw"
        / f"utc_day={DATE_ISO}"
        / "source_kind=binance_aggtrade"
    )
    original = source_dir / f"binance_aggtrade_{DATE_ISO}T00.parquet"
    table = pq.ParquetFile(original).read()
    original.unlink()
    original.with_suffix(".manifest.json").unlink()

    v2_path = source_dir / f"binance_aggtrade_{DATE_ISO}T00--part=1.parquet"
    write_parquet_with_manifest(
        v2_path,
        table.slice(0, 5).drop(["trade_time"]),
    )
    write_parquet_with_manifest(original, table.slice(5))


class GammaHandler(BaseHTTPRequestHandler):
    requests = []
    lock = threading.Lock()

    def do_GET(self):
        with self.lock:
            self.requests.append(self.path)
        slug = self.path.rsplit("/", 1)[-1]
        if slug == OBSERVED_SLUG:
            body = {
                "id": "event-1",
                "closed": False,
                "active": True,
                "eventMetadata": {
                    "priceToBeat": 60180.70970854604,
                    "finalPrice": 60181.00000000001,
                    "ignoredDiagnosticField": "does-not-gate",
                },
            }
        elif slug == f"btc-updown-5m-{DAY_START + 300}":
            body = {"eventMetadata": {"priceToBeat": 1.25}}
        else:
            body = {}
        payload = json.dumps(body).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *_args):
        return


def write_fake_aliyunpan(bin_dir, remote_dir):
    cli = bin_dir / "aliyunpan"
    cli.write_text(
        """#!/usr/bin/env python3
import hashlib
import os
import pathlib
import shutil
import sys

remote = pathlib.Path(os.environ["FAKE_ALIYUNPAN_DIR"])
remote.mkdir(parents=True, exist_ok=True)
command = sys.argv[1]
args = sys.argv[2:]

if command == "upload":
    overwrite = "--ow" in args
    positional = [arg for arg in args if not arg.startswith("-")]
    source = pathlib.Path(positional[-2])
    target = remote / source.name
    if target.exists() and not overwrite:
        raise SystemExit(1)
    shutil.copy2(source, target)
elif command == "ls":
    for path in sorted(remote.iterdir()):
        print(path.name)
elif command == "ll":
    for index, path in enumerate(sorted(remote.iterdir()), 1):
        digest = hashlib.sha1(path.read_bytes()).hexdigest().upper()
        print(index, "file-id", path.stat().st_size, digest, path.stat().st_size,
              "2026-07-10", "00:00:00", "2026-07-10", "00:00:00", path.name)
else:
    raise SystemExit(2)
"""
    )
    cli.chmod(0o755)


def run_archive_with_gamma(local_root, env_overrides=None):
    GammaHandler.requests = []
    server = ThreadingHTTPServer(("127.0.0.1", 0), GammaHandler)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    try:
        env = os.environ.copy()
        env.update(
            {
                "DATA_COLLECT_ARCHIVE_HK_PROXY": "0",
                "DATA_COLLECT_ARCHIVE_SKIP_PULL": "1",
                "DATA_COLLECT_NEW_DATASETS_ROOT": str(local_root),
                "DATA_COLLECT_ARCHIVE_GAMMA_BASE_URL": (
                    f"http://127.0.0.1:{server.server_port}/events/slug"
                ),
                "DATA_COLLECT_ARCHIVE_GAMMA_WORKERS": "4",
                "DATA_COLLECT_ARCHIVE_GAMMA_TIMEOUT_SECONDS": "2",
                "DATA_COLLECT_ARCHIVE_THREADS": "1",
            }
        )
        env.update(env_overrides or {})
        result = subprocess.run(
            [str(SCRIPT), DATE_ISO],
            cwd=REPO_ROOT,
            env=env,
            text=True,
            capture_output=True,
            timeout=60,
        )
        requests = list(GammaHandler.requests)
    finally:
        server.shutdown()
        server.server_close()
        server_thread.join(timeout=5)
    return result, requests


class DailyArchiveContractTest(unittest.TestCase):
    def test_timer_runs_at_utc_0200(self):
        timer = TIMER.read_text()
        script = SCRIPT.read_text()
        self.assertIn("OnCalendar=*-*-* 02:00:00 UTC", timer)
        self.assertIn("at 02:00 UTC", timer)
        self.assertIn("date -u -d 'yesterday' +%F", script)

    def test_daily_archive_fetches_once_and_overwrites_stale_remote_content(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            local_root = root / "new-datasets"
            partial = build_day_fixture(local_root)
            make_aggtrade_day_mixed_v2_v3(local_root)
            fake_bin = root / "bin"
            fake_bin.mkdir()
            remote_dir = root / "aliyunpan"
            remote_dir.mkdir()
            write_fake_aliyunpan(fake_bin, remote_dir)

            archive_name = f"new-datasets-{DATE_ISO}.tar.gz"
            (remote_dir / archive_name).write_bytes(b"stale-archive")
            (remote_dir / f"{archive_name}.sha256").write_bytes(b"stale-checksum")

            result, requests = run_archive_with_gamma(
                local_root,
                {
                    "PATH": f"{fake_bin}:{os.environ['PATH']}",
                    "FAKE_ALIYUNPAN_DIR": str(remote_dir),
                    "DATA_COLLECT_ARCHIVE_UPLOAD_VERIFY_ATTEMPTS": "1",
                },
            )

            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            request_counts = collections.Counter(requests)
            self.assertEqual(sum(request_counts.values()), 288)
            self.assertEqual(len(request_counts), 288)
            self.assertTrue(all(count == 1 for count in request_counts.values()))
            requested_epochs = sorted(int(path.rsplit("-", 1)[-1]) for path in request_counts)
            self.assertEqual(requested_epochs[0], DAY_START)
            self.assertEqual(requested_epochs[-1], DAY_START + (287 * 300))

            report_path = local_root / DATE_ISO / "validation_report.json"
            report = json.loads(report_path.read_text())
            self.assertEqual(report["accepted_market_count"], 1)
            self.assertEqual(report["rejected_market_count"], 0)
            self.assertIn(str(partial.relative_to(local_root / DATE_ISO)), report["partial_artifacts"])
            self.assertEqual(report["price_to_beat"]["target_market_count"], 288)
            self.assertEqual(report["price_to_beat"]["gamma_http_request_count"], 288)
            self.assertFalse(report["price_to_beat"]["artifact_reused"])
            self.assertEqual(report["price_to_beat"]["successful_market_count"], 1)

            p2b_path = local_root / DATE_ISO / "price_to_beat.parquet"
            p2b = pq.read_table(p2b_path)
            self.assertEqual(p2b.num_rows, 288)
            observed = [row for row in p2b.to_pylist() if row["slug"] == OBSERVED_SLUG][0]
            self.assertTrue(observed["observed"])
            self.assertTrue(observed["fetch_succeeded"])
            self.assertEqual(
                observed["polymarket_price_to_beat"], "60180.70970854604"
            )
            self.assertEqual(
                observed["polymarket_final_price"], "60181.00000000001"
            )
            incomplete = [
                row
                for row in p2b.to_pylist()
                if row["slug"] == f"btc-updown-5m-{DAY_START + 300}"
            ][0]
            self.assertFalse(incomplete["fetch_succeeded"])
            self.assertEqual(incomplete["diagnostic_error"], "p2b_final_missing")

            archive_path = local_root / "archives" / archive_name
            with tarfile.open(archive_path, "r:gz") as archive:
                self.assertIn("price_to_beat.parquet", archive.getnames())

            selected_aggtrade = pq.read_table(
                local_root
                / DATE_ISO
                / "validated_markets"
                / OBSERVED_SLUG
                / "binance_aggtrade_selected_window.parquet"
            )
            self.assertIn("event_time", selected_aggtrade.column_names)
            self.assertIn("trade_time", selected_aggtrade.column_names)
            trade_times = selected_aggtrade["trade_time"].to_pylist()
            self.assertTrue(any(value is None for value in trade_times))
            self.assertTrue(any(value is not None for value in trade_times))
            v3_index = next(
                index for index, value in enumerate(trade_times) if value is not None
            )
            self.assertNotEqual(
                selected_aggtrade["event_time"][v3_index].as_py(),
                selected_aggtrade["trade_time"][v3_index].as_py(),
            )

            remote_archive = remote_dir / archive_name
            self.assertEqual(
                hashlib.sha1(remote_archive.read_bytes()).hexdigest(),
                hashlib.sha1(archive_path.read_bytes()).hexdigest(),
            )
            self.assertEqual(
                (remote_dir / f"{archive_name}.sha256").read_bytes(),
                (local_root / "archives" / f"{archive_name}.sha256").read_bytes(),
            )

            p2b_sha256_before = hashlib.sha256(p2b_path.read_bytes()).hexdigest()
            rerun, rerun_requests = run_archive_with_gamma(
                local_root,
                {
                    "PATH": f"{fake_bin}:{os.environ['PATH']}",
                    "FAKE_ALIYUNPAN_DIR": str(remote_dir),
                    "DATA_COLLECT_ARCHIVE_UPLOAD_VERIFY_ATTEMPTS": "1",
                },
            )
            self.assertEqual(rerun.returncode, 0, rerun.stdout + rerun.stderr)
            self.assertEqual(rerun_requests, [])
            self.assertEqual(
                hashlib.sha256(p2b_path.read_bytes()).hexdigest(),
                p2b_sha256_before,
            )
            rerun_report = json.loads(
                (local_root / DATE_ISO / "validation_report.json").read_text()
            )
            self.assertEqual(
                rerun_report["price_to_beat"]["gamma_http_request_count"], 0
            )
            self.assertTrue(rerun_report["price_to_beat"]["artifact_reused"])

    def test_slug_epoch_is_the_market_boundary_and_metadata_mismatch_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            local_root = pathlib.Path(temporary) / "new-datasets"
            build_day_fixture(local_root, metadata_start_offset=30)
            result, requests = run_archive_with_gamma(
                local_root,
                {"DATA_COLLECT_ARCHIVE_SKIP_UPLOAD": "1"},
            )

            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(len(requests), 288)
            report = json.loads(
                (local_root / DATE_ISO / "validation_report.json").read_text()
            )
            self.assertEqual(report["accepted_market_count"], 0)
            self.assertEqual(report["rejected_market_count"], 1)
            rejected = report["rejected_markets"][0]
            self.assertEqual(rejected["start_utc_ms"], DAY_START * 1000)
            self.assertTrue(
                any("market_start_mismatch" in reason for reason in rejected["reasons"])
            )

    def test_off_grid_observation_is_rejected_without_expanding_gamma_requests(self):
        with tempfile.TemporaryDirectory() as temporary:
            local_root = pathlib.Path(temporary) / "new-datasets"
            build_day_fixture(local_root)
            off_grid_slug = append_off_grid_market(local_root)
            result, requests = run_archive_with_gamma(
                local_root,
                {"DATA_COLLECT_ARCHIVE_SKIP_UPLOAD": "1"},
            )

            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertEqual(len(requests), 288)
            self.assertNotIn(f"/events/slug/{off_grid_slug}", requests)
            report = json.loads(
                (local_root / DATE_ISO / "validation_report.json").read_text()
            )
            self.assertEqual(report["market_count"], 2)
            self.assertEqual(report["accepted_market_count"], 1)
            self.assertEqual(report["rejected_market_count"], 1)
            self.assertEqual(report["off_grid_observed_markets"], [off_grid_slug])
            self.assertIn(
                "market_identity:slug_not_on_utc_5m_grid",
                report["rejected_markets"][0]["reasons"],
            )


if __name__ == "__main__":
    unittest.main()
