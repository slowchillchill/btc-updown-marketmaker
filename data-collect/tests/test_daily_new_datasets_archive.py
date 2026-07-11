import collections
import datetime as dt
import hashlib
import json
import os
import pathlib
import socket
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
OBSERVED_START = DAY_START + 900
OBSERVED_SLUG = f"btc-updown-5m-{OBSERVED_START}"
TRANSIENT_SLUG = f"btc-updown-5m-{DAY_START + 600}"


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
    event_seconds = [OBSERVED_START + offset for offset in offsets]
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
            "market_start_utc": utc_timestamp_array(
                [OBSERVED_START + metadata_start_offset]
            ),
            "market_end_utc": utc_timestamp_array(
                [OBSERVED_START + metadata_end_offset]
            ),
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
    snapshot = pa.table(
        {
            "timestamp_received": utc_timestamp_array(
                [OBSERVED_START - 2, OBSERVED_START + 45]
            ),
            "snapshot_fetch_start_utc": utc_timestamp_array(
                [OBSERVED_START - 3, OBSERVED_START + 44]
            ),
            "last_update_id": pa.array([100, 200], type=pa.uint64()),
            "bids": pa.array(
                ['[["100.00","1.00"]]', '[["99.00","3.00"]]']
            ),
            "asks": pa.array(
                ['[["101.00","1.00"]]', '[["102.00","4.00"]]']
            ),
            "symbol": pa.array(["BTCUSDT", "BTCUSDT"]),
            "source_endpoint": pa.array(["https://api.binance.test/depth"] * 2),
            "snapshot_status": pa.array(["complete", "complete"]),
            "collector_host_id": pa.array(["fixture-host"] * 2),
            "collector_region": pa.array(["fixture-region"] * 2),
            "schema_version": pa.array(["binance_depth_snapshot_v2"] * 2),
        }
    )
    update_offsets = [-1, *offsets]
    update_ids = [101, 102, 103, 201, 202, 203, 204, 205, 206, 207, 208]
    local_u_before = [None, 101, 102, None, 201, 202, 203, 204, 205, 206, 207]
    snapshot_ids = [100, 100, 100, 200, 200, 200, 200, 200, 200, 200, 200]
    depth_update = pa.table(
        {
            "timestamp_received": utc_timestamp_array(
                [OBSERVED_START + offset for offset in update_offsets]
            ),
            "event_type": pa.array(["depthUpdate"] * len(update_offsets)),
            "event_time": utc_timestamp_array(
                [OBSERVED_START + offset for offset in update_offsets]
            ),
            "symbol": pa.array(["BTCUSDT"] * len(update_offsets)),
            "first_update_id": pa.array(update_ids, type=pa.uint64()),
            "final_update_id": pa.array(update_ids, type=pa.uint64()),
            "bids": pa.array(['[["100.00","2.00"]]'] * len(update_offsets)),
            "asks": pa.array(
                ['[["101.00","0.00"]]', *(["[]"] * (len(update_offsets) - 1))]
            ),
            "stream": pa.array(["btcusdt@depth"] * len(update_offsets)),
            "snapshot_last_update_id": pa.array(snapshot_ids, type=pa.uint64()),
            "local_u_before": pa.array(local_u_before, type=pa.uint64()),
            "local_u_after": pa.array(update_ids, type=pa.uint64()),
            "stitch_verdict": pa.array(
                [
                    "first_update_bridged_snapshot",
                    "continuous_depth_update",
                    "continuous_depth_update",
                    "first_update_bridged_snapshot",
                    *(["continuous_depth_update"] * 7),
                ]
            ),
            "stitch_gap_reason": pa.array([None] * len(update_offsets), type=pa.string()),
            "collector_host_id": pa.array(["fixture-host"] * len(update_offsets)),
            "collector_region": pa.array(["fixture-region"] * len(update_offsets)),
            "source_endpoint": pa.array(
                ["wss://stream.binance.test/btcusdt@depth"] * len(update_offsets)
            ),
            "schema_version": pa.array(["binance_depth_update_v2"] * len(update_offsets)),
        }
    )

    tables = {
        "polymarket_market": polymarket,
        "polymarket_market_metadata": metadata,
        "binance_depth_snapshot": snapshot,
        "binance_depth_update": depth_update,
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
    transient_failures_before_success = 1

    def do_GET(self):
        with self.lock:
            self.requests.append(self.path)
            request_attempt = self.requests.count(self.path)
        slug = self.path.rsplit("/", 1)[-1]
        if (
            slug == TRANSIENT_SLUG
            and request_attempt <= self.transient_failures_before_success
        ):
            self.close_connection = True
            try:
                self.connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            self.connection.close()
            return
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
        elif slug == TRANSIENT_SLUG:
            body = {
                "eventMetadata": {
                    "priceToBeat": 60182.25,
                    "finalPrice": 60182.50,
                }
            }
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


def run_archive_with_gamma(
    local_root,
    env_overrides=None,
    transient_failures_before_success=1,
):
    GammaHandler.requests = []
    GammaHandler.transient_failures_before_success = transient_failures_before_success
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
                "DATA_COLLECT_ARCHIVE_GAMMA_TRANSIENT_MAX_ATTEMPTS": "3",
                "DATA_COLLECT_ARCHIVE_GAMMA_TRANSIENT_RETRY_DELAY_SECONDS": "0",
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
            self.assertEqual(sum(request_counts.values()), 289)
            self.assertEqual(len(request_counts), 288)
            self.assertEqual(
                request_counts[f"/events/slug/{TRANSIENT_SLUG}"], 2
            )
            self.assertTrue(
                all(
                    count == 1
                    for path, count in request_counts.items()
                    if path != f"/events/slug/{TRANSIENT_SLUG}"
                )
            )
            requested_epochs = sorted(int(path.rsplit("-", 1)[-1]) for path in request_counts)
            self.assertEqual(requested_epochs[0], DAY_START)
            self.assertEqual(requested_epochs[-1], DAY_START + (287 * 300))

            report_path = local_root / DATE_ISO / "validation_report.json"
            report = json.loads(report_path.read_text())
            self.assertEqual(report["accepted_market_count"], 1)
            self.assertEqual(report["rejected_market_count"], 0)
            self.assertIn(str(partial.relative_to(local_root / DATE_ISO)), report["partial_artifacts"])
            self.assertEqual(report["price_to_beat"]["target_market_count"], 288)
            self.assertEqual(report["price_to_beat"]["gamma_http_request_count"], 289)
            self.assertEqual(report["price_to_beat"]["transient_retry_target_count"], 1)
            self.assertFalse(report["price_to_beat"]["artifact_reused"])
            self.assertEqual(report["price_to_beat"]["successful_market_count"], 2)

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
            transient = [
                row for row in p2b.to_pylist() if row["slug"] == TRANSIENT_SLUG
            ][0]
            self.assertTrue(transient["fetch_succeeded"])
            self.assertEqual(transient["gamma_http_attempt_count"], 2)

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

            market_dir = (
                local_root / DATE_ISO / "validated_markets" / OBSERVED_SLUG
            )
            checkpoint = pq.read_table(
                market_dir / "binance_depth_snapshot_bootstrap.parquet"
            )
            selected_depth = pq.read_table(
                market_dir / "binance_depth_update_selected_window.parquet"
            ).sort_by([("timestamp_received", "ascending")])
            self.assertEqual(checkpoint.num_rows, 1)
            self.assertIn("timestamp_received", checkpoint.column_names)
            self.assertNotIn("snapshot_fetch_start_utc", checkpoint.column_names)
            checkpoint_id = checkpoint["last_update_id"][0].as_py()
            self.assertEqual(
                checkpoint["timestamp_received"][0].as_py(),
                dt.datetime.fromtimestamp(OBSERVED_START - 1, tz=dt.timezone.utc),
            )
            self.assertEqual(
                checkpoint["snapshot_status"][0].as_py(),
                "derived_market_start_checkpoint",
            )
            self.assertEqual(
                checkpoint["schema_version"][0].as_py(),
                "binance_depth_market_checkpoint_v1",
            )
            self.assertEqual(
                json.loads(checkpoint["bids"][0].as_py()),
                [["100.00", "2.00"]],
            )
            self.assertEqual(json.loads(checkpoint["asks"][0].as_py()), [])
            first_update = selected_depth.slice(0, 1).to_pylist()[0]
            self.assertEqual(checkpoint_id, 101)
            self.assertEqual(checkpoint_id, first_update["local_u_before"])
            self.assertLessEqual(first_update["first_update_id"], checkpoint_id + 1)
            self.assertLessEqual(checkpoint_id + 1, first_update["final_update_id"])

            resync_snapshots = pq.read_table(
                market_dir / "binance_depth_snapshot_resync_window.parquet"
            )
            self.assertEqual(resync_snapshots.num_rows, 1)
            self.assertIn("timestamp_received", resync_snapshots.column_names)
            self.assertNotIn(
                "snapshot_fetch_start_utc", resync_snapshots.column_names
            )
            self.assertEqual(resync_snapshots["last_update_id"][0].as_py(), 200)
            first_after_resync = next(
                row
                for row in selected_depth.to_pylist()
                if row["snapshot_last_update_id"] == 200
            )
            self.assertIsNone(first_after_resync["local_u_before"])
            self.assertLessEqual(first_after_resync["first_update_id"], 201)
            self.assertLessEqual(201, first_after_resync["final_update_id"])

            market_window = json.loads(
                (market_dir / "market_window.json").read_text()
            )
            reconstruction = market_window["binance_depth_reconstruction"]
            self.assertEqual(reconstruction["status"], "valid")
            self.assertEqual(reconstruction["checkpoint_last_update_id"], 101)
            self.assertEqual(reconstruction["in_window_resnapshot_count"], 1)
            self.assertEqual(
                reconstruction["checkpoint_to_first_update_receive_gap_ms"], 1000
            )

            raw_snapshot = pq.read_table(
                local_root
                / DATE_ISO
                / "typed_raw"
                / f"utc_day={DATE_ISO}"
                / "source_kind=binance_depth_snapshot"
                / f"binance_depth_snapshot_{DATE_ISO}T00.parquet"
            )
            self.assertIn("snapshot_fetch_start_utc", raw_snapshot.column_names)

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

    def test_existing_artifact_retries_only_transient_transport_failures(self):
        with tempfile.TemporaryDirectory() as temporary:
            local_root = pathlib.Path(temporary) / "new-datasets"
            build_day_fixture(local_root)

            first, first_requests = run_archive_with_gamma(
                local_root,
                {
                    "DATA_COLLECT_ARCHIVE_SKIP_UPLOAD": "1",
                    "DATA_COLLECT_ARCHIVE_GAMMA_TRANSIENT_MAX_ATTEMPTS": "1",
                },
            )
            self.assertEqual(first.returncode, 0, first.stdout + first.stderr)
            self.assertEqual(len(first_requests), 288)

            p2b_path = local_root / DATE_ISO / "price_to_beat.parquet"
            first_rows = {row["slug"]: row for row in pq.read_table(p2b_path).to_pylist()}
            self.assertFalse(first_rows[TRANSIENT_SLUG]["fetch_succeeded"])
            self.assertTrue(
                first_rows[TRANSIENT_SLUG]["diagnostic_error"].startswith(
                    "transient_transport_error:"
                )
            )
            self.assertEqual(
                first_rows[f"btc-updown-5m-{DAY_START + 300}"]["diagnostic_error"],
                "p2b_final_missing",
            )

            legacy_rows = list(first_rows.values())
            for row in legacy_rows:
                row.pop("gamma_http_attempt_count", None)
                row["schema_version"] = "price-to-beat-gamma-v1"
            legacy_rows[
                next(
                    index
                    for index, row in enumerate(legacy_rows)
                    if row["slug"] == TRANSIENT_SLUG
                )
            ]["diagnostic_error"] = (
                "request_error:TimeoutError:<urlopen error _ssl.c:1063: "
                "The handshake operation timed out>"
            )
            pq.write_table(pa.Table.from_pylist(legacy_rows), p2b_path)

            second, second_requests = run_archive_with_gamma(
                local_root,
                {"DATA_COLLECT_ARCHIVE_SKIP_UPLOAD": "1"},
            )
            self.assertEqual(second.returncode, 0, second.stdout + second.stderr)
            self.assertEqual(
                second_requests,
                [
                    f"/events/slug/{TRANSIENT_SLUG}",
                    f"/events/slug/{TRANSIENT_SLUG}",
                ],
            )
            migrated = pq.read_table(p2b_path)
            self.assertEqual(migrated.num_rows, 288)
            self.assertIn("gamma_http_attempt_count", migrated.column_names)
            self.assertEqual(migrated["gamma_http_attempt_count"].null_count, 0)
            second_rows = {row["slug"]: row for row in migrated.to_pylist()}
            self.assertEqual(
                {row["schema_version"] for row in second_rows.values()},
                {"price-to-beat-gamma-v2"},
            )
            self.assertTrue(second_rows[TRANSIENT_SLUG]["fetch_succeeded"])
            self.assertEqual(second_rows[TRANSIENT_SLUG]["gamma_http_attempt_count"], 2)
            self.assertFalse(
                second_rows[f"btc-updown-5m-{DAY_START + 300}"]["fetch_succeeded"]
            )
            for slug, first_row in first_rows.items():
                if slug == TRANSIENT_SLUG:
                    continue
                for field in (
                    "fetch_succeeded",
                    "polymarket_price_to_beat",
                    "polymarket_final_price",
                    "diagnostic_error",
                ):
                    self.assertEqual(second_rows[slug][field], first_row[field])
            report = json.loads(
                (local_root / DATE_ISO / "validation_report.json").read_text()
            )
            self.assertTrue(report["price_to_beat"]["artifact_reused"])
            self.assertEqual(report["price_to_beat"]["gamma_http_request_count"], 2)
            self.assertEqual(report["price_to_beat"]["transient_retry_target_count"], 1)

    def test_transient_attempt_override_is_clamped_to_three(self):
        with tempfile.TemporaryDirectory() as temporary:
            local_root = pathlib.Path(temporary) / "new-datasets"
            build_day_fixture(local_root)
            result, requests = run_archive_with_gamma(
                local_root,
                {
                    "DATA_COLLECT_ARCHIVE_SKIP_UPLOAD": "1",
                    "DATA_COLLECT_ARCHIVE_GAMMA_TRANSIENT_MAX_ATTEMPTS": "100",
                },
                transient_failures_before_success=3,
            )

            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertEqual(requests.count(f"/events/slug/{TRANSIENT_SLUG}"), 3)
            rows = {
                row["slug"]: row
                for row in pq.read_table(
                    local_root / DATE_ISO / "price_to_beat.parquet"
                ).to_pylist()
            }
            self.assertFalse(rows[TRANSIENT_SLUG]["fetch_succeeded"])
            self.assertEqual(rows[TRANSIENT_SLUG]["gamma_http_attempt_count"], 3)
            self.assertTrue(
                rows[TRANSIENT_SLUG]["diagnostic_error"].startswith(
                    "transient_transport_error:"
                )
            )

    def test_slug_epoch_is_the_market_boundary_and_metadata_mismatch_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            local_root = pathlib.Path(temporary) / "new-datasets"
            build_day_fixture(local_root, metadata_start_offset=30)
            result, requests = run_archive_with_gamma(
                local_root,
                {"DATA_COLLECT_ARCHIVE_SKIP_UPLOAD": "1"},
            )

            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(len(set(requests)), 288)
            self.assertEqual(requests.count(f"/events/slug/{TRANSIENT_SLUG}"), 2)
            report = json.loads(
                (local_root / DATE_ISO / "validation_report.json").read_text()
            )
            self.assertEqual(report["accepted_market_count"], 0)
            self.assertEqual(report["rejected_market_count"], 1)
            rejected = report["rejected_markets"][0]
            self.assertEqual(rejected["start_utc_ms"], OBSERVED_START * 1000)
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
            self.assertEqual(len(set(requests)), 288)
            self.assertEqual(requests.count(f"/events/slug/{TRANSIENT_SLUG}"), 2)
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

    def test_depth_chain_rejects_missing_in_window_resync_snapshot(self):
        with tempfile.TemporaryDirectory() as temporary:
            local_root = pathlib.Path(temporary) / "new-datasets"
            build_day_fixture(local_root)
            snapshot_path = (
                local_root
                / DATE_ISO
                / "typed_raw"
                / f"utc_day={DATE_ISO}"
                / "source_kind=binance_depth_snapshot"
                / f"binance_depth_snapshot_{DATE_ISO}T00.parquet"
            )
            snapshots = pq.ParquetFile(snapshot_path).read()
            write_parquet_with_manifest(snapshot_path, snapshots.slice(0, 1))

            result, _requests = run_archive_with_gamma(
                local_root,
                {"DATA_COLLECT_ARCHIVE_SKIP_UPLOAD": "1"},
            )

            self.assertNotEqual(result.returncode, 0)
            report = json.loads(
                (local_root / DATE_ISO / "validation_report.json").read_text()
            )
            self.assertEqual(report["accepted_market_count"], 0)
            self.assertTrue(
                any(
                    reason.startswith(
                        "binance_depth_reconstruction:missing_snapshot_anchor"
                    )
                    for reason in report["rejected_markets"][0]["reasons"]
                )
            )

    def test_depth_chain_rejects_checkpoint_to_first_update_id_mismatch(self):
        with tempfile.TemporaryDirectory() as temporary:
            local_root = pathlib.Path(temporary) / "new-datasets"
            build_day_fixture(local_root)
            update_path = (
                local_root
                / DATE_ISO
                / "typed_raw"
                / f"utc_day={DATE_ISO}"
                / "source_kind=binance_depth_update"
                / f"binance_depth_update_{DATE_ISO}T00.parquet"
            )
            updates = pq.ParquetFile(update_path).read()
            rows = updates.to_pylist()
            rows[1]["local_u_before"] = 999
            write_parquet_with_manifest(
                update_path,
                pa.Table.from_pylist(rows, schema=updates.schema),
            )

            result, _requests = run_archive_with_gamma(
                local_root,
                {"DATA_COLLECT_ARCHIVE_SKIP_UPLOAD": "1"},
            )

            self.assertNotEqual(result.returncode, 0)
            report = json.loads(
                (local_root / DATE_ISO / "validation_report.json").read_text()
            )
            self.assertEqual(report["accepted_market_count"], 0)
            self.assertTrue(
                any(
                    reason.startswith(
                        "binance_depth_reconstruction:local_u_before_mismatch"
                    )
                    for reason in report["rejected_markets"][0]["reasons"]
                )
            )

    def test_depth_chain_rejects_malformed_in_window_resync_snapshot(self):
        with tempfile.TemporaryDirectory() as temporary:
            local_root = pathlib.Path(temporary) / "new-datasets"
            build_day_fixture(local_root)
            snapshot_path = (
                local_root
                / DATE_ISO
                / "typed_raw"
                / f"utc_day={DATE_ISO}"
                / "source_kind=binance_depth_snapshot"
                / f"binance_depth_snapshot_{DATE_ISO}T00.parquet"
            )
            snapshots = pq.ParquetFile(snapshot_path).read()
            rows = snapshots.to_pylist()
            rows[1]["bids"] = "not-json"
            write_parquet_with_manifest(
                snapshot_path,
                pa.Table.from_pylist(rows, schema=snapshots.schema),
            )

            result, _requests = run_archive_with_gamma(
                local_root,
                {"DATA_COLLECT_ARCHIVE_SKIP_UPLOAD": "1"},
            )

            self.assertNotEqual(result.returncode, 0)
            report = json.loads(
                (local_root / DATE_ISO / "validation_report.json").read_text()
            )
            self.assertEqual(report["accepted_market_count"], 0)
            self.assertTrue(
                any(
                    reason.startswith(
                        "binance_depth_reconstruction:invalid_resnapshot"
                    )
                    for reason in report["rejected_markets"][0]["reasons"]
                )
            )


if __name__ == "__main__":
    unittest.main()
