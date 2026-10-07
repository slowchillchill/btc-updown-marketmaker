"""Necessary-column projection of the admitted collector schemas.

DuckDB reads/expands/windows native columns in vectors. Python never iterates PM
messages. Wide decoding is restricted to duplicate identity candidates and the
original small explicit metadata initialization, through the existing Rust reader.
"""
from __future__ import annotations
import hashlib
import json
import subprocess
from pathlib import Path


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def ms(column):
    return f"epoch_ms({column})"


# Names are actual collector 8e6e9d8 schema columns, not JSON payload paths.
COLUMNS = {
    "polymarket_market": "timestamp_received,timestamp,event_type,market,asset_id,list_distinct(list_transform(price_changes,x->x.asset_id)) AS changed_assets,source_sequence_epoch_id,collector_admission_sequence,collector_admission_time_utc_ms",
    "binance_depth_snapshot": "timestamp_received,last_update_id,snapshot_status,source_sequence_epoch_id,connection_generation,depth_ingress_sequence,collector_admission_sequence,collector_admission_time_utc_ms",
    "binance_depth_update": "timestamp_received,event_time,first_update_id,final_update_id,snapshot_last_update_id,local_u_before,stitch_verdict,source_sequence_epoch_id,connection_generation,depth_ingress_sequence,collector_admission_sequence,collector_admission_time_utc_ms",
    "binance_aggtrade": "timestamp_received,trade_time,record_kind,aggregate_trade_id,source_sequence_epoch_id,connection_generation,collector_admission_sequence,collector_admission_time_utc_ms",
    "polymarket_rtds_chainlink_btc_usd": "timestamp_received,payload_timestamp,freshness_status,source_native_payload_json",
    "polymarket_rtds_chainlink_twap": "timestamp_received,payload_timestamp,freshness_status,window_s,source_sequence_epoch_id,collector_admission_sequence,collector_admission_time_utc_ms",
    "polymarket_market_metadata": "timestamp_received,metadata_row_kind,metadata_origin,condition_id,asset_ids,market_start_utc,market_end_utc,collector_admission_sequence,collector_admission_time_utc_ms,anchor_query_result,twap_enabled,twap_lookback_seconds",
}


def boundary_identities(db, helper, files, predicate_table, directory, business=False):
    """Read only duplicate candidate row groups, then use the real typed reader.

    Candidate coordinates come from necessary columns. Arrow preserves full
    schema/values here. Canonical comparison uses public Rust normalization, not
    a byte hash of price spelling or JSON object ordering. Work is bounded by
    actual duplicates, never by all unique PM messages.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq
    directory.mkdir(exist_ok=False)
    identities = []
    for file_id, in db.execute(f"SELECT DISTINCT file_id FROM {predicate_table} ORDER BY file_id").fetchall():
        coordinates = [r[0] for r in db.execute(f"SELECT row_id FROM {predicate_table} WHERE file_id=? ORDER BY row_id", [file_id]).fetchall()]
        file = files[file_id]
        parquet = pq.ParquetFile(file["path"])
        batches, consumed, cursor = [], 0, 0
        for group in range(parquet.num_row_groups):
            rows = parquet.metadata.row_group(group).num_rows
            selected = []
            while cursor < len(coordinates) and coordinates[cursor] < consumed + rows:
                selected.append(coordinates[cursor] - consumed)
                cursor += 1
            if selected:
                batches.append(parquet.read_row_group(group).take(pa.array(selected, pa.int64())))
            consumed += rows
        subset = directory / f"duplicate-{file_id}.parquet"
        pq.write_table(pa.concat_tables(batches), subset, compression="zstd",use_compliant_nested_type=False)
        facts = {k: file[k] for k in ("source_kind", "schema_version")}
        facts.update(path=str(subset), bytes=subset.stat().st_size, rows=len(coordinates), sha256=hashlib.sha256(subset.read_bytes()).hexdigest())
        descriptor = subset.with_suffix(".json")
        descriptor.write_text(json.dumps(facts))
        output = subset.with_suffix(".identities.jsonl")
        subprocess.run([str(helper), "identities", str(descriptor), str(output)], check=True)
        for coordinate, line in zip(coordinates, output.read_text().splitlines(), strict=True):
            value = json.loads(line)
            identity = json.dumps(value["business"], sort_keys=True) if business else value["identity"]
            identities.append((file_id, coordinate, identity))
    db.execute("CREATE TEMP TABLE duplicate_identity(file_id BIGINT,row_id BIGINT,identity VARCHAR)")
    if identities:
        db.executemany("INSERT INTO duplicate_identity VALUES (?,?,?)", identities)


def deduplicate(db, table, keys, helper, files, directory, business=False):
    key_sql = ",".join(keys)
    db.execute(f"CREATE TEMP TABLE duplicate_coordinates AS SELECT * FROM {table} QUALIFY count(*) OVER (PARTITION BY {key_sql})>1")
    duplicates = db.execute("SELECT count(*) FROM duplicate_coordinates").fetchone()[0]
    if duplicates:
        boundary_identities(db, helper, files, "duplicate_coordinates", directory, business)
        conflicts = db.execute(f"SELECT count(*) FROM (SELECT {key_sql} FROM duplicate_coordinates JOIN duplicate_identity USING(file_id,row_id) GROUP BY {key_sql} HAVING count(DISTINCT {('(identity,trade_time)' if business else 'identity')})>1)").fetchone()[0]
        if conflicts:
            raise ValueError(f"conflicting native/admission identity in {table}")
        db.execute("DROP TABLE duplicate_identity")
    db.execute("DROP TABLE duplicate_coordinates")
    if not duplicates:
        return table
    first_order='source_order' if business else 'file_id,row_id'
    db.execute(f"CREATE TABLE unique_{table} AS SELECT * FROM {table} QUALIFY row_number() OVER (PARTITION BY {key_sql} ORDER BY {first_order})=1")
    return "unique_" + table


def projected_markets(db, initialization, rules):
    """Identity and rules have their own causal visibility; no future backfill."""
    db.execute("CREATE TABLE initial_metadata(condition_id VARCHAR,assets VARCHAR[],window_start BIGINT,window_end BIGINT,received BIGINT,admitted BIGINT,sequence UBIGINT)")
    rows = [json.loads(line) for line in initialization.read_text().splitlines()]
    if rows:
        db.executemany("INSERT INTO initial_metadata VALUES (?,?,?,?,?,?,?)", [(r["condition_id"],r["assets"],r["window_start"],r["window_end"],r["received"],r["admitted"],r["sequence"]) for r in rows])
    db.execute("""CREATE TABLE identities AS
        SELECT condition_id, list_sort(json_extract_string(asset_ids,'$[*]')) AS assets,
               epoch_ms(market_start_utc) AS window_start,epoch_ms(market_end_utc) AS window_end,
               greatest(epoch_ms(timestamp_received),epoch_ms(collector_admission_time_utc_ms)) AS visible_at,
               epoch_ms(timestamp_received) AS dependency_received
        FROM raw_polymarket_market_metadata
        UNION ALL SELECT condition_id,list_sort(assets),window_start,window_end,greatest(received,admitted),received FROM initial_metadata""")
    # Every admitted metadata kind carries the verified fixed identity. A timer
    # transition or anchor may be its first in-range record. Observe that row at
    # its receive/admission time; never backfill to its earlier verification time.
    conflicts = db.execute("SELECT count(*) FROM (SELECT condition_id FROM identities GROUP BY condition_id HAVING count(DISTINCT (assets,window_start,window_end))>1)").fetchone()[0]
    if conflicts:
        raise ValueError("conflicting admitted fixed market identity")
    db.execute("""CREATE TABLE markets AS SELECT condition_id,any_value(assets) AS assets,
        any_value(window_start) AS window_start,any_value(window_end) AS window_end,
        min(visible_at) AS known_at,min_by(dependency_received,visible_at) AS dependency_received
        FROM identities GROUP BY condition_id""")
    db.execute("CREATE TABLE rule_map(enabled BOOLEAN,lookback INTEGER,reference INTEGER)")
    db.executemany("INSERT INTO rule_map VALUES (?,?,?)", [(r["enabled"],r["window"],r["reference"]) for r in rules])
    db.execute("""CREATE TABLE rules AS SELECT condition_id,
        greatest(epoch_ms(timestamp_received),epoch_ms(collector_admission_time_utc_ms)) AS visible_at,
        collector_admission_sequence AS sequence,reference
        FROM raw_polymarket_market_metadata JOIN rule_map ON twap_enabled=enabled
        AND (CASE WHEN twap_enabled=false THEN 0 ELSE twap_lookback_seconds END)=lookback
        WHERE metadata_origin='gamma_event_by_slug' AND anchor_query_result='observed'""")
