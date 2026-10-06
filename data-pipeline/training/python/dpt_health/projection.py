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


def read_columns(db, files, progress):
    """One projected pass, no raw-row hashes or wide JSON rewrites."""
    kinds = {}
    for ordinal, file in enumerate(files):
        kind = file["source_kind"]
        if kind not in COLUMNS:
            raise ValueError(f"unsupported admitted source: {kind}")
        path = Path(file["path"])
        if path.stat().st_size != file["bytes"]:
            raise ValueError(f"file size changed: {path.name}")
        query = f"SELECT {COLUMNS[kind]}, {ordinal}::BIGINT AS file_id, file_row_number AS row_id FROM read_parquet({literal(path)}, file_row_number=true)"
        table = "raw_" + kind
        if kind not in kinds:
            db.execute(f"CREATE TABLE {table} AS {query}")
            kinds[kind] = True
        else:
            db.execute(f"INSERT INTO {table} {query}")
        inserted=db.fetchone()[0]
        if inserted!=file["rows"]:
            raise ValueError("projected row count differs from admitted manifest")
        progress("necessary-columns", raw_rows=inserted, files=1, source=kind)
    return kinds


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
    db.execute(f"CREATE TABLE unique_{table} AS SELECT * FROM {table} QUALIFY row_number() OVER (PARTITION BY {key_sql} ORDER BY file_id,row_id)=1")
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
        FROM raw_polymarket_market_metadata WHERE metadata_row_kind='metadata_snapshot'
        UNION ALL SELECT condition_id,list_sort(assets),window_start,window_end,greatest(received,admitted),received FROM initial_metadata""")
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


def make_pm_events(db, raw):
    # Expand only asset identifiers, never depth/price payloads.
    db.execute(f"""CREATE TABLE pm_input AS SELECT market AS condition_id,
        epoch_ms(timestamp_received) AS received,epoch_ms(timestamp) AS event,
        greatest(epoch_ms(timestamp_received),epoch_ms(collector_admission_time_utc_ms)) AS base,
        source_sequence_epoch_id AS epoch,collector_admission_sequence AS sequence,
        file_id,row_id,event_type,
        CASE WHEN event_type='price_change' THEN changed_assets
             WHEN event_type IN ('book','best_bid_ask','tick_size_change','last_trade_price') THEN [asset_id]
             ELSE []::VARCHAR[] END AS assets
        FROM {raw}""")
    db.execute("""CREATE TABLE pm_epoch AS SELECT condition_id,epoch,min(struct_pack(received:=received,file_id:=file_id,row_id:=row_id)) AS first_order
        FROM pm_input GROUP BY condition_id,epoch""")
    db.execute("""CREATE TABLE pm_ordered AS SELECT p.*,dense_rank() OVER (PARTITION BY p.condition_id ORDER BY e.first_order) AS generation,
        max(base) OVER (PARTITION BY p.condition_id ORDER BY e.first_order,p.sequence,p.file_id,p.row_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS clock,
        row_number() OVER (PARTITION BY p.condition_id ORDER BY e.first_order,p.sequence,p.file_id,p.row_id) AS order_id
        FROM pm_input p JOIN pm_epoch e USING(condition_id,epoch)""")
    # Every already registered asset becomes unsynchronized on a real epoch change.
    # A generation's first row and its actual asset update may share a millisecond.
    db.execute("""CREATE TABLE pm_asset_first AS SELECT condition_id,asset,min(order_id) AS first_order
        FROM (SELECT condition_id,unnest(assets) AS asset,order_id FROM pm_ordered) GROUP BY condition_id,asset""")
    db.execute("""CREATE TABLE pm_generation_first AS SELECT * FROM pm_ordered WHERE generation>1
        QUALIFY row_number() OVER (PARTITION BY condition_id,generation ORDER BY order_id)=1""")
    db.execute("""CREATE TABLE pm_events AS
        SELECT 'polymarket' AS source,p.condition_id,unnest(p.assets) AS asset,
               greatest(p.clock,m.known_at,m.dependency_received) AS visible_at,p.received,p.event,
               p.generation,true AS valid,p.event_type='book' AS snapshot,false AS forced_sync,
               true AS available,p.file_id,p.row_id,p.order_id,1 AS phase
        FROM pm_ordered p JOIN markets m USING(condition_id)
        WHERE p.base>=m.known_at
        UNION ALL
        SELECT 'polymarket',p.condition_id,f.asset,greatest(p.clock,m.known_at,m.dependency_received),
               NULL,NULL,p.generation,false,false,true,true,p.file_id,p.row_id,p.order_id,0
        FROM pm_generation_first p JOIN markets m USING(condition_id)
        JOIN pm_asset_first f ON p.condition_id=f.condition_id AND f.first_order<p.order_id""")
    invalid = db.execute("SELECT count(*) FROM pm_events e JOIN markets m USING(condition_id) WHERE NOT list_contains(m.assets,e.asset)").fetchone()[0]
    if invalid:
        raise ValueError("PM event references an unregistered asset")
    before_identity = db.execute("SELECT count(*) FROM pm_ordered p LEFT JOIN markets m USING(condition_id) WHERE m.condition_id IS NULL OR p.base<m.known_at").fetchone()[0]
    if before_identity:
        raise ValueError("PM input before its legally visible identity snapshot")


def make_common_events(db, kinds, helper, files, work, progress):
    queries = []
    if "binance_depth_snapshot" in kinds:
        table = deduplicate(db,"raw_binance_depth_snapshot",["source_sequence_epoch_id","collector_admission_sequence"],helper,files,work/"duplicates-depth-snapshot")
        queries.append(f"SELECT 'binance_depth' AS source,'' AS condition_id,'' AS asset,epoch_ms(timestamp_received) AS received,NULL::BIGINT AS event,greatest(epoch_ms(timestamp_received),epoch_ms(collector_admission_time_utc_ms)) AS base,source_sequence_epoch_id AS epoch,collector_admission_sequence AS sequence,true AS valid,true AS snapshot,false AS forced_sync,true AS available,file_id,row_id FROM {table}")
    if "binance_depth_update" in kinds:
        table = deduplicate(db,"raw_binance_depth_update",["source_sequence_epoch_id","collector_admission_sequence"],helper,files,work/"duplicates-depth-update")
        queries.append(f"SELECT 'binance_depth','','',epoch_ms(timestamp_received),epoch_ms(event_time),greatest(epoch_ms(timestamp_received),epoch_ms(collector_admission_time_utc_ms)),source_sequence_epoch_id,collector_admission_sequence,stitch_verdict='applied',false,stitch_verdict IN ('not_applied_confirmed_hole','not_applied_snapshot_unavailable'),stitch_verdict NOT IN ('not_applied_confirmed_hole','not_applied_snapshot_unavailable'),file_id,row_id FROM {table}")
    if "binance_aggtrade" in kinds:
        db.execute("CREATE TABLE valid_trades AS SELECT * FROM raw_binance_aggtrade WHERE record_kind='trade'")
        # Native IDs remain continuous across collector restarts. Preserve the
        # public reducer's backwards-ID failure before suppressing repeats.
        db.execute("""CREATE TEMP TABLE trade_epochs AS SELECT source_sequence_epoch_id,
            min(struct_pack(received:=timestamp_received,file_id:=file_id,row_id:=row_id)) AS first_order
            FROM valid_trades GROUP BY source_sequence_epoch_id""")
        backwards=db.execute("""SELECT count(*) FROM (
            SELECT aggregate_trade_id,lag(aggregate_trade_id) OVER(ORDER BY e.first_order,
                collector_admission_sequence,file_id,row_id) AS previous_id
            FROM valid_trades JOIN trade_epochs e USING(source_sequence_epoch_id))
            WHERE aggregate_trade_id<previous_id""").fetchone()[0]
        if backwards:
            raise ValueError("grouped aggregate trades must not move backwards")
        db.execute("DROP TABLE trade_epochs")
        table = deduplicate(db,"valid_trades",["aggregate_trade_id"],helper,files,work/"duplicates-trades",business=True)
        queries.append(f"SELECT 'binance_trade','','',epoch_ms(timestamp_received),epoch_ms(trade_time),greatest(epoch_ms(timestamp_received),epoch_ms(collector_admission_time_utc_ms)),source_sequence_epoch_id,collector_admission_sequence,true,false,false,true,file_id,row_id FROM {table}")
    for kind, source, window in [("polymarket_rtds_chainlink_btc_usd","chainlink_spot",None),("polymarket_rtds_chainlink_twap","chainlink_twap60",60)]:
        if kind not in kinds:
            continue
        if window:
            table = deduplicate(db,"raw_"+kind,["source_sequence_epoch_id","collector_admission_sequence"],helper,files,work/"duplicates-twap")
            base = "greatest(epoch_ms(timestamp_received),epoch_ms(collector_admission_time_utc_ms))"
            epoch, sequence = "source_sequence_epoch_id", "collector_admission_sequence"
            extra = "AND window_s=60"
        else:
            # Legacy spot has no admission coordinate; native payload + receive is its identity.
            table = "raw_"+kind
            db.execute(f"CREATE TABLE unique_spot AS SELECT * FROM {table} QUALIFY row_number() OVER(PARTITION BY timestamp_received,source_native_payload_json ORDER BY file_id,row_id)=1")
            table="unique_spot"
            base, epoch, sequence, extra="epoch_ms(timestamp_received)","''","row_id", ""
        # Four classified anomalies are no-ops. Stale is a legal refreshing value.
        queries.append(f"SELECT {literal(source)},'','',epoch_ms(timestamp_received),epoch_ms(payload_timestamp),{base},{epoch},{sequence},true,false,false,true,file_id,row_id FROM {table} WHERE freshness_status IN ('fresh','stale') {extra}")
    if not queries:
        db.execute("CREATE TABLE common_events(source VARCHAR,condition_id VARCHAR,asset VARCHAR,visible_at BIGINT,received BIGINT,event BIGINT,generation BIGINT,valid BOOLEAN,snapshot BOOLEAN,forced_sync BOOLEAN,available BOOLEAN,file_id BIGINT,row_id BIGINT,order_id BIGINT,phase INTEGER)")
        return
    db.execute("CREATE TABLE common_raw AS SELECT * FROM (" + " UNION ALL ".join(queries) + ") t(source,condition_id,asset,received,event,base,epoch,sequence,valid,snapshot,forced_sync,available,file_id,row_id)")
    db.execute("""CREATE TABLE common_epochs AS SELECT source,epoch,min(struct_pack(received:=received,file_id:=file_id,row_id:=row_id)) AS first_order FROM common_raw GROUP BY source,epoch""")
    db.execute("""CREATE TABLE common_events AS SELECT r.source,condition_id,asset,
        max(base) OVER(PARTITION BY r.source ORDER BY e.first_order,CASE WHEN r.source='chainlink_spot' THEN r.file_id ELSE 0 END,r.sequence,r.file_id,r.row_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS visible_at,
        received,event,dense_rank() OVER(PARTITION BY r.source ORDER BY e.first_order) AS generation,
        valid,snapshot,forced_sync,available,r.file_id,r.row_id,
        row_number() OVER(PARTITION BY r.source ORDER BY e.first_order,CASE WHEN r.source='chainlink_spot' THEN r.file_id ELSE 0 END,r.sequence,r.file_id,r.row_id) AS order_id,1 AS phase
        FROM common_raw r JOIN common_epochs e USING(source,epoch)""")
    progress("common-minimal-state")


def state_changes(db, output):
    """Persist narrow scalar changes and message samples, not full observations.

    Each scope stores direct last-valid clocks, sync and availability. The SQL
    window is source-local. No global six-head replay clock is introduced.
    """
    db.execute("CREATE TABLE events AS SELECT * FROM pm_events UNION ALL SELECT * FROM common_events")
    db.execute("""CREATE TABLE ordered_events AS SELECT *,
        lag(generation) OVER w AS prior_generation,
        CASE WHEN valid THEN struct_pack(received:=received,event:=event) END AS confirmation
        FROM events WINDOW w AS (PARTITION BY source,condition_id,asset ORDER BY order_id,phase)""")
    db.execute("""CREATE TABLE states AS SELECT *,last_value(confirmation IGNORE NULLS) OVER w AS last_valid,
        last_value(CASE WHEN source='binance_depth' THEN
            CASE WHEN snapshot THEN false WHEN forced_sync OR generation!=prior_generation THEN true END
            WHEN source='polymarket' THEN CASE WHEN snapshot THEN false WHEN forced_sync THEN true END
            ELSE false END IGNORE NULLS) OVER w AS sync,
        last_value(available) OVER w AS availability
        FROM ordered_events WINDOW w AS (PARTITION BY source,condition_id,asset ORDER BY order_id,phase ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)""")
    # Samples retain every effective confirmation even when several occur visible_at one ms.
    db.execute(f"COPY (SELECT source,condition_id,asset,visible_at,received,event,coalesce(sync,false) AS sync_required,availability AS available FROM states WHERE valid) TO {literal(output/'arrivals.parquet')} (FORMAT PARQUET,COMPRESSION ZSTD)")
    # Collapse only zero-duration same-ms intermediate states. Do not drop arrival samples.
    db.execute(f"""COPY (SELECT source,condition_id,asset,visible_at,last_valid.received AS received,last_valid.event AS event,
        coalesce(sync,false) AS sync_required,coalesce(availability,false) AS available,
        (sync IS NULL OR source NOT IN ('polymarket','binance_depth')) AS sync_unknown,
        (generation IS NULL OR source NOT IN ('binance_trade','binance_depth')) AS generation_unknown
        FROM states QUALIFY row_number() OVER(PARTITION BY source,condition_id,asset,visible_at ORDER BY order_id DESC,phase DESC)=1)
        TO {literal(output/'changes.parquet')} (FORMAT PARQUET,COMPRESSION ZSTD)""")
    db.execute(f"COPY markets TO {literal(output/'markets.parquet')} (FORMAT PARQUET,COMPRESSION ZSTD)")
    db.execute(f"COPY rules TO {literal(output/'rules.parquet')} (FORMAT PARQUET,COMPRESSION ZSTD)")
