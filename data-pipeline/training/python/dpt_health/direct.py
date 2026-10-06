"""Direct Parquet query planning and bounded source-state SQL.

Only file/scope summaries and current source state survive a query unit. The
input rows remain in Parquet; there is no eighteen-day raw table or message log.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from .projection import COLUMNS, literal


SOURCE = {
    'polymarket_market': 'polymarket',
    'binance_aggtrade': 'binance_trade',
    'binance_depth_snapshot': 'binance_depth',
    'binance_depth_update': 'binance_depth',
    'polymarket_rtds_chainlink_btc_usd': 'chainlink_spot',
    'polymarket_rtds_chainlink_twap': 'chainlink_twap60',
}


@dataclass(frozen=True)
class FileRange:
    file_id: int
    source: str
    condition_id: str
    epoch: str
    lo: int
    hi: int
    rows: int
    first_base: int
    first_received: int
    first_row: int
    maximum_base: int
    minimum_received: int
    maximum_received: int
    file_rows: int


@dataclass(frozen=True)
class QueryUnit:
    source: str
    condition_id: str
    epoch: str
    generation: int
    lo: int
    hi: int
    file_ids: tuple[int, ...]
    candidate_file_rows: int


def native_columns(kind):
    spot = kind == 'polymarket_rtds_chainlink_btc_usd'
    return (
        'market' if kind == 'polymarket_market' else "''",
        "''" if spot else 'source_sequence_epoch_id',
        'file_row_number' if spot else 'collector_admission_sequence',
        'epoch_ms(timestamp_received)' if spot else
        'greatest(epoch_ms(timestamp_received),epoch_ms(collector_admission_time_utc_ms))',
    )


def range_index(db, files, progress, native_span=1_048_576):
    """One file at a time, aggregate before returning anything to Python.

    PM's largest admitted file has 3,267,444 rows. This index query needs only
    condition/epoch/native coordinates and two clocks. It neither expands assets
    nor keeps those rows. Its result is one row per file/source epoch/nonempty native bucket.
    """
    result = []
    for file_id, file in enumerate(files):
        kind = file['source_kind']
        if kind not in SOURCE:
            continue
        path = Path(file['path'])
        if path.stat().st_size != file['bytes']:
            raise ValueError('file size changed from admitted manifest')
        _condition, epoch, sequence, base = native_columns(kind)
        condition = "''"
        cursor = db.execute(f"""SELECT condition_id,epoch,min(sequence),max(sequence),count(*),
            min_by(base,struct_pack(sequence:=sequence,row_id:=row_id)),
            min_by(received,struct_pack(sequence:=sequence,row_id:=row_id)),
            min_by(row_id,struct_pack(sequence:=sequence,row_id:=row_id)),
            max(base),min(received),max(received)
            FROM (SELECT {condition} AS condition_id,{epoch} AS epoch,
                {sequence} AS sequence,{base} AS base,
                epoch_ms(timestamp_received) AS received,file_row_number AS row_id
                FROM read_parquet({literal(path)},file_row_number=true))
            GROUP BY condition_id,epoch,sequence//{native_span}""")
        result.extend(FileRange(file_id, SOURCE[kind], *row, file['rows']) for row in cursor.fetchall())
        progress('file-scope-index', raw_rows=file['rows'], files=1, source=kind)
    return result


def query_units(ranges, sequence_span=1_048_576):
    """Natural physical files, with overlapping native ranges joined once.

    The native span only controls the small nonempty index buckets. Queries use
    whole-file/epoch ranges: they do not reopen a 54-MiB footer for each bucket.
    A connected overlap component includes all candidate files, so identical
    admission coordinates cannot be split. Its conservative physical input is
    the sum of complete candidate file row counts, explicitly reported.
    """
    sources = {}
    for r in ranges:
        sources.setdefault(r.source, {}).setdefault(r.epoch, []).append(r)
    for source, epochs in sorted(sources.items()):
        def first_order(item):
            records=item[1]
            first=min(records,key=lambda r:(r.file_id,r.lo,r.first_row) if source=='chainlink_spot' else (r.lo,r.file_id,r.first_row))
            return first.first_base,first.file_id,first.first_row
        for generation,(epoch,records) in enumerate(sorted(epochs.items(),key=first_order),1):
            per_file={}
            for r in records:
                current=per_file.setdefault(r.file_id,[r.lo,r.hi,r.file_rows])
                current[0]=min(current[0],r.lo);current[1]=max(current[1],r.hi)
            ordered=sorted(per_file.items(),key=lambda item:(item[0],) if source=='chainlink_spot' else (item[1][0],item[0]))
            components=[]
            for file_id,(lo,hi,rows) in ordered:
                if source!='chainlink_spot' and components and lo<=components[-1][1]:
                    components[-1][1]=max(components[-1][1],hi)
                    components[-1][2].append(file_id);components[-1][3]+=rows
                else:
                    components.append([lo,hi,[file_id],rows])
            for lo,hi,file_ids,rows in components:
                yield QueryUnit(source,'',epoch,generation,lo,hi+1,tuple(sorted(file_ids)),rows)


def unit_query(unit, files):
    """An explicit native predicate allows Parquet row-group statistics pruning.

    Pruning is an engine capability, not a claimed measured property. Worst-case
    physical reads are the indexed matching files, never all eighteen days for
    each market. No LIMIT removes messages; all duplicate coordinates remain.
    """
    queries = []
    for file_id in unit.file_ids:
        file = files[file_id]; kind = file['source_kind']
        condition, epoch, sequence, _base = native_columns(kind)
        predicates = [f'{sequence}>={unit.lo}', f'{sequence}<{unit.hi}']
        if unit.source != 'chainlink_spot':
            predicates.append(f'{epoch}={literal(unit.epoch)}')
        if unit.source == 'polymarket' and unit.condition_id:
            predicates.append(f'{condition}={literal(unit.condition_id)}')
        queries.append(f"SELECT {COLUMNS[kind]},{file_id}::BIGINT AS file_id,"
                       f"file_row_number AS row_id,{literal(kind)} AS source_kind "
                       f"FROM read_parquet({literal(file['path'])},file_row_number=true) "
                       f"WHERE {' AND '.join(predicates)}")
    # Depth snapshot and update have different source-native columns; the caller
    # normalizes their projections before combining them.
    return queries


class SourceQuery:
    """A bounded native unit, and one current fact per registered scope.

    The unit tables disappear after the caller aggregates them. Retained state
    is clocks/sync/epoch plus one current trade identity coordinate. Admission
    duplicates are complete within native units; there is no truncated seen set.
    """
    def __init__(self, db, files, helper, work, source, frontier=0, file_ranges=()):
        self.db, self.files, self.helper, self.work = db, files, helper, work
        self.source, self.frontier, self.ordinal = source, frontier, 0
        self.receive_ranges = {}
        for r in file_ranges:
            if r.source == source:
                bounds=self.receive_ranges.setdefault(r.file_id,[r.minimum_received,r.maximum_received])
                bounds[0]=min(bounds[0],r.minimum_received);bounds[1]=max(bounds[1],r.maximum_received)
        db.execute("""CREATE TEMP TABLE carry(source VARCHAR,condition_id VARCHAR,asset VARCHAR,
            visible_at BIGINT,received BIGINT,event BIGINT,generation BIGINT,sync_required BOOLEAN,
            available BOOLEAN,sync_unknown BOOLEAN,generation_unknown BOOLEAN,registered BOOLEAN)""")
        db.execute("CREATE TEMP TABLE trade_carry(aggregate_trade_id UBIGINT,trade_time TIMESTAMP_MS,file_id BIGINT,row_id BIGINT)")

    def load(self, unit, lower=None, upper=None):
        from . import projection
        db = self.db
        self.ordinal += 1
        db.execute('CREATE TEMP TABLE prior_trade_carry AS SELECT * FROM trade_carry')
        before = self.frontier
        queries = unit_query(unit, self.files)
        db.execute('CREATE TEMP TABLE unit_raw AS ' + ' UNION ALL BY NAME '.join(queries))
        kinds = [r[0] for r in db.execute('SELECT DISTINCT source_kind FROM unit_raw').fetchall()]
        spot = unit.source == 'chainlink_spot'
        base = 'epoch_ms(timestamp_received)' if spot else 'greatest(epoch_ms(timestamp_received),epoch_ms(collector_admission_time_utc_ms))'
        sequence = 'row_id' if spot else 'collector_admission_sequence'
        order = ('file_id,' if spot else '') + sequence + ',file_id,row_id'
        db.execute(f"""CREATE TEMP TABLE native_order AS SELECT file_id,row_id,
            greatest({before},max({base}) OVER(ORDER BY {order} ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS clock,
            row_number() OVER(ORDER BY {order}) AS order_id,{unit.generation}::BIGINT AS generation
            FROM unit_raw""")
        self.frontier = max(before, db.execute('SELECT coalesce(max(clock),0) FROM native_order').fetchone()[0])
        db.execute('CREATE TEMP VIEW retained_raw AS SELECT r.*,n.clock,n.order_id,n.generation FROM unit_raw r JOIN native_order n USING(file_id,row_id)' +
                   (f' WHERE clock>={lower}' if lower is not None else '') +
                   (((' AND ' if lower is not None else ' WHERE ') + f'clock<{upper}') if upper is not None else ''))
        for kind in kinds:
            db.execute(f"CREATE TEMP VIEW raw_{kind} AS SELECT * FROM retained_raw WHERE source_kind={literal(kind)}")
        unique = {}
        directories = []
        for kind in kinds:
            if spot or kind == 'binance_aggtrade':
                continue
            directory = self.work/f'duplicate-{self.source}-{self.ordinal}-{kind}'
            directories.append(directory)
            keys = ['source_sequence_epoch_id','collector_admission_sequence']
            if kind == 'polymarket_market':
                keys.insert(0,'market')
            unique[kind] = projection.deduplicate(db,'raw_'+kind,keys,self.helper,self.files,directory)
        if unit.source == 'polymarket':
            raw = unique['polymarket_market']
            db.execute(f"""CREATE TEMP TABLE pm_native AS SELECT market AS condition_id,
                epoch_ms(timestamp_received) AS received,epoch_ms(timestamp) AS event,
                clock AS visible_at,generation,order_id,file_id,row_id,event_type,
                CASE WHEN event_type='price_change' THEN changed_assets
                     WHEN event_type IN ('book','best_bid_ask','tick_size_change','last_trade_price') THEN [asset_id]
                     ELSE []::VARCHAR[] END AS assets FROM {raw}""")
            invalid = db.execute("""SELECT
                count(*) FILTER(WHERE m.condition_id IS NULL),
                count(*) FILTER(WHERE p.visible_at<m.known_at),
                count(*) FILTER(WHERE EXISTS(SELECT 1 FROM unnest(p.assets) x(asset)
                    WHERE NOT list_contains(m.assets,x.asset)))
                FROM pm_native p LEFT JOIN markets m USING(condition_id)""").fetchone()
            if any(invalid):
                raise ValueError('PM identity/asset check failed: missing_identity=%d, before_identity=%d, unregistered_asset=%d' % invalid)
            db.execute("""CREATE TEMP TABLE unit_events AS
                SELECT 'polymarket' AS source,p.condition_id,unnest(p.assets) AS asset,
                    greatest(p.visible_at,m.known_at,m.dependency_received) AS visible_at,
                    p.received,p.event,p.generation,true AS valid,p.event_type='book' AS snapshot,
                    false AS forced_sync,true AS available,p.order_id,1 AS phase
                FROM pm_native p JOIN markets m USING(condition_id)
                UNION ALL SELECT 'polymarket',p.condition_id,c.asset,greatest(p.visible_at,m.known_at,m.dependency_received),
                    NULL,NULL,p.generation,false,false,true,c.available,p.order_id,0
                FROM (SELECT * FROM pm_native QUALIFY row_number() OVER(PARTITION BY condition_id ORDER BY order_id)=1) p
                JOIN markets m USING(condition_id) JOIN carry c USING(condition_id)
                WHERE c.registered AND c.generation IS DISTINCT FROM p.generation""")
        else:
            pieces = []
            for kind in kinds:
                table = unique.get(kind, 'raw_'+kind)
                if kind == 'binance_aggtrade':
                    db.execute('CREATE TEMP TABLE valid_trades AS SELECT * FROM raw_binance_aggtrade WHERE record_kind=\'trade\'')
                    db.execute('CREATE TEMP TABLE trade_compare AS SELECT aggregate_trade_id,trade_time,file_id,row_id,order_id AS source_order FROM valid_trades UNION ALL SELECT *,0 FROM trade_carry')
                    if db.execute('SELECT count(*) FROM (SELECT aggregate_trade_id,lag(aggregate_trade_id) OVER(ORDER BY source_order) AS previous_id FROM trade_compare) WHERE aggregate_trade_id<previous_id').fetchone()[0]:
                        raise ValueError('grouped aggregate trades must not move backwards')
                    directory = self.work/f'duplicate-trade-{self.ordinal}'
                    directories.append(directory)
                    dedup = projection.deduplicate(db,'trade_compare',['aggregate_trade_id'],self.helper,self.files,directory,business=True)
                    db.execute(f'CREATE TEMP VIEW effective_trades AS SELECT t.* FROM valid_trades t JOIN {dedup} d USING(file_id,row_id) WHERE d.source_order>0')
                    pieces.append("SELECT epoch_ms(trade_time) AS event,true AS valid,false AS snapshot,false AS forced_sync,true AS available,* EXCLUDE(trade_time) FROM effective_trades")
                    if db.execute('SELECT count(*) FROM valid_trades').fetchone()[0]:
                        db.execute('DELETE FROM trade_carry')
                        db.execute('INSERT INTO trade_carry SELECT aggregate_trade_id,trade_time,file_id,row_id FROM valid_trades ORDER BY order_id DESC LIMIT 1')
                elif kind == 'binance_depth_snapshot':
                    pieces.append(f'SELECT NULL::BIGINT AS event,true AS valid,true AS snapshot,false AS forced_sync,true AS available,* FROM {table}')
                elif kind == 'binance_depth_update':
                    pieces.append(f"SELECT epoch_ms(event_time) AS event,stitch_verdict='applied' AS valid,false AS snapshot,stitch_verdict IN ('not_applied_confirmed_hole','not_applied_snapshot_unavailable') AS forced_sync,stitch_verdict NOT IN ('not_applied_confirmed_hole','not_applied_snapshot_unavailable') AS available,* FROM {table}")
                else:
                    if spot:
                        # Legacy identity can repeat non-adjacently. Search only
                        # preceding candidate files whose receive ranges overlap;
                        # retain no historical payload set or arbitrary TTL.
                        limits=db.execute(f'SELECT min(epoch_ms(timestamp_received)),max(epoch_ms(timestamp_received)) FROM raw_{kind}').fetchone()
                        prior = [i for i,f in enumerate(self.files) if f['source_kind']==kind and i<=max(unit.file_ids)
                                 and (i not in self.receive_ranges or limits[0] is None or
                                      (self.receive_ranges[i][0]<=limits[1] and self.receive_ranges[i][1]>=limits[0]))]
                        previous = ' UNION ALL '.join(f"SELECT timestamp_received,source_native_payload_json,{i} AS file_id,file_row_number AS row_id FROM read_parquet({literal(self.files[i]['path'])},file_row_number=true) WHERE timestamp_received IN (SELECT timestamp_received FROM raw_{kind})" for i in prior)
                        db.execute('CREATE TEMP VIEW spot_prior AS '+previous)
                        db.execute(f"CREATE TEMP VIEW effective_spot AS SELECT * FROM raw_{kind} r WHERE NOT EXISTS(SELECT 1 FROM spot_prior p WHERE p.timestamp_received=r.timestamp_received AND p.source_native_payload_json=r.source_native_payload_json AND (p.file_id,p.row_id)<(r.file_id,r.row_id))")
                        table = 'effective_spot'
                    # Both TWAP windows share the capture-native counter. Form
                    # its causal frontier before selecting valid TWAP60 facts.
                    selected_window = ' AND window_s=60' if kind == 'polymarket_rtds_chainlink_twap' else ''
                    pieces.append(f"SELECT epoch_ms(payload_timestamp) AS event,true AS valid,false AS snapshot,false AS forced_sync,true AS available,* FROM {table} WHERE freshness_status IN ('fresh','stale'){selected_window}")
            db.execute('CREATE TEMP VIEW effective_common AS '+ ' UNION ALL BY NAME '.join(pieces))
            db.execute(f"""CREATE TEMP TABLE unit_events AS SELECT {literal(unit.source)} AS source,'' AS condition_id,'' AS asset,
                clock AS visible_at,epoch_ms(timestamp_received) AS received,event,generation,
                valid,snapshot,forced_sync,available,order_id,1 AS phase FROM effective_common""")
        # Seed each window from already visible state. New start-time facts never
        # provide the one-ms look-behind used for left truncation.
        db.execute("""CREATE TEMP TABLE ordered_events AS SELECT *,lag(generation) OVER w AS prior_generation,
            CASE WHEN valid THEN struct_pack(received:=received,event:=event) END AS confirmation,
            NULL::BOOLEAN AS inherited_sync FROM unit_events
            WINDOW w AS(PARTITION BY source,condition_id,asset ORDER BY order_id,phase)
            UNION ALL SELECT source,condition_id,asset,visible_at,received,event,generation,false,false,false,available,0,0,
                generation,struct_pack(received:=received,event:=event),CASE WHEN sync_unknown THEN NULL ELSE sync_required END FROM carry""")
        db.execute("""CREATE TEMP TABLE unit_states AS SELECT *,last_value(confirmation IGNORE NULLS) OVER w AS last_valid,
            last_value(CASE WHEN order_id=0 THEN inherited_sync WHEN source='binance_depth' THEN
                CASE WHEN snapshot THEN false WHEN forced_sync OR generation IS DISTINCT FROM coalesce(prior_generation,(SELECT generation FROM carry WHERE source='binance_depth')) THEN true END
                WHEN source='polymarket' THEN CASE WHEN snapshot THEN false WHEN forced_sync THEN true END
                ELSE false END IGNORE NULLS) OVER w AS sync,
            last_value(available IGNORE NULLS) OVER w AS availability
            FROM ordered_events WINDOW w AS(PARTITION BY source,condition_id,asset ORDER BY order_id,phase ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)""")
        db.execute("""CREATE TEMP TABLE changes AS SELECT source,condition_id,asset,visible_at,
            last_valid.received AS received,last_valid.event AS event,coalesce(sync,false) AS sync_required,
            coalesce(availability,false) AS available,sync IS NULL AS sync_unknown,
            source NOT IN ('binance_trade','binance_depth') AS generation_unknown,generation,
            (order_id>0 OR coalesce((SELECT registered FROM carry c WHERE (c.source,c.condition_id,c.asset)=(unit_states.source,unit_states.condition_id,unit_states.asset)),false)) AS registered
            FROM unit_states QUALIFY row_number() OVER(PARTITION BY source,condition_id,asset,visible_at ORDER BY order_id DESC,phase DESC)=1""")
        db.execute("""CREATE TEMP VIEW arrivals AS SELECT source,condition_id,asset,visible_at,received,event,
            coalesce(sync,false) AS sync_required,coalesce(availability,false) AS available FROM unit_states WHERE valid""")
        return before, directories

    def keep_before(self, cutoff, target='carry'):
        self.db.execute(f"CREATE TEMP TABLE next_carry AS SELECT * FROM changes WHERE visible_at<{cutoff} QUALIFY row_number() OVER(PARTITION BY source,condition_id,asset ORDER BY visible_at DESC)=1")
        self.db.execute(f'DELETE FROM {target}')
        self.db.execute(f'INSERT INTO {target} BY NAME SELECT * FROM next_carry')
        self.db.execute('DROP TABLE next_carry')

    def release(self, directories):
        db = self.db
        for (name,) in db.execute("SELECT view_name FROM duckdb_views() WHERE temporary AND NOT internal").fetchall():
            db.execute(f'DROP VIEW "{name}"')
        for (name,) in db.execute("SELECT table_name FROM duckdb_tables() WHERE NOT internal AND (temporary OR starts_with(table_name,'unique_'))").fetchall():
            if name not in ('carry','trade_carry'):
                db.execute(f'DROP TABLE "{name}"')
        # Successful duplicate subsets no longer serve a result. On failure this
        # method is never entered; the exact failed boundary remains inspectable.
        for directory in directories:
            if directory.exists():
                for path in directory.iterdir():
                    path.unlink()
                directory.rmdir()
