"""Direct query units produce sufficient bins, not a message database.

Each source's native units are sequential. DuckDB owns vectorized windows and
aggregation, with one shared process-wide worker/memory budget. Only current
scope facts and small summaries survive a unit. Frozen validation re-reads raw
necessary columns from the saved causal boundary; it does not need event logs.
"""
from __future__ import annotations
import json
import subprocess
from dataclasses import asdict
from pathlib import Path
from dpt_health import direct, projection
from dpt_health.projection import literal
from . import intervals, statistics

DAY = intervals.DAY


def metadata(db, config, initialization, rules):
    # Metadata is a small identity/rule boundary; aggregate each physical file
    # before accumulating. Never accumulate the PM/trade/depth raw input here.
    db.execute('CREATE TEMP TABLE raw_polymarket_market_metadata AS SELECT '+projection.COLUMNS['polymarket_market_metadata']+' FROM read_parquet('+literal(next(f['path'] for f in config['files'] if f['source_kind']=='polymarket_market_metadata'))+') WHERE false')
    for file in config['files']:
        if file['source_kind'] == 'polymarket_market_metadata':
            db.execute(f"INSERT INTO raw_polymarket_market_metadata BY NAME SELECT DISTINCT {projection.COLUMNS[file['source_kind']]} FROM read_parquet({literal(file['path'])})")
    projection.projected_markets(db, initialization, rules)
    db.execute('DROP TABLE raw_polymarket_market_metadata')
    db.execute('''CREATE TABLE compact_rules AS SELECT condition_id,visible_at,sequence,reference FROM (
        SELECT *,lag(reference) OVER(PARTITION BY condition_id ORDER BY visible_at,sequence) AS prior_reference,
            row_number() OVER(PARTITION BY condition_id ORDER BY visible_at,sequence) AS ordinal
        FROM (SELECT * FROM rules QUALIFY row_number() OVER(PARTITION BY condition_id,visible_at ORDER BY sequence DESC)=1))
        WHERE ordinal=1 OR reference IS DISTINCT FROM prior_reference''')
    db.execute('DROP TABLE rules');db.execute('ALTER TABLE compact_rules RENAME TO rules')
    for table in ('identities','initial_metadata','rule_map'):
        db.execute(f'DROP TABLE {table}')


def first_spot(db, files):
    frontier = 0
    for f in files:
        if f['source_kind'] != 'polymarket_rtds_chainlink_btc_usd':
            continue
        row = db.execute(f"""WITH ordered AS (SELECT freshness_status,file_row_number,
            max(epoch_ms(timestamp_received)) OVER(ORDER BY file_row_number ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS frontier
            FROM read_parquet({literal(f['path'])},file_row_number=true))
            SELECT min_by(frontier,file_row_number) FILTER(WHERE freshness_status IN ('fresh','stale')),max(frontier) FROM ordered""").fetchone()
        if row[0] is not None:
            return max(frontier,row[0])
        frontier = max(frontier,row[1] or 0)
    return None


def setup(db, config, material):
    for table in ('markets','rules'):
        if not db.execute('SELECT count(*) FROM duckdb_tables() WHERE table_name=?',[table]).fetchone()[0]:
            db.execute(f'CREATE TABLE {table} AS SELECT * FROM read_parquet({literal(material/(table+".parquet"))})')
    start,end = config['start'],config['end']
    db.execute(f"""CREATE TABLE scopes AS SELECT 'polymarket' AS source,condition_id,unnest(assets) AS asset,
        greatest(window_start,{start}) AS a,least(window_end,{end}) AS b FROM markets WHERE window_start<{end} AND window_end>{start}
        UNION ALL SELECT source,'','',{start},{end} FROM (VALUES ('chainlink_spot'),('chainlink_twap60'),('binance_trade'),('binance_depth')) x(source)""")
    if (material/'selections.parquet').exists():
        db.execute(f"CREATE TABLE selections AS SELECT * FROM read_parquet({literal(material/'selections.parquet')})")
    else:
        db.execute('CREATE TEMP TABLE changes(source VARCHAR,received BIGINT,available BOOLEAN,visible_at BIGINT)')
        spot = first_spot(db, config['files'])
        if spot is not None:
            db.execute("INSERT INTO changes VALUES('chainlink_spot',?,true,?)",[spot,spot])
        intervals.make_selection(db,start,end,config['binance_trade_enabled'],config['binance_depth_enabled'])
        db.execute('DROP TABLE changes')
    statistics.histogram_macros(db)


def unit_intervals(db, source, lower, upper):
    # The latest millisecond is not consumed until the following native unit
    # supplies its final state. Every real arrival is aggregated immediately.
    db.execute(f"""CREATE TEMP TABLE points AS SELECT s.source,s.condition_id,s.asset,
        greatest(c.visible_at,s.a-1) AS visible_at,c.visible_at AS original_at,c.received,c.event,
        c.sync_required,c.available,c.sync_unknown,c.generation_unknown,s.b
        FROM scopes s JOIN changes c USING(source,condition_id,asset) WHERE s.source={literal(source)} AND c.visible_at<s.b
        UNION ALL SELECT source,condition_id,asset,a-1,-9223372036854775807,NULL,NULL,false,false,true,true,b
        FROM scopes WHERE source={literal(source)}""")
    db.execute("""CREATE TEMP TABLE final_points AS SELECT * FROM points QUALIFY row_number() OVER(
        PARTITION BY source,condition_id,asset,visible_at ORDER BY original_at DESC)=1""")
    db.execute(f"""CREATE TEMP TABLE causal_intervals AS SELECT source,condition_id,asset,visible_at AS a,
        least(coalesce(lead(visible_at) OVER w,b),{upper}) AS b,received,event,sync_required,available,sync_unknown,generation_unknown,
        original_at,CASE WHEN sync_required THEN 'synchronization_required' WHEN received IS NULL THEN 'unknown_business_progress'
            WHEN NOT available THEN 'unavailable' WHEN received>visible_at OR event>received THEN 'time_inconsistent'
            WHEN event IS NULL THEN 'unknown_event_time' END AS exclusion
        FROM final_points WINDOW w AS(PARTITION BY source,condition_id,asset ORDER BY visible_at)""")
    db.execute(f"""CREATE TEMP TABLE intervals AS SELECT i.* REPLACE(greatest(i.a,s.a,{lower}) AS a)
        FROM causal_intervals i JOIN scopes s USING(source,condition_id,asset)
        WHERE i.b>greatest(i.a,s.a,{lower})""")
    db.execute("""CREATE TEMP TABLE selected_intervals AS SELECT * FROM intervals WHERE source='polymarket'
        UNION ALL SELECT i.* REPLACE(greatest(i.a,s.a) AS a,least(i.b,s.b) AS b)
        FROM intervals i JOIN selections s ON i.source=s.source AND i.a<s.b AND i.b>s.a WHERE i.source!='polymarket'""")


def histogram_accumulator(db):
    db.execute("""CREATE TABLE histogram_edges(mode VARCHAR,day_start BIGINT,group_name VARCHAR,metric VARCHAR,
        bin BIGINT,kind VARCHAR,value HUGEINT,PRIMARY KEY(mode,day_start,group_name,metric,bin,kind))""")
    db.execute("""CREATE TABLE denominators(source VARCHAR,condition_id VARCHAR,asset VARCHAR,day_start BIGINT,exclusion VARCHAR,
        duration_ms HUGEINT,sync_unknown_ms HUGEINT,generation_unknown_ms HUGEINT,PRIMARY KEY(source,condition_id,asset,day_start,exclusion))""")
    db.execute("""CREATE TABLE arrival_bins(source VARCHAR,condition_id VARCHAR,asset VARCHAR,day_start BIGINT,status VARCHAR,bin BIGINT,
        samples BIGINT,selected_samples BIGINT,PRIMARY KEY(source,condition_id,asset,day_start,status,bin))""")


def accumulate(db):
    intervals.daily_intervals(db,'intervals','source_daily')
    intervals.daily_intervals(db,'selected_intervals','selected_daily')
    for table,mode,group in [('source_daily','source','source'),('selected_daily','selected',statistics.GROUP)]:
        db.execute(f"""CREATE TEMP TABLE ages AS SELECT day_start,{group} AS group_name,metric,
            da-clock AS x,db-clock AS y,age_bin(da-clock) AS first_bin,age_bin(db-clock-1) AS last_bin
            FROM {table},LATERAL (VALUES ('silence',received),('event_age',event)) v(metric,clock)
            WHERE exclusion IS NULL AND db>da""")
        db.execute(f"""INSERT INTO histogram_edges SELECT {literal(mode)},day_start,group_name,metric,bin,kind,sum(value)::HUGEINT FROM (
            SELECT day_start,group_name,metric,first_bin AS bin,'partial' AS kind,
                CASE WHEN first_bin=last_bin THEN y-x ELSE bin_upper(first_bin)-x END AS value FROM ages
            UNION ALL SELECT day_start,group_name,metric,last_bin,'partial',y-bin_lower(last_bin) FROM ages WHERE last_bin>first_bin
            UNION ALL SELECT day_start,group_name,metric,first_bin+1,'delta',1 FROM ages WHERE last_bin>first_bin+1
            UNION ALL SELECT day_start,group_name,metric,last_bin,'delta',-1 FROM ages WHERE last_bin>first_bin+1)
            GROUP BY day_start,group_name,metric,bin,kind
            ON CONFLICT DO UPDATE SET value=histogram_edges.value+excluded.value""")
        db.execute('DROP TABLE ages')
    db.execute("""INSERT INTO denominators SELECT source,condition_id,asset,day_start,coalesce(exclusion,'valid'),
        sum(db-da)::HUGEINT,sum(CASE WHEN sync_unknown THEN db-da ELSE 0 END)::HUGEINT,
        sum(CASE WHEN generation_unknown THEN db-da ELSE 0 END)::HUGEINT
        FROM source_daily GROUP BY source,condition_id,asset,day_start,exclusion ON CONFLICT DO UPDATE SET
        duration_ms=denominators.duration_ms+excluded.duration_ms,sync_unknown_ms=denominators.sync_unknown_ms+excluded.sync_unknown_ms,
        generation_unknown_ms=denominators.generation_unknown_ms+excluded.generation_unknown_ms""")
    db.execute(f"""INSERT INTO arrival_bins WITH samples AS (SELECT a.*,(a.visible_at//{DAY})*{DAY} AS day_start,
        CASE WHEN sync_required THEN 'synchronization_required' WHEN NOT available THEN 'unavailable' WHEN event IS NULL THEN 'unknown_event_time'
             WHEN event>received THEN 'time_inconsistent' ELSE 'valid' END AS status,
        CASE WHEN NOT sync_required AND available AND event<=received THEN age_bin(received-event) ELSE -1 END AS bin,
        (a.source='polymarket' OR EXISTS(SELECT 1 FROM selections z WHERE z.source=a.source AND a.visible_at>=z.a AND a.visible_at<z.b)) AS selected
        FROM arrivals a JOIN scopes s USING(source,condition_id,asset) WHERE a.visible_at>=s.a AND a.visible_at<s.b)
        SELECT source,condition_id,asset,day_start,status,bin,count(*),sum(selected::INTEGER) FROM samples
        GROUP BY source,condition_id,asset,day_start,status,bin ON CONFLICT DO UPDATE SET
        samples=arrival_bins.samples+excluded.samples,selected_samples=arrival_bins.selected_samples+excluded.selected_samples""")


def finish_histograms(db, output):
    # Expand each observed daily histogram domain once, after all unit edges have
    # been added. No interval-by-bin join and no repeated 86,940-bin expansion.
    for mode in ('selected','source'):
        db.execute(f"""CREATE TABLE {mode}_histogram AS WITH domains AS (
            SELECT day_start,group_name,metric,min(bin) AS lo,max(bin) AS hi FROM histogram_edges
            WHERE mode={literal(mode)} AND kind='delta' GROUP BY day_start,group_name,metric),
        dense AS (SELECT d.*,r.bin FROM domains d,LATERAL range(lo,hi+1) r(bin)), interiors AS (
            SELECT d.day_start,d.group_name,d.metric,d.bin,sum(coalesce(e.value,0)) OVER(
                PARTITION BY d.day_start,d.group_name,d.metric ORDER BY d.bin ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS density
            FROM dense d LEFT JOIN histogram_edges e ON e.mode={literal(mode)} AND e.kind='delta'
                AND (d.day_start,d.group_name,d.metric,d.bin)=(e.day_start,e.group_name,e.metric,e.bin)), mass AS (
            SELECT day_start,group_name,metric,bin,value AS mass FROM histogram_edges WHERE mode={literal(mode)} AND kind='partial'
            UNION ALL SELECT day_start,group_name,metric,bin,density*(bin_upper(bin)-bin_lower(bin)) FROM interiors WHERE density>0)
        SELECT day_start,group_name,metric,bin,sum(mass)::BIGINT AS duration_ms FROM mass
        GROUP BY day_start,group_name,metric,bin HAVING sum(mass)>0""")
        db.execute(f"COPY {mode}_histogram TO {literal(output/(mode+'_histogram.parquet'))} (FORMAT PARQUET,COMPRESSION ZSTD)")
    for table in ('denominators','arrival_bins'):
        query = f'SELECT * REPLACE(duration_ms::BIGINT AS duration_ms,sync_unknown_ms::BIGINT AS sync_unknown_ms,generation_unknown_ms::BIGINT AS generation_unknown_ms) FROM {table}' if table=='denominators' else f'SELECT * FROM {table}'
        db.execute(f"COPY ({query}) TO {literal(output/(table+'.parquet'))} (FORMAT PARQUET,COMPRESSION ZSTD)")


def query_plan(db, config, progress):
    ranges = direct.range_index(db,config['files'],progress,config.get('native_unit_span',1_048_576))
    units = list(direct.query_units(ranges, config.get('native_unit_span',1_048_576)))
    return ranges,units


def save_seed(db, output, fold, source, cutoff, unit_id, before):
    path = output/f'fold-{fold}.{source}.seed.parquet'
    db.execute(f"""COPY (SELECT * FROM changes WHERE visible_at<{cutoff} QUALIFY row_number() OVER(
        PARTITION BY source,condition_id,asset ORDER BY visible_at DESC)=1) TO {literal(path)} (FORMAT PARQUET,COMPRESSION ZSTD)""")
    trade = db.execute("SELECT count(*) FROM duckdb_tables() WHERE table_name='valid_trades'").fetchone()[0]
    trade_path = output/f'fold-{fold}.{source}.trade-seed.parquet'
    db.execute(f"COPY ("+(f'SELECT aggregate_trade_id,trade_time,file_id,row_id FROM (SELECT aggregate_trade_id,trade_time,file_id,row_id,order_id FROM valid_trades WHERE clock<{cutoff} UNION ALL SELECT *,0 FROM prior_trade_carry) ORDER BY order_id DESC LIMIT 1' if trade else 'SELECT * FROM trade_carry')+f") TO {literal(trade_path)} (FORMAT PARQUET,COMPRESSION ZSTD)")
    descriptor = dict(fold=fold,source=source,cutoff=cutoff,unit_id=unit_id,before=before,seed=path.name,trade_seed=trade_path.name)
    (output/f'fold-{fold}.{source}.seed.json').write_text(json.dumps(descriptor))


def distributions(db, config, material, work, units, progress, ranges=()):
    histogram_accumulator(db)
    cutoffs = [config['start']+d*DAY for d in (3,6,9,12,15)]
    sources = sorted(set(u.source for u in units) | set(direct.SOURCE.values()))
    for source in sources:
        runner = direct.SourceQuery(db,config['files'],Path(config['boundary_helper']),work,source,file_ranges=ranges)
        lower = config['start']; saved = set()
        source_units = [(i,u) for i,u in enumerate(units) if u.source==source]
        for i,u in source_units:
            before,dirs = runner.load(u)
            for fold,cutoff in enumerate(cutoffs):
                if fold not in saved and runner.frontier>=cutoff:
                    save_seed(db,material,fold,source,cutoff,i,before); saved.add(fold)
            end = min(config['end'],runner.frontier)
            unit_intervals(db,source,lower,end)
            accumulate(db)
            runner.keep_before(runner.frontier+1)
            runner.release(dirs)
            lower=max(lower,end)
            progress('direct-duration-bins',source=source,unit_id=i,unit_count=len(units),
                     native_range=[u.lo,u.hi],physical_candidate_file_rows=u.candidate_file_rows,frontier=runner.frontier,
                     retained_scope_facts=db.execute('SELECT count(*) FROM carry').fetchone()[0])
        # No-message tail and missing sources are real denominator intervals.
        db.execute('CREATE TEMP TABLE changes AS SELECT * FROM carry')
        db.execute('CREATE TEMP VIEW arrivals AS SELECT source,condition_id,asset,visible_at,received,event,sync_required,available FROM carry WHERE false')
        for fold,cutoff in enumerate(cutoffs):
            if fold not in saved:
                save_seed(db,material,fold,source,cutoff,len(units),runner.frontier)
        unit_intervals(db,source,lower,config['end']);accumulate(db)
        runner.release([])
        db.execute('DROP TABLE carry');db.execute('DROP TABLE trade_carry')
    finish_histograms(db,material)


def ordering_summary(ranges, units):
    result=[]
    for source in sorted({r.source for r in ranges}):
        epochs={u.epoch:u.generation for u in units if u.source==source}
        for epoch,generation in sorted(epochs.items(),key=lambda x:x[1]):
            rows=[r for r in ranges if r.source==source and r.epoch==epoch]
            first=min(rows,key=lambda r:(r.file_id,r.lo,r.first_row) if source=='chainlink_spot' else (r.lo,r.file_id,r.first_row))
            result.append(dict(source=source,epoch=epoch,used_order_rank=generation,first_native=first.lo,
                first_file_id=first.file_id,first_row_id=first.first_row,first_available_base=first.first_base,
                first_received=first.first_received,maximum_available_base=max(r.maximum_base for r in rows),
                minimum_received=min(r.minimum_received for r in rows),maximum_received=max(r.maximum_received for r in rows),
                last_native=max(r.hi for r in rows),indexed_records=sum(r.rows for r in rows),
                ordering_convention='epoch first native base; stable file/row tie; whole-source prefix frontier'))
    return result


class Evaluation:
    """One open continuous episode per selected scope and candidate.

    Only closed episodes contribute counts. Unit boundaries never become
    transitions or censoring. Daily episodes are intersections with UTC days.
    """
    def __init__(self, db, candidate, a, b, name, output):
        self.db,self.candidate,self.a,self.b,self.name,self.output = db,candidate,a,b,name,output
        self.part=0
        self.prefix="ev_"+"".join(c if c.isalnum() else "_" for c in name)+"_"
        self.execute(f"""CREATE TABLE evaluation_scopes AS SELECT source,condition_id,asset,greatest(a,{a}) AS evaluation_start,least(b,{b}) AS b
            FROM scopes WHERE source='polymarket' AND a<{b} AND b>{a}
            UNION ALL SELECT source,'','',greatest(min(a),{a}),least(max(b),{b}) FROM (
                SELECT *,sum((previous_end IS DISTINCT FROM a)::INTEGER) OVER(PARTITION BY source ORDER BY a ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS period
                FROM (SELECT *,lag(b) OVER(PARTITION BY source ORDER BY a) AS previous_end FROM selections))
            GROUP BY source,period HAVING min(a)<{b} AND max(b)>{a}""")
        self.execute("""CREATE TABLE open_episodes(source VARCHAR,condition_id VARCHAR,asset VARCHAR,evaluation_start BIGINT,state VARCHAR,
            a BIGINT,b BIGINT,received BIGINT,event BIGINT,original_at BIGINT,previous_state VARCHAR,previous_end BIGINT)""")
        self.execute("""CREATE TABLE whole_states(source VARCHAR,condition_id VARCHAR,asset VARCHAR,state VARCHAR,duration_ms HUGEINT,
            episodes BIGINT,longest_episode_ms BIGINT,transitions BIGINT,left_censored_episodes BIGINT,right_censored_episodes BIGINT,
            PRIMARY KEY(source,condition_id,asset,state))""")
        self.execute("""CREATE TABLE daily_states(source VARCHAR,condition_id VARCHAR,asset VARCHAR,day_start BIGINT,state VARCHAR,duration_ms HUGEINT,
            episode_fragments BIGINT,transitions BIGINT,left_censored_fragments BIGINT,right_censored_fragments BIGINT,longest_fragment_ms BIGINT,
            PRIMARY KEY(source,condition_id,asset,day_start,state))""")
        (output/(name+'.exceeded')).mkdir()

    def execute(self, sql, parameters=None):
        import re
        names=('evaluation_scopes','open_episodes','whole_states','daily_states')
        sql=re.sub(r'\b('+'|'.join(names)+r')\b',lambda m:self.prefix+m.group(),sql)
        return self.db.execute(sql,parameters) if parameters is not None else self.db.execute(sql)

    def consume(self, source, lower, end):
        db = self.db
        self.execute(f"""CREATE TEMP TABLE evaluation_domain AS SELECT i.* REPLACE(greatest(i.a,s.evaluation_start) AS a,least(i.b,s.b) AS b)
            FROM selected_intervals i JOIN evaluation_scopes s USING(source,condition_id,asset)
            WHERE i.a<s.b AND i.b>s.evaluation_start AND i.source={literal(source)}
            UNION ALL SELECT i.* REPLACE(s.evaluation_start-1 AS a,s.evaluation_start AS b)
            FROM causal_intervals i JOIN evaluation_scopes s USING(source,condition_id,asset)
            WHERE s.source={literal(source)} AND s.evaluation_start-1>=i.a AND s.evaluation_start-1<i.b
              AND s.evaluation_start>={lower} AND s.evaluation_start<={end}
              AND NOT EXISTS (SELECT 1 FROM open_episodes o WHERE
                (o.source,o.condition_id,o.asset,o.evaluation_start)=
                (s.source,s.condition_id,s.asset,s.evaluation_start))""")
        intervals.candidate_intervals(db,self.candidate['parameters'],'evaluation_domain','candidate_spans')
        self.execute("""CREATE TEMP TABLE scoped_spans AS SELECT c.*,s.evaluation_start
            FROM candidate_spans c JOIN evaluation_scopes s USING(source,condition_id,asset)
            WHERE c.a<s.b AND c.b>s.evaluation_start-1
            UNION ALL SELECT source,condition_id,asset,a,b,received,event,original_at,false,false,state,evaluation_start
            FROM open_episodes WHERE source=?""",[source])
        intervals.episodes(db,'scoped_spans','merged_episodes',selection_scopes=True)
        self.execute("""CREATE TEMP TABLE contextual_episodes AS SELECT e.* EXCLUDE(episode,previous_state,previous_end),
            CASE WHEN o.a IS NOT NULL THEN o.previous_state ELSE e.previous_state END AS previous_state,
            CASE WHEN o.a IS NOT NULL THEN o.previous_end ELSE e.previous_end END AS previous_end,s.b AS scope_end
            FROM merged_episodes e JOIN evaluation_scopes s USING(source,condition_id,asset,evaluation_start)
            LEFT JOIN open_episodes o ON (e.source,e.condition_id,e.asset,e.evaluation_start,e.a)=(o.source,o.condition_id,o.asset,o.evaluation_start,o.a)""")
        self.execute(f"""CREATE TEMP TABLE closed_episodes AS SELECT * EXCLUDE(scope_end) REPLACE(greatest(a,evaluation_start) AS a),
            a<evaluation_start AS left_censored,b=scope_end AS right_censored
            FROM contextual_episodes WHERE (b<{end} OR b=scope_end) AND b>evaluation_start""")
        self.execute('DELETE FROM open_episodes WHERE source=?',[source])
        self.execute(f"""INSERT INTO open_episodes BY NAME SELECT * EXCLUDE(scope_end) FROM contextual_episodes WHERE b={end} AND b<scope_end""")
        intervals.daily_intervals(db,'closed_episodes','episode_daily')
        self.execute("""INSERT INTO whole_states SELECT source,condition_id,asset,state,sum(b-a)::HUGEINT,count(*),max(b-a),
            sum((NOT left_censored AND previous_state IS NOT NULL AND previous_end=a)::INTEGER),sum(left_censored::INTEGER),sum(right_censored::INTEGER)
            FROM closed_episodes GROUP BY source,condition_id,asset,state ON CONFLICT DO UPDATE SET
            duration_ms=whole_states.duration_ms+excluded.duration_ms,episodes=whole_states.episodes+excluded.episodes,
            longest_episode_ms=greatest(whole_states.longest_episode_ms,excluded.longest_episode_ms),
            transitions=whole_states.transitions+excluded.transitions,left_censored_episodes=whole_states.left_censored_episodes+excluded.left_censored_episodes,
            right_censored_episodes=whole_states.right_censored_episodes+excluded.right_censored_episodes""")
        self.execute("""INSERT INTO daily_states SELECT source,condition_id,asset,day_start,state,sum(db-da)::HUGEINT,count(*),
            sum((NOT left_censored AND a=da AND previous_state IS NOT NULL AND previous_end=a)::INTEGER),
            sum((left_censored OR a<da)::INTEGER),sum((right_censored OR b>db)::INTEGER),max(db-da)
            FROM episode_daily GROUP BY source,condition_id,asset,day_start,state ON CONFLICT DO UPDATE SET
            duration_ms=daily_states.duration_ms+excluded.duration_ms,episode_fragments=daily_states.episode_fragments+excluded.episode_fragments,
            transitions=daily_states.transitions+excluded.transitions,left_censored_fragments=daily_states.left_censored_fragments+excluded.left_censored_fragments,
            right_censored_fragments=daily_states.right_censored_fragments+excluded.right_censored_fragments,
            longest_fragment_ms=greatest(daily_states.longest_fragment_ms,excluded.longest_fragment_ms)""")
        if self.execute("SELECT count(*) FROM closed_episodes WHERE state='exceeded'").fetchone()[0]:
            path=self.output/(self.name+'.exceeded')/f'{self.part:06d}.parquet';self.part+=1
            self.execute(f"""COPY (SELECT source,condition_id,asset,a AS trigger_at,b AS episode_end,received,event,
                a-received AS silence_ms,a-event AS event_age_ms,previous_state,original_at AS last_fact_visible_at,
                (NOT left_censored AND a>original_at AND previous_state IS NOT NULL AND previous_end=a) AS crossed_without_new_message,
                left_censored AS evaluation_left_censored,right_censored AS evaluation_right_censored
                FROM closed_episodes WHERE state='exceeded') TO {literal(path)} (FORMAT PARQUET,COMPRESSION ZSTD)""")

        for table in ('episode_daily','closed_episodes','contextual_episodes','merged_episodes','scoped_spans','candidate_spans','evaluation_domain'):
            db.execute(f'DROP TABLE {table}')

    def result(self):
        db=self.db
        def rows(table):
            cursor=self.execute(f'SELECT * FROM {table} ORDER BY source,condition_id,asset,state')
            return [dict(zip([c[0] for c in cursor.description],r)) for r in cursor.fetchall()]
        whole,daily=rows('whole_states'),rows('daily_states')
        grouped,markets={},{}
        for row in whole:
            group='chainlink' if row['source'].startswith('chainlink_') else row['source']
            g=grouped.setdefault((group,row['state']),dict(group=group,state=row['state'],duration_ms=0,episodes=0,transitions=0))
            for field in ('duration_ms','episodes','transitions'):
                g[field]+=row[field]
            if row['source']=='polymarket':
                m=markets.setdefault((row['condition_id'],row['state']),dict(condition_id=row['condition_id'],state=row['state'],asset_ms=0,asset_episodes=0,asset_transitions=0))
                for field,target in [('duration_ms','asset_ms'),('episodes','asset_episodes'),('transitions','asset_transitions')]:
                    m[target]+=row[field]
        for row in grouped.values():
            denominator=sum(r['duration_ms'] for r in grouped.values() if r['group']==row['group'])
            row['transitions_per_observed_hour']=row['transitions']*3_600_000/denominator if denominator else None
            row['hour_unit']='asset-hours' if row['group']=='polymarket' else 'source-hours'
        result=dict(candidate=self.candidate,window=[self.a,self.b],states=whole,daily_states=daily,groups=list(grouped.values()),markets=list(markets.values()),
                    exceeded_triggers=self.name+'.exceeded/*.parquet',duration_unit='PM: asset-ms; common: source-ms')
        for table in ('evaluation_scopes','open_episodes','whole_states','daily_states'):
            self.execute(f'DROP TABLE {table}')
        return result


def validate_candidates(db, config, material, output, work, units, candidates, a, b, fold, progress):
    ranges=[direct.FileRange(**r) for r in json.loads((material/'file-ranges.json').read_text())]
    evaluators=[Evaluation(db,candidate,a,b,f"fold-{fold}.{candidate['identifier']}",output) for candidate in candidates]
    for source in sorted(set(direct.SOURCE.values())):
        seed=json.loads((material/f'fold-{fold}.{source}.seed.json').read_text())
        runner=direct.SourceQuery(db,config['files'],Path(config['boundary_helper']),work,source,seed['before'],file_ranges=ranges)
        db.execute(f"INSERT INTO carry BY NAME SELECT * FROM read_parquet({literal(material/seed['seed'])})")
        db.execute(f"INSERT INTO trade_carry SELECT * FROM read_parquet({literal(material/seed['trade_seed'])})")
        lower=a-1
        for i,u in enumerate(units):
            if u.source!=source or i<seed['unit_id']:
                continue
            before,dirs=runner.load(u,lower=a,upper=b)
            end=min(b,runner.frontier)
            unit_intervals(db,source,lower,end)
            for evaluator in evaluators:
                evaluator.consume(source,lower,end)
            runner.keep_before(runner.frontier+1)
            runner.release(dirs)
            lower=max(lower,end)
            progress('direct-frozen-validation',fold=fold,source=source,unit_id=i,frontier=runner.frontier)
            if end>=b:
                break
        if lower<b:
            db.execute('CREATE TEMP TABLE changes AS SELECT * FROM carry')
            unit_intervals(db,source,lower,b)
            for evaluator in evaluators:
                evaluator.consume(source,lower,b)
            runner.release([])
        db.execute('DROP TABLE carry');db.execute('DROP TABLE trade_carry')
    return [evaluator.result() for evaluator in evaluators]
