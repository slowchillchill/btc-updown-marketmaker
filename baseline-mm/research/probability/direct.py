"""按原生单元直接查询，段间只保留必要状态和紧凑采样。

元数据及 WS 终局子集沿公共 Rust 类型边界；价格沿公共精确规范化。
公共深度归约器维护唯一数量映射，只在实际采样时求和。
"""
from __future__ import annotations
import hashlib
import json
import subprocess
from collections import defaultdict
from pathlib import Path
from dpt_health import direct, projection
from dpt_health.projection import literal
from .model import WINDOWS, REMAINING, innovation_steps

DEPTH_COLUMNS = {
    'binance_depth_snapshot': 'timestamp_received,snapshot_fetch_start_utc,last_update_id,bids,asks,symbol,source_endpoint,snapshot_status,source_sequence_epoch_id,connection_generation,depth_ingress_sequence,collector_admission_sequence,collector_admission_time_utc_ms,collector_host_id,collector_region,schema_version',
    'binance_depth_update': 'timestamp_received,event_type,event_time,symbol,first_update_id,final_update_id,bids,asks,stream,snapshot_last_update_id,local_u_before,stitch_verdict,stitch_evidence,stitch_gap_reason,source_sequence_epoch_id,connection_generation,depth_ingress_sequence,collector_admission_sequence,collector_admission_time_utc_ms,collector_host_id,collector_region,source_endpoint,schema_version',
}
PRICE_COLUMNS = dict(projection.COLUMNS,
    polymarket_rtds_chainlink_btc_usd=projection.COLUMNS['polymarket_rtds_chainlink_btc_usd']+',value',
    polymarket_rtds_chainlink_twap=projection.COLUMNS['polymarket_rtds_chainlink_twap']+',full_accuracy_value',
    binance_aggtrade=projection.COLUMNS['binance_aggtrade']+',price,quantity,buyer_is_maker', **DEPTH_COLUMNS)

METADATA_CLOCK_COLUMNS=('schema_version','metadata_row_kind','timestamp_received','collector_host_id',
    'collector_admission_sequence','collector_admission_time_utc_ms','slug','condition_id','clob_market_id',
    'source_evidence_origin','source_evidence_received_at_utc','ws_platform_timestamp_utc',
    'ws_claimant_source_sequence_epoch_id','ws_claimant_collector_admission_sequence','source_evidence_raw_sha256')
ANCHOR_CLOCK_COLUMNS=('metadata_origin','http_request_url','http_body_sha256','anchor_query_ordinal',
    'anchor_query_source_epoch_id','anchor_source_computed_at_utc')
METADATA_CLOCK_TIMES={'timestamp_received','collector_admission_time_utc_ms','source_evidence_received_at_utc',
    'ws_platform_timestamp_utc','anchor_source_computed_at_utc'}

METADATA_SEMANTIC_COLUMNS=('active', 'metadata_row_kind', 'timestamp_received', 'metadata_origin', 'slug', 'condition_id', 'clob_market_id', 'asset_ids', 'market_start_utc', 'market_end_utc', 'market_lifecycle_state', 'market_runtime_role', 'source_evidence_origin', 'source_evidence_event_type', 'source_evidence_channel', 'source_evidence_received_at_utc', 'source_evidence_payload_json', 'market_lifecycle_transition', 'market_runtime_role_transition', 'closed', 'closed_time_utc', 'closed_time_check_failure_reason', 'uma_resolution_status', 'outcomes_raw', 'outcome_prices_raw', 'accepting_orders', 'ws_platform_timestamp_raw', 'ws_platform_timestamp_utc', 'winning_asset_id', 'winning_outcome', 'resolution_evidence_source', 'terminal_claim_owner', 'resolution_poll_result', 'resolution_integrity_error_class', 'resolution_followup_status', 'ws_claimant_source_sequence_epoch_id', 'ws_claimant_collector_admission_sequence', 'schema_version', 'collector_host_id', 'collector_admission_sequence', 'collector_admission_time_utc_ms', 'source_evidence_raw_sha256', 'http_request_url', 'http_body_sha256', 'anchor_query_ordinal', 'anchor_query_source_epoch_id', 'anchor_source_computed_at_utc', 'anchor_query_result', 'price_to_beat_decimal', 'price_to_beat_source_field', 'twap_enabled', 'twap_lookback_seconds')
METADATA_SEMANTIC_TIMES=METADATA_CLOCK_TIMES|{'market_start_utc', 'closed_time_utc', 'market_end_utc'}


class Boundary:
    def __init__(self, executable):
        self.process=subprocess.Popen([str(executable)],stdin=subprocess.PIPE,stdout=subprocess.PIPE,text=True)
    def call(self, **command):
        self.process.stdin.write(json.dumps(command)+'\n'); self.process.stdin.flush()
        response=self.process.stdout.readline()
        if not response:
            raise RuntimeError('public probability boundary stopped; original stderr remains in stage log')
        return json.loads(response)
    def close(self):
        self.process.stdin.close()
        code=self.process.wait()
        self.process.stdout.close()
        if code: raise RuntimeError(f'public probability boundary exit {code}')


def normalize(db, boundary, work, table, expression, e18=False):
    if not db.execute(f'SELECT count(*) FROM {table}').fetchone()[0]:
        db.execute('CREATE TEMP TABLE normalized(raw VARCHAR,price DOUBLE)')
        return
    a,b=work/'values.jsonl',work/'normalized.jsonl'
    db.execute(f"COPY (SELECT DISTINCT {expression} AS raw,{str(e18).lower()} AS e18 FROM {table}) TO {literal(a)} (FORMAT JSON)")
    boundary.call(op='normalize',input=str(a),output=str(b))
    db.execute(f"CREATE TEMP TABLE normalized AS SELECT raw,value::DOUBLE AS price FROM read_json_auto({literal(b)})")
    a.unlink();b.unlink()


def typed_subset(files, coordinates, work, number):
    """仅真实WS终局声明仍使用已有完整平台协议解码。"""
    import pyarrow as pa
    import pyarrow.parquet as pq
    out=[]
    for file_id in sorted(coordinates):
        selected=coordinates[file_id]
        if not selected: continue
        source=files[file_id];parquet=pq.ParquetFile(source['path'])
        wanted=sorted(selected);cursor=0;consumed=0;batches=[];clocks=[]
        for group in range(parquet.num_row_groups):
            n=parquet.metadata.row_group(group).num_rows;indices=[]
            while cursor<len(wanted) and wanted[cursor]<consumed+n:
                row=wanted[cursor]; indices.append(row-consumed); clocks.append(selected[row]);cursor+=1
            if indices: batches.append(parquet.read_row_group(group,use_threads=False).take(pa.array(indices,pa.int64())))
            consumed+=n
        path=work/f'boundary-{number}-{file_id}.parquet'
        pq.write_table(pa.concat_tables(batches),path,compression='zstd',use_compliant_nested_type=False)
        fact={k:source[k] for k in ('source_kind','schema_version')}
        fact.update(path=str(path),bytes=path.stat().st_size,rows=len(clocks),sha256=hashlib.sha256(path.read_bytes()).hexdigest())
        out.append(dict(file=fact,clocks=clocks,raw_path=source["path"],row_ids=wanted))
    return out


def semantic_subset(db, files, file_id, coordinates, work, number):
    """元数据映射/参考/终局仅投影实际字段，不读生产身份时间或交易tick。"""
    if not coordinates:return []
    source=files[file_id];wanted=sorted(coordinates)
    columns=[]
    v6=set(ANCHOR_CLOCK_COLUMNS)|{'anchor_query_result','price_to_beat_decimal','price_to_beat_source_field','twap_enabled','twap_lookback_seconds'}
    v6.discard('metadata_origin')
    for name in METADATA_SEMANTIC_COLUMNS:
        value='NULL' if name in v6 and source['schema_version']!='polymarket_market_metadata_v6' else f'epoch_ms({name})' if name in METADATA_SEMANTIC_TIMES else name
        columns.append(f'{value} AS {name}')
    path=work/f'metadata-semantic-{number}-{file_id}.jsonl'
    ids=','.join(map(str,wanted))
    db.execute('COPY (SELECT '+','.join(columns)+f" FROM read_parquet({literal(source['path'])},file_row_number=true) WHERE file_row_number IN ({ids}) ORDER BY file_row_number) TO {literal(path)} (FORMAT JSON)")
    fact=dict(path=str(path),source_kind=source['source_kind'],schema_version=source['schema_version'],bytes=path.stat().st_size,rows=len(wanted),sha256=hashlib.sha256(path.read_bytes()).hexdigest())
    return [dict(file=fact,semantic_projection=str(path),clocks=[coordinates[r] for r in wanted],raw_path=source['path'],row_ids=wanted)]


def market_facts(db, config, boundary, work, units, progress):
    # 元数据只读研究语义字段；PM先仅读时钟和真实终局类型。
    descriptors=[];ordinal=0
    metadata_files=[f for f in config['files'] if f['source_kind']=='polymarket_market_metadata']
    metadata_ids=[i for i,f in enumerate(config['files']) if f['source_kind']=='polymarket_market_metadata']
    request=work/'metadata-clocks.private.json';clocks=work/'metadata-clocks.jsonl'
    clock_projection=work/'metadata-clock-fields.jsonl';parts=[]
    for local_id,file in enumerate(metadata_files):
        columns=[]
        for name in METADATA_CLOCK_COLUMNS+ANCHOR_CLOCK_COLUMNS:
            value='NULL' if name in ANCHOR_CLOCK_COLUMNS and file['schema_version']!='polymarket_market_metadata_v6' else f'epoch_ms({name})' if name in METADATA_CLOCK_TIMES else name
            columns.append(f'{value} AS {name}')
        parts.append(f"SELECT {local_id} AS file_id,file_row_number AS row_id,"+','.join(columns)+f" FROM read_parquet({literal(file['path'])},file_row_number=true)")
    if parts:db.execute('COPY ('+' UNION ALL '.join(parts)+f') TO {literal(clock_projection)} (FORMAT JSON)')
    else:clock_projection.write_text('')
    request.write_text(json.dumps(dict(files=[],initialization=config['metadata_initialization'],clock_projection=str(clock_projection))))
    progress('metadata-public-native-coordinates',total_metadata_rows=sum(f['rows'] for f in metadata_files))
    boundary.call(op='metadata_clocks',input=str(request),output=str(clocks))
    db.execute(f'''CREATE TEMP TABLE metadata_clocks AS SELECT *,max(greatest(received,admitted)) OVER(
        ORDER BY admitted,sequence,source_message_id,file_id,row_id ROWS UNBOUNDED PRECEDING) AS clock
        FROM read_json({literal(clocks)},columns={{file_id:'BIGINT',row_id:'BIGINT',received:'BIGINT',admitted:'BIGINT',sequence:'UBIGINT',source_message_id:'VARCHAR'}})''')
    initialization_clocks=[v[0] for v in db.execute('SELECT clock FROM metadata_clocks WHERE file_id=-1 ORDER BY row_id').fetchall()]
    catalog_parts=[]
    for local_id,file_id in enumerate(metadata_ids):
        file=config['files'][file_id]
        base=f"FROM read_parquet({literal(file['path'])},file_row_number=true) r JOIN metadata_clocks c ON c.file_id={local_id} AND c.row_id=r.file_row_number WHERE c.clock<{config['end']}"
        # 事后窗口目录只查询共有字段，不建立交易上下文。
        catalog_parts.append(f"SELECT c.clock AS visible_at,c.received AS received_at,c.source_message_id,r.condition_id,r.clob_market_id,epoch_ms(r.market_start_utc) AS start,epoch_ms(r.market_end_utc) AS end_time,r.asset_ids,r.metadata_row_kind,{literal(file['path'])} AS path,c.row_id,c.admitted,c.sequence "+base)
        coordinates={r:c for r,c in db.execute("SELECT c.row_id,c.clock "+base+" AND (r.outcomes_raw IS NOT NULL OR r.metadata_row_kind IN ('market_anchor_rule_update','resolution_terminal_ws','resolution_poll_attempt')) ORDER BY c.row_id").fetchall()}
        descriptors.extend(semantic_subset(db,config['files'],file_id,coordinates,work,ordinal));ordinal+=1
        progress('metadata-necessary-semantic-subset',files=1,selected_rows=len(coordinates))
    catalog=work/'market-catalog.jsonl'
    if catalog_parts:
        db.execute('COPY (SELECT * EXCLUDE(admitted,sequence) FROM ('+' UNION ALL '.join(catalog_parts)+f') ORDER BY admitted,sequence,source_message_id) TO {literal(catalog)} (FORMAT JSON)')
    else:catalog.write_text('')
    db.execute('DROP TABLE metadata_clocks');request.unlink();clocks.unlink();clock_projection.unlink()
    frontier=0
    for unit in (u for u in units if u.source=='polymarket'):
        parts=[]
        for file_id in unit.file_ids:
            file=config['files'][file_id]
            parts.append(f"SELECT {file_id} AS file_id,file_row_number AS row_id,event_type,collector_admission_sequence AS sequence,"
                f"greatest(epoch_ms(timestamp_received),epoch_ms(collector_admission_time_utc_ms)) AS base FROM read_parquet({literal(file['path'])},file_row_number=true) "
                f"WHERE source_sequence_epoch_id={literal(unit.epoch)} AND collector_admission_sequence>={unit.lo} AND collector_admission_sequence<{unit.hi}")
        db.execute('CREATE TEMP TABLE terminal_clocks AS WITH raw AS ('+' UNION ALL '.join(parts)+f'''), ordered AS (
            SELECT *,greatest({frontier},max(base) OVER(ORDER BY sequence,file_id,row_id ROWS UNBOUNDED PRECEDING)) AS clock FROM raw)
            SELECT file_id,row_id,clock,event_type FROM ordered WHERE event_type='market_resolved'
            UNION ALL SELECT -1,-1,max(base),'frontier' FROM raw''')
        coordinates=defaultdict(dict)
        for file_id,row_id,clock,event_type in db.execute('SELECT * FROM terminal_clocks').fetchall():
            if event_type=='frontier':frontier=max(frontier,clock or 0)
            elif clock<config['end']:coordinates[file_id][row_id]=clock
        descriptors.extend(typed_subset(config['files'],coordinates,work,ordinal));ordinal+=1
        db.execute('DROP TABLE terminal_clocks')
        progress('pm-terminal-clock-projection',unit=ordinal,physical_candidate_rows=unit.candidate_file_rows)
    descriptor=work/'markets-boundary.private.json'
    descriptor.write_text(json.dumps(dict(files=descriptors,initialization=config['metadata_initialization'],initialization_clocks=initialization_clocks,catalog=str(catalog))))
    output=work/'market-facts.jsonl'
    boundary.call(op='markets',input=str(descriptor),output=str(output),max_pending_ws_terminals=config['max_pending_ws_terminals'])
    markets,anchors,rules,labels,census=read_market_facts(output)
    # 紧凑标量变化已保存，临时公共边界输入不再有消费者。
    for d in descriptors:Path(d['file']['path']).unlink()
    descriptor.unlink();output.unlink();catalog.unlink()
    return markets,anchors,rules,labels,census


def read_market_facts(path):
    """按公共Rust实际serde格式消费；原始目录不建立合法公共身份。"""
    markets={};anchors=defaultdict(list);rules=defaultdict(list);labels=defaultdict(list);census={};snapshot_observed={}
    with path.open() as lines:
        for line in lines:
            fact=json.loads(line)
            if 'research_census' in fact:
                body=fact['research_census'];census[body['condition_id']]=body;continue
            if 'research_snapshot_observed' in fact:
                body=fact['research_snapshot_observed'];snapshot_observed.setdefault(body['condition_id'],body['at']);continue
            if 'research_market' in fact:
                body=fact['research_market'];condition=body['identity']['condition_id']
                markets[condition]=body
                continue
            if 'research_label' in fact:
                body=fact['research_label'];terminal=body['terminal'];value=None
                if terminal and terminal['status']=='RESOLVED' and body['quality'] in ('fresh','stale'):
                    outcome=(terminal.get('winning_outcome') or '').strip().lower()
                    value=1 if outcome=='up' else 0 if outcome=='down' else None
                labels[body['condition_id']].append(dict(at=body['at'],label=value,quality=body['quality'],reason=body.get('reason')))
                continue
            if 'research_reference' in fact:
                body=fact['research_reference'];at=body['at'];payload=body['payload']
                value=payload['reference'];kind=payload['reference_type']
                if kind=='market_price_anchor_update':
                    condition=value['market']['condition_id']
                    anchors[condition].append(dict(at=at,value=float(value['anchor']['price_usd_per_btc'])))
                if kind=='market_settlement_reference_update':
                    condition=value['market']['condition_id']
                    rules[condition].append(dict(at=at,reference=value['reference']))
                continue
            at=max(fact['at'],fact['dependency_received']);payload=fact['payload']
            if 'MarketFact' in payload:
                body=payload['MarketFact'];market=body['market'];condition=market['condition_id'];event=body['payload']
                if 'FixedIdentity' in event:
                    markets.setdefault(condition,dict(identity=market,known_at=at,
                        asset_ids=sorted(v['token_id'] for v in event['FixedIdentity']['assets']),
                        source_ids=fact['identity_source_ids']))
                if 'Terminal' in event:
                    terminal=event['Terminal'];label=None
                    if terminal['status']=='RESOLVED' and fact['quality']['availability'] in ('fresh','stale'):
                        outcome=terminal['winning_outcome'];label=1 if outcome.lower()=='up' else 0 if outcome.lower()=='down' else None
                    labels[condition].append(dict(at=at,label=label,quality=fact['quality']['availability']))
                elif 'QualityBoundary' in event and event['QualityBoundary']['scope']['kind']=='MARKET':
                    labels[condition].append(dict(at=at,label=None,quality='invalid'))
            if 'ReferenceFact' in payload:
                event=payload['ReferenceFact'];kind=event['reference_type'];body=event['reference']
                if kind=='market_price_anchor_update':
                    condition=body['market']['condition_id'];value=float(body['anchor']['price_usd_per_btc'])
                    if not anchors[condition] or anchors[condition][-1]['value']!=value:anchors[condition].append(dict(at=at,value=value))
                if kind=='market_settlement_reference_update':
                    condition=body['market']['condition_id']
                    if not rules[condition] or rules[condition][-1]['reference']!=body['reference']:rules[condition].append(dict(at=at,reference=body['reference']))
    for condition,market in markets.items():
        if 'production_identity_not_recomputed' in market:market['snapshot_row_at']=snapshot_observed.get(condition)
    return markets,anchors,rules,labels,census


def census_coverage(config,census,markets,rows):
    """分开原始事后覆盖、公共身份与实际采样资格；未知目标数保留null。"""
    details=[]
    for condition,raw in sorted(census.items()):
        research_market=markets.get(condition)
        public=research_market if research_market and not research_market.get('production_identity_not_recomputed') else None
        known=public['known_at'] if public else None
        windows={(v['market']['window']['start_at_utc_ms'],v['market']['window']['end_at_utc_ms']) for v in raw['variants']}
        conflicts=[]
        if public:
            for variant in raw['variants']:
                if variant['market']!=public['identity'] or variant['asset_ids']!=public['asset_ids']:
                    conflicts.append(dict(reason='census_identity_conflict',raw=variant,public=public))
        raw_count=sum(r['raw_target'] for r in rows if r['condition_id']==condition) if len(windows)==1 else None
        gap_start=max(config['start'],raw['raw_first_seen_at']);gap_end=known if known is not None else config['end']
        details.append(dict(condition_id=condition,raw_first_seen_at=raw['raw_first_seen_at'],identity_at=known,
            identity_qualification='not_recomputed' if raw.get('production_identity_not_recomputed') else 'existing_public_facts',
            mapping_at=research_market.get('mapping_known_at') if research_market else None,
            snapshot_row_at=research_market.get('snapshot_row_at') if research_market else None,
            raw_variants=raw['variants'],raw_rows=raw['raw_rows'],raw_target_count=raw_count,
            public_target_count=sum(r['public_target'] is True for r in rows if r['condition_id']==condition),
            identity_known_samples=sum(r['identity_known'] is True for r in rows if r['condition_id']==condition),
            missing_identity_snapshot=raw['missing_identity_snapshot'],census_identity_conflicts=conflicts,
            identity_gap=dict(start=gap_start,end=gap_end,right_censored=known is None) if gap_end>gap_start else None))
    not_recomputed=any(r.get('production_identity_not_recomputed') for r in census.values())
    result=dict(raw_markets=len(census),research_mapped_markets=len(markets),public_markets=None if not_recomputed else len(markets),
        production_identity_diagnostic='not_recomputed; refer to existing audited coverage on unchanged raw input' if not_recomputed else 'existing_public_facts',raw_locatable_markets=sum(d['raw_target_count'] is not None for d in details),
        raw_locatable_targets=sum(r['raw_target'] for r in rows),raw_target_count_unknown_markets=sum(d['raw_target_count'] is None for d in details),
        public_targets=sum(r['public_target'] is True for r in rows),target_union=len(rows),identity_known_samples=sum(r['identity_known'] is True for r in rows),
        identity_missing_samples=sum(not r['identity_known'] for r in rows),markets=details)
    if not_recomputed:
        for key in ('public_targets','identity_known_samples','identity_missing_samples'):result[key]=None
        for detail in details:
            detail['identity_at']=None;detail['identity_gap']=None;detail['public_target_count']=None;detail['identity_known_samples']=None
    return result


def latest(events, at):
    result=None
    for event in events:
        if event['at']<=at:result=event
        else:break
    return result


def target_rows(config, markets, anchors, rules, labels, census=None):
    out=[]
    # 旧合成测试可只给合法公共市场；真实准备总是传显式原始目录。
    conditions=set(markets)|set(census or {})
    for condition in sorted(conditions):
        public=markets.get(condition);raw=(census or {}).get(condition)
        public_window=public['identity']['window'] if public and not public.get('production_identity_not_recomputed') else None
        public_key=(public_window['start_at_utc_ms'],public_window['end_at_utc_ms']) if public_window else None
        windows={(v['market']['window']['start_at_utc_ms'],v['market']['window']['end_at_utc_ms']) for v in raw['variants']} if raw else set()
        raw_key=next(iter(windows)) if len(windows)==1 else None
        for start,end in sorted({w for w in (raw_key,public_key) if w is not None}):
            if start<config['start'] or start>=config['end']:continue
            for remaining in REMAINING:
                at=end-remaining*1000
                if not start<=at<end or not config['start']<=at<config['end']:continue
                anchor=latest(anchors[condition],at);rule=latest(rules[condition],at)
                reference=rule['reference'] if rule else None
                w={'chainlink_spot':0,'chainlink_twap_thirty_seconds':30,'chainlink_twap_sixty_seconds':60}.get(reference)
                public_target=(start,end)==public_key;known=public['known_at'] if public_target else None
                out.append(dict(condition_id=condition,start=start,end=end,at=at,remaining_s=remaining,day=at//86_400_000,
                    raw_target=(start,end)==raw_key,public_target=None if raw and raw.get('production_identity_not_recomputed') else public_target,raw_first_seen_at=raw['raw_first_seen_at'] if raw else None,
                    identity_known=None if raw and raw.get('production_identity_not_recomputed') else known is not None and known<=at,reference=float(anchor['value']) if anchor else None,
                    reference_kind='official' if anchor else 'missing',rule_window_s=w,rule_reference=reference,
                    rule_visible=rule['at'] if rule else None,rule_segments=rules[condition],market_known_at=min((v['first']['visible_at'] for v in raw['variants'] if (v['market']['window']['start_at_utc_ms'],v['market']['window']['end_at_utc_ms'])==(start,end)),default=None) if raw else public['known_at'] if public else None,labels=labels[condition],price=None,price_received=None,
                    price_age_ms=None,trade_price=None,innovation=None,depth_pressure=None,trade_pressure=None,
                    integral=None,variance={str(w):dict(pairs=0,seconds=0.0,squared_change=0.0,descriptive_seconds=0.0,descriptive_squared_change=0.0) for w in WINDOWS}))
    return sorted(out,key=lambda r:(r['at'],r['condition_id'],r['start']))


def source_sampling(db,config,boundary,work,units,ranges,rows,progress,stages=None):
    from .stages import identity
    db.execute('CREATE TABLE targets(id BIGINT,"at" BIGINT,window_start BIGINT)')
    if rows:db.executemany('INSERT INTO targets VALUES (?,?,?)',[(i,r['at'],r['end']-(r['rule_window_s'] or 0)*1000) for i,r in enumerate(rows)])
    db.execute('CREATE TABLE probability_targets(id BIGINT,rule_window_s BIGINT)')
    if rows:db.executemany('INSERT INTO probability_targets VALUES (?,?)',[(i,r['rule_window_s']) for i,r in enumerate(rows)])
    pair_bins=[dict(cutoff=config['start']+d*86_400_000,pairs=0,seconds=0.0,squared_change=0.0) for d in (3,6,9,12,15)]
    twap_candidates=defaultdict(list)
    for source in ('chainlink_spot','chainlink_twap60','binance_trade','binance_depth'):
        stage_key=identity(config,source,rows) if stages else None
        previous=stages.load(source,stage_key) if stages else None
        if previous is not None:
            for row,fields in zip(rows,previous['samples'],strict=True):row.update(fields)
            if source=='chainlink_spot':pair_bins=previous['pairs']
            if source=='chainlink_twap60':twap_candidates.update(previous['candidates'])
            progress('completed-source-reused',source=source,samples=len(rows))
            continue
        before=[dict(r) for r in rows]
        runner=direct.SourceQuery(db,config['files'],Path(config['boundary_helper']),work,source,file_ranges=ranges,columns=PRICE_COLUMNS,twap_windows=(30,60))
        lower=config['start'];last_accepted=None
        if source=='chainlink_spot':db.execute('CREATE TABLE spot_support(received BIGINT,visible_at BIGINT,price DOUBLE)')
        if source=='chainlink_twap60':
            # 固定目标与单元无关；普通内存表跨 SourceQuery.release 保留。
            target=[]
            for condition_rows in group_rows(rows).values():
                first=condition_rows[0];last_sample=condition_rows[-1]['at']
                if first.get('market_known_at',first['start']) is None:continue
                for j,rule in enumerate(first.get('rule_segments',[])):
                    w={'chainlink_twap_thirty_seconds':30,'chainlink_twap_sixty_seconds':60}.get(rule['reference'])
                    if w is None:continue
                    following=first['rule_segments'][j+1]['at'] if j+1<len(first['rule_segments']) else first['end']
                    # 首个可见规则沿公共 OpeningWindow；真实规则变化从变化可见时开始。
                    lower_bound=first['start'] if j==0 else rule['at']
                    target.append((first['condition_id'],rule['at'],first.get('market_known_at',first['start']),
                        lower_bound,following,first['start'],first['end'],last_sample,w))
            import pyarrow as pa
            names=('condition_id','rule_visible','known_at','lower_bound','upper_bound','start','end_time','last_sample','window_s')
            columns=list(zip(*target)) if target else [[] for _ in names]
            table=pa.Table.from_arrays([pa.array(values,type=pa.string() if i==0 else pa.int64())
                for i,values in enumerate(columns)],names=names)
            db.register('twap_target_columns',table)
            try:db.execute('CREATE TABLE temporary_targets AS SELECT * FROM twap_target_columns')
            finally:db.unregister('twap_target_columns')
        carry=[];spot_level=[]
        for number,unit in enumerate(u for u in units if u.source==source):
            _,directories=runner.load(unit)
            upper=min(config['end'],runner.frontier)
            samples=[i for i,r in enumerate(rows) if lower<=r['at']<upper]
            if samples and source!='chainlink_twap60':
                ids=','.join(map(str,samples))
                for i,received,event,sync,available in db.execute(f'''SELECT t.id,c.received,c.event,c.sync_required,c.available
                    FROM targets t ASOF LEFT JOIN changes c ON t.at>=c.visible_at WHERE t.id IN ({ids})''').fetchall():
                    rows[i][source+'_received_age_ms']=rows[i]['at']-received if received is not None else None
                    rows[i][source+'_event_age_ms']=rows[i]['at']-event if event is not None else None
                    rows[i][source+'_status']='synchronization_required' if sync else 'unavailable' if not available else 'available'
            if source=='binance_depth':
                depth_unit(db,boundary,work,samples,rows,carry)
                carry=[]
            elif source=='chainlink_spot':
                normalize(db,boundary,work,'effective_common','value')
                prior=last_accepted if last_accepted is not None else -9223372036854775807
                db.execute(f'''CREATE TEMP TABLE accepted AS SELECT epoch_ms(timestamp_received) AS received,clock AS visible_at,n.price
                    FROM effective_common e JOIN normalized n ON e.value=n.raw
                    QUALIFY epoch_ms(timestamp_received)>greatest({prior},coalesce(max(epoch_ms(timestamp_received)) OVER(
                    ORDER BY order_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),-9223372036854775807))''')
                last_accepted=db.execute('SELECT coalesce(max(received),?) FROM accepted',[last_accepted]).fetchone()[0]
                db.execute('CREATE TEMP TABLE history AS SELECT * FROM spot_support UNION ALL SELECT * FROM accepted')
                db.execute('''CREATE TEMP TABLE price_pairs AS SELECT received,visible_at,price,
                    lag(received) OVER(ORDER BY received) AS left_received,lag(price) OVER(ORDER BY received) AS left_price,
                    lead(received) OVER(ORDER BY received) AS next_received,lead(visible_at) OVER(ORDER BY received) AS next_visible_at FROM history''')
                for bin in pair_bins:
                    n,t,q=db.execute('''SELECT count(*),coalesce(sum((received-left_received)/1000.0),0),
                        coalesce(sum(pow(price-left_price,2)),0) FROM price_pairs WHERE left_received IS NOT NULL
                        AND visible_at>=? AND visible_at<? AND received IN (SELECT received FROM accepted)''',[config['start'],bin['cutoff']]).fetchone()
                    bin['pairs']+=n;bin['seconds']+=t;bin['squared_change']+=q
                # 全部采样查询按列执行，仅使用当前单元及跨段支持。
                sample_spot(db,rows,samples,spot_level)
                cursor=db.execute('SELECT clock AS at,n.price,epoch_ms(timestamp_received) AS received FROM effective_common e JOIN normalized n ON e.value=n.raw ORDER BY order_id DESC LIMIT 1')
                values=cursor.fetchone()
                if values:spot_level=[dict(zip(('at','price','received'),values))]
                oldest=upper-300_000
                db.execute('DELETE FROM spot_support')
                db.execute('''INSERT INTO spot_support SELECT received,visible_at,price FROM history WHERE received>=?
                    OR received=(SELECT max(received) FROM history WHERE received<?)''',[oldest,oldest])
            elif source in ('chainlink_twap60','binance_trade'):
                expression='full_accuracy_value' if source=='chainlink_twap60' else 'price'
                normalize(db,boundary,work,'effective_common',expression,source=='chainlink_twap60')
                if source=='chainlink_twap60':
                    # 与公共选择器共用候选谓词；SQL 先聚合每市场/规则段的首个候选。
                    found=db.execute('''SELECT t.condition_id,t.rule_visible,t.start,t.end_time,
                        greatest(e.clock,t.rule_visible,t.known_at) AS known_at,
                        epoch_ms(e.timestamp_received),epoch_ms(e.payload_timestamp),n.price,e.window_s
                        FROM temporary_targets t JOIN effective_common e ON e.window_s=t.window_s
                        AND e.clock>=t.lower_bound AND greatest(e.clock,t.rule_visible,t.known_at)<t.upper_bound
                        AND greatest(e.clock,t.rule_visible,t.known_at)<=t.last_sample
                        AND epoch_ms(e.payload_timestamp)>=t.start AND epoch_ms(e.payload_timestamp)<t.end_time
                        AND epoch_ms(e.timestamp_received)>=t.start
                        JOIN normalized n ON e.full_accuracy_value=n.raw
                        QUALIFY row_number() OVER(PARTITION BY t.condition_id,t.rule_visible ORDER BY e.order_id)=1''').fetchall()
                    for condition,rule_at,start,end,at,recv,event,price,w in found:
                        reference='chainlink_twap_thirty_seconds' if w==30 else 'chainlink_twap_sixty_seconds'
                        if boundary.call(op='temporary',reference=reference,start=start,end=end,
                            window_s=w,event=event,received=recv,quality='fresh')['eligible']:
                            twap_candidates[condition].append(dict(at=at,price=price))
                    carry=price_samples(db,rows,samples,carry,'twap')
                else:carry=price_samples(db,rows,samples,carry,'trade')
            runner.keep_before(runner.frontier+1);runner.release(directories)
            lower=max(lower,upper)
            progress('causal-source-samples',source=source,unit=number,native_range=[unit.lo,unit.hi],samples=len(samples))
        # 最后事实延续到无消息尾段，不新增 TTL。
        tail=[i for i,r in enumerate(rows) if lower<=r['at']<config['end']]
        carried=db.execute('SELECT visible_at,received,event,sync_required,available FROM carry ORDER BY visible_at DESC').fetchone()
        for i in (tail if source!='chainlink_twap60' else []):
            if carried and carried[0]<=rows[i]['at']:
                _,received,event,sync,available=carried
                rows[i][source+'_received_age_ms']=rows[i]['at']-received if received is not None else None
                rows[i][source+'_event_age_ms']=rows[i]['at']-event if event is not None else None
                rows[i][source+'_status']='synchronization_required' if sync else 'unavailable' if not available else 'available'
        if source=='binance_depth':depth_unit(None,boundary,work,tail,rows,[])
        elif source=='chainlink_spot':
            db.execute('CREATE TEMP TABLE history AS SELECT * FROM spot_support')
            db.execute('''CREATE TEMP TABLE price_pairs AS SELECT *,lag(received) OVER(ORDER BY received) AS left_received,
                lag(price) OVER(ORDER BY received) AS left_price,lead(received) OVER(ORDER BY received) AS next_received,lead(visible_at) OVER(ORDER BY received) AS next_visible_at FROM history''')
            sample_spot(db,rows,tail,spot_level)
            db.execute('DROP TABLE history');db.execute('DROP TABLE price_pairs');db.execute('DROP TABLE spot_support')
        else:apply_price_carry(rows,tail,carry,source)
        if source=='chainlink_twap60':db.execute('DROP TABLE temporary_targets')
        db.execute('DROP TABLE carry');db.execute('DROP TABLE trade_carry')
        if stages:
            # 每个来源只保留它新增/更新的固定采样字段和必要充分统计。
            samples=[{k:v for k,v in r.items() if k not in old or old[k]!=v} for r,old in zip(rows,before,strict=True)]
            # variance的内层在原地更新；spot明确保存该充分统计。
            if source=='chainlink_spot':
                for r,fields in zip(rows,samples,strict=True):fields['variance']=r['variance']
            stages.save(source,stage_key,dict(samples=samples,pairs=pair_bins if source=='chainlink_spot' else None,
                candidates=dict(twap_candidates) if source=='chainlink_twap60' else None))
    for r in rows:
        if r['reference_kind']!='official':
            candidates=sorted(twap_candidates[r['condition_id']],key=lambda c:c['at'])
            if candidates and candidates[0]['at']<=r['at']:
                r['reference']=candidates[0]['price'];r['reference_kind']='temporary_twap'
        r['spot_price']=r['price'];r['spot_received']=r['price_received']
        if r['price'] is None and r['rule_window_s']:
            r['price']=r.get('twap_price');r['price_received']=r.get('twap_received')
            r['price_age_ms']=r['at']-r['price_received'] if r['price_received'] is not None else None
        q=r.get('trade_quantity');total=r.get('depth_total')
        if q is not None and total is not None:
            r['trade_pressure']=(0.0 if q+total==0 else q/(q+total))*(-1 if r.get('buyer_is_maker') else 1)
    for condition_rows in group_rows(rows).values():innovation_steps(condition_rows)
    return pair_bins


def group_rows(rows):
    grouped=defaultdict(list)
    for row in rows:grouped[row['condition_id']].append(row)
    return grouped


def sample_spot(db,rows,samples,spot_level=()):
    if not samples:return
    ids=','.join(map(str,samples))
    query=f'''SELECT t.id,h.price,h.received FROM targets t LEFT JOIN history h ON t.at>=h.visible_at
        WHERE t.id IN ({ids}) QUALIFY row_number() OVER(PARTITION BY t.id ORDER BY h.received DESC NULLS LAST)=1'''
    # 同毫秒历史仅首点接纳；当前报价可以不同，但不替换方差/积分历史点。
    for i,p,recv in db.execute(query).fetchall():
        rows[i]['price']=p;rows[i]['price_received']=recv;rows[i]['price_age_ms']=rows[i]['at']-recv if recv is not None else None
    for i in samples:
        if spot_level and spot_level[0]['at']<=rows[i]['at']:
            level=spot_level[0];rows[i]['price']=level['price'];rows[i]['price_received']=level['received'];rows[i]['price_age_ms']=rows[i]['at']-level['received']
    has=db.execute("SELECT count(*) FROM duckdb_views() WHERE view_name='effective_common'").fetchone()[0]
    if has:
        for i,p,recv in db.execute(f'''SELECT t.id,n.price,epoch_ms(e.timestamp_received) FROM targets t
            JOIN effective_common e ON e.clock<=t.at JOIN normalized n ON e.value=n.raw WHERE t.id IN ({ids})
            QUALIFY row_number() OVER(PARTITION BY t.id ORDER BY e.clock DESC,e.order_id DESC)=1''').fetchall():
            rows[i]['price']=p;rows[i]['price_received']=recv;rows[i]['price_age_ms']=rows[i]['at']-recv
    for w in WINDOWS:
        for i,n,seconds,square,ds,dq in db.execute(f'''SELECT t.id,count(p.left_received),
            coalesce(sum((p.received-p.left_received)/1000.0),0),coalesce(sum(pow(p.price-p.left_price,2)),0),
            coalesce(sum((p.received-p.left_received)/1000.0) FILTER(WHERE p.left_received>=t.at-{w*1000}),0),
            coalesce(sum(pow(p.price-p.left_price,2)) FILTER(WHERE p.left_received>=t.at-{w*1000}),0)
            FROM targets t LEFT JOIN price_pairs p ON p.visible_at<=t.at AND p.received>=t.at-{w*1000}
            AND p.left_received IS NOT NULL WHERE t.id IN ({ids}) GROUP BY t.id''').fetchall():
            rows[i]['variance'][str(w)]=dict(pairs=n,seconds=seconds,squared_change=square,descriptive_seconds=ds,descriptive_squared_change=dq)
    for i,covered,integral in db.execute(f'''SELECT t.id,coalesce(sum(least(CASE WHEN p.next_visible_at<=t.at THEN p.next_received ELSE t.at END,t.at)-greatest(p.received,t.window_start)),0),
        sum(p.price*(least(CASE WHEN p.next_visible_at<=t.at THEN p.next_received ELSE t.at END,t.at)-greatest(p.received,t.window_start))/1000.0)
        FROM targets t LEFT JOIN price_pairs p ON p.visible_at<=t.at AND p.received<t.at
        AND CASE WHEN p.next_visible_at<=t.at THEN p.next_received ELSE t.at END>t.window_start AND t.at>t.window_start
        WHERE t.id IN ({ids}) GROUP BY t.id''').fetchall():
        r=rows[i];r['integral_covered_ms']=covered
        if covered==max(0,r['at']-(r['end']-(r['rule_window_s'] or 0)*1000)):
            r['integral']=integral


def price_samples(db,rows,samples,carry,kind):
    if kind=='trade':
        query='SELECT clock AS at,epoch_ms(timestamp_received) AS received,event,n.price,quantity::DOUBLE AS quantity,buyer_is_maker,order_id FROM effective_common e JOIN normalized n ON e.price=n.raw'
    else:
        query='SELECT clock AS at,epoch_ms(timestamp_received) AS received,event,n.price,window_s,order_id FROM effective_common e JOIN normalized n ON e.full_accuracy_value=n.raw'
    db.execute('CREATE TEMP TABLE price_facts AS '+query)
    if carry:
        names=list(carry[0])
        columns=','.join('"'+name.replace('"','""')+'"' for name in names)
        db.executemany('INSERT INTO price_facts ('+columns+') VALUES ('+','.join('?' for _ in names)+')',[[f[k] for k in names] for f in carry])
    if samples:
        ids=','.join(map(str,samples))
        cursor=db.execute(f'''SELECT t.id,f.* FROM targets t JOIN price_facts f ON f.at<=t.at
            WHERE t.id IN ({ids}){(' AND f.window_s=(SELECT rule_window_s FROM probability_targets WHERE id=t.id)' if kind=='twap' else '')} QUALIFY row_number() OVER(PARTITION BY t.id ORDER BY f.at DESC,f.order_id DESC)=1''')
        names=[c[0] for c in cursor.description]
        for values in cursor.fetchall():
            fact=dict(zip(names,values));i=fact.pop('id')
            apply_price_carry(rows,[i],[fact], 'binance_trade' if kind=='trade' else 'chainlink_twap60')
    cursor=db.execute('SELECT * FROM price_facts QUALIFY row_number() OVER(PARTITION BY '+('window_s' if kind=='twap' else '1')+' ORDER BY "at" DESC,order_id DESC)=1');names=[c[0] for c in cursor.description]
    result=[dict(zip(names,values)) for values in cursor.fetchall()]
    for fact in result:fact['order_id']=0
    db.execute('DROP TABLE price_facts')
    return result


def apply_price_carry(rows,samples,carry,source):
    if not carry:return
    prefix='trade' if source=='binance_trade' else 'twap'
    for i in samples:
        eligible=[f for f in carry if source=='binance_trade' or f.get('window_s')==rows[i]['rule_window_s']]
        if not eligible:continue
        fact=eligible[-1]
        if fact['at']>rows[i]['at']:continue
        rows[i][prefix+'_price']=fact['price'];rows[i][prefix+'_received']=fact['received']
        rows[i][prefix+'_age_ms']=rows[i]['at']-fact['received']
        if prefix=='twap':rows[i]['twap_received_age_ms']=rows[i]['at']-fact['received']
        rows[i][prefix+'_event_age_ms']=rows[i]['at']-fact['event'] if fact['event'] is not None else None
        if prefix=='trade':rows[i]['trade_quantity']=fact['quantity'];rows[i]['buyer_is_maker']=fact['buyer_is_maker']


def depth_unit(db,boundary,work,samples,rows,carry):
    a,b=work/'depth.jsonl',work/'depth-samples.jsonl'
    if db is not None:
        # 沿完整类型语义解析必要原始行；采样间不输出全盘口。
        pieces=[]
        for kind,columns in DEPTH_COLUMNS.items():
            if not db.execute('SELECT count(*) FROM effective_common WHERE source_kind=?',[kind]).fetchone()[0]:continue
            names=columns.split(',');fields=[]
            for name in names:
                value=f'epoch_ms({name})' if name.endswith('_utc_ms') or name in ('timestamp_received','snapshot_fetch_start_utc','event_time') else name
                fields.append(f'{name}:={value}')
            pieces.append(f"SELECT clock AS at,false AS sample,source_kind AS kind,struct_pack({','.join(fields)}) AS row,order_id FROM effective_common WHERE source_kind={literal(kind)}")
        if samples:
            pieces.append('SELECT t.at AS at,true AS sample,NULL AS kind,NULL AS row,9223372036854775807 AS order_id FROM targets t WHERE id IN ('+','.join(map(str,samples))+')')
        if pieces:db.execute('COPY (SELECT * EXCLUDE(order_id) FROM ('+' UNION ALL BY NAME '.join(pieces)+f') ORDER BY "at",order_id) TO {literal(a)} (FORMAT JSON)')
        else:a.write_text('')
    else:a.write_text(''.join(json.dumps(dict(at=rows[i]['at'],sample=True))+'\n' for i in samples))
    boundary.call(op='depth',input=str(a),output=str(b))
    output=list(map(json.loads,b.read_text().splitlines()))
    facts={r['at']:r for r in output if 'at' in r}
    for i in samples:
        fact=facts[rows[i]['at']]
        if fact.get('missing') is not None:
            rows[i]['depth_missing']=fact['missing']
        if fact['bid'] is not None and rows[i].get('binance_depth_status','available')=='available':
            bid,ask=(int(fact[k]['numerator'])/int(fact[k]['denominator']) for k in ('bid','ask'));total=bid+ask
            rows[i]['depth_total']=total;rows[i]['depth_pressure']=(bid-ask)/total if total>0 else None
    for line in output:
        if 'depth_missing_interval' not in line:continue
        interval=line['depth_missing_interval']
        for r in rows:
            if r.get('depth_missing',{}).get('start')==interval['start']:
                r['depth_missing']=interval
    a.unlink();b.unlink()
