"""沿实际计算入口验证有限合法夹具，不使用真实研究数据或基准。"""
import copy
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/'baseline-mm/research'),str(ROOT/'data-pipeline/training/python')]
from probability import direct, evaluation, model

HELPER=Path(os.environ['PROBABILITY_HELPER'])


def row(at=3000,remaining=10):
    return dict(condition_id='condition',at=at,day=0,remaining_s=remaining,rule_window_s=0,
        price=101.0,reference=100.0,reference_kind='official',price_received=at-1,price_age_ms=1,
        variance={str(w):dict(pairs=1,seconds=1.0,squared_change=1.0) for w in model.WINDOWS},
        labels=[dict(at=4000,label=1,quality='fresh')],innovation=None,depth_pressure=None,trade_pressure=None)


class ProbabilityTests(unittest.TestCase):
    def test_fields_compute_without_production_identity_and_local_missing(self):
        r=row();r['identity_known']=False
        self.assertIsNotNone(model.predict(r,model.Candidate(15,1,0),1)[0])
        r.update(spot_price=100,spot_received=1,trade_price=105)
        model.innovation_steps([r]);self.assertEqual(r['innovation'],0)
        r['reference']=None
        self.assertEqual(model.predict(r,model.Candidate(15,1,0),1)[1],'missing_reference')
        r['reference']=100;r['labels']=[]
        self.assertEqual(evaluation.score([r],model.Candidate(15,1,0),1)['scored'],0)
        # 新研究外层仍消费公共原语真实serde，不把参考改成另一套规则。
        lines=[json.loads(l) for l in (ROOT/'tests/probability/fixtures/public-market-scenario.before-identity.jsonl').read_text().splitlines()]
        references=[dict(research_reference=dict(at=f['at'],payload=f['payload']['ReferenceFact'])) for f in lines if 'ReferenceFact' in f.get('payload',{})]
        with tempfile.TemporaryDirectory() as t:
            path=Path(t)/'references.jsonl';path.write_text(''.join(json.dumps(f)+'\n' for f in references))
            _,anchors,rules,_,_=direct.read_market_facts(path)
        # 实际SQL时钟/目录入口只需要共同字段，没有生产Snapshot/tick。
        with tempfile.TemporaryDirectory() as t:
            root=Path(t);path=root/'metadata.parquet'
            values=dict(schema_version='polymarket_market_metadata_v6',metadata_row_kind='market_state_transition',
                timestamp_received=3100,collector_host_id='fixture-collector',collector_admission_sequence=1,
                collector_admission_time_utc_ms=3101,slug='btc-updown-5m-fixture',condition_id='0x'+'11'*32,
                clob_market_id='999',market_start_utc=3000,market_end_utc=303000,asset_ids='["1","2"]',outcomes_raw=None)
            values.update(metadata_origin='gamma_by_slug',market_lifecycle_state='active',market_runtime_role='current',
                source_evidence_origin='gamma_by_slug',source_evidence_event_type='gamma_market_by_slug_response',
                source_evidence_received_at_utc=3100,outcomes_raw='["UP","DOWN"]',outcome_prices_raw='["0.5","0.5"]',
                source_evidence_payload_json='{"orderPriceMinTickSize":"invalid-not-used"}',fixed_identity_verified_at_utc=0)
            import hashlib
            values['source_evidence_raw_sha256']=hashlib.sha256(values['source_evidence_payload_json'].encode()).hexdigest()
            fields={k:values.get(k) for k in set(direct.METADATA_CLOCK_COLUMNS+direct.ANCHOR_CLOCK_COLUMNS+direct.METADATA_SEMANTIC_COLUMNS)}
            fields.update(values)
            times=direct.METADATA_SEMANTIC_TIMES|{'fixed_identity_verified_at_utc'}
            integers={'collector_admission_sequence','ws_claimant_collector_admission_sequence','anchor_query_ordinal','twap_lookback_seconds'}
            booleans={'active','closed','accepting_orders','twap_enabled'}
            schema=pa.schema([(k,pa.timestamp('ms',tz='UTC') if k in times else pa.int64() if k in integers else pa.bool_() if k in booleans else pa.string()) for k in fields])
            valid_terminal=dict(fields,metadata_row_kind='resolution_poll_attempt',timestamp_received=303100,
                collector_admission_sequence=2,collector_admission_time_utc_ms=303101,source_evidence_received_at_utc=303100,
                closed=True,closed_time_utc=303001,uma_resolution_status='resolved',accepting_orders=False,
                market_lifecycle_state='retired',market_lifecycle_transition='active_to_retired',market_runtime_role='none',
                market_runtime_role_transition='resolution_pending_to_none',resolution_evidence_source='gamma_by_slug',
                terminal_claim_owner='gamma_by_slug',resolution_poll_result='terminal',resolution_followup_status='terminal_gamma_fallback',
                winning_asset_id='1',winning_outcome='UP',outcome_prices_raw='["1","0"]')
            conflict=dict(valid_terminal,timestamp_received=304100,collector_admission_sequence=3,collector_admission_time_utc_ms=304101,
                source_evidence_received_at_utc=304100,outcomes_raw='["YES","NO"]',winning_outcome='YES')
            pq.write_table(pa.Table.from_pylist([fields,valid_terminal,conflict],schema=schema),path)
            import hashlib
            config=dict(files=[dict(path=str(path),source_kind='polymarket_market_metadata',schema_version='polymarket_market_metadata_v6',bytes=path.stat().st_size,rows=3,sha256=hashlib.sha256(path.read_bytes()).hexdigest())],
                start=3000,end=903001,max_pending_ws_terminals=10,metadata_initialization=dict(files=[],markets=[],received_start_utc_ms=3000,received_end_utc_ms=3000,purpose='synthetic'))
            db=duckdb.connect();boundary=direct.Boundary(HELPER)
            try:result=direct.market_facts(db,config,boundary,root,[],lambda *a,**k:None)
            finally:boundary.close();db.close()
            targets=direct.target_rows(config,*result)
            self.assertEqual(len(targets),8)
            condition='0x'+'11'*32
            self.assertEqual(result[0][condition]['asset_outcomes'][0]['outcome']['label'],'UP')
            self.assertEqual(result[0][condition]['mapping_known_at'],3101)
            self.assertEqual(direct.latest(result[3][condition],303500)['label'],1)
            self.assertEqual(direct.latest(result[3][condition],304100)['label'],1)
            self.assertIsNone(direct.latest(result[3][condition],304101)['label'])
            self.assertEqual(direct.latest(result[3][condition],304101)['quality'],'invalid')
            self.assertEqual(direct.latest(result[3][condition],304101)['reason'],'POLYMARKET_TERMINAL_ASSET_IDENTITY_CONFLICT')
            labelled=row(at=3000);labelled['labels']=result[3][condition]
            self.assertEqual(evaluation.score([labelled],model.Candidate(15,1,0),1,label_before=303500)['scored'],1)
            self.assertEqual(evaluation.score([labelled],model.Candidate(15,1,0),1,label_before=304102)['scored'],0)
            self.assertTrue(all(r['identity_known'] is None for r in targets))
            summary=direct.census_coverage(config,result[4],result[0],targets)
            self.assertIsNone(summary['public_markets']);self.assertIsNone(summary['markets'][0]['identity_at'])
        condition='0x'+'11'*32
        self.assertEqual(anchors[condition][0]['value'],100)
        self.assertIsNone(direct.latest(anchors[condition],3300))
        self.assertEqual(rules[condition][0]['reference'],'chainlink_twap_sixty_seconds')

    def test_completed_sources_survive_last_source_failure_and_reuse(self):
        from probability.stages import CompletedStages, identity
        import hashlib
        with tempfile.TemporaryDirectory() as t:
            root=Path(t);work=root/'work';work.mkdir();material=root/'material';material.mkdir()
            path=root/'spot.parquet'
            schema=pa.schema([('timestamp_received',pa.timestamp('ms')),('payload_timestamp',pa.timestamp('ms')),
                ('freshness_status',pa.string()),('source_native_payload_json',pa.string()),('value',pa.string())])
            facts=[dict(timestamp_received=at,payload_timestamp=at-1,freshness_status='fresh',source_native_payload_json=json.dumps({'price':p,'at':at}),value=p) for at,p in [(1000,'100'),(2000,'101')]]
            pq.write_table(pa.Table.from_pylist(facts,schema=schema),path)
            files=[dict(path=str(path),source_kind='polymarket_rtds_chainlink_btc_usd',schema_version='polymarket_rtds_chainlink_btc_usd_v4',bytes=path.stat().st_size,rows=2,sha256=hashlib.sha256(path.read_bytes()).hexdigest())]
            cfg=dict(files=files,boundary_helper=str(HELPER),start=0,end=3001,implementation_commit='first',probability_helper_sha256='first')
            original=row(2500);original.update(start=0,end=12500,rule_segments=[])
            rows=[copy.deepcopy(original)];db=duckdb.connect();boundary=direct.Boundary(HELPER);stages=CompletedStages(material,cfg)
            ranges=direct.direct.range_index(db,files,lambda *a,**k:None);units=list(direct.direct.query_units(ranges))
            try:
                with patch.object(direct,'depth_unit',side_effect=RuntimeError('synthetic last-source program failure')):
                    with self.assertRaisesRegex(RuntimeError,'last-source'):
                        direct.source_sampling(db,cfg,boundary,work,units,ranges,rows,lambda *a,**k:None,stages)
            finally:boundary.close();db.close()
            for name in ('chainlink_spot','chainlink_twap60','binance_trade'):
                self.assertIsNotNone(stages.load(name,identity(cfg,name,[original])))
            self.assertIsNone(stages.load('binance_depth',identity(cfg,'binance_depth',[original])))
            self.assertFalse((material/'material.json').exists())
            # New output/resources and unrelated helper provenance preserve completed stages.
            changed=dict(cfg,run_root='another-root',engine={'threads':24},implementation_commit='depth-only-patch',probability_helper_sha256='depth-only-helper')
            other=root/'other';other.mkdir();reused=CompletedStages(other,changed,material)
            db=duckdb.connect();boundary=direct.Boundary(HELPER);rows=[copy.deepcopy(original)]
            try:
                with patch.object(direct.direct,'SourceQuery',wraps=direct.direct.SourceQuery) as query:
                    direct.source_sampling(db,changed,boundary,work,units,ranges,rows,lambda *a,**k:None,reused)
                    self.assertEqual(query.call_count,1) # Only previously incomplete depth starts.
                self.assertEqual(rows[0]['price'],101)
                self.assertEqual(rows[0]['variance']['15']['squared_change'],1)
            finally:boundary.close();db.close()
            for name in ('chainlink_spot','chainlink_twap60','binance_trade'):
                producer=json.loads((other/'stages'/f'{name}.complete.json').read_text())
                self.assertEqual(producer['implementation_commit'],'first')
                self.assertEqual(producer['helper_sha256'],'first')
                self.assertEqual((other/'stages'/f'{name}.complete.json').read_bytes(),(material/'stages'/f'{name}.complete.json').read_bytes())
            self.assertNotEqual(identity(cfg,'chainlink_spot',[original]),identity(dict(cfg,files=[dict(files[0],sha256='changed')]),'chainlink_spot',[original]))

    def test_formula_piecewise_missing_integral_and_degenerate_up(self):
        r=row();c=model.Candidate(15,1,0)
        result,reason=model.predict(r,c,2);self.assertIsNone(reason)
        self.assertAlmostEqual(result['uncertainty'],10**.5)
        r['rule_window_s']=60;r['remaining_s']=60
        result,_=model.predict(r,c,2);self.assertAlmostEqual(result['uncertainty'],20**.5)
        r['remaining_s']=30;r['integral']=None
        self.assertEqual(model.predict(r,c,2)[1],'missing_window_history')
        r['integral']=100*30
        result,_=model.predict(r,c,2);self.assertAlmostEqual(result['mean'],100.5)
        self.assertAlmostEqual(result['uncertainty'],(30**3/(3*60**2))**.5)
        r['rule_window_s']=0;r['price']=r['reference']
        r['variance']['15']['squared_change']=0
        result,_=model.predict(r,c,2);self.assertEqual(result['p'],1)
        self.assertTrue(result['zero_variance']);self.assertFalse(result['fallback'])
        r['variance']['15']['seconds']=0
        self.assertTrue(model.predict(r,c,2)[0]['fallback'])

    def test_innovation_old_basis_then_refresh_and_reversal(self):
        rows=[dict(price=s,trade_price=b,price_received=t) for s,b,t in [(100,105,0),(101,107,1),(101,105,1),(100,104,2)]]
        model.innovation_steps(rows)
        self.assertEqual([r['innovation'] for r in rows],[0,1,-2,-2])

    def test_fit_ignores_future_label_conflict_and_zero_does_not_require_binance(self):
        rows=[row(at=1),row(at=2)];cutoff=5000;pairs=dict(pairs=2,seconds=2,squared_change=2)
        a,_=evaluation.fit(rows,pairs,cutoff)
        changed=copy.deepcopy(rows);changed[0]['labels'].append(dict(at=6000,label=None,quality='invalid'))
        b,_=evaluation.fit(changed,pairs,cutoff)
        self.assertEqual(a,b);self.assertEqual(a['baseline_scores'][0]['score']['scored'],2)
        self.assertEqual(len(a['baseline_scores']),60)
        self.assertEqual(sum(len(s['candidates']) for s in a['external_searches']),15)
        self.assertTrue(all(s['score']['scored']==0 for q in a['external_searches'] for s in q['candidates']))
        self.assertEqual(evaluation.score(changed,model.Candidate(15,1,0),1)['scored'],1)
        changed[1]['labels']=[]
        self.assertEqual(evaluation.score(changed,model.Candidate(15,1,0),1)['scored'],0)

    def test_paired_external_intersection_preserves_missing_denominator(self):
        a,b=row(at=1),row(at=2);a['innovation']=1
        c=model.Candidate(15,1,0,'innovation',.5)
        result=evaluation.evaluate([a,b],[c],1)[0]
        self.assertEqual(result['score']['targets'],2)
        self.assertEqual(result['score']['scored'],1)
        self.assertEqual(result['paired_baseline']['scored'],1)
        self.assertEqual(result['score']['missing'],{'missing_innovation':1})
        self.assertEqual(evaluation.score([a,b],model.Candidate(15,1,0),1)['scored'],2)

    def test_sql_left_crossing_pair_and_received_axis_integral(self):
        db=duckdb.connect();r=row(at=3000);r.update(end=4000,rule_window_s=3)
        db.execute('CREATE TABLE targets AS SELECT 0 AS id,3000::BIGINT AS at,1000::BIGINT AS window_start')
        db.execute('CREATE TABLE history AS SELECT * FROM (VALUES (900,900,100.0),(1500,1500,101.0),(2500,2500,103.0)) h(received,visible_at,price)')
        db.execute('CREATE TABLE price_pairs AS SELECT *,lag(received) OVER(ORDER BY received) AS left_received,lag(price) OVER(ORDER BY received) AS left_price,lead(received) OVER(ORDER BY received) AS next_received,lead(visible_at) OVER(ORDER BY received) AS next_visible_at FROM history')
        with patch.object(direct,'WINDOWS',(2,)):direct.sample_spot(db,[r],[0])
        self.assertEqual(r['variance']['2']['squared_change'],5)
        self.assertAlmostEqual(r['variance']['2']['seconds'],1.6)
        self.assertEqual(r['variance']['2']['descriptive_squared_change'],4)
        self.assertEqual(r['variance']['2']['descriptive_seconds'],1)
        self.assertEqual(r['integral_covered_ms'],2000)
        self.assertAlmostEqual(r['integral'],100*.5+101+103*.5)
        db.close()

    def test_spot_two_files_midnight_and_same_ms_first_acceptance(self):
        # 两文件沿实际 SourceQuery、旧源去重、公共价格边界和概率 SQL。
        midnight=86_400_000
        with tempfile.TemporaryDirectory() as t:
            root=Path(t);files=[]
            schema=pa.schema([('timestamp_received',pa.timestamp('ms')),('payload_timestamp',pa.timestamp('ms')),
                ('freshness_status',pa.string()),('source_native_payload_json',pa.string()),('value',pa.string())])
            for part,points in enumerate([[(midnight-2000,'100'),(midnight-1000,'101')],[(midnight-1000,'200'),(midnight,'103'),(midnight+1000,'104')]]):
                path=root/f'{part}.parquet'
                facts=[dict(timestamp_received=at,payload_timestamp=at-1,freshness_status='fresh',source_native_payload_json=json.dumps({'price':p,'at':at}),value=p) for at,p in points]
                pq.write_table(pa.Table.from_pylist(facts,schema=schema),path)
                files.append(dict(path=str(path),source_kind='polymarket_rtds_chainlink_btc_usd',schema_version='polymarket_rtds_chainlink_btc_usd_v4',bytes=path.stat().st_size,rows=len(facts)))
            db=duckdb.connect();boundary=direct.Boundary(HELPER)
            try:
                ranges=direct.direct.range_index(db,files,lambda *a,**k:None)
                units=list(direct.direct.query_units(ranges))
                r=row(at=midnight+500);r.update(start=midnight-300000,end=midnight+10500,rule_visible=midnight-300000,identity_known=True)
                r['labels']=[]
                cfg=dict(files=files,boundary_helper=str(HELPER),start=midnight-2000,end=midnight+1001)
                pairs=direct.source_sampling(db,cfg,boundary,root,units,ranges,[r],lambda *a,**k:None)
                self.assertEqual(r['price'],103)
                self.assertEqual(r['variance']['15']['pairs'],2)
                self.assertEqual(r['variance']['15']['squared_change'],5)
                self.assertEqual(r['variance']['15']['seconds'],2)
                self.assertEqual(pairs[0]['squared_change'],6)
                self.assertIsNone(r['depth_pressure']);self.assertIsNone(r['trade_pressure'])
            finally:boundary.close();db.close()

    def test_delayed_accepted_successor_does_not_truncate_earlier_integral(self):
        with tempfile.TemporaryDirectory() as t:
            root=Path(t);path=root/'spot.parquet'
            schema=pa.schema([('timestamp_received',pa.timestamp('ms')),('payload_timestamp',pa.timestamp('ms')),
                ('freshness_status',pa.string()),('source_native_payload_json',pa.string()),('value',pa.string())])
            facts=[dict(timestamp_received=at,payload_timestamp=at-1,freshness_status=quality,source_native_payload_json=json.dumps({'at':at,'p':p}),value=p)
                for at,p,quality in [(900,'100','fresh'),(2200,'999','invalid_type'),(1500,'101','fresh'),(1900,'103','fresh'),(2700,'104','fresh')]]
            pq.write_table(pa.Table.from_pylist(facts,schema=schema),path)
            files=[dict(path=str(path),source_kind='polymarket_rtds_chainlink_btc_usd',schema_version='polymarket_rtds_chainlink_btc_usd_v4',bytes=path.stat().st_size,rows=5)]
            rows=[]
            for at in (2000,2600):
                r=row(at);r.update(start=1000,end=31000,rule_window_s=30,rule_visible=1000);rows.append(r)
            db=duckdb.connect();boundary=direct.Boundary(HELPER)
            try:
                ranges=direct.direct.range_index(db,files,lambda *a,**k:None)
                direct.source_sampling(db,dict(files=files,boundary_helper=str(HELPER),start=0,end=2701),boundary,root,list(direct.direct.query_units(ranges)),ranges,rows,lambda *a,**k:None)
                self.assertEqual(rows[0]['price'],100)
                self.assertEqual(rows[0]['integral_covered_ms'],1000)
                self.assertEqual(rows[0]['integral'],100)
                self.assertEqual(rows[1]['price'],103)
                self.assertAlmostEqual(rows[1]['integral'],162.5)
                # 独立历史左连接仍需明确处理共享可见时刻，不借当前报价覆盖掩盖并列。
                db.execute('DROP TABLE targets')
                db.execute('CREATE TABLE targets AS SELECT 0 AS id,2300::BIGINT AS at,1000::BIGINT AS window_start')
                db.execute('CREATE TABLE history AS SELECT * FROM (VALUES (900,900,100.0),(1500,2200,101.0),(1900,2200,103.0)) h(received,visible_at,price)')
                db.execute('CREATE TABLE price_pairs AS SELECT *,lag(received) OVER(ORDER BY received) AS left_received,lag(price) OVER(ORDER BY received) AS left_price,lead(received) OVER(ORDER BY received) AS next_received,lead(visible_at) OVER(ORDER BY received) AS next_visible_at FROM history')
                r=row(2300);r.update(end=31000,rule_window_s=30)
                direct.sample_spot(db,[r],[0]);self.assertEqual(r['price'],103)
            finally:boundary.close();db.close()

    def test_late_initial_rule_and_later_change_keep_first_temporary(self):
        with tempfile.TemporaryDirectory() as t:
            root=Path(t);path=root/'twap.parquet'
            schema=pa.schema([(k,pa.timestamp('ms') if k in ('timestamp_received','payload_timestamp','collector_admission_time_utc_ms')
                else pa.int64() if k in ('window_s','collector_admission_sequence') else pa.string()) for k in direct.PRICE_COLUMNS['polymarket_rtds_chainlink_twap'].split(',')])
            facts=[dict(timestamp_received=at,payload_timestamp=at,collector_admission_time_utc_ms=at,
                freshness_status='fresh',window_s=w,source_sequence_epoch_id='twap-1',collector_admission_sequence=i+1,full_accuracy_value=str(p*10**18))
                for i,(at,w,p) in enumerate([(1000,60,100),(6000,60,101),(7600,30,200),(25000,30,201)])]
            pq.write_table(pa.Table.from_pylist(facts,schema=schema),path)
            files=[dict(path=str(path),source_kind='polymarket_rtds_chainlink_twap',schema_version='polymarket_rtds_chainlink_twap_v1',bytes=path.stat().st_size,rows=4)]
            segments=[dict(at=5500,reference='chainlink_twap_sixty_seconds'),dict(at=7000,reference='chainlink_twap_thirty_seconds')]
            rows=[]
            for at in (5000,8000,20000):
                r=row(at);r.update(start=0,end=31000,reference=None,reference_kind='missing',
                    rule_window_s=None if at<5500 else 30,rule_visible=None if at<5500 else 7000,
                    rule_segments=segments,market_known_at=0)
                rows.append(r)
            db=duckdb.connect();boundary=direct.Boundary(HELPER)
            try:
                ranges=direct.direct.range_index(db,files,lambda *a,**k:None)
                cfg=dict(files=files,boundary_helper=str(HELPER),start=0,end=31001)
                direct.source_sampling(db,cfg,boundary,root,list(direct.direct.query_units(ranges)),ranges,rows,lambda *a,**k:None)
                self.assertEqual([r['reference'] for r in rows],[None,100,100])
                self.assertIsNone(rows[1]['integral'])
                self.assertEqual(rows[1]['price'],200)
                self.assertIsNone(rows[1]['innovation'])
            finally:boundary.close();db.close()

    def test_price_samples_cross_unit_carry_and_new_native_order(self):
        # 同一跨单元入口覆盖TWAP与成交；旧事实可见，新单元不能回填过去。
        for kind in ('twap','trade'):
            with self.subTest(kind=kind):
                db=duckdb.connect()
                try:
                    db.execute('CREATE TABLE targets AS SELECT * FROM (VALUES (0,1500),(1,2000),(2,2500)) t(id,"at")')
                    db.execute('CREATE TABLE probability_targets AS SELECT id,60 AS rule_window_s FROM targets')
                    db.execute("CREATE TABLE normalized AS SELECT * FROM (VALUES ('100',100.0),('101',101.0),('102',102.0)) n(raw,price)")
                    # effective_common是接纳后的窄关系；只构造该入口所需合法字段。
                    expression='price' if kind=='trade' else 'full_accuracy_value'
                    extra='2.0 AS quantity,false AS buyer_is_maker' if kind=='trade' else '60 AS window_s'
                    db.execute(f"CREATE TABLE effective_common AS SELECT 1000 AS clock,epoch_ms(1000::BIGINT) AS timestamp_received,900 AS event,'100' AS {expression},1 AS order_id,{extra}")
                    carry=direct.price_samples(db,[],[],[],kind)
                    self.assertEqual(carry[0]['at'],1000)
                    self.assertEqual(carry[0]['order_id'],0)
                    db.execute('DROP TABLE effective_common')
                    extra='q AS quantity,m AS buyer_is_maker' if kind=='trade' else '60 AS window_s'
                    db.execute(f"CREATE TABLE effective_common AS SELECT 2000 AS clock,epoch_ms(2000::BIGINT) AS timestamp_received,1900 AS event,p AS {expression},o AS order_id,{extra} FROM (VALUES ('101',1,3.0,false),('102',2,4.0,true)) v(p,o,q,m)")
                    rows=[row(at) for at in (1500,2000,2500)]
                    for r in rows:r['rule_window_s']=60
                    carry=direct.price_samples(db,rows,[0,1,2],carry,kind)
                    prefix='trade' if kind=='trade' else 'twap'
                    self.assertEqual([r[prefix+'_price'] for r in rows],[100,102,102])
                    self.assertEqual([r[prefix+'_received'] for r in rows],[1000,2000,2000])
                    self.assertEqual([r[prefix+'_event_age_ms'] for r in rows],[600,100,600])
                    self.assertEqual(carry[0]['price'],102)
                    self.assertEqual(carry[0]['order_id'],0)
                    if kind=='trade':
                        self.assertEqual([r['trade_quantity'] for r in rows],[2,4,4])
                        self.assertEqual([r['buyer_is_maker'] for r in rows],[False,True,True])
                    # 无新消息的下一单元继续保留原事实，不以空关系清掉carry。
                    db.execute('DELETE FROM effective_common')
                    self.assertEqual(direct.price_samples(db,[],[],carry,kind),carry)
                finally:db.close()

    def test_depth_sql_typed_application_overwrite_zero_and_gap(self):
        with tempfile.TemporaryDirectory() as t:
            root=Path(t);base=dict(symbol='BTCUSDT',source_sequence_epoch_id='depth-1',connection_generation=1,
                collector_host_id='collector-1',collector_region='test')
            snap=dict(base,timestamp_received=2001,snapshot_fetch_start_utc=900,last_update_id=100,
                bids='[["100","1"],["99","2"]]',asks='[["101","1"]]',snapshot_status='complete',
                source_endpoint='https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=1000',schema_version='binance_depth_snapshot_v3',
                depth_ingress_sequence=1,collector_admission_sequence=1,collector_admission_time_utc_ms=2101,source_kind='binance_depth_snapshot',clock=2101,order_id=1)
            update=dict(base,timestamp_received=2002,event_type='depthUpdate',event_time=1502,first_update_id=101,final_update_id=101,
                bids='[["100","0"],["99","4"]]',asks='[]',stream='btcusdt@depth',snapshot_last_update_id=100,local_u_before=None,
                stitch_verdict='applied',stitch_evidence='continuous_depth_update',stitch_gap_reason=None,
                source_endpoint='wss://stream.binance.com:9443/ws/btcusdt@depth',schema_version='binance_depth_update_v4',
                depth_ingress_sequence=2,collector_admission_sequence=2,collector_admission_time_utc_ms=2102,source_kind='binance_depth_update',clock=2102,order_id=2)
            gap=dict(update,timestamp_received=2003,event_time=1503,first_update_id=105,final_update_id=105,local_u_before=101,
                bids='[["99","5"]]',stitch_verdict='not_applied_confirmed_hole',stitch_evidence='U > local_u + 1',stitch_gap_reason='U > local_u + 1',
                depth_ingress_sequence=3,collector_admission_sequence=3,collector_admission_time_utc_ms=2103,clock=2103,order_id=3)
            conflict=dict(gap,local_u_before=999,stitch_verdict='applied',stitch_evidence='continuous_depth_update',stitch_gap_reason=None)
            delta=dict(conflict,timestamp_received=2004,event_time=1504,first_update_id=106,final_update_id=106,local_u_before=105,
                depth_ingress_sequence=4,collector_admission_sequence=4,collector_admission_time_utc_ms=2104,clock=2104,order_id=4)
            recovery=dict(snap,timestamp_received=2005,last_update_id=200,depth_ingress_sequence=5,collector_admission_sequence=5,
                collector_admission_time_utc_ms=2105,clock=2105,order_id=5)
            facts=[snap,update,conflict,delta,recovery];keys=sorted(set().union(*(r.keys() for r in facts)));fields=[]
            for k in keys:
                values=[r.get(k) for r in facts];value=next((v for v in values if v is not None),None)
                kind=pa.timestamp('ms') if k in ('timestamp_received','snapshot_fetch_start_utc','event_time','collector_admission_time_utc_ms') else pa.int64() if isinstance(value,int) else pa.string()
                fields.append((k,kind))
            path=root/'depth.parquet';pq.write_table(pa.Table.from_pylist(facts,schema=pa.schema(fields)),path)
            db=duckdb.connect();db.execute('CREATE VIEW effective_common AS SELECT * FROM read_parquet('+direct.literal(path)+')')
            db.execute('CREATE TABLE targets AS SELECT * FROM (VALUES (0,2102),(1,2103),(2,2104),(3,2105)) t(id,"at")')
            boundary=direct.Boundary(HELPER);rows=[row(at) for at in (2102,2103,2104,2105)]
            try:
                direct.depth_unit(db,boundary,root,list(range(4)),rows,[])
                self.assertEqual(rows[0]['depth_total'],5)
                self.assertEqual(rows[0]['depth_pressure'],.6)
                self.assertIsNone(rows[1].get('depth_total'));self.assertIsNone(rows[2].get('depth_total'))
                self.assertEqual(rows[3]['depth_total'],4)
                self.assertIn('stitch_coordinates',rows[1]['depth_missing']['reason'])
                self.assertEqual(rows[1]['depth_missing']['start'],2103)
                self.assertEqual(rows[1]['depth_missing']['end'],2105)
                for r in rows:r.update(spot_price=101,spot_received=r['at'],trade_price=105)
                model.innovation_steps(rows)
                self.assertTrue(all(r['innovation'] is not None for r in rows))
                self.assertEqual(evaluation.score(rows,model.Candidate(15,1,0),1)['scored'],4)
                compared=evaluation.evaluate(rows,[model.Candidate(15,1,0,'depth_pressure',.5)],1)[0]
                self.assertEqual(compared['score']['scored'],2)
                self.assertEqual(compared['paired_baseline']['scored'],2)
            finally:boundary.close();db.close()
            # 经过实际helper类型解析及applied更新后，采样仍保留精确大数量。
            large=dict(update,bids='[["100","79228162514264337593543950335"],["99","79228162514264337593543950335"]]')
            a,b=root/'large-depth.jsonl',root/'large-depth-samples.jsonl'
            lines=[dict(at=fact['clock'],sample=False,kind=fact['source_kind'],
                row={k:fact[k] for k in direct.DEPTH_COLUMNS[fact['source_kind']].split(',')}) for fact in (snap,large)]
            lines.append(dict(at=2102,sample=True))
            a.write_text(''.join(json.dumps(line)+'\n' for line in lines))
            boundary=direct.Boundary(HELPER)
            try:
                boundary.call(op='depth',input=str(a),output=str(b))
                quantity=json.loads(b.read_text())['bid']
                self.assertEqual(quantity,dict(numerator='158456325028528675187087900670',denominator='1'))
            finally:boundary.close()

    def test_actual_rust_census_public_serde_and_late_identity(self):
        fixture=ROOT/'tests/probability/fixtures'
        config=dict(start=3000,end=903001)
        condition='0x'+'11'*32
        before=direct.read_market_facts(fixture/'public-market-scenario.before-identity.jsonl')
        markets,anchors,rules,labels,census=before
        self.assertNotIn(condition,markets)
        self.assertEqual(anchors[condition][0],dict(at=3301,value=100.0))
        self.assertEqual(rules[condition][0],dict(at=3301,reference='chainlink_twap_sixty_seconds'))
        self.assertEqual(labels[condition],[])
        rows=direct.target_rows(config,*before)
        targets=[r for r in rows if r['condition_id']==condition]
        self.assertEqual(len(targets),8)
        self.assertTrue(all(not r['identity_known'] and r['raw_target'] and not r['public_target'] for r in targets))
        self.assertTrue(all(model.predict(r,model.Candidate(15,1,0),1)[1] in ('missing_settlement_rule','missing_settlement_price') for r in targets))
        summary=direct.census_coverage(config,census,markets,rows)
        raw=next(v for v in summary['markets'] if v['condition_id']==condition)
        self.assertIsNone(raw['identity_at']);self.assertTrue(raw['identity_gap']['right_censored'])
        self.assertEqual(raw['missing_identity_snapshot']['rows'],1)
        self.assertEqual(summary['raw_locatable_targets'],16);self.assertEqual(summary['public_targets'],8)
        self.assertGreater(targets[0]['raw_first_seen_at'],targets[0]['at'])
        after=direct.read_market_facts(fixture/'public-market-scenario.jsonl')
        markets,anchors,rules,labels,census=after
        self.assertEqual(markets[condition]['known_at'],304001)
        self.assertEqual(labels[condition][0]['at'],304001);self.assertEqual(labels[condition][0]['label'],1)
        rows=direct.target_rows(config,*after)
        self.assertTrue(all(not r['identity_known'] for r in rows if r['condition_id']==condition))
        summary=direct.census_coverage(config,census,markets,rows)
        raw=next(v for v in summary['markets'] if v['condition_id']==condition)
        self.assertEqual(raw['raw_target_count'],8);self.assertEqual(raw['public_target_count'],8)
        self.assertEqual(raw['census_identity_conflicts'][0]['raw']['market']['clob_market_id'],'999')
        self.assertEqual(raw['census_identity_conflicts'][0]['public']['identity']['clob_market_id'],'123')
        self.assertEqual(raw['raw_variants'][0]['market']['clob_market_id'],'999')
        self.assertFalse(raw['identity_gap']['right_censored'])
        # 有效市场窗口仍按公共身份；原始多窗口的目标数量必须保持未知。
        ambiguous=copy.deepcopy(census);variant=copy.deepcopy(ambiguous[condition]['variants'][0])
        variant['market']['window']['start_at_utc_ms']+=1000
        ambiguous[condition]['variants'].append(variant)
        rows=direct.target_rows(config,markets,anchors,rules,labels,ambiguous)
        summary=direct.census_coverage(config,ambiguous,markets,rows)
        self.assertEqual(summary['raw_target_count_unknown_markets'],1)
        raw=next(v for v in summary['markets'] if v['condition_id']==condition)
        self.assertIsNone(raw['raw_target_count']);self.assertEqual(raw['public_target_count'],8)

    def test_study_freezes_before_evaluation_and_writes_fixed_bins(self):
        from probability import cli
        import hashlib
        with tempfile.TemporaryDirectory() as t:
            root=Path(t);material=root/'material';material.mkdir();cfg=root/'config.json'
            config=dict(run_root=str(root),start=0,end=18*evaluation.DAY,engine=dict(threads=2,memory_limit='128MiB',max_temp_directory_size='128MiB'),progress_interval_seconds=30)
            cfg.write_text(json.dumps(config));(material/'material.json').write_text(json.dumps(dict(config_sha256=hashlib.sha256(cfg.read_bytes()).hexdigest())))
            rows=[]
            for d in range(18):
                r=row(d*evaluation.DAY+1);r.update(day=d,identity_known=True,integral=0,trade_price=None,depth_total=None,
                    labels=[dict(at=d*evaluation.DAY+2,label=1,quality='fresh')]);rows.append(r)
            (material/'samples.json').write_text(json.dumps(rows))
            (material/'fit-pairs.json').write_text(json.dumps([dict(cutoff=d*evaluation.DAY,pairs=1,seconds=1,squared_change=1) for d in (3,6,9,12,15)]))
            original=evaluation.evaluate;calls=[]
            def check_frozen(*args,**kwargs):
                fold=len(calls);self.assertTrue((root/'study'/f'fold-{fold}.frozen.json').is_file());calls.append(fold)
                return original(*args,**kwargs)
            with patch.object(evaluation,'evaluate',check_frozen),patch.object(cli,'Progress'):
                cli.study(cfg)
            self.assertEqual(len(calls),5)
            result=json.loads((root/'study/fold-0.validation.json').read_text())['results'][0]
            bins=[b for b in result['score']['strata'] if b['axis']=='calibration']
            self.assertEqual(len(bins),10);self.assertTrue(any(b['mean_probability'] is None for b in bins))
            self.assertTrue((root/'study/fold-0.candidate-0.calibration.svg').is_file())
            self.assertTrue((root/'study/report.md').is_file())

    def test_public_temporary_predicate_half_open_and_e18(self):
        with tempfile.TemporaryDirectory() as t:
            root=Path(t);boundary=direct.Boundary(HELPER)
            try:
                cmd=dict(op='temporary',reference='chainlink_twap_sixty_seconds',start=1000,end=61000,
                    window_s=60,event=1000,received=1000,quality='stale')
                self.assertTrue(boundary.call(**cmd)['eligible'])
                cmd['event']=61000;self.assertFalse(boundary.call(**cmd)['eligible'])
                cmd['event']=1000;cmd['received']=999;self.assertFalse(boundary.call(**cmd)['eligible'])
                a,b=root/'input.jsonl',root/'out.jsonl'
                a.write_text(json.dumps(dict(raw='100123456789012345678901',e18=True))+'\n')
                boundary.call(op='normalize',input=str(a),output=str(b))
                self.assertEqual(json.loads(b.read_text())['value'],'100123.456789012345678901')
            finally:boundary.close()

if __name__=='__main__':unittest.main()
