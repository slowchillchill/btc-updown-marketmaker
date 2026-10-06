import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/'data-pipeline/training/python'),str(ROOT/'baseline-mm/research')]
from dpt_health import projection
from source_health import intervals, statistics

START=1788912000000
CONDITION='0x'+'11'*32


def material(root, changes, start=START, end=START+30, rules=()):
    schema=pa.schema([('source',pa.string()),('condition_id',pa.string()),('asset',pa.string()),('visible_at',pa.int64()),('received',pa.int64()),('event',pa.int64()),('sync_required',pa.bool_()),('available',pa.bool_()),('sync_unknown',pa.bool_()),('generation_unknown',pa.bool_())])
    pq.write_table(pa.Table.from_pylist(changes,schema=schema),root/'changes.parquet')
    pq.write_table(pa.Table.from_pylist([dict(condition_id=CONDITION,assets=['1','2'],window_start=start,window_end=end,known_at=start-10,dependency_received=start-10)],schema=pa.schema([('condition_id',pa.string()),('assets',pa.list_(pa.string())),('window_start',pa.int64()),('window_end',pa.int64()),('known_at',pa.int64()),('dependency_received',pa.int64())])),root/'markets.parquet')
    pq.write_table(pa.Table.from_pylist(list(rules),schema=pa.schema([('condition_id',pa.string()),('visible_at',pa.int64()),('sequence',pa.uint64()),('reference',pa.int32())])),root/'rules.parquet')
    pq.write_table(pa.Table.from_pylist([],schema=pa.schema([('source',pa.string()),('condition_id',pa.string()),('asset',pa.string()),('visible_at',pa.int64()),('received',pa.int64()),('event',pa.int64())])),root/'arrivals.parquet')


def change(visible_at, received, event, asset='1', source='polymarket', sync=False):
    return dict(source=source,condition_id=CONDITION if source=='polymarket' else '',asset=asset if source=='polymarket' else '',visible_at=visible_at,received=received,event=event,sync_required=sync,available=True,sync_unknown=False,generation_unknown=False)


def candidate(normal=0, maximum=10):
    return dict(identifier='manual',parameters={g:dict(normal_s=normal,normal_a=normal,max_s=maximum,max_a=maximum) for g in ('polymarket','chainlink','binance_trade','binance_depth')})


class ResearchTests(unittest.TestCase):
    def test_midnight_threshold_and_delayed_new_fact(self):
        midnight=START+intervals.DAY
        for future in (False,True):
            with self.subTest(future=future),tempfile.TemporaryDirectory() as tmp:
                root=Path(tmp);a=midnight-10;b=midnight+10
                rows=[change(a,a,a)]
                if future:
                    rows=[change(midnight-5,midnight-5,midnight-5),change(midnight,midnight-1,midnight-100)]
                material(root,rows,a,b)
                db=duckdb.connect();intervals.load_intervals(db,root,a,b,False,False)
                report=statistics.evaluate(db,candidate(),a,b,'manual',root)
                whole=[r for r in report['states'] if r['asset']=='1' and r['state']=='exceeded']
                self.assertEqual(whole[0]['episodes'],1)
                self.assertEqual(whole[0]['duration_ms'],10)
                day=[r for r in report['daily_states'] if r['asset']=='1' and r['state']=='exceeded']
                self.assertEqual(day[0]['transitions'],1)
                self.assertEqual(day[0]['left_censored_fragments'],0)
                triggers=pq.read_table(root/'manual.exceeded.parquet').to_pylist()
                trigger=[r for r in triggers if r['asset']=='1'][0]
                self.assertEqual(trigger['crossed_without_new_message'],not future)
                db.close()

    def test_same_ms_final_state_and_samples(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);db=duckdb.connect()
            db.execute('CREATE TABLE pm_events(source VARCHAR,condition_id VARCHAR,asset VARCHAR,visible_at BIGINT,received BIGINT,event BIGINT,generation BIGINT,valid BOOLEAN,snapshot BOOLEAN,forced_sync BOOLEAN,available BOOLEAN,file_id BIGINT,row_id BIGINT,order_id BIGINT,phase INTEGER)')
            db.execute('INSERT INTO pm_events VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?),(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', ['polymarket',CONDITION,'1',START,START,START,1,True,True,False,True,0,0,1,1,'polymarket',CONDITION,'1',START,START,START-1,1,True,False,False,True,0,1,2,1])
            db.execute('CREATE TABLE common_events AS SELECT * FROM pm_events WHERE false')
            db.execute('CREATE TABLE markets(condition_id VARCHAR,assets VARCHAR[],window_start BIGINT,window_end BIGINT,known_at BIGINT,dependency_received BIGINT)')
            db.execute('CREATE TABLE rules(condition_id VARCHAR,visible_at BIGINT,sequence BIGINT,reference INTEGER)')
            projection.state_changes(db,root)
            self.assertEqual(db.execute(f"SELECT count(*),min(event) FROM read_parquet('{root}/changes.parquet')").fetchone(),(1,START-1))
            self.assertEqual(db.execute(f"SELECT count(*) FROM read_parquet('{root}/arrivals.parquet')").fetchone()[0],2)
            db.close()

    def test_exact_histogram_and_future_candidate_independence(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);a=START;b=START+18*intervals.DAY
            material(root,[change(a,a,a),*[change(a,a,a,source=s) for s in ('chainlink_spot','binance_trade','binance_depth')]],a,a+300000)
            future=a+3*intervals.DAY
            second='0x'+'22'*32
            markets=pq.read_table(root/'markets.parquet')
            second_market=dict(condition_id=second,assets=['3','4'],window_start=future,window_end=future+300000,known_at=future-10,dependency_received=future-10)
            pq.write_table(pa.concat_tables([markets,pa.Table.from_pylist([second_market],schema=markets.schema)]),root/'markets.parquet')
            changes=pq.read_table(root/'changes.parquet');row=change(future,future,future-500,asset='3');row['condition_id']=second
            pq.write_table(pa.concat_tables([changes,pa.Table.from_pylist([row],schema=changes.schema)]),root/'changes.parquet')
            db=duckdb.connect();intervals.load_intervals(db,root,a,b,True,True)
            statistics.histogram_macros(db);intervals.daily_intervals(db,'selected_intervals','selected_daily')
            statistics.histogram(db,'selected_daily','selected_histogram')
            before=statistics.fit_candidates(db,a+3*intervals.DAY,Path(os.environ['STATISTICS_HELPER']),root)
            self.assertEqual(db.execute("SELECT sum(duration_ms) FROM selected_histogram WHERE group_name='polymarket' AND metric='silence'").fetchone()[0],600000)
            self.assertGreater(db.execute('SELECT count(*) FROM selected_histogram WHERE day_start>=?',[a+3*intervals.DAY]).fetchone()[0],0)
            changed=root/'changed';changed.mkdir()
            db.execute('UPDATE selected_histogram SET duration_ms=duration_ms*1000 WHERE day_start>=?',[a+3*intervals.DAY])
            self.assertEqual(before,statistics.fit_candidates(db,a+3*intervals.DAY,Path(os.environ['STATISTICS_HELPER']),changed))
            self.assertEqual(db.execute("SELECT bin_upper(0)").fetchone()[0],100)
            self.assertEqual(db.execute("SELECT bin_upper(?)",[statistics.LAST_LINEAR]).fetchone()[0],2*intervals.DAY)
            db.close()

    def test_zero_equal_limits_and_unknown_asset_denominator(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);material(root,[change(START,START,START)])
            db=duckdb.connect();intervals.load_intervals(db,root,START,START+30,False,False)
            report=statistics.evaluate(db,candidate(0,0),START,START+30,'zero',root)
            self.assertEqual([r['duration_ms'] for r in report['states'] if r['asset']=='1' and r['state']=='exceeded'],[30])
            self.assertEqual([r['duration_ms'] for r in report['states'] if r['asset']=='2' and r['state']=='unknown_business_progress'],[30])
            db.close()


class ProjectionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.helper=Path(os.environ['BOUNDARY_HELPER'])
        cls.generator=Path(os.environ['FIXTURE_GENERATOR'])

    def test_real_pm_cross_file_duplicates_epoch_and_assets(self):
        import subprocess
        from source_health import cli
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);fixture=root/'pm.parquet'
            subprocess.run([str(self.generator),str(fixture),'16'],check=True)
            files=[dict(path=str(fixture),source_kind='polymarket_market',schema_version='polymarket_orderbook_v3',bytes=fixture.stat().st_size,rows=16)]*2
            results=[]
            for preserve in (True,False):
                work=root/str(preserve);work.mkdir()
                db=cli.connection(work/'database',dict(threads=24,memory_limit='32GiB',max_temp_directory_size='1GiB'))
                self.assertFalse(db.execute("SELECT current_setting('preserve_insertion_order')").fetchone()[0])
                if preserve:
                    db.execute('SET preserve_insertion_order=true')
                projection.read_columns(db,files,lambda *a,**k:None)
                coordinates=db.execute('SELECT file_id,row_id,collector_admission_sequence,source_sequence_epoch_id FROM raw_polymarket_market ORDER BY file_id,row_id').fetchall()
                self.assertEqual([(r[0],r[1]) for r in coordinates],[(file,row) for file in range(2) for row in range(16)])
                unique=projection.deduplicate(db,'raw_polymarket_market',['market','source_sequence_epoch_id','collector_admission_sequence'],self.helper,files,work/'duplicates')
                self.assertEqual(db.execute(f'SELECT count(*) FROM {unique}').fetchone()[0],16)
                db.execute('CREATE TABLE markets(condition_id VARCHAR,assets VARCHAR[],window_start BIGINT,window_end BIGINT,known_at BIGINT,dependency_received BIGINT)')
                db.execute('INSERT INTO markets VALUES (?,?,?,?,?,?)',[CONDITION,['1','2'],START,START+30,START-1,START-1])
                projection.make_pm_events(db,unique)
                self.assertEqual(db.execute('SELECT count(*) FROM pm_events WHERE forced_sync').fetchone()[0],2)
                db.execute('CREATE TABLE common_events AS SELECT * FROM pm_events WHERE false')
                db.execute('CREATE TABLE rules(condition_id VARCHAR,visible_at BIGINT,sequence BIGINT,reference INTEGER)')
                output=work/'material';output.mkdir();projection.state_changes(db,output)
                states=db.execute(f"SELECT asset,visible_at,received,event,sync_required FROM read_parquet('{output}/changes.parquet') ORDER BY asset,visible_at").fetchall()
                self.assertTrue(all(r[3]==START for r in states))
                self.assertTrue(all(not r[4] for r in states))
                arrivals=db.execute(f"SELECT * FROM read_parquet('{output}/arrivals.parquet') ORDER BY asset,visible_at,received,event").fetchall()
                self.assertEqual(len(arrivals),16)
                self.assertEqual(db.execute(f"SELECT sum(projected_native_rows),count(*) FROM read_parquet('{output}/epoch-ordering.parquet')").fetchone(),(16,2))
                results.append((coordinates,states,arrivals))
                db.close()
            self.assertEqual(results[0],results[1])

    def test_future_epoch_and_older_later_receive_preserve_visible_prefix(self):
        import hashlib
        import subprocess
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);original=root/'original.parquet'
            subprocess.run([str(self.generator),str(original),'4'],check=True)
            table=pq.read_table(original);rows=table.to_pylist()
            for index,row in enumerate(rows):
                row['timestamp_received']=START+(100 if index<2 else 50)
                row['collector_admission_time_utc_ms']=START+(100 if index<2 else 200)
            later=dict(rows[0],timestamp_received=START+10,collector_admission_time_utc_ms=START+150,collector_admission_sequence=3)
            results=[]
            for label,values in [('prefix',rows[:2]),('future',rows),('later-old-receive',rows+[later])]:
                path=root/(label+'.parquet');pq.write_table(pa.Table.from_pylist(values,schema=table.schema),path,use_compliant_nested_type=False)
                file=dict(path=str(path),source_kind='polymarket_market',schema_version='polymarket_orderbook_v3',bytes=path.stat().st_size,rows=len(values),sha256=hashlib.sha256(path.read_bytes()).hexdigest())
                descriptor=root/(label+'.json');descriptor.write_text(json.dumps(file))
                subprocess.run([str(self.helper),'identities',str(descriptor),str(root/(label+'.identity.jsonl'))],check=True)
                db=duckdb.connect();projection.read_columns(db,[file],lambda *a,**k:None)
                db.execute('CREATE TABLE markets(condition_id VARCHAR,assets VARCHAR[],window_start BIGINT,window_end BIGINT,known_at BIGINT,dependency_received BIGINT)')
                db.execute('INSERT INTO markets VALUES (?,?,?,?,?,?)',[CONDITION,['1','2'],START,START+1000,START-1,START-1])
                projection.make_pm_events(db,'raw_polymarket_market')
                diagnostic=root/(label+'-diagnostic');diagnostic.mkdir()
                projection.epoch_diagnostics(db,diagnostic)
                expected_bases=[START+100] if label=='prefix' else [START+100,START+200]
                self.assertEqual([r[0] for r in db.execute(f"SELECT first_available_base FROM read_parquet('{diagnostic}/epoch-ordering.parquet') ORDER BY used_order_rank").fetchall()],expected_bases)
                results.append(db.execute('SELECT asset,visible_at,received FROM pm_events WHERE valid AND visible_at=? ORDER BY asset',[START+100]).fetchall())
                if label!='prefix':
                    self.assertEqual(db.execute('SELECT max(generation) FROM pm_events').fetchone()[0],2)
                    self.assertEqual(db.execute('SELECT count(*) FROM pm_events WHERE valid AND visible_at=?',[START+200]).fetchone()[0],2)
                db.close()
            self.assertEqual(results,[ [('1',START+100,START+100),('2',START+100,START+100)] ]*3)

    def test_chainlink_anomalies_keep_old_source_and_selection(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);db=duckdb.connect()
            rows=[dict(timestamp_received=START+i,payload_timestamp=START-1,freshness_status=q,source_native_payload_json=json.dumps({'i':i}),file_id=0 if i<5 else 1,row_id=i if i<5 else 0) for i,q in enumerate(['fresh','invalid_type','invalid_topic','invalid_symbol','invalid_timestamp','stale'])]
            rows[4]['timestamp_received']=START+10
            schema=pa.schema([('timestamp_received',pa.timestamp('ms')),('payload_timestamp',pa.timestamp('ms')),('freshness_status',pa.string()),('source_native_payload_json',pa.string()),('file_id',pa.int64()),('row_id',pa.int64())])
            arrow=pa.Table.from_pylist(rows,schema=schema);db.register('spot_fixture',arrow)
            db.execute('CREATE TABLE raw_polymarket_rtds_chainlink_btc_usd AS SELECT * FROM spot_fixture')
            projection.make_common_events(db,{'polymarket_rtds_chainlink_btc_usd':True},self.helper,[],root,lambda *a,**k:None)
            self.assertEqual(db.execute('SELECT received,event FROM common_events ORDER BY order_id').fetchall(),[(START,START-1),(START+5,START-1)])
            self.assertEqual(db.execute('SELECT visible_at FROM common_events ORDER BY order_id').fetchall(),[(START,),(START+10,)])
            self.assertEqual(db.execute('SELECT count(*) FROM common_native_order').fetchone()[0],6)
            db.close()

    def test_conflicting_pm_identity_uses_real_normalization(self):
        import subprocess
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);fixture=root/'pm.parquet'
            subprocess.run([str(self.generator),str(fixture),'16'],check=True)
            raw=pq.read_table(fixture);values=raw.to_pylist()
            # A changed direct event time under the same admission coordinate is a
            # real conflict, not a same-value confirmation on a new coordinate.
            values[4]['timestamp']=START-1
            conflict=root/'conflict.parquet';pq.write_table(pa.Table.from_pylist(values,schema=raw.schema),conflict,use_compliant_nested_type=False)
            files=[dict(path=str(p),source_kind='polymarket_market',schema_version='polymarket_orderbook_v3',bytes=p.stat().st_size,rows=16) for p in (fixture,conflict)]
            db=duckdb.connect();projection.read_columns(db,files,lambda *a,**k:None)
            with self.assertRaisesRegex(ValueError,'conflicting'):
                projection.deduplicate(db,'raw_polymarket_market',['market','source_sequence_epoch_id','collector_admission_sequence'],self.helper,files,root/'duplicates')
            db.close()

    def test_truncated_parquet_representative_boundary(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);path=root/'bad.parquet';path.write_bytes(b'PAR1truncated')
            db=duckdb.connect()
            with self.assertRaises(duckdb.InvalidInputException):
                projection.read_columns(db,[dict(path=str(path),source_kind='polymarket_market',bytes=path.stat().st_size,rows=1)],lambda *a,**k:None)
            db.close()

class BoundaryTests(unittest.TestCase):
    def test_actual_pre_window_fact_is_not_new_fact_back_projection(self):
        for same_time_new in (False,True):
            with self.subTest(new=same_time_new),tempfile.TemporaryDirectory() as tmp:
                root=Path(tmp);rows=[change(START-20,START-20,START-20)]
                if same_time_new:
                    rows.append(change(START,START,START))
                material(root,rows)
                db=duckdb.connect();intervals.load_intervals(db,root,START,START+30,False,False)
                report=statistics.evaluate(db,candidate(),START,START+30,'boundary',root)
                exceeded=[r for r in report['states'] if r['asset']=='1' and r['state']=='exceeded'][0]
                self.assertEqual(exceeded['left_censored_episodes'],0 if same_time_new else 1)
                self.assertEqual(exceeded['duration_ms'],20 if same_time_new else 30)
                db.close()

    def test_source_selection_real_gap_and_same_ms_temporary_rules(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            rows=[change(START,START,START,source='chainlink_twap60')]
            rules=[dict(condition_id=CONDITION,visible_at=START,sequence=1,reference=8),
                   dict(condition_id=CONDITION,visible_at=START+20,sequence=2,reference=0),
                   dict(condition_id=CONDITION,visible_at=START+20,sequence=3,reference=8)]
            material(root,rows,rules=rules)
            db=duckdb.connect();intervals.load_intervals(db,root,START,START+30,False,False)
            report=statistics.evaluate(db,candidate(),START,START+30,'same-ms',root)
            exceeded=[r for r in report['states'] if r['source']=='chainlink_twap60' and r['state']=='exceeded'][0]
            self.assertEqual((exceeded['episodes'],exceeded['duration_ms'],exceeded['longest_episode_ms']),(1,20,20))
            db.close()
            # A true positive-time period selecting TWAP30 is outside calibration.
            pq.write_table(pa.Table.from_pylist(rules[:2]+[dict(condition_id=CONDITION,visible_at=START+25,sequence=3,reference=8)],schema=pq.read_schema(root/'rules.parquet')),root/'rules.parquet')
            db=duckdb.connect();intervals.load_intervals(db,root,START,START+30,False,False)
            report=statistics.evaluate(db,candidate(),START,START+30,'gap',root)
            exceeded=[r for r in report['states'] if r['source']=='chainlink_twap60' and r['state']=='exceeded'][0]
            self.assertEqual((exceeded['episodes'],exceeded['duration_ms']),(2,15))
            self.assertEqual((exceeded['left_censored_episodes'],exceeded['right_censored_episodes']),(1,2))
            db.close()
            # A one-ms causal probe must not fill a real one-ms unselected gap.
            pq.write_table(pa.Table.from_pylist(rules[:2]+[dict(condition_id=CONDITION,visible_at=START+21,sequence=3,reference=8)],schema=pq.read_schema(root/'rules.parquet')),root/'rules.parquet')
            db=duckdb.connect();intervals.load_intervals(db,root,START,START+30,False,False)
            report=statistics.evaluate(db,candidate(),START,START+30,'one-ms-gap',root)
            exceeded=[r for r in report['states'] if r['source']=='chainlink_twap60' and r['state']=='exceeded'][0]
            self.assertEqual((exceeded['episodes'],exceeded['duration_ms'],exceeded['left_censored_episodes'],exceeded['right_censored_episodes']),(2,19,1,2))
            db.close()
            # A newly visible old timestamp cannot label the time before reselect.
            pq.write_table(pa.Table.from_pylist(rules[:2]+[dict(condition_id=CONDITION,visible_at=START+25,sequence=3,reference=8)],schema=pq.read_schema(root/'rules.parquet')),root/'rules.parquet')
            source_rows=rows+[change(START+20,START+20,START+20,source='chainlink_twap60'),change(START+25,START+24,START,source='chainlink_twap60')]
            pq.write_table(pa.Table.from_pylist(source_rows,schema=pq.read_schema(root/'changes.parquet')),root/'changes.parquet')
            db=duckdb.connect();intervals.load_intervals(db,root,START,START+30,False,False)
            report=statistics.evaluate(db,candidate(),START,START+30,'new-fact-at-reselect',root)
            exceeded=[r for r in report['states'] if r['source']=='chainlink_twap60' and r['state']=='exceeded'][0]
            self.assertEqual((exceeded['episodes'],exceeded['duration_ms'],exceeded['left_censored_episodes'],exceeded['right_censored_episodes']),(2,15,0,2))
            db.close()

    def test_captured_depth_gap_snapshot_recovery_and_old_event(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);db=duckdb.connect()
            db.execute('CREATE TABLE raw_binance_depth_snapshot(timestamp_received TIMESTAMP_MS,last_update_id UBIGINT,snapshot_status VARCHAR,source_sequence_epoch_id VARCHAR,connection_generation UBIGINT,depth_ingress_sequence UBIGINT,collector_admission_sequence UBIGINT,collector_admission_time_utc_ms TIMESTAMP_MS,file_id BIGINT,row_id BIGINT)')
            db.execute('CREATE TABLE raw_binance_depth_update(timestamp_received TIMESTAMP_MS,event_time TIMESTAMP_MS,first_update_id UBIGINT,final_update_id UBIGINT,snapshot_last_update_id UBIGINT,local_u_before UBIGINT,stitch_verdict VARCHAR,source_sequence_epoch_id VARCHAR,connection_generation UBIGINT,depth_ingress_sequence UBIGINT,collector_admission_sequence UBIGINT,collector_admission_time_utc_ms TIMESTAMP_MS,file_id BIGINT,row_id BIGINT)')
            for seq,offset,last in [(2,1,10),(6,5,20)]:
                db.execute("INSERT INTO raw_binance_depth_snapshot VALUES(epoch_ms(?),?,'complete','epoch',1,?,?,epoch_ms(?),0,?)",[START+offset,last,seq,seq,START+offset,seq])
            for seq,offset,first,last,local,verdict in [(1,0,1,1,None,'not_applied_snapshot_unavailable'),(3,2,11,11,None,'applied'),(4,3,13,13,11,'not_applied_confirmed_hole'),(5,4,11,11,None,'not_applied_snapshot_unavailable'),(7,6,21,21,None,'applied')]:
                db.execute("INSERT INTO raw_binance_depth_update VALUES(epoch_ms(?),epoch_ms(?),?,?,?, ?,?,'epoch',1,?,?,epoch_ms(?),1,?)",[START+offset,START,first,last,None if verdict=='not_applied_snapshot_unavailable' else (10 if seq<6 else 20),local,verdict,seq,seq,START+offset,seq])
            projection.make_common_events(db,{'binance_depth_snapshot':True,'binance_depth_update':True},Path(os.environ['BOUNDARY_HELPER']),[],root,lambda *a,**k:None)
            db.execute('CREATE TABLE pm_events AS SELECT * FROM common_events WHERE false')
            db.execute('CREATE TABLE markets(condition_id VARCHAR,assets VARCHAR[],window_start BIGINT,window_end BIGINT,known_at BIGINT,dependency_received BIGINT)')
            db.execute('CREATE TABLE rules(condition_id VARCHAR,visible_at BIGINT,sequence BIGINT,reference INTEGER)')
            projection.state_changes(db,root)
            states=db.execute(f"SELECT visible_at,received,event,sync_required FROM read_parquet('{root}/changes.parquet') ORDER BY visible_at").fetchall()
            self.assertEqual([r[3] for r in states],[True,False,False,True,True,False,False])
            self.assertEqual(states[3][1:3],(START+2,START))
            self.assertEqual(states[-1][1:3],(START+6,START))
            db.close()

class TradeClockTests(unittest.TestCase):
    def trade_file(self, root, second_time=None, backwards=False):
        # Real v7 fields and legal WebSocket facts. Duplicate subsets pass the
        # existing public typed reader; E and T deliberately differ.
        timestamps={'timestamp_received','rest_backfill_timestamp_received','event_time','trade_time','collector_admission_time_utc_ms'}
        integers={'aggregate_trade_id','first_trade_id','last_trade_id','connection_generation','websocket_ingress_sequence','rest_response_ingress_sequence','rest_response_item_index','collector_admission_sequence','payload_length_bytes'}
        booleans={'buyer_is_maker','ws_ignore','rest_was_best_price_match'}
        fields='record_kind timestamp_received rest_backfill_timestamp_received ingest_source event_type event_time trade_time symbol aggregate_trade_id price quantity first_trade_id last_trade_id buyer_is_maker stream ws_ignore rest_was_best_price_match source_native_payload_json source_sequence_epoch_id connection_generation websocket_ingress_sequence rest_response_ingress_sequence rest_response_item_index collector_admission_sequence collector_admission_time_utc_ms invalid_reason_code invalid_reason_detail payload_encoding payload_length_bytes payload_sha256 payload_capture_status collector_host_id collector_region source_endpoint schema_version'.split()
        required={'record_kind','timestamp_received','ingest_source','source_native_payload_json','source_sequence_epoch_id','connection_generation','collector_admission_sequence','collector_admission_time_utc_ms','collector_host_id','collector_region','source_endpoint','schema_version'}
        schema=pa.schema([pa.field(f,pa.timestamp('ms',tz='UTC') if f in timestamps else pa.uint64() if f in integers else pa.bool_() if f in booleans else pa.string(),nullable=f not in required) for f in fields])
        rows=[]
        for index in range(2):
            row=dict(record_kind='trade',timestamp_received=START+3000+index,event_time=START+2800,
                trade_time=START+2700 if index==0 or second_time is None else second_time,
                ingest_source='websocket',event_type='aggTrade',symbol='BTCUSDT',aggregate_trade_id=2 if not backwards or index==0 else 1,
                price='60000.00' if index==0 else '60000',quantity='0.01',first_trade_id=10,last_trade_id=10,
                buyer_is_maker=False,stream='btcusdt@aggTrade',ws_ignore=True,source_native_payload_json='{"websocket":{},"rest_backfill":null}',
                source_sequence_epoch_id='trade-epoch-'+str(index),connection_generation=1,websocket_ingress_sequence=1,
                collector_admission_sequence=1,collector_admission_time_utc_ms=START+3001+index,
                collector_host_id='fixture',collector_region='test',source_endpoint='wss://stream.binance.com:9443/ws/btcusdt@aggTrade',schema_version='binance_aggtrade_v7')
            rows.append(row)
        path=root/'trade.parquet';pq.write_table(pa.Table.from_pylist(rows,schema=schema),path)
        return [dict(path=str(path),source_kind='binance_aggtrade',schema_version='binance_aggtrade_v7',bytes=path.stat().st_size,rows=2)]

    def test_trade_clock_and_native_id_survive_collector_restart(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);files=self.trade_file(root);db=duckdb.connect()
            projection.read_columns(db,files,lambda *a,**k:None)
            projection.make_common_events(db,{'binance_aggtrade':True},Path(os.environ['BOUNDARY_HELPER']),files,root,lambda *a,**k:None)
            self.assertEqual(db.execute('SELECT visible_at,received,event FROM common_events').fetchall(),[(START+3001,START+3000,START+2700)])
            db.close()

    def test_trade_time_conflict_and_backwards_id_keep_original_failures(self):
        for backwards in (False,True):
            with self.subTest(backwards=backwards),tempfile.TemporaryDirectory() as tmp:
                root=Path(tmp);files=self.trade_file(root,second_time=START+2701,backwards=backwards);db=duckdb.connect()
                projection.read_columns(db,files,lambda *a,**k:None)
                with self.assertRaisesRegex(ValueError,'backwards' if backwards else 'conflicting'):
                    projection.make_common_events(db,{'binance_aggtrade':True},Path(os.environ['BOUNDARY_HELPER']),files,root,lambda *a,**k:None)
                db.close()

    def test_duplicate_first_trade_keeps_prefilter_epoch_order_and_frontier(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);files=self.trade_file(root)
            table=pq.read_table(files[0]['path']);template=table.to_pylist()[0]
            rows=[]
            for epoch,sequence,trade_id,received,event in [('A',1,1,100,10),('B',1,1,200,10),('B',2,2,400,20),('C',1,3,350,30)]:
                row=dict(template,source_sequence_epoch_id=epoch,collector_admission_sequence=sequence,
                    websocket_ingress_sequence=sequence,aggregate_trade_id=trade_id,
                    timestamp_received=START+received,collector_admission_time_utc_ms=START+received,
                    trade_time=START+event,event_time=START+event)
                rows.append(row)
            pq.write_table(pa.Table.from_pylist(rows,schema=table.schema),files[0]['path'])
            files[0].update(rows=4,bytes=Path(files[0]['path']).stat().st_size)
            db=duckdb.connect();projection.read_columns(db,files,lambda *a,**k:None)
            projection.make_common_events(db,{'binance_aggtrade':True},Path(os.environ['BOUNDARY_HELPER']),files,root,lambda *a,**k:None)
            actual=db.execute('SELECT event,visible_at,received,generation FROM common_events ORDER BY order_id').fetchall()
            self.assertEqual(actual,[(START+10,START+100,START+100,1),(START+20,START+400,START+400,2),(START+30,START+400,START+350,3)])
            projection.epoch_diagnostics(db,root)
            diagnostics=pq.read_table(root/'epoch-ordering.parquet').to_pylist()
            self.assertEqual(sum(r['projected_native_rows'] for r in diagnostics),4)
            self.assertEqual([(r['epoch'],r['first_available_base']) for r in sorted(diagnostics,key=lambda r:r['used_order_rank'])],[('A',START+100),('B',START+200),('C',START+350)])
            db.close()


class ProductionPolicyTests(unittest.TestCase):
    def test_vectorized_labels_match_actual_production_policy(self):
        import subprocess
        cases=[(START,START,False,True,0,10),(START-5,START-5,False,True,0,10),
               (START-10,START-10,False,True,0,10),(START,START,False,True,0,0),
               (START,START+1,False,True,0,10),(START,None,False,True,0,10),
               (START,START,True,True,0,10),(START,START,False,False,0,10)]
        mapping={'SynchronizationRequired':'synchronization_required','Unavailable':'unavailable','UnknownEventTime':'unknown_event_time','TimeInconsistent':'time_inconsistent','UnknownBusinessProgress':'unknown_business_progress'}
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);source=root/'policy.jsonl';result=root/'native.jsonl';inputs=[];actual=[]
            for index,(received,event,sync,available,normal,maximum) in enumerate(cases):
                location=root/str(index);location.mkdir()
                row=change(START,received,event,sync=sync);row['available']=available
                material(location,[row],START,START+1)
                db=duckdb.connect();intervals.load_intervals(db,location,START,START+1,False,False)
                limits=candidate(normal,maximum)
                intervals.candidate_intervals(db,limits['parameters'],'selected_intervals','labelled')
                actual.append(db.execute("SELECT state FROM labelled WHERE source='polymarket' AND asset='1'").fetchone()[0]);db.close()
                inputs.append(dict(facts=dict(synchronization_required=sync,availability='fresh' if available else 'missing',last_valid_message_times=[received,event]),limits=dict(silence_normal_ms=normal,event_normal_ms=normal,silence_max_ms=maximum,event_max_ms=maximum),at=START))
            source.write_text(''.join(json.dumps(x)+'\n' for x in inputs))
            subprocess.run([os.environ['STATISTICS_HELPER'],'classify-columnar-facts',str(source),str(result)],check=True)
            expected=[]
            for line in result.read_text().splitlines():
                value=json.loads(line)
                if isinstance(value,str):expected.append(value.lower())
                elif 'Exceeded' in value:expected.append('exceeded')
                else:expected.append(mapping[value['Unavailable']])
            self.assertEqual(actual,expected)

if __name__=='__main__':
    unittest.main()
