"""Representative direct-query failures. No production/raw dataset is read."""
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from test_health import START,CONDITION,material,change,candidate,load_fixture,trade_file
from dpt_health import direct,projection
from source_health import direct_study,intervals,statistics

HELPER=Path(os.environ['BOUNDARY_HELPER'])
GENERATOR=Path(os.environ['FIXTURE_GENERATOR'])


def pm_file(root,count=16):
    path=root/'pm.parquet';subprocess.run([str(GENERATOR),str(path),str(count)],check=True)
    return path,pq.read_table(path)


def descriptor(path,rows,kind='polymarket_market',schema='polymarket_orderbook_v3'):
    return dict(path=str(path),source_kind=kind,schema_version=schema,bytes=path.stat().st_size,rows=rows)


def market_table(db,second=False):
    db.execute('CREATE TABLE markets(condition_id VARCHAR,assets VARCHAR[],window_start BIGINT,window_end BIGINT,known_at BIGINT,dependency_received BIGINT)')
    db.execute('INSERT INTO markets VALUES (?,?,?,?,?,?)',[CONDITION,['1','2'],START,START+1000,START-1,START-1])
    if second:
        db.execute('INSERT INTO markets VALUES (?,?,?,?,?,?)',['0x'+'22'*32,['3','4'],START,START+1000,START-1,START-1])


class DirectProjectionTests(unittest.TestCase):
    def test_global_pm_frontier_condition_sync_and_sparse_native_ranges(self):
        with tempfile.TemporaryDirectory() as t:
            root=Path(t);path,table=pm_file(root,4);rows=table.to_pylist()
            second='0x'+'22'*32
            for i,row in enumerate(rows):
                row.update(source_sequence_epoch_id='epoch',collector_admission_sequence=10+i,
                           timestamp_received=START+(300 if i==0 else 200),collector_admission_time_utc_ms=START+(300 if i==0 else 200))
                if i>=2:
                    row.update(market=second,asset_id=str(i+1))
            rows[-1]['collector_admission_sequence']=10**15
            pq.write_table(pa.Table.from_pylist(rows,schema=table.schema),path,row_group_size=1,use_compliant_nested_type=False)
            files=[descriptor(path,4)]
            db=duckdb.connect();market_table(db,True)
            ranges=direct.range_index(db,files,lambda *a,**k:None,2)
            units=list(direct.query_units(ranges,2));self.assertLessEqual(len(units),4)
            runner=direct.SourceQuery(db,files,HELPER,root,'polymarket')
            actual=[]
            for u in units:
                _,dirs=runner.load(u)
                actual+=db.execute('SELECT asset,visible_at,received FROM arrivals ORDER BY asset').fetchall()
                runner.keep_before(runner.frontier+1);runner.release(dirs)
                self.assertEqual(db.execute("SELECT table_name FROM duckdb_tables() WHERE temporary AND NOT internal ORDER BY table_name").fetchall(),[('carry',),('trade_carry',)])
                self.assertLessEqual(db.execute('SELECT count(*) FROM carry').fetchone()[0],4)
            self.assertEqual(actual,[('1',START+300,START+300),('2',START+300,START+200),('3',START+300,START+200),('4',START+300,START+200)])
            db.close()

    def test_pm_cross_unit_same_ms_and_true_duplicate(self):
        with tempfile.TemporaryDirectory() as t:
            root=Path(t);path,table=pm_file(root,16);files=[]
            for part,offset in enumerate(range(0,16,3)):
                chunk=table.slice(offset,3)
                for copy in range(2):
                    path=root/f'part-{part}-{copy}.parquet'
                    pq.write_table(chunk,path,use_compliant_nested_type=False)
                    files.append(descriptor(path,len(chunk)))
            db=duckdb.connect();market_table(db)
            units=list(direct.query_units(direct.range_index(db,files,lambda *a,**k:None,1),1))
            runner=direct.SourceQuery(db,files,HELPER,root,'polymarket');samples=0
            for u in units:
                _,dirs=runner.load(u);samples+=db.execute('SELECT count(*) FROM arrivals').fetchone()[0]
                runner.keep_before(runner.frontier+1);runner.release(dirs)
            self.assertEqual(samples,16);self.assertEqual(db.execute('SELECT count(*) FROM carry').fetchone()[0],2)
            db.close()

    def test_trade_time_native_duplicate_and_prefilter_epoch_order(self):
        with tempfile.TemporaryDirectory() as t:
            root=Path(t);files=trade_file(root)
            table=pq.read_table(files[0]['path']);template=table.to_pylist()[0]
            rows=[]
            for epoch,sequence,trade_id,received,event in [('A',1,1,100,10),('B',1,1,200,10),('B',2,2,400,20),('C',1,3,350,30)]:
                rows.append(dict(template,source_sequence_epoch_id=epoch,collector_admission_sequence=sequence,websocket_ingress_sequence=sequence,
                    aggregate_trade_id=trade_id,timestamp_received=START+received,collector_admission_time_utc_ms=START+received,
                    trade_time=START+event,event_time=START+event+5))
            path=Path(files[0]['path']);pq.write_table(pa.Table.from_pylist(rows,schema=table.schema),path)
            files=[descriptor(path,4,'binance_aggtrade','binance_aggtrade_v7')]
            db=duckdb.connect();runner=direct.SourceQuery(db,files,HELPER,root,'binance_trade')
            actual=[]
            for u in direct.query_units(direct.range_index(db,files,lambda *a,**k:None,1),1):
                _,dirs=runner.load(u);actual+=db.execute('SELECT event,visible_at,received FROM arrivals').fetchall()
                runner.keep_before(runner.frontier+1);runner.release(dirs)
            self.assertEqual(actual,[(START+10,START+100,START+100),(START+20,START+400,START+400),(START+30,START+400,START+350)])
            db.close()

    def test_native_filter_is_pushed_and_physical_order_not_required(self):
        with tempfile.TemporaryDirectory() as t:
            root=Path(t);path,table=pm_file(root,16)
            # Four physical groups and reverse order within each group. Queries
            # must use native coordinates, not insertion order or row ordering.
            pq.write_table(table,path,row_group_size=4,use_compliant_nested_type=False)
            files=[descriptor(path,16)];db=duckdb.connect()
            r=direct.range_index(db,files,lambda *a,**k:None,2)
            unit=next(direct.query_units(r,2));query=direct.unit_query(unit,files)[0]
            explain=db.execute('EXPLAIN '+query).fetchall()[0][1]
            self.assertIn('Filters:',explain);self.assertIn('collector_admission_sequenc',explain)
            self.assertEqual(db.execute('SELECT count(*) FROM ('+query+')').fetchone()[0],unit.hi-unit.lo)
            self.assertEqual(unit.candidate_file_rows,16)
            db.close()


class DirectStatisticsTests(unittest.TestCase):
    def test_streamed_midnight_gap_and_new_fact_match_manual_statistics(self):
        midnight=START+intervals.DAY
        for new in (False,True):
            with self.subTest(new=new),tempfile.TemporaryDirectory() as t:
                root=Path(t);a=midnight-10;b=midnight+10
                rows=[change(a,a,a)] if not new else [change(midnight-5,midnight-5,midnight-5),change(midnight,midnight-1,midnight-100)]
                material(root,rows,a,b)
                db=duckdb.connect();load_fixture(db,root,a,b,False,False)
                # Clip each algorithm unit while preserving actual causal facts.
                db.execute('ALTER TABLE selected_intervals RENAME TO full_selected')
                evaluator=direct_study.Evaluation(db,candidate(),a,b,'actual',root)
                for lower,end in [(a,midnight),(midnight,midnight+1),(midnight+1,b)]:
                    db.execute('CREATE TEMP TABLE selected_intervals AS SELECT * REPLACE(greatest(a,?) AS a,least(b,?) AS b) FROM full_selected WHERE a<? AND b>?',[lower,end,end,lower])
                    evaluator.consume('polymarket',lower,end);db.execute('DROP TABLE selected_intervals')
                actual=evaluator.result()
                exceeded=next(r for r in actual['states'] if r['asset']=='1' and r['state']=='exceeded')
                self.assertEqual((exceeded['episodes'],exceeded['duration_ms'],exceeded['transitions']),(1,10,1))
                day=next(r for r in actual['daily_states'] if r['asset']=='1' and r['state']=='exceeded')
                self.assertEqual((day['day_start'],day['transitions'],day['left_censored_fragments']),(midnight,1,0))
                db.close()

    def test_real_selection_gap_retains_both_censoring_boundaries(self):
        with tempfile.TemporaryDirectory() as t:
            root=Path(t);rules=[dict(condition_id=CONDITION,visible_at=START,sequence=1,reference=8),dict(condition_id=CONDITION,visible_at=START+20,sequence=2,reference=0),dict(condition_id=CONDITION,visible_at=START+25,sequence=3,reference=8)]
            material(root,[change(START,START,START,source='chainlink_twap60')],rules=rules)
            db=duckdb.connect();load_fixture(db,root,START,START+30,False,False)
            db.execute('ALTER TABLE selected_intervals RENAME TO full_selected')
            e=direct_study.Evaluation(db,candidate(),START,START+30,'gap',root)
            for lower,end in [(START,START+20),(START+20,START+25),(START+25,START+30)]:
                db.execute('CREATE TEMP TABLE selected_intervals AS SELECT * REPLACE(greatest(a,?) AS a,least(b,?) AS b) FROM full_selected WHERE a<? AND b>?',[lower,end,end,lower])
                e.consume('chainlink_twap60',lower,end);db.execute('DROP TABLE selected_intervals')
            rows=e.result()['states'];exceeded=next(r for r in rows if r['state']=='exceeded')
            self.assertEqual((exceeded['episodes'],exceeded['duration_ms'],exceeded['left_censored_episodes'],exceeded['right_censored_episodes']),(2,15,1,2))
            db.close()

def source_rows(db,files,root,source):
    runner=direct.SourceQuery(db,files,HELPER,root,source)
    changes=[];arrivals=[]
    for unit in direct.query_units(direct.range_index(db,files,lambda *a,**k:None)):
        _,dirs=runner.load(unit)
        changes+=db.execute('SELECT visible_at,received,event,sync_required FROM changes ORDER BY visible_at,asset').fetchall()
        arrivals+=db.execute('SELECT visible_at,received,event FROM arrivals ORDER BY visible_at,asset').fetchall()
        runner.keep_before(runner.frontier+1);runner.release(dirs)
    return changes,arrivals


class DirectValidityTests(unittest.TestCase):
    def test_trade_conflict_and_backwards_id(self):
        for backwards in (False,True):
            with self.subTest(backwards=backwards),tempfile.TemporaryDirectory() as t:
                root=Path(t);files=trade_file(root,START+2701,backwards);db=duckdb.connect()
                with self.assertRaisesRegex(ValueError,'backwards' if backwards else 'conflicting'):
                    source_rows(db,files,root,'binance_trade')
                db.close()

    def test_chainlink_anomalies_do_not_refresh_valid_times(self):
        with tempfile.TemporaryDirectory() as t:
            root=Path(t);path=root/'spot.parquet'
            rows=[dict(timestamp_received=START+i,payload_timestamp=START-1,freshness_status=q,source_native_payload_json=json.dumps({'i':i}))
                for i,q in enumerate(['fresh','invalid_type','invalid_topic','invalid_symbol','invalid_timestamp','stale'])]
            rows[4]['timestamp_received']=START+10
            schema=pa.schema([('timestamp_received',pa.timestamp('ms')),('payload_timestamp',pa.timestamp('ms')),('freshness_status',pa.string()),('source_native_payload_json',pa.string())])
            pq.write_table(pa.Table.from_pylist(rows,schema=schema),path)
            db=duckdb.connect();_,actual=source_rows(db,[descriptor(path,6,'polymarket_rtds_chainlink_btc_usd')],root,'chainlink_spot')
            self.assertEqual(actual,[(START,START,START-1),(START+10,START+5,START-1)])
            db.close()

    def test_future_epoch_and_later_old_receive_keep_visible_prefix(self):
        with tempfile.TemporaryDirectory() as t:
            root=Path(t);_,table=pm_file(root,4);rows=table.to_pylist()
            for i,row in enumerate(rows):
                row['timestamp_received']=START+(100 if i<2 else 50)
                row['collector_admission_time_utc_ms']=START+(100 if i<2 else 200)
            later=dict(rows[0],timestamp_received=START+10,collector_admission_time_utc_ms=START+150,collector_admission_sequence=3)
            for label,values in [('prefix',rows[:2]),('future',rows),('later',rows+[later])]:
                work=root/label;work.mkdir();path=work/'pm.parquet'
                pq.write_table(pa.Table.from_pylist(values,schema=table.schema),path,use_compliant_nested_type=False)
                db=duckdb.connect();market_table(db)
                changes,actual=source_rows(db,[descriptor(path,len(values))],work,'polymarket')
                self.assertEqual([r for r in actual if r[0]==START+100],[(START+100,START+100,START)]*2)
                if label!='prefix':
                    self.assertEqual([r for r in actual if r[0]==START+200],[(START+200,START+50,START)]*2)
                db.close()

    def test_conflicting_pm_coordinate_uses_real_typed_normalization(self):
        with tempfile.TemporaryDirectory() as t:
            root=Path(t);fixture,table=pm_file(root);rows=table.to_pylist();rows[4]['timestamp']=START-1
            path=root/'conflict.parquet';pq.write_table(pa.Table.from_pylist(rows,schema=table.schema),path,use_compliant_nested_type=False)
            db=duckdb.connect();market_table(db)
            with self.assertRaisesRegex(ValueError,'conflicting'):
                source_rows(db,[descriptor(fixture,16),descriptor(path,16)],root,'polymarket')
            db.close()

    def test_truncated_parquet_representative(self):
        with tempfile.TemporaryDirectory() as t:
            path=Path(t)/'bad.parquet';path.write_bytes(b'PAR1truncated');db=duckdb.connect()
            with self.assertRaises(duckdb.InvalidInputException):
                direct.range_index(db,[descriptor(path,1)],lambda *a,**k:None)
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
            files=[]
            for kind in ('binance_depth_snapshot','binance_depth_update'):
                path=root/(kind+'.parquet')
                db.execute(f"COPY raw_{kind} TO {projection.literal(path)} (FORMAT PARQUET)")
                count=db.execute(f'SELECT count(*) FROM raw_{kind}').fetchone()[0]
                db.execute(f'DROP TABLE raw_{kind}')
                files.append(descriptor(path,count,kind))
            states,_=source_rows(db,files,root,'binance_depth')
            self.assertEqual([r[3] for r in states],[True,False,False,True,True,False,False])
            self.assertEqual(states[3][1:3],(START+2,START))
            self.assertEqual(states[-1][1:3],(START+6,START))
            db.close()
