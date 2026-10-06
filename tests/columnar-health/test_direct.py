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
from test_health import START,CONDITION,material,change,candidate
import test_health as previous_tests
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
            root=Path(t);files=previous_tests.TradeClockTests().trade_file(root)
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
                db=duckdb.connect();intervals.load_intervals(db,root,a,b,False,False)
                expected=statistics.evaluate(db,candidate(),a,b,'expected',root)
                # Clip each algorithm unit while preserving actual causal facts.
                db.execute('ALTER TABLE selected_intervals RENAME TO full_selected')
                evaluator=direct_study.Evaluation(db,candidate(),a,b,'actual',root)
                for lower,end in [(a,midnight),(midnight,midnight+1),(midnight+1,b)]:
                    db.execute('CREATE TEMP TABLE selected_intervals AS SELECT * REPLACE(greatest(a,?) AS a,least(b,?) AS b) FROM full_selected WHERE a<? AND b>?',[lower,end,end,lower])
                    evaluator.consume('polymarket',lower,end);db.execute('DROP TABLE selected_intervals')
                actual=evaluator.result()
                self.assertEqual(actual['states'],[r for r in expected['states'] if r['source']=='polymarket'])
                self.assertEqual(sorted(actual['daily_states'],key=lambda r:(r['asset'],r['day_start'],r['state'])),sorted([r for r in expected['daily_states'] if r['source']=='polymarket'],key=lambda r:(r['asset'],r['day_start'],r['state'])))
                db.close()

    def test_real_selection_gap_retains_both_censoring_boundaries(self):
        with tempfile.TemporaryDirectory() as t:
            root=Path(t);rules=[dict(condition_id=CONDITION,visible_at=START,sequence=1,reference=8),dict(condition_id=CONDITION,visible_at=START+20,sequence=2,reference=0),dict(condition_id=CONDITION,visible_at=START+25,sequence=3,reference=8)]
            material(root,[change(START,START,START,source='chainlink_twap60')],rules=rules)
            db=duckdb.connect();intervals.load_intervals(db,root,START,START+30,False,False)
            db.execute('ALTER TABLE selected_intervals RENAME TO full_selected')
            e=direct_study.Evaluation(db,candidate(),START,START+30,'gap',root)
            for lower,end in [(START,START+20),(START+20,START+25),(START+25,START+30)]:
                db.execute('CREATE TEMP TABLE selected_intervals AS SELECT * REPLACE(greatest(a,?) AS a,least(b,?) AS b) FROM full_selected WHERE a<? AND b>?',[lower,end,end,lower])
                e.consume('chainlink_twap60',lower,end);db.execute('DROP TABLE selected_intervals')
            rows=e.result()['states'];exceeded=next(r for r in rows if r['state']=='exceeded')
            self.assertEqual((exceeded['episodes'],exceeded['duration_ms'],exceeded['left_censored_episodes'],exceeded['right_censored_episodes']),(2,15,1,2))
            db.close()
