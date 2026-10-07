"""Current direct SQL assertions from the historical health tests. Tiny legal facts only."""
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
from source_health import intervals, statistics, direct_study

START=1788912000000
CONDITION='0x'+'11'*32


def material(root, changes, start=START, end=START+30, rules=()):
    schema=pa.schema([('source',pa.string()),('condition_id',pa.string()),('asset',pa.string()),('visible_at',pa.int64()),('received',pa.int64()),('event',pa.int64()),('sync_required',pa.bool_()),('available',pa.bool_()),('sync_unknown',pa.bool_()),('generation_unknown',pa.bool_())])
    pq.write_table(pa.Table.from_pylist(changes,schema=schema),root/'changes.parquet')
    pq.write_table(pa.Table.from_pylist([dict(condition_id=CONDITION,assets=['1','2'],window_start=start,window_end=end,known_at=start-10,dependency_received=start-10)],schema=pa.schema([('condition_id',pa.string()),('assets',pa.list_(pa.string())),('window_start',pa.int64()),('window_end',pa.int64()),('known_at',pa.int64()),('dependency_received',pa.int64())])),root/'markets.parquet')
    pq.write_table(pa.Table.from_pylist(list(rules),schema=pa.schema([('condition_id',pa.string()),('visible_at',pa.int64()),('sequence',pa.uint64()),('reference',pa.int32())])),root/'rules.parquet')
    pq.write_table(pa.Table.from_pylist([],schema=schema),root/'arrivals.parquet')


def change(visible_at, received, event, asset='1', source='polymarket', sync=False):
    return dict(source=source,condition_id=CONDITION if source=='polymarket' else '',asset=asset if source=='polymarket' else '',visible_at=visible_at,received=received,event=event,sync_required=sync,available=True,sync_unknown=False,generation_unknown=False)


def candidate(normal=0, maximum=10):
    return dict(identifier='manual',parameters={g:dict(normal_s=normal,normal_a=normal,max_s=maximum,max_a=maximum) for g in ('polymarket','chainlink','binance_trade','binance_depth')})



def load_fixture(db, root, start, end, trade_enabled, depth_enabled):
    # Legal tiny facts drive the current query-unit/selection path. No legacy evaluator.
    for table in ('changes','markets','rules','arrivals'):
        db.execute(f"CREATE TABLE {table} AS SELECT * FROM read_parquet({projection.literal(root/(table+'.parquet'))})")
    intervals.make_selection(db,start,end,trade_enabled,depth_enabled)
    db.execute(f"COPY selections TO {projection.literal(root/'selections.parquet')} (FORMAT PARQUET)")
    db.execute('DROP TABLE selections')
    direct_study.setup(db,dict(start=start,end=end,files=[],binance_trade_enabled=trade_enabled,binance_depth_enabled=depth_enabled),root)
    direct_study.unit_intervals(db,'polymarket',start,end)
    # unit_intervals operates per source. Fixture tests can join the independent
    # source intervals after the same production query has formed each one.
    db.execute('ALTER TABLE selected_intervals RENAME TO fixture_selected')
    db.execute('ALTER TABLE causal_intervals RENAME TO fixture_causal')
    db.execute('ALTER TABLE intervals RENAME TO fixture_intervals')
    db.execute('DROP TABLE points');db.execute('DROP TABLE final_points')
    for source in ('chainlink_spot','chainlink_twap60','binance_trade','binance_depth'):
        direct_study.unit_intervals(db,source,start,end)
        for table in ('selected_intervals','causal_intervals','intervals'):
            target={'selected_intervals':'fixture_selected','causal_intervals':'fixture_causal','intervals':'fixture_intervals'}[table]
            db.execute(f'INSERT INTO {target} SELECT * FROM {table}')
            db.execute(f'DROP TABLE {table}')
        db.execute('DROP TABLE points');db.execute('DROP TABLE final_points')
    for table,target in [('fixture_selected','selected_intervals'),('fixture_causal','causal_intervals'),('fixture_intervals','intervals')]:
        db.execute(f'ALTER TABLE {table} RENAME TO {target}')


def evaluate_fixture(db, limits, start, end, name, output):
    evaluation=direct_study.Evaluation(db,limits,start,end,name,output)
    for source in ('polymarket','chainlink_spot','chainlink_twap60','binance_trade','binance_depth'):
        evaluation.consume(source,start,end)
    return evaluation.result()

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
                db=duckdb.connect();load_fixture(db,root,a,b,False,False)
                report=evaluate_fixture(db,candidate(),a,b,'manual',root)
                whole=[r for r in report['states'] if r['asset']=='1' and r['state']=='exceeded']
                self.assertEqual(whole[0]['episodes'],1)
                self.assertEqual(whole[0]['duration_ms'],10)
                day=[r for r in report['daily_states'] if r['asset']=='1' and r['state']=='exceeded']
                self.assertEqual(day[0]['transitions'],1)
                self.assertEqual(day[0]['left_censored_fragments'],0)
                triggers=pq.read_table(root/'manual.exceeded').to_pylist()
                trigger=[r for r in triggers if r['asset']=='1'][0]
                self.assertEqual(trigger['crossed_without_new_message'],not future)
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
            db=duckdb.connect();load_fixture(db,root,a,b,True,True)
            direct_study.histogram_accumulator(db)
            direct_study.accumulate(db);direct_study.finish_histograms(db,root)
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
            db=duckdb.connect();load_fixture(db,root,START,START+30,False,False)
            report=evaluate_fixture(db,candidate(0,0),START,START+30,'zero',root)
            self.assertEqual([r['duration_ms'] for r in report['states'] if r['asset']=='1' and r['state']=='exceeded'],[30])
            self.assertEqual([r['duration_ms'] for r in report['states'] if r['asset']=='2' and r['state']=='unknown_business_progress'],[30])
            db.close()


class BoundaryTests(unittest.TestCase):
    def test_actual_pre_window_fact_is_not_new_fact_back_projection(self):
        for same_time_new in (False,True):
            with self.subTest(new=same_time_new),tempfile.TemporaryDirectory() as tmp:
                root=Path(tmp);rows=[change(START-20,START-20,START-20)]
                if same_time_new:
                    rows.append(change(START,START,START))
                material(root,rows)
                db=duckdb.connect();load_fixture(db,root,START,START+30,False,False)
                report=evaluate_fixture(db,candidate(),START,START+30,'boundary',root)
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
            db=duckdb.connect();load_fixture(db,root,START,START+30,False,False)
            report=evaluate_fixture(db,candidate(),START,START+30,'same-ms',root)
            exceeded=[r for r in report['states'] if r['source']=='chainlink_twap60' and r['state']=='exceeded'][0]
            self.assertEqual((exceeded['episodes'],exceeded['duration_ms'],exceeded['longest_episode_ms']),(1,20,20))
            db.close()
            # A true positive-time period selecting TWAP30 is outside calibration.
            pq.write_table(pa.Table.from_pylist(rules[:2]+[dict(condition_id=CONDITION,visible_at=START+25,sequence=3,reference=8)],schema=pq.read_schema(root/'rules.parquet')),root/'rules.parquet')
            db=duckdb.connect();load_fixture(db,root,START,START+30,False,False)
            report=evaluate_fixture(db,candidate(),START,START+30,'gap',root)
            exceeded=[r for r in report['states'] if r['source']=='chainlink_twap60' and r['state']=='exceeded'][0]
            self.assertEqual((exceeded['episodes'],exceeded['duration_ms']),(2,15))
            self.assertEqual((exceeded['left_censored_episodes'],exceeded['right_censored_episodes']),(1,2))
            db.close()
            # A one-ms causal probe must not fill a real one-ms unselected gap.
            pq.write_table(pa.Table.from_pylist(rules[:2]+[dict(condition_id=CONDITION,visible_at=START+21,sequence=3,reference=8)],schema=pq.read_schema(root/'rules.parquet')),root/'rules.parquet')
            db=duckdb.connect();load_fixture(db,root,START,START+30,False,False)
            report=evaluate_fixture(db,candidate(),START,START+30,'one-ms-gap',root)
            exceeded=[r for r in report['states'] if r['source']=='chainlink_twap60' and r['state']=='exceeded'][0]
            self.assertEqual((exceeded['episodes'],exceeded['duration_ms'],exceeded['left_censored_episodes'],exceeded['right_censored_episodes']),(2,19,1,2))
            db.close()
            # A newly visible old timestamp cannot label the time before reselect.
            pq.write_table(pa.Table.from_pylist(rules[:2]+[dict(condition_id=CONDITION,visible_at=START+25,sequence=3,reference=8)],schema=pq.read_schema(root/'rules.parquet')),root/'rules.parquet')
            source_rows=rows+[change(START+20,START+20,START+20,source='chainlink_twap60'),change(START+25,START+24,START,source='chainlink_twap60')]
            pq.write_table(pa.Table.from_pylist(source_rows,schema=pq.read_schema(root/'changes.parquet')),root/'changes.parquet')
            db=duckdb.connect();load_fixture(db,root,START,START+30,False,False)
            report=evaluate_fixture(db,candidate(),START,START+30,'new-fact-at-reselect',root)
            exceeded=[r for r in report['states'] if r['source']=='chainlink_twap60' and r['state']=='exceeded'][0]
            self.assertEqual((exceeded['episodes'],exceeded['duration_ms'],exceeded['left_censored_episodes'],exceeded['right_censored_episodes']),(2,15,0,2))
            db.close()

def trade_file( root, second_time=None, backwards=False):
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
                db=duckdb.connect();load_fixture(db,location,START,START+1,False,False)
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
