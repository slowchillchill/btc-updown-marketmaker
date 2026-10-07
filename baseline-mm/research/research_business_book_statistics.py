#!/usr/bin/env python3
"""本轮经营参数所需的完整 book 价差／顶档卖量；不重建盘口或估计自身成交。"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'data-pipeline/training/python'))
from dpt_health.direct import QueryUnit, unit_query
from dpt_health.projection import deduplicate, literal
from source_health.cli import Progress, connection, save

START = 1788912000000
CUTOFFS = [1789171200000, 1789430400000, 1789689600000, 1789948800000, 1790208000000]
COLUMNS = {'polymarket_market': 'timestamp_received,collector_admission_time_utc_ms,'
           'collector_admission_sequence,source_sequence_epoch_id,event_type,market,asset_id,bids,asks'}
SQL = Path(__file__).with_name('business_book_statistics.sql').read_text()


def unit_statistics(db, unit, files, helper, work, frontier, start=START, end=CUTOFFS[-1]):
    # book 过滤不能先于可见前沿计算；非 book 行也可能推迟下一条 book 的可见时间。
    db.execute('CREATE TEMP TABLE business_unit AS ' + ' UNION ALL BY NAME '.join(unit_query(unit, files, COLUMNS)))
    db.execute('''CREATE TEMP TABLE unit_clock AS SELECT *,
        greatest($frontier,max(greatest(epoch_ms(timestamp_received),epoch_ms(collector_admission_time_utc_ms)))
        OVER (ORDER BY collector_admission_sequence,file_id,row_id
              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS clock
        FROM business_unit''', {'frontier': frontier})
    frontier = max(frontier, db.execute('SELECT coalesce(max(clock),0) FROM unit_clock').fetchone()[0])
    db.execute("CREATE TEMP VIEW raw_books AS SELECT * FROM unit_clock WHERE event_type='book'")
    table = deduplicate(db, 'raw_books', ['market', 'source_sequence_epoch_id', 'collector_admission_sequence'],
                        helper, files, work / f'duplicates-{unit.generation}-{unit.lo}')
    db.execute('CREATE TEMP VIEW unit_books AS SELECT * FROM ' + table)
    db.execute(SQL, {'start': start, 'end': end})
    return frontier


def histogram(db, path):
    # 每个精确数值对应整数事件计数；跨文件只合并计数，不平均文件分位。
    db.execute(f'''COPY (
        SELECT day,metric,value,count(*)::BIGINT AS n FROM (
            SELECT day,'spread' AS metric,spread AS value FROM book_statistics WHERE spread IS NOT NULL
            UNION ALL SELECT day,'best_ask_shares',best_ask_shares FROM book_statistics WHERE best_ask_shares IS NOT NULL
        ) GROUP BY day,metric,value
    ) TO {literal(path)} (FORMAT PARQUET,COMPRESSION ZSTD)''')


def quantiles(db, paths, cutoff):
    # 与离散分位一致：选择累计计数首次达到 ceil(q*N) 的观测值。
    query = '''WITH h AS (
        SELECT metric,value,sum(n)::BIGINT AS n FROM read_parquet($paths)
        WHERE day*86400000 < $cutoff GROUP BY metric,value
    ), ranked AS (
        SELECT *,sum(n) OVER(PARTITION BY metric) AS total,
            sum(n) OVER(PARTITION BY metric ORDER BY value ROWS UNBOUNDED PRECEDING) AS cumulative
        FROM h
    ) SELECT metric,max(total)::BIGINT,
        min(value) FILTER(WHERE cumulative>=ceil(total*0.25)),
        min(value) FILTER(WHERE cumulative>=ceil(total*0.50)),
        min(value) FILTER(WHERE cumulative>=ceil(total*0.75))
        FROM ranked GROUP BY metric ORDER BY metric'''
    return {r[0]: dict(events=r[1],q25=r[2],q50=r[3],q75=r[4])
            for r in db.execute(query, {'paths': [str(p) for p in paths], 'cutoff': cutoff}).fetchall()}


def run(config_path, material, output):
    config = json.loads(config_path.read_text())
    output.mkdir(exist_ok=False)
    parts = output / 'histograms';parts.mkdir()
    db = connection(output / 'work', config['engine'])
    progress = Progress(db, config['progress_interval_seconds'], output / 'work')
    try:
        index = json.loads((material / 'stages/index.json').read_text())
        market = json.loads((material / 'stages/market.json').read_text())['markets']
        import pyarrow as pa
        pa.set_cpu_count(1);pa.set_io_thread_count(1)
        rows = [dict(condition_id=k,start_at=v['identity']['window']['start_at_utc_ms'],
                     end_at=v['identity']['window']['end_at_utc_ms'],
                     assets=v['asset_ids']) for k,v in market.items()]
        db.register('market_rows', pa.Table.from_pylist(rows))
        db.execute('CREATE TABLE business_markets AS SELECT * FROM market_rows');db.unregister('market_rows')
        units = [QueryUnit(**u) for u in index['units'] if u['source']=='polymarket'
                 and any(config['files'][i]['first_recv_time_utc_ms']<CUTOFFS[-1] for i in u['file_ids'])]
        frontier = 0;denominators=[];paths=[]
        for i,unit in enumerate(units):
            progress('book-unit',unit=i+1,total_units=len(units))
            frontier = unit_statistics(db, unit, config['files'], Path(config['boundary_helper']),
                                       output / 'work', frontier)
            counts = db.execute('''SELECT day,scope,count(*)::BIGINT,
                count(spread)::BIGINT,count(best_ask_shares)::BIGINT,
                count(*) FILTER(WHERE missing_bid),count(*) FILTER(WHERE missing_ask),count(*) FILTER(WHERE crossed)
                FROM book_statistics GROUP BY day,scope ORDER BY day,scope''').fetchall()
            for r in counts:
                denominators.append(dict(unit=i+1,day=r[0],scope=r[1],book_events=r[2],spread_events=r[3],
                                         ask_quantity_events=r[4],missing_bid=r[5],missing_ask=r[6],crossed=r[7]))
            path=parts / f'unit-{i+1:03}.parquet';histogram(db,path);paths.append(path)
            save(parts / f'unit-{i+1:03}.counts.json', dict(unit=i+1,frontier=frontier,counts=counts))
            progress('book-unit-saved',raw_rows=unit.candidate_file_rows,files=len(unit.file_ids),
                     unit=i+1,total_units=len(units))
            # 只保留紧凑计数和当前前沿；下一单元没有旧盘口状态。
            for name in ['unit_books','raw_books']:db.execute('DROP VIEW '+name)
            for name in ['book_statistics','unit_clock','business_unit','unique_raw_books']:
                db.execute('DROP TABLE IF EXISTS '+name)
        summary=dict(format='business-book-event-statistics-v1',start=START,end=CUTOFFS[-1],
                     config_sha256=hashlib.sha256(config_path.read_bytes()).hexdigest(),
                     units=len(units),denominators=denominators,
                     folds=[dict(fit_cutoff=c,statistics=quantiles(db,paths,c)) for c in CUTOFFS],
                     sample_unit='complete book event; not duration weighted or own fill',
                     numeric_representation='DuckDB DOUBLE descriptive values; discrete ranks from integer counts',
                     formal_adoption=False)
        save(output / 'statistics.json', summary)
        progress('book-statistics-complete',units=len(units))
    finally:
        progress.close();db.close()


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('config',type=Path);parser.add_argument('material',type=Path);parser.add_argument('output',type=Path)
    args=parser.parse_args();run(args.config,args.material,args.output)
