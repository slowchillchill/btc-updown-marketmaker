"""One bounded 1/24-thread PM projection comparison, no historical replay."""
import hashlib
import json
import os
import resource
import subprocess
import sys
import time
from pathlib import Path
import duckdb
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'data-pipeline/training/python'))
from dpt_health import projection


def run(source,output,threads,helper):
    output.mkdir(exist_ok=False)
    db=duckdb.connect(str(output/'working.duckdb'))
    db.execute('SET threads=?',[threads]);db.execute("SET memory_limit='2GiB'")
    db.execute('SET temp_directory=?',[str(output/'spill')])
    file=dict(path=str(source),source_kind='polymarket_market',schema_version='polymarket_orderbook_v3',bytes=source.stat().st_size,rows=262144)
    begin=time.monotonic();cpu=time.process_time()
    projection.read_columns(db,[file],lambda *a,**k:None)
    unique=projection.deduplicate(db,'raw_polymarket_market',['market','source_sequence_epoch_id','collector_admission_sequence'],helper,[file],output/'duplicates')
    db.execute('CREATE TABLE markets(condition_id VARCHAR,assets VARCHAR[],window_start BIGINT,window_end BIGINT,known_at BIGINT,dependency_received BIGINT)')
    db.execute('INSERT INTO markets VALUES (?,?,?,?,?,?)',['0x'+'11'*32,['1','2'],1788912000000,1788912131080,1788911999999,1788911999999])
    projection.make_pm_events(db,unique)
    db.execute('CREATE TABLE common_events AS SELECT * FROM pm_events WHERE false')
    db.execute('CREATE TABLE rules(condition_id VARCHAR,visible_at BIGINT,sequence BIGINT,reference INTEGER)')
    projection.state_changes(db,output)
    elapsed=time.monotonic()-begin
    # Independent expectations come from the real fixture constructor: each row
    # confirms one asset; the source event stays old; no confirmation is lost.
    assert db.execute(f"SELECT count(*) FROM read_parquet('{output}/arrivals.parquet')").fetchone()[0]==262144
    assert db.execute(f"SELECT count(*) FROM read_parquet('{output}/changes.parquet') WHERE event!=1788912000000 OR sync_required").fetchone()[0]==0
    report=dict(threads=threads,rows=262144,wall_s=elapsed,cpu_s=time.process_time()-cpu,peak_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024,
                disk_allocated_bytes=sum(p.stat().st_blocks*512 for p in output.rglob('*') if p.is_file()),duckdb_version=duckdb.__version__)
    db.close()
    (output/'measurement.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report),flush=True)


if __name__=='__main__':
    source,output,threads,helper=sys.argv[1:]
    run(Path(source),Path(output),int(threads),Path(helper))
