"""Bounded command wiring: real PM/metadata constructors, scalar study fixture."""
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
ROOT=Path(__file__).resolve().parents[2]
ENTRY=ROOT/'baseline-mm/research/research_source_health_columnar.py'
BASE=1788912000000
DAY=86400000


def run(root):
    root.mkdir(exist_ok=False)
    generator=os.environ['FIXTURE_GENERATOR'];helper=os.environ['BOUNDARY_HELPER'];statistics=os.environ['STATISTICS_HELPER']
    files=[]
    for name,argument,kind,version,rows in [('pm','16','polymarket_market','polymarket_orderbook_v3',16),('metadata','metadata','polymarket_market_metadata','polymarket_market_metadata_v6',1)]:
        path=root/(name+'.parquet');subprocess.run([generator,str(path),argument],check=True)
        files.append(dict(path=str(path),source_kind=kind,schema_version=version,bytes=path.stat().st_size,rows=rows))
    config=dict(start=BASE,end=BASE+18*DAY,files=files,boundary_helper=helper,statistics_helper=statistics,
                engine=dict(threads=4,memory_limit='1GiB',max_temp_directory_size='1GiB'),
                progress_interval_seconds=30,binance_trade_enabled=True,binance_depth_enabled=True,
                metadata_initialization=dict(files=[],markets=[],received_start_utc_ms=BASE,received_end_utc_ms=BASE+30,purpose='bounded legal empty initialization; fixture publishes its identity at start'),run_root=str(root/'prepare-run'))
    config_file=root/'prepare-config.json';config_file.write_text(json.dumps(config))
    with (root/'prepare.log').open('x') as log:
        subprocess.run([sys.executable,str(ENTRY),'prepare',str(config_file)],stdout=log,stderr=subprocess.STDOUT,check=True)
    summary=json.loads((root/'prepare-run/material/material.json').read_text());assert summary['raw_rows']==17
    # Use a new material directory. Do not alter the prepare result or imply that
    # missing raw source files supplied these deliberately constructed facts.
    study_root=root/'synthetic-study';study_root.mkdir();material=study_root/'material';material.mkdir()
    source=root/'prepare-run/material'
    for path in source.iterdir():
        (material/path.name).write_bytes(path.read_bytes())
    changes=pq.read_table(material/'changes.parquet');arrivals=pq.read_table(material/'arrivals.parquet')
    scalar_rows=[];sample_rows=[]
    for kind in ('chainlink_spot','binance_trade','binance_depth'):
        scalar_rows.append(dict(source=kind,condition_id='',asset='',visible_at=BASE,received=BASE,event=BASE,
            sync_required=False,available=True,sync_unknown=kind!='binance_depth',generation_unknown=kind=='chainlink_spot'))
        sample_rows.append(dict(source=kind,condition_id='',asset='',visible_at=BASE,received=BASE,event=BASE,sync_required=False,available=True))
    pq.write_table(pa.concat_tables([changes,pa.Table.from_pylist(scalar_rows,schema=changes.schema)]),material/'changes.parquet')
    pq.write_table(pa.concat_tables([arrivals,pa.Table.from_pylist(sample_rows,schema=arrivals.schema)]),material/'arrivals.parquet')
    config['run_root']=str(study_root);study_config=root/'study-config.json';study_config.write_text(json.dumps(config))
    summary.update(config_sha256=hashlib.sha256(study_config.read_bytes()).hexdigest(),fixture='bounded constructed scalar source facts, not a historical eighteen-day run')
    (material/'material.json').write_text(json.dumps(summary))
    with (root/'study.log').open('x') as log:
        subprocess.run([sys.executable,str(ENTRY),'study',str(study_config)],stdout=log,stderr=subprocess.STDOUT,check=True)
    folds=[p.name for p in (study_root/'study').glob('fold-*.json')]
    assert len(folds)==10
    print(json.dumps(dict(prepare_raw_rows=17,study_scalar_confirmations=19,fold_files=sorted(folds),historical_eighteen_day_run=False)))


if __name__=='__main__':
    run(Path(sys.argv[1]))
