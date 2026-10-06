"""Read the two existing bounded outputs; do not repeat either measurement."""
import json
import sys
from pathlib import Path
import duckdb
import pyarrow.parquet as pq
root=Path(sys.argv[1]);db=duckdb.connect()
results={}
for name in ('changes','arrivals','markets','rules'):
    a=root/'pm-workers-1'/f'{name}.parquet';b=root/'pm-workers-24'/f'{name}.parquet'
    difference=db.execute(f"SELECT count(*) FROM ((SELECT * FROM read_parquet('{a}') EXCEPT ALL SELECT * FROM read_parquet('{b}')) UNION ALL (SELECT * FROM read_parquet('{b}') EXCEPT ALL SELECT * FROM read_parquet('{a}')))").fetchone()[0]
    assert difference==0
    results[name]=dict(difference_rows=difference,rows=pq.read_metadata(a).num_rows)
metadata=pq.read_metadata(root/'pm-262144.parquet')
results['input']=dict(rows=metadata.num_rows,row_groups=metadata.num_row_groups,files=1)
print(json.dumps(results,indent=2))
