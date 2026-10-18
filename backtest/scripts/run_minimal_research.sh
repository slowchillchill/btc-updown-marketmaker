#!/usr/bin/env bash
# 只准备输出父目录并启动已冻结的独立包；工程交付不自动调用本脚本。
set -euo pipefail
if (( $# != 1 )); then
  printf '用法: %s RUN_PACKAGE_ROOT\n' "$0" >&2
  exit 2
fi
package_root=$(realpath -- "$1")
cd -- "$package_root"
# 正式入口要求证据根尚不存在；只创建它的父目录。
python3 - <<'PY'
import json
from pathlib import Path
config=json.loads(Path('configs/research-days.json').read_text())
Path('logs').mkdir(parents=True,exist_ok=True)
Path('outputs').mkdir(parents=True,exist_ok=True)
for day in config['days']:
    Path(day['preparation']['work_root']).parent.mkdir(parents=True,exist_ok=True)
    for candidate in day['candidates']:
        run=candidate['run']
        for key in ('sim_log','noe_root','ledger_root','metadata_path','result_path'):
            Path(run[key]).parent.mkdir(parents=True,exist_ok=True)
PY
exec env RAYON_NUM_THREADS="${RAYON_NUM_THREADS:-32}" \
  /usr/bin/time -v -o logs/resource-usage.txt \
  ./bin/backtest-cli --mode day --config configs/research-days.json \
  --progress-interval-seconds 60 --research-summary \
  --research-evidence-root outputs/research-evidence \
  > logs/research-output.json 2> logs/progress.log
