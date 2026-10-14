#!/usr/bin/env bash
# 参数：已验证二进制、研究配置、输出目录。业务错误后仍提取现有部分报告。
set -uo pipefail
binary=$1
config=$2
output=$3
script_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
mkdir -p -- "$output" || exit $?
# 宿主先准备原配置指定的文件父目录；错误后仍由CLI保存原启动失败。
python3 - "$config" <<'PREPARE'
import json
import sys
from pathlib import Path
config = json.loads(Path(sys.argv[1]).read_text())
for day in config.get('days', []):
    for candidate in day.get('candidates', []):
        for field in ('sim_log', 'metadata_path', 'result_path'):
            value = candidate['run'].get(field)
            if value is not None:
                Path(value).parent.mkdir(parents=True, exist_ok=True)
PREPARE
export RAYON_NUM_THREADS=${RAYON_NUM_THREADS:-16}
/usr/bin/time -v -o "$output/cli-resource-usage.txt" \
  "$binary" --mode day --config "$config" \
  --progress-interval-seconds 900 --research-summary --wall-budget-seconds 86400 \
  --checkpoint-reserve-seconds 900 > "$output/research-output.json" 2> "$output/progress.log"
cli_status=$?
python3 "$script_root/summarize_research.py" "$config" "$output/research-output.json" "$output/summary.json"
summary_status=$?
if (( cli_status != 0 )); then exit "$cli_status"; fi
exit "$summary_status"
