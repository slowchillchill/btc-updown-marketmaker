"""同一明确公开研究 JSON 的阶段 read/pread/readv；不遍历源材料，不把文件大小当物理读。"""
import json
import pathlib
import re
import sys
trace, source = sys.argv[1:]
source = pathlib.Path(source).resolve()
result, phase = {}, None
for line in pathlib.Path(trace).read_text().splitlines():
    marker = re.search(r"READ_COST_(BEGIN|END) (\w+)\\n", line)
    if marker:
        phase = marker[2] if marker[1] == "BEGIN" else None
        if phase:
            result[phase] = {"calls": 0, "returned_bytes": 0, "syscalls": {}}
        continue
    read = re.match(r"(read|pread64|readv)\(\d+<([^>]+)>.* = (\d+)$", line)
    if phase and read and read[2] == str(source):
        stage = result[phase]
        stage["calls"] += 1
        stage["returned_bytes"] += int(read[3])
        stage["syscalls"][read[1]] = stage["syscalls"].get(read[1], 0) + 1
if set(result) != {"initialize", "select_window", "retry_latest"} or phase is not None:
    raise ValueError("需要三个完整阶段")
print(json.dumps({"source": "baseline-mm/docs/research/" + source.name, "file_bytes": source.stat().st_size, "phases": result}, indent=2))
