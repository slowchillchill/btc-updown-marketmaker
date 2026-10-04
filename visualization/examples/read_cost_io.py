"""汇总 read_cost 的 strace 阶段，仅统计显式首公共来源的文件读取。"""
import json
import pathlib
import re
import sys

trace, config = sys.argv[1:]
source = json.loads(pathlib.Path(config).read_text())["runs"][0]["public_recording"]
root = str(pathlib.Path(source["root"]).resolve()) + "/"
prefix = str(pathlib.Path(source["committed_prefix"]).resolve())
phase = None
result = {}
for line in pathlib.Path(trace).read_text().splitlines():
    marker = re.search(r"READ_COST_(BEGIN|END) (\w+)\\n", line)
    if marker:
        phase = marker[2] if marker[1] == "BEGIN" else None
        if phase:
            result[phase] = {"calls": 0, "returned_bytes": 0, "by_file": {}}
        continue
    read = re.match(r"(read|pread64|readv)\(\d+<([^>]+)>.* = (\d+)$", line)
    if not phase or not read or not (read[2].startswith(root) or read[2] == prefix):
        continue
    name = read[2][len(root):] if read[2].startswith(root) else "explicit-prefix"
    stage = result[phase]
    stage["calls"] += 1
    stage["returned_bytes"] += int(read[3])
    file = stage["by_file"].setdefault(name, {"calls": 0, "returned_bytes": 0})
    file["calls"] += 1
    file["returned_bytes"] += int(read[3])
if set(result) != {"initialize", "page_0", "page_1", "retry_latest"} or phase is not None:
    raise ValueError("需要 initialize、page_0、page_1、retry_latest 四个完整阶段")
print(json.dumps(result, indent=2))
