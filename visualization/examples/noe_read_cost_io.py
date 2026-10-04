"""仅汇总单个 v4 原引用/context/container；metadata 在 traced process 结束后读取。"""
import json
import pathlib
import re
import sys
trace, config = sys.argv[1:]
source = json.loads(pathlib.Path(config).read_text())["runs"][0]["noe_results"][2]
ref_path = pathlib.Path(source["reference_file"]).resolve()
ref = json.loads(ref_path.read_text())
container = pathlib.Path(ref["commit_container"]["location"]).resolve()
context = container.parent.parent / "context.json"
files = {str(ref_path): "writer-reference.json", str(container): "commit-container.parquet", str(context): "context.json"}
phase, result = None, {}
for line in pathlib.Path(trace).read_text().splitlines():
    marker = re.search(r"READ_COST_(BEGIN|END) (\w+)\\n", line)
    if marker:
        phase = marker[2] if marker[1] == "BEGIN" else None
        if phase:
            result[phase] = {"calls": 0, "returned_bytes": 0, "by_file": {}}
        continue
    read = re.match(r"(read|pread64|readv)\(\d+<([^>]+)>.* = (\d+)$", line)
    if not phase or not read or read[2] not in files:
        continue
    stage = result[phase]
    stage["calls"] += 1
    stage["returned_bytes"] += int(read[3])
    file = stage["by_file"].setdefault(files[read[2]], {"calls": 0, "returned_bytes": 0, "syscalls": {}})
    file["calls"] += 1
    file["returned_bytes"] += int(read[3])
    file["syscalls"][read[1]] = file["syscalls"].get(read[1], 0) + 1
if set(result) != {"first_read", "retry_latest"} or phase is not None:
    raise ValueError("需要两个完整阶段")
print(json.dumps({"file_lengths": {name: pathlib.Path(file).stat().st_size for file, name in files.items()}, "phases": result}, indent=2))
