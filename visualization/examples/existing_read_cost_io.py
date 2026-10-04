"""显式来源文件列表的阶段 read/pread64/readv；不枚举目录或读取声明的 Parquet。"""
import json, pathlib, re, sys
trace,config,index=sys.argv[1:]
s=json.loads(pathlib.Path(config).read_text())["runs"][int(index)]["existing_source"]
files={str((pathlib.Path(p)/"manifest.json" if role=="manifest" else pathlib.Path(p)).resolve()):role for role,p in s["paths"].items()}
result={};phase=None
for line in pathlib.Path(trace).read_text().splitlines():
    marker=re.search(r"READ_COST_(BEGIN|END) (\w+)\\n",line)
    if marker:
        phase=marker[2] if marker[1]=="BEGIN" else None
        if phase:result[phase]={role:{"calls":0,"returned_bytes":0,"syscalls":{}} for role in files.values()}
        continue
    read=re.match(r"(read|pread64|readv)\(\d+<([^>]+)>.* = (\d+)$",line)
    if phase and read and read[2] in files:
        stage=result[phase][files[read[2]]];stage["calls"]+=1;stage["returned_bytes"]+=int(read[3]);stage["syscalls"][read[1]]=stage["syscalls"].get(read[1],0)+1
if set(result)!={"initialize","select_fragment","retry_latest"} or phase is not None:raise ValueError("需要三个完整阶段")
print(json.dumps({"kind":s["kind"],"files":{role:{"name":pathlib.Path(p).name,"file_bytes":pathlib.Path(p).stat().st_size} for p,role in files.items()},"phases":result},indent=2))
