"""串行编译归因与正式计时宿主；本机路径仅用于执行，不写入公开构建记录。"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--harnesses',type=Path,required=True)
    p.add_argument('--target',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True)
    p.add_argument('--variants',nargs='+',choices=['baseline','candidate'],default=['baseline','candidate'])
    p.add_argument('--modes',nargs='+',choices=['profile','formal'],default=['profile','formal'])
    a=p.parse_args();a.output.mkdir(parents=True)
    records=[]
    for variant in a.variants:
        for mode,features in [('profile','ort,bench-profile'),('formal','ort')]:
            if mode not in a.modes:continue
            name=variant+'-'+mode
            command=['cargo','build','--manifest-path','model-train/native-inputs/Cargo.toml',
                     '--release','--locked','--offline','--example','frame_latency','--features',features]
            with (a.output/(name+'.log')).open('w') as log:
                code=subprocess.call(command,cwd=a.harnesses/(variant+'-harness'),
                    env={**os.environ,'CARGO_TARGET_DIR':str(a.target.resolve()),'CARGO_BUILD_JOBS':'8'},stdout=log,stderr=subprocess.STDOUT)
            record={'binary':name,'command':command,'exit_code':code}
            if code:raise SystemExit(code)
            target=a.output/name
            shutil.copy2(a.target/'release/examples/frame_latency',target)
            with target.open('rb') as stream:record['sha256']=hashlib.file_digest(stream,'sha256').hexdigest()
            records.append(record);print(json.dumps(record),flush=True)
    (a.output/'binaries.json').write_text(json.dumps(records,indent=2)+'\n')


if __name__=='__main__':main()
