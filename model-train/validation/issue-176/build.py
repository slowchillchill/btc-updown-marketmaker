"""串行编译两份冻结宿主，每次构建后立即保存实际二进制。"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--harnesses',type=Path,required=True)
    parser.add_argument('--build',type=Path,required=True)
    args=parser.parse_args();root=args.harnesses.resolve();build=args.build.resolve()
    binaries=root/'binaries';binaries.mkdir()
    toolchain={'rustc':subprocess.check_output(['rustc','-vV'],text=True).strip(),
        'cargo':subprocess.check_output(['cargo','--version'],text=True).strip(),
        'host':{'system':platform.system(),'release':platform.release(),'architecture':platform.machine()}}
    (root/'toolchain.json').write_text(json.dumps(toolchain,indent=2)+'\n')
    records=[]
    for variant in ('candidate','baseline'):
        manifest=root/(variant+'-harness/model-train/native-inputs/Cargo.toml')
        command=['cargo','build','--release','--locked','--offline','--features','ort','--manifest-path',str(manifest),'--example','frame_latency']
        with (root/(variant+'.log')).open('w') as log:
            result=subprocess.run(command,env=dict(os.environ,CARGO_TARGET_DIR=str(build)),stdout=log,stderr=subprocess.STDOUT)
        if result.returncode:
            print('\n'.join((root/(variant+'.log')).read_text().splitlines()[-60:]),flush=True)
            raise SystemExit(result.returncode)
        target=binaries/(variant+'-frame');shutil.copy2(build/'release/examples/frame_latency',target)
        with target.open('rb') as stream:digest=hashlib.file_digest(stream,'sha256').hexdigest()
        records.append(dict(variant=variant,binary=target.name,sha256=digest))
    (root/'binaries.json').write_text(json.dumps(records,indent=2)+'\n')
    print(json.dumps(records),flush=True)


if __name__=='__main__':main()
