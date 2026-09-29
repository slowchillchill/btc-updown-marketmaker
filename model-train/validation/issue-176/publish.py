"""公开准确宿主源码差异、运行身份与验证结果；不复制私有日志或模型。"""
import argparse
import gzip
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import re
import shutil
import subprocess
import tarfile

spec=importlib.util.spec_from_file_location('prior',Path(__file__).parent.parent/'issue-174/publish.py')
prior=importlib.util.module_from_spec(spec);spec.loader.exec_module(prior)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    for name in ('source','artifacts','harnesses','validation','continuous','output'):
        parser.add_argument('--'+name,type=Path,required=True)
    args=parser.parse_args();source=args.source.resolve();output=args.output.resolve();output.mkdir(parents=True,exist_ok=True)
    def public(value):
        if isinstance(value,str):return value.replace(str(source)+'/','').replace(str(args.artifacts.resolve())+'/','target/')
        if isinstance(value,dict):return {public(k):public(v) for k,v in value.items()}
        if isinstance(value,list):return [public(v) for v in value]
        return value
    def write(path,value):
        path.parent.mkdir(parents=True,exist_ok=True)
        text=json.dumps(public(value),ensure_ascii=False,indent=2)+'\n'
        assert '/home/' not in text and '/tmp/' not in text,path
        path.write_text(text)
    for name in ('source-identities.json','binaries.json','provenance.json','toolchain.json'):
        write(output/'source'/name,json.loads((args.harnesses/name).read_text()))
    identities=json.loads((args.harnesses/'source-identities.json').read_text());archives=[]
    for variant,files in identities['variants'].items():
        path=output/'source'/(variant+'-overlay.tar.gz');saved=[]
        with path.open('wb') as stream,gzip.GzipFile(fileobj=stream,mode='wb',filename='',mtime=0) as zipped,tarfile.open(fileobj=zipped,mode='w') as archive:
            for relative,digest in files.items():
                original=args.harnesses/(variant+'-harness')/relative;data=original.read_bytes()
                assert hashlib.sha256(data).hexdigest()==digest
                before=subprocess.run(['git','show',identities['baseline']+':'+relative],cwd=source,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL)
                if before.returncode==0 and before.stdout==data:continue
                assert b'/home/' not in data and b'/tmp/' not in data,relative
                info=tarfile.TarInfo(relative);info.size=len(data);info.mode=0o644;archive.addfile(info,io.BytesIO(data));saved.append(relative)
        archives.append({'variant':variant,'archive':path.name,'sha256':prior.sha(path),'baseline':identities['baseline'],'overlay_files':saved})
    write(output/'source/archives.json',archives)
    commands=json.loads((args.validation/'commands.json').read_text())
    write(output/'validation/commands.json',commands)
    results={}
    for command in commands:
        path=args.validation/(command['label']+'.log')
        assert command['exit_code']==0 and prior.sha(path)==command['raw_log_sha256']
        if re.search(r'^     Running ',path.read_text(),re.M):results[command['label']]=prior.test_results(path)
    write(output/'validation/results.json',results)
    write(output/'validation/ownership.json',json.loads((args.validation/'ownership.json').read_text()))
    for model in ('MAIN-1x256','MAIN-30x1024'):
        path=args.validation/(model+'-full-book.json');original=json.loads(path.read_text())
        value={k:v for k,v in original.items() if k!='provenance'}
        value['raw_evidence_sha256']=prior.sha(path)
        value['provenance']={k:original['provenance'][k] for k in ('graphs','mapped_ort','native_extension_sha256','sources','ort_sessions')}
        write(output/'validation'/(model+'-full-book.json'),value)
    order=[json.loads(line) for line in (args.continuous/'run-order.jsonl').read_text().splitlines()]
    assert len(order)==16 and all(row['exit_code']==0 for row in order)
    write(output/'continuous/run-order.json',order)


if __name__=='__main__':main()
