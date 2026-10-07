#!/usr/bin/env python3
"""AutoDL复用环境的精确差异安装；不重装Torch/CUDA/Python。"""
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from importlib import metadata
import json
from pathlib import Path
import subprocess
import time
import autodl_environment as environment


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--artifact',type=Path,required=True)
    parser.add_argument('--jobs',type=int,default=8)
    parser.add_argument('--progress-seconds',type=float,default=10)
    args=parser.parse_args()
    if args.jobs<1 or args.progress_seconds<=0:
        parser.error('并发数和进度间隔必须为正')
    environment.on_data_disk(environment.MODEL_ROOT)
    artifact=environment.on_data_disk(args.artifact)
    if artifact.exists():
        raise ValueError('差异安装证据目录必须是新目录')
    artifact.mkdir(parents=True)
    commit=subprocess.check_output(['git','rev-parse','HEAD'],cwd=environment.MODEL_ROOT.parent,text=True).strip()
    status=subprocess.check_output(['git','status','--porcelain'],cwd=environment.MODEL_ROOT.parent,text=True)
    if status:
        raise ValueError('安装前源码必须与准确提交一致')
    (artifact/'source-commit.txt').write_text(commit+'\n')
    (artifact/'status.txt').write_text(status)
    lock=environment.read_lock()
    before={environment.normalized(package.metadata['Name']):package.version for package in metadata.distributions()}
    changed=[package for package in lock['packages'] if before.get(environment.normalized(package['name']))!=package['version']]
    if any(package['name'] in {'torch','numpy'} for package in changed):
        raise ValueError('当前差异入口只补学习依赖；已有Torch/NumPy版本不符须先核对环境')
    (artifact/'installed-before.json').write_text(json.dumps(before,indent=2)+'\n')
    (artifact/'difference.json').write_text(json.dumps({'source_commit':commit,'lock_sha256':environment.digest(environment.GPU_LOCK),
        'packages':[{'name':package['name'],'before':before.get(environment.normalized(package['name'])),'after':package['version']} for package in changed]},indent=2)+'\n')
    items=[item for item in environment.artifacts({'packages':changed}) if item['directory']=='wheels']
    for item in items:
        item['download_urls']=environment.download_candidates(item['url'],None,True)
    batch=environment.DownloadBatch()
    started=time.monotonic()
    def download(item):
        try:
            return environment.download(item,environment.on_data_disk(artifact/'wheels'/item['filename']),args.progress_seconds,batch,True)
        except BaseException as error:
            batch.fail(error)
            raise
    with ThreadPoolExecutor(max_workers=args.jobs) as pool:
        futures=[pool.submit(download,item) for item in items]
        for index,future in enumerate(as_completed(futures),1):
            future.result()
            print(f'stage=download completed={index} total={len(items)} elapsed_seconds={time.monotonic()-started:.3f}',flush=True)
    (artifact/'download.json').write_text(json.dumps(items,indent=2)+'\n')
    requirements=artifact/'requirements.hashed.txt'
    requirements.write_text('\n'.join(f"{package['name']}=={package['version']} "+' '.join(
        f"--hash=sha256:{wheel['hashes']['sha256']}" for wheel in package['wheels']) for package in changed)+'\n')
    python=environment.on_data_disk(Path(__import__('sys').executable))
    uv=environment.on_data_disk(Path('/root/autodl-tmp/tools/uv-0.12.9/uv'))
    commands=[
        [str(uv),'pip','install','--python',str(python),'--offline','--no-index','--find-links',str(artifact/'wheels'),
            '--only-binary',':all:','--require-hashes','--no-deps','-r',str(requirements)],
        [str(uv),'pip','check','--python',str(python),'--offline'],
    ]
    (artifact/'commands.json').write_text(json.dumps(commands,indent=2)+'\n')
    for command in commands:
        print('stage=offline_install command='+__import__('shlex').join(command),flush=True)
        subprocess.run(command,check=True)
    after={environment.normalized(package.metadata['Name']):package.version for package in metadata.distributions()}
    for package in lock['packages']:
        if after.get(environment.normalized(package['name']))!=package['version']:
            raise ValueError('安装后版本未匹配完整目标锁：'+package['name'])
    constraints={name:metadata.requires(name) for name in ['mlflow','mlflow-skinny','mlflow-tracing','opentelemetry-proto','onnx','onnxruntime']}
    (artifact/'installed-after.json').write_text(json.dumps(after,indent=2)+'\n')
    (artifact/'shared-constraints.json').write_text(json.dumps(constraints,indent=2)+'\n')
    print(json.dumps({'source_commit':commit,'changed_packages':len(changed),'all_locked_versions_checked':True,
        'elapsed_seconds':time.monotonic()-started,'formal_training':False}),flush=True)


if __name__=='__main__':
    main()
