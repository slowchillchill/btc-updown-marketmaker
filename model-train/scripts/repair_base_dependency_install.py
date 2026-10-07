#!/usr/bin/env python3
"""撤回本轮误目标日志明确报告的新包；不碰原base内容或B虚拟环境。"""
import argparse
import json
from pathlib import Path
import re
import subprocess
from autodl_environment import on_data_disk,normalized


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--failed-install-log',type=Path,required=True)
    parser.add_argument('--artifact',type=Path,required=True)
    args=parser.parse_args()
    log=on_data_disk(args.failed_install_log).read_text()
    base=on_data_disk(Path('/root/autodl-tmp/tools/python-3.13.15/python/bin/python3.13'))
    expected='Using Python 3.13.15 environment at: /root/autodl-tmp/tools/python-3.13.15/python'
    if expected not in log:raise ValueError('旧日志不是本次误装base的实际记录')
    installed=re.findall(r'^Installed (\d+) packages in .*$',log,re.M)
    added=re.findall(r'^ \+ ([a-zA-Z0-9_.-]+)==([^\s]+)$',log,re.M)
    if len(installed)!=1 or len(added)!=int(installed[0]) or re.search(r'^ [-~] ',log,re.M):
        raise ValueError('旧日志不能证明完整人口均为新增，保留现场')
    names={normalized(name):version for name,version in added}
    if len(names)!=len(added):raise ValueError('旧新增人口重复')
    artifact=on_data_disk(args.artifact)
    if artifact.exists():raise ValueError('修复证据需要新目录')
    artifact.mkdir(parents=True)
    commit=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip()
    status=subprocess.check_output(['git','status','--porcelain'],text=True)
    if status:raise ValueError('修复脚本源码必须干净')
    (artifact/'source-commit.txt').write_text(commit+'\n')
    (artifact/'status.txt').write_text(status)
    def snapshot():
        return json.loads(subprocess.check_output([str(base),'-I','-c',
            'import importlib.metadata as m,json; print(json.dumps({d.metadata["Name"]:d.version for d in m.distributions()}))'],text=True))
    before=snapshot()
    canonical={normalized(name):version for name,version in before.items()}
    if any(canonical.get(name)!=version for name,version in names.items()):
        raise ValueError('本次新增包已被其它操作改变，保留现场')
    (artifact/'base-before.json').write_text(json.dumps(before,indent=2)+'\n')
    (artifact/'owned-added.json').write_text(json.dumps(names,indent=2)+'\n')
    command=['/root/autodl-tmp/tools/uv-0.12.9/uv','pip','uninstall','--python',str(base),*sorted(names)]
    (artifact/'command.json').write_text(json.dumps(command)+'\n')
    subprocess.run(command,check=True)
    after=snapshot()
    canonical_after={normalized(name):version for name,version in after.items()}
    expected_after={name:version for name,version in canonical.items() if name not in names}
    if canonical_after!=expected_after:
        raise ValueError('撤回后的其它base包变化，需要保留现场核对')
    (artifact/'base-after.json').write_text(json.dumps(after,indent=2)+'\n')
    report={'source_commit':commit,'removed_only_log_reported_new_packages':len(names),'other_base_distributions_unchanged':True,
        'venv_modified':False,'historical_base_inventory_available':False}
    (artifact/'report.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report),flush=True)


if __name__=='__main__':main()
