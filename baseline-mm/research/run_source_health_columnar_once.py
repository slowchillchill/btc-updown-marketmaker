#!/usr/bin/env python3
"""Hold one foreground prepare, then one study; no retries or background service."""
import argparse
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path


def archive_ready(root, mount_point):
    if not root.is_dir() or not os.access(root,os.R_OK|os.W_OK|os.X_OK):
        return False
    result=subprocess.run(['findmnt','-T',str(root),'-J','-o','TARGET,OPTIONS'],capture_output=True,text=True)
    if result.returncode:
        return False
    filesystems=json.loads(result.stdout)['filesystems']
    return len(filesystems)==1 and filesystems[0]['target']==str(mount_point) and 'rw' in filesystems[0]['options'].split(',')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('config',type=Path);parser.add_argument('archive_root',type=Path)
    parser.add_argument('mount_point',type=Path);parser.add_argument('logs',type=Path)
    args=parser.parse_args()
    if not archive_ready(args.archive_root,args.mount_point):
        raise RuntimeError('bound archive is unavailable or not on its expected writable mount')
    config=json.loads(args.config.read_text());run_root=Path(config['run_root'])
    if run_root.exists():
        raise RuntimeError('run root already exists; this wrapper never retries')
    args.logs.mkdir(exist_ok=False)
    entry=Path(__file__).with_name('research_source_health_columnar.py')
    for stage in ('prepare','study'):
        command=['/usr/bin/time','-v','-o',str(args.logs/(stage+'.time')),sys.executable,'-u',str(entry),stage,str(args.config)]
        (args.logs/(stage+'.command.json')).write_text(json.dumps(command,ensure_ascii=False,indent=2)+'\n')
        with (args.logs/(stage+'.stdout')).open('xb') as stdout,(args.logs/(stage+'.stderr')).open('xb') as stderr:
            process=subprocess.Popen(command,stdout=stdout,stderr=stderr,start_new_session=True)
            stopped_for_archive=False
            try:
                while process.poll() is None:
                    if not archive_ready(args.archive_root,args.mount_point):
                        stopped_for_archive=True
                        os.killpg(process.pid,signal.SIGTERM)
                        break
                    time.sleep(30)
                code=process.wait()
            except BaseException:
                if process.poll() is None:
                    os.killpg(process.pid,signal.SIGTERM)
                process.wait()
                raise
        time_text=(args.logs/(stage+'.time')).read_text()
        signalled=code<0 or 'Command terminated by signal' in time_text
        result=dict(stage=stage,returncode=code,terminated_by_signal=signalled,stopped_for_archive=stopped_for_archive)
        (args.logs/(stage+'.exit.json')).write_text(json.dumps(result,indent=2)+'\n')
        print(json.dumps(result),flush=True)
        if code or signalled or stopped_for_archive:
            return 1
        if stage=='prepare' and not (run_root/'material/material.json').is_file():
            raise RuntimeError('prepare did not publish its complete material descriptor')
    return 0


if __name__=='__main__':
    raise SystemExit(main())
