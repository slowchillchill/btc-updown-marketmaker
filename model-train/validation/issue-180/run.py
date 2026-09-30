"""冻结 #180/#127 三个预声明窗口并逐次运行；不重试失败或超时。"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import time
from datetime import datetime, timezone


def digest(path):
    with path.open('rb') as f:
        return hashlib.file_digest(f, 'sha256').hexdigest()


def write(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n')


def boundaries(root):
    import pyarrow.parquet as pq
    groups = [json.loads(line) for line in (root/'group-index.jsonl').open()]
    ordinary = groups[201582]
    high = groups[418584]
    end_at = ordinary['closing_received_at'] + 60_000
    # 仅冻结原 Input 的边界；此扫描在准备阶段，不进入模型负载。
    last_ordinary = None
    last_input = None
    high_watermark = -(1 << 63)
    for segment in sorted((root/'recording').glob('stable-*')):
        for parquet in sorted(segment.rglob('*.parquet')):
            for batch in pq.ParquetFile(parquet).iter_batches(batch_size=128, columns=['payload']):
                for payload in batch.column(0).to_pylist():
                    if not payload.startswith(b'{"Input":'):
                        continue
                    row = json.loads(payload)['Input']
                    ingress = row['ingress']
                    if isinstance(ingress, dict) and 'Message' in ingress:
                        high_watermark = max(high_watermark, ingress['Message']['source_evidence']['received_at_utc_ms'])
                        if high_watermark < end_at:
                            last_ordinary = row['input_position']
                    elif ingress != 'EndOfInput':
                        raise RuntimeError('原录制存在非正常终态')
                    last_input = row['input_position']
    def window(first, last, natural, end):
        chosen = [g for g in groups if first['closed_at_input_position'] <= g['closed_at_input_position'] <= last]
        return dict(first_input=first['closed_at_input_position'], last_input=last,
                    first_group=chosen[0]['group_position'], last_group=chosen[-1]['group_position'],
                    first_at=first['closing_received_at'], end_at=end, natural_end=natural,
                    group_count=len(chosen), input_count=last-first['closed_at_input_position']+1)
    return window(ordinary, last_ordinary, False, end_at), window(high, last_input, True, 1789517401000)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for name in ('source', 'diagnostic', 'binary', 'recording', 'graphs', 'checkpoints', 'warm', 'packed', 'ort-library', 'output'):
        p.add_argument('--'+name, type=Path, required=True)
    p.add_argument('--execute', action='store_true')
    a = p.parse_args()
    config = a.recording/'configuration.json'
    identities = json.loads((a.diagnostic/'source-identities.json').read_text())
    revision = identities['baseline']
    assert subprocess.check_output(['git','cat-file','-t',revision],cwd=a.source,text=True).strip() == 'commit', '正式冻结必须来自工具提交'
    assert digest(Path(__file__)) == digest(a.diagnostic/'model-train/validation/issue-180/run.py'), '必须使用冻结头所属运行脚本'
    assert digest(config) == 'ef2e712a6cf44ce31e3a3253d84d7938c2991ec74937bd02f853aed604b1c008'
    assert digest(a.recording/'record-completion.json') == 'cc3120cf4348b06dda62b77c4d101396b9a336e6f96b063eaccd31c3ee932347'
    if not a.execute:
        a.output.mkdir(parents=True)
        ordinary, high = boundaries(a.recording)
        write(a.output/'ordinary-window.json', ordinary)
        write(a.output/'high-window.json', high)
        frozen = a.output/'binary'
        shutil.copy2(a.binary, frozen)
        cfg = json.loads(config.read_text())
        files = {'configuration': digest(config), 'record-completion': digest(a.recording/'record-completion.json'),
                 'rule-snapshot': digest(Path(cfg['rule_snapshot'])), 'finality-policy': digest(Path(cfg['finality_path'])),
                 'diagnostic-patch': digest(a.diagnostic/'diagnostic.patch'), 'binary': digest(frozen),
                 'workspace-lock': digest(a.diagnostic/'Cargo.lock'), 'native-lock': digest(a.diagnostic/'model-train/native-inputs/Cargo.lock'),
                 'ort-library': digest(a.ort_library)}
        for label, folder in (('graphs', a.graphs), ('checkpoints', a.checkpoints), ('warm', a.warm), ('packed', a.packed)):
            for file in sorted(folder.rglob('*')):
                if file.is_file() and (label in ('graphs','packed') or file.name in ('entry-201582.json','entry-418584.json','warm-201582.json','warm-418584.json')):
                    files[label+'/'+str(file.relative_to(folder))] = digest(file)
        write(a.output/'freeze.json', {'production_and_tool_sha': revision, 'files': files, 'source_identities': identities,
              'conditions': {'input_queue':25000, 'dpt_recording_inputs':128, 'sim_recording_groups':128, 'rayon_threads':8,
                             'wall_observation_seconds':1200, 'process_cpu_ticks_per_second':os.sysconf('SC_CLK_TCK'), 'clock':'original finite logical time; independent wall paced TypedRaw feed'},
              'rust':subprocess.check_output(['rustc','--version'],text=True).strip(),
              'cargo':subprocess.check_output(['cargo','--version'],text=True).strip(),
              'cpu':subprocess.check_output(['lscpu'],text=True),
              'memory':Path('/proc/meminfo').read_text(), 'created_utc':datetime.now(timezone.utc).isoformat()})
        return
    freeze = json.loads((a.output/'freeze.json').read_text())
    assert freeze['production_and_tool_sha'] == revision
    assert digest(a.output/'binary') == freeze['files']['binary']
    folders={'graphs':a.graphs,'checkpoints':a.checkpoints,'warm':a.warm,'packed':a.packed}
    singles={'configuration':config,'record-completion':a.recording/'record-completion.json','diagnostic-patch':a.diagnostic/'diagnostic.patch','workspace-lock':a.diagnostic/'Cargo.lock','native-lock':a.diagnostic/'model-train/native-inputs/Cargo.lock','ort-library':a.ort_library}
    cfg=json.loads(config.read_text());singles.update({'rule-snapshot':Path(cfg['rule_snapshot']),'finality-policy':Path(cfg['finality_path'])})
    for label,expected in freeze['files'].items():
        if label=='binary':continue
        if '/' in label:
            role,relative=label.split('/',1);path=folders[role]/relative
        else:path=singles[label]
        assert digest(path)==expected, '冻结材料变化：'+label
    env = dict(os.environ, RAYON_NUM_THREADS='8', OMP_NUM_THREADS='1', MKL_NUM_THREADS='1',
               FRAME_CHECKPOINTS=str(a.checkpoints.resolve()), FRAME_WARM_CHECKPOINTS=str(a.warm.resolve()),
               FRAME_GRAPHS=str(a.graphs.resolve()), FRAME_PACKED=str(a.packed.resolve()),
               ORT_DYLIB_PATH=str(a.ort_library.resolve()), FRAME_REVISION=revision,
               FRAME_CONFIG_SHA=freeze['files']['configuration'], FRAME_BINARY_SHA=freeze['files']['binary'])
    cases = [('small-ordinary','MAIN-1x256',201582,'ordinary'), ('small-high','MAIN-1x256',418584,'high'), ('large-high','MAIN-30x1024',418584,'high')]
    for name, model, first, window in cases:
        case = a.output/name
        assert not case.exists(), '禁止覆盖或自动重复正式测量'
        command = ['/usr/bin/time','-v','-o',str(a.output/(name+'.resources.txt')),str((a.output/'binary').resolve()),str(config.resolve()),str(a.recording.resolve()),str(case.resolve()),'decisions','9999999']
        began = time.monotonic()
        exceeded = False
        window_began = None
        with (a.output/(name+'.log')).open('w') as log:
            process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, env=dict(env,FRAME_MODEL=model,FRAME_FIRST=str(first),FRAME_WINDOW=str((a.output/(window+'-window.json')).resolve())), start_new_session=True)
            while process.poll() is None:
                time.sleep(1)
                try:
                    progress = json.loads((case/'online-progress.json').read_text())
                except (FileNotFoundError,json.JSONDecodeError):
                    continue
                if progress.get('window_started') and window_began is None:
                    window_began = time.monotonic()-(progress['observed_ns']-progress['window_anchor_ns'])/1e9
                if window_began is not None and time.monotonic()-window_began >= 1200:
                    exceeded = True
                    os.killpg(process.pid, 15)
                    try:process.wait(timeout=10)
                    except subprocess.TimeoutExpired:os.killpg(process.pid, 9)
                    break
        write(a.output/(name+'-run.json'), {'run':name,'model':model,'ort_threads':4 if model=='MAIN-1x256' else 16,'revision':revision,'exit_code':process.wait(),'observation_limit_reached':exceeded,'total_wall_seconds_including_preparation':time.monotonic()-began,'completed_utc':datetime.now(timezone.utc).isoformat()})
        print(name, 'completed', flush=True)


if __name__ == '__main__':
    main()
