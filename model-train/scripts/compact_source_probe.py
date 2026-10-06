#!/usr/bin/env python3
"""原日清单事实→有限真实源紧凑准备配置；仅工程接线，不签发正式数据验收。"""
import argparse
import datetime as dt
import hashlib
import json
from pathlib import Path
import subprocess


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--manifest',type=Path,required=True)
    parser.add_argument('--hour',required=True)
    parser.add_argument('--end-ms',type=int,required=True)
    parser.add_argument('--work-root',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--approved-initialization-manifest',type=Path)
    args = parser.parse_args()
    raw = args.manifest.read_bytes()
    manifest = json.loads(raw)
    beginning = int(dt.datetime.strptime(args.hour,'%Y-%m-%dT%H').replace(tzinfo=dt.timezone.utc).timestamp()*1000)
    selected = [f for f in manifest['files'] if f['utc_hour']==args.hour]
    files = [{**{k:f[k] for k in ('source_kind','schema_version','bytes','rows','sha256')},
              'path':str(args.manifest.parent/f['path'])} for f in selected]
    sort = {'max_run_rows':262144,'max_run_bytes':1024**3,'max_temp_run_files':1024,
            'merge_fan_in':8,'merge_workers':8,'max_open_files':72,'batch_rows':32768,
            'max_temp_bytes':64*1024**3}
    config = {'files':files,'received_start_utc_ms':beginning,'received_end_utc_ms':args.end_ms,
              'work_root':str(args.work_root),'initialization':None,
              'header':{'input_stream_id':'issue-181-real-source-short-'+args.hour,
                        'canonical_rule_version':'issue-181-public-core-engineering-v1',
                        'observation_version':'public-observation-v1',
                        'ordering_version':'historical-source-head-v2',
                        'input_scope':'engineering_short_from_manifest_hour_prefix',
                        'max_pending_ws_terminals':65536,'initial_state':'Empty'},
              'canonical':{'decode_batch_rows':32768,'max_decode_batch_bytes':256*1024**2,
                           'max_temporary_row_bytes':256*1024**2,'max_epoch_count_per_source':65536,
                           'max_pending_ws_terminals':65536,'max_retained_reducer_entries':2**24,
                           'canonical_rule_version':'issue-181-public-core-engineering-v1','sort':sort}}
    if args.approved_initialization_manifest is not None:
        if beginning != 1788912000000:
            raise ValueError('该显式#181元数据例外仅用于2026-09-09 00:00 UTC根起点')
        initialization = json.loads((Path(__file__).parent/'metadata_initialization_contract_181.json').read_text())
        initialization['files'][0]['manifest_path'] = str(args.approved_initialization_manifest)
        initialization['received_end_utc_ms'] = args.end_ms
        config['initialization'] = initialization
    args.output.parent.mkdir(parents=True,exist_ok=True)
    with args.output.open('x') as stream:
        json.dump(config,stream,ensure_ascii=False,indent=2)
        stream.write('\n')
    evidence = {'identity':'engineering_short_input_configuration_only',
                'source_commit':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),
                'manifest_sha256':hashlib.sha256(raw).hexdigest(),'hour':args.hour,
                'received_range':[beginning,args.end_ms], 'selected_files':len(files),
                'selected_file_bytes':sum(f['bytes'] for f in files),
                'selected_file_rows':sum(f['rows'] for f in files),
                'selected_source_kinds':[f['source_kind'] for f in files],
                'source_received_ranges':[[f['source_kind'],f['first_recv_time_utc_ms'],f['last_recv_time_utc_ms']] for f in selected],
                'config_path':str(args.output),'config_sha256':hashlib.sha256(args.output.read_bytes()).hexdigest(),
                'note':'范围从所选小时的真实UTC起点开始，保留该前缀全部所选来源；不读取留出或启动训练'}
    print(json.dumps(evidence,ensure_ascii=False),flush=True)


if __name__ == '__main__':
    main()
