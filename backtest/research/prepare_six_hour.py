#!/usr/bin/env python3
"""仅准备D0335本地配置，不读行情行、不执行研究；正式启动须绑定已审实现头。"""
import argparse
import copy
import hashlib
import json
from pathlib import Path

NAMES = ('center', 'no-innovation', 'small-size', 'no-inventory', 'passive-repair', 'health2', 'health8')
START = 1789171200000
END = START + 6 * 3600000


def prepare(template, business, dataset_root, output_root, implementation, rules_path, finality_path):
    document = copy.deepcopy(template)
    day = document['days'][0]
    document['days'] = [day]
    document['parallel_dates'] = 1
    preparation = day['preparation']
    preparation['scope'].update(manifest_paths=[str(dataset_root / '2026-09-12/typed_raw/utc_day=2026-09-12/manifest.json')],
                                received_start_utc_ms=START, received_end_utc_ms=END,
                                purpose='D0335：fold0七候选连续六小时，独立2000pUSD；七小时整体墙钟预算。')
    preparation['header'].update(input_scope='UTC/2026-09-12T00:00:00Z--2026-09-12T06:00:00Z/fold0-seven',
                                 input_stream_id='issue185-six-hour-public-2026-09-12')
    preparation['hidden_stream_id'] = 'issue185-six-hour-hidden-2026-09-12'
    preparation['work_root'] = str(output_root / 'preparation')
    initialization = preparation['metadata_initialization']
    initialization['received_start_utc_ms'] = START
    initialization['received_end_utc_ms'] = END
    for file in initialization['files']:
        file['manifest_path'] = str(dataset_root / '2026-09-11/typed_raw/utc_day=2026-09-11/manifest.json')
    # 保留原规则文件、哈希和因果起点；不加载旧二进制或检查点。
    document['rules']['snapshot_path'] = str(rules_path)
    original = day['candidates'][0]
    params = {candidate['identifier']: candidate['strategy'] for candidate in business['folds'][0]['candidates']}
    day['candidates'] = []
    for name in NAMES:
        candidate = copy.deepcopy(original)
        strategy = copy.deepcopy(params[name])
        candidate['strategy'] = strategy
        identity = 'issue185-six-hour-f0-2026-09-12-' + name
        run = candidate['run']
        run['strategy']['effective_parameters'] = copy.deepcopy(strategy)
        run['implementation_identity'] = copy.deepcopy(implementation)
        run['noe_implementation_identity'] = 'normalization_core_v1@' + implementation['git_commit']
        run['strategy_request_namespace'] = candidate['request_namespace'] = identity + '-requests'
        run['timing'] = dict(public_delivery_delay_ms={'Fixed': 0}, calculation_start_delay_ms={'Fixed': 0},
                             calculation_duration_ms={'Fixed': 15}, dispatch_delay_ms={'Fixed': 0})
        session = run['session']
        for key, suffix in [('run_id',''), ('session_id','-session'), ('account_id','-account'), ('caller_instance_id','-host'),
                            ('creation_request_id','-opening'), ('response_bridge_binding_id','-bridge')]:
            session[key] = identity + suffix
        session['initial_account_state']['available_pusd'] = '2000'
        session['session_seed'] = hashlib.sha256(identity.encode()).hexdigest()
        root = output_root / 'runs/2026-09-12/accounts' / name
        for key, filename in [('sim_log','sim.jsonl'), ('noe_root','noe'), ('ledger_root','ledger'),
                              ('metadata_path','run.json'), ('result_path','result.json')]:
            run[key] = str(root / filename)
        run['finality_policy_path'] = str(finality_path)
        run['activation_id'] = identity + '-activate'
        run['consumption_sequence_ref'] = identity + '-public'
        run['replay_progress_interval_seconds'] = 900
        day['candidates'].append(candidate)
    day.update(checkpoint_root=str(output_root / 'runs/2026-09-12/checkpoints'),
               checkpoint_interval_seconds=300, producer_lead_groups=8192)
    day.pop('checkpoint_every_applied_inputs', None)
    day.pop('checkpoint_every_candidate_events', None)
    return document


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('template', 'business-candidates', 'dataset-root', 'output-root', 'implementation-json', 'rules', 'finality'):
        parser.add_argument('--' + name, type=Path, required=True)
    args = parser.parse_args()
    read = lambda path: json.loads(path.read_text())
    document = prepare(read(args.template), read(args.business_candidates), args.dataset_root, args.output_root,
                       read(args.implementation_json), args.rules, args.finality)
    destination = args.output_root / 'configs/research-days.json'
    destination.parent.mkdir(parents=True, exist_ok=True)
    # 不覆盖前次准备或研究的配置。
    with destination.open('x') as output:
        json.dump(document, output, ensure_ascii=False, indent=2)
        output.write('\n')
    print(destination)


if __name__ == '__main__':
    main()
