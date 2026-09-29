"""以标准库复算16次全轮次分布；新旧策略不要求相同动作或后继账户。"""
import argparse
from collections import Counter, defaultdict
import importlib.util
import json
from pathlib import Path

spec=importlib.util.spec_from_file_location('previous',Path(__file__).parent.parent/'issue-174/summarize.py')
previous=importlib.util.module_from_spec(spec);spec.loader.exec_module(previous)
common=previous.old


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args();args.output.mkdir(parents=True)
    calls=[];summaries=[];preparations=[];stages=[];expected={}
    for run in sorted(args.source.iterdir()):
        if not run.is_dir() or not (run/'completion.json').exists():continue
        round,_,tail,segment,variant=run.name.split('-');model='MAIN-'+tail
        arrivals={r['group_position']:r for r in common.read_lines(run/'host-time-assignment.jsonl')}
        samples=[];signatures=[];node_samples=defaultdict(list)
        for row in common.read_lines(run/'measurements.jsonl'):
            if 'preparation_seconds' in row:
                preparation=dict(run=run.name,**row);continue
            runtime=row['runtimes'][0];timing=runtime['timing']
            actions=[{k:v for k,v in a.items() if k!='execute_ns'} for a in runtime['actions']]
            signature=common.digest({**{k:row[k] for k in ('native_group','native_bits_sha256','native_business','ledger')},
                'hidden':runtime['hidden_sha256'],'z':runtime['z_bits_sha256'],'actions':actions})
            signatures.append((row['position'],signature))
            call={k:row[k] for k in ('position','now','service_ns','native_ns','cpu_ns','native_bits_sha256')}
            call.update(run=run.name,model=model,segment=segment,version=variant,round=int(round[1:]),entry_call=not samples,
                **{k:timing[k] for k in ('packing_ns','state_ns','state_calls','object_reduction_ns','action_steps','action_details')},
                graph_calls=timing.get('graph_calls'),actions=actions,execute_ns=[a['execute_ns'] for a in runtime['actions']],
                complete_result_sha256=signature,hidden_sha256=runtime['hidden_sha256'],z_bits_sha256=runtime['z_bits_sha256'],
                cache=previous.cache_sizes(row['cache']),depths=[[len(s) for s in r['book_levels']] for r in row['native_group']['rows']],
                cancel_objects=len(row['native_group']['cancel']),
                source_received_ms=arrivals[row['position']]['source_closing_received_at_utc_ms'],
                assigned_delivery_ms=arrivals[row['position']]['assigned_delivery_at_utc_ms'])
            for detail in call['action_details']:
                components=detail['components']
                for field in ('support_ns','scoring_ns','choose_ns'):
                    node_samples[(detail['head'],field)].append(detail[field])
                for field,value in components.items():
                    if field.endswith('_ns'):node_samples[(detail['head'],field)].append(value)
                used=sum(value for key,value in components.items() if key.endswith('_ns'))
                assert used<=detail['scoring_ns']
                node_samples[(detail['head'],'remaining_host_ns')].append(detail['scoring_ns']-used)
            samples.append(call)
        assert len(samples)==64
        key=(model,segment,variant)
        assert expected.setdefault(key,signatures)==signatures,(run.name,'同版本两轮业务结果不一致')
        calls.extend(samples)
        for phase,rows in [('all',samples),('entry',samples[:1]),('continuation',samples[1:])]:
            summaries.append(dict(run=run.name,model=model,segment=segment,variant=variant,round=int(round[1:]),phase=phase,
                **{field.removesuffix('_ns')+'_ms':common.stats([r[field] for r in rows])
                   for field in ('service_ns','native_ns','cpu_ns','packing_ns','state_ns')},
                object_preparation_ms=common.stats([sum(r['object_reduction_ns']) for r in rows]),
                action_steps_ms=common.stats([sum(step[2] for step in r['action_steps']) for r in rows]),
                action_families=dict(Counter(a['path'][0] for r in rows for a in r['actions'])),
                same_version_two_rounds_business_equal=True,
                over_4ms=sum(r['service_ns']>4_000_000 for r in rows),over_1ms=sum(r['service_ns']>1_000_000 for r in rows)))
        for (head,field),values in sorted(node_samples.items()):
            stages.append(dict(run=run.name,head=head,component=field,ms=common.stats(values)))
        completion=json.loads((run/'completion.json').read_text());assert completion['complete'] and completion['groups']==64
        preparations.append({**preparation,'completion':completion,'resources':previous.resources(args.source/(run.name+'.resources.txt'))})
    assert len(preparations)==16 and len(calls)==1024
    common.write_csv(args.output/'continuous-calls.csv.gz',calls)
    common.write_json(args.output/'continuous-summary.json',summaries)
    common.write_json(args.output/'continuous-preparation.json',preparations)
    common.write_json(args.output/'action-components.json',stages)
    for path in args.output.glob('*.json'):
        assert '/home/' not in path.read_text() and '/tmp/' not in path.read_text(),path.name


if __name__=='__main__':main()
