"""六项墙钟归属和独立嵌套探针；不把并行累计工作量相加成总耗时。"""
import argparse
import importlib.util
import hashlib
import json
from pathlib import Path

spec=importlib.util.spec_from_file_location('stats172',Path(__file__).parent.parent/'issue-172/summarize.py')
stats=importlib.util.module_from_spec(spec);spec.loader.exec_module(stats)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--source',type=Path,required=True)
    p.add_argument('--candidate-source',type=Path,help='候选局部筛选的独立输出目录；基线仍读取 --source')
    p.add_argument('--output',type=Path,required=True)
    a=p.parse_args();result=[];expected={}
    paths=list(a.source.glob('r1-MAIN-30x1024-*/measurements.jsonl'))
    if a.candidate_source:
        paths=[path for path in paths if path.parent.name.endswith('-baseline')]
        paths+=list(a.candidate_source.glob('r1-MAIN-30x1024-*-candidate/measurements.jsonl'))
    for path in sorted(paths,key=lambda path:path.parent.name):
        rows=[r for r in stats.read_lines(path) if 'native_ns' in r]
        assert len(rows)==64
        signatures=[]
        for row in rows:
            runtime=row['runtimes'][0]
            data={key:row[key] for key in ('native_group','native_bits_sha256','native_business','ledger')}
            data.update(hidden=runtime['hidden_sha256'],z=runtime['z_bits_sha256'],
                        actions=[{key:value for key,value in action.items() if key!='execute_ns'} for action in runtime['actions']])
            signatures.append((row['position'],hashlib.sha256(json.dumps(data,sort_keys=True,separators=(',',':')).encode()).hexdigest()))
        window=path.parent.name.split('-')[-2]
        assert expected.setdefault(window,signatures)==signatures,(path.parent.name,'闭环结果不一致')
        execution=next(row for row in stats.read_lines(path.parent.parent/'run-order.jsonl') if row['run']==path.parent.name)
        assert execution['exit_code']==0
        scopes=[]
        for row in rows:
            c=row['components']
            values={'business_account_read':c.get('account_read_ns',0),'account_features':c.get('account_features_ns',0),
                    'full_book':c.get('book_wall_ns',0),
                    'reference_trade_lifecycle_history':sum(c.get(k,0) for k in ('reference_ns','bba_ns','last_trade_ns','lifecycle_ns','history_ns')),
                    'event_statistics':c.get('events_ns',0)}
            values['rules_cancel_context_assembly']=row['native_ns']-sum(values.values())
            assert values['rules_cancel_context_assembly']>=0,(path,row['position'],values)
            scopes.append(values)
        keys=set().union(*(r['components'] for r in rows))
        result.append({'run':path.parent.name,'execution':execution,'native_ms':stats.stats([r['native_ns'] for r in rows]),
            'input_history_ledger_hidden_z_actions_receipts_equal':True,
            'six_disjoint_wall_scopes_ms':{key:stats.stats([r[key] for r in scopes]) for key in scopes[0]},
            'nested_or_parallel_component_ms':{key:stats.stats([r['components'].get(key,0) for r in rows]) for key in sorted(keys) if key.endswith('_ns')},
            'work_counts':{key:stats.stats([r['components'].get(key,0) for r in rows],scale=1) for key in sorted(keys) if not key.endswith('_ns')},
            'scope':'真实同窗口模型闭环、带探针。第4项是时间密集字段及历史构造的墙钟范围，包含其原价格/状态计算；纯时间投影另列嵌套工作量。六项均值可相加，分位数不可相加；并行累计及嵌套探针不得再次相加。'})
    assert len(result)==4
    stats.write_json(a.output,result)


if __name__=='__main__':main()
