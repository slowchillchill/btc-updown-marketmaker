"""真实热身前缀的native成本；模型历史恢复与未重跑推理单独标示。"""
import argparse
import importlib.util
from pathlib import Path

spec=importlib.util.spec_from_file_location('stats172',Path(__file__).parent.parent/'issue-172/summarize.py')
stats=importlib.util.module_from_spec(spec);spec.loader.exec_module(stats)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    for name in ('baseline','candidate','output'):p.add_argument('--'+name,type=Path,required=True)
    a=p.parse_args();result=[]
    for variant in ('baseline','candidate'):
        rows=list(stats.read_lines(getattr(a,variant)/'warmup-measurements.jsonl'))
        for label,start,end,entry in [('ordinary',1789517070000,1789517100000,201582),('next_market_overlap',1789517370000,1789517400000,418584)]:
            selected=[r for r in rows if start<=r['now']<end]
            assert selected
            def portion(values):
                return {'groups':len(values),'native_rows':sum(len(r['rows']) for r in values),
                        'native_sum_seconds':sum(r['native_ns'] for r in values)/1e9,
                        'native_cpu_sum_seconds':sum(r['native_cpu_ns'] for r in values)/1e9,
                        'first_observed_utc_ms':values[0]['now'] if values else None,
                        'last_observed_utc_ms':values[-1]['now'] if values else None}
            result.append({'variant':variant,'window':label,'scheduled_start_utc_ms':start,'scheduled_end_utc_ms':end,
                'formal_entry_position':entry,
                'before_formal_entry':portion([r for r in selected if r['position']<entry]),
                'overlapping_formal_window_native_replay':portion([r for r in selected if r['position']>=entry]),
                'first_observed_utc_ms':selected[0]['now'],'last_observed_utc_ms':selected[-1]['now'],
                'first_position':selected[0]['position'],'last_position':selected[-1]['position'],
                'groups':len(selected),'native_rows':sum(len(r['rows']) for r in selected),
                'row_populations':sorted(set(len(r['rows']) for r in selected)),
                'native_elapsed_ms':stats.stats([r['native_ns'] for r in selected]),
                'native_cpu_ms':stats.stats([r['native_cpu_ns'] for r in selected]),
                'native_sum_seconds':sum(r['native_ns'] for r in selected)/1e9,
                'native_cpu_sum_seconds':sum(r['native_cpu_ns'] for r in selected)/1e9,
                'first_group_native_ms':selected[0]['native_ns']/1e6,
                'historical_model_inference_rerun':False,
                'scope':'真实因果前缀的native构造；保留首次缓存成本。高窗口只覆盖09:30至09:55.454的已观察前缀，并含活跃市场行竞争；入口前与入口所在64组的native重放另拆分，后者不是额外预先准备成本。这里不是完整模型30秒服务成本，也未模拟实时到达队列。'})
    stats.write_json(a.output,result)


if __name__=='__main__':main()
