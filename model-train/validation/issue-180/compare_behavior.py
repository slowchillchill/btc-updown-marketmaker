"""完整短轨迹精确结构及既定浮点容差核验；不忽略业务字段。"""
import argparse
import hashlib
import json
import math
from collections import Counter
from pathlib import Path


def compare(reference, actual, path='$', counts=None):
    if isinstance(reference, float):
        assert isinstance(actual, (float, int)) and not isinstance(actual, bool), path
        assert math.isfinite(reference) and math.isfinite(actual), path
        error = abs(reference-actual)
        assert error <= 1e-5+1e-4*abs(reference), (path,reference,actual)
        counts['float_values'] += 1
        counts['max_absolute_error'] = max(counts['max_absolute_error'],error)
    elif isinstance(reference, dict):
        assert isinstance(actual,dict) and reference.keys() == actual.keys(), path
        for key, value in reference.items():
            compare(value,actual[key],path+'.'+key,counts)
    elif isinstance(reference,list):
        assert isinstance(actual,list) and len(reference) == len(actual), path
        for i,(left,right) in enumerate(zip(reference,actual)):
            compare(left,right,f'{path}[{i}]',counts)
    else:
        assert type(reference) is type(actual) and reference == actual, (path,reference,actual)
        counts['exact_values'] += 1


def read(path):
    return [json.loads(line) for line in path.read_text().splitlines()]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('baseline',type=Path)
    parser.add_argument('candidate',type=Path)
    parser.add_argument('--output',type=Path,required=True)
    args = parser.parse_args()
    left,right = read(args.baseline/'trace.jsonl'),read(args.candidate/'trace.jsonl')
    counts = {'float_values':0,'exact_values':0,'max_absolute_error':0.}
    compare(left,right,counts=counts)
    # RUN_OPENED为运行来源声明，冻结另存；所有实际输入含发送与关闭逻辑时间均精确比较。
    sim_left = [x for x in read(args.baseline/'sim.jsonl') if x['kind'] != 'RUN_OPENED']
    sim_right = [x for x in read(args.candidate/'sim.jsonl') if x['kind'] != 'RUN_OPENED']
    compare(sim_left,sim_right,path='$.sim_inputs',counts=counts)
    kinds = Counter(x['kind'] for x in left)
    completion = left[-1]
    assert completion['kind'] == 'completion'
    assert max(n for x in left for _,lengths in x.get('history_lengths',[]) for n in lengths) == 16
    inputs = [x['group'] for x in left if x['kind']=='input']
    assert any(any(row['role']=='ContextOnly' for row in g['rows']) for g in inputs)
    assert any(len(g['rows']) >= 2 for g in inputs)
    # 由真实支持集、实际logits得到条件概率，沿已选路径相乘得到联合概率。
    joints = []
    probability = 1.
    for event in left:
        if event['kind']=='actor':
            probability *= event['conditional_probabilities'][event['choice']]
        elif event['kind']=='selection':
            joints.append(probability)
            probability = 1.
    result = {'passed':True,'tolerance':'1e-5 + 1e-4 * abs(reference)',**counts,
        'event_counts':dict(kinds),'completion':completion,'sim_input_records':len(sim_left),
        'joint_probabilities':joints,'files':{name:hashlib.sha256(path.read_bytes()).hexdigest() for name,path in {
            'baseline_trace':args.baseline/'trace.jsonl','candidate_trace':args.candidate/'trace.jsonl',
            'baseline_sim':args.baseline/'sim.jsonl','candidate_sim':args.candidate/'sim.jsonl'}.items()}}
    args.output.write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')


if __name__ == '__main__':
    main()
