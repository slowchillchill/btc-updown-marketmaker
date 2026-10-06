"""目标机原D-0295公共合同精确输入/输出对照，不执行训练或原全窗诊断。"""
from pathlib import Path
import sys
import json
import time
from fractions import Fraction
import torch
sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'sim-system/gpu'))
from queues import Numbers,ExactRational
from finite_attribution import split_scale12,allocate_ordered


def ratio(values):
    return ExactRational.from_values([int(value['numerator']) for value in values],
        [int(value['denominator']) for value in values])


def fractions(value):
    return [Fraction(n,d) for n,d in zip(value.numerator.report_values(),value.denominator.report_values())]


def expected(value):
    return Fraction(int(value['numerator']),int(value['denominator']))


def main():
    source=json.load(sys.stdin)
    start=time.perf_counter()
    splits=source['splits']
    result=Numbers.run_exact_batch(lambda:split_scale12(ratio([row['total'] for row in splits]),
        ratio([row['part'] for row in splits]),ratio([row['whole'] for row in splits])))
    assert result.fault.cpu().tolist()==[0]*len(splits)
    assert fractions(result.allocated)==[expected(row['allocated']) for row in splits]
    assert fractions(result.remaining)==[expected(row['remaining']) for row in splits]
    for row in source['allocations']:
        result,fault=Numbers.run_exact_batch(lambda:allocate_ordered(ratio([row['total']]),
            tuple(ratio([value]) for value in row['capacities'])))
        assert fault.cpu().tolist()==[0]
        assert [fractions(value)[0] for value in result]==[expected(value) for value in row['allocated']]
    print(json.dumps({'split_rows':len(splits),'ordered_capacity_cases':len(source['allocations']),
        'source_endpoints_exact':True,'over_u128_input_checked':True,'elapsed_seconds':time.perf_counter()-start,
        'device_peak_allocated_bytes':torch.cuda.max_memory_allocated(),'Ledger_policy_consumer_delivered':False}))


if __name__=='__main__':
    main()
