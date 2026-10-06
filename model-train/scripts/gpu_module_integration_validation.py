#!/usr/bin/env python3
"""实际Native、NOE、SIM、模型同进程导入与公共精确资源协调的有限验证。"""
import argparse
import json
from pathlib import Path
import sys
import torch


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--order',choices=['native-first','noe-first'],required=True)
    args=parser.parse_args()
    root=Path(__file__).resolve().parents[2]
    sys.path[:0]=[str(root/'model-train'),str(root/'data-pipeline/gpu'),
        str(root/'sim-system/gpu'),str(root/'execution-normalization/gpu'),
        str(root/'model-train/native-inputs/gpu')]
    if args.order=='native-first':
        import input_100ms
        from prepare import _numbers
        public=_numbers()
        import projection
        from model_train import gpu_exact
    else:
        from model_train import gpu_exact
        public=gpu_exact._numbers()
        import projection
        import input_100ms
    import input_projection
    import prepare
    import buy_cost
    import queues
    assert hasattr(projection,'Projection') and hasattr(input_projection,'price_value')
    assert input_100ms.PmBookInputs is not None
    assert public is prepare._numbers() is buy_cost._public_numbers() is queues.Numbers is gpu_exact._numbers()
    assert queues.ExactInt is gpu_exact.ExactInt is buy_cost.ExactInt
    # SIM对象先形成；模型乘法必须将资源需求登记到同一协调批，而非另一ContextVar。
    value=public.ExactInt.from_values([1<<120])
    capacities=[]
    def compute():
        capacities.append(queues.Numbers.device_word_capacity())
        result=value*gpu_exact.ExactInt.from_values([1<<120])
        return result
    result=public.run_exact_batch(compute)
    assert result.report_values()==[1<<240] and result.fault.cpu().tolist()==[0]
    assert capacities==[4,8],capacities
    assert public._batch.get() is None
    print(json.dumps({'import_order':args.order,'Native_NOE_distinct_projection_modules':True,
        'DPT_SIM_model_shared_exact_module_and_resource_batch':True,'resource_retry_capacities':capacities,
        'device_peak_allocated_bytes':torch.cuda.max_memory_allocated()}))


if __name__=='__main__':
    main()
