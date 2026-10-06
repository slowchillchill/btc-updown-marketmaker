#!/usr/bin/env python3
"""真实compact公共步骤→共享设备列→GPU前缀盘口的有限工程验证，不作SIM/hidden验收。"""
import argparse
import json
import subprocess
import sys
import time
from pathlib import Path
from fractions import Fraction
import torch


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--binary',type=Path,required=True)
    parser.add_argument('--material',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    sys.path.insert(0,str(root/'data-pipeline/gpu'))
    from public_state import PublicPmState
    started = time.perf_counter()
    print(json.dumps({'stage':'strict_compact_to_pm_device_columns'}),flush=True)
    wire = subprocess.check_output([str(args.binary),'--pm-device-columns',str(args.material),'268435456'])
    columns = json.loads(wire)
    parsed = time.perf_counter()
    print(json.dumps({'stage':'shared_gpu_load','slots':len(columns['slots']),
                      'assets':len(columns['assets']),'decimals':len(columns['decimals']),
                      'cold_seconds':parsed-started}),flush=True)
    state = PublicPmState(columns)
    prefixes = torch.tensor([-1,0,len(columns['groups'])-1],device='cuda',dtype=torch.int64)
    frame = state.frame(prefixes)
    # 短组件参考：只检查3个事实前缀的末项，不重放候选、账户或原全窗CPU诊断。
    def latest(name,prefix):
        result = {}
        for target,sequence,group,value in columns[name]:
            if group <= prefix: result[target] = (sequence,value)
        return result
    expected_values, expected_masks, expected_initialized, expected_ready = [],[],[],[]
    for prefix in [-1,0,len(columns['groups'])-1]:
        quantity,selection,reset = (latest(name,prefix) for name in ('quantities','selections','resets'))
        initialized,ready = (latest(name,prefix) for name in ('initialized','ready'))
        for slot,(asset,_,_) in enumerate(columns['slots']):
            reset_seq = reset.get(asset,(-1,0))[0]
            qseq,qid = quantity.get(slot,(-1,0))
            sseq,present = selection.get(slot,(-1,0))
            recent = slot in quantity and qseq >= reset_seq
            expected_values.append(Fraction(columns['decimals'][qid]) if recent else Fraction(0))
            expected_masks.append(slot in selection and sseq >= reset_seq and bool(present) and recent)
        for asset in range(len(columns['assets'])):
            expected_initialized.append(bool(initialized.get(asset,(-1,0))[1]))
            rseq,rvalue = ready.get(asset,(-1,0))
            expected_ready.append(asset in ready and bool(rvalue) and rseq >= reset.get(asset,(-1,0))[0])
    n = frame.quantity.numerator.report_values()
    d = frame.quantity.denominator.report_values()
    assert [Fraction(a,b) for a,b in zip(n,d)] == expected_values
    assert frame.present.reshape(-1).cpu().tolist() == expected_masks
    assert frame.initialized.reshape(-1).cpu().tolist() == expected_initialized
    assert frame.ready.reshape(-1).cpu().tolist() == expected_ready
    assert not frame.fault.any().cpu().item()
    assert not frame.present[0].any().cpu().item()
    # 设备前缀可以连续查询且未来价位始终由present mask屏蔽；查询不重置账户或GRU。
    for _ in range(64): state.frame(prefixes)
    invalid = state.frame(prefixes.new_tensor([len(columns['groups'])]))
    assert invalid.fault.ne(0).all().cpu().item()
    torch.cuda.synchronize()
    report = {'source_commit':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),
              'identity':'engineering_real_public_pm_quantity_state_only','device':torch.cuda.get_device_name(),
              'inputs':len(columns['groups']),'original_pm_steps':columns['original_pm_steps'],
              'assets':len(columns['assets']),'slots':len(columns['slots']),
              'decimal_dictionary':len(columns['decimals']),'derived_column_wire_bytes':len(wire),
              'cold_seconds':parsed-started,'total_seconds':time.perf_counter()-started,
              'peak_allocated_bytes':torch.cuda.max_memory_allocated(),
              'passed':['real_source_shared_exact_decimal_columns','gpu_prefix_quantity_and_selection',
                        'reset_preserves_initialization','ready_reset_boundary','future_slots_masked',
                        'continuous_device_query','out_of_material_prefix_fault'],
              'not_delivered':['quality_bba_trade_market_state','binance_state','hidden','sim_noe_ledger',
                               '100ms_window','full_actor','ppo','active_root_restore']}
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps(report,ensure_ascii=False),flush=True)


if __name__ == '__main__': main()
