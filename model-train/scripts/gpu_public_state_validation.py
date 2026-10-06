#!/usr/bin/env python3
"""真实compact公共步骤→共享设备列→GPU前缀盘口的有限工程验证，不作SIM/hidden验收。"""
import argparse
import json
import subprocess
import sys
import time
import math
from pathlib import Path
from fractions import Fraction
import torch


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--binary',type=Path,required=True)
    parser.add_argument('--material',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--original-reference',type=Path)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    sys.path.insert(0,str(root/'data-pipeline/gpu'))
    from public_state import PublicPmState
    sys.path.insert(0,str(root/'model-train/native-inputs/gpu'))
    from input_books import PmBookInputs
    from model_train.book import BookEncoder
    from prepare import _numbers
    started = time.perf_counter()
    print(json.dumps({'stage':'strict_compact_to_pm_device_columns'}),flush=True)
    wire = subprocess.check_output([str(args.binary),'--pm-device-columns',str(args.material),'268435456'])
    columns = json.loads(wire)
    parsed = time.perf_counter()
    print(json.dumps({'stage':'shared_gpu_load','slots':len(columns['slots']),
                      'assets':len(columns['assets']),'decimals':len(columns['decimals']),
                      'cold_seconds':parsed-started}),flush=True)
    state = PublicPmState(columns)
    prefix_values = [-1,0,len(columns['groups'])-1] if columns['groups'] else [-1,-1,-1]
    prefixes = torch.tensor(prefix_values,device='cuda',dtype=torch.int64)
    frame = state.frame(prefixes)
    # 短组件参考：只检查3个事实前缀的末项，不重放候选、账户或原全窗CPU诊断。
    def latest(name,prefix):
        result = {}
        for target,sequence,group,value in columns[name]:
            if group <= prefix: result[target] = (sequence,value)
        return result
    expected_values, expected_masks, expected_initialized, expected_ready = [],[],[],[]
    for prefix in prefix_values:
        quantity,selection,reset = (latest(name,prefix) for name in ('quantities','selections','resets'))
        initialized,ready = (latest(name,prefix) for name in ('initialized','ready'))
        for slot,(asset,_,_) in enumerate(columns['slots']):
            reset_seq = reset.get(asset,(-1,0))[0]
            qseq,qid = quantity.get(slot,(-1,0))
            sseq,present = selection.get(slot,(-1,0))
            recent = slot in quantity and qseq >= reset_seq
            expected_values.append(Fraction(columns['decimals'][qid]) if recent else Fraction(0))
            expected_masks.append(slot in selection and sseq >= reset_seq and bool(present))
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
    # 必须先按当前正量压紧再进入原BookEncoder；不让future槽位改变配对树。
    assets = torch.tensor([[0,1]]*3, dtype=torch.int64, device='cuda')
    now = torch.tensor([columns['groups'][max(0,int(prefix))][2] if columns['groups'] else 0 for prefix in prefix_values],
                       dtype=torch.int64,device='cuda')
    if args.original_reference is not None:
        now[-1] = json.loads(args.original_reference.read_text())['observation_ms']
    packer = PmBookInputs(state, initial_capacity=1)
    packed = packer.pack(frame, assets, now)
    if len(columns['assets']) >= 2:
        assert not packed.fault.any().cpu().item()
    else:
        assert packed.fault.ne(0).all().cpu().item()  # 未知资产坐标返回fault，不补假身份。
    assert torch.all(packed.mask[:, :, 1:].to(torch.int32) <= packed.mask[:, :, :-1].to(torch.int32))
    assert torch.count_nonzero(packed.levels[~packed.mask]) == 0
    torch.manual_seed(181102)  # 当前有限盘口组件验证，不登记正式训练seed。
    encoder = BookEncoder().cuda()
    levels = torch.nn.functional.pad(packed.levels,(0,0,0,0,0,2)).detach().requires_grad_(True)
    mask = torch.nn.functional.pad(packed.mask,(0,0,0,2))
    encoded = encoder(levels,mask)
    # 尾部资源补齐不能改变固定相邻配对树的模型输出。
    padded = torch.nn.functional.pad(levels,(0,0,0,17))
    padded_mask = torch.nn.functional.pad(mask,(0,17))
    assert torch.equal(encoded,encoder(padded,padded_mask))
    encoded.sum().backward()
    assert torch.isfinite(encoded).all() and torch.isfinite(levels.grad).all()
    assert torch.count_nonzero(levels.grad[~mask]) == 0
    # 时间坐标有界边界：两端i64差必须得到2^64-1，不能在先减法时溢出。
    numbers = _numbers()
    bounds = torch.tensor([-(1<<63),(1<<63)-1],device='cuda',dtype=torch.int64)
    exact = numbers.ExactInt.from_i64_tensor(bounds)
    assert exact.report_values() == [-(1<<63),(1<<63)-1]
    delta = numbers.ExactInt.from_i64_tensor(bounds[1:])-numbers.ExactInt.from_i64_tensor(bounds[:1])
    assert delta.report_values() == [(1<<64)-1]
    assert numbers.combine_fault_rows(torch.tensor([[1,2,4]],device='cuda',dtype=torch.int32)).cpu().tolist() == [7]
    if args.original_reference is not None:
        reference = json.loads(args.original_reference.read_text())
        asset = next(i for i,a in enumerate(columns['assets']) if a['token_id'] == reference['token'])
        slot = next(i for i,(a,side,price) in enumerate(columns['slots'])
                    if a == asset and side == 0 and columns['decimals'][price] == reference['price'])
        assert Fraction(n[2*len(columns['slots'])+slot],d[2*len(columns['slots'])+slot]) == Fraction(reference['quantity'])
        assert frame.present[2,slot].cpu().item() == reference['present']
        assert frame.initialized[2,asset].cpu().item() == reference['initialized']
        assert frame.ready[2,asset].cpu().item() == reference['ready']
        quality_names = ['fresh','stale','missing','unknown_not_observed','confirmed_gap','invalid']
        def signed_log(value):
            value = float(value)
            return math.copysign(math.log1p(abs(value)),value) if value else 0.0
        for outcome, asset_id in enumerate([0,1]):
            original = next(a for a in reference['public_assets']
                            if a['asset']['token_id'] == columns['assets'][asset_id]['token_id'])
            for side, name in enumerate(['bids','asks']):
                book = original[name]
                assert frame.quality[2,asset_id].cpu().item() == quality_names.index(book['quality'])
                expected_rows = []
                values = book['value'] if book['quality'] in ('fresh','stale') else None
                for level in values or []:
                    p, q = Fraction(level['price_pusd_per_share']), Fraction(level['quantity_shares'])
                    if q <= 0: continue
                    anchors = [a for a in book['direct_anchors']
                               if a['field_path'] == f"assets[{original['asset']['token_id']}].{name}[{level['price_pusd_per_share']}]" ]
                    time_features = [0.0]*5
                    if len(anchors) == 1:
                        anchor = anchors[0]
                        received, source = anchor['received_at_utc_ms'],anchor['source_event_at_utc_ms']
                        time_features[1] = signed_log(Fraction(reference['observation_ms']-received,1000))
                        time_features[4] = 1.0
                        if source is not None:
                            time_features[0] = signed_log(Fraction(reference['observation_ms']-source,1000))
                            time_features[2] = signed_log(Fraction(received-source,1000))
                            time_features[3] = 1.0
                    best = Fraction((values or [])[0]['price_pusd_per_share'])
                    expected_rows.append([float(2*p-1),signed_log(q),1.0,*time_features,
                                          signed_log(best-p if side == 0 else p-best)])
                actual = packed.levels[2,2*outcome+side][packed.mask[2,2*outcome+side]]
                expected = torch.tensor(expected_rows,device='cuda',dtype=torch.float32).reshape(-1,9)
                assert torch.equal(actual,expected), (actual,expected)
    # 设备前缀可以连续查询且未来价位始终由present mask屏蔽；查询不重置账户或GRU。
    # sentinel与历史是同一块持久存储；连续查询不重建/复制整列。
    columns_for_query = [getattr(state,name) for name in ('quantities','selections','resets','initialized','ready','ticks')]
    storage = [(column.rows.untyped_storage().data_ptr(),column.padded.untyped_storage().data_ptr())
               for column in columns_for_query]
    assert all(a == b for a,b in storage)
    for _ in range(64): state.frame(prefixes)
    assert storage == [(column.rows.untyped_storage().data_ptr(),column.padded.untyped_storage().data_ptr())
                       for column in columns_for_query]
    invalid = state.frame(prefixes.new_tensor([len(columns['groups'])]))
    assert invalid.fault.ne(0).all().cpu().item()
    assert invalid.prefix_fault.ne(0).all().cpu().item()
    torch.cuda.synchronize()
    report = {'source_commit':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),
              'identity':'engineering_real_public_pm_book_inputs_only','device':torch.cuda.get_device_name(),
              'groups':len(columns['groups']),'original_pm_steps':columns['original_pm_steps'],
              'assets':len(columns['assets']),'slots':len(columns['slots']),
              'decimal_dictionary':len(columns['decimals']),'derived_column_wire_bytes':len(wire),
              'cold_seconds':parsed-started,'total_seconds':time.perf_counter()-started,
              'peak_allocated_bytes':torch.cuda.max_memory_allocated(),
              'passed':['real_source_shared_exact_decimal_columns','gpu_prefix_quantity_and_selection',
                        'initialization_and_ready_prefix','future_slots_masked',
                        'continuous_device_query','out_of_material_prefix_fault',
                        'exact_price_sort_current_positive_compaction','native_pm_nine_fields',
                        'real_book_encoder_forward_backward','tail_padding_pairwise_tree',
                        'i64_time_coordinate_exact_difference','fault_bit_union'],
              'original_core_reference_checked':args.original_reference is not None,
              'not_delivered':['bba_trade_market_state','binance_state','hidden','sim_noe_ledger',
                               '100ms_window','full_actor','ppo','active_root_restore']}
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps(report,ensure_ascii=False),flush=True)


if __name__ == '__main__': main()
