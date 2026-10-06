"""原外层合法请求生成失败路径基线；设备实际生成补偿或释放，不伪造反馈。"""
import json
import sys
import time
from fractions import Fraction
import torch
from fill_finality import FillRecord
from validate_base_execution import setup,assert_checkpoint,rational,values,Numbers


def main():
    cases = json.load(sys.stdin)
    started = time.perf_counter()
    paths = [case['path'] for case in cases]
    checkpoints = 0
    for index,case in enumerate(cases):
        ask = case['initial'][0]['payload']['event']['asks'][0]
        queue,account,prepared = Numbers.run_exact_batch(lambda:setup(ask,case))
        _,account,delta,rejected,_ = Numbers.run_exact_batch(lambda:prepared.accept(queue,account))
        assert delta.emitted.cpu().tolist() == [True] and rejected.cpu().tolist() == [False]
        record = FillRecord.create(prepared.asset_plan.matched_base,prepared.asset_plan.used_quote,prepared.fee.total_micro,
            torch.tensor([0],device='cuda'),torch.tensor([case['processing_at']],device='cuda'),delta.emitted,
            paths,selected_path=torch.tensor([index],device='cuda'))
        rebate = Numbers.run_exact_batch(lambda:rational([case['taker_rebate_rate']]))
        for checkpoint in case['checkpoints']:
            at = torch.tensor([checkpoint['at']],device='cuda')
            record,account,transition = Numbers.run_exact_batch(lambda:record.advance_one(account,at,rebate))
            assert transition.emitted.cpu().tolist() == [True]
            assert_checkpoint(checkpoint['state'],prepared.quantity,account)
            original = checkpoint['state']['fill_records'][0]
            assert record.ordinal.cpu().tolist() == [original['ordinal']]
            sequence = original['economic_application_sequence']
            assert record.economic_applied.cpu().tolist() == [sequence is not None]
            assert values(record.economic_application_sequence) == [sequence or 0]
            if record.states[index,original['ordinal']].cpu().item() == 5:
                assert transition.terminal.cpu().tolist() == [True]
                assert transition.failure_delta.emitted.cpu().tolist() == [True]
            record,account,repeated = Numbers.run_exact_batch(lambda:record.advance_one(account,at,rebate))
            assert not repeated.emitted.any().cpu().item()
            assert not repeated.failure_delta.emitted.any().cpu().item()
            assert_checkpoint(checkpoint['state'],prepared.quantity,account)
            checkpoints += 1
        assert [Fraction(n,d) for n,d in zip(values(account.rebates.numerator),values(account.rebates.denominator))] == [0]*4
    print(json.dumps({'original_external_5share_request':True,'paths':len(paths),'checkpoints':checkpoints,
        'checks':['unapplied_release','MATCHED_compensation','MINED_compensation','original_economic_sequence_retained',
                  'terminal_consumption_once','failed_no_success_rebate'],
        'elapsed_seconds_with_reference_reads':time.perf_counter()-started,
        'device_peak_allocated_bytes':torch.cuda.max_memory_allocated(),
        'full_global_clock_NOE_Ledger_delivered':False}))


if __name__ == '__main__':
    main()
