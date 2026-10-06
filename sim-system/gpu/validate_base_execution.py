"""原合法请求短链对照设备自身候选；输出真实调度/容量，不宣称全训练闭环。"""
from dataclasses import fields, replace
from fractions import Fraction
import json
import os
from pathlib import Path
import sys
import time
import torch

sys.path.insert(0,str(Path(__file__).resolve().parent))
from queues import Numbers, ExactInt, ExactRational, PriceQueueBatch, run_queue_batch, _take
from account_state import PlatformAccountState
from base_execution import prepare_base_order, reduce_order_maker, cancel_base_order, append_base_remainder
from fill_finality import NormalFillRecord


def integers(values):
    return ExactInt.from_values(values)


def rational(values):
    values = [Fraction(value) for value in values]
    return ExactRational.ratio(integers([value.numerator for value in values]),integers([value.denominator for value in values]))


def units(value):
    result = Fraction(value)*1_000_000
    assert result.denominator == 1
    return result.numerator


def values(value):
    assert not value.fault.any().cpu().item(),value.fault.cpu().tolist()
    words,signs = value.words.cpu().tolist(),value.sign.cpu().tolist()
    return [sign*sum((word & 0xffffffff) << (32*index) for index,word in enumerate(row)) for sign,row in zip(signs,words)]


def assert_checkpoint(reference, quantity, account):
    expected = reference['quantities']['Base']
    assert not quantity.fault.any().cpu().item()
    for field in fields(quantity):
        if field.name != 'fault':
            assert values(getattr(quantity,field.name)) == [expected[field.name]],(field.name,values(getattr(quantity,field.name)),expected[field.name])
    for name in ('cash','available','principal','fee'):
        assert values(getattr(account,name)) == [units(reference[name])],(name,values(getattr(account,name)),reference[name])
    assert values(account.next_sequence) == [reference['next_sequence']],('next_sequence',values(account.next_sequence),reference)
    expected_outcomes = dict(reference['outcomes'])
    assert values(account.outcomes) == [units(expected_outcomes.get('1','0')),units(expected_outcomes.get('2','0'))]
    numerators,denominators = values(account.rebates.numerator),values(account.rebates.denominator)
    assert [Fraction(n,d) for n,d in zip(numerators,denominators)] == [Fraction(value) for value in reference['rebates']]


def main():
    reference = json.load(sys.stdin)
    initial = reference['initial'][0]['payload']['event']
    ask = initial['asks'][0]
    assert ask['price_pusd_per_share'] == '0.2'
    assert units(ask['quantity_shares']) == 1_000_001
    started = time.perf_counter()
    operators = {}
    # 同一次真实调用记录CUDA调度；profile开销包含在下面的墙钟口径。
    with torch.profiler.profile(activities=[torch.profiler.ProfilerActivity.CPU,torch.profiler.ProfilerActivity.CUDA]) as profile:
        queue,account,prepared = Numbers.run_exact_batch(lambda:setup(ask,reference))
        queue,account,delta,rejected,fault = Numbers.run_exact_batch(lambda:prepared.accept(queue,account))
        assert delta.emitted.cpu().tolist() == [True] and rejected.cpu().tolist() == [False]
        quantity = prepared.quantity
        # 已接单剩余量进入原bid队尾；队列rows0=ask0.2，rows1=bid0.3。
        queue,remainder = run_queue_batch(queue,lambda state:append_base_remainder(state,quantity,
            torch.tensor([1],device='cuda'),torch.tensor([7],device='cuda'),delta.emitted))
        assert not remainder.root_fault.any().cpu().item()
        maker = None
        taker_record = NormalFillRecord.create(prepared.asset_plan.matched_base,prepared.asset_plan.used_quote,prepared.fee.total_micro,
            torch.tensor([0],device='cuda'),torch.tensor([reference['processing_at']],device='cuda'),delta.emitted,reference['finality_path'])
        rebate_rate = Numbers.run_exact_batch(lambda:rational([reference['taker_rebate_rate']]))
        taker_record,account,_ = Numbers.run_exact_batch(lambda:taker_record.advance_one(account,taker_record.matched_at,rebate_rate))
        maker_record = None
        for checkpoint in reference['checkpoints']:
            stage = checkpoint['stage']
            if stage == 'taker_finality':
                at = torch.tensor([checkpoint['at']],device='cuda')
                taker_record,account,transition = Numbers.run_exact_batch(lambda:taker_record.advance_one(account,at,rebate_rate))
                # 同实际原义务时点的重投不重复经济应用或应计。
                taker_record,account,repeated = Numbers.run_exact_batch(lambda:taker_record.advance_one(account,at,rebate_rate))
                assert not repeated.emitted.any().cpu().item()
            elif stage == 'hidden_step':
                event = reference['events'][checkpoint['ordinal']]['payload']['event']
                side = event['side']
                bid = side == 'BID'
                before = integers([0,units(event['quantity_before_shares'])] if bid else [units(event['quantity_before_shares']),0])
                after = integers([0,units(event['quantity_after_shares'])] if bid else [units(event['quantity_after_shares']),0])
                queue,effects = run_queue_batch(queue,lambda state:state.apply_level_transition(before,after))
                # maker接口领取真实订单行的原效果；保留每段身份和fault。
                capacity = queue.capacity
                selected = replace(effects,order_ids=effects.order_ids[1:2],
                    maker_base_units=_take(effects.maker_base_units,torch.arange(capacity,device='cuda')+capacity),fault=effects.fault[1:2])
                quantity,current = Numbers.run_exact_batch(lambda:reduce_order_maker(quantity,selected,torch.tensor([7],device='cuda')))
                if maker is None:
                    maker = current
                else:
                    maker = replace(current,base=current.base.select(current.emitted,maker.base),
                        quote=current.quote.select(current.emitted,maker.quote),emitted=current.emitted | maker.emitted,
                        fault=current.fault | maker.fault)
            elif stage == 'maker_finality':
                assert maker is not None
                if maker_record is None:
                    maker_record = NormalFillRecord.create(maker.base,maker.quote,maker.quote.constant(0),torch.tensor([0],device='cuda'),
                        torch.tensor([reference['maker_at']],device='cuda'),maker.emitted,reference['finality_path'])
                    maker_record,account,_ = Numbers.run_exact_batch(lambda:maker_record.advance_one(account,maker_record.matched_at,rebate_rate))
                maker_record,account,_ = Numbers.run_exact_batch(lambda:maker_record.advance_one(account,torch.tensor([checkpoint['at']],device='cuda'),rebate_rate))
            elif stage == 'canceled':
                queue,quantity,account,_,_,found,_ = Numbers.run_exact_batch(lambda:cancel_base_order(queue,quantity,account,
                    torch.tensor([-1,7],device='cuda'),torch.tensor([0,0],device='cuda'),torch.tensor([True],device='cuda')))
                assert found.cpu().tolist() == [False,True]
            try:
                assert_checkpoint(checkpoint['state'],quantity,account)
            except AssertionError as error:
                raise AssertionError((stage,checkpoint.get('at'),checkpoint.get('ordinal'),error)) from error
    torch.cuda.synchronize()
    elapsed = time.perf_counter()-started
    events = profile.events()
    cuda = [event for event in events if event.device_type == torch.autograd.DeviceType.CUDA]
    kernels = [event for event in cuda if not event.name.startswith(('Memcpy','Memset'))]
    for event in cuda:
        operators[event.name] = operators.get(event.name,0)+1
    artifact = Path(os.environ['B_GPU_ORDER_ARTIFACT_DIR'])
    artifact.mkdir(parents=True,exist_ok=True)
    (artifact/'cuda-device-events.json').write_text(json.dumps(operators,indent=2)+'\n')
    print(json.dumps({'checkpoints':len(reference['checkpoints']),'original_external_5share_request':True,
        'elapsed_seconds_with_profile_and_reference_reads':elapsed,'cuda_device_events':len(cuda),'cuda_kernel_launches':len(kernels),
        'cuda_memcpy_events':sum(event.name.startswith('Memcpy') for event in cuda),
        'public_integer_kernel_launches':sum(event.name.startswith('integer_kernel') for event in kernels),
        'public_floor_sum_kernel_launches':sum(event.name.startswith('floor_sum_kernel') for event in kernels),
        'queue_rows':queue.rows,'segment_capacity':queue.capacity,
        'maximum_exact_words':max(getattr(quantity,field.name).words.shape[1] for field in fields(quantity) if field.name!='fault'),
        'device_peak_allocated_bytes':torch.cuda.max_memory_allocated(),
        'public_clock_NOE_Ledger_actor_delivered':False}))


def setup(ask,reference):
    price = rational([ask['price_pusd_per_share'],'0.3'])
    queue = PriceQueueBatch.empty(price,capacity=2)
    queue,_ = queue.append_external(integers([units(ask['quantity_shares']),0]))
    account = PlatformAccountState.open(integers([10_000_000]),torch.tensor([0,0],device='cuda'))
    prepared = prepare_base_order(queue,account,torch.tensor([[0]],device='cuda'),integers([5_000_000]),
        integers([1_500_000]),rational(['0.3']),rational([reference['fee_rate']]),reference['fee_exponent'],
        torch.tensor([0],device='cuda'),torch.tensor([True],device='cuda'))
    return queue,account,prepared


if __name__ == '__main__':
    main()
