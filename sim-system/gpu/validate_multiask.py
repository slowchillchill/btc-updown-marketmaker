"""原外层合法多ask请求的逐fill及同时间义务生产接线验证。"""
import json
import sys
import time
import torch
from queues import Numbers,PriceQueueBatch
from account_state import PlatformAccountState
from base_execution import prepare_base_order
from base_acceptance import run_base_acceptance_batch
from fill_finality import FillRecord
from fill_pool import FillPool
from validate_base_execution import integers,rational,units,values,assert_checkpoint


def main():
    source = json.load(sys.stdin)
    started = time.perf_counter()
    asks = source['initial'][0]['payload']['event']['asks']
    def prepare():
        queue = PriceQueueBatch.empty(rational([ask['price_pusd_per_share'] for ask in asks]),capacity=2)
        queue,_ = queue.append_external(integers([units(ask['quantity_shares']) for ask in asks]))
        event = source['addition']['payload']['event']
        before = integers([units(event['quantity_before_shares']),units(asks[1]['quantity_shares'])])
        after = integers([units(event['quantity_after_shares']),units(asks[1]['quantity_shares'])])
        queue,_ = queue.apply_level_transition(before,after)
        account = PlatformAccountState.open(integers([10_000_000]),torch.tensor([0,0],device='cuda'))
        prepared = prepare_base_order(queue,account,torch.arange(len(asks),device='cuda')[None,:],
            integers([5_000_000]),integers([1_500_000]),rational(['0.3']),rational([source['fee_rate']]),source['fee_exponent'],
            torch.tensor([0],device='cuda'),torch.tensor([True],device='cuda'))
        return queue,account,prepared
    queue,account,prepared = Numbers.run_exact_batch(prepare)
    at = torch.tensor([source['processing_at']],device='cuda')
    inactive = torch.tensor([False],device='cuda')
    template = FillRecord.create(prepared.asset_plan.matched_base,prepared.asset_plan.used_quote,prepared.fee.total_micro,
        torch.tensor([0],device='cuda'),at,inactive,source['finality_path'])
    pool = FillPool.empty(template,capacity=1)
    rebate = Numbers.run_exact_batch(lambda:rational([source['taker_rebate_rate']]))
    queue,account,pool,accepted = run_base_acceptance_batch(prepared,queue,account,pool,at,
        torch.zeros((1,prepared.maximum_fills),dtype=torch.int64,device='cuda'),rebate,
        torch.zeros(queue.rows,dtype=torch.int64,device='cuda'))
    assert accepted.submission_delta.emitted.cpu().tolist() == [True]
    assert pool.capacity == 4 and values(pool.next_sequence) == [4]
    actual = [values(frame.fill_sequence)[0] for frame in accepted.fills if frame.event.emitted.cpu().item()]
    assert actual == [1,2,3]
    assert values(pool.records.base) == [1_000_001,1_000_000,1_000_000,0]
    assert values(pool.records.quote) == [200_000,200_000,250_000,0]
    assert values(pool.records.fee) == values(prepared.fee.per_fill_micro)
    original_quantity = prepared.quantity
    order = []
    for index,checkpoint in enumerate(source['checkpoints']):
        if index:
            scheduled,slot,pending = pool.next_obligation()
            assert pending.cpu().tolist() == [True] and scheduled.cpu().tolist() == [checkpoint['at']]
            pool,account,frame = Numbers.run_exact_batch(lambda:pool.advance_one(account,
                torch.tensor([checkpoint['at']],device='cuda'),rebate))
            assert frame.event.emitted.cpu().tolist() == [True]
            assert values(frame.fill_sequence) == [checkpoint['fill_sequence']]
            order.append(checkpoint['fill_sequence'])
        assert_checkpoint(checkpoint['state'],original_quantity,account)
        for record in checkpoint['state']['fill_records']:
            coordinate = record['sequence']-1
            assert pool.records.ordinal[coordinate].cpu().item() == record['ordinal']
            assert values(pool.records.economic_application_sequence)[coordinate] == (record['economic_application_sequence'] or 0)
    assert order == [1,2,3,1,2,3] and not pool.next_obligation()[-1].any().cpu().item()
    print(json.dumps({'original_external_5share_request':True,'taker_fills':3,'checkpoints':len(source['checkpoints']),
        'same_time_consumption_fill_sequences':order,'fill_pool_capacity':pool.capacity,
        'elapsed_seconds_with_reference_reads':time.perf_counter()-started,
        'device_peak_allocated_bytes':torch.cuda.max_memory_allocated(),
        'global_clock_NOE_Ledger_delivered':False}))


if __name__ == '__main__':
    main()
