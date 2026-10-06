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
from latency import EmpiricalDelaySampler, byte_column
from pathlib import Path


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
    # 原完整BusinessProjectionBatch→MemoryNOE是冷来源参考，运行热链不调用CPU端口。
    profile_sha='98fd40f843275db8994a29cec1de6a72f95bbaa4309df9779c7ed1285cdff596'
    profile_path=Path(__file__).resolve().parents[1]/'config/latency-profiles'/profile_sha/'profile.json'
    sampler=EmpiricalDelaySampler.build(profile_path.read_bytes(),profile_sha,
        [source['session_seed'].encode('ascii')])
    lifecycle=[]
    direct=[]
    for batch in source['feedback_batches']:
        for category in ['orders','trades','settlements']:
            for row in batch[category]:
                lifecycle.append(row.get('feedback',row)['metadata'])
        direct.extend(row['metadata'] for row in batch['direct'])
    sequence=integers([row['private_feedback_sequence'] for row in lifecycle])
    roots=torch.zeros(len(lifecycle),dtype=torch.int64,device='cuda')
    sampled=sampler.sample_feedback(roots,sequence)
    arrived,fault=sampled.arrival_at(torch.tensor([row['platform_event_at_utc_ms'] for row in lifecycle],device='cuda'))
    assert not fault.any().cpu().item()
    assert arrived.cpu().tolist()==[row['feedback_received_at_utc_ms'] for row in lifecycle]
    identities,offsets=byte_column([row['global_request_id'].encode('utf-8') for row in direct])
    roots=torch.zeros(len(direct),dtype=torch.int64,device='cuda')
    sampled=sampler.sample_requests(roots,identities,offsets,torch.ones_like(roots))
    arrived,fault=sampled.arrival_at(torch.tensor([row['platform_responded_at_utc_ms'] for row in direct],device='cuda'))
    assert not fault.any().cpu().item()
    assert arrived.cpu().tolist()==[row['response_received_at_utc_ms'] for row in direct]
    assert source['initial_noe_records']>0 and source['noe_records']
    import os
    artifact=os.environ.get('B_GPU_ORDER_ARTIFACT_DIR')
    if artifact:
        Path(artifact,'original-noe-reference.json').write_text(json.dumps({
            'feedback_batches':source['feedback_batches'],'noe_records':source['noe_records']},ensure_ascii=False))
    print(json.dumps({'original_external_5share_request':True,'taker_fills':3,'checkpoints':len(source['checkpoints']),
        'same_time_consumption_fill_sequences':order,'fill_pool_capacity':pool.capacity,
        'elapsed_seconds_with_reference_reads':time.perf_counter()-started,
        'device_peak_allocated_bytes':torch.cuda.max_memory_allocated(),
        'original_NOE_records':len(source['noe_records']),'actual_lifecycle_device_delay_checks':len(lifecycle),
        'original_feedback_dual_visibility_gate_checked':True,
        'global_clock_NOE_Ledger_delivered':False}))


if __name__ == '__main__':
    main()
