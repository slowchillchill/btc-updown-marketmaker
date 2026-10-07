"""同一合法多ask来源在两个独立账户上真实接纳，再追加实际最终性自然批。"""
from dataclasses import fields
from pathlib import Path
import torch
from queues import Numbers, PriceQueueBatch
from account_state import PlatformAccountState
from base_execution import prepare_base_order
from base_acceptance import run_base_acceptance_batch
from fill_finality import FillRecord
from fill_pool import FillPool, _take_record
from asset_buy import take_ratio
from feedback import FeedbackEmitter
from latency import EmpiricalDelaySampler, byte_column
from projection import Projection
from validate_base_execution import integers, rational, units, values


def validate_two_root_projection(source):
    from validate_projection import expected_semantic
    tensor=lambda value:torch.tensor(value,dtype=torch.int64,device='cuda')
    roots=tensor([0,1]); zero=torch.zeros_like(roots); active=roots>=0
    asset_roots=tensor([0,0,1,1]); assets=tensor([0,2])
    asks=source['initial'][0]['payload']['event']['asks']
    assert len(asks)==2
    row_roots=tensor([0,0,1,1])
    def prepare():
        queue=PriceQueueBatch.empty(rational([ask['price_pusd_per_share'] for ask in asks]*2),capacity=2)
        queue,_=queue.append_external(integers([units(ask['quantity_shares']) for ask in asks]*2))
        event=source['addition']['payload']['event']
        queue,_=queue.apply_level_transition(
            integers([units(event['quantity_before_shares']),units(asks[1]['quantity_shares'])]*2),
            integers([units(event['quantity_after_shares']),units(asks[1]['quantity_shares'])]*2))
        account=PlatformAccountState.open(integers([10_000_000]*2),asset_roots)
        prepared=prepare_base_order(queue,account,tensor([[0,1],[2,3]]),
            integers([5_000_000]*2),integers([1_500_000]*2),rational(['0.3']*2),
            rational([source['fee_rate']]*2),source['fee_exponent'],assets,active)
        return queue,account,prepared
    queue,account,prepared=Numbers.run_exact_batch(prepare)
    at=torch.full_like(roots,source['processing_at'])
    template=FillRecord.create(prepared.asset_plan.matched_base,prepared.asset_plan.used_quote,
        prepared.fee.total_micro,assets,at,~active,source['finality_path'])
    pool=FillPool.empty(template,capacity=1)
    rebate=Numbers.run_exact_batch(lambda:rational([source['taker_rebate_rate']]*2))
    _,account,pool,effects=run_base_acceptance_batch(prepared,queue,account,pool,at,
        torch.zeros((2,prepared.maximum_fills),dtype=torch.int64,device='cuda'),rebate,row_roots)
    assert effects.submission_delta.emitted.cpu().tolist()==[True,True]
    sha='98fd40f843275db8994a29cec1de6a72f95bbaa4309df9779c7ed1285cdff596'
    root=Path(__file__).resolve().parents[2]
    # 有限工程对照复用同一原seed；两个账户均从实际SIM产生反馈，不复制已提交批。
    sampler=EmpiricalDelaySampler.build((root/'sim-system/config/latency-profiles'/sha/'profile.json').read_bytes(),
        sha,[source['session_seed'].encode('ascii')]*2)
    emitter=FeedbackEmitter.open(2)
    empty=torch.empty((2,0),dtype=torch.int64,device='cuda')
    emitter,_=Numbers.run_exact_batch(lambda:emitter.emit(sampler,empty.bool(),empty.bool(),empty,empty,empty,
        empty.to(torch.int32),control_admission=active))
    state=Projection.open(emitter,roots,zero,zero,assets,asset_roots,integers([1,1]),zero,zero,roots,roots)
    identity,offsets=byte_column([b'gpu-multiask-post']*2)
    received,fault=sampler.sample_requests(roots,identity,offsets,torch.ones_like(roots)).arrival_at(at)
    assert fault.cpu().tolist()==[0,0]
    state,first=Numbers.run_exact_batch(lambda:state.acceptance(sampler,prepared,effects,pool,at,received))
    old={field.name:getattr(state.normalizer.bank,field.name).clone() for field in fields(state.normalizer.bank)
        if isinstance(getattr(state.normalizer.bank,field.name),torch.Tensor)}
    old_rows=first.rows.clone()
    scheduled,_,pending=pool.next_obligation()
    assert pending.cpu().tolist()==[True,True]
    assert scheduled.cpu().tolist()==[source['checkpoints'][1]['at']]*2
    pool,_,frame=Numbers.run_exact_batch(lambda:pool.advance_one(account,scheduled,rebate))
    record=_take_record(pool.records,roots*pool.capacity+frame.slot)
    price=take_ratio(prepared.taker_prices,roots*prepared.maximum_fills+frame.slot)
    state,second=Numbers.run_exact_batch(lambda:state.finality(sampler,record,frame,price,rebate,scheduled))
    assert torch.equal(first.rows,old_rows)
    for name,before in old.items():
        after=getattr(state.normalizer.bank,name)[:len(before)]
        if before.ndim==2: before=torch.nn.functional.pad(before,(0,after.size(1)-before.size(1)))
        assert torch.equal(after,before),('旧来源前缀',name)
    original={(row['position']['source_watermark']['projection_batch_sequence'],row['position']['record_ordinal']):row
        for row in source['noe_records']}
    categories={'DirectResponse':0,'OrderLifecycle':1,'TradeLifecycle':2,'SettlementLifecycle':3}
    checked=0
    for batch in (first,second):
        assert batch.fault.cpu().tolist()==[0,0]
        width=batch.rows.size(1)
        sequences=values(batch.batch_sequences); ordinals=values(batch.record_ordinals)
        private=values(batch.private_sequences)
        for owner in range(2):
            assert batch.emitted[owner].any().cpu().item()
            assert batch.context_slots[owner].cpu().tolist()==[owner]*width
            assert batch.run_slots[owner].cpu().tolist()==[owner]*width
            for column,wanted in enumerate(batch.emitted[owner].cpu().tolist()):
                if not wanted: continue
                index=owner*width+column
                reference=original[(sequences[index],ordinals[index])]
                body=next(iter(reference['record'].values()))
                source_ref=body.get('source_reference') or body['fact']['source_reference']
                tag,metadata=next(iter(source_ref['record'].items()))
                assert private[index]==metadata['private_feedback_sequence']
                assert batch.source_categories[owner,column].cpu().item()==categories[tag]
                assert batch.platform_at[owner,column].cpu().item()==metadata.get('platform_event_at_utc_ms',metadata.get('platform_responded_at_utc_ms'))
                assert batch.received_at[owner,column].cpu().item()==metadata.get('feedback_received_at_utc_ms',metadata.get('response_received_at_utc_ms'))
                row=batch.rows[owner,column]
                assert state.normalizer.bank.source_order[row].cpu().tolist()==[
                    2,private[index]&0xffffffff,private[index]>>32,body['source_order']['ordinal']]
                expected=Numbers.run_exact_batch(lambda:expected_semantic(reference))
                # 公共codec首字段为所属账户标量；原单根oracle的其它业务字段保持原样。
                assert expected[0,1].cpu().item()==1 and expected[0,2].cpu().item()==0
                expected[0,2]=owner
                actual=state.normalizer.bank.semantic[row]
                length=int(expected[0,0].cpu())
                assert torch.equal(actual[:length+1],expected[0,:length+1]),('双根原业务正文',owner,column)
                assert state.normalizer.bank.logical_keys[row,2].cpu().item()==owner
                assert batch.normalized.outcomes[owner,column].cpu().item()==(2 if 'fact' in body else 1)
                if batch.economic_enabled[owner,column].cpu().item():
                    assert batch.economic_assets[owner,column].cpu().item()==assets[owner].cpu().item()
                checked+=1
        assert not torch.isin(batch.rows[0][batch.emitted[0]],batch.rows[1][batch.emitted[1]]).any().cpu().item()
        sequence=sequences[0]
        assert batch.emitted.sum(1).cpu().tolist()==[sum(key[0]==sequence for key in original)]*2
    return {'actual_two_root_SIM_acceptance_and_next_finality_projection_records':checked,
        'distinct_account_context_run_and_original_source_positions_checked':True,
        'complete_original_business_values_and_old_bank_prefix_checked':True}
