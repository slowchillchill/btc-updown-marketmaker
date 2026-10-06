"""原两市场maker真实NOE日结/付款的GPU归属对照；不运行正式训练。"""
import json
import sys
import time
from pathlib import Path
from fractions import Fraction
import torch
sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'sim-system/gpu'))
from queues import Numbers
from validate_base_execution import integers,rational,values
sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'ledger/gpu'))
from rebate_contribution import RebateContributions
from rebate_day import RebateDays
sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'execution-normalization/gpu'))
from fields import IntegerList,RecordList,encode_fields,decode_fields
from lifecycle import map_day_finalized,map_rebate_payment,map_maker_finalized
from core import EvidenceBank,Normalizer,EVIDENCE,OUT_EVIDENCE,OUT_DUPLICATE,OUT_LOGICAL_CONFLICT


def amounts(value):
    return [Fraction(n,d) for n,d in zip(values(value.numerator),values(value.denominator))]


def identity(value):
    return int(json.loads(value.split(':',1)[1])[-1]['Trade']['source_trade_id'].removeprefix('sim-'))


def settlement_results(value):
    if isinstance(value,dict):
        if 'SettlementLifecycleV2' in value:
            yield value['SettlementLifecycleV2']['result']
        else:
            for child in value.values():
                yield from settlement_results(child)
    elif isinstance(value,list):
        for child in value:
            yield from settlement_results(child)


def main():
    source=json.load(sys.stdin)
    started=time.perf_counter()
    # V4来源保留原缺前驱尝试；Mapper等值对照只取原最终成功应用的那次引用。
    mapping_records=[record for record in source['records'] if not record.get('deferred',False)]
    results=list(settlement_results(mapping_records))
    vectors=[value['MakerAccrualFinalized'] for value in results if 'MakerAccrualFinalized' in value]
    finalized=next(value['DayFinalized'] for value in results if 'DayFinalized' in value)
    payment=next(value['Payment'] for value in results if 'Payment' in value)
    assert len(vectors)==2 and finalized['kind']=='Maker' and payment['kind']=='Maker'
    condition={name:i for i,name in enumerate(source['conditions'])}
    enabled=torch.tensor([True],device='cuda')
    kind=torch.tensor([0],device='cuda')
    day=torch.tensor([finalized['attribution_day_start_utc_ms']],device='cuda')
    refs=integers([identity(value) for value in finalized['contribution_refs']])
    ref_mask=torch.ones((1,len(finalized['contribution_refs'])),dtype=torch.bool,device='cuda')
    # 原Day贡献引用列表保留实际人口/原顺序；物理字宽或人口尾补齐不得改变正文。
    encoded=Numbers.run_exact_batch(lambda:encode_fields([kind,day,IntegerList(refs,ref_mask)]))
    padded_ids=[identity(value) for value in finalized['contribution_refs']]+[0,0]
    padded_refs=Numbers.run_exact_batch(lambda:integers(padded_ids),initial_word_capacity=8)
    padded_mask=torch.nn.functional.pad(ref_mask,(0,2),value=False)
    padded=Numbers.run_exact_batch(lambda:encode_fields([kind,day,IntegerList(padded_refs,padded_mask)]))
    common=max(encoded.size(1),padded.size(1))
    assert torch.equal(torch.nn.functional.pad(encoded,(0,common-encoded.size(1))),
        torch.nn.functional.pad(padded,(0,common-padded.size(1))))
    decoded,error=Numbers.run_exact_batch(lambda:decode_fields(padded,['scalar','scalar','integer_list'],enabled))
    assert error.cpu().tolist()==[0] and decoded[0].cpu().tolist()==[0] and torch.equal(decoded[1],day)
    ref_values=values(decoded[2].values)
    assert [value for value,present in zip(ref_values,decoded[2].present.flatten().cpu().tolist()) if present]==padded_ids[:-2]
    zero=torch.zeros_like(day)
    evidence=[]
    maker_vectors_checked=0
    for original in mapping_records:
        body=original['record'].get('Evidence')
        if body is None:
            continue
        settlement=body['evidence'].get('SettlementLifecycleV2')
        if settlement is None:
            continue
        result=settlement['result']
        if 'MakerAccrualFinalized' in result:
            vector=result['MakerAccrualFinalized']
            children=vector['contributions']
            trade=integers([identity(child['trade_identity']) for child in children])
            conditions=torch.tensor([condition[child['condition_id']] for child in children],device='cuda')
            kinds=torch.tensor([{'Maker':0,'Taker':1}[child['kind']] for child in children],device='cuda')
            days=torch.tensor([child['attribution_day_start_utc_ms'] for child in children],device='cuda')
            contributions=RecordList((trade,conditions,kinds,days,rational([child['contribution_pusd'] for child in children])),
                torch.ones((1,len(children)),dtype=torch.bool,device='cuda'))
            reference=body['source_reference']['record']['SettlementLifecycle']
            private=integers([reference['private_feedback_sequence']])
            ordinal=torch.tensor([body['source_order']['ordinal']],device='cuda')
            outer=torch.tensor([condition[vector['condition_id']]],device='cuda')
            status=torch.tensor([{'Calculated':0,'ZeroNoEligibleMakerFill':1,'UnknownIncomplete':2}[vector['status']]],device='cuda')
            mapped=Numbers.run_exact_batch(lambda:map_maker_finalized(outer,status,contributions,private,ordinal,zero,enabled))
            assert mapped.emitted.cpu().tolist()==[True] and mapped.fault.cpu().tolist()==[0]
            assert json.loads(body['logical_identity'].split(':',1)[1])[-1]=={'SettlementLifecycle':{'private_feedback_sequence':values(private)[0]}}
            assert torch.equal(mapped.logical_keys,encode_fields([zero,torch.full_like(zero,9),private]))
            decoded,error=Numbers.run_exact_batch(lambda:decode_fields(mapped.semantic,
                ['scalar','scalar','scalar',('record_list',('integer','scalar','scalar','scalar','rational'))],enabled))
            assert error.cpu().tolist()==[0] and decoded[0].cpu().tolist()==[0] and torch.equal(decoded[1],outer) and torch.equal(decoded[2],status)
            actual=decoded[3]
            mask=actual.present.flatten().cpu().tolist()
            assert [value for value,live in zip(values(actual.fields[0]),mask) if live]==values(trade)
            for member,expected_member in zip(actual.fields[1:4],[conditions,kinds,days]):
                assert [value for value,live in zip(member.cpu().tolist(),mask) if live]==expected_member.cpu().tolist()
            assert [value for value,live in zip(amounts(actual.fields[4]),mask) if live]==[Fraction(child['contribution_pusd']) for child in children]
            maker_vectors_checked+=1
        if not ('DayFinalized' in result or 'Payment' in result):
            continue
        reference=body['source_reference']['record']['SettlementLifecycle']
        private=integers([reference['private_feedback_sequence']])
        ordinal=torch.tensor([body['source_order']['ordinal']],device='cuda')
        actual_key=json.loads(body['logical_identity'].split(':',1)[1])[-1]
        if 'DayFinalized' in result:
            assert actual_key=={'RebateDay':{'kind':'Maker','attribution_day_start_utc_ms':int(day.cpu()[0])}}
            mapped=Numbers.run_exact_batch(lambda:map_day_finalized(kind,day,rational([finalized['accrued_pusd']]),
                rational([finalized['payable_pusd']]),rational([finalized['locked_remainder_pusd']]),
                IntegerList(refs,ref_mask),private,ordinal,zero,enabled))
            expected=Numbers.run_exact_batch(lambda:encode_fields([zero,kind,day,rational([finalized['accrued_pusd']]),
                rational([finalized['payable_pusd']]),rational([finalized['locked_remainder_pusd']]),IntegerList(refs,ref_mask)]))
            expected_key=encode_fields([zero,torch.full_like(zero,12),kind,day])
        else:
            assert actual_key=={'SettlementLifecycle':{'private_feedback_sequence':values(private)[0]}}
            day_reference=json.loads(payment['day_result_identity'].split(':',1)[1])
            own_identity=json.loads(body['logical_identity'].split(':',1)[1])
            assert day_reference[1]==own_identity[1] and day_reference[-1]=={'RebateDay':{'kind':'Maker','attribution_day_start_utc_ms':int(day.cpu()[0])}}
            mapped=Numbers.run_exact_batch(lambda:map_rebate_payment(kind,day,rational([payment['paid_pusd']]),private,ordinal,zero,enabled))
            expected=Numbers.run_exact_batch(lambda:encode_fields([zero,kind,day,zero,torch.full_like(zero,12),kind,day,rational([payment['paid_pusd']])]))
            expected_key=encode_fields([zero,torch.full_like(zero,9),private])
        assert torch.equal(mapped.semantic,expected) and torch.equal(mapped.logical_keys,expected_key)
        assert mapped.emitted.cpu().tolist()==[True] and mapped.fault.cpu().tolist()==[0]
        assert mapped.source_order.cpu().tolist()==[[2,values(private)[0]&0xffffffff,values(private)[0]>>32,body['source_order']['ordinal']]]
        incoming=EvidenceBank(mapped.logical_keys,zero[:,None],mapped.semantic,zero[:,None],zero,
            mapped.source_order,zero,mapped.fault,torch.full_like(zero,mapped.payload_schema))
        evidence.append(incoming)
    assert len(evidence)==2
    assert maker_vectors_checked==2
    normalizer=Normalizer.empty(evidence[0],1).append_bank(evidence[1])
    rows=torch.tensor([[0,1]],device='cuda')
    normalizer,result=normalizer.prepare(rows,torch.ones_like(rows,dtype=torch.bool))
    assert result.outcomes.cpu().tolist()==[[OUT_EVIDENCE,OUT_EVIDENCE]]
    normalizer,repeat=normalizer.prepare(rows,torch.ones_like(rows,dtype=torch.bool))
    assert repeat.outcomes.cpu().tolist()==[[OUT_DUPLICATE,OUT_DUPLICATE]]
    # 同逻辑身份、相同数值token但另一正文类型仍是Conflict；类型不另造逻辑身份。
    from dataclasses import replace
    normalizer=normalizer.append_bank(replace(evidence[1],payload_schemas=torch.full_like(zero,9)))
    normalizer,conflict=normalizer.prepare(torch.tensor([[2]],device='cuda'),enabled[:,None])
    assert conflict.outcomes.cpu().tolist()==[[OUT_LOGICAL_CONFLICT]]
    index=Numbers.run_exact_batch(lambda:RebateContributions.empty(1,len(source['contributions']),day.device))
    state=Numbers.run_exact_batch(lambda:RebateDays.empty(torch.tensor([list(range(len(condition)))],device='cuda'),1))
    def finalize(current):
        return current.finalize(index,kind,day,rational([finalized['accrued_pusd']]),
            rational([finalized['payable_pusd']]),rational([finalized['locked_remainder_pusd']]),refs,ref_mask,enabled)
    state,ready,changed=Numbers.run_exact_batch(lambda:finalize(state))
    assert ready.cpu().tolist()==[False] and not state.occupied.any().cpu().item() and state.fault.cpu().tolist()==[0]
    state,ready,changed=Numbers.run_exact_batch(lambda:state.payment(kind,day,rational([payment['paid_pusd']]),enabled))
    assert ready.cpu().tolist()==[False] and not state.payment_observed.any().cpu().item()
    for vector in vectors:
        for value in vector['contributions']:
            index,required,used=Numbers.run_exact_batch(lambda:index.add(integers([identity(value['trade_identity'])]),
                torch.tensor([0],device='cuda'),torch.tensor([value['attribution_day_start_utc_ms']],device='cuda'),
                torch.tensor([condition[value['condition_id']]],device='cuda'),rational([value['contribution_pusd']]),enabled))
            assert used.cpu().tolist()==[True] and index.fault.cpu().tolist()==[0]
    state,ready,changed=Numbers.run_exact_batch(lambda:finalize(state))
    assert ready.cpu().tolist()==[True] and changed.cpu().tolist()==[True] and state.fault.cpu().tolist()==[0]
    exact=lambda value:Fraction(int(value['numerator']),int(value['denominator']))
    assert amounts(state.condition_payable)==[exact(value) for value in source['expected_pay']]
    assert amounts(state.condition_locked)==[exact(value) for value in source['expected_lock']]
    assert sum(amounts(state.condition_payable))==Fraction(finalized['payable_pusd'])
    assert sum(amounts(state.condition_locked))==Fraction(finalized['locked_remainder_pusd'])
    state,ready,changed=Numbers.run_exact_batch(lambda:state.payment(kind,day,rational([payment['paid_pusd']]),enabled))
    assert ready.cpu().tolist()==[True] and changed.cpu().tolist()==[True]
    again,ready,changed=Numbers.run_exact_batch(lambda:state.payment(kind,day,rational([payment['paid_pusd']]),enabled))
    assert ready.cpu().tolist()==[True] and changed.cpu().tolist()==[False]
    assert amounts(again.condition_payable)==amounts(state.condition_payable)
    restored=Numbers.run_exact_batch(lambda:state.resized(2),initial_word_capacity=8)
    assert amounts(restored.condition_payable)[:len(condition)]==amounts(state.condition_payable)
    assert restored.payment_observed.cpu().tolist()==[[True,False]]
    consumer={}
    if source.get('source_contract')=='execution_normalization_v4':
        from gpu_rebate_admission_validation import validate
        consumer=validate(source)
    print(json.dumps({'actual_maker_vectors':len(vectors),'actual_contributions':len(source['contributions']),
        'actual_DayFinalized_and_Payment':True,'missing_predecessors_no_commit':True,
        'condition_finite_payable_locked_checked':True,'source_totals_conserved':True,'payment_repeat_no_second_allocation':True,
        'actual_day_reference_integer_list_and_padding_checked':True,
        'actual_day_payment_identity_body_source_order_and_repeat_checked':True,
        'actual_maker_full_vector_body_decode_checked':maker_vectors_checked,
        **consumer,
        'elapsed_seconds':time.perf_counter()-started,'device_peak_allocated_bytes':torch.cuda.max_memory_allocated(),
        'full_NOE_Ledger_day_coordinator_delivered':False}))


if __name__=='__main__':
    main()
