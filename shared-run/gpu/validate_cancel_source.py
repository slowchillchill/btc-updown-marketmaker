"""原生产NOE正文的冷解码→设备映射/Core→共享取消领取接缝对照。

冷输入是原强类型引用，不是Registry派生数量。未替代SIM请求时钟、
完整首次领取宿主或原NOE存储；原bank一次装配后各前缀共享复用。
"""
from types import SimpleNamespace
import json
import torch
from queues import Numbers,ExactInt,_take
from fields import RecordList
from core import EvidenceBank,Normalizer,EVIDENCE,OUT_EVIDENCE
from lifecycle import map_cancellation,map_cancellation_completed
from consumption import ClosedRecords
from cancel_projection import cancellation_rows_from_closed,targets_from_rows,append_targets
from validate_base_execution import integers,values


def prepare_cancel_source(references,requests,universe,catalog):
    rows=[]
    item_ids={}
    for reference in references:
        evidence=reference['record']['Evidence']
        control=evidence['evidence']['Control']['result']
        source=evidence['source_reference']['record']['DirectResponse']
        private=evidence['source_order']['sequence']['PrivateFeedback']['private_feedback_sequence']
        source_ordinal=evidence['source_order']['source_ordinal']
        row=dict(reference=reference,source=source,private=private,ordinal=source_ordinal)
        if 'Cancellation' in control:
            association=source['association']['CancellationTarget']
            result=control['Cancellation']
            raw=json.loads(result['order_identity'].split(':',1)[1])[-1]['Order']['source_order_id']
            row.update(kind=15,order=int(raw.removeprefix('sim-')),request=association['global_request_id'],
                target=association['target_ordinal'],outcome=['Canceled','NotCanceledNoEffect','EvidenceUnknownNoEffect'].index(result['outcome']),
                refs=association['input_item_refs'])
            for member in row['refs']:item_ids.setdefault(member['item_id'],len(item_ids))
        else:
            result=control['CancellationRequestCompleted']['result']
            row.update(kind=16,request=source['association']['Request']['global_request_id'],
                counts=[result[name] for name in ['canceled_count','not_canceled_no_effect_count','evidence_unknown_no_effect_count']])
        rows.append(row)
    if not rows:raise ValueError('此原夹具有真实Completed，空来源不是本验证范围')
    def compute():
        bank=None
        for row in rows:
            scalar=lambda value:torch.tensor([value],dtype=torch.int64,device='cuda')
            private=integers([row['private']])
            ordinal=scalar(row['ordinal'])
            enabled=torch.ones(1,dtype=torch.bool,device='cuda')
            if row['kind']==15:
                members=row['refs'];capacity=max(1,len(members))
                refs=RecordList((torch.tensor([item_ids[member['item_id']] for member in members]+[-1]*(capacity-len(members)),device='cuda'),
                    torch.tensor([member['batch_item_ordinal'] for member in members]+[0]*(capacity-len(members)),device='cuda')),
                    torch.tensor([[True]*len(members)+[False]*(capacity-len(members))],device='cuda'))
                mapped=map_cancellation(integers([row['order']]),integers([row['target']]),refs,scalar(row['outcome']),
                    private,ordinal,scalar(0),scalar(requests[row['request']]),enabled)
            else:
                mapped=map_cancellation_completed(*[integers([value]) for value in row['counts']],private,ordinal,
                    scalar(0),scalar(requests[row['request']]),enabled)
            incoming=EvidenceBank(mapped.logical_keys,scalar(0)[:,None],mapped.semantic,
                getattr(mapped,'cancellation_fields',None) if row['kind']==15 else scalar(0)[:,None],
                scalar(EVIDENCE),mapped.source_order,scalar(0),mapped.fault,scalar(row['kind']),frozenset([row['kind']]))
            bank=incoming if bank is None else bank.append(incoming)
        normalizer,batch=Normalizer.empty(bank,1).prepare(torch.arange(len(rows),device='cuda')[None,:],
            torch.ones((1,len(rows)),dtype=torch.bool,device='cuda'))
        return normalizer,batch
    normalizer,normalized=Numbers.run_exact_batch(compute)
    assert normalized.fault.cpu().tolist()==[0] and (normalized.outcomes==OUT_EVIDENCE).all().cpu().item()
    order=normalized.rows.cpu().tolist()[0]  # 验证器冷来源装配，只读取物理不可变列布局。
    ordered=[rows[index] for index in order]
    tensor=lambda entries:torch.tensor([entries],dtype=torch.int64,device='cuda')
    batch=SimpleNamespace(rows=normalized.rows,emitted=normalized.emitted,normalized=normalized,
        request_slots=tensor([requests[row['request']] for row in ordered]),
        private_sequences=integers([row['private'] for row in ordered]),
        batch_sequences=integers([row['reference']['position']['source_watermark']['projection_batch_sequence'] for row in ordered]),
        record_ordinals=integers([row['reference']['position']['record_ordinal'] for row in ordered]),
        context_slots=tensor([catalog['contexts'][row['reference']['position']['context_reference']] for row in ordered]),
        run_slots=tensor([catalog['runs'][row['reference']['position']['source_watermark']['run_id']] for row in ordered]),
        platform_at=tensor([row['source']['platform_responded_at_utc_ms'] for row in ordered]),
        received_at=tensor([row['source']['response_received_at_utc_ms'] for row in ordered]))
    from validate_registry import position_key
    row_index={position_key(row['reference']):index for index,row in enumerate(rows)}
    for key,index in row_index.items():catalog['references'][key]=(index,catalog['references'][key][1])
    return normalizer.bank,[batch],row_index,item_ids


def receive_cancel_source(source,incoming,state,targets,order_sequences,order_present):
    from validate_registry import position_key
    bank,batches,index,item_ids=source
    rows=[index[position_key(reference)] for reference in incoming]
    # 已唯一领取的原Closed引用及可见性；不以Ledger重试刷新delivery或available。
    scalar=lambda entries:torch.tensor([entries],dtype=torch.int64,device='cuda')
    closed=ClosedRecords(scalar(rows),integers([reference['visibility']['Feedback']['delivery_sequence'] for reference in incoming]),
        scalar([reference['visibility']['Feedback']['available_at_utc_ms'] for reference in incoming]),
        torch.ones((1,len(rows)),dtype=torch.bool,device='cuda'),torch.zeros(1,dtype=torch.int32,device='cuda'))
    def compute():
        target,completed=cancellation_rows_from_closed(bank,batches,closed)
        next_targets=append_targets(targets,targets_from_rows(target,order_sequences,order_present))
        return state.receive(completed),next_targets,target,completed
    state,targets,target,completed=Numbers.run_exact_batch(compute)
    assert target.fault.cpu().tolist()==[0] and completed.fault.cpu().tolist()==[0]
    population=target.received.cpu().tolist()[0]
    for column,reference in enumerate(incoming):
        evidence=reference['record']['Evidence'];source_record=evidence['source_reference']['record']['DirectResponse']
        control=evidence['evidence']['Control']['result']
        assert population[column]==('Cancellation' in control)
        if population[column]:
            association=source_record['association']['CancellationTarget']
            assert values(target.target_ordinals)[column]==association['target_ordinal']
            mask=target.input_item_refs.present.cpu().tolist()[column]
            actual_ids=target.input_item_refs.fields[0].cpu().reshape(len(rows),-1).tolist()[column]
            actual_ordinals=target.input_item_refs.fields[1].cpu().reshape(len(rows),-1).tolist()[column]
            assert [(item_ids[member['item_id']],member['batch_item_ordinal']) for member in association['input_item_refs']]==[
                (identifier,ordinal) for identifier,ordinal,present in zip(actual_ids,actual_ordinals,mask) if present]
    return state,targets
