"""显式普通Query发送：原SIM请求登记与Ledger本地关联共同容量/故障提交。

仅接已经按原请求入口形成的单Query动作，不自动触发、重试或选择查询对象。
冷资源增长后调用方使用同一真实输入重算；身份字典继续属于原输入目录。
"""
from dataclasses import replace
import torch
from admission import coordinate


def submit_order_query(runtime,ledger,sampler,identity_bytes,identity_offsets,request_keys,
        methods,input_sequences,sent_at,item_refs,event,original_request,enabled,source_fault):
    requests=runtime.requests
    present=torch.zeros_like(requests.present[:,0])
    present[:,0]=True
    items=torch.full_like(requests.item_refs[:,0],-1)
    items[:,0]=item_refs
    ordinals=torch.zeros_like(items)
    candidate,receipt=requests.register(sampler,identity_bytes,identity_offsets,request_keys,methods,
        input_sequences,sent_at,items,ordinals,present,torch.zeros_like(present),
        torch.zeros_like(sent_at),enabled,source_fault)
    account,association=ledger.record_order_query_association(event,request_keys,original_request,enabled)
    enough=(receipt.required_requests<=requests.capacity)&association.enough
    fault=receipt.fault|association.fault
    committed=enough&(fault==0)
    candidate=candidate.coordinate_result(requests,committed,fault)
    account=replace(account,
        query_associations=coordinate(account.query_associations,ledger.query_associations,committed),
        current_entry_sequence=account.current_entry_sequence.select(committed,ledger.current_entry_sequence),
        current_local_prefix=account.current_local_prefix.select(committed,ledger.current_local_prefix),fault=fault)
    receipt=replace(receipt,registered=receipt.registered&committed,duplicate=receipt.duplicate&committed,fault=fault)
    association=replace(association,changed=association.changed&committed,
        ledger_sequences=association.ledger_sequences.select(committed,association.ledger_sequences.constant(0)),fault=fault)
    return replace(runtime,requests=candidate),account,receipt,association,enough
