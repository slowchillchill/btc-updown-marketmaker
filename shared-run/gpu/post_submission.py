"""原PostOrders发送事实的薄协调；混合GTC/FOK共用请求登记与本地预留关联。

控制面提供真实global/request/item/event身份、send时刻和输入序。此入口不
生成SDK请求正文、平台序或发送时刻；不代替下一批完整Actor发送桥。
"""
from dataclasses import replace
import torch
from admission import coordinate


def submit_post_orders(runtime,ledger,sampler,identity_bytes,identity_offsets,request_keys,methods,
        input_sequences,sent_at,item_refs,ordinals,present,taker_delayed,taker_delay_ms,
        leg_requests,send_events,enabled,source_fault):
    candidate,receipt=runtime.requests.register(sampler,identity_bytes,identity_offsets,request_keys,methods,
        input_sequences,sent_at,item_refs,ordinals,present,taker_delayed,taker_delay_ms,enabled,source_fault)
    account=ledger
    for column in range(present.size(1)):
        account=account.submission_started(leg_requests[:,column],send_events[:,column],request_keys,
            ordinals[:,column],enabled&present[:,column])
    enough=receipt.required_requests<=runtime.requests.capacity
    fault=receipt.fault|account.fault
    committed=enough&(fault==0)
    candidate=candidate.coordinate_result(runtime.requests,committed,fault)
    account=replace(account,reservations=coordinate(account.reservations,ledger.reservations,committed),
        current_entry_sequence=account.current_entry_sequence.select(committed,ledger.current_entry_sequence),
        current_local_prefix=account.current_local_prefix.select(committed,ledger.current_local_prefix),
        global_requests=torch.where(committed[:,None],account.global_requests,ledger.global_requests),
        item_ordinals=torch.where(committed[:,None],account.item_ordinals,ledger.item_ordinals),fault=fault)
    receipt=replace(receipt,registered=receipt.registered&committed,duplicate=receipt.duplicate&committed,fault=fault)
    return replace(runtime,requests=candidate),account,receipt,enough
