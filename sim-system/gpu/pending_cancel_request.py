"""原完整取消pending与逐目标结果/NOE的共同候选。

当前pending未完成时不提交它的部分效果。此前pending的订单、队列、经济和
反馈成功前缀始终保留；市场人口由调用者持有的原ExpandedCancel续接。
"""
from dataclasses import dataclass,replace
import torch
from queues import Numbers
from base_execution import cancel_base_order
from base_acceptance import _coordinate_queue
from pending_base_execution import _coordinate_orders
from cancel_results import CancelTargetResult,CancelResults


@dataclass(frozen=True)
class PendingCancelRequestEffects:
    pending: object
    targets: tuple
    response: object
    required_results: torch.Tensor
    processed: torch.Tensor
    fault: torch.Tensor


def cancel_pending_request(runtime,work,row_roots,result_refs,projection,sampler,asset_tokens,enabled,scenario=None):
    from projection import CancelTargetFeedback
    pending=work.pending
    active=pending.exists & enabled
    orders,queue,account=runtime.orders,runtime.hidden.queues,runtime.account
    source_fault=work.fault|orders.fault|account.fault
    results=runtime.cancel_results if runtime.cancel_results is not None else CancelResults(1)
    current=[];lifecycle=[]
    fault=source_fault
    for target in work.targets:
        read=orders.find(target.order_sequence)
        present=target.active & active
        fault |= torch.where(present,read.fault|target.order_sequence.fault|target.input_item_refs.fault,0)
        cancellable=read.present & (read.quantity.quantity_kind==0) & (
            (read.quantity.open_base_units.sign>0)|(read.quantity.pending_maker_depletion_base_units.sign>0))
        temporary=torch.zeros_like(active)
        if scenario is not None:
            temporary,error=scenario.matches(pending.request_key,target.order_sequence)
            fault |= torch.where(present & cancellable,error,0)
        requested=present & cancellable & ~temporary & (fault==0)
        asset=read.assets.clamp(0,runtime.hidden.initialized.numel()-1)
        closed=runtime.hidden.matching_closed[asset]
        row_ids=torch.where(row_roots==torch.arange(orders.roots,device=row_roots.device)[:,None],
            read.slots[:,None],-1).max(0).values
        queue,quantity,account,canceled,_,_,delta=cancel_base_order(queue,read.quantity,account,
            row_ids,row_roots,closed,requested)
        fault |= quantity.fault|account.fault|delta.fault
        canceled_ok=requested & (fault==0)
        orders=orders.update(read,quantity,canceled_ok,pending.effective_at)
        fault |= orders.fault
        reason=torch.where(~read.present,0,torch.where(~cancellable,1,2))
        reason=torch.where(canceled_ok,-1,reason)
        released=read.quantity.open_encumbered_quote_units.select(canceled_ok,read.quantity.open_encumbered_quote_units.constant(0))
        result=CancelTargetResult(target.order_sequence,target.target_ordinal,target.input_item_refs,pending.effective_at,
            torch.where(canceled_ok,0,1),reason,canceled,released,delta.sequence,delta.emitted,
            present & (fault==0),fault)
        current.append(result)
        lifecycle.append(CancelTargetFeedback(target.order_sequence,target.target_ordinal,target.input_item_refs,
            result.outcome,result.present,delta,fault,read,pending.effective_at))
    results,required,result_fault=results.store(pending,result_refs,current,active & (fault==0))
    fault |= result_fault
    requests,response=runtime.requests.complete(pending,result_refs,active,fault)
    fault |= response.fault
    targets=results.targets_for_response(response)
    # 当前target的生命周期立即发射；全请求控制正文只在原末项封口后发射。
    projection=replace(projection,request_slots=pending.request_key,item_ordinals=pending.ordinal)
    projected,batch=projection.cancellation(sampler,tuple(target.control_feedback() for target in targets),
        response.responded_at,response.received_at,response.method,response.emitted,
        lifecycle_targets=tuple(lifecycle),asset_tokens=asset_tokens)
    fault |= batch.fault
    committed=fault==0
    requests=requests.coordinate_result(runtime.requests,committed,fault)
    orders=_coordinate_orders(orders,runtime.orders,fault)
    queue=_coordinate_queue(queue,runtime.hidden.queues,row_roots.clamp(0,orders.roots-1),fault)
    account=account.coordinate_result(runtime.account,fault)
    results=results.coordinated(runtime.cancel_results or CancelResults(1),committed)
    projected=projected.coordinate_result(projection,committed,fault)
    batch=replace(batch,emitted=batch.emitted & committed[:,None],
        economic_enabled=batch.economic_enabled & committed[:,None],fault=fault)
    response=replace(response,emitted=response.emitted & committed,fault=fault)
    effects=PendingCancelRequestEffects(pending,targets,response,required,active & committed,fault)
    return replace(runtime,requests=requests,orders=orders,account=account,
        hidden=replace(runtime.hidden,queues=queue),cancel_results=results),projected,batch,effects


def run_cancel_pending_request(*args,**kwargs):
    return Numbers.run_exact_batch(lambda:cancel_pending_request(*args,**kwargs))
