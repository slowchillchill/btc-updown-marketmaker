"""已准入取消target的持续设备候选；完成请求与资金/订单/队列一同提交。

这里处理一个原pending item，不展开CancelMarket、不封多target反馈正文。
所有target结果仍须保存至原请求完成，随后统一发Direct；NoEffect没有经济delta。
"""
from dataclasses import dataclass,replace
import torch
from queues import Numbers
from base_execution import cancel_base_order
from base_acceptance import _coordinate_queue
from pending_base_execution import _coordinate_orders


@dataclass(frozen=True)
class BaseCancelTarget:
    item_refs: torch.Tensor
    order_sequence: object
    fault: torch.Tensor


@dataclass(frozen=True)
class PendingCancelEffects:
    pending: object
    order: object
    order_sequence: object
    outcome: torch.Tensor
    processed: torch.Tensor
    delta: object
    response: object
    fault: torch.Tensor


def cancel_pending_base_target(requests,pending,orders,queue,account,target,row_roots,matching_closed,
                               result_refs,enabled):
    if pending.source is not requests:
        raise ValueError('取消须绑定选择pending时的同一请求状态')
    if enabled.shape!=requests.fault.shape or enabled.dtype!=torch.bool or enabled.device!=requests.fault.device:
        raise TypeError('实际取消phase需同设备bool[roots]')
    active=pending.exists & enabled
    read=orders.find(target.order_sequence)
    source_fault=requests.fault | orders.fault | account.fault
    source_fault=source_fault | torch.where(active,target.fault | target.order_sequence.fault |
        read.fault | (target.item_refs!=pending.item_ref).to(torch.int32),0)
    original_orders,original_queue,original_account=orders,queue,account
    requested=active & read.present & (source_fault==0)
    queue,quantity,account,_,_,_,delta=cancel_base_order(queue,read.quantity,account,
        torch.where(row_roots==torch.arange(orders.roots,device=row_roots.device)[:,None],
            read.slots[:,None],-1).max(0).values,row_roots,matching_closed,requested)
    orders=orders.update(read,quantity,delta.emitted)
    fault=source_fault | orders.fault | account.fault | delta.fault
    processed=active & (fault==0)
    requests,response=requests.complete(pending,result_refs,processed,fault)
    fault=fault | response.fault
    valid=fault==0
    orders=_coordinate_orders(orders,original_orders,fault)
    queue=_coordinate_queue(queue,original_queue,row_roots.clamp(0,orders.roots-1),fault)
    account=account.coordinate_result(original_account,fault)
    delta=account.coordinate_delta(delta,valid,fault)
    outcome=torch.where(delta.emitted,0,1).to(torch.int64)
    return requests,orders,queue,account,PendingCancelEffects(pending,read,target.order_sequence,outcome,
        processed & valid,delta,replace(response,emitted=response.emitted & valid,fault=fault),fault)


def run_pending_base_cancel(requests,orders,queue,account,target,row_roots,matching_closed,result_refs,enabled):
    # 不分配新order/fill段；精确临时资源重试始终从同一原输入及状态开始。
    pending=requests.next_pending()
    return Numbers.run_exact_batch(lambda:cancel_pending_base_target(requests,pending,orders,queue,account,
        target,row_roots,matching_closed,result_refs,enabled))
