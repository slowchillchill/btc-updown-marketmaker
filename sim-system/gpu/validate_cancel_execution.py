"""实际GPU取消释放效果→反馈/Core→双门唯一领取→Registry的有限原对照。

原外层请求提供四条V4引用作oracle；GPU输出由自身cancel delta生成。
一次已接单前缀及发射序初始化，不代表完整全局时钟或账户恢复。
"""
from pathlib import Path
from dataclasses import replace
import sys
import torch
from queues import Numbers
from latency import EmpiricalDelaySampler
from feedback import FeedbackEmitter
from validate_base_execution import integers,rational,values


def validate_actual_cancel(source,delta):
    root=Path(__file__).resolve().parents[2]
    sys.path[:0]=[str(root/'execution-normalization/gpu'),str(root/'shared-run/gpu')]
    from projection import Projection,CancelTargetFeedback
    from validate_projection import expected_semantic
    from fields import RecordList
    from consumption import PendingRecords,ReceiveGates
    from cancel_projection import cancellation_rows_from_closed,cancellation_order_events
    from registry_projection import order_events_from_closed
    from registry import PublicRegistryState,FrozenIntentOrders,PublicCancellationState,CancellationCompletions
    zero=torch.zeros(1,dtype=torch.int64,device='cuda')
    active=torch.ones(1,dtype=torch.bool,device='cuda')
    at=torch.tensor([source['cancel_arrived_at']],device='cuda')
    sha='98fd40f843275db8994a29cec1de6a72f95bbaa4309df9779c7ed1285cdff596'
    sampler=EmpiricalDelaySampler.build((root/'sim-system/config/latency-profiles'/sha/'profile.json').read_bytes(),sha,[source['session_seed'].encode('ascii')])
    emitter=replace(FeedbackEmitter.open(1),next_private_sequence=integers([source['next_private']]),next_batch_sequence=integers([source['next_batch']]))
    state=Projection.open(emitter,zero,zero,zero,zero,torch.tensor([0,0],device='cuda'),integers([1]),zero,zero,zero,zero)
    originals={reference['position']['record_ordinal']:reference for reference in source['cancel_records']}
    direct=next(body['record']['Evidence']['source_reference']['record']['DirectResponse'] for body in originals.values()
        if 'Evidence' in body['record'] and 'DirectResponse' in body['record']['Evidence']['source_reference']['record'])
    # 请求入站时延已在原发送登记抽定；工程入口只复用该请求抽样结果，不重抽feedback。
    received=torch.tensor([direct['response_received_at_utc_ms']],device='cuda')
    refs=RecordList((zero,zero),active[:,None])  # 冷目录0绑定真实gpu-legal-gtc-cancel-item。
    target=CancelTargetFeedback(integers([1]),integers([0]),refs,zero,active,delta,delta.fault)
    state,batch=Numbers.run_exact_batch(lambda:state.cancellation(sampler,(target,),at,received,torch.ones_like(zero),active))
    assert batch.fault.cpu().tolist()==[0]
    metadata={ 'DirectResponse':0,'OrderLifecycle':1}
    for column,wanted in enumerate(batch.emitted.cpu().tolist()[0]):
        if not wanted:continue
        ordinal=values(batch.record_ordinals)[column]
        original=originals[ordinal]
        body=next(iter(original['record'].values()))
        reference=body.get('source_reference') or body['fact']['source_reference']
        tag,record=next(iter(reference['record'].items()))
        assert values(batch.private_sequences)[column]==record['private_feedback_sequence']
        assert values(batch.batch_sequences)[column]==original['position']['source_watermark']['projection_batch_sequence']
        assert batch.source_categories.cpu().tolist()[0][column]==metadata[tag]
        assert batch.received_at.cpu().tolist()[0][column]==record['response_received_at_utc_ms' if tag=='DirectResponse' else 'feedback_received_at_utc_ms']
        row=batch.rows[0,column]
        expected=Numbers.run_exact_batch(lambda:expected_semantic(original))
        actual=state.normalizer.bank.semantic[row]
        count=int(expected[0,0].cpu())
        assert torch.equal(actual[:count+1],expected[0,:count+1]),(ordinal,'原强类型取消/释放正文')
    pending=PendingRecords.from_columns(batch.emitted,batch.received_at,batch.private_sequences,
        integers([int(state.normalizer.bank.source_order[row,3].cpu()) for row in batch.rows[0]]),batch.rows,integers([0]))
    gates=ReceiveGates.unset(1)
    pending,empty=Numbers.run_exact_batch(lambda:pending.take_closed(gates))
    assert not empty.emitted.any().cpu().item()
    closed_at=torch.tensor([source['cancel_closed_at']],device='cuda')
    gates=gates.set_receive(closed_at,active).set_public(closed_at,active)
    pending,closed=Numbers.run_exact_batch(lambda:pending.take_closed(gates))
    assert closed.emitted.sum().cpu().item()==4
    _,repeat=Numbers.run_exact_batch(lambda:pending.take_closed(gates))
    assert not repeat.emitted.any().cpu().item()
    rank=torch.arange(closed.record_refs.size(1),device='cuda')[None,:]
    rows,completions=Numbers.run_exact_batch(lambda:cancellation_rows_from_closed(state.normalizer.bank,[batch],closed))
    assert rows.received.sum().cpu().item()==1 and completions.received.sum().cpu().item()==1
    directory=FrozenIntentOrders(active[:,None],integers([1]),active[:,None],rational(['5']),torch.zeros((1,1),dtype=torch.bool,device='cuda'),active[:,None])
    def registry_step():
        target_events=cancellation_order_events(rows,rank)
        # 控制Canceled自身先关闭；尚未接纳CallerCanceled也不能继续报可撤。
        target_state=PublicRegistryState(target_events)
        target_only=target_state.freeze_orders(directory,closed_at)
        combined=target_state.receive(order_events_from_closed(state.normalizer.bank,[batch],closed,rank))
        return target_only,combined.freeze_orders(directory,closed_at)
    first,final=Numbers.run_exact_batch(registry_step)
    assert first.fault.cpu().tolist()==[0] and final.fault.cpu().tolist()==[0]
    assert first.cancellable_known.cpu().tolist()==[[True]] and first.cancellable.cpu().tolist()==[[False]]
    assert final.cancellable.cpu().tolist()==[[False]]
    frozen=Numbers.run_exact_batch(lambda:PublicCancellationState(integers([0]),active[:,None],completions).freeze_global(closed_at))
    assert values(frozen.pending_count)==[0] and values(frozen.response_counts)==[1,0,0]
    print('stage=actual_GPU_cancel_to_NOE_Registry records=4 Control_before_CallerCanceled=true full_root=false',file=sys.stderr)
