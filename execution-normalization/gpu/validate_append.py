"""真实已生成21条NOE的分页持续接纳；参考只检查结果，不驱动apply顺序。"""
from dataclasses import replace
from pathlib import Path
import sys
import torch
from queues import ExactInt,Numbers,_take
from transport import compact_committed_batches,select_batch_columns
from validate_base_execution import values


def check_incremental_receive(bank,batches,delivered,start,expected,gates):
    sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'shared-run/gpu'))
    from memory import EconomicColumns
    from admission_lifecycle import LifecycleColumns
    from closed import ClosedAdmission
    from consumption import PendingRecords
    from registry import PublicRegistryState,PublicRegistryEvents
    from public_ledger import PublicLedgerState
    run=Numbers.run_exact_batch
    (all_rows,),_=run(lambda:compact_committed_batches(batches))
    # 选择器只读取真实原领取顺序的设备ref；不从原Ledger attempts安排业务。
    same=all_rows.rows[:,:,None]==delivered.record_refs[:,None,:]
    positions=same.to(torch.int64).argmax(1)[:,:21]
    ordered=select_batch_columns(all_rows,positions)
    private=values(ordered.private_sequences)
    assert private[7]!=private[8] or ordered.received_at[0,7].cpu().item()!=ordered.received_at[0,8].cpu().item()
    first=select_batch_columns(ordered,torch.arange(8,device=positions.device)[None,:])
    later=select_batch_columns(ordered,torch.arange(8,21,device=positions.device)[None,:])
    first_columns=run(lambda:EconomicColumns.from_projection_batches([first]))
    # 开户/本地预留只初始化一次。旧身份目录可复用，后续仅追加记录列。
    initial=replace(start,columns=first_columns,
        lifecycle=run(lambda:LifecycleColumns.from_projection(bank,first_columns)),
        applied=torch.zeros_like(first_columns.enabled),
        applied_sequences=ExactInt.from_i64_tensor(torch.zeros(8,dtype=torch.int64,device=positions.device)),
        available_at=torch.zeros_like(first_columns.row_refs),
        order_funds=replace(start.order_funds,unassigned_records=start.order_funds.unassigned_records[:,:8]))
    pending=run(lambda:PendingRecords.from_columns(first.emitted,first.received_at,first.private_sequences,
        ExactInt.from_i64_tensor(bank.source_order[first.rows.flatten(),3]),first.rows,
        ExactInt.from_i64_tensor(torch.zeros(1,dtype=torch.int64,device=positions.device))))
    state=PublicLedgerState(pending,PublicRegistryState(PublicRegistryEvents.empty(1,positions.device)),
        ClosedAdmission.empty(first_columns),initial)
    state,first_delivery=run(lambda:state.receive(bank,gates,torch.ones_like(first.emitted),passes=3))
    waiting=state.waiting.received & ~state.waiting.consumed
    assert waiting.any().cpu().item() and not state.ledger.fault.any().cpu().item()
    prefix=values(state.ledger.current_entry_sequence)
    first_visible=state.waiting.available_at.clone()
    first_rank=state.waiting.receive_rank.clone()
    first_sequences=values(state.waiting.delivered_sequences)
    original_balance=values(state.ledger.platform.balances.numerator)
    # 不同纯批字容量也是合法来源布局；增长不截身份或真实金额。
    later=replace(later,private_sequences=later.private_sequences.with_capacity(8))
    state=run(lambda:state.append_batch(bank,later))
    assert values(state.ledger.current_entry_sequence)==prefix
    assert values(state.ledger.platform.balances.numerator)==original_balance
    assert torch.equal(state.waiting.available_at[:,:8],first_visible)
    assert torch.equal(state.waiting.receive_rank[:,:8],first_rank)
    assert values(_take(state.waiting.delivered_sequences,torch.arange(8,device=positions.device)))==first_sequences
    assert torch.equal(state.waiting.received[:,:8]&~state.waiting.consumed[:,:8],waiting)
    state,last_delivery=run(lambda:state.receive(bank,gates,torch.ones_like(state.ledger.columns.enabled),passes=3))
    assert not (state.waiting.received & ~state.waiting.consumed).any().cpu().item()
    assert state.ledger.fault.cpu().tolist()==[0]
    for name in ['current_entry_sequence','current_local_prefix']:
        assert values(getattr(state.ledger,name))==values(getattr(expected,name))
    assert values(state.ledger.platform.platform_source_prefix)==values(expected.platform.platform_source_prefix)
    for name in ['words','sign','fault']:
        actual=getattr(state.ledger.platform.balances.numerator,name)
        oracle=getattr(expected.platform.balances.numerator,name)
        if name=='words':
            capacity=max(actual.size(1),oracle.size(1))
            actual=state.ledger.platform.balances.numerator.with_capacity(capacity).words
            oracle=expected.platform.balances.numerator.with_capacity(capacity).words
        assert torch.equal(actual,oracle)
    positions=state.ledger.columns.positions
    before_positions=expected.columns.positions
    matches=(positions[:,None,:]==before_positions[None,:,:]).all(2)
    assert matches.to(torch.int64).sum(1).cpu().tolist()==[1]*21
    order=matches.to(torch.int64).argmax(1)
    assert values(state.ledger.applied_sequences)==values(_take(expected.applied_sequences,order))
    assert torch.equal(state.waiting.available_at[:,:8],first_visible)
    delivered_values=values(first_delivery.delivered_sequences)+values(last_delivery.delivered_sequences)
    delivered_active=first_delivery.emitted.flatten().cpu().tolist()+last_delivery.emitted.flatten().cpu().tolist()
    assert [v for v,on in zip(delivered_values,delivered_active) if on]==values(delivered.delivered_sequences)[:21]
    saved_width=state.ledger.columns.row_refs.size(1)
    empty=replace(later,emitted=torch.zeros_like(later.emitted))
    again=run(lambda:state.append_batch(bank,empty))
    assert again.ledger.columns.row_refs.size(1)==saved_width
    again,feedback=run(lambda:again.receive(bank,gates,torch.ones_like(again.ledger.columns.enabled),passes=1))
    assert not feedback.closed.emitted.any().cpu().item()
    assert values(again.ledger.current_entry_sequence)==values(state.ledger.current_entry_sequence)
    assert values(again.pending_noe.delivered_through)==values(state.pending_noe.delivered_through)
    damaged=replace(empty,fault=torch.ones_like(empty.fault))
    damaged=run(lambda:state.append_batch(bank,damaged))
    assert damaged.ledger.columns.row_refs.size(1)==saved_width and damaged.ledger.fault.cpu().tolist()==[1]
    return True
