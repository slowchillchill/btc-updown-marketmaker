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
    # 第8条为原seq3；把同received毫秒全部纳入，不能拆开时间闭合前沿。
    first_at=ordered.received_at[:,7]
    first_width=int((ordered.received_at<=first_at[:,None]).to(torch.int64).sum(1).max().cpu())
    assert 8<=first_width<21
    first=select_batch_columns(ordered,torch.arange(first_width,device=positions.device)[None,:])
    later=select_batch_columns(ordered,torch.arange(first_width,21,device=positions.device)[None,:])
    first_gates=replace(gates,closed_receive_at=first_at)
    assert (first.received_at<=first_gates.closed_receive_at[:,None]).all().cpu().item()
    first_columns=run(lambda:EconomicColumns.from_projection_batches([first]))
    # 开户/本地预留只初始化一次。旧身份目录可复用，后续仅追加记录列。
    initial=replace(start,columns=first_columns,
        lifecycle=run(lambda:LifecycleColumns.from_projection(bank,first_columns)),
        applied=torch.zeros_like(first_columns.enabled),
        applied_sequences=ExactInt.from_i64_tensor(torch.zeros(first_width,dtype=torch.int64,device=positions.device)),
        available_at=torch.zeros_like(first_columns.row_refs),
        order_funds=replace(start.order_funds,unassigned_records=start.order_funds.unassigned_records[:,:first_width]))
    pending=run(lambda:PendingRecords.from_columns(first.emitted,first.received_at,first.private_sequences,
        ExactInt.from_i64_tensor(bank.source_order[first.rows.flatten(),3]),first.rows,
        ExactInt.from_i64_tensor(torch.zeros(1,dtype=torch.int64,device=positions.device))))
    state=PublicLedgerState(pending,PublicRegistryState(PublicRegistryEvents.empty(1,positions.device)),
        ClosedAdmission.empty(first_columns),initial)
    state,first_delivery=run(lambda:state.receive(bank,first_gates,torch.ones_like(first.emitted),passes=3))
    waiting=state.waiting.received & ~state.waiting.consumed
    assert waiting.any().cpu().item() and not state.ledger.fault.any().cpu().item(), (first_width,first_at.cpu().tolist())
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
    assert torch.equal(state.waiting.available_at[:,:first_width],first_visible)
    assert torch.equal(state.waiting.receive_rank[:,:first_width],first_rank)
    assert values(_take(state.waiting.delivered_sequences,torch.arange(first_width,device=positions.device)))==first_sequences
    assert torch.equal(state.waiting.received[:,:first_width]&~state.waiting.consumed[:,:first_width],waiting)
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
    matches=(positions[:,None,:]==before_positions[None,:,:]).all(2) & expected.lifecycle.emitted.flatten()[None,:]
    assert matches.to(torch.int64).sum(1).cpu().tolist()==[1]*21
    order=matches.to(torch.int64).argmax(1)
    assert values(state.ledger.applied_sequences)==values(_take(expected.applied_sequences,order))
    assert torch.equal(state.waiting.available_at[:,:first_width],first_visible)
    delivered_values=values(first_delivery.closed.delivered_sequences)+values(last_delivery.closed.delivered_sequences)
    delivered_active=first_delivery.closed.emitted.flatten().cpu().tolist()+last_delivery.closed.emitted.flatten().cpu().tolist()
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
    return {'first_page_records':first_width,'appended_records':21-first_width,
        'first_closed_receive_at':first_at.cpu().tolist(),'retained_waiting_records':int(waiting.sum().cpu()),
        'same_received_time_fully_in_first_page':True}


def check_incremental_rebate_batches(bank,batches,start,expected,gates):
    """原Maker完整来源批逐批追加，最后统一合法双门领取。

    此处检查新批解码/向量人口，不声称宿主已随真实义务自动推进时间。
    """
    sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'shared-run/gpu'))
    from memory import EconomicColumns
    from admission_lifecycle import LifecycleColumns
    from closed import ClosedAdmission
    from consumption import PendingRecords
    from registry import PublicRegistryState,PublicRegistryEvents
    from public_ledger import PublicLedgerState
    run=Numbers.run_exact_batch
    (first,),_=run(lambda:compact_committed_batches([batches[0]]))
    columns=run(lambda:EconomicColumns.from_projection_batches([first]))
    roots,width=columns.row_refs.shape
    device=columns.row_refs.device
    initial=replace(start,columns=columns,lifecycle=run(lambda:LifecycleColumns.from_projection(bank,columns)),
        applied=torch.zeros_like(columns.enabled),available_at=torch.zeros_like(columns.row_refs),
        applied_sequences=ExactInt.from_i64_tensor(torch.zeros(roots*width,dtype=torch.int64,device=device)),
        order_funds=replace(start.order_funds,unassigned_records=torch.zeros_like(columns.enabled)))
    pending=run(lambda:PendingRecords.from_columns(first.emitted,first.received_at,first.private_sequences,
        ExactInt.from_i64_tensor(bank.source_order[first.rows.flatten(),3]),first.rows,
        ExactInt.from_i64_tensor(torch.zeros(roots,dtype=torch.int64,device=device))))
    state=PublicLedgerState(pending,PublicRegistryState(PublicRegistryEvents.empty(roots,device)),
        ClosedAdmission.empty(columns),initial)
    old_entry=values(state.ledger.current_entry_sequence)
    for batch in batches[1:]:
        state=run(lambda:state.append_batch(bank,batch))
        assert values(state.ledger.current_entry_sequence)==old_entry
        assert not state.waiting.received.any().cpu().item()
    assert state.ledger.lifecycle.maker_contributions.present.any().cpu().item()
    assert state.ledger.lifecycle.rebate_refs.present.any().cpu().item()
    state,delivery=run(lambda:state.receive(bank,gates,torch.ones_like(state.ledger.columns.enabled)))
    assert not (state.waiting.received & ~state.waiting.consumed).any().cpu().item()
    assert state.ledger.fault.cpu().tolist()==[0]
    assert int(delivery.closed.emitted.sum().cpu())==9
    for name in ['current_entry_sequence','current_local_prefix']:
        assert values(getattr(state.ledger,name))==values(getattr(expected,name))
    assert values(state.ledger.platform.platform_source_prefix)==values(expected.platform.platform_source_prefix)
    for name in ['balances','outcomes']:
        for part in ['numerator','denominator']:
            a=getattr(getattr(state.ledger.platform,name),part)
            b=getattr(getattr(expected.platform,name),part)
            capacity=max(a.words.size(1),b.words.size(1))
            assert torch.equal(a.with_capacity(capacity).words,b.with_capacity(capacity).words)
            assert torch.equal(a.sign,b.sign)
    for name in ['condition_payable','condition_locked']:
        a=getattr(state.ledger.rebate_days,name)
        b=getattr(expected.rebate_days,name)
        assert values(a.numerator)==values(b.numerator) and values(a.denominator)==values(b.denominator)
    assert torch.equal(state.ledger.rebate_days.payment_observed,expected.rebate_days.payment_observed)
    return True
