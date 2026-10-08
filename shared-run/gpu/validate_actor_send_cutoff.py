"""真实两根公共页/hidden处理/首次领取；暂停根保留整份原Ck。

使用原有界材料的冷账户，不登记旧FOK，也不替换推进调度器。只验证
cutoff嵌套快照的按根更新，不把本代表称为两根完整Actor发送宿主。
"""
from dataclasses import replace
from fractions import Fraction
from pathlib import Path
import importlib.util,json,sys,time
import torch

ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/p) for p in ('shared-run/gpu','sim-system/gpu','ledger/gpu',
    'execution-normalization/gpu','data-pipeline/gpu','model-train/native-inputs/gpu')]
from validate_pending_post import (T,run,integers,rational,values,BALANCES,HiddenAssetState,
    BaseOrderQuantity,FillRecord,BaseSimRuntime,MutationRequests,BaseOrders,PlatformAccountState,
    FillPool,RebateCoordinator,MakerDays,TakerDays,AdmissionLifecycle,SuccessPools,PositionCosts,
    OrderFunds,AcceptedPrincipal,ProjectedMarkets,PublicLedgerState,PendingRecords,
    PublicRegistryState,PublicRegistryEvents,ClosedAdmission,EconomicColumns,SubmissionBindings)
from settlement_accounting import ExternalCashIdentities,SettlementCosts
from core import EvidenceBank
from public_device_page import PreparedPublicPage
from public_source import PublicGroupSource
from hidden_inputs import HiddenPmBlock
from hidden_frontier import HiddenBucketColumns,HiddenBucketFrontier
from scheduling import ScheduledTime
from input_frontier import InputCommitFrontier
from send_clock import BoundedSendClock
from pending_post_intents import _decimal
from registry import PublicCancellationState
from cancel_projection import cancellation_rows_from_closed


def check(source,page,compact):
    started=time.perf_counter();yes=T([True,True],torch.bool);zero=T([0,0])
    prepared=PreparedPublicPage(Path(page),Path(compact))
    # 独立期望来自原完整组header，不从被测cutoff合并生成。
    group_times=[int(row[2]) for row in prepared.columns['groups']]
    assert group_times==[901,1111,1111,1414,1415]
    hidden_source=source['bounded_hidden'];assets=hidden_source['assets']
    assert len(assets)==2 and not source['opening_ledger']['order_funds']
    assert not source['opening_ledger']['resolutions']
    token_axis={asset['token_id']:i for i,asset in enumerate(assets)}
    slots=[]
    for event in source['initial']:
        book=event['payload']['event'];asset=token_axis[book['asset']['token_id']]
        for side in ('bids','asks'):
            for level in book[side]:
                slots.append((asset,side,Fraction(level['price_pusd_per_share'])))
    slots.sort()
    block=HiddenPmBlock(assets,[(assets[a]['token_id'],side.upper().removesuffix('S'),_decimal(price))
        for a,side,price in slots],source_binding=hidden_source['source_binding'])
    assert [b['bucket']['bucket_time_utc_ms'] for b in block.buckets]==[1100,1414]
    assert not block.terminal_columns.terminals
    owner=T([0,0,1,1]);source_assets=T([0,1,0,1])
    level_assets=T([a+root*2 for root in range(2) for a,_,_ in slots])
    level_sides=T([0 if side=='bids' else 1 for _ in range(2) for _,side,_ in slots])
    balance=source['opening_ledger']['projection']['platform_confirmed_projection']
    spec=importlib.util.spec_from_file_location('cutoff_platform',ROOT/'ledger/gpu/platform.py')
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    bank=EvidenceBank.empty('cuda')
    def opening():
        blank=integers([0,0]);one=integers([1,1])
        quantity=BaseOrderQuantity.from_taker_plan(one,one,blank,blank)
        record=FillRecord.create(one,one,blank,zero,zero,~yes,source['finality_path'])
        hidden=HiddenAssetState.empty(level_assets,rational([str(price) for _ in range(2) for _,_,price in slots]),
            T([0,0,1,1]),2,segment_capacity=1)
        runtime=BaseSimRuntime(hidden,MutationRequests.empty(blank,capacity=1,action_capacity=1),
            BaseOrders.empty(quantity,capacity=1),
            PlatformAccountState.open(integers([int(Fraction(source['cash'])*1000000)]*2),owner),
            FillPool.empty(record,capacity=1),
            RebateCoordinator(MakerDays.empty(T([[0],[0]]),T([[source['market_end']]]*2),
                yes[:,None],fills=1,days=1),TakerDays.empty(2,'cuda',fills=1,days=1)),
            hidden_frontier=HiddenBucketFrontier.open(HiddenBucketColumns.from_block(block),2),
            input_frontier=InputCommitFrontier.open(2,'cutoff-independent-replays',source['session_id']))
        pools=SuccessPools.open(owner,T([0]*4),T([0,1]*2),T([1,0,3,2]),2)
        platform=module.PlatformBalances.open(rational([balance[key] for _ in range(2) for key in BALANCES]),
            rational(['0']*4),owner,blank)
        columns=EconomicColumns.empty(2,'cuda')
        ledger=AdmissionLifecycle.open(platform,bank,columns,blank,blank,pools,
            position_costs=PositionCosts.empty(2,1,'cuda'),order_funds=OrderFunds.empty(2,1,1,0,'cuda'),
            accepted_principal=AcceptedPrincipal.open(2,T([0]),known=True),
            projected_markets=ProjectedMarkets.open(T([0]),T([[False]]*2,torch.bool),yes),
            external_cash=ExternalCashIdentities.empty(2,1,'cuda',known=True),
            settlement_costs=SettlementCosts.empty(2,1,'cuda',known=True))
        pending=PendingRecords.from_columns(columns.enabled,columns.row_refs,integers([]),integers([]),
            columns.row_refs,blank)
        public=PublicLedgerState(pending,PublicRegistryState(PublicRegistryEvents.empty(2,'cuda')),
            ClosedAdmission.empty(columns),ledger,
            cancellations=PublicCancellationState.open(integers([]),T([[],[]],torch.bool)),
            submission_bindings=SubmissionBindings.empty(2,'cuda',prefix_known=True))
        return runtime.install_hidden_market_controls(),public
    runtime,public=run(opening)
    def hidden(world,projection,choice):
        world=world.begin_hidden_bucket(choice)
        def refresh():
            return replace(choice,requests=world.requests,fills=world.fills,calendars=world.calendars,
                hidden_frontier=world.hidden_frontier)
        for index in range(block.reference_rows.size(0)):
            world=run(lambda:world.apply_hidden_reference(refresh(),zero+index,source_assets))
        for index in range(len(block.operations)):
            world,effect=run(lambda:world.apply_hidden_selected(refresh(),zero+index,T([0]*4),
                T([[0]*world.hidden.queues.capacity]*2),rational([source['rule']['fee_rate']]*2),
                source['rule']['fee_exponent'],rational([source['rule']['taker_rebate_rate']]*2),
                source_assets,level_sides))
            assert not effect.fault.any().cpu().item(),effect.fault.cpu().tolist()
        world,closed=run(world.finish_hidden_bucket)
        assert torch.equal(closed,choice.present),closed.cpu().tolist()
        return world,projection,None,None
    groups=PublicGroupSource(prepared,2);groups.load_next(1)
    clock=BoundedSendClock(runtime,None,public,groups,{4:hidden},ScheduledTime.absent(zero),lambda _:bank)
    trace=[]
    def progress(stage,**row):
        item=dict(stage=stage,elapsed_seconds=time.perf_counter()-started,**row)
        trace.append(item);print(json.dumps(item),flush=True)
    def advance(at,enabled= yes):
        result=clock.advance_before(T(at),enabled,progress=progress)
        assert not result.fault.any().cpu().item(),result.fault.cpu().tolist()
        return result
    def snapshot():
        cutoff=clock.last_observation
        return dict(positions=cutoff.receipt.positions.cpu().tolist(),
            inputs=values(cutoff.receipt.input_sequence),observed=cutoff.observed_at.cpu().tolist(),
            delivery=values(cutoff.private_feedback_sequence),
            references=[cutoff.receipt.original_reference(root) for root in range(2)])
    assert advance([1000,1000]).complete.cpu().tolist()==[True,True]
    delivery=clock.receive_first(yes)
    assert not delivery.fault.any().cpu().item()
    assert delivery.cancellation_targets.received.shape==(2,0)
    assert clock.public.cancellations.completions.received.shape==(2,0)
    assert torch.equal(delivery.cancellation_targets.delivery_sequences.words,delivery.closed.delivered_sequences.words)
    assert torch.equal(delivery.cancellation_targets.available_at,delivery.closed.available_at)
    empty_fault=T([0,8],torch.int32)
    targets,completions=run(lambda:cancellation_rows_from_closed(bank,(),replace(delivery.closed,fault=empty_fault)))
    assert torch.equal(targets.fault,empty_fault) and torch.equal(completions.fault,empty_fault)
    old=snapshot();old_cutoff=clock.last_observation
    assert old['positions']==[0,0] and old['inputs']==[1,1]
    assert old['observed']==[1000,1000] and old['delivery']==[0,0]
    assert values(clock.runtime.input_frontier.next_sequence)==[4,4]
    groups.load_next(1)
    result=advance([1415,1112])
    assert result.complete.cpu().tolist()==[False,False]
    assert result.public_page_missing.cpu().tolist()==[True,True]
    assert groups.position.cpu().tolist()==[2,2]
    assert clock.runtime.hidden.initialized.all().cpu().item()
    # root1先获得下一页并完成；root0随后也推进其公开组，但仍缺后一页。
    groups.load_next(1)
    assert advance([1415,1112],T([False,True],torch.bool)).complete.cpu().tolist()==[False,True]
    partial=advance([1415,1112])
    assert partial.complete.cpu().tolist()==[False,True]
    assert partial.public_page_missing.cpu().tolist()==[True,False]
    assert groups.position.cpu().tolist()==[3,3]
    assert clock.runtime.input_frontier.public_receipt.positions.cpu().tolist()==[2,2]
    delivery=clock.receive_first(partial.complete)
    assert not delivery.fault.any().cpu().item()
    middle=snapshot()
    assert middle['positions']==[0,2] and middle['inputs']==[1,6],middle
    assert middle['observed']==[1000,1112] and middle['delivery']==[0,0]
    assert middle['references'][0]==old['references'][0]
    assert middle['references'][1]['group_id']==prepared.columns['group_ids'][2]
    assert values(clock.runtime.input_frontier.next_sequence)==[8,9]
    # 原旧Ck对象仍持有相同值，不能仅在最新返回值上补偿。
    assert old_cutoff.receipt.positions.cpu().tolist()==[0,0]
    assert values(old_cutoff.receipt.input_sequence)==[1,1]
    progress('paused_root_snapshot_preserved',snapshot=middle)
    groups.load_next(1)
    assert advance([1415,1112]).complete.cpu().tolist()==[True,True]
    delivery=clock.receive_first(yes)
    assert not delivery.fault.any().cpu().item()
    final=snapshot()
    assert final['positions']==[3,2] and final['inputs']==[8,6],final
    assert final['observed']==[1415,1112] and final['delivery']==[0,0]
    assert final['references'][1]==middle['references'][1]
    assert final['references'][0]['group_id']==prepared.columns['group_ids'][3]
    assert values(clock.runtime.input_frontier.next_sequence)==[11,9]
    assert groups.position.cpu().tolist()==[4,3]
    assert clock.runtime.hidden_frontier.has_closed.cpu().tolist()==[True,True]
    assert clock.runtime.hidden_frontier.closed_at.cpu().tolist()==[1414,1100]
    assert not clock.public.pending_noe.active.any().cpu().item()
    before=snapshot();sequence=values(clock.runtime.input_frontier.next_sequence)
    clock.receive_first(yes)
    assert snapshot()==before and values(clock.runtime.input_frontier.next_sequence)==sequence
    return dict(passed=True,scope='真实公共完整组/两根冷账户/原hidden处理/双门首次领取/cutoff快照；未运行Actor或发送',
        old=old,middle=middle,final=final,next_input=sequence,trace=trace,elapsed_seconds=time.perf_counter()-started)


if __name__=='__main__':
    print(json.dumps(check(json.loads(Path(sys.argv[1]).read_text()),sys.argv[2],sys.argv[3])),flush=True)
