"""实际GPU取消释放效果→反馈/Core→双门唯一领取→Registry的有限原对照。

原外层请求提供四条V4引用作oracle；GPU输出由自身cancel delta生成。
一次已接单前缀及发射序初始化，不代表完整全局时钟或账户恢复。
"""
from pathlib import Path
from dataclasses import replace
from fractions import Fraction
import importlib.util
import json
import sys
import torch
from queues import Numbers
from latency import EmpiricalDelaySampler
from feedback import FeedbackEmitter
from validate_base_execution import integers,rational,values


def validate_actual_cancel(source,delta):
    root=Path(__file__).resolve().parents[2]
    sys.path[:0]=[str(root/'execution-normalization/gpu'),str(root/'shared-run/gpu'),str(root/'ledger/gpu')]
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
    from validate_transport import check_transport_layout
    # 同一实际来源及无事实padding混合，method列不能因旧普通批None而被丢弃。
    padding=replace(batch,emitted=torch.zeros_like(batch.emitted),cancellation_method=None,
        normalized=replace(batch.normalized,emitted=torch.zeros_like(batch.emitted)))
    assert Numbers.run_exact_batch(lambda:check_transport_layout([padding,batch]))
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
    pending_initial=pending
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
    # Ledger只在开头安装原V4已公开取消前缀；四记录应用次序由设备领取缓存决定。
    from memory import EconomicColumns
    from admission import AdmissionLifecycle
    from closed import ClosedAdmission
    from success_pool import SuccessPools
    from validate_order_funds import open_prefix,check as check_order_funds
    from queues import ExactRational
    before=source['cancel_ledger_before']
    original_projection=before['projection']
    names=['cash_pusd','available_pusd','principal_encumbrance_pusd','immediate_taker_fee_encumbrance_pusd',
        'maker_rebate_receivable_pusd','taker_rebate_receivable_pusd','maker_rebate_locked_remainder_pusd','taker_rebate_locked_remainder_pusd']
    platform=original_projection['platform_confirmed_projection']
    spec=importlib.util.spec_from_file_location('gpu_cancel_platform',root/'ledger/gpu/platform.py')
    module=importlib.util.module_from_spec(spec)
    sys.modules[spec.name]=module
    spec.loader.exec_module(module)
    amounts=lambda value:[Fraction(n,d) for n,d in zip(values(value.numerator),values(value.denominator))]
    def prepare_ledger():
        outcomes={item['token_id']:item['balance_shares'] for item in platform['outcome_balances']}
        balances=module.PlatformBalances.open(rational([platform[name] for name in names]),
            rational([outcomes.get(token,'0') for token in ['1','2']]),torch.tensor([0,0],device='cuda'),
            integers([original_projection['platform_source_prefix']]))
        columns=EconomicColumns.from_projection_batches([batch])
        pools=SuccessPools.open(torch.tensor([0,0],device='cuda'),torch.tensor([0,0],device='cuda'),
            torch.tensor([0,1],device='cuda'),torch.tensor([1,0],device='cuda'),1)
        ledger=AdmissionLifecycle.open(balances,state.normalizer.bank,columns,
            integers([original_projection['ledger_entry_seq']]),integers([original_projection['ledger_local_prefix']]),pools,
            order_funds=open_prefix(1,columns.row_refs.size(1),columns.row_refs.size(1),columns.row_refs.size(1),before))
        ledger=replace(ledger,maker_cash=rational([str(Fraction(before['cash']['maker_available_pusd'])+
            Fraction(before['cash']['maker_reserved_pusd']))]))
        # 原目录当前只有这一真实已接纳订单；保存原公开open/经济接纳标志。
        initial_order=before['order_lifecycle'][0]
        assert platform['open_order_count']==1 and initial_order['open']
        orders,error=ledger.orders.set(integers([1]),zero,active,
            integers([initial_order['last_ledger_entry_seq']]),active,economic=active)
        return replace(ledger,orders=orders,fault=ledger.fault|error)
    ledger=Numbers.run_exact_batch(prepare_ledger)
    cache=ClosedAdmission.empty(ledger.columns)
    from public_ledger import PublicLedgerState
    from registry import PublicRegistryEvents
    # 同真实来源另算纯候选，作为统一生产入口的对照，不重复接纳手工分支。
    request_state=PublicCancellationState.open(integers([0]),active[:,None])
    coordinated=PublicLedgerState(pending_initial,PublicRegistryState(PublicRegistryEvents.empty(1,'cuda')),cache,ledger,request_state)
    coordinated_initial=coordinated
    # 缺经济关联只暂停Ledger释放；公开Control已领取，Registry仍先关闭。
    # 原Registry已有故障不能继续提交该根Ledger。NOE实际领取引用仍在返回结果中。
    broken=replace(coordinated,registry=PublicRegistryState(replace(coordinated.registry.events,
        fault=torch.ones_like(coordinated.registry.events.fault))))
    broken,failed_delivery=Numbers.run_exact_batch(lambda:broken.receive(state.normalizer.bank,gates,
        torch.ones_like(ledger.columns.enabled),passes=1))
    assert failed_delivery.fault.cpu().tolist()==[1]
    assert values(broken.ledger.current_entry_sequence)==values(ledger.current_entry_sequence)
    assert amounts(broken.ledger.platform.balances)==amounts(ledger.platform.balances)
    assert not broken.ledger.applied.any().cpu().item()
    blocked_association=torch.zeros_like(ledger.columns.enabled)
    coordinated,delivery=Numbers.run_exact_batch(lambda:coordinated.receive(state.normalizer.bank,gates,blocked_association,passes=1))
    assert delivery.fault.cpu().tolist()==[0] and int(delivery.closed.emitted.sum().cpu())==4
    assert (coordinated.waiting.received & ~coordinated.waiting.consumed).any().cpu().item()
    frozen_blocked=Numbers.run_exact_batch(lambda:coordinated.registry.freeze_orders(directory,closed_at))
    assert frozen_blocked.cancellable_known.cpu().tolist()==[[True]] and frozen_blocked.cancellable.cpu().tolist()==[[False]]
    account_blocked=Numbers.run_exact_batch(lambda:coordinated.freeze_account(directory,closed_at))
    assert account_blocked.fault.cpu().tolist()==[0]
    assert values(account_blocked.pending_noe_count)==[int((coordinated.waiting.received & ~coordinated.waiting.consumed).sum().cpu())]
    assert values(account_blocked.cancellations.pending_count)==[0]
    assert values(account_blocked.cancellations.response_counts)==[1,0,0]
    completion_width=coordinated.cancellations.completions.received.size(1)
    assert completion_width==1, '仅保留实际完整请求完成人口'
    coordinated,again=Numbers.run_exact_batch(lambda:coordinated.receive(state.normalizer.bank,gates,torch.ones_like(ledger.columns.enabled),passes=1))
    assert not again.closed.emitted.any().cpu().item()
    assert coordinated.cancellations.completions.received.size(1)==completion_width, '空领取不增长完成目录'
    empty_faulty=replace(completions,received=torch.zeros_like(completions.received),fault=torch.ones_like(completions.fault))
    faulty_empty=Numbers.run_exact_batch(lambda:coordinated.cancellations.receive(empty_faulty))
    assert faulty_empty.completions.received.size(1)==completion_width
    assert faulty_empty.completions.fault.cpu().tolist()==[1], '空人口仍保留完整来源故障'
    assert not (coordinated.waiting.received & ~coordinated.waiting.consumed).any().cpu().item()
    assert torch.equal(coordinated.waiting.available_at,delivery.closed.available_at)
    assert check_order_funds(coordinated.ledger,source['cancel_ledger_after'])
    assert values(coordinated.ledger.current_entry_sequence)==[source['cancel_ledger_after']['projection']['ledger_entry_seq']]
    assert amounts(coordinated.ledger.platform.balances)==[Fraction(source['cancel_ledger_after']['projection']['platform_confirmed_projection'][name]) for name in names]
    # 共享冻结包含首次领取后的公开请求完成；Ledger资金仍按实际经济提交改变。
    account_after=Numbers.run_exact_batch(lambda:coordinated.freeze_account(directory,closed_at))
    assert values(account_after.pending_noe_count)==[0]
    assert values(account_blocked.pending_noe_count)!=[0], '旧冻结waiting人口不倒灌'
    assert amounts(account_blocked.funds.actual_available)==[Fraction(before['cash']['actual_available_pusd'])]
    assert amounts(account_after.funds.actual_available)==[Fraction(source['cancel_ledger_after']['cash']['actual_available_pusd'])]
    sys.path[:0]=[str(root/'model-train/native-inputs/gpu'),str(root/'data-pipeline/gpu')]
    from input_account import frozen_account_inputs
    current_and_empty_old=torch.tensor([[[True],[False]]],device='cuda')
    scope_known=torch.ones((1,2),dtype=torch.bool,device='cuda')
    condition_scopes=torch.tensor([[[0],[-1]]],device='cuda')
    condition_present=torch.tensor([[[True],[False]]],device='cuda')
    native=Numbers.run_exact_batch(lambda:frozen_account_inputs(account_after,current_and_empty_old,scope_known,scope_known,
        scope_conditions=condition_scopes,scope_present=condition_present,asset_outcomes=torch.tensor([0,1],device='cuda')))
    assert native.fault.cpu().tolist()==[0]
    def log(value):
        import math
        value=float(Fraction(value))
        return math.copysign(math.log1p(abs(value)),value)
    for index,name in [(110,'actual_available_pusd'),(113,'actual_reserved_pusd'),(116,'maker_available_pusd'),(119,'maker_reserved_pusd')]:
        torch.testing.assert_close(native.slow[0,index:index+3].cpu(),
            torch.tensor([log(source['cancel_ledger_after']['cash'][name]),1.,1.]),rtol=1e-6,atol=1e-7)
    after_projection=source['cancel_ledger_after']['projection']['platform_confirmed_projection']
    original_assets={item['token_id']:item['balance_shares'] for item in after_projection['outcome_balances']}
    for side,token in enumerate(['1','2']):
        torch.testing.assert_close(native.slow[0,54+side*3:57+side*3].cpu(),
            torch.tensor([log(original_assets.get(token,'0')),1.,1.]),rtol=1e-6,atol=1e-7)
    assert native.slow[0,60:66].cpu().tolist()==[0.,0.,1.]*2, '未安装中途持仓成本不冒充已知零'
    assert native.slow[0,66:78].cpu().tolist()==[0.,1.,1.]*4, '空旧scope是Known零'
    assert native.provided[0,:78].all().cpu().item()
    assert native.slow[0,412:415].cpu().tolist()==[0.,1.,1.]
    assert native.slow[0,415:418].cpu().tolist()==[0.,1.,1.]
    for offset,count in [(418,1),(421,0),(424,0)]:
        torch.testing.assert_close(native.slow[0,offset:offset+3].cpu(),torch.tensor([log(count),1.,1.]))
    expected_time=[log(Fraction(int(closed_at.cpu()[0])-direct['platform_responded_at_utc_ms'],1000)),
        log(Fraction(int(closed_at.cpu()[0])-direct['response_received_at_utc_ms'],1000)),
        log(Fraction(direct['response_received_at_utc_ms']-direct['platform_responded_at_utc_ms'],1000)),1.,1.,1.]
    torch.testing.assert_close(native.slow[0,427:433].cpu(),torch.tensor(expected_time),rtol=1e-6,atol=1e-7)
    assert native.provided[0,110:122].all().cpu().item() and native.provided[0,382:433].all().cpu().item()
    assert not native.provided.all().cpu().item(), '未提供的经济/结算字段不可冒充完整slow433'
    broken_account=replace(account_after,fault=torch.ones_like(account_after.fault))
    broken_input=Numbers.run_exact_batch(lambda:frozen_account_inputs(broken_account,current_and_empty_old,scope_known,scope_known))
    assert broken_input.fault.cpu().tolist()==[1], 'Native共同携带共享账户故障'
    # 原精确请求身份故障须在尚无完成反馈的正常登记前缀就传播，不靠补account.fault。
    faulty_identity=replace(request_state.requests,fault=torch.ones_like(request_state.requests.fault))
    faulty_request_state=PublicCancellationState.open(faulty_identity,active[:,None])
    faulty_prefix=replace(coordinated_initial,cancellations=faulty_request_state)
    faulty_frozen=Numbers.run_exact_batch(lambda:faulty_prefix.freeze_account(directory,closed_at))
    faulty_native=Numbers.run_exact_batch(lambda:frozen_account_inputs(faulty_frozen,current_and_empty_old,scope_known,scope_known))
    assert faulty_frozen.fault.cpu().tolist()==[1] and faulty_native.fault.cpu().tolist()==[1]
    assert faulty_request_state.completions.received.size(1)==0
    # 较晚Ledger提交不能修改已冻结的公开读取。
    assert frozen_blocked.cancellable.cpu().tolist()==[[False]]
    # 设备实际take_closed输出定义次序；原CPU逐条after只用于比较，不提供应用控制。
    for prefix in range(1,closed.emitted.size(1)+1):
        first=replace(closed,emitted=closed.emitted & (rank<prefix))
        cache=Numbers.run_exact_batch(lambda:cache.accept(ledger.columns,first))
        ledger,cache=Numbers.run_exact_batch(lambda:cache.drain(ledger,torch.ones_like(cache.received),passes=1))
        expected=source['cancel_ledger_attempts'][prefix-1]['after']
        assert check_order_funds(ledger,expected)
        original_order=expected['order_lifecycle'][0]
        assert ledger.orders.flags.cpu().tolist()[0][0]==original_order['open']
        assert ledger.orders.conditions.cpu().tolist()[0][0]==0 and original_order['condition']==before['order_lifecycle'][0]['condition']
        assert values(ledger.orders.last_sequences)[0]==original_order['last_ledger_entry_seq']
        assert amounts(ledger.platform.balances)==[Fraction(expected['projection']['platform_confirmed_projection'][name]) for name in names]
        assert values(ledger.current_entry_sequence)==[expected['projection']['ledger_entry_seq']]
        assert values(ledger.platform.platform_source_prefix)==[expected['projection']['platform_source_prefix']]
    assert ledger.fault.cpu().tolist()==[0] and cache.fault.cpu().tolist()==[0]
    assert not (cache.received & ~cache.consumed).any().cpu().item()
    after=source['cancel_ledger_after']['projection']
    assert values(ledger.current_entry_sequence)==[after['ledger_entry_seq']]
    assert values(ledger.current_local_prefix)==[after['ledger_local_prefix']]
    assert values(ledger.platform.platform_source_prefix)==[after['platform_source_prefix']]
    assert amounts(ledger.platform.balances)==[Fraction(after['platform_confirmed_projection'][name]) for name in names]
    assert ledger.orders.flags.cpu().tolist()[0][0] is False
    assert after['platform_confirmed_projection']['open_order_count']==0
    # 每条空控制条目和经济条目均保存原position的实际Ledger序，只有经济推进platform。
    by_position={json.dumps(item['position'],sort_keys=True):item['after']['projection']['ledger_entry_seq']
        for item in source['cancel_ledger_attempts']}
    applied_sequences=values(ledger.applied_sequences)
    for column,present in enumerate(batch.emitted.cpu().tolist()[0]):
        if present:
            ordinal=values(batch.record_ordinals)[column]
            assert applied_sequences[column]==by_position[json.dumps(originals[ordinal]['position'],sort_keys=True)]
    repeat_cache=Numbers.run_exact_batch(lambda:cache.accept(ledger.columns,replace(closed,available_at=closed.available_at+1)))
    repeated,repeat_cache=Numbers.run_exact_batch(lambda:repeat_cache.drain(ledger,torch.ones_like(cache.received),passes=1))
    assert values(repeated.current_entry_sequence)==values(ledger.current_entry_sequence)
    assert amounts(repeated.platform.balances)==amounts(ledger.platform.balances)
    assert torch.equal(repeat_cache.available_at,cache.available_at)
    return {'actual_GPU_cancel_effects_to_NOE_records':4,'Control_Canceled_before_CallerCanceled_closed':True,
        'Registry_fault_does_not_commit_Ledger_checked':True,'actual_shared_NOE_first_receive_Registry_then_Ledger_checked':True,'Ledger_waiting_does_not_delay_public_cancel_close_checked':True,'real_CallerCanceled_device_decoder_checked':True,'NOE_unset_gates_no_delivery':True,'first_delivery_not_repeated':True,
        'source_private_batch_and_actual_release_checked':True,'Ledger_four_original_positions_and_sequences_checked':True,
        'same_cutoff_public_account_funds_waiting_and_cancel_request_freeze_checked':True,
        'Native_actual_frozen_account_fields_and_fault_checked':True,
        'Native_frozen_asset_shares_and_cold_cost_Unknown_checked':True,
        'registered_request_identity_fault_before_any_completion_reaches_Native':True,
        'empty_completion_population_does_not_grow_and_preserves_source_fault':True,
        'each_actual_receive_prefix_order_condition_open_and_last_sequence_checked':True,
        'Ledger_control_closes_without_cash_and_economic_releases_once':True,
        'cold_original_cancel_prefix_initialized_once':True,'cancellation_method_mixed_transport_checked':True,'each_cancel_prefix_original_OrderFunds_checked':True,'full_root':False}
