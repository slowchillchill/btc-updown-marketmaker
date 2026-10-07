"""原真实FinalSuccess→CPU Ledger独立中间oracle与同冻结Native修复字段。"""
from dataclasses import replace
from fractions import Fraction
from pathlib import Path
from types import SimpleNamespace
import math
import sys
import torch
from queues import Numbers
from validate_base_execution import rational,values


def check(source,start,complete,*,bank,pending,waiting,delivered):
    root=Path(__file__).resolve().parents[2]
    sys.path[:0]=[str(root/'shared-run/gpu'),str(root/'model-train/native-inputs/gpu'),str(root/'data-pipeline/gpu')]
    from repair_gap import repair_gap
    from input_repair import repair_inputs,write_repair_fields
    from input_commitments import commitment_inputs
    from resolution import MarketResolutions,ResolutionSources
    from commitments import SubmittedLegs,SubmissionBindings,freeze_leg_commitments
    from public_ledger import PublicLedgerState
    from registry import PublicRegistryState,PublicRegistryEvents,FrozenIntentOrders
    from registry_projection import public_rows_from_closed
    from input_account import frozen_account_inputs
    tensor=lambda value,dtype=torch.int64:torch.tensor(value,dtype=dtype,device='cuda')
    yes=tensor([True],torch.bool);zero=tensor([0])
    reference=source['final_success_before_economic_reference']
    fact=reference['record']['Evidence']['evidence']['TradeLifecycleV3']['result']['result']
    column=next(i for i,fill in enumerate(values(start.lifecycle.fills))
        if fill==fact['fill_sequence'] and start.lifecycle.emitted[0,i].cpu().item()
        and start.lifecycle.schemas[0,i].cpu().item()==8 and start.lifecycle.trade_dispositions[0,i].cpu().item()==2)
    early,used=Numbers.run_exact_batch(lambda:start.apply_record(tensor([column]),yes,tensor([10000]),yes))
    assert used.cpu().tolist()==[True] and early.fault.cpu().tolist()==[0]
    assert early.trade_success.has_fact.sum().cpu().item()==1
    assert not early.trade_success.economic_applied.any().cpu().item()
    assert values(early.platform.platform_source_prefix)==[0]
    assert values(early.success_pools.success_quantity.numerator)==[0,0]
    condition=source['initial'][0]['market']['condition_id']
    market=next(row for row in reference['checkpoint']['markets'] if row['condition_id']==condition)
    # 两运行资产固定为原token1、2；outcome和CPU数组位置都从实际typed市场读出。
    tokens=['1','2'];assets=market['assets']
    assert {asset['token_id'] for asset in assets}==set(tokens)
    outcomes=tensor([next(asset['outcome']['index'] for asset in assets if asset['token_id']==token) for token in tokens])
    conditions=tensor([[[0],[-1]]]);present=conditions>=0;applicable=tensor([[True,True]],torch.bool)
    # 真实开户/InitialL2两资产已安装且原CPU为Unresolved，显式建立typed冷目录。
    assert market['settlement_state']=='Unresolved'
    resolutions=Numbers.run_exact_batch(lambda:MarketResolutions.open(early.success_pools,early.unresolved_markets,
        ResolutionSources.bind(SimpleNamespace(terminals=[],canonical_ids=[]),tokens)))
    early=replace(early,resolutions=resolutions)
    assert source['repair_request']['method']=='POST_ORDERS'
    post=source['repair_request']['request']
    assert source['repair_receipt']['scheduled_actions']==1 and source['repair_receipt']['input_sequence']==12
    assert source['repair_receipt']['platform_arrived_at_utc_ms']>=10000
    item=post['items'][0];order=item['item']['order'];target=order['kind']['GtcLimit']['size_shares']
    side=next(asset['outcome']['index'] for asset in assets if asset['token_id']==order['token_id'])
    assert order['condition_id']==condition
    # 同一真实receipt的腿在尚无公开Submission绑定时通过原共享冻结入口保留target。
    assert post['context']['global_request_id']!=source['post_request']['request']['context']['global_request_id']
    # 原Projection请求槽0绑定旧请求；槽1绑定此实际新request，不把旧Accepted移给它。
    legs=SubmittedLegs(yes[:,None],yes[:,None],tensor([[1]]),tensor([[item['batch_item_ordinal']]]),
        rational([target]),yes[:,None],zero[:,None],tensor([[side]]),yes[:,None])
    bindings=SubmissionBindings.empty(1,'cuda')
    commitments=Numbers.run_exact_batch(lambda:commitment_inputs(freeze_leg_commitments(legs,bindings,None,
        SimpleNamespace(fault=early.fault)),conditions,present,applicable))
    def fractions(value):return [Fraction(n,d) for n,d in zip(values(value.numerator),values(value.denominator))]
    def expected(checkpoint):
        original=next(row for row in checkpoint['markets'] if row['condition_id']==condition)
        by_token={asset['token_id']:Fraction(value) for asset,value in zip(original['assets'],original['repair_gap_shares'])}
        return [by_token[token] for token in tokens]
    def triples(values):return [v for amount in values for v in
        (math.copysign(math.log1p(abs(float(amount))),float(amount)),1.,1.)]
    assert expected(reference['checkpoint'])[tokens.index(order['token_id'])]>0
    for state,checkpoint in [(early,reference['checkpoint']),(replace(complete,resolutions=resolutions),source['reservation_checkpoints'][-1])]:
        read=Numbers.run_exact_batch(lambda:repair_gap(state.trade_success,state.success_pools))
        original=expected(checkpoint)
        assert fractions(read.quantity)==original and read.fault.cpu().tolist()==[0]
        by_side=[original[outcomes.cpu().tolist().index(side)] for side in range(2)]
        encoded=Numbers.run_exact_batch(lambda:repair_inputs(state,conditions,present,applicable,outcomes,
            prefix_known=yes,settlement_prefix_known=yes,commitments=commitments))
        cap=[max(value-(Fraction(target) if i==side else Fraction(0)),Fraction(0)) for i,value in enumerate(by_side)]
        torch.testing.assert_close(encoded.gap.cpu(),tensor([triples(by_side)],torch.float32).reshape(1,2,3).cpu(),rtol=1e-6,atol=1e-7)
        torch.testing.assert_close(encoded.cap.cpu(),tensor([triples(cap)],torch.float32).reshape(1,2,3).cpu(),rtol=1e-6,atol=1e-7)
        assert encoded.fault.cpu().tolist()==[0]
    assert any(value>0 for value in expected(reference['checkpoint'])), '经济前必须真实有缺口，不能零值虚过'
    # 从同一实际已领取21条NOE构建公开Registry/Submission；waiting复用已完成归约。
    # 新receipt尚未有公开Submission关联，因此原共享freeze仍保存其完整target。
    rank=delivered.emitted.to(torch.int64).cumsum(1)-1
    events,received_bindings=Numbers.run_exact_batch(lambda:public_rows_from_closed(bank,
        complete.columns.projection_batches,delivered,rank))
    registry=Numbers.run_exact_batch(lambda:PublicRegistryState(PublicRegistryEvents.empty(1,'cuda')).receive(events))
    known_bindings=Numbers.run_exact_batch(lambda:SubmissionBindings.empty(1,'cuda').receive(received_bindings))
    assert known_bindings.request_slots[known_bindings.received].cpu().tolist()==[0]
    blank=tensor([[]],torch.bool);integer=Numbers.ExactInt.from_i64_tensor(tensor([]))
    ratio=type(rational(['0']))(integer,integer.constant(1))
    intents=FrozenIntentOrders(blank,integer,blank,ratio,blank,blank)
    complete=replace(complete,resolutions=resolutions)
    public=PublicLedgerState(pending,registry,waiting,complete,submission_bindings=known_bindings)
    frozen_account=Numbers.run_exact_batch(lambda:public.freeze_account(intents,tensor([10000]),submitted_legs=legs))
    assert frozen_account.commitments.known.cpu().tolist()==[[True]] and fractions(frozen_account.commitments.remaining)==[Fraction(target)]
    commitments=Numbers.run_exact_batch(lambda:commitment_inputs(frozen_account.commitments,conditions,present,applicable))
    def encode_frozen(account):
        return frozen_account_inputs(account,tensor([[[],[]]],torch.bool),applicable,applicable,
            scope_conditions=conditions,scope_present=present,asset_outcomes=outcomes,
            executed_prefix_known=yes,settlement_prefix_known=yes,ledger_prefix_known=yes)
    native=Numbers.run_exact_batch(lambda:encode_frozen(frozen_account))
    final_gap=[expected(source['reservation_checkpoints'][-1])[outcomes.cpu().tolist().index(i)] for i in range(2)]
    final_cap=[max(value-(Fraction(target) if i==side else Fraction(0)),Fraction(0)) for i,value in enumerate(final_gap)]
    assert final_cap[side]<final_gap[side]
    assert native.provided[0,164:176].all().cpu().item() and native.fault.cpu().tolist()==[0]
    assert native.provided[0,78:93].all().cpu().item() and native.provided[0,95:98].all().cpu().item()
    original_market=next(row for row in source['reservation_checkpoints'][-1]['projection']['market_projections'] if row['condition_id']==condition)
    torch.testing.assert_close(native.slow[0,95:98].cpu(),torch.tensor(triples([Fraction(original_market['current_all_in_commitment_pusd'])])),rtol=1e-6,atol=1e-7)
    torch.testing.assert_close(native.slow[0,164:176].cpu(),torch.tensor(triples(final_gap)+triples(final_cap)),rtol=1e-6,atol=1e-7)
    incomplete=Numbers.run_exact_batch(lambda:encode_frozen(replace(frozen_account,commitments=None)))
    assert torch.equal(incomplete.slow[:,164:170],native.slow[:,164:170])
    assert incomplete.slow[0,170:176].cpu().tolist()==[0.,0.,1.]*2
    assert incomplete.provided[0,164:176].all().cpu().item() and incomplete.fault.cpu().tolist()==[0]
    # Exact容量沿现有device协议；同地址公开成交更新后重放，保留此前冻结输出。
    from ledger_drain import _owned,_pairs,installed_word_capacity
    live=_owned(early.trade_success,complete.trade_success)
    words=installed_word_capacity((early,complete,commitments))
    def encode_live():
        return repair_inputs(replace(early,trade_success=live),conditions,present,applicable,outcomes,
            prefix_known=yes,settlement_prefix_known=yes,commitments=commitments)
    Numbers.device_exact_batch(encode_live,word_capacity=words,device='cuda')
    graph=torch.cuda.CUDAGraph()
    with torch.cuda.graph(graph):
        candidate=Numbers.device_exact_batch(encode_live,word_capacity=words,device='cuda')
    graph.replay()
    assert candidate.fits.cpu().item() and candidate.value.fault.cpu().tolist()==[0]
    graph_frozen=write_repair_fields(torch.zeros((1,433),device='cuda'),candidate.value)
    early_gap=[expected(reference['checkpoint'])[outcomes.cpu().tolist().index(i)] for i in range(2)]
    early_cap=[max(value-(Fraction(target) if i==side else Fraction(0)),Fraction(0)) for i,value in enumerate(early_gap)]
    torch.testing.assert_close(graph_frozen[0,164:176].cpu(),torch.tensor(triples(early_gap)+triples(early_cap)),rtol=1e-6,atol=1e-7)
    saved=graph_frozen.clone()
    incoming=_owned(complete.trade_success,live)
    for target_tensor,source_tensor in _pairs(live,incoming):target_tensor.copy_(source_tensor)
    graph.replay()
    assert candidate.fits.cpu().item() and candidate.value.fault.cpu().tolist()==[0]
    projected=write_repair_fields(torch.zeros((1,433),device='cuda'),candidate.value)
    torch.testing.assert_close(projected[0,164:176].cpu(),native.slow[0,164:176].cpu(),rtol=1e-6,atol=1e-7)
    assert torch.equal(graph_frozen,saved)
    # 完整冷结算前缀仍不能替代实际typed市场；缺目录为Unknown。
    unknown=Numbers.run_exact_batch(lambda:repair_inputs(replace(early,resolutions=None),conditions,present,applicable,
        outcomes,prefix_known=yes,settlement_prefix_known=yes,commitments=commitments))
    assert unknown.gap.cpu().tolist()==[[[0.,0.,1.],[0.,0.,1.]]]
    # 无current由实际applicable表示NA，空集合的Known0不是可交易零缺口。
    empty=conditions.clone();empty[:,0]=-1
    na=Numbers.run_exact_batch(lambda:repair_inputs(early,empty,empty>=0,tensor([[False,True]],torch.bool),outcomes,
        prefix_known=yes,settlement_prefix_known=yes,commitments=commitments))
    assert na.gap.cpu().tolist()==[[[0.,0.,0.],[0.,0.,0.]]] and torch.equal(na.gap,na.cap)
    return {'actual_public_FinalSuccess_before_economic_gap':True,'actual_receipt_exact_remaining_cap':True,
        'public_freeze_Native_positive_gap_receipt_cap_deduction_and_missing_commitment_Unknown':True,
        'same_device_graph_public_Trade_update_and_old_frozen_stability':True,
        'typed_settlement_unknown_and_no_current_NA':True,'natural_public_schedule_or_full_account':False}
