"""有限公共V2实际NOE/CPU Ledger对照；不是自然SIM非零费用producer。"""
from dataclasses import replace
from fractions import Fraction
from pathlib import Path
import importlib.util
import json
import math
import sys
# 本脚本目录含Ledger platform.py；先移出隐式script搜索项，让torch加载标准库platform。
sys.path=[entry for entry in sys.path if Path(entry).resolve()!=Path(__file__).resolve().parent]
import torch

ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/p) for p in ('sim-system/gpu','execution-normalization/gpu',
    'ledger/gpu','shared-run/gpu','data-pipeline/gpu','model-train/native-inputs/gpu')]
from queues import Numbers
from validate_base_execution import integers,rational,units,values
from account_state import AccountMovement,PlatformAccountDelta
from economic import map_economic
from projection import Projection
from feedback import FeedbackEmitter
from core import ECONOMIC,OUT_FACT
from validate_projection import expected_semantic
from memory import EconomicColumns
from admission import AdmissionLifecycle
from success_pool import SuccessPools
from position_cost import PositionCosts
from projected_markets import ProjectedMarkets
from accepted_principal import AcceptedPrincipal
from consumption import PendingRecords,ReceiveGates
from closed import ClosedAdmission
from registry import PublicRegistryState,PublicRegistryEvents,FrozenIntentOrders
from public_ledger import PublicLedgerState
from input_account import frozen_account_inputs
from liability_read import read_economic_liabilities
from validate_append import _device_scan

spec=importlib.util.spec_from_file_location('taker_liability_platform',ROOT/'ledger/gpu/platform.py')
platform_module=importlib.util.module_from_spec(spec);sys.modules[spec.name]=platform_module
spec.loader.exec_module(platform_module)


def exact(value):
    return [Fraction(n,d) for n,d in zip(values(value.numerator),values(value.denominator))]


def validate(source):
    run=Numbers.run_exact_batch
    zero=torch.zeros(1,dtype=torch.int64,device='cuda');yes=torch.ones(1,dtype=torch.bool,device='cuda')
    assets=source['initial']['assets']
    assert len(assets)==2 and {a['outcome']['index'] for a in assets}=={0,1}
    condition=source['initial']['market']['condition_id']
    projection=run(lambda:Projection.open(FeedbackEmitter.open(1),zero,zero,zero,zero,
        torch.zeros(2,dtype=torch.int64,device='cuda'),integers([0]),zero,zero,zero,zero))
    batches=[]
    # 实际V2记录只作有限公共入口来源；预期金额另取CPU Ledger归约，不能自己互比。
    for row in source['records']:
        body=next(iter(row['record'].values()))
        fact=body['fact'];value=fact['disposition']['AccountTransition']
        assert value['condition_id']==condition
        assert value['transition']=={'Existing':'MakerRebateAccrualFinalized'}
        delta=value['balance_delta'];existing=delta['existing']
        assert existing['outcome_balance_deltas']==[]
        sequence=fact['economic_identity']['AccountTransition']['account_transition_seq']
        original=fact['source_reference']['record']['SettlementLifecycle']
        movement=run(lambda:AccountMovement(*(integers([units(existing[name])]) for name in
            ('cash_pusd_delta','available_pusd_delta','principal_encumbrance_pusd_delta','immediate_taker_fee_encumbrance_pusd_delta')),
            integers([0,0]),rational([existing[name] for name in ('maker_rebate_receivable_pusd_delta',
                'taker_rebate_receivable_pusd_delta','maker_rebate_locked_remainder_pusd_delta','taker_rebate_locked_remainder_pusd_delta')])))
        private=run(lambda:integers([original['private_feedback_sequence']]))
        account_delta=run(lambda:PlatformAccountDelta(integers([sequence]),yes,movement,torch.zeros_like(zero,dtype=torch.int32)))
        mapped=run(lambda:map_economic(account_delta,zero,zero,zero,zero,projection.asset_roots,private,zero,
            torch.full_like(zero,value['economic_effective_at_utc_ms']),torch.full_like(zero,9),integers([0]),zero,
            settlement_fee_liability=rational([delta['settlement_fee_liability_pusd_delta']]),
            actual_taker_fee_liability=rational([delta['actual_taker_fee_liability_pusd_delta']])))
        expected=run(lambda:expected_semantic(row))
        assert torch.equal(mapped.semantic,expected)
        batch_sequence=row['position']['source_watermark']['projection_batch_sequence']
        projection,batch=run(lambda:projection._finish(projection.emitter,[(mapped,ECONOMIC,private,
            integers([batch_sequence]),torch.full_like(zero,original['feedback_received_at_utc_ms']),
            torch.full_like(zero,3),torch.full_like(zero,original['platform_event_at_utc_ms']),yes)],
            torch.zeros_like(zero,dtype=torch.int32)))
        assert batch.fault.cpu().tolist()==[0] and batch.normalized.outcomes.cpu().tolist()==[[OUT_FACT]]
        assert values(batch.batch_sequences)==[batch_sequence]
        assert values(batch.record_ordinals)==[row['position']['record_ordinal']]
        batches.append(batch)
    bank=projection.normalizer.bank
    columns=run(lambda:EconomicColumns.from_projection_batches([batches[0]]))
    platform=run(lambda:platform_module.PlatformBalances.open(
        rational([source['opening_available_pusd']]*2+['0']*6),rational(['0','0']),
        projection.asset_roots,integers([0])))
    pools=run(lambda:SuccessPools.open(projection.asset_roots,torch.zeros(2,dtype=torch.int64,device='cuda'),
        torch.arange(2,device='cuda'),torch.tensor([1,0],device='cuda'),1))
    ledger=run(lambda:AdmissionLifecycle.open(platform,bank,columns,integers([source['opening_entry']]),
        integers([0]),pools,position_costs=PositionCosts.empty(1,2,'cuda'),
        accepted_principal=AcceptedPrincipal.open(1,zero,known=True),
        projected_markets=ProjectedMarkets.open(zero,yes[:,None],yes)))
    first=batches[0]
    pending=run(lambda:PendingRecords.from_columns(first.emitted,first.received_at,first.private_sequences,
        integers([0]),first.rows,integers([0])))
    state=PublicLedgerState(pending,PublicRegistryState(PublicRegistryEvents.empty(1,'cuda')),
        ClosedAdmission.empty(columns),ledger)
    empty=torch.empty((1,0),dtype=torch.bool,device='cuda');empty_int=run(lambda:integers([]))
    empty_ratio=run(lambda:rational([]))
    intents=FrozenIntentOrders(empty,empty_int,empty,empty_ratio,empty,empty)
    scopes=torch.tensor([[[0],[-1]]],device='cuda');present=scopes>=0
    applicable=torch.ones((1,2),dtype=torch.bool,device='cuda')
    def check_frozen(frozen,expected,prefix_known=yes,*,old_scope=False):
        selected=scopes.flip(1) if old_scope else scopes
        selected_applicable=torch.tensor([[False,True]],device='cuda') if old_scope else applicable
        packed=run(lambda:frozen_account_inputs(frozen,torch.zeros((1,2,0),dtype=torch.bool,device='cuda'),
            applicable,selected_applicable,scope_conditions=selected,scope_present=selected>=0,
            economic_liability_prefix_known=prefix_known))
        assert packed.fault.cpu().tolist()==[0]
        encoded=[math.log1p(float(expected)),1.,1.] if prefix_known.cpu().item() else [0.,0.,1.]
        target=packed.slow[0,308:311] if old_scope else packed.slow[0,293:296]
        other=packed.slow[0,293:296] if old_scope else packed.slow[0,308:311]
        torch.testing.assert_close(target.cpu(),torch.tensor(encoded),rtol=1e-6,atol=1e-7)
        assert other.cpu().tolist()==([0.,0.,0.] if old_scope else [0.,1.,1.])
        assert packed.provided[0,293:296].all().cpu().item() and packed.provided[0,308:311].all().cpu().item()
        assert not packed.provided[0,296:308].any().cpu().item()
        return packed
    scans=[];old=None
    for index,(batch,row) in enumerate(zip(batches,source['records'])):
        if index:
            state=run(lambda:state.append_batch(bank,batch))
            # 一个实际有错误的事务不能先记applied/推进经济prefix，再令负债读面提前变动。
            damaged=replace(state.ledger,fault=torch.ones(1,dtype=torch.int32,device='cuda'))
            failed,used=run(lambda:damaged.apply_record(torch.full_like(zero,index),yes,batch.received_at[:,0],yes))
            assert not used.any().cpu().item() and torch.equal(failed.applied,damaged.applied)
            assert values(failed.platform.platform_source_prefix)==values(damaged.platform.platform_source_prefix)
            assert values(failed.current_entry_sequence)==values(damaged.current_entry_sequence)
        available=row['visibility']['Feedback']['available_at_utc_ms']
        gate=ReceiveGates(batch.received_at[:,0],torch.full_like(zero,available),yes,yes)
        state,delivery=run(lambda:state.receive_first(bank,gate))
        state,scan=_device_scan(state,torch.ones_like(state.ledger.columns.enabled),f'taker_v2_{index}')
        scans.append(scan)
        assert state.ledger.fault.cpu().tolist()==[0]
        expected=Fraction(row['checkpoint']['market']['unpaid_taker_fee_pusd'])
        read=run(lambda:read_economic_liabilities(state.ledger,zero[:,None],yes[:,None],prefix_known=yes))
        assert read.known.cpu().tolist()==[True] and exact(read.taker)==[expected]
        assert exact(read.settlement)==[Fraction(0)]
        assert values(state.ledger.current_entry_sequence)==[row['checkpoint']['projection']['ledger_entry_seq']]
        assert state.ledger.available_through.cpu().tolist()==[available]
        assert state.ledger.has_available.cpu().tolist()==[True]
        frozen=run(lambda:state.freeze_account(intents,gate.public_visible_at))
        check_frozen(frozen,expected)
        duplicate,used=run(lambda:state.ledger.apply_record(torch.full_like(zero,index),yes,gate.closed_receive_at,yes))
        assert not used.any().cpu().item()
        again=run(lambda:read_economic_liabilities(duplicate,zero[:,None],yes[:,None],prefix_known=yes))
        assert exact(again.taker)==[expected]
        if old is None:
            old=frozen;old_expected=expected
        else:
            check_frozen(old,old_expected)
    check_frozen(frozen,expected,torch.zeros_like(yes))
    # 原合法market已结束且无新current；指定真实已投影condition为old子集，
    # 检查旧布局非零和current NA，不借本夹具宣称完整Root old人口选择。
    ended=run(lambda:state.freeze_account(intents,torch.full_like(zero,
        source['initial']['market']['window']['end_at_utc_ms']+1)))
    check_frozen(ended,expected,old_scope=True)
    # 非零冷prefix只保留最后一列；即使调用者有开户known，也不包含首个负债增额。
    incomplete=replace(state.ledger,applied=state.ledger.applied&torch.tensor([[False,True]],device='cuda'))
    read=run(lambda:read_economic_liabilities(incomplete,zero[:,None],yes[:,None],prefix_known=yes))
    assert read.known.cpu().tolist()==[False] and read.fault.cpu().tolist()==[0]
    # 同一已提交设备读面可捕获；实际另一合法scope从非零变为真正空scope，旧值不回写。
    live_present=yes[:,None].clone()
    def graph_read():return read_economic_liabilities(state.ledger,zero[:,None],live_present,prefix_known=yes)
    Numbers.device_exact_batch(graph_read,word_capacity=8,device='cuda')
    graph=torch.cuda.CUDAGraph()
    with torch.cuda.graph(graph):captured=Numbers.device_exact_batch(graph_read,word_capacity=8,device='cuda')
    graph.replay()
    assert captured.fits.cpu().item() and exact(captured.value.taker)==[expected]
    live_present.zero_();graph.replay()
    assert captured.fits.cpu().item() and exact(captured.value.taker)==[Fraction(0)]
    assert captured.value.known.cpu().tolist()==[True]
    check_frozen(old,old_expected)
    return {'public_V2_source_not_natural_SIM':True,'CPU_NOE_and_Ledger_records':len(batches),
        'signed_liability_and_original_position_once_only':True,'actual_freeze_native_293_308':True,
        'old_freeze_after_append_and_capture_stable':True,'missing_cold_economic_history_Unknown':True,
        'transaction_failure_preserves_applied_prefix':True,'captured_ledger_scans':scans}


if __name__=='__main__':
    print(json.dumps(validate(json.loads(Path(sys.argv[1]).read_text())),sort_keys=True))
