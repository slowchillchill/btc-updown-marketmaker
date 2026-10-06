"""原V4逐记录前缀初始化后的Ledger日消费者定向对照。

原CPU前缀仅冷构造验证输入；不是生产热循环，也不代表活动根恢复。
本切片不重复实现尚未接齐的Maker接单/日生产者或其它NOE字段。
"""
from dataclasses import replace
import json
import importlib.util
import sys
from pathlib import Path
import torch
from queues import Numbers,ExactInt,ExactRational
from validate_base_execution import integers,rational,values
from fields import IntegerList,RecordList
from lifecycle import map_maker_finalized,map_day_finalized,map_rebate_payment
from core import EvidenceBank,Normalizer,EVIDENCE
from projection import ProjectionBatch
from memory import EconomicColumns
from admission import AdmissionLifecycle
from success_pool import SuccessPools
from rebate_day import RebateDays


def trade_id(value):
    return int(json.loads(value.split(':',1)[1])[-1]['Trade']['source_trade_id'].removeprefix('sim-'))


def settlement(record):
    return record['record'].get('Evidence',{}).get('evidence',{}).get('SettlementLifecycleV2',{}).get('result')


def key(record):
    p=record['position']
    return p['context_reference'],p['source_watermark']['run_id'],p['source_watermark']['projection_batch_sequence'],p['record_ordinal']


def validate(source):
    conditions={value:index for index,value in enumerate(source['conditions'])}
    attempts=[record for record in source['records'] if settlement(record) is not None]
    unique={key(record):record for record in attempts}
    population=list(unique.values())
    population.sort(key=lambda record:(record['record']['Evidence']['source_order']['sequence']['PrivateFeedback']['private_feedback_sequence'],
        record['record']['Evidence']['source_order']['ordinal']))
    rows_by_key={key(record):index for index,record in enumerate(population)}
    zero=torch.zeros(1,dtype=torch.int64,device='cuda')
    enabled=torch.ones_like(zero,dtype=torch.bool)
    mapped=[]
    for record in population:
        body=record['record']['Evidence']
        event=settlement(record)
        private=integers([body['source_reference']['record']['SettlementLifecycle']['private_feedback_sequence']])
        ordinal=torch.tensor([body['source_order']['ordinal']],device='cuda')
        if 'MakerAccrualFinalized' in event:
            value=event['MakerAccrualFinalized']
            vector=value['contributions']
            fields=(integers([trade_id(member['trade_identity']) for member in vector]),
                torch.tensor([conditions[member['condition_id']] for member in vector],device='cuda'),
                torch.zeros(len(vector),dtype=torch.int64,device='cuda'),
                torch.tensor([member['attribution_day_start_utc_ms'] for member in vector],device='cuda'),
                rational([member['contribution_pusd'] for member in vector]))
            function=lambda:map_maker_finalized(torch.tensor([conditions[value['condition_id']]],device='cuda'),
                torch.tensor([{'Calculated':0,'ZeroNoEligibleMakerFill':1,'UnknownIncomplete':2}[value['status']]],device='cuda'),
                RecordList(fields,torch.ones((1,len(vector)),dtype=torch.bool,device='cuda')),private,ordinal,zero,enabled)
        elif 'DayFinalized' in event:
            value=event['DayFinalized']
            refs=IntegerList(integers([trade_id(member) for member in value['contribution_refs']]),
                torch.ones((1,len(value['contribution_refs'])),dtype=torch.bool,device='cuda'))
            function=lambda:map_day_finalized(zero,torch.tensor([value['attribution_day_start_utc_ms']],device='cuda'),
                rational([value['accrued_pusd']]),rational([value['payable_pusd']]),rational([value['locked_remainder_pusd']]),
                refs,private,ordinal,zero,enabled)
        else:
            value=event['Payment']
            function=lambda:map_rebate_payment(zero,torch.tensor([value['attribution_day_start_utc_ms']],device='cuda'),
                rational([value['paid_pusd']]),private,ordinal,zero,enabled)
        mapped.append(Numbers.run_exact_batch(function))
    bank=None
    for value in mapped:
        incoming=EvidenceBank(value.logical_keys,zero[:,None],value.semantic,zero[:,None],zero,
            value.source_order,zero,value.fault,torch.full_like(zero,value.payload_schema),frozenset([value.payload_schema]))
        bank=incoming if bank is None else bank.append(incoming)
    width=len(population)
    rows=torch.arange(width,device='cuda')[None,:]
    normalizer,normalized=Normalizer.empty(bank,1).prepare(rows,torch.ones_like(rows,dtype=torch.bool))
    # 实际位置沿原V4引用保留；这些已经规范化的来源记录不重编号。
    positions=[key(record) for record in population]
    private=integers([record['record']['Evidence']['source_order']['sequence']['PrivateFeedback']['private_feedback_sequence'] for record in population])
    batch=integers([p[2] for p in positions])
    ordinal=integers([p[3] for p in positions])
    platform_at=torch.tensor([[record['record']['Evidence']['source_reference']['record']['SettlementLifecycle']['platform_event_at_utc_ms'] for record in population]],device='cuda')
    received=torch.tensor([[record['record']['Evidence']['source_reference']['record']['SettlementLifecycle']['feedback_received_at_utc_ms'] for record in population]],device='cuda')
    # 原private递增；共同prepare不会重排本人口，必须明确检查。
    assert torch.equal(normalized.rows,rows)
    frame=ProjectionBatch(normalized,private,batch,ordinal,received,rows,normalized.emitted,normalized.fault,
        torch.full_like(rows,3),platform_at,torch.full_like(rows,-1),torch.full_like(rows,-1),torch.zeros_like(rows),torch.zeros_like(rows),
        integers([0]*width),rational(['0']*(width*8)),rational(['0']*width),torch.zeros_like(rows),torch.zeros_like(rows,dtype=torch.bool))
    columns=Numbers.run_exact_batch(lambda:EconomicColumns.from_projection_batches([frame]))
    pools=Numbers.run_exact_batch(lambda:SuccessPools.open(torch.tensor([0,0,0,0],device='cuda'),
        torch.tensor([0,0,1,1],device='cuda'),torch.tensor([0,1,2,3],device='cuda'),torch.tensor([1,0,3,2],device='cuda'),1))
    spec=importlib.util.spec_from_file_location('gpu_ledger_platform_rebate',Path(__file__).resolve().parents[2]/'ledger/gpu/platform.py')
    module=importlib.util.module_from_spec(spec)
    sys.modules[spec.name]=module
    spec.loader.exec_module(module)
    PlatformBalances=module.PlatformBalances
    platform=Numbers.run_exact_batch(lambda:PlatformBalances.open(rational(['0']*8),rational(['0']*4),
        torch.tensor([0,0,0,0],device='cuda'),integers([0])))
    days=Numbers.run_exact_batch(lambda:RebateDays.empty(torch.tensor([list(range(len(conditions)))],device='cuda'),1))
    admission=Numbers.run_exact_batch(lambda:AdmissionLifecycle.open(platform,bank,columns,integers([0]),integers([0]),pools,rebate_days=days))
    checked=0
    deferred=0
    names=['cash_pusd','available_pusd','principal_encumbrance_pusd','immediate_taker_fee_encumbrance_pusd',
        'maker_rebate_receivable_pusd','taker_rebate_receivable_pusd','maker_rebate_locked_remainder_pusd','taker_rebate_locked_remainder_pusd']
    for record in attempts:
        before,after=record['before'],record['after']
        # 只恢复此消费者所需的实际已验前缀；其它模块的经济源另有纵向验证。
        balance=before['base']['platform_confirmed_projection']
        platform=replace(admission.platform,balances=rational([balance[name] for name in names]),
            platform_source_prefix=integers([before['base']['platform_source_prefix']]))
        admission=replace(admission,platform=platform,current_entry_sequence=integers([before['base']['ledger_entry_seq']]),
            current_local_prefix=integers([before['base']['ledger_local_prefix']]))
        for trade in before['trades']:
            identity=integers([trade_id(trade['trade_identity'])])
            condition=torch.tensor([conditions[trade['fact']['condition_id']]],device='cuda')
            directory,error=Numbers.run_exact_batch(lambda:admission.trades.set(identity,condition,torch.tensor([False],device='cuda'),
                admission.current_entry_sequence,enabled))
            assert error.cpu().tolist()==[0]
            at,found,_=directory.lookup(identity)
            assert found.cpu().tolist()==[True]
            mask=torch.zeros_like(admission.trade_success.success_applied).scatter(1,at[:,None],enabled[:,None])
            success=replace(admission.trade_success,success_applied=torch.where(mask,
                torch.tensor([trade['success_applied']],device='cuda')[:,None],admission.trade_success.success_applied))
            admission=replace(admission,trades=directory,trade_success=success)
        row=torch.tensor([rows_by_key[key(record)]],device='cuda')
        available=torch.tensor([record['visibility']['Feedback']['available_at_utc_ms']],device='cuda')
        admission,consumed=Numbers.run_exact_batch(lambda:admission.apply_record(row,enabled,available,enabled))
        assert admission.fault.cpu().tolist()==[0]
        assert consumed.cpu().tolist()==[not record['deferred']]
        assert values(admission.current_entry_sequence)==[after['base']['ledger_entry_seq']]
        assert values(admission.platform.platform_source_prefix)==[after['base']['platform_source_prefix']]
        assert int(admission.applied[0,row].cpu()[0])==int(not record['deferred'])
        checked+=1
        deferred+=int(record['deferred'])
    assert checked>=4 and deferred>0 and admission.rebate_days.payment_observed.cpu().tolist()==[[True]]
    assert admission.rebate_days.maker_accrual_finalized.cpu().tolist()==[[True,True]]
    from gpu_rebate_day_validation import amounts
    from fractions import Fraction
    assert amounts(admission.rebate_days.condition_payable)==[Fraction(value['numerator'])/Fraction(value['denominator']) for value in source['expected_pay']]
    assert amounts(admission.rebate_days.condition_locked)==[Fraction(value['numerator'])/Fraction(value['denominator']) for value in source['expected_lock']]
    return {'actual_V4_rebate_consumer_attempts':checked,'actual_deferred_rebate_attempts':deferred,
        'actual_position_and_ledger_prefix_checked':True,'cold_original_prefix_initialization':True,
        'full_activity_restore_or_day_producer_delivered':False}
