"""原BatchMapper/Core的冷强类型对照，非训练逐事件CPU编码入口。"""
import importlib.util
from pathlib import Path
import sys
import torch


def validate(cases):
    path=Path(__file__).with_name('core.py')
    spec=importlib.util.spec_from_file_location('gpu_noe_core',path)
    core=importlib.util.module_from_spec(spec)
    sys.modules[spec.name]=core
    spec.loader.exec_module(core)
    strings={}

    def token(value):
        # 无hash：冷字符串字典逐值intern；容器类型、字段名、全u64精确词均保留。
        # 仅编码原强类型serde产生的规范值，不能作为任意JSON消费者的等值规则。
        if value is None:
            return [0]
        if isinstance(value,bool):
            return [1,int(value)]
        if isinstance(value,int):
            magnitude=abs(value)
            limbs=[]
            while magnitude:
                limbs.append(magnitude&0xffffffff)
                magnitude>>=32
            return [2,int(value<0),len(limbs),*limbs]
        if isinstance(value,str):
            return [3,strings.setdefault(value,len(strings))]
        if isinstance(value,list):
            return [4,len(value),*[item for child in value for item in token(child)]]
        return [5,len(value),*[item for name in sorted(value) for child in [name,value[name]] for item in token(child)]]

    def columns(rows):
        width=max((len(row) for row in rows),default=0)
        return torch.tensor([row+[0]*(width-len(row)) for row in rows],dtype=torch.int64,device='cuda')

    logical,economic,semantic,cancellation,kinds,source_order,legacy=[],[],[],[],[],[],[]
    coordinates=[]
    for case in cases:
        local=[]
        for row in case['input']['evidence']:
            local.append(len(kinds))
            logical.append([0,*token(row['logical_key'])])
            name,body=next(iter(row['kind'].items()))
            if name=='EconomicV2':
                kind=core.ECONOMIC
                key=body['evidence']['economic_key']
                payload=body['evidence']['disposition']
            elif name=='Economic':
                kind=core.ECONOMIC
                tag,fields=next(iter(body['evidence'].items()))
                identity='opening_evidence_identity' if tag=='AccountOpening' else 'account_transition_seq'
                key={tag:{identity:fields[identity]}}
                payload={tag:{key:value for key,value in fields.items() if key!=identity}}
            elif name=='LateEconomicCoverage':
                kind=core.LATE_COVERAGE
                key=body['economic_key']
                payload=row['kind']
            else:
                kind=core.PENDING if name=='Pending' else core.EVIDENCE
                key=None
                payload=row['kind']
            economic.append([0,*token(key)])
            semantic.append(token(payload))
            record=row['source_record']
            cancel=None
            if record=='AccountOpening':
                sequence_tag,sequence=0,0
            else:
                source_tag,fields=next(iter(record.items()))
                sequence_tag=1 if source_tag=='MarketEnrollment' else 2
                sequence=fields['enrollment_sequence'] if sequence_tag==1 else fields['private_feedback_sequence']
                association=fields.get('association',{})
                if isinstance(association,dict) and 'CancellationTarget' in association:
                    cancel=association['CancellationTarget']['input_item_refs']
            cancellation.append(token(cancel))
            kinds.append(kind)
            source_order.append([sequence_tag,sequence&0xffffffff,sequence>>32,row['source_ordinal']])
            # 真实映射器ordinal唯一，本夹具不会进入同序legacy排序分支。
            legacy.append(0)
        coordinates.append(local)
    bank=core.EvidenceBank(columns(logical),columns(economic),columns(semantic),columns(cancellation),
        torch.tensor(kinds,dtype=torch.int64,device='cuda'),columns(source_order),
        torch.tensor(legacy,dtype=torch.int64,device='cuda'),torch.zeros(len(kinds),dtype=torch.int32,device='cuda'))
    state=core.Normalizer.empty(bank,1)
    outcomes={'Evidence':core.OUT_EVIDENCE,'EconomicFact':core.OUT_FACT,'EconomicFactV2':core.OUT_FACT,
        'Duplicate':core.OUT_DUPLICATE,'EconomicAlias':core.OUT_ALIAS,'Pending':core.OUT_PENDING}
    counts={}
    for case,indices in zip(cases,coordinates):
        rows=torch.tensor([indices],dtype=torch.int64,device='cuda')
        state,batch=state.prepare(rows,torch.ones_like(rows,dtype=torch.bool))
        expected=[]
        for record in case['expected']['records']:
            name,body=next(iter(record.items()))
            counts[name]=counts.get(name,0)+1
            expected.append(core.OUT_LOGICAL_CONFLICT if name=='Conflict' and 'LogicalIdentity' in body['scope'] else
                core.OUT_ECONOMIC_CONFLICT if name=='Conflict' else outcomes[name])
        assert batch.outcomes.cpu().tolist()==[expected],case['name']
        assert batch.emitted.cpu().tolist()==[[True]*len(expected)],case['name']
        # 原排序位置是身份正文之外独立source_order，不改号也不按领取时序排序。
        sorted_rows=batch.rows.cpu().tolist()[0]
        actual=[source_order[row] for row in sorted_rows]
        expected_order=[]
        for record in case['expected']['records']:
            body=next(iter(record.values()))
            order=body['source_order']
            sequence=order['sequence']
            if sequence=='AccountOpening':
                tag,value=0,0
            else:
                name,fields=next(iter(sequence.items()))
                tag=1 if name=='MarketEnrollment' else 2
                value=next(iter(fields.values()))
            expected_order.append([tag,value&0xffffffff,value>>32,order['ordinal']])
        assert actual==expected_order,case['name']
    assert counts.get('Duplicate',0)>0 and counts.get('EconomicAlias',0)>0 and counts.get('Conflict',0)>0 and counts.get('Pending',0)>0
    return {'cases':len(cases),'outcomes':counts,'whole_GPU_NOE_source_mapping_delivered':False}


def validate_trades(records,frames):
    import json
    from types import SimpleNamespace
    from queues import Numbers
    from validate_base_execution import integers,rational,values,units
    sys.path.insert(0,str(Path(__file__).parent))
    from trade import map_trade
    from core import EvidenceBank,Normalizer,EVIDENCE,OUT_EVIDENCE,OUT_DUPLICATE
    from fields import append_rows,encode_fields
    source={}
    status={'MatchedNotBroadcasted':0,'Matched':1,'Retrying':2,'Mined':3,'Confirmed':4,'Failed':5}
    for row in records:
        body=row['record'].get('Evidence')
        if body is None or 'TradeLifecycleV3' not in body['evidence']:
            continue
        fact=body['evidence']['TradeLifecycleV3']['result']['result']
        source[(fact['fill_sequence'],status[fact['state']])]=(body,fact)
    state=None
    count=0
    for record,frame,price in frames:
        body,fact=source[(values(frame.fill_sequence)[0],int(frame.event.state.cpu()[0]))]
        reference=body['source_reference']['record']['TradeLifecycle']
        private=integers([reference['private_feedback_sequence']])
        ordinal=torch.tensor([body['source_order']['ordinal']],device='cuda')
        zero=torch.zeros(1,dtype=torch.int64,device='cuda')
        read_kind=torch.full_like(zero,2)
        mapped=Numbers.run_exact_batch(lambda:map_trade(record,frame,integers([1]),price,zero,zero,zero,
            private,ordinal,liquidity_role=zero,maker_fee_equivalent=rational(['0']),maker_fee_read_kind=read_kind))
        # 原类型参考保留完整实际payload；只在冷对照中解析原sim-N订单身份。
        entity=json.loads(fact['order_identity'].split(':',1)[1])[-1]['Order']['source_order_id']
        order=int(entity.removeprefix('sim-'))
        applied=fact['economic_application_account_transition_seq'] is not None
        expected=Numbers.run_exact_batch(lambda:encode_fields([zero,zero,zero,integers([fact['fill_sequence']]),
            integers([order]),torch.tensor([status[fact['state']]],device='cuda'),
            torch.tensor([{'PendingUnapplied':0,'PendingApplied':1,'FinalSuccess':2,
                'FinalFailureUnapplied':3,'FinalFailureCompensated':4}[fact['economic_disposition']]],device='cuda'),
            torch.tensor([fact['matched_at_utc_ms']],device='cuda'),zero,rational([fact['source_price_pusd_per_share']]),
            integers([units(fact['base_quantity_shares'])]),integers([units(fact['quote_amount_pusd'])]),
            integers([units(fact['immediate_taker_fee_pusd'])]),torch.tensor([applied],device='cuda'),
            integers([fact['economic_application_account_transition_seq'] or 0]),read_kind,rational(['0'])]))
        width=max(expected.size(1),mapped.semantic.size(1))
        assert torch.equal(torch.nn.functional.pad(expected,(0,width-expected.size(1))),
            torch.nn.functional.pad(mapped.semantic,(0,width-mapped.semantic.size(1))))
        assert mapped.emitted.cpu().tolist()==[True]
        bank=EvidenceBank(mapped.logical_keys,torch.zeros((1,1),dtype=torch.int64,device='cuda'),mapped.semantic,
            torch.zeros((1,1),dtype=torch.int64,device='cuda'),torch.full_like(zero,EVIDENCE),mapped.source_order,
            zero,mapped.fault)
        if state is None:
            state=Normalizer.empty(bank,1)
        else:
            state=state.append_bank(bank)
        row=torch.tensor([[count]],device='cuda')
        state,result=state.prepare(row,torch.ones_like(row,dtype=torch.bool))
        assert result.outcomes.cpu().tolist()==[[OUT_EVIDENCE]] and result.emitted.cpu().tolist()==[[True]]
        before=state.logical_rows.size(1)
        state,repeat=state.prepare(row,torch.ones_like(row,dtype=torch.bool))
        assert repeat.outcomes.cpu().tolist()==[[OUT_DUPLICATE]] and state.logical_rows.size(1)==before
        count+=1
    assert count==9
    # 同值不同物理容量的整数必须得到同一字段编码，不能引入伪冲突。
    one=integers([1])
    first=encode_fields([one])
    second=encode_fields([one.with_capacity(32)])
    width=max(first.size(1),second.size(1))
    assert torch.equal(torch.nn.functional.pad(first,(0,width-first.size(1))),
        torch.nn.functional.pad(second,(0,width-second.size(1))))
    return {'actual_trade_lifecycles':count,'same_values_different_word_capacity_equal':True,
        'SIM_feedback_full_batch_mapping_delivered':False}
