"""有限跨UTC后验：原设备语义的无损表示交接，不产生撮合或资金期望。"""
from fractions import Fraction
import json


def fields(encoded):
    """读取fields.encode_fields的原长度、整数、有理及实际容器人口。"""
    end=encoded[0]+1
    def integer(at):
        assert encoded[at]==2
        sign,size=encoded[at+1:at+3]
        value=sign*sum(encoded[at+3+i]<<(32*i) for i in range(size))
        return value,at+3+size
    result=[];at=1
    while at<end:
        tag=encoded[at]
        if tag==1:value=encoded[at+1];at+=2
        elif tag==2:value,at=integer(at)
        elif tag==3:
            numerator,at=integer(at+1);denominator,at=integer(at)
            value=Fraction(numerator,denominator)
        elif tag==4:
            population=encoded[at+1];at+=2;value=[]
            for _ in range(population):member,at=integer(at);value.append(member)
        elif tag==5:
            population=encoded[at+1];at+=2;value=[]
            for _ in range(population):
                size=encoded[at]+1;value.append(fields(encoded[at:at+size]));at+=size
        else:raise ValueError(f'原字段未知tag {tag}')
        result.append(value)
    assert at==end
    return result


def decimal(value):
    """ExactDecimal保持原精度；不能把非有限小数舍入成新的源金额。"""
    value=Fraction(value);denominator=value.denominator;twos=fives=0
    while denominator%2==0:denominator//=2;twos+=1
    while denominator%5==0:denominator//=5;fives+=1
    assert denominator==1,('原ExactDecimal表示不能舍入',str(value))
    scale=max(twos,fives);units=abs(value.numerator)*2**(scale-twos)*5**(scale-fives)
    text=str(units).zfill(scale+1)
    if scale:text=text[:-scale]+'.'+text[-scale:]
    return ('-' if value<0 else '')+text


class ActualNoeRepresentation:
    """只绑定本次实际bank的原目录与正式身份编码器。"""
    def __init__(self,codec,opening,actual):
        self.codec=codec;self.actual=actual;self.context=opening['opening_context']
        self.account=self.context['account_identity'];self.identities={}
        self.run_id=opening['opening_position']['source_watermark']['run_id']

    def identity(self,key,*,entity=False):
        cache=(entity,json.dumps(key,sort_keys=True))
        if cache not in self.identities:
            request=dict(account=self.account,entities=[key] if entity else [],logical=[] if entity else [key])
            entities,logical=json.loads(self.codec.normalization_identities_v1(json.dumps(request)))
            self.identities[cache]=(entities if entity else logical)[0]
        return self.identities[cache]

    def order(self,value):
        return self.identity({'Order':{'source_order_id':self.codec.sim_order_ids_v1([value])[0]}},entity=True)

    def trade(self,value):
        # matching.rs的真实fill来源ID也是sim-{fill_sequence}；实体种类仍为Trade。
        return self.identity({'Trade':{'source_trade_id':self.codec.sim_order_ids_v1([value])[0]}},entity=True)

    def resolution(self,value):
        return self.identity({'Resolution':{'source_resolution_id':self.actual['canonical_ids'][value]}},entity=True)

    def condition(self,value):return self.actual['conditions'][value]
    def token(self,value):return self.actual['tokens'][value]

    def logical(self,value):
        _,kind,*tail=fields(value)
        if kind==3:return {'SubmissionItem':dict(global_request_id=self.actual['requests'][str(tail[0])],batch_item_ordinal=tail[1])}
        if kind==4:return {'CancellationTarget':dict(global_request_id=self.actual['requests'][str(tail[0])],target_ordinal=tail[1])}
        if kind==5:return {'DirectResponseRequest':dict(global_request_id=self.actual['requests'][str(tail[0])])}
        if kind==7:return {'OrderLifecycle':dict(order_identity=self.codec.sim_order_ids_v1([tail[0]])[0],occurrence=tail[1])}
        if kind==8:return {'TradeLifecycle':dict(trade_identity=self.codec.sim_order_ids_v1([tail[0]])[0],occurrence=tail[1])}
        if kind==9:return {'SettlementLifecycle':dict(private_feedback_sequence=tail[0])}
        if kind==11:return {'AccountTransition':dict(account_transition_seq=tail[0])}
        if kind==12:return {'RebateDay':dict(kind=['Maker','Taker'][tail[0]],attribution_day_start_utc_ms=tail[1])}
        raise ValueError(f'本代表尚无实际读口的原逻辑种类 {kind}')

    def contribution(self,value):
        trade,condition,kind,day,amount=value
        return dict(trade_identity=self.trade(trade),condition_id=self.condition(condition),
            kind=['Maker','Taker'][kind],attribution_day_start_utc_ms=day,contribution_pusd=decimal(amount))

    def payload(self,schema,encoded,logical):
        values=fields(encoded);_,*v=values
        order_states=['Open','Matched','VenueDustTerminal','MarketResolvedCanceled','Canceled','Expired','Delayed','Unmatched']
        if schema==3:
            outcome,condition,token,order,state,matched,open_quantity,at=v
            result=['Accepted','RejectedNoEffect','EvidenceUnknownNoEffect'][outcome]
            if outcome==0:
                result={'Accepted':dict(order=dict(order_identity=self.order(order),condition_id=self.condition(condition),
                    token_id=self.token(token),state=order_states[state]),measurement={'Base':dict(
                    matched_base_quantity_shares=decimal(Fraction(matched,1000000)),
                    open_base_quantity_shares=decimal(Fraction(open_quantity,1000000)))},effective_processing_at_utc_ms=at)}
            return {'Control':{'result':{'Submission':{'outcome':result}}}}
        if schema==27:
            outcome,condition,token,order,state,matched,at=v
            result=['Accepted','RejectedNoEffect','EvidenceUnknownNoEffect'][outcome]
            if outcome==0:
                result={'Accepted':dict(order=dict(order_identity=self.order(order),condition_id=self.condition(condition),
                    token_id=self.token(token),state=order_states[state]),measurement={'QuoteBudget':dict(
                    actual_matched_base_quantity_shares=decimal(Fraction(matched,1000000)))},
                    effective_processing_at_utc_ms=at)}
            return {'Control':{'result':{'Submission':{'outcome':result}}}}
        if schema==15:
            order,outcome=v
            return {'Control':{'result':{'Cancellation':dict(order_identity=self.order(order),
                outcome=['Canceled','NotCanceledNoEffect','EvidenceUnknownNoEffect'][outcome])}}}
        if schema==16:
            canceled,no_effect,unknown=v
            return {'Control':{'result':{'CancellationRequestCompleted':{'result':dict(
                canceled_count=canceled,not_canceled_no_effect_count=no_effect,evidence_unknown_no_effect_count=unknown)}}}}
        if schema in (7,19):
            condition,token,order,event,state,*extra=v
            events={0:{'Accepted':{'state':order_states[state]}},4:'CallerCanceled'}
            if schema==19:events[5]={'MarketResolvedCanceled':{'resolution_identity':self.resolution(extra[0])}}
            return {'OrderLifecycle':{'result':dict(order_identity=self.order(order),condition_id=self.condition(condition),
                token_id=self.token(token),event=events[event])}}
        if schema==8:
            condition,token,trade,order,state,disposition,matched_at,role,price,base,quote,fee,has_sequence,sequence,maker_kind,maker_value=v
            trade_result=dict(trade_identity=self.trade(trade),order_identity=self.order(order),condition_id=self.condition(condition),
                token_id=self.token(token),state=['MatchedNotBroadcasted','Matched','Retrying','Mined','Confirmed','Failed'][state],
                economic_disposition=['PendingUnapplied','PendingApplied','FinalSuccess','FinalFailureUnapplied','FinalFailureCompensated'][disposition],
                fill_sequence=trade,matched_at_utc_ms=matched_at,liquidity_role=['Taker','MakerInferred'][role],
                source_price_pusd_per_share=decimal(price),base_quantity_shares=decimal(Fraction(base,1000000)),
                quote_amount_pusd=decimal(Fraction(quote,1000000)),immediate_taker_fee_pusd=decimal(Fraction(fee,1000000)),
                economic_application_account_transition_seq=sequence if has_sequence else None)
            maker={'Known':decimal(maker_value)} if maker_kind==0 else ['Known','Unavailable','NotApplicable'][maker_kind]
            return {'TradeLifecycleV3':{'result':dict(result=trade_result,maker_fee_equivalent_pusd=maker)}}
        if schema==9:return {'SettlementLifecycleV2':{'result':{'Contribution':self.contribution([v[1],v[0],*v[2:]])}}}
        if schema==14:
            condition,status,contributions=v
            result={'MakerAccrualFinalized':dict(condition_id=self.condition(condition),
                status=['Calculated','ZeroNoEligibleMakerFill','UnknownIncomplete'][status],
                contributions=[self.contribution(member) for member in contributions])}
            return {'SettlementLifecycleV2':{'result':result}}
        if schema==12:
            kind,day,accrued,payable,locked,refs=v
            return {'SettlementLifecycleV2':{'result':{'DayFinalized':dict(kind=['Maker','Taker'][kind],
                attribution_day_start_utc_ms=day,accrued_pusd=decimal(accrued),payable_pusd=decimal(payable),
                locked_remainder_pusd=decimal(locked),contribution_refs=[self.trade(ref) for ref in refs])}}}
        if schema==13:
            kind,day,account,reference_kind,reference_rebate,reference_day,paid=v
            assert (account,reference_kind,reference_rebate,reference_day)==(values[0],12,kind,day)
            identity=self.identity({'RebateDay':dict(kind=['Maker','Taker'][kind],attribution_day_start_utc_ms=day)})
            return {'SettlementLifecycleV2':{'result':{'Payment':dict(kind=['Maker','Taker'][kind],
                attribution_day_start_utc_ms=day,day_result_identity=identity,paid_pusd=decimal(paid))}}}
        if schema==17:
            condition,resolution,terminal=v
            return {'SettlementLifecycleV2':{'result':{'Resolved':dict(condition_id=self.condition(condition),
                resolution_identity=self.resolution(resolution),payout_vector=self.actual['terminals'][terminal])}}}
        if schema==28:
            condition,resolution,source_at=v
            return {'SettlementLifecycle':{'result':{'AutoRedeemApplied':dict(condition_id=self.condition(condition),
                resolution_identity=self.resolution(resolution),resolution_source_event_at_utc_ms=source_at)}}}
        if schema in (11,18,29):return self.economic(schema,v,logical)
        raise ValueError(f'本代表实际schema {schema}尚未机械交接')

    def economic(self,schema,v,logical):
        if schema==29:
            condition,at,resolution,*tail=v;balances=tail[:8];members=tail[8];liabilities=tail[9:]
            transition={'AutoRedeemApplied':{'resolution_identity':self.resolution(resolution)}}
        else:
            condition,at,tag,entity,path,*tail=v;balances=tail[:8];has_outcome,token,outcome,*liabilities=tail[8:]
            members=[[token,outcome]] if has_outcome else []
            if schema==18:
                resolution=liabilities.pop();transition={'MarketResolutionEncumbranceReleased':dict(
                    resolution_identity=self.resolution(resolution),order_identity=self.order(entity))}
            elif tag in (0,2):transition={['OrderAcceptedEncumbranceApplied',None,'CallerCancellationEncumbranceReleased'][tag]:{'order_identity':self.order(entity)}}
            elif tag in (6,7,8):transition={['FillFinalityEconomicApplied','FillFinalityUnappliedFailureReleased','FillFinalityCompensationApplied'][tag-6]:dict(trade_identity=self.trade(entity),path_ordinal=path)}
            elif tag==9:transition='MakerRebateAccrualFinalized'
            elif tag==10:transition={'TakerRebateAccrualApplied':{'trade_identity':self.trade(entity)}}
            elif tag in (11,12,13,14):transition={['MakerRebateDayFinalized','TakerRebateDayFinalized','MakerRebatePaid','TakerRebatePaid'][tag-11]:{'attribution_day_utc_ms':entity}}
            else:raise ValueError(f'本代表实际转移 {tag}尚未机械交接')
        names=['cash_pusd_delta','available_pusd_delta','principal_encumbrance_pusd_delta','immediate_taker_fee_encumbrance_pusd_delta',
            'maker_rebate_receivable_pusd_delta','taker_rebate_receivable_pusd_delta','maker_rebate_locked_remainder_pusd_delta','taker_rebate_locked_remainder_pusd_delta']
        existing={name:decimal(value) for name,value in zip(names,balances,strict=True)}
        existing['outcome_balance_deltas']=[dict(token_id=self.token(token),balance_shares_delta=decimal(amount)) for token,amount in members]
        settlement,taker=liabilities
        return {'EconomicV2':{'evidence':dict(economic_key=logical,disposition={'AccountTransition':dict(
            economic_effective_at_utc_ms=at,condition_id=None if condition<0 else self.condition(condition),
            transition={'Existing':transition},balance_delta=dict(existing=existing,settlement_fee_liability_pusd_delta=decimal(settlement),
                actual_taker_fee_liability_pusd_delta=decimal(taker)))})}}

    def batches(self):
        bank=self.actual['bank'];natural={}
        for batch in self.actual['batches']:
            evidence=[]
            for index,on in enumerate(batch['emitted'][0]):
                if not on:continue
                row=batch['rows'][0][index];logical=self.logical(bank['logical_keys'][row])
                private=batch['private_sequences']['integer'][index];received=batch['received_at'][0][index]
                category=batch['source_categories'][0][index];platform=batch['platform_at'][0][index]
                if category==0:
                    if 'SubmissionItem' in logical:association={'SubmissionItem':logical['SubmissionItem']}
                    elif 'DirectResponseRequest' in logical:association={'Request':logical['DirectResponseRequest']}
                    else:
                        target=logical['CancellationTarget']
                        references=fields(bank['cancellation'][row])[0]
                        method=batch['cancellation_method'][0][index]
                        association={'CancellationTarget':dict(target,method={2:'cancel_order',3:'cancel_orders',
                            4:'cancel_market_orders',5:'cancel_all_orders'}[method],input_item_refs=[dict(
                            item_id=self.actual['cancel_item_ids'][item],batch_item_ordinal=ordinal) for item,ordinal in references])}
                    source={'DirectResponse':dict(private_feedback_sequence=private,platform_responded_at_utc_ms=platform,
                        response_received_at_utc_ms=received,association=association)}
                else:
                    source={['DirectResponse','OrderLifecycle','TradeLifecycle','SettlementLifecycle'][category]:dict(
                        private_feedback_sequence=private,platform_event_at_utc_ms=platform,feedback_received_at_utc_ms=received)}
                evidence.append(dict(source_record=source,source_ordinal=bank['source_order'][row][3],
                    logical_key=logical,kind=self.payload(bank['payload_schemas'][row],bank['semantic'][row],logical)))
            if evidence:
                sequences=[seq for seq,on in zip(batch['batch_sequences']['integer'],batch['emitted'][0],strict=True) if on]
                assert len(set(sequences))==1
                natural.setdefault(sequences[0],[]).extend(evidence)
        # GPU片段保留原自然批号；CPU的一个NormalizationBatchInput仍是该原完整批。
        return [dict(source_watermark=dict(run_id=self.run_id,projection_batch_sequence=sequence),evidence=evidence)
            for sequence,evidence in sorted(natural.items())]


def initial_noe_container(config):
    """只选择本次真实冷会话的唯一已提交容器，不打开可写store。"""
    from pathlib import Path
    containers=list(Path(config['opening']['noe_root']).glob('context-*/commits/*.parquet'))
    assert len(containers)==1,('本有限后验需唯一实际冷开户批',containers)
    return containers[0]


def delivery_points(report):
    """原receiver发生顺序；没有领取人口的冷起点单独核对。"""
    return [point for point in report['checkpoints'] if point['received_through'] is not None]


def posterior_input(codec,opening,actual,initial_noe_container,report):
    """原发布上界和实际领取人口决定后验输入；毫秒不是领取身份。"""
    representation=ActualNoeRepresentation(codec,opening,actual)
    points=delivery_points(report)
    deliveries=[[point['received_through'],point['visible_at'][0]] for point in points]
    bounds=[dict(run_id=representation.run_id,projection_batch_sequence=point['accepted_batch']) for point in points]
    return dict(context=opening['opening_context'],opening_position=opening['opening_position'],
        opening_record=opening['opening_record'],batches=representation.batches(),deliveries=deliveries,
        accepted_through=bounds,report_consumed=[point['consumed_population'][0] if point['financial'] else None for point in points],
        initial_noe_container=str(initial_noe_container))


def check_posterior(actual,cpu,report):
    """CPU原账本核对实际NOE；本后验不声称独立撮合或上游规范化。"""
    names=('cash_pusd','available_pusd','principal_encumbrance_pusd','immediate_taker_fee_encumbrance_pusd',
        'maker_rebate_receivable_pusd','taker_rebate_receivable_pusd','maker_rebate_locked_remainder_pusd','taker_rebate_locked_remainder_pusd')
    def compare(gpu,checkpoint):
        projection=checkpoint['projection'];platform=projection['platform_confirmed_projection']
        assert gpu['platform_prefix']==[projection['platform_source_prefix']]
        assert list(map(Fraction,gpu['balances']))==[Fraction(platform[name]) for name in names]
        balances={row['token_id']:Fraction(row['balance_shares']) for row in platform['outcome_balances']}
        assert list(map(Fraction,gpu['outcomes']))==[balances.get(token,Fraction(0)) for token in actual['tokens']]
    points=delivery_points(report)
    assert len(points)==len(cpu['checkpoints'])
    count=0
    for gpu,delivery in zip(points,cpu['checkpoints'],strict=True):
        assert delivery['received_population']==gpu['received_population'][0]
        if not gpu['financial']:continue
        point=next(point for point in cpu['prefixes'] if point['consumed_population']==gpu['consumed_population'][0])
        position=point['last_position']
        coordinates=None if position is None else [position['source_watermark']['projection_batch_sequence'],position['record_ordinal']]
        assert coordinates==gpu['last_position'],(coordinates,gpu['last_position'])
        compare(gpu,point);count+=1
    for gpu in report['checkpoints']:
        if gpu['received_through'] is None and gpu['financial']:compare(gpu,cpu['initial'])
    assert cpu['waiting']==0 and cpu['checkpoints']
    final=cpu['final'];compare(report['final_ledger'],final)
    for name in ('actual_available','actual_reserved','maker_available','maker_reserved'):
        assert list(map(Fraction,report['final_ledger']['funds'][name]))==[Fraction(final['cash'][name+'_pusd'])]
    return dict(checked_financial_checkpoints=count,final_platform_prefix=report['final_ledger']['platform_prefix'],
        scope='实际设备语义→原类型兼容与CPU账本经济归约；未独立验证撮合或GPU上游规范化')


if __name__=='__main__':
    from pathlib import Path
    import importlib.util
    import sys
    import time
    config=json.loads(Path(sys.argv[1]).read_text());artifact=Path(config['artifact'])
    spec=importlib.util.spec_from_file_location('model_native_inputs',config['native_library'])
    codec=importlib.util.module_from_spec(spec);spec.loader.exec_module(codec)
    opening=json.loads((artifact/'actual-opening.json').read_text())
    actual=json.loads((artifact/'actual-noe.json').read_text())
    report=json.loads((artifact/'actual-report.json').read_text())
    started=time.perf_counter();request=posterior_input(codec,opening,actual,initial_noe_container(config),report)
    (artifact/'cpu-posterior-input.json').write_text(json.dumps(request))
    cpu=json.loads(codec.cpu_noe_posterior_v1(json.dumps(request)))
    (artifact/'cpu-posterior.json').write_text(json.dumps(cpu))
    result=check_posterior(actual,cpu,report)
    print(json.dumps(dict(result,batches=len(request['batches']),deliveries=len(request['deliveries']),
        seconds=time.perf_counter()-started)),flush=True)
