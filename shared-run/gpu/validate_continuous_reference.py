"""真实已发正文的独立原CPU后验；本入口不向GPU生产链提供期望或事件。"""
import json
from pathlib import Path
import sys
import time
from fractions import Fraction
import torch  # 先加载标准库依赖，再加入含 ledger/gpu/platform.py 的模块目录。

ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/p) for p in ('sim-system/gpu','model-train/native-inputs/gpu',
    'data-pipeline/gpu','execution-normalization/gpu','ledger/gpu','shared-run/gpu')]
from validate_pending_post import original_semantic,run,units,BALANCES


def integer(column,index=0):return column['integer'][index]


def check(actual,source,requests,initial=None,authority=None,catalog=None):
    started=time.perf_counter()
    extension=source['continuation'];stages=extension['stages']
    if extension.get('goal'):
        assert initial==source['initial_actual_request']
        assert initial['sent_at']==source['sent_at'] and initial['input_sequence']==source['input_sequence']
        assert initial['body']['request']['context']['global_request_id']==source['request_id']
        assert initial['body']['request']['items']==source['posts']
    assert len(stages)==len(requests)
    post=actual['post_results'];seen_posts=set();seen_cancels=set()
    for request,stage in zip(requests,stages,strict=True):
        assert request['sent_at']==stage['sent_at']
        body=request['body']['request']
        if request['body']['method']=='POST_ORDERS':
            for original_item,effect in zip(body['items'],stage['effects'],strict=True):
                ordinal=original_item['batch_item_ordinal'];original=effect['body']['Post']
                # 原稳定正文槽属于请求本身；按实际生产ordinal/processing定位结果。
                positions=[i for i,on in enumerate(post['present']) if on and
                    post['processed_at'][i]==effect['at'] and post['ordinals'][i]==ordinal]
                assert len(positions)==1,(effect,positions)
                index=positions[0];assert index not in seen_posts;seen_posts.add(index)
                if 'Accepted' not in original:
                    from pending_post_intents import REJECTED_REASONS
                    assert post['outcomes'][index]==1 and REJECTED_REASONS[post['reasons'][index]]==original['RejectedNoEffect']
                    continue
                accepted=original['Accepted'];measurement=accepted['measurement'];quantity=post['quantity']
                assert post['outcomes'][index]==0
                assert integer(post['order_sequence'],index)==int(accepted['platform_order_id'].removeprefix('sim-'))
                assert integer(post['accepted_sequence'],index)==accepted['account_transition_sequences'][0]
                quote=measurement['measurement']=='QUOTE_BUDGET'
                assert quantity['quantity_kind'][index]==int(quote)
                mapping={'accepted_quote_budget_units':'accepted_quote_budget_pusd','matched_quote_units':'used_quote_amount_pusd',
                    'released_or_unspent_quote_units':'released_or_unspent_quote_amount_pusd','actual_matched_base_units':'actual_matched_base_quantity_shares',
                    'sdk_taker_amount_audit_base_units':'sdk_taker_amount_audit_base_quantity_shares'} if quote else {
                    'accepted_base_units':'accepted_base_quantity_shares','accepted_quote_units':'accepted_quote_amount_pusd',
                    'matched_base_units':'matched_base_quantity_shares','matched_quote_units':'matched_quote_amount_pusd',
                    'open_base_units':'open_base_quantity_shares','open_encumbered_quote_units':'open_encumbered_quote_amount_pusd',
                    'released_price_improvement_quote_units':'released_price_improvement_quote_amount_pusd',
                    'venue_dust_closed_base_units':'venue_dust_closed_base_quantity_shares','venue_dust_released_quote_units':'venue_dust_released_quote_amount_pusd'}
                for field,key in mapping.items():assert integer(quantity[field],index)==units(measurement[key]),(field,measurement)
                assert integer(post['fee'],index)==units(measurement['immediate_taker_fee_encumbered_pusd'])
                width=len(post['fill_present'][index]);fills=[]
                for column,on in enumerate(post['fill_present'][index]):
                    if on:
                        at=index*width+column
                        fills.append((integer(post['fill_sequences'],at),integer(post['fill_bases'],at),integer(post['fill_quotes'],at),Fraction(post['fill_prices']['rational'][at])))
                expected=[(int(f['trade_id'].removeprefix('sim-')),units(f['base_quantity_shares']),units(f['quote_amount_pusd']),
                    Fraction(f['source_price_pusd_per_share'])) for f in accepted['fills']]
                assert fills==expected,(fills,expected)
        else:
            for effect in stage['effects']:
                for target in effect['body']['Cancel']:
                    matches=[item for group in actual['cancel_results'] if group['present'][0] for item in group['targets']
                        if item['present'][0] and item['processed_at'][0]==target['effective_processing_at_utc_ms'] and
                        integer(item['target_ordinal'])==target['target_ordinal'] and
                        integer(item['order_sequence'])==int(target['platform_order_id'].removeprefix('sim-'))]
                    assert len(matches)==1,(target,matches)
                    item=matches[0];seen_cancels.add((integer(item['order_sequence']),integer(item['target_ordinal'])))
                    refs=item['input_item_refs'];actual_refs=[(i,o) for i,o,on in zip(*refs['fields'],refs['present'][0],strict=True) if on]
                    item_slots={post['item']['item_id']:i for i,post in enumerate(body['items'])}
                    assert actual_refs==[(item_slots[r['item_id']],r['batch_item_ordinal']) for r in target['input_item_refs']]
                    outcome=target['outcome']
                    if 'Canceled' in outcome:
                        value=outcome['Canceled'];assert item['outcome']==[0] and item['reason']==[-1]
                        assert integer(item['canceled_base_units'])==units(value['canceled_base_quantity_shares'])
                        assert integer(item['released_quote_units'])==units(value['released_quote_amount_pusd'])
                        assert item['account_sequence_known']==[value['account_transition_seq'] is not None]
                        if value['account_transition_seq'] is not None:assert integer(item['account_sequence'])==value['account_transition_seq']
                    else:
                        assert item['outcome']==[1] and item['reason']==[['NotFoundOrNotOwned','NoCancellableRemainder','TemporaryFailureNoEffect'].index(outcome['NotCanceledNoEffect'])]
    original_records=[*source['records'],*extension['records']]
    lookup={(r['position']['source_watermark']['projection_batch_sequence'],r['position']['record_ordinal']):r for r in original_records}
    assert len(lookup)==len(original_records)==len(actual['noe'])
    actual_keys=[(row['batch'],row['ordinal']) for row in actual['noe']]
    assert len(set(actual_keys))==len(actual_keys) and set(actual_keys)==set(lookup)
    context=dict(source,records=original_records)
    for count,row in enumerate(actual['noe'],1):
        key=row['batch'],row['ordinal'];original=lookup[key]
        expected=run(lambda:original_semantic(original,context)).cpu().tolist()[0];got=row['semantic'][0]
        width=max(len(expected),len(got));assert got+[0]*(width-len(got))==expected+[0]*(width-len(expected)),key
        body=next(iter(original['record'].values()));ref=body.get('source_reference') or body['fact']['source_reference']
        kind,metadata=next(iter(ref['record'].items()))
        assert row['private']==metadata['private_feedback_sequence']
        assert row['received']==metadata['response_received_at_utc_ms' if kind=='DirectResponse' else 'feedback_received_at_utc_ms']
        if count%4==0 or count==len(actual['noe']):print(json.dumps(dict(stage='independent_noe',processed=count,total=len(actual['noe']),elapsed_seconds=time.perf_counter()-started)),flush=True)
    ledger=extension['goal']['ledger'] if extension.get('goal') else stages[-1]['ledger'];projection=ledger['projection'];got=actual['ledger']
    if extension.get('goal'):
        # 真实冷消费路径不同；比较各自首次预留前基线之后的提交数，不从最终差额反推偏移。
        gpu_initial=authority['ledger_projection'];cpu_initial=source['opening_ledger']['projection']
        assert gpu_initial['account_instance_key']==cpu_initial['account_instance_key']
        for field in ('ledger_local_prefix','platform_source_prefix'):
            assert gpu_initial[field]==cpu_initial[field]
        assert got['entry'][0]-gpu_initial['ledger_entry_seq']==projection['ledger_entry_seq']-cpu_initial['ledger_entry_seq']
        # GPU实际目录与CPU正式消费的InitialMarket需表达同一市场、窗口和token/outcome。
        initialized=[r['record']['Evidence']['evidence']['InitialMarket']['result']
            for r in source['cold_directory_records_consumed'] if 'Evidence' in r['record'] and
            'InitialMarket' in r['record']['Evidence']['evidence']]
        expected=[dict(market=market,assets=[catalog['assets'][asset] for owner,asset,outcome in catalog['market_assets']
            if owner==index]) for index,market in enumerate(catalog['markets'])]
        assert initialized==expected,(initialized,expected)
    else:
        assert got['entry']==[projection['ledger_entry_seq']]
    assert got['local']==[projection['ledger_local_prefix']]
    assert got['platform_prefix']==[projection['platform_source_prefix']]
    assert list(map(Fraction,got['balances']))==[Fraction(projection['platform_confirmed_projection'][name]) for name in BALANCES]
    for field,key in (('actual_available','actual_available_pusd'),('maker_available','maker_available_pusd'),
        ('actual_reserved','actual_reserved_pusd'),('maker_reserved','maker_reserved_pusd')):
        assert list(map(Fraction,got['funds'][field]))==[Fraction(ledger['cash'][key])],(field,got['funds'][field],ledger['cash'][key])
    return dict(passed=True,scope='original CPU actual emitted requests→results/NOE/Ledger posterior; no device event driving',
        complete_finite_goal=extension['goal']['cutoff'] if extension.get('goal') else None,
        entry_baselines=dict(gpu=gpu_initial['ledger_entry_seq'],cpu=cpu_initial['ledger_entry_seq'],
            gpu_final=got['entry'][0],cpu_final=projection['ledger_entry_seq']) if extension.get('goal') else None,requests=len(requests),post_items=len(seen_posts),cancel_targets=len(seen_cancels),noe=len(original_records),elapsed_seconds=time.perf_counter()-started)


if __name__=='__main__':
    actual_path,source_path=map(Path,sys.argv[1:])
    initial_path=actual_path.parent/'actual-continuous-initial-request.json'
    initial=json.loads(initial_path.read_text()) if initial_path.exists() else None
    source=json.loads(source_path.read_text());finite=bool(source['continuation'].get('goal'))
    authority=json.loads((actual_path.parent/'cold-authority.json').read_text()) if finite else None
    catalog=json.loads((actual_path.parent/'public-page/manifest.json').read_text())['catalog'] if finite else None
    report=check(json.loads(actual_path.read_text()),source,
        json.loads((actual_path.parent/'actual-continuous-requests.json').read_text()),initial,authority,catalog)
    print(json.dumps(report),flush=True)
