#!/usr/bin/env python3
"""真实compact公共步骤→共享设备列→GPU前缀盘口的有限工程验证，不作SIM/hidden验收。"""
import argparse
import json
import subprocess
import sys
import time
import math
from pathlib import Path
from fractions import Fraction
import torch


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--binary',type=Path,required=True)
    parser.add_argument('--material',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--original-reference',type=Path)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    sys.path.insert(0,str(root/'data-pipeline/gpu'))
    from public_state import PublicPmState
    sys.path.insert(0,str(root/'model-train/native-inputs/gpu'))
    from input_books import PmBookInputs
    from input_pm_fields import pm_fields
    from fact_window import PublicFactWindow, PublicTradeWindow
    from input_trades import TradeInputs
    from market_state import PublicMarketState
    from input_lifecycle import LifecycleInputs
    from input_events import EventInputs, time_envelope
    from model_train.book import BookEncoder
    from prepare import _numbers
    started = time.perf_counter()
    print(json.dumps({'stage':'strict_compact_to_pm_device_columns'}),flush=True)
    wire = subprocess.check_output([str(args.binary),'--pm-device-columns',str(args.material),'268435456'])
    columns = json.loads(wire)
    parsed = time.perf_counter()
    print(json.dumps({'stage':'shared_gpu_load','slots':len(columns['slots']),
                      'assets':len(columns['assets']),'decimals':len(columns['decimals']),
                      'cold_seconds':parsed-started}),flush=True)
    state = PublicPmState(columns)
    prefix_values = [-1,0,len(columns['groups'])-1] if columns['groups'] else [-1,-1,-1]
    prefixes = torch.tensor(prefix_values,device='cuda',dtype=torch.int64)
    frame = state.frame(prefixes)
    observation = state.observation(prefixes)
    market_state = PublicMarketState(state,columns)
    market_frame = market_state.frame(prefixes)
    assert not observation.bba.fault.any().cpu().item()
    assert not observation.trade.fault.any().cpu().item()
    assert not observation.tick.fault.any().cpu().item()
    # 短组件参考：只检查3个事实前缀的末项，不重放候选、账户或原全窗CPU诊断。
    def latest(name,prefix):
        result = {}
        for target,sequence,group,value in columns[name]:
            if group <= prefix: result[target] = (sequence,value)
        return result
    expected_values, expected_masks, expected_initialized, expected_ready = [],[],[],[]
    for prefix in prefix_values:
        quantity,selection,reset = (latest(name,prefix) for name in ('quantities','selections','resets'))
        initialized,ready = (latest(name,prefix) for name in ('initialized','ready'))
        for slot,(asset,_,_) in enumerate(columns['slots']):
            reset_seq = reset.get(asset,(-1,0))[0]
            qseq,qid = quantity.get(slot,(-1,0))
            sseq,present = selection.get(slot,(-1,0))
            recent = slot in quantity and qseq >= reset_seq
            expected_values.append(Fraction(columns['decimals'][qid]) if recent else Fraction(0))
            expected_masks.append(slot in selection and sseq >= reset_seq and bool(present))
        for asset in range(len(columns['assets'])):
            expected_initialized.append(bool(initialized.get(asset,(-1,0))[1]))
            rseq,rvalue = ready.get(asset,(-1,0))
            expected_ready.append(asset in ready and bool(rvalue) and rseq >= reset.get(asset,(-1,0))[0])
    n = frame.quantity.numerator.report_values()
    d = frame.quantity.denominator.report_values()
    assert [Fraction(a,b) for a,b in zip(n,d)] == expected_values
    assert frame.present.reshape(-1).cpu().tolist() == expected_masks
    assert frame.initialized.reshape(-1).cpu().tolist() == expected_initialized
    assert frame.ready.reshape(-1).cpu().tolist() == expected_ready
    assert not frame.fault.any().cpu().item()
    assert not frame.present[0].any().cpu().item()
    # 必须先按当前正量压紧再进入原BookEncoder；不让future槽位改变配对树。
    # 工程组件选择首次有真实档位事实的原市场；不签发交易资格或创建账户根。
    target_market = (next(entry[0] for entry in columns['market_assets']
                         if entry[1] == columns['slots'][0][0]) if columns['slots'] else 0)
    original_pair = [asset for _,asset,_ in sorted(
        [entry for entry in columns['market_assets'] if entry[0] == target_market],key=lambda entry:entry[2])]
    assets = torch.tensor([original_pair if original_pair else [0,1]]*3, dtype=torch.int64, device='cuda')
    now = torch.tensor([columns['groups'][max(0,int(prefix))][2] if columns['groups'] else 0 for prefix in prefix_values],
                       dtype=torch.int64,device='cuda')
    if args.original_reference is not None:
        now[-1] = json.loads(args.original_reference.read_text())['observation_ms']
    packer = PmBookInputs(state, initial_capacity=1)
    packed = packer.pack(frame, assets, now)
    native_fields = pm_fields(state,observation,assets,now)
    market_indices = torch.full((3,),target_market,dtype=torch.int64,device='cuda')
    native_lifecycle = LifecycleInputs(market_state).encode(market_frame,market_indices,assets,now)
    assert not native_lifecycle.identity_present[0].cpu().item()
    if columns['markets']:
        assert not native_lifecycle.fault.any().cpu().item()
    else:
        assert native_lifecycle.fault.ne(0).all().cpu().item()
    torch.cuda.reset_peak_memory_stats()  # 正常窗口阶段，含已驻留公共列/盘口/直接字段。
    fact_windows = PublicFactWindow(columns,initial_capacity=1)
    events = EventInputs(fact_windows)
    previous = prefixes.new_tensor([-1,-1,max(-1,prefix_values[-1]-1)])
    native_events = events.encode(previous,prefixes,now)
    trades = TradeInputs(state,PublicTradeWindow(columns,initial_capacity=1))
    native_window_trades = trades.encode(previous,prefixes,assets,now)
    if len(columns['assets']) >= 2:
        assert not native_window_trades.fault.any().cpu().item()
    empty_trades = trades.encode(prefixes,prefixes,assets,now)
    assert torch.count_nonzero(empty_trades.values) == 0
    assert not native_events.fault.any().cpu().item()
    empty_window = events.encode(prefixes,prefixes,now)
    assert torch.count_nonzero(empty_window.values) == 0
    capacities_before_invalid = (fact_windows.fact_capacity,fact_windows.source_capacity)
    invalid_window = events.encode(prefixes.new_tensor([-1]),prefixes.new_tensor([len(columns['groups'])]),now[:1])
    assert invalid_window.fault.ne(0).all().cpu().item()
    assert torch.count_nonzero(invalid_window.values) == 0
    assert capacities_before_invalid == (fact_windows.fact_capacity,fact_windows.source_capacity)
    trade_capacity_before_invalid = trades.window.capacity
    invalid_trade_window = trades.encode(prefixes.new_tensor([-1]),prefixes.new_tensor([len(columns['groups'])]),assets[:1],now[:1])
    assert invalid_trade_window.fault.ne(0).all().cpu().item()
    assert torch.count_nonzero(invalid_trade_window.values) == 0
    assert trades.window.capacity == trade_capacity_before_invalid
    # 真正UTC 100ms网格的末10个窗口，GPU选择合法闭组前缀，区间不按source time筛选。
    if columns['groups']:
        last_grid = (columns['groups'][-1][2]//100)*100
        grid = torch.arange(last_grid-900,last_grid+1,100,dtype=torch.int64,device='cuda')
        end_prefix = state.prefix_at(grid)
        begin_prefix = state.prefix_at(grid-100)
        grid_events = events.encode(begin_prefix,end_prefix,grid)
        assert not grid_events.fault.any().cpu().item()
    torch.cuda.synchronize()
    normal_window_peak = torch.cuda.max_memory_allocated()

    positive_rows = int(packed.mask.sum().cpu())
    if columns['slots']:
        assert positive_rows > 0, '真实来源盘口检查必须实际编码正数量档位，不能以空侧冒充'
    if len(columns['assets']) >= 2:
        assert not packed.fault.any().cpu().item()
        assert not native_fields.fault.any().cpu().item()
    else:
        assert packed.fault.ne(0).all().cpu().item()  # 未知资产坐标返回fault，不补假身份。
        assert native_fields.fault.ne(0).all().cpu().item()
    assert torch.all(packed.mask[:, :, 1:].to(torch.int32) <= packed.mask[:, :, :-1].to(torch.int32))
    assert torch.count_nonzero(packed.levels[~packed.mask]) == 0
    torch.manual_seed(181102)  # 当前有限盘口组件验证，不登记正式训练seed。
    encoder = BookEncoder().cuda()
    levels = torch.nn.functional.pad(packed.levels,(0,0,0,0,0,2)).detach().requires_grad_(True)
    mask = torch.nn.functional.pad(packed.mask,(0,0,0,2))
    encoded = encoder(levels,mask)
    # 尾部资源补齐不能改变固定相邻配对树的模型输出。
    padded = torch.nn.functional.pad(levels,(0,0,0,17))
    padded_mask = torch.nn.functional.pad(mask,(0,17))
    assert torch.equal(encoded,encoder(padded,padded_mask))
    encoded.sum().backward()
    assert torch.isfinite(encoded).all() and torch.isfinite(levels.grad).all()
    assert torch.count_nonzero(levels.grad[~mask]) == 0
    # 时间坐标有界边界：两端i64差必须得到2^64-1，不能在先减法时溢出。
    numbers = _numbers()
    bounds = torch.tensor([-(1<<63),(1<<63)-1],device='cuda',dtype=torch.int64)
    exact = numbers.ExactInt.from_i64_tensor(bounds)
    assert exact.report_values() == [-(1<<63),(1<<63)-1]
    delta = numbers.ExactInt.from_i64_tensor(bounds[1:])-numbers.ExactInt.from_i64_tensor(bounds[:1])
    assert delta.report_values() == [(1<<64)-1]
    assert numbers.combine_fault_rows(torch.tensor([[1,2,4]],device='cuda',dtype=torch.int32)).cpu().tolist() == [7]
    if args.original_reference is not None:
        reference = json.loads(args.original_reference.read_text())
        asset = next(i for i,a in enumerate(columns['assets']) if a['token_id'] == reference['token'])
        slot = next(i for i,(a,side,price) in enumerate(columns['slots'])
                    if a == asset and side == 0 and columns['decimals'][price] == reference['price'])
        assert Fraction(n[2*len(columns['slots'])+slot],d[2*len(columns['slots'])+slot]) == Fraction(reference['quantity'])
        assert frame.present[2,slot].cpu().item() == reference['present']
        assert frame.initialized[2,asset].cpu().item() == reference['initialized']
        assert frame.ready[2,asset].cpu().item() == reference['ready']
        # 两个合法原RTDS入口产生200→100可见时间；t150不能越过依赖t200的前组。
        assert reference['public_visible_times'][:2] == [200,100]
        assert [row[2] for row in columns['groups']] == reference['public_visible_times']
        queries = torch.tensor([99,100,150,199,200],device='cuda',dtype=torch.int64)
        assert state.prefix_at(queries).cpu().tolist() == [-1,-1,-1,-1,1]
        # 仅这个有效公共短链读回原公开事实；不是全窗CPU历史诊断。
        def original_class(payload):
            if 'PolymarketMessage' in payload:
                p = payload['PolymarketMessage']['payload']
                if p['kind'] == 'book': return (0 if p['fact']['kind']=='full_book' else 1),0
                return {'best_bid_ask':2,'tick_size_change':3,'public_trade':4,'new_market':5}[p['kind']],0
            if 'ReferenceFact' in payload:
                assert payload['ReferenceFact']['reference_type'] == 'chainlink_btc_usd'
                return 12,2
            p = payload['MarketFact']['payload']
            key = next(iter(p))
            return {'FixedIdentity':6,'InitialTick':7,'LifecycleTransition':8,'MarketLifecycleSnapshot':8,
                    'TradingState':9,'Terminal':10,'QualityBoundary':11}[key],1
        expected = [0.0]*104
        counts, source_times = [0]*23,[[] for _ in range(9)]
        for event in reference['public_events']:
            kind,time_group = original_class(event['payload'])
            counts[kind] += 1
            for source in event['direct_sources']:
                source_times[time_group].append((source['source_event_at_utc_ms'],source['received_at_utc_ms']))
        def original_log(value):
            value = float(value)
            return math.copysign(math.log1p(abs(value)),value) if value else 0.0
        expected[:23] = [original_log(value) for value in counts]
        for group, rows in enumerate(source_times):
            if not rows: continue
            age = [[],[],[]]
            for source,received in rows:
                age[1].append(reference['observation_ms']-received)
                if source is not None:
                    age[0].append(reference['observation_ms']-source)
                    age[2].append(received-source)
            start = 23+9*group
            for k, values in enumerate(age):
                if values:
                    expected[start+2*k] = original_log(Fraction(min(values),1000))
                    expected[start+2*k+1] = original_log(Fraction(max(values),1000))
                expected[start+6+k] = len(values)/len(rows)
        full_window = events.encode(prefixes.new_tensor([-1]),prefixes[-1:],now[-1:])
        assert torch.equal(full_window.values[0],torch.tensor(expected,device='cuda',dtype=torch.float32)), (full_window.values,expected)
        assert not full_window.fault.any().cpu().item()
        # 原canonical PublicTrade缺量/未知方向仍计入整窗，不被public latest trade抑制。
        full_trades = trades.encode(prefixes.new_tensor([-1]),prefixes[-1:],assets[-1:],now[-1:])
        expected_trades = [0.0]*77
        for leg,asset_id in enumerate(original_pair):
            rows = [event for event in reference['public_events']
                    if 'PolymarketMessage' in event['payload']
                    and event['payload']['PolymarketMessage']['payload']['kind'] == 'public_trade'
                    and event['payload']['PolymarketMessage']['payload']['asset']['token_id'] == columns['assets'][asset_id]['token_id']]
            if not rows: continue
            values = [row['payload']['PolymarketMessage']['payload'] for row in rows]
            out = [0.0]*28
            ps = [Fraction(row['price_pusd_per_share']) for row in values]
            qty = [None if row['quantity_shares'] is None else Fraction(row['quantity_shares']) for row in values]
            sides = [row['side'] for row in values]
            fees = [Fraction(row['fee_rate_bps']) for row in values if row['fee_rate_bps'] is not None]
            out[0] = original_log(len(rows))
            out[1:6] = [float(2*p-1) for p in (ps[0],ps[-1],min(ps),max(ps),sum(ps)/len(rows))]
            out[6] = original_log(sum(q for q in qty if q is not None))
            out[7] = sum(q is not None for q in qty)/len(rows)
            out[8] = original_log(sum(p*q for p,q in zip(ps,qty) if q is not None))
            for k,side in enumerate(['BUY','SELL','UNKNOWN',None]): out[9+k] = original_log(sides.count(side))
            for k,side in enumerate(['BUY','SELL']):
                out[13+k] = original_log(sum(q for q,s in zip(qty,sides) if q is not None and s==side))
                out[15+k] = (sum(q is not None and s==side for q,s in zip(qty,sides))/sides.count(side)) if sides.count(side) else 0.0
            out[17] = original_log(sum(fees)/len(fees)) if fees else 0.0
            out[18] = len(fees)/len(rows)
            age = [[],[],[]]
            for event in rows:
                source,received = event['direct_time']['source_event_at_utc_ms'],event['direct_time']['received_at_utc_ms']
                age[1].append(reference['observation_ms']-received)
                if source is not None:
                    age[0].append(reference['observation_ms']-source); age[2].append(received-source)
            for k,values in enumerate(age):
                if values:
                    out[19+2*k] = original_log(Fraction(min(values),1000))
                    out[20+2*k] = original_log(Fraction(max(values),1000))
                out[25+k] = len(values)/len(rows)
            expected_trades[28*leg:28*(leg+1)] = out
        assert torch.equal(full_trades.values[0],torch.tensor(expected_trades,device='cuda',dtype=torch.float32)), (full_trades.values,expected_trades)
        assert not full_trades.fault.any().cpu().item()
        quality_names = ['fresh','stale','missing','unknown_not_observed','confirmed_gap','invalid']
        def signed_log(value):
            value = float(value)
            return math.copysign(math.log1p(abs(value)),value) if value else 0.0
        lifecycle_expected = [0.0]*103
        market = reference['public_market']
        now_reference = reference['observation_ms']
        lifecycle_expected[0] = signed_log(Fraction(now_reference-market['market']['window']['start_at_utc_ms'],1000))
        lifecycle_expected[1] = signed_log(Fraction(market['market']['window']['end_at_utc_ms']-now_reference,1000))
        def original_envelope(field):
            rows = field['direct_anchors']
            result = [0.0]*9
            if not rows: return result
            ages = [[],[],[]]
            for source in rows:
                s,r = source['source_event_at_utc_ms'],source['received_at_utc_ms']
                ages[1].append(now_reference-r)
                if s is not None: ages[0].append(now_reference-s); ages[2].append(r-s)
            for k,age in enumerate(ages):
                if age:
                    result[2*k] = signed_log(Fraction(min(age),1000))
                    result[2*k+1] = signed_log(Fraction(max(age),1000))
                result[6+k] = len(age)/len(rows)
            return result
        f = market['lifecycle']
        lifecycle_expected[2+quality_names.index(f['quality'])] = 1.0
        if f['value'] is not None and f['quality'] in ('fresh','stale'):
            lifecycle_expected[8] = 1.0
            state_index = {'candidate':0,'prefetched':1,'active':2,'retired':3}.get(f['value'].get('value'),4)
            lifecycle_expected[9+state_index] = 1.0
        lifecycle_expected[14:23] = original_envelope(f)
        whole = next((market[name] for name in ['active','closed','accepting_orders'] if market[name]['value'] is not None),None)
        lifecycle_expected[23+quality_names.index(whole['quality'] if whole else 'unknown_not_observed')] = 1.0
        lifecycle_expected[29] = float(whole is not None)
        for name,start in [('active',30),('closed',41),('accepting_orders',52)]:
            f = market[name]
            if f['value'] is not None and f['quality'] in ('fresh','stale'):
                lifecycle_expected[start] = float(f['value']); lifecycle_expected[start+1] = 1.0
            lifecycle_expected[start+2:start+11] = original_envelope(f)
        f = market['terminal']
        lifecycle_expected[63+quality_names.index(f['quality'])] = 1.0
        for start in (72,83,94): lifecycle_expected[start:start+9] = original_envelope(f)
        if f['value'] is not None and f['quality'] in ('fresh','stale'):
            terminal = f['value']; lifecycle_expected[69] = 1.0
            for side,asset_id in enumerate(original_pair):
                token = columns['assets'][asset_id]['token_id']
                lifecycle_expected[70+side] = float(token==terminal['winning_asset_id'])
                payout = next((row for row in terminal['payout_vector'] if row['asset']['token_id']==token),None)
                if payout is not None:
                    lifecycle_expected[81+11*side] = float(2*Fraction(payout['payout_pusd_per_share'])-1)
                    lifecycle_expected[82+11*side] = 1.0
        assert native_lifecycle.identity_present[2].cpu().item()
        assert torch.equal(native_lifecycle.values[2],torch.tensor(lifecycle_expected,device='cuda',dtype=torch.float32)), (native_lifecycle.values[2],lifecycle_expected)
        for outcome, asset_id in enumerate(original_pair):
            original = next(a for a in reference['public_assets']
                            if a['asset']['token_id'] == columns['assets'][asset_id]['token_id'])
            def exact_value(field,index):
                return Fraction(columns['decimals'][int(field.values[2,asset_id,index].cpu())])
            def selected_anchors(field,index):
                anchor_set = int(field.values[2,asset_id,index].cpu())
                return [columns['anchors'][a]['direct'] for a in columns['anchor_sets'][anchor_set]]
            for selected, original_name, anchor_index in (
                    (observation.bba,'best_bid_ask',6),(observation.trade,'public_trade',5),
                    (observation.tick,'tick',2)):
                reference_field = original[original_name]
                assert selected.present[2,asset_id].cpu().item() == (reference_field['value'] is not None)
                assert selected.values[2,asset_id,-1].cpu().item() == quality_names.index(reference_field['quality'])
                actual_anchors = selected_anchors(selected,anchor_index)
                expected_anchors = reference_field['direct_anchors']
                assert [(a['source_message_id'],a['source_event_at_utc_ms'],a['received_at_utc_ms']) for a in actual_anchors] == [
                    (a['source_message_id'],a['source_event_at_utc_ms'],a['received_at_utc_ms']) for a in expected_anchors]
            bba = original['best_bid_ask']['value']
            if bba is not None:
                for present_index,value_index,name in ((0,1,'best_bid_pusd_per_share'),
                        (2,3,'best_ask_pusd_per_share'),(4,5,'spread_pusd_per_share')):
                    assert observation.bba.values[2,asset_id,present_index].cpu().item() == (bba[name] is not None)
                    if bba[name] is not None: assert exact_value(observation.bba,value_index) == Fraction(bba[name])
            trade = original['public_trade']['value']
            if trade is not None:
                assert exact_value(observation.trade,0) == Fraction(trade['price_pusd_per_share'])
                assert exact_value(observation.trade,1) == Fraction(trade['quantity_shares'])
                assert observation.trade.values[2,asset_id,2].cpu().item() == (trade['aggressor_side'] == 'SELL')
                assert observation.trade.values[2,asset_id,3].cpu().item() == (trade['fee_rate_bps'] is not None)
                if trade['fee_rate_bps'] is not None: assert exact_value(observation.trade,4) == Fraction(trade['fee_rate_bps'])
            tick = original['tick']['value']
            if tick is not None: assert exact_value(observation.tick,1) == Fraction(tick)
            def reference_time(field):
                result = [0.0]*5
                if len(field['direct_anchors']) == 1:
                    a = field['direct_anchors'][0]
                    r, s = a['received_at_utc_ms'],a['source_event_at_utc_ms']
                    result[1] = signed_log(Fraction(reference['observation_ms']-r,1000))
                    result[4] = 1.0
                    if s is not None:
                        result[0] = signed_log(Fraction(reference['observation_ms']-s,1000))
                        result[2] = signed_log(Fraction(r-s,1000))
                        result[3] = 1.0
                return result
            for name,width in (('best_bid_ask',28),('public_trade',18)):
                f = original[name]
                expected = [0.0]*width
                expected[quality_names.index(f['quality'])] = 1.0
                value = f['value'] if f['quality'] in ('fresh','stale') else None
                expected[6] = float(value is not None)
                time_fields = reference_time(f)
                if width == 28:
                    for member,key in enumerate(['best_bid_pusd_per_share','best_ask_pusd_per_share','spread_pusd_per_share']):
                        start = 7+7*member
                        if value is not None and value[key] is not None:
                            exact = Fraction(value[key])
                            expected[start] = float(2*exact-1) if member < 2 else signed_log(exact)
                            expected[start+1] = 1.0
                        expected[start+2:start+7] = time_fields
                    actual = native_fields.bba[2,outcome]
                else:
                    if value is not None:
                        expected[7] = float(2*Fraction(value['price_pusd_per_share'])-1)
                        expected[8] = signed_log(Fraction(value['quantity_shares']))
                        expected[9 if value['aggressor_side'] == 'BUY' else 10] = 1.0
                        if value['fee_rate_bps'] is not None:
                            expected[11] = signed_log(Fraction(value['fee_rate_bps']))
                            expected[12] = 1.0
                    expected[13:] = time_fields
                    actual = native_fields.trade[2,outcome]
                assert torch.equal(actual,torch.tensor(expected,dtype=torch.float32,device='cuda'))
            for side, name in enumerate(['bids','asks']):
                book = original[name]
                assert frame.quality[2,asset_id].cpu().item() == quality_names.index(book['quality'])
                expected_rows = []
                values = book['value'] if book['quality'] in ('fresh','stale') else None
                for level in values or []:
                    p, q = Fraction(level['price_pusd_per_share']), Fraction(level['quantity_shares'])
                    if q <= 0: continue
                    anchors = [a for a in book['direct_anchors']
                               if a['field_path'] == f"assets[{original['asset']['token_id']}].{name}[{level['price_pusd_per_share']}]" ]
                    time_features = [0.0]*5
                    if len(anchors) == 1:
                        anchor = anchors[0]
                        received, source = anchor['received_at_utc_ms'],anchor['source_event_at_utc_ms']
                        time_features[1] = signed_log(Fraction(reference['observation_ms']-received,1000))
                        time_features[4] = 1.0
                        if source is not None:
                            time_features[0] = signed_log(Fraction(reference['observation_ms']-source,1000))
                            time_features[2] = signed_log(Fraction(received-source,1000))
                            time_features[3] = 1.0
                    best = Fraction((values or [])[0]['price_pusd_per_share'])
                    expected_rows.append([float(2*p-1),signed_log(q),1.0,*time_features,
                                          signed_log(best-p if side == 0 else p-best)])
                actual = packed.levels[2,2*outcome+side][packed.mask[2,2*outcome+side]]
                expected = torch.tensor(expected_rows,device='cuda',dtype=torch.float32).reshape(-1,9)
                assert torch.equal(actual,expected), (actual,expected)
    # 设备前缀可以连续查询且未来价位始终由present mask屏蔽；查询不重置账户或GRU。
    # sentinel与历史是同一块持久存储；连续查询不重建/复制整列。
    columns_for_query = [getattr(state,name) for name in ('quantities','selections','resets','initialized','ready','ticks')]
    storage = [(column.rows.untyped_storage().data_ptr(),column.padded.untyped_storage().data_ptr())
               for column in columns_for_query]
    assert all(a == b for a,b in storage)
    for _ in range(64): state.frame(prefixes)
    assert storage == [(column.rows.untyped_storage().data_ptr(),column.padded.untyped_storage().data_ptr())
                       for column in columns_for_query]
    invalid = state.frame(prefixes.new_tensor([len(columns['groups'])]))
    assert invalid.fault.ne(0).all().cpu().item()
    assert invalid.prefix_fault.ne(0).all().cpu().item()
    torch.cuda.synchronize()
    passed = ['real_source_shared_exact_decimal_columns','gpu_prefix_quantity_and_selection',
              'initialization_and_ready_prefix','future_slots_masked',
              'continuous_device_query','out_of_material_prefix_fault',
              'i64_time_coordinate_exact_difference','fault_bit_union','pm_direct_observation_fields',
              'native_events104_legal_prefix_window','empty_and_invalid_window','utc_100ms_window_prefix',
              'native_window_trades77_exact_aggregation','public_market_identity_prefix','native_lifecycle103']
    if columns['slots']:
        passed += ['exact_price_sort_current_positive_compaction','native_pm_nine_fields',
                   'real_book_encoder_forward_backward','tail_padding_pairwise_tree','native_pm_bba28_trade18']
    else:
        passed += ['legal_missing_book','metadata_identity_preserved_or_unknown_coordinate_fault']
    report = {'source_commit':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),
              'identity':('engineering_real_public_pm_book_inputs_only' if columns['slots']
                          else 'engineering_missing_book_boundary'),'device':torch.cuda.get_device_name(),
              'groups':len(columns['groups']),'original_pm_steps':columns['original_pm_steps'],
              'assets':len(columns['assets']),'slots':len(columns['slots']),
              'decimal_dictionary':len(columns['decimals']),'derived_column_wire_bytes':len(wire),
              'encoded_positive_rows':positive_rows,'raw_trade_facts':len(columns['raw_trades']),'public_facts':len(columns['facts']),
              'direct_sources':len(columns['fact_sources']),
              'cold_seconds':parsed-started,'total_seconds':time.perf_counter()-started,
              'peak_allocated_bytes':torch.cuda.max_memory_allocated(),
              'normal_window_peak_allocated_bytes':normal_window_peak,
              'window_peak_scope':'驻留共享公共列及累计可见时间、已打包盘口和直接字段、三前缀窗口与末10个100ms窗口；非法窗不扩池',
              'passed':passed,
              'original_core_reference_checked':args.original_reference is not None,
              'not_delivered':['reference_state','binance_state','hidden','sim_noe_ledger',
                               '100ms_window','full_actor','ppo','active_root_restore']}
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps(report,ensure_ascii=False),flush=True)


if __name__ == '__main__': main()
