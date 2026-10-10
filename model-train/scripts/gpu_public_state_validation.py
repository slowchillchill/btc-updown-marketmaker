#!/usr/bin/env python3
"""真实compact公共步骤→共享设备列→GPU前缀盘口的有限工程验证，不作SIM/hidden验收。"""
import argparse
import json
import subprocess
import sys
import time
import math
import os
import bisect
import hashlib
from pathlib import Path
from fractions import Fraction
import torch


def resource_retry(compute, pools, *, metadata=lambda value:value):
    """有限验证入口的图外资源退出；同一输入重算，不在设备算子里扩池。"""
    from prepare import _numbers
    numbers=_numbers(); words=4
    while True:
        candidate=numbers.device_exact_batch(compute,word_capacity=words,device='cuda')
        result=metadata(candidate.value)
        fit=candidate.fits & result.enough.all()
        if bool(fit.cpu()): return candidate.value
        required=result.required.reshape(-1,len(pools)).max(dim=0).values
        requests=torch.cat((candidate.required_words.reshape(1),required)).cpu().tolist()
        words=1<<(max(4,requests[0])-1).bit_length()
        for (owner,name),need in zip(pools,requests[1:]):
            if need>getattr(owner,name): setattr(owner,name,1<<(need-1).bit_length())


def validate_binance_received_day(args,root):
    """独立短材料按实际可见prefix核原BN；不借旧44秒模型观察访问未来源。"""
    sys.path[:0]=[str(root/'data-pipeline/gpu'),str(root/'model-train/native-inputs/gpu')]
    from public_state import PublicPmState
    from binance_state import PublicBinanceState
    from input_binance import BinanceInputs
    columns=json.loads(subprocess.check_output([str(args.binary),'--pm-device-columns',str(args.material),'268435456']))
    state=PublicPmState(columns);binance=PublicBinanceState(state,columns)
    assert columns['binance_symbols']==['BTCUSDT']
    inputs=BinanceInputs(binance,initial_capacity=1)
    rows=json.loads(args.binance_received_day_reference.read_text())
    assert [r['phase'] for r in rows]==['fresh_before_received_day','received_day_missing','fresh_after_received_day']
    assert [r['depth']['quality'] for r in rows]==['fresh','missing','fresh']
    assert all(a['prefix']<b['prefix'] and a['visible_at']<b['visible_at'] for a,b in zip(rows,rows[1:]))
    phases=[];previous_fresh=None
    for row in rows:
        prefix=torch.tensor([row['prefix']],device='cuda');frame=binance.frame(prefix)
        symbol=torch.zeros_like(prefix);now=prefix.new_tensor([row['visible_at']])
        native=resource_retry(lambda:inputs.encode(frame,binance.observation(prefix),symbol,now),
            [(inputs.book,'capacity')],metadata=lambda value:value.packed)
        original=row['depth'];ready=original['value'] is not None and original['quality']=='fresh'
        assert frame.ready.cpu().tolist()==[[ready]] and native.fault.cpu().tolist()==[0]
        quality=['fresh','stale','missing','unknown_not_observed','confirmed_gap','invalid'].index(original['quality'])
        assert native.side.values[0,0,quality].cpu().item()==1
        expected={}
        if ready:
            for side,name in enumerate(('bids','asks')):
                levels=original['value'][name]
                for level in levels:expected[side,Fraction(level['price_usdt_per_btc'])]=Fraction(level['quantity_btc'])
                values=[[math.log1p(float(Fraction(level['price_usdt_per_btc']))),
                    math.log1p(float(Fraction(level['quantity_btc']))),1.,*([0.]*5),
                    math.log1p(float(Fraction(levels[0]['price_usdt_per_btc'])-Fraction(level['price_usdt_per_btc'])
                        if side==0 else Fraction(level['price_usdt_per_btc'])-Fraction(levels[0]['price_usdt_per_btc'])))]
                    for level in levels]
                torch.testing.assert_close(native.packed.levels[0,side][native.packed.mask[0,side]].cpu(),
                    torch.tensor(values,dtype=torch.float32),rtol=0,atol=0)
            previous_fresh=expected.copy()
        else:
            assert not native.packed.mask.any().cpu().item() and not native.side.values[0,0,6:].any().cpu().item()
            assert previous_fresh is not None
            expected=previous_fresh
        # Missing保留前一独立observer原载荷但不可用；拒绝99不写，新snapshot重置旧价位。
        actual=[Fraction(n,d) for n,d in zip(frame.quantity.numerator.report_values(),frame.quantity.denominator.report_values())]
        expected_slots=[expected.get((side,Fraction(columns['decimals'][price])),Fraction(0))
            for symbol,side,price in columns['binance_slots']]
        assert actual==expected_slots
        phases.append({'phase':row['phase'],'prefix':row['prefix'],'visible_at':row['visible_at'],'quality':original['quality']})
    report={'actual_received_day_compact_and_original_observer':True,'GPU_phases':phases,
        'Missing_retained_payload_unavailable':True,'rejected_delta_and_old_levels_reset_by_new_snapshot':True,
        'root_or_model_window':False}
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report),flush=True)


def validate_valuation(args,root):
    """三个原选择反例的实际来源prefix；不建立训练root或以GPU自产预期。"""
    sys.path.insert(0,str(root/'data-pipeline/gpu'))
    from public_state import PublicPmState
    from public_device_page import (PreparedPublicPage,build_public_device_page,
        BASE_PREFIXES,BASE_TABLES,BASE_BANKS,BASE_DERIVED,PREVIOUS_VERSION)
    from dataclasses import fields
    import numpy as np
    import tempfile
    import public_state as public_module
    wire=json.loads(subprocess.check_output([str(args.binary),'--pm-device-columns',str(args.material),'268435456']))
    raw=PublicPmState(wire)
    built=build_public_device_page(args.binary,args.material,args.device_page)
    saved=(torch.argsort,torch.bincount,public_module.decimal_columns,public_module.PrefixColumn.__init__)
    def forbidden(*a,**kw): raise AssertionError('prepared估值加载不得重排或恢复原numeric')
    try:
        torch.argsort=torch.bincount=public_module.decimal_columns=public_module.PrefixColumn.__init__=forbidden
        page=PreparedPublicPage(args.device_page,args.material);state=page.install()[0]
    finally:
        torch.argsort,torch.bincount,public_module.decimal_columns,public_module.PrefixColumn.__init__=saved
    reference=json.loads(args.valuation_reference.read_text())
    assert reference and raw.valuation.complete and state.valuation.complete
    ids={asset['token_id']:i for i,asset in enumerate(page.columns['assets'])}
    prefixes=torch.tensor([row['prefix'] for row in reference],dtype=torch.int64,device='cuda')
    original=raw.valuation_bid(prefixes);actual=state.valuation_bid(prefixes)
    for field in fields(actual): assert torch.equal(getattr(original,field.name),getattr(actual,field.name)),field.name
    assert not actual.fault.any().cpu().item()
    prices=actual.price_ids.cpu().tolist();known=actual.value_present.cpu().tolist()
    bids=actual.bid_present.cpu().tolist();qualities=actual.quality.cpu().tolist()
    arrival=state.observation(prefixes).bba
    values=arrival.values.cpu().tolist();present=arrival.present.cpu().tolist()
    quality_names=['fresh','stale','missing','unknown_not_observed','confirmed_gap','invalid']
    for root_index,row in enumerate(reference):
        for position,asset in enumerate(row['observation']['assets']):
            index=ids[asset['asset']['token_id']];expected=asset['valuation_best_bid']
            assert known[root_index][index] == row['valuation_presence'][position],row['phase']
            assert qualities[root_index][index] == quality_names.index(expected['quality']),row['phase']
            assert bids[root_index][index] == (expected['value'] is not None),row['phase']
            if expected['value'] is not None:
                assert Fraction(page.columns['decimals'][prices[root_index][index]]) == Fraction(expected['value']),row['phase']
            # 相同来源内原到达BBA仍按原observer，独立估值不回写其值。
            bba=asset['best_bid_ask'];v=values[root_index][index]
            assert present[root_index][index] == (bba['value'] is not None),row['phase']
            if bba['value'] is not None:
                for p,d,name in ((0,1,'best_bid_pusd_per_share'),(2,3,'best_ask_pusd_per_share')):
                    assert bool(v[p]) == (bba['value'][name] is not None),row['phase']
                    if v[p]: assert Fraction(page.columns['decimals'][v[d]]) == Fraction(bba['value'][name]),row['phase']
    # 同图沿所有实际prefix重放；旧冻结所有权快照不被下一估值读取覆盖。
    prefix=prefixes[:1].clone()
    stream=torch.cuda.Stream();stream.wait_stream(torch.cuda.current_stream())
    with torch.cuda.stream(stream):
        for _ in range(2): state.valuation_bid(prefix)
    torch.cuda.current_stream().wait_stream(stream)
    graph=torch.cuda.CUDAGraph()
    with torch.cuda.graph(graph,stream=stream): captured=state.valuation_bid(prefix)
    frozen=None;frozen_expected=None
    for index,row in enumerate(reference):
        prefix.copy_(prefixes[index:index+1]);graph.replay()
        for field in fields(captured):
            wanted=getattr(actual,field.name)[index:index+1]
            torch.testing.assert_close(getattr(captured,field.name),wanted,rtol=0,atol=0)
        if frozen is None and (actual.bid_present[index] & actual.value_present[index]).any().cpu().item():
            frozen={field.name:getattr(captured,field.name).clone() for field in fields(captured)}
            frozen_expected={field.name:getattr(actual,field.name)[index:index+1].clone() for field in fields(captured)}
    assert frozen is not None
    for name,value in frozen.items(): torch.testing.assert_close(value,frozen_expected[name],rtol=0,atol=0)
    # 旧v2确有原消费者；格式投影仅用于兼容边界，缺版本不能提供估值。
    with tempfile.TemporaryDirectory(prefix='valuation-old-v2-',dir=args.device_page.parent) as temporary:
        legacy=Path(temporary);manifest=json.loads((args.device_page/'manifest.json').read_text())
        keep=set(BASE_TABLES)|set(BASE_DERIVED)|{'decimal_scale','decimal_present','decimal_text','decimal_offsets'}
        keep|={n+'.'+f for n in BASE_PREFIXES for f in ('padded','offsets')}
        keep|={n+'.'+f for n in ('coefficient','decimal_denominator','numerator','denominator') for f in ('words','sign','fault')}
        keep|={n+'.'+f for n in ('anchor_sets','terminal_payouts') for f in ('values','offsets')}
        keep|={n+'.'+f for n in BASE_BANKS for f in ('utf8','offsets')}
        manifest['version']=PREVIOUS_VERSION;manifest['metadata']['version']='public_pm_device_columns_v1'
        manifest['members']={n:e for n,e in manifest['members'].items() if n in keep}
        for entry in manifest['members'].values(): os.link(args.device_page/entry['file'],legacy/entry['file'])
        (legacy/'manifest.json').write_text(json.dumps(manifest))
        old=PreparedPublicPage(legacy,args.material).install()[0]
        assert not old.valuation_bid(prefixes).value_present.any().cpu().item()
        for field in fields(old.frame(prefixes)):
            before=getattr(old.frame(prefixes),field.name);after=getattr(state.frame(prefixes),field.name)
            if isinstance(before,torch.Tensor): assert torch.equal(before,after),field.name
        for name in ('numerator','denominator'):
            for field in ('words','sign','fault'):
                assert torch.equal(getattr(getattr(old.frame(prefixes).quantity,name),field),
                    getattr(getattr(state.frame(prefixes).quantity,name),field)),(name,field)
        assert torch.equal(old.observation(prefixes).bba.values,arrival.values)
    # 一个新版本成员越界：合法摘要不能替代真实消费边界。
    with tempfile.TemporaryDirectory(prefix='valuation-invalid-',dir=args.device_page.parent) as temporary:
        broken=Path(temporary);manifest=json.loads((args.device_page/'manifest.json').read_text())
        entry=manifest['members']['pm_versions'];target=broken/entry['file']
        import shutil
        shutil.copyfile(args.device_page/entry['file'],target)
        versions=np.memmap(target,dtype='<i8',mode='r+',shape=tuple(entry['shape']))
        assert len(versions)>1
        versions[1,3]=1<<32;versions.flush();del versions
        entry['sha256']=hashlib.sha256(target.read_bytes()).hexdigest()
        for other in manifest['members'].values():
            if other['file']!=entry['file']: os.link(args.device_page/other['file'],broken/other['file'])
        (broken/'manifest.json').write_text(json.dumps(manifest))
        try: PreparedPublicPage(broken,args.material).install()
        except ValueError as error: assert '版本坐标错误' in str(error)
        else: raise AssertionError('完整u64分字越界必须拒绝')
    report={'actual_prefixes':len(reference),'phases':[r['phase'] for r in reference],
        'compact_original_observer_and_prepared_v3_equal':True,'arrival_bba_unchanged':True,
        'capture_replay_and_old_snapshot':True,'old_v2_valuation_unavailable':True,
        'new_version_member_boundary':True,'full_portfolio_NAV':False}
    args.output.write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report),flush=True)


def validate_public_blocks(args,columns,states,destination):
    """同一原Core材料跨完整组块；旧整页仅作有限独立调度对照。"""
    import shutil,resource
    from public_device_page import build_public_device_blocks,PreparedPublicPage,PreparedPublicBlocks
    from input_history import SourceHistoryInputs
    from input_100ms import MarketInputs100ms
    from model_train.continuous_inputs import ContinuousActorInputs
    from input_events import EventInputs
    from fact_window import PublicFactWindow,PublicTradeWindow
    from input_trades import TradeInputs
    from input_pm_fields import pm_fields
    from input_reference import ReferenceInputs
    from prepare import _numbers
    destination=Path(destination);destination.mkdir(exist_ok=False)
    material=destination/'original-public';shutil.copytree(args.material,material)
    if args.original_reference:shutil.copyfile(args.original_reference,destination/'original-reference.json')
    started=time.perf_counter();blocks=build_public_device_blocks(args.binary,material,destination/'blocks',groups_per_block=2)
    pm,market,reference,binance=states
    registry=SourceHistoryInputs(pm,market,reference,columns).registry
    indices=torch.tensor([0],device='cuda');start=market.times[:1,0]-30000
    expected_history=SourceHistoryInputs(pm,market,reference,columns,identity_registry=registry)
    memory=expected_history.empty(1,start);segments=[];checked=0
    for index,entry in enumerate(blocks.parts):
        page=blocks.load(index);current=page.install();cp,cm,cr,cb=current
        groups=torch.arange(entry['group_begin'],entry['group_end'],device='cuda')
        now=cp.causal_visible_at
        assets=cm.asset_pairs[:1].expand(len(groups),-1)
        af,ef=cp.frame(groups),pm.frame(groups)
        for field in ('present','initialized','ready','quality','fault','prefix_fault'):
            assert torch.equal(getattr(af,field),getattr(ef,field)),(entry,field)
        assert list(zip(af.quantity.numerator.report_values(),af.quantity.denominator.report_values()))==list(zip(
            ef.quantity.numerator.report_values(),ef.quantity.denominator.report_values()))
        a=pm_fields(cp,cp.observation(groups),assets,now);e=pm_fields(pm,pm.observation(groups),assets,now)
        for field in ('bba','trade','fault'):assert torch.equal(getattr(a,field),getattr(e,field)),(index,field)
        ar=ReferenceInputs(cr).encode(cr.frame(groups),cm.frame(groups),indices.expand(len(groups)),now)
        er=ReferenceInputs(reference).encode(reference.frame(groups),market.frame(groups),indices.expand(len(groups)),now)
        assert torch.equal(ar.values,er.values) and torch.equal(ar.fault,er.fault),(index,'reference')
        av,ev=cp.valuation_bid(groups),pm.valuation_bid(groups)
        for field in ('value_present','bid_present','quality','fault','prefix_fault'):
            assert torch.equal(getattr(av,field),getattr(ev,field)),(index,'valuation',field)
        for r in range(len(groups)):
            for target in range(len(cp.assets)):
                assert Fraction(page.columns['decimals'][int(av.price_ids[r,target])])==Fraction(columns['decimals'][int(ev.price_ids[r,target])])
        history=SourceHistoryInputs(cp,cm,cr,page.columns,identity_registry=registry)
        memory,error=history.consume(groups,now,indices,start,memory)
        assert not error.any().cpu().item()
        segments.append(MarketInputs100ms(cp,cm,cr,cb,history,None,page.columns,now))
        checked+=len(groups)
    frozen=history.freeze(memory)
    whole,error=expected_history.consume(torch.arange(len(columns['groups']),device='cuda'),pm.causal_visible_at,
        indices,start,expected_history.empty(1,start))
    assert not error.any().cpu().item()
    whole=expected_history.freeze(whole)
    assert torch.equal(frozen.history,whole.history) and torch.equal(frozen.path,whole.path)
    assert torch.equal(frozen.memory.identities,whole.memory.identities)
    # 真实尚未冻结窗口片段调用生产合并入口，不逐块投影后相加。
    owner=ContinuousActorInputs.__new__(ContinuousActorInputs);owner.encoder=segments[-1];owner.pending_inputs=segments
    events,trades=_numbers().run_exact_batch(owner._window_inputs)
    expected_events=EventInputs(PublicFactWindow(columns));expected_trades=TradeInputs(pm,PublicTradeWindow(columns))
    previous=indices-1;end=indices+len(columns['groups'])-1;now=pm.causal_visible_at[-1:]+100
    for owner in (events,expected_events):
        owner.window.fact_capacity=len(columns['facts']) or 1;owner.window.source_capacity=len(columns['fact_sources']) or 1
    for owner in (trades,expected_trades):owner.window.capacity=len(columns['raw_trades']) or 1
    ae,ee=events.encode(previous,end,now),expected_events.encode(previous,end,now)
    at,et=trades.encode(previous,end,market.asset_pairs[:1],now),expected_trades.encode(previous,end,market.asset_pairs[:1],now)
    for a,e in ((ae,ee),(at,et)):
        assert torch.equal(a.values,e.values) and torch.equal(a.fault,e.fault)
        assert torch.equal(a.required,e.required) and a.enough.all().cpu().item()
    assert len(columns['raw_trades'])>0 and at.values.abs().sum().cpu().item()>0
    try:PreparedPublicPage(blocks.directory/blocks.parts[-1]['directory'],material)
    except ValueError:pass
    else:raise AssertionError('内部末子块被完整页入口接纳')
    # 一个真实缺失成员边界；原根不能把坏块当EOF。
    path=blocks.directory/blocks.parts[-1]['directory']/'groups.bin'
    path.rename(path.with_suffix('.saved'))
    try:
        try:blocks.load(len(blocks.parts)-1)
        except (ValueError,FileNotFoundError):pass
        else:raise AssertionError('缺失组列被当作合法EOF')
    finally:path.with_suffix('.saved').rename(path)
    report={'groups':checked,'parts':len(blocks.parts),'raw_trades':len(columns['raw_trades']),
        'history_samples':frozen.memory.counts.cpu().tolist(),'global_prefix_and_previous_selected_projection':True,
        'old_twap_and_native_window_vs_original_full_page':True,'last_child_rejected_as_whole':True,
        'missing_group_member_rejected':True,'device':'cuda','seconds':time.perf_counter()-started,
        'max_rss_kib':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        'device_peak_allocated_bytes':torch.cuda.max_memory_allocated(),
        'resource_scope':'finite original Core validation plus full reference, all pending unobserved window segments; not production15days'}
    (destination/'report.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({'public_blocks':report}),flush=True)
    return report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--binary',type=Path,required=True)
    parser.add_argument('--material',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--original-reference',type=Path)
    parser.add_argument('--binance-received-day-reference',type=Path,help='原producer短接收日链，只核实际公共BN前缀')
    parser.add_argument('--valuation-reference',type=Path,help='原producer有限来源估值反例与完整版本')
    parser.add_argument('--legacy-device-page',type=Path,help='现有v1页的消费列兼容核对')
    parser.add_argument('--device-page',type=Path,help='原reader一次构建并两次直接加载固定二进制列')
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    if args.binance_received_day_reference is not None:
        validate_binance_received_day(args,root)
        return
    if args.valuation_reference is not None:
        validate_valuation(args,root)
        return
    sys.path.insert(0,str(root/'data-pipeline/gpu'))
    from public_state import PublicPmState
    sys.path.insert(0,str(root/'model-train/native-inputs/gpu'))
    from input_books import PmBookInputs
    from input_pm_fields import pm_fields
    from fact_window import PublicFactWindow, PublicTradeWindow
    from input_trades import TradeInputs
    from market_state import PublicMarketState
    from input_lifecycle import LifecycleInputs
    from input_sides import SideInputs
    from reference_state import PublicReferenceState
    from input_reference import ReferenceInputs
    from binance_state import PublicBinanceState
    from input_binance import BinanceInputs
    from input_events import EventInputs, time_envelope
    from model_train.book import BookEncoder
    from prepare import _numbers
    history_reference = None
    native_freeze_reference = None
    started = time.perf_counter()
    print(json.dumps({'stage':'strict_compact_to_pm_device_columns'}),flush=True)
    page_report=None
    prepared=None
    if args.device_page is None:
        wire = subprocess.check_output([str(args.binary),'--pm-device-columns',str(args.material),'268435456'])
        columns = json.loads(wire)
        state = PublicPmState(columns)
        market_state = PublicMarketState(state,columns)
        reference_state = PublicReferenceState(market_state,columns)
        binance_state = PublicBinanceState(state,columns)
    else:
        from public_device_page import PreparedPublicPage, build_public_device_page, PREFIXES, TABLES
        import numpy as np
        import public_state as public_module
        def page_progress(stage,**fields):
            print(json.dumps({'stage':stage,'elapsed_seconds':time.perf_counter()-started,**fields}),flush=True)
        build_started=time.perf_counter()
        built=build_public_device_page(args.binary,args.material,args.device_page,progress=page_progress)
        build_seconds=time.perf_counter()-build_started
        original_columns=built[4]
        # 第二次及后续冷安装禁止GPU数字解析/GCD入口与前缀排序/counts。
        saved=(public_module.decimal_columns,torch.argsort,torch.bincount,public_module.PrefixColumn.__init__)
        saved_ratio=vars(_numbers().ExactRational)['ratio']
        def forbidden(*a,**kw): raise AssertionError('二进制页加载不得重新解析数字或排序offset')
        loads=[]
        try:
            public_module.decimal_columns=torch.argsort=torch.bincount=public_module.PrefixColumn.__init__=forbidden
            _numbers().ExactRational.ratio=classmethod(forbidden)
            for attempt in range(2):
                load_started=time.perf_counter()
                page=PreparedPublicPage(args.device_page,args.material)
                installed=page.install()
                assert all(page.columns[name].decoded_records==0 for name in ('assets','markets','anchors','lifecycle_states'))
                assert set(page.columns)==set(original_columns)
                for name in TABLES: assert np.array_equal(page.columns[name],np.asarray(original_columns[name]).reshape(page.columns[name].shape))
                for name in ('anchor_sets','terminal_payouts'):
                    assert len(page.columns[name])==len(original_columns[name])
                    for current,original in zip(page.columns[name],original_columns[name]): assert np.array_equal(current,np.asarray(original).reshape(current.shape))
                for name in set(original_columns)-set(TABLES)-set(PREFIXES)-{'anchor_sets','terminal_payouts'}:
                    assert page.columns[name]==original_columns[name]
                for old,new in zip(built[:4],installed):
                    for name in PREFIXES:
                        if hasattr(old,name):
                            assert torch.equal(getattr(old,name).padded,getattr(new,name).padded)
                            assert torch.equal(getattr(old,name).offsets,getattr(new,name).offsets)
                for name in ('numerator','denominator'):
                    for field in ('words','sign','fault'):
                        assert torch.equal(getattr(getattr(built[0].decimals,name),field),
                            getattr(getattr(installed[0].decimals,name),field))
                torch.cuda.synchronize()
                loads.append(time.perf_counter()-load_started)
                page_progress('public_device_page_loaded',attempt=attempt+1,seconds=loads[-1])
            prepared=page
            state,market_state,reference_state,binance_state=installed
            columns=prepared.columns
        finally:
            public_module.decimal_columns,torch.argsort,torch.bincount,public_module.PrefixColumn.__init__=saved
            _numbers().ExactRational.ratio=saved_ratio
        # 一个实际身份bank损坏边界；原页不改，复制仅该成员和manifest。
        import tempfile, shutil
        args.output.parent.mkdir(parents=True,exist_ok=True)
        with tempfile.TemporaryDirectory(prefix='page-corrupt-',dir=args.output.parent) as temporary:
            broken=Path(temporary)
            shutil.copyfile(args.device_page/'manifest.json',broken/'manifest.json')
            manifest=json.loads((args.device_page/'manifest.json').read_text())
            identity_name=manifest['members']['anchors.utf8']['file']
            identity=bytearray((args.device_page/identity_name).read_bytes()); identity[-1]^=1
            (broken/identity_name).write_bytes(identity)
            # 其余成员硬链接原只读页；仅独立复制的身份成员发生损坏。
            for entry in manifest['members'].values():
                if entry['file']!=identity_name: os.link(args.device_page/entry['file'],broken/entry['file'])
            try: PreparedPublicPage(broken,args.material)
            except ValueError as error:
                assert '摘要' in str(error) and identity_name in str(error)
            else: raise AssertionError('损坏原身份成员必须拒绝')
        # 实际消费列越界：即使成员摘要随之更新，公共消费者仍拒绝非法值。
        with tempfile.TemporaryDirectory(prefix='page-rehashed-',dir=args.output.parent) as temporary:
            broken=Path(temporary)
            manifest=json.loads((args.device_page/'manifest.json').read_text())
            entry=manifest['members']['quantities.padded']; target=broken/entry['file']
            shutil.copyfile(args.device_page/entry['file'],target)
            rows=np.memmap(target,dtype='<i8',mode='r+',shape=tuple(entry['shape']))
            assert len(rows)>1
            rows[1,3]=len(original_columns['decimals']); rows.flush(); del rows
            entry['sha256']=hashlib.sha256(target.read_bytes()).hexdigest()
            (broken/'manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,separators=(',',':')))
            for other in manifest['members'].values():
                if other['file']!=entry['file']: os.link(args.device_page/other['file'],broken/other['file'])
            try: PreparedPublicPage(broken,args.material).install()
            except ValueError as error: assert '精确值引用越界' in str(error)
            else: raise AssertionError('非法精确值引用不得进入实际消费列')
        if args.legacy_device_page is not None:
            legacy=PreparedPublicPage(args.legacy_device_page,args.material)
            legacy_states=legacy.install()
            assert 'fact_records' not in legacy.columns
            # 新版本加入来源表会改变bank物理编号；核原消费者值而非要求旧编号等于新编号。
            sample=torch.tensor([-1,0,len(columns['groups'])-1],dtype=torch.int64,device='cuda')
            old_frame,new_frame=legacy_states[0].frame(sample),state.frame(sample)
            for name in ('present','initialized','ready','tick_ids','tick_present','quality','fault'):
                assert torch.equal(getattr(old_frame,name),getattr(new_frame,name)),name
            for name in ('numerator','denominator'):
                for field in ('words','sign','fault'):
                    assert torch.equal(getattr(getattr(old_frame.quantity,name),field),
                        getattr(getattr(new_frame.quantity,name),field)),(name,field)
            old_bba,new_bba=legacy_states[0].observation(sample).bba,state.observation(sample).bba
            assert torch.equal(old_bba.present,new_bba.present)
            assert torch.equal(old_bba.values[:,:,:6],new_bba.values[:,:,:6])
            assert torch.equal(old_bba.values[:,:,-1],new_bba.values[:,:,-1])
            assert not legacy_states[0].valuation_bid(sample).value_present.any().cpu().item()
            page_progress('legacy_public_original_consumption_checked_valuation_unavailable')
        wire=b''
        member_sizes=sorted(((p.name,p.stat().st_size) for p in args.device_page.iterdir()),key=lambda entry:entry[1],reverse=True)
        page_report={'build_seconds':build_seconds,'load_seconds':loads,
            'bytes':sum(p.stat().st_size for p in args.device_page.iterdir()),
            'largest_members':member_sizes[:8],
            'complete_original_columns_and_full_identity_equal':True,
            'duplicate_fact_bank_stored':False,
            'identity_json_decodes_during_two_loads':0,
            'two_loads_without_decimal_gcd_or_prefix_sort_checked':True,
            'corrupted_original_identity_member_rejected':True,
            'prepared_value_domain_rejected_by_consumer':True,
            'legacy_v1_checked':args.legacy_device_page is not None,
            'whole_material_ended_page_only':True,'continuous_page_handoff_checked':False}
        del built
    parsed = time.perf_counter()
    print(json.dumps({'stage':'shared_gpu_load','slots':len(columns['slots']),
                      'assets':len(columns['assets']),'decimals':len(columns['decimals']),
                      'cold_seconds':parsed-started}),flush=True)
    prefix_values = [-1,0,len(columns['groups'])-1] if len(columns['groups']) else [-1,-1,-1]
    prefixes = torch.tensor(prefix_values,device='cuda',dtype=torch.int64)
    frame = state.frame(prefixes)
    observation = state.observation(prefixes)
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
                         if entry[1] == columns['slots'][0][0]) if len(columns['slots']) else 0)
    original_pair = [asset for _,asset,_ in sorted(
        [entry for entry in columns['market_assets'] if entry[0] == target_market],key=lambda entry:entry[2])]
    assets = torch.tensor([original_pair if original_pair else [0,1]]*3, dtype=torch.int64, device='cuda')
    now = torch.tensor([columns['groups'][max(0,int(prefix))][2] if len(columns['groups']) else 0 for prefix in prefix_values],
                       dtype=torch.int64,device='cuda')
    if args.original_reference is not None:
        now[-1] = json.loads(args.original_reference.read_text())['observation_ms']
    packer = PmBookInputs(state, initial_capacity=1)
    packed = resource_retry(lambda:packer.pack(frame,assets,now),[(packer,'capacity')])
    native_fields = pm_fields(state,observation,assets,now)
    market_indices = torch.full((3,),target_market,dtype=torch.int64,device='cuda')
    native_lifecycle = LifecycleInputs(market_state).encode(market_frame,market_indices,assets,now)
    side_inputs = SideInputs(state)
    native_sides = side_inputs.encode(frame,assets,now)
    binance_inputs = BinanceInputs(binance_state,initial_capacity=1)
    binance_frame = binance_state.frame(prefixes)
    symbols = torch.zeros_like(prefixes)
    def encode_binance(*inputs):
        return resource_retry(lambda:binance_inputs.encode(*inputs),[(binance_inputs.book,'capacity')],metadata=lambda value:value.packed)
    native_binance = encode_binance(binance_frame,binance_state.observation(prefixes),symbols,now)
    assert not native_binance.fault.any().cpu().item()
    native_reference = ReferenceInputs(reference_state).encode(reference_state.frame(prefixes),market_frame,market_indices,now)
    if columns['reference_preparation_complete'] and columns['markets']:
        assert not native_reference.fault.any().cpu().item()
    else:
        assert native_reference.fault.ne(0).all().cpu().item()
    assert not native_lifecycle.identity_present[0].cpu().item()
    if columns['markets']:
        assert not native_lifecycle.fault.any().cpu().item()
    else:
        assert native_lifecycle.fault.ne(0).all().cpu().item()
    torch.cuda.reset_peak_memory_stats()  # 正常窗口阶段，含已驻留公共列/盘口/直接字段。
    fact_windows = PublicFactWindow(columns,initial_capacity=1)
    events = EventInputs(fact_windows)
    def encode_events(*inputs):
        return resource_retry(lambda:events.encode(*inputs),[(fact_windows,'fact_capacity'),(fact_windows,'source_capacity')])
    previous = prefixes.new_tensor([-1,-1,max(-1,prefix_values[-1]-1)])
    native_events = encode_events(previous,prefixes,now)
    trades = TradeInputs(state,PublicTradeWindow(columns,initial_capacity=1))
    def encode_trades(*inputs):
        return resource_retry(lambda:trades.encode(*inputs),[(trades.window,'capacity')])
    native_window_trades = encode_trades(previous,prefixes,assets,now)
    if len(columns['assets']) >= 2:
        assert not native_window_trades.fault.any().cpu().item()
    empty_trades = encode_trades(prefixes,prefixes,assets,now)
    assert torch.count_nonzero(empty_trades.values) == 0
    assert not native_events.fault.any().cpu().item()
    empty_window = encode_events(prefixes,prefixes,now)
    assert torch.count_nonzero(empty_window.values) == 0
    capacities_before_invalid = (fact_windows.fact_capacity,fact_windows.source_capacity)
    invalid_window = encode_events(prefixes.new_tensor([-1]),prefixes.new_tensor([len(columns['groups'])]),now[:1])
    assert invalid_window.fault.ne(0).all().cpu().item()
    assert torch.count_nonzero(invalid_window.values) == 0
    assert capacities_before_invalid == (fact_windows.fact_capacity,fact_windows.source_capacity)
    trade_capacity_before_invalid = trades.window.capacity
    invalid_trade_window = encode_trades(prefixes.new_tensor([-1]),prefixes.new_tensor([len(columns['groups'])]),assets[:1],now[:1])
    assert invalid_trade_window.fault.ne(0).all().cpu().item()
    assert torch.count_nonzero(invalid_trade_window.values) == 0
    assert trades.window.capacity == trade_capacity_before_invalid
    # 真正UTC 100ms网格的末10个窗口，GPU选择合法闭组前缀，区间不按source time筛选。
    if len(columns['groups']):
        last_grid = (columns['groups'][-1][2]//100)*100
        grid = torch.arange(last_grid-900,last_grid+1,100,dtype=torch.int64,device='cuda')
        end_prefix = state.prefix_at(grid)
        begin_prefix = state.prefix_at(grid-100)
        grid_events = encode_events(begin_prefix,end_prefix,grid)
        assert not grid_events.fault.any().cpu().item()
    torch.cuda.synchronize()
    normal_window_peak = torch.cuda.max_memory_allocated()

    positive_rows = int(packed.mask.sum().cpu())
    if len(columns['slots']):
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
    combined_capacity = max(packed.levels.shape[2],native_binance.packed.levels.shape[2])
    levels = torch.cat((torch.nn.functional.pad(packed.levels,(0,0,0,combined_capacity-packed.levels.shape[2])),
        torch.nn.functional.pad(native_binance.packed.levels,(0,0,0,combined_capacity-native_binance.packed.levels.shape[2]))),dim=1).detach().requires_grad_(True)
    mask = torch.cat((torch.nn.functional.pad(packed.mask,(0,combined_capacity-packed.mask.shape[2])),
                     torch.nn.functional.pad(native_binance.packed.mask,(0,combined_capacity-native_binance.packed.mask.shape[2]))),dim=1)
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
                key = payload['ReferenceFact']['reference_type']
                if key=='chainlink_btc_usd_twap':
                    return (13,3) if payload['ReferenceFact']['reference']['window_s']==30 else (14,4)
                return {'chainlink_btc_usd':(12,2),'binance_aggregate_trade':(18,7),'binance_depth_state':(17,6),'market_settlement_reference_update':(16,5),'market_price_anchor_update':(15,5)}[key]
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
        full_window = encode_events(prefixes.new_tensor([-1]),prefixes[-1:],now[-1:])
        assert torch.equal(full_window.values[0],torch.tensor(expected,device='cuda',dtype=torch.float32)), (full_window.values,expected)
        assert not full_window.fault.any().cpu().item()
        # 原canonical PublicTrade缺量/未知方向仍计入整窗，不被public latest trade抑制。
        full_trades = encode_trades(prefixes.new_tensor([-1]),prefixes[-1:],assets[-1:],now[-1:])
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
        binance_rows = [event for event in reference['public_events'] if 'ReferenceFact' in event['payload']
                        and event['payload']['ReferenceFact']['reference_type']=='binance_aggregate_trade']
        assert len(binance_rows)==2
        binance = [event['payload']['ReferenceFact']['reference'] for event in binance_rows]
        ps = [Fraction(t['price_usdt_per_btc']) for t in binance]
        qty = [Fraction(t['quantity_btc']) for t in binance]
        out = [0.0]*21
        out[0] = original_log(len(binance))
        out[1:6] = [original_log(p) for p in (ps[0],ps[-1],min(ps),max(ps),sum(ps)/len(ps))]
        out[6:8] = [original_log(sum(qty)),original_log(sum(p*q for p,q in zip(ps,qty)))]
        for side,maker in enumerate((False,True)):
            out[8+side] = original_log(sum(t['buyer_is_maker']==maker for t in binance))
            out[10+side] = original_log(sum(q for q,t in zip(qty,binance) if t['buyer_is_maker']==maker))
        ages = [[],[],[]]
        for event in binance_rows:
            s,r = event['direct_time']['source_event_at_utc_ms'],event['direct_time']['received_at_utc_ms']
            ages[1].append(reference['observation_ms']-r)
            if s is not None: ages[0].append(reference['observation_ms']-s); ages[2].append(r-s)
        for k,age in enumerate(ages):
            if age:
                out[12+2*k] = original_log(Fraction(min(age),1000))
                out[13+2*k] = original_log(Fraction(max(age),1000))
            out[18+k] = len(age)/len(binance)
        expected_trades[56:] = out
        assert torch.equal(full_trades.values[0],torch.tensor(expected_trades,device='cuda',dtype=torch.float32)), (full_trades.values,expected_trades)
        assert not full_trades.fault.any().cpu().item()
        quality_names = ['fresh','stale','missing','unknown_not_observed','confirmed_gap','invalid']
        # 原合法终态配对的非空赢家/payout，以及随后原冲突入口的清值。
        terminal_rows = reference['terminal_observations']
        terminal_prefixes = prefixes.new_tensor([row['prefix'] for row in terminal_rows])
        terminal_native = LifecycleInputs(market_state).encode(market_state.frame(terminal_prefixes),
            torch.full_like(terminal_prefixes,target_market),assets[:1].expand(len(terminal_rows),-1),
            torch.full_like(terminal_prefixes,reference['observation_ms']))
        assert not terminal_native.fault.any().cpu().item()
        saw_resolved = saw_invalid = False
        for index,row in enumerate(terminal_rows):
            field = row['terminal']
            assert terminal_native.values[index,63+quality_names.index(field['quality'])].cpu().item() == 1
            if field['value'] is not None:
                saw_resolved = True
                assert terminal_native.values[index,69:72].cpu().tolist() == [1.0,1.0,0.0]
                assert terminal_native.values[index,[81,82,92,93]].cpu().tolist() == [1.0,1.0,-1.0,1.0]
            elif field['quality'] == 'invalid':
                saw_invalid = True
                assert torch.count_nonzero(terminal_native.values[index,69:72]) == 0
                assert torch.count_nonzero(terminal_native.values[index,[81,82,92,93]]) == 0
                assert terminal_native.values[index,78].cpu().item() == 1  # 原terminal锚点仍保留。
        assert saw_resolved and saw_invalid
        def signed_log(value):
            value = float(value)
            return math.copysign(math.log1p(abs(value)),value) if value else 0.0
        # 临时参考是原准备器选中的第一条，不能以全局最新TWAP替代。
        selected_reference = reference['public_market']
        temporary = selected_reference['temporary_reference']
        assert Fraction(temporary['value']['price_usd_per_btc']) == 65000
        assert Fraction(selected_reference['chainlink_twap_thirty']['value']['price_usd_per_btc']) == 66000
        expected_reference = [0.0]*37
        expected_reference[1] = expected_reference[4] = expected_reference[6] = 1.0
        expected_reference[7] = signed_log(65000)
        def original_t5(anchor):
            if anchor is None: return [0.0]*5
            s,r = anchor['source_event_at_utc_ms'],anchor['received_at_utc_ms']
            return [signed_log(Fraction(reference['observation_ms']-s,1000)) if s is not None else 0.0,
                    signed_log(Fraction(reference['observation_ms']-r,1000)),
                    signed_log(Fraction(r-s,1000)) if s is not None else 0.0,float(s is not None),1.0]
        expected_reference[8:13] = original_t5(temporary['direct_anchor'])
        expected_reference[13] = signed_log(30)
        for index,name in enumerate(('chainlink_spot','chainlink_twap_thirty','chainlink_twap_sixty')):
            field = selected_reference[name]
            visible = field['value'] is not None and field['quality'] in ('fresh','stale')
            start = 14+7*index
            if visible:
                expected_reference[start:start+2] = [signed_log(Fraction(field['value']['price_usd_per_btc'])),1.0]
            # 原unique直接锚点规则；不以时间相同合并不同证据。
            expected_reference[start+2:start+7] = original_t5(field['direct_anchors'][0] if len(field['direct_anchors'])==1 else None)
        expected_reference[35:37] = [signed_log(1000),1.0]
        assert torch.equal(native_reference.values[2],torch.tensor(expected_reference,device='cuda',dtype=torch.float32)), (native_reference.values[2],expected_reference)
        from input_history import SourceHistoryInputs
        source_history = SourceHistoryInputs(state,market_state,reference_state,columns)
        start_at = prefixes.new_tensor([columns['markets'][target_market]['window']['start_at_utc_ms']-30000])
        memory = source_history.empty(1,start_at)
        # 原完整组交付时钟，零额外delay短工程配置；不以100ms格点替代。
        count = len(columns['groups'])
        for start in range(0,count,7):
            batch = torch.arange(start,min(count,start+7),device='cuda',dtype=torch.int64)
            memory,error = source_history.consume(batch,state.causal_visible_at[batch],market_indices[-1:],start_at,memory)
            assert not error.any().cpu().item()
        frozen = source_history.freeze(memory)
        assert not frozen.fault.any().cpu().item()
        series = [[] for _ in range(6)]
        for row in reference['source_observations']:
            original = row['observation']
            at = max(reference['public_visible_times'][:row['prefix']+1])
            window = original['market']['window']
            if not (window['start_at_utc_ms']-30000 <= at < window['end_at_utc_ms']): continue
            fields = [None]*6
            for asset in original['assets']:
                outcome = asset['asset']['outcome']['index']
                if outcome < 2: fields[outcome] = asset['best_bid_ask']
            fields[3:] = [original[name] for name in ('chainlink_spot','chainlink_twap_thirty','chainlink_twap_sixty')]
            for route,field in enumerate(fields):
                if field is None or field['value'] is None or field['quality'] not in ('fresh','stale'): continue
                path = 'best_bid_ask' if route<2 else ('chainlink_spot','chainlink_twap_thirty','chainlink_twap_sixty')[route-3]
                identity = [anchor for anchor in field['direct_anchors'] if anchor['field_path']==path]
                if not identity or (series[route] and series[route][-1][1]==identity): continue
                if route<2:
                    bid,ask = field['value']['best_bid_pusd_per_share'],field['value']['best_ask_pusd_per_share']
                    if bid is None or ask is None: continue
                    value = (Fraction(bid)+Fraction(ask))/2
                else: value = Fraction(field['value']['price_usd_per_btc'])
                series[route].append((value,identity,at))
        expected_history,expected_path = [],[]
        for route,samples in enumerate(series):
            tail = samples[-16:]
            history = [len(tail)/16,0.0,0.0,0.0]
            if len(tail)>1:
                squared = sum((b[0]-a[0])**2 for a,b in zip(tail,tail[1:]))/ (len(tail)-1)
                history[1:] = [signed_log(tail[-1][0]-tail[-2][0]),math.log1p(math.sqrt(float(squared))),
                               signed_log(Fraction(tail[-1][2]-tail[0][2],1000))]
            expected_history.extend(history)
            if route==2: continue
            path = [signed_log(len(samples)),0.0,0.0,0.0,0.0,0.0]
            if samples:
                values = [row[0] for row in samples]
                transform = (lambda v:float(2*v-1)) if route<2 else signed_log
                path[1:5] = [transform(v) for v in (values[0],values[-1],min(values),max(values))]
                if len(values)>1:
                    squared = sum((b-a)**2 for a,b in zip(values,values[1:]))/(len(values)-1)
                    path[5] = math.log1p(math.sqrt(float(squared)))
            expected_path.extend(path)
        assert len(series[0])>16  # 路径不可错误地只遍历最后16条。
        assert torch.equal(frozen.history[0],torch.tensor(expected_history,device='cuda',dtype=torch.float32)), (frozen.history,expected_history)
        assert torch.equal(frozen.path[0],torch.tensor(expected_path,device='cuda',dtype=torch.float32)), (frozen.path,expected_path)
        again = source_history.freeze(frozen.memory)
        assert torch.equal(again.history,frozen.history) and not again.path.any().cpu().item()
        # 同一已同步来源按不同批次划分接纳，数值与身份/时间必须相同。
        whole,error = source_history.consume(torch.arange(count,device='cuda',dtype=torch.int64),state.causal_visible_at,
            market_indices[-1:],start_at,source_history.empty(1,start_at))
        whole = source_history.freeze(whole)
        assert torch.equal(whole.history,frozen.history) and torch.equal(whole.path,frozen.path)
        assert torch.equal(whole.memory.identities,frozen.memory.identities)
        ineligible,error = source_history.consume(torch.arange(count,device='cuda',dtype=torch.int64),state.causal_visible_at,
            market_indices[-1:],start_at+1,source_history.empty(1,start_at+1))
        assert not error.any().cpu().item()
        ineligible = source_history.freeze(ineligible)
        assert not ineligible.history.any().cpu().item() and not ineligible.path.any().cpu().item()
        history_reference = {'accepted_samples':[len(samples) for samples in series],
            'retained_samples':frozen.memory.counts.cpu().tolist()[0],
            'group_batch_rows':7,'path_reset_keeps_history':True,'t_minus_30_ineligible_remains_empty':True}

        rule_binary = os.environ.get('B_GPU_RULE_CONTEXT_BINARY')
        if rule_binary:
            sys.path.insert(0,str(root/'sim-system/gpu'))
            from rules import PublicRuleGpu
            from input_rules import RuleInputs
            from input_100ms import MarketInputs100ms,MarketFreezeMemory
            from validate_freeze_resources import freeze_with_resources,validate_freeze_replay
            snapshot = root/'sim-system/tests/fixtures/platform-rule-snapshot-v3-gate-b.json'
            request = args.output.parent/'historical-rule-request.json'
            request.write_text(json.dumps({'snapshot_path':str(snapshot),'expected_file_sha256':hashlib.sha256(snapshot.read_bytes()).hexdigest(),
                'replay_causal_start_utc_ms':2**63-1,'market':columns['markets'][target_market]}))
            context = json.loads(subprocess.check_output([rule_binary,str(request)]))
            process_rules = PublicRuleGpu(context)
            direct_rule = RuleInputs(state,process_rules).encode(observation,assets,now)
            expected_rule = [0.0]*30
            for outcome,asset in enumerate(sorted(reference['public_assets'],key=lambda a:a['asset']['outcome']['index'])):
                field = asset['tick'];visible = field['value'] is not None and field['quality'] in ('fresh','stale')
                start = 7*outcome
                if visible: expected_rule[start:start+2] = [signed_log(Fraction(field['value'])),1.0]
                anchors = [a for a in field['direct_anchors'] if a['field_path']=='tick']
                expected_rule[start+2:start+7] = original_t5(anchors[0] if len(anchors)==1 else None)
            for slot,value in ((14,Fraction(context['limit_min_quantity_shares'])),(16,Fraction(context['market_buy_min_spend_pusd'])),
                               (20,Fraction(context['taker_delay_ms'],1000)),(24,Fraction(context['fee_rate'])),(26,context['fee_exponent'])):
                expected_rule[slot:slot+2] = [signed_log(value),1.0]
            for slot,key in ((18,'taker_delay_enabled'),(22,'fees_enabled'),(28,'fee_taker_only')):
                expected_rule[slot:slot+2] = [float(context[key]),1.0]
            assert torch.equal(direct_rule.values[2],torch.tensor(expected_rule,device='cuda',dtype=torch.float32)), (direct_rule.values[2],expected_rule)
            encoder = MarketInputs100ms(state,market_state,reference_state,binance_state,source_history,process_rules,columns,state.causal_visible_at)
            # 仅重建有界Native字段起始边界；不是模型热身或活动账户恢复。
            clocks = [];frontier = -(2**63)
            for value in reference['public_visible_times']:
                frontier = max(frontier,value);clocks.append(frontier)
            before = 19800;through = bisect.bisect_right(clocks,before)
            continuous = source_history.empty(1,start_at)
            if through:
                batch = torch.arange(through,device='cuda',dtype=torch.int64)
                continuous,error = source_history.consume(batch,state.causal_visible_at[batch],market_indices[-1:],start_at,continuous)
                assert not error.any().cpu().item()
            continuous = source_history.freeze(continuous).memory
            endpoints = MarketFreezeMemory(prefixes.new_tensor([through-1]),prefixes.new_tensor([before]),None,None)
            saved = None;roles = set();native_freezes = 0;capture_inputs=[]
            for grid in range(19900,26300,100):
                end = bisect.bisect_right(clocks,grid)
                if end>through:
                    batch = torch.arange(through,end,device='cuda',dtype=torch.int64)
                    continuous,error = source_history.consume(batch,state.causal_visible_at[batch],market_indices[-1:],start_at,continuous)
                    assert not error.any().cpu().item()
                if grid in (24000,26100):
                    capture_inputs.append((prefixes.new_tensor([grid]),market_indices[-1:].clone(),start_at.clone(),continuous,endpoints))
                frozen_market,_,_ = freeze_with_resources(encoder,
                    (prefixes.new_tensor([grid]),market_indices[-1:],start_at,continuous,endpoints))
                assert not frozen_market.fault.any().cpu().item()
                assert frozen_market.fast.shape==(1,626) and frozen_market.book_levels.shape[1]==6
                assert torch.equal(frozen_market.fast[:,566:596],frozen_market.rule)
                roles.add(int(frozen_market.role.cpu()[0]))
                if saved is not None: assert torch.equal(saved[0].fast,saved[1])
                if grid==24000:
                    # received=24000不能提前接纳；原完整组关闭依赖为24001。
                    assert clocks[4]==24001 and frozen_market.prefixes.cpu().tolist()==[3]
                    assert frozen_market.fast[0,37+6+3].cpu().item()==0.0
                    assert frozen_market.fast[0,37+6+4].cpu().item()==1.0
                if grid==24100:
                    # 下一合法UTC网格才从原完整快照12接纳为13。
                    assert frozen_market.prefixes.cpu().tolist()==[4]
                    assert torch.equal(frozen_market.fast[0,37+6+3],torch.tensor(signed_log(1),device='cuda',dtype=torch.float32)), {
                        'prefix':frozen_market.prefixes.cpu().tolist(),'up_bid16':frozen_market.fast[0,43:59].cpu().tolist(),
                        'before_total':endpoints.pm_sides.total.report_values(),'after_total':frozen_market.endpoint_memory.pm_sides.total.report_values()}
                    assert frozen_market.fast[0,37+6+4].cpu().item()==1.0
                    saved = (frozen_market,frozen_market.fast.clone())
                # 原core样本按实际完整组cutoff划入100ms窗，不按received划窗。
                for route,samples in enumerate(series):
                    count_before_grid = sum(at<=grid for _,_,at in samples)
                    expected_retained = min(16,count_before_grid)/16
                    assert frozen_market.fast[0,542+4*route].cpu().item()==expected_retained
                    if route==2: continue
                    path_route = route if route<2 else route-1
                    window_samples = sum(grid-100<at<=grid for _,_,at in samples)
                    expected_count = torch.tensor(signed_log(window_samples),device='cuda',dtype=torch.float32)
                    assert torch.equal(frozen_market.fast[0,596+6*path_route],expected_count), (grid,route,window_samples)
                if grid==26100:
                    # 首个BBA的关闭cutoff26001与随后20条均在此窗，共21条。
                    assert sum(26000<at<=26100 for _,_,at in series[0])==21
                    assert frozen_market.fast[0,542].cpu().item()==1.0
                continuous,endpoints = frozen_market.history_memory,frozen_market.endpoint_memory
                through=end;native_freezes+=1
                if native_freezes%8==0: print(json.dumps({'stage':'native_100ms_freeze','ticks':native_freezes,'observation_ms':grid}),flush=True)
            assert roles=={0,1}
            replay_report=validate_freeze_replay(encoder,*capture_inputs)
            native_freeze_reference = {'ticks':native_freezes,'first_observation_ms':19900,'last_observation_ms':26200,
                'context_only_and_decision_required':True,'frozen_fast626_immutable':True,
                'received_24000_closed_24001_frozen_at_24100':True,
                'capacity_and_freeze_replay':replay_report,
                'rule_snapshot':'historical_test_fixture_not_formal_training','account_policy_execution':False}
        depth_rows = reference['binance_observations']
        saw_gap = saw_missing = False
        for row in depth_rows:
            prefix = prefixes.new_tensor([row['prefix']])
            current = binance_state.frame(prefix)
            native = encode_binance(current,binance_state.observation(prefix),symbols[:1],now[:1])
            original = row['depth']
            q = quality_names.index(original['quality'])
            assert native.side.values[0,0,q].cpu().item() == 1
            visible = original['value'] is not None and original['quality'] in ('fresh','stale')
            assert current.ready[0,0].cpu().item() == visible
            assert not native.fault.any().cpu().item()
            if not visible:
                assert not native.packed.mask.any().cpu().item()
                assert not native.side.values[0,0,6:].any().cpu().item()
                saw_gap |= original['quality'] == 'confirmed_gap'
                saw_missing |= original['quality'] == 'missing'
            else:
                for side,name in enumerate(('bids','asks')):
                    levels = original['value'][name]
                    expected = [[signed_log(Fraction(level['price_usdt_per_btc'])),
                        signed_log(Fraction(level['quantity_btc'])),1.0,*([0.0]*5),
                        signed_log(Fraction(levels[0]['price_usdt_per_btc'])-Fraction(level['price_usdt_per_btc'])
                            if side==0 else Fraction(level['price_usdt_per_btc'])-Fraction(levels[0]['price_usdt_per_btc']))]
                        for level in levels]
                    actual = native.packed.levels[0,side][native.packed.mask[0,side]]
                    assert torch.equal(actual,torch.tensor(expected,device='cuda',dtype=torch.float32)), (actual,expected)
        assert saw_gap and saw_missing
        # 同一旧载荷跨拒绝边界保持精确；不是只把错误更新遮在mask后。
        for before,after in ((1,2),(3,4)):
            a = binance_state.frame(prefixes.new_tensor([depth_rows[before]['prefix']])).quantity.project()[0]
            b = binance_state.frame(prefixes.new_tensor([depth_rows[after]['prefix']])).quantity.project()[0]
            assert torch.equal(a,b)
        original_binance = selected_reference['binance_depth']
        original_first = reference['first_binance']
        first_binance = encode_binance(binance_state.frame(prefixes.new_tensor([original_first['prefix']])),
            binance_state.observation(prefixes.new_tensor([original_first['prefix']])),symbols[:1],now[:1])
        continued_binance = encode_binance(binance_state.frame(prefixes[-1:]),binance_state.observation(prefixes[-1:]),
            symbols[-1:],now[-1:],first_binance.side.memory)
        expected_depth = [0.0]*38;expected_depth[0]=1.0
        for side,name in enumerate(('bids','asks')):
            rows = original_binance['value'][name]
            old = original_first['depth']['value'][name]
            total = sum(Fraction(row['quantity_btc']) for row in rows)
            before = sum(Fraction(row['quantity_btc']) for row in old)
            out = [1.0,signed_log(len(rows)),signed_log(total),signed_log(total-before),1.0]+[0.0]*9
            out += [signed_log(Fraction(rows[0]['price_usdt_per_btc'])-Fraction(old[0]['price_usdt_per_btc'])),1.0]
            expected_depth[6+16*side:22+16*side] = out
            expected_book = [[signed_log(Fraction(row['price_usdt_per_btc'])),signed_log(Fraction(row['quantity_btc'])),1.0,
                              *([0.0]*5),signed_log((Fraction(rows[0]['price_usdt_per_btc'])-Fraction(row['price_usdt_per_btc']))
                              if side==0 else Fraction(row['price_usdt_per_btc'])-Fraction(rows[0]['price_usdt_per_btc']))] for row in rows]
            actual_book = continued_binance.packed.levels[0,side][continued_binance.packed.mask[0,side]]
            assert torch.equal(actual_book,torch.tensor(expected_book,dtype=torch.float32,device='cuda')), (actual_book,expected_book)
        assert torch.equal(continued_binance.side.values[0,0],torch.tensor(expected_depth,dtype=torch.float32,device='cuda'))
        original_trade = selected_reference['binance_trade']
        expected_trade = [0.0]*15;expected_trade[0]=expected_trade[6]=1.0
        expected_trade[7:10] = [signed_log(Fraction(original_trade['value']['price_usdt_per_btc'])),
                              signed_log(Fraction(original_trade['value']['quantity_btc'])),float(original_trade['value']['buyer_is_maker'])]
        expected_trade[10:15] = original_t5(original_trade['direct_anchors'][0])
        assert torch.equal(continued_binance.trade[0],torch.tensor(expected_trade,dtype=torch.float32,device='cuda'))
        assert not continued_binance.fault.any().cpu().item()
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
        first = reference['first_sides']
        first_frame = state.frame(prefixes.new_tensor([first['prefix']]))
        first_encoded = side_inputs.encode(first_frame,assets[:1],now[:1].new_tensor([22000]))
        continued_sides = side_inputs.encode(state.frame(prefixes[-1:]),assets[-1:],now[-1:],first_encoded.memory)
        assert not continued_sides.fault.any().cpu().item()
        for outcome,asset_id in enumerate(original_pair):
            token = columns['assets'][asset_id]['token_id']
            current = next(a for a in reference['public_assets'] if a['asset']['token_id']==token)
            before = next(a for a in first['assets'] if a['asset']['token_id']==token)
            expected_sides = [0.0]*38
            expected_sides[quality_names.index(current['bids']['quality'])] = 1.0
            for side,name in enumerate(('bids','asks')):
                field,old = current[name],before[name]
                out = [0.0]*16
                out[5:14] = original_envelope({**field,'direct_anchors':[a for a in field['direct_anchors'] if a['field_path'].startswith(f'assets[{token}].{name}[')]})
                if field['value'] is not None and field['quality'] in ('fresh','stale'):
                    levels = [row for row in field['value'] if Fraction(row['quantity_shares'])>0]
                    total = sum(Fraction(row['quantity_shares']) for row in levels)
                    out[:3] = [1.0,original_log(len(levels)),original_log(total)]
                    if old['value'] is not None and old['quality'] in ('fresh','stale'):
                        old_levels = [row for row in old['value'] if Fraction(row['quantity_shares'])>0]
                        out[3:5] = [original_log(total-sum(Fraction(row['quantity_shares']) for row in old_levels)),1.0]
                        if levels and old_levels:
                            out[14:16] = [original_log(Fraction(levels[0]['price_pusd_per_share'])-Fraction(old_levels[0]['price_pusd_per_share'])),1.0]
                expected_sides[6+16*side:22+16*side] = out
            assert torch.equal(continued_sides.values[0,outcome],torch.tensor(expected_sides,device='cuda',dtype=torch.float32)), (continued_sides.values[0,outcome],expected_sides)
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
              'native_window_trades77_exact_aggregation','public_market_identity_prefix','native_lifecycle103','native_pm_side38_endpoint_memory','binance_source_prefix_state','native_binance38_book9_trade15']
    if len(columns['slots']):
        passed += ['exact_price_sort_current_positive_compaction','native_pm_nine_fields',
                   'real_book_encoder_forward_backward','tail_padding_pairwise_tree','native_pm_bba28_trade18']
    else:
        passed += ['legal_missing_book','metadata_identity_preserved_or_unknown_coordinate_fault']
    if columns['reference_preparation_complete'] and columns['markets']:
        passed.append('native_reference37_original_preparation')
    else:
        passed.append('missing_reference_preparation_retains_fault')
    if native_freeze_reference is not None:
        passed.append('native_fast626_rule30_freeze_original_closed_group_clock')
    blocks_report=None
    if os.environ.get('B_GPU_PUBLIC_BLOCK_VALIDATION_DIR'):
        blocks_report=validate_public_blocks(args,columns,(state,market_state,reference_state,binance_state),
            os.environ['B_GPU_PUBLIC_BLOCK_VALIDATION_DIR'])
    report = {'source_commit':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),
              'identity':('engineering_real_public_pm_book_inputs_only' if len(columns['slots'])
                          else 'engineering_missing_book_boundary'),'device':torch.cuda.get_device_name(),
              'groups':len(columns['groups']),'original_pm_steps':columns['original_pm_steps'],
              'assets':len(columns['assets']),'slots':len(columns['slots']),
              'decimal_dictionary':len(columns['decimals']),'derived_column_wire_bytes':len(wire),
              'public_device_page':page_report,'public_blocks':blocks_report,
              'encoded_positive_rows':positive_rows,'raw_trade_facts':len(columns['raw_trades']),'public_facts':len(columns['facts']),
              'binance_positive_rows':int(native_binance.packed.mask.sum().cpu()),
              'direct_sources':len(columns['fact_sources']),
              'reference_preparation_complete':columns['reference_preparation_complete'],
              'cold_seconds':parsed-started,'total_seconds':time.perf_counter()-started,
              'peak_allocated_bytes':torch.cuda.max_memory_allocated(),
              'normal_window_peak_allocated_bytes':normal_window_peak,
              'window_peak_scope':'驻留共享公共列、累计可见时间、PM及Binance盘口打包和直接字段、三前缀与末10个100ms窗口；在BookEncoder创建前采样，非法窗不扩池',
              'peak_scope':('本组件全部检查峰，含六侧BookEncoder前向反向、尾补齐双前向'
                    + ('及本次原短链Native冻结' if native_freeze_reference is not None else '；原短链Native冻结参考未运行')
                    + '；不是完整模型账户或学习工作集'),
              'passed':passed,
              'original_core_reference_checked':args.original_reference is not None,
              'history_original_reference':history_reference,
              'native_100ms_freeze_reference':native_freeze_reference,
              'not_delivered':(['reference_preparation_for_this_material'] if not columns['reference_preparation_complete'] else [])+['hidden','sim_noe_ledger',
                               'whole_root_100ms_scheduler_cross_block','full_actor','ppo','active_root_restore']}
    if prepared is not None:
        report['public_device_page']['cold_identity_records_decoded_by_existing_consumers']={
            name:prepared.columns[name].decoded_records for name in ('assets','markets','anchors','lifecycle_states')}
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps(report,ensure_ascii=False),flush=True)


if __name__ == '__main__': main()
