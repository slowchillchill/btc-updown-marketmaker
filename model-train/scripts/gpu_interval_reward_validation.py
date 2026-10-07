#!/usr/bin/env python3
"""复用原Native合法场景冷来源，再核参数化区间数学；不是奖励采集宿主。"""
import argparse
from dataclasses import replace
from fractions import Fraction
from pathlib import Path
import json
import sys
import time
import torch

ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/p) for p in ('sim-system/gpu','execution-normalization/gpu','ledger/gpu',
    'data-pipeline/gpu','shared-run/gpu','model-train/native-inputs/gpu')]
from queues import Numbers,ExactRational
from validate_base_execution import rational,values
from validate_portfolio_nav import cold_ledger,amount
from public_device_page import PreparedPublicPage
from portfolio_economics import read_portfolio_economics
from exposure import read_portfolio_exposure
from success_pool import SuccessPools
from market_economics import UnresolvedMarkets
from resolution import MarketResolutions
from model_train.interval_reward import RewardIntervalMemory
from model_train.dt_advantages import dt_advantages


def exact(value):return [Fraction(n,d) for n,d in zip(values(value.numerator),values(value.denominator))]
def saved(memory):
    return {name:(value.clone() if isinstance(value,torch.Tensor) else tuple(x.clone() for x in
        (value.numerator.words,value.numerator.sign,value.numerator.fault,
         value.denominator.words,value.denominator.sign,value.denominator.fault)))
        for name,value in ((name,getattr(memory,name)) for name in memory.__dataclass_fields__)}


def unchanged(memory,original):
    for name,old in original.items():
        value=getattr(memory,name)
        current=(value,) if isinstance(value,torch.Tensor) else (value.numerator.words,value.numerator.sign,
            value.numerator.fault,value.denominator.words,value.denominator.sign,value.denominator.fault)
        expected=(old,) if isinstance(old,torch.Tensor) else old
        assert all(torch.equal(a,b) for a,b in zip(current,expected)),name


def original_exposure(checkpoint):
    """CPU市场数组按原assets存储顺序；先各市场abs再合，不用GPU金额定义expected。"""
    differences=[]
    for market in checkpoint['economics']['markets']:
        # 这两个已交原场景没有判定/兑付，原orphan_shares正是确认两腿差。
        assert not checkpoint['resolutions']
        sides={asset['outcome']['index']:Fraction(quantity) for asset,quantity in
            zip(market['assets'],market['orphan_shares'])}
        differences.append(sides[0]-sides[1])
    return sum(map(abs,differences),Fraction()),differences


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--source-root',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args();started=time.perf_counter()
    yes=torch.ones(1,dtype=torch.bool,device='cuda');no=~yes
    t=lambda value:torch.tensor([value],dtype=torch.int64,device='cuda')
    run=Numbers.run_exact_batch;cases=[];reads=[];directories=[]
    history={name:yes for name in ('economic_prefix_known','success_prefix_known','settlement_prefix_known',
        'rebate_prefix_known','economic_liability_prefix_known')}
    for scene in ('all-market-orphans','unrelated-missing-without-exposure'):
        print(json.dumps({'stage':'original_exposure_start','scene':scene,'elapsed_seconds':time.perf_counter()-started}),flush=True)
        directory=args.source_root/scene
        reference=directory/'original-account-reference.json'
        source=json.loads(reference.read_text())
        (args.output.parent/(scene+'-original-reference.json')).write_bytes(reference.read_bytes())
        prepared=PreparedPublicPage(directory/'public-v3-page',Path(source['compact']))
        pm,_,_,_=prepared.install();checkpoint=source['checkpoints'][0]
        ledger=cold_ledger(checkpoint,pm)
        exposure=run(lambda:read_portfolio_exposure(ledger,success_prefix_known=yes,settlement_prefix_known=yes))
        portfolio=run(lambda:read_portfolio_economics(ledger,**history))
        expected,differences=original_exposure(checkpoint)
        assert exposure.known.cpu().tolist()==[True] and exposure.fault.cpu().tolist()==[0]
        assert exact(exposure.amount)==[expected]
        assert portfolio.known[0,0].cpu().item() and portfolio.fault.cpu().tolist()==[0]
        assert exact(portfolio.amounts[0])==[amount(checkpoint['economics']['worst_case_maker_nav_pusd'])]
        if scene=='all-market-orphans':directories.append((ledger,pm))
        missing=run(lambda:read_portfolio_exposure(ledger,success_prefix_known=no,settlement_prefix_known=yes))
        assert not missing.known.any().cpu().item() and missing.fault.cpu().tolist()==[0]
        cases.append({'scene':scene,'cutoff':checkpoint['cutoff'],'E_pusd':str(expected),
            'CPU_per_market_signed_shares':[str(value) for value in differences],
            'reference_path':str(reference),'compact_path':source['compact'],'page_path':str(directory/'public-v3-page')})
        reads.append((exposure,portfolio,expected))
        print(json.dumps({'stage':'original_exposure_done',**cases[-1],'elapsed_seconds':time.perf_counter()-started}),flush=True)

    # 原两场景都是Up孤腿，不冒称它们已有相反方向。仅增加合法公共池组件输入：
    # 沿原市场/资产身份目录，通过原SuccessPools.add接纳10份至第一Up、第二Down。
    # 这是精确份额组件代表，不伪造自然SIM/NOE事实或第二CPU账户。
    ledger,pm=directories[0]
    pool=ledger.success_pools
    counter=run(lambda:SuccessPools.open(pool.asset_roots,pool.condition_slots,pool.token_slots,pool.counterpart,1))
    chosen=[next(asset for market,asset,outcome in pm.market_assets if market==m and outcome==side)
        for m,side in ((0,0),(1,1))]
    quantity=reads[1][2]  # 来自原CPU单市场确认10份；数值不是GPU定义的expected。
    for slot in chosen:
        counter,accepted=run(lambda:counter.add(pool.condition_slots[slot:slot+1],pool.token_slots[slot:slot+1],
            rational([quantity]),rational(['6']),yes))
        assert accepted.cpu().tolist()==[True]
    markets=run(lambda:UnresolvedMarkets.open(counter,ledger.unresolved_markets.asset_indices))
    resolutions=run(lambda:MarketResolutions.open(counter,markets,ledger.resolutions.sources))
    opposite=run(lambda:read_portfolio_exposure(replace(ledger,success_pools=counter,resolutions=resolutions),
        success_prefix_known=yes,settlement_prefix_known=yes))
    assert opposite.known.cpu().tolist()==[True] and exact(opposite.amount)==[abs(quantity)+abs(-quantity)]
    assert abs(quantity-quantity)==0 and exact(opposite.amount)[0]>0

    high,portfolio,high_expected=reads[0];low,_,low_expected=reads[1]
    assert high_expected>low_expected>0
    # 下列仅为数学函数的显式输入序列，E数值来自上面独立核过的两个原场景；
    # 不把不同场景的来源拼成同账户自然NOE轨迹。a=.2仅工程算例，无正式默认。
    alpha=run(lambda:rational(['1/5']))
    initial=run(lambda:RewardIntervalMemory.open(t(1000),portfolio,high))
    steady,reward=run(lambda:initial.close(t(2000),portfolio,alpha,yes))
    expected=Fraction(5)*(-Fraction(1,5)*high_expected*1000/Fraction(6_000_000))
    assert exact(reward.exact)==[expected] and reward.known.cpu().tolist()==[True]
    assert reward.dt_seconds.cpu().tolist()==[1.] and reward.present.cpu().tolist()==[True]
    def projected_reward(actual,wanted):
        torch.testing.assert_close(actual.value,torch.tensor([float(wanted)],dtype=torch.float32,device='cuda'),rtol=0,atol=0)
    projected_reward(reward,expected)
    middle=run(lambda:initial.observe(t(1200),low,yes))
    middle=run(lambda:middle.observe(t(1700),high,yes))
    endpoint,changed=run(lambda:middle.close(t(2000),portfolio,alpha,yes))
    area=high_expected*500+low_expected*500
    assert exact(changed.exact)==[Fraction(5)*(-Fraction(1,5)*area/Fraction(6_000_000))]
    projected_reward(changed,Fraction(5)*(-Fraction(1,5)*area/Fraction(6_000_000)))
    assert exact(changed.exact)!=exact(reward.exact) and exact(endpoint.exposure)==exact(steady.exposure)
    assert exact(endpoint.area_pusd_ms)==[0] and endpoint.opened_at_ms.cpu().tolist()==[2000]
    # 学习/日期切片不重开memory；静默后继区间仍从原末端E与真实时间积分。
    _,tail=run(lambda:endpoint.close(t(2100),portfolio,alpha,yes))
    assert exact(tail.exact)==[Fraction(5)*(-Fraction(1,5)*high_expected*100/Fraction(6_000_000))]
    next_portfolio=replace(portfolio,amounts=(run(lambda:portfolio.amounts[0]+rational(['2'])),*portfolio.amounts[1:]))
    _,gain=run(lambda:initial.close(t(2000),next_portfolio,alpha,yes))
    assert exact(gain.exact)==[expected+10]  # ΔN与时间成本结合后一次投影，不分别标准化。
    projected_reward(gain,expected+10)

    unknown=replace(high,known=no)
    pending=run(lambda:initial.observe(t(1100),unknown,yes))
    pending=run(lambda:pending.observe(t(1400),high,yes))
    _,unavailable=run(lambda:pending.close(t(2000),portfolio,alpha,yes))
    assert unavailable.present.cpu().tolist()==[True] and not unavailable.known.any().cpu().item()
    instant=run(lambda:initial.observe(t(1100),unknown,yes))
    instant=run(lambda:instant.observe(t(1100),high,yes))
    _,instant_reward=run(lambda:instant.close(t(2000),portfolio,alpha,yes))
    assert instant_reward.known.cpu().tolist()==[True] and exact(instant_reward.exact)==[expected]
    backwards=run(lambda:initial.observe(t(999),high,yes))
    assert backwards.fault.cpu().tolist()!=[0] and backwards.last_at_ms.cpu().tolist()==[1000]
    # 真实未知奖励接已有生产GAE包装，保持决定人口并阻止整次更新取得资格。
    def not_ready(actual):
        assert actual.present.cpu().tolist()==[True] and not actual.known.any().cpu().item()
        floating=actual.value.reshape(1,1,1);mask=yes.reshape(1,1,1)
        advantages=dt_advantages(torch.zeros_like(floating),torch.zeros_like(floating),floating,
            actual.dt_seconds.reshape(1,1,1),mask,no.reshape(1,1,1),mask,
            actual.known.reshape(1,1,1),mask,actual.fault.reshape(1,1,1),
            tau_gamma_seconds=1.,tau_lambda_seconds=1.)
        assert not advantages.ready.cpu().item() and advantages.count.cpu().item()==1
        assert torch.equal(advantages.present,mask)
    not_ready(unavailable)
    missing_nav=portfolio.known.clone();missing_nav[:,0]=False
    missing_portfolio=replace(portfolio,known=missing_nav)
    missing_start=run(lambda:RewardIntervalMemory.open(t(1000),missing_portfolio,high))
    _,start_unknown=run(lambda:missing_start.close(t(2000),portfolio,alpha,yes))
    _,end_unknown=run(lambda:initial.close(t(2000),missing_portfolio,alpha,yes))
    not_ready(start_unknown);not_ready(end_unknown)

    # 图内实际区间observe+close；发布小状态拥有capture结果，后续replay不覆盖。
    event_time=t(1200);close_time=t(2000)
    live_exposure=replace(low,amount=ExactRational(low.amount.numerator.with_capacity(16),
        low.amount.denominator.with_capacity(16)))
    def calculate():
        state=initial.observe(event_time,live_exposure,yes)
        next_state,result=state.close(close_time,portfolio,alpha,yes)
        return state,next_state,result
    for _ in range(2):run(calculate)
    graph=torch.cuda.CUDAGraph()
    with torch.cuda.graph(graph):captured=Numbers.device_exact_batch(calculate,word_capacity=16,device='cuda')
    graph.replay();torch.cuda.synchronize();assert captured.fits.cpu().item()
    intermediate,memory,result=captured.value
    wanted=Fraction(5)*(-Fraction(1,5)*(high_expected*200+low_expected*800)/Fraction(6_000_000))
    assert exact(result.exact)==[wanted] and result.known.cpu().tolist()==[True]
    projected_reward(result,wanted)
    assert exact(intermediate.area_pusd_ms)==[high_expected*200] and high_expected*200>0
    owned=intermediate.snapshot();old=saved(owned)
    original_area=exact(intermediate.area_pusd_ms)
    event_time.fill_(1500);close_time.fill_(2200)
    # 同图下一输入使用另一已核原暴露值，确实覆盖发布源的上一E而非只改时间。
    for target,incoming in ((live_exposure.amount.numerator,high.amount.numerator),(live_exposure.amount.denominator,high.amount.denominator)):
        target.words.copy_(incoming.with_capacity(16).words);target.sign.copy_(incoming.sign);target.fault.copy_(incoming.fault)
    graph.replay();torch.cuda.synchronize()
    assert captured.fits.cpu().item() and exact(captured.value[2].exact)!=[wanted]
    assert exact(captured.value[0].area_pusd_ms)==[high_expected*500]
    assert exact(captured.value[0].area_pusd_ms)!=original_area
    unchanged(owned,old)
    # 合法高精度显式系数（非正式参数）令真实乘法超过4字；不构造非法字容量。
    demanding=run(lambda:rational([Fraction((1<<120)+1,(1<<120)+7)]))
    prior=saved(middle)
    limited=Numbers.device_exact_batch(lambda:middle.close(t(2000),portfolio,demanding,yes),word_capacity=4,device='cuda')
    assert not limited.fits.cpu().item() and limited.required_words.cpu().item()>4
    unchanged(middle,prior)  # 未安装截断候选，旧面积/时间/上一E完整保留。
    width=1<<(max(4,int(limited.required_words.cpu()))-1).bit_length()
    retry=Numbers.device_exact_batch(lambda:middle.close(t(2000),portfolio,demanding,yes),word_capacity=width,device='cuda')
    assert retry.fits.cpu().item() and retry.value[1].known.cpu().tolist()==[True]
    required_expected=Fraction(5)*(-Fraction((1<<120)+1,(1<<120)+7)*area/Fraction(6_000_000))
    assert exact(retry.value[1].exact)==[required_expected]
    projected_reward(retry.value[1],required_expected)
    one_pass=Numbers.device_exact_batch(lambda:middle.close(t(2000),portfolio,demanding,yes),word_capacity=width,device='cuda')
    assert exact(retry.value[1].exact)==exact(one_pass.value[1].exact)
    unchanged(retry.value[0],saved(one_pass.value[0]))
    unchanged(middle,prior)
    report={'status':'PASS','original_public_account_sources':cases,
        'original_source_commit':(args.source_root/'source-commit.txt').read_text().strip(),
        'original_source_root':str(args.source_root),
        'reuse_scope':'原已提交Ledger冷事实/WCM与固定市场资产身份；不复用其观察时间作为连续奖励前沿',
        'opposite_legs_public_pool_component':True,'engineering_a_pusd':'1/5',
        'area_pusd_ms':str(area),'equal_endpoint_different_area':True,'positive_unknown_blocks_update':True,
        'zero_duration_unknown_does_not_poison':True,'silent_tail_kept':True,'exact_capacity_retry_words':width,
        'capture_replay_and_owned_nonzero_area_state':True,'start_and_end_WCM_unknown_keep_population_and_block_update':True,
        'independent_float32_reward_projection':True,'actual_continuous_reward_collector_PPO_restore':False,
        'elapsed_seconds':time.perf_counter()-started}
    args.output.write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report),flush=True)


if __name__=='__main__':main()
