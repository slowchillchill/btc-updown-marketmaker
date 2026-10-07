"""原同一GTC账户1500/1600联合冻结与两步GRU；不是完整根采集器。"""
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
import hashlib
import json
import os
import subprocess
import sys
import time
import torch
from queues import Numbers
from validate_base_execution import integers,rational,values

ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/p) for p in ('data-pipeline/gpu','shared-run/gpu','ledger/gpu','model-train/native-inputs/gpu')]


def install_opening(ledger,source):
    """只供这个完整新开户fixture；后继读面随同一Ledger事务维护，不逐cutoff重装。

    规范producer已经导出开户空市场/资产/订单；原fixture后继无Cost/ExternalCash/
    Resolution，相关目录从开户空历史起跟随实际NOE，不能借当前余额猜历史。
    """
    from projected_markets import ProjectedMarkets
    from resolution import ResolutionSources,MarketResolutions
    from settlement_accounting import SettlementCosts,ExternalCashIdentities
    from rebate_day import RebateDays
    zero=torch.zeros(1,dtype=torch.int64,device='cuda');yes=torch.ones(1,dtype=torch.bool,device='cuda')
    opening=source['opening_ledger']['projection']
    assert opening['platform_source_prefix']==0 and not opening['market_projections']
    assert not opening['platform_confirmed_projection']['outcome_balances']
    assert not source['opening_ledger']['resolutions'] and not source['opening_ledger']['settlement_costs']
    assert not source['opening_ledger']['rebate_days'] and source['opening_ledger']['other_cash']=='0'
    sources=ResolutionSources.bind(SimpleNamespace(terminals=[],canonical_ids=[]),['1','2'])
    return replace(ledger,projected_markets=ProjectedMarkets.open(zero,~yes[:,None],yes),
        resolutions=MarketResolutions.open(ledger.success_pools,ledger.unresolved_markets,sources),
        settlement_costs=SettlementCosts.empty(1,1,'cuda',known=True),
        external_cash=ExternalCashIdentities.empty(1,1,'cuda',known=True),
        rebate_days=RebateDays.empty(zero[:,None],1),other_cash=rational(['0']))


class JointFreezeValidation:
    def __init__(self,source,artifact):
        from public_device_page import PreparedPublicPage,build_public_device_page
        from rules import PublicRuleGpu
        from input_history import SourceHistoryInputs
        from input_100ms import MarketInputs100ms,MarketFreezeMemory
        from commitments import SubmittedLegs
        from registry import FrozenIntentOrders
        from model_train.gpu_policy import Policy100ms,ModelShape100ms
        self.source,self.artifact=source,artifact;self.started=time.perf_counter();self.rows=[];self.reports=[]
        self.run=Numbers.run_exact_batch
        self.zero=torch.zeros(1,dtype=torch.int64,device='cuda');self.yes=torch.ones(1,dtype=torch.bool,device='cuda')
        compact=artifact/'public-compact'
        assert compact.is_dir() and source['joint_public']['root_started_at']==-29000
        build_public_device_page(Path(os.environ['B_GPU_COLUMNS_BINARY']),compact,artifact/'public-page')
        self.prepared=PreparedPublicPage(artifact/'public-page',compact)
        self.pm,self.market,reference,binance=self.prepared.install()
        assert len(self.pm.markets)==1 and [a['token_id'] for a in self.pm.assets]==['1','2']
        assert self.pm.markets[0]['condition_id']==source['initial'][0]['market']['condition_id']
        self.start=self.zero-29000  # 构造之初声明完整有限工程源起点；期间没有额外行情。
        snapshot=ROOT/'sim-system/tests/fixtures/platform-rule-snapshot-v3-gate-b.json'
        request=artifact/'rule-request.json'
        request.write_text(json.dumps({'snapshot_path':str(snapshot),
            'expected_file_sha256':hashlib.sha256(snapshot.read_bytes()).hexdigest(),
            'replay_causal_start_utc_ms':2**63-1,'market':self.pm.markets[0]}))
        context=json.loads(subprocess.check_output([os.environ['B_GPU_RULE_CONTEXT_BINARY'],str(request)]))
        (artifact/'rule-context.json').write_text(json.dumps(context))
        history=SourceHistoryInputs(self.pm,self.market,reference,self.prepared.columns)
        self.history=history;self.memory=history.empty(1,self.start);self.through=0
        # 建立本组件1400窗口起点：保留真实此前来源历史，仅清前窗path累计。
        # 不运行1000–1400模型、不制造该段hidden，也不把根起点追改为1400。
        before=int((self.pm.causal_visible_at<=1400).sum().cpu())
        if before:
            groups=torch.arange(before,device='cuda')
            self.memory,error=self.run(lambda:history.consume(groups,self.pm.causal_visible_at[groups],
                self.zero,self.start,self.memory))
            assert not error.any().cpu().item()
        self.memory=self.run(lambda:history.freeze(self.memory)).memory
        self.through=before
        self.endpoints=MarketFreezeMemory(self.zero+before-1,self.zero+1400,None,None)
        self.encoder=MarketInputs100ms(self.pm,self.market,reference,binance,history,
            PublicRuleGpu(context),self.prepared.columns,self.pm.causal_visible_at)
        self.intents=self.run(lambda:FrozenIntentOrders(self.yes[:,None],integers([1]),self.yes[:,None],
            rational(['5']),~self.yes[:,None],self.yes[:,None]))
        # 只登记原Post合法腿；后继Query预留没有Post receipt，不能造为已提交腿。
        self.legs=self.run(lambda:SubmittedLegs(self.yes[:,None],self.yes[:,None],self.zero[:,None],
            self.zero[:,None],rational(['5']),self.yes[:,None],self.zero[:,None],self.zero[:,None],self.yes[:,None]))
        self.identities=torch.zeros((1,3),dtype=torch.int64,device='cuda')
        # 显式有限组件参数，不是正式profile/根预算；hidden数值从1500开始。
        torch.manual_seed(181)
        self.policy=Policy100ms(ModelShape100ms(layers=2,hidden=64)).cuda().eval()
        self.hidden=torch.zeros((2,1,64),device='cuda');self.initial_hidden=self.hidden.clone()
        self.version=self.zero+7;self.saved=None;self.captured_old=None

    def freeze(self,state,cutoff):
        from input_account import account_inputs_at_prefix
        from portfolio_nav import PortfolioValuation
        from model_train.frozen_inputs import FrozenPolicyInputs100ms
        print(json.dumps({'stage':'joint_freeze_start','cutoff':int(cutoff.cpu()[0]),
            'elapsed_seconds':time.perf_counter()-self.started}),flush=True)
        through=int((self.pm.causal_visible_at<=cutoff[0]).sum().cpu())
        if through>self.through:
            groups=torch.arange(self.through,through,device='cuda')
            self.memory,error=self.run(lambda:self.history.consume(groups,self.pm.causal_visible_at[groups],
                self.zero,self.start,self.memory))
            assert not error.any().cpu().item()
        self.through=through
        history={name:self.yes for name in ('executed_prefix_known','success_prefix_known','rebate_prefix_known',
            'settlement_prefix_known','ledger_prefix_known','economic_liability_prefix_known','economic_prefix_known')}
        outcomes=torch.tensor([a['outcome']['index'] for a in self.pm.assets],device='cuda')
        valuation=PortfolioValuation(self.pm,['1','2'],state.ledger.success_pools.asset_roots)
        def encode():
            market=self.encoder.freeze(cutoff,self.zero,self.start,self.memory,self.endpoints)
            account=state.freeze_account(self.intents,cutoff,submitted_legs=self.legs)
            quote=valuation.freeze(market.prefixes,cutoff)
            native=account_inputs_at_prefix(account,self.market,self.zero,market.prefixes,self.start,
                self.zero[:,None],self.yes[:,None],outcomes,row_role=market.role,valuation=quote,**history)
            return market,native.inputs,account
        # 正常路径固定资源；只在共同资源出口冷增长后重算同观察，不重吃来源。
        words=16
        while True:
            candidate=Numbers.device_exact_batch(encode,word_capacity=words,device='cuda')
            market,native,account=candidate.value
            if bool((candidate.fits&market.resources_fit.all()).cpu()):break
            words=1<<(max(4,int(candidate.required_words.cpu()))-1).bit_length()
            self.encoder.grow_resources(market.required.max(0).values.cpu().tolist())
        assert market.fault.cpu().tolist()==[0] and native.fault.cpu().tolist()==[0]
        assert native.provided.all().cpu().item(),torch.nonzero(~native.provided).cpu().tolist()
        assert market.role.cpu().tolist()==[1] and market.book_mask.any().cpu().item()
        # 原账户余额/序号独立oracle已由调用者逐步检查。这里另外核真实未领人口。
        pending=values(account.pending_noe_count)[0]
        # 原pending_noe_count仅统计已领取等待归约；未领取1506响应仍在NOE人口。
        unreceived=int(state.pending_noe.active.sum().cpu())
        assert pending==0 and unreceived==(1 if len(self.rows)==0 else 0)
        # 实际同设备完整账户装配捕获，输入历史/账户候选在本次图地址上稳定。
        graph=torch.cuda.CUDAGraph()
        with torch.cuda.graph(graph):captured=Numbers.device_exact_batch(encode,word_capacity=words,device='cuda')
        graph.replay();torch.cuda.synchronize()
        assert captured.fits.cpu().item()
        for actual,expected in ((captured.value[0].fast,market.fast),(captured.value[1].slow,native.slow),
                (captured.value[1].provided,native.provided)):
            assert torch.equal(actual,expected)
        captured_market,captured_native,_=captured.value
        publishing_keys=self.identities.clone()
        owned=FrozenPolicyInputs100ms.publish(captured_market,captured_native,publishing_keys,captured.fits)
        with torch.no_grad():
            evaluated=self.policy.frozen_state_and_value(owned,self.hidden,self.identities,self.version)
            assert evaluated.ready.cpu().tolist()==[True] and evaluated.fault.cpu().tolist()==[0]
            self.hidden=evaluated.hidden
        self.rows.append(owned)
        if self.saved is None:
            self.saved={name:getattr(owned,name).clone() for name in owned.__dataclass_fields__}
            self.captured_old=(captured,publishing_keys)
        else:
            # 覆盖实际publish源，而不是另一个eager/capture对象。快/慢来自下一真实
            # cutoff；本场景不变化的其它列以退役暂存覆写检验，不成为新业务输入。
            old_capture,old_keys=self.captured_old
            old_market,old_native,_=old_capture.value
            old_market.fast.copy_(captured_market.fast)
            old_native.slow.copy_(captured_native.slow)
            old_market.book_levels.zero_();old_market.book_mask.zero_()
            old_market.role.fill_(-1);old_keys.fill_(-1)
            old_native.cutoff.fill_(-1);old_native.provided.zero_()
            old_market.fault.fill_(1);old_native.fault.fill_(1)
            old_market.resources_fit.zero_();old_capture.fits.zero_()
            assert all(torch.equal(getattr(self.rows[0],name),value) for name,value in self.saved.items())
            assert not torch.equal(self.rows[0].slow,owned.slow)  # 真实时间/公开Query来源变化。
        self.memory=market.history_memory;self.endpoints=market.endpoint_memory
        report={'cutoff':int(cutoff.cpu()[0]),'ledger_entry':values(state.ledger.current_entry_sequence)[0],
            'public_prefix':int(market.prefixes.cpu()[0]),'pending_noe':pending,'unreceived_noe':unreceived,'role':1,
            'provided_fields':433,'elapsed_seconds':time.perf_counter()-self.started}
        self.reports.append(report);print(json.dumps({'stage':'joint_freeze_done',**report}),flush=True)

    def finish(self):
        assert len(self.rows)==2
        inputs=tuple(torch.stack([row.model_inputs()[index] for row in self.rows]) for index in range(4))
        active=torch.stack([row.role>=0 for row in self.rows])
        with torch.no_grad():
            states,values_at,expected=self.policy.unroll(inputs,self.initial_hidden,active)
            torch.testing.assert_close(self.hidden,expected,rtol=1e-5,atol=1e-6)
            def model_pair():
                first=self.policy.frozen_state_and_value(self.rows[0],self.initial_hidden,self.identities,self.version)
                second=self.policy.frozen_state_and_value(self.rows[1],first.hidden,self.identities,self.version)
                return first,second
            graph=torch.cuda.CUDAGraph()
            for _ in range(2):model_pair()
            with torch.cuda.graph(graph):captured=model_pair()
            graph.replay();torch.cuda.synchronize()
            for index,result in enumerate(captured):
                assert result.ready.cpu().tolist()==[True] and result.fault.cpu().tolist()==[0]
                assert torch.equal(result.policy_version,self.version)
                torch.testing.assert_close(result.state,states[index],rtol=1e-5,atol=1e-6)
                torch.testing.assert_close(result.value,values_at[index],rtol=1e-5,atol=1e-6)
            torch.testing.assert_close(captured[1].hidden,expected,rtol=1e-5,atol=1e-6)
            first=self.policy.frozen_state_and_value(self.rows[0],self.initial_hidden,self.identities,self.version)
            assert not torch.equal(self.hidden,first.hidden) and (self.hidden!=0).any(dim=(1,2)).all().cpu().item()
            wrong=self.policy.frozen_state_and_value(self.rows[0],self.initial_hidden,self.identities+1,self.version)
            assert not wrong.ready.any().cpu().item() and torch.equal(wrong.hidden,self.initial_hidden)
        report={'status':'PASS','frames':self.reports,'GRU_layers':2,'model_hidden_start':1500,
            'complete_root_history_1000_1400':False,'continuous_host_PPO_restore':False}
        (self.artifact/'joint-freeze-report.json').write_text(json.dumps(report,indent=2)+'\n')
        return report
