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
        self.source,self.artifact=source,artifact;self.started=time.perf_counter();self.rows=[];self.reports=[];self.actor_reports=[]
        self.actor_saved=None
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
        if os.environ.get('B_GPU_ACTOR_PATH_VALIDATION')=='1':
            from intent_terms import SubmittedBuyTerms
            # 原发送正文提供不可变条款；公开Accepted才给订单建立origin。
            terms=source['post_terms']['order']['kind']['GtcLimit']
            self.legs=replace(self.legs,terms=self.run(lambda:SubmittedBuyTerms(self.zero[:,None],
                torch.full_like(self.yes[:,None],terms['post_only']),rational([terms['limit_price_pusd_per_share']]),
                rational(['0']),self.zero[:,None])))
        self.identities=torch.zeros((1,3),dtype=torch.int64,device='cuda')
        # 显式有限组件参数，不是正式profile/根预算；hidden数值从1500开始。
        torch.manual_seed(181)
        self.policy=Policy100ms(ModelShape100ms(layers=2,hidden=64)).cuda().eval()
        self.hidden=torch.zeros((2,1,64),device='cuda');self.initial_hidden=self.hidden.clone()
        self.version=self.zero+7;self.saved=None;self.captured_old=None

    def freeze(self,state,cutoff,bank=None):
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
            return market,native.inputs,account,native
        # 正常路径固定资源；只在共同资源出口冷增长后重算同观察，不重吃来源。
        words=16
        while True:
            candidate=Numbers.device_exact_batch(encode,word_capacity=words,device='cuda')
            market,native,account,_=candidate.value
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
        captured_market,captured_native,captured_account,captured_scopes=captured.value
        actor_parts=None
        if (os.environ.get('B_GPU_ACTOR_CONTEXT_VALIDATION')=='1'
                or os.environ.get('B_GPU_ACTOR_PATH_VALIDATION')=='1'):
            actor_parts=self.check_actor_context(captured_account,captured_scopes,captured_market)
        publishing_keys=self.identities.clone()
        owned=FrozenPolicyInputs100ms.publish(captured_market,captured_native,publishing_keys,captured.fits)
        incoming_hidden=self.hidden
        with torch.no_grad():
            evaluated=self.policy.frozen_state_and_value(owned,self.hidden,self.identities,self.version)
            assert evaluated.ready.cpu().tolist()==[True] and evaluated.fault.cpu().tolist()==[0]
            self.hidden=evaluated.hidden
        if os.environ.get('B_GPU_ACTOR_PATH_VALIDATION')=='1':
            from model_train.device_actor_validation import check_joint
            context,objects,directory=actor_parts
            from intent_terms import freeze_intent_orders
            from cancel_requests import CancellationScopes,freeze_cancel_snapshot,freeze_scoped_cancel_requests
            from input_cancel import cancel_inputs
            from model_train.action_inputs import ActorPolicyInputs
            intents,error=self.run(lambda:freeze_intent_orders(self.legs,state.submission_bindings,
                self.intents.identities,captured_account.orders.present))
            assert error.cpu().tolist()==[0] and intents.origin_known.cpu().tolist()==[[True]]
            targets=state.cancellation_target_history.targets
            assert targets.fault.cpu().tolist()==[0] and targets.received.any().cpu().item()
            # 原显式单笔Cancel请求scope包含已终态sim-1，不按当前eligible对象裁掉。
            request_scopes=CancellationScopes(~self.yes[:,None],self.zero[:,None]-1,self.yes[:,None,None])
            object_scopes=CancellationScopes(objects.kinds==2,objects.markets,objects.members)
            location=self.zero[:,None];known=self.yes[:,None]
            snapshot=self.run(lambda:freeze_cancel_snapshot(state.cancellations,request_scopes,
                object_scopes,objects.present,location,known,captured_account.orders.present,targets,cutoff))
            requests=self.run(lambda:freeze_scoped_cancel_requests(snapshot.state,request_scopes,
                object_scopes,location,snapshot.location_known,snapshot.targets,cutoff))
            data=self.run(lambda:cancel_inputs(objects,directory,intents,captured_account.orders,
                captured_account.order_sources,captured_account.ledger.order_funds,
                captured_account.ledger.order_funds.read(captured_account.ledger.lifecycle),
                bank,requests,cutoff,location,self.zero[:,None],self.zero[:,None],
                self.zero[:,None,None].expand(-1,2,-1),torch.stack((self.yes,~self.yes),1)[:,:,None]))
            assert data.fault.cpu().tolist()==[0]
            learning_evaluation=self.policy.frozen_state_and_value(owned,incoming_hidden,self.identities,self.version)
            inputs=ActorPolicyInputs.publish(context,captured_market,owned,learning_evaluation,data)
            check_joint(self.policy,inputs,self.artifact,len(self.rows),self.actor_reports,
                published_sources=(captured_market,learning_evaluation,data))
        self.rows.append(owned)
        if self.saved is None:
            self.saved={name:getattr(owned,name).clone() for name in owned.__dataclass_fields__}
            self.captured_old=(captured,publishing_keys)
        else:
            # 覆盖实际publish源，而不是另一个eager/capture对象。快/慢来自下一真实
            # cutoff；本场景不变化的其它列以退役暂存覆写检验，不成为新业务输入。
            old_capture,old_keys=self.captured_old
            old_market,old_native,_,_=old_capture.value
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

    def check_actor_context(self,account,native,market):
        from actor_context import publish_actor_context
        from actor_support import DeviceNativeActor
        from cancel_objects import OrderFormation,CancelObjectDirectory,freeze_cancel_objects
        # 原单腿GTC receipt已实际公开；此两cutoff均已取消。原完整fixture无双腿请求。
        source=account.order_sources.formed
        formation=OrderFormation(account.orders.present,self.zero[:,None],self.zero[:,None],
            self.intents.receipt_known,torch.full_like(self.zero[:,None],self.source['processing_at']),
            integers([self.source['post_input_sequence']]),self.zero[:,None],source.present,
            source.private_sequences,account.fault)
        frame=self.market.frame(market.prefixes)
        directory=CancelObjectDirectory(frame.admitted,self.market.times[:,0][None,:],
            self.zero[:,None],self.yes[:,None],torch.empty((1,0),dtype=torch.bool,device='cuda'),
            torch.empty((1,0,2),dtype=torch.int64,device='cuda'),torch.empty((1,0),dtype=torch.int64,device='cuda'))
        objects=self.run(lambda:freeze_cancel_objects(account.orders,formation,directory))
        # 原请求condition已准入；无人可撤仍保留合法的空成员Market对象。
        selected=objects.present
        assert selected.sum().cpu().item()==1
        assert objects.kinds[selected].cpu().tolist()==[2]
        assert objects.markets[selected].cpu().tolist()==[0]
        assert not objects.members[selected].any().cpu().item()
        assert self.pm.markets[0]['condition_id']==self.source['initial'][0]['market']['condition_id']
        keys=self.identities.clone()
        context=self.run(lambda:publish_actor_context(account,native,self.encoder,market,self.zero,self.zero,keys,objects))
        assert context.fault.cpu().tolist()==[0]
        from fractions import Fraction
        actual,maker=account.funds.actual_available,account.funds.maker_available
        exact=lambda value:[Fraction(n,d) for n,d in zip(values(value.numerator),values(value.denominator))]
        assert values(context.cash_micro)==[max(0,int(min(exact(actual)[0],exact(maker)[0])*1000000))]
        assert values(context.cap_centishares)==[max(0,int(value*100)) for value in exact(native.inputs.repair.cap_exact)]
        assert values(context.tick_units)==[int(Fraction(event['payload']['event']['initial_tick_size_pusd_per_share'])*10000)
            for event in self.source['initial']]
        actor=DeviceNativeActor(context)
        initial=actor.initial();support=self.run(lambda:actor.support(initial))
        assert support.fault.cpu().tolist()==[0] and support.family[:,3:].cpu().tolist()==[[False,False,True]]
        cancel_state=self.run(lambda:actor.choose(initial,torch.full_like(self.zero,5)))
        cancel_support=self.run(lambda:actor.support(cancel_state))
        expected_global=torch.arange(objects.present.shape[1],device='cuda')[None,:]==0
        assert torch.equal(cancel_support.cancel,expected_global)
        physical=actor.cancel_slots[:,0]
        assert objects.present.gather(1,physical[:,None]).cpu().tolist()==[[True]]
        assert objects.kinds.gather(1,physical[:,None]).cpu().tolist()==[[2]]
        assert objects.markets.gather(1,physical[:,None]).cpu().tolist()==[[0]]
        assert not objects.members[0,physical[0]].any().cpu().item()
        selected_cancel=self.run(lambda:actor.choose(cancel_state,self.zero))
        assert selected_cancel.fault.cpu().tolist()==[0] and selected_cancel.stage.cpu().tolist()==[7]
        assert selected_cancel.cancel_index.cpu().tolist()==[0]
        from actor_context import _owned
        if self.actor_saved is None:
            self.actor_saved=(context,_owned(context.cash_micro),_owned(context.cap_centishares),
                _owned(context.tick_units),context.cutoff.clone(),context.identities.clone(),
                _owned(context.cancels),context.gap_positive.clone(),context.gap_known.clone(),
                context.cap_known.clone(),keys,objects,account,native)
        else:
            old,cash,cap,ticks,at,identity,population,gap_positive,gap_known,cap_known,old_keys,old_objects,old_account,old_native=self.actor_saved
            # 覆写实际发布源的退役暂存；这些值不再作为业务输入。
            old_keys.fill_(-1);old_objects.present.fill_(False);old_objects.markets.fill_(-1)
            old_objects.members.fill_(True);old_account.cutoff.fill_(-1)
            old_native.inputs.repair.cap_exact.numerator.words.zero_()
            old_native.inputs.repair.gap_exact.numerator.sign.fill_(1)
            old_native.inputs.repair.gap_known.fill_(False);old_native.inputs.repair.cap_known.fill_(False)
            for actual,expected in ((old.cash_micro,cash),(old.cap_centishares,cap),(old.tick_units,ticks)):
                assert torch.equal(actual.words,expected.words) and torch.equal(actual.sign,expected.sign)
                assert torch.equal(actual.fault,expected.fault)
            assert torch.equal(old.cutoff,at) and torch.equal(old.identities,identity)
            for name in ('present','kinds','members','formation_ranks','origin_pairs','markets','fault'):
                assert torch.equal(getattr(old.cancels,name),getattr(population,name))
            assert torch.equal(old.gap_positive,gap_positive) and torch.equal(old.gap_known,gap_known)
            assert torch.equal(old.cap_known,cap_known)
            again=self.run(lambda:DeviceNativeActor(old).support(DeviceNativeActor(old).initial()))
            assert again.fault.cpu().tolist()==[0] and again.family[:,3:].cpu().tolist()==[[False,False,True]]
            old_actor=DeviceNativeActor(old)
            old_cancel=self.run(lambda:old_actor.choose(old_actor.initial(),torch.full_like(self.zero,5)))
            assert torch.equal(self.run(lambda:old_actor.support(old_cancel)).cancel,expected_global)
            old_physical=old_actor.cancel_slots[:,0]
            assert old.cancels.kinds.gather(1,old_physical[:,None]).cpu().tolist()==[[2]]
            assert old.cancels.markets.gather(1,old_physical[:,None]).cpu().tolist()==[[0]]
            assert not old.cancels.members[0,old_physical[0]].any().cpu().item()
            old_selected=self.run(lambda:old_actor.choose(old_cancel,self.zero))
            assert old_selected.fault.cpu().tolist()==[0] and old_selected.cancel_index.cpu().tolist()==[0]
        self.actor_reports.append({'actor_exact_context_cutoff':int(context.cutoff.cpu()[0]),
            'cash_micro':values(context.cash_micro),'cap_centishares':values(context.cap_centishares),
            'empty_member_market_object':True})
        return context,objects,directory

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
        if self.actor_reports: report['actor_exact_context']=self.actor_reports
        (self.artifact/'joint-freeze-report.json').write_text(json.dumps(report,indent=2)+'\n')
        return report
