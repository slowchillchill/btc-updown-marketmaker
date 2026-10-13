"""原Auto入口事前源码对照；只观察同一生产候选，不另驱动经济。"""
from dataclasses import fields,replace
from pathlib import Path
import importlib.util,json,os,sys,time
import torch
from queues import Numbers,ExactInt
from base_runtime import BaseSimRuntime
from base_runtime_storage import _owned
from validate_maker_projection import equal,compare_projection


def load(name,path):
    spec=importlib.util.spec_from_file_location(name,path);module=importlib.util.module_from_spec(spec)
    sys.modules[name]=module;spec.loader.exec_module(module);return module


def observe(backend,artifact,*,resources=False):
    artifact=Path(artifact);artifact.mkdir(parents=True)
    base=Path(os.environ['B_GPU_AUTO_BASELINE'])
    old_runtime=load('_auto_old_runtime',base/'sim-system/gpu/base_runtime.py')
    old_projection=load('_auto_old_projection',base/'execution-normalization/gpu/projection.py')
    owner=backend.auto_loop;original=type(owner).run.__get__(owner,type(owner));rows=[];held=[];precision=[]
    def run(world,projection,at,enabled,sampler,*,preceding=None):
        inputs=(world.account,world.resolution_obligations.auto_redeemed,
            world.resolution_obligations.fault,projection,at,enabled)
        before=_owned(inputs,inputs);frontier=world.input_frontier
        sequence=None if frontier is None else _owned(frontier.next_sequence,frontier.next_sequence)
        old=old_projection.Projection(**{f.name:getattr(projection,f.name) for f in fields(projection)})
        expected,p,old_batches=Numbers.run_exact_batch(lambda:old_runtime.BaseSimRuntime.try_auto_redeem(
            world,at,enabled,old,sampler,preceding=preceding))
        captured=owner.current
        torch.cuda.synchronize();start=time.perf_counter()
        actual,projected,batches=original(world,projection,at,enabled,sampler,preceding=preceding)
        torch.cuda.synchronize();elapsed=time.perf_counter()-start
        equal(expected.account,actual.account,'account')
        equal(expected.resolution_obligations,actual.resolution_obligations,'obligations')
        compare_projection(p,projected,old_batches,batches,artifact)
        equal(before,inputs,'input')
        assert actual.input_frontier is frontier
        if frontier is not None:equal(frontier.next_sequence,sequence,'input_sequence')
        for value,saved in held:equal(value,saved,'old_output')
        held[:]=[(actual.account,_owned(actual.account,actual.account)),
            (actual.resolution_obligations.auto_redeemed,actual.resolution_obligations.auto_redeemed.clone()),
            (projected,_owned(projected,projected))]
        if resources and batches and not precision:
            from auto_redemption_loop import AutoRedemptionDeviceLoop,_CapturedAuto
            probe=AutoRedemptionDeviceLoop();operation=BaseSimRuntime.prepare_auto_market
            replay=_CapturedAuto.replay
            marker=ExactInt.from_values([(1<<126)+1],device=at.device)
            def compute(self,*args,**kwargs):
                marker*marker  # 机制探针；不改变原Auto金额/人口。
                return operation(self,*args,**kwargs)
            def record(self,*args,**kwargs):
                result=replay(self,*args,**kwargs)
                precision.append(dict(width=self.width,required=int(result[3].cpu()),fits=bool(result[4].cpu()),
                    entering_account_unchanged=True))
                equal(before,inputs,'resource_input')
                return result
            BaseSimRuntime.prepare_auto_market=compute;_CapturedAuto.replay=record
            try:
                candidate,pr,bs=probe.run(world,projection,at,enabled,sampler,preceding=preceding)
            finally:BaseSimRuntime.prepare_auto_market=operation;_CapturedAuto.replay=replay
            assert [(x['width'],x['required'],x['fits']) for x in precision]==[(4,8,False),(8,0,True)],precision
            equal(candidate.account,expected.account,'resource_account')
            equal(candidate.resolution_obligations,expected.resolution_obligations,'resource_obligations')
            compare_projection(p,pr,old_batches,bs,artifact)
        if batches:
            row=dict(at=at.cpu().tolist(),markets=world.resolution_obligations.reference_indices.size(1),
                eligible=world.auto_market_ready(enabled).cpu().tolist(),batches=len(batches),
                batch_sequences=[b.batch_sequences.words.cpu().tolist() for b in batches],
                record_ordinals=[b.record_ordinals.words.cpu().tolist() for b in batches],
                private_sequences=[b.private_sequences.words.cpu().tolist() for b in batches],
                received_at=[b.received_at.cpu().tolist() for b in batches],
                completed_markets=(actual.resolution_obligations.auto_redeemed&~world.resolution_obligations.auto_redeemed).cpu().tolist(),
                emitted=[b.emitted.cpu().tolist() for b in batches],fault=[b.fault.cpu().tolist() for b in batches],
                cold_rebuilt=owner.current is not captured,
                full_auto_handler_seconds_including_cold_input_copy_loop_owned_output=elapsed)
            rows.append(row);print(json.dumps(dict(stage='auto_device_candidate',**row)),flush=True)
        (artifact/'auto-report.json').write_text(json.dumps(dict(calls=rows,precision=precision,
            scope='original per-market frozen source oracle; full Auto candidate includes cold/copy/loop/owned output; public append/receive outside'),indent=2)+'\n')
        return actual,projected,batches
    owner.run=run
    return rows


def two_market_tail(root,rows,config,stage):
    assert root.goal_until is None and root.trading_until.cpu().tolist()==[config['trading_until']]
    assert int(root.sampler.delays.max().cpu())==config['feedback_delay_upper_bound']
    torch.cuda.synchronize();start=time.perf_counter()
    while True:
        advance=root.run.advance_before(root.tensor([config['tail_endpoint']]),root.yes,progress=stage)
        if advance.complete.all().cpu().item():break
        assert root.resume_resources(advance)
    torch.cuda.synchronize();elapsed=time.perf_counter()-start
    assert len(rows)==len(config['terminal_source']),rows
    assert all(row['at'][0]>=source for row,source in zip(rows,config['terminal_source'],strict=True)),rows
    assert all(max(value[0])<config['tail_endpoint'] for row in rows for value in row['received_at']),rows
    assert [row['completed_markets'] for row in rows]==[[[True,False]],[[False,True]]]
    assert all(row['markets']==2 and row['batches']==1 and row['fault']==[[0]] for row in rows)
    assert root.clock.runtime.resolution_obligations.auto_redeemed.cpu().tolist()==[[True,True]]
    assert root.clock.public.ledger.resolutions.auto_redeemed.cpu().tolist()==[True,True]
    assert not root.clock.public.waiting.received.logical_and(~root.clock.public.waiting.consumed).any().cpu().item()
    assert root.clock.source.next().eof.cpu().item()
    owner=root.runtime.execution.auto_loop;world=root.clock.runtime;projection=root.clock.projection
    before=_owned((world.account,projection),(world.account,projection))
    actual,p,batches=owner.run(world,projection,root.tensor([config['tail_endpoint']]),root.yes,root.sampler)
    assert not batches and actual is world and p is projection
    equal(before,(world.account,projection),'repeat')
    from validate_base_execution import values
    ledger=root.clock.public.ledger
    balances=[str(__import__('fractions').Fraction(n,d)) for n,d in zip(values(ledger.platform.balances.numerator),values(ledger.platform.balances.denominator),strict=True)]
    report=dict(ledger_entry=values(ledger.current_entry_sequence),platform_prefix=values(ledger.platform.platform_source_prefix),
        ledger_exact_balances=balances,ledger_auto_redeemed=ledger.resolutions.auto_redeemed.cpu().tolist(),
        endpoint=config['tail_endpoint'],trading_until=config['trading_until'],goal_until=None,
        markets=2,one_original_account=True,actual_calls=rows,repeat_no_payment=True,
        full_tail_seconds_including_SIM_Auto_public_append_receive=elapsed)
    stage('two_market_auto_received',**report)
    return report
