"""共同更新后的活动检查点：受限状态载荷、原owner安装和材料位置。

只保存成功消费边界。函数/Native连接/设备图由绑定的原装配重新连接。
不保存任意Python对象；非dataclass owner只有下面列出的原状态拥有者。
"""
from dataclasses import fields,is_dataclass
import hashlib
import importlib
import json
import os
from pathlib import Path
import sys
import tempfile
import platform
import sqlite3
from importlib.metadata import version
import numpy as np
import torch

VERSION='joint_actor_checkpoint_v1'
SOURCE_ROOT=Path(__file__).resolve().parents[2]
# 实际公共CUDA扩展的原构建输入；并非全仓源码/文档资格门。
EXTENSION_SOURCES=('shared-run/gpu/extension.py','shared-run/gpu/bindings.cpp','shared-run/gpu/conditional_graph.cu',
 'contracts/gpu/exact.cu','contracts/gpu/exact.cuh','contracts/gpu/ordered_attribution.cu',
 'contracts/gpu/policy_random.cu','sim-system/gpu/limit_segment.cu','sim-system/gpu/hidden_gather.cu',
 'sim-system/gpu/latency.cu','execution-normalization/gpu/identity.cu',
 'data-pipeline/gpu/decimal.cu','data-pipeline/gpu/public_state.cu')
DEPENDENCY_LOCKS=('model-train/native-inputs/Cargo.lock','model-train/pyproject.toml',
 'model-train/uv.lock','model-train/pylock.cu128.toml','model-train/gpu-build-tools.lock.json')
# 原恢复/续采/换页/动作消费者的延后导入；不以当前已导入集合代替这些职责。
DEFERRED_SOURCES=('model-train/model_train/frozen_inputs.py','model-train/model_train/device_actor.py',
 'model-train/model_train/logical_batch.py','shared-run/gpu/actor_runtime.py',
 'shared-run/gpu/actor_opening.py','shared-run/gpu/action_selection.py',
 'shared-run/gpu/cancel_requests.py','shared-run/gpu/cancel_submission.py',
 'shared-run/gpu/post_submission.py','shared-run/gpu/commitments.py','shared-run/gpu/registry.py',
 'shared-run/gpu/cancel_objects.py','sim-system/gpu/cancel_results.py',
 'sim-system/gpu/pending_post_intents.py','sim-system/gpu/settlement_redemption.py',
 'sim-system/gpu/hidden_resolution.py','sim-system/gpu/hidden_frontier.py',
 'sim-system/gpu/market_resolution_orders.py','execution-normalization/gpu/device_identity.py',
 'model-train/native-inputs/gpu/actor_features.py','model-train/native-inputs/gpu/actor_price_fee.py',
 'sim-system/gpu/public_trade_execution.py','sim-system/gpu/public_trade_loop.py','sim-system/gpu/conservative_rank.py',
 'sim-system/gpu/hidden_level_loop.py','sim-system/gpu/hidden_transaction_loop.py',
 'ledger/gpu/reservation.py','model-train/model_train/training_report.py')
# 每个排除字段有原装配中的重新连接者；其余实际字段不能静默遗漏。
OWNERS={
 'model_train.continuous_root.ContinuousActorRoot':('codec','tensor'),
 'model_train.continuous_actor.ActorModelQueue':('policy_at_start','timing','sender_factory','release_markets','on_started','on_completed','start_frontier'),
 'model_train.continuous_inputs.ContinuousActorInputs':('account_builder',),
 'continuous_actor.ContinuousActorRun':('freeze','receive','initialize','settlement'),
 'send_clock.BoundedSendClock':('handlers','bank','public_loop'),
 'public_source.PublicGroupSource':('install_listeners',),
 'continuous_execution.ContinuousExecution':('codec','fill_paths','fifo_loop','maker_loop','maker_projection_loop','reference_loop','level_loop','transaction_loop','auto_loop','public_trade','queue_stage','pending_queue_stage'),
 'actor_send.FrozenActorSend':('codec','receive'),
 'actor_send.ActorRequestSubmission':('codec',),
 'actor_send.SubmittedActorRequests':(),
 'task_requests.ActorRequestDirectory':(),
 'task_requests.ActorTaskRequests':(),
 'ledger_drain.PublicLedgerReceiver':('captured','progress'),
 'model_train.device_actor.ActorSelection':(),
 'model_train.logical_batch.LogicalBatch':('learner','on_replayed','_started','started','published'),
 'model_train.logical_batch.JointLearner':('policy','optimizer','loss','roots','on_prepared','on_replayed'),
 'input_history.SourceHistoryInputs':(),
 'input_100ms.MarketInputs100ms':(),
 'input_books.PmBookInputs':(), 'input_sides.SideInputs':(),
 'input_binance.BinanceInputs':(), 'input_reference.ReferenceInputs':(),
 'input_lifecycle.LifecycleInputs':(), 'input_events.EventInputs':(),
 'input_trades.TradeInputs':(), 'input_rules.RuleInputs':(),
 'fact_window.PublicFactWindow':(), 'fact_window.PublicTradeWindow':(),
 'fact_window.FactWindow':(), 'fact_window.TradeWindow':(),
 'public_state.PrefixColumn':(), 'public_state.PublicPmState':(), 'market_state.PublicMarketState':(),
 'reference_state.PublicReferenceState':(), 'binance_state.PublicBinanceState':(),
 'valuation_state.PublicValuationState':(), 'rules.PublicRuleGpu':(),
 'public_device_page.IdentityBank':('cache','decoded_records'),
 'public_device_page.RaggedRows':(), 'public_device_page._GroupMetadataField':(),
 'hidden_device_page.BankSlice':(),
}


def sha(path):
    h=hashlib.sha256()
    with Path(path).open('rb') as f:
        for chunk in iter(lambda:f.read(8*1024*1024),b''):h.update(chunk)
    return h.hexdigest()


def _key(cls):return cls.__module__+'.'+cls.__name__


def runtime_versions(*,tracking=False):
    names=('torch','torchrl','tensordict','triton','numpy')+ (('mlflow',) if tracking else ())
    return dict(python=platform.python_version(),sqlite=sqlite3.sqlite_version,**{name:version(name) for name in names})


def runtime_artifacts(codec=None):
    from queues import Numbers
    return dict(native=Path((codec or sys.modules['model_native_inputs']).__file__).resolve(),
        public_gpu=Path(Numbers.public_extension().__file__).resolve())


def checkpoint_compatibility(codec,*,tracking=False):
    """绑定当前实际运行模块/函数、构建输入和依赖；路径只用作当前定位。"""
    code=set(EXTENSION_SOURCES)|set(DEPENDENCY_LOCKS)|set(DEFERRED_SOURCES)
    for module in tuple(sys.modules.values()):
        filename=getattr(module,'__file__',None)
        if filename is None:continue
        path=Path(filename)
        # Torch classes等虚拟模块可有相对占位__file__，不是仓库源码。
        if not path.is_absolute():continue
        path=path.resolve()
        if path.suffix=='.py' and path.is_relative_to(SOURCE_ROOT):
            code.add(path.relative_to(SOURCE_ROOT).as_posix())
    return dict(mode=VERSION,code={p:sha(SOURCE_ROOT/p) for p in sorted(code)},
        versions=runtime_versions(tracking=tracking),artifacts={role:sha(path) for role,path in runtime_artifacts(codec).items()})


def verify_checkpoint_compatibility(compatibility,root,artifacts,versions):
    """同内容源码可换位置；审计source_map不参与恢复资格。"""
    if compatibility['mode']!=VERSION:raise ValueError('活动模式不兼容')
    for name,digest in compatibility['code'].items():
        if sha(Path(root)/name)!=digest:raise ValueError('实际运行源码/依赖改变: '+name)
    if compatibility['versions']!=versions:raise ValueError('实际运行依赖版本改变')
    for role,digest in compatibility['artifacts'].items():
        if sha(artifacts[role])!=digest:raise ValueError('实际运行制品改变: '+role)


def _class(key, sources):
    module,name=key.rsplit('.',1)
    if module=='actor_opening_ledger_platform':
        from actor_opening import load_actor_ledger_platform
        loaded=load_actor_ledger_platform()
    else:loaded=importlib.import_module(module)
    filename=Path(loaded.__file__).resolve().relative_to(SOURCE_ROOT).as_posix()
    if filename not in sources or sha(SOURCE_ROOT/filename)!=sources[filename]:
        raise ValueError('检查点类型不属于绑定源码: '+key)
    cls=getattr(loaded,name)
    if key not in OWNERS and not is_dataclass(cls):raise TypeError('非受限状态类型: '+key)
    return cls


class StateSnapshot:
    """受限字段图转为Tensor/基础类型。节点引用保持原对象共享关系。"""
    def __init__(self,destination,policy):
        self.directory=Path(destination);self.policy=policy
        self.nodes=[];self.references={};self.storages={};self.storage_refs={}
        self.sources={};self.members={};self.materials={}

    def source(self,cls):
        path=Path(sys.modules[cls.__module__].__file__).resolve()
        self.sources[str(path)]=sha(path)

    def encode(self,value):
        if value is None or type(value) in (bool,int,float,str):return value
        if value is self.policy:return {'policy':True}
        if isinstance(value,torch.device):return {'device':str(value)}
        if isinstance(value,torch.dtype):return {'dtype':str(value).removeprefix('torch.')}
        if isinstance(value,Path):return {'path':str(value)}
        if isinstance(value,bytes):return {'bytes':list(value)}
        if isinstance(value,np.generic):return self.encode(value.item())
        if id(value) in self.references:return {'ref':self.references[id(value)]}
        index=len(self.nodes);self.references[id(value)]=index;self.nodes.append(None)
        if isinstance(value,torch.Tensor):
            storage=value.untyped_storage();identity=(str(value.device),storage.data_ptr(),storage.nbytes())
            if identity not in self.storage_refs:
                sid=str(len(self.storages));self.storage_refs[identity]=sid
                raw=torch.empty(0,dtype=torch.uint8,device=value.device).set_(storage,0,(storage.nbytes(),),(1,))
                self.storages[sid]=raw.detach().to('cpu',copy=True)
            node=dict(kind='tensor',storage=self.storage_refs[identity],dtype=str(value.dtype).removeprefix('torch.'),
                shape=list(value.shape),stride=list(value.stride()),offset=value.storage_offset(),device=str(value.device))
        elif isinstance(value,np.memmap):
            base=value
            while isinstance(getattr(base,'base',None),np.memmap):base=base.base
            filename=str(Path(base.filename).resolve())
            if filename not in self.materials:self.materials[filename]=dict(bytes=Path(filename).stat().st_size,sha256=sha(filename))
            node=dict(kind='mmap',file=filename,dtype=value.dtype.str,shape=list(value.shape),strides=list(value.strides),
                offset=int(base.offset+value.ctypes.data-base.ctypes.data))
        elif isinstance(value,np.ndarray):
            node=dict(kind='array',dtype=value.dtype.str,shape=list(value.shape),values=value.tolist())
        elif type(value) is frozenset:
            node=dict(kind='frozenset',items=[self.encode(v) for v in value])
        elif isinstance(value,(tuple,list,dict)):
            node=dict(kind=type(value).__name__,items=[(self.encode(k),self.encode(v)) for k,v in value.items()]
                if isinstance(value,dict) else [self.encode(v) for v in value])
        else:
            cls=type(value);key=_key(cls)
            supported=key in OWNERS or is_dataclass(value) or key in (
                'input_history.SourceIdentityRegistry','public_device_page.PreparedPublicBlocks',
                'public_device_page.PreparedPublicPage','hidden_device_page.PreparedHiddenPages',
                'hidden_device_page.PreparedHiddenPartPage','hidden_inputs.HiddenPmBlock',
                'model_train.device_actor.CapturedDeviceActor')
            if not supported:raise TypeError('活动状态尚无所属导出接口: '+key)
            self.source(cls)
            if key=='input_history.SourceIdentityRegistry':
                name='identities-'+str(index)+'.sqlite3';value.backup(self.directory/name)
                self.members[name]=dict(bytes=(self.directory/name).stat().st_size,sha256=sha(self.directory/name))
                node=dict(kind='registry',file=name,markets=self.encode(value.markets))
            elif key in ('public_device_page.PreparedPublicBlocks','public_device_page.PreparedPublicPage'):
                node=dict(kind='public_material',type=key,directory=str(value.directory),material=str(value.material),
                    current=self.encode(value.current) if hasattr(value,'current') else None,
                    index=getattr(value,'current_index',None))
                for path in (value.directory/'manifest.json',value.material/'manifest.json'):
                    self.materials[str(path.resolve())]=dict(bytes=path.stat().st_size,sha256=sha(path))
            elif key=='hidden_device_page.PreparedHiddenPages':
                node=dict(kind='hidden_material',directory=str(value.directory),binding=self.encode(value.source_binding))
                path=value.directory/'manifest.json';self.materials[str(path.resolve())]=dict(bytes=path.stat().st_size,sha256=sha(path))
            elif key=='hidden_device_page.PreparedHiddenPartPage':
                node=dict(kind='hidden_page',catalog=self.encode(value.catalog),begin=value.begin,end=value.end)
            elif key=='hidden_inputs.HiddenPmBlock':
                node=dict(kind='hidden_block',page=self.encode(value.prepared_page))
            elif key=='model_train.device_actor.CapturedDeviceActor':
                node=dict(kind='published_driver',inputs=self.encode(value.inputs),random=self.encode(value.random),
                    words=value.word_capacity,capacity=value.capacity)
            elif is_dataclass(value):
                node=dict(kind='state',type=key,fields={f.name:self.encode(getattr(value,f.name)) for f in fields(value)})
            elif key in OWNERS:
                node=dict(kind='state',type=key,fields={k:self.encode(v) for k,v in vars(value).items() if k not in OWNERS[key]})
            else:raise TypeError('活动状态尚无所属导出接口: '+key)
        self.nodes[index]=node
        return {'ref':index}


class StateInstallation:
    def __init__(self,payload,directory,policy,device):
        self.payload=payload;self.directory=Path(directory);self.policy=policy;self.device=torch.device(device)
        # 原sources绝对路径仍留作审计；类型身份仅用其仓库相对路径及内容。
        recorded_root=Path(payload['source_binding']['root'])
        self.sources={Path(p).relative_to(recorded_root).as_posix():h for p,h in payload['sources'].items()}
        self.objects={};self.storages={};self.maps={}

    def decode(self,value):
        if not isinstance(value,dict):return value
        if 'policy' in value:return self.policy
        if 'device' in value:return self.device if value['device'].startswith('cuda') else torch.device(value['device'])
        if 'dtype' in value:return getattr(torch,value['dtype'])
        if 'path' in value:return Path(value['path'])
        if 'bytes' in value:return bytes(value['bytes'])
        index=value['ref']
        if index in self.objects:return self.objects[index]
        n=self.payload['nodes'][index];kind=n['kind']
        if kind=='tensor':
            sid=n['storage'];target=self.device if n['device'].startswith('cuda') else torch.device(n['device'])
            if sid not in self.storages:self.storages[sid]=self.payload['storages'][sid].to(target,copy=True)
            result=torch.empty(0,dtype=getattr(torch,n['dtype']),device=target).set_(
                self.storages[sid].untyped_storage(),n['offset'],tuple(n['shape']),tuple(n['stride']))
        elif kind=='mmap':
            if n['file'] not in self.maps:self.maps[n['file']]=np.memmap(n['file'],mode='r',dtype=np.uint8)
            result=np.ndarray(tuple(n['shape']),dtype=np.dtype(n['dtype']),buffer=self.maps[n['file']],
                offset=n['offset'],strides=tuple(n['strides']));result.flags.writeable=False
        elif kind=='array':result=np.array(n['values'],dtype=n['dtype']).reshape(n['shape'])
        elif kind in ('list','dict'):
            result=[] if kind=='list' else {};self.objects[index]=result
            if kind=='list':result.extend(self.decode(v) for v in n['items'])
            else:result.update((self.decode(k),self.decode(v)) for k,v in n['items'])
        elif kind=='tuple':result=tuple(self.decode(v) for v in n['items'])
        elif kind=='frozenset':result=frozenset(self.decode(v) for v in n['items'])
        elif kind=='registry':
            from input_history import SourceIdentityRegistry
            result=SourceIdentityRegistry.from_backup(self.directory/n['file'],self.decode(n['markets']))
        elif kind=='public_material':
            from public_device_page import PreparedPublicBlocks,PreparedPublicPage
            cls=PreparedPublicBlocks if n['type'].endswith('PreparedPublicBlocks') else PreparedPublicPage
            kwargs={} if cls is PreparedPublicBlocks else dict(_child=True)
            result=cls(n['directory'],n['material'],device=self.device,**kwargs)
            self.objects[index]=result
            if cls is PreparedPublicBlocks:result.current=self.decode(n['current']);result.current_index=n['index']
        elif kind=='hidden_material':
            from hidden_device_page import PreparedHiddenPages
            result=PreparedHiddenPages(n['directory'],self.decode(n['binding']),device=self.device)
        elif kind=='hidden_page':result=self.decode(n['catalog']).page(n['begin'],n['end'])
        elif kind=='hidden_block':
            from hidden_inputs import HiddenPmBlock
            result=HiddenPmBlock.from_prepared(self.decode(n['page']))
        elif kind=='published_driver':
            from .device_actor import CapturedDeviceActor
            result=CapturedDeviceActor.from_published(self.decode(n['inputs']),self.decode(n['random']),
                word_capacity=n['words'],path_capacity=n['capacity'])
        elif kind=='state':
            cls=_class(n['type'],self.sources);result=cls.__new__(cls);self.objects[index]=result
            if is_dataclass(cls) and set(n['fields'])!={f.name for f in fields(cls)}:raise ValueError('状态字段版本不一致')
            for name,item in n['fields'].items():object.__setattr__(result,name,self.decode(item))
            if n['type']=='public_device_page.IdentityBank':result.cache={};result.decoded_records=0
        else:raise ValueError('检查点成员模式不支持')
        self.objects[index]=result
        return result


def _sync_directory(path):
    fd=os.open(path,os.O_DIRECTORY)
    try:os.fsync(fd)
    finally:os.close(fd)


def save_joint_checkpoint(learner,directory,*,bindings,source_binding,final=False,progress=None,before_manifest=None):
    """调用方在共同update成功后、再次推进根之前保存；失败不发布latest。"""
    if any(b.states or b.prepared is not None or b._started for b in learner.roots):
        raise ValueError('检查点须在共同消费后的边界')
    directory=Path(directory);directory.mkdir(parents=True,exist_ok=True)
    name=('final-' if final else 'step-')+str(learner.consumed_batches)
    pending=Path(tempfile.mkdtemp(prefix='.'+name+'-',dir=directory))
    if progress:progress('checkpoint_snapshot_begin',version=int(learner.version[0]))
    torch.cuda.synchronize()
    snapshot=StateSnapshot(pending,learner.policy)
    roots=snapshot.encode(learner.roots)
    state=snapshot.encode(learner)
    source_binding=dict(source_binding,compatibility=checkpoint_compatibility(learner.roots[0].root.codec,tracking=learner.reporter is not None))
    payload=dict(version=VERSION,nodes=snapshot.nodes,storages=snapshot.storages,sources=snapshot.sources,
        roots=roots,learner=state,shape=vars(learner.policy.shape),policy_training=learner.policy.training,
        propagation=learner.policy.propagation,
        policy={k:v.detach().cpu().clone() for k,v in learner.policy.state_dict().items()},
        optimizer=learner.optimizer.state_dict(),optimizer_type=type(learner.optimizer).__name__,
        rng_cpu=torch.get_rng_state(),rng_cuda=[v.cpu().clone() for v in torch.cuda.get_rng_state_all()],
        bindings=bindings,source_binding=source_binding)
    # optimizer张量亦独立复制；不能留写出期间可能变化的设备/CPU页。
    def own(v):
        if isinstance(v,torch.Tensor):return v.detach().to('cpu',copy=True)
        if isinstance(v,dict):return {k:own(x) for k,x in v.items()}
        if isinstance(v,list):return [own(x) for x in v]
        if isinstance(v,tuple):return tuple(own(x) for x in v)
        return v
    payload['optimizer']=own(payload['optimizer'])
    file=pending/'state.pt'
    with file.open('xb') as out:torch.save(payload,out);out.flush();os.fsync(out.fileno())
    snapshot.members[file.name]=dict(bytes=file.stat().st_size,sha256=sha(file))
    for member in snapshot.members:
        with (pending/member).open('rb') as stream:os.fsync(stream.fileno())
    if progress:progress('checkpoint_members_written',members=len(snapshot.members),bytes=sum(m['bytes'] for m in snapshot.members.values()))
    if before_manifest is not None:before_manifest(pending)
    manifest=dict(version=VERSION,final=final,version_number=int(learner.version[0]),consumed_batches=learner.consumed_batches,
        members=snapshot.members,materials=snapshot.materials,source_binding=source_binding)
    with (pending/'manifest.json').open('x') as out:json.dump(manifest,out,sort_keys=True);out.flush();os.fsync(out.fileno())
    _sync_directory(pending);destination=directory/name
    os.rename(pending,destination);_sync_directory(directory)
    latest=directory/'.latest-new'
    with latest.open('w') as out:json.dump(dict(directory=name,manifest_sha256=sha(destination/'manifest.json')),out);out.flush();os.fsync(out.fileno())
    os.replace(latest,directory/'latest.json');_sync_directory(directory)
    complete=sorted((p for p in directory.glob('step-*') if (p/'manifest.json').is_file()),
        key=lambda p:int(p.name.split('-')[-1]))
    import shutil
    for old in complete[:-2]:shutil.rmtree(old)
    if final:
        for old in directory.glob('final-*'):
            if old!=destination:shutil.rmtree(old)
    if progress:progress('checkpoint_published',directory=str(destination))
    if learner.reporter is not None:learner.reporter.checkpoint(destination,learner.consumed_batches)
    return destination


def load_joint_checkpoint(directory,codec,binding_loader,*,device='cuda',progress=None):
    """新进程从完整成员恢复；不调用开户，不重apply来源或重选已发布动作。"""
    directory=Path(directory)
    if (directory/'latest.json').is_file():
        latest=json.loads((directory/'latest.json').read_text());directory=directory/latest['directory']
        if sha(directory/'manifest.json')!=latest['manifest_sha256']:raise ValueError('latest未绑定完整检查点')
    manifest=json.loads((directory/'manifest.json').read_text())
    if manifest['version']!=VERSION:raise ValueError('检查点版本错误')
    for base,entries in ((directory,manifest['members']),(None,manifest['materials'])):
        for name,entry in entries.items():
            path=Path(name) if base is None else base/name
            if path.stat().st_size!=entry['bytes'] or sha(path)!=entry['sha256']:raise ValueError('检查点/材料成员改变: '+name)
    if progress:progress('checkpoint_load_begin',directory=str(directory))
    payload=torch.load(directory/'state.pt',map_location='cpu',weights_only=True)
    binding=payload['source_binding']
    verify_checkpoint_compatibility(binding['compatibility'],SOURCE_ROOT,runtime_artifacts(codec),
        runtime_versions(tracking='mlflow' in binding['compatibility']['versions']))
    for path,digest in payload['sources'].items():
        relative=Path(path).relative_to(binding['root'])
        if sha(SOURCE_ROOT/relative)!=digest:raise ValueError('活动类型源码不匹配: '+str(relative))
    from .gpu_policy import Policy100ms,ModelShape100ms
    from .logical_batch import JointLearner
    policy=Policy100ms(ModelShape100ms(**payload['shape']),propagation=payload['propagation']).to(device)
    policy.load_state_dict(payload['policy'],strict=True);policy.train(payload['policy_training'])
    if payload['optimizer_type']!='AdamW':raise ValueError('当前活动优化器接口只交付AdamW')
    groups=payload['optimizer']['param_groups'];optimizer=torch.optim.AdamW(policy.parameters(),lr=groups[0]['lr'])
    optimizer.load_state_dict(payload['optimizer'])
    installation=StateInstallation(payload,directory,policy,device)
    saved=installation.decode(payload['learner']);learner=JointLearner(policy,optimizer,saved.config,saved.version,reporter=saved.reporter)
    learner.consumed_batches=saved.consumed_batches
    if learner.reporter is not None:learner.reporter.mark_restored(directory)
    batches=installation.decode(payload['roots']);learner.roots=batches
    for batch,spec in zip(batches,payload['bindings'],strict=True):
        batch.learner=learner;batch._started={};batch.on_replayed=None
        root=batch.root;root.learning=batch;root.codec=codec
        callbacks=binding_loader(spec,root)
        _reconnect(root,batch,codec,callbacks)
    torch.set_rng_state(payload['rng_cpu']);torch.cuda.set_rng_state_all(payload['rng_cuda'])
    if progress:progress('checkpoint_restored',roots=len(batches),version=int(learner.version[0]))
    return learner


def _reconnect(root,batch,codec,callbacks):
    from core import EvidenceBank
    from fifo_loop import FifoDeviceLoop
    from maker_loop import MakerDeviceLoop
    from hidden_reference_loop import HiddenReferenceDeviceLoop
    from public_group_loop import PublicGroupDeviceLoop
    root.tensor=lambda value,dtype=torch.int64:torch.tensor(value,dtype=dtype,device=root.started_at.device)
    root.inputs.account_builder=root.account_builder
    models=root.models;models.policy_at_start=batch.policy_at_start;models.timing=callbacks['timing']
    models.sender_factory=root.sender_factory;models.release_markets=root.inputs.release_markets
    models.on_started=None;models.on_completed=None
    models.start_frontier=lambda:torch.where(root.clock.gates.has_public_visible,root.clock.gates.public_visible_at,models.last_completed_at)
    clock=root.clock;clock.source.install_listeners=[root.inputs.install_public_block] if hasattr(clock.source.prepared,'parts') else []
    clock.public_loop=PublicGroupDeviceLoop()
    clock.bank=lambda projection:projection.normalizer.bank if projection.normalizer is not None else EvidenceBank.empty(root.started_at.device)
    backend=root.runtime.execution;backend.codec=codec;backend.fill_paths=callbacks['fill_paths']
    from maker_projection_loop import MakerProjectionDeviceLoop
    backend.fifo_loop=FifoDeviceLoop();backend.maker_loop=MakerDeviceLoop(device_feedback=True)
    backend.maker_projection_loop=MakerProjectionDeviceLoop()
    backend.reference_loop=HiddenReferenceDeviceLoop()
    from hidden_level_loop import HiddenLevelDeviceLoop
    backend.level_loop=HiddenLevelDeviceLoop()
    from hidden_transaction_loop import HiddenTransactionDeviceLoop
    backend.transaction_loop=HiddenTransactionDeviceLoop()
    from auto_redemption_loop import AutoRedemptionDeviceLoop
    backend.auto_loop=AutoRedemptionDeviceLoop()
    from public_trade_execution import PublicTradeExecution
    backend.public_trade=PublicTradeExecution()
    backend.queue_stage=None;backend.pending_queue_stage=None
    clock.handlers=backend.handlers()
    root.receiver.captured=None;root.receiver.progress=None
    root.run.freeze=root.freeze;root.run.receive=root.receive;root.run.initialize=root.initialize;root.run.settlement=None
    for task in models.tasks:
        if task.sender is not None:
            sender=task.sender;sender.codec=codec;sender.receive=root.receive;sender.dispatch.codec=codec
