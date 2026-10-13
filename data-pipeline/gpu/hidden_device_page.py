"""原已结束hidden材料一次构建typed列，按完整桶读取设备页。

复用public的成员codec及实际public／hidden来源关联。源身份bank保持不可变并按需
解码；页末只代表需要换入下一范围，不签发EOF或公开可见前沿。
"""
from pathlib import Path
import json
import subprocess
import bisect
import numpy as np
import torch
from public_device_page import (DTYPES, IdentityBank, _json_bytes, _sha,
    store_array_member, load_array_members)
from prepare import _numbers

VERSION='hidden_device_columns_v2'
PART_VERSION='hidden_device_columns_part_v2'
PAGES_VERSION='hidden_device_pages_v2'
TABLES={'event_indices':(15,2),'step_tags':(None,1),'snapshot_levels':(3,2),
    'snapshot_offsets':(None,1),'reference_rows':(12,2),'terminal_fields':(6,2),
    'terminal_payout_indices':(2,2),'level_assets':(None,1),'level_sides':(None,1),
    'asset_markets':(None,1),'slot_price_ids':(None,1),'operation_sources':(2,2),
    'bucket_times':(None,1),'step_offsets':(None,1),'reference_offsets':(None,1),
    'closure':(2,2)}
BANKS=('assets','slots','buckets','lifecycle_values','terminals','canonical_ids','decimal_texts','source_scene')
EXACTS=('numerator','denominator','index_starts','index_ends')
STATIC_MEMBERS={'asset_markets','level_assets','level_sides','slot_price_ids'}
STATIC_MEMBERS|={bank+'.'+field for bank in ('assets','slots','source_scene') for field in ('utf8','offsets')}


def build_hidden_device_columns(block,destination,source_binding,*,progress=None,shared=None,_part=False):
    """输入由HiddenPmBlock从原严格reader直接装箱。"""
    hidden=Path(source_binding['hidden']); public=Path(source_binding['public'])
    verified=block.source_verification
    public_sha=_sha(public/'manifest.json'); hidden_sha=_sha(hidden/'manifest.json')
    if (verified['source_manifest_sha256']!=public_sha
            or verified['material_manifest_sha256']!=hidden_sha):
        raise ValueError('hidden构建的来源绑定改变')
    destination=Path(destination); destination.mkdir(parents=True,exist_ok=False)
    members={}
    def store(name,value,kind='i8'):
        if shared is not None and name in STATIC_MEMBERS:
            entry=shared[1][name]
            (destination/entry['file']).hardlink_to(shared[0]/entry['file'])
            members[name]=entry
            return
        if isinstance(value,torch.Tensor): value=value.detach().cpu().numpy()
        store_array_member(destination,members,name,value,kind)
    for name in ('event_indices','step_tags','snapshot_levels','snapshot_offsets',
                 'reference_rows','level_assets','level_sides','asset_markets'):
        store(name,getattr(block,name))
    store('terminal_fields',block.terminal_columns.fields)
    store('terminal_payout_indices',block.terminal_columns.payout_indices)
    text_ids={text:index for index,text in enumerate(block.decimal_texts)}
    store('slot_price_ids',[text_ids[price] for _,_,price in block.slots])
    steps,references=[0],[0]; operation_sources=[]; closure=[]; starts=[]; ends=[]
    for index,value in enumerate(block.buckets):
        bucket=value['bucket']
        steps.append(steps[-1]+len(value['steps']))
        references.append(references[-1]+len(value['references']))
        operation_sources.extend((index,step) for step in range(len(value['steps'])))
        proof=bucket['closure']
        closure.append((1,0) if proof=='EndOfInput' else (0,proof['NextGlobalEventAt']))
        starts.append(bucket['index_start']); ends.append(bucket['index_end'])
    store('bucket_times',[value['bucket']['bucket_time_utc_ms'] for value in block.buckets])
    store('step_offsets',steps); store('reference_offsets',references)
    store('operation_sources',np.asarray(operation_sources).reshape(-1,2))
    store('closure',np.asarray(closure).reshape(-1,2))
    Numbers=_numbers()
    index_values=Numbers.run_exact_batch(lambda:(
        Numbers.ExactInt.from_values(starts,device=block.level_assets.device),
        Numbers.ExactInt.from_values(ends,device=block.level_assets.device)))
    for name,value in zip(EXACTS,(block.decimals.numerator,block.decimals.denominator,*index_values)):
        for field,kind in (('words','i4'),('sign','i1'),('fault','i4')):
            store(name+'.'+field,getattr(value,field),kind)
    banks=dict(assets=block.assets,slots=block.slots,buckets=block.buckets,
        lifecycle_values=block.lifecycle_values,terminals=block.terminal_columns.terminals,
        canonical_ids=block.terminal_columns.canonical_ids,decimal_texts=block.decimal_texts,
        source_scene=list(verified['source_scene'].items()))
    for name,values in banks.items():
        raw=bytearray(); offsets=[0]
        for value in values:
            raw.extend(value.encode('ascii') if name=='decimal_texts' else _json_bytes(value))
            offsets.append(len(raw))
        store(name+'.utf8',np.frombuffer(raw,dtype='u1'),'u1'); store(name+'.offsets',offsets)
    manifest={'version':PART_VERSION if _part else VERSION,'members':members,'public_manifest_sha256':public_sha,
        'hidden_manifest_sha256':hidden_sha,'source_prefix':json.loads((hidden/'manifest.json').read_bytes())['summary']['source_prefix'],
        'market_count':block.market_count,'buckets':len(block.buckets)}
    with (destination/'manifest.json').open('xb') as stream: stream.write(_json_bytes(manifest))
    if progress: progress('hidden_columns_written',bytes=sum(p.stat().st_size for p in destination.iterdir()))


class BankRange:
    """原完整身份的不可变范围；构造本范围不解析各事件正文。"""
    def __init__(self,bank,begin,end): self.bank,self.begin,self.end=bank,begin,end
    def __len__(self): return self.end-self.begin
    def __getitem__(self,index):
        if index<0: index+=len(self)
        if not 0<=index<len(self): raise IndexError(index)
        return self.bank[self.begin+index]


class PreparedOperations:
    """仅旧冷类型入口按需读正文；固定图直接使用typed tag/行和ragged列。"""
    def __init__(self,page): self.page=page
    def __len__(self): return self.page.step_end-self.page.step_begin
    def __getitem__(self,index):
        if index<0: index+=len(self)
        if not 0<=index<len(self): raise IndexError(index)
        page=self.page; catalog=page.catalog; absolute=page.step_begin+index
        bucket_index,step_index=catalog.arrays['operation_sources'][absolute]
        bucket=catalog.banks['buckets'][bucket_index]
        market,step=bucket['steps'][step_index]; tag,fields=next(iter(step['change'].items()))
        row=catalog.arrays['event_indices'][absolute]
        begin,end=catalog.arrays['snapshot_offsets'][absolute:absolute+2]
        return dict(tag=tag,fields=fields,step=step,market=market,
            source_time=bucket['bucket']['bucket_time_utc_ms'],asset=int(row[0]),
            quality=int(row[10]),required=bool(row[13]),snapshot_start=int(begin-page.snapshot_begin),
            snapshot_end=int(end-page.snapshot_begin),levels=[tuple(v) for v in catalog.arrays['snapshot_levels'][begin:end]])


class PreparedHiddenMaterial:
    """共享只读bank与成员映射；设备只上传所选完整桶页的步骤列。"""
    def __init__(self,directory,source_binding,*,device='cuda'):
        self._install(directory,source_binding,device=device,version=VERSION,next_bucket_at=None)

    @classmethod
    def _from_part(cls,directory,source_binding,*,device,next_bucket_at):
        result=cls.__new__(cls)
        result._install(directory,source_binding,device=device,version=PART_VERSION,next_bucket_at=next_bucket_at)
        return result

    def _install(self,directory,source_binding,*,device,version,next_bucket_at):
        self.directory=Path(directory); self.device=torch.device(device)
        data=(self.directory/'manifest.json').read_bytes()
        manifest=json.loads(data)
        if (manifest['version'] not in (version,version.replace('_v2','_v1'))
                or _sha(Path(source_binding['public'])/'manifest.json')!=manifest['public_manifest_sha256']
                or _sha(Path(source_binding['hidden'])/'manifest.json')!=manifest['hidden_manifest_sha256']):
            raise ValueError('hidden列页原材料绑定错误')
        original=json.loads((Path(source_binding['hidden'])/'manifest.json').read_bytes())
        if (manifest['source_prefix']!=original['summary']['source_prefix']
                or original['source_manifest_sha256']!=manifest['public_manifest_sha256']):
            raise ValueError('hidden列页完整来源前缀错误')
        expected=set(TABLES)|{name+'.'+field for name in EXACTS for field in ('words','sign','fault')}
        expected|={name+'.'+field for name in BANKS for field in ('utf8','offsets')}
        if set(manifest['members'])!=expected: raise ValueError('hidden列页成员人口错误')
        self.typed_version=manifest['version']
        self.members=manifest['members']
        self.arrays=load_array_members(self.directory,self.members); self._tensors={}
        self.banks={}; self.market_count=manifest['market_count']
        for name,(width,ndim) in TABLES.items():
            value=self.array(name,'i8',ndim)
            if name=='event_indices' and self.typed_version.endswith('_v1'):width=14
            if width is not None and value.shape[1]!=width: raise ValueError('hidden列页固定列宽错误')
        for name in BANKS:
            raw=self.array(name+'.utf8','u1',1); offsets=self.offsets(name+'.offsets',len(raw))
            self.banks[name]=IdentityBank(raw,offsets,text=name=='decimal_texts')
        count=len(self.arrays['bucket_times']); n=len(self.banks['decimal_texts'])
        if count!=manifest['buckets'] or len(self.banks['buckets'])!=count:
            raise ValueError('hidden列页原完整桶人口错误')
        self.offsets('step_offsets',len(self.arrays['event_indices']),count)
        self.offsets('reference_offsets',len(self.arrays['reference_rows'])-1,count)
        self.offsets('snapshot_offsets',len(self.arrays['snapshot_levels']),len(self.arrays['event_indices']))
        if np.any(np.diff(self.arrays['bucket_times'])<=0): raise ValueError('hidden完整桶时间顺序错误')
        closure=self.arrays['closure']
        if (closure.shape!=(count,2) or (count and (np.any(closure[:-1,0]!=0) or (closure[-1,0]!=1 if next_bucket_at is None else
                    closure[-1,0]!=0 or closure[-1,1]!=next_bucket_at)
                or np.any(closure[:-1,1]!=self.arrays['bucket_times'][1:])))):
            raise ValueError('hidden完整桶闭合凭据错误')
        rows=self.arrays['event_indices']; tags=self.arrays['step_tags']; levels=self.arrays['snapshot_levels']
        asset_count=len(self.banks['assets']); level_count=len(self.banks['slots'])
        references=len(self.arrays['reference_rows'])-1
        if (tags.shape!=(len(rows),) or np.any((tags<0)|(tags>(4 if self.typed_version.endswith('_v1') else 5)))
                or self.arrays['operation_sources'].shape!=(len(rows),2)
                or self.arrays['asset_markets'].shape!=(asset_count,)
                or self.arrays['level_assets'].shape!=(level_count,)
                or self.arrays['level_sides'].shape!=(level_count,)
                or self.arrays['slot_price_ids'].shape!=(level_count,)
                or self.arrays['terminal_fields'].shape!=(references+1,6)
                or self.arrays['terminal_payout_indices'].shape!=(references+1,2)):
            raise ValueError('hidden设备行/目录人口错误')
        def domain(value,upper):
            if np.any((value<0)|(value>=upper)): raise ValueError('hidden设备字典引用越界')
        domain(rows[:,2:8],n);
        if rows.shape[1]>14:domain(rows[:,14],n)
        domain(levels[:,1:],n); domain(self.arrays['slot_price_ids'],n)
        domain(rows[tags>0,0],asset_count); domain(self.arrays['level_assets'],asset_count)
        domain(self.arrays['asset_markets'],self.market_count); domain(self.arrays['level_sides'],2)
        domain(levels[:,0],2); domain(self.arrays['terminal_payout_indices'],n)
        reference_rows=self.arrays['reference_rows']
        if np.any((reference_rows[:-1,0]<1)|(reference_rows[:-1,0]>9)) or reference_rows[-1,0]!=0:
            raise ValueError('hidden参考类型或sentinel错误')
        domain(reference_rows[:-1,3],n)
        market_rows=reference_rows[:-1,0]>1
        domain(reference_rows[:-1,1][market_rows],self.market_count)
        terminal=self.arrays['terminal_fields']; active=terminal[:,2]>=0
        domain(terminal[active,2],len(self.banks['terminals'])); domain(terminal[active,3],len(self.banks['canonical_ids']))
        for field in (4,5):
            values=reference_rows[:-1,field]; domain(values[values>=0],len(self.banks['lifecycle_values']))
        exact={}
        for name in EXACTS:
            population=n if name in ('numerator','denominator') else count
            words=self.array(name+'.words','i4',2); sign=self.array(name+'.sign','i1',1); fault=self.array(name+'.fault','i4',1)
            if (len(words)!=population or words.shape[1]<1 or sign.shape!=(population,) or fault.shape!=(population,)
                    or np.any(fault!=0) or np.any((sign<-1)|(sign>1))
                    or np.any((sign==0)!=~np.any(words!=0,axis=1))
                    or (name=='denominator' and np.any(sign!=1))
                    or (name.startswith('index_') and (np.any(sign<0) or np.any(words[:,2:]!=0)))):
                raise ValueError('hidden精确字典或完整u64位置布局错误')
            exact[name]=_numbers().ExactInt(self.tensor(name+'.words'),self.tensor(name+'.sign'),self.tensor(name+'.fault'))
        self.decimals=_numbers().ExactRational(exact['numerator'],exact['denominator'])
        self.index_starts,self.index_ends=exact['index_starts'],exact['index_ends']
        if count>1:
            left=self.arrays['index_ends.words'][:-1]; right=self.arrays['index_starts.words'][1:]
            if not np.array_equal(left,right): raise ValueError('hidden完整桶原index前沿不连续')
        from hidden_resolution import TerminalColumns
        self.terminal_columns=TerminalColumns(self.tensor('terminal_fields'),self.tensor('terminal_payout_indices'),
            self.banks['terminals'],self.banks['canonical_ids'])

    def array(self,name,kind,ndim):
        value=self.arrays[name]
        if value.dtype!=DTYPES[kind] or value.ndim!=ndim: raise ValueError('hidden列页数组类型错误')
        return value

    def offsets(self,name,total,population=None):
        value=self.array(name,'i8',1)
        if (len(value)<1 or value[0]!=0 or value[-1]!=total or np.any(np.diff(value)<0)
                or population is not None and len(value)!=population+1):
            raise ValueError('hidden列页ragged边界错误')
        return value

    def tensor(self,name):
        if name not in self._tensors:
            self._tensors[name]=self.upload(self.arrays[name])
        return self._tensors[name]

    def upload(self,value):
        return torch.from_numpy(np.array(value,copy=True)).pin_memory().to(self.device,non_blocking=True)

    def index_start(self,begin,roots):
        from queues import _take
        return _take(self.index_starts,torch.full_like(roots,begin))

    def page(self,begin,end):
        if type(begin)!=int or type(end)!=int or not 0<=begin<end<=len(self.arrays['bucket_times']):
            raise ValueError('设备页必须是原连续完整桶范围')
        return PreparedHiddenPage(self,begin,end)


class PreparedHiddenPage:
    def __init__(self,catalog,begin,end):
        self.catalog,self.begin,self.end=catalog,begin,end
        self.step_begin,self.step_end=map(int,catalog.arrays['step_offsets'][[begin,end]])
        self.ref_begin,self.ref_end=map(int,catalog.arrays['reference_offsets'][[begin,end]])
        self.snapshot_begin,self.snapshot_end=map(int,catalog.arrays['snapshot_offsets'][[self.step_begin,self.step_end]])

    def install_block(self,block):
        """只上传本页步骤/快照/参考；固定目录/数值/Terminal只上传一次共享。"""
        c=self.catalog; arrays=c.arrays; device=c.device
        block.prepared_page=self; block.catalog=c
        block.buckets=BankRange(c.banks['buckets'],self.begin,self.end)
        block.assets=c.banks['assets']; block.slots=c.banks['slots']; block.market_count=c.market_count
        block.lifecycle_values=c.banks['lifecycle_values']; block.terminal_columns=c.terminal_columns
        block.decimals=c.decimals; block.decimal_fault=c.decimals.fault
        block.decimal_texts=c.banks['decimal_texts']; block.asset_axis=torch.arange(len(block.assets),device=device)
        for name in ('asset_markets','level_assets','level_sides'):
            setattr(block,name,c.tensor(name))
        block.prices=block._values(c.tensor('slot_price_ids')); block.price_fault=block.prices.fault
        block.event_indices=c.upload(arrays['event_indices'][self.step_begin:self.step_end])
        block.step_tags=c.upload(arrays['step_tags'][self.step_begin:self.step_end])
        block.snapshot_levels=c.upload(arrays['snapshot_levels'][self.snapshot_begin:self.snapshot_end])
        block.snapshot_offsets=c.upload(arrays['snapshot_offsets'][self.step_begin:self.step_end+1]-self.snapshot_begin)
        sentinel=arrays['reference_rows'][-1:]
        block.reference_rows=c.upload(np.concatenate((arrays['reference_rows'][self.ref_begin:self.ref_end],sentinel)))
        block.reference_tags=block.reference_rows[:,0]
        block.reference_ids=torch.cat((torch.arange(self.ref_begin,self.ref_end,device=device),
            torch.tensor([len(arrays['reference_rows'])-1],device=device)))
        block.operations=PreparedOperations(self)
        block.event_lookup=torch.cat((block.event_indices,torch.zeros((1,block.event_indices.shape[1]),dtype=torch.int64,device=device)))
        block.tag_lookup=torch.cat((block.step_tags,torch.zeros(1,dtype=torch.int64,device=device)))

    def bucket_columns(self,block):
        from hidden_frontier import HiddenBucketColumns
        from queues import _take
        c=self.catalog; index=torch.arange(self.begin,self.end,device=c.device)
        return HiddenBucketColumns(block,c.upload(c.arrays['bucket_times'][self.begin:self.end]),
            c.upload(c.arrays['step_offsets'][self.begin:self.end+1]-self.step_begin),
            c.upload(c.arrays['reference_offsets'][self.begin:self.end+1]-self.ref_begin),
            _take(c.index_ends,index),not self.end or bool(c.arrays['closure'][self.end-1,0]),self.begin,
            None if not self.end or c.arrays['closure'][self.end-1,0] else c.upload(c.arrays['closure'][self.end-1,1:2]))


def build_hidden_device_pages(assets,slots,destination,source_binding,*,buckets_per_part,
                              device='cuda',progress=None):
    """严格reader逐块装配；只保留当前完整桶块，正常结束后才发布总目录。

    数值和正文属于各块。资产、价格槽及原来源身份不变；跨块仍被结算引用的
    Terminal和lifecycle完整值使用共享目录，不为纯引用保存空Terminal设备行。
    """
    from hidden_inputs import HiddenPmBlock
    destination=Path(destination); destination.mkdir(parents=True,exist_ok=False)
    command=[source_binding['binary'],'--progress-interval-seconds',
        str(source_binding.get('progress_interval_seconds',10)),'--gpu-pm-pages',
        source_binding['hidden'],source_binding['public'],str(source_binding['rows']),
        str(source_binding['bytes']),str(source_binding['max_batch_bytes'])]
    parts=[]; lifecycle=[]; lifecycle_ids={}; terminals=[]; canonical_ids=[]
    terminal_fields=[]; terminal_payouts=[]; terminal_sources=[]
    terminal_ids={}; canonical_indices={}
    # 固定价格目录与所有既有Terminal payout使用共同词法前缀。事件数字仍只属于本块。
    decimal_prefix=list(dict.fromkeys(['0','1']+[price for _,_,price in slots]))
    shared=None; bucket_count=reference_count=0
    def flush(values,verified,times):
        nonlocal shared,bucket_count,reference_count
        for bucket in values:
            for fact in bucket['references']:
                terminal=fact['payload'].get('Inline',{}).get('MarketFact',{}).get('payload',{}).get('Terminal')
                if terminal is not None:
                    for entry in terminal['payout_vector']:
                        lexical=entry['payout_pusd_per_share']
                        if lexical not in decimal_prefix: decimal_prefix.append(lexical)
        block=HiddenPmBlock.__new__(HiddenPmBlock)
        block._install_verified(assets,slots,values,verified,device,decimal_prefix)
        local_lifecycle=[]
        for value in block.lifecycle_values:
            key=json.dumps(value,sort_keys=True,separators=(',',':'))
            if key not in lifecycle_ids:
                lifecycle_ids[key]=len(lifecycle); lifecycle.append(value)
            local_lifecycle.append(lifecycle_ids[key])
        mapping=[]
        fields=block.terminal_columns.fields.cpu().tolist()
        payouts=block.terminal_columns.payout_indices.cpu().tolist()
        for index,(row,payout) in enumerate(zip(fields[:-1],payouts[:-1],strict=True)):
            if row[2]<0:
                mapping.append(-1)
                continue
            mapping.append(len(terminal_fields))
            terminal_sources.append(reference_count+index)
            for field,bank,directory,value in ((2,terminals,terminal_ids,block.terminal_columns.terminals[row[2]]),
                    (3,canonical_ids,canonical_indices,block.terminal_columns.canonical_ids[row[3]])):
                key=json.dumps(value,sort_keys=True,separators=(',',':'))
                if key not in directory:
                    directory[key]=len(bank); bank.append(value)
                row[field]=directory[key]
            terminal_fields.append(row)
            terminal_payouts.append([])
            for lexical in (block.decimal_texts[index] for index in payout):
                if lexical not in decimal_prefix: decimal_prefix.append(lexical)
                terminal_payouts[-1].append(decimal_prefix.index(lexical))
        name=f'part-{len(parts):06d}'
        directory=destination/name
        build_hidden_device_columns(block,directory,source_binding,shared=shared,_part=True)
        manifest=json.loads((directory/'manifest.json').read_bytes())
        if shared is None: shared=(directory,manifest['members'])
        extra={}
        store_array_member(directory,extra,'terminal_mapping',mapping,'i8')
        store_array_member(directory,extra,'lifecycle_mapping',local_lifecycle,'i8')
        proof=values[-1]['bucket']['closure'] if values else 'EndOfInput'
        parts.append(dict(directory=name,begin=bucket_count,end=bucket_count+len(values),
            reference_begin=reference_count,reference_end=reference_count+len(mapping),
            manifest_sha256=_sha(directory/'manifest.json'),members=extra,
            next_bucket_at=None if proof=='EndOfInput' else proof['NextGlobalEventAt']))
        times.write(np.asarray([v['bucket']['bucket_time_utc_ms'] for v in values],dtype='<i8').tobytes())
        bucket_count+=len(values); reference_count+=len(mapping)
        if progress: progress('hidden_part_written',buckets=bucket_count,parts=len(parts),
            resident_buckets=len(values),resident_steps=len(block.operations))
    with (destination/'bucket_times.bin').open('xb') as times:
        with subprocess.Popen(command,stdout=subprocess.PIPE) as reader:
            try:
                verified=json.loads(next(reader.stdout)); last=None; values=[]
                for line in reader.stdout:
                    value=json.loads(line)
                    if 'source_scene' in value:
                        last=value
                    else:
                        values.append(value)
                        if len(values)>=buckets_per_part:
                            flush(values,verified,times); values=[]
                status=reader.wait()
                if status or last!=verified: raise ValueError('hidden严格原材料读取未完成')
                if values or not parts: flush(values,verified,times)
            except BaseException:
                reader.kill(); reader.wait()
                raise
    # 根目录在所有原桶／成员验收后发布；中间块末尾仍保留NextGlobalEventAt。
    members={}
    path=destination/'bucket_times.bin'
    members['bucket_times']=dict(file=path.name,dtype='i8',shape=[bucket_count],
        bytes=path.stat().st_size,sha256=_sha(path))
    store_array_member(destination,members,'terminal_fields',
        np.asarray(terminal_fields+[[0,0,-1,-1,3,0]]).reshape(-1,6),'i8')
    store_array_member(destination,members,'terminal_payout_indices',
        np.asarray(terminal_payouts+[[0,0]]).reshape(-1,2),'i8')
    store_array_member(destination,members,'terminal_source_indices',terminal_sources,'i8')
    manifest=dict(version=PAGES_VERSION,parts=parts,members=members,verification=verified,
        terminal_values=terminals,canonical_ids=canonical_ids,lifecycle_values=lifecycle,
        payout_decimal_texts=decimal_prefix,market_count=len(verified['source_scene']),
        buckets=bucket_count,references=reference_count)
    with (destination/'manifest.json').open('xb') as stream: stream.write(_json_bytes(manifest))


class PreparedHiddenPages:
    """完整根的只读物理块目录；当前块和仍被旧调用方引用的块各自拥有输入。"""
    def __init__(self,directory,source_binding,*,device='cuda'):
        from hidden_resolution import TerminalColumns
        self.directory=Path(directory); self.device=torch.device(device)
        self.source_binding=source_binding
        m=json.loads((self.directory/'manifest.json').read_bytes())
        self.manifest=m
        if (m['version'] not in (PAGES_VERSION,PAGES_VERSION.replace('_v2','_v1'))
            or m['verification']['source_manifest_sha256']!=_sha(Path(source_binding['public'])/'manifest.json')
            or m['verification']['material_manifest_sha256']!=_sha(Path(source_binding['hidden'])/'manifest.json')):
            raise ValueError('hidden完整块目录原材料绑定错误')
        if set(m['members'])!={'bucket_times','terminal_fields','terminal_payout_indices','terminal_source_indices'}:
            raise ValueError('hidden完整根成员人口错误')
        self.arrays=load_array_members(self.directory,m['members'])
        for name,width in (('bucket_times',None),('terminal_fields',6),
                ('terminal_payout_indices',2),('terminal_source_indices',None)):
            value=self.arrays[name]
            if value.dtype!=DTYPES['i8'] or value.ndim!=(1 if width is None else 2) or width is not None and value.shape[1]!=width:
                raise ValueError('hidden完整根成员类型错误')
        fields=self.arrays['terminal_fields']; payouts=self.arrays['terminal_payout_indices']
        sources=self.arrays['terminal_source_indices']
        if (not len(fields) or len(payouts)!=len(fields) or len(sources)!=len(fields)-1
            or fields[-1].tolist()!=[0,0,-1,-1,3,0] or payouts[-1].tolist()!=[0,0]
            or np.any((sources<0)|(sources>=m['references'])) or np.any(np.diff(sources)<=0)
            or np.any((fields[:-1,2]<0)|(fields[:-1,2]>=len(m['terminal_values'])))
            or np.any((fields[:-1,3]<0)|(fields[:-1,3]>=len(m['canonical_ids'])))
            or np.any((payouts<0)|(payouts>=len(m['payout_decimal_texts']))) ):
            raise ValueError('hidden完整根Terminal或来源坐标错误')
        self.parts=m['parts']; self.ends=[p['end'] for p in self.parts]; self.current=None; self.shared=None
        if not self.parts: raise ValueError('hidden完整根缺少原目录块')
        self.market_count=m['market_count']; self.lifecycle_values=m['lifecycle_values']
        self.terminal_columns=TerminalColumns(self.upload(self.arrays['terminal_fields']),
            self.upload(self.arrays['terminal_payout_indices']),m['terminal_values'],m['canonical_ids'])
        # 每块原member loader仍核验类型、身份域、u64位置和闭桶凭据。
        begin=reference=0
        for part in self.parts:
            if (part['begin']!=begin or part['reference_begin']!=reference
                or part['end']<begin or part['reference_end']<reference
                or (part['end']==begin and m['buckets']!=0)
                or Path(part['directory']).name!=part['directory']
                or _sha(self.directory/part['directory']/'manifest.json')!=part['manifest_sha256']):
                raise ValueError('hidden完整块范围或成员绑定错误')
            next_at=None if part['end']==m['buckets'] else int(self.arrays['bucket_times'][part['end']])
            if part['next_bucket_at']!=next_at:
                raise ValueError('hidden跨块闭合凭据错误')
            begin,reference=part['end'],part['reference_end']
        if (begin!=m['buckets'] or reference!=m['references']
            or len(self.arrays['bucket_times'])!=begin
            or (self.parts and self.parts[-1]['next_bucket_at'] is not None)):
            raise ValueError('hidden完整根结束或人口错误')

    def for_public_trade(self,destination,*,progress=None):
        """新冷模型从原已核compact来源取得v2；旧读面及来源文件不修改。"""
        if self.manifest['version']==PAGES_VERSION and all(
            json.loads((self.directory/p['directory']/'manifest.json').read_bytes())['version']==PART_VERSION
            for p in self.parts):
            return self
        # 只读原固定目录，不移动调用者的当前页／游标。原writer恢复完整规范步骤。
        first=self.parts[0]
        catalog=PreparedHiddenMaterial._from_part(self.directory/first['directory'],self.source_binding,
            device=self.device,next_bucket_at=first['next_bucket_at'])
        assets=[catalog.banks['assets'][i] for i in range(len(catalog.banks['assets']))]
        slots=[catalog.banks['slots'][i] for i in range(len(catalog.banks['slots']))]
        if progress is None:
            progress=lambda stage,**row:print(json.dumps(dict(stage=stage,**row)),flush=True)
        import time
        started=time.perf_counter()
        progress('hidden_typed_rebuild_begin',source=str(self.directory),destination=str(destination),
            buckets=self.manifest['buckets'])
        build_hidden_device_pages(assets,slots,destination,self.source_binding,
            buckets_per_part=max(1,max(p['end']-p['begin'] for p in self.parts)),
            device=self.device,progress=progress)
        result=PreparedHiddenPages(destination,self.source_binding,device=self.device)
        progress('hidden_typed_rebuild_complete',buckets=result.manifest['buckets'],
            parts=len(result.parts),seconds=time.perf_counter()-started)
        return result

    def upload(self,value):
        return torch.from_numpy(np.array(value,copy=True)).pin_memory().to(self.device,non_blocking=True)

    def part(self,begin):
        index=bisect.bisect_right(self.ends,begin) if self.ends[-1] else 0
        if self.current is None or self.current[0]!=index:
            p=self.parts[index]; directory=self.directory/p['directory']
            catalog=PreparedHiddenMaterial._from_part(directory,self.source_binding,device=self.device,
                next_bucket_at=p['next_bucket_at'])
            if set(p['members'])!={'terminal_mapping','lifecycle_mapping'}:
                raise ValueError('hidden完整块物理映射人口错误')
            if self.shared is None:
                self.shared=dict(members={name:catalog.members[name] for name in STATIC_MEMBERS},
                    banks={name:catalog.banks[name] for name in ('assets','slots','source_scene')},
                    tensors={name:catalog.tensor(name) for name in ('asset_markets','level_assets','level_sides','slot_price_ids')})
            elif any(catalog.members[name]!=self.shared['members'][name] for name in STATIC_MEMBERS):
                raise ValueError('hidden跨块固定身份或价位目录改变')
            catalog.banks.update(self.shared['banks']); catalog._tensors.update(self.shared['tensors'])
            mapping=load_array_members(directory,p['members'])
            terminal=mapping['terminal_mapping']; lifecycle=mapping['lifecycle_mapping']
            if (terminal.dtype!=DTYPES['i8'] or lifecycle.dtype!=DTYPES['i8']
                or terminal.shape!=(p['reference_end']-p['reference_begin'],)
                or lifecycle.shape!=(len(catalog.banks['lifecycle_values']),)
                or np.any((terminal<-1)|(terminal>=len(self.terminal_columns.fields)-1))
                or np.any((lifecycle<0)|(lifecycle>=len(self.lifecycle_values)))
                or len(catalog.arrays['bucket_times'])!=p['end']-p['begin']
                or not np.array_equal(catalog.arrays['bucket_times'],self.arrays['bucket_times'][p['begin']:p['end']])):
                raise ValueError('hidden完整块来源范围或物理映射错误')
            self.current=(index,catalog,mapping)
        return self.parts[index],self.current[1],self.current[2]

    def index_start(self,begin,roots):
        part,c,_=self.part(begin)
        return c.index_start(begin-part['begin'],roots)

    def page(self,begin,end):
        if type(begin)!=int or type(end)!=int or not (0<=begin<end<=len(self.arrays['bucket_times']) or begin==end==len(self.arrays['bucket_times'])==0):
            raise ValueError('设备页必须是原连续完整桶范围')
        p,c,mapping=self.part(begin)
        end=min(end,p['end'])
        page=c.page(begin-p['begin'],end-p['begin']) if end>begin else PreparedHiddenPage(c,0,0)
        return PreparedHiddenPartPage(self,page,p,mapping)


class PreparedHiddenPartPage:
    def __init__(self,catalog,page,part,mapping):
        self.catalog,self.page,self.part,self.mapping=catalog,page,part,mapping
        self.begin=part['begin']+page.begin; self.end=part['begin']+page.end

    def install_block(self,block):
        p=self.page; c=self.catalog
        p.install_block(block)
        block.catalog=c; block.prepared_page=self
        block.lifecycle_values=c.lifecycle_values; block.terminal_columns=c.terminal_columns
        rows=block.reference_rows.clone()
        life=c.upload(self.mapping['lifecycle_mapping'])
        for field in (4,5):
            ids=rows[:,field]
            if len(life): rows[:,field]=torch.where(ids>=0,life[ids.clamp_min(0)],ids)
        block.reference_rows=rows; block.reference_tags=rows[:,0]
        mapping=self.mapping['terminal_mapping'][p.ref_begin:p.ref_end]
        sentinel=len(c.terminal_columns.fields)-1
        block.reference_ids=c.upload(np.append(np.where(mapping<0,sentinel,mapping),sentinel))

    def bucket_columns(self,block):
        from dataclasses import replace
        return replace(self.page.bucket_columns(block),bucket_base=self.begin)
