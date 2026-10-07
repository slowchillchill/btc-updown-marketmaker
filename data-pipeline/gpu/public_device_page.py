"""一次构建的公共设备列页；原compact仍拥有词法、输入及完整来源旁证。

固定ABI只读little-endian数组和明确身份JSON，不恢复任意Python对象。
完整组顺序不变。当前writer处理一个已结束的材料；不签发运行交付前沿。
"""
import hashlib
import json
import math
from dataclasses import dataclass
from pathlib import Path
import subprocess
import numpy as np
import torch
from prepare import DeviceDecimalColumns, _numbers
from public_state import PrefixColumn, PublicPmState
from market_state import PublicMarketState
from reference_state import PublicReferenceState
from binance_state import PublicBinanceState


VERSION = 'public_device_page_v1'
PM_PREFIXES = ('quantities','selections','resets','initialized','ready','ticks',
    'level_anchors','book_quality','book_anchors','bba','trades','observed_ticks')
MARKET_PREFIXES = ('market_admitted','market_lifecycle','market_trading','market_terminal')
REFERENCE_PREFIXES = ('global_references','reference_rules','reference_official','reference_temporary')
BINANCE_PREFIXES = ('binance_quantities','binance_resets','binance_depth','binance_trades')
PREFIXES = PM_PREFIXES + MARKET_PREFIXES + REFERENCE_PREFIXES + BINANCE_PREFIXES
TABLES = {'slots':3,'market_assets':3,'groups':3,'bba_values':8,'trade_values':7,
    'observed_tick_values':4,'facts':4,'fact_sources':5,'raw_trades':11,
    'lifecycle_values':4,'trading_values':8,'terminal_values':5,'reference_values':4,
    'reference_rule_values':3,'reference_official_values':4,'reference_temporary_values':4,
    'binance_slots':3,'binance_depth_values':3,'binance_trade_values':5}
TABLES.update({name:4 for name in PREFIXES})  # 原来源行顺序也保留；预排列独立服务查询。
EXACTS = ('coefficient','decimal_denominator','numerator','denominator')
DERIVED = {'causal_visible_at':1,'anchor_times':2,'unique_anchor_ids':1,
    'condition_order':1,'asset_pairs':2,'market_times':2,'market_payouts':3}
DTYPES = {'i8':np.dtype('<i8'),'i4':np.dtype('<i4'),'i1':np.dtype('i1'),'u1':np.dtype('u1')}
BANKS = ('assets','markets','anchors','lifecycle_states','fact_records','group_ids',
    'group_members','fact_types','binance_symbols')
TEXT_BANKS = ('group_ids','fact_types','binance_symbols')


@dataclass(frozen=True)
class PublicPageBinding:
    """原reader核验后由publisher交接；调用方保留，不从待加载页推导expected。"""
    source_manifest_sha256: str
    page_manifest_sha256: str
    input_stream_id: str
    next_input_position: int


class IdentityBank:
    """共享UTF8 bank；加载不解析人口级正文，冷身份消费者按需解析并复用。"""
    def __init__(self,raw,offsets,*,text=False):
        self.raw,self.offsets,self.text=raw,offsets,text
        self.cache={}; self.decoded_records=0

    def __len__(self): return len(self.offsets)-1

    def bytes_at(self,index):
        return bytes(self.raw[self.offsets[index]:self.offsets[index+1]])

    def __getitem__(self,index):
        if isinstance(index,slice): return [self[i] for i in range(*index.indices(len(self)))]
        if index<0: index+=len(self)
        if not 0<=index<len(self): raise IndexError(index)
        if index not in self.cache:
            value=self.bytes_at(index).decode('utf8')
            self.cache[index]=value if self.text else json.loads(value)
            self.decoded_records+=1
        return self.cache[index]

    def __eq__(self,other):
        # 仅有限验证沿原build结果逐字节比对；不解包fact/source正文。
        if len(self)!=len(other): return False
        for index,value in enumerate(other):
            expected=value.encode('utf8') if self.text else _json_bytes(value)
            if self.bytes_at(index)!=expected: return False
        return True


class RaggedRows:
    """共享已验收values/offsets，不把人口级数字重装成Python列表。"""
    def __init__(self,values,offsets,width): self.values,self.offsets,self.width=values,offsets,width
    def __len__(self): return len(self.offsets)-1
    def __getitem__(self,index):
        if index<0: index+=len(self)
        if not 0<=index<len(self): raise IndexError(index)
        result=self.values[self.offsets[index]:self.offsets[index+1]]
        return result[:,0] if self.width==1 else result


def _sha(path):
    digest=hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda:stream.read(8*1024*1024),b''): digest.update(chunk)
    return digest.hexdigest()


def _json_bytes(value):
    return json.dumps(value,ensure_ascii=False,separators=(',',':')).encode('utf8')


def store_array_member(directory, members, name, array, kind):
    """public/hidden共用的固定类型成员；调用方负责来源绑定和业务列布局。"""
    array=np.asarray(array,dtype=DTYPES[kind],order='C')
    path=Path(directory)/(name+'.bin')
    with path.open('xb') as stream: stream.write(array.tobytes(order='C'))
    members[name]={'file':path.name,'dtype':kind,'shape':list(array.shape),
        'bytes':path.stat().st_size,'sha256':_sha(path)}


def load_array_members(directory, members):
    """只读typed成员；调用方必须先核验独立publisher绑定。"""
    arrays={}
    for name,entry in members.items():
        filename=entry['file']; shape=entry['shape']; kind=entry['dtype']
        if (not isinstance(filename,str) or Path(filename).name!=filename
                or filename in ('','.', '..') or kind not in DTYPES
                or not isinstance(shape,list) or any(type(n)!=int or n<0 for n in shape)):
            raise ValueError('设备页成员布局或路径错误')
        path=Path(directory)/filename; dtype=DTYPES[kind]
        if (path.is_symlink() or math.prod(shape)*dtype.itemsize!=entry['bytes']
                or path.stat().st_size!=entry['bytes'] or _sha(path)!=entry['sha256']):
            raise ValueError('设备页成员长度或摘要错误: '+filename)
        arrays[name]=(np.memmap(path,dtype=dtype,mode='r',shape=tuple(shape))
            if entry['bytes'] else np.empty(shape,dtype=dtype))
    return arrays


def build_public_device_page(binary, material, destination, *, device='cuda', progress=None):
    """原reader完成来源核验后仅一次解析精确数字/排序，保存最终typed列。"""
    material,destination=Path(material),Path(destination)
    source_manifest=(material/'manifest.json').read_bytes()
    columns=json.loads(subprocess.check_output([
        str(binary),'--pm-device-columns',str(material),'268435456']))
    if (material/'manifest.json').read_bytes()!=source_manifest:
        raise ValueError('构建期间原材料manifest改变')
    source=json.loads(source_manifest)
    if columns['input_stream_id']!=source['header']['input_stream_id'] or not source['normal_end']:
        raise ValueError('公共设备页必须绑定原完整输入流')
    if progress: progress('exact_and_prefix_build',groups=len(columns['groups']))
    pm=PublicPmState(columns,device=device)
    market=PublicMarketState(pm,columns)
    reference=PublicReferenceState(market,columns)
    binance=PublicBinanceState(pm,columns)
    # 不覆盖已发布或归属不明目录；manifest最后落盘，半写目录不能被加载。
    destination.mkdir(parents=True,exist_ok=False)
    members={}
    def store(name,array,kind):
        store_array_member(destination,members,name,array,kind)
    def tensor(name,value,kind='i8'): store(name,value.detach().cpu().numpy(),kind)
    for name,width in TABLES.items(): store(name,np.asarray(columns[name],dtype='<i8').reshape(-1,width),'i8')
    for names,state in ((PM_PREFIXES,pm),(MARKET_PREFIXES,market),
            (REFERENCE_PREFIXES,reference),(BINANCE_PREFIXES,binance)):
        for name in names:
            prefix=getattr(state,name)
            tensor(name+'.padded',prefix.padded); tensor(name+'.offsets',prefix.offsets)
    tensor('causal_visible_at',pm.causal_visible_at)
    tensor('anchor_times',pm.anchor_times); tensor('unique_anchor_ids',pm.unique_anchor_ids)
    tensor('condition_order',market.condition_order); tensor('asset_pairs',market.asset_pairs)
    tensor('market_times',market.times); tensor('market_payouts',market.payouts)
    exact_values=(pm.decimal_columns.coefficient,pm.decimal_columns.denominator,
        pm.decimals.numerator,pm.decimals.denominator)
    for name,value in zip(EXACTS,exact_values):
        tensor(name+'.words',value.words,'i4'); tensor(name+'.sign',value.sign,'i1')
        tensor(name+'.fault',value.fault,'i4')
    tensor('decimal_scale',pm.decimal_columns.scale,'i4')
    tensor('decimal_present',pm.decimal_columns.present,'u1')
    raw=bytearray(); offsets=[0]
    for value in columns['decimals']: raw.extend(value.encode('ascii')); offsets.append(len(raw))
    store('decimal_text',np.frombuffer(raw,dtype='u1'),'u1'); store('decimal_offsets',offsets,'i8')
    for name,width in (('anchor_sets',1),('terminal_payouts',2)):
        values=[]; offsets=[0]
        for row in columns[name]: values.extend(row); offsets.append(len(values))
        store(name+'.values',np.asarray(values,dtype='<i8').reshape(-1,width),'i8')
        store(name+'.offsets',offsets,'i8')
    for name in BANKS:
        raw=bytearray(); offsets=[0]
        for value in columns[name]:
            raw.extend(value.encode('utf8') if name in TEXT_BANKS else _json_bytes(value))
            offsets.append(len(raw))
        store(name+'.utf8',np.frombuffer(raw,dtype='u1'),'u1'); store(name+'.offsets',offsets,'i8')
    # 完整原身份与proof独立于数值等值；不按相同时间或值合并。
    omitted=set(TABLES)|set(PREFIXES)|set(BANKS)|{'decimals','anchor_sets','terminal_payouts'}
    metadata={key:value for key,value in columns.items() if key not in omitted}
    manifest={'version':VERSION,'members':members,'metadata':metadata,
        'source_manifest_sha256':hashlib.sha256(source_manifest).hexdigest(),
        'source_applied_prefix':source['applied_prefix'],'source_members':source['files']}
    with (destination/'manifest.json').open('xb') as stream: stream.write(_json_bytes(manifest))
    binding=PublicPageBinding(manifest['source_manifest_sha256'],_sha(destination/'manifest.json'),
        source['applied_prefix']['input_stream_id'],source['applied_prefix']['next_input_position'])
    if progress: progress('public_device_page_written',bytes=sum(p.stat().st_size for p in destination.iterdir()))
    return pm,market,reference,binance,columns,binding


class PreparedPublicPage:
    """验收页后复用预排数组与精确bank；所有根共享此不可变对象。"""
    def __init__(self,directory,material,*,binding,device='cuda'):
        self.directory=Path(directory); self.device=torch.device(device); self._tensors={}; self._prefixes={}
        manifest_bytes=(self.directory/'manifest.json').read_bytes()
        if (not isinstance(binding,PublicPageBinding)
                or hashlib.sha256(manifest_bytes).hexdigest()!=binding.page_manifest_sha256):
            raise ValueError('公共设备页与调用方已接纳publisher绑定不一致')
        manifest=json.loads(manifest_bytes)
        if manifest['version']!=VERSION: raise ValueError('公共设备页版本错误')
        original=(Path(material)/'manifest.json').read_bytes(); source=json.loads(original)
        if (hashlib.sha256(original).hexdigest()!=binding.source_manifest_sha256
                or binding.source_manifest_sha256!=manifest['source_manifest_sha256']
                or source['applied_prefix']['input_stream_id']!=binding.input_stream_id
                or source['applied_prefix']['next_input_position']!=binding.next_input_position
                or source['applied_prefix']!=manifest['source_applied_prefix']
                or source['files']!=manifest['source_members'] or not source['normal_end']):
            raise ValueError('公共设备页原材料绑定错误')
        self.columns=manifest['metadata']
        if self.columns['input_stream_id']!=source['header']['input_stream_id']:
            raise ValueError('公共设备页输入身份错误')
        expected=set(TABLES)|set(DERIVED)|{'decimal_scale','decimal_present','decimal_text','decimal_offsets'}
        expected|={name+'.'+field for name in PREFIXES for field in ('padded','offsets')}
        expected|={name+'.'+field for name in EXACTS for field in ('words','sign','fault')}
        expected|={name+'.'+field for name in ('anchor_sets','terminal_payouts') for field in ('values','offsets')}
        expected|={name+'.'+field for name in BANKS for field in ('utf8','offsets')}
        if set(manifest['members'])!=expected: raise ValueError('公共设备页列人口错误')
        self.arrays=load_array_members(self.directory,manifest['members'])
        for name,width in TABLES.items():
            value=self._array(name,'i8',2)
            if value.shape[1]!=width: raise ValueError('公共设备页表列宽错误')
            self.columns[name]=value
        for name in BANKS:
            raw=self._array(name+'.utf8','u1',1); offsets=self._offsets(name+'.offsets',len(raw))
            self.columns[name]=IdentityBank(raw,offsets,text=name in TEXT_BANKS)
        for name,ndim in DERIVED.items(): self._array(name,'i8',ndim)
        groups=self.arrays['groups']; count=len(groups)
        if (len(self.columns['group_ids'])!=count or len(self.columns['group_members'])!=count
                or len(self.columns['fact_records'])!=len(self.columns['facts'])
                or count!=source['groups'] or len(self.columns['facts'])!=source['facts']
                or (count and not np.array_equal(groups[:,0],np.arange(count)))):
            raise ValueError('公共设备页原组/事实身份人口错误')
        causal=self._array('causal_visible_at','i8',1)
        if not np.array_equal(causal,np.maximum.accumulate(groups[:,2])):
            raise ValueError('公共设备页因果可见列错误')
        anchors=self.columns['anchors']; markets=self.columns['markets']
        anchor_times=self.arrays['anchor_times']
        if (anchor_times.shape!=(len(anchors)+1,3) or np.any(anchor_times[0])
                or np.any((anchor_times[:,0]!=0)&(anchor_times[:,0]!=1))):
            raise ValueError('公共设备页锚点时间布局错误')
        if (self.arrays['asset_pairs'].shape!=(len(markets),2)
                or self.arrays['market_times'].shape!=(len(markets),2)
                or self.arrays['condition_order'].shape!=(len(markets),)):
            raise ValueError('公共设备页市场派生人口错误')
        pairs=np.full((len(markets),2),-1,dtype='<i8')
        for market,asset,outcome in self.arrays['market_assets']:
            if not (0<=market<len(markets) and 0<=asset<len(self.columns['assets']) and outcome>=0):
                raise ValueError('公共设备页市场资产绑定错误')
            if outcome<2 and pairs[market,outcome]==-1: pairs[market,outcome]=asset
        order=self.arrays['condition_order']
        if (len(set(order.tolist()))!=len(markets) or np.any((order<0)|(order>=len(markets)))
                or not np.array_equal(self.arrays['asset_pairs'],pairs)):
            raise ValueError('公共设备页市场派生绑定错误')
        # 原身份派生时间、condition字典序和双精确表示由独立publisher绑定覆盖；
        # 不为二次加载重复解包整个人口的身份JSON或重跑decimal/GCD。
        text=self._array('decimal_text','u1',1); offsets=self._offsets('decimal_offsets',len(text))
        self.columns['decimals']=IdentityBank(text,offsets,text=True)
        for name,width in (('anchor_sets',1),('terminal_payouts',2)):
            values=self._array(name+'.values','i8',2)
            if values.shape[1]!=width: raise ValueError('公共设备页ragged列宽错误')
            offsets=self._offsets(name+'.offsets',len(values))
            self.columns[name]=RaggedRows(values,offsets,width)
        unique=np.asarray([members[0] if len(members)==1 else -1 for members in self.columns['anchor_sets']],dtype='<i8')
        if not np.array_equal(self.arrays['unique_anchor_ids'],unique):
            raise ValueError('公共设备页直接锚点绑定错误')
        payouts=self.columns['terminal_payouts']; maximum=max(1,max(map(len,payouts),default=0))
        dense=self.arrays['market_payouts']
        if dense.shape!=(len(payouts),maximum,2): raise ValueError('公共设备页payout布局错误')
        for index,values in enumerate(payouts):
            if (not np.array_equal(dense[index,:len(values)],values)
                    or np.any(dense[index,len(values):,0]!=-1) or np.any(dense[index,len(values):,1]!=0)):
                raise ValueError('公共设备页payout成员绑定错误')
        for name in PREFIXES:
            padded=self._array(name+'.padded','i8',2)
            if padded.shape[1]!=4 or len(padded)<1 or np.any(padded[0]):
                raise ValueError('公共设备页prefix sentinel错误')
            rows=padded[1:]; offsets=self._offsets(name+'.offsets',len(rows))
            targets=self._targets(name)
            if len(offsets)!=targets+1: raise ValueError('公共设备页prefix目标人口错误')
            for target,(begin,end) in enumerate(zip(offsets[:-1],offsets[1:])):
                part=rows[begin:end]
                if (np.any(part[:,0]!=target) or np.any(np.diff(part[:,1])<0)
                        or np.any(np.diff(part[:,2])<0)):
                    raise ValueError('公共设备页prefix分段或原顺序错误')
            # 校验原行→已给offset段的稳定映射，O(N)，不重排或重算offset。
            next_row=np.array(offsets[:-1],copy=True)
            for row in self.arrays[name]:
                target=row[0]
                if (not 0<=target<targets or next_row[target]>=offsets[target+1]
                        or not np.array_equal(rows[next_row[target]],row)):
                    raise ValueError('公共设备页预排列与原事实行不一致: '+name)
                next_row[target]+=1
            if not np.array_equal(next_row,offsets[1:]):
                raise ValueError('公共设备页预排列原人口不一致: '+name)
        n=len(self.columns['decimals']); exact={}
        for name in EXACTS:
            words=self._array(name+'.words','i4',2); sign=self._array(name+'.sign','i1',1)
            fault=self._array(name+'.fault','i4',1)
            if (len(words)!=n or words.shape[1]<1 or sign.shape!=(n,) or fault.shape!=(n,)
                    or np.any((sign<-1)|(sign>1)) or np.any(fault!=0)
                    or np.any((sign==0)!=~np.any(words!=0,axis=1))):
                raise ValueError('公共设备页精确bank布局错误')
            if name in ('decimal_denominator','denominator') and np.any(sign!=1):
                raise ValueError('公共设备页精确分母错误')
            exact[name]=_numbers().ExactInt(self.tensor(name+'.words'),self.tensor(name+'.sign'),self.tensor(name+'.fault'))
        scale=self._array('decimal_scale','i4',1); present=self._array('decimal_present','u1',1)
        if scale.shape!=(n,) or present.shape!=(n,) or np.any(scale<0) or np.any(present!=1):
            raise ValueError('公共设备页decimal布局错误')
        self.decimal_columns=DeviceDecimalColumns(exact['coefficient'],exact['decimal_denominator'],
            self.tensor('decimal_scale'),self.tensor('decimal_present').bool())
        self.decimals=_numbers().ExactRational(exact['numerator'],exact['denominator'])

    def _array(self,name,kind,ndim):
        value=self.arrays[name]
        if value.dtype!=DTYPES[kind] or value.ndim!=ndim: raise ValueError('公共设备页列类型错误')
        return value

    def _offsets(self,name,total):
        value=self._array(name,'i8',1)
        if len(value)<1 or value[0]!=0 or value[-1]!=total or np.any(np.diff(value)<0):
            raise ValueError('公共设备页offset错误')
        return value

    def _targets(self,name):
        c=self.columns
        if name in ('quantities','selections','level_anchors'): return len(c['slots'])
        if name in PM_PREFIXES: return len(c['assets'])
        if name=='global_references': return 3
        if name in MARKET_PREFIXES+REFERENCE_PREFIXES: return len(c['markets'])
        if name=='binance_quantities': return len(c['binance_slots'])
        return len(c['binance_symbols'])

    def tensor(self,name):
        if name not in self._tensors:
            # mmap保持只读；pinned staging只在首次安装，共享设备页不重传。
            host=torch.from_numpy(np.array(self.arrays[name],copy=True)).pin_memory()
            self._tensors[name]=host.to(self.device,non_blocking=True)
        return self._tensors[name]

    def prefix(self,name):
        if name not in self._prefixes:
            self._prefixes[name]=PrefixColumn.from_prepared(self.tensor(name+'.padded'),self.tensor(name+'.offsets'))
        return self._prefixes[name]

    def install(self):
        pm=PublicPmState(self.columns,device=self.device,prepared=self)
        market=PublicMarketState(pm,self.columns,prepared=self)
        return pm,market,PublicReferenceState(market,self.columns,prepared=self),PublicBinanceState(pm,self.columns,prepared=self)
