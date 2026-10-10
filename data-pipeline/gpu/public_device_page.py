"""一次构建的公共设备列页；原compact仍拥有词法、输入及完整来源旁证。

固定ABI只读little-endian数组和明确身份JSON，不恢复任意Python对象。
完整组顺序不变。当前writer处理一个已结束的材料；不签发运行交付前沿。
"""
import hashlib
import json
import math
from pathlib import Path
import subprocess
import numpy as np
import torch
from prepare import DeviceDecimalColumns, _numbers
from public_state import PrefixColumn, PublicPmState
from market_state import PublicMarketState
from reference_state import PublicReferenceState
from binance_state import PublicBinanceState


VERSION = 'public_device_page_v3'
PREVIOUS_VERSION = 'public_device_page_v2'
LEGACY_VERSION = 'public_device_page_v1'
CHILD_VERSION = 'public_device_page_block_v1'
ROOT_VERSION = 'public_device_page_blocks_v1'
BASE_PM_PREFIXES = ('quantities','selections','resets','initialized','ready','ticks',
    'level_anchors','book_quality','book_anchors','bba','trades','observed_ticks')
from valuation_state import PREFIXES as VALUATION_PREFIXES
PM_PREFIXES = BASE_PM_PREFIXES + VALUATION_PREFIXES
MARKET_PREFIXES = ('market_admitted','market_lifecycle','market_trading','market_terminal')
REFERENCE_PREFIXES = ('global_references','reference_rules','reference_official','reference_temporary')
BINANCE_PREFIXES = ('binance_quantities','binance_resets','binance_depth','binance_trades')
PREFIXES = PM_PREFIXES + MARKET_PREFIXES + REFERENCE_PREFIXES + BINANCE_PREFIXES
BASE_PREFIXES = BASE_PM_PREFIXES + MARKET_PREFIXES + REFERENCE_PREFIXES + BINANCE_PREFIXES
BASE_TABLES = {'slots':3,'market_assets':3,'groups':3,'bba_values':8,'trade_values':7,
    'observed_tick_values':4,'facts':4,'fact_sources':5,'raw_trades':11,
    'lifecycle_values':4,'trading_values':8,'terminal_values':5,'reference_values':4,
    'reference_rule_values':3,'reference_official_values':4,'reference_temporary_values':4,
    'binance_slots':3,'binance_depth_values':3,'binance_trade_values':5}
TABLES = {**BASE_TABLES,'pm_versions':5}
EXACTS = ('coefficient','decimal_denominator','numerator','denominator')
BASE_DERIVED = {'causal_visible_at':1,'anchor_times':2,'unique_anchor_ids':1,
    'condition_order':1,'asset_pairs':2,'market_times':2,'market_payouts':3}
DERIVED = {**BASE_DERIVED,'valuation_price_order':1,'valuation_price_rank':1,'valuation_price_fault':1}
DTYPES = {'i8':np.dtype('<i8'),'i4':np.dtype('<i4'),'i1':np.dtype('i1'),'u1':np.dtype('u1')}
BASE_BANKS = ('assets','markets','anchors','lifecycle_states','group_ids',
    'group_members','fact_types','binance_symbols')
BANKS = BASE_BANKS + ('pm_epochs',)
TEXT_BANKS = ('group_ids','fact_types','binance_symbols','pm_epochs')


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


def _ordered_offsets(offsets,*,rows_per_segment=65536):
    """相邻原u64偏移逐段比较，段首保留上段末；不复制整份旁列。"""
    for begin in range(1,len(offsets),rows_per_segment):
        end=min(begin+rows_per_segment,len(offsets))
        if np.any(offsets[begin:end]<offsets[begin-1:end-1]):return False
    return True


def store_array_member(directory, members, name, array, kind):
    """public/hidden共用的固定类型成员；调用方负责来源绑定和业务列布局。"""
    array=np.asarray(array,dtype=DTYPES[kind],order='C')
    path=Path(directory)/(name+'.bin')
    with path.open('xb') as stream: stream.write(array.tobytes(order='C'))
    members[name]={'file':path.name,'dtype':kind,'shape':list(array.shape),
        'bytes':path.stat().st_size,'sha256':_sha(path)}


def load_array_members(directory, members):
    """只读typed成员；调用方核对实际来源、版本与消费列布局。"""
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
    return _write_public_columns(columns,source_manifest,destination,device=device,progress=progress)


def _write_public_columns(columns,source_manifest,destination,*,device,progress=None,child=False):
    source=json.loads(source_manifest)
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
    tensor('valuation_price_order',pm.valuation.price_order)
    tensor('valuation_price_rank',pm.valuation.price_rank)
    tensor('valuation_price_fault',pm.valuation.price_fault.to(torch.int64))
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
    manifest={'version':CHILD_VERSION if child else VERSION,'members':members,'metadata':metadata,
        'source_manifest_sha256':hashlib.sha256(source_manifest).hexdigest(),
        'source_applied_prefix':source['applied_prefix'],'source_members':source['files']}
    with (destination/'manifest.json').open('xb') as stream: stream.write(_json_bytes(manifest))
    if progress: progress('public_device_page_written',bytes=sum(p.stat().st_size for p in destination.iterdir()))
    return pm,market,reference,binance,columns


class PreparedPublicPage:
    """验收页后复用预排数组与精确bank；所有根共享此不可变对象。"""
    def __init__(self,directory,material,*,device='cuda',_child=False):
        self.directory=Path(directory); self.material=Path(material)
        self.device=torch.device(device); self._tensors={}; self._prefixes={}
        manifest_bytes=(self.directory/'manifest.json').read_bytes()
        manifest=json.loads(manifest_bytes)
        legacy=manifest['version']==LEGACY_VERSION
        current=manifest['version']==VERSION or (_child and manifest['version']==CHILD_VERSION)
        if manifest['version'] not in ((VERSION,PREVIOUS_VERSION,LEGACY_VERSION,CHILD_VERSION) if _child else (VERSION,PREVIOUS_VERSION,LEGACY_VERSION)): raise ValueError('公共设备页版本错误')
        tables = TABLES if current else BASE_TABLES
        prefixes = PREFIXES if current else BASE_PREFIXES
        banks = BANKS if current else BASE_BANKS
        derived = DERIVED if current else BASE_DERIVED
        original=(Path(material)/'manifest.json').read_bytes(); source=json.loads(original)
        if (hashlib.sha256(original).hexdigest()!=manifest['source_manifest_sha256']
                or source['applied_prefix']!=manifest['source_applied_prefix']
                or source['files']!=manifest['source_members'] or not source['normal_end']):
            raise ValueError('公共设备页原材料绑定错误')
        self.source_manifest_sha256=manifest['source_manifest_sha256']
        self.normal_end=source['normal_end']
        self.columns=manifest['metadata']
        if self.columns['version'] != ('public_pm_device_columns_v2' if current else 'public_pm_device_columns_v1'):
            raise ValueError('公共设备页与消费列语义版本不一致')
        if self.columns['input_stream_id']!=source['header']['input_stream_id']:
            raise ValueError('公共设备页输入身份错误')
        expected=set(tables)|set(derived)|{'decimal_scale','decimal_present','decimal_text','decimal_offsets'}
        expected|={name+'.'+field for name in prefixes for field in ('padded','offsets')}
        expected|={name+'.'+field for name in EXACTS for field in ('words','sign','fault')}
        expected|={name+'.'+field for name in ('anchor_sets','terminal_payouts') for field in ('values','offsets')}
        expected|={name+'.'+field for name in banks for field in ('utf8','offsets')}
        if legacy:
            expected|=set(BASE_PREFIXES)|{'fact_records.utf8','fact_records.offsets'}
        if set(manifest['members'])!=expected: raise ValueError('公共设备页列人口错误')
        self.arrays=load_array_members(self.directory,manifest['members'])
        for name,width in tables.items():
            value=self._array(name,'i8',2)
            if value.shape[1]!=width: raise ValueError('公共设备页表列宽错误')
            self.columns[name]=value
        for name in banks:
            raw=self._array(name+'.utf8','u1',1); offsets=self._offsets(name+'.offsets',len(raw))
            self.columns[name]=IdentityBank(raw,offsets,text=name in TEXT_BANKS)
        for name,ndim in derived.items(): self._array(name,'i8',ndim)
        groups=self.arrays['groups']; count=len(groups)
        if (len(self.columns['group_ids'])!=count or len(self.columns['group_members'])!=count
                or (not _child and (count!=source['groups'] or len(self.columns['facts'])!=source['facts']))
                or (count and not np.array_equal(groups[:,0],np.arange(self.columns.get('group_base',0),self.columns.get('group_base',0)+count)))):
            raise ValueError('公共设备页原组/事实身份人口错误')
        causal=self._array('causal_visible_at','i8',1)
        if not np.array_equal(causal,np.maximum.accumulate(np.maximum(groups[:,2],self.columns.get('causal_before',np.iinfo(np.int64).min)))):
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
        # 来源及成员完整性在冷加载核对；不重复解包整个人口身份或重跑decimal/GCD。
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
        for name in prefixes:
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
            # 唯一消费表示；现公共消费者继续检查序号、group、值域和来源引用。
            self.columns[name]=rows
        n=len(self.columns['decimals']); exact={}
        if current:
            order=self.arrays['valuation_price_order']; rank=self.arrays['valuation_price_rank']
            if (order.shape!=(n,) or rank.shape!=(n,) or len(set(order.tolist()))!=n
                    or np.any((order<0)|(order>=n)) or not np.array_equal(rank[order],np.arange(n))
                    or self.arrays['valuation_price_fault'].shape!=(n,)
                    or np.any(self.arrays['valuation_price_fault']!=0)):
                raise ValueError('公共设备页估值价格排序布局错误')
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

    def ensure_group_lineage(self,binary=None):
        """旧合格页按需从绑定的原compact补旁列；不改材料或既有页成员。"""
        names=('source_progress_changes','group_applied_prefixes')
        if all(name in self.columns for name in names):return
        if binary is None:
            raise ValueError('旧设备页缺原组来源旁列，须提供原compact导出入口')
        manifest=(self.material/'manifest.json').read_bytes()
        if hashlib.sha256(manifest).hexdigest()!=self.source_manifest_sha256:
            raise ValueError('补原组旁列时材料绑定改变')
        original=json.loads(subprocess.check_output([str(binary),'--pm-device-columns',str(self.material),'268435456']))
        if ((self.material/'manifest.json').read_bytes()!=manifest
                or original['input_stream_id']!=self.columns['input_stream_id']
                or not np.array_equal(np.asarray(original['groups'],dtype='<i8').reshape(-1,3),self.columns['groups'])
                or original['group_ids']!=list(self.columns['group_ids'])):
            raise ValueError('旧页与原compact完整组关联不一致')
        for name in names:self.columns[name]=original[name]

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
        if name=='current_pm_epoch': return len(c['markets'])
        if name in ('quantities','selections','level_anchors','selection_versions'): return len(c['slots'])
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


def _carry_public_projection(columns,previous):
    """只搬移原prefix最后选中值；保留原序号/组/版本，不生成新事实。"""
    if previous is None:return
    pm,market,reference,binance,old=previous
    memo={}; decimals={text:index for index,text in enumerate(columns['decimals'])}
    def copy(bank,index):
        key=(bank,int(index))
        if bank=='pm_versions' and index==0:return 0
        if key in memo:return memo[key]
        value=old[bank][index]
        if bank=='decimals':
            if value not in decimals:
                decimals[value]=len(columns[bank]);columns[bank].append(value)
            result=decimals[value]
        else:
            value=dict(value) if isinstance(value,dict) else list(value) if not isinstance(value,str) else value
            if bank=='anchor_sets': value=[copy('anchors',i) for i in value]
            elif bank=='terminal_payouts': value=[[a,copy('decimals',d)] for a,d in value]
            else:
                for field,dependency in dependencies.get(bank,()):value[field]=copy(dependency,value[field])
                if bank=='lifecycle_values' and value[0]:value[1]=copy('lifecycle_states',value[1])
            result=len(columns[bank]);columns[bank].append(value)
        memo[key]=result
        return result
    dependencies={
        'bba_values':((1,'decimals'),(3,'decimals'),(5,'decimals'),(6,'anchor_sets')),
        'trade_values':((0,'decimals'),(1,'decimals'),(4,'decimals'),(5,'anchor_sets')),
        'observed_tick_values':((1,'decimals'),(2,'anchor_sets')),
        'lifecycle_values':((2,'anchor_sets'),),'trading_values':((6,'anchor_sets'),),
        'terminal_values':((2,'terminal_payouts'),(3,'anchor_sets')),
        'reference_values':((0,'decimals'),(1,'anchor_sets')),
        'reference_rule_values':((1,'anchor_sets'),),
        'reference_official_values':((1,'decimals'),(2,'anchor_sets')),
        'reference_temporary_values':((1,'decimals'),(2,'anchor_sets')),
        'binance_depth_values':((1,'anchor_sets'),),
        'binance_trade_values':((0,'decimals'),(1,'decimals'),(3,'anchor_sets'))}
    value_banks={'quantities':'decimals','ticks':'decimals','level_anchors':'anchors','book_anchors':'anchors',
        'bba':'bba_values','trades':'trade_values','observed_ticks':'observed_tick_values',
        'selection_versions':'pm_versions','absence_versions':'pm_versions',
        'valuation_bba':'bba_values','valuation_bba_versions':'pm_versions',
        'market_lifecycle':'lifecycle_values','market_trading':'trading_values','market_terminal':'terminal_values',
        'global_references':'reference_values','reference_rules':'reference_rule_values',
        'reference_official':'reference_official_values','reference_temporary':'reference_temporary_values',
        'binance_quantities':'decimals','binance_depth':'binance_depth_values','binance_trades':'binance_trade_values'}
    for names,state in ((PM_PREFIXES,pm),(MARKET_PREFIXES,market),(REFERENCE_PREFIXES,reference),(BINANCE_PREFIXES,binance)):
        for name in names:
            prefix=getattr(state,name)
            counts=prefix.offsets[1:]-prefix.offsets[:-1]
            rows=prefix.rows[(prefix.offsets[1:][counts>0]-1)].cpu().tolist()
            if name in value_banks:
                for row in rows:row[3]=copy(value_banks[name],row[3])
            columns[name]=rows+columns[name]


def build_public_device_blocks(binary,material,destination,*,groups_per_block=256,device='cuda',progress=None):
    """顺序读当前完整组块；上块仅保留原已选择投影，写成独立内部typed子块。"""
    material,destination=Path(material),Path(destination)
    source_manifest=(material/'manifest.json').read_bytes()
    destination.mkdir(parents=True,exist_ok=False)
    raw=destination/'columns'
    subprocess.run([str(binary),'--pm-device-blocks',str(material),'268435456',str(raw),str(groups_per_block)],check=True)
    if (material/'manifest.json').read_bytes()!=source_manifest:raise ValueError('构建期间原材料manifest改变')
    root=json.loads((raw/'manifest.json').read_bytes())
    if root['version']!='public_pm_device_column_blocks_v1':raise ValueError('公共分块根版本错误')
    previous=None;causal_before=np.iinfo(np.int64).min;parts=[]
    for entry in root['parts']:
        path=raw/entry['directory']/'columns.json'
        if path.stat().st_size!=entry['bytes'] or _sha(path)!=entry['sha256']:raise ValueError('公共原列块损坏')
        block=json.loads(path.read_bytes())
        if (block['version']!='public_pm_device_column_block_v1'
                or block['group_begin']!=entry['group_begin'] or block['group_end']!=entry['group_end']):
            raise ValueError('公共原列块范围不符')
        columns=block['columns'];catalog=root['catalog']
        for name in ('assets','markets','market_assets','fact_types','binance_symbols','pm_epochs','reference_preparation_complete'):
            columns[name]=catalog[name]
        decimals={text:i for i,text in enumerate(columns['decimals'])}
        for name in ('slots','binance_slots'):
            slots=[]
            for asset,side,text in catalog[name]:
                if text not in decimals:decimals[text]=len(columns['decimals']);columns['decimals'].append(text)
                slots.append([asset,side,decimals[text]])
            columns[name]=slots
        columns.update(group_base=entry['group_begin'],group_count=entry['group_end'],causal_before=int(causal_before))
        _carry_public_projection(columns,previous)
        directory=destination/entry['directory']
        previous=_write_public_columns(columns,source_manifest,directory,device=device,progress=progress,child=True)
        causal_before=int(previous[0].causal_visible_at[-1].cpu())
        parts.append({**entry,'manifest_sha256':_sha(directory/'manifest.json')})
        # block正文只有当前和供下一块承接的上一投影；不收集全部块。
        del block,columns
    empty_state=None
    if not parts:
        columns=root['empty_columns']
        _write_public_columns(columns,source_manifest,destination/'empty-state',device=device,progress=progress,child=True)
        empty_state={'directory':'empty-state','manifest_sha256':_sha(destination/'empty-state'/'manifest.json')}
    manifest={**root,'version':ROOT_VERSION,'parts':parts,'empty_state':empty_state}
    with (destination/'manifest.json').open('xb') as stream:stream.write(_json_bytes(manifest))
    return PreparedPublicBlocks(destination,material,device=device)


class _GroupMetadataField:
    def __init__(self,raw,offsets,field):self.raw,self.offsets,self.field=raw,offsets,field
    def __len__(self):return len(self.offsets)-1
    def __getitem__(self,index):
        if isinstance(index,slice):return [self[i] for i in range(*index.indices(len(self)))]
        if index<0:index+=len(self)
        if not 0<=index<len(self):raise IndexError(index)
        return json.loads(bytes(self.raw[self.offsets[index]:self.offsets[index+1]]))[self.field]


class PreparedPublicBlocks:
    """全根身份及原位置冷旁证；设备仅安装当前块，旧冻结持有旧块自己的值。"""
    def __init__(self,directory,material,*,device='cuda'):
        self.directory,self.material=Path(directory),Path(material);self.device=torch.device(device)
        root=json.loads((self.directory/'manifest.json').read_bytes())
        if root['version']!=ROOT_VERSION:raise ValueError('公共分块根版本错误')
        original=(self.material/'manifest.json').read_bytes();source=json.loads(original)
        if (root['input_stream_id']!=source['header']['input_stream_id']
                or hashlib.sha256(original).hexdigest()!=root['source_manifest_sha256']
                or root['source_applied_prefix']!=source['applied_prefix'] or root['source_members']!=source['files']
                or not source['normal_end'] or root['groups']!=source['groups'] or root['facts']!=source['facts']):
            raise ValueError('公共分块根来源绑定错误')
        self.source_manifest_sha256=root['source_manifest_sha256'];self.normal_end=source['normal_end']
        self.parts=root['parts'];end=0
        for entry in self.parts:
            name=entry['directory']
            if Path(name).name!=name or name in ('','.', '..') or entry['group_begin']!=end or entry['group_end']<=end:
                raise ValueError('公共分块根范围/路径错误')
            if _sha(self.directory/name/'manifest.json')!=entry['manifest_sha256']:raise ValueError('公共子块manifest损坏')
            end=entry['group_end']
        if end!=root['groups']:raise ValueError('公共分块根缺失原完整组')
        raw=self.directory/'columns'
        expected={'group_metadata.utf8','group_metadata.offsets.u64le','causal_visible_at.i64le'}
        if len(root['files'])!=len(expected) or {entry['name'] for entry in root['files']}!=expected:
            raise ValueError('公共分块原旁列成员不完整')
        for entry in root['files']:
            path=raw/entry['name']
            if Path(entry['name']).name!=entry['name'] or path.stat().st_size!=entry['bytes'] or _sha(path)!=entry['sha256']:
                raise ValueError('公共分块原旁列损坏')
        count=root['groups']
        offsets=np.memmap(raw/'group_metadata.offsets.u64le',dtype='<u8',mode='r')
        text=(np.memmap(raw/'group_metadata.utf8',dtype='u1',mode='r')
            if (raw/'group_metadata.utf8').stat().st_size else np.empty(0,dtype='u1'))
        if len(offsets)!=count+1 or offsets[0]!=0 or offsets[-1]!=len(text) or not _ordered_offsets(offsets):
            raise ValueError('公共分块组旁列布局错误')
        self.causal=(np.memmap(raw/'causal_visible_at.i64le',dtype='<i8',mode='r',shape=(count,))
            if count else np.empty(0,dtype='<i8'))
        groups=(np.memmap(self.material/'group_columns.i64le',dtype='<i8',mode='r',shape=(count,5))[:,:3]
            if count else np.empty((0,3),dtype='<i8'))
        self.columns={**root['catalog'],'input_stream_id':root['input_stream_id'],'groups':groups,
            'group_ids':_GroupMetadataField(text,offsets,'group_id'),'group_members':_GroupMetadataField(text,offsets,'members'),
            'source_progress_changes':_GroupMetadataField(text,offsets,'progress'),
            'group_applied_prefixes':_GroupMetadataField(text,offsets,'applied_prefix')}
        self.current=None;self.current_index=None
        self.empty_state=root.get('empty_state')

    def ensure_group_lineage(self,binary=None):return

    def load(self,index):
        entry=self.parts[index]
        child=PreparedPublicPage(self.directory/entry['directory'],self.material,device=self.device,_child=True)
        if (child.columns['group_base']!=entry['group_begin'] or child.columns['group_count']!=entry['group_end']
                or len(child.columns['groups'])!=entry['group_end']-entry['group_begin']
                or not np.array_equal(child.columns['groups'],self.columns['groups'][entry['group_begin']:entry['group_end']])
                or not np.array_equal(child.arrays['causal_visible_at'],self.causal[entry['group_begin']:entry['group_end']])):
            raise ValueError('公共子块与全根原组关联错误')
        self.current_index,self.current=index,child
        return child

    def install(self):
        if not self.parts:
            entry=self.empty_state
            if (entry is None or entry['directory']!='empty-state'
                    or _sha(self.directory/entry['directory']/'manifest.json')!=entry['manifest_sha256']):
                raise ValueError('公共零组原状态缺失或损坏')
            if self.current is None:
                self.current=PreparedPublicPage(self.directory/entry['directory'],self.material,device=self.device,_child=True)
                if len(self.current.columns['groups']) or len(self.current.columns['facts']):
                    raise ValueError('公共零组状态人口错误')
            return self.current.install()
        return (self.current or self.load(0)).install()
