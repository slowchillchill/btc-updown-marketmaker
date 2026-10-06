"""强类型设备字段的无损等值编码；不生成JSON或概率哈希。

整数只保存设备有效字，精确有理保存已规范化分子/分母。物理容量增长和尾补齐
不会改变同值字段编码；字段类型与顺序是所属载荷schema的组成部分。
"""
import torch
from dataclasses import dataclass


@dataclass(frozen=True)
class IntegerList:
    values: object  # ExactInt[rows*capacity]，原列表顺序；尾补齐不属于正文。
    present: torch.Tensor

    @property
    def fault(self):
        from queues import Numbers
        return Numbers.combine_fault_rows(torch.where(self.present,self.values.fault.reshape_as(self.present),0))


@dataclass(frozen=True)
class RecordList:
    fields: tuple  # 每列[row*capacity]，成员字段保留原强类型顺序。
    present: torch.Tensor

    @property
    def fault(self):
        from queues import Numbers
        result=torch.zeros_like(self.present,dtype=torch.int32)
        for value in self.fields:
            if not isinstance(value,torch.Tensor):
                result=result | torch.where(self.present,value.fault.reshape_as(self.present),0)
        return Numbers.combine_fault_rows(result)


def take_field(value,index):
    from queues import _take,ExactRational
    if isinstance(value,torch.Tensor):
        return value[index]
    if hasattr(value,'numerator'):
        return ExactRational(_take(value.numerator,index),_take(value.denominator,index))
    return _take(value,index)


def encode_fields(fields):
    if not fields:
        raise ValueError('设备字段schema不能为空')
    values=[]
    masks=[]
    batch=None
    device=None

    def append(value,mask=None):
        nonlocal batch,device
        if batch is None:
            batch=value.size(0)
            device=value.device
        if value.ndim!=2 or value.size(0)!=batch or value.device!=device or value.dtype!=torch.int64:
            raise TypeError('设备字段需同人口、同设备int64列')
        values.append(value)
        masks.append(torch.ones_like(value,dtype=torch.bool) if mask is None else mask)

    def integer(value,present=None):
        words=value.words.to(torch.int64)&0xffffffff
        positions=torch.arange(words.size(1),device=words.device)
        length=torch.where(words!=0,positions+1,0).max(dim=1).values
        header=torch.stack([torch.full_like(length,2),value.sign.to(torch.int64),length],dim=1)
        append(header,None if present is None else present[:,None].expand_as(header))
        valid=positions[None,:]<length[:,None]
        append(words,valid if present is None else valid & present[:,None])

    for value in fields:
        if isinstance(value,RecordList):
            rows,capacity=value.present.shape
            if value.present.dtype!=torch.bool:
                raise TypeError('记录列表需要bool[row,capacity]实际人口')
            for member in value.fields:
                count=member.numel() if isinstance(member,torch.Tensor) else member.fault.numel()
                member_device=member.device if isinstance(member,torch.Tensor) else member.fault.device
                if count!=rows*capacity or member_device!=value.present.device:
                    raise ValueError('记录列表字段人口/设备不符')
            population=value.present.to(torch.int64).sum(dim=1)
            append(torch.stack([torch.full_like(population,5),population],dim=1))
            for column in range(capacity):
                index=torch.arange(rows,device=value.present.device)*capacity+column
                encoded=encode_fields([take_field(member,index) for member in value.fields])
                tokens=torch.arange(encoded.size(1),device=encoded.device)[None,:]
                append(encoded,value.present[:,column,None] & (tokens<=encoded[:,0,None]))
        elif isinstance(value,IntegerList):
            from queues import _take
            rows,capacity=value.present.shape
            if value.present.dtype!=torch.bool or value.present.device!=value.values.words.device or value.values.sign.numel()!=rows*capacity:
                raise TypeError('整数列表须同设备bool[row,capacity]人口及原顺序精确值')
            population=value.present.to(torch.int64).sum(dim=1)
            append(torch.stack([torch.full_like(population,4),population],dim=1))
            for column in range(capacity):
                integer(_take(value.values,torch.arange(rows,device=value.present.device)*capacity+column),value.present[:,column])
        elif isinstance(value,torch.Tensor):
            flat=value.reshape(-1)
            if flat.dtype not in (torch.int64,torch.bool,torch.int8,torch.int32):
                raise TypeError('标量字段必须是精确整数或bool')
            append(torch.stack([torch.full_like(flat,1,dtype=torch.int64),flat.to(torch.int64)],dim=1))
        elif hasattr(value,'numerator'):
            append(torch.full((value.numerator.sign.numel(),1),3,dtype=torch.int64,device=value.numerator.words.device))
            integer(value.numerator)
            integer(value.denominator)
        else:
            integer(value)
    body=torch.cat(values,dim=1)
    present=torch.cat(masks,dim=1)
    positions=present.to(torch.int64).cumsum(dim=1)-1
    packed=torch.zeros_like(body).scatter_add(1,positions.clamp_min(0),
        torch.where(present,body,torch.zeros_like(body)))
    # 长度也是schema：显式缺失/容器边界等必须作为字段输入，不借零padding推断。
    return torch.cat([present.to(torch.int64).sum(dim=1)[:,None],packed],dim=1)


def append_rows(original,incoming):
    """编码已去除无效高位；增长后仅尾补零即可共享旧字段人口。"""
    width=max(original.size(1),incoming.size(1))
    return torch.cat([torch.nn.functional.pad(original,(0,width-original.size(1))),
        torch.nn.functional.pad(incoming,(0,width-incoming.size(1)))],dim=0)


def decode_fields(encoded,schema,enabled):
    """Ledger沿固定原载荷schema读取共享无损字段，不回传CPU或重新生成业务事实。

    schema元素为scalar/integer/rational。只对enabled原记录解析；其它类型的行为
    中性占位。整数容量由公共精确批协调，不截去真实高字或量化金额。
    """
    from dataclasses import dataclass
    from queues import ExactInt,ExactRational,Numbers
    if encoded.ndim!=2 or encoded.dtype!=torch.int64 or enabled.shape!=(encoded.size(0),) or enabled.dtype!=torch.bool or enabled.device!=encoded.device:
        raise TypeError('共享NOE解码需要同设备int64[row,token]与bool[row]')
    count,width=encoded.shape
    if width<1:
        raise ValueError('共享NOE编码缺少字段长度')
    length=encoded[:,0]
    cursor=torch.ones(count,dtype=torch.int64,device=encoded.device)
    fault=((length<0)|(length>=width)).to(torch.int32)
    def read(index):
        nonlocal fault
        outside=(index<1)|(index>=width)|(index>length)
        fault=fault | outside.to(torch.int32)
        return encoded.gather(1,index.clamp(0,width-1)[:,None]).flatten()
    def integer(row_enabled=None):
        nonlocal cursor,fault
        tag=read(cursor)
        sign=read(cursor+1)
        words=read(cursor+2)
        invalid=(tag!=2)|(sign<-1)|(sign>1)|(words<0)|(words>width-cursor-3)
        safe=words.clamp(0,width)
        fault=fault | invalid.to(torch.int32)
        capacity=Numbers.device_word_capacity()
        # 只有合法字段长度才提出真实资源需求；别让非本schema/损坏tokens扩池。
        active=enabled if row_enabled is None else enabled & row_enabled
        valid=active & (fault==0)
        Numbers.record_resource_request(torch.where(valid,words,0))
        positions=torch.arange(capacity,device=encoded.device)
        coordinates=cursor[:,None]+3+positions[None,:]
        raw=encoded.gather(1,coordinates.clamp(0,width-1))
        present=positions[None,:]<safe[:,None]
        fault=fault | ((present & ((raw<0)|(raw>0xffffffff))).any(dim=1)).to(torch.int32)
        tail=read((cursor+2+safe).clamp(1,width-1))
        fault=fault | (((safe==0)&(sign!=0))|((safe>0)&((sign==0)|(tail==0)))).to(torch.int32)
        capacity_fault=torch.where(active & (words>capacity),2,0).to(torch.int32)
        value=ExactInt(torch.where(present & active[:,None],raw,0).to(torch.int32),
            torch.where(active,sign,0).to(torch.int8),torch.where(active,fault,0)|capacity_fault)
        cursor=cursor+3+safe
        return value
    values=[]
    for kind in schema:
        if kind=='scalar':
            tag=read(cursor)
            value=read(cursor+1)
            fault=fault | (tag!=1).to(torch.int32)
            values.append(torch.where(enabled,value,0))
            cursor=cursor+2
        elif kind=='integer':
            values.append(integer())
        elif kind=='rational':
            tag=read(cursor)
            fault=fault | (tag!=3).to(torch.int32)
            cursor=cursor+1
            numerator,denominator=integer(),integer()
            fault=fault | (denominator.sign<=0).to(torch.int32)
            neutral=denominator.constant(1)
            values.append(ExactRational(numerator,denominator.select(enabled,neutral)))
        elif isinstance(kind,tuple) and kind[0]=='record_list':
            from queues import _columns
            tag=read(cursor)
            population=read(cursor+1)
            # 成员至少需要其长度token及非空schema，不设业务人口上限。
            minima={'scalar':2,'integer':3,'rational':7,'integer_list':2}
            minimum=1+sum(2 if isinstance(field,tuple) else minima[field] for field in kind[1])
            capacity=max(1,width//minimum)
            fault=fault | ((tag!=5)|(population<0)|(population>capacity)).to(torch.int32)
            cursor=cursor+2
            members=[]
            masks=[]
            for column in range(capacity):
                live=(column<population) & enabled
                previous_fault=fault
                size=read(cursor)
                coordinates=cursor[:,None]+torch.arange(width,device=encoded.device)[None,:]
                member=encoded.gather(1,coordinates.clamp(0,width-1))
                decoded,error=decode_fields(member,kind[1],live)
                invalid=(size<0)|(size>=width)|(cursor+size>length)
                fault=torch.where(live,fault | error | invalid.to(torch.int32),previous_fault)
                members.append(decoded)
                masks.append(live)
                cursor=cursor+torch.where(live,size.clamp(0,width)+1,0)
            packed=[]
            for field in range(len(kind[1])):
                entries=[member[field] for member in members]
                if isinstance(entries[0],torch.Tensor):
                    packed.append(torch.stack(entries,dim=1).flatten())
                elif hasattr(entries[0],'numerator'):
                    packed.append(ExactRational(_columns([entry.numerator for entry in entries]),
                        _columns([entry.denominator for entry in entries])))
                else:
                    packed.append(_columns(entries))
            values.append(RecordList(tuple(packed),torch.stack(masks,dim=1)))
        elif kind=='integer_list':
            from queues import _columns
            tag=read(cursor)
            population=read(cursor+1)
            # 每个合法整数至少占三个token；这个界来自本编码成员容量而非业务域。
            capacity=max(1,width//3)
            fault=fault | ((tag!=4)|(population<0)|(population>capacity)).to(torch.int32)
            cursor=cursor+2
            entries=[]
            present=[]
            for column in range(capacity):
                live=(column<population) & enabled
                previous_fault,previous_cursor=fault,cursor
                entries.append(integer(live))
                fault=torch.where(live,fault,previous_fault)
                cursor=torch.where(live,cursor,previous_cursor)
                present.append(live)
            values.append(IntegerList(_columns(entries),torch.stack(present,dim=1)))
        else:
            raise ValueError('未知NOE固定字段schema')
    fault=fault | (cursor!=length+1).to(torch.int32)
    return values,torch.where(enabled,fault,0)
