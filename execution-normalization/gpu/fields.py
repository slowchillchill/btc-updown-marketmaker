"""强类型设备字段的无损等值编码；不生成JSON或概率哈希。

整数只保存设备有效字，精确有理保存已规范化分子/分母。物理容量增长和尾补齐
不会改变同值字段编码；字段类型与顺序是所属载荷schema的组成部分。
"""
import torch


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

    def integer(value):
        words=value.words.to(torch.int64)&0xffffffff
        positions=torch.arange(words.size(1),device=words.device)
        length=torch.where(words!=0,positions+1,0).max(dim=1).values
        append(torch.stack([torch.full_like(length,2),value.sign.to(torch.int64),length],dim=1))
        append(words,positions[None,:]<length[:,None])

    for value in fields:
        if isinstance(value,torch.Tensor):
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
    def integer():
        nonlocal cursor,fault
        tag=read(cursor)
        sign=read(cursor+1)
        words=read(cursor+2)
        invalid=(tag!=2)|(sign<-1)|(sign>1)|(words<0)|(words>width-cursor-3)
        safe=words.clamp(0,width)
        fault=fault | invalid.to(torch.int32)
        capacity=Numbers.device_word_capacity()
        # 只有合法字段长度才提出真实资源需求；别让非本schema/损坏tokens扩池。
        valid=enabled & (fault==0)
        Numbers.record_resource_request(torch.where(valid,words,0))
        positions=torch.arange(capacity,device=encoded.device)
        coordinates=cursor[:,None]+3+positions[None,:]
        raw=encoded.gather(1,coordinates.clamp(0,width-1))
        present=positions[None,:]<safe[:,None]
        fault=fault | ((present & ((raw<0)|(raw>0xffffffff))).any(dim=1)).to(torch.int32)
        tail=read((cursor+2+safe).clamp(1,width-1))
        fault=fault | (((safe==0)&(sign!=0))|((safe>0)&((sign==0)|(tail==0)))).to(torch.int32)
        capacity_fault=torch.where(enabled & (words>capacity),2,0).to(torch.int32)
        value=ExactInt(torch.where(present & enabled[:,None],raw,0).to(torch.int32),
            torch.where(enabled,sign,0).to(torch.int8),torch.where(enabled,fault,0)|capacity_fault)
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
        else:
            raise ValueError('未知NOE固定字段schema')
    fault=fault | (cursor!=length+1).to(torch.int32)
    return values,torch.where(enabled,fault,0)
