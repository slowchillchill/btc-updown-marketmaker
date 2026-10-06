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
