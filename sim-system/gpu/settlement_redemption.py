"""原Terminal两腿payout与运行资产的冷绑定；不安装判定或生成经济后态。"""
from dataclasses import dataclass
import torch
from queues import Numbers
from asset_buy import take_ratio


@dataclass(frozen=True)
class RedemptionSources:
    columns: object
    assets: torch.Tensor  # [root,terminal,2]，原public_delta的token长度/字串序。
    tokens: torch.Tensor
    payouts: object
    fault: torch.Tensor

    @classmethod
    def bind(cls,columns,asset_tokens,asset_owners,token_slots,roots,*,device='cuda'):
        from prepare import decimal_columns
        raw,offsets=bytearray(),[0]
        assets,tokens,errors=[],[],[]
        for root in range(roots):
            for terminal in columns.terminals:
                vector=sorted(terminal['payout_vector'],key=lambda e:(len(e['asset']['token_id']),e['asset']['token_id']))
                error=int(len(vector)!=2)
                for entry in vector:
                    token=entry['asset']['token_id']
                    matches=[i for i,(candidate,owner) in enumerate(zip(asset_tokens,asset_owners,strict=True)) if owner==root and candidate==token]
                    index=matches[0] if len(matches)==1 else 0
                    error |= int(len(matches)!=1)
                    assets.append(index);tokens.append(token_slots[index])
                    raw.extend(entry['payout_pusd_per_share'].encode('ascii'));offsets.append(len(raw))
                errors.append(error)
        values=decimal_columns(torch.tensor(list(raw),dtype=torch.uint8).pin_memory().to(device),
            torch.tensor(offsets,dtype=torch.int64).pin_memory().to(device),
            torch.ones(len(assets),dtype=torch.bool,device=device)).rational()
        count=len(columns.terminals)
        fault=torch.tensor(errors,dtype=torch.int32,device=device).reshape(roots,count)
        fault |= Numbers.combine_fault_rows(values.fault.reshape(-1,2)).reshape(roots,count)
        return cls(columns,torch.tensor(assets,dtype=torch.int64,device=device).reshape(roots,count,2),
            torch.tensor(tokens,dtype=torch.int64,device=device).reshape(roots,count,2),values,fault)

    def gather(self,references):
        root=torch.arange(references.numel(),device=references.device)
        terminal=self.columns.fields[references.clamp(0,self.columns.fields.size(0)-1),2]
        safe=terminal.clamp(0,self.assets.size(1)-1)
        row=root*self.assets.size(1)+safe
        amounts=take_ratio(self.payouts,(row[:,None]*2+torch.arange(2,device=root.device)).flatten())
        return self.assets[root,safe],self.tokens[root,safe],amounts,self.fault[root,safe] | ((references<0)|(terminal<0)).to(torch.int32)
