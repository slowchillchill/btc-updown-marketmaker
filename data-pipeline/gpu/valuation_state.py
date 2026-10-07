"""公共Core valuation_bid的设备读面；原到达BBA与盘口列不改语义。"""
from dataclasses import dataclass
import torch
from prepare import _numbers, decimal_price_order


PREFIXES = ('current_pm_epoch','selection_versions','absence_versions',
            'valuation_bba','valuation_bba_versions')


@dataclass(frozen=True)
class PublicValuationBid:
    price_ids: torch.Tensor
    value_present: torch.Tensor  # Some(None)仍为已观察的空买侧。
    bid_present: torch.Tensor
    quality: torch.Tensor
    prefix_fault: torch.Tensor
    fault: torch.Tensor


def _at_least(left, right):
    """完整u64先hi再lo；Option member的None=-1先于Some。"""
    greater = torch.zeros_like(left[...,0],dtype=torch.bool)
    equal = torch.ones_like(greater)
    for field in (1,2,3,4):
        greater = greater | (equal & (left[...,field] > right[...,field]))
        equal = equal & (left[...,field] == right[...,field])
    return (left[...,0] == right[...,0]) & (greater | equal)


class PublicValuationState:
    def __init__(self, pm, columns, *, prepared=None):
        self.pm = pm
        self.complete = columns['version'] == 'public_pm_device_columns_v2'
        if not self.complete:
            return  # 旧页继续原消费者；不从anchors或latest猜估值版本。
        from public_state import PrefixColumn
        versions, epochs = columns['pm_versions'], columns['pm_epochs']
        if not len(versions) or list(versions[0]) != [-1,0,0,0,-1]:
            raise ValueError('公共估值版本sentinel错误')
        if len(set(epochs)) != len(epochs) or any(not isinstance(e,str) or not e for e in epochs):
            raise ValueError('公共估值epoch身份错误')
        for epoch, source, high, low, member in versions[1:]:
            if not (0 <= epoch < len(epochs) and -(1<<63) <= source < (1<<63)
                    and 0 <= high <= 0xffffffff and 0 <= low <= 0xffffffff
                    and -1 <= member <= 0xffffffff):
                raise ValueError('公共估值原版本坐标错误')
        self.versions = (torch.tensor(versions,dtype=torch.int64,device=pm.slots.device).reshape(-1,5)
                         if prepared is None else prepared.tensor('pm_versions'))
        assets, slots = len(pm.assets), len(pm.slots)
        bindings = [-1]*assets
        for market,asset,_ in columns['market_assets']:
            if not (0 <= market < len(pm.markets) and 0 <= asset < assets) or bindings[asset] not in (-1,market):
                raise ValueError('公共估值市场资产绑定错误')
            bindings[asset] = market
        if any(market < 0 for market in bindings):
            raise ValueError('公共估值资产缺实际市场绑定')
        self.asset_markets = torch.tensor(bindings,dtype=torch.int64,device=pm.slots.device)
        for name in PREFIXES:
            targets = len(pm.markets) if name == 'current_pm_epoch' else slots if name == 'selection_versions' else assets
            previous = {}
            bound = len(epochs) if name == 'current_pm_epoch' else len(columns['bba_values']) if name == 'valuation_bba' else len(versions)
            for target, sequence, group, value in columns[name]:
                if not (0 <= target < targets and 0 <= sequence < columns['original_pm_steps']
                        and 0 <= group < pm.group_count and 0 <= value < bound):
                    raise ValueError('公共估值消费列位置或版本引用越界')
                if name in ('selection_versions','valuation_bba_versions') and value == 0:
                    raise ValueError('公共估值真实选择缺原版本')
                prior = previous.get(target,(-1,-1))
                if sequence < prior[0] or group < prior[1]:
                    raise ValueError('公共估值消费列原顺序倒退')
                previous[target] = (sequence,group)
            # 两个BBA候选列共用实际坐标；GPU冷归约后只持久化选中前缀。
            if name not in ('valuation_bba','valuation_bba_versions'):
                setattr(self,name,PrefixColumn(columns[name],targets,pm.slots.device)
                        if prepared is None else prepared.prefix(name))
        if prepared is None:
            bba = columns['valuation_bba']; refs = columns['valuation_bba_versions']
            if len(bba) != len(refs) or any(list(a[:3]) != list(b[:3]) for a,b in zip(bba,refs)):
                raise ValueError('公共估值BBA与版本原坐标不一致')
            rows = torch.tensor(refs,dtype=torch.int64,device=pm.slots.device).reshape(-1,4)
            values = torch.tensor(bba,dtype=torch.int64,device=pm.slots.device).reshape(-1,4)
            order = torch.argsort(rows[:,0],stable=True)
            rows, values = rows[order], values[order]
            version = self.versions[rows[:,3]]
            rank_order = torch.arange(len(versions),device=pm.slots.device)
            for field in (4,3,2,1):
                rank_order = rank_order[torch.argsort(self.versions[rank_order,field],stable=True)]
            ranks = torch.empty_like(rank_order)
            ranks[rank_order] = torch.arange(rank_order.numel(),device=rank_order.device)
            if len(rows):
                # 每资产、连续同epoch来源段内取完整版本前沿；原序相等仍更新来源。
                start = torch.ones(len(rows),dtype=torch.bool,device=rows.device)
                start[1:] = (rows[1:,0] != rows[:-1,0]) | (version[1:,0] != version[:-1,0])
                segments = start.to(torch.int64).cumsum(0)-1
                if len(rows)*len(versions) >= (1<<63):
                    raise ValueError('公共估值冷归约坐标容量不足')
                score = segments*len(versions)+ranks[rows[:,3]]
                frontier = torch.cummax(score,dim=0).values
                accepted = score == frontier
                rows, values = rows[accepted], values[accepted]
            self.valuation_bba = PrefixColumn.from_device_rows(values,assets)
            self.valuation_bba_versions = PrefixColumn.from_device_rows(rows,assets)
            order, self.price_fault = decimal_price_order(pm.decimal_columns,
                torch.arange(len(columns['decimals']),device=pm.slots.device))
            self.price_order = order
            self.price_rank = torch.empty_like(order)
            self.price_rank[order] = torch.arange(order.numel(),device=order.device)
        else:
            self.valuation_bba = prepared.prefix('valuation_bba')
            self.valuation_bba_versions = prepared.prefix('valuation_bba_versions')
            if (not torch.equal(self.valuation_bba.padded[:,:3],self.valuation_bba_versions.padded[:,:3])):
                raise ValueError('公共估值BBA与版本原坐标不一致')
            self.price_order = prepared.tensor('valuation_price_order')
            self.price_rank = prepared.tensor('valuation_price_rank')
            self.price_fault = prepared.tensor('valuation_price_fault').to(torch.int32)

    def query(self, prefixes, *, frame=None):
        pm = self.pm
        roots, assets = prefixes.numel(), len(pm.assets)
        shape = (roots,assets)
        zero = torch.zeros(shape,dtype=torch.int64,device=prefixes.device)
        no = torch.zeros_like(zero,dtype=torch.bool)
        invalid = ((prefixes < -1) | (prefixes >= pm.group_count)).to(torch.int32)
        if not self.complete:
            return PublicValuationBid(zero,no,no,zero+3,invalid,invalid[:,None].expand(shape))
        frame = pm.frame(prefixes) if frame is None else frame
        _, bids, bfound, bfault = self.valuation_bba.query(prefixes)
        _, direct_ids, dfound, dfault = self.valuation_bba_versions.query(prefixes)
        _, epoch, efound, efault = self.current_pm_epoch.query(prefixes)
        epoch, efound, efault = epoch[:,self.asset_markets], efound[:,self.asset_markets], efault[:,self.asset_markets]
        value = pm.bba_values[bids]
        direct = self.versions[direct_ids]
        direct_valid = dfound & bfound & (direct_ids != 0) & efound & (direct[...,0] == epoch)
        # L2不Fresh时沿原独立来源BBA；不读取未来点位或把空买侧当缺失。
        l2 = frame.ready & (frame.quality == 0)
        pseq, points, pfound, pfault = self.selection_versions.query(prefixes)
        aseq, baselines, afound, afault = self.absence_versions.query(prefixes)
        point = self.versions[points]
        baseline = self.versions[baselines]
        reset, _, reset_found, rfault = pm.resets.query(prefixes)
        reset = torch.where(reset_found,reset,-1)
        slot_asset = pm.slots[:,0]
        point_valid = pfound & (points != 0) & (pseq >= reset[:,slot_asset]) & efound[:,slot_asset] & (point[...,0] == epoch[:,slot_asset])
        baseline_valid = afound & (baselines != 0) & (aseq >= reset) & efound & (baseline[...,0] == epoch)
        bp = baseline[:,slot_asset]
        bv = baseline_valid[:,slot_asset]
        use_baseline = bv & (~point_valid | _at_least(bp,point))
        selected = torch.where(use_baseline[...,None],bp,point)
        selected_valid = point_valid | bv
        direct_at_slot = direct[:,slot_asset]
        direct_at_slot_valid = direct_valid[:,slot_asset]
        newer = selected_valid & ~_at_least(direct_at_slot,selected)
        different = (selected != direct_at_slot).any(dim=-1)
        quantity_positive = frame.present & (frame.quantity.numerator.sign.reshape(roots,-1) > 0)
        bid_slot = pm.slots[:,1] == 0
        same_price = pm.slots[:,2][None] == value[:,slot_asset,1]
        deleted = (bid_slot[None] & same_price & value[:,slot_asset,0].bool()
            & selected_valid & different & _at_least(selected,direct_at_slot) & ~quantity_positive)
        removed = torch.zeros(shape,dtype=torch.int64,device=prefixes.device)
        index = slot_asset[None].expand(roots,-1)
        removed.scatter_reduce_(1,index,deleted.to(torch.int64),reduce='amax',include_self=True)
        positive_at_quote = torch.zeros_like(removed)
        positive_at_quote.scatter_reduce_(1,index,(bid_slot[None] & same_price & quantity_positive).to(torch.int64),
            reduce='amax',include_self=True)
        # 报价价位未曾成为共享L2槽位时，真实全簿absence也能撤销该买价。
        baseline_removed = (baseline_valid & value[:,:,0].bool() & ~positive_at_quote.bool()
            & (baseline != direct).any(dim=-1) & _at_least(baseline,direct))
        removed = (removed.bool() | baseline_removed) & direct_valid
        ranks = self.price_rank[pm.slots[:,2]]
        direct_rank = self.price_rank[value[:,:,1]][:,slot_asset]
        equal = ranks[None] == direct_rank
        lower = ranks[None] < direct_rank
        permitted = ~direct_at_slot_valid | torch.where(value[:,slot_asset,0].bool() & (lower | equal),
            removed[:,slot_asset] | (equal & newer),newer)
        candidates = quantity_positive & bid_slot[None] & permitted
        best = torch.full(shape,-1,dtype=torch.int64,device=prefixes.device)
        best.scatter_reduce_(1,index,torch.where(candidates,ranks[None],-1),reduce='amax',include_self=True)
        best_present = best >= 0
        choose_l2 = l2 & (~direct_valid | best_present | removed)
        prices = torch.where(choose_l2,self.price_order[best.clamp_min(0)],value[:,:,1])
        bid_present = torch.where(choose_l2,best_present,bfound & value[:,:,0].bool())
        known = choose_l2 | bfound
        quality = torch.where(choose_l2,frame.quality,torch.where(bfound,value[:,:,-1],3))
        fault = bfault | dfault | afault | rfault | efault | invalid[:,None]
        fault = fault | _numbers().combine_fault_rows(frame.fault | pfault)[:,None]
        fault = fault | self.price_fault[prices]
        return PublicValuationBid(prices,known,bid_present,quality,invalid,fault)
