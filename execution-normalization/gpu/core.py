"""NOE同批准备与身份归约的设备候选。

key为所属账户+原类型+无损标量字段；u64拆为两个非负u32，不转有符号序号。
semantic/cancellation列是已按原强类型等值语义编码的共享字段行，不是hash。
所有行引用指向同一不可变bank；SIM字段生产和冷原类型导出另行接入。
"""
from dataclasses import dataclass
import torch

EVIDENCE, ECONOMIC, LATE_COVERAGE, PENDING = range(4)
OUT_EVIDENCE, OUT_FACT, OUT_DUPLICATE, OUT_ALIAS = 1, 2, 3, 4
OUT_LOGICAL_CONFLICT, OUT_ECONOMIC_CONFLICT, OUT_PENDING = 5, 6, 7


@dataclass(frozen=True)
class EvidenceBank:
    logical_keys: torch.Tensor
    economic_keys: torch.Tensor
    semantic: torch.Tensor
    cancellation: torch.Tensor
    kinds: torch.Tensor
    source_order: torch.Tensor  # sequence tag, u64 low/high, source ordinal
    legacy_order: torch.Tensor  # 同SourceOrder时原完整身份字符串的无损排序rank
    fault: torch.Tensor
    payload_schemas: torch.Tensor | None = None  # 正文类型，不是逻辑身份命名空间。
    declared_schemas: frozenset | None = None  # 冷映射声明的布局集合；未知来源不省略解码。

    @classmethod
    def empty(cls,device):
        """首条真实反馈前的空共享bank；append仍使用完整原身份与正文。"""
        scalar=torch.empty(0,dtype=torch.int64,device=device)
        fields=torch.empty((0,0),dtype=torch.int64,device=device)
        # 正文编码即使零行也保留长度列，供同一个schema解码器的零人口路径。
        semantic=torch.empty((0,1),dtype=torch.int64,device=device)
        return cls(fields,fields.clone(),semantic,fields.clone(),scalar,
            torch.empty((0,4),dtype=torch.int64,device=device),scalar.clone(),
            torch.empty(0,dtype=torch.int32,device=device),scalar.clone(),frozenset())

    def __post_init__(self):
        count = self.kinds.numel()
        device = self.kinds.device
        if self.payload_schemas is None:
            object.__setattr__(self, 'payload_schemas', torch.zeros_like(self.kinds))
        for value in [self.logical_keys, self.economic_keys, self.semantic,
                      self.cancellation, self.source_order]:
            if value.ndim != 2 or value.size(0) != count or value.dtype != torch.int64 or value.device != device:
                raise TypeError('NOE共享字段需同设备int64[row,field]')
        if self.source_order.size(1) != 4:
            raise ValueError('NOE源序需要原sequence tag、完整u64及ordinal')
        for value in [self.kinds, self.legacy_order, self.payload_schemas]:
            if value.shape != (count,) or value.dtype != torch.int64 or value.device != device:
                raise TypeError('NOE类型与排序需同设备int64[row]')
        if self.fault.shape != (count,) or self.fault.dtype != torch.int32 or self.fault.device != device:
            raise TypeError('NOE字段故障需同设备int32[row]')

    def ordered(self, rows):
        """保持原SourceOrder及同序legacy身份顺序，返回共享行坐标。"""
        result = rows
        for key in [self.legacy_order, self.source_order[:, 3], self.source_order[:, 1],
                    self.source_order[:, 2], self.source_order[:, 0]]:
            result = result.gather(1, key[result].argsort(dim=1, stable=True))
        return result

    @classmethod
    def concatenate(cls, banks):
        """同一自然批的记录主序字段一次装箱，旧bank尚未参与复制。"""
        from dataclasses import fields
        columns={}
        for field in fields(cls):
            if field.name=='declared_schemas': continue
            values=[getattr(bank,field.name) for bank in banks]
            if values[0].ndim==2:
                width=max(value.size(1) for value in values)
                values=[torch.nn.functional.pad(value,(0,width-value.size(1))) for value in values]
            columns[field.name]=torch.cat(values)
        declaration=None if any(bank.declared_schemas is None for bank in banks) else frozenset().union(
            *(bank.declared_schemas for bank in banks))
        return cls(**columns,declared_schemas=declaration)

    def append(self, incoming):
        """追加已按同schema编码的不可变行，旧身份/值引用不改号。"""
        from fields import append_rows
        from dataclasses import fields
        declaration=None if self.declared_schemas is None or incoming.declared_schemas is None else self.declared_schemas | incoming.declared_schemas
        return EvidenceBank(**{field.name:append_rows(getattr(self,field.name),getattr(incoming,field.name))
            if getattr(self,field.name).ndim==2 else torch.cat([getattr(self,field.name),getattr(incoming,field.name)])
            for field in fields(self) if field.name!='declared_schemas'},declared_schemas=declaration)


@dataclass(frozen=True)
class NormalizedBatch:
    rows: torch.Tensor
    outcomes: torch.Tensor
    primary_fact_rows: torch.Tensor
    emitted: torch.Tensor
    fault: torch.Tensor


@dataclass(frozen=True)
class Normalizer:
    bank: EvidenceBank
    logical_rows: torch.Tensor
    economic_rows: torch.Tensor
    fault: torch.Tensor

    @classmethod
    def empty(cls, bank, roots):
        rows = torch.full((roots, bank.kinds.numel()), -1, dtype=torch.int64, device=bank.kinds.device)
        return cls(bank, rows, rows.clone(), torch.zeros(roots, dtype=torch.int32, device=rows.device))

    def append_bank(self, incoming):
        """字段已设备生成；按新输入人口增长目录，重复prepare仍复用原容量。"""
        bank=self.bank.append(incoming)
        addition=incoming.kinds.numel()
        return Normalizer(bank,torch.nn.functional.pad(self.logical_rows,(0,addition),value=-1),
            torch.nn.functional.pad(self.economic_rows,(0,addition),value=-1),self.fault)

    def prepare(self, rows, enabled):
        """纯候选；按原输入重试不会接纳半批，旧成功前缀不回滚。

        物理目录按不可变bank人口分配，重复不再增长，无CPU读取GPU业务值。暂用精确线性目录，
        真实根容量阶段再替换索引；目录不是新增业务身份或固定历史上限。
        """
        if rows.dtype != torch.int64 or rows.ndim != 2 or rows.device != self.fault.device:
            raise TypeError('NOE批需同设备int64[root,row]')
        if enabled.shape != rows.shape or enabled.dtype != torch.bool or enabled.device != rows.device:
            raise TypeError('NOE接纳mask需同设备bool[root,row]')
        roots, width = rows.shape
        if roots != self.fault.numel():
            raise ValueError('NOE批根人口不符')
        bank = self.bank
        if not bank.kinds.numel():
            if width:
                raise ValueError('非空NOE批不能引用空bank')
            return self, NormalizedBatch(rows, rows.clone(), rows.clone(), enabled, self.fault)
        invalid = (rows < 0) | (rows >= bank.kinds.numel())
        safe = rows.clamp(0, bank.kinds.numel() - 1)
        # enabled也随同原行顺序移动，不能把masked padding当来源人口。
        order = torch.arange(width, device=rows.device).expand(roots, width)
        for key in [bank.legacy_order, bank.source_order[:, 3], bank.source_order[:, 1],
                    bank.source_order[:, 2], bank.source_order[:, 0]]:
            order = order.gather(1, key[rows.clamp(0, bank.kinds.numel()-1)].gather(1, order).argsort(dim=1, stable=True))
        active = enabled.gather(1, order)
        invalid = invalid.gather(1, order)
        safe = safe.gather(1, order)
        logical = self.logical_rows.clone()
        economic = self.economic_rows.clone()
        outcomes = torch.zeros_like(rows)
        primary = torch.full_like(rows, -1)
        fault = self.fault.clone()
        root_index = torch.arange(roots, device=rows.device)

        def lookup(directory, key, keys):
            coordinates = directory.clamp_min(0)
            same = (keys[coordinates] == key[:, None, :]).all(dim=2) & (directory >= 0)
            found = same.any(dim=1)
            at = same.to(torch.int64).argmax(dim=1)
            return found, directory[root_index, at].clamp_min(0)

        for position in range(width):
            row = safe[:, position]
            use = active[:, position]
            kind = bank.kinds[row]
            error = bank.fault[row] | invalid[:, position].to(torch.int32) | (
                (kind < EVIDENCE) | (kind > PENDING)).to(torch.int32)
            source = bank.source_order[row]
            error = error | ((source[:, 0] < 0) | (source[:, 0] > 2) |
                (source[:, 1:] < 0).any(dim=1) | (source[:, 1:] > 0xffffffff).any(dim=1) |
                (bank.legacy_order[row] < 0)).to(torch.int32)
            fault = fault | torch.where(use, error, torch.zeros_like(error))
            found_logical, previous = lookup(logical, bank.logical_keys[row], bank.logical_keys)
            found_economic, fact = lookup(economic, bank.economic_keys[row], bank.economic_keys)
            equal_body = (bank.semantic[previous] == bank.semantic[row]).all(dim=1)
            equal_body = equal_body & (bank.payload_schemas[previous] == bank.payload_schemas[row])
            equal_cancel = (bank.cancellation[previous] == bank.cancellation[row]).all(dim=1)
            equal_economic = (bank.economic_keys[previous] == bank.economic_keys[row]).all(dim=1)
            incoming_economic = kind == ECONOMIC
            same = equal_body & equal_cancel & torch.where(incoming_economic,
                (bank.kinds[previous] == ECONOMIC) & equal_economic,
                bank.kinds[previous] == kind)
            late = kind == LATE_COVERAGE
            result = torch.where(incoming_economic,
                torch.where(found_economic,
                    torch.where((bank.semantic[fact] == bank.semantic[row]).all(dim=1) &
                        (bank.payload_schemas[fact] == bank.payload_schemas[row]), OUT_ALIAS, OUT_ECONOMIC_CONFLICT),
                    OUT_FACT), torch.where(late,
                    torch.where(found_economic, OUT_ALIAS, OUT_PENDING),
                    torch.where(kind == PENDING, OUT_PENDING, OUT_EVIDENCE)))
            result = torch.where(found_logical,
                torch.where(same, torch.where(late & found_economic, OUT_ALIAS, OUT_DUPLICATE), OUT_LOGICAL_CONFLICT), result)
            write_logical = use & ~found_logical
            write_economic = write_logical & incoming_economic & ~found_economic
            for directory, write in [(logical, write_logical), (economic, write_economic)]:
                vacant = directory < 0
                at = vacant.to(torch.int64).argmax(dim=1)
                fault = fault | (write & ~vacant.any(dim=1)).to(torch.int32)
                directory[root_index, at] = torch.where(write, row, directory[root_index, at])
            outcomes[:, position] = torch.where(use, result, 0)
            primary[:, position] = torch.where(use & (result == OUT_FACT), row,
                torch.where(use & (result == OUT_ALIAS), fact, -1))
        accepted = fault == 0
        logical = torch.where(accepted[:, None], logical, self.logical_rows)
        economic = torch.where(accepted[:, None], economic, self.economic_rows)
        return Normalizer(bank, logical, economic, fault), NormalizedBatch(
            safe, outcomes, primary, active & accepted[:, None], fault)
