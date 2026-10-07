"""六个研究查询阶段的完整成果；不保存或恢复单元内状态。"""
from __future__ import annotations
import hashlib
import json
from pathlib import Path

# 仅相关算法改变时更新对应版本；源码和helper总身份另作provenance记录。
ALGORITHMS = dict(index='native-range-v1', market='research-reference-terminal-v2',
    chainlink_spot='causal-spot-pairs-v1', chainlink_twap60='causal-twap-reference-v2',
    binance_trade='causal-trade-price-v1', binance_depth='quantity-local-missing-v1')
KINDS = dict(market=('polymarket_market_metadata','polymarket_market'),
    chainlink_spot=('polymarket_rtds_chainlink_btc_usd',),
    chainlink_twap60=('polymarket_rtds_chainlink_twap',),
    binance_trade=('binance_aggtrade',),
    binance_depth=('binance_depth_snapshot','binance_depth_update'))


def digest(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':')).encode()).hexdigest()


def identity(config, stage, targets=None):
    kinds=KINDS.get(stage)
    files=[{k:f[k] for k in ('source_kind','schema_version','sha256','rows') if k in f}
           for f in config['files'] if kinds is None or f['source_kind'] in kinds]
    binding=dict(algorithm=ALGORITHMS[stage],files=files,start=config['start'],end=config['end'])
    if stage in ('index','market'):binding['native_unit_span']=config.get('native_unit_span')
    if stage=='chainlink_spot':
        from .model import WINDOWS
        binding['statistics']=dict(windows=list(WINDOWS),fit_days=[3,6,9,12,15],price_normalization='public-decimal-v1')
    if stage=='chainlink_twap60':binding['windows']=[30,60]
    if stage=='market':binding['initialization']=config['metadata_initialization']
    if targets is not None:
        binding['targets']=[{k:r.get(k) for k in ('condition_id','start','end','at','rule_window_s','rule_segments','market_known_at')} for r in targets]
    return digest(binding)


class CompletedStages:
    """描述文件最后原子落盘；输出和摘要须同时匹配才能复用。"""
    def __init__(self, material, config, reuse_material=None):
        self.root=Path(material)/'stages';self.root.mkdir(exist_ok=True)
        self.config=config
        self.reuse_root=Path(reuse_material)/'stages' if reuse_material is not None else None
    def load(self, stage, key):
        descriptor=self.root/f'{stage}.complete.json'
        directory=self.root
        if not descriptor.exists():
            if self.reuse_root is None:return None
            directory=self.reuse_root;descriptor=directory/f'{stage}.complete.json'
            if not descriptor.exists():return None
        record=json.loads(descriptor.read_text())
        if record['identity']!=key:return None
        data=directory/f'{stage}.json'
        content=data.read_bytes()
        if hashlib.sha256(content).hexdigest()!=record['sha256']:
            raise ValueError(f'completed {stage} output identity mismatch')
        value=json.loads(content)
        if directory!=self.root:
            # 输出和complete原件一起复制，保留实际producer而非当前复用者。
            target=self.root/f'{stage}.json';marker=self.root/f'{stage}.complete.json'
            marker.unlink(missing_ok=True)
            temporary=target.with_suffix('.partial');temporary.write_bytes(content);temporary.replace(target)
            temporary=marker.with_suffix('.partial');temporary.write_bytes(descriptor.read_bytes());temporary.replace(marker)
        return value
    def save(self, stage, key, value):
        data=self.root/f'{stage}.json';descriptor=self.root/f'{stage}.complete.json'
        # 不允许旧完成标记把重算中的半写成果识别为完整。
        descriptor.unlink(missing_ok=True)
        content=(json.dumps(value,ensure_ascii=False,separators=(',',':'))+'\n').encode()
        temporary=data.with_suffix('.partial');temporary.write_bytes(content);temporary.replace(data)
        record=dict(stage=stage,identity=key,sha256=hashlib.sha256(content).hexdigest(),
            algorithm=ALGORITHMS[stage],implementation_commit=self.config.get('implementation_commit'),
            helper_sha256=self.config.get('probability_helper_sha256'))
        temporary=descriptor.with_suffix('.partial');temporary.write_text(json.dumps(record,indent=2)+'\n');temporary.replace(descriptor)
