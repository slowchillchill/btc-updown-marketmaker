"""已验收的公共根/hidden根接入原连续宿主；不归约第二份行情或账户。"""
from dataclasses import dataclass,replace
import torch
from hidden_inputs import HiddenPmBlock
from hidden_frontier import HiddenBucketColumns,HiddenBucketFrontier
from public_source import PublicGroupSource
from queues import Numbers


@dataclass(frozen=True)
class ActorSources:
    runtime: object
    source: PublicGroupSource
    states: tuple
    block: HiddenPmBlock
    market_indices: torch.Tensor
    condition_by_asset: torch.Tensor


def install_actor_sources(public,hidden,runtime,condition_ids,*,page_groups=256,
                          hidden_page_buckets=64,observation_inputs=True):
    """根已各自验收；这里只绑定共同来源和两种物理市场坐标。"""
    if public.source_manifest_sha256!=hidden.manifest['verification']['source_manifest_sha256']:
        raise ValueError('连续Actor公共/hidden根不属于同一原公共来源')
    states=public.install()
    source=PublicGroupSource(public,runtime.orders.roots,page_groups=page_groups,
        observation_inputs=observation_inputs)
    count=len(hidden.arrays['bucket_times'])
    block=HiddenPmBlock.from_prepared(hidden.page(0,min(hidden_page_buckets,count)))
    device=block.level_assets.device
    public_markets={market['condition_id']:i for i,market in enumerate(public.columns['markets'])}
    conditions={condition:i for i,condition in enumerate(condition_ids)}
    # hidden市场槽沿严格reader的source_scene；公共行沿公共根目录，不按槽号相等映射。
    scene=hidden.shared['banks']['source_scene']
    hidden_conditions=torch.tensor([conditions[condition] for condition,_ in scene],device=device)
    market_indices=torch.tensor([public_markets[condition] for condition in condition_ids],device=device)
    runtime=replace(runtime,
        hidden=Numbers.run_exact_batch(lambda:block.empty_state(segment_capacity=runtime.hidden.queues.capacity,maker_model=runtime.hidden.maker_model)),
        hidden_frontier=HiddenBucketFrontier.open(HiddenBucketColumns.from_block(block),runtime.orders.roots))
    runtime=runtime.install_hidden_market_controls()
    return ActorSources(runtime,source,states,block,market_indices,hidden_conditions[block.asset_markets])
