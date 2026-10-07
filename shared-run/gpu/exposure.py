"""全账户未判定市场的确认份额暴露E；不跨市场抵消，不产生账本分录。"""
from dataclasses import dataclass
import torch
from queues import ExactInt,ExactRational,Numbers
from asset_buy import take_ratio
from success_pool import select
from success_read import net_success
from trade_success import sum_rows


@dataclass(frozen=True)
class PortfolioExposureRead:
    amount: ExactRational  # pUSD，确认净两腿差每份按1pUSD；不使用行情价格。
    known: torch.Tensor
    fault: torch.Tensor


def read_portfolio_exposure(ledger,*,success_prefix_known,settlement_prefix_known):
    """消费完整公共成功/合格判定历史，不用模型scope或净占款替代份额。

    判定仅取同一已提交Ledger的typed resolution，不能从末端日期猜已判定。
    数量差为0时判定不影响零暴露E；有实际孤腿但缺判定绑定时保留Unknown。
    """
    roots=ledger.fault.numel();device=ledger.fault.device
    for column in (success_prefix_known,settlement_prefix_known):
        if column.shape!=(roots,) or column.dtype!=torch.bool or column.device!=device:
            raise TypeError('暴露E需要同根同设备的实际成功/结算完整历史')
    integer=ExactInt.from_i64_tensor(torch.zeros(roots,dtype=torch.int64,device=device))
    zero=ExactRational(integer,integer.constant(1));fault=ledger.fault.clone()
    known=success_prefix_known&settlement_prefix_known
    pools,resolutions=ledger.success_pools,ledger.resolutions
    if pools is None or resolutions is None:
        return PortfolioExposureRead(zero,torch.zeros_like(known),fault)
    net=net_success(pools,resolutions,prefix_known=settlement_prefix_known)
    assets=pools.asset_roots.numel();fault|=net.fault|resolutions.root_fault
    if not assets:return PortfolioExposureRead(zero,known,fault)
    index=torch.arange(assets,device=device);other=pools.counterpart
    delta=net.quantity-take_ratio(net.quantity,other)
    magnitude=ExactRational(delta.numerator.abs(),delta.denominator)
    pair=index<other  # 完整原对腿目录的每市场只计一次，按各市场先abs。
    markets=resolutions.roots.numel()
    if markets:
        matches=(pools.asset_roots[:,None]==resolutions.roots[None,:]) & (
            pools.condition_slots[:,None]==resolutions.conditions[None,:])
        found=matches.any(1);at=matches.to(torch.int64).argmax(1)
        resolved=found&resolutions.present[at]
        classification_known=found&resolutions.known[at]
    else:
        resolved=torch.zeros_like(pair);classification_known=torch.zeros_like(pair)
    pair_known=net.known&net.known[other]&((delta.numerator.sign==0)|classification_known)
    member=(pools.asset_roots[None,:]==torch.arange(roots,device=device)[:,None])&pair[None,:]
    known&=(~member|pair_known[None,:]).all(1)
    amount=take_ratio(magnitude,index[None,:].expand(roots,assets).flatten())
    absent=ExactRational(amount.numerator.constant(0),amount.denominator.constant(1))
    amount=sum_rows(select(amount,(member&~resolved[None,:]).flatten(),absent),roots,assets)
    fault|=amount.fault|Numbers.combine_fault_rows(torch.where(member,delta.fault[None,:],0))
    return PortfolioExposureRead(amount,known,fault)
