"""成功共同消费后的小型MLflow报告；不参与学习资格或经济归约。"""
from dataclasses import dataclass
from fractions import Fraction
from pathlib import Path
import json
import time
import torch
from actor_send import exact_values
from queues import ExactRational,_columns,Numbers
from trade_success import sum_rows
from .gpu_exact import run_exact_batch


def exact_report(value):
    return [str(Fraction(n,d)) for n,d in zip(exact_values(value.numerator),
        exact_values(value.denominator),strict=True)]


def reward_summary(rewards):
    """只归约原IntervalReward；不保逐决定奖励或重新推导奖励。"""
    def total():
        rows=rewards[0].present.numel()
        return sum_rows(ExactRational(_columns([r.exact.numerator for r in rewards]),
            _columns([r.exact.denominator for r in rewards])),rows,len(rewards))
    exact=run_exact_batch(total)
    return dict(exact=exact_report(exact),unit='training_score',
        projected_sum=torch.stack([r.value.detach() for r in rewards]).sum(0).cpu().tolist(),
        duration_seconds=torch.stack([r.dt_seconds.detach() for r in rewards]).sum(0).cpu().tolist(),
        known=torch.stack([r.known for r in rewards]).all(0).cpu().tolist(),
        fault=Numbers.combine_fault_rows(torch.stack([r.fault for r in rewards],1)).cpu().tolist())


def economic_metrics(root_reports):
    """仅映射原读面；根序号沿学习器稳定顺序，不按本批active人口重编号。"""
    metrics={}
    for root_report in root_reports:
        index=root_report['root_index'];economics=root_report['economics']
        for key in ('wcm','exposure','actual_available','actual_reserved','nav_conservative','nav_directional'):
            for row,value in enumerate(economics[key]):metrics[f'root{index}/row{row}/{key}']=float(Fraction(value))
        for key in ('wcm_known','exposure_known','fault','at_ms','ledger_prefix','local_prefix','platform_prefix'):
            for row,value in enumerate(economics[key]):metrics[f'root{index}/row{row}/{key}']=int(value)
        for row,known in enumerate(economics['nav_known']):
            metrics[f'root{index}/row{row}/nav_conservative_known']=int(known[0])
            metrics[f'root{index}/row{row}/nav_directional_known']=int(known[1])
    return metrics


class ReportWriteError(RuntimeError):
    """业务更新已经提交；仅报告出口失败。"""


def training_configuration(learner):
    """现有owner的小型有效配置读面；不是第二份配置权威或权重副本。"""
    def scalar(value):
        if isinstance(value,torch.Tensor):return value.detach().cpu().tolist()
        if isinstance(value,tuple):return list(value)
        return value
    return dict(model=type(learner.policy).__name__,model_shape=vars(learner.policy.shape),
        input_version=learner.policy.input_version,value_version=learner.policy.value_version,
        propagation=learner.policy.propagation,learning=vars(learner.config),gae=dict(gamma=1,lambda_=1),
        optimizer=dict(kind=type(learner.optimizer).__name__,param_groups=[
            {k:scalar(v) for k,v in group.items() if k!='params'} for group in learner.optimizer.param_groups]),
        roots=[dict(learning=vars(batch.config),
            reward_a_pusd=None if batch.root.reward_a_pusd is None else exact_report(batch.root.reward_a_pusd),
            reward_unit='training_score; coefficient pUSD',
            trading_until=None if batch.root.trading_until is None else scalar(batch.root.trading_until),
            goal_until=None if batch.root.goal_until is None else scalar(batch.root.goal_until)) for batch in learner.roots])


@dataclass
class MLflowReport:
    tracking_uri: str
    run_id: str
    context: dict
    pending: dict | None = None

    def mark_restored(self,directory):
        self.context=dict(self.context,restored_from=str(directory))
        if self.pending is not None:
            self.pending['context']=dict(self.pending['context'],replayed_from_checkpoint=str(directory))

    def _client(self):
        from mlflow import MlflowClient
        return MlflowClient(tracking_uri=self.tracking_uri)

    def write_pending(self):
        """仅补写报告。客户端/连接不进入活动状态；成功即释放待写摘要。"""
        if self.pending is None:return
        from mlflow.entities import Metric
        item=self.pending;client=self._client();at=int(time.time()*1000)
        metrics=[Metric(k,float('nan') if v is None else float(v),at,item['step'])
            for k,v in item['metrics'].items()]
        client.log_batch(self.run_id,metrics=metrics,synchronous=True)
        client.log_text(self.run_id,json.dumps(item,sort_keys=True,allow_nan=False),
            'reports/'+item['kind']+'-'+str(item['step'])+'-'+str(time.time_ns())+'.json')
        self.pending=None

    def publish(self,kind,step,metrics,report):
        # 写出异常后调用方沿真实更新失败边界停止；补写不调用optimizer。
        self.pending=dict(kind=kind,step=step,metrics=metrics,report=report,context=dict(self.context))
        try:self.write_pending()
        except Exception as error:
            raise ReportWriteError('更新/检查点已完成，报告失败；仅补写write_pending，不重跑optimizer') from error

    def checkpoint(self,directory,step):
        path=Path(directory)
        reference=dict(directory=str(path),manifest=str(path/'manifest.json'),
            manifest_sha256=__import__('hashlib').sha256((path/'manifest.json').read_bytes()).hexdigest())
        if self.pending is not None:
            # 同一未写摘要附带刚发布的完整点，不替换未写update或建立队列。
            self.pending['checkpoint']=reference
            self.pending['metrics']['checkpoint_published']=1
            try:self.write_pending()
            except Exception as error:
                raise ReportWriteError('完整检查点已发布，报告失败；待写更新摘要仍保留') from error
        else:self.publish('checkpoint',step,dict(checkpoint_published=1),reference)
