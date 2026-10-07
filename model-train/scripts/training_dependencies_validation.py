#!/usr/bin/env python3
"""真实100ms模型上的学习依赖接线验证；合成观察/奖励，不是业务采集或正式训练。"""
import argparse
import copy
from importlib import metadata
import json
from pathlib import Path
import subprocess
import time
import torch
from tensordict import TensorDict
from tensordict.nn import TensorDictModule
from torchrl.modules import ProbabilisticActor,ValueOperator
from torchrl.objectives import ClipPPOLoss
from torchrl.objectives.value import GAE
import mlflow
import onnx
import onnxruntime
from model_train.gpu_policy import ModelShape100ms,Policy100ms


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--artifact',type=Path,required=True)
    args=parser.parse_args()
    artifact=args.artifact.absolute()
    if not artifact.is_relative_to(Path('/root/autodl-tmp')) or artifact.exists():
        raise ValueError('验证证据需要数据盘的新目录')
    artifact.mkdir(parents=True)
    commit=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip()
    status=subprocess.check_output(['git','status','--porcelain'],text=True)
    if status:raise ValueError('执行源码需要准确提交且干净')
    (artifact/'source-commit.txt').write_text(commit+'\n')
    (artifact/'status.txt').write_text(status)
    started=time.perf_counter()
    torch.manual_seed(181103)  # 工程组件接线，不增加正式候选/种子/账户根。
    device='cuda'
    policy=Policy100ms(ModelShape100ms(layers=2,hidden=64)).to(device).eval()
    inputs=(torch.randn(5,2,626,device=device),torch.randn(5,2,433,device=device),
        torch.randn(5,2,6,3,9,device=device),torch.ones(5,2,6,3,device=device,dtype=torch.bool))
    h=torch.zeros(2,2,64,device=device,requires_grad=True)
    z,value,h_next=policy.learning_unroll(tuple(x[:2] for x in inputs),torch.ones(2,2,device=device,dtype=torch.bool),
        tuple(x[2:] for x in inputs),torch.ones(3,2,device=device,dtype=torch.bool),h)
    actor=ProbabilisticActor(TensorDictModule(policy.stages()['action_family'],
        in_keys=['z','rule','cancel_reduced','cancel_exists'],out_keys=['logits','rule_context']),
        in_keys=['logits'],out_keys=['action'],distribution_class=torch.distributions.Categorical,return_log_prob=True)
    critic=ValueOperator(policy.critic,in_keys=['z'],out_keys=['state_value'])
    flat_z=z.transpose(0,1).reshape(6,64)
    batch=TensorDict({'z':flat_z.detach(),'rule':torch.zeros(6,30,device=device),
        'cancel_reduced':torch.zeros(6,96,device=device),'cancel_exists':torch.zeros(6,device=device,dtype=torch.bool)},batch_size=[6],device=device)
    with torch.no_grad():
        actor(batch)
        # 合成非终态截断，真实模型纯bootstrap不提交h。
        _,_,bootstrap=policy.state_and_value(inputs[0][-1],inputs[1][-1],inputs[2][-1],inputs[3][-1],h_next)
        old=value.detach().transpose(0,1).unsqueeze(-1)
        next_value=torch.cat([old[:,1:],bootstrap[:,None,None]],1)
        trajectory=TensorDict({'state_value':old,'next':{
            'state_value':next_value,'reward':torch.full((2,3,1),0.01,device=device),
            'done':torch.zeros(2,3,1,device=device,dtype=torch.bool),
            'terminated':torch.zeros(2,3,1,device=device,dtype=torch.bool)}},batch_size=[2,3],device=device)
        GAE(gamma=0.97,lmbda=0.91,value_network=None,time_dim=1,device=device)(trajectory)
    batch.set('z',flat_z)
    batch.set('state_value',old.reshape(6,1))
    batch.set('advantage',trajectory['advantage'].reshape(6,1))
    batch.set('value_target',trajectory['value_target'].reshape(6,1))
    loss_module=ClipPPOLoss(actor,critic,clip_epsilon=0.2,entropy_coeff=0.01,critic_coeff=0.5,
        functional=False,device=device)
    optimizer=torch.optim.AdamW(policy.parameters(),lr=0.0001)
    losses=loss_module(batch)
    loss=sum(value for key,value in losses.items() if key.startswith('loss_'))
    assert loss.is_cuda and torch.isfinite(loss)
    before=next(policy.critic.parameters()).detach().clone()
    loss.backward()
    assert h.grad is None
    assert next(policy.state.parameters()).grad is not None
    assert all(parameter.grad is None for parameter in policy.action.actor.price.parameters())
    optimizer.step()
    assert not torch.equal(before,next(policy.critic.parameters()).detach())
    print('stage=CUDA_GAE_PPO_actual_model_update_passed',flush=True)
    # protobuf共同版本的适用ONNX/ORT路径，仅导出原Critic，不声称完整策略导出。
    model=copy.deepcopy(policy.critic).cpu().eval()
    example=flat_z.detach().cpu()
    onnx_path=artifact/'critic.onnx'
    torch.onnx.export(model,(example,),str(onnx_path),opset_version=18,dynamo=False,input_names=['z'],output_names=['value'])
    onnx.checker.check_model(onnx.load(str(onnx_path)))
    session=onnxruntime.InferenceSession(str(onnx_path),providers=['CPUExecutionProvider'])
    actual=torch.from_numpy(session.run(None,{'z':example.numpy()})[0])
    with torch.no_grad():assert torch.allclose(actual,model(example),rtol=1e-5,atol=1e-5)
    report={'source_commit':commit,'engineering_synthetic_observations_and_rewards':True,'formal_training':False,
        'actual_model':'Policy100ms-2x64','CUDA_GAE_PPO_update':True,'burn_in_hidden_detached':True,
        'unused_price_head_grad_none':True,'critic_ONNX_ORT_shared_protobuf_checked':True,
        'full_action_path_and_business_rollout':False,'active_root_restore':False,
        'versions':{name:metadata.version(name) for name in ['torch','numpy','torchrl','tensordict','mlflow','protobuf']},
        'losses':{key:float(value.detach().cpu()) for key,value in losses.items() if key.startswith('loss_')},
        'seconds':time.perf_counter()-started,'device_peak_allocated_bytes':torch.cuda.max_memory_allocated()}
    mlflow.set_tracking_uri('sqlite:///'+str(artifact/'mlflow.db'))
    experiment=mlflow.create_experiment('issue-181-engineering-dependency-wiring',artifact_location=(artifact/'mlflow-artifacts').as_uri())
    with mlflow.start_run(experiment_id=experiment) as run:
        mlflow.log_params({'formal_training':False,'source_commit':commit,'input_version':policy.input_version,'fixture':'synthetic_dependency_wiring'})
        mlflow.log_metrics(report['losses'])
        report['mlflow_run_id']=run.info.run_id
        (artifact/'report.json').write_text(json.dumps(report,indent=2)+'\n')
        mlflow.log_artifact(str(artifact/'report.json'))
    stored=mlflow.get_run(report['mlflow_run_id'])
    assert stored.info.status=='FINISHED' and stored.data.params['formal_training']=='False'
    assert all(key in stored.data.metrics for key in report['losses'])
    report['mlflow_record_readback_checked']=True
    (artifact/'report.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report),flush=True)


if __name__=='__main__':main()
