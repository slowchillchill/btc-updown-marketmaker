"""真实原生组到既有参数图的测试进程；只返回原分数，不选择族或身份。"""
import json
import sys
from pathlib import Path
import torch
from model_train.graphs import CONFIGURATIONS
from model_train.graphs import TwoGraphs
from model_train.native_inputs import NativeComponentConsumer

request = json.load(sys.stdin)
graphs = TwoGraphs(CONFIGURATIONS[2]).eval()
graphs.load_state_dict(torch.load(Path(sys.argv[1]) / "initial_weights.pt", map_location="cpu", weights_only=True))
consumer = NativeComponentConsumer(graphs)
state_calls = []
handle = graphs.state.register_forward_hook(lambda *args: state_calls.append(1))
with torch.no_grad():
    for group in request["groups"]:
        result = consumer.consume(group)
    assert len(state_calls) == sum(len(group["rows"]) for group in request["groups"])
    count = len(state_calls)
    before = {key: value.clone() for key, value in consumer.hidden.items()}
    blocks = consumer.cancel_scores(result, request["row"], request["prefix"], lengths=request.get("lengths"))
    assert len(state_calls) == count
    for key in before:
        torch.testing.assert_close(before[key], consumer.hidden[key], rtol=0, atol=0)
handle.remove()
json.dump(blocks, sys.stdout)
