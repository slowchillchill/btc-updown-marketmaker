"""#176 有限数学验收；原生现金自然形成小域，不向生产加入动作限制。"""
import copy
import json
import os
import unittest
from collections import Counter, defaultdict
from unittest.mock import patch
from pathlib import Path

import torch
from torch.distributions import Categorical

from .actor import consume_actor, joint_entropy_sample
from .graphs import CONFIGURATIONS, TwoGraphs, reduce_objects
from .native_inputs import NativeComponentConsumer


class RepeatedReference(NativeComponentConsumer):
    """独立按当前头重算对象、规则及前缀；不用阶段包装或缓存评分入口。"""
    def parameters(self, group, row, prefix, *, price_fee_features=None, price_indices=None):
        action = self.graphs.action
        z = group.states[row]
        features = group.cancel_features
        # 原路径与候选均按256物理行编码；参考独立重算同一布局，避免另改GEMM形状。
        # 不调用候选objects、阶段包装或缓存；全量人口与有效切片保持。
        count = features.shape[1]
        blocks = []
        for start in range(0, max(count, 1), 256):
            values = features[:, start:start + 256]
            length = values.shape[1]
            slots = 256 if count else 1
            padded = torch.nn.functional.pad(values, (0, 0, 0, slots - length))
            mask = torch.arange(slots, device=z.device).reshape(1, -1) < length
            encoded = torch.where(mask.unsqueeze(-1), action.object_encoder(padded), 0.)
            blocks.append((encoded[:, :length], mask[:, :length]))
        objects = torch.cat([values for values, _ in blocks], dim=1)
        reduced, exists = reduce_objects(blocks)
        rule = action.rule(group.rules[row])
        head = (0, 1, 2, 3, 5)[prefix[7:12].index(1)]
        if head == 0:
            summary = torch.where(exists.unsqueeze(-1), action.summary(reduced), 0.)
            scores = action.actor.family(torch.cat((z, rule, summary), dim=-1))
        else:
            context = action.actor.decoder(torch.cat((z, rule,
                action.prefix(z.new_tensor(prefix).reshape(1, 36))), dim=-1))
            if head == 3:
                # 参考计算全部999行，再由外部合法坐标取概率；与Gather前置路径独立。
                indices = torch.tensor(price_indices, dtype=torch.int64)
                fees = z.new_zeros(1, 999).index_copy(1, indices,
                    z.new_tensor(price_fee_features).reshape(1, -1))
                scores = action.actor.price(context) + action.actor.fee_weight(context) * fees
            elif head == 5:
                expanded = context.unsqueeze(1).expand(-1, objects.shape[1], -1)
                scores = action.actor.cancel(torch.cat((expanded, objects), dim=-1)).squeeze(-1)
            else:
                scores = (action.actor.quantity if head == 1 else action.actor.variant)(context)
        outputs = [scores.new_empty(1, 0) for _ in range(6)]
        outputs[head] = scores
        return [tuple(outputs)]


def small_domain():
    """独立本金算术：cash=600微单位，n/100份，tick=.01，每腿本金=n*k*100。"""
    actions = {(0,): ('NoTrade',), (5, 0): ('CancelMarket',)}
    # 1..3的固定16路分段号；不调用Rust的quantity_segment或支持函数枚举叶子。
    slots = {1: 5, 2: 10, 3: 15}
    for n in range(1, 4):
        for a in range(1, 100):
            for b in range(1, 100):
                if n * (a + b) * 100 <= 600:
                    # 全部本金<1，真实普通GTC无即时费用；FOK每腿至少1，无法完成。
                    actions[(2, slots[n], 0, 0, a - 1, b - 1)] = ('GTC/GTC', n, a, b)
    assert len(actions) == 21
    tree = defaultdict(set)
    for path in actions:
        for at, choice in enumerate(path):
            tree[path[:at]].add(choice)
    return actions, {prefix: sorted(choices) for prefix, choices in tree.items()}


def prepared(model, native, history, reference=False):
    consumer = (RepeatedReference if reference else NativeComponentConsumer)(model)
    for prior in history:
        consumer.consume(prior)
    return consumer, consumer.consume(native.features(shared_books=True))


def at_prefix(native, prefix):
    actor = native.actor(0)
    for choice in prefix:
        actor.choose(choice)
    return actor


def compare_gradients(left, right):
    statistics = Counter()
    maximum = 0.
    for name, parameter in left.named_parameters():
        expected = dict(right.named_parameters())[name]
        assert (parameter.grad is None) == (expected.grad is None), name
        if parameter.grad is None:
            statistics['none_parameters'] += 1
            continue
        assert bool(torch.count_nonzero(parameter.grad)) == bool(torch.count_nonzero(expected.grad)), name
        torch.testing.assert_close(parameter.grad, expected.grad, rtol=1e-4, atol=1e-5, msg=name)
        maximum = max(maximum, float((parameter.grad - expected.grad).abs().max()))
        statistics['zero_parameters' if not torch.count_nonzero(parameter.grad) else 'nonzero_parameters'] += 1
        statistics['gradient_scalars'] += parameter.numel()
    return {**statistics, 'max_absolute_error': maximum}


class NativeProbabilityTests(unittest.TestCase):
    def __init__(self, method, native, history, evidence):
        super().__init__(method)
        self.native, self.history, self.evidence = native, history, evidence

    def test_complete_joint_distribution_entropy_and_weighted_surrogate(self):
        model = TwoGraphs(CONFIGURATIONS[2])
        reference = copy.deepcopy(model)
        consumer, group = prepared(model, self.native, self.history)
        oracle, reference_group = prepared(reference, self.native, self.history, reference=True)
        actions, tree = small_domain()
        factors, raw_prices = {}, {}
        singleton_nodes = 0
        for prefix, choices in tree.items():
            actor = at_prefix(self.native, prefix)
            self.assertEqual([i for i, legal in enumerate(actor.support()) if legal], choices, prefix)
            if len(choices) == 1:
                factors[prefix] = {choices[0]: torch.tensor(1., dtype=torch.float64)}
                singleton_nodes += 1
                continue
            outputs = oracle.parameters(reference_group, 0, actor.prefix_features(),
                price_fee_features=actor.price_fee_features(), price_indices=actor.price_indices())
            scores = outputs[0][actor.head_index()][0][choices].double()
            # 独立概率oracle不用Categorical、conditional_choice、consume_actor或log_softmax。
            weights = torch.exp(scores)
            probabilities = weights / weights.sum()
            factors[prefix] = dict(zip(choices, probabilities.unbind()))
            if actor.head_index() == 3:
                raw_prices[prefix] = scores.detach()
        self.assertGreater(singleton_nodes, 0)
        # A价格改变B的合法集合与神经分数，不是无条件独立两腿的玩具分布。
        first, second = (2, 5, 0, 0, 0), (2, 5, 0, 0, 1)
        self.assertNotEqual(tree[first], tree[second])
        self.assertFalse(torch.equal(raw_prices[first][:2], raw_prices[second][:2]))
        probabilities, actual_probabilities, reports, surrogates, leaves = [], [], [], [], []
        for path, description in actions.items():
            probability = torch.tensor(1., dtype=torch.float64)
            for at, choice in enumerate(path):
                probability = probability * factors[path[:at]][choice]
            probabilities.append(probability)
            replay = consume_actor(consumer, group, 0, self.native.actor(0), replay=path)
            self.assertTrue(replay.actor.complete())
            self.assertEqual(tuple(replay.actor.path()), path)
            torch.testing.assert_close(replay.log_prob.double(), probability.log(), rtol=1e-4, atol=1e-5)
            actual_probabilities.append(replay.log_prob.double().exp())
            actor = self.native.actor(0)
            # 穷举采样器的每一种可能结果；只固定抽到的合法索引，概率/熵仍走生产入口。
            def draw(distribution, sample_shape=torch.Size()):
                self.assertEqual(sample_shape, torch.Size())
                prefix = tuple(actor.path())
                legal = tree[prefix]
                self.assertGreater(len(legal), 1)
                return torch.tensor(legal.index(path[len(prefix)]), device=distribution.logits.device)
            with patch.object(Categorical, 'sample', draw):
                report, surrogate, sampled = joint_entropy_sample(consumer, group, 0, actor)
            self.assertEqual(tuple(sampled.actor.path()), path)
            torch.testing.assert_close(sampled.log_prob, replay.log_prob, rtol=0, atol=0)
            reports.append(probability.detach() * report.double())
            surrogates.append(probability.detach() * surrogate.double())
            leaves.append({'action': description, 'path': path, 'probability': float(probability.detach()),
                'actual_probability': float(actual_probabilities[-1].detach())})
        joint = torch.stack(probabilities)
        normalization = float(joint.detach().sum())
        self.assertAlmostEqual(normalization, 1., places=12)
        self.assertAlmostEqual(float(torch.stack(actual_probabilities).detach().sum()), 1., places=6)
        budget_entropy = -.5 * (joint * joint.log()).sum()
        weighted_report = sum(reports)
        torch.testing.assert_close(weighted_report, budget_entropy.detach(), rtol=1e-4, atol=1e-5)
        # surrogate用于最小化损失：其期望梯度应等于 -H_budget 的梯度。
        sum(surrogates).backward()
        (-budget_entropy).backward()
        gradients = compare_gradients(model, reference)
        self.assertGreater(gradients['nonzero_parameters'], 0)
        self.evidence['joint'] = {'configuration': 'MAIN-1x256', 'parameter_dtype': 'float32',
            'oracle_probability_dtype': 'float64', 'complete_actions': len(actions), 'prefix_nodes': len(tree),
            'singleton_nodes': singleton_nodes, 'normalization': normalization,
            'exact_budget_entropy': float(budget_entropy.detach()), 'weighted_sample_report': float(weighted_report),
            'gradient_target': '-H_budget', 'gradients': gradients, 'leaves': leaves,
            'scope': '完整合法叶子精确枚举；数值与梯度沿atol=1e-5/rtol=1e-4比较；不测试采样频率'}

    def test_actual_singletons_none_zero_and_adamw_state(self):
        rows = []
        cases = [('paired_price', (), (2, 5, 0, 0, 0, 0)),
                 ('paired_single_prices', (), (2, 15, 0, 0, 0, 0)),
                 ('single_cancel_suffix', (5,), (0,))]
        for initialized in (False, True):
            for name, prefix, path in cases:
                with self.subTest(initialized=initialized, case=name):
                    model = TwoGraphs(CONFIGURATIONS[2])
                    reference = copy.deepcopy(model)
                    optimizers = [torch.optim.AdamW(m.parameters(), lr=.001, weight_decay=.1, foreach=False)
                                  for m in (model, reference)]
                    if initialized:
                        # 仅构造合法已有优化器状态；验收步仍来自真实consume_actor的损失。
                        for current, optimizer in zip((model, reference), optimizers):
                            for p in current.parameters():
                                p.grad = torch.full_like(p, .007)
                            optimizer.step()
                            optimizer.zero_grad(set_to_none=True)
                    previous = [{key: p.detach().clone() for key, p in m.named_parameters()}
                                for m in (model, reference)]
                    old_states = [copy.deepcopy(opt.state_dict()) for opt in optimizers]
                    calls, log_probabilities = [], []
                    for current, repeated in ((model, False), (reference, True)):
                        consumer, group = prepared(current, self.native, self.history, reference=repeated)
                        invocations = Counter()
                        original = consumer.parameters
                        def scored(*args, **kwargs):
                            invocations[(0, 1, 2, 3, 5)[args[2][7:12].index(1)]] += 1
                            return original(*args, **kwargs)
                        consumer.parameters = scored
                        actor = at_prefix(self.native, prefix)
                        before_rng = torch.random.get_rng_state().clone()
                        result = consume_actor(consumer, group, 0, actor, replay=path)
                        self.assertTrue(torch.equal(before_rng, torch.random.get_rng_state()))
                        self.assertEqual(tuple(actor.path()), prefix + path)
                        result.log_prob.backward()
                        calls.append(dict(invocations))
                        log_probabilities.append(float(result.log_prob.detach()))
                    self.assertEqual(calls[0], calls[1])
                    self.assertNotIn(2, calls[0])
                    self.assertNotIn(5, calls[0])
                    if name == 'single_cancel_suffix':
                        self.assertEqual(calls[0], {})
                    gradients = compare_gradients(model, reference)
                    categories = {}
                    for key, p in model.named_parameters():
                        categories[key] = 'none' if p.grad is None else ('zero' if not torch.count_nonzero(p.grad) else 'nonzero')
                    self.assertEqual(categories['action.actor.variant.weight'], 'none')
                    self.assertEqual(categories['action.actor.cancel.3.weight'], 'none')
                    self.assertEqual(categories['action.actor.fee_weight.weight'],
                                     'zero' if name == 'paired_price' else 'none')
                    if name == 'single_cancel_suffix':
                        self.assertTrue(all(v == 'zero' for k, v in categories.items() if k.startswith('state.')))
                        self.assertTrue(all(v == 'none' for k, v in categories.items() if k.startswith('action.')))
                    for optimizer in optimizers:
                        optimizer.step()
                    left, right = optimizers[0].state_dict(), optimizers[1].state_dict()
                    self.assertEqual(left['param_groups'], right['param_groups'])
                    self.assertEqual(left['state'].keys(), right['state'].keys())
                    state_fields = 0
                    for index, ((key, p), (_, q)) in enumerate(zip(model.named_parameters(), reference.named_parameters())):
                        try:
                            torch.testing.assert_close(p, q, rtol=1e-4, atol=1e-5, msg=key)
                        except AssertionError:
                            position = int((p.detach() - q.detach()).abs().flatten().argmax())
                            scalar = lambda value: float(value.detach().flatten()[position])
                            details = {'case': name, 'existing_optimizer_state': initialized,
                                'prefix': prefix, 'path': path, 'calls': calls,
                                'log_probabilities': log_probabilities, 'parameter': key,
                                'flat_index': position, 'shape': list(p.shape),
                                'gradient_class': categories[key], 'gradient_comparison': gradients,
                                'max_parameter_error': float((p.detach() - q.detach()).abs().max()),
                                'before': [scalar(values[key]) for values in previous],
                                'after': [scalar(p), scalar(q)], 'gradients': [scalar(p.grad), scalar(q.grad)],
                                'gradient_parameter_max_error': float((p.grad - q.grad).abs().max()),
                                'optimizer_states': [{field: (float(value) if value.numel() == 1 else scalar(value))
                                    for field, value in state['state'][index].items()} for state in (left, right)]}
                            self.evidence.setdefault('optimizer_failures', []).append(details)
                            print(json.dumps(details, ensure_ascii=False), flush=True)
                            raise
                        self.assertEqual(index in left['state'], initialized or categories[key] != 'none', key)
                        if index in left['state']:
                            self.assertEqual(left['state'][index].keys(), right['state'][index].keys())
                            for field, value in left['state'][index].items():
                                torch.testing.assert_close(value, right['state'][index][field], rtol=1e-4, atol=1e-5, msg=key + ':' + field)
                                state_fields += 1
                        if categories[key] == 'none':
                            torch.testing.assert_close(p, previous[0][key], rtol=0, atol=0, msg=key)
                            if initialized:
                                for field, value in left['state'][index].items():
                                    torch.testing.assert_close(value, old_states[0]['state'][index][field], rtol=0, atol=0, msg=key + ':' + field)
                        elif categories[key] == 'zero':
                            # 独立AdamW公式核对零梯度仍推进状态、保留动量衰减并执行权重衰减。
                            before = old_states[0]['state'].get(index, {})
                            step = int(before.get('step', 0)) + 1
                            avg = before.get('exp_avg', torch.zeros_like(p)) * .9
                            square = before.get('exp_avg_sq', torch.zeros_like(p)) * .999
                            expected = previous[0][key] * (1 - .001 * .1) - (.001 / (1 - .9 ** step)) * avg / (square.sqrt() / (1 - .999 ** step) ** .5 + 1e-8)
                            self.assertEqual(int(left['state'][index]['step']), step)
                            torch.testing.assert_close(left['state'][index]['exp_avg'], avg, rtol=1e-4, atol=1e-5)
                            torch.testing.assert_close(left['state'][index]['exp_avg_sq'], square, rtol=1e-4, atol=1e-5)
                            torch.testing.assert_close(p, expected, rtol=1e-6, atol=1e-8, msg=key)
                    fee_key = 'action.actor.fee_weight.weight'
                    fee_delta = float((dict(model.named_parameters())[fee_key].detach() - previous[0][fee_key]).abs().max())
                    if name == 'paired_price':
                        self.assertGreater(fee_delta, 1e-7)
                    else:
                        self.assertEqual(fee_delta, 0.)
                    rows.append({'case': name, 'existing_optimizer_state': initialized, 'calls': calls[0],
                        'gradients': gradients, 'parameter_categories': categories, 'compared_optimizer_state_fields': state_fields,
                        'fee_weight_max_update': fee_delta, 'adamw_zero_gradient_parameter_tolerance': {'rtol': 1e-6, 'atol': 1e-8},
                        'unused_parameters_and_state_unchanged_exactly': True, 'zero_gradient_adamw_formula_matches': True})
        self.evidence['optimizer'] = rows


def run(native, history):
    torch.set_num_threads(1)
    evidence = {'scope': '#176 首轮审查两项P2的有限真实入口验收',
        'cash_pusd': '0.0006', 'ticks': ['0.01', '0.01'], 'minimum_order_size_shares': '5',
        'native_domain_is_not_a_model_mask': True}
    suite = unittest.TestSuite(NativeProbabilityTests(name, native, history, evidence) for name in (
        'test_complete_joint_distribution_entropy_and_weighted_surrogate',
        'test_actual_singletons_none_zero_and_adamw_state'))
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    evidence['tests_run'] = result.testsRun
    evidence['failures'] = len(result.failures)
    evidence['errors'] = len(result.errors)
    if path := os.environ.get('NATIVE_ACTION_PROBABILITY_OUTPUT'):
        Path(path).write_text(json.dumps(evidence, ensure_ascii=False, indent=2) + '\n')
    assert result.wasSuccessful(), '独立概率或单选优化器验收失败'
    return json.dumps(evidence, ensure_ascii=False, indent=2)
