"""有限计算夹具，不代表生产特征表或最终策略图验收。"""

import gc
import unittest

import torch
from torch import nn
from torch.nn import functional as F

from model_train import ActorScores, BareGRU, FeatureEncoder, ResidualGRU


def leaf(tensor):
    return tensor.detach().clone().requires_grad_()


def linear(x, parameters, name):
    return F.linear(x, parameters[name + ".weight"], parameters[name + ".bias"])


def norm(x, parameters, name):
    weight = parameters[name + ".weight"]
    return F.layer_norm(x, weight.shape, weight, parameters[name + ".bias"], 1e-5)


def encode(x, parameters, name):
    x = norm(x, parameters, name + ".0")
    return linear(F.silu(linear(x, parameters, name + ".1")), parameters, name + ".3")


def fixture_weights(model):
    """给各张量赋予固定的非对称小权重，避免只检查随机成功路径。"""
    with torch.no_grad():
        for index, parameter in enumerate(model.parameters()):
            values = torch.arange(parameter.numel(), dtype=parameter.dtype)
            parameter.copy_((0.12 * torch.sin(values + index + 1)).reshape(parameter.shape))


def gru_reference(parameters, prefix, hidden_size, num_layers=1):
    gru = nn.GRU(hidden_size, hidden_size, num_layers=num_layers).double()
    gru.load_state_dict({name: parameters[prefix + name] for name in gru.state_dict()})
    return gru


class ComponentTests(unittest.TestCase):
    def setUp(self):
        self.enterContext(torch.random.fork_rng(devices=[]))
        torch.manual_seed(96)

    def assert_close(self, actual, expected):
        torch.testing.assert_close(actual, expected, rtol=1e-9, atol=1e-11)

    def assert_output_and_gradients(self, model, inputs, actual, expected, references):
        for value, reference in zip(actual, expected):
            self.assert_close(value, reference)
        sum(value.square().sum() for value in actual).backward()
        sum(value.square().sum() for value in expected).backward()
        for name, parameter in model.named_parameters():
            with self.subTest(parameter=name):
                self.assertIsNotNone(parameter.grad)
                self.assertTrue(torch.isfinite(parameter.grad).all().item())
                self.assert_close(parameter.grad, references[name].grad)
        for value, reference in inputs:
            self.assertIsNotNone(value.grad)
            self.assertTrue(torch.isfinite(value.grad).all().item())
            self.assert_close(value.grad, reference.grad)

    def test_feature_encoder_reference_and_independent_instances(self):
        model = FeatureEncoder(3, 8).double()
        fixture_weights(model)
        x = torch.tensor([[1.0, -2.0, 4.0], [-1.0, 3.0, 2.0]], requires_grad=True)
        x = leaf(x.double())
        reference_x = leaf(x)
        parameters = {"feature." + name: leaf(p) for name, p in model.named_parameters()}
        expected = encode(reference_x, parameters, "feature")
        self.assert_output_and_gradients(
            model, [(x, reference_x)], [model(x)], [expected],
            {name: parameters["feature." + name] for name, _ in model.named_parameters()},
        )
        other = FeatureEncoder(3, 8)
        for first, second in zip(model.parameters(), other.parameters()):
            self.assertNotEqual(first.data_ptr(), second.data_ptr())

    def test_residual_outputs_all_states_and_gradients_against_primitives(self):
        for layers, hidden, batch in [(1, 8, 1), (3, 12, 2)]:
            with self.subTest(layers=layers, hidden=hidden, batch=batch):
                model = ResidualGRU(3, 5, hidden, layers).double()
                fixture_weights(model)
                inputs = [leaf(torch.randn(batch, 3, dtype=torch.double)),
                          leaf(torch.randn(batch, 5, dtype=torch.double)),
                          leaf(torch.randn(layers, batch, hidden, dtype=torch.double))]
                fast, slow, previous = [leaf(x) for x in inputs]
                parameters = {name: leaf(p) for name, p in model.named_parameters()}
                joined = torch.cat((encode(fast, parameters, "fast_encoder"),
                                    encode(slow, parameters, "slow_encoder")), dim=-1)
                gate = torch.sigmoid(linear(joined, parameters, "gate"))
                z = norm(gate * joined + (1 - gate) * linear(joined, parameters, "value"),
                         parameters, "fusion_norm")
                states = []
                for layer in range(layers):
                    prefix = f"grus.{layer}."
                    reference_gru = gru_reference(parameters, prefix, hidden)
                    update, state = reference_gru(
                        norm(z, parameters, f"layer_norms.{layer}").unsqueeze(0),
                        previous[layer:layer + 1],
                    )
                    parameters.update({prefix + n: p for n, p in reference_gru.named_parameters()})
                    z = z + update[0]
                    states.append(state)
                self.assert_output_and_gradients(
                    model, list(zip(inputs, (fast, slow, previous))), model(*inputs),
                    (z, torch.cat(states)), parameters,
                )

    def test_bare_outputs_all_states_and_gradients_against_primitives(self):
        for layers, batch in [(1, 1), (3, 2)]:
            with self.subTest(layers=layers, batch=batch):
                model = BareGRU(3, 5, layers).double()
                fixture_weights(model)
                inputs = [leaf(torch.randn(batch, 3, dtype=torch.double)),
                          leaf(torch.randn(batch, 5, dtype=torch.double)),
                          leaf(torch.randn(layers, batch, 256, dtype=torch.double))]
                fast, slow, previous = [leaf(x) for x in inputs]
                parameters = {name: leaf(p) for name, p in model.named_parameters()}
                features = F.silu(linear(norm(torch.cat((fast, slow), -1), parameters,
                                              "encoder.0"), parameters, "encoder.1"))
                reference_gru = gru_reference(parameters, "gru.", 256, layers)
                expected, state = reference_gru(features.unsqueeze(0), previous)
                parameters.update({"gru." + n: p for n, p in reference_gru.named_parameters()})
                self.assert_output_and_gradients(
                    model, list(zip(inputs, (fast, slow, previous))), model(*inputs),
                    (expected[0], state), parameters,
                )

    def test_zero_gru_continuous_states_have_independent_expected_values(self):
        for residual in (False, True):
            for layers, batch in [(1, 1), (3, 2)]:
                with self.subTest(residual=residual, layers=layers, batch=batch):
                    hidden = 8 if residual else 256
                    model = (ResidualGRU(3, 5, hidden, layers) if residual
                             else BareGRU(3, 5, layers)).double()
                    grus = model.grus if residual else [model.gru]
                    with torch.no_grad():
                        for gru in grus:
                            for p in gru.parameters():
                                p.zero_()
                    fast = torch.randn(batch, 3, dtype=torch.double)
                    slow = torch.randn(batch, 5, dtype=torch.double)
                    previous = torch.arange(1, layers * batch * hidden + 1,
                                            dtype=torch.double).reshape(layers, batch, hidden) / 100
                    if residual:
                        # 将融合固定为手工向量，与任一 GRU 计算完全独立。
                        fusion = torch.linspace(-1, 1, hidden, dtype=torch.double)
                        with torch.no_grad():
                            model.fusion_norm.weight.zero_()
                            model.fusion_norm.bias.copy_(fusion)
                    state = previous
                    for step in (1, 2, 3):
                        z, state = model(fast, slow, state)
                        expected_state = previous * (0.5 ** step)
                        self.assert_close(state, expected_state)
                        expected_z = (fusion + expected_state.sum(0) if residual
                                      else expected_state[-1])
                        self.assert_close(z, expected_z)
                        if residual:
                            self.assertFalse(torch.allclose(z, state[-1]))

    def test_fast_and_slow_perturbations_reach_state(self):
        for model in (ResidualGRU(3, 5, 8, 2).double(), BareGRU(3, 5, 2).double()):
            hidden = 8 if isinstance(model, ResidualGRU) else 256
            fast = torch.tensor([[1., 2., -1.]], dtype=torch.double)
            slow = torch.tensor([[2., -3., 1., 0., 4.]], dtype=torch.double)
            previous = torch.zeros(2, 1, hidden, dtype=torch.double)
            original = model(fast, slow, previous)
            for changed in (model(fast + torch.tensor([[0., 1., 0.]]), slow, previous),
                            model(fast, slow + torch.tensor([[0., 1., 0., 0., 0.]]), previous)):
                for baseline, value in zip(original, changed):
                    self.assertFalse(torch.allclose(baseline, value, rtol=1e-9, atol=1e-11))

    def test_actor_reference_outputs_and_gradients(self):
        model = ActorScores(8).double()
        fixture_weights(model)
        inputs = [leaf(torch.randn(2, width, dtype=torch.double)) for width in (8, 4, 4, 4)]
        inputs.append(leaf(torch.randn(2, 3, 4, dtype=torch.double)))
        z, rule, prefix, summary, objects = [leaf(x) for x in inputs]
        parameters = {name: leaf(p) for name, p in model.named_parameters()}
        family = encode(torch.cat((z, rule, summary), -1), parameters, "family")
        decoder = F.silu(linear(norm(torch.cat((z, rule, prefix), -1), parameters,
                                    "decoder.0"), parameters, "decoder.1"))
        expected = {"family": family}
        for name in ("quantity", "variant", "price", "post_only", "expiry"):
            expected[name] = linear(decoder, parameters, name)
        # 按对象分别求值，独立于实现中的扩展和批量拼接路径。
        expected["cancel"] = torch.stack([
            encode(torch.cat((decoder, objects[:, index]), -1), parameters, "cancel")
            for index in range(3)
        ], dim=1)
        actual = model(*inputs)
        self.assertEqual(set(actual), set(expected))
        self.assert_output_and_gradients(
            model, list(zip(inputs, (z, rule, prefix, summary, objects))),
            list(actual.values()), [expected[name] for name in actual], parameters,
        )

    def test_actor_dependencies_objects_and_shared_heads(self):
        model = ActorScores(8).double()
        inputs = [torch.randn(2, width, dtype=torch.double) for width in (8, 4, 4, 4)]
        inputs.append(torch.randn(2, 3, 4, dtype=torch.double))
        base = model(*inputs)
        changed_prefix = list(inputs)
        changed_prefix[2] = inputs[2] + torch.tensor([0., 1., 0., 0.])
        changed_summary = list(inputs)
        changed_summary[3] = inputs[3] + torch.tensor([1., 0., 0., 0.])
        prefix_scores = model(*changed_prefix)
        summary_scores = model(*changed_summary)
        self.assert_close(prefix_scores["family"], base["family"])
        self.assertFalse(torch.allclose(summary_scores["family"], base["family"]))
        for name, width in [("quantity", 2), ("variant", 4), ("price", 9999),
                            ("post_only", 2), ("expiry", 3)]:
            self.assertEqual(base[name].shape, (2, width))
            self.assert_close(summary_scores[name], base[name])
            self.assertFalse(torch.allclose(prefix_scores[name], base[name]))
        self.assertEqual(
            {name for name, module in model.named_modules() if isinstance(module, nn.Linear)},
            {"family.1", "family.3", "decoder.1", "quantity", "variant", "price",
             "post_only", "expiry", "cancel.1", "cancel.3"},
        )
        self.assertEqual(base["family"].shape, (2, 6))
        self.assertEqual(base["cancel"].shape, (2, 3, 1))
        self.assert_close(summary_scores["cancel"], base["cancel"])
        permutation = [2, 0, 1]
        shuffled = model(*inputs[:4], inputs[4][:, permutation])
        self.assert_close(shuffled["cancel"], base["cancel"][:, permutation])
        changed_objects = inputs[4].clone()
        changed_objects[:, 1, 0] += 1
        changed = model(*inputs[:4], changed_objects)
        self.assert_close(changed["cancel"][:, [0, 2]], base["cancel"][:, [0, 2]])
        self.assertFalse(torch.allclose(changed["cancel"][:, 1], base["cancel"][:, 1]))
        self.assertEqual(model(*inputs[:4], inputs[4][:, :0])["cancel"].shape, (2, 0, 1))

    def test_27_formal_configurations_construct_sequentially(self):
        count = 0
        for layers in (1, 2, 3, 10, 20, 30):
            for hidden in (256, 512, 768, 1024):
                with self.subTest(layers=layers, hidden=hidden):
                    model = ResidualGRU(3, 5, hidden, layers)
                    actor = ActorScores(hidden)
                    self.assertEqual(len(model.grus), layers)
                    self.assertEqual(len(model.layer_norms), layers)
                    for encoder, width in ((model.fast_encoder, 3), (model.slow_encoder, 5)):
                        self.assertEqual([type(m) for m in encoder],
                                         [nn.LayerNorm, nn.Linear, nn.SiLU, nn.Linear])
                        self.assertEqual(encoder[0].normalized_shape, (width,))
                        self.assertEqual(encoder[1].weight.shape, (hidden // 2, width))
                        self.assertEqual(encoder[3].weight.shape, (hidden // 2, hidden // 2))
                    self.assertEqual(model.gate.weight.shape, (hidden, hidden))
                    self.assertEqual(model.value.weight.shape, (hidden, hidden))
                    for gru in model.grus:
                        self.assertIs(type(gru), nn.GRU)
                        self.assertEqual(gru.weight_ih_l0.shape, (3 * hidden, hidden))
                        self.assertEqual(gru.weight_hh_l0.shape, (3 * hidden, hidden))
                        self.assertEqual(gru.num_layers, 1)
                        self.assertEqual(gru.dropout, 0)
                    for name, width in (("quantity", 2), ("variant", 4), ("price", 9999),
                                        ("post_only", 2), ("expiry", 3)):
                        self.assertEqual(getattr(actor, name).weight.shape, (width, hidden // 2))
                    self.assertEqual(actor.family[1].weight.shape, (hidden // 2, 2 * hidden))
                    self.assertEqual(actor.family[3].weight.shape, (6, hidden // 2))
                    self.assertEqual(actor.decoder[1].weight.shape, (hidden // 2, 2 * hidden))
                    self.assertEqual(actor.cancel[1].weight.shape, (hidden // 2, hidden))
                    self.assertEqual(actor.cancel[3].weight.shape, (1, hidden // 2))
                    self.assert_unique_parameters(model, actor)
                    # 释放循环局部对子模块的引用，下一配置不保留前一配置。
                    del encoder, gru, model, actor
                    gc.collect()
                    count += 1
        for layers in (1, 2, 3):
            model = BareGRU(3, 5, layers)
            self.assertEqual([type(m) for m in model.encoder], [nn.LayerNorm, nn.Linear, nn.SiLU])
            self.assertEqual(model.encoder[0].normalized_shape, (8,))
            self.assertEqual(model.encoder[1].weight.shape, (256, 8))
            self.assertIs(type(model.gru), nn.GRU)
            self.assertEqual(model.gru.num_layers, layers)
            for layer in range(layers):
                self.assertEqual(getattr(model.gru, f"weight_ih_l{layer}").shape, (768, 256))
                self.assertEqual(getattr(model.gru, f"weight_hh_l{layer}").shape, (768, 256))
            self.assert_unique_parameters(model)
            del model
            gc.collect()
            count += 1
        self.assertEqual(count, 27)

    def assert_unique_parameters(self, *models):
        parameters = [p for model in models
                      for _, p in model.named_parameters(remove_duplicate=False)]
        self.assertEqual(len(parameters), len({p.data_ptr() for p in parameters}))
        for model in models:
            self.assertFalse(any(isinstance(m, (nn.Dropout, nn.modules.batchnorm._BatchNorm,
                                               nn.MultiheadAttention)) for m in model.modules()))


if __name__ == "__main__":
    unittest.main()
