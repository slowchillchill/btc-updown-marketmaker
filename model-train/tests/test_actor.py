import unittest
import torch
from model_train.actor import conditional_choice
from model_train.graphs import CONFIGURATIONS, TwoGraphs
from model_train.graph_fixtures import fixture


class ActorTests(unittest.TestCase):
    def test_normalization_tie_singleton_rng_and_replay(self):
        logits = torch.tensor([2., 100., 2.], requires_grad=True)
        selected, logp, entropy = conditional_choice(logits, [True,False,True], deterministic=True)
        self.assertEqual(selected, 0)
        self.assertAlmostEqual(float(logp.detach()), -float(torch.log(torch.tensor(2.))), places=6)
        self.assertAlmostEqual(float(entropy.detach()), float(torch.log(torch.tensor(2.))), places=6)
        state = torch.random.get_rng_state().clone()
        selected, one_logp, one_entropy = conditional_choice(logits, [False,True,False])
        self.assertEqual(selected, 1)
        self.assertEqual(float(one_logp.detach()), 0)
        self.assertEqual(float(one_entropy.detach()), 0)
        self.assertTrue(torch.equal(state, torch.random.get_rng_state()))
        for selected in [0,2]:
            _, replay, _ = conditional_choice(logits, [True,False,True], selected=selected)
            self.assertTrue(torch.equal(replay, logp))
        logp.backward()
        self.assertEqual(float(logits.grad[1]), 0)

    def test_fee_input_only_changes_price_and_has_signed_trainable_gradient(self):
        torch.set_num_threads(1)
        model=TwoGraphs(CONFIGURATIONS[2])
        fast,slow,rule,prefix,objects,mask=fixture(2,1,True)
        z,_=model.state(fast,slow,torch.zeros(1,2,256))
        args=(z,rule,prefix,objects,mask,torch.zeros(2,384),torch.zeros(2,dtype=torch.bool))
        zero=model.action(*args,torch.zeros(2,9999))
        fee=torch.zeros(2,9999);fee[0,0]=.75;fee[0,1]=1.25
        with torch.no_grad():model.action.actor.fee_weight.weight.fill_(-.5)
        output=model.action(*args,fee)
        for i in [0,1,2,4,5,6,7]:self.assertTrue(torch.equal(output[i],zero[i]))
        self.assertTrue(torch.equal(output[3][1],zero[3][1]))
        delta=output[3][0]-zero[3][0]
        self.assertAlmostEqual(float(delta[0].detach()/delta[1].detach()),.6,places=5)
        output[3][0,0].backward()
        self.assertGreater(float(model.action.actor.fee_weight.weight.grad.abs().sum()),0)

if __name__ == '__main__':unittest.main()
