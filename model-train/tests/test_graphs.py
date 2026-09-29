"""最终两图的原语对照、两遍梯度与宿主分块失败条件。"""
import unittest
import torch
from torch import nn
from torch.nn import functional as F
from model_train.graphs import CONFIGURATIONS, TwoGraphs, reduce_objects, encode_all, score_all, active_rows
from model_train.graph_fixtures import fixture, cancel_slices, account_slice, book_fixture


def norm(x, p, name):
    w=p[name+'.weight'];return F.layer_norm(x,w.shape,w,p[name+'.bias'],1e-5)


def linear(x,p,name):return F.linear(x,p[name+'.weight'],p[name+'.bias'])


def encode(x,p,name):return linear(F.silu(linear(norm(x,p,name+'.0'),p,name+'.1')),p,name+'.3')


class GraphTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):torch.set_num_threads(1)

    def test_action_against_independent_primitives_and_no_gru(self):
        model=TwoGraphs(CONFIGURATIONS[2]).eval();p=dict(model.action.named_parameters())
        fast,slow,rule,prefix,objects,mask=fixture(2,1,True)
        z,h=model.state(fast,slow,*book_fixture(fast.shape[0]),torch.zeros(1,2,256));saved=h.clone()
        calls=[]
        handles=[m.register_forward_hook(lambda *args:calls.append(1)) for m in model.modules() if isinstance(m,nn.GRU)]
        reduced,exists=encode_all(model.action,z,rule,objects,mask,[1,1],empty=False)
        output=model.action(z,rule,prefix,objects,mask,reduced,exists)
        obj=encode(objects,p,'object_encoder');r=encode(rule,p,'rule');pre=encode(prefix,p,'prefix')
        summary=encode(reduced,p,'summary')
        family=encode(torch.cat((z,r,summary),-1),p,'actor.family')
        dec=F.silu(linear(norm(torch.cat((z,r,pre),-1),p,'actor.decoder.0'),p,'actor.decoder.1'))
        expected=[family]+[linear(dec,p,'actor.'+head) for head in ('quantity','variant','price')]
        expected += [obj,torch.stack([encode(torch.cat((dec,obj[:,i]),-1),p,'actor.cancel').squeeze(-1) for i in range(2)],1)]
        for a,b in zip(output,expected):torch.testing.assert_close(a,b,atol=1e-5,rtol=1e-4)
        self.assertEqual(calls,[]);torch.testing.assert_close(h,saved,atol=0,rtol=0)
        for handle in handles:handle.remove()
        pointers=[p.data_ptr() for _,p in model.named_parameters(remove_duplicate=False)]
        self.assertEqual(len(pointers),len(set(pointers)))
        self.assertFalse(any(isinstance(m,nn.GRU) for m in model.action.modules()))

    def test_global_reduction_order_padding_and_first_max_gradient(self):
        x=torch.tensor([[[1.,2.],[4.,2.],[1000.,1000.],[4.,-3.]]],requires_grad=True)
        mask=torch.tensor([[True,True,False,True]])
        reduced,exists=reduce_objects([(x[:,:2],mask[:,:2]),(x[:,2:],mask[:,2:])])
        expected=torch.tensor([[9.,1.,3.,1/3,4.,2.]])
        torch.testing.assert_close(reduced,expected);self.assertTrue(exists.item())
        reduced[:,4:].sum().backward()
        torch.testing.assert_close(x.grad,torch.tensor([[[0.,1.],[1.,0.],[0.,0.],[0.,0.]]]))
        y=torch.tensor([[[1e20],[-1e20],[1.]]],dtype=torch.float32)
        r,_=reduce_objects([(y,torch.ones(1,3,dtype=torch.bool))])
        self.assertEqual(r[0,0].item(),1.)

    def test_two_paths_to_shared_object_encoder_and_split_gradients(self):
        model=TwoGraphs(CONFIGURATIONS[2]).eval()
        fast,slow,rule,prefix,objects,mask=fixture(1,1,False)
        with torch.no_grad():z,_=model.state(fast,slow,*book_fixture(fast.shape[0]),torch.zeros(1,1,256))
        grads=[]
        for lengths in ([4],[1,3]):
            model.zero_grad()
            reduced,exists=encode_all(model.action,z,rule,objects,mask,lengths,empty=False)
            family=model.action(z,rule,prefix,objects[:,:1],mask[:,:1],reduced,exists)[0]
            family.square().sum().backward()
            grad=model.action.object_encoder[1].weight.grad.clone()
            self.assertGreater(grad.abs().sum().item(),0);grads.append(grad)
        torch.testing.assert_close(*grads,atol=1e-5,rtol=1e-4)
        # 真实N259、合法Single A/B两类；隔离直接路径并比较三种块划分。
        fast,slow,rule,prefix,objects,mask=fixture(1,86,False)
        with torch.no_grad():
            z,_=model.state(fast,slow,*book_fixture(fast.shape[0]),torch.zeros(1,1,256))
        prefix = torch.zeros_like(prefix)
        prefix[:,3] = 1; prefix[:,6] = 1; prefix[:,11] = 1
        singles = mask & (objects[:,:,0] == 1)
        direct_grads=[]
        for lengths in ([256,3],[128,128,3],[86,86,86,1]):
            model.zero_grad()
            reduced,exists=encode_all(model.action,z,rule,objects,mask,lengths,empty=False)
            scores=score_all(model.action,z,rule,prefix,objects,singles,lengths,reduced.detach(),exists,empty=False)[0]
            loss=-torch.distributions.Categorical(logits=scores).log_prob(torch.tensor(0))
            loss.backward()
            grad=model.action.object_encoder[1].weight.grad.clone()
            self.assertTrue(torch.isfinite(grad).all())
            self.assertGreater(grad.norm().item(),0.01)
            direct_grads.append(grad)
        for grad in direct_grads[1:]:
            torch.testing.assert_close(grad,direct_grads[0],atol=1e-5,rtol=1e-4)

    def test_empty_summary_mask_and_active_rows(self):
        model=TwoGraphs(CONFIGURATIONS[2]).eval();fast,slow,rule,prefix,objects,mask=fixture(2,empty_probe=True)
        z,h=model.state(fast,slow,*book_fixture(fast.shape[0]),torch.zeros(1,2,256))
        reduced,exists=encode_all(model.action,z,rule,objects,mask,[1],empty=True)
        self.assertFalse(exists.any());self.assertEqual(reduced.count_nonzero(),0)
        with torch.no_grad():model.action.summary[-1].bias.fill_(99)
        captured=[];hook=model.action.actor.register_forward_pre_hook(lambda m,args:captured.append(args[3]))
        args=(z,rule,prefix,objects,mask,reduced,exists)
        expected=model.action(*args)
        self.assertEqual(captured[0].count_nonzero(),0);hook.remove()
        rows,output=active_rows(model.action,torch.tensor([1]),*args)
        for a,b in zip(output,expected):torch.testing.assert_close(a,b[1:2])
        calls=[]
        rows,output=active_rows(lambda *x:calls.append(1),torch.tensor([],dtype=torch.long),*args)
        self.assertIsNone(output);self.assertEqual(calls,[])

    def test_host_padding_shapes_and_logical_trim(self):
        from model_train.graphs import object_blocks
        _,_,_,_,features,mask=fixture(2,86,True,mixed=True)
        blocks=list(object_blocks(features,mask,[128,129],empty=False))
        self.assertEqual([f.shape[1] for f,_,_ in blocks],[256,256])
        for f,m,n in blocks:
            self.assertEqual(f[:,n:].count_nonzero(),0)
            self.assertFalse(m[:,n:].any())
        _,_,_,_,features,mask=fixture(2)
        self.assertEqual(next(object_blocks(features,mask,[1],empty=True))[0].shape[1],1)

    def test_legal_fixture_counts_cash_and_masks(self):
        for pair,removed,n in ((0,False,1),(1,True,2),(85,False,256),(86,True,257),(86,False,259)):
            self.assertEqual(len(cancel_slices(pair,removed)),n)
        single,market=cancel_slices(1,True)
        self.assertEqual(single.values[7],1) # B不得搬到A。
        self.assertEqual(single.values[6],0)
        self.assertEqual(single.values[17],1) # post-only true
        self.assertEqual(single.values[26:29],[0,0,0]) # LIMIT spend不适用
        self.assertEqual(market.values[6:142],[0]*136)
        slow=account_slice().values
        self.assertEqual(slow[382:388],[0]*6) # 空订单年龄不适用
        self.assertEqual(slow[394:400],[0]*6)
        fast,slow,rule,*_=fixture()
        torch.testing.assert_close(fast[:,566:],rule,atol=0,rtol=0)

if __name__=='__main__':unittest.main()
