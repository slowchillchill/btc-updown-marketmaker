"""新策略共享计算与独立重复准备的数学、梯度与优化器等价。"""
import copy
import unittest
import torch
from model_train.graphs import CONFIGURATIONS, TwoGraphs, action_stages, reduce_objects
from model_train.graph_fixtures import fixture, paired_gtc_prefixes


class StagedTests(unittest.TestCase):
    def test_shared_context_selected_rows_and_delayed_backward_match_repeated_reference(self):
        torch.set_num_threads(1)
        model=TwoGraphs(CONFIGURATIONS[2])
        reference=copy.deepcopy(model)
        _,_,rule,_,features,mask=fixture(1,86,True)
        prefix=paired_gtc_prefixes()[2]
        z=torch.linspace(-1,1,256).reshape(1,256)
        indices=torch.arange(19,30)
        fees=torch.linspace(0,.01,11).reshape(1,11)
        def loss(current,shared):
            graphs=action_stages(current.action)
            objects=graphs['action_objects'](features,mask)
            reduced,exists=reduce_objects([(objects,mask)])
            family,context=graphs['action_family'](z,rule,reduced,exists)
            outputs=[family]
            if shared:
                for name in ('action_quantity','action_variant'):
                    outputs.append(graphs[name](z,context,prefix))
                outputs.append(graphs['action_price'](z,context,prefix,indices,fees))
                decoder=graphs['action_cancel_context'](z,context,prefix)
                outputs.append(graphs['action_cancel'](decoder,objects,mask))
            else:
                full_fees=torch.zeros(1,999).index_copy(1,indices,fees)
                for head in (1,2,3,5):
                    # 每阶段独立重建对象与规则，与共享路径使用完全相同参数和数学。
                    encoded=current.action.object_encoder(features)
                    reduced,exists=reduce_objects([(encoded,mask)])
                    scores=current.action(z,rule,prefix,features,mask,reduced,exists,full_fees)[head]
                    outputs.append(scores.index_select(1,indices) if head==3 else scores)
            return sum(-torch.log_softmax(value,dim=-1)[0,0] for value in outputs)
        candidate=loss(model,True)
        expected=loss(reference,False)
        # 下一次前向不覆盖上一个决定仍被训练图引用的共享编码。
        later=loss(model,True)
        torch.testing.assert_close(candidate,expected,rtol=1e-4,atol=1e-5)
        candidate.backward();expected.backward()
        left,right=dict(model.named_parameters()),dict(reference.named_parameters())
        for name,p in left.items():
            q=right[name]
            self.assertEqual(p.grad is None,q.grad is None,name)
            if p.grad is not None:torch.testing.assert_close(p.grad,q.grad,rtol=1e-4,atol=1e-5,msg=name)
        optimizers=[torch.optim.AdamW(m.parameters(),lr=.001,weight_decay=.01) for m in (model,reference)]
        for opt in optimizers:opt.step()
        for name,p in left.items():torch.testing.assert_close(p,right[name],rtol=1e-4,atol=1e-5,msg=name)
        self.assertTrue(later.requires_grad)

    def test_price_rows_are_selected_before_matrix_multiplication(self):
        model=TwoGraphs(CONFIGURATIONS[2])
        graphs=action_stages(model.action)
        shapes=[]
        from torch.utils._python_dispatch import TorchDispatchMode
        class Shapes(TorchDispatchMode):
            def __torch_dispatch__(self,func,types,args=(),kwargs=None):
                if func==torch.ops.aten.addmm.default:shapes.append(tuple(args[2].shape))
                return func(*args,**(kwargs or {}))
        with Shapes():
            value=graphs['action_price'](torch.zeros(1,256),torch.zeros(1,128),torch.zeros(1,36),torch.arange(19,30),torch.zeros(1,11))
        self.assertEqual(value.shape,(1,11))
        self.assertIn((128,11),shapes)
        self.assertNotIn((128,999),shapes)

if __name__=='__main__':unittest.main()
