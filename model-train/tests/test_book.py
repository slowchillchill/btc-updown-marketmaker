"""#166 的表示反例、动态归约、梯度和混深度递推；不声明训练效果。"""
import gc
import unittest
import torch
from model_train.book import BookEncoder, pairwise_sum, reduce_levels
from model_train.graphs import BookState, Configuration, CONFIGURATIONS, TwoGraphs
from model_train.graph_fixtures import book_fixture, fixture
from model_train.components import AlignedLinear


class BookTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        torch.set_num_threads(1)

    def controlled_encoder(self):
        model = BookEncoder()
        with torch.no_grad():
            for p in model.parameters(): p.zero_()
            for encoder in (model.pm, model.binance):
                encoder[0].weight[0,1] = 1
                encoder[0].weight[0,8] = 100
                encoder[0].weight[0,3] = 1
                encoder[2].weight[0,0] = 1
        return model

    def test_compact_values_and_gradients_match_original_dense_coordinates(self):
        class OriginalDense(BookEncoder):
            def forward(self, levels, mask):
                values = torch.cat((self.pm(levels[:, :4]), self.binance(levels[:, 4:])), dim=1)
                return reduce_levels(values, mask).flatten(1)
        torch.manual_seed(168)
        compact = BookEncoder()
        dense = OriginalDense()
        for name in ('pm', 'binance'):
            setattr(dense, name, torch.nn.Sequential(AlignedLinear(9, 32), torch.nn.SiLU(), AlignedLinear(32, 32)))
        dense.load_state_dict(compact.state_dict())
        for depth, empty in ((1, False), (257, False), (4096, False), (1, True)):
            # 故意保留内部空洞、不同侧/批次深度，压紧后不能改归约坐标。
            x = torch.randn(2, 6, depth, 9)
            mask = torch.rand(2, 6, depth) > .3
            mask[0, :4] = False
            mask[1, 4:] = False
            if empty:
                mask[:] = False
            a = x.clone().requires_grad_()
            b = x.clone().requires_grad_()
            compact.zero_grad(); dense.zero_grad()
            actual, expected = compact(a, mask), dense(b, mask)
            torch.testing.assert_close(actual, expected, atol=0, rtol=0)
            upstream = torch.randn_like(actual) / depth
            (actual * upstream).sum().backward()
            (expected * upstream).sum().backward()
            torch.testing.assert_close(a.grad, b.grad, atol=1e-5, rtol=1e-4)
            self.assertEqual(int(a.grad[~mask].count_nonzero()), 0)
            for p, q in zip(compact.parameters(), dense.parameters()):
                torch.testing.assert_close(p.grad, q.grad, atol=1e-5, rtol=1e-4)

    def test_compact_standard_onnx_dynamic_and_empty_groups(self):
        import tempfile
        from pathlib import Path
        import onnx
        from model_train.book import lower_pairwise_sum
        from model_train.validate_graphs import session, compare
        model = BookEncoder().eval()
        B = torch.export.Dim('B', min=1); N = torch.export.Dim('N', min=1)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'book.onnx'
            program = torch.onnx.export(model, book_fixture(2, 7), dynamo=True, opset_version=18,
                input_names=['levels', 'mask'], output_names=['summary'],
                dynamic_shapes=({0:B, 2:N}, {0:B, 2:N}))
            graph = lower_pairwise_sum(program.model_proto)
            self.assertFalse(any(node.domain not in ('', 'ai.onnx') for node in graph.graph.node))
            onnx.save(graph, path)
            runtime = session(path, 1)
            with torch.no_grad():
                for batch, depth in ((1, 1), (3, 257), (1, 4096)):
                    for empty in ('none', 'pm', 'binance', 'all'):
                        levels, mask = book_fixture(batch, depth)
                        if empty in ('pm', 'all'): mask[:, :4] = False
                        if empty in ('binance', 'all'): mask[:, 4:] = False
                        expected = model(levels, mask)
                        actual = torch.from_numpy(runtime.run(None, {'levels': levels.numpy(), 'mask': mask.numpy()})[0])
                        compare((expected,), (actual,), {'batch': batch, 'depth': depth, 'empty': empty}, {})

    def test_price_collision_quantity_swap_keeps_association(self):
        # 两个价格投影相同；新增精确距离坐标仍独立，非归约舍入偶然差。
        model = self.controlled_encoder()
        x = torch.zeros(2,6,2,9)
        x[:,5,:,0] = torch.tensor(11.512935638427734)
        x[:,5,:,2] = 1
        x[:,5,1,8] = torch.log1p(torch.tensor(.01))
        x[0,5,:,1] = torch.log1p(torch.tensor([1.,2.]))
        x[1,5,:,1] = x[0,5,:,1].flip(0)
        mask = torch.zeros(2,6,2,dtype=torch.bool); mask[:,5] = True
        encoded = model(x,mask)
        self.assertGreater(abs(float((encoded[0,480]-encoded[1,480]).detach())), .01)
        x[:,:,:,8] = 0
        lost = model(x,mask)
        torch.testing.assert_close(lost[0],lost[1],rtol=0,atol=0)

    def test_depth_257_distribution_and_direct_time_reach_all_sides(self):
        model = self.controlled_encoder()
        levels, mask = book_fixture(1,257)
        # 两份合法257档盘口：仅互换第6/257档的1份/1000份，原前五档、
        # 价序、档数及每侧精确总量255*100+1+1000均不变。
        levels[:,:,5,1] = torch.log1p(torch.tensor(1.))
        levels[:,:,256,1] = torch.log1p(torch.tensor(1000.))
        with torch.no_grad():
            for encoder in (model.pm,model.binance):
                encoder[0].weight[0,8] = 10
                encoder[0].bias[0] = -5
        reference = model(levels,mask)
        changed = levels.clone(); changed[:,:,[5,256],1] = levels[:,:,[256,5],1]
        self.assertTrue(torch.equal(levels[:,:,:5],changed[:,:,:5]))
        self.assertTrue(torch.equal(levels[:,:,:,0],changed[:,:,:,0]))
        self.assertTrue(torch.equal(levels[:,:,:,1].sort(dim=2).values,changed[:,:,:,1].sort(dim=2).values))
        actual = model(changed,mask)
        for side in range(6):
            self.assertGreater(abs(float((reference[0,96*side]-actual[0,96*side]).detach())), .1)
        levels.requires_grad_()
        # 六侧sum路径均包括第257档，PM直接时间也确实参与编码。
        model(levels,mask)[:,::96].sum().backward()
        self.assertTrue(bool((levels.grad[0,:,256,1] > 0).all()))
        self.assertTrue(bool((levels.grad[0,:4,256,3] > 0).all()))

    def test_negative_max_first_tie_padding_empty_and_cancellation(self):
        x = torch.tensor([-2.,-2.,-4.,99.]).reshape(1,1,4,1).repeat(1,6,1,32).requires_grad_()
        mask = torch.tensor([True,True,True,False]).reshape(1,1,4).repeat(1,6,1)
        summary = reduce_levels(x,mask)
        torch.testing.assert_close(summary[...,:32],torch.full((1,6,32),-8.))
        torch.testing.assert_close(summary[...,64:],torch.full((1,6,32),-2.))
        summary[...,64:].sum().backward()
        self.assertTrue(bool((x.grad[:,:,0] == 1).all()))
        self.assertEqual(int(x.grad[:,:,1:].count_nonzero()),0)
        empty = reduce_levels(x,torch.zeros_like(mask))
        self.assertTrue(bool(torch.isfinite(empty).all()))
        self.assertEqual(int(empty.count_nonzero()),0)
        for n in (7,8,257,1001,4096):
            values = torch.zeros(1,6,n,32)
            values[:,:,0] = 1e20;values[:,:,1] = -1e20;values[:,:,2] = 1
            torch.testing.assert_close(pairwise_sum(values),torch.ones(1,6,32),atol=0,rtol=0)

    def test_independent_padding_keeps_outputs_gradients_and_recurrent_state(self):
        model = TwoGraphs(CONFIGURATIONS[2]).eval()
        fast,slow,*_ = fixture(2)
        hp = torch.zeros(1,1,256);hm = torch.zeros(1,2,256)
        for own,other in ((7,257),(1,8),(0,4096),(257,1001)):
            small,smask = book_fixture(1,own)
            wide,wmask = book_fixture(2,max(own,other))
            wide[0] = 0;wmask[0] = False
            if own:
                wide[0,:,:own] = small[0];wmask[0,:,:own] = True
            with torch.no_grad():
                zp,hp = model.state(fast[:1],slow[:1],small,smask,hp)
                zm,hm = model.state(fast,slow,wide,wmask,hm)
            torch.testing.assert_close(zp,zm[:1],atol=1e-5,rtol=1e-4)
            torch.testing.assert_close(hp,hm[:,:1],atol=1e-5,rtol=1e-4)
        book = self.controlled_encoder()
        small,mask = book_fixture(1,7);small.requires_grad_()
        a=book(small,mask);a.sum().backward()
        wide,wmask = book_fixture(1,257);wide[:, :, 7:]=0;wmask[:,:,7:]=False;wide.requires_grad_()
        b=book(wide,wmask);b.sum().backward()
        torch.testing.assert_close(a,b,atol=0,rtol=0)
        torch.testing.assert_close(small.grad,wide.grad[:,:,:7],atol=0,rtol=0)
        self.assertEqual(int(wide.grad[:,:,7:].count_nonzero()),0)

    def test_standard_onnx_loop_matches_dynamic_signed_reductions(self):
        import tempfile
        from pathlib import Path
        import onnx
        from model_train.book import lower_pairwise_sum
        from model_train.validate_graphs import session, compare
        class Reduction(torch.nn.Module):
            def forward(self, values, mask): return reduce_levels(values,mask)
        model=Reduction()
        B=torch.export.Dim('B',min=1);N=torch.export.Dim('N',min=1)
        values=torch.zeros(2,6,7,32);mask=torch.ones(2,6,7,dtype=torch.bool)
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'reduction.onnx'
            program=torch.onnx.export(model,(values,mask),dynamo=True,opset_version=18,optimize=True,
                input_names=['values','mask'],output_names=['summary'],dynamic_shapes=({0:B,2:N},{0:B,2:N}))
            graph=lower_pairwise_sum(program.model_proto)
            self.assertTrue(any(n.op_type=='Loop' for n in graph.graph.node))
            self.assertFalse(any(n.domain=='native_inputs' for n in graph.graph.node))
            onnx.save(graph,path);ort=session(path,1)
            stats={}
            for batch in (1,2,3):
                for n in (1,7,8,257,1001,4096):
                    x=torch.arange(n,dtype=torch.float32).reshape(1,1,n,1).repeat(batch,6,1,32)/17-13
                    if n >= 3:
                        x[:,:,0]=1e20;x[:,:,1]=-1e20;x[:,:,2]=1
                    m=torch.ones(batch,6,n,dtype=torch.bool);m[:,0]=False
                    m[:,-1,n//2:]=False
                    expected=model(x,m)
                    actual=torch.from_numpy(ort.run(None,{'values':x.numpy(),'mask':m.numpy()})[0])
                    compare((expected,),(actual,),{'B':batch,'N':n},stats)
                    torch.testing.assert_close(expected,actual,atol=0,rtol=0)

    def test_all_27_constructions_have_full_book_and_original_recurrent_architecture(self):
        # 仅构造/布局检查；实际有限导出仍为 CONFIGURATIONS 中四个结构代表。
        configurations = [Configuration(f'BASELINE-{l}x256',l,256,741001,True) for l in (1,2,3)]
        configurations += [Configuration(f'MAIN-{l}x{h}',l,h,741003) for l in (1,2,3,10,20,30) for h in (256,512,768,1024)]
        self.assertEqual(len(configurations),27)
        for c in configurations:
            # 逐个构造实际 v5 权重并释放；不冒称训练或27份图运行验收。
            model = BookState(c)
            self.assertEqual(tuple(model.book.pm[0].weight.shape),(32,9))
            self.assertEqual(tuple(model.book.binance[2].weight.shape),(32,32))
            if c.baseline:
                self.assertEqual(tuple(model.core.encoder[1].weight.shape),(256,1605))
                self.assertEqual(model.core.gru.num_layers,c.layers)
                self.assertFalse(hasattr(model.core,'gate'))
            else:
                self.assertEqual(tuple(model.core.fast_encoder[1].weight.shape),(c.hidden//2,1172))
                self.assertEqual(len(model.core.grus),c.layers)
            del model
        gc.collect()

if __name__ == '__main__': unittest.main()
