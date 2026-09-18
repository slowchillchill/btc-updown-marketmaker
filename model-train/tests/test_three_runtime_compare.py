"""验收比较器的真实失败条件，避免容差方向或非有限值导致假通过。"""
import unittest
import torch
from model_train.validate_three_runtimes import compare


class ThreeRuntimeComparisonTests(unittest.TestCase):
    def test_nonfinite_and_shape_fail(self):
        for value in (float('nan'), float('inf'), -float('inf')):
            with self.assertRaises(AssertionError):
                compare((torch.tensor([0.]),), ([value],), {}, {'max_absolute_error':0.,'comparisons':0})
        with self.assertRaises(AssertionError):
            compare((torch.zeros(2),), ([0.],), {}, {'max_absolute_error':0.,'comparisons':0})

    def test_reference_tolerance_and_exact_selection(self):
        # rtol若错误基于actual，会接受这个位于两条阈值之间的差异。
        with self.assertRaises(AssertionError):
            compare((torch.tensor([0.012997299432754517]),), ([0.013008599169552326],), {}, {'max_absolute_error':0.,'comparisons':0})
        with self.assertRaises(AssertionError):
            compare((torch.tensor([10000.]),), ([10001.],), {}, {'max_absolute_error':0.,'comparisons':0},exact=True)
        stats={'max_absolute_error':0.,'comparisons':0}
        compare((torch.tensor([]),), ([],), {}, stats)
        self.assertEqual(stats['comparisons'],1)
