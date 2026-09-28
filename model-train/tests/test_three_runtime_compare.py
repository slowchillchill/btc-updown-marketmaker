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

    def test_actor_reports_supported_score_before_choice_and_ignores_masked_slots(self):
        from model_train.actor_validation import compare_actor_steps
        prefix = [0.] * 36
        reference = ([True, False, True], 'CancelObject', prefix, [.1807526946, 0., .1807527840])
        actual = ([True, False, True], 'CancelObject', prefix, [.1807528138, float('nan'), .1807527840])
        # 原事故：有效分数在数值门内，但严格赢家反转仍必须失败。
        with self.assertRaisesRegex(AssertionError, 'strict choice mismatch'):
            compare_actor_steps([2], [reference], [0], [actual], {'case': 'original-market-choice'}, {})
        # 无效位置不参与比较；真实分数出门必须先于选择差异报告。
        invalid_score = (*actual[:3], [1., float('nan'), .1807527840])
        with self.assertRaises(AssertionError) as caught:
            compare_actor_steps([2], [reference], [0], [invalid_score], {}, {})
        self.assertIn('reference', caught.exception.args[0])
        compare_actor_steps([2], [reference], [2], [(*reference[:3], [.1807526946, float('nan'), .1807527840])], {}, {})
