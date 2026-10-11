import json
from pathlib import Path
import unittest
from prepare_six_hour import prepare, NAMES, START, END
from summarize_six_hour import summarize


class SixHourPreparation(unittest.TestCase):
    def test_full_fold_zero_parameters_and_independent_continuous_accounts(self):
        business = json.loads((Path(__file__).resolve().parents[2] / 'baseline-mm/configs/strategies/complementary_pair/business-candidates-20261008.json').read_text())
        template = dict(rules=dict(snapshot_path='old', expected_file_sha256='fixed', replay_causal_start_utc_ms=START-1), days=[dict(
            preparation=dict(scope={}, header={}, metadata_initialization=dict(files=[dict(manifest_path='old')], markets=['original-selection'])),
            candidates=[dict(run=dict(session=dict(initial_account_state={}), strategy={}, admission_policy='DualCashNoMarketLimit'), strategy={}, request_namespace='old')])])
        implementation = dict(git_commit='reviewed-source', source_identity_sha256='source', cargo_lock_sha256='lock')
        result = prepare(template, business, Path('raw'), Path('output'), implementation, Path('rules.json'), Path('finality.json'))
        day = result['days'][0]
        expected = {c['identifier']:c['strategy'] for c in business['folds'][0]['candidates']}
        self.assertEqual(len(day['candidates']), 7)
        self.assertEqual(result['parallel_dates'], 1)
        self.assertEqual(day['preparation']['scope']['received_end_utc_ms'], END)
        self.assertEqual(day['preparation']['metadata_initialization']['received_end_utc_ms'], END)
        self.assertEqual(day['preparation']['metadata_initialization']['markets'], ['original-selection'])
        self.assertEqual(result['rules']['expected_file_sha256'], 'fixed')
        accounts=set()
        for name, candidate in zip(NAMES,day['candidates']):
            self.assertEqual(candidate['strategy'], expected[name])
            self.assertEqual(candidate['run']['strategy']['effective_parameters'], expected[name])
            self.assertEqual(candidate['run']['session']['initial_account_state']['available_pusd'], '2000')
            self.assertEqual(candidate['run']['timing']['calculation_duration_ms'], {'Fixed':15})
            self.assertEqual(candidate['run']['implementation_identity'], implementation)
            accounts.add(candidate['run']['session']['account_id'])
        self.assertEqual(len(accounts),7)
        self.assertEqual(template['days'][0]['candidates'][0]['request_namespace'],'old')

    def test_partial_result_keeps_missing_hours_and_last_published_generation(self):
        document=dict(days=[dict(published_checkpoint=dict(generation=3), candidates=[dict(run_id='one', initial_available_pusd='2000', reused=False,
                       report=dict(end_reason='BudgetStop',research=dict(decisions=None,hours=[])))])])
        result=summarize(document)['candidates'][0]
        self.assertEqual(result['published_checkpoint']['generation'],3)
        self.assertEqual(result['research']['hours'],[])
        self.assertIsNone(result['research']['decisions'])
        self.assertEqual(result['termination'],'BudgetStop')


if __name__ == '__main__':
    unittest.main()
