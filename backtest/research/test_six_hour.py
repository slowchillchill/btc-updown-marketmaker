import json
import subprocess
import sys
import tempfile
from pathlib import Path
import unittest
from prepare_six_hour import prepare, NAMES, START, END
from summarize_six_hour import summarize


class SixHourPreparation(unittest.TestCase):
    def test_full_fold_zero_parameters_and_fresh_candidate_output_parents(self):
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

        # 正式启动曾在File::create(run.json)处失败：配置已生成，但账户父目录不存在。
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output_root = root / 'fresh-output'
            template_path = root / 'template.json'
            implementation_path = root / 'implementation.json'
            template_path.write_text(json.dumps(template))
            implementation_path.write_text(json.dumps(implementation))
            prospective = prepare(template, business, root / 'raw', output_root,
                                  implementation, root / 'rules.json', root / 'finality.json')
            for candidate in prospective['days'][0]['candidates']:
                self.assertFalse(Path(candidate['run']['metadata_path']).parent.exists())
            business_path = Path(__file__).resolve().parents[2] / 'baseline-mm/configs/strategies/complementary_pair/business-candidates-20261008.json'
            subprocess.run([sys.executable, str(Path(__file__).with_name('prepare_six_hour.py')),
                            '--template', str(template_path), '--business-candidates', str(business_path),
                            '--dataset-root', str(root / 'raw'), '--output-root', str(output_root),
                            '--implementation-json', str(implementation_path), '--rules', str(root / 'rules.json'),
                            '--finality', str(root / 'finality.json')], check=True, capture_output=True, text=True)
            generated = json.loads((output_root / 'configs/research-days.json').read_text())
            for candidate in generated['days'][0]['candidates']:
                run = candidate['run']
                self.assertEqual(run['admission_policy'], 'ActualCashNoMarketLimit')
                metadata = Path(run['metadata_path'])
                self.assertTrue(metadata.parent.is_dir())
                self.assertEqual(Path(run['sim_log']).parent, metadata.parent)
                self.assertEqual(Path(run['result_path']).parent, metadata.parent)
                with metadata.open('x') as file:
                    file.write('{}')
                self.assertFalse(Path(run['noe_root']).exists())
                self.assertFalse(Path(run['ledger_root']).exists())

    def test_partial_result_keeps_missing_hours_and_real_termination(self):
        document=dict(days=[dict(candidates=[dict(run_id='one', initial_available_pusd='2000',
                       report=dict(end_reason='BudgetStop',research=dict(decisions=None,hours=[])))])])
        result=summarize(document)['candidates'][0]
        self.assertNotIn('published_checkpoint',result)
        self.assertEqual(result['research']['hours'],[])
        self.assertIsNone(result['research']['decisions'])
        self.assertEqual(result['termination'],'BudgetStop')


if __name__ == '__main__':
    unittest.main()
