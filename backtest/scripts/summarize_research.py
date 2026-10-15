#!/usr/bin/env python3
"""从 CLI 已有结果提取经营结果，不扫描、重演或补齐历史反馈。"""
import json
import sys
from pathlib import Path


def summarize(config, result):
    scopes = {candidate['run']['session']['run_id']: day['preparation']['scope']
              for day in config['days'] for candidate in day['candidates']}
    candidates = []
    for day in result.get('days', []):
        for candidate in day['candidates']:
            report = candidate.get('report') or {}
            scope = scopes[candidate['run_id']]
            research = report.get('research') or {}
            complete = (candidate.get('outcome', 'Complete') == 'Complete'
                        and report.get('end_reason') == 'RangeEnd'
                        and report.get('recording_drained') is True
                        and report.get('last_observed_at_utc_ms') == scope['received_end_utc_ms'] - 1)
            candidates.append({
                'run_id': candidate['run_id'], 'requested_scope': scope,
                'initial_pusd': candidate['initial_available_pusd'],
                'outcome': candidate.get('outcome'), 'error': candidate.get('error'),
                'termination': report.get('end_reason'), 'full_window_range_end': complete,
                'hours': research.get('hours', []), 'final_snapshot': research.get('final_snapshot'),
                'account_economics_memory': report.get('account_economics_memory'),
                'conservative_nav_pusd': report.get('conservative_nav_pusd'),
                'day_economics': report.get('day_economics'), 'source_health': report.get('source_health'),
                'calculation_count': report.get('calculation_count'), 'failed_batch': report.get('failed_batch'),
                'observation_cutoff': report.get('observation_cutoff'),
                'durable_noe_prefix': report.get('durable_noe_prefix'),
                'public_applied_inputs': day['public_applied_inputs'],
            })
    return {'status': result.get('status', 'returned'), 'error': result.get('error'),
            'failure_source': result.get('failure_source'), 'candidates': candidates,
            'all_candidates_complete_window': (len(candidates) == len(scopes)
                and bool(candidates) and all(c['full_window_range_end'] for c in candidates)),
            'limits': '未决持仓保持原估值，未到账返佣不计现金。部分报告不补未来反馈；并发阶段耗时不能相加当墙钟。'}


if __name__ == '__main__':
    config_path, result_path, output = map(Path, sys.argv[1:])
    summary = summarize(json.loads(config_path.read_text()), json.loads(result_path.read_text()))
    with output.open('x') as stream:
        json.dump(summary, stream, ensure_ascii=False, indent=2)
        stream.write('\n')
    print(json.dumps({'candidate_count': len(summary['candidates']),
                      'all_candidates_complete_window': summary['all_candidates_complete_window']}, ensure_ascii=False))
