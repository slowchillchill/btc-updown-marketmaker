#!/usr/bin/env python3
"""读取本次CLI返回；小时差额已由原运行低频精确读取，未输出的计时保持未知。"""
import argparse
import json
from pathlib import Path


def summarize(document):
    output = []
    for day in document['days']:
        for candidate in day['candidates']:
            report = candidate['report']
            research = report.get('research')
            output.append(dict(run_id=candidate['run_id'], initial_available_pusd=candidate['initial_available_pusd'],
                               termination=report['end_reason'], published_checkpoint=day.get('published_checkpoint'),
                               research=research, reused=candidate['reused'],
                               persistence='BudgetStop仅在成功发布后返回；部分返回报告不放completed。',
                               stage_timing='仅日志已输出的原stage-timing汇总；本JSON未携带的值为unknown'))
    return dict(candidates=output, interpretation='连续六小时账户窗口；缺失小时不补零，不日化，不推断真实盈利。')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('input', type=Path)
    parser.add_argument('output', type=Path)
    args = parser.parse_args()
    result = summarize(json.loads(args.input.read_text()))
    with args.output.open('x') as output:
        json.dump(result, output, ensure_ascii=False, indent=2)
        output.write('\n')


if __name__ == '__main__':
    main()
