"""当前有限概率研究；准备与冻结评价分阶段执行。"""
from __future__ import annotations
import argparse
import hashlib
import json
from pathlib import Path
from source_health.cli import Progress, connection, save, configure as health_configure
from . import direct, evaluation
from .evaluation import DAY


def configure(args):
    # 复用已准入清单身份和原显式初始化。
    health_configure(args.plan,args.input_root,args.run_root,args.boundary_helper,args.boundary_helper,args.output)
    config=json.loads(args.output.read_text());config.pop('statistics_helper')
    config['probability_helper']=str(args.probability_helper.resolve())
    import subprocess
    config['implementation_commit']=subprocess.check_output(['git','rev-parse','HEAD'],cwd=Path(__file__).resolve().parents[3],text=True).strip()
    config['probability_helper_sha256']=hashlib.sha256(args.probability_helper.read_bytes()).hexdigest()
    args.output.write_text(json.dumps(config,ensure_ascii=False,indent=2)+'\n')


def prepare(config_path, reuse_material=None):
    config=json.loads(config_path.read_text());root=Path(config['run_root']);root.mkdir(exist_ok=True)
    material=root/'material';material.mkdir(exist_ok=True)
    if (material/'material.json').exists():
        raise ValueError('prepare already complete; use a new run root with --reuse-material')
    projection=root/'projection';projection.mkdir(exist_ok=True)
    attempt=1
    while (projection/f'attempt-{attempt}').exists():attempt+=1
    work=projection/f'attempt-{attempt}';db=connection(work,config['engine'])
    if hashlib.sha256(Path(config['probability_helper']).read_bytes()).hexdigest()!=config['probability_helper_sha256']:
        raise ValueError('probability helper identity changed after configuration')
    progress=Progress(db,config['progress_interval_seconds'],work);boundary=direct.Boundary(config['probability_helper'])
    try:
        from .stages import CompletedStages, identity
        from dpt_health.direct import QueryUnit
        from dataclasses import asdict
        stages=CompletedStages(material,config,reuse_material)
        key=identity(config,'index');cached=stages.load('index',key)
        if cached is None:
            ranges=direct.direct.range_index(db,config['files'],progress,config['native_unit_span'])
            units=list(direct.direct.query_units(ranges,config['native_unit_span']))
            # ranges contains dataclasses; save the exact native ranges used by all source stages.
            stages.save('index',key,dict(ranges=[asdict(r) for r in ranges],units=[asdict(u) for u in units]))
        else:
            from dpt_health.direct import FileRange
            ranges=[FileRange(**r) for r in cached['ranges']];units=[QueryUnit(**u) for u in cached['units']]
            progress('completed-index-reused')
        key=identity(config,'market');cached=stages.load('market',key)
        if cached is None:
            markets,anchors,rules,labels,census=direct.market_facts(db,config,boundary,work,units,progress)
            stages.save('market',key,dict(markets=markets,anchors=anchors,rules=rules,labels=labels,census=census))
        else:
            from collections import defaultdict
            markets=cached['markets'];census=cached['census']
            anchors=defaultdict(list,cached['anchors']);rules=defaultdict(list,cached['rules']);labels=defaultdict(list,cached['labels'])
            progress('completed-market-reused')
        rows=direct.target_rows(config,markets,anchors,rules,labels,census)
        market_coverage=direct.census_coverage(config,census,markets,rows)
        (material/'market-coverage.json').unlink(missing_ok=True)
        save(material/'market-coverage.json',market_coverage)
        pairs=direct.source_sampling(db,config,boundary,work,units,ranges,rows,progress,stages)
        for name in ('samples.json','fit-pairs.json','query-units.json','material.json'):
            (material/name).unlink(missing_ok=True)
        save(material/'samples.json',rows);save(material/'fit-pairs.json',pairs)
        from dataclasses import asdict
        save(material/'query-units.json',[asdict(u) for u in units])
        save(material/'material.json',dict(format='direct-probability-samples-v2',
            config_sha256=hashlib.sha256(config_path.read_bytes()).hexdigest(),start=config['start'],end=config['end'],
            target_samples=len(rows),markets=len(markets),market_denominators={k:v for k,v in market_coverage.items() if k!='markets'},files=len(config['files']),
            physical_manifest_rows=sum(f['rows'] for f in config['files']),
            identity='causal-source-direct-query; not production strategy-call memory',
            implementation_commit=config['implementation_commit'],helper_sha256=config['probability_helper_sha256'],formal_adoption=False))
        progress('prepare-complete',target_samples=len(rows))
    finally:
        progress.close();boundary.close();db.close()


def coverage(rows):
    from collections import Counter
    counts=Counter()
    for r in rows:
        key=(r['day'],r['condition_id'],r['remaining_s'])
        counts[(*key,'targets')]+=1
        counts[(*key,'raw_targets')]+=bool(r.get('raw_target'))
        if r.get('public_target') is None:counts[(*key,'public_qualification_unknown')]+=1
        else:counts[(*key,'public_targets')]+=bool(r['public_target'])
        for field in ('identity_known','price','reference','integral','trade_price','depth_total','innovation','depth_pressure','trade_pressure'):
            if field=='identity_known' and r.get(field) is None:continue
            counts[(*key,field+'_available')]+= r.get(field) is not None and r.get(field) is not False
        counts[(*key,'label_observed')]+= bool(r['labels'])
        counts[(*key,'label_final_trusted')]+= bool(r['labels'] and r['labels'][-1]['label'] is not None)
    return [dict(day=d,condition_id=c,remaining_s=s,metric=m,count=n) for (d,c,s,m),n in sorted(counts.items())]


def innovation_reversals(rows):
    from collections import Counter
    counts=Counter();previous={}
    for r in rows:
        value=r.get('innovation');condition=r['condition_id'];prior=previous.get(condition)
        counts['targets']+=1
        if value is None:counts['missing']+=1
        else:
            counts['available']+=1
            counts['positive' if value>0 else 'negative' if value<0 else 'zero']+=1
            if prior is not None:
                counts['adjacent_available_pairs']+=1
                counts['opposite_sign_pairs']+=prior*value<0
        previous[condition]=value
    return dict(counts)


def calibration_plot(result,path):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    bins=[r for r in result['score']['strata'] if r['axis']=='calibration' and r['samples']]
    fig,ax=plt.subplots(figsize=(4.5,4.5))
    ax.plot([0,1],[0,1],color='gray',linestyle='--')
    ax.plot([r['mean_probability'] for r in bins],[r['observed_up'] for r in bins],marker='o')
    ax.set(xlim=(0,1),ylim=(0,1),xlabel='Mean predicted Up',ylabel='Observed Up',title='Ten fixed probability bins')
    fig.tight_layout();fig.savefig(path);plt.close(fig)


def study(config_path):
    config=json.loads(config_path.read_text());root=Path(config['run_root']);material=root/'material'
    # 仅核对材料/配置身份，不重做输入准入。
    descriptor=json.loads((material/'material.json').read_text())
    if descriptor['config_sha256']!=hashlib.sha256(config_path.read_bytes()).hexdigest():raise ValueError('material belongs to another configuration')
    rows=json.loads((material/'samples.json').read_text());pairs=json.loads((material/'fit-pairs.json').read_text())
    out=root/'study';out.mkdir(exist_ok=False);work=root/'statistics';db=connection(work,config['engine'])
    progress=Progress(db,config['progress_interval_seconds'],work)
    try:
        save(out/'coverage.json',coverage(rows))
        save(out/'innovation-reversals.json',innovation_reversals(rows))
        report=['# 直接概率与跨来源候选评价','',
            '本结果仅对应来源级因果统计采样。每市场八次采样相关，不当作独立交易样本。正式参数尚未采用。','',
            '| 折 | 方案 | 有效/目标 | Brier | 共同交集差值 | fallback |',
            '| --- | --- | --- | --- | --- | --- |']
        for fold,fit_days in enumerate((3,6,9,12,15)):
            cutoff=config['start']+fit_days*DAY
            fit_rows=[r for r in rows if config['start']<=r['at']<cutoff]
            progress('forward-fit',fold=fold,fit_samples=len(fit_rows),fit_end=cutoff)
            frozen,candidates=evaluation.fit(fit_rows,next(p for p in pairs if p['cutoff']==cutoff),cutoff)
            save(out/f'fold-{fold}.frozen.json',frozen)
            # 先完成冻结文件写入，再计算对应评价分数。
            validation_rows=[r for r in rows if cutoff<=r['at']<cutoff+3*DAY]
            progress('frozen-validation',fold=fold,validation_samples=len(validation_rows))
            results=evaluation.evaluate(validation_rows,candidates,frozen['sigma_fit'])
            for i,result in enumerate(results):
                save(out/f'fold-{fold}.candidate-{i}.predictions.json',result['score'].pop('predictions'))
                calibration_plot(result,out/f'fold-{fold}.candidate-{i}.calibration.svg')
                metric=result['score'];report.append(f"| {fold} | {json.dumps(result['candidate'])} | {metric['scored']}/{metric['targets']} | {metric['brier']} | {result.get('paired_brier_delta')} | {metric['fallback_samples']} |")
            save(out/f'fold-{fold}.validation.json',dict(window=[cutoff,cutoff+3*DAY],development=fold==4,results=results))
        save(out/'summary.json',dict(material=descriptor,formal_adoption=False,
            method='每折最多75次预声明顺序拟合评分；跨来源以共同交集配对比较',
            limitations=['全局来源历史和每市场八次统计调用不等于生产SignalMemory',
                'epsilon为Brier选择的误差尺度假设，不能称已观察连续结算残差',
                'USD/USDT创新未进行币种转换，终局标签仅为二元结果',
                '开发窗不是独立留出，概率比较不证明账户盈利']))
        report.extend(['','缺失、固定十桶校准及日/市场分块离散见对应 validation.json。空桶为 null。所有目标与来源资格见 coverage.json。',
            '原始目录、研究映射和逐字段采样分母见 material/market-coverage.json；生产身份资格未重算，引用未变输入的既有审计，不能以研究字段冒充合法身份。生产完整回放接受政策保持。',
            '预测误差仅以二元标签的 Brier 检验。不能由公开二元结果伪造连续结算价格残差。',
            '本期末日标签按十八日右边界删失；开发窗属于开发评价。收益、费用和账户风险仍须公共回测。'])
        (out/'report.md').write_text('\n'.join(report)+'\n')
        progress('study-complete')
    finally:progress.close();db.close()


def main():
    parser=argparse.ArgumentParser(description=__doc__);sub=parser.add_subparsers(dest='stage',required=True)
    conf=sub.add_parser('configure')
    for name in ('plan','input_root','run_root','boundary_helper','probability_helper','output'):conf.add_argument(name,type=Path)
    prep=sub.add_parser('prepare');prep.add_argument('config',type=Path)
    prep.add_argument('--reuse-material',type=Path,help='仅复用身份匹配的完整查询阶段，不恢复单元状态')
    sub.add_parser('study').add_argument('config',type=Path)
    args=parser.parse_args()
    if args.stage=='configure':configure(args)
    elif args.stage=='prepare':prepare(args.config,args.reuse_material)
    else:study(args.config)
