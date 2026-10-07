"""前向拟合、明确采样分母及跨来源共同交集比较。"""
from __future__ import annotations
from collections import Counter, defaultdict
from dataclasses import replace
from math import sqrt

def age_bucket(age):
    return 'missing' if age is None else ('<1000' if age<1000 else '1000-5000' if age<5000 else '>=5000')
from .model import FEATURES, WEIGHTS, baseline_candidates, predict

DAY = 86_400_000


def score(rows, candidate, sigma_fit, label_before=None, intersection=None, details=False):
    missing = Counter(); n = 0; loss = 0.0; fallback = 0; zero = 0
    bins = defaultdict(lambda: [0, 0.0, 0.0, 0.0])
    blocks = defaultdict(lambda: [0, 0.0])
    predictions = []
    for row in rows:
        if intersection and row.get(intersection) is None:
            missing['missing_' + intersection] += 1
            continue
        estimate, reason = predict(row, candidate, sigma_fit)
        if reason:
            missing[reason] += 1
            continue
        labels=[v for v in row.get('labels',[]) if label_before is None or v['at']<label_before]
        terminal=labels[-1] if labels else None
        if terminal is None or terminal['label'] is None:
            missing['missing_label' if not row.get('labels') else 'label_not_visible_or_trusted_at_cutoff'] += 1
            continue
        p, y = estimate['p'], terminal['label']
        error = (p - y)**2; n += 1; loss += error
        fallback += estimate['fallback']; zero += estimate['zero_variance']
        if details:
            z = estimate['z']
            distance = 'degenerate' if z is None else ('<1' if abs(z)<1 else '1-2' if abs(z)<2 else '>=2')
            age = row.get('price_age_ms')
            age_bin = age_bucket(age)
            source_missing = ','.join(f for f in FEATURES if row.get(f) is None) or 'complete'
            for axis, key in [('calibration', str(min(9,int(p*10)))), ('remaining_s',str(row['remaining_s'])),
                              ('distance', distance), ('reference_kind',row['reference_kind']),
                              ('price_age_ms',age_bin), ('source_missing',source_missing),
                              ('day',str(row['day'])), ('market',row['condition_id'])]+[
                              (source+'_'+clock+'_age_ms',age_bucket(row.get(source+'_'+clock+'_age_ms')))
                              for source in ('chainlink_spot','twap','binance_trade','binance_depth')
                              for clock in ('received','event')]:
                b = bins[(axis,key)]; b[0] += 1; b[1] += p; b[2] += y; b[3] += error
            for axis,key in [('day',str(row['day'])),('market',row['condition_id'])]:
                b=blocks[(axis,key)];b[0]+=1;b[1]+=error
            predictions.append(dict(condition_id=row['condition_id'],at=row['at'],p=p,label=y,
                                    label_visible=terminal['at'],**{k:estimate[k] for k in ('mean','uncertainty','z','fallback')}))
    result = dict(targets=len(rows),scored=n,missing=dict(missing),brier=loss/n if n else None,
                  fallback_samples=fallback,zero_variance_samples=zero)
    if details:
        for bucket in range(10): bins.setdefault(('calibration',str(bucket)),[0,0.0,0.0,0.0])
        result['strata'] = [dict(axis=a,key=k,samples=v[0],mean_probability=v[1]/v[0] if v[0] else None,
                                observed_up=v[2]/v[0] if v[0] else None,brier=v[3]/v[0] if v[0] else None) for (a,k),v in sorted(bins.items())]
        result['block_dispersion'] = {}
        for axis in ('day','market'):
            values=[v[1]/v[0] for (a,k),v in blocks.items() if a==axis]
            mean=sum(values)/len(values) if values else None
            result['block_dispersion'][axis]=dict(blocks=len(values),equal_block_mean=mean,
                standard_deviation=sqrt(sum((x-mean)**2 for x in values)/len(values)) if values else None,
                minimum=min(values) if values else None,maximum=max(values) if values else None)
        result['predictions']=predictions
    return result


def best(scored):
    available=[(i,c,s) for i,(c,s) in enumerate(scored) if s['brier'] is not None]
    return min(available,key=lambda v:(v[2]['brier'],v[0]))[1] if available else None


def fit(rows, pairs, cutoff):
    # 价对充分统计独立遵守拟合可见截止。
    square=pairs['squared_change']; seconds=pairs['seconds']
    sigma=sqrt(square/seconds) if seconds else None
    baseline=[(c,score(rows,c,sigma,label_before=cutoff)) for c in baseline_candidates()]
    selected=best(baseline)
    searches=[]; frozen=[]
    if selected:
        frozen.append(selected)
        for feature in FEATURES:
            trial=[(replace(selected,feature=feature,weight=w),score(rows,replace(selected,feature=feature,weight=w),
                     sigma,label_before=cutoff,intersection=feature)) for w in WEIGHTS]
            chosen=best(trial)
            searches.append(dict(feature=feature,candidates=[dict(candidate=c.identity(),score=s) for c,s in trial]))
            if chosen:
                frozen.append(chosen)
    return dict(fit_cutoff=cutoff,sigma_fit=sigma,fit_pairs=pairs['pairs'],pair_seconds=seconds,squared_change=square,
                search_order='先60个全覆盖基线，再三类各5个共同交集权重，独立顺序拟合',
                baseline_scores=[dict(candidate=c.identity(),score=s) for c,s in baseline],external_searches=searches,
                candidates=[c.identity() for c in frozen]),frozen


def evaluate(rows, candidates, sigma):
    results=[]
    for c in candidates:
        intersection=c.feature if c.feature!='none' else None
        result=dict(candidate=c.identity(),score=score(rows,c,sigma,intersection=intersection,details=True))
        if intersection:
            base=replace(c,feature='none',weight=0.0)
            paired=score(rows,base,sigma,intersection=intersection)
            result['paired_baseline']=paired
            p,b=result['score']['brier'],paired['brier']
            result['paired_brier_delta']=p-b if p is not None and b is not None else None
        results.append(result)
    return results
