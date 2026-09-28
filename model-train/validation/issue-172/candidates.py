"""只生成离线单项差异；是否进入生产以配对证据决定。"""
import difflib
from pathlib import Path
import subprocess
w=Path(__file__).resolve().parents[3]
base=subprocess.check_output(['git','show','e57503821391a9943ce694eabab00ec0fdff1141:model-train/native-inputs/src/market.rs'],cwd=w,text=True)
root=subprocess.check_output(['git','show','e57503821391a9943ce694eabab00ec0fdff1141:model-train/native-inputs/src/root.rs'],cwd=w,text=True)
out=Path(__file__).parent

def patch(name,old,new):
    # 与 diff --suppress-blank-empty 相同，空上下文行不带尾随空格。
    lines=difflib.unified_diff(old.splitlines(True),new.splitlines(True),fromfile=name,tofile=name)
    (out/(name+'.patch')).write_text(''.join('\n' if line == ' \n' else line for line in lines))

# 一遍解析并生成路径，正数量筛选和顺序保持。
s=base
start=s.index('                    let levels = visible(fact);')
end=s.index('                        fact.direct_anchors',start)
s=s[:start]+'''                    let mut paths=Vec::new();
                    let levels=visible(fact).map(|v| {
                        let mut parsed=Vec::with_capacity(v.len());
                        paths.reserve(v.len());
                        for p in v {
                            let price=decimal(&p.price_pusd_per_share);
                            let quantity=decimal(&p.quantity_shares);
                            if quantity>R::zero() {paths.push(format!("{}{}]",prefix,p.price_pusd_per_share.0));}
                            parsed.push((price,quantity));
                        }
                        parsed
                    });
                    side(features,now,levels,
'''+s[end:]
patch('cache',base,s)

# 根持有快照派生的静态14—29槽；BBA三字段共用相同锚点T5。
s=base
start=s.index('    for (slot, value) in [',s.index('pub(crate) fn rules('))
end=s.index('\n#[derive(Clone, Default)]',start)
tail=s[start:end]
s=s[:start]+'''    out[14..].copy_from_slice(&static_rules(r));
    out
}
pub(crate) fn static_rules(r:&PublicTradingRuleContextV1)->[f32;16] {
    let mut out=[0.;30];
'''+tail.replace('    out\n}', '    out[14..].try_into().unwrap()\n}')+s[end:]
# 复用原rules动态前14槽的唯一实现。
s=s.replace('    let mut out = [0.; 30];\n    for i in 0..2 {','    rules_with_static(o,now,&static_rules(r))\n}\npub(crate) fn rules_with_static(o:&MarketBusinessObservationV1,now:i64,tail:&[f32;16])->[f32;30] {\n    let mut out = [0.; 30];\n    for i in 0..2 {',1).replace('out[14..].copy_from_slice(&static_rules(r));','out[14..].copy_from_slice(tail);',1)
start=s.index('    for i in 0..3 {',s.index('fn bba('));end=s.index('    fields[0]',start)
s=s[:start]+'''    let time=t5(now,one(&a));
    for (i,field) in fields.iter().enumerate() {
        let start=7+7*i;
        if let Some(value)=field.map(decimal) {out[start]=price(&value,i<2);out[start+1]=1.;}
        out[start+2..start+7].copy_from_slice(&time);
    }
'''+s[end:]
start=s.index('pub(crate) fn rules(')
end=s.index('pub(crate) fn rules_with_static(',start)
s=s[:start]+s[end:]
patch('projection',base,s)
r=root.replace('    reset: bool,','    reset: bool,\n    rule_tail: Option<[f32;16]>,').replace('            reset: false,','            reset: false,\n            rule_tail: None,')
r=r.replace('let rule = crate::market::rules(frame.observation, &frame.rule_context, now);','let tail=self.rule_tail.get_or_insert_with(||crate::market::static_rules(&frame.rule_context));\n                let rule=crate::market::rules_with_static(frame.observation,now,tail);')
patch('projection.root',root,r)

# 按侧去重当前未命中键；先构造集合再并行求值，仍沿原当前帧键重建有界缓存。
s=base
start=s.index('        let start = book.len();')
s=s[:start]+'''        let missing: std::collections::BTreeSet<_>=levels.iter().zip(&distances).flat_map(|((p,q),d)| (!pm).then_some(p).into_iter().chain([q,d])).filter(|v|!previous.logs.contains_key(*v)).cloned().collect();
        let missing: Vec<_>=missing.into_iter().collect();
        let computed:BTreeMap<_,_>=missing.into_par_iter().map(|v|{let result=log(&v);(v,result)}).collect();
'''+s[start:]
s=s.replace('.unwrap_or_else(|| log(value))','.unwrap_or_else(|| computed[value])')
patch('combined',base,s)

# 保留临时数组容量，return前清空逻辑内容，不保留上帧特征或第二盘口。
s=base.replace('    logs: BTreeMap<R, f32>,','    logs: BTreeMap<R, f32>,\n    scratch_distances: Vec<R>,\n    scratch_times: Vec<[f32;5]>,')
s=s.replace('let mut row_times = Vec::with_capacity(levels.len());','let mut row_times=std::mem::take(&mut previous.scratch_times);\n        row_times.reserve(levels.len());')
s=s.replace('let distances: Vec<_> = levels','let mut distances=std::mem::take(&mut previous.scratch_distances);\n        distances.extend(levels')
s=s.replace('            .collect();\n        let start = book.len();','            );\n        let start = book.len();')
s=s.replace('.zip(distances)','.zip(distances.drain(..))')
s=s.replace('            .collect();\n    }\n    *previous = next;','            .collect();\n        row_times.clear();\n        next.scratch_distances=distances;next.scratch_times=row_times;\n    }\n    *previous = next;')
patch('buffer',base,s)
# 旧20组筛选仅支持保留time_rules；最终候选就是该单项。
(out/'history.patch').write_text((out/'projection.patch').read_text())
(out/'history.root.patch').write_text((out/'projection.root.patch').read_text())
