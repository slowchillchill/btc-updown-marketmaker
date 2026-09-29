"""仅离线测量副本加计时点；生产函数计算、返回值和调用次序保持。"""
import re


def instrument(source, candidate):
    source = source.replace('    pub hidden: usize,', '''    pub hidden: usize,
    pub profile: std::collections::BTreeMap<&'static str,u128>,''')
    if candidate:
        source = source.replace('            hidden,', '            hidden,\n            profile: Default::default(),')
    # 输入构造与 Session::run 分开；包括 run 的必要输出保留与宿主边界，非算子 profiler。
    pattern = r'let (out|output) = (self\.\w+|session)\.run\(ort::inputs!\[(.*?)\]\)\?;'
    def timed(match):
        variable, session, inputs = match.groups()
        return f'''let input_start = std::time::Instant::now();
        let measured_inputs = ort::inputs![{inputs}];
        *self.profile.entry("graph_input_ns").or_default() += input_start.elapsed().as_nanos();
        let graph_start = std::time::Instant::now();
        let {variable} = {session}.run(measured_inputs)?;
        *self.profile.entry("graph_run_ns").or_default() += graph_start.elapsed().as_nanos();
        *self.profile.entry("graph_calls").or_default() += 1;'''
    source, count = re.subn(pattern, timed, source, flags=re.S)
    assert count == (5 if candidate else 1), count
    if candidate:
        source = source.replace('            let support = row.0.support()', '            let measured_support = std::time::Instant::now();\n            let support = row.0.support()')
        source = source.replace('            // 评分前处理单项', '            *self.profile.entry("support_inside_scores_ns").or_default() += measured_support.elapsed().as_nanos();\n            // 评分前处理单项')
        source = source.replace('            let indices = row.0.price_indices()', '            let measured_indices = std::time::Instant::now();\n            let indices = row.0.price_indices()')
        source = source.replace('            groups\n                .entry', '            *self.profile.entry("price_indices_ns").or_default() += measured_indices.elapsed().as_nanos();\n            groups\n                .entry')
        source = source.replace('                let fees = active', '                let fee_start = std::time::Instant::now();\n                let fees = active')
        source = source.replace('                let p = indices.len();', '                *self.profile.entry("fee_features_ns").or_default() += fee_start.elapsed().as_nanos();\n                let p = indices.len();')
    else:
        source = source.replace('        let fees = actor.price_fee_features()', '        let fee_start = std::time::Instant::now();\n        let fees = actor.price_fee_features()')
        source = source.replace('        let mut scores = Vec::new();', '        *self.profile.entry("fee_features_ns").or_default() += fee_start.elapsed().as_nanos();\n        let mut scores = Vec::new();', 1)
    return source
