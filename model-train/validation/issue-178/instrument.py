"""插桩只进入离线源副本；嵌套/并行工作量不与墙钟阶段相加。"""
import re


def replace(text, old, new):
    assert old in text, old
    return text.replace(old, new)


def timed_function(text, name, key):
    pattern = r'(fn ' + name + r'\([^{}]*?\)(?:\s*->\s*[^{}]+)?\s*\{)'
    text, count = re.subn(pattern, r'\1\n    let _profile=crate::bench_profile::span("' + key + '");', text, count=1)
    assert count == 1, name
    return text


def apply(src):
    path = src / 'root.rs'; text = path.read_text()
    text = replace(text, 'let before = shared.action_before(driver)?;', 'let before = {let _profile=crate::bench_profile::span("account_read_ns");shared.action_before(driver)?};')
    text = replace(text, 'let slow = crate::account::encode(', 'let account_span=crate::bench_profile::span("account_features_ns");\n            let slow = crate::account::encode(')
    text = replace(text, '            if roles', '            drop(account_span);\n            if roles')
    text = timed_function(text, 'from_observation', 'action_context_ns')
    # 最外层总时间供独立核对，不从并行子任务用时相减。
    text = timed_function(text, 'consume', 'consume_ns')
    path.write_text(text)
    path=src/'market.rs'; text=path.read_text()
    text=replace(text,'signed_log_f32 as log','signed_log_f32 as original_log')
    text+='''
fn log(value:&R)->f32 {
    if value.is_zero() {crate::bench_profile::add("market_log_zero_calls",1);} else {
        crate::bench_profile::rational_key(||format!("{}/{}",value.numerator(),value.denominator()));
    }
    let _p=crate::bench_profile::span("market_value_log_work_ns");
    original_log(value)
}
'''
    for name,key in [('side','book_side_work_ns'),('t5','t5_work_ns'),('seconds','time_projection_work_ns'),('features','e9_projection_work_ns'),
                     ('envelope','envelope_work_ns'),('reference','reference_ns'),('history','history_ns'),
                     ('lifecycle','lifecycle_ns'),('bba','bba_ns'),('pm_trade','last_trade_ns'),
                     ('binance_trade','last_trade_ns'),('events','events_ns'),('rules_with_static','rules_ns')]:
        text=timed_function(text,name,key)
    text=replace(text,'    h.sides\n', '    let book_span=crate::bench_profile::span("book_wall_ns");\n    h.sides\n')
    text=replace(text,'        });\n    for i in 0..2 {\n        if let Some(a) = o\n            .assets', '        });\n    drop(book_span);\n    for i in 0..2 {\n        if let Some(a) = o\n            .assets')
    text=replace(text,'    let cached = previous\n', '    crate::bench_profile::add("input_levels",levels.len() as u128);\n    let cached = previous\n')
    text=replace(text,'    let cached = previous\n', '    let scan_span=crate::bench_profile::span("book_match_valid_work_ns");\n    let cached = previous\n')
    text=replace(text,'    let valid: Vec<_> = cached', '    crate::bench_profile::add("quantity_changed",quantity_changed.iter().filter(|v|**v).count() as u128);\n    let valid: Vec<_> = cached')
    text=replace(text,'    let total = if warm', '    drop(scan_span);\n    let total_span=crate::bench_profile::span("book_total_work_ns");\n    let total = if warm')
    total_log='projection.log' if 'cached.summary[0] = projection.log' in text else 'log'
    text=replace(text,f'        cached.summary[0] = {total_log}(&integer(valid.len()));', f'        crate::bench_profile::add("full_sum_levels",valid.len() as u128);\n        cached.summary[0] = {total_log}(&integer(valid.len()));')
    text=replace(text,'    out[0] = 1.;\n    out[1..3]', '    drop(total_span);\n    out[0] = 1.;\n    out[1..3]')
    text=replace(text,'    let mut by_path:', '    let anchors_span=crate::bench_profile::span("book_anchors_times_work_ns");\n    let mut by_path:')
    text=replace(text,'    if changed {\n        let mut values', '    if changed {\n        crate::bench_profile::add("rebuilt_sides",1);\n        crate::bench_profile::add("copied_rows",valid.len() as u128);\n        let mut values')
    text=replace(text,'    if changed {\n        crate::bench_profile::add("rebuilt_sides",1);', '    drop(anchors_span);\n    let output_span=crate::bench_profile::span("book_output_work_ns");\n    if changed {\n        crate::bench_profile::add("rebuilt_sides",1);')
    text=replace(text,'    for level in &mut cached.levels {', '    drop(output_span);\n    let _publish_span=crate::bench_profile::span("book_cache_publish_work_ns");\n    for level in &mut cached.levels {')
    text=replace(text,'    let mut times = HashMap::new();', '    crate::bench_profile::add("side_calls",1);\n    let mut times = HashMap::new();')
    text=replace(text,'    let mut counts = [0usize; 23];', '    crate::bench_profile::add("event_members",o.group_events.len() as u128);\n    crate::bench_profile::add("event_member_visits",4*o.group_events.len() as u128);\n    let mut counts = [0usize; 23];')
    cached='projection.project(i128::from(a) - i128::from(b))' in text
    value='projection.project(i128::from(a) - i128::from(b))' if cached else 'log(&ratio(i128::from(a) - i128::from(b), 1000))'
    text=replace(text,'    '+value,'    crate::bench_profile::time_key(i128::from(a) - i128::from(b));\n    '+value)
    value='projection.project(lo)' if cached else 'log(&ratio(lo, 1000))'
    text=replace(text,'out[2 * i] = '+value+';','crate::bench_profile::time_key(lo);\n                out[2 * i] = '+value+';')
    value='projection.project(hi)' if cached else 'log(&ratio(hi, 1000))'
    text=replace(text,'                    '+value,'                    crate::bench_profile::time_key(hi);\n                    '+value)
    path.write_text(text)
    if cached:
        path=src/'exact.rs';text=path.read_text()
        if 'type SharedProjection' in text:
            text=replace(text,'        *value.get_or_init(|| signed_log_f32(&ratio(milliseconds, 1000)))','''        crate::bench_profile::add("time_cache_calls",1);
        if value.get().is_some(){crate::bench_profile::add("time_cache_ready_hits",1);}
        *value.get_or_init(|| {crate::bench_profile::add("time_cache_misses",1);signed_log_f32(&ratio(milliseconds,1000))})''')
            text=replace(text,'        let key = (exact.numerator().clone(), exact.denominator().clone());','''        crate::bench_profile::rational_key(||format!("{}/{}",exact.numerator(),exact.denominator()));
        let key = (exact.numerator().clone(), exact.denominator().clone());''')
            text=replace(text,'        *value.get_or_init(|| signed_log_f32(exact))','''        crate::bench_profile::add("value_cache_calls",1);
        if value.get().is_some(){crate::bench_profile::add("value_cache_ready_hits",1);}
        *value.get_or_init(|| {let _p=crate::bench_profile::span("value_cache_compute_ns");crate::bench_profile::add("value_cache_misses",1);signed_log_f32(exact)})''')
        else:
            text=replace(text,'            return value;','            crate::bench_profile::add("time_cache_hits",1);\n            return value;')
            text=replace(text,'        let value = signed_log_f32(&ratio(milliseconds, 1000));','        crate::bench_profile::add("time_cache_misses",1);\n        let value = signed_log_f32(&ratio(milliseconds, 1000));')
        path.write_text(text)
    path=src/'cancel.rs';path.write_text(timed_function(path.read_text(),'encode','cancel_features_ns'))
    path=src/'account.rs';text=path.read_text()
    text=replace(text,'            let commitment = sum(read.commitments().filter_map(|c| {', '            let commitments_span=crate::bench_profile::span("account_commitments_ns");\n            let commitment = sum(read.commitments().filter_map(|c| {')
    slot='            slot(\n'+('                projection,\n' if 'slot(\n                projection,' in text else '')+'                &mut out,\n                98 + group'
    text=replace(text,slot,'            drop(commitments_span);\n'+slot)
    path.write_text(text)
    path=src/'values.rs';text=path.read_text()
    text=timed_function(text,'slot','value_slots_ns')
    value_log='projection.log(&v)' if '[projection.log(&v),' in text else 'signed_log_f32(&v)'
    # 新实现已在纯投影入口登记非零精确键；V3 只登记零值，避免重复计数。
    nonzero_probe='' if value_log.startswith('projection.') else 'else {crate::bench_profile::rational_key(||format!("{}/{}",v.numerator(),v.denominator()));}'
    text=replace(text,'Known(v) => out[at..at + 3].copy_from_slice(&['+value_log+', 1., 1.]),', '''Known(v) => {
            if v.is_zero() {crate::bench_profile::add("value_log_zero_calls",1);} NONZERO_PROBE
            let value={let _p=crate::bench_profile::span("value_log_work_ns");VALUE_LOG};
            out[at..at+3].copy_from_slice(&[value,1.,1.]);
        },'''.replace('VALUE_LOG',value_log).replace('NONZERO_PROBE',nonzero_probe))
    path.write_text(text)


def shared(src):
    path=src/'group.rs';text=path.read_text()
    text=replace(text,'        self.apply_closed()?;','        {let _p=crate::bench_profile::span("feedback_apply_ns");self.apply_closed()?;}')
    text=replace(text,'let registry = self.registry.action_snapshot();','let registry = {let _p=crate::bench_profile::span("registry_snapshot_ns");self.registry.action_snapshot()};')
    text=replace(text,'let Some(ledger) = funds.projection() else','let Some(ledger) = ({let _p=crate::bench_profile::span("ledger_projection_ns");funds.projection()}) else')
    # 在原有迭代中计数，不为测量另建账户快照或再扫描订单。
    text=replace(text,'.orders()\n            .filter_map(|o| o.origin)', '.orders()\n            .inspect(|_| crate::bench_profile::add("registry_order_entries",1))\n            .filter_map(|o| o.origin)')
    text=replace(text,'            .map(|r| RequestFunds {','            .inspect(|_| crate::bench_profile::add("request_funds_entries",1))\n            .map(|r| RequestFunds {')
    text=replace(text,'            .map(|r| OrderFunds {','            .inspect(|_| crate::bench_profile::add("order_funds_entries",1))\n            .map(|r| OrderFunds {')
    text=replace(text,'        Ok(Some(ActionBeforeRead {','''        let economy= {let _p=crate::bench_profile::span("ledger_economic_read_ns");self.ledger.economic_read().expect("同组 Ledger 已开户")};
        let economics={let _p=crate::bench_profile::span("account_economics_projection_ns");AccountEconomicsReadV1::from_ledger(&economy)};
        Ok(Some(ActionBeforeRead {''')
    text=replace(text,'''economics: AccountEconomicsReadV1::from_ledger(
                &self.ledger.economic_read().expect("同组 Ledger 已开户"),
            ),''','economics,')
    path.write_text(text)
