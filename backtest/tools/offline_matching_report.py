#!/usr/bin/env python3
"""Read formal day/research evidence and read_noe_prefix JSONL; never replay balances.

Decimals stay exact. Entity identities are opaque. NOE commit/record positions are
preserved; segment-local record_sequence is not used for selection or deduplication.
"""
import argparse
from collections import Counter, defaultdict
from decimal import Decimal as D
import hashlib
import json
from pathlib import Path
import resource
import time
import subprocess
import sys


def dump(value, path):
    with Path(path).open('w') as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2, default=str)


def progress(args, phase, **values):
    print(json.dumps({'report_phase': phase, 'candidate_index': args.candidate_index, **values}, default=str), file=sys.stderr, flush=True)


def decoded_rows(stream, identities, identity, args=None, phase=None):
    digest = hashlib.sha256()
    started = last = time.monotonic()
    count = size = 0
    for line in stream:
        digest.update(line)
        count += 1; size += len(line)
        if args and args.progress_seconds > 0 and time.monotonic()-last >= args.progress_seconds:
            progress(args, phase, rows=count, bytes=size, elapsed_seconds=time.monotonic()-started)
            last = time.monotonic()
        yield json.loads(line)
    identities[identity] = digest.hexdigest()
    if args:
        progress(args, phase+'_eof', rows=count, bytes=size, elapsed_seconds=time.monotonic()-started)


def rows(path, identities, args=None, phase=None):
    with Path(path).open('rb') as stream:
        yield from decoded_rows(stream, identities, str(path), args, phase)


def noe_rows(args, identities, published_prefix):
    if args.noe_reader is None:
        yield from rows(args.noe_jsonl, identities, args, 'noe')
        return
    command = [str(args.noe_reader), str(args.noe_context), str(published_prefix)]
    identity = 'formal-noe-reader:'+str(args.noe_context)+':'+str(published_prefix)
    progress(args, 'noe_reader_start', command=command)
    process = subprocess.Popen(command, stdout=subprocess.PIPE)
    complete = False
    try:
        yield from decoded_rows(process.stdout, identities, identity, args, 'noe')
        complete = True
    finally:
        process.stdout.close()
        if not complete and process.poll() is None:
            process.terminate()
        status = process.wait()
        args.noe_reader_result = {'command': command, 'exit_status': status}
        progress(args, 'noe_reader_exit', exit_status=status)
    if status != 0:
        raise subprocess.CalledProcessError(status, command)



def name(value):
    return next(iter(value)) if isinstance(value, dict) else value


def distribution(values):
    values = sorted(values)
    if not values:
        return {'count': 0, 'min': None, 'p50': None, 'p95': None, 'max': None}
    return {'count': len(values), 'min': values[0], 'p50': values[(len(values)-1)//2],
            'p95': values[((len(values)-1)*95)//100], 'max': values[-1]}


def event_key(evidence, effective):
    order = evidence['source_order']
    sequence = order['sequence']
    sequence = sequence.get('PrivateFeedback', {}).get('private_feedback_sequence', -1) if isinstance(sequence, dict) else -1
    return (effective, sequence, order['ordinal'])


def summarize_order(order, fills, lifecycle, observed_end):
    """Complete order statistics from a closed evidence prefix and its cutoff."""
    fills = sorted(fills, key=lambda t: t['matched_key'])
    quantity = sum((t['shares'] for t in fills), D(0))
    successful = sum((t['shares'] for t in fills if t['dispositions'][-1] == 'FinalSuccess'), D(0))
    full = order['requested_shares'] is not None and quantity >= order['requested_shares']
    partial = order['requested_shares'] is not None and D(0) < quantity < order['requested_shares']
    first = fills[0]['matched_at'] if fills else None
    terminal = next((x for x in sorted(lifecycle, key=lambda x: x['key']) if x['event'] != 'Accepted'), None)
    full_at = fills[-1]['matched_at'] if full else None
    end = min(x for x in [terminal['at'] if terminal else None, full_at, observed_end] if x is not None)
    order.update(matched_shares=quantity, successful_shares=successful, filled=bool(fills), partial=partial,
        full=full if order['requested_shares'] is not None else 'NotApplicableQuoteBudget', first_matched_at=first,
        full_matched_at=full_at, terminal=terminal, observed_until=end,
        first_wait_ms=first-order['accepted_at'] if first is not None else None,
        full_wait_ms=full_at-order['accepted_at'] if full_at is not None else None,
        unfilled_observed_ms=end-order['accepted_at'] if not fills else None,
        right_censored=terminal is None and not full)
    return order


def markout_at(trade, at, state, state_at, coverage_end):
    """Evaluate the original public path, which may extend beyond candidate exit."""
    result = {'at': at, 'status': 'UnknownMissingState', 'signed_mid_minus_fill': None}
    if coverage_end is None or at > coverage_end:
        result['status'] = 'UnknownCoverageEnd'
        return result
    if state:
        best = state['best_bid_ask']
        value = best['value']
        result.update(quality=best['quality'], public_state_at=state_at)
        if best['quality'] == 'fresh' and value and value['best_bid_pusd_per_share'] is not None and value['best_ask_pusd_per_share'] is not None:
            bid = D(value['best_bid_pusd_per_share']); ask = D(value['best_ask_pusd_per_share'])
            if bid <= ask:
                result.update(status='Known', signed_mid_minus_fill=(bid + ask)/2 - trade['price'])
            else:
                result['status'] = 'UnknownCrossed'
        else:
            result['status'] = 'UnknownQualityOrEmpty'
    return result


def calculation_intents(row):
    """The formal writer emits Option<Vec>: only Exit has null intents."""
    value = row['intents']
    if value is None:
        if row.get('outcome') != 'exit':
            raise ValueError('null calculation intents require the formal exit outcome')
        return []
    if not isinstance(value, list):
        raise ValueError('calculation intents must be a list or the formal Exit null')
    return value


def candidate_report(args):
    start = time.monotonic()
    progress(args, 'candidate_start')
    identities = {}
    day_bytes = Path(args.day_output).read_bytes()
    identities[str(args.day_output)] = hashlib.sha256(day_bytes).hexdigest()
    day = json.loads(day_bytes)['days'][args.day_index]
    candidate = day['candidates'][args.candidate_index]
    report = candidate['report']
    config_bytes = args.day_config.read_bytes()
    identities[str(args.day_config)] = hashlib.sha256(config_bytes).hexdigest()
    run_config = json.loads(config_bytes)['days'][args.day_index]['candidates'][args.candidate_index]['run']
    assert run_config['session']['run_id'] == candidate['run_id']
    assert run_config['timing']['public_delivery_delay_ms'] == {'Fixed': 0}, 'markout requires original zero-delay delivery'
    observed_end = args.range_end if report['end_reason'] == 'RangeEnd' else report['last_observed_at_utc_ms']
    requests = {}
    needed_groups = defaultdict(set)
    for row in rows(args.candidate_evidence, identities, args, 'candidate_evidence'):
        if row.get('record_type') == 'calculation':
            for item in calculation_intents(row):
                request = item['context']['global_request_id']
                assert request not in requests, 'reused calculation request identity'
                request_row = {'calculation_index': row['calculation_index'], 'decision_at': row['started_at_utc_ms'],
                               'public_state': row['public_state'], 'intent': item['intent'], 'sent_at': None}
                requests[request] = request_row
                for condition, ref in row['public_state'].items():
                    needed_groups[condition].add(ref['pm_group'])
        if row.get('record_type') == 'dispatched':
            for receipt in row['receipts']:
                request = receipt['context']['global_request_id']
                assert request in requests, 'dispatch without original calculation'
                requests[request]['sent_at'] = receipt['sent_at_utc_ms']

    orders = {}
    trades = {}
    lifecycle = defaultdict(list)
    cancels = defaultdict(list)
    cancel_target_outcomes = Counter()
    evidence_seen = set()
    prefix_begin = prefix_complete = None
    record_count = 0
    for row in noe_rows(args, identities, report['durable_noe_prefix']):
        if row['kind'] == 'begin':
            assert prefix_begin is None
            prefix_begin = row
        elif row['kind'] == 'complete':
            prefix_complete = row
        elif row['kind'] == 'record':
            record_count += 1
            record = row['record'].get('Evidence')
            if record is None:
                continue
            logical = record['logical_identity']
            assert logical not in evidence_seen, 'duplicate NOE logical evidence'
            evidence_seen.add(logical)
            ev = record['evidence']
            direct = record['source_reference']['record'].get('DirectResponse', {})
            control = ev.get('Control', {}).get('result', {})
            accepted = control.get('Submission', {}).get('outcome')
            if isinstance(accepted, dict) and 'Accepted' in accepted:
                accepted = accepted['Accepted']
                association = direct['association']['SubmissionItem']
                request = association['global_request_id']
                ordinal = association['batch_item_ordinal']
                req = requests[request]
                intent_kind = name(req['intent'])
                intent = req['intent'][intent_kind]
                legs = intent['orders'] if intent_kind == 'SubmitPairedBuy' else [intent['order']]
                leg = legs[ordinal]
                terms = next(iter(leg['order'].values()))
                order = accepted['order']
                identity = order['order_identity']
                assert identity not in orders
                assert order['condition_id'] == intent['condition_id'] and order['token_id'] == leg['token_id']
                orders[identity] = {**order, 'request_id': request, 'batch_item_ordinal': ordinal,
                    'order_kind': name(leg['order']), 'requested_shares': D(terms['size_shares']) if 'size_shares' in terms else None,
                    'quote_price': D(terms.get('limit_price_pusd_per_share', terms.get('worst_price_pusd_per_share'))),
                    'quote_budget': terms.get('spend_pusd'), 'accepted_at': accepted['effective_processing_at_utc_ms'],
                    'decision_at': req['decision_at'], 'sent_at': req['sent_at'], 'measurement': accepted['measurement']}
            if 'OrderLifecycle' in ev:
                value = ev['OrderLifecycle']['result']
                source = record['source_reference']['record']['OrderLifecycle']
                lifecycle[value['order_identity']].append({'event': name(value['event']), 'at': source['platform_event_at_utc_ms'],
                    'key': event_key(record, source['platform_event_at_utc_ms'])})
            if 'Cancellation' in control:
                value = control['Cancellation']
                cancel_target_outcomes[name(value['outcome'])] += 1
                association = direct['association']['CancellationTarget']
                request = association['global_request_id']
                cancels[value['order_identity']].append({'request_id': request, 'outcome': value['outcome'],
                    'sent_at': requests[request]['sent_at'], 'effective_at': direct['platform_responded_at_utc_ms'],
                    'key': event_key(record, direct['platform_responded_at_utc_ms'])})
            for version in ['TradeLifecycleV3', 'TradeLifecycleV2']:
                if version not in ev:
                    continue
                value = ev[version]['result']
                if version == 'TradeLifecycleV3':
                    value = value['result']
                identity = value['trade_identity']
                trade = trades.setdefault(identity, {'trade_identity': identity, 'order_identity': value['order_identity'],
                    'condition_id': value['condition_id'], 'token_id': value['token_id'], 'matched_at': value['matched_at_utc_ms'],
                    'price': D(value['source_price_pusd_per_share']), 'shares': D(value['base_quantity_shares']),
                    'principal': D(value['quote_amount_pusd']), 'fee': D(value['immediate_taker_fee_pusd']),
                    'role': value['liquidity_role'], 'states': [], 'dispositions': [], 'matched_key': None})
                assert trade['shares'] == D(value['base_quantity_shares']) and trade['order_identity'] == value['order_identity']
                trade['states'].append(value['state'])
                trade['dispositions'].append(value['economic_disposition'])
                if value['state'] == 'Matched':
                    assert trade['matched_key'] is None, 'duplicate matched trade'
                    trade['matched_key'] = event_key(record, value['matched_at_utc_ms'])
    assert prefix_begin and prefix_complete, 'formal NOE traversal incomplete'
    assert prefix_begin['published_prefix'] == report['durable_noe_prefix']
    assert prefix_complete['stats']['records'] == record_count
    assert all(t['order_identity'] in orders and t['matched_key'] is not None for t in trades.values()), 'unlinked or incomplete trade'

    del evidence_seen
    load_seconds = time.monotonic()-start
    progress(args, 'candidate_loaded', orders=len(orders), trades=len(trades), requests=len(requests), elapsed_seconds=load_seconds)
    # Keep only requested decision groups and three fixed markout times per trade.
    queries = defaultdict(list)
    for identity, trade in trades.items():
        for horizon in (1000, 5000, 30000):
            queries[trade['condition_id']].append((trade['matched_at'] + horizon, identity, horizon))
    for value in queries.values():
        value.sort()
    query_indices = Counter()
    decision_states = {}
    last_states = {}
    last_time = {}
    coverage_end = None
    markouts = defaultdict(dict)

    def answer(condition, before):
        values = queries[condition]
        index = query_indices[condition]
        while index < len(values) and values[index][0] < before:
            at, identity, horizon = values[index]
            trade = trades[identity]
            state = last_states.get(condition, {}).get(trade['token_id'])
            markouts[identity][str(horizon)] = markout_at(trade, at, state, last_time.get(condition), before - 1)
            index += 1
        query_indices[condition] = index

    # One suspension point per public record lets a batch decode each row once.
    # Each candidate retains its original as-of logic and independent queries.
    public_request = {'path': args.public_evidence, 'identities': identities}
    public_started = time.monotonic()
    while True:
        row = yield public_request
        public_request = None
        if row is None:
            break
        if 'visible_at_utc_ms' in row:
            coverage_end = max(coverage_end or row['visible_at_utc_ms'], row['visible_at_utc_ms'])
        if row.get('record_type') != 'pm_state':
            continue
        condition = row['market']['condition_id']
        at = row['visible_at_utc_ms']
        answer(condition, at)  # same-ms states are all installed before a same-ms query
        state = {item['asset']['token_id']: item for item in row['state']}
        group = row['group']['group_position']
        if group in needed_groups[condition]:
            decision_states[(condition, group)] = state
        last_states[condition] = state
        last_time[condition] = at
    for condition in queries:
        answer(condition, (coverage_end + 1) if coverage_end is not None else -1)
        for at, identity, horizon in queries[condition][query_indices[condition]:]:
            markouts[identity][str(horizon)] = markout_at(trades[identity], at, None, None, coverage_end)

    public_seconds = time.monotonic()-public_started
    progress(args, 'finalize_start', elapsed_seconds=time.monotonic()-start)
    finalize_started = time.monotonic()
    by_order = defaultdict(list)
    for trade in trades.values():
        trade['markout'] = markouts[trade['trade_identity']]
        by_order[trade['order_identity']].append(trade)
    counts = Counter()
    wait_first, wait_full, right_censored = [], [], []
    cancel_latency = []
    bins = defaultdict(lambda: {'accepted_orders': 0, 'filled_orders': 0, 'matched_shares': D(0)})
    for identity, order in orders.items():
        fills = sorted(by_order[identity], key=lambda t:t['matched_key'])
        summarize_order(order, fills, lifecycle[identity], observed_end)
        quantity, partial = order['matched_shares'], order['partial']
        full = order['requested_shares'] is not None and order['full']
        first, full_at, terminal = order['first_matched_at'], order['full_matched_at'], order['terminal']
        counts['accepted_orders'] += 1; counts['filled_orders'] += bool(fills); counts['partially_filled_orders'] += partial
        counts['full_base_orders'] += full; counts['quote_budget_orders'] += order['requested_shares'] is None
        if first is not None: wait_first.append(order['first_wait_ms'])
        if full_at is not None: wait_full.append(order['full_wait_ms'])
        if not fills: right_censored.append(order['unfilled_observed_ms'])
        counts['right_censored_orders'] += order['right_censored']
        successful_cancel = next((c for c in cancels[identity] if c['outcome'] == 'Canceled'), None)
        order['cancellations'] = cancels[identity]
        if successful_cancel:
            assert terminal and terminal['event'] == 'CallerCanceled', 'successful cancellation missing lifecycle'
            cancel_latency.append(terminal['at'] - successful_cancel['sent_at'])
            before = sum((t['shares'] for t in fills if t['matched_key'] < terminal['key']), D(0))
            order['matched_before_cancel_shares'] = before
            counts['successful_canceled_orders'] += 1
            counts['canceled_after_any_fill'] += before > 0
            counts['partial_fill_then_cancel'] += before > 0 and order['requested_shares'] is not None and before < order['requested_shares']
        req = requests[order['request_id']]
        ref = req['public_state'].get(order['condition_id'])
        state = decision_states.get((order['condition_id'], ref['pm_group']), {}).get(order['token_id']) if ref else None
        distance = None
        status = 'UnknownMissingFrozenState'
        if state:
            best = state['best_bid_ask']; value = best['value']
            if best['quality'] != 'fresh': status = 'UnknownQuality:' + best['quality']
            elif not value or value['best_bid_pusd_per_share'] is None: status = 'EmptyBid'
            else:
                distance = order['quote_price'] - D(value['best_bid_pusd_per_share'])
                status = 'AboveBest' if distance > 0 else 'AtBest' if distance == 0 else 'BelowBest'
        order['distance_to_decision_best_bid'] = distance; order['decision_best_status'] = status
        order['price_bucket_10c'] = int(order['quote_price'] * 10)
        for dimension, key in [('exact_price', str(order['quote_price'])), ('price_10c_bin', str(order['price_bucket_10c'])),
                               ('distance_to_best', str(distance) if distance is not None else status)]:
            entry = bins[(dimension, key)]; entry['accepted_orders'] += 1; entry['filled_orders'] += bool(fills); entry['matched_shares'] += quantity

    assert all(order['first_wait_ms'] is None or order['first_wait_ms'] >= 0 for order in orders.values()), 'fill before accepted effective time'
    authoritative_executed = sum((D(role['shares']) for market in report['account_economics_memory']['markets'] for role in market['executed']), D(0))
    derived_executed = sum((t['shares'] for t in trades.values() if t['dispositions'][-1] == 'FinalSuccess'), D(0))
    assert authoritative_executed == derived_executed, 'trade success shares disagree with authoritative account'
    fill_bins = defaultdict(lambda: {'trades': 0, 'shares': D(0), 'principal_pusd': D(0), 'fee_pusd': D(0)})
    for trade in trades.values():
        for dimension, key in [('fill_exact_price', str(trade['price'])), ('fill_10c_bin', str(int(trade['price']*10)))]:
            group = fill_bins[(dimension, key)]
            group['trades'] += 1; group['shares'] += trade['shares']; group['principal_pusd'] += trade['principal']; group['fee_pusd'] += trade['fee']
    args.output.mkdir(parents=True, exist_ok=True)
    for filename, values in [('orders.jsonl', orders.values()), ('trades.jsonl', trades.values())]:
        with (args.output/filename).open('w') as stream:
            for value in values:
                stream.write(json.dumps(value, ensure_ascii=False, default=str) + '\n')
    summary = {'format': 'offline-matching-comparison-details-v1', 'run_id': candidate['run_id'],
        'comparison_identity_sha256': candidate['comparison_identity_sha256'], 'scope_end': args.range_end, 'observed_end': observed_end,
        'cancellation_target_outcomes': cancel_target_outcomes,
        'fill_price_groups': [{'dimension': key[0], 'value': key[1], **value} for key,value in sorted(fill_bins.items())],
        'outcome': candidate['outcome'], 'end_reason': report['end_reason'], 'counts': counts,
        'matched_trades': len(trades),
        'final_disposition_counts': dict(Counter(t['dispositions'][-1] for t in trades.values())),
        'compensated_trades': sum(t['dispositions'][-1] == 'FinalFailureCompensated' for t in trades.values()),
        'fee_pusd_matched': sum((t['fee'] for t in trades.values()), D(0)),
        'principal_pusd_matched': sum((t['principal'] for t in trades.values()), D(0)),
        'matched_shares': sum((t['shares'] for t in trades.values()), D(0)),
        'successful_shares': sum((t['shares'] for t in trades.values() if t['dispositions'][-1] == 'FinalSuccess' ), D(0)),
        'wait_first_ms': distribution(wait_first), 'wait_full_ms': distribution(wait_full),
        'unfilled_observation_duration_ms': distribution(right_censored), 'cancel_sent_to_effective_ms': distribution(cancel_latency),
        'price_groups': [{'dimension': key[0], 'value': key[1], **value} for key,value in sorted(bins.items())],
        'markout': {str(h): {'status_counts': dict(Counter(t['markout'][str(h)]['status'] for t in trades.values())),
            'shares_weighted_signed_mid_minus_fill': (sum((t['shares']*t['markout'][str(h)]['signed_mid_minus_fill'] for t in trades.values() if t['markout'][str(h)]['status']=='Known'), D(0)) /
                sum((t['shares'] for t in trades.values() if t['markout'][str(h)]['status']=='Known'), D(0)))
                if any(t['markout'][str(h)]['status']=='Known' for t in trades.values()) else None} for h in (1000,5000,30000)},
        'account_economics': report.get('account_economics_memory'), 'day_economics': report.get('day_economics'),
        'noe_stats': prefix_complete['stats'], 'input_sha256': identities,
        'method': {'decimal_division_precision': 28, 'markout_horizons_ms': [1000,5000,30000], 'path': 'original public visibility; public_delivery_delay must be zero',
            'markout_direction': 'BUY; (bid+ask)/2 - fill_price; no invented depth/freshness threshold',
            'grouping': 'quote price and original frozen decision PM group',
            'order_wait': 'effective accepted time to matched time; range-end right censor',
            'markout_scope': 'Post-trade original public evaluation path; coverage may extend beyond candidate observation/exit',
            'inventory_peak': 'Unknown; terminal inventory from authoritative account report; hourly points are not peaks',
            'no_profit_selection': True},
        'noe_reader': getattr(args, 'noe_reader_result', None),
        'analysis_cost': {'wall_seconds': time.monotonic()-start, 'candidate_load_seconds': load_seconds,
            'shared_public_wait_and_scan_seconds': public_seconds, 'finalize_seconds': time.monotonic()-finalize_started,
            'scope': 'process high-water RSS; candidate elapsed may include other batch candidates',
            'peak_rss_kib': resource.getrusage(resource.RUSAGE_SELF).ru_maxrss}}
    dump(summary, args.output/'summary.json')
    print(json.dumps({'run_id': candidate['run_id'], 'counts': counts, 'matched_trades': len(trades),
                      'matched_shares': summary['matched_shares'], 'analysis_cost': summary['analysis_cost']}, default=str))


def analyze_many(jobs):
    """Fan out one decoded public row, without a retained public history copy."""
    if not jobs:
        raise ValueError('batch needs at least one candidate')
    started = time.monotonic()
    prepared = []
    try:
        for args in jobs:
            generator = candidate_report(args)
            prepared.append((generator, next(generator)))
        groups = defaultdict(list)
        for item in prepared:
            groups[item[1]['path']].append(item)
        public_start = time.monotonic()
        public_identities = {}
        for path, consumers in groups.items():
            shared_identity = {}
            for row in rows(path, shared_identity, jobs[0], 'shared_public'):
                for generator, _ in consumers:
                    generator.send(row)
            public_identities.update(shared_identity)
            for generator, request in consumers:
                request['identities'].update(shared_identity)
                try:
                    generator.send(None)
                except StopIteration:
                    pass
                else:
                    raise RuntimeError('candidate did not finish after public EOF')
        public_seconds = time.monotonic()-public_start
        cost = {'candidates': len(jobs), 'public_scans': len(groups), 'public_sha256': public_identities,
            'shared_public_and_finalize_seconds': public_seconds, 'wall_seconds': time.monotonic()-started,
            'peak_rss_kib': resource.getrusage(resource.RUSAGE_SELF).ru_maxrss}
        print(json.dumps({'batch_cost': cost}), file=sys.stderr, flush=True)
        return cost
    finally:
        for generator, _ in prepared:
            generator.close()


def analyze(args):
    return analyze_many([args])


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--batch-config', type=Path, help='JSON array of candidate arguments; each distinct public path is scanned once')
    parser.add_argument('--batch-cost', type=Path)
    for field in ['day-output', 'day-config', 'candidate-evidence', 'public-evidence', 'noe-jsonl', 'noe-reader', 'noe-context', 'output']:
        parser.add_argument('--'+field, type=Path)
    parser.add_argument('--day-index', type=int, default=0)
    parser.add_argument('--candidate-index', type=int, default=0)
    parser.add_argument('--range-end', type=int)
    parser.add_argument('--progress-seconds', type=int, default=30)
    arguments = parser.parse_args()
    if arguments.batch_config:
        jobs = []
        for config in json.loads(arguments.batch_config.read_text()):
            job = argparse.Namespace(**vars(arguments))
            for key, value in config.items():
                if key not in vars(job):
                    parser.error('unknown batch field: '+key)
                setattr(job, key, Path(value) if key in ['day_output','day_config','candidate_evidence','public_evidence','noe_jsonl','noe_reader','noe_context','output'] and value is not None else value)
            jobs.append(job)
    else:
        jobs = [arguments]
    for job in jobs:
        if any(getattr(job, key) is None for key in ['day_output','day_config','candidate_evidence','public_evidence','output','range_end']):
            parser.error('candidate needs day/config/candidate/public evidence, output and range-end')
        if bool(job.noe_reader) != bool(job.noe_context) or bool(job.noe_reader) == bool(job.noe_jsonl):
            parser.error('select noe-jsonl or both noe-reader and noe-context')
    cost = analyze_many(jobs)
    if arguments.batch_cost:
        dump(cost, arguments.batch_cost)
