"""原预留/发送/确认/经济到达读面的短链冷对照，不冒充完整Ledger协调。"""
import torch
from queues import Numbers,ExactInt
from buy_cost import limit_buy_cost
from validate_base_execution import integers,rational,values
from reservation import ReservationBook,run_reserve_action,PLATFORM_PENDING,CONVERTED


def validate(source):
    stages=source['reservation_checkpoints']
    if not stages:
        return {'original_reservation_reference_available':False}
    begin,reserved,sent=stages[:3]
    roots=torch.tensor([0],device='cuda')
    enabled=torch.tensor([True],device='cuda')
    cost=Numbers.run_exact_batch(lambda:limit_buy_cost(integers([500]),rational(['0.3']),rational([source['fee_rate']]),
        source['fee_exponent'],post_only=torch.tensor([False],device='cuda'),minimum_principal_micro=integers([1_000_000])))
    principal=Numbers.run_exact_batch(lambda:type(rational(['0'])).ratio(cost.principal_micro,cost.principal_micro.constant(1_000_000)))
    fee=Numbers.run_exact_batch(lambda:type(principal).ratio(cost.fee_1e5_units,cost.fee_1e5_units.constant(100_000)))
    book=ReservationBook.empty(1)
    invalid_request=torch.tensor([[-1]],device='cuda')
    invalid_book,result=run_reserve_action(book,invalid_request,roots[:,None],principal,fee,enabled[:,None],
        rational(['10']),rational(['10']),integers([1]),integers([0]))
    assert result.fault.cpu().tolist()==[1] and invalid_book.request_slots.cpu().tolist()==[[-1]]
    padding_book,result=run_reserve_action(book,invalid_request,roots[:,None],principal,fee,torch.zeros_like(enabled[:,None]),
        rational(['10']),rational(['10']),integers([1]),integers([0]))
    assert result.fault.cpu().tolist()==[0] and result.outcomes.cpu().tolist()==[[0]]
    assert padding_book.request_slots.cpu().tolist()==[[-1]]
    current=integers([begin['projection']['ledger_entry_seq']])
    local=integers([begin['projection']['ledger_local_prefix']])
    actual=rational([begin['cash']['actual_available_pusd']])
    maker=rational([begin['cash']['maker_available_pusd']])
    book,decision=run_reserve_action(book,roots[:,None],roots[:,None],principal,fee,enabled[:,None],actual,maker,current,local)
    assert decision.fault.cpu().tolist()==[0] and decision.outcomes.cpu().tolist()==[[1]]
    assert values(decision.current_ledger_sequence)==[reserved['projection']['ledger_entry_seq']]
    assert values(decision.current_local_prefix)==[reserved['projection']['ledger_local_prefix']]
    assert values(book.last_sequences)==[reserved['request']['last_ledger_entry_seq']]
    amount=principal+fee
    for reference,state in [(reserved,book)]:
        local_pending,platform_pending=Numbers.run_exact_batch(state.pending)
        expected=rational([reference['projection']['local_pending_reservation_projection']['principal_pusd']])+rational([
            reference['projection']['local_pending_reservation_projection']['immediate_fee_pusd']])
        assert local_pending.compare(expected).order.cpu().tolist()==[0] and platform_pending.numerator.sign.cpu().tolist()==[0]
    repeated,again=run_reserve_action(book,roots[:,None],roots[:,None],principal,fee,enabled[:,None],actual,maker,
        decision.current_ledger_sequence,decision.current_local_prefix)
    assert values(again.current_ledger_sequence)==values(decision.current_ledger_sequence)
    assert values(repeated.last_sequences)==values(book.last_sequences)
    book,required,changed=Numbers.run_exact_batch(lambda:book.submission_started(roots,roots,
        integers([sent['projection']['ledger_entry_seq']]),enabled))
    assert changed.cpu().tolist()==[True] and required.cpu().tolist()==[1]
    assert book.statuses.cpu().tolist()==[[1]] and values(book.last_sequences)==[sent['request']['last_ledger_entry_seq']]
    again,_,changed=Numbers.run_exact_batch(lambda:book.submission_started(roots,roots,
        integers([sent['projection']['ledger_entry_seq']+1]),enabled))
    assert not changed.any().cpu().item() and values(again.last_sequences)==values(book.last_sequences)
    repeated,decision_after_send=run_reserve_action(book,roots[:,None],roots[:,None],principal,fee,enabled[:,None],actual,maker,
        integers([sent['projection']['ledger_entry_seq']]),integers([sent['projection']['ledger_local_prefix']]))
    assert values(decision_after_send.ledger_sequences)==[reserved['request']['last_ledger_entry_seq']]
    assert values(repeated.last_sequences)==[sent['request']['last_ledger_entry_seq']]
    # 真实领取中平台接受先到、经济占款随后；序号这里只由原Ledger对照提供。
    invalid=Numbers.run_exact_batch(lambda:book.definitive(roots,enabled,enabled,integers([1<<64]),enabled))
    assert invalid.fault.cpu().tolist()==[1] and invalid.statuses.cpu().tolist()==book.statuses.cpu().tolist()
    assert values(invalid.last_sequences)==values(book.last_sequences)
    pending=next(row for row in stages[3:] if row['request']['status']=='PlatformConfirmedPendingEconomicFact')
    converted=next(row for row in stages[3:] if row['request']['status']=='Converted')
    book=Numbers.run_exact_batch(lambda:book.definitive(roots,enabled,torch.tensor([False],device='cuda'),
        integers([pending['request']['last_ledger_entry_seq']]),enabled))
    assert book.statuses.cpu().tolist()==[[PLATFORM_PENDING]]
    local_pending,platform_pending=Numbers.run_exact_batch(book.pending)
    assert local_pending.numerator.sign.cpu().tolist()==[0] and platform_pending.compare(amount).order.cpu().tolist()==[0]
    book=Numbers.run_exact_batch(lambda:book.definitive(roots,enabled,enabled,
        integers([converted['request']['last_ledger_entry_seq']]),enabled))
    assert book.statuses.cpu().tolist()==[[CONVERTED]]
    local_pending,platform_pending=Numbers.run_exact_batch(book.pending)
    assert local_pending.numerator.sign.cpu().tolist()==[0] and platform_pending.numerator.sign.cpu().tolist()==[0]
    repeated,decision_after_convert=run_reserve_action(book,roots[:,None],roots[:,None],principal,fee,enabled[:,None],actual,maker,
        integers([converted['projection']['ledger_entry_seq']]),integers([converted['projection']['ledger_local_prefix']]))
    assert values(decision_after_convert.ledger_sequences)==[reserved['request']['last_ledger_entry_seq']]
    assert values(repeated.last_sequences)==[converted['request']['last_ledger_entry_seq']]
    # 同一已转换预留重复结果不释放/占用第二次，last成功序保留。
    repeated=Numbers.run_exact_batch(lambda:book.definitive(roots,enabled,enabled,
        integers([converted['request']['last_ledger_entry_seq']+1]),enabled))
    assert values(repeated.last_sequences)==values(book.last_sequences)
    # 原整体资金不足决定必须同时缓存两身份；资金后来增多也不改旧决定。
    two=torch.tensor([[1,2]],device='cuda')
    refused,decision=run_reserve_action(book,two,torch.zeros_like(two),rational(['12','9']),rational(['0','0']),
        torch.ones_like(two,dtype=torch.bool),rational(['20']),rational(['20']),integers([100]),integers([1]))
    assert refused.capacity==4 and decision.outcomes.cpu().tolist()==[[2,2]]
    assert values(decision.current_ledger_sequence)==[100]
    refused,decision=run_reserve_action(refused,two,torch.zeros_like(two),rational(['12','9']),rational(['0','0']),
        torch.ones_like(two,dtype=torch.bool),rational(['200']),rational(['200']),integers([100]),integers([1]))
    assert decision.outcomes.cpu().tolist()==[[2,2]]
    return {'original_reservation_reference_available':True,'original_reserve_send_pending_convert_checked':True,
        'request_and_send_repeat_checked':True,'whole_action_refusal_sticky_checked':True,
        'full_NOE_Ledger_sequence_and_binding_coordinator_delivered':False}
