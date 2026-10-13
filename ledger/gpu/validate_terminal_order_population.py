"""正式NOE关闭先到与迟Open/Base：原CPU人口/金额/前缀对照。"""
from pathlib import Path
import sys
import json
import importlib.util
from types import SimpleNamespace
from fractions import Fraction
sys.path=[p for p in sys.path if Path(p).resolve()!=Path(__file__).resolve().parent]
import torch
ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/p) for p in ('sim-system/gpu','execution-normalization/gpu','ledger/gpu','shared-run/gpu')]
from queues import Numbers
from validate_base_execution import integers,rational,values,units
from validate_projection import expected_semantic,entity_sequence
from lifecycle import map_order_canceled,map_order_accepted,map_base_accepted
from projection import Projection
from feedback import FeedbackEmitter
from core import EVIDENCE
from memory import EconomicColumns
from admission import AdmissionLifecycle
from success_pool import SuccessPools
from projected_markets import ProjectedMarkets
from consumption import PendingRecords,ReceiveGates
from public_ledger import PublicLedgerState
from closed import ClosedAdmission
from registry import PublicRegistryState,PublicRegistryEvents
from ledger_drain import PublicLedgerReceiver


def main(source):
    run=Numbers.run_exact_batch
    tensor=lambda v,dtype=torch.int64:torch.tensor(v,dtype=dtype,device='cuda')
    zero=tensor([0]);yes=tensor([True],torch.bool)
    projection=Projection.open(FeedbackEmitter.open(1),zero,zero,zero,zero,tensor([0,0]),integers([416]),zero,zero,zero,zero)
    state=None
    receiver=PublicLedgerReceiver()
    for row,expected in zip(source['records'],source['checkpoints']):
        body=row['record']['Evidence'];kind,value=next(iter(body['evidence'].items()))
        metadata=body['source_reference']['record']
        category=1 if kind=='OrderLifecycle' else 0
        original=metadata['OrderLifecycle' if category else 'DirectResponse']
        private=integers([original['private_feedback_sequence']]);ordinal=tensor([row['position']['record_ordinal']])
        quantity=SimpleNamespace(matched_base_units=integers([0]),open_base_units=integers([5_000_000]),fault=tensor([0],torch.int32))
        if kind=='OrderLifecycle':
            order=integers([entity_sequence(value['result']['order_identity'],'Order')])
            if value['result']['event']=='CallerCanceled':
                mapped=run(lambda:map_order_canceled(order,private,ordinal,zero,zero,zero,yes))
            else:
                mapped=run(lambda:map_order_accepted(quantity,order,private,ordinal,zero,zero,zero,yes))
        else:
            result=value['result']['Submission']['outcome']['Accepted']
            assert result['measurement']['Base']=={'matched_base_quantity_shares':'0','open_base_quantity_shares':'5'}
            order=integers([entity_sequence(result['order']['order_identity'],'Order')])
            mapped=run(lambda:map_base_accepted(quantity,order,private,ordinal,zero,zero,zero,
                tensor([original['private_feedback_sequence']]),zero,
                tensor([result['effective_processing_at_utc_ms']]),yes,yes))
        semantic=run(lambda:expected_semantic(row))
        assert torch.equal(mapped.semantic,semantic)
        projection,batch=run(lambda:projection._finish(projection.emitter,[(mapped,EVIDENCE,private,
            integers([row['position']['source_watermark']['projection_batch_sequence']]),
            tensor([original.get('feedback_received_at_utc_ms',original.get('response_received_at_utc_ms'))]),
            tensor([category]),tensor([original.get('platform_event_at_utc_ms',original.get('platform_responded_at_utc_ms'))]),yes)],tensor([0],torch.int32)))
        assert batch.fault.cpu().tolist()==[0]
        if state is None:
            spec=importlib.util.spec_from_file_location('terminal_population_platform',ROOT/'ledger/gpu/platform.py')
            platform_module=importlib.util.module_from_spec(spec);sys.modules[spec.name]=platform_module;spec.loader.exec_module(platform_module)
            platform=run(lambda:platform_module.PlatformBalances.open(rational(['100','100']+['0']*6),rational(['0','0']),tensor([0,0]),integers([0])))
            columns=run(lambda:EconomicColumns.from_projection_batches([batch]))
            pools=run(lambda:SuccessPools.open(tensor([0,0]),tensor([0,0]),tensor([0,1]),tensor([1,0]),1))
            population=ProjectedMarkets.open(zero,yes[:,None]&False,yes)
            ledger=run(lambda:AdmissionLifecycle.open(platform,projection.normalizer.bank,columns,integers([1]),integers([0]),pools,projected_markets=population))
            pending=run(lambda:PendingRecords.from_columns(batch.emitted,batch.received_at,batch.private_sequences,
                Numbers.ExactInt.from_i64_tensor(projection.normalizer.bank.source_order[batch.rows.flatten(),3]),batch.rows,integers([0])))
            state=PublicLedgerState(pending,PublicRegistryState(PublicRegistryEvents.empty(1,'cuda')),ClosedAdmission.empty(columns),ledger)
        else:state=run(lambda:state.append_batch(projection.normalizer.bank,batch))
        gates=ReceiveGates.unset(1).set_receive(tensor([500]),yes).set_public(tensor([700]),yes)
        state,delivery=run(lambda:state.receive_first(projection.normalizer.bank,gates))
        state,ready=receiver(state,delivery,yes)
        assert ready.cpu().tolist()==[True]
        assert not (state.waiting.received&~state.waiting.consumed).any().cpu().item()
        at,present,fault=run(lambda:state.ledger.orders.lookup(order))
        assert present.cpu().tolist()==[True] and not fault.any().cpu().item()
        assert state.ledger.orders.closed[0,at].cpu().tolist()==[True]
        assert state.ledger.orders.flags[0,at].cpu().tolist()==[False]
        assert not state.ledger.projected_markets.present.any().cpu().item() and not expected['market_projections']
        assert values(state.ledger.current_entry_sequence)==[expected['ledger_entry_seq']]
        assert values(state.ledger.current_local_prefix)==[expected['ledger_local_prefix']]
        assert values(state.ledger.platform.platform_source_prefix)==[expected['platform_source_prefix']]
        assert [Fraction(n,d) for n,d in zip(values(state.ledger.platform.balances.numerator),values(state.ledger.platform.balances.denominator))]==[Fraction(100),Fraction(100)]+[Fraction(0)]*6
    print(json.dumps({'records':3,'market_population':0,'open_order_count':0,'closed':True,'entry':values(state.ledger.current_entry_sequence),'passed':True}))


if __name__=='__main__':main(json.loads(Path(sys.argv[1]).read_text()))
