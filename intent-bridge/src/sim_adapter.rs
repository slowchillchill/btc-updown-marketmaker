use sim_system::execution::{
    MutationRequestReceiptV2, RequestContextV2, SimRunActorErrorV2, TrustedDecisionCutoffV2,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SimIntentCallContextV1 {
    pub request_context: RequestContextV2,
    pub trusted_decision_cutoff: TrustedDecisionCutoffV2,
}

pub type SimIntentImmediateSuccessV1 = MutationRequestReceiptV2;
pub type SimIntentErrorV1 = SimRunActorErrorV2;
