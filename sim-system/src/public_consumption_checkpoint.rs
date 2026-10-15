// 日内恢复专用的共享观察表示；引用只来自本批实际 Arc 身份，不按内容重新编码或哈希。
mod observation_pool {
    use super::*;
    use std::collections::BTreeMap;
    #[derive(Default, Serialize, Deserialize)]
    struct Pool {
        books: Vec<Arc<ObservedFactV1<Vec<PriceLevelV1>>>>,
        depths: Vec<Depth>,
        events: Vec<Vec<Fact>>,
        progress: Vec<Arc<[SourceStreamProgressV1]>>,
        depth_values: Vec<Arc<BinanceDepthStateReferenceV1>>,
        #[serde(default)]
        market_snapshots: Vec<Snapshot>,
        #[serde(default)]
        source_states: Vec<Arc<SourceStreamProgressV1>>,
        #[serde(skip)]
        resolved_market_snapshots: Vec<Arc<MarketObservationSnapshotV1>>,
        #[serde(skip)]
        resolved_depths: Vec<Arc<ObservedFactV1<BinanceDepthObservationV1>>>,
        #[serde(skip)]
        resolved_events: Vec<Arc<[PublicBusinessFactV1]>>,
    }
    #[derive(Default)]
    struct Builder {
        pool: Pool,
        books: BTreeMap<usize, usize>,
        depths: BTreeMap<usize, usize>,
        events: BTreeMap<usize, usize>,
        progress: BTreeMap<usize, usize>,
        depth_values: BTreeMap<usize, usize>,
        market_snapshots: BTreeMap<usize,usize>,
        source_states: BTreeMap<usize,usize>,
    }
    fn intern<T: ?Sized>(
        value: &Arc<T>,
        ids: &mut BTreeMap<usize, usize>,
        values: &mut Vec<Arc<T>>,
    ) -> usize {
        *ids.entry(Arc::as_ptr(value) as *const () as usize)
            .or_insert_with(|| {
                let index = values.len();
                values.push(value.clone());
                index
            })
    }
    fn get<T: ?Sized>(values: &[Arc<T>], index: usize) -> Result<Arc<T>, String> {
        values
            .get(index)
            .cloned()
            .ok_or_else(|| "恢复共享观察引用不存在".into())
    }
    #[derive(Serialize, Deserialize)]
    enum DepthValue {
        Full(usize),
        NotMaterialized,
    }
    #[derive(Serialize, Deserialize)]
    struct Depth {
        value: Option<DepthValue>,
        quality: MarketDataQuality,
        direct_anchors: Vec<DirectObservationAnchorV1>,
        dependency_frontier: Option<AppliedIngressPrefixV1>,
    }
    #[derive(Serialize, Deserialize)]
    enum Payload {
        Full(PublicBusinessPayloadV1),
        Depth(usize),
        DepthPosition(Option<usize>),
    }
    #[derive(Serialize, Deserialize)]
    struct Fact {
        position: PublicFactPositionV1,
        payload: Payload,
        source_refs: Vec<CanonicalInputRecordRefV1>,
        lineage: PublicBusinessLineageV1,
        direct_time: EventTimeV2,
        dependency_time: EventTimeV2,
        quality: EventQualityV2,
        direct_sources: Vec<SourceEvidenceRefV3>,
    }
    impl Builder {
        fn depth(&mut self, value: &Arc<ObservedFactV1<BinanceDepthObservationV1>>) -> usize {
            let key = Arc::as_ptr(value) as usize;
            if let Some(index) = self.depths.get(&key) {
                return *index;
            }
            let index = self.pool.depths.len();
            let body = value.value.as_ref().map(|v| match v {
                BinanceDepthObservationV1::Full(v) => DepthValue::Full(intern(
                    v,
                    &mut self.depth_values,
                    &mut self.pool.depth_values,
                )),
                BinanceDepthObservationV1::NotMaterialized => DepthValue::NotMaterialized,
            });
            self.pool.depths.push(Depth {
                value: body,
                quality: value.quality,
                direct_anchors: value.direct_anchors.clone(),
                dependency_frontier: value.dependency_frontier.clone(),
            });
            self.depths.insert(key, index);
            index
        }
        fn events(&mut self, value: &Arc<[PublicBusinessFactV1]>) -> usize {
            let key = Arc::as_ptr(value) as *const () as usize;
            if let Some(index) = self.events.get(&key) {
                return *index;
            }
            let index = self.pool.events.len();
            let facts = value
                .iter()
                .map(|v| {
                    let payload = match &v.payload {
                        PublicBusinessPayloadV1::ReferenceFact(
                            CanonicalReferenceEventPayloadV2::BinanceDepthState(depth),
                        ) => Payload::Depth(intern(
                            depth,
                            &mut self.depth_values,
                            &mut self.pool.depth_values,
                        )),
                        PublicBusinessPayloadV1::BinanceDepthPositionReference { quantities } =>
                            Payload::DepthPosition(quantities.as_ref().map(|depth|
                                intern(depth, &mut self.depth_values, &mut self.pool.depth_values))),
                        payload => Payload::Full(payload.clone()),
                    };
                    Fact {
                        position: v.position.clone(),
                        payload,
                        source_refs: v.source_refs.clone(),
                        lineage: v.lineage.clone(),
                        direct_time: v.direct_time.clone(),
                        dependency_time: v.dependency_time.clone(),
                        quality: v.quality.clone(),
                        direct_sources: v.direct_sources.clone(),
                    }
                })
                .collect();
            self.pool.events.push(facts);
            self.events.insert(key, index);
            index
        }
    }
    impl Pool {
        fn resolve(&mut self) -> Result<(), String> {
            self.resolved_depths = self
                .depths
                .iter()
                .map(|d| {
                    let value =
                        d.value
                            .as_ref()
                            .map(|v| match v {
                                DepthValue::Full(index) => get(&self.depth_values, *index)
                                    .map(BinanceDepthObservationV1::Full),
                                DepthValue::NotMaterialized => {
                                    Ok(BinanceDepthObservationV1::NotMaterialized)
                                }
                            })
                            .transpose()?;
                    Ok(Arc::new(ObservedFactV1 {
                        value,
                        quality: d.quality,
                        direct_anchors: d.direct_anchors.clone(),
                        dependency_frontier: d.dependency_frontier.clone(),
                    }))
                })
                .collect::<Result<_, String>>()?;
            self.resolved_events = self
                .events
                .iter()
                .map(|events| {
                    events
                        .iter()
                        .map(|v| {
                            let payload = match &v.payload {
                                Payload::Full(payload) => payload.clone(),
                                Payload::DepthPosition(index) => PublicBusinessPayloadV1::BinanceDepthPositionReference {
                                    quantities: index.map(|i| get(&self.depth_values, i)).transpose()?,
                                },
                                Payload::Depth(index) => PublicBusinessPayloadV1::ReferenceFact(
                                    CanonicalReferenceEventPayloadV2::BinanceDepthState(get(
                                        &self.depth_values,
                                        *index,
                                    )?),
                                ),
                            };
                            Ok(PublicBusinessFactV1 {
                                position: v.position.clone(),
                                payload,
                                source_refs: v.source_refs.clone(),
                                lineage: v.lineage.clone(),
                                direct_time: v.direct_time.clone(),
                                dependency_time: v.dependency_time.clone(),
                                quality: v.quality.clone(),
                                direct_sources: v.direct_sources.clone(),
                            })
                        })
                        .collect::<Result<Arc<[_]>, String>>()
                })
                .collect::<Result<_, String>>()?;
            self.resolved_market_snapshots = self.market_snapshots.iter().map(|o| {
                // 复用原观察池；只在恢复边界组装一份原市场版本。
                o.clone().restore(self).map(Arc::new)
            }).collect::<Result<_,_>>()?;
            Ok(())
        }
    }
    #[derive(Clone, Serialize, Deserialize)]
    struct Asset {
        asset: AssetIdentityV1,
        bids: usize,
        asks: usize,
        best_bid_ask: ObservedFactV1<CanonicalBbaFactV1>,
        valuation_best_bid: ObservedFactV1<Option<ExactDecimal>>,
        tick: ObservedFactV1<ExactDecimal>,
        public_trade: ObservedFactV1<PublicTradeV2>,
        empty_valuation_bid: bool,
    }
    impl Asset {
        fn capture(value: &AssetBusinessObservationV1, builder: &mut Builder) -> Self {
            Self {
                asset: value.asset.clone(),
                bids: intern(&value.bids, &mut builder.books, &mut builder.pool.books),
                asks: intern(&value.asks, &mut builder.books, &mut builder.pool.books),
                best_bid_ask: value.best_bid_ask.clone(),
                valuation_best_bid: value.valuation_best_bid.as_ref().clone(),
                tick: value.tick.clone(),
                public_trade: value.public_trade.clone(),
                empty_valuation_bid: value.valuation_best_bid.value == Some(None),
            }
        }
        fn restore(self, pool: &Pool) -> Result<AssetBusinessObservationV1, String> {
            let mut valuation_best_bid = self.valuation_best_bid;
            if self.empty_valuation_bid {
                valuation_best_bid.value = Some(None);
            }
            Ok(AssetBusinessObservationV1 {
                asset: self.asset,
                bids: get(&pool.books, self.bids)?,
                asks: get(&pool.books, self.asks)?,
                best_bid_ask: self.best_bid_ask,
                valuation_best_bid: Arc::new(valuation_best_bid),
                tick: self.tick,
                public_trade: self.public_trade,
            })
        }
    }
    #[derive(Clone, Serialize, Deserialize)]
    struct Observation {
        schema_version: String,
        market: MarketIdentityV2,
        group_id: Sha256Hex,
        group_position: u64,
        visible_prefix: AppliedIngressPrefixV1,
        assets: Vec<Asset>,
        lifecycle: ObservedFactV1<MappedMarketLifecycleStateV1>,
        active: ObservedFactV1<bool>,
        closed: ObservedFactV1<bool>,
        accepting_orders: ObservedFactV1<bool>,
        terminal: ObservedFactV1<MarketResolutionTerminalV3>,
        official_anchor: ObservedFactV1<MarketPriceAnchorV1>,
        settlement_reference: ObservedFactV1<BtcSettlementReferenceV1>,
        temporary_reference: Option<TemporaryTwapReferenceV1>,
        effective_reference: EffectiveMarketReferenceV1,
        chainlink_spot: ObservedFactV1<ChainlinkBtcUsdReferenceV1>,
        chainlink_twap_thirty: ObservedFactV1<ChainlinkBtcUsdTwapReferenceV1>,
        chainlink_twap_sixty: ObservedFactV1<ChainlinkBtcUsdTwapReferenceV1>,
        binance_depth: usize,
        binance_trade: ObservedFactV1<BinanceAggregateTradeReferenceV1>,
        group_events: usize,
        source_progress: usize,
    }
    impl Observation {
        fn capture(value: &MarketBusinessObservationV1, builder: &mut Builder) -> Self {
            Self {
                schema_version: value.schema_version.clone(),
                market: value.market.clone(),
                group_id: value.group_id.clone(),
                group_position: value.group_position,
                visible_prefix: value.visible_prefix.clone(),
                assets: value
                    .assets
                    .iter()
                    .map(|asset| Asset::capture(asset, builder))
                    .collect(),
                lifecycle: value.lifecycle.clone(),
                active: value.active.clone(),
                closed: value.closed.clone(),
                accepting_orders: value.accepting_orders.clone(),
                terminal: value.terminal.clone(),
                official_anchor: value.official_anchor.clone(),
                settlement_reference: value.settlement_reference.clone(),
                temporary_reference: value.temporary_reference.clone(),
                effective_reference: value.effective_reference.clone(),
                chainlink_spot: value.chainlink_spot.clone(),
                chainlink_twap_thirty: value.chainlink_twap_thirty.clone(),
                chainlink_twap_sixty: value.chainlink_twap_sixty.clone(),
                binance_depth: builder.depth(&value.binance_depth),
                binance_trade: value.binance_trade.clone(),
                group_events: builder.events(&value.group_events),
                source_progress: intern(
                    value.source_progress.as_arc(),
                    &mut builder.progress,
                    &mut builder.pool.progress,
                ),
            }
        }
        fn restore(self, pool: &Pool) -> Result<MarketBusinessObservationV1, String> {
            Ok(MarketBusinessObservationV1 {
                schema_version: self.schema_version,
                market: self.market,
                group_id: self.group_id,
                group_position: self.group_position,
                visible_prefix: self.visible_prefix,
                assets: self
                    .assets
                    .into_iter()
                    .map(|asset| asset.restore(pool))
                    .collect::<Result<_, _>>()?,
                lifecycle: self.lifecycle,
                active: self.active,
                closed: self.closed,
                accepting_orders: self.accepting_orders,
                terminal: self.terminal,
                official_anchor: self.official_anchor,
                settlement_reference: self.settlement_reference,
                temporary_reference: self.temporary_reference,
                effective_reference: self.effective_reference,
                chainlink_spot: self.chainlink_spot,
                chainlink_twap_thirty: self.chainlink_twap_thirty,
                chainlink_twap_sixty: self.chainlink_twap_sixty,
                binance_depth: get(&pool.resolved_depths, self.binance_depth)?,
                binance_trade: self.binance_trade,
                group_events: get(&pool.resolved_events, self.group_events)?,
                source_progress: get(&pool.progress, self.source_progress)?.into(),
            })
        }
    }
    #[derive(Clone, Serialize, Deserialize)]
    struct Snapshot {
    market: MarketIdentityV2,
    assets: Vec<Asset>,
    lifecycle: ObservedFactV1<MappedMarketLifecycleStateV1>,
    active: ObservedFactV1<bool>,
    closed: ObservedFactV1<bool>,
    accepting_orders: ObservedFactV1<bool>,
    terminal: ObservedFactV1<MarketResolutionTerminalV3>,
    official_anchor: ObservedFactV1<MarketPriceAnchorV1>,
    settlement_reference: ObservedFactV1<BtcSettlementReferenceV1>,
    temporary_reference: Option<TemporaryTwapReferenceV1>,
    effective_reference: EffectiveMarketReferenceV1,
    }
    impl Snapshot {
        fn capture(o:&MarketObservationSnapshotV1,b:&mut Builder)->Self {
            Self {
                market:o.market.clone(),
                assets:o.assets.iter().map(|a|Asset::capture(a,b)).collect(),
                lifecycle:o.lifecycle.clone(),
                active:o.active.clone(),
                closed:o.closed.clone(),
                accepting_orders:o.accepting_orders.clone(),
                terminal:o.terminal.clone(),
                official_anchor:o.official_anchor.clone(),
                settlement_reference:o.settlement_reference.clone(),
                temporary_reference:o.temporary_reference.clone(),
                effective_reference:o.effective_reference.clone(),
            }
        }
        fn restore(self,p:&Pool)->Result<MarketObservationSnapshotV1,String> {
            Ok(MarketObservationSnapshotV1 {
                market:self.market,
                assets:self.assets.into_iter().map(|a|a.restore(p)).collect::<Result<_,_>>()?,
                lifecycle:self.lifecycle,
                active:self.active,
                closed:self.closed,
                accepting_orders:self.accepting_orders,
                terminal:self.terminal,
                official_anchor:self.official_anchor,
                settlement_reference:self.settlement_reference,
                temporary_reference:self.temporary_reference,
                effective_reference:self.effective_reference,
            })
        }
    }
    #[derive(Serialize,Deserialize)]
    struct SharedGroup { markets:Vec<usize>, facts:GroupFacts }
    #[derive(Serialize,Deserialize)]
    struct GroupFacts {
        schema_version:String, group_id:Sha256Hex, group_position:u64, visible_prefix:AppliedIngressPrefixV1,
        chainlink_spot:ObservedFactV1<ChainlinkBtcUsdReferenceV1>,
        chainlink_twap_thirty:ObservedFactV1<ChainlinkBtcUsdTwapReferenceV1>,
        chainlink_twap_sixty:ObservedFactV1<ChainlinkBtcUsdTwapReferenceV1>,
        binance_depth:usize, binance_trade:ObservedFactV1<BinanceAggregateTradeReferenceV1>,
        group_events:usize, source_progress:Vec<usize>,
        #[serde(default,skip_serializing_if="Option::is_none")]
        original_progress:Option<usize>,
    }
    impl GroupFacts {
        fn capture(g:&MarketObservationGroupV1,b:&mut Builder)->Self {
            Self {schema_version:g.schema_version.clone(),group_id:g.group_id.clone(),group_position:g.group_position,visible_prefix:g.visible_prefix.clone(),
                chainlink_spot:g.chainlink_spot.clone(),chainlink_twap_thirty:g.chainlink_twap_thirty.clone(),chainlink_twap_sixty:g.chainlink_twap_sixty.clone(),
                binance_depth:b.depth(&g.binance_depth),binance_trade:g.binance_trade.clone(),group_events:b.events(&g.group_events),
                source_progress:g.source_progress.shared_values().map(|values|values.values().map(|p|intern(p,&mut b.source_states,&mut b.pool.source_states)).collect()).unwrap_or_default(),
                original_progress:if g.source_progress.shared_values().is_none() {Some(intern(g.source_progress.as_arc(),&mut b.progress,&mut b.pool.progress))} else {None} }
        }
        fn restore(self,p:&Pool)->Result<MarketObservationGroupV1,String> {
            let progress=if let Some(id)=self.original_progress {get(&p.progress,id)?.into()} else {
                SharedSourceProgressV1::new(self.source_progress.into_iter().map(|i|get(&p.source_states,i).map(|v|(v.scope.clone(),v)))
                .collect::<Result<SourceProgressSnapshotsV1,_>>()?) };
            Ok(MarketObservationGroupV1 {schema_version:self.schema_version,group_id:self.group_id,group_position:self.group_position,visible_prefix:self.visible_prefix,
                chainlink_spot:self.chainlink_spot,chainlink_twap_thirty:self.chainlink_twap_thirty,chainlink_twap_sixty:self.chainlink_twap_sixty,
                binance_depth:get(&p.resolved_depths,self.binance_depth)?,binance_trade:self.binance_trade,group_events:get(&p.resolved_events,self.group_events)?,source_progress:progress})
        }
    }
    #[derive(Serialize, Deserialize)]
    struct Group {
        group_id: Sha256Hex,
        group_position: u64,
        member_input_positions: Vec<u64>,
        closed_at_input_position: u64,
        records: usize,
        #[serde(default)]
        observations: Vec<Observation>,
        #[serde(default, skip_serializing_if="Option::is_none")]
        shared: Option<SharedGroup>,
    }
    impl Group {
        fn capture(value: &ClosedBusinessGroupV1, builder: &mut Builder) -> Self {
            Self {
                group_id: value.group_id.clone(),
                group_position: value.group_position,
                member_input_positions: value.member_input_positions.clone(),
                closed_at_input_position: value.closed_at_input_position,
                records: builder.events(&value.records),
                observations: if value.observations.shared_parts().is_some() { Vec::new() } else {
                    value.observations.iter().map(|o| Observation::capture(o,builder)).collect()
                },
                shared: value.observations.shared_parts().map(|(markets,group)| {
                    let markets = markets.values().map(|o| {
                        let key=Arc::as_ptr(o) as usize;
                        if let Some(id)=builder.market_snapshots.get(&key) {return *id;}
                        let id=builder.pool.market_snapshots.len();
                        let saved=Snapshot::capture(o,builder);
                        builder.pool.market_snapshots.push(saved); builder.market_snapshots.insert(key,id); id
                    }).collect();
                    SharedGroup { markets, facts:GroupFacts::capture(group,builder) }
                }),
            }
        }
        fn restore(self, pool: &Pool) -> Result<Arc<ClosedBusinessGroupV1>, String> {
            Ok(Arc::new(ClosedBusinessGroupV1 {
                group_id: self.group_id,
                group_position: self.group_position,
                member_input_positions: self.member_input_positions,
                closed_at_input_position: self.closed_at_input_position,
                records: get(&pool.resolved_events, self.records)?,
                observations: if let Some(shared) = self.shared {
                    let markets=shared.markets.into_iter().map(|id|get(&pool.resolved_market_snapshots,id)
                        .map(|o|(o.market.condition_id.0.clone(),o))).collect::<Result<MarketObservationSnapshotsV1,_>>()?;
                    MarketObservationSetV1::shared(markets,shared.facts.restore(pool)?)
                } else {
                    self.observations.into_iter().map(|o|o.restore(pool)).collect::<Result<Vec<_>,_>>()?.into()
                },
            }))
        }
    }
    #[derive(Serialize, Deserialize)]
    enum Layout {
        SharedObservationV2,
        SharedObservationV3,
        SharedObservationV4,
    }

    pub(super) mod group {
        use super::*;
        #[derive(Serialize, Deserialize)]
        struct Saved {
            layout: Layout,
            pool: Pool,
            group: Group,
        }
        pub fn serialize<S: serde::Serializer>(
            value: &Arc<ClosedBusinessGroupV1>,
            serializer: S,
        ) -> Result<S::Ok, S::Error> {
            let mut builder = Builder::default();
            let group = Group::capture(value, &mut builder);
            Saved {
                layout: Layout::SharedObservationV4,
                pool: builder.pool,
                group,
            }
            .serialize(serializer)
        }
        pub fn deserialize<'de, D: serde::Deserializer<'de>>(
            deserializer: D,
        ) -> Result<Arc<ClosedBusinessGroupV1>, D::Error> {
            let mut saved = Saved::deserialize(deserializer)?;
            saved.pool.resolve().map_err(serde::de::Error::custom)?;
            saved
                .group
                .restore(&saved.pool)
                .map_err(serde::de::Error::custom)
        }
    }
    pub(super) mod deliveries {
        use super::*;
        #[derive(Serialize, Deserialize)]
        struct Item {
            reference: PublicGroupReferenceV1,
            group: Group,
            pm_inputs: Vec<(u64, CanonicalPmMessageV1)>,
            pm_contexts: Vec<PmMarketContextV1<PreparedPmEvidenceV1<SourceEvidenceRefV3>>>,
            delivered_at_utc_ms: i64,
        }
        #[derive(Serialize, Deserialize)]
        struct Saved {
            layout: Layout,
            pool: Pool,
            items: Vec<Item>,
        }
        pub fn serialize<S: serde::Serializer>(
            value: &[Delivery],
            serializer: S,
        ) -> Result<S::Ok, S::Error> {
            let mut builder = Builder::default();
            let items = value
                .iter()
                .map(|d| Item {
                    reference: d.prepared.reference.clone(),
                    group: Group::capture(&d.prepared.group, &mut builder),
                    pm_inputs: d.prepared.pm_inputs.clone(),
                    pm_contexts: d.prepared.pm_contexts.clone(),
                    delivered_at_utc_ms: d.delivered_at_utc_ms,
                })
                .collect();
            Saved {
                layout: Layout::SharedObservationV4,
                pool: builder.pool,
                items,
            }
            .serialize(serializer)
        }
        pub fn deserialize<'de, D: serde::Deserializer<'de>>(
            deserializer: D,
        ) -> Result<Vec<Delivery>, D::Error> {
            let mut saved = Saved::deserialize(deserializer)?;
            saved.pool.resolve().map_err(serde::de::Error::custom)?;
            saved
                .items
                .into_iter()
                .map(|i| {
                    Ok(Delivery {
                        prepared: PreparedGroup {
                            reference: i.reference,
                            group: i
                                .group
                                .restore(&saved.pool)
                                .map_err(serde::de::Error::custom)?,
                            pm_inputs: i.pm_inputs,
                            pm_contexts: i.pm_contexts,
                        },
                        delivered_at_utc_ms: i.delivered_at_utc_ms,
                    })
                })
                .collect()
        }
    }
}
