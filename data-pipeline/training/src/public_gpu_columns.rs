//! 紧凑公共PM步骤到共享设备列。只转已有事实，不重新计算public/hidden归约。
use crate::{compact_public::{CompactPmChangeV1, PublicCompactReaderV1}, public_recording::RecordingResult};
use contracts::*;
use data_pipeline_core::{pm_reduction::PmStepChangeV1, public_input::{DptIngressV1, TypedRawRowV2},
    typed_raw::{TypedRawRow, PolymarketOrderbookEvent, PolymarketOrderbookRow, normalize_decimal_text}};
use serde::Serialize;
use std::collections::BTreeMap;

/// 所有序号为i64设备坐标；各更新列为[目标,原步骤序号,公共组序号,值]。
/// 原身份/精确词法/缺失及其它事实继续由绑定的compact材料拥有。
#[derive(Default, Serialize)]
pub struct PublicPmDeviceColumnsV1 {
    pub version: &'static str,
    pub input_stream_id: String,
    pub assets: Vec<AssetIdentityV1>,
    pub markets: Vec<MarketIdentityV2>,
    /// 原固定身份：[market,asset,outcome.index]；不是由出现盘口的顺序推导市场。
    pub market_assets: Vec<[i64;3]>,
    pub decimals: Vec<String>,
    /// [asset,bid=0/ask=1,price_decimal_id]；保留全部实际价位和显式零量。
    pub slots: Vec<[i64;3]>,
    /// [group_position,closed_input,visible_at_utc_ms]，含无PM输出组。
    pub groups: Vec<[i64;3]>,
    pub quantities: Vec<[i64;4]>,
    pub selections: Vec<[i64;4]>,
    pub resets: Vec<[i64;4]>,
    pub initialized: Vec<[i64;4]>,
    pub ready: Vec<[i64;4]>,
    pub ticks: Vec<[i64;4]>,
    /// 直接锚点的原证据；槽位列决定原level字段路径，不改成依赖时间。
    pub anchors: Vec<PublicLevelAnchorV1>,
    pub level_anchors: Vec<[i64;4]>,
    /// 与原Native Q6一致：Fresh/Stale/Missing/Unknown/Gap/Invalid。
    pub book_quality: Vec<[i64;4]>,
    /// [bidPresent,bidId,askPresent,askId,spreadPresent,spreadId,anchorSet,quality]。
    pub bba_values: Vec<[i64;8]>,
    pub bba: Vec<[i64;4]>,
    /// [priceId,quantityId,buy=0/sell=1,feePresent,feeId,anchorSet,quality]。
    pub trade_values: Vec<[i64;7]>,
    pub trades: Vec<[i64;4]>,
    /// [valuePresent,decimalId,anchorSet,quality]，与原观察tick区分经济tick状态。
    pub observed_tick_values: Vec<[i64;4]>,
    pub observed_ticks: Vec<[i64;4]>,
    pub anchor_sets: Vec<Vec<i64>>,
    /// 原公共payload枚举名；不含模型特征分类或时间聚合。
    pub fact_types: Vec<String>,
    /// [group,typeId,directSourceStart,directSourceEnd]，一行对应一个实际规范事实。
    pub facts: Vec<[i64;4]>,
    /// 全部直接来源的原双时间：[group,typeId,sourcePresent,sourceMs,receivedMs]。
    pub fact_sources: Vec<[i64;5]>,
    /// 原事实成交：[group,asset(-1=Binance),priceId,qPresent,qId,side(B/S/U/None),
    /// feePresent,feeId,sourcePresent,sourceMs,receivedMs]；不等同public最新成交步骤。
    pub raw_trades: Vec<[i64;11]>,
    pub field_updates: i64,
    pub original_pm_steps: i64,
    pub lifecycle_states: Vec<MappedMarketLifecycleStateV1>,
    /// lifecycle [present,stateId,anchorSet,quality]；三开关 [p,v,p,v,p,v,anchorSet,quality]。
    pub lifecycle_values: Vec<[i64;4]>,
    pub trading_values: Vec<[i64;8]>,
    /// terminal [present,winnerAsset,payoutSet,anchorSet,quality]，完整原payout向量另存。
    pub terminal_values: Vec<[i64;5]>,
    pub terminal_payouts: Vec<Vec<[i64;2]>>,
    pub market_admitted: Vec<[i64;4]>,
    pub market_lifecycle: Vec<[i64;4]>,
    pub market_trading: Vec<[i64;4]>,
    pub market_terminal: Vec<[i64;4]>,
}
#[derive(Serialize)]
pub struct PublicLevelAnchorV1 {
    pub direct: SourceEvidenceRefV3,
    pub position: PublicFactPositionV1,
    pub group_id: Sha256Hex,
}
fn quality_code(quality: MarketDataQuality) -> i64 {
    match quality {
        MarketDataQuality::Fresh => 0, MarketDataQuality::Stale => 1,
        MarketDataQuality::Missing => 2, MarketDataQuality::UnknownNotObserved => 3,
        MarketDataQuality::ConfirmedGap => 4, MarketDataQuality::Invalid => 5,
    }
}
fn pm_payload_type(event: &PolymarketOrderbookEvent) -> RecordingResult<&'static str> {
    Ok(match event {
        PolymarketOrderbookEvent::Book {..} => "pm.full_book",
        PolymarketOrderbookEvent::PriceChange {..} => "pm.absolute_level_changes",
        PolymarketOrderbookEvent::BestBidAsk {..} => "pm.best_bid_ask",
        PolymarketOrderbookEvent::TickSizeChange {..} => "pm.tick_size_change",
        PolymarketOrderbookEvent::LastTradePrice {..} => "pm.public_trade",
        PolymarketOrderbookEvent::NewMarket {..} => "pm.new_market",
        PolymarketOrderbookEvent::MarketResolved {..} => return Err("终态原输入不能充当规范PM消息".into()),
    })
}
fn inline_payload_type(payload: &PublicBusinessPayloadV1) -> RecordingResult<&'static str> {
    Ok(match payload {
        PublicBusinessPayloadV1::MarketFact {payload,..} => match payload {
            CanonicalMarketFactPayloadV1::FixedIdentity {..} => "market.fixed_identity",
            CanonicalMarketFactPayloadV1::InitialTick {..} => "market.initial_tick",
            CanonicalMarketFactPayloadV1::LifecycleTransition(_) => "market.lifecycle_transition",
            CanonicalMarketFactPayloadV1::MarketLifecycleSnapshot {..} => "market.lifecycle_snapshot",
            CanonicalMarketFactPayloadV1::TradingState(_) => "market.trading_state",
            CanonicalMarketFactPayloadV1::Terminal(_) => "market.terminal",
            CanonicalMarketFactPayloadV1::QualityBoundary(_) => "market.quality_boundary",
        },
        PublicBusinessPayloadV1::ReferenceFact(payload) => match payload {
            CanonicalReferenceEventPayloadV2::ChainlinkBtcUsd(_) => "reference.chainlink_spot",
            CanonicalReferenceEventPayloadV2::ChainlinkBtcUsdTwap(t) if t.window_s==30 => "reference.chainlink_twap30",
            CanonicalReferenceEventPayloadV2::ChainlinkBtcUsdTwap(_) => "reference.chainlink_twap60",
            CanonicalReferenceEventPayloadV2::MarketPriceAnchorUpdate(_) => "reference.market_price_anchor",
            CanonicalReferenceEventPayloadV2::MarketSettlementReferenceUpdate(_) => "reference.market_settlement",
            CanonicalReferenceEventPayloadV2::BinanceDepthState(_) => "reference.binance_depth",
            CanonicalReferenceEventPayloadV2::BinanceAggregateTrade(_) => "reference.binance_trade",
            CanonicalReferenceEventPayloadV2::RtdsObservedAnomaly(a) => match a.classification {
                RtdsObservedAnomalyClassificationV1::InvalidType => "reference.anomaly_type",
                RtdsObservedAnomalyClassificationV1::InvalidTopic => "reference.anomaly_topic",
                RtdsObservedAnomalyClassificationV1::InvalidSymbol => "reference.anomaly_symbol",
                RtdsObservedAnomalyClassificationV1::InvalidTimestamp => "reference.anomaly_timestamp",
            },
        },
        PublicBusinessPayloadV1::PolymarketMessage(_) => return Err("紧凑PM事实应引用原输入".into()),
    })
}
struct Builder {
    columns: PublicPmDeviceColumnsV1,
    assets: BTreeMap<String,i64>, markets: BTreeMap<String,i64>,
    decimals: BTreeMap<String,i64>, slots: BTreeMap<(i64,i64,i64),i64>,
    last_bba: BTreeMap<i64,i64>, last_tick: BTreeMap<i64,i64>,
    last_terminal: BTreeMap<i64,i64>,
}
impl Builder {
    fn market_field(&mut self, market: i64, group: i64, value: i64) -> RecordingResult<[i64;4]> {
        self.field_row(market,group,value)
    }
    fn anchor_set(&mut self, sources: &[SourceEvidenceRefV3], position: &PublicFactPositionV1,
        group_id: &Sha256Hex) -> RecordingResult<i64> {
        let mut ids = Vec::with_capacity(sources.len());
        for direct in sources {
            let id = i64::try_from(self.columns.anchors.len())?;
            self.columns.anchors.push(PublicLevelAnchorV1 {
                direct:direct.clone(),position:position.clone(),group_id:group_id.clone(),
            });
            ids.push(id);
        }
        let id = i64::try_from(self.columns.anchor_sets.len())?;
        self.columns.anchor_sets.push(ids);
        Ok(id)
    }
    fn field_row(&mut self, asset: i64, group: i64, value: i64) -> RecordingResult<[i64;4]> {
        let sequence = self.columns.field_updates;
        self.columns.field_updates = sequence.checked_add(1).ok_or("设备观察字段位置溢出")?;
        Ok([asset,sequence,group,value])
    }
    fn optional_decimal(&mut self, value: &Option<ExactDecimal>) -> RecordingResult<[i64;2]> {
        Ok(match value {Some(value)=>[1,self.decimal(value)?],None=>[0,0]})
    }
    fn observed_tick(&mut self, asset: &AssetIdentityV1, value: &ExactDecimal,
        present: bool, anchors: i64, quality: i64, group: i64) -> RecordingResult<()> {
        let asset = self.asset(asset);
        let value = [i64::from(present),self.decimal(value)?,anchors,quality];
        let id = i64::try_from(self.columns.observed_tick_values.len())?;
        self.columns.observed_tick_values.push(value);
        let row = self.field_row(asset,group,id)?;
        self.columns.observed_ticks.push(row);
        self.last_tick.insert(asset,id);
        Ok(())
    }
    fn observed_bba(&mut self, asset: &AssetIdentityV1, quote: &CanonicalBbaFactV1,
        anchors: i64, quality: i64, group: i64) -> RecordingResult<()> {
        let asset = self.asset(asset);
        let [bp,b] = self.optional_decimal(&quote.best_bid_pusd_per_share)?;
        let [ap,a] = self.optional_decimal(&quote.best_ask_pusd_per_share)?;
        let [sp,s] = self.optional_decimal(&quote.spread_pusd_per_share)?;
        let id = i64::try_from(self.columns.bba_values.len())?;
        self.columns.bba_values.push([bp,b,ap,a,sp,s,anchors,quality]);
        let row = self.field_row(asset,group,id)?;
        self.columns.bba.push(row);
        self.last_bba.insert(asset,id);
        Ok(())
    }
    fn raw_trade(&mut self, group: i64, asset: i64, price: &ExactDecimal,
        quantity: &Option<ExactDecimal>, side: Option<CanonicalPmSideV1>, fee: &Option<ExactDecimal>,
        time: &EventTimeV2) -> RecordingResult<()> {
        let price = self.decimal(price)?;
        let [qp,q] = self.optional_decimal(quantity)?;
        let [fp,f] = self.optional_decimal(fee)?;
        let side = match side {Some(CanonicalPmSideV1::Buy)=>0,Some(CanonicalPmSideV1::Sell)=>1,
            Some(CanonicalPmSideV1::Unknown)=>2,None=>3};
        self.columns.raw_trades.push([group,asset,price,qp,q,side,fp,f,
            i64::from(time.source_event_at_utc_ms.is_some()),time.source_event_at_utc_ms.unwrap_or(0),time.received_at_utc_ms]);
        Ok(())
    }
    fn asset(&mut self, asset: &AssetIdentityV1) -> i64 {
        let next = self.columns.assets.len() as i64;
        *self.assets.entry(asset.token_id.0.clone()).or_insert_with(|| { self.columns.assets.push(asset.clone()); next })
    }
    fn decimal(&mut self, value: &ExactDecimal) -> RecordingResult<i64> {
        let normalized = normalize_decimal_text(&value.0,"public_device_decimal")?.0;
        let next = self.columns.decimals.len() as i64;
        Ok(*self.decimals.entry(normalized.clone()).or_insert_with(|| { self.columns.decimals.push(normalized); next }))
    }
    fn slot(&mut self, asset: &AssetIdentityV1, side: BookSide, price: &ExactDecimal) -> RecordingResult<i64> {
        let asset = self.asset(asset);
        let side = match side { BookSide::Bid => 0, BookSide::Ask => 1 };
        let price = self.decimal(price)?;
        let next = self.columns.slots.len() as i64;
        Ok(*self.slots.entry((asset,side,price)).or_insert_with(|| { self.columns.slots.push([asset,side,price]); next }))
    }
    fn level(&mut self, asset: &AssetIdentityV1, side: BookSide, price: &ExactDecimal,
        quantity: &ExactDecimal, sequence: i64, group: i64) -> RecordingResult<i64> {
        let slot = self.slot(asset,side,price)?;
        let value = self.decimal(quantity)?;
        self.columns.quantities.push([slot,sequence,group,value]);
        Ok(slot)
    }
}
/// 一次冷准备/加载；输出共享只读外生列，不作为完整GPU观察或hidden材料验收。
pub fn public_pm_device_columns_v1(reader: &mut PublicCompactReaderV1,
    mut progress: impl FnMut(u64)) -> RecordingResult<PublicPmDeviceColumnsV1> {
    let mut builder = Builder { columns: PublicPmDeviceColumnsV1 {
        version:"public_pm_device_columns_v1",input_stream_id:reader.manifest.header.input_stream_id.clone(),
        bba_values:vec![[0,0,0,0,0,0,0,3]],trade_values:vec![[0,0,0,0,0,0,3]],
        observed_tick_values:vec![[0,0,0,3]],anchor_sets:vec![vec![]],
        lifecycle_values:vec![[0,0,0,3]],trading_values:vec![[0,0,0,0,0,0,0,3]],
        terminal_values:vec![[0,0,0,0,3]],terminal_payouts:vec![vec![]],..Default::default()
    }, assets:BTreeMap::new(), markets:BTreeMap::new(), decimals:BTreeMap::new(), slots:BTreeMap::new(),
        last_bba:BTreeMap::new(),last_tick:BTreeMap::new(),last_terminal:BTreeMap::new() };
    // 零值占位不是已知零；设备查询另带initialized/selected/present。
    builder.decimal(&ExactDecimal("0".into()))?;
    let mut snapshots = BTreeMap::new();
    let mut raw_pm: BTreeMap<u64,PolymarketOrderbookRow> = BTreeMap::new();
    let mut pm_types: BTreeMap<u64,&'static str> = BTreeMap::new();
    while let Some(packet) = reader.next_packet()? {
        if let DptIngressV1::Message { row:TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row)), .. } = &packet.input.ingress {
            // 只保存原枚举坐标；实际事实是否存在由闭组决定，不能计入被抑制重试。
            if !matches!(&row.event,PolymarketOrderbookEvent::MarketResolved {..}) {
                pm_types.insert(packet.input.input_position,pm_payload_type(&row.event)?);
            }
            if matches!(&row.event,PolymarketOrderbookEvent::BestBidAsk {..} | PolymarketOrderbookEvent::PriceChange {..} |
                PolymarketOrderbookEvent::LastTradePrice {..}) {
                raw_pm.insert(packet.input.input_position,row.clone());
            }
            if let PolymarketOrderbookEvent::Book {asset_id,bids,asks,..} = &row.event {
                snapshots.insert(packet.input.input_position,(asset_id.clone(),bids.clone(),asks.clone()));
            }
        }
        for group in &packet.groups {
            builder.columns.groups.push([i64::try_from(group.group_position)?,i64::try_from(group.closed_at_input_position)?,group.visible_at_utc_ms]);
            for fact in &group.records {
                let kind = match &fact.payload {
                    crate::compact_public::CompactFactPayloadV1::PmFromInput(m) => *pm_types
                        .get(&m.input_record.input_position).ok_or("规范PM事实缺原类型引用")?,
                    crate::compact_public::CompactFactPayloadV1::BinanceDepthFromPrefix {..} => "reference.binance_depth",
                    crate::compact_public::CompactFactPayloadV1::Inline(p) => inline_payload_type(p)?,
                };
                let type_id = if let Some(id) = builder.columns.fact_types.iter().position(|s|s==kind) {
                    i64::try_from(id)?
                } else {
                    let id = i64::try_from(builder.columns.fact_types.len())?;
                    builder.columns.fact_types.push(kind.into()); id
                };
                let group_position = i64::try_from(group.group_position)?;
                let start = i64::try_from(builder.columns.fact_sources.len())?;
                for source in &fact.direct_sources {
                    builder.columns.fact_sources.push([group_position,type_id,
                        i64::from(source.source_event_at_utc_ms.is_some()),
                        source.source_event_at_utc_ms.unwrap_or(0),source.received_at_utc_ms]);
                }
                let end = i64::try_from(builder.columns.fact_sources.len())?;
                builder.columns.facts.push([group_position,type_id,start,end]);
                match &fact.payload {
                    crate::compact_public::CompactFactPayloadV1::PmFromInput(m) if kind=="pm.public_trade" => {
                        let row = raw_pm.get(&m.input_record.input_position).ok_or("原成交事实缺输入")?;
                        let PolymarketOrderbookEvent::LastTradePrice {asset_id,..} = &row.event else {
                            return Err("规范成交引用了非成交输入".into());
                        };
                        let asset = builder.columns.market_assets.iter().find_map(|entry| {
                            let identity = &builder.columns.assets[entry[1] as usize];
                            (builder.columns.markets[entry[0] as usize].condition_id==m.market.condition_id &&
                                identity.token_id.0==*asset_id).then(||identity.clone())
                        }).ok_or("原成交事实缺所属市场注册资产")?;
                        let Some(CanonicalPmPayloadV1::PublicTrade {asset,price_pusd_per_share,
                            quantity_shares,side,fee_rate_bps}) =
                            data_pipeline_core::pm_message::direct_trade_from_typed_raw_v1(row,&asset)? else {
                                return Err("原成交载荷重建失败".into());
                            };
                        let a = builder.asset(&asset);
                        builder.raw_trade(group_position,a,&price_pusd_per_share,&quantity_shares,side,
                            &fee_rate_bps,&fact.direct_time)?;
                    }
                    crate::compact_public::CompactFactPayloadV1::Inline(PublicBusinessPayloadV1::ReferenceFact(
                        CanonicalReferenceEventPayloadV2::BinanceAggregateTrade(t))) => {
                        builder.raw_trade(group_position,-1,&t.price_usdt_per_btc,&Some(t.quantity_btc.clone()),
                            Some(if t.buyer_is_maker {CanonicalPmSideV1::Sell} else {CanonicalPmSideV1::Buy}),
                            &None,&fact.direct_time)?;
                    }
                    _=>{}
                }
                if let crate::compact_public::CompactFactPayloadV1::Inline(
                    PublicBusinessPayloadV1::MarketFact {market,payload:CanonicalMarketFactPayloadV1::FixedIdentity {assets}}
                ) = &fact.payload {
                    let next = i64::try_from(builder.columns.markets.len())?;
                    let market_id = *builder.markets.entry(market.condition_id.0.clone()).or_insert_with(|| {
                        builder.columns.markets.push(market.clone()); next
                    });
                    let admitted = builder.market_field(market_id,group_position,1)?;
                    builder.columns.market_admitted.push(admitted);
                    for asset in assets {
                        let asset_id = builder.asset(asset);
                        let identity = [market_id, asset_id, i64::from(asset.outcome.index)];
                        if !builder.columns.market_assets.contains(&identity) {
                            builder.columns.market_assets.push(identity);
                        }
                    }
                }
                if let crate::compact_public::CompactFactPayloadV1::Inline(
                    PublicBusinessPayloadV1::MarketFact {market,payload}
                ) = &fact.payload {
                    let id = *builder.markets.get(&market.condition_id.0).ok_or("公共市场字段缺FixedIdentity")?;
                    let q = quality_code(fact.quality.availability);
                    match payload {
                        CanonicalMarketFactPayloadV1::LifecycleTransition(value) => {
                            let state_id = i64::try_from(builder.columns.lifecycle_states.len())?;
                            builder.columns.lifecycle_states.push(value.current_state.clone());
                            let anchors = builder.anchor_set(&fact.direct_sources,&fact.position,&group.group_id)?;
                            let value = i64::try_from(builder.columns.lifecycle_values.len())?;
                            builder.columns.lifecycle_values.push([1,state_id,anchors,q]);
                            let row = builder.market_field(id,group_position,value)?;
                            builder.columns.market_lifecycle.push(row);
                        }
                        CanonicalMarketFactPayloadV1::MarketLifecycleSnapshot {current_state,..} => {
                            let state_id = i64::try_from(builder.columns.lifecycle_states.len())?;
                            builder.columns.lifecycle_states.push(current_state.clone());
                            let anchors = builder.anchor_set(&fact.direct_sources,&fact.position,&group.group_id)?;
                            let value = i64::try_from(builder.columns.lifecycle_values.len())?;
                            builder.columns.lifecycle_values.push([1,state_id,anchors,q]);
                            let row = builder.market_field(id,group_position,value)?;
                            builder.columns.market_lifecycle.push(row);
                        }
                        CanonicalMarketFactPayloadV1::TradingState(value) => {
                            let anchors = builder.anchor_set(&fact.direct_sources,&fact.position,&group.group_id)?;
                            let id_value = i64::try_from(builder.columns.trading_values.len())?;
                            let pairs = [value.active,value.closed,value.accepting_orders]
                                .map(|v|[i64::from(v.is_some()),i64::from(v.unwrap_or(false))]);
                            builder.columns.trading_values.push([pairs[0][0],pairs[0][1],pairs[1][0],pairs[1][1],
                                pairs[2][0],pairs[2][1],anchors,q]);
                            let row = builder.market_field(id,group_position,id_value)?;
                            builder.columns.market_trading.push(row);
                        }
                        CanonicalMarketFactPayloadV1::Terminal(MarketResolutionTerminalV3::Resolved {
                            winning_asset_id,payout_vector,..}) => {
                            let winner = builder.columns.market_assets.iter().find_map(|entry| {
                                (entry[0]==id && builder.columns.assets[entry[1] as usize].token_id.0==*winning_asset_id)
                                    .then_some(entry[1])
                            }).ok_or("公共终态赢家缺所属市场原资产")?;
                            let mut payouts = Vec::new();
                            for payout in payout_vector {
                                let a = builder.asset(&payout.asset);
                                if !builder.columns.market_assets.iter().any(|entry|entry[0]==id && entry[1]==a) {
                                    return Err("公共终态payout引用其它市场资产".into());
                                }
                                payouts.push([a,builder.decimal(&payout.payout_pusd_per_share)?]);
                            }
                            let payout_id = i64::try_from(builder.columns.terminal_payouts.len())?;
                            builder.columns.terminal_payouts.push(payouts);
                            let anchors = builder.anchor_set(&fact.direct_sources,&fact.position,&group.group_id)?;
                            let v = i64::try_from(builder.columns.terminal_values.len())?;
                            builder.columns.terminal_values.push([1,winner,payout_id,anchors,q]);
                            let row = builder.market_field(id,group_position,v)?;
                            builder.columns.market_terminal.push(row);builder.last_terminal.insert(id,v);
                        }
                        CanonicalMarketFactPayloadV1::QualityBoundary(boundary) => {
                            // 原公共apply_fact只改变terminal质量，并在非Fresh/Stale时清除值；锚点保留。
                            let previous = builder.last_terminal.get(&id).copied().unwrap_or(0) as usize;
                            let mut value = builder.columns.terminal_values[previous];
                            value[4] = quality_code(boundary.current_quality);
                            if value[4]>1 { value[0]=0; }
                            let v = i64::try_from(builder.columns.terminal_values.len())?;
                            builder.columns.terminal_values.push(value);
                            let row = builder.market_field(id,group_position,v)?;
                            builder.columns.market_terminal.push(row);builder.last_terminal.insert(id,v);
                        }
                        _=>{}
                    }
                }
                if let crate::compact_public::CompactFactPayloadV1::Inline(
                    PublicBusinessPayloadV1::MarketFact {payload:CanonicalMarketFactPayloadV1::InitialTick {
                        asset,tick_size_pusd_per_share},..}
                ) = &fact.payload {
                    let anchors = builder.anchor_set(&fact.direct_sources,&fact.position,&group.group_id)?;
                    let q = quality_code(fact.quality.availability);
                    builder.observed_tick(asset,tick_size_pusd_per_share,q<=1,anchors,q,
                        i64::try_from(group.group_position)?)?;
                }
            }
        }
        for pm in &packet.pm {
            let group = i64::try_from(packet.groups.iter()
                .find(|group| group.records.iter().any(|fact| fact.position == pm.position))
                .ok_or("设备PM步骤缺公共事实组关联")?.group_position)?;
            // 同事实内各价位Selection共享完全相同的直接源，不逐档复制其身份文本。
            let mut shared_anchors: Vec<(SourceEvidenceRefV3, i64)> = Vec::new();
            let group_id = packet.groups.iter().find(|g| g.group_position as i64 == group)
                .ok_or("设备PM观察字段缺原闭组")?.group_id.clone();
            let input_position = packet.groups.iter().flat_map(|g|&g.records).find(|fact|fact.position==pm.position)
                .and_then(|fact|match &fact.payload {
                    crate::compact_public::CompactFactPayloadV1::PmFromInput(header)=>Some(header.input_record.input_position),
                    _=>None,
                }).ok_or("设备PM字段缺原规范消息输入引用")?;
            for step in &pm.steps {
                let sequence = builder.columns.original_pm_steps;
                builder.columns.original_pm_steps = sequence.checked_add(1).ok_or("设备步骤位置溢出")?;
                // 只有模型直接选择步骤产生档位锚点；每步共享一次原证据。
                let has_anchor = matches!(&step.change,
                    CompactPmChangeV1::InitialSnapshotFromInput {..} |
                    CompactPmChangeV1::Inline(PmStepChangeV1::Selection {..}));
                let anchor = if has_anchor {
                    if let Some((_, id)) = shared_anchors.iter().find(|(direct, _)| direct == &step.direct) {
                        *id
                    } else {
                        let id = i64::try_from(builder.columns.anchors.len())?;
                        let group_id = packet.groups.iter().find(|g| g.group_position as i64 == group)
                            .ok_or("设备锚点缺原闭组")?.group_id.clone();
                        builder.columns.anchors.push(PublicLevelAnchorV1 {
                            direct:step.direct.clone(),position:pm.position.clone(),group_id,
                        });
                        shared_anchors.push((step.direct.clone(), id));
                        id
                    }
                } else { 0 };
                match &step.change {
                    CompactPmChangeV1::InitialSnapshotFromInput {input_record,asset,initial_tick_size_pusd_per_share,..} => {
                        let (token,bids,asks) = snapshots.get(&input_record.input_position).ok_or("设备初始盘口缺原输入快照")?;
                        if token != &asset.token_id.0 { return Err("设备快照引用资产不一致".into()); }
                        let a = builder.asset(asset);
                        builder.columns.resets.push([a,sequence,group,0]);
                        builder.columns.initialized.push([a,sequence,group,1]);
                        builder.columns.ready.push([a,sequence,group,1]);
                        builder.columns.book_quality.push([a,sequence,group,quality_code(step.quality.availability)]);
                        let tick = builder.decimal(initial_tick_size_pusd_per_share)?;
                        builder.columns.ticks.push([a,sequence,group,tick]);
                        for (side,levels) in [(BookSide::Bid,bids),(BookSide::Ask,asks)] {
                            for level in levels {
                                let price = normalize_decimal_text(&level.price,"book.price")?;
                                let quantity = normalize_decimal_text(&level.size,"book.size")?;
                                let slot = builder.level(asset,side, &price,&quantity,sequence,group)?;
                                builder.columns.selections.push([slot,sequence,group,1]);
                                builder.columns.level_anchors.push([slot,sequence,group,anchor]);
                            }
                        }
                    }
                    CompactPmChangeV1::Inline(change) => match change {
                        PmStepChangeV1::ResetAsset {asset,..} => {
                            let a = builder.asset(asset);
                            builder.columns.resets.push([a,sequence,group,0]);
                            builder.columns.ready.push([a,sequence,group,0]);
                        }
                        PmStepChangeV1::LevelChange {transition,..} => {
                            builder.level(&transition.asset,transition.side,&transition.price_pusd_per_share,
                                &transition.quantity_after_shares,sequence,group)?;
                        }
                        PmStepChangeV1::Selection {asset,side,price,selected,..} => {
                            let slot = builder.slot(asset,*side,price)?;
                            builder.columns.selections.push([slot,sequence,group,i64::from(selected.present)]);
                            builder.columns.level_anchors.push([slot,sequence,group,anchor]);
                        }
                        PmStepChangeV1::BookReady {asset} => {
                            let a = builder.asset(asset); builder.columns.ready.push([a,sequence,group,1]);
                            builder.columns.book_quality.push([a,sequence,group,0]);
                        }
                        PmStepChangeV1::Tick {tick,..} => {
                            let a = builder.asset(&tick.asset); let value = builder.decimal(&tick.new_tick_size_pusd_per_share)?;
                            builder.columns.ticks.push([a,sequence,group,value]);
                            let anchors = builder.anchor_set(std::slice::from_ref(&step.direct),&pm.position,&group_id)?;
                            builder.observed_tick(&tick.asset,&tick.new_tick_size_pusd_per_share,true,
                                anchors,quality_code(step.quality.availability),group)?;
                        }
                        PmStepChangeV1::BestBidAsk {quote,..} => {
                            let direct = raw_pm.get(&input_position).map(|row|
                                data_pipeline_core::pm_message::direct_bba_from_typed_raw_v1(row,step.member_index)).transpose()?.flatten();
                            let value = direct.unwrap_or_else(||CanonicalBbaFactV1 {
                                best_bid_pusd_per_share:quote.best_bid_pusd_per_share.clone(),
                                best_ask_pusd_per_share:quote.best_ask_pusd_per_share.clone(),spread_pusd_per_share:None,
                            });
                            let anchors = builder.anchor_set(std::slice::from_ref(&step.direct),&pm.position,&group_id)?;
                            builder.observed_bba(&quote.asset,&value,anchors,quality_code(step.quality.availability),group)?;
                        }
                        PmStepChangeV1::PublicConfirmation {asset,kind} => {
                            let a = builder.asset(asset);
                            match kind {
                                data_pipeline_core::pm_reduction::PmConfirmationComponentV1::Tick => {
                                    if let Some(&old) = builder.last_tick.get(&a) {
                                        let mut value = builder.columns.observed_tick_values[old as usize];
                                        if value[0] != 0 {
                                            value[2] = builder.anchor_set(std::slice::from_ref(&step.direct),&pm.position,&group_id)?;
                                            value[3] = quality_code(step.quality.availability);
                                            let id = i64::try_from(builder.columns.observed_tick_values.len())?;
                                            builder.columns.observed_tick_values.push(value);
                                            let row = builder.field_row(a,group,id)?;
                                            builder.columns.observed_ticks.push(row);
                                            builder.last_tick.insert(a,id);
                                        }
                                    }
                                }
                                data_pipeline_core::pm_reduction::PmConfirmationComponentV1::BestBidAsk => {
                                    let direct = raw_pm.get(&input_position).map(|row|
                                        data_pipeline_core::pm_message::direct_bba_from_typed_raw_v1(row,step.member_index)).transpose()?.flatten();
                                    let old = builder.last_bba.get(&a).map(|&old|builder.columns.bba_values[old as usize]);
                                    if let Some(value) = direct {
                                        let anchors = builder.anchor_set(std::slice::from_ref(&step.direct),&pm.position,&group_id)?;
                                        builder.observed_bba(asset,&value,anchors,quality_code(step.quality.availability),group)?;
                                    } else if let Some(mut value) = old {
                                        value[6] = builder.anchor_set(std::slice::from_ref(&step.direct),&pm.position,&group_id)?;
                                        value[7] = quality_code(step.quality.availability);
                                        let id = i64::try_from(builder.columns.bba_values.len())?;
                                        builder.columns.bba_values.push(value);
                                        let row = builder.field_row(a,group,id)?;
                                        builder.columns.bba.push(row);
                                        builder.last_bba.insert(a,id);
                                    }
                                }
                            }
                        }
                        PmStepChangeV1::PublicTrade {trade} => {
                            let a = builder.asset(&trade.asset);
                            let price = builder.decimal(&trade.price_pusd_per_share)?;
                            let quantity = builder.decimal(&trade.quantity_shares)?;
                            let [fee_present,fee] = builder.optional_decimal(&trade.fee_rate_bps)?;
                            let anchors = builder.anchor_set(std::slice::from_ref(&step.direct),&pm.position,&group_id)?;
                            let id = i64::try_from(builder.columns.trade_values.len())?;
                            builder.columns.trade_values.push([price,quantity,i64::from(trade.aggressor_side==TradeSide::Sell),
                                fee_present,fee,anchors,quality_code(step.quality.availability)]);
                            let row = builder.field_row(a,group,id)?;
                            builder.columns.trades.push(row);
                        }
                        PmStepChangeV1::Quality {boundary} => {
                            if let QualityScopeV1::Asset(asset) = &boundary.scope {
                                let a = builder.asset(asset);
                                builder.columns.book_quality.push([a,sequence,group,quality_code(boundary.current_quality)]);
                            }
                        }
                        _ => {} // 该列消费者只负责数量/选择/初始/ready/tick，其它原事实保留在compact。
                    }
                }
            }
        }
        for group in &packet.groups {
            for member in &group.member_input_positions { snapshots.remove(member); }
            for member in &group.member_input_positions { raw_pm.remove(member); pm_types.remove(member); }
        }
        progress(packet.input.input_position+1);
    }
    Ok(builder.columns)
}
