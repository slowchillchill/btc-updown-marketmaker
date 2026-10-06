//! 紧凑公共PM步骤到共享设备列。只转已有事实，不重新计算public/hidden归约。
use crate::{compact_public::{CompactPmChangeV1, PublicCompactReaderV1}, public_recording::RecordingResult};
use contracts::*;
use data_pipeline_core::{pm_reduction::PmStepChangeV1, public_input::{DptIngressV1, TypedRawRowV2},
    typed_raw::{TypedRawRow, PolymarketOrderbookEvent, normalize_decimal_text}};
use serde::Serialize;
use std::collections::BTreeMap;

/// 所有序号为i64设备坐标；各更新列为[目标,原步骤序号,公共组序号,值]。
/// 原身份/精确词法/缺失及其它事实继续由绑定的compact材料拥有。
#[derive(Default, Serialize)]
pub struct PublicPmDeviceColumnsV1 {
    pub version: &'static str,
    pub input_stream_id: String,
    pub assets: Vec<AssetIdentityV1>,
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
    pub original_pm_steps: i64,
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
struct Builder {
    columns: PublicPmDeviceColumnsV1,
    assets: BTreeMap<String,i64>, decimals: BTreeMap<String,i64>, slots: BTreeMap<(i64,i64,i64),i64>,
}
impl Builder {
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
        ..Default::default()
    }, assets:BTreeMap::new(), decimals:BTreeMap::new(), slots:BTreeMap::new() };
    // 零值占位不是已知零；设备查询另带initialized/selected/present。
    builder.decimal(&ExactDecimal("0".into()))?;
    let mut snapshots = BTreeMap::new();
    while let Some(packet) = reader.next_packet()? {
        if let DptIngressV1::Message { row:TypedRawRowV2::Existing(TypedRawRow::PolymarketOrderbook(row)), .. } = &packet.input.ingress {
            if let PolymarketOrderbookEvent::Book {asset_id,bids,asks,..} = &row.event {
                snapshots.insert(packet.input.input_position,(asset_id.clone(),bids.clone(),asks.clone()));
            }
        }
        for group in &packet.groups {
            builder.columns.groups.push([i64::try_from(group.group_position)?,i64::try_from(group.closed_at_input_position)?,group.visible_at_utc_ms]);
        }
        for pm in &packet.pm {
            let group = i64::try_from(packet.groups.iter()
                .find(|group| group.records.iter().any(|fact| fact.position == pm.position))
                .ok_or("设备PM步骤缺公共事实组关联")?.group_position)?;
            for step in &pm.steps {
                let sequence = builder.columns.original_pm_steps;
                builder.columns.original_pm_steps = sequence.checked_add(1).ok_or("设备步骤位置溢出")?;
                // 只有模型直接选择步骤产生档位锚点；每步共享一次原证据。
                let anchor = i64::try_from(builder.columns.anchors.len())?;
                let has_anchor = matches!(&step.change,
                    CompactPmChangeV1::InitialSnapshotFromInput {..} |
                    CompactPmChangeV1::Inline(PmStepChangeV1::Selection {..}));
                if has_anchor {
                    let group_id = packet.groups.iter().find(|g| g.group_position as i64 == group)
                        .ok_or("设备锚点缺原闭组")?.group_id.clone();
                    builder.columns.anchors.push(PublicLevelAnchorV1 {
                        direct:step.direct.clone(),position:pm.position.clone(),group_id,
                    });
                }
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
        }
        progress(packet.input.input_position+1);
    }
    Ok(builder.columns)
}
