//! 研究私有格式：scope 字典一次写入，带校验 Zstandard 标量记录；不是公共录制合同。
use super::*;
use std::io::{Read, Write};
pub const SELECT: u32 = u32::MAX;
const MAGIC: &[u8; 8] = b"HEALTH01";
const SIZE: usize = 34;
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Scope {
    pub scope: SourceStreamScopeV1,
    pub window: Option<MarketWindowV2>,
}
#[derive(Serialize, Deserialize)]
pub struct Material {
    pub format: String,
    pub start: i64,
    pub end: i64,
    pub selection: String,
    pub scopes: Vec<Scope>,
    pub records: u64,
}
#[derive(Clone, Copy, Debug)]
pub struct Row {
    pub at: i64,
    pub scope: u32,
    pub received: i64,
    pub event: i64,
    pub flags: u8,
    pub samples: u32,
    pub selected: u8,
}
impl Row {
    pub fn facts(self) -> Option<SourceHealthFactsV1> {
        if self.flags & 1 == 0 {
            return None;
        }
        Some(SourceHealthFactsV1 {
            synchronization_required: self.flags & 2 != 0,
            availability: if self.flags & 4 != 0 {
                MarketDataQuality::Fresh
            } else {
                MarketDataQuality::Missing
            },
            last_valid_message_times: (self.flags & 8 != 0)
                .then_some((self.received, (self.flags & 16 != 0).then_some(self.event))),
        })
    }
    pub fn from_progress(at: i64, scope: u32, p: &SourceStreamProgressV1, new_valid: bool) -> Self {
        let f = SourceHealthFactsV1::from(p);
        let mut flags = 1;
        if p.synchronization == SourceSynchronizationV1::Unknown {
            flags |= 32;
        }
        if p.connection_generation.is_none() {
            flags |= 64;
        }
        if f.synchronization_required {
            flags |= 2;
        }
        if matches!(
            f.availability,
            MarketDataQuality::Fresh | MarketDataQuality::Stale
        ) {
            flags |= 4;
        }
        let (received, event) = f.last_valid_message_times.unwrap_or((0, None));
        if f.last_valid_message_times.is_some() {
            flags |= 8;
        }
        if event.is_some() {
            flags |= 16;
        }
        Self {
            at,
            scope,
            received,
            event: event.unwrap_or(0),
            flags,
            samples: u32::from(new_valid),
            selected: 0,
        }
    }
}
pub struct Writer {
    encoder: zstd::stream::write::Encoder<'static, BufWriter<File>>,
    pub records: u64,
}
impl Writer {
    pub fn new(path: &Path) -> Result<Self> {
        let mut encoder =
            zstd::stream::write::Encoder::new(BufWriter::new(File::create(path)?), 3)?;
        encoder.include_checksum(true)?;
        encoder.write_all(MAGIC)?;
        Ok(Self {
            encoder,
            records: 0,
        })
    }
    pub fn row(&mut self, r: Row) -> Result<()> {
        let mut b = [0u8; SIZE];
        b[0..8].copy_from_slice(&r.at.to_le_bytes());
        b[8..12].copy_from_slice(&r.scope.to_le_bytes());
        b[12..20].copy_from_slice(&r.received.to_le_bytes());
        b[20..28].copy_from_slice(&r.event.to_le_bytes());
        b[28] = r.flags;
        b[29..33].copy_from_slice(&r.samples.to_le_bytes());
        b[33] = r.selected;
        self.encoder.write_all(&b)?;
        self.records += 1;
        Ok(())
    }
    pub fn finish(self) -> Result<()> {
        self.encoder.finish()?.flush()?;
        Ok(())
    }
}
pub fn read(path: &Path, m: &Material, callback: impl FnMut(Row) -> Result<()>) -> Result<()> {
    read_until(path, m, None, callback)
}
pub fn read_until(
    path: &Path,
    m: &Material,
    until: Option<i64>,
    mut callback: impl FnMut(Row) -> Result<()>,
) -> Result<()> {
    if m.format != "health-scalars-zstd-v1" || m.start >= m.end {
        return Err("invalid scalar material header".into());
    }
    let mut decoder = zstd::stream::read::Decoder::new(File::open(path)?)?;
    let mut magic = [0; 8];
    decoder.read_exact(&mut magic)?;
    if &magic != MAGIC {
        return Err("invalid scalar stream magic".into());
    }
    let mut count = 0;
    let mut previous = i64::MIN;
    loop {
        let mut b = [0u8; SIZE];
        if decoder.read(&mut b[..1])? == 0 {
            break;
        }
        decoder.read_exact(&mut b[1..])?;
        let r = Row {
            at: i64::from_le_bytes(b[..8].try_into()?),
            scope: u32::from_le_bytes(b[8..12].try_into()?),
            received: i64::from_le_bytes(b[12..20].try_into()?),
            event: i64::from_le_bytes(b[20..28].try_into()?),
            flags: b[28],
            samples: u32::from_le_bytes(b[29..33].try_into()?),
            selected: b[33],
        };
        if r.at < previous || (r.scope != SELECT && r.scope as usize >= m.scopes.len()) {
            return Err("invalid scalar order or scope reference".into());
        }
        if until.is_some_and(|end| r.at >= end) {
            return Ok(());
        }
        previous = r.at;
        count += 1;
        callback(r)?;
    }
    if count != m.records {
        return Err("incomplete scalar record count".into());
    }
    Ok(())
}
