//! 同一封存场景的已校验文件句柄与有界共享解码。
use super::{
    CheckedSceneEvents, OfflineSceneMaterialV1, SceneEventReader, StoredSceneV1, StreamManifestV2,
};
use crate::MaterialError;
use contracts::Sha256Hex;
use sim_system::offline_scene::{Event, EventKind, Level, PreparedScene};
use std::{
    collections::VecDeque,
    fs::File,
    io::Read,
    os::unix::fs::{FileExt, MetadataExt},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};

type FileStamp = (u64, u64, u64, i64, i64, i64, i64);
fn stamp(file: &File) -> std::io::Result<FileStamp> {
    let m = file.metadata()?;
    Ok((
        m.dev(),
        m.ino(),
        m.len(),
        m.mtime(),
        m.mtime_nsec(),
        m.ctime(),
        m.ctime_nsec(),
    ))
}
struct PinnedFile {
    file: Arc<File>,
    stamp: Mutex<FileStamp>,
    sha256: String,
    path: PathBuf,
    interval: u64,
}
impl PinnedFile {
    fn verify(path: &Path, interval: u64) -> Result<Arc<Self>, MaterialError> {
        let file = Arc::new(File::open(path)?);
        let before = stamp(&file)?;
        let sha256 = super::file_sha256_reader_with_progress(
            path,
            PositionedReader {
                file: file.clone(),
                offset: 0,
            },
            before.2,
            interval,
        )?;
        let pinned = Arc::new(Self {
            file,
            stamp: Mutex::new(before),
            sha256,
            path: path.to_owned(),
            interval,
        });
        pinned.check()?;
        Ok(pinned)
    }
    fn check(&self) -> std::io::Result<()> {
        let mut recorded = self
            .stamp
            .lock()
            .map_err(|_| std::io::Error::other("场景文件校验状态不可用"))?;
        let before = stamp(&self.file)?;
        if before != *recorded {
            // stat是重验线索。权限、mtime等元数据变化且内容相同时仍接受。
            let digest = super::file_sha256_reader_with_progress(
                &self.path,
                PositionedReader {
                    file: self.file.clone(),
                    offset: 0,
                },
                before.2,
                self.interval,
            )
            .map_err(|e| std::io::Error::other(e.to_string()))?;
            if stamp(&self.file)? != before {
                return Err(std::io::Error::other(
                    "场景重验期间文件仍在变化，无法确认原内容身份",
                ));
            }
            if digest != self.sha256 {
                return Err(std::io::Error::other("已校验场景内容发生变化"));
            }
            *recorded = before;
        }
        Ok(())
    }
    fn reader(self: &Arc<Self>) -> PinnedReader {
        PinnedReader {
            file: self.clone(),
            offset: 0,
        }
    }
    fn matches_path(&self, path: &Path) -> std::io::Result<bool> {
        self.check()?;
        let current = stamp(&File::open(path)?)?;
        let recorded = *self
            .stamp
            .lock()
            .map_err(|_| std::io::Error::other("场景文件校验状态不可用"))?;
        Ok(current == recorded)
    }
}
struct PositionedReader {
    file: Arc<File>,
    offset: u64,
}
impl Read for PositionedReader {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let n = self.file.read_at(buffer, self.offset)?;
        self.offset += n as u64;
        Ok(n)
    }
}
struct PinnedReader {
    file: Arc<PinnedFile>,
    offset: u64,
}
impl Read for PinnedReader {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        self.file.check()?;
        let n = self.file.file.read_at(buffer, self.offset)?;
        self.file.check()?;
        self.offset += n as u64;
        Ok(n)
    }
}
struct CompactEvents {
    levels: Vec<Level>,
    events: Vec<(i64, u64, usize, EventKind)>,
}
struct CompactReader {
    data: Arc<CompactEvents>,
    position: usize,
}
impl Iterator for CompactReader {
    type Item = Result<Event, MaterialError>;
    fn next(&mut self) -> Option<Self::Item> {
        let (at_ms, source_sequence, level, kind) = self.data.events.get(self.position)?;
        self.position += 1;
        Some(
            self.data
                .levels
                .get(*level)
                .ok_or_else(|| "场景价位引用不存在".into())
                .map(|level| Event {
                    at_ms: *at_ms,
                    source_sequence: *source_sequence,
                    level: level.clone(),
                    kind: kind.clone(),
                }),
        )
    }
}
enum SceneStorage {
    Stream {
        payload: Arc<PinnedFile>,
        relative: PathBuf,
        event_count: u64,
    },
    Compact(Arc<CompactEvents>),
}
/// 保留校验过的打开对象。调用方须保持封存文件不变；读取块及重新绑定时检查失效。
pub struct VerifiedOfflineSceneV1 {
    material: Arc<OfflineSceneMaterialV1>,
    identity: Sha256Hex,
    manifest: Arc<PinnedFile>,
    storage: SceneStorage,
}
impl std::fmt::Debug for VerifiedOfflineSceneV1 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VerifiedOfflineSceneV1")
            .field("identity", &self.identity)
            .finish_non_exhaustive()
    }
}
impl VerifiedOfflineSceneV1 {
    pub fn material(&self) -> Arc<OfflineSceneMaterialV1> {
        self.material.clone()
    }
    pub fn identity(&self) -> &Sha256Hex {
        &self.identity
    }
    fn check(&self) -> std::io::Result<()> {
        self.manifest.check()?;
        if let SceneStorage::Stream { payload, .. } = &self.storage {
            payload.check()?;
        }
        Ok(())
    }
    /// 仍检查候选实际配置的头文件和载荷存在，并绑定到原已校验对象。
    pub fn matches_path(&self, path: &Path) -> Result<bool, MaterialError> {
        if !self.manifest.matches_path(path)? {
            return Ok(false);
        }
        if let SceneStorage::Stream {
            payload, relative, ..
        } = &self.storage
        {
            return Ok(
                payload.matches_path(&path.parent().ok_or("离线场景没有父目录")?.join(relative))?
            );
        }
        Ok(true)
    }
    pub fn independent_reader(&self) -> Result<SceneEventReader, MaterialError> {
        self.check()?;
        match &self.storage {
            SceneStorage::Stream {
                payload,
                event_count,
                ..
            } => {
                let decoder = zstd::stream::read::Decoder::new(payload.reader())?;
                let inner = serde_json::Deserializer::from_reader(std::io::BufReader::new(decoder))
                    .into_iter::<Event>();
                Ok(Box::new(CheckedSceneEvents {
                    inner,
                    remaining: *event_count,
                    last_at: None,
                    ended: false,
                }))
            }
            SceneStorage::Compact(data) => Ok(Box::new(CompactReader {
                data: data.clone(),
                position: 0,
            })),
        }
    }
}
/// 验证封存场景一次，后续从相同文件对象读取，不重新按路径打开解码源。
pub fn verify_scene_material_with_progress(
    path: &Path,
    interval_seconds: u64,
) -> Result<Arc<VerifiedOfflineSceneV1>, MaterialError> {
    let manifest_file = PinnedFile::verify(path, interval_seconds)?;
    let identity = Sha256Hex(manifest_file.sha256.clone());
    let (material, storage) = if path.extension().is_some_and(|e| e == "scene") {
        let manifest: StreamManifestV2 =
            serde_json::from_reader(std::io::BufReader::new(manifest_file.reader()))?;
        if manifest.storage_version != "OFFLINE_SCENE_JSONL_ZSTD_V2"
            || !manifest.material.scene.events.is_empty()
        {
            return Err("离线场景存储版本或头不符".into());
        }
        let relative = PathBuf::from(&manifest.event_file);
        if relative.components().count() != 1
            || !matches!(
                relative.components().next(),
                Some(std::path::Component::Normal(_))
            )
        {
            return Err("离线场景载荷路径越界".into());
        }
        let payload_path = path.parent().ok_or("离线场景没有父目录")?.join(&relative);
        let payload = PinnedFile::verify(&payload_path, interval_seconds)?;
        if payload.sha256 != manifest.event_sha256 {
            return Err("离线场景载荷SHA256不符".into());
        }
        (
            manifest.material,
            SceneStorage::Stream {
                payload,
                relative,
                event_count: manifest.event_count,
            },
        )
    } else {
        let stored: StoredSceneV1 =
            serde_json::from_reader(std::io::BufReader::new(manifest_file.reader()))?;
        (
            OfflineSceneMaterialV1 {
                scene: PreparedScene {
                    model_version: stored.model_version,
                    parameters: stored.parameters,
                    events: Vec::new(),
                    report: stored.report,
                },
                markets: stored.markets,
                market_assets: stored.market_assets,
                source_manifests: stored.source_manifests,
            },
            SceneStorage::Compact(Arc::new(CompactEvents {
                levels: stored.levels,
                events: stored.events,
            })),
        )
    };
    manifest_file.check()?;
    Ok(Arc::new(VerifiedOfflineSceneV1 {
        material: Arc::new(material),
        identity,
        manifest: manifest_file,
        storage,
    }))
}

struct CachedEvent {
    event: Arc<Event>,
    bytes: usize,
}
struct SharedState {
    reader: SceneEventReader,
    cache: VecDeque<CachedEvent>,
    base: u64,
    next: u64,
    payload_bytes: usize,
    peak_bytes: usize,
    decoded_bytes: u64,
    ended: bool,
    failure: Option<String>,
}
impl SharedState {
    fn cache_bytes(&self) -> usize {
        self.payload_bytes + self.cache.capacity() * std::mem::size_of::<CachedEvent>()
    }
    fn retain(&mut self, event: Arc<Event>, limit: usize) {
        let bytes = event_bytes(&event) + 2 * std::mem::size_of::<usize>();
        let slot = std::mem::size_of::<CachedEvent>();
        if bytes + slot > limit {
            self.cache = VecDeque::new();
            self.payload_bytes = 0;
            self.base = self.next;
            return;
        }
        // 先释放前缀，再扩容/放入新事件。预算含VecDeque已分配的槽位。
        loop {
            let capacity = self.cache.capacity();
            let next_capacity = if self.cache.len() == capacity {
                if capacity == 0 {
                    1
                } else {
                    capacity.saturating_mul(2)
                }
            } else {
                capacity
            };
            if self.payload_bytes + bytes + next_capacity * slot <= limit {
                break;
            }
            if let Some(first) = self.cache.pop_front() {
                self.payload_bytes -= first.bytes;
                self.base += 1;
            } else {
                self.cache = VecDeque::new();
                break;
            }
        }
        if self.cache.is_empty() {
            self.base = self.next - 1;
        }
        if self.cache.len() == self.cache.capacity() {
            self.cache.reserve_exact(self.cache.capacity().max(1));
        }
        self.payload_bytes += bytes;
        self.cache.push_back(CachedEvent { event, bytes });
        self.peak_bytes = self.peak_bytes.max(self.cache_bytes());
    }
}
fn event_bytes(event: &Event) -> usize {
    std::mem::size_of::<Event>()
        + event.level.condition.capacity()
        + event.level.buy_token.capacity()
        + match &event.kind {
            EventKind::Cancel { targets, .. } => {
                targets.capacity() * std::mem::size_of::<sim_system::offline_scene::Slice>()
            }
            _ => 0,
        }
}
struct SharedInner {
    verified: Arc<VerifiedOfflineSceneV1>,
    state: Mutex<SharedState>,
    limit: usize,
    active: AtomicUsize,
    fallback_readers: AtomicU64,
    fallback_decoded: AtomicU64,
    fallback_skipped: AtomicU64,
}
/// 同一作业的共享解码源。缓存淘汰从不等待账户，避免与公共供料背压互锁。
#[derive(Clone)]
pub struct SharedSceneEventsV1 {
    inner: Arc<SharedInner>,
}
#[derive(Clone, Debug, serde::Serialize)]
pub struct SharedSceneDiagnosticsV1 {
    pub primary_decoded_events: u64,
    pub primary_decoded_payload_bytes: u64,
    pub cache_limit_bytes: usize,
    pub logical_cache_bytes: usize,
    pub peak_logical_cache_bytes: usize,
    pub active_readers: usize,
    pub fallback_readers: u64,
    pub fallback_decoded_events: u64,
    pub fallback_skipped_events: u64,
}
impl SharedSceneEventsV1 {
    pub fn new(
        verified: Arc<VerifiedOfflineSceneV1>,
        cache_limit_bytes: usize,
    ) -> Result<Self, MaterialError> {
        let reader = verified.independent_reader()?;
        Ok(Self {
            inner: Arc::new(SharedInner {
                verified,
                state: Mutex::new(SharedState {
                    reader,
                    cache: VecDeque::new(),
                    base: 0,
                    next: 0,
                    payload_bytes: 0,
                    peak_bytes: 0,
                    decoded_bytes: 0,
                    ended: false,
                    failure: None,
                }),
                limit: cache_limit_bytes,
                active: AtomicUsize::new(0),
                fallback_readers: AtomicU64::new(0),
                fallback_decoded: AtomicU64::new(0),
                fallback_skipped: AtomicU64::new(0),
            }),
        })
    }
    pub fn reader(&self) -> Result<SceneEventReader, MaterialError> {
        if let Some(failure) = &self
            .inner
            .state
            .lock()
            .map_err(|_| "共享场景解码状态不可用")?
            .failure
        {
            return Err(failure.clone().into());
        }
        if let Err(error) = self.inner.verified.check() {
            let message = error.to_string();
            if let Ok(mut state) = self.inner.state.lock() {
                state.failure.get_or_insert(message.clone());
            }
            return Err(message.into());
        }
        self.inner.active.fetch_add(1, Ordering::Relaxed);
        Ok(Box::new(SharedCursor {
            inner: self.inner.clone(),
            position: 0,
            fallback: None,
            finished: false,
        }))
    }
    pub fn diagnostics(&self) -> Result<SharedSceneDiagnosticsV1, MaterialError> {
        let state = self
            .inner
            .state
            .lock()
            .map_err(|_| "共享场景解码状态不可用")?;
        Ok(SharedSceneDiagnosticsV1 {
            primary_decoded_events: state.next,
            primary_decoded_payload_bytes: state.decoded_bytes,
            cache_limit_bytes: self.inner.limit,
            logical_cache_bytes: state.cache_bytes(),
            peak_logical_cache_bytes: state.peak_bytes,
            active_readers: self.inner.active.load(Ordering::Relaxed),
            fallback_readers: self.inner.fallback_readers.load(Ordering::Relaxed),
            fallback_decoded_events: self.inner.fallback_decoded.load(Ordering::Relaxed),
            fallback_skipped_events: self.inner.fallback_skipped.load(Ordering::Relaxed),
        })
    }
}
struct SharedCursor {
    inner: Arc<SharedInner>,
    position: u64,
    fallback: Option<SceneEventReader>,
    finished: bool,
}
impl SharedCursor {
    fn fail(&mut self, message: String) -> Option<Result<Event, MaterialError>> {
        if let Ok(mut state) = self.inner.state.lock() {
            state.failure.get_or_insert(message.clone());
        }
        self.finished = true;
        Some(Err(message.into()))
    }
    fn start_fallback(&mut self) -> Result<(), MaterialError> {
        self.inner.fallback_readers.fetch_add(1, Ordering::Relaxed);
        let mut reader = self.inner.verified.independent_reader()?;
        for skipped in 0..self.position {
            if skipped.is_multiple_of(1024) {
                let state = self
                    .inner
                    .state
                    .lock()
                    .map_err(|_| "共享场景解码状态不可用")?;
                if let Some(failure) = &state.failure {
                    return Err(failure.clone().into());
                }
            }
            reader.next().ok_or("场景顺序补读提前结束")??;
            self.inner.fallback_decoded.fetch_add(1, Ordering::Relaxed);
            self.inner.fallback_skipped.fetch_add(1, Ordering::Relaxed);
        }
        self.fallback = Some(reader);
        Ok(())
    }
}
impl Iterator for SharedCursor {
    type Item = Result<Event, MaterialError>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        let mut state = match self.inner.state.lock() {
            Ok(s) => s,
            Err(_) => {
                self.finished = true;
                return Some(Err("共享场景解码状态不可用".into()));
            }
        };
        if let Some(failure) = &state.failure {
            self.finished = true;
            return Some(Err(failure.clone().into()));
        }
        if self.fallback.is_some() || self.position < state.base {
            drop(state);
            if self.fallback.is_none() {
                if let Err(e) = self.start_fallback() {
                    return self.fail(e.to_string());
                }
            }
            match self.fallback.as_mut().unwrap().next() {
                Some(Ok(event)) => {
                    self.position += 1;
                    self.inner.fallback_decoded.fetch_add(1, Ordering::Relaxed);
                    Some(Ok(event))
                }
                Some(Err(e)) => self.fail(e.to_string()),
                None => {
                    self.finished = true;
                    None
                }
            }
        } else {
            let event = if self.position < state.next {
                state.cache[(self.position - state.base) as usize]
                    .event
                    .clone()
            } else if state.ended {
                self.finished = true;
                return None;
            } else {
                match state.reader.next() {
                    Some(Ok(event)) => {
                        state.next += 1;
                        state.decoded_bytes += event_bytes(&event) as u64;
                        let event = Arc::new(event);
                        state.retain(event.clone(), self.inner.limit);
                        event
                    }
                    Some(Err(e)) => {
                        let message = e.to_string();
                        state.failure = Some(message.clone());
                        self.finished = true;
                        return Some(Err(message.into()));
                    }
                    None => {
                        state.ended = true;
                        self.finished = true;
                        return None;
                    }
                }
            };
            self.position += 1;
            drop(state);
            Some(Ok((*event).clone()))
        }
    }
}
impl Drop for SharedCursor {
    fn drop(&mut self) {
        if self.inner.active.fetch_sub(1, Ordering::Relaxed) == 1 {
            if let Ok(mut state) = self.inner.state.lock() {
                state.cache = VecDeque::new();
                state.payload_bytes = 0;
                state.base = state.next;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sim_system::offline_scene::{
        Allocation, Cancellation, EventKind, InternalTime, MODEL_VERSION, Parameters,
    };
    fn events(count: usize) -> Vec<Event> {
        (0..count)
            .map(|i| Event {
                at_ms: i as i64,
                source_sequence: i as u64,
                level: Level {
                    condition: format!("0x{}", "1".repeat(64)),
                    buy_token: "20".into(),
                    price_micro: 500_000,
                },
                kind: EventKind::Add {
                    segment: i as u64,
                    quantity: 10,
                    implicit: false,
                },
            })
            .collect()
    }
    fn material(events: Vec<Event>) -> OfflineSceneMaterialV1 {
        OfflineSceneMaterialV1 {
            scene: PreparedScene {
                model_version: MODEL_VERSION.into(),
                parameters: Parameters {
                    association_ms: 250,
                    allocation: Allocation::Nearest,
                    cancellation: Cancellation::SegmentProportionalV2,
                    internal_time: InternalTime::SourceWhenPresent,
                    seed: 7,
                    association_trade_shift_ms: 0,
                },
                events,
                report: Default::default(),
            },
            markets: Default::default(),
            market_assets: Default::default(),
            source_manifests: vec![("2026-11-04".into(), "a".repeat(64))],
        }
    }
    fn scene(path: &Path, events: &[Event]) {
        let mut writer = super::super::SceneStreamWriterV2::create(path).unwrap();
        for event in events {
            writer.push(event.clone()).unwrap();
        }
        writer.finish(material(Vec::new())).unwrap();
    }
    #[test]
    fn three_independent_cursors_decode_once_and_release_cache_after_exit() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.scene");
        let expected = events(128);
        scene(&path, &expected);
        let verified = verify_scene_material_with_progress(&path, 0).unwrap();
        let source = SharedSceneEventsV1::new(verified, 1 << 20).unwrap();
        let readers = (0..3).map(|_| source.reader().unwrap()).collect::<Vec<_>>();
        for reader in readers {
            assert_eq!(reader.collect::<Result<Vec<_>, _>>().unwrap(), expected);
        }
        let d = source.diagnostics().unwrap();
        assert_eq!(d.primary_decoded_events, 128);
        assert_eq!(d.fallback_readers, 0);
        assert_eq!(d.active_readers, 0);
        assert_eq!(d.logical_cache_bytes, 0);
        assert!(d.peak_logical_cache_bytes <= d.cache_limit_bytes);
    }
    #[test]
    fn slow_cursor_switches_once_to_continuous_reader_without_blocking_fast_or_exit() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.scene");
        let expected = events(512);
        scene(&path, &expected);
        let source =
            SharedSceneEventsV1::new(verify_scene_material_with_progress(&path, 0).unwrap(), 2048)
                .unwrap();
        let fast = source.reader().unwrap();
        let mut slow = source.reader().unwrap();
        let early_exit = source.reader().unwrap();
        assert_eq!(slow.next().unwrap().unwrap(), expected[0]);
        drop(early_exit);
        assert_eq!(fast.collect::<Result<Vec<_>, _>>().unwrap(), expected);
        assert_eq!(slow.collect::<Result<Vec<_>, _>>().unwrap(), expected[1..]);
        let d = source.diagnostics().unwrap();
        assert_eq!(d.primary_decoded_events, 512);
        assert_eq!(d.fallback_readers, 1);
        assert_eq!(d.fallback_decoded_events, 512);
        assert_eq!(d.fallback_skipped_events, 1);
        assert_eq!(d.active_readers, 0);
        assert!(d.peak_logical_cache_bytes <= 2048);
    }
    #[test]
    fn compact_format_and_zero_cache_keep_complete_original_sequence() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("legacy.json");
        let expected = events(32);
        material(expected.clone()).write_compact(&path).unwrap();
        let verified = verify_scene_material_with_progress(&path, 0).unwrap();
        let independent = verified
            .independent_reader()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(independent, expected);
        let source = SharedSceneEventsV1::new(verified, 0).unwrap();
        let first = source.reader().unwrap();
        let second = source.reader().unwrap();
        assert_eq!(first.collect::<Result<Vec<_>, _>>().unwrap(), expected);
        assert_eq!(second.collect::<Result<Vec<_>, _>>().unwrap(), expected);
        assert_eq!(source.diagnostics().unwrap().peak_logical_cache_bytes, 0);
    }
    #[test]
    fn event_larger_than_cache_budget_is_delivered_completely() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("large.scene");
        let mut expected = events(4096);
        let level = expected[0].level.clone();
        expected.push(Event {
            at_ms: 4096,
            source_sequence: 4096,
            level,
            kind: EventKind::Cancel {
                targets: (0..4096)
                    .map(|segment| sim_system::offline_scene::Slice {
                        segment,
                        start: 0,
                        end: 1,
                    })
                    .collect(),
                recovery: false,
            },
        });
        scene(&path, &expected);
        let source =
            SharedSceneEventsV1::new(verify_scene_material_with_progress(&path, 0).unwrap(), 1024)
                .unwrap();
        let fast = source.reader().unwrap();
        let slow = source.reader().unwrap();
        assert_eq!(fast.collect::<Result<Vec<_>, _>>().unwrap(), expected);
        assert_eq!(slow.collect::<Result<Vec<_>, _>>().unwrap(), expected);
        let d = source.diagnostics().unwrap();
        assert_eq!(d.primary_decoded_events, 4097);
        assert!(d.peak_logical_cache_bytes <= 1024);
        assert_eq!(d.active_readers, 0);
    }
    #[test]
    fn metadata_change_revalidates_identical_content_and_bad_content_fails_all_readers() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.scene");
        scene(&path, &events(16));
        let verified = verify_scene_material_with_progress(&path, 0).unwrap();
        let original = verified.identity().clone();
        File::open(&path)
            .unwrap()
            .set_modified(std::time::SystemTime::now() + std::time::Duration::from_secs(2))
            .unwrap();
        assert!(verified.matches_path(&path).unwrap());
        assert_eq!(verified.identity(), &original);
        let source = SharedSceneEventsV1::new(verified, 1 << 20).unwrap();
        let mut existing = source.reader().unwrap();
        let manifest: StreamManifestV2 =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        std::fs::write(dir.path().join(manifest.event_file), b"corrupt").unwrap();
        std::thread::scope(|scope| {
            let workers = (0..3)
                .map(|_| scope.spawn(|| source.reader().is_err()))
                .collect::<Vec<_>>();
            for worker in workers {
                assert!(worker.join().unwrap());
            }
        });
        assert!(existing.next().unwrap().is_err());
        assert!(existing.next().is_none());
    }
    #[test]
    fn semantic_decode_failure_propagates_to_other_cursor_before_cached_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.scene");
        scene(&path, &events(2));
        let mut manifest: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        manifest["event_count"] = serde_json::json!(3);
        std::fs::write(&path, serde_json::to_vec(&manifest).unwrap()).unwrap();
        let source = SharedSceneEventsV1::new(
            verify_scene_material_with_progress(&path, 0).unwrap(),
            1 << 20,
        )
        .unwrap();
        let mut fast = source.reader().unwrap();
        let mut slow = source.reader().unwrap();
        assert!(fast.next().unwrap().is_ok());
        assert!(fast.next().unwrap().is_ok());
        let failure = fast.next().unwrap().unwrap_err().to_string();
        assert_eq!(slow.next().unwrap().unwrap_err().to_string(), failure);
    }
}

#[cfg(test)]
mod cost_tests {
    use super::*;
    #[test]
    #[ignore = "explicit #392 bounded shared scene decoding measurement"]
    fn shared_scene_decoding_three_readers_cost() {
        use sim_system::offline_scene::{
            Allocation, Cancellation, InternalTime, MODEL_VERSION, Parameters, Slice,
        };
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cost.scene");
        let level = Level {
            condition: format!("0x{}", "1".repeat(64)),
            buy_token: "20".into(),
            price_micro: 500_000,
        };
        let mut expected = (0..4096)
            .map(|i| Event {
                at_ms: i,
                source_sequence: i as u64,
                level: level.clone(),
                kind: EventKind::Add {
                    segment: i as u64,
                    quantity: 1000,
                    implicit: false,
                },
            })
            .collect::<Vec<_>>();
        for i in 0..15904 {
            let position = i * 32;
            let round = position / 4096;
            let first = position % 4096;
            expected.push(Event {
                at_ms: 4096 + i as i64,
                source_sequence: 4096 + i as u64,
                level: level.clone(),
                kind: EventKind::Cancel {
                    targets: (0..32)
                        .map(|offset| Slice {
                            segment: (first + offset) as u64,
                            start: round as u128,
                            end: round as u128 + 1,
                        })
                        .collect(),
                    recovery: false,
                },
            });
        }
        let mut writer = super::super::SceneStreamWriterV2::create(&path).unwrap();
        for event in &expected {
            writer.push(event.clone()).unwrap();
        }
        writer
            .finish(OfflineSceneMaterialV1 {
                scene: PreparedScene {
                    model_version: MODEL_VERSION.into(),
                    parameters: Parameters {
                        association_ms: 250,
                        allocation: Allocation::Nearest,
                        cancellation: Cancellation::SegmentProportionalV2,
                        internal_time: InternalTime::SourceWhenPresent,
                        seed: 7,
                        association_trade_shift_ms: 0,
                    },
                    events: Vec::new(),
                    report: Default::default(),
                },
                markets: Default::default(),
                market_assets: Default::default(),
                source_manifests: Vec::new(),
            })
            .unwrap();
        let verified = verify_scene_material_with_progress(&path, 0).unwrap();
        let hz = crate::process_clock_ticks_per_second_v1();
        for trial in 0..3 {
            let modes = if trial % 2 == 0 {
                [false, true]
            } else {
                [true, false]
            };
            for shared in modes {
                let before = crate::ProcessCountersV1::read().ok();
                let started = std::time::Instant::now();
                let factory =
                    shared.then(|| SharedSceneEventsV1::new(verified.clone(), 64 << 20).unwrap());
                let readers = (0..3)
                    .map(|_| match &factory {
                        Some(factory) => factory.reader().unwrap(),
                        None => verified.independent_reader().unwrap(),
                    })
                    .collect::<Vec<_>>();
                std::thread::scope(|scope| {
                    let workers = readers
                        .into_iter()
                        .map(|reader| {
                            scope.spawn(|| {
                                let mut count = 0;
                                for (i, event) in reader.enumerate() {
                                    assert_eq!(event.unwrap(), expected[i]);
                                    count += 1;
                                }
                                assert_eq!(count, expected.len());
                            })
                        })
                        .collect::<Vec<_>>();
                    for worker in workers {
                        worker.join().unwrap();
                    }
                });
                let wall = started.elapsed().as_secs_f64();
                let after = crate::ProcessCountersV1::read().ok();
                let diagnostics = factory.as_ref().map(|f| f.diagnostics().unwrap());
                if let Some(d) = &diagnostics {
                    assert_eq!(d.primary_decoded_events, 20000);
                    assert_eq!(d.fallback_readers, 0);
                }
                eprintln!(
                    "scene_decode_cost={}",
                    serde_json::json!({"trial":trial,"mode":if shared {"shared"}else{"independent"},"accounts":3,"events_per_account":expected.len(),"fixture_cancel_targets":32,"preverified_input":true,"measurement":crate::process_measurement_v1(before.as_ref(),after.as_ref(),hz,wall),"diagnostics":diagnostics})
                );
            }
        }
    }
}
