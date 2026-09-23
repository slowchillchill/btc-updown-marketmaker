use super::*;
use execution_normalization::record_file;
pub(super) const MANIFEST: &str = "ledger-parquet-v1.json";
const FORMAT: &str = "ledger_arrow_parquet_v1";
pub(crate) struct ParquetLedger {
    directory: PathBuf,
    segment_count: u64,
    writer: Option<record_file::Writer>,
    next_record_seq: u64,
    poisoned: bool,
}
fn invalid(e: impl fmt::Display) -> LedgerDurableErrorV1 {
    LedgerDurableErrorV1::InvalidStructure(e.to_string())
}
impl ParquetLedger {
    pub(super) fn create(
        directory: &Path,
        genesis: DurablePayloadV1,
    ) -> Result<Self, LedgerDurableErrorV1> {
        fs::create_dir_all(directory)
            .map_err(|e| io_error("创建 Ledger Parquet 根", directory, e))?;
        if directory.join(MANIFEST).exists() || directory.join(ACTIVE_FILE_NAME).exists() {
            return Err(invalid("Ledger 根已存在"));
        }
        for entry in
            fs::read_dir(directory).map_err(|e| io_error("读取 Ledger 耐久目录", directory, e))?
        {
            let entry = entry.map_err(|e| io_error("读取 Ledger 耐久目录项", directory, e))?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if (name.starts_with(SEGMENT_PREFIX) && name.ends_with(SEGMENT_SUFFIX))
                || name.starts_with("ledger-parquet-")
            {
                return Err(invalid("已有 Ledger 分片的根必须使用 open_durable"));
            }
        }
        let mut this = Self {
            directory: directory.into(),
            segment_count: 0,
            writer: None,
            next_record_seq: 1,
            poisoned: false,
        };
        this.publish_json(MANIFEST, &FORMAT)?;
        this.append(&genesis)?;
        Ok(this)
    }
    pub(super) fn open(
        directory: &Path,
        mut apply: impl FnMut(DurablePayloadV1) -> Result<(), LedgerDurableErrorV1>,
    ) -> Result<Self, LedgerDurableErrorV1> {
        let path = directory.join(MANIFEST);
        let format: String = serde_json::from_reader(
            File::open(&path).map_err(|e| io_error("读 Ledger 清单", &path, e))?,
        )
        .map_err(invalid)?;
        if format != FORMAT {
            return Err(invalid("不支持的 Ledger 物理格式"));
        }
        let mut paths = fs::read_dir(directory)
            .map_err(invalid)?
            .map(|e| e.map(|e| e.path()))
            .collect::<Result<Vec<_>, _>>()
            .map_err(invalid)?;
        paths.retain(|p| {
            p.file_name().is_some_and(|n| {
                n.to_string_lossy().starts_with("ledger-parquet-")
                    && n.to_string_lossy().ends_with(".manifest.json")
            })
        });
        paths.sort();
        let segment_count = paths.len() as u64;
        let mut expected = 1;
        let mut entry = 1;
        for (index, manifest_path) in paths.iter().enumerate() {
            if manifest_path.file_name().unwrap()
                != format!("ledger-parquet-{index:020}.manifest.json").as_str()
            {
                return Err(invalid("Ledger 清单序列缺口"));
            }
            let reference: LedgerSealedSegmentReferenceV1 =
                serde_json::from_reader(File::open(manifest_path).map_err(invalid)?)
                    .map_err(invalid)?;
            let path = PathBuf::from(&reference.location);
            if path.parent() != Some(directory) {
                return Err(invalid("Ledger 分片不属于当前根"));
            }
            let mut consume =
                |(seq, kind, body): record_file::Row| -> Result<(), LedgerDurableErrorV1> {
                    if seq != expected {
                        return Err(invalid("Ledger 耐久序号不连续"));
                    }
                    let payload = decode_payload(&kind, &body)?;
                    if let Some(found) = payload.ledger_entry_seq() {
                        if found != entry {
                            return Err(invalid("Ledger 经济序号不连续"));
                        }
                        entry += 1;
                    }
                    apply(payload)?;
                    expected += 1;
                    Ok(())
                };
            let boundary = record_file::visit(&path, FORMAT, |row| {
                consume(row).map_err(std::io::Error::other)
            })
            .map_err(invalid)?;
            if boundary != "ledger_transaction_segment_v2" {
                return Err(invalid("不支持的 Ledger 业务版本"));
            }
        }
        // 只有清单提交过的分片参与恢复；删除唯一当前未提交尾部，允许沿原格式续写。
        for name in [
            format!(".ledger-parquet-{segment_count:020}.tmp"),
            format!("ledger-parquet-{segment_count:020}.parquet"),
            format!(".ledger-parquet-{segment_count:020}.manifest.json.tmp"),
        ] {
            let path = directory.join(name);
            match fs::remove_file(&path) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(io_error("清理 Ledger 未提交尾部", &path, e)),
            }
        }
        sync_directory(directory, "同步 Ledger 未提交尾部恢复")?;
        Ok(Self {
            directory: directory.into(),
            segment_count,
            writer: None,
            next_record_seq: expected,
            poisoned: false,
        })
    }
    fn candidate(&self) -> PathBuf {
        self.directory
            .join(format!(".ledger-parquet-{:020}.tmp", self.segment_count))
    }
    pub(super) fn append(
        &mut self,
        payload: &DurablePayloadV1,
    ) -> Result<(), LedgerDurableErrorV1> {
        if self.poisoned {
            return Err(LedgerDurableErrorV1::HandlePoisoned);
        }
        let (kind, body) = encode_payload(payload)?;
        let row = (self.next_record_seq, kind.into(), body);
        if self.writer.is_none() {
            self.writer = Some(
                record_file::Writer::create(
                    &self.candidate(),
                    FORMAT,
                    "ledger_transaction_segment_v2",
                )
                .map_err(|e| io_error("创建 Ledger Parquet 候选", &self.candidate(), e))?,
            );
        }
        if let Err(e) = self.writer.as_mut().unwrap().append(&[row]) {
            self.poisoned = true;
            return Err(io_error("写入 Ledger Parquet 记录", &self.directory, e));
        }
        self.next_record_seq += 1;
        Ok(())
    }
    pub(super) fn flush(&mut self) -> Result<(), LedgerDurableErrorV1> {
        if self.poisoned {
            return Err(LedgerDurableErrorV1::HandlePoisoned);
        }
        if let Some(writer) = &mut self.writer
            && let Err(error) = writer.flush()
        {
            self.poisoned = true;
            return Err(io_error("写出 Ledger 交接批", &self.directory, error));
        }
        Ok(())
    }
    pub(super) fn seal(
        &mut self,
    ) -> Result<Option<LedgerSealedSegmentReferenceV1>, LedgerDurableErrorV1> {
        if self.poisoned {
            return Err(LedgerDurableErrorV1::HandlePoisoned);
        }
        let Some(writer) = self.writer.take() else {
            return Ok(None);
        };
        self.poisoned = true;
        writer
            .finish()
            .map_err(|e| io_error("完成并同步 Ledger Parquet", &self.candidate(), e))?;
        let candidate = self.candidate();
        let path = self
            .directory
            .join(format!("ledger-parquet-{:020}.parquet", self.segment_count));
        let parent = File::open(&self.directory)
            .map_err(|e| io_error("打开 Ledger Parquet 目录", &self.directory, e))?;
        renameat_with(
            &parent,
            candidate.file_name().unwrap(),
            &parent,
            path.file_name().unwrap(),
            RenameFlags::NOREPLACE,
        )
        .map_err(|e| io_error("发布 Ledger Parquet 分片", &path, e.into()))?;
        parent
            .sync_all()
            .map_err(|e| io_error("同步 Ledger Parquet 分片目录", &self.directory, e))?;
        let reference = LedgerSealedSegmentReferenceV1 {
            location: path.to_string_lossy().into(),
        };
        self.publish_json(
            &format!("ledger-parquet-{:020}.manifest.json", self.segment_count),
            &reference,
        )?;
        self.segment_count += 1;
        self.poisoned = false;
        Ok(Some(reference))
    }
    fn publish_json(&self, name: &str, value: &impl Serialize) -> Result<(), LedgerDurableErrorV1> {
        let candidate = self.directory.join(format!(".{name}.tmp"));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&candidate)
            .map_err(|e| io_error("创建 Ledger Parquet 清单候选", &candidate, e))?;
        serde_json::to_writer(&mut file, value).map_err(invalid)?;
        file.sync_all()
            .map_err(|e| io_error("同步 Ledger Parquet 清单候选", &candidate, e))?;
        let parent = File::open(&self.directory)
            .map_err(|e| io_error("打开 Ledger Parquet 目录", &self.directory, e))?;
        renameat_with(
            &parent,
            candidate.file_name().unwrap(),
            &parent,
            name,
            RenameFlags::NOREPLACE,
        )
        .map_err(|e| {
            io_error(
                "发布 Ledger Parquet 清单",
                &self.directory.join(name),
                e.into(),
            )
        })?;
        sync_directory(&self.directory, "同步 Ledger Parquet 清单").map_err(|e| {
            LedgerDurableErrorV1::DurabilityUncertain {
                operation: "发布 Ledger Parquet 清单",
                message: e.to_string(),
            }
        })?;
        if let Some(parent) = self.directory.parent() {
            sync_directory(parent, "同步 Ledger Parquet 根")?;
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct GenesisBody<P> {
    policy: P,
}
#[derive(Serialize, Deserialize)]
struct ReservationBody<I, R> {
    request_identity: I,
    request: R,
}

fn encode_payload(
    payload: &DurablePayloadV1,
) -> Result<(&'static str, Vec<u8>), LedgerDurableErrorV1> {
    let (kind, body) = match payload {
        DurablePayloadV1::Genesis { policy } => {
            ("GENESIS", serde_json::to_vec(&GenesisBody { policy }))
        }
        DurablePayloadV1::GenesisV2 { policy } => {
            ("GENESIS_V2", serde_json::to_vec(&GenesisBody { policy }))
        }
        DurablePayloadV1::Transaction(effect) => ("TRANSACTION", serde_json::to_vec(effect)),
        DurablePayloadV1::ReservationDecision {
            request_identity,
            request,
        } => (
            "RESERVATION_DECISION",
            serde_json::to_vec(&ReservationBody {
                request_identity,
                request,
            }),
        ),
    };
    Ok((kind, body.map_err(invalid)?))
}
fn decode_payload(kind: &str, body: &[u8]) -> Result<DurablePayloadV1, LedgerDurableErrorV1> {
    Ok(match kind {
        "GENESIS" => {
            let GenesisBody { policy } = serde_json::from_slice(body).map_err(invalid)?;
            DurablePayloadV1::Genesis { policy }
        }
        "GENESIS_V2" => {
            let GenesisBody { policy } = serde_json::from_slice(body).map_err(invalid)?;
            DurablePayloadV1::GenesisV2 { policy }
        }
        "TRANSACTION" => {
            DurablePayloadV1::Transaction(serde_json::from_slice(body).map_err(invalid)?)
        }
        "RESERVATION_DECISION" => {
            let ReservationBody {
                request_identity,
                request,
            } = serde_json::from_slice(body).map_err(invalid)?;
            DurablePayloadV1::ReservationDecision {
                request_identity,
                request,
            }
        }
        _ => return Err(invalid(format!("未知 Ledger 记录变体：{kind}"))),
    })
}
