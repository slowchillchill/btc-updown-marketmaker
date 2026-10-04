use super::*;
use std::io::{Seek, SeekFrom};
use std::sync::atomic::{AtomicU64, Ordering};

/// 原 File ChunkReader 的测试计数包装；计数发生于实际 Read 调用，不是 get_read。
#[derive(Clone)]
pub(in crate::typed_raw) struct CountedFile {
    file: Arc<File>,
    reads: Arc<AtomicU64>,
    bytes: Arc<AtomicU64>,
}

impl CountedFile {
    pub(in crate::typed_raw) fn new(file: File) -> Self {
        Self {
            file: Arc::new(file),
            reads: Arc::new(AtomicU64::new(0)),
            bytes: Arc::new(AtomicU64::new(0)),
        }
    }
    pub(in crate::typed_raw) fn counts(&self) -> (u64, u64) {
        (
            self.reads.load(Ordering::Relaxed),
            self.bytes.load(Ordering::Relaxed),
        )
    }
}

pub(in crate::typed_raw) struct CountedRead {
    file: File,
    reads: Arc<AtomicU64>,
    bytes: Arc<AtomicU64>,
}
impl Read for CountedRead {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        let read = self.file.read(output)?;
        self.bytes.fetch_add(read as u64, Ordering::Relaxed);
        Ok(read)
    }
}
impl Length for CountedFile {
    fn len(&self) -> u64 {
        self.file.len()
    }
}
impl ChunkReader for CountedFile {
    type T = BufReader<CountedRead>;
    fn get_read(&self, start: u64) -> Result<Self::T> {
        let mut file = self.file.try_clone()?;
        file.seek(SeekFrom::Start(start))?;
        Ok(BufReader::new(CountedRead {
            file,
            reads: self.reads.clone(),
            bytes: self.bytes.clone(),
        }))
    }
    fn get_bytes(&self, start: u64, length: usize) -> Result<Bytes> {
        let mut buffer = Vec::with_capacity(length);
        // 原 get_bytes 不经过 BufReader。
        let mut file = self.file.try_clone()?;
        file.seek(SeekFrom::Start(start))?;
        let read = CountedRead {
            file,
            reads: self.reads.clone(),
            bytes: self.bytes.clone(),
        }
        .take(length as u64)
        .read_to_end(&mut buffer)?;
        if read != length {
            return Err(ParquetError::EOF(format!(
                "Expected to read {length} bytes, read only {read}"
            )));
        }
        Ok(buffer.into())
    }
}

#[test]
fn interleaved_positions_owned_bytes_large_reads_and_short_tail() {
    let root = tempfile::TempDir::new().unwrap();
    let path = root.path().join("bytes");
    let bytes: Vec<_> = (0..WINDOW_BYTES * 3 + 17)
        .map(|n| (n % 251) as u8)
        .collect();
    std::fs::write(&path, &bytes).unwrap();
    let window = WindowedFile::new(File::open(&path).unwrap());
    let mut a = window.get_read(0).unwrap();
    let mut b = window.get_read((WINDOW_BYTES - 3) as u64).unwrap();
    let mut prefix = [0; 11];
    a.read_exact(&mut prefix).unwrap();
    assert_eq!(&prefix, &bytes[..11]);
    let owned = window.get_bytes(11, 200).unwrap();
    // 替换共享窗口后，已返回 Bytes 和两个 Read 各自的位置仍有效。
    assert_eq!(
        window
            .get_bytes((WINDOW_BYTES * 2) as u64, 100)
            .unwrap()
            .as_ref(),
        &bytes[WINDOW_BYTES * 2..WINDOW_BYTES * 2 + 100]
    );
    let mut cross = [0; 10];
    b.read_exact(&mut cross).unwrap();
    assert_eq!(&cross, &bytes[WINDOW_BYTES - 3..WINDOW_BYTES + 7]);
    a.read_exact(&mut prefix).unwrap();
    assert_eq!(&prefix, &bytes[11..22]);
    assert_eq!(owned.as_ref(), &bytes[11..211]);
    let before = window.read_counts();
    assert_eq!(
        window.get_bytes(0, WINDOW_BYTES + 1).unwrap().as_ref(),
        &bytes[..WINDOW_BYTES + 1]
    );
    assert_eq!(
        window.read_counts(),
        (before.0 + 1, before.1 + (WINDOW_BYTES + 1) as u64)
    );
    assert_eq!(
        window
            .get_bytes((bytes.len() - 17) as u64, 17)
            .unwrap()
            .as_ref(),
        &bytes[bytes.len() - 17..]
    );
    let mut tail = window.get_read((bytes.len() - 2) as u64).unwrap();
    let mut out = Vec::new();
    tail.read_to_end(&mut out).unwrap();
    assert_eq!(out, bytes[bytes.len() - 2..]);
    let original = File::open(&path)
        .unwrap()
        .get_bytes((bytes.len() - 2) as u64, 3)
        .unwrap_err();
    let new = window.get_bytes((bytes.len() - 2) as u64, 3).unwrap_err();
    assert_eq!(original.to_string(), new.to_string());
    assert!(window.get_bytes(bytes.len() as u64, 0).unwrap().is_empty());
}
