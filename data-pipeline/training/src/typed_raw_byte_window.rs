//! 单一已验证 File 的有界字节供给；不参与 Parquet 或业务校验。
use std::fs::File;
use std::io::{self, BufReader, Read};
use std::os::unix::fs::FileExt;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use parquet::errors::{ParquetError, Result};
use parquet::file::reader::{ChunkReader, Length};

const WINDOW_BYTES: usize = 64 * 1024;

struct Window {
    bytes: Box<[u8; WINDOW_BYTES]>,
    start: u64,
    valid: usize,
}

struct Shared {
    file: File,
    window: Mutex<Window>,
    #[cfg(test)]
    reads: std::sync::atomic::AtomicU64,
    #[cfg(test)]
    read_bytes: std::sync::atomic::AtomicU64,
}

impl Shared {
    fn file_read(&self, output: &mut [u8], start: u64) -> io::Result<usize> {
        #[cfg(test)]
        self.reads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let read = self.file.read_at(output, start)?;
        #[cfg(test)]
        self.read_bytes
            .fetch_add(read as u64, std::sync::atomic::Ordering::Relaxed);
        Ok(read)
    }

    fn read(&self, output: &mut [u8], start: u64) -> io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        if output.len() > WINDOW_BYTES {
            return self.file_read(output, start);
        }
        // 每次缓冲补充只锁一次；页头的逐字节读取由返回 Read 的 BufReader 吸收。
        let mut window = self.window.lock().unwrap();
        let offset = start.checked_sub(window.start);
        if offset.is_none_or(|offset| offset >= window.valid as u64) {
            let read = self.file_read(window.bytes.as_mut_slice(), start)?;
            window.start = start;
            window.valid = read;
        }
        let offset = (start - window.start) as usize;
        let read = output.len().min(window.valid - offset);
        output[..read].copy_from_slice(&window.bytes[offset..offset + read]);
        Ok(read)
    }
}

/// 同一文件共享一个窗口，返回对象不持有窗口切片。
#[derive(Clone)]
pub(crate) struct WindowedFile(Arc<Shared>);

impl WindowedFile {
    pub(super) fn new(file: File) -> Self {
        Self(Arc::new(Shared {
            file,
            window: Mutex::new(Window {
                bytes: Box::new([0; WINDOW_BYTES]),
                start: 0,
                valid: 0,
            }),
            #[cfg(test)]
            reads: std::sync::atomic::AtomicU64::new(0),
            #[cfg(test)]
            read_bytes: std::sync::atomic::AtomicU64::new(0),
        }))
    }

    #[cfg(test)]
    pub(super) fn read_counts(&self) -> (u64, u64) {
        use std::sync::atomic::Ordering;
        (
            self.0.reads.load(Ordering::Relaxed),
            self.0.read_bytes.load(Ordering::Relaxed),
        )
    }
}

impl Length for WindowedFile {
    fn len(&self) -> u64 {
        self.0.file.len()
    }
}

pub(crate) struct WindowRead {
    shared: Arc<Shared>,
    position: u64,
}

impl Read for WindowRead {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        let read = self.shared.read(output, self.position)?;
        self.position += read as u64;
        Ok(read)
    }
}

impl ChunkReader for WindowedFile {
    // 保留原 File 适配器每个返回 Read 的默认 8KiB 缓冲。
    type T = BufReader<WindowRead>;

    fn get_read(&self, start: u64) -> Result<Self::T> {
        Ok(BufReader::new(WindowRead {
            shared: self.0.clone(),
            position: start,
        }))
    }

    fn get_bytes(&self, start: u64, length: usize) -> Result<Bytes> {
        let mut buffer = vec![0; length];
        let mut read = 0;
        while read < length {
            let result = if length > WINDOW_BYTES {
                self.0.file_read(&mut buffer[read..], start + read as u64)
            } else {
                self.0.read(&mut buffer[read..], start + read as u64)
            };
            match result {
                Ok(0) => break,
                Ok(count) => read += count,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error.into()),
            }
        }
        if read != length {
            return Err(ParquetError::EOF(format!(
                "Expected to read {length} bytes, read only {read}"
            )));
        }
        Ok(buffer.into())
    }
}

#[cfg(test)]
#[path = "typed_raw_byte_window_tests.rs"]
pub(super) mod tests;
