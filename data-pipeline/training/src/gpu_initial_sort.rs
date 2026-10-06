//! GPU初始run排序与原CPU跨run归并协作。只传原字节键，载荷留在CPU只读缓冲。
use crate::{InitialRunOrder, SortError, SortRow};
use std::{
    io::{self, BufReader, BufWriter, Read, Write},
    path::Path,
    process::{Child, ChildStdin, ChildStdout, Command, Stdio},
    sync::Mutex,
};

struct Worker {
    child: Child,
    input: BufWriter<ChildStdin>,
    output: BufReader<ChildStdout>,
    failed: bool,
}
impl Drop for Worker {
    fn drop(&mut self) {
        // 仅结束本准备入口拥有的子进程，不触及计算实例或其他作业。
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
pub struct GpuInitialRunOrderV1 {
    worker: Mutex<Worker>,
}
impl GpuInitialRunOrderV1 {
    /// python和worker_script必须来自当前已核对环境及准确执行提交。
    pub fn start(python: &Path, worker_script: &Path) -> Result<Self, SortError> {
        let mut child = Command::new(python).arg("-u").arg(worker_script)
            .stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::inherit()).spawn()?;
        let input = BufWriter::new(child.stdin.take().ok_or_else(|| io::Error::other("GPU worker缺stdin"))?);
        let mut output = BufReader::new(child.stdout.take().ok_or_else(|| io::Error::other("GPU worker缺stdout"))?);
        let mut greeting = [0_u8; 8];
        if let Err(error) = output.read_exact(&mut greeting) {
            let _ = child.kill();
            let _ = child.wait();
            return Err(error.into());
        }
        if &greeting != b"DPTGPU01" {
            let _ = child.kill();
            let _ = child.wait();
            return Err(SortError::InvalidRun("GPU worker协议不符"));
        }
        Ok(Self { worker: Mutex::new(Worker { child, input, output, failed: false }) })
    }
}
impl InitialRunOrder for GpuInitialRunOrderV1 {
    fn permutation(&self, rows: &[SortRow]) -> Result<Vec<usize>, SortError> {
        let mut worker = self.worker.lock().map_err(|_| io::Error::other("GPU worker协调失败"))?;
        if worker.failed { return Err(io::Error::other("GPU worker已失败，不回退CPU排序").into()); }
        worker.failed = true;
        let width = rows.iter().map(|r| r.sort_key_bytes().len()).max().unwrap_or(0);
        let count = u64::try_from(rows.len()).map_err(|_| SortError::InvalidRun("行数超协议容量"))?;
        let key_width = u64::try_from(width).map_err(|_| SortError::InvalidRun("键宽超协议容量"))?;
        worker.input.write_all(&count.to_le_bytes())?;
        worker.input.write_all(&key_width.to_le_bytes())?;
        for row in rows {
            worker.input.write_all(&(row.sort_key_bytes().len() as u64).to_le_bytes())?;
        }
        let padding = vec![0; width];
        for row in rows {
            worker.input.write_all(row.sort_key_bytes())?;
            worker.input.write_all(&padding[..width - row.sort_key_bytes().len()])?;
        }
        worker.input.flush()?;
        let mut bytes = [0_u8; 8];
        worker.output.read_exact(&mut bytes)?;
        if u64::from_le_bytes(bytes) != count { return Err(SortError::InvalidRun("GPU worker响应行数不符")); }
        let mut indices = Vec::with_capacity(rows.len());
        for _ in rows {
            worker.output.read_exact(&mut bytes)?;
            indices.push(usize::try_from(u64::from_le_bytes(bytes))
                .map_err(|_| SortError::InvalidRun("GPU permutation超本机索引容量"))?);
        }
        worker.failed = false;
        Ok(indices)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ExternalSorter, PartCursor, SortConfig};
    use std::sync::Arc;

    #[test]
    #[ignore = "只在已同步准确提交的AutoDL CUDA环境显式运行"]
    fn gpu_initial_runs_preserve_keys_payloads_and_cpu_merge() {
        let python = std::env::var("B_GPU_PYTHON").unwrap();
        let script = std::env::var("B_GPU_SORT_WORKER").unwrap();
        let order = Arc::new(GpuInitialRunOrderV1::start(Path::new(&python), Path::new(&script)).unwrap());
        let root = tempfile::tempdir().unwrap();
        let config = SortConfig {
            max_run_rows: 1024, max_run_bytes: 64*1024*1024,
            max_temp_run_files: 64, merge_fan_in: 2, merge_workers: 8,
            max_open_files: 24, batch_rows: 1024, max_temp_bytes: 256*1024*1024,
        };
        let mut sorter = ExternalSorter::with_initial_order(root.path(), &config, Some(order)).unwrap();
        for index in (0_u64..8192).rev() {
            let mut key = (index/256).to_be_bytes().to_vec();
            key.extend((index%256).to_be_bytes());
            key.extend(format!("original-message-{index:08}").as_bytes());
            sorter.push(SortRow::from_sort_key(key, index.to_le_bytes())).unwrap();
        }
        let (run, metrics) = sorter.finish(None).unwrap();
        assert_eq!(metrics.input_rows,8192);
        assert_eq!(metrics.output_rows,8192);
        assert_eq!(metrics.initial_runs,8);
        assert!(metrics.peak_merge_workers>1);
        let mut cursor = PartCursor::open(&run,1024,64*1024*1024).unwrap();
        for expected in 0_u64..8192 {
            let row = cursor.next_row().unwrap().unwrap();
            assert_eq!(row.payload(),expected.to_le_bytes());
            assert_eq!(&row.sort_key_bytes()[..8],(expected/256).to_be_bytes());
            assert_eq!(&row.sort_key_bytes()[8..16],(expected%256).to_be_bytes());
        }
        assert!(cursor.next_row().unwrap().is_none());
        eprintln!("gpu_initial_run_metrics={}",serde_json::to_string(&metrics).unwrap());
    }
}
