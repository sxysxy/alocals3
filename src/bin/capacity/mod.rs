//! Free-space floor for writes performed by cooperating server processes.
use super::*;

pub(super) struct Capacity {
    root: PathBuf,
    pub(super) min_free_bytes: u64,
}
impl Capacity {
    pub(super) fn new(root: PathBuf, min_free_bytes: u64) -> Self {
        Self {
            root,
            min_free_bytes,
        }
    }
    pub(super) fn available(&self) -> std::io::Result<u64> {
        fs2::available_space(&self.root)
    }
    pub(super) fn total(&self) -> std::io::Result<u64> {
        fs2::total_space(&self.root)
    }
    pub(super) fn check(&self, bytes: u64) -> ApiResult<()> {
        if self.min_free_bytes > 0 && self.available()? < self.min_free_bytes.saturating_add(bytes)
        {
            return Err(ApiError::new(
                StatusCode::INSUFFICIENT_STORAGE,
                "Insufficient free disk space; configured reserve must remain available",
            ));
        }
        Ok(())
    }
    pub(super) async fn write(&self, file: &mut tokio::fs::File, bytes: &[u8]) -> ApiResult<()> {
        if self.min_free_bytes == 0 {
            file.write_all(bytes).await?;
            return Ok(());
        }
        // A separately opened handle per chunk ensures locks also serialize
        // concurrent requests in one process. Never hold it during network waits.
        let lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(self.root.join(".capacity.lock"))?;
        loop {
            match lock.try_lock() {
                Ok(()) => break,
                Err(std::fs::TryLockError::WouldBlock) => {
                    tokio::time::sleep(Duration::from_millis(1)).await
                }
                Err(std::fs::TryLockError::Error(e)) => return Err(e.into()),
            }
        }
        self.check(bytes.len() as u64)?;
        file.write_all(bytes).await?;
        // Tokio File may complete a write into its userspace buffer. Flush to the
        // OS before releasing the shared disk admission lock.
        file.flush().await?;
        Ok(())
    }
}
