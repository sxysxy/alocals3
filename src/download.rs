//! Resumable downloads keep a version-bound partial next to the destination.
//! A filesystem lock prevents two processes from writing the same partial.
use super::{http_error, http_status_error, io_error};
use pyo3::{exceptions::PyRuntimeError, PyResult};
use reqwest::header::HeaderMap;
use serde::{Deserialize, Serialize};
use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Serialize, Deserialize, PartialEq)]
struct Identity {
    url: String,
    etag: String,
    size: u64,
}
struct Partial {
    path: PathBuf,
    state: PathBuf,
    _lock: File,
}
fn sidecar(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(suffix);
    PathBuf::from(name)
}
fn error(detail: &str) -> pyo3::PyErr {
    PyRuntimeError::new_err(format!("ALOCALS3_DOWNLOAD_ERROR:{detail}"))
}
impl Partial {
    fn open(output: &Path) -> PyResult<Self> {
        if let Some(parent) = output.parent().filter(|p| !p.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent).map_err(io_error)?;
        }
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(sidecar(output, ".alocals3.lock"))
            .map_err(io_error)?;
        lock.try_lock()
            .map_err(|_| error("destination is already being downloaded"))?;
        Ok(Self {
            path: sidecar(output, ".alocals3.part"),
            state: sidecar(output, ".alocals3.json"),
            _lock: lock,
        })
    }
    fn prepare(&self, identity: &Identity, resume: bool) -> PyResult<u64> {
        let previous = std::fs::read(&self.state)
            .ok()
            .and_then(|b| serde_json::from_slice::<Identity>(&b).ok());
        let size = std::fs::metadata(&self.path).map(|m| m.len()).unwrap_or(0);
        if !self.path.exists()
            || !resume
            || previous.as_ref() != Some(identity)
            || size > identity.size
        {
            File::create(&self.path).map_err(io_error)?;
        }
        // A stale/missing sidecar only causes a full restart, never mixed versions.
        let bytes = serde_json::to_vec(identity).map_err(|e| error(&e.to_string()))?;
        let mut file = File::create(&self.state).map_err(io_error)?;
        file.write_all(&bytes).map_err(io_error)?;
        file.sync_all().map_err(io_error)?;
        Ok(std::fs::metadata(&self.path).map_err(io_error)?.len())
    }
    fn finish(&self, output: &Path) -> PyResult<()> {
        std::fs::rename(&self.path, output).map_err(io_error)?;
        let _ = std::fs::remove_file(&self.state);
        Ok(())
    }
}
fn identity(url: &str, headers: &HeaderMap) -> PyResult<Identity> {
    let etag = headers
        .get("etag")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| error("HEAD is missing ETag"))?;
    if etag.starts_with("W/") {
        return Err(error("resuming requires a strong ETag"));
    }
    let size = headers
        .get("content-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse().ok())
        .ok_or_else(|| error("HEAD is missing Content-Length"))?;
    Ok(Identity {
        url: url.to_owned(),
        etag: etag.to_owned(),
        size,
    })
}
fn check_response(
    status: reqwest::StatusCode,
    headers: &HeaderMap,
    id: &Identity,
    offset: u64,
) -> PyResult<()> {
    if !status.is_success() {
        return Err(http_status_error(status.as_u16()));
    }
    if headers.get("etag").and_then(|v| v.to_str().ok()) != Some(id.etag.as_str()) {
        return Err(error("object changed during download"));
    }
    if offset > 0 {
        let expected = format!("bytes {}-{}/{}", offset, id.size - 1, id.size);
        if status != reqwest::StatusCode::PARTIAL_CONTENT
            || headers.get("content-range").and_then(|v| v.to_str().ok()) != Some(expected.as_str())
        {
            return Err(error("invalid resumed Content-Range"));
        }
    } else if status != reqwest::StatusCode::OK {
        return Err(error("unexpected download status"));
    }
    if headers
        .get("content-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        != Some(id.size - offset)
    {
        return Err(error("unexpected download length"));
    }
    Ok(())
}
fn validate_digest(id: &Identity, digest: md5::Digest) -> PyResult<()> {
    let etag = id.etag.trim_matches('"');
    if etag.len() == 32
        && etag.bytes().all(|b| b.is_ascii_hexdigit())
        && format!("{:x}", digest) != etag.to_ascii_lowercase()
    {
        return Err(error("download checksum mismatch; retry with resume=False"));
    }
    Ok(())
}

pub(super) fn blocking(
    client: &reqwest::blocking::Client,
    url: &str,
    output: &Path,
    resume: bool,
    max_retries: u32,
) -> PyResult<HeaderMap> {
    let partial = Partial::open(output)?;
    let head = client.head(url).send().map_err(http_error)?;
    if !head.status().is_success() {
        return Err(http_status_error(head.status().as_u16()));
    }
    let headers = head.headers().clone();
    let id = identity(url, &headers)?;
    let mut offset = partial.prepare(&id, resume)?;
    for attempt in 0..=max_retries {
        if offset == id.size {
            break;
        }
        let mut request = client.get(url).header("if-match", &id.etag);
        if offset > 0 {
            request = request.header("range", format!("bytes={offset}-"));
        }
        let response = request.send();
        let mut response = match response {
            Ok(r) => r,
            Err(e) if attempt < max_retries => {
                std::thread::sleep(std::time::Duration::from_millis(100 * (attempt as u64 + 1)));
                let _ = e;
                continue;
            }
            Err(e) => return Err(http_error(e)),
        };
        if response.status().is_server_error() && attempt < max_retries {
            std::thread::sleep(std::time::Duration::from_millis(100 * (attempt as u64 + 1)));
            continue;
        }
        check_response(response.status(), response.headers(), &id, offset)?;
        let mut file = OpenOptions::new()
            .append(true)
            .open(&partial.path)
            .map_err(io_error)?;
        let mut buffer = vec![0; 1024 * 1024];
        loop {
            match response.read(&mut buffer) {
                Ok(0) => break,
                Ok(n) => {
                    file.write_all(&buffer[..n]).map_err(io_error)?;
                    offset += n as u64;
                }
                Err(e) if attempt == max_retries => return Err(io_error(e)),
                Err(_) => break,
            }
        }
        file.sync_all().map_err(io_error)?;
        if offset == id.size {
            break;
        }
    }
    if offset != id.size {
        return Err(error("download incomplete; partial retained"));
    }
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&partial.path)
        .map_err(io_error)?;
    let mut md5 = md5::Context::new();
    let mut buffer = vec![0; 1024 * 1024];
    loop {
        let n = file.read(&mut buffer).map_err(io_error)?;
        if n == 0 {
            break;
        }
        md5.consume(&buffer[..n]);
    }
    validate_digest(&id, md5.compute())?;
    // Sync also covers an empty object or a complete partial from an earlier run.
    file.sync_all().map_err(io_error)?;
    drop(file);
    partial.finish(output)?;
    Ok(headers)
}

pub(super) async fn asynchronous(
    client: &reqwest::Client,
    url: &str,
    output: &Path,
    resume: bool,
    max_retries: u32,
) -> PyResult<HeaderMap> {
    let partial = Partial::open(output)?;
    let head = client.head(url).send().await.map_err(http_error)?;
    if !head.status().is_success() {
        return Err(http_status_error(head.status().as_u16()));
    }
    let headers = head.headers().clone();
    let id = identity(url, &headers)?;
    let mut offset = partial.prepare(&id, resume)?;
    for attempt in 0..=max_retries {
        if offset == id.size {
            break;
        }
        let mut request = client.get(url).header("if-match", &id.etag);
        if offset > 0 {
            request = request.header("range", format!("bytes={offset}-"));
        }
        let mut response = match request.send().await {
            Ok(r) => r,
            Err(_) if attempt < max_retries => {
                tokio::time::sleep(std::time::Duration::from_millis(100 * (attempt as u64 + 1)))
                    .await;
                continue;
            }
            Err(e) => return Err(http_error(e)),
        };
        if response.status().is_server_error() && attempt < max_retries {
            tokio::time::sleep(std::time::Duration::from_millis(100 * (attempt as u64 + 1))).await;
            continue;
        }
        check_response(response.status(), response.headers(), &id, offset)?;
        let mut file = tokio::fs::OpenOptions::new()
            .append(true)
            .open(&partial.path)
            .await
            .map_err(io_error)?;
        loop {
            match response.chunk().await {
                Ok(None) => break,
                Ok(Some(bytes)) => {
                    file.write_all(&bytes).await.map_err(io_error)?;
                    offset += bytes.len() as u64;
                }
                Err(e) if attempt == max_retries => return Err(http_error(e)),
                Err(_) => break,
            }
        }
        file.sync_all().await.map_err(io_error)?;
        if offset == id.size {
            break;
        }
    }
    if offset != id.size {
        return Err(error("download incomplete; partial retained"));
    }
    let mut file = tokio::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&partial.path)
        .await
        .map_err(io_error)?;
    let mut md5 = md5::Context::new();
    let mut buffer = vec![0; 1024 * 1024];
    loop {
        let n = file.read(&mut buffer).await.map_err(io_error)?;
        if n == 0 {
            break;
        }
        md5.consume(&buffer[..n]);
    }
    validate_digest(&id, md5.compute())?;
    file.sync_all().await.map_err(io_error)?;
    drop(file);
    partial.finish(output)?;
    Ok(headers)
}

pub(super) fn blocking_range(
    client: &reqwest::blocking::Client,
    url: &str,
    output: &Path,
    range: &str,
) -> PyResult<HeaderMap> {
    let partial = Partial::open(output)?;
    let _ = std::fs::remove_file(&partial.state);
    let mut response = client
        .get(url)
        .header("range", range)
        .send()
        .map_err(http_error)?;
    if !response.status().is_success() {
        return Err(http_status_error(response.status().as_u16()));
    }
    if response.status() != reqwest::StatusCode::PARTIAL_CONTENT {
        return Err(error("server ignored Range"));
    }
    let headers = response.headers().clone();
    let mut file = File::create(&partial.path).map_err(io_error)?;
    std::io::copy(&mut response, &mut file).map_err(io_error)?;
    file.sync_all().map_err(io_error)?;
    drop(file);
    partial.finish(output)?;
    Ok(headers)
}
pub(super) async fn asynchronous_range(
    client: &reqwest::Client,
    url: &str,
    output: &Path,
    range: &str,
) -> PyResult<HeaderMap> {
    let partial = Partial::open(output)?;
    let _ = std::fs::remove_file(&partial.state);
    let mut response = client
        .get(url)
        .header("range", range)
        .send()
        .await
        .map_err(http_error)?;
    if !response.status().is_success() {
        return Err(http_status_error(response.status().as_u16()));
    }
    if response.status() != reqwest::StatusCode::PARTIAL_CONTENT {
        return Err(error("server ignored Range"));
    }
    let headers = response.headers().clone();
    let mut file = tokio::fs::File::create(&partial.path)
        .await
        .map_err(io_error)?;
    while let Some(bytes) = response.chunk().await.map_err(http_error)? {
        file.write_all(&bytes).await.map_err(io_error)?;
    }
    file.sync_all().await.map_err(io_error)?;
    drop(file);
    partial.finish(output)?;
    Ok(headers)
}
