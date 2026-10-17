use super::{bucket_path, encode_key, encode_segment, http_error, http_status_error, object_path};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBytes};
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE, RANGE};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::sync::Mutex;
use tokio_util::io::ReaderStream;
use uuid::Uuid;

#[derive(Clone)]
struct AsyncHttpInner {
    base_url: String,
    client: reqwest::Client,
    stream_client: reqwest::Client,
}

impl AsyncHttpInner {
    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }
}

#[pyclass]
pub(super) struct RustHttpClientAsync {
    inner: Arc<AsyncHttpInner>,
}

#[pyclass]
pub(super) struct RustObjectReaderAsync {
    state: Arc<Mutex<AsyncReaderState>>,
    headers: HashMap<String, String>,
    closed: Arc<AtomicBool>,
}

struct AsyncReaderState {
    response: Option<reqwest::Response>,
    pending: Vec<u8>,
    pending_offset: usize,
    cache: Option<tokio::fs::File>,
    closed: bool,
}

#[pyclass]
pub(super) struct RustObjectWriterAsync {
    state: Arc<Mutex<AsyncWriterState>>,
    closed: Arc<AtomicBool>,
    temp_path: PathBuf,
}

struct AsyncWriterState {
    client: reqwest::Client,
    url: String,
    content_type: Option<String>,
    file: Option<tokio::fs::File>,
    cache: Option<tokio::fs::File>,
    result_json: Option<String>,
    closed: bool,
    uploaded: bool,
}

#[pymethods]
impl RustHttpClientAsync {
    #[new]
    #[pyo3(signature = (base_url="http://127.0.0.1:8000", timeout=10.0, disable_proxy=false, service_token=None))]
    fn new(
        base_url: &str,
        timeout: f64,
        disable_proxy: bool,
        service_token: Option<&str>,
    ) -> PyResult<Self> {
        let timeout = std::time::Duration::from_secs_f64(timeout.max(0.001));
        let resolved_service_token = service_token
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .or_else(|| {
                std::env::var("ALOCALS3_SERVICE_TOKEN")
                    .ok()
                    .map(|value| value.trim().to_string())
                    .filter(|value| !value.is_empty())
            });
        let mut default_headers = HeaderMap::new();
        if let Some(token) = resolved_service_token {
            let value = HeaderValue::from_str(&format!("Bearer {token}")).map_err(|_| {
                pyo3::exceptions::PyValueError::new_err("invalid alocals3 service token")
            })?;
            default_headers.insert(AUTHORIZATION, value);
        }

        let mut builder = reqwest::Client::builder()
            .timeout(timeout)
            .default_headers(default_headers.clone());
        if disable_proxy {
            builder = builder.no_proxy();
        }
        let client = builder.build().map_err(http_error)?;

        let mut stream_builder = reqwest::Client::builder()
            .connect_timeout(timeout)
            .default_headers(default_headers);
        if disable_proxy {
            stream_builder = stream_builder.no_proxy();
        }
        let stream_client = stream_builder.build().map_err(http_error)?;

        Ok(Self {
            inner: Arc::new(AsyncHttpInner {
                base_url: base_url.trim_end_matches('/').to_string(),
                client,
                stream_client,
            }),
        })
    }

    fn head_object<'py>(
        &self,
        py: Python<'py>,
        bucket: &str,
        key: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let request = self
            .inner
            .client
            .head(self.inner.url(&object_path(bucket, key)));
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let response = checked_response(request).await?;
            Ok(headers_to_map(response.headers()))
        })
    }

    #[pyo3(signature = (bucket, key, content_type=None, metadata=None))]
    fn create_multipart_upload_json<'py>(
        &self,
        py: Python<'py>,
        bucket: &str,
        key: &str,
        content_type: Option<&str>,
        metadata: Option<HashMap<String, String>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let url = self.inner.url(&format!(
            "/s3-multipart/{}/{}",
            encode_segment(bucket),
            encode_key(key)
        ));
        self.request_json(
            py,
            add_request_headers(self.inner.client.post(url), content_type, metadata),
        )
    }

    fn upload_part_json<'py>(
        &self,
        py: Python<'py>,
        upload_id: &str,
        part_number: u64,
        body: &[u8],
    ) -> PyResult<Bound<'py, PyAny>> {
        let url = self.inner.url(&format!(
            "/s3-uploads/{}/{}",
            encode_segment(upload_id),
            part_number
        ));
        self.request_json(py, self.inner.stream_client.put(url).body(body.to_vec()))
    }

    fn list_parts_json<'py>(
        &self,
        py: Python<'py>,
        upload_id: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let url = self
            .inner
            .url(&format!("/s3-uploads/{}", encode_segment(upload_id)));
        self.request_json(py, self.inner.client.get(url))
    }

    #[pyo3(signature = (upload_id, manifest, if_match=None, if_none_match=None))]
    fn complete_multipart_upload_json<'py>(
        &self,
        py: Python<'py>,
        upload_id: &str,
        manifest: &str,
        if_match: Option<&str>,
        if_none_match: Option<&str>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let url = self
            .inner
            .url(&format!("/s3-uploads/{}", encode_segment(upload_id)));
        let mut request = self
            .inner
            .stream_client
            .post(url)
            .header(CONTENT_TYPE, "application/json")
            .body(manifest.to_string());
        if let Some(value) = if_match {
            request = request.header("if-match", value);
        }
        if let Some(value) = if_none_match {
            request = request.header("if-none-match", value);
        }
        self.request_json(py, request)
    }

    fn abort_multipart_upload<'py>(
        &self,
        py: Python<'py>,
        upload_id: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let url = self
            .inner
            .url(&format!("/s3-uploads/{}", encode_segment(upload_id)));
        self.request_empty(py, self.inner.client.delete(url))
    }

    #[pyo3(signature = (bucket, limit=100, continuation_token=None))]
    fn list_multipart_uploads_json<'py>(
        &self,
        py: Python<'py>,
        bucket: &str,
        limit: i64,
        continuation_token: Option<&str>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let request = self
            .inner
            .client
            .get(self.inner.url("/s3-uploads"))
            .query(&[
                ("bucket", bucket.to_string()),
                ("limit", limit.to_string()),
                (
                    "continuation_token",
                    continuation_token.unwrap_or("").to_string(),
                ),
            ]);
        self.request_json(py, request)
    }
    fn abort_multipart_uploads_json<'py>(
        &self,
        py: Python<'py>,
        manifest: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.request_json(
            py,
            self.inner
                .client
                .post(self.inner.url("/s3-uploads/abort"))
                .header(CONTENT_TYPE, "application/json")
                .body(manifest.to_owned()),
        )
    }

    fn health_json<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.request_json(py, self.inner.client.get(self.inner.url("/healthz")))
    }

    fn list_buckets_json<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.request_json(py, self.inner.client.get(self.inner.url("/s3")))
    }

    fn create_bucket_json<'py>(
        &self,
        py: Python<'py>,
        bucket: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.request_json(
            py,
            self.inner.client.put(self.inner.url(&bucket_path(bucket))),
        )
    }

    fn delete_bucket<'py>(&self, py: Python<'py>, bucket: &str) -> PyResult<Bound<'py, PyAny>> {
        self.request_empty(
            py,
            self.inner
                .client
                .delete(self.inner.url(&bucket_path(bucket))),
        )
    }

    #[pyo3(signature = (bucket, prefix=None, limit=1000))]
    fn list_objects_json<'py>(
        &self,
        py: Python<'py>,
        bucket: &str,
        prefix: Option<&str>,
        limit: i64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let mut request = self
            .inner
            .client
            .get(self.inner.url(&format!("{}/objects", bucket_path(bucket))))
            .query(&[("limit", limit.to_string())]);
        if let Some(prefix) = prefix {
            request = request.query(&[("prefix", prefix)]);
        }
        self.request_json(py, request)
    }

    #[pyo3(signature = (bucket, prefix="", delimiter="", max_keys=1000, continuation_token=None))]
    fn list_objects_v2_json<'py>(
        &self,
        py: Python<'py>,
        bucket: &str,
        prefix: &str,
        delimiter: &str,
        max_keys: i64,
        continuation_token: Option<&str>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let mut query = vec![
            ("list-type", "2".to_string()),
            ("prefix", prefix.to_string()),
            ("delimiter", delimiter.to_string()),
            ("max-keys", max_keys.to_string()),
            ("output", "json".to_string()),
        ];
        if let Some(token) = continuation_token {
            query.push(("continuation-token", token.to_string()));
        }
        let request = self
            .inner
            .client
            .get(self.inner.url(&bucket_path(bucket)))
            .query(&query);
        self.request_json(py, request)
    }

    #[pyo3(signature = (bucket, key, file_path, content_type=None, metadata=None, if_match=None, if_none_match=None))]
    fn put_object_json<'py>(
        &self,
        py: Python<'py>,
        bucket: &str,
        key: &str,
        file_path: &str,
        content_type: Option<&str>,
        metadata: Option<HashMap<String, String>>,
        if_match: Option<&str>,
        if_none_match: Option<&str>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        let url = inner.url(&object_path(bucket, key));
        let file_path = PathBuf::from(file_path);
        let content_type = content_type.map(str::to_string);
        let if_match = if_match.map(str::to_owned);
        let if_none_match = if_none_match.map(str::to_owned);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let file = tokio::fs::File::open(&file_path)
                .await
                .map_err(super::io_error)?;
            let len = file.metadata().await.map_err(super::io_error)?.len();
            let stream = ReaderStream::new(BufReader::new(file));
            let mut request = inner
                .stream_client
                .put(url)
                .header(CONTENT_LENGTH, len)
                .body(reqwest::Body::wrap_stream(stream));
            if let Some(value) = if_match {
                request = request.header("if-match", value);
            }
            if let Some(value) = if_none_match {
                request = request.header("if-none-match", value);
            }
            request = add_request_headers(request, content_type.as_deref(), metadata);
            request_json(request).await
        })
    }

    #[pyo3(signature = (bucket, key, body, content_type=None, metadata=None, if_match=None, if_none_match=None))]
    fn put_object_bytes_json<'py>(
        &self,
        py: Python<'py>,
        bucket: &str,
        key: &str,
        body: &[u8],
        content_type: Option<&str>,
        metadata: Option<HashMap<String, String>>,
        if_match: Option<&str>,
        if_none_match: Option<&str>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        let url = inner.url(&object_path(bucket, key));
        let body = body.to_vec();
        let content_type = content_type.map(str::to_string);
        let if_match = if_match.map(str::to_owned);
        let if_none_match = if_none_match.map(str::to_owned);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut request = inner.client.put(url).body(body);
            if let Some(value) = if_match {
                request = request.header("if-match", value);
            }
            if let Some(value) = if_none_match {
                request = request.header("if-none-match", value);
            }
            request_json(add_request_headers(
                request,
                content_type.as_deref(),
                metadata,
            ))
            .await
        })
    }

    #[pyo3(signature = (bucket, key, range_header=None))]
    fn get_object_bytes<'py>(
        &self,
        py: Python<'py>,
        bucket: &str,
        key: &str,
        range_header: Option<&str>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        let url = inner.url(&object_path(bucket, key));
        let range_header = range_header.map(str::to_string);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut request = inner.stream_client.get(url);
            if let Some(range_header) = range_header {
                request = request.header(RANGE, range_header);
            }
            let response = checked_response(request).await?;
            let headers = headers_to_map(response.headers());
            let body = response.bytes().await.map_err(http_error)?;
            Python::attach(|py| Ok((PyBytes::new(py, &body).unbind(), headers)))
        })
    }

    #[pyo3(signature = (bucket, key, output_path, range_header=None, resume=true, max_retries=3))]
    fn get_object_to_file<'py>(
        &self,
        py: Python<'py>,
        bucket: &str,
        key: &str,
        output_path: &str,
        range_header: Option<&str>,
        resume: bool,
        max_retries: u32,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.inner.stream_client.clone();
        let url = self.inner.url(&object_path(bucket, key));
        let output = PathBuf::from(output_path);
        let range = range_header.map(str::to_owned);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let headers = match range {
                Some(range) => {
                    super::download::asynchronous_range(&client, &url, &output, &range).await?
                }
                None => {
                    super::download::asynchronous(&client, &url, &output, resume, max_retries)
                        .await?
                }
            };
            Ok(headers_to_map(&headers))
        })
    }

    fn delete_object<'py>(
        &self,
        py: Python<'py>,
        bucket: &str,
        key: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.request_empty(
            py,
            self.inner
                .client
                .delete(self.inner.url(&object_path(bucket, key))),
        )
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (source_bucket, source_key, destination_bucket, destination_key, metadata=None, metadata_directive=None, content_type=None))]
    fn copy_object_json<'py>(
        &self,
        py: Python<'py>,
        source_bucket: &str,
        source_key: &str,
        destination_bucket: &str,
        destination_key: &str,
        metadata: Option<HashMap<String, String>>,
        metadata_directive: Option<&str>,
        content_type: Option<&str>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let source = format!(
            "/{}/{}",
            encode_segment(source_bucket),
            encode_key(source_key)
        );
        let directive = metadata_directive
            .unwrap_or(if metadata.is_some() || content_type.is_some() {
                "REPLACE"
            } else {
                "COPY"
            })
            .to_string();
        let request = self
            .inner
            .client
            .put(
                self.inner
                    .url(&object_path(destination_bucket, destination_key)),
            )
            .query(&[("output", "json")])
            .header("x-amz-copy-source", source)
            .header("x-amz-metadata-directive", directive);
        self.request_json(py, add_request_headers(request, content_type, metadata))
    }

    #[pyo3(signature = (bucket, key, range_header=None, cache_path=None))]
    fn open_object_reader<'py>(
        &self,
        py: Python<'py>,
        bucket: &str,
        key: &str,
        range_header: Option<&str>,
        cache_path: Option<&str>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        let url = inner.url(&object_path(bucket, key));
        let range_header = range_header.map(str::to_string);
        let cache_path = cache_path.map(PathBuf::from);
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut request = inner.stream_client.get(url);
            if let Some(range_header) = range_header {
                request = request.header(RANGE, range_header);
            }
            let response = checked_response(request).await?;
            let headers = headers_to_map(response.headers());
            let cache = open_cache_file(cache_path.as_deref()).await;
            let closed = Arc::new(AtomicBool::new(false));
            let reader = RustObjectReaderAsync {
                state: Arc::new(Mutex::new(AsyncReaderState {
                    response: Some(response),
                    pending: Vec::new(),
                    pending_offset: 0,
                    cache,
                    closed: false,
                })),
                headers,
                closed,
            };
            Python::attach(|py| Py::new(py, reader))
        })
    }

    #[pyo3(signature = (bucket, key, content_type=None, cache_path=None))]
    fn open_object_writer<'py>(
        &self,
        py: Python<'py>,
        bucket: &str,
        key: &str,
        content_type: Option<&str>,
        cache_path: Option<&str>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        let url = inner.url(&object_path(bucket, key));
        let content_type = content_type.map(str::to_string);
        let cache_path = cache_path.map(PathBuf::from);
        let temp_path = std::env::temp_dir().join(format!(
            "alocals3-async-upload-{}.tmp",
            Uuid::new_v4().simple()
        ));
        let state_temp_path = temp_path.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let file = tokio::fs::File::create(&state_temp_path)
                .await
                .map_err(super::io_error)?;
            let cache = open_cache_file(cache_path.as_deref()).await;
            let closed = Arc::new(AtomicBool::new(false));
            let writer = RustObjectWriterAsync {
                state: Arc::new(Mutex::new(AsyncWriterState {
                    client: inner.stream_client.clone(),
                    url,
                    content_type,
                    file: Some(file),
                    cache,
                    result_json: None,
                    closed: false,
                    uploaded: false,
                })),
                closed,
                temp_path: state_temp_path,
            };
            Python::attach(|py| Py::new(py, writer))
        })
    }
}

impl RustHttpClientAsync {
    fn request_json<'py>(
        &self,
        py: Python<'py>,
        request: reqwest::RequestBuilder,
    ) -> PyResult<Bound<'py, PyAny>> {
        pyo3_async_runtimes::tokio::future_into_py(py, request_json(request))
    }

    fn request_empty<'py>(
        &self,
        py: Python<'py>,
        request: reqwest::RequestBuilder,
    ) -> PyResult<Bound<'py, PyAny>> {
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            checked_response(request).await?;
            Ok(())
        })
    }
}

#[pymethods]
impl RustObjectReaderAsync {
    #[pyo3(signature = (size=-1))]
    fn read<'py>(&self, py: Python<'py>, size: isize) -> PyResult<Bound<'py, PyAny>> {
        let state = self.state.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let body = read_from_state(state, size, false).await?;
            Python::attach(|py| Ok(PyBytes::new(py, &body).unbind()))
        })
    }

    #[pyo3(signature = (size=-1))]
    fn readline<'py>(&self, py: Python<'py>, size: isize) -> PyResult<Bound<'py, PyAny>> {
        let state = self.state.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let body = read_from_state(state, size, true).await?;
            Python::attach(|py| Ok(PyBytes::new(py, &body).unbind()))
        })
    }

    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let state = self.state.clone();
        let closed = self.closed.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut state = state.lock().await;
            state.response.take();
            if let Some(mut cache) = state.cache.take() {
                let _ = cache.flush().await;
            }
            state.closed = true;
            closed.store(true, Ordering::Release);
            Ok(())
        })
    }

    fn headers(&self) -> HashMap<String, String> {
        self.headers.clone()
    }

    fn readable(&self) -> bool {
        !self.closed.load(Ordering::Acquire)
    }

    fn closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
}

#[pymethods]
impl RustObjectWriterAsync {
    fn write<'py>(&self, py: Python<'py>, body: &[u8]) -> PyResult<Bound<'py, PyAny>> {
        let state = self.state.clone();
        let body = body.to_vec();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut state = state.lock().await;
            if state.closed {
                return Err(PyRuntimeError::new_err("I/O operation on closed S3 writer"));
            }
            let Some(file) = state.file.as_mut() else {
                return Err(PyRuntimeError::new_err("S3 writer has no open spool file"));
            };
            file.write_all(&body).await.map_err(super::io_error)?;
            if let Some(cache) = state.cache.as_mut() {
                if cache.write_all(&body).await.is_err() {
                    state.cache.take();
                }
            }
            Ok(body.len())
        })
    }

    fn flush<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let state = self.state.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut state = state.lock().await;
            if state.closed {
                return Err(PyRuntimeError::new_err("I/O operation on closed S3 writer"));
            }
            if let Some(file) = state.file.as_mut() {
                file.flush().await.map_err(super::io_error)?;
            }
            if let Some(cache) = state.cache.as_mut() {
                if cache.flush().await.is_err() {
                    state.cache.take();
                }
            }
            Ok(())
        })
    }

    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let state = self.state.clone();
        let closed = self.closed.clone();
        let temp_path = self.temp_path.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut state = state.lock().await;
            if state.closed {
                return Ok(state.result_json.clone().unwrap_or_default());
            }
            let Some(file) = state.file.as_mut() else {
                return Err(PyRuntimeError::new_err("S3 writer has no open spool file"));
            };
            file.flush().await.map_err(super::io_error)?;
            if let Some(cache) = state.cache.as_mut() {
                let _ = cache.flush().await;
            }

            let upload_file = tokio::fs::File::open(&temp_path)
                .await
                .map_err(super::io_error)?;
            let len = upload_file.metadata().await.map_err(super::io_error)?.len();
            let stream = ReaderStream::new(BufReader::new(upload_file));
            let mut request = state
                .client
                .put(state.url.clone())
                .header(CONTENT_LENGTH, len)
                .body(reqwest::Body::wrap_stream(stream));
            if let Some(content_type) = state.content_type.as_deref() {
                request = request.header(CONTENT_TYPE, content_type);
            }
            let result = request_json(request).await?;

            state.file.take();
            state.cache.take();
            state.result_json = Some(result.clone());
            state.closed = true;
            state.uploaded = true;
            closed.store(true, Ordering::Release);
            let _ = tokio::fs::remove_file(&temp_path).await;
            Ok(result)
        })
    }

    fn discard<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let state = self.state.clone();
        let closed = self.closed.clone();
        let temp_path = self.temp_path.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let mut state = state.lock().await;
            state.file.take();
            state.cache.take();
            state.closed = true;
            closed.store(true, Ordering::Release);
            let _ = tokio::fs::remove_file(temp_path).await;
            Ok(())
        })
    }

    fn writable(&self) -> bool {
        !self.closed.load(Ordering::Acquire)
    }

    fn closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
}

impl Drop for RustObjectWriterAsync {
    fn drop(&mut self) {
        if !self.closed.load(Ordering::Acquire) {
            let _ = std::fs::remove_file(&self.temp_path);
        }
    }
}

async fn request_json(request: reqwest::RequestBuilder) -> PyResult<String> {
    let response = checked_response(request).await?;
    response.text().await.map_err(http_error)
}

async fn checked_response(request: reqwest::RequestBuilder) -> PyResult<reqwest::Response> {
    let response = request.send().await.map_err(http_error)?;
    let status = response.status();
    if !status.is_success() {
        return Err(http_status_error(status.as_u16()));
    }
    Ok(response)
}

fn add_request_headers(
    mut request: reqwest::RequestBuilder,
    content_type: Option<&str>,
    metadata: Option<HashMap<String, String>>,
) -> reqwest::RequestBuilder {
    if let Some(content_type) = content_type {
        request = request.header(CONTENT_TYPE, content_type);
    }
    if let Some(metadata) = metadata {
        for (key, value) in metadata {
            request = request.header(format!("x-amz-meta-{key}"), value);
        }
    }
    request
}

fn headers_to_map(headers: &HeaderMap) -> HashMap<String, String> {
    headers
        .iter()
        .map(|(key, value)| {
            (
                key.as_str().to_string(),
                value.to_str().unwrap_or_default().to_string(),
            )
        })
        .collect()
}

async fn open_cache_file(path: Option<&Path>) -> Option<tokio::fs::File> {
    let path = path?;
    if let Some(parent) = path.parent() {
        if tokio::fs::create_dir_all(parent).await.is_err() {
            return None;
        }
    }
    tokio::fs::File::create(path).await.ok()
}

async fn read_from_state(
    state: Arc<Mutex<AsyncReaderState>>,
    size: isize,
    stop_at_newline: bool,
) -> PyResult<Vec<u8>> {
    if size == 0 {
        return Ok(Vec::new());
    }
    let limit = if size < 0 { usize::MAX } else { size as usize };
    let mut state = state.lock().await;
    if state.closed {
        return Err(PyRuntimeError::new_err("I/O operation on closed S3 reader"));
    }
    let mut output = Vec::new();

    loop {
        if state.pending_offset < state.pending.len() {
            let available = &state.pending[state.pending_offset..];
            let remaining = limit.saturating_sub(output.len());
            let mut take = available.len().min(remaining);
            if stop_at_newline {
                if let Some(pos) = available[..take].iter().position(|byte| *byte == b'\n') {
                    take = pos + 1;
                }
            }
            output.extend_from_slice(&available[..take]);
            state.pending_offset += take;
            if state.pending_offset == state.pending.len() {
                state.pending.clear();
                state.pending_offset = 0;
            }
            if output.len() == limit || (stop_at_newline && output.last() == Some(&b'\n')) {
                break;
            }
            continue;
        }

        let Some(response) = state.response.as_mut() else {
            break;
        };
        match response.chunk().await.map_err(http_error)? {
            Some(chunk) => {
                state.pending = chunk.to_vec();
                state.pending_offset = 0;
            }
            None => {
                state.response.take();
                break;
            }
        }
    }

    if let Some(cache) = state.cache.as_mut() {
        if cache.write_all(&output).await.is_err() {
            state.cache.take();
        }
    }
    Ok(output)
}
