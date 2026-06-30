use chrono::{SecondsFormat, Utc};
use percent_encoding::{percent_encode, NON_ALPHANUMERIC};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyModule};
use rusqlite::{params, Connection, OptionalExtension, Transaction};
use sha2::{Digest, Sha256};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard};
use uuid::Uuid;

const ERR_PREFIX: &str = "ALOCALS3_STORAGE_ERROR:";
const SQLITE_POOL_SIZE: usize = 16;
const KEY_ENCODE_SET: &percent_encoding::AsciiSet = &NON_ALPHANUMERIC.remove(b'/');

#[pyclass]
struct RustStorageBackend {
    objects_root: PathBuf,
    conn_pool: Vec<Mutex<Connection>>,
    next_conn: AtomicUsize,
}

#[pyclass]
struct RustHttpClient {
    base_url: String,
    client: reqwest::blocking::Client,
}

#[pymethods]
impl RustHttpClient {
    #[new]
    #[pyo3(signature = (base_url="http://127.0.0.1:8000", timeout=10.0, disable_proxy=false))]
    fn new(base_url: &str, timeout: f64, disable_proxy: bool) -> PyResult<Self> {
        let mut builder = reqwest::blocking::Client::builder()
            .timeout(std::time::Duration::from_secs_f64(timeout.max(0.001)));
        if disable_proxy {
            builder = builder.no_proxy();
        }
        let client = builder.build().map_err(http_error)?;
        Ok(Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            client,
        })
    }

    fn health_json(&self) -> PyResult<String> {
        self.request_json(self.client.get(self.url("/healthz")))
    }

    fn list_buckets_json(&self) -> PyResult<String> {
        self.request_json(self.client.get(self.url("/s3")))
    }

    fn create_bucket_json(&self, bucket: &str) -> PyResult<String> {
        self.request_json(self.client.put(self.url(&bucket_path(bucket))))
    }

    fn delete_bucket(&self, bucket: &str) -> PyResult<()> {
        self.request_empty(self.client.delete(self.url(&bucket_path(bucket))))
    }

    #[pyo3(signature = (bucket, prefix=None, limit=1000))]
    fn list_objects_json(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        limit: i64,
    ) -> PyResult<String> {
        let mut request = self
            .client
            .get(self.url(&format!("{}/objects", bucket_path(bucket))))
            .query(&[("limit", limit.to_string())]);
        if let Some(prefix) = prefix {
            request = request.query(&[("prefix", prefix)]);
        }
        self.request_json(request)
    }

    #[pyo3(signature = (bucket, prefix="", delimiter="", max_keys=1000, continuation_token=None))]
    fn list_objects_v2_json(
        &self,
        bucket: &str,
        prefix: &str,
        delimiter: &str,
        max_keys: i64,
        continuation_token: Option<&str>,
    ) -> PyResult<String> {
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
        self.request_json(
            self.client
                .get(self.url(&bucket_path(bucket)))
                .query(&query),
        )
    }

    #[pyo3(signature = (bucket, key, file_path, content_type=None))]
    fn put_object_json(
        &self,
        bucket: &str,
        key: &str,
        file_path: &str,
        content_type: Option<&str>,
    ) -> PyResult<String> {
        let body = fs::read(file_path).map_err(io_error)?;
        let mut request = self
            .client
            .put(self.url(&object_path(bucket, key)))
            .body(body);
        if let Some(content_type) = content_type {
            request = request.header(reqwest::header::CONTENT_TYPE, content_type);
        }
        self.request_json(request)
    }

    #[pyo3(signature = (bucket, key, output_path, range_header=None))]
    fn get_object_to_file(
        &self,
        py: Python<'_>,
        bucket: &str,
        key: &str,
        output_path: &str,
        range_header: Option<&str>,
    ) -> PyResult<PyObject> {
        let mut request = self.client.get(self.url(&object_path(bucket, key)));
        if let Some(range_header) = range_header {
            request = request.header(reqwest::header::RANGE, range_header);
        }
        let response = request.send().map_err(http_error)?;
        let status = response.status();
        if !status.is_success() {
            return Err(http_status_error(status.as_u16()));
        }
        let headers = response.headers().clone();
        let body = response.bytes().map_err(http_error)?;
        if let Some(parent) = Path::new(output_path).parent() {
            fs::create_dir_all(parent).map_err(io_error)?;
        }
        fs::write(output_path, &body).map_err(io_error)?;
        headers_to_dict(py, &headers)
    }

    fn get_object_range(
        &self,
        py: Python<'_>,
        bucket: &str,
        key: &str,
        range_header: &str,
    ) -> PyResult<PyObject> {
        let response = self
            .client
            .get(self.url(&object_path(bucket, key)))
            .header(reqwest::header::RANGE, range_header)
            .send()
            .map_err(http_error)?;
        let status = response.status();
        if !status.is_success() {
            return Err(http_status_error(status.as_u16()));
        }
        let headers = response.headers().clone();
        let body = response.bytes().map_err(http_error)?;
        let result = PyDict::new_bound(py);
        result.set_item("body", PyBytes::new_bound(py, &body))?;
        result.set_item("headers", headers_to_dict(py, &headers)?)?;
        Ok(result.into())
    }

    fn delete_object(&self, bucket: &str, key: &str) -> PyResult<()> {
        self.request_empty(self.client.delete(self.url(&object_path(bucket, key))))
    }
}

impl RustHttpClient {
    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    fn request_json(&self, request: reqwest::blocking::RequestBuilder) -> PyResult<String> {
        let response = request.send().map_err(http_error)?;
        let status = response.status();
        if !status.is_success() {
            return Err(http_status_error(status.as_u16()));
        }
        response.text().map_err(http_error)
    }

    fn request_empty(&self, request: reqwest::blocking::RequestBuilder) -> PyResult<()> {
        let response = request.send().map_err(http_error)?;
        let status = response.status();
        if !status.is_success() {
            return Err(http_status_error(status.as_u16()));
        }
        Ok(())
    }
}

#[pymethods]
impl RustStorageBackend {
    #[new]
    fn new(root: String, database_url: String) -> PyResult<Self> {
        let root = PathBuf::from(root);
        let objects_root = root.join("objects");
        fs::create_dir_all(&objects_root).map_err(io_error)?;

        let db_path = sqlite_path(&database_url)?;
        if let Some(parent) = db_path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent).map_err(io_error)?;
            }
        }

        let mut conn_pool = Vec::with_capacity(SQLITE_POOL_SIZE);
        for _ in 0..SQLITE_POOL_SIZE {
            let conn = Connection::open(&db_path).map_err(db_error)?;
            init_connection(&conn)?;
            conn_pool.push(Mutex::new(conn));
        }
        Ok(Self {
            objects_root,
            conn_pool,
            next_conn: AtomicUsize::new(0),
        })
    }

    fn list_buckets(&self, py: Python<'_>) -> PyResult<PyObject> {
        let conn = self.conn()?;
        let mut stmt = conn
            .prepare("SELECT name, created_at FROM buckets ORDER BY name ASC")
            .map_err(db_error)?;
        let mut rows = stmt.query([]).map_err(db_error)?;
        let out = PyList::empty_bound(py);
        while let Some(row) = rows.next().map_err(db_error)? {
            let item = PyDict::new_bound(py);
            item.set_item("name", row.get::<_, String>(0).map_err(db_error)?)?;
            item.set_item("created_at", row.get::<_, String>(1).map_err(db_error)?)?;
            out.append(item)?;
        }
        Ok(out.into())
    }

    fn create_bucket(&self, py: Python<'_>, bucket: String) -> PyResult<PyObject> {
        validate_bucket(&bucket)?;
        let now = now_utc();
        let conn = self.conn()?;
        let changed = conn
            .execute(
                "INSERT OR IGNORE INTO buckets (name, created_at) VALUES (?1, ?2)",
                params![bucket, now],
            )
            .map_err(db_error)?;
        if changed == 0 {
            return Err(storage_error(409, "Bucket already exists"));
        }
        bucket_info(py, &conn, &bucket)
    }

    fn delete_bucket(&self, bucket: String) -> PyResult<()> {
        validate_bucket(&bucket)?;
        let mut conn = self.conn()?;
        let tx = conn.transaction().map_err(db_error)?;
        let bucket_id = bucket_id_or_404(&tx, &bucket)?;
        let count: i64 = tx
            .query_row(
                "SELECT COUNT(*) FROM objects WHERE bucket_id = ?1",
                params![bucket_id],
                |row| row.get(0),
            )
            .map_err(db_error)?;
        if count > 0 {
            return Err(storage_error(409, "Bucket is not empty"));
        }
        tx.execute("DELETE FROM buckets WHERE id = ?1", params![bucket_id])
            .map_err(db_error)?;
        tx.commit().map_err(db_error)?;
        Ok(())
    }

    #[pyo3(signature = (bucket, prefix=None, limit=1000))]
    fn list_objects(
        &self,
        py: Python<'_>,
        bucket: String,
        prefix: Option<String>,
        limit: i64,
    ) -> PyResult<PyObject> {
        validate_bucket(&bucket)?;
        let prefix = prefix.unwrap_or_default();
        ensure_utf8(&prefix, "prefix")?;
        let conn = self.conn()?;
        let bucket_id = bucket_id_or_404(&conn, &bucket)?;
        let mut stmt = conn
            .prepare(
                "SELECT key, size, content_type, etag, updated_at \
                 FROM objects WHERE bucket_id = ?1 ORDER BY key ASC",
            )
            .map_err(db_error)?;
        let mut rows = stmt.query(params![bucket_id]).map_err(db_error)?;
        let out = PyList::empty_bound(py);
        while let Some(row) = rows.next().map_err(db_error)? {
            let key: String = row.get(0).map_err(db_error)?;
            if !prefix.is_empty() && !key.starts_with(&prefix) {
                continue;
            }
            out.append(object_info_from_row(py, &bucket, &key, row)?)?;
            if out.len() >= limit.max(0) as usize {
                break;
            }
        }
        Ok(out.into())
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (bucket, prefix=None, delimiter=None, max_keys=1000, continuation_token=None))]
    fn list_objects_v2(
        &self,
        py: Python<'_>,
        bucket: String,
        prefix: Option<String>,
        delimiter: Option<String>,
        max_keys: i64,
        continuation_token: Option<String>,
    ) -> PyResult<PyObject> {
        validate_bucket(&bucket)?;
        let prefix = prefix.unwrap_or_default();
        let delimiter = delimiter.unwrap_or_default();
        ensure_utf8(&prefix, "prefix")?;
        ensure_utf8(&delimiter, "delimiter")?;
        if let Some(token) = &continuation_token {
            ensure_utf8(token, "continuation_token")?;
        }
        if max_keys < 1 {
            return Err(storage_error(422, "max_keys must be >= 1"));
        }

        let conn = self.conn()?;
        let bucket_id = bucket_id_or_404(&conn, &bucket)?;
        let mut stmt = conn
            .prepare(
                "SELECT key, size, content_type, etag, updated_at \
                 FROM objects WHERE bucket_id = ?1 ORDER BY key ASC",
            )
            .map_err(db_error)?;
        let mut rows = stmt.query(params![bucket_id]).map_err(db_error)?;

        let contents = PyList::empty_bound(py);
        let common_prefixes = PyList::empty_bound(py);
        let mut common_prefix_set: Vec<String> = Vec::new();
        let mut key_count = 0_i64;
        let mut next_token: Option<String> = None;

        while let Some(row) = rows.next().map_err(db_error)? {
            let key: String = row.get(0).map_err(db_error)?;
            if !prefix.is_empty() && !key.starts_with(&prefix) {
                continue;
            }
            if let Some(token) = &continuation_token {
                if key <= *token {
                    continue;
                }
            }

            if !delimiter.is_empty() {
                let tail = if !prefix.is_empty() && key.starts_with(&prefix) {
                    &key[prefix.len()..]
                } else {
                    &key
                };
                if let Some(pos) = tail.find(&delimiter) {
                    let cp = key[..prefix.len() + pos + delimiter.len()].to_string();
                    if !common_prefix_set.iter().any(|seen| seen == &cp) {
                        if key_count >= max_keys {
                            next_token = Some(key);
                            break;
                        }
                        common_prefix_set.push(cp.clone());
                        common_prefixes.append(cp)?;
                        key_count += 1;
                    }
                    continue;
                }
            }

            if key_count >= max_keys {
                next_token = Some(key);
                break;
            }
            contents.append(object_info_from_row(py, &bucket, &key, row)?)?;
            key_count += 1;
        }

        let result = PyDict::new_bound(py);
        result.set_item("bucket", bucket)?;
        result.set_item("prefix", prefix)?;
        result.set_item("delimiter", delimiter)?;
        result.set_item("max_keys", max_keys)?;
        result.set_item("key_count", key_count)?;
        result.set_item("is_truncated", next_token.is_some())?;
        result.set_item("next_continuation_token", next_token)?;
        result.set_item("contents", contents)?;
        result.set_item("common_prefixes", common_prefixes)?;
        Ok(result.into())
    }

    #[pyo3(signature = (bucket, key, body, content_type=None))]
    fn put_object_with_state(
        &self,
        py: Python<'_>,
        bucket: String,
        key: String,
        body: &[u8],
        content_type: Option<String>,
    ) -> PyResult<PyObject> {
        let body = body.to_vec();
        let (info_data, created) = py
            .allow_threads(|| {
                self.put_object_with_state_inner(&bucket, &key, &body, content_type.as_deref())
            })
            .map_err(storage_failure_to_py)?;
        let info = object_info_data_dict(py, &info_data)?;
        let result = PyDict::new_bound(py);
        result.set_item("info", info)?;
        result.set_item("created", created)?;
        Ok(result.into())
    }

    fn get_object(&self, py: Python<'_>, bucket: String, key: String) -> PyResult<PyObject> {
        validate_bucket(&bucket)?;
        let key = normalize_key(&key)?;
        let conn = self.conn()?;
        let bucket_id = bucket_id_or_404(&conn, &bucket)?;
        let row = object_record(&conn, bucket_id, &key)?;
        let body = fs::read(self.objects_root.join(&row.file_path))
            .map_err(|_| storage_error(404, "Object data missing"))?;
        let info = object_info_dict(
            py,
            &bucket,
            &row.key,
            row.size,
            &row.content_type,
            &row.etag,
            &row.updated_at,
        )?;
        let dict = info.downcast_bound::<PyDict>(py)?;
        dict.set_item("body", PyBytes::new_bound(py, &body))?;
        Ok(info)
    }

    fn get_object_info(
        &self,
        py: Python<'_>,
        bucket: String,
        key: String,
    ) -> PyResult<Option<PyObject>> {
        validate_bucket(&bucket)?;
        let key = normalize_key(&key)?;
        let conn = self.conn()?;
        let bucket_id = bucket_id_or_404(&conn, &bucket)?;
        let row = conn
            .query_row(
                "SELECT key, size, content_type, etag, updated_at, file_path \
                 FROM objects WHERE bucket_id = ?1 AND key = ?2",
                params![bucket_id, key],
                |row| {
                    Ok(ObjectRecord {
                        key: row.get(0)?,
                        size: row.get(1)?,
                        content_type: row.get(2)?,
                        etag: row.get(3)?,
                        updated_at: row.get(4)?,
                        file_path: row.get(5)?,
                    })
                },
            )
            .optional()
            .map_err(db_error)?;
        match row {
            Some(row) => Ok(Some(object_info_dict(
                py,
                &bucket,
                &row.key,
                row.size,
                &row.content_type,
                &row.etag,
                &row.updated_at,
            )?)),
            None => Ok(None),
        }
    }

    fn delete_object(&self, bucket: String, key: String) -> PyResult<()> {
        validate_bucket(&bucket)?;
        let key = normalize_key(&key)?;
        let mut conn = self.conn()?;
        let tx = conn.transaction().map_err(db_error)?;
        let bucket_id = bucket_id_or_404(&tx, &bucket)?;
        let changed = tx
            .execute(
                "DELETE FROM objects WHERE bucket_id = ?1 AND key = ?2",
                params![bucket_id, key],
            )
            .map_err(db_error)?;
        if changed == 0 {
            return Err(storage_error(404, "Object not found"));
        }
        tx.commit().map_err(db_error)?;
        Ok(())
    }
}

impl RustStorageBackend {
    fn conn(&self) -> PyResult<MutexGuard<'_, Connection>> {
        let idx = self.next_conn.fetch_add(1, Ordering::Relaxed) % self.conn_pool.len();
        self.conn_pool[idx]
            .lock()
            .map_err(|_| PyRuntimeError::new_err("SQLite connection mutex is poisoned"))
    }
}

#[pymodule]
fn _rust(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<RustStorageBackend>()?;
    m.add_class::<RustHttpClient>()?;
    Ok(())
}

struct ObjectRecord {
    key: String,
    size: i64,
    content_type: String,
    etag: String,
    updated_at: String,
    file_path: String,
}

struct ObjectInfoData {
    bucket: String,
    key: String,
    size: i64,
    content_type: String,
    etag: String,
    updated_at: String,
}

#[derive(Debug)]
struct StorageFailure {
    status: u16,
    detail: String,
}

type StorageResult<T> = Result<T, StorageFailure>;

impl RustStorageBackend {
    fn put_object_with_state_inner(
        &self,
        bucket: &str,
        key: &str,
        body: &[u8],
        content_type: Option<&str>,
    ) -> StorageResult<(ObjectInfoData, bool)> {
        validate_bucket_inner(bucket)?;
        let key = normalize_key_inner(key)?;
        let final_content_type = if let Some(value) = content_type {
            ensure_utf8_inner(value, "content_type")?;
            value.to_string()
        } else {
            mime_guess::from_path(&key)
                .first_raw()
                .unwrap_or("application/octet-stream")
                .to_string()
        };
        let now = now_utc();
        let etag = format!("{:x}", md5::compute(body));
        let digest = hex_sha256(body);
        let relative_path = format!("{}/{}/{}", &digest[..2], &digest[2..4], digest);
        let absolute_path = self.objects_root.join(&relative_path);
        atomic_write_blob_inner(&absolute_path, body)?;

        let mut conn = self.conn_inner()?;
        let tx = conn.transaction().map_err(db_failure)?;
        let bucket_id = bucket_id_or_404_inner(&tx, bucket)?;
        let existing_id = object_id_inner(&tx, bucket_id, &key)?;
        let created = existing_id.is_none();
        if let Some(id) = existing_id {
            tx.execute(
                "UPDATE objects SET file_path = ?1, size = ?2, content_type = ?3, etag = ?4, updated_at = ?5 \
                 WHERE id = ?6",
                params![relative_path, body.len() as i64, final_content_type, etag, now, id],
            )
            .map_err(db_failure)?;
        } else {
            tx.execute(
                "INSERT INTO objects (bucket_id, key, file_path, size, content_type, etag, updated_at, created_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?7)",
                params![bucket_id, key, relative_path, body.len() as i64, final_content_type, etag, now],
            )
            .map_err(db_failure)?;
        }
        tx.commit().map_err(db_failure)?;

        let info = object_info_data(&conn, bucket, &key)?;
        Ok((info, created))
    }

    fn conn_inner(&self) -> StorageResult<MutexGuard<'_, Connection>> {
        let idx = self.next_conn.fetch_add(1, Ordering::Relaxed) % self.conn_pool.len();
        self.conn_pool[idx]
            .lock()
            .map_err(|_| storage_failure(500, "SQLite connection mutex is poisoned"))
    }
}

fn init_connection(conn: &Connection) -> PyResult<()> {
    conn.pragma_update(None, "journal_mode", "WAL")
        .map_err(db_error)?;
    conn.pragma_update(None, "busy_timeout", 10_000)
        .map_err(db_error)?;
    conn.pragma_update(None, "synchronous", "NORMAL")
        .map_err(db_error)?;
    conn.execute_batch(
        "\
        CREATE TABLE IF NOT EXISTS buckets (\
            id INTEGER PRIMARY KEY AUTOINCREMENT,\
            name VARCHAR(255) NOT NULL UNIQUE,\
            created_at DATETIME NOT NULL\
        );\
        CREATE INDEX IF NOT EXISTS ix_buckets_name ON buckets (name);\
        CREATE TABLE IF NOT EXISTS objects (\
            id INTEGER PRIMARY KEY AUTOINCREMENT,\
            bucket_id INTEGER NOT NULL REFERENCES buckets(id) ON DELETE CASCADE,\
            key VARCHAR(1024) NOT NULL,\
            file_path VARCHAR(1024) NOT NULL,\
            size INTEGER NOT NULL,\
            content_type VARCHAR(255) NOT NULL DEFAULT 'application/octet-stream',\
            etag VARCHAR(64) NOT NULL,\
            updated_at DATETIME NOT NULL,\
            created_at DATETIME NOT NULL,\
            CONSTRAINT uq_objects_bucket_key UNIQUE (bucket_id, key)\
        );\
        CREATE INDEX IF NOT EXISTS idx_objects_bucket_key ON objects (bucket_id, key);\
        ",
    )
    .map_err(db_error)?;
    Ok(())
}

fn sqlite_path(database_url: &str) -> PyResult<PathBuf> {
    if let Some(path) = database_url.strip_prefix("sqlite:///") {
        if path == ":memory:" {
            return Ok(PathBuf::from(path));
        }
        return Ok(PathBuf::from(path));
    }
    if let Some(path) = database_url.strip_prefix("sqlite://") {
        return Ok(PathBuf::from(path));
    }
    Err(PyValueError::new_err(
        "RustStorageBackend only supports sqlite database URLs",
    ))
}

fn bucket_id_or_404(conn: &Connection, bucket: &str) -> PyResult<i64> {
    conn.query_row(
        "SELECT id FROM buckets WHERE name = ?1",
        params![bucket],
        |row| row.get(0),
    )
    .optional()
    .map_err(db_error)?
    .ok_or_else(|| storage_error(404, "Bucket not found"))
}

fn object_record(conn: &Connection, bucket_id: i64, key: &str) -> PyResult<ObjectRecord> {
    conn.query_row(
        "SELECT key, size, content_type, etag, updated_at, file_path \
         FROM objects WHERE bucket_id = ?1 AND key = ?2",
        params![bucket_id, key],
        |row| {
            Ok(ObjectRecord {
                key: row.get(0)?,
                size: row.get(1)?,
                content_type: row.get(2)?,
                etag: row.get(3)?,
                updated_at: row.get(4)?,
                file_path: row.get(5)?,
            })
        },
    )
    .optional()
    .map_err(db_error)?
    .ok_or_else(|| storage_error(404, "Object not found"))
}

fn bucket_info(py: Python<'_>, conn: &Connection, bucket: &str) -> PyResult<PyObject> {
    let created_at: String = conn
        .query_row(
            "SELECT created_at FROM buckets WHERE name = ?1",
            params![bucket],
            |row| row.get(0),
        )
        .map_err(db_error)?;
    let dict = PyDict::new_bound(py);
    dict.set_item("name", bucket)?;
    dict.set_item("created_at", created_at)?;
    Ok(dict.into())
}

fn object_info_data(conn: &Connection, bucket: &str, key: &str) -> StorageResult<ObjectInfoData> {
    let bucket_id = bucket_id_or_404_inner(conn, bucket)?;
    let row = object_record_inner(conn, bucket_id, key)?;
    Ok(ObjectInfoData {
        bucket: bucket.to_string(),
        key: row.key,
        size: row.size,
        content_type: row.content_type,
        etag: row.etag,
        updated_at: row.updated_at,
    })
}

fn object_info_data_dict(py: Python<'_>, data: &ObjectInfoData) -> PyResult<PyObject> {
    object_info_dict(
        py,
        &data.bucket,
        &data.key,
        data.size,
        &data.content_type,
        &data.etag,
        &data.updated_at,
    )
}

fn object_info_from_row(
    py: Python<'_>,
    bucket: &str,
    key: &str,
    row: &rusqlite::Row<'_>,
) -> PyResult<PyObject> {
    object_info_dict(
        py,
        bucket,
        key,
        row.get(1).map_err(db_error)?,
        &row.get::<_, String>(2).map_err(db_error)?,
        &row.get::<_, String>(3).map_err(db_error)?,
        &row.get::<_, String>(4).map_err(db_error)?,
    )
}

fn object_info_dict(
    py: Python<'_>,
    bucket: &str,
    key: &str,
    size: i64,
    content_type: &str,
    etag: &str,
    updated_at: &str,
) -> PyResult<PyObject> {
    let dict = PyDict::new_bound(py);
    dict.set_item("bucket", bucket)?;
    dict.set_item("key", key)?;
    dict.set_item("size", size)?;
    dict.set_item("content_type", content_type)?;
    dict.set_item("etag", etag)?;
    dict.set_item("updated_at", updated_at)?;
    Ok(dict.into())
}

fn validate_bucket(bucket: &str) -> PyResult<()> {
    ensure_utf8(bucket, "bucket")?;
    if bucket.is_empty()
        || bucket.contains('/')
        || bucket.contains('\\')
        || bucket == "."
        || bucket == ".."
    {
        return Err(storage_error(422, "Invalid bucket name"));
    }
    Ok(())
}

fn normalize_key(key: &str) -> PyResult<String> {
    ensure_utf8(key, "key")?;
    let normalized = key.trim_matches('/').to_string();
    if normalized.is_empty()
        || normalized.starts_with("../")
        || normalized.contains("/../")
        || normalized == ".."
    {
        return Err(storage_error(422, "Invalid object key"));
    }
    Ok(normalized)
}

fn ensure_utf8(value: &str, field_name: &str) -> PyResult<()> {
    let _ = (value, field_name);
    Ok(())
}

fn bucket_id_or_404_inner(conn: &Connection, bucket: &str) -> StorageResult<i64> {
    conn.query_row(
        "SELECT id FROM buckets WHERE name = ?1",
        params![bucket],
        |row| row.get(0),
    )
    .optional()
    .map_err(db_failure)?
    .ok_or_else(|| storage_failure(404, "Bucket not found"))
}

fn object_id_inner(tx: &Transaction<'_>, bucket_id: i64, key: &str) -> StorageResult<Option<i64>> {
    tx.query_row(
        "SELECT id FROM objects WHERE bucket_id = ?1 AND key = ?2",
        params![bucket_id, key],
        |row| row.get(0),
    )
    .optional()
    .map_err(db_failure)
}

fn object_record_inner(
    conn: &Connection,
    bucket_id: i64,
    key: &str,
) -> StorageResult<ObjectRecord> {
    conn.query_row(
        "SELECT key, size, content_type, etag, updated_at, file_path \
         FROM objects WHERE bucket_id = ?1 AND key = ?2",
        params![bucket_id, key],
        |row| {
            Ok(ObjectRecord {
                key: row.get(0)?,
                size: row.get(1)?,
                content_type: row.get(2)?,
                etag: row.get(3)?,
                updated_at: row.get(4)?,
                file_path: row.get(5)?,
            })
        },
    )
    .optional()
    .map_err(db_failure)?
    .ok_or_else(|| storage_failure(404, "Object not found"))
}

fn validate_bucket_inner(bucket: &str) -> StorageResult<()> {
    ensure_utf8_inner(bucket, "bucket")?;
    if bucket.is_empty()
        || bucket.contains('/')
        || bucket.contains('\\')
        || bucket == "."
        || bucket == ".."
    {
        return Err(storage_failure(422, "Invalid bucket name"));
    }
    Ok(())
}

fn normalize_key_inner(key: &str) -> StorageResult<String> {
    ensure_utf8_inner(key, "key")?;
    let normalized = key.trim_matches('/').to_string();
    if normalized.is_empty()
        || normalized.starts_with("../")
        || normalized.contains("/../")
        || normalized == ".."
    {
        return Err(storage_failure(422, "Invalid object key"));
    }
    Ok(normalized)
}

fn ensure_utf8_inner(value: &str, field_name: &str) -> StorageResult<()> {
    let _ = (value, field_name);
    Ok(())
}

fn now_utc() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Micros, true)
}

fn hex_sha256(body: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(body);
    format!("{:x}", hasher.finalize())
}

fn atomic_write_blob_inner(target_path: &Path, body: &[u8]) -> StorageResult<()> {
    if let Some(parent) = target_path.parent() {
        fs::create_dir_all(parent).map_err(io_failure)?;
    }
    let tmp_path = target_path.with_file_name(format!(
        ".{}.{}.tmp",
        target_path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("blob"),
        Uuid::new_v4().simple()
    ));
    let write_result = (|| -> std::io::Result<()> {
        let mut file = File::create(&tmp_path)?;
        file.write_all(body)?;
        file.sync_all()?;
        fs::rename(&tmp_path, target_path)?;
        Ok(())
    })();
    if write_result.is_err() && tmp_path.exists() {
        let _ = fs::remove_file(&tmp_path);
    }
    write_result.map_err(io_failure)
}

fn storage_failure(status: u16, detail: &str) -> StorageFailure {
    StorageFailure {
        status,
        detail: detail.to_string(),
    }
}

fn db_failure(err: rusqlite::Error) -> StorageFailure {
    storage_failure(500, &format!("SQLite error: {err}"))
}

fn io_failure(err: std::io::Error) -> StorageFailure {
    storage_failure(500, &format!("I/O error: {err}"))
}

fn storage_failure_to_py(err: StorageFailure) -> PyErr {
    storage_error(err.status, &err.detail)
}

fn storage_error(status: u16, detail: &str) -> PyErr {
    PyRuntimeError::new_err(format!("{ERR_PREFIX}{status}:{detail}"))
}

fn db_error(err: rusqlite::Error) -> PyErr {
    PyRuntimeError::new_err(format!("{ERR_PREFIX}500:SQLite error: {err}"))
}

fn io_error(err: std::io::Error) -> PyErr {
    PyRuntimeError::new_err(format!("{ERR_PREFIX}500:I/O error: {err}"))
}

fn http_error(err: reqwest::Error) -> PyErr {
    PyRuntimeError::new_err(format!("ALOCALS3_HTTP_ERROR:{err}"))
}

fn http_status_error(status: u16) -> PyErr {
    PyRuntimeError::new_err(format!("ALOCALS3_HTTP_STATUS:{status}"))
}

fn headers_to_dict(py: Python<'_>, headers: &reqwest::header::HeaderMap) -> PyResult<PyObject> {
    let dict = PyDict::new_bound(py);
    for (name, value) in headers.iter() {
        if let Ok(value) = value.to_str() {
            dict.set_item(name.as_str().to_ascii_lowercase(), value)?;
        }
    }
    Ok(dict.into())
}

fn bucket_path(bucket: &str) -> String {
    format!("/s3/{}", encode_segment(bucket))
}

fn object_path(bucket: &str, key: &str) -> String {
    format!("{}/{}", bucket_path(bucket), encode_key(key))
}

fn encode_segment(value: &str) -> String {
    percent_encode(value.as_bytes(), NON_ALPHANUMERIC).to_string()
}

fn encode_key(value: &str) -> String {
    percent_encode(value.as_bytes(), KEY_ENCODE_SET).to_string()
}
