// The first statement takes a database write lock before any snapshot read.
// PostgreSQL serializes only writers in this bucket; SQLite serializes writers.
macro_rules! database {
    ($storage:expr, $pool:ident, $body:block) => {
        match $storage {
            Storage::Sqlite { pool: $pool, .. } => $body,
            Storage::Postgres { pool: $pool, .. } => $body,
        }
    };
}
macro_rules! lock_bucket {
    ($tx:expr, $id:expr) => {{
        let result = sqlx::query("UPDATE buckets SET name = name WHERE id = $1")
            .bind($id)
            .execute(&mut *$tx)
            .await?;
        if result.rows_affected() == 0 {
            return Err(ApiError::new(StatusCode::NOT_FOUND, "Bucket not found"));
        }
    }};
}
// Caller owns a bucket write lock for the entire check-and-publish transaction.
macro_rules! publish_object {
    ($tx:expr, $id:expr, $key:expr, $path:expr, $info:expr, $conditions:expr) => {{
        let old = sqlx::query("SELECT etag, file_path FROM objects WHERE bucket_id = $1 AND key = $2")
            .bind($id).bind($key).fetch_optional(&mut *$tx).await?;
        let etag = old.as_ref().map(|r| r.get::<String, _>(0));
        $conditions.check(etag.as_deref())?;
        let metadata = serde_json::to_string(&$info.metadata)
            .map_err(|e| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        sqlx::query("INSERT INTO objects (bucket_id, key, file_path, size, content_type, etag, updated_at, created_at, metadata)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $7, $8)
            ON CONFLICT(bucket_id, key) DO UPDATE SET file_path = EXCLUDED.file_path,
            size = EXCLUDED.size, content_type = EXCLUDED.content_type, etag = EXCLUDED.etag,
            updated_at = EXCLUDED.updated_at, metadata = EXCLUDED.metadata")
            .bind($id).bind($key).bind($path).bind($info.size).bind(&$info.content_type)
            .bind(&$info.etag).bind(&$info.updated_at).bind(metadata).execute(&mut *$tx).await?;
        (old.is_none(), old.map(|r| r.get::<String, _>(1)).filter(|p| p != $path))
    }};
}

#[derive(Default)]
struct WriteConditions {
    if_match: Option<String>,
    if_none_match: Option<String>,
}
impl WriteConditions {
    fn from_headers(headers: &HeaderMap) -> Self {
        Self {
            if_match: header_str(headers, "if-match").map(str::to_owned),
            if_none_match: header_str(headers, "if-none-match").map(str::to_owned),
        }
    }
    fn check(&self, current: Option<&str>) -> ApiResult<()> {
        if self
            .if_match
            .as_ref()
            .is_some_and(|v| !current.is_some_and(|e| strong_etag_matches(v, e)))
            || self
                .if_none_match
                .as_ref()
                .is_some_and(|v| current.is_some_and(|e| strong_etag_matches(v, e)))
        {
            return Err(ApiError::new(
                StatusCode::PRECONDITION_FAILED,
                "Write precondition failed",
            ));
        }
        Ok(())
    }
}
fn strong_etag_matches(value: &str, etag: &str) -> bool {
    value
        .split(',')
        .map(str::trim)
        .any(|v| v == "*" || (!v.starts_with("W/") && v.trim_matches('"') == etag))
}

mod multipart;
mod verify;

use axum::body::Body;
use axum::extract::{OriginalUri, Path as AxumPath, Query, State};
use axum::http::{header, HeaderMap, HeaderValue, Request, StatusCode};
use axum::middleware::{from_fn, from_fn_with_state, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, put};
use axum::{Json, Router};
use base64::Engine;
use chrono::{DateTime, SecondsFormat, Utc};
use clap::Parser;
use futures_util::StreamExt;
use hmac::{Hmac, Mac};
use percent_encoding::percent_decode_str;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::postgres::{PgPoolOptions, PgRow};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{PgPool, Row, SqlitePool};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::{Component, Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use subtle::ConstantTimeEq;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::{Mutex, Semaphore};
use tokio::time::MissedTickBehavior;
use tokio_util::io::ReaderStream;
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

static HEALTH_COUNTER: AtomicUsize = AtomicUsize::new(1);
const DOWNLOAD_CHUNK_SIZE: usize = 1024 * 1024;
const PUBLIC_JSON_TRANSFORM_MAX_SIZE: u64 = 16 * 1024 * 1024;
const DEFAULT_MAX_CONCURRENT_UPLOADS: usize = 16;

fn version_info() -> &'static str {
    concat!(
        env!("CARGO_PKG_VERSION"),
        "\nauthors: ",
        env!("CARGO_PKG_AUTHORS")
    )
}

#[derive(Parser, Debug)]
#[command(
    name = "alocals3-server",
    version = version_info(),
    author = env!("CARGO_PKG_AUTHORS"),
    about = "Run the alocals3 Rust S3-compatible local object storage server"
)]
struct Args {
    #[arg(long, default_value = "127.0.0.1")]
    host: String,
    /// Inspect stored objects and exit (JSON report, exit 1 on corruption).
    #[arg(long)]
    verify: bool,
    /// Read and hash every blob during verification.
    #[arg(long)]
    verify_full: bool,
    /// Expire idle incomplete multipart uploads; 0 disables automatic cleanup.
    #[arg(long, env = "ALOCALS3_MULTIPART_IDLE_SECS", default_value_t = 0)]
    multipart_idle_secs: u64,
    #[arg(
        long,
        env = "ALOCALS3_MULTIPART_CLEANUP_INTERVAL_SECS",
        default_value_t = 60
    )]
    multipart_cleanup_interval_secs: u64,
    #[arg(long, default_value_t = 8000)]
    port: u16,
    #[arg(
        long,
        env = "ALOCALS3_DATABASE_URL",
        default_value = "sqlite:///./alocals3.db"
    )]
    database_url: String,
    #[arg(long, env = "ALOCALS3_STORAGE_ROOT", default_value = "./data")]
    storage_root: PathBuf,
    #[arg(long, env = "ALOCALS3_LOG_LEVEL", default_value = "info")]
    log_level: String,
    #[arg(long, env = "ALOCALS3_MAX_UPLOAD_SIZE", default_value_t = 0)]
    max_upload_size: usize,
    #[arg(
        long,
        env = "ALOCALS3_MAX_CONCURRENT_UPLOADS",
        default_value_t = DEFAULT_MAX_CONCURRENT_UPLOADS
    )]
    max_concurrent_uploads: usize,
    #[arg(long, env = "ALOCALS3_DISABLE_GC", default_value_t = false)]
    disable_gc: bool,
    #[arg(long, env = "ALOCALS3_GC_INTERVAL_SECS", default_value_t = 300)]
    gc_interval_secs: u64,
    #[arg(long, env = "ALOCALS3_GC_GRACE_SECS", default_value_t = 300)]
    gc_grace_secs: u64,
    #[arg(long, env = "ALOCALS3_GC_START_DELAY_SECS", default_value_t = 30)]
    gc_start_delay_secs: u64,
    #[arg(
        long,
        env = "ALOCALS3_AUTH_ENABLED",
        default_value_t = false,
        action = clap::ArgAction::Set
    )]
    auth_enabled: bool,
    #[arg(long, env = "ALOCALS3_SERVICE_TOKEN", default_value = "")]
    service_token: String,
    /// Public identity of the deployment embedding this generic storage service.
    #[arg(long, env = "ALOCALS3_HEALTH_SYSTEM", default_value = "alocals3")]
    health_system: String,
    #[arg(long, env = "ALOCALS3_USER_AUTH_SECRET", default_value = "")]
    user_auth_secret: String,
    #[arg(
        long,
        env = "ALOCALS3_OBJECT_AUTH_COOKIE",
        default_value = "alocals3-auth-cookie"
    )]
    object_auth_cookie: String,
}

#[derive(Clone)]
struct AppState {
    health_system: String,
    storage: Arc<Storage>,
    gc: Option<Arc<GarbageCollector>>,
    auth: Arc<AuthConfig>,
    max_upload_size: usize,
    upload_semaphore: Arc<Semaphore>,
}

struct AuthConfig {
    enabled: bool,
    service_token: String,
    user_auth_secret: String,
    object_auth_cookie: String,
}

struct GarbageCollector {
    storage: Arc<Storage>,
    interval: Duration,
    grace: Duration,
    start_delay: Duration,
    running: AtomicBool,
}

#[derive(Default)]
struct GcReport {
    orphan_files_deleted: u64,
    orphan_bytes_deleted: u64,
    missing_records_deleted: u64,
    stale_tmp_files_deleted: u64,
    empty_dirs_removed: u64,
}

enum Storage {
    Sqlite {
        pool: SqlitePool,
        objects_root: PathBuf,
        gc_state: Arc<GcState>,
    },
    Postgres {
        pool: PgPool,
        objects_root: PathBuf,
        gc_state: Arc<GcState>,
    },
}

struct GcState {
    active_writes: Mutex<HashMap<String, usize>>,
}

#[derive(Clone)]
struct BucketRow {
    id: i64,
}

#[derive(Clone)]
struct ObjectRow {
    key: String,
    file_path: String,
    size: i64,
    content_type: String,
    etag: String,
    updated_at: String,
    metadata: HashMap<String, String>,
}

struct GcObjectRef {
    id: i64,
    file_path: String,
    updated_at: String,
}

#[derive(Serialize)]
struct BucketInfo {
    name: String,
    created_at: String,
}

#[derive(Serialize, Clone)]
struct ObjectInfo {
    bucket: String,
    key: String,
    size: i64,
    content_type: String,
    etag: String,
    updated_at: String,
    metadata: HashMap<String, String>,
}

#[derive(Default, Deserialize)]
struct PublicObjectQuery {
    version: Option<String>,
}

struct StoredObject {
    info: ObjectInfo,
    body: Vec<u8>,
}

struct StoredObjectSource {
    info: ObjectInfo,
    path: PathBuf,
    size: u64,
}

struct StagedUpload {
    temp_path: PathBuf,
    size: i64,
    etag: String,
    sha256: String,
}

impl Drop for StagedUpload {
    fn drop(&mut self) {
        if !self.temp_path.as_os_str().is_empty() {
            let _ = std::fs::remove_file(&self.temp_path);
        }
    }
}

struct TempFileGuard {
    path: Option<PathBuf>,
}

impl TempFileGuard {
    fn new(path: PathBuf) -> Self {
        Self { path: Some(path) }
    }

    fn into_path(mut self) -> PathBuf {
        self.path.take().expect("temporary upload path is present")
    }
}

impl Drop for TempFileGuard {
    fn drop(&mut self) {
        if let Some(path) = self.path.take() {
            let _ = std::fs::remove_file(path);
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
struct ListObjectsV2Json {
    name: String,
    prefix: String,
    delimiter: String,
    max_keys: i64,
    key_count: i64,
    is_truncated: bool,
    next_continuation_token: Option<String>,
    contents: Vec<ObjectInfo>,
    common_prefixes: Vec<String>,
}

#[derive(Clone)]
struct ListObjectsV2Result {
    bucket: String,
    prefix: String,
    delimiter: String,
    max_keys: i64,
    key_count: i64,
    is_truncated: bool,
    next_continuation_token: Option<String>,
    contents: Vec<ObjectInfo>,
    common_prefixes: Vec<String>,
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    detail: String,
}

impl ApiError {
    fn new(status: StatusCode, detail: impl Into<String>) -> Self {
        Self {
            status,
            detail: detail.into(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, self.detail).into_response()
    }
}

type ApiResult<T> = Result<T, ApiError>;

impl GarbageCollector {
    fn start(
        storage: Arc<Storage>,
        interval: Duration,
        grace: Duration,
        start_delay: Duration,
    ) -> Arc<Self> {
        let gc = Arc::new(Self {
            storage,
            interval,
            grace,
            start_delay,
            running: AtomicBool::new(false),
        });
        let task_gc = Arc::clone(&gc);
        tokio::spawn(async move {
            task_gc.run_loop().await;
        });
        gc
    }

    async fn run_loop(self: Arc<Self>) {
        if !self.start_delay.is_zero() {
            tokio::time::sleep(self.start_delay).await;
        }
        self.run_once("startup").await;

        let mut interval = tokio::time::interval(self.interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        interval.tick().await;
        loop {
            interval.tick().await;
            self.run_once("periodic").await;
        }
    }

    fn reclaim_path(self: &Arc<Self>, relative_path: String) {
        let gc = Arc::clone(self);
        tokio::spawn(async move {
            // Keep the same grace window as the full GC so an in-flight GET that
            // resolved the previous database row can still open the old blob.
            if !gc.grace.is_zero() {
                tokio::time::sleep(gc.grace).await;
            }
            match gc.storage.reclaim_unreferenced_path(&relative_path).await {
                Ok(Some(bytes)) => tracing::info!(
                    file_path = %relative_path,
                    orphan_bytes_deleted = bytes,
                    "targeted gc completed"
                ),
                Ok(None) => tracing::debug!(file_path = %relative_path, "targeted gc skipped"),
                Err(err) => tracing::warn!(
                    file_path = %relative_path,
                    error = %err.detail,
                    "targeted gc failed"
                ),
            }
        });
    }

    async fn run_once(&self, reason: &'static str) {
        if self.running.swap(true, Ordering::AcqRel) {
            tracing::debug!(reason, "gc skipped because another run is active");
            return;
        }

        let started = Instant::now();
        let result = self.storage.garbage_collect(self.grace).await;
        self.running.store(false, Ordering::Release);

        match result {
            Ok(report) => {
                tracing::info!(
                    reason,
                    duration_ms = started.elapsed().as_secs_f64() * 1000.0,
                    orphan_files_deleted = report.orphan_files_deleted,
                    orphan_bytes_deleted = report.orphan_bytes_deleted,
                    missing_records_deleted = report.missing_records_deleted,
                    stale_tmp_files_deleted = report.stale_tmp_files_deleted,
                    empty_dirs_removed = report.empty_dirs_removed,
                    "gc completed"
                );
            }
            Err(err) => {
                tracing::warn!(
                    reason,
                    duration_ms = started.elapsed().as_secs_f64() * 1000.0,
                    error = %err.detail,
                    "gc failed"
                );
            }
        }
    }
}

fn init_logging(log_level: &str) -> anyhow::Result<()> {
    let filter = EnvFilter::try_new(log_level).or_else(|_| EnvFilter::try_new("info"))?;
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        // A detached launcher may close both output pipes. Reporting a failed
        // log write to stderr can then panic inside a request task, dropping
        // its HTTP connection. Logging failures must never abort storage I/O.
        .log_internal_errors(false)
        .try_init()
        .map_err(|err| anyhow::anyhow!("failed to initialize logging: {err}"))?;
    Ok(())
}

fn database_backend(database_url: &str) -> &'static str {
    if database_url.starts_with("sqlite") {
        "sqlite"
    } else if database_url.starts_with("postgresql") || database_url.starts_with("postgres") {
        "postgresql"
    } else {
        "unknown"
    }
}

async fn request_logging(request: Request<Body>, next: Next) -> Response {
    let method = request.method().clone();
    let path = request.uri().path().to_string();
    let should_log =
        path == "/healthz" || path.starts_with("/s3") || path.starts_with("/api/object/");
    if !should_log {
        return next.run(request).await;
    }

    let started = Instant::now();
    let response = next.run(request).await;
    let duration_ms = started.elapsed().as_secs_f64() * 1000.0;
    let status = response.status();

    if path == "/healthz" {
        let hit = HEALTH_COUNTER.fetch_add(1, Ordering::Relaxed);
        let sampled =
            status.is_client_error() || status.is_server_error() || hit.is_multiple_of(30);
        if sampled {
            log_by_status(
                status,
                format_args!(
                    "health method={} status={} duration_ms={:.2} sampled={}",
                    method,
                    status.as_u16(),
                    duration_ms,
                    sampled
                ),
            );
        }
        return response;
    }

    let (bucket, key) = extract_bucket_key(&path);
    log_by_status(
        status,
        format_args!(
            "s3 method={} status={} duration_ms={:.2} bucket={} key={} path={}",
            method,
            status.as_u16(),
            duration_ms,
            bucket,
            key,
            path
        ),
    );
    response
}

fn log_by_status(status: StatusCode, args: std::fmt::Arguments<'_>) {
    if status.is_server_error() {
        tracing::error!("{args}");
    } else if status.is_client_error() {
        tracing::warn!("{args}");
    } else {
        tracing::info!("{args}");
    }
}

fn extract_bucket_key(path: &str) -> (&str, String) {
    let parts = path
        .split('/')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    let prefix_len = if parts.first() == Some(&"s3") {
        1
    } else if parts.get(0..2) == Some(&["api", "object"][..]) {
        2
    } else {
        return ("-", "-".to_string());
    };
    if parts.len() <= prefix_len {
        return ("-", "-".to_string());
    }
    let bucket = parts[prefix_len];
    if parts.len() == prefix_len + 1 || (prefix_len == 1 && parts.get(2) == Some(&"objects")) {
        return (bucket, "-".to_string());
    }
    (bucket, parts[prefix_len + 1..].join("/"))
}

fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    header_str(headers, header::AUTHORIZATION.as_str())
        .and_then(|value| {
            value
                .strip_prefix("Bearer ")
                .or_else(|| value.strip_prefix("bearer "))
        })
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn cookie_token<'a>(headers: &'a HeaderMap, cookie_name: &str) -> Option<&'a str> {
    let cookie = header_str(headers, header::COOKIE.as_str())?;
    cookie.split(';').find_map(|part| {
        let (name, value) = part.trim().split_once('=')?;
        (name == cookie_name && !value.is_empty()).then_some(value)
    })
}

fn constant_time_equal(left: &str, right: &str) -> bool {
    left.len() == right.len() && bool::from(left.as_bytes().ct_eq(right.as_bytes()))
}

fn valid_user_access_token(token: &str, secret: &str) -> bool {
    if token.is_empty() || secret.is_empty() {
        return false;
    }
    let Some((payload, supplied_signature)) = token.split_once('.') else {
        return false;
    };
    type HmacSha256 = Hmac<Sha256>;
    let Ok(mut mac) = HmacSha256::new_from_slice(secret.as_bytes()) else {
        return false;
    };
    mac.update(payload.as_bytes());
    let expected_signature = mac
        .finalize()
        .into_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    if !constant_time_equal(&expected_signature, supplied_signature) {
        return false;
    }
    let Ok(decoded) = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(payload) else {
        return false;
    };
    let Ok(raw) = std::str::from_utf8(&decoded) else {
        return false;
    };
    let mut parts = raw.splitn(4, '|');
    let username = parts.next().unwrap_or_default();
    let _access = parts.next().unwrap_or_default();
    let expires = parts
        .next()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    let nonce = parts.next().unwrap_or_default();
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(u64::MAX);
    !username.is_empty() && !nonce.is_empty() && expires >= now
}

fn unauthorized_response(detail: &'static str) -> Response {
    let mut response = (StatusCode::UNAUTHORIZED, detail).into_response();
    response
        .headers_mut()
        .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
    response
}

async fn request_authorization(
    State(state): State<AppState>,
    request: Request<Body>,
    next: Next,
) -> Response {
    if !state.auth.enabled {
        return next.run(request).await;
    }
    let path = request.uri().path();
    if path.starts_with("/s3") {
        if !state.auth.service_token.is_empty()
            && !bearer_token(request.headers())
                .map(|value| constant_time_equal(value, &state.auth.service_token))
                .unwrap_or(false)
        {
            return unauthorized_response("Invalid alocals3 service credential");
        }
    } else if path.starts_with("/api/object/") {
        if state.auth.user_auth_secret.is_empty() {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                "alocals3 public object authentication is not configured",
            )
                .into_response();
        }
        let token = bearer_token(request.headers())
            .or_else(|| cookie_token(request.headers(), &state.auth.object_auth_cookie));
        if !token
            .map(|value| valid_user_access_token(value, &state.auth.user_auth_secret))
            .unwrap_or(false)
        {
            return unauthorized_response("Invalid or expired object access credential");
        }
    }
    next.run(request).await
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    if args.verify || args.verify_full {
        let storage = verify::connect(&args.database_url, args.storage_root).await?;
        let report = verify::run(&storage, args.verify_full)
            .await
            .map_err(|e| anyhow::anyhow!(e.detail))?;
        println!("{}", serde_json::to_string(&report)?);
        if !report.issues.is_empty() {
            std::process::exit(1);
        }
        return Ok(());
    }
    if args.auth_enabled && args.service_token.trim().is_empty() {
        anyhow::bail!("--service-token is required when --auth-enabled=true");
    }
    if args.auth_enabled && args.user_auth_secret.trim().is_empty() {
        anyhow::bail!("--user-auth-secret is required when --auth-enabled=true");
    }
    init_logging(&args.log_level)?;
    tracing::info!(
        host = %args.host,
        port = args.port,
        database_backend = database_backend(&args.database_url),
        storage_root = %args.storage_root.display(),
        max_upload_size = args.max_upload_size,
        max_concurrent_uploads = args.max_concurrent_uploads.max(1),
        disable_gc = args.disable_gc,
        gc_interval_secs = args.gc_interval_secs,
        gc_grace_secs = args.gc_grace_secs,
        gc_start_delay_secs = args.gc_start_delay_secs,
        auth_enabled = args.auth_enabled,
        "starting alocals3 server"
    );
    let storage = Arc::new(Storage::connect(&args.database_url, args.storage_root).await?);
    if args.multipart_idle_secs > 0 {
        multipart::start_cleanup(
            storage.clone(),
            Duration::from_secs(args.multipart_idle_secs),
            Duration::from_secs(args.multipart_cleanup_interval_secs.max(1)),
        );
    }
    let gc = if args.disable_gc || args.gc_interval_secs == 0 {
        tracing::info!("background gc disabled");
        None
    } else {
        Some(GarbageCollector::start(
            Arc::clone(&storage),
            Duration::from_secs(args.gc_interval_secs),
            Duration::from_secs(args.gc_grace_secs),
            Duration::from_secs(args.gc_start_delay_secs),
        ))
    };
    let state = AppState {
        health_system: args.health_system,
        storage,
        gc,
        auth: Arc::new(AuthConfig {
            enabled: args.auth_enabled,
            service_token: args.service_token,
            user_auth_secret: args.user_auth_secret,
            object_auth_cookie: args.object_auth_cookie,
        }),
        max_upload_size: args.max_upload_size,
        upload_semaphore: Arc::new(Semaphore::new(args.max_concurrent_uploads.max(1))),
    };
    let app = Router::new()
        .route("/healthz", get(health))
        .route("/s3", get(list_buckets))
        .route(
            "/s3-multipart/:bucket/*key",
            axum::routing::post(multipart::create),
        )
        .route("/s3-uploads", get(multipart::list_uploads))
        .route(
            "/s3-uploads/abort",
            axum::routing::post(multipart::abort_many),
        )
        .route(
            "/s3-uploads/:upload_id",
            get(multipart::list)
                .delete(multipart::abort)
                .post(multipart::complete)
                .layer(axum::extract::DefaultBodyLimit::disable()),
        )
        .route(
            "/s3-uploads/:upload_id/:part_number",
            put(multipart::upload_part),
        )
        .route(
            "/s3/:bucket",
            put(create_bucket)
                .delete(delete_bucket)
                .get(list_objects_v2),
        )
        .route("/s3/:bucket/objects", get(list_objects))
        .route(
            "/s3/:bucket/*key",
            put(put_object)
                .get(get_object)
                .head(head_object)
                .delete(delete_object),
        )
        .route(
            "/api/object/:bucket/*key",
            get(get_public_object).head(head_public_object),
        )
        .layer(from_fn_with_state(state.clone(), request_authorization))
        .layer(from_fn(request_logging))
        .with_state(state);

    let addr: SocketAddr = format!("{}:{}", args.host, args.port).parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "alocals3 server listening");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn health(State(state): State<AppState>) -> Json<serde_json::Value> {
    Json(serde_json::json!({"status": "ok", "system": state.health_system, "module": "alocals3"}))
}

async fn list_buckets(State(state): State<AppState>) -> ApiResult<Json<Vec<BucketInfo>>> {
    Ok(Json(state.storage.list_buckets().await?))
}

async fn create_bucket(
    State(state): State<AppState>,
    AxumPath(bucket): AxumPath<String>,
) -> ApiResult<(StatusCode, Json<BucketInfo>)> {
    Ok((
        StatusCode::CREATED,
        Json(state.storage.create_bucket(&bucket).await?),
    ))
}

async fn delete_bucket(
    State(state): State<AppState>,
    AxumPath(bucket): AxumPath<String>,
) -> ApiResult<StatusCode> {
    let uploads = multipart::bucket_sessions(&state.storage, &bucket).await?;
    state.storage.delete_bucket(&bucket).await?;
    multipart::clean_bucket_sessions(&state.storage, &uploads).await;
    Ok(StatusCode::NO_CONTENT)
}

async fn list_objects(
    State(state): State<AppState>,
    AxumPath(bucket): AxumPath<String>,
    Query(params): Query<HashMap<String, String>>,
) -> ApiResult<Json<Vec<ObjectInfo>>> {
    let prefix = params.get("prefix").cloned();
    let limit = params
        .get("limit")
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(1000)
        .clamp(1, 10000);
    Ok(Json(
        state
            .storage
            .list_objects(&bucket, prefix.as_deref(), limit)
            .await?,
    ))
}

async fn list_objects_v2(
    State(state): State<AppState>,
    AxumPath(bucket): AxumPath<String>,
    Query(params): Query<HashMap<String, String>>,
) -> ApiResult<Response> {
    if params.get("list-type").map(String::as_str).unwrap_or("2") != "2" {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "Only list-type=2 is supported",
        ));
    }
    let prefix = params.get("prefix").cloned().unwrap_or_default();
    let delimiter = params.get("delimiter").cloned().unwrap_or_default();
    let max_keys = params
        .get("max-keys")
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(1000)
        .clamp(0, 10000);
    let continuation_token = params.get("continuation-token").cloned();
    let result = state
        .storage
        .list_objects_v2(
            &bucket,
            &prefix,
            &delimiter,
            max_keys,
            continuation_token.as_deref(),
        )
        .await?;
    if params
        .get("output")
        .map(|v| v.eq_ignore_ascii_case("json"))
        .unwrap_or(false)
    {
        let payload = ListObjectsV2Json {
            name: result.bucket,
            prefix: result.prefix,
            delimiter: result.delimiter,
            max_keys: result.max_keys,
            key_count: result.key_count,
            is_truncated: result.is_truncated,
            next_continuation_token: result.next_continuation_token,
            contents: result.contents,
            common_prefixes: result.common_prefixes,
        };
        return Ok(Json(payload).into_response());
    }
    let xml = list_objects_v2_xml(&result);
    Ok(([(header::CONTENT_TYPE, "application/xml")], xml).into_response())
}

async fn put_object(
    State(state): State<AppState>,
    AxumPath((bucket, key)): AxumPath<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Body,
) -> ApiResult<Response> {
    if let Some(copy_source) = header_str(&headers, "x-amz-copy-source") {
        let (source_bucket, source_key) = parse_copy_source(copy_source)?;
        let directive = header_str(&headers, "x-amz-metadata-directive")
            .unwrap_or("COPY")
            .to_ascii_uppercase();
        if directive != "COPY" && directive != "REPLACE" {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "x-amz-metadata-directive must be COPY or REPLACE",
            ));
        }
        let replacement_metadata = if directive == "REPLACE" {
            Some(extract_user_metadata(&headers)?)
        } else {
            None
        };
        let replacement_content_type = if directive == "REPLACE" {
            header_str(&headers, header::CONTENT_TYPE.as_str())
        } else {
            None
        };
        let (info, retired_path) = state
            .storage
            .copy_object(
                &source_bucket,
                &source_key,
                &bucket,
                &key,
                replacement_content_type,
                replacement_metadata,
            )
            .await?;
        if let (Some(gc), Some(path)) = (&state.gc, retired_path) {
            gc.reclaim_path(path);
        }
        let mut response = if params
            .get("output")
            .map(|value| value.eq_ignore_ascii_case("json"))
            .unwrap_or(false)
        {
            Json(info.clone()).into_response()
        } else {
            (
                [(header::CONTENT_TYPE, "application/xml")],
                copy_object_result_xml(&info),
            )
                .into_response()
        };
        response.headers_mut().insert(
            header::ETAG,
            HeaderValue::from_str(&format!("\"{}\"", info.etag)).unwrap(),
        );
        return Ok(response);
    }

    validate_content_length(&headers, state.max_upload_size)?;
    let expected_md5 = parse_content_md5(&headers)?;
    let conditions = WriteConditions::from_headers(&headers);
    let content_type = header_str(&headers, header::CONTENT_TYPE.as_str()).map(str::to_string);
    let metadata = extract_user_metadata(&headers)?;
    let _upload_permit = state.upload_semaphore.acquire().await.map_err(|_| {
        ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "Upload service unavailable",
        )
    })?;
    let staged = stage_upload(
        &state.storage.objects_root(),
        body,
        state.max_upload_size,
        expected_md5,
    )
    .await?;
    let (info, created, retired_path) = state
        .storage
        .put_staged_object(
            &bucket,
            &key,
            staged,
            content_type.as_deref(),
            metadata,
            &conditions,
        )
        .await?;
    if let (Some(gc), Some(path)) = (&state.gc, retired_path) {
        gc.reclaim_path(path);
    }
    let mut response = (
        if created {
            StatusCode::CREATED
        } else {
            StatusCode::OK
        },
        Json(info.clone()),
    )
        .into_response();
    response.headers_mut().insert(
        header::ETAG,
        HeaderValue::from_str(&format!("\"{}\"", info.etag)).unwrap(),
    );
    Ok(response)
}

async fn get_object(
    State(state): State<AppState>,
    AxumPath((bucket, key)): AxumPath<(String, String)>,
    headers: HeaderMap,
) -> ApiResult<Response> {
    stream_object_response(
        state.storage.get_object_source(&bucket, &key).await?,
        &headers,
    )
    .await
}

async fn get_public_object(
    State(state): State<AppState>,
    AxumPath((bucket, key)): AxumPath<(String, String)>,
    OriginalUri(uri): OriginalUri,
    Query(query): Query<PublicObjectQuery>,
    headers: HeaderMap,
) -> ApiResult<Response> {
    let mut source = state.storage.get_object_source(&bucket, &key).await?;
    let object_version = source.info.etag.clone();
    if query.version.as_deref() != Some(object_version.as_str()) {
        return Ok(public_object_version_redirect(&uri, &object_version));
    }
    let lower_key = source.info.key.to_ascii_lowercase();
    source.info.content_type =
        public_content_type(&lower_key, &source.info.content_type).to_string();
    let terrain_gzip = lower_key.ends_with(".terrain") && file_starts_with_gzip(&source.path).await;
    let mut response = if public_object_requires_transform(&lower_key) {
        if source.size > PUBLIC_JSON_TRANSFORM_MAX_SIZE {
            return Err(ApiError::new(
                StatusCode::PAYLOAD_TOO_LARGE,
                format!(
                    "Public JSON transformation is limited to {PUBLIC_JSON_TRANSFORM_MAX_SIZE} bytes"
                ),
            ));
        }
        let body = tokio::fs::read(&source.path)
            .await
            .map_err(|_| ApiError::new(StatusCode::NOT_FOUND, "Object data missing"))?;
        let mut object = StoredObject {
            info: source.info,
            body,
        };
        prepare_public_object(&mut object);
        object_response(object, &headers, true)?
    } else {
        stream_object_response(source, &headers).await?
    };
    apply_public_object_headers(&mut response, terrain_gzip);
    Ok(response)
}

fn public_object_requires_transform(lower_key: &str) -> bool {
    lower_key.ends_with("/3dtiles/tileset.json") || lower_key.ends_with("/terrain/layer.json")
}

async fn file_starts_with_gzip(path: &Path) -> bool {
    let Ok(mut file) = tokio::fs::File::open(path).await else {
        return false;
    };
    let mut magic = [0_u8; 2];
    file.read_exact(&mut magic).await.is_ok() && magic == [0x1f, 0x8b]
}

async fn head_object(
    State(state): State<AppState>,
    AxumPath((bucket, key)): AxumPath<(String, String)>,
    headers: HeaderMap,
) -> ApiResult<Response> {
    let info = state
        .storage
        .get_object_info(&bucket, &key)
        .await?
        .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "Object not found"))?;
    object_head_response(info, &headers)
}

async fn head_public_object(
    State(state): State<AppState>,
    AxumPath((bucket, key)): AxumPath<(String, String)>,
    OriginalUri(uri): OriginalUri,
    Query(query): Query<PublicObjectQuery>,
    headers: HeaderMap,
) -> ApiResult<Response> {
    let mut info = state
        .storage
        .get_object_info(&bucket, &key)
        .await?
        .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "Object not found"))?;
    if query.version.as_deref() != Some(info.etag.as_str()) {
        return Ok(public_object_version_redirect(&uri, &info.etag));
    }
    let lower_key = info.key.to_ascii_lowercase();
    info.content_type = public_content_type(&lower_key, &info.content_type).to_string();
    let mut response = object_head_response(info, &headers)?;
    apply_public_object_headers(&mut response, false);
    Ok(response)
}

fn prepare_public_object(object: &mut StoredObject) {
    let lower_key = object.info.key.to_ascii_lowercase();
    if lower_key.ends_with("/3dtiles/tileset.json") {
        sanitize_tileset_json(&mut object.body);
    } else if lower_key.ends_with("/terrain/layer.json") {
        sanitize_terrain_layer_json(&mut object.body);
    }
    object.info.content_type =
        public_content_type(&lower_key, &object.info.content_type).to_string();
    object.info.size = object.body.len() as i64;
    object.info.etag = format!("{:x}", Sha256::digest(&object.body));
}

fn public_content_type<'a>(key: &str, fallback: &'a str) -> &'a str {
    if key.ends_with(".png") {
        "image/png"
    } else if key.ends_with(".jpg") || key.ends_with(".jpeg") {
        "image/jpeg"
    } else if key.ends_with(".webp") {
        "image/webp"
    } else if key.ends_with(".gif") {
        "image/gif"
    } else if key.ends_with(".svg") {
        "image/svg+xml"
    } else if key.ends_with(".terrain") {
        "application/vnd.quantized-mesh"
    } else if key.ends_with(".json") {
        "application/json"
    } else if key.ends_with(".gml") || key.ends_with(".citygml") {
        "application/gml+xml"
    } else if key.ends_with(".dae") {
        "model/vnd.collada+xml"
    } else if key.ends_with(".glb") {
        "model/gltf-binary"
    } else if key.ends_with(".gltf") {
        "model/gltf+json"
    } else {
        fallback
    }
}

fn sanitize_tileset_json(body: &mut Vec<u8>) {
    let Ok(mut payload) = serde_json::from_slice::<serde_json::Value>(body) else {
        return;
    };
    fn clean(tile: &mut serde_json::Value) -> bool {
        let Some(tile) = tile.as_object_mut() else {
            return false;
        };
        let mut changed = false;
        if let Some(content) = tile
            .get_mut("content")
            .and_then(|value| value.as_object_mut())
        {
            changed |= content.remove("url").is_some();
        }
        if let Some(contents) = tile
            .get_mut("contents")
            .and_then(|value| value.as_array_mut())
        {
            for content in contents {
                if let Some(content) = content.as_object_mut() {
                    changed |= content.remove("url").is_some();
                }
            }
        }
        if let Some(children) = tile
            .get_mut("children")
            .and_then(|value| value.as_array_mut())
        {
            for child in children {
                changed |= clean(child);
            }
        }
        changed
    }
    let changed = payload.get_mut("root").map(clean).unwrap_or(false);
    if changed {
        if let Ok(encoded) = serde_json::to_vec(&payload) {
            *body = encoded;
        }
    }
}

fn sanitize_terrain_layer_json(body: &mut Vec<u8>) {
    let Ok(mut payload) = serde_json::from_slice::<serde_json::Value>(body) else {
        return;
    };
    let Some(object) = payload.as_object_mut() else {
        return;
    };
    if object
        .get("metadataAvailability")
        .is_some_and(|value| !value.is_null())
        && object.get("available").is_some()
    {
        object.remove("metadataAvailability");
        if let Ok(encoded) = serde_json::to_vec(&payload) {
            *body = encoded;
        }
    }
}

fn public_object_version_redirect(uri: &http::Uri, version: &str) -> Response {
    let mut response = StatusCode::TEMPORARY_REDIRECT.into_response();
    response.headers_mut().insert(
        header::LOCATION,
        HeaderValue::from_str(&format!("{}?version={version}", uri.path())).unwrap(),
    );
    // Never cache the path-only redirect: the object assigned to a bucket/key
    // can change, in which case the canonical versioned URL changes with it.
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("private, no-store"),
    );
    response.headers_mut().insert(
        header::VARY,
        HeaderValue::from_static("Authorization, Cookie"),
    );
    response
}

fn apply_public_object_headers(response: &mut Response, terrain_gzip: bool) {
    // The handler only calls this for a URL whose version query matches the
    // current object's ETag. That makes the complete URL content-addressed and
    // therefore safe to retain indefinitely in a private browser cache.
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("private, max-age=31536000, immutable"),
    );
    // Private browser caches do not automatically include credentials in the
    // cache key. Keep cached authenticated responses separated by credential.
    response.headers_mut().insert(
        header::VARY,
        HeaderValue::from_static("Authorization, Cookie"),
    );
    if terrain_gzip {
        response
            .headers_mut()
            .insert(header::CONTENT_ENCODING, HeaderValue::from_static("gzip"));
    }
}

async fn delete_object(
    State(state): State<AppState>,
    AxumPath((bucket, key)): AxumPath<(String, String)>,
) -> ApiResult<StatusCode> {
    let retired_path = state.storage.delete_object(&bucket, &key).await?;
    if let Some(gc) = &state.gc {
        gc.reclaim_path(retired_path);
    }
    Ok(StatusCode::NO_CONTENT)
}

impl Storage {
    async fn connect(database_url: &str, root: PathBuf) -> anyhow::Result<Self> {
        let objects_root = root.join("objects");
        tokio::fs::create_dir_all(&objects_root).await?;
        let gc_state = Arc::new(GcState {
            active_writes: Mutex::new(HashMap::new()),
        });
        if database_url.starts_with("sqlite") {
            let path = sqlite_path(database_url);
            if let Some(parent) = path.parent() {
                if !parent.as_os_str().is_empty() {
                    tokio::fs::create_dir_all(parent).await?;
                }
            }
            let options = SqliteConnectOptions::from_str(&format!("sqlite://{}", path.display()))?
                .create_if_missing(true)
                .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
                .synchronous(sqlx::sqlite::SqliteSynchronous::Normal)
                .busy_timeout(std::time::Duration::from_secs(10));
            let pool = SqlitePoolOptions::new()
                .max_connections(32)
                .connect_with(options)
                .await?;
            init_sqlite(&pool).await?;
            return Ok(Self::Sqlite {
                pool,
                objects_root,
                gc_state,
            });
        }
        let pool = PgPoolOptions::new()
            .max_connections(32)
            .connect(database_url)
            .await?;
        init_pg(&pool).await?;
        Ok(Self::Postgres {
            pool,
            objects_root,
            gc_state,
        })
    }

    async fn list_buckets(&self) -> ApiResult<Vec<BucketInfo>> {
        match self {
            Self::Sqlite { pool, .. } => {
                let rows = sqlx::query("SELECT name, created_at FROM buckets ORDER BY name ASC")
                    .fetch_all(pool)
                    .await?;
                Ok(rows
                    .into_iter()
                    .map(|row| BucketInfo {
                        name: row.get(0),
                        created_at: row.get(1),
                    })
                    .collect())
            }
            Self::Postgres { pool, .. } => {
                let rows = sqlx::query("SELECT name, created_at FROM buckets ORDER BY name ASC")
                    .fetch_all(pool)
                    .await?;
                Ok(rows
                    .into_iter()
                    .map(|row| BucketInfo {
                        name: row.get(0),
                        created_at: row.get(1),
                    })
                    .collect())
            }
        }
    }

    async fn create_bucket(&self, bucket: &str) -> ApiResult<BucketInfo> {
        validate_bucket(bucket)?;
        let now = now_utc();
        match self {
            Self::Sqlite { pool, .. } => {
                let result =
                    sqlx::query("INSERT OR IGNORE INTO buckets (name, created_at) VALUES (?1, ?2)")
                        .bind(bucket)
                        .bind(&now)
                        .execute(pool)
                        .await?;
                if result.rows_affected() == 0 {
                    return Err(ApiError::new(StatusCode::CONFLICT, "Bucket already exists"));
                }
            }
            Self::Postgres { pool, .. } => {
                let result = sqlx::query(
                    "INSERT INTO buckets (name, created_at) VALUES ($1, $2) ON CONFLICT DO NOTHING",
                )
                .bind(bucket)
                .bind(&now)
                .execute(pool)
                .await?;
                if result.rows_affected() == 0 {
                    return Err(ApiError::new(StatusCode::CONFLICT, "Bucket already exists"));
                }
            }
        }
        Ok(BucketInfo {
            name: bucket.to_string(),
            created_at: now,
        })
    }

    async fn delete_bucket(&self, bucket: &str) -> ApiResult<()> {
        validate_bucket(bucket)?;
        let bucket_row = self.bucket_row(bucket).await?;
        database!(self, pool, {
            let mut tx = pool.begin().await?;
            lock_bucket!(tx, bucket_row.id);
            let count: i64 = sqlx::query("SELECT COUNT(*) FROM objects WHERE bucket_id = $1")
                .bind(bucket_row.id)
                .fetch_one(&mut *tx)
                .await?
                .get(0);
            if count > 0 {
                return Err(ApiError::new(StatusCode::CONFLICT, "Bucket is not empty"));
            }
            sqlx::query("DELETE FROM buckets WHERE id = $1")
                .bind(bucket_row.id)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            Ok(())
        })
    }

    async fn list_objects(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        limit: i64,
    ) -> ApiResult<Vec<ObjectInfo>> {
        let bucket_row = self.bucket_row(bucket).await?;
        Ok(self
            .object_page(
                bucket_row.id,
                prefix.unwrap_or(""),
                prefix.unwrap_or(""),
                true,
                limit,
            )
            .await?
            .into_iter()
            .map(|row| row.info(bucket))
            .collect())
    }

    async fn list_objects_v2(
        &self,
        bucket: &str,
        prefix: &str,
        delimiter: &str,
        max_keys: i64,
        continuation_token: Option<&str>,
    ) -> ApiResult<ListObjectsV2Result> {
        let bucket_id = self.bucket_row(bucket).await?.id;
        let mut cursor = match continuation_token {
            Some(value) => ListCursor::decode(value, bucket, prefix, delimiter)?,
            None => ListCursor {
                bucket: bucket.to_owned(),
                prefix: prefix.to_owned(),
                delimiter: delimiter.to_owned(),
                lower: prefix.to_owned(),
                inclusive: true,
            },
        };
        let mut contents = Vec::new();
        let mut common_prefixes = Vec::new();
        let mut next_token = None;
        let mut exhausted = max_keys == 0;
        while !exhausted {
            let rows = self
                .object_page(bucket_id, prefix, &cursor.lower, cursor.inclusive, 256)
                .await?;
            if rows.is_empty() {
                break;
            }
            for row in rows {
                if row.key < cursor.lower || (!cursor.inclusive && row.key == cursor.lower) {
                    continue;
                }
                if contents.len() + common_prefixes.len() >= max_keys as usize {
                    next_token = Some(cursor.encode());
                    exhausted = true;
                    break;
                }
                if let Some(pos) = (!delimiter.is_empty())
                    .then(|| row.key[prefix.len()..].find(delimiter))
                    .flatten()
                {
                    let cp = row.key[..prefix.len() + pos + delimiter.len()].to_owned();
                    // Seek past the entire directory, including entries not in this SQL page.
                    match prefix_upper_bound(&cp) {
                        Some(upper) => {
                            cursor.lower = upper;
                            cursor.inclusive = true;
                        }
                        None => exhausted = true,
                    }
                    common_prefixes.push(cp);
                } else {
                    cursor.lower = row.key.clone();
                    cursor.inclusive = false;
                    contents.push(row.info(bucket));
                }
                if exhausted {
                    break;
                }
            }
        }
        Ok(ListObjectsV2Result {
            bucket: bucket.to_owned(),
            prefix: prefix.to_owned(),
            delimiter: delimiter.to_owned(),
            max_keys,
            key_count: (contents.len() + common_prefixes.len()) as i64,
            is_truncated: next_token.is_some(),
            next_continuation_token: next_token,
            contents,
            common_prefixes,
        })
    }

    async fn object_page(
        &self,
        bucket_id: i64,
        prefix: &str,
        lower: &str,
        inclusive: bool,
        limit: i64,
    ) -> ApiResult<Vec<ObjectRow>> {
        let collation = match self {
            Self::Sqlite { .. } => "BINARY",
            Self::Postgres { .. } => "\"C\"",
        };
        let key = format!("key COLLATE {collation}");
        let comparison = if inclusive { ">=" } else { ">" };
        let query = format!(
            "SELECT key, file_path, size, content_type, etag, updated_at, metadata FROM objects
            WHERE bucket_id = $1 AND {key} >= $2 AND (CAST($3 AS TEXT) IS NULL OR {key} < $3)
            AND {key} {comparison} $4 ORDER BY {key} ASC LIMIT $5"
        );
        let upper = prefix_upper_bound(prefix);
        database!(self, pool, {
            let rows = sqlx::query(&query)
                .bind(bucket_id)
                .bind(prefix)
                .bind(upper.as_deref())
                .bind(lower)
                .bind(limit)
                .fetch_all(pool)
                .await?;
            Ok(rows
                .into_iter()
                .map(|row| ObjectRow {
                    key: row.get(0),
                    file_path: row.get(1),
                    size: row.get(2),
                    content_type: row.get(3),
                    etag: row.get(4),
                    updated_at: row.get(5),
                    metadata: metadata_from_json(row.get(6)),
                })
                .collect())
        })
    }

    async fn put_staged_object(
        &self,
        bucket: &str,
        key: &str,
        mut staged: StagedUpload,
        content_type: Option<&str>,
        metadata: HashMap<String, String>,
        conditions: &WriteConditions,
    ) -> ApiResult<(ObjectInfo, bool, Option<String>)> {
        validate_bucket(bucket)?;
        let normalized_key = normalize_key(key)?;
        let bucket_row = self.bucket_row(bucket).await?;
        let relative_path = format!(
            "{}/{}/{}",
            &staged.sha256[..2],
            &staged.sha256[2..4],
            staged.sha256
        );
        self.register_active_write(&relative_path).await;
        let result = async {
            let objects_root = self.objects_root();
            install_staged_blob(&mut staged, &objects_root.join(&relative_path)).await?;
            let now = now_utc();
            let final_content_type = content_type.map(str::to_string).unwrap_or_else(|| {
                mime_guess::from_path(&normalized_key)
                    .first_raw()
                    .unwrap_or("application/octet-stream")
                    .to_string()
            });
            let info = ObjectInfo {
                bucket: bucket.to_string(),
                key: normalized_key.clone(),
                size: staged.size,
                content_type: final_content_type,
                etag: staged.etag.clone(),
                updated_at: now,
                metadata,
            };
            database!(self, pool, {
                let mut tx = pool.begin().await?;
                lock_bucket!(tx, bucket_row.id);
                let (created, retired_path) = publish_object!(
                    tx,
                    bucket_row.id,
                    &normalized_key,
                    &relative_path,
                    info,
                    conditions
                );
                tx.commit().await?;
                Ok((info, created, retired_path))
            })
        }
        .await;
        self.unregister_active_write(&relative_path).await;
        result
    }

    #[cfg(test)]
    async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        body: &[u8],
        content_type: Option<&str>,
        metadata: HashMap<String, String>,
    ) -> ApiResult<(ObjectInfo, bool, Option<String>)> {
        let staged = stage_upload(&self.objects_root(), Body::from(body.to_vec()), 0, None).await?;
        self.put_staged_object(
            bucket,
            key,
            staged,
            content_type,
            metadata,
            &WriteConditions::default(),
        )
        .await
    }

    async fn copy_object(
        &self,
        source_bucket: &str,
        source_key: &str,
        destination_bucket: &str,
        destination_key: &str,
        replacement_content_type: Option<&str>,
        replacement_metadata: Option<HashMap<String, String>>,
    ) -> ApiResult<(ObjectInfo, Option<String>)> {
        validate_bucket(source_bucket)?;
        validate_bucket(destination_bucket)?;
        let source_key = normalize_key(source_key)?;
        let destination_key = normalize_key(destination_key)?;
        let source_bucket_row = self.bucket_row(source_bucket).await?;
        let destination_bucket_row = self.bucket_row(destination_bucket).await?;
        let source = self.object_row(source_bucket_row.id, &source_key).await?;
        match tokio::fs::metadata(self.objects_root().join(&source.file_path)).await {
            Ok(metadata) if metadata.is_file() => {}
            _ => return Err(ApiError::new(StatusCode::NOT_FOUND, "Object data missing")),
        }
        let now = now_utc();
        let replacement_metadata_json = replacement_metadata
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(|err| {
                ApiError::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Metadata serialization error: {err}"),
                )
            })?;

        database!(self, pool, {
            let mut tx = pool.begin().await?;
            for id in [
                source_bucket_row.id.min(destination_bucket_row.id),
                source_bucket_row.id.max(destination_bucket_row.id),
            ] {
                lock_bucket!(tx, id);
            }
            let previous =
                sqlx::query("SELECT file_path FROM objects WHERE bucket_id = $1 AND key = $2")
                    .bind(destination_bucket_row.id)
                    .bind(&destination_key)
                    .fetch_optional(&mut *tx)
                    .await?
                    .map(|r| r.get::<String, _>(0));
            let row = sqlx::query("INSERT INTO objects
                (bucket_id, key, file_path, size, content_type, etag, updated_at, created_at, metadata)
                SELECT $3, $4, file_path, size, COALESCE($5, content_type), etag, $7, $7, COALESCE($6, metadata)
                FROM objects WHERE bucket_id = $1 AND key = $2
                ON CONFLICT(bucket_id, key) DO UPDATE SET file_path = EXCLUDED.file_path, size = EXCLUDED.size,
                content_type = EXCLUDED.content_type, etag = EXCLUDED.etag, updated_at = EXCLUDED.updated_at, metadata = EXCLUDED.metadata
                RETURNING key, file_path, size, content_type, etag, updated_at, metadata")
                .bind(source_bucket_row.id).bind(&source_key).bind(destination_bucket_row.id).bind(&destination_key)
                .bind(replacement_content_type).bind(replacement_metadata_json.as_deref()).bind(&now)
                .fetch_optional(&mut *tx).await?
                .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "Object not found"))?;
            let row = ObjectRow {
                key: row.get(0),
                file_path: row.get(1),
                size: row.get(2),
                content_type: row.get(3),
                etag: row.get(4),
                updated_at: row.get(5),
                metadata: metadata_from_json(row.get(6)),
            };
            match tokio::fs::metadata(self.objects_root().join(&row.file_path)).await {
                Ok(metadata) if metadata.is_file() => {}
                _ => return Err(ApiError::new(StatusCode::NOT_FOUND, "Object data missing")),
            }
            tx.commit().await?;
            let retired = previous.filter(|p| p != &row.file_path);
            Ok((row.info(destination_bucket), retired))
        })
    }

    #[cfg(test)]
    async fn get_object(&self, bucket: &str, key: &str) -> ApiResult<StoredObject> {
        let source = self.get_object_source(bucket, key).await?;
        let body = tokio::fs::read(&source.path)
            .await
            .map_err(|_| ApiError::new(StatusCode::NOT_FOUND, "Object data missing"))?;
        Ok(StoredObject {
            info: source.info,
            body,
        })
    }

    async fn get_object_source(&self, bucket: &str, key: &str) -> ApiResult<StoredObjectSource> {
        validate_bucket(bucket)?;
        let normalized_key = normalize_key(key)?;
        let bucket_row = self.bucket_row(bucket).await?;
        let row = self.object_row(bucket_row.id, &normalized_key).await?;
        let path = self.objects_root().join(&row.file_path);
        let metadata = tokio::fs::metadata(&path)
            .await
            .map_err(|_| ApiError::new(StatusCode::NOT_FOUND, "Object data missing"))?;
        if !metadata.is_file() {
            return Err(ApiError::new(StatusCode::NOT_FOUND, "Object data missing"));
        }
        let size = metadata.len();
        let mut info = row.info(bucket);
        info.size = i64::try_from(size)
            .map_err(|_| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "Object too large"))?;
        Ok(StoredObjectSource { info, path, size })
    }

    async fn get_object_info(&self, bucket: &str, key: &str) -> ApiResult<Option<ObjectInfo>> {
        validate_bucket(bucket)?;
        let normalized_key = normalize_key(key)?;
        let bucket_row = self.bucket_row(bucket).await?;
        Ok(self
            .object_row_optional(bucket_row.id, &normalized_key)
            .await?
            .map(|row| row.info(bucket)))
    }

    async fn delete_object(&self, bucket: &str, key: &str) -> ApiResult<String> {
        validate_bucket(bucket)?;
        let normalized_key = normalize_key(key)?;
        let bucket_row = self.bucket_row(bucket).await?;
        database!(self, pool, {
            let mut tx = pool.begin().await?;
            lock_bucket!(tx, bucket_row.id);
            let row = sqlx::query(
                "DELETE FROM objects WHERE bucket_id = $1 AND key = $2 RETURNING file_path",
            )
            .bind(bucket_row.id)
            .bind(&normalized_key)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "Object not found"))?;
            let path: String = row.get(0);
            tx.commit().await?;
            Ok(path)
        })
    }

    async fn reclaim_unreferenced_path(&self, relative_path: &str) -> ApiResult<Option<u64>> {
        if !is_safe_storage_relative_path(relative_path) {
            return Err(ApiError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Unsafe object file_path",
            ));
        }
        let path = self.objects_root().join(relative_path);
        self.remove_orphan_file_if_still_unreferenced(&path, relative_path, Duration::ZERO)
            .await
    }

    async fn garbage_collect(&self, grace: Duration) -> ApiResult<GcReport> {
        let objects_root = self.objects_root();
        tokio::fs::create_dir_all(&objects_root).await?;

        let refs = self.object_refs_for_gc().await?;
        let mut referenced_paths = HashSet::with_capacity(refs.len());
        let mut report = GcReport::default();

        for object_ref in &refs {
            if !is_safe_storage_relative_path(&object_ref.file_path) {
                tracing::warn!(
                    file_path = %object_ref.file_path,
                    object_id = object_ref.id,
                    "gc skipped unsafe object file_path"
                );
                continue;
            }
            referenced_paths.insert(object_ref.file_path.clone());
            let absolute_path = objects_root.join(&object_ref.file_path);
            match tokio::fs::metadata(&absolute_path).await {
                Ok(metadata) if metadata.is_file() => {}
                Ok(_) => {
                    tracing::warn!(
                        file_path = %object_ref.file_path,
                        object_id = object_ref.id,
                        "gc found object file_path that is not a file"
                    );
                }
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                    if rfc3339_older_than(&object_ref.updated_at, grace) {
                        report.missing_records_deleted += self
                            .delete_gc_object_ref(
                                object_ref.id,
                                &object_ref.file_path,
                                &object_ref.updated_at,
                            )
                            .await?;
                    }
                }
                Err(err) => return Err(err.into()),
            }
        }

        let mut pending_dirs = Vec::new();
        let mut stack = vec![objects_root.clone()];
        while let Some(dir) = stack.pop() {
            let mut entries = match tokio::fs::read_dir(&dir).await {
                Ok(entries) => entries,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(err) => return Err(err.into()),
            };
            while let Some(entry) = entries.next_entry().await? {
                let path = entry.path();
                let file_type = entry.file_type().await?;
                if file_type.is_dir() {
                    pending_dirs.push(path.clone());
                    stack.push(path);
                    continue;
                }
                if !file_type.is_file() {
                    continue;
                }

                let metadata = entry.metadata().await?;
                if !file_older_than(&metadata, grace) {
                    continue;
                }

                if is_stale_tmp_file(&path) {
                    if let Some(active_path) = active_path_for_tmp(&objects_root, &path) {
                        if self.is_active_write(&active_path).await {
                            continue;
                        }
                    }
                    let bytes = metadata.len();
                    match tokio::fs::remove_file(&path).await {
                        Ok(()) => {
                            report.stale_tmp_files_deleted += 1;
                            report.orphan_bytes_deleted += bytes;
                        }
                        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                        Err(err) => return Err(err.into()),
                    }
                    continue;
                }

                let Some(relative_path) = storage_relative_path(&objects_root, &path) else {
                    continue;
                };
                if referenced_paths.contains(&relative_path) {
                    continue;
                }

                if let Some(bytes) = self
                    .remove_orphan_file_if_still_unreferenced(&path, &relative_path, grace)
                    .await?
                {
                    report.orphan_files_deleted += 1;
                    report.orphan_bytes_deleted += bytes;
                }
            }
        }

        pending_dirs.sort_by_key(|path| std::cmp::Reverse(path.components().count()));
        for dir in pending_dirs {
            match tokio::fs::remove_dir(&dir).await {
                Ok(()) => report.empty_dirs_removed += 1,
                Err(err)
                    if matches!(
                        err.kind(),
                        std::io::ErrorKind::NotFound | std::io::ErrorKind::DirectoryNotEmpty
                    ) => {}
                Err(err) => return Err(err.into()),
            }
        }

        Ok(report)
    }

    fn objects_root(&self) -> PathBuf {
        match self {
            Self::Sqlite { objects_root, .. } | Self::Postgres { objects_root, .. } => {
                objects_root.clone()
            }
        }
    }

    fn gc_state(&self) -> Arc<GcState> {
        match self {
            Self::Sqlite { gc_state, .. } | Self::Postgres { gc_state, .. } => Arc::clone(gc_state),
        }
    }

    async fn register_active_write(&self, relative_path: &str) {
        let gc_state = self.gc_state();
        let mut active_writes = gc_state.active_writes.lock().await;
        *active_writes.entry(relative_path.to_string()).or_insert(0) += 1;
    }

    async fn unregister_active_write(&self, relative_path: &str) {
        let gc_state = self.gc_state();
        let mut active_writes = gc_state.active_writes.lock().await;
        if let Some(count) = active_writes.get_mut(relative_path) {
            *count -= 1;
            if *count == 0 {
                active_writes.remove(relative_path);
            }
        }
    }

    async fn is_active_write(&self, relative_path: &str) -> bool {
        self.gc_state()
            .active_writes
            .lock()
            .await
            .contains_key(relative_path)
    }

    async fn remove_orphan_file_if_still_unreferenced(
        &self,
        path: &Path,
        relative_path: &str,
        grace: Duration,
    ) -> ApiResult<Option<u64>> {
        let gc_state = self.gc_state();
        let active_writes = gc_state.active_writes.lock().await;
        if active_writes.contains_key(relative_path) {
            return Ok(None);
        }
        if self.file_path_ref_count(relative_path).await? > 0 {
            return Ok(None);
        }
        let metadata = match tokio::fs::metadata(path).await {
            Ok(metadata) if metadata.is_file() => metadata,
            Ok(_) => return Ok(None),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(err.into()),
        };
        if !file_older_than(&metadata, grace) {
            return Ok(None);
        }
        let bytes = metadata.len();
        match tokio::fs::remove_file(path).await {
            Ok(()) => Ok(Some(bytes)),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    async fn bucket_row(&self, bucket: &str) -> ApiResult<BucketRow> {
        match self {
            Self::Sqlite { pool, .. } => {
                let row = sqlx::query("SELECT id, name, created_at FROM buckets WHERE name = ?1")
                    .bind(bucket)
                    .fetch_optional(pool)
                    .await?;
                row.map(bucket_row_from_sqlite)
            }
            Self::Postgres { pool, .. } => {
                let row = sqlx::query("SELECT id, name, created_at FROM buckets WHERE name = $1")
                    .bind(bucket)
                    .fetch_optional(pool)
                    .await?;
                row.map(bucket_row_from_pg)
            }
        }
        .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "Bucket not found"))
    }

    async fn object_row(&self, bucket_id: i64, key: &str) -> ApiResult<ObjectRow> {
        self.object_row_optional(bucket_id, key)
            .await?
            .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "Object not found"))
    }

    async fn object_row_optional(&self, bucket_id: i64, key: &str) -> ApiResult<Option<ObjectRow>> {
        match self {
            Self::Sqlite { pool, .. } => {
                let row = sqlx::query("SELECT key, file_path, size, content_type, etag, updated_at, metadata FROM objects WHERE bucket_id = ?1 AND key = ?2")
                    .bind(bucket_id)
                    .bind(key)
                    .fetch_optional(pool)
                    .await?;
                Ok(row.map(object_row_from_sqlite))
            }
            Self::Postgres { pool, .. } => {
                let row = sqlx::query("SELECT key, file_path, size, content_type, etag, updated_at, metadata FROM objects WHERE bucket_id = $1 AND key = $2")
                    .bind(bucket_id)
                    .bind(key)
                    .fetch_optional(pool)
                    .await?;
                Ok(row.map(object_row_from_pg))
            }
        }
    }

    async fn object_refs_for_gc(&self) -> ApiResult<Vec<GcObjectRef>> {
        match self {
            Self::Sqlite { pool, .. } => {
                let rows =
                    sqlx::query("SELECT id, file_path, updated_at FROM objects ORDER BY id ASC")
                        .fetch_all(pool)
                        .await?;
                Ok(rows
                    .into_iter()
                    .map(|row| GcObjectRef {
                        id: row.get(0),
                        file_path: row.get(1),
                        updated_at: row.get(2),
                    })
                    .collect())
            }
            Self::Postgres { pool, .. } => {
                let rows =
                    sqlx::query("SELECT id, file_path, updated_at FROM objects ORDER BY id ASC")
                        .fetch_all(pool)
                        .await?;
                Ok(rows
                    .into_iter()
                    .map(|row| GcObjectRef {
                        id: row.get(0),
                        file_path: row.get(1),
                        updated_at: row.get(2),
                    })
                    .collect())
            }
        }
    }

    async fn file_path_ref_count(&self, file_path: &str) -> ApiResult<i64> {
        match self {
            Self::Sqlite { pool, .. } => {
                let row = sqlx::query("SELECT COUNT(*) FROM objects WHERE file_path = ?1")
                    .bind(file_path)
                    .fetch_one(pool)
                    .await?;
                Ok(row.get(0))
            }
            Self::Postgres { pool, .. } => {
                let row = sqlx::query("SELECT COUNT(*) FROM objects WHERE file_path = $1")
                    .bind(file_path)
                    .fetch_one(pool)
                    .await?;
                Ok(row.get(0))
            }
        }
    }

    async fn delete_gc_object_ref(
        &self,
        id: i64,
        file_path: &str,
        updated_at: &str,
    ) -> ApiResult<u64> {
        database!(self, pool, {
            let mut tx = pool.begin().await?;
            // Participate in the same write ordering as conditional PUT/complete.
            sqlx::query("UPDATE buckets SET name = name WHERE id = (SELECT bucket_id FROM objects WHERE id = $1)")
                .bind(id).execute(&mut *tx).await?;
            let count = sqlx::query(
                "DELETE FROM objects WHERE id = $1 AND file_path = $2 AND updated_at = $3",
            )
            .bind(id)
            .bind(file_path)
            .bind(updated_at)
            .execute(&mut *tx)
            .await?
            .rows_affected();
            tx.commit().await?;
            Ok(count)
        })
    }
}

impl ObjectRow {
    fn info(self, bucket: &str) -> ObjectInfo {
        ObjectInfo {
            bucket: bucket.to_string(),
            key: self.key,
            size: self.size,
            content_type: self.content_type,
            etag: self.etag,
            updated_at: self.updated_at,
            metadata: self.metadata,
        }
    }
}

fn bucket_row_from_sqlite(row: sqlx::sqlite::SqliteRow) -> BucketRow {
    BucketRow { id: row.get(0) }
}

fn bucket_row_from_pg(row: PgRow) -> BucketRow {
    BucketRow { id: row.get(0) }
}

fn object_row_from_sqlite(row: sqlx::sqlite::SqliteRow) -> ObjectRow {
    ObjectRow {
        key: row.get(0),
        file_path: row.get(1),
        size: row.get(2),
        content_type: row.get(3),
        etag: row.get(4),
        updated_at: row.get(5),
        metadata: metadata_from_json(row.get::<String, _>(6)),
    }
}

fn object_row_from_pg(row: PgRow) -> ObjectRow {
    ObjectRow {
        key: row.get(0),
        file_path: row.get(1),
        size: row.get(2),
        content_type: row.get(3),
        etag: row.get(4),
        updated_at: row.get(5),
        metadata: metadata_from_json(row.get::<String, _>(6)),
    }
}

fn metadata_from_json(value: String) -> HashMap<String, String> {
    serde_json::from_str(&value).unwrap_or_default()
}

async fn init_sqlite(pool: &SqlitePool) -> anyhow::Result<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS buckets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            created_at TEXT NOT NULL
        )",
    )
    .execute(pool)
    .await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS ix_buckets_name ON buckets (name)")
        .execute(pool)
        .await?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS objects (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bucket_id INTEGER NOT NULL REFERENCES buckets(id) ON DELETE CASCADE,
            key TEXT NOT NULL,
            file_path TEXT NOT NULL,
            size INTEGER NOT NULL,
            content_type TEXT NOT NULL,
            etag TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            created_at TEXT NOT NULL,
            metadata TEXT NOT NULL DEFAULT '{}',
            UNIQUE(bucket_id, key)
        )",
    )
    .execute(pool)
    .await?;
    let columns = sqlx::query("PRAGMA table_info(objects)")
        .fetch_all(pool)
        .await?;
    if !columns
        .iter()
        .any(|row| row.get::<String, _>(1) == "metadata")
    {
        sqlx::query("ALTER TABLE objects ADD COLUMN metadata TEXT NOT NULL DEFAULT '{}'")
            .execute(pool)
            .await?;
    }
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_objects_bucket_key ON objects (bucket_id, key)")
        .execute(pool)
        .await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_objects_file_path ON objects (file_path)")
        .execute(pool)
        .await?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS pending_uploads (
 upload_id TEXT PRIMARY KEY, bucket_id BIGINT NOT NULL REFERENCES buckets(id) ON DELETE CASCADE,
 session TEXT NOT NULL)",
    )
    .execute(pool)
    .await?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS pending_upload_parts (
 upload_id TEXT NOT NULL REFERENCES pending_uploads(upload_id) ON DELETE CASCADE,
 part_number BIGINT NOT NULL, etag TEXT NOT NULL, size BIGINT NOT NULL, file TEXT NOT NULL,
 PRIMARY KEY(upload_id, part_number))",
    )
    .execute(pool)
    .await?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS completed_uploads (
 upload_id TEXT PRIMARY KEY, bucket_id BIGINT NOT NULL REFERENCES buckets(id) ON DELETE CASCADE,
 session TEXT NOT NULL)",
    )
    .execute(pool)
    .await?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_pending_uploads_bucket ON pending_uploads(bucket_id)",
    )
    .execute(pool)
    .await?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_completed_uploads_bucket ON completed_uploads(bucket_id)",
    )
    .execute(pool)
    .await?;
    Ok(())
}

async fn init_pg(pool: &PgPool) -> anyhow::Result<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS buckets (
            id BIGSERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL UNIQUE,
            created_at TEXT NOT NULL
        )",
    )
    .execute(pool)
    .await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS ix_buckets_name ON buckets (name)")
        .execute(pool)
        .await?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS objects (
            id BIGSERIAL PRIMARY KEY,
            bucket_id BIGINT NOT NULL REFERENCES buckets(id) ON DELETE CASCADE,
            key VARCHAR(1024) NOT NULL,
            file_path VARCHAR(1024) NOT NULL,
            size BIGINT NOT NULL,
            content_type VARCHAR(255) NOT NULL,
            etag VARCHAR(64) NOT NULL,
            updated_at TEXT NOT NULL,
            created_at TEXT NOT NULL,
            metadata TEXT NOT NULL DEFAULT '{}',
            UNIQUE(bucket_id, key)
        )",
    )
    .execute(pool)
    .await?;
    sqlx::query("ALTER TABLE objects ADD COLUMN IF NOT EXISTS metadata TEXT NOT NULL DEFAULT '{}'")
        .execute(pool)
        .await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_objects_bucket_key ON objects (bucket_id, key)")
        .execute(pool)
        .await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_objects_file_path ON objects (file_path)")
        .execute(pool)
        .await?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS pending_uploads (
 upload_id TEXT PRIMARY KEY, bucket_id BIGINT NOT NULL REFERENCES buckets(id) ON DELETE CASCADE,
 session TEXT NOT NULL)",
    )
    .execute(pool)
    .await?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS pending_upload_parts (
 upload_id TEXT NOT NULL REFERENCES pending_uploads(upload_id) ON DELETE CASCADE,
 part_number BIGINT NOT NULL, etag TEXT NOT NULL, size BIGINT NOT NULL, file TEXT NOT NULL,
 PRIMARY KEY(upload_id, part_number))",
    )
    .execute(pool)
    .await?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS completed_uploads (
 upload_id TEXT PRIMARY KEY, bucket_id BIGINT NOT NULL REFERENCES buckets(id) ON DELETE CASCADE,
 session TEXT NOT NULL)",
    )
    .execute(pool)
    .await?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_pending_uploads_bucket ON pending_uploads(bucket_id)",
    )
    .execute(pool)
    .await?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_completed_uploads_bucket ON completed_uploads(bucket_id)",
    )
    .execute(pool)
    .await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_objects_bucket_key_c ON objects (bucket_id, key COLLATE \"C\")").execute(pool).await?;
    Ok(())
}

async fn stream_object_response(
    source: StoredObjectSource,
    headers: &HeaderMap,
) -> ApiResult<Response> {
    let mut response_headers = HeaderMap::new();
    response_headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("private, no-cache"),
    );
    response_headers.insert(
        header::ETAG,
        HeaderValue::from_str(&format!("\"{}\"", source.info.etag)).unwrap(),
    );
    insert_user_metadata_headers(&mut response_headers, &source.info.metadata)?;
    response_headers.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    response_headers.insert(
        header::LAST_MODIFIED,
        HeaderValue::from_str(&http_date(&source.info.updated_at))
            .unwrap_or_else(|_| HeaderValue::from_static("Thu, 01 Jan 1970 00:00:00 GMT")),
    );

    if header_str(headers, "if-match")
        .is_some_and(|value| !strong_etag_matches(value, &source.info.etag))
    {
        return Err(ApiError::new(
            StatusCode::PRECONDITION_FAILED,
            "Read precondition failed",
        ));
    }
    if header_str(headers, header::IF_NONE_MATCH.as_str())
        .map(|value| etag_matches(value, &source.info.etag))
        .unwrap_or(false)
    {
        return Ok((StatusCode::NOT_MODIFIED, response_headers).into_response());
    }

    let body_len = usize::try_from(source.size)
        .map_err(|_| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "Object too large"))?;
    let (status, start, response_len) =
        if let Some(range_header) = header_str(headers, header::RANGE.as_str()) {
            let (start, end) = parse_range(range_header, body_len)?;
            response_headers.insert(
                header::CONTENT_RANGE,
                HeaderValue::from_str(&format!("bytes {start}-{end}/{body_len}")).unwrap(),
            );
            (StatusCode::PARTIAL_CONTENT, start, end - start + 1)
        } else {
            (StatusCode::OK, 0, body_len)
        };
    response_headers.insert(
        header::CONTENT_LENGTH,
        HeaderValue::from_str(&response_len.to_string()).unwrap(),
    );
    response_headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_str(&source.info.content_type)
            .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream")),
    );

    let mut file = tokio::fs::File::open(&source.path)
        .await
        .map_err(|_| ApiError::new(StatusCode::NOT_FOUND, "Object data missing"))?;
    if start != 0 {
        file.seek(std::io::SeekFrom::Start(start as u64))
            .await
            .map_err(|err| {
                ApiError::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to seek object data: {err}"),
                )
            })?;
    }
    let reader = file.take(response_len as u64);
    let stream = ReaderStream::with_capacity(reader, DOWNLOAD_CHUNK_SIZE);
    let body = Body::from_stream(stream);
    Ok((status, response_headers, body).into_response())
}

fn object_response(
    obj: StoredObject,
    headers: &HeaderMap,
    include_body: bool,
) -> ApiResult<Response> {
    let mut response_headers = HeaderMap::new();
    response_headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("private, no-cache"),
    );
    response_headers.insert(
        header::ETAG,
        HeaderValue::from_str(&format!("\"{}\"", obj.info.etag)).unwrap(),
    );
    insert_user_metadata_headers(&mut response_headers, &obj.info.metadata)?;
    response_headers.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    response_headers.insert(
        header::LAST_MODIFIED,
        HeaderValue::from_str(&http_date(&obj.info.updated_at))
            .unwrap_or_else(|_| HeaderValue::from_static("Thu, 01 Jan 1970 00:00:00 GMT")),
    );

    if header_str(headers, "if-match")
        .is_some_and(|value| !strong_etag_matches(value, &obj.info.etag))
    {
        return Err(ApiError::new(
            StatusCode::PRECONDITION_FAILED,
            "Read precondition failed",
        ));
    }
    if header_str(headers, header::IF_NONE_MATCH.as_str())
        .map(|value| etag_matches(value, &obj.info.etag))
        .unwrap_or(false)
    {
        return Ok((StatusCode::NOT_MODIFIED, response_headers).into_response());
    }

    let body_len = obj.body.len();
    if let Some(range_header) = header_str(headers, header::RANGE.as_str()) {
        let (start, end) = parse_range(range_header, body_len)?;
        let partial = obj.body[start..=end].to_vec();
        response_headers.insert(
            header::CONTENT_RANGE,
            HeaderValue::from_str(&format!("bytes {start}-{end}/{body_len}")).unwrap(),
        );
        response_headers.insert(
            header::CONTENT_LENGTH,
            HeaderValue::from_str(&partial.len().to_string()).unwrap(),
        );
        response_headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_str(&obj.info.content_type)
                .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream")),
        );
        if include_body {
            return Ok((StatusCode::PARTIAL_CONTENT, response_headers, partial).into_response());
        }
        return Ok((StatusCode::PARTIAL_CONTENT, response_headers).into_response());
    }

    response_headers.insert(
        header::CONTENT_LENGTH,
        HeaderValue::from_str(&body_len.to_string()).unwrap(),
    );
    response_headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_str(&obj.info.content_type)
            .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream")),
    );
    if include_body {
        Ok((StatusCode::OK, response_headers, obj.body).into_response())
    } else {
        Ok((StatusCode::OK, response_headers).into_response())
    }
}

fn object_head_response(info: ObjectInfo, headers: &HeaderMap) -> ApiResult<Response> {
    let mut response_headers = HeaderMap::new();
    response_headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("private, no-cache"),
    );
    response_headers.insert(
        header::ETAG,
        HeaderValue::from_str(&format!("\"{}\"", info.etag)).unwrap(),
    );
    insert_user_metadata_headers(&mut response_headers, &info.metadata)?;
    response_headers.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    response_headers.insert(
        header::LAST_MODIFIED,
        HeaderValue::from_str(&http_date(&info.updated_at))
            .unwrap_or_else(|_| HeaderValue::from_static("Thu, 01 Jan 1970 00:00:00 GMT")),
    );
    response_headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_str(&info.content_type)
            .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream")),
    );

    if header_str(headers, "if-match").is_some_and(|value| !strong_etag_matches(value, &info.etag))
    {
        return Err(ApiError::new(
            StatusCode::PRECONDITION_FAILED,
            "Read precondition failed",
        ));
    }
    if header_str(headers, header::IF_NONE_MATCH.as_str())
        .map(|value| etag_matches(value, &info.etag))
        .unwrap_or(false)
    {
        return Ok((StatusCode::NOT_MODIFIED, response_headers).into_response());
    }

    let body_len = usize::try_from(info.size.max(0))
        .map_err(|_| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "Invalid object size"))?;
    if let Some(range_header) = header_str(headers, header::RANGE.as_str()) {
        let (start, end) = parse_range(range_header, body_len)?;
        response_headers.insert(
            header::CONTENT_RANGE,
            HeaderValue::from_str(&format!("bytes {start}-{end}/{body_len}")).unwrap(),
        );
        response_headers.insert(
            header::CONTENT_LENGTH,
            HeaderValue::from_str(&(end - start + 1).to_string()).unwrap(),
        );
        return Ok((StatusCode::PARTIAL_CONTENT, response_headers).into_response());
    }

    response_headers.insert(
        header::CONTENT_LENGTH,
        HeaderValue::from_str(&body_len.to_string()).unwrap(),
    );
    Ok((StatusCode::OK, response_headers).into_response())
}

fn validate_bucket(bucket: &str) -> ApiResult<()> {
    if bucket.is_empty()
        || bucket.contains('/')
        || bucket.contains('\\')
        || bucket == "."
        || bucket == ".."
    {
        return Err(ApiError::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "Invalid bucket name",
        ));
    }
    Ok(())
}

fn normalize_key(key: &str) -> ApiResult<String> {
    let normalized = key.trim_matches('/');
    if normalized.is_empty()
        || normalized == ".."
        || normalized.starts_with("../")
        || normalized.contains("/../")
    {
        return Err(ApiError::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "Invalid object key",
        ));
    }
    Ok(normalized.to_string())
}

fn is_safe_storage_relative_path(value: &str) -> bool {
    let path = Path::new(value);
    !path.is_absolute()
        && path
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
}

fn storage_relative_path(root: &Path, path: &Path) -> Option<String> {
    let relative = path.strip_prefix(root).ok()?;
    let mut parts = Vec::new();
    for component in relative.components() {
        match component {
            Component::Normal(part) => parts.push(part.to_str()?),
            _ => return None,
        }
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join("/"))
    }
}

fn active_path_for_tmp(root: &Path, path: &Path) -> Option<String> {
    let file_name = path.file_name()?.to_str()?;
    let stripped = file_name.strip_prefix('.')?;
    let (target_name, _) = stripped.split_once('.')?;
    if target_name.is_empty() {
        return None;
    }
    let parent = path.parent()?;
    storage_relative_path(root, &parent.join(target_name))
}

fn file_older_than(metadata: &std::fs::Metadata, grace: Duration) -> bool {
    let Ok(modified) = metadata.modified() else {
        return false;
    };
    SystemTime::now()
        .duration_since(modified)
        .map(|age| age >= grace)
        .unwrap_or(false)
}

fn rfc3339_older_than(value: &str, grace: Duration) -> bool {
    let Ok(updated_at) = DateTime::parse_from_rfc3339(value) else {
        return false;
    };
    let Ok(grace) = chrono::Duration::from_std(grace) else {
        return false;
    };
    Utc::now().signed_duration_since(updated_at.with_timezone(&Utc)) >= grace
}

fn is_stale_tmp_file(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .map(|name| name.starts_with('.') && name.ends_with(".tmp"))
        .unwrap_or(false)
}

fn validate_content_length(headers: &HeaderMap, max_upload_size: usize) -> ApiResult<()> {
    if max_upload_size == 0 {
        return Ok(());
    }
    let Some(value) = headers.get(header::CONTENT_LENGTH) else {
        return Ok(());
    };
    let value = value
        .to_str()
        .map_err(|_| ApiError::new(StatusCode::BAD_REQUEST, "Invalid Content-Length header"))?;
    let length = value
        .parse::<u64>()
        .map_err(|_| ApiError::new(StatusCode::BAD_REQUEST, "Invalid Content-Length header"))?;
    if length > max_upload_size as u64 {
        return Err(ApiError::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            format!("Upload exceeds maximum size of {max_upload_size} bytes"),
        ));
    }
    Ok(())
}

fn parse_content_md5(headers: &HeaderMap) -> ApiResult<Option<[u8; 16]>> {
    let Some(value) = headers.get("content-md5") else {
        return Ok(None);
    };
    let value = value
        .to_str()
        .map_err(|_| ApiError::new(StatusCode::BAD_REQUEST, "Invalid Content-MD5 header"))?;
    let provided = base64::engine::general_purpose::STANDARD
        .decode(value)
        .map_err(|_| ApiError::new(StatusCode::BAD_REQUEST, "Invalid Content-MD5 header"))?;
    let provided = provided
        .try_into()
        .map_err(|_| ApiError::new(StatusCode::BAD_REQUEST, "Invalid Content-MD5 header"))?;
    Ok(Some(provided))
}

fn extract_user_metadata(headers: &HeaderMap) -> ApiResult<HashMap<String, String>> {
    let mut metadata = HashMap::new();
    for (name, value) in headers {
        let Some(key) = name.as_str().strip_prefix("x-amz-meta-") else {
            continue;
        };
        if key.is_empty() {
            return Err(ApiError::new(StatusCode::BAD_REQUEST, "Empty metadata key"));
        }
        let value = value
            .to_str()
            .map_err(|_| ApiError::new(StatusCode::BAD_REQUEST, "Invalid metadata value"))?;
        metadata.insert(key.to_string(), value.to_string());
    }
    Ok(metadata)
}

fn insert_user_metadata_headers(
    headers: &mut HeaderMap,
    metadata: &HashMap<String, String>,
) -> ApiResult<()> {
    for (key, value) in metadata {
        let name = format!("x-amz-meta-{key}")
            .parse::<http::HeaderName>()
            .map_err(|_| {
                ApiError::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Invalid stored metadata key",
                )
            })?;
        let value = HeaderValue::from_str(value).map_err(|_| {
            ApiError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Invalid stored metadata value",
            )
        })?;
        headers.insert(name, value);
    }
    Ok(())
}

fn parse_copy_source(value: &str) -> ApiResult<(String, String)> {
    let without_query = value.split('?').next().unwrap_or(value);
    let decoded = percent_decode_str(without_query)
        .decode_utf8()
        .map_err(|_| ApiError::new(StatusCode::BAD_REQUEST, "Invalid x-amz-copy-source"))?;
    let (bucket, key) = decoded
        .trim_start_matches('/')
        .split_once('/')
        .ok_or_else(|| ApiError::new(StatusCode::BAD_REQUEST, "Invalid x-amz-copy-source"))?;
    validate_bucket(bucket)?;
    Ok((bucket.to_string(), normalize_key(key)?))
}

#[derive(Serialize, Deserialize)]
struct ListCursor {
    bucket: String,
    prefix: String,
    delimiter: String,
    lower: String,
    inclusive: bool,
}
impl ListCursor {
    fn encode(&self) -> String {
        format!(
            "v1.{}",
            base64::engine::general_purpose::URL_SAFE_NO_PAD
                .encode(serde_json::to_vec(self).expect("cursor serialization"))
        )
    }
    fn decode(value: &str, bucket: &str, prefix: &str, delimiter: &str) -> ApiResult<Self> {
        let invalid = || {
            ApiError::new(
                StatusCode::BAD_REQUEST,
                "Invalid continuation token or listing context",
            )
        };
        let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(value.strip_prefix("v1.").ok_or_else(invalid)?)
            .map_err(|_| invalid())?;
        let cursor: Self = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
        if cursor.bucket != bucket
            || cursor.prefix != prefix
            || cursor.delimiter != delimiter
            || cursor.lower.as_str() < prefix
        {
            return Err(invalid());
        }
        Ok(cursor)
    }
}
fn prefix_upper_bound(prefix: &str) -> Option<String> {
    for (pos, ch) in prefix.char_indices().rev() {
        let mut next = ch as u32 + 1;
        if next == 0xd800 {
            next = 0xe000;
        }
        if let Some(ch) = char::from_u32(next) {
            return Some(format!("{}{}", &prefix[..pos], ch));
        }
    }
    None
}

fn etag_matches(header_value: &str, current_etag: &str) -> bool {
    header_value.split(',').any(|token| {
        let mut token = token.trim();
        if token == "*" {
            return true;
        }
        if let Some(stripped) = token.strip_prefix("W/") {
            token = stripped.trim();
        }
        token.trim_matches('"') == current_etag
    })
}

fn parse_range(range_header: &str, total_size: usize) -> ApiResult<(usize, usize)> {
    if total_size == 0 || !range_header.starts_with("bytes=") {
        return Err(range_error(total_size));
    }
    let raw = range_header.trim_start_matches("bytes=").trim();
    if raw.contains(',') {
        return Err(range_error(total_size));
    }
    let (start_raw, end_raw) = raw.split_once('-').ok_or_else(|| range_error(total_size))?;
    if start_raw.is_empty() {
        let suffix = end_raw
            .parse::<usize>()
            .map_err(|_| range_error(total_size))?;
        if suffix == 0 {
            return Err(range_error(total_size));
        }
        if suffix >= total_size {
            return Ok((0, total_size - 1));
        }
        return Ok((total_size - suffix, total_size - 1));
    }
    let start = start_raw
        .parse::<usize>()
        .map_err(|_| range_error(total_size))?;
    if start >= total_size {
        return Err(range_error(total_size));
    }
    let end = if end_raw.is_empty() {
        total_size - 1
    } else {
        end_raw
            .parse::<usize>()
            .map_err(|_| range_error(total_size))?
            .min(total_size - 1)
    };
    if end < start {
        return Err(range_error(total_size));
    }
    Ok((start, end))
}

fn range_error(total_size: usize) -> ApiError {
    ApiError::new(
        StatusCode::RANGE_NOT_SATISFIABLE,
        format!("Range not satisfiable: bytes */{total_size}"),
    )
}

async fn stage_upload(
    objects_root: &Path,
    body: Body,
    max_upload_size: usize,
    expected_md5: Option<[u8; 16]>,
) -> ApiResult<StagedUpload> {
    tokio::fs::create_dir_all(objects_root).await?;
    let temp_path = objects_root.join(format!(".upload-{}.tmp", Uuid::new_v4().simple()));
    let guard = TempFileGuard::new(temp_path.clone());
    let mut file = tokio::fs::File::create(&temp_path).await?;
    let mut stream = body.into_data_stream();
    let mut md5_context = md5::Context::new();
    let mut sha256 = Sha256::new();
    let mut size = 0_usize;

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|err| {
            ApiError::new(
                StatusCode::BAD_REQUEST,
                format!("Failed to read request body: {err}"),
            )
        })?;
        size = size
            .checked_add(chunk.len())
            .ok_or_else(|| ApiError::new(StatusCode::PAYLOAD_TOO_LARGE, "Upload size overflow"))?;
        if max_upload_size != 0 && size > max_upload_size {
            return Err(ApiError::new(
                StatusCode::PAYLOAD_TOO_LARGE,
                format!("Upload exceeds maximum size of {max_upload_size} bytes"),
            ));
        }
        md5_context.consume(&chunk);
        sha256.update(&chunk);
        file.write_all(&chunk).await?;
    }

    file.sync_all().await?;
    drop(file);
    let md5 = md5_context.compute();
    if expected_md5
        .map(|expected| expected != md5.0)
        .unwrap_or(false)
    {
        return Err(ApiError::new(StatusCode::BAD_REQUEST, "BadDigest"));
    }
    let size = i64::try_from(size)
        .map_err(|_| ApiError::new(StatusCode::PAYLOAD_TOO_LARGE, "Upload is too large"))?;
    Ok(StagedUpload {
        temp_path: guard.into_path(),
        size,
        etag: format!("{md5:x}"),
        sha256: format!("{:x}", sha256.finalize()),
    })
}

async fn install_staged_blob(staged: &mut StagedUpload, target_path: &Path) -> ApiResult<()> {
    if let Some(parent) = target_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    match tokio::fs::metadata(target_path).await {
        Ok(metadata) if metadata.is_file() => {
            tokio::fs::remove_file(&staged.temp_path).await?;
            staged.temp_path.clear();
            return Ok(());
        }
        Ok(_) => {
            return Err(ApiError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Object blob path is not a file",
            ));
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => return Err(err.into()),
    }

    match tokio::fs::rename(&staged.temp_path, target_path).await {
        Ok(()) => staged.temp_path.clear(),
        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
            match tokio::fs::metadata(target_path).await {
                Ok(metadata) if metadata.is_file() => {
                    tokio::fs::remove_file(&staged.temp_path).await?;
                    staged.temp_path.clear();
                }
                _ => return Err(err.into()),
            }
        }
        Err(err) => return Err(err.into()),
    }
    Ok(())
}

fn now_utc() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Micros, true)
}

fn http_date(value: &str) -> String {
    DateTime::parse_from_rfc3339(value)
        .map(|dt| {
            dt.with_timezone(&Utc)
                .format("%a, %d %b %Y %H:%M:%S GMT")
                .to_string()
        })
        .unwrap_or_else(|_| "Thu, 01 Jan 1970 00:00:00 GMT".to_string())
}

fn header_str<'a>(headers: &'a HeaderMap, key: &str) -> Option<&'a str> {
    headers.get(key).and_then(|value| value.to_str().ok())
}

fn sqlite_path(database_url: &str) -> PathBuf {
    if let Some(path) = database_url.strip_prefix("sqlite:///") {
        return PathBuf::from(path);
    }
    if let Some(path) = database_url.strip_prefix("sqlite://") {
        return PathBuf::from(path);
    }
    PathBuf::from(database_url)
}

fn list_objects_v2_xml(result: &ListObjectsV2Result) -> String {
    let mut xml = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
    xml.push_str("<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">");
    xml.push_str(&format!("<Name>{}</Name>", escape_xml(&result.bucket)));
    xml.push_str(&format!("<Prefix>{}</Prefix>", escape_xml(&result.prefix)));
    xml.push_str(&format!("<KeyCount>{}</KeyCount>", result.key_count));
    xml.push_str(&format!("<MaxKeys>{}</MaxKeys>", result.max_keys));
    xml.push_str(&format!(
        "<Delimiter>{}</Delimiter>",
        escape_xml(&result.delimiter)
    ));
    xml.push_str(&format!(
        "<IsTruncated>{}</IsTruncated>",
        if result.is_truncated { "true" } else { "false" }
    ));
    if let Some(token) = &result.next_continuation_token {
        xml.push_str(&format!(
            "<NextContinuationToken>{}</NextContinuationToken>",
            escape_xml(token)
        ));
    }
    for item in &result.contents {
        xml.push_str("<Contents>");
        xml.push_str(&format!("<Key>{}</Key>", escape_xml(&item.key)));
        xml.push_str(&format!(
            "<LastModified>{}</LastModified>",
            escape_xml(&item.updated_at)
        ));
        xml.push_str(&format!(
            "<ETag>&quot;{}&quot;</ETag>",
            escape_xml(&item.etag)
        ));
        xml.push_str(&format!("<Size>{}</Size>", item.size));
        xml.push_str("<StorageClass>STANDARD</StorageClass>");
        xml.push_str("</Contents>");
    }
    for cp in &result.common_prefixes {
        xml.push_str("<CommonPrefixes>");
        xml.push_str(&format!("<Prefix>{}</Prefix>", escape_xml(cp)));
        xml.push_str("</CommonPrefixes>");
    }
    xml.push_str("</ListBucketResult>");
    xml
}

fn copy_object_result_xml(info: &ObjectInfo) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><CopyObjectResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><LastModified>{}</LastModified><ETag>&quot;{}&quot;</ETag></CopyObjectResult>",
        escape_xml(&info.updated_at),
        escape_xml(&info.etag)
    )
}

fn escape_xml(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

impl From<sqlx::Error> for ApiError {
    fn from(err: sqlx::Error) -> Self {
        ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Database error: {err}"),
        )
    }
}

impl From<std::io::Error> for ApiError {
    fn from(err: std::io::Error) -> Self {
        ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("I/O error: {err}"),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn user_token(secret: &str, expires: u64) -> String {
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(format!("tester|user|{expires}|nonce"));
        let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(payload.as_bytes());
        let signature = mac
            .finalize()
            .into_bytes()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        format!("{payload}.{signature}")
    }

    #[test]
    fn user_access_token_requires_valid_signature_and_expiry() {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let valid = user_token("secret", now + 60);
        assert!(valid_user_access_token(&valid, "secret"));
        assert!(!valid_user_access_token(&valid, "wrong-secret"));
        assert!(!valid_user_access_token(
            &user_token("secret", now.saturating_sub(1)),
            "secret"
        ));
    }

    #[test]
    fn public_tileset_response_removes_legacy_url_but_keeps_uri() {
        let mut body =
            br#"{"root":{"content":{"uri":"tile.b3dm","url":"tile.b3dm"},"children":[]}}"#.to_vec();
        sanitize_tileset_json(&mut body);
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(payload["root"]["content"]["uri"], "tile.b3dm");
        assert!(payload["root"]["content"].get("url").is_none());
    }

    #[test]
    fn public_objects_only_make_versioned_urls_immutable() {
        let uri = "/api/object/models/system/structure/models/model.glb"
            .parse::<http::Uri>()
            .unwrap();
        let redirect = public_object_version_redirect(&uri, "abc123");
        assert_eq!(redirect.status(), StatusCode::TEMPORARY_REDIRECT);
        assert_eq!(
            redirect.headers()[header::LOCATION],
            "/api/object/models/system/structure/models/model.glb?version=abc123"
        );
        assert_eq!(
            redirect.headers()[header::CACHE_CONTROL],
            "private, no-store"
        );
        assert_eq!(redirect.headers()[header::VARY], "Authorization, Cookie");

        let mut versioned_response = Response::new(Body::empty());
        apply_public_object_headers(&mut versioned_response, false);
        assert_eq!(
            versioned_response.headers()[header::CACHE_CONTROL],
            "private, max-age=31536000, immutable"
        );
        assert_eq!(
            versioned_response.headers()[header::VARY],
            "Authorization, Cookie"
        );

        let mut terrain_response = Response::new(Body::empty());
        apply_public_object_headers(&mut terrain_response, true);
        assert_eq!(terrain_response.headers()[header::CONTENT_ENCODING], "gzip");
    }

    async fn test_storage() -> (Storage, PathBuf) {
        let root = std::env::temp_dir().join(format!("alocals3-targeted-gc-{}", Uuid::new_v4()));
        tokio::fs::create_dir_all(&root).await.unwrap();
        let database_url = format!("sqlite:///{}", root.join("alocals3.db").display());
        let storage = Storage::connect(&database_url, root.join("storage"))
            .await
            .unwrap();
        storage.create_bucket("test-bucket").await.unwrap();
        (storage, root)
    }

    #[tokio::test]
    async fn targeted_gc_reclaims_only_the_replaced_blob() {
        let (storage, root) = test_storage().await;
        let (_, created, _) = storage
            .put_object("test-bucket", "item", b"old", None, HashMap::new())
            .await
            .unwrap();
        assert!(created);

        let (_, created, retired_path) = storage
            .put_object("test-bucket", "item", b"new", None, HashMap::new())
            .await
            .unwrap();
        assert!(!created);
        let retired_path = retired_path.expect("overwriting different content must retire a blob");
        assert!(storage.objects_root().join(&retired_path).exists());

        let deleted = storage
            .reclaim_unreferenced_path(&retired_path)
            .await
            .unwrap();
        assert_eq!(deleted, Some(3));
        assert!(!storage.objects_root().join(&retired_path).exists());
        assert_eq!(
            storage
                .get_object("test-bucket", "item")
                .await
                .unwrap()
                .body,
            b"new"
        );
        tokio::fs::remove_dir_all(root).await.unwrap();
    }

    #[tokio::test]
    async fn targeted_gc_preserves_a_blob_shared_by_another_key() {
        let (storage, root) = test_storage().await;
        storage
            .put_object("test-bucket", "first", b"shared", None, HashMap::new())
            .await
            .unwrap();
        storage
            .put_object("test-bucket", "second", b"shared", None, HashMap::new())
            .await
            .unwrap();

        let (_, _, retired_path) = storage
            .put_object("test-bucket", "first", b"replacement", None, HashMap::new())
            .await
            .unwrap();
        let retired_path = retired_path.expect("the first key must retire its old blob");
        let deleted = storage
            .reclaim_unreferenced_path(&retired_path)
            .await
            .unwrap();

        assert_eq!(deleted, None);
        assert!(storage.objects_root().join(&retired_path).exists());
        assert_eq!(
            storage
                .get_object("test-bucket", "second")
                .await
                .unwrap()
                .body,
            b"shared"
        );
        tokio::fs::remove_dir_all(root).await.unwrap();
    }

    #[tokio::test]
    async fn head_response_uses_database_metadata_without_reading_the_blob() {
        let (storage, root) = test_storage().await;
        storage
            .put_object(
                "test-bucket",
                "large.glb",
                b"payload",
                Some("model/gltf-binary"),
                HashMap::new(),
            )
            .await
            .unwrap();
        let bucket = storage.bucket_row("test-bucket").await.unwrap();
        let row = storage.object_row(bucket.id, "large.glb").await.unwrap();
        tokio::fs::remove_file(storage.objects_root().join(row.file_path))
            .await
            .unwrap();

        let info = storage
            .get_object_info("test-bucket", "large.glb")
            .await
            .unwrap()
            .unwrap();
        let response = object_head_response(info, &HeaderMap::new()).unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()[header::CACHE_CONTROL],
            "private, no-cache"
        );
        assert_eq!(response.headers()[header::CONTENT_LENGTH], "7");
        assert_eq!(
            response.headers()[header::CONTENT_TYPE],
            "model/gltf-binary"
        );
        assert!(storage
            .get_object("test-bucket", "large.glb")
            .await
            .is_err());
        tokio::fs::remove_dir_all(root).await.unwrap();
    }

    #[tokio::test]
    async fn streamed_object_response_supports_full_and_range_reads() {
        let (storage, root) = test_storage().await;
        let payload = b"0123456789abcdefghijklmnopqrstuvwxyz";
        let (info, _, _) = storage
            .put_object(
                "test-bucket",
                "stream.bin",
                payload,
                Some("application/octet-stream"),
                HashMap::new(),
            )
            .await
            .unwrap();

        let response = stream_object_response(
            storage
                .get_object_source("test-bucket", "stream.bin")
                .await
                .unwrap(),
            &HeaderMap::new(),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()[header::CACHE_CONTROL],
            "private, no-cache"
        );
        assert_eq!(
            response.headers()[header::CONTENT_LENGTH],
            payload.len().to_string()
        );
        assert_eq!(
            response.headers()[header::ETAG],
            format!("\"{}\"", info.etag)
        );
        let body = axum::body::to_bytes(response.into_body(), payload.len())
            .await
            .unwrap();
        assert_eq!(body.as_ref(), payload);

        let mut range_headers = HeaderMap::new();
        range_headers.insert(header::RANGE, HeaderValue::from_static("bytes=10-19"));
        let response = stream_object_response(
            storage
                .get_object_source("test-bucket", "stream.bin")
                .await
                .unwrap(),
            &range_headers,
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(response.headers()[header::CONTENT_LENGTH], "10");
        assert_eq!(
            response.headers()[header::CONTENT_RANGE],
            format!("bytes 10-19/{}", payload.len())
        );
        let body = axum::body::to_bytes(response.into_body(), 10)
            .await
            .unwrap();
        assert_eq!(body.as_ref(), &payload[10..20]);

        let mut conditional_headers = HeaderMap::new();
        conditional_headers.insert(
            header::IF_NONE_MATCH,
            HeaderValue::from_str(&format!("\"{}\"", info.etag)).unwrap(),
        );
        let response = stream_object_response(
            storage
                .get_object_source("test-bucket", "stream.bin")
                .await
                .unwrap(),
            &conditional_headers,
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
        assert_eq!(
            response.headers()[header::CACHE_CONTROL],
            "private, no-cache"
        );

        tokio::fs::remove_dir_all(root).await.unwrap();
    }

    #[tokio::test]
    async fn copy_object_shares_blob_and_preserves_metadata() {
        let (storage, root) = test_storage().await;
        let mut metadata = HashMap::new();
        metadata.insert("foo".to_string(), "bar".to_string());
        storage
            .put_object(
                "test-bucket",
                "source.bin",
                b"large payload",
                Some("application/x-source"),
                metadata.clone(),
            )
            .await
            .unwrap();

        let source_bucket = storage.bucket_row("test-bucket").await.unwrap();
        let source = storage
            .object_row(source_bucket.id, "source.bin")
            .await
            .unwrap();
        let (copied, retired) = storage
            .copy_object(
                "test-bucket",
                "source.bin",
                "test-bucket",
                "copy.bin",
                None,
                None,
            )
            .await
            .unwrap();
        let destination = storage
            .object_row(source_bucket.id, "copy.bin")
            .await
            .unwrap();

        assert_eq!(destination.file_path, source.file_path);
        assert_eq!(copied.metadata, metadata);
        assert_eq!(copied.content_type, "application/x-source");
        assert_eq!(retired, None);
        tokio::fs::remove_dir_all(root).await.unwrap();
    }

    #[tokio::test]
    async fn copy_object_can_replace_metadata_without_copying_blob() {
        let (storage, root) = test_storage().await;
        storage
            .put_object("test-bucket", "source", b"payload", None, HashMap::new())
            .await
            .unwrap();
        let mut replacement = HashMap::new();
        replacement.insert("foo".to_string(), "new".to_string());
        let (copied, _) = storage
            .copy_object(
                "test-bucket",
                "source",
                "test-bucket",
                "destination",
                Some("text/custom"),
                Some(replacement.clone()),
            )
            .await
            .unwrap();
        assert_eq!(copied.metadata, replacement);
        assert_eq!(copied.content_type, "text/custom");

        let response = object_head_response(copied, &HeaderMap::new()).unwrap();
        assert_eq!(response.headers()["x-amz-meta-foo"], "new");
        tokio::fs::remove_dir_all(root).await.unwrap();
    }

    #[tokio::test]
    async fn upload_body_is_staged_incrementally_with_both_digests() {
        let root = std::env::temp_dir().join(format!("alocals3-stage-{}", Uuid::new_v4()));
        let payload = b"first chunk-second chunk";
        let expected_md5 = md5::compute(payload);
        let chunks = futures_util::stream::iter([
            Ok::<_, std::io::Error>(axum::body::Bytes::from_static(b"first chunk-")),
            Ok::<_, std::io::Error>(axum::body::Bytes::from_static(b"second chunk")),
        ]);

        let staged = stage_upload(
            &root,
            Body::from_stream(chunks),
            payload.len(),
            Some(expected_md5.0),
        )
        .await
        .unwrap();
        assert_eq!(staged.size, payload.len() as i64);
        assert_eq!(staged.etag, format!("{expected_md5:x}"));
        assert_eq!(staged.sha256, format!("{:x}", Sha256::digest(payload)));
        assert_eq!(tokio::fs::read(&staged.temp_path).await.unwrap(), payload);
        let temp_path = staged.temp_path.clone();
        drop(staged);
        assert!(!temp_path.exists());
        tokio::fs::remove_dir_all(root).await.unwrap();
    }

    #[tokio::test]
    async fn oversized_or_bad_digest_upload_removes_staging_file() {
        let root = std::env::temp_dir().join(format!("alocals3-stage-{}", Uuid::new_v4()));
        let oversized = stage_upload(&root, Body::from("too large"), 3, None)
            .await
            .err()
            .unwrap();
        assert_eq!(oversized.status, StatusCode::PAYLOAD_TOO_LARGE);

        let bad_digest = stage_upload(&root, Body::from("payload"), 0, Some([0; 16]))
            .await
            .err()
            .unwrap();
        assert_eq!(bad_digest.status, StatusCode::BAD_REQUEST);
        assert_eq!(bad_digest.detail, "BadDigest");

        let mut entries = tokio::fs::read_dir(&root).await.unwrap();
        assert!(entries.next_entry().await.unwrap().is_none());
        tokio::fs::remove_dir_all(root).await.unwrap();
    }
}
