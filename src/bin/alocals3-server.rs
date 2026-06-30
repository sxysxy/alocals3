use axum::body::{Body, Bytes};
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::{header, HeaderMap, HeaderValue, Request, StatusCode};
use axum::middleware::{from_fn, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, put};
use axum::{Json, Router};
use base64::Engine;
use chrono::{DateTime, SecondsFormat, Utc};
use clap::Parser;
use serde::Serialize;
use sha2::{Digest, Sha256};
use sqlx::postgres::{PgPoolOptions, PgRow};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{PgPool, Row, SqlitePool};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::io::AsyncWriteExt;
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

static HEALTH_COUNTER: AtomicUsize = AtomicUsize::new(1);

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
}

#[derive(Clone)]
struct AppState {
    storage: Arc<Storage>,
}

enum Storage {
    Sqlite {
        pool: SqlitePool,
        objects_root: PathBuf,
    },
    Postgres {
        pool: PgPool,
        objects_root: PathBuf,
    },
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
}

struct StoredObject {
    info: ObjectInfo,
    body: Vec<u8>,
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

fn init_logging(log_level: &str) -> anyhow::Result<()> {
    let filter = EnvFilter::try_new(log_level).or_else(|_| EnvFilter::try_new("info"))?;
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
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
    let should_log = path == "/healthz" || path.starts_with("/s3");
    if !should_log {
        return next.run(request).await;
    }

    let started = Instant::now();
    let response = next.run(request).await;
    let duration_ms = started.elapsed().as_secs_f64() * 1000.0;
    let status = response.status();

    if path == "/healthz" {
        let hit = HEALTH_COUNTER.fetch_add(1, Ordering::Relaxed);
        let sampled = status.is_client_error() || status.is_server_error() || hit % 30 == 0;
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
    if parts.len() < 2 || parts[0] != "s3" {
        return ("-", "-".to_string());
    }
    let bucket = parts[1];
    if parts.len() == 2 || parts.get(2) == Some(&"objects") {
        return (bucket, "-".to_string());
    }
    (bucket, parts[2..].join("/"))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    init_logging(&args.log_level)?;
    tracing::info!(
        host = %args.host,
        port = args.port,
        database_backend = database_backend(&args.database_url),
        storage_root = %args.storage_root.display(),
        "starting alocals3 server"
    );
    let storage = Arc::new(Storage::connect(&args.database_url, args.storage_root).await?);
    let app = Router::new()
        .route("/healthz", get(health))
        .route("/s3", get(list_buckets))
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
        .layer(from_fn(request_logging))
        .with_state(AppState { storage });

    let addr: SocketAddr = format!("{}:{}", args.host, args.port).parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "alocals3 server listening");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({"status": "ok"}))
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
    state.storage.delete_bucket(&bucket).await?;
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
        .clamp(1, 10000);
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
    headers: HeaderMap,
    body: Bytes,
) -> ApiResult<Response> {
    validate_content_md5(&headers, &body)?;
    let existing = state.storage.get_object_info(&bucket, &key).await?;

    if let Some(value) = header_str(&headers, header::IF_NONE_MATCH.as_str()) {
        if let Some(info) = &existing {
            if etag_matches(value, &info.etag) {
                return Err(ApiError::new(
                    StatusCode::PRECONDITION_FAILED,
                    "If-None-Match precondition failed",
                ));
            }
        }
    }
    if let Some(value) = header_str(&headers, header::IF_MATCH.as_str()) {
        if existing
            .as_ref()
            .map(|info| etag_matches(value, &info.etag))
            .unwrap_or(false)
            == false
        {
            return Err(ApiError::new(
                StatusCode::PRECONDITION_FAILED,
                "If-Match precondition failed",
            ));
        }
    }

    let content_type = header_str(&headers, header::CONTENT_TYPE.as_str()).map(str::to_string);
    let (info, created) = state
        .storage
        .put_object(&bucket, &key, &body, content_type.as_deref())
        .await?;
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
    object_response(
        state.storage.get_object(&bucket, &key).await?,
        &headers,
        true,
    )
}

async fn head_object(
    State(state): State<AppState>,
    AxumPath((bucket, key)): AxumPath<(String, String)>,
    headers: HeaderMap,
) -> ApiResult<Response> {
    object_response(
        state.storage.get_object(&bucket, &key).await?,
        &headers,
        false,
    )
}

async fn delete_object(
    State(state): State<AppState>,
    AxumPath((bucket, key)): AxumPath<(String, String)>,
) -> ApiResult<StatusCode> {
    state.storage.delete_object(&bucket, &key).await?;
    Ok(StatusCode::NO_CONTENT)
}

impl Storage {
    async fn connect(database_url: &str, root: PathBuf) -> anyhow::Result<Self> {
        let objects_root = root.join("objects");
        tokio::fs::create_dir_all(&objects_root).await?;
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
            return Ok(Self::Sqlite { pool, objects_root });
        }
        let pool = PgPoolOptions::new()
            .max_connections(32)
            .connect(database_url)
            .await?;
        init_pg(&pool).await?;
        Ok(Self::Postgres { pool, objects_root })
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
        if self.object_count(bucket_row.id).await? > 0 {
            return Err(ApiError::new(StatusCode::CONFLICT, "Bucket is not empty"));
        }
        match self {
            Self::Sqlite { pool, .. } => {
                sqlx::query("DELETE FROM buckets WHERE id = ?1")
                    .bind(bucket_row.id)
                    .execute(pool)
                    .await?;
            }
            Self::Postgres { pool, .. } => {
                sqlx::query("DELETE FROM buckets WHERE id = $1")
                    .bind(bucket_row.id)
                    .execute(pool)
                    .await?;
            }
        }
        Ok(())
    }

    async fn list_objects(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        limit: i64,
    ) -> ApiResult<Vec<ObjectInfo>> {
        let bucket_row = self.bucket_row(bucket).await?;
        let rows = self.object_rows(bucket_row.id).await?;
        let prefix = prefix.unwrap_or("");
        Ok(rows
            .into_iter()
            .filter(|row| prefix.is_empty() || row.key.starts_with(prefix))
            .take(limit as usize)
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
        let bucket_row = self.bucket_row(bucket).await?;
        let rows = self.object_rows(bucket_row.id).await?;
        let mut contents = Vec::new();
        let mut common_prefixes = Vec::new();
        let mut seen = HashSet::new();
        let mut key_count = 0_i64;
        let mut next_token = None;

        for row in rows {
            if !prefix.is_empty() && !row.key.starts_with(prefix) {
                continue;
            }
            if continuation_token
                .map(|token| row.key.as_str() <= token)
                .unwrap_or(false)
            {
                continue;
            }
            if !delimiter.is_empty() {
                let tail = if !prefix.is_empty() && row.key.starts_with(prefix) {
                    &row.key[prefix.len()..]
                } else {
                    row.key.as_str()
                };
                if let Some(pos) = tail.find(delimiter) {
                    let cp = row.key[..prefix.len() + pos + delimiter.len()].to_string();
                    if seen.insert(cp.clone()) {
                        if key_count >= max_keys {
                            next_token = Some(row.key);
                            break;
                        }
                        common_prefixes.push(cp);
                        key_count += 1;
                    }
                    continue;
                }
            }
            if key_count >= max_keys {
                next_token = Some(row.key);
                break;
            }
            contents.push(row.info(bucket));
            key_count += 1;
        }

        Ok(ListObjectsV2Result {
            bucket: bucket.to_string(),
            prefix: prefix.to_string(),
            delimiter: delimiter.to_string(),
            max_keys,
            key_count,
            is_truncated: next_token.is_some(),
            next_continuation_token: next_token,
            contents,
            common_prefixes,
        })
    }

    async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        body: &[u8],
        content_type: Option<&str>,
    ) -> ApiResult<(ObjectInfo, bool)> {
        validate_bucket(bucket)?;
        let normalized_key = normalize_key(key)?;
        let bucket_row = self.bucket_row(bucket).await?;
        let etag = format!("{:x}", md5::compute(body));
        let digest = hex_sha256(body);
        let relative_path = format!("{}/{}/{}", &digest[..2], &digest[2..4], digest);
        let objects_root = self.objects_root();
        atomic_write_blob(&objects_root.join(&relative_path), body).await?;
        let now = now_utc();
        let final_content_type = content_type.map(str::to_string).unwrap_or_else(|| {
            mime_guess::from_path(&normalized_key)
                .first_raw()
                .unwrap_or("application/octet-stream")
                .to_string()
        });
        let created = self
            .upsert_object(
                bucket_row.id,
                &normalized_key,
                &relative_path,
                body.len() as i64,
                &final_content_type,
                &etag,
                &now,
            )
            .await?;
        let row = self.object_row(bucket_row.id, &normalized_key).await?;
        Ok((row.info(bucket), created))
    }

    async fn get_object(&self, bucket: &str, key: &str) -> ApiResult<StoredObject> {
        validate_bucket(bucket)?;
        let normalized_key = normalize_key(key)?;
        let bucket_row = self.bucket_row(bucket).await?;
        let row = self.object_row(bucket_row.id, &normalized_key).await?;
        let body = tokio::fs::read(self.objects_root().join(&row.file_path))
            .await
            .map_err(|_| ApiError::new(StatusCode::NOT_FOUND, "Object data missing"))?;
        Ok(StoredObject {
            info: row.info(bucket),
            body,
        })
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

    async fn delete_object(&self, bucket: &str, key: &str) -> ApiResult<()> {
        validate_bucket(bucket)?;
        let normalized_key = normalize_key(key)?;
        let bucket_row = self.bucket_row(bucket).await?;
        let changed = match self {
            Self::Sqlite { pool, .. } => {
                sqlx::query("DELETE FROM objects WHERE bucket_id = ?1 AND key = ?2")
                    .bind(bucket_row.id)
                    .bind(&normalized_key)
                    .execute(pool)
                    .await?
                    .rows_affected()
            }
            Self::Postgres { pool, .. } => {
                sqlx::query("DELETE FROM objects WHERE bucket_id = $1 AND key = $2")
                    .bind(bucket_row.id)
                    .bind(&normalized_key)
                    .execute(pool)
                    .await?
                    .rows_affected()
            }
        };
        if changed == 0 {
            return Err(ApiError::new(StatusCode::NOT_FOUND, "Object not found"));
        }
        Ok(())
    }

    fn objects_root(&self) -> PathBuf {
        match self {
            Self::Sqlite { objects_root, .. } | Self::Postgres { objects_root, .. } => {
                objects_root.clone()
            }
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

    async fn object_count(&self, bucket_id: i64) -> ApiResult<i64> {
        match self {
            Self::Sqlite { pool, .. } => {
                let row = sqlx::query("SELECT COUNT(*) FROM objects WHERE bucket_id = ?1")
                    .bind(bucket_id)
                    .fetch_one(pool)
                    .await?;
                Ok(row.get(0))
            }
            Self::Postgres { pool, .. } => {
                let row = sqlx::query("SELECT COUNT(*) FROM objects WHERE bucket_id = $1")
                    .bind(bucket_id)
                    .fetch_one(pool)
                    .await?;
                Ok(row.get(0))
            }
        }
    }

    async fn object_rows(&self, bucket_id: i64) -> ApiResult<Vec<ObjectRow>> {
        match self {
            Self::Sqlite { pool, .. } => {
                let rows = sqlx::query("SELECT key, file_path, size, content_type, etag, updated_at FROM objects WHERE bucket_id = ?1 ORDER BY key ASC")
                    .bind(bucket_id)
                    .fetch_all(pool)
                    .await?;
                Ok(rows.into_iter().map(object_row_from_sqlite).collect())
            }
            Self::Postgres { pool, .. } => {
                let rows = sqlx::query("SELECT key, file_path, size, content_type, etag, updated_at FROM objects WHERE bucket_id = $1 ORDER BY key ASC")
                    .bind(bucket_id)
                    .fetch_all(pool)
                    .await?;
                Ok(rows.into_iter().map(object_row_from_pg).collect())
            }
        }
    }

    async fn object_row(&self, bucket_id: i64, key: &str) -> ApiResult<ObjectRow> {
        self.object_row_optional(bucket_id, key)
            .await?
            .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "Object not found"))
    }

    async fn object_row_optional(&self, bucket_id: i64, key: &str) -> ApiResult<Option<ObjectRow>> {
        match self {
            Self::Sqlite { pool, .. } => {
                let row = sqlx::query("SELECT key, file_path, size, content_type, etag, updated_at FROM objects WHERE bucket_id = ?1 AND key = ?2")
                    .bind(bucket_id)
                    .bind(key)
                    .fetch_optional(pool)
                    .await?;
                Ok(row.map(object_row_from_sqlite))
            }
            Self::Postgres { pool, .. } => {
                let row = sqlx::query("SELECT key, file_path, size, content_type, etag, updated_at FROM objects WHERE bucket_id = $1 AND key = $2")
                    .bind(bucket_id)
                    .bind(key)
                    .fetch_optional(pool)
                    .await?;
                Ok(row.map(object_row_from_pg))
            }
        }
    }

    async fn upsert_object(
        &self,
        bucket_id: i64,
        key: &str,
        file_path: &str,
        size: i64,
        content_type: &str,
        etag: &str,
        updated_at: &str,
    ) -> ApiResult<bool> {
        match self {
            Self::Sqlite { pool, .. } => {
                let result = sqlx::query(
                    "INSERT INTO objects (bucket_id, key, file_path, size, content_type, etag, updated_at, created_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?7)
                     ON CONFLICT(bucket_id, key) DO UPDATE SET
                        file_path = excluded.file_path,
                        size = excluded.size,
                        content_type = excluded.content_type,
                        etag = excluded.etag,
                        updated_at = excluded.updated_at
                     RETURNING created_at = updated_at",
                )
                .bind(bucket_id)
                .bind(key)
                .bind(file_path)
                .bind(size)
                .bind(content_type)
                .bind(etag)
                .bind(updated_at)
                .fetch_one(pool)
                .await?;
                Ok(result.get(0))
            }
            Self::Postgres { pool, .. } => {
                let row = sqlx::query(
                    "INSERT INTO objects (bucket_id, key, file_path, size, content_type, etag, updated_at, created_at)
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $7)
                     ON CONFLICT(bucket_id, key) DO UPDATE SET
                        file_path = EXCLUDED.file_path,
                        size = EXCLUDED.size,
                        content_type = EXCLUDED.content_type,
                        etag = EXCLUDED.etag,
                        updated_at = EXCLUDED.updated_at
                     RETURNING xmax = 0",
                )
                .bind(bucket_id)
                .bind(key)
                .bind(file_path)
                .bind(size)
                .bind(content_type)
                .bind(etag)
                .bind(updated_at)
                .fetch_one(pool)
                .await?;
                Ok(row.get(0))
            }
        }
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
    }
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
            UNIQUE(bucket_id, key)
        )",
    )
    .execute(pool)
    .await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_objects_bucket_key ON objects (bucket_id, key)")
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
            UNIQUE(bucket_id, key)
        )",
    )
    .execute(pool)
    .await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_objects_bucket_key ON objects (bucket_id, key)")
        .execute(pool)
        .await?;
    Ok(())
}

fn object_response(
    obj: StoredObject,
    headers: &HeaderMap,
    include_body: bool,
) -> ApiResult<Response> {
    let mut response_headers = HeaderMap::new();
    response_headers.insert(
        header::ETAG,
        HeaderValue::from_str(&format!("\"{}\"", obj.info.etag)).unwrap(),
    );
    response_headers.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    response_headers.insert(
        header::LAST_MODIFIED,
        HeaderValue::from_str(&http_date(&obj.info.updated_at))
            .unwrap_or_else(|_| HeaderValue::from_static("Thu, 01 Jan 1970 00:00:00 GMT")),
    );

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

fn validate_content_md5(headers: &HeaderMap, body: &[u8]) -> ApiResult<()> {
    let Some(value) = header_str(headers, "content-md5") else {
        return Ok(());
    };
    let provided = base64::engine::general_purpose::STANDARD
        .decode(value)
        .map_err(|_| ApiError::new(StatusCode::BAD_REQUEST, "Invalid Content-MD5 header"))?;
    let actual = md5::compute(body);
    if provided != actual.0 {
        return Err(ApiError::new(StatusCode::BAD_REQUEST, "BadDigest"));
    }
    Ok(())
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

async fn atomic_write_blob(target_path: &Path, body: &[u8]) -> ApiResult<()> {
    if let Some(parent) = target_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let tmp_path = target_path.with_file_name(format!(
        ".{}.{}.tmp",
        target_path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("blob"),
        Uuid::new_v4().simple()
    ));
    let mut file = tokio::fs::File::create(&tmp_path).await?;
    file.write_all(body).await?;
    file.sync_all().await?;
    tokio::fs::rename(&tmp_path, target_path).await?;
    Ok(())
}

fn hex_sha256(body: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(body);
    format!("{:x}", hasher.finalize())
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
