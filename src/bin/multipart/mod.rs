//! Pending sessions and parts are separate SQL tables; unpublished part files
//! live outside the published-object GC tree.
//! Async file I/O, bounded-memory assembly, and nonblocking OS locks shared by
//! server processes. No database connection is held while clients upload parts.
use super::*;
use std::collections::BTreeMap;

#[derive(Serialize, Deserialize, Clone)]
pub(super) struct Part {
    part_number: u64,
    etag: String,
    size: i64,
    file: String,
}

#[derive(Serialize, Deserialize)]
struct Session {
    upload_id: String,
    bucket: String,
    key: String,
    content_type: Option<String>,
    metadata: HashMap<String, String>,
    parts: BTreeMap<u64, Part>,
    completed: Option<serde_json::Value>,
    // Old sessions are protected from expiry until their next activity.
    #[serde(default = "now_utc")]
    last_activity: String,
}

#[derive(Deserialize)]
pub(super) struct Completion {
    parts: Vec<PartReference>,
}

#[derive(Deserialize)]
pub(super) struct PartReference {
    part_number: u64,
    etag: String,
}

fn session_dir(storage: &Storage, id: &str) -> ApiResult<PathBuf> {
    // Accept only the exact generated format; never interpret an ID as a path.
    if Uuid::parse_str(id)
        .map(|v| v.simple().to_string())
        .ok()
        .as_deref()
        != Some(id)
    {
        return Err(ApiError::new(StatusCode::BAD_REQUEST, "Invalid upload ID"));
    }
    Ok(storage.objects_root().with_file_name("multipart").join(id))
}

fn missing(err: std::io::Error) -> ApiError {
    if err.kind() == std::io::ErrorKind::NotFound {
        ApiError::new(StatusCode::NOT_FOUND, "Upload not found")
    } else {
        err.into()
    }
}

async fn lock(dir: &Path) -> ApiResult<std::fs::File> {
    let file = tokio::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(dir.join("lock"))
        .await
        .map_err(missing)?
        .into_std()
        .await;
    // try_lock never blocks an executor thread. The OS releases the lock on
    // disconnect cancellation/process exit; no expiring leases or stale locks.
    loop {
        match file.try_lock() {
            Ok(()) => return Ok(file),
            Err(std::fs::TryLockError::WouldBlock) => {
                tokio::time::sleep(Duration::from_millis(10)).await
            }
            Err(std::fs::TryLockError::Error(err)) => return Err(err.into()),
        }
    }
}

// Each operation checks out a connection only for its SQL statements. File
// receive, lock waits and assembly never retain a database connection.
async fn read_session(storage: &Storage, id: &str, with_parts: bool) -> ApiResult<Session> {
    database!(storage, pool, {
        let row = sqlx::query("SELECT session FROM pending_uploads WHERE upload_id = $1 UNION ALL SELECT session FROM completed_uploads WHERE upload_id = $1")
            .bind(id).fetch_optional(pool).await?
            .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "Upload not found"))?;
        let mut session: Session = serde_json::from_str(&row.get::<String, _>(0))
            .map_err(|e| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        if with_parts && session.completed.is_none() {
            for row in sqlx::query("SELECT part_number, etag, size, file FROM pending_upload_parts WHERE upload_id = $1 ORDER BY part_number")
                .bind(id).fetch_all(pool).await? {
                let number = row.get::<i64, _>(0) as u64;
                session.parts.insert(number, Part { part_number: number, etag: row.get(1), size: row.get(2), file: row.get(3) });
            }
        }
        Ok(session)
    })
}

async fn insert_session(storage: &Storage, session: &Session) -> ApiResult<()> {
    let bucket = storage.bucket_row(&session.bucket).await?;
    let json = serde_json::to_string(session)
        .map_err(|e| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    database!(storage, pool, {
        sqlx::query(
            "INSERT INTO pending_uploads (upload_id, bucket_id, session) VALUES ($1, $2, $3)",
        )
        .bind(&session.upload_id)
        .bind(bucket.id)
        .bind(&json)
        .execute(pool)
        .await?;
        Ok(())
    })
}

async fn record_part(
    storage: &Storage,
    id: &str,
    part: &Part,
    max_size: usize,
) -> ApiResult<Option<String>> {
    database!(storage, pool, {
        let mut tx = pool.begin().await?;
        let old = sqlx::query(
            "SELECT file FROM pending_upload_parts WHERE upload_id = $1 AND part_number = $2",
        )
        .bind(id)
        .bind(part.part_number as i64)
        .fetch_optional(&mut *tx)
        .await?
        .map(|row| row.get::<String, _>(0));
        sqlx::query("INSERT INTO pending_upload_parts (upload_id, part_number, etag, size, file) VALUES ($1, $2, $3, $4, $5) ON CONFLICT(upload_id, part_number) DO UPDATE SET etag = EXCLUDED.etag, size = EXCLUDED.size, file = EXCLUDED.file")
            .bind(id).bind(part.part_number as i64).bind(&part.etag).bind(part.size).bind(&part.file).execute(&mut *tx).await?;
        if max_size != 0 {
            // Cast on PostgreSQL SUM(bigint), which otherwise returns NUMERIC.
            let total: i64 = sqlx::query("SELECT CAST(COALESCE(SUM(size), 0) AS BIGINT) FROM pending_upload_parts WHERE upload_id = $1")
                .bind(id).fetch_one(&mut *tx).await?.get(0);
            if total as u64 > max_size as u64 {
                return Err(ApiError::new(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "Total upload exceeds configured maximum",
                ));
            }
        }
        tx.commit().await?;
        Ok(old)
    })
}

async fn publish(
    storage: &Storage,
    session: &mut Session,
    mut staged: StagedUpload,
    conditions: &WriteConditions,
) -> ApiResult<(serde_json::Value, Option<String>)> {
    let bucket = storage.bucket_row(&session.bucket).await?;
    let path = format!(
        "{}/{}/{}",
        &staged.sha256[..2],
        &staged.sha256[2..4],
        staged.sha256
    );
    storage.register_active_write(&path).await;
    let result = async {
        install_staged_blob(&mut staged, &storage.objects_root().join(&path)).await?;
        let info = ObjectInfo {
            bucket: session.bucket.clone(),
            key: session.key.clone(),
            size: staged.size,
            content_type: session.content_type.clone().unwrap_or_else(|| {
                mime_guess::from_path(&session.key)
                    .first_raw()
                    .unwrap_or("application/octet-stream")
                    .to_string()
            }),
            etag: staged.etag.clone(),
            updated_at: now_utc(),
            metadata: session.metadata.clone(),
        };
        let value = serde_json::to_value(&info)
            .map_err(|e| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        session.completed = Some(value.clone());
        let receipt = serde_json::to_string(session)
            .map_err(|e| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        database!(storage, pool, {
            let mut tx = pool.begin().await?;
            lock_bucket!(tx, bucket.id);
            let (_, retired) =
                publish_object!(tx, bucket.id, &session.key, &path, info, conditions);
            sqlx::query(
                "INSERT INTO completed_uploads (upload_id, bucket_id, session) VALUES ($1, $2, $3)",
            )
            .bind(&session.upload_id)
            .bind(bucket.id)
            .bind(&receipt)
            .execute(&mut *tx)
            .await?;
            sqlx::query("DELETE FROM pending_upload_parts WHERE upload_id = $1")
                .bind(&session.upload_id)
                .execute(&mut *tx)
                .await?;
            sqlx::query("DELETE FROM pending_uploads WHERE upload_id = $1")
                .bind(&session.upload_id)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            Ok((value, retired))
        })
    }
    .await;
    storage.unregister_active_write(&path).await;
    result
}

fn active(session: &Session) -> ApiResult<()> {
    if session.completed.is_some() {
        return Err(ApiError::new(
            StatusCode::CONFLICT,
            "Upload already completed",
        ));
    }
    Ok(())
}

pub(super) async fn create(
    State(state): State<AppState>,
    AxumPath((bucket, key)): AxumPath<(String, String)>,
    headers: HeaderMap,
) -> ApiResult<Json<serde_json::Value>> {
    validate_bucket(&bucket)?;
    let key = normalize_key(&key)?;
    state.storage.bucket_row(&bucket).await?;
    let session = Session {
        upload_id: Uuid::new_v4().simple().to_string(),
        bucket,
        key,
        content_type: header_str(&headers, header::CONTENT_TYPE.as_str()).map(str::to_string),
        metadata: extract_user_metadata(&headers)?,
        parts: BTreeMap::new(),
        completed: None,
        last_activity: now_utc(),
    };
    let dir = session_dir(&state.storage, &session.upload_id)?;
    tokio::fs::create_dir_all(&dir).await?;
    tokio::fs::File::create(dir.join("lock")).await?;
    tokio::fs::File::create(dir.join("activity")).await?;
    insert_session(&state.storage, &session).await?;
    Ok(Json(
        serde_json::json!({"upload_id": session.upload_id, "bucket": session.bucket, "key": session.key}),
    ))
}

pub(super) async fn list(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
) -> ApiResult<Json<serde_json::Value>> {
    let dir = session_dir(&state.storage, &id)?;
    let _lock = lock(&dir).await?;
    let session = read_session(&state.storage, &id, true).await?;
    let parts: Vec<_> = session
        .parts
        .values()
        .map(|p| {
            serde_json::json!({
                "part_number": p.part_number, "etag": p.etag, "size": p.size,
            })
        })
        .collect();
    Ok(Json(
        serde_json::json!({"upload_id": id, "bucket": session.bucket,
        "key": session.key, "parts": parts, "completed": session.completed}),
    ))
}

pub(super) async fn upload_part(
    State(state): State<AppState>,
    AxumPath((id, number)): AxumPath<(String, u64)>,
    headers: HeaderMap,
    body: Body,
) -> ApiResult<Json<serde_json::Value>> {
    if number == 0 || number > i64::MAX as u64 {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "Part numbers start at 1",
        ));
    }
    let dir = session_dir(&state.storage, &id)?;
    let _activity = activity_lock(&dir).await?;
    {
        let _lock = lock(&dir).await?;
        active(&read_session(&state.storage, &id, false).await?)?;
        touch(&state.storage, &id).await?;
    }
    validate_content_length(&headers, state.max_upload_size)?;
    let md5 = parse_content_md5(&headers)?;
    let _permit = state.upload_semaphore.acquire().await.map_err(|_| {
        ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "Upload service unavailable",
        )
    })?;
    // Receive outside the session lock, allowing parallel parts. Staging lives
    // outside the session directory so concurrent abort cannot recreate it.
    let staging = state
        .storage
        .objects_root()
        .with_file_name("multipart-staging")
        .join(&id);
    let mut staged = stage_upload(&staging, body, state.max_upload_size, md5).await?;
    let _lock = lock(&dir).await?;
    let session = read_session(&state.storage, &id, false).await?;
    active(&session)?;
    let name = format!("part-{}", Uuid::new_v4().simple());
    tokio::fs::rename(&staged.temp_path, dir.join(&name)).await?;
    staged.temp_path.clear();
    let _ = tokio::fs::remove_dir(&staging).await;
    let part = Part {
        part_number: number,
        etag: staged.etag.clone(),
        size: staged.size,
        file: name,
    };
    let previous = match record_part(&state.storage, &id, &part, state.max_upload_size).await {
        Ok(old) => old,
        Err(err) => {
            let _ = tokio::fs::remove_file(dir.join(&part.file)).await;
            return Err(err);
        }
    };
    if let Some(old) = previous {
        let _ = tokio::fs::remove_file(dir.join(old)).await;
    }
    touch(&state.storage, &id).await?;
    Ok(Json(
        serde_json::json!({"part_number": number, "etag": staged.etag, "size": staged.size}),
    ))
}

async fn assemble(dir: &Path, session: &Session, max_size: usize) -> ApiResult<StagedUpload> {
    let path = dir.join(format!(".complete-{}.tmp", Uuid::new_v4().simple()));
    let guard = TempFileGuard::new(path.clone());
    let mut output = tokio::fs::File::create(&path).await?;
    let mut md5 = md5::Context::new();
    let mut sha256 = Sha256::new();
    let mut total = 0_i64;
    let mut buffer = vec![0_u8; DOWNLOAD_CHUNK_SIZE];
    for part in session.parts.values() {
        let mut input = tokio::fs::File::open(dir.join(&part.file)).await?;
        let mut part_md5 = md5::Context::new();
        let mut size = 0_i64;
        loop {
            let n = input.read(&mut buffer).await?;
            if n == 0 {
                break;
            }
            total = total.checked_add(n as i64).ok_or_else(|| {
                ApiError::new(StatusCode::PAYLOAD_TOO_LARGE, "Upload size overflow")
            })?;
            if max_size != 0 && total as u64 > max_size as u64 {
                return Err(ApiError::new(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "Total upload exceeds configured maximum",
                ));
            }
            size += n as i64;
            part_md5.consume(&buffer[..n]);
            md5.consume(&buffer[..n]);
            sha256.update(&buffer[..n]);
            output.write_all(&buffer[..n]).await?;
        }
        if size != part.size || format!("{:x}", part_md5.compute()) != part.etag {
            return Err(ApiError::new(
                StatusCode::CONFLICT,
                "Stored part checksum mismatch; re-upload part",
            ));
        }
    }
    output.sync_all().await?;
    drop(output);
    Ok(StagedUpload {
        temp_path: guard.into_path(),
        size: total,
        etag: format!("{:x}", md5.compute()),
        sha256: format!("{:x}", sha256.finalize()),
    })
}

pub(super) async fn complete(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    headers: HeaderMap,
    Json(request): Json<Completion>,
) -> ApiResult<Json<serde_json::Value>> {
    let dir = session_dir(&state.storage, &id)?;
    let _permit = state.upload_semaphore.acquire().await.map_err(|_| {
        ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "Upload service unavailable",
        )
    })?;
    let _lock = lock(&dir).await?;
    let mut session = read_session(&state.storage, &id, true).await?;
    // Require the exact contiguous manifest. Never silently omit uploaded parts,
    // accept duplicate numbers, reorder caller input, or commit stale retries.
    if request.parts.len() != session.parts.len()
        || request.parts.iter().enumerate().any(|(i, p)| {
            p.part_number != i as u64 + 1
                || session
                    .parts
                    .get(&p.part_number)
                    .map(|stored| stored.etag != p.etag.trim_matches('"'))
                    .unwrap_or(true)
        })
    {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "Parts must match all uploaded parts in order, numbered consecutively from 1",
        ));
    }
    if let Some(result) = &session.completed {
        clean_part_files(&dir).await;
        return Ok(Json(result.clone()));
    }
    let staged = assemble(&dir, &session, state.max_upload_size).await?;
    let (result, retired) = publish(
        &state.storage,
        &mut session,
        staged,
        &WriteConditions::from_headers(&headers),
    )
    .await?;
    if let (Some(gc), Some(path)) = (&state.gc, retired) {
        gc.reclaim_path(path);
    }
    // Receipt and published object are committed in the same transaction.
    clean_part_files(&dir).await;
    Ok(Json(result))
}

pub(super) async fn abort(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
) -> ApiResult<StatusCode> {
    let dir = session_dir(&state.storage, &id)?;
    let _lock = match lock(&dir).await {
        Ok(lock) => lock,
        Err(err) if err.status == StatusCode::NOT_FOUND => {
            clean_staging(&state.storage, &id).await;
            return Ok(StatusCode::NO_CONTENT);
        }
        Err(err) => return Err(err),
    };
    remove_session(&state.storage, &id, &dir).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn remove_session(storage: &Storage, id: &str, dir: &Path) -> ApiResult<StatusCode> {
    // Deleting a completed receipt never deletes the published object.
    database!(storage, pool, {
        let mut tx = pool.begin().await?;
        sqlx::query("DELETE FROM pending_upload_parts WHERE upload_id = $1")
            .bind(id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM pending_uploads WHERE upload_id = $1")
            .bind(id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM completed_uploads WHERE upload_id = $1")
            .bind(id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
    });
    tokio::fs::remove_dir_all(&dir).await.map_err(missing)?;
    clean_staging(storage, id).await;
    Ok(StatusCode::NO_CONTENT)
}

// Also removes files left by a crash between file rename and SQL commit.
async fn clean_part_files(dir: &Path) {
    if let Ok(mut entries) = tokio::fs::read_dir(dir).await {
        while let Ok(Some(entry)) = entries.next_entry().await {
            if entry.file_name() != "lock" && entry.file_name() != "activity" {
                if let Err(err) = tokio::fs::remove_file(entry.path()).await {
                    tracing::warn!(error = %err, "Failed to clean completed upload part");
                }
            }
        }
    }
}

pub(super) async fn bucket_sessions(storage: &Storage, bucket: &str) -> ApiResult<Vec<String>> {
    let bucket_id = storage.bucket_row(bucket).await?.id;
    database!(storage, pool, {
        Ok(sqlx::query("SELECT upload_id FROM pending_uploads WHERE bucket_id = $1 UNION ALL SELECT upload_id FROM completed_uploads WHERE bucket_id = $1")
            .bind(bucket_id).fetch_all(pool).await?.iter().map(|row| row.get(0)).collect())
    })
}

pub(super) async fn clean_bucket_sessions(storage: &Storage, ids: &[String]) {
    for id in ids {
        clean_staging(storage, id).await;
        let Ok(dir) = session_dir(storage, id) else {
            continue;
        };
        if let Ok(_lock) = lock(&dir).await {
            if let Err(err) = tokio::fs::remove_dir_all(&dir).await {
                tracing::warn!(upload_id = %id, error = %err, "Failed to clean removed bucket upload");
            }
        }
    }
}

async fn clean_staging(storage: &Storage, id: &str) {
    let path = storage
        .objects_root()
        .with_file_name("multipart-staging")
        .join(id);
    if let Err(err) = tokio::fs::remove_dir_all(path).await {
        if err.kind() != std::io::ErrorKind::NotFound {
            tracing::warn!(upload_id = %id, error = %err, "Failed to clean upload staging");
        }
    }
}

// Shared for the complete receive operation, including time spent waiting on
// admission. Cleanup takes an exclusive nonblocking lock on the same inode.
async fn activity_lock(dir: &Path) -> ApiResult<std::fs::File> {
    let file = tokio::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(dir.join("activity"))
        .await
        .map_err(missing)?
        .into_std()
        .await;
    loop {
        match file.try_lock_shared() {
            Ok(()) => return Ok(file),
            Err(std::fs::TryLockError::WouldBlock) => {
                tokio::time::sleep(Duration::from_millis(10)).await
            }
            Err(std::fs::TryLockError::Error(e)) => return Err(e.into()),
        }
    }
}
async fn touch(storage: &Storage, id: &str) -> ApiResult<()> {
    let mut session = read_session(storage, id, false).await?;
    session.last_activity = now_utc();
    let json = serde_json::to_string(&session)
        .map_err(|e| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    database!(storage, pool, {
        sqlx::query("UPDATE pending_uploads SET session = $2 WHERE upload_id = $1")
            .bind(id)
            .bind(json)
            .execute(pool)
            .await?;
    });
    Ok(())
}

#[derive(Deserialize)]
pub(super) struct UploadQuery {
    bucket: String,
    limit: Option<i64>,
    continuation_token: Option<String>,
}

pub(super) async fn list_uploads(
    State(state): State<AppState>,
    Query(query): Query<UploadQuery>,
) -> ApiResult<Json<serde_json::Value>> {
    let bucket = state.storage.bucket_row(&query.bucket).await?;
    let limit = query.limit.unwrap_or(100).clamp(1, 1000);
    let mut uploads = Vec::new();
    database!(&*state.storage, pool, {
        let rows = sqlx::query("SELECT u.session, CAST(COALESCE(SUM(p.size), 0) AS BIGINT), COUNT(p.part_number) FROM pending_uploads u
            LEFT JOIN pending_upload_parts p ON p.upload_id = u.upload_id
            WHERE u.bucket_id = $1 AND u.upload_id > $2 GROUP BY u.upload_id, u.session ORDER BY u.upload_id LIMIT $3")
            .bind(bucket.id).bind(query.continuation_token.as_deref().unwrap_or("")).bind(limit + 1).fetch_all(pool).await?;
        for row in rows {
            let session: Session = serde_json::from_str(&row.get::<String, _>(0))
                .map_err(|e| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
            uploads.push(serde_json::json!({"upload_id": session.upload_id, "bucket": session.bucket, "key": session.key,
                "last_activity": session.last_activity, "size": row.get::<i64, _>(1), "part_count": row.get::<i64, _>(2)}));
        }
    });
    let truncated = uploads.len() > limit as usize;
    uploads.truncate(limit as usize);
    let next = if truncated {
        uploads.last().map(|v| v["upload_id"].clone())
    } else {
        None
    };
    Ok(Json(
        serde_json::json!({"uploads": uploads, "is_truncated": truncated, "next_continuation_token": next}),
    ))
}
#[derive(Deserialize)]
pub(super) struct AbortMany {
    upload_ids: Vec<String>,
}
pub(super) async fn abort_many(
    State(state): State<AppState>,
    Json(request): Json<AbortMany>,
) -> ApiResult<Json<serde_json::Value>> {
    if request.upload_ids.len() > 100 {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "At most 100 uploads per batch",
        ));
    }
    let mut results = Vec::new();
    for id in request.upload_ids {
        let status = match abort(State(state.clone()), AxumPath(id.clone())).await {
            Ok(s) => s,
            Err(e) => e.status,
        };
        results.push(serde_json::json!({"upload_id": id, "status": status.as_u16()}));
    }
    Ok(Json(serde_json::json!({"results": results})))
}

pub(super) fn start_cleanup(storage: Arc<Storage>, idle: Duration, interval: Duration) {
    tokio::spawn(async move {
        let mut timer = tokio::time::interval(interval);
        timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            timer.tick().await;
            match cleanup_idle(&storage, idle).await {
                Ok(count) => tracing::debug!(count, "multipart cleanup complete"),
                Err(e) => tracing::warn!(error = ?e, "multipart cleanup failed"),
            }
        }
    });
}
pub(super) async fn cleanup_idle(storage: &Storage, idle: Duration) -> ApiResult<u64> {
    let mut after = String::new();
    let mut removed = 0;
    loop {
        let ids: Vec<String> = database!(storage, pool, {
            sqlx::query("SELECT upload_id FROM pending_uploads WHERE upload_id > $1 ORDER BY upload_id LIMIT 256")
                .bind(&after).fetch_all(pool).await?.iter().map(|r| r.get(0)).collect()
        });
        if ids.is_empty() {
            break;
        }
        for id in ids {
            after = id.clone();
            let session = match read_session(storage, &id, false).await {
                Ok(s) => s,
                Err(e) if e.status == StatusCode::NOT_FOUND => continue,
                Err(e) => return Err(e),
            };
            if !rfc3339_older_than(&session.last_activity, idle) {
                continue;
            }
            let dir = session_dir(storage, &id)?;
            let session_lock = match std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(dir.join("lock"))
            {
                Ok(f) => f,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e.into()),
            };
            if session_lock.try_lock().is_err() {
                continue;
            }
            let activity = std::fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(dir.join("activity"))?;
            if activity.try_lock().is_err() {
                continue;
            }
            // Recheck inside both locks: a receiver may have refreshed the session.
            let session = match read_session(storage, &id, false).await {
                Ok(s) => s,
                Err(e) if e.status == StatusCode::NOT_FOUND => continue,
                Err(e) => return Err(e),
            };
            if session.completed.is_some() || !rfc3339_older_than(&session.last_activity, idle) {
                continue;
            }
            remove_session(storage, &id, &dir).await?;
            removed += 1;
        }
    }
    Ok(removed)
}
