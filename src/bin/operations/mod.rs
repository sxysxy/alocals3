//! Bucket maintenance: bulk deletion and transactionally maintained usage.
use super::*;

pub(super) async fn init(storage: &Storage) -> ApiResult<()> {
    database!(storage, pool, {
        let mut tx = pool.begin().await?;
        if matches!(storage, Storage::Postgres { .. }) {
            sqlx::query("LOCK TABLE objects IN SHARE ROW EXCLUSIVE MODE")
                .execute(&mut *tx)
                .await?;
        }
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS bucket_usage (
            bucket_id BIGINT PRIMARY KEY REFERENCES buckets(id) ON DELETE CASCADE,
            object_count BIGINT NOT NULL DEFAULT 0, logical_bytes BIGINT NOT NULL DEFAULT 0,
            max_bytes BIGINT NOT NULL DEFAULT 0)",
        )
        .execute(&mut *tx)
        .await?;
        sqlx::query("CREATE TABLE IF NOT EXISTS lifecycle_config (
            bucket_id BIGINT PRIMARY KEY REFERENCES buckets(id) ON DELETE CASCADE, rules TEXT NOT NULL)")
            .execute(&mut *tx).await?;
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_objects_expiration ON objects(bucket_id, updated_at, id)").execute(&mut *tx).await?;
        sqlx::query("INSERT INTO bucket_usage(bucket_id, object_count, logical_bytes)
            SELECT b.id, COUNT(o.id), COALESCE(SUM(o.size), 0) FROM buckets b
            LEFT JOIN objects o ON o.bucket_id = b.id GROUP BY b.id ON CONFLICT(bucket_id) DO NOTHING")
            .execute(&mut *tx).await?;
        if matches!(storage, Storage::Sqlite { .. }) {
            for sql in [
                "CREATE TRIGGER IF NOT EXISTS usage_insert AFTER INSERT ON objects BEGIN
                    INSERT INTO bucket_usage(bucket_id, object_count, logical_bytes) VALUES (NEW.bucket_id, 1, NEW.size)
                    ON CONFLICT(bucket_id) DO UPDATE SET object_count = object_count + 1, logical_bytes = logical_bytes + NEW.size;
                    SELECT CASE WHEN EXISTS(SELECT 1 FROM bucket_usage WHERE bucket_id = NEW.bucket_id AND max_bytes > 0 AND logical_bytes > max_bytes)
                        THEN RAISE(ABORT, 'BucketQuotaExceeded') END; END",
                "CREATE TRIGGER IF NOT EXISTS usage_update AFTER UPDATE OF size ON objects BEGIN
                    UPDATE bucket_usage SET logical_bytes = logical_bytes + NEW.size - OLD.size WHERE bucket_id = NEW.bucket_id;
                    SELECT CASE WHEN NEW.size > OLD.size AND EXISTS(SELECT 1 FROM bucket_usage WHERE bucket_id = NEW.bucket_id AND max_bytes > 0 AND logical_bytes > max_bytes)
                        THEN RAISE(ABORT, 'BucketQuotaExceeded') END; END",
                "CREATE TRIGGER IF NOT EXISTS usage_delete AFTER DELETE ON objects BEGIN
                    UPDATE bucket_usage SET object_count = object_count - 1, logical_bytes = logical_bytes - OLD.size WHERE bucket_id = OLD.bucket_id; END",
            ] { sqlx::query(sql).execute(&mut *tx).await?; }
        } else {
            sqlx::query("CREATE OR REPLACE FUNCTION alocals3_update_usage() RETURNS TRIGGER AS $$
                DECLARE used BIGINT; quota BIGINT;
                BEGIN
                    IF TG_OP = 'DELETE' THEN
                        UPDATE bucket_usage SET object_count = object_count - 1, logical_bytes = logical_bytes - OLD.size WHERE bucket_id = OLD.bucket_id;
                        RETURN OLD;
                    ELSIF TG_OP = 'INSERT' THEN
                        INSERT INTO bucket_usage(bucket_id, object_count, logical_bytes) VALUES (NEW.bucket_id, 1, NEW.size)
                        ON CONFLICT(bucket_id) DO UPDATE SET object_count = bucket_usage.object_count + 1, logical_bytes = bucket_usage.logical_bytes + NEW.size;
                    ELSE
                        UPDATE bucket_usage SET logical_bytes = logical_bytes + NEW.size - OLD.size WHERE bucket_id = NEW.bucket_id;
                    END IF;
                    SELECT logical_bytes, max_bytes INTO used, quota FROM bucket_usage WHERE bucket_id = NEW.bucket_id;
                    IF quota > 0 AND used > quota AND (TG_OP = 'INSERT' OR NEW.size > OLD.size) THEN
                        RAISE EXCEPTION 'BucketQuotaExceeded';
                    END IF;
                    RETURN NEW;
                END; $$ LANGUAGE plpgsql").execute(&mut *tx).await?;
            sqlx::query("DROP TRIGGER IF EXISTS alocals3_usage ON objects")
                .execute(&mut *tx)
                .await?;
            sqlx::query("CREATE TRIGGER alocals3_usage AFTER INSERT OR DELETE OR UPDATE OF size ON objects FOR EACH ROW EXECUTE FUNCTION alocals3_update_usage()")
                .execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(())
    })
}

#[derive(Deserialize)]
#[serde(untagged, deny_unknown_fields)]
pub(super) enum DeleteEntry {
    Key(String),
    Conditional {
        key: String,
        if_match: Option<String>,
    },
}
#[derive(Deserialize)]
pub(super) struct DeleteRequest {
    objects: Vec<DeleteEntry>,
    #[serde(default)]
    quiet: bool,
}

pub(super) async fn delete_many(
    State(state): State<AppState>,
    AxumPath(bucket): AxumPath<String>,
    Json(request): Json<DeleteRequest>,
) -> ApiResult<Json<serde_json::Value>> {
    if request.objects.len() > 1000 {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "At most 1000 objects per batch",
        ));
    }
    let id = state.storage.bucket_row(&bucket).await?.id;
    let mut entries = Vec::new();
    let mut errors = Vec::new();
    for entry in request.objects {
        let (key, condition) = match entry {
            DeleteEntry::Key(k) => (k, None),
            DeleteEntry::Conditional { key, if_match } => (key, if_match),
        };
        if key.contains('\0') {
            errors.push(serde_json::json!({"key": key, "status": 400, "error": "NUL is not allowed in object keys"}));
            continue;
        }
        match normalize_key(&key) {
            Ok(normalized) => entries.push((normalized, condition)),
            Err(e) => errors.push(
                serde_json::json!({"key": key, "status": e.status.as_u16(), "error": e.detail}),
            ),
        }
    }
    let mut deleted = Vec::new();
    let mut retired = HashSet::new();
    if !entries.is_empty() {
        database!(&*state.storage, pool, {
            let mut tx = pool.begin().await?;
            lock_bucket!(tx, id);
            let mut query = sqlx::QueryBuilder::new(
                "SELECT key, etag, file_path FROM objects WHERE bucket_id = ",
            );
            query.push_bind(id).push(" AND key IN (");
            let mut separated = query.separated(",");
            for (key, _) in &entries {
                separated.push_bind(key);
            }
            separated.push_unseparated(")");
            let mut existing = HashMap::new();
            for row in query.build().fetch_all(&mut *tx).await? {
                existing.insert(
                    row.get::<String, _>(0),
                    (row.get::<String, _>(1), row.get::<String, _>(2)),
                );
            }
            let mut keys = Vec::new();
            for (key, condition) in &entries {
                if condition.as_ref().is_some_and(|c| {
                    !existing
                        .get(key)
                        .is_some_and(|(etag, _)| strong_etag_matches(c, etag))
                }) {
                    errors.push(serde_json::json!({"key": key, "status": 412, "error": "Delete precondition failed"}));
                    continue;
                }
                let previous = existing.remove(key);
                if let Some((_, path)) = &previous {
                    keys.push(key);
                    retired.insert(path.clone());
                }
                deleted.push(serde_json::json!({"key": key, "existed": previous.is_some()}));
            }
            if !keys.is_empty() {
                let mut query = sqlx::QueryBuilder::new("DELETE FROM objects WHERE bucket_id = ");
                query.push_bind(id).push(" AND key IN (");
                let mut separated = query.separated(",");
                for key in keys {
                    separated.push_bind(key);
                }
                separated.push_unseparated(")");
                query.build().execute(&mut *tx).await?;
            }
            tx.commit().await?;
        });
    }
    if let Some(gc) = &state.gc {
        gc.reclaim_paths(retired.into_iter().collect());
    }
    state.monitor.deleted_objects.fetch_add(
        deleted.iter().filter(|v| v["existed"] == true).count() as u64,
        Ordering::Relaxed,
    );
    let deleted_count = deleted.iter().filter(|v| v["existed"] == true).count();
    let success_count = deleted.len();
    if request.quiet {
        deleted.clear();
    }
    Ok(Json(
        serde_json::json!({"deleted": deleted, "errors": errors, "deleted_count": deleted_count, "success_count": success_count}),
    ))
}

#[derive(Deserialize)]
pub(super) struct Quota {
    max_bytes: i64,
}
pub(super) async fn set_quota(
    State(state): State<AppState>,
    AxumPath(bucket): AxumPath<String>,
    Json(quota): Json<Quota>,
) -> ApiResult<Json<serde_json::Value>> {
    if quota.max_bytes < 0 {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "max_bytes must be nonnegative; 0 disables quota",
        ));
    }
    let id = state.storage.bucket_row(&bucket).await?.id;
    database!(&*state.storage, pool, {
        let mut tx = pool.begin().await?;
        lock_bucket!(tx, id);
        sqlx::query("INSERT INTO bucket_usage(bucket_id, max_bytes) VALUES ($1, $2) ON CONFLICT(bucket_id) DO UPDATE SET max_bytes = EXCLUDED.max_bytes")
            .bind(id).bind(quota.max_bytes).execute(&mut *tx).await?;
        tx.commit().await?;
    });
    bucket_usage(State(state), AxumPath(bucket)).await
}
pub(super) async fn bucket_usage(
    State(state): State<AppState>,
    AxumPath(bucket): AxumPath<String>,
) -> ApiResult<Json<serde_json::Value>> {
    let id = state.storage.bucket_row(&bucket).await?.id;
    database!(&*state.storage, pool, {
        let row = sqlx::query("SELECT COALESCE(u.object_count, 0), COALESCE(u.logical_bytes, 0), COALESCE(u.max_bytes, 0),
            (SELECT CAST(COALESCE(SUM(p.size),0) AS BIGINT) FROM pending_upload_parts p JOIN pending_uploads s ON p.upload_id = s.upload_id WHERE s.bucket_id = b.id)
            FROM buckets b LEFT JOIN bucket_usage u ON u.bucket_id = b.id WHERE b.id = $1").bind(id).fetch_one(pool).await?;
        Ok(Json(
            serde_json::json!({"bucket": bucket, "object_count": row.get::<i64, _>(0), "logical_bytes": row.get::<i64, _>(1),
            "max_bytes": row.get::<i64, _>(2), "pending_part_bytes": row.get::<i64, _>(3)}),
        ))
    })
}
