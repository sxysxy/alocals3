//! Explicit bucket/prefix expiration rules. Expiration is asynchronous deletion.
use super::*;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Rule {
    id: String,
    #[serde(default)]
    prefix: String,
    expire_after_secs: u64,
    #[serde(default = "enabled")]
    enabled: bool,
}
fn enabled() -> bool {
    true
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Config {
    rules: Vec<Rule>,
}

pub(super) async fn get(
    State(state): State<AppState>,
    AxumPath(bucket): AxumPath<String>,
) -> ApiResult<Json<Config>> {
    let id = state.storage.bucket_row(&bucket).await?.id;
    database!(&*state.storage, pool, {
        let row = sqlx::query("SELECT rules FROM lifecycle_config WHERE bucket_id = $1")
            .bind(id)
            .fetch_optional(pool)
            .await?;
        Ok(Json(Config {
            rules: match row {
                Some(r) => parse(&r.get::<String, _>(0))?,
                None => Vec::new(),
            },
        }))
    })
}
fn parse(json: &str) -> ApiResult<Vec<Rule>> {
    serde_json::from_str(json)
        .map_err(|e| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}
pub(super) async fn put(
    State(state): State<AppState>,
    AxumPath(bucket): AxumPath<String>,
    Json(config): Json<Config>,
) -> ApiResult<Json<Config>> {
    let mut ids = HashSet::new();
    if config.rules.len() > 100
        || config.rules.iter().any(|r| {
            r.id.is_empty()
                || !ids.insert(&r.id)
                || r.expire_after_secs == 0
                || r.expire_after_secs > 3_155_760_000
                || r.prefix.contains('\0')
                || r.prefix.starts_with('/')
        })
    {
        return Err(ApiError::new(StatusCode::BAD_REQUEST, "At most 100 rules; unique nonempty IDs, relative prefixes and retention between 1 second and 100 years required"));
    }
    let id = state.storage.bucket_row(&bucket).await?.id;
    let json = serde_json::to_string(&config.rules)
        .map_err(|e| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    database!(&*state.storage, pool, {
        let mut tx = pool.begin().await?;
        lock_bucket!(tx, id);
        sqlx::query("INSERT INTO lifecycle_config(bucket_id, rules) VALUES ($1, $2) ON CONFLICT(bucket_id) DO UPDATE SET rules = EXCLUDED.rules")
            .bind(id).bind(json).execute(&mut *tx).await?;
        tx.commit().await?;
    });
    Ok(Json(config))
}
pub(super) async fn clear(
    State(state): State<AppState>,
    AxumPath(bucket): AxumPath<String>,
) -> ApiResult<Json<Config>> {
    put(
        State(state),
        AxumPath(bucket),
        Json(Config { rules: Vec::new() }),
    )
    .await
}
#[derive(Default, Deserialize)]
pub(super) struct PreviewQuery {
    limit: Option<i64>,
    after_id: Option<i64>,
    #[serde(default)]
    include_disabled: bool,
}

// SQL selects only expired prefix ranges. Values are always bound, never LIKE
// patterns; percent signs, underscores and Unicode prefixes remain literal.
macro_rules! expiration_query {
    ($id:expr, $after:expr, $rules:expr, $collation:expr, $limit:expr) => {{
        let now = Utc::now();
        let mut query = sqlx::QueryBuilder::new(
            "SELECT id, key, file_path, size, updated_at FROM objects WHERE bucket_id = ",
        );
        query
            .push_bind($id)
            .push(" AND id > ")
            .push_bind($after)
            .push(" AND (");
        let mut first = true;
        for rule in $rules.iter().filter(|r| r.enabled) {
            if !first {
                query.push(" OR ");
            }
            first = false;
            let cutoff = (now - chrono::Duration::seconds(rule.expire_after_secs as i64))
                .to_rfc3339_opts(SecondsFormat::Micros, true);
            query
                .push("(updated_at <= ")
                .push_bind(cutoff)
                .push(" AND key COLLATE ")
                .push($collation)
                .push(" >= ")
                .push_bind(rule.prefix.clone());
            if let Some(upper) = prefix_upper_bound(&rule.prefix) {
                query
                    .push(" AND key COLLATE ")
                    .push($collation)
                    .push(" < ")
                    .push_bind(upper);
            }
            query.push(")");
        }
        if first {
            query.push("1 = 0");
        }
        query.push(") ORDER BY id LIMIT ").push_bind($limit);
        query
    }};
}

pub(super) async fn preview(
    State(state): State<AppState>,
    AxumPath(bucket): AxumPath<String>,
    Query(query): Query<PreviewQuery>,
) -> ApiResult<Json<serde_json::Value>> {
    let (value, _) = page(
        &state.storage,
        &bucket,
        query.after_id.unwrap_or(0),
        query.limit.unwrap_or(100).clamp(1, 1000),
        false,
        query.include_disabled,
    )
    .await?;
    Ok(Json(value))
}

async fn page(
    storage: &Storage,
    bucket: &str,
    after: i64,
    limit: i64,
    delete: bool,
    include_disabled: bool,
) -> ApiResult<(serde_json::Value, Vec<String>)> {
    let id = storage.bucket_row(bucket).await?.id;
    let collation = match storage {
        Storage::Sqlite { .. } => "BINARY",
        Storage::Postgres { .. } => "\"C\"",
    };
    database!(storage, pool, {
        let mut tx = pool.begin().await?;
        if delete {
            lock_bucket!(tx, id);
        }
        let rules = sqlx::query("SELECT rules FROM lifecycle_config WHERE bucket_id = $1")
            .bind(id)
            .fetch_optional(&mut *tx)
            .await?;
        let mut rules = match rules {
            Some(r) => parse(&r.get::<String, _>(0))?,
            None => Vec::new(),
        };
        if include_disabled && !delete {
            for rule in &mut rules {
                rule.enabled = true;
            }
        }
        let mut query = expiration_query!(id, after, rules, collation, limit + 1);
        let rows = query.build().fetch_all(&mut *tx).await?;
        let truncated = rows.len() > limit as usize;
        let mut objects = Vec::new();
        let mut retired = HashSet::new();
        let mut ids = Vec::new();
        for row in rows.into_iter().take(limit as usize) {
            let object_id: i64 = row.get(0);
            ids.push(object_id);
            retired.insert(row.get::<String, _>(2));
            objects.push(serde_json::json!({"id": object_id, "key": row.get::<String, _>(1), "size": row.get::<i64, _>(3), "updated_at": row.get::<String, _>(4)}));
        }
        if delete && !ids.is_empty() {
            let mut query = sqlx::QueryBuilder::new("DELETE FROM objects WHERE bucket_id = ");
            query.push_bind(id).push(" AND id IN (");
            let mut separated = query.separated(",");
            for id in &ids {
                separated.push_bind(id);
            }
            separated.push_unseparated(")");
            query.build().execute(&mut *tx).await?;
        }
        tx.commit().await?;
        let next = if truncated { ids.last().copied() } else { None };
        Ok((
            serde_json::json!({"bucket": bucket, "objects": objects, "is_truncated": truncated, "next_after_id": next}),
            retired.into_iter().collect(),
        ))
    })
}

pub(super) fn start(state: AppState, interval: Duration) {
    tokio::spawn(async move {
        let mut timer = tokio::time::interval(interval);
        timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            timer.tick().await;
            if let Err(e) = sweep(&state).await {
                state
                    .monitor
                    .lifecycle_errors
                    .fetch_add(1, Ordering::Relaxed);
                tracing::warn!(error = %e.detail, "object expiration failed");
            }
        }
    });
}
async fn sweep(state: &AppState) -> ApiResult<()> {
    let mut after_bucket = 0_i64;
    loop {
        let buckets: Vec<(i64, String)> = database!(&*state.storage, pool, {
            sqlx::query("SELECT b.id, b.name FROM buckets b JOIN lifecycle_config c ON c.bucket_id = b.id WHERE b.id > $1 ORDER BY b.id LIMIT 128")
                .bind(after_bucket).fetch_all(pool).await?.into_iter().map(|r| (r.get(0), r.get(1))).collect()
        });
        if buckets.is_empty() {
            break;
        }
        for (id, name) in buckets {
            after_bucket = id;
            // Bound maintenance work per bucket per tick; the next tick catches up.
            let mut after = 0;
            for _ in 0..20 {
                let (value, paths) =
                    match page(&state.storage, &name, after, 500, true, false).await {
                        Ok(v) => v,
                        Err(e) if e.status == StatusCode::NOT_FOUND => break,
                        Err(e) => return Err(e),
                    };
                let count = value["objects"].as_array().map_or(0, Vec::len) as u64;
                state
                    .monitor
                    .expired_objects
                    .fetch_add(count, Ordering::Relaxed);
                if let Some(gc) = &state.gc {
                    if !paths.is_empty() {
                        gc.reclaim_paths(paths);
                    }
                }
                match value["next_after_id"].as_i64() {
                    Some(id) => after = id,
                    None => break,
                }
            }
        }
    }
    Ok(())
}
