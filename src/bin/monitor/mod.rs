//! Bounded-label Prometheus metrics and cached storage gauges.
use super::*;
use std::sync::atomic::AtomicU64;

const BOUNDS: [f64; 10] = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0, 10.0];
#[derive(Default)]
struct RequestMetric {
    count: u64,
    sum: f64,
    buckets: [u64; 10],
}
#[derive(Default, Clone, Serialize)]
struct Snapshot {
    object_count: i64,
    logical_bytes: i64,
    pending_uploads: i64,
    pending_part_bytes: i64,
    filesystem_available_bytes: u64,
    filesystem_total_bytes: u64,
    min_free_bytes: u64,
    sampled_at_unix_seconds: u64,
}
#[derive(Default)]
pub(super) struct Monitor {
    requests: std::sync::Mutex<HashMap<(String, String, u16), RequestMetric>>,
    snapshot: std::sync::Mutex<Snapshot>,
    request_body_bytes: AtomicU64,
    response_body_bytes: AtomicU64,
    inflight_headers: AtomicU64,
    sample_errors: AtomicU64,
    pub(super) expired_objects: AtomicU64,
    pub(super) lifecycle_errors: AtomicU64,
    pub(super) deleted_objects: AtomicU64,
}
struct Inflight(Arc<Monitor>);
impl Drop for Inflight {
    fn drop(&mut self) {
        self.0.inflight_headers.fetch_sub(1, Ordering::Relaxed);
    }
}

pub(super) async fn middleware(
    State(state): State<AppState>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let started = Instant::now();
    let route = request
        .extensions()
        .get::<axum::extract::MatchedPath>()
        .map(|p| p.as_str())
        .unwrap_or("unmatched")
        .to_owned();
    let method = match request.method().as_str() {
        "GET" | "HEAD" | "PUT" | "POST" | "DELETE" | "OPTIONS" | "PATCH" => {
            request.method().as_str()
        }
        _ => "OTHER",
    }
    .to_owned();
    let request_id = HeaderValue::from_str(&Uuid::new_v4().simple().to_string()).unwrap();
    state
        .monitor
        .inflight_headers
        .fetch_add(1, Ordering::Relaxed);
    let _inflight = Inflight(state.monitor.clone());
    let (mut parts, body) = request.into_parts();
    parts.headers.insert("x-request-id", request_id.clone());
    let monitor = state.monitor.clone();
    let body = Body::from_stream(body.into_data_stream().map(move |result| {
        if let Ok(bytes) = &result {
            monitor
                .request_body_bytes
                .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        }
        result
    }));
    let response = next.run(Request::from_parts(parts, body)).await;
    let seconds = started.elapsed().as_secs_f64();
    {
        let mut metrics = state
            .monitor
            .requests
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let metric = metrics
            .entry((method, route, response.status().as_u16()))
            .or_default();
        metric.count += 1;
        metric.sum += seconds;
        for (i, bound) in BOUNDS.iter().enumerate() {
            if seconds <= *bound {
                metric.buckets[i] += 1;
            }
        }
    }
    let (mut parts, body) = response.into_parts();
    parts.headers.insert("x-request-id", request_id);
    let monitor = state.monitor.clone();
    let body = Body::from_stream(body.into_data_stream().map(move |result| {
        if let Ok(bytes) = &result {
            monitor
                .response_body_bytes
                .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        }
        result
    }));
    Response::from_parts(parts, body)
}

async fn snapshot(state: &AppState) -> ApiResult<Snapshot> {
    let mut snapshot = database!(&*state.storage, pool, {
        let row = sqlx::query("SELECT CAST(COALESCE(SUM(object_count),0) AS BIGINT), CAST(COALESCE(SUM(logical_bytes),0) AS BIGINT),
            (SELECT COUNT(*) FROM pending_uploads), (SELECT CAST(COALESCE(SUM(size),0) AS BIGINT) FROM pending_upload_parts) FROM bucket_usage")
            .fetch_one(pool).await?;
        Snapshot {
            object_count: row.get(0),
            logical_bytes: row.get(1),
            pending_uploads: row.get(2),
            pending_part_bytes: row.get(3),
            ..Snapshot::default()
        }
    });
    snapshot.filesystem_available_bytes = state.capacity.available()?;
    snapshot.filesystem_total_bytes = state.capacity.total()?;
    snapshot.min_free_bytes = state.capacity.min_free_bytes;
    snapshot.sampled_at_unix_seconds = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    Ok(snapshot)
}
pub(super) async fn stats(State(state): State<AppState>) -> ApiResult<Json<serde_json::Value>> {
    let value = snapshot(&state).await?;
    *state
        .monitor
        .snapshot
        .lock()
        .unwrap_or_else(|e| e.into_inner()) = value.clone();
    Ok(Json(serde_json::to_value(value).map_err(|e| {
        ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
    })?))
}
pub(super) fn start(state: AppState, interval: Duration) {
    tokio::spawn(async move {
        let mut timer = tokio::time::interval(interval);
        timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            timer.tick().await;
            match tokio::time::timeout(Duration::from_secs(5), snapshot(&state)).await {
                Ok(Ok(value)) => {
                    *state
                        .monitor
                        .snapshot
                        .lock()
                        .unwrap_or_else(|e| e.into_inner()) = value
                }
                _ => {
                    state.monitor.sample_errors.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    });
}
pub(super) async fn ready(State(state): State<AppState>) -> Response {
    let result: Result<ApiResult<()>, _> = tokio::time::timeout(Duration::from_secs(2), async {
        database!(&*state.storage, pool, {
            sqlx::query("SELECT 1").fetch_one(pool).await?;
        });
        state.capacity.check(0)?;
        Ok(())
    })
    .await;
    if matches!(result, Ok(Ok(()))) {
        (StatusCode::OK, Json(serde_json::json!({"status":"ready"}))).into_response()
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"status":"not_ready"})),
        )
            .into_response()
    }
}
fn label(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}
pub(super) async fn metrics(State(state): State<AppState>) -> Response {
    use std::fmt::Write;
    let monitor = &state.monitor;
    let mut text = String::from("# TYPE alocals3_http_requests_total counter\n# TYPE alocals3_http_response_headers_seconds histogram\n");
    {
        let requests = monitor.requests.lock().unwrap_or_else(|e| e.into_inner());
        for ((method, route, status), metric) in requests.iter() {
            let labels = format!(
                "method=\"{}\",route=\"{}\",status=\"{}\"",
                label(method),
                label(route),
                status
            );
            let _ = writeln!(
                text,
                "alocals3_http_requests_total{{{labels}}} {}",
                metric.count
            );
            for (i, bound) in BOUNDS.iter().enumerate() {
                let _ = writeln!(
                    text,
                    "alocals3_http_response_headers_seconds_bucket{{{labels},le=\"{bound}\"}} {}",
                    metric.buckets[i]
                );
            }
            let _ = writeln!(text, "alocals3_http_response_headers_seconds_bucket{{{labels},le=\"+Inf\"}} {}\nalocals3_http_response_headers_seconds_count{{{labels}}} {}\nalocals3_http_response_headers_seconds_sum{{{labels}}} {}", metric.count, metric.count, metric.sum);
        }
    }
    for (name, counter) in [
        ("http_request_body_bytes_total", &monitor.request_body_bytes),
        (
            "http_response_body_bytes_total",
            &monitor.response_body_bytes,
        ),
        ("batch_deleted_objects_total", &monitor.deleted_objects),
        ("expired_objects_total", &monitor.expired_objects),
        ("lifecycle_errors_total", &monitor.lifecycle_errors),
        ("storage_sample_errors_total", &monitor.sample_errors),
    ] {
        let _ = writeln!(
            text,
            "# TYPE alocals3_{name} counter\nalocals3_{name} {}",
            counter.load(Ordering::Relaxed)
        );
    }
    let _ = writeln!(
        text,
        "# TYPE alocals3_http_inflight_headers gauge\nalocals3_http_inflight_headers {}",
        monitor.inflight_headers.load(Ordering::Relaxed)
    );
    let snapshot = monitor
        .snapshot
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone();
    for (name, value) in [
        ("storage_objects", snapshot.object_count.max(0) as u64),
        (
            "storage_logical_bytes",
            snapshot.logical_bytes.max(0) as u64,
        ),
        ("pending_uploads", snapshot.pending_uploads.max(0) as u64),
        (
            "pending_part_bytes",
            snapshot.pending_part_bytes.max(0) as u64,
        ),
        (
            "filesystem_available_bytes",
            snapshot.filesystem_available_bytes,
        ),
        ("filesystem_total_bytes", snapshot.filesystem_total_bytes),
        ("disk_reserve_bytes", snapshot.min_free_bytes),
        (
            "storage_sample_timestamp_seconds",
            snapshot.sampled_at_unix_seconds,
        ),
    ] {
        let _ = writeln!(
            text,
            "# TYPE alocals3_{name} gauge\nalocals3_{name} {value}"
        );
    }
    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        text,
    )
        .into_response()
}
