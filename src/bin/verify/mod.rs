//! Read-only consistency inspection. Never invokes GC or removes metadata.
use super::*;

// Verification must not create databases/directories or run schema migrations.
pub(super) async fn connect(database_url: &str, root: PathBuf) -> anyhow::Result<Storage> {
    let objects_root = root.join("objects");
    let gc_state = Arc::new(GcState {
        active_writes: Mutex::new(HashMap::new()),
    });
    if database_url.starts_with("sqlite") {
        let path = sqlite_path(database_url);
        let options = SqliteConnectOptions::from_str(&format!("sqlite://{}", path.display()))?
            .read_only(true)
            .create_if_missing(false)
            .busy_timeout(Duration::from_secs(10));
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await?;
        Ok(Storage::Sqlite {
            pool,
            objects_root,
            gc_state,
        })
    } else if database_url.starts_with("postgres") {
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .after_connect(|conn, _| {
                Box::pin(async move {
                    sqlx::query("SET default_transaction_read_only = on")
                        .execute(conn)
                        .await?;
                    Ok(())
                })
            })
            .connect(database_url)
            .await?;
        Ok(Storage::Postgres {
            pool,
            objects_root,
            gc_state,
        })
    } else {
        anyhow::bail!("Unsupported database URL")
    }
}

#[derive(Serialize)]
pub(super) struct Report {
    checked_objects: u64,
    full: bool,
    pub(super) issues: Vec<serde_json::Value>,
}

pub(super) async fn run(storage: &Storage, full: bool) -> ApiResult<Report> {
    let mut report = Report {
        checked_objects: 0,
        full,
        issues: Vec::new(),
    };
    let mut after = 0_i64;
    loop {
        let rows: Vec<(i64, String, String, String, i64, String)> = database!(storage, pool, {
            sqlx::query(
                "SELECT o.id, b.name, o.key, o.file_path, o.size, o.etag FROM objects o
                JOIN buckets b ON b.id = o.bucket_id WHERE o.id > $1 ORDER BY o.id LIMIT 256",
            )
            .bind(after)
            .fetch_all(pool)
            .await?
            .into_iter()
            .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3), r.get(4), r.get(5)))
            .collect()
        });
        if rows.is_empty() {
            break;
        }
        for (id, bucket, key, relative, size, etag) in rows {
            after = id;
            report.checked_objects += 1;
            let problem = if !is_safe_storage_relative_path(&relative) {
                Some("unsafe_path".to_string())
            } else {
                inspect(&storage.objects_root().join(&relative), size, &etag, full).await
            };
            if let Some(problem) = problem {
                report.issues.push(serde_json::json!({"bucket": bucket, "key": key, "file_path": relative, "problem": problem}));
            }
        }
    }
    Ok(report)
}
async fn inspect(path: &Path, size: i64, etag: &str, full: bool) -> Option<String> {
    let mut file = match tokio::fs::File::open(path).await {
        Ok(f) => f,
        Err(e) => {
            return Some(if e.kind() == std::io::ErrorKind::NotFound {
                "missing_file".into()
            } else {
                format!("read_error: {e}")
            })
        }
    };
    let metadata = match file.metadata().await {
        Ok(m) => m,
        Err(e) => return Some(format!("read_error: {e}")),
    };
    if !metadata.is_file() {
        return Some("not_a_file".into());
    }
    if size < 0 || metadata.len() != size as u64 {
        return Some("size_mismatch".into());
    }
    if !full {
        return None;
    }
    let mut sha = Sha256::new();
    let mut md5 = md5::Context::new();
    let mut buffer = vec![0; DOWNLOAD_CHUNK_SIZE];
    loop {
        let count = match file.read(&mut buffer).await {
            Ok(n) => n,
            Err(e) => return Some(format!("read_error: {e}")),
        };
        if count == 0 {
            break;
        }
        sha.update(&buffer[..count]);
        md5.consume(&buffer[..count]);
    }
    let digest = format!("{:x}", sha.finalize());
    if path.file_name().and_then(|n| n.to_str()) != Some(digest.as_str()) {
        return Some("sha256_mismatch".into());
    }
    if format!("{:x}", md5.compute()) != etag {
        return Some("etag_mismatch".into());
    }
    None
}
