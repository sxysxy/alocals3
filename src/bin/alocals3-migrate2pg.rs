use anyhow::{bail, Context};
use clap::Parser;
use sqlx::postgres::PgPoolOptions;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{PgPool, Row, SqlitePool};
use std::path::PathBuf;
use std::str::FromStr;

#[derive(Parser, Debug)]
#[command(
    name = "alocals3-migrate2pg",
    version,
    about = "Migrate an alocals3 metadata database from SQLite to PostgreSQL"
)]
struct Args {
    /// SQLite URL or database file path.
    #[arg(long, visible_alias = "sqlite-url", env = "ALOCALS3_SQLITE_URL")]
    source: String,
    /// Destination PostgreSQL URL.
    #[arg(long, visible_alias = "postgres-url", env = "ALOCALS3_POSTGRES_URL")]
    target: String,
    /// Number of object records read from SQLite per batch.
    #[arg(long, default_value_t = 1000)]
    batch_size: i64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    if args.batch_size < 1 {
        bail!("--batch-size must be at least 1");
    }

    let source_path = sqlite_path(&args.source);
    if !source_path.is_file() {
        bail!("SQLite source does not exist: {}", source_path.display());
    }
    let source_options =
        SqliteConnectOptions::from_str(&format!("sqlite://{}", source_path.display()))?
            .read_only(true);
    let source = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(source_options)
        .await
        .context("failed to open SQLite source")?;
    validate_source(&source).await?;

    let target = PgPoolOptions::new()
        .max_connections(5)
        .connect(&args.target)
        .await
        .context("failed to connect to PostgreSQL target")?;
    init_pg(&target).await?;

    let has_metadata = sqlite_has_column(&source, "objects", "metadata").await?;
    let bucket_rows = sqlx::query("SELECT id, name, created_at FROM buckets ORDER BY id")
        .fetch_all(&source)
        .await?;
    let mut transaction = target.begin().await?;
    let mut bucket_count = 0_u64;
    for row in bucket_rows {
        sqlx::query(
            "INSERT INTO buckets (name, created_at) VALUES ($1, $2)
             ON CONFLICT (name) DO UPDATE SET created_at = EXCLUDED.created_at",
        )
        .bind(row.get::<String, _>(1))
        .bind(row.get::<String, _>(2))
        .execute(&mut *transaction)
        .await?;
        bucket_count += 1;
    }

    let metadata_expression = if has_metadata { "o.metadata" } else { "'{}'" };
    let object_sql = format!(
        "SELECT o.id, b.name, o.key, o.file_path, o.size, o.content_type, o.etag, \
         o.updated_at, o.created_at, {metadata_expression} AS metadata \
         FROM objects o JOIN buckets b ON b.id = o.bucket_id \
         WHERE o.id > ?1 ORDER BY o.id LIMIT ?2"
    );
    let mut last_id = 0_i64;
    let mut object_count = 0_u64;
    loop {
        let rows = sqlx::query(&object_sql)
            .bind(last_id)
            .bind(args.batch_size)
            .fetch_all(&source)
            .await?;
        if rows.is_empty() {
            break;
        }
        for row in rows {
            last_id = row.get(0);
            sqlx::query(
                "INSERT INTO objects
                 (bucket_id, key, file_path, size, content_type, etag, updated_at, created_at, metadata)
                 SELECT id, $2, $3, $4, $5, $6, $7, $8, $9 FROM buckets WHERE name = $1
                 ON CONFLICT (bucket_id, key) DO UPDATE SET
                   file_path = EXCLUDED.file_path,
                   size = EXCLUDED.size,
                   content_type = EXCLUDED.content_type,
                   etag = EXCLUDED.etag,
                   updated_at = EXCLUDED.updated_at,
                   created_at = EXCLUDED.created_at,
                   metadata = EXCLUDED.metadata",
            )
            .bind(row.get::<String, _>(1))
            .bind(row.get::<String, _>(2))
            .bind(row.get::<String, _>(3))
            .bind(row.get::<i64, _>(4))
            .bind(row.get::<String, _>(5))
            .bind(row.get::<String, _>(6))
            .bind(row.get::<String, _>(7))
            .bind(row.get::<String, _>(8))
            .bind(row.get::<String, _>(9))
            .execute(&mut *transaction)
            .await?;
            object_count += 1;
        }
    }
    transaction.commit().await?;
    println!("migration complete: {bucket_count} buckets, {object_count} objects");
    Ok(())
}

fn sqlite_path(value: &str) -> PathBuf {
    if let Some(path) = value.strip_prefix("sqlite:///") {
        PathBuf::from(path)
    } else if let Some(path) = value.strip_prefix("sqlite://") {
        PathBuf::from(path)
    } else {
        PathBuf::from(value)
    }
}

async fn validate_source(pool: &SqlitePool) -> anyhow::Result<()> {
    for table in ["buckets", "objects"] {
        let exists: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?1",
        )
        .bind(table)
        .fetch_one(pool)
        .await?;
        if exists == 0 {
            bail!("SQLite source is not an alocals3 database: missing {table} table");
        }
    }
    Ok(())
}

async fn sqlite_has_column(pool: &SqlitePool, table: &str, column: &str) -> anyhow::Result<bool> {
    let sql = format!("PRAGMA table_info({table})");
    Ok(sqlx::query(&sql)
        .fetch_all(pool)
        .await?
        .iter()
        .any(|row| row.get::<String, _>(1) == column))
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
    sqlx::query("CREATE INDEX IF NOT EXISTS ix_buckets_name ON buckets (name)")
        .execute(pool)
        .await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_objects_bucket_key ON objects (bucket_id, key)")
        .execute(pool)
        .await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_objects_file_path ON objects (file_path)")
        .execute(pool)
        .await?;
    Ok(())
}
