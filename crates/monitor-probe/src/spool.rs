use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use serde::Serialize;
use sqlx::{sqlite::SqlitePoolOptions, SqlitePool};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SpoolError {
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error(
        "spool capacity exceeded: {current_bytes} existing bytes + {payload_bytes} payload bytes > {max_bytes} bytes"
    )]
    CapacityExceeded {
        current_bytes: u64,
        payload_bytes: u64,
        max_bytes: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SpoolPressure {
    Normal,
    Warning,
    Full,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpoolStats {
    pub queue_depth: u64,
    pub spool_bytes: u64,
    pub spool_pressure: SpoolPressure,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpoolItem {
    pub sequence: i64,
    pub job_id: String,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct Spool {
    pool: SqlitePool,
    max_bytes: u64,
    write_failed: Arc<AtomicBool>,
}

impl Spool {
    pub async fn in_memory(max_bytes: usize) -> Result<Self, SpoolError> {
        Self::connect("sqlite::memory:", max_bytes).await
    }

    pub async fn connect(url: &str, max_bytes: usize) -> Result<Self, SpoolError> {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(url)
            .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS spool (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id TEXT NOT NULL UNIQUE,
                payload BLOB NOT NULL,
                created_at INTEGER NOT NULL DEFAULT (unixepoch())
            )",
        )
        .execute(&pool)
        .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS spool_state (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                queue_depth INTEGER NOT NULL CHECK (queue_depth >= 0),
                payload_bytes INTEGER NOT NULL CHECK (payload_bytes >= 0)
            )",
        )
        .execute(&pool)
        .await?;

        let mut transaction = pool.begin().await?;
        sqlx::query(
            "INSERT INTO spool_state(singleton, queue_depth, payload_bytes)
             VALUES (1, 0, 0)
             ON CONFLICT(singleton) DO NOTHING",
        )
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "UPDATE spool_state
             SET queue_depth = (SELECT COUNT(*) FROM spool),
                 payload_bytes = (SELECT COALESCE(SUM(length(payload)), 0) FROM spool)
             WHERE singleton = 1",
        )
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;

        Ok(Self {
            pool,
            max_bytes: max_bytes as u64,
            write_failed: Arc::new(AtomicBool::new(false)),
        })
    }

    pub async fn enqueue(&self, job_id: &str, payload: &[u8]) -> Result<(), SpoolError> {
        let payload_bytes = payload.len() as u64;
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|error| self.failed(error))?;
        let current_bytes: i64 =
            sqlx::query_scalar("SELECT payload_bytes FROM spool_state WHERE singleton = 1")
                .fetch_one(&mut *transaction)
                .await
                .map_err(|error| self.failed(error))?;
        let current_bytes = nonnegative_u64(current_bytes);

        let existing: Option<i64> = sqlx::query_scalar("SELECT 1 FROM spool WHERE job_id = ?")
            .bind(job_id)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|error| self.failed(error))?;
        if existing.is_some() {
            transaction
                .commit()
                .await
                .map_err(|error| self.failed(error))?;
            return Ok(());
        }

        if current_bytes.saturating_add(payload_bytes) > self.max_bytes {
            self.write_failed.store(true, Ordering::Release);
            return Err(SpoolError::CapacityExceeded {
                current_bytes,
                payload_bytes,
                max_bytes: self.max_bytes,
            });
        }

        sqlx::query("INSERT INTO spool(job_id, payload) VALUES (?, ?)")
            .bind(job_id)
            .bind(payload)
            .execute(&mut *transaction)
            .await
            .map_err(|error| self.failed(error))?;
        sqlx::query(
            "UPDATE spool_state
             SET queue_depth = queue_depth + 1,
                 payload_bytes = payload_bytes + ?
             WHERE singleton = 1",
        )
        .bind(i64::try_from(payload_bytes).unwrap_or(i64::MAX))
        .execute(&mut *transaction)
        .await
        .map_err(|error| self.failed(error))?;
        transaction
            .commit()
            .await
            .map_err(|error| self.failed(error))?;
        self.write_failed.store(false, Ordering::Release);
        Ok(())
    }

    pub async fn pending(&self, limit: i64) -> Result<Vec<SpoolItem>, SpoolError> {
        self.pending_after(0, limit).await
    }

    pub async fn pending_after(
        &self,
        after_sequence: i64,
        limit: i64,
    ) -> Result<Vec<SpoolItem>, SpoolError> {
        let rows = sqlx::query_as::<_, (i64, String, Vec<u8>)>(
            "SELECT sequence, job_id, payload
             FROM spool
             WHERE sequence > ?
             ORDER BY sequence
             LIMIT ?",
        )
        .bind(after_sequence)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|(sequence, job_id, payload)| SpoolItem {
                sequence,
                job_id,
                payload,
            })
            .collect())
    }

    pub async fn stats(&self) -> Result<SpoolStats, SpoolError> {
        let (queue_depth, spool_bytes): (i64, i64) = sqlx::query_as(
            "SELECT queue_depth, payload_bytes FROM spool_state WHERE singleton = 1",
        )
        .fetch_one(&self.pool)
        .await?;
        let queue_depth = nonnegative_u64(queue_depth);
        let spool_bytes = nonnegative_u64(spool_bytes);
        let spool_pressure = if self.write_failed.load(Ordering::Acquire) {
            SpoolPressure::Full
        } else {
            pressure(spool_bytes, self.max_bytes)
        };
        Ok(SpoolStats {
            queue_depth,
            spool_bytes,
            spool_pressure,
        })
    }

    pub async fn ack(&self, job_ids: &[&str]) -> Result<(), SpoolError> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|error| self.failed(error))?;
        let mut removed_count = 0_i64;
        let mut removed_bytes = 0_i64;
        for job_id in job_ids {
            let payload_bytes: Option<i64> =
                sqlx::query_scalar("SELECT length(payload) FROM spool WHERE job_id = ?")
                    .bind(job_id)
                    .fetch_optional(&mut *transaction)
                    .await
                    .map_err(|error| self.failed(error))?;
            if let Some(payload_bytes) = payload_bytes {
                sqlx::query("DELETE FROM spool WHERE job_id = ?")
                    .bind(job_id)
                    .execute(&mut *transaction)
                    .await
                    .map_err(|error| self.failed(error))?;
                removed_count += 1;
                removed_bytes = removed_bytes.saturating_add(payload_bytes);
            }
        }
        if removed_count > 0 {
            sqlx::query(
                "UPDATE spool_state
                 SET queue_depth = queue_depth - ?,
                     payload_bytes = payload_bytes - ?
                 WHERE singleton = 1",
            )
            .bind(removed_count)
            .bind(removed_bytes)
            .execute(&mut *transaction)
            .await
            .map_err(|error| self.failed(error))?;
        }
        transaction
            .commit()
            .await
            .map_err(|error| self.failed(error))?;
        Ok(())
    }

    fn failed(&self, error: sqlx::Error) -> SpoolError {
        self.write_failed.store(true, Ordering::Release);
        SpoolError::Database(error)
    }
}

fn pressure(spool_bytes: u64, max_bytes: u64) -> SpoolPressure {
    if spool_bytes >= max_bytes {
        SpoolPressure::Full
    } else if u128::from(spool_bytes) * 5 >= u128::from(max_bytes) * 4 {
        SpoolPressure::Warning
    } else {
        SpoolPressure::Normal
    }
}

fn nonnegative_u64(value: i64) -> u64 {
    u64::try_from(value).unwrap_or(0)
}
