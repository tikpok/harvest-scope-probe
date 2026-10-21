use chrono::{DateTime, Utc};
use sqlx::{sqlite::SqlitePoolOptions, Row, SqlitePool};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FinalizationSample {
    pub chain_height: u64,
    pub finalized_height: u64,
    pub reference_finalized_height: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FinalizationAssessment {
    InsufficientHistory,
    Progressing,
    StalledMinutes(u64),
}

#[derive(Debug, Error)]
pub enum HistoryError {
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error("finalization history value is outside the supported range")]
    InvalidValue,
}

#[derive(Debug, Clone)]
pub struct FinalizationHistory {
    pool: SqlitePool,
}

impl FinalizationHistory {
    pub async fn in_memory() -> Result<Self, HistoryError> {
        Self::connect("sqlite::memory:").await
    }

    pub async fn connect(url: &str) -> Result<Self, HistoryError> {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(url)
            .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS finalization_history (
                node_id TEXT PRIMARY KEY,
                finalized_height INTEGER NOT NULL CHECK (finalized_height >= 0),
                stalled_since INTEGER NOT NULL CHECK (stalled_since >= 0),
                baseline_chain_height INTEGER NOT NULL CHECK (baseline_chain_height >= 0),
                baseline_reference_height INTEGER NOT NULL CHECK (baseline_reference_height >= 0),
                last_observed_at INTEGER NOT NULL CHECK (last_observed_at >= stalled_since)
            )",
        )
        .execute(&pool)
        .await?;
        Ok(Self { pool })
    }

    pub async fn evaluate(
        &self,
        node_id: &str,
        sample: FinalizationSample,
        observed_at: DateTime<Utc>,
    ) -> Result<FinalizationAssessment, HistoryError> {
        let observed_at = observed_at.timestamp();
        if observed_at < 0 {
            return Err(HistoryError::InvalidValue);
        }
        let finalized_height = to_i64(sample.finalized_height)?;
        let chain_height = to_i64(sample.chain_height)?;
        let reference_height = to_i64(sample.reference_finalized_height)?;
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query(
            "SELECT finalized_height, stalled_since, baseline_chain_height,
                    baseline_reference_height, last_observed_at
             FROM finalization_history WHERE node_id = ?",
        )
        .bind(node_id)
        .fetch_optional(&mut *transaction)
        .await?;

        let Some(row) = row else {
            replace_baseline(
                &mut transaction,
                node_id,
                finalized_height,
                observed_at,
                chain_height,
                reference_height,
            )
            .await?;
            transaction.commit().await?;
            return Ok(FinalizationAssessment::InsufficientHistory);
        };

        let previous_finalized: i64 = row.try_get("finalized_height")?;
        let stalled_since: i64 = row.try_get("stalled_since")?;
        let baseline_chain: i64 = row.try_get("baseline_chain_height")?;
        let baseline_reference: i64 = row.try_get("baseline_reference_height")?;
        let last_observed_at: i64 = row.try_get("last_observed_at")?;
        if observed_at <= last_observed_at {
            transaction.rollback().await?;
            return Ok(FinalizationAssessment::InsufficientHistory);
        }

        if finalized_height != previous_finalized {
            replace_baseline(
                &mut transaction,
                node_id,
                finalized_height,
                observed_at,
                chain_height,
                reference_height,
            )
            .await?;
            transaction.commit().await?;
            return Ok(if finalized_height > previous_finalized {
                FinalizationAssessment::Progressing
            } else {
                FinalizationAssessment::InsufficientHistory
            });
        }

        sqlx::query("UPDATE finalization_history SET last_observed_at = ? WHERE node_id = ?")
            .bind(observed_at)
            .bind(node_id)
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;

        let chain_progress = chain_height.saturating_sub(baseline_chain);
        if chain_progress < 20 || reference_height <= baseline_reference {
            return Ok(FinalizationAssessment::InsufficientHistory);
        }
        let minutes = u64::try_from(observed_at.saturating_sub(stalled_since) / 60)
            .map_err(|_| HistoryError::InvalidValue)?;
        Ok(FinalizationAssessment::StalledMinutes(minutes))
    }
}

async fn replace_baseline(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    node_id: &str,
    finalized_height: i64,
    observed_at: i64,
    chain_height: i64,
    reference_height: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO finalization_history(
            node_id, finalized_height, stalled_since, baseline_chain_height,
            baseline_reference_height, last_observed_at
         ) VALUES (?, ?, ?, ?, ?, ?)
         ON CONFLICT(node_id) DO UPDATE SET
            finalized_height = excluded.finalized_height,
            stalled_since = excluded.stalled_since,
            baseline_chain_height = excluded.baseline_chain_height,
            baseline_reference_height = excluded.baseline_reference_height,
            last_observed_at = excluded.last_observed_at",
    )
    .bind(node_id)
    .bind(finalized_height)
    .bind(observed_at)
    .bind(chain_height)
    .bind(reference_height)
    .bind(observed_at)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

fn to_i64(value: u64) -> Result<i64, HistoryError> {
    i64::try_from(value).map_err(|_| HistoryError::InvalidValue)
}
