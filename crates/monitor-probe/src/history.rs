use std::{collections::HashMap, sync::Arc};

use chrono::{DateTime, Utc};
use monitor_core::{finalization::MAX_OBSERVATION_GAP_SECONDS, model::Node};
use sqlx::{sqlite::SqlitePoolOptions, Row, SqlitePool};
use thiserror::Error;
use tokio::sync::Mutex;

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
    StalledBlocks(u64),
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
    verifier_context: Option<String>,
    verifier_keys: Option<HashMap<String, ScopedKey>>,
    active_scopes: Arc<Mutex<ActiveScopes>>,
}

#[derive(Debug, Clone)]
struct ScopedKey {
    node_id: String,
    identity: String,
}

#[derive(Debug, Default)]
struct ActiveScopes {
    context: Option<String>,
    identities: HashMap<String, String>,
}

impl ActiveScopes {
    fn activate(&mut self, context: Option<&str>, key: &ScopedKey) {
        if self.context.as_deref() != context {
            self.identities.clear();
            self.context = context.map(str::to_owned);
        }
        self.identities
            .insert(key.node_id.clone(), key.identity.clone());
    }
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
        Ok(Self {
            pool,
            verifier_context: None,
            verifier_keys: None,
            active_scopes: Arc::new(Mutex::new(ActiveScopes::default())),
        })
    }

    /// Isolates Central history by authority and endpoint without changing the
    /// persistent Probe history contract. Clones share only samples and scopes.
    #[must_use]
    pub fn for_verifier_context(&self, context: &str, target: &Node, references: &[Node]) -> Self {
        let key = |role: &str, node: &Node| ScopedKey {
            node_id: format!("verifier:{role}:{:?}", (node.network, &node.id)),
            identity: format!(
                "{:?}",
                (context, &node.host, &node.rest_scheme, node.rest_port)
            ),
        };
        let mut keys = HashMap::from([(target.id.clone(), key("target", target))]);
        for reference in references {
            if let Ok(origin) = crate::engine::reference_history_key(reference) {
                keys.insert(origin, key("reference", reference));
            }
        }
        Self {
            pool: self.pool.clone(),
            verifier_context: Some(context.to_owned()),
            verifier_keys: Some(keys),
            active_scopes: Arc::clone(&self.active_scopes),
        }
    }

    /// Endpoint-scope ordinary Probe history while retaining Central authority scopes.
    #[must_use]
    pub fn for_probe_context(&self, target: &Node, references: &[Node]) -> Self {
        if self.verifier_context.is_some() {
            self.clone()
        } else {
            self.for_verifier_context("probe", target, references)
        }
    }

    pub async fn evaluate(
        &self,
        node_id: &str,
        sample: FinalizationSample,
        observed_at: DateTime<Utc>,
    ) -> Result<FinalizationAssessment, HistoryError> {
        let scoped_key = self
            .verifier_keys
            .as_ref()
            .map(|keys| keys.get(node_id).ok_or(HistoryError::InvalidValue))
            .transpose()?;
        let node_id = scoped_key.map_or(node_id, |key| key.node_id.as_str());
        // Keep scope activation and its SQLite baseline replacement atomic even
        // if a caller holds an engine from an older authority context.
        let mut active_scopes = if scoped_key.is_some() {
            Some(self.active_scopes.lock().await)
        } else {
            None
        };
        let scope_changed = scoped_key.is_some_and(|key| {
            active_scopes.as_ref().is_some_and(|scopes| {
                scopes.context.as_deref() != self.verifier_context.as_deref()
                    || scopes.identities.get(node_id) != Some(&key.identity)
            })
        });
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
            if let (Some(scopes), Some(key)) = (active_scopes.as_mut(), scoped_key) {
                scopes.activate(self.verifier_context.as_deref(), key);
            }
            return Ok(FinalizationAssessment::InsufficientHistory);
        };

        let previous_finalized: i64 = row.try_get("finalized_height")?;
        let baseline_chain: i64 = row.try_get("baseline_chain_height")?;
        let last_observed_at: i64 = row.try_get("last_observed_at")?;
        if observed_at <= last_observed_at {
            transaction.rollback().await?;
            return Ok(FinalizationAssessment::InsufficientHistory);
        }

        // A missing observation interval cannot establish continuous progress.
        let freshness_gap = observed_at - last_observed_at > MAX_OBSERVATION_GAP_SECONDS;
        if scope_changed || freshness_gap {
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
            if let (Some(scopes), Some(key)) = (active_scopes.as_mut(), scoped_key) {
                scopes.activate(self.verifier_context.as_deref(), key);
            }
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
        if chain_progress <= 0 {
            return Ok(FinalizationAssessment::InsufficientHistory);
        }
        let blocks = u64::try_from(chain_progress).map_err(|_| HistoryError::InvalidValue)?;
        Ok(FinalizationAssessment::StalledBlocks(blocks))
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
