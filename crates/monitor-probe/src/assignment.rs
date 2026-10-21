use std::{
    collections::{BTreeMap, HashSet},
    time::Duration,
};

use chrono::{DateTime, SecondsFormat, Utc};
use futures::TryStreamExt;
use monitor_core::{
    model::{Network, Node, NodeKind, Site},
    scheduler::{CheckKind, ScheduledJob},
};
use reqwest::{Client, StatusCode, Url};
use serde::{Deserialize, Serialize};
use sqlx::{sqlite::SqlitePoolOptions, Row, SqlitePool};
use thiserror::Error;

pub const MAX_ASSIGNMENT_RESPONSE_BYTES: usize = 64 * 1024 * 1024;
const MAX_ASSIGNMENTS: usize = 72_000;
const MAX_CACHE_AGE_SECONDS: i64 = 24 * 60 * 60;
const MAX_PROBE_ID_BYTES: usize = 128;
const MAX_HOST_BYTES: usize = 253;
const MAX_REQUEST_UNITS: u8 = 6;
const MAX_COMPARISON_REFERENCES: usize = 5;
const MAX_VOTING_REFERENCES: usize = 3;

#[derive(Debug, Error)]
pub enum AssignmentError {
    #[error("invalid central API URL")]
    InvalidCentralUrl,
    #[error("assignment request failed: {0}")]
    Request(#[from] reqwest::Error),
    #[error("assignment endpoint returned HTTP {0}")]
    Http(StatusCode),
    #[error("assignment response exceeds the configured byte limit")]
    ResponseTooLarge,
    #[error("assignment JSON is invalid: {0}")]
    Decode(#[from] serde_json::Error),
    #[error("assignment envelope is invalid: {0}")]
    InvalidEnvelope(&'static str),
    #[error("assignment cache is unavailable: {0}")]
    Cache(#[from] sqlx::Error),
    #[error("assignment cache is empty")]
    EmptyCache,
    #[error("assignment cache fetchedAt is invalid")]
    InvalidFetchedAt,
    #[error("central assignment and cache are unavailable (central: {central}; cache: {cache})")]
    Unavailable { central: String, cache: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AssignmentEnvelope {
    pub schema_version: u8,
    pub generated_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub probe_id: String,
    pub site: Site,
    pub epoch: u64,
    pub assignments: Vec<Assignment>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Assignment {
    pub job_id: String,
    pub node_id: String,
    pub network: Network,
    pub node_kind: NodeKind,
    pub targets: Vec<AssignmentTarget>,
    pub checks: Vec<CheckKind>,
    pub scheduled_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub not_after: Option<DateTime<Utc>>,
    pub request_units: u8,
    pub detailed_owner: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AssignmentTargetKind {
    Rest,
    PeerTcp,
    WebSocket,
    ReferenceApi,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AssignmentTarget {
    pub kind: AssignmentTargetKind,
    pub scheme: String,
    pub host: String,
    pub port: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subject_public_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssignedCheck {
    pub observation_job_id: String,
    pub assignment_job_id: String,
    pub node: Node,
    pub diagnostic_rest_node: Option<Node>,
    pub reference_nodes: Vec<Node>,
    pub check: CheckKind,
    pub scheduled_at: DateTime<Utc>,
    pub request_units: u8,
    probe_check: CheckKind,
}

impl AssignedCheck {
    #[must_use]
    pub fn scheduled_job(&self) -> ScheduledJob {
        ScheduledJob {
            job_id: self.observation_job_id.clone(),
            node_id: self.node.id.clone(),
            check: self.probe_check,
            offset_seconds: 0,
            site_epoch_offset_seconds: 0,
            request_units: self.request_units,
        }
    }
}

impl Assignment {
    pub fn assigned_checks(&self) -> Result<Vec<AssignedCheck>, AssignmentError> {
        let mut assigned = Vec::with_capacity(self.checks.len());
        for check in &self.checks {
            let target = select_target(*check, &self.targets).ok_or(
                AssignmentError::InvalidEnvelope("check has no compatible target"),
            )?;
            let probe_check = if *check == CheckKind::Availability
                && target.kind == AssignmentTargetKind::PeerTcp
            {
                CheckKind::PeerTcp
            } else {
                *check
            };
            assigned.push(AssignedCheck {
                observation_job_id: format!("{}:{}", self.job_id, check_wire_name(*check)),
                assignment_job_id: self.job_id.clone(),
                node: node_for_target(self, target),
                diagnostic_rest_node: (*check == CheckKind::PeerTcp)
                    .then(|| {
                        self.targets
                            .iter()
                            .find(|candidate| candidate.kind == AssignmentTargetKind::Rest)
                            .map(|rest| node_for_target(self, rest))
                    })
                    .flatten(),
                reference_nodes: reference_targets(*check, target, &self.targets)
                    .into_iter()
                    .map(|reference| node_for_target(self, reference))
                    .collect(),
                check: *check,
                scheduled_at: self.scheduled_at,
                request_units: check.request_units(),
                probe_check,
            });
        }
        Ok(assigned)
    }
}

pub fn deferred_group_key(assignment: &Assignment) -> Result<String, AssignmentError> {
    let checks = assignment.assigned_checks()?;
    let first = checks.first().ok_or(AssignmentError::InvalidEnvelope(
        "deferred assignment has no checks",
    ))?;
    let deep = is_deep_check(first.check);
    if checks
        .iter()
        .any(|check| check.node.host != first.node.host || is_deep_check(check.check) != deep)
    {
        return Err(AssignmentError::InvalidEnvelope(
            "deferred assignment spans execution groups",
        ));
    }
    let kind = if deep { "deep" } else { "availability" };
    let identity = assignment
        .not_after
        .as_ref()
        .map_or(assignment.node_id.as_str(), |_| assignment.job_id.as_str());
    let scope = if assignment.not_after.is_some() {
        "retry"
    } else {
        "normal"
    };
    Ok(format!(
        "v2|{scope}|{}:{identity}|{}:{}|{kind}",
        identity.len(),
        first.node.host.len(),
        first.node.host
    ))
}

const fn is_deep_check(check: CheckKind) -> bool {
    !matches!(check, CheckKind::Availability | CheckKind::PeerTcp)
}

fn reference_targets<'a>(
    check: CheckKind,
    selected: &'a AssignmentTarget,
    targets: &'a [AssignmentTarget],
) -> Vec<&'a AssignmentTarget> {
    if check == CheckKind::Supernode {
        return targets
            .iter()
            .filter(|candidate| {
                matches!(
                    candidate.kind,
                    AssignmentTargetKind::Rest | AssignmentTargetKind::PeerTcp
                ) && !std::ptr::eq(*candidate, selected)
            })
            .collect();
    }
    if !matches!(
        check,
        CheckKind::Chain | CheckKind::Finalization | CheckKind::StateHash | CheckKind::Voting
    ) {
        return Vec::new();
    }

    let mut references = Vec::new();
    for candidate in targets {
        if candidate.kind != AssignmentTargetKind::ReferenceApi
            || std::ptr::eq(candidate, selected)
            || (matches!(
                check,
                CheckKind::Chain | CheckKind::Finalization | CheckKind::StateHash
            ) && candidate.subject_public_key.is_some())
            || (check == CheckKind::Voting && candidate.subject_public_key.is_none())
            || references.contains(&candidate)
        {
            continue;
        }
        references.push(candidate);
    }
    references
}

fn select_target(check: CheckKind, targets: &[AssignmentTarget]) -> Option<&AssignmentTarget> {
    let find = |kind| targets.iter().find(|target| target.kind == kind);
    match check {
        CheckKind::PeerTcp => find(AssignmentTargetKind::PeerTcp),
        CheckKind::WebSocket => find(AssignmentTargetKind::WebSocket),
        CheckKind::Certificate => targets
            .iter()
            .find(|target| target.kind == AssignmentTargetKind::Rest && target.scheme == "https"),
        CheckKind::Voting => targets
            .iter()
            .find(|target| {
                target.kind == AssignmentTargetKind::ReferenceApi
                    && target.subject_public_key.is_some()
            })
            .or_else(|| find(AssignmentTargetKind::ReferenceApi))
            .or_else(|| find(AssignmentTargetKind::Rest)),
        CheckKind::Availability => {
            find(AssignmentTargetKind::Rest).or_else(|| find(AssignmentTargetKind::PeerTcp))
        }
        CheckKind::Chain
        | CheckKind::Finalization
        | CheckKind::Peers
        | CheckKind::StateHash
        | CheckKind::Harvest
        | CheckKind::Supernode
        | CheckKind::Identity => find(AssignmentTargetKind::Rest),
    }
}

fn node_for_target(assignment: &Assignment, target: &AssignmentTarget) -> Node {
    let has_application_endpoint = matches!(
        target.kind,
        AssignmentTargetKind::Rest
            | AssignmentTargetKind::ReferenceApi
            | AssignmentTargetKind::WebSocket
    );
    let peer_port = assignment
        .targets
        .iter()
        .find(|candidate| candidate.kind == AssignmentTargetKind::PeerTcp)
        .map_or_else(
            || match assignment.network {
                Network::SymbolMainnet => 7900,
                Network::Nis1Mainnet => 7890,
            },
            |candidate| candidate.port,
        );
    Node {
        id: assignment.node_id.clone(),
        network: assignment.network,
        kind: if target.kind == AssignmentTargetKind::ReferenceApi {
            NodeKind::Api
        } else {
            assignment.node_kind
        },
        display_name: assignment.node_id.clone(),
        public_key: target.subject_public_key.clone(),
        host: target.host.clone(),
        rest_scheme: has_application_endpoint.then(|| target.scheme.clone()),
        rest_port: has_application_endpoint.then_some(target.port),
        p2p_port: if target.kind == AssignmentTargetKind::PeerTcp {
            target.port
        } else {
            peer_port
        },
    }
}

#[derive(Clone)]
pub struct AssignmentClient {
    http: Client,
    endpoint: Url,
    bearer: String,
    probe_id: String,
    site: Site,
    max_response_bytes: usize,
}

impl AssignmentClient {
    pub fn new(
        http: Client,
        central_url: &str,
        bearer: impl Into<String>,
        probe_id: impl Into<String>,
        site: Site,
    ) -> Result<Self, AssignmentError> {
        let base = Url::parse(central_url).map_err(|_| AssignmentError::InvalidCentralUrl)?;
        let endpoint = base
            .join("internal/v1/assignments")
            .map_err(|_| AssignmentError::InvalidCentralUrl)?;
        let probe_id = probe_id.into();
        validate_probe_id(&probe_id)?;
        Ok(Self {
            http,
            endpoint,
            bearer: bearer.into(),
            probe_id,
            site,
            max_response_bytes: MAX_ASSIGNMENT_RESPONSE_BYTES,
        })
    }

    #[must_use]
    pub fn with_max_response_bytes(mut self, max_response_bytes: usize) -> Self {
        self.max_response_bytes = max_response_bytes;
        self
    }

    pub async fn fetch_at(
        &self,
        fetched_at: DateTime<Utc>,
    ) -> Result<AssignmentEnvelope, AssignmentError> {
        let bytes = self.fetch_bytes(false).await?;
        decode_assignment_envelope(&bytes, &self.probe_id, self.site, fetched_at)
    }

    pub async fn fetch(&self) -> Result<AssignmentEnvelope, AssignmentError> {
        let bytes = self.fetch_bytes(false).await?;
        let fetched_at = Utc::now();
        decode_assignment_envelope(&bytes, &self.probe_id, self.site, fetched_at)
    }

    pub async fn fetch_retries(&self) -> Result<AssignmentEnvelope, AssignmentError> {
        let bytes = self.fetch_bytes(true).await?;
        let fetched_at = Utc::now();
        decode_assignment_envelope(&bytes, &self.probe_id, self.site, fetched_at)
    }

    async fn fetch_bytes(&self, retry_only: bool) -> Result<Vec<u8>, AssignmentError> {
        let mut endpoint = self.endpoint.clone();
        endpoint
            .query_pairs_mut()
            .append_pair("probeId", &self.probe_id);
        endpoint
            .query_pairs_mut()
            .append_pair("site", site_name(self.site));
        endpoint.query_pairs_mut().append_pair("schemaVersion", "2");
        if retry_only {
            endpoint.query_pairs_mut().append_pair("retryOnly", "true");
        }
        let mut response = self
            .http
            .get(endpoint)
            .bearer_auth(&self.bearer)
            .send()
            .await?;
        if !response.status().is_success() {
            return Err(AssignmentError::Http(response.status()));
        }
        if response
            .content_length()
            .is_some_and(|length| length > self.max_response_bytes as u64)
        {
            return Err(AssignmentError::ResponseTooLarge);
        }

        let initial_capacity = response
            .content_length()
            .and_then(|length| usize::try_from(length).ok())
            .unwrap_or(0)
            .min(self.max_response_bytes);
        let mut bytes = Vec::with_capacity(initial_capacity);
        while let Some(chunk) = response.chunk().await? {
            if chunk.len() > self.max_response_bytes.saturating_sub(bytes.len()) {
                return Err(AssignmentError::ResponseTooLarge);
            }
            bytes.extend_from_slice(&chunk);
        }
        Ok(bytes)
    }
}

#[derive(Debug, Clone)]
pub struct AssignmentCache {
    pool: SqlitePool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeferredAssignment {
    pub group_key: String,
    pub assignment: Assignment,
    pub valid_until: DateTime<Utc>,
}

impl AssignmentCache {
    pub async fn in_memory() -> Result<Self, AssignmentError> {
        Self::connect("sqlite::memory:").await
    }

    pub async fn connect(url: &str) -> Result<Self, AssignmentError> {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_secs(5))
            .connect(url)
            .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS assignment_cache (
                 singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                 fetched_at TEXT NOT NULL,
                 envelope_json BLOB NOT NULL
             )",
        )
        .execute(&pool)
        .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS deferred_assignments (
                 group_key TEXT PRIMARY KEY,
                 assignment_json BLOB NOT NULL,
                 deferred_at TEXT NOT NULL,
                 valid_until TEXT NOT NULL
             )",
        )
        .execute(&pool)
        .await?;
        let columns = sqlx::query("PRAGMA table_info(deferred_assignments)")
            .fetch_all(&pool)
            .await?;
        if !columns.iter().any(|column| {
            column
                .try_get::<String, _>("name")
                .is_ok_and(|name| name == "valid_until")
        }) {
            sqlx::query("ALTER TABLE deferred_assignments ADD COLUMN valid_until TEXT")
                .execute(&pool)
                .await?;
        }
        Ok(Self { pool })
    }

    pub async fn defer(
        &self,
        group_key: &str,
        assignment: &Assignment,
        deferred_at: DateTime<Utc>,
        valid_until: DateTime<Utc>,
    ) -> Result<(), AssignmentError> {
        if valid_until <= deferred_at {
            return Err(AssignmentError::InvalidEnvelope(
                "deferred assignment is already expired",
            ));
        }
        let bytes = serde_json::to_vec(assignment)?;
        sqlx::query(
            "INSERT INTO deferred_assignments(
                 group_key, assignment_json, deferred_at, valid_until
             ) VALUES (?, ?, ?, ?)
             ON CONFLICT(group_key) DO UPDATE SET
                 assignment_json = excluded.assignment_json,
                 deferred_at = excluded.deferred_at,
                 valid_until = excluded.valid_until",
        )
        .bind(group_key)
        .bind(bytes)
        .bind(deferred_at.to_rfc3339_opts(SecondsFormat::Nanos, true))
        .bind(valid_until.to_rfc3339_opts(SecondsFormat::Nanos, true))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn load_deferred(
        &self,
        now: DateTime<Utc>,
    ) -> Result<Vec<DeferredAssignment>, AssignmentError> {
        let now = now.to_rfc3339_opts(SecondsFormat::Nanos, true);
        let mut transaction = self.pool.begin().await?;
        sqlx::query(
            "DELETE FROM deferred_assignments
             WHERE valid_until IS NULL OR valid_until <= ?",
        )
        .bind(&now)
        .execute(&mut *transaction)
        .await?;
        struct StoredDeferred {
            original_group_key: String,
            assignment: Assignment,
            deferred_at: String,
            valid_until: String,
        }

        let mut winners = BTreeMap::<String, StoredDeferred>::new();
        let mut stored_row_count = 0_usize;
        {
            let mut rows = sqlx::query(
                "SELECT group_key, assignment_json, deferred_at, valid_until
                 FROM deferred_assignments ORDER BY deferred_at, group_key",
            )
            .fetch(&mut *transaction);
            while let Some(row) = rows.try_next().await? {
                let bytes: Vec<u8> = row.try_get("assignment_json")?;
                let assignment: Assignment = serde_json::from_slice(&bytes)?;
                let canonical_group_key = deferred_group_key(&assignment)?;
                stored_row_count = stored_row_count.saturating_add(1);
                let candidate = StoredDeferred {
                    original_group_key: row.try_get("group_key")?,
                    assignment,
                    deferred_at: row.try_get("deferred_at")?,
                    valid_until: row.try_get("valid_until")?,
                };
                match winners.entry(canonical_group_key) {
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(candidate);
                    }
                    std::collections::btree_map::Entry::Occupied(mut entry) => {
                        let current = entry.get();
                        let candidate_order = (
                            candidate.assignment.scheduled_at,
                            &candidate.deferred_at,
                            &candidate.original_group_key,
                        );
                        let current_order = (
                            current.assignment.scheduled_at,
                            &current.deferred_at,
                            &current.original_group_key,
                        );
                        if candidate_order >= current_order {
                            entry.insert(candidate);
                        }
                    }
                }
            }
        }

        let needs_compaction = stored_row_count != winners.len()
            || winners
                .iter()
                .any(|(canonical, row)| canonical != &row.original_group_key);
        if needs_compaction {
            sqlx::query("DELETE FROM deferred_assignments")
                .execute(&mut *transaction)
                .await?;
            for (canonical_group_key, row) in &winners {
                sqlx::query(
                    "INSERT INTO deferred_assignments(
                         group_key, assignment_json, deferred_at, valid_until
                     ) VALUES (?, ?, ?, ?)",
                )
                .bind(canonical_group_key)
                .bind(serde_json::to_vec(&row.assignment)?)
                .bind(&row.deferred_at)
                .bind(&row.valid_until)
                .execute(&mut *transaction)
                .await?;
            }
        }

        let mut rows = winners
            .into_iter()
            .map(|(group_key, row)| {
                let valid_until = DateTime::parse_from_rfc3339(&row.valid_until)
                    .map_err(|_| AssignmentError::InvalidFetchedAt)?
                    .with_timezone(&Utc);
                Ok((
                    row.deferred_at,
                    DeferredAssignment {
                        group_key,
                        assignment: row.assignment,
                        valid_until,
                    },
                ))
            })
            .collect::<Result<Vec<_>, AssignmentError>>()?;
        rows.sort_by(|left, right| {
            left.0
                .cmp(&right.0)
                .then_with(|| left.1.group_key.cmp(&right.1.group_key))
        });
        transaction.commit().await?;
        Ok(rows.into_iter().map(|(_, row)| row).collect())
    }

    pub async fn complete_deferred(&self, group_key: &str) -> Result<(), AssignmentError> {
        sqlx::query("DELETE FROM deferred_assignments WHERE group_key = ?")
            .bind(group_key)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn replace(
        &self,
        envelope: &AssignmentEnvelope,
        fetched_at: DateTime<Utc>,
        expected_probe_id: &str,
        expected_site: Site,
    ) -> Result<(), AssignmentError> {
        validate_envelope(
            envelope,
            expected_probe_id,
            expected_site,
            fetched_at,
            fetched_at,
        )?;
        let bytes = serde_json::to_vec(envelope)?;
        if bytes.len() > MAX_ASSIGNMENT_RESPONSE_BYTES {
            return Err(AssignmentError::ResponseTooLarge);
        }
        let fetched_at = fetched_at.to_rfc3339_opts(SecondsFormat::Nanos, true);
        let mut transaction = self.pool.begin().await?;
        sqlx::query(
            "INSERT INTO assignment_cache(singleton, fetched_at, envelope_json)
             VALUES (1, ?, ?)
             ON CONFLICT(singleton) DO UPDATE SET
                 fetched_at = excluded.fetched_at,
                 envelope_json = excluded.envelope_json",
        )
        .bind(fetched_at)
        .bind(bytes)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    pub async fn load(
        &self,
        now: DateTime<Utc>,
        expected_probe_id: &str,
        expected_site: Site,
    ) -> Result<AssignmentEnvelope, AssignmentError> {
        let row = sqlx::query(
            "SELECT fetched_at, envelope_json FROM assignment_cache WHERE singleton = 1",
        )
        .fetch_optional(&self.pool)
        .await?
        .ok_or(AssignmentError::EmptyCache)?;
        let fetched_at: String = row.try_get("fetched_at")?;
        let fetched_at = DateTime::parse_from_rfc3339(&fetched_at)
            .map_err(|_| AssignmentError::InvalidFetchedAt)?
            .with_timezone(&Utc);
        if now.signed_duration_since(fetched_at) > chrono::Duration::seconds(MAX_CACHE_AGE_SECONDS)
        {
            return Err(AssignmentError::InvalidEnvelope(
                "fetchedAt is older than 24 hours",
            ));
        }
        let bytes: Vec<u8> = row.try_get("envelope_json")?;
        if bytes.len() > MAX_ASSIGNMENT_RESPONSE_BYTES {
            return Err(AssignmentError::ResponseTooLarge);
        }
        let envelope: AssignmentEnvelope = serde_json::from_slice(&bytes)?;
        validate_envelope(&envelope, expected_probe_id, expected_site, fetched_at, now)?;
        Ok(envelope)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AssignmentSource {
    Central,
    Cache,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadedAssignments {
    pub envelope: AssignmentEnvelope,
    pub source: AssignmentSource,
}

#[derive(Clone)]
pub struct AssignmentProvider {
    client: AssignmentClient,
    cache: AssignmentCache,
}

impl AssignmentProvider {
    #[must_use]
    pub fn new(client: AssignmentClient, cache: AssignmentCache) -> Self {
        Self { client, cache }
    }

    pub async fn refresh_at(
        &self,
        fetched_at: DateTime<Utc>,
    ) -> Result<LoadedAssignments, AssignmentError> {
        match self.client.fetch_at(fetched_at).await {
            Ok(envelope) => {
                self.cache
                    .replace(
                        &envelope,
                        fetched_at,
                        &self.client.probe_id,
                        self.client.site,
                    )
                    .await?;
                Ok(LoadedAssignments {
                    envelope,
                    source: AssignmentSource::Central,
                })
            }
            Err(central) => self
                .cache
                .load(fetched_at, &self.client.probe_id, self.client.site)
                .await
                .map(|envelope| LoadedAssignments {
                    envelope,
                    source: AssignmentSource::Cache,
                })
                .map_err(|cache| AssignmentError::Unavailable {
                    central: central.to_string(),
                    cache: cache.to_string(),
                }),
        }
    }

    pub async fn refresh(&self) -> Result<LoadedAssignments, AssignmentError> {
        match self.client.fetch().await {
            Ok(envelope) => {
                let fetched_at = Utc::now();
                self.cache
                    .replace(
                        &envelope,
                        fetched_at,
                        &self.client.probe_id,
                        self.client.site,
                    )
                    .await?;
                Ok(LoadedAssignments {
                    envelope,
                    source: AssignmentSource::Central,
                })
            }
            Err(central) => {
                let now = Utc::now();
                self.cache
                    .load(now, &self.client.probe_id, self.client.site)
                    .await
                    .map(|envelope| LoadedAssignments {
                        envelope,
                        source: AssignmentSource::Cache,
                    })
                    .map_err(|cache| AssignmentError::Unavailable {
                        central: central.to_string(),
                        cache: cache.to_string(),
                    })
            }
        }
    }
}

pub fn decode_assignment_envelope(
    bytes: &[u8],
    expected_probe_id: &str,
    expected_site: Site,
    fetched_at: DateTime<Utc>,
) -> Result<AssignmentEnvelope, AssignmentError> {
    if bytes.len() > MAX_ASSIGNMENT_RESPONSE_BYTES {
        return Err(AssignmentError::ResponseTooLarge);
    }
    let envelope: AssignmentEnvelope = serde_json::from_slice(bytes)?;
    validate_envelope(
        &envelope,
        expected_probe_id,
        expected_site,
        fetched_at,
        fetched_at,
    )?;
    Ok(envelope)
}

fn validate_envelope(
    envelope: &AssignmentEnvelope,
    expected_probe_id: &str,
    expected_site: Site,
    fetched_at: DateTime<Utc>,
    now: DateTime<Utc>,
) -> Result<(), AssignmentError> {
    validate_probe_id(expected_probe_id)?;
    if !matches!(envelope.schema_version, 1 | 2) {
        return Err(AssignmentError::InvalidEnvelope("unknown schemaVersion"));
    }
    if envelope.probe_id != expected_probe_id {
        return Err(AssignmentError::InvalidEnvelope("probeId mismatch"));
    }
    if envelope.site != expected_site {
        return Err(AssignmentError::InvalidEnvelope("site mismatch"));
    }
    if fetched_at > now {
        return Err(AssignmentError::InvalidEnvelope(
            "fetchedAt is in the future",
        ));
    }
    if now.signed_duration_since(fetched_at) > chrono::Duration::seconds(MAX_CACHE_AGE_SECONDS) {
        return Err(AssignmentError::InvalidEnvelope(
            "fetchedAt is older than 24 hours",
        ));
    }
    if envelope.generated_at > fetched_at {
        return Err(AssignmentError::InvalidEnvelope(
            "generatedAt is after fetchedAt",
        ));
    }
    let generated_epoch = u64::try_from(envelope.generated_at.timestamp())
        .map_err(|_| AssignmentError::InvalidEnvelope("generatedAt predates Unix epoch"))?
        / 600;
    if envelope.epoch != generated_epoch {
        return Err(AssignmentError::InvalidEnvelope(
            "epoch does not match generatedAt",
        ));
    }
    if envelope.assignments.len() > MAX_ASSIGNMENTS {
        return Err(AssignmentError::InvalidEnvelope("too many assignments"));
    }
    let epoch_start = i64::try_from(envelope.epoch)
        .ok()
        .and_then(|epoch| epoch.checked_mul(600))
        .and_then(|timestamp| DateTime::from_timestamp(timestamp, 0))
        .ok_or(AssignmentError::InvalidEnvelope("epoch is invalid"))?;
    let horizon_end = epoch_start + chrono::Duration::hours(24);
    if envelope.expires_at != horizon_end {
        return Err(AssignmentError::InvalidEnvelope(
            "expiresAt does not cover exactly 144 epochs",
        ));
    }
    if envelope.expires_at <= now {
        return Err(AssignmentError::InvalidEnvelope("envelope is expired"));
    }
    let mut job_ids = HashSet::with_capacity(envelope.assignments.len());
    for assignment in &envelope.assignments {
        validate_assignment(
            assignment,
            envelope.schema_version,
            epoch_start,
            envelope.expires_at,
        )?;
        if !job_ids.insert(assignment.job_id.as_str()) {
            return Err(AssignmentError::InvalidEnvelope("duplicate jobId"));
        }
    }
    Ok(())
}

fn validate_assignment(
    assignment: &Assignment,
    schema_version: u8,
    epoch_start: DateTime<Utc>,
    expires_at: DateTime<Utc>,
) -> Result<(), AssignmentError> {
    if assignment.job_id.len() != 64
        || !assignment
            .job_id
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(AssignmentError::InvalidEnvelope("invalid jobId"));
    }
    if assignment.node_id.is_empty() {
        return Err(AssignmentError::InvalidEnvelope("empty nodeId"));
    }
    if assignment.scheduled_at < epoch_start || assignment.scheduled_at >= expires_at {
        return Err(AssignmentError::InvalidEnvelope(
            "scheduledAt outside envelope",
        ));
    }
    if assignment
        .not_after
        .is_some_and(|not_after| not_after < assignment.scheduled_at || not_after > expires_at)
    {
        return Err(AssignmentError::InvalidEnvelope(
            "assignment notAfter is outside the envelope",
        ));
    }
    if assignment.checks.is_empty() || assignment.targets.is_empty() {
        return Err(AssignmentError::InvalidEnvelope("empty checks or targets"));
    }
    let unique_checks = assignment.checks.iter().copied().collect::<HashSet<_>>();
    if unique_checks.len() != assignment.checks.len() {
        return Err(AssignmentError::InvalidEnvelope("duplicate check"));
    }
    let request_units = assignment
        .checks
        .iter()
        .try_fold(0_u8, |total, check| {
            total.checked_add(check.request_units())
        })
        .ok_or(AssignmentError::InvalidEnvelope("requestUnits overflow"))?;
    // Central may coalesce repeated deferred checks into one wire check while
    // retaining their already-admitted units. The wire value is therefore an
    // upper bound, while the Probe must never execute more units than declared.
    if assignment.request_units == 0
        || assignment.request_units > MAX_REQUEST_UNITS
        || assignment.request_units < request_units
    {
        return Err(AssignmentError::InvalidEnvelope("invalid requestUnits"));
    }
    for target in &assignment.targets {
        validate_target(target)?;
        if assignment.network == Network::SymbolMainnet
            && matches!(
                target.kind,
                AssignmentTargetKind::Rest | AssignmentTargetKind::ReferenceApi
            )
            && target.port == 7900
        {
            return Err(AssignmentError::InvalidEnvelope(
                "Symbol P2P port cannot be a REST target",
            ));
        }
    }
    let generic_reference_count = assignment
        .targets
        .iter()
        .filter(|target| {
            target.kind == AssignmentTargetKind::ReferenceApi && target.subject_public_key.is_none()
        })
        .count();
    let voting_reference_count = assignment
        .targets
        .iter()
        .filter(|target| {
            target.kind == AssignmentTargetKind::ReferenceApi && target.subject_public_key.is_some()
        })
        .count();
    let has_comparison_check = assignment.checks.iter().any(|check| {
        matches!(
            check,
            CheckKind::Chain | CheckKind::Finalization | CheckKind::StateHash
        )
    });
    let has_voting_check = assignment.checks.contains(&CheckKind::Voting);
    if generic_reference_count > MAX_COMPARISON_REFERENCES
        || voting_reference_count > MAX_VOTING_REFERENCES
        || (generic_reference_count > 0 && (schema_version != 2 || !has_comparison_check))
        || (voting_reference_count > 0 && !has_voting_check)
    {
        return Err(AssignmentError::InvalidEnvelope(
            "invalid reference target fanout",
        ));
    }
    assignment.assigned_checks()?;
    Ok(())
}

fn validate_target(target: &AssignmentTarget) -> Result<(), AssignmentError> {
    if target.host.is_empty() || target.host.len() > MAX_HOST_BYTES || target.port == 0 {
        return Err(AssignmentError::InvalidEnvelope("invalid target authority"));
    }
    let valid_scheme = match target.kind {
        AssignmentTargetKind::Rest | AssignmentTargetKind::ReferenceApi => {
            matches!(target.scheme.as_str(), "http" | "https")
        }
        AssignmentTargetKind::PeerTcp => target.scheme == "tcp",
        AssignmentTargetKind::WebSocket => matches!(target.scheme.as_str(), "ws" | "wss"),
    };
    if !valid_scheme {
        return Err(AssignmentError::InvalidEnvelope("invalid target scheme"));
    }
    if let Some(subject_public_key) = &target.subject_public_key {
        if target.kind != AssignmentTargetKind::ReferenceApi
            || subject_public_key.len() != 64
            || !subject_public_key
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(AssignmentError::InvalidEnvelope("invalid subjectPublicKey"));
        }
    }
    Ok(())
}

fn validate_probe_id(probe_id: &str) -> Result<(), AssignmentError> {
    if probe_id.is_empty()
        || probe_id.len() > MAX_PROBE_ID_BYTES
        || !probe_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(AssignmentError::InvalidEnvelope("invalid probeId"));
    }
    Ok(())
}

const fn site_name(site: Site) -> &'static str {
    match site {
        Site::A => "a",
        Site::B => "b",
    }
}

const fn check_wire_name(check: CheckKind) -> &'static str {
    match check {
        CheckKind::Availability => "availability",
        CheckKind::PeerTcp => "peer_tcp",
        CheckKind::Chain => "chain",
        CheckKind::Finalization => "finalization",
        CheckKind::Peers => "peers",
        CheckKind::StateHash => "state_hash",
        CheckKind::Harvest => "harvest",
        CheckKind::Supernode => "supernode",
        CheckKind::Identity => "identity",
        CheckKind::Certificate => "certificate",
        CheckKind::Voting => "voting",
        CheckKind::WebSocket => "web_socket",
    }
}
