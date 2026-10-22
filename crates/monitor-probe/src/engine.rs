use std::{
    collections::BTreeMap,
    fmt::Debug,
    net::{IpAddr, SocketAddr},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD, Engine as Base64Engine};
use chrono::{DateTime, TimeZone, Utc};
use futures::future::join_all;
use monitor_core::{
    model::{Dimension, Network, Node, Observation, Outcome, Site},
    safety::{validate_public_target, TargetSafetyError},
    scheduler::{CheckKind, ScheduledJob},
};
use reqwest::{
    header::{
        CONNECTION, LOCATION, SEC_WEBSOCKET_ACCEPT, SEC_WEBSOCKET_KEY, SEC_WEBSOCKET_VERSION,
        UPGRADE,
    },
    redirect::Policy,
    StatusCode,
};
use rustls::{pki_types::ServerName, ClientConfig, RootCertStore};
use serde_json::{json, Value};
use sha1::{Digest, Sha1};
use thiserror::Error;
use tokio::{net::TcpStream, sync::Mutex, time::Instant};
use tokio_rustls::TlsConnector;
use url::Url;
use uuid::Uuid;

use crate::history::{FinalizationAssessment, FinalizationHistory, FinalizationSample};
use crate::limiter::Limits;
use crate::protocol::{
    parse_harvester_count, parse_nis1_chain, parse_nis1_health, parse_nis1_identity,
    parse_peer_count, parse_supernode_info, parse_supernode_registration, parse_supernode_round,
    parse_symbol_chain, parse_symbol_health, parse_symbol_identity, parse_symbol_state_hash,
    parse_voting_remaining, request_path, ChainSnapshot,
};

pub(crate) const SUPER_NODE_API_BASE: &str = "https://nem.io/supernode/api/";
const LOCAL_CHAIN_TTL: Duration = Duration::from_secs(60);
const REFERENCE_CHAIN_TTL: Duration = Duration::from_secs(60);
const REFERENCE_HASH_TTL: Duration = Duration::from_secs(10 * 60);
const MAX_CACHED_LOCAL_CHAINS: usize = 512;
const MAX_CACHED_REFERENCE_CHAINS: usize = 8;
const MAX_CACHED_REFERENCE_HASHES: usize = 1_536;

#[derive(Debug, Error)]
pub enum ProbeError {
    #[error("measurement request or traffic budget exhausted")]
    BudgetExhausted,
    #[error("node has no REST endpoint")]
    NoRestEndpoint,
    #[error("invalid endpoint: {0}")]
    InvalidEndpoint(String),
    #[error(transparent)]
    UnsafeTarget(#[from] TargetSafetyError),
    #[error("DNS lookup failed: {0}")]
    Dns(String),
    #[error("request failed: {0}")]
    Request(String),
    #[error("HTTP {0}")]
    Http(StatusCode),
    #[error("peer TCP connection failed")]
    Tcp,
    #[error("invalid response: {0}")]
    Payload(String),
    #[error("response exceeded the 4 MiB limit")]
    ResponseTooLarge,
    #[error("connected remote address was not one of the pinned DNS answers")]
    RemoteAddressMismatch,
}

/// A durable local budget. Reservations remain charged if a request is cancelled.
#[async_trait]
pub trait MeasurementBudget: Send + Sync + Debug {
    async fn reserve_request(&self, _host: &str) -> Result<(), ProbeError> {
        Ok(())
    }
    async fn reserve(&self, bytes: u64) -> Result<String, ProbeError>;
    async fn settle(&self, receipt: &str, reserved: u64, used: u64) -> Result<(), ProbeError>;
}

#[async_trait]
pub trait ProbeHttpTransport: Send + Sync + Debug {
    async fn get_json(&self, url: Url, timeout: Duration) -> Result<Value, ProbeError>;

    async fn get_json_same_origin(&self, url: Url, timeout: Duration) -> Result<Value, ProbeError> {
        self.get_json(url, timeout).await
    }

    async fn get_json_timed(&self, url: Url, timeout: Duration) -> Result<TimedJson, ProbeError> {
        let started = Instant::now();
        let value = self.get_json(url, timeout).await?;
        Ok(TimedJson {
            value,
            latency_ms: duration_millis(started.elapsed()),
        })
    }

    async fn websocket_handshake(&self, _url: Url, _timeout: Duration) -> Result<(), ProbeError> {
        Err(ProbeError::Request(
            "WebSocket handshake transport is unavailable".into(),
        ))
    }

    async fn certificate_info(
        &self,
        _url: Url,
        _timeout: Duration,
    ) -> Result<CertificateInfo, ProbeError> {
        Err(ProbeError::Request(
            "TLS certificate transport is unavailable".into(),
        ))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TimedJson {
    pub value: Value,
    pub latency_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CertificateInfo {
    pub not_after: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TcpConnectResult {
    Connected,
    ConnectionRefused,
    Timeout,
    NetworkUnreachable,
    Other,
}

impl TcpConnectResult {
    const fn wire_name(self) -> &'static str {
        match self {
            Self::Connected => "connected",
            Self::ConnectionRefused => "connection_refused",
            Self::Timeout => "timeout",
            Self::NetworkUnreachable => "network_unreachable",
            Self::Other => "other",
        }
    }

    const fn description(self) -> &'static str {
        match self {
            Self::Connected => "connected",
            Self::ConnectionRefused => "connection refused",
            Self::Timeout => "timed out",
            Self::NetworkUnreachable => "network unreachable",
            Self::Other => "connection error",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TcpConnectAttempt {
    pub address: SocketAddr,
    pub elapsed_ms: u64,
    pub result: TcpConnectResult,
}

#[async_trait]
pub trait PeerTcpTransport: Send + Sync + Debug {
    async fn resolve(
        &self,
        host: &str,
        port: u16,
        timeout: Duration,
    ) -> Result<Vec<IpAddr>, ProbeError>;

    async fn connect(&self, address: SocketAddr, timeout: Duration) -> TcpConnectAttempt;
}

#[derive(Debug)]
struct NetworkPeerTcpTransport;

#[derive(Debug)]
struct PeerTcpReport {
    outcome: Outcome,
    evidence: Value,
}

#[async_trait]
impl PeerTcpTransport for NetworkPeerTcpTransport {
    async fn resolve(
        &self,
        host: &str,
        port: u16,
        timeout: Duration,
    ) -> Result<Vec<IpAddr>, ProbeError> {
        tokio::time::timeout(timeout, resolve_public(host, port))
            .await
            .map_err(|_| ProbeError::Dns("lookup timed out".into()))?
    }

    async fn connect(&self, address: SocketAddr, timeout: Duration) -> TcpConnectAttempt {
        let started = Instant::now();
        let result = match tokio::time::timeout(timeout, TcpStream::connect(address)).await {
            Ok(Ok(_)) => TcpConnectResult::Connected,
            Ok(Err(error)) => classify_tcp_error(&error),
            Err(_) => TcpConnectResult::Timeout,
        };
        TcpConnectAttempt {
            address,
            elapsed_ms: duration_millis(started.elapsed()),
            result,
        }
    }
}

fn classify_tcp_error(error: &std::io::Error) -> TcpConnectResult {
    match error.kind() {
        std::io::ErrorKind::ConnectionRefused => TcpConnectResult::ConnectionRefused,
        std::io::ErrorKind::TimedOut => TcpConnectResult::Timeout,
        std::io::ErrorKind::NetworkUnreachable | std::io::ErrorKind::HostUnreachable => {
            TcpConnectResult::NetworkUnreachable
        }
        _ if matches!(error.raw_os_error(), Some(101 | 113)) => {
            TcpConnectResult::NetworkUnreachable
        }
        _ => TcpConnectResult::Other,
    }
}

#[derive(Debug)]
struct NetworkHttpTransport {
    user_agent: String,
    rate_limiter: Arc<OutboundRateLimiter>,
    budget: Option<Arc<dyn MeasurementBudget>>,
}

#[derive(Debug)]
struct BudgetedPeerTcpTransport {
    budget: Arc<dyn MeasurementBudget>,
}

#[async_trait]
impl PeerTcpTransport for BudgetedPeerTcpTransport {
    async fn resolve(
        &self,
        host: &str,
        port: u16,
        timeout: Duration,
    ) -> Result<Vec<IpAddr>, ProbeError> {
        // TCP checks exchange no application payload; conservatively account for DNS and connects.
        self.budget.reserve(32 * 1024).await?;
        let mut addresses = NetworkPeerTcpTransport.resolve(host, port, timeout).await?;
        addresses.truncate(8);
        for _ in &addresses {
            self.budget.reserve_request(host).await?;
        }
        Ok(addresses)
    }
    async fn connect(&self, address: SocketAddr, timeout: Duration) -> TcpConnectAttempt {
        NetworkPeerTcpTransport.connect(address, timeout).await
    }
}

#[derive(Debug)]
struct OutboundRateLimiter {
    state: Mutex<RequestRateState>,
    pressure: AtomicBool,
}

#[derive(Debug)]
struct RequestRateState {
    tokens: f64,
    last_refill: Instant,
    rate_per_second: f64,
    burst: f64,
}

#[derive(Debug)]
struct CachedReferenceSample<T> {
    value: Option<T>,
    expires_at: Instant,
}

type ReferenceCacheEntry<T> = Arc<Mutex<Option<CachedReferenceSample<T>>>>;

#[derive(Debug, Default)]
struct ReferenceSampleCache {
    local_chains: Mutex<BTreeMap<String, ReferenceCacheEntry<ChainSnapshot>>>,
    reference_chains: Mutex<BTreeMap<String, ReferenceCacheEntry<ChainSnapshot>>>,
    state_hashes: Mutex<BTreeMap<String, ReferenceCacheEntry<String>>>,
}

#[derive(Debug, Clone)]
pub struct ProbeEngine {
    http: Arc<dyn ProbeHttpTransport>,
    peer_tcp_transport: Arc<dyn PeerTcpTransport>,
    outbound_rate: Arc<OutboundRateLimiter>,
    symbol_reference: Option<Node>,
    community_budgeted: bool,
    finalization_history: Option<FinalizationHistory>,
    reference_cache: Arc<ReferenceSampleCache>,
}

impl ProbeEngine {
    #[must_use]
    pub fn new(contact: &str) -> Self {
        Self::with_limits(contact, Limits::default())
    }

    #[must_use]
    pub fn with_limits(contact: &str, limits: Limits) -> Self {
        let outbound_rate = Arc::new(OutboundRateLimiter::new(limits));
        Self {
            http: Arc::new(NetworkHttpTransport {
                user_agent: format!("Harvest-Scope-Network-Monitor/0.1 ({contact})"),
                rate_limiter: Arc::clone(&outbound_rate),
                budget: None,
            }),
            peer_tcp_transport: Arc::new(NetworkPeerTcpTransport),
            outbound_rate,
            symbol_reference: None,
            community_budgeted: false,
            finalization_history: None,
            reference_cache: Arc::new(ReferenceSampleCache::default()),
        }
    }

    #[must_use]
    pub fn with_measurement_budget(
        mut self,
        contact: &str,
        budget: Arc<dyn MeasurementBudget>,
    ) -> Self {
        self.http = Arc::new(NetworkHttpTransport {
            user_agent: format!("Harvest-Scope-Network-Monitor/0.1 ({contact})"),
            rate_limiter: Arc::clone(&self.outbound_rate),
            budget: Some(Arc::clone(&budget)),
        });
        self.peer_tcp_transport = Arc::new(BudgetedPeerTcpTransport { budget });
        self.community_budgeted = true;
        self
    }

    #[must_use]
    pub fn with_http_transport<T>(mut self, transport: T) -> Self
    where
        T: ProbeHttpTransport + 'static,
    {
        self.http = Arc::new(transport);
        self
    }

    #[must_use]
    pub fn with_peer_tcp_transport<T>(mut self, transport: T) -> Self
    where
        T: PeerTcpTransport + 'static,
    {
        self.peer_tcp_transport = Arc::new(transport);
        self
    }

    #[must_use]
    pub fn with_symbol_reference(mut self, node: Node) -> Self {
        self.symbol_reference = Some(node);
        self
    }

    #[must_use]
    pub fn with_finalization_history(mut self, history: FinalizationHistory) -> Self {
        self.finalization_history = Some(history);
        self
    }

    pub fn set_host_pressure(&self, pressure: bool) {
        self.outbound_rate.set_pressure(pressure);
    }

    pub async fn check(&self, node: &Node, job: &ScheduledJob, site: Site) -> Observation {
        self.check_primary(node, job, site).await
    }

    pub async fn check_all(
        &self,
        node: &Node,
        job: &ScheduledJob,
        site: Site,
        schema_version: u8,
    ) -> Vec<Observation> {
        self.check_all_with_references(node, job, site, schema_version, &[])
            .await
    }

    pub async fn check_all_with_references(
        &self,
        node: &Node,
        job: &ScheduledJob,
        site: Site,
        schema_version: u8,
        reference_nodes: &[Node],
    ) -> Vec<Observation> {
        let references = if schema_version == 2 {
            reference_nodes
        } else {
            &[]
        };
        let primary = self
            .check_primary_with_references(node, job, site, references)
            .await;
        let mut observations = vec![primary];
        if schema_version == 2 && matches!(job.check, CheckKind::Availability | CheckKind::PeerTcp)
        {
            let latency_ms = observations[0]
                .evidence
                .get("latencyMs")
                .and_then(Value::as_u64);
            let outcome = latency_ms.map_or_else(
                || Outcome::Unknown("latency measurement unavailable".into()),
                Outcome::LatencyMillis,
            );
            observations.push(
                Observation::new(
                    format!("{}:latency", job.job_id),
                    &node.id,
                    site,
                    Dimension::Latency,
                    outcome,
                    observations[0].observed_at,
                )
                .with_check_kind(job.check)
                .with_evidence(json!({"sourceJobId": job.job_id, "latencyMs": latency_ms})),
            );
        }
        observations
    }

    async fn check_primary(&self, node: &Node, job: &ScheduledJob, site: Site) -> Observation {
        self.check_primary_with_references(node, job, site, &[])
            .await
    }

    async fn check_primary_with_references(
        &self,
        node: &Node,
        job: &ScheduledJob,
        site: Site,
        reference_nodes: &[Node],
    ) -> Observation {
        let started = Instant::now();
        let result = self.execute(node, job.check, reference_nodes).await;
        let elapsed_ms = duration_millis(started.elapsed());
        let direct_reachable = direct_reachability_for_result(job.check, &result);
        let (dimension, outcome, mut evidence) = match result {
            Ok(result) => result,
            Err(error) => (
                dimension_for(job.check),
                outcome_for_error(job.check, &error),
                json!({}),
            ),
        };
        if let Some(object) = evidence.as_object_mut() {
            object.insert("executionElapsedMs".into(), json!(elapsed_ms));
            if let Some(direct_reachable) = direct_reachable {
                object.insert("directReachable".into(), json!(direct_reachable));
            }
        }
        Observation::new(&job.job_id, &node.id, site, dimension, outcome, Utc::now())
            .with_check_kind(job.check)
            .with_evidence(evidence)
    }

    async fn execute(
        &self,
        node: &Node,
        check: CheckKind,
        reference_nodes: &[Node],
    ) -> Result<(Dimension, Outcome, Value), ProbeError> {
        let Some(path) = request_path(node, check) else {
            return Ok((
                dimension_for(check),
                Outcome::Unsupported(format!(
                    "{check:?} is unsupported for {} {:?}",
                    node.network.as_str(),
                    node.kind
                )),
                json!({}),
            ));
        };
        if path.starts_with('/') && (node.rest_scheme.is_none() || node.rest_port.is_none()) {
            return Ok((
                dimension_for(check),
                Outcome::Unsupported("node has no REST endpoint".into()),
                json!({}),
            ));
        }
        match check {
            CheckKind::PeerTcp => {
                let report = self.peer_tcp(node, Duration::from_secs(3), true).await?;
                Ok((Dimension::Connectivity, report.outcome, report.evidence))
            }
            CheckKind::Availability => self.availability(node).await,
            CheckKind::Chain => self.chain(node, Dimension::Sync, reference_nodes).await,
            CheckKind::Finalization => {
                self.chain(node, Dimension::Finalization, reference_nodes)
                    .await
            }
            CheckKind::Peers => self.peers(node).await,
            CheckKind::StateHash => self.state_hash(node, reference_nodes).await,
            CheckKind::Harvest => {
                let value = self
                    .get_node_json(node, "/node/unlockedaccount", Duration::from_secs(5))
                    .await?;
                let count = parse_harvester_count(&value)
                    .map_err(|error| ProbeError::Payload(error.to_string()))?;
                Ok((
                    Dimension::Harvest,
                    Outcome::Healthy,
                    json!({"delegatedHarvesterCount": count}),
                ))
            }
            CheckKind::Identity => {
                let identity = match node.network {
                    Network::SymbolMainnet => {
                        let node_info = self
                            .get_node_json(node, "/node/info", Duration::from_secs(5))
                            .await?;
                        let server_info = self
                            .get_node_json(node, "/node/server", Duration::from_secs(5))
                            .await?;
                        parse_symbol_identity(&node_info, &server_info)
                    }
                    Network::Nis1Mainnet => {
                        let extended = self
                            .get_node_json(node, "/node/extended-info", Duration::from_secs(5))
                            .await?;
                        parse_nis1_identity(&extended)
                    }
                }
                .map_err(|error| ProbeError::Payload(error.to_string()))?;
                Ok((
                    Dimension::Connectivity,
                    Outcome::Healthy,
                    json!({"identity": identity}),
                ))
            }
            CheckKind::Certificate => {
                let certificate = self
                    .http
                    .certificate_info(node_url(node, "/")?, Duration::from_secs(5))
                    .await?;
                let remaining_days = certificate_remaining_days(certificate.not_after, Utc::now());
                Ok((
                    Dimension::Certificate,
                    Outcome::Remaining(remaining_days),
                    json!({
                        "notAfter": certificate.not_after,
                        "remainingDays": remaining_days
                    }),
                ))
            }
            CheckKind::Voting => self.voting(node, reference_nodes).await,
            CheckKind::Supernode => self.supernode(node, reference_nodes).await,
            CheckKind::WebSocket => {
                let url = websocket_url(node, "/ws")?;
                self.http
                    .websocket_handshake(url, Duration::from_secs(5))
                    .await?;
                Ok((
                    Dimension::Connectivity,
                    Outcome::Healthy,
                    json!({"scheme": node.rest_scheme, "handshake": "upgraded"}),
                ))
            }
        }
    }

    async fn availability(&self, node: &Node) -> Result<(Dimension, Outcome, Value), ProbeError> {
        match node.network {
            Network::SymbolMainnet => {
                let response = self
                    .get_node_json_timed(node, "/node/health", Duration::from_secs(5))
                    .await?;
                let (api, database, healthy) = parse_symbol_health(&response.value)
                    .map_err(|error| ProbeError::Payload(error.to_string()))?;
                let outcome = if healthy {
                    Outcome::Healthy
                } else {
                    Outcome::Failure("REST health is down".into())
                };
                Ok((
                    Dimension::Connectivity,
                    outcome,
                    json!({
                        "apiStatus": api,
                        "dbStatus": database,
                        "latencyMs": response.latency_ms
                    }),
                ))
            }
            Network::Nis1Mainnet => {
                let heartbeat = self
                    .get_node_json_timed(node, "/heartbeat", Duration::from_secs(5))
                    .await?;
                let status = self
                    .get_node_json_timed(node, "/status", Duration::from_secs(5))
                    .await?;
                let (heartbeat_code, status_code, healthy) =
                    parse_nis1_health(&heartbeat.value, &status.value)
                        .map_err(|error| ProbeError::Payload(error.to_string()))?;
                let outcome = if healthy {
                    Outcome::Healthy
                } else {
                    Outcome::Failure("NIS1 heartbeat/status is unhealthy".into())
                };
                Ok((
                    Dimension::Connectivity,
                    outcome,
                    json!({
                        "heartbeatCode": heartbeat_code,
                        "statusCode": status_code,
                        "latencyMs": heartbeat.latency_ms.saturating_add(status.latency_ms)
                    }),
                ))
            }
        }
    }

    async fn chain(
        &self,
        node: &Node,
        dimension: Dimension,
        reference_nodes: &[Node],
    ) -> Result<(Dimension, Outcome, Value), ProbeError> {
        match node.network {
            Network::SymbolMainnet => {
                let snapshot = self.cached_symbol_chain(node).await.ok_or_else(|| {
                    ProbeError::Payload("valid Symbol chain sample unavailable".into())
                })?;
                let mut evidence = serde_json::to_value(&snapshot).unwrap_or(Value::Null);
                let reference_snapshots = self.reference_chain_snapshots(reference_nodes).await;
                let expected = reference_nodes.len();
                let responding = reference_snapshots.len();
                if let Some(object) = evidence.as_object_mut() {
                    object.insert("referenceExpected".into(), json!(expected));
                    object.insert("referenceResponding".into(), json!(responding));
                    object.insert(
                        "referenceHeights".into(),
                        json!(reference_snapshots
                            .iter()
                            .map(|(_, r)| r.height)
                            .collect::<Vec<_>>()),
                    );
                    object.insert(
                        "referenceHosts".into(),
                        json!(reference_snapshots
                            .iter()
                            .map(|(n, _)| n.host.clone())
                            .collect::<Vec<_>>()),
                    );
                }
                let outcome = match dimension {
                    Dimension::Sync => {
                        let mut heights = reference_snapshots
                            .iter()
                            .map(|(_, reference)| reference.height)
                            .collect::<Vec<_>>();
                        heights.sort_unstable();
                        if expected > 0 && responding.saturating_mul(2) > expected {
                            let reference_height = median_u64(&heights);
                            if let Some(object) = evidence.as_object_mut() {
                                object.insert(
                                    "referenceMedianHeight".into(),
                                    json!(reference_height),
                                );
                            }
                            Outcome::LagBlocks(reference_height.saturating_sub(snapshot.height))
                        } else {
                            Outcome::Unknown(
                                "strict majority reference chain sample unavailable".into(),
                            )
                        }
                    }
                    Dimension::Finalization => {
                        if expected == 0 || responding.saturating_mul(2) <= expected {
                            Outcome::Unknown(
                                "strict majority finalization reference sample unavailable".into(),
                            )
                        } else {
                            let mut finalized_heights = reference_snapshots
                                .iter()
                                .filter_map(|(_, reference)| reference.finalized_height)
                                .collect::<Vec<_>>();
                            finalized_heights.sort_unstable();
                            let reference_finalized_height = median_u64(&finalized_heights);
                            if let Some(object) = evidence.as_object_mut() {
                                object.insert(
                                    "referenceMedianFinalizedHeight".into(),
                                    json!(reference_finalized_height),
                                );
                            }
                            let Some(history) = self.finalization_history.as_ref() else {
                                return Ok((
                                    dimension,
                                    Outcome::Unknown("finalization history unavailable".into()),
                                    evidence,
                                ));
                            };
                            let observed_at = Utc::now();
                            let mut usable_stalled_minutes = Vec::new();
                            for (reference, reference_snapshot) in &reference_snapshots {
                                let Some(finalized_height) = reference_snapshot.finalized_height
                                else {
                                    continue;
                                };
                                let Ok(history_key) = reference_history_key(reference) else {
                                    continue;
                                };
                                let Ok(assessment) = history
                                    .evaluate(
                                        &history_key,
                                        FinalizationSample {
                                            chain_height: reference_snapshot.height,
                                            finalized_height,
                                            // A network-wide stall cannot use the reference
                                            // finalized median as its own progress witness: when
                                            // every reference stalls, that median also stops.
                                            // Advancing chain height is the independent witness.
                                            reference_finalized_height: reference_snapshot.height,
                                        },
                                        observed_at,
                                    )
                                    .await
                                else {
                                    continue;
                                };
                                match assessment {
                                    FinalizationAssessment::InsufficientHistory => {}
                                    FinalizationAssessment::Progressing => {
                                        usable_stalled_minutes.push(0)
                                    }
                                    FinalizationAssessment::StalledMinutes(minutes) => {
                                        usable_stalled_minutes.push(minutes)
                                    }
                                }
                            }
                            let usable = usable_stalled_minutes.len();
                            let stalled = usable_stalled_minutes
                                .iter()
                                .filter(|minutes| **minutes > 0)
                                .count();
                            usable_stalled_minutes.sort_unstable_by(|left, right| right.cmp(left));
                            let stalled_minutes = if stalled > expected / 2 {
                                usable_stalled_minutes
                                    .get(expected / 2)
                                    .copied()
                                    .unwrap_or(0)
                            } else {
                                0
                            };
                            if let Some(object) = evidence.as_object_mut() {
                                object.insert(
                                    "networkFinalization".into(),
                                    json!({
                                        "expectedReferences": expected,
                                        "usableReferences": usable,
                                        "stalledReferences": stalled,
                                        "stalledMinutes": stalled_minutes
                                    }),
                                );
                            }
                            let assessment = history
                                .evaluate(
                                    &node.id,
                                    FinalizationSample {
                                        chain_height: snapshot.height,
                                        finalized_height: snapshot.finalized_height.expect(
                                            "validated Symbol snapshot has finalized height",
                                        ),
                                        reference_finalized_height,
                                    },
                                    observed_at,
                                )
                                .await
                                .map_err(|error| ProbeError::Payload(error.to_string()))?;
                            match assessment {
                                FinalizationAssessment::InsufficientHistory => Outcome::Unknown(
                                    "insufficient finalization progress history".into(),
                                ),
                                FinalizationAssessment::Progressing => Outcome::Healthy,
                                FinalizationAssessment::StalledMinutes(minutes) => {
                                    Outcome::FinalizationStalledMinutes(minutes)
                                }
                            }
                        }
                    }
                    _ => Outcome::Unknown("reference sample unavailable".into()),
                };
                Ok((dimension, outcome, evidence))
            }
            Network::Nis1Mainnet => {
                let height = self
                    .get_node_json(node, "/chain/height", Duration::from_secs(5))
                    .await?;
                let network_height = self
                    .get_node_json(
                        node,
                        "/node/active-peers/max-chain-height",
                        Duration::from_secs(5),
                    )
                    .await
                    .ok();
                let score = self
                    .get_node_json(node, "/chain/score", Duration::from_secs(5))
                    .await
                    .ok();
                let snapshot = parse_nis1_chain(&height, network_height.as_ref(), score.as_ref())
                    .map_err(|error| ProbeError::Payload(error.to_string()))?;
                let lag = snapshot
                    .network_height
                    .expect("validated NIS1 snapshot contains network height")
                    .saturating_sub(snapshot.height);
                Ok((
                    dimension,
                    Outcome::LagBlocks(lag),
                    serde_json::to_value(snapshot).unwrap_or(Value::Null),
                ))
            }
        }
    }

    async fn reference_chain_snapshots<'a>(
        &self,
        reference_nodes: &'a [Node],
    ) -> Vec<(&'a Node, ChainSnapshot)> {
        let mut snapshots = Vec::with_capacity(reference_nodes.len());
        for reference in reference_nodes {
            if let Some(snapshot) = self.cached_reference_symbol_chain(reference).await {
                snapshots.push((reference, snapshot));
            }
        }
        snapshots
    }

    async fn cached_symbol_chain(&self, node: &Node) -> Option<ChainSnapshot> {
        self.cached_chain(
            node,
            &self.reference_cache.local_chains,
            MAX_CACHED_LOCAL_CHAINS,
            LOCAL_CHAIN_TTL,
        )
        .await
    }

    async fn cached_reference_symbol_chain(&self, node: &Node) -> Option<ChainSnapshot> {
        self.cached_chain(
            node,
            &self.reference_cache.reference_chains,
            MAX_CACHED_REFERENCE_CHAINS,
            REFERENCE_CHAIN_TTL,
        )
        .await
    }

    async fn cached_chain(
        &self,
        node: &Node,
        cache: &Mutex<BTreeMap<String, ReferenceCacheEntry<ChainSnapshot>>>,
        max_entries: usize,
        ttl: Duration,
    ) -> Option<ChainSnapshot> {
        let key = node_url(node, "/chain/info").ok()?.to_string();
        let entry = reference_cache_entry(cache, key, max_entries).await;
        let now = Instant::now();
        let mut cached = entry.lock().await;
        if let Some(cached) = cached.as_ref().filter(|cached| cached.expires_at > now) {
            return cached.value.clone();
        }
        let value = self
            .get_node_json(node, "/chain/info", Duration::from_secs(5))
            .await
            .ok()
            .and_then(|value| parse_symbol_chain(&value).ok());
        *cached = Some(CachedReferenceSample {
            value: value.clone(),
            expires_at: Instant::now() + ttl,
        });
        value
    }

    async fn cached_reference_state_hash(&self, reference: &Node, height: u64) -> Option<String> {
        let path = format!("/blocks/{height}");
        let key = node_url(reference, &path).ok()?.to_string();
        let entry = reference_cache_entry(
            &self.reference_cache.state_hashes,
            key,
            MAX_CACHED_REFERENCE_HASHES,
        )
        .await;
        let now = Instant::now();
        let mut cached = entry.lock().await;
        if let Some(cached) = cached.as_ref().filter(|cached| cached.expires_at > now) {
            return cached.value.clone();
        }
        let value = self
            .get_node_json(reference, &path, Duration::from_secs(5))
            .await
            .ok()
            .and_then(|value| parse_symbol_state_hash(&value).ok());
        *cached = Some(CachedReferenceSample {
            value: value.clone(),
            expires_at: Instant::now() + REFERENCE_HASH_TTL,
        });
        value
    }

    async fn peers(&self, node: &Node) -> Result<(Dimension, Outcome, Value), ProbeError> {
        let path = if node.network == Network::SymbolMainnet {
            "/node/peers"
        } else {
            "/node/peer-list/all"
        };
        let value = self
            .get_node_json(node, path, Duration::from_secs(5))
            .await?;
        let count = parse_peer_count(node.network, &value)
            .map_err(|error| ProbeError::Payload(error.to_string()))?;
        let outcome = if count == 0 {
            Outcome::ZeroPeers
        } else {
            Outcome::Healthy
        };
        Ok((Dimension::Peers, outcome, json!({"peerCount": count})))
    }

    /// Guardian confirmations always fetch the source observation's fixed block height.
    pub async fn guardian_state_hash(
        &self,
        node: &Node,
        job_id: &str,
        site: Site,
        references: &[Node],
        height: u64,
    ) -> Observation {
        let result = async {
            let block=self.get_node_json(node,&format!("/blocks/{height}"),Duration::from_secs(5)).await?;
            let hash=parse_symbol_state_hash(&block).map_err(|e|ProbeError::Payload(e.to_string()))?;
            let mut counts=BTreeMap::<String,usize>::new();
            let mut hosts=std::collections::HashSet::new();
            for reference in references {
                if reference.host==node.host || !hosts.insert(&reference.host) {continue;}
                if let Some(hash)=self.cached_reference_state_hash(reference,height).await {*counts.entry(hash).or_default()+=1;}
            }
            let consensus=counts.iter().max_by_key(|(_,count)|**count).filter(|(_,count)|**count*2>references.len());
            let outcome=match consensus {Some((other,_)) if *other==hash=>Outcome::Healthy,Some((other,_))=>Outcome::Mismatch(format!("{hash} != {other}")),None=>Outcome::Unknown("strict majority reference stateHash unavailable".into())};
            Ok::<_,ProbeError>((outcome,json!({"comparisonHeight":height,"stateHash":hash,"referenceExpected":references.len(),"referenceConsensusHash":consensus.map(|(h,_)|h),"referenceConsensusCount":consensus.map_or(0,|(_,n)|*n)})))
        }.await;
        let (outcome, evidence) = result.unwrap_or_else(|e| {
            (
                outcome_for_error(CheckKind::StateHash, &e),
                json!({"comparisonHeight":height}),
            )
        });
        Observation::new(
            job_id,
            &node.id,
            site,
            Dimension::StateHash,
            outcome,
            Utc::now(),
        )
        .with_check_kind(CheckKind::StateHash)
        .with_evidence(evidence)
    }

    async fn state_hash(
        &self,
        node: &Node,
        reference_nodes: &[Node],
    ) -> Result<(Dimension, Outcome, Value), ProbeError> {
        let snapshot = self
            .cached_symbol_chain(node)
            .await
            .ok_or_else(|| ProbeError::Payload("valid Symbol chain sample unavailable".into()))?;

        if reference_nodes.is_empty() {
            let block = self
                .get_node_json(
                    node,
                    &format!("/blocks/{}", snapshot.height),
                    Duration::from_secs(5),
                )
                .await?;
            let state_hash = parse_symbol_state_hash(&block)
                .map_err(|error| ProbeError::Payload(error.to_string()))?;
            return Ok((
                Dimension::StateHash,
                Outcome::Unknown("reference stateHash unavailable".into()),
                json!({"height": snapshot.height, "stateHash": state_hash}),
            ));
        }

        let expected = reference_nodes.len();
        let mut responding = Vec::with_capacity(expected);
        for reference in reference_nodes {
            if let Some(reference_snapshot) = self.cached_reference_symbol_chain(reference).await {
                responding.push((reference, reference_snapshot));
            }
        }
        if responding.len().saturating_mul(2) <= expected {
            return Ok((
                Dimension::StateHash,
                Outcome::Unknown("strict majority reference chain sample unavailable".into()),
                json!({
                    "referenceExpected": expected,
                    "referenceResponding": responding.len()
                }),
            ));
        }

        let Some(local_finalized_height) = snapshot.finalized_height else {
            return Ok((
                Dimension::StateHash,
                Outcome::Unknown("local finalized height unavailable".into()),
                json!({
                    "localHeight": snapshot.height,
                    "referenceExpected": expected,
                    "referenceResponding": responding.len()
                }),
            ));
        };
        let reference_finalized_heights = responding
            .iter()
            .filter_map(|(_, reference)| reference.finalized_height)
            .collect::<Vec<_>>();
        if reference_finalized_heights.len() != responding.len() {
            return Ok((
                Dimension::StateHash,
                Outcome::Unknown("reference finalized height unavailable".into()),
                json!({
                    "localHeight": snapshot.height,
                    "localFinalizedHeight": local_finalized_height,
                    "referenceExpected": expected,
                    "referenceResponding": responding.len()
                }),
            ));
        }
        let reference_minimum_finalized_height = reference_finalized_heights
            .iter()
            .copied()
            .min()
            .expect("responding strict majority is non-empty");
        let comparison_height = reference_minimum_finalized_height;
        if local_finalized_height < comparison_height {
            return Ok((
                Dimension::StateHash,
                Outcome::Unknown("local finalization is below reference comparison height".into()),
                json!({
                    "localHeight": snapshot.height,
                    "localFinalizedHeight": local_finalized_height,
                    "comparisonHeight": comparison_height,
                    "referenceExpected": expected,
                    "referenceResponding": responding.len(),
                    "referenceMinimumFinalizedHeight": reference_minimum_finalized_height
                }),
            ));
        }
        let block = self
            .get_node_json(
                node,
                &format!("/blocks/{comparison_height}"),
                Duration::from_secs(5),
            )
            .await?;
        let local_state_hash = parse_symbol_state_hash(&block)
            .map_err(|error| ProbeError::Payload(error.to_string()))?;

        let mut hash_counts = BTreeMap::<String, usize>::new();
        for (reference, _) in &responding {
            if let Some(reference_hash) = self
                .cached_reference_state_hash(reference, comparison_height)
                .await
            {
                *hash_counts.entry(reference_hash).or_default() += 1;
            }
        }
        let consensus = hash_counts
            .iter()
            .max_by(|left, right| left.1.cmp(right.1).then_with(|| right.0.cmp(left.0)));
        let (outcome, consensus_hash, consensus_count) = match consensus {
            Some((hash, count)) if count.saturating_mul(2) > expected => {
                let outcome = if local_state_hash == *hash {
                    Outcome::Healthy
                } else {
                    Outcome::Mismatch(format!("{local_state_hash} != {hash}"))
                };
                (outcome, Some(hash.as_str()), *count)
            }
            _ => (
                Outcome::Unknown("strict majority reference stateHash unavailable".into()),
                None,
                consensus.map_or(0, |(_, count)| *count),
            ),
        };
        Ok((
            Dimension::StateHash,
            outcome,
            json!({
                "localHeight": snapshot.height,
                "localFinalizedHeight": local_finalized_height,
                "comparisonHeight": comparison_height,
                "stateHash": local_state_hash,
                "referenceExpected": expected,
                "referenceResponding": responding.len(),
                "referenceMinimumFinalizedHeight": reference_minimum_finalized_height,
                "referenceConsensusHash": consensus_hash,
                "referenceConsensusCount": consensus_count
            }),
        ))
    }

    async fn voting(
        &self,
        node: &Node,
        reference_nodes: &[Node],
    ) -> Result<(Dimension, Outcome, Value), ProbeError> {
        let Some(public_key) = node.public_key.as_deref() else {
            return Ok((
                Dimension::Voting,
                Outcome::Unknown("node public key unavailable".into()),
                json!({}),
            ));
        };
        let mut sources = Vec::with_capacity(reference_nodes.len().saturating_add(2));
        if node.rest_port.is_some() {
            sources.push(node);
        } else if let Some(reference) = self.symbol_reference.as_ref() {
            sources.push(reference);
        }
        for reference in reference_nodes {
            if !sources.iter().any(|source| {
                source.host == reference.host
                    && source.rest_scheme == reference.rest_scheme
                    && source.rest_port == reference.rest_port
            }) {
                sources.push(reference);
            }
        }
        if sources.is_empty() {
            return Ok((
                Dimension::Voting,
                Outcome::Unknown("reference API unavailable".into()),
                json!({}),
            ));
        }

        for source in &sources {
            let Ok(chain) = self
                .get_node_json(source, "/chain/info", Duration::from_secs(5))
                .await
            else {
                continue;
            };
            let Some(current_epoch) = chain
                .get("latestFinalizedBlock")
                .and_then(|block| block.get("finalizationEpoch"))
                .and_then(|value| {
                    value
                        .as_i64()
                        .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
                })
            else {
                continue;
            };
            let Ok(account) = self
                .get_node_json(
                    source,
                    &format!("/accounts/{public_key}"),
                    Duration::from_secs(5),
                )
                .await
            else {
                continue;
            };
            let Ok(Some((start, end, remaining))) = parse_voting_remaining(&account, current_epoch)
            else {
                continue;
            };
            return Ok((
                Dimension::Voting,
                Outcome::Remaining(remaining),
                json!({
                    "startEpoch": start,
                    "endEpoch": end,
                    "finalizationEpoch": current_epoch,
                    "remainingEpochs": remaining,
                    "referenceHost": source.host
                }),
            ));
        }

        Ok((
            Dimension::Voting,
            Outcome::Unknown("valid Voting reference response unavailable".into()),
            json!({"referenceAttempts": sources.len()}),
        ))
    }

    async fn supernode(
        &self,
        node: &Node,
        aliases: &[Node],
    ) -> Result<(Dimension, Outcome, Value), ProbeError> {
        let mut registrations_url = Url::parse(SUPER_NODE_API_BASE)
            .and_then(|base| base.join("nodes"))
            .map_err(|error| ProbeError::InvalidEndpoint(error.to_string()))?;
        registrations_url
            .query_pairs_mut()
            .append_pair("count", "1000")
            .append_pair("offset", "0");
        let registrations = self
            .http
            .get_json_same_origin(registrations_url, Duration::from_secs(3))
            .await?;
        let mut authorities = Vec::with_capacity(aliases.len().saturating_add(1));
        authorities.push((node.host.as_str(), node.p2p_port));
        authorities.extend(
            aliases
                .iter()
                .map(|alias| (alias.host.as_str(), alias.p2p_port)),
        );
        let registration = parse_supernode_registration(&registrations, &authorities)
            .map_err(|error| ProbeError::Payload(error.to_string()))?;
        let Some(registration) = registration else {
            // The provider may cap count=1000 to a 100-row page. An absent
            // target on a full first page does not prove non-registration.
            let incomplete = registrations
                .as_array()
                .is_some_and(|rows| rows.len() >= 100);
            return Ok((
                Dimension::Supernode,
                if incomplete {
                    Outcome::Unknown("NIS1 Supernode registry page may be incomplete".into())
                } else {
                    Outcome::Unsupported(
                        "node is not registered in the NIS1 Supernode program".into(),
                    )
                },
                json!({"provider": "nem.io", "registration": if incomplete {"incomplete_page"} else {"missing"}}),
            ));
        };
        if !registration.active {
            return Ok((
                Dimension::Supernode,
                Outcome::Unsupported("NIS1 Supernode registration is not active".into()),
                json!({
                    "provider": "nem.io",
                    "registration": registration.status,
                    "supernodeId": registration.id
                }),
            ));
        }

        let base = Url::parse(SUPER_NODE_API_BASE)
            .map_err(|error| ProbeError::InvalidEndpoint(error.to_string()))?;
        let info_url = base
            .join(&format!("node/{}", registration.id))
            .map_err(|error| ProbeError::InvalidEndpoint(error.to_string()))?;
        let info = self
            .http
            .get_json_same_origin(info_url, Duration::from_secs(3))
            .await?;
        let round_id = parse_supernode_info(
            &info,
            registration.id,
            &registration.host,
            registration.port,
        )
        .map_err(|error| ProbeError::Payload(error.to_string()))?;
        let result_url = base
            .join(&format!(
                "node/{}/results/rounds/{round_id}/to/{round_id}",
                registration.id
            ))
            .map_err(|error| ProbeError::InvalidEndpoint(error.to_string()))?;
        let result = self
            .http
            .get_json_same_origin(result_url, Duration::from_secs(3))
            .await?;
        let result = parse_supernode_round(&result, round_id, Utc::now())
            .map_err(|error| ProbeError::Payload(error.to_string()))?;
        let outcome = if result.passed {
            Outcome::Healthy
        } else {
            Outcome::Failure("latest NIS1 Supernode round failed".into())
        };
        Ok((
            Dimension::Supernode,
            outcome,
            json!({
                "provider": "nem.io",
                "supernodeId": registration.id,
                "roundId": result.round_id,
                "roundTimestamp": result.timestamp,
                "passed": result.passed,
                "failedTests": result.failed_tests
            }),
        ))
    }

    async fn peer_tcp(
        &self,
        node: &Node,
        total_budget: Duration,
        use_normal_rate_limit: bool,
    ) -> Result<PeerTcpReport, ProbeError> {
        let budget_started = Instant::now();
        let mut deadline = budget_started + total_budget;
        let dns_started = Instant::now();
        let addresses = match tokio::time::timeout(
            total_budget,
            self.peer_tcp_transport
                .resolve(&node.host, node.p2p_port, total_budget),
        )
        .await
        {
            Ok(Ok(addresses)) => addresses,
            Ok(Err(ProbeError::Dns(message))) => {
                let dns_duration_ms = duration_millis(dns_started.elapsed());
                return Ok(PeerTcpReport {
                    outcome: Outcome::Failure(format!("peer TCP DNS lookup failed: {message}")),
                    evidence: json!({
                        "transport": "p2p",
                        "latencyMs": null,
                        "totalBudgetMs": duration_millis(total_budget),
                        "rateLimitWaitMs": 0,
                        "dns": {
                            "status": "failed",
                            "durationMs": dns_duration_ms,
                            "addresses": [],
                            "error": message,
                        },
                        "attempts": [],
                    }),
                });
            }
            Ok(Err(error)) => return Err(error),
            Err(_) => {
                return Ok(PeerTcpReport {
                    outcome: Outcome::Failure(
                        "peer TCP DNS lookup failed: lookup timed out".into(),
                    ),
                    evidence: json!({
                        "transport": "p2p",
                        "latencyMs": null,
                        "totalBudgetMs": duration_millis(total_budget),
                        "rateLimitWaitMs": 0,
                        "dns": {
                            "status": "failed",
                            "durationMs": duration_millis(dns_started.elapsed()),
                            "addresses": [],
                            "error": "lookup timed out",
                        },
                        "attempts": [],
                    }),
                });
            }
        };
        let dns_duration_ms = duration_millis(dns_started.elapsed());
        let mut rate_limit_wait = Duration::ZERO;
        if use_normal_rate_limit && !self.community_budgeted {
            // Each outbound attempt retains its rate token, while queue waiting extends
            // the deadline so it cannot be misreported as TCP failure time.
            for _ in &addresses {
                let wait_started = Instant::now();
                self.outbound_rate.acquire().await;
                let waited = wait_started.elapsed();
                rate_limit_wait = rate_limit_wait.saturating_add(waited);
                deadline += waited;
            }
        }
        let connect_budget = deadline.saturating_duration_since(Instant::now());
        let attempts = if use_normal_rate_limit && self.community_budgeted {
            let mut attempts = Vec::new();
            for address in &addresses {
                let wait_started = Instant::now();
                self.outbound_rate.acquire().await;
                let waited = wait_started.elapsed();
                rate_limit_wait = rate_limit_wait.saturating_add(waited);
                deadline += waited;
                let remaining = deadline.saturating_duration_since(Instant::now());
                let address = SocketAddr::new(*address, node.p2p_port);
                let attempt = tokio::time::timeout(
                    remaining,
                    self.peer_tcp_transport.connect(address, remaining),
                )
                .await
                .unwrap_or(TcpConnectAttempt {
                    address,
                    elapsed_ms: duration_millis(remaining),
                    result: TcpConnectResult::Timeout,
                });
                attempts.push(attempt);
            }
            attempts
        } else if connect_budget.is_zero() {
            addresses
                .iter()
                .map(|address| TcpConnectAttempt {
                    address: SocketAddr::new(*address, node.p2p_port),
                    elapsed_ms: 0,
                    result: TcpConnectResult::Timeout,
                })
                .collect::<Vec<_>>()
        } else {
            match tokio::time::timeout(
                connect_budget,
                join_all(addresses.iter().map(|address| {
                    self.peer_tcp_transport
                        .connect(SocketAddr::new(*address, node.p2p_port), connect_budget)
                })),
            )
            .await
            {
                Ok(attempts) => attempts,
                Err(_) => addresses
                    .iter()
                    .map(|address| TcpConnectAttempt {
                        address: SocketAddr::new(*address, node.p2p_port),
                        elapsed_ms: duration_millis(connect_budget),
                        result: TcpConnectResult::Timeout,
                    })
                    .collect(),
            }
        };
        let connected = attempts
            .iter()
            .filter(|attempt| attempt.result == TcpConnectResult::Connected)
            .min_by_key(|attempt| attempt.elapsed_ms);
        let attempt_evidence = attempts
            .iter()
            .map(|attempt| {
                json!({
                    "address": attempt.address.to_string(),
                    "family": if attempt.address.is_ipv4() { "ipv4" } else { "ipv6" },
                    "result": attempt.result.wire_name(),
                    "durationMs": attempt.elapsed_ms,
                })
            })
            .collect::<Vec<_>>();
        let address_evidence = addresses
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        let latency_ms =
            connected.map(|attempt| dns_duration_ms.saturating_add(attempt.elapsed_ms));
        let outcome = connected.map_or_else(
            || {
                let representative = attempts
                    .iter()
                    .map(|attempt| attempt.result)
                    .find(|result| *result != TcpConnectResult::Other)
                    .unwrap_or(TcpConnectResult::Other);
                Outcome::Failure(format!(
                    "peer TCP connection failed: {} ({} address{})",
                    representative.description(),
                    attempts.len(),
                    if attempts.len() == 1 { "" } else { "es" }
                ))
            },
            |_| Outcome::Healthy,
        );
        Ok(PeerTcpReport {
            outcome,
            evidence: json!({
                "transport": "p2p",
                "latencyMs": latency_ms,
                "totalBudgetMs": duration_millis(total_budget),
                "connectBudgetMs": duration_millis(connect_budget),
                "rateLimitWaitMs": duration_millis(rate_limit_wait),
                "dns": {
                    "status": "resolved",
                    "durationMs": dns_duration_ms,
                    "addresses": address_evidence,
                },
                "attempts": attempt_evidence,
            }),
        })
    }

    pub async fn enrich_peer_tcp_diagnostic(
        &self,
        observation: &mut Observation,
        node: &Node,
        rest_node: Option<&Node>,
    ) {
        const DIAGNOSTIC_TIMEOUT: Duration = Duration::from_secs(10);
        let diagnostic = tokio::time::timeout(DIAGNOSTIC_TIMEOUT, async {
            let tcp = self.peer_tcp(node, DIAGNOSTIC_TIMEOUT, false);
            let rest = async {
                match rest_node {
                    Some(node) => Some(self.availability(node).await),
                    None => None,
                }
            };
            tokio::join!(tcp, rest)
        })
        .await;

        let (detail, summary) = match diagnostic {
            Ok((Ok(report), rest)) => {
                let rest = rest.map(|result| match result {
                    Ok((_, outcome, evidence)) => json!({
                        "outcome": outcome,
                        "evidence": evidence,
                    }),
                    Err(error) => json!({"error": error.to_string()}),
                });
                let summary = match &report.outcome {
                    Outcome::Healthy => "connected".to_owned(),
                    Outcome::Failure(message) => message.clone(),
                    outcome => format!("{outcome:?}"),
                };
                (
                    json!({
                        "status": "completed",
                        "budgetMs": duration_millis(DIAGNOSTIC_TIMEOUT),
                        "tcp": report.evidence,
                        "restCorrelation": rest,
                    }),
                    summary,
                )
            }
            Ok((Err(error), _)) => (
                json!({
                    "status": "error",
                    "budgetMs": duration_millis(DIAGNOSTIC_TIMEOUT),
                    "error": error.to_string(),
                }),
                error.to_string(),
            ),
            Err(_) => (
                json!({
                    "status": "timeout",
                    "budgetMs": duration_millis(DIAGNOSTIC_TIMEOUT),
                }),
                "timed out after 10 seconds".into(),
            ),
        };
        if let Some(object) = observation.evidence.as_object_mut() {
            object.insert("detailedDiagnostic".into(), detail);
        }
        if let Outcome::Failure(message) = &mut observation.outcome {
            *message = format!("{message}; diagnostic: {summary}");
        }
    }

    async fn get_node_json(
        &self,
        node: &Node,
        path: &str,
        timeout: Duration,
    ) -> Result<Value, ProbeError> {
        let url = node_url(node, path)?;
        self.http.get_json(url, timeout).await
    }

    async fn get_node_json_timed(
        &self,
        node: &Node,
        path: &str,
        timeout: Duration,
    ) -> Result<TimedJson, ProbeError> {
        let url = node_url(node, path)?;
        self.http.get_json_timed(url, timeout).await
    }
}

fn node_url(node: &Node, path: &str) -> Result<Url, ProbeError> {
    let mut url = Url::parse("http://localhost")
        .map_err(|error| ProbeError::InvalidEndpoint(error.to_string()))?;
    let scheme = node
        .rest_scheme
        .as_deref()
        .ok_or(ProbeError::NoRestEndpoint)?;
    if !matches!(scheme, "http" | "https") {
        return Err(ProbeError::InvalidEndpoint("unsupported scheme".into()));
    }
    url.set_scheme(scheme)
        .map_err(|()| ProbeError::InvalidEndpoint("scheme".into()))?;
    url.set_host(Some(&node.host))
        .map_err(|error| ProbeError::InvalidEndpoint(error.to_string()))?;
    let port = node.rest_port.ok_or(ProbeError::NoRestEndpoint)?;
    if node.network == Network::SymbolMainnet && port == 7900 {
        return Err(ProbeError::InvalidEndpoint(
            "Symbol P2P port cannot be used as a REST endpoint".into(),
        ));
    }
    url.set_port(Some(port))
        .map_err(|()| ProbeError::InvalidEndpoint("port".into()))?;
    url.set_path(path);
    Ok(url)
}

fn reference_history_key(node: &Node) -> Result<String, ProbeError> {
    Ok(node_url(node, "/")?.origin().ascii_serialization())
}

fn websocket_url(node: &Node, path: &str) -> Result<Url, ProbeError> {
    let mut url = Url::parse("ws://localhost")
        .map_err(|error| ProbeError::InvalidEndpoint(error.to_string()))?;
    let scheme = node
        .rest_scheme
        .as_deref()
        .ok_or(ProbeError::NoRestEndpoint)?;
    if !matches!(scheme, "ws" | "wss") {
        return Err(ProbeError::InvalidEndpoint(
            "unsupported WebSocket scheme".into(),
        ));
    }
    url.set_scheme(scheme)
        .map_err(|()| ProbeError::InvalidEndpoint("scheme".into()))?;
    url.set_host(Some(&node.host))
        .map_err(|error| ProbeError::InvalidEndpoint(error.to_string()))?;
    url.set_port(Some(node.rest_port.ok_or(ProbeError::NoRestEndpoint)?))
        .map_err(|()| ProbeError::InvalidEndpoint("port".into()))?;
    url.set_path(path);
    Ok(url)
}

#[async_trait]
impl ProbeHttpTransport for NetworkHttpTransport {
    async fn get_json(&self, url: Url, timeout: Duration) -> Result<Value, ProbeError> {
        self.get_json_timed(url, timeout)
            .await
            .map(|response| response.value)
    }

    async fn get_json_timed(&self, url: Url, timeout: Duration) -> Result<TimedJson, ProbeError> {
        if self.budget.is_some() {
            // Local rate waiting is not remote latency or a target timeout.
            return self.get_json_with_redirects(url, timeout, false).await;
        }
        with_request_deadline(timeout, self.get_json_with_redirects(url, timeout, false)).await
    }

    async fn get_json_same_origin(&self, url: Url, timeout: Duration) -> Result<Value, ProbeError> {
        if self.budget.is_some() {
            return self
                .get_json_with_redirects(url, timeout, true)
                .await
                .map(|v| v.value);
        }
        with_request_deadline(timeout, self.get_json_with_redirects(url, timeout, true))
            .await
            .map(|response| response.value)
    }

    async fn websocket_handshake(&self, url: Url, timeout: Duration) -> Result<(), ProbeError> {
        if self.budget.is_some() {
            return self.websocket_handshake_once(url, timeout).await;
        }
        with_request_deadline(timeout, self.websocket_handshake_once(url, timeout)).await
    }

    async fn certificate_info(
        &self,
        url: Url,
        timeout: Duration,
    ) -> Result<CertificateInfo, ProbeError> {
        if self.budget.is_some() {
            return self.certificate_info_once(url, timeout).await;
        }
        with_request_deadline(timeout, self.certificate_info_once(url, timeout)).await
    }
}

impl NetworkHttpTransport {
    async fn reserve_bytes(&self, bytes: u64) -> Result<Option<(String, u64)>, ProbeError> {
        match &self.budget {
            Some(budget) => Ok(Some((budget.reserve(bytes).await?, bytes))),
            None => Ok(None),
        }
    }
    async fn settle_bytes(
        &self,
        receipt: Option<(String, u64)>,
        used: u64,
    ) -> Result<(), ProbeError> {
        if let (Some(budget), Some((id, reserved))) = (&self.budget, receipt) {
            budget.settle(&id, reserved, used).await?;
        }
        Ok(())
    }

    async fn certificate_info_once(
        &self,
        url: Url,
        timeout: Duration,
    ) -> Result<CertificateInfo, ProbeError> {
        let receipt = self.reserve_bytes(1024 * 1024).await?;
        if url.scheme() != "https" || !url.username().is_empty() || url.password().is_some() {
            return Err(ProbeError::InvalidEndpoint(
                "certificate endpoint must be credential-free HTTPS".into(),
            ));
        }
        validate_probe_url(&url)?;
        let host = url
            .host_str()
            .ok_or_else(|| ProbeError::InvalidEndpoint("host missing".into()))?
            .to_owned();
        let port = url
            .port_or_known_default()
            .ok_or_else(|| ProbeError::InvalidEndpoint("port missing".into()))?;
        let addresses = match with_request_deadline(timeout, resolve_public(&host, port)).await {
            Ok(addresses) => addresses,
            Err(error) => {
                self.settle_bytes(receipt, 8192).await?;
                return Err(error);
            }
        };
        let pinned_addresses = addresses
            .iter()
            .copied()
            .map(|address| SocketAddr::new(address, port))
            .collect::<Vec<_>>();
        let server_name = ServerName::try_from(host.clone())
            .map_err(|error| ProbeError::InvalidEndpoint(error.to_string()))?;
        let mut roots = RootCertStore::empty();
        roots
            .roots
            .extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let connector = TlsConnector::from(Arc::new(
            ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth(),
        ));

        let mut last_error = None;
        for address in pinned_addresses {
            if let Some(budget) = &self.budget {
                budget.reserve_request(&host).await?;
            }
            self.rate_limiter.acquire().await;
            let tcp =
                match tokio::time::timeout(Duration::from_secs(3), TcpStream::connect(address))
                    .await
                {
                    Ok(Ok(stream)) => stream,
                    Ok(Err(error)) => {
                        last_error = Some(error.to_string());
                        continue;
                    }
                    Err(_) => {
                        last_error = Some("TLS TCP connect timed out".into());
                        continue;
                    }
                };
            if tcp
                .peer_addr()
                .map_err(|error| ProbeError::Request(error.to_string()))?
                != address
            {
                return Err(ProbeError::RemoteAddressMismatch);
            }
            let tls =
                match tokio::time::timeout(timeout, connector.connect(server_name.clone(), tcp))
                    .await
                {
                    Ok(Ok(stream)) => stream,
                    Ok(Err(error)) => {
                        last_error = Some(error.to_string());
                        continue;
                    }
                    Err(_) => {
                        last_error = Some("TLS handshake timed out".into());
                        continue;
                    }
                };
            let certificate = tls
                .get_ref()
                .1
                .peer_certificates()
                .and_then(|certificates| certificates.first())
                .ok_or_else(|| ProbeError::Payload("TLS peer certificate missing".into()))?;
            self.settle_bytes(receipt, certificate.as_ref().len() as u64 + 65536)
                .await?;
            return Ok(CertificateInfo {
                not_after: certificate_not_after(certificate.as_ref())?,
            });
        }
        Err(ProbeError::Request(
            last_error.unwrap_or_else(|| "TLS connection failed".into()),
        ))
    }

    async fn websocket_handshake_once(
        &self,
        url: Url,
        timeout: Duration,
    ) -> Result<(), ProbeError> {
        let receipt = self.reserve_bytes(128 * 1024).await?;
        let mut request_url = url.clone();
        let request_scheme = match url.scheme() {
            "ws" => "http",
            "wss" => "https",
            _ => {
                return Err(ProbeError::InvalidEndpoint(
                    "unsupported WebSocket scheme".into(),
                ))
            }
        };
        request_url
            .set_scheme(request_scheme)
            .map_err(|()| ProbeError::InvalidEndpoint("scheme".into()))?;
        validate_probe_url(&request_url)?;
        let host = request_url
            .host_str()
            .ok_or_else(|| ProbeError::InvalidEndpoint("host missing".into()))?
            .to_owned();
        let port = request_url
            .port_or_known_default()
            .ok_or_else(|| ProbeError::InvalidEndpoint("port missing".into()))?;
        let addresses = match with_request_deadline(timeout, resolve_public(&host, port)).await {
            Ok(addresses) => addresses,
            Err(error) => {
                self.settle_bytes(receipt, 8192).await?;
                return Err(error);
            }
        };
        let pinned_addresses = addresses
            .iter()
            .copied()
            .map(|address| SocketAddr::new(address, port))
            .collect::<Vec<_>>();
        let mut builder = reqwest::Client::builder()
            .redirect(Policy::none())
            .no_proxy()
            .connect_timeout(Duration::from_secs(3))
            .timeout(timeout)
            .user_agent(&self.user_agent);
        if host.parse::<IpAddr>().is_err() {
            builder = builder.resolve_to_addrs(&host, &pinned_addresses);
        }
        let nonce = Uuid::new_v4();
        let key = STANDARD.encode(nonce.as_bytes());
        let mut digest = Sha1::new();
        digest.update(key.as_bytes());
        digest.update(b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
        let expected_accept = STANDARD.encode(digest.finalize());

        if let Some(budget) = &self.budget {
            budget.reserve_request(&host).await?;
        }
        self.rate_limiter.acquire().await;
        let response = builder
            .build()
            .map_err(|error| ProbeError::Request(error.to_string()))?
            .get(request_url)
            .header(CONNECTION, "Upgrade")
            .header(UPGRADE, "websocket")
            .header(SEC_WEBSOCKET_VERSION, "13")
            .header(SEC_WEBSOCKET_KEY, key)
            .send()
            .await
            .map_err(|error| ProbeError::Request(error.to_string()))?;
        let remote_address = response
            .remote_addr()
            .ok_or(ProbeError::RemoteAddressMismatch)?;
        if !pinned_addresses.contains(&remote_address) {
            return Err(ProbeError::RemoteAddressMismatch);
        }
        if response.status() != StatusCode::SWITCHING_PROTOCOLS {
            return Err(ProbeError::Http(response.status()));
        }
        let upgrade = response
            .headers()
            .get(UPGRADE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.eq_ignore_ascii_case("websocket"));
        let connection = response
            .headers()
            .get(CONNECTION)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| {
                value
                    .split(',')
                    .any(|token| token.trim().eq_ignore_ascii_case("upgrade"))
            });
        let accept = response
            .headers()
            .get(SEC_WEBSOCKET_ACCEPT)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value == expected_accept);
        if !upgrade || !connection || !accept {
            return Err(ProbeError::Payload(
                "invalid WebSocket upgrade response".into(),
            ));
        }
        self.settle_bytes(receipt, 65536).await?;
        Ok(())
    }

    async fn get_json_with_redirects(
        &self,
        mut url: Url,
        timeout: Duration,
        same_origin: bool,
    ) -> Result<TimedJson, ProbeError> {
        const MAX_REDIRECTS: usize = 5;
        const MAX_RESPONSE_BYTES: usize = 4 * 1024 * 1024;

        let original_url = url.clone();
        let mut latency_ms = 0_u64;
        for redirect_count in 0..=MAX_REDIRECTS {
            self.rate_limiter.acquire().await;
            let hop_started = Instant::now();
            validate_probe_url(&url)?;
            if let Some(budget) = &self.budget {
                budget
                    .reserve_request(
                        url.host_str()
                            .ok_or_else(|| ProbeError::InvalidEndpoint("host missing".into()))?,
                    )
                    .await?;
            }
            let receipt = self
                .reserve_bytes(MAX_RESPONSE_BYTES as u64 + 131072)
                .await?;
            let host = url
                .host_str()
                .ok_or_else(|| ProbeError::InvalidEndpoint("host missing".into()))?
                .to_owned();
            let port = url
                .port_or_known_default()
                .ok_or_else(|| ProbeError::InvalidEndpoint("port missing".into()))?;
            let addresses = match with_request_deadline(timeout, resolve_public(&host, port)).await
            {
                Ok(addresses) => addresses,
                Err(error) => {
                    self.settle_bytes(receipt, 8192).await?;
                    return Err(error);
                }
            };
            let pinned_addresses = addresses
                .iter()
                .copied()
                .map(|address| SocketAddr::new(address, port))
                .collect::<Vec<_>>();
            let mut builder = reqwest::Client::builder()
                .redirect(Policy::none())
                .no_proxy()
                .connect_timeout(Duration::from_secs(3))
                .timeout(timeout)
                .user_agent(&self.user_agent);
            if host.parse::<IpAddr>().is_err() {
                builder = builder.resolve_to_addrs(&host, &pinned_addresses);
            }
            let response = builder
                .build()
                .map_err(|error| ProbeError::Request(error.to_string()))?
                .get(url.clone())
                .send()
                .await;
            let mut response = match response {
                Ok(response) => response,
                Err(error) => {
                    self.settle_bytes(receipt, 131072).await?;
                    return Err(ProbeError::Request(error.to_string()));
                }
            };
            let remote_address = response
                .remote_addr()
                .ok_or(ProbeError::RemoteAddressMismatch)?;
            if !pinned_addresses.contains(&remote_address) {
                return Err(ProbeError::RemoteAddressMismatch);
            }
            if response.status().is_redirection() {
                self.settle_bytes(receipt, 65536).await?;
                if redirect_count == MAX_REDIRECTS {
                    return Err(ProbeError::InvalidEndpoint("too many redirects".into()));
                }
                let location = response
                    .headers()
                    .get(LOCATION)
                    .and_then(|value| value.to_str().ok())
                    .ok_or_else(|| {
                        ProbeError::InvalidEndpoint("redirect without Location".into())
                    })?;
                let redirect_url = url
                    .join(location)
                    .map_err(|error| ProbeError::InvalidEndpoint(error.to_string()))?;
                if same_origin {
                    validate_same_origin_redirect(&original_url, &redirect_url)?;
                }
                url = redirect_url;
                latency_ms = latency_ms.saturating_add(duration_millis(hop_started.elapsed()));
                continue;
            }
            if !response.status().is_success() {
                self.settle_bytes(receipt, 65536).await?;
                return Err(ProbeError::Http(response.status()));
            }
            let mut body = Vec::new();
            while let Some(chunk) = response
                .chunk()
                .await
                .map_err(|error| ProbeError::Request(error.to_string()))?
            {
                append_bounded(&mut body, &chunk, MAX_RESPONSE_BYTES)?;
            }
            latency_ms = latency_ms.saturating_add(duration_millis(hop_started.elapsed()));
            self.settle_bytes(receipt, body.len() as u64 + 65536)
                .await?;
            let value = serde_json::from_slice(&body)
                .map_err(|error| ProbeError::Payload(error.to_string()))?;
            return Ok(TimedJson { value, latency_ms });
        }
        Err(ProbeError::InvalidEndpoint("too many redirects".into()))
    }
}

fn validate_same_origin_redirect(origin: &Url, redirect: &Url) -> Result<(), ProbeError> {
    if origin.origin() != redirect.origin() {
        return Err(ProbeError::InvalidEndpoint(
            "redirect changed the fixed provider origin".into(),
        ));
    }
    Ok(())
}

async fn reference_cache_entry<T>(
    cache: &Mutex<BTreeMap<String, ReferenceCacheEntry<T>>>,
    key: String,
    max_entries: usize,
) -> ReferenceCacheEntry<T> {
    let mut cache = cache.lock().await;
    if let Some(entry) = cache.get(&key) {
        return Arc::clone(entry);
    }
    while cache.len() >= max_entries {
        let Some(first) = cache.keys().next().cloned() else {
            break;
        };
        cache.remove(&first);
    }
    let entry = Arc::new(Mutex::new(None));
    cache.insert(key, Arc::clone(&entry));
    entry
}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn median_u64(sorted: &[u64]) -> u64 {
    let middle = sorted.len() / 2;
    if sorted.len() % 2 == 1 {
        sorted[middle]
    } else {
        let lower = sorted[middle - 1];
        lower.saturating_add(sorted[middle].saturating_sub(lower) / 2)
    }
}

fn certificate_not_after(der: &[u8]) -> Result<DateTime<Utc>, ProbeError> {
    let (_, certificate, trailing) = der_tlv(der, Some(0x30))?;
    if !trailing.is_empty() {
        return Err(ProbeError::Payload("trailing TLS certificate bytes".into()));
    }
    let (_, tbs_certificate, _) = der_tlv(certificate, Some(0x30))?;
    let mut cursor = tbs_certificate;
    if cursor.first() == Some(&0xa0) {
        cursor = der_tlv(cursor, Some(0xa0))?.2;
    }
    cursor = der_tlv(cursor, Some(0x02))?.2;
    cursor = der_tlv(cursor, Some(0x30))?.2;
    cursor = der_tlv(cursor, Some(0x30))?.2;
    let (_, validity, _) = der_tlv(cursor, Some(0x30))?;
    let (_, _, after_not_before) = der_tlv(validity, None)?;
    let (tag, not_after, trailing_validity) = der_tlv(after_not_before, None)?;
    if !trailing_validity.is_empty() {
        return Err(ProbeError::Payload(
            "invalid TLS certificate validity".into(),
        ));
    }
    parse_der_time(tag, not_after)
}

fn certificate_remaining_days(not_after: DateTime<Utc>, now: DateTime<Utc>) -> i64 {
    let remaining = not_after.signed_duration_since(now);
    if remaining <= chrono::Duration::zero() {
        return 0;
    }
    let whole_days = remaining.num_days();
    if remaining > chrono::Duration::days(whole_days) {
        whole_days + 1
    } else {
        whole_days
    }
}

fn der_tlv(input: &[u8], expected_tag: Option<u8>) -> Result<(u8, &[u8], &[u8]), ProbeError> {
    let (&tag, remainder) = input
        .split_first()
        .ok_or_else(|| ProbeError::Payload("truncated DER tag".into()))?;
    if expected_tag.is_some_and(|expected| expected != tag) {
        return Err(ProbeError::Payload("unexpected DER tag".into()));
    }
    let (&length_byte, remainder) = remainder
        .split_first()
        .ok_or_else(|| ProbeError::Payload("truncated DER length".into()))?;
    let (length, remainder) = if length_byte & 0x80 == 0 {
        (usize::from(length_byte), remainder)
    } else {
        let length_bytes = usize::from(length_byte & 0x7f);
        if length_bytes == 0 || length_bytes > std::mem::size_of::<usize>() {
            return Err(ProbeError::Payload("invalid DER length".into()));
        }
        let (encoded, remainder) = remainder
            .split_at_checked(length_bytes)
            .ok_or_else(|| ProbeError::Payload("truncated DER length".into()))?;
        if encoded.first() == Some(&0) {
            return Err(ProbeError::Payload("non-canonical DER length".into()));
        }
        let mut length = 0_usize;
        for byte in encoded {
            length = length
                .checked_mul(256)
                .and_then(|value| value.checked_add(usize::from(*byte)))
                .ok_or_else(|| ProbeError::Payload("DER length overflow".into()))?;
        }
        if length < 128 {
            return Err(ProbeError::Payload("non-canonical DER length".into()));
        }
        (length, remainder)
    };
    let (value, trailing) = remainder
        .split_at_checked(length)
        .ok_or_else(|| ProbeError::Payload("truncated DER value".into()))?;
    Ok((tag, value, trailing))
}

fn parse_der_time(tag: u8, value: &[u8]) -> Result<DateTime<Utc>, ProbeError> {
    let (year, offset) = match (tag, value.len()) {
        (0x17, 13) => {
            let short_year = decimal(&value[0..2])?;
            (
                if short_year >= 50 {
                    1900 + short_year
                } else {
                    2000 + short_year
                },
                2,
            )
        }
        (0x18, 15) => (decimal(&value[0..4])?, 4),
        _ => return Err(ProbeError::Payload("invalid TLS certificate time".into())),
    };
    if value.last() != Some(&b'Z') {
        return Err(ProbeError::Payload(
            "TLS certificate time is not UTC".into(),
        ));
    }
    let month = decimal(&value[offset..offset + 2])?;
    let day = decimal(&value[offset + 2..offset + 4])?;
    let hour = decimal(&value[offset + 4..offset + 6])?;
    let minute = decimal(&value[offset + 6..offset + 8])?;
    let second = decimal(&value[offset + 8..offset + 10])?;
    Utc.with_ymd_and_hms(
        i32::try_from(year).map_err(|_| ProbeError::Payload("invalid year".into()))?,
        month,
        day,
        hour,
        minute,
        second,
    )
    .single()
    .ok_or_else(|| ProbeError::Payload("invalid TLS certificate date".into()))
}

fn decimal(value: &[u8]) -> Result<u32, ProbeError> {
    value.iter().try_fold(0_u32, |number, byte| {
        if !byte.is_ascii_digit() {
            return Err(ProbeError::Payload(
                "invalid TLS certificate date digit".into(),
            ));
        }
        Ok(number * 10 + u32::from(byte - b'0'))
    })
}

impl OutboundRateLimiter {
    fn new(limits: Limits) -> Self {
        Self {
            state: Mutex::new(RequestRateState::new(
                Instant::now(),
                limits.global_requests_per_second,
                limits.burst,
            )),
            pressure: AtomicBool::new(false),
        }
    }

    fn set_pressure(&self, pressure: bool) {
        self.pressure.store(pressure, Ordering::Relaxed);
    }

    async fn acquire(&self) {
        loop {
            let wait = {
                let now = Instant::now();
                let mut state = self.state.lock().await;
                let pressure = self.pressure.load(Ordering::Relaxed);
                if state.try_acquire(now, pressure) {
                    return;
                }
                state.wait_duration(pressure)
            };
            tokio::time::sleep(wait).await;
        }
    }
}

impl RequestRateState {
    const PRESSURE_RATE_PER_SECOND: f64 = 0.25;
    const PRESSURE_BURST: f64 = 1.0;

    fn new(now: Instant, rate_per_second: f64, burst: f64) -> Self {
        Self {
            tokens: burst,
            last_refill: now,
            rate_per_second,
            burst,
        }
    }

    fn try_acquire(&mut self, now: Instant, pressure: bool) -> bool {
        let (rate, burst) = if pressure {
            (Self::PRESSURE_RATE_PER_SECOND, Self::PRESSURE_BURST)
        } else {
            (self.rate_per_second, self.burst)
        };
        let elapsed = now.saturating_duration_since(self.last_refill);
        self.tokens = (self.tokens.min(burst) + elapsed.as_secs_f64() * rate).min(burst);
        self.last_refill = now;
        if self.tokens < 1.0 {
            return false;
        }
        self.tokens -= 1.0;
        true
    }

    fn wait_duration(&self, pressure: bool) -> Duration {
        let rate = if pressure {
            Self::PRESSURE_RATE_PER_SECOND
        } else {
            self.rate_per_second
        };
        Duration::from_secs_f64((1.0 - self.tokens).max(0.0) / rate)
    }
}

async fn with_request_deadline<F, T>(timeout: Duration, future: F) -> Result<T, ProbeError>
where
    F: std::future::Future<Output = Result<T, ProbeError>>,
{
    tokio::time::timeout(timeout, future)
        .await
        .unwrap_or_else(|_| Err(ProbeError::Request("request timed out".into())))
}

fn validate_probe_url(url: &Url) -> Result<(), ProbeError> {
    if !matches!(url.scheme(), "http" | "https")
        || !url.username().is_empty()
        || url.password().is_some()
        || url.host().is_none()
    {
        return Err(ProbeError::InvalidEndpoint(
            "only credential-free HTTP(S) endpoints are supported".into(),
        ));
    }
    Ok(())
}

fn append_bounded(body: &mut Vec<u8>, chunk: &[u8], limit: usize) -> Result<(), ProbeError> {
    if body.len().saturating_add(chunk.len()) > limit {
        return Err(ProbeError::ResponseTooLarge);
    }
    body.extend_from_slice(chunk);
    Ok(())
}

fn outcome_for_error(check: CheckKind, error: &ProbeError) -> Outcome {
    match error {
        ProbeError::NoRestEndpoint => Outcome::Unsupported(error.to_string()),
        ProbeError::BudgetExhausted
        | ProbeError::Payload(_)
        | ProbeError::ResponseTooLarge
        | ProbeError::RemoteAddressMismatch
        | ProbeError::InvalidEndpoint(_)
        | ProbeError::UnsafeTarget(_) => Outcome::Unknown(error.to_string()),
        ProbeError::Http(status)
            if check != CheckKind::Availability
                && check != CheckKind::Supernode
                && matches!(status.as_u16(), 401 | 403 | 404 | 405 | 501) =>
        {
            Outcome::Unsupported(error.to_string())
        }
        ProbeError::Dns(_) | ProbeError::Request(_) | ProbeError::Http(_) | ProbeError::Tcp
            if matches!(check, CheckKind::Availability | CheckKind::PeerTcp) =>
        {
            Outcome::Failure(error.to_string())
        }
        ProbeError::Dns(_) | ProbeError::Request(_) | ProbeError::Http(_) | ProbeError::Tcp => {
            Outcome::Unknown(error.to_string())
        }
    }
}

fn direct_reachability_for_result(
    check: CheckKind,
    result: &Result<(Dimension, Outcome, Value), ProbeError>,
) -> Option<bool> {
    if !matches!(check, CheckKind::Availability | CheckKind::PeerTcp) {
        return None;
    }
    match result {
        Ok((_, Outcome::Unsupported(_), _)) => None,
        Ok((_, Outcome::Failure(_), _)) if check == CheckKind::PeerTcp => Some(false),
        Ok(_) => Some(true),
        Err(ProbeError::Dns(_) | ProbeError::Request(_) | ProbeError::Tcp) => Some(false),
        Err(ProbeError::Http(_) | ProbeError::Payload(_) | ProbeError::ResponseTooLarge) => {
            Some(true)
        }
        Err(
            ProbeError::BudgetExhausted
            | ProbeError::NoRestEndpoint
            | ProbeError::InvalidEndpoint(_)
            | ProbeError::UnsafeTarget(_)
            | ProbeError::RemoteAddressMismatch,
        ) => None,
    }
}

fn dimension_for(check: CheckKind) -> Dimension {
    match check {
        CheckKind::Availability
        | CheckKind::PeerTcp
        | CheckKind::Identity
        | CheckKind::WebSocket => Dimension::Connectivity,
        CheckKind::Chain => Dimension::Sync,
        CheckKind::Finalization => Dimension::Finalization,
        CheckKind::Peers => Dimension::Peers,
        CheckKind::StateHash => Dimension::StateHash,
        CheckKind::Harvest => Dimension::Harvest,
        CheckKind::Supernode => Dimension::Supernode,
        CheckKind::Certificate => Dimension::Certificate,
        CheckKind::Voting => Dimension::Voting,
    }
}

async fn resolve_public(host: &str, port: u16) -> Result<Vec<IpAddr>, ProbeError> {
    let mut addresses = if let Ok(address) = host.parse::<IpAddr>() {
        vec![address]
    } else {
        tokio::time::timeout(
            Duration::from_secs(3),
            tokio::net::lookup_host((host, port)),
        )
        .await
        .map_err(|_| ProbeError::Dns("lookup timed out".into()))?
        .map_err(|error| ProbeError::Dns(error.to_string()))?
        .map(|address| address.ip())
        .collect::<Vec<_>>()
    };
    addresses.sort_unstable();
    addresses.dedup();
    validate_public_target("https", host, &addresses)?;
    Ok(addresses)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use chrono::{TimeZone, Utc};
    use reqwest::Url;

    use super::{
        append_bounded, certificate_not_after, certificate_remaining_days,
        direct_reachability_for_result, validate_probe_url, validate_same_origin_redirect,
        with_request_deadline, ProbeError, RequestRateState,
    };
    use monitor_core::model::{CheckKind, Dimension, Outcome};
    use serde_json::json;

    #[test]
    fn response_body_limit_accepts_exact_boundary_and_rejects_one_more_byte() {
        let mut body = vec![0; 4 * 1024 * 1024 - 1];
        append_bounded(&mut body, &[1], 4 * 1024 * 1024).unwrap();
        assert!(append_bounded(&mut body, &[2], 4 * 1024 * 1024).is_err());
        assert_eq!(body.len(), 4 * 1024 * 1024);
    }

    #[test]
    fn direct_reachability_distinguishes_transport_failure_from_unhealthy_response() {
        let application_failure = Ok((
            Dimension::Connectivity,
            Outcome::Failure("REST health is down".into()),
            json!({}),
        ));
        assert_eq!(
            direct_reachability_for_result(CheckKind::Availability, &application_failure),
            Some(true)
        );
        assert_eq!(
            direct_reachability_for_result(
                CheckKind::Availability,
                &Err(ProbeError::Request("connection refused".into()))
            ),
            Some(false)
        );
        assert_eq!(
            direct_reachability_for_result(
                CheckKind::Availability,
                &Err(ProbeError::Payload("malformed response".into()))
            ),
            Some(true)
        );
        assert_eq!(
            direct_reachability_for_result(
                CheckKind::Chain,
                &Err(ProbeError::Request("connection refused".into()))
            ),
            None
        );
    }

    #[test]
    fn redirects_accept_only_credential_free_http_urls() {
        assert!(validate_probe_url(&Url::parse("https://node.example/path").unwrap()).is_ok());
        assert!(validate_probe_url(&Url::parse("ftp://node.example/path").unwrap()).is_err());
        assert!(
            validate_probe_url(&Url::parse("https://user@node.example/path").unwrap()).is_err()
        );
    }

    #[test]
    fn fixed_provider_redirects_cannot_change_origin() {
        let origin = Url::parse("https://nem.io/supernode/api/nodes").unwrap();
        assert!(validate_same_origin_redirect(
            &origin,
            &Url::parse("https://nem.io/supernode/api/node/1").unwrap()
        )
        .is_ok());
        for redirect in [
            "https://example.com/supernode/api/node/1",
            "http://nem.io/supernode/api/node/1",
            "https://nem.io:444/supernode/api/node/1",
        ] {
            assert!(
                validate_same_origin_redirect(&origin, &Url::parse(redirect).unwrap()).is_err()
            );
        }
    }

    #[tokio::test]
    async fn request_deadline_caps_the_complete_redirect_chain() {
        let result = with_request_deadline(
            Duration::from_millis(1),
            std::future::pending::<Result<(), ProbeError>>(),
        )
        .await;
        assert!(
            matches!(result, Err(ProbeError::Request(message)) if message == "request timed out")
        );
    }

    #[test]
    fn production_http_rate_state_uses_the_configured_rate_with_burst_four() {
        let start = tokio::time::Instant::now();
        let mut state = RequestRateState::new(start, 2.5, 4.0);
        for _ in 0..4 {
            assert!(state.try_acquire(start, false));
        }
        assert!(!state.try_acquire(start, false));
        assert!(!state.try_acquire(start + Duration::from_millis(399), false));
        assert!(state.try_acquire(start + Duration::from_millis(400), false));
        assert!(!state.try_acquire(start + Duration::from_millis(400), false));

        let mut pressured = RequestRateState::new(start, 2.5, 4.0);
        assert!(pressured.try_acquire(start, true));
        assert!(!pressured.try_acquire(start, true));
        assert!(!pressured.try_acquire(start + Duration::from_secs(3), true));
        assert!(pressured.try_acquire(start + Duration::from_secs(4), true));
    }

    #[test]
    fn strict_der_certificate_parser_reads_utc_not_after_and_rejects_truncation() {
        fn tlv(tag: u8, value: &[u8]) -> Vec<u8> {
            assert!(value.len() < 128);
            let mut encoded = vec![tag, u8::try_from(value.len()).unwrap()];
            encoded.extend_from_slice(value);
            encoded
        }

        let mut validity = tlv(0x17, b"260101000000Z");
        validity.extend(tlv(0x17, b"270203040506Z"));
        let mut tbs = tlv(0x02, &[1]);
        tbs.extend(tlv(0x30, &[]));
        tbs.extend(tlv(0x30, &[]));
        tbs.extend(tlv(0x30, &validity));
        let certificate = tlv(0x30, &tlv(0x30, &tbs));

        assert_eq!(
            certificate_not_after(&certificate).unwrap(),
            Utc.with_ymd_and_hms(2027, 2, 3, 4, 5, 6).single().unwrap()
        );
        assert!(certificate_not_after(&certificate[..certificate.len() - 1]).is_err());
    }

    #[test]
    fn certificate_remaining_days_keeps_partial_valid_days_above_zero() {
        let now = Utc.with_ymd_and_hms(2026, 9, 21, 12, 0, 0).unwrap();

        assert_eq!(
            certificate_remaining_days(now + chrono::Duration::days(30), now),
            30
        );
        assert_eq!(
            certificate_remaining_days(
                now + chrono::Duration::days(30) + chrono::Duration::seconds(1),
                now,
            ),
            31
        );
        assert_eq!(
            certificate_remaining_days(now + chrono::Duration::seconds(1), now),
            1
        );
        assert_eq!(certificate_remaining_days(now, now), 0);
        assert_eq!(
            certificate_remaining_days(now - chrono::Duration::seconds(1), now),
            0
        );
    }
    #[tokio::test]
    async fn community_tcp_burst_one_spaces_actual_connections() {
        use super::{MeasurementBudget, PeerTcpTransport, TcpConnectAttempt, TcpConnectResult};
        use std::sync::{Arc, Mutex};
        #[derive(Debug)]
        struct Budget;
        #[async_trait::async_trait]
        impl MeasurementBudget for Budget {
            async fn reserve(&self, _: u64) -> Result<String, ProbeError> {
                Ok("test".into())
            }
            async fn settle(&self, _: &str, _: u64, _: u64) -> Result<(), ProbeError> {
                Ok(())
            }
        }
        #[derive(Debug)]
        struct Tcp(Arc<Mutex<Vec<tokio::time::Instant>>>);
        #[async_trait::async_trait]
        impl PeerTcpTransport for Tcp {
            async fn resolve(
                &self,
                _: &str,
                _: u16,
                _: Duration,
            ) -> Result<Vec<std::net::IpAddr>, ProbeError> {
                Ok(vec![
                    "203.0.113.1".parse().unwrap(),
                    "203.0.113.2".parse().unwrap(),
                ])
            }
            async fn connect(
                &self,
                address: std::net::SocketAddr,
                _: Duration,
            ) -> TcpConnectAttempt {
                self.0.lock().unwrap().push(tokio::time::Instant::now());
                TcpConnectAttempt {
                    address,
                    elapsed_ms: 0,
                    result: TcpConnectResult::Connected,
                }
            }
        }
        let times = Arc::new(Mutex::new(Vec::new()));
        let engine = super::ProbeEngine::with_limits(
            "test",
            crate::limiter::Limits {
                global_requests_per_second: 20.0,
                burst: 1.0,
                ..Default::default()
            },
        )
        .with_measurement_budget("test", Arc::new(Budget))
        .with_peer_tcp_transport(Tcp(times.clone()));
        let node = monitor_core::model::Node::new(
            "tcp-quota",
            monitor_core::model::Network::SymbolMainnet,
            monitor_core::model::NodeKind::PeerOnly,
            "fixture.example",
        );
        engine
            .peer_tcp(&node, Duration::from_secs(3), true)
            .await
            .unwrap();
        let times = times.lock().unwrap();
        assert_eq!(times.len(), 2);
        assert!(
            times[1].duration_since(times[0]) >= Duration::from_millis(40),
            "token waits must not be followed by a simultaneous address burst"
        );
    }
}
