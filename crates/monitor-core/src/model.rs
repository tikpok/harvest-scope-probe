use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Network {
    SymbolMainnet,
    Nis1Mainnet,
}

impl Network {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SymbolMainnet => "symbol-mainnet",
            Self::Nis1Mainnet => "nis1-mainnet",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeKind {
    Api,
    PeerOnly,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Node {
    pub id: String,
    pub network: Network,
    pub kind: NodeKind,
    pub display_name: String,
    pub public_key: Option<String>,
    pub host: String,
    pub rest_scheme: Option<String>,
    pub rest_port: Option<u16>,
    pub p2p_port: u16,
}

impl Node {
    #[must_use]
    pub fn new(
        id: impl Into<String>,
        network: Network,
        kind: NodeKind,
        host: impl Into<String>,
    ) -> Self {
        let host = host.into();
        Self {
            display_name: host.clone(),
            id: id.into(),
            network,
            kind,
            public_key: None,
            host,
            rest_scheme: (kind == NodeKind::Api).then(|| match network {
                Network::SymbolMainnet => "https".to_owned(),
                Network::Nis1Mainnet => "http".to_owned(),
            }),
            rest_port: (kind == NodeKind::Api).then_some(match network {
                Network::SymbolMainnet => 3001,
                Network::Nis1Mainnet => 7890,
            }),
            p2p_port: match network {
                Network::SymbolMainnet => 7900,
                Network::Nis1Mainnet => 7890,
            },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Site {
    A,
    B,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckKind {
    Availability,
    PeerTcp,
    Chain,
    Finalization,
    Peers,
    StateHash,
    Harvest,
    Supernode,
    Identity,
    Certificate,
    Voting,
    WebSocket,
}

impl CheckKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Availability => "availability",
            Self::PeerTcp => "peer_tcp",
            Self::Chain => "chain",
            Self::Finalization => "finalization",
            Self::Peers => "peers",
            Self::StateHash => "state_hash",
            Self::Harvest => "harvest",
            Self::Supernode => "supernode",
            Self::Identity => "identity",
            Self::Certificate => "certificate",
            Self::Voting => "voting",
            Self::WebSocket => "web_socket",
        }
    }

    #[must_use]
    pub const fn request_units(self) -> u8 {
        match self {
            Self::Harvest => 2,
            Self::Supernode => 3,
            Self::Availability
            | Self::PeerTcp
            | Self::Chain
            | Self::Finalization
            | Self::Peers
            | Self::StateHash
            | Self::Identity
            | Self::Certificate
            | Self::Voting
            | Self::WebSocket => 1,
        }
    }

    #[must_use]
    pub const fn supports_dimension(self, dimension: Dimension) -> bool {
        matches!(
            (self, dimension),
            (
                Self::Availability | Self::PeerTcp,
                Dimension::Connectivity | Dimension::Latency
            ) | (Self::Identity | Self::WebSocket, Dimension::Connectivity)
                | (Self::Chain, Dimension::Sync)
                | (Self::Finalization, Dimension::Finalization)
                | (Self::Peers, Dimension::Peers)
                | (Self::StateHash, Dimension::StateHash)
                | (Self::Harvest, Dimension::Harvest)
                | (Self::Supernode, Dimension::Supernode)
                | (Self::Certificate, Dimension::Certificate)
                | (Self::Voting, Dimension::Voting)
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Dimension {
    Connectivity,
    Latency,
    Sync,
    Finalization,
    Peers,
    Certificate,
    Voting,
    Harvest,
    Supernode,
    StateHash,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    Warning,
    Critical,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum Outcome {
    Healthy,
    Failure(String),
    LatencyMillis(u64),
    LagBlocks(u64),
    FinalizationStalledMinutes(u64),
    Remaining(i64),
    ZeroPeers,
    Mismatch(String),
    Unknown(String),
    Unsupported(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Observation {
    pub job_id: String,
    pub node_id: String,
    pub site: Site,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub check_kind: Option<CheckKind>,
    pub dimension: Dimension,
    pub outcome: Outcome,
    #[serde(default = "empty_evidence")]
    pub evidence: serde_json::Value,
    pub observed_at: DateTime<Utc>,
}

impl Observation {
    #[must_use]
    pub fn new(
        job_id: impl Into<String>,
        node_id: impl Into<String>,
        site: Site,
        dimension: Dimension,
        outcome: Outcome,
        observed_at: DateTime<Utc>,
    ) -> Self {
        Self {
            job_id: job_id.into(),
            node_id: node_id.into(),
            site,
            check_kind: Some(default_check_kind(dimension)),
            dimension,
            outcome,
            evidence: empty_evidence(),
            observed_at,
        }
    }

    #[must_use]
    pub fn with_evidence(mut self, evidence: serde_json::Value) -> Self {
        self.evidence = evidence;
        self
    }

    #[must_use]
    pub const fn with_check_kind(mut self, check_kind: CheckKind) -> Self {
        self.check_kind = Some(check_kind);
        self
    }
}

const fn default_check_kind(dimension: Dimension) -> CheckKind {
    match dimension {
        Dimension::Connectivity | Dimension::Latency => CheckKind::Availability,
        Dimension::Sync => CheckKind::Chain,
        Dimension::Finalization => CheckKind::Finalization,
        Dimension::Peers => CheckKind::Peers,
        Dimension::Certificate => CheckKind::Certificate,
        Dimension::Voting => CheckKind::Voting,
        Dimension::Harvest => CheckKind::Harvest,
        Dimension::Supernode => CheckKind::Supernode,
        Dimension::StateHash => CheckKind::StateHash,
    }
}

fn empty_evidence() -> serde_json::Value {
    serde_json::json!({})
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ObservationBatch {
    pub probe_id: String,
    #[serde(default)]
    pub nodes: Vec<Node>,
    pub observations: Vec<Observation>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IngestResult {
    pub accepted: usize,
    pub duplicates: usize,
}
