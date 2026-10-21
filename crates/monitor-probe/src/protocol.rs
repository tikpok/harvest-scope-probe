use chrono::{DateTime, Duration, Utc};
use monitor_core::{
    model::{Network, Node, NodeKind},
    scheduler::CheckKind,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use url::Url;

const SUPER_NODE_MAX_SEARCH_RESULTS: usize = 1_000;
const SUPER_NODE_MAX_RESULT_AGE: Duration = Duration::hours(12);
const SUPER_NODE_FUTURE_TOLERANCE: Duration = Duration::minutes(5);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChainSnapshot {
    pub height: u64,
    pub finalized_height: Option<u64>,
    pub finalization_epoch: Option<u64>,
    pub network_height: Option<u64>,
    pub chain_score: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupernodeRegistration {
    pub id: u64,
    pub active: bool,
    pub status: String,
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupernodeRound {
    pub round_id: u64,
    pub timestamp: DateTime<Utc>,
    pub passed: bool,
    pub failed_tests: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ProtocolError {
    #[error("missing or invalid field: {0}")]
    InvalidField(&'static str),
}

pub fn parse_symbol_chain(value: &Value) -> Result<ChainSnapshot, ProtocolError> {
    let height = u64_value(value.get("height")).ok_or(ProtocolError::InvalidField("height"))?;
    let finalized = value
        .get("latestFinalizedBlock")
        .ok_or(ProtocolError::InvalidField("latestFinalizedBlock"))?;
    let finalized_height = u64_value(finalized.get("height"))
        .ok_or(ProtocolError::InvalidField("latestFinalizedBlock.height"))?;
    let finalization_epoch = u64_value(finalized.get("finalizationEpoch")).ok_or(
        ProtocolError::InvalidField("latestFinalizedBlock.finalizationEpoch"),
    )?;
    let score_high =
        u64_value(value.get("scoreHigh")).ok_or(ProtocolError::InvalidField("scoreHigh"))?;
    let score_low =
        u64_value(value.get("scoreLow")).ok_or(ProtocolError::InvalidField("scoreLow"))?;
    let chain_score = ((u128::from(score_high) << 64) | u128::from(score_low)).to_string();
    Ok(ChainSnapshot {
        height,
        finalized_height: Some(finalized_height),
        finalization_epoch: Some(finalization_epoch),
        network_height: None,
        chain_score: Some(chain_score),
    })
}

pub fn parse_nis1_chain(
    height: &Value,
    network_height: Option<&Value>,
    score: Option<&Value>,
) -> Result<ChainSnapshot, ProtocolError> {
    let height = u64_value(height.get("height")).ok_or(ProtocolError::InvalidField("height"))?;
    let network_height = network_height
        .and_then(|value| u64_value(value.get("height")))
        .ok_or(ProtocolError::InvalidField("networkHeight.height"))?;
    let chain_score = score
        .and_then(|value| string_value(value.get("score")))
        .ok_or(ProtocolError::InvalidField("score.score"))?;
    Ok(ChainSnapshot {
        height,
        finalized_height: None,
        finalization_epoch: None,
        network_height: Some(network_height),
        chain_score: Some(chain_score),
    })
}

pub fn parse_symbol_health(value: &Value) -> Result<(String, String, bool), ProtocolError> {
    let status = value.get("status").unwrap_or(value);
    let api = status
        .get("apiNode")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or(ProtocolError::InvalidField("status.apiNode"))?;
    let database = status
        .get("db")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or(ProtocolError::InvalidField("status.db"))?;
    Ok((
        api.to_owned(),
        database.to_owned(),
        api.eq_ignore_ascii_case("up") && database.eq_ignore_ascii_case("up"),
    ))
}

pub fn parse_nis1_health(
    heartbeat: &Value,
    status: &Value,
) -> Result<(i64, i64, bool), ProtocolError> {
    let heartbeat_code =
        i64_value(heartbeat.get("code")).ok_or(ProtocolError::InvalidField("heartbeat.code"))?;
    let status_code =
        i64_value(status.get("code")).ok_or(ProtocolError::InvalidField("status.code"))?;
    Ok((
        heartbeat_code,
        status_code,
        heartbeat_code == 1 && matches!(status_code, 5 | 6),
    ))
}

pub fn parse_peer_count(network: Network, value: &Value) -> Result<usize, ProtocolError> {
    match network {
        Network::SymbolMainnet => value
            .as_array()
            .map(Vec::len)
            .ok_or(ProtocolError::InvalidField("peers")),
        Network::Nis1Mainnet => {
            ["active", "inactive", "busy", "failure"]
                .iter()
                .try_fold(0usize, |count, key| {
                    value
                        .get(key)
                        .and_then(Value::as_array)
                        .map(|peers| count + peers.len())
                        .ok_or(ProtocolError::InvalidField("peer-list"))
                })
        }
    }
}

pub fn parse_harvester_count(value: &Value) -> Result<usize, ProtocolError> {
    value
        .get("unlockedAccount")
        .and_then(Value::as_array)
        .map(Vec::len)
        .ok_or(ProtocolError::InvalidField("unlockedAccount"))
}

pub fn parse_symbol_state_hash(value: &Value) -> Result<String, ProtocolError> {
    value
        .get("block")
        .unwrap_or(value)
        .get("stateHash")
        .and_then(Value::as_str)
        .filter(|value| value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .map(str::to_ascii_uppercase)
        .ok_or(ProtocolError::InvalidField("block.stateHash"))
}

pub fn parse_symbol_identity(node: &Value, server: &Value) -> Result<Value, ProtocolError> {
    let public_key = node
        .get("nodePublicKey")
        .or_else(|| node.get("publicKey"))
        .and_then(Value::as_str)
        .filter(|value| is_public_key(value))
        .ok_or(ProtocolError::InvalidField("nodePublicKey"))?;
    let version = string_value(node.get("version"))
        .filter(|value| !value.is_empty())
        .ok_or(ProtocolError::InvalidField("version"))?;
    let roles = u64_value(node.get("roles")).ok_or(ProtocolError::InvalidField("roles"))?;
    let server_info = server
        .get("serverInfo")
        .ok_or(ProtocolError::InvalidField("serverInfo"))?;
    let rest_version = server_info
        .get("restVersion")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or(ProtocolError::InvalidField("serverInfo.restVersion"))?;
    let sdk_version = server_info
        .get("sdkVersion")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty());
    Ok(serde_json::json!({
        "publicKey": public_key,
        "version": version,
        "roles": roles,
        "restVersion": rest_version,
        "sdkVersion": sdk_version,
    }))
}

pub fn parse_nis1_identity(value: &Value) -> Result<Value, ProtocolError> {
    let node = value
        .get("node")
        .ok_or(ProtocolError::InvalidField("node"))?;
    node.get("metaData")
        .and_then(|value| i64_value(value.get("networkId")))
        .filter(|value| matches!(*value, -104 | 104))
        .ok_or(ProtocolError::InvalidField("node.metaData.networkId"))?;
    let network_id = -104;
    let public_key = node
        .get("identity")
        .and_then(|value| value.get("public-key"))
        .and_then(Value::as_str)
        .filter(|value| is_public_key(value))
        .ok_or(ProtocolError::InvalidField("node.identity.public-key"))?;
    let nis_info = value
        .get("nisInfo")
        .ok_or(ProtocolError::InvalidField("nisInfo"))?;
    let application = nis_info
        .get("application")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or(ProtocolError::InvalidField("nisInfo.application"))?;
    let version = string_value(nis_info.get("version"))
        .filter(|value| !value.is_empty())
        .ok_or(ProtocolError::InvalidField("nisInfo.version"))?;
    Ok(serde_json::json!({
        "publicKey": public_key,
        "networkId": network_id,
        "application": application,
        "version": version,
    }))
}

pub fn parse_supernode_registration(
    value: &Value,
    authorities: &[(&str, u16)],
) -> Result<Option<SupernodeRegistration>, ProtocolError> {
    let rows = value
        .as_array()
        .filter(|rows| rows.len() <= SUPER_NODE_MAX_SEARCH_RESULTS)
        .ok_or(ProtocolError::InvalidField("supernode registrations"))?;
    let mut matched = None;
    for row in rows {
        let Some(endpoint) = row.get("endpoint").and_then(Value::as_str) else {
            continue;
        };
        let Some((host, port)) = authorities
            .iter()
            .find(|(host, port)| endpoint_matches(endpoint, host, *port))
        else {
            continue;
        };
        let id = u64_value(row.get("id"))
            .filter(|id| *id > 0)
            .ok_or(ProtocolError::InvalidField("supernode.id"))?;
        let status = row
            .get("status")
            .and_then(Value::as_str)
            .filter(|status| {
                status.eq_ignore_ascii_case("active")
                    || status.eq_ignore_ascii_case("inactive")
                    || status.eq_ignore_ascii_case("stale")
            })
            .ok_or(ProtocolError::InvalidField("supernode.status"))?
            .to_ascii_lowercase();
        if matched.is_some() {
            return Err(ProtocolError::InvalidField("supernode endpoint match"));
        }
        matched = Some(SupernodeRegistration {
            id,
            active: status == "active",
            status,
            host: (*host).to_owned(),
            port: *port,
        });
    }
    Ok(matched)
}

pub fn parse_supernode_info(
    value: &Value,
    expected_id: u64,
    host: &str,
    port: u16,
) -> Result<u64, ProtocolError> {
    let id = u64_value(value.get("id")).ok_or(ProtocolError::InvalidField("supernode.id"))?;
    if id != expected_id {
        return Err(ProtocolError::InvalidField("supernode.id"));
    }
    let endpoint = value
        .get("endpoint")
        .and_then(Value::as_str)
        .filter(|endpoint| endpoint_matches(endpoint, host, port))
        .ok_or(ProtocolError::InvalidField("supernode.endpoint"))?;
    let _ = endpoint;
    value
        .get("status")
        .and_then(Value::as_str)
        .filter(|status| status.eq_ignore_ascii_case("active"))
        .ok_or(ProtocolError::InvalidField("supernode.status"))?;
    u64_value(value.get("lastResultsRound"))
        .ok_or(ProtocolError::InvalidField("supernode.lastResultsRound"))
}

pub fn parse_supernode_round(
    value: &Value,
    expected_round_id: u64,
    now: DateTime<Utc>,
) -> Result<SupernodeRound, ProtocolError> {
    let rows = value
        .as_array()
        .filter(|rows| rows.len() == 1)
        .ok_or(ProtocolError::InvalidField("supernode round"))?;
    let row = &rows[0];
    let round_id = u64_value(row.get("roundId"))
        .filter(|round_id| *round_id == expected_round_id)
        .ok_or(ProtocolError::InvalidField("supernode.roundId"))?;
    let timestamp = row
        .get("timestamp")
        .and_then(Value::as_str)
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .map(|value| value.with_timezone(&Utc))
        .ok_or(ProtocolError::InvalidField("supernode.timestamp"))?;
    if timestamp > now + SUPER_NODE_FUTURE_TOLERANCE
        || now.signed_duration_since(timestamp) > SUPER_NODE_MAX_RESULT_AGE
    {
        return Err(ProtocolError::InvalidField("supernode.timestamp"));
    }

    let tests = [
        ("balance", "balance"),
        ("bandwidth", "bandwidth"),
        ("chainHeight", "chainHeight"),
        ("chainPart", "chainPart"),
        ("computingPower", "computingPower"),
        ("ping", "ping"),
        ("responsiveness", "responsiveness"),
        ("version", "version"),
    ];
    let mut failed_tests = Vec::new();
    for (field, label) in tests {
        let passed = row
            .get(field)
            .and_then(Value::as_bool)
            .ok_or(ProtocolError::InvalidField("supernode test result"))?;
        if !passed {
            failed_tests.push(label.to_owned());
        }
    }
    Ok(SupernodeRound {
        round_id,
        timestamp,
        passed: failed_tests.is_empty(),
        failed_tests,
    })
}

fn endpoint_matches(endpoint: &str, host: &str, port: u16) -> bool {
    let Ok(endpoint) = Url::parse(endpoint) else {
        return false;
    };
    matches!(endpoint.scheme(), "http" | "https")
        && endpoint.username().is_empty()
        && endpoint.password().is_none()
        && endpoint.query().is_none()
        && endpoint.fragment().is_none()
        && matches!(endpoint.path(), "" | "/")
        && endpoint
            .host_str()
            .is_some_and(|endpoint_host| endpoint_host.eq_ignore_ascii_case(host))
        && endpoint.port_or_known_default() == Some(port)
}

pub fn parse_voting_remaining(
    account: &Value,
    current_epoch: i64,
) -> Result<Option<(i64, i64, i64)>, ProtocolError> {
    if current_epoch < 0 {
        return Err(ProtocolError::InvalidField("finalizationEpoch"));
    }
    let Some(public_keys) = account
        .get("account")
        .and_then(|value| value.get("supplementalPublicKeys"))
        .and_then(|value| value.get("voting"))
        .and_then(|value| value.get("publicKeys"))
    else {
        return Ok(None);
    };
    let public_keys = public_keys.as_array().ok_or(ProtocolError::InvalidField(
        "account.supplementalPublicKeys.voting.publicKeys",
    ))?;
    let mut ranges = Vec::with_capacity(public_keys.len());
    for public_key in public_keys {
        let start = u64_value(public_key.get("startEpoch"))
            .and_then(|value| i64::try_from(value).ok())
            .ok_or(ProtocolError::InvalidField("voting.startEpoch"))?;
        let end = u64_value(public_key.get("endEpoch"))
            .and_then(|value| i64::try_from(value).ok())
            .ok_or(ProtocolError::InvalidField("voting.endEpoch"))?;
        if start > end {
            return Err(ProtocolError::InvalidField("voting.epochRange"));
        }
        ranges.push((start, end));
    }
    ranges.sort_unstable();
    let selected = ranges
        .iter()
        .copied()
        .find(|(start, end)| *start <= current_epoch && current_epoch <= *end)
        .or_else(|| {
            ranges
                .iter()
                .copied()
                .filter(|(start, _)| *start > current_epoch)
                .min_by_key(|(start, _)| *start)
        })
        .or_else(|| ranges.iter().copied().max_by_key(|(_, end)| *end));
    Ok(selected.map(|selected| {
        (
            selected.0,
            selected.1,
            selected.1.saturating_sub(current_epoch),
        )
    }))
}

#[must_use]
pub fn request_path(node: &Node, check: CheckKind) -> Option<&'static str> {
    if node.kind == NodeKind::PeerOnly {
        return match check {
            CheckKind::PeerTcp => Some("tcp"),
            CheckKind::Voting if node.network == Network::SymbolMainnet => Some("reference:voting"),
            _ => None,
        };
    }
    match (node.network, check) {
        (_, CheckKind::PeerTcp) => Some("tcp"),
        (Network::SymbolMainnet, CheckKind::Availability) => Some("/node/health"),
        (Network::SymbolMainnet, CheckKind::Chain | CheckKind::Finalization) => Some("/chain/info"),
        (Network::SymbolMainnet, CheckKind::Peers) => Some("/node/peers"),
        (Network::SymbolMainnet, CheckKind::StateHash) => Some("/blocks/{height}"),
        (Network::SymbolMainnet, CheckKind::Harvest) => Some("/node/unlockedaccount"),
        (Network::SymbolMainnet, CheckKind::Identity) => Some("/node/info"),
        (Network::SymbolMainnet, CheckKind::Certificate) => Some("tls"),
        (Network::SymbolMainnet, CheckKind::Voting) => Some("reference:voting"),
        (Network::SymbolMainnet, CheckKind::WebSocket) => Some("/ws"),
        (Network::SymbolMainnet, CheckKind::Supernode) => None,
        (Network::Nis1Mainnet, CheckKind::Availability) => Some("/heartbeat"),
        (Network::Nis1Mainnet, CheckKind::Chain) => Some("/chain/height"),
        (Network::Nis1Mainnet, CheckKind::Peers) => Some("/node/peer-list/all"),
        (Network::Nis1Mainnet, CheckKind::Supernode) => Some("external:supernode"),
        (Network::Nis1Mainnet, CheckKind::Identity) => Some("/node/extended-info"),
        (Network::Nis1Mainnet, CheckKind::Certificate) => Some("tls"),
        (
            Network::Nis1Mainnet,
            CheckKind::Finalization
            | CheckKind::StateHash
            | CheckKind::Harvest
            | CheckKind::Voting
            | CheckKind::WebSocket,
        ) => None,
    }
}

fn u64_value(value: Option<&Value>) -> Option<u64> {
    value.and_then(|value| {
        value
            .as_u64()
            .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
    })
}

fn i64_value(value: Option<&Value>) -> Option<i64> {
    value.and_then(|value| {
        value
            .as_i64()
            .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
    })
}

fn string_value(value: Option<&Value>) -> Option<String> {
    value.and_then(|value| {
        value
            .as_str()
            .map(ToOwned::to_owned)
            .or_else(|| value.as_u64().map(|number| number.to_string()))
    })
}

fn is_public_key(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}
