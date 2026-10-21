use std::{
    collections::HashMap,
    fmt,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use async_trait::async_trait;
use monitor_core::{
    model::{Network, Node, NodeKind, Observation, Outcome, Site},
    scheduler::{CheckKind, ScheduledJob},
};
use monitor_probe::engine::{
    CertificateInfo, PeerTcpTransport, ProbeEngine, ProbeError, ProbeHttpTransport,
    TcpConnectAttempt, TcpConnectResult, TimedJson,
};
use monitor_probe::history::{FinalizationHistory, FinalizationSample};
use reqwest::{StatusCode, Url};
use serde_json::Value;
use tokio::sync::Barrier;

fn job(check: CheckKind) -> ScheduledJob {
    ScheduledJob {
        job_id: format!("job-{check:?}"),
        node_id: "node".into(),
        check,
        offset_seconds: 0,
        site_epoch_offset_seconds: 0,
        request_units: check.request_units(),
    }
}

#[tokio::test]
async fn reports_unsupported_without_attempting_unavailable_protocols() {
    let engine = ProbeEngine::new("ops@example.com");
    let peer = Node::new(
        "peer",
        Network::SymbolMainnet,
        NodeKind::PeerOnly,
        "127.0.0.1",
    );
    let observation = engine.check(&peer, &job(CheckKind::Chain), Site::A).await;
    assert!(matches!(observation.outcome, Outcome::Unsupported(_)));

    let nis = Node::new("nis", Network::Nis1Mainnet, NodeKind::Api, "127.0.0.1");
    for check in [
        CheckKind::Finalization,
        CheckKind::StateHash,
        CheckKind::Harvest,
        CheckKind::Voting,
        CheckKind::WebSocket,
    ] {
        let observation = engine.check(&nis, &job(check), Site::A).await;
        assert!(
            matches!(observation.outcome, Outcome::Unsupported(_)),
            "{check:?}: {:?}",
            observation.outcome
        );
    }
}

struct FakeHttp {
    responses: HashMap<String, Value>,
    error: Option<FakeError>,
}

enum FakeError {
    Request,
    NotFound,
}

impl fmt::Debug for FakeHttp {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("FakeHttp").finish_non_exhaustive()
    }
}

#[async_trait]
impl ProbeHttpTransport for FakeHttp {
    async fn get_json(&self, url: Url, _timeout: Duration) -> Result<Value, ProbeError> {
        match self.error {
            Some(FakeError::Request) => return Err(ProbeError::Request("offline".into())),
            Some(FakeError::NotFound) => return Err(ProbeError::Http(StatusCode::NOT_FOUND)),
            None => {}
        }
        self.responses
            .get(&format!(
                "{}{}",
                url.host_str().unwrap_or_default(),
                url.path()
            ))
            .or_else(|| self.responses.get(url.path()))
            .cloned()
            .ok_or_else(|| ProbeError::Payload(format!("unexpected path {}", url.path())))
    }

    async fn get_json_timed(&self, url: Url, timeout: Duration) -> Result<TimedJson, ProbeError> {
        Ok(TimedJson {
            value: self.get_json(url, timeout).await?,
            latency_ms: 2_345,
        })
    }
}

#[derive(Clone)]
struct CountingHttp {
    responses: Arc<HashMap<String, Value>>,
    counts: Arc<Mutex<HashMap<String, usize>>>,
}

impl fmt::Debug for CountingHttp {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CountingHttp")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl ProbeHttpTransport for CountingHttp {
    async fn get_json(&self, url: Url, _timeout: Duration) -> Result<Value, ProbeError> {
        let key = format!("{}{}", url.host_str().unwrap_or_default(), url.path());
        *self.counts.lock().unwrap().entry(key.clone()).or_default() += 1;
        self.responses
            .get(&key)
            .cloned()
            .ok_or_else(|| ProbeError::Payload(format!("unexpected path {key}")))
    }
}

#[derive(Debug, Clone)]
struct BarrierHttp {
    barrier: Arc<Barrier>,
}

#[async_trait]
impl ProbeHttpTransport for BarrierHttp {
    async fn get_json(&self, _url: Url, _timeout: Duration) -> Result<Value, ProbeError> {
        self.barrier.wait().await;
        Ok(serde_json::json!({
            "height": 100,
            "scoreHigh": 2,
            "scoreLow": 3,
            "latestFinalizedBlock": {"height": 99, "finalizationEpoch": 77}
        }))
    }
}

#[derive(Debug)]
struct FakeWebSocketHttp;

#[async_trait]
impl ProbeHttpTransport for FakeWebSocketHttp {
    async fn get_json(&self, _url: Url, _timeout: Duration) -> Result<Value, ProbeError> {
        Err(ProbeError::Request("JSON was not expected".into()))
    }

    async fn websocket_handshake(&self, url: Url, _timeout: Duration) -> Result<(), ProbeError> {
        assert_eq!(url.as_str(), "wss://ws.example:3001/ws");
        Ok(())
    }
}

#[derive(Debug)]
struct FakeCertificateHttp;

#[async_trait]
impl ProbeHttpTransport for FakeCertificateHttp {
    async fn get_json(&self, _url: Url, _timeout: Duration) -> Result<Value, ProbeError> {
        Err(ProbeError::Request("JSON was not expected".into()))
    }

    async fn certificate_info(
        &self,
        url: Url,
        _timeout: Duration,
    ) -> Result<CertificateInfo, ProbeError> {
        assert_eq!(url.as_str(), "https://tls.example:3001/");
        Ok(CertificateInfo {
            not_after: chrono::Utc::now() + chrono::Duration::days(365),
        })
    }
}

#[derive(Debug, Clone)]
struct FakePeerTcp {
    addresses: Vec<IpAddr>,
    result: TcpConnectResult,
    timeouts: Arc<Mutex<Vec<Duration>>>,
}

#[async_trait]
impl PeerTcpTransport for FakePeerTcp {
    async fn resolve(
        &self,
        _host: &str,
        _port: u16,
        _timeout: Duration,
    ) -> Result<Vec<IpAddr>, ProbeError> {
        Ok(self.addresses.clone())
    }

    async fn connect(&self, address: SocketAddr, timeout: Duration) -> TcpConnectAttempt {
        self.timeouts.lock().unwrap().push(timeout);
        TcpConnectAttempt {
            address,
            elapsed_ms: if address.is_ipv4() { 12 } else { 18 },
            result: self.result,
        }
    }
}

#[derive(Debug, Clone)]
struct ConcurrentPeerTcp {
    active: Arc<AtomicUsize>,
    maximum: Arc<AtomicUsize>,
}

#[derive(Debug, Clone)]
struct DeadlinePeerTcp {
    connect_timeout: Arc<Mutex<Option<Duration>>>,
}

#[async_trait]
impl PeerTcpTransport for DeadlinePeerTcp {
    async fn resolve(
        &self,
        _host: &str,
        _port: u16,
        _timeout: Duration,
    ) -> Result<Vec<IpAddr>, ProbeError> {
        tokio::time::sleep(Duration::from_millis(200)).await;
        Ok(vec![IpAddr::V4(Ipv4Addr::new(203, 0, 113, 10))])
    }

    async fn connect(&self, address: SocketAddr, timeout: Duration) -> TcpConnectAttempt {
        *self.connect_timeout.lock().unwrap() = Some(timeout);
        // Ignore the requested timeout so the engine-level total deadline is exercised.
        tokio::time::sleep(Duration::from_millis(3_200)).await;
        TcpConnectAttempt {
            address,
            elapsed_ms: 3_200,
            result: TcpConnectResult::Timeout,
        }
    }
}

#[async_trait]
impl PeerTcpTransport for ConcurrentPeerTcp {
    async fn resolve(
        &self,
        _host: &str,
        _port: u16,
        _timeout: Duration,
    ) -> Result<Vec<IpAddr>, ProbeError> {
        Ok(vec![
            IpAddr::V4(Ipv4Addr::new(203, 0, 113, 10)),
            IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 10)),
        ])
    }

    async fn connect(&self, address: SocketAddr, _timeout: Duration) -> TcpConnectAttempt {
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.maximum.fetch_max(active, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(10)).await;
        self.active.fetch_sub(1, Ordering::SeqCst);
        TcpConnectAttempt {
            address,
            elapsed_ms: 10,
            result: TcpConnectResult::Timeout,
        }
    }
}

fn symbol_node() -> Node {
    Node::new(
        "symbol",
        Network::SymbolMainnet,
        NodeKind::Api,
        "example.com",
    )
}

fn nis1_node() -> Node {
    Node::new("nis", Network::Nis1Mainnet, NodeKind::Api, "nis.example")
}

#[tokio::test]
async fn peer_tcp_failure_records_dns_addresses_attempt_reasons_and_durations() {
    let timeouts = Arc::new(Mutex::new(Vec::new()));
    let engine = ProbeEngine::new("ops@example.com").with_peer_tcp_transport(FakePeerTcp {
        addresses: vec![
            IpAddr::V4(Ipv4Addr::new(203, 0, 113, 10)),
            IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 10)),
        ],
        result: TcpConnectResult::ConnectionRefused,
        timeouts: Arc::clone(&timeouts),
    });

    let observation = engine
        .check(&symbol_node(), &job(CheckKind::PeerTcp), Site::A)
        .await;

    assert!(matches!(
        observation.outcome,
        Outcome::Failure(ref message) if message.contains("connection refused")
    ));
    assert_eq!(observation.evidence["directReachable"], false);
    assert_eq!(
        observation.evidence["dns"]["addresses"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    let attempts = observation.evidence["attempts"].as_array().unwrap();
    assert_eq!(attempts.len(), 2);
    assert!(attempts
        .iter()
        .all(|attempt| attempt["result"] == "connection_refused"));
    assert_eq!(attempts[0]["durationMs"], 12);
    assert_eq!(attempts[1]["durationMs"], 18);
    let timeouts = timeouts.lock().unwrap();
    assert_eq!(timeouts.len(), 2);
    assert!(timeouts
        .iter()
        .all(|timeout| *timeout <= Duration::from_secs(3)));
    assert_eq!(observation.evidence["totalBudgetMs"], 3_000);
}

#[tokio::test]
async fn peer_tcp_attempts_ipv4_and_ipv6_concurrently() {
    let maximum = Arc::new(AtomicUsize::new(0));
    let engine = ProbeEngine::new("ops@example.com").with_peer_tcp_transport(ConcurrentPeerTcp {
        active: Arc::new(AtomicUsize::new(0)),
        maximum: Arc::clone(&maximum),
    });

    let observation = engine
        .check(&symbol_node(), &job(CheckKind::PeerTcp), Site::A)
        .await;

    assert!(matches!(observation.outcome, Outcome::Failure(_)));
    assert_eq!(maximum.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn normal_peer_tcp_keeps_dns_and_connect_inside_one_three_second_deadline() {
    let connect_timeout = Arc::new(Mutex::new(None));
    let engine = ProbeEngine::new("ops@example.com").with_peer_tcp_transport(DeadlinePeerTcp {
        connect_timeout: Arc::clone(&connect_timeout),
    });
    let started = std::time::Instant::now();

    let observation = engine
        .check(&symbol_node(), &job(CheckKind::PeerTcp), Site::A)
        .await;

    assert!(started.elapsed() < Duration::from_millis(3_250));
    assert!(connect_timeout.lock().unwrap().unwrap() < Duration::from_secs(3));
    assert!(matches!(observation.outcome, Outcome::Failure(_)));
}

#[tokio::test]
async fn detailed_peer_tcp_diagnostic_uses_the_separate_ten_second_budget() {
    let timeouts = Arc::new(Mutex::new(Vec::new()));
    let engine = ProbeEngine::new("ops@example.com")
        .with_http_transport(FakeHttp {
            responses: HashMap::from([(
                "/node/health".into(),
                serde_json::json!({"status": {"apiNode": "up", "db": "up"}}),
            )]),
            error: None,
        })
        .with_peer_tcp_transport(FakePeerTcp {
            addresses: vec![IpAddr::V4(Ipv4Addr::new(203, 0, 113, 10))],
            result: TcpConnectResult::Timeout,
            timeouts: Arc::clone(&timeouts),
        });
    let mut observation = Observation::new(
        "peer-retry",
        "symbol",
        Site::A,
        monitor_core::model::Dimension::Connectivity,
        Outcome::Failure("peer TCP connection failed".into()),
        chrono::Utc::now(),
    )
    .with_check_kind(CheckKind::PeerTcp)
    .with_evidence(serde_json::json!({"transport": "p2p"}));

    let node = symbol_node();
    engine
        .enrich_peer_tcp_diagnostic(&mut observation, &node, Some(&node))
        .await;

    let timeouts = timeouts.lock().unwrap();
    assert_eq!(timeouts.len(), 1);
    assert!(timeouts[0] <= Duration::from_secs(10));
    assert_eq!(
        observation.evidence["detailedDiagnostic"]["tcp"]["totalBudgetMs"],
        10_000
    );
    assert_eq!(
        observation.evidence["detailedDiagnostic"]["tcp"]["attempts"][0]["result"],
        "timeout"
    );
    assert_eq!(
        observation.evidence["detailedDiagnostic"]["restCorrelation"]["evidence"]["apiStatus"],
        "up"
    );
    assert!(matches!(
        observation.outcome,
        Outcome::Failure(ref message) if message.contains("diagnostic") && message.contains("timed out")
    ));
}

#[tokio::test]
async fn maps_incomplete_payloads_to_unknown_without_opening_an_incident_signal() {
    let engine = ProbeEngine::new("ops@example.com").with_http_transport(FakeHttp {
        responses: HashMap::from([(
            "/node/health".into(),
            serde_json::json!({"status": {"apiNode": "up"}}),
        )]),
        error: None,
    });
    let observation = engine
        .check(&symbol_node(), &job(CheckKind::Availability), Site::A)
        .await;
    assert_eq!(observation.check_kind, Some(CheckKind::Availability));
    assert!(matches!(observation.outcome, Outcome::Unknown(_)));
}

#[tokio::test]
async fn treats_nis1_booted_status_as_available_and_preserves_status_evidence() {
    let engine = ProbeEngine::new("ops@example.com").with_http_transport(FakeHttp {
        responses: HashMap::from([
            ("/heartbeat".into(), serde_json::json!({"code": 1})),
            ("/status".into(), serde_json::json!({"code": 5})),
        ]),
        error: None,
    });

    let observation = engine
        .check(&nis1_node(), &job(CheckKind::Availability), Site::A)
        .await;

    assert_eq!(observation.outcome, Outcome::Healthy);
    assert_eq!(observation.evidence["heartbeatCode"], 1);
    assert_eq!(observation.evidence["statusCode"], 5);
    assert_eq!(observation.evidence["directReachable"], true);
}

#[tokio::test]
async fn keeps_nis1_transport_failure_as_an_availability_failure() {
    let engine = ProbeEngine::new("ops@example.com").with_http_transport(FakeHttp {
        responses: HashMap::new(),
        error: Some(FakeError::Request),
    });

    let observation = engine
        .check(&nis1_node(), &job(CheckKind::Availability), Site::A)
        .await;

    assert!(matches!(observation.outcome, Outcome::Failure(_)));
    assert_eq!(observation.evidence["directReachable"], false);
}

#[tokio::test]
async fn maps_supported_transport_failure_to_failure() {
    let engine = ProbeEngine::new("ops@example.com").with_http_transport(FakeHttp {
        responses: HashMap::new(),
        error: Some(FakeError::Request),
    });
    let observation = engine
        .check(&symbol_node(), &job(CheckKind::Availability), Site::A)
        .await;
    assert!(matches!(observation.outcome, Outcome::Failure(_)));
}

#[tokio::test]
async fn schema_v2_keeps_the_latency_child_id_when_measurement_is_unavailable() {
    let engine = ProbeEngine::new("ops@example.com").with_http_transport(FakeHttp {
        responses: HashMap::new(),
        error: Some(FakeError::Request),
    });
    let observations = engine
        .check_all(&symbol_node(), &job(CheckKind::Availability), Site::A, 2)
        .await;
    assert_eq!(observations.len(), 2);
    assert_eq!(observations[1].job_id, "job-Availability:latency");
    assert_eq!(observations[1].check_kind, Some(CheckKind::Availability));
    assert!(matches!(observations[1].outcome, Outcome::Unknown(_)));
}

#[tokio::test]
async fn maps_diagnostic_transport_failure_to_unknown_not_dimension_failure() {
    let engine = ProbeEngine::new("ops@example.com").with_http_transport(FakeHttp {
        responses: HashMap::new(),
        error: Some(FakeError::Request),
    });
    let observation = engine
        .check(&symbol_node(), &job(CheckKind::Chain), Site::A)
        .await;
    assert!(matches!(observation.outcome, Outcome::Unknown(_)));
}

#[tokio::test]
async fn maps_missing_diagnostic_rest_resource_to_unsupported() {
    let engine = ProbeEngine::new("ops@example.com").with_http_transport(FakeHttp {
        responses: HashMap::new(),
        error: Some(FakeError::NotFound),
    });
    let observation = engine
        .check(&symbol_node(), &job(CheckKind::Identity), Site::A)
        .await;
    assert!(matches!(observation.outcome, Outcome::Unsupported(_)));
}

#[tokio::test]
async fn rejects_non_http_rest_scheme_before_transport() {
    let mut node = symbol_node();
    node.rest_scheme = Some("ftp".into());
    let engine = ProbeEngine::new("ops@example.com").with_http_transport(FakeHttp {
        responses: HashMap::new(),
        error: Some(FakeError::Request),
    });
    let observation = engine
        .check(&node, &job(CheckKind::Availability), Site::A)
        .await;
    assert!(matches!(observation.outcome, Outcome::Unknown(_)));
}

#[tokio::test]
async fn web_socket_check_is_healthy_only_after_a_valid_upgrade_handshake() {
    let mut node = symbol_node();
    node.host = "ws.example".into();
    node.rest_scheme = Some("wss".into());
    node.rest_port = Some(3001);
    let engine = ProbeEngine::new("ops@example.com").with_http_transport(FakeWebSocketHttp);

    let observation = engine
        .check(&node, &job(CheckKind::WebSocket), Site::A)
        .await;

    assert_eq!(observation.outcome, Outcome::Healthy);
    assert_eq!(observation.evidence["scheme"], "wss");
}

#[tokio::test]
async fn certificate_check_emits_remaining_days_after_a_valid_tls_handshake() {
    let mut node = symbol_node();
    node.host = "tls.example".into();
    node.rest_scheme = Some("https".into());
    node.rest_port = Some(3001);
    let engine = ProbeEngine::new("ops@example.com").with_http_transport(FakeCertificateHttp);

    let observation = engine
        .check(&node, &job(CheckKind::Certificate), Site::A)
        .await;

    assert!(matches!(
        observation.outcome,
        Outcome::Remaining(days) if (364..=365).contains(&days)
    ));
    assert!(observation.evidence["notAfter"]
        .as_str()
        .is_some_and(|value| value.ends_with('Z')));
}

#[tokio::test]
async fn supernode_check_uses_the_exact_registration_and_latest_complete_round() {
    let engine = ProbeEngine::new("ops@example.com").with_http_transport(FakeHttp {
        responses: HashMap::from([
            (
                "nem.io/supernode/api/nodes".into(),
                serde_json::json!([{
                    "endpoint": "http://nis.example:7890",
                    "id": 72768917,
                    "status": "active"
                }]),
            ),
            (
                "nem.io/supernode/api/node/72768917".into(),
                serde_json::json!({
                    "endpoint": "http://nis.example:7890",
                    "id": 72768917,
                    "lastResultsRound": 5053,
                    "status": "active"
                }),
            ),
            (
                "nem.io/supernode/api/node/72768917/results/rounds/5053/to/5053".into(),
                serde_json::json!([{
                    "balance": true,
                    "bandwidth": true,
                    "chainHeight": true,
                    "chainPart": true,
                    "computingPower": true,
                    "ping": true,
                    "responsiveness": true,
                    "roundId": 5053,
                    "timestamp": chrono::Utc::now() - chrono::Duration::hours(2),
                    "version": true
                }]),
            ),
        ]),
        error: None,
    });

    let observation = engine
        .check(&nis1_node(), &job(CheckKind::Supernode), Site::A)
        .await;

    assert_eq!(observation.outcome, Outcome::Healthy);
    assert_eq!(observation.evidence["provider"], "nem.io");
    assert_eq!(observation.evidence["supernodeId"], 72768917);
    assert_eq!(observation.evidence["roundId"], 5053);
    assert_eq!(observation.evidence["passed"], true);
}

#[tokio::test]
async fn supernode_check_reports_a_complete_failed_round_but_not_bad_external_data() {
    let registration = serde_json::json!([{
        "endpoint": "http://nis.example:7890",
        "id": 72768917,
        "status": "active"
    }]);
    let info = serde_json::json!({
        "endpoint": "http://nis.example:7890",
        "id": 72768917,
        "lastResultsRound": 5053,
        "status": "active"
    });
    let failed_round = serde_json::json!([{
        "balance": true,
        "bandwidth": true,
        "chainHeight": true,
        "chainPart": true,
        "computingPower": false,
        "ping": true,
        "responsiveness": true,
        "roundId": 5053,
        "timestamp": chrono::Utc::now() - chrono::Duration::hours(2),
        "version": false
    }]);
    let engine = ProbeEngine::new("ops@example.com").with_http_transport(FakeHttp {
        responses: HashMap::from([
            ("nem.io/supernode/api/nodes".into(), registration.clone()),
            ("nem.io/supernode/api/node/72768917".into(), info.clone()),
            (
                "nem.io/supernode/api/node/72768917/results/rounds/5053/to/5053".into(),
                failed_round,
            ),
        ]),
        error: None,
    });
    let failed = engine
        .check(&nis1_node(), &job(CheckKind::Supernode), Site::A)
        .await;
    assert!(matches!(failed.outcome, Outcome::Failure(_)));
    assert_eq!(
        failed.evidence["failedTests"],
        serde_json::json!(["computingPower", "version"])
    );

    let stale_engine = ProbeEngine::new("ops@example.com").with_http_transport(FakeHttp {
        responses: HashMap::from([
            ("nem.io/supernode/api/nodes".into(), registration),
            ("nem.io/supernode/api/node/72768917".into(), info),
            (
                "nem.io/supernode/api/node/72768917/results/rounds/5053/to/5053".into(),
                serde_json::json!([{
                    "balance": true,
                    "bandwidth": true,
                    "chainHeight": true,
                    "chainPart": true,
                    "computingPower": true,
                    "ping": true,
                    "responsiveness": true,
                    "roundId": 5053,
                    "timestamp": chrono::Utc::now() - chrono::Duration::hours(13),
                    "version": true
                }]),
            ),
        ]),
        error: None,
    });
    let stale = stale_engine
        .check(&nis1_node(), &job(CheckKind::Supernode), Site::A)
        .await;
    assert!(matches!(stale.outcome, Outcome::Unknown(_)));
}

#[tokio::test]
async fn supernode_check_marks_missing_or_inactive_registration_unsupported() {
    for registrations in [
        serde_json::json!([]),
        serde_json::json!([{
            "endpoint": "http://nis.example:7890",
            "id": 72768917,
            "status": "inactive"
        }]),
    ] {
        let engine = ProbeEngine::new("ops@example.com").with_http_transport(FakeHttp {
            responses: HashMap::from([("nem.io/supernode/api/nodes".into(), registrations)]),
            error: None,
        });
        let observation = engine
            .check(&nis1_node(), &job(CheckKind::Supernode), Site::A)
            .await;
        assert!(matches!(observation.outcome, Outcome::Unsupported(_)));
    }
}

#[tokio::test]
async fn supernode_provider_http_errors_are_unknown_not_unsupported() {
    let engine = ProbeEngine::new("ops@example.com").with_http_transport(FakeHttp {
        responses: HashMap::new(),
        error: Some(FakeError::NotFound),
    });
    let observation = engine
        .check(&nis1_node(), &job(CheckKind::Supernode), Site::A)
        .await;
    assert!(matches!(observation.outcome, Outcome::Unknown(_)));
}

#[tokio::test]
async fn supernode_registration_can_match_an_assigned_rest_alias() {
    let engine = ProbeEngine::new("ops@example.com").with_http_transport(FakeHttp {
        responses: HashMap::from([
            (
                "nem.io/supernode/api/nodes".into(),
                serde_json::json!([{
                    "endpoint": "http://registered.example:7890",
                    "id": 72768917,
                    "status": "active"
                }]),
            ),
            (
                "nem.io/supernode/api/node/72768917".into(),
                serde_json::json!({
                    "endpoint": "http://registered.example:7890",
                    "id": 72768917,
                    "lastResultsRound": 5053,
                    "status": "active"
                }),
            ),
            (
                "nem.io/supernode/api/node/72768917/results/rounds/5053/to/5053".into(),
                serde_json::json!([{
                    "balance": true, "bandwidth": true, "chainHeight": true,
                    "chainPart": true, "computingPower": true, "ping": true,
                    "responsiveness": true, "roundId": 5053,
                    "timestamp": chrono::Utc::now() - chrono::Duration::hours(2),
                    "version": true
                }]),
            ),
        ]),
        error: None,
    });
    let alias = Node::new(
        "nis",
        Network::Nis1Mainnet,
        NodeKind::Api,
        "registered.example",
    );
    let observations = engine
        .check_all_with_references(
            &nis1_node(),
            &job(CheckKind::Supernode),
            Site::A,
            2,
            &[alias],
        )
        .await;
    assert_eq!(observations[0].outcome, Outcome::Healthy);
}

#[tokio::test]
async fn reports_nis1_lag_against_the_observed_network_height() {
    let engine = ProbeEngine::new("ops@example.com").with_http_transport(FakeHttp {
        responses: HashMap::from([
            ("/chain/height".into(), serde_json::json!({"height": 420})),
            (
                "/node/active-peers/max-chain-height".into(),
                serde_json::json!({"height": 425}),
            ),
            ("/chain/score".into(), serde_json::json!({"score": "9000"})),
        ]),
        error: None,
    });
    let node = Node::new("nis", Network::Nis1Mainnet, NodeKind::Api, "example.com");
    let observation = engine.check(&node, &job(CheckKind::Chain), Site::A).await;
    assert_eq!(observation.outcome, Outcome::LagBlocks(5));
    assert_eq!(observation.evidence["height"], 420);
    assert_eq!(observation.evidence["networkHeight"], 425);
}

#[tokio::test]
async fn keeps_symbol_chain_unknown_without_a_reference_sample() {
    let engine = ProbeEngine::new("ops@example.com").with_http_transport(FakeHttp {
        responses: HashMap::from([(
            "/chain/info".into(),
            serde_json::json!({
                "height": 1234,
                "scoreHigh": 2,
                "scoreLow": 3,
                "latestFinalizedBlock": {"height": 1200, "finalizationEpoch": 77}
            }),
        )]),
        error: None,
    });
    let observation = engine
        .check(&symbol_node(), &job(CheckKind::Chain), Site::A)
        .await;
    assert!(matches!(observation.outcome, Outcome::Unknown(_)));
    assert_eq!(observation.evidence["height"], 1234);
    assert_eq!(observation.evidence["finalizedHeight"], 1200);
}

#[tokio::test]
async fn schema_v2_symbol_chain_uses_a_strict_majority_reference_median() {
    let chain = |height| {
        serde_json::json!({
            "height": height,
            "scoreHigh": 2,
            "scoreLow": 3,
            "latestFinalizedBlock": {"height": height - 1, "finalizationEpoch": 77}
        })
    };
    let engine = ProbeEngine::new("ops@example.com").with_http_transport(FakeHttp {
        responses: HashMap::from([
            ("local.example/chain/info".into(), chain(100)),
            ("reference-0.example/chain/info".into(), chain(102)),
            ("reference-1.example/chain/info".into(), chain(103)),
            ("reference-2.example/chain/info".into(), chain(104)),
        ]),
        error: None,
    });
    let local = Node::new(
        "symbol",
        Network::SymbolMainnet,
        NodeKind::Api,
        "local.example",
    );
    let references = (0..5)
        .map(|index| {
            Node::new(
                format!("reference-{index}"),
                Network::SymbolMainnet,
                NodeKind::Api,
                format!("reference-{index}.example"),
            )
        })
        .collect::<Vec<_>>();

    let observations = engine
        .check_all_with_references(&local, &job(CheckKind::Chain), Site::A, 2, &references)
        .await;

    assert_eq!(observations[0].outcome, Outcome::LagBlocks(3));
    assert_eq!(observations[0].evidence["referenceExpected"], 5);
    assert_eq!(observations[0].evidence["referenceResponding"], 3);
    assert_eq!(observations[0].evidence["referenceMedianHeight"], 103);
}

#[tokio::test]
async fn schema_v2_reuses_bounded_reference_samples_across_subject_nodes() {
    let chain = |height| {
        serde_json::json!({
            "height": height,
            "scoreHigh": 2,
            "scoreLow": 3,
            "latestFinalizedBlock": {"height": height - 1, "finalizationEpoch": 77}
        })
    };
    let hash = "A".repeat(64);
    let counts = Arc::new(Mutex::new(HashMap::new()));
    let transport = CountingHttp {
        responses: Arc::new(HashMap::from([
            ("subject-a.example/chain/info".into(), chain(100)),
            ("subject-b.example/chain/info".into(), chain(100)),
            ("reference.example/chain/info".into(), chain(100)),
            (
                "subject-a.example/blocks/99".into(),
                serde_json::json!({"block": {"stateHash": hash}}),
            ),
            (
                "subject-b.example/blocks/99".into(),
                serde_json::json!({"block": {"stateHash": hash}}),
            ),
            (
                "reference.example/blocks/99".into(),
                serde_json::json!({"block": {"stateHash": hash}}),
            ),
        ])),
        counts: Arc::clone(&counts),
    };
    let engine = ProbeEngine::new("ops@example.com").with_http_transport(transport);
    let reference = Node::new(
        "reference",
        Network::SymbolMainnet,
        NodeKind::Api,
        "reference.example",
    );
    for (id, host) in [
        ("subject-a", "subject-a.example"),
        ("subject-b", "subject-b.example"),
    ] {
        let subject = Node::new(id, Network::SymbolMainnet, NodeKind::Api, host);
        let chain_observations = engine
            .check_all_with_references(
                &subject,
                &job(CheckKind::Chain),
                Site::A,
                2,
                std::slice::from_ref(&reference),
            )
            .await;
        assert_eq!(chain_observations[0].outcome, Outcome::LagBlocks(0));
        let observations = engine
            .check_all_with_references(
                &subject,
                &job(CheckKind::StateHash),
                Site::A,
                2,
                std::slice::from_ref(&reference),
            )
            .await;
        assert_eq!(observations[0].outcome, Outcome::Healthy);
    }
    let counts = counts.lock().unwrap();
    assert_eq!(counts["subject-a.example/chain/info"], 1);
    assert_eq!(counts["subject-b.example/chain/info"], 1);
    assert_eq!(counts["reference.example/chain/info"], 1);
    assert_eq!(counts["reference.example/blocks/99"], 1);
}

#[tokio::test]
async fn symbol_chain_cache_does_not_evict_a_shared_reference_under_subject_churn() {
    let chain = serde_json::json!({
        "height": 100,
        "scoreHigh": 2,
        "scoreLow": 3,
        "latestFinalizedBlock": {"height": 99, "finalizationEpoch": 77}
    });
    let counts = Arc::new(Mutex::new(HashMap::new()));
    let mut responses = HashMap::from([("reference.example/chain/info".into(), chain.clone())]);
    let subjects = (0..40)
        .map(|index| {
            let host = format!("subject-{index:03}.example");
            responses.insert(format!("{host}/chain/info"), chain.clone());
            Node::new(
                format!("subject-{index:03}"),
                Network::SymbolMainnet,
                NodeKind::Api,
                host,
            )
        })
        .collect::<Vec<_>>();
    let engine = ProbeEngine::new("ops@example.com").with_http_transport(CountingHttp {
        responses: Arc::new(responses),
        counts: Arc::clone(&counts),
    });
    let reference = Node::new(
        "reference",
        Network::SymbolMainnet,
        NodeKind::Api,
        "reference.example",
    );

    for subject in &subjects {
        let observations = engine
            .check_all_with_references(
                subject,
                &job(CheckKind::Chain),
                Site::A,
                2,
                std::slice::from_ref(&reference),
            )
            .await;
        assert_eq!(observations[0].outcome, Outcome::LagBlocks(0));
    }

    assert_eq!(counts.lock().unwrap()["reference.example/chain/info"], 1);
}

#[tokio::test]
async fn symbol_chain_cache_does_not_serialize_distinct_hosts() {
    let engine = ProbeEngine::new("ops@example.com").with_http_transport(BarrierHttp {
        barrier: Arc::new(Barrier::new(2)),
    });
    let first = Node::new(
        "first",
        Network::SymbolMainnet,
        NodeKind::Api,
        "first.example",
    );
    let second = Node::new(
        "second",
        Network::SymbolMainnet,
        NodeKind::Api,
        "second.example",
    );
    let first_job = job(CheckKind::Chain);
    let second_job = job(CheckKind::Chain);

    tokio::time::timeout(Duration::from_secs(1), async {
        tokio::join!(
            engine.check_all(&first, &first_job, Site::A, 2),
            engine.check_all(&second, &second_job, Site::A, 2),
        )
    })
    .await
    .expect("distinct cache keys must not share an HTTP critical section");
}

#[tokio::test]
async fn schema_v2_symbol_chain_stays_unknown_without_a_strict_reference_majority() {
    let chain = |height| {
        serde_json::json!({
            "height": height,
            "scoreHigh": 2,
            "scoreLow": 3,
            "latestFinalizedBlock": {"height": height - 1, "finalizationEpoch": 77}
        })
    };
    let engine = ProbeEngine::new("ops@example.com").with_http_transport(FakeHttp {
        responses: HashMap::from([
            ("local.example/chain/info".into(), chain(100)),
            ("reference-0.example/chain/info".into(), chain(102)),
            ("reference-1.example/chain/info".into(), chain(103)),
        ]),
        error: None,
    });
    let local = Node::new(
        "symbol",
        Network::SymbolMainnet,
        NodeKind::Api,
        "local.example",
    );
    let references = (0..5)
        .map(|index| {
            Node::new(
                format!("reference-{index}"),
                Network::SymbolMainnet,
                NodeKind::Api,
                format!("reference-{index}.example"),
            )
        })
        .collect::<Vec<_>>();

    let observations = engine
        .check_all_with_references(&local, &job(CheckKind::Chain), Site::A, 2, &references)
        .await;

    assert!(matches!(observations[0].outcome, Outcome::Unknown(_)));
    assert_eq!(observations[0].evidence["referenceExpected"], 5);
    assert_eq!(observations[0].evidence["referenceResponding"], 2);
}

#[tokio::test]
async fn schema_v2_finalization_emits_stalled_minutes_from_durable_history() {
    let history = FinalizationHistory::in_memory().await.unwrap();
    history
        .evaluate(
            "symbol",
            FinalizationSample {
                chain_height: 1_000,
                finalized_height: 900,
                reference_finalized_height: 950,
            },
            chrono::Utc::now() - chrono::Duration::minutes(31),
        )
        .await
        .unwrap();
    let chain = |height, finalized_height| {
        serde_json::json!({
            "height": height,
            "scoreHigh": 2,
            "scoreLow": 3,
            "latestFinalizedBlock": {"height": finalized_height, "finalizationEpoch": 77}
        })
    };
    let engine = ProbeEngine::new("ops@example.com")
        .with_http_transport(FakeHttp {
            responses: HashMap::from([
                ("local.example/chain/info".into(), chain(1_020, 900)),
                ("reference-0.example/chain/info".into(), chain(1_022, 951)),
                ("reference-1.example/chain/info".into(), chain(1_023, 952)),
                ("reference-2.example/chain/info".into(), chain(1_024, 953)),
            ]),
            error: None,
        })
        .with_finalization_history(history);
    let local = Node::new(
        "symbol",
        Network::SymbolMainnet,
        NodeKind::Api,
        "local.example",
    );
    let references = (0..5)
        .map(|index| {
            Node::new(
                format!("reference-{index}"),
                Network::SymbolMainnet,
                NodeKind::Api,
                format!("reference-{index}.example"),
            )
        })
        .collect::<Vec<_>>();

    let observations = engine
        .check_all_with_references(
            &local,
            &job(CheckKind::Finalization),
            Site::A,
            2,
            &references,
        )
        .await;

    assert!(matches!(
        observations[0].outcome,
        Outcome::FinalizationStalledMinutes(minutes) if minutes >= 30
    ));
    assert_eq!(observations[0].evidence["referenceExpected"], 5);
    assert_eq!(observations[0].evidence["referenceResponding"], 3);
    assert_eq!(
        observations[0].evidence["referenceMedianFinalizedHeight"],
        952
    );
}

#[tokio::test]
async fn schema_v2_finalization_emits_fail_closed_network_reference_evidence() {
    let history = FinalizationHistory::in_memory().await.unwrap();
    let started = chrono::Utc::now() - chrono::Duration::minutes(61);
    for (host, finalized_height) in [
        ("reference-0.example", 900),
        ("reference-1.example", 900),
        ("reference-2.example", 900),
    ] {
        history
            .evaluate(
                &format!("https://{host}:3001"),
                FinalizationSample {
                    chain_height: 1_000,
                    finalized_height,
                    reference_finalized_height: 1_000,
                },
                started,
            )
            .await
            .unwrap();
    }
    let chain = |height, finalized_height| {
        serde_json::json!({
            "height": height,
            "scoreHigh": 2,
            "scoreLow": 3,
            "latestFinalizedBlock": {"height": finalized_height, "finalizationEpoch": 77}
        })
    };
    let engine = ProbeEngine::new("ops@example.com")
        .with_http_transport(FakeHttp {
            responses: HashMap::from([
                ("local.example/chain/info".into(), chain(1_030, 901)),
                ("reference-0.example/chain/info".into(), chain(1_030, 900)),
                ("reference-1.example/chain/info".into(), chain(1_031, 900)),
                ("reference-2.example/chain/info".into(), chain(1_032, 900)),
            ]),
            error: None,
        })
        .with_finalization_history(history);
    let local = Node::new(
        "subject",
        Network::SymbolMainnet,
        NodeKind::Api,
        "local.example",
    );
    let references = (0..3)
        .map(|index| {
            let mut reference = Node::new(
                "subject",
                Network::SymbolMainnet,
                NodeKind::Api,
                format!("reference-{index}.example"),
            );
            reference.display_name = "reference".into();
            reference
        })
        .collect::<Vec<_>>();

    let observations = engine
        .check_all_with_references(
            &local,
            &job(CheckKind::Finalization),
            Site::A,
            2,
            &references,
        )
        .await;

    assert_eq!(
        observations[0].evidence["networkFinalization"],
        serde_json::json!({
            "expectedReferences": 3,
            "usableReferences": 3,
            "stalledReferences": 3,
            "stalledMinutes": 61
        })
    );
}

#[tokio::test]
async fn valid_empty_peers_is_zero_but_malformed_peers_is_unknown() {
    let empty = ProbeEngine::new("ops@example.com").with_http_transport(FakeHttp {
        responses: HashMap::from([("/node/peers".into(), serde_json::json!([]))]),
        error: None,
    });
    let observation = empty
        .check(&symbol_node(), &job(CheckKind::Peers), Site::A)
        .await;
    assert_eq!(observation.outcome, Outcome::ZeroPeers);

    let malformed = ProbeEngine::new("ops@example.com").with_http_transport(FakeHttp {
        responses: HashMap::from([("/node/peers".into(), serde_json::json!({}))]),
        error: None,
    });
    let observation = malformed
        .check(&symbol_node(), &job(CheckKind::Peers), Site::A)
        .await;
    assert!(matches!(observation.outcome, Outcome::Unknown(_)));
}

#[tokio::test]
async fn peer_only_symbol_voting_uses_the_reference_api() {
    let engine = ProbeEngine::new("ops@example.com")
        .with_http_transport(FakeHttp {
            responses: HashMap::from([
                (
                    "/chain/info".into(),
                    serde_json::json!({
                        "latestFinalizedBlock": {"finalizationEpoch": 150}
                    }),
                ),
                (
                    "/accounts/PUBLICKEY".into(),
                    serde_json::json!({
                        "account": {"supplementalPublicKeys": {"voting": {"publicKeys": [
                            {"startEpoch": 100, "endEpoch": 200}
                        ]}}}
                    }),
                ),
            ]),
            error: None,
        })
        .with_symbol_reference(symbol_node());
    let mut peer = Node::new(
        "peer",
        Network::SymbolMainnet,
        NodeKind::PeerOnly,
        "peer.example",
    );
    peer.public_key = Some("PUBLICKEY".into());
    let observation = engine.check(&peer, &job(CheckKind::Voting), Site::A).await;
    assert_eq!(observation.outcome, Outcome::Remaining(50));
}

#[tokio::test]
async fn schema_v2_voting_falls_back_to_the_next_assigned_reference_api() {
    let engine = ProbeEngine::new("ops@example.com").with_http_transport(FakeHttp {
        responses: HashMap::from([
            (
                "reference-1.example/chain/info".into(),
                serde_json::json!({
                    "latestFinalizedBlock": {"finalizationEpoch": 150}
                }),
            ),
            (
                "reference-1.example/accounts/PUBLICKEY".into(),
                serde_json::json!({
                    "account": {"supplementalPublicKeys": {"voting": {"publicKeys": [
                        {"startEpoch": 100, "endEpoch": 200}
                    ]}}}
                }),
            ),
        ]),
        error: None,
    });
    let mut primary = Node::new(
        "subject",
        Network::SymbolMainnet,
        NodeKind::Api,
        "reference-0.example",
    );
    primary.public_key = Some("PUBLICKEY".into());
    let references = ["reference-1.example", "reference-2.example"]
        .into_iter()
        .map(|host| Node::new("subject", Network::SymbolMainnet, NodeKind::Api, host))
        .collect::<Vec<_>>();

    let observations = engine
        .check_all_with_references(&primary, &job(CheckKind::Voting), Site::A, 2, &references)
        .await;

    assert_eq!(observations[0].outcome, Outcome::Remaining(50));
    assert_eq!(
        observations[0].evidence["referenceHost"],
        "reference-1.example"
    );
}

#[tokio::test]
async fn rest_private_and_peer_only_information_gaps_are_not_failures() {
    let mut rest_private = symbol_node();
    rest_private.rest_scheme = None;
    rest_private.rest_port = None;
    let engine = ProbeEngine::new("ops@example.com");
    let observation = engine
        .check(&rest_private, &job(CheckKind::Chain), Site::A)
        .await;
    assert!(matches!(observation.outcome, Outcome::Unsupported(_)));

    let mut missing_port = symbol_node();
    missing_port.rest_port = None;
    let engine_with_failing_transport =
        ProbeEngine::new("ops@example.com").with_http_transport(FakeHttp {
            responses: HashMap::new(),
            error: Some(FakeError::Request),
        });
    let observation = engine_with_failing_transport
        .check(&missing_port, &job(CheckKind::Availability), Site::A)
        .await;
    assert!(matches!(observation.outcome, Outcome::Unsupported(_)));

    let mut peer = Node::new(
        "peer",
        Network::SymbolMainnet,
        NodeKind::PeerOnly,
        "peer.example",
    );
    peer.public_key = Some("PUBLICKEY".into());
    let observation = engine.check(&peer, &job(CheckKind::Voting), Site::A).await;
    assert!(matches!(observation.outcome, Outcome::Unknown(_)));
}

#[tokio::test]
async fn symbol_p2p_port_is_never_used_for_http() {
    let counts = Arc::new(Mutex::new(HashMap::new()));
    let transport = CountingHttp {
        responses: Arc::new(HashMap::new()),
        counts: counts.clone(),
    };
    let mut node = symbol_node();
    node.rest_scheme = Some("https".into());
    node.rest_port = Some(7900);

    let observation = ProbeEngine::new("ops@example.com")
        .with_http_transport(transport)
        .check(&node, &job(CheckKind::Availability), Site::A)
        .await;

    assert!(matches!(observation.outcome, Outcome::Unknown(_)));
    assert!(
        counts.lock().unwrap().is_empty(),
        "HTTP transport must not run"
    );
}

#[tokio::test]
async fn malformed_harvest_and_missing_voting_range_are_unknown() {
    let harvest = ProbeEngine::new("ops@example.com").with_http_transport(FakeHttp {
        responses: HashMap::from([("/node/unlockedaccount".into(), serde_json::json!({}))]),
        error: None,
    });
    let observation = harvest
        .check(&symbol_node(), &job(CheckKind::Harvest), Site::A)
        .await;
    assert!(matches!(observation.outcome, Outcome::Unknown(_)));

    let voting = ProbeEngine::new("ops@example.com").with_http_transport(FakeHttp {
        responses: HashMap::from([
            (
                "/chain/info".into(),
                serde_json::json!({
                    "latestFinalizedBlock": {"finalizationEpoch": 150}
                }),
            ),
            (
                "/accounts/PUBLICKEY".into(),
                serde_json::json!({"account": {}}),
            ),
        ]),
        error: None,
    });
    let mut node = symbol_node();
    node.public_key = Some("PUBLICKEY".into());
    let observation = voting.check(&node, &job(CheckKind::Voting), Site::A).await;
    assert!(matches!(observation.outcome, Outcome::Unknown(_)));
}

#[tokio::test]
async fn local_state_hash_without_a_reference_is_unknown_with_raw_evidence() {
    let engine = ProbeEngine::new("ops@example.com").with_http_transport(FakeHttp {
        responses: HashMap::from([
            (
                "/chain/info".into(),
                serde_json::json!({
                    "height": 1234,
                    "scoreHigh": 2,
                    "scoreLow": 3,
                    "latestFinalizedBlock": {"height": 1200, "finalizationEpoch": 77}
                }),
            ),
            (
                "/blocks/1234".into(),
                serde_json::json!({"block": {"stateHash": "A".repeat(64)}}),
            ),
        ]),
        error: None,
    });
    let observation = engine
        .check(&symbol_node(), &job(CheckKind::StateHash), Site::A)
        .await;
    assert!(matches!(observation.outcome, Outcome::Unknown(_)));
    assert_eq!(observation.evidence["stateHash"], "A".repeat(64));
}

#[tokio::test]
async fn schema_v2_state_hash_compares_a_common_height_against_reference_quorum() {
    let chain = |height| {
        serde_json::json!({
            "height": height,
            "scoreHigh": 2,
            "scoreLow": 3,
            "latestFinalizedBlock": {"height": height - 1, "finalizationEpoch": 77}
        })
    };
    let engine = ProbeEngine::new("ops@example.com").with_http_transport(FakeHttp {
        responses: HashMap::from([
            ("local.example/chain/info".into(), chain(105)),
            (
                "local.example/blocks/101".into(),
                serde_json::json!({"block": {"stateHash": "A".repeat(64)}}),
            ),
            ("reference-0.example/chain/info".into(), chain(102)),
            ("reference-1.example/chain/info".into(), chain(103)),
            ("reference-2.example/chain/info".into(), chain(104)),
            (
                "reference-0.example/blocks/101".into(),
                serde_json::json!({"block": {"stateHash": "B".repeat(64)}}),
            ),
            (
                "reference-1.example/blocks/101".into(),
                serde_json::json!({"block": {"stateHash": "B".repeat(64)}}),
            ),
            (
                "reference-2.example/blocks/101".into(),
                serde_json::json!({"block": {"stateHash": "B".repeat(64)}}),
            ),
        ]),
        error: None,
    });
    let local = Node::new(
        "symbol",
        Network::SymbolMainnet,
        NodeKind::Api,
        "local.example",
    );
    let references = (0..5)
        .map(|index| {
            Node::new(
                format!("reference-{index}"),
                Network::SymbolMainnet,
                NodeKind::Api,
                format!("reference-{index}.example"),
            )
        })
        .collect::<Vec<_>>();

    let observations = engine
        .check_all_with_references(&local, &job(CheckKind::StateHash), Site::A, 2, &references)
        .await;

    assert_eq!(
        observations[0].outcome,
        Outcome::Mismatch(format!("{} != {}", "A".repeat(64), "B".repeat(64)))
    );
    assert_eq!(observations[0].evidence["comparisonHeight"], 101);
    assert_eq!(observations[0].evidence["localFinalizedHeight"], 104);
    assert_eq!(
        observations[0].evidence["referenceMinimumFinalizedHeight"],
        101
    );
    assert_eq!(observations[0].evidence["referenceExpected"], 5);
    assert_eq!(observations[0].evidence["referenceConsensusCount"], 3);
}

#[tokio::test]
async fn schema_v2_state_hash_ignores_unfinalized_tip_forks() {
    let chain = |height, finalized_height| {
        serde_json::json!({
            "height": height,
            "scoreHigh": 2,
            "scoreLow": 3,
            "latestFinalizedBlock": {"height": finalized_height, "finalizationEpoch": 77}
        })
    };
    let finalized_hash = "A".repeat(64);
    let engine = ProbeEngine::new("ops@example.com").with_http_transport(FakeHttp {
        responses: HashMap::from([
            ("local.example/chain/info".into(), chain(105, 100)),
            (
                "local.example/blocks/100".into(),
                serde_json::json!({"block": {"stateHash": finalized_hash}}),
            ),
            ("reference-0.example/chain/info".into(), chain(102, 100)),
            ("reference-1.example/chain/info".into(), chain(103, 101)),
            ("reference-2.example/chain/info".into(), chain(104, 102)),
            (
                "reference-0.example/blocks/100".into(),
                serde_json::json!({"block": {"stateHash": finalized_hash}}),
            ),
            (
                "reference-1.example/blocks/100".into(),
                serde_json::json!({"block": {"stateHash": finalized_hash}}),
            ),
            (
                "reference-2.example/blocks/100".into(),
                serde_json::json!({"block": {"stateHash": finalized_hash}}),
            ),
        ]),
        error: None,
    });
    let local = Node::new(
        "symbol",
        Network::SymbolMainnet,
        NodeKind::Api,
        "local.example",
    );
    let references = (0..3)
        .map(|index| {
            Node::new(
                format!("reference-{index}"),
                Network::SymbolMainnet,
                NodeKind::Api,
                format!("reference-{index}.example"),
            )
        })
        .collect::<Vec<_>>();

    let observations = engine
        .check_all_with_references(&local, &job(CheckKind::StateHash), Site::A, 2, &references)
        .await;

    assert_eq!(observations[0].outcome, Outcome::Healthy);
    assert_eq!(observations[0].evidence["comparisonHeight"], 100);
    assert_eq!(observations[0].evidence["referenceConsensusCount"], 3);
}

#[tokio::test]
async fn schema_v2_state_hash_does_not_fan_out_for_a_lagging_finalized_height() {
    let chain = |height| {
        serde_json::json!({
            "height": height,
            "scoreHigh": 2,
            "scoreLow": 3,
            "latestFinalizedBlock": {"height": height - 1, "finalizationEpoch": 77}
        })
    };
    let engine = ProbeEngine::new("ops@example.com").with_http_transport(FakeHttp {
        responses: HashMap::from([
            ("local.example/chain/info".into(), chain(100)),
            ("reference-0.example/chain/info".into(), chain(102)),
            ("reference-1.example/chain/info".into(), chain(103)),
            ("reference-2.example/chain/info".into(), chain(104)),
        ]),
        error: None,
    });
    let local = Node::new(
        "symbol",
        Network::SymbolMainnet,
        NodeKind::Api,
        "local.example",
    );
    let references = (0..3)
        .map(|index| {
            Node::new(
                format!("reference-{index}"),
                Network::SymbolMainnet,
                NodeKind::Api,
                format!("reference-{index}.example"),
            )
        })
        .collect::<Vec<_>>();

    let observations = engine
        .check_all_with_references(&local, &job(CheckKind::StateHash), Site::A, 2, &references)
        .await;

    assert_eq!(
        observations[0].outcome,
        Outcome::Unknown("local finalization is below reference comparison height".into())
    );
    assert_eq!(observations[0].evidence["localHeight"], 100);
    assert_eq!(observations[0].evidence["localFinalizedHeight"], 99);
    assert_eq!(observations[0].evidence["comparisonHeight"], 101);
}

#[tokio::test]
async fn incomplete_identity_payload_is_unknown_not_connectivity_healthy() {
    let engine = ProbeEngine::new("ops@example.com").with_http_transport(FakeHttp {
        responses: HashMap::from([("/node/info".into(), serde_json::json!(null))]),
        error: None,
    });
    let observation = engine
        .check(&symbol_node(), &job(CheckKind::Identity), Site::A)
        .await;
    assert!(matches!(observation.outcome, Outcome::Unknown(_)));
}

#[tokio::test]
async fn schema_v2_emits_stable_connectivity_and_latency_observations_from_one_request() {
    let engine = ProbeEngine::new("ops@example.com").with_http_transport(FakeHttp {
        responses: HashMap::from([(
            "/node/health".into(),
            serde_json::json!({"status": {"apiNode": "up", "db": "up"}}),
        )]),
        error: None,
    });
    let scheduled = job(CheckKind::Availability);
    let v1 = engine
        .check_all(&symbol_node(), &scheduled, Site::A, 1)
        .await;
    assert_eq!(v1.len(), 1);
    assert_eq!(v1[0].job_id, scheduled.job_id);

    let v2 = engine
        .check_all(&symbol_node(), &scheduled, Site::A, 2)
        .await;
    assert_eq!(v2.len(), 2);
    assert_eq!(v2[0].job_id, scheduled.job_id);
    assert_eq!(v2[1].job_id, format!("{}:latency", scheduled.job_id));
    assert_eq!(v2[1].dimension, monitor_core::model::Dimension::Latency);
    assert_eq!(v2[1].outcome, Outcome::LatencyMillis(2_345));
    let replay = engine
        .check_all(&symbol_node(), &scheduled, Site::A, 2)
        .await;
    assert_eq!(
        replay
            .iter()
            .map(|observation| observation.job_id.as_str())
            .collect::<Vec<_>>(),
        v2.iter()
            .map(|observation| observation.job_id.as_str())
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn supernode_full_first_page_without_target_is_unknown() {
    let rows = (0..100)
        .map(|i| {
            serde_json::json!({
                "endpoint":format!("http://other-{i}.example:7890"),
                "id":i+1,"status":"stale"
            })
        })
        .collect::<Vec<_>>();
    let engine = ProbeEngine::new("ops@example.com").with_http_transport(FakeHttp {
        responses: HashMap::from([("nem.io/supernode/api/nodes".into(), Value::Array(rows))]),
        error: None,
    });
    let observation = engine
        .check(&nis1_node(), &job(CheckKind::Supernode), Site::A)
        .await;
    assert!(matches!(observation.outcome, Outcome::Unknown(_)));
}
