//! Opt-in community mode. Credentials and traffic reservations are local and durable.
use std::{
    env,
    error::Error,
    fs,
    io::{self, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use chrono::{DateTime, Utc};
use monitor_core::{
    community::{
        CapacityLimits, CommunityObservation, EnrollChallenge, EnrollRequest, HeartbeatRequest,
        Lease, LeaseResponse, ResultBatch, RuntimeCapacity, TokenRequest, TokenResponse,
    },
    model::Site,
    scheduler::ScheduledJob,
};
use reqwest::{redirect::Policy, Client, Method, StatusCode, Url};
use ring::{
    rand::SystemRandom,
    signature::{Ed25519KeyPair, KeyPair},
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sqlx::{sqlite::SqlitePoolOptions, SqlitePool};
use tokio::time::sleep;
use uuid::Uuid;

use crate::{
    engine::{MeasurementBudget, ProbeEngine, ProbeError},
    history::FinalizationHistory,
    limiter::Limits,
    spool::Spool,
};

type Result<T> = std::result::Result<T, Box<dyn Error>>;
const PREFIX: &str = "/internal/v2/community";
const VERSION: &str = env!("CARGO_PKG_VERSION");
const CENTRAL_RESPONSE_LIMIT: usize = 512 * 1024;
const CENTRAL_REQUEST_RESERVATION: u64 = 2 * 1024 * 1024;
const CHECK_RESERVATION: u64 = 4 * 1024 * 1024 + 131072;
const QUEUE_RETENTION_SECS: i64 = 24 * 3600;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LocalConfig {
    #[serde(default = "default_cpu_cores")]
    pub cpu_cores: f64,
    #[serde(default)]
    pub limits: CapacityLimits,
    #[serde(default = "default_cpu_percent")]
    pub max_cpu_percent: u8,
    #[serde(default = "default_memory_bytes")]
    pub max_memory_bytes: u64,
}
fn default_cpu_cores() -> f64 {
    0.1
}
fn default_cpu_percent() -> u8 {
    70
}
fn default_memory_bytes() -> u64 {
    256 * 1024 * 1024
}
impl Default for LocalConfig {
    fn default() -> Self {
        Self {
            cpu_cores: default_cpu_cores(),
            limits: CapacityLimits::default(),
            max_cpu_percent: default_cpu_percent(),
            max_memory_bytes: default_memory_bytes(),
        }
    }
}
impl LocalConfig {
    fn validate(&self) -> Result<()> {
        self.limits
            .validate()
            .map_err(|e| bad_input(format!("{e:?}")))?;
        if !self.cpu_cores.is_finite()
            || self.cpu_cores <= 0.0
            || self.max_cpu_percent == 0
            || self.max_cpu_percent > 100
            || self.max_memory_bytes < 32 * 1024 * 1024
        {
            return Err(bad_input("invalid CPU or memory limit").into());
        }
        Ok(())
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LocalState {
    probe_id: Option<String>,
    #[serde(default)]
    left: bool,
    #[serde(default)]
    revoked: bool,
    #[serde(default)]
    identity_nonce: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct QueuedResult {
    expires_at: DateTime<Utc>,
    batch: ResultBatch,
}

#[derive(Debug)]
struct Identity {
    key: Ed25519KeyPair,
    public_key: String,
}
impl Identity {
    fn load_or_create(path: &Path) -> Result<Self> {
        let pkcs8 = if path.exists() {
            fs::read(path)?
        } else {
            let random = SystemRandom::new();
            let key = Ed25519KeyPair::generate_pkcs8(&random)
                .map_err(|_| bad_input("Ed25519 key generation failed"))?;
            atomic_write_private(path, key.as_ref())?;
            key.as_ref().to_vec()
        };
        let key = Ed25519KeyPair::from_pkcs8(&pkcs8)
            .map_err(|_| bad_input("invalid local Ed25519 key"))?;
        Ok(Self {
            public_key: URL_SAFE_NO_PAD.encode(key.public_key().as_ref()),
            key,
        })
    }
    fn token_request(&self, challenge: &EnrollChallenge) -> TokenRequest {
        let message = format!(
            "harvest-scope-community-v2\n{}\n{}",
            challenge.probe_id, challenge.challenge
        );
        TokenRequest {
            probe_id: challenge.probe_id.clone(),
            challenge: challenge.challenge.clone(),
            signature: URL_SAFE_NO_PAD.encode(self.key.sign(message.as_bytes()).as_ref()),
        }
    }
}

#[derive(Debug, Clone)]
struct Ledger {
    db: SqlitePool,
    daily_limit: u64,
}
impl Ledger {
    async fn open(path: &Path, daily_limit: u64) -> Result<Self> {
        let db = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(&format!("sqlite://{}?mode=rwc", path.display()))
            .await?;
        sqlx::query("CREATE TABLE IF NOT EXISTS traffic(day TEXT PRIMARY KEY, reserved_bytes INTEGER NOT NULL CHECK(reserved_bytes >= 0))")
            .execute(&db).await?;
        sqlx::query("DELETE FROM traffic WHERE day < date('now','-7 days')")
            .execute(&db)
            .await?;
        Ok(Self { db, daily_limit })
    }
    async fn used(&self) -> Result<u64> {
        let day = Utc::now().format("%Y-%m-%d").to_string();
        let used: Option<i64> =
            sqlx::query_scalar("SELECT reserved_bytes FROM traffic WHERE day = ?")
                .bind(day)
                .fetch_optional(&self.db)
                .await?;
        Ok(used.unwrap_or(0).max(0) as u64)
    }
    async fn reserve(&self, amount: u64) -> Result<bool> {
        let amount_i64 = i64::try_from(amount)?;
        let limit_i64 = i64::try_from(self.daily_limit)?;
        let day = Utc::now().format("%Y-%m-%d").to_string();
        let mut tx = self.db.begin().await?;
        sqlx::query(
            "INSERT INTO traffic(day,reserved_bytes) VALUES(?,0) ON CONFLICT(day) DO NOTHING",
        )
        .bind(&day)
        .execute(&mut *tx)
        .await?;
        let updated = sqlx::query("UPDATE traffic SET reserved_bytes = reserved_bytes + ? WHERE day = ? AND reserved_bytes <= ? - ?")
            .bind(amount_i64).bind(&day).bind(limit_i64).bind(amount_i64)
            .execute(&mut *tx).await?.rows_affected() == 1;
        tx.commit().await?;
        Ok(updated)
    }
    async fn refund(&self, day: &str, amount: u64) -> Result<()> {
        sqlx::query("UPDATE traffic SET reserved_bytes=max(0,reserved_bytes-?) WHERE day=?")
            .bind(i64::try_from(amount)?)
            .bind(day)
            .execute(&self.db)
            .await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl MeasurementBudget for Ledger {
    async fn reserve(&self, amount: u64) -> std::result::Result<String, ProbeError> {
        let day = Utc::now().format("%Y-%m-%d").to_string();
        // A UTC rollover while awaiting SQLite conservatively keeps the reservation.
        match Ledger::reserve(self, amount).await {
            Ok(true) => Ok(day),
            Ok(false) => Err(ProbeError::BudgetExhausted),
            Err(_) => Err(ProbeError::BudgetExhausted),
        }
    }
    async fn settle(
        &self,
        day: &str,
        reserved: u64,
        used: u64,
    ) -> std::result::Result<(), ProbeError> {
        self.refund(day, reserved.saturating_sub(used))
            .await
            .map_err(|_| ProbeError::BudgetExhausted)
    }
}

fn budget_host_key(host: &str) -> String {
    let host = host.trim_matches(['[', ']']).trim_end_matches('.');
    host.parse::<std::net::IpAddr>()
        .map(|ip| ip.to_string())
        .unwrap_or_else(|_| host.to_ascii_lowercase())
}
#[derive(Debug)]
struct LeaseBudget {
    ledger: Ledger,
    remaining: std::sync::Mutex<std::collections::BTreeMap<String, u16>>,
}
impl LeaseBudget {
    fn new(ledger: Ledger, nodes: &[monitor_core::model::Node], units: u16) -> Self {
        let mut remaining = std::collections::BTreeMap::new();
        for node in nodes {
            remaining.insert(budget_host_key(&node.host), units);
        }
        Self {
            ledger,
            remaining: std::sync::Mutex::new(remaining),
        }
    }
}
#[async_trait::async_trait]
impl MeasurementBudget for LeaseBudget {
    async fn reserve_request(&self, host: &str) -> std::result::Result<(), ProbeError> {
        let mut remaining = self
            .remaining
            .lock()
            .map_err(|_| ProbeError::BudgetExhausted)?;
        let available = remaining
            .get_mut(&budget_host_key(host))
            .ok_or(ProbeError::BudgetExhausted)?;
        if *available == 0 {
            return Err(ProbeError::BudgetExhausted);
        }
        *available -= 1;
        Ok(())
    }
    async fn reserve(&self, bytes: u64) -> std::result::Result<String, ProbeError> {
        MeasurementBudget::reserve(&self.ledger, bytes).await
    }
    async fn settle(
        &self,
        receipt: &str,
        reserved: u64,
        used: u64,
    ) -> std::result::Result<(), ProbeError> {
        MeasurementBudget::settle(&self.ledger, receipt, reserved, used).await
    }
}

struct Api {
    client: Client,
    base: Url,
    ledger: Ledger,
}
impl Api {
    fn new(base: Url, ledger: Ledger) -> Result<Self> {
        let client = Client::builder()
            .redirect(Policy::none())
            .connect_timeout(Duration::from_secs(3))
            .timeout(Duration::from_secs(10))
            .user_agent(format!("Harvest-Scope-Community-Probe/{VERSION}"))
            .build()?;
        Ok(Self {
            client,
            base,
            ledger,
        })
    }
    async fn request<T: DeserializeOwned, B: Serialize>(
        &self,
        method: Method,
        endpoint: &str,
        auth: Option<&str>,
        body: Option<&B>,
    ) -> Result<T> {
        let payload = body.map(serde_json::to_vec).transpose()?;
        let sent_bytes = payload.as_ref().map_or(0, Vec::len);
        if sent_bytes > 512 * 1024 {
            return Err(bad_input("community request too large").into());
        }
        let day = Utc::now().format("%Y-%m-%d").to_string();
        if !self.ledger.reserve(CENTRAL_REQUEST_RESERVATION).await? {
            return Err(bad_input("daily traffic limit reached").into());
        }
        let url = self.base.join(&format!("{PREFIX}/{endpoint}"))?;
        let mut request = self.client.request(method, url);
        if let Some(token) = auth {
            request = request.bearer_auth(token);
        }
        if let Some(payload) = payload {
            request = request
                .header("content-type", "application/json")
                .body(payload);
        }
        let mut response = match request.send().await {
            Ok(response) => response,
            Err(error) => {
                self.ledger
                    .refund(
                        &day,
                        CENTRAL_REQUEST_RESERVATION.saturating_sub((sent_bytes + 4096) as u64),
                    )
                    .await?;
                return Err(error.into());
            }
        };
        let status = response.status();
        if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
            self.ledger
                .refund(
                    &day,
                    CENTRAL_REQUEST_RESERVATION.saturating_sub((sent_bytes + 4096) as u64),
                )
                .await?;
            return Err(AuthError(status).into());
        }
        if !status.is_success() {
            self.ledger
                .refund(
                    &day,
                    CENTRAL_REQUEST_RESERVATION.saturating_sub((sent_bytes + 4096) as u64),
                )
                .await?;
            return Err(bad_input(format!("community API returned HTTP {status}")).into());
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            if bytes.len() + chunk.len() > CENTRAL_RESPONSE_LIMIT {
                return Err(bad_input("community API response too large").into());
            }
            bytes.extend_from_slice(&chunk);
        }
        if bytes.is_empty() {
            bytes.extend_from_slice(b"null");
        }
        self.ledger
            .refund(
                &day,
                CENTRAL_REQUEST_RESERVATION
                    .saturating_sub((sent_bytes + bytes.len() + 4096) as u64),
            )
            .await?;
        Ok(serde_json::from_slice(&bytes)?)
    }
    async fn enroll(&self, identity: &Identity) -> Result<EnrollChallenge> {
        self.request(
            Method::POST,
            "enroll",
            None,
            Some(&EnrollRequest {
                public_key: identity.public_key.clone(),
                version: VERSION.into(),
            }),
        )
        .await
    }
    async fn challenge(&self, probe_id: &str, identity: &Identity) -> Result<EnrollChallenge> {
        self.request(
            Method::POST,
            "challenge",
            None,
            Some(&serde_json::json!({"probeId":probe_id,"publicKey":identity.public_key})),
        )
        .await
    }
    async fn token(
        &self,
        identity: &Identity,
        challenge: &EnrollChallenge,
    ) -> Result<TokenResponse> {
        self.request(
            Method::POST,
            "token",
            None,
            Some(&identity.token_request(challenge)),
        )
        .await
    }
}
#[derive(Debug)]
struct AuthError(StatusCode);
impl std::fmt::Display for AuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "community admission refused: HTTP {}", self.0)
    }
}
impl Error for AuthError {}

pub async fn run(args: Vec<String>) -> Result<()> {
    let state_dir =
        PathBuf::from(env::var("COMMUNITY_STATE_DIR").unwrap_or_else(|_| "community-state".into()));
    fs::create_dir_all(&state_dir)?;
    let state_path = state_dir.join("state.json");
    let key_path = state_dir.join("identity.pkcs8");
    let config_path = env::var("COMMUNITY_LIMITS_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| state_dir.join("limits.json"));
    let config = if config_path.exists() {
        serde_json::from_slice::<LocalConfig>(&fs::read(config_path)?)?
    } else {
        LocalConfig::default()
    };
    config.validate()?;
    let mut state: LocalState = if state_path.exists() {
        serde_json::from_slice(&fs::read(&state_path)?)?
    } else {
        LocalState::default()
    };
    let ledger = Ledger::open(
        &state_dir.join("traffic.db"),
        config.limits.daily_traffic_bytes,
    )
    .await?;
    if args.iter().any(|arg| arg == "--status") {
        let used = ledger.used().await?;
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "version":VERSION, "probeId":state.probe_id, "left":state.left, "revoked":state.revoked,
                "limits":config.limits, "maxCpuPercent":config.max_cpu_percent,
                "maxMemoryBytes":config.max_memory_bytes, "usedDailyTrafficBytes":used,
                "remainingDailyTrafficBytes":config.limits.daily_traffic_bytes.saturating_sub(used),
                "server":fs::read(state_dir.join("server-status.json")).ok().and_then(|v|serde_json::from_slice::<serde_json::Value>(&v).ok()),
                "runtime":fs::read(state_dir.join("runtime.json")).ok().and_then(|v|serde_json::from_slice::<serde_json::Value>(&v).ok()),
            }))?
        );
        return Ok(());
    }
    if args.iter().any(|arg| arg == "--rejoin") {
        // Invalidate an already running worker before clearing its old results.
        state.left = true;
        state.revoked = false;
        state.probe_id = None;
        state.identity_nonce = Some(Uuid::new_v4().to_string());
        save_state(&state_path, &state)?;
        if key_path.exists() {
            fs::remove_file(&key_path)?;
        }
        clear_old_spool(&state_dir.join("results.db")).await?;
        state.left = false;
        save_state(&state_path, &state)?;
    }
    if args.iter().any(|arg| arg == "--leave") {
        let identity = if key_path.exists() {
            Identity::load_or_create(&key_path).ok()
        } else {
            None
        };
        let probe_id = state.probe_id.take();
        state.left = true;
        save_state(&state_path, &state)?;
        if key_path.exists() {
            fs::remove_file(&key_path)?;
        }
        // Local withdrawal is already durable before attempting any network operation.
        let _ = tokio::time::timeout(Duration::from_secs(3), async {
            if let (Some(probe_id), Some(identity), Ok(base)) =
                (probe_id, identity, configured_base())
            {
                if let Ok(api) = Api::new(base, ledger.clone()) {
                    if let Ok(challenge) = api.challenge(&probe_id, &identity).await {
                        if let Ok(token) = api.token(&identity, &challenge).await {
                            let _ = api
                                .request::<serde_json::Value, ()>(
                                    Method::POST,
                                    "revoke",
                                    Some(&token.access_token),
                                    None,
                                )
                                .await;
                        }
                    }
                }
            }
        })
        .await;
        println!("community membership left locally");
        return Ok(());
    }
    if state.left {
        tracing::warn!(
            "community membership is left; waiting for stop, explicit --rejoin required"
        );
        stop_signal().await;
        return Ok(());
    }
    initialize_membership(&state_path, &mut state)?;
    let base = configured_base()?;
    let api = Api::new(base, ledger.clone())?;
    let identity = Identity::load_or_create(&key_path)?;
    let spool = Spool::connect(
        &format!(
            "sqlite://{}?mode=rwc",
            state_dir.join("results.db").display()
        ),
        config.limits.spool_bytes as usize,
    )
    .await?;
    let mut contact = env::var("MONITOR_CONTACT").unwrap_or_else(|_| api.base.as_str().to_owned());
    let limits = Limits {
        concurrency: config.limits.max_concurrency as usize,
        global_requests_per_second: config.limits.requests_per_second,
        burst: config.limits.burst as f64,
        node_units_per_ten_minutes: 6,
    };
    let history_path = state_dir.join("finalization.db");
    let history_url = format!("sqlite://{}?mode=rwc", history_path.display());
    let history = FinalizationHistory::connect(&history_url).await?;
    prune_finalization_history(&history_url).await?;
    let mut history_pruned_at = std::time::Instant::now();
    let mut engine = ProbeEngine::with_limits(&contact, limits)
        .with_measurement_budget(&contact, Arc::new(ledger.clone()))
        .with_finalization_history(history.clone());
    let mut token: Option<TokenResponse> = None;
    let mut failures = 0_u32;
    let mut previous_cpu = cpu_usage_usec().map(|v| (v, std::time::Instant::now()));
    let mut pressure_gate = crate::pressure::PressureGate::default();
    let mut previous_pressure = None;
    let processing = async {
        use futures::{stream::FuturesUnordered, FutureExt, StreamExt};
        let mut jobs = FuturesUnordered::new();
        let mut ticker = tokio::time::interval(Duration::from_secs(15));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut heartbeat_due = std::time::Instant::now();
        loop {
            tokio::select! {
                Some(completed) = jobs.next(), if !jobs.is_empty() => {
                    let (lease_id, expires_at, result): (String, DateTime<Utc>, Result<Option<ResultBatch>>) = completed;
                    if !membership_active(&state_path, &state) { break; }
                    match result {
                        Ok(Some(batch)) => {
                            let bytes = serde_json::to_vec(&QueuedResult { expires_at, batch })?;
                            if bytes.len() <= 512*1024 {
                                if let Err(error) = spool.enqueue(&lease_id,&bytes).await {
                                    tracing::warn!(%error,"community spool paused; completed lease will expire");
                                }
                            }
                        }
                        Ok(None) => (),
                        Err(error) => tracing::warn!(%error,"community lease stopped without a target failure result"),
                    }
                    heartbeat_due = std::time::Instant::now();
                }
                _ = ticker.tick() => {
                    // An explicit --leave in a second process must also stop this worker.
                    if !membership_active(&state_path, &state) { break; }
                    write_runtime(&state_dir, &state, "authentication_pending", jobs.len(), &spool, &ledger).await?;
                    if history_pruned_at.elapsed() >= Duration::from_secs(24 * 3600) {
                        prune_finalization_history(&history_url).await?;
                        history_pruned_at = std::time::Instant::now();
                    }
                    if token.as_ref().is_none_or(|t|t.expires_at <= Utc::now()+chrono::Duration::minutes(5)) {
                        let challenge = if let Some(id)=&state.probe_id {api.challenge(id,&identity).await} else {api.enroll(&identity).await};
                        let refreshed = match challenge {
                            Ok(challenge) => {
                                if !membership_active(&state_path, &state) { break; }
                                if challenge.expires_at <= Utc::now() || state.probe_id.as_ref().is_some_and(|id|id!=&challenge.probe_id) {
                                    return Err(bad_input("invalid enrollment challenge").into());
                                }
                                if state.probe_id.is_none() {state.probe_id=Some(challenge.probe_id.clone());save_state(&state_path,&state)?;}
                                api.token(&identity,&challenge).await
                            }
                            Err(error)=>Err(error),
                        };
                        if !membership_active(&state_path, &state) { break; }
                        match refreshed {
                            Ok(value)=>{
                                if !value.monitor_contact.is_empty() {
                                    contact=value.monitor_contact.clone();
                                    engine=ProbeEngine::with_limits(&value.monitor_contact,limits)
                                        .with_measurement_budget(&value.monitor_contact,Arc::new(ledger.clone()))
                                        .with_finalization_history(history.clone());
                                }
                                token=Some(value);state.revoked=false;save_state(&state_path,&state)?;failures=0;heartbeat_due=std::time::Instant::now();}
                            Err(error)=>{
                                state.revoked=is_auth_error(&error);save_state(&state_path,&state)?;
                                token=None;
                                write_runtime(&state_dir, &state, if state.revoked {"admission_refused"} else {"central_unavailable"}, jobs.len(), &spool, &ledger).await?;
                                tracing::warn!(%error,"community registration or refresh paused");
                                retry_delay(&mut failures).await;continue;
                            }
                        }
                    }
                    let access = &token.as_ref().expect("authenticated above").access_token;
                    let remaining=config.limits.daily_traffic_bytes.saturating_sub(ledger.used().await?);
                    let stats=spool.stats().await?;
                    let resources=resource_pressure(&config,&mut previous_cpu,&mut pressure_gate);
                    let pressure=resources.paused;
                    if previous_pressure!=Some(pressure) {tracing::info!(?resources,"community resource admission changed");previous_pressure=Some(pressure);}
                    let room=spool_has_room(stats.spool_bytes,jobs.len(),config.limits.spool_bytes);
                    let spool_slots=(config.limits.spool_bytes.saturating_sub(stats.spool_bytes).saturating_sub(1)/(512*1024))
                        .saturating_sub(jobs.len() as u64).min(u16::MAX as u64) as u16;
                    let available=if pressure || !room || remaining<CHECK_RESERVATION {0} else {
                        config.limits.max_concurrency.saturating_sub(jobs.len() as u16).min(16).min(spool_slots)
                    };
                    let pause_reason=if pressure {Some("resource_pressure")} else if !room {Some("spool_full")} else if remaining<CHECK_RESERVATION {Some("traffic_budget")} else {None};
                    atomic_write_private(&state_dir.join("runtime.json"),&serde_json::to_vec(&serde_json::json!({
                        "updatedAt":Utc::now(),"identityNonce":state.identity_nonce,"pauseReason":pause_reason,"running":jobs.len(),"resources":resources,
                        "queueDepth":stats.queue_depth,"spoolBytes":stats.spool_bytes,"remainingDailyTrafficBytes":remaining
                    }))?)?;
                    if remaining<CENTRAL_REQUEST_RESERVATION {
                        write_runtime(&state_dir,&state,"traffic_budget",jobs.len(),&spool,&ledger).await?;
                        continue;
                    }
                    if std::time::Instant::now()>=heartbeat_due {
                        if let Ok(server_status)=api.request::<serde_json::Value,()>(Method::GET,"status",Some(access),None).await {
                            atomic_write_private(&state_dir.join("server-status.json"),&serde_json::to_vec(&server_status)?)?;
                        }
                        let heartbeat=HeartbeatRequest{version:VERSION.into(),limits:config.limits.clone(),capacity:RuntimeCapacity{
                            available_slots:available,remaining_daily_traffic_bytes:remaining,
                            pause_reason:pause_reason.map(str::to_owned),queue_depth:stats.queue_depth,spool_bytes:stats.spool_bytes,
                            cpu_limit_millicores:Some((config.cpu_cores*1000.0).ceil().max(1.0) as u32),
                            memory_limit_bytes:Some(config.max_memory_bytes),
                            cpu_usage_percent:resources.cpu_percent,working_memory_percent:resources.working_memory_percent,
                        }};
                        if let Err(error)=api.request::<serde_json::Value,_>(Method::POST,"heartbeat",Some(access),Some(&heartbeat)).await {
                            if is_auth_error(&error) {token=None;}
                            tracing::warn!(%error,"community heartbeat paused");
                            write_runtime(&state_dir,&state,"central_unavailable",jobs.len(),&spool,&ledger).await?;
                            retry_delay(&mut failures).await;continue;
                        }
                        heartbeat_due=std::time::Instant::now()+Duration::from_secs(60);
                    }
                    if let Err(error)=flush_spool(&api,access,&spool,&state_path,&state).await {
                        if is_auth_error(&error) {token=None;continue;}
                        tracing::warn!(%error,"community result delivery paused");
                    }
                    if available==0 || !membership_active(&state_path,&state) {continue;}
                    let response:Result<LeaseResponse>=api.request::<LeaseResponse,()>(Method::GET,&format!("leases?limit={available}"),Some(access),None).await;
                    match response {
                        Ok(response)=>{
                            for lease in response.leases.into_iter().take(usize::from(available)) {
                                if !lease_is_valid(&lease) {continue;}
                                let nodes=std::iter::once(lease.node.clone()).chain(lease.reference_nodes.iter().cloned()).collect::<Vec<_>>();
                                let quota=Arc::new(LeaseBudget::new(ledger.clone(),&nodes,lease.request_units_per_target));
                                let engine=engine.clone().with_measurement_budget(&contact,quota);let ledger=ledger.clone();
                                jobs.push(async move {
                                    let ttl=(lease.expires_at-Utc::now()).to_std().unwrap_or_default();
                                    let result=tokio::time::timeout(ttl,execute_lease(&engine,&ledger,&lease)).await
                                        .unwrap_or_else(|_|Err(bad_input("lease expired locally").into()));
                                    (lease.lease_id,lease.expires_at,result)
                                }.boxed_local());
                            }
                            failures=0;
                        }
                        Err(error)=>{
                            if is_auth_error(&error){token=None;}
                            tracing::warn!(%error,"community lease poll paused");
                            write_runtime(&state_dir,&state,"central_unavailable",jobs.len(),&spool,&ledger).await?;
                            retry_delay(&mut failures).await;
                        }
                    }
                }
            }
        }
        Ok(())
    };
    // Cancellation cannot cause an unstarted check to be reported as a target failure.
    tokio::select! { result=processing=>result, _=stop_signal()=>Ok(()) }
}

fn configured_base() -> Result<Url> {
    let value = env::var("CENTRAL_API_URL")?;
    let url = Url::parse(&value)?;
    let local_test = env::var("COMMUNITY_ALLOW_INSECURE_LOCALHOST")
        .ok()
        .as_deref()
        == Some("1")
        && url
            .host_str()
            .is_some_and(|host| host == "localhost" || host == "127.0.0.1" || host == "[::1]");
    if url.scheme() != "https" && !(url.scheme() == "http" && local_test) {
        return Err(bad_input("community Central URL must use HTTPS").into());
    }
    if !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(bad_input("invalid community Central URL").into());
    }
    Ok(url)
}
fn lease_is_valid(lease: &Lease) -> bool {
    let now = Utc::now();
    lease.issued_at <= now
        && lease.expires_at > now
        && lease.expires_at <= lease.issued_at + chrono::Duration::minutes(5)
        && (1..=16).contains(&lease.request_units_per_target)
        && !lease.checks.is_empty()
        && lease.checks.len() <= 4
        && lease.reference_nodes.len() <= 8
}
async fn execute_lease(
    engine: &ProbeEngine,
    ledger: &Ledger,
    lease: &Lease,
) -> Result<Option<ResultBatch>> {
    let mut observations = Vec::new();
    for check in &lease.checks {
        if lease.expires_at <= Utc::now() + chrono::Duration::seconds(20) {
            break;
        }
        if ledger.used().await?.saturating_add(CHECK_RESERVATION) > ledger.daily_limit {
            break;
        }
        let job = ScheduledJob {
            job_id: lease.job_id.clone(),
            node_id: lease.node.id.clone(),
            check: *check,
            offset_seconds: 0,
            site_epoch_offset_seconds: 0,
            request_units: 1,
        };
        for observation in engine
            .check_all_with_references(&lease.node, &job, Site::A, 2, &lease.reference_nodes)
            .await
        {
            observations.push(CommunityObservation {
                job_id: lease.job_id.clone(),
                node_id: observation.node_id,
                check_kind: observation.check_kind.unwrap_or(*check),
                dimension: observation.dimension,
                outcome: observation.outcome,
                evidence: observation.evidence,
                observed_at: observation.observed_at,
            });
        }
    }
    if observations.is_empty() {
        Ok(None)
    } else {
        Ok(Some(ResultBatch {
            lease_id: lease.lease_id.clone(),
            observations,
        }))
    }
}
async fn flush_spool(
    api: &Api,
    access: &str,
    spool: &Spool,
    state_path: &Path,
    state: &LocalState,
) -> Result<()> {
    for item in spool.pending(8).await? {
        if !membership_active(state_path, state) {
            break;
        }
        let queued: QueuedResult = match serde_json::from_slice(&item.payload) {
            Ok(value) => value,
            Err(_) => {
                spool.ack(&[&item.job_id]).await?;
                continue;
            }
        };
        if queued.expires_at + chrono::Duration::seconds(QUEUE_RETENTION_SECS) <= Utc::now() {
            spool.ack(&[&item.job_id]).await?;
            continue;
        }
        let sent: Result<serde_json::Value> = api
            .request(Method::POST, "results", Some(access), Some(&queued.batch))
            .await;
        match sent {
            Ok(ack) if ack_covers_batch(&ack, queued.batch.observations.len()) => {
                spool.ack(&[&item.job_id]).await?
            }
            Ok(_) => {
                tracing::warn!("community result ACK did not cover the sent batch");
                break;
            }
            Err(error) if is_auth_error(&error) => return Err(error),
            Err(error) => {
                tracing::warn!(error = %error, "community result upload failed");
                break;
            }
        }
    }
    Ok(())
}
fn ack_covers_batch(ack: &serde_json::Value, expected: usize) -> bool {
    let accepted = ack.get("accepted").and_then(serde_json::Value::as_u64);
    let duplicates = ack.get("duplicates").and_then(serde_json::Value::as_u64);
    matches!((accepted, duplicates), (Some(a), Some(d)) if a.checked_add(d) == Some(expected as u64))
}
fn spool_has_room(used: u64, running: usize, limit: u64) -> bool {
    let reserved = (running as u64)
        .saturating_add(1)
        .saturating_mul(512 * 1024);
    used.saturating_add(reserved) < limit
}
fn membership_active(path: &Path, expected: &LocalState) -> bool {
    fs::read(path)
        .ok()
        .and_then(|bytes| serde_json::from_slice::<LocalState>(&bytes).ok())
        .is_some_and(|current| !current.left && current.identity_nonce == expected.identity_nonce)
}
fn initialize_membership(path: &Path, state: &mut LocalState) -> Result<()> {
    if state.identity_nonce.is_none() {
        state.identity_nonce = Some(Uuid::new_v4().to_string());
    }
    save_state(path, state)
}
async fn clear_old_spool(path: &Path) -> Result<()> {
    if !path.exists() {
        return Ok(());
    }
    let db = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(&format!("sqlite://{}?mode=rw", path.display()))
        .await?;
    let mut tx = db.begin().await?;
    sqlx::query("DELETE FROM spool").execute(&mut *tx).await?;
    sqlx::query("UPDATE spool_state SET queue_depth=0,payload_bytes=0 WHERE singleton=1")
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}
async fn prune_finalization_history(url: &str) -> Result<()> {
    let db = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(url)
        .await?;
    sqlx::query("DELETE FROM finalization_history WHERE last_observed_at < unixepoch() - 604800")
        .execute(&db)
        .await?;
    sqlx::query("DELETE FROM finalization_history WHERE node_id NOT IN (SELECT node_id FROM finalization_history ORDER BY last_observed_at DESC LIMIT 4096)")
        .execute(&db).await?;
    Ok(())
}
async fn write_runtime(
    state_dir: &Path,
    state: &LocalState,
    reason: &str,
    running: usize,
    spool: &Spool,
    ledger: &Ledger,
) -> Result<()> {
    let stats = spool.stats().await?;
    let remaining = ledger.daily_limit.saturating_sub(ledger.used().await?);
    atomic_write_private(
        &state_dir.join("runtime.json"),
        &serde_json::to_vec(&serde_json::json!({
            "updatedAt":Utc::now(),"identityNonce":state.identity_nonce,"pauseReason":reason,"running":running,
            "queueDepth":stats.queue_depth,"spoolBytes":stats.spool_bytes,"remainingDailyTrafficBytes":remaining
        }))?,
    )
}
pub fn healthcheck() -> Result<()> {
    let state_dir =
        PathBuf::from(env::var("COMMUNITY_STATE_DIR").unwrap_or_else(|_| "community-state".into()));
    let state: LocalState = serde_json::from_slice(&fs::read(state_dir.join("state.json"))?)?;
    let runtime: serde_json::Value =
        serde_json::from_slice(&fs::read(state_dir.join("runtime.json"))?)?;
    if runtime_is_healthy(&state, &runtime, Utc::now()) {
        Ok(())
    } else {
        Err(bad_input("community runtime is stopped or stale").into())
    }
}
fn runtime_is_healthy(state: &LocalState, runtime: &serde_json::Value, now: DateTime<Utc>) -> bool {
    if state.left
        || runtime
            .get("identityNonce")
            .and_then(serde_json::Value::as_str)
            != state.identity_nonce.as_deref()
    {
        return false;
    }
    let updated = runtime
        .get("updatedAt")
        .and_then(serde_json::Value::as_str)
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok());
    updated.is_some_and(|value| {
        let age = now.signed_duration_since(value.with_timezone(&Utc));
        age >= chrono::Duration::zero() && age <= chrono::Duration::minutes(4)
    })
}
fn is_auth_error(error: &Box<dyn Error>) -> bool {
    error.downcast_ref::<AuthError>().is_some()
}
async fn retry_delay(failures: &mut u32) {
    *failures = failures.saturating_add(1).min(6);
    let base = (1_u64 << *failures).min(60);
    let jitter = Utc::now().timestamp_subsec_millis() as u64 % (base + 1);
    sleep(Duration::from_secs(base + jitter)).await;
}
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ResourceUsage {
    cpu_percent: Option<u8>,
    working_memory_percent: Option<u8>,
    memory_stall_avg10: Option<f64>,
    paused: bool,
}
fn resource_pressure(
    config: &LocalConfig,
    previous_cpu: &mut Option<(u64, std::time::Instant)>,
    gate: &mut crate::pressure::PressureGate,
) -> ResourceUsage {
    let memory = crate::pressure::memory_reading(Some(config.max_memory_bytes));
    let cpu = cpu_usage_usec();
    let normalized = match (previous_cpu.as_ref(), cpu, cpu_quota_millicores()) {
        (Some((before, time)), Some(now), Some(milli))
            if milli > 0 && time.elapsed().as_micros() > 0 =>
        {
            Some(
                (now.saturating_sub(*before).saturating_mul(100_000)
                    / (time.elapsed().as_micros() as u64)
                    / milli)
                    .min(255) as u8,
            )
        }
        _ => None,
    };
    *previous_cpu = cpu.map(|v| (v, std::time::Instant::now()));
    pressure_sample(normalized, memory, config.max_cpu_percent, gate)
}
fn pressure_sample(
    cpu: Option<u8>,
    memory: Option<(u8, f64)>,
    max_cpu_percent: u8,
    gate: &mut crate::pressure::PressureGate,
) -> ResourceUsage {
    let gated_cpu = cpu
        .and_then(|v| (u64::from(v) * 70).checked_div(u64::from(max_cpu_percent)))
        .map(|v| v.min(255) as u8);
    let paused = gate.update(gated_cpu, memory.map(|v| v.0), memory.map(|v| v.1), false);
    ResourceUsage {
        cpu_percent: cpu,
        working_memory_percent: memory.map(|v| v.0),
        memory_stall_avg10: memory.map(|v| v.1),
        paused,
    }
}
fn cpu_usage_usec() -> Option<u64> {
    fs::read_to_string("/sys/fs/cgroup/cpu.stat")
        .ok()?
        .lines()
        .find_map(|line| line.strip_prefix("usage_usec ")?.parse().ok())
}
fn cpu_quota_millicores() -> Option<u64> {
    let text = fs::read_to_string("/sys/fs/cgroup/cpu.max").ok()?;
    let mut fields = text.split_whitespace();
    let quota = match fields.next()? {
        "max" => return Some(1000),
        value => value.parse::<u64>().ok()?,
    };
    let period = fields.next()?.parse::<u64>().ok()?;
    (period != 0).then_some(quota.saturating_mul(1000) / period)
}
#[cfg(unix)]
async fn stop_signal() {
    if let Ok(mut sigterm) =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
    {
        tokio::select! { _ = sigterm.recv() => {}, _ = tokio::signal::ctrl_c() => {} }
    } else {
        let _ = tokio::signal::ctrl_c().await;
    }
}
#[cfg(not(unix))]
async fn stop_signal() {
    let _ = tokio::signal::ctrl_c().await;
}
fn save_state(path: &Path, state: &LocalState) -> Result<()> {
    atomic_write_private(path, &serde_json::to_vec(state)?)
}
fn atomic_write_private(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| bad_input("state path has no parent"))?;
    fs::create_dir_all(parent)?;
    let temp = parent.join(format!(
        ".{}.tmp-{}",
        path.file_name().unwrap_or_default().to_string_lossy(),
        Uuid::new_v4()
    ));
    #[cfg(unix)]
    let mut file = {
        use std::os::unix::fs::OpenOptionsExt;
        fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temp)?
    };
    #[cfg(not(unix))]
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    drop(file);
    fs::rename(&temp, path)?;
    if let Ok(dir) = fs::File::open(parent) {
        let _ = dir.sync_all();
    }
    Ok(())
}
fn bad_input(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn first_start_persists_membership_before_polling_and_restart_preserves_it() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.json");
        let mut state = LocalState::default();
        assert!(!membership_active(&path, &state));
        initialize_membership(&path, &mut state).unwrap();
        assert!(membership_active(&path, &state));
        let nonce = state.identity_nonce.clone();
        initialize_membership(&path, &mut state).unwrap();
        assert_eq!(state.identity_nonce, nonce);
    }
    #[test]
    fn result_ack_requires_every_observation() {
        assert!(ack_covers_batch(
            &serde_json::json!({"accepted": 1, "duplicates": 1}),
            2
        ));
        assert!(!ack_covers_batch(
            &serde_json::json!({"accepted": 1, "duplicates": 0}),
            2
        ));
        assert!(!ack_covers_batch(
            &serde_json::json!({"accepted": 3, "duplicates": 0}),
            2
        ));
        assert!(!ack_covers_batch(&serde_json::json!({}), 2));
    }
    #[test]
    fn spool_admission_reserves_for_running_and_new_jobs() {
        let cap = 2 * 512 * 1024;
        assert!(spool_has_room(0, 0, cap));
        assert!(!spool_has_room(0, 1, cap));
        assert!(!spool_has_room(512 * 1024, 0, cap));
    }
    #[test]
    fn runtime_health_accepts_a_fresh_pause_but_rejects_stale_or_left() {
        let now = Utc::now();
        assert!(runtime_is_healthy(
            &LocalState::default(),
            &serde_json::json!({"updatedAt":now,"pauseReason":"resource_pressure"}),
            now
        ));
        assert!(!runtime_is_healthy(
            &LocalState::default(),
            &serde_json::json!({"updatedAt":now-chrono::Duration::minutes(5)}),
            now
        ));
        assert!(!runtime_is_healthy(
            &LocalState {
                left: true,
                ..LocalState::default()
            },
            &serde_json::json!({"updatedAt":now}),
            now
        ));
        assert!(!runtime_is_healthy(
            &LocalState {
                identity_nonce: Some("new".into()),
                ..LocalState::default()
            },
            &serde_json::json!({"updatedAt":now,"identityNonce":"old"}),
            now
        ));
    }
    #[tokio::test]
    async fn rejoin_discards_queued_results_from_previous_identity() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("results.db");
        let spool = Spool::connect(
            &format!("sqlite://{}?mode=rwc", path.display()),
            1024 * 1024,
        )
        .await
        .unwrap();
        spool.enqueue("old-job", b"old result").await.unwrap();
        assert_eq!(spool.stats().await.unwrap().queue_depth, 1);
        clear_old_spool(&path).await.unwrap();
        assert_eq!(spool.stats().await.unwrap().queue_depth, 0);
        assert!(spool.pending(1).await.unwrap().is_empty());
    }
    #[test]
    fn prior_worker_stops_after_local_membership_changes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.json");
        let old = LocalState {
            identity_nonce: Some("old".into()),
            ..LocalState::default()
        };
        save_state(&path, &old).unwrap();
        assert!(membership_active(&path, &old));
        let new = LocalState {
            identity_nonce: Some("new".into()),
            ..LocalState::default()
        };
        save_state(&path, &new).unwrap();
        assert!(!membership_active(&path, &old));
    }
    #[test]
    fn default_limits_are_small_and_config_rejects_invalid_values() {
        let config = LocalConfig::default();
        assert_eq!(config.limits.max_concurrency, 1);
        assert_eq!(config.limits.daily_traffic_bytes, 100 * 1024 * 1024);
        assert!(config.validate().is_ok());
        let mut invalid = config.clone();
        invalid.max_cpu_percent = 0;
        assert!(invalid.validate().is_err());
    }
    #[tokio::test]
    async fn traffic_reservations_survive_restart_and_enforce_daily_limit() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("traffic.db");
        let first = Ledger::open(&path, 100).await.unwrap();
        assert!(first.reserve(60).await.unwrap());
        drop(first);
        let second = Ledger::open(&path, 100).await.unwrap();
        assert!(!second.reserve(41).await.unwrap());
        assert!(second.reserve(40).await.unwrap());
        assert_eq!(second.used().await.unwrap(), 100);
    }
    #[test]
    fn identity_is_stable_and_challenge_signature_verifies() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("identity.pkcs8");
        let first = Identity::load_or_create(&path).unwrap();
        let challenge = EnrollChallenge {
            probe_id: "probe-id".into(),
            challenge: "nonce".into(),
            expires_at: Utc::now() + chrono::Duration::minutes(2),
        };
        let token_request = first.token_request(&challenge);
        let public = URL_SAFE_NO_PAD.decode(&first.public_key).unwrap();
        let signature = URL_SAFE_NO_PAD.decode(token_request.signature).unwrap();
        ring::signature::UnparsedPublicKey::new(&ring::signature::ED25519, public)
            .verify(b"harvest-scope-community-v2\nprobe-id\nnonce", &signature)
            .unwrap();
        assert_eq!(
            Identity::load_or_create(&path).unwrap().public_key,
            first.public_key
        );
    }
    #[tokio::test]
    async fn lease_request_quota_rejects_extra_requests_and_unassigned_hosts() {
        let dir = tempfile::tempdir().unwrap();
        let ledger = Ledger::open(&dir.path().join("quota.db"), 1024 * 1024)
            .await
            .unwrap();
        let node = monitor_core::model::Node::new(
            "n",
            monitor_core::model::Network::SymbolMainnet,
            monitor_core::model::NodeKind::Api,
            "Node.Example.",
        );
        let budget = LeaseBudget::new(ledger, std::slice::from_ref(&node), 2);
        assert!(budget.reserve_request("node.example").await.is_ok());
        assert!(budget.reserve_request("NODE.EXAMPLE.").await.is_ok());
        assert!(matches!(
            budget.reserve_request("node.example").await,
            Err(ProbeError::BudgetExhausted)
        ));
        assert!(matches!(
            budget.reserve_request("unassigned.example").await,
            Err(ProbeError::BudgetExhausted)
        ));
    }
    #[tokio::test]
    async fn rejected_private_target_does_not_spend_the_whole_response_reservation() {
        let dir = tempfile::tempdir().unwrap();
        let ledger = Ledger::open(&dir.path().join("rejected.db"), 100 * 1024 * 1024)
            .await
            .unwrap();
        let engine = ProbeEngine::with_limits("test", Limits::default())
            .with_measurement_budget("test", Arc::new(ledger.clone()));
        let node = monitor_core::model::Node::new(
            "rejected",
            monitor_core::model::Network::Nis1Mainnet,
            monitor_core::model::NodeKind::Api,
            "10.254.254.254",
        );
        let job = ScheduledJob {
            job_id: "rejected".into(),
            node_id: node.id.clone(),
            check: monitor_core::model::CheckKind::Availability,
            offset_seconds: 0,
            site_epoch_offset_seconds: 0,
            request_units: 1,
        };
        let observations = engine
            .check_all_with_references(&node, &job, Site::A, 2, &[])
            .await;
        assert!(observations
            .iter()
            .all(|o| matches!(o.outcome, monitor_core::model::Outcome::Unknown(_))));
        assert!(
            ledger.used().await.unwrap() <= 8192,
            "preflight rejection must not consume a 4MiB response allowance"
        );
    }
    #[test]
    fn reported_cpu_usage_is_not_scaled_to_the_pause_threshold() {
        let mut gate = crate::pressure::PressureGate::default();
        let usage = pressure_sample(Some(80), Some((30, 0.0)), 90, &mut gate);
        assert_eq!(usage.cpu_percent, Some(80));
        assert!(!usage.paused);
        let usage = pressure_sample(Some(90), Some((30, 0.0)), 90, &mut gate);
        assert_eq!(usage.cpu_percent, Some(90));
        assert!(usage.paused);
    }
}
