use std::{
    collections::{BTreeMap, HashSet},
    env,
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

use chrono::{DateTime, Utc};
use monitor_core::{
    model::{IngestResult, Observation, ObservationBatch, Site},
    scheduler::CheckKind,
};
use monitor_probe::{
    assignment::{
        deferred_group_key, AssignedCheck, Assignment, AssignmentCache, AssignmentClient,
        AssignmentProvider, AssignmentSource,
    },
    engine::ProbeEngine,
    history::FinalizationHistory,
    limiter::{Admission, AdmissionController, Limits},
    spool::{Spool, SpoolError, SpoolItem, SpoolPressure},
};
use reqwest::redirect::Policy;
use serde_json::json;
use tokio::{sync::watch, task::JoinSet};
use tracing_subscriber::EnvFilter;

const MAX_QUEUED_AND_RUNNING: usize = 6;
const TCP_DIAGNOSTIC_TIMEOUT: chrono::Duration = chrono::Duration::seconds(10);
const TCP_DIAGNOSTIC_COOLDOWN: chrono::Duration = chrono::Duration::minutes(30);

#[derive(Default)]
struct GuardianEpochCounts {
    polls: u32,
    claimed: u32,
    empty: u32,
    invalid: u32,
    poll_failed: u32,
    admitted: u32,
    budget_exceeded: u32,
    pressure_paused: u32,
    host_busy: u32,
    concurrency_limited: u32,
    rate_limited: u32,
    uploaded: u32,
    upload_failed: u32,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    install_rustls_crypto_provider()?;
    if env::args().any(|argument| argument == "--healthcheck") {
        if env::var("PROBE_MODE").ok().as_deref() == Some("community") {
            return monitor_probe::community::healthcheck();
        }
        return healthcheck();
    }
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("monitor_probe=info")),
        )
        .init();

    if env::var("PROBE_MODE").ok().as_deref() == Some("community") {
        return monitor_probe::community::run(env::args().collect()).await;
    }

    let central_url = env::var("CENTRAL_API_URL")?
        .trim_end_matches('/')
        .to_owned();
    let api_key = env::var("PROBE_API_KEY")?;
    let probe_id = env::var("PROBE_ID")?;
    let site = parse_site(&env::var("PROBE_SITE").unwrap_or_else(|_| "a".into()))?;
    let contact =
        env::var("MONITOR_CONTACT").unwrap_or_else(|_| "https://example.invalid/contact".into());
    let spool_path = env::var("SPOOL_PATH").unwrap_or_else(|_| "probe-spool.db".into());
    let cache_path = env::var("ASSIGNMENT_CACHE_PATH")
        .unwrap_or_else(|_| format!("{spool_path}.assignments.db"));
    let history_path =
        env::var("PROTOCOL_HISTORY_PATH").unwrap_or_else(|_| format!("{spool_path}.protocol.db"));
    let limits = probe_limits_from_env()?;
    tracing::info!(
        global_requests_per_second = limits.global_requests_per_second,
        burst = limits.burst,
        "probe request limits configured"
    );
    let spool = Spool::connect(
        &format!("sqlite://{spool_path}?mode=rwc"),
        100 * 1024 * 1024,
    )
    .await?;
    let client = reqwest::Client::builder()
        .redirect(Policy::none())
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(10))
        .user_agent(format!("Harvest-Scope-Network-Monitor/0.1 ({contact})"))
        .build()?;
    let assignment_http = reqwest::Client::builder()
        .redirect(Policy::none())
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(35))
        .user_agent(format!("Harvest-Scope-Network-Monitor/0.1 ({contact})"))
        .build()?;
    let assignment_cache = AssignmentCache::connect(&format!(
        "sqlite://{}?mode=rwc",
        cache_path.replace('\\', "/")
    ))
    .await?;
    let assignment_client = AssignmentClient::new(
        assignment_http,
        &central_url,
        api_key.clone(),
        probe_id.clone(),
        site,
    )?;
    let assignment_provider =
        AssignmentProvider::new(assignment_client.clone(), assignment_cache.clone());
    let finalization_history = FinalizationHistory::connect(&format!(
        "sqlite://{}?mode=rwc",
        history_path.replace('\\', "/")
    ))
    .await?;
    spawn_heartbeat(
        client.clone(),
        spool.clone(),
        central_url.clone(),
        api_key.clone(),
        probe_id.clone(),
    );

    let engine = Arc::new(
        ProbeEngine::with_limits(&contact, limits).with_finalization_history(finalization_history),
    );
    let pressure = spawn_host_pressure_sampler(spool.clone(), Arc::clone(&engine));
    let mut tcp_diagnostic_cooldowns = BTreeMap::new();

    loop {
        let loaded = assignment_provider.refresh().await?;
        let refreshed_at = Utc::now();
        let schema_version = loaded.envelope.schema_version;
        let minimum_epoch = loaded.envelope.epoch;
        let envelope_expires_at = loaded.envelope.expires_at;
        let now = refreshed_at.timestamp().max(0) as u64;
        let (epoch, epoch_start, epoch_end) = execution_window(now, site, minimum_epoch);
        let assignments = loaded
            .envelope
            .assignments
            .into_iter()
            .filter(|assignment| {
                let scheduled_at = assignment.scheduled_at.timestamp();
                scheduled_at >= epoch_start && scheduled_at < epoch_end
            })
            .collect::<Vec<_>>();
        let mut jobs = execution_groups_until(&assignments, envelope_expires_at);
        for deferred in assignment_cache.load_deferred(refreshed_at).await? {
            jobs.extend(execution_groups_until(
                &[deferred.assignment],
                deferred.valid_until,
            ));
        }
        tracing::info!(
            epoch,
            source = ?loaded.source,
            grouped_job_count = jobs.len(),
            "epoch started"
        );
        if loaded.source == AssignmentSource::Cache {
            tracing::warn!("central API unavailable; executing valid cached assignments");
        }
        run_epoch(
            site,
            schema_version,
            jobs,
            Arc::clone(&engine),
            limits,
            EpochRuntime {
                spool: &spool,
                retry_client: &assignment_client,
                upload_client: &client,
                central_url: &central_url,
                api_key: &api_key,
                probe_id: &probe_id,
                assignment_cache: &assignment_cache,
                pressure: &pressure,
                tcp_diagnostic_cooldowns: &mut tcp_diagnostic_cooldowns,
                epoch_end,
            },
        )
        .await?;
        upload_pending(&client, &spool, &central_url, &api_key, &probe_id).await;

        let next_epoch = u64::try_from(epoch_end).unwrap_or(u64::MAX);
        let now = Utc::now().timestamp().max(0) as u64;
        tokio::time::sleep(Duration::from_secs(next_epoch.saturating_sub(now).max(1))).await;
    }
}

fn install_rustls_crypto_provider() -> Result<(), &'static str> {
    if rustls::crypto::CryptoProvider::get_default().is_some() {
        return Err("rustls crypto provider was installed before application startup");
    }
    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|_| "failed to install the rustls ring crypto provider")
}

fn execution_window(now: u64, site: Site, minimum_epoch: u64) -> (u64, i64, i64) {
    let site_offset = if site == Site::B { 300 } else { 0 };
    let epoch = (now.saturating_sub(site_offset) / 600).max(minimum_epoch);
    let epoch_start = epoch.saturating_mul(600).saturating_add(site_offset);
    let epoch_start = i64::try_from(epoch_start).unwrap_or(i64::MAX);
    (epoch, epoch_start, epoch_start.saturating_add(600))
}

fn healthcheck() -> Result<(), Box<dyn std::error::Error>> {
    let spool_path = env::var("SPOOL_PATH").unwrap_or_else(|_| "probe-spool.db".into());
    healthcheck_spool_path(Path::new(&spool_path))?;
    Ok(())
}

fn healthcheck_spool_path(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    if std::fs::metadata(path).is_ok_and(|metadata| !metadata.is_file()) {
        return Err("probe spool path is not a file".into());
    }
    Ok(())
}

fn parse_site(value: &str) -> Result<Site, Box<dyn std::error::Error>> {
    match value.to_ascii_lowercase().as_str() {
        "a" | "site-a" => Ok(Site::A),
        "b" | "site-b" => Ok(Site::B),
        _ => Err("PROBE_SITE must be a or b".into()),
    }
}

fn probe_limits_from_env() -> Result<Limits, Box<dyn std::error::Error>> {
    let rate = optional_env("PROBE_GLOBAL_REQUESTS_PER_SECOND")?;
    let burst = optional_env("PROBE_GLOBAL_BURST")?;
    parse_probe_limits(rate.as_deref(), burst.as_deref())
}

fn optional_env(name: &str) -> Result<Option<String>, env::VarError> {
    match env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(error),
    }
}

fn parse_probe_limits(
    rate: Option<&str>,
    burst: Option<&str>,
) -> Result<Limits, Box<dyn std::error::Error>> {
    let defaults = Limits::default();
    let rate = positive_f64(
        "PROBE_GLOBAL_REQUESTS_PER_SECOND",
        rate,
        defaults.global_requests_per_second,
    )?;
    let burst = positive_f64("PROBE_GLOBAL_BURST", burst, defaults.burst)?;
    Ok(Limits {
        global_requests_per_second: rate,
        burst,
        ..defaults
    })
}

fn positive_f64(
    name: &str,
    value: Option<&str>,
    default: f64,
) -> Result<f64, Box<dyn std::error::Error>> {
    let value = value.map_or(Ok(default), str::parse::<f64>)?;
    if !value.is_finite() || value <= 0.0 {
        return Err(format!("{name} must be a finite positive number").into());
    }
    Ok(value)
}

struct EpochRuntime<'a> {
    spool: &'a Spool,
    retry_client: &'a AssignmentClient,
    upload_client: &'a reqwest::Client,
    central_url: &'a str,
    api_key: &'a str,
    probe_id: &'a str,
    assignment_cache: &'a AssignmentCache,
    pressure: &'a watch::Receiver<PressureReading>,
    tcp_diagnostic_cooldowns: &'a mut BTreeMap<String, DateTime<Utc>>,
    epoch_end: i64,
}

#[derive(Debug)]
struct CompletedGroup {
    group_key: String,
    host: String,
    observations: Vec<Observation>,
    diagnostic: Option<TcpDiagnosticTarget>,
}

#[derive(Debug, Clone)]
struct TcpDiagnosticTarget {
    observation_job_id: String,
    endpoint_key: String,
    not_after: DateTime<Utc>,
    node: monitor_core::model::Node,
    rest_node: Option<monitor_core::model::Node>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TcpDiagnosticAdmission {
    Granted,
    Pressure,
    Deadline,
    Busy,
    Cooldown,
}

impl TcpDiagnosticAdmission {
    const fn reason(self) -> Option<&'static str> {
        match self {
            Self::Granted => None,
            Self::Pressure => Some("host_pressure"),
            Self::Deadline => Some("epoch_deadline"),
            Self::Busy => Some("diagnostic_busy"),
            Self::Cooldown => Some("endpoint_cooldown"),
        }
    }
}

fn tcp_diagnostic_candidate(
    group: &ExecutionGroup,
    observations: &[Observation],
) -> Option<TcpDiagnosticTarget> {
    if !group.urgent {
        return None;
    }
    group.checks.iter().find_map(|check| {
        if check.scheduled_job().check != CheckKind::PeerTcp
            || !observations.iter().any(|observation| {
                observation.job_id == check.observation_job_id
                    && observation.check_kind == Some(CheckKind::PeerTcp)
                    && matches!(
                        observation.outcome,
                        monitor_core::model::Outcome::Failure(_)
                    )
            })
        {
            return None;
        }
        Some(TcpDiagnosticTarget {
            observation_job_id: check.observation_job_id.clone(),
            endpoint_key: format!(
                "{}:{}:{}",
                check.node.host.len(),
                check.node.host,
                check.node.p2p_port
            ),
            not_after: group.not_after,
            node: check.node.clone(),
            rest_node: check.diagnostic_rest_node.clone(),
        })
    })
}

fn tcp_diagnostic_admission(
    pressure: HostPressure,
    now: DateTime<Utc>,
    epoch_end: DateTime<Utc>,
    diagnostic_running: bool,
    last_started: Option<DateTime<Utc>>,
) -> TcpDiagnosticAdmission {
    if pressure.active() {
        return TcpDiagnosticAdmission::Pressure;
    }
    if now + TCP_DIAGNOSTIC_TIMEOUT >= epoch_end {
        return TcpDiagnosticAdmission::Deadline;
    }
    if diagnostic_running {
        return TcpDiagnosticAdmission::Busy;
    }
    if last_started.is_some_and(|last| now - last < TCP_DIAGNOSTIC_COOLDOWN) {
        return TcpDiagnosticAdmission::Cooldown;
    }
    TcpDiagnosticAdmission::Granted
}

fn mark_tcp_diagnostic_skipped(
    observations: &mut [Observation],
    target: &TcpDiagnosticTarget,
    admission: TcpDiagnosticAdmission,
) {
    let Some(reason) = admission.reason() else {
        return;
    };
    if let Some(observation) = observations
        .iter_mut()
        .find(|observation| observation.job_id == target.observation_job_id)
    {
        if let Some(evidence) = observation.evidence.as_object_mut() {
            evidence.insert(
                "detailedDiagnostic".into(),
                json!({"status": "skipped", "reason": reason}),
            );
        }
    }
}

async fn spool_completed_group(
    runtime: &EpochRuntime<'_>,
    completed: CompletedGroup,
) -> Result<bool, monitor_probe::assignment::AssignmentError> {
    let mut upload_due = false;
    let mut completely_spooled = !completed.observations.is_empty();
    for observation in completed.observations {
        if let Ok(payload) = serde_json::to_vec(&observation) {
            if let Err(error) = runtime.spool.enqueue(&observation.job_id, &payload).await {
                completely_spooled = false;
                tracing::error!(%error, job_id = %observation.job_id, "failed to spool observation");
            } else {
                upload_due = true;
            }
        } else {
            completely_spooled = false;
        }
    }
    if completely_spooled {
        runtime
            .assignment_cache
            .complete_deferred(&completed.group_key)
            .await?;
    }
    Ok(upload_due)
}

async fn run_epoch(
    site: Site,
    schema_version: u8,
    mut jobs: Vec<ExecutionGroup>,
    engine: Arc<ProbeEngine>,
    limits: Limits,
    runtime: EpochRuntime<'_>,
) -> Result<(), monitor_probe::assignment::AssignmentError> {
    jobs = coalesce_execution_groups(jobs);
    let mut seen_groups = jobs
        .iter()
        .map(|job| job.group_id.clone())
        .collect::<HashSet<_>>();
    let mut pending = Vec::new();
    let mut running: JoinSet<CompletedGroup> = JoinSet::new();
    let mut diagnostics: JoinSet<CompletedGroup> = JoinSet::new();
    let mut limiter = AdmissionController::new(limits);
    let mut next_retry_poll = 0_u64;
    let mut next_guardian_poll = 0_u64;
    let mut next_guardian_dns_poll = 0_u64;
    let guardian_enabled = env::var("GUARDIAN_PROBE_ENABLED").is_ok_and(|v| v == "true");
    let mut guardian_counts = GuardianEpochCounts::default();
    let epoch_end = runtime.epoch_end.max(0) as u64;

    while (Utc::now().timestamp().max(0) as u64) < epoch_end
        || !jobs.is_empty()
        || !pending.is_empty()
        || !running.is_empty()
        || !diagnostics.is_empty()
    {
        let mut upload_due = false;
        while let Some(result) = diagnostics.try_join_next() {
            if let Ok(completed) = result {
                upload_due |= spool_completed_group(&runtime, completed).await?;
            }
        }
        while let Some(result) = running.try_join_next() {
            if let Ok(mut completed) = result {
                limiter.complete(&completed.host);
                if let Some(target) = completed.diagnostic.take() {
                    let now = Utc::now();
                    let epoch_end_at = DateTime::from_timestamp(epoch_end as i64, 0)
                        .unwrap_or(DateTime::<Utc>::MAX_UTC);
                    runtime
                        .tcp_diagnostic_cooldowns
                        .retain(|_, started| now - *started < TCP_DIAGNOSTIC_COOLDOWN);
                    let admission = tcp_diagnostic_admission(
                        runtime.pressure.borrow().effective(Instant::now()),
                        now,
                        epoch_end_at.min(target.not_after),
                        !diagnostics.is_empty(),
                        runtime
                            .tcp_diagnostic_cooldowns
                            .get(&target.endpoint_key)
                            .copied(),
                    );
                    if admission == TcpDiagnosticAdmission::Granted {
                        runtime
                            .tcp_diagnostic_cooldowns
                            .insert(target.endpoint_key.clone(), now);
                        let engine = Arc::clone(&engine);
                        diagnostics.spawn(async move {
                            if let Some(observation) = completed
                                .observations
                                .iter_mut()
                                .find(|observation| observation.job_id == target.observation_job_id)
                            {
                                engine
                                    .enrich_peer_tcp_diagnostic(
                                        observation,
                                        &target.node,
                                        target.rest_node.as_ref(),
                                    )
                                    .await;
                            }
                            completed
                        });
                    } else {
                        mark_tcp_diagnostic_skipped(
                            &mut completed.observations,
                            &target,
                            admission,
                        );
                        upload_due |= spool_completed_group(&runtime, completed).await?;
                    }
                } else {
                    upload_due |= spool_completed_group(&runtime, completed).await?;
                }
            }
        }

        let now = Utc::now().timestamp().max(0) as u64;
        if now < epoch_end && now >= next_retry_poll {
            upload_pending(
                runtime.upload_client,
                runtime.spool,
                runtime.central_url,
                runtime.api_key,
                runtime.probe_id,
            )
            .await;
            match runtime.retry_client.fetch_retries().await {
                Ok(envelope) => {
                    for group in execution_groups_until(&envelope.assignments, envelope.expires_at)
                    {
                        if !group.urgent
                            || group.not_after < Utc::now()
                            || seen_groups.contains(&group.group_id)
                        {
                            continue;
                        }
                        let group_id = group.group_id.clone();
                        if has_queue_capacity(pending.len(), running.len()) {
                            enqueue_retry(&mut pending, group);
                            seen_groups.insert(group_id);
                        } else if let Some(displaced) =
                            prioritize_retry(&mut pending, running.len(), group)
                        {
                            runtime
                                .assignment_cache
                                .defer(
                                    &displaced.group_id,
                                    &displaced.deferred_assignment,
                                    Utc::now(),
                                    displaced.not_after,
                                )
                                .await?;
                            seen_groups.insert(group_id);
                        }
                    }
                }
                Err(error) => tracing::warn!(%error, "incident retry assignment refresh failed"),
            }
            next_retry_poll = now.saturating_add(10);
        }
        if upload_due {
            upload_pending(
                runtime.upload_client,
                runtime.spool,
                runtime.central_url,
                runtime.api_key,
                runtime.probe_id,
            )
            .await;
        }
        let admission_at = Utc::now();
        let admission_now = admission_at.timestamp().max(0) as u64;
        if admission_now >= epoch_end {
            let DeadlineWork { deferred, expired } =
                drain_unstarted_at_deadline(&mut jobs, &mut pending, admission_at);
            for group_key in expired {
                runtime
                    .assignment_cache
                    .complete_deferred(&group_key)
                    .await?;
            }
            for group in deferred {
                runtime
                    .assignment_cache
                    .defer(
                        &group.group_id,
                        &group.deferred_assignment,
                        admission_at,
                        group.not_after,
                    )
                    .await?;
            }
        } else {
            update_pressure(runtime.pressure, &mut limiter, &engine);
            while let Some(job_index) = next_due_job_index(&jobs, admission_at) {
                if jobs[job_index].not_after <= admission_at {
                    let expired = jobs.remove(job_index);
                    runtime
                        .assignment_cache
                        .complete_deferred(&expired.group_id)
                        .await?;
                    continue;
                }
                if queue_due_job(&mut jobs, job_index, &mut pending, running.len())
                    == ScheduledQueueAction::Wait
                {
                    break;
                }
            }

            let mut index = 0;
            while running.len() < 4 && index < pending.len() {
                if pending[index].not_after <= Utc::now() {
                    let expired = pending.remove(index);
                    seen_groups.remove(&expired.group_id);
                    runtime
                        .assignment_cache
                        .complete_deferred(&expired.group_id)
                        .await?;
                    continue;
                }
                let assigned = &pending[index];
                let admission = if assigned.deep {
                    limiter.admit_deep(&assigned.host, admission_now, assigned.request_units)
                } else {
                    limiter.admit(&assigned.host, admission_now, assigned.request_units)
                };
                match admission {
                    Admission::Granted => {
                        let assigned = pending.remove(index);
                        let group_key = assigned.group_id.clone();
                        let host = assigned.host.clone();
                        let engine = Arc::clone(&engine);
                        running.spawn(async move {
                            let mut observations = Vec::new();
                            for check in &assigned.checks {
                                let job = check.scheduled_job();
                                observations.extend(
                                    engine
                                        .check_all_with_references(
                                            &check.node,
                                            &job,
                                            site,
                                            schema_version,
                                            &check.reference_nodes,
                                        )
                                        .await,
                                );
                            }
                            let diagnostic = tcp_diagnostic_candidate(&assigned, &observations);
                            CompletedGroup {
                                group_key,
                                host,
                                observations,
                                diagnostic,
                            }
                        });
                    }
                    Admission::BudgetExceeded | Admission::PressurePaused => {
                        tracing::debug!(job_id = %assigned.group_id, ?admission, "job deferred to a later epoch");
                        let assigned = pending.remove(index);
                        if assigned.urgent {
                            seen_groups.remove(&assigned.group_id);
                        } else {
                            runtime
                                .assignment_cache
                                .defer(
                                    &assigned.group_id,
                                    &assigned.deferred_assignment,
                                    admission_at,
                                    assigned.not_after,
                                )
                                .await?;
                        }
                    }
                    Admission::RateLimited => break,
                    Admission::HostBusy | Admission::ConcurrencyLimited => {
                        index += 1;
                    }
                }
            }
        }

        // Normal and urgent assignments have priority. Guardian executes one task per poll
        // after all normal work has completed, using the existing admission and HTTP limiters.
        if guardian_enabled
            && admission_now >= next_guardian_poll
            && admission_now.saturating_add(30) < epoch_end
            && pending.is_empty()
            && running.is_empty()
            && diagnostics.is_empty()
            && jobs.is_empty()
        {
            next_guardian_poll = admission_now.saturating_add(10);
            guardian_counts.polls += 1;
            match monitor_probe::guardian::fetch(
                runtime.upload_client,
                runtime.central_url,
                runtime.api_key,
                runtime.probe_id,
                site,
            )
            .await
            {
                Ok(Some(work)) if work.valid(site, Utc::now()) => {
                    guardian_counts.claimed += 1;
                    match limiter.admit_deep(&work.node.host, admission_now, 4) {
                        Admission::Granted => {
                            guardian_counts.admitted += 1;
                            let observation = work.execute(&engine).await;
                            limiter.complete(&work.node.host);
                            if let Err(error) = monitor_probe::guardian::submit(
                                runtime.upload_client,
                                runtime.central_url,
                                runtime.api_key,
                                runtime.probe_id,
                                &work,
                                observation,
                            )
                            .await
                            {
                                guardian_counts.upload_failed += 1;
                                tracing::warn!(%error, "Guardian result upload failed");
                            } else {
                                guardian_counts.uploaded += 1;
                            }
                        }
                        Admission::BudgetExceeded => guardian_counts.budget_exceeded += 1,
                        Admission::PressurePaused => guardian_counts.pressure_paused += 1,
                        Admission::HostBusy => guardian_counts.host_busy += 1,
                        Admission::ConcurrencyLimited => guardian_counts.concurrency_limited += 1,
                        Admission::RateLimited => guardian_counts.rate_limited += 1,
                    }
                }
                Ok(Some(_)) => {
                    guardian_counts.claimed += 1;
                    guardian_counts.invalid += 1;
                }
                Ok(None) => guardian_counts.empty += 1,
                Err(error) => {
                    guardian_counts.poll_failed += 1;
                    tracing::debug!(%error, "Guardian work poll failed");
                }
            }
        }

        if guardian_enabled
            && admission_now.saturating_add(10) < epoch_end
            && jobs.is_empty()
            && pending.is_empty()
            && running.is_empty()
            && diagnostics.is_empty()
            && admission_now >= next_guardian_dns_poll
        {
            next_guardian_dns_poll = admission_now.saturating_add(10);
            match monitor_probe::guardian::fetch_dns(
                runtime.upload_client,
                runtime.central_url,
                runtime.api_key,
                runtime.probe_id,
                site,
            )
            .await
            {
                Ok(Some(work)) if work.valid(site, Utc::now()) => {
                    if limiter.admit_deep(&work.endpoint.endpoint_host, admission_now, 1)
                        == Admission::Granted
                    {
                        let result = work.resolve(runtime.probe_id).await;
                        limiter.complete(&work.endpoint.endpoint_host);
                        if let Err(error) = monitor_probe::guardian::submit_dns(
                            runtime.upload_client,
                            runtime.central_url,
                            runtime.api_key,
                            &result,
                        )
                        .await
                        {
                            tracing::warn!(%error,"Guardian DNS upload failed");
                        }
                    }
                }
                Ok(_) => {}
                Err(error) => tracing::debug!(%error,"Guardian DNS poll failed"),
            }
        }

        if admission_now < epoch_end
            || !jobs.is_empty()
            || !pending.is_empty()
            || !running.is_empty()
            || !diagnostics.is_empty()
        {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    if guardian_enabled {
        tracing::info!(
            ?site,
            epoch_end,
            polls = guardian_counts.polls,
            claimed = guardian_counts.claimed,
            empty = guardian_counts.empty,
            invalid = guardian_counts.invalid,
            poll_failed = guardian_counts.poll_failed,
            admitted = guardian_counts.admitted,
            budget_exceeded = guardian_counts.budget_exceeded,
            pressure_paused = guardian_counts.pressure_paused,
            host_busy = guardian_counts.host_busy,
            concurrency_limited = guardian_counts.concurrency_limited,
            rate_limited = guardian_counts.rate_limited,
            uploaded = guardian_counts.uploaded,
            upload_failed = guardian_counts.upload_failed,
            "Guardian epoch summary"
        );
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct HostPressure {
    cpu_percent: u8,
    free_memory_percent: u8,
    disk_pressure: bool,
    stabilized_pressure: bool,
}

impl HostPressure {
    const fn degraded() -> Self {
        Self {
            cpu_percent: 100,
            free_memory_percent: 0,
            disk_pressure: true,
            stabilized_pressure: true,
        }
    }

    const fn active(self) -> bool {
        self.cpu_percent >= 70
            || self.free_memory_percent < 15
            || self.disk_pressure
            || self.stabilized_pressure
    }
}

#[derive(Debug, Clone, Copy)]
struct PressureReading {
    sampled_at: Instant,
    sample: Option<HostPressure>,
}

impl PressureReading {
    fn effective(self, now: Instant) -> HostPressure {
        if now.saturating_duration_since(self.sampled_at) > Duration::from_secs(20) {
            HostPressure::degraded()
        } else {
            self.sample.unwrap_or_else(HostPressure::degraded)
        }
    }
}

#[derive(Debug, Default)]
struct HostPressureSampler {
    previous_cpu: Option<(Instant, u64)>,
    gate: monitor_probe::pressure::PressureGate,
    last_paused: Option<bool>,
}

impl HostPressureSampler {
    fn sample(&mut self, disk_pressure: bool) -> Option<HostPressure> {
        let usage_micros = read_keyed_u64("/sys/fs/cgroup/cpu.stat", "usage_usec")?;
        let cpu_max = std::fs::read_to_string("/sys/fs/cgroup/cpu.max").ok()?;
        let (quota, period) = parse_cpu_max(&cpu_max)?;
        let current = read_trimmed_u64("/sys/fs/cgroup/memory.current")?;
        let maximum = read_trimmed_u64("/sys/fs/cgroup/memory.max")?;
        let mut sample = self.sample_values(
            Instant::now(),
            usage_micros,
            quota,
            period,
            current,
            maximum,
            disk_pressure,
        )?;
        let memory = monitor_probe::pressure::memory_reading(None);
        let paused = self.gate.update(
            Some(sample.cpu_percent),
            memory.map(|v| v.0),
            memory.map(|v| v.1),
            disk_pressure,
        );
        sample.free_memory_percent = memory.map_or(0, |v| 100 - v.0);
        sample.stabilized_pressure = paused;
        if self.last_paused != Some(paused) {
            tracing::info!(paused,cpu_percent=sample.cpu_percent,memory_current_bytes=current,memory_max_bytes=maximum,
                working_memory_percent=?memory.map(|v|v.0),memory_stall_avg10=?memory.map(|v|v.1),disk_pressure,
                "probe resource admission changed");
            self.last_paused = Some(paused);
        }
        Some(sample)
    }

    #[allow(clippy::too_many_arguments)]
    fn sample_values(
        &mut self,
        now: Instant,
        usage_micros: u64,
        quota: u64,
        period: u64,
        current: u64,
        maximum: u64,
        disk_pressure: bool,
    ) -> Option<HostPressure> {
        let (previous_at, previous_usage) = self.previous_cpu.replace((now, usage_micros))?;
        let cpu_percent = normalized_cpu_percent(
            usage_micros.saturating_sub(previous_usage),
            now.saturating_duration_since(previous_at),
            quota,
            period,
        );
        let free_memory_percent = maximum
            .saturating_sub(current)
            .saturating_mul(100)
            .checked_div(maximum)
            .and_then(|value| u8::try_from(value.min(100)).ok())?;
        Some(HostPressure {
            cpu_percent,
            free_memory_percent,
            disk_pressure,
            stabilized_pressure: false,
        })
    }
}

fn spawn_host_pressure_sampler(
    spool: Spool,
    engine: Arc<ProbeEngine>,
) -> watch::Receiver<PressureReading> {
    let (sender, receiver) = watch::channel(PressureReading {
        sampled_at: Instant::now(),
        sample: None,
    });
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut sampler = HostPressureSampler::default();
        loop {
            interval.tick().await;
            let disk_pressure = spool
                .stats()
                .await
                .map_or(true, |stats| stats.spool_pressure != SpoolPressure::Normal);
            let sample = sampler.sample(disk_pressure);
            if sample.is_none() {
                tracing::warn!("host pressure metrics are unavailable; using degraded mode");
            }
            engine.set_host_pressure(sample.unwrap_or_else(HostPressure::degraded).active());
            sender.send_replace(PressureReading {
                sampled_at: Instant::now(),
                sample,
            });
        }
    });
    receiver
}

fn update_pressure(
    reading: &watch::Receiver<PressureReading>,
    limiter: &mut AdmissionController,
    engine: &ProbeEngine,
) {
    let pressure = reading.borrow().effective(Instant::now());
    limiter.update_host_pressure(
        pressure.cpu_percent,
        pressure.free_memory_percent,
        pressure.disk_pressure || pressure.stabilized_pressure,
    );
    engine.set_host_pressure(pressure.active());
}

fn read_trimmed_u64(path: &str) -> Option<u64> {
    std::fs::read_to_string(path).ok()?.trim().parse().ok()
}

fn read_keyed_u64(path: &str, key: &str) -> Option<u64> {
    std::fs::read_to_string(path)
        .ok()?
        .lines()
        .find_map(|line| {
            let (candidate, value) = line.split_once(' ')?;
            (candidate == key)
                .then(|| value.trim().parse().ok())
                .flatten()
        })
}

fn parse_cpu_max(value: &str) -> Option<(u64, u64)> {
    let mut fields = value.split_whitespace();
    let quota = fields.next()?.parse().ok()?;
    let period = fields.next()?.parse().ok()?;
    (quota > 0 && period > 0 && fields.next().is_none()).then_some((quota, period))
}

fn normalized_cpu_percent(usage_micros: u64, elapsed: Duration, quota: u64, period: u64) -> u8 {
    let allowed = elapsed
        .as_micros()
        .saturating_mul(u128::from(quota))
        .checked_div(u128::from(period))
        .unwrap_or_default();
    if allowed == 0 {
        return 0;
    }
    u8::try_from(
        u128::from(usage_micros)
            .saturating_mul(100)
            .checked_div(allowed)
            .unwrap_or_default()
            .min(100),
    )
    .unwrap_or(100)
}

const fn has_queue_capacity(pending: usize, running: usize) -> bool {
    pending.saturating_add(running) < MAX_QUEUED_AND_RUNNING
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScheduledQueueAction {
    Enqueue,
    Displace(usize),
    Wait,
}

fn scheduled_queue_action(
    pending: &[ExecutionGroup],
    running: usize,
    incoming_deep: bool,
) -> ScheduledQueueAction {
    if has_queue_capacity(pending.len(), running) {
        return ScheduledQueueAction::Enqueue;
    }
    if !incoming_deep {
        if let Some(index) = pending
            .iter()
            .rposition(|group| !group.urgent && group.deep)
        {
            return ScheduledQueueAction::Displace(index);
        }
    }
    ScheduledQueueAction::Wait
}

fn queue_due_job(
    jobs: &mut Vec<ExecutionGroup>,
    job_index: usize,
    pending: &mut Vec<ExecutionGroup>,
    running: usize,
) -> ScheduledQueueAction {
    let action = scheduled_queue_action(pending, running, jobs[job_index].deep);
    match action {
        ScheduledQueueAction::Enqueue => {
            let assigned = jobs.remove(job_index);
            enqueue_by_priority(pending, assigned);
        }
        ScheduledQueueAction::Displace(pending_index) => {
            let assigned = jobs.remove(job_index);
            let displaced = pending.remove(pending_index);
            jobs.push(displaced);
            enqueue_by_priority(pending, assigned);
        }
        ScheduledQueueAction::Wait => {}
    }
    action
}

fn execution_priority(group: &ExecutionGroup) -> u8 {
    if group.urgent {
        0
    } else if group.deep {
        2
    } else {
        1
    }
}

fn next_due_job_index(jobs: &[ExecutionGroup], now: DateTime<Utc>) -> Option<usize> {
    jobs.iter()
        .enumerate()
        .filter(|(_, group)| group.scheduled_at <= now)
        .min_by(|(_, left), (_, right)| {
            execution_priority(left)
                .cmp(&execution_priority(right))
                .then_with(|| left.not_after.cmp(&right.not_after))
                .then_with(|| left.scheduled_at.cmp(&right.scheduled_at))
                .then_with(|| left.group_id.cmp(&right.group_id))
        })
        .map(|(index, _)| index)
}

#[derive(Debug)]
struct DeadlineWork {
    deferred: Vec<ExecutionGroup>,
    expired: Vec<String>,
}

fn drain_unstarted_at_deadline(
    jobs: &mut Vec<ExecutionGroup>,
    pending: &mut Vec<ExecutionGroup>,
    now: DateTime<Utc>,
) -> DeadlineWork {
    let mut deferred = Vec::with_capacity(jobs.len().saturating_add(pending.len()));
    let mut expired = Vec::new();
    for group in jobs.drain(..).chain(pending.drain(..)) {
        if group.urgent {
            continue;
        }
        if group.not_after <= now {
            expired.push(group.group_id);
        } else {
            deferred.push(group);
        }
    }
    DeadlineWork { deferred, expired }
}

fn execution_groups_until(
    assignments: &[Assignment],
    fallback_not_after: DateTime<Utc>,
) -> Vec<ExecutionGroup> {
    let mut grouped = BTreeMap::<(String, String, bool), (Vec<AssignedCheck>, Assignment)>::new();
    for assignment in assignments {
        match assignment.assigned_checks() {
            Ok(checks) => {
                for check in checks {
                    let deep = !matches!(
                        check.check,
                        monitor_core::scheduler::CheckKind::Availability
                            | monitor_core::scheduler::CheckKind::PeerTcp
                    );
                    grouped
                        .entry((
                            check.assignment_job_id.clone(),
                            check.node.host.clone(),
                            deep,
                        ))
                        .or_insert_with(|| (Vec::new(), assignment.clone()))
                        .0
                        .push(check);
                }
            }
            Err(error) => {
                tracing::error!(%error, job_id = %assignment.job_id, "validated assignment could not be prepared");
            }
        }
    }
    grouped
        .into_values()
        .filter_map(|(checks, assignment)| {
            ExecutionGroup::new(checks, assignment, fallback_not_after)
        })
        .collect()
}

fn coalesce_execution_groups(jobs: Vec<ExecutionGroup>) -> Vec<ExecutionGroup> {
    let mut latest = BTreeMap::<String, ExecutionGroup>::new();
    for job in jobs {
        match latest.entry(job.group_id.clone()) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(job);
            }
            std::collections::btree_map::Entry::Occupied(mut entry)
                if job.scheduled_at >= entry.get().scheduled_at =>
            {
                entry.insert(job);
            }
            std::collections::btree_map::Entry::Occupied(_) => {}
        }
    }
    let mut jobs = latest.into_values().collect::<Vec<_>>();
    jobs.sort_by_key(|job| (job.scheduled_at, job.group_id.clone()));
    jobs
}

#[derive(Debug, Clone)]
struct ExecutionGroup {
    group_id: String,
    host: String,
    scheduled_at: chrono::DateTime<Utc>,
    request_units: u8,
    deep: bool,
    urgent: bool,
    not_after: DateTime<Utc>,
    deferred_assignment: Assignment,
    checks: Vec<AssignedCheck>,
}

impl ExecutionGroup {
    fn new(
        checks: Vec<AssignedCheck>,
        mut source: Assignment,
        fallback_not_after: DateTime<Utc>,
    ) -> Option<Self> {
        let first = checks.first()?;
        let request_units = checks
            .iter()
            .try_fold(0_u8, |total, check| total.checked_add(check.request_units))?;
        let deep = !matches!(
            first.check,
            monitor_core::scheduler::CheckKind::Availability
                | monitor_core::scheduler::CheckKind::PeerTcp
        );
        source.checks = checks.iter().map(|check| check.check).collect();
        source.request_units = request_units;
        let urgent = source.not_after.is_some();
        let not_after = source.not_after.unwrap_or(fallback_not_after);
        let group_id = deferred_group_key(&source).ok()?;
        Some(Self {
            group_id,
            host: first.node.host.clone(),
            scheduled_at: first.scheduled_at,
            request_units,
            checks,
            deep,
            urgent,
            not_after,
            deferred_assignment: source,
        })
    }
}

fn prioritize_retry(
    pending: &mut Vec<ExecutionGroup>,
    running: usize,
    retry: ExecutionGroup,
) -> Option<ExecutionGroup> {
    if has_queue_capacity(pending.len(), running) {
        enqueue_retry(pending, retry);
        return None;
    }
    let displaced_index = pending.iter().rposition(|group| !group.urgent)?;
    let displaced = pending.remove(displaced_index);
    enqueue_retry(pending, retry);
    Some(displaced)
}

fn enqueue_retry(pending: &mut Vec<ExecutionGroup>, retry: ExecutionGroup) {
    enqueue_by_priority(pending, retry);
}

fn enqueue_by_priority(pending: &mut Vec<ExecutionGroup>, group: ExecutionGroup) {
    let priority = execution_priority(&group);
    let index = pending
        .iter()
        .position(|existing| {
            priority < execution_priority(existing)
                || (priority == execution_priority(existing)
                    && (group.not_after, group.scheduled_at, &group.group_id)
                        < (
                            existing.not_after,
                            existing.scheduled_at,
                            &existing.group_id,
                        ))
        })
        .unwrap_or(pending.len());
    pending.insert(index, group);
}

async fn upload_pending(
    client: &reqwest::Client,
    spool: &Spool,
    central_url: &str,
    api_key: &str,
    probe_id: &str,
) {
    let mut after_sequence = 0;
    loop {
        let (observations, uploaded_ids) =
            match next_decodable_batch(spool, &mut after_sequence).await {
                Ok(Some(batch)) => batch,
                Ok(None) => return,
                Err(error) => {
                    tracing::error!(%error, "failed to read pending spool observations");
                    return;
                }
            };
        let batch = ObservationBatch {
            probe_id: probe_id.to_owned(),
            nodes: Vec::new(),
            observations,
        };
        let response = client
            .post(format!("{central_url}/internal/v1/observation-batches"))
            .bearer_auth(api_key)
            .json(&batch)
            .send()
            .await;
        match response {
            Ok(response) => {
                let status = response.status();
                let body = match response.bytes().await {
                    Ok(body) => body,
                    Err(error) => {
                        tracing::warn!(%error, %status, "observation acknowledgement body failed; observations remain spooled");
                        return;
                    }
                };
                if !confirms_ingest_result(status, &body, uploaded_ids.len()) {
                    tracing::warn!(
                        %status,
                        sent = uploaded_ids.len(),
                        "observation acknowledgement was incomplete; observations remain spooled"
                    );
                    return;
                }
                let ids = uploaded_ids.iter().map(String::as_str).collect::<Vec<_>>();
                if let Err(error) = spool.ack(&ids).await {
                    tracing::error!(%error, "failed to acknowledge uploaded observations");
                    return;
                }
            }
            Err(error) => {
                tracing::warn!(%error, "central API unavailable; observations remain spooled");
                return;
            }
        }
    }
}

fn confirms_ingest_result(status: reqwest::StatusCode, body: &[u8], sent: usize) -> bool {
    status == reqwest::StatusCode::ACCEPTED
        && serde_json::from_slice::<IngestResult>(body)
            .is_ok_and(|result| result.accepted.checked_add(result.duplicates) == Some(sent))
}

async fn next_decodable_batch(
    spool: &Spool,
    after_sequence: &mut i64,
) -> Result<Option<(Vec<Observation>, Vec<String>)>, SpoolError> {
    loop {
        let items = spool.pending_after(*after_sequence, 100).await?;
        let Some(last) = items.last() else {
            return Ok(None);
        };
        *after_sequence = last.sequence;
        let decoded = decode_pending_items(&items);
        if !decoded.0.is_empty() {
            return Ok(Some(decoded));
        }
        tracing::error!(
            after_sequence = *after_sequence,
            "spool page contains no decodable observations; scanning later rows"
        );
    }
}

fn decode_pending_items(items: &[SpoolItem]) -> (Vec<Observation>, Vec<String>) {
    let mut observations = Vec::with_capacity(items.len());
    let mut uploaded_ids = Vec::with_capacity(items.len());
    for item in items {
        match serde_json::from_slice::<Observation>(&item.payload) {
            Ok(observation) if observation.job_id == item.job_id => {
                observations.push(observation);
                uploaded_ids.push(item.job_id.clone());
            }
            Ok(observation) => {
                tracing::error!(
                    row_job_id = %item.job_id,
                    payload_job_id = %observation.job_id,
                    "mismatched spool observation retained without acknowledgement"
                );
            }
            Err(error) => {
                tracing::error!(
                    %error,
                    job_id = %item.job_id,
                    "corrupt spool observation retained without acknowledgement"
                );
            }
        }
    }
    (observations, uploaded_ids)
}

fn spawn_heartbeat(
    client: reqwest::Client,
    spool: Spool,
    central_url: String,
    api_key: String,
    probe_id: String,
) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        loop {
            interval.tick().await;
            let (queue_depth, spool_bytes, spool_pressure) = match spool.stats().await {
                Ok(stats) => (stats.queue_depth, stats.spool_bytes, stats.spool_pressure),
                Err(error) => {
                    tracing::error!(%error, "failed to read spool pressure");
                    (0, 0, SpoolPressure::Full)
                }
            };
            if spool_pressure != SpoolPressure::Normal {
                tracing::warn!(
                    queue_depth,
                    spool_bytes,
                    pressure = ?spool_pressure,
                    "probe spool is under pressure"
                );
            }
            let heartbeat = json!({
                "probeId": probe_id,
                "sentAt": Utc::now(),
                "queueDepth": queue_depth,
                "spoolBytes": spool_bytes,
                "spoolPressure": spool_pressure,
            });
            match client
                .post(format!("{central_url}/internal/v1/heartbeat"))
                .bearer_auth(&api_key)
                .json(&heartbeat)
                .send()
                .await
            {
                Ok(response) if response.status().is_success() => {}
                Ok(response) => tracing::warn!(status = %response.status(), "heartbeat rejected"),
                Err(error) => tracing::warn!(%error, "heartbeat delivery failed"),
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use chrono::{DateTime, TimeZone, Utc};
    use monitor_core::{
        model::{Dimension, Network, NodeKind, Observation, Outcome, Site},
        scheduler::CheckKind,
    };
    use monitor_probe::{
        assignment::{Assignment, AssignmentTarget, AssignmentTargetKind},
        spool::SpoolItem,
    };

    use super::{
        coalesce_execution_groups, confirms_ingest_result, decode_pending_items,
        drain_unstarted_at_deadline, enqueue_by_priority, execution_groups_until, execution_window,
        has_queue_capacity, healthcheck_spool_path, install_rustls_crypto_provider,
        next_decodable_batch, next_due_job_index, normalized_cpu_percent, parse_cpu_max,
        parse_probe_limits, prioritize_retry, queue_due_job, tcp_diagnostic_admission,
        tcp_diagnostic_candidate, DeadlineWork, HostPressure, HostPressureSampler, PressureReading,
        ScheduledQueueAction, TcpDiagnosticAdmission,
    };

    #[test]
    fn installs_ring_as_the_process_crypto_provider() {
        install_rustls_crypto_provider().expect("ring provider should install");
        assert!(rustls::crypto::CryptoProvider::get_default().is_some());
        assert!(install_rustls_crypto_provider().is_err());
    }

    fn execution_group(
        suffix: char,
        check: CheckKind,
        scheduled_at: DateTime<Utc>,
        not_after: Option<DateTime<Utc>>,
    ) -> super::ExecutionGroup {
        let assignment = Assignment {
            job_id: suffix.to_string().repeat(64),
            node_id: format!("node-{suffix}"),
            network: Network::SymbolMainnet,
            node_kind: NodeKind::Api,
            targets: vec![AssignmentTarget {
                kind: if check == CheckKind::PeerTcp {
                    AssignmentTargetKind::PeerTcp
                } else {
                    AssignmentTargetKind::Rest
                },
                scheme: if check == CheckKind::PeerTcp {
                    "tcp".into()
                } else {
                    "https".into()
                },
                host: format!("node-{suffix}.example"),
                port: if check == CheckKind::PeerTcp {
                    7900
                } else {
                    3001
                },
                subject_public_key: None,
            }],
            checks: vec![check],
            scheduled_at,
            not_after,
            request_units: check.request_units(),
            detailed_owner: true,
        };
        execution_groups_until(&[assignment], DateTime::<Utc>::MAX_UTC)
            .pop()
            .unwrap()
    }

    #[test]
    fn probe_limits_default_to_three_requests_per_second() {
        let limits = parse_probe_limits(None, None).unwrap();

        assert_eq!(limits.global_requests_per_second, 3.0);
        assert_eq!(limits.burst, 4.0);
        assert_eq!(limits.concurrency, 4);
    }

    #[test]
    fn due_work_orders_urgent_then_connectivity_then_deep() {
        let now = Utc.with_ymd_and_hms(2026, 9, 14, 0, 1, 0).unwrap();
        let due = now - chrono::Duration::seconds(1);
        let mut jobs = vec![
            execution_group('d', CheckKind::Chain, due, None),
            execution_group('a', CheckKind::Availability, due, None),
            execution_group('u', CheckKind::Availability, due, Some(now)),
        ];

        let urgent = jobs.remove(next_due_job_index(&jobs, now).unwrap());
        let connectivity = jobs.remove(next_due_job_index(&jobs, now).unwrap());
        let deep = jobs.remove(next_due_job_index(&jobs, now).unwrap());

        assert!(urgent.urgent);
        assert!(!connectivity.urgent && !connectivity.deep);
        assert!(deep.deep);

        let mut pending = vec![deep];
        enqueue_by_priority(&mut pending, connectivity);
        enqueue_by_priority(&mut pending, urgent);
        assert!(pending[0].urgent);
        assert!(!pending[1].deep);
        assert!(pending[2].deep);
    }

    #[test]
    fn detailed_tcp_diagnostic_is_limited_to_failed_urgent_peer_checks() {
        let now = Utc.with_ymd_and_hms(2026, 9, 14, 0, 1, 0).unwrap();
        let urgent = execution_group(
            'u',
            CheckKind::PeerTcp,
            now,
            Some(now + chrono::Duration::minutes(1)),
        );
        let regular = execution_group('r', CheckKind::PeerTcp, now, None);
        let failed = Observation::new(
            urgent.checks[0].observation_job_id.clone(),
            urgent.checks[0].node.id.clone(),
            Site::A,
            Dimension::Connectivity,
            Outcome::Failure("peer TCP connection failed".into()),
            now,
        )
        .with_check_kind(CheckKind::PeerTcp);
        let healthy = Observation {
            outcome: Outcome::Healthy,
            ..failed.clone()
        };

        assert!(tcp_diagnostic_candidate(&urgent, std::slice::from_ref(&failed)).is_some());
        assert!(tcp_diagnostic_candidate(&regular, std::slice::from_ref(&failed)).is_none());
        assert!(tcp_diagnostic_candidate(&urgent, &[healthy]).is_none());
    }

    #[test]
    fn detailed_tcp_diagnostic_respects_pressure_deadline_singleton_and_cooldown() {
        let now = Utc.with_ymd_and_hms(2026, 9, 14, 0, 1, 0).unwrap();
        let clear = HostPressure {
            cpu_percent: 10,
            free_memory_percent: 80,
            disk_pressure: false,
            stabilized_pressure: false,
        };
        assert_eq!(
            tcp_diagnostic_admission(clear, now, now + chrono::Duration::seconds(11), false, None),
            TcpDiagnosticAdmission::Granted
        );
        assert_eq!(
            tcp_diagnostic_admission(
                HostPressure::degraded(),
                now,
                now + chrono::Duration::minutes(1),
                false,
                None
            ),
            TcpDiagnosticAdmission::Pressure
        );
        assert_eq!(
            tcp_diagnostic_admission(clear, now, now + chrono::Duration::seconds(10), false, None),
            TcpDiagnosticAdmission::Deadline
        );
        assert_eq!(
            tcp_diagnostic_admission(clear, now, now + chrono::Duration::minutes(1), true, None),
            TcpDiagnosticAdmission::Busy
        );
        assert_eq!(
            tcp_diagnostic_admission(
                clear,
                now,
                now + chrono::Duration::minutes(1),
                false,
                Some(now - chrono::Duration::minutes(29)),
            ),
            TcpDiagnosticAdmission::Cooldown
        );
        assert_eq!(
            tcp_diagnostic_admission(
                clear,
                now,
                now + chrono::Duration::minutes(1),
                false,
                Some(now - chrono::Duration::minutes(30)),
            ),
            TcpDiagnosticAdmission::Granted
        );
    }

    #[test]
    fn full_queue_waits_for_capacity_without_consuming_the_due_job() {
        let now = Utc.with_ymd_and_hms(2026, 9, 14, 0, 1, 0).unwrap();
        let group = execution_group('a', CheckKind::Availability, now, None);
        let mut jobs = vec![group.clone()];
        let mut pending = vec![group.clone(), group];

        assert_eq!(
            queue_due_job(&mut jobs, 0, &mut pending, 4),
            ScheduledQueueAction::Wait
        );
        assert_eq!(jobs.len(), 1);
        assert_eq!(pending.len(), 2);
    }

    #[test]
    fn due_connectivity_displaces_deep_pending_without_growing_the_queue() {
        let now = Utc.with_ymd_and_hms(2026, 9, 14, 0, 1, 0).unwrap();
        let deep = execution_group('d', CheckKind::Chain, now, None);
        let base = execution_group('a', CheckKind::Availability, now, None);
        let mut jobs = vec![base.clone()];
        let mut pending = vec![base, deep.clone()];

        assert_eq!(
            queue_due_job(&mut jobs, 0, &mut pending, 4),
            ScheduledQueueAction::Displace(1)
        );
        assert!(!has_queue_capacity(pending.len(), 4));
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].group_id, deep.group_id);
        assert!(pending.iter().all(|group| !group.deep));
    }

    #[test]
    fn epoch_deadline_defers_normal_work_once_and_discards_urgent_work() {
        let now = Utc.with_ymd_and_hms(2026, 9, 14, 0, 1, 0).unwrap();
        let normal = execution_group('a', CheckKind::Availability, now, None);
        let urgent = execution_group(
            'u',
            CheckKind::Availability,
            now,
            Some(now + chrono::Duration::seconds(30)),
        );
        let mut jobs = vec![normal.clone()];
        let mut pending = vec![urgent];

        let DeadlineWork { deferred, expired } =
            drain_unstarted_at_deadline(&mut jobs, &mut pending, now);

        assert_eq!(deferred.len(), 1);
        assert_eq!(deferred[0].group_id, normal.group_id);
        assert!(expired.is_empty());
        assert!(jobs.is_empty() && pending.is_empty());
        assert!(drain_unstarted_at_deadline(&mut jobs, &mut pending, now)
            .deferred
            .is_empty());
    }

    #[test]
    fn probe_limits_accept_a_two_point_five_ops_per_second_trial() {
        let limits = parse_probe_limits(Some("2.5"), Some("4")).unwrap();

        assert_eq!(limits.global_requests_per_second, 2.5);
        assert_eq!(limits.burst, 4.0);
        assert_eq!(limits.concurrency, 4);
    }

    #[test]
    fn probe_limits_reject_zero_nan_and_infinite_values() {
        for value in ["0", "NaN", "inf", "-1"] {
            assert!(parse_probe_limits(Some(value), Some("4")).is_err());
            assert!(parse_probe_limits(Some("2.5"), Some(value)).is_err());
        }
    }

    #[test]
    fn grouped_comparison_checks_share_one_sequential_host_execution() {
        let assignment = Assignment {
            job_id: "a".repeat(64),
            node_id: "symbol-1".into(),
            network: Network::SymbolMainnet,
            node_kind: NodeKind::Api,
            targets: vec![AssignmentTarget {
                kind: AssignmentTargetKind::Rest,
                scheme: "https".into(),
                host: "node.example".into(),
                port: 3001,
                subject_public_key: None,
            }],
            checks: vec![
                CheckKind::Chain,
                CheckKind::Finalization,
                CheckKind::StateHash,
            ],
            scheduled_at: Utc.with_ymd_and_hms(2026, 8, 9, 0, 0, 0).unwrap(),
            not_after: None,
            request_units: 3,
            detailed_owner: true,
        };

        let groups = execution_groups_until(&[assignment], DateTime::<Utc>::MAX_UTC);

        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].host, "node.example");
        assert_eq!(groups[0].request_units, 3);
        assert_eq!(
            groups[0]
                .checks
                .iter()
                .map(|check| check.check)
                .collect::<Vec<_>>(),
            [
                CheckKind::Chain,
                CheckKind::Finalization,
                CheckKind::StateHash
            ]
        );
    }

    #[test]
    fn production_queue_capacity_and_pressure_math_use_frozen_boundaries() {
        assert!(has_queue_capacity(2, 3));
        assert!(!has_queue_capacity(2, 4));
        assert!(!has_queue_capacity(usize::MAX, 1));
        assert_eq!(parse_cpu_max("20000 100000\n"), Some((20_000, 100_000)));
        assert_eq!(parse_cpu_max("max 100000"), None);
        assert_eq!(
            normalized_cpu_percent(140_000, Duration::from_secs(1), 20_000, 100_000),
            70
        );
        assert_eq!(
            normalized_cpu_percent(200_000, Duration::from_secs(1), 20_000, 100_000),
            100
        );
    }

    #[test]
    fn availability_and_deep_checks_split_for_pressure_degradation() {
        let assignment = Assignment {
            job_id: "b".repeat(64),
            node_id: "symbol-pressure".into(),
            network: Network::SymbolMainnet,
            node_kind: NodeKind::Api,
            targets: vec![AssignmentTarget {
                kind: AssignmentTargetKind::Rest,
                scheme: "https".into(),
                host: "pressure.example".into(),
                port: 3001,
                subject_public_key: None,
            }],
            checks: vec![CheckKind::Availability, CheckKind::Chain],
            scheduled_at: Utc.with_ymd_and_hms(2026, 8, 9, 0, 0, 0).unwrap(),
            not_after: None,
            request_units: 2,
            detailed_owner: true,
        };

        let groups = execution_groups_until(&[assignment], DateTime::<Utc>::MAX_UTC);
        assert_eq!(groups.len(), 2);
        assert_eq!(groups.iter().filter(|group| group.deep).count(), 1);
        assert_eq!(groups.iter().filter(|group| !group.deep).count(), 1);
        assert!(groups
            .iter()
            .all(|group| group.deferred_assignment.checks.len() == 1));
    }

    #[test]
    fn normal_execution_group_identity_is_stable_across_assignment_epochs() {
        let older = Assignment {
            job_id: "1".repeat(64),
            node_id: "stable-deferred-node".into(),
            network: Network::SymbolMainnet,
            node_kind: NodeKind::Api,
            targets: vec![AssignmentTarget {
                kind: AssignmentTargetKind::Rest,
                scheme: "https".into(),
                host: "stable.example".into(),
                port: 3001,
                subject_public_key: None,
            }],
            checks: vec![CheckKind::Chain],
            scheduled_at: Utc.with_ymd_and_hms(2026, 8, 9, 0, 2, 0).unwrap(),
            not_after: None,
            request_units: 1,
            detailed_owner: true,
        };
        let mut newer = older.clone();
        newer.job_id = "2".repeat(64);
        newer.scheduled_at += chrono::Duration::minutes(10);

        let older_group = execution_groups_until(&[older], DateTime::<Utc>::MAX_UTC)
            .pop()
            .unwrap();
        let newer_group = execution_groups_until(&[newer], DateTime::<Utc>::MAX_UTC)
            .pop()
            .unwrap();

        assert_eq!(older_group.group_id, newer_group.group_id);
        assert_ne!(
            older_group.checks[0].observation_job_id, newer_group.checks[0].observation_job_id,
            "replacement identity must not collapse observation replay IDs"
        );
    }

    #[test]
    fn current_epoch_replaces_an_older_restored_group_before_execution() {
        let older = Assignment {
            job_id: "3".repeat(64),
            node_id: "latest-deferred-node".into(),
            network: Network::SymbolMainnet,
            node_kind: NodeKind::Api,
            targets: vec![AssignmentTarget {
                kind: AssignmentTargetKind::Rest,
                scheme: "https".into(),
                host: "latest.example".into(),
                port: 3001,
                subject_public_key: None,
            }],
            checks: vec![CheckKind::Chain],
            scheduled_at: Utc.with_ymd_and_hms(2026, 8, 9, 0, 2, 0).unwrap(),
            not_after: None,
            request_units: 1,
            detailed_owner: true,
        };
        let mut newer = older.clone();
        newer.job_id = "4".repeat(64);
        newer.scheduled_at += chrono::Duration::minutes(10);
        let older_group = execution_groups_until(&[older], DateTime::<Utc>::MAX_UTC)
            .pop()
            .unwrap();
        let newer_group = execution_groups_until(&[newer], DateTime::<Utc>::MAX_UTC)
            .pop()
            .unwrap();

        let groups = coalesce_execution_groups(vec![newer_group.clone(), older_group]);

        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].scheduled_at, newer_group.scheduled_at);
        assert_eq!(
            groups[0].checks[0].observation_job_id,
            newer_group.checks[0].observation_job_id
        );
    }

    #[test]
    fn urgent_retry_identity_remains_distinct_from_normal_epoch_replacement() {
        let older = Assignment {
            job_id: "5".repeat(64),
            node_id: "retry-identity-node".into(),
            network: Network::SymbolMainnet,
            node_kind: NodeKind::Api,
            targets: vec![AssignmentTarget {
                kind: AssignmentTargetKind::Rest,
                scheme: "https".into(),
                host: "retry-identity.example".into(),
                port: 3001,
                subject_public_key: None,
            }],
            checks: vec![CheckKind::Availability],
            scheduled_at: Utc.with_ymd_and_hms(2026, 8, 9, 0, 0, 30).unwrap(),
            not_after: Some(Utc.with_ymd_and_hms(2026, 8, 9, 0, 1, 0).unwrap()),
            request_units: 1,
            detailed_owner: true,
        };
        let mut newer = older.clone();
        newer.job_id = "6".repeat(64);
        newer.scheduled_at += chrono::Duration::seconds(10);
        newer.not_after = Some(Utc.with_ymd_and_hms(2026, 8, 9, 0, 1, 10).unwrap());

        let older_group = execution_groups_until(&[older], DateTime::<Utc>::MAX_UTC)
            .pop()
            .unwrap();
        let newer_group = execution_groups_until(&[newer], DateTime::<Utc>::MAX_UTC)
            .pop()
            .unwrap();

        assert_ne!(older_group.group_id, newer_group.group_id);
        assert_eq!(
            coalesce_execution_groups(vec![older_group, newer_group]).len(),
            2
        );
    }

    #[test]
    fn retry_deadline_displaces_normal_pending_work_at_capacity() {
        let assignment = Assignment {
            job_id: "c".repeat(64),
            node_id: "retry-priority".into(),
            network: Network::SymbolMainnet,
            node_kind: NodeKind::Api,
            targets: vec![AssignmentTarget {
                kind: AssignmentTargetKind::Rest,
                scheme: "https".into(),
                host: "retry.example".into(),
                port: 3001,
                subject_public_key: None,
            }],
            checks: vec![CheckKind::Availability],
            scheduled_at: Utc.with_ymd_and_hms(2026, 8, 9, 0, 0, 30).unwrap(),
            not_after: Some(Utc.with_ymd_and_hms(2026, 8, 9, 0, 1, 0).unwrap()),
            request_units: 1,
            detailed_owner: true,
        };
        let retry = execution_groups_until(&[assignment], DateTime::<Utc>::MAX_UTC)
            .pop()
            .unwrap();
        assert!(retry.urgent);
        assert_eq!(
            retry.not_after,
            Utc.with_ymd_and_hms(2026, 8, 9, 0, 1, 0).unwrap()
        );

        let mut normal = retry.clone();
        normal.urgent = false;
        normal.not_after = Utc.with_ymd_and_hms(2026, 8, 10, 0, 0, 0).unwrap();
        let mut pending = vec![normal.clone(), normal];
        let displaced = prioritize_retry(&mut pending, 4, retry).unwrap();

        assert!(!displaced.urgent);
        assert_eq!(pending.len(), 2);
        assert_eq!(pending.iter().filter(|group| group.urgent).count(), 1);
        assert!(pending[0].urgent, "retry must be the next dispatched work");
    }

    #[test]
    fn first_cpu_sample_is_unavailable_instead_of_reporting_zero_percent() {
        let mut sampler = HostPressureSampler::default();
        let start = std::time::Instant::now();

        assert_eq!(
            sampler.sample_values(start, 1_000, 20_000, 100_000, 10, 100, false),
            None
        );
        assert_eq!(
            sampler
                .sample_values(
                    start + Duration::from_secs(1),
                    141_000,
                    20_000,
                    100_000,
                    10,
                    100,
                    false,
                )
                .unwrap()
                .cpu_percent,
            70
        );
    }

    #[test]
    fn stale_pressure_sample_fails_safe() {
        let start = std::time::Instant::now();
        let healthy = PressureReading {
            sampled_at: start,
            sample: Some(HostPressure {
                cpu_percent: 1,
                free_memory_percent: 99,
                disk_pressure: false,
                stabilized_pressure: false,
            }),
        };

        assert!(!healthy.effective(start + Duration::from_secs(20)).active());
        assert!(healthy.effective(start + Duration::from_secs(21)).active());
    }

    #[test]
    fn site_b_receives_a_full_shifted_ten_minute_execution_window() {
        assert_eq!(execution_window(750, Site::A, 1), (1, 600, 1_200));
        assert_eq!(execution_window(750, Site::B, 1), (1, 900, 1_500));
        assert_eq!(execution_window(950, Site::B, 1), (1, 900, 1_500));
        assert_eq!(execution_window(1_501, Site::B, 1), (2, 1_500, 2_100));
    }

    #[test]
    fn corrupt_spool_rows_are_never_included_in_upload_acknowledgements() {
        let observation = Observation::new(
            "good-job",
            "node-1",
            Site::A,
            Dimension::Connectivity,
            Outcome::Healthy,
            Utc::now(),
        );
        let items = vec![
            SpoolItem {
                sequence: 1,
                job_id: "corrupt-job".into(),
                payload: b"{truncated".to_vec(),
            },
            SpoolItem {
                sequence: 2,
                job_id: "good-job".into(),
                payload: serde_json::to_vec(&observation).unwrap(),
            },
            SpoolItem {
                sequence: 3,
                job_id: "mismatched-row-id".into(),
                payload: serde_json::to_vec(&observation).unwrap(),
            },
        ];

        let (observations, acknowledged_ids) = decode_pending_items(&items);
        assert_eq!(observations.len(), 1);
        assert_eq!(observations[0].job_id, "good-job");
        assert_eq!(acknowledged_ids, ["good-job"]);
    }

    #[tokio::test]
    async fn one_hundred_corrupt_rows_do_not_block_a_later_valid_observation() {
        let spool = monitor_probe::spool::Spool::in_memory(10_000)
            .await
            .unwrap();
        for index in 0..100 {
            spool
                .enqueue(&format!("corrupt-{index}"), b"{")
                .await
                .unwrap();
        }
        let observation = Observation::new(
            "valid-after-corruption",
            "node-1",
            Site::A,
            Dimension::Connectivity,
            Outcome::Healthy,
            Utc::now(),
        );
        spool
            .enqueue(
                &observation.job_id,
                &serde_json::to_vec(&observation).unwrap(),
            )
            .await
            .unwrap();

        let mut after_sequence = 0;
        let (observations, acknowledged_ids) = next_decodable_batch(&spool, &mut after_sequence)
            .await
            .unwrap()
            .expect("the valid row behind corrupt rows should be reached");
        assert_eq!(observations.len(), 1);
        assert_eq!(acknowledged_ids, ["valid-after-corruption"]);
        assert_eq!(spool.stats().await.unwrap().queue_depth, 101);
    }

    #[test]
    fn healthcheck_does_not_treat_sqlite_file_length_as_logical_spool_usage() {
        let directory = tempfile::TempDir::new().unwrap();
        let path = directory.path().join("spool.db");
        let file = std::fs::File::create(&path).unwrap();
        file.set_len(101 * 1024 * 1024).unwrap();

        healthcheck_spool_path(&path).unwrap();
    }

    #[test]
    fn acknowledgement_requires_202_and_complete_ingest_counts() {
        let complete = br#"{"accepted":1,"duplicates":1}"#;
        assert!(confirms_ingest_result(
            reqwest::StatusCode::ACCEPTED,
            complete,
            2
        ));
        assert!(!confirms_ingest_result(
            reqwest::StatusCode::OK,
            complete,
            2
        ));
        assert!(!confirms_ingest_result(
            reqwest::StatusCode::ACCEPTED,
            br#"{"accepted":1,"duplicates":0}"#,
            2
        ));
        assert!(!confirms_ingest_result(
            reqwest::StatusCode::ACCEPTED,
            b"{truncated",
            2
        ));
    }
}
