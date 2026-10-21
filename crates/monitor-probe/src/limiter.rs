use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub concurrency: usize,
    pub global_requests_per_second: f64,
    pub burst: f64,
    pub node_units_per_ten_minutes: u8,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            concurrency: 4,
            global_requests_per_second: 3.0,
            burst: 4.0,
            node_units_per_ten_minutes: 6,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admission {
    Granted,
    HostBusy,
    ConcurrencyLimited,
    RateLimited,
    BudgetExceeded,
    PressurePaused,
}

#[derive(Debug)]
pub struct AdmissionController {
    limits: Limits,
    tokens: f64,
    last_refill: u64,
    active_hosts: HashSet<String>,
    units: HashMap<(String, u64), u8>,
    pressure: bool,
}

impl AdmissionController {
    #[must_use]
    pub fn new(limits: Limits) -> Self {
        Self {
            tokens: limits.burst,
            limits,
            last_refill: 0,
            active_hosts: HashSet::new(),
            units: HashMap::new(),
            pressure: false,
        }
    }

    pub fn update_host_pressure(
        &mut self,
        cpu_percent: u8,
        free_memory_percent: u8,
        disk_pressure: bool,
    ) {
        self.pressure = cpu_percent >= 70 || free_memory_percent < 15 || disk_pressure;
        if self.pressure {
            self.tokens = self.tokens.min(1.0);
        }
    }

    pub fn admit(&mut self, host: &str, now_seconds: u64, units: u8) -> Admission {
        self.admit_kind(host, now_seconds, units, false)
    }

    pub fn admit_availability(&mut self, host: &str, now_seconds: u64) -> Admission {
        self.admit_kind(host, now_seconds, 1, false)
    }

    pub fn admit_deep(&mut self, host: &str, now_seconds: u64, units: u8) -> Admission {
        self.admit_kind(host, now_seconds, units, true)
    }

    fn admit_kind(&mut self, host: &str, now_seconds: u64, units: u8, deep: bool) -> Admission {
        if self.pressure && deep {
            return Admission::PressurePaused;
        }
        if self.active_hosts.contains(host) {
            return Admission::HostBusy;
        }
        let concurrency = if self.pressure {
            1
        } else {
            self.limits.concurrency
        };
        if self.active_hosts.len() >= concurrency {
            return Admission::ConcurrencyLimited;
        }

        let window = now_seconds / 600;
        let budget_key = (host.to_owned(), window);
        let spent = self.units.get(&budget_key).copied().unwrap_or_default();
        if spent.saturating_add(units) > self.limits.node_units_per_ten_minutes {
            return Admission::BudgetExceeded;
        }

        self.refill(now_seconds);
        if self.tokens < 1.0 {
            return Admission::RateLimited;
        }
        self.tokens -= 1.0;
        self.units.insert(budget_key, spent + units);
        self.active_hosts.insert(host.to_owned());
        Admission::Granted
    }

    fn refill(&mut self, now_seconds: u64) {
        let elapsed = now_seconds.saturating_sub(self.last_refill);
        if elapsed > 0 {
            let rate = if self.pressure {
                0.25
            } else {
                self.limits.global_requests_per_second
            };
            let burst = if self.pressure {
                1.0
            } else {
                self.limits.burst
            };
            self.tokens = (self.tokens + elapsed as f64 * rate).min(burst);
            self.last_refill = now_seconds;
        }
    }

    pub fn complete(&mut self, host: &str) {
        self.active_hosts.remove(host);
    }
}
