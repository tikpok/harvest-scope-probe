//! Resource admission based on working memory and sustained recovery.
use std::fs;

pub fn working_memory_percent(current: u64, inactive_file: u64, maximum: u64) -> Option<u8> {
    if maximum == 0 || inactive_file > current {
        return None;
    }
    Some((current.saturating_sub(inactive_file).saturating_mul(100) / maximum).min(100) as u8)
}
pub fn memory_reading(configured_maximum: Option<u64>) -> Option<(u8, f64)> {
    let current = fs::read_to_string("/sys/fs/cgroup/memory.current")
        .ok()?
        .trim()
        .parse()
        .ok()?;
    let maximum: u64 = fs::read_to_string("/sys/fs/cgroup/memory.max")
        .ok()?
        .trim()
        .parse()
        .ok()?;
    let maximum = configured_maximum.map_or(maximum, |configured| configured.min(maximum));
    let stat = fs::read_to_string("/sys/fs/cgroup/memory.stat").ok()?;
    let inactive = stat
        .lines()
        .find_map(|line| line.strip_prefix("inactive_file ")?.parse().ok())?;
    let pressure = fs::read_to_string("/sys/fs/cgroup/memory.pressure").ok()?;
    let avg10: f64 = pressure
        .lines()
        .find(|line| line.starts_with("some "))?
        .split_whitespace()
        .find_map(|field| field.strip_prefix("avg10=")?.parse().ok())?;
    if !avg10.is_finite() || !(0.0..=100.0).contains(&avg10) {
        return None;
    }
    Some((working_memory_percent(current, inactive, maximum)?, avg10))
}
#[derive(Debug, Default)]
pub struct PressureGate {
    paused: bool,
    recovered_samples: u8,
}
impl PressureGate {
    /// Called at the periodic sampling cadence, never per request or diagnostic read.
    pub fn update(
        &mut self,
        cpu: Option<u8>,
        working_memory: Option<u8>,
        memory_stall: Option<f64>,
        disk: bool,
    ) -> bool {
        let Some((cpu, memory, stall)) = cpu
            .zip(working_memory)
            .zip(memory_stall)
            .map(|((c, m), s)| (c, m, s))
        else {
            self.paused = true;
            self.recovered_samples = 0;
            return true;
        };
        if disk || cpu >= 70 || memory >= 85 || !stall.is_finite() || stall >= 1.0 {
            self.paused = true;
            self.recovered_samples = 0;
        } else if self.paused {
            if cpu <= 50 && memory <= 80 && stall <= 0.1 {
                self.recovered_samples = self.recovered_samples.saturating_add(1);
                if self.recovered_samples >= 3 {
                    self.paused = false;
                    self.recovered_samples = 0;
                }
            } else {
                self.recovered_samples = 0;
            }
        }
        self.paused
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn reclaimable_cache_does_not_pause_an_idle_probe() {
        assert_eq!(working_memory_percent(400, 150, 448), Some(55));
        let mut gate = PressureGate::default();
        assert!(!gate.update(Some(5), Some(55), Some(0.0), false));
    }
    #[test]
    fn threshold_noise_and_read_only_diagnostics_cannot_toggle_recovery() {
        let mut gate = PressureGate::default();
        assert!(gate.update(Some(1), Some(85), Some(0.0), false));
        for usage in [84, 85, 83, 84] {
            assert!(gate.update(Some(2), Some(usage), Some(0.0), false));
        }
        assert!(gate.update(Some(1), Some(79), Some(0.0), false));
        assert!(gate.update(Some(1), Some(79), Some(0.0), false));
        assert!(!gate.update(Some(1), Some(79), Some(0.0), false));
    }
    #[test]
    fn real_stalls_and_missing_metrics_fail_closed() {
        let mut gate = PressureGate::default();
        assert!(gate.update(Some(1), Some(50), Some(1.0), false));
        assert!(gate.update(None, Some(50), Some(0.0), false));
        assert!(gate.update(Some(1), None, Some(0.0), false));
        assert!(gate.update(Some(1), Some(50), None, false));
    }
}
