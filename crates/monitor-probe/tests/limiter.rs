use monitor_probe::limiter::{Admission, AdmissionController, Limits};

#[test]
fn enforces_global_rate_burst_host_serial_and_ten_minute_budget() {
    let mut limiter = AdmissionController::new(Limits::default());

    assert_eq!(limiter.admit("node.example", 0, 2), Admission::Granted);
    assert_eq!(limiter.admit("node.example", 0, 1), Admission::HostBusy);
    limiter.complete("node.example");
    assert_eq!(limiter.admit("other.example", 0, 1), Admission::Granted);
    limiter.complete("other.example");
    assert_eq!(limiter.admit("third.example", 0, 1), Admission::Granted);
    limiter.complete("third.example");
    assert_eq!(limiter.admit("fourth.example", 0, 1), Admission::Granted);
    limiter.complete("fourth.example");
    assert_eq!(limiter.admit("fifth.example", 0, 1), Admission::RateLimited);

    assert_eq!(
        limiter.admit("node.example", 2, 5),
        Admission::BudgetExceeded
    );
}

#[test]
fn pressure_mode_preserves_availability_and_pauses_deep_work() {
    let mut limiter = AdmissionController::new(Limits::default());
    limiter.update_host_pressure(72, 20, false);
    assert_eq!(
        limiter.admit_availability("node.example", 0),
        Admission::Granted
    );
    limiter.complete("node.example");
    assert_eq!(
        limiter.admit_deep("node.example", 5, 1),
        Admission::PressurePaused
    );
}

#[test]
fn two_point_five_rate_refills_as_two_then_three_tokens_per_second() {
    let mut limiter = AdmissionController::new(Limits {
        global_requests_per_second: 2.5,
        ..Limits::default()
    });
    for index in 0..4 {
        let host = format!("burst-{index}.example");
        assert_eq!(limiter.admit(&host, 0, 1), Admission::Granted);
        limiter.complete(&host);
    }
    for index in 0..2 {
        let host = format!("second-one-{index}.example");
        assert_eq!(limiter.admit(&host, 1, 1), Admission::Granted);
        limiter.complete(&host);
    }
    assert_eq!(
        limiter.admit("second-one-blocked.example", 1, 1),
        Admission::RateLimited
    );
    for index in 0..3 {
        let host = format!("second-two-{index}.example");
        assert_eq!(limiter.admit(&host, 2, 1), Admission::Granted);
        limiter.complete(&host);
    }
}
