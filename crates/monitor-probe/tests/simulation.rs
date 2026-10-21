use std::collections::HashMap;

use monitor_core::{
    model::{Network, Node, NodeKind},
    scheduler::{Scheduler, Site},
};
use monitor_probe::simulation::simulate_epoch;

#[test]
fn three_hundred_fifty_scheduled_jobs_finish_or_defer_explicitly_inside_ten_minutes() {
    let nodes = (0..350)
        .map(|index| {
            Node::new(
                format!("node-{index}"),
                Network::SymbolMainnet,
                NodeKind::Api,
                format!("node-{index}.example"),
            )
        })
        .collect::<Vec<_>>();
    let hosts = nodes
        .iter()
        .map(|node| (node.id.clone(), node.host.clone()))
        .collect::<HashMap<_, _>>();
    let jobs = Scheduler::default().jobs_for_epoch(Site::A, 1, &nodes);
    let report = simulate_epoch(&jobs, &hosts, 600);

    assert_eq!(report.completed + report.deferred, jobs.len());
    assert!(report.deferred > 0);
    assert_eq!(report.unfinished, 0);
    assert!(report.finished_at_seconds < 600);
    assert!(report.max_started_in_one_second <= 4);
    assert!(report.rate_bound_respected);
}
