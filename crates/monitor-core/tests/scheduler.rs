use std::collections::{HashMap, HashSet};

use monitor_core::{
    model::{Network, Node, NodeKind},
    scheduler::{CheckKind, ScheduledJob, Scheduler, Site},
};

fn node(index: usize) -> Node {
    Node::new(
        format!("node-{index}"),
        if index.is_multiple_of(2) {
            Network::SymbolMainnet
        } else {
            Network::Nis1Mainnet
        },
        NodeKind::Api,
        format!("node-{index}.example"),
    )
}

#[test]
fn spreads_350_nodes_inside_the_epoch_without_host_collisions() {
    let nodes = (0..350).map(node).collect::<Vec<_>>();
    let scheduler = Scheduler::default();

    let jobs = scheduler.jobs_for_epoch(Site::A, 42, &nodes);
    let health_jobs = jobs
        .iter()
        .filter(|job| job.check == CheckKind::Availability)
        .collect::<Vec<_>>();

    assert_eq!(health_jobs.len(), 350);
    assert!(health_jobs.iter().all(|job| job.offset_seconds < 280));
    assert!(
        health_jobs.iter().any(|job| job.offset_seconds >= 240),
        "the production spread must use the added 40-second window"
    );

    let mut seen = HashSet::new();
    for job in health_jobs {
        assert!(seen.insert((job.node_id.clone(), job.check)));
    }
}

#[test]
fn alternates_detailed_ownership_and_offsets_site_b_by_five_minutes() {
    let nodes = (0..40).map(node).collect::<Vec<_>>();
    let scheduler = Scheduler::default();

    let a_even = scheduler.jobs_for_epoch(Site::A, 10, &nodes);
    let b_even = scheduler.jobs_for_epoch(Site::B, 10, &nodes);
    let a_odd = scheduler.jobs_for_epoch(Site::A, 11, &nodes);

    assert!(b_even
        .iter()
        .all(|job| job.site_epoch_offset_seconds == 300));

    let owners = |jobs: &[monitor_core::scheduler::ScheduledJob]| {
        jobs.iter()
            .filter(|job| job.check == CheckKind::Chain)
            .map(|job| job.node_id.clone())
            .collect::<HashSet<_>>()
    };
    let even_a = owners(&a_even);
    let even_b = owners(&b_even);
    let odd_a = owners(&a_odd);

    assert_eq!(even_a.len() + even_b.len(), nodes.len());
    assert!(even_a.is_disjoint(&even_b));
    assert_eq!(odd_a, even_b);
}

#[test]
fn peer_only_nodes_never_receive_chain_or_finalization_jobs() {
    let mut peer = node(2);
    peer.kind = NodeKind::PeerOnly;
    let scheduler = Scheduler::default();
    let mut jobs = Vec::new();
    for epoch in 0..36 {
        jobs.extend(scheduler.jobs_for_epoch(Site::A, epoch, std::slice::from_ref(&peer)));
        jobs.extend(scheduler.jobs_for_epoch(Site::B, epoch, std::slice::from_ref(&peer)));
    }
    let counts = jobs.iter().fold(HashMap::new(), |mut acc, job| {
        *acc.entry(job.check).or_insert(0usize) += 1;
        acc
    });

    assert_eq!(counts.get(&CheckKind::Availability), Some(&72));
    assert_eq!(counts.get(&CheckKind::Voting), Some(&1));
    assert!(!counts.contains_key(&CheckKind::Chain));
    assert!(!counts.contains_key(&CheckKind::Finalization));
}

#[test]
fn supernode_budget_accounts_for_registration_info_and_result_requests() {
    assert_eq!(CheckKind::Supernode.request_units(), 3);
}

#[test]
fn periodic_checks_are_phased_without_exceeding_the_probe_epoch_capacity() {
    let nodes = (0..350)
        .map(|index| {
            Node::new(
                format!("symbol-{index}"),
                Network::SymbolMainnet,
                NodeKind::Api,
                format!("symbol-{index}.example"),
            )
        })
        .collect::<Vec<_>>();
    let scheduler = Scheduler::default();
    let mut seen = HashMap::<(String, CheckKind), usize>::new();
    for epoch in 0..36 {
        for site in [Site::A, Site::B] {
            let jobs = scheduler.jobs_for_epoch(site, epoch, &nodes);
            let units = estimated_symbol_outbound_operations(&jobs);
            assert!(
                units <= 1_204,
                "site {site:?} epoch {epoch} schedules {units} units"
            );
            for job in jobs {
                *seen.entry((job.node_id, job.check)).or_default() += 1;
            }
        }
    }
    for node in &nodes {
        assert_eq!(seen[&(node.id.clone(), CheckKind::Peers)], 12);
        assert_eq!(seen[&(node.id.clone(), CheckKind::StateHash)], 12);
        assert_eq!(seen[&(node.id.clone(), CheckKind::Harvest)], 6);
        assert_eq!(seen[&(node.id.clone(), CheckKind::Identity)], 1);
        assert_eq!(seen[&(node.id.clone(), CheckKind::Certificate)], 1);
        assert_eq!(seen[&(node.id.clone(), CheckKind::WebSocket)], 1);
        assert_eq!(seen[&(node.id.clone(), CheckKind::Voting)], 1);
    }
}

#[test]
fn supported_mixed_inventory_stays_within_the_probe_epoch_capacity() {
    let nodes = (0..350)
        .map(|index| {
            let network = if index < 20 {
                Network::Nis1Mainnet
            } else {
                Network::SymbolMainnet
            };
            Node::new(
                format!("mixed-{index}"),
                network,
                NodeKind::Api,
                format!("mixed-{index}.example"),
            )
        })
        .collect::<Vec<_>>();
    let scheduler = Scheduler::default();

    for epoch in 0..36 {
        for site in [Site::A, Site::B] {
            let jobs = scheduler.jobs_for_epoch(site, epoch, &nodes);
            let units = estimated_mixed_outbound_operations(&jobs, &nodes);
            assert!(
                units <= 1_204,
                "site {site:?} epoch {epoch} schedules {units} physical operations"
            );
        }
    }
}

#[test]
fn current_328_node_inventory_stays_within_the_two_point_five_rate_trial() {
    let scheduler = Scheduler::default();
    for fixture in 0..8 {
        let nodes = (0..328)
            .map(|index| {
                let network = if index < 106 {
                    Network::Nis1Mainnet
                } else {
                    Network::SymbolMainnet
                };
                Node::new(
                    format!("trial-{fixture}-{index}"),
                    network,
                    NodeKind::Api,
                    format!("trial-{fixture}-{index}.example"),
                )
            })
            .collect::<Vec<_>>();
        let mut peak = 0;
        for epoch in 0..36 {
            for site in [Site::A, Site::B] {
                let jobs = scheduler.jobs_for_epoch(site, epoch, &nodes);
                peak = peak.max(estimated_mixed_outbound_operations(&jobs, &nodes));
            }
        }

        assert!(
            peak > 1_204,
            "fixture {fixture} must exercise the old 2.0 ops/s ceiling"
        );
        assert!(
            peak <= 1_504,
            "fixture {fixture} peaks at {peak} physical operations"
        );
    }
}

#[test]
fn current_333_node_connectivity_fits_the_three_rate_280_second_window() {
    let mut nodes = (0..210)
        .map(|index| {
            Node::new(
                format!("symbol-api-{index}"),
                Network::SymbolMainnet,
                NodeKind::Api,
                format!("symbol-api-{index}.example"),
            )
        })
        .collect::<Vec<_>>();
    nodes.extend((0..12).map(|index| {
        Node::new(
            format!("symbol-peer-{index}"),
            Network::SymbolMainnet,
            NodeKind::PeerOnly,
            format!("symbol-peer-{index}.example"),
        )
    }));
    nodes.extend((0..111).map(|index| {
        Node::new(
            format!("nis1-api-{index}"),
            Network::Nis1Mainnet,
            NodeKind::Api,
            format!("nis1-api-{index}.example"),
        )
    }));

    let scheduler = Scheduler::default();
    for epoch in 0..36 {
        for site in [Site::A, Site::B] {
            let jobs = scheduler.jobs_for_epoch(site, epoch, &nodes);
            let connectivity = estimated_connectivity_outbound_operations(&jobs, &nodes);
            assert_eq!(connectivity, 777, "site {site:?} epoch {epoch}");
            assert!(connectivity <= 3 * 280);
            assert!(jobs.iter().all(|job| job.offset_seconds < 280));
        }
    }
}

fn estimated_connectivity_outbound_operations(jobs: &[ScheduledJob], nodes: &[Node]) -> usize {
    let network_by_id = nodes
        .iter()
        .map(|node| (node.id.as_str(), node.network))
        .collect::<HashMap<_, _>>();
    jobs.iter()
        .filter_map(|job| match job.check {
            CheckKind::Availability => Some(match network_by_id[job.node_id.as_str()] {
                Network::SymbolMainnet => 1,
                Network::Nis1Mainnet => 2,
            }),
            CheckKind::PeerTcp => Some(1),
            _ => None,
        })
        .sum()
}

fn estimated_symbol_outbound_operations(jobs: &[ScheduledJob]) -> usize {
    let mut by_node = HashMap::<&str, HashSet<CheckKind>>::new();
    for job in jobs {
        by_node.entry(&job.node_id).or_default().insert(job.check);
    }
    let mut operations = 0;
    let mut comparison_present = false;
    let mut state_hash_present = false;
    for checks in by_node.values() {
        operations += usize::from(checks.contains(&CheckKind::Availability));
        operations += usize::from(checks.contains(&CheckKind::PeerTcp));
        operations += usize::from(checks.contains(&CheckKind::Peers));
        operations += usize::from(checks.contains(&CheckKind::Harvest));
        operations += usize::from(checks.contains(&CheckKind::Certificate));
        operations += usize::from(checks.contains(&CheckKind::WebSocket));
        operations += 2 * usize::from(checks.contains(&CheckKind::Identity));
        operations += 2 * usize::from(checks.contains(&CheckKind::Voting));
        let has_comparison = checks.contains(&CheckKind::Chain)
            || checks.contains(&CheckKind::Finalization)
            || checks.contains(&CheckKind::StateHash);
        operations += usize::from(has_comparison);
        operations += usize::from(checks.contains(&CheckKind::StateHash));
        comparison_present |= has_comparison;
        state_hash_present |= checks.contains(&CheckKind::StateHash);
    }
    operations += 30 * usize::from(comparison_present);
    operations += 30 * usize::from(state_hash_present);
    operations
}

fn estimated_mixed_outbound_operations(jobs: &[ScheduledJob], nodes: &[Node]) -> usize {
    let network_by_id = nodes
        .iter()
        .map(|node| (node.id.as_str(), node.network))
        .collect::<HashMap<_, _>>();
    let mut by_node = HashMap::<&str, HashSet<CheckKind>>::new();
    for job in jobs {
        by_node.entry(&job.node_id).or_default().insert(job.check);
    }
    let mut operations = 0;
    let mut symbol_comparison_present = false;
    let mut symbol_state_hash_present = false;
    for (node_id, checks) in by_node {
        match network_by_id[node_id] {
            Network::Nis1Mainnet => {
                operations += 2 * usize::from(checks.contains(&CheckKind::Availability));
                operations += usize::from(checks.contains(&CheckKind::PeerTcp));
                operations += 3 * usize::from(checks.contains(&CheckKind::Chain));
                operations += usize::from(checks.contains(&CheckKind::Peers));
                operations += 3 * usize::from(checks.contains(&CheckKind::Supernode));
                operations += usize::from(checks.contains(&CheckKind::Identity));
                operations += usize::from(checks.contains(&CheckKind::Certificate));
            }
            Network::SymbolMainnet => {
                operations += usize::from(checks.contains(&CheckKind::Availability));
                operations += usize::from(checks.contains(&CheckKind::PeerTcp));
                operations += usize::from(checks.contains(&CheckKind::Peers));
                operations += usize::from(checks.contains(&CheckKind::Harvest));
                operations += usize::from(checks.contains(&CheckKind::Certificate));
                operations += usize::from(checks.contains(&CheckKind::WebSocket));
                operations += 2 * usize::from(checks.contains(&CheckKind::Identity));
                operations += 2 * usize::from(checks.contains(&CheckKind::Voting));
                let has_comparison = checks.contains(&CheckKind::Chain)
                    || checks.contains(&CheckKind::Finalization)
                    || checks.contains(&CheckKind::StateHash);
                operations += usize::from(has_comparison);
                operations += usize::from(checks.contains(&CheckKind::StateHash));
                symbol_comparison_present |= has_comparison;
                symbol_state_hash_present |= checks.contains(&CheckKind::StateHash);
            }
        }
    }
    operations += 30 * usize::from(symbol_comparison_present);
    operations += 30 * usize::from(symbol_state_hash_present);
    operations
}
