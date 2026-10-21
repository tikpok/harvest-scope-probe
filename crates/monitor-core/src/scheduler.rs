use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub use crate::model::{CheckKind, Site};
use crate::model::{Node, NodeKind};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScheduledJob {
    pub job_id: String,
    pub node_id: String,
    pub check: CheckKind,
    pub offset_seconds: u16,
    pub site_epoch_offset_seconds: u16,
    pub request_units: u8,
}

#[derive(Debug, Clone)]
pub struct Scheduler {
    spread_seconds: u16,
}

impl Default for Scheduler {
    fn default() -> Self {
        Self {
            spread_seconds: 280,
        }
    }
}

impl Scheduler {
    #[must_use]
    pub fn jobs_for_epoch(&self, site: Site, epoch: u64, nodes: &[Node]) -> Vec<ScheduledJob> {
        let mut jobs = Vec::with_capacity(nodes.len() * 4);
        for node in nodes {
            self.push(&mut jobs, node, site, epoch, CheckKind::Availability);
            self.push(&mut jobs, node, site, epoch, CheckKind::PeerTcp);

            if self.is_detailed_owner(node, site, epoch) {
                if node.kind == NodeKind::Api {
                    self.push(&mut jobs, node, site, epoch, CheckKind::Chain);
                    if node.network == crate::model::Network::SymbolMainnet {
                        self.push(&mut jobs, node, site, epoch, CheckKind::Finalization);
                    }

                    if periodic_due(node, epoch, CheckKind::Peers, 3) {
                        self.push(&mut jobs, node, site, epoch, CheckKind::Peers);
                    }
                    if node.network == crate::model::Network::SymbolMainnet
                        && periodic_due(node, epoch, CheckKind::StateHash, 3)
                    {
                        self.push(&mut jobs, node, site, epoch, CheckKind::StateHash);
                    }
                    let hourly_check = match node.network {
                        crate::model::Network::SymbolMainnet => CheckKind::Harvest,
                        crate::model::Network::Nis1Mainnet => CheckKind::Supernode,
                    };
                    if periodic_due(node, epoch, hourly_check, 6) {
                        self.push(&mut jobs, node, site, epoch, hourly_check);
                    }
                }

                if node.kind == NodeKind::Api {
                    if periodic_due(node, epoch, CheckKind::Identity, 36) {
                        self.push(&mut jobs, node, site, epoch, CheckKind::Identity);
                    }
                    if periodic_due(node, epoch, CheckKind::Certificate, 36) {
                        self.push(&mut jobs, node, site, epoch, CheckKind::Certificate);
                    }
                    if node.network == crate::model::Network::SymbolMainnet
                        && periodic_due(node, epoch, CheckKind::WebSocket, 36)
                    {
                        self.push(&mut jobs, node, site, epoch, CheckKind::WebSocket);
                    }
                }
                if node.network == crate::model::Network::SymbolMainnet
                    && periodic_due(node, epoch, CheckKind::Voting, 36)
                {
                    self.push(&mut jobs, node, site, epoch, CheckKind::Voting);
                }
            }
        }
        jobs.sort_by_key(|job| (job.offset_seconds, job.node_id.clone(), job.check as u8));
        jobs
    }

    #[must_use]
    pub fn is_detailed_owner(&self, node: &Node, site: Site, epoch: u64) -> bool {
        let parity = stable_u64(&format!("owner:{}", node.id)) % 2;
        let owner_a = parity == epoch % 2;
        matches!((site, owner_a), (Site::A, true) | (Site::B, false))
    }

    fn push(
        &self,
        jobs: &mut Vec<ScheduledJob>,
        node: &Node,
        site: Site,
        epoch: u64,
        check: CheckKind,
    ) {
        let job_id = format!("{}-{site:?}-{epoch}-{check:?}", node.id);
        let offset_seconds = u16::try_from(stable_u64(&job_id) % u64::from(self.spread_seconds))
            .expect("offset is modulo a u16 spread");
        jobs.push(ScheduledJob {
            job_id,
            node_id: node.id.clone(),
            check,
            offset_seconds,
            site_epoch_offset_seconds: if site == Site::B { 300 } else { 0 },
            request_units: check.request_units(),
        });
    }
}

fn periodic_due(node: &Node, epoch: u64, check: CheckKind, period: u64) -> bool {
    stable_u64(&format!("periodic:{}:{check:?}", node.id)) % period == epoch % period
}

fn stable_u64(input: &str) -> u64 {
    let digest = Sha256::digest(input.as_bytes());
    u64::from_be_bytes(
        digest[..8]
            .try_into()
            .expect("sha256 prefix has eight bytes"),
    )
}
