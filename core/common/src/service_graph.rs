use std::net::Ipv4Addr;

/// Enriched endpoint of a service graph edge.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServiceNode {
    pub ip: Ipv4Addr,
    pub service: Option<String>,
    pub namespace: Option<String>,
    pub command: Option<String>
}

impl ServiceNode {
    pub fn new(ip: Ipv4Addr) -> Self {
        Self {
            ip,
            service: None,
            namespace: None,
            command: None,
        }
    }
}

/// Directed edge observed between two service graph endpoints.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServiceGraphEvent {
    pub source: ServiceNode,
    pub destination: ServiceNode,
}

/// Receives enriched service graph events from the perf-buffer consumers.
pub trait ServiceGraphObserver: Send + Sync {
    fn observe(&self, event: &ServiceGraphEvent);
}
