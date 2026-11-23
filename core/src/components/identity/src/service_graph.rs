use cortexbrain_common::otel_metrics::Metrics;
use cortexbrain_common::service_graph::{ServiceGraphEvent, ServiceGraphObserver};
use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing::warn;

const GRAPH_TTL_ENV: &str = "GRAPH_TTL";
const DEFAULT_GRAPH_TTL_SECS: u64 = 60;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct ServiceGraphKey {
    source: Ipv4Addr,
    destination: Ipv4Addr,
}

#[derive(Clone, Debug)]
struct ServiceGraphEntry {
    event: ServiceGraphEvent,
    last_seen: Instant,
}

enum GraphChange {
    Added(ServiceGraphEvent),
    Updated {
        previous: ServiceGraphEvent,
        current: ServiceGraphEvent,
    },
    Refreshed,
}

/// Directed graph of service-to-service links, with weight fixed to 1.
pub struct ServiceGraph {
    edges: HashMap<ServiceGraphKey, ServiceGraphEntry>,
    ttl: Duration,
}

impl ServiceGraph {
    pub fn new(ttl: Duration) -> Self {
        Self {
            edges: HashMap::new(),
            ttl,
        }
    }

    fn observe(&mut self, event: ServiceGraphEvent, now: Instant) -> GraphChange {
        let key = ServiceGraphKey {
            source: event.source.ip,
            destination: event.destination.ip,
        };

        if let Some(entry) = self.edges.get_mut(&key) {
            entry.last_seen = now;
            if entry.event == event {
                return GraphChange::Refreshed;
            }

            let previous = std::mem::replace(&mut entry.event, event.clone());
            return GraphChange::Updated {
                previous,
                current: event,
            };
        }

        self.edges.insert(
            key,
            ServiceGraphEntry {
                event: event.clone(),
                last_seen: now,
            },
        );
        GraphChange::Added(event)
    }

    fn evict_expired(&mut self, now: Instant) -> Vec<ServiceGraphEvent> {
        let ttl = self.ttl;
        let mut expired = Vec::new();
        self.edges.retain(|_, entry| {
            if now.saturating_duration_since(entry.last_seen) >= ttl {
                expired.push(entry.event.clone());
                false
            } else {
                true
            }
        });
        expired
    }
}

/// Identity-side observer that updates the graph and emits its OTEL metric.
pub struct IdentityServiceGraph {
    graph: Mutex<ServiceGraph>,
    metrics: Arc<Metrics>,
    ttl: Duration,
}

impl IdentityServiceGraph {
    pub fn new(metrics: Arc<Metrics>) -> Self {
        let ttl = graph_ttl_from_env();
        Self {
            graph: Mutex::new(ServiceGraph::new(ttl)),
            metrics,
            ttl,
        }
    }

    pub fn ttl(&self) -> Duration {
        self.ttl
    }

    pub fn evict_expired(&self) {
        let mut graph = self.graph.lock().unwrap_or_else(|e| e.into_inner());
        for event in graph.evict_expired(Instant::now()) {
            self.metrics.record_service_graph_edge(&event, 0);
        }
    }
}

impl ServiceGraphObserver for IdentityServiceGraph {
    fn observe(&self, event: &ServiceGraphEvent) {
        let mut graph = self.graph.lock().unwrap_or_else(|e| e.into_inner());
        match graph.observe(event.clone(), Instant::now()) {
            GraphChange::Added(event) => {
                self.metrics.record_service_graph_edge(&event, 1);
            }
            GraphChange::Updated { previous, current } => {
                self.metrics.record_service_graph_edge(&previous, 0);
                self.metrics.record_service_graph_edge(&current, 1);
            }
            GraphChange::Refreshed => {}
        }
    }
}

pub fn eviction_interval(ttl: Duration) -> Duration {
    Duration::from_secs((ttl.as_secs() / 2).max(1))
}

fn graph_ttl_from_env() -> Duration {
    match std::env::var(GRAPH_TTL_ENV) {
        Ok(value) => match value.parse::<u64>() {
            Ok(seconds) if seconds > 0 => Duration::from_secs(seconds),
            _ => {
                warn!(
                    "Invalid {}={:?}; using {} seconds",
                    GRAPH_TTL_ENV, value, DEFAULT_GRAPH_TTL_SECS
                );
                Duration::from_secs(DEFAULT_GRAPH_TTL_SECS)
            }
        },
        Err(_) => Duration::from_secs(DEFAULT_GRAPH_TTL_SECS),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cortexbrain_common::service_graph::ServiceNode;

    fn event(source: [u8; 4], destination: [u8; 4]) -> ServiceGraphEvent {
        ServiceGraphEvent {
            source: ServiceNode::new(Ipv4Addr::from(source)),
            destination: ServiceNode::new(Ipv4Addr::from(destination)),
        }
    }

    #[test]
    fn repeated_events_refresh_the_same_edge() {
        let mut graph = ServiceGraph::new(Duration::from_secs(60));
        let now = Instant::now();

        assert!(matches!(
            graph.observe(event([10, 0, 0, 1], [10, 0, 0, 2]), now),
            GraphChange::Added(_)
        ));
        assert!(matches!(
            graph.observe(
                event([10, 0, 0, 1], [10, 0, 0, 2]),
                now + Duration::from_secs(10)
            ),
            GraphChange::Refreshed
        ));
    }

    #[test]
    fn eviction_removes_expired_edges() {
        let mut graph = ServiceGraph::new(Duration::from_secs(60));
        let now = Instant::now();
        let _ = graph.observe(event([10, 0, 0, 1], [10, 0, 0, 2]), now);

        let expired = graph.evict_expired(now + Duration::from_secs(60));
        assert_eq!(expired.len(), 1);
        assert!(graph.edges.is_empty());
    }
}
