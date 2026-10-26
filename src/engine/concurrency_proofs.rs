//! Finite protocol/oracle counterexamples. These are tests, not an alternative
//! implementation or a claim to model-check upstream epoch/skiplist internals.
use std::collections::{BTreeSet, VecDeque};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Phase {
    Reserved,
    Live,
    Retired,
    Inspecting,
    Dropping,
    Free,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct State {
    phase: Phase,
    producers: u8,
    readers: u8,
    charged: bool,
    destroyed: bool,
}
fn valid(s: State) -> bool {
    if s.charged != (s.phase != Phase::Free) {
        return false;
    }
    match s.phase {
        Phase::Reserved => s.producers == 0 && s.readers == 0 && !s.destroyed,
        Phase::Live => s.producers > 0 && !s.destroyed,
        Phase::Retired | Phase::Inspecting => s.producers == 0 && !s.destroyed,
        Phase::Dropping => s.producers == 0 && s.readers == 0,
        Phase::Free => s.producers == 0 && s.readers == 0 && s.destroyed,
    }
}
fn successors(s: State) -> Vec<State> {
    let mut next = Vec::new();
    match s.phase {
        Phase::Reserved => {
            next.push(State {
                phase: Phase::Live,
                producers: 1,
                ..s
            });
            next.push(State {
                phase: Phase::Dropping,
                ..s
            });
        }
        Phase::Live => {
            if s.producers < 2 {
                next.push(State {
                    producers: s.producers + 1,
                    ..s
                });
            }
            next.push(State {
                producers: s.producers - 1,
                phase: if s.producers == 1 {
                    Phase::Retired
                } else {
                    Phase::Live
                },
                ..s
            });
        }
        Phase::Retired => next.push(State {
            phase: Phase::Inspecting,
            ..s
        }),
        Phase::Inspecting => next.push(State {
            phase: if s.readers == 0 {
                Phase::Dropping
            } else {
                Phase::Retired
            },
            ..s
        }),
        Phase::Dropping => {
            if !s.destroyed {
                next.push(State {
                    destroyed: true,
                    ..s
                });
            } else {
                next.push(State {
                    phase: Phase::Free,
                    charged: false,
                    ..s
                });
            }
        }
        Phase::Free => {}
    }
    if matches!(s.phase, Phase::Live | Phase::Retired | Phase::Inspecting) {
        if s.readers > 0 {
            next.push(State {
                readers: s.readers - 1,
                ..s
            });
        }
        if (s.producers > 0 || s.readers > 0) && s.readers < 2 {
            next.push(State {
                readers: s.readers + 1,
                ..s
            });
        }
    }
    next
}
#[test]
fn p45a1_finite_lifetime_model_and_required_oracle_mutants() {
    let initial = State {
        phase: Phase::Reserved,
        producers: 0,
        readers: 0,
        charged: true,
        destroyed: false,
    };
    let mut seen = BTreeSet::from([initial]);
    let mut queue = VecDeque::from([initial]);
    while let Some(s) = queue.pop_front() {
        assert!(valid(s), "{s:?}");
        for next in successors(s) {
            if seen.insert(next) {
                queue.push_back(next);
            }
        }
    }
    // Exact finite universe: Reserved; 2 producer counts x 3 reader counts;
    // 3 Retired; 3 Inspecting; 2 Dropping; Free = 16 reachable states.
    assert_eq!(seen.len(), 16);
    assert!(seen.iter().any(|s| s.phase == Phase::Free));
    let retired = State {
        phase: Phase::Retired,
        producers: 0,
        readers: 1,
        charged: true,
        destroyed: false,
    };
    assert!(!valid(State {
        phase: Phase::Live,
        ..retired
    })); // missing retirement
    assert!(!valid(State {
        charged: false,
        ..retired
    })); // early byte return
    assert!(!valid(State {
        phase: Phase::Dropping,
        ..retired
    })); // pinned destruction
}

fn boundary_valid(source: &str) -> bool {
    let guard = source.find("drop(self.guard.take())");
    let unlock = source.find("drop(self.slot.take())");
    source.matches("collector.register()").count() == 1
        && source.matches("slot.0.pin()").count() == 1
        && source
            .matches("unsafe impl Send for ExclusiveParticipant")
            .count()
            == 1
        && !source.contains("unsafe impl Sync")
        && !source.contains("pub(super) fn guard")
        && !source.contains("crossbeam_epoch::pin")
        && !source.contains("unprotected(")
        && !source.contains(".defer(")
        && !source.contains(".flush(")
        && !source.contains("*slot =")
        && guard.zip(unlock).is_some_and(|(g, u)| g < u)
}
#[test]
fn p45a1_collector_source_boundary_rejects_ownership_mutants() {
    let source = include_str!("participant.rs")
        .split("#[cfg(test)]")
        .next()
        .unwrap();
    assert!(boundary_valid(source));
    for mutant in [
        source.replace("fn guard(&self)", "pub(super) fn guard(&self)"),
        format!("{source}\ncollector.register();"),
        format!("{source}\ncrossbeam_epoch::pin();"),
        format!("{source}\n*slot = replacement;"),
        source
            .replace("drop(self.guard.take());", "EARLY_GUARD")
            .replace("drop(self.slot.take());", "drop(self.guard.take());")
            .replace("EARLY_GUARD", "drop(self.slot.take());"),
    ] {
        assert!(!boundary_valid(&mutant));
    }
    for module in [
        include_str!("reclamation.rs"),
        include_str!("append_slice.rs"),
        include_str!("durable_transition.rs"),
        include_str!("concurrency_lifecycle.rs"),
    ] {
        let production = module.split("#[cfg(test)]\nmod tests").next().unwrap();
        assert!(!production.contains("crossbeam_epoch::"));
        assert!(!production.contains("crossbeam_skiplist::"));
    }
}

/// Input-driven serial oracle shared by native traces and concrete observation
/// mutants. Candidate query output never constructs expected state.
#[derive(Default)]
pub(super) struct TraceOracle {
    frontier: u64,
    nodes: std::collections::BTreeMap<u64, crate::types::NodeRecord>,
    edges: std::collections::BTreeMap<u64, crate::types::EdgeRecord>,
    tokens: std::collections::BTreeMap<(u8, u32), String>,
    sources: std::collections::BTreeMap<u64, [u8; 32]>,
}
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub(super) struct TraceObservation {
    pub(super) frontier: u64,
    pub(super) nodes: std::collections::BTreeMap<u64, (u64, Vec<u8>)>,
    pub(super) edges: std::collections::BTreeMap<u64, (u64, Vec<u8>)>,
    pub(super) equality: std::collections::BTreeMap<(u32, i64), Vec<u64>>,
    pub(super) outgoing: std::collections::BTreeMap<u64, Vec<u64>>,
    pub(super) degrees: std::collections::BTreeMap<u64, u64>,
    pub(super) tokens: std::collections::BTreeMap<(u8, u32), String>,
    pub(super) sources: std::collections::BTreeMap<u64, [u8; 32]>,
}
impl TraceOracle {
    pub(super) fn apply(&mut self, ops: &[crate::types::WalOp]) {
        use crate::types::WalOp;
        for op in ops {
            self.frontier += 1;
            match op {
                WalOp::UpsertNode(n) => {
                    let mut n = n.clone();
                    n.last_write_seq = self.frontier;
                    self.nodes.insert(n.id, n);
                }
                WalOp::DeleteNode { id, .. } => {
                    self.nodes.remove(id);
                }
                WalOp::UpsertEdge(e) => {
                    let mut e = e.clone();
                    e.last_write_seq = self.frontier;
                    self.edges.insert(e.id, e);
                }
                WalOp::DeleteEdge { id, .. } => {
                    self.edges.remove(id);
                }
                WalOp::EnsureNodeLabel { label, label_id } => {
                    self.tokens.insert((0, *label_id), label.clone());
                }
                WalOp::EnsureEdgeLabel { label, label_id } => {
                    self.tokens.insert((1, *label_id), label.clone());
                }
                _ => panic!("controls are not oracle input"),
            }
        }
    }
    pub(super) fn adopt(&mut self, outputs: &[crate::types::SegmentInfo]) {
        self.sources = outputs.iter().map(|s| (s.id, s.segment_data_id)).collect();
    }
    pub(super) fn check_adoption(
        &self,
        outputs: &[crate::types::SegmentInfo],
    ) -> Result<(), &'static str> {
        let observed = outputs
            .iter()
            .map(|s| (s.id, s.segment_data_id))
            .collect::<std::collections::BTreeMap<_, _>>();
        if observed == self.sources {
            Ok(())
        } else {
            Err("stale adoption")
        }
    }
    pub(super) fn check(&self, observed: &TraceObservation) -> Result<(), &'static str> {
        use crate::types::{PropValue, WalOp};
        if observed.frontier != self.frontier {
            return Err("frontier");
        }
        let nodes = self
            .nodes
            .values()
            .map(|n| {
                (
                    n.id,
                    (
                        n.last_write_seq,
                        crate::encoding::encode_wal_op(&WalOp::UpsertNode(n.clone())).unwrap(),
                    ),
                )
            })
            .collect();
        let edges = self
            .edges
            .values()
            .map(|e| {
                (
                    e.id,
                    (
                        e.last_write_seq,
                        crate::encoding::encode_wal_op(&WalOp::UpsertEdge(e.clone())).unwrap(),
                    ),
                )
            })
            .collect();
        if observed.nodes != nodes || observed.edges != edges {
            return Err("primary");
        }
        for label in 1..=2 {
            for value in 0..3 {
                let expected = self
                    .nodes
                    .values()
                    .filter(|n| {
                        n.label_ids.contains(label)
                            && n.props.get("value") == Some(&PropValue::Int(value))
                    })
                    .map(|n| n.id)
                    .collect::<Vec<_>>();
                if observed.equality.get(&(label, value)) != Some(&expected) {
                    return Err("equality");
                }
            }
        }
        for owner in 1..=3 {
            let expected = self
                .edges
                .values()
                .filter(|e| e.from == owner)
                .map(|e| e.id)
                .collect::<Vec<_>>();
            if observed.outgoing.get(&owner) != Some(&expected)
                || observed.degrees.get(&owner) != Some(&(expected.len() as u64))
            {
                return Err("outgoing/degree");
            }
        }
        if observed.tokens != self.tokens {
            return Err("tokens");
        }
        if observed.sources != self.sources {
            return Err("stale adoption");
        }
        Ok(())
    }
}
