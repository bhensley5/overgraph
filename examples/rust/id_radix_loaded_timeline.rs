//! Instrumented copy of the frozen loaded fixture; no engine hooks.
use overgraph::*;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::{Arc, Barrier, OnceLock};
use std::time::{Duration, Instant};

const SINGLETONS: usize = 8192;
const SHALLOW: usize = 4096;
const HIGH: u64 = 1 << 48;

fn node(i: usize, version: usize, payload: bool) -> NodeInput {
    let props = if payload {
        BTreeMap::from([
            ("a".into(), PropValue::Int(7)),
            ("b".into(), PropValue::Bool(true)),
            ("c".into(), PropValue::Float(1.25)),
            ("d".into(), PropValue::UInt(9)),
            ("text".into(), PropValue::String("x".repeat(256))),
        ])
    } else {
        BTreeMap::new()
    };
    NodeInput {
        labels: vec!["N".into()],
        key: format!("n{i}"),
        props,
        weight: version as f32,
        dense_vector: None,
        sparse_vector: None,
    }
}
fn edge(i: usize, ids: &[u64], version: usize) -> EdgeInput {
    EdgeInput {
        from: ids[i % ids.len()],
        to: ids[(i + 1) % ids.len()],
        label: "E".into(),
        props: BTreeMap::new(),
        weight: version as f32,
        valid_from: None,
        valid_to: None,
    }
}
fn open() -> (tempfile::TempDir, DatabaseEngine) {
    let mut dir = tempfile::Builder::new()
        .prefix("overgraph-scale-timeline-")
        .tempdir()
        .unwrap();
    // Preserve an unfinished fixture on panic/error. Remove it explicitly only
    // after successful close and verified native drain.
    dir.disable_cleanup(true);
    eprintln!("{}", json!({"owned_fixture":dir.path()}));
    let db = DatabaseEngine::open(
        dir.path(),
        &DbOptions {
            edge_uniqueness: true,
            ..DbOptions::default()
        },
    )
    .unwrap();
    db.ensure_node_label("N").unwrap();
    db.ensure_edge_label("E").unwrap();
    (dir, db)
}
fn close(db: DatabaseEngine) {
    db.close_fast().unwrap();
    let until = Instant::now() + Duration::from_secs(10);
    loop {
        let s = db.reclamation_stats().unwrap();
        if matches!(s.close_state, ReclamationCloseState::Drained) {
            assert_eq!(s.registered_objects, 0);
            assert_eq!(s.leased_participants, 0);
            assert_eq!(s.active_sessions + s.draining_sessions, 0);
            break;
        }
        assert!(Instant::now() < until, "native drain timeout: {s:?}");
        std::thread::sleep(Duration::from_millis(1));
    }
}
fn stats(db: &DatabaseEngine) -> Value {
    let s = db.stats().unwrap();
    json!({"wal_generation":s.active_wal_generation_id,"segments":s.segment_count,
        "immutables":s.immutable_memtable_count,"active_estimate":s.active_memtable_bytes,
        "immutable_estimate":s.immutable_memtable_bytes,"wal_mode":s.wal_sync_mode})
}
struct Fixture {
    _dir: tempfile::TempDir,
    db: DatabaseEngine,
    nodes: Vec<u64>,
    edges: Vec<u64>,
    versions: usize,
    requests: Vec<usize>,
    payload: bool,
}
impl Fixture {
    fn new(shallow: bool, payload: bool) -> Self {
        let (dir, db) = open();
        let n = if shallow { SHALLOW } else { SINGLETONS };
        let versions = if shallow { 4 } else { 1 };
        let mut nodes = Vec::new();
        for v in 0..versions {
            let found = db
                .batch_upsert_nodes((0..n).map(|i| node(i, v, payload)).collect())
                .unwrap();
            if v == 0 {
                nodes = found;
            } else {
                assert_eq!(found, nodes);
            }
        }
        let edges = db
            .batch_upsert_edges((0..n).map(|i| edge(i, &nodes, 0)).collect())
            .unwrap();
        // Allocated tombstones and gaps below a real, very high tombstone.
        for i in (0..n).step_by(8) {
            db.delete_edge(edges[i]).unwrap();
        }
        // Keep endpoints live so node deletion does not alter the edge oracle.
        let dead = db
            .batch_upsert_nodes((n..n + 1024).map(|i| node(i, 0, false)).collect())
            .unwrap();
        for &id in &dead {
            db.delete_node(id).unwrap();
        }
        db.delete_node(HIGH).unwrap();
        db.delete_edge(HIGH).unwrap();
        assert_eq!(db.next_node_id().unwrap(), dead.last().unwrap() + 1);
        Self {
            _dir: dir,
            db,
            nodes,
            edges,
            versions,
            requests: trace(n),
            payload,
        }
    }
    #[expect(
        dead_code,
        reason = "Keep the frozen full oracle beside the loaded-only diagnostic entry point"
    )]
    fn oracle(&self) {
        for (i, &id) in self.nodes.iter().enumerate() {
            let n = self.db.get_node(id).unwrap().unwrap();
            assert_eq!(n.key, format!("n{i}"));
            assert_eq!(n.props, node(i, self.versions - 1, self.payload).props);
            assert_eq!(n.weight, (self.versions - 1) as f32);
        }
        for (i, &id) in self.edges.iter().enumerate() {
            let e = self.db.get_edge(id).unwrap();
            if i % 8 == 0 {
                assert!(e.is_none());
            } else {
                let e = e.unwrap();
                assert_eq!(
                    (e.from, e.to),
                    (self.nodes[i], self.nodes[(i + 1) % self.nodes.len()])
                );
            }
        }
        assert!(self
            .db
            .get_node(self.nodes.len() as u64 + 1)
            .unwrap()
            .is_none());
        assert!(self.db.get_node(HIGH / 2).unwrap().is_none());
        assert!(self.db.get_edge(HIGH / 2).unwrap().is_none());
        assert!(self.db.get_node(HIGH).unwrap().is_none());
        self.ordered(true);
        self.ordered(false);
        assert_eq!(self.db.nodes_by_labels("N").unwrap(), self.nodes);
    }
    fn ordered(&self, hydrated: bool) {
        let mut page = PageRequest {
            limit: Some(10),
            after: None,
        };
        let mut found = Vec::new();
        loop {
            let (ids, next) = if hydrated {
                let p = self.db.get_nodes_by_labels_paged("N", &page).unwrap();
                for n in &p.items {
                    assert_eq!(n.weight, (self.versions - 1) as f32);
                }
                (
                    p.items.into_iter().map(|n| n.id).collect::<Vec<_>>(),
                    p.next_cursor,
                )
            } else {
                let p = self.db.nodes_by_labels_paged("N", &page).unwrap();
                (p.items, p.next_cursor)
            };
            found.extend(ids);
            if next.is_none() {
                break;
            }
            assert!(next > page.after);
            page.after = next;
        }
        assert_eq!(found, self.nodes);
    }
    fn finish(self) {
        close(self.db);
        self._dir.close().unwrap();
    }
}
fn trace(n: usize) -> Vec<usize> {
    let mut seed = 0x1d_2026_1025_u64;
    (0..8192)
        .map(|_| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed as usize % n
        })
        .collect()
}
fn summarize(mut samples: Vec<u64>) -> Value {
    samples.sort_unstable();
    if samples.is_empty() {
        return json!(null);
    }
    json!({"count":samples.len(),"min":samples[0],"median":samples[samples.len()/2],
        "p95":samples[(samples.len()-1)*95/100],"max":samples[samples.len()-1]})
}
fn offset(t: Instant, epoch: Instant) -> i64 {
    if let Some(d) = t.checked_duration_since(epoch) {
        i64::try_from(d.as_nanos()).unwrap()
    } else {
        -i64::try_from(epoch.duration_since(t).as_nanos()).unwrap()
    }
}
fn observe(db: &DatabaseEngine, epoch: Instant, boundary: usize) -> Value {
    let begin = Instant::now();
    let value = stats(db);
    let end = Instant::now();
    json!({"request_boundary_index":boundary,"begin_ns":offset(begin,epoch),"end_ns":offset(end,epoch),
        "stats_duration_ns":end.duration_since(begin).as_nanos() as u64,"sources":value})
}
pub(super) fn run(measure: bool) {
    let f = Fixture::new(false, false);
    let barrier = Arc::new(Barrier::new(6));
    let clock = Arc::new(OnceLock::<Instant>::new());
    let mut joins = Vec::new();
    for lane in 0..5 {
        let db = f.db.clone();
        let b = barrier.clone();
        let ids = f.nodes.clone();
        let requests = f.requests.clone();
        let clock = clock.clone();
        joins.push(std::thread::spawn(move||{
            let rate=if lane==0{1500}else if lane<=2{5000}else{1000};
            let calls=if measure{rate*3}else{10};
            let inputs=if lane==0{(0..calls).map(|i|(0..100).map(|j|node(j,i,false)).collect::<Vec<_>>()).collect::<Vec<_>>()}else{Vec::new()};
            let keys=if lane==1||lane==2 {(0..ids.len()).map(|i|format!("n{i}")).collect::<Vec<_>>()}else{Vec::new()};
            let writer_calls=if measure{4500}else{10};
            let mut inputs=inputs.into_iter();let mut service=Vec::new();let mut due_latency=Vec::new();
            let (mut within,mut late,mut refused,mut failed,mut unfinished)=(0,0,0,0,0);
            let mut after_catchup=None;let mut chronology=Vec::new();let mut observations=Vec::new();
            b.wait(); b.wait(); let start=*clock.get().unwrap();
            for i in 0..calls {
                if lane==0 && i%500==0 {observations.push(observe(&db,start,i));}
                let due=start+Duration::from_nanos(i as u64*1_000_000_000/rate as u64);
                if measure {std::thread::sleep(due.saturating_duration_since(Instant::now()));}
                let offered=Instant::now();
                if measure && offered>start+Duration::from_secs(8){unfinished=(calls-i).min(rate*2);break;}
                let result=match lane {
                    0=>db.batch_upsert_nodes(inputs.next().unwrap()).map(|found|assert_eq!(found,ids[..100])),
                    1|2=>{let k=requests[i%requests.len()]; db.get_node(ids[k]).map(|n|{let n=n.unwrap();assert_eq!(n.id,ids[k]);assert_eq!(n.key,keys[k]);if k>=100 {assert_eq!(n.weight,0.);}else{assert!(n.weight>=0.&&n.weight<writer_calls as f32);}})},
                    3=>db.nodes_by_labels_paged("N",&PageRequest{limit:Some(10),after:None}).map(|p|assert_eq!(p.items,ids[..10])),
                    _=>db.get_nodes_by_labels_paged("N",&PageRequest{limit:Some(10),after:None}).map(|p|{assert_eq!(p.items.len(),10);let weight=p.items[0].weight;assert!(weight>=0.&&weight<writer_calls as f32);for (j,n) in p.items.iter().enumerate(){assert_eq!(n.id,ids[j]);assert_eq!(n.key,format!("n{j}"));assert_eq!(n.weight,weight);}}),
                };
                let done=Instant::now();
                if lane==0 {
                    let status=match &result {Ok(())=>"ok",Err(EngineError::ResourceLimit{..})=>"refused",Err(_)=>"failed"};
                    chronology.push(json!({"index":i,"warmup":measure && i<rate,
                        "due_ns":offset(due,start),"offered_ns":offset(offered,start),"done_ns":offset(done,start),"status":status}));
                }
                // A call still in flight at the five-second catch-up cutoff is
                // unfinished in the panel. Its eventual cleanup completion is
                // recorded separately and remains charged to process wall time.
                if measure && done>start+Duration::from_secs(8) {
                    unfinished=(calls-i).min(rate*2);
                    after_catchup=Some(match result {Ok(())=>"completed",Err(EngineError::ResourceLimit{..})=>"refused",Err(_)=>"failed"});
                    break;
                }
                if measure && i>=rate {
                    service.push(done.duration_since(offered).as_nanos() as u64);
                    due_latency.push(done.saturating_duration_since(due).as_nanos() as u64);
                    match result {Ok(())=>if done<=start+Duration::from_secs(3){within+=1}else{late+=1},Err(EngineError::ResourceLimit{..})=>refused+=1,Err(_)=>failed+=1}
                }else{result.unwrap();}
            }
            if lane==0 {observations.push(observe(&db,start,chronology.len()));}
            json!({"writer_chronology":chronology,"writer_stats":observations,"lane":lane,"rate":rate,"service_ns":summarize(service),"due_to_completion_ns":summarize(due_latency),"completed_in_window":within,"late":late,"refused":refused,"failed":failed,"unfinished":unfinished,"completion_after_catchup":after_catchup})
        }));
    }
    barrier.wait();
    clock
        .set(Instant::now() + Duration::from_millis(10))
        .unwrap();
    barrier.wait();
    let results = joins
        .into_iter()
        .map(|j| j.join().unwrap())
        .collect::<Vec<_>>();
    let sources = stats(&f.db);
    f.finish();
    println!(
        "{}",
        json!({"panel":"loaded-timeline","measure":measure,"lanes":results,"sources":sources,
        "native_drained":true,"instrumentation":"writer chronology includes warmup; stats every500writer boundaries plus end, outside service timers; diagnostic only"})
    );
}
