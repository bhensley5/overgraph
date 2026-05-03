#![allow(clippy::type_complexity)]

use napi::bindgen_prelude::*;
use napi::threadsafe_function::ThreadsafeFunctionCallMode;
use napi_derive::napi;
use overgraph::{
    AdjacencyExport, AllShortestPathsOptions, CompactionPhase, CompactionStats, ComponentOptions,
    DatabaseEngine, DbOptions, DbStats, DegreeOptions, DenseMetric, DenseVectorConfig, Direction,
    EdgeInput, EdgePattern, EdgePostFilterPredicate, EdgeRecord, EngineError, ExportOptions,
    FusionMode, GraphPatch, GraphPatternQuery, HnswConfig, IsConnectedOptions, NeighborEntry,
    NeighborOptions, NodeFilterExpr, NodeIdMap, NodeInput, NodePattern, NodePropertyIndexInfo,
    NodeQuery, NodeQueryOrder, NodeRecord, PageRequest, PageResult, PatternOrder, PprAlgorithm,
    PprOptions, PprResult, PropValue, PropertyRangeBound, PropertyRangeCursor,
    PropertyRangePageRequest, PropertyRangePageResult, PrunePolicy, PruneResult, QueryMatch,
    QueryNodeIdsResult, QueryNodesResult, QueryPatternResult, QueryPlan, QueryPlanKind,
    QueryPlanNode, QueryPlanWarning, ScoringMode, SecondaryIndexKind, SecondaryIndexRangeDomain,
    SecondaryIndexState, ShortestPath, ShortestPathOptions, Subgraph, SubgraphOptions, TopKOptions,
    TraversalCursor, TraversalHit, TraversalPageResult, TraverseOptions, TxnCommitResult,
    TxnEdgeRef, TxnEdgeView, TxnIntent, TxnLocalRef, TxnNodeRef, TxnNodeView, UpsertEdgeOptions,
    UpsertNodeOptions, VectorHit, VectorSearchMode, VectorSearchRequest, VectorSearchScope,
    WalSyncMode, WriteTxn,
};

/// ThreadsafeFunction with `CalleeHandled = false` so the JS callback
/// receives `(progress)` directly, not error-first `(null, progress)`.
type ProgressTsfn = napi::threadsafe_function::ThreadsafeFunction<
    JsCompactionProgress,
    Unknown<'static>,
    JsCompactionProgress,
    Status,
    false,
>;

pub struct JsJsonValue(serde_json::Value);

impl TypeName for JsJsonValue {
    fn type_name() -> &'static str {
        "Object"
    }

    fn value_type() -> napi::ValueType {
        napi::ValueType::Object
    }
}

impl ToNapiValue for JsJsonValue {
    unsafe fn to_napi_value(env: napi::sys::napi_env, val: Self) -> Result<napi::sys::napi_value> {
        unsafe { serde_json::Value::to_napi_value(env, val.0) }
    }
}
use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::{Arc, Condvar, Mutex};

// ============================================================
// Core wrapper
// ============================================================

struct InnerDb {
    engine: DatabaseEngine,
}

#[napi]
pub struct OverGraph {
    inner: Arc<Mutex<Option<InnerDb>>>,
}

#[napi]
impl OverGraph {
    // --- Lifecycle ---

    #[napi(factory)]
    pub fn open(path: String, options: Option<JsDbOptions>) -> Result<OverGraph> {
        let opts = options.map(|o| o.into()).unwrap_or_default();
        let engine = DatabaseEngine::open(Path::new(&path), &opts)
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        Ok(OverGraph {
            inner: Arc::new(Mutex::new(Some(InnerDb { engine }))),
        })
    }

    #[napi]
    pub fn close(&self, options: Option<JsCloseOptions>) -> Result<()> {
        let force = options.as_ref().and_then(|o| o.force).unwrap_or(false);
        let engine = {
            let mut guard = self
                .inner
                .lock()
                .map_err(|e| napi::Error::from_reason(e.to_string()))?;
            guard.take().map(|db| db.engine)
        };
        if let Some(engine) = engine {
            if force {
                engine
                    .close_fast()
                    .map_err(|e| napi::Error::from_reason(e.to_string()))?;
            } else {
                engine
                    .close()
                    .map_err(|e| napi::Error::from_reason(e.to_string()))?;
            }
        }
        Ok(())
    }

    // --- Single upserts ---

    #[napi]
    pub fn upsert_node(
        &self,
        type_id: u32,
        key: String,
        options: Option<JsUpsertNodeOptions>,
    ) -> Result<f64> {
        let (props, weight, dense_vector, sparse_vector) = match options {
            Some(o) => (o.props, o.weight, o.dense_vector, o.sparse_vector),
            None => (None, None, None, None),
        };
        let props = convert_js_props(props);
        let opts = UpsertNodeOptions {
            props,
            weight: weight.unwrap_or(1.0) as f32,
            dense_vector: dense_vector.map(|dv| dv.into_iter().map(|x| x as f32).collect()),
            sparse_vector: sparse_vector.map(|sv| {
                sv.into_iter()
                    .map(|e| (e.dimension, e.value as f32))
                    .collect()
            }),
        };
        let id = with_engine(self, |eng| eng.upsert_node(type_id, &key, opts))?;
        u64_to_f64(id)
    }

    #[napi]
    pub fn upsert_edge(
        &self,
        from: f64,
        to: f64,
        type_id: u32,
        options: Option<JsUpsertEdgeOptions>,
    ) -> Result<f64> {
        let from = f64_to_u64(from)?;
        let to = f64_to_u64(to)?;
        let (props, weight, valid_from, valid_to) = match options {
            Some(o) => (o.props, o.weight, o.valid_from, o.valid_to),
            None => (None, None, None, None),
        };
        let props = convert_js_props(props);
        let opts = UpsertEdgeOptions {
            props,
            weight: weight.unwrap_or(1.0) as f32,
            valid_from,
            valid_to,
        };
        let id = with_engine(self, |eng| eng.upsert_edge(from, to, type_id, opts))?;
        u64_to_f64(id)
    }

    // --- Batch upserts (JSON object path) ---

    #[napi]
    pub fn batch_upsert_nodes(&self, nodes: Vec<JsNodeInput>) -> Result<Float64Array> {
        let inputs: Vec<NodeInput> = nodes.into_iter().map(|n| n.into()).collect();
        let ids = with_engine(self, |eng| eng.batch_upsert_nodes(&inputs))?;
        ids_to_float64_array(&ids)
    }

    #[napi]
    pub fn batch_upsert_edges(&self, edges: Vec<JsEdgeInput>) -> Result<Float64Array> {
        let inputs: std::result::Result<Vec<EdgeInput>, _> =
            edges.into_iter().map(|e| e.try_into()).collect();
        let inputs = inputs?;
        let ids = with_engine(self, |eng| eng.batch_upsert_edges(&inputs))?;
        ids_to_float64_array(&ids)
    }

    // --- Batch upserts (binary buffer path) ---

    /// Batch upsert nodes from a packed binary Buffer. See `packNodeBatch()` in JS.
    ///
    /// Binary format (little-endian):
    ///   [count: u32]
    ///   per node:
    ///     [type_id: u32][weight: f32][key_len: u16][key: utf8][props_len: u32][props: json utf8]
    #[napi]
    pub fn batch_upsert_nodes_binary(&self, buffer: Buffer) -> Result<Float64Array> {
        let inputs = decode_node_batch(&buffer)?;
        let ids = with_engine(self, |eng| eng.batch_upsert_nodes(&inputs))?;
        ids_to_float64_array(&ids)
    }

    /// Batch upsert edges from a packed binary Buffer. See `packEdgeBatch()` in JS.
    ///
    /// Binary format (little-endian):
    ///   [count: u32]
    ///   per edge:
    ///     [from: u64][to: u64][type_id: u32][weight: f32]
    ///     [valid_from: i64][valid_to: i64][props_len: u32][props: json utf8]
    #[napi]
    pub fn batch_upsert_edges_binary(&self, buffer: Buffer) -> Result<Float64Array> {
        let inputs = decode_edge_batch(&buffer)?;
        let ids = with_engine(self, |eng| eng.batch_upsert_edges(&inputs))?;
        ids_to_float64_array(&ids)
    }

    // --- Gets ---

    #[napi]
    pub fn get_node(&self, id: f64) -> Result<Option<JsNodeRecord>> {
        let id = f64_to_u64(id)?;
        let raw = with_engine_ref(self, |eng| eng.get_node(id))?;
        raw.map(JsNodeRecord::try_from).transpose()
    }

    #[napi]
    pub fn get_edge(&self, id: f64) -> Result<Option<JsEdgeRecord>> {
        let id = f64_to_u64(id)?;
        let raw = with_engine_ref(self, |eng| eng.get_edge(id))?;
        raw.map(JsEdgeRecord::try_from).transpose()
    }

    // --- Key/triple lookups ---

    #[napi]
    pub fn get_node_by_key(&self, type_id: u32, key: String) -> Result<Option<JsNodeRecord>> {
        let raw = with_engine_ref(self, |eng| eng.get_node_by_key(type_id, &key))?;
        raw.map(JsNodeRecord::try_from).transpose()
    }

    #[napi]
    pub fn get_edge_by_triple(
        &self,
        from: f64,
        to: f64,
        type_id: u32,
    ) -> Result<Option<JsEdgeRecord>> {
        let from = f64_to_u64(from)?;
        let to = f64_to_u64(to)?;
        let raw = with_engine_ref(self, |eng| eng.get_edge_by_triple(from, to, type_id))?;
        raw.map(JsEdgeRecord::try_from).transpose()
    }

    // --- Bulk reads ---

    #[napi]
    pub fn get_nodes(&self, ids: Vec<f64>) -> Result<Vec<Option<JsNodeRecord>>> {
        let ids: Vec<u64> = ids
            .into_iter()
            .map(f64_to_u64)
            .collect::<Result<Vec<_>>>()?;
        let results = with_engine_ref(self, |eng| eng.get_nodes(&ids))?;
        results
            .into_iter()
            .map(|r| r.map(JsNodeRecord::try_from).transpose())
            .collect::<Result<Vec<_>>>()
    }

    #[napi]
    pub fn get_nodes_by_keys(&self, keys: Vec<JsKeyQuery>) -> Result<Vec<Option<JsNodeRecord>>> {
        let owned: Vec<(u32, String)> = keys.into_iter().map(|k| (k.type_id, k.key)).collect();
        let refs: Vec<(u32, &str)> = owned.iter().map(|(t, k)| (*t, k.as_str())).collect();
        let results = with_engine_ref(self, |eng| eng.get_nodes_by_keys(&refs))?;
        results
            .into_iter()
            .map(|r| r.map(JsNodeRecord::try_from).transpose())
            .collect::<Result<Vec<_>>>()
    }

    #[napi]
    pub fn get_edges(&self, ids: Vec<f64>) -> Result<Vec<Option<JsEdgeRecord>>> {
        let ids: Vec<u64> = ids
            .into_iter()
            .map(f64_to_u64)
            .collect::<Result<Vec<_>>>()?;
        let results = with_engine_ref(self, |eng| eng.get_edges(&ids))?;
        results
            .into_iter()
            .map(|r| r.map(JsEdgeRecord::try_from).transpose())
            .collect::<Result<Vec<_>>>()
    }

    // --- Deletes ---

    #[napi]
    pub fn delete_node(&self, id: f64) -> Result<()> {
        let id = f64_to_u64(id)?;
        with_engine(self, |eng| eng.delete_node(id))
    }

    #[napi]
    pub fn delete_edge(&self, id: f64) -> Result<()> {
        let id = f64_to_u64(id)?;
        with_engine(self, |eng| eng.delete_edge(id))
    }

    // --- Temporal invalidation ---

    #[napi]
    pub fn invalidate_edge(&self, id: f64, valid_to: i64) -> Result<Option<JsEdgeRecord>> {
        let id = f64_to_u64(id)?;
        let raw = with_engine(self, |eng| eng.invalidate_edge(id, valid_to))?;
        raw.map(JsEdgeRecord::try_from).transpose()
    }

    #[napi]
    pub fn graph_patch(&self, patch: JsGraphPatch) -> Result<JsPatchResult> {
        let rust_patch = js_patch_to_rust(patch)?;
        let result = with_engine(self, |eng| eng.graph_patch(&rust_patch))?;
        Ok(JsPatchResult {
            node_ids: ids_to_float64_array(&result.node_ids)?,
            edge_ids: ids_to_float64_array(&result.edge_ids)?,
        })
    }

    #[napi]
    pub fn begin_write_txn(&self) -> Result<JsWriteTxn> {
        let txn = with_engine_ref(self, |eng| eng.begin_write_txn())?;
        Ok(write_txn_to_js(txn))
    }

    #[napi(ts_return_type = "Promise<JsWriteTxn>")]
    pub fn begin_write_txn_async(&self) -> AsyncTask<EngineReadOp<WriteTxn, JsWriteTxn>> {
        AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            |eng| eng.begin_write_txn(),
            |txn| Ok(write_txn_to_js(txn)),
        ))
    }

    // --- Retention / Forgetting ---

    #[napi]
    pub fn prune(&self, policy: JsPrunePolicy) -> Result<JsPruneResult> {
        let rust_policy = PrunePolicy {
            max_age_ms: policy.max_age_ms.map(|v| v as i64),
            max_weight: policy.max_weight.map(|v| v as f32),
            type_id: policy.type_id,
        };
        with_engine(self, |eng| {
            let result = eng.prune(&rust_policy)?;
            Ok(JsPruneResult {
                nodes_pruned: result.nodes_pruned as i64,
                edges_pruned: result.edges_pruned as i64,
            })
        })
    }

    // --- Named prune policies (compaction-filter auto-prune) ---

    #[napi]
    pub fn set_prune_policy(&self, name: String, policy: JsPrunePolicy) -> Result<()> {
        let rust_policy = PrunePolicy {
            max_age_ms: policy.max_age_ms.map(|v| v as i64),
            max_weight: policy.max_weight.map(|v| v as f32),
            type_id: policy.type_id,
        };
        with_engine(self, |eng| {
            eng.set_prune_policy(&name, rust_policy)?;
            Ok(())
        })
    }

    #[napi]
    pub fn remove_prune_policy(&self, name: String) -> Result<bool> {
        with_engine(self, |eng| eng.remove_prune_policy(&name))
    }

    #[napi]
    pub fn list_prune_policies(&self) -> Result<Vec<JsNamedPrunePolicy>> {
        with_engine_ref(self, |eng| {
            Ok(eng
                .list_prune_policies()?
                .into_iter()
                .map(|(name, p)| JsNamedPrunePolicy {
                    name,
                    policy: JsPrunePolicy {
                        max_age_ms: p.max_age_ms.map(|v| v as f64),
                        max_weight: p.max_weight.map(|v| v as f64),
                        type_id: p.type_id,
                    },
                })
                .collect())
        })
    }

    #[napi(ts_return_type = "Promise<void>")]
    pub fn set_prune_policy_async(
        &self,
        name: String,
        policy: JsPrunePolicy,
    ) -> Result<AsyncTask<EngineOp<(), ()>>> {
        let rust_policy = PrunePolicy {
            max_age_ms: policy.max_age_ms.map(|v| v as i64),
            max_weight: policy.max_weight.map(|v| v as f32),
            type_id: policy.type_id,
        };
        Ok(AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| {
                eng.set_prune_policy(&name, rust_policy)?;
                Ok(())
            },
            |_| Ok(()),
        )))
    }

    #[napi(ts_return_type = "Promise<boolean>")]
    pub fn remove_prune_policy_async(
        &self,
        name: String,
    ) -> Result<AsyncTask<EngineOp<bool, bool>>> {
        Ok(AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| eng.remove_prune_policy(&name),
            Ok,
        )))
    }

    #[napi(ts_return_type = "Promise<Array<JsNamedPrunePolicy>>")]
    pub fn list_prune_policies_async(
        &self,
    ) -> Result<AsyncTask<EngineReadOp<Vec<(String, PrunePolicy)>, Vec<JsNamedPrunePolicy>>>> {
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.list_prune_policies(),
            |policies| {
                Ok(policies
                    .into_iter()
                    .map(|(name, p)| JsNamedPrunePolicy {
                        name,
                        policy: JsPrunePolicy {
                            max_age_ms: p.max_age_ms.map(|v| v as f64),
                            max_weight: p.max_weight.map(|v| v as f64),
                            type_id: p.type_id,
                        },
                    })
                    .collect())
            },
        )))
    }

    // --- Queries ---

    #[napi]
    pub fn neighbors(
        &self,
        node_id: f64,
        options: Option<JsNeighborsOptions>,
    ) -> Result<Vec<JsNeighborEntry>> {
        let node_id = f64_to_u64(node_id)?;
        let (direction, type_filter, limit, at_epoch, decay_lambda) = match options {
            Some(o) => (
                o.direction,
                o.type_filter,
                o.limit,
                o.at_epoch,
                o.decay_lambda,
            ),
            None => (None, None, None, None, None),
        };
        let dir = parse_direction(direction.as_deref())?;
        let lim = limit.map(|v| v as usize);
        let decay = decay_lambda.map(|v| v as f32);
        let opts = NeighborOptions {
            direction: dir,
            type_filter,
            limit: lim,
            at_epoch,
            decay_lambda: decay,
        };
        let entries = with_engine_ref(self, |eng| eng.neighbors(node_id, &opts))?;
        neighbor_entries_to_js(entries)
    }

    #[napi]
    pub fn traverse(
        &self,
        start_node_id: f64,
        max_depth: u32,
        options: Option<JsTraverseOptions>,
    ) -> Result<JsTraversalPageResult> {
        let start_node_id = f64_to_u64(start_node_id)?;
        let (
            direction,
            min_depth,
            edge_type_filter,
            node_type_filter,
            at_epoch,
            decay_lambda,
            limit,
            cursor,
        ) = match options {
            Some(o) => (
                o.direction,
                o.min_depth,
                o.edge_type_filter,
                o.node_type_filter,
                o.at_epoch,
                o.decay_lambda,
                o.limit,
                o.cursor,
            ),
            None => (None, None, None, None, None, None, None, None),
        };
        let dir = parse_direction(direction.as_deref())?;
        let min_depth = min_depth.unwrap_or(1);
        let cursor = cursor.map(js_traversal_cursor_to_rust).transpose()?;
        let opts = TraverseOptions {
            min_depth,
            direction: dir,
            edge_type_filter,
            node_type_filter,
            at_epoch,
            decay_lambda,
            limit: limit.map(|v| v as usize),
            cursor,
        };
        let page = with_engine_ref(self, |eng| eng.traverse(start_node_id, max_depth, &opts))?;
        traversal_page_to_js(page)
    }

    #[napi]
    pub fn top_k_neighbors(
        &self,
        node_id: f64,
        k: u32,
        options: Option<JsTopKNeighborsOptions>,
    ) -> Result<Vec<JsNeighborEntry>> {
        let node_id = f64_to_u64(node_id)?;
        let (direction, type_filter, scoring, decay_lambda, at_epoch) = match options {
            Some(o) => (
                o.direction,
                o.type_filter,
                o.scoring,
                o.decay_lambda,
                o.at_epoch,
            ),
            None => (None, None, None, None, None),
        };
        let dir = parse_direction(direction.as_deref())?;
        let scoring_mode = parse_scoring_mode(scoring.as_deref(), decay_lambda)?;
        let opts = TopKOptions {
            direction: dir,
            type_filter,
            scoring: scoring_mode,
            at_epoch,
        };
        let entries = with_engine_ref(self, |eng| eng.top_k_neighbors(node_id, k as usize, &opts))?;
        neighbor_entries_to_js(entries)
    }

    #[napi]
    pub fn extract_subgraph(
        &self,
        start_node_id: f64,
        max_depth: u32,
        options: Option<JsExtractSubgraphOptions>,
    ) -> Result<JsSubgraphResult> {
        let start = f64_to_u64(start_node_id)?;
        let (direction, edge_type_filter, at_epoch) = match options {
            Some(o) => (o.direction, o.edge_type_filter, o.at_epoch),
            None => (None, None, None),
        };
        let dir = parse_direction(direction.as_deref())?;
        let opts = SubgraphOptions {
            direction: dir,
            edge_type_filter,
            at_epoch,
        };
        let sg = with_engine_ref(self, |eng| eng.extract_subgraph(start, max_depth, &opts))?;
        subgraph_to_js(sg)
    }

    /// Batch neighbor query: fetch neighbors for multiple nodes in one call.
    /// Returns an array of entries, each mapping a query node to its neighbors.
    #[napi]
    pub fn neighbors_batch(
        &self,
        node_ids: Vec<f64>,
        options: Option<JsNeighborsBatchOptions>,
    ) -> Result<Vec<JsNeighborBatchEntry>> {
        let ids: Vec<u64> = node_ids
            .into_iter()
            .map(f64_to_u64)
            .collect::<Result<Vec<_>>>()?;
        let (direction, type_filter, at_epoch, decay_lambda) = match options {
            Some(o) => (o.direction, o.type_filter, o.at_epoch, o.decay_lambda),
            None => (None, None, None, None),
        };
        let dir = parse_direction(direction.as_deref())?;
        let decay = decay_lambda.map(|v| v as f32);
        let opts = NeighborOptions {
            direction: dir,
            type_filter,
            limit: None,
            at_epoch,
            decay_lambda: decay,
        };
        let map = with_engine_ref(self, |eng| eng.neighbors_batch(&ids, &opts))?;
        convert_batch_result(map)
    }

    // --- Degree counts + aggregations (Phase 18a) ---

    #[napi]
    pub fn degree(&self, node_id: f64, options: Option<JsDegreeOptions>) -> Result<i64> {
        let node_id = f64_to_u64(node_id)?;
        let (direction, type_filter, at_epoch) = match options {
            Some(o) => (o.direction, o.type_filter, o.at_epoch),
            None => (None, None, None),
        };
        let dir = parse_direction(direction.as_deref())?;
        let opts = DegreeOptions {
            direction: dir,
            type_filter,
            at_epoch,
        };
        let count: u64 = with_engine_ref(self, |eng| eng.degree(node_id, &opts))?;
        u64_to_safe_i64(count)
    }

    #[napi]
    pub fn sum_edge_weights(
        &self,
        node_id: f64,
        options: Option<JsSumEdgeWeightsOptions>,
    ) -> Result<f64> {
        let node_id = f64_to_u64(node_id)?;
        let (direction, type_filter, at_epoch) = match options {
            Some(o) => (o.direction, o.type_filter, o.at_epoch),
            None => (None, None, None),
        };
        let dir = parse_direction(direction.as_deref())?;
        let opts = DegreeOptions {
            direction: dir,
            type_filter,
            at_epoch,
        };
        with_engine_ref(self, |eng| eng.sum_edge_weights(node_id, &opts))
    }

    #[napi]
    pub fn avg_edge_weight(
        &self,
        node_id: f64,
        options: Option<JsAvgEdgeWeightOptions>,
    ) -> Result<Option<f64>> {
        let node_id = f64_to_u64(node_id)?;
        let (direction, type_filter, at_epoch) = match options {
            Some(o) => (o.direction, o.type_filter, o.at_epoch),
            None => (None, None, None),
        };
        let dir = parse_direction(direction.as_deref())?;
        let opts = DegreeOptions {
            direction: dir,
            type_filter,
            at_epoch,
        };
        with_engine_ref(self, |eng| eng.avg_edge_weight(node_id, &opts))
    }

    #[napi]
    pub fn degrees(
        &self,
        node_ids: Vec<f64>,
        options: Option<JsDegreesOptions>,
    ) -> Result<Vec<JsDegreeBatchEntry>> {
        let ids: Vec<u64> = node_ids
            .into_iter()
            .map(f64_to_u64)
            .collect::<Result<Vec<_>>>()?;
        let (direction, type_filter, at_epoch) = match options {
            Some(o) => (o.direction, o.type_filter, o.at_epoch),
            None => (None, None, None),
        };
        let dir = parse_direction(direction.as_deref())?;
        let opts = DegreeOptions {
            direction: dir,
            type_filter,
            at_epoch,
        };
        let map = with_engine_ref(self, |eng| eng.degrees(&ids, &opts))?;
        let mut entries: Vec<JsDegreeBatchEntry> = map
            .into_iter()
            .map(|(node_id, degree)| {
                Ok(JsDegreeBatchEntry {
                    node_id: u64_to_f64(node_id)?,
                    degree: u64_to_safe_i64(degree)?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        entries.sort_by(|a, b| a.node_id.total_cmp(&b.node_id));
        Ok(entries)
    }

    // --- Shortest path (Phase 18b) ---

    #[napi]
    pub fn shortest_path(
        &self,
        from: f64,
        to: f64,
        options: Option<JsShortestPathOptions>,
    ) -> Result<Option<JsShortestPath>> {
        let from = f64_to_u64(from)?;
        let to = f64_to_u64(to)?;
        let (direction, type_filter, weight_field, at_epoch, max_depth, max_cost) = match options {
            Some(o) => (
                o.direction,
                o.type_filter,
                o.weight_field,
                o.at_epoch,
                o.max_depth,
                o.max_cost,
            ),
            None => (None, None, None, None, None, None),
        };
        let dir = parse_direction(direction.as_deref())?;
        let opts = ShortestPathOptions {
            direction: dir,
            type_filter,
            weight_field,
            at_epoch,
            max_depth,
            max_cost,
        };
        let result: Option<ShortestPath> =
            with_engine_ref(self, |eng| eng.shortest_path(from, to, &opts))?;
        result.map(shortest_path_to_js).transpose()
    }

    #[napi]
    pub fn is_connected(
        &self,
        from: f64,
        to: f64,
        options: Option<JsIsConnectedOptions>,
    ) -> Result<bool> {
        let from = f64_to_u64(from)?;
        let to = f64_to_u64(to)?;
        let (direction, type_filter, at_epoch, max_depth) = match options {
            Some(o) => (o.direction, o.type_filter, o.at_epoch, o.max_depth),
            None => (None, None, None, None),
        };
        let dir = parse_direction(direction.as_deref())?;
        let opts = IsConnectedOptions {
            direction: dir,
            type_filter,
            at_epoch,
            max_depth,
        };
        with_engine_ref(self, |eng| eng.is_connected(from, to, &opts))
    }

    #[napi]
    pub fn all_shortest_paths(
        &self,
        from: f64,
        to: f64,
        options: Option<JsAllShortestPathsOptions>,
    ) -> Result<Vec<JsShortestPath>> {
        let from = f64_to_u64(from)?;
        let to = f64_to_u64(to)?;
        let (direction, type_filter, weight_field, at_epoch, max_depth, max_cost, max_paths) =
            match options {
                Some(o) => (
                    o.direction,
                    o.type_filter,
                    o.weight_field,
                    o.at_epoch,
                    o.max_depth,
                    o.max_cost,
                    o.max_paths,
                ),
                None => (None, None, None, None, None, None, None),
            };
        let dir = parse_direction(direction.as_deref())?;
        let opts = AllShortestPathsOptions {
            direction: dir,
            type_filter,
            weight_field,
            at_epoch,
            max_depth,
            max_cost,
            max_paths: max_paths.map(|n| n as usize),
        };
        let paths: Vec<ShortestPath> =
            with_engine_ref(self, |eng| eng.all_shortest_paths(from, to, &opts))?;
        paths.into_iter().map(shortest_path_to_js).collect()
    }

    #[napi]
    pub fn find_nodes(
        &self,
        type_id: u32,
        prop_key: String,
        prop_value: serde_json::Value,
    ) -> Result<Float64Array> {
        let pv = json_to_prop_value(&prop_value);
        let ids = with_engine_ref(self, |eng| eng.find_nodes(type_id, &prop_key, &pv))?;
        ids_to_float64_array(&ids)
    }

    #[napi(
        ts_args_type = "request: import('./query-types').QueryNodeRequest",
        ts_return_type = "JsIdPageResult"
    )]
    pub fn query_node_ids(&self, request: serde_json::Value) -> Result<JsIdPageResult> {
        let query = parse_js_node_query(&request)?;
        let result = with_engine_ref(self, |eng| eng.query_node_ids(&query))?;
        query_node_ids_to_js(result)
    }

    #[napi(
        ts_args_type = "request: import('./query-types').QueryNodeRequest",
        ts_return_type = "JsNodePageResult"
    )]
    pub fn query_nodes(&self, request: serde_json::Value) -> Result<JsNodePageResult> {
        let query = parse_js_node_query(&request)?;
        let result = with_engine_ref(self, |eng| eng.query_nodes(&query))?;
        query_nodes_to_js(result)
    }

    #[napi(
        ts_args_type = "request: import('./query-types').GraphPatternRequest",
        ts_return_type = "import('./query-types').QueryPatternResult"
    )]
    pub fn query_pattern(&self, request: serde_json::Value) -> Result<JsJsonValue> {
        let query = parse_js_graph_pattern_query(&request)?;
        let result = with_engine_ref(self, |eng| eng.query_pattern(&query))?;
        query_pattern_result_to_js(result)
    }

    #[napi(
        ts_args_type = "request: import('./query-types').QueryNodeRequest",
        ts_return_type = "import('./query-types').QueryPlan"
    )]
    pub fn explain_node_query(&self, request: serde_json::Value) -> Result<JsJsonValue> {
        let query = parse_js_node_query(&request)?;
        let plan = with_engine_ref(self, |eng| eng.explain_node_query(&query))?;
        query_plan_to_js(plan)
    }

    #[napi(
        ts_args_type = "request: import('./query-types').GraphPatternRequest",
        ts_return_type = "import('./query-types').QueryPlan"
    )]
    pub fn explain_pattern_query(&self, request: serde_json::Value) -> Result<JsJsonValue> {
        let query = parse_js_graph_pattern_query(&request)?;
        let plan = with_engine_ref(self, |eng| eng.explain_pattern_query(&query))?;
        query_plan_to_js(plan)
    }

    #[napi]
    pub fn ensure_node_property_index(
        &self,
        type_id: u32,
        prop_key: String,
        kind: JsSecondaryIndexKind,
    ) -> Result<JsNodePropertyIndexInfo> {
        let kind = js_secondary_index_kind_to_rust(kind)?;
        let info = with_engine(self, |eng| {
            eng.ensure_node_property_index(type_id, &prop_key, kind.clone())
        })?;
        node_property_index_info_to_js(info)
    }

    #[napi]
    pub fn drop_node_property_index(
        &self,
        type_id: u32,
        prop_key: String,
        kind: JsSecondaryIndexKind,
    ) -> Result<bool> {
        let kind = js_secondary_index_kind_to_rust(kind)?;
        with_engine(self, |eng| {
            eng.drop_node_property_index(type_id, &prop_key, kind.clone())
        })
    }

    #[napi]
    pub fn list_node_property_indexes(&self) -> Result<Vec<JsNodePropertyIndexInfo>> {
        let infos = with_engine_ref(self, |eng| eng.list_node_property_indexes())?;
        infos
            .into_iter()
            .map(node_property_index_info_to_js)
            .collect()
    }

    /// Return all node IDs of a given type (unpaged).
    #[napi]
    pub fn nodes_by_type(&self, type_id: u32) -> Result<Float64Array> {
        let ids = with_engine_ref(self, |eng| eng.nodes_by_type(type_id))?;
        ids_to_float64_array(&ids)
    }

    /// Return all edge IDs of a given type (unpaged).
    #[napi]
    pub fn edges_by_type(&self, type_id: u32) -> Result<Float64Array> {
        let ids = with_engine_ref(self, |eng| eng.edges_by_type(type_id))?;
        ids_to_float64_array(&ids)
    }

    #[napi]
    pub fn get_nodes_by_type(&self, type_id: u32) -> Result<Vec<JsNodeRecord>> {
        let records = with_engine_ref(self, |eng| eng.get_nodes_by_type(type_id))?;
        records
            .into_iter()
            .map(JsNodeRecord::try_from)
            .collect::<Result<Vec<_>>>()
    }

    #[napi]
    pub fn get_edges_by_type(&self, type_id: u32) -> Result<Vec<JsEdgeRecord>> {
        let records = with_engine_ref(self, |eng| eng.get_edges_by_type(type_id))?;
        records
            .into_iter()
            .map(JsEdgeRecord::try_from)
            .collect::<Result<Vec<_>>>()
    }

    #[napi]
    pub fn count_nodes_by_type(&self, type_id: u32) -> Result<i64> {
        with_engine_ref(self, |eng| Ok(eng.count_nodes_by_type(type_id)? as i64))
    }

    #[napi]
    pub fn count_edges_by_type(&self, type_id: u32) -> Result<i64> {
        with_engine_ref(self, |eng| Ok(eng.count_edges_by_type(type_id)? as i64))
    }

    // --- Paginated queries (sync) ---

    #[napi]
    pub fn nodes_by_type_paged(
        &self,
        type_id: u32,
        limit: Option<u32>,
        after: Option<f64>,
    ) -> Result<JsIdPageResult> {
        let page = make_page_request(limit, after)?;
        let raw = with_engine_ref(self, |eng| eng.nodes_by_type_paged(type_id, &page))?;
        id_page_to_js(raw)
    }

    #[napi]
    pub fn edges_by_type_paged(
        &self,
        type_id: u32,
        limit: Option<u32>,
        after: Option<f64>,
    ) -> Result<JsIdPageResult> {
        let page = make_page_request(limit, after)?;
        let raw = with_engine_ref(self, |eng| eng.edges_by_type_paged(type_id, &page))?;
        id_page_to_js(raw)
    }

    #[napi]
    pub fn get_nodes_by_type_paged(
        &self,
        type_id: u32,
        limit: Option<u32>,
        after: Option<f64>,
    ) -> Result<JsNodePageResult> {
        let page = make_page_request(limit, after)?;
        let raw = with_engine_ref(self, |eng| eng.get_nodes_by_type_paged(type_id, &page))?;
        node_page_to_js(raw)
    }

    #[napi]
    pub fn get_edges_by_type_paged(
        &self,
        type_id: u32,
        limit: Option<u32>,
        after: Option<f64>,
    ) -> Result<JsEdgePageResult> {
        let page = make_page_request(limit, after)?;
        let raw = with_engine_ref(self, |eng| eng.get_edges_by_type_paged(type_id, &page))?;
        edge_page_to_js(raw)
    }

    #[napi]
    pub fn find_nodes_paged(
        &self,
        type_id: u32,
        prop_key: String,
        prop_value: serde_json::Value,
        options: Option<JsFindNodesPagedOptions>,
    ) -> Result<JsIdPageResult> {
        let pv = json_to_prop_value(&prop_value);
        let (limit, after) = match options {
            Some(o) => (o.limit, o.after),
            None => (None, None),
        };
        let page = make_page_request(limit, after)?;
        let raw = with_engine_ref(self, |eng| {
            eng.find_nodes_paged(type_id, &prop_key, &pv, &page)
        })?;
        id_page_to_js(raw)
    }

    #[napi]
    pub fn find_nodes_by_time_range(
        &self,
        type_id: u32,
        from_ms: i64,
        to_ms: i64,
    ) -> Result<Float64Array> {
        let ids = with_engine_ref(self, |eng| {
            eng.find_nodes_by_time_range(type_id, from_ms, to_ms)
        })?;
        ids_to_float64_array(&ids)
    }

    #[napi]
    pub fn find_nodes_range(
        &self,
        type_id: u32,
        prop_key: String,
        lower: Option<JsPropertyRangeBound>,
        upper: Option<JsPropertyRangeBound>,
    ) -> Result<Float64Array> {
        let lower = lower
            .as_ref()
            .map(js_property_range_bound_to_rust)
            .transpose()?;
        let upper = upper
            .as_ref()
            .map(js_property_range_bound_to_rust)
            .transpose()?;
        let ids = with_engine_ref(self, |eng| {
            eng.find_nodes_range(type_id, &prop_key, lower.as_ref(), upper.as_ref())
        })?;
        ids_to_float64_array(&ids)
    }

    #[napi]
    pub fn find_nodes_by_time_range_paged(
        &self,
        type_id: u32,
        from_ms: i64,
        to_ms: i64,
        options: Option<JsFindNodesByTimeRangePagedOptions>,
    ) -> Result<JsIdPageResult> {
        let (limit, after) = match options {
            Some(o) => (o.limit, o.after),
            None => (None, None),
        };
        let page = make_page_request(limit, after)?;
        let raw = with_engine_ref(self, |eng| {
            eng.find_nodes_by_time_range_paged(type_id, from_ms, to_ms, &page)
        })?;
        id_page_to_js(raw)
    }

    #[napi]
    pub fn find_nodes_range_paged(
        &self,
        type_id: u32,
        prop_key: String,
        lower: Option<JsPropertyRangeBound>,
        upper: Option<JsPropertyRangeBound>,
        options: Option<JsFindNodesRangePagedOptions>,
    ) -> Result<JsPropertyRangePageResult> {
        let lower = lower
            .as_ref()
            .map(js_property_range_bound_to_rust)
            .transpose()?;
        let upper = upper
            .as_ref()
            .map(js_property_range_bound_to_rust)
            .transpose()?;
        let page = make_property_range_page_request(options)?;
        let raw = with_engine_ref(self, |eng| {
            eng.find_nodes_range_paged(type_id, &prop_key, lower.as_ref(), upper.as_ref(), &page)
        })?;
        property_range_page_to_js(raw)
    }

    #[napi]
    pub fn personalized_pagerank(
        &self,
        seed_node_ids: Vec<f64>,
        options: Option<JsPersonalizedPagerankOptions>,
    ) -> Result<JsPprResult> {
        let seeds: Vec<u64> = seed_node_ids
            .into_iter()
            .map(f64_to_u64)
            .collect::<Result<Vec<_>>>()?;
        let (
            algorithm,
            damping_factor,
            max_iterations,
            epsilon,
            approx_residual_tolerance,
            edge_type_filter,
            max_results,
        ) = match &options {
            Some(o) => (
                o.algorithm.as_deref(),
                o.damping_factor,
                o.max_iterations,
                o.epsilon,
                o.approx_residual_tolerance,
                o.edge_type_filter.clone(),
                o.max_results,
            ),
            None => (None, None, None, None, None, None, None),
        };
        let opts = js_ppr_options_to_ppr_options(
            algorithm,
            &damping_factor,
            &max_iterations,
            &epsilon,
            &approx_residual_tolerance,
            &edge_type_filter,
            &max_results,
        )?;
        let result = with_engine_ref(self, |eng| eng.personalized_pagerank(&seeds, &opts))?;
        ppr_result_to_js(result)
    }

    #[napi]
    pub fn export_adjacency(&self, options: Option<JsExportOptions>) -> Result<JsAdjacencyExport> {
        let include_weights = options
            .as_ref()
            .and_then(|o| o.include_weights)
            .unwrap_or(true);
        let opts = js_export_options_to_rust(options);
        let result = with_engine_ref(self, |eng| eng.export_adjacency(&opts))?;
        adjacency_export_to_js(result, include_weights)
    }

    #[napi]
    pub fn neighbors_paged(
        &self,
        node_id: f64,
        options: Option<JsNeighborsPagedOptions>,
    ) -> Result<JsNeighborPageResult> {
        let node_id = f64_to_u64(node_id)?;
        let (direction, type_filter, limit, after, at_epoch, decay_lambda) = match options {
            Some(o) => (
                o.direction,
                o.type_filter,
                o.limit,
                o.after,
                o.at_epoch,
                o.decay_lambda,
            ),
            None => (None, None, None, None, None, None),
        };
        let dir = parse_direction(direction.as_deref())?;
        let page = make_page_request(limit, after)?;
        let decay = decay_lambda.map(|v| v as f32);
        let opts = NeighborOptions {
            direction: dir,
            type_filter,
            limit: None,
            at_epoch,
            decay_lambda: decay,
        };
        let page = with_engine_ref(self, |eng| eng.neighbors_paged(node_id, &opts, &page))?;
        neighbor_page_to_js(page)
    }

    // --- Connected Components (Phase 18d) ---

    #[napi]
    pub fn connected_components(
        &self,
        options: Option<JsConnectedComponentsOptions>,
    ) -> Result<Vec<JsComponentEntry>> {
        let (edge_type_filter, node_type_filter, at_epoch) = match options {
            Some(o) => (o.edge_type_filter, o.node_type_filter, o.at_epoch),
            None => (None, None, None),
        };
        let opts = ComponentOptions {
            edge_type_filter,
            node_type_filter,
            at_epoch,
        };
        let map = with_engine_ref(self, |eng| eng.connected_components(&opts))?;
        let mut entries: Vec<JsComponentEntry> = map
            .into_iter()
            .map(|(node_id, component_id)| {
                Ok(JsComponentEntry {
                    node_id: u64_to_f64(node_id)?,
                    component_id: u64_to_f64(component_id)?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        entries.sort_by(|a, b| a.node_id.total_cmp(&b.node_id));
        Ok(entries)
    }

    #[napi]
    pub fn component_of(
        &self,
        node_id: f64,
        options: Option<JsComponentOfOptions>,
    ) -> Result<Float64Array> {
        let node_id = f64_to_u64(node_id)?;
        let (edge_type_filter, node_type_filter, at_epoch) = match options {
            Some(o) => (o.edge_type_filter, o.node_type_filter, o.at_epoch),
            None => (None, None, None),
        };
        let opts = ComponentOptions {
            edge_type_filter,
            node_type_filter,
            at_epoch,
        };
        let members = with_engine_ref(self, |eng| eng.component_of(node_id, &opts))?;
        ids_to_float64_array(&members)
    }

    // --- Vector search (Phase 19) ---

    #[napi]
    pub fn vector_search(
        &self,
        mode: String,
        options: JsVectorSearchOptions,
    ) -> Result<Vec<JsVectorHit>> {
        let mode = parse_vector_search_mode(&mode)?;
        let k = options.k;
        let dense_query = options.dense_query;
        let sparse_query = options.sparse_query;
        let type_filter = options.type_filter;
        let ef_search = options.ef_search;
        let scope = options.scope;
        let dense_weight = options.dense_weight;
        let sparse_weight = options.sparse_weight;
        let fusion_mode = options.fusion_mode;
        let fusion = parse_fusion_mode(fusion_mode.as_deref())?;
        let dense_q = dense_query.map(|v| v.into_iter().map(|x| x as f32).collect());
        let sparse_q = sparse_query.map(|v| {
            v.into_iter()
                .map(|e| (e.dimension, e.value as f32))
                .collect()
        });
        let scope = match scope {
            None => None,
            Some(s) => Some(VectorSearchScope {
                start_node_id: f64_to_u64(s.start_node_id)?,
                max_depth: s.max_depth,
                direction: parse_direction(s.direction.as_deref())?,
                edge_type_filter: s.edge_type_filter,
                at_epoch: s.at_epoch,
            }),
        };
        let request = VectorSearchRequest {
            mode,
            dense_query: dense_q,
            sparse_query: sparse_q,
            k: k as usize,
            type_filter,
            ef_search: ef_search.map(|v| v as usize),
            scope,
            dense_weight: dense_weight.map(|v| v as f32),
            sparse_weight: sparse_weight.map(|v| v as f32),
            fusion_mode: fusion,
        };
        let hits = with_engine_ref(self, |eng| eng.vector_search(&request))?;
        hits.into_iter()
            .map(|h| {
                Ok(JsVectorHit {
                    node_id: u64_to_f64(h.node_id)?,
                    score: h.score as f64,
                })
            })
            .collect::<Result<Vec<_>>>()
    }

    // --- Maintenance ---

    /// Force an immediate WAL fsync. In GroupCommit mode, blocks until all
    /// buffered data is durable. In Immediate mode, this is a no-op.
    #[napi]
    pub fn sync(&self) -> Result<()> {
        with_engine(self, |eng| {
            eng.sync()?;
            Ok(())
        })
    }

    #[napi]
    pub fn flush(&self) -> Result<()> {
        with_engine(self, |eng| {
            eng.flush()?;
            Ok(())
        })
    }

    #[napi]
    pub fn ingest_mode(&self) -> Result<()> {
        with_engine(self, |eng| eng.ingest_mode())
    }

    #[napi]
    pub fn end_ingest(&self) -> Result<Option<JsCompactionStats>> {
        with_engine(self, |eng| Ok(eng.end_ingest()?.map(|s| s.into())))
    }

    #[napi]
    pub fn compact(&self) -> Result<Option<JsCompactionStats>> {
        with_engine(self, |eng| Ok(eng.compact()?.map(|s| s.into())))
    }

    /// Compact with a progress callback. The callback receives a progress object
    /// and should return `true` to continue or `false` to cancel.
    /// Runs synchronously. Blocks the event loop.
    #[napi(ts_args_type = "callback: (progress: JsCompactionProgress) => boolean")]
    pub fn compact_with_progress(
        &self,
        callback: Function<JsCompactionProgress, bool>,
    ) -> Result<Option<JsCompactionStats>> {
        with_engine(self, |eng| {
            let result = eng.compact_with_progress(|progress| {
                let js_progress = JsCompactionProgress {
                    phase: match progress.phase {
                        CompactionPhase::CollectingTombstones => {
                            "collecting_tombstones".to_string()
                        }
                        CompactionPhase::MergingNodes => "merging_nodes".to_string(),
                        CompactionPhase::MergingEdges => "merging_edges".to_string(),
                        CompactionPhase::WritingOutput => "writing_output".to_string(),
                    },
                    // Safe: segment counts are bounded by filesystem limits, well within u32.
                    segments_processed: progress.segments_processed as u32,
                    total_segments: progress.total_segments as u32,
                    records_processed: progress.records_processed as i64,
                    total_records: progress.total_records as i64,
                };

                // If the JS callback throws, cancel compaction rather than
                // silently continuing; a broken callback should stop work.
                callback.call(js_progress).unwrap_or(false)
            });

            match result {
                Ok(stats) => Ok(stats.map(|s| s.into())),
                Err(e) => Err(e),
            }
        })
    }

    #[napi]
    pub fn stats(&self) -> Result<JsDbStats> {
        with_engine_ref(self, |eng| Ok(eng.stats()?.into()))
    }

    // ============================
    // Async API (Promise-returning)
    // ============================

    #[napi(ts_return_type = "Promise<void>")]
    pub fn close_async(&self, options: Option<JsCloseOptions>) -> AsyncTask<CloseOp> {
        let force = options.as_ref().and_then(|o| o.force).unwrap_or(false);
        AsyncTask::new(CloseOp {
            db: self.inner.clone(),
            force,
        })
    }

    #[napi(ts_return_type = "Promise<JsDbStats>")]
    pub fn stats_async(&self) -> AsyncTask<EngineReadOp<DbStats, JsDbStats>> {
        AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            |eng| eng.stats(),
            |s| Ok(s.into()),
        ))
    }

    #[napi(ts_return_type = "Promise<number>")]
    pub fn upsert_node_async(
        &self,
        type_id: u32,
        key: String,
        options: Option<JsUpsertNodeOptions>,
    ) -> AsyncTask<EngineOp<u64, f64>> {
        let (props, weight, dense_vector, sparse_vector) = match options {
            Some(o) => (o.props, o.weight, o.dense_vector, o.sparse_vector),
            None => (None, None, None, None),
        };
        let props = convert_js_props(props);
        let opts = UpsertNodeOptions {
            props,
            weight: weight.unwrap_or(1.0) as f32,
            dense_vector: dense_vector.map(|dv| dv.into_iter().map(|x| x as f32).collect()),
            sparse_vector: sparse_vector.map(|sv| {
                sv.into_iter()
                    .map(|e| (e.dimension, e.value as f32))
                    .collect()
            }),
        };
        AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| eng.upsert_node(type_id, &key, opts),
            u64_to_f64,
        ))
    }

    #[napi(ts_return_type = "Promise<number>")]
    pub fn upsert_edge_async(
        &self,
        from: f64,
        to: f64,
        type_id: u32,
        options: Option<JsUpsertEdgeOptions>,
    ) -> Result<AsyncTask<EngineOp<u64, f64>>> {
        let from = f64_to_u64(from)?;
        let to = f64_to_u64(to)?;
        let (props, weight, valid_from, valid_to) = match options {
            Some(o) => (o.props, o.weight, o.valid_from, o.valid_to),
            None => (None, None, None, None),
        };
        let props = convert_js_props(props);
        let opts = UpsertEdgeOptions {
            props,
            weight: weight.unwrap_or(1.0) as f32,
            valid_from,
            valid_to,
        };
        Ok(AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| eng.upsert_edge(from, to, type_id, opts),
            u64_to_f64,
        )))
    }

    #[napi(ts_return_type = "Promise<Float64Array>")]
    pub fn batch_upsert_nodes_async(
        &self,
        nodes: Vec<JsNodeInput>,
    ) -> AsyncTask<EngineOp<Vec<u64>, Float64Array>> {
        let inputs: Vec<NodeInput> = nodes.into_iter().map(|n| n.into()).collect();
        AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| eng.batch_upsert_nodes(&inputs),
            |ids| ids_to_float64_array(&ids),
        ))
    }

    #[napi(ts_return_type = "Promise<Float64Array>")]
    pub fn batch_upsert_edges_async(
        &self,
        edges: Vec<JsEdgeInput>,
    ) -> Result<AsyncTask<EngineOp<Vec<u64>, Float64Array>>> {
        let inputs: std::result::Result<Vec<EdgeInput>, _> =
            edges.into_iter().map(|e| e.try_into()).collect();
        let inputs = inputs?;
        Ok(AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| eng.batch_upsert_edges(&inputs),
            |ids| ids_to_float64_array(&ids),
        )))
    }

    #[napi(ts_return_type = "Promise<Float64Array>")]
    pub fn batch_upsert_nodes_binary_async(
        &self,
        buffer: Buffer,
    ) -> Result<AsyncTask<EngineOp<Vec<u64>, Float64Array>>> {
        let inputs = decode_node_batch(&buffer)?;
        Ok(AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| eng.batch_upsert_nodes(&inputs),
            |ids| ids_to_float64_array(&ids),
        )))
    }

    #[napi(ts_return_type = "Promise<Float64Array>")]
    pub fn batch_upsert_edges_binary_async(
        &self,
        buffer: Buffer,
    ) -> Result<AsyncTask<EngineOp<Vec<u64>, Float64Array>>> {
        let inputs = decode_edge_batch(&buffer)?;
        Ok(AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| eng.batch_upsert_edges(&inputs),
            |ids| ids_to_float64_array(&ids),
        )))
    }

    #[napi(ts_return_type = "Promise<JsNodeRecord | null>")]
    pub fn get_node_async(
        &self,
        id: f64,
    ) -> Result<AsyncTask<EngineReadOp<Option<NodeRecord>, Option<JsNodeRecord>>>> {
        let id = f64_to_u64(id)?;
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.get_node(id),
            |n| n.map(JsNodeRecord::try_from).transpose(),
        )))
    }

    #[napi(ts_return_type = "Promise<JsEdgeRecord | null>")]
    pub fn get_edge_async(
        &self,
        id: f64,
    ) -> Result<AsyncTask<EngineReadOp<Option<EdgeRecord>, Option<JsEdgeRecord>>>> {
        let id = f64_to_u64(id)?;
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.get_edge(id),
            |e| e.map(JsEdgeRecord::try_from).transpose(),
        )))
    }

    #[napi(ts_return_type = "Promise<JsNodeRecord | null>")]
    pub fn get_node_by_key_async(
        &self,
        type_id: u32,
        key: String,
    ) -> AsyncTask<EngineReadOp<Option<NodeRecord>, Option<JsNodeRecord>>> {
        AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.get_node_by_key(type_id, &key),
            |n| n.map(JsNodeRecord::try_from).transpose(),
        ))
    }

    #[napi(ts_return_type = "Promise<JsEdgeRecord | null>")]
    pub fn get_edge_by_triple_async(
        &self,
        from: f64,
        to: f64,
        type_id: u32,
    ) -> Result<AsyncTask<EngineReadOp<Option<EdgeRecord>, Option<JsEdgeRecord>>>> {
        let from = f64_to_u64(from)?;
        let to = f64_to_u64(to)?;
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.get_edge_by_triple(from, to, type_id),
            |e| e.map(JsEdgeRecord::try_from).transpose(),
        )))
    }

    #[napi(ts_return_type = "Promise<Array<JsNodeRecord | null>>")]
    pub fn get_nodes_async(
        &self,
        ids: Vec<f64>,
    ) -> Result<AsyncTask<EngineReadOp<Vec<Option<NodeRecord>>, Vec<Option<JsNodeRecord>>>>> {
        let ids: Vec<u64> = ids
            .into_iter()
            .map(f64_to_u64)
            .collect::<Result<Vec<_>>>()?;
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.get_nodes(&ids),
            |results| {
                results
                    .into_iter()
                    .map(|r| r.map(JsNodeRecord::try_from).transpose())
                    .collect::<Result<Vec<_>>>()
            },
        )))
    }

    #[napi(ts_return_type = "Promise<Array<JsNodeRecord | null>>")]
    pub fn get_nodes_by_keys_async(
        &self,
        keys: Vec<JsKeyQuery>,
    ) -> Result<AsyncTask<EngineReadOp<Vec<Option<NodeRecord>>, Vec<Option<JsNodeRecord>>>>> {
        let owned: Vec<(u32, String)> = keys.into_iter().map(|k| (k.type_id, k.key)).collect();
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| {
                let refs: Vec<(u32, &str)> = owned.iter().map(|(t, k)| (*t, k.as_str())).collect();
                eng.get_nodes_by_keys(&refs)
            },
            |results| {
                results
                    .into_iter()
                    .map(|r| r.map(JsNodeRecord::try_from).transpose())
                    .collect::<Result<Vec<_>>>()
            },
        )))
    }

    #[napi(ts_return_type = "Promise<Array<JsEdgeRecord | null>>")]
    pub fn get_edges_async(
        &self,
        ids: Vec<f64>,
    ) -> Result<AsyncTask<EngineReadOp<Vec<Option<EdgeRecord>>, Vec<Option<JsEdgeRecord>>>>> {
        let ids: Vec<u64> = ids
            .into_iter()
            .map(f64_to_u64)
            .collect::<Result<Vec<_>>>()?;
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.get_edges(&ids),
            |results| {
                results
                    .into_iter()
                    .map(|r| r.map(JsEdgeRecord::try_from).transpose())
                    .collect::<Result<Vec<_>>>()
            },
        )))
    }

    #[napi(ts_return_type = "Promise<void>")]
    pub fn delete_node_async(&self, id: f64) -> Result<AsyncTask<EngineOp<(), ()>>> {
        let id = f64_to_u64(id)?;
        Ok(AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| eng.delete_node(id),
            |_| Ok(()),
        )))
    }

    #[napi(ts_return_type = "Promise<void>")]
    pub fn delete_edge_async(&self, id: f64) -> Result<AsyncTask<EngineOp<(), ()>>> {
        let id = f64_to_u64(id)?;
        Ok(AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| eng.delete_edge(id),
            |_| Ok(()),
        )))
    }

    #[napi(ts_return_type = "Promise<JsEdgeRecord | null>")]
    pub fn invalidate_edge_async(
        &self,
        id: f64,
        valid_to: i64,
    ) -> Result<AsyncTask<EngineOp<Option<EdgeRecord>, Option<JsEdgeRecord>>>> {
        let id = f64_to_u64(id)?;
        Ok(AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| eng.invalidate_edge(id, valid_to),
            |e| e.map(JsEdgeRecord::try_from).transpose(),
        )))
    }

    #[napi(ts_return_type = "Promise<JsPatchResult>")]
    pub fn graph_patch_async(
        &self,
        patch: JsGraphPatch,
    ) -> Result<AsyncTask<EngineOp<overgraph::PatchResult, JsPatchResult>>> {
        let rust_patch = js_patch_to_rust(patch)?;
        Ok(AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| eng.graph_patch(&rust_patch),
            |result| {
                Ok(JsPatchResult {
                    node_ids: ids_to_float64_array(&result.node_ids)?,
                    edge_ids: ids_to_float64_array(&result.edge_ids)?,
                })
            },
        )))
    }

    #[napi(ts_return_type = "Promise<JsPruneResult>")]
    pub fn prune_async(
        &self,
        policy: JsPrunePolicy,
    ) -> Result<AsyncTask<EngineOp<PruneResult, JsPruneResult>>> {
        let rust_policy = PrunePolicy {
            max_age_ms: policy.max_age_ms.map(|v| v as i64),
            max_weight: policy.max_weight.map(|v| v as f32),
            type_id: policy.type_id,
        };
        Ok(AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| eng.prune(&rust_policy),
            |result| {
                Ok(JsPruneResult {
                    nodes_pruned: result.nodes_pruned as i64,
                    edges_pruned: result.edges_pruned as i64,
                })
            },
        )))
    }

    #[napi(ts_return_type = "Promise<Array<JsNeighborEntry>>")]
    pub fn neighbors_async(
        &self,
        node_id: f64,
        options: Option<JsNeighborsOptions>,
    ) -> Result<AsyncTask<EngineReadOp<Vec<NeighborEntry>, Vec<JsNeighborEntry>>>> {
        let node_id = f64_to_u64(node_id)?;
        let (direction, type_filter, limit, at_epoch, decay_lambda) = match options {
            Some(o) => (
                o.direction,
                o.type_filter,
                o.limit,
                o.at_epoch,
                o.decay_lambda,
            ),
            None => (None, None, None, None, None),
        };
        let dir = parse_direction(direction.as_deref())?;
        let lim = limit.map(|v| v as usize);
        let decay = decay_lambda.map(|v| v as f32);
        let opts = NeighborOptions {
            direction: dir,
            type_filter,
            limit: lim,
            at_epoch,
            decay_lambda: decay,
        };
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.neighbors(node_id, &opts),
            neighbor_entries_to_js,
        )))
    }

    #[napi(ts_return_type = "Promise<JsTraversalPageResult>")]
    pub fn traverse_async(
        &self,
        start_node_id: f64,
        max_depth: u32,
        options: Option<JsTraverseOptions>,
    ) -> Result<AsyncTask<EngineReadOp<TraversalPageResult, JsTraversalPageResult>>> {
        let start_node_id = f64_to_u64(start_node_id)?;
        let (
            direction,
            min_depth,
            edge_type_filter,
            node_type_filter,
            at_epoch,
            decay_lambda,
            limit,
            cursor,
        ) = match options {
            Some(o) => (
                o.direction,
                o.min_depth,
                o.edge_type_filter,
                o.node_type_filter,
                o.at_epoch,
                o.decay_lambda,
                o.limit,
                o.cursor,
            ),
            None => (None, None, None, None, None, None, None, None),
        };
        let dir = parse_direction(direction.as_deref())?;
        let min_depth = min_depth.unwrap_or(1);
        let cursor = cursor.map(js_traversal_cursor_to_rust).transpose()?;
        let opts = TraverseOptions {
            min_depth,
            direction: dir,
            edge_type_filter,
            node_type_filter,
            at_epoch,
            decay_lambda,
            limit: limit.map(|v| v as usize),
            cursor,
        };
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.traverse(start_node_id, max_depth, &opts),
            traversal_page_to_js,
        )))
    }

    #[napi(ts_return_type = "Promise<Array<JsNeighborEntry>>")]
    pub fn top_k_neighbors_async(
        &self,
        node_id: f64,
        k: u32,
        options: Option<JsTopKNeighborsOptions>,
    ) -> Result<AsyncTask<EngineReadOp<Vec<NeighborEntry>, Vec<JsNeighborEntry>>>> {
        let node_id = f64_to_u64(node_id)?;
        let (direction, type_filter, scoring, decay_lambda, at_epoch) = match options {
            Some(o) => (
                o.direction,
                o.type_filter,
                o.scoring,
                o.decay_lambda,
                o.at_epoch,
            ),
            None => (None, None, None, None, None),
        };
        let dir = parse_direction(direction.as_deref())?;
        let scoring_mode = parse_scoring_mode(scoring.as_deref(), decay_lambda)?;
        let opts = TopKOptions {
            direction: dir,
            type_filter,
            scoring: scoring_mode,
            at_epoch,
        };
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.top_k_neighbors(node_id, k as usize, &opts),
            neighbor_entries_to_js,
        )))
    }

    #[napi(ts_return_type = "Promise<JsSubgraphResult>")]
    pub fn extract_subgraph_async(
        &self,
        start_node_id: f64,
        max_depth: u32,
        options: Option<JsExtractSubgraphOptions>,
    ) -> Result<AsyncTask<EngineReadOp<Subgraph, JsSubgraphResult>>> {
        let start = f64_to_u64(start_node_id)?;
        let (direction, edge_type_filter, at_epoch) = match options {
            Some(o) => (o.direction, o.edge_type_filter, o.at_epoch),
            None => (None, None, None),
        };
        let dir = parse_direction(direction.as_deref())?;
        let opts = SubgraphOptions {
            direction: dir,
            edge_type_filter,
            at_epoch,
        };
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.extract_subgraph(start, max_depth, &opts),
            subgraph_to_js,
        )))
    }

    #[napi(ts_return_type = "Promise<Float64Array>")]
    pub fn find_nodes_async(
        &self,
        type_id: u32,
        prop_key: String,
        prop_value: serde_json::Value,
    ) -> AsyncTask<EngineReadOp<Vec<u64>, Float64Array>> {
        let pv = json_to_prop_value(&prop_value);
        AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.find_nodes(type_id, &prop_key, &pv),
            |ids| ids_to_float64_array(&ids),
        ))
    }

    #[napi(
        ts_args_type = "request: import('./query-types').QueryNodeRequest",
        ts_return_type = "Promise<JsIdPageResult>"
    )]
    pub fn query_node_ids_async(
        &self,
        request: serde_json::Value,
    ) -> Result<AsyncTask<EngineReadOp<QueryNodeIdsResult, JsIdPageResult>>> {
        let query = parse_js_node_query(&request)?;
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.query_node_ids(&query),
            query_node_ids_to_js,
        )))
    }

    #[napi(
        ts_args_type = "request: import('./query-types').QueryNodeRequest",
        ts_return_type = "Promise<JsNodePageResult>"
    )]
    pub fn query_nodes_async(
        &self,
        request: serde_json::Value,
    ) -> Result<AsyncTask<EngineReadOp<QueryNodesResult, JsNodePageResult>>> {
        let query = parse_js_node_query(&request)?;
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.query_nodes(&query),
            query_nodes_to_js,
        )))
    }

    #[napi(
        ts_args_type = "request: import('./query-types').GraphPatternRequest",
        ts_return_type = "Promise<import('./query-types').QueryPatternResult>"
    )]
    pub fn query_pattern_async(
        &self,
        request: serde_json::Value,
    ) -> Result<AsyncTask<EngineReadOp<QueryPatternResult, JsJsonValue>>> {
        let query = parse_js_graph_pattern_query(&request)?;
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.query_pattern(&query),
            query_pattern_result_to_js,
        )))
    }

    #[napi(
        ts_args_type = "request: import('./query-types').QueryNodeRequest",
        ts_return_type = "Promise<import('./query-types').QueryPlan>"
    )]
    pub fn explain_node_query_async(
        &self,
        request: serde_json::Value,
    ) -> Result<AsyncTask<EngineReadOp<QueryPlan, JsJsonValue>>> {
        let query = parse_js_node_query(&request)?;
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.explain_node_query(&query),
            query_plan_to_js,
        )))
    }

    #[napi(
        ts_args_type = "request: import('./query-types').GraphPatternRequest",
        ts_return_type = "Promise<import('./query-types').QueryPlan>"
    )]
    pub fn explain_pattern_query_async(
        &self,
        request: serde_json::Value,
    ) -> Result<AsyncTask<EngineReadOp<QueryPlan, JsJsonValue>>> {
        let query = parse_js_graph_pattern_query(&request)?;
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.explain_pattern_query(&query),
            query_plan_to_js,
        )))
    }

    #[napi(ts_return_type = "Promise<JsNodePropertyIndexInfo>")]
    pub fn ensure_node_property_index_async(
        &self,
        type_id: u32,
        prop_key: String,
        kind: JsSecondaryIndexKind,
    ) -> Result<AsyncTask<EngineOp<NodePropertyIndexInfo, JsNodePropertyIndexInfo>>> {
        let kind = js_secondary_index_kind_to_rust(kind)?;
        Ok(AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| eng.ensure_node_property_index(type_id, &prop_key, kind.clone()),
            node_property_index_info_to_js,
        )))
    }

    #[napi(ts_return_type = "Promise<boolean>")]
    pub fn drop_node_property_index_async(
        &self,
        type_id: u32,
        prop_key: String,
        kind: JsSecondaryIndexKind,
    ) -> Result<AsyncTask<EngineOp<bool, bool>>> {
        let kind = js_secondary_index_kind_to_rust(kind)?;
        Ok(AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| eng.drop_node_property_index(type_id, &prop_key, kind.clone()),
            Ok,
        )))
    }

    #[napi(ts_return_type = "Promise<Array<JsNodePropertyIndexInfo>>")]
    pub fn list_node_property_indexes_async(
        &self,
    ) -> AsyncTask<EngineReadOp<Vec<NodePropertyIndexInfo>, Vec<JsNodePropertyIndexInfo>>> {
        AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            |eng| eng.list_node_property_indexes(),
            node_property_index_infos_to_js,
        ))
    }

    #[napi(ts_return_type = "Promise<Float64Array>")]
    pub fn find_nodes_range_async(
        &self,
        type_id: u32,
        prop_key: String,
        lower: Option<JsPropertyRangeBound>,
        upper: Option<JsPropertyRangeBound>,
    ) -> Result<AsyncTask<EngineReadOp<Vec<u64>, Float64Array>>> {
        let lower = lower
            .as_ref()
            .map(js_property_range_bound_to_rust)
            .transpose()?;
        let upper = upper
            .as_ref()
            .map(js_property_range_bound_to_rust)
            .transpose()?;
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.find_nodes_range(type_id, &prop_key, lower.as_ref(), upper.as_ref()),
            |ids| ids_to_float64_array(&ids),
        )))
    }

    #[napi(ts_return_type = "Promise<JsPropertyRangePageResult>")]
    pub fn find_nodes_range_paged_async(
        &self,
        type_id: u32,
        prop_key: String,
        lower: Option<JsPropertyRangeBound>,
        upper: Option<JsPropertyRangeBound>,
        options: Option<JsFindNodesRangePagedOptions>,
    ) -> Result<AsyncTask<EngineReadOp<PropertyRangePageResult<u64>, JsPropertyRangePageResult>>>
    {
        let lower = lower
            .as_ref()
            .map(js_property_range_bound_to_rust)
            .transpose()?;
        let upper = upper
            .as_ref()
            .map(js_property_range_bound_to_rust)
            .transpose()?;
        let page = make_property_range_page_request(options)?;
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| {
                eng.find_nodes_range_paged(
                    type_id,
                    &prop_key,
                    lower.as_ref(),
                    upper.as_ref(),
                    &page,
                )
            },
            property_range_page_to_js,
        )))
    }

    #[napi(ts_return_type = "Promise<Array<JsNodeRecord>>")]
    pub fn get_nodes_by_type_async(
        &self,
        type_id: u32,
    ) -> AsyncTask<EngineReadOp<Vec<NodeRecord>, Vec<JsNodeRecord>>> {
        AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.get_nodes_by_type(type_id),
            |records| {
                records
                    .into_iter()
                    .map(JsNodeRecord::try_from)
                    .collect::<Result<Vec<_>>>()
            },
        ))
    }

    #[napi(ts_return_type = "Promise<Array<JsEdgeRecord>>")]
    pub fn get_edges_by_type_async(
        &self,
        type_id: u32,
    ) -> AsyncTask<EngineReadOp<Vec<EdgeRecord>, Vec<JsEdgeRecord>>> {
        AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.get_edges_by_type(type_id),
            |records| {
                records
                    .into_iter()
                    .map(JsEdgeRecord::try_from)
                    .collect::<Result<Vec<_>>>()
            },
        ))
    }

    #[napi(ts_return_type = "Promise<number>")]
    pub fn count_nodes_by_type_async(&self, type_id: u32) -> AsyncTask<EngineReadOp<u64, i64>> {
        AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.count_nodes_by_type(type_id),
            |count| Ok(count as i64),
        ))
    }

    #[napi(ts_return_type = "Promise<number>")]
    pub fn count_edges_by_type_async(&self, type_id: u32) -> AsyncTask<EngineReadOp<u64, i64>> {
        AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.count_edges_by_type(type_id),
            |count| Ok(count as i64),
        ))
    }

    #[napi(ts_return_type = "Promise<Float64Array>")]
    pub fn nodes_by_type_async(
        &self,
        type_id: u32,
    ) -> AsyncTask<EngineReadOp<Vec<u64>, Float64Array>> {
        AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.nodes_by_type(type_id),
            |ids| ids_to_float64_array(&ids),
        ))
    }

    #[napi(ts_return_type = "Promise<Float64Array>")]
    pub fn edges_by_type_async(
        &self,
        type_id: u32,
    ) -> AsyncTask<EngineReadOp<Vec<u64>, Float64Array>> {
        AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.edges_by_type(type_id),
            |ids| ids_to_float64_array(&ids),
        ))
    }

    #[napi(ts_return_type = "Promise<Array<JsNeighborBatchEntry>>")]
    pub fn neighbors_batch_async(
        &self,
        node_ids: Vec<f64>,
        options: Option<JsNeighborsBatchOptions>,
    ) -> Result<AsyncTask<EngineReadOp<NodeIdMap<Vec<NeighborEntry>>, Vec<JsNeighborBatchEntry>>>>
    {
        let ids: Vec<u64> = node_ids
            .into_iter()
            .map(f64_to_u64)
            .collect::<Result<Vec<_>>>()?;
        let (direction, type_filter, at_epoch, decay_lambda) = match options {
            Some(o) => (o.direction, o.type_filter, o.at_epoch, o.decay_lambda),
            None => (None, None, None, None),
        };
        let dir = parse_direction(direction.as_deref())?;
        let decay = decay_lambda.map(|v| v as f32);
        let opts = NeighborOptions {
            direction: dir,
            type_filter,
            limit: None,
            at_epoch,
            decay_lambda: decay,
        };
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.neighbors_batch(&ids, &opts),
            convert_batch_result,
        )))
    }

    // --- Degree counts + aggregations (async, Phase 18a) ---

    #[napi(ts_return_type = "Promise<number>")]
    pub fn degree_async(
        &self,
        node_id: f64,
        options: Option<JsDegreeOptions>,
    ) -> Result<AsyncTask<EngineReadOp<u64, i64>>> {
        let node_id = f64_to_u64(node_id)?;
        let (direction, type_filter, at_epoch) = match options {
            Some(o) => (o.direction, o.type_filter, o.at_epoch),
            None => (None, None, None),
        };
        let dir = parse_direction(direction.as_deref())?;
        let opts = DegreeOptions {
            direction: dir,
            type_filter,
            at_epoch,
        };
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.degree(node_id, &opts),
            u64_to_safe_i64,
        )))
    }

    #[napi(ts_return_type = "Promise<number>")]
    pub fn sum_edge_weights_async(
        &self,
        node_id: f64,
        options: Option<JsSumEdgeWeightsOptions>,
    ) -> Result<AsyncTask<EngineReadOp<f64, f64>>> {
        let node_id = f64_to_u64(node_id)?;
        let (direction, type_filter, at_epoch) = match options {
            Some(o) => (o.direction, o.type_filter, o.at_epoch),
            None => (None, None, None),
        };
        let dir = parse_direction(direction.as_deref())?;
        let opts = DegreeOptions {
            direction: dir,
            type_filter,
            at_epoch,
        };
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.sum_edge_weights(node_id, &opts),
            Ok,
        )))
    }

    #[napi(ts_return_type = "Promise<number | null>")]
    pub fn avg_edge_weight_async(
        &self,
        node_id: f64,
        options: Option<JsAvgEdgeWeightOptions>,
    ) -> Result<AsyncTask<EngineReadOp<Option<f64>, Option<f64>>>> {
        let node_id = f64_to_u64(node_id)?;
        let (direction, type_filter, at_epoch) = match options {
            Some(o) => (o.direction, o.type_filter, o.at_epoch),
            None => (None, None, None),
        };
        let dir = parse_direction(direction.as_deref())?;
        let opts = DegreeOptions {
            direction: dir,
            type_filter,
            at_epoch,
        };
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.avg_edge_weight(node_id, &opts),
            Ok,
        )))
    }

    #[napi(ts_return_type = "Promise<Array<JsDegreeBatchEntry>>")]
    pub fn degrees_async(
        &self,
        node_ids: Vec<f64>,
        options: Option<JsDegreesOptions>,
    ) -> Result<AsyncTask<EngineReadOp<NodeIdMap<u64>, Vec<JsDegreeBatchEntry>>>> {
        let ids: Vec<u64> = node_ids
            .into_iter()
            .map(f64_to_u64)
            .collect::<Result<Vec<_>>>()?;
        let (direction, type_filter, at_epoch) = match options {
            Some(o) => (o.direction, o.type_filter, o.at_epoch),
            None => (None, None, None),
        };
        let dir = parse_direction(direction.as_deref())?;
        let opts = DegreeOptions {
            direction: dir,
            type_filter,
            at_epoch,
        };
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.degrees(&ids, &opts),
            |map| {
                let mut entries: Vec<JsDegreeBatchEntry> = map
                    .into_iter()
                    .map(|(node_id, degree)| {
                        Ok(JsDegreeBatchEntry {
                            node_id: u64_to_f64(node_id)?,
                            degree: u64_to_safe_i64(degree)?,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                entries.sort_by(|a, b| a.node_id.total_cmp(&b.node_id));
                Ok(entries)
            },
        )))
    }

    // --- Shortest path (async, Phase 18b) ---

    #[napi(ts_return_type = "Promise<JsShortestPath | null>")]
    pub fn shortest_path_async(
        &self,
        from: f64,
        to: f64,
        options: Option<JsShortestPathOptions>,
    ) -> Result<AsyncTask<EngineReadOp<Option<ShortestPath>, Option<JsShortestPath>>>> {
        let from = f64_to_u64(from)?;
        let to = f64_to_u64(to)?;
        let (direction, type_filter, weight_field, at_epoch, max_depth, max_cost) = match options {
            Some(o) => (
                o.direction,
                o.type_filter,
                o.weight_field,
                o.at_epoch,
                o.max_depth,
                o.max_cost,
            ),
            None => (None, None, None, None, None, None),
        };
        let dir = parse_direction(direction.as_deref())?;
        let opts = ShortestPathOptions {
            direction: dir,
            type_filter,
            weight_field,
            at_epoch,
            max_depth,
            max_cost,
        };
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.shortest_path(from, to, &opts),
            |opt| opt.map(shortest_path_to_js).transpose(),
        )))
    }

    #[napi(ts_return_type = "Promise<boolean>")]
    pub fn is_connected_async(
        &self,
        from: f64,
        to: f64,
        options: Option<JsIsConnectedOptions>,
    ) -> Result<AsyncTask<EngineReadOp<bool, bool>>> {
        let from = f64_to_u64(from)?;
        let to = f64_to_u64(to)?;
        let (direction, type_filter, at_epoch, max_depth) = match options {
            Some(o) => (o.direction, o.type_filter, o.at_epoch, o.max_depth),
            None => (None, None, None, None),
        };
        let dir = parse_direction(direction.as_deref())?;
        let opts = IsConnectedOptions {
            direction: dir,
            type_filter,
            at_epoch,
            max_depth,
        };
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.is_connected(from, to, &opts),
            Ok,
        )))
    }

    #[napi(ts_return_type = "Promise<Array<JsShortestPath>>")]
    pub fn all_shortest_paths_async(
        &self,
        from: f64,
        to: f64,
        options: Option<JsAllShortestPathsOptions>,
    ) -> Result<AsyncTask<EngineReadOp<Vec<ShortestPath>, Vec<JsShortestPath>>>> {
        let from = f64_to_u64(from)?;
        let to = f64_to_u64(to)?;
        let (direction, type_filter, weight_field, at_epoch, max_depth, max_cost, max_paths) =
            match options {
                Some(o) => (
                    o.direction,
                    o.type_filter,
                    o.weight_field,
                    o.at_epoch,
                    o.max_depth,
                    o.max_cost,
                    o.max_paths,
                ),
                None => (None, None, None, None, None, None, None),
            };
        let dir = parse_direction(direction.as_deref())?;
        let opts = AllShortestPathsOptions {
            direction: dir,
            type_filter,
            weight_field,
            at_epoch,
            max_depth,
            max_cost,
            max_paths: max_paths.map(|n| n as usize),
        };
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.all_shortest_paths(from, to, &opts),
            |paths| paths.into_iter().map(shortest_path_to_js).collect(),
        )))
    }

    // --- Paginated queries (async) ---

    #[napi(ts_return_type = "Promise<JsIdPageResult>")]
    pub fn nodes_by_type_paged_async(
        &self,
        type_id: u32,
        limit: Option<u32>,
        after: Option<f64>,
    ) -> Result<AsyncTask<EngineReadOp<PageResult<u64>, JsIdPageResult>>> {
        let page = make_page_request(limit, after)?;
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.nodes_by_type_paged(type_id, &page),
            id_page_to_js,
        )))
    }

    #[napi(ts_return_type = "Promise<JsIdPageResult>")]
    pub fn edges_by_type_paged_async(
        &self,
        type_id: u32,
        limit: Option<u32>,
        after: Option<f64>,
    ) -> Result<AsyncTask<EngineReadOp<PageResult<u64>, JsIdPageResult>>> {
        let page = make_page_request(limit, after)?;
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.edges_by_type_paged(type_id, &page),
            id_page_to_js,
        )))
    }

    #[napi(ts_return_type = "Promise<JsNodePageResult>")]
    pub fn get_nodes_by_type_paged_async(
        &self,
        type_id: u32,
        limit: Option<u32>,
        after: Option<f64>,
    ) -> Result<AsyncTask<EngineReadOp<PageResult<NodeRecord>, JsNodePageResult>>> {
        let page = make_page_request(limit, after)?;
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.get_nodes_by_type_paged(type_id, &page),
            node_page_to_js,
        )))
    }

    #[napi(ts_return_type = "Promise<JsEdgePageResult>")]
    pub fn get_edges_by_type_paged_async(
        &self,
        type_id: u32,
        limit: Option<u32>,
        after: Option<f64>,
    ) -> Result<AsyncTask<EngineReadOp<PageResult<EdgeRecord>, JsEdgePageResult>>> {
        let page = make_page_request(limit, after)?;
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.get_edges_by_type_paged(type_id, &page),
            edge_page_to_js,
        )))
    }

    #[napi(ts_return_type = "Promise<JsIdPageResult>")]
    pub fn find_nodes_paged_async(
        &self,
        type_id: u32,
        prop_key: String,
        prop_value: serde_json::Value,
        options: Option<JsFindNodesPagedOptions>,
    ) -> Result<AsyncTask<EngineReadOp<PageResult<u64>, JsIdPageResult>>> {
        let pv = json_to_prop_value(&prop_value);
        let (limit, after) = match options {
            Some(o) => (o.limit, o.after),
            None => (None, None),
        };
        let page = make_page_request(limit, after)?;
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.find_nodes_paged(type_id, &prop_key, &pv, &page),
            id_page_to_js,
        )))
    }

    #[napi(ts_return_type = "Promise<Float64Array>")]
    pub fn find_nodes_by_time_range_async(
        &self,
        type_id: u32,
        from_ms: i64,
        to_ms: i64,
    ) -> Result<AsyncTask<EngineReadOp<Vec<u64>, Float64Array>>> {
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.find_nodes_by_time_range(type_id, from_ms, to_ms),
            |ids| ids_to_float64_array(&ids),
        )))
    }

    #[napi(ts_return_type = "Promise<JsIdPageResult>")]
    pub fn find_nodes_by_time_range_paged_async(
        &self,
        type_id: u32,
        from_ms: i64,
        to_ms: i64,
        options: Option<JsFindNodesByTimeRangePagedOptions>,
    ) -> Result<AsyncTask<EngineReadOp<PageResult<u64>, JsIdPageResult>>> {
        let (limit, after) = match options {
            Some(o) => (o.limit, o.after),
            None => (None, None),
        };
        let page = make_page_request(limit, after)?;
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.find_nodes_by_time_range_paged(type_id, from_ms, to_ms, &page),
            id_page_to_js,
        )))
    }

    #[napi(ts_return_type = "Promise<JsPprResult>")]
    pub fn personalized_pagerank_async(
        &self,
        seed_node_ids: Vec<f64>,
        options: Option<JsPersonalizedPagerankOptions>,
    ) -> Result<AsyncTask<EngineReadOp<PprResult, JsPprResult>>> {
        let seeds: Vec<u64> = seed_node_ids
            .into_iter()
            .map(f64_to_u64)
            .collect::<Result<Vec<_>>>()?;
        let (
            algorithm,
            damping_factor,
            max_iterations,
            epsilon,
            approx_residual_tolerance,
            edge_type_filter,
            max_results,
        ) = match &options {
            Some(o) => (
                o.algorithm.as_deref(),
                o.damping_factor,
                o.max_iterations,
                o.epsilon,
                o.approx_residual_tolerance,
                o.edge_type_filter.clone(),
                o.max_results,
            ),
            None => (None, None, None, None, None, None, None),
        };
        let opts = js_ppr_options_to_ppr_options(
            algorithm,
            &damping_factor,
            &max_iterations,
            &epsilon,
            &approx_residual_tolerance,
            &edge_type_filter,
            &max_results,
        )?;
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.personalized_pagerank(&seeds, &opts),
            ppr_result_to_js,
        )))
    }

    #[napi(ts_return_type = "Promise<JsAdjacencyExport>")]
    pub fn export_adjacency_async(
        &self,
        options: Option<JsExportOptions>,
    ) -> Result<AsyncTask<EngineReadOp<(AdjacencyExport, bool), JsAdjacencyExport>>> {
        let include_weights = options
            .as_ref()
            .and_then(|o| o.include_weights)
            .unwrap_or(true);
        let opts = js_export_options_to_rust(options);
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| Ok((eng.export_adjacency(&opts)?, include_weights)),
            |pair| adjacency_export_to_js(pair.0, pair.1),
        )))
    }

    #[napi(ts_return_type = "Promise<JsNeighborPageResult>")]
    pub fn neighbors_paged_async(
        &self,
        node_id: f64,
        options: Option<JsNeighborsPagedOptions>,
    ) -> Result<AsyncTask<EngineReadOp<PageResult<NeighborEntry>, JsNeighborPageResult>>> {
        let node_id = f64_to_u64(node_id)?;
        let (direction, type_filter, limit, after, at_epoch, decay_lambda) = match options {
            Some(o) => (
                o.direction,
                o.type_filter,
                o.limit,
                o.after,
                o.at_epoch,
                o.decay_lambda,
            ),
            None => (None, None, None, None, None, None),
        };
        let dir = parse_direction(direction.as_deref())?;
        let page = make_page_request(limit, after)?;
        let decay = decay_lambda.map(|v| v as f32);
        let opts = NeighborOptions {
            direction: dir,
            type_filter,
            limit: None,
            at_epoch,
            decay_lambda: decay,
        };
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.neighbors_paged(node_id, &opts, &page),
            neighbor_page_to_js,
        )))
    }

    // --- Connected Components (async, Phase 18d) ---

    #[napi(ts_return_type = "Promise<Array<JsComponentEntry>>")]
    pub fn connected_components_async(
        &self,
        options: Option<JsConnectedComponentsOptions>,
    ) -> Result<AsyncTask<EngineReadOp<NodeIdMap<u64>, Vec<JsComponentEntry>>>> {
        let (edge_type_filter, node_type_filter, at_epoch) = match options {
            Some(o) => (o.edge_type_filter, o.node_type_filter, o.at_epoch),
            None => (None, None, None),
        };
        let opts = ComponentOptions {
            edge_type_filter,
            node_type_filter,
            at_epoch,
        };
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.connected_components(&opts),
            |map| {
                let mut entries: Vec<JsComponentEntry> = map
                    .into_iter()
                    .map(|(node_id, component_id)| {
                        Ok(JsComponentEntry {
                            node_id: u64_to_f64(node_id)?,
                            component_id: u64_to_f64(component_id)?,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                entries.sort_by(|a, b| a.node_id.total_cmp(&b.node_id));
                Ok(entries)
            },
        )))
    }

    #[napi(ts_return_type = "Promise<Float64Array>")]
    pub fn component_of_async(
        &self,
        node_id: f64,
        options: Option<JsComponentOfOptions>,
    ) -> Result<AsyncTask<EngineReadOp<Vec<u64>, Float64Array>>> {
        let node_id = f64_to_u64(node_id)?;
        let (edge_type_filter, node_type_filter, at_epoch) = match options {
            Some(o) => (o.edge_type_filter, o.node_type_filter, o.at_epoch),
            None => (None, None, None),
        };
        let opts = ComponentOptions {
            edge_type_filter,
            node_type_filter,
            at_epoch,
        };
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.component_of(node_id, &opts),
            |members| ids_to_float64_array(&members),
        )))
    }

    #[napi(ts_return_type = "Promise<Array<JsVectorHit>>")]
    pub fn vector_search_async(
        &self,
        mode: String,
        options: JsVectorSearchOptions,
    ) -> Result<AsyncTask<EngineReadOp<Vec<VectorHit>, Vec<JsVectorHit>>>> {
        let mode = parse_vector_search_mode(&mode)?;
        let k = options.k;
        let dense_query = options.dense_query;
        let sparse_query = options.sparse_query;
        let type_filter = options.type_filter;
        let ef_search = options.ef_search;
        let scope = options.scope;
        let dense_weight = options.dense_weight;
        let sparse_weight = options.sparse_weight;
        let fusion_mode = options.fusion_mode;
        let fusion = parse_fusion_mode(fusion_mode.as_deref())?;
        let dense_q = dense_query.map(|v| v.into_iter().map(|x| x as f32).collect());
        let sparse_q = sparse_query.map(|v| {
            v.into_iter()
                .map(|e| (e.dimension, e.value as f32))
                .collect()
        });
        let scope = match scope {
            None => None,
            Some(s) => Some(VectorSearchScope {
                start_node_id: f64_to_u64(s.start_node_id)?,
                max_depth: s.max_depth,
                direction: parse_direction(s.direction.as_deref())?,
                edge_type_filter: s.edge_type_filter,
                at_epoch: s.at_epoch,
            }),
        };
        let request = VectorSearchRequest {
            mode,
            dense_query: dense_q,
            sparse_query: sparse_q,
            k: k as usize,
            type_filter,
            ef_search: ef_search.map(|v| v as usize),
            scope,
            dense_weight: dense_weight.map(|v| v as f32),
            sparse_weight: sparse_weight.map(|v| v as f32),
            fusion_mode: fusion,
        };
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.vector_search(&request),
            |hits| {
                hits.into_iter()
                    .map(|h| {
                        Ok(JsVectorHit {
                            node_id: u64_to_f64(h.node_id)?,
                            score: h.score as f64,
                        })
                    })
                    .collect::<Result<Vec<_>>>()
            },
        )))
    }

    #[napi(ts_return_type = "Promise<void>")]
    pub fn sync_async(&self) -> AsyncTask<EngineOp<(), ()>> {
        AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            |eng| {
                eng.sync()?;
                Ok(())
            },
            |_| Ok(()),
        ))
    }

    #[napi(ts_return_type = "Promise<void>")]
    pub fn flush_async(&self) -> AsyncTask<EngineOp<(), ()>> {
        AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            |eng| {
                eng.flush()?;
                Ok(())
            },
            |_| Ok(()),
        ))
    }

    #[napi(ts_return_type = "Promise<void>")]
    pub fn ingest_mode_async(&self) -> AsyncTask<EngineOp<(), ()>> {
        AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            |eng| eng.ingest_mode(),
            |_| Ok(()),
        ))
    }

    #[napi(ts_return_type = "Promise<JsCompactionStats | null>")]
    pub fn end_ingest_async(
        &self,
    ) -> AsyncTask<EngineOp<Option<CompactionStats>, Option<JsCompactionStats>>> {
        AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            |eng| eng.end_ingest(),
            |s| Ok(s.map(|s| s.into())),
        ))
    }

    #[napi(ts_return_type = "Promise<JsCompactionStats | null>")]
    pub fn compact_async(
        &self,
    ) -> AsyncTask<EngineOp<Option<CompactionStats>, Option<JsCompactionStats>>> {
        AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            |eng| eng.compact(),
            |s| Ok(s.map(|s| s.into())),
        ))
    }

    /// Async compaction with a fire-and-forget progress callback.
    /// The callback receives progress updates but cannot cancel compaction (unlike the sync version).
    /// Note: the database write lock is held for the entire compaction, so other operations on this
    /// instance will block until compaction completes. The JS event loop remains responsive.
    #[napi(
        ts_args_type = "callback: (progress: JsCompactionProgress) => void",
        ts_return_type = "Promise<JsCompactionStats | null>"
    )]
    pub fn compact_with_progress_async(
        &self,
        callback: ProgressTsfn,
    ) -> AsyncTask<CompactProgressOp> {
        AsyncTask::new(CompactProgressOp {
            db: self.inner.clone(),
            tsfn: callback,
        })
    }
}

#[napi(js_name = "WriteTxn")]
pub struct JsWriteTxn {
    inner: Arc<Mutex<Option<WriteTxn>>>,
    async_order: Arc<TxnAsyncOrder>,
}

struct TxnAsyncOrder {
    state: Mutex<TxnAsyncState>,
    cvar: Condvar,
}

struct TxnAsyncState {
    next_ticket: u64,
    serving_ticket: u64,
}

impl TxnAsyncOrder {
    fn new() -> Self {
        Self {
            state: Mutex::new(TxnAsyncState {
                next_ticket: 0,
                serving_ticket: 0,
            }),
            cvar: Condvar::new(),
        }
    }

    fn reserve_ticket(&self) -> Result<u64> {
        let mut state = self
            .state
            .lock()
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        let ticket = state.next_ticket;
        state.next_ticket = state.next_ticket.checked_add(1).ok_or_else(|| {
            napi::Error::from_reason("transaction async queue overflow".to_string())
        })?;
        Ok(ticket)
    }

    fn wait_turn(self: &Arc<Self>, ticket: u64) -> Result<TxnAsyncTurn> {
        let mut state = self
            .state
            .lock()
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        while state.serving_ticket != ticket {
            state = self
                .cvar
                .wait(state)
                .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        }
        Ok(TxnAsyncTurn {
            order: self.clone(),
        })
    }

    fn finish_turn(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.serving_ticket = state.serving_ticket.saturating_add(1);
            self.cvar.notify_all();
        }
    }
}

struct TxnAsyncTurn {
    order: Arc<TxnAsyncOrder>,
}

impl Drop for TxnAsyncTurn {
    fn drop(&mut self) {
        self.order.finish_turn();
    }
}

fn write_txn_to_js(txn: WriteTxn) -> JsWriteTxn {
    JsWriteTxn {
        inner: Arc::new(Mutex::new(Some(txn))),
        async_order: Arc::new(TxnAsyncOrder::new()),
    }
}

#[napi]
impl JsWriteTxn {
    #[napi]
    pub fn upsert_node(
        &self,
        type_id: u32,
        key: String,
        options: Option<JsUpsertNodeOptions>,
    ) -> Result<JsTxnNodeRef> {
        with_txn(&self.inner, |txn| {
            txn.upsert_node(type_id, &key, js_upsert_node_options(options))
        })?;
        Ok(JsTxnNodeRef {
            id: None,
            type_id: Some(type_id),
            key: Some(key),
            local: None,
        })
    }

    #[napi]
    pub fn upsert_node_as(
        &self,
        alias: String,
        type_id: u32,
        key: String,
        options: Option<JsUpsertNodeOptions>,
    ) -> Result<JsTxnNodeRef> {
        let node_ref = with_txn(&self.inner, |txn| {
            txn.upsert_node_as(&alias, type_id, &key, js_upsert_node_options(options))
        })?;
        txn_node_ref_to_js(node_ref)
    }

    #[napi]
    pub fn upsert_edge(
        &self,
        from: JsTxnNodeRef,
        to: JsTxnNodeRef,
        type_id: u32,
        options: Option<JsUpsertEdgeOptions>,
    ) -> Result<JsTxnEdgeRef> {
        let from_rust = js_txn_node_ref_to_rust(from.clone())?;
        let to_rust = js_txn_node_ref_to_rust(to.clone())?;
        with_txn(&self.inner, |txn| {
            txn.upsert_edge(from_rust, to_rust, type_id, js_upsert_edge_options(options))
        })?;
        Ok(JsTxnEdgeRef {
            id: None,
            from: Some(from),
            to: Some(to),
            type_id: Some(type_id),
            local: None,
        })
    }

    #[napi]
    pub fn upsert_edge_as(
        &self,
        alias: String,
        from: JsTxnNodeRef,
        to: JsTxnNodeRef,
        type_id: u32,
        options: Option<JsUpsertEdgeOptions>,
    ) -> Result<JsTxnEdgeRef> {
        let from = js_txn_node_ref_to_rust(from)?;
        let to = js_txn_node_ref_to_rust(to)?;
        let edge_ref = with_txn(&self.inner, |txn| {
            txn.upsert_edge_as(&alias, from, to, type_id, js_upsert_edge_options(options))
        })?;
        txn_edge_ref_to_js(edge_ref)
    }

    #[napi]
    pub fn delete_node(&self, target: JsTxnNodeRef) -> Result<()> {
        let target = js_txn_node_ref_to_rust(target)?;
        with_txn(&self.inner, |txn| txn.delete_node(target))
    }

    #[napi]
    pub fn delete_edge(&self, target: JsTxnEdgeRef) -> Result<()> {
        let target = js_txn_edge_ref_to_rust(target)?;
        with_txn(&self.inner, |txn| txn.delete_edge(target))
    }

    #[napi]
    pub fn invalidate_edge(&self, target: JsTxnEdgeRef, valid_to: i64) -> Result<()> {
        let target = js_txn_edge_ref_to_rust(target)?;
        with_txn(&self.inner, |txn| txn.invalidate_edge(target, valid_to))
    }

    #[napi]
    pub fn stage(&self, operations: Vec<JsTxnOperation>) -> Result<()> {
        let intents = operations
            .into_iter()
            .map(js_txn_operation_to_rust)
            .collect::<Result<Vec<_>>>()?;
        with_txn(&self.inner, |txn| txn.stage_intents(intents))
    }

    #[napi]
    pub fn get_node(&self, target: JsTxnNodeRef) -> Result<Option<JsTxnNodeView>> {
        let target = js_txn_node_ref_to_rust(target)?;
        let view = with_txn_ref(&self.inner, |txn| txn.get_node(target))?;
        view.map(txn_node_view_to_js).transpose()
    }

    #[napi]
    pub fn get_edge(&self, target: JsTxnEdgeRef) -> Result<Option<JsTxnEdgeView>> {
        let target = js_txn_edge_ref_to_rust(target)?;
        let view = with_txn_ref(&self.inner, |txn| txn.get_edge(target))?;
        view.map(txn_edge_view_to_js).transpose()
    }

    #[napi]
    pub fn get_node_by_key(&self, type_id: u32, key: String) -> Result<Option<JsTxnNodeView>> {
        let view = with_txn_ref(&self.inner, |txn| txn.get_node_by_key(type_id, &key))?;
        view.map(txn_node_view_to_js).transpose()
    }

    #[napi]
    pub fn get_edge_by_triple(
        &self,
        from: JsTxnNodeRef,
        to: JsTxnNodeRef,
        type_id: u32,
    ) -> Result<Option<JsTxnEdgeView>> {
        let from = js_txn_node_ref_to_rust(from)?;
        let to = js_txn_node_ref_to_rust(to)?;
        let view = with_txn_ref(&self.inner, |txn| txn.get_edge_by_triple(from, to, type_id))?;
        view.map(txn_edge_view_to_js).transpose()
    }

    #[napi]
    pub fn commit(&self) -> Result<JsTxnCommitResult> {
        let result = with_txn_take(&self.inner, |txn| txn.commit())?;
        txn_commit_result_to_js(result)
    }

    #[napi]
    pub fn rollback(&self) -> Result<()> {
        with_txn_take(&self.inner, |txn| txn.rollback())
    }

    #[napi(ts_return_type = "Promise<JsTxnNodeRef>")]
    pub fn upsert_node_async(
        &self,
        type_id: u32,
        key: String,
        options: Option<JsUpsertNodeOptions>,
    ) -> Result<AsyncTask<TxnAsyncOp<JsTxnNodeRef, JsTxnNodeRef>>> {
        let opts = js_upsert_node_options(options);
        Ok(AsyncTask::new(TxnAsyncOp::new(
            self,
            move |txn| {
                txn.upsert_node(type_id, &key, opts)?;
                Ok(JsTxnNodeRef {
                    id: None,
                    type_id: Some(type_id),
                    key: Some(key),
                    local: None,
                })
            },
            napi_identity,
        )?))
    }

    #[napi(ts_return_type = "Promise<JsTxnNodeRef>")]
    pub fn upsert_node_as_async(
        &self,
        alias: String,
        type_id: u32,
        key: String,
        options: Option<JsUpsertNodeOptions>,
    ) -> Result<AsyncTask<TxnAsyncOp<TxnNodeRef, JsTxnNodeRef>>> {
        let opts = js_upsert_node_options(options);
        Ok(AsyncTask::new(TxnAsyncOp::new(
            self,
            move |txn| txn.upsert_node_as(&alias, type_id, &key, opts),
            txn_node_ref_to_js,
        )?))
    }

    #[napi(ts_return_type = "Promise<JsTxnEdgeRef>")]
    pub fn upsert_edge_async(
        &self,
        from: JsTxnNodeRef,
        to: JsTxnNodeRef,
        type_id: u32,
        options: Option<JsUpsertEdgeOptions>,
    ) -> Result<AsyncTask<TxnAsyncOp<JsTxnEdgeRef, JsTxnEdgeRef>>> {
        let from_rust = js_txn_node_ref_to_rust(from.clone())?;
        let to_rust = js_txn_node_ref_to_rust(to.clone())?;
        let opts = js_upsert_edge_options(options);
        Ok(AsyncTask::new(TxnAsyncOp::new(
            self,
            move |txn| {
                txn.upsert_edge(from_rust, to_rust, type_id, opts)?;
                Ok(JsTxnEdgeRef {
                    id: None,
                    from: Some(from),
                    to: Some(to),
                    type_id: Some(type_id),
                    local: None,
                })
            },
            napi_identity,
        )?))
    }

    #[napi(ts_return_type = "Promise<JsTxnEdgeRef>")]
    pub fn upsert_edge_as_async(
        &self,
        alias: String,
        from: JsTxnNodeRef,
        to: JsTxnNodeRef,
        type_id: u32,
        options: Option<JsUpsertEdgeOptions>,
    ) -> Result<AsyncTask<TxnAsyncOp<TxnEdgeRef, JsTxnEdgeRef>>> {
        let from = js_txn_node_ref_to_rust(from)?;
        let to = js_txn_node_ref_to_rust(to)?;
        let opts = js_upsert_edge_options(options);
        Ok(AsyncTask::new(TxnAsyncOp::new(
            self,
            move |txn| txn.upsert_edge_as(&alias, from, to, type_id, opts),
            txn_edge_ref_to_js,
        )?))
    }

    #[napi(ts_return_type = "Promise<void>")]
    pub fn delete_node_async(&self, target: JsTxnNodeRef) -> Result<AsyncTask<TxnAsyncOp<(), ()>>> {
        let target = js_txn_node_ref_to_rust(target)?;
        Ok(AsyncTask::new(TxnAsyncOp::new(
            self,
            move |txn| txn.delete_node(target),
            |_| Ok(()),
        )?))
    }

    #[napi(ts_return_type = "Promise<void>")]
    pub fn delete_edge_async(&self, target: JsTxnEdgeRef) -> Result<AsyncTask<TxnAsyncOp<(), ()>>> {
        let target = js_txn_edge_ref_to_rust(target)?;
        Ok(AsyncTask::new(TxnAsyncOp::new(
            self,
            move |txn| txn.delete_edge(target),
            |_| Ok(()),
        )?))
    }

    #[napi(ts_return_type = "Promise<void>")]
    pub fn invalidate_edge_async(
        &self,
        target: JsTxnEdgeRef,
        valid_to: i64,
    ) -> Result<AsyncTask<TxnAsyncOp<(), ()>>> {
        let target = js_txn_edge_ref_to_rust(target)?;
        Ok(AsyncTask::new(TxnAsyncOp::new(
            self,
            move |txn| txn.invalidate_edge(target, valid_to),
            |_| Ok(()),
        )?))
    }

    #[napi(ts_return_type = "Promise<void>")]
    pub fn stage_async(
        &self,
        operations: Vec<JsTxnOperation>,
    ) -> Result<AsyncTask<TxnAsyncOp<(), ()>>> {
        let intents = operations
            .into_iter()
            .map(js_txn_operation_to_rust)
            .collect::<Result<Vec<_>>>()?;
        Ok(AsyncTask::new(TxnAsyncOp::new(
            self,
            move |txn| txn.stage_intents(intents),
            |_| Ok(()),
        )?))
    }

    #[napi(ts_return_type = "Promise<JsTxnNodeView | null>")]
    pub fn get_node_async(
        &self,
        target: JsTxnNodeRef,
    ) -> Result<AsyncTask<TxnAsyncOp<Option<TxnNodeView>, Option<JsTxnNodeView>>>> {
        let target = js_txn_node_ref_to_rust(target)?;
        Ok(AsyncTask::new(TxnAsyncOp::new(
            self,
            move |txn| txn.get_node(target),
            |view| view.map(txn_node_view_to_js).transpose(),
        )?))
    }

    #[napi(ts_return_type = "Promise<JsTxnEdgeView | null>")]
    pub fn get_edge_async(
        &self,
        target: JsTxnEdgeRef,
    ) -> Result<AsyncTask<TxnAsyncOp<Option<TxnEdgeView>, Option<JsTxnEdgeView>>>> {
        let target = js_txn_edge_ref_to_rust(target)?;
        Ok(AsyncTask::new(TxnAsyncOp::new(
            self,
            move |txn| txn.get_edge(target),
            |view| view.map(txn_edge_view_to_js).transpose(),
        )?))
    }

    #[napi(ts_return_type = "Promise<JsTxnNodeView | null>")]
    pub fn get_node_by_key_async(
        &self,
        type_id: u32,
        key: String,
    ) -> Result<AsyncTask<TxnAsyncOp<Option<TxnNodeView>, Option<JsTxnNodeView>>>> {
        Ok(AsyncTask::new(TxnAsyncOp::new(
            self,
            move |txn| txn.get_node_by_key(type_id, &key),
            |view| view.map(txn_node_view_to_js).transpose(),
        )?))
    }

    #[napi(ts_return_type = "Promise<JsTxnEdgeView | null>")]
    pub fn get_edge_by_triple_async(
        &self,
        from: JsTxnNodeRef,
        to: JsTxnNodeRef,
        type_id: u32,
    ) -> Result<AsyncTask<TxnAsyncOp<Option<TxnEdgeView>, Option<JsTxnEdgeView>>>> {
        let from = js_txn_node_ref_to_rust(from)?;
        let to = js_txn_node_ref_to_rust(to)?;
        Ok(AsyncTask::new(TxnAsyncOp::new(
            self,
            move |txn| txn.get_edge_by_triple(from, to, type_id),
            |view| view.map(txn_edge_view_to_js).transpose(),
        )?))
    }

    #[napi(ts_return_type = "Promise<JsTxnCommitResult>")]
    pub fn commit_async(
        &self,
    ) -> Result<AsyncTask<TxnAsyncTakeOp<TxnCommitResult, JsTxnCommitResult>>> {
        Ok(AsyncTask::new(TxnAsyncTakeOp::new(
            self,
            |txn| txn.commit(),
            txn_commit_result_to_js,
        )?))
    }

    #[napi(ts_return_type = "Promise<void>")]
    pub fn rollback_async(&self) -> Result<AsyncTask<TxnAsyncTakeOp<(), ()>>> {
        Ok(AsyncTask::new(TxnAsyncTakeOp::new(
            self,
            |txn| txn.rollback(),
            |_| Ok(()),
        )?))
    }
}

// ============================================================
// JS-facing types
// ============================================================

#[napi(object)]
pub struct JsCloseOptions {
    /// If true, cancel any in-progress background compaction instead of waiting.
    pub force: Option<bool>,
}

// ============================================================
// Method options structs (positional required + options bag)
// ============================================================

#[napi(object)]
pub struct JsUpsertNodeOptions {
    pub props: Option<HashMap<String, serde_json::Value>>,
    pub weight: Option<f64>,
    pub dense_vector: Option<Vec<f64>>,
    pub sparse_vector: Option<Vec<JsSparseEntry>>,
}

#[napi(object)]
pub struct JsUpsertEdgeOptions {
    pub props: Option<HashMap<String, serde_json::Value>>,
    pub weight: Option<f64>,
    pub valid_from: Option<i64>,
    pub valid_to: Option<i64>,
}

#[napi(object)]
pub struct JsNeighborsOptions {
    pub direction: Option<String>,
    pub type_filter: Option<Vec<u32>>,
    pub limit: Option<u32>,
    pub at_epoch: Option<i64>,
    pub decay_lambda: Option<f64>,
}

#[napi(object)]
pub struct JsNeighborsPagedOptions {
    pub direction: Option<String>,
    pub type_filter: Option<Vec<u32>>,
    pub limit: Option<u32>,
    pub after: Option<f64>,
    pub at_epoch: Option<i64>,
    pub decay_lambda: Option<f64>,
}

#[napi(object)]
pub struct JsNeighborsBatchOptions {
    pub direction: Option<String>,
    pub type_filter: Option<Vec<u32>>,
    pub at_epoch: Option<i64>,
    pub decay_lambda: Option<f64>,
}

#[napi(object)]
pub struct JsTraverseOptions {
    pub min_depth: Option<u32>,
    pub direction: Option<String>,
    pub edge_type_filter: Option<Vec<u32>>,
    pub node_type_filter: Option<Vec<u32>>,
    pub at_epoch: Option<i64>,
    pub decay_lambda: Option<f64>,
    pub limit: Option<u32>,
    pub cursor: Option<JsTraversalCursor>,
}

#[napi(object)]
pub struct JsTopKNeighborsOptions {
    pub direction: Option<String>,
    pub type_filter: Option<Vec<u32>>,
    pub scoring: Option<String>,
    pub decay_lambda: Option<f64>,
    pub at_epoch: Option<i64>,
}

#[napi(object)]
pub struct JsExtractSubgraphOptions {
    pub direction: Option<String>,
    pub edge_type_filter: Option<Vec<u32>>,
    pub at_epoch: Option<i64>,
}

#[napi(object)]
pub struct JsShortestPathOptions {
    pub direction: Option<String>,
    pub type_filter: Option<Vec<u32>>,
    pub weight_field: Option<String>,
    pub at_epoch: Option<i64>,
    pub max_depth: Option<u32>,
    pub max_cost: Option<f64>,
}

#[napi(object)]
pub struct JsAllShortestPathsOptions {
    pub direction: Option<String>,
    pub type_filter: Option<Vec<u32>>,
    pub weight_field: Option<String>,
    pub at_epoch: Option<i64>,
    pub max_depth: Option<u32>,
    pub max_cost: Option<f64>,
    pub max_paths: Option<u32>,
}

#[napi(object)]
pub struct JsIsConnectedOptions {
    pub direction: Option<String>,
    pub type_filter: Option<Vec<u32>>,
    pub at_epoch: Option<i64>,
    pub max_depth: Option<u32>,
}

#[napi(object)]
pub struct JsConnectedComponentsOptions {
    pub edge_type_filter: Option<Vec<u32>>,
    pub node_type_filter: Option<Vec<u32>>,
    pub at_epoch: Option<i64>,
}

#[napi(object)]
pub struct JsComponentOfOptions {
    pub edge_type_filter: Option<Vec<u32>>,
    pub node_type_filter: Option<Vec<u32>>,
    pub at_epoch: Option<i64>,
}

#[napi(object)]
pub struct JsDegreeOptions {
    pub direction: Option<String>,
    pub type_filter: Option<Vec<u32>>,
    pub at_epoch: Option<i64>,
}

#[napi(object)]
pub struct JsSumEdgeWeightsOptions {
    pub direction: Option<String>,
    pub type_filter: Option<Vec<u32>>,
    pub at_epoch: Option<i64>,
}

#[napi(object)]
pub struct JsAvgEdgeWeightOptions {
    pub direction: Option<String>,
    pub type_filter: Option<Vec<u32>>,
    pub at_epoch: Option<i64>,
}

#[napi(object)]
pub struct JsDegreesOptions {
    pub direction: Option<String>,
    pub type_filter: Option<Vec<u32>>,
    pub at_epoch: Option<i64>,
}

#[napi(object)]
pub struct JsVectorSearchOptions {
    pub k: u32,
    pub dense_query: Option<Vec<f64>>,
    pub sparse_query: Option<Vec<JsSparseEntry>>,
    pub type_filter: Option<Vec<u32>>,
    pub ef_search: Option<u32>,
    pub scope: Option<JsVectorSearchScope>,
    pub dense_weight: Option<f64>,
    pub sparse_weight: Option<f64>,
    pub fusion_mode: Option<String>,
}

#[napi(object)]
pub struct JsFindNodesPagedOptions {
    pub limit: Option<u32>,
    pub after: Option<f64>,
}

#[napi(object)]
#[derive(Clone)]
pub struct JsSecondaryIndexKind {
    pub kind: String,
    pub domain: Option<String>,
}

#[napi(object)]
pub struct JsNodePropertyIndexInfo {
    pub index_id: f64,
    pub type_id: u32,
    pub prop_key: String,
    pub kind: String,
    pub domain: Option<String>,
    pub state: String,
    pub last_error: Option<String>,
}

#[napi(object)]
#[derive(Clone)]
pub struct JsPropertyRangeBound {
    pub value: f64,
    pub inclusive: Option<bool>,
    pub domain: String,
}

#[napi(object)]
#[derive(Clone)]
pub struct JsPropertyRangeCursor {
    pub value: f64,
    pub node_id: f64,
    pub domain: String,
}

#[napi(object)]
#[derive(Clone)]
pub struct JsFindNodesRangePagedOptions {
    pub limit: Option<u32>,
    pub after: Option<JsPropertyRangeCursor>,
}

#[napi(object)]
pub struct JsFindNodesByTimeRangePagedOptions {
    pub limit: Option<u32>,
    pub after: Option<f64>,
}

#[napi(object)]
pub struct JsPersonalizedPagerankOptions {
    pub algorithm: Option<String>,
    pub damping_factor: Option<f64>,
    pub max_iterations: Option<u32>,
    pub epsilon: Option<f64>,
    pub approx_residual_tolerance: Option<f64>,
    pub edge_type_filter: Option<Vec<u32>>,
    pub max_results: Option<u32>,
}

#[napi(object)]
pub struct JsDbStats {
    /// Bytes buffered in WAL but not yet fsynced. Always 0 in immediate mode.
    pub pending_wal_bytes: u32,
    /// Number of on-disk segments.
    pub segment_count: u32,
    /// Node tombstones in the memtable.
    pub node_tombstone_count: u32,
    /// Edge tombstones in the memtable.
    pub edge_tombstone_count: u32,
    /// Timestamp (ms since epoch) of last completed compaction, or null.
    pub last_compaction_ms: Option<i64>,
    /// WAL sync mode: "immediate" or "group-commit".
    pub wal_sync_mode: String,
    /// Estimated bytes in the active (mutable) memtable.
    pub active_memtable_bytes: u32,
    /// Estimated bytes across all immutable memtables pending flush.
    pub immutable_memtable_bytes: u32,
    /// Number of immutable memtables pending flush.
    pub immutable_memtable_count: u32,
    /// Number of flush operations currently in flight.
    pub pending_flush_count: u32,
    /// The WAL generation ID currently being written to.
    pub active_wal_generation_id: f64,
    /// The oldest WAL generation ID still retained for recovery.
    pub oldest_retained_wal_generation_id: f64,
}

impl From<DbStats> for JsDbStats {
    fn from(s: DbStats) -> Self {
        JsDbStats {
            pending_wal_bytes: s.pending_wal_bytes.min(u32::MAX as usize) as u32,
            segment_count: s.segment_count.min(u32::MAX as usize) as u32,
            node_tombstone_count: s.node_tombstone_count.min(u32::MAX as usize) as u32,
            edge_tombstone_count: s.edge_tombstone_count.min(u32::MAX as usize) as u32,
            last_compaction_ms: s.last_compaction_ms,
            wal_sync_mode: s.wal_sync_mode,
            active_memtable_bytes: s.active_memtable_bytes.min(u32::MAX as usize) as u32,
            immutable_memtable_bytes: s.immutable_memtable_bytes.min(u32::MAX as usize) as u32,
            immutable_memtable_count: s.immutable_memtable_count.min(u32::MAX as usize) as u32,
            pending_flush_count: s.pending_flush_count.min(u32::MAX as usize) as u32,
            active_wal_generation_id: s.active_wal_generation_id as f64,
            oldest_retained_wal_generation_id: s.oldest_retained_wal_generation_id as f64,
        }
    }
}

#[napi(object)]
pub struct JsDenseVectorConfig {
    pub dimension: u32,
    pub metric: Option<String>,
}

#[napi(object)]
pub struct JsDbOptions {
    pub create_if_missing: Option<bool>,
    pub edge_uniqueness: Option<bool>,
    pub memtable_flush_threshold: Option<u32>,
    /// Trigger compaction automatically after this many flushes. Default 5, 0 = disabled.
    pub compact_after_n_flushes: Option<u32>,
    pub dense_vector: Option<JsDenseVectorConfig>,
    /// WAL sync mode: 'immediate' or 'group-commit' (default).
    pub wal_sync_mode: Option<String>,
    /// Group commit sync interval in milliseconds. Default: 10.
    pub group_commit_interval_ms: Option<u32>,
    /// Hard cap on memtable size in bytes. Writes trigger a flush when exceeded. 0 = disabled.
    pub memtable_hard_cap_bytes: Option<u32>,
    /// Maximum number of immutable memtables pending flush before writers block.
    /// Default: 4. Set to 0 to disable immutable count backpressure.
    pub max_immutable_memtables: Option<u32>,
}

impl From<JsDbOptions> for DbOptions {
    fn from(js: JsDbOptions) -> Self {
        let defaults = DbOptions::default();
        let wal_sync_mode = match js.wal_sync_mode.as_deref() {
            Some("immediate") => WalSyncMode::Immediate,
            _ => {
                // Default to GroupCommit, but allow overriding interval
                let interval_ms = js.group_commit_interval_ms.unwrap_or(50) as u64;
                WalSyncMode::GroupCommit {
                    interval_ms,
                    soft_trigger_bytes: 2 * 1024 * 1024,
                    hard_cap_bytes: 16 * 1024 * 1024,
                }
            }
        };
        let dense_vector = js.dense_vector.map(|dv| {
            let metric = match dv.metric.as_deref() {
                Some("euclidean") => DenseMetric::Euclidean,
                Some("dot_product") => DenseMetric::DotProduct,
                _ => DenseMetric::Cosine,
            };
            DenseVectorConfig {
                dimension: dv.dimension,
                metric,
                hnsw: HnswConfig::default(),
            }
        });
        DbOptions {
            create_if_missing: js.create_if_missing.unwrap_or(defaults.create_if_missing),
            edge_uniqueness: js.edge_uniqueness.unwrap_or(defaults.edge_uniqueness),
            memtable_flush_threshold: js
                .memtable_flush_threshold
                .map(|v| v as usize)
                .unwrap_or(defaults.memtable_flush_threshold),
            compact_after_n_flushes: js
                .compact_after_n_flushes
                .unwrap_or(defaults.compact_after_n_flushes),
            dense_vector,
            wal_sync_mode,
            memtable_hard_cap_bytes: js
                .memtable_hard_cap_bytes
                .map(|v| v as usize)
                .unwrap_or(defaults.memtable_hard_cap_bytes),
            max_immutable_memtables: js
                .max_immutable_memtables
                .map(|v| v as usize)
                .unwrap_or(defaults.max_immutable_memtables),
        }
    }
}

#[napi(object)]
pub struct JsKeyQuery {
    pub type_id: u32,
    pub key: String,
}

#[napi(object)]
pub struct JsNodeInput {
    pub type_id: u32,
    pub key: String,
    pub props: Option<HashMap<String, serde_json::Value>>,
    pub weight: Option<f64>,
    pub dense_vector: Option<Vec<f64>>,
    pub sparse_vector: Option<Vec<JsSparseEntry>>,
}

impl From<JsNodeInput> for NodeInput {
    fn from(js: JsNodeInput) -> Self {
        NodeInput {
            type_id: js.type_id,
            key: js.key,
            props: convert_js_props(js.props),
            weight: js.weight.unwrap_or(1.0) as f32,
            dense_vector: js
                .dense_vector
                .map(|v| v.into_iter().map(|x| x as f32).collect()),
            sparse_vector: js.sparse_vector.map(|v| {
                v.into_iter()
                    .map(|e| (e.dimension, e.value as f32))
                    .collect()
            }),
        }
    }
}

#[napi(object)]
pub struct JsSparseEntry {
    pub dimension: u32,
    pub value: f64,
}

#[napi(object)]
#[derive(Clone)]
pub struct JsTxnNodeRef {
    pub id: Option<f64>,
    pub type_id: Option<u32>,
    pub key: Option<String>,
    pub local: Option<String>,
}

#[napi(object)]
#[derive(Clone)]
pub struct JsTxnEdgeRef {
    pub id: Option<f64>,
    pub from: Option<JsTxnNodeRef>,
    pub to: Option<JsTxnNodeRef>,
    pub type_id: Option<u32>,
    pub local: Option<String>,
}

#[napi(object)]
pub struct JsTxnOperation {
    pub op: String,
    pub alias: Option<String>,
    pub type_id: Option<u32>,
    pub key: Option<String>,
    pub props: Option<HashMap<String, serde_json::Value>>,
    pub weight: Option<f64>,
    pub dense_vector: Option<Vec<f64>>,
    pub sparse_vector: Option<Vec<JsSparseEntry>>,
    pub from: Option<JsTxnNodeRef>,
    pub to: Option<JsTxnNodeRef>,
    pub target: Option<JsTxnEdgeOrNodeRef>,
    pub valid_from: Option<i64>,
    pub valid_to: Option<i64>,
}

#[napi(object)]
#[derive(Clone)]
pub struct JsTxnEdgeOrNodeRef {
    pub id: Option<f64>,
    pub type_id: Option<u32>,
    pub key: Option<String>,
    pub local: Option<String>,
    pub from: Option<JsTxnNodeRef>,
    pub to: Option<JsTxnNodeRef>,
}

#[napi(object)]
pub struct JsTxnNodeView {
    pub id: Option<f64>,
    pub local: Option<String>,
    pub type_id: u32,
    pub key: String,
    pub props: HashMap<String, serde_json::Value>,
    pub created_at: Option<i64>,
    pub updated_at: Option<i64>,
    pub weight: f64,
    pub dense_vector: Option<Vec<f64>>,
    pub sparse_vector: Option<Vec<JsSparseEntry>>,
}

#[napi(object)]
pub struct JsTxnEdgeView {
    pub id: Option<f64>,
    pub local: Option<String>,
    pub from: JsTxnNodeRef,
    pub to: JsTxnNodeRef,
    pub type_id: u32,
    pub props: HashMap<String, serde_json::Value>,
    pub created_at: Option<i64>,
    pub updated_at: Option<i64>,
    pub weight: f64,
    pub valid_from: Option<i64>,
    pub valid_to: Option<i64>,
}

#[napi(object)]
pub struct JsTxnCommitResult {
    pub node_ids: Float64Array,
    pub edge_ids: Float64Array,
    pub node_aliases: HashMap<String, f64>,
    pub edge_aliases: HashMap<String, f64>,
}

#[napi(object)]
pub struct JsVectorSearchScope {
    pub start_node_id: f64,
    pub max_depth: u32,
    pub direction: Option<String>,
    pub edge_type_filter: Option<Vec<u32>>,
    pub at_epoch: Option<i64>,
}

#[napi(object)]
pub struct JsVectorHit {
    pub node_id: f64,
    pub score: f64,
}

#[napi(object)]
pub struct JsEdgeInput {
    pub from: f64,
    pub to: f64,
    pub type_id: u32,
    pub props: Option<HashMap<String, serde_json::Value>>,
    pub weight: Option<f64>,
    pub valid_from: Option<i64>,
    pub valid_to: Option<i64>,
}

impl TryFrom<JsEdgeInput> for EdgeInput {
    type Error = napi::Error;
    fn try_from(js: JsEdgeInput) -> std::result::Result<Self, Self::Error> {
        Ok(EdgeInput {
            from: f64_to_u64(js.from)?,
            to: f64_to_u64(js.to)?,
            type_id: js.type_id,
            props: convert_js_props(js.props),
            weight: js.weight.unwrap_or(1.0) as f32,
            valid_from: js.valid_from,
            valid_to: js.valid_to,
        })
    }
}

/// Node record: eager primitives, lazy props. Props are Arc-shared so
/// container getters (page results, subgraph) avoid cloning the BTreeMap.
#[napi]
pub struct JsNodeRecord {
    id_val: f64,
    type_id_val: u32,
    key_val: String,
    created_at_val: i64,
    updated_at_val: i64,
    weight_val: f64,
    props_raw: Arc<BTreeMap<String, PropValue>>,
}

#[napi]
impl JsNodeRecord {
    #[napi(getter)]
    pub fn id(&self) -> f64 {
        self.id_val
    }
    #[napi(getter)]
    pub fn type_id(&self) -> u32 {
        self.type_id_val
    }
    #[napi(getter)]
    pub fn key(&self) -> String {
        self.key_val.clone()
    }
    #[napi(getter)]
    pub fn props(&self) -> HashMap<String, serde_json::Value> {
        props_to_json((*self.props_raw).clone())
    }
    #[napi(getter)]
    pub fn created_at(&self) -> i64 {
        self.created_at_val
    }
    #[napi(getter)]
    pub fn updated_at(&self) -> i64 {
        self.updated_at_val
    }
    #[napi(getter)]
    pub fn weight(&self) -> f64 {
        self.weight_val
    }
}

impl TryFrom<NodeRecord> for JsNodeRecord {
    type Error = napi::Error;
    fn try_from(n: NodeRecord) -> Result<Self> {
        Ok(JsNodeRecord {
            id_val: u64_to_f64(n.id)?,
            type_id_val: n.type_id,
            key_val: n.key,
            created_at_val: n.created_at,
            updated_at_val: n.updated_at,
            weight_val: n.weight as f64,
            props_raw: Arc::new(n.props),
        })
    }
}

/// Edge record: eager primitives, lazy props. Props are Arc-shared so
/// container getters (page results, subgraph) avoid cloning the BTreeMap.
#[napi]
pub struct JsEdgeRecord {
    id_val: f64,
    from_val: f64,
    to_val: f64,
    type_id_val: u32,
    created_at_val: i64,
    updated_at_val: i64,
    weight_val: f64,
    valid_from_val: i64,
    valid_to_val: i64,
    props_raw: Arc<BTreeMap<String, PropValue>>,
}

#[napi]
impl JsEdgeRecord {
    #[napi(getter)]
    pub fn id(&self) -> f64 {
        self.id_val
    }
    #[napi(getter)]
    pub fn from(&self) -> f64 {
        self.from_val
    }
    #[napi(getter)]
    pub fn to(&self) -> f64 {
        self.to_val
    }
    #[napi(getter)]
    pub fn type_id(&self) -> u32 {
        self.type_id_val
    }
    #[napi(getter)]
    pub fn props(&self) -> HashMap<String, serde_json::Value> {
        props_to_json((*self.props_raw).clone())
    }
    #[napi(getter)]
    pub fn created_at(&self) -> i64 {
        self.created_at_val
    }
    #[napi(getter)]
    pub fn updated_at(&self) -> i64 {
        self.updated_at_val
    }
    #[napi(getter)]
    pub fn weight(&self) -> f64 {
        self.weight_val
    }
    #[napi(getter)]
    pub fn valid_from(&self) -> i64 {
        self.valid_from_val
    }
    #[napi(getter)]
    pub fn valid_to(&self) -> i64 {
        self.valid_to_val
    }
}

impl TryFrom<EdgeRecord> for JsEdgeRecord {
    type Error = napi::Error;
    fn try_from(e: EdgeRecord) -> Result<Self> {
        Ok(JsEdgeRecord {
            id_val: u64_to_f64(e.id)?,
            from_val: u64_to_f64(e.from)?,
            to_val: u64_to_f64(e.to)?,
            type_id_val: e.type_id,
            created_at_val: e.created_at,
            updated_at_val: e.updated_at,
            weight_val: e.weight as f64,
            valid_from_val: e.valid_from,
            valid_to_val: e.valid_to,
            props_raw: Arc::new(e.props),
        })
    }
}

/// A single neighbor entry as a plain JS object.
#[napi(object)]
#[derive(Clone)]
pub struct JsNeighborEntry {
    pub node_id: f64,
    pub edge_id: f64,
    pub edge_type_id: u32,
    pub weight: f64,
    pub valid_from: i64,
    pub valid_to: i64,
}

fn neighbor_to_js_entry(e: &NeighborEntry) -> Result<JsNeighborEntry> {
    Ok(JsNeighborEntry {
        node_id: u64_to_f64(e.node_id)?,
        edge_id: u64_to_f64(e.edge_id)?,
        edge_type_id: e.edge_type_id,
        weight: e.weight as f64,
        valid_from: e.valid_from,
        valid_to: e.valid_to,
    })
}

#[napi(object)]
pub struct JsNeighborBatchEntry {
    pub query_node_id: f64,
    pub neighbors: Vec<JsNeighborEntry>,
}

#[napi(object)]
pub struct JsDegreeBatchEntry {
    pub node_id: f64,
    pub degree: i64,
}

#[napi(object)]
pub struct JsComponentEntry {
    pub node_id: f64,
    pub component_id: f64,
}

#[napi(object)]
pub struct JsShortestPath {
    pub nodes: Vec<f64>,
    pub edges: Vec<f64>,
    pub total_cost: f64,
}

fn shortest_path_to_js(sp: ShortestPath) -> Result<JsShortestPath> {
    Ok(JsShortestPath {
        nodes: sp
            .nodes
            .into_iter()
            .map(u64_to_f64)
            .collect::<Result<Vec<_>>>()?,
        edges: sp
            .edges
            .into_iter()
            .map(u64_to_f64)
            .collect::<Result<Vec<_>>>()?,
        total_cost: sp.total_cost,
    })
}

#[napi(object)]
pub struct JsTraversalHit {
    pub node_id: f64,
    pub depth: u32,
    pub via_edge_id: Option<f64>,
    pub score: Option<f64>,
}

#[napi(object)]
pub struct JsTraversalCursor {
    pub depth: u32,
    pub last_node_id: f64,
}

#[napi(object)]
pub struct JsTraversalPageResult {
    pub items: Vec<JsTraversalHit>,
    pub next_cursor: Option<JsTraversalCursor>,
}

fn traversal_hit_to_js(hit: TraversalHit) -> Result<JsTraversalHit> {
    Ok(JsTraversalHit {
        node_id: u64_to_f64(hit.node_id)?,
        depth: hit.depth,
        via_edge_id: hit.via_edge_id.map(u64_to_f64).transpose()?,
        score: hit.score,
    })
}

fn traversal_cursor_to_js(cursor: TraversalCursor) -> Result<JsTraversalCursor> {
    Ok(JsTraversalCursor {
        depth: cursor.depth,
        last_node_id: u64_to_f64(cursor.last_node_id)?,
    })
}

fn js_traversal_cursor_to_rust(cursor: JsTraversalCursor) -> Result<TraversalCursor> {
    Ok(TraversalCursor {
        depth: cursor.depth,
        last_node_id: f64_to_u64(cursor.last_node_id)?,
    })
}

fn traversal_page_to_js(page: TraversalPageResult) -> Result<JsTraversalPageResult> {
    Ok(JsTraversalPageResult {
        items: page
            .items
            .into_iter()
            .map(traversal_hit_to_js)
            .collect::<Result<Vec<_>>>()?,
        next_cursor: page.next_cursor.map(traversal_cursor_to_js).transpose()?,
    })
}

#[napi]
pub struct JsSubgraphResult {
    nodes_vec: Vec<JsNodeRecord>,
    edges_vec: Vec<JsEdgeRecord>,
}

#[napi]
impl JsSubgraphResult {
    #[napi(getter)]
    pub fn nodes(&self) -> Vec<JsNodeRecord> {
        self.nodes_vec
            .iter()
            .map(|n| JsNodeRecord {
                id_val: n.id_val,
                type_id_val: n.type_id_val,
                key_val: n.key_val.clone(),
                created_at_val: n.created_at_val,
                updated_at_val: n.updated_at_val,
                weight_val: n.weight_val,
                props_raw: Arc::clone(&n.props_raw),
            })
            .collect()
    }
    #[napi(getter)]
    pub fn edges(&self) -> Vec<JsEdgeRecord> {
        self.edges_vec
            .iter()
            .map(|e| JsEdgeRecord {
                id_val: e.id_val,
                from_val: e.from_val,
                to_val: e.to_val,
                type_id_val: e.type_id_val,
                created_at_val: e.created_at_val,
                updated_at_val: e.updated_at_val,
                weight_val: e.weight_val,
                valid_from_val: e.valid_from_val,
                valid_to_val: e.valid_to_val,
                props_raw: Arc::clone(&e.props_raw),
            })
            .collect()
    }
}

fn subgraph_to_js(sg: Subgraph) -> Result<JsSubgraphResult> {
    Ok(JsSubgraphResult {
        nodes_vec: sg
            .nodes
            .into_iter()
            .map(JsNodeRecord::try_from)
            .collect::<Result<Vec<_>>>()?,
        edges_vec: sg
            .edges
            .into_iter()
            .map(JsEdgeRecord::try_from)
            .collect::<Result<Vec<_>>>()?,
    })
}

// --- Pagination result types ---

#[napi(object)]
pub struct JsIdPageResult {
    pub items: Float64Array,
    pub next_cursor: Option<f64>,
}

#[napi]
pub struct JsNodePageResult {
    items_vec: Vec<JsNodeRecord>,
    cursor: Option<u64>,
}

#[napi]
impl JsNodePageResult {
    #[napi(getter)]
    pub fn items(&self) -> Vec<JsNodeRecord> {
        self.items_vec
            .iter()
            .map(|n| JsNodeRecord {
                id_val: n.id_val,
                type_id_val: n.type_id_val,
                key_val: n.key_val.clone(),
                created_at_val: n.created_at_val,
                updated_at_val: n.updated_at_val,
                weight_val: n.weight_val,
                props_raw: Arc::clone(&n.props_raw),
            })
            .collect()
    }
    #[napi(getter)]
    pub fn next_cursor(&self) -> Result<Option<f64>> {
        self.cursor.map(u64_to_f64).transpose()
    }
}

#[napi]
pub struct JsEdgePageResult {
    items_vec: Vec<JsEdgeRecord>,
    cursor: Option<u64>,
}

#[napi]
impl JsEdgePageResult {
    #[napi(getter)]
    pub fn items(&self) -> Vec<JsEdgeRecord> {
        self.items_vec
            .iter()
            .map(|e| JsEdgeRecord {
                id_val: e.id_val,
                from_val: e.from_val,
                to_val: e.to_val,
                type_id_val: e.type_id_val,
                created_at_val: e.created_at_val,
                updated_at_val: e.updated_at_val,
                weight_val: e.weight_val,
                valid_from_val: e.valid_from_val,
                valid_to_val: e.valid_to_val,
                props_raw: Arc::clone(&e.props_raw),
            })
            .collect()
    }
    #[napi(getter)]
    pub fn next_cursor(&self) -> Result<Option<f64>> {
        self.cursor.map(u64_to_f64).transpose()
    }
}

#[napi]
pub struct JsNeighborPageResult {
    items_vec: Vec<JsNeighborEntry>,
    cursor: Option<u64>,
}

#[napi]
impl JsNeighborPageResult {
    #[napi(getter)]
    pub fn items(&self) -> Vec<JsNeighborEntry> {
        self.items_vec.clone()
    }

    #[napi(getter)]
    pub fn next_cursor(&self) -> Result<Option<f64>> {
        self.cursor.map(u64_to_f64).transpose()
    }
}

fn id_page_to_js(page: PageResult<u64>) -> Result<JsIdPageResult> {
    Ok(JsIdPageResult {
        items: ids_to_float64_array(&page.items)?,
        next_cursor: page.next_cursor.map(u64_to_f64).transpose()?,
    })
}

fn node_page_to_js(page: PageResult<NodeRecord>) -> Result<JsNodePageResult> {
    Ok(JsNodePageResult {
        items_vec: page
            .items
            .into_iter()
            .map(JsNodeRecord::try_from)
            .collect::<Result<Vec<_>>>()?,
        cursor: page.next_cursor,
    })
}

fn edge_page_to_js(page: PageResult<EdgeRecord>) -> Result<JsEdgePageResult> {
    Ok(JsEdgePageResult {
        items_vec: page
            .items
            .into_iter()
            .map(JsEdgeRecord::try_from)
            .collect::<Result<Vec<_>>>()?,
        cursor: page.next_cursor,
    })
}

fn neighbor_page_to_js(page: PageResult<NeighborEntry>) -> Result<JsNeighborPageResult> {
    Ok(JsNeighborPageResult {
        items_vec: neighbor_entries_to_js(page.items)?,
        cursor: page.next_cursor,
    })
}

#[napi(object)]
pub struct JsPropertyRangePageResult {
    pub items: Float64Array,
    pub next_cursor: Option<JsPropertyRangeCursor>,
}

fn node_property_index_info_to_js(info: NodePropertyIndexInfo) -> Result<JsNodePropertyIndexInfo> {
    let (kind, domain) = secondary_index_kind_to_js(&info.kind);
    Ok(JsNodePropertyIndexInfo {
        index_id: u64_to_f64(info.index_id)?,
        type_id: info.type_id,
        prop_key: info.prop_key,
        kind,
        domain,
        state: secondary_index_state_to_js(info.state).to_string(),
        last_error: info.last_error,
    })
}

fn node_property_index_infos_to_js(
    infos: Vec<NodePropertyIndexInfo>,
) -> Result<Vec<JsNodePropertyIndexInfo>> {
    infos
        .into_iter()
        .map(node_property_index_info_to_js)
        .collect()
}

fn property_range_cursor_to_js(cursor: PropertyRangeCursor) -> Result<JsPropertyRangeCursor> {
    let (value, domain) = prop_value_to_js_numeric_parts(&cursor.value)?;
    Ok(JsPropertyRangeCursor {
        value,
        node_id: u64_to_f64(cursor.node_id)?,
        domain,
    })
}

fn js_property_range_cursor_to_rust(cursor: JsPropertyRangeCursor) -> Result<PropertyRangeCursor> {
    let domain = parse_secondary_index_range_domain(Some(cursor.domain.as_str()))?;
    Ok(PropertyRangeCursor {
        value: js_numeric_to_prop_value(cursor.value, domain)?,
        node_id: f64_to_u64(cursor.node_id)?,
    })
}

fn property_range_page_to_js(
    page: PropertyRangePageResult<u64>,
) -> Result<JsPropertyRangePageResult> {
    Ok(JsPropertyRangePageResult {
        items: ids_to_float64_array(&page.items)?,
        next_cursor: page
            .next_cursor
            .map(property_range_cursor_to_js)
            .transpose()?,
    })
}

fn make_page_request(limit: Option<u32>, after: Option<f64>) -> napi::Result<PageRequest> {
    let after_val = after.map(f64_to_u64).transpose()?;
    Ok(PageRequest {
        limit: limit.map(|l| l as usize),
        after: after_val,
    })
}

fn query_node_ids_to_js(result: QueryNodeIdsResult) -> Result<JsIdPageResult> {
    Ok(JsIdPageResult {
        items: ids_to_float64_array(&result.items)?,
        next_cursor: result.next_cursor.map(u64_to_f64).transpose()?,
    })
}

fn query_nodes_to_js(result: QueryNodesResult) -> Result<JsNodePageResult> {
    Ok(JsNodePageResult {
        items_vec: result
            .items
            .into_iter()
            .map(JsNodeRecord::try_from)
            .collect::<Result<Vec<_>>>()?,
        cursor: result.next_cursor,
    })
}

fn query_pattern_result_to_js(result: QueryPatternResult) -> Result<JsJsonValue> {
    Ok(JsJsonValue(serde_json::json!({
        "matches": result
            .matches
            .into_iter()
            .map(query_match_to_js)
            .collect::<Result<Vec<_>>>()?,
        "truncated": result.truncated,
    })))
}

fn query_match_to_js(match_: QueryMatch) -> Result<serde_json::Value> {
    let mut nodes = serde_json::Map::new();
    for (alias, id) in match_.nodes {
        nodes.insert(alias, serde_json::json!(u64_to_f64(id)?));
    }
    let mut edges = serde_json::Map::new();
    for (alias, id) in match_.edges {
        edges.insert(alias, serde_json::json!(u64_to_f64(id)?));
    }
    Ok(serde_json::json!({
        "nodes": nodes,
        "edges": edges,
    }))
}

fn query_plan_to_js(plan: QueryPlan) -> Result<JsJsonValue> {
    Ok(JsJsonValue(serde_json::json!({
        "kind": query_plan_kind_to_js(&plan.kind),
        "root": query_plan_node_to_js(plan.root),
        "estimatedCandidates": plan.estimated_candidates.map(|count| count as f64),
        "warnings": plan
            .warnings
            .iter()
            .map(query_plan_warning_to_js)
            .collect::<Vec<_>>(),
    })))
}

fn query_plan_kind_to_js(kind: &QueryPlanKind) -> &'static str {
    match kind {
        QueryPlanKind::NodeQuery => "node_query",
        QueryPlanKind::PatternQuery => "pattern_query",
    }
}

fn query_plan_node_to_js(node: QueryPlanNode) -> serde_json::Value {
    match node {
        QueryPlanNode::ExplicitIds => serde_json::json!({ "kind": "explicit_ids" }),
        QueryPlanNode::KeyLookup => serde_json::json!({ "kind": "key_lookup" }),
        QueryPlanNode::NodeTypeIndex => serde_json::json!({ "kind": "node_type_index" }),
        QueryPlanNode::PropertyEqualityIndex => {
            serde_json::json!({ "kind": "property_equality_index" })
        }
        QueryPlanNode::PropertyRangeIndex => {
            serde_json::json!({ "kind": "property_range_index" })
        }
        QueryPlanNode::TimestampIndex => serde_json::json!({ "kind": "timestamp_index" }),
        QueryPlanNode::AdjacencyExpansion => serde_json::json!({ "kind": "adjacency_expansion" }),
        QueryPlanNode::Intersect { inputs } => serde_json::json!({
            "kind": "intersect",
            "inputs": inputs.into_iter().map(query_plan_node_to_js).collect::<Vec<_>>(),
        }),
        QueryPlanNode::Union { inputs } => serde_json::json!({
            "kind": "union",
            "inputs": inputs.into_iter().map(query_plan_node_to_js).collect::<Vec<_>>(),
        }),
        QueryPlanNode::VerifyNodeFilter { input } => serde_json::json!({
            "kind": "verify_node_filter",
            "input": query_plan_node_to_js(*input),
        }),
        QueryPlanNode::VerifyEdgePredicates { input } => serde_json::json!({
            "kind": "verify_edge_predicates",
            "input": query_plan_node_to_js(*input),
        }),
        QueryPlanNode::PatternExpand {
            anchor_alias,
            input,
        } => serde_json::json!({
            "kind": "pattern_expand",
            "anchorAlias": anchor_alias,
            "input": query_plan_node_to_js(*input),
        }),
        QueryPlanNode::FallbackTypeScan => serde_json::json!({ "kind": "fallback_type_scan" }),
        QueryPlanNode::FallbackFullNodeScan => {
            serde_json::json!({ "kind": "fallback_full_node_scan" })
        }
        QueryPlanNode::EmptyResult => serde_json::json!({ "kind": "empty_result" }),
    }
}

fn query_plan_warning_to_js(warning: &QueryPlanWarning) -> &'static str {
    match warning {
        QueryPlanWarning::MissingReadyIndex => "missing_ready_index",
        QueryPlanWarning::UsingFallbackScan => "using_fallback_scan",
        QueryPlanWarning::FullScanRequiresOptIn => "full_scan_requires_opt_in",
        QueryPlanWarning::FullScanExplicitlyAllowed => "full_scan_explicitly_allowed",
        QueryPlanWarning::UnboundedPatternRejected => "unbounded_pattern_rejected",
        QueryPlanWarning::EdgePropertyPostFilter => "edge_property_post_filter",
        QueryPlanWarning::IndexSkippedAsBroad => "index_skipped_as_broad",
        QueryPlanWarning::CandidateCapExceeded => "candidate_cap_exceeded",
        QueryPlanWarning::RangeCandidateCapExceeded => "range_candidate_cap_exceeded",
        QueryPlanWarning::TimestampCandidateCapExceeded => "timestamp_candidate_cap_exceeded",
        QueryPlanWarning::VerifyOnlyFilter => "verify_only_filter",
        QueryPlanWarning::BooleanBranchFallback => "boolean_branch_fallback",
        QueryPlanWarning::PlanningProbeBudgetExceeded => "planning_probe_budget_exceeded",
    }
}

fn parse_js_node_query(value: &serde_json::Value) -> Result<NodeQuery> {
    let object = js_object(value, "node query request")?;
    let page = PageRequest {
        limit: parse_js_limit(object, "node query limit")?,
        after: parse_js_optional_u64_field(object, "after", "node query after")?,
    };
    let order = match js_non_null_field(object, "orderBy") {
        None => NodeQueryOrder::NodeIdAsc,
        Some(value) => match value.as_str() {
            Some("nodeIdAsc") | Some("node_id_asc") => NodeQueryOrder::NodeIdAsc,
            Some(other) => {
                return Err(napi::Error::from_reason(format!(
                    "Invalid orderBy '{}'. Must be 'nodeIdAsc'.",
                    other
                )));
            }
            None => {
                return Err(napi::Error::from_reason(
                    "node query orderBy must be a string".to_string(),
                ));
            }
        },
    };
    Ok(NodeQuery {
        type_id: parse_js_optional_u32_field(object, "typeId", "node query typeId")?,
        ids: parse_js_optional_u64_array_field(object, "ids", "node query ids")?,
        keys: parse_js_optional_string_array_field(object, "keys", "node query keys")?,
        filter: parse_js_node_filter(object, "updatedAt", "node query")?,
        page,
        order,
        allow_full_scan: parse_js_optional_bool_field(
            object,
            "allowFullScan",
            "node query allowFullScan",
        )?
        .unwrap_or(false),
    })
}

fn parse_js_graph_pattern_query(value: &serde_json::Value) -> Result<GraphPatternQuery> {
    let object = js_object(value, "graph pattern request")?;
    let nodes = match js_non_null_field(object, "nodes") {
        Some(value) => js_array(value, "graph pattern nodes")?
            .iter()
            .map(parse_js_node_pattern)
            .collect::<Result<Vec<_>>>()?,
        None => {
            return Err(napi::Error::from_reason(
                "graph pattern request requires nodes".to_string(),
            ));
        }
    };
    let edges = match js_non_null_field(object, "edges") {
        Some(value) => js_array(value, "graph pattern edges")?
            .iter()
            .map(parse_js_edge_pattern)
            .collect::<Result<Vec<_>>>()?,
        None => {
            return Err(napi::Error::from_reason(
                "graph pattern request requires edges".to_string(),
            ));
        }
    };
    let limit = match object.get("limit") {
        Some(serde_json::Value::Null) | None => {
            return Err(napi::Error::from_reason(
                "graph pattern request requires positive limit".to_string(),
            ));
        }
        Some(value) => {
            let parsed = js_number_to_u64(value, "graph pattern limit")?;
            if parsed == 0 {
                return Err(napi::Error::from_reason(
                    "graph pattern limit must be > 0".to_string(),
                ));
            }
            usize::try_from(parsed).map_err(|_| {
                napi::Error::from_reason("graph pattern limit is too large".to_string())
            })?
        }
    };
    Ok(GraphPatternQuery {
        nodes,
        edges,
        at_epoch: parse_js_optional_i64_field(object, "atEpoch", "graph pattern atEpoch")?,
        limit,
        order: PatternOrder::AnchorThenAliasesAsc,
    })
}

fn parse_js_node_pattern(value: &serde_json::Value) -> Result<NodePattern> {
    let object = js_object(value, "node pattern")?;
    Ok(NodePattern {
        alias: parse_js_required_string_field(object, "alias", "node pattern alias")?,
        type_id: parse_js_optional_u32_field(object, "typeId", "node pattern typeId")?,
        ids: parse_js_optional_u64_array_field(object, "ids", "node pattern ids")?,
        keys: parse_js_optional_string_array_field(object, "keys", "node pattern keys")?,
        filter: parse_js_node_filter(object, "updatedAt", "node pattern")?,
    })
}

fn parse_js_edge_pattern(value: &serde_json::Value) -> Result<EdgePattern> {
    let object = js_object(value, "edge pattern")?;
    if object.contains_key("filter") {
        return Err(napi::Error::from_reason(
            "edge pattern filter is not supported in Phase 24; use edge pattern where or predicates"
                .to_string(),
        ));
    }
    let direction = match js_non_null_field(object, "direction") {
        None => Direction::Outgoing,
        Some(value) => parse_direction(Some(value.as_str().ok_or_else(|| {
            napi::Error::from_reason("edge pattern direction must be a string".to_string())
        })?))?,
    };
    Ok(EdgePattern {
        alias: parse_js_optional_string_field(object, "alias", "edge pattern alias")?,
        from_alias: parse_js_required_string_field(object, "fromAlias", "edge pattern fromAlias")?,
        to_alias: parse_js_required_string_field(object, "toAlias", "edge pattern toAlias")?,
        direction,
        type_filter: parse_js_optional_u32_array_field(
            object,
            "typeFilter",
            "edge pattern typeFilter",
        )?,
        property_predicates: parse_js_edge_predicates(object, "edge pattern")?,
    })
}

fn reject_js_legacy_node_predicate_fields(
    object: &serde_json::Map<String, serde_json::Value>,
    context: &str,
) -> Result<()> {
    if object.contains_key("where") {
        return Err(napi::Error::from_reason(format!(
            "{} where is no longer supported; use filter",
            context
        )));
    }
    if object.contains_key("predicates") {
        return Err(napi::Error::from_reason(format!(
            "{} predicates are no longer supported; use filter",
            context
        )));
    }
    Ok(())
}

fn parse_js_node_filter(
    object: &serde_json::Map<String, serde_json::Value>,
    updated_at_key: &str,
    context: &str,
) -> Result<Option<NodeFilterExpr>> {
    reject_js_legacy_node_predicate_fields(object, context)?;
    match object.get("filter") {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(value) => {
            parse_js_node_filter_expr(value, updated_at_key, &format!("{} filter", context))
                .map(Some)
        }
    }
}

fn parse_js_node_filter_expr(
    value: &serde_json::Value,
    updated_at_key: &str,
    context: &str,
) -> Result<NodeFilterExpr> {
    let object = js_object(value, context)?;
    if object.is_empty() {
        return Err(napi::Error::from_reason(format!(
            "{} must not be an empty object",
            context
        )));
    }

    let selectors = ["and", "or", "not", "property", updated_at_key]
        .iter()
        .filter(|field| object.contains_key(**field))
        .count();
    if selectors != 1 {
        return Err(napi::Error::from_reason(format!(
            "{} must contain exactly one boolean tag or leaf selector",
            context
        )));
    }
    reject_js_uppercase_filter_fields(object, context)?;

    if let Some(value) = object.get("and") {
        ensure_only_js_fields(object, &["and"], context)?;
        let children = js_array(value, &format!("{} and", context))?;
        if children.is_empty() {
            return Err(napi::Error::from_reason(format!(
                "{} and must contain at least one child",
                context
            )));
        }
        return children
            .iter()
            .enumerate()
            .map(|(index, child)| {
                parse_js_node_filter_expr(
                    child,
                    updated_at_key,
                    &format!("{} and[{}]", context, index),
                )
            })
            .collect::<Result<Vec<_>>>()
            .map(NodeFilterExpr::And);
    }
    if let Some(value) = object.get("or") {
        ensure_only_js_fields(object, &["or"], context)?;
        let children = js_array(value, &format!("{} or", context))?;
        if children.is_empty() {
            return Err(napi::Error::from_reason(format!(
                "{} or must contain at least one child",
                context
            )));
        }
        return children
            .iter()
            .enumerate()
            .map(|(index, child)| {
                parse_js_node_filter_expr(
                    child,
                    updated_at_key,
                    &format!("{} or[{}]", context, index),
                )
            })
            .collect::<Result<Vec<_>>>()
            .map(NodeFilterExpr::Or);
    }
    if let Some(value) = object.get("not") {
        ensure_only_js_fields(object, &["not"], context)?;
        return parse_js_node_filter_expr(value, updated_at_key, &format!("{} not", context))
            .map(Box::new)
            .map(NodeFilterExpr::Not);
    }
    if object.contains_key("property") {
        return parse_js_property_node_filter(object, context);
    }
    if let Some(value) = object.get(updated_at_key) {
        ensure_only_js_fields(object, &[updated_at_key], context)?;
        return parse_js_updated_at_filter(value, updated_at_key, context);
    }

    Err(napi::Error::from_reason(format!(
        "{} must contain a valid filter selector",
        context
    )))
}

fn parse_js_edge_predicates(
    object: &serde_json::Map<String, serde_json::Value>,
    context: &str,
) -> Result<Vec<EdgePostFilterPredicate>> {
    let mut predicates = Vec::new();
    if let Some(where_value) = js_non_null_field(object, "where") {
        let where_object = js_object(where_value, &format!("{} where", context))?;
        for (key, value) in where_object {
            predicates.push(parse_js_property_edge_predicate(
                key.clone(),
                value,
                &format!("{} where.{}", context, key),
            )?);
        }
    }
    if let Some(predicates_value) = js_non_null_field(object, "predicates") {
        for (index, value) in js_array(predicates_value, &format!("{} predicates", context))?
            .iter()
            .enumerate()
        {
            let predicate_object = js_object(value, &format!("{} predicates[{}]", context, index))?;
            if predicate_object.len() != 1 {
                return Err(napi::Error::from_reason(format!(
                    "{} predicates[{}] must contain exactly one top-level predicate tag",
                    context, index
                )));
            }
            let (tag, payload) = predicate_object.iter().next().unwrap();
            match tag.as_str() {
                "property" => predicates.push(parse_js_explicit_property_edge_predicate(
                    payload,
                    &format!("{} predicates[{}].property", context, index),
                )?),
                other => {
                    return Err(napi::Error::from_reason(format!(
                        "Unknown edge predicate tag '{}'. Only 'property' is supported.",
                        other
                    )));
                }
            }
        }
    }
    Ok(predicates)
}

fn parse_js_explicit_property_edge_predicate(
    value: &serde_json::Value,
    context: &str,
) -> Result<EdgePostFilterPredicate> {
    let object = js_object(value, context)?;
    let key = parse_js_required_string_field(object, "key", &format!("{} key", context))?;
    parse_js_property_edge_predicate(key, value, context)
}

fn parse_js_property_node_filter(
    object: &serde_json::Map<String, serde_json::Value>,
    context: &str,
) -> Result<NodeFilterExpr> {
    let key = parse_js_required_string_field(object, "property", &format!("{} property", context))?;
    if key.is_empty() {
        return Err(napi::Error::from_reason(format!(
            "{} property must be non-empty",
            context
        )));
    }

    let has_range = has_any_js_field(object, &["gt", "gte", "lt", "lte"]);
    let families = [
        object.contains_key("eq"),
        object.contains_key("in"),
        has_range,
        object.contains_key("exists"),
        object.contains_key("missing"),
    ]
    .into_iter()
    .filter(|present| *present)
    .count();
    if families != 1 {
        return Err(napi::Error::from_reason(format!(
            "{} property filter must specify exactly one operator family",
            context
        )));
    }

    if let Some(value) = object.get("eq") {
        ensure_only_js_fields(object, &["property", "eq"], context)?;
        return Ok(NodeFilterExpr::PropertyEquals {
            key,
            value: json_to_prop_value(value),
        });
    }
    if let Some(value) = object.get("in") {
        ensure_only_js_fields(object, &["property", "in"], context)?;
        let values = js_array(value, &format!("{} in", context))?;
        if values.is_empty() {
            return Err(napi::Error::from_reason(format!(
                "{} in must contain at least one value",
                context
            )));
        }
        return Ok(NodeFilterExpr::PropertyIn {
            key,
            values: values.iter().map(json_to_prop_value).collect(),
        });
    }
    if has_range {
        ensure_only_js_fields(object, &["property", "gt", "gte", "lt", "lte"], context)?;
        let (lower, upper) = parse_js_property_range_bounds(object, context)?;
        return Ok(NodeFilterExpr::PropertyRange { key, lower, upper });
    }
    if object.contains_key("exists") {
        ensure_only_js_fields(object, &["property", "exists"], context)?;
        require_js_true_field(object, "exists", context)?;
        return Ok(NodeFilterExpr::PropertyExists { key });
    }
    if object.contains_key("missing") {
        ensure_only_js_fields(object, &["property", "missing"], context)?;
        require_js_true_field(object, "missing", context)?;
        return Ok(NodeFilterExpr::PropertyMissing { key });
    }

    unreachable!("operator family count was checked above")
}

fn parse_js_property_edge_predicate(
    key: String,
    value: &serde_json::Value,
    context: &str,
) -> Result<EdgePostFilterPredicate> {
    let parsed = parse_js_property_predicate(value, context)?;
    Ok(match parsed {
        JsParsedPropertyPredicate::Equals(value) => {
            EdgePostFilterPredicate::PropertyEquals { key, value }
        }
        JsParsedPropertyPredicate::Range { lower, upper } => {
            EdgePostFilterPredicate::PropertyRange { key, lower, upper }
        }
    })
}

enum JsParsedPropertyPredicate {
    Equals(PropValue),
    Range {
        lower: Option<PropertyRangeBound>,
        upper: Option<PropertyRangeBound>,
    },
}

fn parse_js_property_predicate(
    value: &serde_json::Value,
    context: &str,
) -> Result<JsParsedPropertyPredicate> {
    let object = js_object(value, context)?;
    match js_non_null_field(object, "op") {
        Some(op_value) => match op_value.as_str() {
            Some("eq") => {
                ensure_no_js_fields(object, &["gt", "gte", "lt", "lte", "eq"], context)?;
                let value = object.get("value").ok_or_else(|| {
                    napi::Error::from_reason(format!("{} eq predicate requires value", context))
                })?;
                Ok(JsParsedPropertyPredicate::Equals(json_to_prop_value(value)))
            }
            Some("range") => {
                ensure_no_js_fields(object, &["value", "eq"], context)?;
                let (lower, upper) = parse_js_property_range_bounds(object, context)?;
                Ok(JsParsedPropertyPredicate::Range { lower, upper })
            }
            Some(other) => Err(napi::Error::from_reason(format!(
                "Unknown predicate op '{}'. Valid ops are 'eq' and 'range'.",
                other
            ))),
            None => Err(napi::Error::from_reason(format!(
                "{} predicate op must be a string",
                context
            ))),
        },
        None if object.contains_key("eq") => {
            ensure_no_js_fields(object, &["value", "gt", "gte", "lt", "lte"], context)?;
            Ok(JsParsedPropertyPredicate::Equals(json_to_prop_value(
                object.get("eq").unwrap(),
            )))
        }
        None if has_any_js_field(object, &["gt", "gte", "lt", "lte"]) => {
            ensure_no_js_fields(object, &["value", "eq"], context)?;
            let (lower, upper) = parse_js_property_range_bounds(object, context)?;
            Ok(JsParsedPropertyPredicate::Range { lower, upper })
        }
        None => Err(napi::Error::from_reason(format!(
            "{} predicate requires op, eq, or range bounds",
            context
        ))),
    }
}

fn parse_js_updated_at_filter(
    value: &serde_json::Value,
    tag: &str,
    context: &str,
) -> Result<NodeFilterExpr> {
    let object = js_object(value, &format!("{} {}", context, tag))?;
    ensure_only_js_fields(
        object,
        &["gt", "gte", "lt", "lte"],
        &format!("{} {}", context, tag),
    )?;
    let (lower_ms, upper_ms) = parse_js_i64_range_bounds(object, &format!("{} {}", context, tag))?;
    Ok(NodeFilterExpr::UpdatedAtRange { lower_ms, upper_ms })
}

fn parse_js_property_range_bounds(
    object: &serde_json::Map<String, serde_json::Value>,
    context: &str,
) -> Result<(Option<PropertyRangeBound>, Option<PropertyRangeBound>)> {
    if object.contains_key("gt") && object.contains_key("gte") {
        return Err(napi::Error::from_reason(format!(
            "{} range predicate cannot specify both gt and gte",
            context
        )));
    }
    if object.contains_key("lt") && object.contains_key("lte") {
        return Err(napi::Error::from_reason(format!(
            "{} range predicate cannot specify both lt and lte",
            context
        )));
    }
    let lower = if let Some(value) = object.get("gt") {
        Some(PropertyRangeBound::Excluded(json_to_prop_value(value)))
    } else {
        object
            .get("gte")
            .map(|value| PropertyRangeBound::Included(json_to_prop_value(value)))
    };
    let upper = if let Some(value) = object.get("lt") {
        Some(PropertyRangeBound::Excluded(json_to_prop_value(value)))
    } else {
        object
            .get("lte")
            .map(|value| PropertyRangeBound::Included(json_to_prop_value(value)))
    };
    if lower.is_none() && upper.is_none() {
        return Err(napi::Error::from_reason(format!(
            "{} range predicate requires at least one of gt, gte, lt, or lte",
            context
        )));
    }
    Ok((lower, upper))
}

fn parse_js_i64_range_bounds(
    object: &serde_json::Map<String, serde_json::Value>,
    context: &str,
) -> Result<(Option<i64>, Option<i64>)> {
    if object.contains_key("gt") && object.contains_key("gte") {
        return Err(napi::Error::from_reason(format!(
            "{} range predicate cannot specify both gt and gte",
            context
        )));
    }
    if object.contains_key("lt") && object.contains_key("lte") {
        return Err(napi::Error::from_reason(format!(
            "{} range predicate cannot specify both lt and lte",
            context
        )));
    }
    let mut impossible = false;
    let lower = if let Some(value) = object.get("gt") {
        let value = js_number_to_i64(value, &format!("{} gt", context))?;
        match value.checked_add(1) {
            Some(next) => Some(next),
            None => {
                impossible = true;
                Some(i64::MAX)
            }
        }
    } else {
        object
            .get("gte")
            .map(|value| js_number_to_i64(value, &format!("{} gte", context)))
            .transpose()?
    };
    let upper = if let Some(value) = object.get("lt") {
        let value = js_number_to_i64(value, &format!("{} lt", context))?;
        match value.checked_sub(1) {
            Some(prev) => Some(prev),
            None => {
                impossible = true;
                Some(i64::MIN)
            }
        }
    } else {
        object
            .get("lte")
            .map(|value| js_number_to_i64(value, &format!("{} lte", context)))
            .transpose()?
    };
    if lower.is_none() && upper.is_none() {
        return Err(napi::Error::from_reason(format!(
            "{} range predicate requires at least one of gt, gte, lt, or lte",
            context
        )));
    }
    if impossible {
        return Ok((Some(i64::MAX), Some(i64::MIN)));
    }
    Ok((lower, upper))
}

fn js_object<'a>(
    value: &'a serde_json::Value,
    context: &str,
) -> Result<&'a serde_json::Map<String, serde_json::Value>> {
    value
        .as_object()
        .ok_or_else(|| napi::Error::from_reason(format!("{} must be an object", context)))
}

fn js_array<'a>(value: &'a serde_json::Value, context: &str) -> Result<&'a Vec<serde_json::Value>> {
    value
        .as_array()
        .ok_or_else(|| napi::Error::from_reason(format!("{} must be an array", context)))
}

fn js_non_null_field<'a>(
    object: &'a serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> Option<&'a serde_json::Value> {
    object.get(key).filter(|value| !value.is_null())
}

fn parse_js_limit(
    object: &serde_json::Map<String, serde_json::Value>,
    context: &str,
) -> Result<Option<usize>> {
    match object.get("limit") {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(value) => {
            let limit = js_number_to_u64(value, context)?;
            if limit == 0 {
                Ok(None)
            } else {
                Ok(Some(usize::try_from(limit).map_err(|_| {
                    napi::Error::from_reason(format!("{} is too large", context))
                })?))
            }
        }
    }
}

fn parse_js_optional_u64_field(
    object: &serde_json::Map<String, serde_json::Value>,
    key: &str,
    context: &str,
) -> Result<Option<u64>> {
    js_non_null_field(object, key)
        .map(|value| js_number_to_u64(value, context))
        .transpose()
}

fn parse_js_optional_i64_field(
    object: &serde_json::Map<String, serde_json::Value>,
    key: &str,
    context: &str,
) -> Result<Option<i64>> {
    js_non_null_field(object, key)
        .map(|value| js_number_to_i64(value, context))
        .transpose()
}

fn parse_js_optional_u32_field(
    object: &serde_json::Map<String, serde_json::Value>,
    key: &str,
    context: &str,
) -> Result<Option<u32>> {
    js_non_null_field(object, key)
        .map(|value| {
            let value = js_number_to_u64(value, context)?;
            u32::try_from(value)
                .map_err(|_| napi::Error::from_reason(format!("{} must fit in u32", context)))
        })
        .transpose()
}

fn parse_js_optional_bool_field(
    object: &serde_json::Map<String, serde_json::Value>,
    key: &str,
    context: &str,
) -> Result<Option<bool>> {
    js_non_null_field(object, key)
        .map(|value| {
            value
                .as_bool()
                .ok_or_else(|| napi::Error::from_reason(format!("{} must be a boolean", context)))
        })
        .transpose()
}

fn parse_js_optional_string_field(
    object: &serde_json::Map<String, serde_json::Value>,
    key: &str,
    context: &str,
) -> Result<Option<String>> {
    js_non_null_field(object, key)
        .map(|value| {
            value
                .as_str()
                .map(ToString::to_string)
                .ok_or_else(|| napi::Error::from_reason(format!("{} must be a string", context)))
        })
        .transpose()
}

fn parse_js_required_string_field(
    object: &serde_json::Map<String, serde_json::Value>,
    key: &str,
    context: &str,
) -> Result<String> {
    js_non_null_field(object, key)
        .ok_or_else(|| napi::Error::from_reason(format!("{} is required", context)))
        .and_then(|value| {
            value
                .as_str()
                .map(ToString::to_string)
                .ok_or_else(|| napi::Error::from_reason(format!("{} must be a string", context)))
        })
}

fn parse_js_optional_u64_array_field(
    object: &serde_json::Map<String, serde_json::Value>,
    key: &str,
    context: &str,
) -> Result<Vec<u64>> {
    match js_non_null_field(object, key) {
        None => Ok(Vec::new()),
        Some(value) => js_array(value, context)?
            .iter()
            .enumerate()
            .map(|(index, value)| js_number_to_u64(value, &format!("{}[{}]", context, index)))
            .collect(),
    }
}

fn parse_js_optional_u32_array_field(
    object: &serde_json::Map<String, serde_json::Value>,
    key: &str,
    context: &str,
) -> Result<Option<Vec<u32>>> {
    match js_non_null_field(object, key) {
        None => Ok(None),
        Some(value) => Ok(Some(
            js_array(value, context)?
                .iter()
                .enumerate()
                .map(|(index, value)| {
                    let value = js_number_to_u64(value, &format!("{}[{}]", context, index))?;
                    u32::try_from(value).map_err(|_| {
                        napi::Error::from_reason(format!("{}[{}] must fit in u32", context, index))
                    })
                })
                .collect::<Result<Vec<_>>>()?,
        )),
    }
}

fn parse_js_optional_string_array_field(
    object: &serde_json::Map<String, serde_json::Value>,
    key: &str,
    context: &str,
) -> Result<Vec<String>> {
    match js_non_null_field(object, key) {
        None => Ok(Vec::new()),
        Some(value) => js_array(value, context)?
            .iter()
            .enumerate()
            .map(|(index, value)| {
                value.as_str().map(ToString::to_string).ok_or_else(|| {
                    napi::Error::from_reason(format!("{}[{}] must be a string", context, index))
                })
            })
            .collect(),
    }
}

fn js_number_to_u64(value: &serde_json::Value, context: &str) -> Result<u64> {
    let number = value
        .as_f64()
        .ok_or_else(|| napi::Error::from_reason(format!("{} must be a number", context)))?;
    f64_to_u64(number)
}

fn js_number_to_i64(value: &serde_json::Value, context: &str) -> Result<i64> {
    let number = value
        .as_f64()
        .ok_or_else(|| napi::Error::from_reason(format!("{} must be a number", context)))?;
    if !number.is_finite()
        || number.fract() != 0.0
        || number < i64::MIN as f64
        || number > i64::MAX as f64
    {
        return Err(napi::Error::from_reason(format!(
            "{} must be a finite integer",
            context
        )));
    }
    Ok(number as i64)
}

fn has_any_js_field(object: &serde_json::Map<String, serde_json::Value>, fields: &[&str]) -> bool {
    fields.iter().any(|field| object.contains_key(*field))
}

fn ensure_only_js_fields(
    object: &serde_json::Map<String, serde_json::Value>,
    allowed: &[&str],
    context: &str,
) -> Result<()> {
    for field in object.keys() {
        if !allowed.iter().any(|allowed| *allowed == field) {
            return Err(napi::Error::from_reason(format!(
                "{} does not accept field '{}'",
                context, field
            )));
        }
    }
    Ok(())
}

fn ensure_no_js_fields(
    object: &serde_json::Map<String, serde_json::Value>,
    fields: &[&str],
    context: &str,
) -> Result<()> {
    for field in fields {
        if object.contains_key(*field) {
            return Err(napi::Error::from_reason(format!(
                "{} does not accept field '{}'",
                context, field
            )));
        }
    }
    Ok(())
}

fn require_js_true_field(
    object: &serde_json::Map<String, serde_json::Value>,
    field: &str,
    context: &str,
) -> Result<()> {
    match object.get(field).and_then(serde_json::Value::as_bool) {
        Some(true) => Ok(()),
        _ => Err(napi::Error::from_reason(format!(
            "{} {} must be true",
            context, field
        ))),
    }
}

fn reject_js_uppercase_filter_fields(
    object: &serde_json::Map<String, serde_json::Value>,
    context: &str,
) -> Result<()> {
    for field in object.keys() {
        if matches!(
            field.as_str(),
            "AND" | "OR" | "NOT" | "Eq" | "In" | "Exists" | "Missing"
        ) {
            return Err(napi::Error::from_reason(format!(
                "{} uses unsupported uppercase filter field '{}'",
                context, field
            )));
        }
    }
    Ok(())
}

fn make_property_range_page_request(
    options: Option<JsFindNodesRangePagedOptions>,
) -> Result<PropertyRangePageRequest> {
    let (limit, after) = match options {
        Some(options) => (options.limit, options.after),
        None => (None, None),
    };
    Ok(PropertyRangePageRequest {
        limit: limit.map(|value| value as usize),
        after: after.map(js_property_range_cursor_to_rust).transpose()?,
    })
}

#[napi(object)]
pub struct JsCompactionProgress {
    pub phase: String,
    pub segments_processed: u32,
    pub total_segments: u32,
    pub records_processed: i64,
    pub total_records: i64,
}

#[napi(object)]
pub struct JsCompactionStats {
    pub segments_merged: u32,
    pub nodes_kept: i64,
    pub nodes_removed: i64,
    pub edges_kept: i64,
    pub edges_removed: i64,
    pub duration_ms: i64,
    pub output_segment_id: i64,
    /// Number of nodes auto-pruned by registered compaction policies.
    pub nodes_auto_pruned: i64,
    /// Number of edges cascade-dropped due to auto-pruned nodes.
    pub edges_auto_pruned: i64,
}

impl From<CompactionStats> for JsCompactionStats {
    fn from(s: CompactionStats) -> Self {
        // All casts are safe: segment counts are small, and node/edge counts from compaction
        // never approach i64::MAX in practice (sequential IDs from 1).
        debug_assert!(s.segments_merged <= u32::MAX as usize);
        debug_assert!(s.nodes_kept <= i64::MAX as u64);
        debug_assert!(s.nodes_removed <= i64::MAX as u64);
        debug_assert!(s.edges_kept <= i64::MAX as u64);
        debug_assert!(s.edges_removed <= i64::MAX as u64);
        debug_assert!(s.duration_ms <= i64::MAX as u64);
        debug_assert!(s.output_segment_id <= i64::MAX as u64);
        JsCompactionStats {
            segments_merged: s.segments_merged as u32,
            nodes_kept: s.nodes_kept as i64,
            nodes_removed: s.nodes_removed as i64,
            edges_kept: s.edges_kept as i64,
            edges_removed: s.edges_removed as i64,
            duration_ms: s.duration_ms as i64,
            output_segment_id: s.output_segment_id as i64,
            nodes_auto_pruned: s.nodes_auto_pruned as i64,
            edges_auto_pruned: s.edges_auto_pruned as i64,
        }
    }
}

#[napi(object)]
pub struct JsPrunePolicy {
    /// Prune nodes older than this many milliseconds. Optional.
    pub max_age_ms: Option<f64>,
    /// Prune nodes with weight <= this threshold. Optional.
    pub max_weight: Option<f64>,
    /// Scope to a single node type. Optional.
    pub type_id: Option<u32>,
}

#[napi(object)]
pub struct JsNamedPrunePolicy {
    pub name: String,
    pub policy: JsPrunePolicy,
}

#[napi(object)]
pub struct JsPruneResult {
    /// Number of nodes pruned.
    pub nodes_pruned: i64,
    /// Number of edges cascade-deleted.
    pub edges_pruned: i64,
}

#[napi(object)]
pub struct JsEdgeInvalidation {
    pub edge_id: f64,
    pub valid_to: i64,
}

#[napi(object)]
pub struct JsGraphPatch {
    pub upsert_nodes: Option<Vec<JsNodeInput>>,
    pub upsert_edges: Option<Vec<JsEdgeInput>>,
    pub invalidate_edges: Option<Vec<JsEdgeInvalidation>>,
    pub delete_node_ids: Option<Vec<f64>>,
    pub delete_edge_ids: Option<Vec<f64>>,
}

#[napi(object)]
pub struct JsPatchResult {
    pub node_ids: Float64Array,
    pub edge_ids: Float64Array,
}

// --- PPR types ---

#[napi(object)]
pub struct JsPprResult {
    pub node_ids: Float64Array,
    pub scores: Float64Array,
    pub iterations: u32,
    pub converged: bool,
    pub algorithm: String,
    pub approx: Option<JsPprApproxMeta>,
}

#[napi(object)]
pub struct JsPprApproxMeta {
    pub residual_tolerance: f64,
    pub pushes: f64,
    pub max_remaining_residual: f64,
}

fn ppr_result_to_js(r: PprResult) -> Result<JsPprResult> {
    let mut node_ids_raw = Vec::with_capacity(r.scores.len());
    let mut scores = Vec::with_capacity(r.scores.len());
    for (id, score) in &r.scores {
        node_ids_raw.push(u64_to_f64(*id)?);
        scores.push(*score);
    }
    Ok(JsPprResult {
        node_ids: Float64Array::new(node_ids_raw),
        scores: Float64Array::new(scores),
        iterations: r.iterations,
        converged: r.converged,
        algorithm: ppr_algorithm_to_js(r.algorithm).to_string(),
        approx: r.approx.map(|a| JsPprApproxMeta {
            residual_tolerance: a.residual_tolerance,
            pushes: a.pushes as f64,
            max_remaining_residual: a.max_remaining_residual,
        }),
    })
}

fn js_ppr_options_to_ppr_options(
    algorithm: Option<&str>,
    damping_factor: &Option<f64>,
    max_iterations: &Option<u32>,
    epsilon: &Option<f64>,
    approx_residual_tolerance: &Option<f64>,
    edge_type_filter: &Option<Vec<u32>>,
    max_results: &Option<u32>,
) -> Result<PprOptions> {
    let defaults = PprOptions::default();
    Ok(PprOptions {
        algorithm: parse_ppr_algorithm(algorithm)?,
        damping_factor: damping_factor.unwrap_or(0.85),
        max_iterations: max_iterations.unwrap_or(20),
        epsilon: epsilon.unwrap_or(1e-6),
        approx_residual_tolerance: approx_residual_tolerance
            .unwrap_or(defaults.approx_residual_tolerance),
        edge_type_filter: edge_type_filter.clone(),
        max_results: max_results.map(|v| v as usize),
    })
}

// --- Export types ---

#[napi(object)]
pub struct JsExportOptions {
    pub node_type_filter: Option<Vec<u32>>,
    pub edge_type_filter: Option<Vec<u32>>,
    pub include_weights: Option<bool>,
}

#[napi(object)]
pub struct JsAdjacencyExport {
    pub node_ids: Float64Array,
    pub edge_from: Float64Array,
    pub edge_to: Float64Array,
    pub edge_type_ids: Uint32Array,
    pub edge_weights: Option<Float64Array>,
}

fn adjacency_export_to_js(r: AdjacencyExport, include_weights: bool) -> Result<JsAdjacencyExport> {
    let node_ids_vec: Vec<f64> = r
        .node_ids
        .iter()
        .map(|&id| u64_to_f64(id))
        .collect::<Result<Vec<_>>>()?;
    let node_ids = Float64Array::new(node_ids_vec);
    let mut from_raw = Vec::with_capacity(r.edges.len());
    let mut to_raw = Vec::with_capacity(r.edges.len());
    let mut type_ids = Vec::with_capacity(r.edges.len());
    let mut weights = Vec::with_capacity(r.edges.len());
    for &(f, t, tid, w) in &r.edges {
        from_raw.push(u64_to_f64(f)?);
        to_raw.push(u64_to_f64(t)?);
        type_ids.push(tid);
        weights.push(w as f64);
    }
    Ok(JsAdjacencyExport {
        node_ids,
        edge_from: Float64Array::new(from_raw),
        edge_to: Float64Array::new(to_raw),
        edge_type_ids: Uint32Array::new(type_ids),
        edge_weights: if include_weights {
            Some(Float64Array::new(weights))
        } else {
            None
        },
    })
}

fn js_export_options_to_rust(opts: Option<JsExportOptions>) -> ExportOptions {
    match opts {
        None => ExportOptions::default(),
        Some(o) => ExportOptions {
            node_type_filter: o.node_type_filter,
            edge_type_filter: o.edge_type_filter,
            include_weights: o.include_weights.unwrap_or(true),
        },
    }
}

fn js_patch_to_rust(patch: JsGraphPatch) -> napi::Result<GraphPatch> {
    let upsert_nodes: Vec<NodeInput> = patch
        .upsert_nodes
        .unwrap_or_default()
        .into_iter()
        .map(|n| n.into())
        .collect();

    let upsert_edges: Vec<EdgeInput> = patch
        .upsert_edges
        .unwrap_or_default()
        .into_iter()
        .map(|e| e.try_into())
        .collect::<std::result::Result<Vec<_>, _>>()?;

    let invalidate_edges: Vec<(u64, i64)> = patch
        .invalidate_edges
        .unwrap_or_default()
        .into_iter()
        .map(|inv| Ok((f64_to_u64(inv.edge_id)?, inv.valid_to)))
        .collect::<napi::Result<Vec<_>>>()?;

    let delete_node_ids: Vec<u64> = patch
        .delete_node_ids
        .unwrap_or_default()
        .into_iter()
        .map(f64_to_u64)
        .collect::<napi::Result<Vec<_>>>()?;

    let delete_edge_ids: Vec<u64> = patch
        .delete_edge_ids
        .unwrap_or_default()
        .into_iter()
        .map(f64_to_u64)
        .collect::<napi::Result<Vec<_>>>()?;

    Ok(GraphPatch {
        upsert_nodes,
        upsert_edges,
        invalidate_edges,
        delete_node_ids,
        delete_edge_ids,
    })
}

// ============================================================
// Async task types
// ============================================================

/// Generic async task for write operations: runs on the libuv thread pool
/// using a cloned shared engine handle, without holding the wrapper lock.
pub struct EngineOp<T: Send + 'static, J: ToNapiValue + TypeName + 'static> {
    db: Arc<Mutex<Option<InnerDb>>>,
    op: Option<Box<dyn FnOnce(&DatabaseEngine) -> std::result::Result<T, EngineError> + Send>>,
    convert: fn(T) -> napi::Result<J>,
}

impl<T: Send + 'static, J: ToNapiValue + TypeName + 'static> EngineOp<T, J> {
    fn new(
        db: Arc<Mutex<Option<InnerDb>>>,
        op: impl FnOnce(&DatabaseEngine) -> std::result::Result<T, EngineError> + Send + 'static,
        convert: fn(T) -> napi::Result<J>,
    ) -> Self {
        Self {
            db,
            op: Some(Box::new(op)),
            convert,
        }
    }
}

impl<T: Send + 'static, J: ToNapiValue + TypeName + 'static> Task for EngineOp<T, J> {
    type Output = T;
    type JsValue = J;

    fn compute(&mut self) -> napi::Result<T> {
        let op = self.op.take().ok_or_else(|| {
            napi::Error::from_reason("EngineOp::compute called twice".to_string())
        })?;
        let engine = clone_engine_handle(&self.db)?;
        op(&engine).map_err(|e| napi::Error::from_reason(e.to_string()))
    }

    fn resolve(&mut self, _env: Env, output: T) -> napi::Result<J> {
        (self.convert)(output)
    }
}

/// Generic async task for read-only operations: runs on the libuv thread pool
/// using a cloned shared engine handle, without holding the wrapper lock.
pub struct EngineReadOp<T: Send + 'static, J: ToNapiValue + TypeName + 'static> {
    db: Arc<Mutex<Option<InnerDb>>>,
    op: Option<Box<dyn FnOnce(&DatabaseEngine) -> std::result::Result<T, EngineError> + Send>>,
    convert: fn(T) -> napi::Result<J>,
}

impl<T: Send + 'static, J: ToNapiValue + TypeName + 'static> EngineReadOp<T, J> {
    fn new(
        db: Arc<Mutex<Option<InnerDb>>>,
        op: impl FnOnce(&DatabaseEngine) -> std::result::Result<T, EngineError> + Send + 'static,
        convert: fn(T) -> napi::Result<J>,
    ) -> Self {
        Self {
            db,
            op: Some(Box::new(op)),
            convert,
        }
    }
}

impl<T: Send + 'static, J: ToNapiValue + TypeName + 'static> Task for EngineReadOp<T, J> {
    type Output = T;
    type JsValue = J;

    fn compute(&mut self) -> napi::Result<T> {
        let op = self.op.take().ok_or_else(|| {
            napi::Error::from_reason("EngineReadOp::compute called twice".to_string())
        })?;
        let engine = clone_engine_handle(&self.db)?;
        op(&engine).map_err(|e| napi::Error::from_reason(e.to_string()))
    }

    fn resolve(&mut self, _env: Env, output: T) -> napi::Result<J> {
        (self.convert)(output)
    }
}

/// Close task: takes ownership of the engine to call close(self) or close_fast(self).
pub struct CloseOp {
    db: Arc<Mutex<Option<InnerDb>>>,
    force: bool,
}

impl Task for CloseOp {
    type Output = ();
    type JsValue = ();

    fn compute(&mut self) -> napi::Result<()> {
        let engine = {
            let mut guard = self
                .db
                .lock()
                .map_err(|e| napi::Error::from_reason(e.to_string()))?;
            guard.take().map(|db| db.engine)
        };
        if let Some(engine) = engine {
            let result = if self.force {
                engine.close_fast()
            } else {
                engine.close()
            };
            result.map_err(|e| napi::Error::from_reason(e.to_string()))?;
        }
        Ok(())
    }

    fn resolve(&mut self, _env: Env, _output: ()) -> napi::Result<()> {
        Ok(())
    }
}

/// Async compaction with progress: runs on the libuv thread pool,
/// sends progress updates to the JS main thread via ThreadsafeFunction.
/// Progress callback is fire-and-forget (void return, no cancellation).
pub struct CompactProgressOp {
    db: Arc<Mutex<Option<InnerDb>>>,
    tsfn: ProgressTsfn,
}

impl Task for CompactProgressOp {
    type Output = Option<CompactionStats>;
    type JsValue = Option<JsCompactionStats>;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        let engine = clone_engine_handle(&self.db)?;
        let tsfn = &self.tsfn;
        let result = engine.compact_with_progress(|progress| {
            let js_progress = JsCompactionProgress {
                phase: match progress.phase {
                    CompactionPhase::CollectingTombstones => "collecting_tombstones".to_string(),
                    CompactionPhase::MergingNodes => "merging_nodes".to_string(),
                    CompactionPhase::MergingEdges => "merging_edges".to_string(),
                    CompactionPhase::WritingOutput => "writing_output".to_string(),
                },
                segments_processed: progress.segments_processed as u32,
                total_segments: progress.total_segments as u32,
                records_processed: progress.records_processed as i64,
                total_records: progress.total_records as i64,
            };
            // Fire-and-forget: always continue (no cancellation in async mode)
            let _ = tsfn.call(js_progress, ThreadsafeFunctionCallMode::NonBlocking);
            true
        });

        result.map_err(|e| napi::Error::from_reason(e.to_string()))
    }

    fn resolve(&mut self, _env: Env, output: Self::Output) -> napi::Result<Self::JsValue> {
        Ok(output.map(|s| s.into()))
    }
}

/// Async task for stateful transaction operations. Tickets preserve JS call order
/// even when libuv schedules multiple operations on the same transaction in parallel.
pub struct TxnAsyncOp<T: Send + 'static, J: ToNapiValue + TypeName + 'static> {
    inner: Arc<Mutex<Option<WriteTxn>>>,
    order: Arc<TxnAsyncOrder>,
    ticket: u64,
    op: Option<Box<dyn FnOnce(&mut WriteTxn) -> std::result::Result<T, EngineError> + Send>>,
    convert: fn(T) -> napi::Result<J>,
}

impl<T: Send + 'static, J: ToNapiValue + TypeName + 'static> TxnAsyncOp<T, J> {
    fn new(
        txn: &JsWriteTxn,
        op: impl FnOnce(&mut WriteTxn) -> std::result::Result<T, EngineError> + Send + 'static,
        convert: fn(T) -> napi::Result<J>,
    ) -> Result<Self> {
        let ticket = txn.async_order.reserve_ticket()?;
        Ok(Self {
            inner: txn.inner.clone(),
            order: txn.async_order.clone(),
            ticket,
            op: Some(Box::new(op)),
            convert,
        })
    }
}

impl<T: Send + 'static, J: ToNapiValue + TypeName + 'static> Task for TxnAsyncOp<T, J> {
    type Output = T;
    type JsValue = J;

    fn compute(&mut self) -> napi::Result<T> {
        let _turn = self.order.wait_turn(self.ticket)?;
        let op = self.op.take().ok_or_else(|| {
            napi::Error::from_reason("TxnAsyncOp::compute called twice".to_string())
        })?;
        let mut guard = self
            .inner
            .lock()
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        let txn = guard
            .as_mut()
            .ok_or_else(|| napi::Error::from_reason(EngineError::TxnClosed.to_string()))?;
        op(txn).map_err(|e| napi::Error::from_reason(e.to_string()))
    }

    fn resolve(&mut self, _env: Env, output: T) -> napi::Result<J> {
        (self.convert)(output)
    }
}

/// Async task for transaction operations that consume the transaction handle.
pub struct TxnAsyncTakeOp<T: Send + 'static, J: ToNapiValue + TypeName + 'static> {
    inner: Arc<Mutex<Option<WriteTxn>>>,
    order: Arc<TxnAsyncOrder>,
    ticket: u64,
    op: Option<Box<dyn FnOnce(&mut WriteTxn) -> std::result::Result<T, EngineError> + Send>>,
    convert: fn(T) -> napi::Result<J>,
}

impl<T: Send + 'static, J: ToNapiValue + TypeName + 'static> TxnAsyncTakeOp<T, J> {
    fn new(
        txn: &JsWriteTxn,
        op: impl FnOnce(&mut WriteTxn) -> std::result::Result<T, EngineError> + Send + 'static,
        convert: fn(T) -> napi::Result<J>,
    ) -> Result<Self> {
        let ticket = txn.async_order.reserve_ticket()?;
        Ok(Self {
            inner: txn.inner.clone(),
            order: txn.async_order.clone(),
            ticket,
            op: Some(Box::new(op)),
            convert,
        })
    }
}

impl<T: Send + 'static, J: ToNapiValue + TypeName + 'static> Task for TxnAsyncTakeOp<T, J> {
    type Output = T;
    type JsValue = J;

    fn compute(&mut self) -> napi::Result<T> {
        let _turn = self.order.wait_turn(self.ticket)?;
        let op = self.op.take().ok_or_else(|| {
            napi::Error::from_reason("TxnAsyncTakeOp::compute called twice".to_string())
        })?;
        let mut txn = {
            let mut guard = self
                .inner
                .lock()
                .map_err(|e| napi::Error::from_reason(e.to_string()))?;
            guard
                .take()
                .ok_or_else(|| napi::Error::from_reason(EngineError::TxnClosed.to_string()))?
        };
        op(&mut txn).map_err(|e| napi::Error::from_reason(e.to_string()))
    }

    fn resolve(&mut self, _env: Env, output: T) -> napi::Result<J> {
        (self.convert)(output)
    }
}

// ============================================================
// Helpers
// ============================================================

fn napi_identity<T>(value: T) -> Result<T> {
    Ok(value)
}

fn with_engine<F, T>(db: &OverGraph, f: F) -> Result<T>
where
    F: FnOnce(&DatabaseEngine) -> std::result::Result<T, EngineError>,
{
    let engine = clone_engine_handle(&db.inner)?;
    f(&engine).map_err(|e| napi::Error::from_reason(e.to_string()))
}

fn with_engine_ref<F, T>(db: &OverGraph, f: F) -> Result<T>
where
    F: FnOnce(&DatabaseEngine) -> std::result::Result<T, EngineError>,
{
    let engine = clone_engine_handle(&db.inner)?;
    f(&engine).map_err(|e| napi::Error::from_reason(e.to_string()))
}

fn clone_engine_handle(db: &Arc<Mutex<Option<InnerDb>>>) -> Result<DatabaseEngine> {
    let guard = db
        .lock()
        .map_err(|e| napi::Error::from_reason(e.to_string()))?;
    let inner = guard
        .as_ref()
        .ok_or_else(|| napi::Error::from_reason("Database is closed".to_string()))?;
    Ok(inner.engine.clone())
}

fn with_txn<F, T>(inner: &Arc<Mutex<Option<WriteTxn>>>, f: F) -> Result<T>
where
    F: FnOnce(&mut WriteTxn) -> std::result::Result<T, EngineError>,
{
    let mut guard = inner
        .lock()
        .map_err(|e| napi::Error::from_reason(e.to_string()))?;
    let txn = guard
        .as_mut()
        .ok_or_else(|| napi::Error::from_reason(EngineError::TxnClosed.to_string()))?;
    f(txn).map_err(|e| napi::Error::from_reason(e.to_string()))
}

fn with_txn_ref<F, T>(inner: &Arc<Mutex<Option<WriteTxn>>>, f: F) -> Result<T>
where
    F: FnOnce(&WriteTxn) -> std::result::Result<T, EngineError>,
{
    let guard = inner
        .lock()
        .map_err(|e| napi::Error::from_reason(e.to_string()))?;
    let txn = guard
        .as_ref()
        .ok_or_else(|| napi::Error::from_reason(EngineError::TxnClosed.to_string()))?;
    f(txn).map_err(|e| napi::Error::from_reason(e.to_string()))
}

fn with_txn_take<F, T>(inner: &Arc<Mutex<Option<WriteTxn>>>, f: F) -> Result<T>
where
    F: FnOnce(&mut WriteTxn) -> std::result::Result<T, EngineError>,
{
    let mut txn = {
        let mut guard = inner
            .lock()
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        guard
            .take()
            .ok_or_else(|| napi::Error::from_reason(EngineError::TxnClosed.to_string()))?
    };
    f(&mut txn).map_err(|e| napi::Error::from_reason(e.to_string()))
}

fn js_upsert_node_options(options: Option<JsUpsertNodeOptions>) -> UpsertNodeOptions {
    let (props, weight, dense_vector, sparse_vector) = match options {
        Some(o) => (o.props, o.weight, o.dense_vector, o.sparse_vector),
        None => (None, None, None, None),
    };
    UpsertNodeOptions {
        props: convert_js_props(props),
        weight: weight.unwrap_or(1.0) as f32,
        dense_vector: dense_vector.map(|dv| dv.into_iter().map(|x| x as f32).collect()),
        sparse_vector: sparse_vector.map(|sv| {
            sv.into_iter()
                .map(|e| (e.dimension, e.value as f32))
                .collect()
        }),
    }
}

fn js_upsert_edge_options(options: Option<JsUpsertEdgeOptions>) -> UpsertEdgeOptions {
    let (props, weight, valid_from, valid_to) = match options {
        Some(o) => (o.props, o.weight, o.valid_from, o.valid_to),
        None => (None, None, None, None),
    };
    UpsertEdgeOptions {
        props: convert_js_props(props),
        weight: weight.unwrap_or(1.0) as f32,
        valid_from,
        valid_to,
    }
}

fn js_txn_node_ref_to_rust(value: JsTxnNodeRef) -> Result<TxnNodeRef> {
    let has_id = value.id.is_some();
    let has_key = value.type_id.is_some() || value.key.is_some();
    let has_local = value.local.is_some();
    match (has_id, has_key, has_local) {
        (true, false, false) => Ok(TxnNodeRef::Id(f64_to_u64(value.id.unwrap())?)),
        (false, true, false) => Ok(TxnNodeRef::Key {
            type_id: value.type_id.ok_or_else(|| {
                napi::Error::from_reason("node key ref requires typeId".to_string())
            })?,
            key: value
                .key
                .ok_or_else(|| napi::Error::from_reason("node key ref requires key".to_string()))?,
        }),
        (false, false, true) => Ok(TxnNodeRef::Local(TxnLocalRef::Alias(value.local.unwrap()))),
        _ => Err(napi::Error::from_reason(
            "node ref must be exactly one of { id }, { typeId, key }, or { local }".to_string(),
        )),
    }
}

fn js_txn_edge_ref_to_rust(value: JsTxnEdgeRef) -> Result<TxnEdgeRef> {
    let has_id = value.id.is_some();
    let has_triple = value.from.is_some() || value.to.is_some() || value.type_id.is_some();
    let has_local = value.local.is_some();
    match (has_id, has_triple, has_local) {
        (true, false, false) => Ok(TxnEdgeRef::Id(f64_to_u64(value.id.unwrap())?)),
        (false, true, false) => Ok(TxnEdgeRef::Triple {
            from: js_txn_node_ref_to_rust(value.from.ok_or_else(|| {
                napi::Error::from_reason("edge triple ref requires from".to_string())
            })?)?,
            to: js_txn_node_ref_to_rust(value.to.ok_or_else(|| {
                napi::Error::from_reason("edge triple ref requires to".to_string())
            })?)?,
            type_id: value.type_id.ok_or_else(|| {
                napi::Error::from_reason("edge triple ref requires typeId".to_string())
            })?,
        }),
        (false, false, true) => Ok(TxnEdgeRef::Local(TxnLocalRef::Alias(value.local.unwrap()))),
        _ => Err(napi::Error::from_reason(
            "edge ref must be exactly one of { id }, { from, to, typeId }, or { local }"
                .to_string(),
        )),
    }
}

fn txn_node_ref_to_js(value: TxnNodeRef) -> Result<JsTxnNodeRef> {
    match value {
        TxnNodeRef::Id(id) => Ok(JsTxnNodeRef {
            id: Some(u64_to_f64(id)?),
            type_id: None,
            key: None,
            local: None,
        }),
        TxnNodeRef::Key { type_id, key } => Ok(JsTxnNodeRef {
            id: None,
            type_id: Some(type_id),
            key: Some(key),
            local: None,
        }),
        TxnNodeRef::Local(local) => Ok(JsTxnNodeRef {
            id: None,
            type_id: None,
            key: None,
            local: txn_local_ref_to_js(local),
        }),
    }
}

fn txn_edge_ref_to_js(value: TxnEdgeRef) -> Result<JsTxnEdgeRef> {
    match value {
        TxnEdgeRef::Id(id) => Ok(JsTxnEdgeRef {
            id: Some(u64_to_f64(id)?),
            from: None,
            to: None,
            type_id: None,
            local: None,
        }),
        TxnEdgeRef::Triple { from, to, type_id } => Ok(JsTxnEdgeRef {
            id: None,
            from: Some(txn_node_ref_to_js(from)?),
            to: Some(txn_node_ref_to_js(to)?),
            type_id: Some(type_id),
            local: None,
        }),
        TxnEdgeRef::Local(local) => Ok(JsTxnEdgeRef {
            id: None,
            from: None,
            to: None,
            type_id: None,
            local: txn_local_ref_to_js(local),
        }),
    }
}

fn txn_local_ref_to_js(local: TxnLocalRef) -> Option<String> {
    match local {
        TxnLocalRef::Alias(alias) => Some(alias),
        TxnLocalRef::Slot(_) => None,
    }
}

fn txn_node_view_to_js(view: TxnNodeView) -> Result<JsTxnNodeView> {
    Ok(JsTxnNodeView {
        id: view.id.map(u64_to_f64).transpose()?,
        local: view.local.and_then(txn_local_ref_to_js),
        type_id: view.type_id,
        key: view.key,
        props: props_to_json(view.props),
        created_at: view.created_at,
        updated_at: view.updated_at,
        weight: view.weight as f64,
        dense_vector: view
            .dense_vector
            .map(|v| v.into_iter().map(|x| x as f64).collect()),
        sparse_vector: view.sparse_vector.map(|v| {
            v.into_iter()
                .map(|(dimension, value)| JsSparseEntry {
                    dimension,
                    value: value as f64,
                })
                .collect()
        }),
    })
}

fn txn_edge_view_to_js(view: TxnEdgeView) -> Result<JsTxnEdgeView> {
    Ok(JsTxnEdgeView {
        id: view.id.map(u64_to_f64).transpose()?,
        local: view.local.and_then(txn_local_ref_to_js),
        from: txn_node_ref_to_js(view.from)?,
        to: txn_node_ref_to_js(view.to)?,
        type_id: view.type_id,
        props: props_to_json(view.props),
        created_at: view.created_at,
        updated_at: view.updated_at,
        weight: view.weight as f64,
        valid_from: view.valid_from,
        valid_to: view.valid_to,
    })
}

fn js_txn_operation_to_rust(op: JsTxnOperation) -> Result<TxnIntent> {
    match op.op.as_str() {
        "upsertNode" => Ok(TxnIntent::UpsertNode {
            alias: op.alias,
            type_id: op.type_id.ok_or_else(|| {
                napi::Error::from_reason("upsertNode requires typeId".to_string())
            })?,
            key: op
                .key
                .ok_or_else(|| napi::Error::from_reason("upsertNode requires key".to_string()))?,
            options: UpsertNodeOptions {
                props: convert_js_props(op.props),
                weight: op.weight.unwrap_or(1.0) as f32,
                dense_vector: op
                    .dense_vector
                    .map(|v| v.into_iter().map(|x| x as f32).collect()),
                sparse_vector: op.sparse_vector.map(|v| {
                    v.into_iter()
                        .map(|e| (e.dimension, e.value as f32))
                        .collect()
                }),
            },
        }),
        "upsertEdge" => {
            Ok(TxnIntent::UpsertEdge {
                alias: op.alias,
                from: js_txn_node_ref_to_rust(op.from.ok_or_else(|| {
                    napi::Error::from_reason("upsertEdge requires from".to_string())
                })?)?,
                to: js_txn_node_ref_to_rust(op.to.ok_or_else(|| {
                    napi::Error::from_reason("upsertEdge requires to".to_string())
                })?)?,
                type_id: op.type_id.ok_or_else(|| {
                    napi::Error::from_reason("upsertEdge requires typeId".to_string())
                })?,
                options: UpsertEdgeOptions {
                    props: convert_js_props(op.props),
                    weight: op.weight.unwrap_or(1.0) as f32,
                    valid_from: op.valid_from,
                    valid_to: op.valid_to,
                },
            })
        }
        "deleteNode" => Ok(TxnIntent::DeleteNode {
            target: js_txn_node_ref_to_rust(txn_target_as_node(op.target)?)?,
        }),
        "deleteEdge" => Ok(TxnIntent::DeleteEdge {
            target: js_txn_edge_ref_to_rust(txn_target_as_edge(op.target)?)?,
        }),
        "invalidateEdge" => Ok(TxnIntent::InvalidateEdge {
            target: js_txn_edge_ref_to_rust(txn_target_as_edge(op.target)?)?,
            valid_to: op.valid_to.ok_or_else(|| {
                napi::Error::from_reason("invalidateEdge requires validTo".to_string())
            })?,
        }),
        other => Err(napi::Error::from_reason(format!(
            "invalid transaction op '{}'",
            other
        ))),
    }
}

fn txn_target_as_node(target: Option<JsTxnEdgeOrNodeRef>) -> Result<JsTxnNodeRef> {
    let target = target.ok_or_else(|| napi::Error::from_reason("operation requires target"))?;
    Ok(JsTxnNodeRef {
        id: target.id,
        type_id: target.type_id,
        key: target.key,
        local: target.local,
    })
}

fn txn_target_as_edge(target: Option<JsTxnEdgeOrNodeRef>) -> Result<JsTxnEdgeRef> {
    let target = target.ok_or_else(|| napi::Error::from_reason("operation requires target"))?;
    Ok(JsTxnEdgeRef {
        id: target.id,
        from: target.from,
        to: target.to,
        type_id: target.type_id,
        local: target.local,
    })
}

fn txn_commit_result_to_js(result: TxnCommitResult) -> Result<JsTxnCommitResult> {
    let node_aliases = result
        .local_node_ids
        .into_iter()
        .filter_map(|(local, id)| match local {
            TxnLocalRef::Alias(alias) => Some(u64_to_f64(id).map(|id| (alias, id))),
            TxnLocalRef::Slot(_) => None,
        })
        .collect::<Result<HashMap<_, _>>>()?;
    let edge_aliases = result
        .local_edge_ids
        .into_iter()
        .filter_map(|(local, id)| match local {
            TxnLocalRef::Alias(alias) => Some(u64_to_f64(id).map(|id| (alias, id))),
            TxnLocalRef::Slot(_) => None,
        })
        .collect::<Result<HashMap<_, _>>>()?;
    Ok(JsTxnCommitResult {
        node_ids: ids_to_float64_array(&result.node_ids)?,
        edge_ids: ids_to_float64_array(&result.edge_ids)?,
        node_aliases,
        edge_aliases,
    })
}

const MAX_SAFE_INTEGER: f64 = 9_007_199_254_740_991.0; // 2^53 - 1

fn f64_to_u64(v: f64) -> Result<u64> {
    if !(0.0..=MAX_SAFE_INTEGER).contains(&v) || v.fract() != 0.0 || v.is_nan() {
        return Err(napi::Error::from_reason(
            "ID must be a safe non-negative integer".to_string(),
        ));
    }
    Ok(v as u64)
}

const MAX_SAFE_U64: u64 = 9_007_199_254_740_991; // 2^53 - 1

fn u64_to_safe_i64(v: u64) -> Result<i64> {
    if v > MAX_SAFE_U64 {
        return Err(napi::Error::from_reason(
            "Value exceeds JavaScript safe integer range".to_string(),
        ));
    }
    Ok(v as i64)
}

fn parse_direction(s: Option<&str>) -> Result<Direction> {
    match s {
        None | Some("outgoing") => Ok(Direction::Outgoing),
        Some("incoming") => Ok(Direction::Incoming),
        Some("both") => Ok(Direction::Both),
        Some(other) => Err(napi::Error::from_reason(format!(
            "Invalid direction '{}'. Must be 'outgoing', 'incoming', or 'both'.",
            other
        ))),
    }
}

fn parse_scoring_mode(s: Option<&str>, decay_lambda: Option<f64>) -> Result<ScoringMode> {
    match s {
        None | Some("weight") => Ok(ScoringMode::Weight),
        Some("recency") => Ok(ScoringMode::Recency),
        Some("decay") => {
            let lambda = decay_lambda.ok_or_else(|| {
                napi::Error::from_reason("scoring='decay' requires decayLambda parameter")
            })? as f32;
            if lambda.is_nan() || lambda.is_infinite() {
                return Err(napi::Error::from_reason(
                    "decayLambda must be a finite non-negative number",
                ));
            }
            Ok(ScoringMode::DecayAdjusted { lambda })
        }
        Some(other) => Err(napi::Error::from_reason(format!(
            "Invalid scoring '{}'. Must be 'weight', 'recency', or 'decay'.",
            other
        ))),
    }
}

fn parse_vector_search_mode(s: &str) -> Result<VectorSearchMode> {
    match s {
        "dense" => Ok(VectorSearchMode::Dense),
        "sparse" => Ok(VectorSearchMode::Sparse),
        "hybrid" => Ok(VectorSearchMode::Hybrid),
        other => Err(napi::Error::from_reason(format!(
            "Invalid mode '{}'. Must be 'dense', 'sparse', or 'hybrid'.",
            other
        ))),
    }
}

fn parse_fusion_mode(s: Option<&str>) -> Result<Option<FusionMode>> {
    match s {
        None => Ok(None),
        Some("weighted_rank") => Ok(Some(FusionMode::WeightedRankFusion)),
        Some("reciprocal_rank") => Ok(Some(FusionMode::ReciprocalRankFusion)),
        Some("weighted_score") => Ok(Some(FusionMode::WeightedScoreFusion)),
        Some(other) => Err(napi::Error::from_reason(format!(
            "Invalid fusionMode '{}'. Must be 'weighted_rank', 'reciprocal_rank', or 'weighted_score'.",
            other
        ))),
    }
}

fn parse_ppr_algorithm(s: Option<&str>) -> Result<PprAlgorithm> {
    match s {
        None => Ok(PprAlgorithm::ExactPowerIteration),
        Some("exact") | Some("exact_power_iteration") => Ok(PprAlgorithm::ExactPowerIteration),
        Some("approx") | Some("approx_forward_push") => Ok(PprAlgorithm::ApproxForwardPush),
        Some(other) => Err(napi::Error::from_reason(format!(
            "Invalid PPR algorithm '{}'. Must be 'exact' or 'approx'.",
            other
        ))),
    }
}

fn ppr_algorithm_to_js(algorithm: PprAlgorithm) -> &'static str {
    match algorithm {
        PprAlgorithm::ExactPowerIteration => "exact",
        PprAlgorithm::ApproxForwardPush => "approx",
    }
}

fn parse_secondary_index_range_domain(s: Option<&str>) -> Result<SecondaryIndexRangeDomain> {
    match s {
        Some("int") => Ok(SecondaryIndexRangeDomain::Int),
        Some("uint") => Ok(SecondaryIndexRangeDomain::UInt),
        Some("float") => Ok(SecondaryIndexRangeDomain::Float),
        Some(other) => Err(napi::Error::from_reason(format!(
            "Invalid range domain '{}'. Must be 'int', 'uint', or 'float'.",
            other
        ))),
        None => Err(napi::Error::from_reason(
            "Range indexes require domain 'int', 'uint', or 'float'.".to_string(),
        )),
    }
}

fn secondary_index_domain_to_js(domain: SecondaryIndexRangeDomain) -> &'static str {
    match domain {
        SecondaryIndexRangeDomain::Int => "int",
        SecondaryIndexRangeDomain::UInt => "uint",
        SecondaryIndexRangeDomain::Float => "float",
    }
}

fn secondary_index_state_to_js(state: SecondaryIndexState) -> &'static str {
    match state {
        SecondaryIndexState::Building => "building",
        SecondaryIndexState::Ready => "ready",
        SecondaryIndexState::Failed => "failed",
    }
}

fn secondary_index_kind_to_js(kind: &SecondaryIndexKind) -> (String, Option<String>) {
    match kind {
        SecondaryIndexKind::Equality => ("equality".to_string(), None),
        SecondaryIndexKind::Range { domain } => (
            "range".to_string(),
            Some(secondary_index_domain_to_js(*domain).to_string()),
        ),
    }
}

fn js_secondary_index_kind_to_rust(kind: JsSecondaryIndexKind) -> Result<SecondaryIndexKind> {
    match kind.kind.as_str() {
        "equality" => {
            if kind.domain.is_some() {
                return Err(napi::Error::from_reason(
                    "Equality indexes do not accept a range domain.".to_string(),
                ));
            }
            Ok(SecondaryIndexKind::Equality)
        }
        "range" => Ok(SecondaryIndexKind::Range {
            domain: parse_secondary_index_range_domain(kind.domain.as_deref())?,
        }),
        other => Err(napi::Error::from_reason(format!(
            "Invalid index kind '{}'. Must be 'equality' or 'range'.",
            other
        ))),
    }
}

fn js_numeric_to_prop_value(value: f64, domain: SecondaryIndexRangeDomain) -> Result<PropValue> {
    match domain {
        SecondaryIndexRangeDomain::Int => {
            if !value.is_finite() || value.fract() != 0.0 || value.abs() > MAX_SAFE_INTEGER {
                return Err(napi::Error::from_reason(
                    "Int range values must be finite safe integers.".to_string(),
                ));
            }
            Ok(PropValue::Int(value as i64))
        }
        SecondaryIndexRangeDomain::UInt => {
            if !(0.0..=MAX_SAFE_INTEGER).contains(&value) || value.fract() != 0.0 {
                return Err(napi::Error::from_reason(
                    "UInt range values must be finite non-negative safe integers.".to_string(),
                ));
            }
            Ok(PropValue::UInt(value as u64))
        }
        SecondaryIndexRangeDomain::Float => {
            if !value.is_finite() {
                return Err(napi::Error::from_reason(
                    "Float range values must be finite numbers.".to_string(),
                ));
            }
            Ok(PropValue::Float(value))
        }
    }
}

fn prop_value_to_js_numeric_parts(value: &PropValue) -> Result<(f64, String)> {
    match value {
        PropValue::Int(value) => {
            let as_f64 = *value as f64;
            if !as_f64.is_finite() || as_f64.abs() > MAX_SAFE_INTEGER {
                return Err(napi::Error::from_reason(
                    "Int range values exceed JavaScript safe integer range.".to_string(),
                ));
            }
            Ok((as_f64, "int".to_string()))
        }
        PropValue::UInt(value) => Ok((u64_to_f64(*value)?, "uint".to_string())),
        PropValue::Float(value) if value.is_finite() => Ok((*value, "float".to_string())),
        _ => Err(napi::Error::from_reason(
            "Property range values must use Int, UInt, or finite Float.".to_string(),
        )),
    }
}

fn js_property_range_bound_to_rust(bound: &JsPropertyRangeBound) -> Result<PropertyRangeBound> {
    let domain = parse_secondary_index_range_domain(Some(bound.domain.as_str()))?;
    let value = js_numeric_to_prop_value(bound.value, domain)?;
    if bound.inclusive.unwrap_or(true) {
        Ok(PropertyRangeBound::Included(value))
    } else {
        Ok(PropertyRangeBound::Excluded(value))
    }
}

fn convert_js_props(
    props: Option<HashMap<String, serde_json::Value>>,
) -> BTreeMap<String, PropValue> {
    match props {
        None => BTreeMap::new(),
        Some(map) => map
            .into_iter()
            .map(|(k, v)| (k, json_to_prop_value(&v)))
            .collect(),
    }
}

fn json_to_prop_value(v: &serde_json::Value) -> PropValue {
    match v {
        serde_json::Value::Null => PropValue::Null,
        serde_json::Value::Bool(b) => PropValue::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                PropValue::Int(i)
            } else if let Some(f) = n.as_f64() {
                PropValue::Float(f)
            } else {
                PropValue::Null
            }
        }
        serde_json::Value::String(s) => PropValue::String(s.clone()),
        serde_json::Value::Array(arr) => {
            PropValue::Array(arr.iter().map(json_to_prop_value).collect())
        }
        serde_json::Value::Object(map) => PropValue::Map(
            map.iter()
                .map(|(k, v)| (k.clone(), json_to_prop_value(v)))
                .collect(),
        ),
    }
}

fn prop_value_to_json(v: PropValue) -> serde_json::Value {
    match v {
        PropValue::Null => serde_json::Value::Null,
        PropValue::Bool(b) => serde_json::Value::Bool(b),
        PropValue::Int(i) => serde_json::json!(i),
        PropValue::UInt(u) => serde_json::json!(u),
        PropValue::Float(f) => serde_json::json!(f),
        PropValue::String(s) => serde_json::Value::String(s),
        PropValue::Bytes(b) => serde_json::json!(b),
        PropValue::Array(arr) => {
            serde_json::Value::Array(arr.into_iter().map(prop_value_to_json).collect())
        }
        PropValue::Map(map) => serde_json::Value::Object(
            map.into_iter()
                .map(|(k, v)| (k, prop_value_to_json(v)))
                .collect(),
        ),
    }
}

fn props_to_json(props: BTreeMap<String, PropValue>) -> HashMap<String, serde_json::Value> {
    props
        .into_iter()
        .map(|(k, v)| (k, prop_value_to_json(v)))
        .collect()
}

/// Convert a u64 to f64, returning a JS error if it exceeds MAX_SAFE_INTEGER.
#[inline]
fn u64_to_f64(v: u64) -> Result<f64> {
    if v > MAX_SAFE_U64 {
        return Err(napi::Error::from_reason(
            "Value exceeds JavaScript safe integer range".to_string(),
        ));
    }
    Ok(v as f64)
}

fn ids_to_float64_array(ids: &[u64]) -> Result<Float64Array> {
    let floats: Vec<f64> = ids
        .iter()
        .map(|&id| u64_to_f64(id))
        .collect::<Result<Vec<_>>>()?;
    Ok(Float64Array::new(floats))
}

// ============================================================
// Binary batch decoding
// ============================================================

/// Cursor-based binary reader for packed batch buffers.
struct BinaryReader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> BinaryReader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn ensure(&self, n: usize) -> napi::Result<()> {
        if self.pos + n > self.buf.len() {
            Err(napi::Error::from_reason(format!(
                "Binary buffer truncated at offset {} (need {} bytes, have {})",
                self.pos,
                n,
                self.buf.len().saturating_sub(self.pos)
            )))
        } else {
            Ok(())
        }
    }

    fn read_u16_le(&mut self) -> napi::Result<u16> {
        self.ensure(2)?;
        let v = u16::from_le_bytes([self.buf[self.pos], self.buf[self.pos + 1]]);
        self.pos += 2;
        Ok(v)
    }

    fn read_u32_le(&mut self) -> napi::Result<u32> {
        self.ensure(4)?;
        let v = u32::from_le_bytes(self.buf[self.pos..self.pos + 4].try_into().unwrap());
        self.pos += 4;
        Ok(v)
    }

    fn read_f32_le(&mut self) -> napi::Result<f32> {
        self.ensure(4)?;
        let v = f32::from_le_bytes(self.buf[self.pos..self.pos + 4].try_into().unwrap());
        self.pos += 4;
        Ok(v)
    }

    fn read_u64_le(&mut self) -> napi::Result<u64> {
        self.ensure(8)?;
        let v = u64::from_le_bytes(self.buf[self.pos..self.pos + 8].try_into().unwrap());
        self.pos += 8;
        Ok(v)
    }

    fn read_i64_le(&mut self) -> napi::Result<i64> {
        self.ensure(8)?;
        let v = i64::from_le_bytes(self.buf[self.pos..self.pos + 8].try_into().unwrap());
        self.pos += 8;
        Ok(v)
    }

    fn read_bytes(&mut self, len: usize) -> napi::Result<&'a [u8]> {
        self.ensure(len)?;
        let slice = &self.buf[self.pos..self.pos + len];
        self.pos += len;
        Ok(slice)
    }

    fn read_utf8(&mut self, len: usize) -> napi::Result<&'a str> {
        let bytes = self.read_bytes(len)?;
        std::str::from_utf8(bytes)
            .map_err(|e| napi::Error::from_reason(format!("Invalid UTF-8 in key: {}", e)))
    }
}

/// Decode props from JSON bytes embedded in the binary buffer.
fn decode_props_json(reader: &mut BinaryReader) -> napi::Result<BTreeMap<String, PropValue>> {
    let props_len = reader.read_u32_le()? as usize;
    if props_len == 0 {
        return Ok(BTreeMap::new());
    }
    let props_bytes = reader.read_bytes(props_len)?;
    let json: serde_json::Value = serde_json::from_slice(props_bytes)
        .map_err(|e| napi::Error::from_reason(format!("Invalid props JSON: {}", e)))?;
    match json {
        serde_json::Value::Object(map) => Ok(map
            .into_iter()
            .map(|(k, v)| (k, json_to_prop_value(&v)))
            .collect()),
        _ => Err(napi::Error::from_reason(
            "Props must be a JSON object".to_string(),
        )),
    }
}

/// Decode a binary buffer into a Vec<NodeInput>.
///
/// Format (little-endian):
///   [count: u32]
///   per node:
///     [type_id: u32][weight: f32][key_len: u16][key: utf8][props_len: u32][props: json utf8]
fn decode_node_batch(buf: &[u8]) -> napi::Result<Vec<NodeInput>> {
    let mut reader = BinaryReader::new(buf);
    let count = reader.read_u32_le()? as usize;
    // Cap allocation: minimum node record is 14 bytes (type_id + weight + key_len + props_len)
    let max_possible = buf.len().saturating_sub(4) / 14;
    let mut inputs = Vec::with_capacity(count.min(max_possible));

    for _ in 0..count {
        let type_id = reader.read_u32_le()?;
        let weight = reader.read_f32_le()?;
        let key_len = reader.read_u16_le()? as usize;
        let key = reader.read_utf8(key_len)?.to_string();
        let props = decode_props_json(&mut reader)?;
        inputs.push(NodeInput {
            type_id,
            key,
            props,
            weight,
            dense_vector: None,
            sparse_vector: None,
        });
    }

    if reader.pos != reader.buf.len() {
        return Err(napi::Error::from_reason(format!(
            "Binary node buffer has {} trailing bytes after decoding {} items",
            reader.buf.len() - reader.pos,
            count
        )));
    }

    Ok(inputs)
}

/// Decode a binary buffer into a Vec<EdgeInput>.
///
/// Format (little-endian):
///   [count: u32]
///   per edge:
///     [from: u64][to: u64][type_id: u32][weight: f32]
///     [valid_from: i64][valid_to: i64]
///     [props_len: u32][props: json utf8]
///
/// Sentinel values: valid_from=0 → None (engine default), valid_to=0 → None (engine default).
fn decode_edge_batch(buf: &[u8]) -> napi::Result<Vec<EdgeInput>> {
    let mut reader = BinaryReader::new(buf);
    let count = reader.read_u32_le()? as usize;
    // Cap allocation: minimum edge record is 36 bytes (from + to + type_id + weight + valid_from + valid_to + props_len)
    let max_possible = buf.len().saturating_sub(4) / 36;
    let mut inputs = Vec::with_capacity(count.min(max_possible));

    for _ in 0..count {
        let from = reader.read_u64_le()?;
        let to = reader.read_u64_le()?;
        let type_id = reader.read_u32_le()?;
        let weight = reader.read_f32_le()?;
        let valid_from_raw = reader.read_i64_le()?;
        let valid_to_raw = reader.read_i64_le()?;
        let props = decode_props_json(&mut reader)?;
        inputs.push(EdgeInput {
            from,
            to,
            type_id,
            props,
            weight,
            valid_from: if valid_from_raw == 0 {
                None
            } else {
                Some(valid_from_raw)
            },
            valid_to: if valid_to_raw == 0 {
                None
            } else {
                Some(valid_to_raw)
            },
        });
    }

    if reader.pos != reader.buf.len() {
        return Err(napi::Error::from_reason(format!(
            "Binary edge buffer has {} trailing bytes after decoding {} items",
            reader.buf.len() - reader.pos,
            count
        )));
    }

    Ok(inputs)
}

fn neighbor_entries_to_js(entries: Vec<NeighborEntry>) -> Result<Vec<JsNeighborEntry>> {
    entries.iter().map(neighbor_to_js_entry).collect()
}

fn convert_batch_result(
    map: impl IntoIterator<Item = (u64, Vec<NeighborEntry>)>,
) -> Result<Vec<JsNeighborBatchEntry>> {
    let mut entries: Vec<JsNeighborBatchEntry> = map
        .into_iter()
        .map(|(query_id, neighbors)| {
            Ok(JsNeighborBatchEntry {
                query_node_id: u64_to_f64(query_id)?,
                neighbors: neighbor_entries_to_js(neighbors)?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    // Sort by query_node_id for deterministic output
    entries.sort_by(|a, b| a.query_node_id.total_cmp(&b.query_node_id));
    Ok(entries)
}
