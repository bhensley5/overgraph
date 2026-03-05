use overgraph::{
    AdjacencyExport, CompactionPhase, CompactionStats, DatabaseEngine, DbOptions, DbStats,
    Direction, EdgeInput, EdgeRecord, EngineError, ExportOptions, GraphPatch, NeighborEntry,
    NodeInput, NodeRecord, PageRequest, PageResult, PprOptions, PprResult, PropValue, PrunePolicy,
    PruneResult, ScoringMode, Subgraph, WalSyncMode,
};
use napi::bindgen_prelude::*;
use napi::threadsafe_function::ThreadsafeFunctionCallMode;
use napi_derive::napi;

/// ThreadsafeFunction with `CalleeHandled = false` so the JS callback
/// receives `(progress)` directly, not error-first `(null, progress)`.
type ProgressTsfn =
    napi::threadsafe_function::ThreadsafeFunction<JsCompactionProgress, Unknown<'static>, JsCompactionProgress, Status, false>;
use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::{Arc, Mutex};

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
        let mut guard = self
            .inner
            .lock()
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        if let Some(db) = guard.take() {
            if force {
                db.engine
                    .close_fast()
                    .map_err(|e| napi::Error::from_reason(e.to_string()))?;
            } else {
                db.engine
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
        props: Option<HashMap<String, serde_json::Value>>,
        weight: Option<f64>,
    ) -> Result<f64> {
        let props = convert_js_props(props);
        let w = weight.unwrap_or(1.0) as f32;
        let id = with_engine(self, |eng| eng.upsert_node(type_id, &key, props, w))?;
        Ok(u64_to_f64(id))
    }

    #[napi]
    pub fn upsert_edge(
        &self,
        from: f64,
        to: f64,
        type_id: u32,
        props: Option<HashMap<String, serde_json::Value>>,
        weight: Option<f64>,
        valid_from: Option<i64>,
        valid_to: Option<i64>,
    ) -> Result<f64> {
        let from = f64_to_u64(from)?;
        let to = f64_to_u64(to)?;
        let props = convert_js_props(props);
        let w = weight.unwrap_or(1.0) as f32;
        let id = with_engine(self, |eng| eng.upsert_edge(from, to, type_id, props, w, valid_from, valid_to))?;
        Ok(u64_to_f64(id))
    }

    // --- Batch upserts (JSON object path) ---

    #[napi]
    pub fn batch_upsert_nodes(&self, nodes: Vec<JsNodeInput>) -> Result<Float64Array> {
        let inputs: Vec<NodeInput> = nodes.into_iter().map(|n| n.into()).collect();
        let ids = with_engine(self, |eng| eng.batch_upsert_nodes(&inputs))?;
        Ok(ids_to_float64_array(&ids))
    }

    #[napi]
    pub fn batch_upsert_edges(&self, edges: Vec<JsEdgeInput>) -> Result<Float64Array> {
        let inputs: std::result::Result<Vec<EdgeInput>, _> =
            edges.into_iter().map(|e| e.try_into()).collect();
        let inputs = inputs?;
        let ids = with_engine(self, |eng| eng.batch_upsert_edges(&inputs))?;
        Ok(ids_to_float64_array(&ids))
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
        Ok(ids_to_float64_array(&ids))
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
        Ok(ids_to_float64_array(&ids))
    }

    // --- Gets ---

    #[napi]
    pub fn get_node(&self, id: f64) -> Result<Option<JsNodeRecord>> {
        let id = f64_to_u64(id)?;
        with_engine_ref(self, |eng| Ok(eng.get_node(id)?.map(|n| n.into())))
    }

    #[napi]
    pub fn get_edge(&self, id: f64) -> Result<Option<JsEdgeRecord>> {
        let id = f64_to_u64(id)?;
        with_engine_ref(self, |eng| Ok(eng.get_edge(id)?.map(|e| e.into())))
    }

    // --- Key/triple lookups ---

    #[napi]
    pub fn get_node_by_key(&self, type_id: u32, key: String) -> Result<Option<JsNodeRecord>> {
        with_engine_ref(self, |eng| Ok(eng.get_node_by_key(type_id, &key)?.map(|n| n.into())))
    }

    #[napi]
    pub fn get_edge_by_triple(&self, from: f64, to: f64, type_id: u32) -> Result<Option<JsEdgeRecord>> {
        let from = f64_to_u64(from)?;
        let to = f64_to_u64(to)?;
        with_engine_ref(self, |eng| Ok(eng.get_edge_by_triple(from, to, type_id)?.map(|e| e.into())))
    }

    // --- Bulk reads ---

    #[napi]
    pub fn get_nodes(&self, ids: Vec<f64>) -> Result<Vec<Option<JsNodeRecord>>> {
        let ids: Vec<u64> = ids.into_iter().map(f64_to_u64).collect::<Result<Vec<_>>>()?;
        with_engine_ref(self, |eng| {
            let results = eng.get_nodes(&ids)?;
            Ok(results.into_iter().map(|r| r.map(|n| n.into())).collect())
        })
    }

    #[napi]
    pub fn get_edges(&self, ids: Vec<f64>) -> Result<Vec<Option<JsEdgeRecord>>> {
        let ids: Vec<u64> = ids.into_iter().map(f64_to_u64).collect::<Result<Vec<_>>>()?;
        with_engine_ref(self, |eng| {
            let results = eng.get_edges(&ids)?;
            Ok(results.into_iter().map(|r| r.map(|e| e.into())).collect())
        })
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
        with_engine(self, |eng| Ok(eng.invalidate_edge(id, valid_to)?.map(|e| e.into())))
    }

    #[napi]
    pub fn graph_patch(&self, patch: JsGraphPatch) -> Result<JsPatchResult> {
        let rust_patch = js_patch_to_rust(patch)?;
        with_engine(self, |eng| {
            let result = eng.graph_patch(&rust_patch)?;
            Ok(JsPatchResult {
                node_ids: ids_to_float64_array(&result.node_ids),
                edge_ids: ids_to_float64_array(&result.edge_ids),
            })
        })
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
        with_engine(self, |eng| Ok(eng.remove_prune_policy(&name)?))
    }

    #[napi]
    pub fn list_prune_policies(&self) -> Result<Vec<JsNamedPrunePolicy>> {
        with_engine_ref(self, |eng| {
            Ok(eng
                .list_prune_policies()
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
            move |eng| Ok(eng.remove_prune_policy(&name)?),
            |v| Ok(v),
        )))
    }

    #[napi(ts_return_type = "Promise<Array<JsNamedPrunePolicy>>")]
    pub fn list_prune_policies_async(
        &self,
    ) -> Result<AsyncTask<EngineOp<Vec<(String, PrunePolicy)>, Vec<JsNamedPrunePolicy>>>> {
        Ok(AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| Ok(eng.list_prune_policies()),
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
        direction: Option<String>,
        type_filter: Option<Vec<u32>>,
        limit: Option<u32>,
        at_epoch: Option<i64>,
        decay_lambda: Option<f64>,
    ) -> Result<JsNeighborList> {
        let node_id = f64_to_u64(node_id)?;
        let dir = parse_direction(direction.as_deref())?;
        let lim = limit.unwrap_or(0) as usize;
        let decay = decay_lambda.map(|v| v as f32); // JS f64 → Rust f32 (sufficient precision for λ)
        with_engine_ref(self, |eng| {
            let entries = eng.neighbors(node_id, dir, type_filter.as_deref(), lim, at_epoch, decay)?;
            Ok(neighbor_entries_to_js(entries))
        })
    }

    #[napi]
    pub fn neighbors_2hop(
        &self,
        node_id: f64,
        direction: Option<String>,
        type_filter: Option<Vec<u32>>,
        limit: Option<u32>,
        at_epoch: Option<i64>,
        decay_lambda: Option<f64>,
    ) -> Result<JsNeighborList> {
        let node_id = f64_to_u64(node_id)?;
        let dir = parse_direction(direction.as_deref())?;
        let lim = limit.unwrap_or(0) as usize;
        let decay = decay_lambda.map(|v| v as f32); // JS f64 → Rust f32 (sufficient precision for λ)
        with_engine_ref(self, |eng| {
            let entries = eng.neighbors_2hop(node_id, dir, type_filter.as_deref(), lim, at_epoch, decay)?;
            Ok(neighbor_entries_to_js(entries))
        })
    }

    #[napi]
    pub fn neighbors_2hop_constrained(
        &self,
        node_id: f64,
        direction: Option<String>,
        traverse_edge_types: Option<Vec<u32>>,
        target_node_types: Option<Vec<u32>>,
        limit: Option<u32>,
        at_epoch: Option<i64>,
        decay_lambda: Option<f64>,
    ) -> Result<JsNeighborList> {
        let node_id = f64_to_u64(node_id)?;
        let dir = parse_direction(direction.as_deref())?;
        let lim = limit.unwrap_or(0) as usize;
        let decay = decay_lambda.map(|v| v as f32);
        with_engine_ref(self, |eng| {
            let entries = eng.neighbors_2hop_constrained(
                node_id,
                dir,
                traverse_edge_types.as_deref(),
                target_node_types.as_deref(),
                lim,
                at_epoch,
                decay,
            )?;
            Ok(neighbor_entries_to_js(entries))
        })
    }

    #[napi]
    pub fn top_k_neighbors(
        &self,
        node_id: f64,
        direction: Option<String>,
        type_filter: Option<Vec<u32>>,
        k: u32,
        scoring: Option<String>,
        decay_lambda: Option<f64>,
        at_epoch: Option<i64>,
    ) -> Result<JsNeighborList> {
        let node_id = f64_to_u64(node_id)?;
        let dir = parse_direction(direction.as_deref())?;
        let scoring_mode = parse_scoring_mode(scoring.as_deref(), decay_lambda)?;
        with_engine_ref(self, |eng| {
            let entries = eng.top_k_neighbors(
                node_id,
                dir,
                type_filter.as_deref(),
                k as usize,
                scoring_mode,
                at_epoch,
            )?;
            Ok(neighbor_entries_to_js(entries))
        })
    }

    #[napi]
    pub fn extract_subgraph(
        &self,
        start_node_id: f64,
        max_depth: u32,
        direction: Option<String>,
        edge_type_filter: Option<Vec<u32>>,
        at_epoch: Option<i64>,
    ) -> Result<JsSubgraphResult> {
        let start = f64_to_u64(start_node_id)?;
        let dir = parse_direction(direction.as_deref())?;
        with_engine_ref(self, |eng| {
            let sg = eng.extract_subgraph(start, max_depth, dir, edge_type_filter.as_deref(), at_epoch)?;
            Ok(subgraph_to_js(sg))
        })
    }

    /// Batch neighbor query: fetch neighbors for multiple nodes in one call.
    /// Returns an array of entries, each mapping a query node to its neighbors.
    #[napi]
    pub fn neighbors_batch(
        &self,
        node_ids: Vec<f64>,
        direction: Option<String>,
        type_filter: Option<Vec<u32>>,
        at_epoch: Option<i64>,
        decay_lambda: Option<f64>,
    ) -> Result<Vec<JsNeighborBatchEntry>> {
        let ids: Vec<u64> = node_ids.into_iter().map(f64_to_u64).collect::<Result<Vec<_>>>()?;
        let dir = parse_direction(direction.as_deref())?;
        let decay = decay_lambda.map(|v| v as f32);
        with_engine_ref(self, |eng| {
            let map = eng.neighbors_batch(&ids, dir, type_filter.as_deref(), at_epoch, decay)?;
            Ok(convert_batch_result(map))
        })
    }

    #[napi]
    pub fn find_nodes(
        &self,
        type_id: u32,
        prop_key: String,
        prop_value: serde_json::Value,
    ) -> Result<Float64Array> {
        let pv = json_to_prop_value(&prop_value);
        with_engine_ref(self, |eng| {
            let ids = eng.find_nodes(type_id, &prop_key, &pv)?;
            Ok(ids_to_float64_array(&ids))
        })
    }

    /// Return all node IDs of a given type (unpaged).
    #[napi]
    pub fn nodes_by_type(&self, type_id: u32) -> Result<Float64Array> {
        with_engine_ref(self, |eng| {
            let ids = eng.nodes_by_type(type_id)?;
            Ok(ids_to_float64_array(&ids))
        })
    }

    /// Return all edge IDs of a given type (unpaged).
    #[napi]
    pub fn edges_by_type(&self, type_id: u32) -> Result<Float64Array> {
        with_engine_ref(self, |eng| {
            let ids = eng.edges_by_type(type_id)?;
            Ok(ids_to_float64_array(&ids))
        })
    }

    #[napi]
    pub fn get_nodes_by_type(&self, type_id: u32) -> Result<Vec<JsNodeRecord>> {
        with_engine_ref(self, |eng| {
            let records = eng.get_nodes_by_type(type_id)?;
            Ok(records.into_iter().map(|n| n.into()).collect())
        })
    }

    #[napi]
    pub fn get_edges_by_type(&self, type_id: u32) -> Result<Vec<JsEdgeRecord>> {
        with_engine_ref(self, |eng| {
            let records = eng.get_edges_by_type(type_id)?;
            Ok(records.into_iter().map(|e| e.into()).collect())
        })
    }

    #[napi]
    pub fn count_nodes_by_type(&self, type_id: u32) -> Result<i64> {
        with_engine_ref(self, |eng| {
            Ok(eng.count_nodes_by_type(type_id)? as i64)
        })
    }

    #[napi]
    pub fn count_edges_by_type(&self, type_id: u32) -> Result<i64> {
        with_engine_ref(self, |eng| {
            Ok(eng.count_edges_by_type(type_id)? as i64)
        })
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
        with_engine_ref(self, |eng| {
            Ok(id_page_to_js(eng.nodes_by_type_paged(type_id, &page)?))
        })
    }

    #[napi]
    pub fn edges_by_type_paged(
        &self,
        type_id: u32,
        limit: Option<u32>,
        after: Option<f64>,
    ) -> Result<JsIdPageResult> {
        let page = make_page_request(limit, after)?;
        with_engine_ref(self, |eng| {
            Ok(id_page_to_js(eng.edges_by_type_paged(type_id, &page)?))
        })
    }

    #[napi]
    pub fn get_nodes_by_type_paged(
        &self,
        type_id: u32,
        limit: Option<u32>,
        after: Option<f64>,
    ) -> Result<JsNodePageResult> {
        let page = make_page_request(limit, after)?;
        with_engine_ref(self, |eng| {
            Ok(node_page_to_js(eng.get_nodes_by_type_paged(type_id, &page)?))
        })
    }

    #[napi]
    pub fn get_edges_by_type_paged(
        &self,
        type_id: u32,
        limit: Option<u32>,
        after: Option<f64>,
    ) -> Result<JsEdgePageResult> {
        let page = make_page_request(limit, after)?;
        with_engine_ref(self, |eng| {
            Ok(edge_page_to_js(eng.get_edges_by_type_paged(type_id, &page)?))
        })
    }

    #[napi]
    pub fn find_nodes_paged(
        &self,
        type_id: u32,
        prop_key: String,
        prop_value: serde_json::Value,
        limit: Option<u32>,
        after: Option<f64>,
    ) -> Result<JsIdPageResult> {
        let pv = json_to_prop_value(&prop_value);
        let page = make_page_request(limit, after)?;
        with_engine_ref(self, |eng| {
            Ok(id_page_to_js(eng.find_nodes_paged(type_id, &prop_key, &pv, &page)?))
        })
    }

    #[napi]
    pub fn find_nodes_by_time_range(
        &self,
        type_id: u32,
        from_ms: i64,
        to_ms: i64,
    ) -> Result<Float64Array> {
        with_engine_ref(self, |eng| {
            let ids = eng.find_nodes_by_time_range(type_id, from_ms, to_ms)?;
            Ok(ids_to_float64_array(&ids))
        })
    }

    #[napi]
    pub fn find_nodes_by_time_range_paged(
        &self,
        type_id: u32,
        from_ms: i64,
        to_ms: i64,
        limit: Option<u32>,
        after: Option<f64>,
    ) -> Result<JsIdPageResult> {
        let page = make_page_request(limit, after)?;
        with_engine_ref(self, |eng| {
            Ok(id_page_to_js(eng.find_nodes_by_time_range_paged(type_id, from_ms, to_ms, &page)?))
        })
    }

    #[napi]
    pub fn personalized_pagerank(
        &self,
        seed_node_ids: Float64Array,
        options: Option<JsPprOptions>,
    ) -> Result<JsPprResult> {
        let seeds: Vec<u64> = seed_node_ids.iter().map(|v| f64_to_u64(*v)).collect::<Result<Vec<_>>>()?;
        let opts = js_ppr_options_to_rust(options);
        with_engine_ref(self, |eng| {
            let result = eng.personalized_pagerank(&seeds, &opts)?;
            Ok(ppr_result_to_js(result))
        })
    }

    #[napi]
    pub fn export_adjacency(
        &self,
        options: Option<JsExportOptions>,
    ) -> Result<JsAdjacencyExport> {
        let include_weights = options.as_ref().and_then(|o| o.include_weights).unwrap_or(true);
        with_engine_ref(self, |eng| {
            let opts = js_export_options_to_rust(options);
            let result = eng.export_adjacency(&opts)?;
            Ok(adjacency_export_to_js(result, include_weights))
        })
    }

    #[napi]
    pub fn neighbors_paged(
        &self,
        node_id: f64,
        direction: Option<String>,
        type_filter: Option<Vec<u32>>,
        limit: Option<u32>,
        after: Option<f64>,
        at_epoch: Option<i64>,
        decay_lambda: Option<f64>,
    ) -> Result<JsNeighborPageResult> {
        let node_id = f64_to_u64(node_id)?;
        let dir = parse_direction(direction.as_deref())?;
        let page = make_page_request(limit, after)?;
        let decay = decay_lambda.map(|v| v as f32);
        with_engine_ref(self, |eng| {
            Ok(neighbor_page_to_js(eng.neighbors_paged(
                node_id, dir, type_filter.as_deref(), &page, at_epoch, decay,
            )?))
        })
    }

    #[napi]
    pub fn neighbors_2hop_paged(
        &self,
        node_id: f64,
        direction: Option<String>,
        type_filter: Option<Vec<u32>>,
        limit: Option<u32>,
        after: Option<f64>,
        at_epoch: Option<i64>,
        decay_lambda: Option<f64>,
    ) -> Result<JsNeighborPageResult> {
        let node_id = f64_to_u64(node_id)?;
        let dir = parse_direction(direction.as_deref())?;
        let page = make_page_request(limit, after)?;
        let decay = decay_lambda.map(|v| v as f32);
        with_engine_ref(self, |eng| {
            Ok(neighbor_page_to_js(eng.neighbors_2hop_paged(
                node_id, dir, type_filter.as_deref(), &page, at_epoch, decay,
            )?))
        })
    }

    #[napi]
    pub fn neighbors_2hop_constrained_paged(
        &self,
        node_id: f64,
        direction: Option<String>,
        traverse_edge_types: Option<Vec<u32>>,
        target_node_types: Option<Vec<u32>>,
        limit: Option<u32>,
        after: Option<f64>,
        at_epoch: Option<i64>,
        decay_lambda: Option<f64>,
    ) -> Result<JsNeighborPageResult> {
        let node_id = f64_to_u64(node_id)?;
        let dir = parse_direction(direction.as_deref())?;
        let page = make_page_request(limit, after)?;
        let decay = decay_lambda.map(|v| v as f32);
        with_engine_ref(self, |eng| {
            Ok(neighbor_page_to_js(eng.neighbors_2hop_constrained_paged(
                node_id, dir, traverse_edge_types.as_deref(),
                target_node_types.as_deref(), &page, at_epoch, decay,
            )?))
        })
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
    pub fn compact(&self) -> Result<Option<JsCompactionStats>> {
        with_engine(self, |eng| {
            Ok(eng.compact()?.map(|s| s.into()))
        })
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
        with_engine_ref(self, |eng| Ok(eng.stats().into()))
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
    pub fn stats_async(&self) -> AsyncTask<EngineOp<DbStats, JsDbStats>> {
        AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            |eng| Ok(eng.stats()),
            |s| Ok(s.into()),
        ))
    }

    #[napi(ts_return_type = "Promise<number>")]
    pub fn upsert_node_async(
        &self,
        type_id: u32,
        key: String,
        props: Option<HashMap<String, serde_json::Value>>,
        weight: Option<f64>,
    ) -> AsyncTask<EngineOp<u64, f64>> {
        let props = convert_js_props(props);
        let w = weight.unwrap_or(1.0) as f32;
        AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| eng.upsert_node(type_id, &key, props, w),
            |id| Ok(u64_to_f64(id)),
        ))
    }

    #[napi(ts_return_type = "Promise<number>")]
    pub fn upsert_edge_async(
        &self,
        from: f64,
        to: f64,
        type_id: u32,
        props: Option<HashMap<String, serde_json::Value>>,
        weight: Option<f64>,
        valid_from: Option<i64>,
        valid_to: Option<i64>,
    ) -> Result<AsyncTask<EngineOp<u64, f64>>> {
        let from = f64_to_u64(from)?;
        let to = f64_to_u64(to)?;
        let props = convert_js_props(props);
        let w = weight.unwrap_or(1.0) as f32;
        Ok(AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| eng.upsert_edge(from, to, type_id, props, w, valid_from, valid_to),
            |id| Ok(u64_to_f64(id)),
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
            |ids| Ok(ids_to_float64_array(&ids)),
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
            |ids| Ok(ids_to_float64_array(&ids)),
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
            |ids| Ok(ids_to_float64_array(&ids)),
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
            |ids| Ok(ids_to_float64_array(&ids)),
        )))
    }

    #[napi(ts_return_type = "Promise<JsNodeRecord | null>")]
    pub fn get_node_async(
        &self,
        id: f64,
    ) -> Result<AsyncTask<EngineOp<Option<NodeRecord>, Option<JsNodeRecord>>>> {
        let id = f64_to_u64(id)?;
        Ok(AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| eng.get_node(id),
            |n| Ok(n.map(|n| n.into())),
        )))
    }

    #[napi(ts_return_type = "Promise<JsEdgeRecord | null>")]
    pub fn get_edge_async(
        &self,
        id: f64,
    ) -> Result<AsyncTask<EngineOp<Option<EdgeRecord>, Option<JsEdgeRecord>>>> {
        let id = f64_to_u64(id)?;
        Ok(AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| eng.get_edge(id),
            |e| Ok(e.map(|e| e.into())),
        )))
    }

    #[napi(ts_return_type = "Promise<JsNodeRecord | null>")]
    pub fn get_node_by_key_async(
        &self,
        type_id: u32,
        key: String,
    ) -> AsyncTask<EngineOp<Option<NodeRecord>, Option<JsNodeRecord>>> {
        AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| eng.get_node_by_key(type_id, &key),
            |n| Ok(n.map(|n| n.into())),
        ))
    }

    #[napi(ts_return_type = "Promise<JsEdgeRecord | null>")]
    pub fn get_edge_by_triple_async(
        &self,
        from: f64,
        to: f64,
        type_id: u32,
    ) -> Result<AsyncTask<EngineOp<Option<EdgeRecord>, Option<JsEdgeRecord>>>> {
        let from = f64_to_u64(from)?;
        let to = f64_to_u64(to)?;
        Ok(AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| eng.get_edge_by_triple(from, to, type_id),
            |e| Ok(e.map(|e| e.into())),
        )))
    }

    #[napi(ts_return_type = "Promise<Array<JsNodeRecord | null>>")]
    pub fn get_nodes_async(
        &self,
        ids: Vec<f64>,
    ) -> Result<AsyncTask<EngineOp<Vec<Option<NodeRecord>>, Vec<Option<JsNodeRecord>>>>> {
        let ids: Vec<u64> = ids.into_iter().map(f64_to_u64).collect::<Result<Vec<_>>>()?;
        Ok(AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| eng.get_nodes(&ids),
            |results| Ok(results.into_iter().map(|r| r.map(|n| n.into())).collect()),
        )))
    }

    #[napi(ts_return_type = "Promise<Array<JsEdgeRecord | null>>")]
    pub fn get_edges_async(
        &self,
        ids: Vec<f64>,
    ) -> Result<AsyncTask<EngineOp<Vec<Option<EdgeRecord>>, Vec<Option<JsEdgeRecord>>>>> {
        let ids: Vec<u64> = ids.into_iter().map(f64_to_u64).collect::<Result<Vec<_>>>()?;
        Ok(AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| eng.get_edges(&ids),
            |results| Ok(results.into_iter().map(|r| r.map(|e| e.into())).collect()),
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
            |e| Ok(e.map(|e| e.into())),
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
                    node_ids: ids_to_float64_array(&result.node_ids),
                    edge_ids: ids_to_float64_array(&result.edge_ids),
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

    #[napi(ts_return_type = "Promise<JsNeighborList>")]
    pub fn neighbors_async(
        &self,
        node_id: f64,
        direction: Option<String>,
        type_filter: Option<Vec<u32>>,
        limit: Option<u32>,
        at_epoch: Option<i64>,
        decay_lambda: Option<f64>,
    ) -> Result<AsyncTask<EngineOp<Vec<NeighborEntry>, JsNeighborList>>> {
        let node_id = f64_to_u64(node_id)?;
        let dir = parse_direction(direction.as_deref())?;
        let lim = limit.unwrap_or(0) as usize;
        let decay = decay_lambda.map(|v| v as f32); // JS f64 → Rust f32 (sufficient precision for λ)
        Ok(AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| eng.neighbors(node_id, dir, type_filter.as_deref(), lim, at_epoch, decay),
            |entries| Ok(neighbor_entries_to_js(entries)),
        )))
    }

    #[napi(ts_return_type = "Promise<JsNeighborList>")]
    pub fn neighbors_2hop_async(
        &self,
        node_id: f64,
        direction: Option<String>,
        type_filter: Option<Vec<u32>>,
        limit: Option<u32>,
        at_epoch: Option<i64>,
        decay_lambda: Option<f64>,
    ) -> Result<AsyncTask<EngineOp<Vec<NeighborEntry>, JsNeighborList>>> {
        let node_id = f64_to_u64(node_id)?;
        let dir = parse_direction(direction.as_deref())?;
        let lim = limit.unwrap_or(0) as usize;
        let decay = decay_lambda.map(|v| v as f32); // JS f64 → Rust f32 (sufficient precision for λ)
        Ok(AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| eng.neighbors_2hop(node_id, dir, type_filter.as_deref(), lim, at_epoch, decay),
            |entries| Ok(neighbor_entries_to_js(entries)),
        )))
    }

    #[napi(ts_return_type = "Promise<JsNeighborList>")]
    pub fn neighbors_2hop_constrained_async(
        &self,
        node_id: f64,
        direction: Option<String>,
        traverse_edge_types: Option<Vec<u32>>,
        target_node_types: Option<Vec<u32>>,
        limit: Option<u32>,
        at_epoch: Option<i64>,
        decay_lambda: Option<f64>,
    ) -> Result<AsyncTask<EngineOp<Vec<NeighborEntry>, JsNeighborList>>> {
        let node_id = f64_to_u64(node_id)?;
        let dir = parse_direction(direction.as_deref())?;
        let lim = limit.unwrap_or(0) as usize;
        let decay = decay_lambda.map(|v| v as f32);
        Ok(AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| {
                eng.neighbors_2hop_constrained(
                    node_id,
                    dir,
                    traverse_edge_types.as_deref(),
                    target_node_types.as_deref(),
                    lim,
                    at_epoch,
                    decay,
                )
            },
            |entries| Ok(neighbor_entries_to_js(entries)),
        )))
    }

    #[napi(ts_return_type = "Promise<JsNeighborList>")]
    pub fn top_k_neighbors_async(
        &self,
        node_id: f64,
        direction: Option<String>,
        type_filter: Option<Vec<u32>>,
        k: u32,
        scoring: Option<String>,
        decay_lambda: Option<f64>,
        at_epoch: Option<i64>,
    ) -> Result<AsyncTask<EngineOp<Vec<NeighborEntry>, JsNeighborList>>> {
        let node_id = f64_to_u64(node_id)?;
        let dir = parse_direction(direction.as_deref())?;
        let scoring_mode = parse_scoring_mode(scoring.as_deref(), decay_lambda)?;
        Ok(AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| {
                eng.top_k_neighbors(
                    node_id,
                    dir,
                    type_filter.as_deref(),
                    k as usize,
                    scoring_mode,
                    at_epoch,
                )
            },
            |entries| Ok(neighbor_entries_to_js(entries)),
        )))
    }

    #[napi(ts_return_type = "Promise<JsSubgraphResult>")]
    pub fn extract_subgraph_async(
        &self,
        start_node_id: f64,
        max_depth: u32,
        direction: Option<String>,
        edge_type_filter: Option<Vec<u32>>,
        at_epoch: Option<i64>,
    ) -> Result<AsyncTask<EngineOp<Subgraph, JsSubgraphResult>>> {
        let start = f64_to_u64(start_node_id)?;
        let dir = parse_direction(direction.as_deref())?;
        Ok(AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| eng.extract_subgraph(start, max_depth, dir, edge_type_filter.as_deref(), at_epoch),
            |sg| Ok(subgraph_to_js(sg)),
        )))
    }

    #[napi(ts_return_type = "Promise<Float64Array>")]
    pub fn find_nodes_async(
        &self,
        type_id: u32,
        prop_key: String,
        prop_value: serde_json::Value,
    ) -> AsyncTask<EngineOp<Vec<u64>, Float64Array>> {
        let pv = json_to_prop_value(&prop_value);
        AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| eng.find_nodes(type_id, &prop_key, &pv),
            |ids| Ok(ids_to_float64_array(&ids)),
        ))
    }

    #[napi(ts_return_type = "Promise<Array<JsNodeRecord>>")]
    pub fn get_nodes_by_type_async(
        &self,
        type_id: u32,
    ) -> AsyncTask<EngineOp<Vec<NodeRecord>, Vec<JsNodeRecord>>> {
        AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| eng.get_nodes_by_type(type_id),
            |records| Ok(records.into_iter().map(|n| n.into()).collect()),
        ))
    }

    #[napi(ts_return_type = "Promise<Array<JsEdgeRecord>>")]
    pub fn get_edges_by_type_async(
        &self,
        type_id: u32,
    ) -> AsyncTask<EngineOp<Vec<EdgeRecord>, Vec<JsEdgeRecord>>> {
        AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| eng.get_edges_by_type(type_id),
            |records| Ok(records.into_iter().map(|e| e.into()).collect()),
        ))
    }

    #[napi(ts_return_type = "Promise<number>")]
    pub fn count_nodes_by_type_async(
        &self,
        type_id: u32,
    ) -> AsyncTask<EngineOp<u64, i64>> {
        AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| eng.count_nodes_by_type(type_id),
            |count| Ok(count as i64),
        ))
    }

    #[napi(ts_return_type = "Promise<number>")]
    pub fn count_edges_by_type_async(
        &self,
        type_id: u32,
    ) -> AsyncTask<EngineOp<u64, i64>> {
        AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| eng.count_edges_by_type(type_id),
            |count| Ok(count as i64),
        ))
    }

    #[napi(ts_return_type = "Promise<Float64Array>")]
    pub fn nodes_by_type_async(
        &self,
        type_id: u32,
    ) -> AsyncTask<EngineOp<Vec<u64>, Float64Array>> {
        AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| eng.nodes_by_type(type_id),
            |ids| Ok(ids_to_float64_array(&ids)),
        ))
    }

    #[napi(ts_return_type = "Promise<Float64Array>")]
    pub fn edges_by_type_async(
        &self,
        type_id: u32,
    ) -> AsyncTask<EngineOp<Vec<u64>, Float64Array>> {
        AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| eng.edges_by_type(type_id),
            |ids| Ok(ids_to_float64_array(&ids)),
        ))
    }

    #[napi(ts_return_type = "Promise<Array<JsNeighborBatchEntry>>")]
    pub fn neighbors_batch_async(
        &self,
        node_ids: Vec<f64>,
        direction: Option<String>,
        type_filter: Option<Vec<u32>>,
        at_epoch: Option<i64>,
        decay_lambda: Option<f64>,
    ) -> Result<AsyncTask<EngineOp<HashMap<u64, Vec<NeighborEntry>>, Vec<JsNeighborBatchEntry>>>> {
        let ids: Vec<u64> = node_ids.into_iter().map(f64_to_u64).collect::<Result<Vec<_>>>()?;
        let dir = parse_direction(direction.as_deref())?;
        let decay = decay_lambda.map(|v| v as f32);
        Ok(AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| eng.neighbors_batch(&ids, dir, type_filter.as_deref(), at_epoch, decay),
            |map| Ok(convert_batch_result(map)),
        )))
    }

    // --- Paginated queries (async) ---

    #[napi(ts_return_type = "Promise<JsIdPageResult>")]
    pub fn nodes_by_type_paged_async(
        &self,
        type_id: u32,
        limit: Option<u32>,
        after: Option<f64>,
    ) -> Result<AsyncTask<EngineOp<PageResult<u64>, JsIdPageResult>>> {
        let page = make_page_request(limit, after)?;
        Ok(AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| eng.nodes_by_type_paged(type_id, &page),
            |pr| Ok(id_page_to_js(pr)),
        )))
    }

    #[napi(ts_return_type = "Promise<JsIdPageResult>")]
    pub fn edges_by_type_paged_async(
        &self,
        type_id: u32,
        limit: Option<u32>,
        after: Option<f64>,
    ) -> Result<AsyncTask<EngineOp<PageResult<u64>, JsIdPageResult>>> {
        let page = make_page_request(limit, after)?;
        Ok(AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| eng.edges_by_type_paged(type_id, &page),
            |pr| Ok(id_page_to_js(pr)),
        )))
    }

    #[napi(ts_return_type = "Promise<JsNodePageResult>")]
    pub fn get_nodes_by_type_paged_async(
        &self,
        type_id: u32,
        limit: Option<u32>,
        after: Option<f64>,
    ) -> Result<AsyncTask<EngineOp<PageResult<NodeRecord>, JsNodePageResult>>> {
        let page = make_page_request(limit, after)?;
        Ok(AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| eng.get_nodes_by_type_paged(type_id, &page),
            |pr| Ok(node_page_to_js(pr)),
        )))
    }

    #[napi(ts_return_type = "Promise<JsEdgePageResult>")]
    pub fn get_edges_by_type_paged_async(
        &self,
        type_id: u32,
        limit: Option<u32>,
        after: Option<f64>,
    ) -> Result<AsyncTask<EngineOp<PageResult<EdgeRecord>, JsEdgePageResult>>> {
        let page = make_page_request(limit, after)?;
        Ok(AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| eng.get_edges_by_type_paged(type_id, &page),
            |pr| Ok(edge_page_to_js(pr)),
        )))
    }

    #[napi(ts_return_type = "Promise<JsIdPageResult>")]
    pub fn find_nodes_paged_async(
        &self,
        type_id: u32,
        prop_key: String,
        prop_value: serde_json::Value,
        limit: Option<u32>,
        after: Option<f64>,
    ) -> Result<AsyncTask<EngineOp<PageResult<u64>, JsIdPageResult>>> {
        let pv = json_to_prop_value(&prop_value);
        let page = make_page_request(limit, after)?;
        Ok(AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| eng.find_nodes_paged(type_id, &prop_key, &pv, &page),
            |pr| Ok(id_page_to_js(pr)),
        )))
    }

    #[napi(ts_return_type = "Promise<Float64Array>")]
    pub fn find_nodes_by_time_range_async(
        &self,
        type_id: u32,
        from_ms: i64,
        to_ms: i64,
    ) -> Result<AsyncTask<EngineOp<Vec<u64>, Float64Array>>> {
        Ok(AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| eng.find_nodes_by_time_range(type_id, from_ms, to_ms),
            |ids| Ok(ids_to_float64_array(&ids)),
        )))
    }

    #[napi(ts_return_type = "Promise<JsIdPageResult>")]
    pub fn find_nodes_by_time_range_paged_async(
        &self,
        type_id: u32,
        from_ms: i64,
        to_ms: i64,
        limit: Option<u32>,
        after: Option<f64>,
    ) -> Result<AsyncTask<EngineOp<PageResult<u64>, JsIdPageResult>>> {
        let page = make_page_request(limit, after)?;
        Ok(AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| eng.find_nodes_by_time_range_paged(type_id, from_ms, to_ms, &page),
            |pr| Ok(id_page_to_js(pr)),
        )))
    }

    #[napi(ts_return_type = "Promise<JsPprResult>")]
    pub fn personalized_pagerank_async(
        &self,
        seed_node_ids: Float64Array,
        options: Option<JsPprOptions>,
    ) -> Result<AsyncTask<EngineOp<PprResult, JsPprResult>>> {
        let seeds: Vec<u64> = seed_node_ids.iter().map(|v| f64_to_u64(*v)).collect::<Result<Vec<_>>>()?;
        let opts = js_ppr_options_to_rust(options);
        Ok(AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| eng.personalized_pagerank(&seeds, &opts),
            |r| Ok(ppr_result_to_js(r)),
        )))
    }

    #[napi(ts_return_type = "Promise<JsAdjacencyExport>")]
    pub fn export_adjacency_async(
        &self,
        options: Option<JsExportOptions>,
    ) -> Result<AsyncTask<EngineOp<(AdjacencyExport, bool), JsAdjacencyExport>>> {
        let include_weights = options.as_ref().and_then(|o| o.include_weights).unwrap_or(true);
        let opts = js_export_options_to_rust(options);
        Ok(AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| Ok((eng.export_adjacency(&opts)?, include_weights)),
            |pair| Ok(adjacency_export_to_js(pair.0, pair.1)),
        )))
    }

    #[napi(ts_return_type = "Promise<JsNeighborPageResult>")]
    pub fn neighbors_paged_async(
        &self,
        node_id: f64,
        direction: Option<String>,
        type_filter: Option<Vec<u32>>,
        limit: Option<u32>,
        after: Option<f64>,
        at_epoch: Option<i64>,
        decay_lambda: Option<f64>,
    ) -> Result<AsyncTask<EngineOp<PageResult<NeighborEntry>, JsNeighborPageResult>>> {
        let node_id = f64_to_u64(node_id)?;
        let dir = parse_direction(direction.as_deref())?;
        let page = make_page_request(limit, after)?;
        let decay = decay_lambda.map(|v| v as f32);
        Ok(AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| eng.neighbors_paged(node_id, dir, type_filter.as_deref(), &page, at_epoch, decay),
            |pr| Ok(neighbor_page_to_js(pr)),
        )))
    }

    #[napi(ts_return_type = "Promise<JsNeighborPageResult>")]
    pub fn neighbors_2hop_paged_async(
        &self,
        node_id: f64,
        direction: Option<String>,
        type_filter: Option<Vec<u32>>,
        limit: Option<u32>,
        after: Option<f64>,
        at_epoch: Option<i64>,
        decay_lambda: Option<f64>,
    ) -> Result<AsyncTask<EngineOp<PageResult<NeighborEntry>, JsNeighborPageResult>>> {
        let node_id = f64_to_u64(node_id)?;
        let dir = parse_direction(direction.as_deref())?;
        let page = make_page_request(limit, after)?;
        let decay = decay_lambda.map(|v| v as f32);
        Ok(AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| eng.neighbors_2hop_paged(node_id, dir, type_filter.as_deref(), &page, at_epoch, decay),
            |pr| Ok(neighbor_page_to_js(pr)),
        )))
    }

    #[napi(ts_return_type = "Promise<JsNeighborPageResult>")]
    pub fn neighbors_2hop_constrained_paged_async(
        &self,
        node_id: f64,
        direction: Option<String>,
        traverse_edge_types: Option<Vec<u32>>,
        target_node_types: Option<Vec<u32>>,
        limit: Option<u32>,
        after: Option<f64>,
        at_epoch: Option<i64>,
        decay_lambda: Option<f64>,
    ) -> Result<AsyncTask<EngineOp<PageResult<NeighborEntry>, JsNeighborPageResult>>> {
        let node_id = f64_to_u64(node_id)?;
        let dir = parse_direction(direction.as_deref())?;
        let page = make_page_request(limit, after)?;
        let decay = decay_lambda.map(|v| v as f32);
        Ok(AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| {
                eng.neighbors_2hop_constrained_paged(
                    node_id, dir, traverse_edge_types.as_deref(),
                    target_node_types.as_deref(), &page, at_epoch, decay,
                )
            },
            |pr| Ok(neighbor_page_to_js(pr)),
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
    /// Note: the database mutex is held for the entire compaction, so other operations on this
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

// ============================================================
// JS-facing types
// ============================================================

#[napi(object)]
pub struct JsCloseOptions {
    /// If true, cancel any in-progress background compaction instead of waiting.
    pub force: Option<bool>,
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
        }
    }
}

#[napi(object)]
pub struct JsDbOptions {
    pub create_if_missing: Option<bool>,
    pub edge_uniqueness: Option<bool>,
    pub memtable_flush_threshold: Option<u32>,
    /// Trigger compaction automatically after this many flushes. Default 5, 0 = disabled.
    pub compact_after_n_flushes: Option<u32>,
    /// WAL sync mode: 'immediate' or 'group-commit' (default).
    pub wal_sync_mode: Option<String>,
    /// Group commit sync interval in milliseconds. Default: 10.
    pub group_commit_interval_ms: Option<u32>,
    /// Hard cap on memtable size in bytes. Writes trigger a flush when exceeded. 0 = disabled.
    pub memtable_hard_cap_bytes: Option<u32>,
}

impl From<JsDbOptions> for DbOptions {
    fn from(js: JsDbOptions) -> Self {
        let defaults = DbOptions::default();
        let wal_sync_mode = match js.wal_sync_mode.as_deref() {
            Some("immediate") => WalSyncMode::Immediate,
            _ => {
                // Default to GroupCommit, but allow overriding interval
                let interval_ms = js.group_commit_interval_ms.unwrap_or(10) as u64;
                WalSyncMode::GroupCommit {
                    interval_ms,
                    soft_trigger_bytes: 4 * 1024 * 1024,
                    hard_cap_bytes: 16 * 1024 * 1024,
                }
            }
        };
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
            wal_sync_mode,
            memtable_hard_cap_bytes: js
                .memtable_hard_cap_bytes
                .map(|v| v as usize)
                .unwrap_or(defaults.memtable_hard_cap_bytes),
        }
    }
}

#[napi(object)]
pub struct JsNodeInput {
    pub type_id: u32,
    pub key: String,
    pub props: Option<HashMap<String, serde_json::Value>>,
    pub weight: Option<f64>,
}

impl From<JsNodeInput> for NodeInput {
    fn from(js: JsNodeInput) -> Self {
        NodeInput {
            type_id: js.type_id,
            key: js.key,
            props: convert_js_props(js.props),
            weight: js.weight.unwrap_or(1.0) as f32,
        }
    }
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

impl From<NodeRecord> for JsNodeRecord {
    fn from(n: NodeRecord) -> Self {
        JsNodeRecord {
            id_val: u64_to_f64(n.id),
            type_id_val: n.type_id,
            key_val: n.key,
            created_at_val: n.created_at,
            updated_at_val: n.updated_at,
            weight_val: n.weight as f64,
            props_raw: Arc::new(n.props),
        }
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

impl From<EdgeRecord> for JsEdgeRecord {
    fn from(e: EdgeRecord) -> Self {
        JsEdgeRecord {
            id_val: u64_to_f64(e.id),
            from_val: u64_to_f64(e.from),
            to_val: u64_to_f64(e.to),
            type_id_val: e.type_id,
            created_at_val: e.created_at,
            updated_at_val: e.updated_at,
            weight_val: e.weight as f64,
            valid_from_val: e.valid_from,
            valid_to_val: e.valid_to,
            props_raw: Arc::new(e.props),
        }
    }
}

/// A single neighbor entry as a plain JS object.
/// Used by `JsNeighborList.get(i)` and `JsNeighborList.toArray()`.
#[napi(object)]
pub struct JsNeighborEntry {
    pub node_id: f64,
    pub edge_id: f64,
    pub edge_type_id: u32,
    pub weight: f64,
    pub valid_from: i64,
    pub valid_to: i64,
}

/// Lazy sequence wrapper around neighbor entries. Data stays in Rust;
/// individual fields are only converted to JS values when accessed via
/// indexed methods. One V8 allocation regardless of result set size.
/// Arc-wrapped so parent containers (batch entries, page results) share
/// data instead of cloning the full vec on every getter access.
#[napi]
pub struct JsNeighborList {
    entries: Arc<Vec<NeighborEntry>>,
}

#[napi]
impl JsNeighborList {
    #[napi(getter)]
    pub fn length(&self) -> u32 {
        self.entries.len() as u32
    }

    #[napi]
    pub fn node_id(&self, index: u32) -> Result<f64> {
        let e = self.entry_at(index)?;
        Ok(u64_to_f64(e.node_id))
    }

    #[napi]
    pub fn edge_id(&self, index: u32) -> Result<f64> {
        let e = self.entry_at(index)?;
        Ok(u64_to_f64(e.edge_id))
    }

    #[napi]
    pub fn edge_type_id(&self, index: u32) -> Result<u32> {
        let e = self.entry_at(index)?;
        Ok(e.edge_type_id)
    }

    #[napi]
    pub fn weight(&self, index: u32) -> Result<f64> {
        let e = self.entry_at(index)?;
        Ok(e.weight as f64)
    }

    #[napi]
    pub fn valid_from(&self, index: u32) -> Result<i64> {
        let e = self.entry_at(index)?;
        Ok(e.valid_from)
    }

    #[napi]
    pub fn valid_to(&self, index: u32) -> Result<i64> {
        let e = self.entry_at(index)?;
        Ok(e.valid_to)
    }

    /// Get a single entry as a plain object with all fields materialized.
    #[napi]
    pub fn get(&self, index: u32) -> Result<JsNeighborEntry> {
        let e = self.entry_at(index)?;
        Ok(neighbor_to_js_entry(e))
    }

    /// Materialize all entries as an array of plain objects.
    #[napi]
    pub fn to_array(&self) -> Vec<JsNeighborEntry> {
        self.entries.iter().map(neighbor_to_js_entry).collect()
    }
}

impl JsNeighborList {
    fn entry_at(&self, index: u32) -> Result<&NeighborEntry> {
        self.entries
            .get(index as usize)
            .ok_or_else(|| {
                napi::Error::from_reason(format!(
                    "Index {} out of bounds (length {})",
                    index,
                    self.entries.len()
                ))
            })
    }
}

fn neighbor_to_js_entry(e: &NeighborEntry) -> JsNeighborEntry {
    JsNeighborEntry {
        node_id: u64_to_f64(e.node_id),
        edge_id: u64_to_f64(e.edge_id),
        edge_type_id: e.edge_type_id,
        weight: e.weight as f64,
        valid_from: e.valid_from,
        valid_to: e.valid_to,
    }
}

#[napi]
pub struct JsNeighborBatchEntry {
    query_id: u64,
    neighbor_entries: Arc<Vec<NeighborEntry>>,
}

#[napi]
impl JsNeighborBatchEntry {
    #[napi(getter)]
    pub fn query_node_id(&self) -> f64 {
        u64_to_f64(self.query_id)
    }

    /// Lazy neighbor list for this batch entry.
    #[napi(getter)]
    pub fn neighbors(&self) -> JsNeighborList {
        JsNeighborList {
            entries: Arc::clone(&self.neighbor_entries),
        }
    }
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
        self.nodes_vec.iter().map(|n| JsNodeRecord {
            id_val: n.id_val,
            type_id_val: n.type_id_val,
            key_val: n.key_val.clone(),
            created_at_val: n.created_at_val,
            updated_at_val: n.updated_at_val,
            weight_val: n.weight_val,
            props_raw: Arc::clone(&n.props_raw),
        }).collect()
    }
    #[napi(getter)]
    pub fn edges(&self) -> Vec<JsEdgeRecord> {
        self.edges_vec.iter().map(|e| JsEdgeRecord {
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
        }).collect()
    }
}

fn subgraph_to_js(sg: Subgraph) -> JsSubgraphResult {
    JsSubgraphResult {
        nodes_vec: sg.nodes.into_iter().map(|n| n.into()).collect(),
        edges_vec: sg.edges.into_iter().map(|e| e.into()).collect(),
    }
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
        self.items_vec.iter().map(|n| JsNodeRecord {
            id_val: n.id_val,
            type_id_val: n.type_id_val,
            key_val: n.key_val.clone(),
            created_at_val: n.created_at_val,
            updated_at_val: n.updated_at_val,
            weight_val: n.weight_val,
            props_raw: Arc::clone(&n.props_raw),
        }).collect()
    }
    #[napi(getter)]
    pub fn next_cursor(&self) -> Option<f64> {
        self.cursor.map(u64_to_f64)
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
        self.items_vec.iter().map(|e| JsEdgeRecord {
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
        }).collect()
    }
    #[napi(getter)]
    pub fn next_cursor(&self) -> Option<f64> {
        self.cursor.map(u64_to_f64)
    }
}

#[napi]
pub struct JsNeighborPageResult {
    entries: Arc<Vec<NeighborEntry>>,
    cursor: Option<u64>,
}

#[napi]
impl JsNeighborPageResult {
    /// Lazy neighbor list for this page.
    #[napi(getter)]
    pub fn items(&self) -> JsNeighborList {
        JsNeighborList {
            entries: Arc::clone(&self.entries),
        }
    }

    #[napi(getter)]
    pub fn next_cursor(&self) -> Option<f64> {
        self.cursor.map(u64_to_f64)
    }
}

fn id_page_to_js(page: PageResult<u64>) -> JsIdPageResult {
    JsIdPageResult {
        items: ids_to_float64_array(&page.items),
        next_cursor: page.next_cursor.map(u64_to_f64),
    }
}

fn node_page_to_js(page: PageResult<NodeRecord>) -> JsNodePageResult {
    JsNodePageResult {
        items_vec: page.items.into_iter().map(|n| n.into()).collect(),
        cursor: page.next_cursor,
    }
}

fn edge_page_to_js(page: PageResult<EdgeRecord>) -> JsEdgePageResult {
    JsEdgePageResult {
        items_vec: page.items.into_iter().map(|e| e.into()).collect(),
        cursor: page.next_cursor,
    }
}

fn neighbor_page_to_js(page: PageResult<NeighborEntry>) -> JsNeighborPageResult {
    JsNeighborPageResult {
        entries: Arc::new(page.items),
        cursor: page.next_cursor,
    }
}

fn make_page_request(limit: Option<u32>, after: Option<f64>) -> napi::Result<PageRequest> {
    let after_val = after.map(f64_to_u64).transpose()?;
    Ok(PageRequest {
        limit: limit.map(|l| l as usize),
        after: after_val,
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
pub struct JsPprOptions {
    pub damping_factor: Option<f64>,
    pub max_iterations: Option<u32>,
    pub epsilon: Option<f64>,
    pub edge_type_filter: Option<Vec<u32>>,
    pub max_results: Option<u32>,
}

#[napi(object)]
pub struct JsPprResult {
    pub node_ids: Float64Array,
    pub scores: Float64Array,
    pub iterations: u32,
    pub converged: bool,
}

fn ppr_result_to_js(r: PprResult) -> JsPprResult {
    let mut node_ids_raw = Vec::with_capacity(r.scores.len());
    let mut scores = Vec::with_capacity(r.scores.len());
    for (id, score) in &r.scores {
        node_ids_raw.push(u64_to_f64(*id));
        scores.push(*score);
    }
    JsPprResult {
        node_ids: Float64Array::new(node_ids_raw),
        scores: Float64Array::new(scores),
        iterations: r.iterations,
        converged: r.converged,
    }
}

fn js_ppr_options_to_rust(opts: Option<JsPprOptions>) -> PprOptions {
    match opts {
        None => PprOptions::default(),
        Some(o) => PprOptions {
            damping_factor: o.damping_factor.unwrap_or(0.85),
            max_iterations: o.max_iterations.unwrap_or(20),
            epsilon: o.epsilon.unwrap_or(1e-6),
            edge_type_filter: o.edge_type_filter,
            max_results: o.max_results.map(|v| v as usize),
        },
    }
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

fn adjacency_export_to_js(r: AdjacencyExport, include_weights: bool) -> JsAdjacencyExport {
    let node_ids = Float64Array::new(r.node_ids.iter().map(|&id| u64_to_f64(id)).collect());
    let mut from_raw = Vec::with_capacity(r.edges.len());
    let mut to_raw = Vec::with_capacity(r.edges.len());
    let mut type_ids = Vec::with_capacity(r.edges.len());
    let mut weights = Vec::with_capacity(r.edges.len());
    for &(f, t, tid, w) in &r.edges {
        from_raw.push(u64_to_f64(f));
        to_raw.push(u64_to_f64(t));
        type_ids.push(tid);
        weights.push(w as f64);
    }
    JsAdjacencyExport {
        node_ids,
        edge_from: Float64Array::new(from_raw),
        edge_to: Float64Array::new(to_raw),
        edge_type_ids: Uint32Array::new(type_ids),
        edge_weights: if include_weights { Some(Float64Array::new(weights)) } else { None },
    }
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

/// Generic async task: runs an engine operation on the libuv thread pool.
pub struct EngineOp<T: Send + 'static, J: ToNapiValue + TypeName + 'static> {
    db: Arc<Mutex<Option<InnerDb>>>,
    op: Option<Box<dyn FnOnce(&mut DatabaseEngine) -> std::result::Result<T, EngineError> + Send>>,
    convert: fn(T) -> napi::Result<J>,
}

impl<T: Send + 'static, J: ToNapiValue + TypeName + 'static> EngineOp<T, J> {
    fn new(
        db: Arc<Mutex<Option<InnerDb>>>,
        op: impl FnOnce(&mut DatabaseEngine) -> std::result::Result<T, EngineError> + Send + 'static,
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
        let op = self
            .op
            .take()
            .ok_or_else(|| napi::Error::from_reason("EngineOp::compute called twice".to_string()))?;
        let mut guard = self
            .db
            .lock()
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        let inner = guard
            .as_mut()
            .ok_or_else(|| napi::Error::from_reason("Database is closed".to_string()))?;
        op(&mut inner.engine).map_err(|e| napi::Error::from_reason(e.to_string()))
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
        let mut guard = self
            .db
            .lock()
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        if let Some(db) = guard.take() {
            let result = if self.force {
                db.engine.close_fast()
            } else {
                db.engine.close()
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
        let mut guard = self
            .db
            .lock()
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        let inner = guard
            .as_mut()
            .ok_or_else(|| napi::Error::from_reason("Database is closed".to_string()))?;

        let tsfn = &self.tsfn;
        let result = inner.engine.compact_with_progress(|progress| {
            let js_progress = JsCompactionProgress {
                phase: match progress.phase {
                    CompactionPhase::CollectingTombstones => {
                        "collecting_tombstones".to_string()
                    }
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

// ============================================================
// Helpers
// ============================================================

fn with_engine<F, T>(db: &OverGraph, f: F) -> Result<T>
where
    F: FnOnce(&mut DatabaseEngine) -> std::result::Result<T, EngineError>,
{
    let mut guard = db
        .inner
        .lock()
        .map_err(|e| napi::Error::from_reason(e.to_string()))?;
    let inner = guard
        .as_mut()
        .ok_or_else(|| napi::Error::from_reason("Database is closed".to_string()))?;
    f(&mut inner.engine).map_err(|e| napi::Error::from_reason(e.to_string()))
}

fn with_engine_ref<F, T>(db: &OverGraph, f: F) -> Result<T>
where
    F: FnOnce(&DatabaseEngine) -> std::result::Result<T, EngineError>,
{
    let guard = db
        .inner
        .lock()
        .map_err(|e| napi::Error::from_reason(e.to_string()))?;
    let inner = guard
        .as_ref()
        .ok_or_else(|| napi::Error::from_reason("Database is closed".to_string()))?;
    f(&inner.engine).map_err(|e| napi::Error::from_reason(e.to_string()))
}

const MAX_SAFE_INTEGER: f64 = 9_007_199_254_740_991.0; // 2^53 - 1

fn f64_to_u64(v: f64) -> Result<u64> {
    if v < 0.0 || v > MAX_SAFE_INTEGER || v.fract() != 0.0 || v.is_nan() {
        return Err(napi::Error::from_reason(
            "ID must be a safe non-negative integer".to_string(),
        ));
    }
    Ok(v as u64)
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
        serde_json::Value::Object(map) => {
            PropValue::Map(map.iter().map(|(k, v)| (k.clone(), json_to_prop_value(v))).collect())
        }
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
        PropValue::Map(map) => {
            serde_json::Value::Object(
                map.into_iter().map(|(k, v)| (k, prop_value_to_json(v))).collect(),
            )
        }
    }
}

fn props_to_json(
    props: BTreeMap<String, PropValue>,
) -> HashMap<String, serde_json::Value> {
    props
        .into_iter()
        .map(|(k, v)| (k, prop_value_to_json(v)))
        .collect()
}

#[inline]
fn u64_to_f64(v: u64) -> f64 {
    assert!(
        (v as f64) <= MAX_SAFE_INTEGER,
        "ID exceeds Number.MAX_SAFE_INTEGER"
    );
    v as f64
}

fn ids_to_float64_array(ids: &[u64]) -> Float64Array {
    let floats: Vec<f64> = ids.iter().map(|&id| u64_to_f64(id)).collect();
    Float64Array::new(floats)
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
        serde_json::Value::Object(map) => {
            Ok(map
                .into_iter()
                .map(|(k, v)| (k, json_to_prop_value(&v)))
                .collect())
        }
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
            valid_from: if valid_from_raw == 0 { None } else { Some(valid_from_raw) },
            valid_to: if valid_to_raw == 0 { None } else { Some(valid_to_raw) },
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

fn neighbor_entries_to_js(entries: Vec<NeighborEntry>) -> JsNeighborList {
    JsNeighborList { entries: Arc::new(entries) }
}

fn convert_batch_result(map: HashMap<u64, Vec<NeighborEntry>>) -> Vec<JsNeighborBatchEntry> {
    let mut entries: Vec<JsNeighborBatchEntry> = map
        .into_iter()
        .map(|(query_id, neighbors)| JsNeighborBatchEntry {
            query_id,
            neighbor_entries: Arc::new(neighbors),
        })
        .collect();
    // Sort by query_node_id for deterministic output
    entries.sort_by_key(|e| e.query_id);
    entries
}
