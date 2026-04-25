#![allow(clippy::too_many_arguments)]

use eg::{
    AdjacencyExport, AllShortestPathsOptions, CompactionPhase, CompactionProgress, CompactionStats,
    ComponentOptions, DatabaseEngine, DbOptions, DbStats, DegreeOptions, DenseMetric,
    DenseVectorConfig, Direction, EdgeInput, EdgeRecord, EngineError, ExportOptions, FusionMode,
    GraphPatch, HnswConfig, IsConnectedOptions, NeighborEntry, NeighborOptions, NodeIdMap,
    NodeInput, NodePropertyIndexInfo, NodeRecord, PageRequest, PprAlgorithm, PprOptions, PprResult,
    PropValue, PropertyRangeBound, PropertyRangeCursor, PropertyRangePageRequest,
    PropertyRangePageResult, PrunePolicy, PruneResult, ScoringMode, SecondaryIndexKind,
    SecondaryIndexRangeDomain, SecondaryIndexState, ShortestPath, ShortestPathOptions, Subgraph,
    SubgraphOptions, TopKOptions, TraversalCursor, TraversalHit, TraversalPageResult,
    TraverseOptions, TxnCommitResult, TxnEdgeRef, TxnEdgeView, TxnIntent, TxnLocalRef, TxnNodeRef,
    TxnNodeView, UpsertEdgeOptions, UpsertNodeOptions, VectorSearchMode, VectorSearchRequest,
    VectorSearchScope, WalSyncMode, WriteTxn,
};
use pyo3::exceptions::{PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyDict, PyList};
use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::{Arc, Mutex};

// ============================================================
// Error type
// ============================================================

pyo3::create_exception!(overgraph, OverGraphError, pyo3::exceptions::PyException);

fn to_py_err(e: EngineError) -> PyErr {
    OverGraphError::new_err(e.to_string())
}

fn lock_err<T>(e: std::sync::PoisonError<T>) -> PyErr {
    PyRuntimeError::new_err(format!("Lock poisoned: {}", e))
}

fn closed_err() -> PyErr {
    OverGraphError::new_err("Database is closed")
}

// ============================================================
// Core wrapper
// ============================================================

struct InnerDb {
    engine: DatabaseEngine,
}

#[pyclass]
pub struct OverGraph {
    inner: Arc<Mutex<Option<InnerDb>>>,
}

/// Execute a closure with mutable engine access, releasing the GIL.
fn with_engine<F, T>(db: &OverGraph, py: Python<'_>, f: F) -> PyResult<T>
where
    F: FnOnce(&DatabaseEngine) -> Result<T, EngineError> + Send,
    T: Send,
{
    let engine = clone_engine_handle(&db.inner)?;
    py.allow_threads(move || f(&engine).map_err(to_py_err))
}

/// Execute a closure with shared engine access, releasing the GIL.
fn with_engine_ref<F, T>(db: &OverGraph, py: Python<'_>, f: F) -> PyResult<T>
where
    F: FnOnce(&DatabaseEngine) -> Result<T, EngineError> + Send,
    T: Send,
{
    let engine = clone_engine_handle(&db.inner)?;
    py.allow_threads(move || f(&engine).map_err(to_py_err))
}

fn clone_engine_handle(inner: &Arc<Mutex<Option<InnerDb>>>) -> PyResult<DatabaseEngine> {
    let guard = inner.lock().map_err(lock_err)?;
    let db = guard.as_ref().ok_or_else(closed_err)?;
    Ok(db.engine.clone())
}

#[pymethods]
impl OverGraph {
    // --- Lifecycle ---

    #[staticmethod]
    #[pyo3(signature = (path, **kwargs))]
    fn open(path: &str, kwargs: Option<&Bound<'_, PyDict>>) -> PyResult<Self> {
        let opts = match kwargs {
            Some(d) => parse_db_options(d)?,
            None => DbOptions::default(),
        };
        let engine = DatabaseEngine::open(Path::new(path), &opts).map_err(to_py_err)?;
        Ok(OverGraph {
            inner: Arc::new(Mutex::new(Some(InnerDb { engine }))),
        })
    }

    #[pyo3(signature = (force=false))]
    fn close(&self, py: Python<'_>, force: bool) -> PyResult<()> {
        let inner = self.inner.clone();
        py.allow_threads(move || {
            let engine = {
                let mut guard = inner.lock().map_err(lock_err)?;
                guard.take().map(|db| db.engine)
            };
            if let Some(engine) = engine {
                if force {
                    engine.close_fast().map_err(to_py_err)?;
                } else {
                    engine.close().map_err(to_py_err)?;
                }
            }
            Ok(())
        })
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    #[pyo3(signature = (_exc_type=None, _exc_val=None, _exc_tb=None))]
    fn __exit__(
        &self,
        py: Python<'_>,
        _exc_type: Option<PyObject>,
        _exc_val: Option<PyObject>,
        _exc_tb: Option<PyObject>,
    ) -> PyResult<bool> {
        self.close(py, false)?;
        Ok(false)
    }

    fn stats(&self, py: Python<'_>) -> PyResult<PyDbStats> {
        with_engine_ref(self, py, |eng| Ok(PyDbStats::from(eng.stats()?)))
    }

    // --- Single CRUD ---

    #[pyo3(signature = (type_id, key, *, props=None, weight=1.0, dense_vector=None, sparse_vector=None))]
    fn upsert_node(
        &self,
        py: Python<'_>,
        type_id: u32,
        key: String,
        props: Option<&Bound<'_, PyDict>>,
        weight: f64,
        dense_vector: Option<Vec<f32>>,
        sparse_vector: Option<Vec<(u32, f32)>>,
    ) -> PyResult<u64> {
        let props = convert_py_props(py, props)?;
        let opts = UpsertNodeOptions {
            props,
            weight: weight as f32,
            dense_vector,
            sparse_vector,
        };
        with_engine(self, py, move |eng| eng.upsert_node(type_id, &key, opts))
    }

    #[pyo3(signature = (from_id, to_id, type_id, *, props=None, weight=1.0, valid_from=None, valid_to=None))]
    fn upsert_edge(
        &self,
        py: Python<'_>,
        from_id: u64,
        to_id: u64,
        type_id: u32,
        props: Option<&Bound<'_, PyDict>>,
        weight: f64,
        valid_from: Option<i64>,
        valid_to: Option<i64>,
    ) -> PyResult<u64> {
        let props = convert_py_props(py, props)?;
        let opts = UpsertEdgeOptions {
            props,
            weight: weight as f32,
            valid_from,
            valid_to,
        };
        with_engine(self, py, move |eng| {
            eng.upsert_edge(from_id, to_id, type_id, opts)
        })
    }

    fn get_node(&self, py: Python<'_>, id: u64) -> PyResult<Option<PyNodeRecord>> {
        with_engine_ref(self, py, |eng| {
            Ok(eng.get_node(id)?.map(PyNodeRecord::from))
        })
    }

    fn get_edge(&self, py: Python<'_>, id: u64) -> PyResult<Option<PyEdgeRecord>> {
        with_engine_ref(self, py, |eng| {
            Ok(eng.get_edge(id)?.map(PyEdgeRecord::from))
        })
    }

    fn get_node_by_key(
        &self,
        py: Python<'_>,
        type_id: u32,
        key: String,
    ) -> PyResult<Option<PyNodeRecord>> {
        with_engine_ref(self, py, move |eng| {
            Ok(eng.get_node_by_key(type_id, &key)?.map(PyNodeRecord::from))
        })
    }

    fn get_edge_by_triple(
        &self,
        py: Python<'_>,
        from_id: u64,
        to_id: u64,
        type_id: u32,
    ) -> PyResult<Option<PyEdgeRecord>> {
        with_engine_ref(self, py, move |eng| {
            Ok(eng
                .get_edge_by_triple(from_id, to_id, type_id)?
                .map(PyEdgeRecord::from))
        })
    }

    fn delete_node(&self, py: Python<'_>, id: u64) -> PyResult<()> {
        with_engine(self, py, move |eng| eng.delete_node(id))
    }

    fn delete_edge(&self, py: Python<'_>, id: u64) -> PyResult<()> {
        with_engine(self, py, move |eng| eng.delete_edge(id))
    }

    fn invalidate_edge(
        &self,
        py: Python<'_>,
        id: u64,
        valid_to: i64,
    ) -> PyResult<Option<PyEdgeRecord>> {
        with_engine(self, py, move |eng| {
            Ok(eng.invalidate_edge(id, valid_to)?.map(PyEdgeRecord::from))
        })
    }

    // --- Batch ops ---

    fn batch_upsert_nodes(&self, py: Python<'_>, nodes: &Bound<'_, PyList>) -> PyResult<Vec<u64>> {
        let inputs = parse_node_inputs(py, nodes)?;
        with_engine(self, py, move |eng| eng.batch_upsert_nodes(&inputs))
    }

    fn batch_upsert_edges(&self, py: Python<'_>, edges: &Bound<'_, PyList>) -> PyResult<Vec<u64>> {
        let inputs = parse_edge_inputs(py, edges)?;
        with_engine(self, py, move |eng| eng.batch_upsert_edges(&inputs))
    }

    fn get_nodes(&self, py: Python<'_>, ids: Vec<u64>) -> PyResult<Vec<Option<PyNodeRecord>>> {
        with_engine_ref(self, py, move |eng| {
            let results = eng.get_nodes(&ids)?;
            Ok(results
                .into_iter()
                .map(|r| r.map(PyNodeRecord::from))
                .collect())
        })
    }

    fn get_nodes_by_keys(
        &self,
        py: Python<'_>,
        keys: Vec<(u32, String)>,
    ) -> PyResult<Vec<Option<PyNodeRecord>>> {
        with_engine_ref(self, py, move |eng| {
            let refs: Vec<(u32, &str)> = keys.iter().map(|(t, k)| (*t, k.as_str())).collect();
            let results = eng.get_nodes_by_keys(&refs)?;
            Ok(results
                .into_iter()
                .map(|r| r.map(PyNodeRecord::from))
                .collect())
        })
    }

    fn get_edges(&self, py: Python<'_>, ids: Vec<u64>) -> PyResult<Vec<Option<PyEdgeRecord>>> {
        with_engine_ref(self, py, move |eng| {
            let results = eng.get_edges(&ids)?;
            Ok(results
                .into_iter()
                .map(|r| r.map(PyEdgeRecord::from))
                .collect())
        })
    }

    fn graph_patch(&self, py: Python<'_>, patch: &Bound<'_, PyDict>) -> PyResult<PyPatchResult> {
        let rust_patch = parse_graph_patch(py, patch)?;
        with_engine(self, py, move |eng| {
            let result = eng.graph_patch(&rust_patch)?;
            Ok(PyPatchResult {
                node_ids: result.node_ids,
                edge_ids: result.edge_ids,
            })
        })
    }

    fn begin_write_txn(&self, py: Python<'_>) -> PyResult<PyWriteTxn> {
        let txn = with_engine_ref(self, py, |eng| eng.begin_write_txn())?;
        Ok(PyWriteTxn {
            inner: Arc::new(Mutex::new(Some(txn))),
        })
    }

    // --- Queries ---

    fn find_nodes(
        &self,
        py: Python<'_>,
        type_id: u32,
        prop_key: String,
        prop_value: &Bound<'_, pyo3::PyAny>,
    ) -> PyResult<IdArray> {
        let pv = py_to_prop_value(py, prop_value)?;
        with_engine_ref(self, py, move |eng| {
            Ok(IdArray {
                ids: Arc::new(eng.find_nodes(type_id, &prop_key, &pv)?),
            })
        })
    }

    #[pyo3(signature = (type_id, prop_key, kind, *, domain=None))]
    fn ensure_node_property_index(
        &self,
        py: Python<'_>,
        type_id: u32,
        prop_key: String,
        kind: &str,
        domain: Option<&str>,
    ) -> PyResult<PyNodePropertyIndexInfo> {
        let kind = parse_secondary_index_kind(kind, domain)?;
        with_engine(self, py, move |eng| {
            Ok(PyNodePropertyIndexInfo::from(
                eng.ensure_node_property_index(type_id, &prop_key, kind.clone())?,
            ))
        })
    }

    #[pyo3(signature = (type_id, prop_key, kind, *, domain=None))]
    fn drop_node_property_index(
        &self,
        py: Python<'_>,
        type_id: u32,
        prop_key: String,
        kind: &str,
        domain: Option<&str>,
    ) -> PyResult<bool> {
        let kind = parse_secondary_index_kind(kind, domain)?;
        with_engine(self, py, move |eng| {
            eng.drop_node_property_index(type_id, &prop_key, kind.clone())
        })
    }

    fn list_node_property_indexes(&self, py: Python<'_>) -> PyResult<Vec<PyNodePropertyIndexInfo>> {
        with_engine_ref(self, py, |eng| {
            Ok(eng
                .list_node_property_indexes()?
                .into_iter()
                .map(PyNodePropertyIndexInfo::from)
                .collect())
        })
    }

    fn nodes_by_type(&self, py: Python<'_>, type_id: u32) -> PyResult<IdArray> {
        with_engine_ref(self, py, move |eng| {
            Ok(IdArray {
                ids: Arc::new(eng.nodes_by_type(type_id)?),
            })
        })
    }

    fn get_nodes_by_type(&self, py: Python<'_>, type_id: u32) -> PyResult<Vec<PyNodeRecord>> {
        with_engine_ref(self, py, move |eng| {
            Ok(eng
                .get_nodes_by_type(type_id)?
                .into_iter()
                .map(PyNodeRecord::from)
                .collect())
        })
    }

    fn edges_by_type(&self, py: Python<'_>, type_id: u32) -> PyResult<IdArray> {
        with_engine_ref(self, py, move |eng| {
            Ok(IdArray {
                ids: Arc::new(eng.edges_by_type(type_id)?),
            })
        })
    }

    fn get_edges_by_type(&self, py: Python<'_>, type_id: u32) -> PyResult<Vec<PyEdgeRecord>> {
        with_engine_ref(self, py, move |eng| {
            Ok(eng
                .get_edges_by_type(type_id)?
                .into_iter()
                .map(PyEdgeRecord::from)
                .collect())
        })
    }

    fn count_nodes_by_type(&self, py: Python<'_>, type_id: u32) -> PyResult<u64> {
        with_engine_ref(self, py, move |eng| eng.count_nodes_by_type(type_id))
    }

    fn count_edges_by_type(&self, py: Python<'_>, type_id: u32) -> PyResult<u64> {
        with_engine_ref(self, py, move |eng| eng.count_edges_by_type(type_id))
    }

    fn find_nodes_by_time_range(
        &self,
        py: Python<'_>,
        type_id: u32,
        from_ms: i64,
        to_ms: i64,
    ) -> PyResult<IdArray> {
        with_engine_ref(self, py, move |eng| {
            Ok(IdArray {
                ids: Arc::new(eng.find_nodes_by_time_range(type_id, from_ms, to_ms)?),
            })
        })
    }

    #[pyo3(signature = (type_id, prop_key, lower=None, upper=None))]
    fn find_nodes_range(
        &self,
        py: Python<'_>,
        type_id: u32,
        prop_key: String,
        lower: Option<PyPropertyRangeBound>,
        upper: Option<PyPropertyRangeBound>,
    ) -> PyResult<IdArray> {
        let lower = lower.map(PropertyRangeBound::from);
        let upper = upper.map(PropertyRangeBound::from);
        with_engine_ref(self, py, move |eng| {
            Ok(IdArray {
                ids: Arc::new(eng.find_nodes_range(
                    type_id,
                    &prop_key,
                    lower.as_ref(),
                    upper.as_ref(),
                )?),
            })
        })
    }

    // --- Traversal ---

    #[pyo3(signature = (node_id, *, direction="outgoing", type_filter=None, limit=None, at_epoch=None, decay_lambda=None))]
    fn neighbors(
        &self,
        py: Python<'_>,
        node_id: u64,
        direction: &str,
        type_filter: Option<Vec<u32>>,
        limit: Option<usize>,
        at_epoch: Option<i64>,
        decay_lambda: Option<f64>,
    ) -> PyResult<Vec<PyNeighborEntry>> {
        let dir = parse_direction(direction)?;
        let dl = decay_lambda.map(|v| v as f32);
        let opts = NeighborOptions {
            direction: dir,
            type_filter,
            limit,
            at_epoch,
            decay_lambda: dl,
        };
        with_engine_ref(self, py, move |eng| {
            Ok(eng
                .neighbors(node_id, &opts)?
                .into_iter()
                .map(PyNeighborEntry::from)
                .collect())
        })
    }

    #[pyo3(signature = (start, max_depth, *, min_depth=1, direction="outgoing", edge_type_filter=None, node_type_filter=None, at_epoch=None, decay_lambda=None, limit=None, cursor=None))]
    fn traverse(
        &self,
        py: Python<'_>,
        start: u64,
        max_depth: u32,
        min_depth: u32,
        direction: &str,
        edge_type_filter: Option<Vec<u32>>,
        node_type_filter: Option<Vec<u32>>,
        at_epoch: Option<i64>,
        decay_lambda: Option<f64>,
        limit: Option<usize>,
        cursor: Option<PyTraversalCursor>,
    ) -> PyResult<PyTraversalPageResult> {
        let dir = parse_direction(direction)?;
        let cursor = cursor.map(TraversalCursor::from);
        let opts = TraverseOptions {
            min_depth,
            direction: dir,
            edge_type_filter,
            node_type_filter,
            at_epoch,
            decay_lambda,
            limit,
            cursor,
        };
        with_engine_ref(self, py, move |eng| {
            Ok(PyTraversalPageResult::from(
                eng.traverse(start, max_depth, &opts)?,
            ))
        })
    }

    #[pyo3(signature = (node_id, k, *, direction="outgoing", type_filter=None, scoring="weight", at_epoch=None, decay_lambda=None))]
    fn top_k_neighbors(
        &self,
        py: Python<'_>,
        node_id: u64,
        k: usize,
        direction: &str,
        type_filter: Option<Vec<u32>>,
        scoring: &str,
        at_epoch: Option<i64>,
        decay_lambda: Option<f64>,
    ) -> PyResult<Vec<PyNeighborEntry>> {
        let dir = parse_direction(direction)?;
        let sm = parse_scoring_mode(scoring, decay_lambda)?;
        let opts = TopKOptions {
            direction: dir,
            type_filter,
            scoring: sm,
            at_epoch,
        };
        with_engine_ref(self, py, move |eng| {
            Ok(eng
                .top_k_neighbors(node_id, k, &opts)?
                .into_iter()
                .map(PyNeighborEntry::from)
                .collect())
        })
    }

    #[pyo3(signature = (start_node_id, max_depth, *, direction="outgoing", edge_type_filter=None, at_epoch=None))]
    fn extract_subgraph(
        &self,
        py: Python<'_>,
        start_node_id: u64,
        max_depth: u32,
        direction: &str,
        edge_type_filter: Option<Vec<u32>>,
        at_epoch: Option<i64>,
    ) -> PyResult<PySubgraph> {
        let dir = parse_direction(direction)?;
        let opts = SubgraphOptions {
            direction: dir,
            edge_type_filter,
            at_epoch,
        };
        with_engine_ref(self, py, move |eng| {
            let sg = eng.extract_subgraph(start_node_id, max_depth, &opts)?;
            Ok(PySubgraph::from(sg))
        })
    }

    /// Batch neighbor query: fetch neighbors for multiple nodes in one call.
    /// Returns dict[int, list[PyNeighborEntry]] mapping each queried node_id to its neighbors.
    #[pyo3(signature = (node_ids, *, direction="outgoing", type_filter=None, at_epoch=None, decay_lambda=None))]
    fn neighbors_batch(
        &self,
        py: Python<'_>,
        node_ids: Vec<u64>,
        direction: &str,
        type_filter: Option<Vec<u32>>,
        at_epoch: Option<i64>,
        decay_lambda: Option<f64>,
    ) -> PyResult<HashMap<u64, Vec<PyNeighborEntry>>> {
        let dir = parse_direction(direction)?;
        let dl = decay_lambda.map(|v| v as f32);
        let opts = NeighborOptions {
            direction: dir,
            type_filter,
            limit: None,
            at_epoch,
            decay_lambda: dl,
        };
        with_engine_ref(self, py, move |eng| {
            let map = eng.neighbors_batch(&node_ids, &opts)?;
            Ok(map
                .into_iter()
                .map(|(k, v)| (k, v.into_iter().map(PyNeighborEntry::from).collect()))
                .collect())
        })
    }

    // --- Degree counts + aggregations (Phase 18a) ---

    #[pyo3(signature = (node_id, *, direction="outgoing", type_filter=None, at_epoch=None))]
    fn degree(
        &self,
        py: Python<'_>,
        node_id: u64,
        direction: &str,
        type_filter: Option<Vec<u32>>,
        at_epoch: Option<i64>,
    ) -> PyResult<u64> {
        let dir = parse_direction(direction)?;
        let opts = DegreeOptions {
            direction: dir,
            type_filter,
            at_epoch,
        };
        with_engine_ref(self, py, move |eng| eng.degree(node_id, &opts))
    }

    #[pyo3(signature = (node_id, *, direction="outgoing", type_filter=None, at_epoch=None))]
    fn sum_edge_weights(
        &self,
        py: Python<'_>,
        node_id: u64,
        direction: &str,
        type_filter: Option<Vec<u32>>,
        at_epoch: Option<i64>,
    ) -> PyResult<f64> {
        let dir = parse_direction(direction)?;
        let opts = DegreeOptions {
            direction: dir,
            type_filter,
            at_epoch,
        };
        with_engine_ref(self, py, move |eng| eng.sum_edge_weights(node_id, &opts))
    }

    #[pyo3(signature = (node_id, *, direction="outgoing", type_filter=None, at_epoch=None))]
    fn avg_edge_weight(
        &self,
        py: Python<'_>,
        node_id: u64,
        direction: &str,
        type_filter: Option<Vec<u32>>,
        at_epoch: Option<i64>,
    ) -> PyResult<Option<f64>> {
        let dir = parse_direction(direction)?;
        let opts = DegreeOptions {
            direction: dir,
            type_filter,
            at_epoch,
        };
        with_engine_ref(self, py, move |eng| eng.avg_edge_weight(node_id, &opts))
    }

    #[pyo3(signature = (node_ids, *, direction="outgoing", type_filter=None, at_epoch=None))]
    fn degrees(
        &self,
        py: Python<'_>,
        node_ids: Vec<u64>,
        direction: &str,
        type_filter: Option<Vec<u32>>,
        at_epoch: Option<i64>,
    ) -> PyResult<NodeIdMap<u64>> {
        let dir = parse_direction(direction)?;
        let opts = DegreeOptions {
            direction: dir,
            type_filter,
            at_epoch,
        };
        with_engine_ref(self, py, move |eng| eng.degrees(&node_ids, &opts))
    }

    // --- Shortest path (Phase 18b) ---

    #[pyo3(signature = (from_id, to_id, *, direction="outgoing", type_filter=None, weight_field=None, at_epoch=None, max_depth=None, max_cost=None))]
    fn shortest_path(
        &self,
        py: Python<'_>,
        from_id: u64,
        to_id: u64,
        direction: &str,
        type_filter: Option<Vec<u32>>,
        weight_field: Option<&str>,
        at_epoch: Option<i64>,
        max_depth: Option<u32>,
        max_cost: Option<f64>,
    ) -> PyResult<Option<PyShortestPath>> {
        let dir = parse_direction(direction)?;
        let opts = ShortestPathOptions {
            direction: dir,
            type_filter,
            weight_field: weight_field.map(|s| s.to_string()),
            at_epoch,
            max_depth,
            max_cost,
        };
        with_engine_ref(self, py, move |eng| {
            Ok(eng
                .shortest_path(from_id, to_id, &opts)?
                .map(PyShortestPath::from))
        })
    }

    #[pyo3(signature = (from_id, to_id, *, direction="outgoing", type_filter=None, at_epoch=None, max_depth=None))]
    fn is_connected(
        &self,
        py: Python<'_>,
        from_id: u64,
        to_id: u64,
        direction: &str,
        type_filter: Option<Vec<u32>>,
        at_epoch: Option<i64>,
        max_depth: Option<u32>,
    ) -> PyResult<bool> {
        let dir = parse_direction(direction)?;
        let opts = IsConnectedOptions {
            direction: dir,
            type_filter,
            at_epoch,
            max_depth,
        };
        with_engine_ref(self, py, move |eng| eng.is_connected(from_id, to_id, &opts))
    }

    #[pyo3(signature = (from_id, to_id, *, direction="outgoing", type_filter=None, weight_field=None, at_epoch=None, max_depth=None, max_cost=None, max_paths=None))]
    fn all_shortest_paths(
        &self,
        py: Python<'_>,
        from_id: u64,
        to_id: u64,
        direction: &str,
        type_filter: Option<Vec<u32>>,
        weight_field: Option<&str>,
        at_epoch: Option<i64>,
        max_depth: Option<u32>,
        max_cost: Option<f64>,
        max_paths: Option<usize>,
    ) -> PyResult<Vec<PyShortestPath>> {
        let dir = parse_direction(direction)?;
        let opts = AllShortestPathsOptions {
            direction: dir,
            type_filter,
            weight_field: weight_field.map(|s| s.to_string()),
            at_epoch,
            max_depth,
            max_cost,
            max_paths,
        };
        with_engine_ref(self, py, move |eng| {
            Ok(eng
                .all_shortest_paths(from_id, to_id, &opts)?
                .into_iter()
                .map(PyShortestPath::from)
                .collect())
        })
    }

    // --- Binary batch upserts ---

    /// Batch upsert nodes from a packed binary buffer.
    ///
    /// Binary format (little-endian):
    ///   [count: u32]
    ///   per node:
    ///     [type_id: u32][weight: f32][key_len: u16][key: utf8][props_len: u32][props: json utf8]
    fn batch_upsert_nodes_binary(&self, py: Python<'_>, buffer: &[u8]) -> PyResult<Vec<u64>> {
        let inputs = decode_node_batch_py(buffer)?;
        with_engine(self, py, move |eng| eng.batch_upsert_nodes(&inputs))
    }

    /// Batch upsert edges from a packed binary buffer.
    ///
    /// Binary format (little-endian):
    ///   [count: u32]
    ///   per edge:
    ///     [from: u64][to: u64][type_id: u32][weight: f32]
    ///     [valid_from: i64][valid_to: i64][props_len: u32][props: json utf8]
    fn batch_upsert_edges_binary(&self, py: Python<'_>, buffer: &[u8]) -> PyResult<Vec<u64>> {
        let inputs = decode_edge_batch_py(buffer)?;
        with_engine(self, py, move |eng| eng.batch_upsert_edges(&inputs))
    }

    // --- Retention ---

    #[pyo3(signature = (*, max_age_ms=None, max_weight=None, type_id=None))]
    fn prune(
        &self,
        py: Python<'_>,
        max_age_ms: Option<i64>,
        max_weight: Option<f64>,
        type_id: Option<u32>,
    ) -> PyResult<PyPruneResult> {
        let policy = PrunePolicy {
            max_age_ms,
            max_weight: max_weight.map(|v| v as f32),
            type_id,
        };
        with_engine(self, py, move |eng| {
            Ok(PyPruneResult::from(eng.prune(&policy)?))
        })
    }

    #[pyo3(signature = (name, *, max_age_ms=None, max_weight=None, type_id=None))]
    fn set_prune_policy(
        &self,
        py: Python<'_>,
        name: String,
        max_age_ms: Option<i64>,
        max_weight: Option<f64>,
        type_id: Option<u32>,
    ) -> PyResult<()> {
        let policy = PrunePolicy {
            max_age_ms,
            max_weight: max_weight.map(|v| v as f32),
            type_id,
        };
        with_engine(self, py, move |eng| eng.set_prune_policy(&name, policy))
    }

    fn remove_prune_policy(&self, py: Python<'_>, name: String) -> PyResult<bool> {
        with_engine(self, py, move |eng| eng.remove_prune_policy(&name))
    }

    fn list_prune_policies(&self, py: Python<'_>) -> PyResult<Vec<PyNamedPrunePolicy>> {
        with_engine_ref(self, py, |eng| {
            Ok(eng
                .list_prune_policies()?
                .into_iter()
                .map(|(name, policy)| PyNamedPrunePolicy {
                    name,
                    max_age_ms: policy.max_age_ms,
                    max_weight: policy.max_weight.map(|w| w as f64),
                    type_id: policy.type_id,
                })
                .collect())
        })
    }

    // --- Maintenance ---

    fn sync(&self, py: Python<'_>) -> PyResult<()> {
        with_engine_ref(self, py, |eng| eng.sync())
    }

    fn flush(&self, py: Python<'_>) -> PyResult<Option<PySegmentInfo>> {
        with_engine(self, py, |eng| {
            Ok(eng.flush()?.map(|si| PySegmentInfo {
                id: si.id,
                node_count: si.node_count,
                edge_count: si.edge_count,
            }))
        })
    }

    fn ingest_mode(&self, py: Python<'_>) -> PyResult<()> {
        with_engine(self, py, |eng| eng.ingest_mode())
    }

    fn end_ingest(&self, py: Python<'_>) -> PyResult<Option<PyCompactionStats>> {
        with_engine(self, py, |eng| {
            Ok(eng.end_ingest()?.map(PyCompactionStats::from))
        })
    }

    fn compact(&self, py: Python<'_>) -> PyResult<Option<PyCompactionStats>> {
        with_engine(self, py, |eng| {
            Ok(eng.compact()?.map(PyCompactionStats::from))
        })
    }

    fn compact_with_progress(
        &self,
        py: Python<'_>,
        callback: PyObject,
    ) -> PyResult<Option<PyCompactionStats>> {
        let engine = clone_engine_handle(&self.inner)?;
        let captured_err: Arc<std::sync::Mutex<Option<PyErr>>> =
            Arc::new(std::sync::Mutex::new(None));
        let err_clone = captured_err.clone();
        // We can't hold the GIL for the whole compaction, but we need it
        // for callback invocations. Use a closure that acquires the GIL
        // only when calling the Python callback.
        let engine_result = py.allow_threads(move || {
            let result = engine
                .compact_with_progress(|progress| {
                    Python::with_gil(|py| {
                        let py_progress = PyCompactionProgress::from(progress);
                        match callback.call1(py, (py_progress,)) {
                            Ok(result) => result.extract::<bool>(py).unwrap_or(true),
                            Err(e) => {
                                *err_clone.lock().unwrap() = Some(e);
                                false // Cancel compaction
                            }
                        }
                    })
                })
                .map_err(to_py_err)?;
            Ok(result.map(PyCompactionStats::from))
        });
        // If cancellation was due to a Python error, re-raise that instead
        if let Some(py_err) = captured_err.lock().unwrap().take() {
            return Err(py_err);
        }
        engine_result
    }

    // --- Pagination ---

    #[pyo3(signature = (type_id, *, limit=None, after=None))]
    fn nodes_by_type_paged(
        &self,
        py: Python<'_>,
        type_id: u32,
        limit: Option<usize>,
        after: Option<u64>,
    ) -> PyResult<PyIdPageResult> {
        let page = PageRequest { limit, after };
        with_engine_ref(self, py, move |eng| {
            Ok(PyIdPageResult::from(
                eng.nodes_by_type_paged(type_id, &page)?,
            ))
        })
    }

    #[pyo3(signature = (type_id, *, limit=None, after=None))]
    fn edges_by_type_paged(
        &self,
        py: Python<'_>,
        type_id: u32,
        limit: Option<usize>,
        after: Option<u64>,
    ) -> PyResult<PyIdPageResult> {
        let page = PageRequest { limit, after };
        with_engine_ref(self, py, move |eng| {
            Ok(PyIdPageResult::from(
                eng.edges_by_type_paged(type_id, &page)?,
            ))
        })
    }

    #[pyo3(signature = (type_id, *, limit=None, after=None))]
    fn get_nodes_by_type_paged(
        &self,
        py: Python<'_>,
        type_id: u32,
        limit: Option<usize>,
        after: Option<u64>,
    ) -> PyResult<PyNodePageResult> {
        let page = PageRequest { limit, after };
        with_engine_ref(self, py, move |eng| {
            let result = eng.get_nodes_by_type_paged(type_id, &page)?;
            Ok(PyNodePageResult {
                items: result.items.into_iter().map(PyNodeRecord::from).collect(),
                next_cursor: result.next_cursor,
            })
        })
    }

    #[pyo3(signature = (type_id, *, limit=None, after=None))]
    fn get_edges_by_type_paged(
        &self,
        py: Python<'_>,
        type_id: u32,
        limit: Option<usize>,
        after: Option<u64>,
    ) -> PyResult<PyEdgePageResult> {
        let page = PageRequest { limit, after };
        with_engine_ref(self, py, move |eng| {
            let result = eng.get_edges_by_type_paged(type_id, &page)?;
            Ok(PyEdgePageResult {
                items: result.items.into_iter().map(PyEdgeRecord::from).collect(),
                next_cursor: result.next_cursor,
            })
        })
    }

    #[pyo3(signature = (type_id, prop_key, prop_value, *, limit=None, after=None))]
    fn find_nodes_paged(
        &self,
        py: Python<'_>,
        type_id: u32,
        prop_key: String,
        prop_value: PyObject,
        limit: Option<usize>,
        after: Option<u64>,
    ) -> PyResult<PyIdPageResult> {
        let pv = py_to_prop_value(py, prop_value.bind(py))?;
        let page = PageRequest { limit, after };
        with_engine_ref(self, py, move |eng| {
            Ok(PyIdPageResult::from(
                eng.find_nodes_paged(type_id, &prop_key, &pv, &page)?,
            ))
        })
    }

    #[pyo3(signature = (type_id, from_ms, to_ms, *, limit=None, after=None))]
    fn find_nodes_by_time_range_paged(
        &self,
        py: Python<'_>,
        type_id: u32,
        from_ms: i64,
        to_ms: i64,
        limit: Option<usize>,
        after: Option<u64>,
    ) -> PyResult<PyIdPageResult> {
        let page = PageRequest { limit, after };
        with_engine_ref(self, py, move |eng| {
            Ok(PyIdPageResult::from(eng.find_nodes_by_time_range_paged(
                type_id, from_ms, to_ms, &page,
            )?))
        })
    }

    #[pyo3(signature = (type_id, prop_key, lower=None, upper=None, *, limit=None, after=None))]
    fn find_nodes_range_paged(
        &self,
        py: Python<'_>,
        type_id: u32,
        prop_key: String,
        lower: Option<PyPropertyRangeBound>,
        upper: Option<PyPropertyRangeBound>,
        limit: Option<usize>,
        after: Option<PyPropertyRangeCursor>,
    ) -> PyResult<PyPropertyRangePageResult> {
        let lower = lower.map(PropertyRangeBound::from);
        let upper = upper.map(PropertyRangeBound::from);
        let page = PropertyRangePageRequest {
            limit,
            after: after.map(PropertyRangeCursor::from),
        };
        with_engine_ref(self, py, move |eng| {
            Ok(PyPropertyRangePageResult::from(
                eng.find_nodes_range_paged(
                    type_id,
                    &prop_key,
                    lower.as_ref(),
                    upper.as_ref(),
                    &page,
                )?,
            ))
        })
    }

    #[pyo3(signature = (node_id, *, direction="outgoing", type_filter=None, limit=None, after=None, at_epoch=None, decay_lambda=None))]
    fn neighbors_paged(
        &self,
        py: Python<'_>,
        node_id: u64,
        direction: &str,
        type_filter: Option<Vec<u32>>,
        limit: Option<usize>,
        after: Option<u64>,
        at_epoch: Option<i64>,
        decay_lambda: Option<f64>,
    ) -> PyResult<PyNeighborPageResult> {
        let dir = parse_direction(direction)?;
        let dl = decay_lambda.map(|v| v as f32);
        let opts = NeighborOptions {
            direction: dir,
            type_filter,
            limit: None,
            at_epoch,
            decay_lambda: dl,
        };
        let page = PageRequest { limit, after };
        with_engine_ref(self, py, move |eng| {
            let result = eng.neighbors_paged(node_id, &opts, &page)?;
            Ok(PyNeighborPageResult {
                items: result
                    .items
                    .into_iter()
                    .map(PyNeighborEntry::from)
                    .collect(),
                next_cursor: result.next_cursor,
            })
        })
    }

    // --- Analytics ---

    #[pyo3(signature = (seed_node_ids, *, algorithm=None, damping_factor=None, max_iterations=None, epsilon=None, approx_residual_tolerance=None, edge_type_filter=None, max_results=None))]
    fn personalized_pagerank(
        &self,
        py: Python<'_>,
        seed_node_ids: Vec<u64>,
        algorithm: Option<&str>,
        damping_factor: Option<f64>,
        max_iterations: Option<u32>,
        epsilon: Option<f64>,
        approx_residual_tolerance: Option<f64>,
        edge_type_filter: Option<Vec<u32>>,
        max_results: Option<usize>,
    ) -> PyResult<PyPprResult> {
        let defaults = PprOptions::default();
        let options = PprOptions {
            algorithm: parse_ppr_algorithm(algorithm)?,
            damping_factor: damping_factor.unwrap_or(defaults.damping_factor),
            max_iterations: max_iterations.unwrap_or(defaults.max_iterations),
            epsilon: epsilon.unwrap_or(defaults.epsilon),
            approx_residual_tolerance: approx_residual_tolerance
                .unwrap_or(defaults.approx_residual_tolerance),
            edge_type_filter,
            max_results,
        };
        with_engine_ref(self, py, move |eng| {
            Ok(PyPprResult::from(
                eng.personalized_pagerank(&seed_node_ids, &options)?,
            ))
        })
    }

    #[pyo3(signature = (*, node_type_filter=None, edge_type_filter=None, include_weights=true))]
    fn export_adjacency(
        &self,
        py: Python<'_>,
        node_type_filter: Option<Vec<u32>>,
        edge_type_filter: Option<Vec<u32>>,
        include_weights: bool,
    ) -> PyResult<PyAdjacencyExport> {
        let options = ExportOptions {
            node_type_filter,
            edge_type_filter,
            include_weights,
        };
        with_engine_ref(self, py, move |eng| {
            Ok(PyAdjacencyExport::from(eng.export_adjacency(&options)?))
        })
    }

    // --- Connected Components (Phase 18d) ---

    /// Weakly connected components over the visible graph.
    ///
    /// Returns a dict mapping each visible node ID to its component ID
    /// (the minimum node ID in that component). WCC treats all edges as
    /// undirected. Isolated nodes become singleton components.
    #[pyo3(signature = (*, edge_type_filter=None, node_type_filter=None, at_epoch=None))]
    fn connected_components(
        &self,
        py: Python<'_>,
        edge_type_filter: Option<Vec<u32>>,
        node_type_filter: Option<Vec<u32>>,
        at_epoch: Option<i64>,
    ) -> PyResult<NodeIdMap<u64>> {
        let opts = ComponentOptions {
            edge_type_filter,
            node_type_filter,
            at_epoch,
        };
        with_engine_ref(self, py, move |eng| eng.connected_components(&opts))
    }

    /// Returns the sorted list of node IDs in the same weakly connected
    /// component as the given node. Returns an empty list if the node
    /// doesn't exist, is deleted, or is hidden by prune policy.
    #[pyo3(signature = (node_id, *, edge_type_filter=None, node_type_filter=None, at_epoch=None))]
    fn component_of(
        &self,
        py: Python<'_>,
        node_id: u64,
        edge_type_filter: Option<Vec<u32>>,
        node_type_filter: Option<Vec<u32>>,
        at_epoch: Option<i64>,
    ) -> PyResult<Vec<u64>> {
        let opts = ComponentOptions {
            edge_type_filter,
            node_type_filter,
            at_epoch,
        };
        with_engine_ref(self, py, move |eng| eng.component_of(node_id, &opts))
    }

    // --- Vector search (Phase 19) ---

    #[pyo3(signature = (mode, k, *, dense_query=None, sparse_query=None, type_filter=None, ef_search=None, scope_start_node_id=None, scope_max_depth=None, scope_direction=None, scope_edge_type_filter=None, scope_at_epoch=None, dense_weight=None, sparse_weight=None, fusion_mode=None))]
    fn vector_search(
        &self,
        py: Python<'_>,
        mode: &str,
        k: usize,
        dense_query: Option<Vec<f32>>,
        sparse_query: Option<Vec<(u32, f32)>>,
        type_filter: Option<Vec<u32>>,
        ef_search: Option<usize>,
        scope_start_node_id: Option<u64>,
        scope_max_depth: Option<u32>,
        scope_direction: Option<&str>,
        scope_edge_type_filter: Option<Vec<u32>>,
        scope_at_epoch: Option<i64>,
        dense_weight: Option<f32>,
        sparse_weight: Option<f32>,
        fusion_mode: Option<&str>,
    ) -> PyResult<Vec<PyVectorHit>> {
        let mode = parse_vector_search_mode(mode)?;
        let fusion = parse_fusion_mode(fusion_mode)?;
        let scope = match scope_start_node_id {
            None => None,
            Some(start) => Some(VectorSearchScope {
                start_node_id: start,
                max_depth: scope_max_depth.ok_or_else(|| {
                    PyErr::new::<PyValueError, _>(
                        "scope_max_depth is required when scope_start_node_id is provided",
                    )
                })?,
                direction: parse_direction(scope_direction.unwrap_or("outgoing"))?,
                edge_type_filter: scope_edge_type_filter,
                at_epoch: scope_at_epoch,
            }),
        };
        let request = VectorSearchRequest {
            mode,
            dense_query,
            sparse_query,
            k,
            type_filter,
            ef_search,
            scope,
            dense_weight,
            sparse_weight,
            fusion_mode: fusion,
        };
        with_engine_ref(self, py, move |eng| {
            let hits = eng.vector_search(&request)?;
            Ok(hits
                .into_iter()
                .map(|h| PyVectorHit {
                    node_id: h.node_id,
                    score: h.score as f64,
                })
                .collect())
        })
    }
}

#[pyclass]
pub struct PyWriteTxn {
    inner: Arc<Mutex<Option<WriteTxn>>>,
}

#[pymethods]
impl PyWriteTxn {
    #[pyo3(signature = (type_id, key, *, props=None, weight=1.0, dense_vector=None, sparse_vector=None))]
    fn upsert_node(
        &self,
        py: Python<'_>,
        type_id: u32,
        key: String,
        props: Option<&Bound<'_, PyDict>>,
        weight: f64,
        dense_vector: Option<Vec<f32>>,
        sparse_vector: Option<Vec<(u32, f32)>>,
    ) -> PyResult<PyObject> {
        let options = UpsertNodeOptions {
            props: convert_py_props(py, props)?,
            weight: weight as f32,
            dense_vector,
            sparse_vector,
        };
        with_py_txn(&self.inner, |txn| {
            txn.upsert_node(type_id, &key, options).map_err(to_py_err)
        })?;
        txn_node_ref_to_py(py, TxnNodeRef::Key { type_id, key })
    }

    #[pyo3(signature = (alias, type_id, key, *, props=None, weight=1.0, dense_vector=None, sparse_vector=None))]
    fn upsert_node_as(
        &self,
        py: Python<'_>,
        alias: String,
        type_id: u32,
        key: String,
        props: Option<&Bound<'_, PyDict>>,
        weight: f64,
        dense_vector: Option<Vec<f32>>,
        sparse_vector: Option<Vec<(u32, f32)>>,
    ) -> PyResult<PyObject> {
        let options = UpsertNodeOptions {
            props: convert_py_props(py, props)?,
            weight: weight as f32,
            dense_vector,
            sparse_vector,
        };
        with_py_txn(&self.inner, |txn| {
            txn.upsert_node_as(&alias, type_id, &key, options)
                .map_err(to_py_err)
                .and_then(|r| txn_node_ref_to_py(py, r))
        })
    }

    #[pyo3(signature = (from_ref, to_ref, type_id, *, props=None, weight=1.0, valid_from=None, valid_to=None))]
    fn upsert_edge(
        &self,
        py: Python<'_>,
        from_ref: &Bound<'_, PyDict>,
        to_ref: &Bound<'_, PyDict>,
        type_id: u32,
        props: Option<&Bound<'_, PyDict>>,
        weight: f64,
        valid_from: Option<i64>,
        valid_to: Option<i64>,
    ) -> PyResult<PyObject> {
        let from = parse_txn_node_ref(from_ref)?;
        let to = parse_txn_node_ref(to_ref)?;
        let options = UpsertEdgeOptions {
            props: convert_py_props(py, props)?,
            weight: weight as f32,
            valid_from,
            valid_to,
        };
        with_py_txn(&self.inner, |txn| {
            txn.upsert_edge(from.clone(), to.clone(), type_id, options)
                .map_err(to_py_err)
        })?;
        txn_edge_ref_to_py(py, TxnEdgeRef::Triple { from, to, type_id })
    }

    #[pyo3(signature = (alias, from_ref, to_ref, type_id, *, props=None, weight=1.0, valid_from=None, valid_to=None))]
    fn upsert_edge_as(
        &self,
        py: Python<'_>,
        alias: String,
        from_ref: &Bound<'_, PyDict>,
        to_ref: &Bound<'_, PyDict>,
        type_id: u32,
        props: Option<&Bound<'_, PyDict>>,
        weight: f64,
        valid_from: Option<i64>,
        valid_to: Option<i64>,
    ) -> PyResult<PyObject> {
        let from = parse_txn_node_ref(from_ref)?;
        let to = parse_txn_node_ref(to_ref)?;
        let options = UpsertEdgeOptions {
            props: convert_py_props(py, props)?,
            weight: weight as f32,
            valid_from,
            valid_to,
        };
        with_py_txn(&self.inner, |txn| {
            txn.upsert_edge_as(&alias, from, to, type_id, options)
                .map_err(to_py_err)
                .and_then(|r| txn_edge_ref_to_py(py, r))
        })
    }

    fn delete_node(&self, target: &Bound<'_, PyDict>) -> PyResult<()> {
        let target = parse_txn_node_ref(target)?;
        with_py_txn(&self.inner, |txn| {
            txn.delete_node(target).map_err(to_py_err)
        })
    }

    fn delete_edge(&self, target: &Bound<'_, PyDict>) -> PyResult<()> {
        let target = parse_txn_edge_ref(target)?;
        with_py_txn(&self.inner, |txn| {
            txn.delete_edge(target).map_err(to_py_err)
        })
    }

    fn invalidate_edge(&self, target: &Bound<'_, PyDict>, valid_to: i64) -> PyResult<()> {
        let target = parse_txn_edge_ref(target)?;
        with_py_txn(&self.inner, |txn| {
            txn.invalidate_edge(target, valid_to).map_err(to_py_err)
        })
    }

    fn stage(&self, py: Python<'_>, operations: &Bound<'_, PyList>) -> PyResult<()> {
        let intents = parse_txn_operations(py, operations)?;
        with_py_txn(&self.inner, |txn| {
            txn.stage_intents(intents).map_err(to_py_err)
        })
    }

    fn get_node(&self, py: Python<'_>, target: &Bound<'_, PyDict>) -> PyResult<Option<PyObject>> {
        let target = parse_txn_node_ref(target)?;
        with_py_txn_ref(&self.inner, |txn| {
            txn.get_node(target)
                .map_err(to_py_err)?
                .map(|v| txn_node_view_to_py(py, v))
                .transpose()
        })
    }

    fn get_edge(&self, py: Python<'_>, target: &Bound<'_, PyDict>) -> PyResult<Option<PyObject>> {
        let target = parse_txn_edge_ref(target)?;
        with_py_txn_ref(&self.inner, |txn| {
            txn.get_edge(target)
                .map_err(to_py_err)?
                .map(|v| txn_edge_view_to_py(py, v))
                .transpose()
        })
    }

    fn get_node_by_key(
        &self,
        py: Python<'_>,
        type_id: u32,
        key: String,
    ) -> PyResult<Option<PyObject>> {
        with_py_txn_ref(&self.inner, |txn| {
            txn.get_node_by_key(type_id, &key)
                .map_err(to_py_err)?
                .map(|v| txn_node_view_to_py(py, v))
                .transpose()
        })
    }

    fn get_edge_by_triple(
        &self,
        py: Python<'_>,
        from_ref: &Bound<'_, PyDict>,
        to_ref: &Bound<'_, PyDict>,
        type_id: u32,
    ) -> PyResult<Option<PyObject>> {
        let from = parse_txn_node_ref(from_ref)?;
        let to = parse_txn_node_ref(to_ref)?;
        with_py_txn_ref(&self.inner, |txn| {
            txn.get_edge_by_triple(from, to, type_id)
                .map_err(to_py_err)?
                .map(|v| txn_edge_view_to_py(py, v))
                .transpose()
        })
    }

    fn commit(&self, py: Python<'_>) -> PyResult<PyTxnCommitResult> {
        let mut txn = {
            let mut guard = self.inner.lock().map_err(lock_err)?;
            guard
                .take()
                .ok_or_else(|| OverGraphError::new_err(EngineError::TxnClosed.to_string()))?
        };
        let result = py.allow_threads(move || txn.commit()).map_err(to_py_err)?;
        Ok(PyTxnCommitResult::from(result))
    }

    fn rollback(&self) -> PyResult<()> {
        with_py_txn_take(&self.inner, |txn| txn.rollback().map_err(to_py_err))
    }
}

// ============================================================
// Python-facing types
// ============================================================

#[pyclass]
#[derive(Clone)]
pub struct PyDbStats {
    #[pyo3(get)]
    pub pending_wal_bytes: usize,
    #[pyo3(get)]
    pub segment_count: usize,
    #[pyo3(get)]
    pub node_tombstone_count: usize,
    #[pyo3(get)]
    pub edge_tombstone_count: usize,
    #[pyo3(get)]
    pub last_compaction_ms: Option<i64>,
    #[pyo3(get)]
    pub wal_sync_mode: String,
    #[pyo3(get)]
    pub active_memtable_bytes: usize,
    #[pyo3(get)]
    pub immutable_memtable_bytes: usize,
    #[pyo3(get)]
    pub immutable_memtable_count: usize,
    #[pyo3(get)]
    pub pending_flush_count: usize,
    #[pyo3(get)]
    pub active_wal_generation_id: u64,
    #[pyo3(get)]
    pub oldest_retained_wal_generation_id: u64,
}

impl From<DbStats> for PyDbStats {
    fn from(s: DbStats) -> Self {
        PyDbStats {
            pending_wal_bytes: s.pending_wal_bytes,
            segment_count: s.segment_count,
            node_tombstone_count: s.node_tombstone_count,
            edge_tombstone_count: s.edge_tombstone_count,
            last_compaction_ms: s.last_compaction_ms,
            wal_sync_mode: s.wal_sync_mode,
            active_memtable_bytes: s.active_memtable_bytes,
            immutable_memtable_bytes: s.immutable_memtable_bytes,
            immutable_memtable_count: s.immutable_memtable_count,
            pending_flush_count: s.pending_flush_count,
            active_wal_generation_id: s.active_wal_generation_id,
            oldest_retained_wal_generation_id: s.oldest_retained_wal_generation_id,
        }
    }
}

#[pymethods]
impl PyDbStats {
    fn __repr__(&self) -> String {
        format!(
            "DbStats(segments={}, wal_bytes={}, tombstones=({}, {}), sync='{}', \
             immutables={}, pending_flushes={}, wal_gen={})",
            self.segment_count,
            self.pending_wal_bytes,
            self.node_tombstone_count,
            self.edge_tombstone_count,
            self.wal_sync_mode,
            self.immutable_memtable_count,
            self.pending_flush_count,
            self.active_wal_generation_id,
        )
    }
}

#[pyclass]
#[derive(Clone)]
pub struct PyNodeRecord {
    #[pyo3(get)]
    pub id: u64,
    #[pyo3(get)]
    pub type_id: u32,
    #[pyo3(get)]
    pub key: String,
    #[pyo3(get)]
    pub created_at: i64,
    #[pyo3(get)]
    pub updated_at: i64,
    #[pyo3(get)]
    pub weight: f64,
    props_internal: BTreeMap<String, PropValue>,
}

impl From<NodeRecord> for PyNodeRecord {
    fn from(n: NodeRecord) -> Self {
        PyNodeRecord {
            id: n.id,
            type_id: n.type_id,
            key: n.key,
            created_at: n.created_at,
            updated_at: n.updated_at,
            weight: n.weight as f64,
            props_internal: n.props,
        }
    }
}

#[pymethods]
impl PyNodeRecord {
    #[getter]
    fn props(&self, py: Python<'_>) -> PyResult<PyObject> {
        props_to_py(py, &self.props_internal)
    }

    fn __repr__(&self) -> String {
        format!(
            "NodeRecord(id={}, type_id={}, key='{}')",
            self.id, self.type_id, self.key
        )
    }
}

#[pyclass]
#[derive(Clone)]
pub struct PyEdgeRecord {
    #[pyo3(get)]
    pub id: u64,
    #[pyo3(get)]
    pub from_id: u64,
    #[pyo3(get)]
    pub to_id: u64,
    #[pyo3(get)]
    pub type_id: u32,
    #[pyo3(get)]
    pub created_at: i64,
    #[pyo3(get)]
    pub updated_at: i64,
    #[pyo3(get)]
    pub weight: f64,
    #[pyo3(get)]
    pub valid_from: i64,
    #[pyo3(get)]
    pub valid_to: i64,
    props_internal: BTreeMap<String, PropValue>,
}

impl From<EdgeRecord> for PyEdgeRecord {
    fn from(e: EdgeRecord) -> Self {
        PyEdgeRecord {
            id: e.id,
            from_id: e.from,
            to_id: e.to,
            type_id: e.type_id,
            created_at: e.created_at,
            updated_at: e.updated_at,
            weight: e.weight as f64,
            valid_from: e.valid_from,
            valid_to: e.valid_to,
            props_internal: e.props,
        }
    }
}

#[pymethods]
impl PyEdgeRecord {
    #[getter]
    fn props(&self, py: Python<'_>) -> PyResult<PyObject> {
        props_to_py(py, &self.props_internal)
    }

    fn __repr__(&self) -> String {
        format!(
            "EdgeRecord(id={}, {}->{})",
            self.id, self.from_id, self.to_id
        )
    }
}

#[pyclass]
#[derive(Clone)]
pub struct PyPatchResult {
    #[pyo3(get)]
    pub node_ids: Vec<u64>,
    #[pyo3(get)]
    pub edge_ids: Vec<u64>,
}

#[pymethods]
impl PyPatchResult {
    fn __repr__(&self) -> String {
        format!(
            "PatchResult(nodes={}, edges={})",
            self.node_ids.len(),
            self.edge_ids.len()
        )
    }
}

#[pyclass]
#[derive(Clone)]
pub struct PyTxnCommitResult {
    #[pyo3(get)]
    pub node_ids: Vec<u64>,
    #[pyo3(get)]
    pub edge_ids: Vec<u64>,
    #[pyo3(get)]
    pub node_aliases: HashMap<String, u64>,
    #[pyo3(get)]
    pub edge_aliases: HashMap<String, u64>,
}

impl From<TxnCommitResult> for PyTxnCommitResult {
    fn from(result: TxnCommitResult) -> Self {
        let node_aliases = result
            .local_node_ids
            .into_iter()
            .filter_map(|(local, id)| match local {
                TxnLocalRef::Alias(alias) => Some((alias, id)),
                TxnLocalRef::Slot(_) => None,
            })
            .collect();
        let edge_aliases = result
            .local_edge_ids
            .into_iter()
            .filter_map(|(local, id)| match local {
                TxnLocalRef::Alias(alias) => Some((alias, id)),
                TxnLocalRef::Slot(_) => None,
            })
            .collect();
        PyTxnCommitResult {
            node_ids: result.node_ids,
            edge_ids: result.edge_ids,
            node_aliases,
            edge_aliases,
        }
    }
}

#[pymethods]
impl PyTxnCommitResult {
    fn __repr__(&self) -> String {
        format!(
            "TxnCommitResult(nodes={}, edges={})",
            self.node_ids.len(),
            self.edge_ids.len()
        )
    }
}

// --- CP2 types ---

#[pyclass]
#[derive(Clone)]
pub struct PyNeighborEntry {
    #[pyo3(get)]
    pub node_id: u64,
    #[pyo3(get)]
    pub edge_id: u64,
    #[pyo3(get)]
    pub edge_type_id: u32,
    #[pyo3(get)]
    pub weight: f64,
    #[pyo3(get)]
    pub valid_from: i64,
    #[pyo3(get)]
    pub valid_to: i64,
}

impl From<NeighborEntry> for PyNeighborEntry {
    fn from(n: NeighborEntry) -> Self {
        PyNeighborEntry {
            node_id: n.node_id,
            edge_id: n.edge_id,
            edge_type_id: n.edge_type_id,
            weight: n.weight as f64,
            valid_from: n.valid_from,
            valid_to: n.valid_to,
        }
    }
}

#[pymethods]
impl PyNeighborEntry {
    fn __repr__(&self) -> String {
        format!(
            "NeighborEntry(node_id={}, edge_id={}, type={})",
            self.node_id, self.edge_id, self.edge_type_id
        )
    }
}

#[pyclass]
#[derive(Clone)]
pub struct PyTraversalHit {
    #[pyo3(get)]
    pub node_id: u64,
    #[pyo3(get)]
    pub depth: u32,
    #[pyo3(get)]
    pub via_edge_id: Option<u64>,
    #[pyo3(get)]
    pub score: Option<f64>,
}

impl From<TraversalHit> for PyTraversalHit {
    fn from(hit: TraversalHit) -> Self {
        PyTraversalHit {
            node_id: hit.node_id,
            depth: hit.depth,
            via_edge_id: hit.via_edge_id,
            score: hit.score,
        }
    }
}

#[pymethods]
impl PyTraversalHit {
    fn __repr__(&self) -> String {
        format!(
            "TraversalHit(node_id={}, depth={}, via_edge_id={:?})",
            self.node_id, self.depth, self.via_edge_id
        )
    }
}

#[pyclass]
#[derive(Clone)]
pub struct PyVectorHit {
    #[pyo3(get)]
    pub node_id: u64,
    #[pyo3(get)]
    pub score: f64,
}

#[pymethods]
impl PyVectorHit {
    fn __repr__(&self) -> String {
        format!(
            "VectorHit(node_id={}, score={:.4})",
            self.node_id, self.score
        )
    }
}

#[pyclass]
#[derive(Clone)]
pub struct PyNodePropertyIndexInfo {
    #[pyo3(get)]
    pub index_id: u64,
    #[pyo3(get)]
    pub type_id: u32,
    #[pyo3(get)]
    pub prop_key: String,
    #[pyo3(get)]
    pub kind: String,
    #[pyo3(get)]
    pub domain: Option<String>,
    #[pyo3(get)]
    pub state: String,
    #[pyo3(get)]
    pub last_error: Option<String>,
}

impl From<NodePropertyIndexInfo> for PyNodePropertyIndexInfo {
    fn from(info: NodePropertyIndexInfo) -> Self {
        let (kind, domain) = secondary_index_kind_to_py(&info.kind);
        PyNodePropertyIndexInfo {
            index_id: info.index_id,
            type_id: info.type_id,
            prop_key: info.prop_key,
            kind: kind.to_string(),
            domain: domain.map(str::to_string),
            state: secondary_index_state_to_py(info.state).to_string(),
            last_error: info.last_error,
        }
    }
}

#[pymethods]
impl PyNodePropertyIndexInfo {
    fn __repr__(&self) -> String {
        format!(
            "NodePropertyIndexInfo(index_id={}, type_id={}, prop_key='{}', kind='{}', domain={:?}, state='{}')",
            self.index_id, self.type_id, self.prop_key, self.kind, self.domain, self.state
        )
    }
}

#[pyclass]
#[derive(Clone)]
pub struct PyPropertyRangeBound {
    value_internal: PropValue,
    #[pyo3(get)]
    pub inclusive: bool,
}

impl From<PyPropertyRangeBound> for PropertyRangeBound {
    fn from(bound: PyPropertyRangeBound) -> Self {
        if bound.inclusive {
            PropertyRangeBound::Included(bound.value_internal)
        } else {
            PropertyRangeBound::Excluded(bound.value_internal)
        }
    }
}

impl From<PropertyRangeBound> for PyPropertyRangeBound {
    fn from(bound: PropertyRangeBound) -> Self {
        match bound {
            PropertyRangeBound::Included(value_internal) => PyPropertyRangeBound {
                value_internal,
                inclusive: true,
            },
            PropertyRangeBound::Excluded(value_internal) => PyPropertyRangeBound {
                value_internal,
                inclusive: false,
            },
        }
    }
}

#[pymethods]
impl PyPropertyRangeBound {
    #[new]
    #[pyo3(signature = (value, *, inclusive=true, domain))]
    fn new(value: &Bound<'_, pyo3::PyAny>, inclusive: bool, domain: &str) -> PyResult<Self> {
        let domain = parse_secondary_index_range_domain(domain)?;
        Ok(PyPropertyRangeBound {
            value_internal: py_numeric_to_prop_value(value.py(), value, domain)?,
            inclusive,
        })
    }

    #[getter]
    fn value(&self, py: Python<'_>) -> PyResult<PyObject> {
        prop_value_to_py_obj(py, &self.value_internal)
    }

    #[getter]
    fn domain(&self) -> PyResult<String> {
        Ok(range_domain_to_py(range_domain_from_prop_value(
            &self.value_internal,
            "property range bound",
        )?)
        .to_string())
    }

    fn __repr__(&self) -> PyResult<String> {
        let value = Python::with_gil(|py| prop_value_debug_repr(py, &self.value_internal))?;
        Ok(format!(
            "PropertyRangeBound(value={}, inclusive={}, domain='{}')",
            value,
            self.inclusive,
            self.domain()?
        ))
    }
}

#[pyclass]
#[derive(Clone)]
pub struct PyPropertyRangeCursor {
    value_internal: PropValue,
    #[pyo3(get)]
    pub node_id: u64,
}

impl From<PyPropertyRangeCursor> for PropertyRangeCursor {
    fn from(cursor: PyPropertyRangeCursor) -> Self {
        PropertyRangeCursor {
            value: cursor.value_internal,
            node_id: cursor.node_id,
        }
    }
}

impl From<PropertyRangeCursor> for PyPropertyRangeCursor {
    fn from(cursor: PropertyRangeCursor) -> Self {
        PyPropertyRangeCursor {
            value_internal: cursor.value,
            node_id: cursor.node_id,
        }
    }
}

#[pymethods]
impl PyPropertyRangeCursor {
    #[new]
    #[pyo3(signature = (value, node_id, *, domain))]
    fn new(value: &Bound<'_, pyo3::PyAny>, node_id: u64, domain: &str) -> PyResult<Self> {
        let domain = parse_secondary_index_range_domain(domain)?;
        Ok(PyPropertyRangeCursor {
            value_internal: py_numeric_to_prop_value(value.py(), value, domain)?,
            node_id,
        })
    }

    #[getter]
    fn value(&self, py: Python<'_>) -> PyResult<PyObject> {
        prop_value_to_py_obj(py, &self.value_internal)
    }

    #[getter]
    fn domain(&self) -> PyResult<String> {
        Ok(range_domain_to_py(range_domain_from_prop_value(
            &self.value_internal,
            "property range cursor",
        )?)
        .to_string())
    }

    fn __repr__(&self) -> PyResult<String> {
        let value = Python::with_gil(|py| prop_value_debug_repr(py, &self.value_internal))?;
        Ok(format!(
            "PropertyRangeCursor(value={}, node_id={}, domain='{}')",
            value,
            self.node_id,
            self.domain()?
        ))
    }
}

#[pyclass]
#[derive(Clone)]
pub struct PyTraversalCursor {
    #[pyo3(get)]
    pub depth: u32,
    #[pyo3(get)]
    pub last_node_id: u64,
}

impl From<TraversalCursor> for PyTraversalCursor {
    fn from(cursor: TraversalCursor) -> Self {
        PyTraversalCursor {
            depth: cursor.depth,
            last_node_id: cursor.last_node_id,
        }
    }
}

impl From<PyTraversalCursor> for TraversalCursor {
    fn from(cursor: PyTraversalCursor) -> Self {
        TraversalCursor {
            depth: cursor.depth,
            last_node_id: cursor.last_node_id,
        }
    }
}

#[pymethods]
impl PyTraversalCursor {
    #[new]
    fn new(depth: u32, last_node_id: u64) -> Self {
        PyTraversalCursor {
            depth,
            last_node_id,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "TraversalCursor(depth={}, last_node_id={})",
            self.depth, self.last_node_id
        )
    }
}

#[pyclass]
#[derive(Clone)]
pub struct PyShortestPath {
    #[pyo3(get)]
    pub nodes: Vec<u64>,
    #[pyo3(get)]
    pub edges: Vec<u64>,
    #[pyo3(get)]
    pub total_cost: f64,
}

impl From<ShortestPath> for PyShortestPath {
    fn from(sp: ShortestPath) -> Self {
        PyShortestPath {
            nodes: sp.nodes,
            edges: sp.edges,
            total_cost: sp.total_cost,
        }
    }
}

#[pymethods]
impl PyShortestPath {
    fn __repr__(&self) -> String {
        format!(
            "ShortestPath(nodes={}, edges={}, cost={:.4})",
            self.nodes.len(),
            self.edges.len(),
            self.total_cost
        )
    }
}

#[pyclass]
#[derive(Clone)]
pub struct PySubgraph {
    nodes: Vec<PyNodeRecord>,
    edges: Vec<PyEdgeRecord>,
}

impl From<Subgraph> for PySubgraph {
    fn from(sg: Subgraph) -> Self {
        PySubgraph {
            nodes: sg.nodes.into_iter().map(PyNodeRecord::from).collect(),
            edges: sg.edges.into_iter().map(PyEdgeRecord::from).collect(),
        }
    }
}

#[pymethods]
impl PySubgraph {
    #[getter]
    fn nodes(&self) -> Vec<PyNodeRecord> {
        self.nodes.clone()
    }
    #[getter]
    fn edges(&self) -> Vec<PyEdgeRecord> {
        self.edges.clone()
    }
    fn __repr__(&self) -> String {
        format!(
            "Subgraph(nodes={}, edges={})",
            self.nodes.len(),
            self.edges.len()
        )
    }
}

#[pyclass]
#[derive(Clone)]
pub struct PyPruneResult {
    #[pyo3(get)]
    pub nodes_pruned: u64,
    #[pyo3(get)]
    pub edges_pruned: u64,
}

impl From<PruneResult> for PyPruneResult {
    fn from(r: PruneResult) -> Self {
        PyPruneResult {
            nodes_pruned: r.nodes_pruned,
            edges_pruned: r.edges_pruned,
        }
    }
}

#[pymethods]
impl PyPruneResult {
    fn __repr__(&self) -> String {
        format!(
            "PruneResult(nodes={}, edges={})",
            self.nodes_pruned, self.edges_pruned
        )
    }
}

#[pyclass]
#[derive(Clone)]
pub struct PyNamedPrunePolicy {
    #[pyo3(get)]
    pub name: String,
    #[pyo3(get)]
    pub max_age_ms: Option<i64>,
    #[pyo3(get)]
    pub max_weight: Option<f64>,
    #[pyo3(get)]
    pub type_id: Option<u32>,
}

#[pymethods]
impl PyNamedPrunePolicy {
    fn __repr__(&self) -> String {
        format!(
            "PrunePolicy(name='{}', max_age_ms={:?}, max_weight={:?}, type_id={:?})",
            self.name, self.max_age_ms, self.max_weight, self.type_id
        )
    }
}

#[pyclass]
#[derive(Clone)]
pub struct PySegmentInfo {
    #[pyo3(get)]
    pub id: u64,
    #[pyo3(get)]
    pub node_count: u64,
    #[pyo3(get)]
    pub edge_count: u64,
}

#[pymethods]
impl PySegmentInfo {
    fn __repr__(&self) -> String {
        format!(
            "SegmentInfo(id={}, nodes={}, edges={})",
            self.id, self.node_count, self.edge_count
        )
    }
}

#[pyclass]
#[derive(Clone)]
pub struct PyCompactionStats {
    #[pyo3(get)]
    pub segments_merged: usize,
    #[pyo3(get)]
    pub nodes_kept: u64,
    #[pyo3(get)]
    pub nodes_removed: u64,
    #[pyo3(get)]
    pub edges_kept: u64,
    #[pyo3(get)]
    pub edges_removed: u64,
    #[pyo3(get)]
    pub duration_ms: u64,
    #[pyo3(get)]
    pub output_segment_id: u64,
    #[pyo3(get)]
    pub nodes_auto_pruned: u64,
    #[pyo3(get)]
    pub edges_auto_pruned: u64,
}

impl From<CompactionStats> for PyCompactionStats {
    fn from(s: CompactionStats) -> Self {
        PyCompactionStats {
            segments_merged: s.segments_merged,
            nodes_kept: s.nodes_kept,
            nodes_removed: s.nodes_removed,
            edges_kept: s.edges_kept,
            edges_removed: s.edges_removed,
            duration_ms: s.duration_ms,
            output_segment_id: s.output_segment_id,
            nodes_auto_pruned: s.nodes_auto_pruned,
            edges_auto_pruned: s.edges_auto_pruned,
        }
    }
}

#[pymethods]
impl PyCompactionStats {
    fn __repr__(&self) -> String {
        format!(
            "CompactionStats(merged={}, kept={}/{}, removed={}/{}, {}ms)",
            self.segments_merged,
            self.nodes_kept,
            self.edges_kept,
            self.nodes_removed,
            self.edges_removed,
            self.duration_ms,
        )
    }
}

#[pyclass]
#[derive(Clone)]
pub struct PyCompactionProgress {
    #[pyo3(get)]
    pub phase: String,
    #[pyo3(get)]
    pub segments_processed: usize,
    #[pyo3(get)]
    pub total_segments: usize,
    #[pyo3(get)]
    pub records_processed: u64,
    #[pyo3(get)]
    pub total_records: u64,
}

impl From<&CompactionProgress> for PyCompactionProgress {
    fn from(p: &CompactionProgress) -> Self {
        let phase_str = match p.phase {
            CompactionPhase::CollectingTombstones => "collecting_tombstones",
            CompactionPhase::MergingNodes => "merging_nodes",
            CompactionPhase::MergingEdges => "merging_edges",
            CompactionPhase::WritingOutput => "writing_output",
        }
        .to_string();
        PyCompactionProgress {
            phase: phase_str,
            segments_processed: p.segments_processed,
            total_segments: p.total_segments,
            records_processed: p.records_processed,
            total_records: p.total_records,
        }
    }
}

#[pymethods]
impl PyCompactionProgress {
    fn __repr__(&self) -> String {
        format!(
            "CompactionProgress(phase='{}', {}/{})",
            self.phase, self.records_processed, self.total_records
        )
    }
}

// ============================================================
// Lazy ID array wrapper
// ============================================================

/// A lazy sequence wrapper around a Vec<u64>. Data stays in Rust;
/// individual elements are only converted to Python ints on access.
/// One Python object regardless of result set size.
#[pyclass]
#[derive(Clone)]
pub struct IdArray {
    ids: Arc<Vec<u64>>,
}

#[pymethods]
impl IdArray {
    fn __len__(&self) -> usize {
        self.ids.len()
    }

    fn __getitem__(&self, index: isize) -> PyResult<u64> {
        let len = self.ids.len() as isize;
        let i = if index < 0 { len + index } else { index };
        if i < 0 || i >= len {
            Err(pyo3::exceptions::PyIndexError::new_err(
                "index out of range",
            ))
        } else {
            Ok(self.ids[i as usize])
        }
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyIdArrayIter {
        PyIdArrayIter {
            ids: Arc::clone(&slf.ids),
            index: 0,
        }
    }

    fn __bool__(&self) -> bool {
        !self.ids.is_empty()
    }

    fn __contains__(&self, val: u64) -> bool {
        self.ids.contains(&val)
    }

    fn __eq__(&self, other: &Bound<'_, PyAny>) -> PyResult<bool> {
        if let Ok(other_arr) = other.extract::<PyRef<'_, IdArray>>() {
            return Ok(*self.ids == *other_arr.ids);
        }
        if let Ok(other_list) = other.extract::<Vec<u64>>() {
            return Ok(*self.ids == other_list);
        }
        Ok(false)
    }

    fn __repr__(&self) -> String {
        format!("IdArray(len={})", self.ids.len())
    }

    /// Materialize as a plain Python list.
    fn to_list(&self) -> Vec<u64> {
        (*self.ids).clone()
    }
}

#[pyclass]
pub struct PyIdArrayIter {
    ids: Arc<Vec<u64>>,
    index: usize,
}

#[pymethods]
impl PyIdArrayIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self) -> Option<u64> {
        if self.index < self.ids.len() {
            let val = self.ids[self.index];
            self.index += 1;
            Some(val)
        } else {
            None
        }
    }
}

// ============================================================
// Page result types
// ============================================================

#[pyclass]
#[derive(Clone)]
pub struct PyIdPageResult {
    #[pyo3(get)]
    pub items: IdArray,
    #[pyo3(get)]
    pub next_cursor: Option<u64>,
}

impl From<eg::PageResult<u64>> for PyIdPageResult {
    fn from(r: eg::PageResult<u64>) -> Self {
        PyIdPageResult {
            items: IdArray {
                ids: Arc::new(r.items),
            },
            next_cursor: r.next_cursor,
        }
    }
}

#[pymethods]
impl PyIdPageResult {
    fn __repr__(&self) -> String {
        format!(
            "IdPageResult(count={}, has_next={})",
            self.items.ids.len(),
            self.next_cursor.is_some()
        )
    }
    fn __len__(&self) -> usize {
        self.items.ids.len()
    }
    fn __bool__(&self) -> bool {
        !self.items.ids.is_empty()
    }
}

#[pyclass]
#[derive(Clone)]
pub struct PyNodePageResult {
    items: Vec<PyNodeRecord>,
    #[pyo3(get)]
    pub next_cursor: Option<u64>,
}

#[pymethods]
impl PyNodePageResult {
    #[getter]
    fn items(&self) -> Vec<PyNodeRecord> {
        self.items.clone()
    }
    fn __repr__(&self) -> String {
        format!(
            "NodePageResult(count={}, has_next={})",
            self.items.len(),
            self.next_cursor.is_some()
        )
    }
    fn __len__(&self) -> usize {
        self.items.len()
    }
    fn __bool__(&self) -> bool {
        !self.items.is_empty()
    }
}

#[pyclass]
#[derive(Clone)]
pub struct PyEdgePageResult {
    items: Vec<PyEdgeRecord>,
    #[pyo3(get)]
    pub next_cursor: Option<u64>,
}

#[pymethods]
impl PyEdgePageResult {
    #[getter]
    fn items(&self) -> Vec<PyEdgeRecord> {
        self.items.clone()
    }
    fn __repr__(&self) -> String {
        format!(
            "EdgePageResult(count={}, has_next={})",
            self.items.len(),
            self.next_cursor.is_some()
        )
    }
    fn __len__(&self) -> usize {
        self.items.len()
    }
    fn __bool__(&self) -> bool {
        !self.items.is_empty()
    }
}

#[pyclass]
#[derive(Clone)]
pub struct PyNeighborPageResult {
    items: Vec<PyNeighborEntry>,
    #[pyo3(get)]
    pub next_cursor: Option<u64>,
}

#[pymethods]
impl PyNeighborPageResult {
    #[getter]
    fn items(&self) -> Vec<PyNeighborEntry> {
        self.items.clone()
    }
    fn __repr__(&self) -> String {
        format!(
            "NeighborPageResult(count={}, has_next={})",
            self.items.len(),
            self.next_cursor.is_some()
        )
    }
    fn __len__(&self) -> usize {
        self.items.len()
    }
    fn __bool__(&self) -> bool {
        !self.items.is_empty()
    }
}

#[pyclass]
#[derive(Clone)]
pub struct PyPropertyRangePageResult {
    #[pyo3(get)]
    pub items: IdArray,
    next_cursor: Option<PyPropertyRangeCursor>,
}

impl From<PropertyRangePageResult<u64>> for PyPropertyRangePageResult {
    fn from(result: PropertyRangePageResult<u64>) -> Self {
        PyPropertyRangePageResult {
            items: IdArray {
                ids: Arc::new(result.items),
            },
            next_cursor: result.next_cursor.map(PyPropertyRangeCursor::from),
        }
    }
}

#[pymethods]
impl PyPropertyRangePageResult {
    #[getter]
    fn next_cursor(&self) -> Option<PyPropertyRangeCursor> {
        self.next_cursor.clone()
    }

    fn __repr__(&self) -> String {
        format!(
            "PropertyRangePageResult(count={}, has_next={})",
            self.items.ids.len(),
            self.next_cursor.is_some()
        )
    }

    fn __len__(&self) -> usize {
        self.items.ids.len()
    }

    fn __bool__(&self) -> bool {
        !self.items.ids.is_empty()
    }
}

#[pyclass]
#[derive(Clone)]
pub struct PyTraversalPageResult {
    items: Vec<PyTraversalHit>,
    next_cursor: Option<PyTraversalCursor>,
}

impl From<TraversalPageResult> for PyTraversalPageResult {
    fn from(result: TraversalPageResult) -> Self {
        PyTraversalPageResult {
            items: result.items.into_iter().map(PyTraversalHit::from).collect(),
            next_cursor: result.next_cursor.map(PyTraversalCursor::from),
        }
    }
}

#[pymethods]
impl PyTraversalPageResult {
    #[getter]
    fn items(&self) -> Vec<PyTraversalHit> {
        self.items.clone()
    }

    #[getter]
    fn next_cursor(&self) -> Option<PyTraversalCursor> {
        self.next_cursor.clone()
    }

    fn __repr__(&self) -> String {
        format!(
            "TraversalPageResult(count={}, has_next={})",
            self.items.len(),
            self.next_cursor.is_some()
        )
    }

    fn __len__(&self) -> usize {
        self.items.len()
    }

    fn __bool__(&self) -> bool {
        !self.items.is_empty()
    }
}

// ============================================================
// Analytics types
// ============================================================

#[pyclass]
#[derive(Clone)]
pub struct PyPprResult {
    #[pyo3(get)]
    pub node_ids: Vec<u64>,
    #[pyo3(get)]
    pub scores: Vec<f64>,
    #[pyo3(get)]
    pub iterations: u32,
    #[pyo3(get)]
    pub converged: bool,
    #[pyo3(get)]
    pub algorithm: String,
    #[pyo3(get)]
    pub approx: Option<PyPprApproxMeta>,
}

#[pyclass]
#[derive(Clone)]
pub struct PyPprApproxMeta {
    #[pyo3(get)]
    pub residual_tolerance: f64,
    #[pyo3(get)]
    pub pushes: u64,
    #[pyo3(get)]
    pub max_remaining_residual: f64,
}

#[pymethods]
impl PyPprApproxMeta {
    fn __repr__(&self) -> String {
        format!(
            "PprApproxMeta(residual_tolerance={}, pushes={}, max_remaining_residual={})",
            self.residual_tolerance, self.pushes, self.max_remaining_residual
        )
    }
}

impl From<PprResult> for PyPprResult {
    fn from(r: PprResult) -> Self {
        let (node_ids, scores): (Vec<u64>, Vec<f64>) = r.scores.into_iter().unzip();
        PyPprResult {
            node_ids,
            scores,
            iterations: r.iterations,
            converged: r.converged,
            algorithm: ppr_algorithm_to_py(r.algorithm).to_string(),
            approx: r.approx.map(|a| PyPprApproxMeta {
                residual_tolerance: a.residual_tolerance,
                pushes: a.pushes,
                max_remaining_residual: a.max_remaining_residual,
            }),
        }
    }
}

#[pymethods]
impl PyPprResult {
    fn __repr__(&self) -> String {
        format!(
            "PprResult(nodes={}, iterations={}, converged={}, algorithm='{}')",
            self.node_ids.len(),
            self.iterations,
            self.converged,
            self.algorithm
        )
    }
}

#[pyclass]
#[derive(Clone)]
pub struct PyExportEdge {
    #[pyo3(get)]
    pub from_id: u64,
    #[pyo3(get)]
    pub to_id: u64,
    #[pyo3(get)]
    pub type_id: u32,
    #[pyo3(get)]
    pub weight: f64,
}

#[pymethods]
impl PyExportEdge {
    fn __repr__(&self) -> String {
        format!(
            "ExportEdge(from={}, to={}, type={}, weight={})",
            self.from_id, self.to_id, self.type_id, self.weight
        )
    }
}

#[pyclass]
#[derive(Clone)]
pub struct PyAdjacencyExport {
    #[pyo3(get)]
    pub node_ids: Vec<u64>,
    edges: Vec<PyExportEdge>,
}

impl From<AdjacencyExport> for PyAdjacencyExport {
    fn from(a: AdjacencyExport) -> Self {
        PyAdjacencyExport {
            node_ids: a.node_ids,
            edges: a
                .edges
                .into_iter()
                .map(|(from_id, to_id, type_id, weight)| PyExportEdge {
                    from_id,
                    to_id,
                    type_id,
                    weight: weight as f64,
                })
                .collect(),
        }
    }
}

#[pymethods]
impl PyAdjacencyExport {
    #[getter]
    fn edges(&self) -> Vec<PyExportEdge> {
        self.edges.clone()
    }
    fn __repr__(&self) -> String {
        format!(
            "AdjacencyExport(nodes={}, edges={})",
            self.node_ids.len(),
            self.edges.len()
        )
    }
}

// ============================================================
// Property conversion: Python <-> Rust PropValue
// ============================================================

#[allow(clippy::only_used_in_recursion)]
fn py_to_prop_value(py: Python<'_>, obj: &Bound<'_, pyo3::PyAny>) -> PyResult<PropValue> {
    if obj.is_none() {
        Ok(PropValue::Null)
    } else if let Ok(b) = obj.extract::<bool>() {
        Ok(PropValue::Bool(b))
    } else if let Ok(i) = obj.extract::<i64>() {
        Ok(PropValue::Int(i))
    } else if let Ok(f) = obj.extract::<f64>() {
        Ok(PropValue::Float(f))
    } else if let Ok(s) = obj.extract::<String>() {
        Ok(PropValue::String(s))
    } else if let Ok(b) = obj.downcast::<PyBytes>() {
        Ok(PropValue::Bytes(b.as_bytes().to_vec()))
    } else if let Ok(list) = obj.downcast::<PyList>() {
        let items: PyResult<Vec<PropValue>> = list
            .iter()
            .map(|item| py_to_prop_value(py, &item))
            .collect();
        Ok(PropValue::Array(items?))
    } else if let Ok(dict) = obj.downcast::<PyDict>() {
        let mut map = BTreeMap::new();
        for (k, v) in dict.iter() {
            let key: String = k.extract()?;
            map.insert(key, py_to_prop_value(py, &v)?);
        }
        Ok(PropValue::Map(map))
    } else {
        Err(PyTypeError::new_err(format!(
            "Unsupported property value type: {}",
            obj.get_type().name()?
        )))
    }
}

fn prop_value_to_py_obj(py: Python<'_>, v: &PropValue) -> PyResult<PyObject> {
    match v {
        PropValue::Null => Ok(py.None()),
        PropValue::Bool(b) => Ok(b.into_pyobject(py)?.to_owned().into_any().unbind()),
        PropValue::Int(i) => Ok(i.into_pyobject(py)?.into_any().unbind()),
        PropValue::UInt(u) => Ok(u.into_pyobject(py)?.into_any().unbind()),
        PropValue::Float(f) => Ok(f.into_pyobject(py)?.into_any().unbind()),
        PropValue::String(s) => Ok(s.into_pyobject(py)?.into_any().unbind()),
        PropValue::Bytes(b) => Ok(PyBytes::new(py, b).into_any().unbind()),
        PropValue::Array(arr) => {
            let items: PyResult<Vec<PyObject>> = arr
                .iter()
                .map(|item| prop_value_to_py_obj(py, item))
                .collect();
            Ok(PyList::new(py, items?)?.into_any().unbind())
        }
        PropValue::Map(map) => {
            let dict = PyDict::new(py);
            for (k, v) in map {
                dict.set_item(k, prop_value_to_py_obj(py, v)?)?;
            }
            Ok(dict.into_any().unbind())
        }
    }
}

fn props_to_py(py: Python<'_>, props: &BTreeMap<String, PropValue>) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    for (k, v) in props {
        dict.set_item(k, prop_value_to_py_obj(py, v)?)?;
    }
    Ok(dict.into_any().unbind())
}

fn prop_value_debug_repr(py: Python<'_>, value: &PropValue) -> PyResult<String> {
    prop_value_to_py_obj(py, value)?.bind(py).repr()?.extract()
}

fn parse_secondary_index_range_domain(domain: &str) -> PyResult<SecondaryIndexRangeDomain> {
    match domain {
        "int" => Ok(SecondaryIndexRangeDomain::Int),
        "uint" => Ok(SecondaryIndexRangeDomain::UInt),
        "float" => Ok(SecondaryIndexRangeDomain::Float),
        other => Err(PyValueError::new_err(format!(
            "Invalid range domain '{}'. Must be 'int', 'uint', or 'float'.",
            other
        ))),
    }
}

fn range_domain_to_py(domain: SecondaryIndexRangeDomain) -> &'static str {
    match domain {
        SecondaryIndexRangeDomain::Int => "int",
        SecondaryIndexRangeDomain::UInt => "uint",
        SecondaryIndexRangeDomain::Float => "float",
    }
}

fn secondary_index_state_to_py(state: SecondaryIndexState) -> &'static str {
    match state {
        SecondaryIndexState::Building => "building",
        SecondaryIndexState::Ready => "ready",
        SecondaryIndexState::Failed => "failed",
    }
}

fn secondary_index_kind_to_py(kind: &SecondaryIndexKind) -> (&'static str, Option<&'static str>) {
    match kind {
        SecondaryIndexKind::Equality => ("equality", None),
        SecondaryIndexKind::Range { domain } => ("range", Some(range_domain_to_py(*domain))),
    }
}

fn parse_secondary_index_kind(kind: &str, domain: Option<&str>) -> PyResult<SecondaryIndexKind> {
    match kind {
        "equality" => {
            if domain.is_some() {
                return Err(PyValueError::new_err(
                    "equality indexes do not accept a range domain",
                ));
            }
            Ok(SecondaryIndexKind::Equality)
        }
        "range" => Ok(SecondaryIndexKind::Range {
            domain: parse_secondary_index_range_domain(domain.ok_or_else(|| {
                PyValueError::new_err("range indexes require domain='int', 'uint', or 'float'")
            })?)?,
        }),
        other => Err(PyValueError::new_err(format!(
            "Invalid index kind '{}'. Must be 'equality' or 'range'.",
            other
        ))),
    }
}

fn range_domain_from_prop_value(
    value: &PropValue,
    context: &str,
) -> PyResult<SecondaryIndexRangeDomain> {
    match value {
        PropValue::Int(_) => Ok(SecondaryIndexRangeDomain::Int),
        PropValue::UInt(_) => Ok(SecondaryIndexRangeDomain::UInt),
        PropValue::Float(value) if value.is_finite() => Ok(SecondaryIndexRangeDomain::Float),
        _ => Err(PyValueError::new_err(format!(
            "{} must use Int, UInt, or finite Float values",
            context
        ))),
    }
}

fn py_numeric_to_prop_value(
    _py: Python<'_>,
    obj: &Bound<'_, pyo3::PyAny>,
    domain: SecondaryIndexRangeDomain,
) -> PyResult<PropValue> {
    if obj.is_instance_of::<PyBool>() {
        return Err(PyTypeError::new_err(
            "property range values must be numeric, not bool",
        ));
    }

    match domain {
        SecondaryIndexRangeDomain::Int => Ok(PropValue::Int(obj.extract::<i64>()?)),
        SecondaryIndexRangeDomain::UInt => Ok(PropValue::UInt(obj.extract::<u64>()?)),
        SecondaryIndexRangeDomain::Float => {
            let value = obj.extract::<f64>()?;
            if !value.is_finite() {
                return Err(PyValueError::new_err(
                    "property range float values must be finite",
                ));
            }
            Ok(PropValue::Float(value))
        }
    }
}

fn convert_py_props(
    py: Python<'_>,
    props: Option<&Bound<'_, PyDict>>,
) -> PyResult<BTreeMap<String, PropValue>> {
    match props {
        None => Ok(BTreeMap::new()),
        Some(dict) => {
            let mut map = BTreeMap::new();
            for (k, v) in dict.iter() {
                let key: String = k.extract()?;
                map.insert(key, py_to_prop_value(py, &v)?);
            }
            Ok(map)
        }
    }
}

fn with_py_txn<F, T>(inner: &Arc<Mutex<Option<WriteTxn>>>, f: F) -> PyResult<T>
where
    F: FnOnce(&mut WriteTxn) -> PyResult<T>,
{
    let mut guard = inner.lock().map_err(lock_err)?;
    let txn = guard
        .as_mut()
        .ok_or_else(|| OverGraphError::new_err(EngineError::TxnClosed.to_string()))?;
    f(txn)
}

fn with_py_txn_ref<F, T>(inner: &Arc<Mutex<Option<WriteTxn>>>, f: F) -> PyResult<T>
where
    F: FnOnce(&WriteTxn) -> PyResult<T>,
{
    let guard = inner.lock().map_err(lock_err)?;
    let txn = guard
        .as_ref()
        .ok_or_else(|| OverGraphError::new_err(EngineError::TxnClosed.to_string()))?;
    f(txn)
}

fn with_py_txn_take<F, T>(inner: &Arc<Mutex<Option<WriteTxn>>>, f: F) -> PyResult<T>
where
    F: FnOnce(&mut WriteTxn) -> PyResult<T>,
{
    let mut txn = {
        let mut guard = inner.lock().map_err(lock_err)?;
        guard
            .take()
            .ok_or_else(|| OverGraphError::new_err(EngineError::TxnClosed.to_string()))?
    };
    f(&mut txn)
}

fn parse_txn_node_ref(d: &Bound<'_, PyDict>) -> PyResult<TxnNodeRef> {
    let id = d.get_item("id")?;
    let type_id = d.get_item("type_id")?;
    let key = d.get_item("key")?;
    let local = d.get_item("local")?;
    let has_id = id.is_some();
    let has_key = type_id.is_some() || key.is_some();
    let has_local = local.is_some();
    match (has_id, has_key, has_local) {
        (true, false, false) => Ok(TxnNodeRef::Id(id.unwrap().extract()?)),
        (false, true, false) => Ok(TxnNodeRef::Key {
            type_id: type_id
                .ok_or_else(|| PyValueError::new_err("node key ref requires type_id"))?
                .extract()?,
            key: key
                .ok_or_else(|| PyValueError::new_err("node key ref requires key"))?
                .extract()?,
        }),
        (false, false, true) => Ok(TxnNodeRef::Local(TxnLocalRef::Alias(
            local.unwrap().extract()?,
        ))),
        _ => Err(PyValueError::new_err(
            "node ref must be exactly one of {'id'}, {'type_id', 'key'}, or {'local'}",
        )),
    }
}

fn parse_txn_edge_ref(d: &Bound<'_, PyDict>) -> PyResult<TxnEdgeRef> {
    let id = d.get_item("id")?;
    let from = d.get_item("from")?;
    let to = d.get_item("to")?;
    let type_id = d.get_item("type_id")?;
    let local = d.get_item("local")?;
    let has_id = id.is_some();
    let has_triple = from.is_some() || to.is_some() || type_id.is_some();
    let has_local = local.is_some();
    match (has_id, has_triple, has_local) {
        (true, false, false) => Ok(TxnEdgeRef::Id(id.unwrap().extract()?)),
        (false, true, false) => {
            let from = from.ok_or_else(|| PyValueError::new_err("edge ref requires from"))?;
            let to = to.ok_or_else(|| PyValueError::new_err("edge ref requires to"))?;
            Ok(TxnEdgeRef::Triple {
                from: parse_txn_node_ref(from.downcast::<PyDict>()?)?,
                to: parse_txn_node_ref(to.downcast::<PyDict>()?)?,
                type_id: type_id
                    .ok_or_else(|| PyValueError::new_err("edge ref requires type_id"))?
                    .extract()?,
            })
        }
        (false, false, true) => Ok(TxnEdgeRef::Local(TxnLocalRef::Alias(
            local.unwrap().extract()?,
        ))),
        _ => Err(PyValueError::new_err(
            "edge ref must be exactly one of {'id'}, {'from', 'to', 'type_id'}, or {'local'}",
        )),
    }
}

fn txn_node_ref_to_py(py: Python<'_>, value: TxnNodeRef) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    match value {
        TxnNodeRef::Id(id) => dict.set_item("id", id)?,
        TxnNodeRef::Key { type_id, key } => {
            dict.set_item("type_id", type_id)?;
            dict.set_item("key", key)?;
        }
        TxnNodeRef::Local(local) => {
            dict.set_item("local", txn_local_ref_to_py(local))?;
        }
    }
    Ok(dict.into())
}

fn txn_edge_ref_to_py(py: Python<'_>, value: TxnEdgeRef) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    match value {
        TxnEdgeRef::Id(id) => dict.set_item("id", id)?,
        TxnEdgeRef::Triple { from, to, type_id } => {
            dict.set_item("from", txn_node_ref_to_py(py, from)?)?;
            dict.set_item("to", txn_node_ref_to_py(py, to)?)?;
            dict.set_item("type_id", type_id)?;
        }
        TxnEdgeRef::Local(local) => {
            dict.set_item("local", txn_local_ref_to_py(local))?;
        }
    }
    Ok(dict.into())
}

fn txn_local_ref_to_py(local: TxnLocalRef) -> Option<String> {
    match local {
        TxnLocalRef::Alias(alias) => Some(alias),
        TxnLocalRef::Slot(_) => None,
    }
}

fn txn_node_view_to_py(py: Python<'_>, view: TxnNodeView) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    dict.set_item("id", view.id)?;
    dict.set_item("local", view.local.and_then(txn_local_ref_to_py))?;
    dict.set_item("type_id", view.type_id)?;
    dict.set_item("key", view.key)?;
    dict.set_item("props", props_to_py(py, &view.props)?)?;
    dict.set_item("created_at", view.created_at)?;
    dict.set_item("updated_at", view.updated_at)?;
    dict.set_item("weight", view.weight as f64)?;
    dict.set_item(
        "dense_vector",
        view.dense_vector
            .map(|v| v.into_iter().map(|x| x as f64).collect::<Vec<_>>()),
    )?;
    dict.set_item("sparse_vector", view.sparse_vector)?;
    Ok(dict.into())
}

fn txn_edge_view_to_py(py: Python<'_>, view: TxnEdgeView) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    dict.set_item("id", view.id)?;
    dict.set_item("local", view.local.and_then(txn_local_ref_to_py))?;
    dict.set_item("from", txn_node_ref_to_py(py, view.from)?)?;
    dict.set_item("to", txn_node_ref_to_py(py, view.to)?)?;
    dict.set_item("type_id", view.type_id)?;
    dict.set_item("props", props_to_py(py, &view.props)?)?;
    dict.set_item("created_at", view.created_at)?;
    dict.set_item("updated_at", view.updated_at)?;
    dict.set_item("weight", view.weight as f64)?;
    dict.set_item("valid_from", view.valid_from)?;
    dict.set_item("valid_to", view.valid_to)?;
    Ok(dict.into())
}

fn parse_txn_operations(py: Python<'_>, list: &Bound<'_, PyList>) -> PyResult<Vec<TxnIntent>> {
    let mut intents = Vec::with_capacity(list.len());
    for item in list.iter() {
        let op = item.downcast::<PyDict>()?;
        let op_name: String = op
            .get_item("op")?
            .ok_or_else(|| PyValueError::new_err("transaction operation missing 'op'"))?
            .extract()?;
        let intent = match op_name.as_str() {
            "upsert_node" => TxnIntent::UpsertNode {
                alias: op.get_item("alias")?.map(|v| v.extract()).transpose()?,
                type_id: op
                    .get_item("type_id")?
                    .ok_or_else(|| PyValueError::new_err("upsert_node requires type_id"))?
                    .extract()?,
                key: op
                    .get_item("key")?
                    .ok_or_else(|| PyValueError::new_err("upsert_node requires key"))?
                    .extract()?,
                options: UpsertNodeOptions {
                    props: match op.get_item("props")? {
                        Some(v) if !v.is_none() => {
                            convert_py_props(py, Some(v.downcast::<PyDict>()?))?
                        }
                        _ => BTreeMap::new(),
                    },
                    weight: op
                        .get_item("weight")?
                        .map(|v| v.extract::<f64>())
                        .transpose()?
                        .unwrap_or(1.0) as f32,
                    dense_vector: op
                        .get_item("dense_vector")?
                        .map(|v| v.extract())
                        .transpose()?,
                    sparse_vector: op
                        .get_item("sparse_vector")?
                        .map(|v| v.extract())
                        .transpose()?,
                },
            },
            "upsert_edge" => TxnIntent::UpsertEdge {
                alias: op.get_item("alias")?.map(|v| v.extract()).transpose()?,
                from: parse_txn_node_ref(
                    op.get_item("from")?
                        .ok_or_else(|| PyValueError::new_err("upsert_edge requires from"))?
                        .downcast::<PyDict>()?,
                )?,
                to: parse_txn_node_ref(
                    op.get_item("to")?
                        .ok_or_else(|| PyValueError::new_err("upsert_edge requires to"))?
                        .downcast::<PyDict>()?,
                )?,
                type_id: op
                    .get_item("type_id")?
                    .ok_or_else(|| PyValueError::new_err("upsert_edge requires type_id"))?
                    .extract()?,
                options: UpsertEdgeOptions {
                    props: match op.get_item("props")? {
                        Some(v) if !v.is_none() => {
                            convert_py_props(py, Some(v.downcast::<PyDict>()?))?
                        }
                        _ => BTreeMap::new(),
                    },
                    weight: op
                        .get_item("weight")?
                        .map(|v| v.extract::<f64>())
                        .transpose()?
                        .unwrap_or(1.0) as f32,
                    valid_from: op
                        .get_item("valid_from")?
                        .and_then(|v| if v.is_none() { None } else { Some(v) })
                        .map(|v| v.extract())
                        .transpose()?,
                    valid_to: op
                        .get_item("valid_to")?
                        .and_then(|v| if v.is_none() { None } else { Some(v) })
                        .map(|v| v.extract())
                        .transpose()?,
                },
            },
            "delete_node" => TxnIntent::DeleteNode {
                target: parse_txn_node_ref(
                    op.get_item("target")?
                        .ok_or_else(|| PyValueError::new_err("delete_node requires target"))?
                        .downcast::<PyDict>()?,
                )?,
            },
            "delete_edge" => TxnIntent::DeleteEdge {
                target: parse_txn_edge_ref(
                    op.get_item("target")?
                        .ok_or_else(|| PyValueError::new_err("delete_edge requires target"))?
                        .downcast::<PyDict>()?,
                )?,
            },
            "invalidate_edge" => TxnIntent::InvalidateEdge {
                target: parse_txn_edge_ref(
                    op.get_item("target")?
                        .ok_or_else(|| PyValueError::new_err("invalidate_edge requires target"))?
                        .downcast::<PyDict>()?,
                )?,
                valid_to: op
                    .get_item("valid_to")?
                    .ok_or_else(|| PyValueError::new_err("invalidate_edge requires valid_to"))?
                    .extract()?,
            },
            other => {
                return Err(PyValueError::new_err(format!(
                    "invalid transaction op '{}'",
                    other
                )));
            }
        };
        intents.push(intent);
    }
    Ok(intents)
}

// ============================================================
// Input parsing helpers
// ============================================================

const KNOWN_OPTIONS: &[&str] = &[
    "create_if_missing",
    "edge_uniqueness",
    "memtable_flush_threshold",
    "compact_after_n_flushes",
    "wal_sync_mode",
    "group_commit_interval_ms",
    "memtable_hard_cap_bytes",
    "max_immutable_memtables",
    "dense_vector_dimension",
    "dense_vector_metric",
];

fn parse_db_options(d: &Bound<'_, PyDict>) -> PyResult<DbOptions> {
    for key in d.keys() {
        let k: String = key.extract()?;
        if !KNOWN_OPTIONS.contains(&k.as_str()) {
            return Err(PyValueError::new_err(format!(
                "Unknown option '{}'. Valid options: {}",
                k,
                KNOWN_OPTIONS.join(", ")
            )));
        }
    }
    let defaults = DbOptions::default();
    let wal_sync_mode = match d.get_item("wal_sync_mode")? {
        Some(v) => {
            let mode: String = v.extract()?;
            if mode == "immediate" {
                WalSyncMode::Immediate
            } else if mode == "group_commit" {
                let interval_ms: u64 = d
                    .get_item("group_commit_interval_ms")?
                    .map(|v| v.extract())
                    .transpose()?
                    .unwrap_or(50);
                WalSyncMode::GroupCommit {
                    interval_ms,
                    soft_trigger_bytes: 2 * 1024 * 1024,
                    hard_cap_bytes: 16 * 1024 * 1024,
                }
            } else {
                return Err(PyValueError::new_err(format!(
                    "Invalid wal_sync_mode '{}': expected 'immediate' or 'group_commit'",
                    mode
                )));
            }
        }
        None => defaults.wal_sync_mode,
    };

    let dense_vector = match d.get_item("dense_vector_dimension")? {
        Some(v) => {
            let dimension: u32 = v.extract()?;
            let metric = match d.get_item("dense_vector_metric")? {
                Some(m) => {
                    let s: String = m.extract()?;
                    match s.as_str() {
                        "euclidean" => DenseMetric::Euclidean,
                        "dot_product" => DenseMetric::DotProduct,
                        _ => DenseMetric::Cosine,
                    }
                }
                None => DenseMetric::Cosine,
            };
            Some(DenseVectorConfig {
                dimension,
                metric,
                hnsw: HnswConfig::default(),
            })
        }
        None => None,
    };

    Ok(DbOptions {
        create_if_missing: d
            .get_item("create_if_missing")?
            .map(|v| v.extract())
            .transpose()?
            .unwrap_or(defaults.create_if_missing),
        edge_uniqueness: d
            .get_item("edge_uniqueness")?
            .map(|v| v.extract())
            .transpose()?
            .unwrap_or(defaults.edge_uniqueness),
        memtable_flush_threshold: d
            .get_item("memtable_flush_threshold")?
            .map(|v| v.extract::<usize>())
            .transpose()?
            .unwrap_or(defaults.memtable_flush_threshold),
        compact_after_n_flushes: d
            .get_item("compact_after_n_flushes")?
            .map(|v| v.extract())
            .transpose()?
            .unwrap_or(defaults.compact_after_n_flushes),
        dense_vector,
        wal_sync_mode,
        memtable_hard_cap_bytes: d
            .get_item("memtable_hard_cap_bytes")?
            .map(|v| v.extract::<usize>())
            .transpose()?
            .unwrap_or(defaults.memtable_hard_cap_bytes),
        max_immutable_memtables: d
            .get_item("max_immutable_memtables")?
            .map(|v| v.extract::<usize>())
            .transpose()?
            .unwrap_or(defaults.max_immutable_memtables),
    })
}

fn parse_node_inputs(py: Python<'_>, list: &Bound<'_, PyList>) -> PyResult<Vec<NodeInput>> {
    let mut inputs = Vec::with_capacity(list.len());
    for item in list.iter() {
        let d = item.downcast::<PyDict>()?;
        let type_id: u32 = d
            .get_item("type_id")?
            .ok_or_else(|| PyValueError::new_err("Node input missing 'type_id'"))?
            .extract()?;
        let key: String = d
            .get_item("key")?
            .ok_or_else(|| PyValueError::new_err("Node input missing 'key'"))?
            .extract()?;
        let props = match d.get_item("props")? {
            Some(v) if !v.is_none() => {
                let dict = v.downcast::<PyDict>()?;
                convert_py_props(py, Some(dict))?
            }
            _ => BTreeMap::new(),
        };
        let weight: f32 = d
            .get_item("weight")?
            .map(|v| v.extract::<f64>())
            .transpose()?
            .unwrap_or(1.0) as f32;
        let dense_vector: Option<Vec<f32>> = d
            .get_item("dense_vector")?
            .map(|v| v.extract())
            .transpose()?;
        let sparse_vector: Option<Vec<(u32, f32)>> = d
            .get_item("sparse_vector")?
            .map(|v| v.extract())
            .transpose()?;
        inputs.push(NodeInput {
            type_id,
            key,
            props,
            weight,
            dense_vector,
            sparse_vector,
        });
    }
    Ok(inputs)
}

fn parse_edge_inputs(py: Python<'_>, list: &Bound<'_, PyList>) -> PyResult<Vec<EdgeInput>> {
    let mut inputs = Vec::with_capacity(list.len());
    for item in list.iter() {
        let d = item.downcast::<PyDict>()?;
        let from: u64 = d
            .get_item("from_id")?
            .ok_or_else(|| PyValueError::new_err("Edge input missing 'from_id'"))?
            .extract()?;
        let to: u64 = d
            .get_item("to_id")?
            .ok_or_else(|| PyValueError::new_err("Edge input missing 'to_id'"))?
            .extract()?;
        let type_id: u32 = d
            .get_item("type_id")?
            .ok_or_else(|| PyValueError::new_err("Edge input missing 'type_id'"))?
            .extract()?;
        let props = match d.get_item("props")? {
            Some(v) if !v.is_none() => {
                let dict = v.downcast::<PyDict>()?;
                convert_py_props(py, Some(dict))?
            }
            _ => BTreeMap::new(),
        };
        let weight: f32 = d
            .get_item("weight")?
            .map(|v| v.extract::<f64>())
            .transpose()?
            .unwrap_or(1.0) as f32;
        let valid_from: Option<i64> = d
            .get_item("valid_from")?
            .and_then(|v| if v.is_none() { None } else { Some(v) })
            .map(|v| v.extract())
            .transpose()?;
        let valid_to: Option<i64> = d
            .get_item("valid_to")?
            .and_then(|v| if v.is_none() { None } else { Some(v) })
            .map(|v| v.extract())
            .transpose()?;
        inputs.push(EdgeInput {
            from,
            to,
            type_id,
            props,
            weight,
            valid_from,
            valid_to,
        });
    }
    Ok(inputs)
}

fn parse_graph_patch(py: Python<'_>, d: &Bound<'_, PyDict>) -> PyResult<GraphPatch> {
    let upsert_nodes = match d.get_item("upsert_nodes")? {
        Some(v) if !v.is_none() => {
            let list = v.downcast::<PyList>()?;
            parse_node_inputs(py, list)?
        }
        _ => Vec::new(),
    };

    let upsert_edges = match d.get_item("upsert_edges")? {
        Some(v) if !v.is_none() => {
            let list = v.downcast::<PyList>()?;
            parse_edge_inputs(py, list)?
        }
        _ => Vec::new(),
    };

    let invalidate_edges = match d.get_item("invalidate_edges")? {
        Some(v) if !v.is_none() => {
            let list = v.downcast::<PyList>()?;
            let mut inv = Vec::with_capacity(list.len());
            for item in list.iter() {
                let d = item.downcast::<PyDict>()?;
                let edge_id: u64 = d
                    .get_item("edge_id")?
                    .ok_or_else(|| PyValueError::new_err("Missing 'edge_id'"))?
                    .extract()?;
                let valid_to: i64 = d
                    .get_item("valid_to")?
                    .ok_or_else(|| PyValueError::new_err("Missing 'valid_to'"))?
                    .extract()?;
                inv.push((edge_id, valid_to));
            }
            inv
        }
        _ => Vec::new(),
    };

    let delete_node_ids: Vec<u64> = match d.get_item("delete_node_ids")? {
        Some(v) if !v.is_none() => v.extract()?,
        _ => Vec::new(),
    };

    let delete_edge_ids: Vec<u64> = match d.get_item("delete_edge_ids")? {
        Some(v) if !v.is_none() => v.extract()?,
        _ => Vec::new(),
    };

    Ok(GraphPatch {
        upsert_nodes,
        upsert_edges,
        invalidate_edges,
        delete_node_ids,
        delete_edge_ids,
    })
}

// ============================================================
// Direction / scoring helpers
// ============================================================

fn parse_vector_search_mode(s: &str) -> PyResult<VectorSearchMode> {
    match s {
        "dense" => Ok(VectorSearchMode::Dense),
        "sparse" => Ok(VectorSearchMode::Sparse),
        "hybrid" => Ok(VectorSearchMode::Hybrid),
        other => Err(PyValueError::new_err(format!(
            "Invalid mode '{}'. Must be 'dense', 'sparse', or 'hybrid'.",
            other
        ))),
    }
}

fn parse_fusion_mode(s: Option<&str>) -> PyResult<Option<FusionMode>> {
    match s {
        None => Ok(None),
        Some("weighted_rank") => Ok(Some(FusionMode::WeightedRankFusion)),
        Some("reciprocal_rank") => Ok(Some(FusionMode::ReciprocalRankFusion)),
        Some("weighted_score") => Ok(Some(FusionMode::WeightedScoreFusion)),
        Some(other) => Err(PyValueError::new_err(format!(
            "Invalid fusion_mode '{}'. Must be 'weighted_rank', 'reciprocal_rank', or 'weighted_score'.",
            other
        ))),
    }
}

fn parse_ppr_algorithm(s: Option<&str>) -> PyResult<PprAlgorithm> {
    match s {
        None => Ok(PprAlgorithm::ExactPowerIteration),
        Some("exact") | Some("exact_power_iteration") => Ok(PprAlgorithm::ExactPowerIteration),
        Some("approx") | Some("approx_forward_push") => Ok(PprAlgorithm::ApproxForwardPush),
        Some(other) => Err(PyValueError::new_err(format!(
            "Invalid algorithm '{}'. Must be 'exact' or 'approx'.",
            other
        ))),
    }
}

fn ppr_algorithm_to_py(algorithm: PprAlgorithm) -> &'static str {
    match algorithm {
        PprAlgorithm::ExactPowerIteration => "exact",
        PprAlgorithm::ApproxForwardPush => "approx",
    }
}

fn parse_direction(s: &str) -> PyResult<Direction> {
    match s {
        "outgoing" => Ok(Direction::Outgoing),
        "incoming" => Ok(Direction::Incoming),
        "both" => Ok(Direction::Both),
        other => Err(PyValueError::new_err(format!(
            "Invalid direction '{}'. Must be 'outgoing', 'incoming', or 'both'.",
            other
        ))),
    }
}

fn parse_scoring_mode(s: &str, decay_lambda: Option<f64>) -> PyResult<ScoringMode> {
    match s {
        "weight" => Ok(ScoringMode::Weight),
        "recency" => Ok(ScoringMode::Recency),
        "decay" => {
            let lambda = decay_lambda.ok_or_else(|| {
                PyValueError::new_err("scoring='decay' requires decay_lambda parameter")
            })? as f32;
            if lambda.is_nan() || lambda.is_infinite() || lambda < 0.0 {
                return Err(PyValueError::new_err(
                    "decay_lambda must be a finite non-negative number",
                ));
            }
            Ok(ScoringMode::DecayAdjusted { lambda })
        }
        other => Err(PyValueError::new_err(format!(
            "Invalid scoring mode '{}'. Must be 'weight', 'recency', or 'decay'.",
            other
        ))),
    }
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

    fn ensure(&self, n: usize) -> PyResult<()> {
        if self.pos + n > self.buf.len() {
            Err(PyValueError::new_err(format!(
                "Binary buffer truncated at offset {} (need {} bytes, have {})",
                self.pos,
                n,
                self.buf.len().saturating_sub(self.pos)
            )))
        } else {
            Ok(())
        }
    }

    fn read_u16_le(&mut self) -> PyResult<u16> {
        self.ensure(2)?;
        let v = u16::from_le_bytes([self.buf[self.pos], self.buf[self.pos + 1]]);
        self.pos += 2;
        Ok(v)
    }

    fn read_u32_le(&mut self) -> PyResult<u32> {
        self.ensure(4)?;
        let v = u32::from_le_bytes(self.buf[self.pos..self.pos + 4].try_into().unwrap());
        self.pos += 4;
        Ok(v)
    }

    fn read_f32_le(&mut self) -> PyResult<f32> {
        self.ensure(4)?;
        let v = f32::from_le_bytes(self.buf[self.pos..self.pos + 4].try_into().unwrap());
        self.pos += 4;
        Ok(v)
    }

    fn read_u64_le(&mut self) -> PyResult<u64> {
        self.ensure(8)?;
        let v = u64::from_le_bytes(self.buf[self.pos..self.pos + 8].try_into().unwrap());
        self.pos += 8;
        Ok(v)
    }

    fn read_i64_le(&mut self) -> PyResult<i64> {
        self.ensure(8)?;
        let v = i64::from_le_bytes(self.buf[self.pos..self.pos + 8].try_into().unwrap());
        self.pos += 8;
        Ok(v)
    }

    fn read_bytes(&mut self, n: usize) -> PyResult<&'a [u8]> {
        self.ensure(n)?;
        let slice = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(slice)
    }
}

fn json_to_prop_value(v: &serde_json::Value) -> eg::PropValue {
    match v {
        serde_json::Value::Null => eg::PropValue::Null,
        serde_json::Value::Bool(b) => eg::PropValue::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                eg::PropValue::Int(i)
            } else if let Some(f) = n.as_f64() {
                eg::PropValue::Float(f)
            } else {
                eg::PropValue::Null
            }
        }
        serde_json::Value::String(s) => eg::PropValue::String(s.clone()),
        serde_json::Value::Array(arr) => {
            eg::PropValue::Array(arr.iter().map(json_to_prop_value).collect())
        }
        serde_json::Value::Object(map) => eg::PropValue::Map(
            map.iter()
                .map(|(k, v)| (k.clone(), json_to_prop_value(v)))
                .collect(),
        ),
    }
}

fn decode_node_batch_py(buf: &[u8]) -> PyResult<Vec<NodeInput>> {
    let mut r = BinaryReader::new(buf);
    let count = r.read_u32_le()? as usize;
    // Cap allocation: minimum node record is 14 bytes (type_id + weight + key_len + props_len)
    let max_possible = buf.len().saturating_sub(4) / 14;
    let mut inputs = Vec::with_capacity(count.min(max_possible));
    for _ in 0..count {
        let type_id = r.read_u32_le()?;
        let weight = r.read_f32_le()?;
        let key_len = r.read_u16_le()? as usize;
        let key_bytes = r.read_bytes(key_len)?;
        let key = std::str::from_utf8(key_bytes)
            .map_err(|e| PyValueError::new_err(format!("Invalid UTF-8 in node key: {}", e)))?
            .to_string();
        let props_len = r.read_u32_le()? as usize;
        let props = if props_len == 0 {
            BTreeMap::new()
        } else {
            let props_bytes = r.read_bytes(props_len)?;
            let json: serde_json::Value = serde_json::from_slice(props_bytes)
                .map_err(|e| PyValueError::new_err(format!("Invalid JSON in node props: {}", e)))?;
            match json {
                serde_json::Value::Object(map) => map
                    .into_iter()
                    .map(|(k, v)| (k, json_to_prop_value(&v)))
                    .collect(),
                _ => return Err(PyValueError::new_err("Node props must be a JSON object")),
            }
        };
        inputs.push(NodeInput {
            type_id,
            key,
            props,
            weight,
            dense_vector: None,
            sparse_vector: None,
        });
    }
    if r.pos != buf.len() {
        return Err(PyValueError::new_err(format!(
            "Binary buffer has {} trailing bytes after decoding {} nodes",
            buf.len() - r.pos,
            count
        )));
    }
    Ok(inputs)
}

fn decode_edge_batch_py(buf: &[u8]) -> PyResult<Vec<EdgeInput>> {
    let mut r = BinaryReader::new(buf);
    let count = r.read_u32_le()? as usize;
    // Cap allocation: minimum edge record is 36 bytes (from + to + type_id + weight + valid_from + valid_to + props_len)
    let max_possible = buf.len().saturating_sub(4) / 36;
    let mut inputs = Vec::with_capacity(count.min(max_possible));
    for _ in 0..count {
        let from = r.read_u64_le()?;
        let to = r.read_u64_le()?;
        let type_id = r.read_u32_le()?;
        let weight = r.read_f32_le()?;
        let valid_from_raw = r.read_i64_le()?;
        let valid_to_raw = r.read_i64_le()?;
        let valid_from = if valid_from_raw == 0 {
            None
        } else {
            Some(valid_from_raw)
        };
        let valid_to = if valid_to_raw == 0 {
            None
        } else {
            Some(valid_to_raw)
        };
        let props_len = r.read_u32_le()? as usize;
        let props = if props_len == 0 {
            BTreeMap::new()
        } else {
            let props_bytes = r.read_bytes(props_len)?;
            let json: serde_json::Value = serde_json::from_slice(props_bytes)
                .map_err(|e| PyValueError::new_err(format!("Invalid JSON in edge props: {}", e)))?;
            match json {
                serde_json::Value::Object(map) => map
                    .into_iter()
                    .map(|(k, v)| (k, json_to_prop_value(&v)))
                    .collect(),
                _ => return Err(PyValueError::new_err("Edge props must be a JSON object")),
            }
        };
        inputs.push(EdgeInput {
            from,
            to,
            type_id,
            props,
            weight,
            valid_from,
            valid_to,
        });
    }
    if r.pos != buf.len() {
        return Err(PyValueError::new_err(format!(
            "Binary buffer has {} trailing bytes after decoding {} edges",
            buf.len() - r.pos,
            count
        )));
    }
    Ok(inputs)
}

// ============================================================
// Module registration
// ============================================================

#[pymodule]
fn overgraph(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<OverGraph>()?;
    m.add_class::<PyWriteTxn>()?;
    m.add_class::<PyDbStats>()?;
    m.add_class::<PyNodeRecord>()?;
    m.add_class::<PyEdgeRecord>()?;
    m.add_class::<PyPatchResult>()?;
    m.add_class::<PyTxnCommitResult>()?;
    m.add_class::<PyNeighborEntry>()?;
    m.add_class::<PyTraversalHit>()?;
    m.add_class::<PyVectorHit>()?;
    m.add_class::<PyNodePropertyIndexInfo>()?;
    m.add_class::<PyPropertyRangeBound>()?;
    m.add_class::<PyPropertyRangeCursor>()?;
    m.add_class::<PyTraversalCursor>()?;
    m.add_class::<PyShortestPath>()?;
    m.add_class::<PySubgraph>()?;
    m.add_class::<PyPruneResult>()?;
    m.add_class::<PyNamedPrunePolicy>()?;
    m.add_class::<PySegmentInfo>()?;
    m.add_class::<PyCompactionStats>()?;
    m.add_class::<PyCompactionProgress>()?;
    m.add_class::<PyIdPageResult>()?;
    m.add_class::<PyNodePageResult>()?;
    m.add_class::<PyEdgePageResult>()?;
    m.add_class::<PyNeighborPageResult>()?;
    m.add_class::<PyPropertyRangePageResult>()?;
    m.add_class::<PyTraversalPageResult>()?;
    m.add_class::<PyPprApproxMeta>()?;
    m.add_class::<PyPprResult>()?;
    m.add_class::<PyExportEdge>()?;
    m.add_class::<PyAdjacencyExport>()?;
    m.add_class::<IdArray>()?;
    m.add_class::<PyIdArrayIter>()?;
    m.add("OverGraphError", m.py().get_type::<OverGraphError>())?;
    Ok(())
}
