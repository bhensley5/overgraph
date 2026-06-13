#![allow(clippy::too_many_arguments)]

use eg::types::{
    CompoundIndexPlanDetails, GqlPath, GraphAggregateFunction, QueryPlanCompoundTargetKind,
};
use eg::{
    gql_referenced_param_names, AdjacencyExport as CoreAdjacencyExport, AllShortestPathsOptions,
    CompactionPhase, CompactionProgress as CoreCompactionProgress,
    CompactionStats as CoreCompactionStats, ComponentOptions,
    ComponentScrubFinding as CoreComponentScrubFinding, DatabaseEngine, DbOptions,
    DbStats as CoreDbStats, DegreeOptions, DenseMetric, DenseVectorConfig,
    DenseVectorSchema as CoreDenseVectorSchema, Direction, EdgeFilterExpr, EdgeInput,
    EdgeLabelInfo as CoreEdgeLabelInfo, EdgeMetadataIndexField as CoreEdgeMetadataIndexField,
    EdgePropertyIndexInfo as CoreEdgePropertyIndexInfo, EdgeQuery, EdgeQueryOrder,
    EdgeSchema as CoreEdgeSchema, EdgeSchemaInfo as CoreEdgeSchemaInfo,
    EdgeValiditySchema as CoreEdgeValiditySchema, EdgeView as CoreEdgeView,
    EndpointLabelSchema as CoreEndpointLabelSchema, EngineError, ExportOptions, FusionMode,
    GqlCapSummary, GqlEdge, GqlExecutionCapSummary, GqlExecutionExplain, GqlExecutionMode,
    GqlExecutionOptions, GqlExecutionResult, GqlExecutionStats, GqlExplain, GqlLoweringTarget,
    GqlNode, GqlParamValue, GqlParams, GqlRowOperation, GqlStatementKind, GqlValue, GraphBinaryOp,
    GraphCapExplain, GraphCaseBranch, GraphCursorExplain, GraphEdgePattern, GraphEdgeValue,
    GraphElementProjection, GraphExecutionSummaries, GraphExplainNode, GraphExpr, GraphFunction,
    GraphNodeField, GraphNodePattern, GraphNodeValue, GraphOrderDirection, GraphOrderExplain,
    GraphOrderItem, GraphOutputMode, GraphOutputOptions, GraphPageRequest, GraphParamValue,
    GraphPatch, GraphPathField, GraphPathValue, GraphPatternPiece, GraphPipelineCapExplain,
    GraphPipelineExplain, GraphPipelineMatchStage, GraphPipelineOptions, GraphPipelineQuery,
    GraphPipelineResult, GraphPipelineStage, GraphPipelineStageExplain, GraphPipelineStats,
    GraphProjectItem, GraphProjectKind, GraphProjectStage, GraphProjectionExplain,
    GraphProjectionItems, GraphQueryOptions, GraphReturnItem, GraphReturnProjection,
    GraphRowExplain, GraphRowOperationExplain, GraphRowQuery, GraphRowResult, GraphRowStats,
    GraphSchema as CoreGraphSchema, GraphSchemaCheckOptions as CoreGraphSchemaCheckOptions,
    GraphSchemaCheckReport as CoreGraphSchemaCheckReport,
    GraphSchemaDropAction as CoreGraphSchemaDropAction,
    GraphSchemaDropTargetResult as CoreGraphSchemaDropTargetResult,
    GraphSchemaOperation as CoreGraphSchemaOperation,
    GraphSchemaOperationKind as CoreGraphSchemaOperationKind,
    GraphSchemaPublishResult as CoreGraphSchemaPublishResult,
    GraphSchemaSetOptions as CoreGraphSchemaSetOptions,
    GraphSchemaValidationReportEntry as CoreGraphSchemaValidationReportEntry,
    GraphSelectedEdgeProjection, GraphSelectedNodeProjection, GraphSelectedPathProjection,
    GraphSelectedProjection, GraphShortestPathEndpoint, GraphShortestPathMode,
    GraphShortestPathStage, GraphSubqueryStage, GraphUnaryOp, GraphUnionStage, GraphValue,
    GraphVectorSelection, HnswConfig, IsConnectedOptions, LabelMatchMode,
    NeighborEntry as CoreNeighborEntry, NeighborOptions, NodeFilterExpr, NodeIdMap, NodeInput,
    NodeKeyQuery, NodeLabelConstraintSchema as CoreNodeLabelConstraintSchema, NodeLabelFilter,
    NodeLabelInfo as CoreNodeLabelInfo, NodeMetadataIndexField as CoreNodeMetadataIndexField,
    NodePropertyIndexInfo as CoreNodePropertyIndexInfo, NodeQuery, NodeQueryOrder,
    NodeSchema as CoreNodeSchema, NodeSchemaInfo as CoreNodeSchemaInfo, NodeView as CoreNodeView,
    NumericFieldSchema as CoreNumericFieldSchema, PageRequest, PprAlgorithm, PprOptions,
    PprResult as CorePprResult, PropValue, PropertyRangeBound as CorePropertyRangeBound,
    PropertyRangeCursor as CorePropertyRangeCursor, PropertyRangePageRequest,
    PropertyRangePageResult as CorePropertyRangePageResult, PropertySchema as CorePropertySchema,
    PrunePolicy, PrunePolicyInfo, PruneResult as CorePruneResult, QueryPlan, QueryPlanKind,
    QueryPlanNode, QueryPlanNote, QueryPlanPublicInputs, QueryPlanPublicName, QueryPlanWarning,
    SchemaAdditionalProperties, SchemaCheckOptions, SchemaNumericBound as CoreSchemaNumericBound,
    SchemaSetOptions, SchemaTargetKind as CoreSchemaTargetKind,
    SchemaValidationReport as CoreSchemaValidationReport, SchemaValueType, SchemaVectorPresence,
    SchemaViolation as CoreSchemaViolation, SchemaViolationTarget as CoreSchemaViolationTarget,
    ScoringMode, ScrubReport as CoreScrubReport, SecondaryIndexField as CoreSecondaryIndexField,
    SecondaryIndexKind, SecondaryIndexSpec as CoreSecondaryIndexSpec, SecondaryIndexState,
    SegmentScrubResult as CoreSegmentScrubResult, ShortestPath as CoreShortestPath,
    ShortestPathOptions, SparseVectorSchema as CoreSparseVectorSchema,
    StringFieldSchema as CoreStringFieldSchema, Subgraph as CoreSubgraph, SubgraphOptions,
    TopKOptions, TraversalCursor as CoreTraversalCursor, TraversalHit as CoreTraversalHit,
    TraversalPageResult as CoreTraversalPageResult, TraverseOptions,
    TxnCommitResult as CoreTxnCommitResult, TxnEdgeRef, TxnEdgeView, TxnIntent, TxnLocalRef,
    TxnNodeRef, TxnNodeView, UpsertEdgeOptions, UpsertNodeOptions, VectorSearchMode,
    VectorSearchRequest, VectorSearchScope, WalSyncMode, WriteTxn as CoreWriteTxn,
};
use pyo3::exceptions::{PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{
    PyAny, PyBool, PyByteArray, PyBytes, PyDict, PyList, PyString, PyStringMethods, PyTuple,
};
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

    fn stats(&self, py: Python<'_>) -> PyResult<DbStats> {
        with_engine_ref(self, py, |eng| Ok(DbStats::from(eng.stats()?)))
    }

    fn scrub(&self, py: Python<'_>) -> PyResult<ScrubReport> {
        with_engine_ref(self, py, |eng| Ok(ScrubReport::from(eng.scrub()?)))
    }

    // --- Catalog diagnostics ---

    fn ensure_node_label(&self, py: Python<'_>, label: String) -> PyResult<u32> {
        with_engine(self, py, move |eng| eng.ensure_node_label(&label))
    }

    fn ensure_edge_label(&self, py: Python<'_>, label: String) -> PyResult<u32> {
        with_engine(self, py, move |eng| eng.ensure_edge_label(&label))
    }

    fn get_node_label_id(&self, py: Python<'_>, label: String) -> PyResult<Option<u32>> {
        with_engine_ref(self, py, move |eng| eng.get_node_label_id(&label))
    }

    fn get_edge_label_id(&self, py: Python<'_>, label: String) -> PyResult<Option<u32>> {
        with_engine_ref(self, py, move |eng| eng.get_edge_label_id(&label))
    }

    fn get_node_label(&self, py: Python<'_>, label_id: u32) -> PyResult<Option<String>> {
        with_engine_ref(self, py, move |eng| eng.get_node_label(label_id))
    }

    fn get_edge_label(&self, py: Python<'_>, label_id: u32) -> PyResult<Option<String>> {
        with_engine_ref(self, py, move |eng| eng.get_edge_label(label_id))
    }

    fn list_node_labels(&self, py: Python<'_>) -> PyResult<Vec<NodeLabelInfo>> {
        with_engine_ref(self, py, |eng| {
            Ok(eng
                .list_node_labels()?
                .into_iter()
                .map(NodeLabelInfo::from)
                .collect())
        })
    }

    fn list_edge_labels(&self, py: Python<'_>) -> PyResult<Vec<EdgeLabelInfo>> {
        with_engine_ref(self, py, |eng| {
            Ok(eng
                .list_edge_labels()?
                .into_iter()
                .map(EdgeLabelInfo::from)
                .collect())
        })
    }

    // --- Schemas ---

    #[pyo3(signature = (label, schema, *, max_violations=1, chunk_size=4096, scan_limit=None))]
    fn set_node_schema(
        &self,
        py: Python<'_>,
        label: String,
        schema: &Bound<'_, PyDict>,
        max_violations: usize,
        chunk_size: usize,
        scan_limit: Option<u64>,
    ) -> PyResult<NodeSchemaInfo> {
        let schema = parse_py_node_schema(py, schema)?;
        let options = SchemaSetOptions {
            max_violations,
            chunk_size,
            scan_limit,
        };
        with_engine(self, py, move |eng| {
            Ok(NodeSchemaInfo::from(
                eng.set_node_schema_with_options(&label, schema, options)?,
            ))
        })
    }

    #[pyo3(signature = (label, schema, *, max_violations=100, chunk_size=4096, scan_limit=None))]
    fn check_node_schema(
        &self,
        py: Python<'_>,
        label: String,
        schema: &Bound<'_, PyDict>,
        max_violations: usize,
        chunk_size: usize,
        scan_limit: Option<u64>,
    ) -> PyResult<SchemaValidationReport> {
        let schema = parse_py_node_schema(py, schema)?;
        let options = SchemaCheckOptions {
            max_violations,
            chunk_size,
            scan_limit,
        };
        with_engine_ref(self, py, move |eng| {
            Ok(SchemaValidationReport::from(
                eng.check_node_schema(&label, schema, options)?,
            ))
        })
    }

    fn drop_node_schema(&self, py: Python<'_>, label: String) -> PyResult<bool> {
        with_engine(self, py, move |eng| eng.drop_node_schema(&label))
    }

    fn get_node_schema(&self, py: Python<'_>, label: String) -> PyResult<Option<NodeSchemaInfo>> {
        with_engine_ref(self, py, move |eng| {
            Ok(eng.get_node_schema(&label)?.map(NodeSchemaInfo::from))
        })
    }

    fn list_node_schemas(&self, py: Python<'_>) -> PyResult<Vec<NodeSchemaInfo>> {
        with_engine_ref(self, py, |eng| {
            Ok(eng
                .list_node_schemas()?
                .into_iter()
                .map(NodeSchemaInfo::from)
                .collect())
        })
    }

    #[pyo3(signature = (label, schema, *, max_violations=1, chunk_size=4096, scan_limit=None))]
    fn set_edge_schema(
        &self,
        py: Python<'_>,
        label: String,
        schema: &Bound<'_, PyDict>,
        max_violations: usize,
        chunk_size: usize,
        scan_limit: Option<u64>,
    ) -> PyResult<EdgeSchemaInfo> {
        let schema = parse_py_edge_schema(py, schema)?;
        let options = SchemaSetOptions {
            max_violations,
            chunk_size,
            scan_limit,
        };
        with_engine(self, py, move |eng| {
            Ok(EdgeSchemaInfo::from(
                eng.set_edge_schema_with_options(&label, schema, options)?,
            ))
        })
    }

    #[pyo3(signature = (label, schema, *, max_violations=100, chunk_size=4096, scan_limit=None))]
    fn check_edge_schema(
        &self,
        py: Python<'_>,
        label: String,
        schema: &Bound<'_, PyDict>,
        max_violations: usize,
        chunk_size: usize,
        scan_limit: Option<u64>,
    ) -> PyResult<SchemaValidationReport> {
        let schema = parse_py_edge_schema(py, schema)?;
        let options = SchemaCheckOptions {
            max_violations,
            chunk_size,
            scan_limit,
        };
        with_engine_ref(self, py, move |eng| {
            Ok(SchemaValidationReport::from(
                eng.check_edge_schema(&label, schema, options)?,
            ))
        })
    }

    fn drop_edge_schema(&self, py: Python<'_>, label: String) -> PyResult<bool> {
        with_engine(self, py, move |eng| eng.drop_edge_schema(&label))
    }

    fn get_edge_schema(&self, py: Python<'_>, label: String) -> PyResult<Option<EdgeSchemaInfo>> {
        with_engine_ref(self, py, move |eng| {
            Ok(eng.get_edge_schema(&label)?.map(EdgeSchemaInfo::from))
        })
    }

    fn list_edge_schemas(&self, py: Python<'_>) -> PyResult<Vec<EdgeSchemaInfo>> {
        with_engine_ref(self, py, |eng| {
            Ok(eng
                .list_edge_schemas()?
                .into_iter()
                .map(EdgeSchemaInfo::from)
                .collect())
        })
    }

    #[pyo3(signature = (schema, *, max_violations=1, chunk_size=4096, scan_limit=None))]
    fn set_graph_schema(
        &self,
        py: Python<'_>,
        schema: &Bound<'_, PyDict>,
        max_violations: usize,
        chunk_size: usize,
        scan_limit: Option<u64>,
    ) -> PyResult<GraphSchemaPublishResult> {
        let schema = parse_py_graph_schema(py, schema)?;
        let options = CoreGraphSchemaSetOptions {
            max_violations,
            chunk_size,
            scan_limit,
        };
        with_engine(self, py, move |eng| {
            Ok(GraphSchemaPublishResult::from(
                eng.set_graph_schema(schema, options)?,
            ))
        })
    }

    #[pyo3(signature = (operations, *, max_violations=1, chunk_size=4096, scan_limit=None))]
    fn alter_graph_schema(
        &self,
        py: Python<'_>,
        operations: &Bound<'_, PyAny>,
        max_violations: usize,
        chunk_size: usize,
        scan_limit: Option<u64>,
    ) -> PyResult<GraphSchemaPublishResult> {
        let operations = parse_py_graph_schema_operations(py, operations)?;
        let options = CoreGraphSchemaSetOptions {
            max_violations,
            chunk_size,
            scan_limit,
        };
        with_engine(self, py, move |eng| {
            Ok(GraphSchemaPublishResult::from(
                eng.alter_graph_schema(operations, options)?,
            ))
        })
    }

    #[pyo3(signature = (schema, *, max_violations=100, chunk_size=4096, scan_limit=None))]
    fn check_graph_schema_set(
        &self,
        py: Python<'_>,
        schema: &Bound<'_, PyDict>,
        max_violations: usize,
        chunk_size: usize,
        scan_limit: Option<u64>,
    ) -> PyResult<GraphSchemaCheckReport> {
        let schema = parse_py_graph_schema(py, schema)?;
        let options = CoreGraphSchemaCheckOptions {
            max_violations,
            chunk_size,
            scan_limit,
        };
        with_engine_ref(self, py, move |eng| {
            Ok(GraphSchemaCheckReport::from(
                eng.check_graph_schema_set(schema, options)?,
            ))
        })
    }

    #[pyo3(signature = (schema, *, max_violations=100, chunk_size=4096, scan_limit=None))]
    fn check_graph_schema_add(
        &self,
        py: Python<'_>,
        schema: &Bound<'_, PyDict>,
        max_violations: usize,
        chunk_size: usize,
        scan_limit: Option<u64>,
    ) -> PyResult<GraphSchemaCheckReport> {
        let schema = parse_py_graph_schema(py, schema)?;
        let options = CoreGraphSchemaCheckOptions {
            max_violations,
            chunk_size,
            scan_limit,
        };
        with_engine_ref(self, py, move |eng| {
            Ok(GraphSchemaCheckReport::from(
                eng.check_graph_schema_add(schema, options)?,
            ))
        })
    }

    fn drop_graph_schema(&self, py: Python<'_>) -> PyResult<GraphSchemaPublishResult> {
        with_engine(self, py, move |eng| {
            Ok(GraphSchemaPublishResult::from(eng.drop_graph_schema()?))
        })
    }

    // --- Single CRUD ---

    #[pyo3(signature = (labels, key, *, props=None, weight=1.0, dense_vector=None, sparse_vector=None))]
    fn upsert_node(
        &self,
        py: Python<'_>,
        labels: &Bound<'_, PyAny>,
        key: String,
        props: Option<&Bound<'_, PyDict>>,
        weight: f64,
        dense_vector: Option<Vec<f32>>,
        sparse_vector: Option<Vec<(u32, f32)>>,
    ) -> PyResult<u64> {
        let labels = parse_node_labels_arg(labels, "upsert_node labels")?;
        let props = convert_py_props(py, props)?;
        let opts = UpsertNodeOptions {
            props,
            weight: weight as f32,
            dense_vector,
            sparse_vector,
        };
        with_engine(self, py, move |eng| eng.upsert_node(labels, &key, opts))
    }

    fn add_node_label(&self, py: Python<'_>, node_id: u64, label: String) -> PyResult<bool> {
        with_engine(self, py, move |eng| eng.add_node_label(node_id, &label))
    }

    fn remove_node_label(&self, py: Python<'_>, node_id: u64, label: String) -> PyResult<bool> {
        with_engine(self, py, move |eng| eng.remove_node_label(node_id, &label))
    }

    #[pyo3(signature = (from_id, to_id, label, *, props=None, weight=1.0, valid_from=None, valid_to=None))]
    fn upsert_edge(
        &self,
        py: Python<'_>,
        from_id: u64,
        to_id: u64,
        label: String,
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
            eng.upsert_edge(from_id, to_id, &label, opts)
        })
    }

    fn get_node(&self, py: Python<'_>, id: u64) -> PyResult<Option<NodeView>> {
        with_engine_ref(self, py, |eng| {
            eng.get_node(id)?.map(NodeView::try_from).transpose()
        })
    }

    fn get_edge(&self, py: Python<'_>, id: u64) -> PyResult<Option<EdgeView>> {
        with_engine_ref(self, py, |eng| Ok(eng.get_edge(id)?.map(EdgeView::from)))
    }

    fn get_node_by_key(
        &self,
        py: Python<'_>,
        label: String,
        key: String,
    ) -> PyResult<Option<NodeView>> {
        with_engine_ref(self, py, move |eng| {
            eng.get_node_by_key(&label, &key)?
                .map(NodeView::try_from)
                .transpose()
        })
    }

    fn get_edge_by_triple(
        &self,
        py: Python<'_>,
        from_id: u64,
        to_id: u64,
        label: String,
    ) -> PyResult<Option<EdgeView>> {
        with_engine_ref(self, py, move |eng| {
            Ok(eng
                .get_edge_by_triple(from_id, to_id, &label)?
                .map(EdgeView::from))
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
    ) -> PyResult<Option<EdgeView>> {
        with_engine(self, py, move |eng| {
            Ok(eng.invalidate_edge(id, valid_to)?.map(EdgeView::from))
        })
    }

    // --- Batch ops ---

    fn batch_upsert_nodes(&self, py: Python<'_>, nodes: &Bound<'_, PyList>) -> PyResult<Vec<u64>> {
        let inputs = parse_node_inputs(py, nodes)?;
        with_engine(self, py, move |eng| eng.batch_upsert_nodes(inputs))
    }

    fn batch_upsert_edges(&self, py: Python<'_>, edges: &Bound<'_, PyList>) -> PyResult<Vec<u64>> {
        let inputs = parse_edge_inputs(py, edges)?;
        with_engine(self, py, move |eng| eng.batch_upsert_edges(inputs))
    }

    fn get_nodes(&self, py: Python<'_>, ids: Vec<u64>) -> PyResult<Vec<Option<NodeView>>> {
        with_engine_ref(self, py, move |eng| {
            let results = eng.get_nodes(&ids)?;
            results
                .into_iter()
                .map(|r| r.map(NodeView::try_from).transpose())
                .collect()
        })
    }

    fn get_nodes_by_keys(
        &self,
        py: Python<'_>,
        keys: &Bound<'_, PyList>,
    ) -> PyResult<Vec<Option<NodeView>>> {
        let keys = parse_node_key_queries(keys)?;
        with_engine_ref(self, py, move |eng| {
            let results = eng.get_nodes_by_keys(&keys)?;
            results
                .into_iter()
                .map(|r| r.map(NodeView::try_from).transpose())
                .collect()
        })
    }

    fn get_edges(&self, py: Python<'_>, ids: Vec<u64>) -> PyResult<Vec<Option<EdgeView>>> {
        with_engine_ref(self, py, move |eng| {
            let results = eng.get_edges(&ids)?;
            Ok(results.into_iter().map(|r| r.map(EdgeView::from)).collect())
        })
    }

    fn graph_patch(&self, py: Python<'_>, patch: &Bound<'_, PyDict>) -> PyResult<PatchResult> {
        let rust_patch = parse_graph_patch(py, patch)?;
        with_engine(self, py, move |eng| {
            let result = eng.graph_patch(rust_patch)?;
            Ok(PatchResult {
                node_ids: result.node_ids,
                edge_ids: result.edge_ids,
            })
        })
    }

    fn begin_write_txn(&self, py: Python<'_>) -> PyResult<WriteTxn> {
        let txn = with_engine_ref(self, py, |eng| eng.begin_write_txn())?;
        Ok(WriteTxn {
            inner: Arc::new(Mutex::new(Some(txn))),
        })
    }

    // --- Queries ---

    fn find_nodes(
        &self,
        py: Python<'_>,
        label: String,
        prop_key: String,
        prop_value: &Bound<'_, pyo3::PyAny>,
    ) -> PyResult<IdArray> {
        let pv = py_to_prop_value(py, prop_value)?;
        with_engine_ref(self, py, move |eng| {
            Ok(IdArray {
                ids: Arc::new(eng.find_nodes(&label, &prop_key, &pv)?),
            })
        })
    }

    fn query_node_ids(&self, py: Python<'_>, request: &Bound<'_, PyAny>) -> PyResult<IdPageResult> {
        let query = parse_py_node_query(py, request)?;
        with_engine_ref(self, py, move |eng| {
            let result = eng.query_node_ids(&query)?;
            Ok(IdPageResult {
                items: IdArray {
                    ids: Arc::new(result.items),
                },
                next_cursor: result.next_cursor,
            })
        })
    }

    fn query_nodes(&self, py: Python<'_>, request: &Bound<'_, PyAny>) -> PyResult<NodePageResult> {
        let query = parse_py_node_query(py, request)?;
        with_engine_ref(self, py, move |eng| {
            let result = eng.query_nodes(&query)?;
            Ok(NodePageResult {
                items: result
                    .items
                    .into_iter()
                    .map(NodeView::try_from)
                    .collect::<Result<Vec<_>, EngineError>>()?,
                next_cursor: result.next_cursor,
            })
        })
    }

    fn query_edge_ids(&self, py: Python<'_>, request: &Bound<'_, PyAny>) -> PyResult<IdPageResult> {
        let query = parse_py_edge_query(py, request)?;
        with_engine_ref(self, py, move |eng| {
            let result = eng.query_edge_ids(&query)?;
            Ok(IdPageResult {
                items: IdArray {
                    ids: Arc::new(result.edge_ids),
                },
                next_cursor: result.next_cursor,
            })
        })
    }

    fn query_edges(&self, py: Python<'_>, request: &Bound<'_, PyAny>) -> PyResult<EdgePageResult> {
        let query = parse_py_edge_query(py, request)?;
        with_engine_ref(self, py, move |eng| {
            let result = eng.query_edges(&query)?;
            Ok(EdgePageResult {
                items: result.edges.into_iter().map(EdgeView::from).collect(),
                next_cursor: result.next_cursor,
            })
        })
    }

    fn query_pattern(&self, _py: Python<'_>, _request: &Bound<'_, PyAny>) -> PyResult<PyObject> {
        Err(OverGraphError::new_err(
            "query_pattern is unsupported; use query_graph_rows",
        ))
    }

    fn explain_node_query(&self, py: Python<'_>, request: &Bound<'_, PyAny>) -> PyResult<PyObject> {
        let query = parse_py_node_query(py, request)?;
        let plan = with_engine_ref(self, py, move |eng| eng.explain_node_query(&query))?;
        query_plan_to_py(py, plan)
    }

    fn explain_edge_query(&self, py: Python<'_>, request: &Bound<'_, PyAny>) -> PyResult<PyObject> {
        let query = parse_py_edge_query(py, request)?;
        let plan = with_engine_ref(self, py, move |eng| eng.explain_edge_query(&query))?;
        query_plan_to_py(py, plan)
    }

    fn explain_pattern_query(
        &self,
        _py: Python<'_>,
        _request: &Bound<'_, PyAny>,
    ) -> PyResult<PyObject> {
        Err(OverGraphError::new_err(
            "explain_pattern_query is unsupported; use explain_graph_rows",
        ))
    }

    fn query_graph_rows(&self, py: Python<'_>, request: &Bound<'_, PyAny>) -> PyResult<PyObject> {
        let query = parse_py_graph_row_query(py, request)?;
        let compact_rows = query.output.compact_rows;
        let result = with_engine_ref(self, py, move |eng| eng.query_graph_rows(&query))?;
        graph_row_result_to_py(py, result, compact_rows)
    }

    fn explain_graph_rows(&self, py: Python<'_>, request: &Bound<'_, PyAny>) -> PyResult<PyObject> {
        let query = parse_py_graph_row_query(py, request)?;
        let explain = with_engine_ref(self, py, move |eng| eng.explain_graph_rows(&query))?;
        graph_row_explain_to_py(py, explain)
    }

    fn query_graph_pipeline(
        &self,
        py: Python<'_>,
        request: &Bound<'_, PyAny>,
    ) -> PyResult<PyObject> {
        let query = parse_py_graph_pipeline_query(py, request)?;
        let compact_rows = query.output.compact_rows;
        let result = with_engine_ref(self, py, move |eng| eng.query_graph_pipeline(&query))?;
        graph_pipeline_result_to_py(py, result, compact_rows)
    }

    fn explain_graph_pipeline(
        &self,
        py: Python<'_>,
        request: &Bound<'_, PyAny>,
    ) -> PyResult<PyObject> {
        let query = parse_py_graph_pipeline_query(py, request)?;
        let explain = with_engine_ref(self, py, move |eng| eng.explain_graph_pipeline(&query))?;
        graph_pipeline_explain_to_py(py, explain)
    }

    #[pyo3(signature = (query, params=None, *, mode="auto", allow_full_scan=false, max_rows=None, cursor=None, max_cursor_bytes=None, max_mutation_rows=None, max_mutation_ops=None, max_pipeline_rows=None, max_groups=None, max_collect_items=None, max_union_branches=None, max_subquery_invocations=None, max_subquery_depth=None, max_shortest_path_pairs=None, max_intermediate_bindings=None, max_frontier=None, max_path_hops=None, max_paths_per_start=None, max_order_materialization=None, max_skip=None, max_query_bytes=None, max_param_bytes=None, max_ast_depth=None, max_literal_items=None, include_plan=false, profile=false, compact_rows=false, include_vectors=false))]
    fn execute_gql(
        &self,
        py: Python<'_>,
        query: String,
        params: Option<&Bound<'_, PyDict>>,
        mode: &str,
        allow_full_scan: bool,
        max_rows: Option<usize>,
        cursor: Option<String>,
        max_cursor_bytes: Option<usize>,
        max_mutation_rows: Option<usize>,
        max_mutation_ops: Option<usize>,
        max_pipeline_rows: Option<usize>,
        max_groups: Option<usize>,
        max_collect_items: Option<usize>,
        max_union_branches: Option<usize>,
        max_subquery_invocations: Option<usize>,
        max_subquery_depth: Option<usize>,
        max_shortest_path_pairs: Option<usize>,
        max_intermediate_bindings: Option<usize>,
        max_frontier: Option<usize>,
        max_path_hops: Option<u8>,
        max_paths_per_start: Option<usize>,
        max_order_materialization: Option<usize>,
        max_skip: Option<usize>,
        max_query_bytes: Option<usize>,
        max_param_bytes: Option<usize>,
        max_ast_depth: Option<usize>,
        max_literal_items: Option<usize>,
        include_plan: bool,
        profile: bool,
        compact_rows: bool,
        include_vectors: bool,
    ) -> PyResult<PyObject> {
        let options = parse_py_gql_options(
            mode,
            allow_full_scan,
            max_rows,
            cursor,
            max_cursor_bytes,
            max_mutation_rows,
            max_mutation_ops,
            max_pipeline_rows,
            max_groups,
            max_collect_items,
            max_union_branches,
            max_subquery_invocations,
            max_subquery_depth,
            max_shortest_path_pairs,
            max_intermediate_bindings,
            max_frontier,
            max_path_hops,
            max_paths_per_start,
            max_order_materialization,
            max_skip,
            max_query_bytes,
            max_param_bytes,
            max_ast_depth,
            max_literal_items,
            include_plan,
            profile,
            compact_rows,
            include_vectors,
        )?;
        let referenced_params = gql_referenced_param_names(&query, &options).map_err(to_py_err)?;
        let params = parse_py_gql_params(py, params, &referenced_params, &options)?;
        let result = with_engine_ref(self, py, move |eng| {
            eng.execute_gql(&query, &params, &options)
        })?;
        gql_result_to_py(py, result, compact_rows)
    }

    #[pyo3(signature = (query, params=None, *, mode="auto", allow_full_scan=false, max_rows=None, cursor=None, max_cursor_bytes=None, max_mutation_rows=None, max_mutation_ops=None, max_pipeline_rows=None, max_groups=None, max_collect_items=None, max_union_branches=None, max_subquery_invocations=None, max_subquery_depth=None, max_shortest_path_pairs=None, max_intermediate_bindings=None, max_frontier=None, max_path_hops=None, max_paths_per_start=None, max_order_materialization=None, max_skip=None, max_query_bytes=None, max_param_bytes=None, max_ast_depth=None, max_literal_items=None, include_plan=false, profile=false, compact_rows=false, include_vectors=false))]
    fn explain_gql(
        &self,
        py: Python<'_>,
        query: String,
        params: Option<&Bound<'_, PyDict>>,
        mode: &str,
        allow_full_scan: bool,
        max_rows: Option<usize>,
        cursor: Option<String>,
        max_cursor_bytes: Option<usize>,
        max_mutation_rows: Option<usize>,
        max_mutation_ops: Option<usize>,
        max_pipeline_rows: Option<usize>,
        max_groups: Option<usize>,
        max_collect_items: Option<usize>,
        max_union_branches: Option<usize>,
        max_subquery_invocations: Option<usize>,
        max_subquery_depth: Option<usize>,
        max_shortest_path_pairs: Option<usize>,
        max_intermediate_bindings: Option<usize>,
        max_frontier: Option<usize>,
        max_path_hops: Option<u8>,
        max_paths_per_start: Option<usize>,
        max_order_materialization: Option<usize>,
        max_skip: Option<usize>,
        max_query_bytes: Option<usize>,
        max_param_bytes: Option<usize>,
        max_ast_depth: Option<usize>,
        max_literal_items: Option<usize>,
        include_plan: bool,
        profile: bool,
        compact_rows: bool,
        include_vectors: bool,
    ) -> PyResult<PyObject> {
        let options = parse_py_gql_options(
            mode,
            allow_full_scan,
            max_rows,
            cursor,
            max_cursor_bytes,
            max_mutation_rows,
            max_mutation_ops,
            max_pipeline_rows,
            max_groups,
            max_collect_items,
            max_union_branches,
            max_subquery_invocations,
            max_subquery_depth,
            max_shortest_path_pairs,
            max_intermediate_bindings,
            max_frontier,
            max_path_hops,
            max_paths_per_start,
            max_order_materialization,
            max_skip,
            max_query_bytes,
            max_param_bytes,
            max_ast_depth,
            max_literal_items,
            include_plan,
            profile,
            compact_rows,
            include_vectors,
        )?;
        let referenced_params = gql_referenced_param_names(&query, &options).map_err(to_py_err)?;
        let params = parse_py_gql_params(py, params, &referenced_params, &options)?;
        let explain = with_engine_ref(self, py, move |eng| {
            eng.explain_gql(&query, &params, &options)
        })?;
        gql_explain_to_py(py, explain)
    }

    #[pyo3(signature = (label, spec))]
    fn ensure_node_property_index(
        &self,
        py: Python<'_>,
        label: String,
        spec: &Bound<'_, PyAny>,
    ) -> PyResult<NodePropertyIndexInfo> {
        let spec = parse_py_secondary_index_spec(spec, PySecondaryIndexTargetKind::Node)?;
        with_engine(self, py, move |eng| {
            Ok(NodePropertyIndexInfo::from(
                eng.ensure_node_property_index(&label, spec.clone())?,
            ))
        })
    }

    #[pyo3(signature = (label, spec))]
    fn drop_node_property_index(
        &self,
        py: Python<'_>,
        label: String,
        spec: &Bound<'_, PyAny>,
    ) -> PyResult<bool> {
        let spec = parse_py_secondary_index_spec(spec, PySecondaryIndexTargetKind::Node)?;
        with_engine(self, py, move |eng| {
            eng.drop_node_property_index(&label, spec.clone())
        })
    }

    fn list_node_property_indexes(&self, py: Python<'_>) -> PyResult<Vec<NodePropertyIndexInfo>> {
        with_engine_ref(self, py, |eng| {
            Ok(eng
                .list_node_property_indexes()?
                .into_iter()
                .map(NodePropertyIndexInfo::from)
                .collect())
        })
    }

    #[pyo3(signature = (label, spec))]
    fn ensure_edge_property_index(
        &self,
        py: Python<'_>,
        label: String,
        spec: &Bound<'_, PyAny>,
    ) -> PyResult<EdgePropertyIndexInfo> {
        let spec = parse_py_secondary_index_spec(spec, PySecondaryIndexTargetKind::Edge)?;
        with_engine(self, py, move |eng| {
            Ok(EdgePropertyIndexInfo::from(
                eng.ensure_edge_property_index(&label, spec.clone())?,
            ))
        })
    }

    #[pyo3(signature = (label, spec))]
    fn drop_edge_property_index(
        &self,
        py: Python<'_>,
        label: String,
        spec: &Bound<'_, PyAny>,
    ) -> PyResult<bool> {
        let spec = parse_py_secondary_index_spec(spec, PySecondaryIndexTargetKind::Edge)?;
        with_engine(self, py, move |eng| {
            eng.drop_edge_property_index(&label, spec.clone())
        })
    }

    fn list_edge_property_indexes(&self, py: Python<'_>) -> PyResult<Vec<EdgePropertyIndexInfo>> {
        with_engine_ref(self, py, |eng| {
            Ok(eng
                .list_edge_property_indexes()?
                .into_iter()
                .map(EdgePropertyIndexInfo::from)
                .collect())
        })
    }

    fn nodes_by_labels(&self, py: Python<'_>, labels: &Bound<'_, PyAny>) -> PyResult<IdArray> {
        let labels = parse_node_labels_arg(labels, "nodes_by_labels labels")?;
        with_engine_ref(self, py, move |eng| {
            Ok(IdArray {
                ids: Arc::new(eng.nodes_by_labels(labels)?),
            })
        })
    }

    fn get_nodes_by_labels(
        &self,
        py: Python<'_>,
        labels: &Bound<'_, PyAny>,
    ) -> PyResult<Vec<NodeView>> {
        let labels = parse_node_labels_arg(labels, "get_nodes_by_labels labels")?;
        with_engine_ref(self, py, move |eng| {
            eng.get_nodes_by_labels(labels)?
                .into_iter()
                .map(NodeView::try_from)
                .collect()
        })
    }

    fn edges_by_label(&self, py: Python<'_>, label: String) -> PyResult<IdArray> {
        with_engine_ref(self, py, move |eng| {
            Ok(IdArray {
                ids: Arc::new(eng.edges_by_label(&label)?),
            })
        })
    }

    fn get_edges_by_label(&self, py: Python<'_>, label: String) -> PyResult<Vec<EdgeView>> {
        with_engine_ref(self, py, move |eng| {
            Ok(eng
                .get_edges_by_label(&label)?
                .into_iter()
                .map(EdgeView::from)
                .collect())
        })
    }

    fn count_nodes_by_labels(&self, py: Python<'_>, labels: &Bound<'_, PyAny>) -> PyResult<u64> {
        let labels = parse_node_labels_arg(labels, "count_nodes_by_labels labels")?;
        with_engine_ref(self, py, move |eng| eng.count_nodes_by_labels(labels))
    }

    fn count_edges_by_label(&self, py: Python<'_>, label: String) -> PyResult<u64> {
        with_engine_ref(self, py, move |eng| eng.count_edges_by_label(&label))
    }

    fn find_nodes_by_time_range(
        &self,
        py: Python<'_>,
        label: String,
        from_ms: i64,
        to_ms: i64,
    ) -> PyResult<IdArray> {
        with_engine_ref(self, py, move |eng| {
            Ok(IdArray {
                ids: Arc::new(eng.find_nodes_by_time_range(&label, from_ms, to_ms)?),
            })
        })
    }

    #[pyo3(signature = (label, prop_key, lower=None, upper=None))]
    fn find_nodes_range(
        &self,
        py: Python<'_>,
        label: String,
        prop_key: String,
        lower: Option<PropertyRangeBound>,
        upper: Option<PropertyRangeBound>,
    ) -> PyResult<IdArray> {
        let lower = lower.map(CorePropertyRangeBound::from);
        let upper = upper.map(CorePropertyRangeBound::from);
        with_engine_ref(self, py, move |eng| {
            Ok(IdArray {
                ids: Arc::new(eng.find_nodes_range(
                    &label,
                    &prop_key,
                    lower.as_ref(),
                    upper.as_ref(),
                )?),
            })
        })
    }

    // --- Traversal ---

    #[pyo3(signature = (node_id, *, direction="outgoing", edge_label_filter=None, limit=None, at_epoch=None, decay_lambda=None))]
    fn neighbors(
        &self,
        py: Python<'_>,
        node_id: u64,
        direction: &str,
        edge_label_filter: Option<Vec<String>>,
        limit: Option<usize>,
        at_epoch: Option<i64>,
        decay_lambda: Option<f64>,
    ) -> PyResult<Vec<NeighborEntry>> {
        let dir = parse_direction(direction)?;
        let dl = decay_lambda.map(|v| v as f32);
        let opts = NeighborOptions {
            direction: dir,
            edge_label_filter,
            limit,
            at_epoch,
            decay_lambda: dl,
        };
        with_engine_ref(self, py, move |eng| {
            Ok(eng
                .neighbors(node_id, &opts)?
                .into_iter()
                .map(NeighborEntry::from)
                .collect())
        })
    }

    #[pyo3(signature = (start, max_depth, *, min_depth=1, direction="outgoing", edge_label_filter=None, emit_node_label_filter=None, at_epoch=None, decay_lambda=None, limit=None, cursor=None))]
    fn traverse(
        &self,
        py: Python<'_>,
        start: u64,
        max_depth: u32,
        min_depth: u32,
        direction: &str,
        edge_label_filter: Option<Vec<String>>,
        emit_node_label_filter: Option<&Bound<'_, PyAny>>,
        at_epoch: Option<i64>,
        decay_lambda: Option<f64>,
        limit: Option<usize>,
        cursor: Option<TraversalCursor>,
    ) -> PyResult<TraversalPageResult> {
        let dir = parse_direction(direction)?;
        let cursor = cursor.map(CoreTraversalCursor::from);
        let emit_node_label_filter = parse_optional_node_label_filter_arg(
            emit_node_label_filter,
            "traverse emit_node_label_filter",
        )?;
        let opts = TraverseOptions {
            min_depth,
            direction: dir,
            edge_label_filter,
            emit_node_label_filter,
            at_epoch,
            decay_lambda,
            limit,
            cursor,
        };
        with_engine_ref(self, py, move |eng| {
            Ok(TraversalPageResult::from(
                eng.traverse(start, max_depth, &opts)?,
            ))
        })
    }

    #[pyo3(signature = (node_id, k, *, direction="outgoing", edge_label_filter=None, scoring="weight", at_epoch=None, decay_lambda=None))]
    fn top_k_neighbors(
        &self,
        py: Python<'_>,
        node_id: u64,
        k: usize,
        direction: &str,
        edge_label_filter: Option<Vec<String>>,
        scoring: &str,
        at_epoch: Option<i64>,
        decay_lambda: Option<f64>,
    ) -> PyResult<Vec<NeighborEntry>> {
        let dir = parse_direction(direction)?;
        let sm = parse_scoring_mode(scoring, decay_lambda)?;
        let opts = TopKOptions {
            direction: dir,
            edge_label_filter,
            scoring: sm,
            at_epoch,
        };
        with_engine_ref(self, py, move |eng| {
            Ok(eng
                .top_k_neighbors(node_id, k, &opts)?
                .into_iter()
                .map(NeighborEntry::from)
                .collect())
        })
    }

    #[pyo3(signature = (start_node_id, max_depth, *, direction="outgoing", edge_label_filter=None, node_label_filter=None, at_epoch=None))]
    fn extract_subgraph(
        &self,
        py: Python<'_>,
        start_node_id: u64,
        max_depth: u32,
        direction: &str,
        edge_label_filter: Option<Vec<String>>,
        node_label_filter: Option<&Bound<'_, PyAny>>,
        at_epoch: Option<i64>,
    ) -> PyResult<Subgraph> {
        let dir = parse_direction(direction)?;
        let node_label_filter = parse_optional_node_label_filter_arg(
            node_label_filter,
            "extract_subgraph node_label_filter",
        )?;
        let opts = SubgraphOptions {
            direction: dir,
            edge_label_filter,
            node_label_filter,
            at_epoch,
        };
        with_engine_ref(self, py, move |eng| {
            let sg = eng.extract_subgraph(start_node_id, max_depth, &opts)?;
            Subgraph::try_from(sg)
        })
    }

    /// Batch neighbor query: fetch neighbors for multiple nodes in one call.
    /// Returns dict[int, list[NeighborEntry]] mapping each queried node_id to its neighbors.
    #[pyo3(signature = (node_ids, *, direction="outgoing", edge_label_filter=None, at_epoch=None, decay_lambda=None))]
    fn neighbors_batch(
        &self,
        py: Python<'_>,
        node_ids: Vec<u64>,
        direction: &str,
        edge_label_filter: Option<Vec<String>>,
        at_epoch: Option<i64>,
        decay_lambda: Option<f64>,
    ) -> PyResult<HashMap<u64, Vec<NeighborEntry>>> {
        let dir = parse_direction(direction)?;
        let dl = decay_lambda.map(|v| v as f32);
        let opts = NeighborOptions {
            direction: dir,
            edge_label_filter,
            limit: None,
            at_epoch,
            decay_lambda: dl,
        };
        with_engine_ref(self, py, move |eng| {
            let map = eng.neighbors_batch(&node_ids, &opts)?;
            Ok(map
                .into_iter()
                .map(|(k, v)| (k, v.into_iter().map(NeighborEntry::from).collect()))
                .collect())
        })
    }

    // --- Degree counts + aggregations (Phase 18a) ---

    #[pyo3(signature = (node_id, *, direction="outgoing", edge_label_filter=None, at_epoch=None))]
    fn degree(
        &self,
        py: Python<'_>,
        node_id: u64,
        direction: &str,
        edge_label_filter: Option<Vec<String>>,
        at_epoch: Option<i64>,
    ) -> PyResult<u64> {
        let dir = parse_direction(direction)?;
        let opts = DegreeOptions {
            direction: dir,
            edge_label_filter,
            at_epoch,
        };
        with_engine_ref(self, py, move |eng| eng.degree(node_id, &opts))
    }

    #[pyo3(signature = (node_id, *, direction="outgoing", edge_label_filter=None, at_epoch=None))]
    fn sum_edge_weights(
        &self,
        py: Python<'_>,
        node_id: u64,
        direction: &str,
        edge_label_filter: Option<Vec<String>>,
        at_epoch: Option<i64>,
    ) -> PyResult<f64> {
        let dir = parse_direction(direction)?;
        let opts = DegreeOptions {
            direction: dir,
            edge_label_filter,
            at_epoch,
        };
        with_engine_ref(self, py, move |eng| eng.sum_edge_weights(node_id, &opts))
    }

    #[pyo3(signature = (node_id, *, direction="outgoing", edge_label_filter=None, at_epoch=None))]
    fn avg_edge_weight(
        &self,
        py: Python<'_>,
        node_id: u64,
        direction: &str,
        edge_label_filter: Option<Vec<String>>,
        at_epoch: Option<i64>,
    ) -> PyResult<Option<f64>> {
        let dir = parse_direction(direction)?;
        let opts = DegreeOptions {
            direction: dir,
            edge_label_filter,
            at_epoch,
        };
        with_engine_ref(self, py, move |eng| eng.avg_edge_weight(node_id, &opts))
    }

    #[pyo3(signature = (node_ids, *, direction="outgoing", edge_label_filter=None, at_epoch=None))]
    fn degrees(
        &self,
        py: Python<'_>,
        node_ids: Vec<u64>,
        direction: &str,
        edge_label_filter: Option<Vec<String>>,
        at_epoch: Option<i64>,
    ) -> PyResult<NodeIdMap<u64>> {
        let dir = parse_direction(direction)?;
        let opts = DegreeOptions {
            direction: dir,
            edge_label_filter,
            at_epoch,
        };
        with_engine_ref(self, py, move |eng| eng.degrees(&node_ids, &opts))
    }

    // --- Shortest path (Phase 18b) ---

    #[pyo3(signature = (from_id, to_id, *, direction="outgoing", edge_label_filter=None, weight_field=None, at_epoch=None, max_depth=None, max_cost=None))]
    fn shortest_path(
        &self,
        py: Python<'_>,
        from_id: u64,
        to_id: u64,
        direction: &str,
        edge_label_filter: Option<Vec<String>>,
        weight_field: Option<&str>,
        at_epoch: Option<i64>,
        max_depth: Option<u32>,
        max_cost: Option<f64>,
    ) -> PyResult<Option<ShortestPath>> {
        let dir = parse_direction(direction)?;
        let opts = ShortestPathOptions {
            direction: dir,
            edge_label_filter,
            weight_field: weight_field.map(|s| s.to_string()),
            at_epoch,
            max_depth,
            max_cost,
        };
        with_engine_ref(self, py, move |eng| {
            Ok(eng
                .shortest_path(from_id, to_id, &opts)?
                .map(ShortestPath::from))
        })
    }

    #[pyo3(signature = (from_id, to_id, *, direction="outgoing", edge_label_filter=None, at_epoch=None, max_depth=None))]
    fn is_connected(
        &self,
        py: Python<'_>,
        from_id: u64,
        to_id: u64,
        direction: &str,
        edge_label_filter: Option<Vec<String>>,
        at_epoch: Option<i64>,
        max_depth: Option<u32>,
    ) -> PyResult<bool> {
        let dir = parse_direction(direction)?;
        let opts = IsConnectedOptions {
            direction: dir,
            edge_label_filter,
            at_epoch,
            max_depth,
        };
        with_engine_ref(self, py, move |eng| eng.is_connected(from_id, to_id, &opts))
    }

    #[pyo3(signature = (from_id, to_id, *, direction="outgoing", edge_label_filter=None, weight_field=None, at_epoch=None, max_depth=None, max_cost=None, max_paths=None))]
    fn all_shortest_paths(
        &self,
        py: Python<'_>,
        from_id: u64,
        to_id: u64,
        direction: &str,
        edge_label_filter: Option<Vec<String>>,
        weight_field: Option<&str>,
        at_epoch: Option<i64>,
        max_depth: Option<u32>,
        max_cost: Option<f64>,
        max_paths: Option<usize>,
    ) -> PyResult<Vec<ShortestPath>> {
        let dir = parse_direction(direction)?;
        let opts = AllShortestPathsOptions {
            direction: dir,
            edge_label_filter,
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
                .map(ShortestPath::from)
                .collect())
        })
    }

    // --- Binary batch upserts ---

    /// Batch upsert nodes from a packed binary buffer.
    ///
    /// Binary format (little-endian):
    ///   [magic: "OGNB"][version: u16 = 2][count: u32]
    ///   per node:
    ///     [label_count: u8] repeated labels: [label_len: u16][label: utf8]
    ///     [weight: f32][key_len: u16][key: utf8][props_len: u32][props: json utf8]
    fn batch_upsert_nodes_binary(&self, py: Python<'_>, buffer: &[u8]) -> PyResult<Vec<u64>> {
        let inputs = decode_node_batch_py(buffer)?;
        with_engine(self, py, move |eng| eng.batch_upsert_nodes(inputs))
    }

    /// Batch upsert edges from a packed binary buffer.
    ///
    /// Binary format (little-endian):
    ///   [count: u32]
    ///   per edge:
    ///     [from: u64][to: u64][label_len: u16][label: utf8][weight: f32]
    ///     [valid_from: i64][valid_to: i64][props_len: u32][props: json utf8]
    fn batch_upsert_edges_binary(&self, py: Python<'_>, buffer: &[u8]) -> PyResult<Vec<u64>> {
        let inputs = decode_edge_batch_py(buffer)?;
        with_engine(self, py, move |eng| eng.batch_upsert_edges(inputs))
    }

    // --- Retention ---

    #[pyo3(signature = (*, max_age_ms=None, max_weight=None, label=None))]
    fn prune(
        &self,
        py: Python<'_>,
        max_age_ms: Option<i64>,
        max_weight: Option<f64>,
        label: Option<String>,
    ) -> PyResult<PruneResult> {
        let policy = PrunePolicy {
            max_age_ms,
            max_weight: max_weight.map(|v| v as f32),
            label,
        };
        with_engine(self, py, move |eng| {
            Ok(PruneResult::from(eng.prune(&policy)?))
        })
    }

    #[pyo3(signature = (name, *, max_age_ms=None, max_weight=None, label=None))]
    fn set_prune_policy(
        &self,
        py: Python<'_>,
        name: String,
        max_age_ms: Option<i64>,
        max_weight: Option<f64>,
        label: Option<String>,
    ) -> PyResult<()> {
        let policy = PrunePolicy {
            max_age_ms,
            max_weight: max_weight.map(|v| v as f32),
            label,
        };
        with_engine(self, py, move |eng| eng.set_prune_policy(&name, policy))
    }

    fn remove_prune_policy(&self, py: Python<'_>, name: String) -> PyResult<bool> {
        with_engine(self, py, move |eng| eng.remove_prune_policy(&name))
    }

    fn list_prune_policies(&self, py: Python<'_>) -> PyResult<Vec<NamedPrunePolicy>> {
        with_engine_ref(self, py, |eng| {
            Ok(eng
                .list_prune_policies()?
                .into_iter()
                .map(NamedPrunePolicy::from)
                .collect())
        })
    }

    // --- Maintenance ---

    fn sync(&self, py: Python<'_>) -> PyResult<()> {
        with_engine_ref(self, py, |eng| eng.sync())
    }

    fn flush(&self, py: Python<'_>) -> PyResult<Option<SegmentInfo>> {
        with_engine(self, py, |eng| {
            Ok(eng.flush()?.map(|si| SegmentInfo {
                id: si.id,
                node_count: si.node_count,
                edge_count: si.edge_count,
            }))
        })
    }

    fn ingest_mode(&self, py: Python<'_>) -> PyResult<()> {
        with_engine(self, py, |eng| eng.ingest_mode())
    }

    fn end_ingest(&self, py: Python<'_>) -> PyResult<Option<CompactionStats>> {
        with_engine(self, py, |eng| {
            Ok(eng.end_ingest()?.map(CompactionStats::from))
        })
    }

    fn compact(&self, py: Python<'_>) -> PyResult<Option<CompactionStats>> {
        with_engine(self, py, |eng| {
            Ok(eng.compact()?.map(CompactionStats::from))
        })
    }

    fn compact_with_progress(
        &self,
        py: Python<'_>,
        callback: PyObject,
    ) -> PyResult<Option<CompactionStats>> {
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
                        let py_progress = CompactionProgress::from(progress);
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
            Ok(result.map(CompactionStats::from))
        });
        // If cancellation was due to a Python error, re-raise that instead
        if let Some(py_err) = captured_err.lock().unwrap().take() {
            return Err(py_err);
        }
        engine_result
    }

    // --- Pagination ---

    #[pyo3(signature = (labels, *, limit=None, after=None))]
    fn nodes_by_labels_paged(
        &self,
        py: Python<'_>,
        labels: &Bound<'_, PyAny>,
        limit: Option<usize>,
        after: Option<u64>,
    ) -> PyResult<IdPageResult> {
        let labels = parse_node_labels_arg(labels, "nodes_by_labels_paged labels")?;
        let page = PageRequest { limit, after };
        with_engine_ref(self, py, move |eng| {
            Ok(IdPageResult::from(
                eng.nodes_by_labels_paged(labels, &page)?,
            ))
        })
    }

    #[pyo3(signature = (label, *, limit=None, after=None))]
    fn edges_by_label_paged(
        &self,
        py: Python<'_>,
        label: String,
        limit: Option<usize>,
        after: Option<u64>,
    ) -> PyResult<IdPageResult> {
        let page = PageRequest { limit, after };
        with_engine_ref(self, py, move |eng| {
            Ok(IdPageResult::from(eng.edges_by_label_paged(&label, &page)?))
        })
    }

    #[pyo3(signature = (labels, *, limit=None, after=None))]
    fn get_nodes_by_labels_paged(
        &self,
        py: Python<'_>,
        labels: &Bound<'_, PyAny>,
        limit: Option<usize>,
        after: Option<u64>,
    ) -> PyResult<NodePageResult> {
        let labels = parse_node_labels_arg(labels, "get_nodes_by_labels_paged labels")?;
        let page = PageRequest { limit, after };
        with_engine_ref(self, py, move |eng| {
            let result = eng.get_nodes_by_labels_paged(labels, &page)?;
            Ok(NodePageResult {
                items: result
                    .items
                    .into_iter()
                    .map(NodeView::try_from)
                    .collect::<Result<Vec<_>, EngineError>>()?,
                next_cursor: result.next_cursor,
            })
        })
    }

    #[pyo3(signature = (label, *, limit=None, after=None))]
    fn get_edges_by_label_paged(
        &self,
        py: Python<'_>,
        label: String,
        limit: Option<usize>,
        after: Option<u64>,
    ) -> PyResult<EdgePageResult> {
        let page = PageRequest { limit, after };
        with_engine_ref(self, py, move |eng| {
            let result = eng.get_edges_by_label_paged(&label, &page)?;
            Ok(EdgePageResult {
                items: result.items.into_iter().map(EdgeView::from).collect(),
                next_cursor: result.next_cursor,
            })
        })
    }

    #[pyo3(signature = (label, prop_key, prop_value, *, limit=None, after=None))]
    fn find_nodes_paged(
        &self,
        py: Python<'_>,
        label: String,
        prop_key: String,
        prop_value: PyObject,
        limit: Option<usize>,
        after: Option<u64>,
    ) -> PyResult<IdPageResult> {
        let pv = py_to_prop_value(py, prop_value.bind(py))?;
        let page = PageRequest { limit, after };
        with_engine_ref(self, py, move |eng| {
            Ok(IdPageResult::from(
                eng.find_nodes_paged(&label, &prop_key, &pv, &page)?,
            ))
        })
    }

    #[pyo3(signature = (label, from_ms, to_ms, *, limit=None, after=None))]
    fn find_nodes_by_time_range_paged(
        &self,
        py: Python<'_>,
        label: String,
        from_ms: i64,
        to_ms: i64,
        limit: Option<usize>,
        after: Option<u64>,
    ) -> PyResult<IdPageResult> {
        let page = PageRequest { limit, after };
        with_engine_ref(self, py, move |eng| {
            Ok(IdPageResult::from(eng.find_nodes_by_time_range_paged(
                &label, from_ms, to_ms, &page,
            )?))
        })
    }

    #[pyo3(signature = (label, prop_key, lower=None, upper=None, *, limit=None, after=None))]
    fn find_nodes_range_paged(
        &self,
        py: Python<'_>,
        label: String,
        prop_key: String,
        lower: Option<PropertyRangeBound>,
        upper: Option<PropertyRangeBound>,
        limit: Option<usize>,
        after: Option<PropertyRangeCursor>,
    ) -> PyResult<PropertyRangePageResult> {
        let lower = lower.map(CorePropertyRangeBound::from);
        let upper = upper.map(CorePropertyRangeBound::from);
        let page = PropertyRangePageRequest {
            limit,
            after: after.map(CorePropertyRangeCursor::from),
        };
        with_engine_ref(self, py, move |eng| {
            Ok(PropertyRangePageResult::from(eng.find_nodes_range_paged(
                &label,
                &prop_key,
                lower.as_ref(),
                upper.as_ref(),
                &page,
            )?))
        })
    }

    #[pyo3(signature = (node_id, *, direction="outgoing", edge_label_filter=None, limit=None, after=None, at_epoch=None, decay_lambda=None))]
    fn neighbors_paged(
        &self,
        py: Python<'_>,
        node_id: u64,
        direction: &str,
        edge_label_filter: Option<Vec<String>>,
        limit: Option<usize>,
        after: Option<u64>,
        at_epoch: Option<i64>,
        decay_lambda: Option<f64>,
    ) -> PyResult<NeighborPageResult> {
        let dir = parse_direction(direction)?;
        let dl = decay_lambda.map(|v| v as f32);
        let opts = NeighborOptions {
            direction: dir,
            edge_label_filter,
            limit: None,
            at_epoch,
            decay_lambda: dl,
        };
        let page = PageRequest { limit, after };
        with_engine_ref(self, py, move |eng| {
            let result = eng.neighbors_paged(node_id, &opts, &page)?;
            Ok(NeighborPageResult {
                items: result.items.into_iter().map(NeighborEntry::from).collect(),
                next_cursor: result.next_cursor,
            })
        })
    }

    // --- Analytics ---

    #[pyo3(signature = (seed_node_ids, *, algorithm=None, damping_factor=None, max_iterations=None, epsilon=None, approx_residual_tolerance=None, edge_label_filter=None, max_results=None))]
    fn personalized_pagerank(
        &self,
        py: Python<'_>,
        seed_node_ids: Vec<u64>,
        algorithm: Option<&str>,
        damping_factor: Option<f64>,
        max_iterations: Option<u32>,
        epsilon: Option<f64>,
        approx_residual_tolerance: Option<f64>,
        edge_label_filter: Option<Vec<String>>,
        max_results: Option<usize>,
    ) -> PyResult<PprResult> {
        let defaults = PprOptions::default();
        let options = PprOptions {
            algorithm: parse_ppr_algorithm(algorithm)?,
            damping_factor: damping_factor.unwrap_or(defaults.damping_factor),
            max_iterations: max_iterations.unwrap_or(defaults.max_iterations),
            epsilon: epsilon.unwrap_or(defaults.epsilon),
            approx_residual_tolerance: approx_residual_tolerance
                .unwrap_or(defaults.approx_residual_tolerance),
            edge_label_filter,
            max_results,
        };
        with_engine_ref(self, py, move |eng| {
            Ok(PprResult::from(
                eng.personalized_pagerank(&seed_node_ids, &options)?,
            ))
        })
    }

    #[pyo3(signature = (*, node_label_filter=None, edge_label_filter=None, include_weights=true))]
    fn export_adjacency(
        &self,
        py: Python<'_>,
        node_label_filter: Option<&Bound<'_, PyAny>>,
        edge_label_filter: Option<Vec<String>>,
        include_weights: bool,
    ) -> PyResult<AdjacencyExport> {
        let node_label_filter = parse_optional_node_label_filter_arg(
            node_label_filter,
            "export_adjacency node_label_filter",
        )?;
        let options = ExportOptions {
            node_label_filter,
            edge_label_filter,
            include_weights,
        };
        with_engine_ref(self, py, move |eng| {
            Ok(AdjacencyExport::from(eng.export_adjacency(&options)?))
        })
    }

    // --- Connected Components (Phase 18d) ---

    /// Weakly connected components over the visible graph.
    ///
    /// Returns a dict mapping each visible node ID to its component ID
    /// (the minimum node ID in that component). WCC treats all edges as
    /// undirected. Isolated nodes become singleton components.
    #[pyo3(signature = (*, edge_label_filter=None, node_label_filter=None, at_epoch=None))]
    fn connected_components(
        &self,
        py: Python<'_>,
        edge_label_filter: Option<Vec<String>>,
        node_label_filter: Option<&Bound<'_, PyAny>>,
        at_epoch: Option<i64>,
    ) -> PyResult<NodeIdMap<u64>> {
        let node_label_filter = parse_optional_node_label_filter_arg(
            node_label_filter,
            "connected_components node_label_filter",
        )?;
        let opts = ComponentOptions {
            edge_label_filter,
            node_label_filter,
            at_epoch,
        };
        with_engine_ref(self, py, move |eng| eng.connected_components(&opts))
    }

    /// Returns the sorted list of node IDs in the same weakly connected
    /// component as the given node. Returns an empty list if the node
    /// doesn't exist, is deleted, or is hidden by prune policy.
    #[pyo3(signature = (node_id, *, edge_label_filter=None, node_label_filter=None, at_epoch=None))]
    fn component_of(
        &self,
        py: Python<'_>,
        node_id: u64,
        edge_label_filter: Option<Vec<String>>,
        node_label_filter: Option<&Bound<'_, PyAny>>,
        at_epoch: Option<i64>,
    ) -> PyResult<Vec<u64>> {
        let node_label_filter = parse_optional_node_label_filter_arg(
            node_label_filter,
            "component_of node_label_filter",
        )?;
        let opts = ComponentOptions {
            edge_label_filter,
            node_label_filter,
            at_epoch,
        };
        with_engine_ref(self, py, move |eng| eng.component_of(node_id, &opts))
    }

    // --- Vector search (Phase 19) ---

    #[pyo3(signature = (mode, k, *, dense_query=None, sparse_query=None, label_filter=None, ef_search=None, scope_start_node_id=None, scope_max_depth=None, scope_direction=None, scope_edge_label_filter=None, scope_at_epoch=None, dense_weight=None, sparse_weight=None, fusion_mode=None))]
    fn vector_search(
        &self,
        py: Python<'_>,
        mode: &str,
        k: usize,
        dense_query: Option<Vec<f32>>,
        sparse_query: Option<Vec<(u32, f32)>>,
        label_filter: Option<&Bound<'_, PyAny>>,
        ef_search: Option<usize>,
        scope_start_node_id: Option<u64>,
        scope_max_depth: Option<u32>,
        scope_direction: Option<&str>,
        scope_edge_label_filter: Option<Vec<String>>,
        scope_at_epoch: Option<i64>,
        dense_weight: Option<f32>,
        sparse_weight: Option<f32>,
        fusion_mode: Option<&str>,
    ) -> PyResult<Vec<VectorHit>> {
        let mode = parse_vector_search_mode(mode)?;
        let label_filter =
            parse_optional_node_label_filter_arg(label_filter, "vector_search label_filter")?;
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
                edge_label_filter: scope_edge_label_filter,
                at_epoch: scope_at_epoch,
            }),
        };
        let request = VectorSearchRequest {
            mode,
            dense_query,
            sparse_query,
            k,
            label_filter,
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
                .map(|h| VectorHit {
                    node_id: h.node_id,
                    score: h.score as f64,
                })
                .collect())
        })
    }
}

#[pyclass]
pub struct WriteTxn {
    inner: Arc<Mutex<Option<CoreWriteTxn>>>,
}

#[pymethods]
impl WriteTxn {
    #[pyo3(signature = (labels, key, *, props=None, weight=1.0, dense_vector=None, sparse_vector=None))]
    fn upsert_node(
        &self,
        py: Python<'_>,
        labels: &Bound<'_, PyAny>,
        key: String,
        props: Option<&Bound<'_, PyDict>>,
        weight: f64,
        dense_vector: Option<Vec<f32>>,
        sparse_vector: Option<Vec<(u32, f32)>>,
    ) -> PyResult<PyObject> {
        let labels = parse_node_labels_arg(labels, "transaction upsert_node labels")?;
        let key_ref_label = labels.first().cloned().ok_or_else(|| {
            PyValueError::new_err("transaction upsert_node labels must not be empty")
        })?;
        let options = UpsertNodeOptions {
            props: convert_py_props(py, props)?,
            weight: weight as f32,
            dense_vector,
            sparse_vector,
        };
        with_py_txn(&self.inner, |txn| {
            txn.upsert_node(labels, &key, options).map_err(to_py_err)
        })?;
        txn_node_ref_to_py(
            py,
            TxnNodeRef::Key {
                label: key_ref_label,
                key,
            },
        )
    }

    #[pyo3(signature = (alias, labels, key, *, props=None, weight=1.0, dense_vector=None, sparse_vector=None))]
    fn upsert_node_as(
        &self,
        py: Python<'_>,
        alias: String,
        labels: &Bound<'_, PyAny>,
        key: String,
        props: Option<&Bound<'_, PyDict>>,
        weight: f64,
        dense_vector: Option<Vec<f32>>,
        sparse_vector: Option<Vec<(u32, f32)>>,
    ) -> PyResult<PyObject> {
        let labels = parse_node_labels_arg(labels, "transaction upsert_node_as labels")?;
        let options = UpsertNodeOptions {
            props: convert_py_props(py, props)?,
            weight: weight as f32,
            dense_vector,
            sparse_vector,
        };
        with_py_txn(&self.inner, |txn| {
            txn.upsert_node_as(&alias, labels, &key, options)
                .map_err(to_py_err)
                .and_then(|r| txn_node_ref_to_py(py, r))
        })
    }

    fn add_node_label(
        &self,
        _py: Python<'_>,
        target: &Bound<'_, PyDict>,
        label: String,
    ) -> PyResult<bool> {
        let target = parse_txn_node_ref(target)?;
        with_py_txn(&self.inner, |txn| {
            txn.add_node_label(target, &label).map_err(to_py_err)
        })
    }

    fn remove_node_label(
        &self,
        _py: Python<'_>,
        target: &Bound<'_, PyDict>,
        label: String,
    ) -> PyResult<bool> {
        let target = parse_txn_node_ref(target)?;
        with_py_txn(&self.inner, |txn| {
            txn.remove_node_label(target, &label).map_err(to_py_err)
        })
    }

    #[pyo3(signature = (from_ref, to_ref, label, *, props=None, weight=1.0, valid_from=None, valid_to=None))]
    fn upsert_edge(
        &self,
        py: Python<'_>,
        from_ref: &Bound<'_, PyDict>,
        to_ref: &Bound<'_, PyDict>,
        label: String,
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
            txn.upsert_edge(from.clone(), to.clone(), &label, options)
                .map_err(to_py_err)
        })?;
        txn_edge_ref_to_py(py, TxnEdgeRef::Triple { from, to, label })
    }

    #[pyo3(signature = (alias, from_ref, to_ref, label, *, props=None, weight=1.0, valid_from=None, valid_to=None))]
    fn upsert_edge_as(
        &self,
        py: Python<'_>,
        alias: String,
        from_ref: &Bound<'_, PyDict>,
        to_ref: &Bound<'_, PyDict>,
        label: String,
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
            txn.upsert_edge_as(&alias, from, to, &label, options)
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
        label: String,
        key: String,
    ) -> PyResult<Option<PyObject>> {
        with_py_txn_ref(&self.inner, |txn| {
            txn.get_node_by_key(&label, &key)
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
        label: String,
    ) -> PyResult<Option<PyObject>> {
        let from = parse_txn_node_ref(from_ref)?;
        let to = parse_txn_node_ref(to_ref)?;
        with_py_txn_ref(&self.inner, |txn| {
            txn.get_edge_by_triple(from, to, &label)
                .map_err(to_py_err)?
                .map(|v| txn_edge_view_to_py(py, v))
                .transpose()
        })
    }

    fn commit(&self, py: Python<'_>) -> PyResult<TxnCommitResult> {
        let mut txn = {
            let mut guard = self.inner.lock().map_err(lock_err)?;
            guard
                .take()
                .ok_or_else(|| OverGraphError::new_err(EngineError::TxnClosed.to_string()))?
        };
        let result = py.allow_threads(move || txn.commit()).map_err(to_py_err)?;
        Ok(TxnCommitResult::from(result))
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
pub struct DbStats {
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

impl From<CoreDbStats> for DbStats {
    fn from(s: CoreDbStats) -> Self {
        DbStats {
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
impl DbStats {
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
pub struct ScrubReport {
    #[pyo3(get)]
    pub total_components_checked: u64,
    #[pyo3(get)]
    pub total_components_ok: u64,
    #[pyo3(get)]
    pub total_components_failed: u64,
    #[pyo3(get)]
    pub total_bytes_digested: u64,
    #[pyo3(get)]
    pub duration_ms: u64,
    segments_internal: Vec<SegmentScrubResult>,
}

impl From<CoreScrubReport> for ScrubReport {
    fn from(r: CoreScrubReport) -> Self {
        ScrubReport {
            total_components_checked: r.total_components_checked,
            total_components_ok: r.total_components_ok,
            total_components_failed: r.total_components_failed,
            total_bytes_digested: r.total_bytes_digested,
            duration_ms: r.duration_ms,
            segments_internal: r
                .segments
                .into_iter()
                .map(SegmentScrubResult::from)
                .collect(),
        }
    }
}

#[pymethods]
impl ScrubReport {
    #[getter]
    fn segments(&self) -> Vec<SegmentScrubResult> {
        self.segments_internal.clone()
    }

    fn __repr__(&self) -> String {
        format!(
            "ScrubReport(segments={}, checked={}, ok={}, failed={}, duration_ms={})",
            self.segments_internal.len(),
            self.total_components_checked,
            self.total_components_ok,
            self.total_components_failed,
            self.duration_ms,
        )
    }
}

#[pyclass]
#[derive(Clone)]
pub struct SegmentScrubResult {
    #[pyo3(get)]
    pub segment_id: u64,
    #[pyo3(get)]
    pub components_ok: u64,
    #[pyo3(get)]
    pub bytes_digested: u64,
    findings_internal: Vec<ComponentScrubFinding>,
}

impl From<CoreSegmentScrubResult> for SegmentScrubResult {
    fn from(s: CoreSegmentScrubResult) -> Self {
        SegmentScrubResult {
            segment_id: s.segment_id,
            components_ok: s.components_ok,
            bytes_digested: s.bytes_digested,
            findings_internal: s
                .findings
                .into_iter()
                .map(ComponentScrubFinding::from)
                .collect(),
        }
    }
}

#[pymethods]
impl SegmentScrubResult {
    #[getter]
    fn findings(&self) -> Vec<ComponentScrubFinding> {
        self.findings_internal.clone()
    }

    fn __repr__(&self) -> String {
        format!(
            "SegmentScrubResult(segment_id={}, ok={}, findings={})",
            self.segment_id,
            self.components_ok,
            self.findings_internal.len(),
        )
    }
}

#[pyclass]
#[derive(Clone)]
pub struct ComponentScrubFinding {
    #[pyo3(get)]
    pub component_kind: String,
    #[pyo3(get)]
    pub finding_type: String,
    #[pyo3(get)]
    pub detail: String,
}

impl From<CoreComponentScrubFinding> for ComponentScrubFinding {
    fn from(f: CoreComponentScrubFinding) -> Self {
        ComponentScrubFinding {
            component_kind: f.component_kind,
            finding_type: format!("{:?}", f.finding_type),
            detail: f.detail,
        }
    }
}

#[pymethods]
impl ComponentScrubFinding {
    fn __repr__(&self) -> String {
        format!(
            "ScrubFinding(kind='{}', type='{}', detail='{}')",
            self.component_kind, self.finding_type, self.detail,
        )
    }
}

#[pyclass]
#[derive(Clone)]
pub struct NodeView {
    #[pyo3(get)]
    pub id: u64,
    #[pyo3(get)]
    pub labels: Vec<String>,
    #[pyo3(get)]
    pub key: String,
    #[pyo3(get)]
    pub created_at: i64,
    #[pyo3(get)]
    pub updated_at: i64,
    #[pyo3(get)]
    pub weight: f64,
    #[pyo3(get)]
    pub dense_vector: Option<Vec<f32>>,
    #[pyo3(get)]
    pub sparse_vector: Option<Vec<(u32, f32)>>,
    props_internal: BTreeMap<String, PropValue>,
}

impl TryFrom<CoreNodeView> for NodeView {
    type Error = EngineError;

    fn try_from(n: CoreNodeView) -> Result<Self, Self::Error> {
        Ok(NodeView {
            id: n.id,
            labels: n.labels,
            key: n.key,
            created_at: n.created_at,
            updated_at: n.updated_at,
            weight: n.weight as f64,
            dense_vector: n.dense_vector,
            sparse_vector: n.sparse_vector,
            props_internal: n.props,
        })
    }
}

#[pymethods]
impl NodeView {
    #[getter]
    fn props(&self, py: Python<'_>) -> PyResult<PyObject> {
        props_to_py(py, &self.props_internal)
    }

    fn __repr__(&self) -> String {
        format!(
            "NodeView(id={}, labels={:?}, key='{}')",
            self.id, self.labels, self.key
        )
    }
}

#[pyclass]
#[derive(Clone)]
pub struct EdgeView {
    #[pyo3(get)]
    pub id: u64,
    #[pyo3(get)]
    pub from_id: u64,
    #[pyo3(get)]
    pub to_id: u64,
    #[pyo3(get)]
    pub label: String,
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

impl From<CoreEdgeView> for EdgeView {
    fn from(e: CoreEdgeView) -> Self {
        EdgeView {
            id: e.id,
            from_id: e.from,
            to_id: e.to,
            label: e.label,
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
impl EdgeView {
    #[getter]
    fn props(&self, py: Python<'_>) -> PyResult<PyObject> {
        props_to_py(py, &self.props_internal)
    }

    fn __repr__(&self) -> String {
        format!(
            "EdgeView(id={}, {}->{}, label='{}')",
            self.id, self.from_id, self.to_id, self.label
        )
    }
}

#[pyclass]
#[derive(Clone)]
pub struct PatchResult {
    #[pyo3(get)]
    pub node_ids: Vec<u64>,
    #[pyo3(get)]
    pub edge_ids: Vec<u64>,
}

#[pymethods]
impl PatchResult {
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
pub struct TxnCommitResult {
    #[pyo3(get)]
    pub node_ids: Vec<u64>,
    #[pyo3(get)]
    pub edge_ids: Vec<u64>,
    #[pyo3(get)]
    pub node_aliases: HashMap<String, u64>,
    #[pyo3(get)]
    pub edge_aliases: HashMap<String, u64>,
}

impl From<CoreTxnCommitResult> for TxnCommitResult {
    fn from(result: CoreTxnCommitResult) -> Self {
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
        TxnCommitResult {
            node_ids: result.node_ids,
            edge_ids: result.edge_ids,
            node_aliases,
            edge_aliases,
        }
    }
}

#[pymethods]
impl TxnCommitResult {
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
pub struct NeighborEntry {
    #[pyo3(get)]
    pub node_id: u64,
    #[pyo3(get)]
    pub edge_id: u64,
    #[pyo3(get)]
    pub label: String,
    #[pyo3(get)]
    pub weight: f64,
    #[pyo3(get)]
    pub valid_from: i64,
    #[pyo3(get)]
    pub valid_to: i64,
}

impl From<CoreNeighborEntry> for NeighborEntry {
    fn from(n: CoreNeighborEntry) -> Self {
        NeighborEntry {
            node_id: n.node_id,
            edge_id: n.edge_id,
            label: n.label,
            weight: n.weight as f64,
            valid_from: n.valid_from,
            valid_to: n.valid_to,
        }
    }
}

#[pymethods]
impl NeighborEntry {
    fn __repr__(&self) -> String {
        format!(
            "NeighborEntry(node_id={}, edge_id={}, label='{}')",
            self.node_id, self.edge_id, self.label
        )
    }
}

#[pyclass]
#[derive(Clone)]
pub struct TraversalHit {
    #[pyo3(get)]
    pub node_id: u64,
    #[pyo3(get)]
    pub depth: u32,
    #[pyo3(get)]
    pub via_edge_id: Option<u64>,
    #[pyo3(get)]
    pub score: Option<f64>,
}

impl From<CoreTraversalHit> for TraversalHit {
    fn from(hit: CoreTraversalHit) -> Self {
        TraversalHit {
            node_id: hit.node_id,
            depth: hit.depth,
            via_edge_id: hit.via_edge_id,
            score: hit.score,
        }
    }
}

#[pymethods]
impl TraversalHit {
    fn __repr__(&self) -> String {
        format!(
            "TraversalHit(node_id={}, depth={}, via_edge_id={:?})",
            self.node_id, self.depth, self.via_edge_id
        )
    }
}

#[pyclass]
#[derive(Clone)]
pub struct VectorHit {
    #[pyo3(get)]
    pub node_id: u64,
    #[pyo3(get)]
    pub score: f64,
}

#[pymethods]
impl VectorHit {
    fn __repr__(&self) -> String {
        format!(
            "VectorHit(node_id={}, score={:.4})",
            self.node_id, self.score
        )
    }
}

#[pyclass]
#[derive(Clone)]
pub struct NodePropertyIndexInfo {
    #[pyo3(get)]
    pub index_id: u64,
    #[pyo3(get)]
    pub label: String,
    #[pyo3(get)]
    pub fields: Vec<BTreeMap<String, String>>,
    #[pyo3(get)]
    pub kind: String,
    #[pyo3(get)]
    pub state: String,
    #[pyo3(get)]
    pub last_error: Option<String>,
    #[pyo3(get)]
    pub compound: bool,
}

impl From<CoreNodePropertyIndexInfo> for NodePropertyIndexInfo {
    fn from(info: CoreNodePropertyIndexInfo) -> Self {
        let kind = secondary_index_kind_to_py(&info.kind);
        NodePropertyIndexInfo {
            index_id: info.index_id,
            label: info.label,
            fields: info
                .fields
                .into_iter()
                .map(secondary_index_field_to_py)
                .collect(),
            kind: kind.to_string(),
            state: secondary_index_state_to_py(info.state).to_string(),
            last_error: info.last_error,
            compound: info.compound,
        }
    }
}

#[pymethods]
impl NodePropertyIndexInfo {
    fn __repr__(&self) -> String {
        format!(
            "NodePropertyIndexInfo(index_id={}, label='{}', fields={:?}, kind='{}', state='{}', compound={})",
            self.index_id, self.label, self.fields, self.kind, self.state, self.compound
        )
    }
}

#[pyclass]
#[derive(Clone)]
pub struct NodeLabelInfo {
    #[pyo3(get)]
    pub label: String,
    #[pyo3(get)]
    pub label_id: u32,
}

impl From<CoreNodeLabelInfo> for NodeLabelInfo {
    fn from(info: CoreNodeLabelInfo) -> Self {
        NodeLabelInfo {
            label: info.label,
            label_id: info.label_id,
        }
    }
}

#[pymethods]
impl NodeLabelInfo {
    fn __repr__(&self) -> String {
        format!(
            "NodeLabelInfo(label='{}', label_id={})",
            self.label, self.label_id
        )
    }
}

#[pyclass]
#[derive(Clone)]
pub struct EdgeLabelInfo {
    #[pyo3(get)]
    pub label: String,
    #[pyo3(get)]
    pub label_id: u32,
}

impl From<CoreEdgeLabelInfo> for EdgeLabelInfo {
    fn from(info: CoreEdgeLabelInfo) -> Self {
        EdgeLabelInfo {
            label: info.label,
            label_id: info.label_id,
        }
    }
}

#[pymethods]
impl EdgeLabelInfo {
    fn __repr__(&self) -> String {
        format!(
            "EdgeLabelInfo(label='{}', label_id={})",
            self.label, self.label_id
        )
    }
}

#[pyclass]
#[derive(Clone)]
pub struct EdgePropertyIndexInfo {
    #[pyo3(get)]
    pub index_id: u64,
    #[pyo3(get)]
    pub label: String,
    #[pyo3(get)]
    pub fields: Vec<BTreeMap<String, String>>,
    #[pyo3(get)]
    pub kind: String,
    #[pyo3(get)]
    pub state: String,
    #[pyo3(get)]
    pub last_error: Option<String>,
    #[pyo3(get)]
    pub compound: bool,
}

impl From<CoreEdgePropertyIndexInfo> for EdgePropertyIndexInfo {
    fn from(info: CoreEdgePropertyIndexInfo) -> Self {
        let kind = secondary_index_kind_to_py(&info.kind);
        EdgePropertyIndexInfo {
            index_id: info.index_id,
            label: info.label,
            fields: info
                .fields
                .into_iter()
                .map(secondary_index_field_to_py)
                .collect(),
            kind: kind.to_string(),
            state: secondary_index_state_to_py(info.state).to_string(),
            last_error: info.last_error,
            compound: info.compound,
        }
    }
}

#[pymethods]
impl EdgePropertyIndexInfo {
    fn __repr__(&self) -> String {
        format!(
            "EdgePropertyIndexInfo(index_id={}, label='{}', fields={:?}, kind='{}', state='{}', compound={})",
            self.index_id, self.label, self.fields, self.kind, self.state, self.compound
        )
    }
}

#[pyclass]
#[derive(Clone)]
pub struct NodeSchemaInfo {
    #[pyo3(get)]
    pub label: String,
    schema_internal: CoreNodeSchema,
}

impl From<CoreNodeSchemaInfo> for NodeSchemaInfo {
    fn from(info: CoreNodeSchemaInfo) -> Self {
        NodeSchemaInfo {
            label: info.label,
            schema_internal: info.schema,
        }
    }
}

#[pymethods]
impl NodeSchemaInfo {
    #[getter]
    fn schema(&self, py: Python<'_>) -> PyResult<PyObject> {
        node_schema_to_py(py, &self.schema_internal)
    }

    fn __repr__(&self) -> String {
        format!("NodeSchemaInfo(label='{}')", self.label)
    }
}

#[pyclass]
#[derive(Clone)]
pub struct EdgeSchemaInfo {
    #[pyo3(get)]
    pub label: String,
    schema_internal: CoreEdgeSchema,
}

impl From<CoreEdgeSchemaInfo> for EdgeSchemaInfo {
    fn from(info: CoreEdgeSchemaInfo) -> Self {
        EdgeSchemaInfo {
            label: info.label,
            schema_internal: info.schema,
        }
    }
}

#[pymethods]
impl EdgeSchemaInfo {
    #[getter]
    fn schema(&self, py: Python<'_>) -> PyResult<PyObject> {
        edge_schema_to_py(py, &self.schema_internal)
    }

    fn __repr__(&self) -> String {
        format!("EdgeSchemaInfo(label='{}')", self.label)
    }
}

#[pyclass]
#[derive(Clone)]
pub struct SchemaValidationReport {
    #[pyo3(get)]
    pub checked_records: u64,
    #[pyo3(get)]
    pub violation_count: u64,
    #[pyo3(get)]
    pub truncated: bool,
    #[pyo3(get)]
    pub scan_limit_hit: bool,
    violations_internal: Vec<SchemaViolation>,
}

impl From<CoreSchemaValidationReport> for SchemaValidationReport {
    fn from(report: CoreSchemaValidationReport) -> Self {
        SchemaValidationReport {
            checked_records: report.checked_records,
            violation_count: report.violation_count,
            truncated: report.truncated,
            scan_limit_hit: report.scan_limit_hit,
            violations_internal: report
                .violations
                .into_iter()
                .map(SchemaViolation::from)
                .collect(),
        }
    }
}

#[pymethods]
impl SchemaValidationReport {
    #[getter]
    fn violations(&self) -> Vec<SchemaViolation> {
        self.violations_internal.clone()
    }

    fn __repr__(&self) -> String {
        format!(
            "SchemaValidationReport(checked_records={}, violation_count={}, truncated={}, scan_limit_hit={})",
            self.checked_records, self.violation_count, self.truncated, self.scan_limit_hit
        )
    }
}

#[pyclass]
#[derive(Clone)]
pub struct GraphSchemaValidationReportEntry {
    #[pyo3(get)]
    pub target_kind: String,
    #[pyo3(get)]
    pub label: String,
    #[pyo3(get)]
    pub report: SchemaValidationReport,
}

impl From<CoreGraphSchemaValidationReportEntry> for GraphSchemaValidationReportEntry {
    fn from(entry: CoreGraphSchemaValidationReportEntry) -> Self {
        GraphSchemaValidationReportEntry {
            target_kind: schema_target_kind_to_py(entry.target_kind).to_string(),
            label: entry.label,
            report: SchemaValidationReport::from(entry.report),
        }
    }
}

#[pymethods]
impl GraphSchemaValidationReportEntry {
    fn __repr__(&self) -> String {
        format!(
            "GraphSchemaValidationReportEntry(target_kind='{}', label='{}')",
            self.target_kind, self.label
        )
    }
}

#[pyclass]
#[derive(Clone)]
pub struct GraphSchemaCheckReport {
    #[pyo3(get)]
    pub operation: String,
    #[pyo3(get)]
    pub checked_records: u64,
    #[pyo3(get)]
    pub violation_count: u64,
    #[pyo3(get)]
    pub truncated: bool,
    #[pyo3(get)]
    pub scan_limit_hit: bool,
    entries_internal: Vec<GraphSchemaValidationReportEntry>,
}

impl From<CoreGraphSchemaCheckReport> for GraphSchemaCheckReport {
    fn from(report: CoreGraphSchemaCheckReport) -> Self {
        GraphSchemaCheckReport {
            operation: graph_schema_operation_kind_to_py(report.operation).to_string(),
            checked_records: report.checked_records,
            violation_count: report.violation_count,
            truncated: report.truncated,
            scan_limit_hit: report.scan_limit_hit,
            entries_internal: report
                .entries
                .into_iter()
                .map(GraphSchemaValidationReportEntry::from)
                .collect(),
        }
    }
}

#[pymethods]
impl GraphSchemaCheckReport {
    #[getter]
    fn entries(&self) -> Vec<GraphSchemaValidationReportEntry> {
        self.entries_internal.clone()
    }

    fn __repr__(&self) -> String {
        format!(
            "GraphSchemaCheckReport(operation='{}', entries={}, checked_records={}, violation_count={})",
            self.operation,
            self.entries_internal.len(),
            self.checked_records,
            self.violation_count
        )
    }
}

#[pyclass]
#[derive(Clone)]
pub struct GraphSchemaDropTargetResult {
    #[pyo3(get)]
    pub target_kind: String,
    #[pyo3(get)]
    pub label: String,
    #[pyo3(get)]
    pub action: String,
}

impl From<CoreGraphSchemaDropTargetResult> for GraphSchemaDropTargetResult {
    fn from(target: CoreGraphSchemaDropTargetResult) -> Self {
        GraphSchemaDropTargetResult {
            target_kind: schema_target_kind_to_py(target.target_kind).to_string(),
            label: target.label,
            action: graph_schema_drop_action_to_py(target.action).to_string(),
        }
    }
}

#[pymethods]
impl GraphSchemaDropTargetResult {
    fn __repr__(&self) -> String {
        format!(
            "GraphSchemaDropTargetResult(target_kind='{}', label='{}', action='{}')",
            self.target_kind, self.label, self.action
        )
    }
}

#[pyclass]
#[derive(Clone)]
pub struct GraphSchemaPublishResult {
    #[pyo3(get)]
    pub operation: String,
    #[pyo3(get)]
    pub validation: GraphSchemaCheckReport,
    #[pyo3(get)]
    pub targets_published: u64,
    #[pyo3(get)]
    pub targets_dropped: u64,
    #[pyo3(get)]
    pub node_schemas_dropped: u64,
    #[pyo3(get)]
    pub edge_schemas_dropped: u64,
    node_schemas_internal: Vec<NodeSchemaInfo>,
    edge_schemas_internal: Vec<EdgeSchemaInfo>,
    drop_targets_internal: Vec<GraphSchemaDropTargetResult>,
}

impl From<CoreGraphSchemaPublishResult> for GraphSchemaPublishResult {
    fn from(result: CoreGraphSchemaPublishResult) -> Self {
        GraphSchemaPublishResult {
            operation: graph_schema_operation_kind_to_py(result.operation).to_string(),
            node_schemas_internal: result
                .node_schemas
                .into_iter()
                .map(NodeSchemaInfo::from)
                .collect(),
            edge_schemas_internal: result
                .edge_schemas
                .into_iter()
                .map(EdgeSchemaInfo::from)
                .collect(),
            validation: GraphSchemaCheckReport::from(result.validation),
            targets_published: result.targets_published as u64,
            targets_dropped: result.targets_dropped as u64,
            drop_targets_internal: result
                .drop_targets
                .into_iter()
                .map(GraphSchemaDropTargetResult::from)
                .collect(),
            node_schemas_dropped: result.node_schemas_dropped as u64,
            edge_schemas_dropped: result.edge_schemas_dropped as u64,
        }
    }
}

#[pymethods]
impl GraphSchemaPublishResult {
    #[getter]
    fn node_schemas(&self) -> Vec<NodeSchemaInfo> {
        self.node_schemas_internal.clone()
    }

    #[getter]
    fn edge_schemas(&self) -> Vec<EdgeSchemaInfo> {
        self.edge_schemas_internal.clone()
    }

    #[getter]
    fn drop_targets(&self) -> Vec<GraphSchemaDropTargetResult> {
        self.drop_targets_internal.clone()
    }

    fn __repr__(&self) -> String {
        format!(
            "GraphSchemaPublishResult(operation='{}', targets_published={}, targets_dropped={})",
            self.operation, self.targets_published, self.targets_dropped
        )
    }
}

#[pyclass]
#[derive(Clone)]
pub struct SchemaViolation {
    target_internal: CoreSchemaViolationTarget,
    #[pyo3(get)]
    pub path: String,
    #[pyo3(get)]
    pub message: String,
}

impl From<CoreSchemaViolation> for SchemaViolation {
    fn from(violation: CoreSchemaViolation) -> Self {
        SchemaViolation {
            target_internal: violation.target,
            path: violation.path,
            message: violation.message,
        }
    }
}

#[pymethods]
impl SchemaViolation {
    #[getter]
    fn target(&self, py: Python<'_>) -> PyResult<PyObject> {
        schema_violation_target_to_py(py, &self.target_internal)
    }

    fn __repr__(&self) -> String {
        format!(
            "SchemaViolation(path='{}', message='{}')",
            self.path, self.message
        )
    }
}

#[pyclass]
#[derive(Clone)]
pub struct PropertyRangeBound {
    value_internal: PropValue,
    #[pyo3(get)]
    pub inclusive: bool,
}

impl From<PropertyRangeBound> for CorePropertyRangeBound {
    fn from(bound: PropertyRangeBound) -> Self {
        if bound.inclusive {
            CorePropertyRangeBound::Included(bound.value_internal)
        } else {
            CorePropertyRangeBound::Excluded(bound.value_internal)
        }
    }
}

impl From<CorePropertyRangeBound> for PropertyRangeBound {
    fn from(bound: CorePropertyRangeBound) -> Self {
        match bound {
            CorePropertyRangeBound::Included(value_internal) => PropertyRangeBound {
                value_internal,
                inclusive: true,
            },
            CorePropertyRangeBound::Excluded(value_internal) => PropertyRangeBound {
                value_internal,
                inclusive: false,
            },
        }
    }
}

#[pymethods]
impl PropertyRangeBound {
    #[new]
    #[pyo3(signature = (value, *, inclusive=true, domain))]
    fn new(value: &Bound<'_, pyo3::PyAny>, inclusive: bool, domain: &str) -> PyResult<Self> {
        let domain = parse_range_value_domain(domain)?;
        Ok(PropertyRangeBound {
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
        Ok(range_value_domain_to_py(range_value_domain_from_prop_value(
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
pub struct PropertyRangeCursor {
    value_internal: PropValue,
    #[pyo3(get)]
    pub node_id: u64,
}

impl From<PropertyRangeCursor> for CorePropertyRangeCursor {
    fn from(cursor: PropertyRangeCursor) -> Self {
        CorePropertyRangeCursor {
            value: cursor.value_internal,
            node_id: cursor.node_id,
        }
    }
}

impl From<CorePropertyRangeCursor> for PropertyRangeCursor {
    fn from(cursor: CorePropertyRangeCursor) -> Self {
        PropertyRangeCursor {
            value_internal: cursor.value,
            node_id: cursor.node_id,
        }
    }
}

#[pymethods]
impl PropertyRangeCursor {
    #[new]
    #[pyo3(signature = (value, node_id, *, domain))]
    fn new(value: &Bound<'_, pyo3::PyAny>, node_id: u64, domain: &str) -> PyResult<Self> {
        let domain = parse_range_value_domain(domain)?;
        Ok(PropertyRangeCursor {
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
        Ok(range_value_domain_to_py(range_value_domain_from_prop_value(
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
pub struct TraversalCursor {
    #[pyo3(get)]
    pub depth: u32,
    #[pyo3(get)]
    pub last_node_id: u64,
}

impl From<TraversalCursor> for CoreTraversalCursor {
    fn from(cursor: TraversalCursor) -> Self {
        CoreTraversalCursor {
            depth: cursor.depth,
            last_node_id: cursor.last_node_id,
        }
    }
}

impl From<CoreTraversalCursor> for TraversalCursor {
    fn from(cursor: CoreTraversalCursor) -> Self {
        TraversalCursor {
            depth: cursor.depth,
            last_node_id: cursor.last_node_id,
        }
    }
}

#[pymethods]
impl TraversalCursor {
    #[new]
    fn new(depth: u32, last_node_id: u64) -> Self {
        TraversalCursor {
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
pub struct ShortestPath {
    #[pyo3(get)]
    pub nodes: Vec<u64>,
    #[pyo3(get)]
    pub edges: Vec<u64>,
    #[pyo3(get)]
    pub total_cost: f64,
}

impl From<CoreShortestPath> for ShortestPath {
    fn from(sp: CoreShortestPath) -> Self {
        ShortestPath {
            nodes: sp.nodes,
            edges: sp.edges,
            total_cost: sp.total_cost,
        }
    }
}

#[pymethods]
impl ShortestPath {
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
pub struct Subgraph {
    nodes: Vec<NodeView>,
    edges: Vec<EdgeView>,
}

impl TryFrom<CoreSubgraph> for Subgraph {
    type Error = EngineError;

    fn try_from(sg: CoreSubgraph) -> Result<Self, Self::Error> {
        Ok(Subgraph {
            nodes: sg
                .nodes
                .into_iter()
                .map(NodeView::try_from)
                .collect::<Result<Vec<_>, EngineError>>()?,
            edges: sg.edges.into_iter().map(EdgeView::from).collect(),
        })
    }
}

#[pymethods]
impl Subgraph {
    #[getter]
    fn nodes(&self) -> Vec<NodeView> {
        self.nodes.clone()
    }
    #[getter]
    fn edges(&self) -> Vec<EdgeView> {
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
pub struct PruneResult {
    #[pyo3(get)]
    pub nodes_pruned: u64,
    #[pyo3(get)]
    pub edges_pruned: u64,
}

impl From<CorePruneResult> for PruneResult {
    fn from(r: CorePruneResult) -> Self {
        PruneResult {
            nodes_pruned: r.nodes_pruned,
            edges_pruned: r.edges_pruned,
        }
    }
}

#[pymethods]
impl PruneResult {
    fn __repr__(&self) -> String {
        format!(
            "PruneResult(nodes={}, edges={})",
            self.nodes_pruned, self.edges_pruned
        )
    }
}

#[pyclass]
#[derive(Clone)]
pub struct NamedPrunePolicy {
    #[pyo3(get)]
    pub name: String,
    #[pyo3(get)]
    pub max_age_ms: Option<i64>,
    #[pyo3(get)]
    pub max_weight: Option<f64>,
    #[pyo3(get)]
    pub label: Option<String>,
}

impl From<PrunePolicyInfo> for NamedPrunePolicy {
    fn from(info: PrunePolicyInfo) -> Self {
        NamedPrunePolicy {
            name: info.name,
            max_age_ms: info.policy.max_age_ms,
            max_weight: info.policy.max_weight.map(|w| w as f64),
            label: info.policy.label,
        }
    }
}

#[pymethods]
impl NamedPrunePolicy {
    fn __repr__(&self) -> String {
        format!(
            "PrunePolicy(name='{}', max_age_ms={:?}, max_weight={:?}, label={:?})",
            self.name, self.max_age_ms, self.max_weight, self.label
        )
    }
}

#[pyclass]
#[derive(Clone)]
pub struct SegmentInfo {
    #[pyo3(get)]
    pub id: u64,
    #[pyo3(get)]
    pub node_count: u64,
    #[pyo3(get)]
    pub edge_count: u64,
}

#[pymethods]
impl SegmentInfo {
    fn __repr__(&self) -> String {
        format!(
            "SegmentInfo(id={}, nodes={}, edges={})",
            self.id, self.node_count, self.edge_count
        )
    }
}

#[pyclass]
#[derive(Clone)]
pub struct CompactionStats {
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

impl From<CoreCompactionStats> for CompactionStats {
    fn from(s: CoreCompactionStats) -> Self {
        CompactionStats {
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
impl CompactionStats {
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
pub struct CompactionProgress {
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

impl From<&CoreCompactionProgress> for CompactionProgress {
    fn from(p: &CoreCompactionProgress) -> Self {
        let phase_str = match p.phase {
            CompactionPhase::CollectingTombstones => "collecting_tombstones",
            CompactionPhase::MergingNodes => "merging_nodes",
            CompactionPhase::MergingEdges => "merging_edges",
            CompactionPhase::WritingOutput => "writing_output",
        }
        .to_string();
        CompactionProgress {
            phase: phase_str,
            segments_processed: p.segments_processed,
            total_segments: p.total_segments,
            records_processed: p.records_processed,
            total_records: p.total_records,
        }
    }
}

#[pymethods]
impl CompactionProgress {
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

    fn __iter__(slf: PyRef<'_, Self>) -> IdArrayIter {
        IdArrayIter {
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
pub struct IdArrayIter {
    ids: Arc<Vec<u64>>,
    index: usize,
}

#[pymethods]
impl IdArrayIter {
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
pub struct IdPageResult {
    #[pyo3(get)]
    pub items: IdArray,
    #[pyo3(get)]
    pub next_cursor: Option<u64>,
}

impl From<eg::PageResult<u64>> for IdPageResult {
    fn from(r: eg::PageResult<u64>) -> Self {
        IdPageResult {
            items: IdArray {
                ids: Arc::new(r.items),
            },
            next_cursor: r.next_cursor,
        }
    }
}

#[pymethods]
impl IdPageResult {
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
pub struct NodePageResult {
    items: Vec<NodeView>,
    #[pyo3(get)]
    pub next_cursor: Option<u64>,
}

#[pymethods]
impl NodePageResult {
    #[getter]
    fn items(&self) -> Vec<NodeView> {
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
pub struct EdgePageResult {
    items: Vec<EdgeView>,
    #[pyo3(get)]
    pub next_cursor: Option<u64>,
}

#[pymethods]
impl EdgePageResult {
    #[getter]
    fn items(&self) -> Vec<EdgeView> {
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
pub struct NeighborPageResult {
    items: Vec<NeighborEntry>,
    #[pyo3(get)]
    pub next_cursor: Option<u64>,
}

#[pymethods]
impl NeighborPageResult {
    #[getter]
    fn items(&self) -> Vec<NeighborEntry> {
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
pub struct PropertyRangePageResult {
    #[pyo3(get)]
    pub items: IdArray,
    next_cursor: Option<PropertyRangeCursor>,
}

impl From<CorePropertyRangePageResult<u64>> for PropertyRangePageResult {
    fn from(result: CorePropertyRangePageResult<u64>) -> Self {
        PropertyRangePageResult {
            items: IdArray {
                ids: Arc::new(result.items),
            },
            next_cursor: result.next_cursor.map(PropertyRangeCursor::from),
        }
    }
}

#[pymethods]
impl PropertyRangePageResult {
    #[getter]
    fn next_cursor(&self) -> Option<PropertyRangeCursor> {
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
pub struct TraversalPageResult {
    items: Vec<TraversalHit>,
    next_cursor: Option<TraversalCursor>,
}

impl From<CoreTraversalPageResult> for TraversalPageResult {
    fn from(result: CoreTraversalPageResult) -> Self {
        TraversalPageResult {
            items: result.items.into_iter().map(TraversalHit::from).collect(),
            next_cursor: result.next_cursor.map(TraversalCursor::from),
        }
    }
}

#[pymethods]
impl TraversalPageResult {
    #[getter]
    fn items(&self) -> Vec<TraversalHit> {
        self.items.clone()
    }

    #[getter]
    fn next_cursor(&self) -> Option<TraversalCursor> {
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
pub struct PprResult {
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
    pub approx: Option<PprApproxMeta>,
}

#[pyclass]
#[derive(Clone)]
pub struct PprApproxMeta {
    #[pyo3(get)]
    pub residual_tolerance: f64,
    #[pyo3(get)]
    pub pushes: u64,
    #[pyo3(get)]
    pub max_remaining_residual: f64,
}

#[pymethods]
impl PprApproxMeta {
    fn __repr__(&self) -> String {
        format!(
            "PprApproxMeta(residual_tolerance={}, pushes={}, max_remaining_residual={})",
            self.residual_tolerance, self.pushes, self.max_remaining_residual
        )
    }
}

impl From<CorePprResult> for PprResult {
    fn from(r: CorePprResult) -> Self {
        let (node_ids, scores): (Vec<u64>, Vec<f64>) = r.scores.into_iter().unzip();
        PprResult {
            node_ids,
            scores,
            iterations: r.iterations,
            converged: r.converged,
            algorithm: ppr_algorithm_to_py(r.algorithm).to_string(),
            approx: r.approx.map(|a| PprApproxMeta {
                residual_tolerance: a.residual_tolerance,
                pushes: a.pushes,
                max_remaining_residual: a.max_remaining_residual,
            }),
        }
    }
}

#[pymethods]
impl PprResult {
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
pub struct ExportEdge {
    #[pyo3(get)]
    pub from_id: u64,
    #[pyo3(get)]
    pub to_id: u64,
    #[pyo3(get)]
    pub edge_label_index: u32,
    #[pyo3(get)]
    pub weight: Option<f64>,
}

#[pymethods]
impl ExportEdge {
    fn __repr__(&self) -> String {
        format!(
            "ExportEdge(from={}, to={}, edge_label_index={}, weight={:?})",
            self.from_id, self.to_id, self.edge_label_index, self.weight
        )
    }
}

#[pyclass]
#[derive(Clone)]
pub struct AdjacencyExport {
    #[pyo3(get)]
    pub node_ids: Vec<u64>,
    #[pyo3(get)]
    pub node_labels: Vec<String>,
    #[pyo3(get)]
    pub node_label_indexes: Vec<Vec<u32>>,
    #[pyo3(get)]
    pub edge_labels: Vec<String>,
    edges: Vec<ExportEdge>,
}

impl From<CoreAdjacencyExport> for AdjacencyExport {
    fn from(a: CoreAdjacencyExport) -> Self {
        AdjacencyExport {
            node_ids: a.node_ids,
            node_labels: a.node_labels,
            node_label_indexes: a.node_label_indexes,
            edge_labels: a.edge_labels,
            edges: a
                .edges
                .into_iter()
                .map(|edge| ExportEdge {
                    from_id: edge.from,
                    to_id: edge.to,
                    edge_label_index: edge.edge_label_index,
                    weight: edge.weight.map(|w| w as f64),
                })
                .collect(),
        }
    }
}

#[pymethods]
impl AdjacencyExport {
    #[getter]
    fn edges(&self) -> Vec<ExportEdge> {
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

fn parse_py_graph_schema(py: Python<'_>, dict: &Bound<'_, PyDict>) -> PyResult<CoreGraphSchema> {
    ensure_only_py_fields(dict, &["node_schemas", "edge_schemas"], "graph schema")?;
    Ok(CoreGraphSchema {
        node_schemas: match py_non_none_item(dict, "node_schemas")? {
            Some(value) => parse_py_node_schema_infos(py, &value, "graph schema node_schemas")?,
            None => Vec::new(),
        },
        edge_schemas: match py_non_none_item(dict, "edge_schemas")? {
            Some(value) => parse_py_edge_schema_infos(py, &value, "graph schema edge_schemas")?,
            None => Vec::new(),
        },
    })
}

fn parse_py_node_schema_infos(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    context: &str,
) -> PyResult<Vec<CoreNodeSchemaInfo>> {
    if let Ok(list) = value.downcast::<PyList>() {
        let mut infos = Vec::with_capacity(list.len());
        for (index, item) in list.iter().enumerate() {
            infos.push(parse_py_node_schema_info(
                py,
                &item,
                &format!("{context}[{index}]"),
            )?);
        }
        return Ok(infos);
    }
    if let Ok(tuple) = value.downcast::<PyTuple>() {
        let mut infos = Vec::with_capacity(tuple.len());
        for (index, item) in tuple.iter().enumerate() {
            infos.push(parse_py_node_schema_info(
                py,
                &item,
                &format!("{context}[{index}]"),
            )?);
        }
        return Ok(infos);
    }
    Err(PyTypeError::new_err(format!(
        "{context} must be a list or tuple"
    )))
}

fn parse_py_node_schema_info(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    context: &str,
) -> PyResult<CoreNodeSchemaInfo> {
    let dict = value.downcast::<PyDict>().map_err(|_| {
        PyTypeError::new_err(format!("{context} must be a mapping with label and schema"))
    })?;
    ensure_only_py_fields(dict, &["label", "schema"], context)?;
    let label: String = py_required_extract(dict, "label")?;
    let schema_value = py_non_none_item(dict, "schema")?
        .ok_or_else(|| PyValueError::new_err(format!("{context} schema is required")))?;
    let schema = parse_py_node_schema(
        py,
        schema_value
            .downcast::<PyDict>()
            .map_err(|_| PyTypeError::new_err(format!("{context} schema must be a mapping")))?,
    )?;
    Ok(CoreNodeSchemaInfo { label, schema })
}

fn parse_py_edge_schema_infos(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    context: &str,
) -> PyResult<Vec<CoreEdgeSchemaInfo>> {
    if let Ok(list) = value.downcast::<PyList>() {
        let mut infos = Vec::with_capacity(list.len());
        for (index, item) in list.iter().enumerate() {
            infos.push(parse_py_edge_schema_info(
                py,
                &item,
                &format!("{context}[{index}]"),
            )?);
        }
        return Ok(infos);
    }
    if let Ok(tuple) = value.downcast::<PyTuple>() {
        let mut infos = Vec::with_capacity(tuple.len());
        for (index, item) in tuple.iter().enumerate() {
            infos.push(parse_py_edge_schema_info(
                py,
                &item,
                &format!("{context}[{index}]"),
            )?);
        }
        return Ok(infos);
    }
    Err(PyTypeError::new_err(format!(
        "{context} must be a list or tuple"
    )))
}

fn parse_py_edge_schema_info(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    context: &str,
) -> PyResult<CoreEdgeSchemaInfo> {
    let dict = value.downcast::<PyDict>().map_err(|_| {
        PyTypeError::new_err(format!("{context} must be a mapping with label and schema"))
    })?;
    ensure_only_py_fields(dict, &["label", "schema"], context)?;
    let label: String = py_required_extract(dict, "label")?;
    let schema_value = py_non_none_item(dict, "schema")?
        .ok_or_else(|| PyValueError::new_err(format!("{context} schema is required")))?;
    let schema = parse_py_edge_schema(
        py,
        schema_value
            .downcast::<PyDict>()
            .map_err(|_| PyTypeError::new_err(format!("{context} schema must be a mapping")))?,
    )?;
    Ok(CoreEdgeSchemaInfo { label, schema })
}

fn parse_py_graph_schema_operations(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
) -> PyResult<Vec<CoreGraphSchemaOperation>> {
    if let Ok(list) = value.downcast::<PyList>() {
        let mut operations = Vec::with_capacity(list.len());
        for (index, item) in list.iter().enumerate() {
            operations.push(parse_py_graph_schema_operation(
                py,
                &item,
                &format!("graph schema operations[{index}]"),
            )?);
        }
        return Ok(operations);
    }
    if let Ok(tuple) = value.downcast::<PyTuple>() {
        let mut operations = Vec::with_capacity(tuple.len());
        for (index, item) in tuple.iter().enumerate() {
            operations.push(parse_py_graph_schema_operation(
                py,
                &item,
                &format!("graph schema operations[{index}]"),
            )?);
        }
        return Ok(operations);
    }
    Err(PyTypeError::new_err(
        "graph schema operations must be a list or tuple",
    ))
}

fn parse_py_graph_schema_operation(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    context: &str,
) -> PyResult<CoreGraphSchemaOperation> {
    let dict = value
        .downcast::<PyDict>()
        .map_err(|_| PyTypeError::new_err(format!("{context} must be a mapping")))?;
    ensure_only_py_fields(dict, &["kind", "label", "schema"], context)?;
    let kind: String = py_required_extract(dict, "kind")?;
    let label: String = py_required_extract(dict, "label")?;
    match kind.as_str() {
        "set_node" => {
            let schema_value = py_non_none_item(dict, "schema")?
                .ok_or_else(|| PyValueError::new_err(format!("{context} schema is required")))?;
            let schema = parse_py_node_schema(
                py,
                schema_value.downcast::<PyDict>().map_err(|_| {
                    PyTypeError::new_err(format!("{context} schema must be a mapping"))
                })?,
            )?;
            Ok(CoreGraphSchemaOperation::SetNode { label, schema })
        }
        "set_edge" => {
            let schema_value = py_non_none_item(dict, "schema")?
                .ok_or_else(|| PyValueError::new_err(format!("{context} schema is required")))?;
            let schema = parse_py_edge_schema(
                py,
                schema_value.downcast::<PyDict>().map_err(|_| {
                    PyTypeError::new_err(format!("{context} schema must be a mapping"))
                })?,
            )?;
            Ok(CoreGraphSchemaOperation::SetEdge { label, schema })
        }
        "drop_node" => {
            reject_py_graph_schema_operation_schema(dict, context)?;
            Ok(CoreGraphSchemaOperation::DropNode { label })
        }
        "drop_edge" => {
            reject_py_graph_schema_operation_schema(dict, context)?;
            Ok(CoreGraphSchemaOperation::DropEdge { label })
        }
        other => Err(PyValueError::new_err(format!(
            "{context} kind must be set_node, set_edge, drop_node, or drop_edge, got '{other}'"
        ))),
    }
}

fn reject_py_graph_schema_operation_schema(
    dict: &Bound<'_, PyDict>,
    context: &str,
) -> PyResult<()> {
    if py_non_none_item(dict, "schema")?.is_some() {
        return Err(PyValueError::new_err(format!(
            "{context} schema is only accepted for set_node and set_edge operations"
        )));
    }
    Ok(())
}

fn parse_py_node_schema(py: Python<'_>, dict: &Bound<'_, PyDict>) -> PyResult<CoreNodeSchema> {
    ensure_only_py_fields(
        dict,
        &[
            "additional_properties",
            "properties",
            "key",
            "label_constraints",
            "weight",
            "dense_vector",
            "sparse_vector",
        ],
        "node schema",
    )?;
    Ok(CoreNodeSchema {
        additional_properties: parse_py_additional_properties(
            py_optional_extract::<String>(dict, "additional_properties")?
                .as_deref()
                .unwrap_or("allow"),
        )?,
        properties: parse_py_schema_properties(py, dict, "node schema")?,
        key: parse_optional_py_schema_dict(dict, "key", "node schema key", |value| {
            parse_py_string_field_schema(value, "node schema key")
        })?,
        label_constraints: parse_optional_py_schema_dict(
            dict,
            "label_constraints",
            "node schema label_constraints",
            |value| parse_py_node_label_constraints(value, "node schema label_constraints"),
        )?,
        weight: parse_optional_py_schema_dict(dict, "weight", "node schema weight", |value| {
            parse_py_numeric_field_schema(py, value, "node schema weight")
        })?,
        dense_vector: parse_optional_py_schema_dict(
            dict,
            "dense_vector",
            "node schema dense_vector",
            |value| parse_py_dense_vector_schema(value, "node schema dense_vector"),
        )?,
        sparse_vector: parse_optional_py_schema_dict(
            dict,
            "sparse_vector",
            "node schema sparse_vector",
            |value| parse_py_sparse_vector_schema(value, "node schema sparse_vector"),
        )?,
    })
}

fn parse_py_edge_schema(py: Python<'_>, dict: &Bound<'_, PyDict>) -> PyResult<CoreEdgeSchema> {
    ensure_only_py_fields(
        dict,
        &[
            "additional_properties",
            "properties",
            "from",
            "to",
            "allow_self_loops",
            "weight",
            "validity",
        ],
        "edge schema",
    )?;
    Ok(CoreEdgeSchema {
        additional_properties: parse_py_additional_properties(
            py_optional_extract::<String>(dict, "additional_properties")?
                .as_deref()
                .unwrap_or("allow"),
        )?,
        properties: parse_py_schema_properties(py, dict, "edge schema")?,
        from: parse_optional_py_schema_dict(dict, "from", "edge schema from", |value| {
            parse_py_endpoint_label_schema(value, "edge schema from")
        })?,
        to: parse_optional_py_schema_dict(dict, "to", "edge schema to", |value| {
            parse_py_endpoint_label_schema(value, "edge schema to")
        })?,
        allow_self_loops: py_optional_extract::<bool>(dict, "allow_self_loops")?.unwrap_or(true),
        weight: parse_optional_py_schema_dict(dict, "weight", "edge schema weight", |value| {
            parse_py_numeric_field_schema(py, value, "edge schema weight")
        })?,
        validity: parse_optional_py_schema_dict(
            dict,
            "validity",
            "edge schema validity",
            |value| parse_py_edge_validity_schema(value, "edge schema validity"),
        )?,
    })
}

fn parse_py_schema_properties(
    py: Python<'_>,
    dict: &Bound<'_, PyDict>,
    context: &str,
) -> PyResult<BTreeMap<String, CorePropertySchema>> {
    let Some(value) = py_non_none_item(dict, "properties")? else {
        return Ok(BTreeMap::new());
    };
    let props = value.downcast::<PyDict>().map_err(|_| {
        PyTypeError::new_err(format!(
            "{context} properties must be a mapping of property schemas"
        ))
    })?;
    let mut parsed = BTreeMap::new();
    for (key, value) in props.iter() {
        let prop_key: String = key.extract()?;
        let schema = value.downcast::<PyDict>().map_err(|_| {
            PyTypeError::new_err(format!(
                "{context} properties['{prop_key}'] must be a mapping"
            ))
        })?;
        parsed.insert(
            prop_key.clone(),
            parse_py_property_schema(py, schema, &format!("{context} properties['{prop_key}']"))?,
        );
    }
    Ok(parsed)
}

fn parse_py_property_schema(
    py: Python<'_>,
    dict: &Bound<'_, PyDict>,
    context: &str,
) -> PyResult<CorePropertySchema> {
    ensure_only_py_fields(
        dict,
        &[
            "required",
            "nullable",
            "types",
            "numeric_min",
            "numeric_max",
            "string_min_bytes",
            "string_max_bytes",
            "bytes_min_len",
            "bytes_max_len",
            "array_min_items",
            "array_max_items",
            "map_min_entries",
            "map_max_entries",
            "enum_values",
        ],
        context,
    )?;
    let enum_values = match py_non_none_item(dict, "enum_values")? {
        Some(value) => {
            let values = value.downcast::<PyList>().map_err(|_| {
                PyTypeError::new_err(format!("{context} enum_values must be a list"))
            })?;
            values
                .iter()
                .map(|item| py_to_schema_literal_value(py, &item))
                .collect::<PyResult<Vec<_>>>()?
        }
        None => Vec::new(),
    };
    Ok(CorePropertySchema {
        required: py_optional_extract::<bool>(dict, "required")?.unwrap_or(false),
        nullable: py_optional_extract::<bool>(dict, "nullable")?.unwrap_or(true),
        types: parse_py_schema_value_types(dict, "types", context)?,
        numeric_min: parse_optional_py_schema_dict(
            dict,
            "numeric_min",
            &format!("{context} numeric_min"),
            |value| parse_py_schema_numeric_bound(py, value, &format!("{context} numeric_min")),
        )?,
        numeric_max: parse_optional_py_schema_dict(
            dict,
            "numeric_max",
            &format!("{context} numeric_max"),
            |value| parse_py_schema_numeric_bound(py, value, &format!("{context} numeric_max")),
        )?,
        string_min_bytes: py_optional_extract(dict, "string_min_bytes")?,
        string_max_bytes: py_optional_extract(dict, "string_max_bytes")?,
        bytes_min_len: py_optional_extract(dict, "bytes_min_len")?,
        bytes_max_len: py_optional_extract(dict, "bytes_max_len")?,
        array_min_items: py_optional_extract(dict, "array_min_items")?,
        array_max_items: py_optional_extract(dict, "array_max_items")?,
        map_min_entries: py_optional_extract(dict, "map_min_entries")?,
        map_max_entries: py_optional_extract(dict, "map_max_entries")?,
        enum_values,
    })
}

fn parse_py_string_field_schema(
    dict: &Bound<'_, PyDict>,
    context: &str,
) -> PyResult<CoreStringFieldSchema> {
    ensure_only_py_fields(dict, &["min_bytes", "max_bytes", "enum_values"], context)?;
    Ok(CoreStringFieldSchema {
        min_bytes: py_optional_extract(dict, "min_bytes")?,
        max_bytes: py_optional_extract(dict, "max_bytes")?,
        enum_values: py_optional_extract::<Vec<String>>(dict, "enum_values")?.unwrap_or_default(),
    })
}

fn parse_py_numeric_field_schema(
    py: Python<'_>,
    dict: &Bound<'_, PyDict>,
    context: &str,
) -> PyResult<CoreNumericFieldSchema> {
    ensure_only_py_fields(dict, &["min", "max", "finite"], context)?;
    Ok(CoreNumericFieldSchema {
        min: parse_optional_py_schema_dict(dict, "min", &format!("{context} min"), |value| {
            parse_py_schema_numeric_bound(py, value, &format!("{context} min"))
        })?,
        max: parse_optional_py_schema_dict(dict, "max", &format!("{context} max"), |value| {
            parse_py_schema_numeric_bound(py, value, &format!("{context} max"))
        })?,
        finite: py_optional_extract::<bool>(dict, "finite")?.unwrap_or(true),
    })
}

fn parse_py_node_label_constraints(
    dict: &Bound<'_, PyDict>,
    context: &str,
) -> PyResult<CoreNodeLabelConstraintSchema> {
    ensure_only_py_fields(dict, &["all_of", "any_of", "none_of"], context)?;
    Ok(CoreNodeLabelConstraintSchema {
        all_of: py_optional_extract::<Vec<String>>(dict, "all_of")?.unwrap_or_default(),
        any_of: py_optional_extract::<Vec<String>>(dict, "any_of")?.unwrap_or_default(),
        none_of: py_optional_extract::<Vec<String>>(dict, "none_of")?.unwrap_or_default(),
    })
}

fn parse_py_endpoint_label_schema(
    dict: &Bound<'_, PyDict>,
    context: &str,
) -> PyResult<CoreEndpointLabelSchema> {
    ensure_only_py_fields(dict, &["all_of", "any_of", "none_of"], context)?;
    Ok(CoreEndpointLabelSchema {
        all_of: py_optional_extract::<Vec<String>>(dict, "all_of")?.unwrap_or_default(),
        any_of: py_optional_extract::<Vec<String>>(dict, "any_of")?.unwrap_or_default(),
        none_of: py_optional_extract::<Vec<String>>(dict, "none_of")?.unwrap_or_default(),
    })
}

fn parse_py_dense_vector_schema(
    dict: &Bound<'_, PyDict>,
    context: &str,
) -> PyResult<CoreDenseVectorSchema> {
    ensure_only_py_fields(dict, &["presence", "dimension"], context)?;
    Ok(CoreDenseVectorSchema {
        presence: parse_py_schema_vector_presence(
            py_optional_extract::<String>(dict, "presence")?
                .as_deref()
                .unwrap_or("optional"),
        )?,
        dimension: py_optional_extract(dict, "dimension")?,
    })
}

fn parse_py_sparse_vector_schema(
    dict: &Bound<'_, PyDict>,
    context: &str,
) -> PyResult<CoreSparseVectorSchema> {
    ensure_only_py_fields(
        dict,
        &["presence", "min_entries", "max_entries", "max_dimension_id"],
        context,
    )?;
    Ok(CoreSparseVectorSchema {
        presence: parse_py_schema_vector_presence(
            py_optional_extract::<String>(dict, "presence")?
                .as_deref()
                .unwrap_or("optional"),
        )?,
        min_entries: py_optional_extract(dict, "min_entries")?,
        max_entries: py_optional_extract(dict, "max_entries")?,
        max_dimension_id: py_optional_extract(dict, "max_dimension_id")?,
    })
}

fn parse_py_edge_validity_schema(
    dict: &Bound<'_, PyDict>,
    context: &str,
) -> PyResult<CoreEdgeValiditySchema> {
    ensure_only_py_fields(
        dict,
        &[
            "require_valid_from_before_valid_to",
            "valid_from_min",
            "valid_from_max",
            "valid_to_min",
            "valid_to_max",
            "allow_open_ended_valid_to",
        ],
        context,
    )?;
    Ok(CoreEdgeValiditySchema {
        require_valid_from_before_valid_to: py_optional_extract::<bool>(
            dict,
            "require_valid_from_before_valid_to",
        )?
        .unwrap_or(false),
        valid_from_min: py_optional_extract(dict, "valid_from_min")?,
        valid_from_max: py_optional_extract(dict, "valid_from_max")?,
        valid_to_min: py_optional_extract(dict, "valid_to_min")?,
        valid_to_max: py_optional_extract(dict, "valid_to_max")?,
        allow_open_ended_valid_to: py_optional_extract::<bool>(dict, "allow_open_ended_valid_to")?
            .unwrap_or(true),
    })
}

fn parse_optional_py_schema_dict<T, F>(
    dict: &Bound<'_, PyDict>,
    key: &str,
    context: &str,
    parse: F,
) -> PyResult<Option<T>>
where
    F: FnOnce(&Bound<'_, PyDict>) -> PyResult<T>,
{
    py_non_none_item(dict, key)?
        .map(|value| {
            value
                .downcast::<PyDict>()
                .map_err(|_| PyTypeError::new_err(format!("{context} must be a mapping")))
                .and_then(parse)
        })
        .transpose()
}

fn parse_py_schema_numeric_bound(
    py: Python<'_>,
    dict: &Bound<'_, PyDict>,
    context: &str,
) -> PyResult<CoreSchemaNumericBound> {
    ensure_only_py_fields(dict, &["value", "inclusive"], context)?;
    let value = dict
        .get_item("value")?
        .ok_or_else(|| PyValueError::new_err(format!("{context} value is required")))?;
    Ok(CoreSchemaNumericBound {
        value: py_to_schema_literal_value(py, &value)?,
        inclusive: py_optional_extract::<bool>(dict, "inclusive")?.unwrap_or(true),
    })
}

fn parse_py_additional_properties(value: &str) -> PyResult<SchemaAdditionalProperties> {
    match value {
        "allow" => Ok(SchemaAdditionalProperties::Allow),
        "reject" => Ok(SchemaAdditionalProperties::Reject),
        other => Err(PyValueError::new_err(format!(
            "additional_properties must be 'allow' or 'reject', got '{other}'"
        ))),
    }
}

fn additional_properties_to_py(value: SchemaAdditionalProperties) -> &'static str {
    match value {
        SchemaAdditionalProperties::Allow => "allow",
        SchemaAdditionalProperties::Reject => "reject",
    }
}

fn parse_py_schema_value_types(
    dict: &Bound<'_, PyDict>,
    key: &str,
    context: &str,
) -> PyResult<Vec<SchemaValueType>> {
    let Some(value) = py_non_none_item(dict, key)? else {
        return Ok(Vec::new());
    };
    let values = value
        .downcast::<PyList>()
        .map_err(|_| PyTypeError::new_err(format!("{context} {key} must be a list")))?;
    values
        .iter()
        .map(|item| parse_py_schema_value_type(&item.extract::<String>()?))
        .collect()
}

fn parse_py_schema_value_type(value: &str) -> PyResult<SchemaValueType> {
    match value {
        "bool" => Ok(SchemaValueType::Bool),
        "int" => Ok(SchemaValueType::Int),
        "uint" => Ok(SchemaValueType::UInt),
        "float" => Ok(SchemaValueType::Float),
        "number" => Ok(SchemaValueType::Number),
        "string" => Ok(SchemaValueType::String),
        "bytes" => Ok(SchemaValueType::Bytes),
        "array" => Ok(SchemaValueType::Array),
        "map" => Ok(SchemaValueType::Map),
        other => Err(PyValueError::new_err(format!(
            "schema value type must be one of bool, int, uint, float, number, string, bytes, array, map; got '{other}'"
        ))),
    }
}

fn schema_value_type_to_py(value: SchemaValueType) -> &'static str {
    match value {
        SchemaValueType::Bool => "bool",
        SchemaValueType::Int => "int",
        SchemaValueType::UInt => "uint",
        SchemaValueType::Float => "float",
        SchemaValueType::Number => "number",
        SchemaValueType::String => "string",
        SchemaValueType::Bytes => "bytes",
        SchemaValueType::Array => "array",
        SchemaValueType::Map => "map",
    }
}

fn parse_py_schema_vector_presence(value: &str) -> PyResult<SchemaVectorPresence> {
    match value {
        "optional" => Ok(SchemaVectorPresence::Optional),
        "required" => Ok(SchemaVectorPresence::Required),
        "forbidden" => Ok(SchemaVectorPresence::Forbidden),
        other => Err(PyValueError::new_err(format!(
            "vector presence must be 'optional', 'required', or 'forbidden', got '{other}'"
        ))),
    }
}

fn schema_vector_presence_to_py(value: SchemaVectorPresence) -> &'static str {
    match value {
        SchemaVectorPresence::Optional => "optional",
        SchemaVectorPresence::Required => "required",
        SchemaVectorPresence::Forbidden => "forbidden",
    }
}

#[allow(clippy::only_used_in_recursion)]
fn py_to_schema_literal_value(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<PropValue> {
    if obj.is_none() {
        Ok(PropValue::Null)
    } else if obj.is_instance_of::<PyBool>() {
        Ok(PropValue::Bool(obj.extract::<bool>()?))
    } else if let Ok(i) = obj.extract::<i64>() {
        Ok(PropValue::Int(i))
    } else if obj.get_type().name()? == "int" {
        Err(PyValueError::new_err(
            "schema integer literals must fit signed i64; use {'type': 'uint', 'value': ...} for unsigned u64 values",
        ))
    } else if let Ok(f) = obj.extract::<f64>() {
        if !f.is_finite() {
            return Err(PyValueError::new_err(
                "schema numeric literals must be finite",
            ));
        }
        Ok(PropValue::Float(f))
    } else if let Ok(s) = obj.extract::<String>() {
        Ok(PropValue::String(s))
    } else if let Ok(b) = obj.downcast::<PyBytes>() {
        Ok(PropValue::Bytes(b.as_bytes().to_vec()))
    } else if obj.is_instance_of::<PyByteArray>() {
        Ok(PropValue::Bytes(obj.extract::<Vec<u8>>()?))
    } else if let Ok(list) = obj.downcast::<PyList>() {
        let items: PyResult<Vec<PropValue>> = list
            .iter()
            .map(|item| py_to_schema_literal_value(py, &item))
            .collect();
        Ok(PropValue::Array(items?))
    } else if let Ok(tuple) = obj.downcast::<PyTuple>() {
        let items: PyResult<Vec<PropValue>> = tuple
            .iter()
            .map(|item| py_to_schema_literal_value(py, &item))
            .collect();
        Ok(PropValue::Array(items?))
    } else if let Ok(dict) = obj.downcast::<PyDict>() {
        if let Some(value) = parse_py_schema_uint_literal_marker(dict)? {
            return Ok(value);
        }
        let mut map = BTreeMap::new();
        for (k, v) in dict.iter() {
            let key: String = k.extract()?;
            map.insert(key, py_to_schema_literal_value(py, &v)?);
        }
        Ok(PropValue::Map(map))
    } else {
        Err(PyTypeError::new_err(format!(
            "Unsupported schema literal type: {}",
            obj.get_type().name()?
        )))
    }
}

fn parse_py_schema_uint_literal_marker(dict: &Bound<'_, PyDict>) -> PyResult<Option<PropValue>> {
    let Some(type_value) = dict.get_item("type")? else {
        return Ok(None);
    };
    let Ok(type_name) = type_value.extract::<String>() else {
        return Ok(None);
    };
    if type_name != "uint" {
        return Ok(None);
    }
    ensure_only_py_fields(dict, &["type", "value"], "schema uint literal")?;
    let value = dict
        .get_item("value")?
        .ok_or_else(|| PyValueError::new_err("schema uint literal value is required"))?;
    if value.is_instance_of::<PyBool>() {
        return Err(PyTypeError::new_err(
            "schema uint literal value must be int or str, not bool",
        ));
    }
    if let Ok(text) = value.extract::<String>() {
        return text
            .parse::<u64>()
            .map(PropValue::UInt)
            .map(Some)
            .map_err(|_| {
                PyValueError::new_err("schema uint literal string value must be a valid u64")
            });
    }
    value.extract::<u64>().map(PropValue::UInt).map(Some)
}

fn node_schema_to_py(py: Python<'_>, schema: &CoreNodeSchema) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    dict.set_item(
        "additional_properties",
        additional_properties_to_py(schema.additional_properties),
    )?;
    dict.set_item(
        "properties",
        schema_properties_to_py(py, &schema.properties)?,
    )?;
    set_optional_py_object(
        py,
        &dict,
        "key",
        schema
            .key
            .as_ref()
            .map(|value| string_field_schema_to_py(py, value)),
    )?;
    set_optional_py_object(
        py,
        &dict,
        "label_constraints",
        schema
            .label_constraints
            .as_ref()
            .map(|value| node_label_constraints_to_py(py, value)),
    )?;
    set_optional_py_object(
        py,
        &dict,
        "weight",
        schema
            .weight
            .as_ref()
            .map(|value| numeric_field_schema_to_py(py, value)),
    )?;
    set_optional_py_object(
        py,
        &dict,
        "dense_vector",
        schema
            .dense_vector
            .as_ref()
            .map(|value| dense_vector_schema_to_py(py, value)),
    )?;
    set_optional_py_object(
        py,
        &dict,
        "sparse_vector",
        schema
            .sparse_vector
            .as_ref()
            .map(|value| sparse_vector_schema_to_py(py, value)),
    )?;
    Ok(dict.into_any().unbind())
}

fn edge_schema_to_py(py: Python<'_>, schema: &CoreEdgeSchema) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    dict.set_item(
        "additional_properties",
        additional_properties_to_py(schema.additional_properties),
    )?;
    dict.set_item(
        "properties",
        schema_properties_to_py(py, &schema.properties)?,
    )?;
    set_optional_py_object(
        py,
        &dict,
        "from",
        schema
            .from
            .as_ref()
            .map(|value| endpoint_label_schema_to_py(py, value)),
    )?;
    set_optional_py_object(
        py,
        &dict,
        "to",
        schema
            .to
            .as_ref()
            .map(|value| endpoint_label_schema_to_py(py, value)),
    )?;
    dict.set_item("allow_self_loops", schema.allow_self_loops)?;
    set_optional_py_object(
        py,
        &dict,
        "weight",
        schema
            .weight
            .as_ref()
            .map(|value| numeric_field_schema_to_py(py, value)),
    )?;
    set_optional_py_object(
        py,
        &dict,
        "validity",
        schema
            .validity
            .as_ref()
            .map(|value| edge_validity_schema_to_py(py, value)),
    )?;
    Ok(dict.into_any().unbind())
}

fn schema_properties_to_py(
    py: Python<'_>,
    properties: &BTreeMap<String, CorePropertySchema>,
) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    for (key, value) in properties {
        dict.set_item(key, property_schema_to_py(py, value)?)?;
    }
    Ok(dict.into_any().unbind())
}

fn property_schema_to_py(py: Python<'_>, schema: &CorePropertySchema) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    dict.set_item("required", schema.required)?;
    dict.set_item("nullable", schema.nullable)?;
    dict.set_item(
        "types",
        schema
            .types
            .iter()
            .map(|value| schema_value_type_to_py(*value))
            .collect::<Vec<_>>(),
    )?;
    set_optional_py_object(
        py,
        &dict,
        "numeric_min",
        schema
            .numeric_min
            .as_ref()
            .map(|value| schema_numeric_bound_to_py(py, value)),
    )?;
    set_optional_py_object(
        py,
        &dict,
        "numeric_max",
        schema
            .numeric_max
            .as_ref()
            .map(|value| schema_numeric_bound_to_py(py, value)),
    )?;
    dict.set_item("string_min_bytes", schema.string_min_bytes)?;
    dict.set_item("string_max_bytes", schema.string_max_bytes)?;
    dict.set_item("bytes_min_len", schema.bytes_min_len)?;
    dict.set_item("bytes_max_len", schema.bytes_max_len)?;
    dict.set_item("array_min_items", schema.array_min_items)?;
    dict.set_item("array_max_items", schema.array_max_items)?;
    dict.set_item("map_min_entries", schema.map_min_entries)?;
    dict.set_item("map_max_entries", schema.map_max_entries)?;
    let enum_values: PyResult<Vec<PyObject>> = schema
        .enum_values
        .iter()
        .map(|value| schema_literal_to_py_obj(py, value))
        .collect();
    dict.set_item("enum_values", PyList::new(py, enum_values?)?)?;
    Ok(dict.into_any().unbind())
}

fn schema_numeric_bound_to_py(
    py: Python<'_>,
    bound: &CoreSchemaNumericBound,
) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    dict.set_item("value", schema_literal_to_py_obj(py, &bound.value)?)?;
    dict.set_item("inclusive", bound.inclusive)?;
    Ok(dict.into_any().unbind())
}

fn string_field_schema_to_py(py: Python<'_>, schema: &CoreStringFieldSchema) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    dict.set_item("min_bytes", schema.min_bytes)?;
    dict.set_item("max_bytes", schema.max_bytes)?;
    dict.set_item("enum_values", schema.enum_values.clone())?;
    Ok(dict.into_any().unbind())
}

fn numeric_field_schema_to_py(
    py: Python<'_>,
    schema: &CoreNumericFieldSchema,
) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    set_optional_py_object(
        py,
        &dict,
        "min",
        schema
            .min
            .as_ref()
            .map(|value| schema_numeric_bound_to_py(py, value)),
    )?;
    set_optional_py_object(
        py,
        &dict,
        "max",
        schema
            .max
            .as_ref()
            .map(|value| schema_numeric_bound_to_py(py, value)),
    )?;
    dict.set_item("finite", schema.finite)?;
    Ok(dict.into_any().unbind())
}

fn node_label_constraints_to_py(
    py: Python<'_>,
    schema: &CoreNodeLabelConstraintSchema,
) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    dict.set_item("all_of", schema.all_of.clone())?;
    dict.set_item("any_of", schema.any_of.clone())?;
    dict.set_item("none_of", schema.none_of.clone())?;
    Ok(dict.into_any().unbind())
}

fn endpoint_label_schema_to_py(
    py: Python<'_>,
    schema: &CoreEndpointLabelSchema,
) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    dict.set_item("all_of", schema.all_of.clone())?;
    dict.set_item("any_of", schema.any_of.clone())?;
    dict.set_item("none_of", schema.none_of.clone())?;
    Ok(dict.into_any().unbind())
}

fn dense_vector_schema_to_py(py: Python<'_>, schema: &CoreDenseVectorSchema) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    dict.set_item("presence", schema_vector_presence_to_py(schema.presence))?;
    dict.set_item("dimension", schema.dimension)?;
    Ok(dict.into_any().unbind())
}

fn sparse_vector_schema_to_py(
    py: Python<'_>,
    schema: &CoreSparseVectorSchema,
) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    dict.set_item("presence", schema_vector_presence_to_py(schema.presence))?;
    dict.set_item("min_entries", schema.min_entries)?;
    dict.set_item("max_entries", schema.max_entries)?;
    dict.set_item("max_dimension_id", schema.max_dimension_id)?;
    Ok(dict.into_any().unbind())
}

fn edge_validity_schema_to_py(
    py: Python<'_>,
    schema: &CoreEdgeValiditySchema,
) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    dict.set_item(
        "require_valid_from_before_valid_to",
        schema.require_valid_from_before_valid_to,
    )?;
    dict.set_item("valid_from_min", schema.valid_from_min)?;
    dict.set_item("valid_from_max", schema.valid_from_max)?;
    dict.set_item("valid_to_min", schema.valid_to_min)?;
    dict.set_item("valid_to_max", schema.valid_to_max)?;
    dict.set_item(
        "allow_open_ended_valid_to",
        schema.allow_open_ended_valid_to,
    )?;
    Ok(dict.into_any().unbind())
}

fn set_optional_py_object(
    py: Python<'_>,
    dict: &Bound<'_, PyDict>,
    key: &str,
    value: Option<PyResult<PyObject>>,
) -> PyResult<()> {
    match value {
        Some(value) => dict.set_item(key, value?)?,
        None => dict.set_item(key, py.None())?,
    }
    Ok(())
}

fn schema_literal_to_py_obj(py: Python<'_>, value: &PropValue) -> PyResult<PyObject> {
    match value {
        PropValue::Null => Ok(py.None()),
        PropValue::Bool(value) => Ok(value.into_pyobject(py)?.to_owned().into_any().unbind()),
        PropValue::Int(value) => Ok(value.into_pyobject(py)?.into_any().unbind()),
        PropValue::UInt(value) => {
            let dict = PyDict::new(py);
            dict.set_item("type", "uint")?;
            dict.set_item("value", *value)?;
            Ok(dict.into_any().unbind())
        }
        PropValue::Float(value) => Ok(value.into_pyobject(py)?.into_any().unbind()),
        PropValue::String(value) => Ok(value.into_pyobject(py)?.into_any().unbind()),
        PropValue::Bytes(value) => Ok(PyBytes::new(py, value).into_any().unbind()),
        PropValue::Array(values) => {
            let items: PyResult<Vec<PyObject>> = values
                .iter()
                .map(|item| schema_literal_to_py_obj(py, item))
                .collect();
            Ok(PyList::new(py, items?)?.into_any().unbind())
        }
        PropValue::Map(values) => {
            let dict = PyDict::new(py);
            for (key, value) in values {
                dict.set_item(key, schema_literal_to_py_obj(py, value)?)?;
            }
            Ok(dict.into_any().unbind())
        }
    }
}

fn schema_violation_target_to_py(
    py: Python<'_>,
    target: &CoreSchemaViolationTarget,
) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    match target {
        CoreSchemaViolationTarget::Node { id, labels, key } => {
            dict.set_item("kind", "node")?;
            dict.set_item("id", *id)?;
            dict.set_item("labels", labels.clone())?;
            dict.set_item("key", key)?;
        }
        CoreSchemaViolationTarget::Edge {
            id,
            label,
            from,
            to,
        } => {
            dict.set_item("kind", "edge")?;
            dict.set_item("id", *id)?;
            dict.set_item("label", label)?;
            dict.set_item("from_id", *from)?;
            dict.set_item("to_id", *to)?;
        }
    }
    Ok(dict.into_any().unbind())
}

fn schema_target_kind_to_py(kind: CoreSchemaTargetKind) -> &'static str {
    match kind {
        CoreSchemaTargetKind::Node => "node",
        CoreSchemaTargetKind::Edge => "edge",
    }
}

fn graph_schema_operation_kind_to_py(kind: CoreGraphSchemaOperationKind) -> &'static str {
    match kind {
        CoreGraphSchemaOperationKind::Add => "add",
        CoreGraphSchemaOperationKind::Set => "set",
        CoreGraphSchemaOperationKind::Drop => "drop",
        CoreGraphSchemaOperationKind::DropAll => "drop_all",
        CoreGraphSchemaOperationKind::CheckAdd => "check_add",
        CoreGraphSchemaOperationKind::CheckSet => "check_set",
    }
}

fn graph_schema_drop_action_to_py(action: CoreGraphSchemaDropAction) -> &'static str {
    match action {
        CoreGraphSchemaDropAction::Dropped => "dropped",
        CoreGraphSchemaDropAction::NotFound => "not_found",
    }
}

fn parse_py_gql_options(
    mode: &str,
    allow_full_scan: bool,
    max_rows: Option<usize>,
    cursor: Option<String>,
    max_cursor_bytes: Option<usize>,
    max_mutation_rows: Option<usize>,
    max_mutation_ops: Option<usize>,
    max_pipeline_rows: Option<usize>,
    max_groups: Option<usize>,
    max_collect_items: Option<usize>,
    max_union_branches: Option<usize>,
    max_subquery_invocations: Option<usize>,
    max_subquery_depth: Option<usize>,
    max_shortest_path_pairs: Option<usize>,
    max_intermediate_bindings: Option<usize>,
    max_frontier: Option<usize>,
    max_path_hops: Option<u8>,
    max_paths_per_start: Option<usize>,
    max_order_materialization: Option<usize>,
    max_skip: Option<usize>,
    max_query_bytes: Option<usize>,
    max_param_bytes: Option<usize>,
    max_ast_depth: Option<usize>,
    max_literal_items: Option<usize>,
    include_plan: bool,
    profile: bool,
    compact_rows: bool,
    include_vectors: bool,
) -> PyResult<GqlExecutionOptions> {
    let mut options = GqlExecutionOptions {
        mode: parse_py_gql_execution_mode(mode)?,
        allow_full_scan,
        cursor,
        include_plan,
        profile,
        compact_rows,
        include_vectors,
        ..GqlExecutionOptions::default()
    };
    if let Some(max_rows) = max_rows {
        options.max_rows = max_rows;
    }
    if let Some(max_cursor_bytes) = max_cursor_bytes {
        options.max_cursor_bytes = max_cursor_bytes;
    }
    if let Some(max_mutation_rows) = max_mutation_rows {
        options.max_mutation_rows = max_mutation_rows;
    }
    if let Some(max_mutation_ops) = max_mutation_ops {
        options.max_mutation_ops = max_mutation_ops;
    }
    if let Some(max_pipeline_rows) = max_pipeline_rows {
        options.max_pipeline_rows = max_pipeline_rows;
    }
    if let Some(max_groups) = max_groups {
        options.max_groups = max_groups;
    }
    if let Some(max_collect_items) = max_collect_items {
        options.max_collect_items = max_collect_items;
    }
    if let Some(max_union_branches) = max_union_branches {
        options.max_union_branches = max_union_branches;
    }
    if let Some(max_subquery_invocations) = max_subquery_invocations {
        options.max_subquery_invocations = max_subquery_invocations;
    }
    if let Some(max_subquery_depth) = max_subquery_depth {
        options.max_subquery_depth = max_subquery_depth;
    }
    if let Some(max_shortest_path_pairs) = max_shortest_path_pairs {
        options.max_shortest_path_pairs = max_shortest_path_pairs;
    }
    if let Some(max_intermediate_bindings) = max_intermediate_bindings {
        options.max_intermediate_bindings = max_intermediate_bindings;
    }
    if let Some(max_frontier) = max_frontier {
        options.max_frontier = max_frontier;
    }
    if let Some(max_path_hops) = max_path_hops {
        options.max_path_hops = max_path_hops;
    }
    if let Some(max_paths_per_start) = max_paths_per_start {
        options.max_paths_per_start = max_paths_per_start;
    }
    if let Some(max_order_materialization) = max_order_materialization {
        options.max_order_materialization = max_order_materialization;
    }
    if let Some(max_skip) = max_skip {
        options.max_skip = max_skip;
    }
    if let Some(max_query_bytes) = max_query_bytes {
        options.max_query_bytes = max_query_bytes;
    }
    if let Some(max_param_bytes) = max_param_bytes {
        options.max_param_bytes = max_param_bytes;
    }
    if let Some(max_ast_depth) = max_ast_depth {
        options.max_ast_depth = max_ast_depth;
    }
    if let Some(max_literal_items) = max_literal_items {
        options.max_literal_items = max_literal_items;
    }
    Ok(options)
}

fn parse_py_gql_execution_mode(value: &str) -> PyResult<GqlExecutionMode> {
    match value {
        "auto" => Ok(GqlExecutionMode::Auto),
        "read_only" => Ok(GqlExecutionMode::ReadOnly),
        other => Err(OverGraphError::new_err(format!(
            "GQL mode must be 'auto' or 'read_only', got '{other}'"
        ))),
    }
}

struct GqlParamConversionBudget {
    total_items: usize,
    total_bytes: usize,
}

fn parse_py_gql_params(
    py: Python<'_>,
    params: Option<&Bound<'_, PyDict>>,
    referenced_params: &[String],
    options: &GqlExecutionOptions,
) -> PyResult<GqlParams> {
    let mut parsed = GqlParams::new();
    if referenced_params.is_empty() {
        return Ok(parsed);
    }
    let mut budget = GqlParamConversionBudget {
        total_items: 0,
        total_bytes: 0,
    };
    if let Some(params) = params {
        for key in referenced_params {
            if let Some(value) = params.get_item(key)? {
                parsed.insert(
                    key.clone(),
                    py_to_gql_param_value(py, key, &value, 0, options, &mut budget)?,
                );
            }
        }
    }
    Ok(parsed)
}

#[allow(clippy::only_used_in_recursion)]
fn py_to_gql_param_value(
    py: Python<'_>,
    name: &str,
    obj: &Bound<'_, PyAny>,
    container_depth: usize,
    options: &GqlExecutionOptions,
    budget: &mut GqlParamConversionBudget,
) -> PyResult<GqlParamValue> {
    if obj.is_none() {
        Ok(GqlParamValue::Null)
    } else if obj.is_instance_of::<PyBool>() {
        Ok(GqlParamValue::Bool(obj.extract::<bool>()?))
    } else if let Ok(i) = obj.extract::<i64>() {
        if i < 0 {
            Ok(GqlParamValue::Int(i))
        } else {
            Ok(GqlParamValue::UInt(i as u64))
        }
    } else if let Ok(u) = obj.extract::<u64>() {
        Ok(GqlParamValue::UInt(u))
    } else if let Ok(f) = obj.extract::<f64>() {
        if !f.is_finite() {
            return Err(PyValueError::new_err("GQL numeric params must be finite"));
        }
        Ok(GqlParamValue::Float(f))
    } else if let Ok(b) = obj.downcast::<PyBytes>() {
        add_py_param_bytes(name, b.as_bytes().len(), "bytes", budget, options)?;
        Ok(GqlParamValue::Bytes(b.as_bytes().to_vec()))
    } else if let Ok(s) = obj.downcast::<PyString>() {
        let value = s.to_str()?;
        add_py_param_bytes(name, value.len(), "string", budget, options)?;
        Ok(GqlParamValue::String(value.to_string()))
    } else if let Ok(list) = obj.downcast::<PyList>() {
        let depth = container_depth.saturating_add(1);
        check_py_param_depth(name, depth, options)?;
        add_py_param_items(name, list.len(), "list", budget, options)?;
        let items: PyResult<Vec<GqlParamValue>> = list
            .iter()
            .map(|item| py_to_gql_param_value(py, name, &item, depth, options, budget))
            .collect();
        Ok(GqlParamValue::List(items?))
    } else if let Ok(tuple) = obj.downcast::<PyTuple>() {
        let depth = container_depth.saturating_add(1);
        check_py_param_depth(name, depth, options)?;
        add_py_param_items(name, tuple.len(), "list", budget, options)?;
        let items: PyResult<Vec<GqlParamValue>> = tuple
            .iter()
            .map(|item| py_to_gql_param_value(py, name, &item, depth, options, budget))
            .collect();
        Ok(GqlParamValue::List(items?))
    } else if let Ok(dict) = obj.downcast::<PyDict>() {
        let depth = container_depth.saturating_add(1);
        check_py_param_depth(name, depth, options)?;
        add_py_param_items(name, dict.len(), "map", budget, options)?;
        let mut map = BTreeMap::new();
        for (k, v) in dict.iter() {
            let key = k.downcast::<PyString>().map_err(|_| {
                PyTypeError::new_err(format!("GQL parameter '${name}' map keys must be strings"))
            })?;
            let key = key.to_str()?;
            add_py_param_bytes(name, key.len(), "map key", budget, options)?;
            map.insert(
                key.to_string(),
                py_to_gql_param_value(py, name, &v, depth, options, budget)?,
            );
        }
        Ok(GqlParamValue::Map(map))
    } else {
        Err(PyTypeError::new_err(format!(
            "Unsupported GQL param value type: {}",
            obj.get_type().name()?
        )))
    }
}

fn check_py_param_depth(name: &str, depth: usize, options: &GqlExecutionOptions) -> PyResult<()> {
    if depth > options.max_ast_depth {
        return Err(PyValueError::new_err(format!(
            "GQL parameter '${name}' nested list/map depth exceeds max_ast_depth of {}",
            options.max_ast_depth
        )));
    }
    Ok(())
}

fn add_py_param_items(
    name: &str,
    count: usize,
    container_kind: &str,
    budget: &mut GqlParamConversionBudget,
    options: &GqlExecutionOptions,
) -> PyResult<()> {
    if count > options.max_literal_items {
        return Err(PyValueError::new_err(format!(
            "GQL parameter '${name}' {container_kind} contains {count} items, exceeding max_literal_items of {}",
            options.max_literal_items
        )));
    }
    budget.total_items = budget
        .total_items
        .checked_add(count)
        .filter(|total| *total <= options.max_literal_items)
        .ok_or_else(|| {
            PyValueError::new_err(format!(
                "Referenced GQL parameters contain more than max_literal_items={} total list/map items",
                options.max_literal_items
            ))
        })?;
    Ok(())
}

fn add_py_param_bytes(
    name: &str,
    bytes: usize,
    value_kind: &str,
    budget: &mut GqlParamConversionBudget,
    options: &GqlExecutionOptions,
) -> PyResult<()> {
    if bytes > options.max_param_bytes {
        return Err(PyValueError::new_err(format!(
            "GQL parameter '${name}' {value_kind} is {bytes} bytes, exceeding max_param_bytes of {}",
            options.max_param_bytes
        )));
    }
    budget.total_bytes = budget
        .total_bytes
        .checked_add(bytes)
        .filter(|total| *total <= options.max_param_bytes)
        .ok_or_else(|| {
            PyValueError::new_err(format!(
                "Referenced GQL parameters contain more than max_param_bytes={} total string/bytes/map-key bytes",
                options.max_param_bytes
            ))
        })?;
    Ok(())
}

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

fn gql_result_to_py(
    py: Python<'_>,
    result: GqlExecutionResult,
    compact_rows: bool,
) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    dict.set_item("kind", gql_statement_kind_to_py(result.kind))?;
    dict.set_item("columns", result.columns.clone())?;
    let rows: PyResult<Vec<PyObject>> = result
        .rows
        .into_iter()
        .map(|row| {
            if compact_rows {
                let values: PyResult<Vec<PyObject>> = row
                    .values
                    .into_iter()
                    .map(|value| gql_value_to_py(py, value))
                    .collect();
                Ok(PyList::new(py, values?)?.into_any().unbind())
            } else {
                let row_dict = PyDict::new(py);
                for (column, value) in result.columns.iter().zip(row.values) {
                    row_dict.set_item(column, gql_value_to_py(py, value)?)?;
                }
                Ok(row_dict.into_any().unbind())
            }
        })
        .collect();
    dict.set_item("rows", rows?)?;
    dict.set_item("next_cursor", result.next_cursor)?;
    dict.set_item("stats", gql_stats_to_py(py, result.stats)?)?;
    match result.mutation_stats {
        Some(stats) => dict.set_item("mutation_stats", gql_mutation_stats_to_py(py, stats)?)?,
        None => dict.set_item("mutation_stats", py.None())?,
    }
    match result.schema_stats {
        Some(stats) => dict.set_item("schema_stats", gql_schema_stats_to_py(py, stats)?)?,
        None => dict.set_item("schema_stats", py.None())?,
    }
    match result.index_stats {
        Some(stats) => dict.set_item("index_stats", gql_index_stats_to_py(py, stats)?)?,
        None => dict.set_item("index_stats", py.None())?,
    }
    match result.plan {
        Some(plan) => dict.set_item("plan", gql_explain_to_py(py, plan)?)?,
        None => dict.set_item("plan", py.None())?,
    }
    Ok(dict.into_any().unbind())
}

fn gql_value_to_py(py: Python<'_>, value: GqlValue) -> PyResult<PyObject> {
    match value {
        GqlValue::Null => Ok(py.None()),
        GqlValue::Bool(value) => Ok(value.into_pyobject(py)?.to_owned().into_any().unbind()),
        GqlValue::Int(value) => Ok(value.into_pyobject(py)?.into_any().unbind()),
        GqlValue::UInt(value) => Ok(value.into_pyobject(py)?.into_any().unbind()),
        GqlValue::Float(value) => Ok(value.into_pyobject(py)?.into_any().unbind()),
        GqlValue::String(value) => Ok(value.into_pyobject(py)?.into_any().unbind()),
        GqlValue::Bytes(value) => Ok(PyBytes::new(py, &value).into_any().unbind()),
        GqlValue::List(values) => {
            let items: PyResult<Vec<PyObject>> = values
                .into_iter()
                .map(|value| gql_value_to_py(py, value))
                .collect();
            Ok(PyList::new(py, items?)?.into_any().unbind())
        }
        GqlValue::Map(values) => {
            let dict = PyDict::new(py);
            for (key, value) in values {
                dict.set_item(key, gql_value_to_py(py, value)?)?;
            }
            Ok(dict.into_any().unbind())
        }
        GqlValue::Node(node) => gql_node_to_py(py, node),
        GqlValue::Edge(edge) => gql_edge_to_py(py, edge),
        GqlValue::Path(path) => gql_path_to_py(py, path),
    }
}

fn gql_node_to_py(py: Python<'_>, node: GqlNode) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    if let Some(id) = node.id {
        dict.set_item("id", id)?;
    }
    if let Some(labels) = node.labels {
        dict.set_item("labels", labels)?;
    }
    if let Some(key) = node.key {
        dict.set_item("key", key)?;
    }
    if let Some(props) = node.props {
        dict.set_item("props", gql_value_to_py(py, GqlValue::Map(props))?)?;
    }
    if let Some(weight) = node.weight {
        dict.set_item("weight", weight as f64)?;
    }
    if let Some(created_at) = node.created_at {
        dict.set_item("created_at", created_at)?;
    }
    if let Some(updated_at) = node.updated_at {
        dict.set_item("updated_at", updated_at)?;
    }
    if let Some(dense_vector) = node.dense_vector {
        dict.set_item(
            "dense_vector",
            dense_vector
                .into_iter()
                .map(|value| value as f64)
                .collect::<Vec<_>>(),
        )?;
    }
    if let Some(sparse_vector) = node.sparse_vector {
        dict.set_item(
            "sparse_vector",
            sparse_vector
                .into_iter()
                .map(|(dimension, value)| (dimension, value as f64))
                .collect::<Vec<_>>(),
        )?;
    }
    Ok(dict.into_any().unbind())
}

fn gql_path_to_py(py: Python<'_>, path: GqlPath) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    dict.set_item("node_ids", path.node_ids)?;
    dict.set_item("edge_ids", path.edge_ids)?;
    if let Some(nodes) = path.nodes {
        let values: PyResult<Vec<PyObject>> = nodes
            .into_iter()
            .map(|node| gql_node_to_py(py, node))
            .collect();
        dict.set_item("nodes", values?)?;
    }
    if let Some(edges) = path.edges {
        let values: PyResult<Vec<PyObject>> = edges
            .into_iter()
            .map(|edge| gql_edge_to_py(py, edge))
            .collect();
        dict.set_item("edges", values?)?;
    }
    Ok(dict.into_any().unbind())
}

fn graph_row_result_to_py(
    py: Python<'_>,
    result: GraphRowResult,
    compact_rows: bool,
) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    dict.set_item("columns", result.columns.clone())?;
    let rows: PyResult<Vec<PyObject>> = result
        .rows
        .into_iter()
        .map(|row| {
            if compact_rows {
                let values: PyResult<Vec<PyObject>> = row
                    .values
                    .into_iter()
                    .map(|value| graph_value_to_py(py, value))
                    .collect();
                Ok(PyList::new(py, values?)?.into_any().unbind())
            } else {
                let row_dict = PyDict::new(py);
                for (column, value) in result.columns.iter().zip(row.values) {
                    row_dict.set_item(column, graph_value_to_py(py, value)?)?;
                }
                Ok(row_dict.into_any().unbind())
            }
        })
        .collect();
    dict.set_item("rows", rows?)?;
    dict.set_item("next_cursor", result.next_cursor)?;
    dict.set_item("stats", graph_row_stats_to_py(py, result.stats)?)?;
    match result.plan {
        Some(plan) => dict.set_item("plan", graph_row_explain_to_py(py, plan)?)?,
        None => dict.set_item("plan", py.None())?,
    }
    Ok(dict.into_any().unbind())
}

fn graph_pipeline_result_to_py(
    py: Python<'_>,
    result: GraphPipelineResult,
    compact_rows: bool,
) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    dict.set_item("columns", result.columns.clone())?;
    let rows: PyResult<Vec<PyObject>> = result
        .rows
        .into_iter()
        .map(|row| {
            if compact_rows {
                let values: PyResult<Vec<PyObject>> = row
                    .values
                    .into_iter()
                    .map(|value| graph_value_to_py(py, value))
                    .collect();
                Ok(PyList::new(py, values?)?.into_any().unbind())
            } else {
                let row_dict = PyDict::new(py);
                for (column, value) in result.columns.iter().zip(row.values) {
                    row_dict.set_item(column, graph_value_to_py(py, value)?)?;
                }
                Ok(row_dict.into_any().unbind())
            }
        })
        .collect();
    dict.set_item("rows", rows?)?;
    dict.set_item("next_cursor", result.next_cursor)?;
    dict.set_item("stats", graph_pipeline_stats_to_py(py, result.stats)?)?;
    match result.plan {
        Some(plan) => dict.set_item("plan", graph_pipeline_explain_to_py(py, plan)?)?,
        None => dict.set_item("plan", py.None())?,
    }
    Ok(dict.into_any().unbind())
}

fn graph_value_to_py(py: Python<'_>, value: GraphValue) -> PyResult<PyObject> {
    match value {
        GraphValue::Null => Ok(py.None()),
        GraphValue::Bool(value) => Ok(value.into_pyobject(py)?.to_owned().into_any().unbind()),
        GraphValue::Int(value) => Ok(value.into_pyobject(py)?.into_any().unbind()),
        GraphValue::UInt(value) | GraphValue::NodeId(value) | GraphValue::EdgeId(value) => {
            Ok(value.into_pyobject(py)?.into_any().unbind())
        }
        GraphValue::Float(value) => Ok(value.into_pyobject(py)?.into_any().unbind()),
        GraphValue::String(value) => Ok(value.into_pyobject(py)?.into_any().unbind()),
        GraphValue::Bytes(value) => Ok(PyBytes::new(py, &value).into_any().unbind()),
        GraphValue::List(values) => {
            let items: PyResult<Vec<PyObject>> = values
                .into_iter()
                .map(|value| graph_value_to_py(py, value))
                .collect();
            Ok(PyList::new(py, items?)?.into_any().unbind())
        }
        GraphValue::Map(values) => {
            let dict = PyDict::new(py);
            for (key, value) in values {
                dict.set_item(key, graph_value_to_py(py, value)?)?;
            }
            Ok(dict.into_any().unbind())
        }
        GraphValue::Node(node) => graph_node_value_to_py(py, node),
        GraphValue::Edge(edge) => graph_edge_value_to_py(py, edge),
        GraphValue::Path(path) => graph_path_value_to_py(py, path),
    }
}

fn graph_node_value_to_py(py: Python<'_>, node: GraphNodeValue) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    if let Some(id) = node.id {
        dict.set_item("id", id)?;
    }
    if let Some(labels) = node.labels {
        dict.set_item("labels", labels)?;
    }
    if let Some(key) = node.key {
        dict.set_item("key", key)?;
    }
    if let Some(props) = node.props {
        let props_dict = PyDict::new(py);
        for (key, value) in props {
            props_dict.set_item(key, graph_value_to_py(py, value)?)?;
        }
        dict.set_item("props", props_dict)?;
    }
    if let Some(weight) = node.weight {
        dict.set_item("weight", weight as f64)?;
    }
    if let Some(created_at) = node.created_at {
        dict.set_item("created_at", created_at)?;
    }
    if let Some(updated_at) = node.updated_at {
        dict.set_item("updated_at", updated_at)?;
    }
    if let Some(dense_vector) = node.dense_vector {
        dict.set_item(
            "dense_vector",
            dense_vector
                .into_iter()
                .map(|value| value as f64)
                .collect::<Vec<_>>(),
        )?;
    }
    if let Some(sparse_vector) = node.sparse_vector {
        dict.set_item(
            "sparse_vector",
            sparse_vector
                .into_iter()
                .map(|(dimension, value)| (dimension, value as f64))
                .collect::<Vec<_>>(),
        )?;
    }
    Ok(dict.into_any().unbind())
}

fn graph_edge_value_to_py(py: Python<'_>, edge: GraphEdgeValue) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    if let Some(id) = edge.id {
        dict.set_item("id", id)?;
    }
    if let Some(from) = edge.from {
        dict.set_item("from_id", from)?;
    }
    if let Some(to) = edge.to {
        dict.set_item("to_id", to)?;
    }
    if let Some(label) = edge.label {
        dict.set_item("label", label)?;
    }
    if let Some(props) = edge.props {
        let props_dict = PyDict::new(py);
        for (key, value) in props {
            props_dict.set_item(key, graph_value_to_py(py, value)?)?;
        }
        dict.set_item("props", props_dict)?;
    }
    if let Some(weight) = edge.weight {
        dict.set_item("weight", weight as f64)?;
    }
    if let Some(created_at) = edge.created_at {
        dict.set_item("created_at", created_at)?;
    }
    if let Some(updated_at) = edge.updated_at {
        dict.set_item("updated_at", updated_at)?;
    }
    if let Some(valid_from) = edge.valid_from {
        dict.set_item("valid_from", valid_from)?;
    }
    if let Some(valid_to) = edge.valid_to {
        dict.set_item("valid_to", valid_to)?;
    }
    Ok(dict.into_any().unbind())
}

fn graph_path_value_to_py(py: Python<'_>, path: GraphPathValue) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    dict.set_item("node_ids", path.node_ids)?;
    dict.set_item("edge_ids", path.edge_ids)?;
    if let Some(nodes) = path.nodes {
        let values: PyResult<Vec<PyObject>> = nodes
            .into_iter()
            .map(|node| graph_node_value_to_py(py, node))
            .collect();
        dict.set_item("nodes", values?)?;
    }
    if let Some(edges) = path.edges {
        let values: PyResult<Vec<PyObject>> = edges
            .into_iter()
            .map(|edge| graph_edge_value_to_py(py, edge))
            .collect();
        dict.set_item("edges", values?)?;
    }
    Ok(dict.into_any().unbind())
}

fn graph_row_stats_to_py(py: Python<'_>, stats: GraphRowStats) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    dict.set_item("rows_returned", stats.rows_returned)?;
    dict.set_item("rows_after_filter", stats.rows_after_filter)?;
    dict.set_item("rows_seen_for_page", stats.rows_seen_for_page)?;
    dict.set_item(
        "intermediate_bindings_peak",
        stats.intermediate_bindings_peak,
    )?;
    dict.set_item("frontier_peak", stats.frontier_peak)?;
    dict.set_item("paths_enumerated", stats.paths_enumerated)?;
    dict.set_item("db_hits", stats.db_hits)?;
    dict.set_item("elapsed_us", stats.elapsed_us)?;
    dict.set_item("effective_at_epoch", stats.effective_at_epoch)?;
    dict.set_item("warnings", stats.warnings)?;
    Ok(dict.into_any().unbind())
}

fn graph_pipeline_stats_to_py(py: Python<'_>, stats: GraphPipelineStats) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    dict.set_item("rows_returned", stats.rows_returned)?;
    dict.set_item("rows_entered_pipeline", stats.rows_entered_pipeline)?;
    dict.set_item("rows_after_filter", stats.rows_after_filter)?;
    dict.set_item("intermediate_rows", stats.intermediate_rows)?;
    dict.set_item(
        "pipeline_rows_materialized",
        stats.pipeline_rows_materialized,
    )?;
    dict.set_item("groups", stats.groups)?;
    dict.set_item("collect_items", stats.collect_items)?;
    dict.set_item("union_branches", stats.union_branches)?;
    dict.set_item("union_dedup_keys", stats.union_dedup_keys)?;
    dict.set_item("subquery_invocations", stats.subquery_invocations)?;
    dict.set_item("subquery_cache_hits", stats.subquery_cache_hits)?;
    dict.set_item("shortest_path_pairs", stats.shortest_path_pairs)?;
    dict.set_item("shortest_path_cache_hits", stats.shortest_path_cache_hits)?;
    dict.set_item("db_hits", stats.db_hits)?;
    dict.set_item("elapsed_us", stats.elapsed_us)?;
    dict.set_item("effective_at_epoch", stats.effective_at_epoch)?;
    dict.set_item("warnings", stats.warnings)?;
    Ok(dict.into_any().unbind())
}

fn gql_edge_to_py(py: Python<'_>, edge: GqlEdge) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    if let Some(id) = edge.id {
        dict.set_item("id", id)?;
    }
    if let Some(from) = edge.from {
        dict.set_item("from_id", from)?;
    }
    if let Some(to) = edge.to {
        dict.set_item("to_id", to)?;
    }
    if let Some(label) = edge.label {
        dict.set_item("label", label)?;
    }
    if let Some(props) = edge.props {
        dict.set_item("props", gql_value_to_py(py, GqlValue::Map(props))?)?;
    }
    if let Some(weight) = edge.weight {
        dict.set_item("weight", weight as f64)?;
    }
    if let Some(created_at) = edge.created_at {
        dict.set_item("created_at", created_at)?;
    }
    if let Some(updated_at) = edge.updated_at {
        dict.set_item("updated_at", updated_at)?;
    }
    if let Some(valid_from) = edge.valid_from {
        dict.set_item("valid_from", valid_from)?;
    }
    if let Some(valid_to) = edge.valid_to {
        dict.set_item("valid_to", valid_to)?;
    }
    Ok(dict.into_any().unbind())
}

fn gql_stats_to_py(py: Python<'_>, stats: GqlExecutionStats) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    dict.set_item("rows_returned", stats.rows_returned)?;
    dict.set_item("rows_matched", stats.rows_matched)?;
    dict.set_item("rows_after_filter", stats.rows_after_filter)?;
    dict.set_item("intermediate_bindings", stats.intermediate_bindings)?;
    dict.set_item("db_hits", stats.db_hits)?;
    dict.set_item("elapsed_us", stats.elapsed_us)?;
    dict.set_item("warnings", stats.warnings)?;
    Ok(dict.into_any().unbind())
}

fn gql_mutation_stats_to_py(py: Python<'_>, stats: eg::GqlMutationStats) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    dict.set_item("rows_matched", stats.rows_matched)?;
    dict.set_item("mutation_rows", stats.mutation_rows)?;
    dict.set_item("mutation_ops", stats.mutation_ops)?;
    dict.set_item("nodes_created", stats.nodes_created)?;
    dict.set_item("nodes_updated", stats.nodes_updated)?;
    dict.set_item("nodes_deleted", stats.nodes_deleted)?;
    dict.set_item("edges_created", stats.edges_created)?;
    dict.set_item("edges_updated", stats.edges_updated)?;
    dict.set_item("edges_deleted", stats.edges_deleted)?;
    dict.set_item("labels_added", stats.labels_added)?;
    dict.set_item("labels_removed", stats.labels_removed)?;
    dict.set_item("properties_set", stats.properties_set)?;
    dict.set_item("properties_removed", stats.properties_removed)?;
    dict.set_item("skipped_null_targets", stats.skipped_null_targets)?;
    dict.set_item("duplicate_targets", stats.duplicate_targets)?;
    dict.set_item("db_hits", stats.db_hits)?;
    dict.set_item("elapsed_us", stats.elapsed_us)?;
    dict.set_item("warnings", stats.warnings)?;
    Ok(dict.into_any().unbind())
}

fn gql_schema_stats_to_py(py: Python<'_>, stats: eg::GqlSchemaStats) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    dict.set_item("operation", stats.operation)?;
    dict.set_item("targets_checked", stats.targets_checked)?;
    dict.set_item("targets_published", stats.targets_published)?;
    dict.set_item("targets_dropped", stats.targets_dropped)?;
    dict.set_item("checked_records", stats.checked_records)?;
    dict.set_item("violation_count", stats.violation_count)?;
    dict.set_item("truncated", stats.truncated)?;
    dict.set_item("scan_limit_hit", stats.scan_limit_hit)?;
    dict.set_item("elapsed_us", stats.elapsed_us)?;
    dict.set_item("warnings", stats.warnings)?;
    Ok(dict.into_any().unbind())
}

fn gql_index_stats_to_py(py: Python<'_>, stats: eg::GqlIndexStats) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    dict.set_item("operation", stats.operation)?;
    dict.set_item("indexes_ensured", stats.indexes_ensured)?;
    dict.set_item("indexes_dropped", stats.indexes_dropped)?;
    dict.set_item("indexes_returned", stats.indexes_returned)?;
    dict.set_item("elapsed_us", stats.elapsed_us)?;
    dict.set_item("warnings", stats.warnings)?;
    Ok(dict.into_any().unbind())
}

fn gql_statement_kind_to_py(kind: GqlStatementKind) -> &'static str {
    match kind {
        GqlStatementKind::Query => "query",
        GqlStatementKind::Mutation => "mutation",
        GqlStatementKind::Schema => "schema",
        GqlStatementKind::Index => "index",
    }
}

fn gql_explain_to_py(py: Python<'_>, explain: GqlExecutionExplain) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    dict.set_item("kind", gql_statement_kind_to_py(explain.kind))?;
    dict.set_item("columns", explain.columns)?;
    match explain.read {
        Some(read) => dict.set_item("read", gql_read_explain_to_py(py, read)?)?,
        None => dict.set_item("read", py.None())?,
    }
    match explain.mutation {
        Some(mutation) => dict.set_item("mutation", gql_mutation_explain_to_py(py, mutation)?)?,
        None => dict.set_item("mutation", py.None())?,
    }
    match explain.schema {
        Some(schema) => dict.set_item("schema", gql_schema_explain_to_py(py, schema)?)?,
        None => dict.set_item("schema", py.None())?,
    }
    match explain.index {
        Some(index) => dict.set_item("index", gql_index_explain_to_py(py, index)?)?,
        None => dict.set_item("index", py.None())?,
    }
    dict.set_item("caps", gql_execution_caps_to_py(py, explain.caps)?)?;
    dict.set_item("warnings", explain.warnings)?;
    dict.set_item("notes", explain.notes)?;
    Ok(dict.into_any().unbind())
}

fn gql_read_explain_to_py(py: Python<'_>, explain: GqlExplain) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    dict.set_item("columns", explain.columns)?;
    dict.set_item("target", gql_lowering_target_to_py(explain.target))?;
    match explain.native_plan {
        Some(plan) => dict.set_item("native_plan", query_plan_to_py(py, plan)?)?,
        None => dict.set_item("native_plan", py.None())?,
    }
    dict.set_item("pushed_down", explain.pushed_down)?;
    dict.set_item("residual", explain.residual)?;
    dict.set_item("projection", explain.projection)?;
    dict.set_item(
        "row_ops",
        explain
            .row_ops
            .into_iter()
            .map(gql_row_operation_to_py)
            .collect::<Vec<_>>(),
    )?;
    dict.set_item("caps", gql_caps_to_py(py, explain.caps)?)?;
    dict.set_item("warnings", explain.warnings)?;
    Ok(dict.into_any().unbind())
}

fn gql_mutation_explain_to_py(
    py: Python<'_>,
    explain: eg::GqlMutationExplain,
) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    if let Some(prefix) = explain.read_prefix {
        let prefix_dict = PyDict::new(py);
        prefix_dict.set_item(
            "graph_row_target",
            gql_read_explain_to_py(py, prefix.graph_row_target)?,
        )?;
        prefix_dict.set_item("internal_columns", prefix.internal_columns)?;
        prefix_dict.set_item("target_aliases", prefix.target_aliases)?;
        prefix_dict.set_item("expression_columns", prefix.expression_columns)?;
        dict.set_item("read_prefix", prefix_dict)?;
    } else {
        dict.set_item("read_prefix", py.None())?;
    }
    let operations = explain
        .operations
        .into_iter()
        .map(|operation| {
            let item = PyDict::new(py);
            item.set_item("op", operation.op)?;
            item.set_item("target_alias", operation.target_alias)?;
            item.set_item("row_multiplicity", operation.row_multiplicity)?;
            item.set_item("detail", operation.detail)?;
            Ok(item.into_any().unbind())
        })
        .collect::<PyResult<Vec<PyObject>>>()?;
    dict.set_item("operations", operations)?;
    if let Some(plan) = explain.return_plan {
        let plan_dict = PyDict::new(py);
        plan_dict.set_item("columns", plan.columns)?;
        plan_dict.set_item("order_items", plan.order_items)?;
        plan_dict.set_item("skip", plan.skip)?;
        plan_dict.set_item("limit", plan.limit)?;
        plan_dict.set_item("post_commit_hydration", plan.post_commit_hydration)?;
        dict.set_item("return_plan", plan_dict)?;
    } else {
        dict.set_item("return_plan", py.None())?;
    }
    dict.set_item("would_create_node_labels", explain.would_create_node_labels)?;
    dict.set_item("would_create_edge_labels", explain.would_create_edge_labels)?;
    dict.set_item(
        "uses_transaction_snapshot",
        explain.uses_transaction_snapshot,
    )?;
    dict.set_item("uses_write_txn", explain.uses_write_txn)?;
    dict.set_item("replacement_adapters", explain.replacement_adapters)?;
    dict.set_item("atomic_commit", explain.atomic_commit)?;
    Ok(dict.into_any().unbind())
}

fn gql_schema_explain_to_py(py: Python<'_>, explain: eg::GqlSchemaExplain) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    dict.set_item("operation", explain.operation)?;
    let targets = explain
        .targets
        .into_iter()
        .map(|target| {
            let item = PyDict::new(py);
            item.set_item("target_kind", target.target_kind)?;
            item.set_item("label", target.label)?;
            item.set_item("action", target.action)?;
            Ok(item.into_any().unbind())
        })
        .collect::<PyResult<Vec<PyObject>>>()?;
    dict.set_item("targets", targets)?;
    dict.set_item("replaces_entire_catalog", explain.replaces_entire_catalog)?;
    dict.set_item("publishes_manifest", explain.publishes_manifest)?;
    dict.set_item("validates_existing_data", explain.validates_existing_data)?;
    dict.set_item("uses_core_write_queue", explain.uses_core_write_queue)?;
    dict.set_item("side_effect_free", explain.side_effect_free)?;
    let options = PyDict::new(py);
    options.set_item("max_violations", explain.options.max_violations)?;
    options.set_item("chunk_size", explain.options.chunk_size)?;
    options.set_item("scan_limit", explain.options.scan_limit)?;
    dict.set_item("options", options)?;
    Ok(dict.into_any().unbind())
}

fn gql_index_explain_to_py(py: Python<'_>, explain: eg::GqlIndexExplain) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    dict.set_item("operation", explain.operation)?;
    let targets = explain
        .targets
        .into_iter()
        .map(|target| {
            let item = PyDict::new(py);
            let fields = target
                .fields
                .into_iter()
                .map(|field| {
                    let field_item = PyDict::new(py);
                    field_item.set_item("source", field.source)?;
                    field_item.set_item("key", field.key)?;
                    field_item.set_item("field", field.field)?;
                    Ok(field_item.into_any().unbind())
                })
                .collect::<PyResult<Vec<PyObject>>>()?;
            item.set_item("target_kind", target.target_kind)?;
            item.set_item("label", target.label)?;
            item.set_item("fields", fields)?;
            item.set_item("kind", target.kind)?;
            item.set_item("action", target.action)?;
            item.set_item("compound", target.compound)?;
            Ok(item.into_any().unbind())
        })
        .collect::<PyResult<Vec<PyObject>>>()?;
    dict.set_item("targets", targets)?;
    dict.set_item("uses_core_write_queue", explain.uses_core_write_queue)?;
    dict.set_item("publishes_manifest", explain.publishes_manifest)?;
    dict.set_item("creates_labels", explain.creates_labels)?;
    dict.set_item(
        "schedules_background_build",
        explain.schedules_background_build,
    )?;
    dict.set_item("drops_index_data_async", explain.drops_index_data_async)?;
    dict.set_item("side_effect_free", explain.side_effect_free)?;
    Ok(dict.into_any().unbind())
}

fn gql_execution_caps_to_py(py: Python<'_>, caps: GqlExecutionCapSummary) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    dict.set_item("allow_full_scan", caps.allow_full_scan)?;
    dict.set_item("max_rows", caps.max_rows)?;
    dict.set_item("max_cursor_bytes", caps.max_cursor_bytes)?;
    dict.set_item("max_mutation_rows", caps.max_mutation_rows)?;
    dict.set_item("max_mutation_ops", caps.max_mutation_ops)?;
    dict.set_item("max_pipeline_rows", caps.max_pipeline_rows)?;
    dict.set_item("max_groups", caps.max_groups)?;
    dict.set_item("max_collect_items", caps.max_collect_items)?;
    dict.set_item("max_union_branches", caps.max_union_branches)?;
    dict.set_item("max_subquery_invocations", caps.max_subquery_invocations)?;
    dict.set_item("max_subquery_depth", caps.max_subquery_depth)?;
    dict.set_item("max_shortest_path_pairs", caps.max_shortest_path_pairs)?;
    dict.set_item("max_query_bytes", caps.max_query_bytes)?;
    dict.set_item("max_param_bytes", caps.max_param_bytes)?;
    dict.set_item("max_ast_depth", caps.max_ast_depth)?;
    dict.set_item("max_literal_items", caps.max_literal_items)?;
    dict.set_item("max_intermediate_bindings", caps.max_intermediate_bindings)?;
    dict.set_item("max_frontier", caps.max_frontier)?;
    dict.set_item("max_path_hops", caps.max_path_hops)?;
    dict.set_item("max_paths_per_start", caps.max_paths_per_start)?;
    dict.set_item("max_order_materialization", caps.max_order_materialization)?;
    dict.set_item("max_skip", caps.max_skip)?;
    Ok(dict.into_any().unbind())
}

fn gql_caps_to_py(py: Python<'_>, caps: GqlCapSummary) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    dict.set_item("allow_full_scan", caps.allow_full_scan)?;
    dict.set_item("max_rows", caps.max_rows)?;
    dict.set_item("max_intermediate_bindings", caps.max_intermediate_bindings)?;
    dict.set_item("max_skip", caps.max_skip)?;
    dict.set_item("max_query_bytes", caps.max_query_bytes)?;
    dict.set_item("max_param_bytes", caps.max_param_bytes)?;
    dict.set_item("max_ast_depth", caps.max_ast_depth)?;
    dict.set_item("max_literal_items", caps.max_literal_items)?;
    Ok(dict.into_any().unbind())
}

fn gql_lowering_target_to_py(target: GqlLoweringTarget) -> &'static str {
    match target {
        GqlLoweringTarget::NodeQuery => "node_query",
        GqlLoweringTarget::EdgeQuery => "edge_query",
        GqlLoweringTarget::GraphRowQuery => "graph_row_query",
        GqlLoweringTarget::GraphPipelineQuery => "graph_pipeline_query",
    }
}

fn graph_row_explain_to_py(py: Python<'_>, explain: GraphRowExplain) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    dict.set_item("columns", explain.columns)?;
    dict.set_item("effective_at_epoch", explain.effective_at_epoch)?;
    dict.set_item("fingerprint", explain.fingerprint)?;
    let plan: PyResult<Vec<PyObject>> = explain
        .plan
        .into_iter()
        .map(|node| graph_explain_node_to_py(py, node))
        .collect();
    dict.set_item("plan", plan?)?;
    let row_ops = explain
        .row_ops
        .into_iter()
        .map(|op| graph_row_operation_to_py(py, op))
        .collect::<PyResult<Vec<_>>>()?;
    dict.set_item("row_ops", row_ops)?;
    dict.set_item("order", graph_order_explain_to_py(py, explain.order)?)?;
    dict.set_item("cursor", graph_cursor_explain_to_py(py, explain.cursor)?)?;
    dict.set_item(
        "projection",
        graph_projection_explain_to_py(py, explain.projection)?,
    )?;
    dict.set_item("caps", graph_cap_explain_to_py(py, explain.caps)?)?;
    dict.set_item(
        "summaries",
        graph_execution_summaries_to_py(py, explain.summaries)?,
    )?;
    dict.set_item("warnings", explain.warnings)?;
    dict.set_item("notes", explain.notes)?;
    Ok(dict.into_any().unbind())
}

fn graph_pipeline_explain_to_py(
    py: Python<'_>,
    explain: GraphPipelineExplain,
) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    dict.set_item("columns", explain.columns)?;
    dict.set_item("effective_at_epoch", explain.effective_at_epoch)?;
    dict.set_item("fingerprint", explain.fingerprint)?;
    let stages = explain
        .stages
        .into_iter()
        .map(|stage| graph_pipeline_stage_explain_to_py(py, stage))
        .collect::<PyResult<Vec<_>>>()?;
    dict.set_item("stages", stages)?;
    let row_ops = explain
        .row_ops
        .into_iter()
        .map(|op| graph_row_operation_to_py(py, op))
        .collect::<PyResult<Vec<_>>>()?;
    dict.set_item("row_ops", row_ops)?;
    dict.set_item("order", graph_order_explain_to_py(py, explain.order)?)?;
    dict.set_item("cursor", graph_cursor_explain_to_py(py, explain.cursor)?)?;
    dict.set_item(
        "projection",
        graph_projection_explain_to_py(py, explain.projection)?,
    )?;
    dict.set_item("caps", graph_pipeline_cap_explain_to_py(py, explain.caps)?)?;
    dict.set_item(
        "summaries",
        graph_execution_summaries_to_py(py, explain.summaries)?,
    )?;
    dict.set_item("stats", graph_pipeline_stats_to_py(py, explain.stats)?)?;
    dict.set_item("warnings", explain.warnings)?;
    dict.set_item("notes", explain.notes)?;
    Ok(dict.into_any().unbind())
}

fn graph_pipeline_stage_explain_to_py(
    py: Python<'_>,
    stage: GraphPipelineStageExplain,
) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    dict.set_item("index", stage.index)?;
    dict.set_item("kind", stage.kind)?;
    dict.set_item("detail", stage.detail)?;
    dict.set_item("columns", stage.columns)?;
    match stage.graph_row {
        Some(explain) => dict.set_item("graph_row", graph_row_explain_to_py(py, *explain)?)?,
        None => dict.set_item("graph_row", py.None())?,
    }
    dict.set_item("warnings", stage.warnings)?;
    dict.set_item("notes", stage.notes)?;
    Ok(dict.into_any().unbind())
}

fn graph_explain_node_to_py(py: Python<'_>, node: GraphExplainNode) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    dict.set_item("kind", node.kind)?;
    dict.set_item("detail", node.detail)?;
    let children: PyResult<Vec<PyObject>> = node
        .children
        .into_iter()
        .map(|child| graph_explain_node_to_py(py, child))
        .collect();
    dict.set_item("children", children?)?;
    Ok(dict.into_any().unbind())
}

fn graph_row_operation_to_py(py: Python<'_>, op: GraphRowOperationExplain) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    dict.set_item("kind", op.kind)?;
    dict.set_item("detail", op.detail)?;
    Ok(dict.into_any().unbind())
}

fn graph_order_explain_to_py(py: Python<'_>, order: GraphOrderExplain) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    dict.set_item("explicit", order.explicit)?;
    dict.set_item("items", order.items)?;
    dict.set_item("stable_logical_row_key", order.stable_logical_row_key)?;
    Ok(dict.into_any().unbind())
}

fn graph_cursor_explain_to_py(py: Python<'_>, cursor: GraphCursorExplain) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    dict.set_item("supplied", cursor.supplied)?;
    dict.set_item("codec_implemented", cursor.codec_implemented)?;
    dict.set_item("message", cursor.message)?;
    Ok(dict.into_any().unbind())
}

fn graph_projection_explain_to_py(
    py: Python<'_>,
    projection: GraphProjectionExplain,
) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    dict.set_item("columns", projection.columns)?;
    dict.set_item(
        "output_mode",
        graph_output_mode_to_py(&projection.output_mode),
    )?;
    dict.set_item("include_vectors", projection.include_vectors)?;
    dict.set_item("compact_rows", projection.compact_rows)?;
    Ok(dict.into_any().unbind())
}

fn graph_cap_explain_to_py(py: Python<'_>, caps: GraphCapExplain) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    dict.set_item("allow_full_scan", caps.allow_full_scan)?;
    dict.set_item("max_intermediate_bindings", caps.max_intermediate_bindings)?;
    dict.set_item("max_frontier", caps.max_frontier)?;
    dict.set_item("max_path_hops", caps.max_path_hops)?;
    dict.set_item("max_paths_per_start", caps.max_paths_per_start)?;
    dict.set_item("max_page_limit", caps.max_page_limit)?;
    dict.set_item("max_order_materialization", caps.max_order_materialization)?;
    dict.set_item("max_cursor_bytes", caps.max_cursor_bytes)?;
    dict.set_item("max_query_bytes", caps.max_query_bytes)?;
    Ok(dict.into_any().unbind())
}

fn graph_pipeline_cap_explain_to_py(
    py: Python<'_>,
    caps: GraphPipelineCapExplain,
) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    dict.set_item("allow_full_scan", caps.allow_full_scan)?;
    dict.set_item("max_rows", caps.max_rows)?;
    dict.set_item("max_pipeline_rows", caps.max_pipeline_rows)?;
    dict.set_item("max_groups", caps.max_groups)?;
    dict.set_item("max_collect_items", caps.max_collect_items)?;
    dict.set_item("max_union_branches", caps.max_union_branches)?;
    dict.set_item("max_subquery_invocations", caps.max_subquery_invocations)?;
    dict.set_item("max_subquery_depth", caps.max_subquery_depth)?;
    dict.set_item("max_shortest_path_pairs", caps.max_shortest_path_pairs)?;
    dict.set_item("max_intermediate_bindings", caps.max_intermediate_bindings)?;
    dict.set_item("max_frontier", caps.max_frontier)?;
    dict.set_item("max_path_hops", caps.max_path_hops)?;
    dict.set_item("max_paths_per_start", caps.max_paths_per_start)?;
    dict.set_item("max_order_materialization", caps.max_order_materialization)?;
    dict.set_item("max_skip", caps.max_skip)?;
    dict.set_item("max_cursor_bytes", caps.max_cursor_bytes)?;
    dict.set_item("max_query_bytes", caps.max_query_bytes)?;
    dict.set_item("max_param_bytes", caps.max_param_bytes)?;
    dict.set_item("max_ast_depth", caps.max_ast_depth)?;
    dict.set_item("max_literal_items", caps.max_literal_items)?;
    Ok(dict.into_any().unbind())
}

fn graph_execution_summaries_to_py(
    py: Python<'_>,
    summaries: GraphExecutionSummaries,
) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    dict.set_item("validation_only", summaries.validation_only)?;
    dict.set_item("rows_planned", summaries.rows_planned)?;
    dict.set_item("warnings", summaries.warnings)?;
    Ok(dict.into_any().unbind())
}

fn graph_output_mode_to_py(mode: &GraphOutputMode) -> &'static str {
    match mode {
        GraphOutputMode::Ids => "ids",
        GraphOutputMode::Elements => "elements",
        GraphOutputMode::Projected => "projected",
    }
}

fn gql_row_operation_to_py(op: GqlRowOperation) -> &'static str {
    match op {
        GqlRowOperation::ResidualFilter => "residual_filter",
        GqlRowOperation::Projection => "projection",
        GqlRowOperation::Sort => "sort",
        GqlRowOperation::Skip => "skip",
        GqlRowOperation::Limit => "limit",
    }
}

fn query_plan_to_py(py: Python<'_>, plan: QueryPlan) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    dict.set_item("kind", query_plan_kind_to_py(&plan.kind))?;
    dict.set_item("root", query_plan_node_to_py(py, plan.root)?)?;
    dict.set_item("estimated_candidates", plan.estimated_candidates)?;
    dict.set_item(
        "warnings",
        plan.warnings
            .iter()
            .map(query_plan_warning_to_py)
            .collect::<Vec<_>>(),
    )?;
    dict.set_item(
        "notes",
        plan.notes
            .iter()
            .map(query_plan_note_to_py)
            .collect::<Vec<_>>(),
    )?;
    dict.set_item(
        "public_inputs",
        query_plan_public_inputs_to_py(py, plan.public_inputs)?,
    )?;
    Ok(dict.into_any().unbind())
}

fn query_plan_kind_to_py(kind: &QueryPlanKind) -> &'static str {
    match kind {
        QueryPlanKind::NodeQuery => "node_query",
        QueryPlanKind::EdgeQuery => "edge_query",
    }
}

fn query_plan_node_to_py(py: Python<'_>, node: QueryPlanNode) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    match node {
        QueryPlanNode::ExplicitIds => dict.set_item("kind", "explicit_ids")?,
        QueryPlanNode::KeyLookup => dict.set_item("kind", "key_lookup")?,
        QueryPlanNode::NodeLabelIndex => dict.set_item("kind", "node_label_index")?,
        QueryPlanNode::NodeLabelAnyIndex => dict.set_item("kind", "node_label_any_index")?,
        QueryPlanNode::CompoundEqualityIndex { details } => {
            dict.set_item("kind", "compound_equality_index")?;
            dict.set_item("details", compound_index_plan_details_to_py(py, details)?)?;
        }
        QueryPlanNode::CompoundRangeIndex { details } => {
            dict.set_item("kind", "compound_range_index")?;
            dict.set_item("details", compound_index_plan_details_to_py(py, details)?)?;
        }
        QueryPlanNode::PropertyEqualityIndex => dict.set_item("kind", "property_equality_index")?,
        QueryPlanNode::PropertyRangeIndex => dict.set_item("kind", "property_range_index")?,
        QueryPlanNode::TimestampIndex => dict.set_item("kind", "timestamp_index")?,
        QueryPlanNode::AdjacencyExpansion => dict.set_item("kind", "adjacency_expansion")?,
        QueryPlanNode::ExplicitEdgeIds => dict.set_item("kind", "explicit_edge_ids")?,
        QueryPlanNode::EdgeLabelIndex => dict.set_item("kind", "edge_label_index")?,
        QueryPlanNode::EdgeTripleIndex => dict.set_item("kind", "edge_triple_index")?,
        QueryPlanNode::EdgeEndpointAdjacency => dict.set_item("kind", "edge_endpoint_adjacency")?,
        QueryPlanNode::EdgeWeightIndex => dict.set_item("kind", "edge_weight_index")?,
        QueryPlanNode::EdgeUpdatedAtIndex => dict.set_item("kind", "edge_updated_at_index")?,
        QueryPlanNode::EdgeValidityIndex => dict.set_item("kind", "edge_validity_index")?,
        QueryPlanNode::EdgeMetadataScan => dict.set_item("kind", "edge_metadata_scan")?,
        QueryPlanNode::EdgePropertyEqualityIndex => {
            dict.set_item("kind", "edge_property_equality_index")?
        }
        QueryPlanNode::EdgePropertyRangeIndex => {
            dict.set_item("kind", "edge_property_range_index")?
        }
        QueryPlanNode::Intersect { inputs } => {
            dict.set_item("kind", "intersect")?;
            let inputs: PyResult<Vec<PyObject>> = inputs
                .into_iter()
                .map(|input| query_plan_node_to_py(py, input))
                .collect();
            dict.set_item("inputs", inputs?)?;
        }
        QueryPlanNode::Union { inputs } => {
            dict.set_item("kind", "union")?;
            let inputs: PyResult<Vec<PyObject>> = inputs
                .into_iter()
                .map(|input| query_plan_node_to_py(py, input))
                .collect();
            dict.set_item("inputs", inputs?)?;
        }
        QueryPlanNode::VerifyNodeFilter { input } => {
            dict.set_item("kind", "verify_node_filter")?;
            dict.set_item("input", query_plan_node_to_py(py, *input)?)?;
        }
        QueryPlanNode::VerifyEdgeFilter { input } => {
            dict.set_item("kind", "verify_edge_filter")?;
            dict.set_item("input", query_plan_node_to_py(py, *input)?)?;
        }
        QueryPlanNode::VerifyEdgePredicates { input } => {
            dict.set_item("kind", "verify_edge_predicates")?;
            dict.set_item("input", query_plan_node_to_py(py, *input)?)?;
        }
        QueryPlanNode::FallbackNodeLabelScan => {
            dict.set_item("kind", "fallback_node_label_scan")?
        }
        QueryPlanNode::FallbackFullNodeScan => dict.set_item("kind", "fallback_full_node_scan")?,
        QueryPlanNode::FallbackEdgeLabelScan => {
            dict.set_item("kind", "fallback_edge_label_scan")?
        }
        QueryPlanNode::FallbackFullEdgeScan => dict.set_item("kind", "fallback_full_edge_scan")?,
        QueryPlanNode::EmptyResult => dict.set_item("kind", "empty_result")?,
    }
    Ok(dict.into_any().unbind())
}

fn query_plan_note_to_py(note: &QueryPlanNote) -> &'static str {
    match note {
        QueryPlanNote::NodeLabelAnyDedupeBeforePagination => {
            "node_label_any_dedupe_before_pagination"
        }
        QueryPlanNote::NodeLabelAnyFinalVerification => "node_label_any_final_verification",
        QueryPlanNote::NodeLabelAllSupersetVerification => "node_label_all_superset_verification",
        QueryPlanNote::StaleNodeLabelMembershipVerification => {
            "stale_node_label_membership_verification"
        }
    }
}

fn query_plan_public_inputs_to_py(
    py: Python<'_>,
    inputs: QueryPlanPublicInputs,
) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    let node_labels = inputs
        .node_labels
        .into_iter()
        .map(|name| query_plan_public_name_to_py(py, name))
        .collect::<PyResult<Vec<_>>>()?;
    let edge_labels = inputs
        .edge_labels
        .into_iter()
        .map(|name| query_plan_public_name_to_py(py, name))
        .collect::<PyResult<Vec<_>>>()?;
    dict.set_item("node_labels", node_labels)?;
    dict.set_item("edge_labels", edge_labels)?;
    Ok(dict.into_any().unbind())
}

fn query_plan_public_name_to_py(py: Python<'_>, name: QueryPlanPublicName) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    dict.set_item("alias", name.alias)?;
    dict.set_item("name", name.name)?;
    dict.set_item("known", name.known)?;
    dict.set_item(
        "mode",
        name.mode.map(|mode| match mode {
            LabelMatchMode::Any => "any",
            LabelMatchMode::All => "all",
        }),
    )?;
    Ok(dict.into_any().unbind())
}

fn query_plan_warning_to_py(warning: &QueryPlanWarning) -> &'static str {
    match warning {
        QueryPlanWarning::MissingReadyIndex => "missing_ready_index",
        QueryPlanWarning::UsingFallbackScan => "using_fallback_scan",
        QueryPlanWarning::FullScanRequiresOptIn => "full_scan_requires_opt_in",
        QueryPlanWarning::FullScanExplicitlyAllowed => "full_scan_explicitly_allowed",
        QueryPlanWarning::EdgePropertyPostFilter => "edge_property_post_filter",
        QueryPlanWarning::IndexSkippedAsBroad => "index_skipped_as_broad",
        QueryPlanWarning::CandidateCapExceeded => "candidate_cap_exceeded",
        QueryPlanWarning::RangeCandidateCapExceeded => "range_candidate_cap_exceeded",
        QueryPlanWarning::TimestampCandidateCapExceeded => "timestamp_candidate_cap_exceeded",
        QueryPlanWarning::VerifyOnlyFilter => "verify_only_filter",
        QueryPlanWarning::BooleanBranchFallback => "boolean_branch_fallback",
        QueryPlanWarning::PlanningProbeBudgetExceeded => "planning_probe_budget_exceeded",
        QueryPlanWarning::CompoundIndexPrefixNotSatisfied => "compound_index_prefix_not_satisfied",
        QueryPlanWarning::UnknownNodeLabel => "unknown_node_label",
        QueryPlanWarning::UnknownEdgeLabel => "unknown_edge_label",
    }
}

fn compound_index_plan_details_to_py(
    py: Python<'_>,
    details: CompoundIndexPlanDetails,
) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    dict.set_item("index_id", details.index_id)?;
    dict.set_item(
        "target_kind",
        query_plan_compound_target_kind_to_py(details.target_kind),
    )?;
    dict.set_item("label", details.label)?;
    dict.set_item("kind", secondary_index_kind_to_py(&details.kind))?;
    dict.set_item(
        "fields",
        details
            .fields
            .into_iter()
            .map(secondary_index_field_to_py)
            .collect::<Vec<_>>(),
    )?;
    dict.set_item("compound", details.compound)?;
    dict.set_item("matched_prefix_len", details.matched_prefix_len)?;
    dict.set_item(
        "range_field",
        details.range_field.map(secondary_index_field_to_py),
    )?;
    dict.set_item("in_expansions", details.in_expansions)?;
    dict.set_item("estimated_candidates", details.estimated_candidates)?;
    dict.set_item("coverage", details.coverage)?;
    dict.set_item("residual_predicates", details.residual_predicates)?;
    dict.set_item("final_verification", details.final_verification)?;
    dict.set_item("fallback_reason", details.fallback_reason)?;
    Ok(dict.into_any().unbind())
}

fn query_plan_compound_target_kind_to_py(kind: QueryPlanCompoundTargetKind) -> &'static str {
    match kind {
        QueryPlanCompoundTargetKind::Node => "node",
        QueryPlanCompoundTargetKind::Edge => "edge",
    }
}

fn parse_py_node_query(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<NodeQuery> {
    if let Ok(dict) = value.downcast::<PyDict>() {
        return parse_py_node_query_dict(py, dict);
    }
    if value.hasattr("to_dict")? {
        let dict_value = value.call_method0("to_dict")?;
        let dict = dict_value.downcast::<PyDict>()?;
        return parse_py_node_query_dict(py, dict);
    }
    Err(PyTypeError::new_err(
        "node query request must be a dict or expose to_dict()",
    ))
}

fn parse_py_node_query_dict(py: Python<'_>, dict: &Bound<'_, PyDict>) -> PyResult<NodeQuery> {
    reject_py_legacy_node_label_field(dict, "node query")?;
    let page = PageRequest {
        limit: parse_py_query_limit(dict, "node query limit")?,
        after: py_optional_query_u64(dict, "after", "node query after")?,
    };
    let order = match py_non_none_item(dict, "order_by")? {
        None => NodeQueryOrder::NodeIdAsc,
        Some(value) => {
            let order_by: String = value.extract()?;
            match order_by.as_str() {
                "node_id_asc" | "nodeIdAsc" => NodeQueryOrder::NodeIdAsc,
                other => {
                    return Err(PyValueError::new_err(format!(
                        "Invalid order_by '{}'. Must be 'node_id_asc'.",
                        other
                    )));
                }
            }
        }
    };
    Ok(NodeQuery {
        label_filter: parse_optional_node_label_filter_field(
            dict,
            "label_filter",
            "node query label_filter",
        )?,
        ids: py_optional_query_u64_vec(dict, "ids", "node query ids")?,
        keys: py_optional_extract::<Vec<String>>(dict, "keys")?.unwrap_or_default(),
        filter: parse_py_node_filter(py, dict, "updated_at", "node query")?,
        page,
        order,
        allow_full_scan: py_optional_extract::<bool>(dict, "allow_full_scan")?.unwrap_or(false),
    })
}

fn parse_py_edge_query(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<EdgeQuery> {
    if let Ok(dict) = value.downcast::<PyDict>() {
        return parse_py_edge_query_dict(py, dict);
    }
    if value.hasattr("to_dict")? {
        let dict_value = value.call_method0("to_dict")?;
        let dict = dict_value.downcast::<PyDict>()?;
        return parse_py_edge_query_dict(py, dict);
    }
    Err(PyTypeError::new_err(
        "edge query request must be a dict or expose to_dict()",
    ))
}

fn parse_py_edge_query_dict(py: Python<'_>, dict: &Bound<'_, PyDict>) -> PyResult<EdgeQuery> {
    reject_py_legacy_node_predicate_fields(dict, "edge query")?;
    let page = PageRequest {
        limit: parse_py_query_limit(dict, "edge query limit")?,
        after: py_optional_query_u64(dict, "after", "edge query after")?,
    };
    Ok(EdgeQuery {
        label: py_optional_extract::<String>(dict, "label")?,
        ids: py_optional_query_u64_vec(dict, "ids", "edge query ids")?,
        from_ids: py_optional_query_u64_vec(dict, "from_ids", "edge query from_ids")?,
        to_ids: py_optional_query_u64_vec(dict, "to_ids", "edge query to_ids")?,
        endpoint_ids: py_optional_query_u64_vec(dict, "endpoint_ids", "edge query endpoint_ids")?,
        filter: parse_py_edge_filter(
            py,
            dict,
            "updated_at",
            "valid_at",
            "valid_from",
            "valid_to",
            "edge query",
        )?,
        page,
        order: EdgeQueryOrder::EdgeIdAsc,
        allow_full_scan: py_optional_extract::<bool>(dict, "allow_full_scan")?.unwrap_or(false),
    })
}

fn parse_py_graph_row_query(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<GraphRowQuery> {
    if let Ok(dict) = value.downcast::<PyDict>() {
        return parse_py_graph_row_query_dict(py, dict);
    }
    if value.hasattr("to_dict")? {
        let dict_value = value.call_method0("to_dict")?;
        let dict = dict_value.downcast::<PyDict>()?;
        return parse_py_graph_row_query_dict(py, dict);
    }
    Err(PyTypeError::new_err(
        "graph row request must be a dict or expose to_dict()",
    ))
}

fn parse_py_graph_row_query_dict(
    py: Python<'_>,
    dict: &Bound<'_, PyDict>,
) -> PyResult<GraphRowQuery> {
    let options = parse_py_graph_query_options(dict)?;
    let page = GraphPageRequest {
        skip: py_optional_query_usize(dict, "skip", "graph row skip")?.unwrap_or(0),
        limit: py_optional_query_usize(dict, "limit", "graph row limit")?
            .unwrap_or(options.max_page_limit.min(1000)),
        cursor: py_optional_extract(dict, "cursor")?,
    };

    let mut nodes = Vec::new();
    if let Some(nodes_value) = py_non_none_item(dict, "nodes")? {
        let nodes_list = nodes_value.downcast::<PyList>()?;
        nodes.reserve(nodes_list.len());
        for (index, item) in nodes_list.iter().enumerate() {
            nodes.push(parse_py_graph_node_pattern(
                py,
                item.downcast::<PyDict>()?,
                &format!("graph row nodes[{index}]"),
            )?);
        }
    }

    let mut pieces = Vec::new();
    if let Some(pieces_value) = py_non_none_item(dict, "pieces")? {
        let pieces_list = pieces_value.downcast::<PyList>()?;
        pieces.reserve(pieces_list.len());
        for (index, item) in pieces_list.iter().enumerate() {
            pieces.push(parse_py_graph_pattern_piece(
                py,
                item.downcast::<PyDict>()?,
                &format!("graph row pieces[{index}]"),
            )?);
        }
    }

    if page.limit == 0 {
        return Err(PyValueError::new_err("graph row limit must be > 0"));
    }

    Ok(GraphRowQuery {
        nodes,
        pieces,
        where_: py_non_none_item(dict, "where")?
            .map(|value| parse_py_graph_expr(py, &value, "graph row where"))
            .transpose()?,
        return_items: parse_py_graph_return_items(py, dict)?,
        order_by: parse_py_graph_order_items(py, dict)?,
        page,
        at_epoch: py_optional_query_i64(dict, "at_epoch", "graph row at_epoch")?,
        params: parse_py_graph_params(py, dict)?,
        output: parse_py_graph_output_options(dict)?,
        options,
    })
}

fn parse_py_graph_pipeline_query(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
) -> PyResult<GraphPipelineQuery> {
    if let Ok(dict) = value.downcast::<PyDict>() {
        return parse_py_graph_pipeline_query_dict(py, dict);
    }
    if value.hasattr("to_dict")? {
        let dict_value = value.call_method0("to_dict")?;
        let dict = dict_value.downcast::<PyDict>()?;
        return parse_py_graph_pipeline_query_dict(py, dict);
    }
    Err(PyTypeError::new_err(
        "graph pipeline request must be a dict or expose to_dict()",
    ))
}

fn parse_py_graph_pipeline_query_dict(
    py: Python<'_>,
    dict: &Bound<'_, PyDict>,
) -> PyResult<GraphPipelineQuery> {
    let options = parse_py_graph_pipeline_options(dict)?;
    let page = GraphPageRequest {
        skip: py_optional_query_usize(dict, "skip", "graph pipeline skip")?.unwrap_or(0),
        limit: py_optional_query_usize(dict, "limit", "graph pipeline limit")?
            .unwrap_or(options.max_rows),
        cursor: py_optional_extract(dict, "cursor")?,
    };
    if page.limit == 0 {
        return Err(PyValueError::new_err("graph pipeline limit must be > 0"));
    }
    let stages_value = py_non_none_item(dict, "stages")?
        .ok_or_else(|| PyValueError::new_err("graph pipeline request requires stages"))?;
    let stages_list = stages_value.downcast::<PyList>()?;
    let mut stages = Vec::with_capacity(stages_list.len());
    for (index, item) in stages_list.iter().enumerate() {
        stages.push(parse_py_graph_pipeline_stage(
            py,
            item.downcast::<PyDict>()?,
            &format!("graph pipeline stages[{index}]"),
        )?);
    }
    Ok(GraphPipelineQuery {
        stages,
        params: parse_py_graph_params(py, dict)?,
        at_epoch: py_optional_query_i64(dict, "at_epoch", "graph pipeline at_epoch")?,
        page,
        output: parse_py_graph_output_options(dict)?,
        options,
    })
}

fn parse_py_graph_pipeline_stage(
    py: Python<'_>,
    dict: &Bound<'_, PyDict>,
    context: &str,
) -> PyResult<GraphPipelineStage> {
    let kind: String = py_required_extract(dict, "kind")?;
    match kind.as_str() {
        "match" => Ok(GraphPipelineStage::Match(parse_py_graph_pipeline_match_stage(
            py, dict, context,
        )?)),
        "project" | "with" | "return" => Ok(GraphPipelineStage::Project(
            parse_py_graph_pipeline_project_stage(py, dict, &kind, context)?,
        )),
        "shortest_path" | "shortestPath" => Ok(GraphPipelineStage::ShortestPath(
            parse_py_graph_pipeline_shortest_path_stage(py, dict, context)?,
        )),
        "call" => Ok(GraphPipelineStage::Call(parse_py_graph_pipeline_call_stage(
            py, dict, context,
        )?)),
        "union" => Ok(GraphPipelineStage::Union(parse_py_graph_pipeline_union_stage(
            py, dict, context,
        )?)),
        other => Err(PyValueError::new_err(format!(
            "{context} kind must be 'match', 'project', 'with', 'return', 'shortest_path', 'call', or 'union', got '{other}'"
        ))),
    }
}

fn parse_py_graph_pipeline_match_stage(
    py: Python<'_>,
    dict: &Bound<'_, PyDict>,
    context: &str,
) -> PyResult<GraphPipelineMatchStage> {
    let mut nodes = Vec::new();
    if let Some(nodes_value) = py_non_none_item(dict, "nodes")? {
        let nodes_list = nodes_value.downcast::<PyList>()?;
        nodes.reserve(nodes_list.len());
        for (index, item) in nodes_list.iter().enumerate() {
            nodes.push(parse_py_graph_node_pattern(
                py,
                item.downcast::<PyDict>()?,
                &format!("{context} nodes[{index}]"),
            )?);
        }
    }
    let mut pieces = Vec::new();
    if let Some(pieces_value) = py_non_none_item(dict, "pieces")? {
        let pieces_list = pieces_value.downcast::<PyList>()?;
        pieces.reserve(pieces_list.len());
        for (index, item) in pieces_list.iter().enumerate() {
            pieces.push(parse_py_graph_pattern_piece(
                py,
                item.downcast::<PyDict>()?,
                &format!("{context} pieces[{index}]"),
            )?);
        }
    }
    Ok(GraphPipelineMatchStage {
        optional: py_optional_extract(dict, "optional")?.unwrap_or(false),
        nodes,
        pieces,
        where_: py_non_none_item(dict, "where")?
            .map(|value| parse_py_graph_expr(py, &value, &format!("{context} where")))
            .transpose()?,
        optional_candidate_where: py_non_none_item(dict, "optional_candidate_where")?
            .map(|value| {
                parse_py_graph_expr(py, &value, &format!("{context} optional_candidate_where"))
            })
            .transpose()?,
    })
}

fn parse_py_graph_pipeline_project_stage(
    py: Python<'_>,
    dict: &Bound<'_, PyDict>,
    kind: &str,
    context: &str,
) -> PyResult<GraphProjectStage> {
    let project_kind = match kind {
        "with" => GraphProjectKind::With,
        "return" => GraphProjectKind::Return,
        _ => match py_non_none_item(dict, "project_kind")? {
            None => GraphProjectKind::Return,
            Some(value) => match value.extract::<String>()?.as_str() {
                "with" => GraphProjectKind::With,
                "return" => GraphProjectKind::Return,
                other => {
                    return Err(PyValueError::new_err(format!(
                        "{context} project_kind must be 'with' or 'return', got '{other}'"
                    )));
                }
            },
        },
    };
    Ok(GraphProjectStage {
        kind: project_kind,
        items: parse_py_graph_projection_items(py, dict, context)?,
        distinct: py_optional_extract(dict, "distinct")?.unwrap_or(false),
        where_: py_non_none_item(dict, "where")?
            .map(|value| parse_py_graph_expr(py, &value, &format!("{context} where")))
            .transpose()?,
        order_by: parse_py_graph_order_items(py, dict)?,
        skip: py_non_none_item(dict, "skip")?
            .map(|value| parse_py_graph_expr(py, &value, &format!("{context} skip")))
            .transpose()?,
        limit: py_non_none_item(dict, "limit")?
            .map(|value| parse_py_graph_expr(py, &value, &format!("{context} limit")))
            .transpose()?,
    })
}

fn parse_py_graph_projection_items(
    py: Python<'_>,
    dict: &Bound<'_, PyDict>,
    context: &str,
) -> PyResult<GraphProjectionItems> {
    let Some(value) = py_non_none_item(dict, "items")? else {
        return Ok(GraphProjectionItems::Star);
    };
    if let Ok(name) = value.extract::<String>() {
        if name == "star" || name == "*" {
            return Ok(GraphProjectionItems::Star);
        }
        return Err(PyValueError::new_err(format!(
            "{context} items string must be 'star' or '*'"
        )));
    }
    let items = value.downcast::<PyList>()?;
    let mut parsed = Vec::with_capacity(items.len());
    for (index, item) in items.iter().enumerate() {
        let item = item.downcast::<PyDict>()?;
        let item_context = format!("{context} items[{index}]");
        let expr_value = py_non_none_item(item, "expr")?
            .ok_or_else(|| PyValueError::new_err(format!("{item_context} requires expr")))?;
        parsed.push(GraphProjectItem {
            expr: parse_py_graph_expr(py, &expr_value, &format!("{item_context} expr"))?,
            alias: parse_py_graph_return_alias(item, &item_context)?,
            projection: parse_py_graph_return_projection(item, &item_context)?,
        });
    }
    Ok(GraphProjectionItems::Items(parsed))
}

fn parse_py_graph_pipeline_union_stage(
    py: Python<'_>,
    dict: &Bound<'_, PyDict>,
    context: &str,
) -> PyResult<GraphUnionStage> {
    let branches_value = py_non_none_item(dict, "branches")?
        .ok_or_else(|| PyValueError::new_err(format!("{context} requires branches")))?;
    let branches_list = branches_value.downcast::<PyList>()?;
    let mut branches = Vec::with_capacity(branches_list.len());
    for (index, item) in branches_list.iter().enumerate() {
        branches.push(
            parse_py_graph_pipeline_query(py, &item).map_err(|err| {
                PyValueError::new_err(format!("{context} branches[{index}]: {err}"))
            })?,
        );
    }
    Ok(GraphUnionStage {
        branches,
        all: py_optional_extract(dict, "all")?.unwrap_or(false),
    })
}

fn parse_py_graph_pipeline_call_stage(
    py: Python<'_>,
    dict: &Bound<'_, PyDict>,
    context: &str,
) -> PyResult<GraphSubqueryStage> {
    let query_value = py_non_none_item(dict, "query")?
        .ok_or_else(|| PyValueError::new_err(format!("{context} requires query")))?;
    Ok(GraphSubqueryStage {
        query: Box::new(parse_py_graph_pipeline_query(py, &query_value)?),
        import_aliases: py_optional_extract::<Vec<String>>(dict, "import_aliases")?
            .unwrap_or_default(),
    })
}

fn parse_py_graph_pipeline_shortest_path_stage(
    py: Python<'_>,
    dict: &Bound<'_, PyDict>,
    context: &str,
) -> PyResult<GraphShortestPathStage> {
    let direction = match py_non_none_item(dict, "direction")? {
        None => Direction::Outgoing,
        Some(value) => parse_direction(&value.extract::<String>()?)?,
    };
    let mode = match py_non_none_item(dict, "mode")? {
        None => GraphShortestPathMode::One,
        Some(value) => match value.extract::<String>()?.as_str() {
            "one" => GraphShortestPathMode::One,
            "all" => GraphShortestPathMode::All,
            other => {
                return Err(PyValueError::new_err(format!(
                    "{context} mode must be 'one' or 'all', got '{other}'"
                )));
            }
        },
    };
    let max_cost = py_non_none_item(dict, "max_cost")?
        .map(|value| {
            let cost: f64 = value.extract()?;
            if !cost.is_finite() {
                return Err(PyValueError::new_err(format!(
                    "{context} max_cost must be finite"
                )));
            }
            Ok(cost)
        })
        .transpose()?;
    Ok(GraphShortestPathStage {
        optional: py_optional_extract(dict, "optional")?.unwrap_or(false),
        output_path_alias: py_required_extract(dict, "output_path_alias")?,
        mode,
        from: parse_py_shortest_path_endpoint(
            py,
            &py_non_none_item(dict, "from")?
                .ok_or_else(|| PyValueError::new_err(format!("{context} requires from")))?,
            &format!("{context} from"),
        )?,
        to: parse_py_shortest_path_endpoint(
            py,
            &py_non_none_item(dict, "to")?
                .ok_or_else(|| PyValueError::new_err(format!("{context} requires to")))?,
            &format!("{context} to"),
        )?,
        direction,
        edge_label_filter: py_optional_extract::<Vec<String>>(dict, "edge_label_filter")?
            .unwrap_or_default(),
        min_hops: py_optional_query_u8(dict, "min_hops", &format!("{context} min_hops"))?
            .ok_or_else(|| PyValueError::new_err(format!("{context} requires min_hops")))?,
        max_hops: py_optional_query_u8(dict, "max_hops", &format!("{context} max_hops"))?
            .ok_or_else(|| PyValueError::new_err(format!("{context} requires max_hops")))?,
        weight_field: py_optional_extract(dict, "weight_field")?,
        max_cost,
        max_paths: py_optional_query_usize(dict, "max_paths", &format!("{context} max_paths"))?,
    })
}

fn parse_py_shortest_path_endpoint(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    context: &str,
) -> PyResult<GraphShortestPathEndpoint> {
    if let Ok(alias) = value.extract::<String>() {
        return Ok(GraphShortestPathEndpoint::Alias(alias));
    }
    if let Ok(id) = value.extract::<u64>() {
        return Ok(GraphShortestPathEndpoint::NodeId(id));
    }
    let dict = value
        .downcast::<PyDict>()
        .map_err(|_| PyTypeError::new_err(format!("{context} must be an endpoint")))?;
    let discriminants = ["alias", "node_id", "node_key", "expr"];
    let present = discriminants
        .iter()
        .map(|field| py_has_field(dict, field))
        .collect::<PyResult<Vec<_>>>()?
        .into_iter()
        .filter(|value| *value)
        .count();
    if present != 1 {
        return Err(PyValueError::new_err(format!(
            "{context} must contain exactly one of alias, node_id, node_key, or expr"
        )));
    }
    if let Some(value) = py_non_none_item(dict, "alias")? {
        return Ok(GraphShortestPathEndpoint::Alias(value.extract()?));
    }
    if let Some(value) = py_non_none_item(dict, "node_id")? {
        return Ok(GraphShortestPathEndpoint::NodeId(value.extract()?));
    }
    if let Some(value) = py_non_none_item(dict, "node_key")? {
        let payload = value.downcast::<PyDict>()?;
        return Ok(GraphShortestPathEndpoint::NodeKey {
            label: py_required_extract(payload, "label")?,
            key: py_required_extract(payload, "key")?,
        });
    }
    let expr = py_non_none_item(dict, "expr")?
        .ok_or_else(|| PyValueError::new_err(format!("{context} requires expr")))?;
    Ok(GraphShortestPathEndpoint::Expr(parse_py_graph_expr(
        py,
        &expr,
        &format!("{context} expr"),
    )?))
}

fn parse_py_graph_pipeline_options(dict: &Bound<'_, PyDict>) -> PyResult<GraphPipelineOptions> {
    let mut options = GraphPipelineOptions::default();
    let Some(value) = py_non_none_item(dict, "options")? else {
        return Ok(options);
    };
    let options_dict = value.downcast::<PyDict>()?;
    if let Some(value) = py_optional_extract(options_dict, "allow_full_scan")? {
        options.allow_full_scan = value;
    }
    if let Some(value) = py_optional_query_usize(options_dict, "max_rows", "max_rows")? {
        options.max_rows = value;
    }
    if let Some(value) =
        py_optional_query_usize(options_dict, "max_pipeline_rows", "max_pipeline_rows")?
    {
        options.max_pipeline_rows = value;
    }
    if let Some(value) = py_optional_query_usize(options_dict, "max_groups", "max_groups")? {
        options.max_groups = value;
    }
    if let Some(value) =
        py_optional_query_usize(options_dict, "max_collect_items", "max_collect_items")?
    {
        options.max_collect_items = value;
    }
    if let Some(value) =
        py_optional_query_usize(options_dict, "max_union_branches", "max_union_branches")?
    {
        options.max_union_branches = value;
    }
    if let Some(value) = py_optional_query_usize(
        options_dict,
        "max_subquery_invocations",
        "max_subquery_invocations",
    )? {
        options.max_subquery_invocations = value;
    }
    if let Some(value) =
        py_optional_query_usize(options_dict, "max_subquery_depth", "max_subquery_depth")?
    {
        options.max_subquery_depth = value;
    }
    if let Some(value) = py_optional_query_usize(
        options_dict,
        "max_shortest_path_pairs",
        "max_shortest_path_pairs",
    )? {
        options.max_shortest_path_pairs = value;
    }
    if let Some(value) = py_optional_query_usize(
        options_dict,
        "max_intermediate_bindings",
        "max_intermediate_bindings",
    )? {
        options.max_intermediate_bindings = value;
    }
    if let Some(value) = py_optional_query_usize(options_dict, "max_frontier", "max_frontier")? {
        options.max_frontier = value;
    }
    if let Some(value) = py_optional_query_u8(options_dict, "max_path_hops", "max_path_hops")? {
        options.max_path_hops = value;
    }
    if let Some(value) =
        py_optional_query_usize(options_dict, "max_paths_per_start", "max_paths_per_start")?
    {
        options.max_paths_per_start = value;
    }
    if let Some(value) = py_optional_query_usize(
        options_dict,
        "max_order_materialization",
        "max_order_materialization",
    )? {
        options.max_order_materialization = value;
    }
    if let Some(value) = py_optional_query_usize(options_dict, "max_skip", "max_skip")? {
        options.max_skip = value;
    }
    if let Some(value) =
        py_optional_query_usize(options_dict, "max_cursor_bytes", "max_cursor_bytes")?
    {
        options.max_cursor_bytes = value;
    }
    if let Some(value) =
        py_optional_query_usize(options_dict, "max_query_bytes", "max_query_bytes")?
    {
        options.max_query_bytes = value;
    }
    if let Some(value) =
        py_optional_query_usize(options_dict, "max_param_bytes", "max_param_bytes")?
    {
        options.max_param_bytes = value;
    }
    if let Some(value) = py_optional_query_usize(options_dict, "max_ast_depth", "max_ast_depth")? {
        options.max_ast_depth = value;
    }
    if let Some(value) =
        py_optional_query_usize(options_dict, "max_literal_items", "max_literal_items")?
    {
        options.max_literal_items = value;
    }
    if let Some(value) = py_optional_extract(options_dict, "include_plan")? {
        options.include_plan = value;
    }
    if let Some(value) = py_optional_extract(options_dict, "profile")? {
        options.profile = value;
    }
    Ok(options)
}

fn parse_py_graph_node_pattern(
    py: Python<'_>,
    dict: &Bound<'_, PyDict>,
    context: &str,
) -> PyResult<GraphNodePattern> {
    reject_py_legacy_node_label_field(dict, "node pattern")?;
    Ok(GraphNodePattern {
        alias: py_required_extract(dict, "alias")?,
        label_filter: parse_optional_node_label_filter_field(
            dict,
            "label_filter",
            &format!("{context} label_filter"),
        )?,
        ids: py_optional_query_u64_vec(dict, "ids", &format!("{context} ids"))?,
        keys: parse_py_node_key_queries(dict, context)?,
        filter: parse_py_node_filter(py, dict, "updated_at", context)?,
    })
}

fn parse_py_graph_pattern_piece(
    py: Python<'_>,
    dict: &Bound<'_, PyDict>,
    context: &str,
) -> PyResult<GraphPatternPiece> {
    let kind: String = py_required_extract(dict, "kind")?;
    match kind.as_str() {
        "edge" => Ok(GraphPatternPiece::Edge(parse_py_graph_edge_pattern(
            py, dict, context,
        )?)),
        "optional" => Ok(GraphPatternPiece::Optional(parse_py_graph_optional_group(
            py, dict, context,
        )?)),
        "variable_length" => Ok(GraphPatternPiece::VariableLength(
            parse_py_graph_variable_length_pattern(py, dict, context)?,
        )),
        other => Err(PyValueError::new_err(format!(
            "{context} kind must be 'edge', 'optional', or 'variable_length', got '{other}'"
        ))),
    }
}

fn parse_py_graph_edge_pattern(
    py: Python<'_>,
    dict: &Bound<'_, PyDict>,
    context: &str,
) -> PyResult<GraphEdgePattern> {
    reject_py_legacy_node_predicate_fields(dict, context)?;
    let direction = match py_non_none_item(dict, "direction")? {
        None => Direction::Outgoing,
        Some(value) => parse_direction(&value.extract::<String>()?)?,
    };
    Ok(GraphEdgePattern {
        alias: py_optional_extract(dict, "alias")?,
        from_alias: parse_py_alias_field(dict, &["from", "from_alias"], context)?,
        to_alias: parse_py_alias_field(dict, &["to", "to_alias"], context)?,
        direction,
        label_filter: parse_py_graph_edge_labels(dict, context)?,
        filter: parse_py_edge_filter(
            py,
            dict,
            "updated_at",
            "valid_at",
            "valid_from",
            "valid_to",
            context,
        )?,
    })
}

fn parse_py_graph_optional_group(
    py: Python<'_>,
    dict: &Bound<'_, PyDict>,
    context: &str,
) -> PyResult<eg::GraphOptionalGroup> {
    let pieces_value = py_non_none_item(dict, "pieces")?
        .ok_or_else(|| PyValueError::new_err(format!("{context} requires pieces")))?;
    let pieces_list = pieces_value.downcast::<PyList>()?;
    let mut pieces = Vec::with_capacity(pieces_list.len());
    for (index, item) in pieces_list.iter().enumerate() {
        pieces.push(parse_py_graph_pattern_piece(
            py,
            item.downcast::<PyDict>()?,
            &format!("{context} pieces[{index}]"),
        )?);
    }
    Ok(eg::GraphOptionalGroup {
        pieces,
        where_: py_non_none_item(dict, "where")?
            .map(|value| parse_py_graph_expr(py, &value, &format!("{context} where")))
            .transpose()?,
    })
}

fn parse_py_graph_variable_length_pattern(
    py: Python<'_>,
    dict: &Bound<'_, PyDict>,
    context: &str,
) -> PyResult<eg::GraphVariableLengthPattern> {
    reject_py_legacy_node_predicate_fields(dict, context)?;
    let direction = match py_non_none_item(dict, "direction")? {
        None => Direction::Outgoing,
        Some(value) => parse_direction(&value.extract::<String>()?)?,
    };
    Ok(eg::GraphVariableLengthPattern {
        path_alias: py_optional_extract(dict, "path_alias")?,
        edge_alias: py_optional_extract(dict, "edge_alias")?,
        from_alias: parse_py_alias_field(dict, &["from", "from_alias"], context)?,
        to_alias: parse_py_alias_field(dict, &["to", "to_alias"], context)?,
        direction,
        label_filter: parse_py_graph_edge_labels(dict, context)?,
        filter: parse_py_edge_filter(
            py,
            dict,
            "updated_at",
            "valid_at",
            "valid_from",
            "valid_to",
            context,
        )?,
        min_hops: py_query_u8(
            &py_non_none_item(dict, "min_hops")?
                .ok_or_else(|| PyValueError::new_err(format!("{context} requires min_hops")))?,
            &format!("{context} min_hops"),
        )?,
        max_hops: py_query_u8(
            &py_non_none_item(dict, "max_hops")?
                .ok_or_else(|| PyValueError::new_err(format!("{context} requires max_hops")))?,
            &format!("{context} max_hops"),
        )?,
    })
}

fn parse_py_node_key_queries(
    dict: &Bound<'_, PyDict>,
    context: &str,
) -> PyResult<Vec<NodeKeyQuery>> {
    let Some(value) = py_non_none_item(dict, "keys")? else {
        return Ok(Vec::new());
    };
    let items = value.downcast::<PyList>()?;
    let mut parsed = Vec::with_capacity(items.len());
    for (index, item) in items.iter().enumerate() {
        let item_context = format!("{context} keys[{index}]");
        if let Ok(key) = item.extract::<String>() {
            let label_filter = parse_optional_node_label_filter_field(
                dict,
                "label_filter",
                &format!("{context} label_filter"),
            )?;
            let Some(NodeLabelFilter {
                labels,
                mode: LabelMatchMode::All,
            }) = label_filter
            else {
                return Err(PyValueError::new_err(format!(
                    "{item_context} string form requires label_filter with exactly one all-mode label"
                )));
            };
            if labels.len() != 1 {
                return Err(PyValueError::new_err(format!(
                    "{item_context} string form requires exactly one label"
                )));
            }
            parsed.push(NodeKeyQuery {
                label: labels[0].clone(),
                key,
            });
            continue;
        }
        let dict = item.downcast::<PyDict>()?;
        parsed.push(NodeKeyQuery {
            label: py_required_extract(dict, "label")?,
            key: py_required_extract(dict, "key")?,
        });
    }
    Ok(parsed)
}

fn parse_py_alias_field(
    dict: &Bound<'_, PyDict>,
    keys: &[&str],
    context: &str,
) -> PyResult<String> {
    let mut found: Option<String> = None;
    for key in keys {
        if let Some(value) = py_non_none_item(dict, key)? {
            if found.is_some() {
                return Err(PyValueError::new_err(format!(
                    "{context} accepts only one of {}",
                    keys.join(", ")
                )));
            }
            found = Some(value.extract()?);
        }
    }
    found.ok_or_else(|| PyValueError::new_err(format!("{context} requires {}", keys.join(" or "))))
}

fn parse_py_graph_edge_labels(dict: &Bound<'_, PyDict>, context: &str) -> PyResult<Vec<String>> {
    let has_labels = py_has_field(dict, "labels")?;
    let has_label_filter = py_has_field(dict, "label_filter")?;
    if has_labels && has_label_filter {
        return Err(PyValueError::new_err(format!(
            "{context} accepts only one of labels or label_filter"
        )));
    }
    if has_labels {
        py_optional_string_vec(dict, "labels", &format!("{context} labels"))
            .map(|value| value.unwrap_or_default())
    } else {
        py_optional_string_vec(dict, "label_filter", &format!("{context} label_filter"))
            .map(|value| value.unwrap_or_default())
    }
}

fn parse_py_graph_return_items(
    py: Python<'_>,
    dict: &Bound<'_, PyDict>,
) -> PyResult<Option<Vec<GraphReturnItem>>> {
    let Some(value) = py_non_none_item(dict, "return")? else {
        return Ok(None);
    };
    let items = value.downcast::<PyList>()?;
    let mut parsed = Vec::with_capacity(items.len());
    for (index, item) in items.iter().enumerate() {
        let item = item.downcast::<PyDict>()?;
        let context = format!("graph row return[{index}]");
        let expr_value = py_non_none_item(item, "expr")?
            .ok_or_else(|| PyValueError::new_err(format!("{context} requires expr")))?;
        let alias = parse_py_graph_return_alias(item, &context)?;
        parsed.push(GraphReturnItem {
            expr: parse_py_graph_expr(py, &expr_value, &format!("{context} expr"))?,
            alias,
            projection: parse_py_graph_return_projection(item, &context)?,
        });
    }
    Ok(Some(parsed))
}

fn parse_py_graph_return_alias(
    dict: &Bound<'_, PyDict>,
    context: &str,
) -> PyResult<Option<String>> {
    let as_alias = py_non_none_item(dict, "as")?
        .map(|value| {
            value
                .extract::<String>()
                .map_err(|_| PyTypeError::new_err(format!("{context} as alias must be a string")))
        })
        .transpose()?;
    let alias = py_non_none_item(dict, "alias")?
        .map(|value| {
            value
                .extract::<String>()
                .map_err(|_| PyTypeError::new_err(format!("{context} alias must be a string")))
        })
        .transpose()?;
    match (as_alias, alias) {
        (Some(_), Some(_)) => Err(PyValueError::new_err(format!(
            "{context} accepts only one of 'as' or 'alias'"
        ))),
        (Some(value), None) | (None, Some(value)) => Ok(Some(value)),
        (None, None) => Ok(None),
    }
}

fn parse_py_graph_return_projection(
    dict: &Bound<'_, PyDict>,
    context: &str,
) -> PyResult<GraphReturnProjection> {
    let Some(value) = py_non_none_item(dict, "projection")? else {
        return Ok(GraphReturnProjection::Auto);
    };
    if let Ok(name) = value.extract::<String>() {
        return parse_py_graph_projection_name(&name, context);
    }
    let projection = value.downcast::<PyDict>()?;
    let discriminants = ["element", "selected"]
        .iter()
        .map(|field| py_has_field(projection, field))
        .collect::<PyResult<Vec<_>>>()?
        .into_iter()
        .filter(|present| *present)
        .count();
    if discriminants != 1 {
        return Err(PyValueError::new_err(format!(
            "{context} projection object must contain exactly one of 'element' or 'selected'"
        )));
    }
    if let Some(element) = projection.get_item("element")? {
        let name: String = element.extract()?;
        return Ok(GraphReturnProjection::Element(
            parse_py_graph_element_projection(&name, context)?,
        ));
    }
    if let Some(selected) = projection.get_item("selected")? {
        return Ok(GraphReturnProjection::Selected(
            parse_py_graph_selected_projection(selected.downcast::<PyDict>()?, context)?,
        ));
    }
    Err(PyValueError::new_err(format!(
        "{context} projection must be 'auto', 'id', 'compact', 'full', or a selected projection"
    )))
}

fn parse_py_graph_projection_name(name: &str, context: &str) -> PyResult<GraphReturnProjection> {
    match name {
        "auto" => Ok(GraphReturnProjection::Auto),
        "id" | "id_only" => Ok(GraphReturnProjection::IdOnly),
        "element_id" => Ok(GraphReturnProjection::Element(
            GraphElementProjection::IdOnly,
        )),
        "compact" => Ok(GraphReturnProjection::Element(
            GraphElementProjection::Compact,
        )),
        "full" | "element" => Ok(GraphReturnProjection::Element(GraphElementProjection::Full)),
        other => Err(PyValueError::new_err(format!(
            "{context} projection has unsupported value '{other}'"
        ))),
    }
}

fn parse_py_graph_element_projection(
    name: &str,
    context: &str,
) -> PyResult<GraphElementProjection> {
    match name {
        "id" | "id_only" => Ok(GraphElementProjection::IdOnly),
        "compact" => Ok(GraphElementProjection::Compact),
        "full" | "element" => Ok(GraphElementProjection::Full),
        other => Err(PyValueError::new_err(format!(
            "{context} element projection has unsupported value '{other}'"
        ))),
    }
}

fn parse_py_graph_selected_projection(
    dict: &Bound<'_, PyDict>,
    context: &str,
) -> PyResult<GraphSelectedProjection> {
    let discriminants = ["node", "edge", "path"]
        .iter()
        .map(|field| py_has_field(dict, field))
        .collect::<PyResult<Vec<_>>>()?
        .into_iter()
        .filter(|present| *present)
        .count();
    if discriminants != 1 {
        return Err(PyValueError::new_err(format!(
            "{context} selected projection must contain exactly one of 'node', 'edge', or 'path'"
        )));
    }
    if let Some(node) = py_non_none_item(dict, "node")? {
        return Ok(GraphSelectedProjection::Node(
            parse_py_selected_node_projection(node.downcast::<PyDict>()?)?,
        ));
    }
    if let Some(edge) = py_non_none_item(dict, "edge")? {
        return Ok(GraphSelectedProjection::Edge(
            parse_py_selected_edge_projection(edge.downcast::<PyDict>()?)?,
        ));
    }
    if let Some(path) = py_non_none_item(dict, "path")? {
        return Ok(GraphSelectedProjection::Path(
            parse_py_selected_path_projection(path.downcast::<PyDict>()?)?,
        ));
    }
    Err(PyValueError::new_err(format!(
        "{context} selected projection requires node, edge, or path"
    )))
}

fn parse_py_selected_node_projection(
    dict: &Bound<'_, PyDict>,
) -> PyResult<GraphSelectedNodeProjection> {
    ensure_only_py_fields(
        dict,
        &[
            "id",
            "labels",
            "key",
            "props",
            "weight",
            "created_at",
            "updated_at",
            "vectors",
        ],
        "selected node projection",
    )?;
    Ok(GraphSelectedNodeProjection {
        id: py_optional_extract(dict, "id")?.unwrap_or(false),
        labels: py_optional_extract(dict, "labels")?.unwrap_or(false),
        key: py_optional_extract(dict, "key")?.unwrap_or(false),
        props: parse_py_property_selection(dict)?,
        weight: py_optional_extract(dict, "weight")?.unwrap_or(false),
        created_at: py_optional_extract(dict, "created_at")?.unwrap_or(false),
        updated_at: py_optional_extract(dict, "updated_at")?.unwrap_or(false),
        vectors: parse_py_vector_selection(dict)?,
    })
}

fn parse_py_selected_edge_projection(
    dict: &Bound<'_, PyDict>,
) -> PyResult<GraphSelectedEdgeProjection> {
    ensure_only_py_fields(
        dict,
        &[
            "id",
            "from",
            "from_id",
            "to",
            "to_id",
            "label",
            "props",
            "weight",
            "created_at",
            "updated_at",
            "valid_from",
            "valid_to",
        ],
        "selected edge projection",
    )?;
    let from = parse_py_projection_bool_alias(dict, "from_id", "from", "selected edge projection")?;
    let to = parse_py_projection_bool_alias(dict, "to_id", "to", "selected edge projection")?;
    Ok(GraphSelectedEdgeProjection {
        id: py_optional_extract(dict, "id")?.unwrap_or(false),
        from,
        to,
        label: py_optional_extract(dict, "label")?.unwrap_or(false),
        props: parse_py_property_selection(dict)?,
        weight: py_optional_extract(dict, "weight")?.unwrap_or(false),
        created_at: py_optional_extract(dict, "created_at")?.unwrap_or(false),
        updated_at: py_optional_extract(dict, "updated_at")?.unwrap_or(false),
        valid_from: py_optional_extract(dict, "valid_from")?.unwrap_or(false),
        valid_to: py_optional_extract(dict, "valid_to")?.unwrap_or(false),
    })
}

fn parse_py_projection_bool_alias(
    dict: &Bound<'_, PyDict>,
    primary: &str,
    alias: &str,
    context: &str,
) -> PyResult<bool> {
    let primary_value: Option<bool> = py_optional_extract(dict, primary)?;
    let alias_value: Option<bool> = py_optional_extract(dict, alias)?;
    match (primary_value, alias_value) {
        (Some(_), Some(_)) => Err(PyValueError::new_err(format!(
            "{context} accepts only one of '{primary}' or '{alias}'"
        ))),
        (Some(value), None) | (None, Some(value)) => Ok(value),
        (None, None) => Ok(false),
    }
}

fn parse_py_selected_path_projection(
    dict: &Bound<'_, PyDict>,
) -> PyResult<GraphSelectedPathProjection> {
    ensure_only_py_fields(
        dict,
        &["node_ids", "edge_ids", "nodes", "edges"],
        "selected path projection",
    )?;
    Ok(GraphSelectedPathProjection {
        node_ids: py_optional_extract(dict, "node_ids")?.unwrap_or(true),
        edge_ids: py_optional_extract(dict, "edge_ids")?.unwrap_or(true),
        nodes: py_non_none_item(dict, "nodes")?
            .map(|value| parse_py_selected_node_projection(value.downcast::<PyDict>()?))
            .transpose()?,
        edges: py_non_none_item(dict, "edges")?
            .map(|value| parse_py_selected_edge_projection(value.downcast::<PyDict>()?))
            .transpose()?,
    })
}

fn parse_py_property_selection(dict: &Bound<'_, PyDict>) -> PyResult<eg::GraphPropertySelection> {
    let Some(value) = py_non_none_item(dict, "props")? else {
        return Ok(eg::GraphPropertySelection::None);
    };
    if value.is_instance_of::<PyBool>() {
        return if value.extract::<bool>()? {
            Ok(eg::GraphPropertySelection::All)
        } else {
            Ok(eg::GraphPropertySelection::None)
        };
    }
    if let Ok(name) = value.extract::<String>() {
        return match name.as_str() {
            "all" => Ok(eg::GraphPropertySelection::All),
            "none" => Ok(eg::GraphPropertySelection::None),
            other => Err(PyValueError::new_err(format!(
                "props selection must be 'all', 'none', bool, or a list of keys, got '{other}'"
            ))),
        };
    }
    Ok(eg::GraphPropertySelection::Keys(
        value.extract::<Vec<String>>()?,
    ))
}

fn parse_py_vector_selection(dict: &Bound<'_, PyDict>) -> PyResult<GraphVectorSelection> {
    let Some(value) = py_non_none_item(dict, "vectors")? else {
        return Ok(GraphVectorSelection::None);
    };
    let name: String = value.extract()?;
    match name.as_str() {
        "none" => Ok(GraphVectorSelection::None),
        "dense" => Ok(GraphVectorSelection::Dense),
        "sparse" => Ok(GraphVectorSelection::Sparse),
        "both" | "all" => Ok(GraphVectorSelection::Both),
        other => Err(PyValueError::new_err(format!(
            "vectors selection must be 'none', 'dense', 'sparse', or 'both', got '{other}'"
        ))),
    }
}

fn parse_py_graph_order_items(
    py: Python<'_>,
    dict: &Bound<'_, PyDict>,
) -> PyResult<Vec<GraphOrderItem>> {
    let Some(value) = py_non_none_item(dict, "order_by")? else {
        return Ok(Vec::new());
    };
    let items = value.downcast::<PyList>()?;
    let mut parsed = Vec::with_capacity(items.len());
    for (index, item) in items.iter().enumerate() {
        let item = item.downcast::<PyDict>()?;
        let context = format!("graph row order_by[{index}]");
        let expr_value = py_non_none_item(item, "expr")?
            .ok_or_else(|| PyValueError::new_err(format!("{context} requires expr")))?;
        let direction = match py_non_none_item(item, "direction")? {
            None => GraphOrderDirection::Asc,
            Some(value) => match value.extract::<String>()?.as_str() {
                "asc" => GraphOrderDirection::Asc,
                "desc" => GraphOrderDirection::Desc,
                other => {
                    return Err(PyValueError::new_err(format!(
                        "{context} direction must be 'asc' or 'desc', got '{other}'"
                    )));
                }
            },
        };
        parsed.push(GraphOrderItem {
            expr: parse_py_graph_expr(py, &expr_value, &format!("{context} expr"))?,
            direction,
        });
    }
    Ok(parsed)
}

fn parse_py_graph_output_options(dict: &Bound<'_, PyDict>) -> PyResult<GraphOutputOptions> {
    let Some(value) = py_non_none_item(dict, "output")? else {
        return Ok(GraphOutputOptions::default());
    };
    let output = value.downcast::<PyDict>()?;
    let mut parsed = GraphOutputOptions::default();
    if let Some(mode) = py_non_none_item(output, "mode")? {
        parsed.mode = match mode.extract::<String>()?.as_str() {
            "ids" => GraphOutputMode::Ids,
            "elements" => GraphOutputMode::Elements,
            "projected" => GraphOutputMode::Projected,
            other => {
                return Err(PyValueError::new_err(format!(
                    "graph row output mode must be 'ids', 'elements', or 'projected', got '{other}'"
                )));
            }
        };
    }
    if let Some(compact_rows) = py_optional_extract(output, "compact_rows")? {
        parsed.compact_rows = compact_rows;
    }
    if let Some(include_vectors) = py_optional_extract(output, "include_vectors")? {
        parsed.include_vectors = include_vectors;
    }
    Ok(parsed)
}

fn parse_py_graph_query_options(dict: &Bound<'_, PyDict>) -> PyResult<GraphQueryOptions> {
    let mut options = GraphQueryOptions::default();
    if let Some(value) = py_non_none_item(dict, "options")? {
        let options_dict = value.downcast::<PyDict>()?;
        if let Some(value) = py_optional_extract(options_dict, "allow_full_scan")? {
            options.allow_full_scan = value;
        }
        if let Some(value) = py_optional_query_usize(
            options_dict,
            "max_intermediate_bindings",
            "max_intermediate_bindings",
        )? {
            options.max_intermediate_bindings = value;
        }
        if let Some(value) = py_optional_query_usize(options_dict, "max_frontier", "max_frontier")?
        {
            options.max_frontier = value;
        }
        if let Some(value) = py_optional_query_u8(options_dict, "max_path_hops", "max_path_hops")? {
            options.max_path_hops = value;
        }
        if let Some(value) =
            py_optional_query_usize(options_dict, "max_paths_per_start", "max_paths_per_start")?
        {
            options.max_paths_per_start = value;
        }
        if let Some(value) =
            py_optional_query_usize(options_dict, "max_page_limit", "max_page_limit")?
        {
            options.max_page_limit = value;
        }
        if let Some(value) = py_optional_query_usize(
            options_dict,
            "max_order_materialization",
            "max_order_materialization",
        )? {
            options.max_order_materialization = value;
        }
        if let Some(value) =
            py_optional_query_usize(options_dict, "max_cursor_bytes", "max_cursor_bytes")?
        {
            options.max_cursor_bytes = value;
        }
        if let Some(value) =
            py_optional_query_usize(options_dict, "max_query_bytes", "max_query_bytes")?
        {
            options.max_query_bytes = value;
        }
        if let Some(value) = py_optional_extract(options_dict, "include_plan")? {
            options.include_plan = value;
        }
        if let Some(value) = py_optional_extract(options_dict, "profile")? {
            options.profile = value;
        }
    }
    Ok(options)
}

fn parse_py_graph_params(
    py: Python<'_>,
    dict: &Bound<'_, PyDict>,
) -> PyResult<BTreeMap<String, GraphParamValue>> {
    let Some(value) = py_non_none_item(dict, "params")? else {
        return Ok(BTreeMap::new());
    };
    let params = value.downcast::<PyDict>()?;
    let mut parsed = BTreeMap::new();
    for (key, value) in params.iter() {
        let key: String = key.extract()?;
        parsed.insert(key.clone(), py_to_graph_param_value(py, &value, &key)?);
    }
    Ok(parsed)
}

#[allow(clippy::only_used_in_recursion)]
fn py_to_graph_param_value(
    py: Python<'_>,
    obj: &Bound<'_, PyAny>,
    context: &str,
) -> PyResult<GraphParamValue> {
    if obj.is_none() {
        Ok(GraphParamValue::Null)
    } else if obj.is_instance_of::<PyBool>() {
        Ok(GraphParamValue::Bool(obj.extract()?))
    } else if let Ok(i) = obj.extract::<i64>() {
        if i < 0 {
            Ok(GraphParamValue::Int(i))
        } else {
            Ok(GraphParamValue::UInt(i as u64))
        }
    } else if let Ok(u) = obj.extract::<u64>() {
        Ok(GraphParamValue::UInt(u))
    } else if let Ok(f) = obj.extract::<f64>() {
        if !f.is_finite() {
            return Err(PyValueError::new_err(format!(
                "graph row param '{context}' must be finite"
            )));
        }
        Ok(GraphParamValue::Float(f))
    } else if let Ok(bytes) = obj.downcast::<PyBytes>() {
        Ok(GraphParamValue::Bytes(bytes.as_bytes().to_vec()))
    } else if let Ok(string) = obj.downcast::<PyString>() {
        Ok(GraphParamValue::String(string.to_str()?.to_string()))
    } else if let Ok(list) = obj.downcast::<PyList>() {
        let values = list
            .iter()
            .map(|item| py_to_graph_param_value(py, &item, context))
            .collect::<PyResult<Vec<_>>>()?;
        Ok(GraphParamValue::List(values))
    } else if let Ok(tuple) = obj.downcast::<PyTuple>() {
        let values = tuple
            .iter()
            .map(|item| py_to_graph_param_value(py, &item, context))
            .collect::<PyResult<Vec<_>>>()?;
        Ok(GraphParamValue::List(values))
    } else if let Ok(dict) = obj.downcast::<PyDict>() {
        if let Some(bytes) = parse_py_tagged_bytes(dict, context)? {
            return Ok(GraphParamValue::Bytes(bytes));
        }
        if let Some(list) = py_non_none_item(dict, "list")? {
            ensure_only_py_fields(dict, &["list"], context)?;
            let items = list.downcast::<PyList>()?;
            let values = items
                .iter()
                .map(|item| py_to_graph_param_value(py, &item, context))
                .collect::<PyResult<Vec<_>>>()?;
            return Ok(GraphParamValue::List(values));
        }
        if let Some(map) = py_non_none_item(dict, "map")? {
            ensure_only_py_fields(dict, &["map"], context)?;
            let map = map.downcast::<PyDict>()?;
            let mut parsed = BTreeMap::new();
            for (key, value) in map.iter() {
                let key: String = key.extract()?;
                parsed.insert(key.clone(), py_to_graph_param_value(py, &value, &key)?);
            }
            return Ok(GraphParamValue::Map(parsed));
        }
        let mut parsed = BTreeMap::new();
        for (key, value) in dict.iter() {
            let key: String = key.extract()?;
            parsed.insert(key.clone(), py_to_graph_param_value(py, &value, &key)?);
        }
        Ok(GraphParamValue::Map(parsed))
    } else {
        Err(PyTypeError::new_err(format!(
            "Unsupported graph row param value type: {}",
            obj.get_type().name()?
        )))
    }
}

fn parse_py_graph_expr(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    context: &str,
) -> PyResult<GraphExpr> {
    if value.is_none() {
        return Ok(GraphExpr::Null);
    }
    if value.is_instance_of::<PyBool>() {
        return Ok(GraphExpr::Bool(value.extract()?));
    }
    if let Ok(i) = value.extract::<i64>() {
        return if i < 0 {
            Ok(GraphExpr::Int(i))
        } else {
            Ok(GraphExpr::UInt(i as u64))
        };
    }
    if let Ok(u) = value.extract::<u64>() {
        return Ok(GraphExpr::UInt(u));
    }
    if let Ok(f) = value.extract::<f64>() {
        if !f.is_finite() {
            return Err(PyValueError::new_err(format!("{context} must be finite")));
        }
        return Ok(GraphExpr::Float(f));
    }
    if let Ok(bytes) = value.downcast::<PyBytes>() {
        return Ok(GraphExpr::Bytes(bytes.as_bytes().to_vec()));
    }
    if let Ok(string) = value.downcast::<PyString>() {
        return Ok(GraphExpr::String(string.to_str()?.to_string()));
    }
    let dict = value
        .downcast::<PyDict>()
        .map_err(|_| PyTypeError::new_err(format!("{context} must be a graph expression")))?;
    parse_py_graph_expr_dict(py, dict, context)
}

fn parse_py_graph_expr_dict(
    py: Python<'_>,
    dict: &Bound<'_, PyDict>,
    context: &str,
) -> PyResult<GraphExpr> {
    if dict.is_empty() {
        return Err(PyValueError::new_err(format!(
            "{context} expression object must not be empty"
        )));
    }
    let discriminants = [
        "bytes",
        "list",
        "map",
        "param",
        "binding",
        "property",
        "node_field",
        "edge_field",
        "path_field",
        "fn",
        "aggregate",
        "exists",
        "op",
        "case",
        "is_null",
        "is_not_null",
    ];
    let present = discriminants
        .iter()
        .map(|field| py_has_field(dict, field))
        .collect::<PyResult<Vec<_>>>()?
        .into_iter()
        .filter(|value| *value)
        .count();
    if present != 1 {
        return Err(PyValueError::new_err(format!(
            "{context} expression object must contain exactly one known discriminant"
        )));
    }
    if let Some(bytes) = parse_py_tagged_bytes(dict, context)? {
        ensure_only_py_fields(dict, &["bytes"], context)?;
        return Ok(GraphExpr::Bytes(bytes));
    }
    if let Some(value) = dict.get_item("list")? {
        ensure_only_py_fields(dict, &["list"], context)?;
        let items = value.downcast::<PyList>()?;
        let parsed = items
            .iter()
            .enumerate()
            .map(|(index, item)| {
                parse_py_graph_expr(py, &item, &format!("{context} list[{index}]"))
            })
            .collect::<PyResult<Vec<_>>>()?;
        return Ok(GraphExpr::List(parsed));
    }
    if let Some(value) = dict.get_item("map")? {
        ensure_only_py_fields(dict, &["map"], context)?;
        let items = value.downcast::<PyDict>()?;
        let mut parsed = BTreeMap::new();
        for (key, value) in items.iter() {
            let key: String = key.extract()?;
            parsed.insert(
                key.clone(),
                parse_py_graph_expr(py, &value, &format!("{context} map.{key}"))?,
            );
        }
        return Ok(GraphExpr::Map(parsed));
    }
    if let Some(value) = dict.get_item("param")? {
        ensure_only_py_fields(dict, &["param"], context)?;
        return Ok(GraphExpr::Param(value.extract()?));
    }
    if let Some(value) = dict.get_item("binding")? {
        ensure_only_py_fields(dict, &["binding"], context)?;
        return Ok(GraphExpr::Binding(value.extract()?));
    }
    if let Some(value) = dict.get_item("property")? {
        ensure_only_py_fields(dict, &["property"], context)?;
        let payload = value.downcast::<PyDict>()?;
        ensure_only_py_fields(payload, &["alias", "key"], context)?;
        return Ok(GraphExpr::Property {
            alias: py_required_extract(payload, "alias")?,
            key: py_required_extract(payload, "key")?,
        });
    }
    if let Some(value) = dict.get_item("node_field")? {
        ensure_only_py_fields(dict, &["node_field"], context)?;
        let payload = value.downcast::<PyDict>()?;
        ensure_only_py_fields(payload, &["alias", "field"], context)?;
        return Ok(GraphExpr::NodeField {
            alias: py_required_extract(payload, "alias")?,
            field: parse_py_graph_node_field(
                &py_required_extract::<String>(payload, "field")?,
                context,
            )?,
        });
    }
    if let Some(value) = dict.get_item("edge_field")? {
        ensure_only_py_fields(dict, &["edge_field"], context)?;
        let payload = value.downcast::<PyDict>()?;
        ensure_only_py_fields(payload, &["alias", "field"], context)?;
        return Ok(GraphExpr::EdgeField {
            alias: py_required_extract(payload, "alias")?,
            field: parse_py_graph_edge_field(
                &py_required_extract::<String>(payload, "field")?,
                context,
            )?,
        });
    }
    if let Some(value) = dict.get_item("path_field")? {
        ensure_only_py_fields(dict, &["path_field"], context)?;
        let payload = value.downcast::<PyDict>()?;
        ensure_only_py_fields(payload, &["alias", "field"], context)?;
        return Ok(GraphExpr::PathField {
            alias: py_required_extract(payload, "alias")?,
            field: parse_py_graph_path_field(
                &py_required_extract::<String>(payload, "field")?,
                context,
            )?,
        });
    }
    if let Some(value) = dict.get_item("fn")? {
        let name: String = value.extract()?;
        let args_value = dict
            .get_item("args")?
            .ok_or_else(|| PyValueError::new_err(format!("{context} function requires args")))?;
        ensure_only_py_fields(dict, &["fn", "args"], context)?;
        let args_list = args_value.downcast::<PyList>()?;
        let args = args_list
            .iter()
            .enumerate()
            .map(|(index, item)| {
                parse_py_graph_expr(py, &item, &format!("{context} args[{index}]"))
            })
            .collect::<PyResult<Vec<_>>>()?;
        return parse_py_graph_function_expr(name, args, context);
    }
    if let Some(value) = dict.get_item("aggregate")? {
        ensure_only_py_fields(dict, &["aggregate"], context)?;
        return parse_py_graph_aggregate_expr(py, value.downcast::<PyDict>()?, context);
    }
    if let Some(value) = dict.get_item("exists")? {
        ensure_only_py_fields(dict, &["exists"], context)?;
        return Ok(GraphExpr::ExistsSubquery(
            parse_py_graph_pipeline_call_stage(
                py,
                value.downcast::<PyDict>()?,
                &format!("{context} exists"),
            )?,
        ));
    }
    if let Some(value) = dict.get_item("op")? {
        let op: String = value.extract()?;
        if op == "not" {
            ensure_only_py_fields(dict, &["op", "expr"], context)?;
            let expr = dict
                .get_item("expr")?
                .ok_or_else(|| PyValueError::new_err(format!("{context} not requires expr")))?;
            return Ok(GraphExpr::Unary {
                op: GraphUnaryOp::Not,
                expr: Box::new(parse_py_graph_expr(py, &expr, &format!("{context} expr"))?),
            });
        }
        if op == "neg" || op == "-" && py_has_field(dict, "expr")? {
            ensure_only_py_fields(dict, &["op", "expr"], context)?;
            let expr = dict
                .get_item("expr")?
                .ok_or_else(|| PyValueError::new_err(format!("{context} neg requires expr")))?;
            return Ok(GraphExpr::Unary {
                op: GraphUnaryOp::Neg,
                expr: Box::new(parse_py_graph_expr(py, &expr, &format!("{context} expr"))?),
            });
        }
        ensure_only_py_fields(dict, &["op", "left", "right"], context)?;
        let left = dict
            .get_item("left")?
            .ok_or_else(|| PyValueError::new_err(format!("{context} binary op requires left")))?;
        let right = dict
            .get_item("right")?
            .ok_or_else(|| PyValueError::new_err(format!("{context} binary op requires right")))?;
        return Ok(GraphExpr::Binary {
            left: Box::new(parse_py_graph_expr(py, &left, &format!("{context} left"))?),
            op: parse_py_graph_binary_op(&op, context)?,
            right: Box::new(parse_py_graph_expr(
                py,
                &right,
                &format!("{context} right"),
            )?),
        });
    }
    if let Some(value) = dict.get_item("case")? {
        ensure_only_py_fields(dict, &["case"], context)?;
        return parse_py_graph_case_expr(py, value.downcast::<PyDict>()?, context);
    }
    if let Some(value) = dict.get_item("is_null")? {
        ensure_only_py_fields(dict, &["is_null"], context)?;
        return Ok(GraphExpr::IsNull(Box::new(parse_py_graph_expr(
            py,
            &value,
            &format!("{context} is_null"),
        )?)));
    }
    if let Some(value) = dict.get_item("is_not_null")? {
        ensure_only_py_fields(dict, &["is_not_null"], context)?;
        return Ok(GraphExpr::IsNotNull(Box::new(parse_py_graph_expr(
            py,
            &value,
            &format!("{context} is_not_null"),
        )?)));
    }
    unreachable!("expression discriminant count already checked")
}

fn parse_py_graph_aggregate_expr(
    py: Python<'_>,
    dict: &Bound<'_, PyDict>,
    context: &str,
) -> PyResult<GraphExpr> {
    let function: String = py_required_extract(dict, "function")?;
    let function = match function.as_str() {
        "count" => GraphAggregateFunction::Count,
        "sum" => GraphAggregateFunction::Sum,
        "avg" => GraphAggregateFunction::Avg,
        "min" => GraphAggregateFunction::Min,
        "max" => GraphAggregateFunction::Max,
        "collect" => GraphAggregateFunction::Collect,
        other => {
            return Err(PyValueError::new_err(format!(
                "{context} aggregate function is unsupported: '{other}'"
            )));
        }
    };
    Ok(GraphExpr::AggregateCall {
        function,
        distinct: py_optional_extract(dict, "distinct")?.unwrap_or(false),
        arg: py_non_none_item(dict, "arg")?
            .map(|value| parse_py_graph_expr(py, &value, &format!("{context} aggregate arg")))
            .transpose()?
            .map(Box::new),
    })
}

fn parse_py_graph_case_expr(
    py: Python<'_>,
    dict: &Bound<'_, PyDict>,
    context: &str,
) -> PyResult<GraphExpr> {
    let branches_value = py_non_none_item(dict, "branches")?
        .ok_or_else(|| PyValueError::new_err(format!("{context} case requires branches")))?;
    let branches_list = branches_value.downcast::<PyList>()?;
    let mut branches = Vec::with_capacity(branches_list.len());
    for (index, item) in branches_list.iter().enumerate() {
        let item = item.downcast::<PyDict>()?;
        let item_context = format!("{context} case branches[{index}]");
        let when = py_non_none_item(item, "when")?
            .ok_or_else(|| PyValueError::new_err(format!("{item_context} requires when")))?;
        let then = py_non_none_item(item, "then")?
            .ok_or_else(|| PyValueError::new_err(format!("{item_context} requires then")))?;
        branches.push(GraphCaseBranch {
            when: parse_py_graph_expr(py, &when, &format!("{item_context} when"))?,
            then: parse_py_graph_expr(py, &then, &format!("{item_context} then"))?,
        });
    }
    Ok(GraphExpr::Case {
        operand: py_non_none_item(dict, "operand")?
            .map(|value| parse_py_graph_expr(py, &value, &format!("{context} case operand")))
            .transpose()?
            .map(Box::new),
        branches,
        else_expr: py_non_none_item(dict, "else")?
            .map(|value| parse_py_graph_expr(py, &value, &format!("{context} case else")))
            .transpose()?
            .map(Box::new),
    })
}

fn parse_py_tagged_bytes(dict: &Bound<'_, PyDict>, context: &str) -> PyResult<Option<Vec<u8>>> {
    let Some(value) = dict.get_item("bytes")? else {
        return Ok(None);
    };
    let items = value.downcast::<PyList>()?;
    let mut parsed = Vec::with_capacity(items.len());
    for (index, item) in items.iter().enumerate() {
        parsed.push(py_query_u8(&item, &format!("{context} bytes[{index}]"))?);
    }
    Ok(Some(parsed))
}

fn parse_py_graph_node_field(name: &str, context: &str) -> PyResult<GraphNodeField> {
    match name {
        "id" => Ok(GraphNodeField::Id),
        "labels" => Ok(GraphNodeField::Labels),
        "key" => Ok(GraphNodeField::Key),
        "weight" => Ok(GraphNodeField::Weight),
        "created_at" => Ok(GraphNodeField::CreatedAt),
        "updated_at" => Ok(GraphNodeField::UpdatedAt),
        other => Err(PyValueError::new_err(format!(
            "{context} node_field field is unsupported: '{other}'"
        ))),
    }
}

fn parse_py_graph_edge_field(name: &str, context: &str) -> PyResult<eg::GraphEdgeField> {
    match name {
        "id" => Ok(eg::GraphEdgeField::Id),
        "from" | "from_id" => Ok(eg::GraphEdgeField::From),
        "to" | "to_id" => Ok(eg::GraphEdgeField::To),
        "label" | "type" => Ok(eg::GraphEdgeField::Label),
        "weight" => Ok(eg::GraphEdgeField::Weight),
        "created_at" => Ok(eg::GraphEdgeField::CreatedAt),
        "updated_at" => Ok(eg::GraphEdgeField::UpdatedAt),
        "valid_from" => Ok(eg::GraphEdgeField::ValidFrom),
        "valid_to" => Ok(eg::GraphEdgeField::ValidTo),
        other => Err(PyValueError::new_err(format!(
            "{context} edge_field field is unsupported: '{other}'"
        ))),
    }
}

fn parse_py_graph_path_field(name: &str, context: &str) -> PyResult<GraphPathField> {
    match name {
        "node_ids" => Ok(GraphPathField::NodeIds),
        "edge_ids" => Ok(GraphPathField::EdgeIds),
        "length" => Ok(GraphPathField::Length),
        other => Err(PyValueError::new_err(format!(
            "{context} path_field field is unsupported: '{other}'"
        ))),
    }
}

fn parse_py_graph_function_expr(
    name: String,
    args: Vec<GraphExpr>,
    context: &str,
) -> PyResult<GraphExpr> {
    if matches!(name.as_str(), "node_ids" | "edge_ids") {
        if args.len() != 1 {
            return Err(PyValueError::new_err(format!(
                "{context} {name}() requires exactly one binding argument"
            )));
        }
        let GraphExpr::Binding(alias) = args.into_iter().next().unwrap() else {
            return Err(PyValueError::new_err(format!(
                "{context} {name}() currently requires a binding argument"
            )));
        };
        return Ok(GraphExpr::PathField {
            alias,
            field: if name == "node_ids" {
                GraphPathField::NodeIds
            } else {
                GraphPathField::EdgeIds
            },
        });
    }
    let function = match name.as_str() {
        "id" => GraphFunction::Id,
        "labels" => GraphFunction::Labels,
        "type" => GraphFunction::Type,
        "length" => GraphFunction::Length,
        "start_node" => GraphFunction::StartNode,
        "end_node" => GraphFunction::EndNode,
        "nodes" => GraphFunction::Nodes,
        "relationships" => GraphFunction::Relationships,
        "coalesce" => GraphFunction::Coalesce,
        "to_string" => GraphFunction::ToString,
        "to_integer" => GraphFunction::ToInteger,
        "to_float" => GraphFunction::ToFloat,
        "abs" => GraphFunction::Abs,
        "floor" => GraphFunction::Floor,
        "ceil" => GraphFunction::Ceil,
        "round" => GraphFunction::Round,
        "lower" => GraphFunction::Lower,
        "upper" => GraphFunction::Upper,
        "trim" => GraphFunction::Trim,
        "substring" => GraphFunction::Substring,
        "size" => GraphFunction::Size,
        "head" => GraphFunction::Head,
        "last" => GraphFunction::Last,
        other => {
            return Err(PyValueError::new_err(format!(
                "{context} function is unsupported: '{other}'"
            )));
        }
    };
    Ok(GraphExpr::Function {
        name: function,
        args,
    })
}

fn parse_py_graph_binary_op(name: &str, context: &str) -> PyResult<GraphBinaryOp> {
    match name {
        "or" => Ok(GraphBinaryOp::Or),
        "and" => Ok(GraphBinaryOp::And),
        "=" | "==" | "eq" => Ok(GraphBinaryOp::Eq),
        "<>" | "!=" | "neq" => Ok(GraphBinaryOp::Neq),
        "<" => Ok(GraphBinaryOp::Lt),
        "<=" => Ok(GraphBinaryOp::Le),
        ">" => Ok(GraphBinaryOp::Gt),
        ">=" => Ok(GraphBinaryOp::Ge),
        "in" => Ok(GraphBinaryOp::In),
        "+" | "add" => Ok(GraphBinaryOp::Add),
        "-" | "sub" => Ok(GraphBinaryOp::Sub),
        "*" | "mul" => Ok(GraphBinaryOp::Mul),
        "/" | "div" => Ok(GraphBinaryOp::Div),
        "starts_with" => Ok(GraphBinaryOp::StartsWith),
        "ends_with" => Ok(GraphBinaryOp::EndsWith),
        "contains" => Ok(GraphBinaryOp::Contains),
        other => Err(PyValueError::new_err(format!(
            "{context} binary op is unsupported: '{other}'"
        ))),
    }
}

fn reject_py_legacy_node_predicate_fields(dict: &Bound<'_, PyDict>, context: &str) -> PyResult<()> {
    if py_has_field(dict, "where")? {
        return Err(PyValueError::new_err(format!(
            "{} where is no longer supported; use filter",
            context
        )));
    }
    if py_has_field(dict, "predicates")? {
        return Err(PyValueError::new_err(format!(
            "{} predicates are no longer supported; use filter",
            context
        )));
    }
    Ok(())
}

fn reject_py_legacy_node_label_field(dict: &Bound<'_, PyDict>, context: &str) -> PyResult<()> {
    if py_has_field(dict, "label")? {
        return Err(PyValueError::new_err(format!(
            "{} label is no longer supported; use label_filter",
            context
        )));
    }
    Ok(())
}

fn parse_py_node_filter(
    py: Python<'_>,
    dict: &Bound<'_, PyDict>,
    updated_at_key: &str,
    context: &str,
) -> PyResult<Option<NodeFilterExpr>> {
    reject_py_legacy_node_predicate_fields(dict, context)?;
    match py_non_none_item(dict, "filter")? {
        None => Ok(None),
        Some(value) => {
            parse_py_node_filter_expr(py, &value, updated_at_key, &format!("{} filter", context))
                .map(Some)
        }
    }
}

fn parse_py_edge_filter(
    py: Python<'_>,
    dict: &Bound<'_, PyDict>,
    updated_at_key: &str,
    valid_at_key: &str,
    valid_from_key: &str,
    valid_to_key: &str,
    context: &str,
) -> PyResult<Option<EdgeFilterExpr>> {
    match py_non_none_item(dict, "filter")? {
        None => Ok(None),
        Some(value) => parse_py_edge_filter_expr(
            py,
            &value,
            updated_at_key,
            valid_at_key,
            valid_from_key,
            valid_to_key,
            &format!("{} filter", context),
        )
        .map(Some),
    }
}

fn parse_py_node_filter_expr(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    updated_at_key: &str,
    context: &str,
) -> PyResult<NodeFilterExpr> {
    let dict = value
        .downcast::<PyDict>()
        .map_err(|_| PyTypeError::new_err(format!("{} must be a dict", context)))?;
    if dict.is_empty() {
        return Err(PyValueError::new_err(format!(
            "{} must not be an empty object",
            context
        )));
    }

    let selectors = ["and", "or", "not", "property", updated_at_key]
        .iter()
        .map(|field| py_has_field(dict, field))
        .collect::<PyResult<Vec<_>>>()?
        .into_iter()
        .filter(|present| *present)
        .count();
    if selectors != 1 {
        return Err(PyValueError::new_err(format!(
            "{} must contain exactly one boolean tag or leaf selector",
            context
        )));
    }
    reject_py_uppercase_filter_fields(dict, context)?;

    if let Some(value) = dict.get_item("and")? {
        ensure_only_py_fields(dict, &["and"], context)?;
        let children = value.downcast::<PyList>()?;
        if children.is_empty() {
            return Err(PyValueError::new_err(format!(
                "{} and must contain at least one child",
                context
            )));
        }
        let mut parsed = Vec::with_capacity(children.len());
        for (index, child) in children.iter().enumerate() {
            parsed.push(parse_py_node_filter_expr(
                py,
                &child,
                updated_at_key,
                &format!("{} and[{}]", context, index),
            )?);
        }
        return Ok(NodeFilterExpr::And(parsed));
    }
    if let Some(value) = dict.get_item("or")? {
        ensure_only_py_fields(dict, &["or"], context)?;
        let children = value.downcast::<PyList>()?;
        if children.is_empty() {
            return Err(PyValueError::new_err(format!(
                "{} or must contain at least one child",
                context
            )));
        }
        let mut parsed = Vec::with_capacity(children.len());
        for (index, child) in children.iter().enumerate() {
            parsed.push(parse_py_node_filter_expr(
                py,
                &child,
                updated_at_key,
                &format!("{} or[{}]", context, index),
            )?);
        }
        return Ok(NodeFilterExpr::Or(parsed));
    }
    if let Some(value) = dict.get_item("not")? {
        ensure_only_py_fields(dict, &["not"], context)?;
        return Ok(NodeFilterExpr::Not(Box::new(parse_py_node_filter_expr(
            py,
            &value,
            updated_at_key,
            &format!("{} not", context),
        )?)));
    }
    if py_has_field(dict, "property")? {
        return parse_py_property_node_filter(py, dict, context);
    }
    if let Some(value) = dict.get_item(updated_at_key)? {
        ensure_only_py_fields(dict, &[updated_at_key], context)?;
        return parse_py_updated_at_filter(&value, updated_at_key, context);
    }

    Err(PyValueError::new_err(format!(
        "{} must contain a valid filter selector",
        context
    )))
}

fn parse_py_edge_filter_expr(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    updated_at_key: &str,
    valid_at_key: &str,
    valid_from_key: &str,
    valid_to_key: &str,
    context: &str,
) -> PyResult<EdgeFilterExpr> {
    let dict = value
        .downcast::<PyDict>()
        .map_err(|_| PyTypeError::new_err(format!("{} must be a dict", context)))?;
    if dict.is_empty() {
        return Err(PyValueError::new_err(format!(
            "{} must not be an empty object",
            context
        )));
    }

    let selectors = [
        "and",
        "or",
        "not",
        "property",
        "weight",
        updated_at_key,
        valid_at_key,
        valid_from_key,
        valid_to_key,
    ]
    .iter()
    .map(|field| py_has_field(dict, field))
    .collect::<PyResult<Vec<_>>>()?
    .into_iter()
    .filter(|present| *present)
    .count();
    if selectors != 1 {
        return Err(PyValueError::new_err(format!(
            "{} must contain exactly one boolean tag or leaf selector",
            context
        )));
    }
    reject_py_uppercase_filter_fields(dict, context)?;

    if let Some(value) = dict.get_item("and")? {
        ensure_only_py_fields(dict, &["and"], context)?;
        let children = value.downcast::<PyList>()?;
        if children.is_empty() {
            return Err(PyValueError::new_err(format!(
                "{} and must contain at least one child",
                context
            )));
        }
        let mut parsed = Vec::with_capacity(children.len());
        for (index, child) in children.iter().enumerate() {
            parsed.push(parse_py_edge_filter_expr(
                py,
                &child,
                updated_at_key,
                valid_at_key,
                valid_from_key,
                valid_to_key,
                &format!("{} and[{}]", context, index),
            )?);
        }
        return Ok(EdgeFilterExpr::And(parsed));
    }
    if let Some(value) = dict.get_item("or")? {
        ensure_only_py_fields(dict, &["or"], context)?;
        let children = value.downcast::<PyList>()?;
        if children.is_empty() {
            return Err(PyValueError::new_err(format!(
                "{} or must contain at least one child",
                context
            )));
        }
        let mut parsed = Vec::with_capacity(children.len());
        for (index, child) in children.iter().enumerate() {
            parsed.push(parse_py_edge_filter_expr(
                py,
                &child,
                updated_at_key,
                valid_at_key,
                valid_from_key,
                valid_to_key,
                &format!("{} or[{}]", context, index),
            )?);
        }
        return Ok(EdgeFilterExpr::Or(parsed));
    }
    if let Some(value) = dict.get_item("not")? {
        ensure_only_py_fields(dict, &["not"], context)?;
        return Ok(EdgeFilterExpr::Not(Box::new(parse_py_edge_filter_expr(
            py,
            &value,
            updated_at_key,
            valid_at_key,
            valid_from_key,
            valid_to_key,
            &format!("{} not", context),
        )?)));
    }
    if py_has_field(dict, "property")? {
        return parse_py_property_edge_filter(py, dict, context);
    }
    if let Some(value) = dict.get_item("weight")? {
        ensure_only_py_fields(dict, &["weight"], context)?;
        let range = value.downcast::<PyDict>()?;
        let (lower, upper) = parse_py_f32_range_bounds(range, &format!("{} weight", context))?;
        return Ok(EdgeFilterExpr::WeightRange { lower, upper });
    }
    if let Some(value) = dict.get_item(updated_at_key)? {
        ensure_only_py_fields(dict, &[updated_at_key], context)?;
        let range = value.downcast::<PyDict>()?;
        let (lower_ms, upper_ms) =
            parse_py_i64_range_bounds(range, &format!("{} {}", context, updated_at_key))?;
        return Ok(EdgeFilterExpr::UpdatedAtRange { lower_ms, upper_ms });
    }
    if let Some(value) = dict.get_item(valid_at_key)? {
        ensure_only_py_fields(dict, &[valid_at_key], context)?;
        return Ok(EdgeFilterExpr::ValidAt {
            epoch_ms: py_query_i64(&value, &format!("{} {}", context, valid_at_key))?,
        });
    }
    if let Some(value) = dict.get_item(valid_from_key)? {
        ensure_only_py_fields(dict, &[valid_from_key], context)?;
        let range = value.downcast::<PyDict>()?;
        let (lower_ms, upper_ms) =
            parse_py_i64_range_bounds(range, &format!("{} {}", context, valid_from_key))?;
        return Ok(EdgeFilterExpr::ValidFromRange { lower_ms, upper_ms });
    }
    if let Some(value) = dict.get_item(valid_to_key)? {
        ensure_only_py_fields(dict, &[valid_to_key], context)?;
        let range = value.downcast::<PyDict>()?;
        let (lower_ms, upper_ms) =
            parse_py_i64_range_bounds(range, &format!("{} {}", context, valid_to_key))?;
        return Ok(EdgeFilterExpr::ValidToRange { lower_ms, upper_ms });
    }

    Err(PyValueError::new_err(format!(
        "{} must contain a valid filter selector",
        context
    )))
}

fn parse_py_property_node_filter(
    py: Python<'_>,
    dict: &Bound<'_, PyDict>,
    context: &str,
) -> PyResult<NodeFilterExpr> {
    let key_value = py_non_none_item(dict, "property")?
        .ok_or_else(|| PyValueError::new_err(format!("{} property is required", context)))?;
    let key: String = key_value.extract()?;
    if key.is_empty() {
        return Err(PyValueError::new_err(format!(
            "{} property must be non-empty",
            context
        )));
    }

    let has_range = py_has_any_field(dict, &["gt", "gte", "lt", "lte"])?;
    let families = [
        py_has_field(dict, "eq")?,
        py_has_field(dict, "in")?,
        has_range,
        py_has_field(dict, "exists")?,
        py_has_field(dict, "missing")?,
    ]
    .into_iter()
    .filter(|present| *present)
    .count();
    if families != 1 {
        return Err(PyValueError::new_err(format!(
            "{} property filter must specify exactly one operator family",
            context
        )));
    }

    if let Some(value) = dict.get_item("eq")? {
        ensure_only_py_fields(dict, &["property", "eq"], context)?;
        return Ok(NodeFilterExpr::PropertyEquals {
            key,
            value: py_to_prop_value(py, &value)?,
        });
    }
    if let Some(value) = dict.get_item("in")? {
        ensure_only_py_fields(dict, &["property", "in"], context)?;
        let values = value.downcast::<PyList>()?;
        if values.is_empty() {
            return Err(PyValueError::new_err(format!(
                "{} in must contain at least one value",
                context
            )));
        }
        let parsed = values
            .iter()
            .map(|value| py_to_prop_value(py, &value))
            .collect::<PyResult<Vec<_>>>()?;
        return Ok(NodeFilterExpr::PropertyIn {
            key,
            values: parsed,
        });
    }
    if has_range {
        ensure_only_py_fields(dict, &["property", "gt", "gte", "lt", "lte"], context)?;
        let (lower, upper) = parse_py_property_range_bounds(py, dict, context)?;
        return Ok(NodeFilterExpr::PropertyRange { key, lower, upper });
    }
    if py_has_field(dict, "exists")? {
        ensure_only_py_fields(dict, &["property", "exists"], context)?;
        require_py_true_field(dict, "exists", context)?;
        return Ok(NodeFilterExpr::PropertyExists { key });
    }
    if py_has_field(dict, "missing")? {
        ensure_only_py_fields(dict, &["property", "missing"], context)?;
        require_py_true_field(dict, "missing", context)?;
        return Ok(NodeFilterExpr::PropertyMissing { key });
    }

    unreachable!("operator family count was checked above")
}

fn parse_py_property_edge_filter(
    py: Python<'_>,
    dict: &Bound<'_, PyDict>,
    context: &str,
) -> PyResult<EdgeFilterExpr> {
    let key_value = py_non_none_item(dict, "property")?
        .ok_or_else(|| PyValueError::new_err(format!("{} property is required", context)))?;
    let key: String = key_value.extract()?;
    if key.is_empty() {
        return Err(PyValueError::new_err(format!(
            "{} property must be non-empty",
            context
        )));
    }

    let has_range = py_has_any_field(dict, &["gt", "gte", "lt", "lte"])?;
    let families = [
        py_has_field(dict, "eq")?,
        py_has_field(dict, "in")?,
        has_range,
        py_has_field(dict, "exists")?,
        py_has_field(dict, "missing")?,
    ]
    .into_iter()
    .filter(|present| *present)
    .count();
    if families != 1 {
        return Err(PyValueError::new_err(format!(
            "{} property filter must specify exactly one operator family",
            context
        )));
    }

    if let Some(value) = dict.get_item("eq")? {
        ensure_only_py_fields(dict, &["property", "eq"], context)?;
        return Ok(EdgeFilterExpr::PropertyEquals {
            key,
            value: py_to_prop_value(py, &value)?,
        });
    }
    if let Some(value) = dict.get_item("in")? {
        ensure_only_py_fields(dict, &["property", "in"], context)?;
        let values = value.downcast::<PyList>()?;
        if values.is_empty() {
            return Err(PyValueError::new_err(format!(
                "{} in must contain at least one value",
                context
            )));
        }
        let parsed = values
            .iter()
            .map(|value| py_to_prop_value(py, &value))
            .collect::<PyResult<Vec<_>>>()?;
        return Ok(EdgeFilterExpr::PropertyIn {
            key,
            values: parsed,
        });
    }
    if has_range {
        ensure_only_py_fields(dict, &["property", "gt", "gte", "lt", "lte"], context)?;
        let (lower, upper) = parse_py_property_range_bounds(py, dict, context)?;
        return Ok(EdgeFilterExpr::PropertyRange { key, lower, upper });
    }
    if py_has_field(dict, "exists")? {
        ensure_only_py_fields(dict, &["property", "exists"], context)?;
        require_py_true_field(dict, "exists", context)?;
        return Ok(EdgeFilterExpr::PropertyExists { key });
    }
    if py_has_field(dict, "missing")? {
        ensure_only_py_fields(dict, &["property", "missing"], context)?;
        require_py_true_field(dict, "missing", context)?;
        return Ok(EdgeFilterExpr::PropertyMissing { key });
    }

    unreachable!("operator family count was checked above")
}

fn parse_py_updated_at_filter(
    value: &Bound<'_, PyAny>,
    tag: &str,
    context: &str,
) -> PyResult<NodeFilterExpr> {
    let dict = value.downcast::<PyDict>()?;
    ensure_only_py_fields(
        dict,
        &["gt", "gte", "lt", "lte"],
        &format!("{} {}", context, tag),
    )?;
    let (lower_ms, upper_ms) = parse_py_i64_range_bounds(dict, &format!("{} {}", context, tag))?;
    Ok(NodeFilterExpr::UpdatedAtRange { lower_ms, upper_ms })
}

fn parse_py_property_range_bounds(
    py: Python<'_>,
    dict: &Bound<'_, PyDict>,
    context: &str,
) -> PyResult<(
    Option<CorePropertyRangeBound>,
    Option<CorePropertyRangeBound>,
)> {
    if py_has_field(dict, "gt")? && py_has_field(dict, "gte")? {
        return Err(PyValueError::new_err(format!(
            "{} range predicate cannot specify both gt and gte",
            context
        )));
    }
    if py_has_field(dict, "lt")? && py_has_field(dict, "lte")? {
        return Err(PyValueError::new_err(format!(
            "{} range predicate cannot specify both lt and lte",
            context
        )));
    }
    let lower = if let Some(value) = dict.get_item("gt")? {
        Some(CorePropertyRangeBound::Excluded(py_to_prop_value(
            py, &value,
        )?))
    } else {
        dict.get_item("gte")?
            .map(|value| py_to_prop_value(py, &value).map(CorePropertyRangeBound::Included))
            .transpose()?
    };
    let upper = if let Some(value) = dict.get_item("lt")? {
        Some(CorePropertyRangeBound::Excluded(py_to_prop_value(
            py, &value,
        )?))
    } else {
        dict.get_item("lte")?
            .map(|value| py_to_prop_value(py, &value).map(CorePropertyRangeBound::Included))
            .transpose()?
    };
    if lower.is_none() && upper.is_none() {
        return Err(PyValueError::new_err(format!(
            "{} range predicate requires at least one of gt, gte, lt, or lte",
            context
        )));
    }
    Ok((lower, upper))
}

fn parse_py_i64_range_bounds(
    dict: &Bound<'_, PyDict>,
    context: &str,
) -> PyResult<(Option<i64>, Option<i64>)> {
    if py_has_field(dict, "gt")? && py_has_field(dict, "gte")? {
        return Err(PyValueError::new_err(format!(
            "{} range predicate cannot specify both gt and gte",
            context
        )));
    }
    if py_has_field(dict, "lt")? && py_has_field(dict, "lte")? {
        return Err(PyValueError::new_err(format!(
            "{} range predicate cannot specify both lt and lte",
            context
        )));
    }
    let mut impossible = false;
    let lower = if let Some(value) = dict.get_item("gt")? {
        let value = py_query_i64(&value, &format!("{} gt", context))?;
        match value.checked_add(1) {
            Some(next) => Some(next),
            None => {
                impossible = true;
                Some(i64::MAX)
            }
        }
    } else {
        dict.get_item("gte")?
            .map(|value| py_query_i64(&value, &format!("{} gte", context)))
            .transpose()?
    };
    let upper = if let Some(value) = dict.get_item("lt")? {
        let value = py_query_i64(&value, &format!("{} lt", context))?;
        match value.checked_sub(1) {
            Some(prev) => Some(prev),
            None => {
                impossible = true;
                Some(i64::MIN)
            }
        }
    } else {
        dict.get_item("lte")?
            .map(|value| py_query_i64(&value, &format!("{} lte", context)))
            .transpose()?
    };
    if lower.is_none() && upper.is_none() {
        return Err(PyValueError::new_err(format!(
            "{} range predicate requires at least one of gt, gte, lt, or lte",
            context
        )));
    }
    if impossible {
        return Ok((Some(i64::MAX), Some(i64::MIN)));
    }
    Ok((lower, upper))
}

fn parse_py_f32_range_bounds(
    dict: &Bound<'_, PyDict>,
    context: &str,
) -> PyResult<(Option<f32>, Option<f32>)> {
    if py_has_field(dict, "gt")? && py_has_field(dict, "gte")? {
        return Err(PyValueError::new_err(format!(
            "{} range predicate cannot specify both gt and gte",
            context
        )));
    }
    if py_has_field(dict, "lt")? && py_has_field(dict, "lte")? {
        return Err(PyValueError::new_err(format!(
            "{} range predicate cannot specify both lt and lte",
            context
        )));
    }
    let lower = if let Some(value) = dict.get_item("gt")? {
        Some(next_up_f32(py_query_f32(
            &value,
            &format!("{} gt", context),
        )?))
    } else {
        dict.get_item("gte")?
            .map(|value| py_query_f32(&value, &format!("{} gte", context)))
            .transpose()?
    };
    let upper = if let Some(value) = dict.get_item("lt")? {
        Some(next_down_f32(py_query_f32(
            &value,
            &format!("{} lt", context),
        )?))
    } else {
        dict.get_item("lte")?
            .map(|value| py_query_f32(&value, &format!("{} lte", context)))
            .transpose()?
    };
    if lower.is_none() && upper.is_none() {
        return Err(PyValueError::new_err(format!(
            "{} range predicate requires at least one of gt, gte, lt, or lte",
            context
        )));
    }
    Ok((lower, upper))
}

fn parse_py_query_limit(dict: &Bound<'_, PyDict>, context: &str) -> PyResult<Option<usize>> {
    match py_non_none_item(dict, "limit")? {
        None => Ok(None),
        Some(value) => {
            let limit = py_query_usize(&value, context)?;
            if limit == 0 {
                Ok(None)
            } else {
                Ok(Some(limit))
            }
        }
    }
}

fn py_optional_query_u64(
    dict: &Bound<'_, PyDict>,
    key: &str,
    context: &str,
) -> PyResult<Option<u64>> {
    py_non_none_item(dict, key)?
        .map(|value| py_query_u64(&value, context))
        .transpose()
}

fn py_optional_query_i64(
    dict: &Bound<'_, PyDict>,
    key: &str,
    context: &str,
) -> PyResult<Option<i64>> {
    py_non_none_item(dict, key)?
        .map(|value| py_query_i64(&value, context))
        .transpose()
}

fn py_optional_query_usize(
    dict: &Bound<'_, PyDict>,
    key: &str,
    context: &str,
) -> PyResult<Option<usize>> {
    py_non_none_item(dict, key)?
        .map(|value| py_query_usize(&value, context))
        .transpose()
}

fn py_optional_query_u8(
    dict: &Bound<'_, PyDict>,
    key: &str,
    context: &str,
) -> PyResult<Option<u8>> {
    py_non_none_item(dict, key)?
        .map(|value| py_query_u8(&value, context))
        .transpose()
}

fn py_optional_query_u64_vec(
    dict: &Bound<'_, PyDict>,
    key: &str,
    context: &str,
) -> PyResult<Vec<u64>> {
    match py_non_none_item(dict, key)? {
        None => Ok(Vec::new()),
        Some(value) => {
            let items = value.downcast::<PyList>()?;
            let mut parsed = Vec::with_capacity(items.len());
            for (index, item) in items.iter().enumerate() {
                parsed.push(py_query_u64(&item, &format!("{}[{}]", context, index))?);
            }
            Ok(parsed)
        }
    }
}

fn py_optional_string_vec(
    dict: &Bound<'_, PyDict>,
    key: &str,
    context: &str,
) -> PyResult<Option<Vec<String>>> {
    match py_non_none_item(dict, key)? {
        None => Ok(None),
        Some(value) => {
            let items = value.downcast::<PyList>()?;
            let mut parsed = Vec::with_capacity(items.len());
            for (index, item) in items.iter().enumerate() {
                parsed.push(item.extract::<String>().map_err(|_| {
                    PyTypeError::new_err(format!("{}[{}] must be str", context, index))
                })?);
            }
            Ok(Some(parsed))
        }
    }
}

fn parse_node_labels_arg(value: &Bound<'_, PyAny>, context: &str) -> PyResult<Vec<String>> {
    if let Ok(label) = value.extract::<String>() {
        return Ok(vec![label]);
    }
    value
        .extract::<Vec<String>>()
        .map_err(|_| PyTypeError::new_err(format!("{context} must be str or a sequence of str")))
}

fn parse_node_labels_list_field(
    dict: &Bound<'_, PyDict>,
    key: &str,
    context: &str,
) -> PyResult<Vec<String>> {
    let value = py_non_none_item(dict, key)?
        .ok_or_else(|| PyValueError::new_err(format!("{context} requires {key}")))?;
    if value.extract::<String>().is_ok() {
        return Err(PyTypeError::new_err(format!(
            "{context} {key} must be a sequence of str, not str"
        )));
    }
    value
        .extract::<Vec<String>>()
        .map_err(|_| PyTypeError::new_err(format!("{context} {key} must be a sequence of str")))
}

fn parse_optional_node_label_filter_arg(
    value: Option<&Bound<'_, PyAny>>,
    context: &str,
) -> PyResult<Option<NodeLabelFilter>> {
    value
        .filter(|value| !value.is_none())
        .map(|value| parse_node_label_filter_arg(value, context))
        .transpose()
}

fn parse_optional_node_label_filter_field(
    dict: &Bound<'_, PyDict>,
    key: &str,
    context: &str,
) -> PyResult<Option<NodeLabelFilter>> {
    py_non_none_item(dict, key)?
        .map(|value| parse_node_label_filter_arg(&value, context))
        .transpose()
}

fn parse_node_label_filter_arg(
    value: &Bound<'_, PyAny>,
    context: &str,
) -> PyResult<NodeLabelFilter> {
    let dict = value
        .downcast::<PyDict>()
        .map_err(|_| PyTypeError::new_err(format!("{context} must be a dict")))?;
    ensure_only_py_fields(dict, &["labels", "mode"], context)?;
    let labels = parse_node_labels_list_field(dict, "labels", context)?;
    let mode = match py_non_none_item(dict, "mode")? {
        Some(mode_value) => parse_label_match_mode(&mode_value.extract::<String>()?, context)?,
        None => LabelMatchMode::All,
    };
    Ok(NodeLabelFilter { labels, mode })
}

fn parse_label_match_mode(value: &str, context: &str) -> PyResult<LabelMatchMode> {
    match value {
        "any" => Ok(LabelMatchMode::Any),
        "all" => Ok(LabelMatchMode::All),
        other => Err(PyValueError::new_err(format!(
            "{context} mode must be 'any' or 'all', got '{other}'"
        ))),
    }
}

fn py_query_u64(value: &Bound<'_, PyAny>, context: &str) -> PyResult<u64> {
    reject_py_bool(value, context)?;
    value.extract::<u64>()
}

fn py_query_i64(value: &Bound<'_, PyAny>, context: &str) -> PyResult<i64> {
    reject_py_bool(value, context)?;
    value.extract::<i64>()
}

fn py_query_f32(value: &Bound<'_, PyAny>, context: &str) -> PyResult<f32> {
    reject_py_bool(value, context)?;
    let parsed = value
        .extract::<f64>()
        .map_err(|_| PyValueError::new_err(format!("{} must be a number", context)))?;
    if !parsed.is_finite() || parsed < f32::MIN as f64 || parsed > f32::MAX as f64 {
        return Err(PyValueError::new_err(format!(
            "{} must be a finite f32 number",
            context
        )));
    }
    let parsed = parsed as f32;
    if parsed.is_nan() {
        return Err(PyValueError::new_err(format!(
            "{} must not be NaN",
            context
        )));
    }
    Ok(parsed)
}

fn next_up_f32(value: f32) -> f32 {
    if value == f32::INFINITY {
        return value;
    }
    if value == -0.0 {
        return f32::from_bits(1);
    }
    let bits = value.to_bits();
    if value >= 0.0 {
        f32::from_bits(bits + 1)
    } else {
        f32::from_bits(bits - 1)
    }
}

fn next_down_f32(value: f32) -> f32 {
    if value == f32::NEG_INFINITY {
        return value;
    }
    if value == 0.0 {
        return -f32::from_bits(1);
    }
    let bits = value.to_bits();
    if value > 0.0 {
        f32::from_bits(bits - 1)
    } else {
        f32::from_bits(bits + 1)
    }
}

fn py_query_usize(value: &Bound<'_, PyAny>, context: &str) -> PyResult<usize> {
    reject_py_bool(value, context)?;
    value.extract::<usize>()
}

fn py_query_u8(value: &Bound<'_, PyAny>, context: &str) -> PyResult<u8> {
    reject_py_bool(value, context)?;
    value.extract::<u8>()
}

fn reject_py_bool(value: &Bound<'_, PyAny>, context: &str) -> PyResult<()> {
    if value.is_instance_of::<PyBool>() {
        return Err(PyTypeError::new_err(format!(
            "{} must be an integer, not bool",
            context
        )));
    }
    Ok(())
}

fn py_non_none_item<'py>(
    dict: &Bound<'py, PyDict>,
    key: &str,
) -> PyResult<Option<Bound<'py, PyAny>>> {
    Ok(dict.get_item(key)?.filter(|value| !value.is_none()))
}

fn py_optional_extract<T>(dict: &Bound<'_, PyDict>, key: &str) -> PyResult<Option<T>>
where
    for<'a> T: FromPyObject<'a>,
{
    py_non_none_item(dict, key)?
        .map(|value| value.extract::<T>())
        .transpose()
}

fn py_required_extract<T>(dict: &Bound<'_, PyDict>, key: &str) -> PyResult<T>
where
    for<'a> T: FromPyObject<'a>,
{
    py_non_none_item(dict, key)?
        .ok_or_else(|| PyValueError::new_err(format!("{} is required", key)))?
        .extract::<T>()
}

fn py_has_field(dict: &Bound<'_, PyDict>, key: &str) -> PyResult<bool> {
    Ok(dict.get_item(key)?.is_some())
}

fn py_has_any_field(dict: &Bound<'_, PyDict>, fields: &[&str]) -> PyResult<bool> {
    for field in fields {
        if py_has_field(dict, field)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn ensure_only_py_fields(
    dict: &Bound<'_, PyDict>,
    allowed: &[&str],
    context: &str,
) -> PyResult<()> {
    for (key, _) in dict.iter() {
        let key: String = key.extract()?;
        if !allowed.iter().any(|allowed| *allowed == key) {
            return Err(PyValueError::new_err(format!(
                "{} does not accept field '{}'",
                context, key
            )));
        }
    }
    Ok(())
}

fn ensure_only_py_secondary_index_fields(
    dict: &Bound<'_, PyDict>,
    allowed: &[&str],
    context: &str,
) -> PyResult<()> {
    for (key, _) in dict.iter() {
        let key: String = key.extract().map_err(|_| {
            PyValueError::new_err(format!(
                "invalid secondary index: {context} field names must be strings"
            ))
        })?;
        if !allowed.iter().any(|allowed| *allowed == key) {
            return Err(PyValueError::new_err(format!(
                "invalid secondary index: {context} does not accept field '{key}'"
            )));
        }
    }
    Ok(())
}

fn require_py_true_field(dict: &Bound<'_, PyDict>, field: &str, context: &str) -> PyResult<()> {
    let value = dict
        .get_item(field)?
        .ok_or_else(|| PyValueError::new_err(format!("{} {} is required", context, field)))?;
    if value.is_instance_of::<PyBool>() && value.extract::<bool>()? {
        Ok(())
    } else {
        Err(PyValueError::new_err(format!(
            "{} {} must be true",
            context, field
        )))
    }
}

fn reject_py_uppercase_filter_fields(dict: &Bound<'_, PyDict>, context: &str) -> PyResult<()> {
    for (key, _) in dict.iter() {
        let key: String = key.extract()?;
        if matches!(
            key.as_str(),
            "AND" | "OR" | "NOT" | "Eq" | "In" | "Exists" | "Missing"
        ) {
            return Err(PyValueError::new_err(format!(
                "{} uses unsupported uppercase filter field '{}'",
                context, key
            )));
        }
    }
    Ok(())
}

fn prop_value_debug_repr(py: Python<'_>, value: &PropValue) -> PyResult<String> {
    prop_value_to_py_obj(py, value)?.bind(py).repr()?.extract()
}

#[derive(Clone, Copy)]
enum RangeValueDomain {
    Int,
    UInt,
    Float,
}

fn parse_range_value_domain(domain: &str) -> PyResult<RangeValueDomain> {
    match domain {
        "int" => Ok(RangeValueDomain::Int),
        "uint" => Ok(RangeValueDomain::UInt),
        "float" => Ok(RangeValueDomain::Float),
        other => Err(PyValueError::new_err(format!(
            "Invalid range value type annotation '{}'. Must be 'int', 'uint', or 'float'.",
            other
        ))),
    }
}

fn range_value_domain_to_py(domain: RangeValueDomain) -> &'static str {
    match domain {
        RangeValueDomain::Int => "int",
        RangeValueDomain::UInt => "uint",
        RangeValueDomain::Float => "float",
    }
}

fn secondary_index_state_to_py(state: SecondaryIndexState) -> &'static str {
    match state {
        SecondaryIndexState::Building => "building",
        SecondaryIndexState::Ready => "ready",
        SecondaryIndexState::Failed => "failed",
    }
}

fn secondary_index_kind_to_py(kind: &SecondaryIndexKind) -> &'static str {
    match kind {
        SecondaryIndexKind::Equality => "equality",
        SecondaryIndexKind::Range => "range",
    }
}

#[derive(Clone, Copy)]
enum PySecondaryIndexTargetKind {
    Node,
    Edge,
}

fn secondary_index_field_to_py(field: CoreSecondaryIndexField) -> BTreeMap<String, String> {
    let mut map = BTreeMap::new();
    match field {
        CoreSecondaryIndexField::Property { key } => {
            map.insert("source".to_string(), "property".to_string());
            map.insert("key".to_string(), key);
        }
        CoreSecondaryIndexField::NodeMetadata(field) => {
            map.insert("source".to_string(), "metadata".to_string());
            map.insert(
                "field".to_string(),
                node_metadata_index_field_to_py(field).to_string(),
            );
        }
        CoreSecondaryIndexField::EdgeMetadata(field) => {
            map.insert("source".to_string(), "metadata".to_string());
            map.insert(
                "field".to_string(),
                edge_metadata_index_field_to_py(field).to_string(),
            );
        }
    }
    map
}

fn parse_py_secondary_index_spec(
    value: &Bound<'_, PyAny>,
    target_kind: PySecondaryIndexTargetKind,
) -> PyResult<CoreSecondaryIndexSpec> {
    let dict = value
        .downcast::<PyDict>()
        .map_err(|_| PyTypeError::new_err("invalid secondary index: spec must be a mapping"))?;
    ensure_only_py_secondary_index_fields(dict, &["fields", "kind"], "secondary index spec")?;
    let kind = py_required_string(dict, "kind", "kind")?;
    let kind = parse_secondary_index_kind(&kind)?;
    let fields_value = py_non_none_item(dict, "fields")?
        .ok_or_else(|| PyValueError::new_err("invalid secondary index: fields are required"))?;
    let fields_list = fields_value
        .downcast::<PyList>()
        .map_err(|_| PyTypeError::new_err("invalid secondary index: fields must be a list"))?;
    let mut fields = Vec::with_capacity(fields_list.len());
    for field in fields_list.iter() {
        fields.push(parse_py_secondary_index_field(&field, target_kind)?);
    }
    Ok(CoreSecondaryIndexSpec { fields, kind })
}

fn parse_py_secondary_index_field(
    value: &Bound<'_, PyAny>,
    target_kind: PySecondaryIndexTargetKind,
) -> PyResult<CoreSecondaryIndexField> {
    let dict = value
        .downcast::<PyDict>()
        .map_err(|_| PyTypeError::new_err("invalid secondary index: field must be a mapping"))?;
    ensure_only_py_secondary_index_fields(
        dict,
        &["source", "key", "field"],
        "secondary index field",
    )?;
    let source_value = py_non_none_item(dict, "source")?.ok_or_else(|| {
        PyValueError::new_err("invalid secondary index: field source is required")
    })?;
    let source = py_extract_secondary_index_string(&source_value, "field source")?;
    match source.as_str() {
        "property" => {
            let key_value = py_non_none_item(dict, "key")?.ok_or_else(|| {
                PyValueError::new_err("invalid secondary index: property fields require key")
            })?;
            let key = py_extract_secondary_index_string(&key_value, "property key")?;
            Ok(CoreSecondaryIndexField::property(key))
        }
        "metadata" => {
            let field_value = py_non_none_item(dict, "field")?.ok_or_else(|| {
                PyValueError::new_err("invalid secondary index: metadata fields require field")
            })?;
            let field = py_extract_secondary_index_string(&field_value, "metadata field")?;
            match target_kind {
                PySecondaryIndexTargetKind::Node => Ok(CoreSecondaryIndexField::node_meta(
                    parse_py_node_metadata_index_field(&field)?,
                )),
                PySecondaryIndexTargetKind::Edge => Ok(CoreSecondaryIndexField::edge_meta(
                    parse_py_edge_metadata_index_field(&field)?,
                )),
            }
        }
        other => Err(PyValueError::new_err(format!(
            "invalid secondary index: field source must be 'property' or 'metadata', got '{other}'"
        ))),
    }
}

fn py_required_string(dict: &Bound<'_, PyDict>, key: &str, display: &str) -> PyResult<String> {
    let value = py_non_none_item(dict, key)?.ok_or_else(|| {
        PyValueError::new_err(format!("invalid secondary index: {display} is required"))
    })?;
    py_extract_secondary_index_string(&value, display)
}

fn py_extract_secondary_index_string(value: &Bound<'_, PyAny>, display: &str) -> PyResult<String> {
    value.extract::<String>().map_err(|_| {
        PyValueError::new_err(format!(
            "invalid secondary index: {display} must be a string"
        ))
    })
}

fn node_metadata_index_field_to_py(field: CoreNodeMetadataIndexField) -> &'static str {
    match field {
        CoreNodeMetadataIndexField::Id => "id",
        CoreNodeMetadataIndexField::Key => "key",
        CoreNodeMetadataIndexField::Weight => "weight",
        CoreNodeMetadataIndexField::CreatedAt => "created_at",
        CoreNodeMetadataIndexField::UpdatedAt => "updated_at",
    }
}

fn edge_metadata_index_field_to_py(field: CoreEdgeMetadataIndexField) -> &'static str {
    match field {
        CoreEdgeMetadataIndexField::Id => "id",
        CoreEdgeMetadataIndexField::From => "from",
        CoreEdgeMetadataIndexField::To => "to",
        CoreEdgeMetadataIndexField::Weight => "weight",
        CoreEdgeMetadataIndexField::CreatedAt => "created_at",
        CoreEdgeMetadataIndexField::UpdatedAt => "updated_at",
        CoreEdgeMetadataIndexField::ValidFrom => "valid_from",
        CoreEdgeMetadataIndexField::ValidTo => "valid_to",
    }
}

fn parse_py_node_metadata_index_field(field: &str) -> PyResult<CoreNodeMetadataIndexField> {
    match field {
        "id" => Ok(CoreNodeMetadataIndexField::Id),
        "key" => Ok(CoreNodeMetadataIndexField::Key),
        "weight" => Ok(CoreNodeMetadataIndexField::Weight),
        "created_at" => Ok(CoreNodeMetadataIndexField::CreatedAt),
        "updated_at" => Ok(CoreNodeMetadataIndexField::UpdatedAt),
        other => Err(PyValueError::new_err(format!(
            "invalid secondary index: unsupported node metadata field '{other}'"
        ))),
    }
}

fn parse_py_edge_metadata_index_field(field: &str) -> PyResult<CoreEdgeMetadataIndexField> {
    match field {
        "id" => Ok(CoreEdgeMetadataIndexField::Id),
        "from" => Ok(CoreEdgeMetadataIndexField::From),
        "to" => Ok(CoreEdgeMetadataIndexField::To),
        "weight" => Ok(CoreEdgeMetadataIndexField::Weight),
        "created_at" => Ok(CoreEdgeMetadataIndexField::CreatedAt),
        "updated_at" => Ok(CoreEdgeMetadataIndexField::UpdatedAt),
        "valid_from" => Ok(CoreEdgeMetadataIndexField::ValidFrom),
        "valid_to" => Ok(CoreEdgeMetadataIndexField::ValidTo),
        other => Err(PyValueError::new_err(format!(
            "invalid secondary index: unsupported edge metadata field '{other}'"
        ))),
    }
}

fn parse_secondary_index_kind(kind: &str) -> PyResult<SecondaryIndexKind> {
    match kind {
        "equality" => Ok(SecondaryIndexKind::Equality),
        "range" => Ok(SecondaryIndexKind::Range),
        other => Err(PyValueError::new_err(format!(
            "invalid secondary index: kind must be 'equality' or 'range', got '{other}'"
        ))),
    }
}

fn range_value_domain_from_prop_value(
    value: &PropValue,
    context: &str,
) -> PyResult<RangeValueDomain> {
    match value {
        PropValue::Int(_) => Ok(RangeValueDomain::Int),
        PropValue::UInt(_) => Ok(RangeValueDomain::UInt),
        PropValue::Float(value) if value.is_finite() => Ok(RangeValueDomain::Float),
        _ => Err(PyValueError::new_err(format!(
            "{} must use Int, UInt, or finite Float values",
            context
        ))),
    }
}

fn py_numeric_to_prop_value(
    _py: Python<'_>,
    obj: &Bound<'_, pyo3::PyAny>,
    domain: RangeValueDomain,
) -> PyResult<PropValue> {
    if obj.is_instance_of::<PyBool>() {
        return Err(PyTypeError::new_err(
            "property range values must be numeric, not bool",
        ));
    }

    match domain {
        RangeValueDomain::Int => Ok(PropValue::Int(obj.extract::<i64>()?)),
        RangeValueDomain::UInt => Ok(PropValue::UInt(obj.extract::<u64>()?)),
        RangeValueDomain::Float => {
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

fn with_py_txn<F, T>(inner: &Arc<Mutex<Option<CoreWriteTxn>>>, f: F) -> PyResult<T>
where
    F: FnOnce(&mut CoreWriteTxn) -> PyResult<T>,
{
    let mut guard = inner.lock().map_err(lock_err)?;
    let txn = guard
        .as_mut()
        .ok_or_else(|| OverGraphError::new_err(EngineError::TxnClosed.to_string()))?;
    f(txn)
}

fn with_py_txn_ref<F, T>(inner: &Arc<Mutex<Option<CoreWriteTxn>>>, f: F) -> PyResult<T>
where
    F: FnOnce(&CoreWriteTxn) -> PyResult<T>,
{
    let guard = inner.lock().map_err(lock_err)?;
    let txn = guard
        .as_ref()
        .ok_or_else(|| OverGraphError::new_err(EngineError::TxnClosed.to_string()))?;
    f(txn)
}

fn with_py_txn_take<F, T>(inner: &Arc<Mutex<Option<CoreWriteTxn>>>, f: F) -> PyResult<T>
where
    F: FnOnce(&mut CoreWriteTxn) -> PyResult<T>,
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
    let labels = d.get_item("labels")?;
    let key = d.get_item("key")?;
    let local = d.get_item("local")?;
    let has_id = id.is_some();
    let has_key = labels.is_some() || key.is_some();
    let has_local = local.is_some();
    match (has_id, has_key, has_local) {
        (true, false, false) => Ok(TxnNodeRef::Id(id.unwrap().extract()?)),
        (false, true, false) => {
            let labels_value =
                labels.ok_or_else(|| PyValueError::new_err("node key ref requires labels"))?;
            let labels = parse_node_labels_arg(&labels_value, "node key ref labels")?;
            let [label]: [String; 1] = labels.try_into().map_err(|_| {
                PyValueError::new_err("node key ref labels must contain exactly one label")
            })?;
            Ok(TxnNodeRef::Key {
                label,
                key: key
                    .ok_or_else(|| PyValueError::new_err("node key ref requires key"))?
                    .extract()?,
            })
        }
        (false, false, true) => Ok(TxnNodeRef::Local(TxnLocalRef::Alias(
            local.unwrap().extract()?,
        ))),
        _ => Err(PyValueError::new_err(
            "node ref must be exactly one of {'id'}, {'labels', 'key'}, or {'local'}",
        )),
    }
}

fn parse_txn_edge_ref(d: &Bound<'_, PyDict>) -> PyResult<TxnEdgeRef> {
    let id = d.get_item("id")?;
    let from = d.get_item("from")?;
    let to = d.get_item("to")?;
    let label = d.get_item("label")?;
    let local = d.get_item("local")?;
    let has_id = id.is_some();
    let has_triple = from.is_some() || to.is_some() || label.is_some();
    let has_local = local.is_some();
    match (has_id, has_triple, has_local) {
        (true, false, false) => Ok(TxnEdgeRef::Id(id.unwrap().extract()?)),
        (false, true, false) => {
            let from = from.ok_or_else(|| PyValueError::new_err("edge ref requires from"))?;
            let to = to.ok_or_else(|| PyValueError::new_err("edge ref requires to"))?;
            Ok(TxnEdgeRef::Triple {
                from: parse_txn_node_ref(from.downcast::<PyDict>()?)?,
                to: parse_txn_node_ref(to.downcast::<PyDict>()?)?,
                label: label
                    .ok_or_else(|| PyValueError::new_err("edge ref requires label"))?
                    .extract()?,
            })
        }
        (false, false, true) => Ok(TxnEdgeRef::Local(TxnLocalRef::Alias(
            local.unwrap().extract()?,
        ))),
        _ => Err(PyValueError::new_err(
            "edge ref must be exactly one of {'id'}, {'from', 'to', 'label'}, or {'local'}",
        )),
    }
}

fn txn_node_ref_to_py(py: Python<'_>, value: TxnNodeRef) -> PyResult<PyObject> {
    let dict = PyDict::new(py);
    match value {
        TxnNodeRef::Id(id) => dict.set_item("id", id)?,
        TxnNodeRef::Key { label, key } => {
            dict.set_item("labels", vec![label])?;
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
        TxnEdgeRef::Triple { from, to, label } => {
            dict.set_item("from", txn_node_ref_to_py(py, from)?)?;
            dict.set_item("to", txn_node_ref_to_py(py, to)?)?;
            dict.set_item("label", label)?;
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
    dict.set_item("labels", view.labels)?;
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
    dict.set_item("label", view.label)?;
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
                labels: parse_node_labels_list_field(op, "labels", "upsert_node")?,
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
                label: op
                    .get_item("label")?
                    .ok_or_else(|| PyValueError::new_err("upsert_edge requires label"))?
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
        let labels = parse_node_labels_list_field(d, "labels", "Node input")?;
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
            labels,
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
        let label: String = d
            .get_item("label")?
            .ok_or_else(|| PyValueError::new_err("Edge input missing 'label'"))?
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
            label,
            props,
            weight,
            valid_from,
            valid_to,
        });
    }
    Ok(inputs)
}

fn parse_node_key_queries(list: &Bound<'_, PyList>) -> PyResult<Vec<NodeKeyQuery>> {
    let mut queries = Vec::with_capacity(list.len());
    for (index, item) in list.iter().enumerate() {
        let d = item.downcast::<PyDict>().map_err(|_| {
            PyTypeError::new_err(format!(
                "get_nodes_by_keys[{}] must be a dict with 'labels' and 'key'",
                index
            ))
        })?;
        let labels_value = d
            .get_item("labels")?
            .ok_or_else(|| PyValueError::new_err("node key query requires labels"))?;
        let labels = parse_node_labels_arg(&labels_value, "node key query labels")?;
        let [label]: [String; 1] = labels.try_into().map_err(|_| {
            PyValueError::new_err("node key query labels must contain exactly one label")
        })?;
        let key: String = d
            .get_item("key")?
            .ok_or_else(|| PyValueError::new_err("node key query requires key"))?
            .extract()?;
        queries.push(NodeKeyQuery { label, key });
    }
    Ok(queries)
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

    fn read_u8(&mut self) -> PyResult<u8> {
        self.ensure(1)?;
        let v = self.buf[self.pos];
        self.pos += 1;
        Ok(v)
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
    let magic = r.read_bytes(4)?;
    if magic != b"OGNB" {
        return Err(PyValueError::new_err(
            "Unsupported node binary batch format: expected OGNB version 2 header; old version 1 buffers are not supported",
        ));
    }
    let version = r.read_u16_le()?;
    if version != 2 {
        return Err(PyValueError::new_err(format!(
            "Unsupported node binary batch version {}; expected version 2",
            version
        )));
    }
    let count = r.read_u32_le()? as usize;
    // Cap allocation: minimum v2 node record is label_count + one 1-byte label + weight + key_len + props_len.
    let max_possible = buf.len().saturating_sub(10) / 14;
    let mut inputs = Vec::with_capacity(count.min(max_possible));
    for _ in 0..count {
        let label_count = r.read_u8()? as usize;
        if label_count == 0 || label_count > 10 {
            return Err(PyValueError::new_err(
                "node binary label_count must be between 1 and 10",
            ));
        }
        let mut labels = Vec::with_capacity(label_count);
        for _ in 0..label_count {
            let label_len = r.read_u16_le()? as usize;
            validate_binary_token_len(label_len, "label")?;
            let label_bytes = r.read_bytes(label_len)?;
            let label = std::str::from_utf8(label_bytes)
                .map_err(|e| PyValueError::new_err(format!("Invalid UTF-8 in node label: {}", e)))?
                .to_string();
            validate_py_type_token_name(&label, "node label")?;
            if labels.iter().any(|existing| existing == &label) {
                return Err(PyValueError::new_err(format!(
                    "node binary labels contain duplicate label '{}'",
                    label
                )));
            }
            labels.push(label);
        }
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
            labels,
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
    // Cap allocation: minimum edge record is 34 bytes (from + to + label_len + weight + valid_from + valid_to + props_len)
    let max_possible = buf.len().saturating_sub(4) / 34;
    let mut inputs = Vec::with_capacity(count.min(max_possible));
    for _ in 0..count {
        let from = r.read_u64_le()?;
        let to = r.read_u64_le()?;
        let label_len = r.read_u16_le()? as usize;
        validate_binary_token_len(label_len, "label")?;
        let label_bytes = r.read_bytes(label_len)?;
        let label = std::str::from_utf8(label_bytes)
            .map_err(|e| PyValueError::new_err(format!("Invalid UTF-8 in edge label: {}", e)))?
            .to_string();
        validate_py_type_token_name(&label, "edge label")?;
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
            label,
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

fn validate_binary_token_len(len: usize, field: &str) -> PyResult<()> {
    if len == 0 || len > 255 {
        return Err(PyValueError::new_err(format!(
            "{} length must be between 1 and 255 bytes",
            field
        )));
    }
    Ok(())
}

fn validate_py_type_token_name(name: &str, context: &str) -> PyResult<()> {
    if name.is_empty() {
        return Err(PyValueError::new_err(format!(
            "{} must not be empty",
            context
        )));
    }
    if name.len() > 255 {
        return Err(PyValueError::new_err(format!(
            "{} must be at most 255 UTF-8 bytes",
            context
        )));
    }
    if name.trim_matches(char::is_whitespace).len() != name.len() {
        return Err(PyValueError::new_err(format!(
            "{} must not contain leading or trailing whitespace",
            context
        )));
    }
    if name
        .chars()
        .any(|ch| ch == '\0' || (ch.is_ascii() && ch.is_control()))
    {
        return Err(PyValueError::new_err(format!(
            "{} must not contain ASCII control characters or NUL",
            context
        )));
    }
    Ok(())
}

// ============================================================
// Module registration
// ============================================================

#[pymodule]
fn overgraph(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<OverGraph>()?;
    m.add_class::<WriteTxn>()?;
    m.add_class::<DbStats>()?;
    m.add_class::<NodeView>()?;
    m.add_class::<EdgeView>()?;
    m.add_class::<PatchResult>()?;
    m.add_class::<TxnCommitResult>()?;
    m.add_class::<NeighborEntry>()?;
    m.add_class::<TraversalHit>()?;
    m.add_class::<VectorHit>()?;
    m.add_class::<NodeLabelInfo>()?;
    m.add_class::<EdgeLabelInfo>()?;
    m.add_class::<NodePropertyIndexInfo>()?;
    m.add_class::<EdgePropertyIndexInfo>()?;
    m.add_class::<NodeSchemaInfo>()?;
    m.add_class::<EdgeSchemaInfo>()?;
    m.add_class::<SchemaValidationReport>()?;
    m.add_class::<GraphSchemaValidationReportEntry>()?;
    m.add_class::<GraphSchemaCheckReport>()?;
    m.add_class::<GraphSchemaDropTargetResult>()?;
    m.add_class::<GraphSchemaPublishResult>()?;
    m.add_class::<SchemaViolation>()?;
    m.add_class::<PropertyRangeBound>()?;
    m.add_class::<PropertyRangeCursor>()?;
    m.add_class::<TraversalCursor>()?;
    m.add_class::<ShortestPath>()?;
    m.add_class::<Subgraph>()?;
    m.add_class::<PruneResult>()?;
    m.add_class::<NamedPrunePolicy>()?;
    m.add_class::<SegmentInfo>()?;
    m.add_class::<CompactionStats>()?;
    m.add_class::<CompactionProgress>()?;
    m.add_class::<IdPageResult>()?;
    m.add_class::<NodePageResult>()?;
    m.add_class::<EdgePageResult>()?;
    m.add_class::<NeighborPageResult>()?;
    m.add_class::<PropertyRangePageResult>()?;
    m.add_class::<TraversalPageResult>()?;
    m.add_class::<PprApproxMeta>()?;
    m.add_class::<PprResult>()?;
    m.add_class::<ExportEdge>()?;
    m.add_class::<AdjacencyExport>()?;
    m.add_class::<IdArray>()?;
    m.add_class::<IdArrayIter>()?;
    m.add_class::<ScrubReport>()?;
    m.add_class::<SegmentScrubResult>()?;
    m.add_class::<ComponentScrubFinding>()?;
    m.add("OverGraphError", m.py().get_type::<OverGraphError>())?;
    Ok(())
}
