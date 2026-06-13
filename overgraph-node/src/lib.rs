#![allow(clippy::type_complexity)]

use napi::bindgen_prelude::*;
use napi::threadsafe_function::ThreadsafeFunctionCallMode;
use napi::JsString;
use napi_derive::napi;
use overgraph::types::{
    CompoundIndexPlanDetails, GqlPath, GraphAggregateFunction, QueryPlanCompoundTargetKind,
};
use overgraph::{
    gql_referenced_param_names, AdjacencyExport as CoreAdjacencyExport,
    AllShortestPathsOptions as CoreAllShortestPathsOptions, CompactionPhase,
    CompactionStats as CoreCompactionStats, ComponentOptions, DatabaseEngine,
    DbOptions as CoreDbOptions, DbStats as CoreDbStats, DegreeOptions as CoreDegreeOptions,
    DenseMetric, DenseVectorConfig as CoreDenseVectorConfig,
    DenseVectorSchema as CoreDenseVectorSchema, Direction, EdgeFilterExpr,
    EdgeInput as CoreEdgeInput, EdgeLabelInfo as CoreEdgeLabelInfo,
    EdgeMetadataIndexField as CoreEdgeMetadataIndexField,
    EdgePropertyIndexInfo as CoreEdgePropertyIndexInfo, EdgeQuery, EdgeQueryOrder,
    EdgeSchema as CoreEdgeSchema, EdgeSchemaInfo as CoreEdgeSchemaInfo,
    EdgeValiditySchema as CoreEdgeValiditySchema, EdgeView as CoreEdgeView,
    EndpointLabelSchema as CoreEndpointLabelSchema, EngineError,
    ExportOptions as CoreExportOptions, FusionMode, GqlCapSummary, GqlEdge, GqlExecutionCapSummary,
    GqlExecutionExplain, GqlExecutionMode, GqlExecutionOptions, GqlExecutionResult,
    GqlExecutionStats, GqlExplain, GqlLoweringTarget, GqlNode, GqlParamValue, GqlParams, GqlRow,
    GqlRowOperation, GqlStatementKind, GqlValue, GraphBinaryOp, GraphCapExplain, GraphCaseBranch,
    GraphCursorExplain, GraphEdgeField, GraphEdgePattern as CoreGraphEdgePattern, GraphEdgeValue,
    GraphElementProjection, GraphExplainNode, GraphExpr, GraphFunction, GraphNodeField,
    GraphNodePattern as CoreGraphNodePattern, GraphNodeValue, GraphOptionalGroup,
    GraphOrderDirection, GraphOrderExplain, GraphOrderItem, GraphOutputMode, GraphOutputOptions,
    GraphPageRequest, GraphParamValue, GraphPatch as CoreGraphPatch, GraphPathField,
    GraphPathValue, GraphPatternPiece, GraphPipelineCapExplain, GraphPipelineExplain,
    GraphPipelineMatchStage, GraphPipelineOptions, GraphPipelineQuery, GraphPipelineResult,
    GraphPipelineStage, GraphPipelineStageExplain, GraphPipelineStats, GraphProjectItem,
    GraphProjectKind, GraphProjectStage, GraphProjectionExplain, GraphProjectionItems,
    GraphPropertySelection, GraphQueryOptions, GraphReturnItem, GraphReturnProjection, GraphRow,
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
    GraphVariableLengthPattern, GraphVectorSelection, HnswConfig,
    IsConnectedOptions as CoreIsConnectedOptions, LabelMatchMode as CoreLabelMatchMode,
    NeighborEntry as CoreNeighborEntry, NeighborOptions, NodeFilterExpr, NodeIdMap,
    NodeInput as CoreNodeInput, NodeKeyQuery,
    NodeLabelConstraintSchema as CoreNodeLabelConstraintSchema,
    NodeLabelFilter as CoreNodeLabelFilter, NodeLabelInfo as CoreNodeLabelInfo,
    NodeMetadataIndexField as CoreNodeMetadataIndexField,
    NodePropertyIndexInfo as CoreNodePropertyIndexInfo, NodeQuery, NodeQueryOrder,
    NodeSchema as CoreNodeSchema, NodeSchemaInfo as CoreNodeSchemaInfo, NodeView as CoreNodeView,
    NumericFieldSchema as CoreNumericFieldSchema, PageRequest, PageResult, PprAlgorithm,
    PprOptions, PprResult as CorePprResult, PropValue,
    PropertyRangeBound as CorePropertyRangeBound, PropertyRangeCursor as CorePropertyRangeCursor,
    PropertyRangePageRequest, PropertyRangePageResult as CorePropertyRangePageResult,
    PropertySchema as CorePropertySchema, PrunePolicy as CorePrunePolicy, PrunePolicyInfo,
    PruneResult as CorePruneResult, QueryEdgeIdsResult, QueryEdgesResult, QueryNodeIdsResult,
    QueryNodesResult, QueryPlan, QueryPlanKind, QueryPlanNode, QueryPlanWarning,
    SchemaAdditionalProperties as CoreSchemaAdditionalProperties,
    SchemaCheckOptions as CoreSchemaCheckOptions, SchemaNumericBound as CoreSchemaNumericBound,
    SchemaSetOptions as CoreSchemaSetOptions, SchemaTargetKind as CoreSchemaTargetKind,
    SchemaValidationReport as CoreSchemaValidationReport, SchemaValueType as CoreSchemaValueType,
    SchemaVectorPresence as CoreSchemaVectorPresence, SchemaViolation as CoreSchemaViolation,
    SchemaViolationTarget as CoreSchemaViolationTarget, ScoringMode,
    ScrubReport as CoreScrubReport, SecondaryIndexField as CoreSecondaryIndexField,
    SecondaryIndexKind as CoreSecondaryIndexKind, SecondaryIndexSpec as CoreSecondaryIndexSpec,
    SecondaryIndexState, ShortestPath as CoreShortestPath,
    ShortestPathOptions as CoreShortestPathOptions, SparseVectorSchema as CoreSparseVectorSchema,
    StringFieldSchema as CoreStringFieldSchema, Subgraph, SubgraphOptions, TopKOptions,
    TraversalCursor as CoreTraversalCursor, TraversalHit as CoreTraversalHit,
    TraversalPageResult as CoreTraversalPageResult, TraverseOptions as CoreTraverseOptions,
    TxnCommitResult as CoreTxnCommitResult, TxnEdgeRef as CoreTxnEdgeRef,
    TxnEdgeView as CoreTxnEdgeView, TxnIntent, TxnLocalRef, TxnNodeRef as CoreTxnNodeRef,
    TxnNodeView as CoreTxnNodeView, UpsertEdgeOptions as CoreUpsertEdgeOptions,
    UpsertNodeOptions as CoreUpsertNodeOptions, VectorHit as CoreVectorHit, VectorSearchMode,
    VectorSearchRequest, VectorSearchScope as CoreVectorSearchScope, WalSyncMode,
    WriteTxn as CoreWriteTxn,
};

/// ThreadsafeFunction with `CalleeHandled = false` so the JS callback
/// receives `(progress)` directly, not error-first `(null, progress)`.
type ProgressTsfn = napi::threadsafe_function::ThreadsafeFunction<
    CompactionProgress,
    Unknown<'static>,
    CompactionProgress,
    Status,
    false,
>;

pub struct JsonPayload(serde_json::Value);

impl TypeName for JsonPayload {
    fn type_name() -> &'static str {
        "Object"
    }

    fn value_type() -> napi::ValueType {
        napi::ValueType::Object
    }
}

impl ToNapiValue for JsonPayload {
    unsafe fn to_napi_value(env: napi::sys::napi_env, val: Self) -> Result<napi::sys::napi_value> {
        unsafe { serde_json::Value::to_napi_value(env, val.0) }
    }
}

pub struct GqlJsPayload(GqlJsPayloadKind);

enum GqlJsPayloadKind {
    Result(GqlExecutionResultPayload),
    Explain(GqlExecutionExplain),
}

pub struct GqlExecutionResultPayload {
    result: GqlExecutionResult,
    compact_rows: bool,
}

struct GqlJsValue(GqlValue);

struct GqlJsExplain(GqlExecutionExplain);
struct GqlJsReadExplain(GqlExplain);

pub struct GraphRowResultPayload {
    result: GraphRowResult,
    compact_rows: bool,
}

pub struct GraphPipelineResultPayload {
    result: GraphPipelineResult,
    compact_rows: bool,
}

struct GraphJsValue(GraphValue);

pub struct GraphJsExplain(GraphRowExplain);
pub struct GraphPipelineJsExplain(GraphPipelineExplain);

impl TypeName for GqlJsPayload {
    fn type_name() -> &'static str {
        "Object"
    }

    fn value_type() -> napi::ValueType {
        napi::ValueType::Object
    }
}

impl ToNapiValue for GqlJsPayload {
    unsafe fn to_napi_value(env: napi::sys::napi_env, val: Self) -> Result<napi::sys::napi_value> {
        match val.0 {
            GqlJsPayloadKind::Result(payload) => unsafe {
                GqlExecutionResultPayload::to_napi_value(env, payload)
            },
            GqlJsPayloadKind::Explain(explain) => unsafe {
                GqlJsExplain::to_napi_value(env, GqlJsExplain(explain))
            },
        }
    }
}

impl ToNapiValue for GqlExecutionResultPayload {
    unsafe fn to_napi_value(env: napi::sys::napi_env, val: Self) -> Result<napi::sys::napi_value> {
        let env = Env::from_raw(env);
        let mut object = Object::new(&env)?;
        object.set("columns", val.result.columns.clone())?;
        object.set("kind", gql_statement_kind_to_js(val.result.kind))?;
        let rows =
            gql_rows_to_js_array(&env, &val.result.columns, val.result.rows, val.compact_rows)?;
        object.set("rows", rows)?;
        object.set("nextCursor", val.result.next_cursor)?;
        object.set("stats", GqlJsValue(gql_stats_to_value(val.result.stats)))?;
        match val.result.mutation_stats {
            Some(stats) => object.set(
                "mutationStats",
                GqlJsValue(gql_mutation_stats_to_value(stats)),
            )?,
            None => object.set("mutationStats", Option::<serde_json::Value>::None)?,
        }
        match val.result.schema_stats {
            Some(stats) => {
                object.set("schemaStats", GqlJsValue(gql_schema_stats_to_value(stats)))?
            }
            None => object.set("schemaStats", Option::<serde_json::Value>::None)?,
        }
        match val.result.index_stats {
            Some(stats) => object.set("indexStats", GqlJsValue(gql_index_stats_to_value(stats)))?,
            None => object.set("indexStats", Option::<serde_json::Value>::None)?,
        }
        match val.result.plan {
            Some(plan) => object.set("plan", GqlJsExplain(plan))?,
            None => object.set("plan", Option::<serde_json::Value>::None)?,
        }
        unsafe { <&Object<'_> as ToNapiValue>::to_napi_value(env.raw(), &object) }
    }
}

impl ToNapiValue for GqlJsExplain {
    unsafe fn to_napi_value(env: napi::sys::napi_env, val: Self) -> Result<napi::sys::napi_value> {
        let env = Env::from_raw(env);
        let mut object = Object::new(&env)?;
        object.set("kind", gql_statement_kind_to_js(val.0.kind))?;
        object.set("columns", val.0.columns)?;
        match val.0.read {
            Some(read) => object.set("read", GqlJsReadExplain(read))?,
            None => object.set("read", Option::<serde_json::Value>::None)?,
        }
        match val.0.mutation {
            Some(mutation) => object.set("mutation", gql_mutation_explain_to_json(mutation))?,
            None => object.set("mutation", Option::<serde_json::Value>::None)?,
        }
        match val.0.schema {
            Some(schema) => object.set("schema", gql_schema_explain_to_json(schema))?,
            None => object.set("schema", Option::<serde_json::Value>::None)?,
        }
        match val.0.index {
            Some(index) => object.set("index", gql_index_explain_to_json(index))?,
            None => object.set("index", Option::<serde_json::Value>::None)?,
        }
        object.set("caps", GqlJsValue(gql_execution_caps_to_value(val.0.caps)))?;
        object.set("warnings", val.0.warnings)?;
        object.set("notes", val.0.notes)?;
        unsafe { <&Object<'_> as ToNapiValue>::to_napi_value(env.raw(), &object) }
    }
}

impl ToNapiValue for GqlJsReadExplain {
    unsafe fn to_napi_value(env: napi::sys::napi_env, val: Self) -> Result<napi::sys::napi_value> {
        let env = Env::from_raw(env);
        let mut object = Object::new(&env)?;
        object.set("columns", val.0.columns)?;
        object.set("target", gql_lowering_target_to_js(val.0.target))?;
        match val.0.native_plan {
            Some(plan) => object.set("nativePlan", query_plan_to_json(plan))?,
            None => object.set("nativePlan", Option::<serde_json::Value>::None)?,
        }
        object.set("pushedDown", val.0.pushed_down)?;
        object.set("residual", val.0.residual)?;
        object.set("projection", val.0.projection)?;
        object.set(
            "rowOps",
            val.0
                .row_ops
                .into_iter()
                .map(gql_row_operation_to_js)
                .collect::<Vec<_>>(),
        )?;
        object.set("caps", GqlJsValue(gql_caps_to_value(val.0.caps)))?;
        object.set("warnings", val.0.warnings)?;
        unsafe { <&Object<'_> as ToNapiValue>::to_napi_value(env.raw(), &object) }
    }
}

impl ToNapiValue for GqlJsValue {
    unsafe fn to_napi_value(env: napi::sys::napi_env, val: Self) -> Result<napi::sys::napi_value> {
        gql_value_to_napi(env, val.0)
    }
}

impl TypeName for GraphRowResultPayload {
    fn type_name() -> &'static str {
        "Object"
    }

    fn value_type() -> napi::ValueType {
        napi::ValueType::Object
    }
}

impl ToNapiValue for GraphRowResultPayload {
    unsafe fn to_napi_value(env: napi::sys::napi_env, val: Self) -> Result<napi::sys::napi_value> {
        let env = Env::from_raw(env);
        let mut object = Object::new(&env)?;
        object.set("columns", val.result.columns.clone())?;
        let rows =
            graph_rows_to_js_array(&env, &val.result.columns, val.result.rows, val.compact_rows)?;
        object.set("rows", rows)?;
        object.set("nextCursor", val.result.next_cursor)?;
        object.set(
            "stats",
            GraphJsValue(graph_stats_to_value(val.result.stats)),
        )?;
        match val.result.plan {
            Some(plan) => object.set("plan", GraphJsExplain(plan))?,
            None => object.set("plan", Option::<serde_json::Value>::None)?,
        }
        unsafe { <&Object<'_> as ToNapiValue>::to_napi_value(env.raw(), &object) }
    }
}

impl TypeName for GraphPipelineResultPayload {
    fn type_name() -> &'static str {
        "Object"
    }

    fn value_type() -> napi::ValueType {
        napi::ValueType::Object
    }
}

impl ToNapiValue for GraphPipelineResultPayload {
    unsafe fn to_napi_value(env: napi::sys::napi_env, val: Self) -> Result<napi::sys::napi_value> {
        let env = Env::from_raw(env);
        let mut object = Object::new(&env)?;
        object.set("columns", val.result.columns.clone())?;
        let rows =
            graph_rows_to_js_array(&env, &val.result.columns, val.result.rows, val.compact_rows)?;
        object.set("rows", rows)?;
        object.set("nextCursor", val.result.next_cursor)?;
        object.set(
            "stats",
            GraphJsValue(graph_pipeline_stats_to_value(val.result.stats)),
        )?;
        match val.result.plan {
            Some(plan) => object.set("plan", GraphPipelineJsExplain(plan))?,
            None => object.set("plan", Option::<serde_json::Value>::None)?,
        }
        unsafe { <&Object<'_> as ToNapiValue>::to_napi_value(env.raw(), &object) }
    }
}

impl TypeName for GraphJsExplain {
    fn type_name() -> &'static str {
        "Object"
    }

    fn value_type() -> napi::ValueType {
        napi::ValueType::Object
    }
}

impl ToNapiValue for GraphJsExplain {
    unsafe fn to_napi_value(env: napi::sys::napi_env, val: Self) -> Result<napi::sys::napi_value> {
        let env = Env::from_raw(env);
        let json = graph_explain_to_json(val.0)?;
        unsafe { serde_json::Value::to_napi_value(env.raw(), json) }
    }
}

impl TypeName for GraphPipelineJsExplain {
    fn type_name() -> &'static str {
        "Object"
    }

    fn value_type() -> napi::ValueType {
        napi::ValueType::Object
    }
}

impl ToNapiValue for GraphPipelineJsExplain {
    unsafe fn to_napi_value(env: napi::sys::napi_env, val: Self) -> Result<napi::sys::napi_value> {
        let env = Env::from_raw(env);
        let json = graph_pipeline_explain_to_json(val.0)?;
        unsafe { serde_json::Value::to_napi_value(env.raw(), json) }
    }
}

impl ToNapiValue for GraphJsValue {
    unsafe fn to_napi_value(env: napi::sys::napi_env, val: Self) -> Result<napi::sys::napi_value> {
        graph_value_to_napi(env, val.0)
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
    pub fn open(path: String, options: Option<DbOptions>) -> Result<OverGraph> {
        let opts = options.map(|o| o.into()).unwrap_or_default();
        let engine = DatabaseEngine::open(Path::new(&path), &opts)
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        Ok(OverGraph {
            inner: Arc::new(Mutex::new(Some(InnerDb { engine }))),
        })
    }

    #[napi]
    pub fn close(&self, options: Option<CloseOptions>) -> Result<()> {
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

    // --- Catalog diagnostics ---

    #[napi]
    pub fn ensure_node_label(&self, label: String) -> Result<u32> {
        with_engine(self, |eng| eng.ensure_node_label(&label))
    }

    #[napi]
    pub fn ensure_edge_label(&self, label: String) -> Result<u32> {
        with_engine(self, |eng| eng.ensure_edge_label(&label))
    }

    #[napi]
    pub fn get_node_label_id(&self, label: String) -> Result<Option<u32>> {
        with_engine_ref(self, |eng| eng.get_node_label_id(&label))
    }

    #[napi]
    pub fn get_edge_label_id(&self, label: String) -> Result<Option<u32>> {
        with_engine_ref(self, |eng| eng.get_edge_label_id(&label))
    }

    #[napi]
    pub fn get_node_label(&self, label_id: u32) -> Result<Option<String>> {
        with_engine_ref(self, |eng| eng.get_node_label(label_id))
    }

    #[napi]
    pub fn get_edge_label(&self, label_id: u32) -> Result<Option<String>> {
        with_engine_ref(self, |eng| eng.get_edge_label(label_id))
    }

    #[napi]
    pub fn list_node_labels(&self) -> Result<Vec<NodeLabelInfo>> {
        let infos = with_engine_ref(self, |eng| eng.list_node_labels())?;
        Ok(infos.into_iter().map(Into::into).collect())
    }

    #[napi]
    pub fn list_edge_labels(&self) -> Result<Vec<EdgeLabelInfo>> {
        let infos = with_engine_ref(self, |eng| eng.list_edge_labels())?;
        Ok(infos.into_iter().map(Into::into).collect())
    }

    // --- Schema management ---

    #[napi(
        ts_args_type = "label: string, schema: import('./schema-types').NodeSchema, options?: SchemaSetOptions | null",
        ts_return_type = "import('./schema-types').NodeSchemaInfo"
    )]
    pub fn set_node_schema(
        &self,
        label: String,
        schema: Unknown<'_>,
        options: Option<SchemaSetOptions>,
    ) -> Result<NodeSchemaInfoPayload> {
        let schema = parse_js_node_schema(schema, "setNodeSchema schema")?;
        let options = schema_set_options_to_core(options)?;
        let info = with_engine(self, |eng| {
            eng.set_node_schema_with_options(&label, schema, options)
        })?;
        Ok(NodeSchemaInfoPayload(info))
    }

    #[napi(
        ts_args_type = "label: string, schema: import('./schema-types').NodeSchema, options?: SchemaCheckOptions | null",
        ts_return_type = "import('./schema-types').SchemaValidationReport"
    )]
    pub fn check_node_schema(
        &self,
        label: String,
        schema: Unknown<'_>,
        options: Option<SchemaCheckOptions>,
    ) -> Result<SchemaValidationReportPayload> {
        let schema = parse_js_node_schema(schema, "checkNodeSchema schema")?;
        let options = schema_check_options_to_core(options)?;
        let report = with_engine_ref(self, |eng| eng.check_node_schema(&label, schema, options))?;
        Ok(SchemaValidationReportPayload(report))
    }

    #[napi]
    pub fn drop_node_schema(&self, label: String) -> Result<bool> {
        with_engine(self, |eng| eng.drop_node_schema(&label))
    }

    #[napi(ts_return_type = "import('./schema-types').NodeSchemaInfo | null")]
    pub fn get_node_schema(&self, label: String) -> Result<Option<NodeSchemaInfoPayload>> {
        let info = with_engine_ref(self, |eng| eng.get_node_schema(&label))?;
        Ok(info.map(NodeSchemaInfoPayload))
    }

    #[napi(ts_return_type = "Array<import('./schema-types').NodeSchemaInfo>")]
    pub fn list_node_schemas(&self) -> Result<Vec<NodeSchemaInfoPayload>> {
        let infos = with_engine_ref(self, |eng| eng.list_node_schemas())?;
        Ok(infos.into_iter().map(NodeSchemaInfoPayload).collect())
    }

    #[napi(
        ts_args_type = "label: string, schema: import('./schema-types').EdgeSchema, options?: SchemaSetOptions | null",
        ts_return_type = "import('./schema-types').EdgeSchemaInfo"
    )]
    pub fn set_edge_schema(
        &self,
        label: String,
        schema: Unknown<'_>,
        options: Option<SchemaSetOptions>,
    ) -> Result<EdgeSchemaInfoPayload> {
        let schema = parse_js_edge_schema(schema, "setEdgeSchema schema")?;
        let options = schema_set_options_to_core(options)?;
        let info = with_engine(self, |eng| {
            eng.set_edge_schema_with_options(&label, schema, options)
        })?;
        Ok(EdgeSchemaInfoPayload(info))
    }

    #[napi(
        ts_args_type = "label: string, schema: import('./schema-types').EdgeSchema, options?: SchemaCheckOptions | null",
        ts_return_type = "import('./schema-types').SchemaValidationReport"
    )]
    pub fn check_edge_schema(
        &self,
        label: String,
        schema: Unknown<'_>,
        options: Option<SchemaCheckOptions>,
    ) -> Result<SchemaValidationReportPayload> {
        let schema = parse_js_edge_schema(schema, "checkEdgeSchema schema")?;
        let options = schema_check_options_to_core(options)?;
        let report = with_engine_ref(self, |eng| eng.check_edge_schema(&label, schema, options))?;
        Ok(SchemaValidationReportPayload(report))
    }

    #[napi]
    pub fn drop_edge_schema(&self, label: String) -> Result<bool> {
        with_engine(self, |eng| eng.drop_edge_schema(&label))
    }

    #[napi(ts_return_type = "import('./schema-types').EdgeSchemaInfo | null")]
    pub fn get_edge_schema(&self, label: String) -> Result<Option<EdgeSchemaInfoPayload>> {
        let info = with_engine_ref(self, |eng| eng.get_edge_schema(&label))?;
        Ok(info.map(EdgeSchemaInfoPayload))
    }

    #[napi(ts_return_type = "Array<import('./schema-types').EdgeSchemaInfo>")]
    pub fn list_edge_schemas(&self) -> Result<Vec<EdgeSchemaInfoPayload>> {
        let infos = with_engine_ref(self, |eng| eng.list_edge_schemas())?;
        Ok(infos.into_iter().map(EdgeSchemaInfoPayload).collect())
    }

    #[napi(
        ts_args_type = "schema: import('./schema-types').GraphSchema, options?: SchemaSetOptions | null",
        ts_return_type = "import('./schema-types').GraphSchemaPublishResult"
    )]
    pub fn set_graph_schema(
        &self,
        schema: Unknown<'_>,
        options: Option<SchemaSetOptions>,
    ) -> Result<GraphSchemaPublishResultPayload> {
        let schema = parse_js_graph_schema(schema, "setGraphSchema schema")?;
        let options = graph_schema_set_options_to_core(options)?;
        let result = with_engine(self, |eng| eng.set_graph_schema(schema, options))?;
        Ok(GraphSchemaPublishResultPayload(result))
    }

    #[napi(
        ts_args_type = "operations: Array<import('./schema-types').GraphSchemaOperation>, options?: SchemaSetOptions | null",
        ts_return_type = "import('./schema-types').GraphSchemaPublishResult"
    )]
    pub fn alter_graph_schema(
        &self,
        operations: Unknown<'_>,
        options: Option<SchemaSetOptions>,
    ) -> Result<GraphSchemaPublishResultPayload> {
        let operations =
            parse_js_graph_schema_operations(operations, "alterGraphSchema operations")?;
        let options = graph_schema_set_options_to_core(options)?;
        let result = with_engine(self, |eng| eng.alter_graph_schema(operations, options))?;
        Ok(GraphSchemaPublishResultPayload(result))
    }

    #[napi(
        ts_args_type = "schema: import('./schema-types').GraphSchema, options?: SchemaCheckOptions | null",
        ts_return_type = "import('./schema-types').GraphSchemaCheckReport"
    )]
    pub fn check_graph_schema_set(
        &self,
        schema: Unknown<'_>,
        options: Option<SchemaCheckOptions>,
    ) -> Result<GraphSchemaCheckReportPayload> {
        let schema = parse_js_graph_schema(schema, "checkGraphSchemaSet schema")?;
        let options = graph_schema_check_options_to_core(options)?;
        let report = with_engine_ref(self, |eng| eng.check_graph_schema_set(schema, options))?;
        Ok(GraphSchemaCheckReportPayload(report))
    }

    #[napi(
        ts_args_type = "schema: import('./schema-types').GraphSchema, options?: SchemaCheckOptions | null",
        ts_return_type = "import('./schema-types').GraphSchemaCheckReport"
    )]
    pub fn check_graph_schema_add(
        &self,
        schema: Unknown<'_>,
        options: Option<SchemaCheckOptions>,
    ) -> Result<GraphSchemaCheckReportPayload> {
        let schema = parse_js_graph_schema(schema, "checkGraphSchemaAdd schema")?;
        let options = graph_schema_check_options_to_core(options)?;
        let report = with_engine_ref(self, |eng| eng.check_graph_schema_add(schema, options))?;
        Ok(GraphSchemaCheckReportPayload(report))
    }

    #[napi(ts_return_type = "import('./schema-types').GraphSchemaPublishResult")]
    pub fn drop_graph_schema(&self) -> Result<GraphSchemaPublishResultPayload> {
        let result = with_engine(self, |eng| eng.drop_graph_schema())?;
        Ok(GraphSchemaPublishResultPayload(result))
    }

    // --- Single upserts ---

    #[napi(
        ts_args_type = "labels: string | string[], key: string, options?: UpsertNodeOptions | null"
    )]
    pub fn upsert_node(
        &self,
        labels: serde_json::Value,
        key: String,
        options: Option<UpsertNodeOptions>,
    ) -> Result<f64> {
        let labels = parse_js_node_labels_arg(&labels, "upsertNode labels")?;
        let (props, weight, dense_vector, sparse_vector) = match options {
            Some(o) => (o.props, o.weight, o.dense_vector, o.sparse_vector),
            None => (None, None, None, None),
        };
        let props = convert_js_props(props);
        let opts = CoreUpsertNodeOptions {
            props,
            weight: weight.unwrap_or(1.0) as f32,
            dense_vector: dense_vector.map(|dv| dv.into_iter().map(|x| x as f32).collect()),
            sparse_vector: sparse_vector.map(|sv| {
                sv.into_iter()
                    .map(|e| (e.dimension, e.value as f32))
                    .collect()
            }),
        };
        let id = with_engine(self, |eng| eng.upsert_node(labels, &key, opts))?;
        u64_to_f64(id)
    }

    #[napi]
    pub fn add_node_label(&self, node_id: f64, label: String) -> Result<bool> {
        let node_id = f64_to_u64(node_id)?;
        with_engine(self, |eng| eng.add_node_label(node_id, &label))
    }

    #[napi]
    pub fn remove_node_label(&self, node_id: f64, label: String) -> Result<bool> {
        let node_id = f64_to_u64(node_id)?;
        with_engine(self, |eng| eng.remove_node_label(node_id, &label))
    }

    #[napi]
    pub fn upsert_edge(
        &self,
        from: f64,
        to: f64,
        label: String,
        options: Option<UpsertEdgeOptions>,
    ) -> Result<f64> {
        let from = f64_to_u64(from)?;
        let to = f64_to_u64(to)?;
        let (props, weight, valid_from, valid_to) = match options {
            Some(o) => (o.props, o.weight, o.valid_from, o.valid_to),
            None => (None, None, None, None),
        };
        let props = convert_js_props(props);
        let opts = CoreUpsertEdgeOptions {
            props,
            weight: weight.unwrap_or(1.0) as f32,
            valid_from,
            valid_to,
        };
        let id = with_engine(self, |eng| eng.upsert_edge(from, to, &label, opts))?;
        u64_to_f64(id)
    }

    // --- Batch upserts (JSON object path) ---

    #[napi]
    pub fn batch_upsert_nodes(&self, nodes: Vec<NodeInput>) -> Result<Float64Array> {
        let inputs: Vec<CoreNodeInput> = nodes
            .into_iter()
            .map(NodeInput::try_into)
            .collect::<Result<Vec<_>>>()?;
        let ids = with_engine(self, |eng| eng.batch_upsert_nodes(inputs))?;
        ids_to_float64_array(&ids)
    }

    #[napi]
    pub fn batch_upsert_edges(&self, edges: Vec<EdgeInput>) -> Result<Float64Array> {
        let inputs: std::result::Result<Vec<CoreEdgeInput>, _> =
            edges.into_iter().map(|e| e.try_into()).collect();
        let inputs = inputs?;
        let ids = with_engine(self, |eng| eng.batch_upsert_edges(inputs))?;
        ids_to_float64_array(&ids)
    }

    // --- Batch upserts (binary buffer path) ---

    /// Batch upsert nodes from a packed binary Buffer. See `packNodeBatch()` in JS.
    ///
    /// Binary format (little-endian):
    ///   [magic: 4 bytes "OGNB"][version: u16 = 2][count: u32]
    ///   per node:
    ///     [label_count: u8] repeated [label_len: u16][label: utf8][weight: f32]
    ///     [key_len: u16][key: utf8][props_len: u32][props: json utf8]
    #[napi]
    pub fn batch_upsert_nodes_binary(&self, buffer: Buffer) -> Result<Float64Array> {
        let inputs = decode_node_batch(&buffer)?;
        let ids = with_engine(self, |eng| eng.batch_upsert_nodes(inputs))?;
        ids_to_float64_array(&ids)
    }

    /// Batch upsert edges from a packed binary Buffer. See `packEdgeBatch()` in JS.
    ///
    /// Binary format (little-endian):
    ///   [magic: 4 bytes "OGEB"][version: u16 = 1][count: u32]
    ///   per edge:
    ///     [from: u64][to: u64][label_len: u16][label: utf8][weight: f32]
    ///     [valid_from: i64][valid_to: i64][props_len: u32][props: json utf8]
    /// In this packed format, valid_from=0 and valid_to=0 are sentinels for
    /// engine defaults (created_at and no expiration), not explicit epoch 0.
    #[napi]
    pub fn batch_upsert_edges_binary(&self, buffer: Buffer) -> Result<Float64Array> {
        let inputs = decode_edge_batch(&buffer)?;
        let ids = with_engine(self, |eng| eng.batch_upsert_edges(inputs))?;
        ids_to_float64_array(&ids)
    }

    // --- Gets ---

    #[napi]
    pub fn get_node(&self, id: f64) -> Result<Option<NodeView>> {
        let id = f64_to_u64(id)?;
        let raw = with_engine_ref(self, |eng| eng.get_node(id))?;
        raw.map(NodeView::try_from).transpose()
    }

    #[napi]
    pub fn get_edge(&self, id: f64) -> Result<Option<EdgeView>> {
        let id = f64_to_u64(id)?;
        let raw = with_engine_ref(self, |eng| eng.get_edge(id))?;
        raw.map(EdgeView::try_from).transpose()
    }

    // --- Key/triple lookups ---

    #[napi]
    pub fn get_node_by_key(&self, label: String, key: String) -> Result<Option<NodeView>> {
        let raw = with_engine_ref(self, |eng| eng.get_node_by_key(&label, &key))?;
        raw.map(NodeView::try_from).transpose()
    }

    #[napi]
    pub fn get_edge_by_triple(
        &self,
        from: f64,
        to: f64,
        label: String,
    ) -> Result<Option<EdgeView>> {
        let from = f64_to_u64(from)?;
        let to = f64_to_u64(to)?;
        let raw = with_engine_ref(self, |eng| eng.get_edge_by_triple(from, to, &label))?;
        raw.map(EdgeView::try_from).transpose()
    }

    // --- Bulk reads ---

    #[napi]
    pub fn get_nodes(&self, ids: Vec<f64>) -> Result<Vec<Option<NodeView>>> {
        let ids: Vec<u64> = ids
            .into_iter()
            .map(f64_to_u64)
            .collect::<Result<Vec<_>>>()?;
        let results = with_engine_ref(self, |eng| eng.get_nodes(&ids))?;
        results
            .into_iter()
            .map(|r| r.map(NodeView::try_from).transpose())
            .collect::<Result<Vec<_>>>()
    }

    #[napi]
    pub fn get_nodes_by_keys(&self, keys: Vec<KeyQuery>) -> Result<Vec<Option<NodeView>>> {
        let owned: Vec<NodeKeyQuery> = keys
            .into_iter()
            .map(KeyQuery::try_into)
            .collect::<Result<Vec<_>>>()?;
        let results = with_engine_ref(self, |eng| eng.get_nodes_by_keys(&owned))?;
        results
            .into_iter()
            .map(|r| r.map(NodeView::try_from).transpose())
            .collect::<Result<Vec<_>>>()
    }

    #[napi]
    pub fn get_edges(&self, ids: Vec<f64>) -> Result<Vec<Option<EdgeView>>> {
        let ids: Vec<u64> = ids
            .into_iter()
            .map(f64_to_u64)
            .collect::<Result<Vec<_>>>()?;
        let results = with_engine_ref(self, |eng| eng.get_edges(&ids))?;
        results
            .into_iter()
            .map(|r| r.map(EdgeView::try_from).transpose())
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
    pub fn invalidate_edge(&self, id: f64, valid_to: i64) -> Result<Option<EdgeView>> {
        let id = f64_to_u64(id)?;
        let raw = with_engine(self, |eng| eng.invalidate_edge(id, valid_to))?;
        raw.map(EdgeView::try_from).transpose()
    }

    #[napi]
    pub fn graph_patch(&self, patch: GraphPatch) -> Result<PatchResult> {
        let rust_patch = js_patch_to_rust(patch)?;
        let result = with_engine(self, |eng| eng.graph_patch(rust_patch))?;
        Ok(PatchResult {
            node_ids: ids_to_float64_array(&result.node_ids)?,
            edge_ids: ids_to_float64_array(&result.edge_ids)?,
        })
    }

    #[napi]
    pub fn begin_write_txn(&self) -> Result<WriteTxn> {
        let txn = with_engine_ref(self, |eng| eng.begin_write_txn())?;
        Ok(write_txn_to_js(txn))
    }

    #[napi(ts_return_type = "Promise<WriteTxn>")]
    pub fn begin_write_txn_async(&self) -> AsyncTask<EngineReadOp<CoreWriteTxn, WriteTxn>> {
        AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            |eng| eng.begin_write_txn(),
            |txn| Ok(write_txn_to_js(txn)),
        ))
    }

    // --- Retention / Forgetting ---

    #[napi]
    pub fn prune(&self, policy: PrunePolicy) -> Result<PruneResult> {
        let rust_policy = js_prune_policy_to_rust(policy, "prune")?;
        with_engine(self, |eng| {
            let result = eng.prune(&rust_policy)?;
            Ok(PruneResult {
                nodes_pruned: result.nodes_pruned as i64,
                edges_pruned: result.edges_pruned as i64,
            })
        })
    }

    // --- Named prune policies (compaction-filter auto-prune) ---

    #[napi]
    pub fn set_prune_policy(&self, name: String, policy: PrunePolicy) -> Result<()> {
        let rust_policy = js_prune_policy_to_rust(policy, "setPrunePolicy")?;
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
    pub fn list_prune_policies(&self) -> Result<Vec<NamedPrunePolicy>> {
        with_engine_ref(self, |eng| {
            Ok(eng
                .list_prune_policies()?
                .into_iter()
                .map(|info| NamedPrunePolicy {
                    name: info.name,
                    policy: PrunePolicy {
                        max_age_ms: info.policy.max_age_ms.map(|v| v as f64),
                        max_weight: info.policy.max_weight.map(|v| v as f64),
                        label: info.policy.label,
                    },
                })
                .collect())
        })
    }

    #[napi(ts_return_type = "Promise<void>")]
    pub fn set_prune_policy_async(
        &self,
        name: String,
        policy: PrunePolicy,
    ) -> Result<AsyncTask<EngineOp<(), ()>>> {
        let rust_policy = js_prune_policy_to_rust(policy, "setPrunePolicyAsync")?;
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

    #[napi(ts_return_type = "Promise<Array<NamedPrunePolicy>>")]
    pub fn list_prune_policies_async(
        &self,
    ) -> Result<AsyncTask<EngineReadOp<Vec<PrunePolicyInfo>, Vec<NamedPrunePolicy>>>> {
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.list_prune_policies(),
            |policies| {
                Ok(policies
                    .into_iter()
                    .map(|info| NamedPrunePolicy {
                        name: info.name,
                        policy: PrunePolicy {
                            max_age_ms: info.policy.max_age_ms.map(|v| v as f64),
                            max_weight: info.policy.max_weight.map(|v| v as f64),
                            label: info.policy.label,
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
        options: Option<NeighborsOptions>,
    ) -> Result<Vec<NeighborEntry>> {
        let node_id = f64_to_u64(node_id)?;
        let (direction, edge_label_filter, limit, at_epoch, decay_lambda) = match options {
            Some(o) => (
                o.direction,
                o.edge_label_filter,
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
            edge_label_filter,
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
        options: Option<TraverseOptions>,
    ) -> Result<TraversalPageResult> {
        let start_node_id = f64_to_u64(start_node_id)?;
        let (
            direction,
            min_depth,
            edge_label_filter,
            emit_node_label_filter,
            at_epoch,
            decay_lambda,
            limit,
            cursor,
        ) = match options {
            Some(o) => (
                o.direction,
                o.min_depth,
                o.edge_label_filter,
                o.emit_node_label_filter
                    .map(js_node_label_filter_to_rust)
                    .transpose()?,
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
        let opts = CoreTraverseOptions {
            min_depth,
            direction: dir,
            edge_label_filter,
            emit_node_label_filter,
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
        options: Option<TopKNeighborsOptions>,
    ) -> Result<Vec<NeighborEntry>> {
        let node_id = f64_to_u64(node_id)?;
        let (direction, edge_label_filter, scoring, decay_lambda, at_epoch) = match options {
            Some(o) => (
                o.direction,
                o.edge_label_filter,
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
            edge_label_filter,
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
        options: Option<ExtractSubgraphOptions>,
    ) -> Result<SubgraphResult> {
        let start = f64_to_u64(start_node_id)?;
        let (direction, edge_label_filter, node_label_filter, at_epoch) = match options {
            Some(o) => (
                o.direction,
                o.edge_label_filter,
                o.node_label_filter
                    .map(js_node_label_filter_to_rust)
                    .transpose()?,
                o.at_epoch,
            ),
            None => (None, None, None, None),
        };
        let dir = parse_direction(direction.as_deref())?;
        let opts = SubgraphOptions {
            direction: dir,
            edge_label_filter,
            node_label_filter,
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
        options: Option<NeighborsBatchOptions>,
    ) -> Result<Vec<NeighborBatchEntry>> {
        let ids: Vec<u64> = node_ids
            .into_iter()
            .map(f64_to_u64)
            .collect::<Result<Vec<_>>>()?;
        let (direction, edge_label_filter, at_epoch, decay_lambda) = match options {
            Some(o) => (o.direction, o.edge_label_filter, o.at_epoch, o.decay_lambda),
            None => (None, None, None, None),
        };
        let dir = parse_direction(direction.as_deref())?;
        let decay = decay_lambda.map(|v| v as f32);
        let opts = NeighborOptions {
            direction: dir,
            edge_label_filter,
            limit: None,
            at_epoch,
            decay_lambda: decay,
        };
        let map = with_engine_ref(self, |eng| eng.neighbors_batch(&ids, &opts))?;
        convert_batch_result(map)
    }

    // --- Degree counts + aggregations (Phase 18a) ---

    #[napi]
    pub fn degree(&self, node_id: f64, options: Option<DegreeOptions>) -> Result<i64> {
        let node_id = f64_to_u64(node_id)?;
        let (direction, edge_label_filter, at_epoch) = match options {
            Some(o) => (o.direction, o.edge_label_filter, o.at_epoch),
            None => (None, None, None),
        };
        let dir = parse_direction(direction.as_deref())?;
        let opts = CoreDegreeOptions {
            direction: dir,
            edge_label_filter,
            at_epoch,
        };
        let count: u64 = with_engine_ref(self, |eng| eng.degree(node_id, &opts))?;
        u64_to_safe_i64(count)
    }

    #[napi]
    pub fn sum_edge_weights(
        &self,
        node_id: f64,
        options: Option<SumEdgeWeightsOptions>,
    ) -> Result<f64> {
        let node_id = f64_to_u64(node_id)?;
        let (direction, edge_label_filter, at_epoch) = match options {
            Some(o) => (o.direction, o.edge_label_filter, o.at_epoch),
            None => (None, None, None),
        };
        let dir = parse_direction(direction.as_deref())?;
        let opts = CoreDegreeOptions {
            direction: dir,
            edge_label_filter,
            at_epoch,
        };
        with_engine_ref(self, |eng| eng.sum_edge_weights(node_id, &opts))
    }

    #[napi]
    pub fn avg_edge_weight(
        &self,
        node_id: f64,
        options: Option<AvgEdgeWeightOptions>,
    ) -> Result<Option<f64>> {
        let node_id = f64_to_u64(node_id)?;
        let (direction, edge_label_filter, at_epoch) = match options {
            Some(o) => (o.direction, o.edge_label_filter, o.at_epoch),
            None => (None, None, None),
        };
        let dir = parse_direction(direction.as_deref())?;
        let opts = CoreDegreeOptions {
            direction: dir,
            edge_label_filter,
            at_epoch,
        };
        with_engine_ref(self, |eng| eng.avg_edge_weight(node_id, &opts))
    }

    #[napi]
    pub fn degrees(
        &self,
        node_ids: Vec<f64>,
        options: Option<DegreesOptions>,
    ) -> Result<Vec<DegreeBatchEntry>> {
        let ids: Vec<u64> = node_ids
            .into_iter()
            .map(f64_to_u64)
            .collect::<Result<Vec<_>>>()?;
        let (direction, edge_label_filter, at_epoch) = match options {
            Some(o) => (o.direction, o.edge_label_filter, o.at_epoch),
            None => (None, None, None),
        };
        let dir = parse_direction(direction.as_deref())?;
        let opts = CoreDegreeOptions {
            direction: dir,
            edge_label_filter,
            at_epoch,
        };
        let map = with_engine_ref(self, |eng| eng.degrees(&ids, &opts))?;
        let mut entries: Vec<DegreeBatchEntry> = map
            .into_iter()
            .map(|(node_id, degree)| {
                Ok(DegreeBatchEntry {
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
        options: Option<ShortestPathOptions>,
    ) -> Result<Option<ShortestPath>> {
        let from = f64_to_u64(from)?;
        let to = f64_to_u64(to)?;
        let (direction, edge_label_filter, weight_field, at_epoch, max_depth, max_cost) =
            match options {
                Some(o) => (
                    o.direction,
                    o.edge_label_filter,
                    o.weight_field,
                    o.at_epoch,
                    o.max_depth,
                    o.max_cost,
                ),
                None => (None, None, None, None, None, None),
            };
        let dir = parse_direction(direction.as_deref())?;
        let opts = CoreShortestPathOptions {
            direction: dir,
            edge_label_filter,
            weight_field,
            at_epoch,
            max_depth,
            max_cost,
        };
        let result: Option<CoreShortestPath> =
            with_engine_ref(self, |eng| eng.shortest_path(from, to, &opts))?;
        result.map(shortest_path_to_js).transpose()
    }

    #[napi]
    pub fn is_connected(
        &self,
        from: f64,
        to: f64,
        options: Option<IsConnectedOptions>,
    ) -> Result<bool> {
        let from = f64_to_u64(from)?;
        let to = f64_to_u64(to)?;
        let (direction, edge_label_filter, at_epoch, max_depth) = match options {
            Some(o) => (o.direction, o.edge_label_filter, o.at_epoch, o.max_depth),
            None => (None, None, None, None),
        };
        let dir = parse_direction(direction.as_deref())?;
        let opts = CoreIsConnectedOptions {
            direction: dir,
            edge_label_filter,
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
        options: Option<AllShortestPathsOptions>,
    ) -> Result<Vec<ShortestPath>> {
        let from = f64_to_u64(from)?;
        let to = f64_to_u64(to)?;
        let (direction, edge_label_filter, weight_field, at_epoch, max_depth, max_cost, max_paths) =
            match options {
                Some(o) => (
                    o.direction,
                    o.edge_label_filter,
                    o.weight_field,
                    o.at_epoch,
                    o.max_depth,
                    o.max_cost,
                    o.max_paths,
                ),
                None => (None, None, None, None, None, None, None),
            };
        let dir = parse_direction(direction.as_deref())?;
        let opts = CoreAllShortestPathsOptions {
            direction: dir,
            edge_label_filter,
            weight_field,
            at_epoch,
            max_depth,
            max_cost,
            max_paths: max_paths.map(|n| n as usize),
        };
        let paths: Vec<CoreShortestPath> =
            with_engine_ref(self, |eng| eng.all_shortest_paths(from, to, &opts))?;
        paths.into_iter().map(shortest_path_to_js).collect()
    }

    #[napi]
    pub fn find_nodes(
        &self,
        label: String,
        prop_key: String,
        prop_value: serde_json::Value,
    ) -> Result<Float64Array> {
        let pv = json_to_prop_value(&prop_value);
        let ids = with_engine_ref(self, |eng| eng.find_nodes(&label, &prop_key, &pv))?;
        ids_to_float64_array(&ids)
    }

    #[napi(
        ts_args_type = "request: import('./query-types').QueryNodeRequest",
        ts_return_type = "IdPageResult"
    )]
    pub fn query_node_ids(&self, request: serde_json::Value) -> Result<IdPageResult> {
        let query = parse_js_node_query(&request)?;
        let result = with_engine_ref(self, |eng| eng.query_node_ids(&query))?;
        query_node_ids_to_js(result)
    }

    #[napi(
        ts_args_type = "request: import('./query-types').QueryNodeRequest",
        ts_return_type = "NodePageResult"
    )]
    pub fn query_nodes(&self, request: serde_json::Value) -> Result<NodePageResult> {
        let query = parse_js_node_query(&request)?;
        let result = with_engine_ref(self, |eng| eng.query_nodes(&query))?;
        query_nodes_to_js(result)
    }

    #[napi(
        ts_args_type = "request: import('./query-types').QueryEdgeRequest",
        ts_return_type = "IdPageResult"
    )]
    pub fn query_edge_ids(&self, request: serde_json::Value) -> Result<IdPageResult> {
        let query = parse_js_edge_query(&request)?;
        let result = with_engine_ref(self, |eng| eng.query_edge_ids(&query))?;
        query_edge_ids_to_js(result)
    }

    #[napi(
        ts_args_type = "request: import('./query-types').QueryEdgeRequest",
        ts_return_type = "EdgePageResult"
    )]
    pub fn query_edges(&self, request: serde_json::Value) -> Result<EdgePageResult> {
        let query = parse_js_edge_query(&request)?;
        let result = with_engine_ref(self, |eng| eng.query_edges(&query))?;
        query_edges_to_js(result)
    }

    #[napi(
        ts_args_type = "request: import('./query-types').GraphRowRequest",
        ts_return_type = "import('./query-types').GraphRowResult"
    )]
    pub fn query_graph_rows(&self, request: serde_json::Value) -> Result<GraphRowResultPayload> {
        let query = parse_js_graph_row_query(&request)?;
        let compact_rows = query.output.compact_rows;
        let result = with_engine_ref(self, |eng| eng.query_graph_rows(&query))?;
        Ok(GraphRowResultPayload {
            result,
            compact_rows,
        })
    }

    #[napi(
        ts_args_type = "request: import('./query-types').GraphPipelineRequest",
        ts_return_type = "import('./query-types').GraphPipelineResult"
    )]
    pub fn query_graph_pipeline(
        &self,
        request: serde_json::Value,
    ) -> Result<GraphPipelineResultPayload> {
        let query = parse_js_graph_pipeline_query(&request)?;
        let compact_rows = query.output.compact_rows;
        let result = with_engine_ref(self, |eng| eng.query_graph_pipeline(&query))?;
        Ok(GraphPipelineResultPayload {
            result,
            compact_rows,
        })
    }

    #[napi(
        ts_args_type = "request: import('./query-types').QueryNodeRequest",
        ts_return_type = "import('./query-types').QueryPlan"
    )]
    pub fn explain_node_query(&self, request: serde_json::Value) -> Result<JsonPayload> {
        let query = parse_js_node_query(&request)?;
        let plan = with_engine_ref(self, |eng| eng.explain_node_query(&query))?;
        query_plan_to_js(plan)
    }

    #[napi(
        ts_args_type = "request: import('./query-types').QueryEdgeRequest",
        ts_return_type = "import('./query-types').QueryPlan"
    )]
    pub fn explain_edge_query(&self, request: serde_json::Value) -> Result<JsonPayload> {
        let query = parse_js_edge_query(&request)?;
        let plan = with_engine_ref(self, |eng| eng.explain_edge_query(&query))?;
        query_plan_to_js(plan)
    }

    #[napi(
        ts_args_type = "request: import('./query-types').GraphRowRequest",
        ts_return_type = "import('./query-types').GraphRowExplain"
    )]
    pub fn explain_graph_rows(&self, request: serde_json::Value) -> Result<GraphJsExplain> {
        let query = parse_js_graph_row_query(&request)?;
        let explain = with_engine_ref(self, |eng| eng.explain_graph_rows(&query))?;
        Ok(GraphJsExplain(explain))
    }

    #[napi(
        ts_args_type = "request: import('./query-types').GraphPipelineRequest",
        ts_return_type = "import('./query-types').GraphPipelineExplain"
    )]
    pub fn explain_graph_pipeline(
        &self,
        request: serde_json::Value,
    ) -> Result<GraphPipelineJsExplain> {
        let query = parse_js_graph_pipeline_query(&request)?;
        let explain = with_engine_ref(self, |eng| eng.explain_graph_pipeline(&query))?;
        Ok(GraphPipelineJsExplain(explain))
    }

    #[napi(
        ts_args_type = "query: string, params?: import('./query-types').GqlParams | null, options?: import('./query-types').GqlExecutionOptions | null",
        ts_return_type = "import('./query-types').GqlExecutionResult"
    )]
    pub fn execute_gql(
        &self,
        query: String,
        params: Option<Unknown<'_>>,
        options: Option<GqlExecutionOptionsInput>,
    ) -> Result<GqlJsPayload> {
        let (options, compact_rows) = parse_js_gql_options(options)?;
        let referenced_params = gql_referenced_param_names(&query, &options)
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        let params = parse_js_gql_params(params, &referenced_params, &options)?;
        let result = with_engine_ref(self, |eng| eng.execute_gql(&query, &params, &options))?;
        Ok(GqlJsPayload(GqlJsPayloadKind::Result(
            GqlExecutionResultPayload {
                result,
                compact_rows,
            },
        )))
    }

    #[napi(
        ts_args_type = "query: string, params?: import('./query-types').GqlParams | null, options?: import('./query-types').GqlExecutionOptions | null",
        ts_return_type = "import('./query-types').GqlExecutionExplain"
    )]
    pub fn explain_gql(
        &self,
        query: String,
        params: Option<Unknown<'_>>,
        options: Option<GqlExecutionOptionsInput>,
    ) -> Result<GqlJsPayload> {
        let (options, _) = parse_js_gql_options(options)?;
        let referenced_params = gql_referenced_param_names(&query, &options)
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        let params = parse_js_gql_params(params, &referenced_params, &options)?;
        let explain = with_engine_ref(self, |eng| eng.explain_gql(&query, &params, &options))?;
        Ok(GqlJsPayload(GqlJsPayloadKind::Explain(explain)))
    }

    #[napi]
    pub fn ensure_node_property_index(
        &self,
        label: String,
        spec: SecondaryIndexSpec,
    ) -> Result<NodePropertyIndexInfo> {
        let spec = js_secondary_index_spec_to_rust(spec, JsSecondaryIndexTargetKind::Node)?;
        let info = with_engine(self, |eng| {
            eng.ensure_node_property_index(&label, spec.clone())
        })?;
        node_property_index_info_to_js(info)
    }

    #[napi]
    pub fn drop_node_property_index(
        &self,
        label: String,
        spec: SecondaryIndexSpec,
    ) -> Result<bool> {
        let spec = js_secondary_index_spec_to_rust(spec, JsSecondaryIndexTargetKind::Node)?;
        with_engine(self, |eng| {
            eng.drop_node_property_index(&label, spec.clone())
        })
    }

    #[napi]
    pub fn list_node_property_indexes(&self) -> Result<Vec<NodePropertyIndexInfo>> {
        let infos = with_engine_ref(self, |eng| eng.list_node_property_indexes())?;
        infos
            .into_iter()
            .map(node_property_index_info_to_js)
            .collect()
    }

    #[napi]
    pub fn ensure_edge_property_index(
        &self,
        label: String,
        spec: SecondaryIndexSpec,
    ) -> Result<EdgePropertyIndexInfo> {
        let spec = js_secondary_index_spec_to_rust(spec, JsSecondaryIndexTargetKind::Edge)?;
        let info = with_engine(self, |eng| {
            eng.ensure_edge_property_index(&label, spec.clone())
        })?;
        edge_property_index_info_to_js(info)
    }

    #[napi]
    pub fn drop_edge_property_index(
        &self,
        label: String,
        spec: SecondaryIndexSpec,
    ) -> Result<bool> {
        let spec = js_secondary_index_spec_to_rust(spec, JsSecondaryIndexTargetKind::Edge)?;
        with_engine(self, |eng| {
            eng.drop_edge_property_index(&label, spec.clone())
        })
    }

    #[napi]
    pub fn list_edge_property_indexes(&self) -> Result<Vec<EdgePropertyIndexInfo>> {
        let infos = with_engine_ref(self, |eng| eng.list_edge_property_indexes())?;
        infos
            .into_iter()
            .map(edge_property_index_info_to_js)
            .collect()
    }

    /// Return all node IDs containing every supplied node label (unpaged).
    #[napi(ts_args_type = "labels: string | string[]")]
    pub fn nodes_by_labels(&self, labels: serde_json::Value) -> Result<Float64Array> {
        let labels = parse_js_node_labels_arg(&labels, "nodesByLabels labels")?;
        let ids = with_engine_ref(self, |eng| eng.nodes_by_labels(labels))?;
        ids_to_float64_array(&ids)
    }

    /// Return all edge IDs of a given label (unpaged).
    #[napi]
    pub fn edges_by_label(&self, label: String) -> Result<Float64Array> {
        let ids = with_engine_ref(self, |eng| eng.edges_by_label(&label))?;
        ids_to_float64_array(&ids)
    }

    #[napi(ts_args_type = "labels: string | string[]")]
    pub fn get_nodes_by_labels(&self, labels: serde_json::Value) -> Result<Vec<NodeView>> {
        let labels = parse_js_node_labels_arg(&labels, "getNodesByLabels labels")?;
        let records = with_engine_ref(self, |eng| eng.get_nodes_by_labels(labels))?;
        records
            .into_iter()
            .map(NodeView::try_from)
            .collect::<Result<Vec<_>>>()
    }

    #[napi]
    pub fn get_edges_by_label(&self, label: String) -> Result<Vec<EdgeView>> {
        let records = with_engine_ref(self, |eng| eng.get_edges_by_label(&label))?;
        records
            .into_iter()
            .map(EdgeView::try_from)
            .collect::<Result<Vec<_>>>()
    }

    #[napi(ts_args_type = "labels: string | string[]")]
    pub fn count_nodes_by_labels(&self, labels: serde_json::Value) -> Result<i64> {
        let labels = parse_js_node_labels_arg(&labels, "countNodesByLabels labels")?;
        with_engine_ref(self, |eng| Ok(eng.count_nodes_by_labels(labels)? as i64))
    }

    #[napi]
    pub fn count_edges_by_label(&self, label: String) -> Result<i64> {
        with_engine_ref(self, |eng| Ok(eng.count_edges_by_label(&label)? as i64))
    }

    // --- Paginated queries (sync) ---

    #[napi(
        ts_args_type = "labels: string | string[], limit?: number | null, after?: number | null"
    )]
    pub fn nodes_by_labels_paged(
        &self,
        labels: serde_json::Value,
        limit: Option<u32>,
        after: Option<f64>,
    ) -> Result<IdPageResult> {
        let labels = parse_js_node_labels_arg(&labels, "nodesByLabelsPaged labels")?;
        let page = make_page_request(limit, after)?;
        let raw = with_engine_ref(self, |eng| eng.nodes_by_labels_paged(labels, &page))?;
        id_page_to_js(raw)
    }

    #[napi]
    pub fn edges_by_label_paged(
        &self,
        label: String,
        limit: Option<u32>,
        after: Option<f64>,
    ) -> Result<IdPageResult> {
        let page = make_page_request(limit, after)?;
        let raw = with_engine_ref(self, |eng| eng.edges_by_label_paged(&label, &page))?;
        id_page_to_js(raw)
    }

    #[napi(
        ts_args_type = "labels: string | string[], limit?: number | null, after?: number | null"
    )]
    pub fn get_nodes_by_labels_paged(
        &self,
        labels: serde_json::Value,
        limit: Option<u32>,
        after: Option<f64>,
    ) -> Result<NodePageResult> {
        let labels = parse_js_node_labels_arg(&labels, "getNodesByLabelsPaged labels")?;
        let page = make_page_request(limit, after)?;
        let raw = with_engine_ref(self, |eng| eng.get_nodes_by_labels_paged(labels, &page))?;
        node_page_to_js(raw)
    }

    #[napi]
    pub fn get_edges_by_label_paged(
        &self,
        label: String,
        limit: Option<u32>,
        after: Option<f64>,
    ) -> Result<EdgePageResult> {
        let page = make_page_request(limit, after)?;
        let raw = with_engine_ref(self, |eng| eng.get_edges_by_label_paged(&label, &page))?;
        edge_page_to_js(raw)
    }

    #[napi]
    pub fn find_nodes_paged(
        &self,
        label: String,
        prop_key: String,
        prop_value: serde_json::Value,
        options: Option<FindNodesPagedOptions>,
    ) -> Result<IdPageResult> {
        let pv = json_to_prop_value(&prop_value);
        let (limit, after) = match options {
            Some(o) => (o.limit, o.after),
            None => (None, None),
        };
        let page = make_page_request(limit, after)?;
        let raw = with_engine_ref(self, |eng| {
            eng.find_nodes_paged(&label, &prop_key, &pv, &page)
        })?;
        id_page_to_js(raw)
    }

    #[napi]
    pub fn find_nodes_by_time_range(
        &self,
        label: String,
        from_ms: i64,
        to_ms: i64,
    ) -> Result<Float64Array> {
        let ids = with_engine_ref(self, |eng| {
            eng.find_nodes_by_time_range(&label, from_ms, to_ms)
        })?;
        ids_to_float64_array(&ids)
    }

    #[napi]
    pub fn find_nodes_range(
        &self,
        label: String,
        prop_key: String,
        lower: Option<PropertyRangeBound>,
        upper: Option<PropertyRangeBound>,
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
            eng.find_nodes_range(&label, &prop_key, lower.as_ref(), upper.as_ref())
        })?;
        ids_to_float64_array(&ids)
    }

    #[napi]
    pub fn find_nodes_by_time_range_paged(
        &self,
        label: String,
        from_ms: i64,
        to_ms: i64,
        options: Option<FindNodesByTimeRangePagedOptions>,
    ) -> Result<IdPageResult> {
        let (limit, after) = match options {
            Some(o) => (o.limit, o.after),
            None => (None, None),
        };
        let page = make_page_request(limit, after)?;
        let raw = with_engine_ref(self, |eng| {
            eng.find_nodes_by_time_range_paged(&label, from_ms, to_ms, &page)
        })?;
        id_page_to_js(raw)
    }

    #[napi]
    pub fn find_nodes_range_paged(
        &self,
        label: String,
        prop_key: String,
        lower: Option<PropertyRangeBound>,
        upper: Option<PropertyRangeBound>,
        options: Option<FindNodesRangePagedOptions>,
    ) -> Result<PropertyRangePageResult> {
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
            eng.find_nodes_range_paged(&label, &prop_key, lower.as_ref(), upper.as_ref(), &page)
        })?;
        property_range_page_to_js(raw)
    }

    #[napi]
    pub fn personalized_pagerank(
        &self,
        seed_node_ids: Vec<f64>,
        options: Option<PersonalizedPagerankOptions>,
    ) -> Result<PprResult> {
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
            edge_label_filter,
            max_results,
        ) = match &options {
            Some(o) => (
                o.algorithm.as_deref(),
                o.damping_factor,
                o.max_iterations,
                o.epsilon,
                o.approx_residual_tolerance,
                o.edge_label_filter.clone(),
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
            &edge_label_filter,
            &max_results,
        )?;
        let result = with_engine_ref(self, |eng| eng.personalized_pagerank(&seeds, &opts))?;
        ppr_result_to_js(result)
    }

    #[napi]
    pub fn export_adjacency(&self, options: Option<ExportOptions>) -> Result<AdjacencyExport> {
        let include_weights = options
            .as_ref()
            .and_then(|o| o.include_weights)
            .unwrap_or(true);
        let opts = js_export_options_to_rust(options)?;
        let result = with_engine_ref(self, |eng| eng.export_adjacency(&opts))?;
        adjacency_export_to_js(result, include_weights)
    }

    #[napi]
    pub fn neighbors_paged(
        &self,
        node_id: f64,
        options: Option<NeighborsPagedOptions>,
    ) -> Result<NeighborPageResult> {
        let node_id = f64_to_u64(node_id)?;
        let (direction, edge_label_filter, limit, after, at_epoch, decay_lambda) = match options {
            Some(o) => (
                o.direction,
                o.edge_label_filter,
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
            edge_label_filter: edge_label_filter,
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
        options: Option<ConnectedComponentsOptions>,
    ) -> Result<Vec<ComponentEntry>> {
        let (edge_label_filter, node_label_filter, at_epoch) = match options {
            Some(o) => (
                o.edge_label_filter,
                o.node_label_filter
                    .map(js_node_label_filter_to_rust)
                    .transpose()?,
                o.at_epoch,
            ),
            None => (None, None, None),
        };
        let opts = ComponentOptions {
            edge_label_filter,
            node_label_filter,
            at_epoch,
        };
        let map = with_engine_ref(self, |eng| eng.connected_components(&opts))?;
        let mut entries: Vec<ComponentEntry> = map
            .into_iter()
            .map(|(node_id, component_id)| {
                Ok(ComponentEntry {
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
        options: Option<ComponentOfOptions>,
    ) -> Result<Float64Array> {
        let node_id = f64_to_u64(node_id)?;
        let (edge_label_filter, node_label_filter, at_epoch) = match options {
            Some(o) => (
                o.edge_label_filter,
                o.node_label_filter
                    .map(js_node_label_filter_to_rust)
                    .transpose()?,
                o.at_epoch,
            ),
            None => (None, None, None),
        };
        let opts = ComponentOptions {
            edge_label_filter,
            node_label_filter,
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
        options: VectorSearchOptions,
    ) -> Result<Vec<VectorHit>> {
        let mode = parse_vector_search_mode(&mode)?;
        let k = options.k;
        let dense_query = options.dense_query;
        let sparse_query = options.sparse_query;
        let label_filter = options
            .label_filter
            .map(js_node_label_filter_to_rust)
            .transpose()?;
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
            Some(s) => Some(CoreVectorSearchScope {
                start_node_id: f64_to_u64(s.start_node_id)?,
                max_depth: s.max_depth,
                direction: parse_direction(s.direction.as_deref())?,
                edge_label_filter: s.edge_label_filter,
                at_epoch: s.at_epoch,
            }),
        };
        let request = VectorSearchRequest {
            mode,
            dense_query: dense_q,
            sparse_query: sparse_q,
            k: k as usize,
            label_filter,
            ef_search: ef_search.map(|v| v as usize),
            scope,
            dense_weight: dense_weight.map(|v| v as f32),
            sparse_weight: sparse_weight.map(|v| v as f32),
            fusion_mode: fusion,
        };
        let hits = with_engine_ref(self, |eng| eng.vector_search(&request))?;
        hits.into_iter()
            .map(|h| {
                Ok(VectorHit {
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
    pub fn end_ingest(&self) -> Result<Option<CompactionStats>> {
        with_engine(self, |eng| Ok(eng.end_ingest()?.map(|s| s.into())))
    }

    #[napi]
    pub fn compact(&self) -> Result<Option<CompactionStats>> {
        with_engine(self, |eng| Ok(eng.compact()?.map(|s| s.into())))
    }

    /// Compact with a progress callback. The callback receives a progress object
    /// and should return `true` to continue or `false` to cancel.
    /// Runs synchronously. Blocks the event loop.
    #[napi(ts_args_type = "callback: (progress: CompactionProgress) => boolean")]
    pub fn compact_with_progress(
        &self,
        callback: Function<CompactionProgress, bool>,
    ) -> Result<Option<CompactionStats>> {
        with_engine(self, |eng| {
            let result = eng.compact_with_progress(|progress| {
                let js_progress = CompactionProgress {
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
    pub fn stats(&self) -> Result<DbStats> {
        with_engine_ref(self, |eng| Ok(eng.stats()?.into()))
    }

    #[napi]
    pub fn scrub(&self) -> Result<ScrubReport> {
        with_engine_ref(self, |eng| Ok(eng.scrub()?.into()))
    }

    // ============================
    // Async API (Promise-returning)
    // ============================

    #[napi(ts_return_type = "Promise<void>")]
    pub fn close_async(&self, options: Option<CloseOptions>) -> AsyncTask<CloseOp> {
        let force = options.as_ref().and_then(|o| o.force).unwrap_or(false);
        AsyncTask::new(CloseOp {
            db: self.inner.clone(),
            force,
        })
    }

    #[napi(ts_return_type = "Promise<DbStats>")]
    pub fn stats_async(&self) -> AsyncTask<EngineReadOp<CoreDbStats, DbStats>> {
        AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            |eng| eng.stats(),
            |s| Ok(s.into()),
        ))
    }

    #[napi(ts_return_type = "Promise<ScrubReport>")]
    pub fn scrub_async(&self) -> AsyncTask<EngineReadOp<CoreScrubReport, ScrubReport>> {
        AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            |eng| eng.scrub(),
            |r| Ok(r.into()),
        ))
    }

    #[napi(ts_return_type = "Promise<number>")]
    pub fn ensure_node_label_async(&self, label: String) -> AsyncTask<EngineOp<u32, u32>> {
        AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| eng.ensure_node_label(&label),
            Ok,
        ))
    }

    #[napi(ts_return_type = "Promise<number>")]
    pub fn ensure_edge_label_async(&self, label: String) -> AsyncTask<EngineOp<u32, u32>> {
        AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| eng.ensure_edge_label(&label),
            Ok,
        ))
    }

    #[napi(ts_return_type = "Promise<number | null>")]
    pub fn get_node_label_id_async(
        &self,
        label: String,
    ) -> AsyncTask<EngineReadOp<Option<u32>, Option<u32>>> {
        AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.get_node_label_id(&label),
            Ok,
        ))
    }

    #[napi(ts_return_type = "Promise<number | null>")]
    pub fn get_edge_label_id_async(
        &self,
        label: String,
    ) -> AsyncTask<EngineReadOp<Option<u32>, Option<u32>>> {
        AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.get_edge_label_id(&label),
            Ok,
        ))
    }

    #[napi(ts_return_type = "Promise<string | null>")]
    pub fn get_node_label_async(
        &self,
        label_id: u32,
    ) -> AsyncTask<EngineReadOp<Option<String>, Option<String>>> {
        AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.get_node_label(label_id),
            Ok,
        ))
    }

    #[napi(ts_return_type = "Promise<string | null>")]
    pub fn get_edge_label_async(
        &self,
        label_id: u32,
    ) -> AsyncTask<EngineReadOp<Option<String>, Option<String>>> {
        AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.get_edge_label(label_id),
            Ok,
        ))
    }

    #[napi(ts_return_type = "Promise<Array<NodeLabelInfo>>")]
    pub fn list_node_labels_async(
        &self,
    ) -> AsyncTask<EngineReadOp<Vec<CoreNodeLabelInfo>, Vec<NodeLabelInfo>>> {
        AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            |eng| eng.list_node_labels(),
            |infos| Ok(infos.into_iter().map(Into::into).collect()),
        ))
    }

    #[napi(ts_return_type = "Promise<Array<EdgeLabelInfo>>")]
    pub fn list_edge_labels_async(
        &self,
    ) -> AsyncTask<EngineReadOp<Vec<CoreEdgeLabelInfo>, Vec<EdgeLabelInfo>>> {
        AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            |eng| eng.list_edge_labels(),
            |infos| Ok(infos.into_iter().map(Into::into).collect()),
        ))
    }

    #[napi(
        ts_args_type = "label: string, schema: import('./schema-types').NodeSchema, options?: SchemaSetOptions | null",
        ts_return_type = "Promise<import('./schema-types').NodeSchemaInfo>"
    )]
    pub fn set_node_schema_async(
        &self,
        label: String,
        schema: Unknown<'_>,
        options: Option<SchemaSetOptions>,
    ) -> Result<AsyncTask<EngineOp<CoreNodeSchemaInfo, NodeSchemaInfoPayload>>> {
        let schema = parse_js_node_schema(schema, "setNodeSchemaAsync schema")?;
        let options = schema_set_options_to_core(options)?;
        Ok(AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| eng.set_node_schema_with_options(&label, schema, options),
            |info| Ok(NodeSchemaInfoPayload(info)),
        )))
    }

    #[napi(
        ts_args_type = "label: string, schema: import('./schema-types').NodeSchema, options?: SchemaCheckOptions | null",
        ts_return_type = "Promise<import('./schema-types').SchemaValidationReport>"
    )]
    pub fn check_node_schema_async(
        &self,
        label: String,
        schema: Unknown<'_>,
        options: Option<SchemaCheckOptions>,
    ) -> Result<AsyncTask<EngineReadOp<CoreSchemaValidationReport, SchemaValidationReportPayload>>>
    {
        let schema = parse_js_node_schema(schema, "checkNodeSchemaAsync schema")?;
        let options = schema_check_options_to_core(options)?;
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.check_node_schema(&label, schema, options),
            |report| Ok(SchemaValidationReportPayload(report)),
        )))
    }

    #[napi(ts_return_type = "Promise<boolean>")]
    pub fn drop_node_schema_async(&self, label: String) -> AsyncTask<EngineOp<bool, bool>> {
        AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| eng.drop_node_schema(&label),
            Ok,
        ))
    }

    #[napi(ts_return_type = "Promise<import('./schema-types').NodeSchemaInfo | null>")]
    pub fn get_node_schema_async(
        &self,
        label: String,
    ) -> AsyncTask<EngineReadOp<Option<CoreNodeSchemaInfo>, Option<NodeSchemaInfoPayload>>> {
        AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.get_node_schema(&label),
            |info| Ok(info.map(NodeSchemaInfoPayload)),
        ))
    }

    #[napi(ts_return_type = "Promise<Array<import('./schema-types').NodeSchemaInfo>>")]
    pub fn list_node_schemas_async(
        &self,
    ) -> AsyncTask<EngineReadOp<Vec<CoreNodeSchemaInfo>, Vec<NodeSchemaInfoPayload>>> {
        AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            |eng| eng.list_node_schemas(),
            |infos| Ok(infos.into_iter().map(NodeSchemaInfoPayload).collect()),
        ))
    }

    #[napi(
        ts_args_type = "label: string, schema: import('./schema-types').EdgeSchema, options?: SchemaSetOptions | null",
        ts_return_type = "Promise<import('./schema-types').EdgeSchemaInfo>"
    )]
    pub fn set_edge_schema_async(
        &self,
        label: String,
        schema: Unknown<'_>,
        options: Option<SchemaSetOptions>,
    ) -> Result<AsyncTask<EngineOp<CoreEdgeSchemaInfo, EdgeSchemaInfoPayload>>> {
        let schema = parse_js_edge_schema(schema, "setEdgeSchemaAsync schema")?;
        let options = schema_set_options_to_core(options)?;
        Ok(AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| eng.set_edge_schema_with_options(&label, schema, options),
            |info| Ok(EdgeSchemaInfoPayload(info)),
        )))
    }

    #[napi(
        ts_args_type = "label: string, schema: import('./schema-types').EdgeSchema, options?: SchemaCheckOptions | null",
        ts_return_type = "Promise<import('./schema-types').SchemaValidationReport>"
    )]
    pub fn check_edge_schema_async(
        &self,
        label: String,
        schema: Unknown<'_>,
        options: Option<SchemaCheckOptions>,
    ) -> Result<AsyncTask<EngineReadOp<CoreSchemaValidationReport, SchemaValidationReportPayload>>>
    {
        let schema = parse_js_edge_schema(schema, "checkEdgeSchemaAsync schema")?;
        let options = schema_check_options_to_core(options)?;
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.check_edge_schema(&label, schema, options),
            |report| Ok(SchemaValidationReportPayload(report)),
        )))
    }

    #[napi(ts_return_type = "Promise<boolean>")]
    pub fn drop_edge_schema_async(&self, label: String) -> AsyncTask<EngineOp<bool, bool>> {
        AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| eng.drop_edge_schema(&label),
            Ok,
        ))
    }

    #[napi(ts_return_type = "Promise<import('./schema-types').EdgeSchemaInfo | null>")]
    pub fn get_edge_schema_async(
        &self,
        label: String,
    ) -> AsyncTask<EngineReadOp<Option<CoreEdgeSchemaInfo>, Option<EdgeSchemaInfoPayload>>> {
        AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.get_edge_schema(&label),
            |info| Ok(info.map(EdgeSchemaInfoPayload)),
        ))
    }

    #[napi(ts_return_type = "Promise<Array<import('./schema-types').EdgeSchemaInfo>>")]
    pub fn list_edge_schemas_async(
        &self,
    ) -> AsyncTask<EngineReadOp<Vec<CoreEdgeSchemaInfo>, Vec<EdgeSchemaInfoPayload>>> {
        AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            |eng| eng.list_edge_schemas(),
            |infos| Ok(infos.into_iter().map(EdgeSchemaInfoPayload).collect()),
        ))
    }

    #[napi(
        ts_args_type = "schema: import('./schema-types').GraphSchema, options?: SchemaSetOptions | null",
        ts_return_type = "Promise<import('./schema-types').GraphSchemaPublishResult>"
    )]
    pub fn set_graph_schema_async(
        &self,
        schema: Unknown<'_>,
        options: Option<SchemaSetOptions>,
    ) -> Result<AsyncTask<EngineOp<CoreGraphSchemaPublishResult, GraphSchemaPublishResultPayload>>>
    {
        let schema = parse_js_graph_schema(schema, "setGraphSchemaAsync schema")?;
        let options = graph_schema_set_options_to_core(options)?;
        Ok(AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| eng.set_graph_schema(schema, options),
            |result| Ok(GraphSchemaPublishResultPayload(result)),
        )))
    }

    #[napi(
        ts_args_type = "operations: Array<import('./schema-types').GraphSchemaOperation>, options?: SchemaSetOptions | null",
        ts_return_type = "Promise<import('./schema-types').GraphSchemaPublishResult>"
    )]
    pub fn alter_graph_schema_async(
        &self,
        operations: Unknown<'_>,
        options: Option<SchemaSetOptions>,
    ) -> Result<AsyncTask<EngineOp<CoreGraphSchemaPublishResult, GraphSchemaPublishResultPayload>>>
    {
        let operations =
            parse_js_graph_schema_operations(operations, "alterGraphSchemaAsync operations")?;
        let options = graph_schema_set_options_to_core(options)?;
        Ok(AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| eng.alter_graph_schema(operations, options),
            |result| Ok(GraphSchemaPublishResultPayload(result)),
        )))
    }

    #[napi(
        ts_args_type = "schema: import('./schema-types').GraphSchema, options?: SchemaCheckOptions | null",
        ts_return_type = "Promise<import('./schema-types').GraphSchemaCheckReport>"
    )]
    pub fn check_graph_schema_set_async(
        &self,
        schema: Unknown<'_>,
        options: Option<SchemaCheckOptions>,
    ) -> Result<AsyncTask<EngineReadOp<CoreGraphSchemaCheckReport, GraphSchemaCheckReportPayload>>>
    {
        let schema = parse_js_graph_schema(schema, "checkGraphSchemaSetAsync schema")?;
        let options = graph_schema_check_options_to_core(options)?;
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.check_graph_schema_set(schema, options),
            |report| Ok(GraphSchemaCheckReportPayload(report)),
        )))
    }

    #[napi(
        ts_args_type = "schema: import('./schema-types').GraphSchema, options?: SchemaCheckOptions | null",
        ts_return_type = "Promise<import('./schema-types').GraphSchemaCheckReport>"
    )]
    pub fn check_graph_schema_add_async(
        &self,
        schema: Unknown<'_>,
        options: Option<SchemaCheckOptions>,
    ) -> Result<AsyncTask<EngineReadOp<CoreGraphSchemaCheckReport, GraphSchemaCheckReportPayload>>>
    {
        let schema = parse_js_graph_schema(schema, "checkGraphSchemaAddAsync schema")?;
        let options = graph_schema_check_options_to_core(options)?;
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.check_graph_schema_add(schema, options),
            |report| Ok(GraphSchemaCheckReportPayload(report)),
        )))
    }

    #[napi(ts_return_type = "Promise<import('./schema-types').GraphSchemaPublishResult>")]
    pub fn drop_graph_schema_async(
        &self,
    ) -> AsyncTask<EngineOp<CoreGraphSchemaPublishResult, GraphSchemaPublishResultPayload>> {
        AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            |eng| eng.drop_graph_schema(),
            |result| Ok(GraphSchemaPublishResultPayload(result)),
        ))
    }

    #[napi(
        ts_args_type = "labels: string | string[], key: string, options?: UpsertNodeOptions | null",
        ts_return_type = "Promise<number>"
    )]
    pub fn upsert_node_async(
        &self,
        labels: serde_json::Value,
        key: String,
        options: Option<UpsertNodeOptions>,
    ) -> Result<AsyncTask<EngineOp<u64, f64>>> {
        let labels = parse_js_node_labels_arg(&labels, "upsertNodeAsync labels")?;
        let (props, weight, dense_vector, sparse_vector) = match options {
            Some(o) => (o.props, o.weight, o.dense_vector, o.sparse_vector),
            None => (None, None, None, None),
        };
        let props = convert_js_props(props);
        let opts = CoreUpsertNodeOptions {
            props,
            weight: weight.unwrap_or(1.0) as f32,
            dense_vector: dense_vector.map(|dv| dv.into_iter().map(|x| x as f32).collect()),
            sparse_vector: sparse_vector.map(|sv| {
                sv.into_iter()
                    .map(|e| (e.dimension, e.value as f32))
                    .collect()
            }),
        };
        Ok(AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| eng.upsert_node(labels, &key, opts),
            u64_to_f64,
        )))
    }

    #[napi(ts_return_type = "Promise<boolean>")]
    pub fn add_node_label_async(
        &self,
        node_id: f64,
        label: String,
    ) -> Result<AsyncTask<EngineOp<bool, bool>>> {
        let node_id = f64_to_u64(node_id)?;
        Ok(AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| eng.add_node_label(node_id, &label),
            Ok,
        )))
    }

    #[napi(ts_return_type = "Promise<boolean>")]
    pub fn remove_node_label_async(
        &self,
        node_id: f64,
        label: String,
    ) -> Result<AsyncTask<EngineOp<bool, bool>>> {
        let node_id = f64_to_u64(node_id)?;
        Ok(AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| eng.remove_node_label(node_id, &label),
            Ok,
        )))
    }

    #[napi(ts_return_type = "Promise<number>")]
    pub fn upsert_edge_async(
        &self,
        from: f64,
        to: f64,
        label: String,
        options: Option<UpsertEdgeOptions>,
    ) -> Result<AsyncTask<EngineOp<u64, f64>>> {
        let from = f64_to_u64(from)?;
        let to = f64_to_u64(to)?;
        let (props, weight, valid_from, valid_to) = match options {
            Some(o) => (o.props, o.weight, o.valid_from, o.valid_to),
            None => (None, None, None, None),
        };
        let props = convert_js_props(props);
        let opts = CoreUpsertEdgeOptions {
            props,
            weight: weight.unwrap_or(1.0) as f32,
            valid_from,
            valid_to,
        };
        Ok(AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| eng.upsert_edge(from, to, &label, opts),
            u64_to_f64,
        )))
    }

    #[napi(ts_return_type = "Promise<Float64Array>")]
    pub fn batch_upsert_nodes_async(
        &self,
        nodes: Vec<NodeInput>,
    ) -> Result<AsyncTask<EngineOp<Vec<u64>, Float64Array>>> {
        let inputs: Vec<CoreNodeInput> = nodes
            .into_iter()
            .map(NodeInput::try_into)
            .collect::<Result<Vec<_>>>()?;
        Ok(AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| eng.batch_upsert_nodes(inputs),
            |ids| ids_to_float64_array(&ids),
        )))
    }

    #[napi(ts_return_type = "Promise<Float64Array>")]
    pub fn batch_upsert_edges_async(
        &self,
        edges: Vec<EdgeInput>,
    ) -> Result<AsyncTask<EngineOp<Vec<u64>, Float64Array>>> {
        let inputs: std::result::Result<Vec<CoreEdgeInput>, _> =
            edges.into_iter().map(|e| e.try_into()).collect();
        let inputs = inputs?;
        Ok(AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| eng.batch_upsert_edges(inputs),
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
            move |eng| eng.batch_upsert_nodes(inputs),
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
            move |eng| eng.batch_upsert_edges(inputs),
            |ids| ids_to_float64_array(&ids),
        )))
    }

    #[napi(ts_return_type = "Promise<NodeView | null>")]
    pub fn get_node_async(
        &self,
        id: f64,
    ) -> Result<AsyncTask<EngineReadOp<Option<CoreNodeView>, Option<NodeView>>>> {
        let id = f64_to_u64(id)?;
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.get_node(id),
            |n| n.map(NodeView::try_from).transpose(),
        )))
    }

    #[napi(ts_return_type = "Promise<EdgeView | null>")]
    pub fn get_edge_async(
        &self,
        id: f64,
    ) -> Result<AsyncTask<EngineReadOp<Option<CoreEdgeView>, Option<EdgeView>>>> {
        let id = f64_to_u64(id)?;
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.get_edge(id),
            |e| e.map(EdgeView::try_from).transpose(),
        )))
    }

    #[napi(ts_return_type = "Promise<NodeView | null>")]
    pub fn get_node_by_key_async(
        &self,
        label: String,
        key: String,
    ) -> AsyncTask<EngineReadOp<Option<CoreNodeView>, Option<NodeView>>> {
        AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.get_node_by_key(&label, &key),
            |n| n.map(NodeView::try_from).transpose(),
        ))
    }

    #[napi(ts_return_type = "Promise<EdgeView | null>")]
    pub fn get_edge_by_triple_async(
        &self,
        from: f64,
        to: f64,
        label: String,
    ) -> Result<AsyncTask<EngineReadOp<Option<CoreEdgeView>, Option<EdgeView>>>> {
        let from = f64_to_u64(from)?;
        let to = f64_to_u64(to)?;
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.get_edge_by_triple(from, to, &label),
            |e| e.map(EdgeView::try_from).transpose(),
        )))
    }

    #[napi(ts_return_type = "Promise<Array<NodeView | null>>")]
    pub fn get_nodes_async(
        &self,
        ids: Vec<f64>,
    ) -> Result<AsyncTask<EngineReadOp<Vec<Option<CoreNodeView>>, Vec<Option<NodeView>>>>> {
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
                    .map(|r| r.map(NodeView::try_from).transpose())
                    .collect::<Result<Vec<_>>>()
            },
        )))
    }

    #[napi(ts_return_type = "Promise<Array<NodeView | null>>")]
    pub fn get_nodes_by_keys_async(
        &self,
        keys: Vec<KeyQuery>,
    ) -> Result<AsyncTask<EngineReadOp<Vec<Option<CoreNodeView>>, Vec<Option<NodeView>>>>> {
        let owned: Vec<NodeKeyQuery> = keys
            .into_iter()
            .map(KeyQuery::try_into)
            .collect::<Result<Vec<_>>>()?;
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.get_nodes_by_keys(&owned),
            |results| {
                results
                    .into_iter()
                    .map(|r| r.map(NodeView::try_from).transpose())
                    .collect::<Result<Vec<_>>>()
            },
        )))
    }

    #[napi(ts_return_type = "Promise<Array<EdgeView | null>>")]
    pub fn get_edges_async(
        &self,
        ids: Vec<f64>,
    ) -> Result<AsyncTask<EngineReadOp<Vec<Option<CoreEdgeView>>, Vec<Option<EdgeView>>>>> {
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
                    .map(|r| r.map(EdgeView::try_from).transpose())
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

    #[napi(ts_return_type = "Promise<EdgeView | null>")]
    pub fn invalidate_edge_async(
        &self,
        id: f64,
        valid_to: i64,
    ) -> Result<AsyncTask<EngineOp<Option<CoreEdgeView>, Option<EdgeView>>>> {
        let id = f64_to_u64(id)?;
        Ok(AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| eng.invalidate_edge(id, valid_to),
            |e| e.map(EdgeView::try_from).transpose(),
        )))
    }

    #[napi(ts_return_type = "Promise<PatchResult>")]
    pub fn graph_patch_async(
        &self,
        patch: GraphPatch,
    ) -> Result<AsyncTask<EngineOp<overgraph::PatchResult, PatchResult>>> {
        let rust_patch = js_patch_to_rust(patch)?;
        Ok(AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| eng.graph_patch(rust_patch),
            |result| {
                Ok(PatchResult {
                    node_ids: ids_to_float64_array(&result.node_ids)?,
                    edge_ids: ids_to_float64_array(&result.edge_ids)?,
                })
            },
        )))
    }

    #[napi(ts_return_type = "Promise<PruneResult>")]
    pub fn prune_async(
        &self,
        policy: PrunePolicy,
    ) -> Result<AsyncTask<EngineOp<CorePruneResult, PruneResult>>> {
        let rust_policy = js_prune_policy_to_rust(policy, "pruneAsync")?;
        Ok(AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| eng.prune(&rust_policy),
            |result| {
                Ok(PruneResult {
                    nodes_pruned: result.nodes_pruned as i64,
                    edges_pruned: result.edges_pruned as i64,
                })
            },
        )))
    }

    #[napi(ts_return_type = "Promise<Array<NeighborEntry>>")]
    pub fn neighbors_async(
        &self,
        node_id: f64,
        options: Option<NeighborsOptions>,
    ) -> Result<AsyncTask<EngineReadOp<Vec<CoreNeighborEntry>, Vec<NeighborEntry>>>> {
        let node_id = f64_to_u64(node_id)?;
        let (direction, edge_label_filter, limit, at_epoch, decay_lambda) = match options {
            Some(o) => (
                o.direction,
                o.edge_label_filter,
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
            edge_label_filter: edge_label_filter,
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

    #[napi(ts_return_type = "Promise<TraversalPageResult>")]
    pub fn traverse_async(
        &self,
        start_node_id: f64,
        max_depth: u32,
        options: Option<TraverseOptions>,
    ) -> Result<AsyncTask<EngineReadOp<CoreTraversalPageResult, TraversalPageResult>>> {
        let start_node_id = f64_to_u64(start_node_id)?;
        let (
            direction,
            min_depth,
            edge_label_filter,
            node_label_filter,
            at_epoch,
            decay_lambda,
            limit,
            cursor,
        ) = match options {
            Some(o) => (
                o.direction,
                o.min_depth,
                o.edge_label_filter,
                o.emit_node_label_filter
                    .map(js_node_label_filter_to_rust)
                    .transpose()?,
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
        let opts = CoreTraverseOptions {
            min_depth,
            direction: dir,
            edge_label_filter,
            emit_node_label_filter: node_label_filter,
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

    #[napi(ts_return_type = "Promise<Array<NeighborEntry>>")]
    pub fn top_k_neighbors_async(
        &self,
        node_id: f64,
        k: u32,
        options: Option<TopKNeighborsOptions>,
    ) -> Result<AsyncTask<EngineReadOp<Vec<CoreNeighborEntry>, Vec<NeighborEntry>>>> {
        let node_id = f64_to_u64(node_id)?;
        let (direction, edge_label_filter, scoring, decay_lambda, at_epoch) = match options {
            Some(o) => (
                o.direction,
                o.edge_label_filter,
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
            edge_label_filter: edge_label_filter,
            scoring: scoring_mode,
            at_epoch,
        };
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.top_k_neighbors(node_id, k as usize, &opts),
            neighbor_entries_to_js,
        )))
    }

    #[napi(ts_return_type = "Promise<SubgraphResult>")]
    pub fn extract_subgraph_async(
        &self,
        start_node_id: f64,
        max_depth: u32,
        options: Option<ExtractSubgraphOptions>,
    ) -> Result<AsyncTask<EngineReadOp<Subgraph, SubgraphResult>>> {
        let start = f64_to_u64(start_node_id)?;
        let (direction, edge_label_filter, node_label_filter, at_epoch) = match options {
            Some(o) => (
                o.direction,
                o.edge_label_filter,
                o.node_label_filter
                    .map(js_node_label_filter_to_rust)
                    .transpose()?,
                o.at_epoch,
            ),
            None => (None, None, None, None),
        };
        let dir = parse_direction(direction.as_deref())?;
        let opts = SubgraphOptions {
            direction: dir,
            edge_label_filter,
            node_label_filter,
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
        label: String,
        prop_key: String,
        prop_value: serde_json::Value,
    ) -> AsyncTask<EngineReadOp<Vec<u64>, Float64Array>> {
        let pv = json_to_prop_value(&prop_value);
        AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.find_nodes(&label, &prop_key, &pv),
            |ids| ids_to_float64_array(&ids),
        ))
    }

    #[napi(
        ts_args_type = "request: import('./query-types').QueryNodeRequest",
        ts_return_type = "Promise<IdPageResult>"
    )]
    pub fn query_node_ids_async(
        &self,
        request: serde_json::Value,
    ) -> Result<AsyncTask<EngineReadOp<QueryNodeIdsResult, IdPageResult>>> {
        let query = parse_js_node_query(&request)?;
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.query_node_ids(&query),
            query_node_ids_to_js,
        )))
    }

    #[napi(
        ts_args_type = "request: import('./query-types').QueryNodeRequest",
        ts_return_type = "Promise<NodePageResult>"
    )]
    pub fn query_nodes_async(
        &self,
        request: serde_json::Value,
    ) -> Result<AsyncTask<EngineReadOp<QueryNodesResult, NodePageResult>>> {
        let query = parse_js_node_query(&request)?;
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.query_nodes(&query),
            query_nodes_to_js,
        )))
    }

    #[napi(
        ts_args_type = "request: import('./query-types').QueryEdgeRequest",
        ts_return_type = "Promise<IdPageResult>"
    )]
    pub fn query_edge_ids_async(
        &self,
        request: serde_json::Value,
    ) -> Result<AsyncTask<EngineReadOp<QueryEdgeIdsResult, IdPageResult>>> {
        let query = parse_js_edge_query(&request)?;
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.query_edge_ids(&query),
            query_edge_ids_to_js,
        )))
    }

    #[napi(
        ts_args_type = "request: import('./query-types').QueryEdgeRequest",
        ts_return_type = "Promise<EdgePageResult>"
    )]
    pub fn query_edges_async(
        &self,
        request: serde_json::Value,
    ) -> Result<AsyncTask<EngineReadOp<QueryEdgesResult, EdgePageResult>>> {
        let query = parse_js_edge_query(&request)?;
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.query_edges(&query),
            query_edges_to_js,
        )))
    }

    #[napi(
        ts_args_type = "request: import('./query-types').GraphRowRequest",
        ts_return_type = "Promise<import('./query-types').GraphRowResult>"
    )]
    pub fn query_graph_rows_async(
        &self,
        request: serde_json::Value,
    ) -> Result<AsyncTask<EngineReadOp<GraphRowResultPayload, GraphRowResultPayload>>> {
        let query = parse_js_graph_row_query(&request)?;
        let compact_rows = query.output.compact_rows;
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| {
                let result = eng.query_graph_rows(&query)?;
                Ok(GraphRowResultPayload {
                    result,
                    compact_rows,
                })
            },
            napi_identity,
        )))
    }

    #[napi(
        ts_args_type = "request: import('./query-types').GraphPipelineRequest",
        ts_return_type = "Promise<import('./query-types').GraphPipelineResult>"
    )]
    pub fn query_graph_pipeline_async(
        &self,
        request: serde_json::Value,
    ) -> Result<AsyncTask<EngineReadOp<GraphPipelineResultPayload, GraphPipelineResultPayload>>>
    {
        let query = parse_js_graph_pipeline_query(&request)?;
        let compact_rows = query.output.compact_rows;
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| {
                let result = eng.query_graph_pipeline(&query)?;
                Ok(GraphPipelineResultPayload {
                    result,
                    compact_rows,
                })
            },
            napi_identity,
        )))
    }

    #[napi(
        ts_args_type = "request: import('./query-types').QueryNodeRequest",
        ts_return_type = "Promise<import('./query-types').QueryPlan>"
    )]
    pub fn explain_node_query_async(
        &self,
        request: serde_json::Value,
    ) -> Result<AsyncTask<EngineReadOp<QueryPlan, JsonPayload>>> {
        let query = parse_js_node_query(&request)?;
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.explain_node_query(&query),
            query_plan_to_js,
        )))
    }

    #[napi(
        ts_args_type = "request: import('./query-types').QueryEdgeRequest",
        ts_return_type = "Promise<import('./query-types').QueryPlan>"
    )]
    pub fn explain_edge_query_async(
        &self,
        request: serde_json::Value,
    ) -> Result<AsyncTask<EngineReadOp<QueryPlan, JsonPayload>>> {
        let query = parse_js_edge_query(&request)?;
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.explain_edge_query(&query),
            query_plan_to_js,
        )))
    }

    #[napi(
        ts_args_type = "request: import('./query-types').GraphRowRequest",
        ts_return_type = "Promise<import('./query-types').GraphRowExplain>"
    )]
    pub fn explain_graph_rows_async(
        &self,
        request: serde_json::Value,
    ) -> Result<AsyncTask<EngineReadOp<GraphRowExplain, GraphJsExplain>>> {
        let query = parse_js_graph_row_query(&request)?;
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.explain_graph_rows(&query),
            |explain| Ok(GraphJsExplain(explain)),
        )))
    }

    #[napi(
        ts_args_type = "request: import('./query-types').GraphPipelineRequest",
        ts_return_type = "Promise<import('./query-types').GraphPipelineExplain>"
    )]
    pub fn explain_graph_pipeline_async(
        &self,
        request: serde_json::Value,
    ) -> Result<AsyncTask<EngineReadOp<GraphPipelineExplain, GraphPipelineJsExplain>>> {
        let query = parse_js_graph_pipeline_query(&request)?;
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.explain_graph_pipeline(&query),
            |explain| Ok(GraphPipelineJsExplain(explain)),
        )))
    }

    #[napi(
        ts_args_type = "query: string, params?: import('./query-types').GqlParams | null, options?: import('./query-types').GqlExecutionOptions | null",
        ts_return_type = "Promise<import('./query-types').GqlExecutionResult>"
    )]
    pub fn execute_gql_async(
        &self,
        query: String,
        params: Option<Unknown<'_>>,
        options: Option<GqlExecutionOptionsInput>,
    ) -> Result<AsyncTask<EngineOp<GqlExecutionResultPayload, GqlJsPayload>>> {
        let (options, compact_rows) = parse_js_gql_options(options)?;
        let referenced_params = gql_referenced_param_names(&query, &options)
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        let params = parse_js_gql_params(params, &referenced_params, &options)?;
        Ok(AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| {
                let result = eng.execute_gql(&query, &params, &options)?;
                Ok(GqlExecutionResultPayload {
                    result,
                    compact_rows,
                })
            },
            |payload| Ok(GqlJsPayload(GqlJsPayloadKind::Result(payload))),
        )))
    }

    #[napi(
        ts_args_type = "query: string, params?: import('./query-types').GqlParams | null, options?: import('./query-types').GqlExecutionOptions | null",
        ts_return_type = "Promise<import('./query-types').GqlExecutionExplain>"
    )]
    pub fn explain_gql_async(
        &self,
        query: String,
        params: Option<Unknown<'_>>,
        options: Option<GqlExecutionOptionsInput>,
    ) -> Result<AsyncTask<EngineReadOp<GqlExecutionExplain, GqlJsPayload>>> {
        let (options, _) = parse_js_gql_options(options)?;
        let referenced_params = gql_referenced_param_names(&query, &options)
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        let params = parse_js_gql_params(params, &referenced_params, &options)?;
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.explain_gql(&query, &params, &options),
            |explain| Ok(GqlJsPayload(GqlJsPayloadKind::Explain(explain))),
        )))
    }

    #[napi(ts_return_type = "Promise<NodePropertyIndexInfo>")]
    pub fn ensure_node_property_index_async(
        &self,
        label: String,
        spec: SecondaryIndexSpec,
    ) -> Result<AsyncTask<EngineOp<CoreNodePropertyIndexInfo, NodePropertyIndexInfo>>> {
        let spec = js_secondary_index_spec_to_rust(spec, JsSecondaryIndexTargetKind::Node)?;
        Ok(AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| eng.ensure_node_property_index(&label, spec.clone()),
            node_property_index_info_to_js,
        )))
    }

    #[napi(ts_return_type = "Promise<boolean>")]
    pub fn drop_node_property_index_async(
        &self,
        label: String,
        spec: SecondaryIndexSpec,
    ) -> Result<AsyncTask<EngineOp<bool, bool>>> {
        let spec = js_secondary_index_spec_to_rust(spec, JsSecondaryIndexTargetKind::Node)?;
        Ok(AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| eng.drop_node_property_index(&label, spec.clone()),
            Ok,
        )))
    }

    #[napi(ts_return_type = "Promise<Array<NodePropertyIndexInfo>>")]
    pub fn list_node_property_indexes_async(
        &self,
    ) -> AsyncTask<EngineReadOp<Vec<CoreNodePropertyIndexInfo>, Vec<NodePropertyIndexInfo>>> {
        AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            |eng| eng.list_node_property_indexes(),
            node_property_index_infos_to_js,
        ))
    }

    #[napi(ts_return_type = "Promise<EdgePropertyIndexInfo>")]
    pub fn ensure_edge_property_index_async(
        &self,
        label: String,
        spec: SecondaryIndexSpec,
    ) -> Result<AsyncTask<EngineOp<CoreEdgePropertyIndexInfo, EdgePropertyIndexInfo>>> {
        let spec = js_secondary_index_spec_to_rust(spec, JsSecondaryIndexTargetKind::Edge)?;
        Ok(AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| eng.ensure_edge_property_index(&label, spec.clone()),
            edge_property_index_info_to_js,
        )))
    }

    #[napi(ts_return_type = "Promise<boolean>")]
    pub fn drop_edge_property_index_async(
        &self,
        label: String,
        spec: SecondaryIndexSpec,
    ) -> Result<AsyncTask<EngineOp<bool, bool>>> {
        let spec = js_secondary_index_spec_to_rust(spec, JsSecondaryIndexTargetKind::Edge)?;
        Ok(AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            move |eng| eng.drop_edge_property_index(&label, spec.clone()),
            Ok,
        )))
    }

    #[napi(ts_return_type = "Promise<Array<EdgePropertyIndexInfo>>")]
    pub fn list_edge_property_indexes_async(
        &self,
    ) -> AsyncTask<EngineReadOp<Vec<CoreEdgePropertyIndexInfo>, Vec<EdgePropertyIndexInfo>>> {
        AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            |eng| eng.list_edge_property_indexes(),
            edge_property_index_infos_to_js,
        ))
    }

    #[napi(ts_return_type = "Promise<Float64Array>")]
    pub fn find_nodes_range_async(
        &self,
        label: String,
        prop_key: String,
        lower: Option<PropertyRangeBound>,
        upper: Option<PropertyRangeBound>,
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
            move |eng| eng.find_nodes_range(&label, &prop_key, lower.as_ref(), upper.as_ref()),
            |ids| ids_to_float64_array(&ids),
        )))
    }

    #[napi(ts_return_type = "Promise<PropertyRangePageResult>")]
    pub fn find_nodes_range_paged_async(
        &self,
        label: String,
        prop_key: String,
        lower: Option<PropertyRangeBound>,
        upper: Option<PropertyRangeBound>,
        options: Option<FindNodesRangePagedOptions>,
    ) -> Result<AsyncTask<EngineReadOp<CorePropertyRangePageResult<u64>, PropertyRangePageResult>>>
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
                eng.find_nodes_range_paged(&label, &prop_key, lower.as_ref(), upper.as_ref(), &page)
            },
            property_range_page_to_js,
        )))
    }

    #[napi(
        ts_args_type = "labels: string | string[]",
        ts_return_type = "Promise<Array<NodeView>>"
    )]
    pub fn get_nodes_by_labels_async(
        &self,
        labels: serde_json::Value,
    ) -> Result<AsyncTask<EngineReadOp<Vec<CoreNodeView>, Vec<NodeView>>>> {
        let labels = parse_js_node_labels_arg(&labels, "getNodesByLabelsAsync labels")?;
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.get_nodes_by_labels(labels),
            |records| {
                records
                    .into_iter()
                    .map(NodeView::try_from)
                    .collect::<Result<Vec<_>>>()
            },
        )))
    }

    #[napi(ts_return_type = "Promise<Array<EdgeView>>")]
    pub fn get_edges_by_label_async(
        &self,
        label: String,
    ) -> AsyncTask<EngineReadOp<Vec<CoreEdgeView>, Vec<EdgeView>>> {
        AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.get_edges_by_label(&label),
            |records| {
                records
                    .into_iter()
                    .map(EdgeView::try_from)
                    .collect::<Result<Vec<_>>>()
            },
        ))
    }

    #[napi(
        ts_args_type = "labels: string | string[]",
        ts_return_type = "Promise<number>"
    )]
    pub fn count_nodes_by_labels_async(
        &self,
        labels: serde_json::Value,
    ) -> Result<AsyncTask<EngineReadOp<u64, i64>>> {
        let labels = parse_js_node_labels_arg(&labels, "countNodesByLabelsAsync labels")?;
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.count_nodes_by_labels(labels),
            |count| Ok(count as i64),
        )))
    }

    #[napi(ts_return_type = "Promise<number>")]
    pub fn count_edges_by_label_async(&self, label: String) -> AsyncTask<EngineReadOp<u64, i64>> {
        AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.count_edges_by_label(&label),
            |count| Ok(count as i64),
        ))
    }

    #[napi(
        ts_args_type = "labels: string | string[]",
        ts_return_type = "Promise<Float64Array>"
    )]
    pub fn nodes_by_labels_async(
        &self,
        labels: serde_json::Value,
    ) -> Result<AsyncTask<EngineReadOp<Vec<u64>, Float64Array>>> {
        let labels = parse_js_node_labels_arg(&labels, "nodesByLabelsAsync labels")?;
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.nodes_by_labels(labels),
            |ids| ids_to_float64_array(&ids),
        )))
    }

    #[napi(ts_return_type = "Promise<Float64Array>")]
    pub fn edges_by_label_async(
        &self,
        label: String,
    ) -> AsyncTask<EngineReadOp<Vec<u64>, Float64Array>> {
        AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.edges_by_label(&label),
            |ids| ids_to_float64_array(&ids),
        ))
    }

    #[napi(ts_return_type = "Promise<Array<NeighborBatchEntry>>")]
    pub fn neighbors_batch_async(
        &self,
        node_ids: Vec<f64>,
        options: Option<NeighborsBatchOptions>,
    ) -> Result<AsyncTask<EngineReadOp<NodeIdMap<Vec<CoreNeighborEntry>>, Vec<NeighborBatchEntry>>>>
    {
        let ids: Vec<u64> = node_ids
            .into_iter()
            .map(f64_to_u64)
            .collect::<Result<Vec<_>>>()?;
        let (direction, edge_label_filter, at_epoch, decay_lambda) = match options {
            Some(o) => (o.direction, o.edge_label_filter, o.at_epoch, o.decay_lambda),
            None => (None, None, None, None),
        };
        let dir = parse_direction(direction.as_deref())?;
        let decay = decay_lambda.map(|v| v as f32);
        let opts = NeighborOptions {
            direction: dir,
            edge_label_filter: edge_label_filter,
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
        options: Option<DegreeOptions>,
    ) -> Result<AsyncTask<EngineReadOp<u64, i64>>> {
        let node_id = f64_to_u64(node_id)?;
        let (direction, edge_label_filter, at_epoch) = match options {
            Some(o) => (o.direction, o.edge_label_filter, o.at_epoch),
            None => (None, None, None),
        };
        let dir = parse_direction(direction.as_deref())?;
        let opts = CoreDegreeOptions {
            direction: dir,
            edge_label_filter: edge_label_filter,
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
        options: Option<SumEdgeWeightsOptions>,
    ) -> Result<AsyncTask<EngineReadOp<f64, f64>>> {
        let node_id = f64_to_u64(node_id)?;
        let (direction, edge_label_filter, at_epoch) = match options {
            Some(o) => (o.direction, o.edge_label_filter, o.at_epoch),
            None => (None, None, None),
        };
        let dir = parse_direction(direction.as_deref())?;
        let opts = CoreDegreeOptions {
            direction: dir,
            edge_label_filter: edge_label_filter,
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
        options: Option<AvgEdgeWeightOptions>,
    ) -> Result<AsyncTask<EngineReadOp<Option<f64>, Option<f64>>>> {
        let node_id = f64_to_u64(node_id)?;
        let (direction, edge_label_filter, at_epoch) = match options {
            Some(o) => (o.direction, o.edge_label_filter, o.at_epoch),
            None => (None, None, None),
        };
        let dir = parse_direction(direction.as_deref())?;
        let opts = CoreDegreeOptions {
            direction: dir,
            edge_label_filter: edge_label_filter,
            at_epoch,
        };
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.avg_edge_weight(node_id, &opts),
            Ok,
        )))
    }

    #[napi(ts_return_type = "Promise<Array<DegreeBatchEntry>>")]
    pub fn degrees_async(
        &self,
        node_ids: Vec<f64>,
        options: Option<DegreesOptions>,
    ) -> Result<AsyncTask<EngineReadOp<NodeIdMap<u64>, Vec<DegreeBatchEntry>>>> {
        let ids: Vec<u64> = node_ids
            .into_iter()
            .map(f64_to_u64)
            .collect::<Result<Vec<_>>>()?;
        let (direction, edge_label_filter, at_epoch) = match options {
            Some(o) => (o.direction, o.edge_label_filter, o.at_epoch),
            None => (None, None, None),
        };
        let dir = parse_direction(direction.as_deref())?;
        let opts = CoreDegreeOptions {
            direction: dir,
            edge_label_filter: edge_label_filter,
            at_epoch,
        };
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.degrees(&ids, &opts),
            |map| {
                let mut entries: Vec<DegreeBatchEntry> = map
                    .into_iter()
                    .map(|(node_id, degree)| {
                        Ok(DegreeBatchEntry {
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

    #[napi(ts_return_type = "Promise<ShortestPath | null>")]
    pub fn shortest_path_async(
        &self,
        from: f64,
        to: f64,
        options: Option<ShortestPathOptions>,
    ) -> Result<AsyncTask<EngineReadOp<Option<CoreShortestPath>, Option<ShortestPath>>>> {
        let from = f64_to_u64(from)?;
        let to = f64_to_u64(to)?;
        let (direction, edge_label_filter, weight_field, at_epoch, max_depth, max_cost) =
            match options {
                Some(o) => (
                    o.direction,
                    o.edge_label_filter,
                    o.weight_field,
                    o.at_epoch,
                    o.max_depth,
                    o.max_cost,
                ),
                None => (None, None, None, None, None, None),
            };
        let dir = parse_direction(direction.as_deref())?;
        let opts = CoreShortestPathOptions {
            direction: dir,
            edge_label_filter: edge_label_filter,
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
        options: Option<IsConnectedOptions>,
    ) -> Result<AsyncTask<EngineReadOp<bool, bool>>> {
        let from = f64_to_u64(from)?;
        let to = f64_to_u64(to)?;
        let (direction, edge_label_filter, at_epoch, max_depth) = match options {
            Some(o) => (o.direction, o.edge_label_filter, o.at_epoch, o.max_depth),
            None => (None, None, None, None),
        };
        let dir = parse_direction(direction.as_deref())?;
        let opts = CoreIsConnectedOptions {
            direction: dir,
            edge_label_filter: edge_label_filter,
            at_epoch,
            max_depth,
        };
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.is_connected(from, to, &opts),
            Ok,
        )))
    }

    #[napi(ts_return_type = "Promise<Array<ShortestPath>>")]
    pub fn all_shortest_paths_async(
        &self,
        from: f64,
        to: f64,
        options: Option<AllShortestPathsOptions>,
    ) -> Result<AsyncTask<EngineReadOp<Vec<CoreShortestPath>, Vec<ShortestPath>>>> {
        let from = f64_to_u64(from)?;
        let to = f64_to_u64(to)?;
        let (direction, edge_label_filter, weight_field, at_epoch, max_depth, max_cost, max_paths) =
            match options {
                Some(o) => (
                    o.direction,
                    o.edge_label_filter,
                    o.weight_field,
                    o.at_epoch,
                    o.max_depth,
                    o.max_cost,
                    o.max_paths,
                ),
                None => (None, None, None, None, None, None, None),
            };
        let dir = parse_direction(direction.as_deref())?;
        let opts = CoreAllShortestPathsOptions {
            direction: dir,
            edge_label_filter: edge_label_filter,
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

    #[napi(
        ts_args_type = "labels: string | string[], limit?: number | null, after?: number | null",
        ts_return_type = "Promise<IdPageResult>"
    )]
    pub fn nodes_by_labels_paged_async(
        &self,
        labels: serde_json::Value,
        limit: Option<u32>,
        after: Option<f64>,
    ) -> Result<AsyncTask<EngineReadOp<PageResult<u64>, IdPageResult>>> {
        let labels = parse_js_node_labels_arg(&labels, "nodesByLabelsPagedAsync labels")?;
        let page = make_page_request(limit, after)?;
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.nodes_by_labels_paged(labels, &page),
            id_page_to_js,
        )))
    }

    #[napi(ts_return_type = "Promise<IdPageResult>")]
    pub fn edges_by_label_paged_async(
        &self,
        label: String,
        limit: Option<u32>,
        after: Option<f64>,
    ) -> Result<AsyncTask<EngineReadOp<PageResult<u64>, IdPageResult>>> {
        let page = make_page_request(limit, after)?;
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.edges_by_label_paged(&label, &page),
            id_page_to_js,
        )))
    }

    #[napi(
        ts_args_type = "labels: string | string[], limit?: number | null, after?: number | null",
        ts_return_type = "Promise<NodePageResult>"
    )]
    pub fn get_nodes_by_labels_paged_async(
        &self,
        labels: serde_json::Value,
        limit: Option<u32>,
        after: Option<f64>,
    ) -> Result<AsyncTask<EngineReadOp<PageResult<CoreNodeView>, NodePageResult>>> {
        let labels = parse_js_node_labels_arg(&labels, "getNodesByLabelsPagedAsync labels")?;
        let page = make_page_request(limit, after)?;
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.get_nodes_by_labels_paged(labels, &page),
            node_page_to_js,
        )))
    }

    #[napi(ts_return_type = "Promise<EdgePageResult>")]
    pub fn get_edges_by_label_paged_async(
        &self,
        label: String,
        limit: Option<u32>,
        after: Option<f64>,
    ) -> Result<AsyncTask<EngineReadOp<PageResult<CoreEdgeView>, EdgePageResult>>> {
        let page = make_page_request(limit, after)?;
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.get_edges_by_label_paged(&label, &page),
            edge_page_to_js,
        )))
    }

    #[napi(ts_return_type = "Promise<IdPageResult>")]
    pub fn find_nodes_paged_async(
        &self,
        label: String,
        prop_key: String,
        prop_value: serde_json::Value,
        options: Option<FindNodesPagedOptions>,
    ) -> Result<AsyncTask<EngineReadOp<PageResult<u64>, IdPageResult>>> {
        let pv = json_to_prop_value(&prop_value);
        let (limit, after) = match options {
            Some(o) => (o.limit, o.after),
            None => (None, None),
        };
        let page = make_page_request(limit, after)?;
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.find_nodes_paged(&label, &prop_key, &pv, &page),
            id_page_to_js,
        )))
    }

    #[napi(ts_return_type = "Promise<Float64Array>")]
    pub fn find_nodes_by_time_range_async(
        &self,
        label: String,
        from_ms: i64,
        to_ms: i64,
    ) -> Result<AsyncTask<EngineReadOp<Vec<u64>, Float64Array>>> {
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.find_nodes_by_time_range(&label, from_ms, to_ms),
            |ids| ids_to_float64_array(&ids),
        )))
    }

    #[napi(ts_return_type = "Promise<IdPageResult>")]
    pub fn find_nodes_by_time_range_paged_async(
        &self,
        label: String,
        from_ms: i64,
        to_ms: i64,
        options: Option<FindNodesByTimeRangePagedOptions>,
    ) -> Result<AsyncTask<EngineReadOp<PageResult<u64>, IdPageResult>>> {
        let (limit, after) = match options {
            Some(o) => (o.limit, o.after),
            None => (None, None),
        };
        let page = make_page_request(limit, after)?;
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.find_nodes_by_time_range_paged(&label, from_ms, to_ms, &page),
            id_page_to_js,
        )))
    }

    #[napi(ts_return_type = "Promise<PprResult>")]
    pub fn personalized_pagerank_async(
        &self,
        seed_node_ids: Vec<f64>,
        options: Option<PersonalizedPagerankOptions>,
    ) -> Result<AsyncTask<EngineReadOp<CorePprResult, PprResult>>> {
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
            edge_label_filter,
            max_results,
        ) = match &options {
            Some(o) => {
                let edge_label_filter = o.edge_label_filter.clone();
                (
                    o.algorithm.as_deref(),
                    o.damping_factor,
                    o.max_iterations,
                    o.epsilon,
                    o.approx_residual_tolerance,
                    edge_label_filter,
                    o.max_results,
                )
            }
            None => (None, None, None, None, None, None, None),
        };
        let opts = js_ppr_options_to_ppr_options(
            algorithm,
            &damping_factor,
            &max_iterations,
            &epsilon,
            &approx_residual_tolerance,
            &edge_label_filter,
            &max_results,
        )?;
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.personalized_pagerank(&seeds, &opts),
            ppr_result_to_js,
        )))
    }

    #[napi(ts_return_type = "Promise<AdjacencyExport>")]
    pub fn export_adjacency_async(
        &self,
        options: Option<ExportOptions>,
    ) -> Result<AsyncTask<EngineReadOp<(CoreAdjacencyExport, bool), AdjacencyExport>>> {
        let include_weights = options
            .as_ref()
            .and_then(|o| o.include_weights)
            .unwrap_or(true);
        let opts = js_export_options_to_rust(options)?;
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| Ok((eng.export_adjacency(&opts)?, include_weights)),
            |pair| adjacency_export_to_js(pair.0, pair.1),
        )))
    }

    #[napi(ts_return_type = "Promise<NeighborPageResult>")]
    pub fn neighbors_paged_async(
        &self,
        node_id: f64,
        options: Option<NeighborsPagedOptions>,
    ) -> Result<AsyncTask<EngineReadOp<PageResult<CoreNeighborEntry>, NeighborPageResult>>> {
        let node_id = f64_to_u64(node_id)?;
        let (direction, edge_label_filter, limit, after, at_epoch, decay_lambda) = match options {
            Some(o) => (
                o.direction,
                o.edge_label_filter,
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
            edge_label_filter: edge_label_filter,
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

    #[napi(ts_return_type = "Promise<Array<ComponentEntry>>")]
    pub fn connected_components_async(
        &self,
        options: Option<ConnectedComponentsOptions>,
    ) -> Result<AsyncTask<EngineReadOp<NodeIdMap<u64>, Vec<ComponentEntry>>>> {
        let (edge_label_filter, node_label_filter, at_epoch) = match options {
            Some(o) => (
                o.edge_label_filter,
                o.node_label_filter
                    .map(js_node_label_filter_to_rust)
                    .transpose()?,
                o.at_epoch,
            ),
            None => (None, None, None),
        };
        let opts = ComponentOptions {
            edge_label_filter,
            node_label_filter: node_label_filter,
            at_epoch,
        };
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.connected_components(&opts),
            |map| {
                let mut entries: Vec<ComponentEntry> = map
                    .into_iter()
                    .map(|(node_id, component_id)| {
                        Ok(ComponentEntry {
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
        options: Option<ComponentOfOptions>,
    ) -> Result<AsyncTask<EngineReadOp<Vec<u64>, Float64Array>>> {
        let node_id = f64_to_u64(node_id)?;
        let (edge_label_filter, node_label_filter, at_epoch) = match options {
            Some(o) => (
                o.edge_label_filter,
                o.node_label_filter
                    .map(js_node_label_filter_to_rust)
                    .transpose()?,
                o.at_epoch,
            ),
            None => (None, None, None),
        };
        let opts = ComponentOptions {
            edge_label_filter,
            node_label_filter: node_label_filter,
            at_epoch,
        };
        Ok(AsyncTask::new(EngineReadOp::new(
            self.inner.clone(),
            move |eng| eng.component_of(node_id, &opts),
            |members| ids_to_float64_array(&members),
        )))
    }

    #[napi(ts_return_type = "Promise<Array<VectorHit>>")]
    pub fn vector_search_async(
        &self,
        mode: String,
        options: VectorSearchOptions,
    ) -> Result<AsyncTask<EngineReadOp<Vec<CoreVectorHit>, Vec<VectorHit>>>> {
        let mode = parse_vector_search_mode(&mode)?;
        let k = options.k;
        let dense_query = options.dense_query;
        let sparse_query = options.sparse_query;
        let label_filter = options
            .label_filter
            .map(js_node_label_filter_to_rust)
            .transpose()?;
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
            Some(s) => Some(CoreVectorSearchScope {
                start_node_id: f64_to_u64(s.start_node_id)?,
                max_depth: s.max_depth,
                direction: parse_direction(s.direction.as_deref())?,
                edge_label_filter: s.edge_label_filter,
                at_epoch: s.at_epoch,
            }),
        };
        let request = VectorSearchRequest {
            mode,
            dense_query: dense_q,
            sparse_query: sparse_q,
            k: k as usize,
            label_filter,
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
                        Ok(VectorHit {
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

    #[napi(ts_return_type = "Promise<CompactionStats | null>")]
    pub fn end_ingest_async(
        &self,
    ) -> AsyncTask<EngineOp<Option<CoreCompactionStats>, Option<CompactionStats>>> {
        AsyncTask::new(EngineOp::new(
            self.inner.clone(),
            |eng| eng.end_ingest(),
            |s| Ok(s.map(|s| s.into())),
        ))
    }

    #[napi(ts_return_type = "Promise<CompactionStats | null>")]
    pub fn compact_async(
        &self,
    ) -> AsyncTask<EngineOp<Option<CoreCompactionStats>, Option<CompactionStats>>> {
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
        ts_args_type = "callback: (progress: CompactionProgress) => void",
        ts_return_type = "Promise<CompactionStats | null>"
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

#[napi]
pub struct WriteTxn {
    inner: Arc<Mutex<Option<CoreWriteTxn>>>,
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

fn write_txn_to_js(txn: CoreWriteTxn) -> WriteTxn {
    WriteTxn {
        inner: Arc::new(Mutex::new(Some(txn))),
        async_order: Arc::new(TxnAsyncOrder::new()),
    }
}

#[napi]
impl WriteTxn {
    #[napi(
        ts_args_type = "labels: string | string[], key: string, options?: UpsertNodeOptions | null"
    )]
    pub fn upsert_node(
        &self,
        labels: serde_json::Value,
        key: String,
        options: Option<UpsertNodeOptions>,
    ) -> Result<TxnNodeRef> {
        let labels = parse_js_node_labels_arg(&labels, "transaction upsertNode labels")?;
        let ref_label = labels.first().cloned().ok_or_else(|| {
            napi::Error::from_reason("transaction upsertNode requires labels".to_string())
        })?;
        with_txn(&self.inner, |txn| {
            txn.upsert_node(labels, &key, js_upsert_node_options(options))
        })?;
        Ok(TxnNodeRef {
            id: None,
            labels: Some(txn_node_ref_labels_value(ref_label)),
            key: Some(key),
            local: None,
        })
    }

    #[napi(
        ts_args_type = "alias: string, labels: string | string[], key: string, options?: UpsertNodeOptions | null"
    )]
    pub fn upsert_node_as(
        &self,
        alias: String,
        labels: serde_json::Value,
        key: String,
        options: Option<UpsertNodeOptions>,
    ) -> Result<TxnNodeRef> {
        let labels = parse_js_node_labels_arg(&labels, "transaction upsertNodeAs labels")?;
        let node_ref = with_txn(&self.inner, |txn| {
            txn.upsert_node_as(&alias, labels, &key, js_upsert_node_options(options))
        })?;
        txn_node_ref_to_js(node_ref)
    }

    #[napi]
    pub fn add_node_label(&self, target: TxnNodeRef, label: String) -> Result<bool> {
        let target = js_txn_node_ref_to_rust(target)?;
        with_txn(&self.inner, |txn| txn.add_node_label(target, &label))
    }

    #[napi]
    pub fn remove_node_label(&self, target: TxnNodeRef, label: String) -> Result<bool> {
        let target = js_txn_node_ref_to_rust(target)?;
        with_txn(&self.inner, |txn| txn.remove_node_label(target, &label))
    }

    #[napi]
    pub fn upsert_edge(
        &self,
        from: TxnNodeRef,
        to: TxnNodeRef,
        label: String,
        options: Option<UpsertEdgeOptions>,
    ) -> Result<TxnEdgeRef> {
        let from_rust = js_txn_node_ref_to_rust(from.clone())?;
        let to_rust = js_txn_node_ref_to_rust(to.clone())?;
        with_txn(&self.inner, |txn| {
            txn.upsert_edge(from_rust, to_rust, &label, js_upsert_edge_options(options))
        })?;
        Ok(TxnEdgeRef {
            id: None,
            from: Some(from),
            to: Some(to),
            label: Some(label),
            local: None,
        })
    }

    #[napi]
    pub fn upsert_edge_as(
        &self,
        alias: String,
        from: TxnNodeRef,
        to: TxnNodeRef,
        label: String,
        options: Option<UpsertEdgeOptions>,
    ) -> Result<TxnEdgeRef> {
        let from = js_txn_node_ref_to_rust(from)?;
        let to = js_txn_node_ref_to_rust(to)?;
        let edge_ref = with_txn(&self.inner, |txn| {
            txn.upsert_edge_as(&alias, from, to, &label, js_upsert_edge_options(options))
        })?;
        txn_edge_ref_to_js(edge_ref)
    }

    #[napi]
    pub fn delete_node(&self, target: TxnNodeRef) -> Result<()> {
        let target = js_txn_node_ref_to_rust(target)?;
        with_txn(&self.inner, |txn| txn.delete_node(target))
    }

    #[napi]
    pub fn delete_edge(&self, target: TxnEdgeRef) -> Result<()> {
        let target = js_txn_edge_ref_to_rust(target)?;
        with_txn(&self.inner, |txn| txn.delete_edge(target))
    }

    #[napi]
    pub fn invalidate_edge(&self, target: TxnEdgeRef, valid_to: i64) -> Result<()> {
        let target = js_txn_edge_ref_to_rust(target)?;
        with_txn(&self.inner, |txn| txn.invalidate_edge(target, valid_to))
    }

    #[napi(
        ts_args_type = "operations: Array<{ op: 'upsertNode'; alias?: string; labels: string | string[]; key: string; props?: Record<string, any>; weight?: number; denseVector?: Array<number>; sparseVector?: Array<SparseEntry> } | { op: 'upsertEdge'; alias?: string; from: TxnNodeRef; to: TxnNodeRef; label: string; props?: Record<string, any>; weight?: number; validFrom?: number; validTo?: number } | { op: 'deleteNode'; target: TxnEdgeOrNodeRef } | { op: 'deleteEdge'; target: TxnEdgeOrNodeRef } | { op: 'invalidateEdge'; target: TxnEdgeOrNodeRef; validTo: number }>"
    )]
    pub fn stage(&self, operations: Vec<serde_json::Value>) -> Result<()> {
        let intents = operations
            .into_iter()
            .map(js_txn_operation_to_rust)
            .collect::<Result<Vec<_>>>()?;
        with_txn(&self.inner, |txn| txn.stage_intents(intents))
    }

    #[napi]
    pub fn get_node(&self, target: TxnNodeRef) -> Result<Option<TxnNodeView>> {
        let target = js_txn_node_ref_to_rust(target)?;
        let view = with_txn_ref(&self.inner, |txn| txn.get_node(target))?;
        view.map(txn_node_view_to_js).transpose()
    }

    #[napi]
    pub fn get_edge(&self, target: TxnEdgeRef) -> Result<Option<TxnEdgeView>> {
        let target = js_txn_edge_ref_to_rust(target)?;
        let view = with_txn_ref(&self.inner, |txn| txn.get_edge(target))?;
        view.map(txn_edge_view_to_js).transpose()
    }

    #[napi]
    pub fn get_node_by_key(&self, label: String, key: String) -> Result<Option<TxnNodeView>> {
        let view = with_txn_ref(&self.inner, |txn| txn.get_node_by_key(&label, &key))?;
        view.map(txn_node_view_to_js).transpose()
    }

    #[napi]
    pub fn get_edge_by_triple(
        &self,
        from: TxnNodeRef,
        to: TxnNodeRef,
        label: String,
    ) -> Result<Option<TxnEdgeView>> {
        let from = js_txn_node_ref_to_rust(from)?;
        let to = js_txn_node_ref_to_rust(to)?;
        let view = with_txn_ref(&self.inner, |txn| txn.get_edge_by_triple(from, to, &label))?;
        view.map(txn_edge_view_to_js).transpose()
    }

    #[napi]
    pub fn commit(&self) -> Result<TxnCommitResult> {
        let result = with_txn_take(&self.inner, |txn| txn.commit())?;
        txn_commit_result_to_js(result)
    }

    #[napi]
    pub fn rollback(&self) -> Result<()> {
        with_txn_take(&self.inner, |txn| txn.rollback())
    }

    #[napi(
        ts_args_type = "labels: string | string[], key: string, options?: UpsertNodeOptions | null",
        ts_return_type = "Promise<TxnNodeRef>"
    )]
    pub fn upsert_node_async(
        &self,
        labels: serde_json::Value,
        key: String,
        options: Option<UpsertNodeOptions>,
    ) -> Result<AsyncTask<TxnAsyncOp<TxnNodeRef, TxnNodeRef>>> {
        let labels = parse_js_node_labels_arg(&labels, "transaction upsertNodeAsync labels")?;
        let ref_label = labels.first().cloned().ok_or_else(|| {
            napi::Error::from_reason("transaction upsertNodeAsync requires labels".to_string())
        })?;
        let opts = js_upsert_node_options(options);
        Ok(AsyncTask::new(TxnAsyncOp::new(
            self,
            move |txn| {
                txn.upsert_node(labels, &key, opts)?;
                Ok(TxnNodeRef {
                    id: None,
                    labels: Some(txn_node_ref_labels_value(ref_label)),
                    key: Some(key),
                    local: None,
                })
            },
            napi_identity,
        )?))
    }

    #[napi(
        ts_args_type = "alias: string, labels: string | string[], key: string, options?: UpsertNodeOptions | null",
        ts_return_type = "Promise<TxnNodeRef>"
    )]
    pub fn upsert_node_as_async(
        &self,
        alias: String,
        labels: serde_json::Value,
        key: String,
        options: Option<UpsertNodeOptions>,
    ) -> Result<AsyncTask<TxnAsyncOp<CoreTxnNodeRef, TxnNodeRef>>> {
        let labels = parse_js_node_labels_arg(&labels, "transaction upsertNodeAsAsync labels")?;
        let opts = js_upsert_node_options(options);
        Ok(AsyncTask::new(TxnAsyncOp::new(
            self,
            move |txn| txn.upsert_node_as(&alias, labels, &key, opts),
            txn_node_ref_to_js,
        )?))
    }

    #[napi(ts_return_type = "Promise<boolean>")]
    pub fn add_node_label_async(
        &self,
        target: TxnNodeRef,
        label: String,
    ) -> Result<AsyncTask<TxnAsyncOp<bool, bool>>> {
        let target = js_txn_node_ref_to_rust(target)?;
        Ok(AsyncTask::new(TxnAsyncOp::new(
            self,
            move |txn| txn.add_node_label(target, &label),
            Ok,
        )?))
    }

    #[napi(ts_return_type = "Promise<boolean>")]
    pub fn remove_node_label_async(
        &self,
        target: TxnNodeRef,
        label: String,
    ) -> Result<AsyncTask<TxnAsyncOp<bool, bool>>> {
        let target = js_txn_node_ref_to_rust(target)?;
        Ok(AsyncTask::new(TxnAsyncOp::new(
            self,
            move |txn| txn.remove_node_label(target, &label),
            Ok,
        )?))
    }

    #[napi(ts_return_type = "Promise<TxnEdgeRef>")]
    pub fn upsert_edge_async(
        &self,
        from: TxnNodeRef,
        to: TxnNodeRef,
        label: String,
        options: Option<UpsertEdgeOptions>,
    ) -> Result<AsyncTask<TxnAsyncOp<TxnEdgeRef, TxnEdgeRef>>> {
        let from_rust = js_txn_node_ref_to_rust(from.clone())?;
        let to_rust = js_txn_node_ref_to_rust(to.clone())?;
        let opts = js_upsert_edge_options(options);
        Ok(AsyncTask::new(TxnAsyncOp::new(
            self,
            move |txn| {
                txn.upsert_edge(from_rust, to_rust, &label, opts)?;
                Ok(TxnEdgeRef {
                    id: None,
                    from: Some(from),
                    to: Some(to),
                    label: Some(label),
                    local: None,
                })
            },
            napi_identity,
        )?))
    }

    #[napi(ts_return_type = "Promise<TxnEdgeRef>")]
    pub fn upsert_edge_as_async(
        &self,
        alias: String,
        from: TxnNodeRef,
        to: TxnNodeRef,
        label: String,
        options: Option<UpsertEdgeOptions>,
    ) -> Result<AsyncTask<TxnAsyncOp<CoreTxnEdgeRef, TxnEdgeRef>>> {
        let from = js_txn_node_ref_to_rust(from)?;
        let to = js_txn_node_ref_to_rust(to)?;
        let opts = js_upsert_edge_options(options);
        Ok(AsyncTask::new(TxnAsyncOp::new(
            self,
            move |txn| txn.upsert_edge_as(&alias, from, to, &label, opts),
            txn_edge_ref_to_js,
        )?))
    }

    #[napi(ts_return_type = "Promise<void>")]
    pub fn delete_node_async(&self, target: TxnNodeRef) -> Result<AsyncTask<TxnAsyncOp<(), ()>>> {
        let target = js_txn_node_ref_to_rust(target)?;
        Ok(AsyncTask::new(TxnAsyncOp::new(
            self,
            move |txn| txn.delete_node(target),
            |_| Ok(()),
        )?))
    }

    #[napi(ts_return_type = "Promise<void>")]
    pub fn delete_edge_async(&self, target: TxnEdgeRef) -> Result<AsyncTask<TxnAsyncOp<(), ()>>> {
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
        target: TxnEdgeRef,
        valid_to: i64,
    ) -> Result<AsyncTask<TxnAsyncOp<(), ()>>> {
        let target = js_txn_edge_ref_to_rust(target)?;
        Ok(AsyncTask::new(TxnAsyncOp::new(
            self,
            move |txn| txn.invalidate_edge(target, valid_to),
            |_| Ok(()),
        )?))
    }

    #[napi(
        ts_args_type = "operations: Array<{ op: 'upsertNode'; alias?: string; labels: string | string[]; key: string; props?: Record<string, any>; weight?: number; denseVector?: Array<number>; sparseVector?: Array<SparseEntry> } | { op: 'upsertEdge'; alias?: string; from: TxnNodeRef; to: TxnNodeRef; label: string; props?: Record<string, any>; weight?: number; validFrom?: number; validTo?: number } | { op: 'deleteNode'; target: TxnEdgeOrNodeRef } | { op: 'deleteEdge'; target: TxnEdgeOrNodeRef } | { op: 'invalidateEdge'; target: TxnEdgeOrNodeRef; validTo: number }>",
        ts_return_type = "Promise<void>"
    )]
    pub fn stage_async(
        &self,
        operations: Vec<serde_json::Value>,
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

    #[napi(ts_return_type = "Promise<TxnNodeView | null>")]
    pub fn get_node_async(
        &self,
        target: TxnNodeRef,
    ) -> Result<AsyncTask<TxnAsyncOp<Option<CoreTxnNodeView>, Option<TxnNodeView>>>> {
        let target = js_txn_node_ref_to_rust(target)?;
        Ok(AsyncTask::new(TxnAsyncOp::new(
            self,
            move |txn| txn.get_node(target),
            |view| view.map(txn_node_view_to_js).transpose(),
        )?))
    }

    #[napi(ts_return_type = "Promise<TxnEdgeView | null>")]
    pub fn get_edge_async(
        &self,
        target: TxnEdgeRef,
    ) -> Result<AsyncTask<TxnAsyncOp<Option<CoreTxnEdgeView>, Option<TxnEdgeView>>>> {
        let target = js_txn_edge_ref_to_rust(target)?;
        Ok(AsyncTask::new(TxnAsyncOp::new(
            self,
            move |txn| txn.get_edge(target),
            |view| view.map(txn_edge_view_to_js).transpose(),
        )?))
    }

    #[napi(ts_return_type = "Promise<TxnNodeView | null>")]
    pub fn get_node_by_key_async(
        &self,
        label: String,
        key: String,
    ) -> Result<AsyncTask<TxnAsyncOp<Option<CoreTxnNodeView>, Option<TxnNodeView>>>> {
        Ok(AsyncTask::new(TxnAsyncOp::new(
            self,
            move |txn| txn.get_node_by_key(&label, &key),
            |view| view.map(txn_node_view_to_js).transpose(),
        )?))
    }

    #[napi(ts_return_type = "Promise<TxnEdgeView | null>")]
    pub fn get_edge_by_triple_async(
        &self,
        from: TxnNodeRef,
        to: TxnNodeRef,
        label: String,
    ) -> Result<AsyncTask<TxnAsyncOp<Option<CoreTxnEdgeView>, Option<TxnEdgeView>>>> {
        let from = js_txn_node_ref_to_rust(from)?;
        let to = js_txn_node_ref_to_rust(to)?;
        Ok(AsyncTask::new(TxnAsyncOp::new(
            self,
            move |txn| txn.get_edge_by_triple(from, to, &label),
            |view| view.map(txn_edge_view_to_js).transpose(),
        )?))
    }

    #[napi(ts_return_type = "Promise<TxnCommitResult>")]
    pub fn commit_async(
        &self,
    ) -> Result<AsyncTask<TxnAsyncTakeOp<CoreTxnCommitResult, TxnCommitResult>>> {
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
pub struct CloseOptions {
    /// If true, cancel any in-progress background compaction instead of waiting.
    pub force: Option<bool>,
}

#[napi(object)]
pub struct GqlExecutionOptionsInput {
    pub mode: Option<String>,
    pub allow_full_scan: Option<bool>,
    pub max_rows: Option<f64>,
    pub cursor: Option<String>,
    pub max_cursor_bytes: Option<f64>,
    pub max_mutation_rows: Option<f64>,
    pub max_mutation_ops: Option<f64>,
    pub max_pipeline_rows: Option<f64>,
    pub max_groups: Option<f64>,
    pub max_collect_items: Option<f64>,
    pub max_union_branches: Option<f64>,
    pub max_subquery_invocations: Option<f64>,
    pub max_subquery_depth: Option<f64>,
    pub max_shortest_path_pairs: Option<f64>,
    pub max_intermediate_bindings: Option<f64>,
    pub max_frontier: Option<f64>,
    pub max_path_hops: Option<f64>,
    pub max_paths_per_start: Option<f64>,
    pub max_order_materialization: Option<f64>,
    pub max_skip: Option<f64>,
    pub max_query_bytes: Option<f64>,
    pub max_param_bytes: Option<f64>,
    pub max_ast_depth: Option<f64>,
    pub max_literal_items: Option<f64>,
    pub include_plan: Option<bool>,
    pub profile: Option<bool>,
    pub compact_rows: Option<bool>,
    pub include_vectors: Option<bool>,
}

// ============================================================
// Method options structs (positional required + options bag)
// ============================================================

#[napi(object)]
pub struct UpsertNodeOptions {
    pub props: Option<HashMap<String, serde_json::Value>>,
    pub weight: Option<f64>,
    pub dense_vector: Option<Vec<f64>>,
    pub sparse_vector: Option<Vec<SparseEntry>>,
}

#[napi(object)]
pub struct UpsertEdgeOptions {
    pub props: Option<HashMap<String, serde_json::Value>>,
    pub weight: Option<f64>,
    pub valid_from: Option<i64>,
    pub valid_to: Option<i64>,
}

#[napi(object)]
pub struct NeighborsOptions {
    pub direction: Option<String>,
    pub edge_label_filter: Option<Vec<String>>,
    pub limit: Option<u32>,
    pub at_epoch: Option<i64>,
    pub decay_lambda: Option<f64>,
}

#[napi(object)]
pub struct NeighborsPagedOptions {
    pub direction: Option<String>,
    pub edge_label_filter: Option<Vec<String>>,
    pub limit: Option<u32>,
    pub after: Option<f64>,
    pub at_epoch: Option<i64>,
    pub decay_lambda: Option<f64>,
}

#[napi(object)]
pub struct NeighborsBatchOptions {
    pub direction: Option<String>,
    pub edge_label_filter: Option<Vec<String>>,
    pub at_epoch: Option<i64>,
    pub decay_lambda: Option<f64>,
}

#[napi(object)]
#[derive(Clone)]
pub struct NodeLabelFilter {
    pub labels: Vec<String>,
    #[napi(ts_type = "'any' | 'all'")]
    pub mode: String,
}

#[napi(object)]
pub struct TraverseOptions {
    pub min_depth: Option<u32>,
    pub direction: Option<String>,
    pub edge_label_filter: Option<Vec<String>>,
    pub emit_node_label_filter: Option<NodeLabelFilter>,
    pub at_epoch: Option<i64>,
    pub decay_lambda: Option<f64>,
    pub limit: Option<u32>,
    pub cursor: Option<TraversalCursor>,
}

#[napi(object)]
pub struct TopKNeighborsOptions {
    pub direction: Option<String>,
    pub edge_label_filter: Option<Vec<String>>,
    pub scoring: Option<String>,
    pub decay_lambda: Option<f64>,
    pub at_epoch: Option<i64>,
}

#[napi(object)]
pub struct ExtractSubgraphOptions {
    pub direction: Option<String>,
    pub edge_label_filter: Option<Vec<String>>,
    pub node_label_filter: Option<NodeLabelFilter>,
    pub at_epoch: Option<i64>,
}

#[napi(object)]
pub struct ShortestPathOptions {
    pub direction: Option<String>,
    pub edge_label_filter: Option<Vec<String>>,
    pub weight_field: Option<String>,
    pub at_epoch: Option<i64>,
    pub max_depth: Option<u32>,
    pub max_cost: Option<f64>,
}

#[napi(object)]
pub struct AllShortestPathsOptions {
    pub direction: Option<String>,
    pub edge_label_filter: Option<Vec<String>>,
    pub weight_field: Option<String>,
    pub at_epoch: Option<i64>,
    pub max_depth: Option<u32>,
    pub max_cost: Option<f64>,
    pub max_paths: Option<u32>,
}

#[napi(object)]
pub struct IsConnectedOptions {
    pub direction: Option<String>,
    pub edge_label_filter: Option<Vec<String>>,
    pub at_epoch: Option<i64>,
    pub max_depth: Option<u32>,
}

#[napi(object)]
pub struct ConnectedComponentsOptions {
    pub edge_label_filter: Option<Vec<String>>,
    pub node_label_filter: Option<NodeLabelFilter>,
    pub at_epoch: Option<i64>,
}

#[napi(object)]
pub struct ComponentOfOptions {
    pub edge_label_filter: Option<Vec<String>>,
    pub node_label_filter: Option<NodeLabelFilter>,
    pub at_epoch: Option<i64>,
}

#[napi(object)]
pub struct DegreeOptions {
    pub direction: Option<String>,
    pub edge_label_filter: Option<Vec<String>>,
    pub at_epoch: Option<i64>,
}

#[napi(object)]
pub struct SumEdgeWeightsOptions {
    pub direction: Option<String>,
    pub edge_label_filter: Option<Vec<String>>,
    pub at_epoch: Option<i64>,
}

#[napi(object)]
pub struct AvgEdgeWeightOptions {
    pub direction: Option<String>,
    pub edge_label_filter: Option<Vec<String>>,
    pub at_epoch: Option<i64>,
}

#[napi(object)]
pub struct DegreesOptions {
    pub direction: Option<String>,
    pub edge_label_filter: Option<Vec<String>>,
    pub at_epoch: Option<i64>,
}

#[napi(object)]
pub struct VectorSearchOptions {
    pub k: u32,
    pub dense_query: Option<Vec<f64>>,
    pub sparse_query: Option<Vec<SparseEntry>>,
    pub label_filter: Option<NodeLabelFilter>,
    pub ef_search: Option<u32>,
    pub scope: Option<VectorSearchScope>,
    pub dense_weight: Option<f64>,
    pub sparse_weight: Option<f64>,
    pub fusion_mode: Option<String>,
}

#[napi(object)]
pub struct FindNodesPagedOptions {
    pub limit: Option<u32>,
    pub after: Option<f64>,
}

#[napi(object)]
pub struct SecondaryIndexField {
    pub source: String,
    pub key: Option<String>,
    pub field: Option<String>,
}

#[napi(object)]
pub struct SecondaryIndexSpec {
    pub fields: Option<serde_json::Value>,
    pub kind: Option<serde_json::Value>,
}

#[napi(object)]
pub struct NodePropertyIndexInfo {
    pub index_id: f64,
    pub label: String,
    pub fields: Vec<SecondaryIndexField>,
    pub kind: String,
    pub state: String,
    pub last_error: serde_json::Value,
    pub compound: bool,
}

#[napi(object)]
pub struct EdgePropertyIndexInfo {
    pub index_id: f64,
    pub label: String,
    pub fields: Vec<SecondaryIndexField>,
    pub kind: String,
    pub state: String,
    pub last_error: serde_json::Value,
    pub compound: bool,
}

#[napi(object)]
pub struct SchemaSetOptions {
    pub max_violations: Option<f64>,
    pub chunk_size: Option<f64>,
    #[napi(ts_type = "number | null")]
    pub scan_limit: Option<serde_json::Value>,
}

#[napi(object)]
pub struct SchemaCheckOptions {
    pub max_violations: Option<f64>,
    pub chunk_size: Option<f64>,
    #[napi(ts_type = "number | null")]
    pub scan_limit: Option<serde_json::Value>,
}

pub struct NodeSchemaInfoPayload(CoreNodeSchemaInfo);
pub struct EdgeSchemaInfoPayload(CoreEdgeSchemaInfo);
pub struct SchemaValidationReportPayload(CoreSchemaValidationReport);
pub struct GraphSchemaCheckReportPayload(CoreGraphSchemaCheckReport);
pub struct GraphSchemaPublishResultPayload(CoreGraphSchemaPublishResult);
struct SchemaLiteralPayload(PropValue);

#[napi(object)]
pub struct NodeLabelInfo {
    pub label: String,
    pub label_id: u32,
}

impl From<CoreNodeLabelInfo> for NodeLabelInfo {
    fn from(info: CoreNodeLabelInfo) -> Self {
        Self {
            label: info.label,
            label_id: info.label_id,
        }
    }
}

#[napi(object)]
pub struct EdgeLabelInfo {
    pub label: String,
    pub label_id: u32,
}

impl From<CoreEdgeLabelInfo> for EdgeLabelInfo {
    fn from(info: CoreEdgeLabelInfo) -> Self {
        Self {
            label: info.label,
            label_id: info.label_id,
        }
    }
}

#[napi(object)]
#[derive(Clone)]
pub struct PropertyRangeBound {
    pub value: f64,
    pub inclusive: Option<bool>,
    pub domain: String,
}

#[napi(object)]
#[derive(Clone)]
pub struct PropertyRangeCursor {
    pub value: f64,
    pub node_id: f64,
    pub domain: String,
}

#[napi(object)]
#[derive(Clone)]
pub struct FindNodesRangePagedOptions {
    pub limit: Option<u32>,
    pub after: Option<PropertyRangeCursor>,
}

#[napi(object)]
pub struct FindNodesByTimeRangePagedOptions {
    pub limit: Option<u32>,
    pub after: Option<f64>,
}

#[napi(object)]
pub struct PersonalizedPagerankOptions {
    pub algorithm: Option<String>,
    pub damping_factor: Option<f64>,
    pub max_iterations: Option<u32>,
    pub epsilon: Option<f64>,
    pub approx_residual_tolerance: Option<f64>,
    pub edge_label_filter: Option<Vec<String>>,
    pub max_results: Option<u32>,
}

#[napi(object)]
pub struct DbStats {
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

impl From<CoreDbStats> for DbStats {
    fn from(s: CoreDbStats) -> Self {
        DbStats {
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
pub struct ScrubReport {
    pub segments: Vec<SegmentScrubResult>,
    pub total_components_checked: f64,
    pub total_components_ok: f64,
    pub total_components_failed: f64,
    pub total_bytes_digested: f64,
    pub duration_ms: f64,
}

#[napi(object)]
pub struct SegmentScrubResult {
    pub segment_id: f64,
    pub findings: Vec<ComponentScrubFinding>,
    pub components_ok: f64,
    pub bytes_digested: f64,
}

#[napi(object)]
pub struct ComponentScrubFinding {
    pub component_kind: String,
    pub finding_type: String,
    pub detail: String,
}

impl From<CoreScrubReport> for ScrubReport {
    fn from(r: CoreScrubReport) -> Self {
        ScrubReport {
            segments: r.segments.into_iter().map(|s| s.into()).collect(),
            total_components_checked: r.total_components_checked as f64,
            total_components_ok: r.total_components_ok as f64,
            total_components_failed: r.total_components_failed as f64,
            total_bytes_digested: r.total_bytes_digested as f64,
            duration_ms: r.duration_ms as f64,
        }
    }
}

impl From<overgraph::SegmentScrubResult> for SegmentScrubResult {
    fn from(s: overgraph::SegmentScrubResult) -> Self {
        SegmentScrubResult {
            segment_id: s.segment_id as f64,
            findings: s.findings.into_iter().map(|f| f.into()).collect(),
            components_ok: s.components_ok as f64,
            bytes_digested: s.bytes_digested as f64,
        }
    }
}

impl From<overgraph::ComponentScrubFinding> for ComponentScrubFinding {
    fn from(f: overgraph::ComponentScrubFinding) -> Self {
        ComponentScrubFinding {
            component_kind: f.component_kind,
            finding_type: format!("{:?}", f.finding_type),
            detail: f.detail,
        }
    }
}

#[napi(object)]
pub struct DenseVectorConfig {
    pub dimension: u32,
    pub metric: Option<String>,
}

#[napi(object)]
pub struct DbOptions {
    pub create_if_missing: Option<bool>,
    pub edge_uniqueness: Option<bool>,
    pub memtable_flush_threshold: Option<u32>,
    /// Trigger compaction automatically after this many flushes. Default 4, 0 = disabled.
    pub compact_after_n_flushes: Option<u32>,
    pub dense_vector: Option<DenseVectorConfig>,
    /// WAL sync mode: 'immediate' or 'group-commit' (default).
    pub wal_sync_mode: Option<String>,
    /// Group commit sync interval in milliseconds. Default: 50.
    pub group_commit_interval_ms: Option<u32>,
    /// Hard cap on memtable size in bytes. Writes trigger a flush when exceeded. 0 = disabled.
    pub memtable_hard_cap_bytes: Option<u32>,
    /// Maximum number of immutable memtables pending flush before writers block.
    /// Default: 4. Set to 0 to disable immutable count backpressure.
    pub max_immutable_memtables: Option<u32>,
}

impl From<DbOptions> for CoreDbOptions {
    fn from(js: DbOptions) -> Self {
        let defaults = CoreDbOptions::default();
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
            CoreDenseVectorConfig {
                dimension: dv.dimension,
                metric,
                hnsw: HnswConfig::default(),
            }
        });
        CoreDbOptions {
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
pub struct KeyQuery {
    pub label: String,
    pub key: String,
}

impl TryFrom<KeyQuery> for NodeKeyQuery {
    type Error = napi::Error;

    fn try_from(js: KeyQuery) -> std::result::Result<Self, Self::Error> {
        Ok(NodeKeyQuery {
            label: js.label,
            key: js.key,
        })
    }
}

#[napi(object)]
pub struct NodeInput {
    #[napi(ts_type = "string | string[]")]
    pub labels: serde_json::Value,
    pub key: String,
    pub props: Option<HashMap<String, serde_json::Value>>,
    pub weight: Option<f64>,
    pub dense_vector: Option<Vec<f64>>,
    pub sparse_vector: Option<Vec<SparseEntry>>,
}

impl TryFrom<NodeInput> for CoreNodeInput {
    type Error = napi::Error;

    fn try_from(js: NodeInput) -> std::result::Result<Self, Self::Error> {
        let labels = parse_js_node_labels_arg(&js.labels, "NodeInput labels")?;
        Ok(CoreNodeInput {
            labels,
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
        })
    }
}

#[napi(object)]
#[derive(Clone)]
pub struct SparseEntry {
    pub dimension: u32,
    pub value: f64,
}

#[napi(object)]
#[derive(Clone)]
pub struct TxnNodeRef {
    pub id: Option<f64>,
    #[napi(ts_type = "string | string[]")]
    pub labels: Option<serde_json::Value>,
    pub key: Option<String>,
    pub local: Option<String>,
}

#[napi(object)]
#[derive(Clone)]
pub struct TxnEdgeRef {
    pub id: Option<f64>,
    pub from: Option<TxnNodeRef>,
    pub to: Option<TxnNodeRef>,
    pub label: Option<String>,
    pub local: Option<String>,
}

#[napi(object)]
#[derive(Clone)]
pub struct TxnEdgeOrNodeRef {
    pub id: Option<f64>,
    #[napi(ts_type = "string | string[]")]
    pub labels: Option<serde_json::Value>,
    pub label: Option<String>,
    pub key: Option<String>,
    pub local: Option<String>,
    pub from: Option<TxnNodeRef>,
    pub to: Option<TxnNodeRef>,
}

#[napi(object)]
pub struct TxnNodeView {
    pub id: Option<f64>,
    pub local: Option<String>,
    pub labels: Vec<String>,
    pub key: String,
    pub props: HashMap<String, serde_json::Value>,
    pub created_at: Option<i64>,
    pub updated_at: Option<i64>,
    pub weight: f64,
    pub dense_vector: Option<Vec<f64>>,
    pub sparse_vector: Option<Vec<SparseEntry>>,
}

#[napi(object)]
pub struct TxnEdgeView {
    pub id: Option<f64>,
    pub local: Option<String>,
    pub from: TxnNodeRef,
    pub to: TxnNodeRef,
    pub label: String,
    pub props: HashMap<String, serde_json::Value>,
    pub created_at: Option<i64>,
    pub updated_at: Option<i64>,
    pub weight: f64,
    pub valid_from: Option<i64>,
    pub valid_to: Option<i64>,
}

#[napi(object)]
pub struct TxnCommitResult {
    pub node_ids: Float64Array,
    pub edge_ids: Float64Array,
    pub node_aliases: HashMap<String, f64>,
    pub edge_aliases: HashMap<String, f64>,
}

#[napi(object)]
pub struct VectorSearchScope {
    pub start_node_id: f64,
    pub max_depth: u32,
    pub direction: Option<String>,
    pub edge_label_filter: Option<Vec<String>>,
    pub at_epoch: Option<i64>,
}

#[napi(object)]
pub struct VectorHit {
    pub node_id: f64,
    pub score: f64,
}

#[napi(object)]
pub struct EdgeInput {
    pub from: f64,
    pub to: f64,
    pub label: String,
    pub props: Option<HashMap<String, serde_json::Value>>,
    pub weight: Option<f64>,
    pub valid_from: Option<i64>,
    pub valid_to: Option<i64>,
}

impl TryFrom<EdgeInput> for CoreEdgeInput {
    type Error = napi::Error;
    fn try_from(js: EdgeInput) -> std::result::Result<Self, Self::Error> {
        Ok(CoreEdgeInput {
            from: f64_to_u64(js.from)?,
            to: f64_to_u64(js.to)?,
            label: js.label,
            props: convert_js_props(js.props),
            weight: js.weight.unwrap_or(1.0) as f32,
            valid_from: js.valid_from,
            valid_to: js.valid_to,
        })
    }
}

/// Node view: eager primitives, lazy props. Props are Arc-shared so
/// container getters (page results, subgraph) avoid cloning the BTreeMap.
#[napi]
pub struct NodeView {
    id_val: f64,
    labels_val: Vec<String>,
    key_val: String,
    created_at_val: i64,
    updated_at_val: i64,
    weight_val: f64,
    dense_vector_val: Option<Vec<f64>>,
    sparse_vector_val: Option<Vec<SparseEntry>>,
    props_raw: Arc<BTreeMap<String, PropValue>>,
}

#[napi]
impl NodeView {
    #[napi(getter)]
    pub fn id(&self) -> f64 {
        self.id_val
    }
    #[napi(getter)]
    pub fn labels(&self) -> Vec<String> {
        self.labels_val.clone()
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
    #[napi(getter)]
    pub fn dense_vector(&self) -> Option<Vec<f64>> {
        self.dense_vector_val.clone()
    }
    #[napi(getter)]
    pub fn sparse_vector(&self) -> Option<Vec<SparseEntry>> {
        self.sparse_vector_val.clone()
    }
}

impl TryFrom<CoreNodeView> for NodeView {
    type Error = napi::Error;
    fn try_from(n: CoreNodeView) -> Result<Self> {
        Ok(NodeView {
            id_val: u64_to_f64(n.id)?,
            labels_val: n.labels,
            key_val: n.key,
            created_at_val: n.created_at,
            updated_at_val: n.updated_at,
            weight_val: n.weight as f64,
            dense_vector_val: n
                .dense_vector
                .map(|values| values.into_iter().map(|v| v as f64).collect()),
            sparse_vector_val: n.sparse_vector.map(|entries| {
                entries
                    .into_iter()
                    .map(|(dimension, value)| SparseEntry {
                        dimension,
                        value: value as f64,
                    })
                    .collect()
            }),
            props_raw: Arc::new(n.props),
        })
    }
}

/// Edge view: eager primitives, lazy props. Props are Arc-shared so
/// container getters (page results, subgraph) avoid cloning the BTreeMap.
#[napi]
pub struct EdgeView {
    id_val: f64,
    from_val: f64,
    to_val: f64,
    label_val: String,
    created_at_val: i64,
    updated_at_val: i64,
    weight_val: f64,
    valid_from_val: i64,
    valid_to_val: i64,
    props_raw: Arc<BTreeMap<String, PropValue>>,
}

#[napi]
impl EdgeView {
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
    pub fn label(&self) -> String {
        self.label_val.clone()
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

impl TryFrom<CoreEdgeView> for EdgeView {
    type Error = napi::Error;
    fn try_from(e: CoreEdgeView) -> Result<Self> {
        Ok(EdgeView {
            id_val: u64_to_f64(e.id)?,
            from_val: u64_to_f64(e.from)?,
            to_val: u64_to_f64(e.to)?,
            label_val: e.label,
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
pub struct NeighborEntry {
    pub node_id: f64,
    pub edge_id: f64,
    pub label: String,
    pub weight: f64,
    pub valid_from: i64,
    pub valid_to: i64,
}

fn neighbor_to_js_entry(e: &CoreNeighborEntry) -> Result<NeighborEntry> {
    Ok(NeighborEntry {
        node_id: u64_to_f64(e.node_id)?,
        edge_id: u64_to_f64(e.edge_id)?,
        label: e.label.clone(),
        weight: e.weight as f64,
        valid_from: e.valid_from,
        valid_to: e.valid_to,
    })
}

#[napi(object)]
pub struct NeighborBatchEntry {
    pub query_node_id: f64,
    pub neighbors: Vec<NeighborEntry>,
}

#[napi(object)]
pub struct DegreeBatchEntry {
    pub node_id: f64,
    pub degree: i64,
}

#[napi(object)]
pub struct ComponentEntry {
    pub node_id: f64,
    pub component_id: f64,
}

#[napi(object)]
pub struct ShortestPath {
    pub nodes: Vec<f64>,
    pub edges: Vec<f64>,
    pub total_cost: f64,
}

fn shortest_path_to_js(sp: CoreShortestPath) -> Result<ShortestPath> {
    Ok(ShortestPath {
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
pub struct TraversalHit {
    pub node_id: f64,
    pub depth: u32,
    pub via_edge_id: Option<f64>,
    pub score: Option<f64>,
}

#[napi(object)]
pub struct TraversalCursor {
    pub depth: u32,
    pub last_node_id: f64,
}

#[napi(object)]
pub struct TraversalPageResult {
    pub items: Vec<TraversalHit>,
    pub next_cursor: Option<TraversalCursor>,
}

fn traversal_hit_to_js(hit: CoreTraversalHit) -> Result<TraversalHit> {
    Ok(TraversalHit {
        node_id: u64_to_f64(hit.node_id)?,
        depth: hit.depth,
        via_edge_id: hit.via_edge_id.map(u64_to_f64).transpose()?,
        score: hit.score,
    })
}

fn traversal_cursor_to_js(cursor: CoreTraversalCursor) -> Result<TraversalCursor> {
    Ok(TraversalCursor {
        depth: cursor.depth,
        last_node_id: u64_to_f64(cursor.last_node_id)?,
    })
}

fn js_traversal_cursor_to_rust(cursor: TraversalCursor) -> Result<CoreTraversalCursor> {
    Ok(CoreTraversalCursor {
        depth: cursor.depth,
        last_node_id: f64_to_u64(cursor.last_node_id)?,
    })
}

fn traversal_page_to_js(page: CoreTraversalPageResult) -> Result<TraversalPageResult> {
    Ok(TraversalPageResult {
        items: page
            .items
            .into_iter()
            .map(traversal_hit_to_js)
            .collect::<Result<Vec<_>>>()?,
        next_cursor: page.next_cursor.map(traversal_cursor_to_js).transpose()?,
    })
}

#[napi]
pub struct SubgraphResult {
    nodes_vec: Vec<NodeView>,
    edges_vec: Vec<EdgeView>,
}

#[napi]
impl SubgraphResult {
    #[napi(getter)]
    pub fn nodes(&self) -> Vec<NodeView> {
        self.nodes_vec
            .iter()
            .map(|n| NodeView {
                id_val: n.id_val,
                labels_val: n.labels_val.clone(),
                key_val: n.key_val.clone(),
                created_at_val: n.created_at_val,
                updated_at_val: n.updated_at_val,
                weight_val: n.weight_val,
                dense_vector_val: n.dense_vector_val.clone(),
                sparse_vector_val: n.sparse_vector_val.clone(),
                props_raw: Arc::clone(&n.props_raw),
            })
            .collect()
    }
    #[napi(getter)]
    pub fn edges(&self) -> Vec<EdgeView> {
        self.edges_vec
            .iter()
            .map(|e| EdgeView {
                id_val: e.id_val,
                from_val: e.from_val,
                to_val: e.to_val,
                label_val: e.label_val.clone(),
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

fn subgraph_to_js(sg: Subgraph) -> Result<SubgraphResult> {
    Ok(SubgraphResult {
        nodes_vec: sg
            .nodes
            .into_iter()
            .map(NodeView::try_from)
            .collect::<Result<Vec<_>>>()?,
        edges_vec: sg
            .edges
            .into_iter()
            .map(EdgeView::try_from)
            .collect::<Result<Vec<_>>>()?,
    })
}

// --- Pagination result types ---

#[napi(object)]
pub struct IdPageResult {
    pub items: Float64Array,
    pub next_cursor: Option<f64>,
}

#[napi]
pub struct NodePageResult {
    items_vec: Vec<NodeView>,
    cursor: Option<u64>,
}

#[napi]
impl NodePageResult {
    #[napi(getter)]
    pub fn items(&self) -> Vec<NodeView> {
        self.items_vec
            .iter()
            .map(|n| NodeView {
                id_val: n.id_val,
                labels_val: n.labels_val.clone(),
                key_val: n.key_val.clone(),
                created_at_val: n.created_at_val,
                updated_at_val: n.updated_at_val,
                weight_val: n.weight_val,
                dense_vector_val: n.dense_vector_val.clone(),
                sparse_vector_val: n.sparse_vector_val.clone(),
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
pub struct EdgePageResult {
    items_vec: Vec<EdgeView>,
    cursor: Option<u64>,
}

#[napi]
impl EdgePageResult {
    #[napi(getter)]
    pub fn items(&self) -> Vec<EdgeView> {
        self.items_vec
            .iter()
            .map(|e| EdgeView {
                id_val: e.id_val,
                from_val: e.from_val,
                to_val: e.to_val,
                label_val: e.label_val.clone(),
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
pub struct NeighborPageResult {
    items_vec: Vec<NeighborEntry>,
    cursor: Option<u64>,
}

#[napi]
impl NeighborPageResult {
    #[napi(getter)]
    pub fn items(&self) -> Vec<NeighborEntry> {
        self.items_vec.clone()
    }

    #[napi(getter)]
    pub fn next_cursor(&self) -> Result<Option<f64>> {
        self.cursor.map(u64_to_f64).transpose()
    }
}

fn id_page_to_js(page: PageResult<u64>) -> Result<IdPageResult> {
    Ok(IdPageResult {
        items: ids_to_float64_array(&page.items)?,
        next_cursor: page.next_cursor.map(u64_to_f64).transpose()?,
    })
}

fn node_page_to_js(page: PageResult<CoreNodeView>) -> Result<NodePageResult> {
    Ok(NodePageResult {
        items_vec: page
            .items
            .into_iter()
            .map(NodeView::try_from)
            .collect::<Result<Vec<_>>>()?,
        cursor: page.next_cursor,
    })
}

fn edge_page_to_js(page: PageResult<CoreEdgeView>) -> Result<EdgePageResult> {
    Ok(EdgePageResult {
        items_vec: page
            .items
            .into_iter()
            .map(EdgeView::try_from)
            .collect::<Result<Vec<_>>>()?,
        cursor: page.next_cursor,
    })
}

fn neighbor_page_to_js(page: PageResult<CoreNeighborEntry>) -> Result<NeighborPageResult> {
    Ok(NeighborPageResult {
        items_vec: neighbor_entries_to_js(page.items)?,
        cursor: page.next_cursor,
    })
}

impl TypeName for NodeSchemaInfoPayload {
    fn type_name() -> &'static str {
        "Object"
    }

    fn value_type() -> napi::ValueType {
        napi::ValueType::Object
    }
}

impl ToNapiValue for NodeSchemaInfoPayload {
    unsafe fn to_napi_value(env: napi::sys::napi_env, val: Self) -> Result<napi::sys::napi_value> {
        let env = Env::from_raw(env);
        let object = node_schema_info_to_js_object(&env, val.0)?;
        unsafe { <&Object<'_> as ToNapiValue>::to_napi_value(env.raw(), &object) }
    }
}

impl TypeName for EdgeSchemaInfoPayload {
    fn type_name() -> &'static str {
        "Object"
    }

    fn value_type() -> napi::ValueType {
        napi::ValueType::Object
    }
}

impl ToNapiValue for EdgeSchemaInfoPayload {
    unsafe fn to_napi_value(env: napi::sys::napi_env, val: Self) -> Result<napi::sys::napi_value> {
        let env = Env::from_raw(env);
        let object = edge_schema_info_to_js_object(&env, val.0)?;
        unsafe { <&Object<'_> as ToNapiValue>::to_napi_value(env.raw(), &object) }
    }
}

impl TypeName for SchemaValidationReportPayload {
    fn type_name() -> &'static str {
        "Object"
    }

    fn value_type() -> napi::ValueType {
        napi::ValueType::Object
    }
}

impl ToNapiValue for SchemaValidationReportPayload {
    unsafe fn to_napi_value(env: napi::sys::napi_env, val: Self) -> Result<napi::sys::napi_value> {
        let env = Env::from_raw(env);
        let object = schema_validation_report_to_js_object(&env, val.0)?;
        unsafe { <&Object<'_> as ToNapiValue>::to_napi_value(env.raw(), &object) }
    }
}

impl TypeName for GraphSchemaCheckReportPayload {
    fn type_name() -> &'static str {
        "Object"
    }

    fn value_type() -> napi::ValueType {
        napi::ValueType::Object
    }
}

impl ToNapiValue for GraphSchemaCheckReportPayload {
    unsafe fn to_napi_value(env: napi::sys::napi_env, val: Self) -> Result<napi::sys::napi_value> {
        let env = Env::from_raw(env);
        let object = graph_schema_check_report_to_js_object(&env, val.0)?;
        unsafe { <&Object<'_> as ToNapiValue>::to_napi_value(env.raw(), &object) }
    }
}

impl TypeName for GraphSchemaPublishResultPayload {
    fn type_name() -> &'static str {
        "Object"
    }

    fn value_type() -> napi::ValueType {
        napi::ValueType::Object
    }
}

impl ToNapiValue for GraphSchemaPublishResultPayload {
    unsafe fn to_napi_value(env: napi::sys::napi_env, val: Self) -> Result<napi::sys::napi_value> {
        let env = Env::from_raw(env);
        let object = graph_schema_publish_result_to_js_object(&env, val.0)?;
        unsafe { <&Object<'_> as ToNapiValue>::to_napi_value(env.raw(), &object) }
    }
}

impl TypeName for SchemaLiteralPayload {
    fn type_name() -> &'static str {
        "any"
    }

    fn value_type() -> napi::ValueType {
        napi::ValueType::Unknown
    }
}

impl ToNapiValue for SchemaLiteralPayload {
    unsafe fn to_napi_value(env: napi::sys::napi_env, val: Self) -> Result<napi::sys::napi_value> {
        unsafe { schema_literal_to_napi(env, val.0) }
    }
}

fn node_schema_info_to_js_object<'env>(
    env: &'env Env,
    info: CoreNodeSchemaInfo,
) -> Result<Object<'env>> {
    let mut object = Object::new(env)?;
    object.set("label", info.label)?;
    object.set("schema", node_schema_to_js_object(env, info.schema)?)?;
    Ok(object)
}

fn edge_schema_info_to_js_object<'env>(
    env: &'env Env,
    info: CoreEdgeSchemaInfo,
) -> Result<Object<'env>> {
    let mut object = Object::new(env)?;
    object.set("label", info.label)?;
    object.set("schema", edge_schema_to_js_object(env, info.schema)?)?;
    Ok(object)
}

fn node_schema_to_js_object<'env>(env: &'env Env, schema: CoreNodeSchema) -> Result<Object<'env>> {
    let mut object = Object::new(env)?;
    object.set(
        "additionalProperties",
        schema_additional_properties_to_js(schema.additional_properties),
    )?;
    object.set(
        "properties",
        property_schema_map_to_js_object(env, schema.properties)?,
    )?;
    if let Some(key) = schema.key {
        object.set("key", string_field_schema_to_js_object(env, key)?)?;
    }
    if let Some(label_constraints) = schema.label_constraints {
        object.set(
            "labelConstraints",
            node_label_constraints_to_js_object(env, label_constraints)?,
        )?;
    }
    if let Some(weight) = schema.weight {
        object.set("weight", numeric_field_schema_to_js_object(env, weight)?)?;
    }
    if let Some(dense_vector) = schema.dense_vector {
        object.set(
            "denseVector",
            dense_vector_schema_to_js_object(env, dense_vector)?,
        )?;
    }
    if let Some(sparse_vector) = schema.sparse_vector {
        object.set(
            "sparseVector",
            sparse_vector_schema_to_js_object(env, sparse_vector)?,
        )?;
    }
    Ok(object)
}

fn edge_schema_to_js_object<'env>(env: &'env Env, schema: CoreEdgeSchema) -> Result<Object<'env>> {
    let mut object = Object::new(env)?;
    object.set(
        "additionalProperties",
        schema_additional_properties_to_js(schema.additional_properties),
    )?;
    object.set(
        "properties",
        property_schema_map_to_js_object(env, schema.properties)?,
    )?;
    if let Some(from) = schema.from {
        object.set("from", endpoint_label_schema_to_js_object(env, from)?)?;
    }
    if let Some(to) = schema.to {
        object.set("to", endpoint_label_schema_to_js_object(env, to)?)?;
    }
    object.set("allowSelfLoops", schema.allow_self_loops)?;
    if let Some(weight) = schema.weight {
        object.set("weight", numeric_field_schema_to_js_object(env, weight)?)?;
    }
    if let Some(validity) = schema.validity {
        object.set(
            "validity",
            edge_validity_schema_to_js_object(env, validity)?,
        )?;
    }
    Ok(object)
}

fn property_schema_map_to_js_object<'env>(
    env: &'env Env,
    properties: BTreeMap<String, CorePropertySchema>,
) -> Result<Object<'env>> {
    let mut object = Object::new(env)?;
    for (key, schema) in properties {
        object.set(&key, property_schema_to_js_object(env, schema)?)?;
    }
    Ok(object)
}

fn property_schema_to_js_object<'env>(
    env: &'env Env,
    schema: CorePropertySchema,
) -> Result<Object<'env>> {
    let mut object = Object::new(env)?;
    object.set("required", schema.required)?;
    object.set("nullable", schema.nullable)?;
    object.set(
        "types",
        schema
            .types
            .into_iter()
            .map(schema_value_type_to_js)
            .collect::<Vec<_>>(),
    )?;
    if let Some(bound) = schema.numeric_min {
        object.set("numericMin", schema_numeric_bound_to_js_object(env, bound)?)?;
    }
    if let Some(bound) = schema.numeric_max {
        object.set("numericMax", schema_numeric_bound_to_js_object(env, bound)?)?;
    }
    if let Some(value) = schema.string_min_bytes {
        object.set("stringMinBytes", value as f64)?;
    }
    if let Some(value) = schema.string_max_bytes {
        object.set("stringMaxBytes", value as f64)?;
    }
    if let Some(value) = schema.bytes_min_len {
        object.set("bytesMinLen", value as f64)?;
    }
    if let Some(value) = schema.bytes_max_len {
        object.set("bytesMaxLen", value as f64)?;
    }
    if let Some(value) = schema.array_min_items {
        object.set("arrayMinItems", value as f64)?;
    }
    if let Some(value) = schema.array_max_items {
        object.set("arrayMaxItems", value as f64)?;
    }
    if let Some(value) = schema.map_min_entries {
        object.set("mapMinEntries", value as f64)?;
    }
    if let Some(value) = schema.map_max_entries {
        object.set("mapMaxEntries", value as f64)?;
    }
    let mut enum_values = env.create_array(schema.enum_values.len() as u32)?;
    for (index, value) in schema.enum_values.into_iter().enumerate() {
        enum_values.set(index as u32, SchemaLiteralPayload(value))?;
    }
    object.set("enumValues", enum_values)?;
    Ok(object)
}

fn schema_numeric_bound_to_js_object<'env>(
    env: &'env Env,
    bound: CoreSchemaNumericBound,
) -> Result<Object<'env>> {
    let mut object = Object::new(env)?;
    object.set("value", SchemaLiteralPayload(bound.value))?;
    object.set("inclusive", bound.inclusive)?;
    Ok(object)
}

fn string_field_schema_to_js_object<'env>(
    env: &'env Env,
    schema: CoreStringFieldSchema,
) -> Result<Object<'env>> {
    let mut object = Object::new(env)?;
    if let Some(value) = schema.min_bytes {
        object.set("minBytes", value as f64)?;
    }
    if let Some(value) = schema.max_bytes {
        object.set("maxBytes", value as f64)?;
    }
    object.set("enumValues", schema.enum_values)?;
    Ok(object)
}

fn numeric_field_schema_to_js_object<'env>(
    env: &'env Env,
    schema: CoreNumericFieldSchema,
) -> Result<Object<'env>> {
    let mut object = Object::new(env)?;
    if let Some(bound) = schema.min {
        object.set("min", schema_numeric_bound_to_js_object(env, bound)?)?;
    }
    if let Some(bound) = schema.max {
        object.set("max", schema_numeric_bound_to_js_object(env, bound)?)?;
    }
    object.set("finite", schema.finite)?;
    Ok(object)
}

fn node_label_constraints_to_js_object<'env>(
    env: &'env Env,
    schema: CoreNodeLabelConstraintSchema,
) -> Result<Object<'env>> {
    let mut object = Object::new(env)?;
    object.set("allOf", schema.all_of)?;
    object.set("anyOf", schema.any_of)?;
    object.set("noneOf", schema.none_of)?;
    Ok(object)
}

fn endpoint_label_schema_to_js_object<'env>(
    env: &'env Env,
    schema: CoreEndpointLabelSchema,
) -> Result<Object<'env>> {
    let mut object = Object::new(env)?;
    object.set("allOf", schema.all_of)?;
    object.set("anyOf", schema.any_of)?;
    object.set("noneOf", schema.none_of)?;
    Ok(object)
}

fn dense_vector_schema_to_js_object<'env>(
    env: &'env Env,
    schema: CoreDenseVectorSchema,
) -> Result<Object<'env>> {
    let mut object = Object::new(env)?;
    object.set("presence", schema_vector_presence_to_js(schema.presence))?;
    if let Some(value) = schema.dimension {
        object.set("dimension", value as f64)?;
    }
    Ok(object)
}

fn sparse_vector_schema_to_js_object<'env>(
    env: &'env Env,
    schema: CoreSparseVectorSchema,
) -> Result<Object<'env>> {
    let mut object = Object::new(env)?;
    object.set("presence", schema_vector_presence_to_js(schema.presence))?;
    if let Some(value) = schema.min_entries {
        object.set("minEntries", value as f64)?;
    }
    if let Some(value) = schema.max_entries {
        object.set("maxEntries", value as f64)?;
    }
    if let Some(value) = schema.max_dimension_id {
        object.set("maxDimensionId", value)?;
    }
    Ok(object)
}

fn edge_validity_schema_to_js_object<'env>(
    env: &'env Env,
    schema: CoreEdgeValiditySchema,
) -> Result<Object<'env>> {
    let mut object = Object::new(env)?;
    object.set(
        "requireValidFromBeforeValidTo",
        schema.require_valid_from_before_valid_to,
    )?;
    if let Some(value) = schema.valid_from_min {
        object.set("validFromMin", value)?;
    }
    if let Some(value) = schema.valid_from_max {
        object.set("validFromMax", value)?;
    }
    if let Some(value) = schema.valid_to_min {
        object.set("validToMin", value)?;
    }
    if let Some(value) = schema.valid_to_max {
        object.set("validToMax", value)?;
    }
    object.set("allowOpenEndedValidTo", schema.allow_open_ended_valid_to)?;
    Ok(object)
}

fn schema_validation_report_to_js_object<'env>(
    env: &'env Env,
    report: CoreSchemaValidationReport,
) -> Result<Object<'env>> {
    let mut object = Object::new(env)?;
    object.set("checkedRecords", report.checked_records as f64)?;
    object.set("violationCount", report.violation_count as f64)?;
    let mut violations = env.create_array(report.violations.len() as u32)?;
    for (index, violation) in report.violations.into_iter().enumerate() {
        violations.set(index as u32, schema_violation_to_js_object(env, violation)?)?;
    }
    object.set("violations", violations)?;
    object.set("truncated", report.truncated)?;
    object.set("scanLimitHit", report.scan_limit_hit)?;
    Ok(object)
}

fn graph_schema_check_report_to_js_object<'env>(
    env: &'env Env,
    report: CoreGraphSchemaCheckReport,
) -> Result<Object<'env>> {
    let mut object = Object::new(env)?;
    object.set(
        "operation",
        graph_schema_operation_kind_to_js(report.operation),
    )?;
    let mut entries = env.create_array(report.entries.len() as u32)?;
    for (index, entry) in report.entries.into_iter().enumerate() {
        entries.set(
            index as u32,
            graph_schema_validation_entry_to_js_object(env, entry)?,
        )?;
    }
    object.set("entries", entries)?;
    object.set("checkedRecords", report.checked_records as f64)?;
    object.set("violationCount", report.violation_count as f64)?;
    object.set("truncated", report.truncated)?;
    object.set("scanLimitHit", report.scan_limit_hit)?;
    Ok(object)
}

fn graph_schema_validation_entry_to_js_object<'env>(
    env: &'env Env,
    entry: CoreGraphSchemaValidationReportEntry,
) -> Result<Object<'env>> {
    let mut object = Object::new(env)?;
    object.set("targetKind", schema_target_kind_to_js(entry.target_kind))?;
    object.set("label", entry.label)?;
    object.set(
        "report",
        schema_validation_report_to_js_object(env, entry.report)?,
    )?;
    Ok(object)
}

fn graph_schema_publish_result_to_js_object<'env>(
    env: &'env Env,
    result: CoreGraphSchemaPublishResult,
) -> Result<Object<'env>> {
    let mut object = Object::new(env)?;
    object.set(
        "operation",
        graph_schema_operation_kind_to_js(result.operation),
    )?;

    let mut node_schemas = env.create_array(result.node_schemas.len() as u32)?;
    for (index, info) in result.node_schemas.into_iter().enumerate() {
        node_schemas.set(index as u32, node_schema_info_to_js_object(env, info)?)?;
    }
    object.set("nodeSchemas", node_schemas)?;

    let mut edge_schemas = env.create_array(result.edge_schemas.len() as u32)?;
    for (index, info) in result.edge_schemas.into_iter().enumerate() {
        edge_schemas.set(index as u32, edge_schema_info_to_js_object(env, info)?)?;
    }
    object.set("edgeSchemas", edge_schemas)?;

    object.set(
        "validation",
        graph_schema_check_report_to_js_object(env, result.validation)?,
    )?;
    object.set("targetsPublished", result.targets_published as f64)?;
    object.set("targetsDropped", result.targets_dropped as f64)?;

    let mut drop_targets = env.create_array(result.drop_targets.len() as u32)?;
    for (index, target) in result.drop_targets.into_iter().enumerate() {
        drop_targets.set(
            index as u32,
            graph_schema_drop_target_to_js_object(env, target)?,
        )?;
    }
    object.set("dropTargets", drop_targets)?;
    object.set("nodeSchemasDropped", result.node_schemas_dropped as f64)?;
    object.set("edgeSchemasDropped", result.edge_schemas_dropped as f64)?;
    Ok(object)
}

fn graph_schema_drop_target_to_js_object<'env>(
    env: &'env Env,
    target: CoreGraphSchemaDropTargetResult,
) -> Result<Object<'env>> {
    let mut object = Object::new(env)?;
    object.set("targetKind", schema_target_kind_to_js(target.target_kind))?;
    object.set("label", target.label)?;
    object.set("action", graph_schema_drop_action_to_js(target.action))?;
    Ok(object)
}

fn schema_violation_to_js_object<'env>(
    env: &'env Env,
    violation: CoreSchemaViolation,
) -> Result<Object<'env>> {
    let mut object = Object::new(env)?;
    object.set(
        "target",
        schema_violation_target_to_js_object(env, violation.target)?,
    )?;
    object.set("path", violation.path)?;
    object.set("message", violation.message)?;
    Ok(object)
}

fn schema_violation_target_to_js_object<'env>(
    env: &'env Env,
    target: CoreSchemaViolationTarget,
) -> Result<Object<'env>> {
    let mut object = Object::new(env)?;
    match target {
        CoreSchemaViolationTarget::Node { id, labels, key } => {
            object.set("kind", "node")?;
            object.set("id", u64_to_f64(id)?)?;
            object.set("labels", labels)?;
            object.set("key", key)?;
        }
        CoreSchemaViolationTarget::Edge {
            id,
            label,
            from,
            to,
        } => {
            object.set("kind", "edge")?;
            object.set("id", u64_to_f64(id)?)?;
            object.set("label", label)?;
            object.set("from", u64_to_f64(from)?)?;
            object.set("to", u64_to_f64(to)?)?;
        }
    }
    Ok(object)
}

unsafe fn schema_literal_to_napi(
    env: napi::sys::napi_env,
    value: PropValue,
) -> Result<napi::sys::napi_value> {
    match value {
        PropValue::Null => unsafe {
            <Option<serde_json::Value> as ToNapiValue>::to_napi_value(env, None)
        },
        PropValue::Bool(value) => unsafe { bool::to_napi_value(env, value) },
        PropValue::Int(value) => unsafe { i64::to_napi_value(env, value) },
        PropValue::UInt(value) => {
            let env_ref = Env::from_raw(env);
            let mut object = Object::new(&env_ref)?;
            object.set("type", "uint")?;
            object.set("value", value.to_string())?;
            unsafe { <&Object<'_> as ToNapiValue>::to_napi_value(env_ref.raw(), &object) }
        }
        PropValue::Float(value) => unsafe { f64::to_napi_value(env, value) },
        PropValue::String(value) => unsafe { String::to_napi_value(env, value) },
        PropValue::Bytes(value) => {
            let env_ref = Env::from_raw(env);
            let mut object = Object::new(&env_ref)?;
            object.set("type", "bytes")?;
            object.set("value", Buffer::from(value))?;
            unsafe { <&Object<'_> as ToNapiValue>::to_napi_value(env_ref.raw(), &object) }
        }
        PropValue::Array(values) => {
            let env_ref = Env::from_raw(env);
            let mut array = env_ref.create_array(values.len() as u32)?;
            for (index, value) in values.into_iter().enumerate() {
                array.set(index as u32, SchemaLiteralPayload(value))?;
            }
            unsafe { <Array<'_> as ToNapiValue>::to_napi_value(env_ref.raw(), array) }
        }
        PropValue::Map(values) => {
            let env_ref = Env::from_raw(env);
            if schema_map_literal_needs_escape(&values) {
                let mut object = Object::new(&env_ref)?;
                object.set("type", "map")?;
                object.set("value", schema_map_to_js_object(&env_ref, values)?)?;
                unsafe { <&Object<'_> as ToNapiValue>::to_napi_value(env_ref.raw(), &object) }
            } else {
                let object = schema_map_to_js_object(&env_ref, values)?;
                unsafe { <&Object<'_> as ToNapiValue>::to_napi_value(env_ref.raw(), &object) }
            }
        }
    }
}

fn schema_map_to_js_object<'env>(
    env: &'env Env,
    values: BTreeMap<String, PropValue>,
) -> Result<Object<'env>> {
    let mut object = Object::new(env)?;
    for (key, value) in values {
        object.set(&key, SchemaLiteralPayload(value))?;
    }
    Ok(object)
}

fn schema_map_literal_needs_escape(values: &BTreeMap<String, PropValue>) -> bool {
    if values.len() != 2 || !values.contains_key("value") {
        return false;
    }
    matches!(
        values.get("type"),
        Some(PropValue::String(marker))
            if matches!(marker.as_str(), "uint" | "bytes" | "map")
    )
}

fn schema_target_kind_to_js(kind: CoreSchemaTargetKind) -> &'static str {
    match kind {
        CoreSchemaTargetKind::Node => "node",
        CoreSchemaTargetKind::Edge => "edge",
    }
}

fn graph_schema_operation_kind_to_js(kind: CoreGraphSchemaOperationKind) -> &'static str {
    match kind {
        CoreGraphSchemaOperationKind::Add => "add",
        CoreGraphSchemaOperationKind::Set => "set",
        CoreGraphSchemaOperationKind::Drop => "drop",
        CoreGraphSchemaOperationKind::DropAll => "dropAll",
        CoreGraphSchemaOperationKind::CheckAdd => "checkAdd",
        CoreGraphSchemaOperationKind::CheckSet => "checkSet",
    }
}

fn graph_schema_drop_action_to_js(action: CoreGraphSchemaDropAction) -> &'static str {
    match action {
        CoreGraphSchemaDropAction::Dropped => "dropped",
        CoreGraphSchemaDropAction::NotFound => "notFound",
    }
}

fn schema_additional_properties_to_js(value: CoreSchemaAdditionalProperties) -> &'static str {
    match value {
        CoreSchemaAdditionalProperties::Allow => "allow",
        CoreSchemaAdditionalProperties::Reject => "reject",
    }
}

fn schema_value_type_to_js(value: CoreSchemaValueType) -> &'static str {
    match value {
        CoreSchemaValueType::Bool => "bool",
        CoreSchemaValueType::Int => "int",
        CoreSchemaValueType::UInt => "uint",
        CoreSchemaValueType::Float => "float",
        CoreSchemaValueType::Number => "number",
        CoreSchemaValueType::String => "string",
        CoreSchemaValueType::Bytes => "bytes",
        CoreSchemaValueType::Array => "array",
        CoreSchemaValueType::Map => "map",
    }
}

fn schema_vector_presence_to_js(value: CoreSchemaVectorPresence) -> &'static str {
    match value {
        CoreSchemaVectorPresence::Optional => "optional",
        CoreSchemaVectorPresence::Required => "required",
        CoreSchemaVectorPresence::Forbidden => "forbidden",
    }
}

#[napi(object)]
pub struct PropertyRangePageResult {
    pub items: Float64Array,
    pub next_cursor: Option<PropertyRangeCursor>,
}

fn optional_string_to_json_null(value: Option<String>) -> serde_json::Value {
    value
        .map(serde_json::Value::String)
        .unwrap_or(serde_json::Value::Null)
}

fn node_property_index_info_to_js(
    info: CoreNodePropertyIndexInfo,
) -> Result<NodePropertyIndexInfo> {
    let kind = secondary_index_kind_to_js(&info.kind);
    Ok(NodePropertyIndexInfo {
        index_id: u64_to_f64(info.index_id)?,
        label: info.label,
        fields: info
            .fields
            .into_iter()
            .map(secondary_index_field_to_js)
            .collect(),
        kind,
        state: secondary_index_state_to_js(info.state).to_string(),
        last_error: optional_string_to_json_null(info.last_error),
        compound: info.compound,
    })
}

fn node_property_index_infos_to_js(
    infos: Vec<CoreNodePropertyIndexInfo>,
) -> Result<Vec<NodePropertyIndexInfo>> {
    infos
        .into_iter()
        .map(node_property_index_info_to_js)
        .collect()
}

fn edge_property_index_info_to_js(
    info: CoreEdgePropertyIndexInfo,
) -> Result<EdgePropertyIndexInfo> {
    let kind = secondary_index_kind_to_js(&info.kind);
    Ok(EdgePropertyIndexInfo {
        index_id: u64_to_f64(info.index_id)?,
        label: info.label,
        fields: info
            .fields
            .into_iter()
            .map(secondary_index_field_to_js)
            .collect(),
        kind,
        state: secondary_index_state_to_js(info.state).to_string(),
        last_error: optional_string_to_json_null(info.last_error),
        compound: info.compound,
    })
}

fn edge_property_index_infos_to_js(
    infos: Vec<CoreEdgePropertyIndexInfo>,
) -> Result<Vec<EdgePropertyIndexInfo>> {
    infos
        .into_iter()
        .map(edge_property_index_info_to_js)
        .collect()
}

fn property_range_cursor_to_js(cursor: CorePropertyRangeCursor) -> Result<PropertyRangeCursor> {
    let (value, domain) = prop_value_to_js_numeric_parts(&cursor.value)?;
    Ok(PropertyRangeCursor {
        value,
        node_id: u64_to_f64(cursor.node_id)?,
        domain,
    })
}

fn js_property_range_cursor_to_rust(
    cursor: PropertyRangeCursor,
) -> Result<CorePropertyRangeCursor> {
    let domain = parse_range_value_domain(cursor.domain.as_str())?;
    Ok(CorePropertyRangeCursor {
        value: js_numeric_to_prop_value(cursor.value, domain)?,
        node_id: f64_to_u64(cursor.node_id)?,
    })
}

fn property_range_page_to_js(
    page: CorePropertyRangePageResult<u64>,
) -> Result<PropertyRangePageResult> {
    Ok(PropertyRangePageResult {
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

fn query_node_ids_to_js(result: QueryNodeIdsResult) -> Result<IdPageResult> {
    Ok(IdPageResult {
        items: ids_to_float64_array(&result.items)?,
        next_cursor: result.next_cursor.map(u64_to_f64).transpose()?,
    })
}

fn query_edge_ids_to_js(result: QueryEdgeIdsResult) -> Result<IdPageResult> {
    Ok(IdPageResult {
        items: ids_to_float64_array(&result.edge_ids)?,
        next_cursor: result.next_cursor.map(u64_to_f64).transpose()?,
    })
}

fn query_nodes_to_js(result: QueryNodesResult) -> Result<NodePageResult> {
    Ok(NodePageResult {
        items_vec: result
            .items
            .into_iter()
            .map(NodeView::try_from)
            .collect::<Result<Vec<_>>>()?,
        cursor: result.next_cursor,
    })
}

fn query_edges_to_js(result: QueryEdgesResult) -> Result<EdgePageResult> {
    Ok(EdgePageResult {
        items_vec: result
            .edges
            .into_iter()
            .map(EdgeView::try_from)
            .collect::<Result<Vec<_>>>()?,
        cursor: result.next_cursor,
    })
}

fn gql_rows_to_js_array<'env>(
    env: &'env Env,
    columns: &[String],
    rows: Vec<GqlRow>,
    compact_rows: bool,
) -> Result<Array<'env>> {
    let mut array = env.create_array(rows.len() as u32)?;
    for (row_index, row) in rows.into_iter().enumerate() {
        if compact_rows {
            let mut values = env.create_array(row.values.len() as u32)?;
            for (value_index, value) in row.values.into_iter().enumerate() {
                values.set(value_index as u32, GqlJsValue(value))?;
            }
            array.set(row_index as u32, values)?;
        } else {
            let mut object = Object::new(env)?;
            for (column, value) in columns.iter().zip(row.values) {
                object.set(column, GqlJsValue(value))?;
            }
            array.set(row_index as u32, &object)?;
        }
    }
    Ok(array)
}

fn gql_value_to_napi(env: napi::sys::napi_env, value: GqlValue) -> Result<napi::sys::napi_value> {
    match value {
        GqlValue::Null => unsafe {
            <Option<serde_json::Value> as ToNapiValue>::to_napi_value(
                env,
                None::<serde_json::Value>,
            )
        },
        GqlValue::Bool(value) => unsafe { bool::to_napi_value(env, value) },
        GqlValue::Int(value) => unsafe { f64::to_napi_value(env, i64_to_safe_f64(value)?) },
        GqlValue::UInt(value) => unsafe { f64::to_napi_value(env, u64_to_f64(value)?) },
        GqlValue::Float(value) => unsafe { f64::to_napi_value(env, value) },
        GqlValue::String(value) => unsafe { String::to_napi_value(env, value) },
        GqlValue::Bytes(value) => unsafe { Buffer::to_napi_value(env, Buffer::from(value)) },
        GqlValue::List(values) => {
            let env_ref = Env::from_raw(env);
            let mut array = env_ref.create_array(values.len() as u32)?;
            for (index, value) in values.into_iter().enumerate() {
                array.set(index as u32, GqlJsValue(value))?;
            }
            unsafe { Array::to_napi_value(env, array) }
        }
        GqlValue::Map(values) => {
            let env_ref = Env::from_raw(env);
            let mut object = Object::new(&env_ref)?;
            for (key, value) in values {
                object.set(key, GqlJsValue(value))?;
            }
            unsafe { <&Object<'_> as ToNapiValue>::to_napi_value(env, &object) }
        }
        GqlValue::Node(node) => gql_node_to_napi(env, node),
        GqlValue::Edge(edge) => gql_edge_to_napi(env, edge),
        GqlValue::Path(path) => gql_path_to_napi(env, path),
    }
}

fn gql_node_to_napi(env: napi::sys::napi_env, node: GqlNode) -> Result<napi::sys::napi_value> {
    let env_ref = Env::from_raw(env);
    let mut object = Object::new(&env_ref)?;
    if let Some(id) = node.id {
        object.set("id", u64_to_f64(id)?)?;
    }
    if let Some(labels) = node.labels {
        object.set("labels", labels)?;
    }
    if let Some(key) = node.key {
        object.set("key", key)?;
    }
    if let Some(props) = node.props {
        object.set("props", GqlJsValue(GqlValue::Map(props)))?;
    }
    if let Some(weight) = node.weight {
        object.set("weight", weight as f64)?;
    }
    if let Some(created_at) = node.created_at {
        object.set("createdAt", created_at)?;
    }
    if let Some(updated_at) = node.updated_at {
        object.set("updatedAt", updated_at)?;
    }
    if let Some(dense_vector) = node.dense_vector {
        object.set(
            "denseVector",
            dense_vector
                .into_iter()
                .map(|value| value as f64)
                .collect::<Vec<_>>(),
        )?;
    }
    if let Some(sparse_vector) = node.sparse_vector {
        object.set(
            "sparseVector",
            sparse_vector
                .into_iter()
                .map(|(dimension, value)| SparseEntry {
                    dimension,
                    value: value as f64,
                })
                .collect::<Vec<_>>(),
        )?;
    }
    unsafe { <&Object<'_> as ToNapiValue>::to_napi_value(env, &object) }
}

fn gql_path_to_napi(env: napi::sys::napi_env, path: GqlPath) -> Result<napi::sys::napi_value> {
    let env_ref = Env::from_raw(env);
    let mut object = Object::new(&env_ref)?;
    object.set(
        "nodeIds",
        path.node_ids
            .into_iter()
            .map(u64_to_f64)
            .collect::<Result<Vec<_>>>()?,
    )?;
    object.set(
        "edgeIds",
        path.edge_ids
            .into_iter()
            .map(u64_to_f64)
            .collect::<Result<Vec<_>>>()?,
    )?;
    if let Some(nodes) = path.nodes {
        let mut array = env_ref.create_array(nodes.len() as u32)?;
        for (index, node) in nodes.into_iter().enumerate() {
            array.set(index as u32, GqlJsValue(GqlValue::Node(node)))?;
        }
        object.set("nodes", array)?;
    }
    if let Some(edges) = path.edges {
        let mut array = env_ref.create_array(edges.len() as u32)?;
        for (index, edge) in edges.into_iter().enumerate() {
            array.set(index as u32, GqlJsValue(GqlValue::Edge(edge)))?;
        }
        object.set("edges", array)?;
    }
    unsafe { <&Object<'_> as ToNapiValue>::to_napi_value(env, &object) }
}

fn graph_rows_to_js_array<'env>(
    env: &'env Env,
    columns: &[String],
    rows: Vec<GraphRow>,
    compact_rows: bool,
) -> Result<Array<'env>> {
    let mut array = env.create_array(rows.len() as u32)?;
    for (row_index, row) in rows.into_iter().enumerate() {
        if compact_rows {
            let mut values = env.create_array(row.values.len() as u32)?;
            for (value_index, value) in row.values.into_iter().enumerate() {
                values.set(value_index as u32, GraphJsValue(value))?;
            }
            array.set(row_index as u32, values)?;
        } else {
            let mut object = Object::new(env)?;
            for (column, value) in columns.iter().zip(row.values) {
                object.set(column, GraphJsValue(value))?;
            }
            array.set(row_index as u32, &object)?;
        }
    }
    Ok(array)
}

fn graph_value_to_napi(
    env: napi::sys::napi_env,
    value: GraphValue,
) -> Result<napi::sys::napi_value> {
    match value {
        GraphValue::Null => unsafe {
            <Option<serde_json::Value> as ToNapiValue>::to_napi_value(
                env,
                None::<serde_json::Value>,
            )
        },
        GraphValue::Bool(value) => unsafe { bool::to_napi_value(env, value) },
        GraphValue::Int(value) => unsafe { f64::to_napi_value(env, i64_to_safe_f64(value)?) },
        GraphValue::UInt(value) | GraphValue::NodeId(value) | GraphValue::EdgeId(value) => unsafe {
            f64::to_napi_value(env, u64_to_f64(value)?)
        },
        GraphValue::Float(value) => unsafe { f64::to_napi_value(env, value) },
        GraphValue::String(value) => unsafe { String::to_napi_value(env, value) },
        GraphValue::Bytes(value) => unsafe { Buffer::to_napi_value(env, Buffer::from(value)) },
        GraphValue::List(values) => {
            let env_ref = Env::from_raw(env);
            let mut array = env_ref.create_array(values.len() as u32)?;
            for (index, value) in values.into_iter().enumerate() {
                array.set(index as u32, GraphJsValue(value))?;
            }
            unsafe { Array::to_napi_value(env, array) }
        }
        GraphValue::Map(values) => {
            let env_ref = Env::from_raw(env);
            let mut object = Object::new(&env_ref)?;
            for (key, value) in values {
                object.set(key, GraphJsValue(value))?;
            }
            unsafe { <&Object<'_> as ToNapiValue>::to_napi_value(env, &object) }
        }
        GraphValue::Node(node) => graph_node_to_napi(env, node),
        GraphValue::Edge(edge) => graph_edge_to_napi(env, edge),
        GraphValue::Path(path) => graph_path_to_napi(env, path),
    }
}

fn graph_node_to_napi(
    env: napi::sys::napi_env,
    node: GraphNodeValue,
) -> Result<napi::sys::napi_value> {
    let env_ref = Env::from_raw(env);
    let mut object = Object::new(&env_ref)?;
    if let Some(id) = node.id {
        object.set("id", u64_to_f64(id)?)?;
    }
    if let Some(labels) = node.labels {
        object.set("labels", labels)?;
    }
    if let Some(key) = node.key {
        object.set("key", key)?;
    }
    if let Some(props) = node.props {
        object.set("props", GraphJsValue(GraphValue::Map(props)))?;
    }
    if let Some(weight) = node.weight {
        object.set("weight", weight as f64)?;
    }
    if let Some(created_at) = node.created_at {
        object.set("createdAt", created_at)?;
    }
    if let Some(updated_at) = node.updated_at {
        object.set("updatedAt", updated_at)?;
    }
    if let Some(dense_vector) = node.dense_vector {
        object.set(
            "denseVector",
            dense_vector
                .into_iter()
                .map(|value| value as f64)
                .collect::<Vec<_>>(),
        )?;
    }
    if let Some(sparse_vector) = node.sparse_vector {
        object.set(
            "sparseVector",
            sparse_vector
                .into_iter()
                .map(|(dimension, value)| SparseEntry {
                    dimension,
                    value: value as f64,
                })
                .collect::<Vec<_>>(),
        )?;
    }
    unsafe { <&Object<'_> as ToNapiValue>::to_napi_value(env, &object) }
}

fn graph_edge_to_napi(
    env: napi::sys::napi_env,
    edge: GraphEdgeValue,
) -> Result<napi::sys::napi_value> {
    let env_ref = Env::from_raw(env);
    let mut object = Object::new(&env_ref)?;
    if let Some(id) = edge.id {
        object.set("id", u64_to_f64(id)?)?;
    }
    if let Some(from) = edge.from {
        object.set("from", u64_to_f64(from)?)?;
    }
    if let Some(to) = edge.to {
        object.set("to", u64_to_f64(to)?)?;
    }
    if let Some(label) = edge.label {
        object.set("label", label)?;
    }
    if let Some(props) = edge.props {
        object.set("props", GraphJsValue(GraphValue::Map(props)))?;
    }
    if let Some(weight) = edge.weight {
        object.set("weight", weight as f64)?;
    }
    if let Some(created_at) = edge.created_at {
        object.set("createdAt", created_at)?;
    }
    if let Some(updated_at) = edge.updated_at {
        object.set("updatedAt", updated_at)?;
    }
    if let Some(valid_from) = edge.valid_from {
        object.set("validFrom", valid_from)?;
    }
    if let Some(valid_to) = edge.valid_to {
        object.set("validTo", valid_to)?;
    }
    unsafe { <&Object<'_> as ToNapiValue>::to_napi_value(env, &object) }
}

fn graph_path_to_napi(
    env: napi::sys::napi_env,
    path: GraphPathValue,
) -> Result<napi::sys::napi_value> {
    let env_ref = Env::from_raw(env);
    let mut object = Object::new(&env_ref)?;
    object.set(
        "nodeIds",
        path.node_ids
            .into_iter()
            .map(u64_to_f64)
            .collect::<Result<Vec<_>>>()?,
    )?;
    object.set(
        "edgeIds",
        path.edge_ids
            .into_iter()
            .map(u64_to_f64)
            .collect::<Result<Vec<_>>>()?,
    )?;
    if let Some(nodes) = path.nodes {
        let mut array = env_ref.create_array(nodes.len() as u32)?;
        for (index, node) in nodes.into_iter().enumerate() {
            array.set(index as u32, GraphJsValue(GraphValue::Node(node)))?;
        }
        object.set("nodes", array)?;
    }
    if let Some(edges) = path.edges {
        let mut array = env_ref.create_array(edges.len() as u32)?;
        for (index, edge) in edges.into_iter().enumerate() {
            array.set(index as u32, GraphJsValue(GraphValue::Edge(edge)))?;
        }
        object.set("edges", array)?;
    }
    unsafe { <&Object<'_> as ToNapiValue>::to_napi_value(env, &object) }
}

fn graph_stats_to_value(stats: GraphRowStats) -> GraphValue {
    GraphValue::Map(BTreeMap::from([
        (
            "rowsReturned".to_string(),
            GraphValue::UInt(stats.rows_returned as u64),
        ),
        (
            "rowsAfterFilter".to_string(),
            GraphValue::UInt(stats.rows_after_filter as u64),
        ),
        (
            "rowsSeenForPage".to_string(),
            GraphValue::UInt(stats.rows_seen_for_page as u64),
        ),
        (
            "intermediateBindingsPeak".to_string(),
            GraphValue::UInt(stats.intermediate_bindings_peak as u64),
        ),
        (
            "frontierPeak".to_string(),
            GraphValue::UInt(stats.frontier_peak as u64),
        ),
        (
            "pathsEnumerated".to_string(),
            GraphValue::UInt(stats.paths_enumerated as u64),
        ),
        ("dbHits".to_string(), GraphValue::UInt(stats.db_hits as u64)),
        (
            "elapsedUs".to_string(),
            stats
                .elapsed_us
                .map(GraphValue::UInt)
                .unwrap_or(GraphValue::Null),
        ),
        (
            "effectiveAtEpoch".to_string(),
            GraphValue::Int(stats.effective_at_epoch),
        ),
        (
            "warnings".to_string(),
            GraphValue::List(stats.warnings.into_iter().map(GraphValue::String).collect()),
        ),
    ]))
}

fn graph_pipeline_stats_to_value(stats: GraphPipelineStats) -> GraphValue {
    GraphValue::Map(BTreeMap::from([
        (
            "rowsReturned".to_string(),
            GraphValue::UInt(stats.rows_returned as u64),
        ),
        (
            "rowsEnteredPipeline".to_string(),
            GraphValue::UInt(stats.rows_entered_pipeline as u64),
        ),
        (
            "rowsAfterFilter".to_string(),
            GraphValue::UInt(stats.rows_after_filter as u64),
        ),
        (
            "intermediateRows".to_string(),
            GraphValue::UInt(stats.intermediate_rows as u64),
        ),
        (
            "pipelineRowsMaterialized".to_string(),
            GraphValue::UInt(stats.pipeline_rows_materialized as u64),
        ),
        ("groups".to_string(), GraphValue::UInt(stats.groups as u64)),
        (
            "collectItems".to_string(),
            GraphValue::UInt(stats.collect_items as u64),
        ),
        (
            "unionBranches".to_string(),
            GraphValue::UInt(stats.union_branches as u64),
        ),
        (
            "unionDedupKeys".to_string(),
            GraphValue::UInt(stats.union_dedup_keys as u64),
        ),
        (
            "subqueryInvocations".to_string(),
            GraphValue::UInt(stats.subquery_invocations as u64),
        ),
        (
            "subqueryCacheHits".to_string(),
            GraphValue::UInt(stats.subquery_cache_hits as u64),
        ),
        (
            "shortestPathPairs".to_string(),
            GraphValue::UInt(stats.shortest_path_pairs as u64),
        ),
        (
            "shortestPathCacheHits".to_string(),
            GraphValue::UInt(stats.shortest_path_cache_hits as u64),
        ),
        ("dbHits".to_string(), GraphValue::UInt(stats.db_hits as u64)),
        (
            "elapsedUs".to_string(),
            stats
                .elapsed_us
                .map(GraphValue::UInt)
                .unwrap_or(GraphValue::Null),
        ),
        (
            "effectiveAtEpoch".to_string(),
            GraphValue::Int(stats.effective_at_epoch),
        ),
        (
            "warnings".to_string(),
            GraphValue::List(stats.warnings.into_iter().map(GraphValue::String).collect()),
        ),
    ]))
}

fn graph_explain_to_json(explain: GraphRowExplain) -> Result<serde_json::Value> {
    Ok(serde_json::json!({
        "columns": explain.columns,
        "effectiveAtEpoch": explain.effective_at_epoch,
        "fingerprint": explain.fingerprint,
        "plan": explain.plan.into_iter().map(graph_explain_node_to_json).collect::<Vec<_>>(),
        "rowOps": explain.row_ops.into_iter().map(graph_row_op_to_json).collect::<Vec<_>>(),
        "order": graph_order_explain_to_json(explain.order),
        "cursor": graph_cursor_explain_to_json(explain.cursor),
        "projection": graph_projection_explain_to_json(explain.projection),
        "caps": graph_caps_to_json(explain.caps),
        "summaries": graph_summaries_to_json(explain.summaries),
        "warnings": explain.warnings,
        "notes": explain.notes,
    }))
}

fn graph_pipeline_explain_to_json(explain: GraphPipelineExplain) -> Result<serde_json::Value> {
    Ok(serde_json::json!({
        "columns": explain.columns,
        "effectiveAtEpoch": explain.effective_at_epoch,
        "fingerprint": explain.fingerprint,
        "stages": explain.stages.into_iter().map(graph_pipeline_stage_explain_to_json).collect::<Vec<_>>(),
        "rowOps": explain.row_ops.into_iter().map(graph_row_op_to_json).collect::<Vec<_>>(),
        "order": graph_order_explain_to_json(explain.order),
        "cursor": graph_cursor_explain_to_json(explain.cursor),
        "projection": graph_projection_explain_to_json(explain.projection),
        "caps": graph_pipeline_caps_to_json(explain.caps),
        "summaries": graph_summaries_to_json(explain.summaries),
        "stats": graph_pipeline_stats_to_json(explain.stats),
        "warnings": explain.warnings,
        "notes": explain.notes,
    }))
}

fn graph_pipeline_stage_explain_to_json(stage: GraphPipelineStageExplain) -> serde_json::Value {
    serde_json::json!({
        "index": stage.index,
        "kind": stage.kind,
        "detail": stage.detail,
        "columns": stage.columns,
        "graphRow": stage.graph_row.map(|explain| graph_explain_to_json(*explain)).transpose().ok().flatten(),
        "warnings": stage.warnings,
        "notes": stage.notes,
    })
}

fn graph_pipeline_stats_to_json(stats: GraphPipelineStats) -> serde_json::Value {
    serde_json::json!({
        "rowsReturned": stats.rows_returned,
        "rowsEnteredPipeline": stats.rows_entered_pipeline,
        "rowsAfterFilter": stats.rows_after_filter,
        "intermediateRows": stats.intermediate_rows,
        "pipelineRowsMaterialized": stats.pipeline_rows_materialized,
        "groups": stats.groups,
        "collectItems": stats.collect_items,
        "unionBranches": stats.union_branches,
        "unionDedupKeys": stats.union_dedup_keys,
        "subqueryInvocations": stats.subquery_invocations,
        "subqueryCacheHits": stats.subquery_cache_hits,
        "shortestPathPairs": stats.shortest_path_pairs,
        "shortestPathCacheHits": stats.shortest_path_cache_hits,
        "dbHits": stats.db_hits,
        "elapsedUs": stats.elapsed_us,
        "effectiveAtEpoch": stats.effective_at_epoch,
        "warnings": stats.warnings,
    })
}

fn graph_explain_node_to_json(node: GraphExplainNode) -> serde_json::Value {
    serde_json::json!({
        "kind": node.kind,
        "detail": node.detail,
        "children": node.children.into_iter().map(graph_explain_node_to_json).collect::<Vec<_>>(),
    })
}

fn graph_row_op_to_json(op: GraphRowOperationExplain) -> serde_json::Value {
    serde_json::json!({
        "kind": op.kind,
        "detail": op.detail,
    })
}

fn graph_order_explain_to_json(order: GraphOrderExplain) -> serde_json::Value {
    serde_json::json!({
        "explicit": order.explicit,
        "items": order.items,
        "stableLogicalRowKey": order.stable_logical_row_key,
    })
}

fn graph_cursor_explain_to_json(cursor: GraphCursorExplain) -> serde_json::Value {
    serde_json::json!({
        "supplied": cursor.supplied,
        "codecImplemented": cursor.codec_implemented,
        "message": cursor.message,
    })
}

fn graph_projection_explain_to_json(projection: GraphProjectionExplain) -> serde_json::Value {
    serde_json::json!({
        "columns": projection.columns,
        "outputMode": graph_output_mode_to_js(&projection.output_mode),
        "includeVectors": projection.include_vectors,
        "compactRows": projection.compact_rows,
    })
}

fn graph_caps_to_json(caps: GraphCapExplain) -> serde_json::Value {
    serde_json::json!({
        "allowFullScan": caps.allow_full_scan,
        "maxIntermediateBindings": caps.max_intermediate_bindings,
        "maxFrontier": caps.max_frontier,
        "maxPathHops": caps.max_path_hops,
        "maxPathsPerStart": caps.max_paths_per_start,
        "maxPageLimit": caps.max_page_limit,
        "maxOrderMaterialization": caps.max_order_materialization,
        "maxCursorBytes": caps.max_cursor_bytes,
        "maxQueryBytes": caps.max_query_bytes,
    })
}

fn graph_pipeline_caps_to_json(caps: GraphPipelineCapExplain) -> serde_json::Value {
    serde_json::json!({
        "allowFullScan": caps.allow_full_scan,
        "maxRows": caps.max_rows,
        "maxPipelineRows": caps.max_pipeline_rows,
        "maxGroups": caps.max_groups,
        "maxCollectItems": caps.max_collect_items,
        "maxUnionBranches": caps.max_union_branches,
        "maxSubqueryInvocations": caps.max_subquery_invocations,
        "maxSubqueryDepth": caps.max_subquery_depth,
        "maxShortestPathPairs": caps.max_shortest_path_pairs,
        "maxIntermediateBindings": caps.max_intermediate_bindings,
        "maxFrontier": caps.max_frontier,
        "maxPathHops": caps.max_path_hops,
        "maxPathsPerStart": caps.max_paths_per_start,
        "maxOrderMaterialization": caps.max_order_materialization,
        "maxSkip": caps.max_skip,
        "maxCursorBytes": caps.max_cursor_bytes,
        "maxQueryBytes": caps.max_query_bytes,
        "maxParamBytes": caps.max_param_bytes,
        "maxAstDepth": caps.max_ast_depth,
        "maxLiteralItems": caps.max_literal_items,
    })
}

fn graph_summaries_to_json(summaries: overgraph::GraphExecutionSummaries) -> serde_json::Value {
    serde_json::json!({
        "validationOnly": summaries.validation_only,
        "rowsPlanned": summaries.rows_planned,
        "warnings": summaries.warnings,
    })
}

fn graph_output_mode_to_js(mode: &GraphOutputMode) -> &'static str {
    match mode {
        GraphOutputMode::Ids => "ids",
        GraphOutputMode::Elements => "elements",
        GraphOutputMode::Projected => "projected",
    }
}

fn gql_edge_to_napi(env: napi::sys::napi_env, edge: GqlEdge) -> Result<napi::sys::napi_value> {
    let env_ref = Env::from_raw(env);
    let mut object = Object::new(&env_ref)?;
    if let Some(id) = edge.id {
        object.set("id", u64_to_f64(id)?)?;
    }
    if let Some(from) = edge.from {
        object.set("from", u64_to_f64(from)?)?;
    }
    if let Some(to) = edge.to {
        object.set("to", u64_to_f64(to)?)?;
    }
    if let Some(label) = edge.label {
        object.set("label", label)?;
    }
    if let Some(props) = edge.props {
        object.set("props", GqlJsValue(GqlValue::Map(props)))?;
    }
    if let Some(weight) = edge.weight {
        object.set("weight", weight as f64)?;
    }
    if let Some(created_at) = edge.created_at {
        object.set("createdAt", created_at)?;
    }
    if let Some(updated_at) = edge.updated_at {
        object.set("updatedAt", updated_at)?;
    }
    if let Some(valid_from) = edge.valid_from {
        object.set("validFrom", valid_from)?;
    }
    if let Some(valid_to) = edge.valid_to {
        object.set("validTo", valid_to)?;
    }
    unsafe { <&Object<'_> as ToNapiValue>::to_napi_value(env, &object) }
}

fn gql_stats_to_value(stats: GqlExecutionStats) -> GqlValue {
    GqlValue::Map(BTreeMap::from([
        (
            "rowsReturned".to_string(),
            GqlValue::UInt(stats.rows_returned as u64),
        ),
        (
            "rowsMatched".to_string(),
            GqlValue::UInt(stats.rows_matched as u64),
        ),
        (
            "rowsAfterFilter".to_string(),
            GqlValue::UInt(stats.rows_after_filter as u64),
        ),
        (
            "intermediateBindings".to_string(),
            GqlValue::UInt(stats.intermediate_bindings as u64),
        ),
        ("dbHits".to_string(), GqlValue::UInt(stats.db_hits as u64)),
        (
            "elapsedUs".to_string(),
            stats
                .elapsed_us
                .map(GqlValue::UInt)
                .unwrap_or(GqlValue::Null),
        ),
        (
            "warnings".to_string(),
            GqlValue::List(stats.warnings.into_iter().map(GqlValue::String).collect()),
        ),
    ]))
}

fn gql_mutation_stats_to_value(stats: overgraph::GqlMutationStats) -> GqlValue {
    GqlValue::Map(BTreeMap::from([
        (
            "rowsMatched".to_string(),
            GqlValue::UInt(stats.rows_matched as u64),
        ),
        (
            "mutationRows".to_string(),
            GqlValue::UInt(stats.mutation_rows as u64),
        ),
        (
            "mutationOps".to_string(),
            GqlValue::UInt(stats.mutation_ops as u64),
        ),
        (
            "nodesCreated".to_string(),
            GqlValue::UInt(stats.nodes_created as u64),
        ),
        (
            "nodesUpdated".to_string(),
            GqlValue::UInt(stats.nodes_updated as u64),
        ),
        (
            "nodesDeleted".to_string(),
            GqlValue::UInt(stats.nodes_deleted as u64),
        ),
        (
            "edgesCreated".to_string(),
            GqlValue::UInt(stats.edges_created as u64),
        ),
        (
            "edgesUpdated".to_string(),
            GqlValue::UInt(stats.edges_updated as u64),
        ),
        (
            "edgesDeleted".to_string(),
            GqlValue::UInt(stats.edges_deleted as u64),
        ),
        (
            "labelsAdded".to_string(),
            GqlValue::UInt(stats.labels_added as u64),
        ),
        (
            "labelsRemoved".to_string(),
            GqlValue::UInt(stats.labels_removed as u64),
        ),
        (
            "propertiesSet".to_string(),
            GqlValue::UInt(stats.properties_set as u64),
        ),
        (
            "propertiesRemoved".to_string(),
            GqlValue::UInt(stats.properties_removed as u64),
        ),
        (
            "skippedNullTargets".to_string(),
            GqlValue::UInt(stats.skipped_null_targets as u64),
        ),
        (
            "duplicateTargets".to_string(),
            GqlValue::UInt(stats.duplicate_targets as u64),
        ),
        ("dbHits".to_string(), GqlValue::UInt(stats.db_hits as u64)),
        (
            "elapsedUs".to_string(),
            stats
                .elapsed_us
                .map(GqlValue::UInt)
                .unwrap_or(GqlValue::Null),
        ),
        (
            "warnings".to_string(),
            GqlValue::List(stats.warnings.into_iter().map(GqlValue::String).collect()),
        ),
    ]))
}

fn gql_schema_stats_to_value(stats: overgraph::GqlSchemaStats) -> GqlValue {
    GqlValue::Map(BTreeMap::from([
        ("operation".to_string(), GqlValue::String(stats.operation)),
        (
            "targetsChecked".to_string(),
            GqlValue::UInt(stats.targets_checked),
        ),
        (
            "targetsPublished".to_string(),
            GqlValue::UInt(stats.targets_published),
        ),
        (
            "targetsDropped".to_string(),
            GqlValue::UInt(stats.targets_dropped),
        ),
        (
            "checkedRecords".to_string(),
            GqlValue::UInt(stats.checked_records),
        ),
        (
            "violationCount".to_string(),
            GqlValue::UInt(stats.violation_count),
        ),
        ("truncated".to_string(), GqlValue::Bool(stats.truncated)),
        (
            "scanLimitHit".to_string(),
            GqlValue::Bool(stats.scan_limit_hit),
        ),
        (
            "elapsedUs".to_string(),
            stats
                .elapsed_us
                .map(GqlValue::UInt)
                .unwrap_or(GqlValue::Null),
        ),
        (
            "warnings".to_string(),
            GqlValue::List(stats.warnings.into_iter().map(GqlValue::String).collect()),
        ),
    ]))
}

fn gql_index_stats_to_value(stats: overgraph::GqlIndexStats) -> GqlValue {
    GqlValue::Map(BTreeMap::from([
        ("operation".to_string(), GqlValue::String(stats.operation)),
        (
            "indexesEnsured".to_string(),
            GqlValue::UInt(stats.indexes_ensured),
        ),
        (
            "indexesDropped".to_string(),
            GqlValue::UInt(stats.indexes_dropped),
        ),
        (
            "indexesReturned".to_string(),
            GqlValue::UInt(stats.indexes_returned),
        ),
        (
            "elapsedUs".to_string(),
            stats
                .elapsed_us
                .map(GqlValue::UInt)
                .unwrap_or(GqlValue::Null),
        ),
        (
            "warnings".to_string(),
            GqlValue::List(stats.warnings.into_iter().map(GqlValue::String).collect()),
        ),
    ]))
}

fn gql_statement_kind_to_js(kind: GqlStatementKind) -> &'static str {
    match kind {
        GqlStatementKind::Query => "query",
        GqlStatementKind::Mutation => "mutation",
        GqlStatementKind::Schema => "schema",
        GqlStatementKind::Index => "index",
    }
}

fn gql_execution_caps_to_value(caps: GqlExecutionCapSummary) -> GqlValue {
    GqlValue::Map(BTreeMap::from([
        (
            "allowFullScan".to_string(),
            GqlValue::Bool(caps.allow_full_scan),
        ),
        ("maxRows".to_string(), GqlValue::UInt(caps.max_rows as u64)),
        (
            "maxCursorBytes".to_string(),
            GqlValue::UInt(caps.max_cursor_bytes as u64),
        ),
        (
            "maxMutationRows".to_string(),
            GqlValue::UInt(caps.max_mutation_rows as u64),
        ),
        (
            "maxMutationOps".to_string(),
            GqlValue::UInt(caps.max_mutation_ops as u64),
        ),
        (
            "maxPipelineRows".to_string(),
            GqlValue::UInt(caps.max_pipeline_rows as u64),
        ),
        (
            "maxGroups".to_string(),
            GqlValue::UInt(caps.max_groups as u64),
        ),
        (
            "maxCollectItems".to_string(),
            GqlValue::UInt(caps.max_collect_items as u64),
        ),
        (
            "maxUnionBranches".to_string(),
            GqlValue::UInt(caps.max_union_branches as u64),
        ),
        (
            "maxSubqueryInvocations".to_string(),
            GqlValue::UInt(caps.max_subquery_invocations as u64),
        ),
        (
            "maxSubqueryDepth".to_string(),
            GqlValue::UInt(caps.max_subquery_depth as u64),
        ),
        (
            "maxShortestPathPairs".to_string(),
            GqlValue::UInt(caps.max_shortest_path_pairs as u64),
        ),
        (
            "maxQueryBytes".to_string(),
            GqlValue::UInt(caps.max_query_bytes as u64),
        ),
        (
            "maxParamBytes".to_string(),
            GqlValue::UInt(caps.max_param_bytes as u64),
        ),
        (
            "maxAstDepth".to_string(),
            GqlValue::UInt(caps.max_ast_depth as u64),
        ),
        (
            "maxLiteralItems".to_string(),
            GqlValue::UInt(caps.max_literal_items as u64),
        ),
        (
            "maxIntermediateBindings".to_string(),
            GqlValue::UInt(caps.max_intermediate_bindings as u64),
        ),
        (
            "maxFrontier".to_string(),
            GqlValue::UInt(caps.max_frontier as u64),
        ),
        (
            "maxPathHops".to_string(),
            GqlValue::UInt(caps.max_path_hops as u64),
        ),
        (
            "maxPathsPerStart".to_string(),
            GqlValue::UInt(caps.max_paths_per_start as u64),
        ),
        (
            "maxOrderMaterialization".to_string(),
            GqlValue::UInt(caps.max_order_materialization as u64),
        ),
        ("maxSkip".to_string(), GqlValue::UInt(caps.max_skip as u64)),
    ]))
}

fn gql_mutation_explain_to_json(explain: overgraph::GqlMutationExplain) -> serde_json::Value {
    serde_json::json!({
        "readPrefix": explain.read_prefix.map(|prefix| serde_json::json!({
            "graphRowTarget": gql_read_explain_to_json(prefix.graph_row_target),
            "internalColumns": prefix.internal_columns,
            "targetAliases": prefix.target_aliases,
            "expressionColumns": prefix.expression_columns,
        })),
        "operations": explain.operations.into_iter().map(|operation| serde_json::json!({
            "op": operation.op,
            "targetAlias": operation.target_alias,
            "rowMultiplicity": operation.row_multiplicity,
            "detail": operation.detail,
        })).collect::<Vec<_>>(),
        "returnPlan": explain.return_plan.map(|plan| serde_json::json!({
            "columns": plan.columns,
            "orderItems": plan.order_items,
            "skip": plan.skip,
            "limit": plan.limit,
            "postCommitHydration": plan.post_commit_hydration,
        })),
        "wouldCreateNodeLabels": explain.would_create_node_labels,
        "wouldCreateEdgeLabels": explain.would_create_edge_labels,
        "usesTransactionSnapshot": explain.uses_transaction_snapshot,
        "usesWriteTxn": explain.uses_write_txn,
        "replacementAdapters": explain.replacement_adapters,
        "atomicCommit": explain.atomic_commit,
    })
}

fn gql_schema_explain_to_json(explain: overgraph::GqlSchemaExplain) -> serde_json::Value {
    serde_json::json!({
        "operation": explain.operation,
        "targets": explain.targets.into_iter().map(|target| serde_json::json!({
            "targetKind": target.target_kind,
            "label": target.label,
            "action": target.action,
        })).collect::<Vec<_>>(),
        "replacesEntireCatalog": explain.replaces_entire_catalog,
        "publishesManifest": explain.publishes_manifest,
        "validatesExistingData": explain.validates_existing_data,
        "usesCoreWriteQueue": explain.uses_core_write_queue,
        "sideEffectFree": explain.side_effect_free,
        "options": {
            "maxViolations": explain.options.max_violations,
            "chunkSize": explain.options.chunk_size,
            "scanLimit": explain.options.scan_limit,
        },
    })
}

fn gql_index_explain_to_json(explain: overgraph::GqlIndexExplain) -> serde_json::Value {
    serde_json::json!({
        "operation": explain.operation,
        "targets": explain.targets.into_iter().map(|target| {
            let fields = target.fields.into_iter().map(|field| serde_json::json!({
                "source": field.source,
                "key": field.key,
                "field": field.field,
            })).collect::<Vec<_>>();
            serde_json::json!({
                "targetKind": target.target_kind,
                "label": target.label,
                "fields": fields,
                "kind": target.kind,
                "action": target.action,
                "compound": target.compound,
            })
        }).collect::<Vec<_>>(),
        "usesCoreWriteQueue": explain.uses_core_write_queue,
        "publishesManifest": explain.publishes_manifest,
        "createsLabels": explain.creates_labels,
        "schedulesBackgroundBuild": explain.schedules_background_build,
        "dropsIndexDataAsync": explain.drops_index_data_async,
        "sideEffectFree": explain.side_effect_free,
    })
}

fn gql_read_explain_to_json(explain: GqlExplain) -> serde_json::Value {
    serde_json::json!({
        "columns": explain.columns,
        "target": gql_lowering_target_to_js(explain.target),
        "nativePlan": explain.native_plan.map(query_plan_to_json),
        "pushedDown": explain.pushed_down,
        "residual": explain.residual,
        "projection": explain.projection,
        "rowOps": explain.row_ops.into_iter().map(gql_row_operation_to_js).collect::<Vec<_>>(),
        "caps": gql_caps_to_json(explain.caps),
        "warnings": explain.warnings,
    })
}

fn gql_caps_to_json(caps: GqlCapSummary) -> serde_json::Value {
    serde_json::json!({
        "allowFullScan": caps.allow_full_scan,
        "maxRows": caps.max_rows,
        "maxIntermediateBindings": caps.max_intermediate_bindings,
        "maxSkip": caps.max_skip,
        "maxQueryBytes": caps.max_query_bytes,
        "maxParamBytes": caps.max_param_bytes,
        "maxAstDepth": caps.max_ast_depth,
        "maxLiteralItems": caps.max_literal_items,
    })
}

fn gql_caps_to_value(caps: GqlCapSummary) -> GqlValue {
    GqlValue::Map(BTreeMap::from([
        (
            "allowFullScan".to_string(),
            GqlValue::Bool(caps.allow_full_scan),
        ),
        ("maxRows".to_string(), GqlValue::UInt(caps.max_rows as u64)),
        (
            "maxIntermediateBindings".to_string(),
            GqlValue::UInt(caps.max_intermediate_bindings as u64),
        ),
        ("maxSkip".to_string(), GqlValue::UInt(caps.max_skip as u64)),
        (
            "maxQueryBytes".to_string(),
            GqlValue::UInt(caps.max_query_bytes as u64),
        ),
        (
            "maxParamBytes".to_string(),
            GqlValue::UInt(caps.max_param_bytes as u64),
        ),
        (
            "maxAstDepth".to_string(),
            GqlValue::UInt(caps.max_ast_depth as u64),
        ),
        (
            "maxLiteralItems".to_string(),
            GqlValue::UInt(caps.max_literal_items as u64),
        ),
    ]))
}

fn gql_lowering_target_to_js(target: GqlLoweringTarget) -> &'static str {
    match target {
        GqlLoweringTarget::NodeQuery => "node_query",
        GqlLoweringTarget::EdgeQuery => "edge_query",
        GqlLoweringTarget::GraphRowQuery => "graph_row_query",
        GqlLoweringTarget::GraphPipelineQuery => "graph_pipeline_query",
    }
}

fn gql_row_operation_to_js(op: GqlRowOperation) -> &'static str {
    match op {
        GqlRowOperation::ResidualFilter => "residual_filter",
        GqlRowOperation::Projection => "projection",
        GqlRowOperation::Sort => "sort",
        GqlRowOperation::Skip => "skip",
        GqlRowOperation::Limit => "limit",
    }
}

fn query_plan_to_js(plan: QueryPlan) -> Result<JsonPayload> {
    Ok(JsonPayload(query_plan_to_json(plan)))
}

fn query_plan_to_json(plan: QueryPlan) -> serde_json::Value {
    serde_json::json!({
        "kind": query_plan_kind_to_js(&plan.kind),
        "root": query_plan_node_to_js(plan.root),
        "estimatedCandidates": plan.estimated_candidates.map(|count| count as f64),
        "warnings": plan
            .warnings
            .iter()
            .map(query_plan_warning_to_js)
            .collect::<Vec<_>>(),
        "notes": plan
            .notes
            .iter()
            .map(query_plan_note_to_js)
            .collect::<Vec<_>>(),
        "publicInputs": query_plan_public_inputs_to_js(plan.public_inputs),
    })
}

fn query_plan_kind_to_js(kind: &QueryPlanKind) -> &'static str {
    match kind {
        QueryPlanKind::NodeQuery => "node_query",
        QueryPlanKind::EdgeQuery => "edge_query",
    }
}

fn query_plan_node_to_js(node: QueryPlanNode) -> serde_json::Value {
    match node {
        QueryPlanNode::ExplicitIds => serde_json::json!({ "kind": "explicit_ids" }),
        QueryPlanNode::KeyLookup => serde_json::json!({ "kind": "key_lookup" }),
        QueryPlanNode::NodeLabelIndex => serde_json::json!({ "kind": "node_label_index" }),
        QueryPlanNode::NodeLabelAnyIndex => serde_json::json!({ "kind": "node_label_any_index" }),
        QueryPlanNode::CompoundEqualityIndex { details } => serde_json::json!({
            "kind": "compound_equality_index",
            "details": compound_index_plan_details_to_json(details),
        }),
        QueryPlanNode::CompoundRangeIndex { details } => serde_json::json!({
            "kind": "compound_range_index",
            "details": compound_index_plan_details_to_json(details),
        }),
        QueryPlanNode::PropertyEqualityIndex => {
            serde_json::json!({ "kind": "property_equality_index" })
        }
        QueryPlanNode::PropertyRangeIndex => {
            serde_json::json!({ "kind": "property_range_index" })
        }
        QueryPlanNode::TimestampIndex => serde_json::json!({ "kind": "timestamp_index" }),
        QueryPlanNode::AdjacencyExpansion => serde_json::json!({ "kind": "adjacency_expansion" }),
        QueryPlanNode::ExplicitEdgeIds => serde_json::json!({ "kind": "explicit_edge_ids" }),
        QueryPlanNode::EdgeLabelIndex => serde_json::json!({ "kind": "edge_label_index" }),
        QueryPlanNode::EdgeTripleIndex => serde_json::json!({ "kind": "edge_triple_index" }),
        QueryPlanNode::EdgeEndpointAdjacency => {
            serde_json::json!({ "kind": "edge_endpoint_adjacency" })
        }
        QueryPlanNode::EdgeWeightIndex => serde_json::json!({ "kind": "edge_weight_index" }),
        QueryPlanNode::EdgeUpdatedAtIndex => {
            serde_json::json!({ "kind": "edge_updated_at_index" })
        }
        QueryPlanNode::EdgeValidityIndex => serde_json::json!({ "kind": "edge_validity_index" }),
        QueryPlanNode::EdgeMetadataScan => serde_json::json!({ "kind": "edge_metadata_scan" }),
        QueryPlanNode::EdgePropertyEqualityIndex => {
            serde_json::json!({ "kind": "edge_property_equality_index" })
        }
        QueryPlanNode::EdgePropertyRangeIndex => {
            serde_json::json!({ "kind": "edge_property_range_index" })
        }
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
        QueryPlanNode::VerifyEdgeFilter { input } => serde_json::json!({
            "kind": "verify_edge_filter",
            "input": query_plan_node_to_js(*input),
        }),
        QueryPlanNode::VerifyEdgePredicates { input } => serde_json::json!({
            "kind": "verify_edge_predicates",
            "input": query_plan_node_to_js(*input),
        }),
        QueryPlanNode::FallbackNodeLabelScan => {
            serde_json::json!({ "kind": "fallback_node_label_scan" })
        }
        QueryPlanNode::FallbackFullNodeScan => {
            serde_json::json!({ "kind": "fallback_full_node_scan" })
        }
        QueryPlanNode::FallbackEdgeLabelScan => {
            serde_json::json!({ "kind": "fallback_edge_label_scan" })
        }
        QueryPlanNode::FallbackFullEdgeScan => {
            serde_json::json!({ "kind": "fallback_full_edge_scan" })
        }
        QueryPlanNode::EmptyResult => serde_json::json!({ "kind": "empty_result" }),
    }
}

fn query_plan_note_to_js(note: &overgraph::QueryPlanNote) -> &'static str {
    match note {
        overgraph::QueryPlanNote::NodeLabelAnyDedupeBeforePagination => {
            "node_label_any_dedupe_before_pagination"
        }
        overgraph::QueryPlanNote::NodeLabelAnyFinalVerification => {
            "node_label_any_final_verification"
        }
        overgraph::QueryPlanNote::NodeLabelAllSupersetVerification => {
            "node_label_all_superset_verification"
        }
        overgraph::QueryPlanNote::StaleNodeLabelMembershipVerification => {
            "stale_node_label_membership_verification"
        }
    }
}

fn query_plan_public_inputs_to_js(inputs: overgraph::QueryPlanPublicInputs) -> serde_json::Value {
    serde_json::json!({
        "nodeLabels": inputs
            .node_labels
            .into_iter()
            .map(query_plan_public_name_to_js)
            .collect::<Vec<_>>(),
        "edgeLabels": inputs
            .edge_labels
            .into_iter()
            .map(query_plan_public_name_to_js)
            .collect::<Vec<_>>(),
    })
}

fn query_plan_public_name_to_js(name: overgraph::QueryPlanPublicName) -> serde_json::Value {
    serde_json::json!({
        "alias": name.alias,
        "name": name.name,
        "known": name.known,
        "mode": name.mode.map(|mode| match mode {
            CoreLabelMatchMode::Any => "any",
            CoreLabelMatchMode::All => "all",
        }),
    })
}

fn query_plan_warning_to_js(warning: &QueryPlanWarning) -> &'static str {
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

fn compound_index_plan_details_to_json(details: CompoundIndexPlanDetails) -> serde_json::Value {
    serde_json::json!({
        "indexId": details.index_id as f64,
        "targetKind": query_plan_compound_target_kind_to_js(details.target_kind),
        "label": details.label,
        "kind": secondary_index_kind_to_js(&details.kind),
        "fields": details
            .fields
            .into_iter()
            .map(secondary_index_field_to_json)
            .collect::<Vec<_>>(),
        "compound": details.compound,
        "matchedPrefixLen": details.matched_prefix_len as f64,
        "rangeField": details.range_field.map(secondary_index_field_to_json),
        "inExpansions": details.in_expansions as f64,
        "estimatedCandidates": details.estimated_candidates.map(|count| count as f64),
        "coverage": details.coverage,
        "residualPredicates": details.residual_predicates as f64,
        "finalVerification": details.final_verification,
        "fallbackReason": details.fallback_reason,
    })
}

fn query_plan_compound_target_kind_to_js(kind: QueryPlanCompoundTargetKind) -> &'static str {
    match kind {
        QueryPlanCompoundTargetKind::Node => "node",
        QueryPlanCompoundTargetKind::Edge => "edge",
    }
}

fn secondary_index_field_to_json(field: CoreSecondaryIndexField) -> serde_json::Value {
    let field = secondary_index_field_to_js(field);
    serde_json::json!({
        "source": field.source,
        "key": field.key,
        "field": field.field,
    })
}

fn parse_js_gql_options(
    options: Option<GqlExecutionOptionsInput>,
) -> Result<(GqlExecutionOptions, bool)> {
    let mut parsed = GqlExecutionOptions::default();
    let mut compact_rows = false;
    if let Some(options) = options {
        if let Some(value) = options.mode {
            parsed.mode = parse_js_gql_execution_mode(&value)?;
        }
        if let Some(value) = options.allow_full_scan {
            parsed.allow_full_scan = value;
        }
        if let Some(value) = options.max_rows {
            parsed.max_rows = f64_to_usize(value, "GQL maxRows")?;
        }
        if let Some(value) = options.cursor {
            parsed.cursor = Some(value);
        }
        if let Some(value) = options.max_cursor_bytes {
            parsed.max_cursor_bytes = f64_to_usize(value, "GQL maxCursorBytes")?;
        }
        if let Some(value) = options.max_mutation_rows {
            parsed.max_mutation_rows = f64_to_usize(value, "GQL maxMutationRows")?;
        }
        if let Some(value) = options.max_mutation_ops {
            parsed.max_mutation_ops = f64_to_usize(value, "GQL maxMutationOps")?;
        }
        if let Some(value) = options.max_pipeline_rows {
            parsed.max_pipeline_rows = f64_to_usize(value, "GQL maxPipelineRows")?;
        }
        if let Some(value) = options.max_groups {
            parsed.max_groups = f64_to_usize(value, "GQL maxGroups")?;
        }
        if let Some(value) = options.max_collect_items {
            parsed.max_collect_items = f64_to_usize(value, "GQL maxCollectItems")?;
        }
        if let Some(value) = options.max_union_branches {
            parsed.max_union_branches = f64_to_usize(value, "GQL maxUnionBranches")?;
        }
        if let Some(value) = options.max_subquery_invocations {
            parsed.max_subquery_invocations = f64_to_usize(value, "GQL maxSubqueryInvocations")?;
        }
        if let Some(value) = options.max_subquery_depth {
            parsed.max_subquery_depth = f64_to_usize(value, "GQL maxSubqueryDepth")?;
        }
        if let Some(value) = options.max_shortest_path_pairs {
            parsed.max_shortest_path_pairs = f64_to_usize(value, "GQL maxShortestPathPairs")?;
        }
        if let Some(value) = options.max_intermediate_bindings {
            parsed.max_intermediate_bindings = f64_to_usize(value, "GQL maxIntermediateBindings")?;
        }
        if let Some(value) = options.max_frontier {
            parsed.max_frontier = f64_to_usize(value, "GQL maxFrontier")?;
        }
        if let Some(value) = options.max_path_hops {
            let parsed_value = f64_to_usize(value, "GQL maxPathHops")?;
            parsed.max_path_hops = u8::try_from(parsed_value).map_err(|_| {
                napi::Error::from_reason("GQL maxPathHops must be between 0 and 255".to_string())
            })?;
        }
        if let Some(value) = options.max_paths_per_start {
            parsed.max_paths_per_start = f64_to_usize(value, "GQL maxPathsPerStart")?;
        }
        if let Some(value) = options.max_order_materialization {
            parsed.max_order_materialization = f64_to_usize(value, "GQL maxOrderMaterialization")?;
        }
        if let Some(value) = options.max_skip {
            parsed.max_skip = f64_to_usize(value, "GQL maxSkip")?;
        }
        if let Some(value) = options.max_query_bytes {
            parsed.max_query_bytes = f64_to_usize(value, "GQL maxQueryBytes")?;
        }
        if let Some(value) = options.max_param_bytes {
            parsed.max_param_bytes = f64_to_usize(value, "GQL maxParamBytes")?;
        }
        if let Some(value) = options.max_ast_depth {
            parsed.max_ast_depth = f64_to_usize(value, "GQL maxAstDepth")?;
        }
        if let Some(value) = options.max_literal_items {
            parsed.max_literal_items = f64_to_usize(value, "GQL maxLiteralItems")?;
        }
        if let Some(value) = options.include_plan {
            parsed.include_plan = value;
        }
        if let Some(value) = options.profile {
            parsed.profile = value;
        }
        if let Some(value) = options.compact_rows {
            compact_rows = value;
            parsed.compact_rows = value;
        }
        if let Some(value) = options.include_vectors {
            parsed.include_vectors = value;
        }
    }
    Ok((parsed, compact_rows))
}

fn parse_js_gql_execution_mode(value: &str) -> Result<GqlExecutionMode> {
    match value {
        "auto" => Ok(GqlExecutionMode::Auto),
        "readOnly" => Ok(GqlExecutionMode::ReadOnly),
        other => Err(napi::Error::from_reason(format!(
            "GQL mode must be 'auto' or 'readOnly', got '{other}'"
        ))),
    }
}

struct GqlParamConversionBudget {
    total_items: usize,
    total_bytes: usize,
}

fn parse_js_gql_params(
    params: Option<Unknown<'_>>,
    referenced_params: &[String],
    options: &GqlExecutionOptions,
) -> Result<GqlParams> {
    if referenced_params.is_empty() {
        return Ok(GqlParams::new());
    }
    let Some(params) = params else {
        return Ok(GqlParams::new());
    };
    match params.get_type()? {
        napi::ValueType::Null | napi::ValueType::Undefined => Ok(GqlParams::new()),
        napi::ValueType::Object if !params.is_array()? && !params.is_buffer()? => {
            let object = unsafe { params.cast::<Object<'_>>()? };
            let mut parsed = GqlParams::new();
            let mut budget = GqlParamConversionBudget {
                total_items: 0,
                total_bytes: 0,
            };
            for key in referenced_params {
                if !object.has_own_property(key)? {
                    continue;
                }
                let value = match object.get::<Unknown<'_>>(key)? {
                    Some(value) => parse_js_gql_param_value(key, value, 0, options, &mut budget)?,
                    None => GqlParamValue::Null,
                };
                parsed.insert(key.clone(), value);
            }
            Ok(parsed)
        }
        _ => Err(napi::Error::from_reason(
            "GQL params must be a plain object, null, or undefined".to_string(),
        )),
    }
}

fn parse_js_gql_param_value(
    name: &str,
    value: Unknown<'_>,
    container_depth: usize,
    options: &GqlExecutionOptions,
    budget: &mut GqlParamConversionBudget,
) -> Result<GqlParamValue> {
    match value.get_type()? {
        napi::ValueType::Null | napi::ValueType::Undefined => Ok(GqlParamValue::Null),
        napi::ValueType::Boolean => Ok(GqlParamValue::Bool(unsafe { value.cast::<bool>()? })),
        napi::ValueType::Number => {
            let number = unsafe { value.cast::<f64>()? };
            if !number.is_finite() {
                return Err(napi::Error::from_reason(
                    "GQL numeric params must be finite".to_string(),
                ));
            }
            if number.fract() == 0.0 && number.abs() <= MAX_SAFE_INTEGER {
                if number < 0.0 {
                    Ok(GqlParamValue::Int(number as i64))
                } else {
                    Ok(GqlParamValue::UInt(number as u64))
                }
            } else {
                Ok(GqlParamValue::Float(number))
            }
        }
        napi::ValueType::String => {
            let string = unsafe { value.cast::<JsString<'_>>()? };
            add_js_param_bytes(name, string.utf8_len()?, "string", budget, options)?;
            Ok(GqlParamValue::String(string.into_utf8()?.into_owned()?))
        }
        napi::ValueType::Object if value.is_buffer()? => {
            let buffer = unsafe { value.cast::<BufferSlice<'_>>()? };
            add_js_param_bytes(name, buffer.as_ref().len(), "bytes", budget, options)?;
            Ok(GqlParamValue::Bytes(buffer.as_ref().to_vec()))
        }
        napi::ValueType::Object if value.is_arraybuffer()? => {
            let buffer = unsafe { value.cast::<ArrayBuffer<'_>>()? };
            add_js_param_bytes(name, buffer.len(), "bytes", budget, options)?;
            Ok(GqlParamValue::Bytes(buffer.to_vec()))
        }
        napi::ValueType::Object if value.is_array()? => {
            let array = unsafe { value.cast::<Array<'_>>()? };
            let depth = container_depth.saturating_add(1);
            check_js_param_depth(name, depth, options)?;
            add_js_param_items(name, array.len() as usize, "list", budget, options)?;
            let mut items = Vec::with_capacity(array.len() as usize);
            for index in 0..array.len() {
                let item = array.get::<Unknown<'_>>(index)?;
                items.push(match item {
                    Some(item) => parse_js_gql_param_value(name, item, depth, options, budget)?,
                    None => GqlParamValue::Null,
                });
            }
            Ok(GqlParamValue::List(items))
        }
        napi::ValueType::Object => {
            let object = unsafe { value.cast::<Object<'_>>()? };
            let depth = container_depth.saturating_add(1);
            check_js_param_depth(name, depth, options)?;
            let keys = js_object_property_names_array(&object)?;
            add_js_param_items(name, keys.len() as usize, "map", budget, options)?;
            let mut map = BTreeMap::new();
            for index in 0..keys.len() {
                let key = keys.get::<JsString<'_>>(index)?.ok_or_else(|| {
                    napi::Error::from_reason(format!(
                        "GQL parameter '${name}' map key at index {index} is missing"
                    ))
                })?;
                add_js_param_bytes(name, key.utf8_len()?, "map key", budget, options)?;
                let key = key.into_utf8()?.into_owned()?;
                let item = object.get::<Unknown<'_>>(&key)?;
                map.insert(
                    key,
                    match item {
                        Some(item) => parse_js_gql_param_value(name, item, depth, options, budget)?,
                        None => GqlParamValue::Null,
                    },
                );
            }
            Ok(GqlParamValue::Map(map))
        }
        other => Err(napi::Error::from_reason(format!(
            "Unsupported GQL param value type: {other}"
        ))),
    }
}

fn js_object_property_names_array<'env>(object: &Object<'env>) -> Result<Array<'env>> {
    let names = object.get_property_names()?;
    let value = names.value();
    unsafe { Array::from_napi_value(value.env, value.value) }
}

fn check_js_param_depth(name: &str, depth: usize, options: &GqlExecutionOptions) -> Result<()> {
    if depth > options.max_ast_depth {
        return Err(napi::Error::from_reason(format!(
            "GQL parameter '${name}' nested list/map depth exceeds maxAstDepth of {}",
            options.max_ast_depth
        )));
    }
    Ok(())
}

fn add_js_param_items(
    name: &str,
    count: usize,
    container_kind: &str,
    budget: &mut GqlParamConversionBudget,
    options: &GqlExecutionOptions,
) -> Result<()> {
    if count > options.max_literal_items {
        return Err(napi::Error::from_reason(format!(
            "GQL parameter '${name}' {container_kind} contains {count} items, exceeding maxLiteralItems of {}",
            options.max_literal_items
        )));
    }
    budget.total_items = budget
        .total_items
        .checked_add(count)
        .filter(|total| *total <= options.max_literal_items)
        .ok_or_else(|| {
            napi::Error::from_reason(format!(
                "Referenced GQL parameters contain more than maxLiteralItems={} total list/map items",
                options.max_literal_items
            ))
        })?;
    Ok(())
}

fn add_js_param_bytes(
    name: &str,
    bytes: usize,
    value_kind: &str,
    budget: &mut GqlParamConversionBudget,
    options: &GqlExecutionOptions,
) -> Result<()> {
    if bytes > options.max_param_bytes {
        return Err(napi::Error::from_reason(format!(
            "GQL parameter '${name}' {value_kind} is {bytes} bytes, exceeding maxParamBytes of {}",
            options.max_param_bytes
        )));
    }
    budget.total_bytes = budget
        .total_bytes
        .checked_add(bytes)
        .filter(|total| *total <= options.max_param_bytes)
        .ok_or_else(|| {
            napi::Error::from_reason(format!(
                "Referenced GQL parameters contain more than maxParamBytes={} total string/bytes/map-key bytes",
                options.max_param_bytes
            ))
        })?;
    Ok(())
}

fn parse_js_node_query(value: &serde_json::Value) -> Result<NodeQuery> {
    let object = js_object(value, "node query request")?;
    ensure_only_js_fields(
        object,
        &[
            "labelFilter",
            "ids",
            "keys",
            "filter",
            "orderBy",
            "limit",
            "after",
            "allowFullScan",
            "where",
            "predicates",
        ],
        "node query request",
    )?;
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
        label_filter: parse_js_node_label_filter_field(
            object,
            "labelFilter",
            "node query labelFilter",
        )?,
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

fn parse_js_edge_query(value: &serde_json::Value) -> Result<EdgeQuery> {
    let object = js_object(value, "edge query request")?;
    reject_js_legacy_node_predicate_fields(object, "edge query")?;
    let page = PageRequest {
        limit: parse_js_limit(object, "edge query limit")?,
        after: parse_js_optional_u64_field(object, "after", "edge query after")?,
    };
    Ok(EdgeQuery {
        label: parse_js_optional_string_field(object, "label", "edge query label")?,
        ids: parse_js_optional_u64_array_field(object, "ids", "edge query ids")?,
        from_ids: parse_js_optional_u64_array_field(object, "fromIds", "edge query fromIds")?,
        to_ids: parse_js_optional_u64_array_field(object, "toIds", "edge query toIds")?,
        endpoint_ids: parse_js_optional_u64_array_field(
            object,
            "endpointIds",
            "edge query endpointIds",
        )?,
        filter: parse_js_edge_filter(
            object,
            "updatedAt",
            "validAt",
            "validFrom",
            "validTo",
            "edge query",
        )?,
        page,
        order: EdgeQueryOrder::EdgeIdAsc,
        allow_full_scan: parse_js_optional_bool_field(
            object,
            "allowFullScan",
            "edge query allowFullScan",
        )?
        .unwrap_or(false),
    })
}

fn parse_js_graph_row_query(value: &serde_json::Value) -> Result<GraphRowQuery> {
    let object = js_object(value, "graph row request")?;
    ensure_only_js_fields(
        object,
        &[
            "nodes", "pieces", "where", "return", "orderBy", "skip", "limit", "cursor", "atEpoch",
            "params", "output", "options",
        ],
        "graph row request",
    )?;
    let options = parse_js_graph_query_options(js_non_null_field(object, "options"))?;
    let output = parse_js_graph_output_options(js_non_null_field(object, "output"))?;
    let limit = match js_non_null_field(object, "limit") {
        Some(value) => {
            let parsed = js_number_to_u64(value, "graph row limit")?;
            if parsed == 0 {
                return Err(napi::Error::from_reason(
                    "graph row limit must be > 0".to_string(),
                ));
            }
            usize::try_from(parsed)
                .map_err(|_| napi::Error::from_reason("graph row limit is too large".to_string()))?
        }
        None => 1000.min(options.max_page_limit),
    };
    let nodes = match js_non_null_field(object, "nodes") {
        Some(value) => js_array(value, "graph row nodes")?
            .iter()
            .enumerate()
            .map(|(index, value)| {
                parse_js_graph_node_pattern(value, &format!("graph row nodes[{index}]"))
            })
            .collect::<Result<Vec<_>>>()?,
        None => Vec::new(),
    };
    let pieces = match js_non_null_field(object, "pieces") {
        Some(value) => js_array(value, "graph row pieces")?
            .iter()
            .enumerate()
            .map(|(index, value)| {
                parse_js_graph_piece(value, &format!("graph row pieces[{index}]"))
            })
            .collect::<Result<Vec<_>>>()?,
        None => Vec::new(),
    };
    let return_items = match js_non_null_field(object, "return") {
        Some(value) => Some(
            js_array(value, "graph row return")?
                .iter()
                .enumerate()
                .map(|(index, value)| {
                    parse_js_graph_return_item(value, &format!("graph row return[{index}]"))
                })
                .collect::<Result<Vec<_>>>()?,
        ),
        None => None,
    };
    let order_by = match js_non_null_field(object, "orderBy") {
        Some(value) => js_array(value, "graph row orderBy")?
            .iter()
            .enumerate()
            .map(|(index, value)| {
                parse_js_graph_order_item(value, &format!("graph row orderBy[{index}]"))
            })
            .collect::<Result<Vec<_>>>()?,
        None => Vec::new(),
    };
    Ok(GraphRowQuery {
        nodes,
        pieces,
        where_: js_non_null_field(object, "where")
            .map(|value| parse_js_graph_expr(value, "graph row where"))
            .transpose()?,
        return_items,
        order_by,
        page: GraphPageRequest {
            skip: js_non_null_field(object, "skip")
                .map(|value| js_number_to_usize(value, "graph row skip"))
                .transpose()?
                .unwrap_or(0),
            limit,
            cursor: parse_js_optional_string_field(object, "cursor", "graph row cursor")?,
        },
        at_epoch: parse_js_optional_i64_field(object, "atEpoch", "graph row atEpoch")?,
        params: parse_js_graph_params(js_non_null_field(object, "params"))?,
        output,
        options,
    })
}

fn parse_js_graph_pipeline_query(value: &serde_json::Value) -> Result<GraphPipelineQuery> {
    let object = js_object(value, "graph pipeline request")?;
    ensure_only_js_fields(
        object,
        &[
            "stages", "params", "atEpoch", "skip", "limit", "cursor", "output", "options",
        ],
        "graph pipeline request",
    )?;
    let options = parse_js_graph_pipeline_options(js_non_null_field(object, "options"))?;
    let output = parse_js_graph_output_options(js_non_null_field(object, "output"))?;
    let limit = match js_non_null_field(object, "limit") {
        Some(value) => {
            let parsed = js_number_to_u64(value, "graph pipeline limit")?;
            if parsed == 0 {
                return Err(napi::Error::from_reason(
                    "graph pipeline limit must be > 0".to_string(),
                ));
            }
            usize::try_from(parsed).map_err(|_| {
                napi::Error::from_reason("graph pipeline limit is too large".to_string())
            })?
        }
        None => options.max_rows,
    };
    let stages_value = js_non_null_field(object, "stages").ok_or_else(|| {
        napi::Error::from_reason("graph pipeline request requires stages".to_string())
    })?;
    let stages = js_array(stages_value, "graph pipeline stages")?
        .iter()
        .enumerate()
        .map(|(index, value)| {
            parse_js_graph_pipeline_stage(value, &format!("graph pipeline stages[{index}]"))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(GraphPipelineQuery {
        stages,
        params: parse_js_graph_params(js_non_null_field(object, "params"))?,
        at_epoch: parse_js_optional_i64_field(object, "atEpoch", "graph pipeline atEpoch")?,
        page: GraphPageRequest {
            skip: js_non_null_field(object, "skip")
                .map(|value| js_number_to_usize(value, "graph pipeline skip"))
                .transpose()?
                .unwrap_or(0),
            limit,
            cursor: parse_js_optional_string_field(object, "cursor", "graph pipeline cursor")?,
        },
        output,
        options,
    })
}

fn parse_js_graph_pipeline_stage(
    value: &serde_json::Value,
    context: &str,
) -> Result<GraphPipelineStage> {
    let object = js_object(value, context)?;
    let kind = parse_js_required_string_field(object, "kind", &format!("{context} kind"))?;
    match kind.as_str() {
        "match" => parse_js_graph_pipeline_match_stage(object, context).map(GraphPipelineStage::Match),
        "project" | "with" | "return" => parse_js_graph_pipeline_project_stage(object, &kind, context)
            .map(GraphPipelineStage::Project),
        "shortestPath" | "shortest_path" => parse_js_graph_pipeline_shortest_path_stage(object, context)
            .map(GraphPipelineStage::ShortestPath),
        "call" => parse_js_graph_pipeline_call_stage(object, context).map(GraphPipelineStage::Call),
        "union" => parse_js_graph_pipeline_union_stage(object, context).map(GraphPipelineStage::Union),
        other => Err(napi::Error::from_reason(format!(
            "{context} kind must be 'match', 'project', 'with', 'return', 'shortestPath', 'call', or 'union', got '{other}'"
        ))),
    }
}

fn parse_js_graph_pipeline_match_stage(
    object: &serde_json::Map<String, serde_json::Value>,
    context: &str,
) -> Result<GraphPipelineMatchStage> {
    ensure_only_js_fields(
        object,
        &[
            "kind",
            "optional",
            "nodes",
            "pieces",
            "where",
            "optionalCandidateWhere",
        ],
        context,
    )?;
    let nodes = match js_non_null_field(object, "nodes") {
        Some(value) => js_array(value, &format!("{context} nodes"))?
            .iter()
            .enumerate()
            .map(|(index, value)| {
                parse_js_graph_node_pattern(value, &format!("{context} nodes[{index}]"))
            })
            .collect::<Result<Vec<_>>>()?,
        None => Vec::new(),
    };
    let pieces = match js_non_null_field(object, "pieces") {
        Some(value) => js_array(value, &format!("{context} pieces"))?
            .iter()
            .enumerate()
            .map(|(index, value)| {
                parse_js_graph_piece(value, &format!("{context} pieces[{index}]"))
            })
            .collect::<Result<Vec<_>>>()?,
        None => Vec::new(),
    };
    Ok(GraphPipelineMatchStage {
        optional: parse_js_optional_bool_field(object, "optional", &format!("{context} optional"))?
            .unwrap_or(false),
        nodes,
        pieces,
        where_: js_non_null_field(object, "where")
            .map(|value| parse_js_graph_expr(value, &format!("{context} where")))
            .transpose()?,
        optional_candidate_where: js_non_null_field(object, "optionalCandidateWhere")
            .map(|value| parse_js_graph_expr(value, &format!("{context} optionalCandidateWhere")))
            .transpose()?,
    })
}

fn parse_js_graph_pipeline_project_stage(
    object: &serde_json::Map<String, serde_json::Value>,
    kind: &str,
    context: &str,
) -> Result<GraphProjectStage> {
    ensure_only_js_fields(
        object,
        &[
            "kind",
            "projectKind",
            "items",
            "distinct",
            "where",
            "orderBy",
            "skip",
            "limit",
        ],
        context,
    )?;
    let project_kind = match kind {
        "with" => GraphProjectKind::With,
        "return" => GraphProjectKind::Return,
        _ => match parse_js_optional_string_field(
            object,
            "projectKind",
            &format!("{context} projectKind"),
        )?
        .as_deref()
        {
            None | Some("return") => GraphProjectKind::Return,
            Some("with") => GraphProjectKind::With,
            Some(other) => {
                return Err(napi::Error::from_reason(format!(
                    "{context} projectKind must be 'with' or 'return', got '{other}'"
                )));
            }
        },
    };
    let items = js_non_null_field(object, "items")
        .map(|value| parse_js_graph_projection_items(value, &format!("{context} items")))
        .transpose()?
        .unwrap_or(GraphProjectionItems::Star);
    let order_by = match js_non_null_field(object, "orderBy") {
        Some(value) => js_array(value, &format!("{context} orderBy"))?
            .iter()
            .enumerate()
            .map(|(index, value)| {
                parse_js_graph_order_item(value, &format!("{context} orderBy[{index}]"))
            })
            .collect::<Result<Vec<_>>>()?,
        None => Vec::new(),
    };
    Ok(GraphProjectStage {
        kind: project_kind,
        items,
        distinct: parse_js_optional_bool_field(object, "distinct", &format!("{context} distinct"))?
            .unwrap_or(false),
        where_: js_non_null_field(object, "where")
            .map(|value| parse_js_graph_expr(value, &format!("{context} where")))
            .transpose()?,
        order_by,
        skip: js_non_null_field(object, "skip")
            .map(|value| parse_js_graph_expr(value, &format!("{context} skip")))
            .transpose()?,
        limit: js_non_null_field(object, "limit")
            .map(|value| parse_js_graph_expr(value, &format!("{context} limit")))
            .transpose()?,
    })
}

fn parse_js_graph_projection_items(
    value: &serde_json::Value,
    context: &str,
) -> Result<GraphProjectionItems> {
    if matches!(value.as_str(), Some("star" | "*")) {
        return Ok(GraphProjectionItems::Star);
    }
    Ok(GraphProjectionItems::Items(
        js_array(value, context)?
            .iter()
            .enumerate()
            .map(|(index, value)| {
                parse_js_graph_project_item(value, &format!("{context}[{index}]"))
            })
            .collect::<Result<Vec<_>>>()?,
    ))
}

fn parse_js_graph_project_item(
    value: &serde_json::Value,
    context: &str,
) -> Result<GraphProjectItem> {
    let object = js_object(value, context)?;
    ensure_only_js_fields(object, &["expr", "as", "projection"], context)?;
    let expr_value = js_non_null_field(object, "expr")
        .ok_or_else(|| napi::Error::from_reason(format!("{context} expr is required")))?;
    Ok(GraphProjectItem {
        expr: parse_js_graph_expr(expr_value, &format!("{context} expr"))?,
        alias: parse_js_optional_string_field(object, "as", &format!("{context} as"))?,
        projection: parse_js_graph_return_projection(
            js_non_null_field(object, "projection"),
            context,
        )?,
    })
}

fn parse_js_graph_pipeline_union_stage(
    object: &serde_json::Map<String, serde_json::Value>,
    context: &str,
) -> Result<GraphUnionStage> {
    ensure_only_js_fields(object, &["kind", "branches", "all"], context)?;
    let branches_value = js_non_null_field(object, "branches")
        .ok_or_else(|| napi::Error::from_reason(format!("{context} requires branches")))?;
    let branches = js_array(branches_value, &format!("{context} branches"))?
        .iter()
        .enumerate()
        .map(|(index, value)| {
            parse_js_graph_pipeline_query(value).map_err(|err| {
                napi::Error::from_reason(format!("{context} branches[{index}]: {}", err.reason))
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(GraphUnionStage {
        branches,
        all: parse_js_optional_bool_field(object, "all", &format!("{context} all"))?
            .unwrap_or(false),
    })
}

fn parse_js_graph_pipeline_call_stage(
    object: &serde_json::Map<String, serde_json::Value>,
    context: &str,
) -> Result<GraphSubqueryStage> {
    ensure_only_js_fields(object, &["kind", "query", "importAliases"], context)?;
    let query_value = js_non_null_field(object, "query")
        .ok_or_else(|| napi::Error::from_reason(format!("{context} requires query")))?;
    Ok(GraphSubqueryStage {
        query: Box::new(parse_js_graph_pipeline_query(query_value)?),
        import_aliases: parse_js_optional_string_array_field(
            object,
            "importAliases",
            &format!("{context} importAliases"),
        )?,
    })
}

fn parse_js_graph_pipeline_shortest_path_stage(
    object: &serde_json::Map<String, serde_json::Value>,
    context: &str,
) -> Result<GraphShortestPathStage> {
    ensure_only_js_fields(
        object,
        &[
            "kind",
            "optional",
            "outputPathAlias",
            "mode",
            "from",
            "to",
            "direction",
            "edgeLabelFilter",
            "minHops",
            "maxHops",
            "weightField",
            "maxCost",
            "maxPaths",
        ],
        context,
    )?;
    let mode = match parse_js_optional_string_field(object, "mode", &format!("{context} mode"))?
        .as_deref()
    {
        None | Some("one") => GraphShortestPathMode::One,
        Some("all") => GraphShortestPathMode::All,
        Some(other) => {
            return Err(napi::Error::from_reason(format!(
                "{context} mode must be 'one' or 'all', got '{other}'"
            )));
        }
    };
    let direction = match js_non_null_field(object, "direction") {
        None => Direction::Outgoing,
        Some(value) => parse_direction(Some(value.as_str().ok_or_else(|| {
            napi::Error::from_reason(format!("{context} direction must be a string"))
        })?))?,
    };
    let max_cost = match js_non_null_field(object, "maxCost") {
        Some(value) => {
            let cost = value.as_f64().ok_or_else(|| {
                napi::Error::from_reason(format!("{context} maxCost must be a number"))
            })?;
            if !cost.is_finite() {
                return Err(napi::Error::from_reason(format!(
                    "{context} maxCost must be finite"
                )));
            }
            Some(cost)
        }
        None => None,
    };
    Ok(GraphShortestPathStage {
        optional: parse_js_optional_bool_field(object, "optional", &format!("{context} optional"))?
            .unwrap_or(false),
        output_path_alias: parse_js_required_string_field(
            object,
            "outputPathAlias",
            &format!("{context} outputPathAlias"),
        )?,
        mode,
        from: parse_js_shortest_path_endpoint(
            js_non_null_field(object, "from")
                .ok_or_else(|| napi::Error::from_reason(format!("{context} requires from")))?,
            &format!("{context} from"),
        )?,
        to: parse_js_shortest_path_endpoint(
            js_non_null_field(object, "to")
                .ok_or_else(|| napi::Error::from_reason(format!("{context} requires to")))?,
            &format!("{context} to"),
        )?,
        direction,
        edge_label_filter: parse_js_optional_string_array_field(
            object,
            "edgeLabelFilter",
            &format!("{context} edgeLabelFilter"),
        )?,
        min_hops: parse_js_required_u8_field(object, "minHops", &format!("{context} minHops"))?,
        max_hops: parse_js_required_u8_field(object, "maxHops", &format!("{context} maxHops"))?,
        weight_field: parse_js_optional_string_field(
            object,
            "weightField",
            &format!("{context} weightField"),
        )?,
        max_cost,
        max_paths: js_non_null_field(object, "maxPaths")
            .map(|value| js_number_to_usize(value, &format!("{context} maxPaths")))
            .transpose()?,
    })
}

fn parse_js_shortest_path_endpoint(
    value: &serde_json::Value,
    context: &str,
) -> Result<GraphShortestPathEndpoint> {
    if let Some(alias) = value.as_str() {
        return Ok(GraphShortestPathEndpoint::Alias(alias.to_string()));
    }
    if value.is_number() {
        return Ok(GraphShortestPathEndpoint::NodeId(js_number_to_u64(
            value, context,
        )?));
    }
    let object = js_object(value, context)?;
    let tags = ["alias", "nodeId", "nodeKey", "expr"]
        .iter()
        .filter(|field| object.contains_key(**field))
        .count();
    if tags != 1 {
        return Err(napi::Error::from_reason(format!(
            "{context} must contain exactly one of alias, nodeId, nodeKey, or expr"
        )));
    }
    if let Some(value) = js_non_null_field(object, "alias") {
        ensure_only_js_fields(object, &["alias"], context)?;
        return Ok(GraphShortestPathEndpoint::Alias(
            value.as_str().map(ToString::to_string).ok_or_else(|| {
                napi::Error::from_reason(format!("{context} alias must be a string"))
            })?,
        ));
    }
    if let Some(value) = js_non_null_field(object, "nodeId") {
        ensure_only_js_fields(object, &["nodeId"], context)?;
        return Ok(GraphShortestPathEndpoint::NodeId(js_number_to_u64(
            value, context,
        )?));
    }
    if let Some(value) = js_non_null_field(object, "nodeKey") {
        ensure_only_js_fields(object, &["nodeKey"], context)?;
        let payload = js_object(value, &format!("{context} nodeKey"))?;
        ensure_only_js_fields(payload, &["label", "key"], &format!("{context} nodeKey"))?;
        return Ok(GraphShortestPathEndpoint::NodeKey {
            label: parse_js_required_string_field(
                payload,
                "label",
                &format!("{context} nodeKey label"),
            )?,
            key: parse_js_required_string_field(payload, "key", &format!("{context} nodeKey key"))?,
        });
    }
    ensure_only_js_fields(object, &["expr"], context)?;
    Ok(GraphShortestPathEndpoint::Expr(parse_js_graph_expr(
        js_non_null_field(object, "expr")
            .ok_or_else(|| napi::Error::from_reason(format!("{context} requires expr")))?,
        &format!("{context} expr"),
    )?))
}

fn parse_js_graph_pipeline_options(
    value: Option<&serde_json::Value>,
) -> Result<GraphPipelineOptions> {
    let Some(value) = value else {
        return Ok(GraphPipelineOptions::default());
    };
    let object = js_object(value, "graph pipeline options")?;
    ensure_only_js_fields(
        object,
        &[
            "allowFullScan",
            "maxRows",
            "maxPipelineRows",
            "maxGroups",
            "maxCollectItems",
            "maxUnionBranches",
            "maxSubqueryInvocations",
            "maxSubqueryDepth",
            "maxShortestPathPairs",
            "maxIntermediateBindings",
            "maxFrontier",
            "maxPathHops",
            "maxPathsPerStart",
            "maxOrderMaterialization",
            "maxSkip",
            "maxCursorBytes",
            "maxQueryBytes",
            "maxParamBytes",
            "maxAstDepth",
            "maxLiteralItems",
            "includePlan",
            "profile",
        ],
        "graph pipeline options",
    )?;
    let mut options = GraphPipelineOptions::default();
    if let Some(value) = parse_js_optional_bool_field(
        object,
        "allowFullScan",
        "graph pipeline options allowFullScan",
    )? {
        options.allow_full_scan = value;
    }
    if let Some(value) = js_non_null_field(object, "maxRows") {
        options.max_rows = js_number_to_usize(value, "graph pipeline options maxRows")?;
    }
    if let Some(value) = js_non_null_field(object, "maxPipelineRows") {
        options.max_pipeline_rows =
            js_number_to_usize(value, "graph pipeline options maxPipelineRows")?;
    }
    if let Some(value) = js_non_null_field(object, "maxGroups") {
        options.max_groups = js_number_to_usize(value, "graph pipeline options maxGroups")?;
    }
    if let Some(value) = js_non_null_field(object, "maxCollectItems") {
        options.max_collect_items =
            js_number_to_usize(value, "graph pipeline options maxCollectItems")?;
    }
    if let Some(value) = js_non_null_field(object, "maxUnionBranches") {
        options.max_union_branches =
            js_number_to_usize(value, "graph pipeline options maxUnionBranches")?;
    }
    if let Some(value) = js_non_null_field(object, "maxSubqueryInvocations") {
        options.max_subquery_invocations =
            js_number_to_usize(value, "graph pipeline options maxSubqueryInvocations")?;
    }
    if let Some(value) = js_non_null_field(object, "maxSubqueryDepth") {
        options.max_subquery_depth =
            js_number_to_usize(value, "graph pipeline options maxSubqueryDepth")?;
    }
    if let Some(value) = js_non_null_field(object, "maxShortestPathPairs") {
        options.max_shortest_path_pairs =
            js_number_to_usize(value, "graph pipeline options maxShortestPathPairs")?;
    }
    if let Some(value) = js_non_null_field(object, "maxIntermediateBindings") {
        options.max_intermediate_bindings =
            js_number_to_usize(value, "graph pipeline options maxIntermediateBindings")?;
    }
    if let Some(value) = js_non_null_field(object, "maxFrontier") {
        options.max_frontier = js_number_to_usize(value, "graph pipeline options maxFrontier")?;
    }
    if let Some(value) = js_non_null_field(object, "maxPathHops") {
        options.max_path_hops = parse_js_u8_number(value, "graph pipeline options maxPathHops")?;
    }
    if let Some(value) = js_non_null_field(object, "maxPathsPerStart") {
        options.max_paths_per_start =
            js_number_to_usize(value, "graph pipeline options maxPathsPerStart")?;
    }
    if let Some(value) = js_non_null_field(object, "maxOrderMaterialization") {
        options.max_order_materialization =
            js_number_to_usize(value, "graph pipeline options maxOrderMaterialization")?;
    }
    if let Some(value) = js_non_null_field(object, "maxSkip") {
        options.max_skip = js_number_to_usize(value, "graph pipeline options maxSkip")?;
    }
    if let Some(value) = js_non_null_field(object, "maxCursorBytes") {
        options.max_cursor_bytes =
            js_number_to_usize(value, "graph pipeline options maxCursorBytes")?;
    }
    if let Some(value) = js_non_null_field(object, "maxQueryBytes") {
        options.max_query_bytes =
            js_number_to_usize(value, "graph pipeline options maxQueryBytes")?;
    }
    if let Some(value) = js_non_null_field(object, "maxParamBytes") {
        options.max_param_bytes =
            js_number_to_usize(value, "graph pipeline options maxParamBytes")?;
    }
    if let Some(value) = js_non_null_field(object, "maxAstDepth") {
        options.max_ast_depth = js_number_to_usize(value, "graph pipeline options maxAstDepth")?;
    }
    if let Some(value) = js_non_null_field(object, "maxLiteralItems") {
        options.max_literal_items =
            js_number_to_usize(value, "graph pipeline options maxLiteralItems")?;
    }
    if let Some(value) =
        parse_js_optional_bool_field(object, "includePlan", "graph pipeline options includePlan")?
    {
        options.include_plan = value;
    }
    if let Some(value) =
        parse_js_optional_bool_field(object, "profile", "graph pipeline options profile")?
    {
        options.profile = value;
    }
    Ok(options)
}

fn parse_js_graph_node_pattern(
    value: &serde_json::Value,
    context: &str,
) -> Result<CoreGraphNodePattern> {
    let object = js_object(value, context)?;
    ensure_only_js_fields(
        object,
        &[
            "alias",
            "labelFilter",
            "ids",
            "keys",
            "filter",
            "where",
            "predicates",
        ],
        context,
    )?;
    let label_filter =
        parse_js_node_label_filter_field(object, "labelFilter", &format!("{context} labelFilter"))?;
    let keys = parse_js_optional_node_keys_field(
        object,
        "keys",
        &format!("{context} keys"),
        label_filter.as_ref(),
    )?;
    Ok(CoreGraphNodePattern {
        alias: parse_js_required_string_field(object, "alias", &format!("{context} alias"))?,
        label_filter,
        ids: parse_js_optional_u64_array_field(object, "ids", &format!("{context} ids"))?,
        keys,
        filter: parse_js_node_filter(object, "updatedAt", context)?,
    })
}

fn parse_js_graph_piece(value: &serde_json::Value, context: &str) -> Result<GraphPatternPiece> {
    let object = js_object(value, context)?;
    let kind = parse_js_required_string_field(object, "kind", &format!("{context} kind"))?;
    match kind.as_str() {
        "edge" => parse_js_graph_edge_pattern(object, context).map(GraphPatternPiece::Edge),
        "optional" => {
            parse_js_graph_optional_group(object, context).map(GraphPatternPiece::Optional)
        }
        "variableLength" => parse_js_graph_variable_length_pattern(object, context)
            .map(GraphPatternPiece::VariableLength),
        other => Err(napi::Error::from_reason(format!(
            "{context} kind must be 'edge', 'optional', or 'variableLength', got '{other}'"
        ))),
    }
}

fn parse_js_graph_edge_pattern(
    object: &serde_json::Map<String, serde_json::Value>,
    context: &str,
) -> Result<CoreGraphEdgePattern> {
    ensure_only_js_fields(
        object,
        &[
            "kind",
            "alias",
            "fromAlias",
            "toAlias",
            "direction",
            "labelFilter",
            "filter",
        ],
        context,
    )?;
    let direction = match js_non_null_field(object, "direction") {
        None => Direction::Outgoing,
        Some(value) => parse_direction(Some(value.as_str().ok_or_else(|| {
            napi::Error::from_reason(format!("{context} direction must be a string"))
        })?))?,
    };
    Ok(CoreGraphEdgePattern {
        alias: parse_js_optional_string_field(object, "alias", &format!("{context} alias"))?,
        from_alias: parse_js_required_string_field(
            object,
            "fromAlias",
            &format!("{context} fromAlias"),
        )?,
        to_alias: parse_js_required_string_field(object, "toAlias", &format!("{context} toAlias"))?,
        direction,
        label_filter: parse_js_optional_string_array_field(
            object,
            "labelFilter",
            &format!("{context} labelFilter"),
        )?,
        filter: parse_js_edge_filter(
            object,
            "updatedAt",
            "validAt",
            "validFrom",
            "validTo",
            context,
        )?,
    })
}

fn parse_js_graph_optional_group(
    object: &serde_json::Map<String, serde_json::Value>,
    context: &str,
) -> Result<GraphOptionalGroup> {
    ensure_only_js_fields(object, &["kind", "pieces", "where"], context)?;
    let pieces = match js_non_null_field(object, "pieces") {
        Some(value) => js_array(value, &format!("{context} pieces"))?
            .iter()
            .enumerate()
            .map(|(index, value)| {
                parse_js_graph_piece(value, &format!("{context} pieces[{index}]"))
            })
            .collect::<Result<Vec<_>>>()?,
        None => {
            return Err(napi::Error::from_reason(format!(
                "{context} requires pieces"
            )));
        }
    };
    Ok(GraphOptionalGroup {
        pieces,
        where_: js_non_null_field(object, "where")
            .map(|value| parse_js_graph_expr(value, &format!("{context} where")))
            .transpose()?,
    })
}

fn parse_js_graph_variable_length_pattern(
    object: &serde_json::Map<String, serde_json::Value>,
    context: &str,
) -> Result<GraphVariableLengthPattern> {
    ensure_only_js_fields(
        object,
        &[
            "kind",
            "pathAlias",
            "edgeAlias",
            "fromAlias",
            "toAlias",
            "direction",
            "labelFilter",
            "filter",
            "minHops",
            "maxHops",
        ],
        context,
    )?;
    let direction = match js_non_null_field(object, "direction") {
        None => Direction::Outgoing,
        Some(value) => parse_direction(Some(value.as_str().ok_or_else(|| {
            napi::Error::from_reason(format!("{context} direction must be a string"))
        })?))?,
    };
    Ok(GraphVariableLengthPattern {
        path_alias: parse_js_optional_string_field(
            object,
            "pathAlias",
            &format!("{context} pathAlias"),
        )?,
        edge_alias: parse_js_optional_string_field(
            object,
            "edgeAlias",
            &format!("{context} edgeAlias"),
        )?,
        from_alias: parse_js_required_string_field(
            object,
            "fromAlias",
            &format!("{context} fromAlias"),
        )?,
        to_alias: parse_js_required_string_field(object, "toAlias", &format!("{context} toAlias"))?,
        direction,
        label_filter: parse_js_optional_string_array_field(
            object,
            "labelFilter",
            &format!("{context} labelFilter"),
        )?,
        filter: parse_js_edge_filter(
            object,
            "updatedAt",
            "validAt",
            "validFrom",
            "validTo",
            context,
        )?,
        min_hops: parse_js_required_u8_field(object, "minHops", &format!("{context} minHops"))?,
        max_hops: parse_js_required_u8_field(object, "maxHops", &format!("{context} maxHops"))?,
    })
}

fn parse_js_graph_return_item(value: &serde_json::Value, context: &str) -> Result<GraphReturnItem> {
    let object = js_object(value, context)?;
    ensure_only_js_fields(object, &["expr", "as", "projection"], context)?;
    let expr_value = js_non_null_field(object, "expr")
        .ok_or_else(|| napi::Error::from_reason(format!("{context} expr is required")))?;
    Ok(GraphReturnItem {
        expr: parse_js_graph_expr(expr_value, &format!("{context} expr"))?,
        alias: parse_js_optional_string_field(object, "as", &format!("{context} as"))?,
        projection: parse_js_graph_return_projection(
            js_non_null_field(object, "projection"),
            context,
        )?,
    })
}

fn parse_js_graph_order_item(value: &serde_json::Value, context: &str) -> Result<GraphOrderItem> {
    let object = js_object(value, context)?;
    ensure_only_js_fields(object, &["expr", "direction"], context)?;
    let expr_value = js_non_null_field(object, "expr")
        .ok_or_else(|| napi::Error::from_reason(format!("{context} expr is required")))?;
    let direction =
        match parse_js_optional_string_field(object, "direction", &format!("{context} direction"))?
            .as_deref()
        {
            None | Some("asc") => GraphOrderDirection::Asc,
            Some("desc") => GraphOrderDirection::Desc,
            Some(other) => {
                return Err(napi::Error::from_reason(format!(
                    "{context} direction must be 'asc' or 'desc', got '{other}'"
                )));
            }
        };
    Ok(GraphOrderItem {
        expr: parse_js_graph_expr(expr_value, &format!("{context} expr"))?,
        direction,
    })
}

fn parse_js_graph_return_projection(
    value: Option<&serde_json::Value>,
    context: &str,
) -> Result<GraphReturnProjection> {
    let Some(value) = value else {
        return Ok(GraphReturnProjection::Auto);
    };
    if let Some(name) = value.as_str() {
        return match name {
            "auto" => Ok(GraphReturnProjection::Auto),
            "id" | "idOnly" => Ok(GraphReturnProjection::IdOnly),
            "element" | "full" => Ok(GraphReturnProjection::Element(GraphElementProjection::Full)),
            "compact" => Ok(GraphReturnProjection::Element(GraphElementProjection::Compact)),
            other => Err(napi::Error::from_reason(format!(
                "{context} projection must be 'auto', 'idOnly', 'element', 'full', or 'compact', got '{other}'"
            ))),
        };
    }
    let object = js_object(value, &format!("{context} projection"))?;
    let selectors = ["element", "selected"]
        .iter()
        .filter(|field| object.contains_key(**field))
        .count();
    if selectors != 1 {
        return Err(napi::Error::from_reason(format!(
            "{context} projection must contain exactly one of 'element' or 'selected'"
        )));
    }
    ensure_only_js_fields(
        object,
        &["element", "selected"],
        &format!("{context} projection"),
    )?;
    if let Some(element) = js_non_null_field(object, "element") {
        let mode = element.as_str().ok_or_else(|| {
            napi::Error::from_reason(format!("{context} projection element must be a string"))
        })?;
        return Ok(GraphReturnProjection::Element(match mode {
            "id" | "idOnly" => GraphElementProjection::IdOnly,
            "compact" => GraphElementProjection::Compact,
            "full" => GraphElementProjection::Full,
            other => {
                return Err(napi::Error::from_reason(format!(
                    "{context} projection element must be 'idOnly', 'compact', or 'full', got '{other}'"
                )));
            }
        }));
    }
    let selected = js_non_null_field(object, "selected").ok_or_else(|| {
        napi::Error::from_reason(format!("{context} selected projection is required"))
    })?;
    Ok(GraphReturnProjection::Selected(
        parse_js_selected_projection(selected, &format!("{context} selected"))?,
    ))
}

fn parse_js_selected_projection(
    value: &serde_json::Value,
    context: &str,
) -> Result<GraphSelectedProjection> {
    let object = js_object(value, context)?;
    let selectors = ["node", "edge", "path"]
        .iter()
        .filter(|field| object.contains_key(**field))
        .count();
    if selectors != 1 {
        return Err(napi::Error::from_reason(format!(
            "{context} must contain exactly one of 'node', 'edge', or 'path'"
        )));
    }
    ensure_only_js_fields(object, &["node", "edge", "path"], context)?;
    if let Some(value) = js_non_null_field(object, "node") {
        return Ok(GraphSelectedProjection::Node(
            parse_js_selected_node_projection(value, &format!("{context} node"))?,
        ));
    }
    if let Some(value) = js_non_null_field(object, "edge") {
        return Ok(GraphSelectedProjection::Edge(
            parse_js_selected_edge_projection(value, &format!("{context} edge"))?,
        ));
    }
    let value = js_non_null_field(object, "path").ok_or_else(|| {
        napi::Error::from_reason(format!("{context} path projection is required"))
    })?;
    Ok(GraphSelectedProjection::Path(
        parse_js_selected_path_projection(value, &format!("{context} path"))?,
    ))
}

fn parse_js_selected_node_projection(
    value: &serde_json::Value,
    context: &str,
) -> Result<GraphSelectedNodeProjection> {
    let object = js_object(value, context)?;
    ensure_only_js_fields(
        object,
        &[
            "id",
            "labels",
            "key",
            "props",
            "weight",
            "createdAt",
            "updatedAt",
            "vectors",
        ],
        context,
    )?;
    Ok(GraphSelectedNodeProjection {
        id: parse_js_optional_bool_field(object, "id", &format!("{context} id"))?.unwrap_or(false),
        labels: parse_js_optional_bool_field(object, "labels", &format!("{context} labels"))?
            .unwrap_or(false),
        key: parse_js_optional_bool_field(object, "key", &format!("{context} key"))?
            .unwrap_or(false),
        props: parse_js_property_selection(js_non_null_field(object, "props"), context)?,
        weight: parse_js_optional_bool_field(object, "weight", &format!("{context} weight"))?
            .unwrap_or(false),
        created_at: parse_js_optional_bool_field(
            object,
            "createdAt",
            &format!("{context} createdAt"),
        )?
        .unwrap_or(false),
        updated_at: parse_js_optional_bool_field(
            object,
            "updatedAt",
            &format!("{context} updatedAt"),
        )?
        .unwrap_or(false),
        vectors: parse_js_vector_selection(js_non_null_field(object, "vectors"), context)?,
    })
}

fn parse_js_selected_edge_projection(
    value: &serde_json::Value,
    context: &str,
) -> Result<GraphSelectedEdgeProjection> {
    let object = js_object(value, context)?;
    ensure_only_js_fields(
        object,
        &[
            "id",
            "from",
            "to",
            "label",
            "props",
            "weight",
            "createdAt",
            "updatedAt",
            "validFrom",
            "validTo",
        ],
        context,
    )?;
    Ok(GraphSelectedEdgeProjection {
        id: parse_js_optional_bool_field(object, "id", &format!("{context} id"))?.unwrap_or(false),
        from: parse_js_optional_bool_field(object, "from", &format!("{context} from"))?
            .unwrap_or(false),
        to: parse_js_optional_bool_field(object, "to", &format!("{context} to"))?.unwrap_or(false),
        label: parse_js_optional_bool_field(object, "label", &format!("{context} label"))?
            .unwrap_or(false),
        props: parse_js_property_selection(js_non_null_field(object, "props"), context)?,
        weight: parse_js_optional_bool_field(object, "weight", &format!("{context} weight"))?
            .unwrap_or(false),
        created_at: parse_js_optional_bool_field(
            object,
            "createdAt",
            &format!("{context} createdAt"),
        )?
        .unwrap_or(false),
        updated_at: parse_js_optional_bool_field(
            object,
            "updatedAt",
            &format!("{context} updatedAt"),
        )?
        .unwrap_or(false),
        valid_from: parse_js_optional_bool_field(
            object,
            "validFrom",
            &format!("{context} validFrom"),
        )?
        .unwrap_or(false),
        valid_to: parse_js_optional_bool_field(object, "validTo", &format!("{context} validTo"))?
            .unwrap_or(false),
    })
}

fn parse_js_selected_path_projection(
    value: &serde_json::Value,
    context: &str,
) -> Result<GraphSelectedPathProjection> {
    let object = js_object(value, context)?;
    ensure_only_js_fields(object, &["nodeIds", "edgeIds", "nodes", "edges"], context)?;
    Ok(GraphSelectedPathProjection {
        node_ids: parse_js_optional_bool_field(object, "nodeIds", &format!("{context} nodeIds"))?
            .unwrap_or(false),
        edge_ids: parse_js_optional_bool_field(object, "edgeIds", &format!("{context} edgeIds"))?
            .unwrap_or(false),
        nodes: js_non_null_field(object, "nodes")
            .map(|value| parse_js_selected_node_projection(value, &format!("{context} nodes")))
            .transpose()?,
        edges: js_non_null_field(object, "edges")
            .map(|value| parse_js_selected_edge_projection(value, &format!("{context} edges")))
            .transpose()?,
    })
}

fn parse_js_property_selection(
    value: Option<&serde_json::Value>,
    context: &str,
) -> Result<GraphPropertySelection> {
    let Some(value) = value else {
        return Ok(GraphPropertySelection::None);
    };
    if let Some(value) = value.as_bool() {
        return Ok(if value {
            GraphPropertySelection::All
        } else {
            GraphPropertySelection::None
        });
    }
    if let Some(mode) = value.as_str() {
        return match mode {
            "all" => Ok(GraphPropertySelection::All),
            "none" => Ok(GraphPropertySelection::None),
            other => Err(napi::Error::from_reason(format!(
                "{context} props must be true, false, 'all', 'none', or string[]; got '{other}'"
            ))),
        };
    }
    Ok(GraphPropertySelection::Keys(
        js_array(value, &format!("{context} props"))?
            .iter()
            .enumerate()
            .map(|(index, value)| {
                value.as_str().map(ToString::to_string).ok_or_else(|| {
                    napi::Error::from_reason(format!("{context} props[{index}] must be a string"))
                })
            })
            .collect::<Result<Vec<_>>>()?,
    ))
}

fn parse_js_vector_selection(
    value: Option<&serde_json::Value>,
    context: &str,
) -> Result<GraphVectorSelection> {
    let Some(value) = value else {
        return Ok(GraphVectorSelection::None);
    };
    if let Some(value) = value.as_bool() {
        return Ok(if value {
            GraphVectorSelection::Both
        } else {
            GraphVectorSelection::None
        });
    }
    let mode = value.as_str().ok_or_else(|| {
        napi::Error::from_reason(format!("{context} vectors must be a boolean or string"))
    })?;
    match mode {
        "none" => Ok(GraphVectorSelection::None),
        "dense" => Ok(GraphVectorSelection::Dense),
        "sparse" => Ok(GraphVectorSelection::Sparse),
        "both" => Ok(GraphVectorSelection::Both),
        other => Err(napi::Error::from_reason(format!(
            "{context} vectors must be 'none', 'dense', 'sparse', or 'both', got '{other}'"
        ))),
    }
}

fn parse_js_graph_expr(value: &serde_json::Value, context: &str) -> Result<GraphExpr> {
    match value {
        serde_json::Value::Null => Ok(GraphExpr::Null),
        serde_json::Value::Bool(value) => Ok(GraphExpr::Bool(*value)),
        serde_json::Value::Number(_) => parse_js_graph_number_expr(value, context),
        serde_json::Value::String(value) => Ok(GraphExpr::String(value.clone())),
        serde_json::Value::Array(_) => Err(napi::Error::from_reason(format!(
            "{context} array literals must use {{ list: [...] }}"
        ))),
        serde_json::Value::Object(object) => parse_js_graph_expr_object(object, context),
    }
}

fn parse_js_graph_number_expr(value: &serde_json::Value, context: &str) -> Result<GraphExpr> {
    let number = value
        .as_f64()
        .ok_or_else(|| napi::Error::from_reason(format!("{context} must be a finite number")))?;
    if !number.is_finite() {
        return Err(napi::Error::from_reason(format!(
            "{context} numeric literals must be finite"
        )));
    }
    if number.fract() == 0.0 && number.abs() <= MAX_SAFE_INTEGER {
        if number < 0.0 {
            Ok(GraphExpr::Int(number as i64))
        } else {
            Ok(GraphExpr::UInt(number as u64))
        }
    } else {
        Ok(GraphExpr::Float(number))
    }
}

fn parse_js_graph_expr_object(
    object: &serde_json::Map<String, serde_json::Value>,
    context: &str,
) -> Result<GraphExpr> {
    let tags = [
        "bytes",
        "list",
        "map",
        "param",
        "binding",
        "property",
        "nodeField",
        "edgeField",
        "pathField",
        "fn",
        "aggregate",
        "exists",
        "op",
        "case",
        "isNull",
        "isNotNull",
    ];
    let tag_count = tags.iter().filter(|tag| object.contains_key(**tag)).count();
    if tag_count != 1 {
        return Err(napi::Error::from_reason(format!(
            "{context} expression object must contain exactly one known expression tag"
        )));
    }
    if let Some(value) = object.get("bytes") {
        ensure_only_js_fields(object, &["bytes"], context)?;
        return Ok(GraphExpr::Bytes(parse_js_byte_array(
            value,
            &format!("{context} bytes"),
        )?));
    }
    if let Some(value) = object.get("list") {
        ensure_only_js_fields(object, &["list"], context)?;
        return Ok(GraphExpr::List(
            js_array(value, &format!("{context} list"))?
                .iter()
                .enumerate()
                .map(|(index, value)| {
                    parse_js_graph_expr(value, &format!("{context} list[{index}]"))
                })
                .collect::<Result<Vec<_>>>()?,
        ));
    }
    if let Some(value) = object.get("map") {
        ensure_only_js_fields(object, &["map"], context)?;
        let map = js_object(value, &format!("{context} map"))?;
        return Ok(GraphExpr::Map(
            map.iter()
                .map(|(key, value)| {
                    Ok((
                        key.clone(),
                        parse_js_graph_expr(value, &format!("{context} map.{key}"))?,
                    ))
                })
                .collect::<Result<BTreeMap<_, _>>>()?,
        ));
    }
    if let Some(value) = object.get("param") {
        ensure_only_js_fields(object, &["param"], context)?;
        return Ok(GraphExpr::Param(
            value.as_str().map(ToString::to_string).ok_or_else(|| {
                napi::Error::from_reason(format!("{context} param must be a string"))
            })?,
        ));
    }
    if let Some(value) = object.get("binding") {
        ensure_only_js_fields(object, &["binding"], context)?;
        return Ok(GraphExpr::Binding(
            value.as_str().map(ToString::to_string).ok_or_else(|| {
                napi::Error::from_reason(format!("{context} binding must be a string"))
            })?,
        ));
    }
    if let Some(value) = object.get("property") {
        ensure_only_js_fields(object, &["property"], context)?;
        let payload = js_object(value, &format!("{context} property"))?;
        ensure_only_js_fields(payload, &["alias", "key"], &format!("{context} property"))?;
        return Ok(GraphExpr::Property {
            alias: parse_js_required_string_field(
                payload,
                "alias",
                &format!("{context} property alias"),
            )?,
            key: parse_js_required_string_field(
                payload,
                "key",
                &format!("{context} property key"),
            )?,
        });
    }
    if let Some(value) = object.get("nodeField") {
        ensure_only_js_fields(object, &["nodeField"], context)?;
        let (alias, field) = parse_js_field_payload(value, "nodeField", context)?;
        return Ok(GraphExpr::NodeField {
            alias,
            field: parse_js_graph_node_field(&field, context)?,
        });
    }
    if let Some(value) = object.get("edgeField") {
        ensure_only_js_fields(object, &["edgeField"], context)?;
        let (alias, field) = parse_js_field_payload(value, "edgeField", context)?;
        return Ok(GraphExpr::EdgeField {
            alias,
            field: parse_js_graph_edge_field(&field, context)?,
        });
    }
    if let Some(value) = object.get("pathField") {
        ensure_only_js_fields(object, &["pathField"], context)?;
        let (alias, field) = parse_js_field_payload(value, "pathField", context)?;
        return Ok(GraphExpr::PathField {
            alias,
            field: parse_js_graph_path_field(&field, context)?,
        });
    }
    if object.contains_key("fn") {
        ensure_only_js_fields(object, &["fn", "args"], context)?;
        return parse_js_graph_function_expr(object, context);
    }
    if let Some(value) = object.get("aggregate") {
        ensure_only_js_fields(object, &["aggregate"], context)?;
        return parse_js_graph_aggregate_expr(value, context);
    }
    if let Some(value) = object.get("exists") {
        ensure_only_js_fields(object, &["exists"], context)?;
        let payload = js_object(value, &format!("{context} exists"))?;
        let stage = parse_js_graph_pipeline_call_stage(payload, &format!("{context} exists"))?;
        return Ok(GraphExpr::ExistsSubquery(stage));
    }
    if object.contains_key("op") {
        ensure_only_js_fields(object, &["op", "left", "right", "expr"], context)?;
        return parse_js_graph_op_expr(object, context);
    }
    if let Some(value) = object.get("case") {
        ensure_only_js_fields(object, &["case"], context)?;
        return parse_js_graph_case_expr(value, context);
    }
    if let Some(value) = object.get("isNull") {
        ensure_only_js_fields(object, &["isNull"], context)?;
        return Ok(GraphExpr::IsNull(Box::new(parse_js_graph_expr(
            value,
            &format!("{context} isNull"),
        )?)));
    }
    if let Some(value) = object.get("isNotNull") {
        ensure_only_js_fields(object, &["isNotNull"], context)?;
        return Ok(GraphExpr::IsNotNull(Box::new(parse_js_graph_expr(
            value,
            &format!("{context} isNotNull"),
        )?)));
    }
    unreachable!("tag count was checked above")
}

fn parse_js_field_payload(
    value: &serde_json::Value,
    tag: &str,
    context: &str,
) -> Result<(String, String)> {
    let payload = js_object(value, &format!("{context} {tag}"))?;
    ensure_only_js_fields(payload, &["alias", "field"], &format!("{context} {tag}"))?;
    Ok((
        parse_js_required_string_field(payload, "alias", &format!("{context} {tag} alias"))?,
        parse_js_required_string_field(payload, "field", &format!("{context} {tag} field"))?,
    ))
}

fn parse_js_graph_function_expr(
    object: &serde_json::Map<String, serde_json::Value>,
    context: &str,
) -> Result<GraphExpr> {
    let name = object
        .get("fn")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| napi::Error::from_reason(format!("{context} fn must be a string")))?;
    let args_value = object
        .get("args")
        .ok_or_else(|| napi::Error::from_reason(format!("{context} fn args are required")))?;
    let args = js_array(args_value, &format!("{context} args"))?
        .iter()
        .enumerate()
        .map(|(index, value)| parse_js_graph_expr(value, &format!("{context} args[{index}]")))
        .collect::<Result<Vec<_>>>()?;
    if matches!(name, "nodeIds" | "node_ids" | "edgeIds" | "edge_ids") {
        if args.len() != 1 {
            return Err(napi::Error::from_reason(format!(
                "{context} {name}() requires exactly one path binding argument"
            )));
        }
        let GraphExpr::Binding(alias) = args.into_iter().next().unwrap() else {
            return Err(napi::Error::from_reason(format!(
                "{context} {name}() currently requires a direct path binding argument"
            )));
        };
        return Ok(GraphExpr::PathField {
            alias,
            field: if matches!(name, "nodeIds" | "node_ids") {
                GraphPathField::NodeIds
            } else {
                GraphPathField::EdgeIds
            },
        });
    }
    Ok(GraphExpr::Function {
        name: match name {
            "id" => GraphFunction::Id,
            "labels" => GraphFunction::Labels,
            "type" => GraphFunction::Type,
            "length" => GraphFunction::Length,
            "startNode" | "start_node" => GraphFunction::StartNode,
            "endNode" | "end_node" => GraphFunction::EndNode,
            "nodes" => GraphFunction::Nodes,
            "relationships" => GraphFunction::Relationships,
            "coalesce" => GraphFunction::Coalesce,
            "toString" | "to_string" => GraphFunction::ToString,
            "toInteger" | "to_integer" => GraphFunction::ToInteger,
            "toFloat" | "to_float" => GraphFunction::ToFloat,
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
                return Err(napi::Error::from_reason(format!(
                    "{context} unsupported graph function '{other}'"
                )));
            }
        },
        args,
    })
}

fn parse_js_graph_aggregate_expr(value: &serde_json::Value, context: &str) -> Result<GraphExpr> {
    let payload = js_object(value, &format!("{context} aggregate"))?;
    ensure_only_js_fields(
        payload,
        &["function", "distinct", "arg"],
        &format!("{context} aggregate"),
    )?;
    let function = parse_js_required_string_field(
        payload,
        "function",
        &format!("{context} aggregate function"),
    )?;
    let function = match function.as_str() {
        "count" => GraphAggregateFunction::Count,
        "sum" => GraphAggregateFunction::Sum,
        "avg" => GraphAggregateFunction::Avg,
        "min" => GraphAggregateFunction::Min,
        "max" => GraphAggregateFunction::Max,
        "collect" => GraphAggregateFunction::Collect,
        other => {
            return Err(napi::Error::from_reason(format!(
                "{context} aggregate function is unsupported: '{other}'"
            )));
        }
    };
    Ok(GraphExpr::AggregateCall {
        function,
        distinct: parse_js_optional_bool_field(
            payload,
            "distinct",
            &format!("{context} aggregate distinct"),
        )?
        .unwrap_or(false),
        arg: js_non_null_field(payload, "arg")
            .map(|value| parse_js_graph_expr(value, &format!("{context} aggregate arg")))
            .transpose()?
            .map(Box::new),
    })
}

fn parse_js_graph_case_expr(value: &serde_json::Value, context: &str) -> Result<GraphExpr> {
    let payload = js_object(value, &format!("{context} case"))?;
    ensure_only_js_fields(
        payload,
        &["operand", "branches", "else"],
        &format!("{context} case"),
    )?;
    let branches_value = js_non_null_field(payload, "branches")
        .ok_or_else(|| napi::Error::from_reason(format!("{context} case requires branches")))?;
    let branches = js_array(branches_value, &format!("{context} case branches"))?
        .iter()
        .enumerate()
        .map(|(index, value)| {
            let item = js_object(value, &format!("{context} case branches[{index}]"))?;
            ensure_only_js_fields(
                item,
                &["when", "then"],
                &format!("{context} case branches[{index}]"),
            )?;
            let when = js_non_null_field(item, "when").ok_or_else(|| {
                napi::Error::from_reason(format!("{context} case branches[{index}] requires when"))
            })?;
            let then = js_non_null_field(item, "then").ok_or_else(|| {
                napi::Error::from_reason(format!("{context} case branches[{index}] requires then"))
            })?;
            Ok(GraphCaseBranch {
                when: parse_js_graph_expr(when, &format!("{context} case branches[{index}] when"))?,
                then: parse_js_graph_expr(then, &format!("{context} case branches[{index}] then"))?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(GraphExpr::Case {
        operand: js_non_null_field(payload, "operand")
            .map(|value| parse_js_graph_expr(value, &format!("{context} case operand")))
            .transpose()?
            .map(Box::new),
        branches,
        else_expr: js_non_null_field(payload, "else")
            .map(|value| parse_js_graph_expr(value, &format!("{context} case else")))
            .transpose()?
            .map(Box::new),
    })
}

fn parse_js_graph_op_expr(
    object: &serde_json::Map<String, serde_json::Value>,
    context: &str,
) -> Result<GraphExpr> {
    let op = object
        .get("op")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| napi::Error::from_reason(format!("{context} op must be a string")))?;
    if op == "not" {
        let expr = object
            .get("expr")
            .ok_or_else(|| napi::Error::from_reason(format!("{context} not expr is required")))?;
        if object.contains_key("left") || object.contains_key("right") {
            return Err(napi::Error::from_reason(format!(
                "{context} not expression must not contain left or right"
            )));
        }
        return Ok(GraphExpr::Unary {
            op: GraphUnaryOp::Not,
            expr: Box::new(parse_js_graph_expr(expr, &format!("{context} expr"))?),
        });
    }
    if op == "neg" || op == "-" && object.contains_key("expr") {
        let expr = object
            .get("expr")
            .ok_or_else(|| napi::Error::from_reason(format!("{context} neg expr is required")))?;
        if object.contains_key("left") || object.contains_key("right") {
            return Err(napi::Error::from_reason(format!(
                "{context} neg expression must not contain left or right"
            )));
        }
        return Ok(GraphExpr::Unary {
            op: GraphUnaryOp::Neg,
            expr: Box::new(parse_js_graph_expr(expr, &format!("{context} expr"))?),
        });
    }
    if object.contains_key("expr") {
        return Err(napi::Error::from_reason(format!(
            "{context} binary expression must not contain expr"
        )));
    }
    let left = object
        .get("left")
        .ok_or_else(|| napi::Error::from_reason(format!("{context} left is required")))?;
    let right = object
        .get("right")
        .ok_or_else(|| napi::Error::from_reason(format!("{context} right is required")))?;
    Ok(GraphExpr::Binary {
        left: Box::new(parse_js_graph_expr(left, &format!("{context} left"))?),
        op: match op {
            "or" => GraphBinaryOp::Or,
            "and" => GraphBinaryOp::And,
            "=" | "==" | "eq" => GraphBinaryOp::Eq,
            "<>" | "!=" | "neq" => GraphBinaryOp::Neq,
            "<" | "lt" => GraphBinaryOp::Lt,
            "<=" | "lte" => GraphBinaryOp::Le,
            ">" | "gt" => GraphBinaryOp::Gt,
            ">=" | "gte" => GraphBinaryOp::Ge,
            "in" => GraphBinaryOp::In,
            "+" | "add" => GraphBinaryOp::Add,
            "-" | "sub" => GraphBinaryOp::Sub,
            "*" | "mul" => GraphBinaryOp::Mul,
            "/" | "div" => GraphBinaryOp::Div,
            "startsWith" | "starts_with" => GraphBinaryOp::StartsWith,
            "endsWith" | "ends_with" => GraphBinaryOp::EndsWith,
            "contains" => GraphBinaryOp::Contains,
            other => {
                return Err(napi::Error::from_reason(format!(
                    "{context} unsupported graph binary op '{other}'"
                )));
            }
        },
        right: Box::new(parse_js_graph_expr(right, &format!("{context} right"))?),
    })
}

fn parse_js_graph_node_field(field: &str, context: &str) -> Result<GraphNodeField> {
    match field {
        "id" => Ok(GraphNodeField::Id),
        "labels" => Ok(GraphNodeField::Labels),
        "key" => Ok(GraphNodeField::Key),
        "weight" => Ok(GraphNodeField::Weight),
        "createdAt" => Ok(GraphNodeField::CreatedAt),
        "updatedAt" => Ok(GraphNodeField::UpdatedAt),
        other => Err(napi::Error::from_reason(format!(
            "{context} unsupported node field '{other}'"
        ))),
    }
}

fn parse_js_graph_edge_field(field: &str, context: &str) -> Result<GraphEdgeField> {
    match field {
        "id" => Ok(GraphEdgeField::Id),
        "from" => Ok(GraphEdgeField::From),
        "to" => Ok(GraphEdgeField::To),
        "label" => Ok(GraphEdgeField::Label),
        "weight" => Ok(GraphEdgeField::Weight),
        "createdAt" => Ok(GraphEdgeField::CreatedAt),
        "updatedAt" => Ok(GraphEdgeField::UpdatedAt),
        "validFrom" => Ok(GraphEdgeField::ValidFrom),
        "validTo" => Ok(GraphEdgeField::ValidTo),
        other => Err(napi::Error::from_reason(format!(
            "{context} unsupported edge field '{other}'"
        ))),
    }
}

fn parse_js_graph_path_field(field: &str, context: &str) -> Result<GraphPathField> {
    match field {
        "nodeIds" => Ok(GraphPathField::NodeIds),
        "edgeIds" => Ok(GraphPathField::EdgeIds),
        "length" => Ok(GraphPathField::Length),
        other => Err(napi::Error::from_reason(format!(
            "{context} unsupported path field '{other}'"
        ))),
    }
}

fn parse_js_graph_params(
    value: Option<&serde_json::Value>,
) -> Result<BTreeMap<String, GraphParamValue>> {
    let Some(value) = value else {
        return Ok(BTreeMap::new());
    };
    let object = js_object(value, "graph row params")?;
    object
        .iter()
        .map(|(key, value)| {
            Ok((
                key.clone(),
                parse_js_graph_param_value(value, &format!("graph row params.{key}"))?,
            ))
        })
        .collect()
}

fn parse_js_graph_param_value(value: &serde_json::Value, context: &str) -> Result<GraphParamValue> {
    match value {
        serde_json::Value::Null => Ok(GraphParamValue::Null),
        serde_json::Value::Bool(value) => Ok(GraphParamValue::Bool(*value)),
        serde_json::Value::Number(_) => {
            let number = value
                .as_f64()
                .ok_or_else(|| napi::Error::from_reason(format!("{context} must be a number")))?;
            if !number.is_finite() {
                return Err(napi::Error::from_reason(format!(
                    "{context} numeric params must be finite"
                )));
            }
            if number.fract() == 0.0 && number.abs() <= MAX_SAFE_INTEGER {
                if number < 0.0 {
                    Ok(GraphParamValue::Int(number as i64))
                } else {
                    Ok(GraphParamValue::UInt(number as u64))
                }
            } else {
                Ok(GraphParamValue::Float(number))
            }
        }
        serde_json::Value::String(value) => Ok(GraphParamValue::String(value.clone())),
        serde_json::Value::Array(_) => Err(napi::Error::from_reason(format!(
            "{context} list params must use {{ list: [...] }}"
        ))),
        serde_json::Value::Object(object) => {
            let tags = ["bytes", "list", "map"]
                .iter()
                .filter(|tag| object.contains_key(**tag))
                .count();
            if tags != 1 {
                return Err(napi::Error::from_reason(format!(
                    "{context} object params must contain exactly one of bytes, list, or map"
                )));
            }
            if let Some(value) = object.get("bytes") {
                ensure_only_js_fields(object, &["bytes"], context)?;
                return Ok(GraphParamValue::Bytes(parse_js_byte_array(
                    value,
                    &format!("{context} bytes"),
                )?));
            }
            if let Some(value) = object.get("list") {
                ensure_only_js_fields(object, &["list"], context)?;
                return Ok(GraphParamValue::List(
                    js_array(value, &format!("{context} list"))?
                        .iter()
                        .enumerate()
                        .map(|(index, value)| {
                            parse_js_graph_param_value(value, &format!("{context} list[{index}]"))
                        })
                        .collect::<Result<Vec<_>>>()?,
                ));
            }
            let value = object.get("map").unwrap();
            ensure_only_js_fields(object, &["map"], context)?;
            Ok(GraphParamValue::Map(
                js_object(value, &format!("{context} map"))?
                    .iter()
                    .map(|(key, value)| {
                        Ok((
                            key.clone(),
                            parse_js_graph_param_value(value, &format!("{context} map.{key}"))?,
                        ))
                    })
                    .collect::<Result<BTreeMap<_, _>>>()?,
            ))
        }
    }
}

fn parse_js_graph_output_options(value: Option<&serde_json::Value>) -> Result<GraphOutputOptions> {
    let Some(value) = value else {
        return Ok(GraphOutputOptions::default());
    };
    let object = js_object(value, "graph row output")?;
    ensure_only_js_fields(
        object,
        &["mode", "compactRows", "includeVectors"],
        "graph row output",
    )?;
    let mut output = GraphOutputOptions::default();
    if let Some(mode) = parse_js_optional_string_field(object, "mode", "graph row output mode")? {
        output.mode = match mode.as_str() {
            "ids" => GraphOutputMode::Ids,
            "elements" => GraphOutputMode::Elements,
            "projected" => GraphOutputMode::Projected,
            other => {
                return Err(napi::Error::from_reason(format!(
                    "graph row output mode must be 'ids', 'elements', or 'projected', got '{other}'"
                )));
            }
        };
    }
    if let Some(value) =
        parse_js_optional_bool_field(object, "compactRows", "graph row output compactRows")?
    {
        output.compact_rows = value;
    }
    if let Some(value) =
        parse_js_optional_bool_field(object, "includeVectors", "graph row output includeVectors")?
    {
        output.include_vectors = value;
    }
    Ok(output)
}

fn parse_js_graph_query_options(value: Option<&serde_json::Value>) -> Result<GraphQueryOptions> {
    let Some(value) = value else {
        return Ok(GraphQueryOptions::default());
    };
    let object = js_object(value, "graph row options")?;
    ensure_only_js_fields(
        object,
        &[
            "allowFullScan",
            "maxIntermediateBindings",
            "maxFrontier",
            "maxPathHops",
            "maxPathsPerStart",
            "maxPageLimit",
            "maxOrderMaterialization",
            "maxCursorBytes",
            "maxQueryBytes",
            "includePlan",
            "profile",
        ],
        "graph row options",
    )?;
    let mut options = GraphQueryOptions::default();
    if let Some(value) =
        parse_js_optional_bool_field(object, "allowFullScan", "graph row options allowFullScan")?
    {
        options.allow_full_scan = value;
    }
    if let Some(value) = js_non_null_field(object, "maxIntermediateBindings") {
        options.max_intermediate_bindings =
            js_number_to_usize(value, "graph row options maxIntermediateBindings")?;
    }
    if let Some(value) = js_non_null_field(object, "maxFrontier") {
        options.max_frontier = js_number_to_usize(value, "graph row options maxFrontier")?;
    }
    if let Some(value) = js_non_null_field(object, "maxPathHops") {
        options.max_path_hops = parse_js_u8_number(value, "graph row options maxPathHops")?;
    }
    if let Some(value) = js_non_null_field(object, "maxPathsPerStart") {
        options.max_paths_per_start =
            js_number_to_usize(value, "graph row options maxPathsPerStart")?;
    }
    if let Some(value) = js_non_null_field(object, "maxPageLimit") {
        options.max_page_limit = js_number_to_usize(value, "graph row options maxPageLimit")?;
    }
    if let Some(value) = js_non_null_field(object, "maxOrderMaterialization") {
        options.max_order_materialization =
            js_number_to_usize(value, "graph row options maxOrderMaterialization")?;
    }
    if let Some(value) = js_non_null_field(object, "maxCursorBytes") {
        options.max_cursor_bytes = js_number_to_usize(value, "graph row options maxCursorBytes")?;
    }
    if let Some(value) = js_non_null_field(object, "maxQueryBytes") {
        options.max_query_bytes = js_number_to_usize(value, "graph row options maxQueryBytes")?;
    }
    if let Some(value) =
        parse_js_optional_bool_field(object, "includePlan", "graph row options includePlan")?
    {
        options.include_plan = value;
    }
    if let Some(value) =
        parse_js_optional_bool_field(object, "profile", "graph row options profile")?
    {
        options.profile = value;
    }
    Ok(options)
}

fn parse_js_optional_node_keys_field(
    object: &serde_json::Map<String, serde_json::Value>,
    key: &str,
    context: &str,
    label_filter: Option<&CoreNodeLabelFilter>,
) -> Result<Vec<NodeKeyQuery>> {
    let Some(value) = js_non_null_field(object, key) else {
        return Ok(Vec::new());
    };
    js_array(value, context)?
        .iter()
        .enumerate()
        .map(|(index, value)| match value {
            serde_json::Value::String(key) => {
                let label = label_filter
                    .and_then(|filter| {
                        if filter.labels.len() == 1 {
                            Some(filter.labels[0].clone())
                        } else {
                            None
                        }
                    })
                    .ok_or_else(|| {
                        napi::Error::from_reason(format!(
                            "{context}[{index}] string key requires a single-label labelFilter; use {{ label, key }} otherwise"
                        ))
                    })?;
                Ok(NodeKeyQuery {
                    label,
                    key: key.clone(),
                })
            }
            serde_json::Value::Object(object) => {
                ensure_only_js_fields(object, &["label", "key"], &format!("{context}[{index}]"))?;
                Ok(NodeKeyQuery {
                    label: parse_js_required_string_field(
                        object,
                        "label",
                        &format!("{context}[{index}] label"),
                    )?,
                    key: parse_js_required_string_field(
                        object,
                        "key",
                        &format!("{context}[{index}] key"),
                    )?,
                })
            }
            _ => Err(napi::Error::from_reason(format!(
                "{context}[{index}] must be a string or {{ label, key }}"
            ))),
        })
        .collect()
}

fn parse_js_node_labels_arg(value: &serde_json::Value, context: &str) -> Result<Vec<String>> {
    match value {
        serde_json::Value::String(label) => Ok(vec![label.clone()]),
        serde_json::Value::Array(labels) => labels
            .iter()
            .enumerate()
            .map(|(index, value)| {
                value.as_str().map(ToString::to_string).ok_or_else(|| {
                    napi::Error::from_reason(format!("{}[{}] must be a string", context, index))
                })
            })
            .collect(),
        _ => Err(napi::Error::from_reason(format!(
            "{} must be a string or string array",
            context
        ))),
    }
}

fn js_node_label_filter_to_rust(filter: NodeLabelFilter) -> Result<CoreNodeLabelFilter> {
    let mode = match filter.mode.as_str() {
        "any" => CoreLabelMatchMode::Any,
        "all" => CoreLabelMatchMode::All,
        other => {
            return Err(napi::Error::from_reason(format!(
                "node label filter mode must be 'any' or 'all', got '{}'",
                other
            )));
        }
    };
    Ok(CoreNodeLabelFilter {
        labels: filter.labels,
        mode,
    })
}

fn parse_js_node_label_filter_field(
    object: &serde_json::Map<String, serde_json::Value>,
    key: &str,
    context: &str,
) -> Result<Option<CoreNodeLabelFilter>> {
    let Some(value) = js_non_null_field(object, key) else {
        return Ok(None);
    };
    let filter = js_object(value, context)?;
    ensure_only_js_fields(filter, &["labels", "mode"], context)?;
    let labels = parse_js_required_string_array_field(filter, "labels", context)?;
    let mode = parse_js_required_string_field(filter, "mode", context)?;
    js_node_label_filter_to_rust(NodeLabelFilter { labels, mode }).map(Some)
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

fn parse_js_edge_filter(
    object: &serde_json::Map<String, serde_json::Value>,
    updated_at_key: &str,
    valid_at_key: &str,
    valid_from_key: &str,
    valid_to_key: &str,
    context: &str,
) -> Result<Option<EdgeFilterExpr>> {
    match object.get("filter") {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(value) => parse_js_edge_filter_expr(
            value,
            updated_at_key,
            valid_at_key,
            valid_from_key,
            valid_to_key,
            &format!("{} filter", context),
        )
        .map(Some),
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

fn parse_js_edge_filter_expr(
    value: &serde_json::Value,
    updated_at_key: &str,
    valid_at_key: &str,
    valid_from_key: &str,
    valid_to_key: &str,
    context: &str,
) -> Result<EdgeFilterExpr> {
    let object = js_object(value, context)?;
    if object.is_empty() {
        return Err(napi::Error::from_reason(format!(
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
                parse_js_edge_filter_expr(
                    child,
                    updated_at_key,
                    valid_at_key,
                    valid_from_key,
                    valid_to_key,
                    &format!("{} and[{}]", context, index),
                )
            })
            .collect::<Result<Vec<_>>>()
            .map(EdgeFilterExpr::And);
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
                parse_js_edge_filter_expr(
                    child,
                    updated_at_key,
                    valid_at_key,
                    valid_from_key,
                    valid_to_key,
                    &format!("{} or[{}]", context, index),
                )
            })
            .collect::<Result<Vec<_>>>()
            .map(EdgeFilterExpr::Or);
    }
    if let Some(value) = object.get("not") {
        ensure_only_js_fields(object, &["not"], context)?;
        return parse_js_edge_filter_expr(
            value,
            updated_at_key,
            valid_at_key,
            valid_from_key,
            valid_to_key,
            &format!("{} not", context),
        )
        .map(Box::new)
        .map(EdgeFilterExpr::Not);
    }
    if object.contains_key("property") {
        return parse_js_property_edge_filter(object, context);
    }
    if let Some(value) = object.get("weight") {
        ensure_only_js_fields(object, &["weight"], context)?;
        let range = js_object(value, &format!("{} weight", context))?;
        let (lower, upper) = parse_js_f32_range_bounds(range, &format!("{} weight", context))?;
        return Ok(EdgeFilterExpr::WeightRange { lower, upper });
    }
    if let Some(value) = object.get(updated_at_key) {
        ensure_only_js_fields(object, &[updated_at_key], context)?;
        let range = js_object(value, &format!("{} {}", context, updated_at_key))?;
        let (lower_ms, upper_ms) =
            parse_js_i64_range_bounds(range, &format!("{} {}", context, updated_at_key))?;
        return Ok(EdgeFilterExpr::UpdatedAtRange { lower_ms, upper_ms });
    }
    if let Some(value) = object.get(valid_at_key) {
        ensure_only_js_fields(object, &[valid_at_key], context)?;
        return Ok(EdgeFilterExpr::ValidAt {
            epoch_ms: js_number_to_i64(value, &format!("{} {}", context, valid_at_key))?,
        });
    }
    if let Some(value) = object.get(valid_from_key) {
        ensure_only_js_fields(object, &[valid_from_key], context)?;
        let range = js_object(value, &format!("{} {}", context, valid_from_key))?;
        let (lower_ms, upper_ms) =
            parse_js_i64_range_bounds(range, &format!("{} {}", context, valid_from_key))?;
        return Ok(EdgeFilterExpr::ValidFromRange { lower_ms, upper_ms });
    }
    if let Some(value) = object.get(valid_to_key) {
        ensure_only_js_fields(object, &[valid_to_key], context)?;
        let range = js_object(value, &format!("{} {}", context, valid_to_key))?;
        let (lower_ms, upper_ms) =
            parse_js_i64_range_bounds(range, &format!("{} {}", context, valid_to_key))?;
        return Ok(EdgeFilterExpr::ValidToRange { lower_ms, upper_ms });
    }

    Err(napi::Error::from_reason(format!(
        "{} must contain a valid filter selector",
        context
    )))
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

fn parse_js_property_edge_filter(
    object: &serde_json::Map<String, serde_json::Value>,
    context: &str,
) -> Result<EdgeFilterExpr> {
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
        return Ok(EdgeFilterExpr::PropertyEquals {
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
        return Ok(EdgeFilterExpr::PropertyIn {
            key,
            values: values.iter().map(json_to_prop_value).collect(),
        });
    }
    if has_range {
        ensure_only_js_fields(object, &["property", "gt", "gte", "lt", "lte"], context)?;
        let (lower, upper) = parse_js_property_range_bounds(object, context)?;
        return Ok(EdgeFilterExpr::PropertyRange { key, lower, upper });
    }
    if object.contains_key("exists") {
        ensure_only_js_fields(object, &["property", "exists"], context)?;
        require_js_true_field(object, "exists", context)?;
        return Ok(EdgeFilterExpr::PropertyExists { key });
    }
    if object.contains_key("missing") {
        ensure_only_js_fields(object, &["property", "missing"], context)?;
        require_js_true_field(object, "missing", context)?;
        return Ok(EdgeFilterExpr::PropertyMissing { key });
    }

    unreachable!("operator family count was checked above")
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
) -> Result<(
    Option<CorePropertyRangeBound>,
    Option<CorePropertyRangeBound>,
)> {
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
        Some(CorePropertyRangeBound::Excluded(json_to_prop_value(value)))
    } else {
        object
            .get("gte")
            .map(|value| CorePropertyRangeBound::Included(json_to_prop_value(value)))
    };
    let upper = if let Some(value) = object.get("lt") {
        Some(CorePropertyRangeBound::Excluded(json_to_prop_value(value)))
    } else {
        object
            .get("lte")
            .map(|value| CorePropertyRangeBound::Included(json_to_prop_value(value)))
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

fn parse_js_f32_range_bounds(
    object: &serde_json::Map<String, serde_json::Value>,
    context: &str,
) -> Result<(Option<f32>, Option<f32>)> {
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
        Some(next_up_f32(js_number_to_f32(
            value,
            &format!("{} gt", context),
        )?))
    } else {
        object
            .get("gte")
            .map(|value| js_number_to_f32(value, &format!("{} gte", context)))
            .transpose()?
    };
    let upper = if let Some(value) = object.get("lt") {
        Some(next_down_f32(js_number_to_f32(
            value,
            &format!("{} lt", context),
        )?))
    } else {
        object
            .get("lte")
            .map(|value| js_number_to_f32(value, &format!("{} lte", context)))
            .transpose()?
    };
    if lower.is_none() && upper.is_none() {
        return Err(napi::Error::from_reason(format!(
            "{} range predicate requires at least one of gt, gte, lt, or lte",
            context
        )));
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

fn parse_js_required_u8_field(
    object: &serde_json::Map<String, serde_json::Value>,
    key: &str,
    context: &str,
) -> Result<u8> {
    let value = js_non_null_field(object, key)
        .ok_or_else(|| napi::Error::from_reason(format!("{context} is required")))?;
    parse_js_u8_number(value, context)
}

fn parse_js_required_string_array_field(
    object: &serde_json::Map<String, serde_json::Value>,
    key: &str,
    context: &str,
) -> Result<Vec<String>> {
    let value = js_non_null_field(object, key)
        .ok_or_else(|| napi::Error::from_reason(format!("{} {} is required", context, key)))?;
    js_array(value, &format!("{} {}", context, key))?
        .iter()
        .enumerate()
        .map(|(index, value)| {
            value.as_str().map(ToString::to_string).ok_or_else(|| {
                napi::Error::from_reason(format!("{} {}[{}] must be a string", context, key, index))
            })
        })
        .collect()
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

fn js_number_to_usize(value: &serde_json::Value, context: &str) -> Result<usize> {
    let number = value
        .as_f64()
        .ok_or_else(|| napi::Error::from_reason(format!("{context} must be a number")))?;
    f64_to_usize(number, context)
}

fn parse_js_u8_number(value: &serde_json::Value, context: &str) -> Result<u8> {
    let parsed = js_number_to_u64(value, context)?;
    u8::try_from(parsed)
        .map_err(|_| napi::Error::from_reason(format!("{context} must be between 0 and 255")))
}

fn parse_js_byte_array(value: &serde_json::Value, context: &str) -> Result<Vec<u8>> {
    js_array(value, context)?
        .iter()
        .enumerate()
        .map(|(index, value)| {
            let parsed = js_number_to_u64(value, &format!("{context}[{index}]"))?;
            u8::try_from(parsed).map_err(|_| {
                napi::Error::from_reason(format!("{context}[{index}] must be between 0 and 255"))
            })
        })
        .collect()
}

fn js_number_to_f32(value: &serde_json::Value, context: &str) -> Result<f32> {
    let number = value
        .as_f64()
        .ok_or_else(|| napi::Error::from_reason(format!("{} must be a number", context)))?;
    if !number.is_finite() || number < f32::MIN as f64 || number > f32::MAX as f64 {
        return Err(napi::Error::from_reason(format!(
            "{} must be a finite f32 number",
            context
        )));
    }
    let parsed = number as f32;
    if parsed.is_nan() {
        return Err(napi::Error::from_reason(format!(
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
    options: Option<FindNodesRangePagedOptions>,
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
pub struct CompactionProgress {
    pub phase: String,
    pub segments_processed: u32,
    pub total_segments: u32,
    pub records_processed: i64,
    pub total_records: i64,
}

#[napi(object)]
pub struct CompactionStats {
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

impl From<CoreCompactionStats> for CompactionStats {
    fn from(s: CoreCompactionStats) -> Self {
        // All casts are safe: segment counts are small, and node/edge counts from compaction
        // never approach i64::MAX in practice (sequential IDs from 1).
        debug_assert!(s.segments_merged <= u32::MAX as usize);
        debug_assert!(s.nodes_kept <= i64::MAX as u64);
        debug_assert!(s.nodes_removed <= i64::MAX as u64);
        debug_assert!(s.edges_kept <= i64::MAX as u64);
        debug_assert!(s.edges_removed <= i64::MAX as u64);
        debug_assert!(s.duration_ms <= i64::MAX as u64);
        debug_assert!(s.output_segment_id <= i64::MAX as u64);
        CompactionStats {
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
pub struct PrunePolicy {
    /// Prune nodes older than this many milliseconds. Optional.
    pub max_age_ms: Option<f64>,
    /// Prune nodes with weight <= this threshold. Optional.
    pub max_weight: Option<f64>,
    /// Scope to a single node label. Optional.
    pub label: Option<String>,
}

#[napi(object)]
pub struct NamedPrunePolicy {
    pub name: String,
    pub policy: PrunePolicy,
}

#[napi(object)]
pub struct PruneResult {
    /// Number of nodes pruned.
    pub nodes_pruned: i64,
    /// Number of edges cascade-deleted.
    pub edges_pruned: i64,
}

#[napi(object)]
pub struct EdgeInvalidation {
    pub edge_id: f64,
    pub valid_to: i64,
}

#[napi(object)]
pub struct GraphPatch {
    pub upsert_nodes: Option<Vec<NodeInput>>,
    pub upsert_edges: Option<Vec<EdgeInput>>,
    pub invalidate_edges: Option<Vec<EdgeInvalidation>>,
    pub delete_node_ids: Option<Vec<f64>>,
    pub delete_edge_ids: Option<Vec<f64>>,
}

#[napi(object)]
pub struct PatchResult {
    pub node_ids: Float64Array,
    pub edge_ids: Float64Array,
}

// --- PPR types ---

#[napi(object)]
pub struct PprResult {
    pub node_ids: Float64Array,
    pub scores: Float64Array,
    pub iterations: u32,
    pub converged: bool,
    pub algorithm: String,
    pub approx: Option<PprApproxMeta>,
}

#[napi(object)]
pub struct PprApproxMeta {
    pub residual_tolerance: f64,
    pub pushes: f64,
    pub max_remaining_residual: f64,
}

fn ppr_result_to_js(r: CorePprResult) -> Result<PprResult> {
    let mut node_ids_raw = Vec::with_capacity(r.scores.len());
    let mut scores = Vec::with_capacity(r.scores.len());
    for (id, score) in &r.scores {
        node_ids_raw.push(u64_to_f64(*id)?);
        scores.push(*score);
    }
    Ok(PprResult {
        node_ids: Float64Array::new(node_ids_raw),
        scores: Float64Array::new(scores),
        iterations: r.iterations,
        converged: r.converged,
        algorithm: ppr_algorithm_to_js(r.algorithm).to_string(),
        approx: r.approx.map(|a| PprApproxMeta {
            residual_tolerance: a.residual_tolerance,
            pushes: a.pushes as f64,
            max_remaining_residual: a.max_remaining_residual,
        }),
    })
}

fn js_prune_policy_to_rust(policy: PrunePolicy, _context: &str) -> Result<CorePrunePolicy> {
    Ok(CorePrunePolicy {
        max_age_ms: policy.max_age_ms.map(|v| v as i64),
        max_weight: policy.max_weight.map(|v| v as f32),
        label: policy.label,
    })
}

fn js_ppr_options_to_ppr_options(
    algorithm: Option<&str>,
    damping_factor: &Option<f64>,
    max_iterations: &Option<u32>,
    epsilon: &Option<f64>,
    approx_residual_tolerance: &Option<f64>,
    edge_label_filter: &Option<Vec<String>>,
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
        edge_label_filter: edge_label_filter.clone(),
        max_results: max_results.map(|v| v as usize),
    })
}

// --- Export types ---

#[napi(object)]
pub struct ExportOptions {
    pub node_label_filter: Option<NodeLabelFilter>,
    pub edge_label_filter: Option<Vec<String>>,
    pub include_weights: Option<bool>,
}

#[napi(object)]
pub struct AdjacencyExport {
    pub node_ids: Float64Array,
    pub edge_labels: Vec<String>,
    pub edge_from: Float64Array,
    pub edge_to: Float64Array,
    pub edge_label_indexes: Uint32Array,
    pub edge_weights: Option<Float64Array>,
}

fn adjacency_export_to_js(
    r: CoreAdjacencyExport,
    include_weights: bool,
) -> Result<AdjacencyExport> {
    let node_ids_vec: Vec<f64> = r
        .node_ids
        .iter()
        .map(|&id| u64_to_f64(id))
        .collect::<Result<Vec<_>>>()?;
    let node_ids = Float64Array::new(node_ids_vec);
    let mut from_raw = Vec::with_capacity(r.edges.len());
    let mut to_raw = Vec::with_capacity(r.edges.len());
    let mut type_indexes = Vec::with_capacity(r.edges.len());
    let mut weights = Vec::with_capacity(r.edges.len());
    for edge in &r.edges {
        from_raw.push(u64_to_f64(edge.from)?);
        to_raw.push(u64_to_f64(edge.to)?);
        type_indexes.push(edge.edge_label_index);
        if let Some(weight) = edge.weight {
            weights.push(weight as f64);
        }
    }
    Ok(AdjacencyExport {
        node_ids,
        edge_labels: r.edge_labels,
        edge_from: Float64Array::new(from_raw),
        edge_to: Float64Array::new(to_raw),
        edge_label_indexes: Uint32Array::new(type_indexes),
        edge_weights: if include_weights {
            Some(Float64Array::new(weights))
        } else {
            None
        },
    })
}

fn js_export_options_to_rust(opts: Option<ExportOptions>) -> Result<CoreExportOptions> {
    match opts {
        None => Ok(CoreExportOptions::default()),
        Some(o) => Ok(CoreExportOptions {
            node_label_filter: o
                .node_label_filter
                .map(js_node_label_filter_to_rust)
                .transpose()?,
            edge_label_filter: o.edge_label_filter,
            include_weights: o.include_weights.unwrap_or(true),
        }),
    }
}

fn js_patch_to_rust(patch: GraphPatch) -> napi::Result<CoreGraphPatch> {
    let upsert_nodes: Vec<CoreNodeInput> = patch
        .upsert_nodes
        .unwrap_or_default()
        .into_iter()
        .map(NodeInput::try_into)
        .collect::<Result<Vec<_>>>()?;

    let upsert_edges: Vec<CoreEdgeInput> = patch
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

    Ok(CoreGraphPatch {
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
    type Output = Option<CoreCompactionStats>;
    type JsValue = Option<CompactionStats>;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        let engine = clone_engine_handle(&self.db)?;
        let tsfn = &self.tsfn;
        let result = engine.compact_with_progress(|progress| {
            let js_progress = CompactionProgress {
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
    inner: Arc<Mutex<Option<CoreWriteTxn>>>,
    order: Arc<TxnAsyncOrder>,
    ticket: u64,
    op: Option<Box<dyn FnOnce(&mut CoreWriteTxn) -> std::result::Result<T, EngineError> + Send>>,
    convert: fn(T) -> napi::Result<J>,
}

impl<T: Send + 'static, J: ToNapiValue + TypeName + 'static> TxnAsyncOp<T, J> {
    fn new(
        txn: &WriteTxn,
        op: impl FnOnce(&mut CoreWriteTxn) -> std::result::Result<T, EngineError> + Send + 'static,
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
    inner: Arc<Mutex<Option<CoreWriteTxn>>>,
    order: Arc<TxnAsyncOrder>,
    ticket: u64,
    op: Option<Box<dyn FnOnce(&mut CoreWriteTxn) -> std::result::Result<T, EngineError> + Send>>,
    convert: fn(T) -> napi::Result<J>,
}

impl<T: Send + 'static, J: ToNapiValue + TypeName + 'static> TxnAsyncTakeOp<T, J> {
    fn new(
        txn: &WriteTxn,
        op: impl FnOnce(&mut CoreWriteTxn) -> std::result::Result<T, EngineError> + Send + 'static,
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

fn with_txn<F, T>(inner: &Arc<Mutex<Option<CoreWriteTxn>>>, f: F) -> Result<T>
where
    F: FnOnce(&mut CoreWriteTxn) -> std::result::Result<T, EngineError>,
{
    let mut guard = inner
        .lock()
        .map_err(|e| napi::Error::from_reason(e.to_string()))?;
    let txn = guard
        .as_mut()
        .ok_or_else(|| napi::Error::from_reason(EngineError::TxnClosed.to_string()))?;
    f(txn).map_err(|e| napi::Error::from_reason(e.to_string()))
}

fn with_txn_ref<F, T>(inner: &Arc<Mutex<Option<CoreWriteTxn>>>, f: F) -> Result<T>
where
    F: FnOnce(&CoreWriteTxn) -> std::result::Result<T, EngineError>,
{
    let guard = inner
        .lock()
        .map_err(|e| napi::Error::from_reason(e.to_string()))?;
    let txn = guard
        .as_ref()
        .ok_or_else(|| napi::Error::from_reason(EngineError::TxnClosed.to_string()))?;
    f(txn).map_err(|e| napi::Error::from_reason(e.to_string()))
}

fn with_txn_take<F, T>(inner: &Arc<Mutex<Option<CoreWriteTxn>>>, f: F) -> Result<T>
where
    F: FnOnce(&mut CoreWriteTxn) -> std::result::Result<T, EngineError>,
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

fn js_upsert_node_options(options: Option<UpsertNodeOptions>) -> CoreUpsertNodeOptions {
    let (props, weight, dense_vector, sparse_vector) = match options {
        Some(o) => (o.props, o.weight, o.dense_vector, o.sparse_vector),
        None => (None, None, None, None),
    };
    CoreUpsertNodeOptions {
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

fn js_upsert_edge_options(options: Option<UpsertEdgeOptions>) -> CoreUpsertEdgeOptions {
    let (props, weight, valid_from, valid_to) = match options {
        Some(o) => (o.props, o.weight, o.valid_from, o.valid_to),
        None => (None, None, None, None),
    };
    CoreUpsertEdgeOptions {
        props: convert_js_props(props),
        weight: weight.unwrap_or(1.0) as f32,
        valid_from,
        valid_to,
    }
}

fn txn_node_ref_labels_value(label: String) -> serde_json::Value {
    serde_json::Value::Array(vec![serde_json::Value::String(label)])
}

fn parse_txn_node_ref_label(labels: serde_json::Value, context: &str) -> Result<String> {
    let labels = parse_js_node_labels_arg(&labels, context)?;
    if labels.len() != 1 {
        return Err(napi::Error::from_reason(format!(
            "{} must contain exactly one label",
            context
        )));
    }
    Ok(labels.into_iter().next().unwrap())
}

fn js_txn_node_ref_to_rust(value: TxnNodeRef) -> Result<CoreTxnNodeRef> {
    let has_id = value.id.is_some();
    let has_key = value.labels.is_some() || value.key.is_some();
    let has_local = value.local.is_some();
    match (has_id, has_key, has_local) {
        (true, false, false) => Ok(CoreTxnNodeRef::Id(f64_to_u64(value.id.unwrap())?)),
        (false, true, false) => Ok(CoreTxnNodeRef::Key {
            label: parse_txn_node_ref_label(
                value.labels.ok_or_else(|| {
                    napi::Error::from_reason("node key ref requires labels".to_string())
                })?,
                "node key ref labels",
            )?,
            key: value
                .key
                .ok_or_else(|| napi::Error::from_reason("node key ref requires key".to_string()))?,
        }),
        (false, false, true) => Ok(CoreTxnNodeRef::Local(TxnLocalRef::Alias(
            value.local.unwrap(),
        ))),
        _ => Err(napi::Error::from_reason(
            "node ref must be exactly one of { id }, { labels, key }, or { local }".to_string(),
        )),
    }
}

fn js_txn_edge_ref_to_rust(value: TxnEdgeRef) -> Result<CoreTxnEdgeRef> {
    let has_id = value.id.is_some();
    let has_triple = value.from.is_some() || value.to.is_some() || value.label.is_some();
    let has_local = value.local.is_some();
    match (has_id, has_triple, has_local) {
        (true, false, false) => Ok(CoreTxnEdgeRef::Id(f64_to_u64(value.id.unwrap())?)),
        (false, true, false) => Ok(CoreTxnEdgeRef::Triple {
            from: js_txn_node_ref_to_rust(value.from.ok_or_else(|| {
                napi::Error::from_reason("edge triple ref requires from".to_string())
            })?)?,
            to: js_txn_node_ref_to_rust(value.to.ok_or_else(|| {
                napi::Error::from_reason("edge triple ref requires to".to_string())
            })?)?,
            label: value.label.ok_or_else(|| {
                napi::Error::from_reason("edge triple ref requires label".to_string())
            })?,
        }),
        (false, false, true) => Ok(CoreTxnEdgeRef::Local(TxnLocalRef::Alias(
            value.local.unwrap(),
        ))),
        _ => Err(napi::Error::from_reason(
            "edge ref must be exactly one of { id }, { from, to, label }, or { local }".to_string(),
        )),
    }
}

fn txn_node_ref_to_js(value: CoreTxnNodeRef) -> Result<TxnNodeRef> {
    match value {
        CoreTxnNodeRef::Id(id) => Ok(TxnNodeRef {
            id: Some(u64_to_f64(id)?),
            labels: None,
            key: None,
            local: None,
        }),
        CoreTxnNodeRef::Key { label, key } => Ok(TxnNodeRef {
            id: None,
            labels: Some(txn_node_ref_labels_value(label)),
            key: Some(key),
            local: None,
        }),
        CoreTxnNodeRef::Local(local) => Ok(TxnNodeRef {
            id: None,
            labels: None,
            key: None,
            local: txn_local_ref_to_js(local),
        }),
    }
}

fn txn_edge_ref_to_js(value: CoreTxnEdgeRef) -> Result<TxnEdgeRef> {
    match value {
        CoreTxnEdgeRef::Id(id) => Ok(TxnEdgeRef {
            id: Some(u64_to_f64(id)?),
            from: None,
            to: None,
            label: None,
            local: None,
        }),
        CoreTxnEdgeRef::Triple { from, to, label } => Ok(TxnEdgeRef {
            id: None,
            from: Some(txn_node_ref_to_js(from)?),
            to: Some(txn_node_ref_to_js(to)?),
            label: Some(label),
            local: None,
        }),
        CoreTxnEdgeRef::Local(local) => Ok(TxnEdgeRef {
            id: None,
            from: None,
            to: None,
            label: None,
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

fn txn_node_view_to_js(view: CoreTxnNodeView) -> Result<TxnNodeView> {
    Ok(TxnNodeView {
        id: view.id.map(u64_to_f64).transpose()?,
        local: view.local.and_then(txn_local_ref_to_js),
        labels: view.labels,
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
                .map(|(dimension, value)| SparseEntry {
                    dimension,
                    value: value as f64,
                })
                .collect()
        }),
    })
}

fn txn_edge_view_to_js(view: CoreTxnEdgeView) -> Result<TxnEdgeView> {
    Ok(TxnEdgeView {
        id: view.id.map(u64_to_f64).transpose()?,
        local: view.local.and_then(txn_local_ref_to_js),
        from: txn_node_ref_to_js(view.from)?,
        to: txn_node_ref_to_js(view.to)?,
        label: view.label,
        props: props_to_json(view.props),
        created_at: view.created_at,
        updated_at: view.updated_at,
        weight: view.weight as f64,
        valid_from: view.valid_from,
        valid_to: view.valid_to,
    })
}

fn js_txn_operation_to_rust(value: serde_json::Value) -> Result<TxnIntent> {
    let object = js_object(&value, "transaction operation")?;
    let op = parse_js_required_string_field(object, "op", "transaction operation op")?;
    match op.as_str() {
        "upsertNode" => Ok(TxnIntent::UpsertNode {
            alias: parse_js_optional_string_field(object, "alias", "upsertNode alias")?,
            labels: parse_js_node_labels_arg(
                js_non_null_field(object, "labels")
                    .ok_or_else(|| napi::Error::from_reason("upsertNode requires labels"))?,
                "upsertNode labels",
            )?,
            key: js_non_null_field(object, "key")
                .and_then(|value| value.as_str())
                .map(ToString::to_string)
                .ok_or_else(|| napi::Error::from_reason("upsertNode requires key"))?,
            options: CoreUpsertNodeOptions {
                props: convert_js_props(parse_js_optional_props_field(
                    object,
                    "props",
                    "upsertNode props",
                )?),
                weight: parse_js_optional_f64_field(object, "weight", "upsertNode weight")?
                    .unwrap_or(1.0) as f32,
                dense_vector: parse_js_optional_f64_array_field(
                    object,
                    "denseVector",
                    "upsertNode denseVector",
                )?
                .map(|v| v.into_iter().map(|x| x as f32).collect()),
                sparse_vector: parse_js_optional_sparse_vector_field(
                    object,
                    "sparseVector",
                    "upsertNode sparseVector",
                )?
                .map(|v| {
                    v.into_iter()
                        .map(|e| (e.dimension, e.value as f32))
                        .collect()
                }),
            },
        }),
        "upsertEdge" => Ok(TxnIntent::UpsertEdge {
            alias: parse_js_optional_string_field(object, "alias", "upsertEdge alias")?,
            from: js_txn_node_ref_to_rust(parse_js_required_txn_node_ref_field(
                object,
                "from",
                "upsertEdge from",
            )?)?,
            to: js_txn_node_ref_to_rust(parse_js_required_txn_node_ref_field(
                object,
                "to",
                "upsertEdge to",
            )?)?,
            label: js_non_null_field(object, "label")
                .and_then(|value| value.as_str())
                .map(ToString::to_string)
                .ok_or_else(|| napi::Error::from_reason("upsertEdge requires label"))?,
            options: CoreUpsertEdgeOptions {
                props: convert_js_props(parse_js_optional_props_field(
                    object,
                    "props",
                    "upsertEdge props",
                )?),
                weight: parse_js_optional_f64_field(object, "weight", "upsertEdge weight")?
                    .unwrap_or(1.0) as f32,
                valid_from: parse_js_optional_i64_field(
                    object,
                    "validFrom",
                    "upsertEdge validFrom",
                )?,
                valid_to: parse_js_optional_i64_field(object, "validTo", "upsertEdge validTo")?,
            },
        }),
        "deleteNode" => Ok(TxnIntent::DeleteNode {
            target: js_txn_node_ref_to_rust(txn_target_as_node(
                parse_js_required_txn_target_field(object, "target", "deleteNode target")?,
            )?)?,
        }),
        "deleteEdge" => Ok(TxnIntent::DeleteEdge {
            target: js_txn_edge_ref_to_rust(txn_target_as_edge(
                parse_js_required_txn_target_field(object, "target", "deleteEdge target")?,
            )?)?,
        }),
        "invalidateEdge" => Ok(TxnIntent::InvalidateEdge {
            target: js_txn_edge_ref_to_rust(txn_target_as_edge(
                parse_js_required_txn_target_field(object, "target", "invalidateEdge target")?,
            )?)?,
            valid_to: js_non_null_field(object, "validTo")
                .map(|value| js_number_to_i64(value, "invalidateEdge validTo"))
                .transpose()?
                .ok_or_else(|| napi::Error::from_reason("invalidateEdge requires validTo"))?,
        }),
        other => Err(napi::Error::from_reason(format!(
            "invalid transaction op '{}'",
            other
        ))),
    }
}

fn parse_js_optional_f64_field(
    object: &serde_json::Map<String, serde_json::Value>,
    key: &str,
    context: &str,
) -> Result<Option<f64>> {
    js_non_null_field(object, key)
        .map(|value| {
            value
                .as_f64()
                .ok_or_else(|| napi::Error::from_reason(format!("{} must be a number", context)))
        })
        .transpose()
}

fn parse_js_optional_props_field(
    object: &serde_json::Map<String, serde_json::Value>,
    key: &str,
    context: &str,
) -> Result<Option<HashMap<String, serde_json::Value>>> {
    js_non_null_field(object, key)
        .map(|value| {
            Ok(js_object(value, context)?
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect())
        })
        .transpose()
}

fn parse_js_optional_f64_array_field(
    object: &serde_json::Map<String, serde_json::Value>,
    key: &str,
    context: &str,
) -> Result<Option<Vec<f64>>> {
    js_non_null_field(object, key)
        .map(|value| {
            js_array(value, context)?
                .iter()
                .enumerate()
                .map(|(index, value)| {
                    value.as_f64().ok_or_else(|| {
                        napi::Error::from_reason(format!("{}[{}] must be a number", context, index))
                    })
                })
                .collect()
        })
        .transpose()
}

fn parse_js_optional_sparse_vector_field(
    object: &serde_json::Map<String, serde_json::Value>,
    key: &str,
    context: &str,
) -> Result<Option<Vec<SparseEntry>>> {
    js_non_null_field(object, key)
        .map(|value| {
            js_array(value, context)?
                .iter()
                .enumerate()
                .map(|(index, value)| {
                    let entry = js_object(value, &format!("{}[{}]", context, index))?;
                    let dimension = js_non_null_field(entry, "dimension")
                        .ok_or_else(|| {
                            napi::Error::from_reason(format!(
                                "{}[{}].dimension is required",
                                context, index
                            ))
                        })
                        .and_then(|value| {
                            let dimension = js_number_to_u64(
                                value,
                                &format!("{}[{}].dimension", context, index),
                            )?;
                            u32::try_from(dimension).map_err(|_| {
                                napi::Error::from_reason(format!(
                                    "{}[{}].dimension is too large",
                                    context, index
                                ))
                            })
                        })?;
                    let value = js_non_null_field(entry, "value")
                        .and_then(|value| value.as_f64())
                        .ok_or_else(|| {
                            napi::Error::from_reason(format!(
                                "{}[{}].value must be a number",
                                context, index
                            ))
                        })?;
                    Ok(SparseEntry { dimension, value })
                })
                .collect()
        })
        .transpose()
}

fn parse_js_required_txn_node_ref_field(
    object: &serde_json::Map<String, serde_json::Value>,
    key: &str,
    context: &str,
) -> Result<TxnNodeRef> {
    parse_js_txn_node_ref(
        js_non_null_field(object, key)
            .ok_or_else(|| napi::Error::from_reason(format!("{} is required", context)))?,
        context,
    )
}

fn parse_js_txn_node_ref(value: &serde_json::Value, context: &str) -> Result<TxnNodeRef> {
    let object = js_object(value, context)?;
    Ok(TxnNodeRef {
        id: parse_js_optional_f64_field(object, "id", &format!("{} id", context))?,
        labels: js_non_null_field(object, "labels").cloned(),
        key: parse_js_optional_string_field(object, "key", &format!("{} key", context))?,
        local: parse_js_optional_string_field(object, "local", &format!("{} local", context))?,
    })
}

fn parse_js_required_txn_target_field(
    object: &serde_json::Map<String, serde_json::Value>,
    key: &str,
    context: &str,
) -> Result<TxnEdgeOrNodeRef> {
    parse_js_txn_target(
        js_non_null_field(object, key)
            .ok_or_else(|| napi::Error::from_reason(format!("{} is required", context)))?,
        context,
    )
}

fn parse_js_txn_target(value: &serde_json::Value, context: &str) -> Result<TxnEdgeOrNodeRef> {
    let object = js_object(value, context)?;
    Ok(TxnEdgeOrNodeRef {
        id: parse_js_optional_f64_field(object, "id", &format!("{} id", context))?,
        labels: js_non_null_field(object, "labels").cloned(),
        label: parse_js_optional_string_field(object, "label", &format!("{} label", context))?,
        key: parse_js_optional_string_field(object, "key", &format!("{} key", context))?,
        local: parse_js_optional_string_field(object, "local", &format!("{} local", context))?,
        from: js_non_null_field(object, "from")
            .map(|value| parse_js_txn_node_ref(value, &format!("{} from", context)))
            .transpose()?,
        to: js_non_null_field(object, "to")
            .map(|value| parse_js_txn_node_ref(value, &format!("{} to", context)))
            .transpose()?,
    })
}

fn txn_target_as_node(target: TxnEdgeOrNodeRef) -> Result<TxnNodeRef> {
    Ok(TxnNodeRef {
        id: target.id,
        labels: target.labels,
        key: target.key,
        local: target.local,
    })
}

fn txn_target_as_edge(target: TxnEdgeOrNodeRef) -> Result<TxnEdgeRef> {
    Ok(TxnEdgeRef {
        id: target.id,
        from: target.from,
        to: target.to,
        label: target.label,
        local: target.local,
    })
}

fn txn_commit_result_to_js(result: CoreTxnCommitResult) -> Result<TxnCommitResult> {
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
    Ok(TxnCommitResult {
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

fn f64_to_usize(v: f64, context: &str) -> Result<usize> {
    if !(0.0..=MAX_SAFE_INTEGER).contains(&v) || v.fract() != 0.0 {
        return Err(napi::Error::from_reason(format!(
            "{context} must be a safe non-negative integer"
        )));
    }
    usize::try_from(v as u64)
        .map_err(|_| napi::Error::from_reason(format!("{context} is too large")))
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

fn i64_to_safe_f64(v: i64) -> Result<f64> {
    let value = v as f64;
    if !value.is_finite() || value.abs() > MAX_SAFE_INTEGER {
        return Err(napi::Error::from_reason(
            "Value exceeds JavaScript safe integer range".to_string(),
        ));
    }
    Ok(value)
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

#[derive(Clone, Copy)]
enum RangeValueDomain {
    Int,
    UInt,
    Float,
}

fn parse_range_value_domain(s: &str) -> Result<RangeValueDomain> {
    match s {
        "int" => Ok(RangeValueDomain::Int),
        "uint" => Ok(RangeValueDomain::UInt),
        "float" => Ok(RangeValueDomain::Float),
        other => Err(napi::Error::from_reason(format!(
            "Invalid range value type annotation '{}'. Must be 'int', 'uint', or 'float'.",
            other
        ))),
    }
}

fn secondary_index_state_to_js(state: SecondaryIndexState) -> &'static str {
    match state {
        SecondaryIndexState::Building => "building",
        SecondaryIndexState::Ready => "ready",
        SecondaryIndexState::Failed => "failed",
    }
}

fn secondary_index_kind_to_js(kind: &CoreSecondaryIndexKind) -> String {
    match kind {
        CoreSecondaryIndexKind::Equality => "equality".to_string(),
        CoreSecondaryIndexKind::Range => "range".to_string(),
    }
}

#[derive(Clone, Copy)]
enum JsSecondaryIndexTargetKind {
    Node,
    Edge,
}

fn secondary_index_field_to_js(field: CoreSecondaryIndexField) -> SecondaryIndexField {
    match field {
        CoreSecondaryIndexField::Property { key } => SecondaryIndexField {
            source: "property".to_string(),
            key: Some(key),
            field: None,
        },
        CoreSecondaryIndexField::NodeMetadata(field) => SecondaryIndexField {
            source: "metadata".to_string(),
            key: None,
            field: Some(node_metadata_index_field_to_js(field).to_string()),
        },
        CoreSecondaryIndexField::EdgeMetadata(field) => SecondaryIndexField {
            source: "metadata".to_string(),
            key: None,
            field: Some(edge_metadata_index_field_to_js(field).to_string()),
        },
    }
}

fn js_secondary_index_spec_to_rust(
    spec: SecondaryIndexSpec,
    target_kind: JsSecondaryIndexTargetKind,
) -> Result<CoreSecondaryIndexSpec> {
    let kind_value = spec.kind.ok_or_else(|| {
        napi::Error::from_reason("invalid secondary index: kind is required".to_string())
    })?;
    let kind = js_secondary_index_value_string(&kind_value, "kind")?;
    let kind = js_secondary_index_kind_to_rust(&kind)?;
    let fields_value = spec.fields.ok_or_else(|| {
        napi::Error::from_reason("invalid secondary index: fields are required".to_string())
    })?;
    let field_values = fields_value.as_array().ok_or_else(|| {
        napi::Error::from_reason("invalid secondary index: fields must be an array".to_string())
    })?;
    let fields = field_values
        .iter()
        .map(|field| js_secondary_index_field_to_rust(field, target_kind))
        .collect::<Result<Vec<_>>>()?;
    Ok(CoreSecondaryIndexSpec { fields, kind })
}

fn js_secondary_index_field_to_rust(
    field: &serde_json::Value,
    target_kind: JsSecondaryIndexTargetKind,
) -> Result<CoreSecondaryIndexField> {
    let field = field.as_object().ok_or_else(|| {
        napi::Error::from_reason("invalid secondary index: field must be an object".to_string())
    })?;
    let source = js_secondary_index_required_string(field, "source", "field source")?;
    match source.as_str() {
        "property" => {
            let key = js_secondary_index_required_string(field, "key", "property key")?;
            Ok(CoreSecondaryIndexField::property(key))
        }
        "metadata" => {
            let metadata_field =
                js_secondary_index_required_string(field, "field", "metadata field")?;
            match target_kind {
                JsSecondaryIndexTargetKind::Node => Ok(CoreSecondaryIndexField::node_meta(
                    js_node_metadata_index_field_to_rust(&metadata_field)?,
                )),
                JsSecondaryIndexTargetKind::Edge => Ok(CoreSecondaryIndexField::edge_meta(
                    js_edge_metadata_index_field_to_rust(&metadata_field)?,
                )),
            }
        }
        other => Err(napi::Error::from_reason(format!(
            "invalid secondary index: field source must be 'property' or 'metadata', got '{other}'"
        ))),
    }
}

fn js_secondary_index_required_string(
    field: &serde_json::Map<String, serde_json::Value>,
    key: &str,
    display: &str,
) -> Result<String> {
    let value = field
        .get(key)
        .filter(|value| !value.is_null())
        .ok_or_else(|| {
            napi::Error::from_reason(format!("invalid secondary index: {display} is required"))
        })?;
    js_secondary_index_value_string(value, display)
}

fn js_secondary_index_value_string(value: &serde_json::Value, display: &str) -> Result<String> {
    value.as_str().map(str::to_string).ok_or_else(|| {
        napi::Error::from_reason(format!(
            "invalid secondary index: {display} must be a string"
        ))
    })
}

fn node_metadata_index_field_to_js(field: CoreNodeMetadataIndexField) -> &'static str {
    match field {
        CoreNodeMetadataIndexField::Id => "id",
        CoreNodeMetadataIndexField::Key => "key",
        CoreNodeMetadataIndexField::Weight => "weight",
        CoreNodeMetadataIndexField::CreatedAt => "created_at",
        CoreNodeMetadataIndexField::UpdatedAt => "updated_at",
    }
}

fn edge_metadata_index_field_to_js(field: CoreEdgeMetadataIndexField) -> &'static str {
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

fn js_node_metadata_index_field_to_rust(field: &str) -> Result<CoreNodeMetadataIndexField> {
    match field {
        "id" => Ok(CoreNodeMetadataIndexField::Id),
        "key" => Ok(CoreNodeMetadataIndexField::Key),
        "weight" => Ok(CoreNodeMetadataIndexField::Weight),
        "created_at" => Ok(CoreNodeMetadataIndexField::CreatedAt),
        "updated_at" => Ok(CoreNodeMetadataIndexField::UpdatedAt),
        other => Err(napi::Error::from_reason(format!(
            "invalid secondary index: unsupported node metadata field '{other}'"
        ))),
    }
}

fn js_edge_metadata_index_field_to_rust(field: &str) -> Result<CoreEdgeMetadataIndexField> {
    match field {
        "id" => Ok(CoreEdgeMetadataIndexField::Id),
        "from" => Ok(CoreEdgeMetadataIndexField::From),
        "to" => Ok(CoreEdgeMetadataIndexField::To),
        "weight" => Ok(CoreEdgeMetadataIndexField::Weight),
        "created_at" => Ok(CoreEdgeMetadataIndexField::CreatedAt),
        "updated_at" => Ok(CoreEdgeMetadataIndexField::UpdatedAt),
        "valid_from" => Ok(CoreEdgeMetadataIndexField::ValidFrom),
        "valid_to" => Ok(CoreEdgeMetadataIndexField::ValidTo),
        other => Err(napi::Error::from_reason(format!(
            "invalid secondary index: unsupported edge metadata field '{other}'"
        ))),
    }
}

fn js_secondary_index_kind_to_rust(kind: &str) -> Result<CoreSecondaryIndexKind> {
    match kind {
        "equality" => Ok(CoreSecondaryIndexKind::Equality),
        "range" => Ok(CoreSecondaryIndexKind::Range),
        other => Err(napi::Error::from_reason(format!(
            "invalid secondary index: kind must be 'equality' or 'range', got '{other}'"
        ))),
    }
}

fn js_numeric_to_prop_value(value: f64, domain: RangeValueDomain) -> Result<PropValue> {
    match domain {
        RangeValueDomain::Int => {
            if !value.is_finite() || value.fract() != 0.0 || value.abs() > MAX_SAFE_INTEGER {
                return Err(napi::Error::from_reason(
                    "Int range values must be finite safe integers.".to_string(),
                ));
            }
            Ok(PropValue::Int(value as i64))
        }
        RangeValueDomain::UInt => {
            if !(0.0..=MAX_SAFE_INTEGER).contains(&value) || value.fract() != 0.0 {
                return Err(napi::Error::from_reason(
                    "UInt range values must be finite non-negative safe integers.".to_string(),
                ));
            }
            Ok(PropValue::UInt(value as u64))
        }
        RangeValueDomain::Float => {
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

fn js_property_range_bound_to_rust(bound: &PropertyRangeBound) -> Result<CorePropertyRangeBound> {
    let domain = parse_range_value_domain(bound.domain.as_str())?;
    let value = js_numeric_to_prop_value(bound.value, domain)?;
    if bound.inclusive.unwrap_or(true) {
        Ok(CorePropertyRangeBound::Included(value))
    } else {
        Ok(CorePropertyRangeBound::Excluded(value))
    }
}

fn schema_set_options_to_core(options: Option<SchemaSetOptions>) -> Result<CoreSchemaSetOptions> {
    let mut core = CoreSchemaSetOptions::default();
    if let Some(options) = options {
        if let Some(value) = options.max_violations {
            core.max_violations = f64_to_usize(value, "schema options maxViolations")?;
        }
        if let Some(value) = options.chunk_size {
            core.chunk_size = f64_to_usize(value, "schema options chunkSize")?;
        }
        core.scan_limit = parse_schema_scan_limit_option(options.scan_limit)?;
    }
    Ok(core)
}

fn schema_check_options_to_core(
    options: Option<SchemaCheckOptions>,
) -> Result<CoreSchemaCheckOptions> {
    let mut core = CoreSchemaCheckOptions::default();
    if let Some(options) = options {
        if let Some(value) = options.max_violations {
            core.max_violations = f64_to_usize(value, "schema options maxViolations")?;
        }
        if let Some(value) = options.chunk_size {
            core.chunk_size = f64_to_usize(value, "schema options chunkSize")?;
        }
        core.scan_limit = parse_schema_scan_limit_option(options.scan_limit)?;
    }
    Ok(core)
}

fn graph_schema_set_options_to_core(
    options: Option<SchemaSetOptions>,
) -> Result<CoreGraphSchemaSetOptions> {
    let mut core = CoreGraphSchemaSetOptions::default();
    if let Some(options) = options {
        if let Some(value) = options.max_violations {
            core.max_violations = f64_to_usize(value, "schema options maxViolations")?;
        }
        if let Some(value) = options.chunk_size {
            core.chunk_size = f64_to_usize(value, "schema options chunkSize")?;
        }
        core.scan_limit = parse_schema_scan_limit_option(options.scan_limit)?;
    }
    Ok(core)
}

fn graph_schema_check_options_to_core(
    options: Option<SchemaCheckOptions>,
) -> Result<CoreGraphSchemaCheckOptions> {
    let mut core = CoreGraphSchemaCheckOptions::default();
    if let Some(options) = options {
        if let Some(value) = options.max_violations {
            core.max_violations = f64_to_usize(value, "schema options maxViolations")?;
        }
        if let Some(value) = options.chunk_size {
            core.chunk_size = f64_to_usize(value, "schema options chunkSize")?;
        }
        core.scan_limit = parse_schema_scan_limit_option(options.scan_limit)?;
    }
    Ok(core)
}

fn parse_schema_scan_limit_option(value: Option<serde_json::Value>) -> Result<Option<u64>> {
    match value {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::Number(number)) => {
            let number = number.as_f64().ok_or_else(|| {
                napi::Error::from_reason(
                    "schema options scanLimit must be a number or null".to_string(),
                )
            })?;
            f64_to_u64_for_context(number, "schema options scanLimit").map(Some)
        }
        Some(_) => Err(napi::Error::from_reason(
            "schema options scanLimit must be a number or null".to_string(),
        )),
    }
}

fn parse_js_graph_schema(value: Unknown<'_>, context: &str) -> Result<CoreGraphSchema> {
    let object = cast_js_plain_object(value, context)?;
    ensure_only_js_object_fields(&object, &["nodeSchemas", "edgeSchemas"], context)?;
    Ok(CoreGraphSchema {
        node_schemas: parse_js_optional_object_field(&object, "nodeSchemas")?
            .map(|value| parse_js_node_schema_infos(value, &format!("{context} nodeSchemas")))
            .transpose()?
            .unwrap_or_default(),
        edge_schemas: parse_js_optional_object_field(&object, "edgeSchemas")?
            .map(|value| parse_js_edge_schema_infos(value, &format!("{context} edgeSchemas")))
            .transpose()?
            .unwrap_or_default(),
    })
}

fn parse_js_node_schema_infos(
    value: Unknown<'_>,
    context: &str,
) -> Result<Vec<CoreNodeSchemaInfo>> {
    if !value.is_array()? {
        return Err(napi::Error::from_reason(format!(
            "{context} must be an array"
        )));
    }
    let array = unsafe { value.cast::<Array<'_>>()? };
    let mut infos = Vec::with_capacity(array.len() as usize);
    for index in 0..array.len() {
        let value = array
            .get::<Unknown<'_>>(index)?
            .ok_or_else(|| napi::Error::from_reason(format!("{context}[{index}] is missing")))?;
        let item_context = format!("{context}[{index}]");
        let object = cast_js_plain_object(value, &item_context)?;
        ensure_only_js_object_fields(&object, &["label", "schema"], &item_context)?;
        let label = parse_js_string_unknown(
            parse_js_required_object_field(&object, "label", &format!("{item_context} label"))?,
            &format!("{item_context} label"),
        )?;
        let schema = parse_js_node_schema(
            parse_js_required_object_field(&object, "schema", &format!("{item_context} schema"))?,
            &format!("{item_context} schema"),
        )?;
        infos.push(CoreNodeSchemaInfo { label, schema });
    }
    Ok(infos)
}

fn parse_js_edge_schema_infos(
    value: Unknown<'_>,
    context: &str,
) -> Result<Vec<CoreEdgeSchemaInfo>> {
    if !value.is_array()? {
        return Err(napi::Error::from_reason(format!(
            "{context} must be an array"
        )));
    }
    let array = unsafe { value.cast::<Array<'_>>()? };
    let mut infos = Vec::with_capacity(array.len() as usize);
    for index in 0..array.len() {
        let value = array
            .get::<Unknown<'_>>(index)?
            .ok_or_else(|| napi::Error::from_reason(format!("{context}[{index}] is missing")))?;
        let item_context = format!("{context}[{index}]");
        let object = cast_js_plain_object(value, &item_context)?;
        ensure_only_js_object_fields(&object, &["label", "schema"], &item_context)?;
        let label = parse_js_string_unknown(
            parse_js_required_object_field(&object, "label", &format!("{item_context} label"))?,
            &format!("{item_context} label"),
        )?;
        let schema = parse_js_edge_schema(
            parse_js_required_object_field(&object, "schema", &format!("{item_context} schema"))?,
            &format!("{item_context} schema"),
        )?;
        infos.push(CoreEdgeSchemaInfo { label, schema });
    }
    Ok(infos)
}

fn parse_js_graph_schema_operations(
    value: Unknown<'_>,
    context: &str,
) -> Result<Vec<CoreGraphSchemaOperation>> {
    if !value.is_array()? {
        return Err(napi::Error::from_reason(format!(
            "{context} must be an array"
        )));
    }
    let array = unsafe { value.cast::<Array<'_>>()? };
    let mut operations = Vec::with_capacity(array.len() as usize);
    for index in 0..array.len() {
        let value = array
            .get::<Unknown<'_>>(index)?
            .ok_or_else(|| napi::Error::from_reason(format!("{context}[{index}] is missing")))?;
        operations.push(parse_js_graph_schema_operation(
            value,
            &format!("{context}[{index}]"),
        )?);
    }
    Ok(operations)
}

fn parse_js_graph_schema_operation(
    value: Unknown<'_>,
    context: &str,
) -> Result<CoreGraphSchemaOperation> {
    let object = cast_js_plain_object(value, context)?;
    ensure_only_js_object_fields(&object, &["kind", "label", "schema"], context)?;
    let kind = parse_js_string_unknown(
        parse_js_required_object_field(&object, "kind", &format!("{context} kind"))?,
        &format!("{context} kind"),
    )?;
    let label = parse_js_string_unknown(
        parse_js_required_object_field(&object, "label", &format!("{context} label"))?,
        &format!("{context} label"),
    )?;
    match kind.as_str() {
        "setNode" => {
            let schema = parse_js_node_schema(
                parse_js_required_object_field(&object, "schema", &format!("{context} schema"))?,
                &format!("{context} schema"),
            )?;
            Ok(CoreGraphSchemaOperation::SetNode { label, schema })
        }
        "setEdge" => {
            let schema = parse_js_edge_schema(
                parse_js_required_object_field(&object, "schema", &format!("{context} schema"))?,
                &format!("{context} schema"),
            )?;
            Ok(CoreGraphSchemaOperation::SetEdge { label, schema })
        }
        "dropNode" => {
            reject_js_graph_schema_operation_schema(&object, context)?;
            Ok(CoreGraphSchemaOperation::DropNode { label })
        }
        "dropEdge" => {
            reject_js_graph_schema_operation_schema(&object, context)?;
            Ok(CoreGraphSchemaOperation::DropEdge { label })
        }
        other => Err(napi::Error::from_reason(format!(
            "{context} kind must be setNode, setEdge, dropNode, or dropEdge, got '{other}'"
        ))),
    }
}

fn reject_js_graph_schema_operation_schema(object: &Object<'_>, context: &str) -> Result<()> {
    if parse_js_optional_object_field(object, "schema")?.is_some() {
        return Err(napi::Error::from_reason(format!(
            "{context} schema is only accepted for setNode and setEdge operations"
        )));
    }
    Ok(())
}

fn parse_js_node_schema(value: Unknown<'_>, context: &str) -> Result<CoreNodeSchema> {
    let object = cast_js_plain_object(value, context)?;
    ensure_only_js_object_fields(
        &object,
        &[
            "additionalProperties",
            "properties",
            "key",
            "labelConstraints",
            "weight",
            "denseVector",
            "sparseVector",
        ],
        context,
    )?;
    let mut schema = CoreNodeSchema::default();
    if let Some(value) = parse_js_optional_object_field(&object, "additionalProperties")? {
        schema.additional_properties = parse_js_schema_additional_properties(
            value,
            &format!("{context} additionalProperties"),
        )?;
    }
    if let Some(value) = parse_js_optional_object_field(&object, "properties")? {
        schema.properties = parse_js_property_schema_map(value, &format!("{context} properties"))?;
    }
    schema.key = parse_js_optional_object_field(&object, "key")?
        .map(|value| parse_js_string_field_schema(value, &format!("{context} key")))
        .transpose()?;
    schema.label_constraints = parse_js_optional_object_field(&object, "labelConstraints")?
        .map(|value| parse_js_node_label_constraints(value, &format!("{context} labelConstraints")))
        .transpose()?;
    schema.weight = parse_js_optional_object_field(&object, "weight")?
        .map(|value| parse_js_numeric_field_schema(value, &format!("{context} weight")))
        .transpose()?;
    schema.dense_vector = parse_js_optional_object_field(&object, "denseVector")?
        .map(|value| parse_js_dense_vector_schema(value, &format!("{context} denseVector")))
        .transpose()?;
    schema.sparse_vector = parse_js_optional_object_field(&object, "sparseVector")?
        .map(|value| parse_js_sparse_vector_schema(value, &format!("{context} sparseVector")))
        .transpose()?;
    Ok(schema)
}

fn parse_js_edge_schema(value: Unknown<'_>, context: &str) -> Result<CoreEdgeSchema> {
    let object = cast_js_plain_object(value, context)?;
    ensure_only_js_object_fields(
        &object,
        &[
            "additionalProperties",
            "properties",
            "from",
            "to",
            "allowSelfLoops",
            "weight",
            "validity",
        ],
        context,
    )?;
    let mut schema = CoreEdgeSchema::default();
    if let Some(value) = parse_js_optional_object_field(&object, "additionalProperties")? {
        schema.additional_properties = parse_js_schema_additional_properties(
            value,
            &format!("{context} additionalProperties"),
        )?;
    }
    if let Some(value) = parse_js_optional_object_field(&object, "properties")? {
        schema.properties = parse_js_property_schema_map(value, &format!("{context} properties"))?;
    }
    schema.from = parse_js_optional_object_field(&object, "from")?
        .map(|value| parse_js_endpoint_label_schema(value, &format!("{context} from")))
        .transpose()?;
    schema.to = parse_js_optional_object_field(&object, "to")?
        .map(|value| parse_js_endpoint_label_schema(value, &format!("{context} to")))
        .transpose()?;
    if let Some(value) = parse_js_optional_object_field(&object, "allowSelfLoops")? {
        schema.allow_self_loops =
            parse_js_bool_unknown(value, &format!("{context} allowSelfLoops"))?;
    }
    schema.weight = parse_js_optional_object_field(&object, "weight")?
        .map(|value| parse_js_numeric_field_schema(value, &format!("{context} weight")))
        .transpose()?;
    schema.validity = parse_js_optional_object_field(&object, "validity")?
        .map(|value| parse_js_edge_validity_schema(value, &format!("{context} validity")))
        .transpose()?;
    Ok(schema)
}

fn parse_js_property_schema_map(
    value: Unknown<'_>,
    context: &str,
) -> Result<BTreeMap<String, CorePropertySchema>> {
    let object = cast_js_plain_object(value, context)?;
    let keys = js_object_property_names_array(&object)?;
    let mut properties = BTreeMap::new();
    for index in 0..keys.len() {
        let key = keys
            .get::<JsString<'_>>(index)?
            .ok_or_else(|| napi::Error::from_reason(format!("{context} key {index} is missing")))?
            .into_utf8()?
            .into_owned()?;
        let value = object.get::<Unknown<'_>>(&key)?.ok_or_else(|| {
            napi::Error::from_reason(format!("{context}.{key} property schema is missing"))
        })?;
        if is_js_null_or_undefined(&value)? {
            return Err(napi::Error::from_reason(format!(
                "{context}.{key} property schema must be an object"
            )));
        }
        properties.insert(
            key.clone(),
            parse_js_property_schema(value, &format!("{context}.{key}"))?,
        );
    }
    Ok(properties)
}

fn parse_js_property_schema(value: Unknown<'_>, context: &str) -> Result<CorePropertySchema> {
    let object = cast_js_plain_object(value, context)?;
    ensure_only_js_object_fields(
        &object,
        &[
            "required",
            "nullable",
            "types",
            "numericMin",
            "numericMax",
            "stringMinBytes",
            "stringMaxBytes",
            "bytesMinLen",
            "bytesMaxLen",
            "arrayMinItems",
            "arrayMaxItems",
            "mapMinEntries",
            "mapMaxEntries",
            "enumValues",
        ],
        context,
    )?;
    let mut schema = CorePropertySchema::default();
    if let Some(value) = parse_js_optional_object_field(&object, "required")? {
        schema.required = parse_js_bool_unknown(value, &format!("{context} required"))?;
    }
    if let Some(value) = parse_js_optional_object_field(&object, "nullable")? {
        schema.nullable = parse_js_bool_unknown(value, &format!("{context} nullable"))?;
    }
    if let Some(value) = parse_js_optional_object_field(&object, "types")? {
        schema.types = parse_js_schema_value_types(value, &format!("{context} types"))?;
    }
    schema.numeric_min = parse_js_optional_object_field(&object, "numericMin")?
        .map(|value| parse_js_schema_numeric_bound(value, &format!("{context} numericMin")))
        .transpose()?;
    schema.numeric_max = parse_js_optional_object_field(&object, "numericMax")?
        .map(|value| parse_js_schema_numeric_bound(value, &format!("{context} numericMax")))
        .transpose()?;
    schema.string_min_bytes = parse_js_optional_usize_field(
        &object,
        "stringMinBytes",
        &format!("{context} stringMinBytes"),
    )?;
    schema.string_max_bytes = parse_js_optional_usize_field(
        &object,
        "stringMaxBytes",
        &format!("{context} stringMaxBytes"),
    )?;
    schema.bytes_min_len =
        parse_js_optional_usize_field(&object, "bytesMinLen", &format!("{context} bytesMinLen"))?;
    schema.bytes_max_len =
        parse_js_optional_usize_field(&object, "bytesMaxLen", &format!("{context} bytesMaxLen"))?;
    schema.array_min_items = parse_js_optional_usize_field(
        &object,
        "arrayMinItems",
        &format!("{context} arrayMinItems"),
    )?;
    schema.array_max_items = parse_js_optional_usize_field(
        &object,
        "arrayMaxItems",
        &format!("{context} arrayMaxItems"),
    )?;
    schema.map_min_entries = parse_js_optional_usize_field(
        &object,
        "mapMinEntries",
        &format!("{context} mapMinEntries"),
    )?;
    schema.map_max_entries = parse_js_optional_usize_field(
        &object,
        "mapMaxEntries",
        &format!("{context} mapMaxEntries"),
    )?;
    if let Some(value) = parse_js_optional_object_field(&object, "enumValues")? {
        schema.enum_values =
            parse_js_schema_literal_array(value, &format!("{context} enumValues"))?;
    }
    Ok(schema)
}

fn parse_js_schema_numeric_bound(
    value: Unknown<'_>,
    context: &str,
) -> Result<CoreSchemaNumericBound> {
    let object = cast_js_plain_object(value, context)?;
    ensure_only_js_object_fields(&object, &["value", "inclusive"], context)?;
    let value = parse_js_required_object_field(&object, "value", &format!("{context} value"))?;
    let inclusive = parse_js_optional_object_field(&object, "inclusive")?
        .map(|value| parse_js_bool_unknown(value, &format!("{context} inclusive")))
        .transpose()?
        .unwrap_or(true);
    Ok(CoreSchemaNumericBound {
        value: parse_js_schema_literal(value, &format!("{context} value"))?,
        inclusive,
    })
}

fn parse_js_string_field_schema(
    value: Unknown<'_>,
    context: &str,
) -> Result<CoreStringFieldSchema> {
    let object = cast_js_plain_object(value, context)?;
    ensure_only_js_object_fields(&object, &["minBytes", "maxBytes", "enumValues"], context)?;
    Ok(CoreStringFieldSchema {
        min_bytes: parse_js_optional_usize_field(
            &object,
            "minBytes",
            &format!("{context} minBytes"),
        )?,
        max_bytes: parse_js_optional_usize_field(
            &object,
            "maxBytes",
            &format!("{context} maxBytes"),
        )?,
        enum_values: parse_js_optional_object_field(&object, "enumValues")?
            .map(|value| parse_js_string_array_unknown(value, &format!("{context} enumValues")))
            .transpose()?
            .unwrap_or_default(),
    })
}

fn parse_js_numeric_field_schema(
    value: Unknown<'_>,
    context: &str,
) -> Result<CoreNumericFieldSchema> {
    let object = cast_js_plain_object(value, context)?;
    ensure_only_js_object_fields(&object, &["min", "max", "finite"], context)?;
    let mut schema = CoreNumericFieldSchema::default();
    schema.min = parse_js_optional_object_field(&object, "min")?
        .map(|value| parse_js_schema_numeric_bound(value, &format!("{context} min")))
        .transpose()?;
    schema.max = parse_js_optional_object_field(&object, "max")?
        .map(|value| parse_js_schema_numeric_bound(value, &format!("{context} max")))
        .transpose()?;
    if let Some(value) = parse_js_optional_object_field(&object, "finite")? {
        schema.finite = parse_js_bool_unknown(value, &format!("{context} finite"))?;
    }
    Ok(schema)
}

fn parse_js_node_label_constraints(
    value: Unknown<'_>,
    context: &str,
) -> Result<CoreNodeLabelConstraintSchema> {
    let object = cast_js_plain_object(value, context)?;
    ensure_only_js_object_fields(&object, &["allOf", "anyOf", "noneOf"], context)?;
    Ok(CoreNodeLabelConstraintSchema {
        all_of: parse_js_optional_string_array_object_field(
            &object,
            "allOf",
            &format!("{context} allOf"),
        )?,
        any_of: parse_js_optional_string_array_object_field(
            &object,
            "anyOf",
            &format!("{context} anyOf"),
        )?,
        none_of: parse_js_optional_string_array_object_field(
            &object,
            "noneOf",
            &format!("{context} noneOf"),
        )?,
    })
}

fn parse_js_endpoint_label_schema(
    value: Unknown<'_>,
    context: &str,
) -> Result<CoreEndpointLabelSchema> {
    let object = cast_js_plain_object(value, context)?;
    ensure_only_js_object_fields(&object, &["allOf", "anyOf", "noneOf"], context)?;
    Ok(CoreEndpointLabelSchema {
        all_of: parse_js_optional_string_array_object_field(
            &object,
            "allOf",
            &format!("{context} allOf"),
        )?,
        any_of: parse_js_optional_string_array_object_field(
            &object,
            "anyOf",
            &format!("{context} anyOf"),
        )?,
        none_of: parse_js_optional_string_array_object_field(
            &object,
            "noneOf",
            &format!("{context} noneOf"),
        )?,
    })
}

fn parse_js_dense_vector_schema(
    value: Unknown<'_>,
    context: &str,
) -> Result<CoreDenseVectorSchema> {
    let object = cast_js_plain_object(value, context)?;
    ensure_only_js_object_fields(&object, &["presence", "dimension"], context)?;
    let mut schema = CoreDenseVectorSchema::default();
    if let Some(value) = parse_js_optional_object_field(&object, "presence")? {
        schema.presence = parse_js_schema_vector_presence(value, &format!("{context} presence"))?;
    }
    schema.dimension =
        parse_js_optional_usize_field(&object, "dimension", &format!("{context} dimension"))?;
    Ok(schema)
}

fn parse_js_sparse_vector_schema(
    value: Unknown<'_>,
    context: &str,
) -> Result<CoreSparseVectorSchema> {
    let object = cast_js_plain_object(value, context)?;
    ensure_only_js_object_fields(
        &object,
        &["presence", "minEntries", "maxEntries", "maxDimensionId"],
        context,
    )?;
    let mut schema = CoreSparseVectorSchema::default();
    if let Some(value) = parse_js_optional_object_field(&object, "presence")? {
        schema.presence = parse_js_schema_vector_presence(value, &format!("{context} presence"))?;
    }
    schema.min_entries =
        parse_js_optional_usize_field(&object, "minEntries", &format!("{context} minEntries"))?;
    schema.max_entries =
        parse_js_optional_usize_field(&object, "maxEntries", &format!("{context} maxEntries"))?;
    schema.max_dimension_id = parse_js_optional_u32_field(
        &object,
        "maxDimensionId",
        &format!("{context} maxDimensionId"),
    )?;
    Ok(schema)
}

fn parse_js_edge_validity_schema(
    value: Unknown<'_>,
    context: &str,
) -> Result<CoreEdgeValiditySchema> {
    let object = cast_js_plain_object(value, context)?;
    ensure_only_js_object_fields(
        &object,
        &[
            "requireValidFromBeforeValidTo",
            "validFromMin",
            "validFromMax",
            "validToMin",
            "validToMax",
            "allowOpenEndedValidTo",
        ],
        context,
    )?;
    let mut schema = CoreEdgeValiditySchema::default();
    if let Some(value) = parse_js_optional_object_field(&object, "requireValidFromBeforeValidTo")? {
        schema.require_valid_from_before_valid_to =
            parse_js_bool_unknown(value, &format!("{context} requireValidFromBeforeValidTo"))?;
    }
    schema.valid_from_min = parse_js_optional_i64_object_field(
        &object,
        "validFromMin",
        &format!("{context} validFromMin"),
    )?;
    schema.valid_from_max = parse_js_optional_i64_object_field(
        &object,
        "validFromMax",
        &format!("{context} validFromMax"),
    )?;
    schema.valid_to_min = parse_js_optional_i64_object_field(
        &object,
        "validToMin",
        &format!("{context} validToMin"),
    )?;
    schema.valid_to_max = parse_js_optional_i64_object_field(
        &object,
        "validToMax",
        &format!("{context} validToMax"),
    )?;
    if let Some(value) = parse_js_optional_object_field(&object, "allowOpenEndedValidTo")? {
        schema.allow_open_ended_valid_to =
            parse_js_bool_unknown(value, &format!("{context} allowOpenEndedValidTo"))?;
    }
    Ok(schema)
}

fn parse_js_schema_literal_array(value: Unknown<'_>, context: &str) -> Result<Vec<PropValue>> {
    if !value.is_array()? {
        return Err(napi::Error::from_reason(format!(
            "{context} must be an array"
        )));
    }
    let array = unsafe { value.cast::<Array<'_>>()? };
    let mut values = Vec::with_capacity(array.len() as usize);
    for index in 0..array.len() {
        match array.get::<Unknown<'_>>(index)? {
            Some(value) => values.push(parse_js_schema_literal(
                value,
                &format!("{context}[{index}]"),
            )?),
            None => values.push(PropValue::Null),
        }
    }
    Ok(values)
}

fn parse_js_schema_literal(value: Unknown<'_>, context: &str) -> Result<PropValue> {
    match value.get_type()? {
        napi::ValueType::Null | napi::ValueType::Undefined => Ok(PropValue::Null),
        napi::ValueType::Boolean => Ok(PropValue::Bool(unsafe { value.cast::<bool>()? })),
        napi::ValueType::Number => {
            let number = unsafe { value.cast::<f64>()? };
            if !number.is_finite() {
                return Err(napi::Error::from_reason(format!(
                    "{context} number must be finite"
                )));
            }
            if number.fract() == 0.0 && number.abs() <= MAX_SAFE_INTEGER {
                Ok(PropValue::Int(number as i64))
            } else {
                Ok(PropValue::Float(number))
            }
        }
        napi::ValueType::String => Ok(PropValue::String(parse_js_string_unknown(value, context)?)),
        napi::ValueType::Object if value.is_array()? => {
            let array = unsafe { value.cast::<Array<'_>>()? };
            let mut values = Vec::with_capacity(array.len() as usize);
            for index in 0..array.len() {
                match array.get::<Unknown<'_>>(index)? {
                    Some(value) => values.push(parse_js_schema_literal(
                        value,
                        &format!("{context}[{index}]"),
                    )?),
                    None => values.push(PropValue::Null),
                }
            }
            Ok(PropValue::Array(values))
        }
        napi::ValueType::Object
            if value.is_buffer()? || value.is_arraybuffer()? || value.is_typedarray()? =>
        {
            Err(napi::Error::from_reason(format!(
                "{context} bytes literal must use {{ type: 'bytes', value }}"
            )))
        }
        napi::ValueType::Object => parse_js_schema_literal_object(value, context),
        _ => Err(napi::Error::from_reason(format!(
            "{context} must be a schema literal value"
        ))),
    }
}

fn parse_js_schema_literal_object(value: Unknown<'_>, context: &str) -> Result<PropValue> {
    let object = unsafe { value.cast::<Object<'_>>()? };
    if let Some(type_value) = object.get::<Unknown<'_>>("type")? {
        if type_value.get_type()? == napi::ValueType::String {
            let marker = parse_js_string_unknown(type_value, &format!("{context} type"))?;
            if matches!(marker.as_str(), "bytes" | "uint" | "map")
                && js_object_has_exact_fields(&object, &["type", "value"])?
            {
                let value =
                    parse_js_required_object_field(&object, "value", &format!("{context} value"))?;
                return match marker.as_str() {
                    "bytes" => Ok(PropValue::Bytes(parse_js_schema_bytes_value(
                        value,
                        &format!("{context} value"),
                    )?)),
                    "uint" => Ok(PropValue::UInt(parse_js_schema_uint_value(
                        value,
                        &format!("{context} value"),
                    )?)),
                    "map" => Ok(parse_js_schema_literal_raw_map(
                        value,
                        &format!("{context} value"),
                    )?),
                    _ => unreachable!("schema literal marker checked above"),
                };
            }
        }
    }
    parse_js_schema_literal_map_object(&object, context)
}

fn parse_js_schema_literal_raw_map(value: Unknown<'_>, context: &str) -> Result<PropValue> {
    let object = cast_js_plain_object(value, context)?;
    parse_js_schema_literal_map_object(&object, context)
}

fn parse_js_schema_literal_map_object(object: &Object<'_>, context: &str) -> Result<PropValue> {
    let keys = js_object_property_names_array(object)?;
    let mut map = BTreeMap::new();
    for index in 0..keys.len() {
        let key = keys
            .get::<JsString<'_>>(index)?
            .ok_or_else(|| {
                napi::Error::from_reason(format!("{context} map key {index} is missing"))
            })?
            .into_utf8()?
            .into_owned()?;
        let value = object
            .get::<Unknown<'_>>(&key)?
            .ok_or_else(|| napi::Error::from_reason(format!("{context}.{key} is missing")))?;
        map.insert(
            key.clone(),
            parse_js_schema_literal(value, &format!("{context}.{key}"))?,
        );
    }
    Ok(PropValue::Map(map))
}

fn parse_js_schema_bytes_value(value: Unknown<'_>, context: &str) -> Result<Vec<u8>> {
    match value.get_type()? {
        napi::ValueType::Object if value.is_buffer()? => {
            let buffer = unsafe { value.cast::<BufferSlice<'_>>()? };
            Ok(buffer.as_ref().to_vec())
        }
        napi::ValueType::Object if value.is_typedarray()? => {
            let array = unsafe { value.cast::<Uint8ArraySlice<'_>>()? };
            Ok(array.as_ref().to_vec())
        }
        napi::ValueType::Object if value.is_array()? => parse_js_byte_array_unknown(value, context),
        _ => Err(napi::Error::from_reason(format!(
            "{context} must be a number array, Uint8Array, or Buffer"
        ))),
    }
}

fn parse_js_schema_uint_value(value: Unknown<'_>, context: &str) -> Result<u64> {
    match value.get_type()? {
        napi::ValueType::Number => {
            let number = unsafe { value.cast::<f64>()? };
            f64_to_u64_for_context(number, context)
        }
        napi::ValueType::String => {
            let string = parse_js_string_unknown(value, context)?;
            parse_decimal_u64(&string, context)
        }
        _ => Err(napi::Error::from_reason(format!(
            "{context} must be a number or decimal string"
        ))),
    }
}

fn parse_js_schema_additional_properties(
    value: Unknown<'_>,
    context: &str,
) -> Result<CoreSchemaAdditionalProperties> {
    match parse_js_string_unknown(value, context)?.as_str() {
        "allow" => Ok(CoreSchemaAdditionalProperties::Allow),
        "reject" => Ok(CoreSchemaAdditionalProperties::Reject),
        other => Err(napi::Error::from_reason(format!(
            "{context} must be 'allow' or 'reject', got '{other}'"
        ))),
    }
}

fn parse_js_schema_value_types(
    value: Unknown<'_>,
    context: &str,
) -> Result<Vec<CoreSchemaValueType>> {
    let strings = parse_js_string_array_unknown(value, context)?;
    strings
        .into_iter()
        .map(|value| match value.as_str() {
            "bool" => Ok(CoreSchemaValueType::Bool),
            "int" => Ok(CoreSchemaValueType::Int),
            "uint" => Ok(CoreSchemaValueType::UInt),
            "float" => Ok(CoreSchemaValueType::Float),
            "number" => Ok(CoreSchemaValueType::Number),
            "string" => Ok(CoreSchemaValueType::String),
            "bytes" => Ok(CoreSchemaValueType::Bytes),
            "array" => Ok(CoreSchemaValueType::Array),
            "map" => Ok(CoreSchemaValueType::Map),
            other => Err(napi::Error::from_reason(format!(
                "{context} contains invalid value type '{other}'"
            ))),
        })
        .collect()
}

fn parse_js_schema_vector_presence(
    value: Unknown<'_>,
    context: &str,
) -> Result<CoreSchemaVectorPresence> {
    match parse_js_string_unknown(value, context)?.as_str() {
        "optional" => Ok(CoreSchemaVectorPresence::Optional),
        "required" => Ok(CoreSchemaVectorPresence::Required),
        "forbidden" => Ok(CoreSchemaVectorPresence::Forbidden),
        other => Err(napi::Error::from_reason(format!(
            "{context} must be 'optional', 'required', or 'forbidden', got '{other}'"
        ))),
    }
}

fn cast_js_plain_object<'env>(value: Unknown<'env>, context: &str) -> Result<Object<'env>> {
    match value.get_type()? {
        napi::ValueType::Object
            if !value.is_array()?
                && !value.is_buffer()?
                && !value.is_arraybuffer()?
                && !value.is_typedarray()? =>
        unsafe { value.cast::<Object<'env>>() },
        _ => Err(napi::Error::from_reason(format!(
            "{context} must be an object"
        ))),
    }
}

fn ensure_only_js_object_fields(
    object: &Object<'_>,
    allowed: &[&str],
    context: &str,
) -> Result<()> {
    let keys = js_object_property_names_array(object)?;
    for index in 0..keys.len() {
        let key = keys
            .get::<JsString<'_>>(index)?
            .ok_or_else(|| napi::Error::from_reason(format!("{context} key {index} is missing")))?
            .into_utf8()?
            .into_owned()?;
        if !allowed.iter().any(|allowed| *allowed == key) {
            return Err(napi::Error::from_reason(format!(
                "{context} does not accept field '{key}'"
            )));
        }
    }
    Ok(())
}

fn js_object_has_exact_fields(object: &Object<'_>, expected: &[&str]) -> Result<bool> {
    let keys = js_object_property_names_array(object)?;
    if keys.len() as usize != expected.len() {
        return Ok(false);
    }
    for index in 0..keys.len() {
        let key = keys
            .get::<JsString<'_>>(index)?
            .ok_or_else(|| napi::Error::from_reason(format!("object key {index} is missing")))?
            .into_utf8()?
            .into_owned()?;
        if !expected.iter().any(|expected| *expected == key) {
            return Ok(false);
        }
    }
    Ok(true)
}

fn parse_js_optional_object_field<'env>(
    object: &Object<'env>,
    key: &str,
) -> Result<Option<Unknown<'env>>> {
    if !object.has_own_property(key)? {
        return Ok(None);
    }
    match object.get::<Unknown<'env>>(key)? {
        Some(value) if !is_js_null_or_undefined(&value)? => Ok(Some(value)),
        _ => Ok(None),
    }
}

fn parse_js_required_object_field<'env>(
    object: &Object<'env>,
    key: &str,
    context: &str,
) -> Result<Unknown<'env>> {
    match object.get::<Unknown<'env>>(key)? {
        Some(value) if !is_js_null_or_undefined(&value)? => Ok(value),
        _ => Err(napi::Error::from_reason(format!("{context} is required"))),
    }
}

fn parse_js_optional_usize_field(
    object: &Object<'_>,
    key: &str,
    context: &str,
) -> Result<Option<usize>> {
    parse_js_optional_object_field(object, key)?
        .map(|value| parse_js_usize_unknown(value, context))
        .transpose()
}

fn parse_js_optional_u32_field(
    object: &Object<'_>,
    key: &str,
    context: &str,
) -> Result<Option<u32>> {
    parse_js_optional_object_field(object, key)?
        .map(|value| {
            let parsed = parse_js_u64_unknown(value, context)?;
            u32::try_from(parsed)
                .map_err(|_| napi::Error::from_reason(format!("{context} is too large")))
        })
        .transpose()
}

fn parse_js_optional_i64_object_field(
    object: &Object<'_>,
    key: &str,
    context: &str,
) -> Result<Option<i64>> {
    parse_js_optional_object_field(object, key)?
        .map(|value| parse_js_i64_unknown(value, context))
        .transpose()
}

fn parse_js_optional_string_array_object_field(
    object: &Object<'_>,
    key: &str,
    context: &str,
) -> Result<Vec<String>> {
    parse_js_optional_object_field(object, key)?
        .map(|value| parse_js_string_array_unknown(value, context))
        .transpose()
        .map(Option::unwrap_or_default)
}

fn parse_js_string_array_unknown(value: Unknown<'_>, context: &str) -> Result<Vec<String>> {
    if !value.is_array()? {
        return Err(napi::Error::from_reason(format!(
            "{context} must be an array"
        )));
    }
    let array = unsafe { value.cast::<Array<'_>>()? };
    let mut values = Vec::with_capacity(array.len() as usize);
    for index in 0..array.len() {
        let value = array
            .get::<Unknown<'_>>(index)?
            .ok_or_else(|| napi::Error::from_reason(format!("{context}[{index}] is missing")))?;
        values.push(parse_js_string_unknown(
            value,
            &format!("{context}[{index}]"),
        )?);
    }
    Ok(values)
}

fn parse_js_byte_array_unknown(value: Unknown<'_>, context: &str) -> Result<Vec<u8>> {
    if !value.is_array()? {
        return Err(napi::Error::from_reason(format!(
            "{context} must be an array"
        )));
    }
    let array = unsafe { value.cast::<Array<'_>>()? };
    let mut values = Vec::with_capacity(array.len() as usize);
    for index in 0..array.len() {
        let value = array
            .get::<Unknown<'_>>(index)?
            .ok_or_else(|| napi::Error::from_reason(format!("{context}[{index}] is missing")))?;
        let parsed = parse_js_u64_unknown(value, &format!("{context}[{index}]"))?;
        values.push(u8::try_from(parsed).map_err(|_| {
            napi::Error::from_reason(format!("{context}[{index}] must be between 0 and 255"))
        })?);
    }
    Ok(values)
}

fn parse_js_bool_unknown(value: Unknown<'_>, context: &str) -> Result<bool> {
    if value.get_type()? != napi::ValueType::Boolean {
        return Err(napi::Error::from_reason(format!(
            "{context} must be a boolean"
        )));
    }
    unsafe { value.cast::<bool>() }
}

fn parse_js_string_unknown(value: Unknown<'_>, context: &str) -> Result<String> {
    if value.get_type()? != napi::ValueType::String {
        return Err(napi::Error::from_reason(format!(
            "{context} must be a string"
        )));
    }
    unsafe { value.cast::<JsString<'_>>()?.into_utf8()?.into_owned() }
}

fn parse_js_usize_unknown(value: Unknown<'_>, context: &str) -> Result<usize> {
    let number = parse_js_number_unknown(value, context)?;
    f64_to_usize(number, context)
}

fn parse_js_u64_unknown(value: Unknown<'_>, context: &str) -> Result<u64> {
    let number = parse_js_number_unknown(value, context)?;
    f64_to_u64_for_context(number, context)
}

fn parse_js_i64_unknown(value: Unknown<'_>, context: &str) -> Result<i64> {
    let number = parse_js_number_unknown(value, context)?;
    if !number.is_finite()
        || number.fract() != 0.0
        || number < i64::MIN as f64
        || number > i64::MAX as f64
    {
        return Err(napi::Error::from_reason(format!(
            "{context} must be a finite integer"
        )));
    }
    Ok(number as i64)
}

fn parse_js_number_unknown(value: Unknown<'_>, context: &str) -> Result<f64> {
    if value.get_type()? != napi::ValueType::Number {
        return Err(napi::Error::from_reason(format!(
            "{context} must be a number"
        )));
    }
    Ok(unsafe { value.cast::<f64>()? })
}

fn is_js_null_or_undefined(value: &Unknown<'_>) -> Result<bool> {
    Ok(matches!(
        value.get_type()?,
        napi::ValueType::Null | napi::ValueType::Undefined
    ))
}

fn f64_to_u64_for_context(value: f64, context: &str) -> Result<u64> {
    if !(0.0..=MAX_SAFE_INTEGER).contains(&value) || value.fract() != 0.0 {
        return Err(napi::Error::from_reason(format!(
            "{context} must be a safe non-negative integer"
        )));
    }
    Ok(value as u64)
}

fn parse_decimal_u64(value: &str, context: &str) -> Result<u64> {
    if value.is_empty() || value.starts_with('+') || value.starts_with('-') {
        return Err(napi::Error::from_reason(format!(
            "{context} must be an unsigned decimal string"
        )));
    }
    value.parse::<u64>().map_err(|_| {
        napi::Error::from_reason(format!(
            "{context} must be an unsigned decimal string in the u64 range"
        ))
    })
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

    fn read_utf8_with_context(&mut self, len: usize, context: &str) -> napi::Result<&'a str> {
        let bytes = self.read_bytes(len)?;
        std::str::from_utf8(bytes)
            .map_err(|e| napi::Error::from_reason(format!("Invalid UTF-8 in {}: {}", context, e)))
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

const NODE_BATCH_MAGIC: &[u8; 4] = b"OGNB";
const EDGE_BATCH_MAGIC: &[u8; 4] = b"OGEB";
const NODE_BINARY_BATCH_VERSION: u16 = 2;
const EDGE_BINARY_BATCH_VERSION: u16 = 1;
const BINARY_BATCH_HEADER_LEN: usize = 10;
const MAX_NODE_LABELS_PER_NODE: usize = 10;

fn decode_binary_batch_header(
    reader: &mut BinaryReader<'_>,
    expected_magic: &[u8; 4],
    expected_version: u16,
    context: &str,
) -> napi::Result<usize> {
    let magic = reader.read_bytes(4)?;
    if magic != expected_magic {
        return Err(napi::Error::from_reason(format!(
            "Invalid {} binary batch format: missing magic header",
            context
        )));
    }
    let version = reader.read_u16_le()?;
    if version != expected_version {
        if context == "node" && version == 1 {
            return Err(napi::Error::from_reason(
                "Unsupported node binary batch version 1; OGNB v1 single-label buffers are no longer supported, expected version 2".to_string(),
            ));
        }
        return Err(napi::Error::from_reason(format!(
            "Unsupported {} binary batch version {}; expected {}",
            context, version, expected_version
        )));
    }
    Ok(reader.read_u32_le()? as usize)
}

/// Decode a binary buffer into a Vec<CoreNodeInput>.
///
/// Format (little-endian):
///   [magic: 4 bytes "OGNB"][version: u16 = 2][count: u32]
///   per node:
///     [label_count: u8] repeated [label_len: u16][label: utf8][weight: f32]
///     [key_len: u16][key: utf8][props_len: u32][props: json utf8]
fn decode_node_batch(buf: &[u8]) -> napi::Result<Vec<CoreNodeInput>> {
    let mut reader = BinaryReader::new(buf);
    let count = decode_binary_batch_header(
        &mut reader,
        NODE_BATCH_MAGIC,
        NODE_BINARY_BATCH_VERSION,
        "node",
    )?;
    // Cap allocation: minimum v2 node record is 14 bytes.
    let max_possible = buf.len().saturating_sub(BINARY_BATCH_HEADER_LEN) / 14;
    let mut inputs = Vec::with_capacity(count.min(max_possible));

    for _ in 0..count {
        let label_count = reader.read_bytes(1)?[0] as usize;
        if label_count == 0 || label_count > MAX_NODE_LABELS_PER_NODE {
            return Err(napi::Error::from_reason(
                "Binary node label count must be between 1 and 10".to_string(),
            ));
        }
        let mut labels = Vec::with_capacity(label_count);
        for label_index in 0..label_count {
            let label_len = reader.read_u16_le()? as usize;
            if label_len == 0 || label_len > 255 {
                return Err(napi::Error::from_reason(format!(
                    "Binary node label {} length must be between 1 and 255 bytes",
                    label_index
                )));
            }
            labels.push(
                reader
                    .read_utf8_with_context(label_len, "node label")?
                    .to_string(),
            );
        }
        let weight = reader.read_f32_le()?;
        let key_len = reader.read_u16_le()? as usize;
        let key = reader
            .read_utf8_with_context(key_len, "node key")?
            .to_string();
        let props = decode_props_json(&mut reader)?;
        inputs.push(CoreNodeInput {
            labels,
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

/// Decode a binary buffer into a Vec<CoreEdgeInput>.
///
/// Format (little-endian):
///   [magic: 4 bytes "OGEB"][version: u16 = 1][count: u32]
///   per edge:
///     [from: u64][to: u64][label_len: u16][label: utf8][weight: f32]
///     [valid_from: i64][valid_to: i64]
///     [props_len: u32][props: json utf8]
///
/// Sentinel values: valid_from=0 → None (engine default), valid_to=0 → None (engine default).
fn decode_edge_batch(buf: &[u8]) -> napi::Result<Vec<CoreEdgeInput>> {
    let mut reader = BinaryReader::new(buf);
    let count = decode_binary_batch_header(
        &mut reader,
        EDGE_BATCH_MAGIC,
        EDGE_BINARY_BATCH_VERSION,
        "edge",
    )?;
    // Cap allocation: minimum edge record is 35 bytes.
    let max_possible = buf.len().saturating_sub(BINARY_BATCH_HEADER_LEN) / 35;
    let mut inputs = Vec::with_capacity(count.min(max_possible));

    for _ in 0..count {
        let from = reader.read_u64_le()?;
        let to = reader.read_u64_le()?;
        let label_len = reader.read_u16_le()? as usize;
        if label_len == 0 || label_len > 255 {
            return Err(napi::Error::from_reason(
                "Binary edge label length must be between 1 and 255 bytes".to_string(),
            ));
        }
        let label = reader
            .read_utf8_with_context(label_len, "edge label")?
            .to_string();
        let weight = reader.read_f32_le()?;
        let valid_from_raw = reader.read_i64_le()?;
        let valid_to_raw = reader.read_i64_le()?;
        let props = decode_props_json(&mut reader)?;
        inputs.push(CoreEdgeInput {
            from,
            to,
            label,
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

fn neighbor_entries_to_js(entries: Vec<CoreNeighborEntry>) -> Result<Vec<NeighborEntry>> {
    entries.iter().map(neighbor_to_js_entry).collect()
}

fn convert_batch_result(
    map: impl IntoIterator<Item = (u64, Vec<CoreNeighborEntry>)>,
) -> Result<Vec<NeighborBatchEntry>> {
    let mut entries: Vec<NeighborBatchEntry> = map
        .into_iter()
        .map(|(query_id, neighbors)| {
            Ok(NeighborBatchEntry {
                query_node_id: u64_to_f64(query_id)?,
                neighbors: neighbor_entries_to_js(neighbors)?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    // Sort by query_node_id for deterministic output
    entries.sort_by(|a, b| a.query_node_id.total_cmp(&b.query_node_id));
    Ok(entries)
}
