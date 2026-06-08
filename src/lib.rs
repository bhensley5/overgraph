//! # OverGraph
//!
//! An absurdly fast embedded graph database. Pure Rust, sub-microsecond reads.
//!
//! OverGraph stores labeled nodes and edges with schemaless properties (MessagePack),
//! temporal validity windows, exponential decay scoring, and automatic retention
//! policies. It runs inside your process with no separate server or network calls.
//!
//! ## Quick start
//!
//! ```no_run
//! use overgraph::{DatabaseEngine, DbOptions, UpsertNodeOptions, NeighborOptions};
//!
//! use std::path::Path;
//! let mut db = DatabaseEngine::open(Path::new("./my-db"), &DbOptions::default()).unwrap();
//! let id = db.upsert_node("User", "alice", UpsertNodeOptions::default()).unwrap();
//! let neighbors = db.neighbors(id, &NeighborOptions { limit: Some(50), ..Default::default() }).unwrap();
//! db.close().unwrap();
//! ```
//!
//! ## Storage engine
//!
//! Log-structured merge tree: WAL -> memtable -> immutable segments -> background
//! compaction. Reads never block writes. Segments are memory-mapped for zero-copy
//! access through the OS page cache.
//!
//! ## Language connectors
//!
//! Native bindings for Node.js (napi-rs) and Python (PyO3) with full API parity.

// Public API: the types and engine that library users interact with.
pub mod engine;
pub mod error;
pub mod schema;
pub mod types;

// Internal modules stay crate-private so public Rust callers cannot depend on
// storage, WAL, segment, or planner implementation details.
#[doc(hidden)]
pub(crate) mod degree_cache;
#[doc(hidden)]
pub(crate) mod dense_hnsw;
#[doc(hidden)]
pub(crate) mod edge_metadata;
#[doc(hidden)]
pub(crate) mod encoding;
#[doc(hidden)]
pub(crate) mod gql;
#[doc(hidden)]
pub(crate) mod graph_row;
// Diagnostic exception: `overgraph-inspect` uses the read-only manifest loader,
// and `DatabaseEngine::manifest()` remains an explicit introspection surface.
#[doc(hidden)]
pub mod manifest;
#[doc(hidden)]
pub(crate) mod memtable;
#[doc(hidden)]
pub(crate) mod parallel;
#[doc(hidden)]
pub(crate) mod planner_stats;
#[doc(hidden)]
pub(crate) mod property_value_semantics;
#[doc(hidden)]
pub(crate) mod row_projection;
#[doc(hidden)]
pub(crate) mod scrub;
#[doc(hidden)]
pub(crate) mod segment_components;
#[doc(hidden)]
pub(crate) mod segment_reader;
#[doc(hidden)]
pub(crate) mod segment_writer;
#[doc(hidden)]
pub(crate) mod source_list;
#[doc(hidden)]
pub(crate) mod sparse_postings;
#[doc(hidden)]
pub(crate) mod wal;
#[doc(hidden)]
pub(crate) mod wal_sync;

pub use engine::{DatabaseEngine, WriteTxn};
pub use error::EngineError;
pub use schema::{
    DenseVectorSchema, EdgeSchema, EdgeSchemaInfo, EdgeValiditySchema, EndpointLabelSchema,
    GraphSchema, GraphSchemaCheckOptions, GraphSchemaCheckReport, GraphSchemaDropAction,
    GraphSchemaDropTargetResult, GraphSchemaOperation, GraphSchemaOperationKind,
    GraphSchemaPublishResult, GraphSchemaSetOptions, GraphSchemaValidationReportEntry,
    NodeLabelConstraintSchema, NodeSchema, NodeSchemaInfo, NumericFieldSchema, PropertySchema,
    SchemaAdditionalProperties, SchemaCheckOptions, SchemaNumericBound, SchemaSetOptions,
    SchemaTargetKind, SchemaValidationReport, SchemaValueType, SchemaVectorPresence,
    SchemaViolation, SchemaViolationTarget, SparseVectorSchema, StringFieldSchema,
};
pub use types::{
    canonicalize_sparse_vector, canonicalize_sparse_vector_owned, hash_prop_key, hash_prop_value,
    validate_dense_vector, validate_dense_vector_config, AdjacencyExport, AllShortestPathsOptions,
    CompactionPhase, CompactionProgress, CompactionStats, ComponentOptions, ComponentScrubFinding,
    DbOptions, DbStats, DegreeOptions, DenseMetric, DenseVector, DenseVectorConfig, Direction,
    EdgeFilterExpr, EdgeInput, EdgeLabelInfo, EdgePropertyIndexInfo, EdgeQuery, EdgeQueryOrder,
    EdgeView, ExportEdge, ExportOptions, FusionMode, GqlCapSummary, GqlEdge,
    GqlExecutionCapSummary, GqlExecutionExplain, GqlExecutionMode, GqlExecutionOptions,
    GqlExecutionResult, GqlExecutionStats, GqlExplain, GqlIndexExplain, GqlIndexExplainTarget,
    GqlIndexStats, GqlLoweringTarget, GqlMutationExplain, GqlMutationOperationExplain,
    GqlMutationReadPrefixExplain, GqlMutationReturnExplain, GqlMutationStats, GqlNode,
    GqlParamValue, GqlParams, GqlRow, GqlRowOperation, GqlSchemaExplain, GqlSchemaExplainOptions,
    GqlSchemaExplainTarget, GqlSchemaStats, GqlSemanticErrorCode, GqlStatementKind, GqlValue,
    GraphBinaryOp, GraphCapExplain, GraphCaseBranch, GraphCursorExplain, GraphEdgeField,
    GraphEdgePattern, GraphEdgeValue, GraphElementProjection, GraphExecutionSummaries,
    GraphExplainNode, GraphExpr, GraphFunction, GraphNodeField, GraphNodePattern, GraphNodeValue,
    GraphOptionalGroup, GraphOrderDirection, GraphOrderExplain, GraphOrderItem, GraphOutputMode,
    GraphOutputOptions, GraphPageRequest, GraphParamValue, GraphPatch, GraphPath, GraphPathField,
    GraphPathValue, GraphPatternPiece, GraphPipelineCapExplain, GraphPipelineExplain,
    GraphPipelineMatchStage, GraphPipelineOptions, GraphPipelineQuery, GraphPipelineResult,
    GraphPipelineStage, GraphPipelineStageExplain, GraphPipelineStats, GraphProjectItem,
    GraphProjectKind, GraphProjectStage, GraphProjectionExplain, GraphProjectionItems,
    GraphPropertySelection, GraphQueryOptions, GraphReturnItem, GraphReturnProjection, GraphRow,
    GraphRowExplain, GraphRowOperationExplain, GraphRowQuery, GraphRowResult, GraphRowStats,
    GraphSelectedEdgeProjection, GraphSelectedNodeProjection, GraphSelectedPathProjection,
    GraphSelectedProjection, GraphShortestPathEndpoint, GraphShortestPathMode,
    GraphShortestPathStage, GraphSubqueryStage, GraphUnaryOp, GraphUnionStage, GraphValue,
    GraphVariableLengthPattern, GraphVectorSelection, HnswConfig, IntoNodeLabels,
    IsConnectedOptions, LabelMatchMode, ManifestState, NeighborEntry, NeighborOptions,
    NodeFilterExpr, NodeIdBuildHasher, NodeIdHasher, NodeIdMap, NodeIdSet, NodeInput, NodeKeyQuery,
    NodeLabelFilter, NodeLabelInfo, NodePropertyIndexInfo, NodeQuery, NodeQueryOrder, NodeView,
    PageRequest, PageResult, PatchResult, PprAlgorithm, PprApproxMeta, PprOptions, PprResult,
    PropValue, PropertyRangeBound, PropertyRangeCursor, PropertyRangePageRequest,
    PropertyRangePageResult, PrunePolicy, PrunePolicyInfo, PruneResult, QueryEdgeIdsResult,
    QueryEdgesResult, QueryNodeIdsResult, QueryNodesResult, QueryPlan, QueryPlanKind,
    QueryPlanNode, QueryPlanNote, QueryPlanPublicInputs, QueryPlanPublicName, QueryPlanWarning,
    ScoringMode, ScrubFindingType, ScrubReport, SecondaryIndexKind, SecondaryIndexManifestEntry,
    SecondaryIndexState, SecondaryIndexTarget, SegmentInfo, SegmentScrubResult, ShortestPath,
    ShortestPathOptions, SourceSpan, SparseVector, Subgraph, SubgraphOptions, TombstoneEntry,
    TopKOptions, TraversalCursor, TraversalHit, TraversalPageResult, TraverseOptions,
    TxnCommitResult, TxnEdgeRef, TxnEdgeView, TxnIntent, TxnLocalRef, TxnNodeRef, TxnNodeView,
    UpsertEdgeOptions, UpsertNodeOptions, VectorHit, VectorSearchMode, VectorSearchRequest,
    VectorSearchScope, WalSyncMode, DEFAULT_DENSE_EF_SEARCH,
};

#[doc(hidden)]
pub fn gql_referenced_param_names(
    query: &str,
    options: &GqlExecutionOptions,
) -> Result<Vec<String>, EngineError> {
    crate::gql::params::referenced_param_names_for_query(query, options)
}

#[cfg(test)]
mod public_api_boundary_tests {
    fn source(path: &str) -> String {
        std::fs::read_to_string(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(path))
            .unwrap()
    }

    fn rust_source_paths() -> Vec<std::path::PathBuf> {
        fn collect(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
            for entry in std::fs::read_dir(dir).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    collect(&path, out);
                } else if path.extension().is_some_and(|ext| ext == "rs") {
                    out.push(path);
                }
            }
        }

        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        let mut paths = Vec::new();
        for dir in ["src", "tests", "benches"] {
            collect(&root.join(dir), &mut paths);
        }
        paths
    }

    fn assert_files_do_not_contain(paths: &[std::path::PathBuf], patterns: &[String]) {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        for path in paths {
            let contents = std::fs::read_to_string(path).unwrap();
            let display = path.strip_prefix(root).unwrap_or(path).display();
            for pattern in patterns {
                assert!(
                    !contents.contains(pattern),
                    "`{pattern}` must not remain in Rust active edge-label source ({display})"
                );
            }
        }
    }

    #[test]
    fn internal_numeric_records_are_not_publicly_exported() {
        let lib = source("src/lib.rs");
        let types = source("src/types.rs");

        assert!(
            !lib.contains(concat!("pub use types", "::*")),
            "public API must explicitly re-export stable DTOs and not glob-export internal records"
        );
        for forbidden in [
            concat!("pub struct ", "NodeRecord"),
            concat!("pub struct ", "EdgeRecord"),
        ] {
            assert!(
                !types.contains(forbidden),
                "`{forbidden}` would expose internal numeric label/type records"
            );
        }
        for required in [
            concat!("pub(crate) struct ", "NodeRecord"),
            concat!("pub(crate) struct ", "EdgeRecord"),
        ] {
            assert!(
                types.contains(required),
                "`{required}` must remain the internal storage/WAL record boundary"
            );
        }
    }

    #[test]
    fn graph_row_replacement_names_do_not_use_v2_suffixes() {
        let paths = rust_source_paths();
        assert_files_do_not_contain(
            &paths,
            &[
                concat!("GraphRow", "V2").to_string(),
                concat!("GraphRowQuery", "V2").to_string(),
                concat!("GraphPatternPiece", "V2").to_string(),
                concat!("GraphNodePattern", "V2").to_string(),
                concat!("GraphEdgePattern", "V2").to_string(),
                concat!("GraphReturnItem", "V2").to_string(),
                concat!("GraphOutputOptions", "V2").to_string(),
                concat!("GraphQueryOptions", "V2").to_string(),
                concat!("GraphRowResult", "V2").to_string(),
                concat!("GraphRowExplain", "V2").to_string(),
                concat!("NormalizedGraphRowQuery", "V2").to_string(),
            ],
        );
    }

    #[test]
    fn old_graph_pattern_public_exports_are_removed() {
        let lib = source("src/lib.rs");
        let types = source("src/types.rs");

        for forbidden in [
            concat!("Graph", "Pattern", "Query"),
            concat!("Pattern", "Order"),
            concat!("Query", "Match"),
            concat!("Query", "Pattern", "Result"),
        ] {
            assert!(
                !lib.contains(forbidden),
                "`{forbidden}` must not be re-exported from the Rust public API"
            );
        }

        for forbidden in [
            concat!("pub struct ", "Graph", "Pattern", "Query"),
            concat!("pub struct ", "Node", "Pattern"),
            concat!("pub struct ", "Edge", "Pattern"),
            concat!("pub enum ", "Pattern", "Order"),
            concat!("pub struct ", "Query", "Pattern", "Result"),
            concat!("pub struct ", "Query", "Match"),
            concat!("Pattern", "Query"),
            concat!("Pattern", "Expand"),
            concat!("Pattern", "Edge", "Anchor"),
            concat!("Unbounded", "Pattern", "Rejected"),
        ] {
            assert!(
                !types.contains(forbidden),
                "`{forbidden}` must not remain as a public/core graph-pattern DTO"
            );
        }
    }

    #[test]
    fn old_graph_pattern_engine_methods_are_removed() {
        let engine_query = source("src/engine/query.rs");
        for forbidden in [
            concat!("pub fn ", "query", "_pattern"),
            concat!("pub fn ", "explain", "_pattern", "_query"),
        ] {
            assert!(
                !engine_query.contains(forbidden),
                "`{forbidden}` must not remain on DatabaseEngine"
            );
        }

        for path in [
            "src/engine/query_ir.rs",
            "src/engine/query_exec.rs",
            "src/engine/query_plan.rs",
            "src/engine/projection.rs",
        ] {
            let contents = source(path);
            for forbidden in [
                concat!("Graph", "Pattern", "Query"),
                concat!("Pattern", "Order"),
                concat!("Query", "Match"),
                concat!("Query", "Pattern", "Result"),
                concat!("Normalized", "Graph", "Pattern", "Query"),
                concat!("Planned", "Pattern", "Query"),
                concat!("Pattern", "Plan", "Cost"),
                concat!("Pattern", "Query"),
                concat!("Pattern", "Expand"),
                concat!("Pattern", "Edge", "Anchor"),
                concat!("Unbounded", "Pattern", "Rejected"),
                concat!("project", "_pattern", "_rows"),
            ] {
                assert!(
                    !contents.contains(forbidden),
                    "`{forbidden}` must not remain in old graph-pattern core path ({path})"
                );
            }
        }
    }

    #[test]
    fn rust_active_edge_label_id_vocabulary_has_no_backend_type_terms() {
        let paths = rust_source_paths();
        assert_files_do_not_contain(
            &paths,
            &[
                concat!("EDGE", "_TYPE").to_string(),
                concat!("Edge", "Type").to_string(),
                concat!("edge", "_type").to_string(),
                concat!("edge ", "type").to_string(),
                concat!("edge", "-", "type").to_string(),
                concat!("edges_by", "_type").to_string(),
                concat!("visible_edges_by", "_type").to_string(),
                concat!("type", "_edge_index").to_string(),
                concat!("type", "_ids").to_string(),
                concat!("type", "Id").to_string(),
                concat!("type", " IDs").to_string(),
                concat!("distinct ", "type").to_string(),
                concat!("these ", "types").to_string(),
                concat!("filtered", "_types").to_string(),
                concat!("filtered", "_type", "_labels").to_string(),
                concat!("Type ", "filter works").to_string(),
                concat!("let ", "typed").to_string(),
                concat!(":", "type", ":").to_string(),
                concat!(":", "types", ":{").to_string(),
            ],
        );

        let segment_reader = source("src/segment_reader.rs");
        for pattern in [
            concat!("entry", "_type"),
            concat!("let e", "_type"),
            concat!("match e", "_type"),
        ] {
            assert!(
                !segment_reader.contains(pattern),
                "`{pattern}` must not remain in segment edge label readers"
            );
        }
    }

    #[test]
    fn implementation_modules_are_not_public_api() {
        let lib = source("src/lib.rs");
        for forbidden in [
            concat!("pub mod ", "dense_hnsw;"),
            concat!("pub mod ", "encoding;"),
            concat!("pub mod ", "memtable;"),
            concat!("pub mod ", "segment_reader;"),
            concat!("pub mod ", "segment_writer;"),
            concat!("pub mod ", "source_list;"),
            concat!("pub mod ", "sparse_postings;"),
            concat!("pub mod ", "wal;"),
            concat!("pub mod ", "wal_sync;"),
        ] {
            assert!(
                !lib.contains(forbidden),
                "`{forbidden}` would expose implementation internals as Rust public API"
            );
        }
    }

    #[test]
    fn manifest_module_public_surface_stays_read_only_diagnostic_only() {
        let manifest = source("src/manifest.rs");
        for forbidden in [
            concat!("pub fn ", "write_manifest"),
            concat!("pub fn ", "load_manifest("),
            concat!("pub fn ", "default_manifest"),
        ] {
            assert!(
                !manifest.contains(forbidden),
                "`{forbidden}` must stay crate-private; manifest diagnostics expose read-only loading only"
            );
        }
        assert!(
            manifest.contains(concat!("pub fn ", "load_manifest_readonly")),
            "the inspect binary relies on the explicit read-only diagnostic manifest loader"
        );
    }

    #[test]
    fn rust_public_edge_vocabulary_uses_labels() {
        let lib = source("src/lib.rs");
        let types = source("src/types.rs");
        let engine = source("src/engine/mod.rs");
        let read = source("src/engine/read.rs");
        let write = source("src/engine/write.rs");
        let txn = source("src/engine/txn.rs");
        let manifest = source("src/manifest.rs");

        for forbidden in [
            concat!("Edge", "TypeInfo"),
            concat!("pub edge", "_", "type:"),
            concat!("pub edge", "_", "type", "_filter:"),
            concat!("pub edge", "_", "type", "_index:"),
            concat!("pub fn ensure_edge", "_", "type"),
            concat!("pub fn get_edge", "_", "type("),
            concat!("pub fn list_edge", "_", "types"),
            concat!("pub fn ", "edges_by_label_id"),
            concat!("pub fn ", "get_edges_by_label_id"),
            concat!("pub fn ", "count_edges_by_label_id"),
        ] {
            assert!(
                !lib.contains(forbidden)
                    && !types.contains(forbidden)
                    && !engine.contains(forbidden),
                "`{forbidden}` must not remain in the Rust public edge-label API"
            );
        }

        for forbidden in [
            concat!("edge ", "type"),
            concat!("edge", "-", "type"),
            concat!("edge ", "type token"),
            concat!("edge ", "type catalog"),
            concat!("resolved by edge ", "type"),
            concat!("transaction edge ", "type"),
        ] {
            assert!(
                !types.contains(forbidden)
                    && !engine.contains(forbidden)
                    && !read.contains(forbidden)
                    && !write.contains(forbidden)
                    && !txn.contains(forbidden)
                    && !manifest.contains(forbidden),
                "`{forbidden}` must not remain in public-facing Rust edge-label diagnostics or docs"
            );
        }

        for required in [
            "EdgeLabelInfo",
            "pub label: String",
            "pub edge_label_filter: Option<Vec<String>>",
            "pub edge_label_index: u32",
            "pub fn ensure_edge_label",
            "pub fn get_edge_label_id",
            "pub fn get_edge_label(",
            "pub fn list_edge_labels",
            "pub fn edges_by_label",
            "pub fn get_edges_by_label",
            "pub fn count_edges_by_label",
        ] {
            assert!(
                lib.contains(required) || types.contains(required) || engine.contains(required),
                "`{required}` should exist in the Rust public edge-label API"
            );
        }
    }
}
