"""Type stubs for the overgraph Python connector."""

from typing import Any, Callable, Literal, Mapping, Sequence, TypedDict

IntList = list[int] | tuple[int, ...]
StrList = list[str] | tuple[str, ...]
NodeLabels = str | list[str] | tuple[str, ...]
MappingList = list[Mapping[str, Any]] | tuple[Mapping[str, Any], ...]
QueryNodeFilter = Mapping[str, Any]
QueryEdgeFilter = Mapping[str, Any]
NodeLabelFilter = Mapping[str, Any]
GqlScalar = None | bool | int | float | str | bytes
GqlParam = GqlScalar | list["GqlParam"] | tuple["GqlParam", ...] | Mapping[str, "GqlParam"]
GqlParams = Mapping[str, GqlParam]
SchemaValueTypeName = Literal[
    "bool",
    "int",
    "uint",
    "float",
    "number",
    "string",
    "bytes",
    "array",
    "map",
]
SchemaVectorPresenceName = Literal["optional", "required", "forbidden"]
SchemaAdditionalPropertiesMode = Literal["allow", "reject"]
SecondaryIndexKind = Literal["equality", "range"]
SecondaryIndexState = Literal["building", "ready", "failed"]
NodeMetadataIndexField = Literal["id", "key", "weight", "created_at", "updated_at"]
EdgeMetadataIndexField = Literal[
    "id",
    "from",
    "to",
    "weight",
    "created_at",
    "updated_at",
    "valid_from",
    "valid_to",
]

class SecondaryIndexPropertyField(TypedDict):
    source: Literal["property"]
    key: str

class SecondaryIndexMetadataField(TypedDict):
    source: Literal["metadata"]
    field: NodeMetadataIndexField | EdgeMetadataIndexField

SecondaryIndexField = SecondaryIndexPropertyField | SecondaryIndexMetadataField

class SecondaryIndexSpecLike(TypedDict):
    fields: list[SecondaryIndexField]
    kind: SecondaryIndexKind

class SchemaUIntLiteral(TypedDict):
    type: Literal["uint"]
    value: int | str

class SchemaUIntOutputLiteral(TypedDict):
    type: Literal["uint"]
    value: int

SchemaLiteral = (
    None
    | bool
    | int
    | float
    | str
    | bytes
    | bytearray
    | SchemaUIntLiteral
    | list["SchemaLiteral"]
    | tuple["SchemaLiteral", ...]
    | dict[str, "SchemaLiteral"]
)
SchemaOutputLiteral = (
    None
    | bool
    | int
    | float
    | str
    | bytes
    | SchemaUIntOutputLiteral
    | list["SchemaOutputLiteral"]
    | dict[str, "SchemaOutputLiteral"]
)

class SchemaNumericBoundBase(TypedDict):
    value: SchemaLiteral

class SchemaNumericBound(SchemaNumericBoundBase, total=False):
    inclusive: bool

class SchemaOutputNumericBound(TypedDict):
    value: SchemaOutputLiteral
    inclusive: bool

class StringFieldSchema(TypedDict, total=False):
    min_bytes: int | None
    max_bytes: int | None
    enum_values: list[str]

class NumericFieldSchema(TypedDict, total=False):
    min: SchemaNumericBound | None
    max: SchemaNumericBound | None
    finite: bool

class NumericFieldSchemaOutput(TypedDict, total=False):
    min: SchemaOutputNumericBound | None
    max: SchemaOutputNumericBound | None
    finite: bool

class PropertySchema(TypedDict, total=False):
    required: bool
    nullable: bool
    types: list[SchemaValueTypeName]
    numeric_min: SchemaNumericBound | None
    numeric_max: SchemaNumericBound | None
    string_min_bytes: int | None
    string_max_bytes: int | None
    bytes_min_len: int | None
    bytes_max_len: int | None
    array_min_items: int | None
    array_max_items: int | None
    map_min_entries: int | None
    map_max_entries: int | None
    enum_values: list[SchemaLiteral]

class PropertySchemaOutput(TypedDict, total=False):
    required: bool
    nullable: bool
    types: list[SchemaValueTypeName]
    numeric_min: SchemaOutputNumericBound | None
    numeric_max: SchemaOutputNumericBound | None
    string_min_bytes: int | None
    string_max_bytes: int | None
    bytes_min_len: int | None
    bytes_max_len: int | None
    array_min_items: int | None
    array_max_items: int | None
    map_min_entries: int | None
    map_max_entries: int | None
    enum_values: list[SchemaOutputLiteral]

class NodeLabelConstraintSchema(TypedDict, total=False):
    all_of: list[str]
    any_of: list[str]
    none_of: list[str]

class DenseVectorSchema(TypedDict, total=False):
    presence: SchemaVectorPresenceName
    dimension: int | None

class SparseVectorSchema(TypedDict, total=False):
    presence: SchemaVectorPresenceName
    min_entries: int | None
    max_entries: int | None
    max_dimension_id: int | None

class EndpointLabelSchema(TypedDict, total=False):
    all_of: list[str]
    any_of: list[str]
    none_of: list[str]

class EdgeValiditySchema(TypedDict, total=False):
    require_valid_from_before_valid_to: bool
    valid_from_min: int | None
    valid_from_max: int | None
    valid_to_min: int | None
    valid_to_max: int | None
    allow_open_ended_valid_to: bool

class NodeSchema(TypedDict, total=False):
    additional_properties: SchemaAdditionalPropertiesMode
    properties: dict[str, PropertySchema]
    key: StringFieldSchema | None
    label_constraints: NodeLabelConstraintSchema | None
    weight: NumericFieldSchema | None
    dense_vector: DenseVectorSchema | None
    sparse_vector: SparseVectorSchema | None

class NodeSchemaOutput(TypedDict, total=False):
    additional_properties: SchemaAdditionalPropertiesMode
    properties: dict[str, PropertySchemaOutput]
    key: StringFieldSchema | None
    label_constraints: NodeLabelConstraintSchema | None
    weight: NumericFieldSchemaOutput | None
    dense_vector: DenseVectorSchema | None
    sparse_vector: SparseVectorSchema | None

EdgeSchema = TypedDict(
    "EdgeSchema",
    {
        "additional_properties": SchemaAdditionalPropertiesMode,
        "properties": dict[str, PropertySchema],
        "from": EndpointLabelSchema | None,
        "to": EndpointLabelSchema | None,
        "allow_self_loops": bool,
        "weight": NumericFieldSchema | None,
        "validity": EdgeValiditySchema | None,
    },
    total=False,
)
EdgeSchemaOutput = TypedDict(
    "EdgeSchemaOutput",
    {
        "additional_properties": SchemaAdditionalPropertiesMode,
        "properties": dict[str, PropertySchemaOutput],
        "from": EndpointLabelSchema | None,
        "to": EndpointLabelSchema | None,
        "allow_self_loops": bool,
        "weight": NumericFieldSchemaOutput | None,
        "validity": EdgeValiditySchema | None,
    },
    total=False,
)

class SchemaScanOptions(TypedDict, total=False):
    max_violations: int
    chunk_size: int
    scan_limit: int | None

class SchemaViolationTarget(TypedDict, total=False):
    kind: Literal["node", "edge"]
    id: int
    labels: list[str]
    key: str
    label: str
    from_id: int
    to_id: int

class SchemaViolationDict(TypedDict):
    target: SchemaViolationTarget
    path: str
    message: str

class SchemaValidationReportDict(TypedDict):
    checked_records: int
    violation_count: int
    violations: list[SchemaViolationDict]
    truncated: bool
    scan_limit_hit: bool

class NodeSchemaInfoDict(TypedDict):
    label: str
    schema: NodeSchemaOutput

class EdgeSchemaInfoDict(TypedDict):
    label: str
    schema: EdgeSchemaOutput

class GraphSchemaNodeEntry(TypedDict):
    label: str
    schema: NodeSchema

class GraphSchemaEdgeEntry(TypedDict):
    label: str
    schema: EdgeSchema

class GraphSchema(TypedDict, total=False):
    node_schemas: list[GraphSchemaNodeEntry]
    edge_schemas: list[GraphSchemaEdgeEntry]

class GraphSchemaSetNodeOperation(TypedDict):
    kind: Literal["set_node"]
    label: str
    schema: NodeSchema

class GraphSchemaSetEdgeOperation(TypedDict):
    kind: Literal["set_edge"]
    label: str
    schema: EdgeSchema

class GraphSchemaDropNodeOperation(TypedDict):
    kind: Literal["drop_node"]
    label: str

class GraphSchemaDropEdgeOperation(TypedDict):
    kind: Literal["drop_edge"]
    label: str

GraphSchemaOperation = (
    GraphSchemaSetNodeOperation
    | GraphSchemaSetEdgeOperation
    | GraphSchemaDropNodeOperation
    | GraphSchemaDropEdgeOperation
)
GraphSchemaOperationKindName = Literal["add", "set", "drop", "drop_all", "check_add", "check_set"]
GraphSchemaTargetKindName = Literal["node", "edge"]
GraphSchemaDropActionName = Literal["dropped", "not_found"]

# ============================================================
# Data types
# ============================================================

class ScrubReport:
    total_components_checked: int
    total_components_ok: int
    total_components_failed: int
    total_bytes_digested: int
    duration_ms: int
    @property
    def segments(self) -> list[SegmentScrubResult]: ...
    def __repr__(self) -> str: ...

class SegmentScrubResult:
    segment_id: int
    components_ok: int
    bytes_digested: int
    @property
    def findings(self) -> list[ComponentScrubFinding]: ...
    def __repr__(self) -> str: ...

class ComponentScrubFinding:
    component_kind: str
    finding_type: str
    detail: str
    def __repr__(self) -> str: ...

class DbStats:
    pending_wal_bytes: int
    segment_count: int
    node_tombstone_count: int
    edge_tombstone_count: int
    last_compaction_ms: int | None
    wal_sync_mode: str
    active_memtable_bytes: int
    immutable_memtable_bytes: int
    immutable_memtable_count: int
    pending_flush_count: int
    active_wal_generation_id: int
    oldest_retained_wal_generation_id: int
    def __repr__(self) -> str: ...

class NodeView:
    id: int
    labels: list[str]
    key: str
    props: dict[str, Any]
    weight: float
    dense_vector: list[float] | None
    sparse_vector: list[tuple[int, float]] | None
    created_at: int
    updated_at: int
    def __repr__(self) -> str: ...

class EdgeView:
    id: int
    from_id: int
    to_id: int
    label: str
    props: dict[str, Any]
    weight: float
    valid_from: int
    valid_to: int
    created_at: int
    updated_at: int
    def __repr__(self) -> str: ...

class PatchResult:
    node_ids: list[int]
    edge_ids: list[int]
    def __repr__(self) -> str: ...

class TxnCommitResult:
    node_ids: list[int]
    edge_ids: list[int]
    node_aliases: dict[str, int]
    edge_aliases: dict[str, int]
    def __repr__(self) -> str: ...

class NeighborEntry:
    node_id: int
    edge_id: int
    label: str
    weight: float
    valid_from: int
    valid_to: int
    def __repr__(self) -> str: ...

class TraversalHit:
    node_id: int
    depth: int
    via_edge_id: int | None
    score: float | None
    def __repr__(self) -> str: ...

class VectorHit:
    node_id: int
    score: float
    def __repr__(self) -> str: ...

class NodePropertyIndexInfo:
    index_id: int
    label: str
    fields: list[SecondaryIndexField]
    kind: SecondaryIndexKind
    state: SecondaryIndexState
    last_error: str | None
    compound: bool
    def __repr__(self) -> str: ...

class EdgePropertyIndexInfo:
    index_id: int
    label: str
    fields: list[SecondaryIndexField]
    kind: SecondaryIndexKind
    state: SecondaryIndexState
    last_error: str | None
    compound: bool
    def __repr__(self) -> str: ...

class NodeSchemaInfo:
    label: str
    @property
    def schema(self) -> NodeSchemaOutput: ...
    def __repr__(self) -> str: ...

class EdgeSchemaInfo:
    label: str
    @property
    def schema(self) -> EdgeSchemaOutput: ...
    def __repr__(self) -> str: ...

class SchemaValidationReport:
    checked_records: int
    violation_count: int
    truncated: bool
    scan_limit_hit: bool
    @property
    def violations(self) -> list[SchemaViolation]: ...
    def __repr__(self) -> str: ...

class GraphSchemaValidationReportEntry:
    target_kind: GraphSchemaTargetKindName
    label: str
    report: SchemaValidationReport
    def __repr__(self) -> str: ...

class GraphSchemaCheckReport:
    operation: GraphSchemaOperationKindName
    checked_records: int
    violation_count: int
    truncated: bool
    scan_limit_hit: bool
    @property
    def entries(self) -> list[GraphSchemaValidationReportEntry]: ...
    def __repr__(self) -> str: ...

class GraphSchemaDropTargetResult:
    target_kind: GraphSchemaTargetKindName
    label: str
    action: GraphSchemaDropActionName
    def __repr__(self) -> str: ...

class GraphSchemaPublishResult:
    operation: GraphSchemaOperationKindName
    validation: GraphSchemaCheckReport
    targets_published: int
    targets_dropped: int
    node_schemas_dropped: int
    edge_schemas_dropped: int
    @property
    def node_schemas(self) -> list[NodeSchemaInfo]: ...
    @property
    def edge_schemas(self) -> list[EdgeSchemaInfo]: ...
    @property
    def drop_targets(self) -> list[GraphSchemaDropTargetResult]: ...
    def __repr__(self) -> str: ...

class SchemaViolation:
    path: str
    message: str
    @property
    def target(self) -> SchemaViolationTarget: ...
    def __repr__(self) -> str: ...

class PropertyRangeBound:
    value: int | float
    inclusive: bool
    domain: str
    def __init__(self, value: int | float, *, inclusive: bool = True, domain: str) -> None: ...
    def __repr__(self) -> str: ...

class PropertyRangeCursor:
    value: int | float
    node_id: int
    domain: str
    def __init__(self, value: int | float, node_id: int, *, domain: str) -> None: ...
    def __repr__(self) -> str: ...

class TraversalCursor:
    depth: int
    last_node_id: int
    def __init__(self, depth: int, last_node_id: int) -> None: ...
    def __repr__(self) -> str: ...

class ShortestPath:
    nodes: list[int]
    edges: list[int]
    total_cost: float
    def __repr__(self) -> str: ...

class Subgraph:
    nodes: list[NodeView]
    edges: list[EdgeView]
    def __repr__(self) -> str: ...

class PruneResult:
    nodes_pruned: int
    edges_pruned: int
    def __repr__(self) -> str: ...

class NamedPrunePolicy:
    name: str
    max_age_ms: int | None
    max_weight: float | None
    label: str | None
    def __repr__(self) -> str: ...

class NodeLabelInfo:
    label: str
    label_id: int
    def __repr__(self) -> str: ...

class EdgeLabelInfo:
    label: str
    label_id: int
    def __repr__(self) -> str: ...

class SegmentInfo:
    id: int
    node_count: int
    edge_count: int
    def __repr__(self) -> str: ...

class CompactionStats:
    segments_merged: int
    nodes_kept: int
    nodes_removed: int
    edges_kept: int
    edges_removed: int
    duration_ms: int
    output_segment_id: int
    nodes_auto_pruned: int
    edges_auto_pruned: int
    def __repr__(self) -> str: ...

class CompactionProgress:
    phase: str
    segments_processed: int
    total_segments: int
    records_processed: int
    total_records: int
    def __repr__(self) -> str: ...

class IdArray:
    """Lazy sequence wrapper. Data stays in Rust, converted on access."""
    def __len__(self) -> int: ...
    def __getitem__(self, index: int) -> int: ...
    def __iter__(self) -> IdArrayIter: ...
    def __bool__(self) -> bool: ...
    def __contains__(self, val: int) -> bool: ...
    def __eq__(self, other: object) -> bool: ...
    def __repr__(self) -> str: ...
    def to_list(self) -> list[int]: ...

class IdArrayIter:
    def __iter__(self) -> IdArrayIter: ...
    def __next__(self) -> int: ...

class IdPageResult:
    items: IdArray
    next_cursor: int | None
    def __len__(self) -> int: ...
    def __bool__(self) -> bool: ...
    def __repr__(self) -> str: ...

class NodePageResult:
    items: list[NodeView]
    next_cursor: int | None
    def __len__(self) -> int: ...
    def __bool__(self) -> bool: ...
    def __repr__(self) -> str: ...

class EdgePageResult:
    items: list[EdgeView]
    next_cursor: int | None
    def __len__(self) -> int: ...
    def __bool__(self) -> bool: ...
    def __repr__(self) -> str: ...

class NeighborPageResult:
    items: list[NeighborEntry]
    next_cursor: int | None
    def __len__(self) -> int: ...
    def __bool__(self) -> bool: ...
    def __repr__(self) -> str: ...

class PropertyRangePageResult:
    items: IdArray
    next_cursor: PropertyRangeCursor | None
    def __len__(self) -> int: ...
    def __bool__(self) -> bool: ...
    def __repr__(self) -> str: ...

class TraversalPageResult:
    items: list[TraversalHit]
    next_cursor: TraversalCursor | None
    def __len__(self) -> int: ...
    def __bool__(self) -> bool: ...
    def __repr__(self) -> str: ...

class PprResult:
    node_ids: list[int]
    scores: list[float]
    iterations: int
    converged: bool
    algorithm: str
    approx: PprApproxMeta | None
    def __repr__(self) -> str: ...

class PprApproxMeta:
    residual_tolerance: float
    pushes: int
    max_remaining_residual: float
    def __repr__(self) -> str: ...

class ExportEdge:
    from_id: int
    to_id: int
    edge_label_index: int
    weight: float | None
    def __repr__(self) -> str: ...

class AdjacencyExport:
    node_ids: list[int]
    node_labels: list[str]
    node_label_indexes: list[list[int]]
    edge_labels: list[str]
    edges: list[ExportEdge]
    def __repr__(self) -> str: ...

class NodeQueryRequest:
    label_filter: NodeLabelFilter | None
    ids: IntList | None
    keys: StrList | None
    filter: QueryNodeFilter | None
    order_by: str | None
    limit: int | None
    after: int | None
    allow_full_scan: bool
    def __init__(
        self,
        label_filter: NodeLabelFilter | None = None,
        ids: IntList | None = None,
        keys: StrList | None = None,
        filter: QueryNodeFilter | None = None,
        order_by: str | None = None,
        limit: int | None = None,
        after: int | None = None,
        allow_full_scan: bool = False,
    ) -> None: ...
    def to_dict(self) -> dict[str, Any]: ...

QueryNodeRequest = NodeQueryRequest

class EdgeQueryRequest:
    label: str | None
    ids: IntList | None
    from_ids: IntList | None
    to_ids: IntList | None
    endpoint_ids: IntList | None
    filter: QueryEdgeFilter | None
    limit: int | None
    after: int | None
    allow_full_scan: bool
    def __init__(
        self,
        label: str | None = None,
        ids: IntList | None = None,
        from_ids: IntList | None = None,
        to_ids: IntList | None = None,
        endpoint_ids: IntList | None = None,
        filter: QueryEdgeFilter | None = None,
        limit: int | None = None,
        after: int | None = None,
        allow_full_scan: bool = False,
    ) -> None: ...
    def to_dict(self) -> dict[str, Any]: ...

QueryEdgeRequest = EdgeQueryRequest

GraphParamValue = None | bool | int | float | str | bytes | list["GraphParamValue"] | dict[str, "GraphParamValue"]
GraphExpr = Any
GraphRowRequest = Mapping[str, Any]
GraphPipelineRequest = dict[str, Any]

class GraphPathValue(TypedDict, total=False):
    node_ids: list[int]
    edge_ids: list[int]
    nodes: list[dict[str, Any]]
    edges: list[dict[str, Any]]

GraphValue = None | bool | int | float | str | bytes | list["GraphValue"] | dict[str, Any] | GraphPathValue

class GraphRowStats(TypedDict):
    rows_returned: int
    rows_after_filter: int
    rows_seen_for_page: int
    intermediate_bindings_peak: int
    frontier_peak: int
    paths_enumerated: int
    db_hits: int
    elapsed_us: int | None
    effective_at_epoch: int
    warnings: list[str]

class GraphRowResult(TypedDict):
    columns: list[str]
    rows: list[dict[str, GraphValue]] | list[list[GraphValue]]
    next_cursor: str | None
    stats: GraphRowStats
    plan: dict[str, Any] | None

GraphRowExplain = dict[str, Any]

class GraphPipelineStats(TypedDict):
    rows_returned: int
    rows_entered_pipeline: int
    rows_after_filter: int
    intermediate_rows: int
    pipeline_rows_materialized: int
    groups: int
    collect_items: int
    union_branches: int
    union_dedup_keys: int
    subquery_invocations: int
    subquery_cache_hits: int
    shortest_path_pairs: int
    shortest_path_cache_hits: int
    db_hits: int
    elapsed_us: int | None
    effective_at_epoch: int
    warnings: list[str]

class GraphPipelineResult(TypedDict):
    columns: list[str]
    rows: list[dict[str, GraphValue]] | list[list[GraphValue]]
    next_cursor: str | None
    stats: GraphPipelineStats
    plan: dict[str, Any] | None

GraphPipelineExplain = dict[str, Any]

QueryPlanKind = Literal["node_query", "edge_query"]
QueryPlanWarning = Literal[
    "missing_ready_index",
    "using_fallback_scan",
    "full_scan_requires_opt_in",
    "full_scan_explicitly_allowed",
    "edge_property_post_filter",
    "index_skipped_as_broad",
    "candidate_cap_exceeded",
    "range_candidate_cap_exceeded",
    "timestamp_candidate_cap_exceeded",
    "verify_only_filter",
    "boolean_branch_fallback",
    "planning_probe_budget_exceeded",
    "compound_index_prefix_not_satisfied",
    "unknown_node_label",
    "unknown_edge_label",
]
QueryPlanNote = Literal[
    "node_label_any_dedupe_before_pagination",
    "node_label_any_final_verification",
    "node_label_all_superset_verification",
    "stale_node_label_membership_verification",
]

class QueryPlanCompoundIndexDetails(TypedDict):
    index_id: int
    target_kind: Literal["node", "edge"]
    label: str | None
    kind: SecondaryIndexKind
    fields: list[SecondaryIndexField]
    compound: bool
    matched_prefix_len: int
    range_field: SecondaryIndexField | None
    in_expansions: int
    estimated_candidates: int | None
    coverage: str
    residual_predicates: int
    final_verification: bool
    fallback_reason: str | None

class QueryPlanSimpleNode(TypedDict):
    kind: Literal[
        "explicit_ids",
        "key_lookup",
        "node_label_index",
        "node_label_any_index",
        "property_equality_index",
        "property_range_index",
        "timestamp_index",
        "adjacency_expansion",
        "explicit_edge_ids",
        "edge_label_index",
        "edge_triple_index",
        "edge_endpoint_adjacency",
        "edge_weight_index",
        "edge_updated_at_index",
        "edge_validity_index",
        "edge_metadata_scan",
        "edge_property_equality_index",
        "edge_property_range_index",
        "fallback_node_label_scan",
        "fallback_full_node_scan",
        "fallback_edge_label_scan",
        "fallback_full_edge_scan",
        "empty_result",
    ]

class QueryPlanCompoundEqualityNode(TypedDict):
    kind: Literal["compound_equality_index"]
    details: QueryPlanCompoundIndexDetails

class QueryPlanCompoundRangeNode(TypedDict):
    kind: Literal["compound_range_index"]
    details: QueryPlanCompoundIndexDetails

class QueryPlanInputsNode(TypedDict):
    kind: Literal["intersect", "union"]
    inputs: list["QueryPlanNode"]

class QueryPlanInputNode(TypedDict):
    kind: Literal["verify_node_filter", "verify_edge_filter", "verify_edge_predicates"]
    input: "QueryPlanNode"

QueryPlanNode = QueryPlanSimpleNode | QueryPlanCompoundEqualityNode | QueryPlanCompoundRangeNode | QueryPlanInputsNode | QueryPlanInputNode

class QueryPlanPublicName(TypedDict, total=False):
    alias: str | None
    name: str
    known: bool
    mode: LabelMatchMode | None

class QueryPlanPublicInputs(TypedDict):
    node_labels: list[QueryPlanPublicName]
    edge_labels: list[QueryPlanPublicName]

class QueryPlan(TypedDict):
    kind: QueryPlanKind
    root: QueryPlanNode
    estimated_candidates: int | None
    warnings: list[QueryPlanWarning]
    notes: list[QueryPlanNote]
    public_inputs: QueryPlanPublicInputs

class GqlNode(TypedDict, total=False):
    id: int
    labels: list[str]
    key: str
    props: dict[str, Any]
    weight: float
    created_at: int
    updated_at: int
    dense_vector: list[float]
    sparse_vector: list[tuple[int, float]]

class GqlEdge(TypedDict, total=False):
    id: int
    from_id: int
    to_id: int
    label: str
    props: dict[str, Any]
    weight: float
    created_at: int
    updated_at: int
    valid_from: int
    valid_to: int

class GqlPath(TypedDict, total=False):
    node_ids: list[int]
    edge_ids: list[int]
    nodes: list[GqlNode]
    edges: list[GqlEdge]

GqlValue = GqlScalar | list["GqlValue"] | dict[str, "GqlValue"] | GqlNode | GqlEdge | GqlPath
GqlStatementKind = Literal["query", "mutation", "schema", "index"]

class GqlExecutionStats(TypedDict):
    rows_returned: int
    rows_matched: int
    rows_after_filter: int
    intermediate_bindings: int
    db_hits: int
    elapsed_us: int | None
    warnings: list[str]

class GqlCapSummary(TypedDict):
    allow_full_scan: bool
    max_rows: int
    max_intermediate_bindings: int
    max_skip: int
    max_query_bytes: int
    max_param_bytes: int
    max_ast_depth: int
    max_literal_items: int

class GqlExecutionCapSummary(TypedDict):
    allow_full_scan: bool
    max_rows: int
    max_cursor_bytes: int
    max_mutation_rows: int
    max_mutation_ops: int
    max_pipeline_rows: int
    max_groups: int
    max_collect_items: int
    max_union_branches: int
    max_subquery_invocations: int
    max_subquery_depth: int
    max_shortest_path_pairs: int
    max_query_bytes: int
    max_param_bytes: int
    max_ast_depth: int
    max_literal_items: int
    max_intermediate_bindings: int
    max_frontier: int
    max_path_hops: int
    max_paths_per_start: int
    max_order_materialization: int
    max_skip: int

class GqlReadExplain(TypedDict):
    columns: list[str]
    target: str
    native_plan: QueryPlan | None
    pushed_down: list[str]
    residual: list[str]
    projection: list[str]
    row_ops: list[str]
    caps: GqlCapSummary
    warnings: list[str]

class GqlMutationReadPrefixExplain(TypedDict):
    graph_row_target: GqlReadExplain
    internal_columns: list[str]
    target_aliases: list[str]
    expression_columns: int

class GqlMutationOperationExplain(TypedDict):
    op: str
    target_alias: str | None
    row_multiplicity: str
    detail: str

class GqlMutationReturnExplain(TypedDict):
    columns: list[str]
    order_items: int
    skip: int
    limit: int | None
    post_commit_hydration: str

class GqlMutationExplain(TypedDict):
    read_prefix: GqlMutationReadPrefixExplain | None
    operations: list[GqlMutationOperationExplain]
    return_plan: GqlMutationReturnExplain | None
    would_create_node_labels: list[str]
    would_create_edge_labels: list[str]
    uses_transaction_snapshot: bool
    uses_write_txn: bool
    replacement_adapters: bool
    atomic_commit: bool

class GqlSchemaExplainTarget(TypedDict):
    target_kind: str
    label: str | None
    action: str | None

class GqlSchemaExplainOptions(TypedDict):
    max_violations: int | None
    chunk_size: int | None
    scan_limit: int | None

class GqlSchemaExplain(TypedDict):
    operation: str
    targets: list[GqlSchemaExplainTarget]
    replaces_entire_catalog: bool
    publishes_manifest: bool
    validates_existing_data: bool
    uses_core_write_queue: bool
    side_effect_free: bool
    options: GqlSchemaExplainOptions

class GqlIndexExplainField(TypedDict):
    source: str
    key: str | None
    field: str | None

class GqlIndexExplainTarget(TypedDict):
    target_kind: str
    label: str | None
    fields: list[GqlIndexExplainField]
    kind: str | None
    action: str | None
    compound: bool

class GqlIndexExplain(TypedDict):
    operation: str
    targets: list[GqlIndexExplainTarget]
    uses_core_write_queue: bool
    publishes_manifest: bool
    creates_labels: bool
    schedules_background_build: bool
    drops_index_data_async: bool
    side_effect_free: bool

class GqlExecutionExplain(TypedDict):
    kind: GqlStatementKind
    columns: list[str]
    read: GqlReadExplain | None
    mutation: GqlMutationExplain | None
    schema: GqlSchemaExplain | None
    index: GqlIndexExplain | None
    caps: GqlExecutionCapSummary
    warnings: list[str]
    notes: list[str]

class GqlMutationStats(TypedDict):
    rows_matched: int
    mutation_rows: int
    mutation_ops: int
    nodes_created: int
    nodes_updated: int
    nodes_deleted: int
    edges_created: int
    edges_updated: int
    edges_deleted: int
    labels_added: int
    labels_removed: int
    properties_set: int
    properties_removed: int
    skipped_null_targets: int
    duplicate_targets: int
    db_hits: int
    elapsed_us: int | None
    warnings: list[str]

class GqlSchemaStats(TypedDict):
    operation: str
    targets_checked: int
    targets_published: int
    targets_dropped: int
    checked_records: int
    violation_count: int
    truncated: bool
    scan_limit_hit: bool
    elapsed_us: int | None
    warnings: list[str]

class GqlIndexStats(TypedDict):
    operation: str
    indexes_ensured: int
    indexes_dropped: int
    indexes_returned: int
    elapsed_us: int | None
    warnings: list[str]

class GqlExecutionResult(TypedDict):
    kind: GqlStatementKind
    columns: list[str]
    rows: list[dict[str, GqlValue]] | list[list[GqlValue]]
    next_cursor: str | None
    stats: GqlExecutionStats
    mutation_stats: GqlMutationStats | None
    schema_stats: GqlSchemaStats | None
    index_stats: GqlIndexStats | None
    plan: GqlExecutionExplain | None

# ============================================================
# Exception
# ============================================================

class OverGraphError(Exception): ...

# ============================================================
# Sync API
# ============================================================

class OverGraph:
    @staticmethod
    def open(path: str, **kwargs: Any) -> OverGraph: ...
    def close(self, force: bool = False) -> None: ...
    def __enter__(self) -> OverGraph: ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_val: BaseException | None = None,
        exc_tb: Any = None,
    ) -> bool: ...
    def stats(self) -> DbStats: ...
    def scrub(self) -> ScrubReport: ...

    # Catalog
    def ensure_node_label(self, label: str) -> int: ...
    def ensure_edge_label(self, label: str) -> int: ...
    def get_node_label_id(self, label: str) -> int | None: ...
    def get_edge_label_id(self, label: str) -> int | None: ...
    def get_node_label(self, label_id: int) -> str | None: ...
    def get_edge_label(self, label_id: int) -> str | None: ...
    def list_node_labels(self) -> list[NodeLabelInfo]: ...
    def list_edge_labels(self) -> list[EdgeLabelInfo]: ...

    # Schemas
    def set_node_schema(self, label: str, schema: NodeSchema, *, max_violations: int = 1, chunk_size: int = 4096, scan_limit: int | None = None) -> NodeSchemaInfo: ...
    def check_node_schema(self, label: str, schema: NodeSchema, *, max_violations: int = 100, chunk_size: int = 4096, scan_limit: int | None = None) -> SchemaValidationReport: ...
    def drop_node_schema(self, label: str) -> bool: ...
    def get_node_schema(self, label: str) -> NodeSchemaInfo | None: ...
    def list_node_schemas(self) -> list[NodeSchemaInfo]: ...
    def set_edge_schema(self, label: str, schema: EdgeSchema, *, max_violations: int = 1, chunk_size: int = 4096, scan_limit: int | None = None) -> EdgeSchemaInfo: ...
    def check_edge_schema(self, label: str, schema: EdgeSchema, *, max_violations: int = 100, chunk_size: int = 4096, scan_limit: int | None = None) -> SchemaValidationReport: ...
    def drop_edge_schema(self, label: str) -> bool: ...
    def get_edge_schema(self, label: str) -> EdgeSchemaInfo | None: ...
    def list_edge_schemas(self) -> list[EdgeSchemaInfo]: ...
    def set_graph_schema(self, schema: GraphSchema, *, max_violations: int = 1, chunk_size: int = 4096, scan_limit: int | None = None) -> GraphSchemaPublishResult: ...
    def alter_graph_schema(self, operations: list[GraphSchemaOperation] | tuple[GraphSchemaOperation, ...], *, max_violations: int = 1, chunk_size: int = 4096, scan_limit: int | None = None) -> GraphSchemaPublishResult: ...
    def check_graph_schema_set(self, schema: GraphSchema, *, max_violations: int = 100, chunk_size: int = 4096, scan_limit: int | None = None) -> GraphSchemaCheckReport: ...
    def check_graph_schema_add(self, schema: GraphSchema, *, max_violations: int = 100, chunk_size: int = 4096, scan_limit: int | None = None) -> GraphSchemaCheckReport: ...
    def drop_graph_schema(self) -> GraphSchemaPublishResult: ...

    # Single CRUD
    def upsert_node(
        self,
        labels: NodeLabels,
        key: str,
        *,
        props: dict[str, Any] | None = None,
        weight: float = 1.0,
        dense_vector: list[float] | None = None,
        sparse_vector: list[tuple[int, float]] | None = None,
    ) -> int: ...
    def add_node_label(self, node_id: int, label: str) -> bool: ...
    def remove_node_label(self, node_id: int, label: str) -> bool: ...
    def upsert_edge(
        self,
        from_id: int,
        to_id: int,
        label: str,
        *,
        props: dict[str, Any] | None = None,
        weight: float = 1.0,
        valid_from: int | None = None,
        valid_to: int | None = None,
    ) -> int: ...
    def get_node(self, node_id: int) -> NodeView | None: ...
    def get_edge(self, edge_id: int) -> EdgeView | None: ...
    def get_node_by_key(self, label: str, key: str) -> NodeView | None: ...
    def get_edge_by_triple(self, from_id: int, to_id: int, label: str) -> EdgeView | None: ...
    def delete_node(self, node_id: int) -> None: ...
    def delete_edge(self, edge_id: int) -> None: ...
    def invalidate_edge(self, edge_id: int, valid_to: int) -> EdgeView | None: ...

    # Batch
    def batch_upsert_nodes(self, nodes: list[dict[str, Any]]) -> list[int]: ...
    def batch_upsert_edges(self, edges: list[dict[str, Any]]) -> list[int]: ...
    def get_nodes(self, node_ids: list[int]) -> list[NodeView | None]: ...
    def get_nodes_by_keys(self, keys: list[dict[str, Any]]) -> list[NodeView | None]: ...
    def get_edges(self, edge_ids: list[int]) -> list[EdgeView | None]: ...
    def graph_patch(self, patch: dict[str, Any]) -> PatchResult: ...
    def begin_write_txn(self) -> WriteTxn: ...

    # Queries
    def find_nodes(self, label: str, prop_key: str, prop_value: Any) -> IdArray: ...
    def query_node_ids(self, request: dict[str, Any] | NodeQueryRequest) -> IdPageResult: ...
    def query_nodes(self, request: dict[str, Any] | NodeQueryRequest) -> NodePageResult: ...
    def query_edge_ids(self, request: dict[str, Any] | EdgeQueryRequest) -> IdPageResult: ...
    def query_edges(self, request: dict[str, Any] | EdgeQueryRequest) -> EdgePageResult: ...
    def query_graph_rows(self, request: dict[str, Any] | GraphRowRequest) -> GraphRowResult: ...
    def query_graph_pipeline(self, request: GraphPipelineRequest) -> GraphPipelineResult: ...
    def explain_node_query(self, request: dict[str, Any] | NodeQueryRequest) -> QueryPlan: ...
    def explain_edge_query(self, request: dict[str, Any] | EdgeQueryRequest) -> QueryPlan: ...
    def explain_graph_rows(self, request: dict[str, Any] | GraphRowRequest) -> GraphRowExplain: ...
    def explain_graph_pipeline(self, request: GraphPipelineRequest) -> GraphPipelineExplain: ...
    def execute_gql(
        self,
        query: str,
        params: GqlParams | None = None,
        *,
        mode: Literal["auto", "read_only"] = "auto",
        allow_full_scan: bool = False,
        max_rows: int | None = None,
        cursor: str | None = None,
        max_cursor_bytes: int | None = None,
        max_mutation_rows: int | None = None,
        max_mutation_ops: int | None = None,
        max_pipeline_rows: int | None = None,
        max_groups: int | None = None,
        max_collect_items: int | None = None,
        max_union_branches: int | None = None,
        max_subquery_invocations: int | None = None,
        max_subquery_depth: int | None = None,
        max_shortest_path_pairs: int | None = None,
        max_intermediate_bindings: int | None = None,
        max_frontier: int | None = None,
        max_path_hops: int | None = None,
        max_paths_per_start: int | None = None,
        max_order_materialization: int | None = None,
        max_skip: int | None = None,
        max_query_bytes: int | None = None,
        max_param_bytes: int | None = None,
        max_ast_depth: int | None = None,
        max_literal_items: int | None = None,
        include_plan: bool = False,
        profile: bool = False,
        compact_rows: bool = False,
        include_vectors: bool = False,
    ) -> GqlExecutionResult: ...
    def explain_gql(
        self,
        query: str,
        params: GqlParams | None = None,
        *,
        mode: Literal["auto", "read_only"] = "auto",
        allow_full_scan: bool = False,
        max_rows: int | None = None,
        cursor: str | None = None,
        max_cursor_bytes: int | None = None,
        max_mutation_rows: int | None = None,
        max_mutation_ops: int | None = None,
        max_pipeline_rows: int | None = None,
        max_groups: int | None = None,
        max_collect_items: int | None = None,
        max_union_branches: int | None = None,
        max_subquery_invocations: int | None = None,
        max_subquery_depth: int | None = None,
        max_shortest_path_pairs: int | None = None,
        max_intermediate_bindings: int | None = None,
        max_frontier: int | None = None,
        max_path_hops: int | None = None,
        max_paths_per_start: int | None = None,
        max_order_materialization: int | None = None,
        max_skip: int | None = None,
        max_query_bytes: int | None = None,
        max_param_bytes: int | None = None,
        max_ast_depth: int | None = None,
        max_literal_items: int | None = None,
        include_plan: bool = False,
        profile: bool = False,
        compact_rows: bool = False,
        include_vectors: bool = False,
    ) -> GqlExecutionExplain: ...
    def ensure_node_property_index(self, label: str, spec: SecondaryIndexSpecLike) -> NodePropertyIndexInfo: ...
    def drop_node_property_index(self, label: str, spec: SecondaryIndexSpecLike) -> bool: ...
    def list_node_property_indexes(self) -> list[NodePropertyIndexInfo]: ...
    def ensure_edge_property_index(self, label: str, spec: SecondaryIndexSpecLike) -> EdgePropertyIndexInfo: ...
    def drop_edge_property_index(self, label: str, spec: SecondaryIndexSpecLike) -> bool: ...
    def list_edge_property_indexes(self) -> list[EdgePropertyIndexInfo]: ...
    def nodes_by_labels(self, labels: NodeLabels) -> IdArray: ...
    def edges_by_label(self, label: str) -> IdArray: ...
    def get_nodes_by_labels(self, labels: NodeLabels) -> list[NodeView]: ...
    def get_edges_by_label(self, label: str) -> list[EdgeView]: ...
    def count_nodes_by_labels(self, labels: NodeLabels) -> int: ...
    def count_edges_by_label(self, label: str) -> int: ...
    def find_nodes_by_time_range(self, label: str, from_ms: int, to_ms: int) -> IdArray: ...
    def find_nodes_range(self, label: str, prop_key: str, lower: PropertyRangeBound | None = None, upper: PropertyRangeBound | None = None) -> IdArray: ...

    # Binary batch
    def batch_upsert_nodes_binary(self, buffer: bytes) -> list[int]: ...
    def batch_upsert_edges_binary(self, buffer: bytes) -> list[int]: ...

    # Traversal
    def neighbors(
        self,
        node_id: int,
        *,
        direction: str = "outgoing",
        edge_label_filter: list[str] | None = None,
        limit: int | None = None,
        at_epoch: int | None = None,
        decay_lambda: float | None = None,
    ) -> list[NeighborEntry]: ...
    def traverse(
        self,
        start: int,
        max_depth: int,
        *,
        min_depth: int = 1,
        direction: str = "outgoing",
        edge_label_filter: list[str] | None = None,
        emit_node_label_filter: NodeLabelFilter | None = None,
        at_epoch: int | None = None,
        decay_lambda: float | None = None,
        limit: int | None = None,
        cursor: TraversalCursor | None = None,
    ) -> TraversalPageResult: ...
    def top_k_neighbors(
        self,
        node_id: int,
        k: int,
        *,
        direction: str = "outgoing",
        edge_label_filter: list[str] | None = None,
        scoring: str = "weight",
        at_epoch: int | None = None,
        decay_lambda: float | None = None,
    ) -> list[NeighborEntry]: ...
    def extract_subgraph(
        self,
        start_node_id: int,
        max_depth: int,
        *,
        direction: str = "outgoing",
        edge_label_filter: list[str] | None = None,
        node_label_filter: NodeLabelFilter | None = None,
        at_epoch: int | None = None,
    ) -> Subgraph: ...
    def neighbors_batch(
        self,
        node_ids: list[int],
        *,
        direction: str = "outgoing",
        edge_label_filter: list[str] | None = None,
        at_epoch: int | None = None,
        decay_lambda: float | None = None,
    ) -> dict[int, list[NeighborEntry]]: ...

    # Degree counts + aggregations
    def degree(
        self,
        node_id: int,
        *,
        direction: str = "outgoing",
        edge_label_filter: list[str] | None = None,
        at_epoch: int | None = None,
    ) -> int: ...
    def sum_edge_weights(
        self,
        node_id: int,
        *,
        direction: str = "outgoing",
        edge_label_filter: list[str] | None = None,
        at_epoch: int | None = None,
    ) -> float: ...
    def avg_edge_weight(
        self,
        node_id: int,
        *,
        direction: str = "outgoing",
        edge_label_filter: list[str] | None = None,
        at_epoch: int | None = None,
    ) -> float | None: ...
    def degrees(
        self,
        node_ids: list[int],
        *,
        direction: str = "outgoing",
        edge_label_filter: list[str] | None = None,
        at_epoch: int | None = None,
    ) -> dict[int, int]: ...

    # Shortest path
    def shortest_path(
        self,
        from_id: int,
        to_id: int,
        *,
        direction: str = "outgoing",
        edge_label_filter: list[str] | None = None,
        weight_field: str | None = None,
        at_epoch: int | None = None,
        max_depth: int | None = None,
        max_cost: float | None = None,
    ) -> ShortestPath | None: ...
    def is_connected(
        self,
        from_id: int,
        to_id: int,
        *,
        direction: str = "outgoing",
        edge_label_filter: list[str] | None = None,
        at_epoch: int | None = None,
        max_depth: int | None = None,
    ) -> bool: ...
    def all_shortest_paths(
        self,
        from_id: int,
        to_id: int,
        *,
        direction: str = "outgoing",
        edge_label_filter: list[str] | None = None,
        weight_field: str | None = None,
        at_epoch: int | None = None,
        max_depth: int | None = None,
        max_cost: float | None = None,
        max_paths: int | None = None,
    ) -> list[ShortestPath]: ...

    # Retention
    def prune(
        self,
        *,
        max_age_ms: int | None = None,
        max_weight: float | None = None,
        label: str | None = None,
    ) -> PruneResult: ...
    def set_prune_policy(
        self,
        name: str,
        *,
        max_age_ms: int | None = None,
        max_weight: float | None = None,
        label: str | None = None,
    ) -> None: ...
    def remove_prune_policy(self, name: str) -> bool: ...
    def list_prune_policies(self) -> list[NamedPrunePolicy]: ...

    # Maintenance
    def sync(self) -> None: ...
    def flush(self) -> SegmentInfo | None: ...
    def ingest_mode(self) -> None: ...
    def end_ingest(self) -> CompactionStats | None: ...
    def compact(self) -> CompactionStats | None: ...
    def compact_with_progress(self, callback: Callable[[CompactionProgress], bool]) -> CompactionStats | None: ...

    # Pagination
    def nodes_by_labels_paged(self, labels: NodeLabels, *, limit: int | None = None, after: int | None = None) -> IdPageResult: ...
    def edges_by_label_paged(self, label: str, *, limit: int | None = None, after: int | None = None) -> IdPageResult: ...
    def get_nodes_by_labels_paged(self, labels: NodeLabels, *, limit: int | None = None, after: int | None = None) -> NodePageResult: ...
    def get_edges_by_label_paged(self, label: str, *, limit: int | None = None, after: int | None = None) -> EdgePageResult: ...
    def find_nodes_paged(self, label: str, prop_key: str, prop_value: Any, *, limit: int | None = None, after: int | None = None) -> IdPageResult: ...
    def find_nodes_by_time_range_paged(self, label: str, from_ms: int, to_ms: int, *, limit: int | None = None, after: int | None = None) -> IdPageResult: ...
    def find_nodes_range_paged(self, label: str, prop_key: str, lower: PropertyRangeBound | None = None, upper: PropertyRangeBound | None = None, *, limit: int | None = None, after: PropertyRangeCursor | None = None) -> PropertyRangePageResult: ...
    def neighbors_paged(
        self,
        node_id: int,
        *,
        direction: str = "outgoing",
        edge_label_filter: list[str] | None = None,
        limit: int | None = None,
        after: int | None = None,
        at_epoch: int | None = None,
        decay_lambda: float | None = None,
    ) -> NeighborPageResult: ...

    # Analytics
    def personalized_pagerank(
        self,
        seed_node_ids: list[int],
        *,
        algorithm: str | None = None,
        damping_factor: float | None = None,
        max_iterations: int | None = None,
        epsilon: float | None = None,
        approx_residual_tolerance: float | None = None,
        edge_label_filter: list[str] | None = None,
        max_results: int | None = None,
    ) -> PprResult: ...
    def export_adjacency(
        self,
        *,
        node_label_filter: NodeLabelFilter | None = None,
        edge_label_filter: list[str] | None = None,
        include_weights: bool = True,
    ) -> AdjacencyExport: ...

    # Connected components
    def connected_components(
        self,
        *,
        edge_label_filter: list[str] | None = None,
        node_label_filter: NodeLabelFilter | None = None,
        at_epoch: int | None = None,
    ) -> dict[int, int]: ...
    def component_of(
        self,
        node_id: int,
        *,
        edge_label_filter: list[str] | None = None,
        node_label_filter: NodeLabelFilter | None = None,
        at_epoch: int | None = None,
    ) -> list[int]: ...

    # Vector search
    def vector_search(
        self,
        mode: str,
        k: int,
        *,
        dense_query: list[float] | None = None,
        sparse_query: list[tuple[int, float]] | None = None,
        label_filter: NodeLabelFilter | None = None,
        ef_search: int | None = None,
        scope_start_node_id: int | None = None,
        scope_max_depth: int | None = None,
        scope_direction: str | None = None,
        scope_edge_label_filter: list[str] | None = None,
        scope_at_epoch: int | None = None,
        dense_weight: float | None = None,
        sparse_weight: float | None = None,
        fusion_mode: str | None = None,
    ) -> list[VectorHit]: ...

class WriteTxn:
    def upsert_node(
        self,
        labels: NodeLabels,
        key: str,
        *,
        props: dict[str, Any] | None = None,
        weight: float = 1.0,
        dense_vector: list[float] | None = None,
        sparse_vector: list[tuple[int, float]] | None = None,
    ) -> dict[str, Any]: ...
    def upsert_node_as(
        self,
        alias: str,
        labels: NodeLabels,
        key: str,
        *,
        props: dict[str, Any] | None = None,
        weight: float = 1.0,
        dense_vector: list[float] | None = None,
        sparse_vector: list[tuple[int, float]] | None = None,
    ) -> dict[str, Any]: ...
    def add_node_label(self, target: dict[str, Any], label: str) -> bool: ...
    def remove_node_label(self, target: dict[str, Any], label: str) -> bool: ...
    def upsert_edge(
        self,
        from_ref: dict[str, Any],
        to_ref: dict[str, Any],
        label: str,
        *,
        props: dict[str, Any] | None = None,
        weight: float = 1.0,
        valid_from: int | None = None,
        valid_to: int | None = None,
    ) -> dict[str, Any]: ...
    def upsert_edge_as(
        self,
        alias: str,
        from_ref: dict[str, Any],
        to_ref: dict[str, Any],
        label: str,
        *,
        props: dict[str, Any] | None = None,
        weight: float = 1.0,
        valid_from: int | None = None,
        valid_to: int | None = None,
    ) -> dict[str, Any]: ...
    def delete_node(self, target: dict[str, Any]) -> None: ...
    def delete_edge(self, target: dict[str, Any]) -> None: ...
    def invalidate_edge(self, target: dict[str, Any], valid_to: int) -> None: ...
    def stage(self, operations: list[dict[str, Any]]) -> None: ...
    def get_node(self, target: dict[str, Any]) -> dict[str, Any] | None: ...
    def get_edge(self, target: dict[str, Any]) -> dict[str, Any] | None: ...
    def get_node_by_key(self, label: str, key: str) -> dict[str, Any] | None: ...
    def get_edge_by_triple(
        self,
        from_ref: dict[str, Any],
        to_ref: dict[str, Any],
        label: str,
    ) -> dict[str, Any] | None: ...
    def commit(self) -> TxnCommitResult: ...
    def rollback(self) -> None: ...

# ============================================================
# Async API
# ============================================================

class AsyncOverGraph:
    @staticmethod
    async def open(path: str, **kwargs: Any) -> AsyncOverGraph: ...
    async def close(self, force: bool = False) -> None: ...
    async def __aenter__(self) -> AsyncOverGraph: ...
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None: ...
    async def stats(self) -> DbStats: ...
    async def scrub(self) -> ScrubReport: ...

    # Catalog
    async def ensure_node_label(self, label: str) -> int: ...
    async def ensure_edge_label(self, label: str) -> int: ...
    async def get_node_label_id(self, label: str) -> int | None: ...
    async def get_edge_label_id(self, label: str) -> int | None: ...
    async def get_node_label(self, label_id: int) -> str | None: ...
    async def get_edge_label(self, label_id: int) -> str | None: ...
    async def list_node_labels(self) -> list[NodeLabelInfo]: ...
    async def list_edge_labels(self) -> list[EdgeLabelInfo]: ...

    # Schemas
    async def set_node_schema(self, label: str, schema: NodeSchema, *, max_violations: int = 1, chunk_size: int = 4096, scan_limit: int | None = None) -> NodeSchemaInfo: ...
    async def check_node_schema(self, label: str, schema: NodeSchema, *, max_violations: int = 100, chunk_size: int = 4096, scan_limit: int | None = None) -> SchemaValidationReport: ...
    async def drop_node_schema(self, label: str) -> bool: ...
    async def get_node_schema(self, label: str) -> NodeSchemaInfo | None: ...
    async def list_node_schemas(self) -> list[NodeSchemaInfo]: ...
    async def set_edge_schema(self, label: str, schema: EdgeSchema, *, max_violations: int = 1, chunk_size: int = 4096, scan_limit: int | None = None) -> EdgeSchemaInfo: ...
    async def check_edge_schema(self, label: str, schema: EdgeSchema, *, max_violations: int = 100, chunk_size: int = 4096, scan_limit: int | None = None) -> SchemaValidationReport: ...
    async def drop_edge_schema(self, label: str) -> bool: ...
    async def get_edge_schema(self, label: str) -> EdgeSchemaInfo | None: ...
    async def list_edge_schemas(self) -> list[EdgeSchemaInfo]: ...
    async def set_graph_schema(self, schema: GraphSchema, *, max_violations: int = 1, chunk_size: int = 4096, scan_limit: int | None = None) -> GraphSchemaPublishResult: ...
    async def alter_graph_schema(self, operations: list[GraphSchemaOperation] | tuple[GraphSchemaOperation, ...], *, max_violations: int = 1, chunk_size: int = 4096, scan_limit: int | None = None) -> GraphSchemaPublishResult: ...
    async def check_graph_schema_set(self, schema: GraphSchema, *, max_violations: int = 100, chunk_size: int = 4096, scan_limit: int | None = None) -> GraphSchemaCheckReport: ...
    async def check_graph_schema_add(self, schema: GraphSchema, *, max_violations: int = 100, chunk_size: int = 4096, scan_limit: int | None = None) -> GraphSchemaCheckReport: ...
    async def drop_graph_schema(self) -> GraphSchemaPublishResult: ...

    # Single CRUD
    async def upsert_node(self, labels: NodeLabels, key: str, *, props: dict[str, Any] | None = None, weight: float = 1.0, dense_vector: list[float] | None = None, sparse_vector: list[tuple[int, float]] | None = None) -> int: ...
    async def add_node_label(self, node_id: int, label: str) -> bool: ...
    async def remove_node_label(self, node_id: int, label: str) -> bool: ...
    async def upsert_edge(self, from_id: int, to_id: int, label: str, *, props: dict[str, Any] | None = None, weight: float = 1.0, valid_from: int | None = None, valid_to: int | None = None) -> int: ...
    async def get_node(self, node_id: int) -> NodeView | None: ...
    async def get_edge(self, edge_id: int) -> EdgeView | None: ...
    async def get_node_by_key(self, label: str, key: str) -> NodeView | None: ...
    async def get_edge_by_triple(self, from_id: int, to_id: int, label: str) -> EdgeView | None: ...
    async def delete_node(self, node_id: int) -> None: ...
    async def delete_edge(self, edge_id: int) -> None: ...
    async def invalidate_edge(self, edge_id: int, valid_to: int) -> EdgeView | None: ...

    # Batch
    async def batch_upsert_nodes(self, nodes: list[dict[str, Any]]) -> list[int]: ...
    async def batch_upsert_edges(self, edges: list[dict[str, Any]]) -> list[int]: ...
    async def batch_upsert_nodes_binary(self, buffer: bytes) -> list[int]: ...
    async def batch_upsert_edges_binary(self, buffer: bytes) -> list[int]: ...
    async def get_nodes(self, node_ids: list[int]) -> list[NodeView | None]: ...
    async def get_nodes_by_keys(self, keys: list[dict[str, Any]]) -> list[NodeView | None]: ...
    async def get_edges(self, edge_ids: list[int]) -> list[EdgeView | None]: ...
    async def graph_patch(self, patch: dict[str, Any]) -> PatchResult: ...
    async def begin_write_txn(self) -> AsyncWriteTxn: ...

    # Queries
    async def find_nodes(self, label: str, prop_key: str, prop_value: Any) -> IdArray: ...
    async def query_node_ids(self, request: dict[str, Any] | NodeQueryRequest) -> IdPageResult: ...
    async def query_nodes(self, request: dict[str, Any] | NodeQueryRequest) -> NodePageResult: ...
    async def query_edge_ids(self, request: dict[str, Any] | EdgeQueryRequest) -> IdPageResult: ...
    async def query_edges(self, request: dict[str, Any] | EdgeQueryRequest) -> EdgePageResult: ...
    async def query_graph_rows(self, request: dict[str, Any] | GraphRowRequest) -> GraphRowResult: ...
    async def query_graph_pipeline(self, request: GraphPipelineRequest) -> GraphPipelineResult: ...
    async def explain_node_query(self, request: dict[str, Any] | NodeQueryRequest) -> QueryPlan: ...
    async def explain_edge_query(self, request: dict[str, Any] | EdgeQueryRequest) -> QueryPlan: ...
    async def explain_graph_rows(self, request: dict[str, Any] | GraphRowRequest) -> GraphRowExplain: ...
    async def explain_graph_pipeline(self, request: GraphPipelineRequest) -> GraphPipelineExplain: ...
    async def execute_gql(
        self,
        query: str,
        params: GqlParams | None = None,
        *,
        mode: Literal["auto", "read_only"] = "auto",
        allow_full_scan: bool = False,
        max_rows: int | None = None,
        cursor: str | None = None,
        max_cursor_bytes: int | None = None,
        max_mutation_rows: int | None = None,
        max_mutation_ops: int | None = None,
        max_pipeline_rows: int | None = None,
        max_groups: int | None = None,
        max_collect_items: int | None = None,
        max_union_branches: int | None = None,
        max_subquery_invocations: int | None = None,
        max_subquery_depth: int | None = None,
        max_shortest_path_pairs: int | None = None,
        max_intermediate_bindings: int | None = None,
        max_frontier: int | None = None,
        max_path_hops: int | None = None,
        max_paths_per_start: int | None = None,
        max_order_materialization: int | None = None,
        max_skip: int | None = None,
        max_query_bytes: int | None = None,
        max_param_bytes: int | None = None,
        max_ast_depth: int | None = None,
        max_literal_items: int | None = None,
        include_plan: bool = False,
        profile: bool = False,
        compact_rows: bool = False,
        include_vectors: bool = False,
    ) -> GqlExecutionResult: ...
    async def explain_gql(
        self,
        query: str,
        params: GqlParams | None = None,
        *,
        mode: Literal["auto", "read_only"] = "auto",
        allow_full_scan: bool = False,
        max_rows: int | None = None,
        cursor: str | None = None,
        max_cursor_bytes: int | None = None,
        max_mutation_rows: int | None = None,
        max_mutation_ops: int | None = None,
        max_pipeline_rows: int | None = None,
        max_groups: int | None = None,
        max_collect_items: int | None = None,
        max_union_branches: int | None = None,
        max_subquery_invocations: int | None = None,
        max_subquery_depth: int | None = None,
        max_shortest_path_pairs: int | None = None,
        max_intermediate_bindings: int | None = None,
        max_frontier: int | None = None,
        max_path_hops: int | None = None,
        max_paths_per_start: int | None = None,
        max_order_materialization: int | None = None,
        max_skip: int | None = None,
        max_query_bytes: int | None = None,
        max_param_bytes: int | None = None,
        max_ast_depth: int | None = None,
        max_literal_items: int | None = None,
        include_plan: bool = False,
        profile: bool = False,
        compact_rows: bool = False,
        include_vectors: bool = False,
    ) -> GqlExecutionExplain: ...
    async def ensure_node_property_index(self, label: str, spec: SecondaryIndexSpecLike) -> NodePropertyIndexInfo: ...
    async def drop_node_property_index(self, label: str, spec: SecondaryIndexSpecLike) -> bool: ...
    async def list_node_property_indexes(self) -> list[NodePropertyIndexInfo]: ...
    async def ensure_edge_property_index(self, label: str, spec: SecondaryIndexSpecLike) -> EdgePropertyIndexInfo: ...
    async def drop_edge_property_index(self, label: str, spec: SecondaryIndexSpecLike) -> bool: ...
    async def list_edge_property_indexes(self) -> list[EdgePropertyIndexInfo]: ...
    async def nodes_by_labels(self, labels: NodeLabels) -> IdArray: ...
    async def edges_by_label(self, label: str) -> IdArray: ...
    async def get_nodes_by_labels(self, labels: NodeLabels) -> list[NodeView]: ...
    async def get_edges_by_label(self, label: str) -> list[EdgeView]: ...
    async def count_nodes_by_labels(self, labels: NodeLabels) -> int: ...
    async def count_edges_by_label(self, label: str) -> int: ...
    async def find_nodes_by_time_range(self, label: str, from_ms: int, to_ms: int) -> IdArray: ...
    async def find_nodes_range(self, label: str, prop_key: str, lower: PropertyRangeBound | None = None, upper: PropertyRangeBound | None = None) -> IdArray: ...

    # Traversal
    async def neighbors(self, node_id: int, *, direction: str = "outgoing", edge_label_filter: list[str] | None = None, limit: int | None = None, at_epoch: int | None = None, decay_lambda: float | None = None) -> list[NeighborEntry]: ...
    async def traverse(self, start: int, max_depth: int, *, min_depth: int = 1, direction: str = "outgoing", edge_label_filter: list[str] | None = None, emit_node_label_filter: NodeLabelFilter | None = None, at_epoch: int | None = None, decay_lambda: float | None = None, limit: int | None = None, cursor: TraversalCursor | None = None) -> TraversalPageResult: ...
    async def top_k_neighbors(self, node_id: int, k: int, *, direction: str = "outgoing", edge_label_filter: list[str] | None = None, scoring: str = "weight", at_epoch: int | None = None, decay_lambda: float | None = None) -> list[NeighborEntry]: ...
    async def extract_subgraph(self, start_node_id: int, max_depth: int, *, direction: str = "outgoing", edge_label_filter: list[str] | None = None, node_label_filter: NodeLabelFilter | None = None, at_epoch: int | None = None) -> Subgraph: ...
    async def neighbors_batch(self, node_ids: list[int], *, direction: str = "outgoing", edge_label_filter: list[str] | None = None, at_epoch: int | None = None, decay_lambda: float | None = None) -> dict[int, list[NeighborEntry]]: ...

    # Degree counts + aggregations
    async def degree(self, node_id: int, *, direction: str = "outgoing", edge_label_filter: list[str] | None = None, at_epoch: int | None = None) -> int: ...
    async def sum_edge_weights(self, node_id: int, *, direction: str = "outgoing", edge_label_filter: list[str] | None = None, at_epoch: int | None = None) -> float: ...
    async def avg_edge_weight(self, node_id: int, *, direction: str = "outgoing", edge_label_filter: list[str] | None = None, at_epoch: int | None = None) -> float | None: ...
    async def degrees(self, node_ids: list[int], *, direction: str = "outgoing", edge_label_filter: list[str] | None = None, at_epoch: int | None = None) -> dict[int, int]: ...

    # Shortest path
    async def shortest_path(self, from_id: int, to_id: int, *, direction: str = "outgoing", edge_label_filter: list[str] | None = None, weight_field: str | None = None, at_epoch: int | None = None, max_depth: int | None = None, max_cost: float | None = None) -> ShortestPath | None: ...
    async def is_connected(self, from_id: int, to_id: int, *, direction: str = "outgoing", edge_label_filter: list[str] | None = None, at_epoch: int | None = None, max_depth: int | None = None) -> bool: ...
    async def all_shortest_paths(self, from_id: int, to_id: int, *, direction: str = "outgoing", edge_label_filter: list[str] | None = None, weight_field: str | None = None, at_epoch: int | None = None, max_depth: int | None = None, max_cost: float | None = None, max_paths: int | None = None) -> list[ShortestPath]: ...

    # Retention
    async def prune(self, *, max_age_ms: int | None = None, max_weight: float | None = None, label: str | None = None) -> PruneResult: ...
    async def set_prune_policy(self, name: str, *, max_age_ms: int | None = None, max_weight: float | None = None, label: str | None = None) -> None: ...
    async def remove_prune_policy(self, name: str) -> bool: ...
    async def list_prune_policies(self) -> list[NamedPrunePolicy]: ...

    # Maintenance
    async def sync(self) -> None: ...
    async def flush(self) -> SegmentInfo | None: ...
    async def ingest_mode(self) -> None: ...
    async def end_ingest(self) -> CompactionStats | None: ...
    async def compact(self) -> CompactionStats | None: ...
    async def compact_with_progress(self, callback: Callable[[CompactionProgress], bool]) -> CompactionStats | None: ...

    # Pagination
    async def nodes_by_labels_paged(self, labels: NodeLabels, *, limit: int | None = None, after: int | None = None) -> IdPageResult: ...
    async def edges_by_label_paged(self, label: str, *, limit: int | None = None, after: int | None = None) -> IdPageResult: ...
    async def get_nodes_by_labels_paged(self, labels: NodeLabels, *, limit: int | None = None, after: int | None = None) -> NodePageResult: ...
    async def get_edges_by_label_paged(self, label: str, *, limit: int | None = None, after: int | None = None) -> EdgePageResult: ...
    async def find_nodes_paged(self, label: str, prop_key: str, prop_value: Any, *, limit: int | None = None, after: int | None = None) -> IdPageResult: ...
    async def find_nodes_by_time_range_paged(self, label: str, from_ms: int, to_ms: int, *, limit: int | None = None, after: int | None = None) -> IdPageResult: ...
    async def find_nodes_range_paged(self, label: str, prop_key: str, lower: PropertyRangeBound | None = None, upper: PropertyRangeBound | None = None, *, limit: int | None = None, after: PropertyRangeCursor | None = None) -> PropertyRangePageResult: ...
    async def neighbors_paged(self, node_id: int, *, direction: str = "outgoing", edge_label_filter: list[str] | None = None, limit: int | None = None, after: int | None = None, at_epoch: int | None = None, decay_lambda: float | None = None) -> NeighborPageResult: ...

    # Analytics
    async def personalized_pagerank(self, seed_node_ids: list[int], *, algorithm: str | None = None, damping_factor: float | None = None, max_iterations: int | None = None, epsilon: float | None = None, approx_residual_tolerance: float | None = None, edge_label_filter: list[str] | None = None, max_results: int | None = None) -> PprResult: ...
    async def export_adjacency(self, *, node_label_filter: NodeLabelFilter | None = None, edge_label_filter: list[str] | None = None, include_weights: bool = True) -> AdjacencyExport: ...
    async def connected_components(self, *, edge_label_filter: list[str] | None = None, node_label_filter: NodeLabelFilter | None = None, at_epoch: int | None = None) -> dict[int, int]: ...
    async def component_of(self, node_id: int, *, edge_label_filter: list[str] | None = None, node_label_filter: NodeLabelFilter | None = None, at_epoch: int | None = None) -> list[int]: ...
    async def vector_search(self, mode: str, k: int, *, dense_query: list[float] | None = None, sparse_query: list[tuple[int, float]] | None = None, label_filter: NodeLabelFilter | None = None, ef_search: int | None = None, scope_start_node_id: int | None = None, scope_max_depth: int | None = None, scope_direction: str | None = None, scope_edge_label_filter: list[str] | None = None, scope_at_epoch: int | None = None, dense_weight: float | None = None, sparse_weight: float | None = None, fusion_mode: str | None = None) -> list[VectorHit]: ...

class AsyncWriteTxn:
    async def upsert_node(self, labels: NodeLabels, key: str, *, props: dict[str, Any] | None = None, weight: float = 1.0, dense_vector: list[float] | None = None, sparse_vector: list[tuple[int, float]] | None = None) -> dict[str, Any]: ...
    async def upsert_node_as(self, alias: str, labels: NodeLabels, key: str, *, props: dict[str, Any] | None = None, weight: float = 1.0, dense_vector: list[float] | None = None, sparse_vector: list[tuple[int, float]] | None = None) -> dict[str, Any]: ...
    async def add_node_label(self, target: dict[str, Any], label: str) -> bool: ...
    async def remove_node_label(self, target: dict[str, Any], label: str) -> bool: ...
    async def upsert_edge(self, from_ref: dict[str, Any], to_ref: dict[str, Any], label: str, *, props: dict[str, Any] | None = None, weight: float = 1.0, valid_from: int | None = None, valid_to: int | None = None) -> dict[str, Any]: ...
    async def upsert_edge_as(self, alias: str, from_ref: dict[str, Any], to_ref: dict[str, Any], label: str, *, props: dict[str, Any] | None = None, weight: float = 1.0, valid_from: int | None = None, valid_to: int | None = None) -> dict[str, Any]: ...
    async def delete_node(self, target: dict[str, Any]) -> None: ...
    async def delete_edge(self, target: dict[str, Any]) -> None: ...
    async def invalidate_edge(self, target: dict[str, Any], valid_to: int) -> None: ...
    async def stage(self, operations: list[dict[str, Any]]) -> None: ...
    async def get_node(self, target: dict[str, Any]) -> dict[str, Any] | None: ...
    async def get_edge(self, target: dict[str, Any]) -> dict[str, Any] | None: ...
    async def get_node_by_key(self, label: str, key: str) -> dict[str, Any] | None: ...
    async def get_edge_by_triple(self, from_ref: dict[str, Any], to_ref: dict[str, Any], label: str) -> dict[str, Any] | None: ...
    async def commit(self) -> TxnCommitResult: ...
    async def rollback(self) -> None: ...
