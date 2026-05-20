import type {
  EdgeInput,
  EdgeLabelInfo,
  NeighborEntry,
  NeighborsOptions,
  NodeInput,
  NodeLabelFilter,
  NodeLabelInfo,
  OverGraph,
} from '../../index.js'
import type {
  GraphPatternRequest,
  QueryEdgeRequest,
  QueryPlanNode,
} from '../../query-types.js'

declare const db: OverGraph

const edgeInput: EdgeInput = {
  from: 1,
  to: 2,
  label: 'WORKS_AT',
  props: { since: 2026 },
}

const edgeInfo: EdgeLabelInfo = {
  label: 'WORKS_AT',
  labelId: 1,
}

const nodeInfo: NodeLabelInfo = {
  label: 'Person',
  labelId: 1,
}

const neighbor: NeighborEntry = {
  nodeId: 2,
  edgeId: 3,
  label: 'WORKS_AT',
  weight: 1,
  validFrom: 0,
  validTo: 0,
}

const neighborOptions: NeighborsOptions = {
  direction: 'outgoing',
  edgeLabelFilter: ['WORKS_AT'],
}

const edgeQuery: QueryEdgeRequest = {
  label: 'WORKS_AT',
  allowFullScan: true,
}

const nodeInput: NodeInput = {
  labels: ['Person', 'Admin'],
  key: 'alice',
  props: { active: true },
}

const nodeLabelFilter: NodeLabelFilter = {
  labels: ['Person'],
  mode: 'all',
}

const graphPattern: GraphPatternRequest = {
  nodes: [
    { alias: 'person', labelFilter: { labels: ['Person'], mode: 'all' } },
    { alias: 'company', labelFilter: { labels: ['Company'], mode: 'all' } },
  ],
  edges: [
    {
      fromAlias: 'person',
      toAlias: 'company',
      labelFilter: ['WORKS_AT'],
    },
  ],
  limit: 10,
}

const fallbackEdgeLabelScan: QueryPlanNode = {
  kind: 'fallback_edge_label_scan',
}

void db
void edgeInput
void edgeInfo
void nodeInfo
void neighbor
void neighborOptions
void edgeQuery
void nodeInput
void nodeLabelFilter
void graphPattern
void fallbackEdgeLabelScan
