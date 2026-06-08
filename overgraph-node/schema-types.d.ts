import type { Buffer } from 'node:buffer'

export interface EdgeSchema<TLiteral = SchemaLiteral> {
  additionalProperties?: SchemaAdditionalProperties
  properties?: Record<string, PropertySchema<TLiteral>>
  from?: EndpointLabelSchema
  to?: EndpointLabelSchema
  allowSelfLoops?: boolean
  weight?: NumericFieldSchema<TLiteral>
  validity?: EdgeValiditySchema
}

export interface EdgeSchemaInfo {
  label: string
  schema: EdgeSchema<CanonicalSchemaLiteral>
}

export interface EdgeValiditySchema {
  requireValidFromBeforeValidTo?: boolean
  validFromMin?: number
  validFromMax?: number
  validToMin?: number
  validToMax?: number
  allowOpenEndedValidTo?: boolean
}

export interface EndpointLabelSchema {
  allOf?: Array<string>
  anyOf?: Array<string>
  noneOf?: Array<string>
}

export interface NodeLabelConstraintSchema {
  allOf?: Array<string>
  anyOf?: Array<string>
  noneOf?: Array<string>
}

export interface NodeSchema<TLiteral = SchemaLiteral> {
  additionalProperties?: SchemaAdditionalProperties
  properties?: Record<string, PropertySchema<TLiteral>>
  key?: StringFieldSchema
  labelConstraints?: NodeLabelConstraintSchema
  weight?: NumericFieldSchema<TLiteral>
  denseVector?: DenseVectorSchema
  sparseVector?: SparseVectorSchema
}

export interface NodeSchemaInfo {
  label: string
  schema: NodeSchema<CanonicalSchemaLiteral>
}

export interface GraphSchemaNodeEntry<TLiteral = SchemaLiteral> {
  label: string
  schema: NodeSchema<TLiteral>
}

export interface GraphSchemaEdgeEntry<TLiteral = SchemaLiteral> {
  label: string
  schema: EdgeSchema<TLiteral>
}

export interface GraphSchema<TLiteral = SchemaLiteral> {
  nodeSchemas?: Array<GraphSchemaNodeEntry<TLiteral>>
  edgeSchemas?: Array<GraphSchemaEdgeEntry<TLiteral>>
}

export type GraphSchemaOperation =
  | {
      kind: 'setNode'
      label: string
      schema: NodeSchema
    }
  | {
      kind: 'setEdge'
      label: string
      schema: EdgeSchema
    }
  | {
      kind: 'dropNode'
      label: string
    }
  | {
      kind: 'dropEdge'
      label: string
    }

export type GraphSchemaOperationKind = 'add' | 'set' | 'drop' | 'dropAll' | 'checkAdd' | 'checkSet'

export type GraphSchemaTargetKind = 'node' | 'edge'

export type GraphSchemaDropAction = 'dropped' | 'notFound'

export type SchemaAdditionalProperties = 'allow' | 'reject'

export type SchemaValueType =
  | 'bool'
  | 'int'
  | 'uint'
  | 'float'
  | 'number'
  | 'string'
  | 'bytes'
  | 'array'
  | 'map'

export type SchemaVectorPresence = 'optional' | 'required' | 'forbidden'

export interface SchemaUIntLiteral {
  type: 'uint'
  value: number | string
}

export interface SchemaBytesLiteral {
  type: 'bytes'
  value: Array<number> | Uint8Array | Buffer
}

export interface SchemaMapLiteral {
  type: 'map'
  value: { [key: string]: SchemaLiteral }
}

export type SchemaLiteral =
  | null
  | boolean
  | number
  | string
  | SchemaUIntLiteral
  | SchemaBytesLiteral
  | SchemaMapLiteral
  | Array<SchemaLiteral>
  | { [key: string]: SchemaLiteral }

export interface CanonicalSchemaUIntLiteral {
  type: 'uint'
  value: string
}

export interface CanonicalSchemaBytesLiteral {
  type: 'bytes'
  value: Buffer
}

export interface CanonicalSchemaMapLiteral {
  type: 'map'
  value: { [key: string]: CanonicalSchemaLiteral }
}

export type CanonicalSchemaLiteral =
  | null
  | boolean
  | number
  | string
  | CanonicalSchemaUIntLiteral
  | CanonicalSchemaBytesLiteral
  | CanonicalSchemaMapLiteral
  | Array<CanonicalSchemaLiteral>
  | { [key: string]: CanonicalSchemaLiteral }

export interface SchemaNumericBound<TLiteral = SchemaLiteral> {
  value: TLiteral
  inclusive?: boolean
}

export interface PropertySchema<TLiteral = SchemaLiteral> {
  required?: boolean
  nullable?: boolean
  types?: Array<SchemaValueType>
  numericMin?: SchemaNumericBound<TLiteral>
  numericMax?: SchemaNumericBound<TLiteral>
  stringMinBytes?: number
  stringMaxBytes?: number
  bytesMinLen?: number
  bytesMaxLen?: number
  arrayMinItems?: number
  arrayMaxItems?: number
  mapMinEntries?: number
  mapMaxEntries?: number
  enumValues?: Array<TLiteral>
}

export interface StringFieldSchema {
  minBytes?: number
  maxBytes?: number
  enumValues?: Array<string>
}

export interface NumericFieldSchema<TLiteral = SchemaLiteral> {
  min?: SchemaNumericBound<TLiteral>
  max?: SchemaNumericBound<TLiteral>
  finite?: boolean
}

export interface DenseVectorSchema {
  presence?: SchemaVectorPresence
  dimension?: number
}

export interface SparseVectorSchema {
  presence?: SchemaVectorPresence
  minEntries?: number
  maxEntries?: number
  maxDimensionId?: number
}

export interface SchemaValidationReport {
  checkedRecords: number
  violationCount: number
  violations: Array<SchemaViolation>
  truncated: boolean
  scanLimitHit: boolean
}

export interface GraphSchemaValidationReportEntry {
  targetKind: GraphSchemaTargetKind
  label: string
  report: SchemaValidationReport
}

export interface GraphSchemaCheckReport {
  operation: GraphSchemaOperationKind
  entries: Array<GraphSchemaValidationReportEntry>
  checkedRecords: number
  violationCount: number
  truncated: boolean
  scanLimitHit: boolean
}

export interface GraphSchemaDropTargetResult {
  targetKind: GraphSchemaTargetKind
  label: string
  action: GraphSchemaDropAction
}

export interface GraphSchemaPublishResult {
  operation: GraphSchemaOperationKind
  nodeSchemas: Array<NodeSchemaInfo>
  edgeSchemas: Array<EdgeSchemaInfo>
  validation: GraphSchemaCheckReport
  targetsPublished: number
  targetsDropped: number
  dropTargets: Array<GraphSchemaDropTargetResult>
  nodeSchemasDropped: number
  edgeSchemasDropped: number
}

export interface SchemaViolation {
  target: SchemaViolationTarget
  path: string
  message: string
}

export type SchemaViolationTarget =
  | {
      kind: 'node'
      id: number
      labels: Array<string>
      key: string
    }
  | {
      kind: 'edge'
      id: number
      label: string
      from: number
      to: number
    }
