// Public aliases and strict contracts must survive ordinary declaration generation.
import type {
  SecondaryIndexKind, NodeMetadataIndexField, EdgeMetadataIndexField,
  SecondaryIndexPropertyField, SecondaryIndexMetadataField, SecondaryIndexField,
  SecondaryIndexSpec, NodePropertyIndexInfo, EdgePropertyIndexInfo,
  DatabaseScrubFindingType, DatabaseScrubSeverity, ManifestScrubSource, WalScrubRole,
  DatabaseScrubFinding, DatabaseScrubReport, ManifestScrubSummary,
  OrphanSegmentScrubResult, WalScrubResult,
} from '../../index.js'

type Assert<T extends true> = T
type IsAny<T> = 0 extends (1 & T) ? true : false
type Equal<A, B> = (<T>() => T extends A ? 1 : 2) extends (<T>() => T extends B ? 1 : 2) ? true : false
type NotAny<T> = IsAny<T> extends true ? false : true
type Contracts = [
  Assert<Equal<SecondaryIndexSpec['fields'], SecondaryIndexField[]>>,
  Assert<Equal<SecondaryIndexSpec['kind'], SecondaryIndexKind>>,
  Assert<Equal<NodePropertyIndexInfo['kind'], SecondaryIndexKind>>,
  Assert<Equal<EdgePropertyIndexInfo['kind'], SecondaryIndexKind>>,
  Assert<Equal<NodePropertyIndexInfo['state'], 'building' | 'ready' | 'failed'>>,
  Assert<Equal<EdgePropertyIndexInfo['state'], 'building' | 'ready' | 'failed'>>,
  Assert<NotAny<NodePropertyIndexInfo['lastError']>>,
  Assert<Equal<NodePropertyIndexInfo['lastError'], string | null>>,
  Assert<Equal<EdgePropertyIndexInfo['lastError'], string | null>>,
  Assert<Equal<DatabaseScrubFinding['findingType'], DatabaseScrubFindingType>>,
  Assert<Equal<DatabaseScrubFinding['severity'], DatabaseScrubSeverity>>,
  Assert<NotAny<DatabaseScrubReport['manifest']>>,
  Assert<Equal<DatabaseScrubReport['manifest'], ManifestScrubSummary | null>>,
  Assert<Equal<ManifestScrubSummary['source'], ManifestScrubSource>>,
  Assert<NotAny<OrphanSegmentScrubResult['segmentId']>>,
  Assert<Equal<OrphanSegmentScrubResult['segmentId'], number | null>>,
  Assert<Equal<WalScrubResult['role'], WalScrubRole>>,
  Assert<NotAny<WalScrubResult['epochId']>>,
  Assert<Equal<WalScrubResult['epochId'], number | null>>,
  Assert<Equal<WalScrubResult['segmentId'], number | null>>,
]
declare const contracts: Contracts
void contracts

const kind: SecondaryIndexKind = 'range'
const nodeField: NodeMetadataIndexField = 'updated_at'
const edgeField: EdgeMetadataIndexField = 'valid_to'
const property: SecondaryIndexPropertyField = { source: 'property', key: 'tenant' }
const metadata: SecondaryIndexMetadataField = { source: 'metadata', field: nodeField }
const specification: SecondaryIndexSpec = { fields: [property, metadata], kind }
void edgeField; void specification
function narrow(field: SecondaryIndexField) {
  if (field.source === 'property') {
    const key: string = field.key
    void key
    // @ts-expect-error property fields have no metadata selector
    field.field
  } else {
    const selected: NodeMetadataIndexField | EdgeMetadataIndexField = field.field
    void selected
    // @ts-expect-error metadata fields have no property key
    field.key
  }
}
void narrow
// @ts-expect-error fields are required
const noFields: SecondaryIndexSpec = { kind: 'equality' }
// @ts-expect-error kind is required
const noKind: SecondaryIndexSpec = { fields: [property] }
// @ts-expect-error unknown kind
const invalidKind: SecondaryIndexSpec = { fields: [property], kind: 'hash' }
// @ts-expect-error property key is required
const missingKey: SecondaryIndexField = { source: 'property' }
// @ts-expect-error unknown metadata selector
const unknownMetadata: SecondaryIndexField = { source: 'metadata', field: 'arbitrary' }
// @ts-expect-error discriminant and selector must agree
const mixed: SecondaryIndexField = { source: 'property', key: 'x', field: 'id' }
// @ts-expect-error scrub severities remain a literal union
const severity: DatabaseScrubSeverity = 'fatal'
// @ts-expect-error scrub findings remain a literal union
const finding: DatabaseScrubFindingType = 'UnknownFinding'
// @ts-expect-error manifest sources remain a literal union
const source: ManifestScrubSource = 'Backup'
// @ts-expect-error WAL roles remain a literal union
const role: WalScrubRole = 'Historical'
void noFields; void noKind; void invalidKind; void missingKey; void unknownMetadata
void mixed; void severity; void finding; void source; void role
