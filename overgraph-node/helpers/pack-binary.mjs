/**
 * Binary batch packing helpers for OverGraph.
 *
 * These encode JS objects into the binary buffer format accepted by
 * batchUpsertNodesBinary() and batchUpsertEdgesBinary().
 */

const NODE_BATCH_MAGIC = Buffer.from('OGNB');
const EDGE_BATCH_MAGIC = Buffer.from('OGEB');
const NODE_BINARY_BATCH_VERSION = 2;
const EDGE_BINARY_BATCH_VERSION = 1;
const BINARY_BATCH_HEADER_BYTES = 10;
const MAX_NODE_LABELS_PER_NODE = 10;

/**
 * Pack an array of node objects into a binary Buffer.
 *
 * Format (little-endian):
 *   [magic: "OGNB"][version: u16 = 2][count: u32]
 *   per node:
 *     [label_count: u8] repeated [label_len: u16][label: utf8]
 *     [weight: f32][key_len: u16][key: utf8]
 *     [props_len: u32][props: json utf8]
 *
 * @param {Array<{labels: string | string[], key: string, props?: object, weight?: number}>} nodes
 * @returns {Buffer}
 */
export function packNodeBatch(nodes) {
  let size = BINARY_BATCH_HEADER_BYTES;
  const encoded = nodes.map((n) => {
    const labels = typeof n.labels === 'string' ? [n.labels] : n.labels;
    if (!Array.isArray(labels)) {
      throw new TypeError('node.labels must be a string or string array');
    }
    if (labels.length < 1 || labels.length > MAX_NODE_LABELS_PER_NODE) {
      throw new RangeError('node.labels must contain 1..10 labels');
    }
    const labelBufs = labels.map((label, index) => {
      if (typeof label !== 'string') {
        throw new TypeError(`node.labels[${index}] must be a string`);
      }
      const labelBuf = Buffer.from(label, 'utf8');
      if (labelBuf.length < 1 || labelBuf.length > 255) {
        throw new RangeError(`node.labels[${index}] must encode to 1..255 UTF-8 bytes`);
      }
      return labelBuf;
    });
    const keyBuf = Buffer.from(n.key, 'utf8');
    const propsBuf = n.props ? Buffer.from(JSON.stringify(n.props), 'utf8') : null;
    size += 1 + labelBufs.reduce((sum, labelBuf) => sum + 2 + labelBuf.length, 0) + 4 + 2 + keyBuf.length + 4 + (propsBuf ? propsBuf.length : 0);
    return { labelBufs, keyBuf, propsBuf, weight: n.weight ?? 1.0 };
  });
  const buf = Buffer.alloc(size);
  let off = 0;
  NODE_BATCH_MAGIC.copy(buf, off); off += 4;
  buf.writeUInt16LE(NODE_BINARY_BATCH_VERSION, off); off += 2;
  buf.writeUInt32LE(nodes.length, off); off += 4;
  for (const { labelBufs, weight, keyBuf, propsBuf } of encoded) {
    buf.writeUInt8(labelBufs.length, off); off += 1;
    for (const labelBuf of labelBufs) {
      buf.writeUInt16LE(labelBuf.length, off); off += 2;
      labelBuf.copy(buf, off); off += labelBuf.length;
    }
    buf.writeFloatLE(weight, off); off += 4;
    buf.writeUInt16LE(keyBuf.length, off); off += 2;
    keyBuf.copy(buf, off); off += keyBuf.length;
    if (propsBuf) {
      buf.writeUInt32LE(propsBuf.length, off); off += 4;
      propsBuf.copy(buf, off); off += propsBuf.length;
    } else {
      buf.writeUInt32LE(0, off); off += 4;
    }
  }
  return buf;
}

/**
 * Pack an array of edge objects into a binary Buffer.
 *
 * Format (little-endian):
 *   [magic: "OGEB"][version: u16 = 1][count: u32]
 *   per edge:
 *     [from: u64][to: u64][label_len: u16][label: utf8][weight: f32]
 *     [valid_from: i64][valid_to: i64]
 *     [props_len: u32][props: json utf8]
 *
 * Sentinel values: valid_from=0 and valid_to=0 mean "use engine default"
 * (created_at and i64::MAX respectively). Actual Unix epoch 0 cannot be
 * represented. This is acceptable for practical graph database timestamps.
 *
 * @param {Array<{from: number, to: number, label: string, props?: object, weight?: number, validFrom?: number, validTo?: number}>} edges
 * @returns {Buffer}
 */
export function packEdgeBatch(edges) {
  let size = BINARY_BATCH_HEADER_BYTES;
  const encoded = edges.map((e) => {
    const labelBuf = Buffer.from(e.label, 'utf8');
    const propsBuf = e.props ? Buffer.from(JSON.stringify(e.props), 'utf8') : null;
    if (labelBuf.length < 1 || labelBuf.length > 255) {
      throw new RangeError('edge.label must encode to 1..255 UTF-8 bytes');
    }
    size += 8 + 8 + 2 + labelBuf.length + 4 + 8 + 8 + 4 + (propsBuf ? propsBuf.length : 0);
    return { from: e.from, to: e.to, labelBuf, weight: e.weight, validFrom: e.validFrom, validTo: e.validTo, propsBuf };
  });
  const buf = Buffer.alloc(size);
  let off = 0;
  EDGE_BATCH_MAGIC.copy(buf, off); off += 4;
  buf.writeUInt16LE(EDGE_BINARY_BATCH_VERSION, off); off += 2;
  buf.writeUInt32LE(edges.length, off); off += 4;
  for (const e of encoded) {
    if (!Number.isSafeInteger(e.from) || e.from < 0) throw new RangeError('edge.from must be a safe non-negative integer');
    if (!Number.isSafeInteger(e.to) || e.to < 0) throw new RangeError('edge.to must be a safe non-negative integer');
    buf.writeBigUInt64LE(BigInt(e.from), off); off += 8;
    buf.writeBigUInt64LE(BigInt(e.to), off); off += 8;
    buf.writeUInt16LE(e.labelBuf.length, off); off += 2;
    e.labelBuf.copy(buf, off); off += e.labelBuf.length;
    buf.writeFloatLE(e.weight ?? 1.0, off); off += 4;
    buf.writeBigInt64LE(BigInt(e.validFrom ?? 0), off); off += 8;
    buf.writeBigInt64LE(BigInt(e.validTo ?? 0), off); off += 8;
    if (e.propsBuf) {
      buf.writeUInt32LE(e.propsBuf.length, off); off += 4;
      e.propsBuf.copy(buf, off); off += e.propsBuf.length;
    } else {
      buf.writeUInt32LE(0, off); off += 4;
    }
  }
  return buf;
}
