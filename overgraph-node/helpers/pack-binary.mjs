/**
 * Binary batch packing helpers for OverGraph.
 *
 * These encode JS objects into the binary buffer format accepted by
 * batchUpsertNodesBinary() and batchUpsertEdgesBinary().
 */

/**
 * Pack an array of node objects into a binary Buffer.
 *
 * Format (little-endian):
 *   [count: u32]
 *   per node:
 *     [type_id: u32][weight: f32][key_len: u16][key: utf8]
 *     [props_len: u32][props: json utf8]
 *
 * @param {Array<{typeId: number, key: string, props?: object, weight?: number}>} nodes
 * @returns {Buffer}
 */
export function packNodeBatch(nodes) {
  let size = 4;
  const encoded = nodes.map((n) => {
    const keyBuf = Buffer.from(n.key, 'utf8');
    const propsBuf = n.props ? Buffer.from(JSON.stringify(n.props), 'utf8') : null;
    size += 4 + 4 + 2 + keyBuf.length + 4 + (propsBuf ? propsBuf.length : 0);
    return { keyBuf, propsBuf, typeId: n.typeId, weight: n.weight ?? 1.0 };
  });
  const buf = Buffer.alloc(size);
  let off = 0;
  buf.writeUInt32LE(nodes.length, off); off += 4;
  for (const { typeId, weight, keyBuf, propsBuf } of encoded) {
    buf.writeUInt32LE(typeId, off); off += 4;
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
 *   [count: u32]
 *   per edge:
 *     [from: u64][to: u64][type_id: u32][weight: f32]
 *     [valid_from: i64][valid_to: i64]
 *     [props_len: u32][props: json utf8]
 *
 * Sentinel values: valid_from=0 and valid_to=0 mean "use engine default"
 * (created_at and i64::MAX respectively). Actual Unix epoch 0 cannot be
 * represented. This is acceptable for practical graph database timestamps.
 *
 * @param {Array<{from: number, to: number, typeId: number, props?: object, weight?: number, validFrom?: number, validTo?: number}>} edges
 * @returns {Buffer}
 */
export function packEdgeBatch(edges) {
  let size = 4;
  const encoded = edges.map((e) => {
    const propsBuf = e.props ? Buffer.from(JSON.stringify(e.props), 'utf8') : null;
    size += 8 + 8 + 4 + 4 + 8 + 8 + 4 + (propsBuf ? propsBuf.length : 0);
    return { from: e.from, to: e.to, typeId: e.typeId, weight: e.weight, validFrom: e.validFrom, validTo: e.validTo, propsBuf };
  });
  const buf = Buffer.alloc(size);
  let off = 0;
  buf.writeUInt32LE(edges.length, off); off += 4;
  for (const e of encoded) {
    if (!Number.isSafeInteger(e.from) || e.from < 0) throw new RangeError('edge.from must be a safe non-negative integer');
    if (!Number.isSafeInteger(e.to) || e.to < 0) throw new RangeError('edge.to must be a safe non-negative integer');
    buf.writeBigUInt64LE(BigInt(e.from), off); off += 8;
    buf.writeBigUInt64LE(BigInt(e.to), off); off += 8;
    buf.writeUInt32LE(e.typeId, off); off += 4;
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
