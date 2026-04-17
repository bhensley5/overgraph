/**
 * P6-009: Full lifecycle integration test.
 *
 * open → batch insert 500 nodes + 1000 edges → neighbors → traverse
 * → find_nodes → delete → compact → close → reopen → verify
 */
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { OverGraph } from '../index.js';

describe('Full lifecycle integration', () => {
  let tmpDir, dbPath;

  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-lifecycle-'));
    dbPath = join(tmpDir, 'lifecycle-db');
  });
  after(() => { rmSync(tmpDir, { recursive: true, force: true }); });

  // Shared state across sequential test steps
  let nodeIds;       // Float64Array of 500 node ids
  let edgeIds;       // Float64Array of 1000 edge ids
  let deletedNodeId; // one node we'll delete
  let deletedEdgeId; // one edge we'll delete

  it('Step 1: open database', () => {
    const db = OverGraph.open(dbPath, {
      createIfMissing: true,
      edgeUniqueness: true,
      memtableFlushThreshold: 64 * 1024, // low threshold to trigger auto-flush
    });
    db.close();
  });

  it('Step 2: batch insert 500 nodes across 5 types', async () => {
    const db = OverGraph.open(dbPath);

    const nodes = [];
    for (let i = 0; i < 500; i++) {
      const typeId = (i % 5) + 1; // types 1-5
      nodes.push({
        typeId,
        key: `node-${i}`,
        props: { idx: i, label: `Node ${i}`, active: i % 3 !== 0 },
        weight: (i % 10) / 10.0,
      });
    }

    nodeIds = await db.batchUpsertNodesAsync(nodes);
    assert.equal(nodeIds.length, 500);

    // Verify a sample
    const sample = await db.getNodeAsync(nodeIds[42]);
    assert.ok(sample);
    assert.equal(sample.key, 'node-42');
    assert.equal(sample.props.idx, 42);

    db.close();
  });

  it('Step 3: batch insert 1000 edges (each node gets ~2 outgoing)', async () => {
    const db = OverGraph.open(dbPath);

    const edges = [];
    for (let i = 0; i < 1000; i++) {
      const fromIdx = i % 500;
      const toIdx = (i * 7 + 13) % 500; // pseudo-random but deterministic
      if (fromIdx === toIdx) continue;   // skip self-loops
      edges.push({
        from: nodeIds[fromIdx],
        to: nodeIds[toIdx],
        typeId: (i % 3) + 10, // edge types 10, 11, 12
        props: { order: i },
        weight: 1.0 + (i % 5) * 0.1,
      });
    }

    edgeIds = await db.batchUpsertEdgesAsync(edges);
    assert.ok(edgeIds.length > 0);

    // Verify a sample edge
    const sampleEdge = await db.getEdgeAsync(edgeIds[0]);
    assert.ok(sampleEdge);
    assert.equal(sampleEdge.props.order, 0);

    db.close();
  });

  it('Step 4: neighbor queries work on the graph', async () => {
    const db = OverGraph.open(dbPath);

    // Node 0 should have outgoing neighbors
    const nbrs = await db.neighborsAsync(nodeIds[0], { direction: 'outgoing' });
    assert.ok(nbrs.length > 0, 'node 0 should have outgoing neighbors');

    // Neighbors with type filter
    const filtered = await db.neighborsAsync(nodeIds[0], { direction: 'outgoing', typeFilter: [10] });
    assert.ok(filtered.length <= nbrs.length);

    // Neighbors with limit
    if (nbrs.length > 1) {
      const limited = await db.neighborsAsync(nodeIds[0], { direction: 'outgoing', limit: 1 });
      assert.equal(limited.length, 1);
    }

    // Incoming neighbors
    const incoming = await db.neighborsAsync(nodeIds[13], { direction: 'incoming' });
    // node 13 is a target for edges where (i*7+13)%500 == 13, i.e., i=0
    assert.ok(incoming.length > 0, 'node 13 should have incoming neighbors');

    db.close();
  });

  it('Step 5: traverse reaches exact 2nd-hop nodes', async () => {
    const db = OverGraph.open(dbPath);

    const hop2 = await db.traverseAsync(nodeIds[0], 2, { minDepth: 2, direction: 'outgoing' });
    const hop1 = await db.neighborsAsync(nodeIds[0], { direction: 'outgoing' });
    const hop1Set = new Set(hop1.map(e => e.nodeId));
    assert.ok(hop2.items.length > 0, 'depth-2 traversal should emit at least one hit');
    for (const hit of hop2.items) {
      assert.equal(hit.depth, 2);
      assert.ok(!hop1Set.has(hit.nodeId), 'depth-2 traversal should not include 1-hop nodes');
    }

    db.close();
  });

  it('Step 6: find_nodes by property', async () => {
    const db = OverGraph.open(dbPath);

    // Find all type-1 nodes where active=true
    // Type 1 nodes: indices 0, 5, 10, ... (every 5th). Active: idx % 3 !== 0
    const activeType1 = await db.findNodesAsync(1, 'active', true);
    assert.ok(activeType1.length > 0);

    // Verify they're actually active type-1 nodes
    for (const id of activeType1) {
      const n = await db.getNodeAsync(id);
      assert.ok(n);
      assert.equal(n.typeId, 1);
      assert.equal(n.props.active, true);
    }

    // Find by string property
    const specific = await db.findNodesAsync(1, 'label', 'Node 0');
    assert.equal(specific.length, 1);

    db.close();
  });

  it('Step 7: delete a node and an edge', async () => {
    const db = OverGraph.open(dbPath);

    // Delete node at index 499 (last one)
    deletedNodeId = nodeIds[499];
    assert.ok(await db.getNodeAsync(deletedNodeId));
    await db.deleteNodeAsync(deletedNodeId);
    assert.equal(await db.getNodeAsync(deletedNodeId), null);

    // Delete first edge
    deletedEdgeId = edgeIds[0];
    assert.ok(await db.getEdgeAsync(deletedEdgeId));
    await db.deleteEdgeAsync(deletedEdgeId);
    assert.equal(await db.getEdgeAsync(deletedEdgeId), null);

    db.close();
  });

  it('Step 8: flush and compact', async () => {
    const db = OverGraph.open(dbPath);

    // Force flush to create segments
    await db.flushAsync();

    // Insert a few more nodes to create a second segment
    for (let i = 0; i < 20; i++) {
      db.upsertNode(1, `extra-${i}`);
    }
    await db.flushAsync();

    // Compact
    const stats = await db.compactAsync();
    assert.ok(stats, 'compact should return stats after multiple flushes');
    assert.ok(stats.segmentsMerged >= 2);
    assert.ok(stats.nodesKept > 0);

    db.close();
  });

  it('Step 9: close and reopen, all data survives', async () => {
    const db = OverGraph.open(dbPath);

    // Verify surviving nodes
    const node0 = await db.getNodeAsync(nodeIds[0]);
    assert.ok(node0, 'node 0 should survive');
    assert.equal(node0.key, 'node-0');
    assert.equal(node0.props.idx, 0);

    const node42 = await db.getNodeAsync(nodeIds[42]);
    assert.ok(node42, 'node 42 should survive');
    assert.equal(node42.key, 'node-42');

    // Verify deleted node stays deleted
    assert.equal(await db.getNodeAsync(deletedNodeId), null, 'deleted node should stay deleted');

    // Verify deleted edge stays deleted
    assert.equal(await db.getEdgeAsync(deletedEdgeId), null, 'deleted edge should stay deleted');

    // Verify neighbors still work after compaction + reopen
    const nbrs = await db.neighborsAsync(nodeIds[0], { direction: 'outgoing' });
    assert.ok(nbrs.length > 0, 'neighbors should work after reopen');

    // Verify find_nodes still works after compaction + reopen
    const found = await db.findNodesAsync(1, 'label', 'Node 0');
    assert.equal(found.length, 1);

    // Verify edge data survived
    if (edgeIds.length > 1) {
      const survivingEdge = await db.getEdgeAsync(edgeIds[1]);
      assert.ok(survivingEdge, 'surviving edge should be readable');
    }

    db.close();
  });
});
