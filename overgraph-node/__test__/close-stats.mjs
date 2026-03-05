import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { OverGraph } from '../index.js';

// ============================================================
// P11-005: close({ force: true }) / close_fast
// ============================================================

describe('close({ force: true })', () => {
  let tmpDir;

  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-close-fast-'));
  });

  after(() => {
    rmSync(tmpDir, { recursive: true, force: true });
  });

  it('should close normally without options (backwards compat)', () => {
    const db = OverGraph.open(join(tmpDir, 'normal'));
    db.upsertNode(1, 'n1');
    db.close();
  });

  it('should close with force: false (same as default)', () => {
    const db = OverGraph.open(join(tmpDir, 'force-false'));
    db.upsertNode(1, 'n1');
    db.close({ force: false });
  });

  it('should close with force: true', () => {
    const db = OverGraph.open(join(tmpDir, 'force-true'));
    db.upsertNode(1, 'n1');
    db.close({ force: true });

    // Reopen to verify data is intact
    const db2 = OverGraph.open(join(tmpDir, 'force-true'));
    const node = db2.getNodeByKey(1, 'n1');
    assert.ok(node, 'node should survive close_fast');
    db2.close();
  });

  it('should close_fast after flush', () => {
    const db = OverGraph.open(join(tmpDir, 'force-flushed'));
    for (let i = 0; i < 10; i++) {
      db.upsertNode(1, `n${i}`);
    }
    db.flush();
    db.close({ force: true });

    const db2 = OverGraph.open(join(tmpDir, 'force-flushed'));
    const node = db2.getNodeByKey(1, 'n5');
    assert.ok(node, 'flushed nodes should survive close_fast');
    db2.close();
  });

  it('should work with closeAsync({ force: true })', async () => {
    const db = OverGraph.open(join(tmpDir, 'async-force'));
    db.upsertNode(1, 'async_node');
    await db.closeAsync({ force: true });

    const db2 = OverGraph.open(join(tmpDir, 'async-force'));
    const node = db2.getNodeByKey(1, 'async_node');
    assert.ok(node, 'node should survive async close_fast');
    db2.close();
  });

  it('should work with closeAsync() no options', async () => {
    const db = OverGraph.open(join(tmpDir, 'async-normal'));
    db.upsertNode(1, 'n1');
    await db.closeAsync();
  });
});

// ============================================================
// P11-006: stats()
// ============================================================

describe('stats()', () => {
  let tmpDir;

  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-stats-'));
  });

  after(() => {
    rmSync(tmpDir, { recursive: true, force: true });
  });

  it('should return correct shape on fresh DB', () => {
    const db = OverGraph.open(join(tmpDir, 'fresh'));
    const s = db.stats();

    assert.equal(typeof s.pendingWalBytes, 'number');
    assert.equal(typeof s.segmentCount, 'number');
    assert.equal(typeof s.nodeTombstoneCount, 'number');
    assert.equal(typeof s.edgeTombstoneCount, 'number');
    assert.equal(typeof s.walSyncMode, 'string');
    // lastCompactionMs is null/undefined on fresh DB
    assert.ok(s.lastCompactionMs === null || s.lastCompactionMs === undefined);

    assert.equal(s.segmentCount, 0);
    assert.equal(s.nodeTombstoneCount, 0);
    assert.equal(s.edgeTombstoneCount, 0);

    db.close();
  });

  it('should report sync mode', () => {
    // Default is group-commit
    const db1 = OverGraph.open(join(tmpDir, 'mode-gc'));
    assert.equal(db1.stats().walSyncMode, 'group-commit');
    db1.close();

    const db2 = OverGraph.open(join(tmpDir, 'mode-imm'), { walSyncMode: 'immediate' });
    assert.equal(db2.stats().walSyncMode, 'immediate');
    db2.close();
  });

  it('should count segments after flush', () => {
    const db = OverGraph.open(join(tmpDir, 'segments'), { walSyncMode: 'immediate' });

    assert.equal(db.stats().segmentCount, 0);

    db.upsertNode(1, 'a');
    db.flush();
    assert.equal(db.stats().segmentCount, 1);

    db.upsertNode(1, 'b');
    db.flush();
    assert.equal(db.stats().segmentCount, 2);

    db.close();
  });

  it('should count tombstones after deletes', () => {
    const db = OverGraph.open(join(tmpDir, 'tombstones'), { walSyncMode: 'immediate' });
    const n1 = db.upsertNode(1, 'a');
    const n2 = db.upsertNode(1, 'b');
    db.upsertEdge(n1, n2, 1);

    assert.equal(db.stats().nodeTombstoneCount, 0);
    assert.equal(db.stats().edgeTombstoneCount, 0);

    // delete_node cascades to incident edges
    db.deleteNode(n1);
    assert.equal(db.stats().nodeTombstoneCount, 1);
    assert.equal(db.stats().edgeTombstoneCount, 1); // cascade

    db.close();
  });

  it('should report lastCompactionMs after compact', () => {
    const db = OverGraph.open(join(tmpDir, 'compaction'), {
      walSyncMode: 'immediate',
      compactAfterNFlushes: 0,
    });

    assert.ok(
      db.stats().lastCompactionMs === null || db.stats().lastCompactionMs === undefined,
    );

    db.upsertNode(1, 'a');
    db.flush();
    db.upsertNode(1, 'b');
    db.flush();

    const before = Date.now();
    db.compact();
    const after = Date.now();

    const ts = db.stats().lastCompactionMs;
    assert.ok(typeof ts === 'number', 'lastCompactionMs should be a number after compaction');
    assert.ok(ts >= before && ts <= after + 1, `timestamp ${ts} should be between ${before} and ${after}`);
    assert.equal(db.stats().segmentCount, 1);

    db.close();
  });

  it('should work with statsAsync()', async () => {
    const db = OverGraph.open(join(tmpDir, 'async-stats'));
    db.upsertNode(1, 'x');

    const s = await db.statsAsync();
    assert.equal(typeof s.segmentCount, 'number');
    assert.equal(typeof s.walSyncMode, 'string');

    db.close();
  });
});
