import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { OverGraph } from '../index.js';

describe('OverGraph open/close', () => {
  let tmpDir;

  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-test-'));
  });

  after(() => {
    rmSync(tmpDir, { recursive: true, force: true });
  });

  it('should open a new database with default options', () => {
    const dbPath = join(tmpDir, 'db1');
    const db = OverGraph.open(dbPath);
    assert.ok(db instanceof OverGraph);
    db.close();
  });

  it('should open with custom options', () => {
    const dbPath = join(tmpDir, 'db2');
    const db = OverGraph.open(dbPath, {
      createIfMissing: true,
      edgeUniqueness: true,
      memtableFlushThreshold: 1024 * 1024,
    });
    db.close();
  });

  it('should reopen an existing database', () => {
    const dbPath = join(tmpDir, 'db3');
    const db1 = OverGraph.open(dbPath);
    db1.close();

    const db2 = OverGraph.open(dbPath);
    db2.close();
  });

  it('should be a no-op on double close', () => {
    const dbPath = join(tmpDir, 'db4');
    const db = OverGraph.open(dbPath);
    db.close();
    // Second close should be a no-op (inner is None), not throw
    db.close();
  });

  it('should fail to open non-existent path without create', () => {
    const dbPath = join(tmpDir, 'nonexistent', 'deep', 'path');
    assert.throws(() => {
      OverGraph.open(dbPath, { createIfMissing: false });
    });
  });
});
