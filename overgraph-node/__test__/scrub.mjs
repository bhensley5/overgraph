import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { OverGraph } from '../index.js';

describe('scrub', () => {
  let tmpDir;

  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-scrub-'));
  });

  after(() => {
    rmSync(tmpDir, { recursive: true, force: true });
  });

  it('reports healthy database with no findings', () => {
    const db = OverGraph.open(join(tmpDir, 'healthy'));
    for (let i = 0; i < 5; i++) {
      db.upsertNode('Person', `node_${i}`);
    }
    db.flush();

    const report = db.scrub();
    assert.equal(report.totalComponentsFailed, 0);
    assert(report.totalComponentsOk > 0);
    assert(report.totalComponentsChecked > 0);
    assert(report.durationMs >= 0);
    assert.equal(report.segments.length, 1);
    assert.equal(report.segments[0].segmentId, 1);
    assert(report.segments[0].componentsOk > 0);
    assert.equal(report.segments[0].findings.length, 0);
    db.close();
  });

  it('detects packed range corruption', () => {
    const dbPath = join(tmpDir, 'corrupt');
    let db = OverGraph.open(dbPath);
    for (let i = 0; i < 5; i++) {
      db.upsertNode('Person', `node_${i}`);
    }
    db.flush();
    db.close();

    const corePath = join(dbPath, 'segments', 'seg_0001', 'segment.core');
    const data = Buffer.from(readFileSync(corePath));
    data[Math.floor(data.length / 2)] ^= 0xFF;
    writeFileSync(corePath, data);

    db = OverGraph.open(dbPath);
    const report = db.scrub();
    assert(report.totalComponentsFailed > 0);

    const findings = report.segments[0].findings;
    assert(findings.length > 0);
    assert(findings.some(f => f.findingType === 'PayloadDigestMismatch'));
    assert(findings[0].componentKind.length > 0);
    assert(findings[0].detail.length > 0);
    db.close();
  });
});
