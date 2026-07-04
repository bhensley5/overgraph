import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import {
  cpSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { tmpdir } from 'node:os';
import { OverGraph, scrubPath, scrubPathAsync } from '../index.js';

const __dirname = dirname(fileURLToPath(import.meta.url));

function createClosedDb(dbPath) {
  const db = OverGraph.open(dbPath, { compactAfterNFlushes: 0 });
  for (let i = 0; i < 8; i++) {
    db.upsertNode('Person', `node_${i}`);
  }
  db.flush();
  db.close();
}

function corruptSegmentCore(dbPath) {
  const corePath = join(dbPath, 'segments', 'seg_0001', 'segment.core');
  const data = Buffer.from(readFileSync(corePath));
  data[Math.floor(data.length / 2)] ^= 0xff;
  writeFileSync(corePath, data);
}

function writeEmptyWal(path) {
  const data = Buffer.alloc(8);
  data.write('OVGR', 0, 'ascii');
  data.writeUInt32LE(3, 4);
  writeFileSync(path, data);
}

function findingTypes(report) {
  return report.findings.map(finding => finding.findingType);
}

describe('scrubPath', () => {
  let tmpDir;

  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-scrub-path-'));
  });

  after(() => {
    rmSync(tmpDir, { recursive: true, force: true });
  });

  it('reports a closed healthy database through the sync API', () => {
    const dbPath = join(tmpDir, 'healthy-sync');
    createClosedDb(dbPath);

    const report = scrubPath(dbPath);

    assert.equal(report.manifest.source, 'Current');
    assert.equal(report.manifest.segmentCount, 1);
    assert.equal(report.totalComponentsFailed, 0);
    assert(report.totalComponentsChecked > 0);
    assert(report.totalWalBytesChecked > 0);
    assert.equal(report.segments.length, 1);
    assert.equal(report.segments[0].findings.length, 0);
    assert(report.walGenerations.some(wal => wal.role === 'Active'));
    assert.equal(report.orphanSegments.length, 0);
    assert(!report.findings.some(finding => finding.severity === 'error'));
  });

  it('reports a closed healthy database through the async API', async () => {
    const dbPath = join(tmpDir, 'healthy-async');
    createClosedDb(dbPath);

    const report = await scrubPathAsync(dbPath);

    assert.equal(report.manifest.source, 'Current');
    assert.equal(report.totalComponentsFailed, 0);
    assert.equal(report.segments.length, 1);
    assert(report.walGenerations.some(wal => wal.role === 'Active'));
  });

  it('detects segment.core corruption without reopening the engine', () => {
    const dbPath = join(tmpDir, 'corrupt-core');
    createClosedDb(dbPath);
    corruptSegmentCore(dbPath);

    const report = scrubPath(dbPath);

    assert(report.totalComponentsFailed > 0);
    assert(report.segments.some(segment =>
      segment.findings.some(finding =>
        finding.findingType === 'PayloadDigestMismatch'
        || finding.findingType === 'IdentityHeaderMismatch'
      )
    ));
  });

  it('reports missing manifest and current-layout artifacts without creating a manifest', () => {
    const dbPath = join(tmpDir, 'missing-manifest-artifacts');
    mkdirSync(join(dbPath, 'segments', 'seg_0001'), { recursive: true });
    writeEmptyWal(join(dbPath, 'wal_0.wal'));

    const report = scrubPath(dbPath);

    assert.equal(report.manifest, null);
    assert(findingTypes(report).includes('ManifestMissing'));
    assert(findingTypes(report).includes('OrphanSegment'));
    assert(findingTypes(report).includes('OrphanWal'));
    assert(!existsSync(join(dbPath, 'manifest.current')));
  });

  it('reports orphan segment and WAL artifacts without deleting them', () => {
    const dbPath = join(tmpDir, 'orphan-artifacts');
    createClosedDb(dbPath);
    const orphanSegment = join(dbPath, 'segments', 'seg_0099');
    const orphanWal = join(dbPath, 'wal_99.wal');
    cpSync(join(dbPath, 'segments', 'seg_0001'), orphanSegment, { recursive: true });
    writeEmptyWal(orphanWal);

    const report = scrubPath(dbPath);

    assert(findingTypes(report).includes('OrphanSegment'));
    assert(findingTypes(report).includes('OrphanWal'));
    assert(existsSync(orphanSegment));
    assert(existsSync(orphanWal));
    assert.equal(report.orphanSegments.length, 0);
  });

  it('deep-scrubs orphan segments when includeOrphanSegments is true', () => {
    const dbPath = join(tmpDir, 'orphan-deep-scrub');
    createClosedDb(dbPath);
    const orphanSegment = join(dbPath, 'segments', 'seg_0099');
    cpSync(join(dbPath, 'segments', 'seg_0001'), orphanSegment, { recursive: true });

    const report = scrubPath(dbPath, { includeOrphanSegments: true });

    assert.equal(report.orphanSegments.length, 1);
    assert.equal(report.orphanSegments[0].semanticChecksSkipped, true);
    assert(report.orphanSegments[0].componentsOk > 0);
    assert(findingTypes(report).includes('OrphanSegmentUnpinned'));
    assert(existsSync(orphanSegment));
  });

  it('applies all path scrub options', () => {
    const dbPath = join(tmpDir, 'options');
    createClosedDb(dbPath);

    const report = scrubPath(dbPath, {
      validateWal: false,
      includeOrphanSegments: false,
      checkManifestStability: false,
    });

    assert.equal(report.walGenerations.length, 0);
    assert.equal(report.orphanSegments.length, 0);
    assert.equal(report.totalWalBytesChecked, 0);
  });

  it('uses the standalone path scrub task and core path API', () => {
    const source = readFileSync(join(__dirname, '..', 'src', 'lib.rs'), 'utf8');

    assert.match(source, /pub fn scrub_path\(/);
    assert.match(source, /pub fn scrub_path_async\(/);
    assert.match(source, /pub struct ScrubPathOp/);
    assert.match(source, /overgraph::scrub_path_with_options/);

    const syncStart = source.indexOf('pub fn scrub_path(');
    const asyncStart = source.indexOf('pub fn scrub_path_async(');
    const wrapperStart = source.indexOf('#[napi]\npub struct OverGraph');
    const pathApiSource = source.slice(syncStart, wrapperStart);
    assert(!pathApiSource.includes('DatabaseEngine::open'));
    assert(!pathApiSource.includes('EngineReadOp'));
    assert(syncStart >= 0 && asyncStart > syncStart && wrapperStart > asyncStart);
  });
});
