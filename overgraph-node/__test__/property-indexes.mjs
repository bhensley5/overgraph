import { after, before, describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { setTimeout as delay } from 'node:timers/promises';
import { OverGraph } from '../index.js';

async function waitForIndexState(db, predicate, expectedState = 'ready', timeoutMs = 5000) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const info = predicate(db.listNodePropertyIndexes());
    if (info?.state === expectedState) {
      return info;
    }
    if (Date.now() >= deadline) {
      throw new Error(`timed out waiting for secondary index state '${expectedState}'`);
    }
    await delay(20);
  }
}

async function ensureRangeIndexReady(db, propKey = 'score') {
  db.ensureNodePropertyIndex(1, propKey, { kind: 'range', domain: 'int' });
  return waitForIndexState(
    db,
    infos => infos.find(info => info.typeId === 1 && info.propKey === propKey && info.kind === 'range')
  );
}

describe('node property index APIs', () => {
  let tmpDir;
  let db;

  before(() => {
    tmpDir = mkdtempSync(join(tmpdir(), 'overgraph-prop-index-'));
    db = OverGraph.open(join(tmpDir, 'db'), { walSyncMode: 'immediate' });
    for (let i = 0; i < 6; i++) {
      db.upsertNode(1, `node-${i}`, {
        props: {
          color: i % 2 === 0 ? 'red' : 'blue',
          score: (i + 1) * 10,
          temp: (i + 1) * 5,
        },
      });
    }
  });

  after(() => {
    db.close();
    rmSync(tmpDir, { recursive: true, force: true });
  });

  it('ensures, lists, and drops declared property indexes', async () => {
    const eq = db.ensureNodePropertyIndex(1, 'color', { kind: 'equality' });
    assert.equal(eq.kind, 'equality');
    assert.equal(eq.domain, undefined);
    assert.equal(eq.state, 'building');

    const range = db.ensureNodePropertyIndex(1, 'score', { kind: 'range', domain: 'int' });
    assert.equal(range.kind, 'range');
    assert.equal(range.domain, 'int');
    assert.equal(range.state, 'building');

    await waitForIndexState(
      db,
      infos => infos.find(info => info.typeId === 1 && info.propKey === 'color' && info.kind === 'equality')
    );
    const readyRange = await waitForIndexState(
      db,
      infos => infos.find(info => info.typeId === 1 && info.propKey === 'score' && info.kind === 'range')
    );
    assert.equal(readyRange.domain, 'int');

    const listed = db.listNodePropertyIndexes();
    assert.equal(listed.length, 2);
    assert.deepEqual(
      listed.map(info => [info.propKey, info.kind, info.domain ?? null, info.state]).sort(),
      [
        ['color', 'equality', null, 'ready'],
        ['score', 'range', 'int', 'ready'],
      ]
    );

    assert.throws(
      () => db.ensureNodePropertyIndex(1, 'score', { kind: 'range', domain: 'float' }),
      /different domain/i
    );

    assert.equal(db.dropNodePropertyIndex(1, 'color', { kind: 'equality' }), true);
    assert.equal(db.dropNodePropertyIndex(1, 'color', { kind: 'equality' }), false);
  });

  it('runs range queries and paging through the public API', async () => {
    await ensureRangeIndexReady(db);

    const all = Array.from(
      db.findNodesRange(
        1,
        'score',
        { value: 20, inclusive: true, domain: 'int' },
        { value: 50, inclusive: false, domain: 'int' }
      )
    );
    assert.equal(all.length, 3);

    const first = db.findNodesRangePaged(
      1,
      'score',
      { value: 20, inclusive: true, domain: 'int' },
      { value: 50, inclusive: false, domain: 'int' },
      { limit: 2 }
    );
    assert.deepEqual(Array.from(first.items), all.slice(0, 2));
    assert.equal(first.nextCursor?.domain, 'int');
    assert.equal(typeof first.nextCursor?.value, 'number');
    assert.equal(typeof first.nextCursor?.nodeId, 'number');

    const second = db.findNodesRangePaged(
      1,
      'score',
      { value: 20, inclusive: true, domain: 'int' },
      { value: 50, inclusive: false, domain: 'int' },
      { limit: 2, after: first.nextCursor }
    );
    assert.deepEqual(Array.from(second.items), all.slice(2));
    assert.ok(second.nextCursor == null);

    const fallback = Array.from(
      db.findNodesRange(
        1,
        'temp',
        { value: 10, inclusive: true, domain: 'int' },
        { value: 25, inclusive: true, domain: 'int' }
      )
    );
    assert.equal(fallback.length, 4);
  });

  it('validates kind and domain inputs at the binding boundary', () => {
    assert.throws(
      () => db.ensureNodePropertyIndex(1, 'score', { kind: 'bogus' }),
      /Invalid index kind/i
    );
    assert.throws(
      () => db.ensureNodePropertyIndex(1, 'score', { kind: 'range' }),
      /Range indexes require domain/i
    );
    assert.throws(
      () => db.ensureNodePropertyIndex(1, 'score', { kind: 'equality', domain: 'int' }),
      /do not accept a range domain/i
    );
    assert.throws(
      () => db.findNodesRange(1, 'score', { value: 10, inclusive: true, domain: 'bogus' }),
      /Invalid range domain/i
    );
    assert.throws(
      () =>
        db.findNodesRange(
          1,
          'score',
          { value: 10, inclusive: true, domain: 'int' },
          { value: 20, inclusive: true, domain: 'float' }
        ),
      /same PropValue variant/i
    );
    assert.throws(
      () =>
        db.findNodesRangePaged(
          1,
          'score',
          { value: 10, inclusive: true, domain: 'int' },
          { value: 20, inclusive: true, domain: 'int' },
          {
            limit: 2,
            after: { value: 15, nodeId: 1, domain: 'float' },
          }
        ),
      /cursor must use the same PropValue variant/i
    );
  });

  it('supports async property index and range APIs', async () => {
    const asyncEq = await db.ensureNodePropertyIndexAsync(1, 'temp', { kind: 'equality' });
    assert.equal(asyncEq.kind, 'equality');
    await waitForIndexState(
      db,
      infos => infos.find(info => info.typeId === 1 && info.propKey === 'temp' && info.kind === 'equality')
    );
    await ensureRangeIndexReady(db);

    const listed = await db.listNodePropertyIndexesAsync();
    assert.ok(listed.some(info => info.propKey === 'temp' && info.kind === 'equality'));

    const ids = await db.findNodesRangeAsync(
      1,
      'score',
      { value: 20, inclusive: true, domain: 'int' },
      { value: 30, inclusive: true, domain: 'int' }
    );
    assert.equal(ids.length, 2);

    const page = await db.findNodesRangePagedAsync(
      1,
      'score',
      { value: 20, inclusive: true, domain: 'int' },
      { value: 40, inclusive: true, domain: 'int' },
      { limit: 2 }
    );
    assert.equal(page.items.length, 2);
    assert.equal(page.nextCursor?.domain, 'int');

    assert.equal(await db.dropNodePropertyIndexAsync(1, 'temp', { kind: 'equality' }), true);
  });
});
