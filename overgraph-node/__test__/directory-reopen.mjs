import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { OverGraph } from '../index.js';
import { drainAfterClose } from './helpers/native-lifecycle.mjs';

for (const asyncClose of [false, true]) {
  for (const force of [false, true]) {
    test(`immediate reopen after ${asyncClose ? 'closeAsync' : 'close'} force=${force}`, async () => {
      const root = mkdtempSync(join(tmpdir(), 'og-reopen-'));
      const path = join(root, 'db');
      let db;
      try {
        for (let i = 0; i < 12; i++) {
          db = OverGraph.open(path);
          db.upsertNode('N', `k${i}`, { props: { value: i } });
          if (asyncClose) await db.closeAsync({ force });
          else db.close({ force });
          // No retry, timer, GC, or diagnostic drain between close and open.
          db = OverGraph.open(path, { createIfMissing: false });
          assert.equal(db.getNodeByKey('N', `k${i}`).props.value, i);
          assert.equal(db.countNodesByLabels('N'), i + 1);
          db.close();
        }
      } finally {
        db?.close({ force: true });
        if (db) drainAfterClose(db);
        rmSync(root, { recursive: true, force: true });
      }
    });
  }
}


test('directory admission timeout has a retryable structured error',
  { skip: process.env.OVERGRAPH_TEST_HOOKS !== '1' }, () => {
    const root = mkdtempSync(join(tmpdir(), 'og-reopen-timeout-'));
    const path = join(root, 'db');
    const hold = join(root, 'completed');
    const concurrency = { admissionTimeoutMs: 30 };
    const db = OverGraph.open(path, { concurrency });
    let reopened;
    try {
      db.upsertNode('N', 'persisted');
      db.bindingTestHold('completed', hold);
      db.close();
      assert.throws(() => OverGraph.open(path, { concurrency }), error => {
        assert.equal(error.code, 'resource_limit');
        assert.equal(error.resource, 'directory_admission');
        assert.equal(error.retryable, true);
        return true;
      });
      writeFileSync(hold + '.release', 'go');
      reopened = OverGraph.open(path, { concurrency });
      assert.ok(reopened.getNodeByKey('N', 'persisted'));
    } finally {
      writeFileSync(hold + '.release', 'go');
      reopened?.close();
      if (reopened) drainAfterClose(reopened);
      drainAfterClose(db);
      rmSync(root, { recursive: true, force: true });
    }
  });
