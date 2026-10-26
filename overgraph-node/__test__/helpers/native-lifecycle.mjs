export function drainAfterClose(db) {
  if (typeof db.reclamationStats !== 'function') return;
  let stats;
  try { stats = db.reclamationStats(); }
  catch (error) { if (error.message === 'reclamationStats requires production-concurrency') return; throw error; }
  const until = Date.now() + 15000;
  while (stats.closeState !== 'drained') {
    if (Date.now() > until) throw Error('native fixture drain timed out');
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 1);
    stats = db.reclamationStats();
  }
}
