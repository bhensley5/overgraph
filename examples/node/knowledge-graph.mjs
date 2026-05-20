/**
 * OverGraph Example: Knowledge Graph
 *
 * This example shows how to use OverGraph to build and query a
 * knowledge graph of people, projects, facts, and relationships.
 *
 * Run: node examples/node/knowledge-graph.mjs
 * (Requires: npm run build in overgraph-node/ first)
 */

import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { OverGraph } from '../../overgraph-node/index.js';

// --- Edge labels for this application ---
const RELATED_TO = 'RELATED_TO';
const MENTIONED_IN = 'MENTIONED_IN';
const SUPPORTS = 'SUPPORTS';

function main() {
  const dbPath = mkdtempSync(join(tmpdir(), 'overgraph-knowledge-node-'));
  let db;

  try {
    db = OverGraph.open(dbPath);

    // --- Build a knowledge graph ---

    // Create people and a project
    const [alice, bob, project] = db.batchUpsertNodes([
      { labels: ['Person', 'Contributor'], key: 'alice', props: { name: 'Alice', role: 'engineer' }, weight: 1.0 },
      { labels: ['Person', 'Contributor'], key: 'bob', props: { name: 'Bob', role: 'designer' }, weight: 0.9 },
      { labels: ['Project'], key: 'atlas', props: { name: 'Atlas', status: 'active' }, weight: 0.95 },
    ]);

    // Create some facts
    const [fact1, fact2] = db.batchUpsertNodes([
      { labels: ['Fact'], key: 'alice-leads-atlas', props: { text: 'Alice leads the Atlas project' }, weight: 0.9 },
      { labels: ['Fact'], key: 'bob-designs-atlas', props: { text: 'Bob is the lead designer on Atlas' }, weight: 0.85 },
    ]);

    // Create a conversation node
    const convo = db.upsertNode(['Conversation'], '2024-01-15', {
      props: { summary: 'Discussed Atlas project timeline' },
      weight: 0.7,
    });

    // Connect everything with labeled edges
    db.batchUpsertEdges([
      { from: alice, to: project, label: RELATED_TO, props: { role: 'lead' }, weight: 1.0 },
      { from: bob, to: project, label: RELATED_TO, props: { role: 'designer' }, weight: 0.9 },
      { from: alice, to: bob, label: RELATED_TO, props: { context: 'teammates' }, weight: 0.8 },
      { from: fact1, to: convo, label: MENTIONED_IN, weight: 0.9 },
      { from: fact2, to: convo, label: MENTIONED_IN, weight: 0.85 },
      { from: fact1, to: alice, label: SUPPORTS, weight: 0.9 },
      { from: fact1, to: project, label: SUPPORTS, weight: 0.9 },
    ]);

    console.log('Knowledge graph built!\n');

    // --- Query the graph ---

    // 1. Who is Alice connected to?
    const aliceNeighbors = db.neighbors(alice, {
      direction: 'outgoing',
      edgeLabelFilter: [RELATED_TO],
      limit: 10,
    });
    console.log(`Alice's connections (${aliceNeighbors.length}):`);
    for (let i = 0; i < aliceNeighbors.length; i++) {
      const node = db.getNode(aliceNeighbors[i].nodeId);
      console.log(`  -> ${node.props.name} (weight: ${aliceNeighbors[i].weight})`);
    }

    // 2. Find all people
    const people = db.getNodesByLabels('Person');
    console.log(`\nAll people (${people.length}):`);
    for (const person of people) {
      console.log(`  ${person.props.name} (${person.props.role})`);
    }

    // 3. Top-K: most important connections from the Atlas project
    const topK = db.topKNeighbors(project, 5, {
      direction: 'incoming',
      edgeLabelFilter: [RELATED_TO],
      scoring: 'weight',
    });
    console.log(`\nTop connections to Atlas (${topK.length}):`);
    for (let i = 0; i < topK.length; i++) {
      const node = db.getNode(topK[i].nodeId);
      console.log(`  ${node.props.name} (score: ${topK[i].weight})`);
    }

    // 4. Personalized PageRank: what's most relevant to Alice?
    const ppr = db.personalizedPagerank([alice], {
      maxResults: 5,
      maxIterations: 50,
    });
    console.log(`\nMost relevant to Alice (PPR, ${ppr.scores.length} results):`);
    for (let i = 0; i < ppr.scores.length; i++) {
      const node = db.getNode(ppr.nodeIds[i]);
      if (node) {
        const name = node.props.name || node.props.text || node.props.summary || node.key;
        console.log(`  ${name}: ${ppr.scores[i].toFixed(4)}`);
      }
    }

    // 5. Paginated listing
    let page = db.nodesByLabelsPaged('Person', 1);
    console.log(`\nPaginated people (page 1, ${page.items.length} items):`);
    for (const id of page.items) {
      const node = db.getNode(id);
      console.log(`  ${node.props.name}`);
    }
    if (page.nextCursor !== null) {
      const page2 = db.nodesByLabelsPaged('Person', 1, page.nextCursor);
      console.log(`Page 2 (${page2.items.length} items):`);
      for (const id of page2.items) {
        const node = db.getNode(id);
        console.log(`  ${node.props.name}`);
      }
    }

    // 6. Database stats
    const stats = db.stats();
    console.log(`\nDatabase stats:`);
    console.log(`  Segments: ${stats.segmentCount}`);
    console.log(`  WAL sync mode: ${stats.walSyncMode}`);

  } finally {
    let closeError = null;
    if (db) {
      try {
        db.close();
      } catch (err) {
        closeError = err;
      }
    }
    rmSync(dbPath, { recursive: true, force: true });
    console.log('\nDatabase closed.');
    if (closeError) {
      throw closeError;
    }
  }
}

try {
  main();
} catch (err) {
  console.error(err);
  process.exitCode = 1;
}
