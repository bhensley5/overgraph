/**
 * OverGraph Example: Knowledge Graph
 *
 * This example shows how to use OverGraph to build and query a
 * knowledge graph of entities, facts, and relationships.
 *
 * Run: node examples/node/knowledge-graph.mjs
 * (Requires: npm run build in overgraph-node/ first)
 */

import { OverGraph } from '../../overgraph-node/index.js';

// --- Type IDs (you define these for your application) ---
const ENTITY = 1;
const FACT = 2;
const CONVERSATION = 3;

const RELATED_TO = 10;
const MENTIONED_IN = 11;
const SUPPORTS = 12;

async function main() {
  // Open (or create) a database in a local directory
  const db = OverGraph.open('./example-graph');

  try {
    // --- Build a knowledge graph ---

    // Create some entities
    const [alice, bob, project] = await db.batchUpsertNodesAsync([
      { typeId: ENTITY, key: 'person:alice', props: { name: 'Alice', role: 'engineer' }, weight: 1.0 },
      { typeId: ENTITY, key: 'person:bob', props: { name: 'Bob', role: 'designer' }, weight: 0.9 },
      { typeId: ENTITY, key: 'project:atlas', props: { name: 'Atlas', status: 'active' }, weight: 0.95 },
    ]);

    // Create some facts
    const [fact1, fact2] = await db.batchUpsertNodesAsync([
      { typeId: FACT, key: 'fact:alice-leads-atlas', props: { text: 'Alice leads the Atlas project' }, weight: 0.9 },
      { typeId: FACT, key: 'fact:bob-designs-atlas', props: { text: 'Bob is the lead designer on Atlas' }, weight: 0.85 },
    ]);

    // Create a conversation node
    const convo = await db.upsertNodeAsync(CONVERSATION, 'convo:2024-01-15', {
      props: { summary: 'Discussed Atlas project timeline' },
      weight: 0.7,
    });

    // Connect everything with typed edges
    await db.batchUpsertEdgesAsync([
      { from: alice, to: project, typeId: RELATED_TO, props: { role: 'lead' }, weight: 1.0 },
      { from: bob, to: project, typeId: RELATED_TO, props: { role: 'designer' }, weight: 0.9 },
      { from: alice, to: bob, typeId: RELATED_TO, props: { context: 'teammates' }, weight: 0.8 },
      { from: fact1, to: convo, typeId: MENTIONED_IN, weight: 0.9 },
      { from: fact2, to: convo, typeId: MENTIONED_IN, weight: 0.85 },
      { from: fact1, to: alice, typeId: SUPPORTS, weight: 0.9 },
      { from: fact1, to: project, typeId: SUPPORTS, weight: 0.9 },
    ]);

    console.log('Knowledge graph built!\n');

    // --- Query the graph ---

    // 1. Who is Alice connected to?
    const aliceNeighbors = await db.neighborsAsync(alice, {
      direction: 'outgoing',
      typeFilter: [RELATED_TO],
      limit: 10,
    });
    console.log(`Alice's connections (${aliceNeighbors.length}):`);
    for (let i = 0; i < aliceNeighbors.length; i++) {
      const node = await db.getNodeAsync(aliceNeighbors.nodeId(i));
      console.log(`  -> ${node.props.name} (weight: ${aliceNeighbors.weight(i)})`);
    }

    // 2. Find all entities
    const entities = await db.getNodesByTypeAsync(ENTITY);
    console.log(`\nAll entities (${entities.length}):`);
    for (const entity of entities) {
      console.log(`  ${entity.props.name} (${entity.props.role || entity.props.status})`);
    }

    // 3. Top-K: most important connections from the Atlas project
    const topK = await db.topKNeighborsAsync(project, 5, {
      direction: 'incoming',
      typeFilter: [RELATED_TO],
      scoring: 'weight',
    });
    console.log(`\nTop connections to Atlas (${topK.length}):`);
    for (let i = 0; i < topK.length; i++) {
      const node = await db.getNodeAsync(topK.nodeId(i));
      console.log(`  ${node.props.name} (score: ${topK.weight(i)})`);
    }

    // 4. Personalized PageRank: what's most relevant to Alice?
    const ppr = await db.personalizedPagerankAsync([alice], {
      maxResults: 5,
      maxIterations: 50,
    });
    console.log(`\nMost relevant to Alice (PPR, ${ppr.scores.length} results):`);
    for (let i = 0; i < ppr.scores.length; i++) {
      const node = await db.getNodeAsync(ppr.nodeIds[i]);
      if (node) {
        const name = node.props.name || node.props.text || node.props.summary || node.key;
        console.log(`  ${name}: ${ppr.scores[i].toFixed(4)}`);
      }
    }

    // 5. Paginated listing
    let page = await db.nodesByTypePagedAsync(ENTITY, 2);
    console.log(`\nPaginated entities (page 1, ${page.items.length} items):`);
    for (const id of page.items) {
      const node = await db.getNodeAsync(id);
      console.log(`  ${node.props.name}`);
    }
    if (page.nextCursor !== null) {
      const page2 = await db.nodesByTypePagedAsync(ENTITY, 2, page.nextCursor);
      console.log(`Page 2 (${page2.items.length} items):`);
      for (const id of page2.items) {
        const node = await db.getNodeAsync(id);
        console.log(`  ${node.props.name}`);
      }
    }

    // 6. Database stats
    const stats = await db.statsAsync();
    console.log(`\nDatabase stats:`);
    console.log(`  Segments: ${stats.segmentCount}`);
    console.log(`  WAL sync mode: ${stats.walSyncMode}`);

  } finally {
    await db.closeAsync();
    console.log('\nDatabase closed.');
  }
}

main().catch(console.error);
