"""
OverGraph Example: Knowledge Graph

This example shows how to use OverGraph to build and query a
knowledge graph of entities, facts, and relationships.

Run: python examples/python/knowledge_graph.py
(Requires: maturin develop in overgraph-python/ first)
"""

from overgraph import OverGraph

# Type IDs (you define these for your application)
ENTITY = 1
FACT = 2
CONVERSATION = 3

RELATED_TO = 10
MENTIONED_IN = 11
SUPPORTS = 12


def main():
    # Open (or create) a database in a local directory
    with OverGraph.open("./example-graph") as db:
        # --- Build a knowledge graph ---

        # Create some entities
        alice, bob, project = db.batch_upsert_nodes([
            {"type_id": ENTITY, "key": "person:alice", "props": {"name": "Alice", "role": "engineer"}, "weight": 1.0},
            {"type_id": ENTITY, "key": "person:bob", "props": {"name": "Bob", "role": "designer"}, "weight": 0.9},
            {"type_id": ENTITY, "key": "project:atlas", "props": {"name": "Atlas", "status": "active"}, "weight": 0.95},
        ])

        # Create some facts
        fact1, fact2 = db.batch_upsert_nodes([
            {"type_id": FACT, "key": "fact:alice-leads-atlas", "props": {"text": "Alice leads the Atlas project"}, "weight": 0.9},
            {"type_id": FACT, "key": "fact:bob-designs-atlas", "props": {"text": "Bob is the lead designer on Atlas"}, "weight": 0.85},
        ])

        # Create a conversation node
        convo = db.upsert_node(CONVERSATION, "convo:2024-01-15",
                               props={"summary": "Discussed Atlas project timeline"}, weight=0.7)

        # Connect everything with typed edges
        db.batch_upsert_edges([
            {"from_id": alice, "to_id": project, "type_id": RELATED_TO, "props": {"role": "lead"}, "weight": 1.0},
            {"from_id": bob, "to_id": project, "type_id": RELATED_TO, "props": {"role": "designer"}, "weight": 0.9},
            {"from_id": alice, "to_id": bob, "type_id": RELATED_TO, "props": {"context": "teammates"}, "weight": 0.8},
            {"from_id": fact1, "to_id": convo, "type_id": MENTIONED_IN, "weight": 0.9},
            {"from_id": fact2, "to_id": convo, "type_id": MENTIONED_IN, "weight": 0.85},
            {"from_id": fact1, "to_id": alice, "type_id": SUPPORTS, "weight": 0.9},
            {"from_id": fact1, "to_id": project, "type_id": SUPPORTS, "weight": 0.9},
        ])

        print("Knowledge graph built!\n")

        # --- Query the graph ---

        # 1. Who is Alice connected to?
        neighbors = db.neighbors(alice, "outgoing", type_filter=[RELATED_TO], limit=10)
        print(f"Alice's connections ({len(neighbors)}):")
        for n in neighbors:
            node = db.get_node(n.node_id)
            print(f"  -> {node.props['name']} (weight: {n.weight})")

        # 2. Find all entities
        entities = db.get_nodes_by_type(ENTITY)
        print(f"\nAll entities ({len(entities)}):")
        for entity in entities:
            detail = entity.props.get("role") or entity.props.get("status")
            print(f"  {entity.props['name']} ({detail})")

        # 3. Top-K: most important connections to the Atlas project
        top_k = db.top_k_neighbors(project, k=5, direction="incoming",
                                    type_filter=[RELATED_TO], scoring="weight")
        print(f"\nTop connections to Atlas ({len(top_k)}):")
        for n in top_k:
            node = db.get_node(n.node_id)
            print(f"  {node.props['name']} (score: {n.weight})")

        # 4. Personalized PageRank: what's most relevant to Alice?
        ppr = db.personalized_pagerank([alice], max_results=5, max_iterations=50)
        print(f"\nMost relevant to Alice (PPR, {len(ppr.node_ids)} results):")
        for node_id, score in zip(ppr.node_ids, ppr.scores):
            node = db.get_node(node_id)
            if node:
                name = node.props.get("name") or node.props.get("text") or node.props.get("summary") or node.key
                print(f"  {name}: {score:.4f}")

        # 5. Paginated listing
        page = db.nodes_by_type_paged(ENTITY, limit=2)
        print(f"\nPaginated entities (page 1, {len(page.items)} items):")
        for node_id in page.items:
            node = db.get_node(node_id)
            print(f"  {node.props['name']}")

        if page.next_cursor is not None:
            page2 = db.nodes_by_type_paged(ENTITY, limit=2, after=page.next_cursor)
            print(f"Page 2 ({len(page2.items)} items):")
            for node_id in page2.items:
                node = db.get_node(node_id)
                print(f"  {node.props['name']}")

        # 6. Retention policies
        db.set_prune_policy("short_term", max_age_ms=86_400_000, type_id=CONVERSATION)
        policies = db.list_prune_policies()
        print(f"\nActive prune policies ({len(policies)}):")
        for p in policies:
            print(f"  {p.name}: max_age_ms={p.max_age_ms}, type_id={p.type_id}")

        # Clean up the policy for this example
        db.remove_prune_policy("short_term")

        # 7. Database stats
        stats = db.stats()
        print(f"\nDatabase stats:")
        print(f"  Segments: {stats.segment_count}")
        print(f"  WAL sync mode: {stats.wal_sync_mode}")

    print("\nDatabase closed.")


if __name__ == "__main__":
    main()
