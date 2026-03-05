import pytest
from overgraph import OverGraph

from conftest import make_chain, make_star


class TestNeighbors:
    def test_outgoing(self, db):
        center, spokes = make_star(db)
        nbrs = db.neighbors(center, "outgoing")
        assert len(nbrs) == 5
        node_ids = {n.node_id for n in nbrs}
        assert node_ids == set(spokes)

    def test_incoming(self, db):
        center, spokes = make_star(db)
        nbrs = db.neighbors(spokes[0], "incoming")
        assert len(nbrs) == 1
        assert nbrs[0].node_id == center

    def test_both(self, db):
        nodes, _ = make_chain(db, 3)
        nbrs = db.neighbors(nodes[1], "both")
        assert len(nbrs) == 2

    def test_type_filter(self, db):
        n1 = db.upsert_node(1, "a")
        n2 = db.upsert_node(1, "b")
        n3 = db.upsert_node(1, "c")
        db.upsert_edge(n1, n2, 10)
        db.upsert_edge(n1, n3, 20)
        nbrs = db.neighbors(n1, "outgoing", type_filter=[10])
        assert len(nbrs) == 1
        assert nbrs[0].node_id == n2

    def test_limit(self, db):
        center, _ = make_star(db, spokes=10)
        nbrs = db.neighbors(center, "outgoing", limit=3)
        assert len(nbrs) == 3

    def test_empty(self, db):
        nid = db.upsert_node(1, "lonely")
        nbrs = db.neighbors(nid, "outgoing")
        assert nbrs == []

    def test_invalid_direction(self, db):
        nid = db.upsert_node(1, "a")
        with pytest.raises(ValueError, match="Invalid direction"):
            db.neighbors(nid, "sideways")

    def test_neighbor_entry_fields(self, db):
        n1 = db.upsert_node(1, "a")
        n2 = db.upsert_node(1, "b")
        eid = db.upsert_edge(n1, n2, 10, weight=2.5)
        nbrs = db.neighbors(n1, "outgoing")
        assert len(nbrs) == 1
        entry = nbrs[0]
        assert entry.node_id == n2
        assert entry.edge_id == eid
        assert entry.edge_type_id == 10
        assert abs(entry.weight - 2.5) < 0.01
        assert "NeighborEntry" in repr(entry)


class TestNeighbors2Hop:
    def test_2hop(self, db):
        nodes, _ = make_chain(db, 4)
        nbrs = db.neighbors_2hop(nodes[0], "outgoing")
        node_ids = {n.node_id for n in nbrs}
        # 2-hop returns the 2nd-hop frontier (deduped against 1-hop)
        assert nodes[2] in node_ids
        # Should NOT include the start node or 3-hop
        assert nodes[0] not in node_ids
        assert nodes[3] not in node_ids

    def test_2hop_reaches_beyond_1hop(self, db):
        n1 = db.upsert_node(1, "a")
        n2 = db.upsert_node(1, "b")
        n3 = db.upsert_node(1, "c")
        db.upsert_edge(n1, n2, 10)
        db.upsert_edge(n2, n3, 10)
        nbrs = db.neighbors_2hop(n1, "outgoing")
        node_ids = {n.node_id for n in nbrs}
        assert n3 in node_ids


class TestNeighbors2HopConstrained:
    def test_constrained(self, db):
        n1 = db.upsert_node(1, "a")
        n2 = db.upsert_node(2, "b")  # type 2
        n3 = db.upsert_node(3, "c")  # type 3
        db.upsert_edge(n1, n2, 10)
        db.upsert_edge(n2, n3, 10)
        # Only target type 3
        nbrs = db.neighbors_2hop_constrained(
            n1, "outgoing",
            traverse_edge_types=[10],
            target_node_types=[3],
        )
        node_ids = {n.node_id for n in nbrs}
        assert n3 in node_ids
        assert n2 not in node_ids


class TestTopKNeighbors:
    def test_top_k_by_weight(self, db):
        center = db.upsert_node(1, "center")
        for i in range(10):
            spoke = db.upsert_node(1, f"s{i}")
            db.upsert_edge(center, spoke, 10, weight=float(i))
        top3 = db.top_k_neighbors(center, 3, "outgoing", scoring="weight")
        assert len(top3) == 3
        # Highest weights first
        weights = [n.weight for n in top3]
        assert weights == sorted(weights, reverse=True)

    def test_top_k_by_recency(self, db):
        center = db.upsert_node(1, "center")
        for i in range(5):
            spoke = db.upsert_node(1, f"s{i}")
            db.upsert_edge(center, spoke, 10)
        top2 = db.top_k_neighbors(center, 2, "outgoing", scoring="recency")
        assert len(top2) == 2

    def test_invalid_scoring(self, db):
        nid = db.upsert_node(1, "a")
        with pytest.raises(ValueError, match="Invalid scoring"):
            db.top_k_neighbors(nid, 3, "outgoing", scoring="magic")

    def test_decay_requires_lambda(self, db):
        nid = db.upsert_node(1, "a")
        with pytest.raises(ValueError, match="decay_lambda"):
            db.top_k_neighbors(nid, 3, "outgoing", scoring="decay")

    def test_decay_with_lambda(self, db):
        center = db.upsert_node(1, "center")
        for i in range(5):
            spoke = db.upsert_node(1, f"s{i}")
            db.upsert_edge(center, spoke, 10, weight=float(i))
        top2 = db.top_k_neighbors(
            center, 2, "outgoing", scoring="decay", decay_lambda=0.01
        )
        assert len(top2) == 2

    def test_decay_negative_lambda_rejected(self, db):
        nid = db.upsert_node(1, "a")
        with pytest.raises(ValueError, match="decay_lambda"):
            db.top_k_neighbors(
                nid, 3, "outgoing", scoring="decay", decay_lambda=-0.5
            )

    def test_top_k_at_epoch(self, db):
        center = db.upsert_node(1, "center")
        for i in range(5):
            spoke = db.upsert_node(1, f"s{i}")
            db.upsert_edge(center, spoke, 10, weight=float(i))
        # at_epoch in the far future should still return results
        import time
        future_ms = int(time.time() * 1000) + 60_000
        top2 = db.top_k_neighbors(
            center, 2, "outgoing", scoring="weight", at_epoch=future_ms
        )
        assert len(top2) == 2


class TestNeighborsBatch:
    def test_basic(self, db):
        n1 = db.upsert_node(1, "a")
        n2 = db.upsert_node(1, "b")
        n3 = db.upsert_node(1, "c")
        db.upsert_edge(n1, n2, 10)
        db.upsert_edge(n1, n3, 20)
        result = db.neighbors_batch([n1])
        assert isinstance(result, dict)
        assert n1 in result
        assert len(result[n1]) == 2
        node_ids = {e.node_id for e in result[n1]}
        assert node_ids == {n2, n3}

    def test_multiple_nodes(self, db):
        n1 = db.upsert_node(1, "a")
        n2 = db.upsert_node(1, "b")
        n3 = db.upsert_node(1, "c")
        db.upsert_edge(n1, n2, 10)
        db.upsert_edge(n2, n3, 10)
        result = db.neighbors_batch([n1, n2])
        assert n1 in result
        assert n2 in result
        assert len(result[n1]) == 1
        assert result[n1][0].node_id == n2
        assert len(result[n2]) == 1
        assert result[n2][0].node_id == n3

    def test_direction(self, db):
        n1 = db.upsert_node(1, "a")
        n2 = db.upsert_node(1, "b")
        db.upsert_edge(n1, n2, 10)
        result = db.neighbors_batch([n2], direction="incoming")
        assert n2 in result
        assert result[n2][0].node_id == n1

    def test_type_filter(self, db):
        n1 = db.upsert_node(1, "a")
        n2 = db.upsert_node(1, "b")
        n3 = db.upsert_node(1, "c")
        db.upsert_edge(n1, n2, 10)
        db.upsert_edge(n1, n3, 20)
        result = db.neighbors_batch([n1], type_filter=[10])
        assert len(result[n1]) == 1
        assert result[n1][0].node_id == n2

    def test_empty_input(self, db):
        result = db.neighbors_batch([])
        assert result == {}

    def test_no_neighbors(self, db):
        lonely = db.upsert_node(1, "lonely")
        result = db.neighbors_batch([lonely])
        # Engine filters empty entries
        assert lonely not in result

    def test_matches_individual(self, db):
        """Batch results match individual neighbors calls."""
        n1 = db.upsert_node(1, "a")
        n2 = db.upsert_node(1, "b")
        n3 = db.upsert_node(1, "c")
        db.upsert_edge(n1, n2, 10)
        db.upsert_edge(n1, n3, 10)

        individual = db.neighbors(n1, "outgoing")
        batch = db.neighbors_batch([n1])
        assert {e.node_id for e in individual} == {e.node_id for e in batch[n1]}


class TestExtractSubgraph:
    def test_subgraph_depth1(self, db):
        center, spokes = make_star(db)
        sg = db.extract_subgraph(center, max_depth=1)
        assert len(sg.nodes) == 6  # center + 5 spokes
        assert len(sg.edges) == 5

    def test_subgraph_depth2(self, db):
        nodes, _ = make_chain(db, 4)
        sg = db.extract_subgraph(nodes[0], max_depth=2)
        # Should include nodes[0], nodes[1], nodes[2] (depth 0, 1, 2)
        sg_ids = {n.id for n in sg.nodes}
        assert nodes[0] in sg_ids
        assert nodes[1] in sg_ids
        assert nodes[2] in sg_ids

    def test_subgraph_repr(self, db):
        center, _ = make_star(db, spokes=2)
        sg = db.extract_subgraph(center, max_depth=1)
        r = repr(sg)
        assert "Subgraph" in r

    def test_subgraph_edge_type_filter(self, db):
        n1 = db.upsert_node(1, "a")
        n2 = db.upsert_node(1, "b")
        n3 = db.upsert_node(1, "c")
        db.upsert_edge(n1, n2, 10)
        db.upsert_edge(n1, n3, 20)
        sg = db.extract_subgraph(n1, max_depth=1, edge_type_filter=[10])
        assert len(sg.edges) == 1
        sg_ids = {n.id for n in sg.nodes}
        assert n2 in sg_ids
