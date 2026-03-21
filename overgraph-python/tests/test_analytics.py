from conftest import make_chain, make_star


class TestPersonalizedPagerank:
    def test_basic_ppr(self, db):
        center, spokes = make_star(db)
        result = db.personalized_pagerank([center])
        assert len(result.node_ids) > 0
        assert len(result.scores) == len(result.node_ids)
        assert result.iterations > 0
        assert "PprResult" in repr(result)
        # Scores are sorted descending
        assert result.scores == sorted(result.scores, reverse=True)

    def test_ppr_seed_has_highest_score(self, db):
        center, spokes = make_star(db)
        result = db.personalized_pagerank([center])
        # The seed node should have the highest score
        assert result.node_ids[0] == center

    def test_ppr_multiple_seeds(self, db):
        # Two disconnected stars
        c1 = db.upsert_node(1, "c1")
        for i in range(3):
            s = db.upsert_node(1, f"s1_{i}")
            db.upsert_edge(c1, s, 10)
        c2 = db.upsert_node(1, "c2")
        for i in range(3):
            s = db.upsert_node(1, f"s2_{i}")
            db.upsert_edge(c2, s, 10)
        result = db.personalized_pagerank([c1, c2])
        assert len(result.node_ids) > 0
        # Both seeds should appear
        assert c1 in result.node_ids
        assert c2 in result.node_ids

    def test_ppr_edge_type_filter(self, db):
        n1 = db.upsert_node(1, "a")
        n2 = db.upsert_node(1, "b")
        n3 = db.upsert_node(1, "c")
        db.upsert_edge(n1, n2, 10)
        db.upsert_edge(n1, n3, 20)
        # Only follow type 10 edges
        result = db.personalized_pagerank([n1], edge_type_filter=[10])
        assert n2 in result.node_ids
        # n3 should not be reachable via type 10
        assert n3 not in result.node_ids

    def test_ppr_max_results(self, db):
        center, spokes = make_star(db, spokes=10)
        result = db.personalized_pagerank([center], max_results=3)
        assert len(result.node_ids) == 3
        assert len(result.scores) == 3

    def test_ppr_custom_params(self, db):
        center, spokes = make_star(db)
        result = db.personalized_pagerank(
            [center],
            damping_factor=0.5,
            max_iterations=100,
            epsilon=1e-4,
        )
        assert result.iterations > 0
        assert len(result.node_ids) > 0

    def test_ppr_approx_mode_exposes_metadata(self, db):
        center, spokes = make_star(db)
        result = db.personalized_pagerank(
            [center],
            algorithm="approx",
            approx_residual_tolerance=1e-6,
        )
        assert result.algorithm == "approx"
        assert result.approx is not None
        assert result.approx.residual_tolerance == 1e-6
        assert result.approx.pushes >= 0
        assert len(result.node_ids) > 0

    def test_ppr_isolated_node(self, db):
        nid = db.upsert_node(1, "lonely")
        result = db.personalized_pagerank([nid])
        assert len(result.node_ids) == 1
        assert result.node_ids[0] == nid
        assert abs(result.scores[0] - 1.0) < 0.01

    def test_ppr_chain(self, db):
        nodes, _ = make_chain(db, 5)
        result = db.personalized_pagerank([nodes[0]])
        # All nodes should be reachable
        for n in nodes:
            assert n in result.node_ids
        # Seed should have highest score
        assert result.node_ids[0] == nodes[0]
        # Scores should decrease with distance from seed
        for i in range(1, len(nodes)):
            prev_idx = result.node_ids.index(nodes[i - 1])
            curr_idx = result.node_ids.index(nodes[i])
            assert result.scores[prev_idx] >= result.scores[curr_idx]

    def test_ppr_empty_seeds(self, db):
        db.upsert_node(1, "a")
        result = db.personalized_pagerank([])
        assert len(result.node_ids) == 0
        assert len(result.scores) == 0


class TestExportAdjacency:
    def test_basic_export(self, db):
        center, spokes = make_star(db)
        export = db.export_adjacency()
        assert len(export.node_ids) == 6  # center + 5 spokes
        assert len(export.edges) == 5
        assert "AdjacencyExport" in repr(export)
        # Check edge fields
        e = export.edges[0]
        assert "ExportEdge" in repr(e)
        assert e.from_id in export.node_ids
        assert e.to_id in export.node_ids

    def test_export_node_type_filter(self, db):
        n1 = db.upsert_node(1, "a")
        n2 = db.upsert_node(2, "b")
        n3 = db.upsert_node(1, "c")
        db.upsert_edge(n1, n2, 10)
        db.upsert_edge(n1, n3, 10)
        # Only type 1 nodes
        export = db.export_adjacency(node_type_filter=[1])
        assert n1 in export.node_ids
        assert n3 in export.node_ids
        assert n2 not in export.node_ids
        # Edge n1->n2 should be excluded (n2 not in subgraph)
        for e in export.edges:
            assert e.to_id != n2

    def test_export_edge_type_filter(self, db):
        n1 = db.upsert_node(1, "a")
        n2 = db.upsert_node(1, "b")
        n3 = db.upsert_node(1, "c")
        db.upsert_edge(n1, n2, 10)
        db.upsert_edge(n1, n3, 20)
        export = db.export_adjacency(edge_type_filter=[10])
        # All nodes should be present (node filter is separate)
        assert len(export.node_ids) == 3
        # Only type 10 edges
        assert all(e.type_id == 10 for e in export.edges)

    def test_export_include_weights(self, db):
        n1 = db.upsert_node(1, "a")
        n2 = db.upsert_node(1, "b")
        db.upsert_edge(n1, n2, 10, weight=3.5)
        export = db.export_adjacency(include_weights=True)
        assert len(export.edges) == 1
        assert abs(export.edges[0].weight - 3.5) < 0.01

    def test_export_without_weights(self, db):
        n1 = db.upsert_node(1, "a")
        n2 = db.upsert_node(1, "b")
        db.upsert_edge(n1, n2, 10, weight=3.5)
        export = db.export_adjacency(include_weights=False)
        assert len(export.edges) == 1
        # Without weights, weight should be 0
        assert export.edges[0].weight == 0.0

    def test_export_empty(self, db):
        export = db.export_adjacency()
        assert len(export.node_ids) == 0
        assert len(export.edges) == 0

    def test_export_consistent_subgraph(self, db):
        """Edges should only appear if both endpoints are in the node set."""
        n1 = db.upsert_node(1, "a")
        n2 = db.upsert_node(2, "b")
        db.upsert_edge(n1, n2, 10)
        # Filter to only type 1 (n2 excluded)
        export = db.export_adjacency(node_type_filter=[1])
        assert n1 in export.node_ids
        assert n2 not in export.node_ids
        # No edges should be present since n2 is not in node set
        assert len(export.edges) == 0
