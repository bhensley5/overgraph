"""Vector search tests for the Python connector (Phase 19f)."""

import pytest
from overgraph import OverGraph, VectorHit


@pytest.fixture
def hybrid_db(tmp_path):
    db = OverGraph.open(
        str(tmp_path / "hybrid"),
        dense_vector_dimension=4,
        dense_vector_metric="cosine",
    )
    # Node 1: dense rank #1, sparse rank #4
    n1 = db.upsert_node("Person", "n1", dense_vector=[0.95, 0.05, 0.05, 0.05],
                         sparse_vector=[(0, 0.2), (1, 0.1)])
    # Node 2: dense rank #4, sparse rank #1
    n2 = db.upsert_node("Person", "n2", dense_vector=[0.3, 0.5, 0.5, 0.5],
                         sparse_vector=[(0, 0.9), (1, 0.8), (2, 0.7)])
    # Node 3: dense rank #2, sparse rank #2. Balanced.
    n3 = db.upsert_node("Person", "n3", dense_vector=[0.85, 0.1, 0.1, 0.1],
                         sparse_vector=[(0, 0.7), (1, 0.6)])
    # Node 4: dense rank #3, sparse rank #3
    n4 = db.upsert_node("Person", "n4", dense_vector=[0.6, 0.3, 0.3, 0.3],
                         sparse_vector=[(0, 0.5), (2, 0.3)])
    # Node 5: dense rank #5, sparse rank #5
    n5 = db.upsert_node("Person", "n5", dense_vector=[0.1, 0.4, 0.6, 0.6],
                         sparse_vector=[(1, 0.1)])
    db.flush()
    yield db, [n1, n2, n3, n4, n5]
    db.close()


class TestDenseSearch:
    def test_hydrated_node_records_preserve_vectors(self, hybrid_db):
        db, ids = hybrid_db
        node = db.get_node(ids[0])
        assert node.dense_vector == pytest.approx([0.95, 0.05, 0.05, 0.05])
        assert [dim for dim, _ in node.sparse_vector] == [0, 1]
        assert [weight for _, weight in node.sparse_vector] == pytest.approx([0.2, 0.1])

        queried = db.query_nodes({"label_filter": {"labels": ["Person"], "mode": "all"}, "keys": ["n1"]})
        assert len(queried.items) == 1
        assert queried.items[0].dense_vector == pytest.approx([0.95, 0.05, 0.05, 0.05])
        assert [dim for dim, _ in queried.items[0].sparse_vector] == [0, 1]
        assert [weight for _, weight in queried.items[0].sparse_vector] == pytest.approx([0.2, 0.1])

    def test_returns_sorted_results(self, hybrid_db):
        db, ids = hybrid_db
        hits = db.vector_search("dense", 5, dense_query=[1, 0, 0, 0])
        assert len(hits) == 5
        assert hits[0].node_id == ids[0]  # highest cosine sim
        for i in range(1, len(hits)):
            assert hits[i - 1].score >= hits[i].score

    def test_sparse_search(self, hybrid_db):
        db, ids = hybrid_db
        hits = db.vector_search("sparse", 5, sparse_query=[(0, 1.0), (1, 0.5)])
        assert len(hits) == 5
        assert hits[0].node_id == ids[1]  # highest sparse dot product

    def test_errors_when_dense_search_is_not_configured(self, tmp_path):
        db = OverGraph.open(str(tmp_path / "no-dense-config"))
        try:
            with pytest.raises(Exception, match="dense vector search is not configured"):
                db.vector_search("dense", 1, dense_query=[1, 0, 0, 0])
        finally:
            db.close()


class TestHybridSearch:
    def test_weighted_rank_fusion_promotes_balanced(self, hybrid_db):
        db, ids = hybrid_db
        hits = db.vector_search(
            "hybrid", 5,
            dense_query=[1, 0, 0, 0],
            sparse_query=[(0, 1.0), (1, 0.5), (2, 0.3)],
            dense_weight=1.0, sparse_weight=1.0,
            fusion_mode="weighted_rank",
        )
        assert len(hits) == 5
        assert hits[0].node_id == ids[2]  # balanced node

    def test_heavy_dense_weight_promotes_dense_top(self, hybrid_db):
        db, ids = hybrid_db
        hits = db.vector_search(
            "hybrid", 5,
            dense_query=[1, 0, 0, 0],
            sparse_query=[(0, 1.0), (1, 0.5)],
            dense_weight=5.0, sparse_weight=1.0,
            fusion_mode="weighted_rank",
        )
        assert hits[0].node_id == ids[0]

    def test_reciprocal_rank_ignores_weights(self, hybrid_db):
        db, ids = hybrid_db
        a = db.vector_search(
            "hybrid", 5,
            dense_query=[1, 0, 0, 0], sparse_query=[(0, 1.0), (1, 0.5)],
            fusion_mode="reciprocal_rank",
        )
        b = db.vector_search(
            "hybrid", 5,
            dense_query=[1, 0, 0, 0], sparse_query=[(0, 1.0), (1, 0.5)],
            dense_weight=99.0, sparse_weight=0.01,
            fusion_mode="reciprocal_rank",
        )
        assert len(a) == len(b)
        for x, y in zip(a, b):
            assert x.node_id == y.node_id

    def test_weighted_score_fusion(self, hybrid_db):
        db, ids = hybrid_db
        hits = db.vector_search(
            "hybrid", 5,
            dense_query=[1, 0, 0, 0], sparse_query=[(0, 1.0)],
            dense_weight=1.0, sparse_weight=1.0,
            fusion_mode="weighted_score",
        )
        assert len(hits) == 5
        for i in range(1, len(hits)):
            assert hits[i - 1].score >= hits[i].score

    def test_degeneration_dense_only(self, hybrid_db):
        db, ids = hybrid_db
        dense = db.vector_search("dense", 5, dense_query=[1, 0, 0, 0])
        hybrid = db.vector_search("hybrid", 5, dense_query=[1, 0, 0, 0])
        assert len(dense) == len(hybrid)
        for d, h in zip(dense, hybrid):
            assert d.node_id == h.node_id

    def test_missing_both_queries_errors(self, hybrid_db):
        db, _ = hybrid_db
        with pytest.raises(Exception, match="requires at least one"):
            db.vector_search("hybrid", 5)

    def test_k_zero_returns_empty(self, hybrid_db):
        db, _ = hybrid_db
        hits = db.vector_search(
            "hybrid", 0,
            dense_query=[1, 0, 0, 0], sparse_query=[(0, 1.0)],
        )
        assert len(hits) == 0

    def test_default_fusion_mode(self, hybrid_db):
        db, _ = hybrid_db
        default = db.vector_search(
            "hybrid", 5,
            dense_query=[1, 0, 0, 0], sparse_query=[(0, 1.0)],
            dense_weight=1.0, sparse_weight=1.0,
        )
        explicit = db.vector_search(
            "hybrid", 5,
            dense_query=[1, 0, 0, 0], sparse_query=[(0, 1.0)],
            dense_weight=1.0, sparse_weight=1.0,
            fusion_mode="weighted_rank",
        )
        assert len(default) == len(explicit)
        for d, e in zip(default, explicit):
            assert d.node_id == e.node_id


class TestVectorHitRepr:
    def test_repr(self, hybrid_db):
        db, _ = hybrid_db
        hits = db.vector_search("dense", 1, dense_query=[1, 0, 0, 0])
        assert "VectorHit" in repr(hits[0])


class TestVectorSearchScope:
    def test_scope_limits_results(self, tmp_path):
        db = OverGraph.open(
            str(tmp_path / "scope"),
            dense_vector_dimension=4,
            dense_vector_metric="cosine",
        )
        ids = []
        for i in range(4):
            ids.append(db.upsert_node(
                "Person", f"n{i}",
                dense_vector=[1, 0, 0, 0],
                sparse_vector=[(0, (i + 1) * 0.3)],
            ))
        db.upsert_edge(ids[0], ids[1], "RELATES_TO")
        db.upsert_edge(ids[1], ids[2], "RELATES_TO")
        db.flush()

        hits = db.vector_search(
            "hybrid", 10,
            dense_query=[1, 0, 0, 0], sparse_query=[(0, 1.0)],
            scope_start_node_id=ids[0], scope_max_depth=1,
            scope_direction="outgoing",
        )
        hit_ids = [h.node_id for h in hits]
        assert ids[0] in hit_ids
        assert ids[1] in hit_ids
        assert ids[2] not in hit_ids  # depth 2
        assert ids[3] not in hit_ids  # disconnected
        db.close()


class TestVectorlessReplacement:
    @pytest.mark.parametrize("clear_count", [1, 64])
    def test_hnsw_hides_old_vectors_after_flush_and_reopen(self, tmp_path, clear_count):
        path = str(tmp_path / "vectorless-hnsw")
        opts = dict(dense_vector_dimension=4, compact_after_n_flushes=100)
        query = [1.0, 0.0, 0.0, 0.0]

        def assert_visible_hits(db):
            hits = db.vector_search("dense", 10, dense_query=query)
            assert [hit.node_id for hit in hits] == ids[clear_count:clear_count + 10]

        with OverGraph.open(path, **opts) as db:
            ids = [db.upsert_node("Fragment", f"n{i}") for i in range(64)]
            db.flush()
            for i in range(len(ids)):
                db.upsert_node("Fragment", f"n{i}", dense_vector=query)
            db.flush()
            assert list(tmp_path.rglob("dense_hnsw_graph.dat"))
            assert [hit.node_id for hit in db.vector_search(
                "dense", 10, dense_query=query
            )] == ids[:10]

            for i in range(clear_count):
                db.upsert_node("Fragment", f"n{i}", props={"scope": "backfill"})
            db.flush()
            assert db.get_node(ids[0]).dense_vector is None
            assert_visible_hits(db)

        with OverGraph.open(path, **opts) as db:
            assert_visible_hits(db)

    def test_scoped_search_ignores_cleared_and_deleted_vectors(self, tmp_path):
        dense = [1.0, 0.0, 0.0, 0.0]
        sparse = [(1, 1.0)]
        path = str(tmp_path / "vectorless-scope")
        opts = dict(dense_vector_dimension=4, compact_after_n_flushes=100)

        def assert_visible_hits(db):
            for mode, query in (
                ("dense", {"dense_query": dense}),
                ("sparse", {"sparse_query": sparse}),
                ("hybrid", {"dense_query": dense, "sparse_query": sparse}),
            ):
                hits = db.vector_search(
                    mode, 10, scope_start_node_id=ids[0], scope_max_depth=1,
                    **query,
                )
                assert {hit.node_id for hit in hits} == {ids[0], ids[2]}

        with OverGraph.open(path, **opts) as db:
            ids = [
                db.upsert_node("Fragment", f"n{i}", dense_vector=dense, sparse_vector=sparse)
                for i in range(4)
            ]
            for node in ids[1:]:
                db.upsert_edge(ids[0], node, "PATH")
            db.flush()
            db.upsert_node("Fragment", "n1")
            db.delete_node(ids[3])
            db.flush()
            assert_visible_hits(db)

        with OverGraph.open(path, **opts) as db:
            assert_visible_hits(db)
