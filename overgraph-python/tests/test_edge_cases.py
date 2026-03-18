"""Tests filling gaps identified in the test audit.

Covers: temporal filtering (at_epoch), PPR damping edge cases,
pagination cursor edge cases, self-loops, compact_with_progress depth,
concurrent async stress, and prune-by-age semantics.
"""

import os
import shutil
import tempfile
import time

import pytest

from overgraph import OverGraph, OverGraphError

from conftest import make_chain, make_star


# ---------------------------------------------------------------------------
# 1. Temporal filtering via at_epoch
# ---------------------------------------------------------------------------


class TestTemporalFiltering:
    def test_neighbors_at_epoch_filters_by_validity(self, db):
        """Edges with non-overlapping validity windows are filtered by epoch."""
        a = db.upsert_node(1, "a")
        b = db.upsert_node(1, "b")
        c = db.upsert_node(1, "c")
        # A->B valid [1000, 5000), A->C valid [3000, 9000)
        db.upsert_edge(a, b, 10, valid_from=1000, valid_to=5000)
        db.upsert_edge(a, c, 10, valid_from=3000, valid_to=9000)

        # epoch 2000: only A->B is valid
        nbrs = db.neighbors(a, direction="outgoing", at_epoch=2000)
        ids = {n.node_id for n in nbrs}
        assert ids == {b}

        # epoch 4000: both valid
        nbrs = db.neighbors(a, direction="outgoing", at_epoch=4000)
        ids = {n.node_id for n in nbrs}
        assert ids == {b, c}

        # epoch 6000: only A->C is valid (A->B expired at 5000)
        nbrs = db.neighbors(a, direction="outgoing", at_epoch=6000)
        ids = {n.node_id for n in nbrs}
        assert ids == {c}

        # epoch 99999: neither valid
        nbrs = db.neighbors(a, direction="outgoing", at_epoch=99999)
        assert nbrs == []

    def test_neighbors_at_epoch_without_filter_uses_current_time(self, db):
        """Without at_epoch, defaults to current time -- expired edges excluded."""
        a = db.upsert_node(1, "a")
        b = db.upsert_node(1, "b")
        c = db.upsert_node(1, "c")
        now = int(time.time() * 1000)
        # One edge valid now, one expired
        db.upsert_edge(a, b, 10, valid_from=0, valid_to=now + 60000)  # valid
        db.upsert_edge(a, c, 10, valid_from=1000, valid_to=5000)  # expired

        nbrs = db.neighbors(a, direction="outgoing")
        ids = {n.node_id for n in nbrs}
        assert b in ids, "currently-valid edge should be returned"
        assert c not in ids, "expired edge should be excluded"

    def test_traverse_two_hop_at_epoch(self, db):
        """Traversal respects at_epoch across both hops."""
        a = db.upsert_node(1, "a")
        b = db.upsert_node(1, "b")
        c = db.upsert_node(1, "c")
        # A->B valid [1000, 5000), B->C valid [2000, 6000)
        db.upsert_edge(a, b, 10, valid_from=1000, valid_to=5000)
        db.upsert_edge(b, c, 10, valid_from=2000, valid_to=6000)

        # epoch 3000: both hops valid, C reachable
        page = db.traverse(a, 2, min_depth=2, direction="outgoing", at_epoch=3000)
        assert [(hit.node_id, hit.depth) for hit in page.items] == [(c, 2)]

        # epoch 500: A->B not yet valid, so nothing reachable
        nbrs_1hop = db.neighbors(a, direction="outgoing", at_epoch=500)
        assert len(nbrs_1hop) == 0
        page = db.traverse(a, 2, min_depth=2, direction="outgoing", at_epoch=500)
        assert len(page.items) == 0

        # epoch 5500: A->B expired, so B not reachable at 1-hop, C not reachable
        nbrs_1hop = db.neighbors(a, direction="outgoing", at_epoch=5500)
        assert len(nbrs_1hop) == 0
        page = db.traverse(a, 2, min_depth=2, direction="outgoing", at_epoch=5500)
        assert len(page.items) == 0

    def test_neighbors_batch_at_epoch(self, db):
        """Batch neighbor queries respect at_epoch."""
        a = db.upsert_node(1, "a")
        b = db.upsert_node(1, "b")
        c = db.upsert_node(1, "c")
        db.upsert_edge(a, b, 10, valid_from=1000, valid_to=5000)
        db.upsert_edge(a, c, 10, valid_from=3000, valid_to=9000)

        # epoch 2000: only A->B valid
        result = db.neighbors_batch([a], at_epoch=2000)
        if a in result:
            ids = {n.node_id for n in result[a]}
            assert ids == {b}
        else:
            pytest.fail("Expected node 'a' in batch result at epoch 2000")

        # epoch 99999: neither valid
        result = db.neighbors_batch([a], at_epoch=99999)
        # Either key absent or empty list
        assert a not in result or len(result[a]) == 0

    def test_top_k_at_epoch(self, db):
        """top_k_neighbors with at_epoch filters correctly."""
        center = db.upsert_node(1, "center")
        s1 = db.upsert_node(1, "s1")
        s2 = db.upsert_node(1, "s2")
        s3 = db.upsert_node(1, "s3")
        db.upsert_edge(center, s1, 10, weight=3.0, valid_from=1000, valid_to=5000)
        db.upsert_edge(center, s2, 10, weight=2.0, valid_from=3000, valid_to=9000)
        db.upsert_edge(center, s3, 10, weight=1.0, valid_from=3000, valid_to=9000)

        # epoch 2000: only s1 valid
        top = db.top_k_neighbors(center, 3, direction="outgoing", scoring="weight", at_epoch=2000)
        ids = {n.node_id for n in top}
        assert ids == {s1}

        # epoch 4000: all three valid, top 2 by weight should be s1 and s2
        top2 = db.top_k_neighbors(center, 2, direction="outgoing", scoring="weight", at_epoch=4000)
        ids = {n.node_id for n in top2}
        assert ids == {s1, s2}

        # epoch 99999: none valid
        top_none = db.top_k_neighbors(
            center, 3, direction="outgoing", scoring="weight", at_epoch=99999
        )
        assert len(top_none) == 0

    def test_extract_subgraph_at_epoch(self, db):
        """Subgraph extraction respects at_epoch."""
        a = db.upsert_node(1, "a")
        b = db.upsert_node(1, "b")
        c = db.upsert_node(1, "c")
        db.upsert_edge(a, b, 10, valid_from=1000, valid_to=5000)
        db.upsert_edge(b, c, 10, valid_from=2000, valid_to=6000)

        # epoch 3000: both edges valid
        sg = db.extract_subgraph(a, 2, at_epoch=3000)
        sg_ids = {n.id for n in sg.nodes}
        assert {a, b, c} == sg_ids

        # epoch 500: A->B not valid yet, only A reachable
        sg = db.extract_subgraph(a, 2, at_epoch=500)
        sg_ids = {n.id for n in sg.nodes}
        assert sg_ids == {a}


# ---------------------------------------------------------------------------
# 2. PPR damping_factor edge cases
# ---------------------------------------------------------------------------


class TestPprEdgeCases:
    def test_ppr_low_damping_seed_dominates(self, db):
        """With very low damping, almost all rank stays at the seed."""
        nodes, _ = make_chain(db, 4)
        result = db.personalized_pagerank([nodes[0]], damping_factor=0.01)
        # Seed should have the vast majority of rank
        seed_idx = result.node_ids.index(nodes[0])
        assert result.scores[seed_idx] > 0.90

    def test_ppr_high_damping_spreads_rank(self, db):
        """With very high damping, rank should spread beyond the seed."""
        nodes, _ = make_chain(db, 4)
        result = db.personalized_pagerank(
            [nodes[0]], damping_factor=0.99, max_iterations=200
        )
        # Seed should still be first, but with less dominance
        seed_idx = result.node_ids.index(nodes[0])
        assert result.scores[seed_idx] < 0.90
        # At least some downstream nodes should have meaningful rank
        total = sum(result.scores)
        non_seed_total = total - result.scores[seed_idx]
        assert non_seed_total > 0.10

    def test_ppr_damping_zero_point_five(self, db):
        """damping=0.5 should converge and give a reasonable distribution."""
        center, spokes = make_star(db, spokes=5)
        result = db.personalized_pagerank([center], damping_factor=0.5)
        assert result.iterations > 0
        assert len(result.node_ids) >= 1
        # Seed should still lead
        assert result.node_ids[0] == center
        # All scores should be positive
        assert all(s > 0 for s in result.scores)

    def test_ppr_scores_sum_approximately_one(self, db):
        """PPR scores should sum to approximately 1.0."""
        center, spokes = make_star(db, spokes=5)
        result = db.personalized_pagerank([center])
        total = sum(result.scores)
        assert abs(total - 1.0) < 0.05


# ---------------------------------------------------------------------------
# 3. Pagination cursor edge cases
# ---------------------------------------------------------------------------


class TestPaginationEdgeCases:
    def test_cursor_past_all_ids_returns_empty(self, db):
        """A cursor beyond all existing IDs should return empty results."""
        for i in range(5):
            db.upsert_node(1, f"n{i}")
        page = db.nodes_by_type_paged(1, limit=50, after=999999999)
        assert len(page.items) == 0
        assert page.next_cursor is None

    def test_cursor_zero_returns_first_page(self, db):
        """after=0 should behave like the first page (all IDs are > 0)."""
        for i in range(5):
            db.upsert_node(1, f"n{i}")
        page_no_cursor = db.nodes_by_type_paged(1, limit=50)
        page_zero = db.nodes_by_type_paged(1, limit=50, after=0)
        assert page_no_cursor.items == page_zero.items

    def test_find_nodes_paged_cursor_traversal(self, db):
        """Exhaustive cursor traversal via find_nodes_paged."""
        for i in range(7):
            db.upsert_node(1, f"n{i}", props={"tag": "x"})
        all_ids = []
        cursor = None
        while True:
            page = db.find_nodes_paged(1, "tag", "x", limit=3, after=cursor)
            all_ids.extend(page.items)
            if page.next_cursor is None:
                break
            cursor = page.next_cursor
        assert len(all_ids) == 7
        # No duplicates
        assert len(set(all_ids)) == 7

    def test_edges_paged_cursor_past_all_returns_empty(self, db):
        """Edge pagination with cursor past all IDs returns empty."""
        nodes, edges = make_chain(db, 4)
        page = db.edges_by_type_paged(10, limit=50, after=999999999)
        assert len(page.items) == 0


# ---------------------------------------------------------------------------
# 4. Self-loops
# ---------------------------------------------------------------------------


class TestSelfLoops:
    def test_self_loop_in_neighbors_outgoing(self, db):
        """A self-loop should appear in outgoing neighbors."""
        a = db.upsert_node(1, "a")
        db.upsert_edge(a, a, 10)
        nbrs = db.neighbors(a, direction="outgoing")
        assert len(nbrs) == 1
        assert nbrs[0].node_id == a

    def test_self_loop_in_neighbors_incoming(self, db):
        """A self-loop should appear in incoming neighbors."""
        a = db.upsert_node(1, "a")
        db.upsert_edge(a, a, 10)
        nbrs = db.neighbors(a, direction="incoming")
        assert len(nbrs) == 1
        assert nbrs[0].node_id == a

    def test_self_loop_both_direction_no_duplicate(self, db):
        """A self-loop queried with 'both' should not produce duplicates."""
        a = db.upsert_node(1, "a")
        db.upsert_edge(a, a, 10)
        nbrs = db.neighbors(a, direction="both")
        # Self-loop should appear exactly once (deduplicated)
        assert len(nbrs) == 1
        assert nbrs[0].node_id == a

    def test_self_loop_in_subgraph(self, db):
        """Subgraph extraction handles self-loops without infinite recursion."""
        a = db.upsert_node(1, "a")
        db.upsert_edge(a, a, 10)
        sg = db.extract_subgraph(a, 2)
        assert len(sg.nodes) == 1
        assert len(sg.edges) == 1

    def test_self_loop_with_regular_edges(self, db):
        """Self-loop combined with regular edges should all appear."""
        a = db.upsert_node(1, "a")
        b = db.upsert_node(1, "b")
        db.upsert_edge(a, a, 10)
        db.upsert_edge(a, b, 10)
        nbrs = db.neighbors(a, direction="outgoing")
        ids = {n.node_id for n in nbrs}
        assert ids == {a, b}

    def test_self_loop_in_batch_neighbors(self, db):
        """Batch neighbor query includes self-loops."""
        a = db.upsert_node(1, "a")
        db.upsert_edge(a, a, 10)
        result = db.neighbors_batch([a])
        assert a in result
        ids = {n.node_id for n in result[a]}
        assert a in ids


# ---------------------------------------------------------------------------
# 5. compact_with_progress callback depth
# ---------------------------------------------------------------------------


class TestCompactWithProgressDepth:
    def test_callback_receives_valid_progress_fields(self, db):
        """Progress callback receives objects with phase, records_processed, total_records."""
        # Create data and flush multiple times to guarantee segments to compact
        for batch in range(3):
            for i in range(20):
                db.upsert_node(1, f"b{batch}_n{i}")
            db.flush()

        progress_events = []

        def on_progress(p):
            progress_events.append({
                "phase": p.phase,
                "records_processed": p.records_processed,
                "total_records": p.total_records,
            })
            return True

        result = db.compact_with_progress(on_progress)
        if result is not None:
            assert len(progress_events) > 0
            # Every event should have string phase and non-negative counters
            for ev in progress_events:
                assert isinstance(ev["phase"], str)
                assert ev["records_processed"] >= 0
                assert ev["total_records"] >= 0

    def test_callback_cancellation_stops_compaction(self, db):
        """Returning False from callback cancels compaction without data loss."""
        for batch in range(3):
            for i in range(20):
                db.upsert_node(1, f"b{batch}_n{i}")
            db.flush()

        def cancel_first_call(_p):
            return False

        try:
            db.compact_with_progress(cancel_first_call)
        except OverGraphError:
            pass  # CompactionCancelled is acceptable

        # Data should still be intact after cancelled compaction
        # We inserted 60 unique nodes total (3 batches x 20)
        # Each batch re-upserts the same keys within its group, but keys are unique
        page = db.nodes_by_type_paged(1, limit=100)
        assert len(page.items) == 60

    def test_callback_exception_propagates(self, db):
        """A RuntimeError raised in the callback should propagate."""
        for batch in range(2):
            for i in range(20):
                db.upsert_node(1, f"b{batch}_n{i}")
            db.flush()

        def raise_error(_p):
            raise RuntimeError("test error in callback")

        with pytest.raises(RuntimeError, match="test error in callback"):
            db.compact_with_progress(raise_error)

    def test_callback_receives_monotonic_progress(self, db):
        """records_processed should be monotonically non-decreasing within a phase."""
        for batch in range(3):
            for i in range(20):
                db.upsert_node(1, f"b{batch}_n{i}")
            db.flush()

        events_by_phase = {}

        def on_progress(p):
            phase = p.phase
            if phase not in events_by_phase:
                events_by_phase[phase] = []
            events_by_phase[phase].append(p.records_processed)
            return True

        result = db.compact_with_progress(on_progress)
        if result is not None:
            for phase, counts in events_by_phase.items():
                for i in range(1, len(counts)):
                    assert counts[i] >= counts[i - 1], (
                        f"records_processed decreased in phase {phase}: "
                        f"{counts[i - 1]} -> {counts[i]}"
                    )


# ---------------------------------------------------------------------------
# 6. Concurrent async stress
# ---------------------------------------------------------------------------


class TestAsyncStress:
    @pytest.mark.asyncio
    async def test_concurrent_upserts(self, tmp_dir):
        """Fire 50 concurrent upsert_node calls and verify all succeed."""
        import asyncio
        from overgraph import AsyncOverGraph

        db_path = os.path.join(tmp_dir, "stress_db")
        db = await AsyncOverGraph.open(db_path)
        try:
            tasks = [db.upsert_node(1, f"node-{i}") for i in range(50)]
            ids = await asyncio.gather(*tasks)
            assert len(ids) == 50
            assert len(set(ids)) == 50  # all unique

            # Verify all readable
            for node_id in ids:
                n = await db.get_node(node_id)
                assert n is not None
        finally:
            await db.close()

    @pytest.mark.asyncio
    async def test_concurrent_mixed_operations(self, tmp_dir):
        """Concurrent upserts and reads should not interfere with each other."""
        import asyncio
        from overgraph import AsyncOverGraph

        db_path = os.path.join(tmp_dir, "stress_mixed")
        db = await AsyncOverGraph.open(db_path)
        try:
            # Pre-create some nodes
            pre_ids = []
            for i in range(10):
                nid = await db.upsert_node(1, f"pre-{i}")
                pre_ids.append(nid)

            # Mix upserts and reads concurrently
            upsert_tasks = [db.upsert_node(1, f"new-{i}") for i in range(20)]
            read_tasks = [db.get_node(nid) for nid in pre_ids]
            all_results = await asyncio.gather(*upsert_tasks, *read_tasks)

            new_ids = all_results[:20]
            read_nodes = all_results[20:]

            assert len(set(new_ids)) == 20
            assert all(n is not None for n in read_nodes)
        finally:
            await db.close()

    @pytest.mark.asyncio
    async def test_concurrent_edge_creation(self, tmp_dir):
        """Concurrent edge upserts between pre-created nodes."""
        import asyncio
        from overgraph import AsyncOverGraph

        db_path = os.path.join(tmp_dir, "stress_edges")
        db = await AsyncOverGraph.open(db_path)
        try:
            # Create nodes first
            nodes = []
            for i in range(10):
                nid = await db.upsert_node(1, f"n{i}")
                nodes.append(nid)

            # Create edges concurrently (each from nodes[i] to nodes[i+1])
            edge_tasks = [
                db.upsert_edge(nodes[i], nodes[(i + 1) % 10], 10)
                for i in range(10)
            ]
            edge_ids = await asyncio.gather(*edge_tasks)
            assert len(edge_ids) == 10
            assert len(set(edge_ids)) == 10
        finally:
            await db.close()


# ---------------------------------------------------------------------------
# 7. Prune by age
# ---------------------------------------------------------------------------


class TestPruneByAge:
    def test_prune_max_age_removes_old_nodes(self, db):
        """Pruning with max_age_ms=1 should remove everything after a short sleep."""
        for i in range(5):
            db.upsert_node(1, f"n{i}")
        # Wait just enough so all nodes are older than 1ms
        time.sleep(0.05)
        result = db.prune(max_age_ms=1)
        assert result.nodes_pruned == 5

    def test_prune_max_age_keeps_recent(self, db):
        """Pruning with a very large max_age_ms should keep everything."""
        for i in range(5):
            db.upsert_node(1, f"n{i}")
        result = db.prune(max_age_ms=999_999_999)
        assert result.nodes_pruned == 0

    def test_prune_max_age_selective(self, db):
        """Only nodes older than threshold are pruned."""
        db.upsert_node(1, "old")
        time.sleep(0.05)  # 50ms
        db.upsert_node(1, "new")
        # Prune anything older than 10ms
        result = db.prune(max_age_ms=10)
        assert result.nodes_pruned >= 1
        # "new" should survive
        assert db.get_node_by_key(1, "new") is not None

    def test_prune_max_age_cascades_edges(self, db):
        """Pruning a node by age should also remove its edges."""
        n1 = db.upsert_node(1, "a")
        n2 = db.upsert_node(1, "b")
        eid = db.upsert_edge(n1, n2, 10)
        time.sleep(0.05)
        result = db.prune(max_age_ms=1)
        assert result.nodes_pruned >= 1
        assert result.edges_pruned >= 1
        assert db.get_edge(eid) is None

    def test_prune_max_age_with_type_filter(self, db):
        """max_age_ms combined with type_id only prunes matching type."""
        db.upsert_node(1, "t1")
        db.upsert_node(2, "t2")
        time.sleep(0.05)
        result = db.prune(max_age_ms=1, type_id=1)
        assert result.nodes_pruned == 1
        # Type 2 should survive
        assert db.get_node_by_key(2, "t2") is not None
