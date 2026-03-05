import time

from overgraph import OverGraph


class TestPrune:
    def test_prune_by_weight(self, db):
        db.upsert_node(1, "low", weight=0.1)
        db.upsert_node(1, "high", weight=5.0)
        result = db.prune(max_weight=0.5)
        assert result.nodes_pruned == 1
        assert result.edges_pruned == 0
        assert db.get_node_by_key(1, "low") is None
        assert db.get_node_by_key(1, "high") is not None

    def test_prune_by_type(self, db):
        db.upsert_node(1, "a", weight=0.1)
        db.upsert_node(2, "b", weight=0.1)
        result = db.prune(max_weight=0.5, type_id=1)
        assert result.nodes_pruned == 1
        # Type 2 should survive
        assert db.get_node_by_key(2, "b") is not None

    def test_prune_cascades_edges(self, db):
        n1 = db.upsert_node(1, "a", weight=0.1)
        n2 = db.upsert_node(1, "b", weight=5.0)
        eid = db.upsert_edge(n1, n2, 10)
        result = db.prune(max_weight=0.5)
        assert result.nodes_pruned == 1
        assert result.edges_pruned >= 1
        assert db.get_edge(eid) is None

    def test_prune_no_match(self, db):
        # weight 5.0 > threshold 0.1, so node survives
        db.upsert_node(1, "a", weight=5.0)
        result = db.prune(max_weight=0.1)
        assert result.nodes_pruned == 0

    def test_prune_no_criteria(self, db):
        db.upsert_node(1, "a")
        result = db.prune()  # no criteria = prune nothing
        assert result.nodes_pruned == 0
        assert result.edges_pruned == 0

    def test_prune_by_age(self, db):
        db.upsert_node(1, "old", weight=5.0)
        time.sleep(0.05)  # 50ms
        db.upsert_node(1, "new", weight=5.0)
        result = db.prune(max_age_ms=10)  # Prune anything older than 10ms
        # The "old" node should be pruned (50ms old > 10ms threshold)
        assert result.nodes_pruned >= 1

    def test_prune_result_repr(self, db):
        result = db.prune(max_weight=0.0)
        r = repr(result)
        assert "PruneResult" in r


class TestPrunePolicies:
    def test_set_and_list(self, db):
        db.set_prune_policy("low_weight", max_weight=0.5)
        policies = db.list_prune_policies()
        assert len(policies) == 1
        assert policies[0].name == "low_weight"
        assert abs(policies[0].max_weight - 0.5) < 0.01
        assert "PrunePolicy" in repr(policies[0])

    def test_set_multiple(self, db):
        db.set_prune_policy("p1", max_weight=0.5)
        db.set_prune_policy("p2", max_age_ms=60000)
        policies = db.list_prune_policies()
        assert len(policies) == 2

    def test_remove_policy(self, db):
        db.set_prune_policy("p1", max_weight=0.5)
        removed = db.remove_prune_policy("p1")
        assert removed is True
        assert db.list_prune_policies() == []

    def test_remove_nonexistent(self, db):
        removed = db.remove_prune_policy("nope")
        assert removed is False

    def test_policy_with_type_id(self, db):
        db.set_prune_policy("typed", max_weight=0.5, type_id=1)
        policies = db.list_prune_policies()
        assert policies[0].type_id == 1

    def test_policy_filtering_reads(self, db):
        """Registered policies should filter reads immediately."""
        n1 = db.upsert_node(1, "low", weight=0.1)
        n2 = db.upsert_node(1, "high", weight=5.0)
        # Before policy: both visible
        assert db.get_node(n1) is not None
        assert db.get_node(n2) is not None
        # Set policy
        db.set_prune_policy("low_weight", max_weight=0.5)
        # After policy: low-weight node invisible
        assert db.get_node(n1) is None
        assert db.get_node(n2) is not None
        # Remove policy: visible again
        db.remove_prune_policy("low_weight")
        assert db.get_node(n1) is not None
