impl DatabaseEngine {
    pub fn query_node_ids(
        &self,
        query: &NodeQuery,
    ) -> Result<QueryNodeIdsResult, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        let outcome = published.view.query_node_ids_outcome(query)?;
        for followup in outcome.followups {
            self.runtime.enqueue_secondary_index_read_followup(followup);
        }
        Ok(outcome.value)
    }

    pub fn query_nodes(&self, query: &NodeQuery) -> Result<QueryNodesResult, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        let outcome = published.view.query_nodes_outcome(query)?;
        for followup in outcome.followups {
            self.runtime.enqueue_secondary_index_read_followup(followup);
        }
        Ok(outcome.value)
    }

    pub fn explain_node_query(&self, query: &NodeQuery) -> Result<QueryPlan, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        published.view.explain_node_query(query)
    }

    pub fn query_edge_ids(
        &self,
        query: &EdgeQuery,
    ) -> Result<QueryEdgeIdsResult, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        #[cfg(test)]
        published
            .view
            .query_execution_counters
            .public_edge_query_calls
            .fetch_add(1, Ordering::Relaxed);
        let outcome = published.view.query_edge_ids_outcome(query)?;
        for followup in outcome.followups {
            self.runtime.enqueue_secondary_index_read_followup(followup);
        }
        Ok(outcome.value)
    }

    pub fn query_edges(&self, query: &EdgeQuery) -> Result<QueryEdgesResult, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        #[cfg(test)]
        published
            .view
            .query_execution_counters
            .public_edge_query_calls
            .fetch_add(1, Ordering::Relaxed);
        let outcome = published.view.query_edges_outcome(query)?;
        for followup in outcome.followups {
            self.runtime.enqueue_secondary_index_read_followup(followup);
        }
        Ok(outcome.value)
    }

    pub fn explain_edge_query(&self, query: &EdgeQuery) -> Result<QueryPlan, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        published.view.explain_edge_query(query)
    }

    pub fn query_pattern(
        &self,
        query: &GraphPatternQuery,
    ) -> Result<QueryPatternResult, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        let outcome = published.view.query_pattern_outcome(query)?;
        for followup in outcome.followups {
            self.runtime.enqueue_secondary_index_read_followup(followup);
        }
        Ok(outcome.value)
    }

    pub fn explain_pattern_query(&self, query: &GraphPatternQuery) -> Result<QueryPlan, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        published.view.explain_pattern_query(query)
    }
}
