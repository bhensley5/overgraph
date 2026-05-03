#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NodeQueryCandidateSourceKind {
    ExplicitIds,
    KeyLookup,
    NodeTypeIndex,
    PropertyEqualityIndex,
    PropertyRangeIndex,
    TimestampIndex,
    FallbackTypeScan,
    FallbackFullNodeScan,
}

#[derive(Clone, Debug, PartialEq)]
enum NormalizedNodeFilter {
    AlwaysTrue,
    AlwaysFalse,
    PropertyEquals {
        key: String,
        value: PropValue,
    },
    PropertyIn {
        key: String,
        values: Vec<PropValue>,
        value_keys: Vec<Vec<u8>>,
    },
    PropertyRange {
        key: String,
        lower: Option<PropertyRangeBound>,
        upper: Option<PropertyRangeBound>,
    },
    PropertyExists {
        key: String,
    },
    PropertyMissing {
        key: String,
    },
    UpdatedAtRange {
        lower_ms: i64,
        upper_ms: i64,
    },
    And(Vec<NormalizedNodeFilter>),
    Or(Vec<NormalizedNodeFilter>),
    Not(Box<NormalizedNodeFilter>),
}

#[derive(Clone, Debug)]
struct NormalizedNodeQuery {
    type_id: Option<u32>,
    ids: Vec<u64>,
    keys: Vec<String>,
    filter: NormalizedNodeFilter,
    allow_full_scan: bool,
    page: PageRequest,
}

#[derive(Clone, Debug)]
struct NormalizedNodePattern {
    alias: String,
    query: NormalizedNodeQuery,
}

#[derive(Clone, Debug)]
struct NormalizedEdgePattern {
    alias: Option<String>,
    from_index: usize,
    to_index: usize,
    direction: Direction,
    type_filter: Option<Vec<u32>>,
    property_predicates: Vec<EdgePostFilterPredicate>,
}

#[derive(Clone, Debug)]
struct NormalizedGraphPatternQuery {
    nodes: Vec<NormalizedNodePattern>,
    edges: Vec<NormalizedEdgePattern>,
    at_epoch: Option<i64>,
    limit: usize,
    order: PatternOrder,
}

fn prop_value_canonical_bytes(value: &PropValue) -> Vec<u8> {
    rmp_serde::to_vec(value).expect("PropValue must be serializable")
}

fn prop_values_equal_for_filter(left: &PropValue, right: &PropValue) -> bool {
    left == right
}

fn push_len_prefixed_bytes(target: &mut Vec<u8>, bytes: &[u8]) {
    target.extend_from_slice(&(bytes.len() as u64).to_be_bytes());
    target.extend_from_slice(bytes);
}

impl NormalizedNodeFilter {
    fn is_always_true(&self) -> bool {
        matches!(self, Self::AlwaysTrue)
    }

    fn is_always_false(&self) -> bool {
        matches!(self, Self::AlwaysFalse)
    }

    fn structural_key(&self) -> Vec<u8> {
        let mut key = Vec::new();
        match self {
            Self::AlwaysTrue => key.push(0),
            Self::AlwaysFalse => key.push(1),
            Self::PropertyEquals { key: prop_key, value } => {
                key.push(2);
                push_len_prefixed_bytes(&mut key, prop_key.as_bytes());
                push_len_prefixed_bytes(&mut key, &prop_value_canonical_bytes(value));
            }
            Self::PropertyIn {
                key: prop_key,
                value_keys,
                ..
            } => {
                key.push(3);
                push_len_prefixed_bytes(&mut key, prop_key.as_bytes());
                for value_key in value_keys {
                    push_len_prefixed_bytes(&mut key, value_key);
                }
            }
            Self::PropertyRange {
                key: prop_key,
                lower,
                upper,
            } => {
                key.push(4);
                push_len_prefixed_bytes(&mut key, prop_key.as_bytes());
                key.extend_from_slice(format!("{lower:?}:{upper:?}").as_bytes());
            }
            Self::PropertyExists { key: prop_key } => {
                key.push(5);
                push_len_prefixed_bytes(&mut key, prop_key.as_bytes());
            }
            Self::PropertyMissing { key: prop_key } => {
                key.push(6);
                push_len_prefixed_bytes(&mut key, prop_key.as_bytes());
            }
            Self::UpdatedAtRange { lower_ms, upper_ms } => {
                key.push(7);
                key.extend_from_slice(&lower_ms.to_be_bytes());
                key.extend_from_slice(&upper_ms.to_be_bytes());
            }
            Self::And(children) => {
                key.push(8);
                for child in children {
                    push_len_prefixed_bytes(&mut key, &child.structural_key());
                }
            }
            Self::Or(children) => {
                key.push(9);
                for child in children {
                    push_len_prefixed_bytes(&mut key, &child.structural_key());
                }
            }
            Self::Not(child) => {
                key.push(10);
                push_len_prefixed_bytes(&mut key, &child.structural_key());
            }
        }
        key
    }
}

fn require_non_empty_filter_key(key: &str, context: &str) -> Result<(), EngineError> {
    if key.is_empty() {
        Err(EngineError::InvalidOperation(format!(
            "{context} property key must be non-empty"
        )))
    } else {
        Ok(())
    }
}

fn filter_children_sorted_dedup(mut children: Vec<NormalizedNodeFilter>) -> Vec<NormalizedNodeFilter> {
    children.sort_by_key(NormalizedNodeFilter::structural_key);
    children.dedup_by(|left, right| left.structural_key() == right.structural_key());
    children
}

fn normalize_normalized_and_filter(
    mut flattened: Vec<NormalizedNodeFilter>,
) -> Result<NormalizedNodeFilter, EngineError> {
    let mut eq_by_key: HashMap<String, PropValue> = HashMap::new();
    let mut exists_keys = HashSet::new();
    let mut missing_keys = HashSet::new();
    for child in &flattened {
        match child {
            NormalizedNodeFilter::PropertyEquals { key, value } => {
                if let Some(existing) = eq_by_key.get(key) {
                    if !prop_values_equal_for_filter(existing, value) {
                        return Ok(NormalizedNodeFilter::AlwaysFalse);
                    }
                } else {
                    eq_by_key.insert(key.clone(), value.clone());
                }
                exists_keys.insert(key.clone());
            }
            NormalizedNodeFilter::PropertyExists { key } => {
                exists_keys.insert(key.clone());
            }
            NormalizedNodeFilter::PropertyMissing { key } => {
                missing_keys.insert(key.clone());
            }
            _ => {}
        }
    }

    if exists_keys.iter().any(|key| missing_keys.contains(key)) {
        return Ok(NormalizedNodeFilter::AlwaysFalse);
    }

    flattened.retain(|child| match child {
        NormalizedNodeFilter::PropertyExists { key } => !eq_by_key.contains_key(key),
        _ => true,
    });

    let flattened = filter_children_sorted_dedup(flattened);
    Ok(match flattened.len() {
        0 => NormalizedNodeFilter::AlwaysTrue,
        1 => flattened.into_iter().next().unwrap(),
        _ => NormalizedNodeFilter::And(flattened),
    })
}

fn normalize_and_filter(
    children: &[NodeFilterExpr],
) -> Result<NormalizedNodeFilter, EngineError> {
    if children.is_empty() {
        return Err(EngineError::InvalidOperation(
            "and filters must contain at least one child".into(),
        ));
    }

    let mut flattened = Vec::new();
    for child in children {
        match normalize_node_filter_expr(child)? {
            NormalizedNodeFilter::AlwaysFalse => return Ok(NormalizedNodeFilter::AlwaysFalse),
            NormalizedNodeFilter::AlwaysTrue => {}
            NormalizedNodeFilter::And(grandchildren) => flattened.extend(grandchildren),
            normalized => flattened.push(normalized),
        }
    }

    normalize_normalized_and_filter(flattened)
}

fn normalize_or_filter(children: &[NodeFilterExpr]) -> Result<NormalizedNodeFilter, EngineError> {
    if children.is_empty() {
        return Err(EngineError::InvalidOperation(
            "or filters must contain at least one child".into(),
        ));
    }

    let mut flattened = Vec::new();
    for child in children {
        match normalize_node_filter_expr(child)? {
            NormalizedNodeFilter::AlwaysTrue => return Ok(NormalizedNodeFilter::AlwaysTrue),
            NormalizedNodeFilter::AlwaysFalse => {}
            NormalizedNodeFilter::Or(grandchildren) => flattened.extend(grandchildren),
            normalized => flattened.push(normalized),
        }
    }

    let flattened = filter_children_sorted_dedup(flattened);
    Ok(match flattened.len() {
        0 => NormalizedNodeFilter::AlwaysFalse,
        1 => flattened.into_iter().next().unwrap(),
        _ => NormalizedNodeFilter::Or(flattened),
    })
}

fn normalize_node_filter_expr(expr: &NodeFilterExpr) -> Result<NormalizedNodeFilter, EngineError> {
    match expr {
        NodeFilterExpr::PropertyEquals { key, value } => {
            require_non_empty_filter_key(key, "property equals filter")?;
            Ok(NormalizedNodeFilter::PropertyEquals {
                key: key.clone(),
                value: value.clone(),
            })
        }
        NodeFilterExpr::PropertyIn { key, values } => {
            require_non_empty_filter_key(key, "property in filter")?;
            if values.is_empty() {
                return Err(EngineError::InvalidOperation(
                    "property in filters must contain at least one value".into(),
                ));
            }
            let mut keyed_values: Vec<(Vec<u8>, PropValue)> = values
                .iter()
                .map(|value| (prop_value_canonical_bytes(value), value.clone()))
                .collect();
            keyed_values.sort_by(|left, right| left.0.cmp(&right.0));
            keyed_values.dedup_by(|left, right| left.0 == right.0);
            if keyed_values.len() == 1 {
                let (_, value) = keyed_values.into_iter().next().unwrap();
                return Ok(NormalizedNodeFilter::PropertyEquals {
                    key: key.clone(),
                    value,
                });
            }
            let (value_keys, deduped_values): (Vec<Vec<u8>>, Vec<PropValue>) =
                keyed_values.into_iter().unzip();
            Ok(NormalizedNodeFilter::PropertyIn {
                key: key.clone(),
                values: deduped_values,
                value_keys,
            })
        }
        NodeFilterExpr::PropertyRange { key, lower, upper } => {
            require_non_empty_filter_key(key, "property range filter")?;
            ReadView::validate_property_range_bounds(lower.as_ref(), upper.as_ref(), None)?;
            Ok(NormalizedNodeFilter::PropertyRange {
                key: key.clone(),
                lower: lower.clone(),
                upper: upper.clone(),
            })
        }
        NodeFilterExpr::PropertyExists { key } => {
            require_non_empty_filter_key(key, "property exists filter")?;
            Ok(NormalizedNodeFilter::PropertyExists { key: key.clone() })
        }
        NodeFilterExpr::PropertyMissing { key } => {
            require_non_empty_filter_key(key, "property missing filter")?;
            Ok(NormalizedNodeFilter::PropertyMissing { key: key.clone() })
        }
        NodeFilterExpr::UpdatedAtRange { lower_ms, upper_ms } => {
            if lower_ms.is_none() && upper_ms.is_none() {
                return Err(EngineError::InvalidOperation(
                    "updated-at range filters require at least one bound".into(),
                ));
            }
            let lower_ms = lower_ms.unwrap_or(i64::MIN);
            let upper_ms = upper_ms.unwrap_or(i64::MAX);
            if lower_ms > upper_ms {
                return Ok(NormalizedNodeFilter::AlwaysFalse);
            }
            Ok(NormalizedNodeFilter::UpdatedAtRange { lower_ms, upper_ms })
        }
        NodeFilterExpr::And(children) => normalize_and_filter(children),
        NodeFilterExpr::Or(children) => normalize_or_filter(children),
        NodeFilterExpr::Not(child) => match normalize_node_filter_expr(child)? {
            NormalizedNodeFilter::AlwaysTrue => Ok(NormalizedNodeFilter::AlwaysFalse),
            NormalizedNodeFilter::AlwaysFalse => Ok(NormalizedNodeFilter::AlwaysTrue),
            NormalizedNodeFilter::PropertyExists { key } => {
                Ok(NormalizedNodeFilter::PropertyMissing { key })
            }
            NormalizedNodeFilter::PropertyMissing { key } => {
                Ok(NormalizedNodeFilter::PropertyExists { key })
            }
            NormalizedNodeFilter::Not(grandchild) => Ok(*grandchild),
            normalized => Ok(NormalizedNodeFilter::Not(Box::new(normalized))),
        },
    }
}

fn normalize_optional_node_filter(
    filter: Option<&NodeFilterExpr>,
) -> Result<NormalizedNodeFilter, EngineError> {
    filter.map(normalize_node_filter_expr).unwrap_or(Ok(NormalizedNodeFilter::AlwaysTrue))
}

impl ReadView {
    fn normalize_node_query_with_anchor_requirement(
        &self,
        query: &NodeQuery,
        require_anchor: bool,
    ) -> Result<NormalizedNodeQuery, EngineError> {
        let mut ids = query.ids.clone();
        ids.sort_unstable();
        ids.dedup();

        let mut keys = query.keys.clone();
        keys.sort();
        keys.dedup();

        if !keys.is_empty() && query.type_id.is_none() {
            return Err(EngineError::InvalidOperation(
                "node query keys require type_id".into(),
            ));
        }

        let filter = normalize_optional_node_filter(query.filter.as_ref())?;

        if require_anchor
            && ids.is_empty()
            && keys.is_empty()
            && query.type_id.is_none()
            && !query.allow_full_scan
            && !filter.is_always_false()
        {
            return Err(EngineError::InvalidOperation(
                "node query requires type_id, ids, keys, or allow_full_scan".into(),
            ));
        }

        Ok(NormalizedNodeQuery {
            type_id: query.type_id,
            ids,
            keys,
            filter,
            allow_full_scan: query.allow_full_scan,
            page: query.page.clone(),
        })
    }

    fn normalize_node_query(&self, query: &NodeQuery) -> Result<NormalizedNodeQuery, EngineError> {
        self.normalize_node_query_with_anchor_requirement(query, true)
    }

    fn normalize_node_pattern(
        &self,
        pattern: &NodePattern,
    ) -> Result<NormalizedNodePattern, EngineError> {
        if pattern.alias.is_empty() {
            return Err(EngineError::InvalidOperation(
                "node pattern aliases must be non-empty".into(),
            ));
        }

        let query = NodeQuery {
            type_id: pattern.type_id,
            ids: pattern.ids.clone(),
            keys: pattern.keys.clone(),
            filter: pattern.filter.clone(),
            page: PageRequest::default(),
            order: NodeQueryOrder::NodeIdAsc,
            allow_full_scan: false,
        };
        Ok(NormalizedNodePattern {
            alias: pattern.alias.clone(),
            query: self.normalize_node_query_with_anchor_requirement(&query, false)?,
        })
    }

    fn normalize_edge_post_filter_predicates(
        &self,
        predicates: &[EdgePostFilterPredicate],
    ) -> Result<Vec<EdgePostFilterPredicate>, EngineError> {
        let mut normalized = Vec::with_capacity(predicates.len());
        for predicate in predicates {
            match predicate {
                EdgePostFilterPredicate::PropertyEquals { .. } => {
                    normalized.push(predicate.clone());
                }
                EdgePostFilterPredicate::PropertyRange { lower, upper, .. } => {
                    Self::validate_property_range_bounds(lower.as_ref(), upper.as_ref(), None)?;
                    normalized.push(predicate.clone());
                }
            }
        }
        Ok(normalized)
    }

    fn normalize_pattern_query(
        &self,
        query: &GraphPatternQuery,
    ) -> Result<NormalizedGraphPatternQuery, EngineError> {
        if query.nodes.is_empty() {
            return Err(EngineError::InvalidOperation(
                "pattern query requires at least one node pattern".into(),
            ));
        }
        if query.edges.is_empty() {
            return Err(EngineError::InvalidOperation(
                "pattern query requires at least one edge pattern".into(),
            ));
        }
        if query.limit == 0 {
            return Err(EngineError::InvalidOperation(
                "pattern query limit must be greater than zero".into(),
            ));
        }

        let mut nodes = Vec::with_capacity(query.nodes.len());
        let mut alias_to_index = HashMap::with_capacity(query.nodes.len());
        for pattern in &query.nodes {
            let normalized = self.normalize_node_pattern(pattern)?;
            if alias_to_index
                .insert(normalized.alias.clone(), nodes.len())
                .is_some()
            {
                return Err(EngineError::InvalidOperation(
                    "node pattern aliases must be unique".into(),
                ));
            }
            nodes.push(normalized);
        }

        let mut edge_aliases = HashSet::new();
        let mut edges = Vec::with_capacity(query.edges.len());
        let mut adjacency = vec![Vec::<usize>::new(); nodes.len()];
        let mut incident_counts = vec![0usize; nodes.len()];
        for pattern in &query.edges {
            if let Some(alias) = pattern.alias.as_ref() {
                if alias.is_empty() {
                    return Err(EngineError::InvalidOperation(
                        "edge pattern aliases must be non-empty".into(),
                    ));
                }
                if !edge_aliases.insert(alias.clone()) {
                    return Err(EngineError::InvalidOperation(
                        "edge pattern aliases must be unique".into(),
                    ));
                }
            }

            let Some(&from_index) = alias_to_index.get(&pattern.from_alias) else {
                return Err(EngineError::InvalidOperation(format!(
                    "edge pattern references unknown from alias '{}'",
                    pattern.from_alias
                )));
            };
            let Some(&to_index) = alias_to_index.get(&pattern.to_alias) else {
                return Err(EngineError::InvalidOperation(format!(
                    "edge pattern references unknown to alias '{}'",
                    pattern.to_alias
                )));
            };

            incident_counts[from_index] += 1;
            if from_index != to_index {
                incident_counts[to_index] += 1;
            }
            adjacency[from_index].push(to_index);
            adjacency[to_index].push(from_index);

            let mut type_filter = pattern.type_filter.clone();
            if let Some(filter) = type_filter.as_mut() {
                filter.sort_unstable();
                filter.dedup();
                if filter.is_empty() {
                    return Err(EngineError::InvalidOperation(
                        "edge pattern type_filter must not be empty".into(),
                    ));
                }
            }

            edges.push(NormalizedEdgePattern {
                alias: pattern.alias.clone(),
                from_index,
                to_index,
                direction: pattern.direction,
                type_filter,
                property_predicates: self
                    .normalize_edge_post_filter_predicates(&pattern.property_predicates)?,
            });
        }

        if incident_counts.contains(&0) {
            return Err(EngineError::InvalidOperation(
                "every pattern node alias must be attached to an edge".into(),
            ));
        }

        let mut visited = vec![false; nodes.len()];
        let mut stack = vec![edges[0].from_index];
        visited[edges[0].from_index] = true;
        while let Some(index) = stack.pop() {
            for &neighbor in &adjacency[index] {
                if !visited[neighbor] {
                    visited[neighbor] = true;
                    stack.push(neighbor);
                }
            }
        }
        if visited.iter().any(|seen| !*seen) {
            return Err(EngineError::InvalidOperation(
                "pattern query must be one connected component".into(),
            ));
        }

        Ok(NormalizedGraphPatternQuery {
            nodes,
            edges,
            at_epoch: query.at_epoch,
            limit: query.limit,
            order: query.order,
        })
    }
}
