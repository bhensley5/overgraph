#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NodeQueryCandidateSourceKind {
    ExplicitIds,
    KeyLookup,
    NodeLabelIndex,
    PropertyEqualityIndex,
    PropertyRangeIndex,
    TimestampIndex,
    FallbackNodeLabelScan,
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
    single_label_id: Option<u32>,
    label_filter: ResolvedNodeLabelFilter,
    ids: Vec<u64>,
    keys: Vec<String>,
    filter: NormalizedNodeFilter,
    allow_full_scan: bool,
    page: PageRequest,
    warnings: Vec<QueryPlanWarning>,
}

#[derive(Clone, Debug, PartialEq)]
enum NormalizedEdgeFilter {
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
    WeightRange {
        lower: Option<f32>,
        upper: Option<f32>,
    },
    UpdatedAtRange {
        lower_ms: i64,
        upper_ms: i64,
    },
    ValidAt {
        epoch_ms: i64,
    },
    ValidFromRange {
        lower_ms: i64,
        upper_ms: i64,
    },
    ValidToRange {
        lower_ms: i64,
        upper_ms: i64,
    },
    And(Vec<NormalizedEdgeFilter>),
    Or(Vec<NormalizedEdgeFilter>),
    Not(Box<NormalizedEdgeFilter>),
}

#[derive(Clone, Debug)]
struct NormalizedEdgeQuery {
    label_id: Option<u32>,
    ids: Vec<u64>,
    from_ids: Vec<u64>,
    to_ids: Vec<u64>,
    endpoint_ids: Vec<u64>,
    filter: NormalizedEdgeFilter,
    allow_full_scan: bool,
    page: PageRequest,
    warnings: Vec<QueryPlanWarning>,
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
    label_filter_ids: Option<Vec<u32>>,
    filter: NormalizedEdgeFilter,
}

#[derive(Clone, Debug)]
struct NormalizedGraphPatternQuery {
    nodes: Vec<NormalizedNodePattern>,
    edges: Vec<NormalizedEdgePattern>,
    at_epoch: Option<i64>,
    limit: usize,
    order: PatternOrder,
    warnings: Vec<QueryPlanWarning>,
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

impl NormalizedEdgeFilter {
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
            Self::WeightRange { lower, upper } => {
                key.push(7);
                key.extend_from_slice(&lower.map(f32::to_bits).unwrap_or(0).to_be_bytes());
                key.extend_from_slice(&upper.map(f32::to_bits).unwrap_or(0).to_be_bytes());
                key.push(lower.is_some() as u8);
                key.push(upper.is_some() as u8);
            }
            Self::UpdatedAtRange { lower_ms, upper_ms } => {
                key.push(8);
                key.extend_from_slice(&lower_ms.to_be_bytes());
                key.extend_from_slice(&upper_ms.to_be_bytes());
            }
            Self::ValidAt { epoch_ms } => {
                key.push(9);
                key.extend_from_slice(&epoch_ms.to_be_bytes());
            }
            Self::ValidFromRange { lower_ms, upper_ms } => {
                key.push(10);
                key.extend_from_slice(&lower_ms.to_be_bytes());
                key.extend_from_slice(&upper_ms.to_be_bytes());
            }
            Self::ValidToRange { lower_ms, upper_ms } => {
                key.push(11);
                key.extend_from_slice(&lower_ms.to_be_bytes());
                key.extend_from_slice(&upper_ms.to_be_bytes());
            }
            Self::And(children) => {
                key.push(12);
                for child in children {
                    push_len_prefixed_bytes(&mut key, &child.structural_key());
                }
            }
            Self::Or(children) => {
                key.push(13);
                for child in children {
                    push_len_prefixed_bytes(&mut key, &child.structural_key());
                }
            }
            Self::Not(child) => {
                key.push(14);
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

fn edge_filter_children_sorted_dedup(mut children: Vec<NormalizedEdgeFilter>) -> Vec<NormalizedEdgeFilter> {
    children.sort_by_key(NormalizedEdgeFilter::structural_key);
    children.dedup_by(|left, right| left.structural_key() == right.structural_key());
    children
}

fn normalize_normalized_edge_and_filter(
    mut flattened: Vec<NormalizedEdgeFilter>,
) -> Result<NormalizedEdgeFilter, EngineError> {
    let mut eq_by_key: HashMap<String, PropValue> = HashMap::new();
    let mut exists_keys = HashSet::new();
    let mut missing_keys = HashSet::new();
    for child in &flattened {
        match child {
            NormalizedEdgeFilter::PropertyEquals { key, value } => {
                if let Some(existing) = eq_by_key.get(key) {
                    if !prop_values_equal_for_filter(existing, value) {
                        return Ok(NormalizedEdgeFilter::AlwaysFalse);
                    }
                } else {
                    eq_by_key.insert(key.clone(), value.clone());
                }
                exists_keys.insert(key.clone());
            }
            NormalizedEdgeFilter::PropertyIn { key, .. }
            | NormalizedEdgeFilter::PropertyRange { key, .. }
            | NormalizedEdgeFilter::PropertyExists { key } => {
                exists_keys.insert(key.clone());
            }
            NormalizedEdgeFilter::PropertyMissing { key } => {
                missing_keys.insert(key.clone());
            }
            _ => {}
        }
    }

    if exists_keys.iter().any(|key| missing_keys.contains(key)) {
        return Ok(NormalizedEdgeFilter::AlwaysFalse);
    }

    flattened.retain(|child| match child {
        NormalizedEdgeFilter::PropertyExists { key } => !eq_by_key.contains_key(key),
        _ => true,
    });

    let flattened = edge_filter_children_sorted_dedup(flattened);
    Ok(match flattened.len() {
        0 => NormalizedEdgeFilter::AlwaysTrue,
        1 => flattened.into_iter().next().unwrap(),
        _ => NormalizedEdgeFilter::And(flattened),
    })
}

fn normalize_edge_and_filter(
    children: &[EdgeFilterExpr],
) -> Result<NormalizedEdgeFilter, EngineError> {
    if children.is_empty() {
        return Err(EngineError::InvalidOperation(
            "edge and filters must contain at least one child".into(),
        ));
    }

    let mut flattened = Vec::new();
    for child in children {
        match normalize_edge_filter_expr(child)? {
            NormalizedEdgeFilter::AlwaysFalse => return Ok(NormalizedEdgeFilter::AlwaysFalse),
            NormalizedEdgeFilter::AlwaysTrue => {}
            NormalizedEdgeFilter::And(grandchildren) => flattened.extend(grandchildren),
            normalized => flattened.push(normalized),
        }
    }

    normalize_normalized_edge_and_filter(flattened)
}

fn normalize_edge_or_filter(children: &[EdgeFilterExpr]) -> Result<NormalizedEdgeFilter, EngineError> {
    if children.is_empty() {
        return Err(EngineError::InvalidOperation(
            "edge or filters must contain at least one child".into(),
        ));
    }

    let mut flattened = Vec::new();
    for child in children {
        match normalize_edge_filter_expr(child)? {
            NormalizedEdgeFilter::AlwaysTrue => return Ok(NormalizedEdgeFilter::AlwaysTrue),
            NormalizedEdgeFilter::AlwaysFalse => {}
            NormalizedEdgeFilter::Or(grandchildren) => flattened.extend(grandchildren),
            normalized => flattened.push(normalized),
        }
    }

    let flattened = edge_filter_children_sorted_dedup(flattened);
    Ok(match flattened.len() {
        0 => NormalizedEdgeFilter::AlwaysFalse,
        1 => flattened.into_iter().next().unwrap(),
        _ => NormalizedEdgeFilter::Or(flattened),
    })
}

fn normalize_i64_range(
    lower: Option<i64>,
    upper: Option<i64>,
    context: &str,
) -> Result<Option<(i64, i64)>, EngineError> {
    if lower.is_none() && upper.is_none() {
        return Err(EngineError::InvalidOperation(format!(
            "{context} range filters require at least one bound"
        )));
    }
    let lower = lower.unwrap_or(i64::MIN);
    let upper = upper.unwrap_or(i64::MAX);
    if lower > upper {
        Ok(None)
    } else {
        Ok(Some((lower, upper)))
    }
}

fn normalize_weight_range(
    lower: Option<f32>,
    upper: Option<f32>,
) -> Result<NormalizedEdgeFilter, EngineError> {
    if lower.is_none() && upper.is_none() {
        return Err(EngineError::InvalidOperation(
            "edge weight range filters require at least one bound".into(),
        ));
    }
    if lower.is_some_and(f32::is_nan) || upper.is_some_and(f32::is_nan) {
        return Err(EngineError::InvalidOperation(
            "edge weight range bounds must not be NaN".into(),
        ));
    }
    if let (Some(lower), Some(upper)) = (lower, upper) {
        if lower > upper {
            return Ok(NormalizedEdgeFilter::AlwaysFalse);
        }
    }
    Ok(NormalizedEdgeFilter::WeightRange { lower, upper })
}

fn normalize_edge_filter_expr(expr: &EdgeFilterExpr) -> Result<NormalizedEdgeFilter, EngineError> {
    match expr {
        EdgeFilterExpr::PropertyEquals { key, value } => {
            require_non_empty_filter_key(key, "edge property equals filter")?;
            Ok(NormalizedEdgeFilter::PropertyEquals {
                key: key.clone(),
                value: value.clone(),
            })
        }
        EdgeFilterExpr::PropertyIn { key, values } => {
            require_non_empty_filter_key(key, "edge property in filter")?;
            if values.is_empty() {
                return Ok(NormalizedEdgeFilter::AlwaysFalse);
            }
            let mut keyed_values: Vec<(Vec<u8>, PropValue)> = values
                .iter()
                .map(|value| (prop_value_canonical_bytes(value), value.clone()))
                .collect();
            keyed_values.sort_by(|left, right| left.0.cmp(&right.0));
            keyed_values.dedup_by(|left, right| left.0 == right.0);
            if keyed_values.len() == 1 {
                let (_, value) = keyed_values.into_iter().next().unwrap();
                return Ok(NormalizedEdgeFilter::PropertyEquals {
                    key: key.clone(),
                    value,
                });
            }
            let (value_keys, deduped_values): (Vec<Vec<u8>>, Vec<PropValue>) =
                keyed_values.into_iter().unzip();
            Ok(NormalizedEdgeFilter::PropertyIn {
                key: key.clone(),
                values: deduped_values,
                value_keys,
            })
        }
        EdgeFilterExpr::PropertyRange { key, lower, upper } => {
            require_non_empty_filter_key(key, "edge property range filter")?;
            ReadView::validate_property_range_bounds(lower.as_ref(), upper.as_ref(), None)?;
            Ok(NormalizedEdgeFilter::PropertyRange {
                key: key.clone(),
                lower: lower.clone(),
                upper: upper.clone(),
            })
        }
        EdgeFilterExpr::PropertyExists { key } => {
            require_non_empty_filter_key(key, "edge property exists filter")?;
            Ok(NormalizedEdgeFilter::PropertyExists { key: key.clone() })
        }
        EdgeFilterExpr::PropertyMissing { key } => {
            require_non_empty_filter_key(key, "edge property missing filter")?;
            Ok(NormalizedEdgeFilter::PropertyMissing { key: key.clone() })
        }
        EdgeFilterExpr::WeightRange { lower, upper } => normalize_weight_range(*lower, *upper),
        EdgeFilterExpr::UpdatedAtRange { lower_ms, upper_ms } => {
            let Some((lower_ms, upper_ms)) =
                normalize_i64_range(*lower_ms, *upper_ms, "edge updated-at")?
            else {
                return Ok(NormalizedEdgeFilter::AlwaysFalse);
            };
            Ok(NormalizedEdgeFilter::UpdatedAtRange { lower_ms, upper_ms })
        }
        EdgeFilterExpr::ValidAt { epoch_ms } => {
            Ok(NormalizedEdgeFilter::ValidAt { epoch_ms: *epoch_ms })
        }
        EdgeFilterExpr::ValidFromRange { lower_ms, upper_ms } => {
            let Some((lower_ms, upper_ms)) =
                normalize_i64_range(*lower_ms, *upper_ms, "edge valid-from")?
            else {
                return Ok(NormalizedEdgeFilter::AlwaysFalse);
            };
            Ok(NormalizedEdgeFilter::ValidFromRange { lower_ms, upper_ms })
        }
        EdgeFilterExpr::ValidToRange { lower_ms, upper_ms } => {
            let Some((lower_ms, upper_ms)) =
                normalize_i64_range(*lower_ms, *upper_ms, "edge valid-to")?
            else {
                return Ok(NormalizedEdgeFilter::AlwaysFalse);
            };
            Ok(NormalizedEdgeFilter::ValidToRange { lower_ms, upper_ms })
        }
        EdgeFilterExpr::And(children) => normalize_edge_and_filter(children),
        EdgeFilterExpr::Or(children) => normalize_edge_or_filter(children),
        EdgeFilterExpr::Not(child) => match normalize_edge_filter_expr(child)? {
            NormalizedEdgeFilter::AlwaysTrue => Ok(NormalizedEdgeFilter::AlwaysFalse),
            NormalizedEdgeFilter::AlwaysFalse => Ok(NormalizedEdgeFilter::AlwaysTrue),
            NormalizedEdgeFilter::PropertyExists { key } => {
                Ok(NormalizedEdgeFilter::PropertyMissing { key })
            }
            NormalizedEdgeFilter::PropertyMissing { key } => {
                Ok(NormalizedEdgeFilter::PropertyExists { key })
            }
            NormalizedEdgeFilter::Not(grandchild) => Ok(*grandchild),
            normalized => Ok(NormalizedEdgeFilter::Not(Box::new(normalized))),
        },
    }
}

fn normalize_optional_edge_filter(
    filter: Option<&EdgeFilterExpr>,
) -> Result<NormalizedEdgeFilter, EngineError> {
    filter.map(normalize_edge_filter_expr).unwrap_or(Ok(NormalizedEdgeFilter::AlwaysTrue))
}

fn sorted_dedup_u64(mut values: Vec<u64>) -> Vec<u64> {
    values.sort_unstable();
    values.dedup();
    values
}

fn push_query_warning(warnings: &mut Vec<QueryPlanWarning>, warning: QueryPlanWarning) {
    if !warnings.contains(&warning) {
        warnings.push(warning);
    }
}

fn unknown_node_label_count(filter: &ResolvedNodeLabelFilter) -> usize {
    match filter {
        ResolvedNodeLabelFilter::Unconstrained => 0,
        ResolvedNodeLabelFilter::Empty {
            unknown_label_count,
            ..
        }
        | ResolvedNodeLabelFilter::LabelSet {
            unknown_label_count,
            ..
        } => *unknown_label_count,
    }
}

fn single_resolved_label_id(filter: &ResolvedNodeLabelFilter) -> Option<u32> {
    match filter {
        ResolvedNodeLabelFilter::LabelSet { label_ids, .. } if label_ids.len() == 1 => {
            Some(label_ids.single_label_id())
        }
        _ => None,
    }
}

fn normalized_query_has_label_anchor(query: &NormalizedNodeQuery) -> bool {
    matches!(
        query.label_filter,
        ResolvedNodeLabelFilter::LabelSet { .. } | ResolvedNodeLabelFilter::Empty { .. }
    )
}

impl ReadView {
    fn resolve_node_query_label_filter(
        &self,
        label_filter: Option<&NodeLabelFilter>,
    ) -> Result<(ResolvedNodeLabelFilter, Option<u32>, Vec<QueryPlanWarning>), EngineError> {
        let resolved = self
            .label_catalog
            .resolve_node_label_filter_request(label_filter)?;
        let mut warnings = Vec::new();
        if unknown_node_label_count(&resolved) > 0 {
            push_query_warning(&mut warnings, QueryPlanWarning::UnknownNodeLabel);
        }
        let single_label_id = single_resolved_label_id(&resolved);
        Ok((resolved, single_label_id, warnings))
    }

    fn resolve_node_pattern_label_filter(
        &self,
        pattern: &NodePattern,
    ) -> Result<(ResolvedNodeLabelFilter, Option<u32>, Vec<QueryPlanWarning>), EngineError> {
        let resolved = self
            .label_catalog
            .resolve_node_label_filter_request(pattern.label_filter.as_ref())?;
        let mut warnings = Vec::new();
        if unknown_node_label_count(&resolved) > 0 {
            push_query_warning(&mut warnings, QueryPlanWarning::UnknownNodeLabel);
        }
        let single_label_id = single_resolved_label_id(&resolved);
        Ok((resolved, single_label_id, warnings))
    }

    fn normalize_edge_query_with_anchor_requirement(
        &self,
        query: &EdgeQuery,
        require_anchor: bool,
    ) -> Result<NormalizedEdgeQuery, EngineError> {
        let mut warnings = Vec::new();
        let mut label_id = None;
        let mut filter = normalize_optional_edge_filter(query.filter.as_ref())?;
        if let Some(label) = query.label.as_deref() {
            match self.label_catalog.resolve_edge_label_for_read(label)? {
                Some(resolved) => label_id = Some(resolved),
                None => {
                    filter = NormalizedEdgeFilter::AlwaysFalse;
                    push_query_warning(&mut warnings, QueryPlanWarning::UnknownEdgeLabel);
                }
            }
        }
        let ids = sorted_dedup_u64(query.ids.clone());
        let from_ids = sorted_dedup_u64(query.from_ids.clone());
        let to_ids = sorted_dedup_u64(query.to_ids.clone());
        let endpoint_ids = sorted_dedup_u64(query.endpoint_ids.clone());

        if require_anchor
            && label_id.is_none()
            && ids.is_empty()
            && from_ids.is_empty()
            && to_ids.is_empty()
            && endpoint_ids.is_empty()
            && !query.allow_full_scan
            && !filter.is_always_false()
        {
            return Err(EngineError::InvalidOperation(
                "edge query requires label, ids, from_ids, to_ids, endpoint_ids, or allow_full_scan".into(),
            ));
        }

        Ok(NormalizedEdgeQuery {
            label_id,
            ids,
            from_ids,
            to_ids,
            endpoint_ids,
            filter,
            allow_full_scan: query.allow_full_scan,
            page: query.page.clone(),
            warnings,
        })
    }

    fn normalize_edge_query(&self, query: &EdgeQuery) -> Result<NormalizedEdgeQuery, EngineError> {
        self.normalize_edge_query_with_anchor_requirement(query, true)
    }

    fn normalize_node_query_with_anchor_requirement(
        &self,
        query: &NodeQuery,
        require_anchor: bool,
    ) -> Result<NormalizedNodeQuery, EngineError> {
        let (label_filter, single_label_id, warnings) = self.resolve_node_query_label_filter(
            query.label_filter.as_ref(),
        )?;
        let mut filter = normalize_optional_node_filter(query.filter.as_ref())?;
        if label_filter.is_empty_constraint() {
            filter = NormalizedNodeFilter::AlwaysFalse;
        }
        let mut ids = query.ids.clone();
        ids.sort_unstable();
        ids.dedup();

        let mut keys = query.keys.clone();
        keys.sort();
        keys.dedup();

        if !keys.is_empty() {
            match label_filter {
                ResolvedNodeLabelFilter::LabelSet { label_ids, .. } if label_ids.len() == 1 => {}
                ResolvedNodeLabelFilter::Empty { .. } => {}
                ResolvedNodeLabelFilter::Unconstrained => {
                    return Err(EngineError::InvalidOperation(
                        "node query keys require exactly one resolved label".into(),
                    ));
                }
                ResolvedNodeLabelFilter::LabelSet { .. } => {
                    return Err(EngineError::InvalidOperation(
                        "node query keys require exactly one resolved label".into(),
                    ));
                }
            }
        }

        let has_label_anchor = matches!(
            label_filter,
            ResolvedNodeLabelFilter::LabelSet { .. } | ResolvedNodeLabelFilter::Empty { .. }
        );
        if require_anchor
            && ids.is_empty()
            && keys.is_empty()
            && !has_label_anchor
            && !query.allow_full_scan
            && !filter.is_always_false()
        {
            return Err(EngineError::InvalidOperation(
                "node query requires label_filter, ids, keys, or allow_full_scan".into(),
            ));
        }

        Ok(NormalizedNodeQuery {
            single_label_id,
            label_filter,
            ids,
            keys,
            filter,
            allow_full_scan: query.allow_full_scan,
            page: query.page.clone(),
            warnings,
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

        let (label_filter, single_label_id, warnings) =
            self.resolve_node_pattern_label_filter(pattern)?;
        let mut filter = normalize_optional_node_filter(pattern.filter.as_ref())?;
        if label_filter.is_empty_constraint() {
            filter = NormalizedNodeFilter::AlwaysFalse;
        }
        if !pattern.keys.is_empty() {
            match label_filter {
                ResolvedNodeLabelFilter::LabelSet { label_ids, .. } if label_ids.len() == 1 => {}
                ResolvedNodeLabelFilter::Empty { .. } => {}
                ResolvedNodeLabelFilter::Unconstrained => {
                    return Err(EngineError::InvalidOperation(
                        "node pattern keys require exactly one resolved label".into(),
                    ));
                }
                ResolvedNodeLabelFilter::LabelSet { .. } => {
                    return Err(EngineError::InvalidOperation(
                        "node pattern keys require exactly one resolved label".into(),
                    ));
                }
            }
        }
        let mut ids = pattern.ids.clone();
        ids.sort_unstable();
        ids.dedup();
        let mut keys = pattern.keys.clone();
        keys.sort();
        keys.dedup();
        Ok(NormalizedNodePattern {
            alias: pattern.alias.clone(),
            query: NormalizedNodeQuery {
                single_label_id,
                label_filter,
                ids,
                keys,
                filter,
                allow_full_scan: false,
                page: PageRequest::default(),
                warnings,
            },
        })
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

        let mut query_warnings = Vec::new();

        let mut nodes = Vec::with_capacity(query.nodes.len());
        let mut alias_to_index = HashMap::with_capacity(query.nodes.len());
        for pattern in &query.nodes {
            let normalized = self.normalize_node_pattern(pattern)?;
            for warning in &normalized.query.warnings {
                push_query_warning(&mut query_warnings, *warning);
            }
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

            let (label_filter_ids, mut warnings) =
                match self
                    .label_catalog
                    .resolve_edge_label_filter(Some(&pattern.label_filter))?
                {
                    (LabelFilterResolution::Unconstrained, warnings) => (None, warnings),
                    (LabelFilterResolution::Known(label_ids), warnings) => (Some(label_ids), warnings),
                    (LabelFilterResolution::EmptyConstraint, warnings) => {
                        (Some(Vec::new()), warnings)
                    }
                };
            for warning in warnings.drain(..) {
                push_query_warning(&mut query_warnings, warning);
            }

            let filter = normalize_optional_edge_filter(pattern.filter.as_ref())?;

            edges.push(NormalizedEdgePattern {
                alias: pattern.alias.clone(),
                from_index,
                to_index,
                direction: pattern.direction,
                label_filter_ids,
                filter,
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
            warnings: query_warnings,
        })
    }
}
