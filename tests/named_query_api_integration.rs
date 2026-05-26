use overgraph::{
    DatabaseEngine, DbOptions, Direction, EdgeQuery, GraphEdgePattern, GraphExpr, GraphNodePattern,
    GraphOutputOptions, GraphPageRequest, GraphPatternPiece, GraphQueryOptions, GraphReturnItem,
    GraphReturnProjection, GraphRowQuery, GraphValue, LabelMatchMode, NodeFilterExpr,
    NodeLabelFilter, NodeQuery, PropValue, QueryPlanPublicName, QueryPlanWarning,
    UpsertEdgeOptions, UpsertNodeOptions,
};
use std::collections::BTreeMap;
use tempfile::TempDir;

fn props(entries: &[(&str, PropValue)]) -> BTreeMap<String, PropValue> {
    entries
        .iter()
        .map(|(key, value)| ((*key).to_string(), value.clone()))
        .collect()
}

fn node_options(entries: &[(&str, PropValue)]) -> UpsertNodeOptions {
    UpsertNodeOptions {
        props: props(entries),
        ..Default::default()
    }
}

fn edge_options(entries: &[(&str, PropValue)]) -> UpsertEdgeOptions {
    UpsertEdgeOptions {
        props: props(entries),
        ..Default::default()
    }
}

#[test]
fn explain_plans_surface_public_names_without_token_ids() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let alice = engine
        .upsert_node("Person", "alice", UpsertNodeOptions::default())
        .unwrap();
    let acme = engine
        .upsert_node("Company", "acme", UpsertNodeOptions::default())
        .unwrap();
    engine
        .upsert_edge(alice, acme, "WORKS_AT", UpsertEdgeOptions::default())
        .unwrap();

    let node_plan = engine
        .explain_node_query(&NodeQuery {
            label_filter: Some(NodeLabelFilter {
                labels: vec!["Person".to_string()],
                mode: LabelMatchMode::All,
            }),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(
        node_plan.public_inputs.node_labels,
        vec![QueryPlanPublicName {
            alias: None,
            name: "Person".to_string(),
            known: true,
            mode: Some(LabelMatchMode::All),
        }]
    );
    assert!(node_plan.public_inputs.edge_labels.is_empty());

    let edge_plan = engine
        .explain_edge_query(&EdgeQuery {
            label: Some("WORKS_AT".to_string()),
            from_ids: vec![alice],
            ..Default::default()
        })
        .unwrap();
    assert!(edge_plan.public_inputs.node_labels.is_empty());
    assert_eq!(
        edge_plan.public_inputs.edge_labels,
        vec![QueryPlanPublicName {
            alias: None,
            name: "WORKS_AT".to_string(),
            known: true,
            mode: None,
        }]
    );

    let debug = format!("{:?}", edge_plan.public_inputs);
    assert!(!debug.contains(concat!("type", "_id")));
    assert!(!debug.contains(concat!("type", "Id")));
}

#[test]
fn planner_queries_use_public_names_and_hydrate_views() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();

    let alice = engine
        .upsert_node(
            "Person",
            "alice",
            node_options(&[("status", PropValue::String("active".to_string()))]),
        )
        .unwrap();
    engine
        .upsert_node(
            "Person",
            "bob",
            node_options(&[("status", PropValue::String("inactive".to_string()))]),
        )
        .unwrap();
    let acme = engine
        .upsert_node("Company", "acme", UpsertNodeOptions::default())
        .unwrap();
    let works_at = engine
        .upsert_edge(
            alice,
            acme,
            "WORKS_AT",
            edge_options(&[("role", PropValue::String("engineer".to_string()))]),
        )
        .unwrap();

    let node_query = NodeQuery {
        label_filter: Some(NodeLabelFilter {
            labels: vec!["Person".to_string()],
            mode: LabelMatchMode::All,
        }),
        filter: Some(NodeFilterExpr::PropertyEquals {
            key: "status".to_string(),
            value: PropValue::String("active".to_string()),
        }),
        ..Default::default()
    };
    assert_eq!(
        engine.query_node_ids(&node_query).unwrap().items,
        vec![alice]
    );
    let nodes = engine.query_nodes(&node_query).unwrap();
    assert_eq!(nodes.items.len(), 1);
    assert_eq!(nodes.items[0].labels.as_slice(), ["Person"]);
    assert_eq!(nodes.items[0].id, alice);

    let edge_query = EdgeQuery {
        label: Some("WORKS_AT".to_string()),
        from_ids: vec![alice],
        ..Default::default()
    };
    assert_eq!(
        engine.query_edge_ids(&edge_query).unwrap().edge_ids,
        vec![works_at]
    );
    let edges = engine.query_edges(&edge_query).unwrap();
    assert_eq!(edges.edges.len(), 1);
    assert_eq!(edges.edges[0].label, "WORKS_AT");
    assert_eq!(edges.edges[0].id, works_at);
}

#[test]
fn planner_unknown_names_are_read_only_empty_constraints() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let alice = engine
        .upsert_node("Person", "alice", UpsertNodeOptions::default())
        .unwrap();
    let bob = engine
        .upsert_node("Person", "bob", UpsertNodeOptions::default())
        .unwrap();
    engine
        .upsert_edge(alice, bob, "KNOWS", UpsertEdgeOptions::default())
        .unwrap();

    let missing_node_query = NodeQuery {
        label_filter: Some(NodeLabelFilter {
            labels: vec!["Missing".to_string()],
            mode: LabelMatchMode::All,
        }),
        ..Default::default()
    };
    assert!(engine
        .query_node_ids(&missing_node_query)
        .unwrap()
        .items
        .is_empty());
    let node_plan = engine.explain_node_query(&missing_node_query).unwrap();
    assert!(node_plan
        .warnings
        .contains(&QueryPlanWarning::UnknownNodeLabel));
    assert_eq!(engine.get_node_label_id("Missing").unwrap(), None);

    let missing_edge_query = EdgeQuery {
        label: Some("MISSING".to_string()),
        from_ids: vec![alice],
        ..Default::default()
    };
    assert!(engine
        .query_edge_ids(&missing_edge_query)
        .unwrap()
        .edge_ids
        .is_empty());
    let edge_plan = engine.explain_edge_query(&missing_edge_query).unwrap();
    assert!(edge_plan
        .warnings
        .contains(&QueryPlanWarning::UnknownEdgeLabel));
    assert_eq!(engine.get_edge_label_id("MISSING").unwrap(), None);

    let invalid = NodeQuery {
        label_filter: Some(NodeLabelFilter {
            labels: vec![" Missing".to_string()],
            mode: LabelMatchMode::All,
        }),
        ids: vec![alice],
        ..Default::default()
    };
    assert!(engine.query_node_ids(&invalid).is_err());
}

#[test]
fn graph_rows_resolve_named_filters_without_changing_bindings() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("db");
    let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let alice = engine
        .upsert_node("Person", "alice", UpsertNodeOptions::default())
        .unwrap();
    let acme = engine
        .upsert_node("Company", "acme", UpsertNodeOptions::default())
        .unwrap();
    let works_at = engine
        .upsert_edge(alice, acme, "WORKS_AT", UpsertEdgeOptions::default())
        .unwrap();

    let mut base = GraphRowQuery {
        nodes: vec![
            GraphNodePattern {
                alias: "p".to_string(),
                label_filter: Some(NodeLabelFilter {
                    labels: vec!["Person".to_string()],
                    mode: LabelMatchMode::All,
                }),
                ids: Vec::new(),
                keys: Vec::new(),
                filter: None,
            },
            GraphNodePattern {
                alias: "c".to_string(),
                label_filter: Some(NodeLabelFilter {
                    labels: vec!["Company".to_string()],
                    mode: LabelMatchMode::All,
                }),
                ids: Vec::new(),
                keys: Vec::new(),
                filter: None,
            },
        ],
        pieces: vec![GraphPatternPiece::Edge(GraphEdgePattern {
            alias: Some("e".to_string()),
            from_alias: "p".to_string(),
            to_alias: "c".to_string(),
            direction: Direction::Outgoing,
            label_filter: vec!["WORKS_AT".to_string(), "MISSING".to_string()],
            filter: None,
        })],
        where_: None,
        return_items: Some(vec![
            GraphReturnItem {
                expr: GraphExpr::Binding("p".to_string()),
                projection: GraphReturnProjection::IdOnly,
                alias: Some("p".to_string()),
            },
            GraphReturnItem {
                expr: GraphExpr::Binding("e".to_string()),
                projection: GraphReturnProjection::IdOnly,
                alias: Some("e".to_string()),
            },
            GraphReturnItem {
                expr: GraphExpr::Binding("c".to_string()),
                projection: GraphReturnProjection::IdOnly,
                alias: Some("c".to_string()),
            },
        ]),
        order_by: Vec::new(),
        page: GraphPageRequest {
            skip: 0,
            limit: 10,
            cursor: None,
        },
        at_epoch: None,
        params: BTreeMap::new(),
        output: GraphOutputOptions::default(),
        options: GraphQueryOptions::default(),
    };
    base.options.allow_full_scan = true;

    let result = engine.query_graph_rows(&base).unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        result.rows[0].values,
        vec![
            GraphValue::NodeId(alice),
            GraphValue::EdgeId(works_at),
            GraphValue::NodeId(acme),
        ]
    );

    let mut all_unknown = base.clone();
    let GraphPatternPiece::Edge(edge) = &mut all_unknown.pieces[0] else {
        unreachable!("test query has one edge piece");
    };
    edge.label_filter = vec!["MISSING".to_string()];
    let result = engine.query_graph_rows(&all_unknown).unwrap();
    assert!(result.rows.is_empty());
    assert!(result
        .stats
        .warnings
        .iter()
        .any(|warning| warning.contains("UnknownEdgeLabel")));

    let mut unknown_node = base;
    unknown_node.nodes[0].label_filter = Some(NodeLabelFilter {
        labels: vec!["Missing".to_string()],
        mode: LabelMatchMode::All,
    });
    let result = engine.query_graph_rows(&unknown_node).unwrap();
    assert!(result.rows.is_empty());
    assert!(result
        .stats
        .warnings
        .iter()
        .any(|warning| warning.contains("UnknownNodeLabel")));
}
