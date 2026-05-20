use overgraph::{
    DatabaseEngine, DbOptions, Direction, EdgePattern, EdgeQuery, GraphPatternQuery,
    LabelMatchMode, NodeFilterExpr, NodeLabelFilter, NodePattern, NodeQuery, PatternOrder,
    PropValue, QueryPlanPublicName, QueryPlanWarning, UpsertEdgeOptions, UpsertNodeOptions,
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

    let pattern_plan = engine
        .explain_pattern_query(&GraphPatternQuery {
            nodes: vec![
                NodePattern {
                    alias: "p".to_string(),
                    label_filter: Some(NodeLabelFilter {
                        labels: vec!["Person".to_string()],
                        mode: LabelMatchMode::All,
                    }),
                    ids: Vec::new(),
                    keys: Vec::new(),
                    filter: None,
                },
                NodePattern {
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
            edges: vec![EdgePattern {
                alias: Some("e".to_string()),
                from_alias: "p".to_string(),
                to_alias: "c".to_string(),
                direction: Direction::Outgoing,
                label_filter: vec!["WORKS_AT".to_string(), "MISSING".to_string()],
                filter: None,
            }],
            at_epoch: None,
            limit: 10,
            order: PatternOrder::AnchorThenAliasesAsc,
        })
        .unwrap();
    assert_eq!(
        pattern_plan.public_inputs.node_labels,
        vec![
            QueryPlanPublicName {
                alias: Some("p".to_string()),
                name: "Person".to_string(),
                known: true,
                mode: Some(LabelMatchMode::All),
            },
            QueryPlanPublicName {
                alias: Some("c".to_string()),
                name: "Company".to_string(),
                known: true,
                mode: Some(LabelMatchMode::All),
            },
        ]
    );
    assert_eq!(
        pattern_plan.public_inputs.edge_labels,
        vec![
            QueryPlanPublicName {
                alias: Some("e".to_string()),
                name: "WORKS_AT".to_string(),
                known: true,
                mode: None,
            },
            QueryPlanPublicName {
                alias: Some("e".to_string()),
                name: "MISSING".to_string(),
                known: false,
                mode: None,
            },
        ]
    );
    assert!(pattern_plan
        .warnings
        .contains(&QueryPlanWarning::UnknownEdgeLabel));

    let debug = format!("{:?}", pattern_plan.public_inputs);
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
fn graph_pattern_resolves_named_filters_without_changing_bindings() {
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

    let base = GraphPatternQuery {
        nodes: vec![
            NodePattern {
                alias: "p".to_string(),
                label_filter: Some(NodeLabelFilter {
                    labels: vec!["Person".to_string()],
                    mode: LabelMatchMode::All,
                }),
                ids: Vec::new(),
                keys: Vec::new(),
                filter: None,
            },
            NodePattern {
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
        edges: vec![EdgePattern {
            alias: Some("e".to_string()),
            from_alias: "p".to_string(),
            to_alias: "c".to_string(),
            direction: Direction::Outgoing,
            label_filter: vec!["WORKS_AT".to_string(), "MISSING".to_string()],
            filter: None,
        }],
        at_epoch: None,
        limit: 10,
        order: PatternOrder::AnchorThenAliasesAsc,
    };

    let result = engine.query_pattern(&base).unwrap();
    assert_eq!(result.matches.len(), 1);
    assert_eq!(result.matches[0].nodes["p"], alice);
    assert_eq!(result.matches[0].nodes["c"], acme);
    assert_eq!(result.matches[0].edges["e"], works_at);

    let mut all_unknown = base.clone();
    all_unknown.edges[0].label_filter = vec!["MISSING".to_string()];
    assert!(engine
        .query_pattern(&all_unknown)
        .unwrap()
        .matches
        .is_empty());
    assert!(engine
        .explain_pattern_query(&all_unknown)
        .unwrap()
        .warnings
        .contains(&QueryPlanWarning::UnknownEdgeLabel));

    let mut unknown_node = base;
    unknown_node.nodes[0].label_filter = Some(NodeLabelFilter {
        labels: vec!["Missing".to_string()],
        mode: LabelMatchMode::All,
    });
    assert!(engine
        .query_pattern(&unknown_node)
        .unwrap()
        .matches
        .is_empty());
    assert!(engine
        .explain_pattern_query(&unknown_node)
        .unwrap()
        .warnings
        .contains(&QueryPlanWarning::UnknownNodeLabel));
}
