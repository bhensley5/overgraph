use overgraph::{
    AllShortestPathsOptions, ComponentOptions, DatabaseEngine, DbOptions, DegreeOptions,
    DenseMetric, DenseVectorConfig, Direction, ExportOptions, HnswConfig, IsConnectedOptions,
    LabelMatchMode, NeighborOptions, NodeLabelFilter, PageRequest, PprOptions, PropValue,
    PrunePolicy, ShortestPathOptions, SubgraphOptions, TopKOptions, TraverseOptions,
    UpsertEdgeOptions, UpsertNodeOptions, VectorSearchMode, VectorSearchRequest, VectorSearchScope,
};
use std::collections::{BTreeMap, HashSet};
use std::path::Path;
use tempfile::TempDir;

#[derive(Debug, Clone, Copy)]
struct GraphIds {
    alice: u64,
    bob: u64,
    carol: u64,
    acme: u64,
    doc: u64,
}

fn node_options(weight: f32, sparse_vector: Option<Vec<(u32, f32)>>) -> UpsertNodeOptions {
    let mut props = BTreeMap::new();
    props.insert("weight".to_string(), PropValue::Float(weight as f64));
    UpsertNodeOptions {
        props,
        weight,
        sparse_vector,
        ..Default::default()
    }
}

fn weighted_edge(weight: f32) -> UpsertEdgeOptions {
    UpsertEdgeOptions {
        weight,
        ..Default::default()
    }
}

fn named_node_label_filter(labels: &[&str], mode: LabelMatchMode) -> NodeLabelFilter {
    NodeLabelFilter {
        labels: labels.iter().map(|label| (*label).to_string()).collect(),
        mode,
    }
}

fn open_graph(path: &Path) -> (DatabaseEngine, GraphIds) {
    let engine = DatabaseEngine::open(path, &DbOptions::default()).unwrap();
    let alice = engine
        .upsert_node("Person", "alice", node_options(1.0, Some(vec![(1, 1.0)])))
        .unwrap();
    let bob = engine
        .upsert_node("Person", "bob", node_options(1.0, Some(vec![(1, 0.9)])))
        .unwrap();
    let carol = engine
        .upsert_node("Person", "carol", node_options(1.0, Some(vec![(2, 1.0)])))
        .unwrap();
    let acme = engine
        .upsert_node("Company", "acme", node_options(1.0, Some(vec![(1, 0.7)])))
        .unwrap();
    let doc = engine
        .upsert_node("Document", "doc", node_options(1.0, Some(vec![(1, 0.8)])))
        .unwrap();

    engine
        .upsert_edge(alice, bob, "KNOWS", weighted_edge(3.0))
        .unwrap();
    engine
        .upsert_edge(bob, carol, "KNOWS", weighted_edge(2.0))
        .unwrap();
    engine
        .upsert_edge(alice, acme, "WORKS_AT", weighted_edge(5.0))
        .unwrap();
    let deleted = engine
        .upsert_edge(alice, doc, "KNOWS", weighted_edge(7.0))
        .unwrap();
    engine.delete_edge(deleted).unwrap();

    (
        engine,
        GraphIds {
            alice,
            bob,
            carol,
            acme,
            doc,
        },
    )
}

fn ids<T>(items: impl IntoIterator<Item = T>, f: impl Fn(T) -> u64) -> HashSet<u64> {
    items.into_iter().map(f).collect()
}

fn hit_ids(hits: &[(u64, f64)]) -> HashSet<u64> {
    hits.iter().map(|(id, _)| *id).collect()
}

#[test]
fn named_edge_filters_cover_neighbors_degrees_paths_and_pagination() {
    let dir = TempDir::new().unwrap();
    let (engine, graph) = open_graph(&dir.path().join("db"));

    let known = Some(vec!["KNOWS".to_string()]);
    let mixed = Some(vec!["MISSING_EDGE".to_string(), "KNOWS".to_string()]);
    let all_unknown = Some(vec!["MISSING_EDGE".to_string()]);
    let empty = Some(Vec::new());

    let known_neighbors = engine
        .neighbors(
            graph.alice,
            &NeighborOptions {
                edge_label_filter: known.clone(),
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(known_neighbors.len(), 1);
    assert_eq!(known_neighbors[0].node_id, graph.bob);
    assert_eq!(known_neighbors[0].label, "KNOWS");

    let mixed_neighbors = engine
        .neighbors(
            graph.alice,
            &NeighborOptions {
                edge_label_filter: mixed.clone(),
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(mixed_neighbors, known_neighbors);

    assert!(engine
        .neighbors(
            graph.alice,
            &NeighborOptions {
                edge_label_filter: all_unknown.clone(),
                ..Default::default()
            },
        )
        .unwrap()
        .is_empty());

    let unconstrained_neighbors = engine
        .neighbors(
            graph.alice,
            &NeighborOptions {
                edge_label_filter: empty.clone(),
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(
        ids(&unconstrained_neighbors, |entry| entry.node_id),
        HashSet::from([graph.bob, graph.acme])
    );

    let first_page = engine
        .neighbors_paged(
            graph.alice,
            &NeighborOptions {
                edge_label_filter: empty.clone(),
                ..Default::default()
            },
            &PageRequest {
                limit: Some(1),
                after: None,
            },
        )
        .unwrap();
    assert_eq!(first_page.items.len(), 1);
    assert!(first_page.next_cursor.is_some());
    let second_page = engine
        .neighbors_paged(
            graph.alice,
            &NeighborOptions {
                edge_label_filter: empty.clone(),
                ..Default::default()
            },
            &PageRequest {
                limit: Some(10),
                after: first_page.next_cursor,
            },
        )
        .unwrap();
    assert_eq!(second_page.items.len(), 1);

    let top = engine
        .top_k_neighbors(
            graph.alice,
            1,
            &TopKOptions {
                edge_label_filter: known.clone(),
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(top.len(), 1);
    assert_eq!(top[0].label, "KNOWS");

    let known_degree = DegreeOptions {
        edge_label_filter: known.clone(),
        ..Default::default()
    };
    let mixed_degree = DegreeOptions {
        edge_label_filter: mixed.clone(),
        ..Default::default()
    };
    let unknown_degree = DegreeOptions {
        edge_label_filter: all_unknown.clone(),
        ..Default::default()
    };
    let empty_degree = DegreeOptions {
        edge_label_filter: empty.clone(),
        ..Default::default()
    };
    assert_eq!(engine.degree(graph.alice, &known_degree).unwrap(), 1);
    assert_eq!(engine.degree(graph.alice, &mixed_degree).unwrap(), 1);
    assert_eq!(engine.degree(graph.alice, &unknown_degree).unwrap(), 0);
    assert_eq!(engine.degree(graph.alice, &empty_degree).unwrap(), 2);
    assert_eq!(
        engine.sum_edge_weights(graph.alice, &known_degree).unwrap(),
        3.0
    );
    assert_eq!(
        engine.avg_edge_weight(graph.alice, &known_degree).unwrap(),
        Some(3.0)
    );
    assert_eq!(
        engine
            .degrees(&[graph.alice, graph.bob], &known_degree)
            .unwrap()
            .get(&graph.alice)
            .copied(),
        Some(1)
    );
    assert!(engine
        .degrees(&[graph.alice, graph.bob], &unknown_degree)
        .unwrap()
        .is_empty());

    let path = engine
        .shortest_path(
            graph.alice,
            graph.carol,
            &ShortestPathOptions {
                edge_label_filter: known.clone(),
                ..Default::default()
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(path.nodes, vec![graph.alice, graph.bob, graph.carol]);
    assert!(engine
        .shortest_path(
            graph.alice,
            graph.carol,
            &ShortestPathOptions {
                edge_label_filter: all_unknown.clone(),
                ..Default::default()
            },
        )
        .unwrap()
        .is_none());
    assert!(engine
        .is_connected(
            graph.alice,
            graph.carol,
            &IsConnectedOptions {
                edge_label_filter: mixed.clone(),
                ..Default::default()
            },
        )
        .unwrap());
    assert!(!engine
        .is_connected(
            graph.alice,
            graph.carol,
            &IsConnectedOptions {
                edge_label_filter: all_unknown.clone(),
                ..Default::default()
            },
        )
        .unwrap());
    assert_eq!(
        engine
            .all_shortest_paths(
                graph.alice,
                graph.carol,
                &AllShortestPathsOptions {
                    edge_label_filter: mixed.clone(),
                    ..Default::default()
                },
            )
            .unwrap()
            .len(),
        1
    );
    assert!(engine
        .all_shortest_paths(
            graph.alice,
            graph.carol,
            &AllShortestPathsOptions {
                edge_label_filter: all_unknown.clone(),
                ..Default::default()
            },
        )
        .unwrap()
        .is_empty());

    let zero_hop = engine
        .shortest_path(
            graph.alice,
            graph.alice,
            &ShortestPathOptions {
                edge_label_filter: all_unknown.clone(),
                ..Default::default()
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(zero_hop.nodes, vec![graph.alice]);
    assert!(zero_hop.edges.is_empty());
    assert_eq!(zero_hop.total_cost, 0.0);
    let all_zero_hop = engine
        .all_shortest_paths(
            graph.alice,
            graph.alice,
            &AllShortestPathsOptions {
                edge_label_filter: all_unknown.clone(),
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(all_zero_hop.len(), 1);
    assert_eq!(all_zero_hop[0].nodes, vec![graph.alice]);
    assert!(all_zero_hop[0].edges.is_empty());
    assert!(engine
        .is_connected(
            graph.alice,
            graph.alice,
            &IsConnectedOptions {
                edge_label_filter: all_unknown.clone(),
                ..Default::default()
            },
        )
        .unwrap());

    let traversal = engine
        .traverse(
            graph.alice,
            2,
            &TraverseOptions {
                edge_label_filter: mixed.clone(),
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(
        ids(&traversal.items, |hit| hit.node_id),
        HashSet::from([graph.bob, graph.carol])
    );
    assert!(engine
        .traverse(
            graph.alice,
            2,
            &TraverseOptions {
                edge_label_filter: all_unknown.clone(),
                ..Default::default()
            },
        )
        .unwrap()
        .items
        .is_empty());
    let start_only = engine
        .traverse(
            graph.alice,
            2,
            &TraverseOptions {
                min_depth: 0,
                edge_label_filter: all_unknown.clone(),
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(start_only.items.len(), 1);
    assert_eq!(start_only.items[0].node_id, graph.alice);
    assert_eq!(start_only.items[0].depth, 0);
    assert_eq!(start_only.items[0].via_edge_id, None);
    let emitted_people = engine
        .traverse(
            graph.alice,
            1,
            &TraverseOptions {
                edge_label_filter: empty,
                emit_node_label_filter: Some(named_node_label_filter(
                    &["Person"],
                    LabelMatchMode::Any,
                )),
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(
        ids(&emitted_people.items, |hit| hit.node_id),
        HashSet::from([graph.bob])
    );

    assert!(engine
        .neighbors(
            graph.alice,
            &NeighborOptions {
                edge_label_filter: Some(vec!["".to_string()]),
                ..Default::default()
            },
        )
        .is_err());
    assert_eq!(engine.get_edge_label_id("MISSING_EDGE").unwrap(), None);
    assert_eq!(engine.get_node_label_id("MissingLabel").unwrap(), None);

    engine.close().unwrap();
}

#[test]
fn named_filters_cover_components_ppr_subgraph_export_and_prune() {
    let dir = TempDir::new().unwrap();
    let (engine, graph) = open_graph(&dir.path().join("db"));

    let knows = Some(vec!["KNOWS".to_string()]);
    let mixed_knows = Some(vec!["KNOWS".to_string(), "MISSING_EDGE".to_string()]);
    let missing_edge = Some(vec!["MISSING_EDGE".to_string()]);

    let known_components = engine
        .connected_components(&ComponentOptions {
            edge_label_filter: mixed_knows.clone(),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(known_components[&graph.alice], known_components[&graph.bob]);
    assert_eq!(known_components[&graph.bob], known_components[&graph.carol]);
    assert_eq!(known_components[&graph.acme], graph.acme);

    let singleton_components = engine
        .connected_components(&ComponentOptions {
            edge_label_filter: missing_edge.clone(),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(singleton_components[&graph.alice], graph.alice);
    assert_eq!(singleton_components[&graph.bob], graph.bob);

    let person_components = engine
        .connected_components(&ComponentOptions {
            node_label_filter: Some(named_node_label_filter(&["Person"], LabelMatchMode::Any)),
            ..Default::default()
        })
        .unwrap();
    assert!(person_components.contains_key(&graph.alice));
    assert!(!person_components.contains_key(&graph.acme));
    assert!(engine
        .connected_components(&ComponentOptions {
            node_label_filter: Some(named_node_label_filter(
                &["MissingLabel"],
                LabelMatchMode::Any,
            )),
            ..Default::default()
        })
        .unwrap()
        .is_empty());

    assert_eq!(
        engine
            .component_of(
                graph.alice,
                &ComponentOptions {
                    edge_label_filter: knows.clone(),
                    ..Default::default()
                },
            )
            .unwrap(),
        vec![graph.alice, graph.bob, graph.carol]
    );
    assert_eq!(
        engine
            .component_of(
                graph.alice,
                &ComponentOptions {
                    edge_label_filter: missing_edge.clone(),
                    ..Default::default()
                },
            )
            .unwrap(),
        vec![graph.alice]
    );
    assert_eq!(
        engine
            .component_of(
                graph.alice,
                &ComponentOptions {
                    node_label_filter: Some(named_node_label_filter(
                        &["MissingLabel"],
                        LabelMatchMode::Any,
                    )),
                    ..Default::default()
                },
            )
            .unwrap(),
        Vec::<u64>::new()
    );

    let ppr = engine
        .personalized_pagerank(
            &[graph.alice],
            &PprOptions {
                edge_label_filter: mixed_knows.clone(),
                max_results: Some(10),
                ..Default::default()
            },
        )
        .unwrap();
    assert!(!hit_ids(&ppr.scores).contains(&graph.acme));
    let ppr_unknown = engine
        .personalized_pagerank(
            &[graph.alice],
            &PprOptions {
                edge_label_filter: missing_edge.clone(),
                max_results: Some(10),
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(hit_ids(&ppr_unknown.scores), HashSet::from([graph.alice]));

    let subgraph = engine
        .extract_subgraph(
            graph.alice,
            2,
            &SubgraphOptions {
                edge_label_filter: mixed_knows,
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(
        ids(&subgraph.nodes, |node| node.id),
        HashSet::from([graph.alice, graph.bob, graph.carol])
    );
    assert!(subgraph
        .nodes
        .iter()
        .all(|node| node.labels.as_slice() == ["Person"]));
    assert!(subgraph.edges.iter().all(|edge| edge.label == "KNOWS"));

    let empty_subgraph = engine
        .extract_subgraph(
            graph.alice,
            2,
            &SubgraphOptions {
                edge_label_filter: missing_edge.clone(),
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(empty_subgraph.nodes.len(), 1);
    assert_eq!(empty_subgraph.nodes[0].id, graph.alice);
    assert!(empty_subgraph.edges.is_empty());

    let export = engine
        .export_adjacency(&ExportOptions {
            node_label_filter: Some(named_node_label_filter(
                &["Person", "MissingLabel"],
                LabelMatchMode::Any,
            )),
            edge_label_filter: knows,
            include_weights: true,
        })
        .unwrap();
    assert_eq!(export.edge_labels, vec!["KNOWS".to_string()]);
    assert!(export.edges.iter().all(|edge| edge.edge_label_index == 0));
    assert!(export.edges.iter().all(|edge| edge.weight.is_some()));
    assert!(export.edges.iter().all(|edge| edge.to != graph.doc));

    let empty_export = engine
        .export_adjacency(&ExportOptions {
            node_label_filter: Some(named_node_label_filter(
                &["MissingLabel"],
                LabelMatchMode::Any,
            )),
            ..Default::default()
        })
        .unwrap();
    assert!(empty_export.node_ids.is_empty());
    assert!(empty_export.edge_labels.is_empty());
    assert!(empty_export.edges.is_empty());
    assert!(engine
        .export_adjacency(&ExportOptions {
            node_label_filter: Some(named_node_label_filter(
                &["MissingLabel"],
                LabelMatchMode::Any,
            )),
            edge_label_filter: Some(vec!["".to_string()]),
            ..Default::default()
        })
        .is_err());

    let edge_empty_export = engine
        .export_adjacency(&ExportOptions {
            edge_label_filter: missing_edge.clone(),
            include_weights: false,
            ..Default::default()
        })
        .unwrap();
    assert!(!edge_empty_export.node_ids.is_empty());
    assert!(edge_empty_export.edge_labels.is_empty());
    assert!(edge_empty_export.edges.is_empty());

    let pruned = engine
        .upsert_node("Person", "pruned", node_options(0.1, Some(vec![(1, 0.2)])))
        .unwrap();
    engine
        .upsert_edge(graph.alice, pruned, "KNOWS", weighted_edge(9.0))
        .unwrap();
    assert_eq!(
        engine
            .degree(
                graph.alice,
                &DegreeOptions {
                    edge_label_filter: Some(vec!["KNOWS".to_string()]),
                    ..Default::default()
                },
            )
            .unwrap(),
        2
    );
    engine
        .set_prune_policy(
            "low_weight_people",
            PrunePolicy {
                max_age_ms: None,
                max_weight: Some(0.5),
                label: Some("Person".to_string()),
            },
        )
        .unwrap();
    let visible_after_policy = engine
        .neighbors(
            graph.alice,
            &NeighborOptions {
                edge_label_filter: Some(vec!["KNOWS".to_string()]),
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(
        ids(&visible_after_policy, |entry| entry.node_id),
        HashSet::from([graph.bob])
    );
    assert_eq!(engine.get_edge_label_id("MISSING_EDGE").unwrap(), None);
    assert_eq!(engine.get_node_label_id("MissingLabel").unwrap(), None);

    engine.close().unwrap();
}

#[test]
fn named_graph_filters_survive_flush_compaction_and_reopen() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("db");
    {
        let engine = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
        let alice = engine
            .upsert_node("Person", "alice", UpsertNodeOptions::default())
            .unwrap();
        let bob = engine
            .upsert_node("Person", "bob", UpsertNodeOptions::default())
            .unwrap();
        engine
            .upsert_edge(alice, bob, "KNOWS", weighted_edge(1.0))
            .unwrap();
        engine.flush().unwrap();

        let acme = engine
            .upsert_node("Company", "acme", UpsertNodeOptions::default())
            .unwrap();
        engine
            .upsert_edge(alice, acme, "WORKS_AT", weighted_edge(2.0))
            .unwrap();
        engine.flush().unwrap();
        engine.compact().unwrap();
        engine.close().unwrap();
    }

    let reopened = DatabaseEngine::open(&db_path, &DbOptions::default()).unwrap();
    let alice = reopened
        .get_node_by_key("Person", "alice")
        .unwrap()
        .unwrap()
        .id;
    let neighbors = reopened
        .neighbors(
            alice,
            &NeighborOptions {
                edge_label_filter: Some(vec!["KNOWS".to_string(), "MISSING_EDGE".to_string()]),
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(neighbors.len(), 1);
    assert_eq!(neighbors[0].label, "KNOWS");

    let export = reopened
        .export_adjacency(&ExportOptions {
            edge_label_filter: Some(Vec::new()),
            include_weights: false,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(
        export.edge_labels.iter().cloned().collect::<HashSet<_>>(),
        HashSet::from(["KNOWS".to_string(), "WORKS_AT".to_string()])
    );
    assert!(export.edges.iter().all(|edge| edge.weight.is_none()));
    assert_eq!(reopened.get_edge_label_id("MISSING_EDGE").unwrap(), None);
    reopened.close().unwrap();
}

#[test]
fn vector_search_uses_named_label_and_scope_filters_but_returns_id_scores() {
    let dir = TempDir::new().unwrap();
    let (engine, graph) = open_graph(&dir.path().join("db"));

    let request = |label_filter, scope| VectorSearchRequest {
        mode: VectorSearchMode::Sparse,
        dense_query: None,
        sparse_query: Some(vec![(1, 1.0)]),
        k: 10,
        label_filter,
        ef_search: None,
        scope,
        dense_weight: None,
        sparse_weight: None,
        fusion_mode: None,
    };

    let person_hits = engine
        .vector_search(&request(
            Some(named_node_label_filter(
                &["Person", "MissingLabel"],
                LabelMatchMode::Any,
            )),
            None,
        ))
        .unwrap();
    assert_eq!(
        ids(&person_hits, |hit| hit.node_id),
        HashSet::from([graph.alice, graph.bob])
    );

    assert!(engine
        .vector_search(&request(
            Some(named_node_label_filter(
                &["MissingLabel"],
                LabelMatchMode::Any
            )),
            None,
        ))
        .unwrap()
        .is_empty());
    assert!(engine
        .vector_search(&request(
            Some(named_node_label_filter(
                &["MissingLabel"],
                LabelMatchMode::Any
            )),
            Some(VectorSearchScope {
                start_node_id: graph.alice,
                max_depth: 1,
                direction: Direction::Outgoing,
                edge_label_filter: Some(vec!["".to_string()]),
                at_epoch: None,
            }),
        ))
        .is_err());

    let unconstrained_hits = engine.vector_search(&request(None, None)).unwrap();
    let unconstrained_ids = ids(&unconstrained_hits, |hit| hit.node_id);
    assert!(unconstrained_ids.contains(&graph.doc));
    assert!(unconstrained_ids.contains(&graph.acme));
    assert!(engine
        .vector_search(&request(
            Some(named_node_label_filter(&[], LabelMatchMode::Any)),
            None,
        ))
        .is_err());

    let knows_scope = Some(VectorSearchScope {
        start_node_id: graph.alice,
        max_depth: 1,
        direction: Direction::Outgoing,
        edge_label_filter: Some(vec!["KNOWS".to_string(), "MISSING_EDGE".to_string()]),
        at_epoch: None,
    });
    let scoped_hits = engine.vector_search(&request(None, knows_scope)).unwrap();
    assert_eq!(
        ids(&scoped_hits, |hit| hit.node_id),
        HashSet::from([graph.alice, graph.bob])
    );

    let unknown_scope = Some(VectorSearchScope {
        start_node_id: graph.alice,
        max_depth: 1,
        direction: Direction::Outgoing,
        edge_label_filter: Some(vec!["MISSING_EDGE".to_string()]),
        at_epoch: None,
    });
    let start_only = engine.vector_search(&request(None, unknown_scope)).unwrap();
    assert_eq!(
        ids(&start_only, |hit| hit.node_id),
        HashSet::from([graph.alice])
    );

    let empty_scope = Some(VectorSearchScope {
        start_node_id: graph.alice,
        max_depth: 1,
        direction: Direction::Outgoing,
        edge_label_filter: Some(Vec::new()),
        at_epoch: None,
    });
    let empty_scope_ids = ids(
        &engine.vector_search(&request(None, empty_scope)).unwrap(),
        |hit| hit.node_id,
    );
    assert!(empty_scope_ids.contains(&graph.alice));
    assert!(empty_scope_ids.contains(&graph.bob));
    assert!(empty_scope_ids.contains(&graph.acme));
    assert!(!empty_scope_ids.contains(&graph.doc));
    assert_eq!(engine.get_edge_label_id("MISSING_EDGE").unwrap(), None);
    assert_eq!(engine.get_node_label_id("MissingLabel").unwrap(), None);

    engine.close().unwrap();
}

#[test]
fn vector_search_unknown_label_filter_does_not_hide_invalid_shape() {
    let dir = TempDir::new().unwrap();
    let opts = DbOptions {
        dense_vector: Some(DenseVectorConfig {
            dimension: 3,
            metric: DenseMetric::Cosine,
            hnsw: HnswConfig::default(),
        }),
        ..Default::default()
    };
    let engine = DatabaseEngine::open(dir.path(), &opts).unwrap();
    let unknown_label = Some(named_node_label_filter(
        &["MissingLabel"],
        LabelMatchMode::Any,
    ));

    let dense_request = |dense_query, ef_search| VectorSearchRequest {
        mode: VectorSearchMode::Dense,
        dense_query,
        sparse_query: None,
        k: 5,
        label_filter: unknown_label.clone(),
        ef_search,
        scope: None,
        dense_weight: None,
        sparse_weight: None,
        fusion_mode: None,
    };

    let err = engine
        .vector_search(&dense_request(None, None))
        .unwrap_err();
    assert!(err.to_string().contains("requires dense_query"));

    let err = engine
        .vector_search(&dense_request(Some(vec![0.1, 0.2]), None))
        .unwrap_err();
    assert!(err
        .to_string()
        .contains("does not match configured dimension"));

    let err = engine
        .vector_search(&dense_request(Some(vec![0.1, 0.2, 0.3]), Some(0)))
        .unwrap_err();
    assert!(err.to_string().contains("ef_search must be > 0"));

    let err = engine
        .vector_search(&VectorSearchRequest {
            mode: VectorSearchMode::Sparse,
            dense_query: None,
            sparse_query: Some(vec![(1, -1.0)]),
            k: 5,
            label_filter: unknown_label.clone(),
            ef_search: None,
            scope: None,
            dense_weight: None,
            sparse_weight: None,
            fusion_mode: None,
        })
        .unwrap_err();
    assert!(err
        .to_string()
        .contains("sparse vector weights must be non-negative"));

    let err = engine
        .vector_search(&VectorSearchRequest {
            mode: VectorSearchMode::Hybrid,
            dense_query: None,
            sparse_query: None,
            k: 5,
            label_filter: unknown_label,
            ef_search: None,
            scope: None,
            dense_weight: None,
            sparse_weight: None,
            fusion_mode: None,
        })
        .unwrap_err();
    assert!(err.to_string().contains("requires at least one"));
}
