//! OverGraph Example: Knowledge Graph
//!
//! This example shows how to use OverGraph to build and query a
//! knowledge graph of people, projects, facts, and relationships.
//!
//! Run: cargo run --example knowledge_graph

use overgraph::{
    DatabaseEngine, DbOptions, EdgeInput, NeighborOptions, NodeInput, PprOptions, PropValue,
    UpsertNodeOptions,
};
use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

const RELATED_TO: &str = "RELATED_TO";

fn props(pairs: &[(&str, &str)]) -> BTreeMap<String, PropValue> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), PropValue::String(v.to_string())))
        .collect()
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let run_id = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    let db_path = env::temp_dir().join(format!("overgraph-knowledge-rust-{run_id}"));

    let result = run_example(&db_path);
    let _ = fs::remove_dir_all(&db_path);
    result
}

fn run_example(db_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let db = DatabaseEngine::open(db_path, &DbOptions::default())?;
    db.ensure_node_label("Person")?;
    db.ensure_node_label("Contributor")?;
    db.ensure_node_label("Project")?;
    db.ensure_node_label("Fact")?;
    db.ensure_node_label("Conversation")?;
    db.ensure_edge_label("RELATED_TO")?;
    db.ensure_edge_label("MENTIONED_IN")?;
    db.ensure_edge_label("SUPPORTS")?;

    // --- Build a knowledge graph ---

    // Create people and a project
    let node_ids = db.batch_upsert_nodes(vec![
        NodeInput {
            labels: vec!["Person".into(), "Contributor".into()],
            key: "alice".into(),
            props: props(&[("name", "Alice"), ("role", "engineer")]),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        },
        NodeInput {
            labels: vec!["Person".into(), "Contributor".into()],
            key: "bob".into(),
            props: props(&[("name", "Bob"), ("role", "designer")]),
            weight: 0.9,
            dense_vector: None,
            sparse_vector: None,
        },
        NodeInput {
            labels: vec!["Project".into()],
            key: "atlas".into(),
            props: props(&[("name", "Atlas"), ("status", "active")]),
            weight: 0.95,
            dense_vector: None,
            sparse_vector: None,
        },
    ])?;
    let (alice, bob, project) = (node_ids[0], node_ids[1], node_ids[2]);

    // Create some facts
    let fact_ids = db.batch_upsert_nodes(vec![
        NodeInput {
            labels: vec!["Fact".into()],
            key: "alice-leads-atlas".into(),
            props: props(&[("text", "Alice leads the Atlas project")]),
            weight: 0.9,
            dense_vector: None,
            sparse_vector: None,
        },
        NodeInput {
            labels: vec!["Fact".into()],
            key: "bob-designs-atlas".into(),
            props: props(&[("text", "Bob is the lead designer on Atlas")]),
            weight: 0.85,
            dense_vector: None,
            sparse_vector: None,
        },
    ])?;
    let (fact1, fact2) = (fact_ids[0], fact_ids[1]);

    // Create a conversation node
    let convo = db.upsert_node(
        "Conversation",
        "2024-01-15",
        UpsertNodeOptions {
            props: props(&[("summary", "Discussed Atlas project timeline")]),
            weight: 0.7,
            ..Default::default()
        },
    )?;

    // Connect everything with labeled edges
    db.batch_upsert_edges(vec![
        EdgeInput {
            from: alice,
            to: project,
            label: RELATED_TO.into(),
            props: props(&[("role", "lead")]),
            weight: 1.0,
            valid_from: None,
            valid_to: None,
        },
        EdgeInput {
            from: bob,
            to: project,
            label: RELATED_TO.into(),
            props: props(&[("role", "designer")]),
            weight: 0.9,
            valid_from: None,
            valid_to: None,
        },
        EdgeInput {
            from: alice,
            to: bob,
            label: RELATED_TO.into(),
            props: props(&[("context", "teammates")]),
            weight: 0.8,
            valid_from: None,
            valid_to: None,
        },
        EdgeInput {
            from: fact1,
            to: convo,
            label: "MENTIONED_IN".into(),
            props: Default::default(),
            weight: 0.9,
            valid_from: None,
            valid_to: None,
        },
        EdgeInput {
            from: fact2,
            to: convo,
            label: "MENTIONED_IN".into(),
            props: Default::default(),
            weight: 0.85,
            valid_from: None,
            valid_to: None,
        },
        EdgeInput {
            from: fact1,
            to: alice,
            label: "SUPPORTS".into(),
            props: Default::default(),
            weight: 0.9,
            valid_from: None,
            valid_to: None,
        },
        EdgeInput {
            from: fact1,
            to: project,
            label: "SUPPORTS".into(),
            props: Default::default(),
            weight: 0.9,
            valid_from: None,
            valid_to: None,
        },
    ])?;

    println!("Knowledge graph built!\n");

    // --- Query the graph ---

    // 1. Who is Alice connected to?
    let neighbors = db.neighbors(
        alice,
        &NeighborOptions {
            edge_label_filter: Some(vec![RELATED_TO.to_string()]),
            limit: Some(10),
            ..Default::default()
        },
    )?;
    println!("Alice's connections ({}):", neighbors.len());
    for n in &neighbors {
        if let Some(node) = db.get_node(n.node_id)? {
            if let Some(PropValue::String(name)) = node.props.get("name") {
                println!("  -> {} (weight: {})", name, n.weight);
            }
        }
    }

    // 2. Find all people
    let people = db.get_nodes_by_labels("Person")?;
    println!("\nAll people ({}):", people.len());
    for person in &people {
        let name = match person.props.get("name") {
            Some(PropValue::String(s)) => s.as_str(),
            _ => &person.key,
        };
        let role = match person.props.get("role") {
            Some(PropValue::String(s)) => s.as_str(),
            _ => "unknown",
        };
        println!("  {} ({})", name, role);
    }

    // 3. Personalized PageRank: what's most relevant to Alice?
    let ppr = db.personalized_pagerank(
        &[alice],
        &PprOptions {
            max_results: Some(5),
            max_iterations: 50,
            ..Default::default()
        },
    )?;
    println!(
        "\nMost relevant to Alice (PPR, {} results):",
        ppr.scores.len()
    );
    for (node_id, score) in &ppr.scores {
        if let Some(node) = db.get_node(*node_id)? {
            let name = node
                .props
                .get("name")
                .or(node.props.get("text"))
                .or(node.props.get("summary"));
            let label = match name {
                Some(PropValue::String(s)) => s.clone(),
                _ => node.key.clone(),
            };
            println!("  {}: {:.4}", label, score);
        }
    }

    // 4. Database stats
    let stats = db.stats()?;
    println!("\nDatabase stats:");
    println!("  Segments: {}", stats.segment_count);
    println!("  WAL sync mode: {}", stats.wal_sync_mode);

    db.close()?;
    println!("\nDatabase closed.");
    Ok(())
}
