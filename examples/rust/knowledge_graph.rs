//! OverGraph Example: Knowledge Graph
//!
//! This example shows how to use OverGraph to build and query a
//! knowledge graph of entities, facts, and relationships.
//!
//! Run: cargo run --example knowledge_graph

use overgraph::{
    DatabaseEngine, DbOptions, Direction, NodeInput, EdgeInput, PprOptions, PropValue,
};
use std::collections::BTreeMap;
use std::path::Path;

// Type IDs (you define these for your application)
const ENTITY: u32 = 1;
const FACT: u32 = 2;
const CONVERSATION: u32 = 3;

const RELATED_TO: u32 = 10;
const MENTIONED_IN: u32 = 11;
const SUPPORTS: u32 = 12;

fn props(pairs: &[(&str, &str)]) -> BTreeMap<String, PropValue> {
    pairs.iter().map(|(k, v)| (k.to_string(), PropValue::String(v.to_string()))).collect()
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut db = DatabaseEngine::open(Path::new("./example-graph"), &DbOptions::default())?;

    // --- Build a knowledge graph ---

    // Create some entities
    let entity_ids = db.batch_upsert_nodes(&[
        NodeInput { type_id: ENTITY, key: "person:alice".into(), props: props(&[("name", "Alice"), ("role", "engineer")]), weight: 1.0 },
        NodeInput { type_id: ENTITY, key: "person:bob".into(), props: props(&[("name", "Bob"), ("role", "designer")]), weight: 0.9 },
        NodeInput { type_id: ENTITY, key: "project:atlas".into(), props: props(&[("name", "Atlas"), ("status", "active")]), weight: 0.95 },
    ])?;
    let (alice, bob, project) = (entity_ids[0], entity_ids[1], entity_ids[2]);

    // Create some facts
    let fact_ids = db.batch_upsert_nodes(&[
        NodeInput { type_id: FACT, key: "fact:alice-leads-atlas".into(), props: props(&[("text", "Alice leads the Atlas project")]), weight: 0.9 },
        NodeInput { type_id: FACT, key: "fact:bob-designs-atlas".into(), props: props(&[("text", "Bob is the lead designer on Atlas")]), weight: 0.85 },
    ])?;
    let (fact1, fact2) = (fact_ids[0], fact_ids[1]);

    // Create a conversation node
    let convo = db.upsert_node(CONVERSATION, "convo:2024-01-15",
        props(&[("summary", "Discussed Atlas project timeline")]), 0.7)?;

    // Connect everything with typed edges
    db.batch_upsert_edges(&[
        EdgeInput { from: alice, to: project, type_id: RELATED_TO, props: props(&[("role", "lead")]), weight: 1.0, valid_from: None, valid_to: None },
        EdgeInput { from: bob, to: project, type_id: RELATED_TO, props: props(&[("role", "designer")]), weight: 0.9, valid_from: None, valid_to: None },
        EdgeInput { from: alice, to: bob, type_id: RELATED_TO, props: props(&[("context", "teammates")]), weight: 0.8, valid_from: None, valid_to: None },
        EdgeInput { from: fact1, to: convo, type_id: MENTIONED_IN, props: Default::default(), weight: 0.9, valid_from: None, valid_to: None },
        EdgeInput { from: fact2, to: convo, type_id: MENTIONED_IN, props: Default::default(), weight: 0.85, valid_from: None, valid_to: None },
        EdgeInput { from: fact1, to: alice, type_id: SUPPORTS, props: Default::default(), weight: 0.9, valid_from: None, valid_to: None },
        EdgeInput { from: fact1, to: project, type_id: SUPPORTS, props: Default::default(), weight: 0.9, valid_from: None, valid_to: None },
    ])?;

    println!("Knowledge graph built!\n");

    // --- Query the graph ---

    // 1. Who is Alice connected to?
    let neighbors = db.neighbors(alice, Direction::Outgoing, Some(&[RELATED_TO]), 10, None, None)?;
    println!("Alice's connections ({}):", neighbors.len());
    for n in &neighbors {
        if let Some(node) = db.get_node(n.node_id)? {
            if let Some(PropValue::String(name)) = node.props.get("name") {
                println!("  -> {} (weight: {})", name, n.weight);
            }
        }
    }

    // 2. Find all entities
    let entities = db.get_nodes_by_type(ENTITY)?;
    println!("\nAll entities ({}):", entities.len());
    for entity in &entities {
        let name = match entity.props.get("name") {
            Some(PropValue::String(s)) => s.as_str(),
            _ => &entity.key,
        };
        let detail = entity.props.get("role").or(entity.props.get("status"));
        let detail_str = match detail {
            Some(PropValue::String(s)) => s.as_str(),
            _ => "unknown",
        };
        println!("  {} ({})", name, detail_str);
    }

    // 3. Personalized PageRank: what's most relevant to Alice?
    let ppr = db.personalized_pagerank(&[alice], &PprOptions {
        max_results: Some(5),
        max_iterations: 50,
        ..Default::default()
    })?;
    println!("\nMost relevant to Alice (PPR, {} results):", ppr.scores.len());
    for (node_id, score) in &ppr.scores {
        if let Some(node) = db.get_node(*node_id)? {
            let name = node.props.get("name")
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
    let stats = db.stats();
    println!("\nDatabase stats:");
    println!("  Segments: {}", stats.segment_count);
    println!("  WAL sync mode: {}", stats.wal_sync_mode);

    db.close()?;
    println!("\nDatabase closed.");
    Ok(())
}
