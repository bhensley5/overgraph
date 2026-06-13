//! OverGraph Example: GQL
//!
//! Run: cargo run --example gql_readonly

use overgraph::{
    DatabaseEngine, DbOptions, GqlExecutionOptions, GqlParamValue, GqlParams, PropValue,
    UpsertEdgeOptions, UpsertNodeOptions,
};
use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

fn props(pairs: &[(&str, PropValue)]) -> BTreeMap<String, PropValue> {
    pairs
        .iter()
        .map(|(key, value)| ((*key).to_string(), value.clone()))
        .collect()
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let run_id = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    let db_path = env::temp_dir().join(format!("overgraph-gql-rust-{run_id}"));

    let result = run_example(&db_path);
    let _ = fs::remove_dir_all(&db_path);
    result
}

fn run_example(db_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let db = DatabaseEngine::open(db_path, &DbOptions::default())?;
    let ada = db.upsert_node(
        "Person",
        "ada",
        UpsertNodeOptions {
            props: props(&[
                ("name", PropValue::String("Ada".to_string())),
                ("status", PropValue::String("active".to_string())),
                ("rank", PropValue::Int(2)),
            ]),
            ..Default::default()
        },
    )?;
    let ben = db.upsert_node(
        "Person",
        "ben",
        UpsertNodeOptions {
            props: props(&[
                ("name", PropValue::String("Ben".to_string())),
                ("status", PropValue::String("active".to_string())),
                ("rank", PropValue::Int(1)),
            ]),
            ..Default::default()
        },
    )?;
    let acme = db.upsert_node(
        "Company",
        "acme",
        UpsertNodeOptions {
            props: props(&[("name", PropValue::String("Acme".to_string()))]),
            ..Default::default()
        },
    )?;
    db.upsert_edge(
        ada,
        acme,
        "WORKS_AT",
        UpsertEdgeOptions {
            props: props(&[
                ("role", PropValue::String("engineer".to_string())),
                ("since", PropValue::Int(2020)),
            ]),
            ..Default::default()
        },
    )?;
    db.upsert_edge(
        ben,
        acme,
        "WORKS_AT",
        UpsertEdgeOptions {
            props: props(&[
                ("role", PropValue::String("designer".to_string())),
                ("since", PropValue::Int(2022)),
            ]),
            ..Default::default()
        },
    )?;

    let result = db.execute_gql(
        "MATCH (p:Person)-[r:WORKS_AT]->(c:Company)
         WHERE p.status = $status
         RETURN p.name AS person, r.role AS role, c.name AS company
         ORDER BY p.rank ASC
         LIMIT 10",
        &GqlParams::from([(
            "status".to_string(),
            GqlParamValue::String("active".to_string()),
        )]),
        &GqlExecutionOptions {
            include_plan: true,
            profile: true,
            ..GqlExecutionOptions::default()
        },
    )?;

    println!("columns: {:?}", result.columns);
    println!("rows: {:?}", result.rows);
    println!("stats: {:?}", result.stats);

    Ok(())
}
