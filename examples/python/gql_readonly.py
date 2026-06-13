"""
OverGraph Example: GQL

Run: python examples/python/gql_readonly.py
(Requires: maturin develop in overgraph-python/ first)
"""

import tempfile

from overgraph import AsyncOverGraph, OverGraph


def sync_example() -> None:
    with tempfile.TemporaryDirectory(prefix="overgraph-gql-python-") as db_dir:
        with OverGraph.open(db_dir, dense_vector_dimension=3) as db:
            ada = db.upsert_node(
                "Person",
                "ada",
                props={"name": "Ada", "status": "active", "rank": 2},
                dense_vector=[0.1, 0.2, 0.3],
            )
            ben = db.upsert_node(
                "Person",
                "ben",
                props={"name": "Ben", "status": "active", "rank": 1},
            )
            acme = db.upsert_node("Company", "acme", props={"name": "Acme"})
            db.upsert_edge(ada, acme, "WORKS_AT", props={"role": "engineer", "since": 2020})
            db.upsert_edge(ben, acme, "WORKS_AT", props={"role": "designer", "since": 2022})

            result = db.execute_gql(
                """
                MATCH (p:Person)-[r:WORKS_AT]->(c:Company)
                WHERE p.status = $status
                RETURN p.name AS person, r.role AS role, c.name AS company
                ORDER BY p.rank ASC
                LIMIT 10
                """,
                {"status": "active"},
                include_plan=True,
                profile=True,
            )
            print(result["rows"])
            print(result["stats"])
            print(result["plan"]["row_ops"])

            compact = db.execute_gql(
                "MATCH (n:Person) RETURN n.name AS name, n.rank AS rank ORDER BY n.rank",
                compact_rows=True,
            )
            print(compact["columns"], compact["rows"])

            with_vectors = db.execute_gql(
                "MATCH (n:Person) WHERE n.name = 'Ada' RETURN n",
                include_vectors=True,
            )
            print(with_vectors["rows"][0]["n"]["dense_vector"])


async def async_example() -> None:
    with tempfile.TemporaryDirectory(prefix="overgraph-gql-python-async-") as db_dir:
        db = await AsyncOverGraph.open(db_dir)
        try:
            await db.upsert_node("Person", "ada", props={"name": "Ada", "rank": 1})
            result = await db.execute_gql(
                "MATCH (n:Person) RETURN n.name AS name ORDER BY n.rank",
                compact_rows=True,
            )
            print(result["rows"])
        finally:
            await db.close()


if __name__ == "__main__":
    sync_example()
