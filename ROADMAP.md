# Roadmap

OverGraph started with a simple idea: a graph database should be something you can
drop into your application and get on with building. A directory on disk, a Rust
engine in your process, and native APIs for Rust, Node.js, and Python.

The part we're excited about is what happens when relationships, search, and memory
live together. Find a piece of code, follow it to its callers, and pull in the context
that explains it. Give an agent memory that stays useful as it grows. Explore the
shape of a knowledge graph without exporting it to another system.

Here's where we want to take that. These are product directions; priorities and API
details will evolve with real applications. Released features live in the
[changelog](CHANGELOG.md).

## What you can build with today

OverGraph already combines graph traversal, dense and sparse vector search, hybrid
ranking, and search within graph neighborhoods. It has transactions, temporal and
weighted relationships, optional schemas, and property indexes. You can work through
native APIs or GQL/Cypher-style queries over the same engine.

The releases through **0.17.0** added GQL reads, writes, and pipelines; compound
indexes; offline integrity checks; and streaming graph execution with bounded row
production and early exit for eligible queries. That's the foundation for the next
set of possibilities.

## Where we're heading

### Add vectors when your application needs them

You shouldn't have to settle your embedding strategy when you open the database.
The direction is straightforward: open a graph, then declare and manage vector
spaces without closing it.

Today, dense vectors use one database-wide configuration, set when opening or
reopening and persisted for later opens; sparse vectors don't need that setup.
We want multiple named spaces, each with its own dimensions, distance metric, and
index settings. Code and document embeddings could share a graph even when they
come from different models.
A node could carry more than one representation.

That also gives model upgrades a much better story: populate a new space alongside
the old one, compare retrieval, and switch when you're ready. Runtime declarations
and space management are the intended experience; the API and migration details
still need design work.

### Search by meaning, words, and relationships together

“Find similar things” gets much more useful when you can say which things count.
Property-filtered vector search would let you ask for the best matches in a
particular repository, language, or time range directly. Path-prefix acceleration
would make searches within a source directory practical too.

Graph scope should go beyond a fixed neighborhood. We want dense, sparse, and hybrid
search to work over query results, graph patterns, PageRank-selected neighborhoods,
communities, or a set of nodes you've already picked. Composition should work in
both directions: narrow the graph before searching, or use vector hits to seed a
graph query and carry their scores through the results.

Built-in text indexing and **BM25** are another big piece. Exact names and unusual
terms matter, especially in code. Code-aware tokenization could connect
`write_segment` with “segment writer,” while dense embeddings capture meaning and
learned sparse vectors provide another useful signal. We want all three available
in a combined retrieval workflow.

### Keep reads moving while the graph changes

An indexer should be able to update a repository while people search it. An agent
should be able to record new memories while other tasks explore the graph.

The goal is to let ordinary reads keep running while write batches are installed,
with each read still seeing a consistent view. The proposed design uses append-only,
versioned indexes and moves expensive cleanup to engine-owned workers.

This has a reviewed design; implementation approval, production integration, and
qualification are still ahead. Prototype results exposed tradeoffs in throughput,
memory, and tail latency. The job now is to prove the whole durable engine under
mixed workloads, including recovery and long-running readers.

### Make bigger workloads feel predictable

Small result sets should stay manageable even when the graph behind them is large.
Recent streaming work gets us further along that path. Next candidates include
better planning around heavily connected nodes, less repeated work on deep cursor
pages, and earlier filtering in native graph queries.

We also want explicit work budgets and cancellation for expensive queries. On the
vector side, we're exploring opt-in approximate search with clear recall tradeoffs,
more efficient sparse scoring, and better control over hybrid ranking. Search
profiling should show where time went, how filtering behaved, and when an index
fell back to a scan. Retrieval quality needs measurement alongside speed.

## More to explore

### Memory that can change and forget

OverGraph already has temporal relationships, decay scoring, pruning, and retention
policies. We'd like to make it easier to express how an application's memory should
age.

Per-record expiration could give a conversation's temporary context a natural end.
Decay-based cleanup could let low-weight memories fade away while important ones
last longer. More efficient retention could keep large histories affordable to
maintain.

These are opt-in directions for automatic expiration and cleanup. The application
should decide what deserves to stay, with understandable rules for when something
stops appearing in reads and when its storage is reclaimed.

### Give code tools and agents more useful context

A good code index needs to keep up with edits. We want smoother incremental
indexing, including scoped synchronization that removes chunks a file no longer
contains. Recipes can show how to connect parsers, embedding models, and graph
updates into a working pipeline.

An agent-facing CLI or MCP server could then expose useful operations: search for a
concept, find a definition, inspect callers and importers, or assemble a focused
repository map. Diversity reranking and graph-based expansion could help a small
context window cover more ground. Embedding generation and model inference would
remain in the application, so you can choose the models that fit your work.

### See the larger shape of your graph

There's more to a graph than the next hop. Community detection could uncover groups
of related documents, tightly connected code, or clusters of activity. Centrality
could highlight the nodes that connect otherwise separate parts of a system.

Reusable graph projections would let us build an analytics snapshot once and run
Louvain community detection, betweenness, triangle counting, and clustering over
it. Harmonic closeness is a candidate if it fits that same foundation. Strongly
connected components would extend the live directed-graph APIs. Community and
projection membership could then guide retrieval.

### Take the graph into more places

Further out, we want a clearer path from an embedded application to shared use:
safe process ownership, read-only replicas, and an optional IPC or server layer
where one process owns the database and others connect to it. These deployment
modes are exploratory. Today, applications must ensure only one process owns a
database directory; automatic protection is part of the proposed work.

Import and migration tooling also matter. RDF import/export is a promising first
step toward connecting existing knowledge graphs; a stable RDF view would let us
evaluate SPARQL and limited ontology reasoning after that.

If one of these directions would unlock something you're building, tell us the
workflow. A concrete “I want to do this with my graph” is the most useful input to
what comes next.
