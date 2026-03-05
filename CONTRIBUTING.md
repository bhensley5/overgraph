# Contributing to OverGraph

Thanks for your interest in contributing. This doc covers everything you need to build, test, and submit changes.

## Prerequisites

You'll need:

- **Rust** (stable, 1.75+): https://rustup.rs
- **Node.js** (18+) and npm: for the Node.js connector
- **Python** (3.9+) and pip: for the Python connector
- **maturin**: `pip install maturin` (for building the Python extension)

## Building from source

### Rust core

```bash
cargo build
cargo build --release  # optimized build
```

The `cli` feature flag enables the `overgraph-inspect` binary:

```bash
cargo build --features cli
```

### Node.js connector

```bash
cd overgraph-node
npm install
npm run build        # release build
npm run build:debug  # debug build
```

### Python connector

```bash
cd overgraph-python
pip install maturin
maturin develop      # debug build, installs into current venv
maturin develop --release  # optimized build
```

## Running tests

```bash
# Rust (all tests including integration tests)
cargo test

# Rust with CLI feature tests
cargo test --features cli

# Node.js
cd overgraph-node && npm test

# Python
cd overgraph-python && pytest
```

All tests must pass before submitting a PR. The CI matrix runs on macOS, Linux, and Windows.

## Project structure

```
overgraph/
  src/                  # Rust core library
    engine.rs           # Main database engine
    types.rs            # Data types (Node, Edge, PropValue, etc.)
    wal.rs              # Write-ahead log
    memtable.rs         # In-memory table
    segment_reader.rs   # Reading immutable segments
    segment_writer.rs   # Writing/flushing segments
    manifest.rs         # Manifest management
    encoding.rs         # Binary encoding helpers
    error.rs            # Error types
    bin/                # CLI tools (behind "cli" feature)
  tests/                # Integration tests
  benches/              # Criterion benchmarks
  overgraph-node/       # Node.js connector (napi-rs)
    src/lib.rs          # napi-rs bindings
    __test__/           # Node.js tests
    index.d.ts          # TypeScript declarations
  overgraph-python/     # Python connector (PyO3)
    src/lib.rs          # PyO3 bindings
    python/overgraph/   # Python package (type stubs, async wrapper)
    tests/              # pytest tests
  docs/                 # Documentation
```

## Coding conventions

- **Rust style:** Standard `rustfmt` formatting. Run `cargo fmt` before committing.
- **Error handling:** Use `Result<T, EngineError>` for all fallible operations. Don't panic in library code.
- **Tests:** Every change that touches behavior should have a test. Unit tests go in `#[cfg(test)]` modules alongside the code. Integration tests go in `tests/`.
- **Dependencies:** Minimize external crates. Add a dependency only when it clearly beats a hand-rolled solution. We intentionally keep the dependency tree small for fast compile times and a small binary.
- **No C/C++ dependencies.** The core must stay pure Rust. This is a hard constraint.

## Dual index path

If you're adding a new index type, there's an important invariant to know about. Indexes are written in two places:

1. **Flush path** (`write_segment` in `segment_writer.rs`): builds indexes from the in-memory memtable
2. **Compaction path** (`write_indexes_from_metadata` in `segment_writer.rs`): builds indexes from metadata sidecars

Both paths must produce the same indexes. If you add a new index in the flush path but forget the compaction path, it will work for newly flushed segments but break after compaction. This is the most common source of subtle bugs in the storage engine.

## Submitting a PR

1. Fork the repo and create a branch from `main`.
2. Make your changes. Write tests.
3. Run `cargo test`, `npm test` (in overgraph-node), and `pytest` (in overgraph-python) to make sure everything passes.
4. Run `cargo fmt` to format your Rust code.
5. Open a PR against `main` with a clear description of what you changed and why.

## Reporting bugs

Open an issue with:

- What you expected to happen
- What actually happened
- Steps to reproduce (ideally a minimal code example)
- Your OS, Rust version, and OverGraph version

## Feature requests

Open an issue describing the use case. The more concrete the better: "I'm building X and I need Y because Z" is much more useful than "it would be cool if...".

## License

By contributing, you agree that your contributions will be licensed under both the MIT License and the Apache License 2.0, at the user's option.
