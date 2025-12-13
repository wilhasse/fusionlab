# FusionLab

A Rust CLI + library that runs the same query in multiple strategies and compares results.

## What it does

- **Direct MySQL** (baseline) - Step 0
- **DataFusion over MySQL** (using MySQL TableProvider + pushdowns) - Steps 1-5
- **"Smart path"**: semijoin reduction - Step 7
- **Router**: strategy selection with learning - Step 8
- **Replay harness**: validation + benchmarking - Step 9

## Quick Start

### Prerequisites

1. **Rust** (1.70+)
   ```bash
   curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
   ```

2. **Docker** (for MySQL)
   ```bash
   # Install docker and docker-compose
   ```

### Setup

```bash
# Start MySQL with SSB schema
cd docker
docker compose up -d

# Build FusionLab
cargo build --release

# The binary is at target/release/fusionlab
```

### Load SSB Data

See `data/generator/README.md` for instructions to generate and load SSB benchmark data.

### Usage

```bash
# Run a query and see timing
fusionlab mysql "SELECT COUNT(*) FROM lineorder"

# Run with EXPLAIN output
fusionlab mysql "SELECT COUNT(*) FROM lineorder WHERE lo_quantity > 10" --explain

# Run with EXPLAIN ANALYZE (MySQL 8.0.18+)
fusionlab mysql "SELECT COUNT(*) FROM lineorder" --analyze

# Run a query file
fusionlab mysql --file data/queries/q1.1.sql

# Custom connection
fusionlab mysql "SELECT 1" --host 127.0.0.1 --port 3306 --user root --password root --database ssb

# Control result display
fusionlab mysql "SELECT * FROM customer LIMIT 100" --show-rows 20
```

## Project Structure

```
fusionlab/
├── Cargo.toml                 # Workspace root
├── crates/
│   ├── fusionlab-cli/         # CLI entrypoint
│   └── fusionlab-core/        # Shared logic (query runner, metrics)
├── docker/
│   ├── docker-compose.yml     # MySQL container
│   └── init.sql               # SSB schema + indexes
├── data/
│   ├── generator/             # Data generation instructions
│   └── queries/               # 13 SSB benchmark queries
└── results/                   # Query execution results
```

## SSB Queries

The `data/queries/` directory contains the 13 Star Schema Benchmark queries:

| Query | Description |
|-------|-------------|
| Q1.1-Q1.3 | Revenue calculations with date/discount/quantity filters |
| Q2.1-Q2.3 | Revenue by year and brand with region filters |
| Q3.1-Q3.4 | Revenue by customer/supplier geography |
| Q4.1-Q4.3 | Profit analysis by geography and product |

## Development Roadmap

- [x] **Step 0**: MySQL baseline (this PR)
- [ ] **Step 1**: Hello DataFusion
- [ ] **Step 2**: EXPLAIN / EXPLAIN ANALYZE
- [ ] **Step 3**: Custom TableProvider (pushdown logging)
- [ ] **Step 4**: DataFusion → MySQL via existing providers
- [ ] **Step 5**: Minimal MySQL TableProvider
- [ ] **Step 6**: Custom optimizer rules
- [ ] **Step 7**: Semijoin reduction strategy
- [ ] **Step 8**: Strategy router
- [ ] **Step 9**: Replay harness
- [ ] **Step 10**: Dynamic filters
- [ ] **Step 11**: Custom ExecutionPlan node

## License

MIT
