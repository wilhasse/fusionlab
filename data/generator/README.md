# SSB Data Generator

This directory contains scripts to generate Star Schema Benchmark (SSB) test data for FusionLab.

## Quick Start

```bash
# 1. Setup dbgen (download and compile)
./setup-dbgen.sh

# 2. Generate data (1GB scale factor)
./generate.sh -s 1

# 3. Load into MySQL
../../infra/pulumi/load-data.sh 10.1.0.50
```

## Scripts

### setup-dbgen.sh

Downloads and compiles the SSB dbgen tool from [tidb-bench](https://github.com/pingcap/tidb-bench).

```bash
./setup-dbgen.sh
```

**Requirements:**
- `gcc` (build-essential)
- `git`

### generate.sh

Generates SSB data with configurable scale factor.

```bash
# Usage
./generate.sh [OPTIONS]

# Options
  -s SCALE    Scale factor (1=1GB, 10=10GB, 100=100GB). Default: 1
  -o DIR      Output directory for .tbl files. Default: current directory
  -c          Clean existing .tbl files before generating
  -h          Show help

# Examples
./generate.sh -s 1              # Generate 1GB dataset
./generate.sh -s 10 -c          # Generate 10GB dataset, clean first
./generate.sh -s 1 -o /tmp/ssb  # Generate to specific directory
```

## Scale Factor Reference

| SF | lineorder rows | Total Size | Use Case |
|----|----------------|------------|----------|
| 1  | ~6 million     | ~600 MB    | Development, quick tests |
| 10 | ~60 million    | ~6 GB      | Performance testing |
| 100| ~600 million   | ~60 GB     | Production benchmarks |

### Row Counts at SF=1

| Table     | Rows     | Size    |
|-----------|----------|---------|
| lineorder | 6,001,171| ~600 MB |
| customer  | 30,000   | ~2.4 MB |
| part      | 200,000  | ~23 MB  |
| supplier  | 2,000    | ~200 KB |
| date      | 2,556    | ~24 KB  |

## Loading Data into MySQL

### Option 1: Use the load script

```bash
# Load into VM at default IP (10.1.0.50)
../../infra/pulumi/load-data.sh

# Load into specific VM
../../infra/pulumi/load-data.sh 10.1.0.51 .
```

### Option 2: Manual loading

```bash
# Copy files to VM
scp *.tbl ubuntu@10.1.0.50:~/

# SSH into VM and load
ssh ubuntu@10.1.0.50
mysql -u root -proot --local-infile=1 ssb << 'EOF'
SET FOREIGN_KEY_CHECKS = 0;

LOAD DATA LOCAL INFILE 'customer.tbl' INTO TABLE customer
  FIELDS TERMINATED BY '|' LINES TERMINATED BY '|\n';

LOAD DATA LOCAL INFILE 'supplier.tbl' INTO TABLE supplier
  FIELDS TERMINATED BY '|' LINES TERMINATED BY '|\n';

LOAD DATA LOCAL INFILE 'part.tbl' INTO TABLE part
  FIELDS TERMINATED BY '|' LINES TERMINATED BY '|\n';

LOAD DATA LOCAL INFILE 'date.tbl' INTO TABLE `date`
  FIELDS TERMINATED BY '|' LINES TERMINATED BY '|\n';

LOAD DATA LOCAL INFILE 'lineorder.tbl' INTO TABLE lineorder
  FIELDS TERMINATED BY '|' LINES TERMINATED BY '|\n';

SET FOREIGN_KEY_CHECKS = 1;
EOF
```

## Verifying Data

```bash
# Using FusionLab CLI
fusionlab mysql "SELECT 'lineorder' as t, COUNT(*) as cnt FROM lineorder
UNION ALL SELECT 'customer', COUNT(*) FROM customer
UNION ALL SELECT 'supplier', COUNT(*) FROM supplier
UNION ALL SELECT 'part', COUNT(*) FROM part
UNION ALL SELECT 'date', COUNT(*) FROM \`date\`"

# Or individual tables
fusionlab mysql "SELECT COUNT(*) FROM lineorder"
```

## Running SSB Queries

After loading data, run the benchmark queries:

```bash
# Run a specific query
fusionlab mysql --file ../queries/q1.1.sql

# Run with EXPLAIN
fusionlab mysql --file ../queries/q1.1.sql --explain
```

## Troubleshooting

### "gcc not found"
```bash
sudo apt update && sudo apt install build-essential
```

### "dbgen compilation fails"
The setup script creates a compatible Makefile. If issues persist, manually check:
```bash
cd dbgen
make clean
make
```

### "local_infile" error during load
The MySQL VMs are configured with `local_infile=1`. If using Docker:
```bash
docker exec -it mysql mysql -u root -proot --local-infile=1 ssb
```

### Slow data loading
For large datasets (SF >= 10):
1. Increase MySQL buffer pool: `SET GLOBAL innodb_buffer_pool_size = 2147483648;`
2. Disable binary logging temporarily
3. Use parallel loading if available

## Using Existing Data

If you already have SSB data from another source (e.g., `github.com/wilhasse/courses/db/ssb`):

1. Copy the `.tbl` files to this directory
2. Run the load script:
   ```bash
   ../../infra/pulumi/load-data.sh 10.1.0.50 .
   ```
