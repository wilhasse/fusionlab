# SSB Data Generator

This guide explains how to generate Star Schema Benchmark (SSB) data for FusionLab.

## Prerequisites

- C compiler (gcc)
- MySQL client
- Docker (for MySQL container)

## Step 1: Clone and Build dbgen

```bash
# Clone the TiDB bench repository
git clone https://github.com/pingcap/tidb-bench.git
cd tidb-bench/ssb/dbgen

# Build dbgen
make
```

## Step 2: Generate Data

```bash
# Generate 1GB scale factor (good for development/testing)
./dbgen -s 1 -T a

# This creates:
# - customer.tbl (~2.4 MB at SF=1)
# - date.tbl     (~24 KB)
# - lineorder.tbl (~600 MB at SF=1)
# - part.tbl     (~23 MB at SF=1)
# - supplier.tbl (~200 KB)
```

Scale factor options:
- `-s 1` = ~600K lineorder rows (development)
- `-s 10` = ~60M lineorder rows (testing)
- `-s 100` = ~600M lineorder rows (production benchmarks)

## Step 3: Start MySQL

```bash
cd /path/to/fusionlab/docker
docker compose up -d

# Wait for MySQL to be ready
docker compose logs -f mysql
# Look for "ready for connections"
```

## Step 4: Load Data into MySQL

```bash
# Connect to MySQL
mysql -h 127.0.0.1 -u root -proot ssb

# Enable local infile if needed
SET GLOBAL local_infile = 1;

# Load dimension tables first
LOAD DATA LOCAL INFILE 'customer.tbl' INTO TABLE customer
FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';

LOAD DATA LOCAL INFILE 'supplier.tbl' INTO TABLE supplier
FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';

LOAD DATA LOCAL INFILE 'part.tbl' INTO TABLE part
FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';

LOAD DATA LOCAL INFILE 'date.tbl' INTO TABLE `date`
FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';

# Load fact table (largest)
LOAD DATA LOCAL INFILE 'lineorder.tbl' INTO TABLE lineorder
FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';
```

## Step 5: Verify Data

```bash
# Using FusionLab CLI
fusionlab mysql "SELECT COUNT(*) FROM lineorder"
fusionlab mysql "SELECT COUNT(*) FROM customer"
fusionlab mysql "SELECT COUNT(*) FROM supplier"
fusionlab mysql "SELECT COUNT(*) FROM part"
fusionlab mysql "SELECT COUNT(*) FROM \`date\`"
```

Expected row counts at SF=1:
- lineorder: ~6,001,171
- customer: ~30,000
- supplier: ~2,000
- part: ~200,000
- date: ~2,556

## Alternative: Use Your Existing SSB Data

If you already have SSB data generated from `github.com/wilhasse/courses/tree/main/db/ssb`, you can:

1. Copy the `.tbl` files to this directory
2. Follow Step 4 to load them into the FusionLab MySQL container

## Troubleshooting

### "local_infile" error
```sql
SET GLOBAL local_infile = 1;
```
Or add to docker-compose.yml: `--local-infile=1`

### Permission denied on files
Make sure the `.tbl` files are readable by the MySQL container user.

### Slow loading
For large datasets, consider:
- Disabling indexes before load, re-enabling after
- Increasing `innodb_buffer_pool_size`
- Using `LOAD DATA` with `CONCURRENT` option
