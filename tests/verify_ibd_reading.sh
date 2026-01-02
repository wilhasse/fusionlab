#!/bin/bash
# Verify that FusionLab reads the same data as MySQL
# This script compares data from the .ibd file with live MySQL

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "========================================"
echo "FusionLab IBD Reading Verification Test"
echo "========================================"
echo

# Configuration
MYSQL_USER="${MYSQL_USER:-root}"
MYSQL_PASS="${MYSQL_PASS:-root}"
MYSQL_HOST="${MYSQL_HOST:-localhost}"
TEST_DB="fusionlab_verify"
TEST_TABLE="types_fixture"

IBD_PATH="/home/cslog/mysql/percona-parser/tests/types_test.ibd"
SDI_PATH="/home/cslog/mysql/percona-parser/tests/types_test_sdi.json"
PERCONA_BUILD="/home/cslog/mysql/percona-parser/build"

# Step 1: Create test table in MySQL with matching data
echo -e "${YELLOW}Step 1: Creating test table in MySQL...${NC}"

mysql -u"$MYSQL_USER" -p"$MYSQL_PASS" -h"$MYSQL_HOST" <<EOF
DROP DATABASE IF EXISTS $TEST_DB;
CREATE DATABASE $TEST_DB;
USE $TEST_DB;

CREATE TABLE $TEST_TABLE (
    id INT PRIMARY KEY,
    amount DECIMAL(10,2),
    d DATE,
    t TIME(6),
    dt DATETIME(6),
    ts TIMESTAMP NULL,
    y YEAR,
    e ENUM('small','medium','large'),
    s SET('red','green','blue'),
    b BIT(10),
    note VARCHAR(50)
);

-- Insert the same data that's in the test .ibd file
INSERT INTO $TEST_TABLE VALUES
(1, 1234.56, '2024-12-31', '12:34:56.123456', '2024-12-31 12:34:56.123456', '2024-12-31 12:34:56', 2024, 'medium', 'red,blue', b'1010101010', 'alpha'),
(2, -0.99, '2001-01-02', '01:02:03.000004', '2001-01-02 03:04:05.000006', NULL, 1999, 'small', 'green', b'0000000001', 'beta');
EOF

echo -e "${GREEN}✓ MySQL table created${NC}"
echo

# Step 2: Query MySQL
echo -e "${YELLOW}Step 2: Querying MySQL...${NC}"
echo
echo "MySQL Result:"
echo "-------------"
mysql -u"$MYSQL_USER" -p"$MYSQL_PASS" -h"$MYSQL_HOST" -t "$TEST_DB" <<EOF
SELECT id, amount, d, t, dt, ts, y, e, s, b+0 as b, note FROM $TEST_TABLE ORDER BY id;
EOF
echo

# Step 3: Query via FusionLab
echo -e "${YELLOW}Step 3: Querying via FusionLab (reading .ibd directly)...${NC}"
echo

# Run the Rust example
cd /home/cslog/mysql/fusionlab
export LD_LIBRARY_PATH="$PERCONA_BUILD:$LD_LIBRARY_PATH"

# Use cargo run with inline example
cargo run -p fusionlab-core --example ibd_verify -- "$IBD_PATH" "$SDI_PATH" 2>&1 | grep -v "^\[Debug\]" | grep -v "^discover_target"

echo
echo -e "${YELLOW}Step 4: Comparison${NC}"
echo "=================="
echo
echo "Key observations:"
echo "  • MySQL creates a new table with INSERT statements"
echo "  • FusionLab reads directly from the .ibd binary file"
echo "  • Both should show the same 2 rows with identical values"
echo
echo "If the data matches, FusionLab is correctly reading MySQL's InnoDB storage format!"
echo

# Cleanup
echo -e "${YELLOW}Cleaning up test database...${NC}"
mysql -u"$MYSQL_USER" -p"$MYSQL_PASS" -h"$MYSQL_HOST" -e "DROP DATABASE IF EXISTS $TEST_DB;"
echo -e "${GREEN}✓ Done${NC}"
