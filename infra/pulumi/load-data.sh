#!/bin/bash
# FusionLab SSB Data Loader
# Loads SSB .tbl files into the MySQL VM
#
# Usage: ./load-data.sh [VM_IP] [DATA_DIR]
#   VM_IP:   IP of MySQL VM (default: 10.1.0.50)
#   DATA_DIR: Directory containing .tbl files (default: /home/cslog/fusionlab/data/generator)

set -e

VM_IP="${1:-10.1.0.50}"
DATA_DIR="${2:-/home/cslog/fusionlab/data/generator}"
SSH_USER="ubuntu"

# Colors
GREEN='\033[0;32m'
NC='\033[0m'

log() {
    echo -e "${GREEN}[$(date '+%H:%M:%S')]${NC} $1"
}

log "=== Loading SSB Data into MySQL ==="
log "VM: $VM_IP"
log "Data directory: $DATA_DIR"

# Check for .tbl files
TBL_FILES=$(ls "$DATA_DIR"/*.tbl 2>/dev/null | wc -l)
if [[ $TBL_FILES -eq 0 ]]; then
    echo "ERROR: No .tbl files found in $DATA_DIR"
    echo ""
    echo "Generate data first with dbgen:"
    echo "  cd $DATA_DIR"
    echo "  ./dbgen -s 1 -T a"
    exit 1
fi

log "Found $TBL_FILES .tbl files"

# Copy files to VM
log "Copying data files to VM..."
scp "$DATA_DIR"/*.tbl "$SSH_USER@$VM_IP:~/"

# Load each table
log "Loading data into MySQL..."

ssh "$SSH_USER@$VM_IP" 'mysql -u root -proot --local-infile=1 ssb << EOF
-- Disable foreign key checks and autocommit for faster loading
SET FOREIGN_KEY_CHECKS = 0;
SET AUTOCOMMIT = 0;

-- Load dimension tables first
LOAD DATA LOCAL INFILE "customer.tbl" INTO TABLE customer FIELDS TERMINATED BY "|" LINES TERMINATED BY "|\n";
SELECT "Loaded customer table" AS status;

LOAD DATA LOCAL INFILE "supplier.tbl" INTO TABLE supplier FIELDS TERMINATED BY "|" LINES TERMINATED BY "|\n";
SELECT "Loaded supplier table" AS status;

LOAD DATA LOCAL INFILE "part.tbl" INTO TABLE part FIELDS TERMINATED BY "|" LINES TERMINATED BY "|\n";
SELECT "Loaded part table" AS status;

LOAD DATA LOCAL INFILE "date.tbl" INTO TABLE \`date\` FIELDS TERMINATED BY "|" LINES TERMINATED BY "|\n";
SELECT "Loaded date table" AS status;

-- Load fact table (largest)
LOAD DATA LOCAL INFILE "lineorder.tbl" INTO TABLE lineorder FIELDS TERMINATED BY "|" LINES TERMINATED BY "|\n";
SELECT "Loaded lineorder table" AS status;

COMMIT;
SET FOREIGN_KEY_CHECKS = 1;
EOF'

# Verify row counts
log "Verifying row counts..."
ssh "$SSH_USER@$VM_IP" 'mysql -u root -proot -e "
SELECT \"customer\" AS table_name, COUNT(*) AS rows FROM ssb.customer
UNION ALL SELECT \"supplier\", COUNT(*) FROM ssb.supplier
UNION ALL SELECT \"part\", COUNT(*) FROM ssb.part
UNION ALL SELECT \"date\", COUNT(*) FROM ssb.\`date\`
UNION ALL SELECT \"lineorder\", COUNT(*) FROM ssb.lineorder;
"'

# Clean up .tbl files on VM
log "Cleaning up temporary files on VM..."
ssh "$SSH_USER@$VM_IP" "rm -f ~/*.tbl"

log "=== Data Loading Complete ==="
echo ""
echo "Test queries:"
echo "  mysql -h $VM_IP -u root -proot ssb -e 'SELECT COUNT(*) FROM lineorder'"
echo ""
echo "Or with FusionLab CLI:"
echo "  fusionlab mysql 'SELECT COUNT(*) FROM lineorder' --host $VM_IP"
