#!/bin/bash
# SSB Data Generator Script
# Generates SSB benchmark data using dbgen
#
# Usage: ./generate.sh [OPTIONS]
#   -s SCALE    Scale factor (1=1GB, 10=10GB, 100=100GB). Default: 1
#   -o DIR      Output directory for .tbl files. Default: current directory
#   -c          Clean existing .tbl files before generating
#   -h          Show this help

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DBGEN_BIN="$SCRIPT_DIR/dbgen/dbgen"

# Defaults
SCALE_FACTOR=1
OUTPUT_DIR="$SCRIPT_DIR"
CLEAN=false

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

log() {
    echo -e "${GREEN}[$(date '+%H:%M:%S')]${NC} $1"
}

info() {
    echo -e "${CYAN}$1${NC}"
}

usage() {
    echo "SSB Data Generator"
    echo ""
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -s SCALE    Scale factor (1=1GB, 10=10GB, 100=100GB). Default: 1"
    echo "  -o DIR      Output directory for .tbl files. Default: current directory"
    echo "  -c          Clean existing .tbl files before generating"
    echo "  -h          Show this help"
    echo ""
    echo "Scale factor reference:"
    echo "  SF=1   ~6M lineorder rows,   ~600MB total"
    echo "  SF=10  ~60M lineorder rows,  ~6GB total"
    echo "  SF=100 ~600M lineorder rows, ~60GB total"
    echo ""
    echo "Example:"
    echo "  $0 -s 1 -c              # Generate 1GB dataset, clean first"
    echo "  $0 -s 10 -o /data/ssb   # Generate 10GB dataset to /data/ssb"
    exit 0
}

# Parse arguments
while getopts "s:o:ch" opt; do
    case $opt in
        s) SCALE_FACTOR="$OPTARG" ;;
        o) OUTPUT_DIR="$OPTARG" ;;
        c) CLEAN=true ;;
        h) usage ;;
        *) usage ;;
    esac
done

# Validate scale factor
if ! [[ "$SCALE_FACTOR" =~ ^[0-9]+$ ]]; then
    echo "ERROR: Scale factor must be a positive integer"
    exit 1
fi

# Check if dbgen exists
if [[ ! -f "$DBGEN_BIN" ]]; then
    echo "ERROR: dbgen not found at $DBGEN_BIN"
    echo ""
    echo "Run setup first:"
    echo "  ./setup-dbgen.sh"
    exit 1
fi

# Ensure output directory exists
mkdir -p "$OUTPUT_DIR"

# Clean if requested
if [[ "$CLEAN" == true ]]; then
    log "Cleaning existing .tbl files in $OUTPUT_DIR..."
    rm -f "$OUTPUT_DIR"/*.tbl
fi

log "=== SSB Data Generation ==="
echo ""
info "Scale Factor:     $SCALE_FACTOR"
info "Output Directory: $OUTPUT_DIR"
echo ""

# Estimate sizes
case $SCALE_FACTOR in
    1)
        info "Estimated sizes:"
        info "  lineorder.tbl: ~600MB (~6M rows)"
        info "  customer.tbl:  ~2.4MB (~30K rows)"
        info "  part.tbl:      ~23MB (~200K rows)"
        info "  supplier.tbl:  ~200KB (~2K rows)"
        info "  date.tbl:      ~24KB (~2.5K rows)"
        ;;
    10)
        info "Estimated sizes:"
        info "  lineorder.tbl: ~6GB (~60M rows)"
        info "  customer.tbl:  ~24MB (~300K rows)"
        info "  part.tbl:      ~117MB (~1M rows)"
        info "  supplier.tbl:  ~2MB (~20K rows)"
        info "  date.tbl:      ~24KB (~2.5K rows)"
        ;;
    100)
        info "Estimated sizes:"
        info "  lineorder.tbl: ~60GB (~600M rows)"
        info "  customer.tbl:  ~240MB (~3M rows)"
        info "  part.tbl:      ~586MB (~1.4M rows)"
        info "  supplier.tbl:  ~20MB (~200K rows)"
        info "  date.tbl:      ~24KB (~2.5K rows)"
        ;;
    *)
        info "Estimated lineorder rows: ~$((SCALE_FACTOR * 6))M"
        ;;
esac
echo ""

# Generate data
log "Generating data (this may take a while for large scale factors)..."
cd "$SCRIPT_DIR/dbgen"

# Run dbgen
# -s: scale factor
# -T a: generate all tables
./dbgen -s "$SCALE_FACTOR" -T a

# Move generated files to output directory
if [[ "$OUTPUT_DIR" != "$SCRIPT_DIR/dbgen" ]]; then
    log "Moving .tbl files to $OUTPUT_DIR..."
    mv *.tbl "$OUTPUT_DIR/"
fi

# Show generated files
log "Generated files:"
echo ""
ls -lh "$OUTPUT_DIR"/*.tbl
echo ""

# Calculate total size
TOTAL_SIZE=$(du -ch "$OUTPUT_DIR"/*.tbl | grep total | awk '{print $1}')
log "Total data size: $TOTAL_SIZE"

# Show row counts
log "Row counts:"
for file in "$OUTPUT_DIR"/*.tbl; do
    filename=$(basename "$file" .tbl)
    rows=$(wc -l < "$file")
    printf "  %-15s %'d rows\n" "$filename:" "$rows"
done

echo ""
log "=== Data Generation Complete ==="
echo ""
echo "Next steps:"
echo "  1. Load data into MySQL:"
echo "     ../infra/pulumi/load-data.sh 10.1.0.50 $OUTPUT_DIR"
echo ""
echo "  2. Or copy files to VM and load manually:"
echo "     scp $OUTPUT_DIR/*.tbl ubuntu@10.1.0.50:~/"
echo "     ssh ubuntu@10.1.0.50 'mysql -u root -proot --local-infile=1 ssb'"
echo ""
