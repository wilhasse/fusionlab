#!/bin/bash
# SSB Data Generator Setup Script
# Downloads and compiles dbgen from tidb-bench
#
# Usage: ./setup-dbgen.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DBGEN_DIR="$SCRIPT_DIR/dbgen"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log() {
    echo -e "${GREEN}[$(date '+%H:%M:%S')]${NC} $1"
}

warn() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# Check for required tools
check_requirements() {
    log "Checking requirements..."

    if ! command -v gcc &> /dev/null; then
        echo "ERROR: gcc is required but not installed"
        echo "Install with: sudo apt install build-essential"
        exit 1
    fi

    if ! command -v git &> /dev/null; then
        echo "ERROR: git is required but not installed"
        echo "Install with: sudo apt install git"
        exit 1
    fi

    log "All requirements satisfied"
}

# Download dbgen source
download_dbgen() {
    if [[ -d "$DBGEN_DIR" ]]; then
        warn "dbgen directory already exists at $DBGEN_DIR"
        read -p "Remove and re-download? [y/N] " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            rm -rf "$DBGEN_DIR"
        else
            log "Using existing dbgen directory"
            return
        fi
    fi

    log "Cloning tidb-bench repository (sparse checkout for ssb/dbgen only)..."

    # Use sparse checkout to only get ssb/dbgen
    git clone --filter=blob:none --sparse https://github.com/pingcap/tidb-bench.git "$DBGEN_DIR-temp"
    cd "$DBGEN_DIR-temp"
    git sparse-checkout set ssb/dbgen

    # Move dbgen to final location
    mv ssb/dbgen "$DBGEN_DIR"
    cd "$SCRIPT_DIR"
    rm -rf "$DBGEN_DIR-temp"

    log "Downloaded dbgen to $DBGEN_DIR"
}

# Compile dbgen
compile_dbgen() {
    log "Compiling dbgen..."
    cd "$DBGEN_DIR"

    # Create makefile if it doesn't exist or fix it
    if [[ ! -f "Makefile" ]] || ! grep -q "^CC" Makefile; then
        log "Creating Makefile..."
        cat > Makefile << 'EOF'
CC = gcc
DATABASE = SQLSERVER
CFLAGS = -g -DDBNAME=\"dss\" -D$(DATABASE) -DRNG_TEST -D_FILE_OFFSET_BITS=64
LDFLAGS = -O
LOAD = LOAD
PROGS = dbgen qgen

SRC = build.c driver.c bm_utils.c rnd.c print.c permute.c speed_seed.c text.c
OBJS = build.o driver.o bm_utils.o rnd.o print.o permute.o speed_seed.o text.o
QSRC = qgen.c varsub.c
QOBJS = qgen.o varsub.o bm_utils.o rnd.o permute.o speed_seed.o text.o

all: $(PROGS)

dbgen: $(OBJS)
	$(CC) $(LDFLAGS) -o $@ $^ -lm

qgen: $(QOBJS)
	$(CC) $(LDFLAGS) -o $@ $^ -lm

clean:
	rm -f *.o $(PROGS) *.tbl

.c.o:
	$(CC) $(CFLAGS) -c $<
EOF
    fi

    make clean 2>/dev/null || true
    make

    if [[ -f "dbgen" ]]; then
        log "dbgen compiled successfully!"
        chmod +x dbgen
    else
        echo "ERROR: dbgen compilation failed"
        exit 1
    fi

    cd "$SCRIPT_DIR"
}

# Main
log "=== SSB dbgen Setup ==="
check_requirements
download_dbgen
compile_dbgen

echo ""
log "=== Setup Complete ==="
echo ""
echo "Generate data with:"
echo "  ./generate.sh -s 1    # 1GB scale factor (~6M lineorder rows)"
echo "  ./generate.sh -s 10   # 10GB scale factor (~60M lineorder rows)"
echo ""
echo "Or manually:"
echo "  cd $DBGEN_DIR"
echo "  ./dbgen -s 1 -T a"
echo ""
