#!/bin/bash
# FusionLab MySQL VM - Complete Deployment Script
# This script creates the VM, installs MySQL, and loads the SSB schema
#
# Usage: ./deploy.sh [--destroy]
#
# Prerequisites:
# - Pulumi installed and configured
# - SSH key set up
# - Access to Proxmox cluster

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FUSIONLAB_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
PULUMI_DIR="/home/cslog/pulumi-proxmox-test"
VM_IP="10.1.0.50"
SSH_USER="ubuntu"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log() {
    echo -e "${GREEN}[$(date '+%H:%M:%S')]${NC} $1"
}

warn() {
    echo -e "${YELLOW}[$(date '+%H:%M:%S')] WARNING:${NC} $1"
}

error() {
    echo -e "${RED}[$(date '+%H:%M:%S')] ERROR:${NC} $1"
    exit 1
}

# Handle --destroy flag
if [[ "$1" == "--destroy" ]]; then
    log "Destroying FusionLab MySQL VM..."
    cd "$PULUMI_DIR"
    export PULUMI_CONFIG_PASSPHRASE="${PULUMI_CONFIG_PASSPHRASE:-07farm}"
    pulumi destroy -y -s dev2
    log "VM destroyed."
    exit 0
fi

log "=== FusionLab MySQL VM Deployment ==="

# Step 1: Check prerequisites
log "Checking prerequisites..."

if ! command -v pulumi &> /dev/null; then
    error "Pulumi is not installed. Install it first."
fi

if [[ ! -d "$PULUMI_DIR" ]]; then
    error "Pulumi project not found at $PULUMI_DIR"
fi

if [[ ! -f "$FUSIONLAB_ROOT/docker/init.sql" ]]; then
    error "SSB schema not found at $FUSIONLAB_ROOT/docker/init.sql"
fi

# Step 2: Run Pulumi to create VM
log "Creating VM with Pulumi..."
cd "$PULUMI_DIR"
export PULUMI_CONFIG_PASSPHRASE="${PULUMI_CONFIG_PASSPHRASE:-07farm}"

pulumi up -y -s dev2

log "VM created. Waiting for boot..."

# Step 3: Wait for VM to be ready
log "Waiting for VM at $VM_IP to be reachable..."
MAX_WAIT=180
WAITED=0

while ! ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no "$SSH_USER@$VM_IP" "echo ready" &>/dev/null; do
    sleep 5
    WAITED=$((WAITED + 5))
    if [[ $WAITED -ge $MAX_WAIT ]]; then
        error "Timeout waiting for VM. Check Proxmox console."
    fi
    echo -n "."
done
echo ""
log "VM is reachable!"

# Step 4: Wait for cloud-init to complete
log "Waiting for cloud-init to complete..."
ssh -o StrictHostKeyChecking=no "$SSH_USER@$VM_IP" "cloud-init status --wait" || true

# Step 5: Install MySQL
log "Installing MySQL..."
ssh "$SSH_USER@$VM_IP" 'bash -s' < "$SCRIPT_DIR/setup-mysql.sh"

# Step 6: Load SSB schema
log "Loading SSB schema..."
scp "$FUSIONLAB_ROOT/docker/init.sql" "$SSH_USER@$VM_IP:~/init.sql"
ssh "$SSH_USER@$VM_IP" "mysql -u root -proot ssb < ~/init.sql"

# Step 7: Verify installation
log "Verifying installation..."
TABLES=$(ssh "$SSH_USER@$VM_IP" "mysql -u root -proot -N -e 'SHOW TABLES' ssb" | wc -l)
if [[ $TABLES -eq 5 ]]; then
    log "Schema verified: 5 tables created."
else
    warn "Expected 5 tables, found $TABLES"
fi

# Step 8: Print summary
echo ""
log "=== Deployment Complete ==="
echo ""
echo "MySQL VM Details:"
echo "  IP Address: $VM_IP"
echo "  SSH User:   $SSH_USER"
echo "  MySQL Port: 3306"
echo "  MySQL User: root"
echo "  MySQL Pass: root"
echo "  Database:   ssb"
echo ""
echo "Connect:"
echo "  ssh $SSH_USER@$VM_IP"
echo "  mysql -h $VM_IP -u root -proot ssb"
echo ""
echo "Test with FusionLab CLI:"
echo "  cd $FUSIONLAB_ROOT"
echo "  cargo run -- mysql 'SELECT COUNT(*) FROM customer' --host $VM_IP"
echo ""
echo "Next steps:"
echo "  1. Generate SSB data: cd $FUSIONLAB_ROOT/data/generator && ./generate.sh"
echo "  2. Load data: ./load-data.sh $VM_IP"
