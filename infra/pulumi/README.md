# FusionLab MySQL VM - Pulumi Infrastructure

This directory contains automation scripts to provision a MySQL VM on Proxmox for FusionLab SSB benchmarking.

## Quick Start

### Full Automated Deployment

```bash
cd /home/cslog/fusionlab/infra/pulumi

# Deploy VM + MySQL + Schema (takes ~5 minutes)
./deploy.sh

# Load SSB data (after generating with dbgen)
./load-data.sh
```

### Destroy VM

```bash
./deploy.sh --destroy
```

## Scripts

| Script | Description |
|--------|-------------|
| `deploy.sh` | Creates VM, installs MySQL, loads SSB schema |
| `setup-mysql.sh` | MySQL installation (called by deploy.sh) |
| `load-data.sh` | Loads SSB .tbl files into MySQL |

## Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  godev3/godev4  │     │   Proxmox pve1   │     │  fusionlab-mysql│
│  (Pulumi runs)  │────▶│   (API + Host)   │────▶│  VM: 10.1.0.50  │
└─────────────────┘     └──────────────────┘     └─────────────────┘
        │                                                │
        │ SSH after VM ready                             │
        └───────────────────────────────────────────────▶│
          - setup-mysql.sh                               │
          - init.sql (schema)                            │
          - *.tbl (data)                                 │
```

## What Gets Provisioned

**Via Pulumi (automatic):**
- Ubuntu 24.04 VM cloned from template
- 2 CPU cores, 4GB RAM, 20GB disk
- Static IP: 10.1.0.50
- SSH key configured
- QEMU agent enabled

**Via Scripts (after VM boots):**
- MySQL 8.0 server
- Remote access enabled (0.0.0.0)
- SSB database created
- Schema with 5 tables loaded

## Connection Details

| Property | Value |
|----------|-------|
| VM IP | 10.1.0.50 |
| SSH User | ubuntu |
| MySQL Port | 3306 |
| MySQL User | root |
| MySQL Password | root |
| Database | ssb |

## Manual Steps (if needed)

### 1. Just Create VM (Pulumi only)

```bash
cd ~/pulumi-proxmox-test
export PULUMI_CONFIG_PASSPHRASE="07farm"
pulumi up -s dev2
```

### 2. Just Install MySQL

```bash
ssh ubuntu@10.1.0.50 'bash -s' < setup-mysql.sh
```

### 3. Just Load Schema

```bash
scp /home/cslog/fusionlab/docker/init.sql ubuntu@10.1.0.50:~/
ssh ubuntu@10.1.0.50 "mysql -u root -proot ssb < ~/init.sql"
```

### 4. Generate & Load Data

```bash
# Generate SSB data (1GB scale factor)
cd /home/cslog/fusionlab/data/generator
./dbgen -s 1 -T a

# Load into MySQL
./load-data.sh 10.1.0.50
```

## Testing

```bash
# SSH to VM
ssh ubuntu@10.1.0.50

# Direct MySQL connection
mysql -h 10.1.0.50 -u root -proot ssb -e "SELECT COUNT(*) FROM lineorder"

# FusionLab CLI
cd /home/cslog/fusionlab
cargo run -- mysql "SELECT COUNT(*) FROM lineorder" --host 10.1.0.50
```

## Troubleshooting

### VM won't get IP

The network uses static IP (no DHCP). If IP isn't working:
```bash
# Check from Proxmox host
ssh root@10.1.0.1 "qm guest cmd 107 network-get-interfaces"

# Manually set IP
ssh root@10.1.0.1 "qm set 107 --ipconfig0 ip=10.1.0.50/23,gw=10.1.0.1"
```

### MySQL not accessible remotely

```bash
# Check bind address
ssh ubuntu@10.1.0.50 "sudo ss -tlnp | grep 3306"

# Should show 0.0.0.0:3306, not 127.0.0.1:3306
# If wrong, the fix is in setup-mysql.sh (comments out default bind-address)
```

### Pulumi state issues

```bash
cd ~/pulumi-proxmox-test
export PULUMI_CONFIG_PASSPHRASE="07farm"

# Check state
pulumi stack -s dev2

# Refresh from Proxmox
pulumi refresh -s dev2

# Force destroy stuck resources
pulumi destroy -s dev2 --target urn:pulumi:dev2::...
```

## Files

```
infra/pulumi/
├── README.md           # This file
├── deploy.sh           # Full deployment script
├── setup-mysql.sh      # MySQL setup (called by deploy.sh)
├── load-data.sh        # SSB data loader
└── index.ts            # Pulumi VM definition (reference)
```

The actual Pulumi project is at `~/pulumi-proxmox-test/` - the `index.ts` here is for reference.
