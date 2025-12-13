import * as pulumi from "@pulumi/pulumi";
import * as proxmoxve from "@muhlba91/pulumi-proxmoxve";

// FusionLab MySQL VM
// Creates a VM with MySQL for SSB benchmark testing

const config = new pulumi.Config();
const sshPublicKey = config.get("sshPublicKey") || "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIGKmFzT3... your-key";

// MySQL setup script (runs on first boot via cloud-init)
const mysqlSetupScript = `#!/bin/bash
set -e

# Wait for cloud-init to complete
cloud-init status --wait

# Update system
apt-get update
apt-get upgrade -y

# Install MySQL 8.0
DEBIAN_FRONTEND=noninteractive apt-get install -y mysql-server

# Configure MySQL for remote access
cat > /etc/mysql/mysql.conf.d/fusionlab.cnf << 'EOF'
[mysqld]
bind-address = 0.0.0.0
local_infile = 1
innodb_buffer_pool_size = 1G
innodb_log_file_size = 256M
max_connections = 100
EOF

# Start MySQL
systemctl enable mysql
systemctl restart mysql

# Create database and user for FusionLab
mysql -u root << 'EOSQL'
CREATE DATABASE IF NOT EXISTS ssb;
CREATE USER IF NOT EXISTS 'fusionlab'@'%' IDENTIFIED BY 'fusionlab123';
GRANT ALL PRIVILEGES ON ssb.* TO 'fusionlab'@'%';
ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY 'root';
CREATE USER IF NOT EXISTS 'root'@'%' IDENTIFIED WITH mysql_native_password BY 'root';
GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION;
FLUSH PRIVILEGES;
EOSQL

# Log completion
echo "FusionLab MySQL setup complete" | tee /var/log/fusionlab-setup.log
`;

// Create the VM
const fusionlabMysql = new proxmoxve.vm.VirtualMachine("fusionlab-mysql", {
    nodeName: "pve1",
    name: "fusionlab-mysql",
    description: "FusionLab MySQL Server for SSB Benchmark",
    tags: ["pulumi", "fusionlab", "mysql"],

    // Clone from Ubuntu 24.04 template
    clone: {
        vmId: 9000,  // ubuntu24-cloud-template
        full: true,
        datastoreId: "pve1-ssd-1T-1",
    },

    // Resources
    cpu: {
        cores: 2,
        sockets: 1,
    },
    memory: {
        dedicated: 4096,  // 4GB RAM
    },

    // Disk - expand to 20GB for SSB data
    disks: [{
        interface: "scsi0",
        datastoreId: "pve1-ssd-1T-1",
        size: 20,  // 20GB
        fileFormat: "qcow2",
    }],

    // Network
    networkDevices: [{
        bridge: "vmbr0",
        model: "virtio",
    }],

    // Cloud-init configuration
    initialization: {
        type: "nocloud",
        datastoreId: "pve1-ssd-1T-1",

        userAccount: {
            username: "ubuntu",
            keys: [sshPublicKey],
        },

        // Use DHCP for simplicity
        ipConfigs: [{
            ipv4: {
                address: "dhcp",
            },
        }],

        dns: {
            servers: ["8.8.8.8", "8.8.4.4"],
        },

        // Run MySQL setup script on first boot
        userDataFileId: pulumi.interpolate``,  // We'll use vendorDataFileId instead
    },

    // QEMU agent for IP detection
    agent: {
        enabled: true,
    },

    // Start the VM
    started: true,
});

// Export VM information
export const vmId = fusionlabMysql.vmId;
export const vmName = fusionlabMysql.name;
export const vmNode = "pve1";

// Note: After VM is created, get IP with:
// ssh root@10.1.0.1 'qm guest cmd <VMID> network-get-interfaces'
// Or check DHCP leases / Proxmox UI
