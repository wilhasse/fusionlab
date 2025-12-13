#!/bin/bash
# FusionLab MySQL Setup Script
# Run this on the VM after it's created
# Usage: ssh ubuntu@<VM_IP> 'bash -s' < setup-mysql.sh

set -e

echo "=== FusionLab MySQL Setup ==="

# Update system
echo "Updating system..."
sudo apt-get update
sudo apt-get upgrade -y

# Install MySQL 8.0
echo "Installing MySQL..."
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y mysql-server

# Configure MySQL for remote access and FusionLab
echo "Configuring MySQL..."

# Comment out the default bind-address to allow our config to take effect
sudo sed -i 's/^bind-address.*=.*127.0.0.1/# bind-address = 127.0.0.1/' /etc/mysql/mysql.conf.d/mysqld.cnf

# Add FusionLab configuration
sudo tee /etc/mysql/mysql.conf.d/fusionlab.cnf > /dev/null << 'EOF'
[mysqld]
bind-address = 0.0.0.0
local_infile = 1
innodb_buffer_pool_size = 1G
innodb_log_file_size = 256M
max_connections = 100
EOF

# Restart MySQL
sudo systemctl enable mysql
sudo systemctl restart mysql

# Create database and configure users
echo "Creating database and users..."
sudo mysql << 'EOSQL'
-- Create SSB database
CREATE DATABASE IF NOT EXISTS ssb;

-- Configure root for remote access
ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY 'root';
CREATE USER IF NOT EXISTS 'root'@'%' IDENTIFIED WITH mysql_native_password BY 'root';
GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION;

-- Create fusionlab user
CREATE USER IF NOT EXISTS 'fusionlab'@'%' IDENTIFIED BY 'fusionlab123';
GRANT ALL PRIVILEGES ON ssb.* TO 'fusionlab'@'%';

FLUSH PRIVILEGES;
EOSQL

# Allow MySQL through firewall (if UFW is active)
if command -v ufw &> /dev/null; then
    sudo ufw allow 3306/tcp 2>/dev/null || true
fi

# Print status
echo ""
echo "=== Setup Complete ==="
echo "MySQL is running and configured for remote access."
echo ""
echo "Connection details:"
echo "  Host: $(hostname -I | awk '{print $1}')"
echo "  Port: 3306"
echo "  User: root"
echo "  Password: root"
echo "  Database: ssb"
echo ""
echo "Test connection:"
echo "  mysql -h $(hostname -I | awk '{print $1}') -u root -proot ssb"
echo ""
echo "Next steps:"
echo "  1. Copy SSB schema: scp init.sql ubuntu@<IP>:~/"
echo "  2. Load schema: mysql -u root -proot ssb < ~/init.sql"
echo "  3. Load data (see data/generator/README.md)"
