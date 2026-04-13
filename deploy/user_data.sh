#!/bin/bash
set -euo pipefail

# BigAl Sniper — EC2 Bootstrap Script
# Installs all dependencies, builds Rust executor, configures systemd services

exec > /var/log/bigal-bootstrap.log 2>&1
echo "=== BigAl bootstrap started at $(date) ==="

INSTANCE_NAME="${instance_name}"
AWS_REGION="${aws_region}"
JITO_BLOCK_ENGINE="${jito_block_engine}"
APP_DIR="/opt/bigal"
APP_USER="bigal"

# ──────────────────────────────────────────
#  Swap (2GB — critical for t3.micro with 1GB RAM)
# ──────────────────────────────────────────

fallocate -l 2G /swapfile
chmod 600 /swapfile
mkswap /swapfile
swapon /swapfile
echo '/swapfile none swap sw 0 0' >> /etc/fstab

# ──────────────────────────────────────────
#  System packages
# ──────────────────────────────────────────

apt-get update -y
apt-get install -y \
  build-essential pkg-config libssl-dev \
  python3 python3-pip python3-venv \
  curl git jq awscli

# ──────────────────────────────────────────
#  Node.js 20 LTS (for gRPC sidecar)
# ──────────────────────────────────────────

curl -fsSL https://deb.nodesource.com/setup_20.x | bash -
apt-get install -y nodejs

# ──────────────────────────────────────────
#  Rust toolchain
# ──────────────────────────────────────────

curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source "$HOME/.cargo/env"

# ──────────────────────────────────────────
#  Application user + directory
# ──────────────────────────────────────────

useradd -r -m -s /bin/bash $APP_USER || true
mkdir -p $APP_DIR
chown $APP_USER:$APP_USER $APP_DIR

# ──────────────────────────────────────────
#  Clone / copy application
#  (assumes repo is uploaded via scp or git clone after terraform apply)
# ──────────────────────────────────────────

cat > $APP_DIR/fetch-secrets.sh << 'FETCHEOF'
#!/bin/bash
# Fetch secrets from SSM Parameter Store and write to .env
REGION="$1"
NAME="$2"

get_param() {
  aws ssm get-parameter --region "$REGION" --name "/$NAME/$1" --with-decryption --query 'Parameter.Value' --output text
}

cat > /opt/bigal/.env << EOF
HELIUS_RPC_URL=$$(get_param HELIUS_RPC_URL)
HELIUS_API_KEY=$$(get_param HELIUS_API_KEY)
BIGAL_PRIVATE_KEY=$$(get_param BIGAL_PRIVATE_KEY)
JITO_BLOCK_ENGINE=$${JITO_BLOCK_ENGINE}
JITO_BUNDLE_URL=$${JITO_BLOCK_ENGINE}/api/v1/bundles
ZMQ_SIGNAL_PORT=5555
ZMQ_RESULT_PORT=5556
DEFAULT_PRIORITY_FEE=10000
RUST_LOG=bigal_executor=info
EOF

chmod 600 /opt/bigal/.env
chown bigal:bigal /opt/bigal/.env
echo "Secrets written to /opt/bigal/.env"
FETCHEOF
chmod +x $APP_DIR/fetch-secrets.sh

# ──────────────────────────────────────────
#  Setup script (run after code is deployed)
# ──────────────────────────────────────────

cat > $APP_DIR/setup.sh << 'SETUPEOF'
#!/bin/bash
set -euo pipefail
cd /opt/bigal

echo "=== Installing Python dependencies ==="
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install websockets aiohttp pyzmq solders solana base58 scikit-learn

echo "=== Building Rust executor ==="
source "$HOME/.cargo/env" 2>/dev/null || true
cd rust/bigal-executor
cargo build --release
cd /opt/bigal

echo "=== Installing sidecar dependencies ==="
cd sidecar
npm install --production
cd /opt/bigal

echo "=== Setup complete ==="
SETUPEOF
chmod +x $APP_DIR/setup.sh

# ──────────────────────────────────────────
#  systemd service — BigAl Bot
# ──────────────────────────────────────────

cat > /etc/systemd/system/bigal.service << 'SVCEOF'
[Unit]
Description=BigAl Sniper Bot
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=bigal
Group=bigal
WorkingDirectory=/opt/bigal
EnvironmentFile=/opt/bigal/.env
ExecStart=/opt/bigal/venv/bin/python bigalbot_customizable.py
Restart=on-failure
RestartSec=5
StandardOutput=journal
StandardError=journal
SyslogIdentifier=bigal

# Performance: prevent OOM killer from targeting us
OOMScoreAdjust=-500

# Security hardening
NoNewPrivileges=true
ProtectSystem=strict
ReadWritePaths=/opt/bigal /tmp

[Install]
WantedBy=multi-user.target
SVCEOF

systemctl daemon-reload

# ──────────────────────────────────────────
#  Kernel tuning for low-latency trading
# ──────────────────────────────────────────

cat >> /etc/sysctl.d/99-bigal.conf << 'KERNEOF'
# Network latency optimization
net.core.rmem_max = 16777216
net.core.wmem_max = 16777216
net.ipv4.tcp_rmem = 4096 87380 16777216
net.ipv4.tcp_wmem = 4096 65536 16777216
net.ipv4.tcp_low_latency = 1
net.ipv4.tcp_nodelay = 1
net.core.netdev_max_backlog = 5000

# Reduce swap pressure
vm.swappiness = 10
KERNEOF
sysctl --system

echo "=== BigAl bootstrap complete at $(date) ==="
echo "Next steps:"
echo "  1. Upload your code: scp -r ./* bigal@<IP>:/opt/bigal/"
echo "  2. SSH in: ssh bigal@<IP>"
echo "  3. Fetch secrets: sudo /opt/bigal/fetch-secrets.sh $AWS_REGION $INSTANCE_NAME"
echo "  4. Run setup: /opt/bigal/setup.sh"
echo "  5. Start: sudo systemctl enable --now bigal"
