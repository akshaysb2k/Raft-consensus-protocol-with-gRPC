#!/bin/bash

# Check if Docker is already installed
if command -v docker > /dev/null 2>&1; then
    echo "Docker is already installed"
    docker --version
    exit 0
fi

echo "Installing Docker..."

# Install prerequisites
sudo apt-get update
sudo apt-get install -y ca-certificates curl gnupg lsb-release

# Add Docker's official GPG key
sudo mkdir -p /etc/apt/keyrings
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc

sudo tee /etc/apt/sources.list.d/docker.sources <<EOF
Types: deb
URIs: https://download.docker.com/linux/ubuntu
Suites: $(. /etc/os-release && echo "${UBUNTU_CODENAME:-$VERSION_CODENAME}")
Components: stable
Signed-By: /etc/apt/keyrings/docker.asc
EOF

# Install Docker Engine, CLI, and plugins
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# Create docker group and add current user
sudo groupadd -f docker
sudo usermod -aG docker $USER

echo "Docker installed successfully"
docker --version
docker compose version

# Activate docker group immediately
echo "Activating docker group for current session..."
newgrp docker << 'EOF'
echo "Testing Docker in new group context..."
docker ps
docker run --rm hello-world
EOF

echo ""
echo "Docker installation complete!"
echo "Note: If you open a new terminal, the docker group will be active automatically."
echo "For this terminal session, you're now in a shell with docker group active."
