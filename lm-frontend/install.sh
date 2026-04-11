#!/bin/bash
# install-docker-ubuntu.sh - Instalasi Docker di Ubuntu

set -e  # Hentikan script jika ada error

echo "=== Memulai instalasi Docker di Ubuntu ==="

# 1. Update sistem
echo "Updating system packages..."
sudo apt update -y

# 2. Install prerequisite packages
echo "Installing prerequisites..."
sudo apt install -y apt-transport-https ca-certificates curl software-properties-common

# 3. Add Docker's official GPG key
echo "Adding Docker's GPG key..."
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg

# 4. Add Docker repository
echo "Adding Docker repository..."
echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

# 5. Update apt again with Docker repo
sudo apt update -y

# 6. Install Docker Engine
echo "Installing Docker Engine..."
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

# 7. Start Docker service
echo "Starting Docker service..."
sudo systemctl start docker

# 8. Enable Docker on boot
echo "Enabling Docker to start on boot..."
sudo systemctl enable docker

# 9. Add current user to docker group
echo "Adding user $USER to docker group..."
sudo usermod -a -G docker $USER

# 10. Verifikasi instalasi
echo "Verifying installation..."
docker --version
docker ps

echo "==========================================="
echo "Instalasi selesai."
echo "Agar perubahan grup docker berlaku, silakan logout dan login kembali,"
echo "atau jalankan 'newgrp docker' pada terminal ini."
echo "==========================================="