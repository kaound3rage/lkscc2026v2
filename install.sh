#!/bin/bash
set -e

echo "=== Deteksi OS ==="
if [ -f /etc/os-release ]; then
    . /etc/os-release
    OS=$ID
    VER=$VERSION_ID
else
    echo "Tidak dapat mendeteksi OS."
    exit 1
fi

if [[ "$OS" != "amzn" ]]; then
    echo "OS bukan Amazon Linux. Ditemukan: $OS"
    exit 1
fi

echo "OS: $OS $VER"
echo ""

# --- Install Docker ---
echo "=== Install Docker ==="
if [[ "$VER" == "2" || "$VER" == 2.* ]]; then
    # Amazon Linux 2
    amazon-linux-extras install docker -y
    systemctl enable docker
    systemctl start docker
elif [[ "$VER" == "2023"* ]]; then
    # Amazon Linux 2023
    dnf install -y docker
    systemctl enable docker
    systemctl start docker
else
    echo "Versi Amazon Linux tidak didukung: $VER"
    exit 1
fi

# Tambahkan user (misal ec2-user) ke group docker jika ada
if id "ec2-user" &>/dev/null; then
    usermod -a -G docker ec2-user
    echo "User ec2-user ditambahkan ke group docker."
fi

echo "Docker berhasil diinstal."
echo ""

# --- Install Ollama ---
echo "=== Install Ollama ==="
curl -fsSL https://ollama.com/install.sh | sh

# Tunggu service Ollama siap
sleep 5

echo "=== Pull model qwen2.5:1.5b ==="
ollama pull qwen2.5:1.5b

echo ""
echo "=== INSTALLASI SELESAI ==="
echo "Docker dan Ollama siap digunakan."
echo "Model qwen2.5:1.5b sudah terunduh."
echo ""
echo "Jika Anda login sebagai ec2-user, jalankan 'newgrp docker' atau logout/login agar bisa menggunakan Docker tanpa sudo."