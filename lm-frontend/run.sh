#!/bin/bash
# rebuild-docker-ubuntu.sh - Hapus container & image lama, lalu build ulang dan jalankan (versi Ubuntu)

# Konfigurasi
IMAGE_NAME="golang-prediction-app"
CONTAINER_NAME="golang-prediction-container"
PORT_HOST=3000
PORT_CONTAINER=3000
ENV_FILE=".env"

# Warna untuk output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}=== MEMBANGUN ULANG DOCKER UNTUK GOLANG APP (Ubuntu) ===${NC}"

# 1. Cek apakah Docker terinstal
if ! command -v docker &> /dev/null; then
    echo -e "${RED}Docker tidak ditemukan. Silakan instal Docker terlebih dahulu:${NC}"
    echo "  sudo apt update && sudo apt install docker.io"
    echo "  sudo systemctl start docker"
    echo "  sudo systemctl enable docker"
    exit 1
fi

# 2. Cek apakah user bisa menjalankan docker tanpa sudo
if ! docker ps &> /dev/null; then
    echo -e "${YELLOW}Perlu hak akses sudo untuk Docker. Menggunakan 'sudo' secara otomatis...${NC}"
    DOCKER_CMD="sudo docker"
else
    DOCKER_CMD="docker"
fi

# 3. Cek apakah file .env ada
if [ ! -f "${ENV_FILE}" ]; then
    echo -e "${RED}File ${ENV_FILE} tidak ditemukan di direktori ini!${NC}"
    echo "Buat file .env yang berisi konfigurasi yang diperlukan."
    exit 1
fi

# 4. Hentikan container jika sedang berjalan
if [ "$(${DOCKER_CMD} ps -q -f name=^/${CONTAINER_NAME}$)" ]; then
    echo "Menghentikan container ${CONTAINER_NAME}..."
    ${DOCKER_CMD} stop ${CONTAINER_NAME}
fi

# 5. Hapus container (jika ada, baik running maupun stopped)
if [ "$(${DOCKER_CMD} ps -aq -f name=^/${CONTAINER_NAME}$)" ]; then
    echo "Menghapus container ${CONTAINER_NAME}..."
    ${DOCKER_CMD} rm ${CONTAINER_NAME}
fi

# 6. Hapus image lama (force)
if [ "$(${DOCKER_CMD} images -q ${IMAGE_NAME})" ]; then
    echo "Menghapus image lama ${IMAGE_NAME}..."
    ${DOCKER_CMD} rmi -f ${IMAGE_NAME}
fi

# 7. Build image baru
echo "Membangun image baru ${IMAGE_NAME}..."
${DOCKER_CMD} build -t ${IMAGE_NAME} .

# 8. Jalankan container baru
echo "Menjalankan container ${CONTAINER_NAME}..."
${DOCKER_CMD} run -d \
  --name ${CONTAINER_NAME} \
  -p ${PORT_HOST}:${PORT_CONTAINER} \
  --env-file ${ENV_FILE} \
  ${IMAGE_NAME}

# 9. Cek status
if [ "$(${DOCKER_CMD} ps -q -f name=^/${CONTAINER_NAME}$)" ]; then
    echo -e "${GREEN}✅ Container berhasil dijalankan.${NC}"
    echo "Aplikasi dapat diakses di http://localhost:${PORT_HOST}"
    echo "Log container: ${DOCKER_CMD} logs ${CONTAINER_NAME}"
else
    echo -e "${RED}❌ Gagal menjalankan container. Periksa error dengan: ${DOCKER_CMD} logs ${CONTAINER_NAME}${NC}"
    exit 1
fi