set -e 
OS_RAW=$(uname -s)
ARCH=$(uname -m)

case "$OS_RAW" in
    Linux)
        OS="ubuntu-latest"
        ;;
    Darwin)
        OS="macos-latest"
        ;;
    *)
        echo "Error: Unsupported operating system '$OS_RAW'."
        echo "This script currently supports Linux and macOS."
        exit 1
        ;;
esac

GITHUB_REPO="<you>/dittofs"
DOWNLOAD_URL="https://github.com/${GITHUB_REPO}/releases/latest/download/dittofs-${OS}"
echo "Downloading DittoFS for your system (${OS_RAW})..."
# Note: Using sudo because /usr/local/bin is a system directory
curl -L "${DOWNLOAD_URL}" -o "dittofs-temp"
sudo mv dittofs-temp /usr/local/bin/dittofs

echo "Making DittoFS executable..."
sudo chmod +x /usr/local/bin/dittofs

echo "DittoFS installed successfully to /usr/local/bin/dittofs"