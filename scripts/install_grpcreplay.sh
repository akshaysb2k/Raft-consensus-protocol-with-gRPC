#!/usr/bin/env bash
set -e  # Exit on error

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_ok() {
    echo -e "${GREEN}[OK]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

# Check prerequisites
command -v go >/dev/null 2>&1 || {
    log_error "Go is not installed. Please install Go first."
    exit 1
}

command -v dpkg >/dev/null 2>&1 && {
    dpkg -l libpcap-dev >/dev/null 2>&1 || {
        log_error "libpcap-dev is not installed. Run: apt-get install libpcap-dev"
        exit 1
    }
} || {
    command -v yum >/dev/null 2>&1 && {
        yum list installed libpcap-devel >/dev/null 2>&1 || {
            log_error "libpcap-devel is not installed. Run: yum install libpcap-devel"
            exit 1
        }
    } || {
        log_warn "Skipping libpcap check (unsupported package manager)"
    }
}

# Clone repo
BUILD_DIR=$(mktemp -d /tmp/grpcreplay-build-XXXXXX)
log_info "Cloning grpcreplay to $BUILD_DIR..."
git clone https://github.com/vearne/grpcreplay.git "$BUILD_DIR"

# Build
cd "$BUILD_DIR"
log_info "Building grpcreplay..."
make build

# Verify binary exists
if [ ! -f "grpcr" ]; then
    log_error "Build failed: grpcr binary not found"
    exit 1
fi

# Install
log_info "Installing grpcr to \$HOME/.local/bin..."
mkdir -p "$HOME/.local/bin"
cp grpcr "$HOME/.local/bin/grpcr"
chmod +x "$HOME/.local/bin/grpcr"

# Verify installation
log_info "Verifying installation..."
"$HOME/.local/bin/grpcr" --help >/dev/null 2>&1 || {
    log_error "Installation verification failed"
    exit 1
}

log_ok "grpcreplay installed successfully"
echo ""
echo "You can now use 'grpcr' to record/replay gRPC traffic."
echo "Example usage:"
echo "  grpcr --input-raw=\"0.0.0.0:50050\" --output-stdout --record-response --codec=json"

# Cleanup
cd /  # Exit build dir before cleanup
rm -rf "$BUILD_DIR"
log_ok "Cleanup complete"
