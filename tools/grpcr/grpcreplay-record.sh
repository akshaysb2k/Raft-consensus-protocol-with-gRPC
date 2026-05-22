#!/usr/bin/env bash
set -e

SCRIPT_DIR=$(realpath "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)")
ROOT_DIR=$(realpath "$SCRIPT_DIR/../..")

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

GRPCR_PATH="$HOME/.local/bin/grpcr"

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Check args
if [ $# -lt 3 ]; then
    echo "Usage: $0 <node-id> <port> <ctrl-port> [output-dir]"
    echo "Example: $0 0 50050 55001 ./captures"
    exit 1
fi

NODE_ID=$1
PORT=$2
CTRL_PORT=$3
OUTPUT_DIR=${4:-"$SCRIPT_DIR/grpcreplay-captures"}

# Check grpcr exists
if ! command -v "$GRPCR_PATH" &> /dev/null; then
    log_error "grpcr not found. Run: $ROOT_DIR/scripts/install_grpcreplay.sh"
    exit 1
fi

sudo setcap cap_net_raw,cap_net_admin=eip "$GRPCR_PATH"

mkdir -p "$OUTPUT_DIR"
log_info "Recording gRPC traffic on port $PORT..."
log_info "Output directory: $OUTPUT_DIR"

PIDS=()
cleanup() {
    log_info "Stopping all recorders..."
    for pid in "${PIDS[@]}"; do
        kill $pid 2>/dev/null || true
    done
    exit 0
}
trap cleanup SIGINT SIGTERM

function start_raft_node() {
    local node_id=$1
    local port=$2
    local ctrl_port=$3

    local node_ctrl_dir="$OUTPUT_DIR/node${node_id}_ctrl"
    mkdir -p "$node_ctrl_dir"
    local node_dir="$OUTPUT_DIR/node${node_id}"
    mkdir -p "$node_dir"
    
    log_info "Starting recorder for node ctrl ${node_id} on port ${ctrl_port}..."
    "$GRPCR_PATH" \
        --input-raw="127.0.0.1:$ctrl_port" \
        --output-file-directory="$node_ctrl_dir" \
        --record-response \
        --codec=json \
        > "$node_ctrl_dir/stdout.log" 2>&1 &
    PIDS+=($!)

    log_info "Starting recorder for node ${node_id} on port ${port}..."
    "$GRPCR_PATH" \
        --input-raw="127.0.0.1:$port" \
        --output-file-directory="$node_dir" \
        --record-response \
        --codec=json \
        > "$node_dir/stdout.log" 2>&1 &
    PIDS+=($!)
}

start_raft_node $NODE_ID $PORT $CTRL_PORT
log_info "Recorder started. Press Ctrl+C to stop."
wait
