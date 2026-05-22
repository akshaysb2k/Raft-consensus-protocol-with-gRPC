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

if [ $# -lt 3 ]; then
    echo "Usage: $0 <node-id> <capture-dir> <target-address> [speed]"
    echo "Example: $0 0 ./grpcreplay-captures localhost:50050 10"
    exit 1
fi

NODE_ID=$1
CAPTURE_DIR=$2
PORT=$3
CTRL_PORT=$4
SPEED=${5:-1}

if ! command -v "$GRPCR_PATH" &> /dev/null; then
    log_error "grpcr not found. Run: $ROOT_DIR/scripts/install_grpcreplay.sh"
    exit 1
fi

sudo setcap cap_net_raw,cap_net_admin=eip "$GRPCR_PATH"

if [ ! -d "$CAPTURE_DIR" ]; then
    log_error "Capture directory not found: $CAPTURE_DIR"
    exit 1
fi

PIDS=()
cleanup() {
    log_info "Stopping all replay processes..."
    for pid in "${PIDS[@]}"; do
        kill $pid 2>/dev/null || true
    done
    exit 0
}
trap cleanup SIGINT SIGTERM

log_info "Replaying traffic from $CAPTURE_DIR on raft node port: $PORT and ctrl port: $CTRL_PORT"
log_info "Replay speed: ${SPEED}x"

function start_raft_node() {
    local node_id=$1
    local port=$2
    local node_target="127.0.0.1:$port"
    local ctrl_port=$3
    local ctrl_target="127.0.0.1:$ctrl_port"

    local node_ctrl_dir="$CAPTURE_DIR/node${node_id}_ctrl"
    mkdir -p "$node_ctrl_dir"
    local node_dir="$CAPTURE_DIR/node${node_id}"
    mkdir -p "$node_dir"
    
    log_info "Starting replayer for node ctrl ${node_id} on port ${ctrl_port}..."
    "$GRPCR_PATH" \
        --input-file-directory="$node_ctrl_dir" \
        --output-grpc="grpc://$ctrl_target" \
        --input-file-replay-speed="$SPEED" \
        --output-stdout \
        --codec=json \
        > "$node_ctrl_dir/replay-node${node_id}_ctrl.log" 2>&1 &
    PIDS+=($!)

    log_info "Starting replayer for node ${node_id} on port ${port}..."
    "$GRPCR_PATH" \
        --input-file-directory="$node_dir" \
        --output-grpc="grpc://$node_target" \
        --input-file-replay-speed="$SPEED" \
        --output-stdout \
        --codec=json \
        > "$node_dir/replay-node${node_id}.log" 2>&1 &
    PIDS+=($!)
}

start_raft_node $NODE_ID $PORT $CTRL_PORT

log_info "All replay processes started. Press Ctrl+C to stop."
wait

