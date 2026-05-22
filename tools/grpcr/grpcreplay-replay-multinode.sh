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

NUM_NODES=${1:-3}
START_PORT=${2:-50050}
START_CTRL_PORT=${3:-55001}
CAPTURE_BASE=${4:-"$SCRIPT_DIR/grpcreplay-captures"}
SPEED=${5:-1}

if ! command -v "$GRPCR_PATH" &> /dev/null; then
    log_error "grpcr not found. Run: $ROOT_DIR/scripts/install_grpcreplay.sh"
    exit 1
fi

# Check if capture directories exist
MISSING_CAPTURES=0

for i in $(seq 0 $((NUM_NODES - 1))); do
    CAPTURE_DIR="$CAPTURE_BASE/node$i"
    if [ ! -d "$CAPTURE_DIR" ]; then
        log_warn "Capture directory missing: $CAPTURE_DIR (will replay nothing)"
        MISSING_CAPTURES=1
    fi
done

if [ $MISSING_CAPTURES -eq $NUM_NODES ]; then
    log_error "No capture directories found. Run recording first."
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

log_info "Replaying captured traffic to $NUM_NODES Raft nodes..."
log_info "Capture source: $CAPTURE_BASE"
log_info "Replay speed: ${SPEED}x"
echo ""

function start_raft_node() {
    local node_id=$1
    local port=$2
    local node_target="127.0.0.1:$port"
    local ctrl_port=$3
    local ctrl_target="127.0.0.1:$ctrl_port"

    local node_ctrl_dir="$CAPTURE_BASE/node${node_id}_ctrl"
    mkdir -p "$node_ctrl_dir"
    local node_dir="$CAPTURE_BASE/node${node_id}"
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

for i in $(seq 0 $((NUM_NODES - 1))); do
    PORT=$((START_PORT + i))
    CTRL_PORT=$((START_CTRL_PORT + i))
    start_raft_node $i $PORT $CTRL_PORT
done

log_info "All replay processes started. Press Ctrl+C to stop."
wait
