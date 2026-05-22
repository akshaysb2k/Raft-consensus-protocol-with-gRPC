# Raft Consensus + Distributed Key-Value Store

A production-style implementation of the **Raft consensus protocol** in **C++20**, with a fault-tolerant distributed key-value store built on top. Designed for correctness and performance under real-world failure conditions — node crashes, network partitions, and delayed RPC delivery.

---

## Overview

This project implements Raft from the ground up, including:

- **Leader election** with randomized election timeouts and term-based voting
- **Log replication** with AppendEntries RPCs, per-peer worker threads, and commit index tracking
- **Log compaction** and entry consistency enforcement on follower divergence
- **Lease-based reads** for linearizable read-only queries without log round-trips
- **Distributed KV store** layered over Raft, supporting `Put`, `Get`, and `Append` operations with client-side deduplication (RIFL)
- **Full observability stack** with OpenTelemetry tracing, Grafana + Loki + Tempo dashboards, and Jaeger distributed tracing
- **Latency and throughput benchmarks** with p50/p99 reporting and matplotlib plots

---

## Architecture

```
┌─────────────────────────────────────────────┐
│                 Client                      │
└─────────────────┬───────────────────────────┘
                  │ gRPC (KV service, port P+1000)
┌─────────────────▼───────────────────────────┐
│              KV Server                      │
│  Put / Get / Append  +  RIFL dedup table    │
└─────────────────┬───────────────────────────┘
                  │ propose() / on_apply()
┌─────────────────▼───────────────────────────┐
│             Raft Node                       │
│  Leader election  •  Log replication        │
│  Lease reads      •  Commit + apply queue   │
└──────┬──────────────────────────────┬───────┘
       │ AppendEntries / RequestVote   │
       │       gRPC (port P)          │
  ┌────▼────┐                   ┌─────▼───┐
  │  Peer 1 │  ←── Raft RPC ──→ │  Peer 2 │
  └─────────┘                   └─────────┘
```

Each node runs three servers:
- **Raft gRPC server** (port `P`) — inter-node consensus RPCs (`AppendEntries`, `RequestVote`)
- **KV gRPC server** (port `P+1000`) — client-facing key-value operations
- **Tester gRPC server** — test infrastructure control plane for fault injection

---

## Key Design Decisions

**Per-peer worker threads** — each peer has a dedicated worker thread with a condition variable, so replication to a slow or partitioned peer never blocks the leader's critical path.

**Lease-based reads** — the leader tracks the time at which it last received a quorum of heartbeat acknowledgements. If the lease is still valid, `Get` requests are served locally without an additional log round-trip, dramatically reducing read latency.

**RIFL deduplication** — each client operation carries a `(client_id, seq_num)` pair. The KV server maintains a per-client table of the last applied sequence number, making all operations exactly-once even under retries and crashes.

**Randomized election timeouts** — followers pick a random timeout in `[150ms, 300ms]` to minimize split-vote scenarios. Heartbeats fire every 50ms, keeping the leader-to-timeout ratio well below 1.

---

## Performance

Benchmarks run on a 3-node cluster (loopback):

| Metric | Result |
|---|---|
| Closed-loop unloaded Put latency (avg) | < 5ms |
| p99 Put latency | < 15ms |
| Leader re-election under crash | < 500ms |
| Uptime across simulated failure scenarios | 99.9%+ |

Latency/throughput curves are in [`bench/lat-tput.png`](bench/lat-tput.png).

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | C++20 |
| RPC | gRPC + Protocol Buffers |
| Build | CMake 3.22+ |
| Testing | GoogleTest (unit + integration) |
| Tracing | OpenTelemetry SDK |
| Dashboards | Grafana, Loki, Tempo, Alloy |
| Distributed tracing | Jaeger |
| Logging | spdlog |
| Benchmarking | Custom latency + throughput harness |

---

## Project Structure

```
.
├── src/
│   └── raft.cpp                  # Core Raft implementation
├── inc/
│   ├── rafty/raft.hpp            # Raft class interface
│   ├── kv/kv_server.hpp          # KV state machine + RIFL dedup
│   ├── kv/kv_client.hpp          # KV client library
│   └── toolings/                 # Test harness, config gen, msg queue
├── app/
│   ├── raft_node.cpp             # Raft node binary
│   ├── kv_node.cpp               # KV node binary (Raft + KV servers)
│   ├── latency.cpp               # Closed-loop latency benchmark
│   ├── tput.cpp                  # Throughput benchmark
│   └── multinode.cpp             # Multi-node orchestration
├── proto/
│   ├── raft.proto                # AppendEntries, RequestVote messages
│   ├── kv.proto                  # Put, Get, Append messages
│   └── tester.proto              # Test control plane messages
├── integration_tests/            # Full cluster integration tests
├── unittests/                    # Unit tests (msg queue, internals)
├── bench/                        # Latency/throughput plots
├── tools/
│   ├── grafana-suite/            # Grafana + Loki + Tempo + Alloy config
│   ├── jaeger-suite/             # Jaeger + OTel collector config
│   └── grpcr/                   # gRPC replay scripts for deterministic debugging
└── scripts/                      # Dependency installation scripts
```

---

## Getting Started

### Prerequisites

- `cmake` >= 3.22.1
- `g++` >= 13.1.0 (C++20 required)
- Docker (for observability stack)

### Install Dependencies

The setup script installs `grpc`, `googletest`, `spdlog`, and OpenTelemetry. gRPC binaries/headers go to `$HOME/.local`.

```bash
./setup.sh
```

Or install individually:

```bash
./scripts/install_grpc.sh
./scripts/install_gcc-13.sh
./scripts/install_otel.sh
```

### Build

```bash
mkdir build && cd build
cmake ..
make -j$(nproc)
```

### Run Tests

```bash
# From build/
./integration_tests/raft_test
./integration_tests/kv_test
./unittests/test_msg_queue
```

### Run Benchmarks

```bash
# From build/app/
./latency      # reports avg, p50, p99 latency for 1000 sequential Puts
./tput         # reports throughput under concurrent load
```

---

## Observability

### Grafana Stack (logs + metrics + traces)

```bash
cd tools/grafana-suite
docker compose up -d
```

Grafana UI available at `http://localhost:3000`. Alloy scrapes logs; Tempo stores traces; Loki aggregates.

### Jaeger (distributed tracing)

```bash
cd tools/jaeger-suite
docker compose up -d
```

Jaeger UI available at `http://localhost:16686`. Requires building with `-DTRACING=ON`.

---

## Testing Approach

**Unit tests** cover internal components like the message queue (thread-safe producer/consumer under concurrent load).

**Integration tests** spin up real multi-process Raft clusters and test:
- Basic leader election and re-election after crash
- Log replication correctness under concurrent proposals
- Safety under network partitions (no two leaders in the same term)
- Liveness — cluster recovers and makes progress after majority returns
- KV linearizability under concurrent client operations and node failures

Fault injection uses a network interceptor that can drop, delay, or reorder gRPC calls deterministically. The `grpcr` tooling supports recording and replaying RPC sequences for reproducible debugging.

---

## Fault Tolerance Guarantees

- **Safety**: at most one leader per term; committed entries are never lost
- **Liveness**: cluster makes progress as long as a majority of nodes are reachable
- **Exactly-once KV semantics**: RIFL deduplication ensures no double-applies on retry

---

## Authors

Akshay Shivashankar Bharadwaj & Karthik — USC CS, Distributed Systems (Spring 2026)
