// latency.cpp -- closed-loop unloaded latency benchmark for the KV service.
//
// Sends 1000 sequential Put requests to a 3-replica KV cluster and reports
// average, p50, and p99 latency in milliseconds.
//
// Usage (from build/app/): ./latency

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <format>
#include <memory>
#include <numeric>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "common/logger.hpp"
#include "kv/kv_client.hpp"
#include "toolings/config_gen.hpp"
#include "toolings/test_ctrl.hpp"

static constexpr uint64_t NUM_NODES        = 3;
static constexpr uint64_t RAFT_BASE_PORT   = 50150;
static constexpr uint64_t KV_PORT_OFFSET   = 1000;
static constexpr uint64_t CTRL_PORT        = 55100;
static constexpr uint64_t TESTER_BASE_PORT = 55101;
static constexpr int      NUM_REQUESTS     = 1000;

int main() {
    // Ensure logs directory exists
    std::system("mkdir -p logs");

    rafty::utils::init_logger();

    auto logger_name = "latency_bench";
    auto logger = spdlog::get(logger_name);
    if (!logger) {
        logger = spdlog::basic_logger_mt(
            logger_name,
            std::format("logs/{}.log", logger_name),
            /*truncate=*/true);
    }

    // -------------------------------------------------------------------------
    // Cluster setup
    // -------------------------------------------------------------------------
    auto insts = toolings::ConfigGen::gen_local_instances(NUM_NODES, RAFT_BASE_PORT);

    std::vector<rafty::Config>                configs;
    std::unordered_map<uint64_t, uint64_t>   node_tester_ports;
    std::vector<std::string>                  kv_addrs;
    uint64_t tester_port = TESTER_BASE_PORT;

    for (const auto &inst : insts) {
        std::map<uint64_t, std::string> peer_addrs;
        for (const auto &peer : insts) {
            if (peer.id == inst.id) continue;
            peer_addrs[peer.id] = peer.external_addr;
        }
        configs.push_back({.id = inst.id, .addr = inst.listening_addr,
                           .peer_addrs = peer_addrs});
        node_tester_ports[inst.id] = tester_port++;
        kv_addrs.push_back(
            "localhost:" + std::to_string(inst.port + KV_PORT_OFFSET));
    }

    const std::string ctrl_addr = "0.0.0.0:" + std::to_string(CTRL_PORT);
    auto ctrl = std::make_unique<toolings::RaftTestCtrl>(
        configs, node_tester_ports, "./kv_node", ctrl_addr,
        /*fail_type=*/0, /*verbosity=*/0, logger);

    ctrl->register_applier_handler([](testerpb::ApplyResult m) { (void)m; });
    ctrl->run();

    // Allow the cluster to elect a leader before benchmarking
    std::this_thread::sleep_for(std::chrono::seconds(2));

    // -------------------------------------------------------------------------
    // Benchmark: 1000 sequential Put operations
    // -------------------------------------------------------------------------
    kv::KvClient client(kv_addrs);

    std::vector<double> latencies;
    latencies.reserve(NUM_REQUESTS);

    for (int i = 0; i < NUM_REQUESTS; i++) {
        auto t0 = std::chrono::steady_clock::now();
        client.put("bench_key", "bench_value");
        auto t1 = std::chrono::steady_clock::now();
        latencies.push_back(
            std::chrono::duration<double, std::milli>(t1 - t0).count());
    }

    // -------------------------------------------------------------------------
    // Compute statistics
    // -------------------------------------------------------------------------
    std::sort(latencies.begin(), latencies.end());
    double avg = std::accumulate(latencies.begin(), latencies.end(), 0.0)
                 / static_cast<double>(latencies.size());
    double p50 = latencies[latencies.size() * 50 / 100];
    double p99 = latencies[latencies.size() * 99 / 100];

    // -------------------------------------------------------------------------
    // Print results in the required format
    // -------------------------------------------------------------------------
    printf("######################################\n");
    printf("#      latAvg       latP50      latP99\n");
    printf("#        (ms)         (ms)        (ms)\n");
    printf("--------------------------------------\n");
    printf("  %10.2f   %10.2f  %10.2f\n", avg, p50, p99);

    ctrl->kill();
    return 0;
}
