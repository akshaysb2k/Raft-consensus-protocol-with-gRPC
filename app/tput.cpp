// tput.cpp -- multi-threaded throughput scaling benchmark for the KV service.
//
// Usage (from build/app/): ./tput <MaxClientCount> <PutRatio> [NumNodes]
//
//   MaxClientCount  -- run rounds with 1, 2, 4, … clients up to this limit
//   PutRatio        -- integer 0-100; percentage of operations that are Puts
//                      (the remainder are Gets)
//   NumNodes        -- cluster size (default: 3)
//
// Each round:
//   1. Spins up a fresh NumNodes-replica KV cluster on a dedicated port range.
//   2. Pre-populates keys key_1 … key_1000.
//   3. Launches <N> client threads each issuing 1000 synchronous requests
//      against a uniformly random key.
//   4. Records throughput (ops/sec) and latency statistics.
//   5. Tears down the cluster.
//
// Results are written to result.txt (3-node) or result_5rep.txt (5-node).

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <format>
#include <fstream>
#include <memory>
#include <mutex>
#include <numeric>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "common/logger.hpp"
#include "kv/kv_client.hpp"
#include "toolings/config_gen.hpp"
#include "toolings/test_ctrl.hpp"

static constexpr uint64_t KV_PORT_OFFSET         = 1000;
static constexpr int      REQUESTS_PER_CLIENT    = 1000;
static constexpr int      KEYSPACE_SIZE          = 1000;

// Per-round port layout:
//   Raft  : RAFT_BASE  + round * RAFT_STEP  …  + (num_nodes-1)
//   KV    : Raft + KV_PORT_OFFSET  (automatic)
//   Ctrl  : CTRL_BASE  + round * port_step
//   Tester: TESTER_BASE + round * port_step  …  + (num_nodes-1)
// port_step = num_nodes + 2 ensures no overlap between rounds.
static constexpr uint64_t RAFT_BASE   = 50160;
static constexpr uint64_t RAFT_STEP   = 10;
static constexpr uint64_t CTRL_BASE   = 55200;
static constexpr uint64_t TESTER_BASE = 55201;

// ---------------------------------------------------------------------------
// Result record for one round
// ---------------------------------------------------------------------------
struct RoundResult {
    int    client_count;
    double lat_avg;
    double lat_p50;
    double lat_p90;
    double lat_p99;
    double throughput; // ops/sec
};

// ---------------------------------------------------------------------------
// Spin up a num_nodes-replica cluster on ports derived from 'round_idx'.
// Fills kv_addrs with the client-facing KV addresses.
// ---------------------------------------------------------------------------
static std::unique_ptr<toolings::RaftTestCtrl>
create_cluster(int round_idx, int num_nodes,
               std::shared_ptr<spdlog::logger> logger,
               std::vector<std::string> &kv_addrs)
{
    uint64_t port_step   = static_cast<uint64_t>(num_nodes) + 2;
    uint64_t raft_base   = RAFT_BASE   + static_cast<uint64_t>(round_idx) * RAFT_STEP;
    uint64_t ctrl_port   = CTRL_BASE   + static_cast<uint64_t>(round_idx) * port_step;
    uint64_t tester_base = TESTER_BASE + static_cast<uint64_t>(round_idx) * port_step;

    auto insts = toolings::ConfigGen::gen_local_instances(
        static_cast<uint64_t>(num_nodes), raft_base);

    std::vector<rafty::Config>               configs;
    std::unordered_map<uint64_t, uint64_t>  node_tester_ports;
    kv_addrs.clear();

    uint64_t tp = tester_base;
    for (const auto &inst : insts) {
        std::map<uint64_t, std::string> peer_addrs;
        for (const auto &peer : insts) {
            if (peer.id == inst.id) continue;
            peer_addrs[peer.id] = peer.external_addr;
        }
        configs.push_back({.id = inst.id, .addr = inst.listening_addr,
                           .peer_addrs = peer_addrs});
        node_tester_ports[inst.id] = tp++;
        kv_addrs.push_back(
            "localhost:" + std::to_string(inst.port + KV_PORT_OFFSET));
    }

    std::string ctrl_addr = "0.0.0.0:" + std::to_string(ctrl_port);
    auto ctrl = std::make_unique<toolings::RaftTestCtrl>(
        configs, node_tester_ports, "./kv_node", ctrl_addr,
        /*fail_type=*/0, /*verbosity=*/0, logger);

    ctrl->register_applier_handler([](testerpb::ApplyResult m) { (void)m; });
    ctrl->run();

    // Allow leader election to settle
    std::this_thread::sleep_for(std::chrono::seconds(2));
    return ctrl;
}

// ---------------------------------------------------------------------------
// Pre-populate key_1 … key_<KEYSPACE_SIZE> so Gets always hit real keys.
// ---------------------------------------------------------------------------
static void populate_keyspace(kv::KvClient &client)
{
    for (int i = 1; i <= KEYSPACE_SIZE; i++) {
        client.put("key_" + std::to_string(i), "init");
    }
}

// ---------------------------------------------------------------------------
// Run one benchmark round with num_clients parallel clients.
// ---------------------------------------------------------------------------
static RoundResult run_round(int num_clients, int put_ratio,
                             const std::vector<std::string> &kv_addrs)
{
    // Pre-populate the keyspace before the timed phase
    {
        kv::KvClient setup_client(kv_addrs);
        populate_keyspace(setup_client);
    }

    std::mutex              result_mutex;
    std::vector<double>     all_latencies;
    all_latencies.reserve(static_cast<size_t>(num_clients) * REQUESTS_PER_CLIENT);

    std::atomic<int>  ready{0};
    std::atomic<bool> go{false};

    std::vector<std::thread> threads;
    threads.reserve(num_clients);
    for (int c = 0; c < num_clients; c++) {
        threads.emplace_back([&, c]() {
            kv::KvClient client(kv_addrs);

            // Warmup: force leader discovery off the timing path
            client.put("warmup_" + std::to_string(c), "v");

            // Signal readiness and wait for start signal
            ready.fetch_add(1);
            while (!go.load()) std::this_thread::yield();

            std::mt19937 rng(std::random_device{}() ^
                             static_cast<uint32_t>(c * 6364136223846793005ULL));
            std::uniform_int_distribution<int> key_dist(1, KEYSPACE_SIZE);
            std::uniform_int_distribution<int> op_dist(1, 100);

            std::vector<double> local_lats;
            local_lats.reserve(REQUESTS_PER_CLIENT);

            for (int i = 0; i < REQUESTS_PER_CLIENT; i++) {
                std::string key = "key_" + std::to_string(key_dist(rng));
                bool do_put = (op_dist(rng) <= put_ratio);

                auto t0 = std::chrono::steady_clock::now();
                if (do_put) {
                    client.put(key, "v");
                } else {
                    client.get(key);
                }
                auto t1 = std::chrono::steady_clock::now();

                local_lats.push_back(
                    std::chrono::duration<double, std::milli>(t1 - t0).count());
            }

            std::lock_guard<std::mutex> lk(result_mutex);
            all_latencies.insert(all_latencies.end(),
                                 local_lats.begin(), local_lats.end());
        });
    }

    // Wait for all threads to finish warmup, then start timer + release them
    while (ready.load() < num_clients) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    auto wall_start = std::chrono::steady_clock::now();
    go.store(true);

    for (auto &t : threads) t.join();

    auto wall_end  = std::chrono::steady_clock::now();
    double wall_s  = std::chrono::duration<double>(wall_end - wall_start).count();

    // -------------------------------------------------------------------------
    // Compute statistics
    // -------------------------------------------------------------------------
    std::sort(all_latencies.begin(), all_latencies.end());
    size_t n   = all_latencies.size();
    double avg = std::accumulate(all_latencies.begin(), all_latencies.end(), 0.0)
                 / static_cast<double>(n);
    double p50 = all_latencies[n * 50 / 100];
    double p90 = all_latencies[n * 90 / 100];
    double p99 = all_latencies[n * 99 / 100];
    double tput = static_cast<double>(num_clients * REQUESTS_PER_CLIENT) / wall_s;

    return {num_clients, avg, p50, p90, p99, tput};
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------
int main(int argc, char *argv[])
{
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <MaxClientCount> <PutRatio> [NumNodes]\n", argv[0]);
        return 1;
    }

    int max_clients = std::stoi(argv[1]);
    int put_ratio   = std::stoi(argv[2]);
    int num_nodes   = (argc >= 4) ? std::stoi(argv[3]) : 3;

    if (put_ratio < 0 || put_ratio > 100) {
        fprintf(stderr, "PutRatio must be 0-100\n");
        return 1;
    }
    if (num_nodes < 1) {
        fprintf(stderr, "NumNodes must be >= 1\n");
        return 1;
    }

    std::system("mkdir -p logs");
    rafty::utils::init_logger();

    auto logger_name = "tput_bench";
    auto logger = spdlog::get(logger_name);
    if (!logger) {
        logger = spdlog::basic_logger_mt(
            logger_name,
            std::format("logs/{}.log", logger_name),
            /*truncate=*/true);
    }

    std::vector<RoundResult> results;
    int round_idx = 0;

    for (int n = 1; n <= max_clients; n *= 2, ++round_idx) {
        fprintf(stderr, "[tput] round %d — %d client(s)...\n", round_idx, n);

        std::vector<std::string> kv_addrs;
        auto ctrl = create_cluster(round_idx, num_nodes, logger, kv_addrs);

        auto res = run_round(n, put_ratio, kv_addrs);
        results.push_back(res);

        fprintf(stderr,
                "[tput]   tput=%.0f ops/s  avg=%.2fms  p50=%.2fms  "
                "p90=%.2fms  p99=%.2fms\n",
                res.throughput, res.lat_avg, res.lat_p50,
                res.lat_p90, res.lat_p99);

        ctrl->kill();

        // Give OS a moment to reclaim ports before the next round
        std::this_thread::sleep_for(std::chrono::seconds(3));
    }

    // -------------------------------------------------------------------------
    // Write result file
    // -------------------------------------------------------------------------
    std::string result_file = (num_nodes == 5) ? "result_5rep.txt" : "result.txt";
    {
        std::ofstream out(result_file);
        out << "##################################################################################\n";
        out << "# clientCount      latAvg       latP50       latP90       latP99        throughput\n";
        out << "#                    (ms)         (ms)         (ms)         (ms)         (ops/sec)\n";
        out << "----------------------------------------------------------------------------------\n";
        for (const auto &r : results) {
            out << std::format(
                "{:>13}   {:>10.2f}   {:>10.2f}   {:>10.2f}   {:>10.2f}   {:>12.0f}\n",
                r.client_count, r.lat_avg, r.lat_p50,
                r.lat_p90, r.lat_p99, r.throughput);
        }
    }

    // -------------------------------------------------------------------------
    // Echo to stdout
    // -------------------------------------------------------------------------
    printf("##################################################################################\n");
    printf("# clientCount      latAvg       latP50       latP90       latP99        throughput\n");
    printf("#                    (ms)         (ms)         (ms)         (ms)         (ops/sec)\n");
    printf("----------------------------------------------------------------------------------\n");
    for (const auto &r : results) {
        printf("%13d   %10.2f   %10.2f   %10.2f   %10.2f   %12.0f\n",
               r.client_count, r.lat_avg, r.lat_p50,
               r.lat_p90, r.lat_p99, r.throughput);
    }

    fprintf(stderr, "[tput] results written to %s\n", result_file.c_str());
    return 0;
}
