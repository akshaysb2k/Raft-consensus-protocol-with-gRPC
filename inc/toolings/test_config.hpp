#include <chrono>
#include <cstddef>
#include <cstdint>
#include <ctime>
#include <format>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <stdexcept>
#include <string>
#include <sys/types.h>
#include <thread>
#include <vector>

#include "common/common.hpp"
#include "common/config.hpp"
#include "toolings/test_ctrl.hpp"

#include "tester.pb.h"

#include <gtest/gtest.h>

namespace toolings {
using time_point = std::chrono::time_point<std::chrono::system_clock>;

constexpr uint64_t kTEST_TIMEOUT = 300; // seconds

static std::once_flag init_flag;

static std::string generate_random_string(size_t length) {
  // Character set to choose from (letters and digits)
  const std::string characters =
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

  std::random_device rd;
  // Random number generator
  std::default_random_engine rng(static_cast<unsigned int>(rd()));
  std::uniform_int_distribution<size_t> dist(0, characters.size() - 1);

  // Generate the random string
  std::string random_string;
  for (size_t i = 0; i < length; ++i) {
    random_string += characters[dist(rng)];
  }

  return random_string;
}

// ---------------------------------------------------------------------------
// MultiprocTestConfig public API behavior (testing infrastructure)
//
// Lifecycle / ownership:
//   - MultiprocTestConfig(...) constructs the controller (RaftTestCtrl),
//     registers the applier callback, and initializes in-memory commit
//     tracking. It does not start the control plane until begin().
//   - begin() must be called before using the rest of the API; it connects to
//     node testers (ctrl->run()) and starts a background timeout monitor.
//   - ~MultiprocTestConfig() calls cleanup() as best-effort teardown.
//
// Failure semantics (asserting helpers):
//   - Most helpers are written to fail-fast: on invariant violation or timeout
//     they emit logs, trigger GTest failures, and throw std::runtime_error to
//     abort the test body.
//   - Catching exceptions is optional; failures are already reported via GTest.
//
// Threading / reentrancy expectations:
//   - check_timeout() is intended to run on the background thread started by
//     begin().
//   - Apply results arrive on threads owned by RaftTestCtrl and feed into
//     check_logs() via the registered applier callback.
//   - Treat the class as not externally thread-safe; call methods from the
//     test thread unless you fully understand the concurrency implications.
//
// Method contracts:
//   - begin():
//       Starts the controller connections and begins timeout monitoring.
//   - check_timeout():
//       Runs until stopped; fails the test if an internal failure is observed
//       or if the overall test duration exceeds kTEST_TIMEOUT.
//   - cleanup():
//       Requests shutdown, joins the timeout thread (best-effort), then kills
//       all spawned raft_node processes via the controller. After cleanup(),
//       other APIs are not expected to make progress.
//   - check_logs(id, ApplyResult):
//       Updates commit tracking for (index,data) and checks cross-node
//       consistency. Returns {err_msg, prevok} where:
//         * err_msg is non-empty if different nodes committed different values
//           at the same index.
//         * prevok indicates whether this node had already committed (index-1)
//           when index>1 (used to detect out-of-order application).
//   - check_one_leader():
//       Polls cluster state and returns the current leader id if one exists;
//       fails if more than one leader is observed for any term, or if no leader
//       is found within the polling window.
//   - check_terms():
//       Returns the unanimous term across nodes; fails if nodes disagree.
//   - check_no_leader():
//       Fails if any node claims leadership.
//   - n_committed(index):
//       Returns {num,data} where num is how many nodes have a value for index;
//       fails on apply errors or mismatched committed values across nodes.
//   - wait(index, n, start_term):
//       Waits (bounded, with exponential backoff) until >=n nodes have committed
//       index and returns the committed data. If start_term is provided, returns
//       std::nullopt early if any node advances to a higher term (signaling that
//       the cluster changed terms while waiting).
//   - one(data, expected_servers, retry):
//       Submits a command until a leader accepts it and it is committed by
//       expected_servers nodes. If retry==false, fails after a single attempt;
//       if retry==true, retries for a bounded time window.
//   - disconnect(id) / reconnect(id):
//       Simulates network partition/heal for the given node id and updates the
//       internal “connected” filter used by other helpers.
//   - disconnect_all() / reconnect_all():
//       Partitions/heals the entire cluster.
//   - get_rpc_total_stats() / get_rpc_total_count() / get_rpc_total_bytes():
//       Returns aggregate RPC usage as reported by the controller.
//   - pick_n_servers(...):
//       Returns a randomized subset of node ids, optionally excluding some.
//   - propose(id, data):
//       Issues a proposal to a specific server and returns {index,term,is_leader};
//       throws if the id is not part of this config.
// ---------------------------------------------------------------------------
class MultiprocTestConfig {
public:
  MultiprocTestConfig(std::vector<rafty::Config> configs,
                      const std::string &node_path,
                      std::shared_ptr<spdlog::logger> logger,
                      std::optional<DDBConfig> ddb_conf = std::nullopt,
                      size_t fail_type = 0, size_t raft_node_verb = 0)
      : logger(logger), configs(configs) {
    std::call_once(init_flag, []() {
      if (std::thread::hardware_concurrency() < 2) {
        std::cout << "Warning: only one CPU, which may conceal locking bugs"
                  << std::endl;
      }
    });

    const std::string ctrl_addr = "0.0.0.0:55000";
    std::unordered_map<uint64_t, uint64_t> node_tester_ports;
    uint64_t tester_port = 55001;

    for (const auto &config : this->configs) {
      // initialize all data structures
      auto id = config.id;
      this->raft_ids.insert(id);
      this->logs[id] = {};
      this->apply_err[id] = "";
      this->connected[id] = true;

      this->raft_stopped[id] = true;
      node_tester_ports[id] = tester_port;
      tester_port++;
    }

    this->ctrl = std::make_unique<toolings::RaftTestCtrl>(
        configs, node_tester_ports, node_path, ctrl_addr, fail_type,
        raft_node_verb, this->logger, ddb_conf);

    this->ctrl->register_applier_handler(
        {[this](testerpb::ApplyResult m) -> void {
          auto i = m.id();
          if (this->stop.load() || this->failed.load() || this->raft_stopped[i])
            return;
          // auto apply_result = ready_queue.dequeue();
          auto apply_result = rafty::ApplyResult{
              .valid = m.valid(),
              .data = m.data(),
              .index = m.index(),
          };
          this->logger->info("ApplyResult: id={}, index={}, data={}, valid={}",
                             i, m.index(), m.data(), m.valid());
          if (this->stop.load() || this->failed.load() || this->raft_stopped[i])
            return;
          if (!apply_result.valid) {
            // ignore for now...
          } else {
            this->mtx.lock();
            auto [err_msg, preok] = this->check_logs(i, apply_result);
            this->mtx.unlock();
            if (apply_result.index > 1 && !preok) {
              err_msg = std::format("server {} apply out of order {}", i,
                                    apply_result.index);
            }
            if (!err_msg.empty()) {
              this->logger->critical("apply error: {}\n", err_msg);
              this->apply_err[i] = err_msg;
              this->failed.store(true);
              this->stop.store(true);
              FAIL() << "apply error: " << err_msg;
              throw std::runtime_error("apply error");
            }
          }
          return;
        }});

    this->start_time = std::chrono::system_clock::now();
    this->stop.store(false);
    this->failed.store(false);
  }

  ~MultiprocTestConfig() { this->cleanup(); }

  // begin must be invoked before any other methods
  inline void begin() {
    this->t0 = std::chrono::system_clock::now();
    this->rpcs0 = 0;
    this->cmds0 = 0;
    this->bytes0 = 0;
    this->max_index = 0;
    this->max_index_0 = 0;

    for (auto id : this->raft_ids) {
      this->raft_stopped[id] = false;
    }
    this->ctrl->run();

    this->timeout_thread = std::thread([this] { this->check_timeout(); });
  }

  //   inline void crash1(uint64_t i) {
  //     // TODO: disable connection

  //     this->mtx.lock();
  //     if (this->rafts.contains(i)) {
  //       this->mtx.unlock();
  //       this->rafts[i]->kill();
  //       this->ready_queues[i]->close();
  //       this->mtx.lock();
  //       this->rafts.erase(i);
  //       this->ready_queues.erase(i);
  //       this->connected[i] = false;
  //       this->raft_stopped[i] = true;
  //     }
  //     this->mtx.unlock();
  //     // this->stop.store(true);
  //   }

  //   inline void start1(uint64_t i) {
  //     this->crash1(i);
  //     this->mtx.lock();
  //     this->ready_queues[i] =
  //         std::make_unique<MessageQueue<rafty::ApplyResult>>(10000);
  //     this->rafts[i] = std::make_unique<toolings::RaftWrapper>(
  //         this->configs[i], *this->ready_queues[i]);

  //     if (verbosity < 2) {
  //       DisableLogging(i, false);
  //     }
  //     if (verbosity < 1) {
  //       DisableLogging(i, true);
  //     }
  //     // start the server
  //     this->rafts[i]->start_server();
  //     this->raft_stopped[i] = false;
  //     this->mtx.unlock();

  //     std::thread([this, i] {
  //       this->applier(i, *this->ready_queues[i]);
  //     }).detach();
  //   }

  inline void check_timeout() {
    while (true) {
      // Check stop signal FIRST for immediate exit
      if (this->stop.load()) {
        return;
      }

      if (this->failed.load()) {
        this->stop.store(true);
        FAIL() << "test failed";
        throw std::runtime_error("test failed");
      }

      if (!this->failed.load() &&
          std::chrono::system_clock::now() - this->start_time >
              std::chrono::seconds(kTEST_TIMEOUT)) {
        this->failed.store(true);
        std::string out =
            std::format("test took longer than {} seconds", kTEST_TIMEOUT);
        this->logger->critical(out);
        this->stop.store(true);
        FAIL() << out;
        throw std::runtime_error(out);
      }

      // Use shorter sleep with frequent stop checks for faster exit
      for (int i = 0; i < 10; i++) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        if (this->stop.load()) {
          return;
        }
      }
    }
  }

  inline void cleanup() {
    // Signal all threads to stop
    this->stop.store(true);

    // Wait for timeout thread to exit FIRST
    if (this->timeout_thread.joinable()) {
      // Give it time to see the stop signal and exit naturally
      auto join_start = std::chrono::system_clock::now();
      bool joined = false;
      while (!joined && std::chrono::system_clock::now() - join_start <
                            std::chrono::seconds(3)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        // Try to join with timeout
        if (this->timeout_thread.joinable()) {
          // The thread should exit on its own after seeing stop signal
          // Wait a bit more in the next iteration
        } else {
          joined = true;
        }
      }
      // Final join attempt
      if (this->timeout_thread.joinable()) {
        try {
          this->timeout_thread.join();
        } catch (...) {
          // Thread already joined or detached
        }
      }
    }

    // Now kill all processes
    this->ctrl->kill();
  }

  inline std::tuple<std::string, bool> check_logs(uint64_t i,
                                                  const rafty::ApplyResult &m) {
    std::string err_msg = "";
    auto v = m.data;
    for (auto &[id, log] : this->logs) {
      if (log.contains(m.index) && log[m.index] != v) {
        // this->logger->info(
        //     "{}: log {}; server {}", i, m.index, v, log[m.index]
        // );
        err_msg = std::format("commit index={} server={} {} != server={} {}",
                              m.index, i, m.data, id, log[m.index]);
      }
    }
    auto prevok =
        this->logs.contains(i) ? this->logs[i].contains(m.index - 1) : false;
    this->logs[i][m.index] = v;
    if (m.index > this->max_index) {
      this->max_index = m.index;
    }
    return {err_msg, prevok};
  }

  inline std::optional<uint64_t> check_one_leader() {
    for (auto iters = 0; iters < 10; iters++) {
      if (this->stop.load() || this->failed.load())
        throw std::runtime_error(
            "Abort due to test failure. See details above.");
      {
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<uint64_t> dist(450, 550);
        uint64_t random_number = dist(gen);
        std::this_thread::sleep_for(std::chrono::milliseconds(random_number));
      }

      std::unordered_map<uint64_t, std::vector<uint64_t>> leaders;
      auto states = this->ctrl->get_all_states(this->connected);
      for (const auto &state : states) {
        auto id = state.id();
        // if (!connected[id])
        //   continue;
        // auto state = raft->get_state();
        if (state.is_leader()) {
          if (!leaders.contains(state.term())) {
            leaders[state.term()] = {id};
          } else {
            leaders[state.term()].emplace_back(id);
          }
        }
      }

      auto last_term_with_leader = -1;
      for (auto &[term, ids] : leaders) {
        if (ids.size() > 1) {
          this->logger->critical("term {} has {} (>1) leaders", term,
                                 ids.size());
          EXPECT_FALSE(ids.size() > 1)
              << "term " << term << " has " << ids.size() << " (>1) leaders";
          throw std::runtime_error(
              "Abort due to test failure. See details above.");
        }
        if (static_cast<int>(term) > last_term_with_leader) {
          last_term_with_leader = static_cast<int>(term);
        }
      }

      if (!leaders.empty()) {
        this->logger->info("leader is node {} in term {}",
                           leaders[last_term_with_leader].front(),
                           last_term_with_leader);
        return leaders[last_term_with_leader].front();
      }
    }
    this->logger->critical("expected one leader, got none");
    EXPECT_TRUE(false) << "expected one leader, got none";
    throw std::runtime_error("Abort due to test failure. See details above.");
  }

  inline std::optional<uint64_t> check_terms() {
    std::optional<uint64_t> term = std::nullopt;
    auto states = this->ctrl->get_all_states(connected);
    // std::cout << sta
    for (const auto &state : states) {
      // auto id = state.id();
      // if (!connected[id])
      //   continue;
      auto xterm = state.term();
      if (term == std::nullopt) {
        term = xterm;
      } else if (term != xterm) {
        this->logger->critical("servers disagree on term");
        EXPECT_FALSE(term != xterm) << "servers disagree on term";
        throw std::runtime_error(
            "Abort due to test failure. See details above.");
      }
    }
    return term;
  }

  inline void check_no_leader() {
    auto states = this->ctrl->get_all_states(connected);
    for (const auto &state : states) {
      auto id = state.id();
      // if (!connected[id])
      //   continue;
      if (state.is_leader()) {
        this->logger->critical("expected no leader, but {} claims to be leader",
                               id);
        ASSERT_TRUE(false) << "expected no leader, but " << id
                           << " claims to be leader";
        throw std::runtime_error(
            "Abort due to test failure. See details above.");
      }
    }
  }

  struct CommittedCheck {
    uint64_t num;
    std::string data;
  };

  inline CommittedCheck n_committed(uint64_t i) {
    uint64_t count = 0;
    std::string data = "";
    for (auto id : this->raft_ids) {
      if (this->apply_err.contains(id) && !this->apply_err[id].empty()) {
        this->logger->critical("server {} apply error: {}", id,
                               this->apply_err[id]);
        // TODO: fail the test
        EXPECT_TRUE(false) << "server " << id
                           << " apply error: " << this->apply_err[id];
        this->stop.store(true);
        this->failed.store(true);
        throw std::runtime_error(
            "Abort due to test failure. See details above.");
      }
      std::lock_guard<std::mutex> lock(this->mtx);
      auto ok = this->logs[id].contains(i);
      if (ok) {
        auto data1 = this->logs[id][i];
        if (count > 0 && data != data1) {
          this->logger->critical(
              "committed values do not match: index {}, {}, {}", i, data,
              data1);
          EXPECT_TRUE(false) << "committed values do not match: index " << i
                             << ", " << data << ", " << data1;
          this->stop.store(true);
          this->failed.store(true);
          throw std::runtime_error(
              "Abort due to test failure. See details above.");
        }
        count++;
        data = data1;
      }
    }
    return {count, data};
  }

  inline std::optional<std::string>
  wait(uint64_t index, uint64_t n,
       std::optional<uint64_t> start_term = std::nullopt) {
    auto to = std::chrono::milliseconds(10);
    for (uint64_t iters = 0; iters < 30; iters++) {
      auto committed = n_committed(index);
      if (committed.num >= n) {
        break;
      }
      std::this_thread::sleep_for(to);
      if (this->stop.load() || this->failed.load()) {
        throw std::runtime_error(
            "Abort due to test failure. See details above.");
      }
      if (to < std::chrono::seconds(1)) {
        to *= 2;
      }
      if (start_term) {
        auto states = this->ctrl->get_all_states();
        for (const auto &state : states) {
          if (state.term() > *start_term) {
            return std::nullopt;
          }
        }
      }
    }
    auto committed = n_committed(index);
    if (committed.num < n) {
      this->logger->critical("only {} decided for index {}; wanted {}",
                             committed.num, index, n);
      EXPECT_FALSE(committed.num < n)
          << "only " << committed.num << " decided for index " << index
          << "; wanted " << n;
      this->stop.store(true);
      this->failed.store(true);
      throw std::runtime_error("Abort due to test failure. See details above.");
    }
    return committed.data;
  }

  inline std::optional<uint64_t> one(std::string data,
                                     uint64_t expected_servers, bool retry) {
    auto t0 = std::chrono::system_clock::now();
    while (std::chrono::system_clock::now() - t0 < std::chrono::seconds(25)) {
      std::optional<uint64_t> index = std::nullopt;
      this->mtx.lock();
      // this->ctrl->propose_to_one(uint64_t id, const std::string &data)
      auto rs = this->ctrl->propose_to_all(data, this->connected);
      for (auto &r : rs) {
        if (r.is_leader()) {
          index = r.index();
          break;
        }
      }
      // for (auto id: this->raft_ids) {
      //   if (!this->connected[id])
      //     continue;
      //   auto p_result = this->ctrl->propose_to_one(id, data);
      //   // assumption: this function will always return a result
      //   auto result = p_result.value();
      //   if (result.is_leader()) {
      //     index = result.index();
      //     break;
      //   }
      // }
      this->mtx.unlock();

      // somebody claimed to be the leader and to have
      // submitted our command; wait a while for agreement
      if (index) {
        auto t1 = std::chrono::system_clock::now();
        while (std::chrono::system_clock::now() - t1 <
               std::chrono::seconds(5)) {
          auto committed = n_committed(*index);
          if (committed.num > 0 && committed.num >= expected_servers) {
            // committed
            if (committed.data == data) {
              // command check passed.
              return index;
            }
          }
          std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        if (!retry) {
          this->logger->critical("one ({}) failed to reach agreement", data);
          // TODO: fail the test
          EXPECT_TRUE(false)
              << "one (" << data << ") failed to reach agreement";
          this->stop.store(true);
          this->failed.store(true);
          throw std::runtime_error(
              "Abort due to test failure. See details above.");
        }
      } else {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
      }
      if (this->stop.load() || this->failed.load()) {
        throw std::runtime_error(
            "Abort due to test failure. See details above.");
      }
    }
    this->logger->critical("one ({}) failed to reach agreement", data);
    EXPECT_TRUE(false) << "one (" << data << ") failed to reach agreement";
    this->stop.store(true);
    this->failed.store(true);
    throw std::runtime_error("Abort due to test failure. See details above.");
  }

  inline void disconnect(uint64_t id) {
    if (this->raft_ids.contains(id)) {
      this->logger->info("disconnect {}", id);
      this->ctrl->disconnect({id});
      this->connected[id] = false;
    }
  }

  inline void disconnect_all() {
    this->logger->info("disconnect all");
    this->ctrl->disconnect({this->raft_ids.begin(), this->raft_ids.end()});
    for (auto id : this->raft_ids) {
      this->connected[id] = false;
    }
  }

  inline void reconnect(uint64_t id) {
    if (this->raft_ids.contains(id)) {
      this->logger->info("reconnect {}", id);
      this->ctrl->reconnect({id});
      this->connected[id] = true;
    }
  }

  inline void reconnect_all() {
    this->logger->info("reconnect all");
    this->ctrl->reconnect({this->raft_ids.begin(), this->raft_ids.end()});
    for (auto id : this->raft_ids) {
      this->connected[id] = true;
    }
  }

  inline testerpb::RPCStats get_rpc_total_stats() {
    return this->ctrl->get_total_rpc_stats();
  }

  inline uint64_t get_rpc_total_count() {
    return this->get_rpc_total_stats().count();
  }

  inline uint64_t get_rpc_total_bytes() {
    return this->get_rpc_total_stats().bytes();
  }

  // helper functions for randomly picking n servers
  // returns ids of n servers
  // returned vector size is less than n if there are less than n servers
  inline std::vector<uint64_t> pick_n_servers(uint64_t n,
                                              std::set<uint64_t> exclude) {
    std::vector<uint64_t> picked;
    std::vector<uint64_t> ids;
    for (auto id : this->raft_ids) {
      if (exclude.find(id) != exclude.end())
        continue;
      ids.push_back(id);
    }
    std::random_device rd;
    std::mt19937 gen(rd());
    std::shuffle(ids.begin(), ids.end(), gen);
    for (uint64_t i = 0; i < n && i < ids.size(); i++) {
      picked.push_back(ids[i]);
    }
    return picked;
  }

  inline std::vector<uint64_t>
  pick_n_servers(uint64_t n, std::optional<uint64_t> exclude = std::nullopt) {
    std::set<uint64_t> exclude_set;
    if (exclude) {
      exclude_set.insert(*exclude);
    }
    return pick_n_servers(n, exclude_set);
  }

  inline rafty::ProposalResult propose(uint64_t id, const std::string &data) {
    if (this->raft_ids.contains(id)) {
      auto r = this->ctrl->propose_to_one(id, data).value();
      return {
          .index = r.index(),
          .term = r.term(),
          .is_leader = r.is_leader(),
      };
    }
    throw std::runtime_error("server not found");
  }

public:
  std::shared_ptr<spdlog::logger> logger;
  std::unique_ptr<toolings::RaftTestCtrl> ctrl;

private:
  std::mutex mtx;

  std::vector<rafty::Config> configs;
  std::unordered_map<uint64_t, std::unordered_map<uint64_t, std::string>> logs;
  std::unordered_map<uint64_t, std::string> apply_err;

  std::unordered_map<uint64_t, bool> raft_stopped;
  std::unordered_map<uint64_t, bool> connected;
  std::set<uint64_t> raft_ids;

  time_point start_time;
  time_point t0;
  uint64_t rpcs0;
  uint64_t cmds0;
  uint64_t bytes0;
  uint64_t max_index;
  uint64_t max_index_0;

  std::atomic<bool> stop;
  std::atomic<bool> failed;
  std::thread timeout_thread;
};
} // namespace toolings
