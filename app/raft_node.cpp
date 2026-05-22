#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <csignal>
#include <thread>
#include <poll.h>
#include <unistd.h>

#include <ddb/integration.hpp>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/strings/str_join.h"
#include "absl/strings/str_split.h"

#include "common/config.hpp"
#include "common/logger.hpp"
#include "common/utils/net_intercepter.hpp"
#ifdef TRACING
#include "common/utils/tracing.hpp"
#endif
#include "toolings/msg_queue.hpp"

#include "toolings/raft_wrapper.hpp"

struct PeerAddrs {
  std::map<uint64_t, std::string> values;
};

// Parsing function for the custom flag type
bool AbslParseFlag(absl::string_view text, PeerAddrs *out, std::string *error) {
  if (text.empty()) {
    return true;
  }
  // Split the input text on commas (or any other delimiter you prefer)
  std::vector<std::string> parts = absl::StrSplit(text, ',');
  for (const auto &part : parts) {
    // Split each part on the first colon to get the id and address
    std::vector<std::string> id_addr = absl::StrSplit(part, '+');
    if (id_addr.size() != 2) {
      *error = "Invalid peer address: " + part;
      return false;
    }
    // Parse the id and address
    uint64_t id;
    if (!absl::SimpleAtoi(id_addr[0], &id)) {
      *error = "Invalid peer id: " + id_addr[0];
      return false;
    }
    // Assign the id and address to the out value
    out->values[id] = id_addr[1];
  }
  return true;
}

// Unparsing function for the custom flag type
std::string AbslUnparseFlag(const PeerAddrs &flag) {
  // Join the vector of strings into a single comma-separated string
  std::vector<std::string> parts;
  for (const auto &[id, addr] : flag.values) {
    parts.emplace_back(std::to_string(id) + "+" + addr);
  }
  if (parts.empty()) {
    return "";
  }
  return absl::StrJoin(parts, ",");
}

ABSL_FLAG(
    uint16_t, id, 0,
    "id for the current raft node (must be unique across the raft cluster)");
ABSL_FLAG(uint16_t, port, 50051, "Server port for the service");
ABSL_FLAG(PeerAddrs, peers, PeerAddrs(), "a list of peer addresses");
ABSL_FLAG(int, verbosity, 1,
          "Verbosity level: 0 (silent), 1 (raft message (file sink only)), 2 "
          "(all message, file sink + console sink)");
ABSL_FLAG(int, fail_type, 0, "Failure Type: 0 (disonnection), 1 (partition)");

// used for multiprocess tester
ABSL_FLAG(bool, enable_ctrl, false, "Enable a test controller");
ABSL_FLAG(std::string, ctrl_addr, "localhost:55000",
          "Address for the test controller");
ABSL_FLAG(uint64_t, node_tester_port, 55001,
          "Port for the raft node tester that will listen to.");

// used by DDB
ABSL_FLAG(bool, ddb, false, "Enable DDB for debugging.");
ABSL_FLAG(std::string, ddb_host_ip, "127.0.0.1",
          "Host IP of this machine for DDB to connect to.");
ABSL_FLAG(bool, wait_for_attach, true,
          "Wait for DDB debugger to attach before proceeding.");

static std::unique_ptr<RaftWrapper> raft = nullptr;
std::atomic<bool> keep_running(true);

bool handle_cmd(std::string& command) {
  auto command_ = absl::StripAsciiWhitespace(command);
  std::vector<std::string> splits = absl::StrSplit(command_, " ");
  if (splits.empty())
    return false;
  if (splits.size() == 1) {
    auto cmd = splits.front();
    if (cmd == "r") {
      raft->connect_peers();
      raft->run();
    } else if (cmd == "k") {
      std::cout << "exiting..." << std::endl;
      raft->kill();
      raft->stop_server();
      return true;
    } else if (cmd.empty()) {
      return false;
    } else {
      std::cout << "Unknown command: " << cmd << std::endl;
    }
  } else if (splits.size() >= 2) {
    auto cmd = splits[0];
    std::vector<std::string> params = {splits.begin() + 1, splits.end()};
    if (cmd == "dis") {
      for (const auto &param : params) {
        rafty::NetInterceptor::disconnect(param);
      }
    } else if (cmd == "conn") {
      for (const auto &param : params) {
        rafty::NetInterceptor::reconnect(param);
      }
    } else {
      std::cout << "Unknown command: " << cmd << " ";
      for (const auto &param : params) {
        std::cout << param << " ";
      }
      std::cout << std::endl;
    }
  } else {
    std::cout << "Unknown command: " << command_ << std::endl;
  }
  return false;
}

void command_loop() {
  struct pollfd pfd;
  pfd.fd = STDIN_FILENO;
  pfd.events = POLLIN;

  std::string command;
  std::cout
      << "Enter commands: "
         "\n\tr \tconnect peers + run the raft instance, "
         "\n\tdis <id1{, id2}> \tdisconnect the raft node (specified by id) "
         "from the cluster, "
         "\n\tconn <id1{, id2}> \tconnect the raft node (specified by id) to "
         "the cluster, "
         "\n\tk \tkill the raft instance"
      << std::endl;

  std::cout << "> " << std::flush;
  while (keep_running.load(std::memory_order_relaxed)) {
    int ready = poll(&pfd, 1, 100); 
    if (ready > 0 && (pfd.revents & POLLIN)) {
        std::string cmd;
        if (std::getline(std::cin, cmd)) {
            if (handle_cmd(cmd)) {
              break;
            }
            if (keep_running.load(std::memory_order_relaxed)) {
              std::cout << "> " << std::flush;
            }
        } else {
          if (std::cin.eof()) {
            break; // End of file (e.g., Ctrl+D)
          }
          if (!keep_running.load(std::memory_order_seq_cst)) {
            break;
          }
          std::cin.clear(); // Clear error flags
          continue; // Try reading again
        }
    } else if (ready < 0) {
        if (errno == EINTR) continue; // Signal occurred
        break; 
    }
  }
  std::cout << "wait 1 seconds for cleanup." << std::endl;
  std::this_thread::sleep_for(std::chrono::seconds(1)); // wait for threads to finish
}

void prepare_logging(uint64_t id) {
  int verbosity = absl::GetFlag(FLAGS_verbosity);
  auto logger_name = std::format("rafty_node_{}", static_cast<uint64_t>(id));
  if (verbosity < 1) {
    rafty::utils::disable_console_logging(spdlog::get(logger_name), true);
  } else if (verbosity < 2) {
    rafty::utils::disable_console_logging(spdlog::get(logger_name), false);
  }
}

void prepare_interceptor(const std::set<std::string> &world) {
  // setup network interceptor
  rafty::NetInterceptor::setup_rank(world);
  int failure = absl::GetFlag(FLAGS_fail_type);
  if (failure == 0) {
    rafty::NetInterceptor::set_type(
        rafty::NetInterceptionType::NETWORK_FAILURE);
  } else if (failure == 1) {
    rafty::NetInterceptor::set_type(
        rafty::NetInterceptionType::NETWORK_PARTITION);
  } else {
    rafty::NetInterceptor::set_type(
        rafty::NetInterceptionType::NETWORK_FAILURE);
  }
}
          
void handle_sigint(int sig) {
  (void)sig;
  std::cout << "\nCaught SIGINT (Ctrl+C), cleaning up..." << std::endl;
  std::cout << "Killing raft instance..." << std::endl;
  if (raft) {
    raft->kill();
    raft->stop_server();
    raft.reset();
  }
  keep_running.store(false, std::memory_order_seq_cst);
}
          
int setup_signal() {
  struct sigaction sa;
  sa.sa_handler = handle_sigint;
  sigemptyset(&sa.sa_mask);
  sa.sa_flags = 0;
  
  if (sigaction(SIGINT, &sa, NULL) == -1) {
      perror("sigaction");
      return 1;
  }
  return 0;
}

int main(int argc, char **argv) {
  absl::ParseCommandLine(argc, argv);

  auto id = absl::GetFlag(FLAGS_id);
  std::string node_alias = "raft_node_" + std::to_string(id);
  auto port = absl::GetFlag(FLAGS_port);

  // optionally enable DDB
  if (absl::GetFlag(FLAGS_ddb)) {
    auto cfg = DDB::Config::get_default(absl::GetFlag(FLAGS_ddb_host_ip))
                   .with_alias(node_alias)
                   .with_logical_group(node_alias);
    cfg.wait_for_attach = absl::GetFlag(FLAGS_wait_for_attach);
    auto connector = DDB::DDBConnector(cfg);
    connector.init();
  }

  if (setup_signal() != 0) {
    return 1;
  }

  rafty::utils::init_logger();

#ifdef TRACING
  tracing::InitOtelInfra(node_alias);
#endif

  std::map<uint64_t, std::string> peers;
  if (!absl::GetFlag(FLAGS_peers).values.empty()) {
    peers = absl::GetFlag(FLAGS_peers).values;
  }

  auto addr = "0.0.0.0:" + std::to_string(port);

  rafty::Config config = {.id = id, .addr = addr, .peer_addrs = peers};

  std::set<std::string> world;
  world.insert(std::to_string(id));

  std::cout << "Raft node (id=" << id << ") starting at " << addr
            << " with peers: " << std::endl;
  for (const auto &[peer_id, addr] : peers) {
    std::cout << "\t" << peer_id << " " << addr << std::endl;
    world.insert(std::to_string(peer_id));
  }

  auto enable_tester = absl::GetFlag(FLAGS_enable_ctrl);
  toolings::MessageQueue<rafty::ApplyResult> ready;

  raft = [&]() {
    if (enable_tester) {
      auto ctrl_addr = absl::GetFlag(FLAGS_ctrl_addr);
      auto node_tester_port = absl::GetFlag(FLAGS_node_tester_port);
      return std::make_unique<toolings::RaftWrapper>(
          ctrl_addr, node_tester_port, config, ready);
    }
    return std::make_unique<toolings::RaftWrapper>(config, ready);
  }();

  prepare_interceptor(world);
  prepare_logging(id);

  raft->start_server();


  if (enable_tester) {
    // control via grpc tester controller
    raft->start_svr_loop();
  } else {
    // cli command loop
    command_loop();
  }
  raft.reset();
  return 0;
}
