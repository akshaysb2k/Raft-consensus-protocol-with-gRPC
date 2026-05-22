#include <cstdint>
#include <memory>
#include <unordered_map>
#include <vector>
#include <csignal>
#include <poll.h>
#include <unistd.h>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"

#include "common/config.hpp"
#include "common/logger.hpp"
#include "toolings/config_gen.hpp"
#include "toolings/test_ctrl.hpp"

constexpr std::string_view node_path = "./raft_node";
constexpr std::string_view ctrl_addr = "0.0.0.0:55000";

ABSL_FLAG(uint64_t, num, 3, "number of nodes to spawn (>= 3)");
ABSL_FLAG(std::string, bin, "./raft_node", "the binary of raft node app");
ABSL_FLAG(int, verbosity, 1,
          "Verbosity level: 0 (silent), 1 (raft message (file sink only))");
ABSL_FLAG(int, fail_type, 0, "Failure Type: 0 (disonnection), 1 (partition)");

// used by DDB
ABSL_FLAG(bool, ddb, false, "Enable DDB for debugging.");
ABSL_FLAG(std::string, ddb_host_ip, "127.0.0.1",
          "Host IP of this machine for DDB to connect to.");
ABSL_FLAG(bool, wait_for_attach, true,
          "Wait for DDB debugger to attach before proceeding.");
ABSL_FLAG(bool, ddb_app_wrapper, false,
          "Use DDB app runner wrapper to start raft node (supports PET).");

static std::unique_ptr<toolings::RaftTestCtrl> ctrl = nullptr;
std::atomic<bool> keep_running(true);
          
void handle_sigint(int sig) {
  (void)sig;
  std::cout << "\nCaught SIGINT (Ctrl+C), cleaning up..." << std::endl;
  std::cout << "Killing raft test controller..." << std::endl;
  if (ctrl) {
    ctrl->kill();
    ctrl.reset();
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

bool handle_cmd(std::string& command) {
  auto command_ = absl::StripAsciiWhitespace(command);
  std::vector<std::string> splits = absl::StrSplit(command_, " ");
  if (splits.empty())
    return false;
  if (splits.size() == 1) {
    auto cmd = splits.front();
    if (cmd == "r") {
      ctrl->run();
    } else if (cmd == "k") {
      ctrl->kill();
      return true;
    } else if (cmd.empty()) {
      return false;
    } else {
      std::cout << "Unknown command: " << cmd << std::endl;
    }
  } else if (splits.size() >= 2) {
    auto cmd = splits[0];
    if (cmd == "dis" || cmd == "conn") {
      std::vector<uint64_t> ids;
      for (auto begin = splits.begin() + 1; begin != splits.end(); begin++) {
        ids.push_back(std::stoul(*begin));
      }

      if (cmd == "dis") {
        ctrl->disconnect(ids);
      } else {
        ctrl->reconnect(ids);
      }
      return false;
    }

    if (cmd == "prop") {
      std::string data = splits[1];
      std::cout << "proposing data = " << data << std::endl;
      ctrl->propose_to_all(data);
      return false;
    }

    std::cout << "Unknown command: " << command_ << std::endl;
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
         "\n\tr \tstart multinode raft cluster, "
         "\n\tdis <id1{, id2}> \tdisconnect a raft node from the cluster, "
         "\n\tconn <id1{, id2}> \treconnect a raft node into the cluster, "
         "\n\tprop <data> \tpropose data to the cluster, "
         "\n\tk \tkill the multinode raft cluster"
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
  std::cout << "exiting..." << std::endl;
}

int main(int argc, char **argv) {
  if (setup_signal() != 0) {
    return 1;
  }
  absl::ParseCommandLine(argc, argv);
  rafty::utils::init_logger();

  auto num = absl::GetFlag(FLAGS_num);
  auto binary_path = absl::GetFlag(FLAGS_bin);
  auto fail_type = absl::GetFlag(FLAGS_fail_type);
  auto verbosity = absl::GetFlag(FLAGS_verbosity);

  // Initialize DDB Config
  toolings::DDBConfig ddb_conf{
      .enable_ddb = absl::GetFlag(FLAGS_ddb),
      .ddb_host_ip = absl::GetFlag(FLAGS_ddb_host_ip),
      .wait_for_attach = absl::GetFlag(FLAGS_wait_for_attach),
      .ddb_app_wrapper = absl::GetFlag(FLAGS_ddb_app_wrapper)};

  std::vector<rafty::Config> configs;
  std::unordered_map<uint64_t, uint64_t> node_tester_ports;
  uint64_t tester_port = 55001;

  auto insts = toolings::ConfigGen::gen_local_instances(num, 50050);
  for (const auto &inst : insts) {
    std::map<uint64_t, std::string> peer_addrs;
    for (const auto &peer : insts) {
      if (peer.id == inst.id)
        continue;
      peer_addrs[peer.id] = peer.external_addr;
    }
    rafty::Config config = {
        .id = inst.id, .addr = inst.listening_addr, .peer_addrs = peer_addrs};
    configs.push_back(config);
    node_tester_ports[inst.id] = tester_port;
    tester_port++;
  }

  // logger setup
  auto logger_name = "multinode";
  auto logger = spdlog::get(logger_name);
  if (!logger) {
    // Create the logger if it doesn't exist
    logger = spdlog::basic_logger_mt(
        logger_name, std::format("logs/{}.log", logger_name), true);
  }

  ctrl = std::make_unique<toolings::RaftTestCtrl>(
      configs, node_tester_ports, std::string(node_path),
      std::string(ctrl_addr), fail_type, verbosity, logger, ddb_conf);

  ctrl->register_applier_handler({[logger](testerpb::ApplyResult m) -> void {
    auto i = m.id();
    logger->info("ApplyResult: id={}, index={}, data={}", i, m.index(),
                 m.data());
  }});

  command_loop();
  ctrl.reset();
  return 0;
}