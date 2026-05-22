#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <fcntl.h>
#include <gtest/gtest.h>
#include <memory>
#include <optional>
#include <sys/wait.h>
#include <unistd.h>
#include <unordered_map>

#include <ddb/integration.hpp>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"

#include "common/logger.hpp"
#include "toolings/config_gen.hpp"
#include "toolings/test_config.hpp"
#include "toolings/test_ctrl.hpp"

using namespace toolings;

const std::string NODE_APP_PATH = "../app/raft_node";

ABSL_FLAG(
    int, tester_verb, 1,
    "Tester verbosity level: 0 (silent), 1 (tester message (file sink only)), "
    "2 (all tester messages, file sink + console sink)");
ABSL_FLAG(int, raft_node_verb, 0,
          "Raft node verbosity level: 0 (silent), 1 (raft message (file sink "
          "only)), 2 "
          "(all raft node messages, file sink + console sink)");

// used by DDB
ABSL_FLAG(bool, ddb, false, "Enable DDB for debugging.");
ABSL_FLAG(std::string, ddb_host_ip, "127.0.0.1",
          "Host IP of this machine for DDB to connect to.");
ABSL_FLAG(bool, wait_for_attach, true,
          "Wait for DDB debugger to attach before proceeding.");
ABSL_FLAG(bool, ddb_app_wrapper, true,
          "Use DDB app runner wrapper to start raft node (supports PET).");

static DDBConfig ddb_conf;

static constexpr std::chrono::milliseconds TIMEOUT(1000);

// Helper class to suppress output
class OutputSuppressor {
public:
  OutputSuppressor() = default;
  ~OutputSuppressor() = default;

  void Enable() {
    // devnull.open("/dev/null");
    // old_buf = std::cout.rdbuf(devnull.rdbuf());

    // Open /dev/null
    devnull_fd = open("/dev/null", O_WRONLY);
    if (devnull_fd == -1) {
      throw std::runtime_error("Failed to open /dev/null");
    }

    // Save the original stdout file descriptor
    saved_stdout_fd = dup(STDOUT_FILENO);
    if (saved_stdout_fd == -1) {
      throw std::runtime_error("Failed to duplicate stdout file descriptor");
    }

    // Redirect stdout to /dev/null
    if (dup2(devnull_fd, STDOUT_FILENO) == -1) {
      throw std::runtime_error("Failed to redirect stdout to /dev/null");
    }
  }

  void Disable() {
    // Restore the original stdout
    if (dup2(saved_stdout_fd, STDOUT_FILENO) == -1) {
      std::cerr << "Failed to restore stdout" << std::endl;
    }

    // Close file descriptors
    close(devnull_fd);
    close(saved_stdout_fd);
  }

private:
  int devnull_fd;
  int saved_stdout_fd;
};

void cleanup_raft_node_processes() {
  std::system("pkill -9 raft_node 2>/dev/null");
  // Note: pkill returns 0 if processes killed, 1 if none found, 2+ on error
  // We don't check the result because this is best-effort cleanup
}

class RaftTest : public ::testing::Test {
protected:
  void SetUp() override {
    this->tester_verb = absl::GetFlag(FLAGS_tester_verb);
    this->raft_node_verb = absl::GetFlag(FLAGS_raft_node_verb);
    // this->suppressor.Enable();
  }

  void TearDown() override {
    // Optionally re-enable if needed
    // this->suppressor.Disable();
    if (this->logger) {
      this->logger->flush();
    }

    // Additional safety: kill any remaining raft_node processes
    cleanup_raft_node_processes();
  }

  void init_logger(const std::string &name) {
    // logger setup
    auto logger_name = name.empty() ? "raft_test" : "raft_test_" + name;
    this->logger = spdlog::get(logger_name);
    if (!this->logger) {
      // Create the logger if it doesn't exist
      this->logger = spdlog::basic_logger_mt(
          logger_name, std::format("logs/{}.log", logger_name), true);
    }
    if (tester_verb < 1) {
      rafty::utils::disable_console_logging(this->logger, true);
    } else if (tester_verb < 2) {
      rafty::utils::disable_console_logging(this->logger, false);
    }
  }

  std::shared_ptr<spdlog::logger> logger = nullptr;
  int tester_verb = 0;
  int raft_node_verb = 0;
  OutputSuppressor suppressor;
};

TEST_F(RaftTest, ExampleTestA) {
  this->init_logger("ExampleTestA");
  auto local_confs = ConfigGen::gen_local_instances(3, 50051);
  auto r_confs = ConfigGen::gen_raft_configs(local_confs);
  toolings::MultiprocTestConfig cfg(r_confs, NODE_APP_PATH, this->logger,
                                    ddb_conf, 0, this->raft_node_verb);

  try {
    cfg.begin();

    logger->info("Checking if the cluster has elected a leader...");
    cfg.check_one_leader();

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    logger->info("Querying for all nodes regarding their term ...");
    logger->info("Error out if they cannot get a unanimous term...");
    logger->info("This is used when you expect their term is converged and get "
                 "the current term.");
    auto term1 = cfg.check_terms();

    // Basic Assertions in GTest
    ASSERT_TRUE(term1.has_value())
        << "Failed to get terms after first election";
    ASSERT_TRUE(term1.value() >= 1)
        << "term is " << term1.value() << ", but should be at least 1";

    cfg.check_one_leader();
  } catch (const std::exception &e) {
    logger->error("Exception: {}", e.what());
    FAIL() << "Exception: " << e.what();
  }
}

static pid_t pgid = 0;

void signal_handler(int signal) {
  if (signal == SIGINT) {
    std::cout << "Caught SIGINT (Ctrl+C), cleaning up..." << std::endl;
    if (::kill(-pgid, SIGKILL) == 0) {
      // well... dead...
    } else {
      std::perror("Failed to kill process");
      std::exit(1);
    }
    exit(0);
  }
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  absl::ParseCommandLine(argc, argv);

  // Initialize DDB Config
  ddb_conf = {
      .enable_ddb = absl::GetFlag(FLAGS_ddb),
      .ddb_host_ip = absl::GetFlag(FLAGS_ddb_host_ip),
      .wait_for_attach = absl::GetFlag(FLAGS_wait_for_attach),
      .ddb_app_wrapper = absl::GetFlag(FLAGS_ddb_app_wrapper),
  };

  // optionally enable DDB
  if (absl::GetFlag(FLAGS_ddb)) {
    std::string app_alias = "raft_test_extra_app";
    auto cfg = DDB::Config::get_default(absl::GetFlag(FLAGS_ddb_host_ip))
                   .with_alias(app_alias)
                   .with_logical_group(app_alias);
    cfg.wait_for_attach = absl::GetFlag(FLAGS_wait_for_attach);
    auto connector = DDB::DDBConnector(cfg);
    connector.init();
  }

  rafty::utils::init_logger();

  pgid = getpid();
  // Register the signal handler for Ctrl+C (SIGINT)
  struct sigaction sigIntHandler;
  sigIntHandler.sa_handler = signal_handler;
  sigemptyset(&sigIntHandler.sa_mask);
  sigIntHandler.sa_flags = 0;
  sigaction(SIGINT, &sigIntHandler, nullptr);

  return RUN_ALL_TESTS();
}
