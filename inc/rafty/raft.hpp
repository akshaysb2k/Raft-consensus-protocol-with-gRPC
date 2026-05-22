#pragma once

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include <grpcpp/grpcpp.h>

#include "common/common.hpp"
#include "common/config.hpp"
#include "common/logger.hpp"
#include "toolings/msg_queue.hpp"

#include "raft.grpc.pb.h"

using namespace toolings;

namespace rafty {
  using RaftServiceStub = std::unique_ptr<raftpb::RaftService::Stub>;
  using grpc::Server;

  class RaftServiceImpl;

  class Raft {
  public:
    Raft(const Config &config, MessageQueue<ApplyResult> &ready);
    ~Raft();

    void run();
    ProposalResult propose(const std::string &data);
    State get_state() const;

    ProposalResult propose_sync(const std::string &data);

    std::pair<bool, uint64_t> get_lease_state() const;

    void start_server();
    void stop_server();
    void connect_peers();
    bool is_dead() const;
    void kill();
    void handle_append_entries(const raftpb::AppendEntriesRequest* request, raftpb::AppendEntriesReply* reply);
    void handle_request_vote(const raftpb::RequestVoteReq* request, raftpb::RequestVoteReply* reply);

  private:
    enum class Role { FOLLOWER, CANDIDATE, LEADER };
    Role role_ = Role::FOLLOWER;
    uint64_t current_term_;
    std::optional<uint64_t> voted_for_ = std::nullopt;

    std::chrono::milliseconds heartbeat_timeout{50};
    std::chrono::milliseconds election_timeout_;

    static constexpr std::chrono::milliseconds kLeaseDuration{250};
    std::chrono::steady_clock::time_point lease_granted_at_{};
    std::chrono::steady_clock::time_point last_majority_at_{};

    std::atomic<bool> is_leader_atomic_{false};
    std::atomic<int64_t> lease_expires_ns_{0};
    std::atomic<uint64_t> last_term_commit_idx_{0};

    std::condition_variable propose_cv_;

    void send_heartbeats();
    void reset_election_timer();
    std::unique_ptr<RaftServiceImpl> raft_service_;

    std::unique_ptr<grpc::ClientContext> create_context(uint64_t to) const;
    void apply(const ApplyResult &result);

  protected:
    mutable std::mutex mtx;
    std::unique_ptr<rafty::utils::logger> logger;

  private:
    uint64_t id;
    std::string listening_addr;
    std::map<uint64_t, std::string> peer_addrs;
    std::atomic<bool> received_heartbeat_{false};

    std::atomic<bool> dead;
    MessageQueue<ApplyResult> &ready_queue;

    std::unordered_map<uint64_t, RaftServiceStub> peers_;
    std::unique_ptr<Server> server_;

    std::vector<raftpb::Entry> log_;
    uint64_t commit_index_ = 0;
    uint64_t last_applied_ = 0;
    std::unordered_map<uint64_t, uint64_t> nextIndex_;
    std::unordered_map<uint64_t, uint64_t> matchIndex_;

    struct PeerWorker {
      std::mutex mtx;
      std::condition_variable cv;
      std::function<void()> task;
      bool has_task = false;
      bool done     = true;
      bool stop     = false;
      std::thread thread;
    };
    std::unordered_map<uint64_t, std::unique_ptr<PeerWorker>> peer_workers_;
  };
} // namespace rafty

#include "rafty/impl/raft.ipp"
