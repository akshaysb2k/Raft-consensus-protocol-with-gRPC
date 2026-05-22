#include "common/utils/rand_gen.hpp"
#include "raft.pb.h"
#include <algorithm>
#include <cstdint>
#include <mutex>
#include "rafty/raft.hpp"
#ifdef TRACING
#include "common/utils/tracing.hpp"
#endif
#include <thread>
#include <chrono>

namespace rafty {
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::experimental::ClientInterceptorFactoryInterface;
using grpc::experimental::CreateCustomChannelWithInterceptors;

Raft::Raft(const Config &config, MessageQueue<ApplyResult> &ready)
    : role_(Role::FOLLOWER),
      current_term_(0),
      voted_for_(std::nullopt),
      heartbeat_timeout(50),
      election_timeout_(),
      logger(utils::logger::get_logger(config.id)),
      id(config.id),
      listening_addr(config.addr),
      peer_addrs(config.peer_addrs),
      dead(false),
      ready_queue(ready)
{
    reset_election_timer();
}

Raft::~Raft() { this->stop_server(); }

void Raft::run() {
  std::cout << "Node " << id << " run() entered\n";
  std::thread([this]() {
    std::cout << "Run Started - Node " << id << "\n";
    reset_election_timer();
    while (!is_dead()) {
      bool am_leader = false;
      {
          std::lock_guard<std::mutex> lk(mtx);
          am_leader = (role_ == Role::LEADER);
          if (!am_leader) received_heartbeat_.store(false);
      }

      if (am_leader) {
          send_heartbeats();
          {
              std::unique_lock<std::mutex> lk(mtx);
              if (role_ == Role::LEADER) {
                  propose_cv_.wait_for(lk, heartbeat_timeout, [this] {
                      return log_.size() > commit_index_ || is_dead();
                  });
              }
          }
          continue;
      }
      auto start = std::chrono::steady_clock::now();

      while (!is_dead()) {
          auto now = std::chrono::steady_clock::now();
          {
            std::lock_guard<std::mutex> lk(mtx);
            if (received_heartbeat_.load()) {
                break;
            }
          }
          if (now - start >= election_timeout_) {
              break;
          }
          std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }

      bool start_election = false;
      {
          std::lock_guard<std::mutex> lk(mtx);
          if (!received_heartbeat_.load() && role_ != Role::LEADER) {
              start_election = true;
              role_ = Role::CANDIDATE;
              current_term_++;
              voted_for_ = id;
              reset_election_timer();
          }
      }
      if (!start_election) continue;

      logger->info("Node {} starting election for term {}", id, current_term_);
      uint64_t votes = 1;
      uint64_t majority = (peers_.size()+1)/2 + 1;
      std::vector<std::pair<uint64_t, raftpb::RaftService::Stub*>> peers_snapshot;
      {
          std::lock_guard<std::mutex> lock(mtx);
          for (auto &kv : peers_) {
              peers_snapshot.emplace_back(kv.first, kv.second.get());
          }
      }
      for (auto &peer : peers_snapshot) {
        raftpb::RequestVoteReq req;
        raftpb::RequestVoteReply reply;
        req.set_term(current_term_);
        req.set_candidateid(id);
        req.set_lastlogindex(log_.size());
        req.set_lastlogterm(log_.empty() ? 0 : log_.back().term());

        auto ctx = create_context(peer.first);
        ctx->set_deadline(std::chrono::system_clock::now() +
                          std::chrono::milliseconds(100));
        auto status = peer.second->RequestVote(ctx.get(), req, &reply);
        if (!status.ok()) {
          continue;
        }
        uint64_t my_term;
        {
          std::lock_guard<std::mutex> lk(mtx);
          my_term = current_term_;
        }
        if (reply.term() > my_term) {
          std::lock_guard<std::mutex> lock(mtx);
          current_term_ = reply.term();
          role_ = Role::FOLLOWER;
          voted_for_ = std::nullopt;
          reset_election_timer();
          break;
        }
        if (reply.votegranted()) {
          votes++;
          logger->debug("Node {} got vote from {} (votes={})", id, peer.first, votes);
          if (votes >= majority) break;
        } else {
          logger->debug("Node {} vote denied by {} (term={})", id, peer.first, reply.term());
        }
      }
      bool became_leader = false;
      {
        std::lock_guard<std::mutex> lk(mtx);
        if (role_ == Role::CANDIDATE && votes >= majority) {
          role_ = Role::LEADER;
          became_leader = true;
        } else if (role_ == Role::CANDIDATE) {
          role_ = Role::FOLLOWER;
          reset_election_timer();
        }
      }
      if (became_leader) {
      {
        std::lock_guard<std::mutex> lk(mtx);
        for (const auto& peer : peers_) {
          nextIndex_[peer.first] = log_.size() + 1;
          matchIndex_[peer.first] = 0;
        }
        last_majority_at_ = std::chrono::steady_clock::now();
        is_leader_atomic_.store(true, std::memory_order_release);
      }
        logger->info("New leader {} elected for term {}", id, current_term_);
        send_heartbeats();
      }
      std::cout << "Node " << id << " role: " << static_cast<int>(role_) << "\n";
    }
  }).detach();
}

void Raft::reset_election_timer() {
  static thread_local std::mt19937 gen(std::random_device{}());
  std::uniform_int_distribution<int> dist(300, 600);
  election_timeout_ = std::chrono::milliseconds(dist(gen));
  logger->debug("Node {} election timeout set to {}ms", id, election_timeout_.count());
}

void Raft::send_heartbeats() {
  struct PeerReq {
    uint64_t                     peer_id;
    raftpb::RaftService::Stub   *stub;
    raftpb::AppendEntriesRequest request;
    uint64_t                     prev_idx;
    uint64_t                     entries_sent;
  };
  struct PeerResult {
    uint64_t                   peer_id;
    uint64_t                   prev_idx;
    uint64_t                   entries_sent;
    raftpb::AppendEntriesReply reply;
    bool                       rpc_ok = false;
  };

  std::vector<PeerReq> peer_reqs;

  {
    std::lock_guard<std::mutex> lock(mtx);
    if (role_ != Role::LEADER) return;

    for (auto &kv : peers_) {
      uint64_t peer_id = kv.first;
      raftpb::RaftService::Stub *stub = kv.second.get();
      if (!stub) {
        logger->warn("Node {}: no stub for peer {}, skipping", id, peer_id);
        continue;
      }

      if (nextIndex_.find(peer_id) == nextIndex_.end())
        nextIndex_[peer_id] = log_.size() + 1;

      uint64_t nextIdx = nextIndex_[peer_id];
      uint64_t prevIdx = nextIdx - 1;

      raftpb::AppendEntriesRequest req;
      req.set_term(current_term_);
      req.set_leaderid(id);
      req.set_prevlogindex(prevIdx);
      req.set_prevlogterm(prevIdx == 0 ? 0 : log_[prevIdx - 1].term());
      req.set_leadercommit(commit_index_);
      for (size_t i = nextIdx - 1; i < log_.size(); ++i)
        *req.add_entries() = log_[i];

      uint64_t n_entries = static_cast<uint64_t>(req.entries_size());
      peer_reqs.push_back({peer_id, stub, std::move(req), prevIdx, n_entries});
    }
  }

  std::vector<PeerResult> results(peer_reqs.size());

  for (size_t i = 0; i < peer_reqs.size(); ++i) {
    auto &pr  = peer_reqs[i];
    auto &res = results[i];
    auto wit  = peer_workers_.find(pr.peer_id);
    if (wit == peer_workers_.end()) continue;
    auto &w = *wit->second;
    std::unique_lock<std::mutex> lk(w.mtx);
    w.cv.wait(lk, [&w] { return w.done; });
    res.peer_id      = pr.peer_id;
    res.prev_idx     = pr.prev_idx;
    res.entries_sent = pr.entries_sent;
    w.task = [this, &pr, &res]() {
      auto ctx = create_context(pr.peer_id);
      ctx->set_deadline(std::chrono::system_clock::now() +
                        std::chrono::milliseconds(30));
      auto status = pr.stub->AppendEntries(ctx.get(), pr.request, &res.reply);
      res.rpc_ok = status.ok();
      if (!status.ok())
        logger->debug("Node {} AE to {} failed: {}", id, pr.peer_id,
                      status.error_message());
    };
    w.done     = false;
    w.has_task = true;
    w.cv.notify_one();
  }

  for (auto &pr : peer_reqs) {
    auto wit = peer_workers_.find(pr.peer_id);
    if (wit == peer_workers_.end()) continue;
    auto &w = *wit->second;
    std::unique_lock<std::mutex> lk(w.mtx);
    w.cv.wait(lk, [&w] { return w.done; });
  }

  uint64_t confirmations = 1;
  std::vector<ApplyResult> to_apply;
  {
    std::unique_lock<std::mutex> lk(mtx);
    if (role_ != Role::LEADER) return;

    for (auto &res : results) {
      if (!res.rpc_ok) continue;

      if (res.reply.term() > current_term_) {
        current_term_ = res.reply.term();
        role_         = Role::FOLLOWER;
        voted_for_    = std::nullopt;
        is_leader_atomic_.store(false, std::memory_order_release);
        reset_election_timer();
        return;
      }

      if (res.reply.term() == current_term_)
        confirmations++;

      if (res.reply.success()) {
        logger->debug("Sent {} entries from leader {} to peer {}",
                      res.entries_sent, id, res.peer_id);
        matchIndex_[res.peer_id] = res.prev_idx + res.entries_sent;
        nextIndex_[res.peer_id]  = matchIndex_[res.peer_id] + 1;
      } else {
        if (nextIndex_[res.peer_id] > 1)
          nextIndex_[res.peer_id]--;
      }
    }

    for (uint64_t idx = log_.size(); idx > commit_index_; --idx) {
      if (log_[idx - 1].term() != current_term_) continue;
      uint64_t count = 1;
      for (const auto &[peer, matchIdx] : matchIndex_)
        if (matchIdx >= idx) count++;
      if (count >= (peers_.size() + 1) / 2 + 1) {
        commit_index_ = idx;
        break;
      }
    }

    while (last_applied_ < commit_index_) {
      last_applied_++;
      ApplyResult ar;
      ar.valid = true;
      ar.index = last_applied_;
      ar.data  = log_[last_applied_ - 1].command();
      to_apply.push_back(std::move(ar));
    }

    uint64_t majority = (peers_.size() + 1) / 2 + 1;
    if (confirmations >= majority) {
      auto now = std::chrono::steady_clock::now();
      lease_granted_at_ = now;
      last_majority_at_ = now;
      auto expires = now + kLeaseDuration;
      lease_expires_ns_.store(expires.time_since_epoch().count(),
                              std::memory_order_release);
      if (commit_index_ > 0 && log_[commit_index_ - 1].term() == current_term_) {
        last_term_commit_idx_.store(commit_index_, std::memory_order_release);
      }
    } else {
      auto since_majority = std::chrono::steady_clock::now() - last_majority_at_;
      if (since_majority > election_timeout_) {
        role_ = Role::FOLLOWER;
        voted_for_ = std::nullopt;
        is_leader_atomic_.store(false, std::memory_order_release);
        reset_election_timer();
      }
    }
  } 

  for (auto &r : to_apply)
    apply(r);
}

void Raft::handle_append_entries(
    const raftpb::AppendEntriesRequest* request,
    raftpb::AppendEntriesReply* reply) {
      std::lock_guard<std::mutex> lock(mtx);
      if (request->term() < current_term_) {
          reply->set_term(current_term_);
          reply->set_success(false);
          return;
      }

      if (request->term() > current_term_) {
        current_term_ = request->term();
        voted_for_ = std::nullopt;
      }

      if (role_ == Role::LEADER) {
          is_leader_atomic_.store(false, std::memory_order_release);
      }
      role_ = Role::FOLLOWER;
      received_heartbeat_.store(true);
      reset_election_timer();
      logger->debug("Node {} received {} entries from leader {}", id, request->entries_size(), request->leaderid());
      uint64_t prevLogIndex = request->prevlogindex();

      reply->set_term(current_term_);
      if (prevLogIndex > log_.size()) {
          reply->set_success(false);
          return;
      }
      if (prevLogIndex > 0 && log_[prevLogIndex-1].term() != request->prevlogterm()) {
          reply->set_success(false);
          return;
      }
    uint64_t idx = prevLogIndex;

    for (const auto &entry : request->entries()) {
      idx++;
      if (log_.size() >= idx) {
          if (log_[idx - 1].term() != entry.term()) {
              log_.resize(idx - 1);
              log_.push_back(entry);
          }
      } else {
          log_.push_back(entry);
      }
    }
    if (request->leadercommit() > commit_index_) {
        commit_index_ = std::min(request->leadercommit(), (uint64_t)log_.size());
    }

    while (last_applied_ < commit_index_) {
        last_applied_++;

        ApplyResult result;
        result.valid = true;
        result.index = last_applied_;
        result.data = log_[last_applied_ - 1].command();

        apply(result);
    }
    reply->set_term(current_term_);
    reply->set_success(true);
}

void Raft::handle_request_vote(
    const raftpb::RequestVoteReq* request,
    raftpb::RequestVoteReply* reply) {
      logger->debug("Node {} received vote request from {} (term {})",
              id, request->candidateid(), request->term());
    std::lock_guard<std::mutex> lock(mtx);

    if (request->term() < current_term_) {
        reply->set_term(current_term_);
        reply->set_votegranted(false);
        return;
    }

    if (request->term() > current_term_) {
        current_term_ = request->term();
        voted_for_ = std::nullopt;
        role_ = Role::FOLLOWER;
        reset_election_timer();
    }

    reply->set_term(current_term_);

    uint64_t myLastIndex = log_.size();
    uint64_t myLastTerm = (myLastIndex == 0) ? 0 : log_[myLastIndex - 1].term();
    uint64_t candidateLastIndex = request->lastlogindex();
    uint64_t candidateLastTerm = request->lastlogterm();

    bool up_to_date = false;

    if (candidateLastTerm > myLastTerm) {
        up_to_date = true;
    } else if (candidateLastTerm == myLastTerm && candidateLastIndex >= myLastIndex) {
        up_to_date = true;
    } else {
        up_to_date = false;
    }

    if ((!voted_for_.has_value() || voted_for_.value() == request->candidateid()) && up_to_date) {
        voted_for_ = request->candidateid();
        reply->set_votegranted(true);
        reset_election_timer();
    } else {
        reply->set_votegranted(false);
    }
}

std::pair<bool, uint64_t> Raft::get_lease_state() const {
  if (!is_leader_atomic_.load(std::memory_order_acquire))
    return {false, 0};
  auto now_ns = std::chrono::steady_clock::now().time_since_epoch().count();
  int64_t expires = lease_expires_ns_.load(std::memory_order_acquire);
  if (now_ns >= expires)
    return {false, 0};
  uint64_t commit_idx = last_term_commit_idx_.load(std::memory_order_acquire);
  if (commit_idx == 0)
    return {false, 0};
  return {true, commit_idx};
}

State Raft::get_state() const {
  std::lock_guard<std::mutex> lock(mtx);
  State inst_state;
  inst_state.term = current_term_;
  if (role_ == Role::LEADER) {
    inst_state.is_leader = true;
  } else {
    inst_state.is_leader = false;
  }
  return inst_state;
}

ProposalResult Raft::propose(const std::string &data) {
  ProposalResult result;
  {
    std::lock_guard<std::mutex> lock(mtx);
    result.term = current_term_;
    result.is_leader = (role_ == Role::LEADER);

    if (!result.is_leader) {
      result.index = 0;
      return result;
    }

    raftpb::Entry new_entry;
    new_entry.set_term(current_term_);
    new_entry.set_command(data);

    log_.push_back(new_entry);
    result.index = log_.size();

    logger->debug("Leader {} appended entry idx={} term={}", id, result.index, result.term);
  }
  propose_cv_.notify_one();

  return result;
}

ProposalResult Raft::propose_sync(const std::string &data) {
  ProposalResult result = propose(data);
  if (!result.is_leader) return result;

  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!is_dead() && std::chrono::steady_clock::now() < deadline) {
    {
      std::lock_guard<std::mutex> lk(mtx);
      if (last_applied_ >= result.index) return result;
      if (role_ != Role::LEADER) break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  result.is_leader = false;
  return result;
}

} // namespace rafty
