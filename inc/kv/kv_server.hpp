#pragma once

#include <chrono>
#include <cstdint>
#include <cstring>
#include <future>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>

#include <grpcpp/grpcpp.h>

#include "common/common.hpp"
#include "kv.grpc.pb.h"
#include "rafty/raft.hpp"

namespace kv {

class KvServer : public kvpb::KvService::Service {
public:
  explicit KvServer(rafty::Raft &raft) : raft_(raft) {}

  ~KvServer() {}

  void on_apply(const rafty::ApplyResult &result) {
    if (!result.valid)
      return;

    OpType op;
    uint64_t client_id, seq_num;
    std::string key, value;

    if (!deserialize(result.data, op, client_id, seq_num, key, value)) {
      std::lock_guard<std::mutex> lock(state_mu_);
      notify_waiter_state_locked(result.index, 0, 0, kvpb::KV_TIMEOUT, "");
      return;
    }

    kvpb::KvStatus status = kvpb::KV_SUCCESS;
    std::string reply_value;
    bool is_dup = false;

    {
      std::lock_guard<std::mutex> lock(state_mu_);
      auto it = rifl_.find(client_id);
      is_dup = (it != rifl_.end() && it->second.seq_num >= seq_num);
      if (is_dup) {
        status = it->second.status;
        reply_value = it->second.value;
      }
    }

    if (!is_dup) {
      if (op == OpType::PUT) {
        std::unique_lock<std::shared_mutex> lock(store_mu_);
        store_[key] = std::move(value);
      } else if (op == OpType::GET) {
        std::shared_lock<std::shared_mutex> lock(store_mu_);
        auto sit = store_.find(key);
        if (sit != store_.end())
          reply_value = sit->second;
      } else { // APPEND
        std::unique_lock<std::shared_mutex> lock(store_mu_);
        store_[key] += value;
      }
    }

    std::promise<std::pair<kvpb::KvStatus, std::string>> p;
    bool should_fulfill = false;
    std::pair<kvpb::KvStatus, std::string> fulfill_val;

    {
      std::lock_guard<std::mutex> lock(state_mu_);
      if (!is_dup) {
        rifl_[client_id] = {seq_num, status, reply_value};
      }
      last_kv_applied_ = result.index;

      auto wit = pending_.find(result.index);
      if (wit != pending_.end()) {
        if (wit->second.client_id == client_id &&
            wit->second.seq_num == seq_num) {
          fulfill_val = {status, reply_value};
        } else {
          fulfill_val = {kvpb::KV_TIMEOUT, ""};
        }
        p = std::move(wit->second.promise);
        pending_.erase(wit);
        should_fulfill = true;
      }
    }

    if (should_fulfill) {
      p.set_value(std::move(fulfill_val));
    }
  }

  grpc::Status Put(grpc::ServerContext *context,
                   const kvpb::PutRequest *request,
                   kvpb::KvResponse *response) override {
    (void)context;
    auto data = serialize(OpType::PUT, request->client_id(), request->seq_num(),
                          request->key(), request->value());
    auto res = propose_and_wait(data, request->client_id(), request->seq_num());
    response->set_status(res.first);
    return grpc::Status::OK;
  }

  grpc::Status Get(grpc::ServerContext *context,
                   const kvpb::GetRequest *request,
                   kvpb::GetResponse *response) override {
    (void)context;
    std::string fast_value;
    if (try_leader_read(request->key(), fast_value)) {
      response->set_status(kvpb::KV_SUCCESS);
      response->set_value(fast_value);
      return grpc::Status::OK;
    }
    auto data = serialize(OpType::GET, request->client_id(), request->seq_num(),
                          request->key(), "");
    auto res = propose_and_wait(data, request->client_id(), request->seq_num());
    response->set_status(res.first);
    if (res.first == kvpb::KV_SUCCESS)
      response->set_value(res.second);
    return grpc::Status::OK;
  }

  grpc::Status Append(grpc::ServerContext *context,
                      const kvpb::AppendRequest *request,
                      kvpb::KvResponse *response) override {
    (void)context;
    auto data = serialize(OpType::APPEND, request->client_id(),
                          request->seq_num(), request->key(), request->value());
    auto res = propose_and_wait(data, request->client_id(), request->seq_num());
    response->set_status(res.first);
    return grpc::Status::OK;
  }

private:
  enum class OpType : uint8_t { PUT = 0, GET = 1, APPEND = 2 };

  struct CachedReply {
    uint64_t seq_num = 0;
    kvpb::KvStatus status = kvpb::KV_SUCCESS;
    std::string value;
  };

  struct PendingOp {
    uint64_t client_id = 0;
    uint64_t seq_num = 0;
    std::promise<std::pair<kvpb::KvStatus, std::string>> promise;
  };

  static std::string serialize(OpType op, uint64_t client_id, uint64_t seq_num,
                                const std::string &key,
                                const std::string &val) {
    uint32_t klen = static_cast<uint32_t>(key.size());
    uint32_t vlen = static_cast<uint32_t>(val.size());
    size_t total = 1 + 8 + 8 + 4 + klen + 4 + vlen;
    std::string out(total, '\0');
    char *p = out.data();

    *p++ = static_cast<char>(static_cast<uint8_t>(op));
    memcpy(p, &client_id, 8); p += 8;
    memcpy(p, &seq_num,   8); p += 8;
    memcpy(p, &klen,      4); p += 4;
    if (klen) { memcpy(p, key.data(), klen); p += klen; }
    memcpy(p, &vlen, 4); p += 4;
    if (vlen)   memcpy(p, val.data(), vlen);

    return out;
  }

  static bool deserialize(const std::string &data, OpType &op,
                           uint64_t &client_id, uint64_t &seq_num,
                           std::string &key, std::string &value) {
    static constexpr size_t HDR = 1 + 8 + 8 + 4 + 4; // minimum size
    if (data.size() < HDR)
      return false;

    const char *p   = data.data();
    const char *end = p + data.size();

    uint8_t op_byte = static_cast<uint8_t>(*p++);
    if (op_byte > 2)
      return false;
    op = static_cast<OpType>(op_byte);

    memcpy(&client_id, p, 8); p += 8;
    memcpy(&seq_num,   p, 8); p += 8;

    uint32_t klen;
    memcpy(&klen, p, 4); p += 4;
    if (p + klen + 4 > end)
      return false;
    key.assign(p, klen); p += klen;

    uint32_t vlen;
    memcpy(&vlen, p, 4); p += 4;
    if (p + vlen > end)
      return false;
    value.assign(p, vlen);

    return true;
  }

  void notify_waiter_state_locked(uint64_t index, uint64_t applied_client_id,
                                   uint64_t applied_seq_num,
                                   kvpb::KvStatus status,
                                   const std::string &value) {
    auto wit = pending_.find(index);
    if (wit == pending_.end())
      return;

    if (wit->second.client_id == applied_client_id &&
        wit->second.seq_num == applied_seq_num) {
      wit->second.promise.set_value({status, value});
    } else {
      wit->second.promise.set_value({kvpb::KV_TIMEOUT, ""});
    }
    pending_.erase(wit);
  }

  bool try_leader_read(const std::string &key, std::string &value) {
    auto [has_lease, commit_idx] = raft_.get_lease_state();
    if (!has_lease)
      return false;
    (void)commit_idx;

    std::shared_lock<std::shared_mutex> lock(store_mu_);
    auto it = store_.find(key);
    value = (it != store_.end()) ? it->second : "";
    return true;
  }

  std::pair<kvpb::KvStatus, std::string>
  propose_and_wait(const std::string &data, uint64_t client_id,
                   uint64_t seq_num) {
    auto proposal = raft_.propose(data);
    if (!proposal.is_leader)
      return {kvpb::KV_NOTLEADER, ""};

    std::future<std::pair<kvpb::KvStatus, std::string>> fut;
    {
      std::lock_guard<std::mutex> lock(state_mu_);
      PendingOp op;
      op.client_id = client_id;
      op.seq_num   = seq_num;
      fut          = op.promise.get_future();
      pending_[proposal.index] = std::move(op);
    }

    if (fut.wait_for(std::chrono::seconds(5)) == std::future_status::timeout) {
      std::lock_guard<std::mutex> lock(state_mu_);
      pending_.erase(proposal.index);
      return {kvpb::KV_TIMEOUT, ""};
    }

    return fut.get();
  }

  rafty::Raft &raft_;

  mutable std::shared_mutex store_mu_;
  std::unordered_map<std::string, std::string> store_;

  mutable std::mutex state_mu_;
  std::unordered_map<uint64_t, CachedReply> rifl_;
  std::unordered_map<uint64_t, PendingOp> pending_;

  uint64_t last_kv_applied_ = 0;
};

} //namespace kv
