#pragma once

#include <grpcpp/client_context.h>
#include <grpcpp/health_check_service_interface.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/server_context.h>
#include <grpcpp/support/status.h>
#include <memory>
#include "common/utils/net_intercepter.hpp"
#ifdef TRACING
#include "common/utils/tracing.hpp"
#endif
#include "rafty/raft.hpp"

namespace rafty {
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::experimental::ClientInterceptorFactoryInterface;
using grpc::experimental::CreateCustomChannelWithInterceptors;

class RaftServiceImpl final: public raftpb::RaftService::Service {

  public:
  explicit RaftServiceImpl(Raft* raft) : raft_(raft) {}
  grpc::Status AppendEntries(grpc::ServerContext* context, const raftpb::AppendEntriesRequest* request, raftpb::AppendEntriesReply* reply) override {
    raft_->handle_append_entries(request, reply);
    return grpc::Status::OK;
  }
  grpc::Status RequestVote(grpc::ServerContext* context, const raftpb::RequestVoteReq* request, raftpb::RequestVoteReply* reply) override {
    raft_->handle_request_vote(request, reply);
    return grpc::Status::OK;
  }

  private:
  Raft* raft_;
};

inline void Raft::start_server() {
  grpc::EnableDefaultHealthCheckService(false);
  grpc::reflection::InitProtoReflectionServerBuilderPlugin();

  ServerBuilder builder;
  builder.AddListeningPort(this->listening_addr,
                           grpc::InsecureServerCredentials());

#ifdef TRACING
  builder.experimental().SetInterceptorCreators(
      tracing::CreateServerTracingInterceptors());
#endif

  // TODO: implement RaftService RPC
  // and register the service.
  raft_service_ = std::make_unique<RaftServiceImpl>(this);
  builder.RegisterService(raft_service_.get()); /* replace nullptr with actual gRPC service */

  std::unique_ptr<Server> server(builder.BuildAndStart());
  logger->info("Raft server {} listening on {}", id, listening_addr);

  this->server_ = std::move(server);

  std::thread([this] { this->server_->Wait(); }).detach();
}

inline void Raft::stop_server() {
  if (this->server_) {
    this->server_->Shutdown();
  }
}

inline void Raft::connect_peers() {
  grpc::ChannelArguments args;
  args.SetInt(GRPC_ARG_MAX_RECONNECT_BACKOFF_MS, 200);
  args.SetInt(GRPC_ARG_MIN_RECONNECT_BACKOFF_MS, 50);
  args.SetInt(GRPC_ARG_INITIAL_RECONNECT_BACKOFF_MS, 50);
  // Disable Nagle's algorithm: sends small AppendEntries packets immediately
  // without waiting to fill a segment, reducing per-RPC latency.
  args.SetInt("grpc.tcp_nodelay", 1);

  for (const auto &peer_addr : peer_addrs) {
    logger->info("Connecting to peer {} at {}", peer_addr.first,
                 peer_addr.second);
    std::vector<std::unique_ptr<ClientInterceptorFactoryInterface>>
        interceptor_creators;
    interceptor_creators.push_back(
        std::make_unique<ByteCountingInterceptorFactory>());
    interceptor_creators.push_back(std::make_unique<NetInterceptorFactory>());
#ifdef TRACING
    interceptor_creators.push_back(std::make_unique<tracing::TracingClientInterceptorFactory>());
#endif
    auto channel = CreateCustomChannelWithInterceptors(
        peer_addr.second, grpc::InsecureChannelCredentials(), args,
        std::move(interceptor_creators));
    auto stub = raftpb::RaftService::NewStub(std::move(channel));
    peers_[peer_addr.first] = std::move(stub);

    auto w = std::make_unique<PeerWorker>();
    PeerWorker *pw = w.get();
    w->thread = std::thread([pw]() {
      while (true) {
        std::unique_lock<std::mutex> lk(pw->mtx);
        pw->cv.wait(lk, [pw] { return pw->has_task || pw->stop; });
        if (pw->stop) break;
        pw->has_task = false;
        auto task = std::move(pw->task);
        lk.unlock();
        task();
        lk.lock();
        pw->done = true;
        pw->cv.notify_one();
      }
    });
    peer_workers_[peer_addr.first] = std::move(w);
  }
}

inline bool Raft::is_dead() const { return this->dead.load(); }

inline void Raft::kill() {
  this->dead.store(true);
  for (auto &[peer_id, pw] : peer_workers_) {
    {
      std::lock_guard<std::mutex> lk(pw->mtx);
      pw->stop = true;
    }
    pw->cv.notify_one();
    if (pw->thread.joinable()) pw->thread.join();
  }
}

inline std::unique_ptr<grpc::ClientContext>
Raft::create_context(uint64_t to) const {
  std::unique_ptr<grpc::ClientContext> context =
      std::make_unique<grpc::ClientContext>();
  context->AddMetadata("from", std::to_string(this->id));
  context->AddMetadata("to", std::to_string(to));
  return context;
}

inline void Raft::apply(const ApplyResult &result) {
  this->ready_queue.enqueue(result);
}

} // namespace rafty
