#pragma once

#include <cstdlib>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include <google/protobuf/message.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/support/client_interceptor.h>
#include <grpcpp/support/server_interceptor.h>
#include <opentelemetry/context/propagation/global_propagator.h>
#include <opentelemetry/exporters/otlp/otlp_grpc_exporter_factory.h>
#include <opentelemetry/exporters/otlp/otlp_grpc_exporter_options.h>
#include <opentelemetry/exporters/otlp/otlp_grpc_log_record_exporter_factory.h>
#include <opentelemetry/logs/provider.h>
#include <opentelemetry/sdk/logs/batch_log_record_processor_factory.h>
#include <opentelemetry/sdk/logs/logger_provider_factory.h>
#include <opentelemetry/sdk/logs/simple_log_record_processor_factory.h>
#include <opentelemetry/sdk/resource/resource.h>
#include <opentelemetry/sdk/trace/simple_processor_factory.h>
#include <opentelemetry/sdk/trace/tracer_provider.h>
#include <opentelemetry/trace/propagation/http_trace_context.h>
#include <opentelemetry/trace/provider.h>

namespace tracing {

namespace detail {

namespace trace = opentelemetry::trace;
namespace ctx = opentelemetry::context;
using HookPoint = grpc::experimental::InterceptionHookPoints;

inline bool Debug() { return std::getenv("TRACING_DEBUG") != nullptr; }

inline auto Tracer() {
  return opentelemetry::trace::Provider::GetTracerProvider()->GetTracer(
      "raft-otel-tracer");
}

inline auto Logger() {
  return opentelemetry::logs::Provider::GetLoggerProvider()->GetLogger(
      "raft-otel-logger");
}

inline std::string MsgStr(const void *m) {
  return m ? static_cast<const google::protobuf::Message *>(m)
                 ->ShortDebugString()
           : "";
}

// grpc_call* -> Span registry (for handler-side AddEvent)
class ServerSpanRegistry final {
public:
  static ServerSpanRegistry &Instance() {
    static ServerSpanRegistry inst;
    return inst;
  }

  void Put(grpc::ServerContextBase *ctx,
           const opentelemetry::nostd::shared_ptr<trace::Span> &span) {
    if (!ctx || !ctx->c_call())
      return;
    std::lock_guard<std::mutex> lk(mu_);
    spans_[ctx->c_call()] = span;
  }

  void Erase(grpc::ServerContextBase *ctx) {
    if (!ctx || !ctx->c_call())
      return;
    std::lock_guard<std::mutex> lk(mu_);
    spans_.erase(ctx->c_call());
  }

  opentelemetry::nostd::shared_ptr<trace::Span>
  Get(grpc::ServerContextBase *ctx) {
    if (!ctx || !ctx->c_call())
      return {};
    std::lock_guard<std::mutex> lk(mu_);
    auto it = spans_.find(ctx->c_call());
    return it == spans_.end() ? opentelemetry::nostd::shared_ptr<trace::Span>{}
                              : it->second;
  }

private:
  std::mutex mu_;
  std::unordered_map<grpc_call *, opentelemetry::nostd::shared_ptr<trace::Span>>
      spans_;
};

inline trace::StartSpanOptions
MakeServerStartOptions(const grpc::ServerContextBase *ctx_in) {
  const auto &md = ctx_in->client_metadata();
  auto get = [&](const char *k) -> std::string {
    auto it = md.find(k);
    return it != md.end() ? std::string(it->second.data(), it->second.size())
                          : "";
  };

  struct Carrier final : ctx::propagation::TextMapCarrier {
    std::string tp, ts;
    Carrier(std::string a, std::string b)
        : tp(std::move(a)), ts(std::move(b)) {}
    opentelemetry::nostd::string_view
    Get(opentelemetry::nostd::string_view k) const noexcept override {
      if (k == "traceparent")
        return opentelemetry::nostd::string_view(tp.data(), tp.size());
      if (k == "tracestate")
        return opentelemetry::nostd::string_view(ts.data(), ts.size());
      return "";
    }
    void Set(opentelemetry::nostd::string_view,
             opentelemetry::nostd::string_view) noexcept override {}
  } carrier(get("traceparent"), get("tracestate"));

  auto current = ctx::RuntimeContext::GetCurrent();
  auto parent =
      ctx::propagation::GlobalTextMapPropagator::GetGlobalPropagator()->Extract(
          carrier, current);
  trace::StartSpanOptions opts;
  opts.kind = trace::SpanKind::kServer;
  if (auto sp = trace::GetSpan(parent); sp->GetContext().IsValid())
    opts.parent = sp->GetContext();
  return opts;
}

inline void InjectClientContext(std::multimap<std::string, std::string> *md) {
  if (!md)
    return;
  struct Carrier final : ctx::propagation::TextMapCarrier {
    std::multimap<std::string, std::string> *m;
    explicit Carrier(std::multimap<std::string, std::string> *m) : m(m) {}
    opentelemetry::nostd::string_view
    Get(opentelemetry::nostd::string_view) const noexcept override {
      return "";
    }
    void Set(opentelemetry::nostd::string_view k,
             opentelemetry::nostd::string_view v) noexcept override {
      m->emplace(std::string(k), std::string(v));
    }
  } carrier(md);

  ctx::propagation::GlobalTextMapPropagator::GetGlobalPropagator()->Inject(
      carrier, ctx::RuntimeContext::GetCurrent());
}

class ServerInterceptor final : public grpc::experimental::Interceptor {
public:
  explicit ServerInterceptor(grpc::experimental::ServerRpcInfo *i) : info_(i) {}

  void Intercept(grpc::experimental::InterceptorBatchMethods *m) override {
    // Start early so handler can attach events; PRE_SEND_* may happen after
    // handler returns.
    if (!span_ &&
        m->QueryInterceptionHookPoint(HookPoint::POST_RECV_INITIAL_METADATA)) {
      auto opts = MakeServerStartOptions(info_->server_context());
      span_ = Tracer()->StartSpan(info_->method(), opts);
      span_->SetAttribute("rpc.system", "grpc");
      span_->SetAttribute("rpc.method", info_->method());
      scope_ = std::make_unique<trace::Scope>(span_);
      ServerSpanRegistry::Instance().Put(info_->server_context(), span_);
      if (Debug())
        std::cerr << "[tracing] server span started: " << info_->method()
                  << "\n";
    }

    if (m->QueryInterceptionHookPoint(HookPoint::POST_RECV_MESSAGE) && span_)
      if (auto s = MsgStr(m->GetRecvMessage()); !s.empty())
        span_->SetAttribute("rpc.request", s);

    if (m->QueryInterceptionHookPoint(HookPoint::PRE_SEND_MESSAGE) && span_)
      if (auto s = MsgStr(m->GetSendMessage()); !s.empty())
        span_->SetAttribute("rpc.response", s);

    if (m->QueryInterceptionHookPoint(HookPoint::PRE_SEND_STATUS) && span_)
      span_->SetAttribute("rpc.grpc.status_code",
                          static_cast<int>(m->GetSendStatus().error_code()));

    if (m->QueryInterceptionHookPoint(HookPoint::POST_RECV_CLOSE) && span_) {
      span_->End();
      ServerSpanRegistry::Instance().Erase(info_->server_context());
    }
    m->Proceed();
  }

private:
  grpc::experimental::ServerRpcInfo *info_;
  opentelemetry::nostd::shared_ptr<trace::Span> span_;
  std::unique_ptr<trace::Scope> scope_;
};

class ClientInterceptor final : public grpc::experimental::Interceptor {
public:
  explicit ClientInterceptor(grpc::experimental::ClientRpcInfo *i) : info_(i) {}

  void Intercept(grpc::experimental::InterceptorBatchMethods *m) override {
    if (Debug())
      std::cerr << "[tracing] client Intercept(method=" << info_->method()
                << ")\n";

    if (m->QueryInterceptionHookPoint(HookPoint::PRE_SEND_INITIAL_METADATA)) {
      trace::StartSpanOptions opts;
      opts.kind = trace::SpanKind::kClient;

      span_ = Tracer()->StartSpan(info_->method(), opts);
      span_->SetAttribute("rpc.system", "grpc");
      span_->SetAttribute("rpc.method", info_->method());
      scope_ = std::make_unique<trace::Scope>(span_);
      auto scope_ = Tracer()->WithActiveSpan(span_);

      if (auto *md = m->GetSendInitialMetadata())
        InjectClientContext(md);
    }

    if (m->QueryInterceptionHookPoint(HookPoint::PRE_SEND_MESSAGE) && span_)
      if (auto s = MsgStr(m->GetSendMessage()); !s.empty())
        span_->SetAttribute("rpc.request", s);

    if (m->QueryInterceptionHookPoint(HookPoint::POST_RECV_MESSAGE) && span_)
      if (auto s = MsgStr(m->GetRecvMessage()); !s.empty())
        span_->SetAttribute("rpc.response", s);

    if (m->QueryInterceptionHookPoint(HookPoint::POST_RECV_STATUS) && span_) {
      if (auto *st = m->GetRecvStatus())
        span_->SetAttribute("rpc.grpc.status_code",
                            static_cast<int>(st->error_code()));
      span_->End();
    }
    m->Proceed();
  }

private:
  grpc::experimental::ClientRpcInfo *info_;
  opentelemetry::nostd::shared_ptr<trace::Span> span_;
  std::unique_ptr<trace::Scope> scope_;
};

} // namespace detail

// ------------------------------- Public API ---------------------------------

inline auto get_tracer() { return detail::Tracer(); }

inline auto get_logger() { return detail::Logger(); }

static inline void
InitTracing(const std::string &service,
            const std::string &endpoint = "http://localhost:4317") {
  namespace otlp = opentelemetry::exporter::otlp;
  namespace sdk = opentelemetry::sdk::trace;

  otlp::OtlpGrpcExporterOptions opts;
  opts.endpoint = endpoint;

  auto provider = std::make_shared<sdk::TracerProvider>(
      sdk::SimpleSpanProcessorFactory::Create(
          otlp::OtlpGrpcExporterFactory::Create(opts)),
      opentelemetry::sdk::resource::Resource::Create(
          {{"service.name", service}}));

  // Important: Explicit opentelemetry::nostd::shared_ptr wrappers
  // (std::shared_ptr is not enough).
  detail::trace::Provider::SetTracerProvider(
      opentelemetry::nostd::shared_ptr<detail::trace::TracerProvider>(
          provider));
  detail::ctx::propagation::GlobalTextMapPropagator::SetGlobalPropagator(
      opentelemetry::nostd::shared_ptr<
          detail::ctx::propagation::TextMapPropagator>(
          std::make_shared<detail::trace::propagation::HttpTraceContext>()));

  std::cout << "Tracing: " << service << " -> " << endpoint << std::endl;
}

static inline void
InitLogger(const std::string &service,
           const std::string &endpoint = "http://localhost:4317") {
  namespace otlp = opentelemetry::exporter::otlp;

  otlp::OtlpGrpcLogRecordExporterOptions opts;
  opts.endpoint = endpoint;
  auto exporter = otlp::OtlpGrpcLogRecordExporterFactory::Create(opts);
  auto batch_options =
      opentelemetry::sdk::logs::BatchLogRecordProcessorOptions();
  auto processor =
      opentelemetry::sdk::logs::BatchLogRecordProcessorFactory::Create(
          std::move(exporter), batch_options);
  auto provider = std::make_shared<opentelemetry::sdk::logs::LoggerProvider>(
      std::move(processor), opentelemetry::sdk::resource::Resource::Create(
                                {{"service.name", service}}));
  opentelemetry::logs::Provider::SetLoggerProvider(
      opentelemetry::nostd::shared_ptr<opentelemetry::logs::LoggerProvider>(
          provider));
}

inline void InitOtelInfra(const std::string &service_name) {
  tracing::InitTracing(service_name);
  tracing::InitLogger(service_name);
}

// Add an event to the RPC's server span from inside handler code.
inline void AddEvent(grpc::ServerContextBase *ctx, const std::string &name) {
  auto span = detail::ServerSpanRegistry::Instance().Get(ctx);
  if (span && span->GetContext().IsValid()) {
    span->AddEvent(name);
    return;
  }
  if (detail::Debug())
    std::cerr << "[tracing] AddEvent(server) -> no span: " << name << "\n";
}

struct TracingServerInterceptorFactory final
    : grpc::experimental::ServerInterceptorFactoryInterface {
  grpc::experimental::Interceptor *
  CreateServerInterceptor(grpc::experimental::ServerRpcInfo *i) override {
    return new detail::ServerInterceptor(i);
  }
};

inline auto CreateServerTracingInterceptors() {
  std::vector<
      std::unique_ptr<grpc::experimental::ServerInterceptorFactoryInterface>>
      v;
  v.push_back(std::make_unique<TracingServerInterceptorFactory>());
  return v;
}

struct TracingClientInterceptorFactory final
    : grpc::experimental::ClientInterceptorFactoryInterface {
  grpc::experimental::Interceptor *
  CreateClientInterceptor(grpc::experimental::ClientRpcInfo *i) override {
    return new detail::ClientInterceptor(i);
  }
};

inline auto CreateClientTracingInterceptors() {
  std::vector<
      std::unique_ptr<grpc::experimental::ClientInterceptorFactoryInterface>>
      v;
  v.push_back(std::make_unique<TracingClientInterceptorFactory>());
  return v;
}

} // namespace tracing