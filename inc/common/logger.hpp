#pragma once

#include <chrono>
#include <iostream>
#include <memory>

#ifdef TRACING
#include "spdlog/logger.h"
#include <opentelemetry/instrumentation/spdlog/sink.h>
#endif

#include "spdlog/async.h"
#include "spdlog/cfg/env.h"
#include "spdlog/common.h"
#include "spdlog/sinks/basic_file_sink.h"
#include "spdlog/sinks/stdout_color_sinks.h"
#include "spdlog/sinks/stdout_sinks.h"
#include "spdlog/spdlog.h"

namespace rafty {
namespace utils {
class logger {
private:
  std::shared_ptr<spdlog::logger> _logger;

public:
  logger(const logger &) = delete;
  logger &operator=(const logger &) = delete;

  inline static std::unique_ptr<logger> get_logger(uint64_t id) {
    std::string name = std::format("rafty_node_{}", id);
    return std::make_unique<logger>(name);
  }

  // inline static logger &get_instance() {
  //   static logger instance;
  //   return instance;
  // }

  inline void set_level(spdlog::level::level_enum level) {
    _logger->set_level(level);
  }

  template <typename... Args>
  inline void trace(spdlog::format_string_t<Args...> fmt, Args &&...args) {
    this->_logger->trace(fmt, std::forward<Args>(args)...);
  }

  template <typename... Args>
  inline void debug(spdlog::format_string_t<Args...> fmt, Args &&...args) {
    this->_logger->debug(fmt, std::forward<Args>(args)...);
  }

  template <typename... Args>
  inline void info(spdlog::format_string_t<Args...> fmt, Args &&...args) {
    this->_logger->info(fmt, std::forward<Args>(args)...);
  }

  template <typename... Args>
  inline void warn(spdlog::format_string_t<Args...> fmt, Args &&...args) {
    this->_logger->warn(fmt, std::forward<Args>(args)...);
  }

  template <typename... Args>
  inline void error(spdlog::format_string_t<Args...> fmt, Args &&...args) {
    this->_logger->error(fmt, std::forward<Args>(args)...);
  }

  template <typename... Args>
  inline void critical(spdlog::format_string_t<Args...> fmt, Args &&...args) {
    this->_logger->critical(fmt, std::forward<Args>(args)...);
  }

  template <typename T> inline void trace(const T &msg) {
    this->_logger->trace(msg);
  }

  template <typename T> inline void debug(const T &msg) {
    this->_logger->debug(msg);
  }

  template <typename T> inline void info(const T &msg) {
    this->_logger->info(msg);
  }

  template <typename T> inline void warn(const T &msg) {
    this->_logger->warn(msg);
  }

  template <typename T> inline void error(const T &msg) {
    this->_logger->error(msg);
  }

  template <typename T> inline void critical(const T &msg) {
    this->_logger->critical(msg);
  }

  inline void flush() { this->_logger->flush(); }

  // You generally don't need to use this function
  // unless you want to use spdlog's advanced features
  inline std::shared_ptr<spdlog::logger> get_raw() { return this->_logger; }

public:
  logger(const std::string &name);
  ~logger();
};

inline void shutdown_all_loggers() { spdlog::shutdown(); }

inline logger::logger(const std::string &name) {
  try {
#ifdef TRACING
    auto spdlogger =
        std::dynamic_pointer_cast<spdlog::logger>(spdlog::get(name));
#else
    auto spdlogger =
        std::dynamic_pointer_cast<spdlog::async_logger>(spdlog::get(name));
#endif
    if (!spdlogger) {
#ifdef TRACING
        auto otel_sink = std::make_shared<spdlog::sinks::opentelemetry_sink_mt>();
        auto logger = std::make_shared<spdlog::logger>(name, otel_sink);
#else
        auto stdout_sink =
            std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
        auto file_sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>(
            std::format("logs/{}.log", name), true);
        std::vector<spdlog::sink_ptr> sinks{stdout_sink, file_sink};
        auto logger = std::make_shared<spdlog::async_logger>(
            name, sinks.begin(), sinks.end(), spdlog::thread_pool(),
            spdlog::async_overflow_policy::block);
#endif
      spdlog::register_logger(logger);
      this->_logger = std::move(logger);
    } else {
      this->_logger = std::move(spdlogger);
    }
    spdlog::cfg::load_env_levels();
  } catch (const spdlog::spdlog_ex &ex) {
    std::cout << "Log init failed: " << ex.what() << std::endl;
  }
}

inline logger::~logger() { this->flush(); }

static inline __attribute__((always_inline)) void init_spdlog() {
  spdlog::init_thread_pool(8192, 3);
}

static inline __attribute__((always_inline)) void init_logger() {
  init_spdlog();
  spdlog::flush_every(std::chrono::seconds(5));
}

static inline void
disable_console_logging(std::shared_ptr<spdlog::logger> logger,
                        bool disable_file_sink = false) {
  // disable console logging
  // file sink won't be affected unless specified
  for (auto &sink : logger->sinks()) {
    if (auto console_sink =
            std::dynamic_pointer_cast<spdlog::sinks::stdout_sink_mt>(sink)) {
      console_sink->set_level(spdlog::level::off);
    } else if (auto console_sink = std::dynamic_pointer_cast<
                   spdlog::sinks::stdout_color_sink_mt>(sink)) {
      console_sink->set_level(spdlog::level::off);
    } else if (auto file_sink =
                   std::dynamic_pointer_cast<spdlog::sinks::basic_file_sink_mt>(
                       sink)) {
      if (disable_file_sink)
        file_sink->set_level(spdlog::level::off);
    }
  }
}
} // namespace utils
} // namespace rafty