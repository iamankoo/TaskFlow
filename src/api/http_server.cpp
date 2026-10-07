#include "taskflow/api/http_server.hpp"

#include <drogon/drogon.h>
#include <spdlog/spdlog.h>
#include <trantor/utils/ConcurrentTaskQueue.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <filesystem>
#include <string>
#include <string_view>
#include <thread>
#include <utility>

namespace taskflow {

namespace {

// Drogon/trantor formats its own lines (timestamp, level, source location);
// forward them to spdlog so the process emits a single log stream.
void ForwardDrogonLog(const char* message, std::uint64_t length) {
  std::string_view text(message, static_cast<std::size_t>(length));
  while (!text.empty() && (text.back() == '\n' || text.back() == '\r')) text.remove_suffix(1);
  spdlog::warn("[drogon] {}", text);
}

// True when something accepts TCP connections on the loopback address.
bool AcceptsConnections(std::uint16_t port) {
  const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) return false;
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = htons(port);
  ::inet_pton(AF_INET, "127.0.0.1", &address.sin_addr);
  const bool ok = ::connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0;
  ::close(fd);
  return ok;
}

}  // namespace

HttpServer::HttpServer(const HttpConfig& config, HealthService& health)
    : config_(config),
      probe_queue_(std::make_unique<trantor::ConcurrentTaskQueue>(2, "taskflow-health")),
      running_(running_promise_.get_future().share()) {
  trantor::Logger::setOutputFunction(ForwardDrogonLog, [] {});
  auto& app = drogon::app();
  app.setLogLevel(trantor::Logger::kWarn)
      .setThreadNum(config_.threads)
      // TaskFlow accepts no file uploads, but Drogon pre-creates its upload directory tree;
      // keep it somewhere the non-root container user can write.
      .setUploadPath((std::filesystem::temp_directory_path() / "taskflow-uploads").string())
      .addListener(config_.host, config_.port);

  app.registerBeginningAdvice([this] {
    try {
      running_promise_.set_value();
    } catch (const std::future_error&) {
      // already signalled
    }
  });

  app.registerHandler(
      "/health",
      [this, &health](const drogon::HttpRequestPtr&,
                      std::function<void(const drogon::HttpResponsePtr&)>&& callback) {
        probe_queue_->runTaskInQueue([&health, callback = std::move(callback)] {
          const HealthReport report = health.Check();
          auto response = drogon::HttpResponse::newHttpJsonResponse(report.ToJson());
          response->setStatusCode(static_cast<drogon::HttpStatusCode>(report.HttpStatus()));
          response->addHeader("Cache-Control", "no-store");
          callback(response);
        });
      },
      {drogon::Get});
}

HttpServer::~HttpServer() = default;

void HttpServer::Run() {
  spdlog::info("HTTP server listening on {}:{}", config_.host, config_.port);
  drogon::app().run();
  spdlog::info("HTTP server stopped");
}

void HttpServer::Quit(std::chrono::milliseconds delay) {
  // Both paths are safe before the loop starts: queued work runs once it does.
  auto* loop = drogon::app().getLoop();
  if (delay.count() <= 0) {
    loop->queueInLoop([] { drogon::app().quit(); });
    return;
  }
  loop->runAfter(std::chrono::duration<double>(delay).count(), [] { drogon::app().quit(); });
}

bool HttpServer::WaitUntilRunning(std::chrono::milliseconds timeout) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  if (running_.wait_for(timeout) != std::future_status::ready) return false;
  // Drogon signals "started" slightly before its listeners are bound, so also
  // wait until the port really accepts connections.
  while (!AcceptsConnections(config_.port)) {
    if (std::chrono::steady_clock::now() >= deadline) return false;
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  return true;
}

}  // namespace taskflow
