#include <spdlog/details/os.h>

#include <cstdio>
#include <ctime>

#include "taskflow/logging/logging.hpp"

namespace taskflow {

std::string EscapeJson(std::string_view text) {
  std::string out;
  out.reserve(text.size());
  for (const char raw : text) {
    const auto c = static_cast<unsigned char>(raw);
    switch (c) {
      case '"': out += "\\\""; break;
      case '\\': out += "\\\\"; break;
      case '\n': out += "\\n"; break;
      case '\r': out += "\\r"; break;
      case '\t': out += "\\t"; break;
      default:
        if (c < 0x20) {
          char buf[8];
          std::snprintf(buf, sizeof(buf), "\\u%04x", c);
          out += buf;
        } else {
          out += raw;
        }
    }
  }
  return out;
}

void JsonFormatter::format(const spdlog::details::log_msg& msg, spdlog::memory_buf_t& dest) {
  const auto seconds = std::chrono::duration_cast<std::chrono::seconds>(msg.time.time_since_epoch());
  const auto millis =
      std::chrono::duration_cast<std::chrono::milliseconds>(msg.time.time_since_epoch()) -
      std::chrono::duration_cast<std::chrono::milliseconds>(seconds);
  const std::time_t t = static_cast<std::time_t>(seconds.count());
  std::tm tm{};
  gmtime_r(&t, &tm);

  char ts[64];
  std::snprintf(ts, sizeof(ts), "%04d-%02d-%02dT%02d:%02d:%02d.%03dZ", tm.tm_year + 1900,
                tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec,
                static_cast<int>(millis.count() % 1000));

  const auto level = spdlog::level::to_string_view(msg.level);
  std::string line = "{\"ts\":\"";
  line += ts;
  line += "\",\"level\":\"";
  line.append(level.data(), level.size());
  line += "\",\"logger\":\"";
  line += EscapeJson(std::string_view(msg.logger_name.data(), msg.logger_name.size()));
  line += "\",\"thread\":";
  line += std::to_string(msg.thread_id);
  line += ",\"msg\":\"";
  line += EscapeJson(std::string_view(msg.payload.data(), msg.payload.size()));
  line += "\"}";
  line += spdlog::details::os::default_eol;
  dest.append(line.data(), line.data() + line.size());
}

std::unique_ptr<spdlog::formatter> JsonFormatter::clone() const {
  return std::make_unique<JsonFormatter>();
}

}  // namespace taskflow
