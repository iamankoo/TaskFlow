#include <gtest/gtest.h>
#include <json/json.h>
#include <spdlog/sinks/ostream_sink.h>

#include <algorithm>
#include <memory>
#include <sstream>

#include "taskflow/logging/logging.hpp"

namespace taskflow {
namespace {

Json::Value ParseJson(const std::string& text) {
  Json::Value root;
  Json::CharReaderBuilder builder;
  std::string errors;
  std::istringstream stream(text);
  EXPECT_TRUE(Json::parseFromStream(builder, stream, &root, &errors)) << errors << "\n" << text;
  return root;
}

TEST(EscapeJsonTest, EscapesQuotesBackslashesAndControlCharacters) {
  EXPECT_EQ(EscapeJson("plain"), "plain");
  EXPECT_EQ(EscapeJson("say \"hi\""), "say \\\"hi\\\"");
  EXPECT_EQ(EscapeJson("a\\b"), "a\\\\b");
  EXPECT_EQ(EscapeJson("line1\nline2\ttab\r"), "line1\\nline2\\ttab\\r");
  EXPECT_EQ(EscapeJson(std::string("\x01", 1)), "\\u0001");
}

TEST(JsonFormatterTest, EmitsOneValidJsonObjectPerLine) {
  std::ostringstream captured;
  auto sink = std::make_shared<spdlog::sinks::ostream_sink_mt>(captured);
  spdlog::logger logger("taskflow-test", sink);
  logger.set_formatter(std::make_unique<JsonFormatter>());

  logger.warn("job {} said \"hello\"\nsecond line", 42);

  const std::string output = captured.str();
  ASSERT_FALSE(output.empty());
  EXPECT_EQ(output.back(), '\n');
  EXPECT_EQ(std::count(output.begin(), output.end(), '\n'), 1) << "embedded newline must be escaped";

  const Json::Value line = ParseJson(output);
  EXPECT_EQ(line["level"].asString(), "warning");
  EXPECT_EQ(line["logger"].asString(), "taskflow-test");
  EXPECT_EQ(line["msg"].asString(), "job 42 said \"hello\"\nsecond line");
  EXPECT_TRUE(line.isMember("ts"));
  EXPECT_TRUE(line.isMember("thread"));
  EXPECT_EQ(line["ts"].asString().back(), 'Z');
}

TEST(InitLoggingTest, AppliesConfiguredLevelForBothFormats) {
  for (const char* format : {"text", "json"}) {
    LoggingConfig config;
    config.level = "error";
    config.format = format;
    InitLogging(config);
    EXPECT_EQ(spdlog::default_logger()->name(), "taskflow");
    EXPECT_EQ(spdlog::default_logger()->level(), spdlog::level::err) << format;
    EXPECT_FALSE(spdlog::default_logger()->should_log(spdlog::level::info));
    EXPECT_TRUE(spdlog::default_logger()->should_log(spdlog::level::critical));
  }
}

}  // namespace
}  // namespace taskflow
