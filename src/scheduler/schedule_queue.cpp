#include "taskflow/scheduler/schedule_queue.hpp"

#include <algorithm>
#include <cmath>

#include "taskflow/common/errors.hpp"
#include "taskflow/scheduler/priority_queue.hpp"

namespace taskflow {

namespace {

constexpr std::size_t kMemberDigits = 20;
constexpr std::size_t kZaddChunk = 500;

// KEYS[1..4]: ready queues in the order of kPrioritiesHighestFirst
// ARGV[1]: aging interval in microseconds (0 = strict priority)
// ARGV[2..5]: the priority level of each queue
// Mirrors PriorityPolicy::Before exactly; a test compares both on random data.
const char* const kPopNextScript = R"lua(
local aging = tonumber(ARGV[1])
local best = nil
local best_member, best_score, best_level, best_key
for i = 1, 4 do
  local head = redis.call('ZRANGE', KEYS[i], 0, 0, 'WITHSCORES')
  if #head > 0 then
    local member = head[1]
    local score = tonumber(head[2])
    local level = tonumber(ARGV[i + 1])
    local key = score - level * aging
    local better = false
    if best == nil then
      better = true
    elseif aging == 0 then
      if level ~= best_level then better = level > best_level
      elseif score ~= best_score then better = score < best_score
      else better = member < best_member end
    else
      if key ~= best_key then better = key < best_key
      else better = member < best_member end
    end
    if better then
      best = i; best_member = member; best_score = score; best_level = level; best_key = key
    end
  end
end
if best == nil then return false end
redis.call('ZREM', KEYS[best], best_member)
return {best, best_member, string.format('%.0f', best_score)}
)lua";

// KEYS[1]: delayed set; KEYS[2..5]: ready queues
// ARGV: repeated (queue index 1..4, score, member)
const char* const kMarkReadyScript = R"lua(
for i = 1, #ARGV, 3 do
  redis.call('ZREM', KEYS[1], ARGV[i + 2])
  redis.call('ZADD', KEYS[1 + tonumber(ARGV[i])], ARGV[i + 1], ARGV[i + 2])
end
return #ARGV / 3
)lua";

const char* const kUnlockScript = R"lua(
if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end
return 0
)lua";

std::size_t QueueIndex(JobPriority priority) {
  for (std::size_t i = 0; i < kPrioritiesHighestFirst.size(); ++i) {
    if (kPrioritiesHighestFirst[i] == priority) return i;
  }
  return 0;
}

std::int64_t ParseScore(const std::string& text) {
  return static_cast<std::int64_t>(std::llround(std::stod(text)));
}

void ChunkedZadd(RedisClient& redis, const std::string& key,
                 const std::vector<std::pair<std::int64_t, std::string>>& items) {
  for (std::size_t begin = 0; begin < items.size(); begin += kZaddChunk) {
    const std::size_t end = std::min(items.size(), begin + kZaddChunk);
    std::vector<std::string> args{"ZADD", key};
    for (std::size_t i = begin; i < end; ++i) {
      args.push_back(std::to_string(items[i].first));
      args.push_back(items[i].second);
    }
    redis.Execute(args);
  }
}

}  // namespace

ScheduleQueue::ScheduleQueue(RedisClient& redis, std::string prefix)
    : redis_(redis), prefix_(std::move(prefix)) {}

std::string ScheduleQueue::ReadyKey(JobPriority priority) const {
  return prefix_ + ":ready:" + std::string(ToString(priority));
}
std::string ScheduleQueue::DelayedKey() const { return prefix_ + ":delayed"; }
std::string ScheduleQueue::LockKey(std::string_view name) const {
  return prefix_ + ":lock:" + std::string(name);
}
std::string ScheduleQueue::SentinelKey() const { return prefix_ + ":sentinel"; }

std::string ScheduleQueue::MakeMember(const SchedulingEntry& entry) {
  std::string sequence = std::to_string(entry.sequence);
  if (sequence.size() < kMemberDigits) sequence.insert(0, kMemberDigits - sequence.size(), '0');
  return sequence + ":" + entry.id.str();
}

std::int64_t ScheduleQueue::ScoreOf(const SchedulingEntry& entry) {
  return std::chrono::duration_cast<std::chrono::microseconds>(entry.scheduled_at.time_since_epoch())
      .count();
}

std::optional<std::pair<std::int64_t, JobId>> ScheduleQueue::ParseMember(std::string_view member) {
  if (member.size() != kMemberDigits + 1 + 36 || member[kMemberDigits] != ':') return std::nullopt;
  std::int64_t sequence = 0;
  for (std::size_t i = 0; i < kMemberDigits; ++i) {
    if (member[i] < '0' || member[i] > '9') return std::nullopt;
    sequence = sequence * 10 + (member[i] - '0');
  }
  try {
    return std::make_pair(sequence, JobId::Parse(member.substr(kMemberDigits + 1)));
  } catch (const ValidationError&) {
    return std::nullopt;
  }
}

void ScheduleQueue::AddReady(const std::vector<SchedulingEntry>& entries) {
  for (const JobPriority priority : kPrioritiesHighestFirst) {
    std::vector<std::pair<std::int64_t, std::string>> items;
    for (const auto& entry : entries) {
      if (entry.priority == priority) items.emplace_back(ScoreOf(entry), MakeMember(entry));
    }
    if (!items.empty()) ChunkedZadd(redis_, ReadyKey(priority), items);
  }
}

void ScheduleQueue::AddDelayed(const std::vector<SchedulingEntry>& entries) {
  std::vector<std::pair<std::int64_t, std::string>> items;
  items.reserve(entries.size());
  for (const auto& entry : entries) items.emplace_back(ScoreOf(entry), MakeMember(entry));
  if (!items.empty()) ChunkedZadd(redis_, DelayedKey(), items);
}

void ScheduleQueue::MarkReady(const std::vector<SchedulingEntry>& entries) {
  for (std::size_t begin = 0; begin < entries.size(); begin += kZaddChunk) {
    const std::size_t end = std::min(entries.size(), begin + kZaddChunk);
    std::vector<std::string> args{"EVAL", kMarkReadyScript, "5", DelayedKey()};
    for (const JobPriority priority : kPrioritiesHighestFirst) args.push_back(ReadyKey(priority));
    for (std::size_t i = begin; i < end; ++i) {
      args.push_back(std::to_string(QueueIndex(entries[i].priority) + 1));
      args.push_back(std::to_string(ScoreOf(entries[i])));
      args.push_back(MakeMember(entries[i]));
    }
    redis_.Execute(args);
  }
}

void ScheduleQueue::RemoveReady(JobPriority priority, const std::vector<std::string>& members) {
  for (std::size_t begin = 0; begin < members.size(); begin += kZaddChunk) {
    std::vector<std::string> args{"ZREM", ReadyKey(priority)};
    const std::size_t end = std::min(members.size(), begin + kZaddChunk);
    args.insert(args.end(), members.begin() + static_cast<std::ptrdiff_t>(begin),
                members.begin() + static_cast<std::ptrdiff_t>(end));
    redis_.Execute(args);
  }
}

void ScheduleQueue::RemoveDelayed(const std::vector<std::string>& members) {
  for (std::size_t begin = 0; begin < members.size(); begin += kZaddChunk) {
    std::vector<std::string> args{"ZREM", DelayedKey()};
    const std::size_t end = std::min(members.size(), begin + kZaddChunk);
    args.insert(args.end(), members.begin() + static_cast<std::ptrdiff_t>(begin),
                members.begin() + static_cast<std::ptrdiff_t>(end));
    redis_.Execute(args);
  }
}

std::optional<ScheduleQueue::Popped> ScheduleQueue::PopNext(std::chrono::microseconds aging_interval) {
  std::vector<std::string> args{"EVAL", kPopNextScript, "4"};
  for (const JobPriority priority : kPrioritiesHighestFirst) args.push_back(ReadyKey(priority));
  args.push_back(std::to_string(aging_interval.count() < 0 ? 0 : aging_interval.count()));
  for (const JobPriority priority : kPrioritiesHighestFirst) {
    args.push_back(std::to_string(PriorityPolicy::Level(priority)));
  }
  const RedisReply reply = redis_.Execute(args);
  if (reply.type != RedisReply::Type::kArray || reply.elements.size() != 3) return std::nullopt;

  const auto parsed = ParseMember(reply.elements[1].text);
  if (!parsed) throw RedisError("malformed member in a ready queue: " + reply.elements[1].text);
  Popped popped;
  popped.id = parsed->second;
  popped.sequence = parsed->first;
  popped.priority = kPrioritiesHighestFirst.at(static_cast<std::size_t>(reply.elements[0].integer - 1));
  popped.scheduled_micros = ParseScore(reply.elements[2].text);
  return popped;
}

std::size_t ScheduleQueue::ReadyCount(JobPriority priority) {
  return static_cast<std::size_t>(redis_.Execute({"ZCARD", ReadyKey(priority)}).integer);
}

std::size_t ScheduleQueue::DelayedCount() {
  return static_cast<std::size_t>(redis_.Execute({"ZCARD", DelayedKey()}).integer);
}

namespace {
ScheduleQueue::MemberScores ReadRange(RedisClient& redis, const std::string& key, std::size_t offset,
                                      std::size_t count) {
  const RedisReply reply =
      redis.Execute({"ZRANGE", key, std::to_string(offset), std::to_string(offset + count - 1),
                     "WITHSCORES"});
  ScheduleQueue::MemberScores out;
  for (std::size_t i = 0; i + 1 < reply.elements.size(); i += 2) {
    out.emplace_back(reply.elements[i].text, ParseScore(reply.elements[i + 1].text));
  }
  return out;
}
}  // namespace

ScheduleQueue::MemberScores ScheduleQueue::ReadyMembers(JobPriority priority, std::size_t offset,
                                                        std::size_t count) {
  return ReadRange(redis_, ReadyKey(priority), offset, count);
}

ScheduleQueue::MemberScores ScheduleQueue::DelayedMembers(std::size_t offset, std::size_t count) {
  return ReadRange(redis_, DelayedKey(), offset, count);
}

bool ScheduleQueue::TryLock(std::string_view name, const std::string& owner,
                            std::chrono::milliseconds ttl) {
  const RedisReply reply = redis_.Execute(
      {"SET", LockKey(name), owner, "NX", "PX", std::to_string(ttl.count())});
  return reply.type == RedisReply::Type::kStatus && reply.text == "OK";
}

void ScheduleQueue::Unlock(std::string_view name, const std::string& owner) {
  redis_.Execute({"EVAL", kUnlockScript, "1", LockKey(name), owner});
}

bool ScheduleQueue::HasSentinel() {
  return redis_.Execute({"EXISTS", SentinelKey()}).integer == 1;
}

void ScheduleQueue::SetSentinel() { redis_.Execute({"SET", SentinelKey(), "1"}); }

}  // namespace taskflow
