#include "taskflow/scheduler/priority_queue.hpp"

#include <utility>

namespace taskflow {

namespace {

std::int64_t Micros(TimePoint time) noexcept {
  return std::chrono::duration_cast<std::chrono::microseconds>(time.time_since_epoch()).count();
}

}  // namespace

// ---------- PriorityPolicy ----------

PriorityPolicy::PriorityPolicy(std::chrono::microseconds aging_interval)
    : aging_(aging_interval.count() < 0 ? std::chrono::microseconds(0) : aging_interval) {}

std::int64_t PriorityPolicy::AgedKey(const SchedulingEntry& entry) const noexcept {
  return Micros(entry.scheduled_at) - static_cast<std::int64_t>(Level(entry.priority)) * aging_.count();
}

bool PriorityPolicy::Before(const SchedulingEntry& a, const SchedulingEntry& b) const noexcept {
  if (aging_enabled()) {
    const auto key_a = AgedKey(a);
    const auto key_b = AgedKey(b);
    if (key_a != key_b) return key_a < key_b;
  } else {
    const int level_a = Level(a.priority);
    const int level_b = Level(b.priority);
    if (level_a != level_b) return level_a > level_b;
    const auto time_a = Micros(a.scheduled_at);
    const auto time_b = Micros(b.scheduled_at);
    if (time_a != time_b) return time_a < time_b;
  }
  if (a.sequence != b.sequence) return a.sequence < b.sequence;
  return a.id.str() < b.id.str();
}

double PriorityPolicy::EffectiveLevel(const SchedulingEntry& entry, TimePoint now) const noexcept {
  const double level = Level(entry.priority);
  if (!aging_enabled()) return level;
  const double waited = static_cast<double>(Micros(now) - Micros(entry.scheduled_at));
  return level + (waited > 0 ? waited : 0.0) / static_cast<double>(aging_.count());
}

// ---------- JobPriorityQueue ----------

JobPriorityQueue::JobPriorityQueue(PriorityPolicy policy) : policy_(policy) {}

void JobPriorityQueue::Place(std::size_t index, SchedulingEntry entry) {
  position_[entry.id.str()] = index;
  heap_[index] = std::move(entry);
}

void JobPriorityQueue::SiftUp(std::size_t index) {
  SchedulingEntry moving = std::move(heap_[index]);
  while (index > 0) {
    const std::size_t parent = (index - 1) / 2;
    if (!policy_.Before(moving, heap_[parent])) break;
    Place(index, std::move(heap_[parent]));
    index = parent;
  }
  Place(index, std::move(moving));
}

void JobPriorityQueue::SiftDown(std::size_t index) {
  SchedulingEntry moving = std::move(heap_[index]);
  const std::size_t count = heap_.size();
  for (;;) {
    std::size_t child = 2 * index + 1;
    if (child >= count) break;
    if (child + 1 < count && policy_.Before(heap_[child + 1], heap_[child])) ++child;
    if (!policy_.Before(heap_[child], moving)) break;
    Place(index, std::move(heap_[child]));
    index = child;
  }
  Place(index, std::move(moving));
}

bool JobPriorityQueue::Push(const SchedulingEntry& entry) {
  const auto existing = position_.find(entry.id.str());
  if (existing != position_.end()) {  // duplicate: replace and re-sort
    const std::size_t index = existing->second;
    heap_[index] = entry;
    SiftUp(index);
    SiftDown(position_[entry.id.str()]);
    return false;
  }
  heap_.push_back(entry);
  position_[entry.id.str()] = heap_.size() - 1;
  SiftUp(heap_.size() - 1);
  return true;
}

const SchedulingEntry* JobPriorityQueue::Top() const noexcept {
  return heap_.empty() ? nullptr : &heap_.front();
}

void JobPriorityQueue::RemoveAt(std::size_t index) {
  position_.erase(heap_[index].id.str());
  const std::size_t last = heap_.size() - 1;
  if (index == last) {
    heap_.pop_back();
    return;
  }
  SchedulingEntry tail = std::move(heap_[last]);
  heap_.pop_back();
  const std::string moved_id = tail.id.str();
  Place(index, std::move(tail));
  SiftUp(index);
  SiftDown(position_.at(moved_id));
}

std::optional<SchedulingEntry> JobPriorityQueue::Pop() {
  if (heap_.empty()) return std::nullopt;
  SchedulingEntry top = heap_.front();
  RemoveAt(0);
  return top;
}

bool JobPriorityQueue::Remove(const JobId& id) {
  const auto found = position_.find(id.str());
  if (found == position_.end()) return false;
  RemoveAt(found->second);
  return true;
}

std::size_t JobPriorityQueue::RemoveIf(const std::function<bool(const SchedulingEntry&)>& is_stale) {
  std::vector<SchedulingEntry> keep;
  keep.reserve(heap_.size());
  std::size_t removed = 0;
  for (auto& entry : heap_) {
    if (is_stale(entry)) {
      ++removed;
    } else {
      keep.push_back(std::move(entry));
    }
  }
  if (removed == 0) {
    heap_ = std::move(keep);  // moved-from entries restored unchanged
    return 0;
  }
  Clear();
  for (const auto& entry : keep) Push(entry);
  return removed;
}

bool JobPriorityQueue::Contains(const JobId& id) const { return position_.count(id.str()) > 0; }

void JobPriorityQueue::Clear() {
  heap_.clear();
  position_.clear();
}

}  // namespace taskflow
