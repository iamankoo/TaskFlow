#pragma once

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

#include "taskflow/common/errors.hpp"

namespace taskflow {

// Bounded, thread-safe pool of lazily created resources (database or Redis
// connections). Acquire() blocks until a resource is free or the timeout
// expires. Resources that fail the validator, or that the caller marks broken,
// are discarded and replaced on demand.
template <typename T>
class ResourcePool {
  struct State {
    std::mutex mutex;
    std::condition_variable available;
    std::vector<std::unique_ptr<T>> idle;
    std::size_t created = 0;
    std::size_t capacity = 0;
  };

 public:
  using Factory = std::function<std::unique_ptr<T>()>;
  using Validator = std::function<bool(T&)>;

  class Lease {
   public:
    Lease() = default;
    Lease(Lease&& other) noexcept
        : state_(std::move(other.state_)),
          item_(std::move(other.item_)),
          broken_(other.broken_) {}
    Lease& operator=(Lease&& other) noexcept {
      if (this != &other) {
        Release();
        state_ = std::move(other.state_);
        item_ = std::move(other.item_);
        broken_ = other.broken_;
      }
      return *this;
    }
    Lease(const Lease&) = delete;
    Lease& operator=(const Lease&) = delete;
    ~Lease() { Release(); }

    T& operator*() const { return *item_; }
    T* operator->() const { return item_.get(); }
    explicit operator bool() const { return item_ != nullptr; }

    // Drop the resource instead of returning it to the pool.
    void MarkBroken() noexcept { broken_ = true; }

   private:
    friend class ResourcePool;
    Lease(std::shared_ptr<State> state, std::unique_ptr<T> item)
        : state_(std::move(state)), item_(std::move(item)) {}

    void Release() noexcept {
      if (!item_) return;
      {
        std::lock_guard lock(state_->mutex);
        if (broken_) {
          --state_->created;
          item_.reset();
        } else {
          state_->idle.push_back(std::move(item_));
        }
      }
      state_->available.notify_one();
      broken_ = false;
    }

    std::shared_ptr<State> state_;
    std::unique_ptr<T> item_;
    bool broken_ = false;
  };

  ResourcePool(std::size_t capacity, Factory factory, Validator validator,
               std::chrono::milliseconds acquire_timeout)
      : state_(std::make_shared<State>()),
        factory_(std::move(factory)),
        validator_(std::move(validator)),
        acquire_timeout_(acquire_timeout) {
    if (capacity == 0) throw Error(ErrorCode::kInvalidArgument, "pool capacity must be positive");
    state_->capacity = capacity;
  }

  Lease Acquire() {
    std::unique_lock lock(state_->mutex);
    const auto deadline = std::chrono::steady_clock::now() + acquire_timeout_;
    for (;;) {
      if (!state_->idle.empty()) {
        auto item = std::move(state_->idle.back());
        state_->idle.pop_back();
        lock.unlock();
        if (!validator_ || validator_(*item)) return Lease(state_, std::move(item));
        item.reset();
        lock.lock();
        --state_->created;  // stale resource: discard and retry
        continue;
      }
      if (state_->created < state_->capacity) {
        ++state_->created;  // reserve the slot before connecting outside the lock
        lock.unlock();
        std::unique_ptr<T> item;
        try {
          item = factory_();
        } catch (...) {
          lock.lock();
          --state_->created;
          lock.unlock();
          state_->available.notify_one();
          throw;
        }
        return Lease(state_, std::move(item));
      }
      if (!state_->available.wait_until(lock, deadline, [&] {
            return !state_->idle.empty() || state_->created < state_->capacity;
          })) {
        throw PoolTimeoutError("timed out waiting for a pooled resource");
      }
    }
  }

  std::size_t Capacity() const { return state_->capacity; }

  std::size_t IdleCount() const {
    std::lock_guard lock(state_->mutex);
    return state_->idle.size();
  }

 private:
  std::shared_ptr<State> state_;
  Factory factory_;
  Validator validator_;
  std::chrono::milliseconds acquire_timeout_;
};

}  // namespace taskflow
