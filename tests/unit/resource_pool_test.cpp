#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include "taskflow/common/errors.hpp"
#include "taskflow/common/resource_pool.hpp"

namespace taskflow {
namespace {

using namespace std::chrono_literals;

struct Widget {
  explicit Widget(int id_in) : id(id_in) {}
  int id;
  bool healthy = true;
};

class ResourcePoolTest : public ::testing::Test {
 protected:
  ResourcePool<Widget> MakePool(std::size_t capacity, std::chrono::milliseconds timeout = 100ms) {
    return ResourcePool<Widget>(
        capacity,
        [this] {
          ++created_;
          if (fail_next_) {
            fail_next_ = false;
            throw DatabaseError("factory failure");
          }
          return std::make_unique<Widget>(created_);
        },
        [](Widget& w) { return w.healthy; }, timeout);
  }

  std::atomic<int> created_{0};
  bool fail_next_ = false;
};

TEST_F(ResourcePoolTest, ReusesReturnedResources) {
  auto pool = MakePool(2);
  int first_id = 0;
  {
    auto lease = pool.Acquire();
    first_id = lease->id;
  }
  EXPECT_EQ(pool.IdleCount(), 1u);
  EXPECT_EQ(pool.Acquire()->id, first_id);
  EXPECT_EQ(created_.load(), 1);
}

TEST_F(ResourcePoolTest, CreatesResourcesLazilyUpToCapacity) {
  auto pool = MakePool(2);
  EXPECT_EQ(created_.load(), 0);
  auto a = pool.Acquire();
  auto b = pool.Acquire();
  EXPECT_EQ(created_.load(), 2);
  EXPECT_NE(a->id, b->id);
}

TEST_F(ResourcePoolTest, TimesOutWhenExhausted) {
  auto pool = MakePool(1, 50ms);
  auto held = pool.Acquire();
  const auto start = std::chrono::steady_clock::now();
  EXPECT_THROW(pool.Acquire(), PoolTimeoutError);
  EXPECT_GE(std::chrono::steady_clock::now() - start, 40ms);
}

TEST_F(ResourcePoolTest, WaiterIsWokenWhenALeaseIsReleased) {
  auto pool = MakePool(1, 2000ms);
  auto held = pool.Acquire();
  std::thread releaser([&] {
    std::this_thread::sleep_for(50ms);
    held = {};
  });
  EXPECT_NO_THROW(pool.Acquire());
  releaser.join();
}

TEST_F(ResourcePoolTest, BrokenResourcesAreDiscardedAndReplaced) {
  auto pool = MakePool(1);
  {
    auto lease = pool.Acquire();
    lease.MarkBroken();
  }
  EXPECT_EQ(pool.IdleCount(), 0u);
  auto replacement = pool.Acquire();  // the slot was freed, so this must not block
  EXPECT_EQ(replacement->id, 2);
}

TEST_F(ResourcePoolTest, ResourcesFailingValidationAreReplaced) {
  auto pool = MakePool(1);
  {
    auto lease = pool.Acquire();
    lease->healthy = false;  // goes idle while unhealthy
  }
  auto lease = pool.Acquire();
  EXPECT_EQ(lease->id, 2);
  EXPECT_TRUE(lease->healthy);
}

TEST_F(ResourcePoolTest, FactoryFailureDoesNotLeakACapacitySlot) {
  auto pool = MakePool(1);
  fail_next_ = true;
  EXPECT_THROW(pool.Acquire(), DatabaseError);
  EXPECT_NO_THROW(pool.Acquire());  // capacity 1 would be exhausted if the slot leaked
}

TEST_F(ResourcePoolTest, RejectsZeroCapacity) {
  EXPECT_THROW(MakePool(0), Error);
}

TEST_F(ResourcePoolTest, NeverExceedsCapacityUnderContention) {
  constexpr int kCapacity = 3;
  auto pool = MakePool(kCapacity, 5000ms);
  std::atomic<int> in_use{0};
  std::atomic<int> max_in_use{0};

  std::vector<std::thread> threads;
  for (int t = 0; t < 12; ++t) {
    threads.emplace_back([&] {
      for (int i = 0; i < 50; ++i) {
        auto lease = pool.Acquire();
        const int now = ++in_use;
        int seen = max_in_use.load();
        while (now > seen && !max_in_use.compare_exchange_weak(seen, now)) {}
        std::this_thread::sleep_for(100us);
        --in_use;
      }
    });
  }
  for (auto& thread : threads) thread.join();

  EXPECT_LE(max_in_use.load(), kCapacity);
  EXPECT_LE(created_.load(), kCapacity);
  EXPECT_EQ(in_use.load(), 0);
}

}  // namespace
}  // namespace taskflow
