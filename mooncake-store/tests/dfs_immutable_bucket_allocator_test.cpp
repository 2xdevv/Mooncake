#include <atomic>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <fstream>
#include <future>
#include <limits>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "config/distributed_storage_config.h"
#include "storage/distributed/bucket_entry_layout.h"
#include "storage/distributed/immutable_bucket_allocator.h"
#include "storage/distributed/posix_fs_adapter.h"

namespace mooncake {
namespace {

// Holds bucket creation open until released, standing in for a filesystem
// whose create and resize take metadata round trips.
struct PreallocateGate {
    std::mutex mutex;
    std::condition_variable changed;
    bool hold = false;
    bool fail_next = false;
    int started = 0;

    void Hold() {
        std::lock_guard lock(mutex);
        hold = true;
    }
    void Release() {
        std::lock_guard lock(mutex);
        hold = false;
        changed.notify_all();
    }
    bool WaitStarted(int count) {
        std::unique_lock lock(mutex);
        return changed.wait_for(lock, std::chrono::seconds(10),
                                [&] { return started >= count; });
    }
};

class GatedPosixAdapter : public PosixFsAdapter {
   public:
    explicit GatedPosixAdapter(std::shared_ptr<PreallocateGate> gate)
        : gate_(std::move(gate)) {}

    tl::expected<void, ErrorCode> PreallocateFile(const std::string& path,
                                                  uint64_t size) override {
        bool fail = false;
        {
            std::unique_lock lock(gate_->mutex);
            ++gate_->started;
            gate_->changed.notify_all();
            gate_->changed.wait(lock, [&] { return !gate_->hold; });
            fail = std::exchange(gate_->fail_next, false);
        }
        if (fail) return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
        return PosixFsAdapter::PreallocateFile(path, size);
    }

   private:
    std::shared_ptr<PreallocateGate> gate_;
};

class TempDir {
   public:
    TempDir() {
        static std::atomic<uint64_t> sequence{0};
        path_ =
            std::filesystem::temp_directory_path() /
            ("mooncake-bucket-allocator-" +
             std::to_string(sequence.fetch_add(1)) + "-" +
             std::to_string(
                 std::chrono::steady_clock::now().time_since_epoch().count()));
        std::filesystem::create_directories(path_);
    }

    ~TempDir() {
        std::error_code error;
        std::filesystem::remove_all(path_, error);
    }

    std::string string() const { return path_.string(); }
    std::filesystem::path file(const std::string& name) const {
        return path_ / name;
    }

   private:
    std::filesystem::path path_;
};

DistributedStorageConfig BucketConfig(const TempDir& dir) {
    DistributedStorageConfig config;
    config.fsdir = dir.string();
    config.fs_adapter_type = "posix";
    config.allocator_type = "bucket";
    config.alignment = 4096;
    config.bucket_capacity = 8192;
    config.max_bucket_count = 8;
    config.eviction_enabled = true;
    config.eviction_high_watermark = 0.01;
    config.eviction_low_watermark = 0.0;
    return config;
}

TEST(BucketEntryLayoutTest, ComputesValueAndPaddingWithOverflowChecks) {
    auto layout = ComputeBucketEntryLayout(1, 4097, 4096);
    ASSERT_TRUE(layout);
    EXPECT_EQ(layout->offset, 4096u);
    EXPECT_EQ(layout->object_size, 4097u);
    EXPECT_EQ(layout->aligned_size, 8192u);
    EXPECT_EQ(layout->end(), 12288u);

    const auto descriptor =
        MakeBucketDescriptor("/dfs/bucket_000007.data", *layout, 7);
    EXPECT_EQ(descriptor.file_path, "/dfs/bucket_000007.data");
    EXPECT_EQ(descriptor.offset, layout->offset);
    EXPECT_EQ(descriptor.object_size, layout->object_size);
    EXPECT_EQ(descriptor.aligned_size, layout->aligned_size);
    EXPECT_EQ(descriptor.shard_idx, 7);

    EXPECT_FALSE(CheckedAlignUp(std::numeric_limits<uint64_t>::max(), 4096));
    EXPECT_FALSE(ComputeBucketEntryLayout(0, 1, 3));
    EXPECT_FALSE(ComputeBucketEntryLayout(0, 0, 4096));
}

TEST(ImmutableBucketAllocatorTest, AppendsAndPacksBatchAcrossBuckets) {
    TempDir dir;
    ImmutableBucketAllocator allocator;
    ASSERT_TRUE(allocator.Init(BucketConfig(dir)));

    const std::vector<BatchAllocateRequest> requests = {
        {"a", 3000}, {"b", 3000}, {"c", 3000}};
    auto results = allocator.BatchAllocate(requests);
    ASSERT_EQ(results.size(), requests.size());
    ASSERT_TRUE(results[0].success);
    ASSERT_TRUE(results[1].success);
    ASSERT_TRUE(results[2].success);
    EXPECT_EQ(results[0].descriptor.shard_idx, results[1].descriptor.shard_idx);
    EXPECT_NE(results[1].descriptor.shard_idx, results[2].descriptor.shard_idx);
    EXPECT_EQ(results[0].descriptor.offset, 0u);
    EXPECT_EQ(results[1].descriptor.offset, 4096u);
    EXPECT_EQ(results[2].descriptor.offset, 0u);
    EXPECT_EQ(allocator.GetBucketCount(), 2u);
    // GetFileCount mirrors the immutable bucket file count in bucket mode.
    EXPECT_EQ(allocator.GetFileCount(), 2u);
}

TEST(ImmutableBucketAllocatorTest, DuplicateBatchRollsBackWithoutOffsetReuse) {
    TempDir dir;
    ImmutableBucketAllocator allocator;
    ASSERT_TRUE(allocator.Init(BucketConfig(dir)));

    auto rolled_back = allocator.BatchAllocate({{"same", 100}, {"same", 100}});
    ASSERT_EQ(rolled_back.size(), 2u);
    EXPECT_FALSE(rolled_back[0].success);
    EXPECT_FALSE(rolled_back[1].success);
    EXPECT_TRUE(rolled_back[0].descriptor.file_path.empty());
    EXPECT_FALSE(allocator.GetBucketIdForKey("same"));

    auto replacement = allocator.Allocate("same", 100);
    ASSERT_TRUE(replacement);
    EXPECT_EQ(replacement->offset, 4096u);
    EXPECT_FALSE(allocator.MarkCommitted("same", rolled_back[0].descriptor));
    EXPECT_TRUE(allocator.MarkCommitted("same", *replacement));
}

TEST(ImmutableBucketAllocatorTest, StaleFreeCannotTombstoneReplacement) {
    TempDir dir;
    ImmutableBucketAllocator allocator;
    ASSERT_TRUE(allocator.Init(BucketConfig(dir)));

    auto first = allocator.Allocate("key", 100);
    ASSERT_TRUE(first);
    allocator.Free("key", *first);
    auto replacement = allocator.Allocate("key", 100);
    ASSERT_TRUE(replacement);
    ASSERT_NE(first->offset, replacement->offset);
    allocator.Free("key", *first);
    EXPECT_TRUE(allocator.MarkCommitted("key", *replacement));
}

TEST(ImmutableBucketAllocatorTest, TombstonesPreserveConsumedBytesForEviction) {
    TempDir dir;
    auto config = BucketConfig(dir);
    config.bucket_capacity = 4096;
    config.max_bucket_count = 2;
    config.eviction_high_watermark = 0.75;
    config.eviction_low_watermark = 0.25;
    ImmutableBucketAllocator allocator;
    ASSERT_TRUE(allocator.Init(config));

    auto stale = allocator.Allocate("stale", 100);
    auto active = allocator.Allocate("active", 100);
    ASSERT_TRUE(stale);
    ASSERT_TRUE(active);
    ASSERT_TRUE(allocator.MarkCommitted("stale", *stale));
    ASSERT_TRUE(allocator.MarkCommitted("active", *active));
    const uint64_t consumed = allocator.GetUsedBytes();
    ASSERT_EQ(consumed, 2 * config.bucket_capacity);

    allocator.Free("stale", *stale);
    EXPECT_EQ(allocator.GetUsedBytes(), consumed);

    auto eviction = allocator.PrepareEviction();
    ASSERT_FALSE(eviction.Empty());
    EXPECT_EQ(eviction.bucket_id(), stale->shard_idx);
    allocator.AbortEviction(std::move(eviction));
}

TEST(ImmutableBucketAllocatorTest, PendingEntriesProtectSealedBucket) {
    TempDir dir;
    auto config = BucketConfig(dir);
    config.bucket_capacity = 4096;
    ImmutableBucketAllocator allocator;
    ASSERT_TRUE(allocator.Init(config));

    auto pending = allocator.Allocate("pending", 100);
    auto active = allocator.Allocate("active", 100);
    ASSERT_TRUE(pending);
    ASSERT_TRUE(active);
    EXPECT_TRUE(allocator.PrepareEvictionForAllocationFailure().Empty());

    ASSERT_TRUE(allocator.MarkCommitted("pending", *pending));
    auto eviction = allocator.PrepareEvictionForAllocationFailure();
    ASSERT_FALSE(eviction.Empty());
    ASSERT_EQ(eviction.Candidates().size(), 1u);
    EXPECT_EQ(eviction.Candidates()[0].key, "pending");
    allocator.AbortEviction(std::move(eviction));
}

TEST(ImmutableBucketAllocatorTest, RetiresWholeBucketAfterDelete) {
    TempDir dir;
    auto config = BucketConfig(dir);
    config.bucket_capacity = 4096;
    ImmutableBucketAllocator allocator;
    ASSERT_TRUE(allocator.Init(config));

    auto cold = allocator.Allocate("cold", 100);
    auto active = allocator.Allocate("active", 100);
    ASSERT_TRUE(cold);
    ASSERT_TRUE(active);
    ASSERT_TRUE(allocator.MarkCommitted("cold", *cold));
    ASSERT_TRUE(allocator.MarkCommitted("active", *active));
    auto eviction = allocator.PrepareEvictionForAllocationFailure();
    ASSERT_FALSE(eviction.Empty());
    const auto path = eviction.Candidates().front().descriptor.file_path;
    auto logical = allocator.CommitEvictionLogical(std::move(eviction));
    ASSERT_TRUE(logical);
    EXPECT_TRUE(std::filesystem::exists(path));
    EXPECT_EQ(allocator.GetBucketCount(), 2u);
    EXPECT_FALSE(allocator.GetBucketIdForKey("cold"));

    ASSERT_TRUE(allocator.DeleteEvictedBucket(std::move(*logical)));
    EXPECT_FALSE(std::filesystem::exists(path));
    EXPECT_EQ(allocator.GetBucketCount(), 1u);
    // GetFileCount tracks the bucket count after eviction deletes a bucket.
    EXPECT_EQ(allocator.GetFileCount(), 1u);
    EXPECT_FALSE(allocator.GetBucketIdForKey("cold"));
}

TEST(ImmutableBucketAllocatorTest, DeferredDeletionWaitsForItsTime) {
    TempDir dir;
    auto config = BucketConfig(dir);
    config.bucket_capacity = 4096;
    ImmutableBucketAllocator allocator;
    ASSERT_TRUE(allocator.Init(config));

    auto cold = allocator.Allocate("cold", 100);
    auto active = allocator.Allocate("active", 100);
    ASSERT_TRUE(cold);
    ASSERT_TRUE(active);
    ASSERT_TRUE(allocator.MarkCommitted("cold", *cold));
    ASSERT_TRUE(allocator.MarkCommitted("active", *active));
    auto eviction = allocator.PrepareEvictionForAllocationFailure();
    ASSERT_FALSE(eviction.Empty());
    const auto path = eviction.Candidates().front().descriptor.file_path;
    auto logical = allocator.CommitEvictionLogical(std::move(eviction));
    ASSERT_TRUE(logical);

    const auto not_before =
        std::chrono::system_clock::now() + std::chrono::milliseconds(200);
    allocator.DeferDeletion(std::move(*logical), not_before);
    // Until its time the file and its capacity stay.
    EXPECT_EQ(allocator.RetryFailedEvictions(), 0u);
    EXPECT_TRUE(std::filesystem::exists(path));
    EXPECT_EQ(allocator.GetBucketCount(), 2u);
    EXPECT_EQ(allocator.GetUsedBytes(), 2 * config.bucket_capacity);

    std::this_thread::sleep_until(not_before);
    EXPECT_EQ(allocator.RetryFailedEvictions(), 1u);
    EXPECT_FALSE(std::filesystem::exists(path));
    EXPECT_EQ(allocator.GetBucketCount(), 1u);
}

TEST(ImmutableBucketAllocatorTest,
     LogicalEvictionAllowsSameKeyBeforePhysicalDelete) {
    TempDir dir;
    auto config = BucketConfig(dir);
    config.bucket_capacity = 4096;
    ImmutableBucketAllocator allocator;
    ASSERT_TRUE(allocator.Init(config));

    auto old = allocator.Allocate("same", 100);
    auto active = allocator.Allocate("active", 100);
    ASSERT_TRUE(old);
    ASSERT_TRUE(active);
    ASSERT_TRUE(allocator.MarkCommitted("same", *old));
    ASSERT_TRUE(allocator.MarkCommitted("active", *active));

    auto eviction = allocator.PrepareEvictionForAllocationFailure();
    ASSERT_FALSE(eviction.Empty());
    ASSERT_EQ(eviction.Candidates().size(), 1u);
    ASSERT_EQ(eviction.Candidates().front().key, "same");
    const std::filesystem::path old_bucket_path = old->file_path;

    auto logical = allocator.CommitEvictionLogical(std::move(eviction));
    ASSERT_TRUE(logical);
    EXPECT_TRUE(std::filesystem::exists(old_bucket_path));
    EXPECT_FALSE(allocator.GetBucketIdForKey("same"));

    auto replacement = allocator.Allocate("same", 100);
    ASSERT_TRUE(replacement);
    ASSERT_NE(replacement->shard_idx, old->shard_idx);
    ASSERT_TRUE(allocator.MarkCommitted("same", *replacement));

    ASSERT_TRUE(allocator.DeleteEvictedBucket(std::move(*logical)));
    EXPECT_FALSE(std::filesystem::exists(old_bucket_path));
    EXPECT_EQ(allocator.GetBucketCount(), 2u);
    const auto replacement_bucket = allocator.GetBucketIdForKey("same");
    ASSERT_TRUE(replacement_bucket);
    EXPECT_EQ(*replacement_bucket, replacement->shard_idx);
}

TEST(ImmutableBucketAllocatorTest,
     FailedEvictionDeleteAllowsSameKeyReallocation) {
    TempDir dir;
    auto config = BucketConfig(dir);
    ImmutableBucketAllocator allocator;
    ASSERT_TRUE(allocator.Init(config));

    auto old = allocator.Allocate("same", 100);
    auto active = allocator.Allocate("active", 4097);
    ASSERT_TRUE(old);
    ASSERT_TRUE(active);
    ASSERT_NE(old->shard_idx, active->shard_idx);
    ASSERT_TRUE(allocator.MarkCommitted("same", *old));
    ASSERT_TRUE(allocator.MarkCommitted("active", *active));

    auto eviction = allocator.PrepareEvictionForAllocationFailure();
    ASSERT_FALSE(eviction.Empty());
    ASSERT_EQ(eviction.Candidates().size(), 1u);
    ASSERT_EQ(eviction.Candidates().front().key, "same");
    const auto old_bucket_id = old->shard_idx;
    const std::filesystem::path old_bucket_path = old->file_path;

    ASSERT_TRUE(std::filesystem::remove(old_bucket_path));
    ASSERT_TRUE(std::filesystem::create_directory(old_bucket_path));
    std::ofstream(old_bucket_path / "keep") << "force unlink failure";

    auto logical = allocator.CommitEvictionLogical(std::move(eviction));
    ASSERT_TRUE(logical);
    EXPECT_FALSE(allocator.GetBucketIdForKey("same"));
    EXPECT_EQ(allocator.GetBucketCount(), 2u);
    EXPECT_EQ(allocator.GetUsedBytes(), 2 * config.bucket_capacity);

    auto replacement = allocator.Allocate("same", 100);
    ASSERT_TRUE(replacement);
    ASSERT_NE(replacement->shard_idx, old_bucket_id);
    ASSERT_TRUE(allocator.MarkCommitted("same", *replacement));
    const auto replacement_bucket = allocator.GetBucketIdForKey("same");
    ASSERT_TRUE(replacement_bucket);
    EXPECT_EQ(*replacement_bucket, replacement->shard_idx);
    EXPECT_EQ(allocator.GetBucketCount(), 3u);
    EXPECT_EQ(allocator.GetUsedBytes(),
              2 * config.bucket_capacity + config.alignment);

    auto deleted = allocator.DeleteEvictedBucket(std::move(*logical));
    ASSERT_FALSE(deleted);
    EXPECT_EQ(deleted.error(), ErrorCode::FILE_WRITE_FAIL);

    ASSERT_TRUE(std::filesystem::remove_all(old_bucket_path));
    std::ofstream(old_bucket_path) << "retry deletion";
    EXPECT_EQ(allocator.RetryFailedEvictions(), 1u);

    EXPECT_FALSE(std::filesystem::exists(old_bucket_path));
    EXPECT_EQ(allocator.GetBucketCount(), 2u);
    const auto preserved_bucket = allocator.GetBucketIdForKey("same");
    ASSERT_TRUE(preserved_bucket);
    EXPECT_EQ(*preserved_bucket, replacement->shard_idx);
    EXPECT_EQ(allocator.GetUsedBytes(),
              config.bucket_capacity + config.alignment);
}

TEST(ImmutableBucketAllocatorTest, RejectsExistingBucketArtifacts) {
    TempDir dir;
    std::ofstream(dir.file("bucket_000000.data")) << "old";
    ImmutableBucketAllocator allocator;
    auto initialized = allocator.Init(BucketConfig(dir));
    ASSERT_FALSE(initialized);
    EXPECT_EQ(initialized.error(), ErrorCode::INVALID_PARAMS);
}

TEST(ImmutableBucketAllocatorTest, BucketCreationDoesNotBlockOtherCalls) {
    TempDir dir;
    auto config = BucketConfig(dir);
    config.bucket_capacity = 4096;
    auto gate = std::make_shared<PreallocateGate>();
    ImmutableBucketAllocator allocator;
    ASSERT_TRUE(
        allocator.Init(config, std::make_unique<GatedPosixAdapter>(gate)));

    auto committed = allocator.Allocate("committed", 100);
    auto pending = allocator.Allocate("pending", 100);
    auto freed = allocator.Allocate("freed", 100);
    ASSERT_TRUE(committed);
    ASSERT_TRUE(pending);
    ASSERT_TRUE(freed);
    ASSERT_TRUE(allocator.MarkCommitted("committed", *committed));

    // The active bucket is full, so the next allocation creates a bucket.
    gate->Hold();
    auto creating = std::async(std::launch::async,
                               [&] { return allocator.Allocate("next", 100); });
    ASSERT_TRUE(gate->WaitStarted(4));

    auto others = std::async(std::launch::async, [&] {
        allocator.UpdateAccess("committed", *committed);
        allocator.Free("freed", *freed);
        const bool marked = allocator.MarkCommitted("pending", *pending);
        allocator.AbortEviction(allocator.PrepareEviction());
        return marked && allocator.GetUsedBytes() == 3 * config.bucket_capacity;
    });
    const auto status = others.wait_for(std::chrono::seconds(10));
    gate->Release();
    ASSERT_EQ(status, std::future_status::ready);
    EXPECT_TRUE(others.get());
    auto next = creating.get();
    ASSERT_TRUE(next);
    EXPECT_EQ(next->shard_idx, 3);
    EXPECT_EQ(allocator.GetBucketCount(), 4u);
}

TEST(ImmutableBucketAllocatorTest, WaitersRetryAfterFailedBucketCreation) {
    TempDir dir;
    auto config = BucketConfig(dir);
    auto gate = std::make_shared<PreallocateGate>();
    ImmutableBucketAllocator allocator;
    ASSERT_TRUE(
        allocator.Init(config, std::make_unique<GatedPosixAdapter>(gate)));

    gate->Hold();
    gate->fail_next = true;
    auto failed = std::async(std::launch::async,
                             [&] { return allocator.Allocate("a", 100); });
    ASSERT_TRUE(gate->WaitStarted(1));
    // These wait for the creation in flight instead of starting their own.
    auto first = std::async(std::launch::async,
                            [&] { return allocator.Allocate("b", 100); });
    auto second = std::async(std::launch::async,
                             [&] { return allocator.Allocate("c", 100); });
    gate->Release();

    auto failed_result = failed.get();
    ASSERT_FALSE(failed_result);
    EXPECT_EQ(failed_result.error(), ErrorCode::FILE_WRITE_FAIL);
    auto first_result = first.get();
    auto second_result = second.get();
    ASSERT_TRUE(first_result);
    ASSERT_TRUE(second_result);
    // One retried creation serves both waiters.
    EXPECT_EQ(first_result->shard_idx, second_result->shard_idx);
    EXPECT_EQ(allocator.GetBucketCount(), 1u);
    EXPECT_EQ(first_result->shard_idx, 1);
    EXPECT_EQ(gate->started, 2);
    EXPECT_FALSE(std::filesystem::exists(dir.file("bucket_000000.data")));
}

TEST(ImmutableBucketAllocatorTest, ConcurrentAllocationDoesNotOverlap) {
    TempDir dir;
    auto config = BucketConfig(dir);
    config.bucket_capacity = 4096 * 16;
    config.max_bucket_count = 16;
    ImmutableBucketAllocator allocator;
    ASSERT_TRUE(allocator.Init(config));

    constexpr int kThreads = 8;
    constexpr int kPerThread = 16;
    std::mutex results_mutex;
    std::atomic<bool> allocation_failed{false};
    std::vector<DistributedFSDescriptor> descriptors;
    std::vector<std::thread> threads;
    for (int thread = 0; thread < kThreads; ++thread) {
        threads.emplace_back([&, thread] {
            for (int i = 0; i < kPerThread; ++i) {
                const std::string key =
                    std::to_string(thread) + ":" + std::to_string(i);
                auto descriptor = allocator.Allocate(key, 1);
                if (!descriptor) {
                    allocation_failed.store(true);
                    return;
                }
                std::lock_guard lock(results_mutex);
                descriptors.push_back(*descriptor);
            }
        });
    }
    for (auto& thread : threads) thread.join();

    ASSERT_FALSE(allocation_failed.load());
    ASSERT_EQ(descriptors.size(), kThreads * kPerThread);
    std::set<std::pair<int, uint64_t>> addresses;
    for (const auto& descriptor : descriptors) {
        EXPECT_TRUE(
            addresses.emplace(descriptor.shard_idx, descriptor.offset).second);
    }
}

}  // namespace
}  // namespace mooncake
