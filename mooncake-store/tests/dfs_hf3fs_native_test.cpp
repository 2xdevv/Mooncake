// Live tests for Hf3fsNativeAdapter. They need a running 3FS cluster, the
// libmooncake_hf3fs_native.so shim, and:
//   MOONCAKE_HF3FS_NATIVE_CONFIG  shim config (e.g. the FUSE launcher TOML)
//   MOONCAKE_DFS_ROOT_DIR         writable test root under the 3FS mount
//   MOONCAKE_HF3FS_NATIVE_LIB     shim path, if not on the loader path
// The FUSE mount is used to cross-check what the native client writes.
// Tests skip when the variables are unset.

#include <fcntl.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <atomic>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <string>
#include <vector>

#include "storage/distributed/distributed_storage_backend.h"
#include "storage/distributed/hf3fs_native_adapter.h"
#include "storage/distributed/shard_allocator.h"
#include "storage_backend.h"

namespace mooncake::test {
namespace {

class Hf3fsNativeLiveTest : public ::testing::Test {
   protected:
    void SetUp() override {
        const char* config = std::getenv("MOONCAKE_HF3FS_NATIVE_CONFIG");
        const char* root = std::getenv("MOONCAKE_DFS_ROOT_DIR");
        if (!config || !*config || !root || !*root) {
            GTEST_SKIP() << "set MOONCAKE_HF3FS_NATIVE_CONFIG and "
                            "MOONCAKE_DFS_ROOT_DIR to run live 3FS tests";
        }
        static std::atomic<int> counter{0};
        dir_ = std::filesystem::path(root) /
               ("native_test_" + std::to_string(::getpid()) + "_" +
                std::to_string(++counter));
        std::filesystem::create_directories(dir_);
    }

    void TearDown() override {
        std::error_code ec;
        if (!dir_.empty()) std::filesystem::remove_all(dir_, ec);
    }

    std::string File(const std::string& name) const {
        return (dir_ / name).string();
    }

    static std::string Pattern(size_t size, unsigned seed) {
        std::string data(size, '\0');
        for (size_t i = 0; i < size; ++i) {
            data[i] =
                static_cast<char>((i * 131 + seed * 17 + i / 4096) & 0xff);
        }
        return data;
    }

    static std::string ReadThroughFuse(const std::string& path, size_t offset,
                                       size_t len) {
        std::string out(len, '\0');
        int fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) return {};
        ssize_t got = ::pread(fd, out.data(), len, static_cast<off_t>(offset));
        ::close(fd);
        if (got != static_cast<ssize_t>(len)) return {};
        return out;
    }

    std::filesystem::path dir_;
};

TEST_F(Hf3fsNativeLiveTest, RoundTripAcrossChunksAndFuseInterop) {
    Hf3fsNativeAdapter adapter;
    ASSERT_TRUE(adapter.Init(dir_.string()));

    constexpr size_t kFileSize = 16 << 20;
    const auto path = File("interop.data");
    ASSERT_TRUE(adapter.PreallocateFile(path, kFileSize));
    auto size = adapter.GetFileSize(path);
    ASSERT_TRUE(size);
    EXPECT_EQ(*size, kFileSize);
    auto files = adapter.ListFiles(dir_.string());
    ASSERT_TRUE(files);
    EXPECT_EQ(*files, std::vector<std::string>{"interop.data"});

    auto fd = adapter.OpenExistingFile(path);
    ASSERT_TRUE(fd);

    // Fill the whole file first. 3FS FUSE returns a short read for unwritten
    // chunks, which the kernel treats as EOF, so buffered FUSE reads of a
    // sparse file can stop early regardless of how the data was written.
    auto fill = Pattern(kFileSize, 1000);
    iovec fill_iov{fill.data(), fill.size()};
    auto filled = adapter.WriteAt(*fd, &fill_iov, 1, 0);
    ASSERT_TRUE(filled);
    EXPECT_EQ(*filled, kFileSize);

    // Unaligned requests that straddle chunk boundaries, split into iovecs.
    struct Span {
        size_t offset;
        size_t len;
    };
    const std::vector<Span> spans = {{0, 4096},
                                     {(1 << 20) - 100, 300},
                                     {3 << 20, 5 << 20},
                                     {15 << 20, 1 << 20}};
    std::vector<std::string> values;
    std::vector<std::vector<iovec>> iovs;
    std::vector<FdIoRequest> writes;
    for (size_t i = 0; i < spans.size(); ++i) {
        values.push_back(Pattern(spans[i].len, static_cast<unsigned>(i)));
        const size_t half = spans[i].len / 2;
        iovs.push_back({{values[i].data(), half},
                        {values[i].data() + half, spans[i].len - half}});
    }
    for (size_t i = 0; i < spans.size(); ++i) {
        writes.push_back(
            {*fd, iovs[i].data(), 2, static_cast<int64_t>(spans[i].offset)});
    }
    auto written = adapter.BatchWriteAt(writes);
    ASSERT_EQ(written.size(), spans.size());
    for (size_t i = 0; i < spans.size(); ++i) {
        ASSERT_TRUE(written[i]) << i;
        EXPECT_EQ(*written[i], spans[i].len) << i;
    }

    std::vector<std::string> outputs;
    std::vector<iovec> read_iovs;
    std::vector<FdIoRequest> reads;
    for (const auto& span : spans) outputs.emplace_back(span.len, '\0');
    for (size_t i = 0; i < spans.size(); ++i) {
        read_iovs.push_back({outputs[i].data(), outputs[i].size()});
    }
    for (size_t i = 0; i < spans.size(); ++i) {
        reads.push_back(
            {*fd, &read_iovs[i], 1, static_cast<int64_t>(spans[i].offset)});
    }
    auto read = adapter.BatchReadAt(reads);
    ASSERT_EQ(read.size(), spans.size());
    for (size_t i = 0; i < spans.size(); ++i) {
        ASSERT_TRUE(read[i]) << i;
        EXPECT_TRUE(outputs[i] == values[i]) << "native read mismatch " << i;
        EXPECT_TRUE(ReadThroughFuse(path, spans[i].offset, spans[i].len) ==
                    values[i])
            << "FUSE read mismatch " << i;
    }

    // Data written through FUSE is visible to the native client.
    const auto fuse_value = Pattern(2 << 20, 99);
    {
        int raw = ::open(path.c_str(), O_WRONLY);
        ASSERT_GE(raw, 0);
        ASSERT_EQ(::pwrite(raw, fuse_value.data(), fuse_value.size(), 9 << 20),
                  static_cast<ssize_t>(fuse_value.size()));
        ASSERT_EQ(::fsync(raw), 0);
        ::close(raw);
    }
    std::string out(fuse_value.size(), '\0');
    iovec riov{out.data(), out.size()};
    auto fuse_read = adapter.ReadAt(*fd, &riov, 1, 9 << 20);
    ASSERT_TRUE(fuse_read);
    EXPECT_TRUE(out == fuse_value);

    EXPECT_TRUE(adapter.CloseFile(*fd));
    EXPECT_TRUE(adapter.DeleteFile(path));
    auto exists = adapter.FileExists(path);
    ASSERT_TRUE(exists);
    EXPECT_FALSE(*exists);
}

TEST_F(Hf3fsNativeLiveTest, DistributedBackendBatchWriteAndRead) {
    DistributedStorageConfig config;
    config.fsdir = dir_.string();
    config.fs_adapter_type = "hf3fs_native";
    config.shard_count = 2;
    config.shard_capacity = 64 << 20;
    config.alignment = 4096;

    // The master creates the shard files through the native client.
    ShardAllocator allocator;
    ASSERT_TRUE(allocator.Init(config));

    FileStorageConfig file_config;
    DistributedStorageBackend backend(file_config, config,
                                      std::make_unique<Hf3fsNativeAdapter>());
    ASSERT_TRUE(backend.Init());

    constexpr size_t kObjects = 24;
    constexpr size_t kObjectSize = (1 << 20) + 4096;
    std::vector<std::string> values;
    std::vector<DfsWriteRequest> writes;
    values.reserve(kObjects);
    for (size_t i = 0; i < kObjects; ++i) {
        const auto key = "native_key_" + std::to_string(i);
        auto descriptor = allocator.Allocate(key, kObjectSize);
        ASSERT_TRUE(descriptor) << i;
        values.push_back(Pattern(kObjectSize, static_cast<unsigned>(i)));
        writes.push_back(
            {key, *descriptor, {{values.back().data(), kObjectSize}}});
    }
    auto write_results = backend.BatchWrite(writes);
    ASSERT_EQ(write_results.size(), kObjects);
    for (size_t i = 0; i < kObjects; ++i) ASSERT_TRUE(write_results[i]) << i;

    std::vector<std::string> outputs(kObjects, std::string(kObjectSize, '\0'));
    std::vector<DfsReadRequest> reads;
    for (size_t i = 0; i < kObjects; ++i) {
        reads.push_back({writes[i].key,
                         writes[i].descriptor,
                         {{outputs[i].data(), kObjectSize}}});
    }
    auto read_results = backend.BatchRead(reads);
    ASSERT_EQ(read_results.size(), kObjects);
    for (size_t i = 0; i < kObjects; ++i) {
        ASSERT_TRUE(read_results[i]) << i;
        EXPECT_TRUE(outputs[i] == values[i]) << i;
    }
}

}  // namespace
}  // namespace mooncake::test
