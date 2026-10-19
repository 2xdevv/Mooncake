#include <gtest/gtest.h>

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "storage/distributed/hf3fs_native_adapter.h"

namespace mooncake {
namespace {

class ScopedEnvVar {
   public:
    ScopedEnvVar(const char* name, const char* value) : name_(name) {
        if (const char* original = std::getenv(name)) original_ = original;
        if (value) {
            setenv(name, value, 1);
        } else {
            unsetenv(name);
        }
    }

    ~ScopedEnvVar() {
        if (original_.has_value()) {
            setenv(name_.c_str(), original_->c_str(), 1);
        } else {
            unsetenv(name_.c_str());
        }
    }

    ScopedEnvVar(const ScopedEnvVar&) = delete;
    ScopedEnvVar& operator=(const ScopedEnvVar&) = delete;

   private:
    std::string name_;
    std::optional<std::string> original_;
};

// In-memory stand-in for libmooncake_hf3fs_native.so.
struct FakeFs {
    std::map<std::string, std::string> files;
    std::set<std::string> dirs;
    std::map<int32_t, std::string> handles;
    int32_t next_handle = 1;
    int creates = 0;
    int destroys = 0;
    std::vector<size_t> batch_sizes;
    // Transfers at this file offset fail.
    std::optional<uint64_t> fail_offset;
};

FakeFs* fake = nullptr;
int fake_client_token = 0;

int FakeCreate(const char*, mc_hf3fs_client** out) {
    ++fake->creates;
    *out = reinterpret_cast<mc_hf3fs_client*>(&fake_client_token);
    return 0;
}

void FakeDestroy(mc_hf3fs_client*) { ++fake->destroys; }

const char* FakeLastError() { return "fake error"; }

int FakeOpen(mc_hf3fs_client*, const char* path, int32_t* handle) {
    if (!fake->files.count(path)) return -ENOENT;
    *handle = fake->next_handle++;
    fake->handles[*handle] = path;
    return 0;
}

int FakeClose(mc_hf3fs_client*, int32_t handle) {
    return fake->handles.erase(handle) ? 0 : -EBADF;
}

int FakePreallocate(mc_hf3fs_client*, const char* path, uint64_t size) {
    auto& data = fake->files[path];
    if (data.size() < size) data.resize(size, '\0');
    return 0;
}

int FakeRemove(mc_hf3fs_client*, const char* path) {
    return fake->files.erase(path) ? 0 : -ENOENT;
}

int FakeStat(mc_hf3fs_client*, const char* path, uint64_t* size, int* is_dir) {
    if (fake->dirs.count(path)) {
        if (size) *size = 0;
        if (is_dir) *is_dir = 1;
        return 0;
    }
    auto it = fake->files.find(path);
    if (it == fake->files.end()) return -ENOENT;
    if (size) *size = it->second.size();
    if (is_dir) *is_dir = 0;
    return 0;
}

int FakeList(mc_hf3fs_client*, const char* path, mc_hf3fs_list_cb cb,
             void* ctx) {
    const std::string prefix = std::string(path) + "/";
    if (!fake->dirs.count(path)) return -ENOENT;
    for (const auto& [name, data] : fake->files) {
        if (name.starts_with(prefix)) cb(ctx, name.c_str() + prefix.size(), 0);
    }
    for (const auto& dir : fake->dirs) {
        if (dir.starts_with(prefix)) cb(ctx, dir.c_str() + prefix.size(), 1);
    }
    return 0;
}

int FakeBatchIo(mc_hf3fs_client*, int is_write, mc_hf3fs_io* ios, size_t n) {
    fake->batch_sizes.push_back(n);
    for (size_t i = 0; i < n; ++i) {
        auto& io = ios[i];
        auto handle = fake->handles.find(io.handle);
        if (handle == fake->handles.end()) {
            io.result = -EBADF;
            continue;
        }
        if (fake->fail_offset == io.offset) {
            io.result = -EIO;
            continue;
        }
        auto& data = fake->files[handle->second];
        if (io.offset + io.len > data.size()) {
            io.result = -EIO;
            continue;
        }
        if (is_write) {
            std::memcpy(data.data() + io.offset, io.buf, io.len);
        } else {
            std::memcpy(io.buf, data.data() + io.offset, io.len);
        }
        io.result = static_cast<int64_t>(io.len);
    }
    return 0;
}

const mc_hf3fs_api kFakeApi = {
    MC_HF3FS_NATIVE_ABI_VERSION,
    FakeCreate,
    FakeDestroy,
    FakeLastError,
    FakeOpen,
    FakeClose,
    FakePreallocate,
    FakeRemove,
    FakeStat,
    FakeList,
    FakeBatchIo,
};

class Hf3fsNativeAdapterTest : public ::testing::Test {
   protected:
    void SetUp() override {
        fake = &fs_;
        fs_.dirs.insert("/root");
    }

    void TearDown() override { fake = nullptr; }

    FakeFs fs_;
    ScopedEnvVar config_{"MOONCAKE_HF3FS_NATIVE_CONFIG", "/etc/fake.toml"};
};

TEST_F(Hf3fsNativeAdapterTest, InitRequiresConfig) {
    ScopedEnvVar no_config("MOONCAKE_HF3FS_NATIVE_CONFIG", nullptr);
    Hf3fsNativeAdapter adapter(&kFakeApi);
    auto init = adapter.Init("/root");
    ASSERT_FALSE(init.has_value());
    EXPECT_EQ(init.error(), ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(fs_.creates, 0);
}

TEST_F(Hf3fsNativeAdapterTest, AdaptersShareOneClientPerConfig) {
    {
        Hf3fsNativeAdapter first(&kFakeApi);
        Hf3fsNativeAdapter second(&kFakeApi);
        ASSERT_TRUE(first.Init("/root"));
        ASSERT_TRUE(second.Init("/root"));
        EXPECT_EQ(fs_.creates, 1);
        ASSERT_TRUE(first.Shutdown());
        EXPECT_EQ(fs_.destroys, 0);
    }
    EXPECT_EQ(fs_.destroys, 1);
}

TEST_F(Hf3fsNativeAdapterTest, MetadataOperations) {
    Hf3fsNativeAdapter adapter(&kFakeApi);
    ASSERT_TRUE(adapter.Init("/root"));
    fs_.dirs.insert("/root/subdir");

    auto exists = adapter.FileExists("/root/a.data");
    ASSERT_TRUE(exists);
    EXPECT_FALSE(*exists);
    ASSERT_TRUE(adapter.PreallocateFile("/root/a.data", 8192));
    exists = adapter.FileExists("/root/a.data");
    ASSERT_TRUE(exists);
    EXPECT_TRUE(*exists);
    auto size = adapter.GetFileSize("/root/a.data");
    ASSERT_TRUE(size);
    EXPECT_EQ(*size, 8192u);

    auto files = adapter.ListFiles("/root");
    ASSERT_TRUE(files);
    EXPECT_EQ(*files, std::vector<std::string>{"a.data"});

    auto missing = adapter.OpenExistingFile("/root/missing.data");
    ASSERT_FALSE(missing);
    EXPECT_EQ(missing.error(), ErrorCode::FILE_NOT_FOUND);
    EXPECT_EQ(adapter.GetFileSize("/root/missing.data").error(),
              ErrorCode::FILE_NOT_FOUND);
    EXPECT_EQ(adapter.ListFiles("/nowhere").error(), ErrorCode::FILE_NOT_FOUND);

    ASSERT_TRUE(adapter.DeleteFile("/root/a.data"));
    EXPECT_EQ(adapter.DeleteFile("/root/a.data").error(),
              ErrorCode::FILE_NOT_FOUND);
}

TEST_F(Hf3fsNativeAdapterTest, OpenFileCreatesMissingFile) {
    Hf3fsNativeAdapter adapter(&kFakeApi);
    ASSERT_TRUE(adapter.Init("/root"));
    auto fd = adapter.OpenFile("/root/new.data");
    ASSERT_TRUE(fd);
    EXPECT_TRUE(fs_.files.count("/root/new.data"));
    EXPECT_TRUE(adapter.CloseFile(*fd));
    EXPECT_EQ(adapter.CloseFile(*fd).error(), ErrorCode::FILE_INVALID_HANDLE);
}

TEST_F(Hf3fsNativeAdapterTest, BatchSplitsIovecsIntoOneShimCall) {
    Hf3fsNativeAdapter adapter(&kFakeApi);
    ASSERT_TRUE(adapter.Init("/root"));
    ASSERT_TRUE(adapter.PreallocateFile("/root/a.data", 4096));
    auto fd = adapter.OpenExistingFile("/root/a.data");
    ASSERT_TRUE(fd);

    std::string first(100, 'A'), second(50, 'B'), third(30, 'C');
    iovec w0[] = {{first.data(), first.size()}, {second.data(), second.size()}};
    iovec w1[] = {{third.data(), third.size()}};
    std::vector<FdIoRequest> writes = {{*fd, w0, 2, 10}, {*fd, w1, 1, 1000}};
    auto written = adapter.BatchWriteAt(writes);
    ASSERT_EQ(written.size(), 2u);
    ASSERT_TRUE(written[0]);
    EXPECT_EQ(*written[0], 150u);
    ASSERT_TRUE(written[1]);
    EXPECT_EQ(*written[1], 30u);
    EXPECT_EQ(fs_.batch_sizes, (std::vector<size_t>{3}));
    EXPECT_EQ(fs_.files["/root/a.data"].substr(10, 150), first + second);
    EXPECT_EQ(fs_.files["/root/a.data"].substr(1000, 30), third);

    std::string out(150, '\0');
    iovec r0[] = {{out.data(), 60}, {out.data() + 60, 90}};
    auto read = adapter.ReadAt(*fd, r0, 2, 10);
    ASSERT_TRUE(read);
    EXPECT_EQ(*read, 150u);
    EXPECT_EQ(out, first + second);
}

TEST_F(Hf3fsNativeAdapterTest, FailedTransferFailsOnlyItsRequest) {
    Hf3fsNativeAdapter adapter(&kFakeApi);
    ASSERT_TRUE(adapter.Init("/root"));
    ASSERT_TRUE(adapter.PreallocateFile("/root/a.data", 4096));
    auto fd = adapter.OpenExistingFile("/root/a.data");
    ASSERT_TRUE(fd);

    std::string data(64, 'x');
    iovec two[] = {{data.data(), 32}, {data.data() + 32, 32}};
    iovec one[] = {{data.data(), 64}};
    fs_.fail_offset = 32;  // second iovec of the first request
    std::vector<FdIoRequest> writes = {{*fd, two, 2, 0}, {*fd, one, 1, 128}};
    auto written = adapter.BatchWriteAt(writes);
    ASSERT_EQ(written.size(), 2u);
    ASSERT_FALSE(written[0]);
    EXPECT_EQ(written[0].error(), ErrorCode::FILE_WRITE_FAIL);
    ASSERT_TRUE(written[1]);
    EXPECT_EQ(*written[1], 64u);

    std::string out(64, '\0');
    iovec r[] = {{out.data(), 64}};
    auto read = adapter.ReadAt(*fd, r, 1, 32);
    ASSERT_FALSE(read);
    EXPECT_EQ(read.error(), ErrorCode::FILE_READ_FAIL);
}

TEST_F(Hf3fsNativeAdapterTest, InvalidRequestsAreRejectedBeforeIo) {
    Hf3fsNativeAdapter adapter(&kFakeApi);
    ASSERT_TRUE(adapter.Init("/root"));
    ASSERT_TRUE(adapter.PreallocateFile("/root/a.data", 4096));
    auto fd = adapter.OpenExistingFile("/root/a.data");
    ASSERT_TRUE(fd);

    std::string data(16, 'x');
    iovec good[] = {{data.data(), data.size()}};
    iovec null_base[] = {{data.data(), 8}, {nullptr, 8}};
    std::vector<FdIoRequest> writes = {{-1, good, 1, 0},
                                       {*fd, good, 1, -5},
                                       {*fd, null_base, 2, 0},
                                       {*fd, good, 1, 64}};
    auto written = adapter.BatchWriteAt(writes);
    ASSERT_EQ(written.size(), 4u);
    for (size_t i = 0; i < 3; ++i) {
        ASSERT_FALSE(written[i]) << i;
        EXPECT_EQ(written[i].error(), ErrorCode::INVALID_PARAMS) << i;
    }
    ASSERT_TRUE(written[3]);
    // Only the valid request reached the shim.
    EXPECT_EQ(fs_.batch_sizes, (std::vector<size_t>{1}));
    EXPECT_EQ(fs_.files["/root/a.data"].substr(0, 8), std::string(8, '\0'));
}

TEST_F(Hf3fsNativeAdapterTest, ZeroLengthRequestSucceedsWithoutIo) {
    Hf3fsNativeAdapter adapter(&kFakeApi);
    ASSERT_TRUE(adapter.Init("/root"));
    auto written = adapter.WriteAt(3, nullptr, 0, 0);
    ASSERT_TRUE(written);
    EXPECT_EQ(*written, 0u);
    EXPECT_TRUE(fs_.batch_sizes.empty());
}

}  // namespace
}  // namespace mooncake
