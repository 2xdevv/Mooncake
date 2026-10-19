#pragma once

#include <memory>
#include <string>

#include "hf3fs/hf3fs_native_api.h"
#include "storage/distributed/fs_adapter.h"

namespace mooncake {

/**
 * @brief 3FS adapter that bypasses FUSE and USRBIO.
 *
 * Delegates to libmooncake_hf3fs_native.so (src/hf3fs/native), which runs the
 * 3FS mgmtd/meta/storage clients in-process and moves data over RDMA. The
 * library is loaded at Init() with dlopen, so Mooncake itself has no build
 * dependency on 3FS. File descriptors returned by this adapter are shim
 * handles, not kernel fds.
 *
 * Configured through MOONCAKE_HF3FS_NATIVE_CONFIG (required: path to the
 * shim's TOML config) and MOONCAKE_HF3FS_NATIVE_LIB (optional: library path).
 */
class Hf3fsNativeAdapter : public FileSystemAdapter {
   public:
    struct Session;

    Hf3fsNativeAdapter();
    // For tests: use the given API table instead of loading the library.
    explicit Hf3fsNativeAdapter(const mc_hf3fs_api* api);
    ~Hf3fsNativeAdapter() override;

    tl::expected<void, ErrorCode> DeleteFile(const std::string& path) override;

    tl::expected<bool, ErrorCode> FileExists(const std::string& path) override;

    tl::expected<std::vector<std::string>, ErrorCode> ListFiles(
        const std::string& dir) override;

    tl::expected<size_t, ErrorCode> GetFileSize(
        const std::string& path) override;

    tl::expected<int, ErrorCode> OpenFile(const std::string& path) override;

    tl::expected<int, ErrorCode> OpenExistingFile(
        const std::string& path) override;

    tl::expected<void, ErrorCode> CloseFile(int fd) override;

    tl::expected<void, ErrorCode> PreallocateFile(const std::string& path,
                                                  uint64_t size) override;

    tl::expected<size_t, ErrorCode> WriteAt(int fd, const iovec* iov,
                                            int iovcnt,
                                            int64_t offset) override;

    tl::expected<size_t, ErrorCode> ReadAt(int fd, iovec* iov, int iovcnt,
                                           int64_t offset) override;

    bool SupportsBatchIo() const override { return true; }

    std::vector<tl::expected<size_t, ErrorCode>> BatchWriteAt(
        std::span<const FdIoRequest> requests) override;

    std::vector<tl::expected<size_t, ErrorCode>> BatchReadAt(
        std::span<const FdIoRequest> requests) override;

    tl::expected<void, ErrorCode> Init(const std::string& mount_path) override;

    tl::expected<void, ErrorCode> Shutdown() override;

    const char* GetName() const override { return "hf3fs_native"; }

   private:
    std::vector<tl::expected<size_t, ErrorCode>> BatchIo(
        std::span<const FdIoRequest> requests, bool write);

    const mc_hf3fs_api* injected_api_ = nullptr;
    std::shared_ptr<Session> session_;
};

}  // namespace mooncake
