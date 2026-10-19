#include "storage/distributed/hf3fs_native_adapter.h"

#include <dlfcn.h>

#include <cerrno>
#include <map>
#include <mutex>
#include <utility>

#include <glog/logging.h>

#include "environ.h"
#include "environment_variables.h"

namespace mooncake {

namespace {

constexpr const char* kDefaultLibrary = "libmooncake_hf3fs_native.so";

// The shim is never unloaded: it owns folly singletons and 3FS threads whose
// teardown is not safe at dlclose time.
const mc_hf3fs_api* LoadApi(const std::string& library) {
    static std::mutex mutex;
    static std::map<std::string, const mc_hf3fs_api*> loaded;
    std::lock_guard lock(mutex);
    if (auto it = loaded.find(library); it != loaded.end()) return it->second;

    void* handle = ::dlopen(library.c_str(), RTLD_NOW | RTLD_LOCAL);
    if (!handle) {
        LOG(ERROR) << "Failed to load native 3FS client " << library << ": "
                   << ::dlerror();
        return nullptr;
    }
    auto get_api = reinterpret_cast<mc_hf3fs_get_api_fn>(
        ::dlsym(handle, MC_HF3FS_NATIVE_GET_API_SYMBOL));
    const mc_hf3fs_api* api = get_api ? get_api() : nullptr;
    if (!api || api->abi_version != MC_HF3FS_NATIVE_ABI_VERSION) {
        LOG(ERROR) << "Native 3FS client " << library
                   << " has an incompatible ABI (expected version "
                   << MC_HF3FS_NATIVE_ABI_VERSION << ")";
        return nullptr;
    }
    loaded.emplace(library, api);
    return api;
}

ErrorCode ToErrorCode(int err, ErrorCode fallback) {
    switch (err) {
        case -ENOENT:
            return ErrorCode::FILE_NOT_FOUND;
        case -EBADF:
            return ErrorCode::FILE_INVALID_HANDLE;
        case -EINVAL:
            return ErrorCode::INVALID_PARAMS;
        default:
            return fallback;
    }
}

}  // namespace

// One native client per (library, config) in the process: the 3FS RDMA
// device manager is a process-wide singleton, and the allocator and backend
// each own an adapter.
struct Hf3fsNativeAdapter::Session {
    const mc_hf3fs_api* api = nullptr;
    mc_hf3fs_client* client = nullptr;

    ~Session() {
        if (client) api->destroy(client);
    }

    const char* LastError() const { return api->last_error(); }

    static std::shared_ptr<Session> Acquire(const mc_hf3fs_api* api,
                                            const std::string& config) {
        static std::mutex mutex;
        static std::map<std::pair<const mc_hf3fs_api*, std::string>,
                        std::weak_ptr<Session>>
            sessions;
        std::lock_guard lock(mutex);
        auto& slot = sessions[{api, config}];
        if (auto session = slot.lock()) return session;

        auto session = std::make_shared<Session>();
        session->api = api;
        int rc = api->create(config.c_str(), &session->client);
        if (rc != 0) {
            LOG(ERROR) << "Failed to start native 3FS client with config "
                       << config << ": " << api->last_error();
            session->client = nullptr;
            return nullptr;
        }
        slot = session;
        return session;
    }
};

Hf3fsNativeAdapter::Hf3fsNativeAdapter() = default;

Hf3fsNativeAdapter::Hf3fsNativeAdapter(const mc_hf3fs_api* api)
    : injected_api_(api) {}

Hf3fsNativeAdapter::~Hf3fsNativeAdapter() = default;

tl::expected<void, ErrorCode> Hf3fsNativeAdapter::Init(
    const std::string& /*mount_path*/) {
    using Variables = DistributedStorageEnvironmentVariables;
    const auto config =
        Environ::ReadOr(Variables::MOONCAKE_HF3FS_NATIVE_CONFIG, std::string());
    if (config.empty()) {
        LOG(ERROR) << "hf3fs_native adapter requires "
                   << Variables::MOONCAKE_HF3FS_NATIVE_CONFIG.name;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    const mc_hf3fs_api* api = injected_api_;
    if (!api) {
        api = LoadApi(Environ::ReadOr(Variables::MOONCAKE_HF3FS_NATIVE_LIB,
                                      std::string(kDefaultLibrary)));
    }
    if (!api) return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
    session_ = Session::Acquire(api, config);
    if (!session_) return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    return {};
}

tl::expected<void, ErrorCode> Hf3fsNativeAdapter::Shutdown() {
    session_.reset();
    return {};
}

tl::expected<void, ErrorCode> Hf3fsNativeAdapter::DeleteFile(
    const std::string& path) {
    if (!session_) return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    int rc = session_->api->remove(session_->client, path.c_str());
    if (rc == 0) return {};
    if (rc != -ENOENT) {
        LOG(WARNING) << "native 3FS remove failed: " << session_->LastError();
    }
    return tl::make_unexpected(ToErrorCode(rc, ErrorCode::FILE_WRITE_FAIL));
}

tl::expected<bool, ErrorCode> Hf3fsNativeAdapter::FileExists(
    const std::string& path) {
    if (!session_) return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    int rc =
        session_->api->stat(session_->client, path.c_str(), nullptr, nullptr);
    if (rc == 0) return true;
    if (rc == -ENOENT) return false;
    LOG(WARNING) << "native 3FS stat failed: " << session_->LastError();
    return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
}

tl::expected<size_t, ErrorCode> Hf3fsNativeAdapter::GetFileSize(
    const std::string& path) {
    if (!session_) return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    uint64_t size = 0;
    int rc =
        session_->api->stat(session_->client, path.c_str(), &size, nullptr);
    if (rc == 0) return static_cast<size_t>(size);
    if (rc != -ENOENT) {
        LOG(WARNING) << "native 3FS stat failed: " << session_->LastError();
    }
    return tl::make_unexpected(ToErrorCode(rc, ErrorCode::FILE_READ_FAIL));
}

tl::expected<std::vector<std::string>, ErrorCode> Hf3fsNativeAdapter::ListFiles(
    const std::string& dir) {
    if (!session_) return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    std::vector<std::string> files;
    auto collect = [](void* ctx, const char* name, int is_dir) {
        if (!is_dir)
            static_cast<std::vector<std::string>*>(ctx)->push_back(name);
    };
    int rc =
        session_->api->list(session_->client, dir.c_str(), collect, &files);
    if (rc == 0) return files;
    if (rc != -ENOENT) {
        LOG(WARNING) << "native 3FS list failed: " << session_->LastError();
    }
    return tl::make_unexpected(ToErrorCode(rc, ErrorCode::FILE_READ_FAIL));
}

tl::expected<int, ErrorCode> Hf3fsNativeAdapter::OpenFile(
    const std::string& path) {
    if (!session_) return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    auto exists = FileExists(path);
    if (!exists) return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    if (!*exists) {
        int rc = session_->api->preallocate(session_->client, path.c_str(), 0);
        if (rc != 0) {
            LOG(WARNING) << "native 3FS create failed: "
                         << session_->LastError();
            return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
        }
    }
    return OpenExistingFile(path);
}

tl::expected<int, ErrorCode> Hf3fsNativeAdapter::OpenExistingFile(
    const std::string& path) {
    if (!session_) return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    int32_t handle = -1;
    int rc = session_->api->open(session_->client, path.c_str(), &handle);
    if (rc == 0) return static_cast<int>(handle);
    if (rc != -ENOENT) {
        LOG(WARNING) << "native 3FS open failed: " << session_->LastError();
    }
    return tl::make_unexpected(ToErrorCode(rc, ErrorCode::FILE_OPEN_FAIL));
}

tl::expected<void, ErrorCode> Hf3fsNativeAdapter::CloseFile(int fd) {
    if (!session_) return tl::make_unexpected(ErrorCode::FILE_INVALID_HANDLE);
    if (session_->api->close(session_->client, fd) != 0) {
        return tl::make_unexpected(ErrorCode::FILE_INVALID_HANDLE);
    }
    return {};
}

tl::expected<void, ErrorCode> Hf3fsNativeAdapter::PreallocateFile(
    const std::string& path, uint64_t size) {
    if (!session_) return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    int rc = session_->api->preallocate(session_->client, path.c_str(), size);
    if (rc == 0) return {};
    LOG(ERROR) << "Failed to preallocate DFS file " << path << ", size=" << size
               << ", error=" << session_->LastError();
    return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
}

tl::expected<size_t, ErrorCode> Hf3fsNativeAdapter::WriteAt(int fd,
                                                            const iovec* iov,
                                                            int iovcnt,
                                                            int64_t offset) {
    FdIoRequest request{fd, const_cast<iovec*>(iov), iovcnt, offset};
    return BatchIo({&request, 1}, /*write=*/true).front();
}

tl::expected<size_t, ErrorCode> Hf3fsNativeAdapter::ReadAt(int fd, iovec* iov,
                                                           int iovcnt,
                                                           int64_t offset) {
    FdIoRequest request{fd, iov, iovcnt, offset};
    return BatchIo({&request, 1}, /*write=*/false).front();
}

std::vector<tl::expected<size_t, ErrorCode>> Hf3fsNativeAdapter::BatchWriteAt(
    std::span<const FdIoRequest> requests) {
    return BatchIo(requests, /*write=*/true);
}

std::vector<tl::expected<size_t, ErrorCode>> Hf3fsNativeAdapter::BatchReadAt(
    std::span<const FdIoRequest> requests) {
    return BatchIo(requests, /*write=*/false);
}

std::vector<tl::expected<size_t, ErrorCode>> Hf3fsNativeAdapter::BatchIo(
    std::span<const FdIoRequest> requests, bool write) {
    const ErrorCode io_error =
        write ? ErrorCode::FILE_WRITE_FAIL : ErrorCode::FILE_READ_FAIL;
    std::vector<tl::expected<size_t, ErrorCode>> results(requests.size(),
                                                         size_t{0});
    if (!session_) {
        results.assign(requests.size(),
                       tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL));
        return results;
    }

    // Each iovec of a request becomes one shim transfer at the next offset.
    std::vector<mc_hf3fs_io> ios;
    std::vector<size_t> owner;
    for (size_t i = 0; i < requests.size(); ++i) {
        const auto& request = requests[i];
        if (request.fd < 0 || request.offset < 0 || request.iovcnt < 0 ||
            (request.iovcnt > 0 && request.iov == nullptr)) {
            results[i] = tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            continue;
        }
        uint64_t offset = static_cast<uint64_t>(request.offset);
        for (int k = 0; k < request.iovcnt; ++k) {
            const auto& iov = request.iov[k];
            if (iov.iov_len == 0) continue;
            if (!iov.iov_base) {
                results[i] = tl::make_unexpected(ErrorCode::INVALID_PARAMS);
                break;
            }
            ios.push_back({request.fd, offset, iov.iov_base, iov.iov_len, 0});
            owner.push_back(i);
            offset += iov.iov_len;
        }
    }
    // Drop transfers of requests that failed validation part-way through.
    size_t kept = 0;
    for (size_t j = 0; j < ios.size(); ++j) {
        if (!results[owner[j]]) continue;
        ios[kept] = ios[j];
        owner[kept] = owner[j];
        ++kept;
    }
    ios.resize(kept);
    owner.resize(kept);
    if (ios.empty()) return results;

    int rc = session_->api->batch_io(session_->client, write ? 1 : 0,
                                     ios.data(), ios.size());
    if (rc != 0) {
        LOG(WARNING) << "native 3FS batch " << (write ? "write" : "read")
                     << " failed: " << session_->LastError();
        for (size_t j = 0; j < ios.size(); ++j) {
            results[owner[j]] = tl::make_unexpected(io_error);
        }
        return results;
    }
    for (size_t j = 0; j < ios.size(); ++j) {
        auto& result = results[owner[j]];
        if (!result) continue;
        if (ios[j].result != static_cast<int64_t>(ios[j].len)) {
            LOG(WARNING) << "native 3FS " << (write ? "write" : "read")
                         << " failed at offset " << ios[j].offset
                         << ", len=" << ios[j].len
                         << ", result=" << ios[j].result;
            result = tl::make_unexpected(io_error);
        } else {
            *result += ios[j].len;
        }
    }
    return results;
}

}  // namespace mooncake
