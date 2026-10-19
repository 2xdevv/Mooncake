// Native 3FS client shim for Mooncake. See README.md in this directory.
//
// Talks to 3FS in-process through MgmtdClient/StorageClient/MetaClient, so
// data moves between this process and the storage targets over RDMA without
// a FUSE mount or USRBIO ring. Exposes only the C ABI in
// mooncake-store/include/hf3fs/hf3fs_native_api.h.

#include <folly/Singleton.h>
#include <folly/experimental/coro/BlockingWait.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <condition_variable>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "client/meta/MetaClient.h"
#include "client/mgmtd/MgmtdClientForClient.h"
#include "client/storage/StorageClient.h"
#include "common/app/ClientId.h"
#include "common/logging/LogInit.h"
#include "common/net/Client.h"
#include "common/net/ib/IBDevice.h"
#include "common/utils/ConfigBase.h"
#include "common/utils/FileUtils.h"
#include "common/utils/SysResource.h"
#include "fbs/mgmtd/ClientSessionData.h"
#include "fuse/PioV.h"
#include "hf3fs/hf3fs_native_api.h"
#include "stubs/common/RealStubFactory.h"
#include "stubs/mgmtd/MgmtdServiceStub.h"

namespace mooncake::hf3fs_native {
namespace {

using namespace hf3fs;
using hf3fs::meta::Inode;
using hf3fs::meta::InodeId;
using hf3fs::meta::client::MetaClient;
using hf3fs::storage::client::IOBuffer;
using hf3fs::storage::client::StorageClient;

// Accepts a FUSE launcher config (cluster_id, token_file, mountpoint,
// ib_devices, mgmtd_client) unchanged, plus optional client tuning sections.
struct Config : public ConfigBase<Config> {
    CONFIG_ITEM(cluster_id, "");
    CONFIG_ITEM(token_file, "");
    // Prefix stripped from Mooncake paths to get 3FS paths, e.g. /mnt/3fs.
    // The mount itself is not required on this host.
    CONFIG_ITEM(mountpoint, "");
    CONFIG_ITEM(allow_other, true);  // accepted for launcher-config compat
    CONFIG_ITEM(log, "WARN");
    // Staging buffers registered with the RDMA devices once at startup.
    // Each batch_io call holds one buffer; transfers larger than a buffer
    // are split.
    CONFIG_ITEM(staging_buffer_size, 64_MB);
    CONFIG_ITEM(staging_buffer_count, 8u);
    CONFIG_OBJ(ib_devices, net::IBDevice::Config);
    CONFIG_OBJ(client, net::Client::Config);
    CONFIG_OBJ(mgmtd_client, hf3fs::client::MgmtdClientForClient::Config);
    CONFIG_OBJ(storage, StorageClient::Config);
    CONFIG_OBJ(meta, MetaClient::Config,
               [](auto& cfg) { cfg.set_dynamic_stripe(true); });
};

thread_local std::string tls_last_error;

int Fail(int err, std::string message) {
    tls_last_error = std::move(message);
    return err;
}

int ToErrno(status_code_t code) {
    switch (code) {
        case MetaCode::kNotFound:
            return -ENOENT;
        case MetaCode::kExists:
            return -EEXIST;
        case MetaCode::kNotDirectory:
            return -ENOTDIR;
        case MetaCode::kIsDirectory:
            return -EISDIR;
        case MetaCode::kNoPermission:
            return -EACCES;
        case MetaCode::kNotEmpty:
            return -ENOTEMPTY;
        case StatusCode::kInvalidArg:
            return -EINVAL;
        default:
            return -EIO;
    }
}

int Fail(const Status& status, std::string_view what) {
    return Fail(ToErrno(status.code()),
                fmt::format("{}: {}", what, status.describe()));
}

class StagingPool {
   public:
    struct Buffer {
        uint8_t* data = nullptr;
        std::unique_ptr<IOBuffer> io_buffer;
    };

    ~StagingPool() {
        for (auto& buffer : buffers_) {
            buffer.io_buffer.reset();
            std::free(buffer.data);
        }
    }

    Result<Void> Init(StorageClient& storage, size_t size, size_t count) {
        size_ = size;
        buffers_.resize(count);
        for (auto& buffer : buffers_) {
            buffer.data = static_cast<uint8_t*>(std::aligned_alloc(4096, size));
            if (!buffer.data) {
                return makeError(StatusCode::kInvalidArg,
                                 "staging buffer allocation failed");
            }
            auto io_buffer = storage.registerIOBuffer(buffer.data, size);
            RETURN_ON_ERROR(io_buffer);
            buffer.io_buffer =
                std::make_unique<IOBuffer>(std::move(*io_buffer));
            free_.push_back(&buffer);
        }
        return Void{};
    }

    Buffer* Acquire() {
        std::unique_lock lock(mutex_);
        cv_.wait(lock, [&] { return !free_.empty(); });
        auto* buffer = free_.front();
        free_.pop_front();
        return buffer;
    }

    void Release(Buffer* buffer) {
        {
            std::lock_guard lock(mutex_);
            free_.push_back(buffer);
        }
        cv_.notify_one();
    }

    size_t size() const { return size_; }

   private:
    size_t size_ = 0;
    std::vector<Buffer> buffers_;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::deque<Buffer*> free_;
};

class Client {
   public:
    ~Client() { Stop(); }

    int Start(const char* config_path) {
        // The host process does not run folly::init for this library's
        // private copy of folly.
        static std::once_flag folly_singletons;
        std::call_once(folly_singletons, [] {
            folly::SingletonVault::singleton()->registrationComplete();
        });
        auto loaded = config_.atomicallyUpdate(Path(config_path),
                                               /*isHotUpdate=*/false);
        if (!loaded) return Fail(loaded.error(), "load config");
        if (config_.cluster_id().empty()) {
            return Fail(-EINVAL, "config: cluster_id is required");
        }
        logging::initOrDie(config_.log());

        if (const char* token = std::getenv("HF3FS_FUSE_TOKEN")) {
            token_ = token;
        } else if (!config_.token_file().empty()) {
            auto token = loadFile(config_.token_file());
            if (!token) return Fail(token.error(), "load token_file");
            token_ = folly::trimWhitespace(*token);
        }
        user_ = flat::UserInfo(flat::Uid(::getuid()), flat::Gid(::getgid()),
                               token_);
        mountpoint_ = Path(config_.mountpoint()).lexically_normal().string();
        while (mountpoint_.size() > 1 && mountpoint_.back() == '/') {
            mountpoint_.pop_back();
        }

        if (!net::IBManager::initialized()) {
            auto ib = net::IBManager::start(config_.ib_devices());
            if (!ib) return Fail(ib.error(), "start IB devices");
            started_ib_ = true;
        }

        net_client_ = std::make_unique<net::Client>(config_.client());
        auto started = net_client_->start("mc-3fs");
        if (!started) return Fail(started.error(), "start net client");
        auto ctx_creator = [this](net::Address addr) {
            return net_client_->serdeCtx(addr);
        };

        mgmtd_ = std::make_shared<hf3fs::client::MgmtdClientForClient>(
            config_.cluster_id(),
            std::make_unique<stubs::RealStubFactory<mgmtd::MgmtdServiceStub>>(
                ctx_creator),
            config_.mgmtd_client());
        auto physical_host = SysResource::hostname(true);
        if (!physical_host) return Fail(physical_host.error(), "hostname");
        auto client_id = ClientId::random(*physical_host);
        mgmtd_->setClientSessionPayload(
            {client_id.uuid.toHexString(), flat::NodeType::CLIENT,
             flat::ClientSessionData::create(
                 *physical_host, "mooncake-store native 3FS client",
                 std::vector<flat::ServiceGroupInfo>{},
                 flat::ReleaseVersion::fromVersionInfo()),
             flat::UserInfo{}});
        folly::coro::blockingWait(
            mgmtd_->start(&net_client_->tpg().bgThreadPool().randomPick()));
        folly::coro::blockingWait(mgmtd_->refreshRoutingInfo(false));
        auto session = folly::coro::blockingWait(mgmtd_->extendClientSession());
        if (!session) return Fail(session.error(), "establish client session");

        storage_ = StorageClient::create(client_id, config_.storage(), *mgmtd_);
        if (!storage_) return Fail(-EIO, "create storage client");
        meta_ = std::make_shared<MetaClient>(
            client_id, config_.meta(),
            std::make_unique<MetaClient::StubFactory>(ctx_creator), mgmtd_,
            storage_, /*dynStripe=*/true);
        meta_->start(net_client_->tpg().bgThreadPool());

        staging_ = std::make_unique<StagingPool>();
        auto staged =
            staging_->Init(*storage_, config_.staging_buffer_size(),
                           std::max(1u, config_.staging_buffer_count()));
        if (!staged) return Fail(staged.error(), "register staging buffers");
        return 0;
    }

    void Stop() {
        if (meta_) {
            meta_->stop();
            meta_.reset();
        }
        staging_.reset();
        if (storage_) {
            storage_->stop();
            storage_.reset();
        }
        if (mgmtd_) {
            folly::coro::blockingWait(mgmtd_->stop());
            mgmtd_.reset();
        }
        if (net_client_) {
            net_client_->stopAndJoin();
            net_client_.reset();
        }
        if (started_ib_) {
            net::IBManager::stop();
            started_ib_ = false;
        }
    }

    int Open(const char* path, int32_t* handle) {
        auto inode = StatPath(path);
        if (!inode) return Fail(inode.error(), fmt::format("open {}", path));
        if (!inode->isFile()) return Fail(-EISDIR, "open: not a file");
        auto extended = EnsureStripe(*inode, inode->asFile().length);
        if (!extended) return Fail(extended.error(), "extend stripe");
        std::unique_lock lock(handles_mutex_);
        int32_t id = next_handle_++;
        handles_.emplace(id, std::make_shared<const Inode>(std::move(*inode)));
        *handle = id;
        return 0;
    }

    int Close(int32_t handle) {
        std::unique_lock lock(handles_mutex_);
        if (handles_.erase(handle) == 0)
            return Fail(-EBADF, "close: bad handle");
        return 0;
    }

    int Preallocate(const char* path, uint64_t size) {
        auto inode = StatPath(path);
        if (!inode && inode.error().code() == MetaCode::kNotFound) {
            // Data never goes through a meta write session, so create the
            // file without write intent (O_RDWR would require one).
            inode = folly::coro::blockingWait(
                meta_->create(user_, InodeId::root(), ToFsPath(path),
                              std::nullopt, meta::Permission(0644), O_RDONLY));
        }
        if (!inode) return Fail(inode.error(), fmt::format("create {}", path));
        if (!inode->isFile()) return Fail(-EISDIR, "preallocate: not a file");
        if (inode->asFile().length < size) {
            inode = folly::coro::blockingWait(
                meta_->truncate(user_, inode->id, size));
            if (!inode) return Fail(inode.error(), "truncate");
        }
        auto extended = EnsureStripe(*inode, size);
        if (!extended) return Fail(extended.error(), "extend stripe");
        return 0;
    }

    int Remove(const char* path) {
        auto removed = folly::coro::blockingWait(
            meta_->remove(user_, InodeId::root(), ToFsPath(path), false));
        if (!removed)
            return Fail(removed.error(), fmt::format("remove {}", path));
        return 0;
    }

    int Stat(const char* path, uint64_t* size, int* is_dir) {
        auto inode = StatPath(path);
        if (!inode) return Fail(inode.error(), fmt::format("stat {}", path));
        if (is_dir) *is_dir = inode->isDirectory() ? 1 : 0;
        if (size) *size = inode->isFile() ? inode->asFile().length : 0;
        return 0;
    }

    int List(const char* path, mc_hf3fs_list_cb cb, void* ctx) {
        std::string prev;
        while (true) {
            auto listed = folly::coro::blockingWait(
                meta_->list(user_, InodeId::root(), ToFsPath(path), prev,
                            /*limit=*/0, /*needStatus=*/false));
            if (!listed)
                return Fail(listed.error(), fmt::format("list {}", path));
            for (const auto& entry : listed->entries) {
                cb(ctx, entry.name.c_str(), entry.isDirectory() ? 1 : 0);
            }
            if (!listed->more || listed->entries.empty()) return 0;
            prev = listed->entries.back().name;
        }
    }

    int BatchIo(bool write, mc_hf3fs_io* ios, size_t n);

   private:
    // Mooncake passes paths under the configured mountpoint; 3FS paths are
    // relative to the filesystem root.
    Path ToFsPath(const char* path) const {
        std::string_view p(path);
        if (!mountpoint_.empty() && mountpoint_ != "/" &&
            p.substr(0, mountpoint_.size()) == mountpoint_ &&
            (p.size() == mountpoint_.size() || p[mountpoint_.size()] == '/')) {
            p.remove_prefix(mountpoint_.size());
        }
        while (!p.empty() && p.front() == '/') p.remove_prefix(1);
        return p.empty() ? Path(".") : Path(std::string(p));
    }

    Result<Inode> StatPath(const char* path) {
        return folly::coro::blockingWait(
            meta_->stat(user_, InodeId::root(), ToFsPath(path),
                        /*followLastSymlink=*/true));
    }

    // Writes go straight to chunks, so the stripe that meta tracks for the
    // file (and garbage-collects on removal) must already cover them.
    Result<Void> EnsureStripe(const Inode& inode, uint64_t length) {
        const auto& file = inode.asFile();
        if (!file.dynStripe || !file.layout.chunkSize) return Void{};
        uint32_t stripe = std::min<uint64_t>(
            folly::divCeil(length, file.layout.chunkSize.u64()),
            file.layout.stripeSize);
        if (file.dynStripe >= stripe) return Void{};
        auto extended = folly::coro::blockingWait(
            meta_->extendStripe(user_, inode.id, stripe));
        RETURN_ON_ERROR(extended);
        return Void{};
    }

    std::shared_ptr<const Inode> Lookup(int32_t handle) {
        std::shared_lock lock(handles_mutex_);
        auto it = handles_.find(handle);
        return it == handles_.end() ? nullptr : it->second;
    }

    Config config_;
    std::string token_;
    std::string mountpoint_;
    flat::UserInfo user_;
    bool started_ib_ = false;
    std::unique_ptr<net::Client> net_client_;
    std::shared_ptr<hf3fs::client::MgmtdClientForClient> mgmtd_;
    std::shared_ptr<StorageClient> storage_;
    std::shared_ptr<MetaClient> meta_;
    std::unique_ptr<StagingPool> staging_;

    std::shared_mutex handles_mutex_;
    int32_t next_handle_ = 1;
    std::unordered_map<int32_t, std::shared_ptr<const Inode>> handles_;
};

// A contiguous slice of one transfer that fits in a staging buffer.
struct Piece {
    size_t io;
    uint64_t file_offset;
    uint8_t* user;
    uint64_t len;
};

int Client::BatchIo(bool write, mc_hf3fs_io* ios, size_t n) {
    std::vector<std::shared_ptr<const Inode>> inodes(n);
    std::vector<Piece> pieces;
    const size_t slab = staging_->size();
    for (size_t i = 0; i < n; ++i) {
        auto& io = ios[i];
        io.result = 0;
        inodes[i] = Lookup(io.handle);
        if (!inodes[i]) {
            io.result = -EBADF;
            continue;
        }
        if (io.len > 0 && !io.buf) {
            io.result = -EINVAL;
            continue;
        }
        for (uint64_t done = 0; done < io.len; done += slab) {
            pieces.push_back({i, io.offset + done,
                              static_cast<uint8_t*>(io.buf) + done,
                              std::min<uint64_t>(slab, io.len - done)});
        }
    }

    auto* buffer = staging_->Acquire();
    auto fail_piece = [&](const Piece& piece, int64_t err) {
        if (ios[piece.io].result >= 0) ios[piece.io].result = err;
    };
    for (size_t begin = 0; begin < pieces.size();) {
        // Fill one staging buffer, then issue every piece in it at once;
        // StorageClient spreads them across chains and storage servers.
        size_t end = begin;
        for (size_t used = 0;
             end < pieces.size() && used + pieces[end].len <= slab; ++end) {
            used += pieces[end].len;
        }
        std::vector<ssize_t> res(end - begin, 0);
        lib::agent::PioV pio(*storage_, /*chunkSizeLim=*/0, res);
        uint8_t* staged = buffer->data;
        for (size_t k = begin; k < end; ++k) {
            const auto& piece = pieces[k];
            if (ios[piece.io].result < 0) continue;
            const auto& inode = *inodes[piece.io];
            Result<Void> added = Void{};
            if (write) {
                std::memcpy(staged, piece.user, piece.len);
                added = pio.addWrite(k - begin, inode, 0, piece.file_offset,
                                     piece.len, staged, *buffer->io_buffer);
            } else {
                added = pio.addRead(k - begin, inode, 0, piece.file_offset,
                                    piece.len, staged, *buffer->io_buffer);
            }
            if (!added) {
                Fail(added.error(), "prepare I/O");
                fail_piece(piece, ToErrno(added.error().code()));
            }
            staged += piece.len;
        }

        auto executed = write
                            ? folly::coro::blockingWait(pio.executeWrite(user_))
                            : folly::coro::blockingWait(pio.executeRead(user_));
        if (!executed) {
            Fail(executed.error(), write ? "batch write" : "batch read");
            for (size_t k = begin; k < end; ++k) {
                fail_piece(pieces[k], ToErrno(executed.error().code()));
            }
        } else {
            pio.finishIo(/*allowHoles=*/false);
        }

        staged = buffer->data;
        for (size_t k = begin; k < end; ++k) {
            const auto& piece = pieces[k];
            const ssize_t got = executed ? res[k - begin] : -1;
            if (executed && got < 0) {
                tls_last_error = fmt::format("{} failed with 3FS status {}",
                                             write ? "write" : "read", -got);
                fail_piece(piece, ToErrno(static_cast<status_code_t>(-got)));
            } else if (executed && static_cast<uint64_t>(got) != piece.len) {
                tls_last_error =
                    fmt::format("short {}: {} of {} bytes at offset {}",
                                write ? "write" : "read", got, piece.len,
                                piece.file_offset);
                fail_piece(piece, -EIO);
            } else if (executed && !write && ios[piece.io].result >= 0) {
                std::memcpy(piece.user, staged, piece.len);
            }
            staged += piece.len;
        }
        begin = end;
    }
    staging_->Release(buffer);

    for (size_t i = 0; i < n; ++i) {
        if (ios[i].result >= 0)
            ios[i].result = static_cast<int64_t>(ios[i].len);
    }
    return 0;
}

Client* AsClient(mc_hf3fs_client* client) {
    return reinterpret_cast<Client*>(client);
}

int ApiCreate(const char* config_path, mc_hf3fs_client** out) {
    if (!config_path || !out) return Fail(-EINVAL, "create: null argument");
    auto client = std::make_unique<Client>();
    int rc = client->Start(config_path);
    if (rc != 0) return rc;
    *out = reinterpret_cast<mc_hf3fs_client*>(client.release());
    return 0;
}

void ApiDestroy(mc_hf3fs_client* client) { delete AsClient(client); }

const char* ApiLastError() { return tls_last_error.c_str(); }

int ApiOpen(mc_hf3fs_client* client, const char* path, int32_t* handle) {
    return AsClient(client)->Open(path, handle);
}

int ApiClose(mc_hf3fs_client* client, int32_t handle) {
    return AsClient(client)->Close(handle);
}

int ApiPreallocate(mc_hf3fs_client* client, const char* path, uint64_t size) {
    return AsClient(client)->Preallocate(path, size);
}

int ApiRemove(mc_hf3fs_client* client, const char* path) {
    return AsClient(client)->Remove(path);
}

int ApiStat(mc_hf3fs_client* client, const char* path, uint64_t* size,
            int* is_dir) {
    return AsClient(client)->Stat(path, size, is_dir);
}

int ApiList(mc_hf3fs_client* client, const char* path, mc_hf3fs_list_cb cb,
            void* ctx) {
    return AsClient(client)->List(path, cb, ctx);
}

int ApiBatchIo(mc_hf3fs_client* client, int is_write, mc_hf3fs_io* ios,
               size_t n) {
    if (n > 0 && !ios) return Fail(-EINVAL, "batch_io: null ios");
    return AsClient(client)->BatchIo(is_write != 0, ios, n);
}

// Exceptions must not cross the C ABI.
template <auto Fn, typename... Args>
int Guarded(Args... args) noexcept {
    try {
        return Fn(args...);
    } catch (const std::exception& e) {
        return Fail(-EIO, fmt::format("exception: {}", e.what()));
    } catch (...) {
        return Fail(-EIO, "unknown exception");
    }
}

const mc_hf3fs_api kApi = {
    MC_HF3FS_NATIVE_ABI_VERSION,
    &Guarded<ApiCreate, const char*, mc_hf3fs_client**>,
    &ApiDestroy,
    &ApiLastError,
    &Guarded<ApiOpen, mc_hf3fs_client*, const char*, int32_t*>,
    &Guarded<ApiClose, mc_hf3fs_client*, int32_t>,
    &Guarded<ApiPreallocate, mc_hf3fs_client*, const char*, uint64_t>,
    &Guarded<ApiRemove, mc_hf3fs_client*, const char*>,
    &Guarded<ApiStat, mc_hf3fs_client*, const char*, uint64_t*, int*>,
    &Guarded<ApiList, mc_hf3fs_client*, const char*, mc_hf3fs_list_cb, void*>,
    &Guarded<ApiBatchIo, mc_hf3fs_client*, int, mc_hf3fs_io*, size_t>,
};

}  // namespace
}  // namespace mooncake::hf3fs_native

extern "C" __attribute__((visibility("default"))) const mc_hf3fs_api*
mc_hf3fs_get_api(void) {
    return &mooncake::hf3fs_native::kApi;
}
