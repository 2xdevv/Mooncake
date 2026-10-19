#pragma once

// C ABI between Mooncake's Hf3fsNativeAdapter and the separately built
// libmooncake_hf3fs_native.so shim (mooncake-store/src/hf3fs/native).
//
// The shim links the 3FS client stack (MgmtdClient, StorageClient,
// MetaClient, folly, ...) with the compiler and flags of the 3FS build and
// hides every symbol except mc_hf3fs_get_api(). Mooncake only sees this
// header, so it neither compiles 3FS headers nor links 3FS libraries, and the
// two dependency trees cannot collide in one process.
//
// Bump MC_HF3FS_NATIVE_ABI_VERSION on any incompatible change to this file.

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define MC_HF3FS_NATIVE_ABI_VERSION 1
#define MC_HF3FS_NATIVE_GET_API_SYMBOL "mc_hf3fs_get_api"

typedef struct mc_hf3fs_client mc_hf3fs_client;

// One positional transfer. Mooncake paths are mapped into the 3FS namespace
// by stripping the client's configured mountpoint prefix.
typedef struct mc_hf3fs_io {
    int32_t handle;   // from open()
    uint64_t offset;  // byte offset in the file
    void* buf;        // caller memory, need not be registered
    uint64_t len;
    // out: len on success, otherwise a negative errno. A transfer is either
    // complete or failed; partial transfers are reported as -EIO.
    int64_t result;
} mc_hf3fs_io;

typedef void (*mc_hf3fs_list_cb)(void* ctx, const char* name, int is_dir);

// All int-returning functions return 0 on success or a negative errno.
// last_error() returns a description of the calling thread's most recent
// failure; the pointer is valid until that thread's next call.
typedef struct mc_hf3fs_api {
    uint32_t abi_version;

    // config_path: TOML accepted by the shim (a FUSE launcher config such as
    // hf3fs_fuse_main_launcher.toml works unchanged). See the shim README.
    int (*create)(const char* config_path, mc_hf3fs_client** out);
    void (*destroy)(mc_hf3fs_client* client);
    const char* (*last_error)(void);

    int (*open)(mc_hf3fs_client* client, const char* path, int32_t* handle);
    int (*close)(mc_hf3fs_client* client, int32_t handle);

    // Create the file if needed and make its length at least `size`.
    int (*preallocate)(mc_hf3fs_client* client, const char* path,
                       uint64_t size);
    // Returns -ENOENT if the path does not exist.
    int (*remove)(mc_hf3fs_client* client, const char* path);
    // Returns -ENOENT if the path does not exist.
    int (*stat)(mc_hf3fs_client* client, const char* path, uint64_t* size,
                int* is_dir);
    // Invokes cb once per entry (excluding "." and "..").
    int (*list)(mc_hf3fs_client* client, const char* path, mc_hf3fs_list_cb cb,
                void* ctx);

    // Executes all transfers, keeping many of them in flight. Sets every
    // ios[i].result. Returns 0 if the batch was attempted (individual
    // transfers may still have failed), or a negative errno if none was.
    // Safe to call concurrently, including on the same handle.
    int (*batch_io)(mc_hf3fs_client* client, int is_write, mc_hf3fs_io* ios,
                    size_t n);
} mc_hf3fs_api;

typedef const mc_hf3fs_api* (*mc_hf3fs_get_api_fn)(void);

const mc_hf3fs_api* mc_hf3fs_get_api(void);

#ifdef __cplusplus
}
#endif
