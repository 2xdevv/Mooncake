# Native 3FS Client for Mooncake DFS (`hf3fs_native`)

> **Experimental.** Like the rest of descriptor-based DFS, this adapter is for
> development and evaluation only.

`MOONCAKE_DFS_FS_ADAPTER=hf3fs_native` selects `Hf3fsNativeAdapter`. Instead of
going through the FUSE daemon (the `hf3fs` adapter submits USRBIO I/O to the
local FUSE client), it runs the 3FS management, metadata and storage clients
inside the Mooncake process and moves data to and from the storage targets over
RDMA directly. Batched reads and writes are issued together through 3FS's
`StorageClient`, which spreads them across chains and storage servers.

The 3FS client stack is built into a separate library,
`libmooncake_hf3fs_native.so`, that Mooncake loads with `dlopen`. Mooncake
itself is built without 3FS headers or libraries (`USE_3FS` is not required);
the library exports a single C symbol and keeps its statically linked 3FS,
folly and fmt symbols private.

## Building the library

The library must be built with the same toolchain and flags as 3FS
(clang-14 and the dependencies in 3FS's `dockerfile/`), and against the same
glog/gflags shared libraries as the Mooncake build that loads it. This
directory is a standalone CMake project that builds 3FS's client targets as a
subproject:

```bash
cmake -S mooncake-store/src/hf3fs/native -B build-hf3fs-native -G Ninja \
  -DHF3FS_SOURCE_DIR=/path/to/3FS \
  -DCMAKE_C_COMPILER=clang-14 -DCMAKE_CXX_COMPILER=clang++-14 \
  -DCMAKE_BUILD_TYPE=Release -DSHUFFLE_METHOD=g++11
cmake --build build-hf3fs-native --target mooncake_hf3fs_native
```

Add `-DGLOG_INCLUDE_DIR=... -DGLOG_LIBRARY=...` if 3FS would otherwise pick a
different glog than Mooncake (for example, glog 0.5 `libglog.so.0` versus
Mooncake's glog 0.6 `libglog.so.1`): two glog versions in one process abort
at startup with duplicate gflags definitions.

Install the library on the loader path, or point
`MOONCAKE_HF3FS_NATIVE_LIB` at it. Its remaining shared-library dependencies
(boost, double-conversion, libevent, ...) are those of the 3FS client.

## Configuration

| Variable | Meaning |
| --- | --- |
| `MOONCAKE_DFS_FS_ADAPTER=hf3fs_native` | Select this adapter (master and clients). |
| `MOONCAKE_HF3FS_NATIVE_CONFIG` | Required. Path to the client TOML below. |
| `MOONCAKE_HF3FS_NATIVE_LIB` | Optional. Library path; default `libmooncake_hf3fs_native.so`. |

The TOML accepts the 3FS FUSE launcher config (`hf3fs_fuse_main_launcher.toml`)
unchanged:

```toml
cluster_id = 'stage'
token_file = '/opt/3fs/etc/token.txt'   # or set HF3FS_FUSE_TOKEN
mountpoint = '/mnt/3fs'                  # prefix stripped from Mooncake paths

[ib_devices]
device_filter = ['mlx5_0']

[mgmtd_client]
mgmtd_server_addresses = ['RDMA://10.0.0.1:8000']
```

Optional settings:

| Key | Default | Meaning |
| --- | --- | --- |
| `staging_buffer_size` | `64MB` | RDMA-registered buffer that one batch call fills per round trip. |
| `staging_buffer_count` | `8` | Buffers registered at startup; this many batch calls run concurrently. |
| `log` | `WARN` | 3FS client log level. |
| `[client]`, `[storage]`, `[meta]` | 3FS defaults | 3FS `net::Client`, `StorageClient` and `MetaClient` settings, as in `hf3fs_fuse_main.toml`. |

`MOONCAKE_DFS_ROOT_DIR` keeps its usual meaning: shard and bucket files live
under it, and `mountpoint` is stripped from it to get the path inside 3FS.

## Behavior and limitations

- Data is staged through the registered buffers (one memcpy each way), as with
  USRBIO. Transfers larger than `staging_buffer_size` are split.
- Files are created and preallocated through the metadata service. Writes go
  directly to chunks and do not update file length, so they are valid only
  within a file's preallocated length, which is how DFS shard and bucket files
  are used. Before writing, the adapter extends a file's dynamic stripe to cover
  its length so that meta garbage-collects every chunk when the file is removed.
- Files written natively are ordinary 3FS files: FUSE and USRBIO readers see
  the same data.
- The DFS backend and allocators still create the root directory and validate
  shard paths (`std::filesystem::canonical`) through the local filesystem, so
  the 3FS mount must currently be present on every node. Moving those checks
  behind the adapter interface would remove that requirement.

## Tests

- `hf3fs_native_adapter_test`: adapter logic against an in-memory fake; no 3FS
  needed.
- `dfs_hf3fs_native_test`: live tests; set `MOONCAKE_HF3FS_NATIVE_CONFIG`,
  `MOONCAKE_DFS_ROOT_DIR` (a writable directory under the 3FS mount) and, if
  needed, `MOONCAKE_HF3FS_NATIVE_LIB`. Skipped otherwise.
