# Standalone build of libmooncake_hf3fs_native.so, the native 3FS client shim
# loaded at runtime by Hf3fsNativeAdapter. It is not part of the main Mooncake
# build: it must be compiled with the same toolchain and flags as 3FS, so it
# pulls in the 3FS source tree as a subproject. See README.md.
#
# cmake -S mooncake-store/src/hf3fs/native -B build-hf3fs-native -G Ninja \
# -DHF3FS_SOURCE_DIR=/path/to/3FS \ -DCMAKE_C_COMPILER=clang-14
# -DCMAKE_CXX_COMPILER=clang++-14 \ -DCMAKE_BUILD_TYPE=Release
# -DSHUFFLE_METHOD=g++11 cmake --build build-hf3fs-native --target
# mooncake_hf3fs_native
cmake_minimum_required(VERSION 3.16)
project(mooncake_hf3fs_native LANGUAGES C CXX)

set(HF3FS_SOURCE_DIR
    ""
    CACHE PATH "Path to the 3FS source tree")
if(NOT EXISTS "${HF3FS_SOURCE_DIR}/src/client/storage/StorageClient.h")
  message(FATAL_ERROR "Set HF3FS_SOURCE_DIR to a 3FS source tree")
endif()

# Link a private, static gflags so the flags that folly registers stay in this
# library. With the shared libgflags, they would collide with any other folly
# copy in the host process (e.g. libhf3fs_api_shared.so in USE_3FS builds) and
# abort at load time. Distribution libgflags.a is usually not PIC, so build one.
# Its version must match the gflags headers 3FS compiles against.
set(HF3FS_NATIVE_GFLAGS_VERSION
    "2.2.2"
    CACHE STRING "gflags version to embed")
set(HF3FS_NATIVE_GFLAGS_SHA256
    "34af2f15cf7367513b352bdcd2493ab14ce43692d2dcd9dfc499492966c64dcf"
    CACHE STRING "SHA-256 of the gflags release tarball")
include(ExternalProject)
set(_gflags_prefix ${CMAKE_CURRENT_BINARY_DIR}/gflags-pic)
ExternalProject_Add(
  gflags_pic
  URL https://github.com/gflags/gflags/archive/refs/tags/v${HF3FS_NATIVE_GFLAGS_VERSION}.tar.gz
  URL_HASH SHA256=${HF3FS_NATIVE_GFLAGS_SHA256}
  PREFIX ${_gflags_prefix}
  CMAKE_ARGS -DCMAKE_BUILD_TYPE=Release
             -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
             -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
             -DCMAKE_POSITION_INDEPENDENT_CODE=ON
             -DBUILD_SHARED_LIBS=OFF
             -DBUILD_STATIC_LIBS=ON
             -DBUILD_TESTING=OFF
             -DREGISTER_INSTALL_PREFIX=OFF
             -DCMAKE_INSTALL_PREFIX=${_gflags_prefix}/install
             -DCMAKE_INSTALL_LIBDIR=lib
  BUILD_BYPRODUCTS ${_gflags_prefix}/install/lib/libgflags.a)
# Import gflags before 3FS does so that 3FS links these targets, then point the
# shared target at the private static library.
find_package(gflags CONFIG REQUIRED)
set(_gflags_retargeted FALSE)
foreach(_ns "" "gflags::")
  if(TARGET ${_ns}gflags_shared)
    set_target_properties(
      ${_ns}gflags_shared
      PROPERTIES IMPORTED_LOCATION_RELEASE
                 ${_gflags_prefix}/install/lib/libgflags.a
                 IMPORTED_SONAME_RELEASE ""
                 IMPORTED_NO_SONAME TRUE)
    set(_gflags_retargeted TRUE)
  endif()
endforeach()
if(NOT _gflags_retargeted)
  message(FATAL_ERROR "gflags_shared target not found")
endif()

add_subdirectory(${HF3FS_SOURCE_DIR} 3fs EXCLUDE_FROM_ALL)

# 3FS configures its compiler flags as directory-scoped variables; adopt them so
# this translation unit sees the same folly/coroutine configuration.
string(TOUPPER "${CMAKE_BUILD_TYPE}" _build_type)
foreach(var CMAKE_CXX_STANDARD CMAKE_CXX_FLAGS CMAKE_CXX_FLAGS_${_build_type}
            CMAKE_POSITION_INDEPENDENT_CODE)
  get_directory_property(_value DIRECTORY ${HF3FS_SOURCE_DIR} DEFINITION ${var})
  set(${var} "${_value}")
endforeach()
get_directory_property(_defs DIRECTORY ${HF3FS_SOURCE_DIR} COMPILE_DEFINITIONS)
get_directory_property(_opts DIRECTORY ${HF3FS_SOURCE_DIR} COMPILE_OPTIONS)
get_directory_property(_link_opts DIRECTORY ${HF3FS_SOURCE_DIR} LINK_OPTIONS)

add_library(mooncake_hf3fs_native SHARED hf3fs_native_client.cc
                                         ${HF3FS_SOURCE_DIR}/src/fuse/PioV.cc)
target_compile_definitions(mooncake_hf3fs_native PRIVATE ${_defs})
target_compile_options(mooncake_hf3fs_native PRIVATE ${_opts})
target_include_directories(mooncake_hf3fs_native
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../../../include)
target_link_libraries(mooncake_hf3fs_native PRIVATE meta-client storage-client
                                                    mgmtd-client)
add_dependencies(mooncake_hf3fs_native gflags_pic)
# Export only mc_hf3fs_get_api: the statically linked 3FS, folly and fmt symbols
# stay private so they cannot interpose on the host process.
set_target_properties(
  mooncake_hf3fs_native
  PROPERTIES CXX_VISIBILITY_PRESET hidden
             VISIBILITY_INLINES_HIDDEN ON
             LINK_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/exports.map)
target_link_options(
  mooncake_hf3fs_native PRIVATE ${_link_opts} -Wl,--exclude-libs,ALL
  -Wl,--version-script=${CMAKE_CURRENT_SOURCE_DIR}/exports.map
  -Wl,--no-undefined)
