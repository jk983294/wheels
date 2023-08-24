cmake_minimum_required(VERSION 3.9)

# Keep the version below in sync with the one in db.h
project(leveldb VERSION 1.23.0 LANGUAGES C CXX)

# C standard can be overridden when this is used as a sub-project.
if(NOT CMAKE_C_STANDARD)
  # This project can use C11, but will gracefully decay down to C89.
  set(CMAKE_C_STANDARD 11)
  set(CMAKE_C_STANDARD_REQUIRED OFF)
  set(CMAKE_C_EXTENSIONS OFF)
endif(NOT CMAKE_C_STANDARD)

# C++ standard can be overridden when this is used as a sub-project.
if(NOT CMAKE_CXX_STANDARD)
  # This project requires C++11.
  set(CMAKE_CXX_STANDARD 11)
  set(CMAKE_CXX_STANDARD_REQUIRED ON)
  set(CMAKE_CXX_EXTENSIONS OFF)
endif(NOT CMAKE_CXX_STANDARD)

include(CheckLibraryExists)
check_library_exists(crc32c crc32c_value "" HAVE_CRC32C)
check_library_exists(snappy snappy_compress "" HAVE_SNAPPY)
check_library_exists(zstd zstd_compress "" HAVE_ZSTD)
check_library_exists(tcmalloc malloc "" HAVE_TCMALLOC)

include(CheckCXXSymbolExists)
check_cxx_symbol_exists(fdatasync "unistd.h" HAVE_FDATASYNC)
check_cxx_symbol_exists(F_FULLFSYNC "fcntl.h" HAVE_FULLFSYNC)
check_cxx_symbol_exists(O_CLOEXEC "fcntl.h" HAVE_O_CLOEXEC)

# Enable strict prototype warnings for C code in clang and gcc.
if(NOT CMAKE_C_FLAGS MATCHES "-Wstrict-prototypes")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wstrict-prototypes")
endif(NOT CMAKE_C_FLAGS MATCHES "-Wstrict-prototypes")

# Disable C++ exceptions.
#string(REGEX REPLACE "-fexceptions" "" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-exceptions")

# Disable RTTI.
string(REGEX REPLACE "-frtti" "" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-rtti")

message(HAVE_CRC32C="${HAVE_CRC32C}")
message(HAVE_SNAPPY="${HAVE_SNAPPY}")
message(HAVE_ZSTD="${HAVE_ZSTD}")
message(HAVE_TCMALLOC="${HAVE_TCMALLOC}")
message(HAVE_FDATASYNC="${HAVE_FDATASYNC}")
message(HAVE_FULLFSYNC="${HAVE_FULLFSYNC}")
message(HAVE_O_CLOEXEC="${HAVE_O_CLOEXEC}")

set(LEVELDB_PUBLIC_INCLUDE_DIR "include/leveldb")
set(LEVELDB_PORT_CONFIG_DIR "include/port")

include_directories(include)

# Only export LEVELDB_EXPORT symbols from the shared library.
add_compile_options(-fvisibility=hidden)

file(GLOB LevelDBSrc "src/db/*.cc" "src/memenv/*.cc" "src/table/*.cc" "src/util/*.cc")
add_library(leveldb SHARED ${LevelDBSrc})
set_target_properties(leveldb PROPERTIES VERSION ${PROJECT_VERSION} SOVERSION ${PROJECT_VERSION_MAJOR})

add_library(leveldb_static STATIC ${LevelDBSrc})
set_target_properties(leveldb_static PROPERTIES VERSION ${PROJECT_VERSION} SOVERSION ${PROJECT_VERSION_MAJOR})

if(HAVE_CRC32C)
  target_link_libraries(leveldb crc32c)
  target_link_libraries(leveldb_static crc32c)
endif(HAVE_CRC32C)
if(HAVE_SNAPPY)
  target_link_libraries(leveldb snappy)
  target_link_libraries(leveldb_static snappy)
endif(HAVE_SNAPPY)
if(HAVE_ZSTD)
  target_link_libraries(leveldb zstd)
  target_link_libraries(leveldb_static zstd)
endif(HAVE_ZSTD)
if(HAVE_TCMALLOC)
  target_link_libraries(leveldb tcmalloc)
  target_link_libraries(leveldb_static tcmalloc)
endif(HAVE_TCMALLOC)

# Needed by port_stdcxx.h
find_package(Threads REQUIRED)
target_link_libraries(leveldb Threads::Threads)
target_link_libraries(leveldb_static Threads::Threads)

add_subdirectory(src/play)
add_subdirectory(src/test)
