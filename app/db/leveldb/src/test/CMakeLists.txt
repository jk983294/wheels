file(GLOB ldb_test_srcs "*.cpp" "*.cc")

enable_testing()

add_executable(test_ldb ${ldb_test_srcs})
target_include_directories(test_ldb PUBLIC include)
target_include_directories(test_ldb SYSTEM PRIVATE /opt/3rd/common/include)
target_link_libraries(test_ldb leveldb_static pthread rt dl)
