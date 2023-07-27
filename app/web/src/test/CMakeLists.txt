link_directories(/opt/version/latest/ornate/lib)

file(GLOB web_test_srcs "*.cpp")

enable_testing()

add_executable(test.web ${web_test_srcs})
target_include_directories(test.web PUBLIC include)
target_link_libraries(test.web webcommon common_3rd_lib pthread rt dl)
