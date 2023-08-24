file(GLOB CommonSrc "*.cpp")
add_library(fra_common STATIC ${CommonSrc})
target_link_libraries(fra_common ${OpenMP_CXX_LIBRARIES} pthread)
set_property(TARGET fra_common PROPERTY POSITION_INDEPENDENT_CODE ON)
