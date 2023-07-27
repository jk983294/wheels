cmake_minimum_required(VERSION 3.16)
project(webapp)

set(CMAKE_CXX_STANDARD 17)

#SET(CMAKE_CXX_FLAGS "-std=c++17 -O3 -s -Wall -Werror")
SET(CMAKE_CXX_FLAGS "-std=c++17 -g -Wall -Werror")

FIND_PACKAGE(pugixml)

include_directories(SYSTEM /opt/3rd/common/include)
include_directories(SYSTEM /opt/version/latest/frenzy/include)
include_directories(/opt/version/latest/zergbase/include)
include_directories(/opt/version/latest/zergtool/include)
include_directories(include)

link_directories(/opt/3rd/common/lib)
link_directories(/opt/version/latest/frenzy/lib)
link_directories(/opt/version/latest/lib)

add_subdirectory(src/common)
add_subdirectory(src/play)
add_subdirectory(src/test)

