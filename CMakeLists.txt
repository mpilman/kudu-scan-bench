project(kudu-scan)
cmake_minimum_required(VERSION 2.8)

set(kuduClient_DIR "/mnt/local/tell/kudu_install/share/kuduClient/cmake")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

find_package(kuduClient REQUIRED)
include_directories(${KUDU_CLIENT_INCLUDE_DIR})

find_package(TCL)
if(NOT TCL_FOUND)
    message(ERROR "TCL not found, install tcl-dev (on debian)")
endif()

include_directories(${TCL_INCLUDE_PATH})

add_executable(kudu-scan main.cpp)
target_link_libraries(kudu-scan kudu_client ${TCL_LIBRARY})

