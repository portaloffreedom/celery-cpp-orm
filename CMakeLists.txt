cmake_minimum_required(VERSION 3.19)
project(celery_cpp)

set(CMAKE_CXX_STANDARD 17)

find_package(PkgConfig)
add_subdirectory(thirdparty/SimpleAmqpClient)
pkg_check_modules(JSONCPP REQUIRED jsoncpp>1.7)

add_executable(call_task
        call_task.cpp)

add_executable(test_worker
        test_worker.cpp)


target_link_libraries(call_task
        PUBLIC SimpleAmqpClient
        PUBLIC ${JSONCPP_LIBRARIES}
        )
target_link_directories(call_task
        PUBLIC ${JSONCPP_LIBRARY_DIRS}
        )
target_include_directories(call_task
        PUBLIC ${JSONCPP_INCLUDE_DIRS}
)

target_link_libraries(test_worker
        PUBLIC SimpleAmqpClient
        PUBLIC ${JSONCPP_LIBRARIES}
        )
target_link_directories(test_worker
        PUBLIC ${JSONCPP_LIBRARY_DIRS}
        )
target_include_directories(test_worker
        PUBLIC ${JSONCPP_INCLUDE_DIRS}
)
