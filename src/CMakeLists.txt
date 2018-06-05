cmake_minimum_required(VERSION 2.6)

project(Suanfen C CXX)

ADD_COMPILE_OPTIONS(-DOUTPUT)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -Wall -Wextra")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -lpthread")

include_directories(include)
add_subdirectory(include)

#OPTION(FORCE_TESTS "Build tests, regardless of build type." ON)
#if (CMAKE_BUILD_TYPE MATCHES "[Dd][Ee][Bb][Uu][Gg]" OR FORCE_TESTS)
    #add_subdirectory(test)
#endif()

if (MAKE_JOINER)
	add_executable(Joiner main.cpp)
	target_link_libraries(Joiner database threadpool pthread)
	install(TARGETS Joiner
		DESTINATION ${CMAKE_CURRENT_SOURCE_DIR}/../bin)

endif()

if (MAKE_TEST)
	add_executable(test test.cpp)
	target_link_libraries(test threadpool pthread)
	install(TARGETS test 
		DESTINATION ${CMAKE_CURRENT_SOURCE_DIR}/../bin)
endif()


