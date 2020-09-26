#cmake_minimum_required(VERSION 3.12)
#project(ex3_try2)
#
#set(CMAKE_CXX_STANDARD 11)
#
#add_library(ex3_try2 library.cpp library.h)

cmake_minimum_required(VERSION 3.12)
project(ex3_try2)

set(CMAKE_CXX_STANDARD 11)
SET(CMAKE_CXX_FLAGS -pthread)

add_library(ex3final MapReduceFramework.cpp MapReduceFramework.h atomicdemo.cpp Barrier.cpp Barrier.h
        test_2.cpp SampleClient.cpp barrierdemo.cpp WordFrequencies.cpp eurovisionClient.cpp bigClient.cpp
        littleClient.cpp)


add_executable(test test_2.cpp)
#add_executable(WOrdTEts test_2.cpp)