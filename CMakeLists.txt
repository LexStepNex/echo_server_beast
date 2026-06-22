cmake_minimum_required(VERSION 3.16)
project(EchoServerBeast LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Boost REQUIRED COMPONENTS system)
find_package(OpenSSL REQUIRED)
find_package(nlohmann_json REQUIRED)

if(MINGW)
    add_compile_options(-Wa,-mbig-obj)
endif()

add_subdirectory(Server)
add_subdirectory(Client)
add_subdirectory(Websocket_server)