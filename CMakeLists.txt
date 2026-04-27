cmake_minimum_required(VERSION 3.16)
project(EchoServerAsio LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Boost REQUIRED COMPONENTS system)
find_package(OpenSSL REQUIRED)

add_executable(echo_server_asio echo_server_asio.cpp)

target_link_libraries(echo_server_asio PRIVATE
        Boost::system
        OpenSSL::SSL
        OpenSSL::Crypto
        ws2_32
        mswsock
        crypt32
)