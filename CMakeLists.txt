cmake_minimum_required(VERSION 3.16)
project(EchoServerAsio LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 20)

# === Только заголовки Boost ===
if(WIN32)
    set(BOOST_ROOT "C:/Boost/boost_1_85_0")
endif()

cmake_policy(SET CMP0144 NEW)
cmake_policy(SET CMP0167 OLD)

find_package(Boost REQUIRED)  # Без COMPONENTS

add_executable(echo_server_asio echo_server_asio.cpp)

target_include_directories(echo_server_asio PRIVATE ${Boost_INCLUDE_DIRS})

# Windows-специфичные либы (обязательно для Asio)
if(WIN32)
    target_link_libraries(echo_server_asio PRIVATE ws2_32 mswsock)
endif()