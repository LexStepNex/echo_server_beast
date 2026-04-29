add_executable(echo_server_asio echo_server_asio.cpp)

target_link_libraries(echo_server_asio PRIVATE
        Boost::system
        OpenSSL::SSL
        OpenSSL::Crypto
        ws2_32
        mswsock
        crypt32
)
