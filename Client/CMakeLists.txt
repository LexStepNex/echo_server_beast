add_executable(echo_client_asio echo_client_asio.cpp)

target_link_libraries(echo_client_asio PRIVATE
        Boost::system
        OpenSSL::SSL
        OpenSSL::Crypto
        ws2_32
        mswsock
        crypt32
)