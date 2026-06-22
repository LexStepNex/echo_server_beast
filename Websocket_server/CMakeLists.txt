add_executable(websocket_server websocket_server.cpp)

target_link_libraries(websocket_server PRIVATE
        Boost::system
        OpenSSL::SSL
        OpenSSL::Crypto
        nlohmann_json::nlohmann_json
        ws2_32
        mswsock
        crypt32
)
