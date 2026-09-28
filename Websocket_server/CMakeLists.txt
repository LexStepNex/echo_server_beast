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

target_precompile_headers(websocket_server PRIVATE
        <boost/asio.hpp>
        <boost/asio/ssl.hpp>
        <boost/beast/core.hpp>
        <boost/beast/websocket.hpp>
        <boost/beast/http.hpp>

        <algorithm>
        <chrono>
        <deque>
        <fstream>
        <iomanip>
        <iostream>
        <map>
        <memory>
        <mutex>
        <set>
        <string>
        <string_view>
        <thread>
        <vector>

        <nlohmann/json.hpp>
)