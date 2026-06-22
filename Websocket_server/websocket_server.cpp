// websocket_server.cpp
// Упрощенный WebSocket Secure Server (только WS)

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/websocket/ssl.hpp>
#include <boost/beast/http.hpp>

#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <fstream>
#include <ctime>
#include <iomanip>
#include <thread>
#include <mutex>

#include <nlohmann/json.hpp>

#ifdef _WIN32
#include <windows.h>
#endif

namespace asio = boost::asio;
namespace beast = boost::beast;
namespace websocket = beast::websocket;
namespace ssl = asio::ssl;
using asio::ip::tcp;
using json = nlohmann::json;

auto nowTime() {
    std::time_t now = std::time(nullptr);
    std::tm *tm = std::localtime(&now);

    return std::put_time(tm, "%Y-%m-%d %H:%M:%S");
}

// === Сессия одного клиента ===
class WsSession : public std::enable_shared_from_this<WsSession> {
public:
    WsSession(tcp::socket socket, ssl::context &ctx)
        : websocket_(std::move(socket), ctx) {
        server_log_.open(path_server_log_, std::ios::app);
    }

    ~WsSession() {
        server_log_.close();
    }

    void start() { do_ssl_handshake(); }

private:
    // 1. TLS Handshake
    void do_ssl_handshake() {
        auto self = shared_from_this();
        websocket_.next_layer().async_handshake(
            ssl::stream_base::server,
            [self](boost::system::error_code ec) {
                if (!ec) {
                    std::cout << "[WS] TLS OK. Waiting for HTTP request...\n";
                    self->write_server_log("CONNECTING", "", self->address_and_port(ec));

                    self->read_http_request();
                } else {
                    std::cerr << "[WS] TLS failed: " << ec.message() << "\n";
                }
            });
    }

    // 2. Чтение запроса
    void read_http_request() {
        auto self = shared_from_this();
        beast::http::async_read(
            websocket_.next_layer(),
            http_buffer_,
            http_request_,
            [self](boost::system::error_code ec, std::size_t /*bytes*/) {
                if (ec) {
                    std::cerr << "[WS] Request read error: " << ec.message() << "\n";
                    return;
                }

                if (beast::websocket::is_upgrade(self->http_request_)) {
                    self->websocket_.async_accept(
                        self->http_request_,
                        [self](boost::system::error_code ec) {
                            self->on_ws_accept(ec);
                        });
                } else {
                    std::cout << "[WS] Non-WS request. Closing.\n";
                    self->close_connection();
                }
            });
    }

    // 3. После Upgrade -> читаем фреймы
    void on_ws_accept(boost::system::error_code ec) {
        if (ec) {
            std::cerr << "[WS] Accept failed: " << ec.message() << "\n";
            return;
        }
        std::cout << "[WS] Client connected (JSON protocol)\n";
        do_read();
    }

    // 4. Чтение сообщений
    void do_read() {
        auto self = shared_from_this();
        websocket_.async_read(
            buffer_,
            [self](boost::system::error_code ec, std::size_t /*bytes*/) {
                if (ec == websocket::error::closed) {
                    std::cout << "[WS] Client disconnected gracefully\n";
                    self->write_server_log("DISCONNECT GRACEFUL", "", "");
                    return;
                }

                //Игнорируем "обрыв потока" SSL, если клиент просто закрыл вкладку
                if (ec == ssl::error::stream_truncated) {
                    std::cout << "[WS] Client connection truncated (browser closed)\n";

                    self->write_server_log("DISCONNECT TRUNCATED", "", "");
                    return;
                }

                if (ec) {
                    std::cerr << "[WS] Read error: " << ec.message() << "\n";
                    return;
                }

                std::string raw_msg = beast::buffers_to_string(self->buffer_.data());
                std::cout << "[WS] Received: " << raw_msg << "\n";

                self->buffer_.consume(self->buffer_.size());

                std::string response = self->process_message(raw_msg);

                self->write_server_log("READ MESSAGE", raw_msg);
                self->do_write(std::move(response));
            });
    }

    // 5. Отправка WS сообщения
    void do_write(std::string msg) {
        auto self = shared_from_this();
        websocket_.async_write(
            asio::buffer(msg),
            [self, msg](boost::system::error_code ec, std::size_t /*bytes*/) {
                if (!ec) {
                    self->write_server_log("WRITE MESSAGE", msg);
                    if (self->should_close_) {
                        self->close_connection();
                        return;
                    }
                    self->do_read();
                } else { std::cerr << "[WS] Write error: " << ec.message() << "\n"; }
            });
    }

    // 6. Закрытие соединения
    void close_connection() {
        auto self = shared_from_this();
        std::string address = self->address_and_port();

        boost::system::error_code ignored;
        websocket_.next_layer().lowest_layer().close(ignored);
        std::cout << "[WS] Connection closed.\n";

        self->write_server_log("DISCONNECTING", "", address);
    }

    std::string process_message(const std::string& raw_msg) {
        json response;

        //Попытка распарсить JSON
        json request;
        try {
            request = json::parse(raw_msg);
        } catch ( const json::parse_error& e) {
            std::cerr << "[JSON Parse Error] " << e.what() << "\n";

            response["type"] = "error";
            response["code"] = 400;
            response["message"] = "Invalid JSON format";
            return response.dump();
        } catch (const json::exception& e) {
            std::cerr << "[JSON Error] " << e.what() << "\n";

            response["type"] = "error";
            response["code"] = 400;
            response["message"] = "JSON processing error";

            return response.dump();
        }

        //проверка наличия поля type
        if (!request.contains("type") || !request["type"].is_string()) {
            response["type"] = "error";
            response["code"] = 400;
            response["message"] = "Missing or invalid JSON \"type\" field";
            return response.dump();
        }

        std::string type = request["type"].get<std::string>();

        //Маршрутизация по тупу
        if (type == "echo") {
            return handle_echo(request);
        } else if (type == "chat") {
            return handle_chat(request);
        } else if (type == "time") {
            return handle_time();
        } else if (type == "info") {
            return handle_info();
        } else if (type == "ping") {
            return handle_ping();
        } else if (type == "quit") {
            return handle_quit();
        } else {
            response["type"] = "error";
            response["code"] = 404;
            response["message"] = "Unknown message type: " + type;
            return response.dump();
        }
    }

    std::string handle_echo(const json& request) {
        json response;
        response["type"] = "echo_response";
        response["payload"] = request.value("payload", "");

        return response.dump();
    }

    std::string handle_chat(const json& request) {
        json response;
        response["type"] = "chat_response";
        std::string payload = request.value("payload", "");
        response["payload"] = "[CHAT]: " + payload;

        return response.dump();
    }

    std::string handle_time() {
        json response;
        response["type"] = "time_response";

        std::time_t now = std::time(nullptr);
        std::tm* tm = std::localtime(&now);

        std::ostringstream oss;
        oss << "[TIME]: " << std::put_time(tm, "%Y-%m-%d %H:%M:%S");
        response["payload"] = oss.str();
        response["timestamp"] = static_cast<long long>(now);

        return response.dump();
    }

    std::string handle_info() {
        json response;
        response["type"] = "info_response";
        response["version"] = "1.0";
        response["protocol"] = "JSON";
        response["uptime"] = "N/A"; //время бесперебойной работы реализовать позже
        response["clients"] = 1; //временная заглушка
        response["endpoints"] = {"echo", "chat", "time","info", "ping", "quit"};
        return response.dump();
    }

    std::string handle_ping() {
        json response;
        response["type"] = "ping_response";
        response["timestamp"] = static_cast<long long>(time(nullptr));
        response["latency_ms"] = 0; //добавить расчёт задержки
        return response.dump();
    }

    std::string handle_quit() {
        json response;
        response["type"] = "quit_response";
        response["message"] = "Goodbye!";

        //Соединение закроется после отправки в do_write();
        should_close_ = true;
        return response.dump();
    }

    void write_server_log(std::string type, std::string msg = "", std::string address = "") {
        std::lock_guard<std::mutex> guard(mutex_log_);

        if (server_log_.is_open()) {
            server_log_ << '[' << nowTime() << "] " << type << " ";

            if (address.empty() && msg.empty()) {
                server_log_ << "\n";
                return;
            }

            if (!address.empty()) {
                server_log_ << address << (msg.empty() ? "\n" : " ");
            }

            if (!msg.empty()) {
                server_log_ << "\"" << msg << "\"" << " (" << msg.size() << " bytes) " << "\n";
            }
        }

        server_log_.flush();
    }

    std::string address_and_port(boost::system::error_code ec = boost::system::error_code()) {
        auto endpoint = websocket_.next_layer().next_layer().remote_endpoint(ec);
        if (ec) {
            return "unknown Address";  // Или другое значение по умолчанию
        }
        return endpoint.address().to_string() + ":" + std::to_string(endpoint.port());
    }

    websocket::stream<ssl::stream<tcp::socket> > websocket_;
    beast::flat_buffer buffer_;
    beast::flat_buffer http_buffer_;
    beast::http::request<beast::http::string_body> http_request_;

    std::ofstream server_log_;
    const std::string path_server_log_ = "server.log";
    std::mutex mutex_log_;
    bool should_close_ = false;
};

// === Сервер ===
class WsServer {
public:
    WsServer(asio::io_context &io, short port, const std::string &cert, const std::string &key)
        : acceptor_(io, tcp::endpoint(tcp::v4(), port)), ssl_context_(ssl::context::tls) {
        ssl_context_.set_options(
            ssl::context::default_workarounds | ssl::context::no_compression);
        ssl_context_.use_certificate_chain_file(cert);
        ssl_context_.use_private_key_file(key, ssl::context::pem);
        do_accept();
    }

private:
    void do_accept() {
        acceptor_.async_accept(
            [this](boost::system::error_code ec, tcp::socket socket) {
                if (!ec) std::make_shared<WsSession>(std::move(socket), ssl_context_)->start();
                do_accept();
            });
    }

    tcp::acceptor acceptor_;
    ssl::context ssl_context_;
};

int main() {
#ifdef WIN32
    SetConsoleOutputCP(CP_UTF8);
#endif

    try {
        asio::io_context io_context;
        std::string cert = "C:/develop/server_lessons/echo_server_beast/certs/server.crt";
        std::string key = "C:/develop/server_lessons/echo_server_beast/certs/server.key";

        std::cout << "=== WSS Router Server ===\n";
        std::cout << "Listening on wss://127.0.0.1:8443\n";

        WsServer server(io_context, 8443, cert, key);
        io_context.run();
    } catch (std::exception &e) {
        std::cerr << "Exception: " << e.what() << "\n";
    }

    return 0;
}
