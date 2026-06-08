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

#ifdef _WIN32
#include <windows.h>
#endif

namespace asio = boost::asio;
namespace beast = boost::beast;
namespace websocket = beast::websocket;
namespace ssl = asio::ssl;
using asio::ip::tcp;

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
    // Режимы маршрутизации
    enum class Mode { Echo, Chat };

    Mode mode_ = Mode::Echo;

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

                std::string_view target = self->http_request_.target();
                std::cout << "[WS] Request path: " << target << "\n";

                // Проверяем Upgrade-заголовки
                if (beast::websocket::is_upgrade(self->http_request_)) {
                    // Маршрутизация
                    if (target == "/echo" || target == "/") {
                        self->mode_ = Mode::Echo;
                        std::cout << "[WS] Mode: Echo\n";
                    } else if (target == "/chat") {
                        self->mode_ = Mode::Chat;
                        std::cout << "[WS] Mode: Chat\n";
                    } else {
                        // Неизвестный WS путь -> закрываем
                        std::cout << "[WS] Unknown WS path: " << target << "\n";
                        self->close_connection();
                        return;
                    }

                    // Принимаем как WebSocket
                    self->websocket_.async_accept(
                        self->http_request_,
                        [self](boost::system::error_code ec) {
                            self->on_ws_accept(ec);
                        });
                } else {
                    // Это обычный HTTP запрос (браузер зашел просто так)
                    // Мы не поддерживаем HTTP ответы, просто закрываем соединение
                    std::cout << "[WS] Non-WS request received. Closing.\n";
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
        std::cout << "[WS] Client upgraded (mode: "
                << (mode_ == Mode::Echo ? "Echo" : "Chat") << ")\n";
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

                std::string msg = beast::buffers_to_string(self->buffer_.data());
                std::cout << "[WS] Received ("
                        << (self->mode_ == Mode::Echo ? "Echo" : "Chat")
                        << "): " << msg << "\n";

                std::string response = (self->mode_ == Mode::Echo)
                                           ? msg
                                           : "[CHAT] " + msg;

                self->buffer_.consume(self->buffer_.size());

                self->write_server_log("READ MESSAGE", msg);

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
                    self->do_read();
                    self->write_server_log("WRITE MESSAGE", msg);
                } else { std::cerr << "[WS] Write error: " << ec.message() << "\n"; }
            });
    }

    // 6. Закрытие соединения
    void close_connection() {
        auto self = shared_from_this();
        boost::system::error_code ignored;
        websocket_.next_layer().lowest_layer().close(ignored);
        std::cout << "[WS] Connection closed.\n";

        self->write_server_log("DISCONNECTING", "", address_and_port());
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
                server_log_ << (mode_ == Mode::Echo ? "(Echo)" : "(Chat)") << ": \"" << msg << "\""
                        << " (" << msg.size() << " bytes) " << "\n";
            }

        }

        server_log_.flush();
    }

    std::string address_and_port(boost::system::error_code ec = boost::system::error_code()) {
        auto endpoint = websocket_.next_layer().next_layer().remote_endpoint(ec);
        auto address = endpoint.address().to_string() + ":";
        address += std::to_string(endpoint.port());
        return address;
    }

    websocket::stream<ssl::stream<tcp::socket> > websocket_;
    beast::flat_buffer buffer_;
    beast::flat_buffer http_buffer_;
    beast::http::request<beast::http::string_body> http_request_;

    std::ofstream server_log_;
    const std::string path_server_log_ = "server.log";
    std::mutex mutex_log_;
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
    SetConsoleOutputCP(CP_UTF8);

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
