// websocket_server.cpp
// WebSocket Secure Server с множественными клиентами и broadcast

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
#include <set>
#include <deque>
#include <vector>
#include <map>
#include <algorithm>

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

class WsSession;

// === Сервер ===
class WsServer {
public:
    WsServer(asio::io_context &io, short port, const std::string &cert, const std::string &key);

    //регистрация новой сессии
    void add_session(std::shared_ptr<WsSession> session);

    void add_session_map(std::shared_ptr<WsSession> session);

    //Очистка от мёртвых сессий
    void cleanup_dead();

    //удаление сессии
    void remove_session(WsSession *session_ptr);

    bool is_username_taken(const std::string &username);

    //получение списка онлайн пользователей
    std::vector<std::string> get_online_users();

    //Приватное сообщение
    bool send_private_message(const std::string &username, const std::string &message);

    //Рассылка всем кроме отправителя
    void broadcast(const std::string &message, const WsSession *sender);

    //Рассылка всем, включая отправителя
    void broadcast_all(const std::string &message);

private:
    void do_accept();

    void cleanup_dead_unlocked();

    asio::io_context &io_context_;
    tcp::acceptor acceptor_;
    ssl::context ssl_context_;

    std::set<std::weak_ptr<WsSession>, std::owner_less<std::weak_ptr<WsSession> > > sessions_;
    std::map<std::string, std::weak_ptr<WsSession> > sessions_map_username;

    std::mutex sessions_mutex_;
};

// === Сессия одного клиента ===
class WsSession : public std::enable_shared_from_this<WsSession> {
public:
    WsSession(tcp::socket socket, ssl::context &ctx, WsServer &server)
        : websocket_(std::move(socket), ctx),
          server_(server) {
        server_log_.open(path_server_log_, std::ios::app);
    }

    ~WsSession() {
        server_log_.close();
    }

    void start() { do_ssl_handshake(); }

    std::string get_username() const {
        return username_;
    }

    void send_to_client(const std::string &message) {
        auto self = shared_from_this();

        asio::post(websocket_.get_executor(), [self, message]() {
            if (self->is_writing_) {
                self->write_queue_.push_back(message);
                return;
            }
            self->do_write_internal(message);
        });
    }

    // 5. Отправка WS сообщения
    void do_write(std::string msg) {
        auto self = shared_from_this();
        asio::post(websocket_.get_executor(), [self, msg = std::move(msg)]() {
            if (self->is_writing_) {
                self->write_queue_.push_back(msg);
                return;
            }

            self->do_write_internal(msg);
        });
    }

private:
    void do_write_internal(const std::string &message) {
        auto self = shared_from_this();
        is_writing_ = true;

        websocket_.async_write(
            asio::buffer(message),
            [self, message](boost::system::error_code ec, std::size_t bytes_transferred) {
                self->is_writing_ = false;

                if (!ec) {
                    self->write_server_log("WRITE MESSAGE", message);

                    if (self->should_close_) {
                        self->close_connection();
                        return;
                    }

                    if (!self->write_queue_.empty()) {
                        std::string next_message = self->write_queue_.front();
                        self->write_queue_.pop_front();
                        self->do_write_internal(next_message);
                    } else {
                        if (!self->is_reading_) {
                            self->do_read();
                        }
                    }
                } else {
                    std::cerr << "[WS] Write error: " << ec.message() << std::endl;
                    self->close_connection();
                }
            });
    };

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

        server_.add_session(shared_from_this());

        do_read();
    }

    // 4. Чтение сообщений
    void do_read() {
        auto self = shared_from_this();
        is_reading_ = true;
        websocket_.async_read(
            buffer_,
            [self](boost::system::error_code ec, std::size_t /*bytes*/) {
                self->is_reading_ = false;

                if (ec == websocket::error::closed) {
                    std::cout << "[WS] Client disconnected gracefully\n";
                    self->write_server_log("DISCONNECT GRACEFUL", "", "");
                    self->close_connection();
                    return;
                }

                //Игнорируем "обрыв потока" SSL, если клиент просто закрыл вкладку
                if (ec == ssl::error::stream_truncated) {
                    std::cout << "[WS] Client connection truncated (browser closed)\n";

                    self->write_server_log("DISCONNECT TRUNCATED", "", "");
                    self->close_connection();
                    return;
                }

                if (ec) {
                    std::cerr << "[WS] Read error: " << ec.message() << "\n";
                    self->close_connection();
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

    // 6. Закрытие соединения
    void close_connection() {
        if (is_closing_) return;
        is_closing_ = true;

        auto self = shared_from_this();
        std::string address = self->address_and_port();

        if (logged_in_ && !username_.empty()) {
            json system_msg;
            system_msg["type"] = "system";
            system_msg["message"] = username_ + " left";
            server_.broadcast(system_msg.dump(), this);

            write_server_log("USER LEFT", username_);
        }

        server_.remove_session(self.get());

        boost::system::error_code ignored;
        websocket_.next_layer().lowest_layer().close(ignored);
        std::cout << "[WS] Connection closed. " << username_ << " left";

        self->write_server_log("DISCONNECTING", "", address);
    }

    std::string process_message(const std::string &raw_msg) {
        json response;
        json request;

        try {
            request = json::parse(raw_msg);
        } catch (const json::parse_error &e) {
            std::cerr << "[JSON Parse Error] " << e.what() << "\n";

            response["type"] = "error";
            response["code"] = 400;
            response["message"] = "Invalid JSON format";
            return response.dump();
        } catch (const json::exception &e) {
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

        //Маршрутизация по типу
        if (type == "login") {
            return handle_login(request);
        } else if (type == "pm") {
            return handle_private_message(request);
        } else if (type == "broadcast") {
            return handle_broadcast(request);
        } else if (type == "list") {
            return handle_list();
        } else if (type == "echo") {
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

    std::string handle_login(const json &request) {
        json response;

        if (logged_in_) {
            response["type"] = "login_response";
            response["status"] = "error";
            response["message"] = "Already logged in as " + username_;
            return response.dump();
        }

        if (!request.contains("username") || !request["username"].is_string()) {
            response["type"] = "login_response";
            response["status"] = "error";
            response["message"] = "Missing username field";
            return response.dump();
        }

        std::string new_username = request["username"].get<std::string>();

        if (new_username.empty() || new_username.size() > 20) {
            response["type"] = "login_response";
            response["status"] = "error";
            response["message"] = "Username must be 1-20 characters";
            return response.dump();
        }

        if (server_.is_username_taken(new_username)) {
            response["type"] = "login_response";
            response["status"] = "error";
            response["message"] = "Username already taken";
            return response.dump();
        }

        username_ = new_username;
        logged_in_ = true;

        response["type"] = "login_response";
        response["status"] = "ok";
        response["username"] = username_;
        response["message"] = "Welcome, " + username_ + "!";

        //Массовая рассылка о подключении
        json system_msg;
        system_msg["type"] = "system";
        system_msg["message"] = username_ + " joined";

        server_.add_session_map(shared_from_this());
        server_.broadcast(system_msg.dump(), this);

        std::cout << "USER LOGIN " << username_ << std::endl;
        write_server_log("USER LOGIN", username_);

        return response.dump();
    };

    std::string handle_private_message(const json &request) {
        json response;

        if (!logged_in_) {
            response["type"] = "error";
            response["code"] = 403;
            response["message"] = "Please login first";
            return response.dump();
        }

        if (!request.contains("to") || !request["to"].is_string()) {
            response["type"] = "error";
            response["code"] = 403;
            response["message"] = "Recipient not specified";
            return response.dump();
        }

        if (!request.contains("message") || !request["message"].is_string()) {
            response["type"] = "error";
            response["code"] = 400;
            response["message"] = "Missing message field";
            return response.dump();
        }

        std::string recipient = request["to"].get<std::string>();
        std::string message_text = request["message"].get<std::string>();

        json private_msg;
        private_msg["type"] = "pm";
        private_msg["from"] = username_;
        private_msg["message"] = message_text;
        private_msg["timestamp"] = static_cast<long long>(std::time(nullptr));

        std::string private_msg_str = private_msg.dump();

        bool delivered = server_.send_private_message(recipient, private_msg_str);
        if (!delivered) {
            response["type"] = "error";
            response["code"] = 404;
            response["message"] = "User not found or disconnected";
            return response.dump();
        }

        write_server_log("PRIVATE MESSAGE", username_, recipient);

        response["type"] = "pm_response";
        response["status"] = "ok";
        response["to"] = recipient;
        return response.dump();
    }

    std::string handle_broadcast(const json &request) {
        json response;

        if (!logged_in_) {
            response["type"] = "error";
            response["code"] = 403;
            response["message"] = "Please login first";
            return response.dump();
        }

        if (!request.contains("message") || !request["message"].is_string()) {
            response["type"] = "error";
            response["code"] = 400;
            response["message"] = "Missing message field";
            return response.dump();
        }

        std::string message_text = request["message"].get<std::string>();

        json broadcast_msg;
        broadcast_msg["type"] = "message";
        broadcast_msg["from"] = username_;
        broadcast_msg["text"] = message_text;

        std::time_t now = time(nullptr);
        broadcast_msg["timestamp"] = static_cast<long long>(now);

        std::string broadcast_str = broadcast_msg.dump();

        server_.broadcast(broadcast_str, this);

        write_server_log("BROADCAST", broadcast_str, username_);

        response["type"] = "broadcast_response";
        response["status"] = "ok";
        return response.dump();
    };

    std::string handle_list() {
        json response;
        response["type"] = "list_response";
        response["users"] = server_.get_online_users();
        return response.dump();
    }

    std::string handle_echo(const json &request) {
        json response;
        response["type"] = "echo_response";
        response["payload"] = request.value("payload", "");

        return response.dump();
    }

    std::string handle_chat(const json &request) {
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
        std::tm *tm = std::localtime(&now);

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
        response["endpoints"] = {"echo", "chat", "time", "info", "ping", "quit"};
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
            return "unknown Address"; // Или другое значение по умолчанию
        }
        return endpoint.address().to_string() + ":" + std::to_string(endpoint.port());
    }

    WsServer &server_;

    std::string username_;
    bool logged_in_ = false;

    bool is_writing_ = false;
    bool is_reading_ = false;
    std::deque<std::string> write_queue_;

    websocket::stream<ssl::stream<tcp::socket> > websocket_;
    beast::flat_buffer buffer_;
    beast::flat_buffer http_buffer_;
    beast::http::request<beast::http::string_body> http_request_;

    std::ofstream server_log_;
    const std::string path_server_log_ = "server.log";
    std::mutex mutex_log_;
    bool should_close_ = false;
    bool is_closing_ = false;
};

// Определение методов сервера
WsServer::WsServer(asio::io_context &io, short port, const std::string &cert, const std::string &key)
    : io_context_(io),
      acceptor_(io, tcp::endpoint(tcp::v4(), port)),
      ssl_context_(ssl::context::tls) {
    ssl_context_.set_options(
        ssl::context::default_workarounds | ssl::context::no_compression);
    ssl_context_.use_certificate_chain_file(cert);
    ssl_context_.use_private_key_file(key, ssl::context::pem);
    do_accept();
}

void WsServer::add_session(std::shared_ptr<WsSession> session) {
    std::lock_guard<std::mutex> lock(sessions_mutex_);
    sessions_.insert(session);
}

void WsServer::add_session_map(std::shared_ptr<WsSession> session) {
    std::lock_guard<std::mutex> lock(sessions_mutex_);
    sessions_map_username.insert({session->get_username(), session});
}

void WsServer::remove_session(WsSession *session_ptr) {
    std::lock_guard<std::mutex> lock(sessions_mutex_);

    if (session_ptr) {
        std::string username = session_ptr->get_username();

        auto it = sessions_map_username.find(username);
        if (it != sessions_map_username.end()) {
            sessions_.erase(it->second);
            sessions_map_username.erase(it);
        }
    }

    cleanup_dead_unlocked();
}

void WsServer::cleanup_dead() {
    std::lock_guard<std::mutex> lock(sessions_mutex_);
    cleanup_dead_unlocked();
}

void WsServer::cleanup_dead_unlocked() {
    for (auto it = sessions_.begin(); it != sessions_.end();) {
        if (it->expired()) { it = sessions_.erase(it); } else { ++it; }
    }
    for (auto it = sessions_map_username.begin(); it != sessions_map_username.end();) {
        if (it->second.expired()) { it = sessions_map_username.erase(it); } else { ++it; }
    }
};

bool WsServer::is_username_taken(const std::string &username) {
    std::lock_guard<std::mutex> lock(sessions_mutex_);

    auto it = sessions_map_username.find(username);
    return it != sessions_map_username.end() && !it->second.expired();
}

std::vector<std::string> WsServer::get_online_users() {
    std::lock_guard<std::mutex> lock(sessions_mutex_);
    std::vector<std::string> users;

    for (auto &weak: sessions_) {
        if (auto session = weak.lock()) {
            std::string username = session->get_username();
            if (!username.empty()) {
                users.emplace_back(username);
            }
        }
    }

    return users;
}

bool WsServer::send_private_message(const std::string &username, const std::string &message) {
    std::lock_guard<std::mutex> lock(sessions_mutex_);

    auto it = sessions_map_username.find(username);
    if (it == sessions_map_username.end()) return false; // Пользователь не найден

    auto session = it->second.lock();
    if (!session) {
        // Сессия мертва — удаляем из map
        sessions_map_username.erase(it);
        return false;
    }

    session->send_to_client(message);
    return true;
}

void WsServer::broadcast(const std::string &message, const WsSession *sender) {
    std::lock_guard<std::mutex> lock(sessions_mutex_);
    for (auto &weak: sessions_) {
        if (auto session = weak.lock()) {
            if (session.get() != sender) {
                session->send_to_client(message);
            }
        }
    }
}

void WsServer::broadcast_all(const std::string &message) {
    std::lock_guard<std::mutex> lock(sessions_mutex_);
    for (auto &weak: sessions_) {
        if (auto session = weak.lock()) {
            session->send_to_client(message);
        }
    }
}


void WsServer::do_accept() {
    acceptor_.async_accept(
        [this](boost::system::error_code ec, tcp::socket socket) {
            if (!ec) {
                auto session = std::make_shared<WsSession>(std::move(socket), ssl_context_, *this);
                session->start();
            }

            do_accept();
        });
}

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
