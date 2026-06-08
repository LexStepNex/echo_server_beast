// echo_server_beast.cpp
// Асинхронный Echo Server на Boost.Asio

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/asio/steady_timer.hpp>

#include <openssl/ssl.h>

#include <iostream>
#include <memory>
#include <ctime>
#include <iomanip>
#include <sstream>

namespace asio = boost::asio;
using asio::ip::tcp;
namespace ssl = asio::ssl;

//Сессия для одного клиента
class Session : public std::enable_shared_from_this<Session> {
public:
    Session(tcp::socket socket, ssl::context &sslContext)
        : stream_(std::move(socket), sslContext),
          data_(max_length, '\0'),
          timer_(stream_.get_executor()) {
    }

    void start() {
        do_handshake();
    }

private:
    void do_handshake() {
        auto self = shared_from_this();
        stream_.async_handshake(
            ssl::stream_base::server, // ← Мы сервер
            [this, self](boost::system::error_code ec) {
                if (!ec) {
                    std::cout << "[Session] TLS handshake OK\n";
                    const char *protocol = SSL_get_version(stream_.native_handle());
                    std::cout << "[Session] Protocol: " << (protocol ? protocol : "unknown") << "\n";

                    // 🔑 Опционально: логирование шифра
                    const SSL_CIPHER *cipher = SSL_get_current_cipher(stream_.native_handle());
                    if (cipher) {
                        std::cout << "[Session] Cipher: " << SSL_CIPHER_get_name(cipher) << "\n";
                    }
                    auto endpoint = stream_.lowest_layer().remote_endpoint();
                    std::cout << "Client connected: " << endpoint.address().to_string()
                            << ":" << endpoint.port() << std::endl;

                    start_timeout();
                    do_read();
                } else {
                    std::cout << "[Session] Rejected non-TLS client: " << ec.message() << "\n";
                }
            });
    }

    void start_timeout() {
        timer_.expires_after(std::chrono::seconds(10));
        timer_.async_wait(
            [self = shared_from_this()](boost::system::error_code ec) {
                if (!ec) {
                    std::cout << "[Session] Client timeout, disconnecting\n";
                    self->send_timeout_and_close();
                }
            });
    }

    void cancel_timeout() {
        timer_.cancel();
    }

    void send_timeout_and_close() {
        is_active_ = false;

        auto self = shared_from_this();
        std::string timeout_msg = "ERROR: Connection timeout\r\n";

        boost::asio::async_write(
            stream_,
            asio::buffer(timeout_msg),
            [self](boost::system::error_code ec, std::size_t length) {
                // Закрываем окончательно
                self->stream_.async_shutdown(
                    [self](boost::system::error_code) {
                        boost::system::error_code ignored_ec;
                        self->stream_.lowest_layer().close(ignored_ec);
                        std::cout << "[Session] Timeout. Secure connection closed\n";
                    });
            });
    }

    void timeNow() {
        data_.clear();
        time_t now = time(nullptr);
        tm *timeInfo = localtime(&now);

        std::ostringstream oss;
        oss << std::put_time(timeInfo, "%H:%M:%S");
        data_ = oss.str();
    }

    void do_read() {
        auto self = shared_from_this();

        if (!is_active_) {
            return;
        }

        stream_.async_read_some(
            asio::buffer(data_.data(), max_length),
            [this, self](boost::system::error_code ec, std::size_t length) {
                if (!is_active_) {
                    return;
                }

                if (!ec) {
                    std::cout << "Received: " << length << " bytes" << std::endl;

                    data_.resize(length);
                    while (!data_.empty() && (data_.back() == '\r' || data_.back() == '\n')) {
                        data_.pop_back();
                    }

                    cancel_timeout();

                    //Данные получены, отправляем эхо
                    if (data_.starts_with("TIME")) {
                        timeNow();
                    } else if (data_.starts_with("QUIT")) {
                        data_ = "Goodbye!";
                        do_write(); //
                        return;
                    } else if (data_.starts_with("ECHO ")) {
                        data_.erase(0, 5);
                        if (data_.empty()) {
                            data_ = "EMPTY DATA";
                        }
                    } else {
                        data_ = "ERROR: Unknown command";
                    }

                    std::cout << data_ << std::endl;
                    do_write();
                } else {
                    std::cout << "Client disconnected" << std::endl;
                }
            });
    }

    void do_write() {
        if (!is_active_) {
            return;
        }

        auto self = shared_from_this();

        std::string message = data_ + "\r\n";

        asio::async_write(
            stream_,
            asio::buffer(message),
            [this, self](boost::system::error_code ec, std::size_t length) {
                if (!is_active_) {
                    return;
                }

                if (!ec) {
                    std::cout << "Sent: " << length << " bytes" << std::endl;

                    cancel_timeout();
                    //Эхо отправлено - ждём следующее сообщение
                    data_.resize(max_length, '\0');

                    start_timeout();
                    do_read();
                } else {
                    std::cout << "[Session] Write error: " << ec.message() << "\n";
                }
            });
    }

    ssl::stream<tcp::socket> stream_;
    asio::steady_timer timer_;
    bool is_active_ = true;

    static constexpr std::size_t max_length = 1024;
    std::string data_;
};

// Сервер принимает подключения
class Server {
public:
    Server(asio::io_context &io_context, short port,
           const std::string &cert_file, const std::string &key_file)
        : acceptor_(io_context, tcp::endpoint(tcp::v4(), port)),
          ssl_context_(ssl::context::tls) {
        // ← Авто-выбор современной версии
        configure_ssl_context(cert_file, key_file);
        do_accept();
    }

private:
    void configure_ssl_context(const std::string &cert_file, const std::string &key_file) {
        // Безопасные настройки
        ssl_context_.set_options(
            ssl::context::default_workarounds |
            ssl::context::no_compression |
            ssl::context::single_dh_use);

        // Отключаем устаревшие версии явно (опционально, но полезно для логов)
        SSL_CTX_set_min_proto_version(ssl_context_.native_handle(), TLS1_2_VERSION);

        ssl_context_.use_certificate_chain_file(cert_file);
        ssl_context_.use_private_key_file(key_file, ssl::context::pem);

        std::cout << "[Server] SSL context configured with cert & key\n";
    }

    void do_accept() {
        acceptor_.async_accept([this](boost::system::error_code ec, tcp::socket socket) {
            if (!ec) {
                //Создаём сессию для нового клиента
                std::make_shared<Session>(std::move(socket), ssl_context_)->start();

                //Принимаем следующего клиента
                do_accept();
            }
        });
    }

    tcp::acceptor acceptor_;
    ssl::context ssl_context_;
};

int main() {
    try {
        asio::io_context io_context;

        std::cout << "=== Secure Echo Server Starts ===\n";
        std::cout << "Listening on port 8443 (TLS 1.2+)...\n";
        std::string cert_path = "C:/develop/server_lessons/echo_server_beast/certs/server.crt";
        std::string key_path = "C:/develop/server_lessons/echo_server_beast/certs/server.key";

        Server server(io_context, 8443, cert_path, key_path);

        io_context.run();
    } catch (std::exception &e) {
        std::cerr << "Exception: " << e.what() << std::endl;
    }
    return 0;
}
