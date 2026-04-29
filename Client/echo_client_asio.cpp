// echo_client_asio.cpp

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/asio/dispatch.hpp>

#include <iostream>
#include <string>
#include <thread>
#include <memory>
#include <mutex>
#include <deque>
#include <atomic>

#define PORT "8443"
#define HOST "127.0.0.1"

namespace asio = boost::asio;
using asio::ip::tcp;
namespace ssl = asio::ssl;

class Client : public std::enable_shared_from_this<Client> {
public:
    Client(asio::io_context &io_context, std::string host, std::string port, std::string &path_cert)
        : io_context_(io_context),
          ssl_context_(ssl::context::tls),
          stream_(io_context, ssl_context_),
          resolver_(io_context),
          host_(std::move(host)),
          port_(std::move(port)),
          work_guard_(asio::make_work_guard(io_context)),
          read_buffer_(max_length) {
        ssl_context_.load_verify_file(path_cert);
        ssl_context_.set_verify_mode(ssl::verify_peer);
    }

    void start() {
        auto self = shared_from_this();

        tcp::resolver::results_type endpoint = resolver_.resolve(host_, port_);

        asio::async_connect(
            stream_.lowest_layer(),
            endpoint,
            [self](boost::system::error_code ec, tcp::endpoint) { self->on_connect(ec); });
    }

private:
    //коллбек для подключения
    void on_connect(boost::system::error_code ec) {
        if (ec) {
            std::cerr << "Connect failed: " << ec.message() << "\n";
            return;
        }
        std::cout << "TCP connected. Starting TLS handshake...\n";

        //ассинхронное рукопожатие
        stream_.async_handshake(
            ssl::stream_base::client,
            [self = shared_from_this()](boost::system::error_code ec) {
                self->on_handshake(ec);
            });
    }

    void on_handshake(boost::system::error_code ec) {
        if (ec) {
            std::cerr << "TLS Handshake failed: " << ec.message() << "\n";
            return;
        }
        std::cout << "TLS secure channel established!\n";

        //вывод протокола подключения
        const char *protocol = SSL_get_version(stream_.native_handle());
        const SSL_CIPHER *cipher = SSL_get_current_cipher(stream_.native_handle());
        std::cout << "[Session] Protocol: " << (protocol ? protocol : "unknown") << "\n";
        if (cipher) std::cout << "[Client] Cipher: " << SSL_CIPHER_get_name(cipher) << "\n\n";

        print_prompt();
        //Запуск интерактивного ввода/вывода
        start_input_thread();
    }

    void start_input_thread() {
        std::thread([self = shared_from_this()]() {
            std::string command;

            while (getline(std::cin, command)) {
                if (command.empty()) {
                    self->print_prompt();
                    continue;
                }
                if (command == "QUIT") {
                    asio::post(self->io_context_, [self]() {
                        self->close();
                    });
                    break;
                }
                // Отправляем команду в io_context
                asio::post(self->io_context_, [self, msg = std::move(command)]() {
                    self->queue_command(msg);
                });
            }
        }).detach();
    }

    // Очередь команд (защищает от конкурентных async_write)
    void queue_command(std::string command) {
        write_queue_.push_back(std::move(command));
        if (!is_writing_) { do_write(); }
    }

    void do_write() {
        if (write_queue_.empty()) return;

        auto self = shared_from_this();
        // Ранний выход
        if (self->is_closing_) return;

        is_writing_ = true;
        std::string message = write_queue_.front() + "\r\n";
        write_queue_.pop_front();

        asio::async_write(
            stream_,
            asio::buffer(message),
            [self](boost::system::error_code ec, std::size_t) {
                if (ec) {
                    if (self->is_closing_.load()) return;

                    std::lock_guard<std::mutex> lock(self->cout_mutex_);
                    std::cerr << "Write failed: " << ec.message() << "\n";
                    self->close();
                    return;
                }

                self->do_read();
            });
    }

    // ===== Метод do_read() полностью =====
    void do_read() {
        auto self = shared_from_this();
        if (self->is_closing_.load()) return;

        asio::async_read_until(
            stream_,
            read_buffer_,
            "\r\n",
            [self](boost::system::error_code ec, std::size_t /*length*/) {
                if (self->is_closing_.load()) return;

                if (ec) {
                    if (ec == asio::error::eof || ec == ssl::error::stream_truncated) {
                        std::lock_guard<std::mutex> lock(self->cout_mutex_);
                        std::cout << "Server closed connection.\n";
                    }
                    if (!self->is_closing_.load()) { self->close(); }
                    return;
                }

                // Извлекаем строку
                std::istream is(&self->read_buffer_);
                std::string line;
                std::getline(is, line);

                // Вывод
                {
                    std::lock_guard<std::mutex> lock(self->cout_mutex_);
                    std::cout << "Server: " << line << "\n";
                    if (line.starts_with(SERVER_ERR_TIMEOUT_) && !self->is_closing_.load()) {
                        self->close();
                        return;
                    }
                }

                // Разрешаем следующую команду
                self->is_writing_ = false;
                if (!self->write_queue_.empty()) {
                    self->do_write();
                } else {
                    self->print_prompt();
                }
            });
    }

    void close() {
        //Защита от двойного закрытия, если уже закрывали, то просто выходим
        if (is_closing_.exchange(true)) { return; }
        is_closing_ = true;

        auto self = shared_from_this();

        //отменяем удержание, что бы io_context мог выйти
        work_guard_.reset();
        // 🔑 1. Отменяем все pending async-операции на stream
        // Это мгновенно разбудит async_read_until с кодом operation_aborted
        boost::system::error_code ec;
        stream_.lowest_layer().cancel(ec);

        stream_.lowest_layer().close(ec);

        // 🔑 3. Останавливаем event loop
        io_context_.stop();
    }

    void print_prompt() {
        std::lock_guard<std::mutex> lock(cout_mutex_);
        std::cout << "Input command:\n> ";
        std::cout.flush();
    }

    asio::io_context &io_context_;
    ssl::context ssl_context_;
    ssl::stream<tcp::socket> stream_;
    tcp::resolver resolver_;
    std::string host_, port_;

    static constexpr std::size_t max_length = 1024;
    asio::streambuf read_buffer_;

    // Синхронизация и очередь
    std::mutex cout_mutex_;
    std::deque<std::string> write_queue_;
    bool is_writing_ = false;
    asio::executor_work_guard<asio::io_context::executor_type> work_guard_; // Удерживает run() от выхода

    std::atomic<bool> is_closing_{false};

    //ошибки сервера
    static constexpr std::string_view SERVER_ERR_TIMEOUT_ = "ERROR: Connection timeout";
};

int main() {
    try {
        asio::io_context io_context;
        std::cout << "=== Secure Echo Client Starts ===\n";

        std::string cert_path = "C:/develop/server_lessons/echo_server_asio/certs/server.crt";

        auto client = std::make_shared<Client>(io_context, HOST, PORT, cert_path);
        client->start();

        io_context.run();
    } catch (const std::exception &e) {
        std::cerr << "Exception:" << e.what() << "\n";
    }
}