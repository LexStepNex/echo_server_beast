// echo_server_asio.cpp
// Асинхронный Echo Server на Boost.Asio

#include <boost/asio.hpp>
#include <iostream>
#include <memory>
#include <ctime>
#include <iomanip>
#include <sstream>

using boost::asio::ip::tcp;

//Сессия один клиент
class Session : public std::enable_shared_from_this<Session> {
public:
    Session(tcp::socket socket) : socket_(std::move(socket)), data_(max_length, '\0') {
    }

    void start() {
        auto endpoint = socket_.remote_endpoint();
        std::cout << "Client connected: " << endpoint.address().to_string() << ":" << endpoint.port() << std::endl;
        do_read();
    }

private:
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
        socket_.async_read_some(
            boost::asio::buffer(data_.data(), max_length),
            [this, self](boost::system::error_code ec, std::size_t length) {
                if (!ec) {
                    std::cout << "Received: " << length << " bytes" << std::endl;

                    data_.resize(length);
                    while (!data_.empty() && (data_.back() == '\r' || data_.back() == '\n')) {
                        data_.pop_back();
                    }
                    //Данные получены, отправляем эхо
                    if (data_.starts_with("TIME")) {
                        timeNow();
                    } else if (data_.starts_with("QUIT")) {
                        data_ = "Goodbye!";
                        do_write(data_.length()); // ✅ Сначала отправляем ответ
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
                    do_write(data_.length());
                } else {
                    std::cout << "Client disconnected" << std::endl;
                }
            });
    }

    void do_write(std::size_t length) {
        auto self = shared_from_this();
        boost::asio::async_write(
            socket_,
            boost::asio::buffer(data_.data(), length),
            [this, self](boost::system::error_code ec, std::size_t length) {
                if (!ec) {
                    std::cout << "Sent: " << length << " bytes" << std::endl;

                    //Эхо отправлено - ждём следующее сообщение
                    data_.resize(max_length, '\0');
                    do_read();
                } else {
                    std::cout << "[Session] Write error: " << ec.message() << "\n";
                }
            });
    }

    tcp::socket socket_;
    static constexpr std::size_t max_length = 1024;
    // char data_[max_length];
    std::string data_;
};

// Сервер принимает подключения
class Server {
public:
    Server(boost::asio::io_context &io_context, short port)
        : acceptor_(io_context, tcp::endpoint(tcp::v4(), port)) {
        do_accept();
    }

private:
    void do_accept() {
        acceptor_.async_accept([this](boost::system::error_code ec, tcp::socket socket) {
            if (!ec) {
                //Создаём сессию для нового клиента
                std::make_shared<Session>(std::move(socket))->start();

                //Принимаем следующего клиента
                do_accept();
            }
        });
    }

    tcp::acceptor acceptor_;
};

int main() {
    try {
        boost::asio::io_context io_context;

        std::cout << "=== Asio Echo Server Starts ===\n";
        std::cout << "Listen port 8080\n";
        Server server(io_context, 8080);

        io_context.run();
    } catch (std::exception &e) {
        std::cerr << "Exception: " << e.what() << std::endl;
    }
    return 0;
}
