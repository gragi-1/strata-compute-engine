#include "docker.hpp"
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstring>
#include <curl/curl.h>
#include <filesystem>
#include <string>
#include <sys/socket.h>
#include <sys/un.h>
#include <thread>
#include <unistd.h>
#include <vector>

// Exercise the real libcurl timeout against a concurrent Docker-protocol peer.
// A start request can finish after its caller disconnects, leaving Created observable first.
class Peer {
  public:
    std::string socket_path;
    std::atomic<int> starts{0}, workloads{0}, deletes{0}, inspections{0};
    std::atomic<int> workload_starts{0};
    std::atomic<bool> inspected_created{false};
    std::atomic<bool> workload_inspected_created{false};
    explicit Peer(bool reject_start = false) : reject_(reject_start) {
        char directory[] = "/tmp/strata-docker-start-XXXXXX";
        const auto allocated = mkdtemp(directory);
        assert(allocated);
        directory_ = allocated;
        socket_path = directory_ + "/docker.sock";
        listener_ = socket(AF_UNIX, SOCK_STREAM, 0);
        assert(listener_ >= 0);
        sockaddr_un address{};
        address.sun_family = AF_UNIX;
        assert(socket_path.size() < sizeof(address.sun_path));
        std::memcpy(address.sun_path, socket_path.c_str(), socket_path.size() + 1);
        assert(bind(listener_, reinterpret_cast<sockaddr *>(&address), sizeof(address)) == 0);
        assert(listen(listener_, 16) == 0);
        acceptor_ = std::thread([this] {
            while (!stopped_) {
                const auto client = accept(listener_, nullptr, nullptr);
                if (client < 0)
                    break;
                clients_.emplace_back([this, client] {
                    try {
                        serve(client);
                    } catch (...) {
                    }
                    close(client);
                });
            }
        });
    }
    ~Peer() {
        stopped_ = true;
        shutdown(listener_, SHUT_RDWR);
        acceptor_.join();
        close(listener_);
        for (auto &client : clients_)
            client.join();
        std::filesystem::remove_all(directory_);
    }

  private:
    int listener_;
    std::string directory_;
    bool reject_;
    std::atomic<bool> stopped_{false}, running_{false};
    std::atomic<bool> workload_finished_{false};
    std::thread acceptor_;
    std::vector<std::thread> clients_;
    static void reply(int client, int code, const std::string &body) {
        const auto message = "HTTP/1.1 " + std::to_string(code) +
                             " Result\r\nContent-Length: " + std::to_string(body.size()) +
                             "\r\nConnection: close\r\n\r\n" + body;
        std::size_t sent = 0;
        while (sent < message.size()) {
            const auto count =
                send(client, message.data() + sent, message.size() - sent, MSG_NOSIGNAL);
            if (count <= 0)
                break; // The timed-out start caller has deliberately disconnected.
            sent += static_cast<std::size_t>(count);
        }
    }
    void serve(int client) {
        std::string request;
        char bytes[4096];
        auto read = [&] {
            const auto count = recv(client, bytes, sizeof(bytes), 0);
            if (count <= 0)
                throw std::runtime_error("incomplete test request");
            request.append(bytes, static_cast<std::size_t>(count));
            if (request.size() > 65536)
                throw std::runtime_error("oversized test request");
        };
        while (request.find("\r\n\r\n") == std::string::npos)
            read();
        const auto header_end = request.find("\r\n\r\n") + 4;
        const auto length_at = request.find("Content-Length: ");
        const auto length =
            length_at == std::string::npos
                ? 0
                : std::stoul(request.substr(length_at + std::strlen("Content-Length: ")));
        while (request.size() < header_end + length)
            read();
        const auto first_space = request.find(' '),
                   second_space = request.find(' ', first_space + 1);
        const auto method = request.substr(0, first_space);
        const auto path = request.substr(first_space + 1, second_space - first_space - 1);
        if (path == "/v1.45/volumes/create") {
            const auto body = nlohmann::json::parse(request.substr(header_end, length));
            reply(client, 201,
                  nlohmann::json{{"Options", body.at("DriverOpts")}, {"Labels", body.at("Labels")}}
                      .dump());
        } else if (path.starts_with("/v1.45/images/")) {
            reply(client, 200, R"({"Id":"reviewed-image"})");
        } else if (path == "/v1.45/containers/create?name=strata-keeper-attempt") {
            reply(client, 201, R"({"Id":"keeper"})");
        } else if (path == "/v1.45/containers/keeper/start") {
            ++starts;
            if (reject_) {
                reply(client, 500, R"({"message":"explicit start failure"})");
            } else {
                std::this_thread::sleep_for(std::chrono::milliseconds(3500));
                while (!inspected_created && !stopped_)
                    std::this_thread::sleep_for(std::chrono::milliseconds(10));
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                running_ = true;
                reply(client, 204, "");
            }
        } else if (path == "/v1.45/containers/keeper/json" ||
                   path == "/v1.45/containers/strata-keeper-attempt/json") {
            ++inspections;
            const bool running = running_;
            if (!running)
                inspected_created = true;
            reply(
                client, 200,
                nlohmann::json{
                    {"State", {{"Running", running}, {"Status", running ? "running" : "created"}}}}
                    .dump());
        } else if (path.starts_with("/v1.45/containers/keeper/logs")) {
            reply(client, 200, "storage keeper ready");
        } else if (path == "/v1.45/containers/create?name=strata-attempt") {
            ++workloads;
            reply(client, 201, R"({"Id":"workload"})");
        } else if (path == "/v1.45/containers/workload/start") {
            ++workload_starts;
            if (reject_) {
                reply(client, 500, R"({"message":"explicit workload start failure"})");
            } else {
                std::this_thread::sleep_for(std::chrono::milliseconds(3500));
                while (!workload_inspected_created && !stopped_)
                    std::this_thread::sleep_for(std::chrono::milliseconds(10));
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                workload_finished_ = true;
                reply(client, 204, "");
            }
        } else if (path == "/v1.45/containers/workload/json") {
            const bool finished = workload_finished_;
            if (!finished)
                workload_inspected_created = true;
            reply(client, 200,
                  nlohmann::json{{"State",
                                  {{"Running", false},
                                   {"ExitCode", 0},
                                   {"Status", finished ? "exited" : "created"}}}}
                      .dump());
        } else if (method == "DELETE") {
            if (path.starts_with("/v1.45/containers/keeper"))
                ++deletes;
            reply(client, 204, "");
        } else if (path.starts_with("/v1.45/containers/strata-keeper-attempt/kill")) {
            reply(client, 204, "");
        } else {
            reply(client, 404, "unexpected test request");
        }
    }
};

int main() {
    assert(curl_global_init(CURL_GLOBAL_DEFAULT) == CURLE_OK);
    {
        Peer peer;
        strata::Docker docker(peer.socket_path);
        int progress = 0;
        docker.configure_progress([&] { ++progress; });
        assert(docker.create("attempt", "worker", "image", {"true"}, 1, 64) == "workload");
        assert(peer.starts == 1 && peer.workloads == 1 && peer.deletes == 0);
        assert(peer.inspected_created && peer.inspections > 1 && progress > 4);
        docker.start("workload", [&] { ++progress; });
        assert(peer.workload_starts == 1 && peer.workload_inspected_created);
    }
    {
        Peer peer(true);
        strata::Docker docker(peer.socket_path);
        try {
            docker.create("attempt", "worker", "image", {"true"}, 1, 64);
            assert(false);
        } catch (const strata::DockerError &error) {
            assert(error.code == 500);
        }
        assert(peer.starts == 1 && peer.workloads == 0 && peer.deletes == 1);
        try {
            docker.start("workload", [] {});
            assert(false);
        } catch (const strata::DockerError &error) {
            assert(error.code == 500);
        }
        assert(peer.workload_starts == 1 && !peer.workload_inspected_created);
    }
    {
        Peer peer;
        strata::Docker docker(peer.socket_path);
        docker.configure_progress([&] {
            if (peer.inspected_created)
                throw std::runtime_error("assignment fenced");
        });
        try {
            docker.create("attempt", "worker", "image", {"true"}, 1, 64);
            assert(false);
        } catch (const std::runtime_error &error) {
            assert(std::string(error.what()) == "assignment fenced");
        }
        assert(peer.starts == 1 && peer.workloads == 0 && peer.deletes == 1);
    }
    {
        Peer peer;
        strata::Docker docker(peer.socket_path);
        try {
            docker.start("workload", [&] {
                if (peer.workload_inspected_created)
                    throw std::runtime_error("workload fenced");
            });
            assert(false);
        } catch (const std::runtime_error &error) {
            assert(std::string(error.what()) == "workload fenced");
        }
        assert(peer.workload_starts == 1 && peer.workloads == 0);
    }
    curl_global_cleanup();
}
