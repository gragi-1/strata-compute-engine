#pragma once
#include <cstdio>
#include <functional>
#include <nlohmann/json.hpp>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace strata {
struct OutputError : std::runtime_error {
    using std::runtime_error::runtime_error;
};
struct DockerError : std::runtime_error {
    long code;
    DockerError(long status, const std::string &message)
        : std::runtime_error(message), code(status) {}
};
class Docker {
  public:
    explicit Docker(std::string socket) : socket_(std::move(socket)) {}
    void set_cluster(const std::string &cluster) { cluster_ = cluster; }
    void configure_storage(std::string image, unsigned long long bytes, unsigned long long inodes);
    void configure_progress(std::function<void()> progress) { progress_ = std::move(progress); }
    void renew_storage(const std::string &attempt) const;
    bool storage_alive(const std::string &attempt) const;
    std::string request(const std::string &method, const std::string &path,
                        const std::string &body = "", std::size_t limit = 17 * 1024 * 1024,
                        long timeout = 3) const;
    std::string create(const std::string &attempt, const std::string &worker,
                       const std::string &image, const std::vector<std::string> &command,
                       double cpu, long memory, bool inputs = false,
                       const std::vector<std::string> &gpu_ids = {}, double lease_seconds = 30,
                       int grace = 5, const std::string &runtime_context = "") const;
    nlohmann::json discover_gpus(const std::string &image) const;
    void start(const std::string &id, const std::function<void()> &progress) const;
    void stage(const std::string &attempt, const std::string &worker, const std::string &image,
               const std::string &alias, const std::string &name, FILE *input, long size,
               const std::function<void()> &progress) const;
    void stop(const std::string &id, int grace) const;
    void remove(const std::string &id, const std::string &attempt) const;
    void cleanup(const std::string &worker, int grace) const;
    std::vector<std::pair<std::string, std::string>> artifacts(const std::string &id) const;
    std::string read_runtime(const std::string &id) const;
    void write_runtime(const std::string &id, const std::string &name,
                       const std::string &content) const;

  private:
    std::string socket_;
    std::string cluster_;
    std::string keeper_image_ = "strata/control-plane:local";
    unsigned long long output_bytes_ = 64 * 1024 * 1024;
    unsigned long long output_inodes_ = 4096;
    std::function<void()> progress_ = [] {};
    std::string create_keeper(const std::string &attempt, const std::string &worker,
                              const std::string &volume, double lease_seconds) const;
    nlohmann::json labels(const std::string &worker, const std::string &attempt) const;
};
} // namespace strata
