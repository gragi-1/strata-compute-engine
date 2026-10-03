#pragma once
#include <cstdio>
#include <functional>
#include <nlohmann/json.hpp>
#include <string>
#include <utility>
#include <vector>

namespace strata {
class Docker {
  public:
    explicit Docker(std::string socket) : socket_(std::move(socket)) {}
    std::string request(const std::string &method, const std::string &path,
                        const std::string &body = "", std::size_t limit = 17 * 1024 * 1024,
                        long timeout = 3) const;
    std::string create(const std::string &attempt, const std::string &worker,
                       const std::string &image, const std::vector<std::string> &command,
                       double cpu, long memory, bool inputs = false) const;
    void stage(const std::string &attempt, const std::string &worker, const std::string &image,
               const std::string &alias, const std::string &name, FILE *input, long size,
               const std::function<void()> &progress) const;
    void stop(const std::string &id, int grace) const;
    void remove(const std::string &id, const std::string &attempt) const;
    void cleanup(const std::string &worker, int grace) const;
    std::vector<std::pair<std::string, std::string>> artifacts(const std::string &id) const;

  private:
    std::string socket_;
};
} // namespace strata
