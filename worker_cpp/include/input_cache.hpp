#pragma once
#include "file.hpp"
#include <algorithm>
#include <chrono>
#include <fcntl.h>
#include <filesystem>
#include <functional>
#include <iomanip>
#include <openssl/evp.h>
#include <sstream>
#include <stdexcept>
#include <string>
#include <sys/file.h>
#include <sys/statvfs.h>
#include <thread>
#include <unistd.h>
#include <vector>

namespace strata {
namespace fs = std::filesystem;
class InputCache {
  public:
    InputCache(fs::path root, std::uintmax_t budget, std::uintmax_t minimum_free)
        : root_(std::move(root)), budget_(budget), minimum_free_(minimum_free) {
        if (!budget_)
            throw std::runtime_error("worker cache budget must be positive");
    }
    static std::string key(const std::string &value) {
        unsigned char hash[EVP_MAX_MD_SIZE];
        unsigned int length = 0;
        EVP_Digest(value.data(), value.size(), hash, &length, EVP_sha256(), nullptr);
        return hex(hash, length);
    }
    void with_file(const std::string &digest, std::uintmax_t size,
                   const std::function<void(FILE *)> &fill, const std::function<void(FILE *)> &use,
                   const std::function<void()> &progress) {
        if (digest.size() != 64 ||
            digest.find_first_not_of("0123456789abcdef") != std::string::npos)
            throw std::runtime_error("invalid input digest");
        if (size > budget_)
            throw std::runtime_error("input exceeds worker cache budget");
        fs::create_directories(root_);
        struct Lock {
            int fd;
            ~Lock() {
                if (fd >= 0) {
                    flock(fd, LOCK_UN);
                    close(fd);
                }
            }
        } lock{::open((root_ / ".storage.lock").c_str(), O_CREAT | O_RDWR, 0600)};
        if (lock.fd < 0)
            throw std::runtime_error("cannot open cache lock");
        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
        while (flock(lock.fd, LOCK_EX | LOCK_NB) != 0) {
            progress();
            if (std::chrono::steady_clock::now() >= deadline)
                throw std::runtime_error("input cache is busy");
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        auto target = root_ / digest;
        if (fs::is_symlink(target))
            throw std::runtime_error("symbolic cache paths are forbidden");
        File stream(std::fopen(target.c_str(), "rb"));
        bool valid =
            stream && fs::file_size(target) == size && checksum(stream.get(), progress) == digest;
        if (!valid) {
            stream.reset();
            fs::remove(target);
            evict(size);
            struct statvfs space {};
            if (statvfs(root_.c_str(), &space) != 0 ||
                static_cast<std::uintmax_t>(space.f_bavail) * space.f_frsize < size + minimum_free_)
                throw std::runtime_error("worker cache disk capacity exhausted");
            auto pattern = (root_ / "input-XXXXXX").string();
            std::vector<char> name(pattern.begin(), pattern.end());
            name.push_back('\0');
            int descriptor = mkstemp(name.data());
            if (descriptor < 0)
                throw std::runtime_error("cannot create cache staging file");
            fs::path temporary(name.data());
            try {
                File staging(fdopen(descriptor, "w+b"));
                if (!staging) {
                    close(descriptor);
                    throw std::runtime_error("cannot open cache staging file");
                }
                fill(staging.get());
                if (std::fflush(staging.get()) != 0 || fs::file_size(temporary) != size ||
                    checksum(staging.get(), progress) != digest)
                    throw std::runtime_error("input cache checksum or size mismatch");
                staging.reset();
                fs::rename(temporary, target);
            } catch (...) {
                fs::remove(temporary);
                throw;
            }
            stream.reset(std::fopen(target.c_str(), "rb"));
        }
        if (!stream)
            throw std::runtime_error("cannot open cached input");
        fs::last_write_time(target, fs::file_time_type::clock::now());
        std::rewind(stream.get());
        progress();
        use(stream.get());
    }

  private:
    fs::path root_;
    std::uintmax_t budget_, minimum_free_;
    static std::string hex(const unsigned char *hash, unsigned int size) {
        std::ostringstream out;
        for (unsigned int i = 0; i < size; ++i)
            out << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(hash[i]);
        return out.str();
    }
    static std::string checksum(FILE *file, const std::function<void()> &progress) {
        std::rewind(file);
        std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)> ctx(EVP_MD_CTX_new(),
                                                                    EVP_MD_CTX_free);
        if (!ctx || EVP_DigestInit_ex(ctx.get(), EVP_sha256(), nullptr) != 1)
            throw std::runtime_error("cannot initialize input checksum");
        unsigned char buffer[65536];
        std::size_t count;
        while ((count = std::fread(buffer, 1, sizeof(buffer), file))) {
            progress();
            EVP_DigestUpdate(ctx.get(), buffer, count);
        }
        if (std::ferror(file))
            throw std::runtime_error("cannot read cached input");
        unsigned char hash[EVP_MAX_MD_SIZE];
        unsigned int length = 0;
        EVP_DigestFinal_ex(ctx.get(), hash, &length);
        return hex(hash, length);
    }
    void evict(std::uintmax_t required) {
        std::uintmax_t used = 0;
        std::vector<fs::directory_entry> files;
        for (const auto &file : fs::directory_iterator(root_)) {
            if (file.is_symlink() || !file.is_regular_file() ||
                file.path().filename() == ".storage.lock")
                continue;
            used += file.file_size();
            const auto name = file.path().filename().string();
            if (name.size() == 64 &&
                name.find_first_not_of("0123456789abcdef") == std::string::npos)
                files.push_back(file);
        }
        std::sort(files.begin(), files.end(), [](const auto &a, const auto &b) {
            return a.last_write_time() < b.last_write_time();
        });
        for (const auto &file : files) {
            if (used + required <= budget_)
                break;
            used -= file.file_size();
            fs::remove(file.path());
        }
        if (used + required > budget_)
            throw std::runtime_error("worker cache budget exhausted");
    }
};
} // namespace strata
