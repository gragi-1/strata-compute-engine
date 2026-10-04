#include "docker.hpp"
#include "file.hpp"
#include <algorithm>
#include <archive.h>
#include <archive_entry.h>
#include <array>
#include <chrono>
#include <cmath>
#include <curl/curl.h>
#include <exception>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <thread>

namespace strata {
namespace {
struct Buffer {
    std::string data;
    std::size_t limit;
    bool exceeded = false;
};
std::size_t receive(char *ptr, std::size_t size, std::size_t count, void *user) {
    auto &buffer = *static_cast<Buffer *>(user);
    const auto bytes = size * count;
    if (bytes > buffer.limit - buffer.data.size()) {
        buffer.exceeded = true;
        return 0;
    }
    buffer.data.append(ptr, bytes);
    return bytes;
}
} // namespace
std::string Docker::request(const std::string &method, const std::string &path,
                            const std::string &body, std::size_t limit, long timeout) const {
    std::unique_ptr<CURL, decltype(&curl_easy_cleanup)> handle(curl_easy_init(), curl_easy_cleanup);
    if (!handle)
        throw std::runtime_error("curl init failed");
    const auto url = "http://localhost/v1.45" + path;
    auto *raw_headers = curl_slist_append(nullptr, path.find("/archive?") != std::string::npos
                                                       ? "Content-Type: application/x-tar"
                                                       : "Content-Type: application/json");
    std::unique_ptr<curl_slist, decltype(&curl_slist_free_all)> headers(raw_headers,
                                                                        curl_slist_free_all);
    Buffer buffer{{}, limit};
    curl_easy_setopt(handle.get(), CURLOPT_UNIX_SOCKET_PATH, socket_.c_str());
    curl_easy_setopt(handle.get(), CURLOPT_URL, url.c_str());
    curl_easy_setopt(handle.get(), CURLOPT_PROXY, "");
    curl_easy_setopt(handle.get(), CURLOPT_PROTOCOLS_STR, "http");
    curl_easy_setopt(handle.get(), CURLOPT_REDIR_PROTOCOLS_STR, "http");
    curl_easy_setopt(handle.get(), CURLOPT_CUSTOMREQUEST, method.c_str());
    curl_easy_setopt(handle.get(), CURLOPT_HTTPHEADER, headers.get());
    if (method == "POST" || method == "PUT") {
        curl_easy_setopt(handle.get(), CURLOPT_POSTFIELDS, body.c_str());
        curl_easy_setopt(handle.get(), CURLOPT_POSTFIELDSIZE, static_cast<long>(body.size()));
    }
    curl_easy_setopt(handle.get(), CURLOPT_TIMEOUT, timeout);
    curl_easy_setopt(handle.get(), CURLOPT_NOSIGNAL, 1L);
    curl_easy_setopt(handle.get(), CURLOPT_WRITEFUNCTION, receive);
    curl_easy_setopt(handle.get(), CURLOPT_WRITEDATA, &buffer);
    const auto result = curl_easy_perform(handle.get());
    if (buffer.exceeded)
        throw OutputError("Docker response exceeds transfer limit");
    long code = 0;
    curl_easy_getinfo(handle.get(), CURLINFO_RESPONSE_CODE, &code);
    if (result == CURLE_OK && code == 404 && method == "DELETE")
        return "";
    if (result != CURLE_OK || code >= 400) {
        throw DockerError(code, "Docker " + method + " " + path + " failed: " +
                                    std::to_string(code) + " " + curl_easy_strerror(result));
    }
    return buffer.data;
}
nlohmann::json Docker::labels(const std::string &worker, const std::string &attempt) const {
    nlohmann::json result = {{"strata.worker", worker}, {"strata.attempt", attempt}};
    if (!cluster_.empty())
        result["strata.cluster"] = cluster_;
    return result;
}
std::string Docker::create(const std::string &attempt, const std::string &worker,
                           const std::string &image, const std::vector<std::string> &command,
                           double cpu, long memory, bool inputs,
                           const std::vector<std::string> &gpu_ids, double lease_seconds, int grace,
                           const std::string &runtime_context) const {
    progress_();
    const auto volume = "strata-output-" + attempt;
    const nlohmann::json options = {
        {"type", "tmpfs"},
        {"device", "tmpfs"},
        {"o", "rw,nosuid,nodev,noexec,noswap,uid=65534,gid=65534,mode=0700,size=" +
                  std::to_string(output_bytes_) + ",nr_inodes=" + std::to_string(output_inodes_)}};
    const auto allocated =
        nlohmann::json::parse(request("POST", "/volumes/create",
                                      nlohmann::json{{"Name", volume},
                                                     {"Driver", "local"},
                                                     {"DriverOpts", options},
                                                     {"Labels", labels(worker, attempt)}}
                                          .dump()));
    if (allocated.value("Options", nlohmann::json::object()) != options ||
        allocated.value("Labels", nlohmann::json::object()) != labels(worker, attempt))
        throw std::runtime_error("output volume ownership or quota does not match the assignment");
    std::string keeper;
    try {
        keeper = create_keeper(attempt, worker, volume, std::max(5.0, lease_seconds) + grace + 3);
        nlohmann::json host_config = {
            {"NanoCpus", static_cast<long long>(cpu * 1e9)},
            {"Memory", memory * 1024 * 1024},
            {"MemorySwap", memory * 1024 * 1024},
            {"NetworkMode", "none"},
            {"ReadonlyRootfs", true},
            {"PidsLimit", 128},
            {"Init", true},
            {"CapDrop", {"ALL"}},
            {"SecurityOpt", {"no-new-privileges:true"}},
            {"Tmpfs", {{"/tmp", "rw,nosuid,nodev,size=64m"}}},
            {"Mounts", {{{"Type", "volume"}, {"Source", volume}, {"Target", "/output"}}}},
            {"LogConfig",
             {{"Type", "json-file"}, {"Config", {{"max-size", "1m"}, {"max-file", "1"}}}}}};
        if (inputs) {
            const auto input_volume = "strata-input-" + attempt;
            request(
                "POST", "/volumes/create",
                nlohmann::json{{"Name", input_volume}, {"Labels", labels(worker, attempt)}}.dump());
            host_config["Mounts"].push_back({{"Type", "volume"},
                                             {"Source", input_volume},
                                             {"Target", "/inputs"},
                                             {"ReadOnly", true}});
        }
        std::string visible;
        for (const auto &device : gpu_ids) {
            if (!visible.empty())
                visible += ",";
            visible += device;
        }
        if (!gpu_ids.empty())
            host_config["DeviceRequests"] =
                nlohmann::json::array({{{"DeviceIDs", gpu_ids}, {"Capabilities", {{"gpu"}}}}});
        const nlohmann::json body = {
            {"Image", image},
            {"Cmd", command},
            {"User", "65534:65534"},
            {"Env",
             {"NVIDIA_VISIBLE_DEVICES=" + (visible.empty() ? "void" : visible),
              "NVIDIA_DRIVER_CAPABILITIES=compute,utility",
              "STRATA_RUNTIME_CONTEXT=" + runtime_context}},
            {"Labels", labels(worker, attempt)},
            {"HostConfig", host_config}};
        return nlohmann::json::parse(
                   request("POST", "/containers/create?name=strata-" + attempt, body.dump()))
            .at("Id")
            .get<std::string>();
    } catch (...) {
        const auto failure = std::current_exception();
        if (!keeper.empty()) {
            try {
                request("DELETE", "/containers/" + keeper + "?force=true");
            } catch (...) {
            }
        }
        try {
            request("DELETE", "/volumes/" + volume);
        } catch (...) {
        }
        try {
            request("DELETE", "/volumes/strata-input-" + attempt);
        } catch (...) {
        }
        std::rethrow_exception(failure);
    }
}
void Docker::configure_storage(std::string image, unsigned long long bytes,
                               unsigned long long inodes) {
    if (image.empty() || image.size() > 256 || image.find_first_of("?#") != std::string::npos ||
        bytes < 1024 * 1024 || bytes > 16ULL * 1024 * 1024 * 1024 || inodes < 64 ||
        inodes > 1048576)
        throw std::runtime_error("invalid bounded output configuration");
    keeper_image_ = std::move(image);
    output_bytes_ = bytes;
    output_inodes_ = inodes;
}
std::string Docker::create_keeper(const std::string &attempt, const std::string &worker,
                                  const std::string &volume, double lease_seconds) const {
    if (!std::isfinite(lease_seconds) || lease_seconds <= 0)
        throw std::runtime_error("invalid output retention lease");
    progress_();
    const auto image = nlohmann::json::parse(request("GET", "/images/" + keeper_image_ + "/json"))
                           .at("Id")
                           .get<std::string>();
    auto owner = labels(worker, attempt);
    owner["strata.role"] = "output-keeper";
    progress_();
    const nlohmann::json body = {
        {"Image", image},
        {"User", "65534:65534"},
        {"Entrypoint", {"python"}},
        {"Cmd", {"-m", "worker.storage_keeper", std::to_string(std::min(604800.0, lease_seconds))}},
        {"Labels", owner},
        {"HostConfig",
         {{"NetworkMode", "none"},
          {"ReadonlyRootfs", true},
          {"Memory", 32 * 1024 * 1024},
          {"MemorySwap", 32 * 1024 * 1024},
          {"NanoCpus", 10000000},
          {"PidsLimit", 8},
          {"CapDrop", {"ALL"}},
          {"SecurityOpt", {"no-new-privileges:true"}},
          {"Mounts",
           {{{"Type", "volume"}, {"Source", volume}, {"Target", "/retained"}, {"ReadOnly", true}}}},
          {"LogConfig",
           {{"Type", "json-file"}, {"Config", {{"max-size", "4k"}, {"max-file", "1"}}}}}}}};
    const auto id =
        nlohmann::json::parse(
            request("POST", "/containers/create?name=strata-keeper-" + attempt, body.dump()))
            .at("Id")
            .get<std::string>();
    try {
        std::exception_ptr start_failure;
        try {
            progress_();
            request("POST", "/containers/" + id + "/start");
        } catch (const DockerError &error) {
            if (error.code != 0)
                throw;
            // Docker may still be starting this exact ID after a lost response.
            start_failure = std::current_exception();
        }
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (std::chrono::steady_clock::now() < deadline) {
            progress_();
            try {
                const auto state =
                    nlohmann::json::parse(request("GET", "/containers/" + id + "/json"))
                        .at("State");
                if (state.at("Running").get<bool>()) {
                    const auto logs = request(
                        "GET", "/containers/" + id + "/logs?stdout=true&stderr=false&tail=1", "",
                        4096);
                    if (logs.find("storage keeper ready") != std::string::npos) {
                        renew_storage(attempt);
                        return id;
                    }
                } else if (state.value("Status", "") != "created") {
                    break;
                }
            } catch (const DockerError &error) {
                if (error.code != 0)
                    throw;
                // Only bounded inspection is retried; never repeat the start mutation.
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        if (start_failure)
            std::rethrow_exception(start_failure);
        throw std::runtime_error("bounded output keeper did not become ready");
    } catch (...) {
        const auto failure = std::current_exception();
        try {
            request("DELETE", "/containers/" + id + "?force=true");
        } catch (...) {
        }
        std::rethrow_exception(failure);
    }
}
void Docker::renew_storage(const std::string &attempt) const {
    request("POST", "/containers/strata-keeper-" + attempt + "/kill?signal=SIGUSR1");
}
bool Docker::storage_alive(const std::string &attempt) const {
    try {
        return nlohmann::json::parse(
                   request("GET", "/containers/strata-keeper-" + attempt + "/json"))
            .at("State")
            .at("Running")
            .get<bool>();
    } catch (...) {
        return false;
    }
}
nlohmann::json Docker::discover_gpus(const std::string &image) const {
    if (image.empty())
        return nlohmann::json::array();
    const nlohmann::json body = {
        {"Image", image},
        {"User", "65534:65534"},
        {"Entrypoint", {"nvidia-smi"}},
        {"Cmd", {"--query-gpu=uuid,name,memory.total", "--format=csv,noheader,nounits"}},
        {"Labels", {{"strata.purpose", "gpu-discovery"}}},
        {"HostConfig",
         {{"NetworkMode", "none"},
          {"ReadonlyRootfs", true},
          {"Memory", 128 * 1024 * 1024},
          {"NanoCpus", 100000000},
          {"PidsLimit", 32},
          {"CapDrop", {"ALL"}},
          {"SecurityOpt", {"no-new-privileges:true"}},
          {"DeviceRequests",
           nlohmann::json::array({{{"Count", -1}, {"Capabilities", {{"gpu"}}}}})}}}};
    const auto id = nlohmann::json::parse(request("POST", "/containers/create", body.dump()))
                        .at("Id")
                        .get<std::string>();
    try {
        request("POST", "/containers/" + id + "/start");
        const auto result =
            nlohmann::json::parse(request("POST", "/containers/" + id + "/wait", "", 65536, 5));
        if (result.at("StatusCode").get<int>() != 0)
            throw std::runtime_error("GPU discovery failed");
        const auto raw = request(
            "GET", "/containers/" + id + "/logs?stdout=true&stderr=false&tail=64", "", 65536);
        std::string output;
        std::size_t offset = 0;
        while (offset + 8 <= raw.size()) {
            std::size_t size = 0;
            for (int i = 4; i < 8; ++i)
                size = (size << 8) | static_cast<unsigned char>(raw[offset + i]);
            if (size > raw.size() - offset - 8)
                throw std::runtime_error("invalid GPU inventory log");
            output.append(raw, offset + 8, size);
            offset += size + 8;
        }
        nlohmann::json devices = nlohmann::json::array();
        std::istringstream lines(output);
        std::string line;
        auto trim = [](std::string value) {
            const auto first = value.find_first_not_of(" \r\t");
            const auto last = value.find_last_not_of(" \r\t");
            return first == std::string::npos ? std::string{}
                                              : value.substr(first, last - first + 1);
        };
        while (std::getline(lines, line)) {
            if (line.empty())
                continue;
            std::istringstream row(line);
            std::string uuid, name, memory;
            if (!std::getline(row, uuid, ',') || !std::getline(row, name, ',') ||
                !std::getline(row, memory))
                throw std::runtime_error("invalid GPU inventory row");
            devices.push_back(
                {{"id", trim(uuid)}, {"name", trim(name)}, {"memory_mb", std::stol(trim(memory))}});
        }
        if (devices.empty() || devices.size() > 64)
            throw std::runtime_error("invalid GPU inventory size");
        request("DELETE", "/containers/" + id + "?force=true");
        return devices;
    } catch (...) {
        try {
            request("DELETE", "/containers/" + id + "?force=true");
        } catch (...) {
        }
        throw;
    }
}
void Docker::start(const std::string &id, const std::function<void()> &progress) const {
    progress();
    try {
        request("POST", "/containers/" + id + "/start");
    } catch (const DockerError &error) {
        if (error.code != 0)
            throw;
        const auto failure = std::current_exception();
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (std::chrono::steady_clock::now() < deadline) {
            progress();
            try {
                const auto state =
                    nlohmann::json::parse(request("GET", "/containers/" + id + "/json"))
                        .at("State");
                const auto status = state.value("Status", "");
                if (state.at("Running").get<bool>() || status == "exited" || status == "dead") {
                    progress();
                    return;
                }
                if (status != "created")
                    break;
            } catch (const DockerError &inspection_error) {
                if (inspection_error.code != 0)
                    throw;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        std::rethrow_exception(failure);
    }
    progress();
}
void Docker::stop(const std::string &id, int grace) const {
    try {
        request("POST", "/containers/" + id + "/stop?t=" + std::to_string(grace), "", 65536,
                grace + 3);
    } catch (const std::exception &) {
        request("POST", "/containers/" + id + "/kill");
    }
}
void Docker::remove(const std::string &id, const std::string &attempt) const {
    request("DELETE", "/containers/" + id + "?force=true");
    request("DELETE", "/containers/strata-keeper-" + attempt + "?force=true");
    request("DELETE", "/volumes/strata-output-" + attempt + "?force=true");
    request("DELETE", "/volumes/strata-input-" + attempt + "?force=true");
}

void Docker::stage(const std::string &attempt, const std::string &worker, const std::string &image,
                   const std::string &alias, const std::string &name, FILE *input, long size,
                   const std::function<void()> &progress) const {
    const nlohmann::json body = {{"Image", image},
                                 {"Cmd", {"true"}},
                                 {"Env", {"NVIDIA_VISIBLE_DEVICES=void"}},
                                 {"User", "65534:65534"},
                                 {"Labels", labels(worker, attempt)},
                                 {"HostConfig",
                                  {{"NetworkMode", "none"},
                                   {"Memory", 64 * 1024 * 1024},
                                   {"CapDrop", {"ALL"}},
                                   {"SecurityOpt", {"no-new-privileges:true"}},
                                   {"Mounts",
                                    {{{"Type", "volume"},
                                      {"Source", "strata-input-" + attempt},
                                      {"Target", "/staging"}}}}}}};
    const auto helper = nlohmann::json::parse(request("POST", "/containers/create", body.dump()))
                            .at("Id")
                            .get<std::string>();
    try {
        File tar(std::tmpfile());
        if (!tar)
            throw std::runtime_error("cannot create staging archive");
        std::unique_ptr<archive, decltype(&archive_write_free)> writer(archive_write_new(),
                                                                       archive_write_free);
        archive_write_set_format_pax_restricted(writer.get());
        if (archive_write_open_FILE(writer.get(), tar.get()) != ARCHIVE_OK)
            throw std::runtime_error("cannot open staging archive");
        std::unique_ptr<archive_entry, decltype(&archive_entry_free)> entry(archive_entry_new(),
                                                                            archive_entry_free);
        archive_entry_set_pathname(entry.get(), alias.c_str());
        archive_entry_set_filetype(entry.get(), AE_IFDIR);
        archive_entry_set_perm(entry.get(), 0755);
        archive_entry_set_size(entry.get(), 0);
        if (archive_write_header(writer.get(), entry.get()) != ARCHIVE_OK)
            throw std::runtime_error("cannot write input directory");
        archive_entry_clear(entry.get());
        const auto relative = alias + "/" + name;
        archive_entry_set_pathname(entry.get(), relative.c_str());
        archive_entry_set_filetype(entry.get(), AE_IFREG);
        archive_entry_set_perm(entry.get(), 0444);
        archive_entry_set_uid(entry.get(), 65534);
        archive_entry_set_gid(entry.get(), 65534);
        archive_entry_set_size(entry.get(), size);
        if (archive_write_header(writer.get(), entry.get()) != ARCHIVE_OK)
            throw std::runtime_error("cannot write input header");
        std::rewind(input);
        std::array<char, 65536> bytes{};
        while (auto n = std::fread(bytes.data(), 1, bytes.size(), input)) {
            progress();
            if (archive_write_data(writer.get(), bytes.data(), n) != static_cast<la_ssize_t>(n))
                throw std::runtime_error("cannot write input data");
        }
        archive_write_close(writer.get());
        const auto length = std::ftell(tar.get());
        std::rewind(tar.get());
        std::unique_ptr<CURL, decltype(&curl_easy_cleanup)> handle(curl_easy_init(),
                                                                   curl_easy_cleanup);
        struct Source {
            FILE *file;
            const std::function<void()> *pump;
            std::exception_ptr error;
        } source{tar.get(), &progress, {}};
        auto read = +[](char *buffer, std::size_t a, std::size_t b, void *user) -> std::size_t {
            auto &s = *static_cast<Source *>(user);
            try {
                (*s.pump)();
                return std::fread(buffer, 1, a * b, s.file);
            } catch (...) {
                s.error = std::current_exception();
                return CURL_READFUNC_ABORT;
            }
        };
        const auto url = "http://localhost/v1.45/containers/" + helper + "/archive?path=%2Fstaging";
        std::unique_ptr<curl_slist, decltype(&curl_slist_free_all)> headers(
            curl_slist_append(nullptr, "Content-Type: application/x-tar"), curl_slist_free_all);
        Buffer result{{}, 65536};
        curl_easy_setopt(handle.get(), CURLOPT_UNIX_SOCKET_PATH, socket_.c_str());
        curl_easy_setopt(handle.get(), CURLOPT_URL, url.c_str());
        curl_easy_setopt(handle.get(), CURLOPT_PROXY, "");
        curl_easy_setopt(handle.get(), CURLOPT_PROTOCOLS_STR, "http");
        curl_easy_setopt(handle.get(), CURLOPT_REDIR_PROTOCOLS_STR, "http");
        curl_easy_setopt(handle.get(), CURLOPT_UPLOAD, 1L);
        curl_easy_setopt(handle.get(), CURLOPT_READFUNCTION, read);
        curl_easy_setopt(handle.get(), CURLOPT_READDATA, &source);
        curl_easy_setopt(handle.get(), CURLOPT_INFILESIZE_LARGE, static_cast<curl_off_t>(length));
        curl_easy_setopt(handle.get(), CURLOPT_HTTPHEADER, headers.get());
        curl_easy_setopt(handle.get(), CURLOPT_TIMEOUT, 600L);
        curl_easy_setopt(handle.get(), CURLOPT_NOSIGNAL, 1L);
        curl_easy_setopt(handle.get(), CURLOPT_WRITEFUNCTION, receive);
        curl_easy_setopt(handle.get(), CURLOPT_WRITEDATA, &result);
        const auto status = curl_easy_perform(handle.get());
        if (source.error)
            std::rethrow_exception(source.error);
        long code = 0;
        curl_easy_getinfo(handle.get(), CURLINFO_RESPONSE_CODE, &code);
        if (status != CURLE_OK || code >= 400)
            throw std::runtime_error("input staging failed");
        request("DELETE", "/containers/" + helper + "?force=true");
    } catch (...) {
        try {
            request("DELETE", "/containers/" + helper + "?force=true");
        } catch (...) {
        }
        throw;
    }
}
std::string Docker::read_runtime(const std::string &id) const {
    std::string data;
    try {
        data =
            request("GET", "/containers/" + id + "/archive?path=%2Foutput%2F.strata%2Frequest.json",
                    "", 65536);
    } catch (const DockerError &error) {
        if (error.code == 404)
            return "";
        throw;
    }
    std::unique_ptr<archive, decltype(&archive_read_free)> reader(archive_read_new(),
                                                                  archive_read_free);
    archive_read_support_format_tar(reader.get());
    if (archive_read_open_memory(reader.get(), data.data(), data.size()) != ARCHIVE_OK)
        throw OutputError("invalid runtime archive");
    archive_entry *entry = nullptr;
    if (archive_read_next_header(reader.get(), &entry) != ARCHIVE_OK ||
        archive_entry_filetype(entry) != AE_IFREG || archive_entry_size(entry) < 0 ||
        archive_entry_size(entry) > 16384)
        throw OutputError("invalid runtime request file");
    std::string message(static_cast<std::size_t>(archive_entry_size(entry)), '\0');
    if (archive_read_data(reader.get(), message.data(), message.size()) !=
        static_cast<la_ssize_t>(message.size()))
        throw OutputError("truncated runtime message");
    if (archive_read_next_header(reader.get(), &entry) != ARCHIVE_EOF)
        throw OutputError("runtime archive has multiple entries");
    return message;
}
void Docker::write_runtime(const std::string &id, const std::string &name,
                           const std::string &content) const {
    if ((name != "runtime.py" && name != "reply.json") || content.size() > 16384)
        throw OutputError("invalid runtime response");
    std::array<char, 65536> buffer{};
    std::size_t used = 0;
    std::unique_ptr<archive, decltype(&archive_write_free)> writer(archive_write_new(),
                                                                   archive_write_free);
    archive_write_set_format_pax_restricted(writer.get());
    if (archive_write_open_memory(writer.get(), buffer.data(), buffer.size(), &used) != ARCHIVE_OK)
        throw OutputError("cannot construct runtime archive");
    std::unique_ptr<archive_entry, decltype(&archive_entry_free)> entry(archive_entry_new(),
                                                                        archive_entry_free);
    archive_entry_set_pathname(entry.get(), ".strata");
    archive_entry_set_filetype(entry.get(), AE_IFDIR);
    archive_entry_set_perm(entry.get(), 0700);
    archive_entry_set_uid(entry.get(), 65534);
    archive_entry_set_gid(entry.get(), 65534);
    if (archive_write_header(writer.get(), entry.get()) != ARCHIVE_OK)
        throw OutputError("cannot construct runtime directory");
    const auto path = ".strata/" + name;
    archive_entry_set_pathname(entry.get(), path.c_str());
    archive_entry_set_filetype(entry.get(), AE_IFREG);
    archive_entry_set_perm(entry.get(), 0600);
    archive_entry_set_size(entry.get(), static_cast<la_int64_t>(content.size()));
    if (archive_write_header(writer.get(), entry.get()) != ARCHIVE_OK ||
        archive_write_data(writer.get(), content.data(), content.size()) !=
            static_cast<la_ssize_t>(content.size()) ||
        archive_write_close(writer.get()) != ARCHIVE_OK)
        throw OutputError("cannot construct runtime response");
    request("PUT", "/containers/" + id + "/archive?path=%2Foutput",
            std::string(buffer.data(), used), 65536);
}
void Docker::cleanup(const std::string &worker, int grace) const {
    // Worker IDs are validated to alphanumerics, underscores and hyphens by the server.
    const auto filters = "%7B%22label%22%3A%5B%22strata.worker%3D" + worker + "%22" +
                         (cluster_.empty() ? "" : "%2C%22strata.cluster%3D" + cluster_ + "%22") +
                         "%5D%7D";
    for (const auto &c :
         nlohmann::json::parse(request("GET", "/containers/json?all=true&filters=" + filters))) {
        const auto id = c.at("Id").get<std::string>();
        stop(id, grace);
        request("DELETE", "/containers/" + id + "?force=true");
    }
    const auto volumes = nlohmann::json::parse(request("GET", "/volumes?filters=" + filters));
    if (volumes.contains("Volumes") && !volumes.at("Volumes").is_null()) {
        for (const auto &v : volumes.at("Volumes"))
            request("DELETE", "/volumes/" + v.at("Name").get<std::string>() + "?force=true");
    }
}
std::vector<std::pair<std::string, std::string>> Docker::artifacts(const std::string &id) const {
    const auto data = request("GET", "/containers/" + id + "/archive?path=%2Foutput");
    std::unique_ptr<archive, decltype(&archive_read_free)> reader(archive_read_new(),
                                                                  archive_read_free);
    archive_read_support_format_tar(reader.get());
    if (archive_read_open_memory(reader.get(), data.data(), data.size()) != ARCHIVE_OK)
        throw OutputError("invalid output archive");
    std::vector<std::pair<std::string, std::string>> files;
    archive_entry *entry = nullptr;
    while (archive_read_next_header(reader.get(), &entry) == ARCHIVE_OK) {
        std::string name = archive_entry_pathname(entry);
        if (name.starts_with("output/"))
            name.erase(0, 7);
        if (archive_entry_filetype(entry) != AE_IFREG || name.find('/') != std::string::npos) {
            archive_read_data_skip(reader.get());
            continue;
        }
        const auto size = archive_entry_size(entry);
        if (size < 0 || size > 16 * 1024 * 1024)
            throw OutputError("output file exceeds limit");
        std::string bytes(static_cast<std::size_t>(size), '\0');
        const auto read = archive_read_data(reader.get(), bytes.data(), bytes.size());
        if (read != size)
            throw OutputError("truncated output file");
        files.emplace_back(name, std::move(bytes));
    }
    return files;
}
} // namespace strata
