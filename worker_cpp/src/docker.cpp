#include "docker.hpp"
#include <archive.h>
#include <archive_entry.h>
#include <array>
#include <curl/curl.h>
#include <exception>
#include <memory>
#include <stdexcept>

namespace strata {
namespace {
struct Buffer {
    std::string data;
    std::size_t limit;
};
std::size_t receive(char *ptr, std::size_t size, std::size_t count, void *user) {
    auto &buffer = *static_cast<Buffer *>(user);
    const auto bytes = size * count;
    if (bytes > buffer.limit - buffer.data.size())
        return 0;
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
    auto *raw_headers = curl_slist_append(nullptr, "Content-Type: application/json");
    std::unique_ptr<curl_slist, decltype(&curl_slist_free_all)> headers(raw_headers,
                                                                        curl_slist_free_all);
    Buffer buffer{{}, limit};
    curl_easy_setopt(handle.get(), CURLOPT_UNIX_SOCKET_PATH, socket_.c_str());
    curl_easy_setopt(handle.get(), CURLOPT_URL, url.c_str());
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
    long code = 0;
    curl_easy_getinfo(handle.get(), CURLINFO_RESPONSE_CODE, &code);
    if (result == CURLE_OK && code == 404 && method == "DELETE")
        return "";
    if (result != CURLE_OK || code >= 400) {
        throw std::runtime_error("Docker " + method + " " + path + " failed: " +
                                 std::to_string(code) + " " + curl_easy_strerror(result));
    }
    return buffer.data;
}
std::string Docker::create(const std::string &attempt, const std::string &worker,
                           const std::string &image, const std::vector<std::string> &command,
                           double cpu, long memory, bool inputs) const {
    const auto volume = "strata-output-" + attempt;
    request("POST", "/volumes/create",
            nlohmann::json{{"Name", volume}, {"Labels", {{"strata.worker", worker}}}}.dump());
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
            nlohmann::json{{"Name", input_volume}, {"Labels", {{"strata.worker", worker}}}}.dump());
        host_config["Mounts"].push_back({{"Type", "volume"},
                                         {"Source", input_volume},
                                         {"Target", "/inputs"},
                                         {"ReadOnly", true}});
    }
    const nlohmann::json body = {
        {"Image", image},
        {"Cmd", command},
        {"User", "65534:65534"},
        {"Labels", {{"strata.worker", worker}, {"strata.attempt", attempt}}},
        {"HostConfig", host_config}};
    return nlohmann::json::parse(
               request("POST", "/containers/create?name=strata-" + attempt, body.dump()))
        .at("Id")
        .get<std::string>();
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
    request("DELETE", "/volumes/strata-output-" + attempt + "?force=true");
    request("DELETE", "/volumes/strata-input-" + attempt + "?force=true");
}

void Docker::stage(const std::string &attempt, const std::string &worker, const std::string &image,
                   const std::string &alias, const std::string &name, FILE *input, long size,
                   const std::function<void()> &progress) const {
    const nlohmann::json body = {
        {"Image", image},
        {"Cmd", {"true"}},
        {"User", "65534:65534"},
        {"Labels", {{"strata.worker", worker}, {"strata.attempt", attempt}}},
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
        std::unique_ptr<FILE, decltype(&std::fclose)> tar(std::tmpfile(), std::fclose);
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
void Docker::cleanup(const std::string &worker, int grace) const {
    // Worker IDs are validated to alphanumerics, underscores and hyphens by the server.
    const auto filters = "%7B%22label%22%3A%5B%22strata.worker%3D" + worker + "%22%5D%7D";
    for (const auto &c :
         nlohmann::json::parse(request("GET", "/containers/json?all=true&filters=" + filters))) {
        const auto id = c.at("Id").get<std::string>();
        stop(id, grace);
        remove(id, c.at("Labels").at("strata.attempt").get<std::string>());
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
        throw std::runtime_error("invalid output archive");
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
            throw std::runtime_error("output file exceeds limit");
        std::string bytes(static_cast<std::size_t>(size), '\0');
        const auto read = archive_read_data(reader.get(), bytes.data(), bytes.size());
        if (read != size)
            throw std::runtime_error("truncated output file");
        files.emplace_back(name, std::move(bytes));
    }
    return files;
}
} // namespace strata
