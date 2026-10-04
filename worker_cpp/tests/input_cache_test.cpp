#include "input_cache.hpp"
#include <cassert>
#include <fstream>
#include <iostream>

int main() {
    char name[] = "/tmp/strata-cache-test-XXXXXX";
    auto directory = mkdtemp(name);
    assert(directory);
    const std::filesystem::path root(directory);
    try {
        strata::InputCache cache(root, 12, 0);
        int fills = 0;
        const std::string content = "measurements";
        auto key = strata::InputCache::key(content);
        auto fill = [&](FILE *stream) {
            ++fills;
            assert(std::fwrite(content.data(), 1, content.size(), stream) == content.size());
        };
        auto use = [&](FILE *stream) {
            char bytes[12];
            assert(std::fread(bytes, 1, 12, stream) == 12);
            assert(std::string(bytes, 12) == content);
        };
        for (int i = 0; i < 2; ++i)
            cache.with_file(key, content.size(), fill, use, [] {});
        assert(fills == 1);
        {
            std::ofstream corrupt(root / key);
            corrupt << "bad-contents";
        }
        cache.with_file(key, content.size(), fill, use, [] {});
        assert(fills == 2);
        cache.with_file(
            strata::InputCache::key("new"), 3, [](FILE *file) { std::fwrite("new", 1, 3, file); },
            [](FILE *) {}, [] {});
        assert(!std::filesystem::exists(root / key));
        bool rejected = false;
        try {
            cache.with_file(
                key, content.size(), [](FILE *file) { std::fwrite("bad", 1, 3, file); },
                [](FILE *) {}, [] {});
        } catch (const std::runtime_error &) {
            rejected = true;
        }
        assert(rejected);
        std::filesystem::remove_all(root);
        std::cout
            << "Verified cache hits, corruption recovery, eviction and failed transfer cleanup\n";
    } catch (...) {
        std::filesystem::remove_all(root);
        throw;
    }
}
