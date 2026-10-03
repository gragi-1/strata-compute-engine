#pragma once
#include <cstdio>
#include <memory>

namespace strata {
struct FileCloser {
    void operator()(std::FILE *file) const noexcept {
        if (file)
            std::fclose(file);
    }
};
using File = std::unique_ptr<std::FILE, FileCloser>;
} // namespace strata
