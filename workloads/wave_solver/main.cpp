#include <cmath>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

int main(int argc, char **argv) {
    try {
        int nx = 2000, steps = 2000;
        std::string output;
        for (int i = 1; i < argc; i += 2) {
            if (i + 1 >= argc)
                throw std::invalid_argument("missing argument value");
            const std::string key = argv[i];
            if (key == "--nx")
                nx = std::stoi(argv[i + 1]);
            else if (key == "--steps")
                steps = std::stoi(argv[i + 1]);
            else if (key == "--output")
                output = argv[i + 1];
            else
                throw std::invalid_argument("unknown argument");
        }
        if (nx < 3 || nx > 10000000 || steps < 1)
            throw std::invalid_argument("invalid grid or steps");
        std::vector<double> previous(nx), current(nx), next(nx);
        for (int i = 0; i < nx; ++i) {
            const double x = static_cast<double>(i) / (nx - 1);
            current[i] = std::exp(-400 * (x - 0.5) * (x - 0.5));
        }
        current.front() = current.back() = 0;
        // u_tt = u_xx; CFL c*dt/dx = 0.5. Zero initial velocity needs a half-step.
        previous = current;
        for (int i = 1; i < nx - 1; ++i)
            previous[i] += 0.125 * (current[i - 1] - 2 * current[i] + current[i + 1]);
        for (int step = 0; step < steps; ++step) {
            for (int i = 1; i < nx - 1; ++i)
                next[i] = 2 * current[i] - previous[i] +
                          0.25 * (current[i - 1] - 2 * current[i] + current[i + 1]);
            previous.swap(current);
            current.swap(next);
        }
        double norm = 0;
        for (double value : current) {
            if (!std::isfinite(value))
                throw std::runtime_error("nonfinite solution");
            norm += value * value;
        }
        if (!output.empty()) {
            std::ofstream file(output);
            if (!file)
                throw std::runtime_error("cannot write output");
            file << "x,u\n";
            for (int i = 0; i < nx; ++i)
                file << static_cast<double>(i) / (nx - 1) << ',' << current[i] << '\n';
        }
        std::cout << "{\"nx\":" << nx << ",\"steps\":" << steps
                  << ",\"l2\":" << std::sqrt(norm / nx) << "}\n";
    } catch (const std::exception &error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
