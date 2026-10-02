#include "lease_tracker.hpp"
#include <stdexcept>

int main() {
    using Clock = strata::LeaseTracker::Clock;
    const auto t = Clock::now();
    strata::LeaseTracker lease(30, t);
    if (lease.expired(t + std::chrono::seconds(29)))
        throw std::runtime_error("early expiry");
    if (!lease.expired(t + std::chrono::seconds(30)))
        throw std::runtime_error("boundary expiry");
    lease.renew(10, t);
    if (!lease.expired(t + std::chrono::seconds(11)))
        throw std::runtime_error("network latency ignored");
    strata::LeaseTracker zero(-1, t);
    if (!zero.expired(t))
        throw std::runtime_error("negative TTL");
}
