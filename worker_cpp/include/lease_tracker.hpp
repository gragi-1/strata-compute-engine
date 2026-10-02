#pragma once
#include <algorithm>
#include <chrono>

namespace strata {
class LeaseTracker {
  public:
    using Clock = std::chrono::steady_clock;
    explicit LeaseTracker(double seconds, Clock::time_point sent = Clock::now()) {
        renew(seconds, sent);
    }
    void renew(double seconds, Clock::time_point sent) {
        deadline_ = sent + std::chrono::duration_cast<Clock::duration>(
                               std::chrono::duration<double>(std::max(0.0, seconds)));
    }
    bool expired(Clock::time_point now = Clock::now()) const { return now >= deadline_; }

  private:
    Clock::time_point deadline_;
};
} // namespace strata
