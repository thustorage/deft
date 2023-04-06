#pragma once

#include <cstdint>
#include <cstdio>
#include <chrono>
// #include <time.h>

class Timer {
 public:
  Timer() = default;

  void begin() { start_time_ = std::chrono::steady_clock::now(); }

  // nano seconds
  uint64_t end() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now() - start_time_)
        .count();
  }

  uint64_t end(uint64_t loop) {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now() - start_time_)
               .count() /
           loop / 1000;
  }

  // micro seconds
  uint64_t end_us() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now() - start_time_)
               .count() /
           1000;
  }

  uint64_t end_us(uint64_t loop) {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now() - start_time_)
               .count() /
           loop / 1000;
  }

  static void sleep(uint64_t sleep_ns) {
    auto start = std::chrono::steady_clock::now();

    while (true) {
      uint64_t cur = std::chrono::duration_cast<std::chrono::nanoseconds>(
                         std::chrono::steady_clock::now() - start)
                         .count();
      if (cur >= sleep_ns) {
        return;
      }
    }
  }

  void end_print(uint64_t loop = 1) {
    uint64_t ns = end(loop);
    if (ns < 1000) {
      printf("%ldns per loop\n", ns);
    } else {
      printf("%lfus per loop\n", ns * 1.0 / 1000);
    }
  }

 private:
  std::chrono::steady_clock::time_point start_time_;
};
