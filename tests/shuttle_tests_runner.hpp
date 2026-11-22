#pragma once

// Per-suite result printing for the tests (belongs to tests/ only; not part of
// the public headers).
//
//   void shuttle_tests_codec() {          // called by tests/shuttle_tests.cpp
//     run_case(__func__, "case1", [&] { ...case body... });
//     ...
//     report(__func__);   // final summary and counter reset
//   }
//
// The suite files carry no WEAVE_TEST of their own: tests/shuttle_tests.cpp owns the only
// registration and calls the suites in order, so `__func__` (the suite name printed in every
// line) is exactly the function name defined here.
//
// Same style as ../weave/tests/case_runner.hpp (the two projects evolve
// independently, so each keeps its own copy).
//
// The case body returning normally → [PASS] is printed; throwing (by
// convention throw std::logic_error("case1: ...")) → [FAIL] is printed and the
// exception is rethrown as-is, so the backend reporting process judges the
// whole test as FAILED.

#include <cstdio>
#include <exception>
#include <stdexcept>

namespace shuttle_test {

inline int& passed_count() {
  static int n = 0;
  return n;
}

inline int& failed_count() {
  static int n = 0;
  return n;
}

// Assertion outlet: throws when unmet; run_case prints and counts uniformly.
inline void expect(bool cond, const char* what) {
  if (!cond) throw std::logic_error(what);
}

// Assertion with an integer value (carries the actual value into the failure
// message for easier diagnosis)
inline void expect_eq(long long actual, long long wanted, const char* what) {
  if (actual != wanted) {
    char buf[256];
    std::snprintf(buf, sizeof buf, "%s (actual=%lld, wanted=%lld)", what, actual, wanted);
    throw std::logic_error(buf);
  }
}

template <typename F>
void run_case(const char* test, const char* name, F&& body) {
  try {
    body();
  } catch (const std::exception& e) {
    ++failed_count();
    std::printf("[FAIL] %s::%s: %s\n", test, name, e.what());
    std::fflush(stdout);
    throw;
  } catch (...) {
    ++failed_count();
    std::printf("[FAIL] %s::%s: non-std exception\n", test, name);
    std::fflush(stdout);
    throw;
  }
  ++passed_count();
  std::printf("[PASS] %s::%s\n", test, name);
  std::fflush(stdout);
}

// Summarize and reset (tests run serially, so the counters are exactly this
// test's pass/fail counts).
inline void report(const char* test) {
  std::printf("== %s: %d passed, %d failed ==\n", test, passed_count(), failed_count());
  std::fflush(stdout);
  passed_count() = 0;
  failed_count() = 0;
}

}  // namespace shuttle_test
