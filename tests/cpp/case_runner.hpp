#pragma once

// 测试侧的逐用例结果打印（只属于 tests/，不进公开头文件）。
//
//   WEAVE_TEST(codec_tests) {
//     run_case(__func__, "case1", [&] { ...用例体... });
//     ...
//     report(__func__);   // 末尾汇总并清零计数
//   }
//
// 用例体正常返回 → 打印 [PASS]；抛出（约定 throw std::logic_error("case1: ...")）→ 打印
// [FAIL] 后原样 rethrow，让后端报告进程把整个测试判为 FAILED。
//
// 与 ../weave/tests/case_runner.hpp 同一套风格（两个项目独立演进，故各自维护一份）。

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

// 断言出口：不满足即抛，由 run_case 统一打印/计数。
inline void expect(bool cond, const char* what) {
  if (!cond) throw std::logic_error(what);
}

// 带整数值的断言（把实际值带进失败信息，便于定位）
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

// 汇总并清零（测试串行执行，计数恰为本次测试的通过/失败数）。
inline void report(const char* test) {
  std::printf("== %s: %d passed, %d failed ==\n", test, passed_count(), failed_count());
  std::fflush(stdout);
  passed_count() = 0;
  failed_count() = 0;
}

}  // namespace shuttle_test
