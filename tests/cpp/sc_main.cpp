// systemc 后端的测试入口：在真实 SystemC 内核上运行 tests/cpp 下注册的全部测试。
// svdpi 后端不需要本文件——测试由仿真器内的 weave_pkg::weave_run 启动，
// 见 tests/sv/shuttle_tests.sv。

#include <weave/sc.hpp>

// xmake-repo 的 SystemC 构建以 C 链接查找 sc_main，故显式 extern "C"。
extern "C" __attribute__((visibility("default"))) int sc_main(int, char**);

extern "C" __attribute__((visibility("default"))) int sc_main(int, char**) {
  return weave::sc::run();
}
