// SystemC 后端的示例入口：在真实 SystemC 内核上运行全部注册的示例（WEAVE_TEST）。
// svdpi 后端不需要本文件——示例由仿真器内的 weave_pkg::weave_run 启动。

#include <weave/sc.hpp>

// xmake-repo 的 SystemC 构建以 C 链接查找 sc_main，故显式 extern "C"。
extern "C" __attribute__((visibility("default"))) int sc_main(int, char**);

extern "C" __attribute__((visibility("default"))) int sc_main(int, char**) {
  return weave::sc::run();
}
