#pragma once

// svdpi 后端内部头：只被 `src/backend/svdpi/` 的两个 TU 包含，不对外。
//
// 这里放的是"svdpi 这个后端自己怎么实现"的东西，broker 一概看不见：
//   - 包级 export 的作用域修正（SvScopeGuard）；
//   - C++→SV 的帧字节拉取通道（FrameContext + frame_len/frame_qword）。
//
// 协议定义见 docs/dpi_protocol.md。

#include <svdpi.h>

#include <cstddef>
#include <cstdint>
#include <vector>

// 包名可用 -DSHUTTLE_SV_PACKAGE= / -DSHUTTLE_WEAVE_SV_PACKAGE= 覆盖。
#ifndef SHUTTLE_SV_PACKAGE
#define SHUTTLE_SV_PACKAGE "shuttle_pkg"
#endif
// weave 的 SV 包名（其 export 定义于此；shuttle 在 import 入口处把作用域切到它，
// 使 C++ 侧调用 weave 原语（sleep/create_event/spawn…）时的 export 可见）。
#ifndef SHUTTLE_WEAVE_SV_PACKAGE
#define SHUTTLE_WEAVE_SV_PACKAGE "weave_pkg"
#endif

namespace shuttle {
namespace detail {
namespace backend {

// 按包名取作用域（带缓存；仿真线程单活跃，无需加锁）。
inline svScope scope_for(const char* pkg) {
  static const char* cached_pkg = nullptr;
  static svScope cached = nullptr;
  if (cached_pkg != pkg) {
    cached = svGetScopeFromName(pkg);
    cached_pkg = pkg;
  }
  return cached;
}

// ── 包级 export 的作用域修正 ────────────────────────────────────────────────
// VCS 2018.09 实测（../dpi_probe 最小复现）：在同一个 SV 进程里调用过返回 string
// 的 import（shuttle_c_last_error）之后，再调用包级 export 会报
//   Error-[DPI-DXFNF] DPI export function not found
// 原因是 import 调用改变了当前 svScope，导致 export 在当前作用域内不可见。
// 因此在每次进入 export 前显式把作用域切到 shuttle 的 SV 包，返回后还原，
// 以免影响其它库（如 weave_pkg）的 export 解析。
class SvScopeGuard {
public:
  explicit SvScopeGuard(const char* pkg = SHUTTLE_SV_PACKAGE) : prev_(svGetScope()) {
    if (svScope s = scope_for(pkg)) svSetScope(s);
  }
  ~SvScopeGuard() {
    if (prev_) svSetScope(prev_);
  }
  SvScopeGuard(const SvScopeGuard&) = delete;
  SvScopeGuard& operator=(const SvScopeGuard&) = delete;

private:
  svScope prev_;
};

// 当前待拉取帧的字节流：C++ 调 export 前设置，SV 在 export 体内经
// `shuttle_c_frame_len` / `shuttle_c_frame_qword` 分块拉取。线程局部即可见，
// 因为"任一时刻只有一个进程执行 C++ 侧记账代码"（AGENTS.md 不变量 1）。
inline thread_local const std::vector<std::byte>* g_frame = nullptr;

class FrameContext {
public:
  explicit FrameContext(const std::vector<std::byte>& bytes) : prev_(g_frame) {
    g_frame = &bytes;
  }
  ~FrameContext() { g_frame = prev_; }
  FrameContext(const FrameContext&) = delete;
  FrameContext& operator=(const FrameContext&) = delete;

private:
  const std::vector<std::byte>* prev_;
};

// 帧字节流的分块视图（SV 侧只能拉标量，所以按 8 字节小端分块）。
int frame_len();
std::uint64_t frame_qword(int index);

}  // namespace backend
}  // namespace detail
}  // namespace shuttle
