// svdpi 后端：`detail::backend` 的实现（broker 认识的唯一接口面）。
//
// 帧的载荷不走 DPI 参数：C++ 调用 export 前把当前帧字节流放进进程级上下文
// （FrameContext），SV 侧在 export 体内经 shuttle_c_frame_len / _qword 标量 import
// 分块拉取。DPI-C 导出面（shuttle_c_*）在 dpi.cpp。

#include <cstdint>
#include <string>
#include <vector>

#include <svdpi.h>

#include "internal/backend.hpp"
#include "svdpi_internal.hpp"

// ── SV 侧 export 的 C 原型（定义在 shuttle_pkg.sv）────────────────────────
extern "C" {
void shuttle_sv_dispatch(int sv_sid, const char* topic, const char* type_tag,
                         long long seq, long long time_fs);
void shuttle_sv_dispatch_nb(int sv_sid, const char* topic, const char* type_tag,
                            long long seq, long long time_fs);
long long shuttle_sv_time_fs();
}

namespace shuttle::detail {
namespace backend {

namespace {

// 本次 dispatch 期间 SV 注册表是否回告了"目标已退订"（见 dpi.cpp 的
// shuttle_c_sub_retired）。单活跃进程保证无需原子操作。
bool g_retired_hit = false;

}  // namespace

bool available() { return true; }

void dispatch(int handle, const std::string& topic, const std::string& type_tag,
              std::uint64_t seq, std::int64_t time_fs, const std::vector<std::byte>& payload) {
  SvScopeGuard scope;
  FrameContext ctx(payload);
  g_retired_hit = false;
  shuttle_sv_dispatch(handle, topic.c_str(), type_tag.c_str(), static_cast<long long>(seq),
                      static_cast<long long>(time_fs));
}

void dispatch_nb(int handle, const std::string& topic, const std::string& type_tag,
                 std::uint64_t seq, std::int64_t time_fs,
                 const std::vector<std::byte>& payload) {
  SvScopeGuard scope;
  FrameContext ctx(payload);
  g_retired_hit = false;
  shuttle_sv_dispatch_nb(handle, topic.c_str(), type_tag.c_str(), static_cast<long long>(seq),
                         static_cast<long long>(time_fs));
}

void note_retired_delivery() { g_retired_hit = true; }

bool take_retired_delivery() {
  const bool hit = g_retired_hit;
  g_retired_hit = false;
  return hit;
}

std::int64_t now_fs() {
  SvScopeGuard scope;
  return static_cast<std::int64_t>(shuttle_sv_time_fs());
}

int frame_len() { return g_frame ? static_cast<int>(g_frame->size()) : 0; }

std::uint64_t frame_qword(int index) {
  if (!g_frame) return 0;
  std::uint64_t v = 0;
  for (int k = 0; k < 8; ++k) {
    const std::size_t i = static_cast<std::size_t>(index) * 8 + static_cast<std::size_t>(k);
    if (i < g_frame->size())
      v |= static_cast<std::uint64_t>(static_cast<std::uint8_t>((*g_frame)[i])) << (8 * k);
  }
  return v;
}

}  // namespace backend
}  // namespace shuttle::detail
