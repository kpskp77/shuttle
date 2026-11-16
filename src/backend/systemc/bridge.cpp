// systemc 后端：`detail::backend` 的退化实现（无外部订阅者通道，broker 只剩纯 C++ 路径）。
//
// now_fs() 用 sc_time_stamp 换算仿真时间（飞秒）；精度受 double 表示限制，
// 对 Meta.time_fs 这类诊断时间戳足够，不建议用于计时逻辑。

#include <cmath>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

#include <systemc.h>

#include "internal/backend.hpp"

namespace shuttle {
namespace detail {
namespace backend {

bool available() { return false; }

void dispatch(int, const std::string&, const std::string&, std::uint64_t, std::int64_t,
              const std::vector<std::byte>&) {
  throw std::logic_error("shuttle: systemc 后端无 SV 订阅者通道");
}

void dispatch_nb(int, const std::string&, const std::string&, std::uint64_t, std::int64_t,
                 const std::vector<std::byte>&) {
  throw std::logic_error("shuttle: systemc 后端无 SV 订阅者通道");
}

std::int64_t now_fs() {
  const double res_fs = sc_get_time_resolution().to_seconds() * 1e15;
  return static_cast<std::int64_t>(std::llround(sc_time_stamp().value() * res_fs));
}

// systemc 后端没有 SV 注册表，永远不会报告"已退订的在途投递"。
void note_retired_delivery() {}
bool take_retired_delivery() { return false; }

}  // namespace backend
}  // namespace detail
}  // namespace shuttle
