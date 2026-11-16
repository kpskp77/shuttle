// include/shuttle/meta.hpp 的后端无关实现。
//
// now_fs() 是 Meta.time_fs 的唯一来源，只做“转发给当前后端”，因此不并入发布或订阅的
// 任一 TU：它不属于句柄，也不产生句柄。

#include <cstdint>
#include <shuttle/meta.hpp>

#include "backend/backend.hpp"

namespace shuttle {

std::int64_t now_fs() {
  return detail::backend::now_fs();
}

}  // namespace shuttle
