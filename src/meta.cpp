// Backend-agnostic implementation of include/shuttle/meta.hpp.
//
// now_fs() is the sole source of Meta.time_fs; it only "forwards to the
// current backend", so it is not merged into either the publish or subscribe
// TU: it belongs to no handle and produces no handle.

#include <cstdint>
#include <shuttle/meta.hpp>

#include "backend/backend.hpp"

namespace shuttle {

std::int64_t now_fs() {
  return detail::backend::now_fs();
}

}  // namespace shuttle
