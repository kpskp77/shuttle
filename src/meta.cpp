// Backend-agnostic implementation of include/shuttle/meta.hpp.
//
// now_fs() is the sole source of Meta.time_fs. It is not a backend entry: the
// clock belongs to weave (`weave::now()`, the port's only non-suspending
// primitive), so "what time is it" has exactly one answer in every backend and
// `Meta::time_fs` and `weave::now()` can never disagree.
//
// It belongs to no handle and produces no handle, so it is not merged into
// either the publish or the subscribe TU.

#include <cstdint>
#include <shuttle/meta.hpp>
#include <weave/time.hpp>

namespace shuttle {

std::int64_t now_fs() {
  return weave::now().count();
}

}  // namespace shuttle
