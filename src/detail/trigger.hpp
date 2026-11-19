#pragma once

// Internal construction channel for Trigger: used only under src/, not
// installed. The path detail/trigger.hpp mirrors the public header
// shuttle/trigger.hpp, but the content is strictly limited to the internal
// friend channel.
//
// The public factories are shuttle::after() / after_all() / after_any();
// from_fn here lets the bridge layer (svdpi assembling SHUTTLE_TRIG_PUB /
// _EDGE) and after_any() construct a Trigger directly from any
// std::function.
// Trigger's constructor is private, so entry goes through the named friend
// detail::TriggerAccess.

#include <functional>
#include <shuttle/trigger.hpp>
#include <utility>

namespace shuttle::detail {

struct TriggerAccess {
  static Trigger from_fn(std::function<void()> fn) { return Trigger(std::move(fn)); }
};

}  // namespace shuttle::detail
