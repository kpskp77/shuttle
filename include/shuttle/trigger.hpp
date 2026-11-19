#pragma once

// Trigger: deferred start condition for `serial`/`concurrent` publications —
// "publish now, start the callback process only once the condition holds".
//
// Semantic essentials:
//   · an empty trigger (default-constructed) = start immediately;
//   · publish_serial / publish_concurrent lock in the subscriber set and the
//     message copy at the moment of the call (snapshot semantics); the
//     trigger only decides *when* the background callback process *starts*,
//     not *to whom* or *what*;
//   · under `publish_concurrent` all subscriber processes share the same
//     trigger (they wait on the same condition);
//   · a deferred frame does not hold its topic's serial chain while the
//     trigger is pending: it joins the chain at trigger time, so a delayed
//     `publish_serial` never blocks later frames of the same topic (delivery
//     order is therefore "join order", not "publish order" for such frames);
//   · Trigger::wait() is a blocking primitive, called by the broker's
//     dispatch task; it requires a weave task context.

#include <functional>
#include <initializer_list>
#include <vector>
#include <weave/event.hpp>
#include <weave/task.hpp>
#include <weave/time.hpp>

namespace weave::sv {
class Edge;  // include/weave/sv.hpp (only forward-declared here: plain C++ users never need it)
}  // namespace weave::sv

namespace shuttle {

namespace detail {
// Internal construction channel (defined in src/detail/trigger.hpp, not
// installed). The public factories are the after() family below.
struct TriggerAccess;
}  // namespace detail

class Trigger {
public:
  Trigger() = default;  // empty: immediate

  // Block until the condition holds (an empty trigger returns immediately).
  // Only invoked by the broker's internal dispatch paths.
  void wait() const {
    if (fn_) fn_();
  }
  [[nodiscard]] bool immediate() const { return !fn_; }

private:
  explicit Trigger(std::function<void()> fn) : fn_(std::move(fn)) {}

  std::function<void()> fn_;  // may be awaited concurrently by multiple dispatch processes

  friend Trigger after(weave::Event& ev);
  friend Trigger after(weave::Pulse& p);
  friend Trigger after(weave::sv::Edge& e);
  friend Trigger after(weave::duration d);
  friend Trigger after(const weave::Task& t);
  friend Trigger after_all(std::vector<Trigger> trigs);
  friend Trigger after_any(std::vector<Trigger> trigs);
  friend struct detail::TriggerAccess;
};

// Wait for a C++ event to be set (always a weave::Event, which is sticky). For a cross-language or
// single-shot edge use after(Pulse) / after(sv::Edge) below.
Trigger after(weave::Event& ev);

// Wait for a weave::Pulse to fire. A Pulse is edge semantics, not a latch: a fire() that lands
// before the frame's delivery process has registered its wait is lost and the frame stays pending
// until the next fire(). It is the in-process counterpart of sv::Edge (no name table, no DPI).
Trigger after(weave::Pulse& p);

// Wait for a named cross-language edge (weave::sv::Edge) to fire — the form the SV side reaches as
// `shuttle_trigger::after_edge(new("clk"))`. The Edge constructor is a find-or-insert into weave's
// own name table, so whichever side creates it first, both sides wait on the same id; no id ever
// crosses the DPI boundary (only the name does). Same edge semantics as after(Pulse).
// Works on both backends as long as *someone* fires it: Edge::fire() goes through the port, so a
// C++-owned edge needs no SystemVerilog at all.
Trigger after(weave::sv::Edge& e);

// Wait for a simulation-time delay
Trigger after(weave::duration d);

// Wait for a weave task to reach a terminal state (DONE/FAILED/CANCELLED all
// count as "done"; its exception is not rethrown: a trigger only conveys the
// fact of completion — failure handling belongs to Publication::result())
Trigger after(const weave::Task& t);

// All of them / any one of them (an empty collection counts as immediately satisfied)
Trigger after_all(std::vector<Trigger> trigs);
// Note: `after_any` with >= 2 elements spawns a racing task per sub-trigger
// and starts waiting right at **construction** (a single element is returned
// as-is), so it must be called in a weave task / simulation-process context —
// never inside a nonblocking callback.
Trigger after_any(std::vector<Trigger> trigs);
inline Trigger after_all(std::initializer_list<Trigger> trigs) {
  return after_all(std::vector<Trigger>(trigs));
}
inline Trigger after_any(std::initializer_list<Trigger> trigs) {
  return after_any(std::vector<Trigger>(trigs));
}

}  // namespace shuttle
