// Trigger factory implementations (declarations in trigger.hpp).
//
// after_any spawns one weave task per sub-trigger at the call site (which
// must be a weave task / simulation-process context) to race them; after_all
// waits in order (equivalent to "all satisfied").

#include "detail/trigger.hpp"

#include <cstddef>
#include <memory>
#include <shuttle/trigger.hpp>
#include <utility>
#include <vector>
#include <weave/event.hpp>
#include <weave/sv.hpp>
#include <weave/task.hpp>
#include <weave/time.hpp>

#include <shuttle/detail/trace.hpp>

namespace shuttle {

Trigger after(weave::Event& ev) {
  // std::function instantiates against a const target: forward the non-const wait() via shared_ptr
  auto evp = std::make_shared<weave::Event>(ev);
  return Trigger([evp]() { evp->wait(); });
}

Trigger after(weave::Pulse& p) {
  // Same const-target constraint as above: forward through shared_ptr (a Pulse is a shared pimpl,
  // so the copy is cheap).
  auto pp = std::make_shared<weave::Pulse>(p);
  return Trigger([pp]() { pp->wait(); });
}

Trigger after(weave::sv::Edge& e) {
  // Holding the Edge (not an id) keeps the name alive for the lifetime of the trigger and goes
  // through Edge::wait(), i.e. weave's own suspend path (cancellation injection, wait graph) —
  // waiting on a raw id would bypass both.
  auto ep = std::make_shared<weave::sv::Edge>(e);
  return Trigger([ep]() { ep->wait(); });
}

Trigger after(weave::duration d) {
  return Trigger([d]() { weave::sleep(d); });
}

Trigger after(const weave::Task& t) {
  // Only conveys the fact of reaching a terminal state: FAILED/CANCELLED are
  // not rethrown (see the header comment for semantics).
  // Note: std::function's invocation path instantiates against a const target
  // (a libstdc++ implementation constraint) while await() is non-const — so
  // capture it via shared_ptr forwarding.
  auto tp = std::make_shared<weave::Task>(t);
  return Trigger([tp]() {
    try {
      tp->await();
    } catch (...) {
    }
  });
}

Trigger after_all(std::vector<Trigger> trigs) {
  return Trigger([trigs = std::move(trigs)]() {
    // wait in order; the first failure (non-cancellation) propagates out
    for (const auto& tr : trigs) tr.wait();
  });
}

Trigger after_any(std::vector<Trigger> trigs) {
  if (trigs.empty()) return Trigger();  // empty set: treated as immediately satisfied
  if (trigs.size() == 1) return trigs.front();
  struct State {
    weave::Event fired;  // set as soon as the first sub-trigger settles (level semantics are
                         // naturally idempotent)
  };
  auto st = std::make_shared<State>();
  detail::trace_trigger()->debug("after_any: racing {} sub-trigger(s)", trigs.size());
  for (auto& tr : trigs) {
    weave::create_task("shuttle-trig-any", [st, tr]() mutable {
      try {
        tr.wait();
      } catch (...) {
        // Racing semantics: cancellation/failure also counts as "this sub-trigger
        // has settled" and does not affect the other sub-triggers
      }
      detail::trace_trigger()->trace("after_any: one sub-trigger settled, releasing the race");
      st->fired.set();
    });
  }
  return detail::TriggerAccess::from_fn([st]() { st->fired.wait(); });
}

}  // namespace shuttle
