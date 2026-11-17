// Trigger factory implementations (declarations in trigger.hpp).
//
// after_any spawns one weave task per sub-trigger at the call site (which
// must be a weave task / simulation-process context) to race them; after_all
// waits in order (equivalent to "all satisfied").

#include "detail/trigger.hpp"

#include <memory>
#include <shuttle/trigger.hpp>
#include <utility>
#include <vector>
#include <weave/event.hpp>
#include <weave/task.hpp>
#include <weave/time.hpp>

namespace shuttle {

Trigger after(weave::Event& ev) {
  // std::function instantiates against a const target: forward the non-const wait() via shared_ptr
  auto evp = std::make_shared<weave::Event>(ev);
  return Trigger([evp]() { evp->wait(); });
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
    for (const auto& tr : trigs)
      tr.wait();  // wait in order; the first failure (non-cancellation) propagates out
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
  for (auto& tr : trigs) {
    weave::create_task("shuttle-trig-any", [st, tr]() mutable {
      try {
        tr.wait();
      } catch (...) {
        // Racing semantics: cancellation/failure also counts as "this sub-trigger
        // has settled" and does not affect the other sub-triggers
      }
      st->fired.set();
    });
  }
  return detail::TriggerAccess::from_fn([st]() { st->fired.wait(); });
}

}  // namespace shuttle
