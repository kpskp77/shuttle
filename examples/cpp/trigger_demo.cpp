// Deferred trigger (Trigger) demo.
//
// All three publish flavors copy the message and lock in the subscriber set at
// the moment of the call (snapshot semantics); a Trigger only decides *when*
// the background callback process of `serial`/`concurrent` *starts*:
//   after(weave::Event&)    — start when the event is set
//   after(weave::duration)  — start after a simulation delay
//   after(weave::Task&)     — start once the given task reaches a terminal state
//   (failure/cancellation included) after_all / after_any   — combine multiple triggers
// Waiting for completion (wait/result) is orthogonal to triggers: the handle
// becomes done only after all callbacks finish.

#include <cstdint>
#include <cstdio>
#include <shuttle/shuttle.hpp>
#include <stdexcept>
#include <weave/test.hpp>
#include <weave/weave.hpp>

using namespace weave::literals;

struct Alarm {
  std::uint32_t code;
};
SHUTTLE_REFLECT(Alarm, code)

WEAVE_TEST(trigger_demo) {
  shuttle::Publisher<Alarm> pub("soc.alarm");

  int received = 0;
  auto sub = shuttle::subscribe<Alarm>("soc.alarm", [&](const Alarm& a, const shuttle::Meta&) {
    std::printf("[alarm] code=%u\n", a.code);
    ++received;
  });

  // ── Simulation-delay trigger: the callback process starts only after 100ns ──
  auto h1 = pub.publish_serial(Alarm{1}, shuttle::after(100_ns));

  // ── Event trigger: starts when the weave::Event is set (SV counterpart: TRIG_EVENT) ───────
  weave::Event ev;
  auto h2 = pub.publish_concurrent(Alarm{2}, shuttle::after(ev));
  if (h2.done()) throw std::logic_error("trigger_demo: must not complete before the event fires");
  ev.set();

  // ── Task-following trigger: starts once the worker reaches a terminal state
  // (success/failure/cancel all count as done) ────────
  auto worker = weave::create_task("worker", [] { weave::sleep(50_ns); });
  auto h3 = pub.publish_serial(Alarm{3}, shuttle::after(worker));

  // ── Combined trigger: starts only after both events are set ───────────
  weave::Event a, b;
  auto h4 =
      pub.publish_concurrent(Alarm{4}, shuttle::after_all({shuttle::after(a), shuttle::after(b)}));
  a.set();
  b.set();

  // Unified wrap-up: result() = wait + rethrow the first failure; failures() gives all details.
  h1.result();
  h2.result();
  h3.result();
  h4.result();
  if (received != 4) throw std::logic_error("trigger_demo: callback count mismatch");
}
