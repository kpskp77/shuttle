// Trigger semantics self-checks: deferred triggers (duration / weave::Event / Task / all / any) and
// waiting semantics.
//
// Semantic essentials of a Trigger:
//   · the snapshot (subscriber set + message copy) completes at publish time; the trigger only
//   decides "when callbacks start"; · subscribers receive nothing before the condition holds, and
//   the Publication stays incomplete; · Trigger::wait() runs inside the broker's dispatch task, so
//   the callback may block.

#include <cstdint>
#include <shuttle/shuttle.hpp>
#include <stdexcept>
#include <string>
#include <weave/test.hpp>
#include <weave/weave.hpp>

#include "case_runner.hpp"

using namespace weave::literals;
using shuttle_test::expect;
using shuttle_test::expect_eq;
using shuttle_test::report;
using shuttle_test::run_case;

// ── Message type (private to trigger_tests): SHUTTLE_REFLECT must be used at global scope;
// the type name is the cross-language type_tag; each test file uses distinct names to avoid ODR
// conflicts.
struct TrigMsg {
  std::uint32_t v = 0;
};
SHUTTLE_REFLECT(TrigMsg, v)

WEAVE_TEST(trigger_tests) {
  // case1: after(duration) — nothing is delivered before the deadline; after it, delivery happens
  // and completion is waitable
  run_case(__func__, "case1", [] {
    shuttle::Publisher<TrigMsg> pub("trig.delay");
    std::size_t frames = 0;
    std::vector<std::uint32_t> seen;
    auto s = shuttle::subscribe<TrigMsg>("trig.delay", [&](const TrigMsg& m, const shuttle::Meta&) {
      ++frames;
      seen.push_back(m.v);
    });

    auto h = pub.publish_serial(TrigMsg{42}, shuttle::after(1_ms));
    expect_eq(static_cast<long long>(h.subscriber_count()), 1, "snapshot locked in the subscriber");
    expect(!h.wait(200_us), "not complete before the trigger");
    expect_eq(static_cast<long long>(frames), 0, "no delivery before the trigger");

    h.wait();
    expect_eq(static_cast<long long>(frames), 1, "delivered once after the trigger");
    expect_eq(static_cast<long long>(seen.at(0)), 42, "message content unchanged (snapshot)");
  });

  // case2: after(weave::Event&) — no delivery before the event is set
  run_case(__func__, "case2", [] {
    shuttle::Publisher<TrigMsg> pub("trig.event");
    std::size_t frames = 0;
    auto s = shuttle::subscribe<TrigMsg>("trig.event",
                                         [&](const TrigMsg&, const shuttle::Meta&) { ++frames; });

    weave::Event ev;
    auto h = pub.publish_serial(TrigMsg{1}, shuttle::after(ev));
    expect(!h.wait(200_us), "not complete while the event is unset");
    expect_eq(static_cast<long long>(frames), 0, "no delivery while the event is unset");

    ev.set();
    h.wait();
    expect_eq(static_cast<long long>(frames), 1, "delivered after the event is set");
  });

  // case3: after(Task) — completion (not success) is enough; an exception inside the task does not
  // affect delivery
  run_case(__func__, "case3", [] {
    shuttle::Publisher<TrigMsg> pub("trig.task");
    std::size_t frames = 0;
    auto s = shuttle::subscribe<TrigMsg>("trig.task",
                                         [&](const TrigMsg&, const shuttle::Meta&) { ++frames; });

    auto slow = weave::create_task("slow", [] {
      weave::sleep(300_us);
      throw std::runtime_error("task failed but still completed");
    });
    auto h = pub.publish_serial(TrigMsg{1}, shuttle::after(slow));
    expect(!h.wait(50_us), "no delivery while the task is incomplete");
    expect_eq(static_cast<long long>(frames), 0, "zero delivery while the task is incomplete");

    h.wait();
    expect_eq(static_cast<long long>(frames), 1,
              "delivered once the task completes (success irrelevant)");
    bool threw = false;
    try {
      slow.await();  // the task's own exception still belongs to its caller
    } catch (const std::runtime_error&) {
      threw = true;
    }
    expect(threw, "after(Task) does not alter the task's own exception semantics");
  });

  // case4: after_all — released only when all conditions hold
  run_case(__func__, "case4", [] {
    shuttle::Publisher<TrigMsg> pub("trig.all");
    std::size_t frames = 0;
    auto s = shuttle::subscribe<TrigMsg>("trig.all",
                                         [&](const TrigMsg&, const shuttle::Meta&) { ++frames; });

    weave::Event ev;
    auto h = pub.publish_serial(TrigMsg{1},
                                shuttle::after_all({shuttle::after(200_us), shuttle::after(ev)}));
    weave::sleep(400_us);  // satisfies only the time leg
    expect_eq(static_cast<long long>(frames), 0,
              "no delivery with only part of the conditions met");
    ev.set();
    h.wait();
    expect_eq(static_cast<long long>(frames), 1, "delivered once all conditions hold");
  });

  // case5: after_any — released as soon as any condition holds
  run_case(__func__, "case5", [] {
    shuttle::Publisher<TrigMsg> pub("trig.any");
    std::size_t frames = 0;
    auto s = shuttle::subscribe<TrigMsg>("trig.any",
                                         [&](const TrigMsg&, const shuttle::Meta&) { ++frames; });

    weave::Event ev;  // never set
    auto h = pub.publish_serial(TrigMsg{1},
                                shuttle::after_any({shuttle::after(ev), shuttle::after(200_us)}));
    h.wait();
    expect_eq(static_cast<long long>(frames), 1, "released once the time leg fires first");
  });

  // case6: under `publish_concurrent` all subscribers share one trigger (the condition is awaited
  // once and releases everyone together)
  run_case(__func__, "case6", [] {
    shuttle::Publisher<TrigMsg> pub("trig.par");
    std::size_t a = 0, b = 0;
    auto sa =
        shuttle::subscribe<TrigMsg>("trig.par", [&](const TrigMsg&, const shuttle::Meta&) { ++a; });
    auto sb =
        shuttle::subscribe<TrigMsg>("trig.par", [&](const TrigMsg&, const shuttle::Meta&) { ++b; });

    weave::Event ev;
    auto h = pub.publish_concurrent(TrigMsg{1}, shuttle::after(ev));
    expect_eq(static_cast<long long>(h.subscriber_count()), 2,
              "both subscribers are in the snapshot");
    expect(!h.wait(200_us), "not triggered");
    ev.set();
    h.wait();
    expect_eq(static_cast<long long>(a), 1, "subscriber A released");
    expect_eq(static_cast<long long>(b), 1, "subscriber B released");
  });

  // case7: unsubscribe during the trigger wait — unsubscribe takes effect immediately: the snapshot
  // only says "who was selected"; if the target has unsubscribed by release time it never enters
  // the callback ("in-flight frames
  //  are unaffected by unsubscribe" was the old, deprecated contract: the SV sink is deleted from
  //  the registry at unsubscribe time, both languages now share "unsubscribe takes effect
  //  immediately", and such deliveries are recorded as retired).
  run_case(__func__, "case7", [] {
    shuttle::Publisher<TrigMsg> pub("trig.unsub");
    std::size_t frames = 0;
    shuttle::Publication h;
    {
      auto s = shuttle::subscribe<TrigMsg>("trig.unsub",
                                           [&](const TrigMsg&, const shuttle::Meta&) { ++frames; });
      weave::Event ev;
      h = pub.publish_serial(TrigMsg{1}, shuttle::after(ev));
      s.unsubscribe();  // the frame is snapshotted, but the target has unsubscribed
      ev.set();
      h.wait();
    }
    expect_eq(static_cast<long long>(frames), 0,
              "an in-flight frame released after unsubscribing never enters the callback");
    expect_eq(static_cast<long long>(h.retired()), 1,
              "that target is recorded as retired (not delivered)");
    expect(h.done(), "still reaches a terminal state");
    pub.publish_serial(TrigMsg{2}).wait();
    expect_eq(static_cast<long long>(frames), 0, "no new frames after unsubscribing");
  });

  report(__func__);
}
