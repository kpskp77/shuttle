// Trigger semantics self-checks: deferred triggers (duration / weave::Event / Task / all / any) and
// waiting semantics.
//
// Semantic essentials of a Trigger:
//   · the snapshot (subscriber set + message copy) completes at publish time; the trigger only
//   decides "when callbacks start"; · subscribers receive nothing before the condition holds, and
//   the Publication stays incomplete; · Trigger::wait() runs inside the broker's dispatch task, so
//   the callback may block.

// Suite entry: void shuttle_tests_trigger() is called by tests/shuttle_tests.cpp, which owns the
// only WEAVE_TEST and the suite order; the [PASS] label is the function name.

#include <cstdint>
#include <shuttle/shuttle.hpp>
#include <stdexcept>
#include <string>
#include <weave/sv.hpp>
#include <weave/weave.hpp>

#include "shuttle_tests_runner.hpp"

using namespace weave::literals;
using shuttle_test::expect;
using shuttle_test::expect_eq;
using shuttle_test::report;
using shuttle_test::run_case;

// ── Message type (private to shuttle_tests_trigger): SHUTTLE_REFLECT must be used at global scope;
// the type name is the cross-language type_tag; each test file uses distinct names to avoid ODR
// conflicts.
struct TrigMsg {
  std::uint32_t v = 0;
};
SHUTTLE_REFLECT(TrigMsg, v)

void shuttle_tests_trigger() {
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
  // the callback ("in-flight frames are unaffected by unsubscribe" was the old, deprecated
  // contract: the SV sink is deleted from the registry at unsubscribe time, so both languages now
  // share "unsubscribe takes effect immediately" and such deliveries are recorded as retired).
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

  // case8: a pending trigger does not hold the topic's serial chain — a deferred frame joins the
  // chain only when its trigger fires, so a later trigger-free frame is not blocked behind it
  run_case(__func__, "case8", [] {
    shuttle::Publisher<TrigMsg> pub("trig.chain");
    weave::Event ev;  // deliberately left unset while the second frame is published
    std::string order;
    auto s = shuttle::subscribe<TrigMsg>("trig.chain", [&](const TrigMsg& m, const shuttle::Meta&) {
      order += std::to_string(m.v);
    });

    auto deferred = pub.publish_serial(TrigMsg{1}, shuttle::after(ev));
    auto plain = pub.publish_serial(TrigMsg{2});

    expect(plain.wait(200_us), "a trigger-free frame is not held back by a pending trigger");
    expect_eq(static_cast<long long>(order.size()), 1, "only the trigger-free frame was delivered");
    expect(order == "2", "the trigger-free frame overtakes the deferred one");
    expect(!deferred.done(), "the deferred frame is still waiting for its trigger");

    ev.set();
    expect(deferred.wait(200_us), "the deferred frame delivers once its trigger fires");
    expect(order == "21", "the deferred frame delivers once its trigger fires (ready order)");
    expect(deferred.done(), "the deferred frame reaches a terminal state");
  });

  // case9: a trigger may be satisfied by the callback of a frame published later on the same topic
  // — this is the deadlock case that reserving the chain tail at publish time used to create
  run_case(__func__, "case9", [] {
    shuttle::Publisher<TrigMsg> pub("trig.feed");
    weave::Event gate;
    std::string order;
    auto s = shuttle::subscribe<TrigMsg>("trig.feed", [&](const TrigMsg& m, const shuttle::Meta&) {
      order += std::to_string(m.v);
      if (m.v == 2) gate.set();  // the later frame's callback releases the earlier frame
    });

    auto first = pub.publish_serial(TrigMsg{1}, shuttle::after(gate));
    auto second = pub.publish_serial(TrigMsg{2});
    // Timed waits: with a pending trigger holding the chain this case would hang forever instead of
    // failing, which is exactly the failure mode being guarded against.
    expect(second.wait(200_us), "the later frame runs first");
    expect(first.wait(200_us),
           "the gate set by the later frame's callback releases the earlier one");
    expect(order == "21", "the later frame runs first and releases the earlier one");
    expect(first.done() && second.done(), "both frames reach a terminal state");
    expect(first.failures().empty() && second.failures().empty(), "no failures");
  });

  // case10: a pending trigger does not hold the subscriber's concurrent chain either — the deferred
  // frame joins its own per-(topic, subscriber) chain only when its trigger fires, so a later
  // trigger-free frame of the same subscriber is not blocked behind it
  run_case(__func__, "case10", [] {
    shuttle::Publisher<TrigMsg> pub("trig.par.chain");
    weave::Event ev;  // deliberately left unset while the second frame is published
    std::string a_order;
    std::string b_order;
    auto sa = shuttle::subscribe<TrigMsg>(
        "trig.par.chain",
        [&](const TrigMsg& m, const shuttle::Meta&) { a_order += std::to_string(m.v); });
    auto sb = shuttle::subscribe<TrigMsg>(
        "trig.par.chain",
        [&](const TrigMsg& m, const shuttle::Meta&) { b_order += std::to_string(m.v); });

    auto deferred = pub.publish_concurrent(TrigMsg{1}, shuttle::after(ev));
    auto plain = pub.publish_concurrent(TrigMsg{2});
    expect(plain.wait(200_us), "a trigger-free frame is not held back by a pending trigger");
    expect(a_order == "2" && b_order == "2", "only the trigger-free frame was delivered");
    expect(!deferred.done(), "the deferred frame is still waiting for its trigger");

    ev.set();
    expect(deferred.wait(200_us), "the deferred frame delivers once its trigger fires");
    expect(a_order == "21" && b_order == "21",
           "the deferred frame joins each subscriber's chain at trigger time");
    expect(deferred.done() && plain.done(), "both frames reach a terminal state");
  });

  // case11: unsubscribing during a deferred concurrent wait — the retired target skips the chain
  // handoff (no stale chain entry) and the surviving subscriber is unaffected
  run_case(__func__, "case11", [] {
    shuttle::Publisher<TrigMsg> pub("trig.par.unsub");
    weave::Event ev;
    std::size_t gone = 0;
    std::size_t left = 0;
    shuttle::Publication h;
    {
      auto s = shuttle::subscribe<TrigMsg>("trig.par.unsub",
                                           [&](const TrigMsg&, const shuttle::Meta&) { ++gone; });
      auto keep = shuttle::subscribe<TrigMsg>(
          "trig.par.unsub", [&](const TrigMsg&, const shuttle::Meta&) { ++left; });
      h = pub.publish_concurrent(TrigMsg{1}, shuttle::after(ev));
      s.unsubscribe();  // the frame is snapshotted, but this target has unsubscribed
      ev.set();
      h.wait();
      expect_eq(static_cast<long long>(gone), 0,
                "an unsubscribed concurrent target never enters its callback");
      expect_eq(static_cast<long long>(left), 1, "the surviving subscriber receives");
      expect_eq(static_cast<long long>(h.retired()), 1, "the retired target is counted");
      expect(h.done(), "still reaches a terminal state");
      pub.publish_concurrent(TrigMsg{2}).wait();
      expect_eq(static_cast<long long>(left), 2, "later frames still reach the survivor");
    }
    expect_eq(static_cast<long long>(gone), 0, "no phantom deliveries");
  });

  // case12: after(weave::Pulse&) — same edge contract as after(Event) but *not* sticky: a fire()
  // that lands before the delivery process registered its wait is lost, so the frame stays pending
  // until the next fire()
  run_case(__func__, "case12", [] {
    shuttle::Publisher<TrigMsg> pub("trig.pulse");
    weave::Pulse pulse;
    std::uint32_t seen = 0;
    int hits = 0;
    auto s = shuttle::subscribe<TrigMsg>("trig.pulse", [&](const TrigMsg& m, const shuttle::Meta&) {
      seen = m.v;
      ++hits;
    });

    auto early = pub.publish_serial(TrigMsg{11}, shuttle::after(pulse));
    pulse.fire();  // no waiter registered yet at publish time -> lost
    weave::sleep(20_us);
    expect_eq(static_cast<long long>(seen), 0,
              "a pulse fired before the wait is lost, not latched");
    expect(!early.done(), "the frame is still pending after the lost edge");

    pulse.fire();
    expect(early.wait(200_us), "the next fire releases the pending frame");
    expect_eq(static_cast<long long>(seen), 11, "payload delivered after the second edge");
    expect_eq(static_cast<long long>(hits), 1, "one delivery, no duplicate");
  });

  // case13: after(weave::sv::Edge&) — a named edge, i.e. the same object the SV side reaches as
  // `weave_edge`. Named by string only: the Edge constructor find-or-inserts into weave's own
  // table, so no id crosses the DPI boundary. Works on both backends as long as someone fires it
  // (here C++ does), and it composes with the other factories (after_all with a duration).
  run_case(__func__, "case13", [] {
    shuttle::Publisher<TrigMsg> pub("trig.edge");
    weave::sv::Edge edge("trig.edge.sync");
    std::uint32_t seen = 0;
    auto s = shuttle::subscribe<TrigMsg>(
        "trig.edge", [&](const TrigMsg& m, const shuttle::Meta&) { seen = m.v; });

    auto h = pub.publish_serial(TrigMsg{7}, shuttle::after(edge));
    weave::sleep(20_us);
    expect_eq(static_cast<long long>(seen), 0, "nothing is delivered before the edge fires");
    expect(!h.done(), "the frame is pending while the edge is quiet");

    edge.fire();
    expect(h.wait(200_us), "firing the named edge releases the frame");
    expect_eq(static_cast<long long>(seen), 7, "the edge-triggered frame delivers its payload");

    // composition: after_all(edge, duration) needs both legs
    auto both = pub.publish_serial(
        TrigMsg{9}, shuttle::after_all({shuttle::after(edge), shuttle::after(5_us)}));
    weave::sleep(1_us);  // let the delivery process register its wait (an edge is not a latch)
    edge.fire();
    expect_eq(static_cast<long long>(seen), 7, "the duration leg still holds the frame back");
    expect(both.wait(200_us), "both legs satisfied by now");
    expect_eq(static_cast<long long>(seen), 9, "composed trigger delivers");
  });

  report(__func__);
}