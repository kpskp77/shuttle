// Behavior self-checks of the three publish flavors:
//   `publish_now()`         — synchronous callbacks to nonblocking entries on the publisher's
//   stack; never suspends `publish_serial()`      — one background weave task per topic, in-order
//   callbacks, per-topic FIFO `publish_concurrent()`  — one background weave task per subscriber,
//   parallel across subscribers, FIFO per (topic, subscriber)
// plus their waiting semantics (wait / wait(timeout) / done / result / failures).

// Suite entry: void shuttle_tests_publish() is called by tests/shuttle_tests.cpp, which owns the
// only WEAVE_TEST and the suite order; the [PASS] label is the function name.

#include <cstdint>
#include <memory>
#include <shuttle/shuttle.hpp>
#include <stdexcept>
#include <string>
#include <vector>
#include <weave/weave.hpp>

#include "shuttle_tests_runner.hpp"

using namespace weave::literals;
using shuttle_test::expect;
using shuttle_test::expect_eq;
using shuttle_test::report;
using shuttle_test::run_case;

// ── Message types (private to shuttle_tests_publish): SHUTTLE_REFLECT must be used at global
// scope; the type name is the cross-language type_tag, so each test file uses distinct names to
// avoid ODR conflicts.
struct PubMsg {
  std::uint32_t v = 0;
};
SHUTTLE_REFLECT(PubMsg, v)

// Another type, used to verify "type not in the target set" (a different type_tag is not a target
// of this frame)
struct PubMsgOther {
  std::uint32_t v = 0;
};
SHUTTLE_REFLECT(PubMsgOther, v)

void shuttle_tests_publish() {
  // case1: `publish_now` is synchronous — the nb callback has already run before publish_now
  // returns (the publisher never suspends)
  run_case(__func__, "case1", [] {
    shuttle::Publisher<PubMsg> pub("pub.now.sync");
    std::vector<std::uint32_t> order;
    auto s1 = shuttle::subscribe<PubMsg>(
        "pub.now.sync",
        shuttle::nb([&](const PubMsg& m, const shuttle::Meta&) { order.push_back(m.v + 100); }));
    auto s2 = shuttle::subscribe<PubMsg>(
        "pub.now.sync",
        shuttle::nb([&](const PubMsg& m, const shuttle::Meta&) { order.push_back(m.v + 200); }));

    pub.publish_now(PubMsg{1});
    expect_eq(static_cast<long long>(order.size()), 2,
              "all callbacks finished by the time publish_now returns");
    expect(order[0] == 101 && order[1] == 201, "synchronous callbacks in subscription order");
  });

  // case2: default policy = defer — blocking-entry subscribers move to the background serial chain;
  // the publisher is not suspended
  run_case(__func__, "case2", [] {
    shuttle::Publisher<PubMsg> pub("pub.now.blk");
    std::size_t blocking_frames = 0;
    auto s = shuttle::subscribe<PubMsg>(
        "pub.now.blk", [&](const PubMsg&, const shuttle::Meta&) { ++blocking_frames; });

    auto h = pub.publish_now(PubMsg{1});  // does not throw
    expect(h.valid(), "the default policy returns a handle");
    expect_eq(static_cast<long long>(h.deferred()), 1,
              "only the blocking-entry subscriber is deferred");
    expect_eq(static_cast<long long>(blocking_frames), 0, "defer does not block the publisher");
    h.wait();
    expect_eq(static_cast<long long>(blocking_frames), 1,
              "the frame arrives after control is yielded");
    expect(h.done(), "the handle completes once the deferred part finishes");
  });

  // case3: skip policy — subscribers with only a blocking entry are skipped, no exception, and the
  // skip is countable
  run_case(__func__, "case3", [] {
    shuttle::Publisher<PubMsg> pub("pub.now.skip");
    std::size_t blocking_frames = 0;
    auto s = shuttle::subscribe<PubMsg>(
        "pub.now.skip", [&](const PubMsg&, const shuttle::Meta&) { ++blocking_frames; });
    std::size_t nb_frames = 0;
    auto n = shuttle::subscribe<PubMsg>(
        "pub.now.skip", shuttle::nb([&](const PubMsg&, const shuttle::Meta&) { ++nb_frames; }));

    auto h = pub.publish_now(PubMsg{1}, shuttle::NowPolicy::skip);
    expect_eq(static_cast<long long>(blocking_frames), 0,
              "skip skips subscribers with only a blocking entry");
    expect_eq(static_cast<long long>(nb_frames), 1,
              "the nonblocking-entry subscriber is still delivered");
    expect_eq(static_cast<long long>(h.skipped()), 1, "skip counts into skipped");
    // A target removed by policy is not a frame-level target (not in subscriber_count), but must be
    // countable at subscriber level: skipped is one of the three dropped components. The
    // difference between the two views is written at the top of publication.hpp.
    expect_eq(static_cast<long long>(s.dropped()), 1,
              "a skipped target counts into that subscriber dropped (skipped component)");
    expect_eq(static_cast<long long>(n.dropped()), 0, "a delivered subscriber has no dropped");
  });

  // case4: reject / defer — reject errors and delivers partially to no one; defer does not block
  // the publisher
  run_case(__func__, "case4", [] {
    shuttle::Publisher<PubMsg> pub("pub.now.defer");
    std::size_t frames = 0;
    auto s = shuttle::subscribe<PubMsg>("pub.now.defer",
                                        [&](const PubMsg&, const shuttle::Meta&) { ++frames; });

    bool threw = false;
    try {
      pub.publish_now(PubMsg{0}, shuttle::NowPolicy::reject);
    } catch (const shuttle::CapabilityError&) {
      threw = true;
    }
    expect(threw, "the reject policy must error instead of silently dropping the frame");
    expect_eq(static_cast<long long>(frames), 0, "no partial delivery on error");

    pub.publish_now(PubMsg{1}, shuttle::NowPolicy::defer);
    expect_eq(static_cast<long long>(frames), 0, "defer does not block the publisher");
    for (int i = 0; i < 50 && frames == 0; ++i) weave::sleep(100_us);
    expect_eq(static_cast<long long>(frames), 1,
              "the subscriber receives the frame after control is yielded");
  });

  // case5: `publish_serial` per-topic FIFO — three consecutive frames arrive in publish order
  run_case(__func__, "case5", [] {
    shuttle::Publisher<PubMsg> pub("pub.seq.fifo");
    std::vector<std::uint32_t> seen;
    std::vector<std::uint64_t> seqs;
    auto s =
        shuttle::subscribe<PubMsg>("pub.seq.fifo", [&](const PubMsg& m, const shuttle::Meta& meta) {
          seen.push_back(m.v);
          seqs.push_back(meta.topic_seq);
          weave::sleep(50_us);  // deliberately slow: does not change the ordering guarantee
        });

    pub.publish_serial(PubMsg{1});
    pub.publish_serial(PubMsg{2});
    pub.publish_serial(PubMsg{3});
    auto last = pub.publish_serial(PubMsg{4});
    last.wait();

    expect_eq(static_cast<long long>(seen.size()), 4, "all four frames arrived");
    for (std::uint32_t i = 0; i < 4; ++i) {
      expect_eq(static_cast<long long>(seen.at(i)), i + 1, "publish_serial keeps topic-level FIFO");
    }
    expect(seqs[0] < seqs[1] && seqs[1] < seqs[2] && seqs[2] < seqs[3],
           "seq consistent with delivery order");
  });

  // case6: `publish_serial` snapshot semantics — subscribers added after the publish call do not
  // join this publication
  run_case(__func__, "case6", [] {
    shuttle::Publisher<PubMsg> pub("pub.seq.snap");
    std::size_t first = 0;
    auto s1 = shuttle::subscribe<PubMsg>("pub.seq.snap", [&](const PubMsg&, const shuttle::Meta&) {
      ++first;
      weave::sleep(200_us);  // widen the window: let the new subscriber register mid-delivery
    });

    auto h = pub.publish_serial(PubMsg{1});
    expect_eq(static_cast<long long>(h.subscriber_count()), 1,
              "the snapshot is locked at publish time");

    std::size_t late = 0;
    auto s2 = shuttle::subscribe<PubMsg>("pub.seq.snap",
                                         [&](const PubMsg&, const shuttle::Meta&) { ++late; });
    h.wait();

    expect_eq(static_cast<long long>(first), 1, "the existing subscriber receives");
    expect_eq(static_cast<long long>(late), 0,
              "a subscriber added after the publish does not receive this frame");

    // The target set of the *next* frame is computed at its publish time, so it includes the
    // subscriber that registered in between.
    auto h2 = pub.publish_serial(PubMsg{2});
    expect_eq(static_cast<long long>(h2.subscriber_count()), 2,
              "the next publish sees the subscriber registered in between");
    h2.wait();
    expect_eq(static_cast<long long>(late), 1, "the late subscriber receives the next frame");
  });

  // case7: `publish_serial` failure aggregation — one subscriber throwing does not affect the
  // others; result() rethrows
  run_case(__func__, "case7", [] {
    shuttle::Publisher<PubMsg> pub("pub.seq.fail");
    std::size_t ok = 0;
    auto bad = shuttle::subscribe<PubMsg>("pub.seq.fail", [](const PubMsg&, const shuttle::Meta&) {
      throw std::runtime_error("subscriber failing on purpose");
    });
    auto good = shuttle::subscribe<PubMsg>("pub.seq.fail",
                                           [&](const PubMsg&, const shuttle::Meta&) { ++ok; });

    auto h = pub.publish_serial(PubMsg{1});
    h.wait();
    expect(h.done(), "all subscriber callbacks finished (failures included)");
    expect_eq(static_cast<long long>(h.finished()), 2, "finished == subscriber_count");
    expect_eq(static_cast<long long>(ok), 1, "a failure does not block the remaining subscribers");
    expect_eq(static_cast<long long>(h.failures().size()), 1,
              "failures are aggregated per subscriber");
    expect(!h.failures().at(0).subscriber.empty(),
           "the failure record carries a subscriber description");
    // The failure is recorded as failed at both subscriber and topic level (the third dropped
    // component;
    expect_eq(static_cast<long long>(bad.dropped()), 1, "the failed subscriber records dropped");
    expect_eq(static_cast<long long>(good.dropped()), 0,
              "the successful subscriber records no dropped");
    const auto info = shuttle::broker::topic_info("pub.seq.fail");
    expect(info.has_value(), "topic is introspectable");
    expect_eq(static_cast<long long>(info->failed), 1, "topic-level failed recorded");
    expect_eq(static_cast<long long>(info->retired), 0, "no unsubscribes");
    expect_eq(static_cast<long long>(info->skipped), 0, "no policy skips");
    expect_eq(static_cast<long long>(info->dropped), 1,
              "topic-level dropped == retired + skipped + failed");

    bool threw = false;
    std::string what;
    try {
      h.result();
    } catch (const std::exception& e) {
      threw = true;
      what = e.what();
    }
    expect(threw, "result() must rethrow the failure");
    expect(what.find("subscriber failing on purpose") != std::string::npos,
           "the rethrown exception is identifiable");
  });

  // case8: `publish_concurrent` — each subscriber gets its own callback process; complete only
  // after all arrive
  run_case(__func__, "case8", [] {
    shuttle::Publisher<PubMsg> pub("pub.par.all");
    std::size_t a = 0, b = 0, c = 0;
    auto sa = shuttle::subscribe<PubMsg>("pub.par.all", [&](const PubMsg&, const shuttle::Meta&) {
      ++a;
      weave::sleep(20_us);
    });
    auto sb = shuttle::subscribe<PubMsg>("pub.par.all", [&](const PubMsg&, const shuttle::Meta&) {
      ++b;
      weave::sleep(40_us);
    });
    auto sc = shuttle::subscribe<PubMsg>("pub.par.all",
                                         [&](const PubMsg&, const shuttle::Meta&) { ++c; });

    auto h = pub.publish_concurrent(PubMsg{7});
    expect_eq(static_cast<long long>(h.subscriber_count()), 3, "delivered to three subscribers");
    expect_eq(static_cast<long long>(a + b + c), 0,
              "publish_concurrent returns immediately without blocking the publisher");

    h.wait();
    expect_eq(static_cast<long long>(a), 1, "subscriber A completed");
    expect_eq(static_cast<long long>(b), 1, "subscriber B completed");
    expect_eq(static_cast<long long>(c), 1, "subscriber C completed");
    expect(h.done(), "done() is true once all complete");
  });

  // case9: publishing with zero subscribers — completes immediately, does not hang
  run_case(__func__, "case9", [] {
    shuttle::Publisher<PubMsg> pub("pub.empty");
    auto h = pub.publish_serial(PubMsg{1});
    expect_eq(static_cast<long long>(h.subscriber_count()), 0, "no subscribers");
    h.wait();
    expect(h.done(), "a publication with zero subscribers completes immediately");

    auto p = pub.publish_concurrent(PubMsg{2});
    p.wait();
    expect(p.done(), "publish_concurrent with zero subscribers also completes immediately");
  });

  // case10: wait(timeout) returns false on timeout and does not swallow the later completion
  run_case(__func__, "case10", [] {
    shuttle::Publisher<PubMsg> pub("pub.timeout");
    auto s = shuttle::subscribe<PubMsg>(
        "pub.timeout", [](const PubMsg&, const shuttle::Meta&) { weave::sleep(20_ms); });

    auto h = pub.publish_serial(PubMsg{1});
    expect(!h.wait(1_us), "timeout returns false");
    h.wait();  // keep waiting; must be able to complete
    expect(h.done(), "still able to wait to completion after the timeout");
    expect(h.failures().empty(), "no failures");
  });

  // case11: entry selection — `now` only uses the nonblocking entry; `serial`/`concurrent` deliver
  // to all (each subscriber on the entry it registered)
  run_case(__func__, "case11", [] {
    shuttle::Publisher<PubMsg> pub("pub.once");
    std::size_t blocking_frames = 0;
    std::size_t nb_frames = 0;
    auto sb = shuttle::subscribe<PubMsg>(
        "pub.once", [&](const PubMsg&, const shuttle::Meta&) { ++blocking_frames; });
    auto sn = shuttle::subscribe<PubMsg>(
        "pub.once", shuttle::nb([&](const PubMsg&, const shuttle::Meta&) { ++nb_frames; }));

    pub.publish_now(PubMsg{1}, shuttle::NowPolicy::skip);  // only hits the nonblocking entry
    expect_eq(static_cast<long long>(nb_frames), 1, "publish_now hits the nonblocking entry");
    expect_eq(static_cast<long long>(blocking_frames), 0,
              "publish_now skip does not hit the blocking entry");

    // The target set of `serial`/`concurrent` is "all matching subscribers": each receives one
    // frame
    auto hp = pub.publish_concurrent(PubMsg{2});
    hp.wait();
    expect_eq(static_cast<long long>(blocking_frames), 1,
              "publish_concurrent hits the blocking entry");
    expect_eq(static_cast<long long>(nb_frames), 2,
              "publish_concurrent also hits the nonblocking entry");
    expect_eq(static_cast<long long>(hp.subscriber_count()), 2,
              "publish_concurrent target count = all subscribers");

    auto hs = pub.publish_serial(PubMsg{3});
    hs.wait();
    expect_eq(static_cast<long long>(blocking_frames), 2, "publish_serial hits the blocking entry");
    expect_eq(static_cast<long long>(nb_frames), 3,
              "publish_serial also hits the nonblocking entry");
    expect_eq(static_cast<long long>(hs.subscriber_count()), 2,
              "publish_serial target count = all subscribers");
  });

  // case12: visibility counters — the handle returned by publish_now(defer) covers both the
  // nonblocking and the deferred parts
  run_case(__func__, "case12", [] {
    shuttle::Publisher<PubMsg> pub("pub.vis");
    std::size_t blk = 0;
    std::size_t nbh = 0;
    auto sb =
        shuttle::subscribe<PubMsg>("pub.vis", [&](const PubMsg&, const shuttle::Meta&) { ++blk; });
    auto sn = shuttle::subscribe<PubMsg>(
        "pub.vis", shuttle::nb([&](const PubMsg&, const shuttle::Meta&) { ++nbh; }));

    auto h = pub.publish_now(PubMsg{1}, shuttle::NowPolicy::defer);
    expect_eq(static_cast<long long>(h.subscriber_count()), 2,
              "targets include both the nonblocking and the deferred parts");
    expect_eq(static_cast<long long>(h.deferred()), 1, "deferred count is 1");
    expect_eq(static_cast<long long>(h.skipped()), 0, "defer skips no one");
    expect_eq(static_cast<long long>(nbh), 1,
              "the nonblocking part is done when publish_now returns");
    expect_eq(static_cast<long long>(blk), 0, "the deferred part has not started yet");
    expect(!h.done(), "deferred not finished → the handle is not done");
    h.wait();
    expect(h.done(), "the handle completes once the deferred part finishes");
    expect_eq(static_cast<long long>(blk), 1, "the deferred subscriber eventually receives");
    expect_eq(static_cast<long long>(h.finished()), 2, "finished covers both parts");

    auto skip_h = pub.publish_now(PubMsg{2}, shuttle::NowPolicy::skip);
    expect_eq(static_cast<long long>(skip_h.skipped()), 1, "skip counts into skipped");
    expect_eq(static_cast<long long>(skip_h.deferred()), 0, "skip does not count into deferred");

    auto info = shuttle::broker::topic_info("pub.vis");
    expect(info.has_value(), "topic exists");
    expect_eq(static_cast<long long>(info->deferred), 1, "topic-level cumulative deferred counter");
  });

  // case13: defer shares the same serial chain as publish_serial → background delivery order ==
  // publish order
  run_case(__func__, "case13", [] {
    shuttle::Publisher<PubMsg> pub("pub.defer.order");
    std::vector<std::uint32_t> order;
    auto sb =
        shuttle::subscribe<PubMsg>("pub.defer.order", [&](const PubMsg& m, const shuttle::Meta&) {
          order.push_back(m.v);
          weave::sleep(20_us);
        });

    pub.publish_now(PubMsg{1},
                    shuttle::NowPolicy::defer);  // skeleton: goes through the background chain
    pub.publish_serial(PubMsg{2});
    pub.publish_now(PubMsg{3}, shuttle::NowPolicy::defer);
    auto last = pub.publish_serial(PubMsg{4});
    last.wait();

    expect_eq(static_cast<long long>(order.size()), 4,
              "all four frames delivered in the background");
    for (std::size_t i = 0; i < order.size(); ++i)
      expect_eq(static_cast<long long>(order.at(i)), static_cast<long long>(i + 1),
                "mixed defer and serial still keeps publication order");
  });

  // case14: a failure in the synchronous section of `publish_now` — the remaining nonblocking
  // targets are still attempted, the defer section still enters the chain, and the first
  // exception is thrown to the caller only after all attempts (no more "pub_complete first, throw
  // later").
  run_case(__func__, "case14", [] {
    shuttle::Publisher<PubMsg> pub("pub.now.throw");
    std::size_t second = 0, blocking_hits = 0;
    auto s1 = shuttle::subscribe<PubMsg>(
        "pub.now.throw",
        shuttle::nb([](const PubMsg&, const shuttle::Meta&) { throw std::runtime_error("boom"); }));
    auto s2 = shuttle::subscribe<PubMsg>(
        "pub.now.throw", shuttle::nb([&](const PubMsg&, const shuttle::Meta&) { ++second; }));
    auto s3 = shuttle::subscribe<PubMsg>(
        "pub.now.throw", [&](const PubMsg&, const shuttle::Meta&) { ++blocking_hits; });

    bool threw = false;
    try {
      pub.publish_now(PubMsg{1});  // default defer
    } catch (const std::exception&) {
      threw = true;
    }
    expect(threw, "a synchronous-section exception still propagates to the caller");
    expect_eq(
        static_cast<long long>(second), 1,
        "the remaining nonblocking targets are still attempted after the first subscriber fails");
    weave::sleep(20_us);
    expect_eq(static_cast<long long>(blocking_hits), 1,
              "a synchronous-section failure does not affect the defer-section delivery");
    auto info = shuttle::broker::topic_info("pub.now.throw");
    expect(info.has_value(), "topic exists");
    expect_eq(static_cast<long long>(info->published), 1, "the frame was published");
    expect_eq(static_cast<long long>(info->deferred), 1,
              "the defer section still entered the chain");
  });

  // case15: a type outside the target set — a wildcard subscriber is a target only for frames
  // matching "name ∩ type"; type-disjoint frames are not even targets: subscriber_count() is 0,
  // completes immediately, no delivery unit, no counters at all
  run_case(__func__, "case15", [] {
    shuttle::Publisher<PubMsg> pub_a("pub.filter.a");
    shuttle::Publisher<PubMsgOther> pub_b("pub.filter.b");
    std::size_t got = 0;
    auto s = shuttle::subscribe<PubMsg>("pub.filter.*",
                                        [&](const PubMsg&, const shuttle::Meta&) { ++got; });

    auto ha = pub_a.publish_serial(PubMsg{1});
    ha.wait();
    expect_eq(static_cast<long long>(got), 1, "a matching type is still delivered");
    expect_eq(static_cast<long long>(ha.subscriber_count()), 1, "a target when matching");

    auto hb = pub_b.publish_serial(PubMsgOther{2});
    hb.wait();
    expect_eq(static_cast<long long>(got), 1, "a type-disjoint frame never enters the callback");
    expect_eq(static_cast<long long>(hb.subscriber_count()), 0, "type-disjoint means not a target");
    expect_eq(static_cast<long long>(hb.finished()), 0, "zero targets");
    expect(hb.done(), "zero targets complete immediately");
    expect_eq(static_cast<long long>(hb.retired()), 0,
              "not a target means nothing lands in retired either");
    expect_eq(static_cast<long long>(s.dropped()), 0,
              "no subscriber-level dropped either (it was never a target)");

    // Same publish flavor, same topic entry: reject of `now` also counts only blocking subscribers
    // in the target set
    auto hc = pub_b.publish_now(PubMsgOther{3}, shuttle::NowPolicy::defer);
    hc.wait();
    expect_eq(static_cast<long long>(hc.subscriber_count()), 0,
              "publish_now also counts by the target set");
  });

  // case16: `publish_serial` with zero targets completes immediately; a `reject`ed frame is never
  // published (consumes no seq/published)
  run_case(__func__, "case16", [] {
    shuttle::Publisher<PubMsg> pub("pub.now.empty");
    auto h = pub.publish_serial(PubMsg{1});
    expect(h.done(), "zero targets complete immediately, no delivery unit created");
    expect_eq(static_cast<long long>(h.subscriber_count()), 0, "zero targets");
    expect_eq(static_cast<long long>(h.finished()), 0, "completed with zero counts");

    shuttle::Publisher<PubMsg> p2("pub.now.reject2");
    std::size_t hits = 0;
    auto s = shuttle::subscribe<PubMsg>("pub.now.reject2",
                                        [&](const PubMsg&, const shuttle::Meta&) { ++hits; });
    bool threw = false;
    try {
      p2.publish_now(PubMsg{1}, shuttle::NowPolicy::reject);
    } catch (const shuttle::CapabilityError&) {
      threw = true;
    }
    expect(threw, "reject throws CapabilityError");
    auto info = shuttle::broker::topic_info("pub.now.reject2");
    expect(info.has_value(), "topic exists");
    expect_eq(static_cast<long long>(info->published), 0,
              "a rejected frame was never published (consumes no seq)");
    expect_eq(static_cast<long long>(hits), 0, "no callback was invoked at all");
  });

  // case17: a publish_now cycle is stopped by the reentrancy bound instead of blowing the stack
  // (A's nonblocking subscriber publishes to B, B's publishes back to A)
  run_case(__func__, "case17", [] {
    shuttle::Publisher<PubMsg> pa("pub.cycle.a");
    shuttle::Publisher<PubMsg> pb("pub.cycle.b");
    std::size_t a_hits = 0;
    std::size_t b_hits = 0;
    auto sa = shuttle::subscribe<PubMsg>("pub.cycle.a",
                                         shuttle::nb([&](const PubMsg& m, const shuttle::Meta&) {
                                           ++a_hits;
                                           pb.publish_now(PubMsg{m.v});
                                         }));
    auto sb = shuttle::subscribe<PubMsg>("pub.cycle.b",
                                         shuttle::nb([&](const PubMsg& m, const shuttle::Meta&) {
                                           ++b_hits;
                                           pa.publish_now(PubMsg{m.v});
                                         }));

    bool threw = false;
    try {
      pa.publish_now(PubMsg{1});
    } catch (const shuttle::PublishError& e) {
      threw = std::string(e.what()).find("nested deeper than") != std::string::npos;
    }
    expect(threw, "a publish_now cycle is refused with PublishError");
    expect(a_hits > 1 && b_hits > 1, "the cycle really ran before the bound stopped it");
  });

  // case18: `publish_concurrent` is FIFO per (topic, subscriber) — under the old "no ordering at
  // all" contract a slow frame could be overtaken by the next frame inside the same subscriber
  run_case(__func__, "case18", [] {
    shuttle::Publisher<PubMsg> pub("pub.par.fifo");
    std::string slow_order;
    std::string fast_order;
    auto slow =
        shuttle::subscribe<PubMsg>("pub.par.fifo", [&](const PubMsg& m, const shuttle::Meta&) {
          if (m.v == 1)
            weave::sleep(50_us);  // frame 1 stalls: without a per-subscriber chain the
                                  // later frames run straight past it
          slow_order += std::to_string(m.v);
        });
    auto fast = shuttle::subscribe<PubMsg>(
        "pub.par.fifo",
        [&](const PubMsg& m, const shuttle::Meta&) { fast_order += std::to_string(m.v); });

    std::vector<shuttle::Publication> hs;
    for (std::uint32_t v = 1; v <= 6; ++v) hs.push_back(pub.publish_concurrent(PubMsg{v}));
    for (auto& h : hs) h.wait();

    expect(slow_order == "123456", "a subscriber sees its own frames in publish order");
    expect(fast_order == "123456", "the other subscriber sees publish order too");
    expect(hs.back().done(), "every frame reaches a terminal state");
  });

  // case19: the per-subscriber chain serializes only that subscriber — a slow subscriber does not
  // delay another subscriber's callbacks (parallel across subscribers is preserved)
  run_case(__func__, "case19", [] {
    shuttle::Publisher<PubMsg> pub("pub.par.indep");
    std::size_t slow_done = 0;
    std::size_t fast_done = 0;
    auto slow =
        shuttle::subscribe<PubMsg>("pub.par.indep", [&](const PubMsg&, const shuttle::Meta&) {
          weave::sleep(100_us);
          ++slow_done;
        });
    auto fast = shuttle::subscribe<PubMsg>(
        "pub.par.indep", [&](const PubMsg&, const shuttle::Meta&) { ++fast_done; });

    auto h1 = pub.publish_concurrent(PubMsg{1});
    auto h2 = pub.publish_concurrent(PubMsg{2});
    // Frame 1 sleeps 100us, frame 2 is queued behind it for the slow subscriber, while the fast
    // subscriber already finished both.
    weave::sleep(20_us);
    expect_eq(static_cast<long long>(fast_done), 2, "the fast subscriber is not held back");
    expect_eq(static_cast<long long>(slow_done), 0, "the slow subscriber is still on frame 1");
    h1.wait();
    h2.wait();
    expect_eq(static_cast<long long>(slow_done), 2, "the queued frame runs after the first");
    expect_eq(static_cast<long long>(fast_done), 2, "no duplicate deliveries");
  });

  // case20: waiting for a frame that is queued behind the waiting task itself can never finish.
  // The wait diagnoses it (weave::DeadlockError) instead of hanging the simulation.
  run_case(__func__, "case20", [] {
    shuttle::Publisher<PubMsg> pub("pub.selfwait");
    bool diagnosed = false;
    std::size_t delivered = 0;
    auto sub =
        shuttle::subscribe<PubMsg>("pub.selfwait", [&](const PubMsg& m, const shuttle::Meta&) {
          ++delivered;
          if (m.v != 1) return;
          std::vector<shuttle::Publication> inner;
          inner.push_back(pub.publish_serial(PubMsg{2}));
          try {
            inner.back().wait();
          } catch (const weave::DeadlockError&) {
            diagnosed = true;  // frame 2 sits behind frame 1, whose delivery is this callback
          }
        });
    pub.publish_serial(PubMsg{1}).wait();
    expect(diagnosed, "a self-blocked frame is reported, not hung on");
    expect_eq(static_cast<long long>(delivered), 2, "the diagnosing callback still completes");
  });

  // case21: the same publish from a callback is legal when it does not wait — the diagnosis must
  // not fire on fire-and-forget (publishing on your own topic is a normal relay)
  run_case(__func__, "case21", [] {
    shuttle::Publisher<PubMsg> pub("pub.selfpub");
    std::string order;
    std::vector<shuttle::Publication> inner;
    auto sub =
        shuttle::subscribe<PubMsg>("pub.selfpub", [&](const PubMsg& m, const shuttle::Meta&) {
          order += std::to_string(m.v);
          if (m.v == 1) inner.push_back(pub.publish_serial(PubMsg{2}));
        });
    auto outer = pub.publish_serial(PubMsg{1});
    outer.wait();
    for (auto& h : inner) h.wait();
    expect(order == "12", "fire-and-forget publishing from a callback is not a deadlock");
  });

  // case22: the per-subscriber chain of publish_concurrent gets the same diagnosis
  run_case(__func__, "case22", [] {
    shuttle::Publisher<PubMsg> pub("pub.selfwait.par");
    bool diagnosed = false;
    std::size_t hits = 0;
    std::vector<shuttle::Publication> inner;
    auto sub =
        shuttle::subscribe<PubMsg>("pub.selfwait.par", [&](const PubMsg& m, const shuttle::Meta&) {
          ++hits;
          if (m.v != 1) return;
          inner.push_back(pub.publish_concurrent(PubMsg{2}));
          try {
            inner.back().wait();
          } catch (const weave::DeadlockError&) {
            diagnosed = true;
          }
        });
    auto outer = pub.publish_concurrent(PubMsg{1});
    outer.wait();
    for (auto& h : inner) h.wait();
    expect(diagnosed, "publish_concurrent diagnoses the same self-wait");
    expect_eq(static_cast<long long>(hits), 2, "both frames still reach the subscriber");
  });

  report(__func__);
}
