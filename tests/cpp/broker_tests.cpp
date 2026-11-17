// Broker and subscription-semantics self-checks: wildcard matching, retained, unsubscribe,
//
// These cases use independent topic namespaces (bkr.*) and share no state;
// the broker is a process-level singleton, so assertions only reference topics
// exclusive to the case (or its delta versus this case).
#include <cstdint>
#include <memory>
#include <shuttle/shuttle.hpp>
#include <stdexcept>
#include <string>
#include <vector>
#include <weave/test.hpp>
#include <weave/weave.hpp>

#include "case_runner.hpp"

using namespace weave::literals;
using shuttle_test::expect;
using shuttle_test::expect_eq;
using shuttle_test::report;
using shuttle_test::run_case;

// ── Test message types (SHUTTLE_REFLECT can only be used at global scope) ──
// The type name doubles as the cross-language type_tag, so each test file uses distinct names (to
// avoid ODR conflicts).
struct BkrTx {
  std::uint32_t addr = 0;
  std::uint32_t data = 0;
};
SHUTTLE_REFLECT(BkrTx, addr, data)

struct BkrOther {
  std::uint16_t v = 0;
};
SHUTTLE_REFLECT(BkrOther, v)

namespace {

// Byte subscriber collecting a topic's frames (used by the wildcard cases)
struct ByteTaps {
  std::vector<std::string> topics;
  std::size_t frames = 0;
};

std::shared_ptr<ByteTaps> tap(shuttle::Subscription& keep, std::string pattern) {
  auto rec = std::make_shared<ByteTaps>();
  // These cases all deliver via `publish_now`, so subscribers must register a nonblocking entry;
  // the callbacks only record and consume no simulation time, honoring the nb contract.
  keep = shuttle::subscribe_bytes(
      std::move(pattern),
      [rec](const std::vector<std::byte>& b, const shuttle::Meta& m) {
        ++rec->frames;
        rec->topics.push_back(m.topic);
        rec->topics.push_back(
            std::to_string(b.size()));  // also record the wire length to verify the encoding length
      },
      shuttle::Capability::nonblocking);
  return rec;
}

}  // namespace

WEAVE_TEST(broker_tests) {
  // case1: wildcards — "*" matches exactly one level
  run_case(__func__, "case1", [] {
    shuttle::Publisher<BkrTx> pub_a("bkr.w.a");
    shuttle::Publisher<BkrTx> pub_ab("bkr.w.a.b");
    shuttle::Subscription keep;
    auto rec = tap(keep, "bkr.w.*");

    pub_a.publish_now(BkrTx{1, 1}, shuttle::NowPolicy::skip);
    pub_ab.publish_now(BkrTx{2, 2}, shuttle::NowPolicy::skip);

    expect_eq(static_cast<long long>(rec->frames), 1, "\"*\" received only the single-level topic");
    expect(rec->topics.at(0) == "bkr.w.a", "received frame carries the single-level topic");
  });

  // case2: wildcards — ">" matches any number of further levels (at least one)
  run_case(__func__, "case2", [] {
    shuttle::Publisher<BkrTx> pub_a("bkr.m.a");
    shuttle::Publisher<BkrTx> pub_ab("bkr.m.a.b");
    shuttle::Subscription keep;
    auto rec = tap(keep, "bkr.m.>");

    pub_a.publish_now(BkrTx{1, 1}, shuttle::NowPolicy::skip);
    pub_ab.publish_now(BkrTx{2, 2}, shuttle::NowPolicy::skip);

    expect_eq(static_cast<long long>(rec->frames), 2, "\">\" received the multi-level topics");
    // wire length = two u32s
    expect(rec->topics.at(1) == "8" && rec->topics.at(3) == "8", "wire length is 8 bytes");
  });

  // case3: an exact topic does not leak across topics
  run_case(__func__, "case3", [] {
    shuttle::Publisher<BkrTx> pub_x("bkr.x.one");
    shuttle::Publisher<BkrTx> pub_y("bkr.x.two");
    shuttle::Subscription keep;
    auto rec = tap(keep, "bkr.x.one");

    pub_x.publish_now(BkrTx{1, 1}, shuttle::NowPolicy::skip);
    pub_y.publish_now(BkrTx{2, 2}, shuttle::NowPolicy::skip);

    expect_eq(static_cast<long long>(rec->frames), 1, "received only the frame of the exact topic");
  });

  // case4: retained — once enabled, a new subscriber immediately receives the latest frame
  run_case(__func__, "case4", [] {
    shuttle::Publisher<BkrTx> pub("bkr.ret.tx");
    pub.set_retained(true);
    pub.publish_now(BkrTx{0x13, 0xC3}, shuttle::NowPolicy::skip);

    BkrTx seen{};
    std::size_t frames = 0;
    auto late = shuttle::subscribe<BkrTx>("bkr.ret.tx",
                                          shuttle::nb([&](const BkrTx& tx, const shuttle::Meta&) {
                                            seen = tx;
                                            ++frames;
                                          }));
    expect_eq(static_cast<long long>(frames), 1, "retained frame replayed synchronously");
    expect(seen.addr == 0x13, "retained frame carries the most recent publication");

    pub.publish_now(BkrTx{0x14, 0xC4}, shuttle::NowPolicy::skip);
    expect_eq(static_cast<long long>(frames), 2, "normal delivery continues after the replay");
  });

  // case5: after retained is turned off, new subscribers receive no historical frame
  run_case(__func__, "case5", [] {
    shuttle::Publisher<BkrTx> pub("bkr.ret.off");
    pub.set_retained(true);
    pub.publish_now(BkrTx{0x20, 0x20}, shuttle::NowPolicy::skip);
    pub.set_retained(false);

    std::size_t frames = 0;
    auto late = shuttle::subscribe<BkrTx>(
        "bkr.ret.off", shuttle::nb([&](const BkrTx&, const shuttle::Meta&) { ++frames; }));
    expect_eq(static_cast<long long>(frames), 0, "no replay after retained is disabled");
  });

  // case6: after an explicit unsubscribe (idempotent) nothing arrives anymore; received() reflects
  // delivered frames
  run_case(__func__, "case6", [] {
    shuttle::Publisher<BkrTx> pub("bkr.unsub");
    std::size_t frames = 0;
    auto sub = shuttle::subscribe<BkrTx>(
        "bkr.unsub", shuttle::nb([&](const BkrTx&, const shuttle::Meta&) { ++frames; }));

    pub.publish_now(BkrTx{1, 1}, shuttle::NowPolicy::skip);
    expect_eq(static_cast<long long>(frames), 1, "received normally before unsubscribing");
    expect_eq(static_cast<long long>(sub.received()), 1, "received() counter");

    sub.unsubscribe();
    expect(sub.valid() && sub.received() == 1, "the handle stays queryable after unsubscribing");
    sub.unsubscribe();  // idempotent, must not throw
    pub.publish_now(BkrTx{2, 2}, shuttle::NowPolicy::skip);
    expect_eq(static_cast<long long>(frames), 1, "nothing received after unsubscribing");
    expect_eq(static_cast<long long>(sub.received()), 1,
              "counter does not grow once delivery stops");
  });

  // case7: handle shared semantics — the last destruction unsubscribes automatically
  run_case(__func__, "case7", [] {
    shuttle::Publisher<BkrTx> pub("bkr.raii");
    std::size_t frames = 0;
    {
      auto a = shuttle::subscribe<BkrTx>(
          "bkr.raii", shuttle::nb([&](const BkrTx&, const shuttle::Meta&) { ++frames; }));
      shuttle::Subscription b = a;  // shares the same subscription
      pub.publish_now(BkrTx{1, 1}, shuttle::NowPolicy::skip);
      expect_eq(static_cast<long long>(frames), 1,
                "delivered normally while a shared handle exists");
    }  // both destroyed → auto-unsubscribe
    pub.publish_now(BkrTx{2, 2}, shuttle::NowPolicy::skip);
    expect_eq(static_cast<long long>(frames), 1, "auto-unsubscribed once all handles are gone");
  });

  // case8: introspection — topic_info counters versus subscriber counts
  run_case(__func__, "case8", [] {
    shuttle::Publisher<BkrTx> pub("bkr.info");
    auto s1 = shuttle::subscribe<BkrTx>("bkr.info",
                                        shuttle::nb([](const BkrTx&, const shuttle::Meta&) {}));
    pub.publish_now(BkrTx{1, 1}, shuttle::NowPolicy::skip);

    const auto info = shuttle::broker::topic_info("bkr.info");
    expect(info.has_value(), "topic_info returns the bound topic");
    expect(info->name == "bkr.info", "topic name");
    expect(info->type_tag == "BkrTx", "type tag comes from SHUTTLE_REFLECT");
    expect_eq(static_cast<long long>(info->subscriber_count), 1, "subscriber count");
    expect(info->published >= 1, "published counter");
    expect(info->delivered >= 1, "delivered counter");
    expect(!info->retained, "retained is off by default");

    expect(!shuttle::broker::topic_info("bkr.info.absent").has_value(),
           "a topic that never appeared returns nullopt");

    // the topics() snapshot contains this topic
    bool found = false;
    for (const std::string& name : shuttle::broker::topics()) found = found || name == "bkr.info";
    expect(found, "topics() contains this topic");
  });

  // case9: many-to-many — two Publishers on one topic, Meta.seq globally monotonic per topic
  run_case(__func__, "case9", [] {
    shuttle::Publisher<BkrTx> pub_a("bkr.multi");
    shuttle::Publisher<BkrTx> pub_b("bkr.multi");
    std::vector<std::uint64_t> seqs;
    std::vector<std::uint32_t> addrs;
    auto sub = shuttle::subscribe<BkrTx>("bkr.multi",
                                         shuttle::nb([&](const BkrTx& tx, const shuttle::Meta& m) {
                                           seqs.push_back(m.seq);
                                           addrs.push_back(tx.addr);
                                         }));

    pub_a.publish_now(BkrTx{0xA1, 0}, shuttle::NowPolicy::skip);
    pub_b.publish_now(BkrTx{0xB1, 0}, shuttle::NowPolicy::skip);
    pub_a.publish_now(BkrTx{0xA2, 0}, shuttle::NowPolicy::skip);

    expect_eq(static_cast<long long>(seqs.size()), 3, "all three published frames arrived");
    expect(seqs[0] < seqs[1] && seqs[1] < seqs[2], "seq is globally monotonic across Publishers");
    expect(addrs[0] == 0xA1 && addrs[1] == 0xB1 && addrs[2] == 0xA2,
           "publish_now keeps publication order");
  });

  // case10: type-binding conflict — publishing/subscribing the same topic with a different type is
  // rejected
  run_case(__func__, "case10", [] {
    shuttle::Publisher<BkrTx> pub("bkr.typed");
    pub.publish_now(BkrTx{1, 1}, shuttle::NowPolicy::skip);

    bool threw = false;
    try {
      shuttle::Publisher<BkrOther> bad("bkr.typed");  // construction declares the type
    } catch (const shuttle::TypeMismatchError&) {
      threw = true;
    }
    expect(threw, "binding a different type to the same topic throws TypeMismatchError");

    threw = false;
    try {
      auto bad_sub = shuttle::subscribe<BkrOther>(
          "bkr.typed", shuttle::nb([](const BkrOther&, const shuttle::Meta&) {}));
    } catch (const shuttle::TypeMismatchError&) {
      threw = true;
    }
    expect(threw, "subscribing to a bound topic with the wrong type throws TypeMismatchError");
  });

  // case11: disjoint types = not in the target set — a wildcard subscriber (BkrTx) is a target only
  // for frames with a matching tag; frames of bkr.mix.other neither enter the callback nor count as
  // dropped (they were never targets). This also pins the **wildcard exemption** of
  // `Broker::check_bind_compat`: bkr.mix.other's first binding has a tag different from the
  // wildcard subscriber above (BkrTx), but wildcards span types by design, so the binding must not
  // error.
  run_case(__func__, "case11", [] {
    // A wildcard subscription cannot know the concrete topic's type at subscribe time;
    // the target-set decision is deferred to each frame's snapshot.
    std::size_t uart_frames = 0;
    auto wild = shuttle::subscribe<BkrTx>(
        "bkr.mix.>", shuttle::nb([&](const BkrTx&, const shuttle::Meta&) { ++uart_frames; }));

    shuttle::Publisher<BkrTx> pub_u("bkr.mix.uart");
    shuttle::Publisher<BkrOther> pub_o("bkr.mix.other");
    pub_u.publish_now(BkrTx{1, 1}, shuttle::NowPolicy::skip);
    pub_o.publish_now(BkrOther{7}, shuttle::NowPolicy::skip);

    expect_eq(static_cast<long long>(uart_frames), 1,
              "only frames with a matching tag are delivered");
    expect_eq(static_cast<long long>(wild.received()), 1, "only matching frames count as received");
    expect_eq(static_cast<long long>(wild.dropped()), 0,
              "disjoint-type frames are not targets and do not count as dropped");
  });

  // case12: unsubscribe takes effect immediately — a target unsubscribed after the snapshot but
  // before the callback is recorded as retired. The snapshot guarantees "who was selected", not
  // "will certainly be invoked": on the C++ side the snapshot holds a shared_ptr and the object is
  // still alive, so deliver must explicitly check `removed`; on the SV side the sink has been
  // deleted from the registry and shuttle_c_sub_retired reports it back. Both languages share one
  // rule.
  run_case(__func__, "case12", [] {
    shuttle::Publisher<BkrTx> pub("bkr.inflight");
    std::size_t a_frames = 0;
    std::size_t b_frames = 0;
    shuttle::Subscription a;
    // registration order = delivery order on the serial chain: B runs first and unsubscribes A,
    // which is queued after it, inside its own callback
    auto b = shuttle::subscribe<BkrTx>("bkr.inflight",
                                       shuttle::nb([&](const BkrTx&, const shuttle::Meta&) {
                                         ++b_frames;
                                         a.unsubscribe();
                                       }));
    a = shuttle::subscribe<BkrTx>(
        "bkr.inflight", shuttle::nb([&](const BkrTx&, const shuttle::Meta&) { ++a_frames; }));

    auto p = pub.publish_serial(BkrTx{1, 1});
    p.wait();

    expect_eq(static_cast<long long>(b_frames), 1, "the earlier subscriber still receives");
    expect_eq(static_cast<long long>(a_frames), 0,
              "a target unsubscribed within the same frame never enters the callback");
    expect_eq(static_cast<long long>(p.subscriber_count()), 2,
              "target count frozen at publish time");
    expect_eq(static_cast<long long>(p.finished()), 2, "both targets reached a terminal state");
    expect(p.done(), "done() holds once all targets are terminal");
    const auto info = shuttle::broker::topic_info("bkr.inflight");
    expect(info.has_value() && info->delivered == 1,
           "delivered counts only frames that entered a callback");
    expect_eq(static_cast<long long>(p.retired()), 1,
              "the unsubscribed one is recorded as retired");
    expect_eq(static_cast<long long>(a.dropped()), 1,
              "retiring counts into the dropped of that subscriber");
    expect(b.valid(), "b never unsubscribed");
    // three-way split: dropped is the sum of retired / skipped / failed (same accounting as
    // architecture §9.2 item 5)
    expect_eq(static_cast<long long>(info->retired), 1, "retired component readable separately");
    expect_eq(static_cast<long long>(info->skipped), 0, "no policy skips");
    expect_eq(static_cast<long long>(info->failed), 0, "no failures");
    expect_eq(static_cast<long long>(info->dropped),
              static_cast<long long>(info->retired + info->skipped + info->failed),
              "topic-level dropped == retired + skipped + failed");
  });

  // case13: neither ">" nor "*" matches the prefix itself (at least one more level is required).
  // This boundary is the easiest place for the matcher to go wrong when rewritten from "split into
  // a vector then compare" to "compare segment views".
  run_case(__func__, "case13", [] {
    shuttle::Publisher<BkrTx> p_prefix("bkr.deep");
    shuttle::Publisher<BkrTx> p_child("bkr.deep.x");
    shuttle::Subscription keep_a;
    shuttle::Subscription keep_b;
    auto gt = tap(keep_a, "bkr.deep.>");
    auto star = tap(keep_b, "bkr.deep.*");

    p_prefix.publish_now(BkrTx{1, 1}, shuttle::NowPolicy::skip);
    expect_eq(static_cast<long long>(gt->frames), 0, "\">\" does not match the prefix itself");
    expect_eq(static_cast<long long>(star->frames), 0, "\"*\" does not match the prefix itself");

    p_child.publish_now(BkrTx{2, 2}, shuttle::NowPolicy::skip);
    expect_eq(static_cast<long long>(gt->frames), 1, "\">\" receives a child level");
    expect_eq(static_cast<long long>(star->frames), 1, "\"*\" receives a single-level child topic");
  });

  // case14: wildcards belong to subscription patterns only. Using a wildcard-bearing name for
  // publish / type declaration / retained is a user error and must be reported before any state is
  // created — otherwise the topics table keeps a phantom topic "whose name contains *", with its
  // own seq / type binding / retained slot, and since routing is string comparison, pub("a.*")
  // silently fans out to the a.* / a.> / > subscribers (looking like a "broadcast", but really a
  // name colliding with a pattern).
  run_case(__func__, "case14", [] {
    const auto topics_before = shuttle::broker::topics().size();

    // 1) a wildcard-bearing type declaration → TopicPatternError, and no topic entry is left behind
    bool declared = false;
    try {
      shuttle::detail::topic_declare("bkr.bad.*", shuttle::detail::type_ops<BkrTx>());
      declared = true;
    } catch (const shuttle::TopicPatternError&) {
    }
    expect(!declared, "topic_declare rejects wildcard names");
    expect_eq(static_cast<long long>(shuttle::broker::topics().size()),
              static_cast<long long>(topics_before),
              "the rejected declaration leaves no phantom topic");

    // 2) the Publisher constructor (which goes through topic_declare internally) rejects as well
    bool constructed = false;
    try {
      shuttle::Publisher<BkrTx> bad("bkr.bad2.*");
      constructed = true;
    } catch (const shuttle::TopicPatternError&) {
    }
    expect(!constructed, "Publisher rejects wildcard topics");

    // 3) a wildcard-bearing retained declaration → rejected
    bool retained = false;
    try {
      shuttle::detail::topic_set_retained("bkr.ret.*", true);
      retained = true;
    } catch (const shuttle::TopicPatternError&) {
    }
    expect(!retained, "topic_set_retained rejects wildcard names");

    // 4) subscription-pattern syntax: ">" must be the last segment, no empty segments, no wildcards
    // inside a plain segment, not empty
    auto rejected = [](const std::string& pattern) {
      try {
        auto keep = shuttle::subscribe<BkrTx>(
            pattern, shuttle::nb([](const BkrTx&, const shuttle::Meta&) {}));
        (void)keep;
        return false;
      } catch (const shuttle::TopicPatternError&) {
        return true;
      }
    };
    expect(rejected("bkr.>.x"), "\">\" must be the last segment in a pattern");
    expect(rejected("bkr..x"), "a pattern must not contain empty segments");
    expect(rejected("bkr.x*"), "a wildcard must occupy a whole segment");
    expect(rejected(""), "a pattern must not be empty");

    // 5) legal patterns are unaffected
    auto ok = shuttle::subscribe<BkrTx>("bkr.fine.>",
                                        shuttle::nb([](const BkrTx&, const shuttle::Meta&) {}));
    expect(ok.valid(), "legal wildcard patterns still register");
  });

  report(__func__);
}
