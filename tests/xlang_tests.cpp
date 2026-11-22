// Cross-language round-trip self-checks (svdpi backend only): C++ → SV and SV → C++
// publish/subscribe interoperability.
//
// Paired with the xlang section of tests/shuttle_tests.sv; the handshake order is:
//   1. the SV side first subscribes for C++→SV, then publishes one frame on "xlang.sv_ready"
//   (retained);
//   2. the C++ side subscribes to "xlang.sv_ready" (retained guarantees delivery even if SV
//   published first),
//      then publishes 3 frames in order on "xlang.cpp2sv" and waits for each — completion means all
//      3 SV-side callbacks have already finished (that is exactly the Publication completion
//      semantics);
//   3. once the SV side confirms the 3 frames, it publishes 3 frames on "xlang.sv2cpp";
//   4. the C++ side waits for and validates these 3 frames (content, type_tag, monotonic seq);
//   5. case6 covers the one shape that used to break: a *mixed* target set on a single topic — an
//   SV blocking subscriber registered first, then blocking C++ subscribers that consume time. The
//   SV callback publishes a nested frame and suspends, so the simulator runs a nested C++ frame in
//   the middle of the outer delivery; weave <= 1.1.0 came back from it with no current task and the
//   next blocking C++ callback threw "weave: blocking primitive outside task context" (fixed by
//   keying task identity per simulation process in weave v1.2.0).
//   6. case7 covers nested message types (struct XPoint inside struct XTrack): C++ → SV decode via
//   the SV-side three-piece macros, SV → C++ decode via Codec<XTrack> recursing Codec<XPoint>.
//   7. case8 covers an inheritance chain (XHead → XMid → XChain) registered with
//   SHUTTLE_REFLECT_DERIVED: each level delegates its base subobject to the base codec, so both
//   directions exercise the delegated encoding against the SV `shuttle_base chain.
//
// After both sides finish their assertions, run_cpp_tests in tests/shuttle_tests.sv renders the
// verdict.

#include <cstdint>
#include <cstdio>
#include <exception>
#include <memory>
#include <shuttle/shuttle.hpp>
#include <string>
#include <vector>
#include <weave/test.hpp>
#include <weave/weave.hpp>

#include "case_runner.hpp"

// Compiled only into the svdpi backend's test target: this file calls the SV bridge entries
// directly, which do not exist under the systemc backend (no SHUTTLE_BACKEND_* macros — backends
// are selected by directory).

using namespace weave::literals;
using shuttle_test::expect;
using shuttle_test::expect_eq;
using shuttle_test::report;
using shuttle_test::run_case;

// Request/response payloads for the Requester ↔ SV-responder case (the SV mirror is
// SvRqReq/SvRqResp in tests/shuttle_tests.sv; the type_tag must match the SHUTTLE_REFLECT
// names). Only domain data here: the correlation id rides in the frame header, so neither side
// needs a handshake field in its payload.
struct XRqReq {
  std::uint32_t op = 0;
};
SHUTTLE_REFLECT(XRqReq, op)

struct XRqResp {
  std::uint32_t data = 0;
};
SHUTTLE_REFLECT(XRqResp, data)

// Message type (SHUTTLE_REFLECT must be used at global scope).
// Byte-for-byte counterpart of `class XMsg` in tests/shuttle_tests.sv:
//   {>>{addr, data}} — two unsigned 32-bit values, big-endian; both tags are "XMsg".
struct XMsg {
  std::uint32_t addr = 0;
  std::uint32_t data = 0;
};
SHUTTLE_REFLECT(XMsg, addr, data)

// Nested message types for case7: a track is two points plus an id. SHUTTLE_REFLECT recurses
// per field (Codec<XTrack> delegates to Codec<XPoint> for the point fields), so the wire format
// is fields in declaration order with the nested structs expanded inline — no length prefix. The
// SV mirror is class XPoint/XTrack in tests/shuttle_tests.sv, registered through the three-piece
// macros with `shuttle_field(origin, XPoint)`: the field's codec is the nested class itself.
struct XPoint {
  std::int32_t x = 0;
  std::int32_t y = 0;
};
SHUTTLE_REFLECT(XPoint, x, y)

struct XTrack {
  std::uint32_t id = 0;
  XPoint origin;
  XPoint dest;
};
SHUTTLE_REFLECT(XTrack, id, origin, dest)

// Inheritance chain for case8: each level lists only its own fields and delegates its base
// subobject to Codec<Base> via SHUTTLE_REFLECT_DERIVED, so the wire format is the base encoding
// followed by the own fields (here 4 + 2 + 1 bytes). The SV mirror is class
// XHead/XMid/XChain in tests/shuttle_tests.sv, which reaches the same layout through
// `shuttle_base. The middle level carries a signed field to pin the sign convention.
struct XHead {
  std::uint32_t seq = 0;
};
struct XMid : XHead {
  std::int16_t level = 0;
};
struct XChain : XMid {
  std::uint8_t flag = 0;
};
SHUTTLE_REFLECT(XHead, seq)
SHUTTLE_REFLECT_DERIVED(XMid, XHead, level)
SHUTTLE_REFLECT_DERIVED(XChain, XMid, flag)

namespace {

constexpr const char* kReadyTopic = "xlang.sv_ready";      // SV → C++ ready broadcast (retained)
constexpr const char* kToSvTopic = "xlang.cpp2sv";         // C++ → SV
constexpr const char* kFromSvTopic = "xlang.sv2cpp";       // SV → C++
constexpr const char* kMixedTopic = "xlang.mixed.tx";      // mixed target set: SV sink + C++ subs
constexpr const char* kMixedMonTopic = "xlang.mixed.mon";  // published by the SV sink itself
constexpr const char* kMixedReadyTopic =
    "xlang.mixed.ready";                                     // SV → C++ ready broadcast (retained)
constexpr const char* kNestReadyTopic = "xlang.nest.ready";  // SV → C++ ready (retained)
constexpr const char* kNestToSvTopic = "xlang.nest.cpp2sv";  // C++ → SV, nested types
constexpr const char* kNestFromSvTopic = "xlang.nest.sv2cpp";    // SV → C++, nested types
constexpr const char* kInhReadyTopic = "xlang.inherit.ready";    // SV → C++ ready (retained)
constexpr const char* kInhToSvTopic = "xlang.inherit.cpp2sv";    // C++ → SV, inheritance chain
constexpr const char* kInhFromSvTopic = "xlang.inherit.sv2cpp";  // SV → C++, inheritance chain

// State shared across cases (subscription handles must outlive the test, so they live in a heap
// holder)
struct XlangState {
  shuttle::Subscription ready;    // retained ready frame
  shuttle::Subscription from_sv;  // SV → C++
  weave::Event ready_ev;
  std::vector<XMsg> got;
  std::vector<std::uint64_t> seqs;
  std::vector<std::string> tags;
  std::size_t ready_frames = 0;

  // case6 state: the mixed target set
  shuttle::Subscription mixed_ready;
  shuttle::Subscription mixed_victim;
  shuttle::Subscription mixed_monitor;
  weave::Event mixed_ready_ev;
  std::size_t mixed_hits = 0;      // the C++ subscriber that blocks after the SV sink ran
  std::size_t mixed_mon_hits = 0;  // receives the frame the SV sink publishes from its callback

  // case7 state: nested message types (XPoint inside XTrack)
  shuttle::Subscription nest_ready;
  shuttle::Subscription nest_from_sv;
  weave::Event nest_ready_ev;
  std::vector<XTrack> nest_got;
  std::vector<std::string> nest_tags;

  // case8 state: the inheritance chain XHead → XMid → XChain
  shuttle::Subscription inh_ready;
  shuttle::Subscription inh_from_sv;
  weave::Event inh_ready_ev;
  std::vector<XChain> inh_got;
  std::vector<std::string> inh_tags;
};

}  // namespace

WEAVE_TEST(xlang_tests) {
  auto st = std::make_shared<XlangState>();

  // case1: wait for the SV-side ready signal (subscribing to "xlang.sv_ready"; retained semantics
  // guarantee no loss)
  run_case(__func__, "case1", [st] {
    st->ready = shuttle::subscribe_bytes(
        kReadyTopic, [st](const std::vector<std::byte>& bytes, const shuttle::Meta& meta) {
          ++st->ready_frames;
          // the ready frame shares the business-frame wire format: two 32-bit values
          expect(bytes.size() == 8, "the ready frame wire length should be 8 bytes");
          expect(meta.type_tag == "XMsg", "the ready frame type tag comes from the SV class XMsg");
          st->ready_ev.set();
        });

    for (int i = 0; i < 200 && !st->ready_ev.is_set(); ++i) weave::sleep(100_us);
    expect(st->ready_ev.is_set(), "never saw the SV-side ready broadcast (xlang.sv_ready)");
    expect_eq(static_cast<long long>(st->ready_frames), 1, "exactly one ready frame");
  });

  // case2: C++ → SV — publish 3 frames in order, waiting for each "all SV-side callbacks finished"
  run_case(__func__, "case2", [st] {
    // subscribe first: the SV side sends back only after receiving the 3 frames, and the handle
    // must stay alive for the whole test
    st->from_sv =
        shuttle::subscribe<XMsg>(kFromSvTopic, [st](const XMsg& m, const shuttle::Meta& meta) {
          st->got.push_back(m);
          st->seqs.push_back(meta.topic_seq);
          st->tags.push_back(std::string(meta.type_tag));
        });

    shuttle::Publisher<XMsg> pub(kToSvTopic);
    for (std::uint32_t i = 1; i <= 3; ++i) {
      auto h = pub.publish_serial(XMsg{i, i * 0x100});
      expect_eq(static_cast<long long>(h.subscriber_count()), 1,
                "the SV side should have 1 blocking subscriber");
      h.wait();
      expect(h.done(), "the handle should be complete after the publish finishes");
      expect(h.failures().empty(), "the SV-side callbacks should not fail");
    }
    auto info = shuttle::broker::topic_info(kToSvTopic);
    expect(info.has_value(), "the topic should be registered");
    expect_eq(static_cast<long long>(info->published), 3, "3 frames published in total");
    expect_eq(static_cast<long long>(info->delivered), 3, "3 deliveries in total");
  });

  // case3: SV → C++ — validate the 3 frames sent back (content, type tag, monotonic seq)
  run_case(__func__, "case3", [st] {
    for (int i = 0; i < 200 && st->got.size() < 3; ++i) weave::sleep(100_us);
    expect_eq(static_cast<long long>(st->got.size()), 3, "3 frames should be received SV → C++");

    for (std::size_t i = 0; i < st->got.size(); ++i) {
      expect_eq(static_cast<long long>(st->got[i].addr), static_cast<long long>(0x51 + i),
                "the addr field is consistent across languages");
      expect_eq(static_cast<long long>(st->got[i].data), static_cast<long long>(0xA0 + i),
                "the data field is consistent across languages");
      expect(st->tags[i] == "XMsg", "type_tag is the XMsg declared on the SV side");
    }
    expect(st->seqs[0] < st->seqs[1] && st->seqs[1] < st->seqs[2],
           "the seq published from the SV side is monotonically increasing within the topic");
  });

  // case4: an SV-published frame reaches a C++ mailbox. It registers through the same nonblocking
  // delivery path as a callback subscription, so cross-language frames land in the queue unchanged;
  // the retained ready broadcast makes the assertion independent of the publish/receive order.
  run_case(__func__, "case4", [st] {
    (void)st;
    auto box = shuttle::mailbox_bytes(kReadyTopic);  // retained: replayed on subscribe
    auto item = box.recv(1_ms);
    expect(item.has_value(), "the retained SV broadcast is replayed into a new C++ mailbox");
    expect_eq(static_cast<long long>(item->value.size()), 8,
              "the ready frame keeps its wire length in the mailbox");
    expect(item->meta.type_tag == "XMsg", "type_tag survives the cross-language hop");
    expect(item->meta.topic_name == kReadyTopic, "metadata identifies the SV-published topic");
  });

  // case5: cross-language request/response — a C++ Requester calls the SV responder registered by
  // case_rq_xlang in tests/shuttle_tests.sv (it registers first and broadcasts readiness as a
  // retained frame). The SV side copies meta.corr_id into its response frame, so the C++ reply
  // table can match the answer across the language boundary.
  run_case(__func__, "case5", [] {
    auto ready = shuttle::mailbox_bytes("rq.xlang.ready");
    expect(ready.recv(50_ms).has_value(), "the SV responder announced readiness");

    shuttle::Requester<XRqReq, XRqResp> client("rq.xlang.req", "rq.xlang.resp");
    auto call = client.send_serial(XRqReq{20});
    expect_eq(static_cast<long long>(call.publication().subscriber_count()), 1,
              "the SV responder is the only target");
    auto resps = call.wait_all(10_ms);
    expect_eq(static_cast<long long>(resps.size()), 1, "the SV responder answered");
    expect_eq(static_cast<long long>(resps.at(0).data), 41, "answer computed on the SV side");
    expect_eq(static_cast<long long>(client.stale_responses()), 0,
              "the corr_id matched the live call, nothing landed as stale");
  });

  // case6: a mixed target set — SV blocking entry first, blocking C++ subscribers after it. The SV
  // sink publishes a nested frame and suspends for one time step, so a nested C++ frame runs inside
  // the outer delivery; with the frame identity not restored (weave <= 1.1.0) the *next* C++
  // callback that blocks fails with "weave: blocking primitive outside task context" and shows up
  // as a subscriber failure instead of a delivery.
  run_case(__func__, "case6", [st] {
    st->mixed_ready = shuttle::subscribe_bytes(
        kMixedReadyTopic,
        shuttle::nb([st](const std::vector<std::byte>& bytes, const shuttle::Meta& meta) {
          expect(bytes.size() == 8, "the mixed ready frame wire length should be 8 bytes");
          expect(meta.type_tag == "XMsg", "the mixed ready frame type tag comes from XMsg");
          st->mixed_ready_ev.set();
        }));
    for (int i = 0; i < 2000 && !st->mixed_ready_ev.is_set(); ++i) weave::sleep(100_us);
    expect(st->mixed_ready_ev.is_set(),
           "never saw the SV-side ready broadcast (xlang.mixed.ready) — the SV sink registers on "
           "the topic before it broadcasts, so the frame has an SV target ahead of the C++ ones");

    // Both C++ subscribers block. The victim is the one that used to blow up: it is served after
    // the SV entry, whose callback has already forced a nested C++ frame through the simulator.
    st->mixed_victim =
        shuttle::subscribe<XMsg>(kMixedTopic, [st](const XMsg&, const shuttle::Meta&) {
          ++st->mixed_hits;
          weave::sleep(1_us);
        });
    st->mixed_monitor =
        shuttle::subscribe<XMsg>(kMixedMonTopic, [st](const XMsg&, const shuttle::Meta&) {
          ++st->mixed_mon_hits;
          weave::sleep(1_us);
        });

    shuttle::Publisher<XMsg> pub(kMixedTopic);
    auto h = pub.publish_serial(XMsg{0xA1, 0xB2});
    h.wait();
    for (const auto& f : h.failures()) {
      std::string why = "unknown";
      try {
        std::rethrow_exception(f.error);
      } catch (const std::exception& e) {
        why = e.what();
      } catch (...) {
      }
      std::printf("[xlang] mixed subscriber failed: %s: %s\n", f.subscriber.c_str(), why.c_str());
    }
    expect_eq(static_cast<long long>(h.subscriber_count()), 2,
              "the mixed frame goes to the SV sink and the C++ subscriber");
    expect(h.done(), "the mixed publication should reach its terminal state");
    expect(
        h.failures().empty(),
        "no subscriber may fail when the SV entry runs ahead of the C++ ones (task identity must "
        "survive the nested frame, see weave v1.2.0)");
    expect_eq(static_cast<long long>(st->mixed_hits), 1, "the C++ subscriber blocks and completes");
    expect_eq(static_cast<long long>(st->mixed_mon_hits), 1,
              "the frame published from inside the SV callback reaches a C++ subscriber");

    // The SV sink must have been served first; that ordering is what the case is about.
    auto info = shuttle::broker::topic_info(kMixedTopic);
    expect(info.has_value(), "the mixed topic should be registered");
    expect_eq(static_cast<long long>(info->delivered), 2, "both targets were served");
  });

  // case7: nested message types — a struct containing two structs of another reflected type,
  // crossing the language boundary in both directions. The C++ frame is decoded by the SV-side
  // XTrack class (whose `shuttle_field(origin, XPoint)` entries delegate to XPoint's generated
  // codec); the SV response is decoded here by Codec<XTrack> recursing Codec<XPoint>. Handshake:
  // the SV side subscribes first and announces readiness with a retained frame, so the C++
  // publish below always has its target. Negative coordinates check two's-complement decoding on
  // both sides; the ready frame's 20-byte wire length checks the inline nested layout
  // (u32 id + 2 x (i32 x + i32 y), no length prefix).
  run_case(__func__, "case7", [st] {
    st->nest_ready = shuttle::subscribe_bytes(
        kNestReadyTopic, [st](const std::vector<std::byte>& bytes, const shuttle::Meta& meta) {
          expect(bytes.size() == 20, "the nested ready frame wire length should be 20 bytes");
          expect(meta.type_tag == "XTrack",
                 "the nested ready frame type tag comes from the SV class XTrack");
          st->nest_ready_ev.set();
        });
    for (int i = 0; i < 2000 && !st->nest_ready_ev.is_set(); ++i) weave::sleep(100_us);
    expect(st->nest_ready_ev.is_set(), "never saw the SV-side ready broadcast (xlang.nest.ready)");

    st->nest_from_sv = shuttle::subscribe<XTrack>(
        kNestFromSvTopic, [st](const XTrack& t, const shuttle::Meta& meta) {
          st->nest_got.push_back(t);
          st->nest_tags.push_back(std::string(meta.type_tag));
        });

    shuttle::Publisher<XTrack> pub(kNestToSvTopic);
    XTrack out;
    out.id = 0xC0FFEE01u;
    out.origin = XPoint{-100, 200};
    out.dest = XPoint{300, -400};
    auto h = pub.publish_serial(out);
    expect_eq(static_cast<long long>(h.subscriber_count()), 1,
              "the SV side should have 1 blocking subscriber");
    h.wait();
    expect(h.done() && h.failures().empty(), "the SV-side nested decode should not fail");

    for (int i = 0; i < 200 && st->nest_got.size() < 1; ++i) weave::sleep(100_us);
    expect_eq(static_cast<long long>(st->nest_got.size()), 1,
              "one nested frame should come back SV → C++");
    if (!st->nest_got.empty()) {
      const XTrack& t = st->nest_got[0];
      expect_eq(static_cast<long long>(t.id), 2, "the id field decoded from the SV publish");
      expect_eq(static_cast<long long>(t.origin.x), 1, "origin.x from the SV publish");
      expect_eq(static_cast<long long>(t.origin.y), -2, "origin.y negative from the SV publish");
      expect_eq(static_cast<long long>(t.dest.x), -3, "dest.x negative from the SV publish");
      expect_eq(static_cast<long long>(t.dest.y), 4, "dest.y from the SV publish");
      expect(st->nest_tags[0] == "XTrack", "type_tag is the XTrack declared on the SV side");
    }
  });

  // case8: inheritance chain — each level delegates its base subobject to the base codec
  run_case(__func__, "case8", [st] {
    st->inh_ready = shuttle::subscribe_bytes(
        kInhReadyTopic, [st](const std::vector<std::byte>& bytes, const shuttle::Meta& meta) {
          expect(bytes.size() == 7, "the chain ready frame wire length should be 4 + 2 + 1 bytes");
          expect(meta.type_tag == "XChain",
                 "the chain ready frame type tag comes from the SV class XChain");
          st->inh_ready_ev.set();
        });
    for (int i = 0; i < 2000 && !st->inh_ready_ev.is_set(); ++i) weave::sleep(100_us);
    expect(st->inh_ready_ev.is_set(),
           "never saw the SV-side ready broadcast (xlang.inherit.ready)");

    st->inh_from_sv = shuttle::subscribe<XChain>(
        kInhFromSvTopic, [st](const XChain& c, const shuttle::Meta& meta) {
          st->inh_got.push_back(c);
          st->inh_tags.push_back(std::string(meta.type_tag));
        });

    shuttle::Publisher<XChain> pub(kInhToSvTopic);
    XChain out;
    out.seq = 0xC0FFEE02u;
    out.level = -7;
    out.flag = 0x5A;
    auto h = pub.publish_serial(out);
    expect_eq(static_cast<long long>(h.subscriber_count()), 1,
              "the SV side should have 1 blocking subscriber");
    h.wait();
    expect(h.done() && h.failures().empty(), "the SV-side chain decode should not fail");

    for (int i = 0; i < 200 && st->inh_got.size() < 1; ++i) weave::sleep(100_us);
    expect_eq(static_cast<long long>(st->inh_got.size()), 1,
              "one chain frame should come back SV → C++");
    if (!st->inh_got.empty()) {
      const XChain& c = st->inh_got[0];
      expect_eq(static_cast<long long>(c.seq), 2, "the base field decoded from the SV publish");
      expect_eq(static_cast<long long>(c.level), -3,
                "the middle field, negative, from the SV publish");
      expect_eq(static_cast<long long>(c.flag), 0xA5, "the own field from the SV publish");
      expect(st->inh_tags[0] == "XChain", "type_tag is the XChain declared on the SV side");
    }
  });

  report(__func__);
}
