// Cross-language round-trip self-checks (svdpi backend only): C++ → SV and SV → C++
// publish/subscribe interoperability.
//
// Paired with the xlang section of tests/sv/shuttle_tests.sv; the handshake order is:
//   1. the SV side first subscribes for C++→SV, then publishes one frame on "xlang.sv_ready"
//   (retained);
//   2. the C++ side subscribes to "xlang.sv_ready" (retained guarantees delivery even if SV
//   published first),
//      then publishes 3 frames in order on "xlang.cpp2sv" and waits for each — completion means all
//      3 SV-side callbacks have already finished (that is exactly the Publication completion
//      semantics);
//   3. once the SV side confirms the 3 frames, it publishes 3 frames on "xlang.sv2cpp";
//   4. the C++ side waits for and validates these 3 frames (content, type_tag, monotonic seq).
//
// After both sides finish their assertions, run_cpp_tests in tests/sv/shuttle_tests.sv renders the
// verdict.

#include <cstdint>
#include <cstdio>
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
// SvRqReq/SvRqResp in tests/sv/shuttle_tests.sv; the type_tag must match the SHUTTLE_REFLECT
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
// Byte-for-byte counterpart of `class XMsg` in tests/sv/shuttle_tests.sv:
//   {>>{addr, data}} — two unsigned 32-bit values, big-endian; both tags are "XMsg".
struct XMsg {
  std::uint32_t addr = 0;
  std::uint32_t data = 0;
};
SHUTTLE_REFLECT(XMsg, addr, data)

namespace {

constexpr const char* kReadyTopic = "xlang.sv_ready";  // SV → C++ ready broadcast (retained)
constexpr const char* kToSvTopic = "xlang.cpp2sv";     // C++ → SV
constexpr const char* kFromSvTopic = "xlang.sv2cpp";   // SV → C++

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
          st->tags.push_back(meta.type_tag);
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
  // case_rq_xlang in tests/sv/shuttle_tests.sv (it registers first and broadcasts readiness as a
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

  report(__func__);
}
