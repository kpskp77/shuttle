// Request/response sugar self-checks: Requester/Call on the caller side, serve()
// on the responder side — correlation over a payload field, rid demultiplexing,
// reply counting, timeouts, cancellation and stale replies.
//
// Independent namespace (rq.*), shared broker singleton.
#include <cstdint>
#include <memory>
#include <optional>
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
struct RqReq {
  std::uint32_t rid = 0;
  std::uint32_t op = 0;
};
SHUTTLE_REFLECT(RqReq, rid, op)

struct RqResp {
  std::uint32_t rid = 0;
  std::uint32_t data = 0;
};
SHUTTLE_REFLECT(RqResp, rid, data)

// Custom correlation traits (correlate on `corr` instead of `rid`)
struct CorrReq {
  std::uint32_t corr = 0;
  std::uint32_t op = 0;
};
SHUTTLE_REFLECT(CorrReq, corr, op)

struct CorrResp {
  std::uint32_t corr = 0;
  std::uint32_t data = 0;
};
SHUTTLE_REFLECT(CorrResp, corr, data)

struct CorrTraits {
  static std::uint32_t get_rid(const CorrReq& m) { return m.corr; }
  static void set_rid(CorrReq& m, std::uint32_t v) { m.corr = v; }
  static std::uint32_t get_rid(const CorrResp& m) { return m.corr; }
  static void set_rid(CorrResp& m, std::uint32_t v) { m.corr = v; }
};

WEAVE_TEST(requester_tests) {
  // case1: one responder round trip. The requester allocates the rid, serve() echoes it, and
  // wait_all() expects one reply per request target subscriber
  run_case(__func__, "case1", [] {
    auto server = shuttle::serve<RqReq, RqResp>(
        "rq.basic.req", "rq.basic.resp",
        [](const RqReq& req, const shuttle::Meta&) { return RqResp{0, req.op * 2}; });

    shuttle::Requester<RqReq, RqResp> client("rq.basic.req", "rq.basic.resp");
    auto call = client.send(RqReq{0, 21});
    expect_eq(static_cast<long long>(call.publication().subscriber_count()), 1,
              "the request targets the single responder");
    expect_eq(static_cast<long long>(call.rid()), 1, "the requester allocates rid 1");

    auto replies = call.wait_all(1_ms);
    expect_eq(static_cast<long long>(replies.size()), 1, "one reply");
    expect_eq(static_cast<long long>(replies.at(0).data), 42, "the responder's answer");
    expect_eq(static_cast<long long>(replies.at(0).rid), 1, "serve echoed the rid");
    expect(call.publication().done(), "the request frame completed");
    expect_eq(static_cast<long long>(call.queued()), 0, "the reply was taken out of the slot");

    // A dropped handle frees its slot: the table only holds weak references.
    const std::size_t baseline = client.inflight();
    {
      auto abandoned = client.send(RqReq{0, 1});
      expect_eq(static_cast<long long>(client.inflight()),
                static_cast<long long>(baseline) + 1, "the live handle owns a slot");
      (void)abandoned;
    }
    expect_eq(static_cast<long long>(client.inflight()), static_cast<long long>(baseline),
              "dropping the handle releases the slot");
  });

  // case2: several requests on one Requester — distinct rids, replies demultiplexed to their own
  // Call handles even though both travel on the same reply topic
  run_case(__func__, "case2", [] {
    auto server = shuttle::serve<RqReq, RqResp>(
        "rq.demux.req", "rq.demux.resp",
        [](const RqReq& req, const shuttle::Meta&) { return RqResp{0, req.op + 100}; });

    shuttle::Requester<RqReq, RqResp> client("rq.demux.req", "rq.demux.resp");
    auto a = client.send(RqReq{0, 1});
    auto b = client.send(RqReq{0, 2});
    expect(a.rid() != b.rid(), "each request gets its own rid");

    // Take them out of order on purpose: each handle only ever sees its own rid.
    auto rb = b.wait_all(1_ms);
    auto ra = a.wait_all(1_ms);
    expect_eq(static_cast<long long>(rb.size()), 1, "second call got exactly one reply");
    expect_eq(static_cast<long long>(ra.size()), 1, "first call got exactly one reply");
    expect_eq(static_cast<long long>(rb.at(0).data), 102, "b's reply belongs to b");
    expect_eq(static_cast<long long>(ra.at(0).data), 101, "a's reply belongs to a");
    expect_eq(static_cast<long long>(ra.at(0).rid), a.rid(), "a's rid came back");
    expect_eq(static_cast<long long>(rb.at(0).rid), b.rid(), "b's rid came back");
  });

  // case3: two responders — wait_for(2) collects both, in completion order (concurrent replies)
  run_case(__func__, "case3", [] {
    auto first = shuttle::serve<RqReq, RqResp>(
        "rq.multi.req", "rq.multi.resp",
        [](const RqReq& req, const shuttle::Meta&) { return RqResp{0, req.op * 10}; });
    auto second = shuttle::serve<RqReq, RqResp>(
        "rq.multi.req", "rq.multi.resp",
        [](const RqReq& req, const shuttle::Meta&) { return RqResp{0, req.op * 10 + 1}; });

    shuttle::Requester<RqReq, RqResp> client("rq.multi.req", "rq.multi.resp");
    auto call = client.send(RqReq{0, 3});
    expect_eq(static_cast<long long>(call.publication().subscriber_count()), 2,
              "two responders are targets");
    auto replies = call.wait_all(1_ms);  // n = subscriber_count() = 2
    expect_eq(static_cast<long long>(replies.size()), 2, "both responders answered");
    std::uint32_t sum = 0;
    for (const auto& r : replies) sum += r.data;
    expect_eq(static_cast<long long>(sum), 61, "both answers arrived (30 + 31)");
  });

  // case4: no responder — recv/wait_all time out without hanging, and the handle stays usable
  run_case(__func__, "case4", [] {
    shuttle::Requester<RqReq, RqResp> client("rq.none.req", "rq.none.resp");
    auto call = client.send(RqReq{0, 5});
    expect(!call.recv(200_us).has_value(), "recv times out when nobody answers");
    expect(call.wait_all(100_us).empty(), "wait_all times out with no replies");
    expect_eq(static_cast<long long>(call.publication().subscriber_count()), 0,
              "zero targets: the request frame completes immediately");
  });

  // case5: cancel() frees the slot; a reply arriving afterwards is counted as stale and never
  // reaches the handle
  run_case(__func__, "case5", [] {
    weave::Event gate;
    auto server = shuttle::serve<RqReq, RqResp>(
        "rq.cancel.req", "rq.cancel.resp",
        [&gate](const RqReq& req, const shuttle::Meta&) {
          gate.wait();  // answer only once the test opens the gate
          return RqResp{0, req.op};
        });

    shuttle::Requester<RqReq, RqResp> client("rq.cancel.req", "rq.cancel.resp");
    auto call = client.send(RqReq{0, 9});
    expect_eq(static_cast<long long>(client.inflight()), 1, "one call in flight");
    call.cancel();
    expect(call.cancelled(), "cancel() marks the handle");
    expect_eq(static_cast<long long>(client.inflight()), 0, "the slot is released");
    expect(!call.recv(100_us).has_value(), "a cancelled handle never receives");

    gate.set();  // the responder answers now: the reply has no live call
    auto req_pub = call.publication();
    req_pub.wait();
    for (int i = 0; i < 50 && client.stale_replies() == 0; ++i) weave::sleep(100_us);
    expect_eq(static_cast<long long>(client.stale_replies()), 1,
              "the reply for the cancelled call is counted as stale");
    (void)server;
  });

  // case6: a responder whose handler throws is recorded as a delivery failure on the request frame
  run_case(__func__, "case6", [] {
    auto bad = shuttle::serve<RqReq, RqResp>(
        "rq.throw.req", "rq.throw.resp",
        [](const RqReq&, const shuttle::Meta&) -> RqResp {
          throw std::runtime_error("responder failed");
        });

    shuttle::Requester<RqReq, RqResp> client("rq.throw.req", "rq.throw.resp");
    auto call = client.send(RqReq{0, 1});
    call.publication().wait();
    expect_eq(static_cast<long long>(call.publication().failures().size()), 1,
              "the throwing responder is recorded as a failure");
    expect(call.wait_all(200_us).empty(), "no reply from the failing responder");
    (void)bad;
  });

  // case7: a nonblocking responder works with the synchronous flavor (send_now): the reply is queued
  // by the time send_now returns, because the nonblocking entry runs on the publisher's stack
  run_case(__func__, "case7", [] {
    auto fast = shuttle::serve<RqReq, RqResp>(
        "rq.now.req", "rq.now.resp",
        [](const RqReq& req, const shuttle::Meta&) { return RqResp{0, req.op + 1}; },
        shuttle::Capability::nonblocking);

    shuttle::Requester<RqReq, RqResp> client("rq.now.req", "rq.now.resp");
    auto call = client.send_now(RqReq{0, 41}, shuttle::NowPolicy::skip);
    auto reply = call.recv(1_ms);
    expect(reply.has_value(), "the nonblocking responder answered");
    expect(reply.has_value() && reply->data == 42, "answer content");
  });

  // case8: custom correlation traits compensate for a payload without a `rid` field
  run_case(__func__, "case8", [] {
    auto server = shuttle::serve<CorrReq, CorrResp, CorrTraits>(
        "rq.traits.req", "rq.traits.resp",
        [](const CorrReq& req, const shuttle::Meta&) { return CorrResp{0, req.op * 3}; });

    shuttle::Requester<CorrReq, CorrResp, CorrTraits> client("rq.traits.req", "rq.traits.resp");
    auto call = client.send(CorrReq{0, 7});
    auto replies = call.wait_all(1_ms);
    expect_eq(static_cast<long long>(replies.size()), 1, "reply received through the custom traits");
    expect_eq(static_cast<long long>(replies.at(0).data), 21, "answer content");
    expect_eq(static_cast<long long>(replies.at(0).corr), static_cast<long long>(call.rid()),
              "the custom correlation field carries the rid");
    (void)server;
  });

  // case9: the request payload handed to the responder carries the requester's rid (that is what
  // lets an SV responder echo it), and the caller's own copy is untouched
  run_case(__func__, "case9", [] {
    std::uint32_t seen_rid = 0;
    auto server = shuttle::serve<RqReq, RqResp>(
        "rq.echo.req", "rq.echo.resp", [&seen_rid](const RqReq& req, const shuttle::Meta&) {
          seen_rid = req.rid;
          return RqResp{0, req.op};
        });

    shuttle::Requester<RqReq, RqResp> client("rq.echo.req", "rq.echo.resp");
    RqReq mine{0, 5};
    auto call = client.send(mine);
    call.wait_all(1_ms);
    expect_eq(static_cast<long long>(seen_rid), static_cast<long long>(call.rid()),
              "the responder saw the allocated rid");
    expect_eq(static_cast<long long>(mine.rid), 0, "the caller's message is not modified");
    (void)server;
  });

  // case10: request and reply topics must differ (same-topic replies would sit behind the callback
  // that publishes them), and the rid counter is exposed for diagnostics
  run_case(__func__, "case10", [] {
    bool threw = false;
    try {
      shuttle::Requester<RqReq, RqResp> bad("rq.same", "rq.same");
      (void)bad;
    } catch (const std::invalid_argument& e) {
      threw = std::string(e.what()).find("must differ") != std::string::npos;
    }
    expect(threw, "a Requester with identical request/reply topics throws");

    shuttle::Requester<RqReq, RqResp> client("rq.diag.req", "rq.diag.resp");
    const std::uint32_t first = client.next_rid();
    auto call = client.send(RqReq{0, 1});
    expect_eq(static_cast<long long>(call.rid()), static_cast<long long>(first),
              "next_rid() predicts the next allocation");
    expect_eq(static_cast<long long>(client.next_rid()), static_cast<long long>(first) + 1,
              "the counter advances");
    expect(client.request_topic() == "rq.diag.req" && client.reply_topic() == "rq.diag.resp",
           "topic accessors");
  });

  report(__func__);
}
