// Request/response sugar self-checks: Requester/Call on the caller side, serve()
// on the responder side — correlation over a payload field, req_id demultiplexing,
// resp counting, timeouts, cancellation and stale resps.
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
  std::uint32_t req_id = 0;
  std::uint32_t op = 0;
};
SHUTTLE_REFLECT(RqReq, req_id, op)

struct RqResp {
  std::uint32_t req_id = 0;
  std::uint32_t data = 0;
};
SHUTTLE_REFLECT(RqResp, req_id, data)

// Custom correlation traits (correlate on `corr` instead of `req_id`)
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
  static std::uint32_t get_req_id(const CorrReq& m) { return m.corr; }
  static void set_req_id(CorrReq& m, std::uint32_t v) { m.corr = v; }
  static std::uint32_t get_req_id(const CorrResp& m) { return m.corr; }
  static void set_req_id(CorrResp& m, std::uint32_t v) { m.corr = v; }
};

WEAVE_TEST(requester_tests) {
  // case1: one responder round trip. The requester allocates the req_id, serve() echoes it, and
  // wait_all() expects one resp per request target subscriber
  run_case(__func__, "case1", [] {
    auto server = shuttle::serve<RqReq, RqResp>(
        "rq.basic.req", "rq.basic.resp",
        [](const RqReq& req, const shuttle::Meta&) { return RqResp{0, req.op * 2}; });

    shuttle::Requester<RqReq, RqResp> client("rq.basic.req", "rq.basic.resp");
    auto call = client.send_serial(RqReq{0, 21});
    expect_eq(static_cast<long long>(call.publication().subscriber_count()), 1,
              "the request targets the single responder");
    expect_eq(static_cast<long long>(call.req_id()), 1, "the requester allocates req_id 1");

    auto resps = call.wait_all(1_ms);
    expect_eq(static_cast<long long>(resps.size()), 1, "one reply");
    expect_eq(static_cast<long long>(resps.at(0).data), 42, "the responder's answer");
    expect_eq(static_cast<long long>(resps.at(0).req_id), 1, "serve echoed the req_id");
    expect(call.publication().done(), "the request frame completed");
    expect_eq(static_cast<long long>(call.queued()), 0, "the reply was taken out of the slot");

    // A dropped handle frees its slot: the table only holds weak references.
    const std::size_t baseline = client.inflight();
    {
      auto abandoned = client.send_serial(RqReq{0, 1});
      expect_eq(static_cast<long long>(client.inflight()), static_cast<long long>(baseline) + 1,
                "the live handle owns a slot");
      (void)abandoned;
    }
    expect_eq(static_cast<long long>(client.inflight()), static_cast<long long>(baseline),
              "dropping the handle releases the slot");
  });

  // case2: several requests on one Requester — distinct req_ids, resps demultiplexed to their own
  // Call handles even though both travel on the same resp topic
  run_case(__func__, "case2", [] {
    auto server = shuttle::serve<RqReq, RqResp>(
        "rq.demux.req", "rq.demux.resp",
        [](const RqReq& req, const shuttle::Meta&) { return RqResp{0, req.op + 100}; });

    shuttle::Requester<RqReq, RqResp> client("rq.demux.req", "rq.demux.resp");
    auto a = client.send_serial(RqReq{0, 1});
    auto b = client.send_serial(RqReq{0, 2});
    expect(a.req_id() != b.req_id(), "each request gets its own req_id");

    // Take them out of order on purpose: each handle only ever sees its own req_id.
    auto rb = b.wait_all(1_ms);
    auto ra = a.wait_all(1_ms);
    expect_eq(static_cast<long long>(rb.size()), 1, "second call got exactly one reply");
    expect_eq(static_cast<long long>(ra.size()), 1, "first call got exactly one reply");
    expect_eq(static_cast<long long>(rb.at(0).data), 102, "b's reply belongs to b");
    expect_eq(static_cast<long long>(ra.at(0).data), 101, "a's reply belongs to a");
    expect_eq(static_cast<long long>(ra.at(0).req_id), a.req_id(), "a's req_id came back");
    expect_eq(static_cast<long long>(rb.at(0).req_id), b.req_id(), "b's req_id came back");
  });

  // case3: two responders — wait_for(2) collects both, in completion order (concurrent resps)
  run_case(__func__, "case3", [] {
    auto first = shuttle::serve<RqReq, RqResp>(
        "rq.multi.req", "rq.multi.resp",
        [](const RqReq& req, const shuttle::Meta&) { return RqResp{0, req.op * 10}; });
    auto second = shuttle::serve<RqReq, RqResp>(
        "rq.multi.req", "rq.multi.resp",
        [](const RqReq& req, const shuttle::Meta&) { return RqResp{0, req.op * 10 + 1}; });

    shuttle::Requester<RqReq, RqResp> client("rq.multi.req", "rq.multi.resp");
    auto call = client.send_serial(RqReq{0, 3});
    expect_eq(static_cast<long long>(call.publication().subscriber_count()), 2,
              "two responders are targets");
    auto resps = call.wait_all(1_ms);  // n = subscriber_count() = 2
    expect_eq(static_cast<long long>(resps.size()), 2, "both responders answered");
    std::uint32_t sum = 0;
    for (const auto& r : resps) sum += r.data;
    expect_eq(static_cast<long long>(sum), 61, "both answers arrived (30 + 31)");
  });

  // case4: no responder — recv/wait_all time out without hanging, and the handle stays usable
  run_case(__func__, "case4", [] {
    shuttle::Requester<RqReq, RqResp> client("rq.none.req", "rq.none.resp");
    auto call = client.send_serial(RqReq{0, 5});
    expect(!call.recv(200_us).has_value(), "recv times out when nobody answers");
    expect(call.wait_all(100_us).empty(), "wait_all times out with no replies");
    expect_eq(static_cast<long long>(call.publication().subscriber_count()), 0,
              "zero targets: the request frame completes immediately");
  });

  // case5: cancel() frees the slot; a resp arriving afterwards is counted as stale and never
  // reaches the handle
  run_case(__func__, "case5", [] {
    weave::Event gate;
    auto server = shuttle::serve<RqReq, RqResp>(
        "rq.cancel.req", "rq.cancel.resp", [&gate](const RqReq& req, const shuttle::Meta&) {
          gate.wait();  // answer only once the test opens the gate
          return RqResp{0, req.op};
        });

    shuttle::Requester<RqReq, RqResp> client("rq.cancel.req", "rq.cancel.resp");
    auto call = client.send_serial(RqReq{0, 9});
    expect_eq(static_cast<long long>(client.inflight()), 1, "one call in flight");
    call.cancel();
    expect(call.cancelled(), "cancel() marks the handle");
    expect_eq(static_cast<long long>(client.inflight()), 0, "the slot is released");
    expect(!call.recv(100_us).has_value(), "a cancelled handle never receives");

    gate.set();  // the responder answers now: the resp has no live call
    auto pub = call.publication();
    pub.wait();
    for (int i = 0; i < 50 && client.stale_responses() == 0; ++i) weave::sleep(100_us);
    expect_eq(static_cast<long long>(client.stale_responses()), 1,
              "the reply for the cancelled call is counted as stale");
    (void)server;
  });

  // case6: a responder whose handler throws is recorded as a delivery failure on the request frame
  run_case(__func__, "case6", [] {
    auto bad = shuttle::serve<RqReq, RqResp>("rq.throw.req", "rq.throw.resp",
                                             [](const RqReq&, const shuttle::Meta&) -> RqResp {
                                               throw std::runtime_error("responder failed");
                                             });

    shuttle::Requester<RqReq, RqResp> client("rq.throw.req", "rq.throw.resp");
    auto call = client.send_serial(RqReq{0, 1});
    call.publication().wait();
    expect_eq(static_cast<long long>(call.publication().failures().size()), 1,
              "the throwing responder is recorded as a failure");
    expect(call.wait_all(200_us).empty(), "no reply from the failing responder");
    (void)bad;
  });

  // case7: a nonblocking responder works with the synchronous flavor (send_now): the resp is
  // queued by the time send_now returns, because the nonblocking entry runs on the publisher's
  // stack
  run_case(__func__, "case7", [] {
    auto fast = shuttle::serve<RqReq, RqResp>(
        "rq.now.req", "rq.now.resp",
        shuttle::nb([](const RqReq& req, const shuttle::Meta&) { return RqResp{0, req.op + 1}; }));

    shuttle::Requester<RqReq, RqResp> client("rq.now.req", "rq.now.resp");
    auto call = client.send_now(RqReq{0, 41}, shuttle::NowPolicy::skip);
    auto resp = call.recv(1_ms);
    expect(resp.has_value(), "the nonblocking responder answered");
    expect(resp.has_value() && resp->data == 42, "answer content");
  });

  // case8: custom correlation traits compensate for a payload without a `req_id` field
  run_case(__func__, "case8", [] {
    auto server = shuttle::serve<CorrReq, CorrResp, CorrTraits>(
        "rq.traits.req", "rq.traits.resp",
        [](const CorrReq& req, const shuttle::Meta&) { return CorrResp{0, req.op * 3}; });

    shuttle::Requester<CorrReq, CorrResp, CorrTraits> client("rq.traits.req", "rq.traits.resp");
    auto call = client.send_serial(CorrReq{0, 7});
    auto resps = call.wait_all(1_ms);
    expect_eq(static_cast<long long>(resps.size()), 1, "reply received through the custom traits");
    expect_eq(static_cast<long long>(resps.at(0).data), 21, "answer content");
    expect_eq(static_cast<long long>(resps.at(0).corr), static_cast<long long>(call.req_id()),
              "the custom correlation field carries the req_id");
    (void)server;
  });

  // case9: the request payload handed to the responder carries the requester's req_id (that is what
  // lets an SV responder echo it), and the caller's own copy is untouched
  run_case(__func__, "case9", [] {
    std::uint32_t seen_req_id = 0;
    auto server = shuttle::serve<RqReq, RqResp>(
        "rq.echo.req", "rq.echo.resp", [&seen_req_id](const RqReq& req, const shuttle::Meta&) {
          seen_req_id = req.req_id;
          return RqResp{0, req.op};
        });

    shuttle::Requester<RqReq, RqResp> client("rq.echo.req", "rq.echo.resp");
    RqReq mine{0, 5};
    auto call = client.send_serial(mine);
    call.wait_all(1_ms);
    expect_eq(static_cast<long long>(seen_req_id), static_cast<long long>(call.req_id()),
              "the responder saw the allocated req_id");
    expect_eq(static_cast<long long>(mine.req_id), 0, "the caller's message is not modified");
    (void)server;
  });

  // case10: request and resp topics must differ (same-topic resps would sit behind the callback
  // that publishes them), and the req_id counter is exposed for diagnostics
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
    const std::uint32_t first = client.next_req_id();
    auto call = client.send_serial(RqReq{0, 1});
    expect_eq(static_cast<long long>(call.req_id()), static_cast<long long>(first),
              "next_req_id() predicts the next allocation");
    expect_eq(static_cast<long long>(client.next_req_id()), static_cast<long long>(first) + 1,
              "the counter advances");
    expect(client.req_topic() == "rq.diag.req" && client.resp_topic() == "rq.diag.resp",
           "topic accessors");
  });

  report(__func__);
}
