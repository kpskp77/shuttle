// Request/response sugar self-checks: Requester/Call on the caller side, serve()
// on the responder side — correlation carried in the frame header, demultiplexing,
// resp counting, timeouts, cancellation and stale resps.
//
// The payload types deliberately hold no handshake field: the corr_id lives in
// shuttle::Meta, so a third-party type can be used as-is.
//
// Independent namespace (rq.*), shared broker singleton.

// Suite entry: void shuttle_tests_requester() is called by tests/shuttle_tests.cpp, which owns the
// only WEAVE_TEST and the suite order; the [PASS] label is the function name.
#include <cstdint>
#include <memory>
#include <optional>
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

// ── Test message types (SHUTTLE_REFLECT can only be used at global scope) ──
struct RqReq {
  std::uint32_t op = 0;
};
SHUTTLE_REFLECT(RqReq, op)

struct RqResp {
  std::uint32_t data = 0;
};
SHUTTLE_REFLECT(RqResp, data)

void shuttle_tests_requester() {
  // case1: one responder round trip. The requester allocates the corr_id, serve() copies it into
  // the response frame, and wait_all() expects one resp per request target subscriber
  run_case(__func__, "case1", [] {
    auto server = shuttle::serve<RqReq, RqResp>(
        "rq.basic.req", "rq.basic.resp",
        [](const RqReq& req, const shuttle::Meta&) { return RqResp{req.op * 2}; });

    shuttle::Requester<RqReq, RqResp> client("rq.basic.req", "rq.basic.resp");
    auto call = client.send_serial(RqReq{21});
    expect_eq(static_cast<long long>(call.publication().subscriber_count()), 1,
              "the request targets the single responder");
    expect(call.corr_id() != 0, "the requester allocated a corr_id before publishing");

    auto resps = call.wait_all(1_ms);
    expect_eq(static_cast<long long>(resps.size()), 1, "one resp");
    expect_eq(static_cast<long long>(resps.at(0).data), 42, "the responder's answer");
    expect(call.publication().done(), "the request frame completed");
    expect_eq(static_cast<long long>(call.queued()), 0, "the resp was taken out of the slot");

    // A dropped handle frees its slot: the table only holds weak references.
    const std::size_t baseline = client.inflight();
    {
      auto abandoned = client.send_serial(RqReq{1});
      expect_eq(static_cast<long long>(client.inflight()), static_cast<long long>(baseline) + 1,
                "the live handle owns a slot");
      (void)abandoned;
    }
    expect_eq(static_cast<long long>(client.inflight()), static_cast<long long>(baseline),
              "dropping the handle releases the slot");
  });

  // case2: several requests on one Requester — distinct corr_ids, resps demultiplexed to their own
  // Call handles even though both travel on the same resp topic
  run_case(__func__, "case2", [] {
    auto server = shuttle::serve<RqReq, RqResp>(
        "rq.demux.req", "rq.demux.resp",
        [](const RqReq& req, const shuttle::Meta&) { return RqResp{req.op + 100}; });

    shuttle::Requester<RqReq, RqResp> client("rq.demux.req", "rq.demux.resp");
    auto a = client.send_serial(RqReq{1});
    auto b = client.send_serial(RqReq{2});
    expect(a.corr_id() != b.corr_id(), "each request gets its own corr_id");

    // Take them out of order on purpose: each handle only ever sees its own resp.
    auto rb = b.wait_all(1_ms);
    auto ra = a.wait_all(1_ms);
    expect_eq(static_cast<long long>(rb.size()), 1, "second call got exactly one resp");
    expect_eq(static_cast<long long>(ra.size()), 1, "first call got exactly one resp");
    expect_eq(static_cast<long long>(rb.at(0).data), 102, "b's resp belongs to b");
    expect_eq(static_cast<long long>(ra.at(0).data), 101, "a's resp belongs to a");
    expect_eq(static_cast<long long>(client.stale_responses()), 0,
              "no resp was orphaned by the demultiplexing");
  });

  // case3: two responders — wait_for(2) collects both, in completion order (concurrent resps)
  run_case(__func__, "case3", [] {
    auto first = shuttle::serve<RqReq, RqResp>(
        "rq.multi.req", "rq.multi.resp",
        [](const RqReq& req, const shuttle::Meta&) { return RqResp{req.op * 10}; });
    auto second = shuttle::serve<RqReq, RqResp>(
        "rq.multi.req", "rq.multi.resp",
        [](const RqReq& req, const shuttle::Meta&) { return RqResp{req.op * 10 + 1}; });

    shuttle::Requester<RqReq, RqResp> client("rq.multi.req", "rq.multi.resp");
    auto call = client.send_serial(RqReq{3});
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
    auto call = client.send_serial(RqReq{5});
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
          return RqResp{req.op};
        });

    shuttle::Requester<RqReq, RqResp> client("rq.cancel.req", "rq.cancel.resp");
    auto call = client.send_serial(RqReq{9});
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
    auto call = client.send_serial(RqReq{1});
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
        shuttle::nb([](const RqReq& req, const shuttle::Meta&) { return RqResp{req.op + 1}; }));

    shuttle::Requester<RqReq, RqResp> client("rq.now.req", "rq.now.resp");
    auto call = client.send_now(RqReq{41}, shuttle::NowPolicy::skip);
    auto resp = call.recv(1_ms);
    expect(resp.has_value(), "the nonblocking responder answered");
    expect(resp.has_value() && resp->data == 42, "answer content");
  });

  // case8: the corr_id never touches the payload — the responder reads meta.corr_id, so a
  // third-party payload type (no handshake field, not ours to change) works unmodified
  run_case(__func__, "case8", [] {
    std::uint32_t seen = 0;
    std::string seen_topic;
    auto server = shuttle::serve<RqReq, RqResp>("rq.header.req", "rq.header.resp",
                                                [&](const RqReq& req, const shuttle::Meta& meta) {
                                                  seen = meta.corr_id;
                                                  seen_topic = meta.topic_name;
                                                  return RqResp{req.op * 3};
                                                });

    shuttle::Requester<RqReq, RqResp> client("rq.header.req", "rq.header.resp");
    auto call = client.send_serial(RqReq{7});
    auto resps = call.wait_all(1_ms);
    expect_eq(static_cast<long long>(resps.size()), 1, "resp received");
    expect_eq(static_cast<long long>(resps.at(0).data), 21, "answer content");
    expect_eq(static_cast<long long>(seen), static_cast<long long>(call.corr_id()),
              "the responder saw the caller's corr_id in the frame header");
    expect(seen_topic == "rq.header.req", "the rest of the header is untouched");
    (void)server;
  });

  // case9: the corr_id counter is process-wide, so two independent Requesters sharing one resp
  // topic can never collide, and the value is exposed for diagnostics
  run_case(__func__, "case9", [] {
    shuttle::Requester<RqReq, RqResp> one("rq.ctr1.req", "rq.ctr.resp");
    shuttle::Requester<RqReq, RqResp> two("rq.ctr2.req", "rq.ctr.resp");
    const std::uint32_t first = one.next_corr_id();
    auto a = one.send_serial(RqReq{1});
    expect_eq(static_cast<long long>(a.corr_id()), static_cast<long long>(first),
              "next_corr_id() predicts the next allocation");
    auto b = two.send_serial(RqReq{2});
    expect_eq(static_cast<long long>(b.corr_id()), static_cast<long long>(first) + 1,
              "a second Requester draws from the same counter");
  });

  // case10: request and resp topics must differ (same-topic resps would sit behind the callback
  // that publishes them)
  run_case(__func__, "case10", [] {
    bool threw = false;
    try {
      shuttle::Requester<RqReq, RqResp> bad("rq.same", "rq.same");
      (void)bad;
    } catch (const std::invalid_argument& e) {
      threw = std::string(e.what()).find("must differ") != std::string::npos;
    }
    expect(threw, "a Requester with identical request/response topics throws");

    shuttle::Requester<RqReq, RqResp> client("rq.diag.req", "rq.diag.resp");
    auto call = client.send_serial(RqReq{1});
    expect(call.corr_id() != 0, "every call carries a corr_id");
    expect(client.req_topic() == "rq.diag.req" && client.resp_topic() == "rq.diag.resp",
           "topic accessors");
  });

  // case11: a nonblocking responder answers on the requester's stack — send_now() returns with the
  // resp already in the Call (no task switch, no simulation time spent)
  run_case(__func__, "case11", [] {
    auto server = shuttle::serve<RqReq, RqResp>(
        "rq.sync.req", "rq.sync.resp",
        shuttle::nb([](const RqReq& req, const shuttle::Meta&) { return RqResp{req.op + 1}; }));
    (void)server;

    shuttle::Requester<RqReq, RqResp> client("rq.sync.req", "rq.sync.resp");
    const std::int64_t before = weave::now().count();
    auto call = client.send_now(RqReq{41});
    expect_eq(static_cast<long long>(call.queued()), 1,
              "the resp is already in the Call when send_now returns");
    expect_eq(static_cast<long long>(weave::now().count()), static_cast<long long>(before),
              "no simulation time was spent: the round trip ran on the caller's stack");
    expect(call.publication().done(), "the request frame is complete on return");
    auto resp = call.recv();
    expect(resp.has_value() && resp->data == 42, "recv() takes it without waiting");
    expect_eq(static_cast<long long>(call.queued()), 0, "the resp was taken out of the slot");
  });

  // case12: a response topic is an ordinary topic with one delivery rule — a blocking entry on it
  // registers fine (wildcard included) but is never a reply target: the skip is counted in its
  // dropped(), while nonblocking watchers receive every reply and tell it apart by corr_id
  run_case(__func__, "case12", [] {
    auto server = shuttle::serve<RqReq, RqResp>(
        "rq.mark.req", "rq.mark.resp",
        [](const RqReq& req, const shuttle::Meta&) { return RqResp{req.op}; });
    (void)server;

    // Blocking observers register without complaint — the response topic is not special at
    // registration — but replies never reach them.
    std::size_t blocking_seen = 0;
    auto blocking = shuttle::subscribe<RqResp>(
        "rq.mark.resp", [&blocking_seen](const RqResp&, const shuttle::Meta&) { ++blocking_seen; });
    auto wildcard = shuttle::subscribe<RqResp>(
        "rq.>", [&blocking_seen](const RqResp&, const shuttle::Meta&) { ++blocking_seen; });

    // The nonblocking form is the supported way to watch responses; corr_id marks a reply.
    std::size_t seen = 0;
    bool reply_corr = false;
    auto watcher = shuttle::subscribe<RqResp>(
        "rq.mark.resp", shuttle::nb([&seen, &reply_corr](const RqResp&, const shuttle::Meta& meta) {
          ++seen;
          reply_corr = meta.corr_id != 0;
        }));

    shuttle::Requester<RqReq, RqResp> client("rq.mark.req", "rq.mark.resp");
    auto call = client.send_serial(RqReq{7});
    expect_eq(static_cast<long long>(call.publication().subscriber_count()), 1,
              "the request still targets only the responder");
    auto resps = call.wait_all(1_ms);
    expect_eq(static_cast<long long>(resps.size()), 1, "the resp still arrives");
    expect_eq(static_cast<long long>(seen), 1, "the nonblocking watcher saw the reply");
    expect(reply_corr, "the reply carries a corr_id, a hand-published frame would carry 0");
    expect_eq(static_cast<long long>(blocking_seen), 0,
              "the blocking observer never saw the reply");
    expect_eq(static_cast<long long>(blocking.dropped()), 1,
              "the skipped reply is counted for the observer");
    expect_eq(static_cast<long long>(wildcard.dropped()), 1,
              "and for the wildcard observer covering the topic");
  });

  report(__func__);
}
