// Mailbox self-checks: a mailbox is a subscription with a queue — synchronous
// fill by publish_now, background fill by serial/concurrent, receive with and
// without a deadline, capacity/overflow policies, close semantics, retained
// replay, wildcard patterns, and the request/response round trip it enables.
//
// Independent namespace (mbx.*), shared broker singleton: every assertion only
// refers to topics owned by its own case.
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
struct MbxReq {
  std::uint32_t op = 0;
};
SHUTTLE_REFLECT(MbxReq, op)

struct MbxResp {
  std::uint32_t data = 0;
};
SHUTTLE_REFLECT(MbxResp, data)

WEAVE_TEST(mailbox_tests) {
  // case1: a mailbox is a nonblocking subscriber, so publish_now fills it synchronously on the
  // publisher's stack — no background scheduling, no CapabilityError, no defer
  run_case(__func__, "case1", [] {
    auto box = shuttle::mailbox<MbxResp>("mbx.now");
    shuttle::Publisher<MbxResp> pub("mbx.now");
    expect_eq(static_cast<long long>(box.size()), 0, "a fresh mailbox is empty");

    auto h = pub.publish_now(MbxResp{0xAA});
    expect(h.done(), "zero blocking targets: the frame completes synchronously");
    expect_eq(static_cast<long long>(box.size()), 1,
              "publish_now filled the mailbox before returning");
    auto item = box.try_recv();
    expect(item.has_value(), "the frame is already queued");
    expect_eq(static_cast<long long>(item->value.data), 0xAA, "payload unchanged");
    expect(item->meta.topic_name == "mbx.now", "metadata travels with the frame");
    expect_eq(static_cast<long long>(item->meta.topic_seq), 1, "the frame carries the topic's seq");
    expect(!box.try_recv().has_value(), "the queue is drained");
  });

  // case2: serial/concurrent fill it from the background; Publication::wait() guarantees the frame
  // has been queued (delivered means the callback returned, and the callback is the push)
  run_case(__func__, "case2", [] {
    auto box = shuttle::mailbox<MbxResp>("mbx.serial");
    shuttle::Publisher<MbxResp> pub("mbx.serial");

    auto hs = pub.publish_serial(MbxResp{11});
    expect_eq(static_cast<long long>(hs.subscriber_count()), 1,
              "a mailbox is an ordinary target subscriber");
    hs.wait();
    auto first = box.try_recv();
    expect(first.has_value() && first->value.data == 11, "serial filled the mailbox");

    auto hc = pub.publish_concurrent(MbxResp{22});
    hc.wait();
    expect(box.try_recv().has_value(), "concurrent filled the mailbox too");
  });

  // case3: ordering — the queue order is the callback invocation order (serial = join order)
  run_case(__func__, "case3", [] {
    auto box = shuttle::mailbox<MbxResp>("mbx.order");
    shuttle::Publisher<MbxResp> pub("mbx.order");
    std::vector<std::uint32_t> got;

    for (std::uint32_t i = 1; i <= 3; ++i) pub.publish_serial(MbxResp{i}).wait();
    while (auto item = box.try_recv()) got.push_back(item->value.data);
    expect_eq(static_cast<long long>(got.size()), 3, "three frames queued");
    expect(got[0] == 1 && got[1] == 2 && got[2] == 3, "serial keeps publish/join order");
  });

  // case4: recv(timeout) returns nullopt on timeout and loses nothing — a frame arriving after the
  // deadline is still queued
  run_case(__func__, "case4", [] {
    auto box = shuttle::mailbox<MbxResp>("mbx.timeout");
    shuttle::Publisher<MbxResp> pub("mbx.timeout");

    expect(!box.recv(50_us).has_value(), "nothing published: the wait times out");
    expect_eq(static_cast<long long>(box.size()), 0, "a timeout consumes nothing");

    pub.publish_serial(MbxResp{7}).wait();
    auto late = box.recv(1_ms);
    expect(late.has_value(), "a frame published after the timeout is still received");
    expect_eq(static_cast<long long>(late->value.data), 7, "and it is the expected frame");
  });

  // case5: a bounded mailbox needs an explicit drop policy; OverflowPolicy::block is rejected at
  // construction (the delivery callback may run inside publish_now and must never block)
  run_case(__func__, "case5", [] {
    shuttle::SubOptions bad;
    bad.max_pending = 2;  // overflow defaults to block
    bool threw = false;
    try {
      auto box = shuttle::mailbox<MbxResp>("mbx.block", bad);
      (void)box;
    } catch (const std::invalid_argument& e) {
      threw = std::string(e.what()).find("must not block") != std::string::npos;
    }
    expect(threw, "a bounded mailbox with OverflowPolicy::block throws at construction");
  });

  // case6: drop_oldest / drop_newest accounting; overflow is a queue-level loss, distinct from
  // Subscription::dropped()
  run_case(__func__, "case6", [] {
    shuttle::SubOptions opts;
    opts.max_pending = 2;
    opts.overflow = shuttle::OverflowPolicy::drop_oldest;
    auto box = shuttle::mailbox<MbxResp>("mbx.dropold", opts);
    shuttle::Publisher<MbxResp> pub("mbx.dropold");

    for (std::uint32_t i = 1; i <= 4; ++i) pub.publish_now(MbxResp{i});
    expect_eq(static_cast<long long>(box.size()), 2, "the queue is bounded");
    expect_eq(static_cast<long long>(box.received()), 4, "all four frames reached the mailbox");
    expect_eq(static_cast<long long>(box.overflowed()), 2, "two were dropped");
    expect_eq(static_cast<long long>(box.subscription().received()), 4,
              "at the delivery level all four were delivered");
    auto a = box.try_recv();
    expect(a.has_value() && a->value.data == 3, "drop_oldest keeps the newest two");
  });

  // case7: OverflowPolicy::reject surfaces as a normal delivery failure, not as a silent drop
  run_case(__func__, "case7", [] {
    shuttle::SubOptions opts;
    opts.max_pending = 1;
    opts.overflow = shuttle::OverflowPolicy::reject;
    auto box = shuttle::mailbox<MbxResp>("mbx.reject", opts);
    shuttle::Publisher<MbxResp> pub("mbx.reject");

    pub.publish_serial(MbxResp{1}).wait();
    auto bad = pub.publish_serial(MbxResp{2});
    bad.wait();
    expect_eq(static_cast<long long>(bad.failures().size()), 1,
              "the overflowing delivery is recorded as a subscriber failure");
    expect_eq(static_cast<long long>(box.size()), 1, "the first frame is untouched");
    expect_eq(static_cast<long long>(box.overflowed()), 0, "reject is not a queue-level drop");
  });

  // case8: close() wakes a blocked receiver; queued frames stay readable and only then does the
  // blocking form throw (drain-then-throw)
  run_case(__func__, "case8", [] {
    auto box = shuttle::mailbox<MbxResp>("mbx.close");
    shuttle::Publisher<MbxResp> pub("mbx.close");
    pub.publish_serial(MbxResp{5}).wait();

    bool closed_threw = false;
    auto waiter = weave::create_task("mbx-waiter", [&] {
      try {
        (void)box.recv();  // queue is non-empty: returns without blocking
        (void)box.recv();  // empty and closed: throws
      } catch (const shuttle::MailboxClosed&) {
        closed_threw = true;
      }
    });
    weave::sleep(1_us);
    box.close();
    waiter.await();
    expect(closed_threw, "receiving from a drained, closed mailbox throws MailboxClosed");
    expect(!box.try_recv().has_value(), "try_recv stays non-throwing after close");
    expect(box.closed(), "closed() reports the state");
    expect_eq(static_cast<long long>(box.received()), 1, "counters stay readable after close");
  });

  // case9: close() while a receiver is blocked on an empty mailbox wakes it up (no lost wakeup)
  run_case(__func__, "case9", [] {
    auto box = shuttle::mailbox<MbxResp>("mbx.closewait");
    bool closed_threw = false;
    auto waiter = weave::create_task("mbx-close-waiter", [&] {
      try {
        (void)box.recv();
      } catch (const shuttle::MailboxClosed&) {
        closed_threw = true;
      }
    });
    weave::sleep(10_us);
    box.close();
    waiter.await();
    expect(closed_threw, "a receiver blocked on an empty mailbox is woken by close()");
  });

  // case10: retained replay reaches a mailbox created afterwards (it is an ordinary subscriber)
  run_case(__func__, "case10", [] {
    shuttle::Publisher<MbxResp> pub("mbx.retained");
    pub.set_retained(true);
    pub.publish_now(MbxResp{42});

    auto box = shuttle::mailbox<MbxResp>("mbx.retained");
    auto item = box.try_recv();
    expect(item.has_value(), "a new mailbox receives the retained frame");
    expect(item.has_value() && item->value.data == 42, "retained payload delivered");
  });

  // case11: wildcard pattern — one mailbox collects several topics, meta.topic_name tells them
  // apart
  run_case(__func__, "case11", [] {
    auto box = shuttle::mailbox<MbxResp>("mbx.wild.>");
    shuttle::Publisher<MbxResp> a("mbx.wild.a");
    shuttle::Publisher<MbxResp> b("mbx.wild.b");
    a.publish_now(MbxResp{1});
    b.publish_now(MbxResp{2});

    auto first = box.try_recv();
    auto second = box.try_recv();
    expect(first.has_value() && second.has_value(), "both topics reach the mailbox");
    expect(first->meta.topic_name == "mbx.wild.a" && second->meta.topic_name == "mbx.wild.b",
           "metadata identifies the publishing topic");
  });

  // case12: copies of a handle share one queue; the byte flavor works without a codec on the
  // subscriber side
  run_case(__func__, "case12", [] {
    auto box = shuttle::mailbox_bytes("mbx.bytes");
    auto alias = box;  // shared semantics
    shuttle::Publisher<MbxResp> pub("mbx.bytes");
    pub.publish_now(MbxResp{0x01020304});

    expect_eq(static_cast<long long>(alias.size()), 1, "copies share the queue");
    auto item = alias.try_recv();
    expect(item.has_value(), "the byte mailbox received the frame");
    // MbxResp is one uint32 field: 4 bytes, big-endian
    expect_eq(static_cast<long long>(item->value.size()), 4, "wire length of MbxResp");
    expect(static_cast<unsigned char>(item->value[0]) == 0x01 &&
               static_cast<unsigned char>(item->value[3]) == 0x04,
           "big-endian wire bytes, no decoding");
    expect_eq(static_cast<long long>(box.size()), 0, "both handles see the same drained queue");
  });

  // case13: request/response round trip over mailboxes — one request in flight needs no correlation
  // at all; only Requester/serve allocate a corr_id, and a plain publish leaves it 0
  run_case(__func__, "case13", [] {
    auto resps = shuttle::mailbox<MbxResp>("mbx.rr.resp");
    shuttle::Publisher<MbxResp> responder_pub("mbx.rr.resp");
    auto sub =
        shuttle::subscribe<MbxReq>("mbx.rr.req", [&](const MbxReq& req, const shuttle::Meta&) {
          responder_pub.publish_serial(MbxResp{req.op * 2});
        });

    shuttle::Publisher<MbxReq> req_pub("mbx.rr.req");
    auto req = req_pub.publish_serial(MbxReq{21});
    req.wait();  // the responder's callback has returned => the resp is published
    expect(req.done() && req.failures().empty(), "request delivered without failures");

    auto resp = resps.recv(1_ms);
    expect(resp.has_value(), "the resp arrived");
    expect(resp.has_value() && resp->value.data == 42, "the responder's answer is visible");
    expect(resp.has_value() && resp->meta.corr_id == 0,
           "a plain publish leaves corr_id 0 (only Requester/serve allocate one)");
  });

  report(__func__);
}
