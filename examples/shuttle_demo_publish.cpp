// Publish demo: subscriptions with both callback capabilities, the three publish flavors,
// retained, wildcards, introspection and multi-publisher.
//
// Subject file of the C++ half of the uart demo (the SV mirror is
// examples/shuttle_demo_<subject>.sv): exposes void shuttle_demo_<subject>(), called in
// order by examples/shuttle_demo.cpp. Independent of the other subjects; see
// examples/shuttle_demo_msgs.hpp for the message types.

#include <cstdint>
#include <cstdio>
#include <shuttle/shuttle.hpp>
#include <stdexcept>
#include <string>
#include <vector>
#include <weave/weave.hpp>

#include "shuttle_demo_msgs.hpp"

using namespace weave::literals;

void shuttle_demo_publish() {
  // ── Subscription: both callback capabilities ─────────────────────────
  // Blocking capability (delivered by `serial`/`concurrent`): the callback runs
  // in a broker background simulation process and may consume simulation time.
  auto async_mon =
      shuttle::subscribe<UartTx>("uart0.tx", [](const UartTx& tx, const shuttle::Meta& m) {
        std::printf("[async] seq=%llu addr=0x%08x data=0x%08x\n",
                    static_cast<unsigned long long>(m.topic_seq), tx.addr, tx.data);
        weave::sleep(10_ns);  // blocking-capability callback: waiting is allowed
      });

  // Nonblocking capability (delivered by `publish_now`): the callback runs
  // synchronously inside the publisher's process and must not block by contract
  // (nb() is a capability declaration that cannot be enforced at compile time;
  // see the notes in types.hpp).
  UartTx last{};
  auto sync_mon = shuttle::subscribe<UartTx>(
      "uart0.tx", shuttle::nb([&](const UartTx& tx, const shuttle::Meta&) { last = tx; }));

  // The Publisher constructor performs the topic's type binding; a conflicting
  // binding throws TypeMismatchError.
  shuttle::Publisher<UartTx> pub("uart0.tx");

  // ── `publish_now`: synchronous callbacks; the publisher never suspends ──
  // Delivers to sync_mon; async_mon's registration entry is blocking and is
  // handled per NowPolicy: the default is defer (hand to the background serial
  // chain); here we demo skip (skip and count).
  pub.publish_now(UartTx{0x10, 0xA5}, shuttle::NowPolicy::skip);
  if (last.addr != 0x10 || last.data != 0xA5)
    throw std::logic_error("shuttle_demo_publish: publish_now did not deliver synchronously");

  // ── `publish_serial`: background single-chain, in-order callbacks ──────
  // Locks in the subscriber set and the message snapshot at the call and
  // returns immediately; FIFO per topic.
  auto h2 = pub.publish_serial(UartTx{0x11, 0x5A});
  h2.result();  // wait until all subscriber callbacks finish; rethrows the first failure

  // ── `publish_concurrent`: background, one process per subscriber ───────
  // No callback ordering is promised; completion semantics match `publish_serial`.
  auto h3 = pub.publish_concurrent(UartTx{0x12, 0x3C});
  h3.wait();
  if (!h3.done() || h3.subscriber_count() == 0)
    throw std::logic_error("shuttle_demo_publish: publish_concurrent completion state abnormal");
  for (const auto& f : h3.failures())  // inspect the failure details (empty in this demo)
    std::printf("[warn] subscriber %s failed\n", f.subscriber.c_str());

  // ── retained (latched): a new subscriber immediately receives the latest frame ──
  pub.set_retained(true);
  pub.publish_now(UartTx{0x13, 0xC3}, shuttle::NowPolicy::skip);
  bool got_retained = false;
  auto late_joiner = shuttle::subscribe<UartTx>(
      "uart0.tx", shuttle::nb([&](const UartTx& tx, const shuttle::Meta&) {
        got_retained =
            (tx.addr == 0x13);  // the retained frame arrives synchronously at subscribe time
      }));
  if (!got_retained) throw std::logic_error("shuttle_demo_publish: retained frame not delivered");

  // ── Wildcards + raw-byte subscription ──────────────────────────────────
  // "uart0.>" matches any number of further levels; byte subscriptions are for
  // debugging / pass-through, or for consuming messages published from SV.
  auto raw_tap = shuttle::subscribe_bytes(
      "uart0.>", [](const std::vector<std::byte>& bytes, const shuttle::Meta& m) {
        // Meta strings are views, not necessarily NUL-terminated: materialize before c_str().
        const std::string name{m.topic_name};
        std::printf("[raw] topic=%s payload=%zu bytes\n", name.c_str(), bytes.size());
      });
  pub.publish_now(UartTx{0x14, 0x44}, shuttle::NowPolicy::skip);

  // ── Introspection: read-only snapshots, off the delivery path ─────────
  if (auto info = shuttle::broker::topic_info("uart0.tx")) {
    std::printf("[broker] topic=%s type=%s subs=%zu published=%llu delivered=%llu\n",
                info->name.c_str(), info->type_tag.c_str(), info->subscriber_count,
                static_cast<unsigned long long>(info->published),
                static_cast<unsigned long long>(info->delivered));
    if (info->type_tag != "UartTx")
      throw std::logic_error("shuttle_demo_publish: type tag does not match SHUTTLE_REFLECT");
  }

  // ── One-to-many: multiple Publisher instances on one topic deliver to the same subscribers ──
  // Multiple Publishers are legal and common when the types agree (identical type
  // tags): several driver components each instantiate a Publisher for the same
  // topic; every subscriber independently receives every frame from every Publisher.
  shuttle::Publisher<UartTx> other_pub("uart0.tx");
  other_pub.publish_now(UartTx{0x15, 0x55}, shuttle::NowPolicy::skip);
  if (last.addr != 0x15)
    throw std::logic_error("shuttle_demo_publish: multi-publisher did not deliver");

  // ── Unsubscribe: handles have shared semantics — the last destruction
  // unsubscribes automatically; explicit unsubscribe() also works.
  async_mon.unsubscribe();
  // sync_mon / late_joiner / raw_tap unsubscribe automatically at scope exit
}
