// Basic demo: message definition, subscriptions with both callback capabilities,
// the three publish flavors, retained, wildcards, and introspection.
//
// This file uses only the public shuttle/weave headers and is built exactly like
// a user linking libshuttle:
//   · systemc backend: compiled into the shuttle_examples_sc executable, run locally;
//   · svdpi backend:   compiled into libshuttle_examples_svdpi.so, loaded by the
//                      simulator via -sv_lib and started by weave_pkg::weave_run
//                      (see examples/shuttle_demo.sv).
// The SV-side counterpart is examples/shuttle_demo.sv.

#include <cstdint>
#include <cstdio>
#include <shuttle/shuttle.hpp>
#include <stdexcept>
#include <string>
#include <vector>
#include <weave/test.hpp>
#include <weave/weave.hpp>

using namespace weave::literals;

// ── Message type ─────────────────────────────────────────────────────────
// User-defined struct; member types must belong to the codec subset (see
// include/shuttle/codec.hpp). Macro-free reflection generates the serialization
// support (required for cross-language delivery) and the type tag "UartTx";
// the SV side must define a message class with the same fields in the same
// order and a matching type_tag (see examples/shuttle_demo.sv).
struct UartTx {
  std::uint32_t addr;
  std::uint32_t data;
};

WEAVE_TEST(basic_demo) {
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
    throw std::logic_error("basic_demo: publish_now did not deliver synchronously");

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
    throw std::logic_error("basic_demo: publish_concurrent completion state abnormal");
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
  if (!got_retained) throw std::logic_error("basic_demo: retained frame not delivered");

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
      throw std::logic_error("basic_demo: type tag does not match the struct name");
  }

  // ── One-to-many: multiple Publisher instances on one topic deliver to the same subscribers ──
  // Multiple Publishers are legal and common when the types agree (identical type
  // tags): several driver components each instantiate a Publisher for the same
  // topic; every subscriber independently receives every frame from every Publisher.
  shuttle::Publisher<UartTx> other_pub("uart0.tx");
  other_pub.publish_now(UartTx{0x15, 0x55}, shuttle::NowPolicy::skip);
  if (last.addr != 0x15) throw std::logic_error("basic_demo: multi-publisher did not deliver");

  // ── Edge cases: `publish_now` with a blocking-entry subscriber → CapabilityError; wait timeout
  // ── Isolated on a separate topic so the delayed frame does not disturb the FIFO observation
  // above.
  {
    auto blk_sub =
        shuttle::subscribe<UartTx>("uart1.edge", [](const UartTx&, const shuttle::Meta&) {
          // a blocking-entry subscriber cannot receive publish_now's synchronous callback
        });
    shuttle::Publisher<UartTx> ep("uart1.edge");
    bool threw = false;
    try {
      ep.publish_now(UartTx{0x20, 0xE0}, shuttle::NowPolicy::reject);
    } catch (const shuttle::CapabilityError&) {
      threw = true;
    }
    if (!threw)
      throw std::logic_error("basic_demo: publish_now did not error under the reject policy");
    // timeout: triggered after a 1us delay but we wait only 1ns → not yet
    // complete; then wait indefinitely until it truly completes.
    auto hp = ep.publish_serial(UartTx{0x21, 0xE1}, shuttle::after(1_us));
    if (hp.wait(1_ns)) throw std::logic_error("basic_demo: wait-timeout verdict wrong");
    hp.wait();
    if (!hp.done()) throw std::logic_error("basic_demo: not completed after the timeout wait");
    std::printf("[edge] CapabilityError and wait-timeout behavior match the contract\n");
  }

  // ── Unsubscribe: handles have shared semantics — the last destruction
  // unsubscribes automatically; explicit unsubscribe() also works.
  async_mon.unsubscribe();
  // sync_mon / late_joiner / raw_tap unsubscribe automatically at scope exit
}
