// Object-reference demo, C++ side: the encoding is a reflected struct with one u32, and this file
// never sees an object.
//
// Subject file of the C++ half of the uart demo (the SV mirror is examples/shuttle_demo_ref.sv):
// exposes void shuttle_demo_ref(), called in order by examples/shuttle_demo.cpp.
//
// The SV half owns the objects and the table that numbers them. It publishes a reference on
// "ref.demo.req"; this file forwards the frame untouched to "ref.demo.resp"; the SV mailbox there
// resolves the id back to the *same* handle. That is the whole C++-side contract: a reference is
// data in the encoding, so a peer that only routes it needs no table, no lifetime rules and no
// knowledge of any object.
//
// The tag is "RefId" because the SV publisher declares it explicitly (a cross-language peer passes
// the mirror's spelling; the SV default $typename would carry the declaring scope). Here the plain
// global struct name already is that spelling.

#include <cstdint>
#include <shuttle/shuttle.hpp>
#include <stdexcept>

struct RefId {
  std::uint32_t object = 0;
};
using namespace weave::literals;

SHUTTLE_REFLECT(RefId, object)

void shuttle_demo_ref() {
  // The SV half reports the finished round trip here. Waiting on it keeps the forwarding
  // subscription alive: the SV demos run on their own schedule, and a forwarding peer that
  // returned early would silently drop the request. On the systemc backend nobody publishes it,
  // so the wait simply times out (Simulated time, like the mailbox demo's recv(1ms)).
  shuttle::Mailbox<RefId> finished = shuttle::mailbox<RefId>("ref.demo.done");

  // Announce: retained, so the SV half sees it whenever it gets there — it may start later than
  // this demo, and it must not publish its request before the echo topic has a subscriber.
  shuttle::Publisher<RefId> ready("ref.demo.ready");
  ready.set_retained(true);
  ready.publish_serial(RefId{0}).wait();

  // Forward: receive a reference frame, publish the same bytes back on the reply topic. No
  // resolution, no lookup, no bookkeeping — an id is opaque here.
  shuttle::Publisher<RefId> echo("ref.demo.resp");
  auto forward = shuttle::subscribe<RefId>(
      "ref.demo.req", [&](const RefId& r, const shuttle::Meta&) { echo.publish_now(r); });

  // Self-check on a private topic pair, so this demo passes on the systemc backend too, where no
  // SV half runs and the mirror type is all there is.
  shuttle::Publisher<RefId> loop("ref.cpp.req");
  RefId back{};
  auto mirror =
      shuttle::subscribe<RefId>("ref.cpp.req", [&](const RefId& r, const shuttle::Meta& meta) {
        if (meta.type_tag != "RefId")
          throw std::logic_error("shuttle_demo_ref: tag must be the struct name");
        back = r;
      });
  loop.publish_serial(RefId{42}).wait();
  if (back.object != 42)
    throw std::logic_error("shuttle_demo_ref: the mirror struct did not round trip");

  // Bounded: a missing SV half (systemc) answers nothing, and that is not a failure here — the SV
  // side owns the assertions about the echo.
  (void)finished.recv(20_ms);
}
