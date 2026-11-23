// Objects crossing by reference (svdpi backend only): the SV side publishes an object handle,
// the frame carries a 4-byte reference, and the receiver resolves it back to the same handle.
//
// Paired with case_ref_xlang in tests/shuttle_tests_ref.sv. On the wire this is a plain u32 —
// class FObj on the SV side, struct FObj here — so the frame lives inside the broker's existing
// type space: serial/concurrent/retained, triggers and cross-language routing all apply
// unchanged. This suite plays the peer that does not own the object: it never resolves the
// reference, it only routes it back, which is exactly the indirection's promise.
//
// The suite entry exists only in the svdpi build (xmake defines SHUTTLE_TESTS_XLANG there): the
// case pairs with an SV task, so it cannot run on the systemc backend at all.
#include <cstdint>
#include <memory>
#include <shuttle/shuttle.hpp>
#include <string>
#include <vector>
#include <weave/weave.hpp>

#include "shuttle_tests_runner.hpp"

// Compiled only into the svdpi backend's test target: this file pairs with an SV task, which does
// not exist under the systemc backend (no SHUTTLE_BACKEND_* macros — backends are selected by
// directory).

using namespace weave::literals;
using shuttle_test::expect;
using shuttle_test::report;
using shuttle_test::run_case;

// The wire form: one 32-bit reference. Byte-for-byte counterpart of `class FObj` in
// tests/shuttle_tests_ref.sv (the SV side tags its frames $typename(FObj) == "FObj"), so both
// ends agree on tag "FObj" and on a single big-endian u32.
struct FObj {
  std::uint32_t id = 0;
};
SHUTTLE_REFLECT(FObj, id)

// Handshake marker for the per-case rendezvous: one byte, no asserted fields. The SV mirror is
// class XHs in tests/shuttle_tests_runner.sv; both sides only care that a frame arrives.
struct XHs {
  std::uint8_t v = 0;
};
SHUTTLE_REFLECT(XHs, v)

namespace {

constexpr const char* kEchoReqTopic = "ref.echo.req";    // SV → C++, carries the reference
constexpr const char* kEchoRespTopic = "ref.echo.resp";  // C++ → SV, routes it back

// Per-case rendezvous with the SV side (the mirror task is xlang_barrier in tests/
// shuttle_tests_runner.sv): publish a retained marker on "xlang.hs.cpp.<name>", then wait
// (bounded — the bound is a watchdog, not the synchronization) for the SV marker on
// "xlang.hs.sv.<name>". The retained replay makes the pairing order-free.
void xlang_barrier(const char* name) {
  weave::Event ev;
  shuttle::Subscription peer = shuttle::subscribe_bytes(
      "xlang.hs.sv." + std::string(name),
      [&ev](const std::vector<std::byte>&, const shuttle::Meta&) { ev.set(); });
  shuttle::Publisher<XHs> mine("xlang.hs.cpp." + std::string(name));
  mine.set_retained(true);
  mine.publish_serial(XHs{1}).wait();
  for (int i = 0; i < 2000 && !ev.is_set(); ++i) weave::sleep(100_us);
  expect(ev.is_set(), "xlang_barrier: the SV peer never showed up for this case");
  (void)peer;
}

}  // namespace

void shuttle_tests_ref() {
  // case11: an SV object crosses as a reference. The SV side binds the object, publishes the
  // frame, and this peer routes the 4-byte reference back untouched — a C++ subscriber that
  // does not own the object never resolves it. The barrier first proves the SV task is past its
  // mailbox registration; the echo subscription here is registered before the barrier, so the SV
  // publish after the barrier always has its target.
  run_case("shuttle_tests_ref", "case11", [] {
    auto seen = std::make_shared<std::uint32_t>(0);
    weave::Event ev;
    shuttle::Subscription echo =
        shuttle::subscribe<FObj>(kEchoReqTopic, [seen, &ev](const FObj& m, const shuttle::Meta&) {
          *seen = m.id;
          shuttle::Publisher<FObj> resp(kEchoRespTopic);
          resp.publish_serial(FObj{m.id});
          ev.set();
        });

    xlang_barrier("case11");
    for (int i = 0; i < 2000 && !ev.is_set(); ++i) weave::sleep(100_us);
    expect(ev.is_set(), "case11: the SV request never arrived or the echo never ran");
    expect(*seen != 0, "case11: the frame carried a non-zero reference");
    (void)echo;
  });

  report(__func__);
}
