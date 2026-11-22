// Nested message demo: a message that carries two messages of another reflected type.
//
// Subject file of the C++ half of the uart demo (the SV mirror is
// examples/shuttle_demo_<subject>.sv): exposes void shuttle_demo_<subject>(), called in
// order by examples/shuttle_demo.cpp. Independent of the other subjects; see
// examples/shuttle_demo_msgs.hpp for the wire types.

#include <shuttle/shuttle.hpp>
#include <stdexcept>

#include "shuttle_demo_msgs.hpp"

void shuttle_demo_nested() {
  // ── Nested message types: a message containing two messages of another reflected type ──
  // Retained so the SV demo decodes the same frame whenever it subscribes (the demo is compiled
  // only on the svdpi backend); on the systemc backend the typed subscriber below is the only
  // target. The assertions run silently — a wrong nested field throws, matching this demo's
  // "assertion by exception" style.
  UartBurst burst_last{};
  auto burst_sub = shuttle::subscribe<UartBurst>(
      "uart0.burst", [&burst_last](const UartBurst& b, const shuttle::Meta&) { burst_last = b; });
  shuttle::Publisher<UartBurst> burst_pub("uart0.burst");
  burst_pub.set_retained(true);
  burst_pub.publish_serial(UartBurst{2, UartTx{0x21, 0xAA}, UartTx{0x22, 0xBB}}).wait();
  if (burst_last.count != 2 || burst_last.first.addr != 0x21 || burst_last.first.data != 0xAA ||
      burst_last.last.addr != 0x22 || burst_last.last.data != 0xBB)
    throw std::logic_error("shuttle_demo_nested: nested message round trip failed");
}
