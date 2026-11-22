// Edge-case demo: `publish_now` against a blocking entry (CapabilityError) and the
// wait-timeout verdict.
//
// Subject file of the C++ half of the uart demo (the SV mirror is
// examples/shuttle_demo_<subject>.sv): exposes void shuttle_demo_<subject>(), called in
// order by examples/shuttle_demo.cpp. Independent of the other subjects; see
// examples/shuttle_demo_msgs.hpp for the wire types.

#include <cstdint>
#include <cstdio>
#include <shuttle/shuttle.hpp>
#include <stdexcept>
#include <weave/weave.hpp>

#include "shuttle_demo_msgs.hpp"

using namespace weave::literals;

void shuttle_demo_edge() {
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
      throw std::logic_error(
          "shuttle_demo_edge: publish_now did not error under the reject policy");
    // timeout: triggered after a 1us delay but we wait only 1ns → not yet
    // complete; then wait indefinitely until it truly completes.
    auto hp = ep.publish_serial(UartTx{0x21, 0xE1}, shuttle::after(1_us));
    if (hp.wait(1_ns)) throw std::logic_error("shuttle_demo_edge: wait-timeout verdict wrong");
    hp.wait();
    if (!hp.done())
      throw std::logic_error("shuttle_demo_edge: not completed after the timeout wait");
    std::printf("[edge] CapabilityError and wait-timeout behavior match the contract\n");
  }
}
