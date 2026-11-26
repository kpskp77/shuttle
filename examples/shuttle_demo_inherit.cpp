// Inherited message demo: each level delegates its base subobject to the base codec.
//
// Subject file of the C++ half of the uart demo (the SV mirror is
// examples/shuttle_demo_<subject>.sv): exposes void shuttle_demo_<subject>(), called in
// order by examples/shuttle_demo.cpp. Independent of the other subjects; see
// examples/shuttle_demo_msgs.hpp for the message types.

#include <shuttle/shuttle.hpp>
#include <stdexcept>

#include "shuttle_demo_msgs.hpp"

void shuttle_demo_inherit() {
  // ── Inherited message types: each level delegates its base subobject to the base codec ──
  // Retained for the same reason as the burst above: the SV demo decodes the frame whenever it
  // subscribes. The encoded length is the sum over the chain (4 + 4 + 1 bytes).
  UartTagged tag_last{};
  auto tag_sub = shuttle::subscribe<UartTagged>(
      "uart0.tag", [&tag_last](const UartTagged& t, const shuttle::Meta&) { tag_last = t; });
  shuttle::Publisher<UartTagged> tag_pub("uart0.tag");
  tag_pub.set_retained(true);
  UartTagged tagged{};
  tagged.addr = 0x30;
  tagged.data = 0xCAFE;
  tagged.flags = 0x5;
  tag_pub.publish_serial(tagged).wait();
  if (tag_last.addr != 0x30 || tag_last.data != 0xCAFE || tag_last.flags != 0x5)
    throw std::logic_error("shuttle_demo_inherit: inherited message round trip failed");
}
