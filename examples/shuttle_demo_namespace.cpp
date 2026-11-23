// Namespaced message demo: a message type declared inside a namespace, reflected through a global
// unqualified alias so its type tag matches an SV class.
//
// Subject file of the C++ half of the uart demo (the SV mirror is
// examples/shuttle_demo_namespace.sv): exposes void shuttle_demo_namespace(), called in order by
// examples/shuttle_demo.cpp. Independent of the other subjects.
//
// SHUTTLE_REFLECT can only run at global scope (it reopens namespace shuttle to specialize
// Codec<T>), so a type declared in a namespace cannot be reflected in place. The global alias
// below lifts the name out. Because the macro's first argument is also the type tag, the alias
// spelling "UartCfg" is what the SV class declares as type_tag. Reflecting `demo_wire::UartCfg`
// instead would tag every frame "demo_wire::UartCfg" — a string no SV class can carry, and the SV
// subscriber would silently never enter the frame's target set.

#include <cstdint>
#include <shuttle/shuttle.hpp>
#include <stdexcept>

namespace demo_wire {
struct UartCfg {
  std::uint32_t baud = 0;
  std::uint8_t mode = 0;
};
}  // namespace demo_wire

using UartCfg = demo_wire::UartCfg;
SHUTTLE_REFLECT(UartCfg, baud, mode)

void shuttle_demo_namespace() {
  // ── A message type from a namespace, registered through its global alias ──
  // Retained for the same reason as the nested and inherited demos: the SV demo decodes the frame
  // whenever it subscribes. The subscriber also asserts the tag, so a regression to the qualified
  // spelling fails here instead of silently dropping the SV peer.
  UartCfg cfg_last{};
  auto cfg_sub = shuttle::subscribe<UartCfg>(
      "cfg.ns.uart", [&cfg_last](const UartCfg& c, const shuttle::Meta& meta) {
        if (meta.type_tag != "UartCfg")
          throw std::logic_error("shuttle_demo_namespace: tag must be the alias spelling");
        cfg_last = c;
      });
  shuttle::Publisher<UartCfg> cfg_pub("cfg.ns.uart");
  cfg_pub.set_retained(true);
  cfg_pub.publish_serial(UartCfg{115200, 3}).wait();
  if (cfg_last.baud != 115200 || cfg_last.mode != 3)
    throw std::logic_error("shuttle_demo_namespace: namespaced message round trip failed");
}
