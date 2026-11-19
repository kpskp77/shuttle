// systemc backend: degraded implementation of `detail::backend` (no external
// subscriber channel; only pure-C++ paths remain in the broker).
//
// All of these are contract stubs — every entry of `detail::backend` must have
// a definition (switching backends = switching directories, not the interface),
// and under systemc no external subscriber exists so dispatch/dispatch_nb can
// never be called: the throws here both satisfy the linker and, should a
// misconfiguration actually invoke them, fail fast instead of staying silent.
// So this is not dead code, and it must not be merged into a common file with
// #ifdef (each configuration then compiles only half, errors arrive late and
// only half the protection remains).
//
// Simulation time needs nothing here: `weave::now()` is backend-agnostic and
// reads sc_time_stamp() through weave's systemc port.

#include <cstdint>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "backend/backend.hpp"

namespace shuttle::detail {
namespace backend {

bool available() {
  return false;
}

void dispatch(int, std::string_view, std::string_view, std::uint64_t, std::int64_t, std::uint32_t,
              const std::vector<std::byte>&) {
  throw std::logic_error("shuttle: the systemc backend has no SV subscriber channel");
}

void dispatch_nb(int, std::string_view, std::string_view, std::uint64_t, std::int64_t,
                 std::uint32_t, const std::vector<std::byte>&) {
  throw std::logic_error("shuttle: the systemc backend has no SV subscriber channel");
}

// The systemc backend has no SV registry and never reports an in-flight
// delivery as retired.
void note_retired_delivery() {}
bool take_retired_delivery() {
  return false;
}

}  // namespace backend
}  // namespace shuttle::detail
