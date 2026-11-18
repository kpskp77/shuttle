#pragma once

// Internal framework interface: the **only** dependency surface between the
// broker core (`src/broker/`, backend-agnostic) and the simulation backend
// (`src/backend/<backend>/`). The broker knows a backend only through here.
//
// Backend-specific implementation details — the frame-byte pull channel, the
// SV event registry, the DPI-C export surface, scope fixing — all stay inside
// `src/backend/<backend>/` and do not enter this header. Adding a backend =
// adding a directory; neither the broker nor this header changes (unless the
// new backend cannot even provide "delivery + time").
//
// Current implementations:
//   src/backend/svdpi/    bridge.cpp (this interface) + dpi.cpp (DPI-C export surface)
//   src/backend/systemc/  bridge.cpp (degraded: no external-subscriber channel)
//
// This header belongs to the backend boundary, hence it lives in
// `src/backend/`; `src/` is still the include root, so includers write
// `#include "backend/backend.hpp"`.

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace shuttle::detail {
namespace backend {

// Whether the current build has an external (SV) subscriber channel:
// svdpi = true, systemc = false.
bool available();

// Delivery for `serial` / `concurrent`: called in a weave task (simulation
// process) context; the backend runs the user callback inside the external
// process. `handle` is the backend-side number the subscriber received at
// registration; the broker only passes it through without interpreting it.
// `topic_seq` / `time_fs` / `corr_id` are the frame header's fields, forwarded
// verbatim so that Meta is identical in both languages.
void dispatch(int handle, std::string_view topic_name, std::string_view type_tag,
              std::uint64_t topic_seq, std::int64_t time_fs, std::uint32_t corr_id,
              const std::vector<std::byte>& payload);

// Delivery for `publish_now`: called in the publisher's process context and
// **must not block at all** (an import function's call chain cannot invoke an
// export task; see AGENTS.md invariant 3).
void dispatch_nb(int handle, std::string_view topic_name, std::string_view type_tag,
                 std::uint64_t topic_seq, std::int64_t time_fs, std::uint32_t corr_id,
                 const std::vector<std::byte>& payload);

// Current simulation time (femtoseconds), used as `Meta::time_fs`.
std::int64_t now_fs();

// Unsubscribe feedback channel: when the backend finds during dispatch that
// the target has already unsubscribed (an in-flight delivery was dropped), it
// calls `note_retired_delivery()`; after the export returns, the broker calls
// `take_retired_delivery()` to record this delivery as retired (AGENTS.md
// invariant 7).
void note_retired_delivery();
bool take_retired_delivery();

}  // namespace backend
}  // namespace shuttle::detail
