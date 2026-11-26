// svdpi backend: implementation of `detail::backend` (the only interface face
// the broker knows).
//
// Frame payloads do not go through DPI arguments: before C++ calls the export,
// it puts the current frame's byte stream into the process-level context
// (FrameContext), and the SV side pulls it inside the export body via the
// imports shuttle_c_frame_len / shuttle_c_frame_payload. The DPI-C export
// surface (shuttle_c_*) lives in dpi.cpp.

#include <svdpi.h>

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "backend/backend.hpp"
#include "svdpi_internal.hpp"

// ── C prototypes of the SV-side exports (defined in shuttle_pkg.sv) ──────
extern "C" {
void shuttle_sv_dispatch(int sv_sid, const char* topic_name, const char* type_tag,
                         long long topic_seq, long long time_fs, unsigned int corr_id);
void shuttle_sv_dispatch_nb(int sv_sid, const char* topic_name, const char* type_tag,
                            long long topic_seq, long long time_fs, unsigned int corr_id);
}

namespace shuttle::detail {
namespace backend {

namespace {

// Whether the SV registry reported "target already unsubscribed" during this
// dispatch (see shuttle_c_sub_retired in dpi.cpp). The single-active-process
// guarantee makes atomics unnecessary.
bool g_retired_hit = false;

}  // namespace

bool available() {
  return true;
}

void dispatch(int handle, std::string_view topic_name, std::string_view type_tag,
              std::uint64_t topic_seq, std::int64_t time_fs, std::uint32_t corr_id,
              const std::vector<std::byte>& payload) {
  SvScopeGuard scope;
  FrameContext ctx(payload);
  g_retired_hit = false;
  // The export table takes NUL-terminated C strings, and a Meta view is not
  // guaranteed to be one: materialize here (on the SV-delivery path only).
  const std::string topic{topic_name};
  const std::string tag{type_tag};
  shuttle_sv_dispatch(handle, topic.c_str(), tag.c_str(), static_cast<long long>(topic_seq),
                      static_cast<long long>(time_fs), corr_id);
}

void dispatch_nb(int handle, std::string_view topic_name, std::string_view type_tag,
                 std::uint64_t topic_seq, std::int64_t time_fs, std::uint32_t corr_id,
                 const std::vector<std::byte>& payload) {
  SvScopeGuard scope;
  FrameContext ctx(payload);
  g_retired_hit = false;
  // Same materialization as dispatch(): the view may not be NUL-terminated.
  const std::string topic{topic_name};
  const std::string tag{type_tag};
  shuttle_sv_dispatch_nb(handle, topic.c_str(), tag.c_str(), static_cast<long long>(topic_seq),
                         static_cast<long long>(time_fs), corr_id);
}

void note_retired_delivery() {
  g_retired_hit = true;
}

bool take_retired_delivery() {
  const bool hit = g_retired_hit;
  g_retired_hit = false;
  return hit;
}

int frame_len() {
  return g_frame ? static_cast<int>(g_frame->size()) : 0;
}

const std::byte* frame_data() {
  return (g_frame != nullptr && !g_frame->empty()) ? g_frame->data() : nullptr;
}

}  // namespace backend
}  // namespace shuttle::detail
