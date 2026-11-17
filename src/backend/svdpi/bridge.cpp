// svdpi backend: implementation of `detail::backend` (the only interface face
// the broker knows).
//
// Frame payloads do not go through DPI arguments: before C++ calls the export,
// it puts the current frame's byte stream into the process-level context
// (FrameContext), and the SV side pulls it in chunks inside the export body
// via the scalar imports shuttle_c_frame_len / _qword. The DPI-C export
// surface (shuttle_c_*) lives in dpi.cpp.

#include <svdpi.h>

#include <cstdint>
#include <string>
#include <vector>

#include "backend/backend.hpp"
#include "svdpi_internal.hpp"

// ── C prototypes of the SV-side exports (defined in shuttle_pkg.sv) ──────
extern "C" {
void shuttle_sv_dispatch(int sv_sid, const char* topic_name, const char* type_tag,
                         long long topic_seq, long long time_fs);
void shuttle_sv_dispatch_nb(int sv_sid, const char* topic_name, const char* type_tag,
                            long long topic_seq, long long time_fs);
long long shuttle_sv_time_fs();
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

void dispatch(int handle, const std::string& topic_name, const std::string& type_tag,
              std::uint64_t topic_seq, std::int64_t time_fs,
              const std::vector<std::byte>& payload) {
  SvScopeGuard scope;
  FrameContext ctx(payload);
  g_retired_hit = false;
  shuttle_sv_dispatch(handle, topic_name.c_str(), type_tag.c_str(),
                      static_cast<long long>(topic_seq), static_cast<long long>(time_fs));
}

void dispatch_nb(int handle, const std::string& topic_name, const std::string& type_tag,
                 std::uint64_t topic_seq, std::int64_t time_fs,
                 const std::vector<std::byte>& payload) {
  SvScopeGuard scope;
  FrameContext ctx(payload);
  g_retired_hit = false;
  shuttle_sv_dispatch_nb(handle, topic_name.c_str(), type_tag.c_str(),
                         static_cast<long long>(topic_seq), static_cast<long long>(time_fs));
}

void note_retired_delivery() {
  g_retired_hit = true;
}

bool take_retired_delivery() {
  const bool hit = g_retired_hit;
  g_retired_hit = false;
  return hit;
}

std::int64_t now_fs() {
  SvScopeGuard scope;
  return static_cast<std::int64_t>(shuttle_sv_time_fs());
}

int frame_len() {
  return g_frame ? static_cast<int>(g_frame->size()) : 0;
}

std::uint64_t frame_qword(int index) {
  if (!g_frame) return 0;
  std::uint64_t v = 0;
  for (int k = 0; k < 8; ++k) {
    const std::size_t i = static_cast<std::size_t>(index) * 8 + static_cast<std::size_t>(k);
    if (i < g_frame->size())
      v |= static_cast<std::uint64_t>(static_cast<std::uint8_t>((*g_frame)[i])) << (8 * k);
  }
  return v;
}

}  // namespace backend
}  // namespace shuttle::detail
