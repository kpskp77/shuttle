#pragma once

// svdpi backend internal header: included only by TUs under
// `src/backend/svdpi/`; not visible outside.
//
// What lives here is "how the svdpi backend itself is implemented":
//   - scope fixing for package-level exports (SvScopeGuard);
//   - the C++→SV frame-byte pull channel (FrameContext + frame_len/frame_qword);
//   - declarations of the external facade (`shuttle::detail::ext_*` / `pub_*` /
//     the event registry / set_last_error; implemented in facade.cpp).
//
// Note: svdpi is the only backend in this repository with an FFI channel (and
// there will never be a second one), so the external facade declarations live
// here too — its dependency on broker internals (broker/broker.hpp) is an
// explicitly accepted, permanent asymmetry (the systemc backend still knows
// the broker only through the `backend/backend.hpp` narrow port).
// The protocol definition is docs/dpi_protocol.md.

#include <svdpi.h>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <shuttle/publication.hpp>
#include <shuttle/trigger.hpp>
#include <shuttle/types.hpp>
#include <string>
#include <vector>

// The package names can be overridden with -DSHUTTLE_SV_PACKAGE= /
// -DSHUTTLE_WEAVE_SV_PACKAGE=.
#ifndef SHUTTLE_SV_PACKAGE
#define SHUTTLE_SV_PACKAGE "shuttle_pkg"
#endif
// weave's SV package (its exports are defined there; shuttle switches the
// scope to it at import entry points, so the exports are visible when the C++
// side calls weave primitives (sleep/create_event/spawn…)).
#ifndef SHUTTLE_WEAVE_SV_PACKAGE
#define SHUTTLE_WEAVE_SV_PACKAGE "weave_pkg"
#endif

namespace shuttle::detail {
namespace backend {

// Look up the scope by package name (cached; the simulation thread is
// single-active, so no locking is needed).
inline svScope scope_for(const char* pkg) {
  static const char* cached_pkg = nullptr;
  static svScope cached = nullptr;
  if (cached_pkg != pkg) {
    cached = svGetScopeFromName(pkg);
    cached_pkg = pkg;
  }
  return cached;
}

// ── Scope fixing for package-level exports ────────────────────────────────
// Measured on VCS 2018.09 (minimal repro in ../dpi_probe): after an import
// returning a string (shuttle_c_last_error) has been called within the same SV
// process, subsequent calls to package-level exports fail with
//   Error-[DPI-DXFNF] DPI export function not found
// The import call changes the current svScope, making the export invisible in
// the current scope. Therefore we explicitly switch the scope to shuttle's SV
// package before entering each export and restore it afterwards, so other
// libraries' (e.g. weave_pkg) export resolution is not affected.
class SvScopeGuard {
public:
  explicit SvScopeGuard(const char* pkg = SHUTTLE_SV_PACKAGE) : prev_(svGetScope()) {
    if (svScope s = scope_for(pkg)) svSetScope(s);
  }
  ~SvScopeGuard() {
    if (prev_) svSetScope(prev_);
  }
  SvScopeGuard(const SvScopeGuard&) = delete;
  SvScopeGuard& operator=(const SvScopeGuard&) = delete;

private:
  svScope prev_;
};

// Byte stream of the frame currently being pulled: set before C++ calls the
// export; SV pulls it in chunks inside the export body via
// `shuttle_c_frame_len` / `shuttle_c_frame_qword`. A plain global is visible
// enough because "only one process executes the C++-side bookkeeping at any
// moment" (AGENTS.md invariant 1) — the simulation is not multithreaded, so no
// thread_local is needed.
inline const std::vector<std::byte>* g_frame = nullptr;

class FrameContext {
public:
  explicit FrameContext(const std::vector<std::byte>& bytes) : prev_(g_frame) { g_frame = &bytes; }
  ~FrameContext() { g_frame = prev_; }
  FrameContext(const FrameContext&) = delete;
  FrameContext& operator=(const FrameContext&) = delete;

private:
  const std::vector<std::byte>* prev_;
};

// Chunked view of the frame byte stream (the SV side can only pull scalars,
// so it is chunked into little-endian 8-byte words).
int frame_len();
std::uint64_t frame_qword(int index);

}  // namespace backend

// ── External facade (implemented in facade.cpp; namespace stays
//    shuttle::detail so dpi.cpp can call it as detail::* directly) ────────

// Per-process most-recent error message (data source for shuttle_c_last_error).
// Every internal entry point that reports an error code records its detailed
// reason here.
void set_last_error(std::string msg);
const std::string& last_error();

// `ext_*` = actions coming in from an external caller (byte-frame publish,
// external-subscriber registration; errors propagate as exceptions and are
// converted by the DPI wrapper layer into set_last_error + a return code).
// `pub_*` (first parameter is pub_id) = handle queries/waits by publication id.
Publication ext_pub_now(const std::string& topic, const std::string& tag,
                        const std::vector<std::byte>& frame, NowPolicy policy);
Publication ext_pub_serial(const std::string& topic, const std::string& tag,
                           const std::vector<std::byte>& frame, Trigger trig);
Publication ext_pub_concurrent(const std::string& topic, const std::string& tag,
                               const std::vector<std::byte>& frame, Trigger trig);

// Look up the pub_id for a handle (0 for an invalid handle) — the DPI layer
// uses it to convert a Publication into an int.
int pub_id(const Publication& p);

// External subscription registration: handle = SV registry id, recorded by the
// broker as-is and passed back at dispatch time.
// Returns 0 on success, non-zero on failure (type-binding conflict, etc.).
int ext_sub(std::string topic_pattern, int caps, std::string type_tag, int handle);
void ext_unsub(int handle);

// Publication completion tracking (pub_id assigned at publish time)
bool pub_done(int pub_id);

// Publication counter query kinds: the values correspond 1:1 to the on-wire
// kinds 0-4 (see the constant table in docs/dpi_protocol.md, invariant across
// DPI). The enum lets the compiler check switch exhaustiveness.
enum class PubStatKind { target_count = 0, finished = 1, skipped = 2, deferred = 3, retired = 4 };

// Publication counter query: kind 0=target_count 1=finished 2=skipped-by-policy
// 3=deferred 4=retired; unknown pub_id returns -1
long long pub_stat(int pub_id, int kind);
std::string pub_error(int pub_id);
// Blocking wait (must be called in a weave task / simulation-process context).
// status output: 0=completed (not necessarily failure-free — check pub_state)
// 1=timed out 2=a subscriber failed (see pub_error) or an internal error (see
// last_error).
void pub_wait(int pub_id, std::int64_t timeout_fs, int* status);

// Publication completion state: 0=in progress 1=completed, no failures
// 2=completed, with failures -1=unknown pub_id
int pub_state(int pub_id);
// Register an "on completion" callback (meaningful only while the pub is
// incomplete; must not block). There must be no suspension point between the
// pub_state()==0 check and the registration, so no completion event can be
// missed.
void pub_on_complete(int pub_id, std::function<void()> fn);

// ── Event registry (SV shuttle_event / TRIG_EVENT; semantically SV-specific,
//    hence living in this directory) ──
// event_wait_id: the wait entry of TRIG_EVENT (must be called in a weave task
// context).
void event_wait_id(int ev_id);
// The underlying objects of SV-side shuttle_events are owned by the registry
// (they stay valid until close).
int event_registry_create();
void event_registry_set(int ev_id);
void event_registry_destroy(int ev_id);

}  // namespace shuttle::detail
