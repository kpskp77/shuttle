#pragma once

// svdpi backend internal header: included only by TUs under
// `src/backend/svdpi/`; not visible outside.
//
// What lives here is "how the svdpi backend itself is implemented":
//   - scope fixing for our own package-level exports (SvScopeGuard);
//   - the C++→SV frame-byte pull channel (FrameContext + frame_len/frame_qword);
//   - declarations of the external facade (`shuttle::detail::ext_*` / `pub_*` /
//     the event registry / set_last_error; implemented in facade.cpp).
//
// Note: svdpi is the only backend in this repository with an FFI channel (and
// there will never be a second one), so the external facade declarations live
// here too — its dependency on broker internals (broker/broker.hpp) is an
// explicitly accepted, permanent asymmetry (the systemc backend still knows
// the broker only through the `backend/backend.hpp` narrow port).
// The protocol definition is docs/backends.md.

#include <svdpi.h>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <shuttle/publication.hpp>
#include <shuttle/trigger.hpp>
#include <shuttle/types.hpp>
#include <string>
#include <string_view>
#include <vector>

// The package name can be overridden with -DSHUTTLE_SV_PACKAGE=. Only shuttle's
// own package needs it: weave's DPI backend fixes the svScope around each of its
// own imports, so nothing here switches scope for weave.
#ifndef SHUTTLE_SV_PACKAGE
#define SHUTTLE_SV_PACKAGE "shuttle_pkg"
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
// the current scope. Therefore we explicitly switch the scope to shuttle's own
// SV package before entering each export of ours and restore it afterwards
// (weave handles the same problem for its own exports internally).
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

// ── On-wire codes ────────────────────────────────────────────────────────
// SV asks for these with bare ints, so every family gets a name on this side
// too: the numbers are part of the DPI protocol (constant table in
// docs/backends.md) and must not drift. Convert once at the boundary,
// compare enums inside. Codes that already have a public enum in shuttle
// (`NowPolicy`, `Capability`) use that one instead of a second spelling — the
// protocol table pins their numeric values.

// Trigger kinds (SHUTTLE_TRIG_*): how a background frame waits for its release.
enum class TrigKind : int { none = 0, delay = 1, completion = 2, edge = 3 };

// State of a publication (pub_state); "no such id" is kUnknownPub, not a state.
enum class PubLifecycle : int { running = 0, done = 1, done_with_failures = 2 };

// Outcome of one blocking wait (shuttle_c_wait_pub; SHUTTLE_WAIT_*):
// done -> ok, deadline won -> timeout, failure present or internal error -> failed.
enum class WaitStatus : int { ok = 0, timeout = 1, failed = 2 };

// The wire carries ints, so the comparisons at the boundary read
// `wire(WaitStatus::timeout)` instead of a bare 1. (Enums only: there is no
// reason to spell an int this way.)
template <class Code>
constexpr int wire(Code code) noexcept {
  return static_cast<int>(code);
}

// Answer for "this id is not a publication, or this query makes no sense".
inline constexpr int kUnknownPub = -1;
// pub_id of an invalid Publication handle: ids start at 1, so 0 reads as "none".
inline constexpr int kNoPub = 0;
// Return codes of the facade entries that answer with a code instead of throwing.
inline constexpr int kFacadeOk = 0;
inline constexpr int kFacadeError = 1;

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
//
// The name parameters are string_views: DPI hands us `const char*`, and the
// broker copies what it keeps, so there is no reason to materialize a
// std::string per frame here. `frame` is by value so the raw path can move the
// caller's buffer into the envelope instead of copying it.
Publication ext_pub_now(std::string_view topic, std::string_view tag, std::vector<std::byte> frame,
                        NowPolicy policy, std::uint32_t corr_id);
Publication ext_pub_serial(std::string_view topic, std::string_view tag,
                           std::vector<std::byte> frame, Trigger trig, std::uint32_t corr_id);
Publication ext_pub_concurrent(std::string_view topic, std::string_view tag,
                               std::vector<std::byte> frame, Trigger trig, std::uint32_t corr_id);

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
// kinds 0-4 (see the constant table in docs/backends.md, invariant across
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
// Ends the run through weave::fatal when the frame can never start because the
// task that awaits it is the task that holds its delivery chain. Called by the
// DPI wait entry before it suspends, so a hung SV process becomes a diagnosed
// death instead of a dead simulation.
void pub_check_self_blocked(int pub_id);
// Register an "on completion" callback (meaningful only while the pub is
// incomplete; must not block). There must be no suspension point between the
// pub_state()==0 check and the registration, so no completion event can be
// missed.
void pub_on_complete(int pub_id, std::function<void()> fn);

}  // namespace shuttle::detail
