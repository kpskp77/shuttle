// svdpi backend: the SV-side import surface (`shuttle_c_*`; definitions in
// docs/dpi_protocol.md).
//
// Only "DPI-C type ↔ C++ type" translation and error reporting happen here;
// no broker state is held. The broker-side external entry facade is `ext_*` /
// `pub_*` in facade.cpp in the same directory (declared in
// svdpi_internal.hpp, invoked as detail::*).
// The `detail::backend` implementation (dispatch / now_fs / retired channel)
// is in bridge.cpp.

#include <svdpi.h>

#include <cstdint>
#include <cstring>
#include <memory>
#include <shuttle/detail/abi.hpp>
#include <shuttle/exception.hpp>
#include <shuttle/trigger.hpp>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>
#include <weave/time.hpp>

#include "backend/backend.hpp"
#include "detail/trigger.hpp"
#include "svdpi_internal.hpp"

// ── SV primitives exported by weave (defined in weave_pkg.sv) ────────────
// shuttle needs the ability to "suspend the current SV process", but
// weave::Event::wait() is only legal inside a weave task context (it throws
// "weave: blocking primitive outside task context" in a plain SV native
// process), so we use weave's DPI exports directly: the event id is allocated
// on the C++ side and fired by the C++ side at the moment of publication
// completion.
// Semantics (see ../weave/sv/weave_pkg.sv, weave_sv_wait): a three-way race
// between the ev_id leg, the tick-timeout leg, and the abort leg; status
// 0=event hit 1=timeout 2=cancelled; ev_id<0 means no event leg,
// timeout_ticks<0 means no timeout leg, abort_id<0 means no cancel leg.
// timeout_ticks is in units of $time ticks.
extern "C" {
int weave_sv_create_event();
void weave_sv_trigger(int ev_id);
void weave_sv_destroy_event(int ev_id);
long long weave_sv_ticks_per_sec();
void weave_sv_wait(int ev_id, long long timeout_ticks, int abort_id, int* status);
}

namespace {

using shuttle::detail::last_error;
using shuttle::detail::set_last_error;
using WeaveScopeGuard = shuttle::detail::backend::SvScopeGuard;

// DPI constants → NowPolicy: 0 = reject / 1 = skip / 2 = defer (out-of-range
// falls back to the default defer). An explicit mapping instead of
// static_cast: a changed constant table cannot silently misalign.
shuttle::NowPolicy now_policy_of(int v) {
  switch (v) {
    case 0:
      return shuttle::NowPolicy::reject;
    case 1:
      return shuttle::NowPolicy::skip;
    default:
      return shuttle::NowPolicy::defer;
  }
}

// Read a byte stream from an SV open array (byte payload[] → contiguous 8-bit
// elements).
std::vector<std::byte> read_payload(const svOpenArrayHandle h) {
  std::vector<std::byte> out;
  if (!h) return out;
  const int n = svSize(h, 1);
  out.reserve(n > 0 ? static_cast<std::size_t>(n) : 0);
  for (int i = 0; i < n; ++i) {
    // SV byte is a 2-state 8-bit type; svdpi.h exposes its element type as char
    const char* p = static_cast<const char*>(svGetArrElemPtr1(h, i));
    out.push_back(static_cast<std::byte>(static_cast<unsigned char>(*p)));
  }
  return out;
}

// Assemble the deferred trigger condition per protocol (trig_kind constants in shuttle_pkg.sv).
shuttle::Trigger build_trigger(int trig_kind, long long trig_arg) {
  using namespace shuttle;
  switch (trig_kind) {
    case 0:
      return Trigger();
    case 1:
      return after(weave::duration(trig_arg));
    case 2:
      return detail::TriggerAccess::from_fn(
          [ev_id = static_cast<int>(trig_arg)]() { detail::event_wait_id(ev_id); });
    case 3:  // wait for another publication to complete (TRIG_PUB); its subscriber failures also
             // count as "reached a terminal state"
      return detail::TriggerAccess::from_fn([pub_id = static_cast<int>(trig_arg)]() {
        int status = 0;
        detail::pub_wait(pub_id, 0, &status);
      });
    default:
      throw std::invalid_argument("shuttle: unknown trig_kind " + std::to_string(trig_kind));
  }
}

// Common skeleton of `serial`/`concurrent`: frame bytes → serial/concurrent
// scheduling → pub_id. The two flavors differ only in which broker entry they
// call; the rest (frame read, trigger, error reporting) is identical.
int publish_frame_bg(const char* topic, const char* type_tag, const svOpenArrayHandle payload,
                     int trig_kind, long long trig_arg, unsigned int corr_id, bool concurrent) {
  const WeaveScopeGuard weave_scope(SHUTTLE_WEAVE_SV_PACKAGE);
  try {
    std::vector<std::byte> frame = read_payload(payload);
    shuttle::Trigger trig = build_trigger(trig_kind, trig_arg);
    // The frame buffer is moved into the broker (raw frames keep it untouched);
    // topic/type_tag stay views into the SV strings for the duration of the call.
    const char* const t = topic ? topic : "";
    const char* const tag = type_tag ? type_tag : "";
    const shuttle::Publication pub =
        concurrent
            ? shuttle::detail::ext_pub_concurrent(t, tag, std::move(frame), std::move(trig),
                                                  corr_id)
            : shuttle::detail::ext_pub_serial(t, tag, std::move(frame), std::move(trig), corr_id);
    return shuttle::detail::pub_id(pub);
  } catch (const std::exception& e) {
    set_last_error(e.what());
    return 0;
  } catch (...) {
    set_last_error("shuttle: unknown exception");
    return 0;
  }
}

}  // namespace

extern "C" {

// Called back by the SV registry when it finds during dispatch that the target
// has already unsubscribed (retired): only sets a flag, no error (contract:
// in-flight deliveries after unsubscribing are dropped silently, not treated
// as errors). dispatch/dispatch_nb turn the flag into one retired accounting
// record after the export returns, so the C++ side does not record it as
// delivered (a phantom success).
void shuttle_c_sub_retired(int /*sv_sid*/) {
  shuttle::detail::backend::note_retired_delivery();
}

// `publish_now`: returns this frame's pub_id (>0), or 0 on failure (see
// shuttle_c_last_error). The nonblocking part has finished before returning;
// the deferred subscribers are still on the background serial chain, and the
// SV side can pass the pub_id to shuttle_c_wait_pub to wait for them.
int shuttle_c_publish_now(const char* topic, const char* type_tag, const svOpenArrayHandle payload,
                          int now_policy, unsigned int corr_id) {
  // These imports may indirectly drive weave (creating tasks/events/suspending
  // waits); weave's exports are defined in weave_pkg, so switch the scope to
  // weave_pkg at the entry, otherwise VCS reports Error-[DPI-DXFNF]
  // 'weave_sv_*' invisible (the entry scope is shuttle_pkg).
  const WeaveScopeGuard weave_scope(SHUTTLE_WEAVE_SV_PACKAGE);
  try {
    const shuttle::Publication pub =
        shuttle::detail::ext_pub_now(topic ? topic : "", type_tag ? type_tag : "",
                                     read_payload(payload), now_policy_of(now_policy), corr_id);
    return shuttle::detail::pub_id(pub);
  } catch (const std::exception& e) {
    set_last_error(e.what());
    return 0;
  } catch (...) {
    set_last_error("shuttle: unknown exception");
    return 0;
  }
}

// `publish_serial`: background single-chain, in-order callbacks. Returns pub_id (>0), or 0 on
// failure.
int shuttle_c_publish_serial(const char* topic, const char* type_tag,
                             const svOpenArrayHandle payload, int trig_kind, long long trig_arg,
                             unsigned int corr_id) {
  return publish_frame_bg(topic, type_tag, payload, trig_kind, trig_arg, corr_id, false);
}

// `publish_concurrent`: background, one task per subscriber, parallel across subscribers and FIFO
// per (topic, subscriber). Returns pub_id (>0), or 0 on failure.
int shuttle_c_publish_concurrent(const char* topic, const char* type_tag,
                                 const svOpenArrayHandle payload, int trig_kind, long long trig_arg,
                                 unsigned int corr_id) {
  return publish_frame_bg(topic, type_tag, payload, trig_kind, trig_arg, corr_id, true);
}

// DPI task: block until one publication completes.
// The suspension point is the task exported by weave, weave_sv_wait — it
// suspends **the calling SV process itself**, so an ordinary initial/always
// process can wait directly (it need not be inside a weave task).
// timeout_fs<=0 means wait forever.
void shuttle_c_wait_pub(int pub_id, long long timeout_fs, int* status) {
  int sink = 0;
  if (!status) status = &sink;
  try {
    const int st = shuttle::detail::pub_state(pub_id);
    if (st < 0) {
      set_last_error("shuttle: unknown pub_id " + std::to_string(pub_id));
      *status = 2;
      return;
    }
    if (st != 0) {  // already completed: answer directly, without suspending
      *status = (st == 2) ? 2 : 0;
      return;
    }

    const WeaveScopeGuard weave_scope(SHUTTLE_WEAVE_SV_PACKAGE);
    const int w = weave_sv_create_event();
    if (w < 0) {
      set_last_error("shuttle: failed to create the SV wakeup event");
      *status = 2;
      return;
    }
    // After a timeout the event id is destroyed, so the completion hook must
    // know it is already invalid (weave's convention: a destroyed id must not
    // be triggered again). The single-active-process guarantee makes atomics
    // unnecessary here.
    struct Slot {
      int ev;
      bool active = true;
    };
    auto slot = std::make_shared<Slot>();
    slot->ev = w;
    shuttle::detail::pub_on_complete(pub_id, [slot] {
      if (!slot->active) return;
      const WeaveScopeGuard weave_scope(SHUTTLE_WEAVE_SV_PACKAGE);
      weave_sv_trigger(slot->ev);
    });

    long long ticks = -1;
    if (timeout_fs > 0) {
      const long long tps = weave_sv_ticks_per_sec();
      const unsigned long long per_sec = static_cast<unsigned long long>(tps > 0 ? tps : 1);
      ticks = static_cast<long long>(
          (static_cast<unsigned __int128>(static_cast<unsigned long long>(timeout_fs)) * per_sec) /
          1000000000000000ull);
    }
    int wst = 0;
    weave_sv_wait(w, ticks, -1,
                  &wst);  // ← the only suspension point: suspends the current SV process
    slot->active = false;
    weave_sv_destroy_event(w);

    if (wst == 1) {  // the timeout leg won
      *status = 1;
      return;
    }
    *status = (shuttle::detail::pub_state(pub_id) == 2) ? 2 : 0;
  } catch (const std::exception& e) {
    set_last_error(e.what());
    *status = 2;
  } catch (...) {
    set_last_error("shuttle: wait cancelled or unknown exception");
    *status = 2;
  }
}

int shuttle_c_pub_done(int pub_id) {
  return shuttle::detail::pub_done(pub_id) ? 1 : 0;
}

// Publication counter query: kind 0=target_count 1=finished 2=skipped-by-policy
// 3=deferred 4=retired; unknown pub_id returns -1
int shuttle_c_pub_stat(int pub_id, int kind) {
  const long long v = shuttle::detail::pub_stat(pub_id, kind);
  return v > 2147483647LL ? 2147483647 : static_cast<int>(v);
}

const char* shuttle_c_pub_error(int pub_id) {
  static std::string msg;
  msg = shuttle::detail::pub_error(pub_id);
  return msg.c_str();
}

int shuttle_c_subscribe(const char* topic_pattern, int caps, const char* type_tag, int sv_sid) {
  const WeaveScopeGuard weave_scope(SHUTTLE_WEAVE_SV_PACKAGE);
  try {
    return shuttle::detail::ext_sub(topic_pattern ? topic_pattern : "", caps,
                                    type_tag ? type_tag : "", sv_sid);
  } catch (const std::exception& e) {
    set_last_error(e.what());
    return 1;
  } catch (...) {
    set_last_error("shuttle: unknown exception");
    return 1;
  }
}

void shuttle_c_unsubscribe(int sv_sid) {
  const WeaveScopeGuard weave_scope(SHUTTLE_WEAVE_SV_PACKAGE);
  shuttle::detail::ext_unsub(sv_sid);
}

void shuttle_c_set_retained(const char* topic, int retained) {
  try {
    shuttle::detail::topic_set_retained(topic ? topic : "", retained != 0);
  } catch (const std::exception& e) {
    set_last_error(e.what());
  }
}

const char* shuttle_c_last_error() {
  return last_error().c_str();
}

// Current frame byte stream: length and little-endian 8-byte chunks (SV pulls
// them inside the shuttle_sv_dispatch body).
int shuttle_c_frame_len() {
  return shuttle::detail::backend::frame_len();
}

long long shuttle_c_frame_qword(int index) {
  return static_cast<long long>(shuttle::detail::backend::frame_qword(index));
}

int shuttle_c_event_create() {
  const WeaveScopeGuard weave_scope(SHUTTLE_WEAVE_SV_PACKAGE);
  try {
    return shuttle::detail::event_registry_create();
  } catch (const std::exception& e) {
    set_last_error(e.what());
    return 0;
  }
}

void shuttle_c_event_set(int ev_id) {
  const WeaveScopeGuard weave_scope(SHUTTLE_WEAVE_SV_PACKAGE);
  try {
    shuttle::detail::event_registry_set(ev_id);
  } catch (const std::exception& e) {
    set_last_error(e.what());
  }
}

// shuttle_pkg.sv's shuttle_event::close() goes through here (each event
// occupies one id in the C++ registry; not destroying it leaks it until the
// end of simulation — which is also acceptable).
void shuttle_c_event_destroy(int ev_id) {
  const WeaveScopeGuard weave_scope(SHUTTLE_WEAVE_SV_PACKAGE);
  try {
    shuttle::detail::event_registry_destroy(ev_id);
  } catch (const std::exception& e) {
    set_last_error(e.what());
  }
}

}  // extern "C"
