// svdpi backend: the SV-side import surface (`shuttle_c_*`; definitions in
// docs/backends.md).
//
// Only "DPI-C type ↔ C++ type" translation and error reporting happen here;
// no broker state is held. The broker-side external entry facade is `ext_*` /
// `pub_*` in facade.cpp in the same directory (declared in
// svdpi_internal.hpp, invoked as detail::*).
// The `detail::backend` implementation (dispatch / retired channel)
// is in bridge.cpp.

#include <svdpi.h>

#include <cstdint>
#include <cstring>
#include <memory>
#include <shuttle/detail/abi.hpp>
#include <shuttle/exception.hpp>
#include <shuttle/requester.hpp>
#include <shuttle/trigger.hpp>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>
#include <weave/event.hpp>
#include <weave/sv.hpp>
#include <weave/task.hpp>
#include <weave/time.hpp>

#include "backend/backend.hpp"
#include "detail/trigger.hpp"
#include "svdpi_internal.hpp"

// Deliberately no `import "DPI-C"` of weave's exports here: a consumer that
// declares weave's exports as its own imports bypasses weave's DPI backend (and
// with it the scope fixing that backend does). Everything below drives weave
// through its C++ API instead, which also means no svSetScope for weave_pkg is
// needed anywhere in shuttle — weave owns its own package's scope.

namespace {

using shuttle::detail::kUnknownPub;
using shuttle::detail::last_error;
using shuttle::detail::PubLifecycle;
using shuttle::detail::set_last_error;
using shuttle::detail::TrigKind;
using shuttle::detail::WaitStatus;
using shuttle::detail::wire;

// On-wire strategy code → NowPolicy. The numeric values are protocol constants
// (docs/backends.md pins NowPolicy::reject/skip/defer to SHUTTLE_NOW_* 0/1/2),
// so the code is the enum; anything else falls back to the default, defer.
shuttle::NowPolicy now_policy_of(int v) {
  switch (static_cast<shuttle::NowPolicy>(v)) {
    case shuttle::NowPolicy::reject:
    case shuttle::NowPolicy::skip:
      return static_cast<shuttle::NowPolicy>(v);
    default:
      return shuttle::NowPolicy::defer;
  }
}

// Read a byte stream from an SV open array (byte payload[] → contiguous 8-bit elements).
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
shuttle::Trigger build_trigger(int trig_kind, long long trig_arg, const char* trig_name) {
  using namespace shuttle;
  switch (static_cast<TrigKind>(trig_kind)) {
    case TrigKind::none:
      return Trigger();
    case TrigKind::delay:
      return after(weave::duration(trig_arg));
    case TrigKind::completion:  // wait for another publication to complete; its subscriber failures
                                // also count as "reached a terminal state"
      return detail::TriggerAccess::from_fn([pub_id = static_cast<int>(trig_arg)]() {
        int status = 0;
        detail::pub_wait(pub_id, 0, &status);
      });
    case TrigKind::edge: {  // wait for a weave edge. Only the *name* crosses the boundary: the
      // Edge constructor is a find-or-insert into weave's own name table, so the SV
      // side and this side end up holding the same id without shuttle owning a
      // registry. Waiting through Edge::wait() (not a raw weave_sv_wait) keeps the
      // cancellation injection and the wait graph that weave's dispatch installs.
      if (trig_name == nullptr || *trig_name == '\0')
        throw std::invalid_argument("shuttle: TRIG_EDGE requires an edge name");
      auto edge = std::make_shared<weave::sv::Edge>(trig_name);
      return detail::TriggerAccess::from_fn([edge]() { edge->wait(); });
    }
    default:
      throw std::invalid_argument("shuttle: unknown trig_kind " + std::to_string(trig_kind));
  }
}

// Common skeleton of `serial`/`concurrent`: frame bytes → serial/concurrent
// scheduling → pub_id. The two flavors differ only in which broker entry they
// call; the rest (frame read, trigger, error reporting) is identical.
int publish_frame_bg(const char* topic, const char* type_tag, const svOpenArrayHandle payload,
                     int trig_kind, long long trig_arg, const char* trig_name, unsigned int corr_id,
                     bool concurrent) {
  try {
    std::vector<std::byte> frame = read_payload(payload);
    shuttle::Trigger trig = build_trigger(trig_kind, trig_arg, trig_name);
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
  // waits). No scope switch is needed for that: weave's DPI backend fixes the
  // svScope around every one of its own imports, so its exports resolve
  // whatever scope the caller happens to be in.
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
                             const char* trig_name, unsigned int corr_id) {
  return publish_frame_bg(topic, type_tag, payload, trig_kind, trig_arg, trig_name, corr_id, false);
}

// `publish_concurrent`: background, one task per subscriber, parallel across subscribers and FIFO
// per (topic, subscriber). Returns pub_id (>0), or 0 on failure.
int shuttle_c_publish_concurrent(const char* topic, const char* type_tag,
                                 const svOpenArrayHandle payload, int trig_kind, long long trig_arg,
                                 const char* trig_name, unsigned int corr_id) {
  return publish_frame_bg(topic, type_tag, payload, trig_kind, trig_arg, trig_name, corr_id, true);
}

// DPI task: block until one publication completes.
// The suspension is a leaf wait on a weave event (weave allows an ownerless leaf
// wait: the importing SV process runs no weave task, and there is nothing to
// cancel or to publish in the wait graph), so an ordinary initial/always process
// can wait directly.
// timeout_fs<=0 means wait forever.
void shuttle_c_wait_pub(int pub_id, long long timeout_fs, int* status) {
  int sink = 0;
  if (!status) status = &sink;
  try {
    const int st = shuttle::detail::pub_state(pub_id);
    if (st == kUnknownPub) {
      set_last_error("shuttle: unknown pub_id " + std::to_string(pub_id));
      *status = wire(WaitStatus::failed);
      return;
    }
    if (st != wire(PubLifecycle::running)) {  // already completed: answer without suspending
      *status = (st == wire(PubLifecycle::done_with_failures)) ? wire(WaitStatus::failed)
                                                               : wire(WaitStatus::ok);
      return;
    }
    // A frame whose chain is held by the very SV process that waits for it can
    // never complete; the check ends the run (weave::fatal → $fatal) instead of
    // suspending forever.
    shuttle::detail::pub_check_self_blocked(pub_id);

    auto wake = std::make_shared<weave::Event>();
    shuttle::detail::pub_on_complete(pub_id, [wake] { wake->set(); });

    // The deadline leg needs a task of its own (a timeout is a cancellation of
    // something), and it must not outlive this wait: whoever loses is taken down.
    weave::Task deadline;
    if (timeout_fs > 0) {
      deadline = weave::create_task("shuttle-pub-deadline", [wake, timeout_fs] {
        weave::sleep(weave::duration(timeout_fs));
        wake->set();
      });
    }
    wake->wait();
    if (deadline.valid()) deadline.cancel();

    const int fin = shuttle::detail::pub_state(pub_id);
    if (fin == wire(PubLifecycle::running)) {  // the deadline leg won
      *status = wire(WaitStatus::timeout);
      return;
    }
    *status = (fin == wire(PubLifecycle::done_with_failures)) ? wire(WaitStatus::failed)
                                                              : wire(WaitStatus::ok);
  } catch (const std::exception& e) {
    set_last_error(e.what());
    *status = wire(WaitStatus::failed);
  } catch (...) {
    set_last_error("shuttle: wait cancelled or unknown exception");
    *status = wire(WaitStatus::failed);
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
  shuttle::detail::ext_unsub(sv_sid);
}

void shuttle_c_set_retained(const char* topic, int retained) {
  try {
    shuttle::detail::topic_set_retained(topic ? topic : "", retained != 0);
  } catch (const std::exception& e) {
    set_last_error(e.what());
  }
}

// Request/response: allocate the next correlation id. SV has no counterpart to
// the Requester's counter, so the allocation point stays C++-side and both
// languages draw from one sequence — an SV corr_id can never collide with a C++
// one on a shared response topic. Never 0 (0 means "uncorrelated").
int shuttle_c_take_corr_id() {
  return static_cast<int>(shuttle::detail::take_corr_id());
}

// Claim `topic` as a response channel, exactly as the C++ Requester and serve()
// do: from here on a blocking-entry subscription on it is refused. Returns 0 on
// success, 1 on failure (reason in shuttle_c_last_error).
int shuttle_c_mark_responses(const char* topic) {
  try {
    shuttle::detail::topic_mark_responses(topic ? topic : "");
    return 0;
  } catch (const std::exception& e) {
    set_last_error(e.what());
    return 1;
  } catch (...) {
    set_last_error("shuttle: unknown exception");
    return 1;
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

}  // extern "C"
