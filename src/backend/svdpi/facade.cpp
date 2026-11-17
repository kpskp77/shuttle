// svdpi backend: external facade — byte-frame publish, external-subscriber
// registration, per-pub_id queries/waits, and the cross-language event
// registry (SV shuttle_event / SHUTTLE_TRIG_EVENT).
//
// This is the "FFI entry" half of the broker, opposite
// `backend/backend.hpp` (the broker→SV exit direction). svdpi is the only
// backend in this repository with an FFI channel (systemc has no external
// subscribers and there will never be a second FFI caller), so it lives in the
// svdpi directory and the systemc build does not compile this file. The price
// is that the svdpi directory sees broker internals (broker/broker.hpp) — an
// explicitly accepted, permanent asymmetry; the systemc backend still knows
// the broker only through the `backend/backend.hpp` narrow port.
//
// A separate file rather than being merged into dpi.cpp: everything here is
// broker orchestration (preflight / envelope / target-set snapshot / serial
// chain), while dpi.cpp only does DPI-C type translation — the two abstraction
// layers are not mixed.
//
// The two name families:
//   ext_*  = actions coming in from an external caller (frames from outside,
//            subscribers living outside);
//   pub_*  = handle queries/waits by pub_id.
//
// Accounting semantics (who counts into what) are decided by the broker's
// target_finished / pub_complete; this file only does the "handle ↔ id"
// translation and error reporting. The namespace stays shuttle::detail:
// declarations are in svdpi_internal.hpp and dpi.cpp calls them as detail::*.

#include <algorithm>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <shuttle/detail/abi.hpp>
#include <shuttle/publication.hpp>
#include <shuttle/trigger.hpp>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>
#include <weave/event.hpp>

#include "broker/broker.hpp"
#include "detail/publication.hpp"
#include "svdpi_internal.hpp"

namespace shuttle::detail {

namespace {
// Per-process most-recent error message (data source for shuttle_c_last_error;
// the single-active-process guarantee makes locking unnecessary).
std::string g_last_error;
}  // namespace

void set_last_error(std::string msg) {
  g_last_error = std::move(msg);
}
const std::string& last_error() {
  return g_last_error;
}

// ── External entry facade (errors reported via set_last_error + return code/exception) ──

Publication ext_pub_now(const std::string& topic, const std::string& tag,
                        const std::vector<std::byte>& frame, NowPolicy policy) {
  Broker& b = Broker::get();
  b.now_preflight(
      topic, tag,
      policy);  // decide before building the frame: reject consumes no seq / refreshes no retained
  auto env = b.make_envelope_bytes(topic, tag, frame);
  return detail_pub_access::make(dispatch_now(b, std::move(env), policy));
}

Publication ext_pub_serial(const std::string& topic, const std::string& tag,
                           const std::vector<std::byte>& frame, Trigger trig) {
  Broker& b = Broker::get();
  auto env = b.make_envelope_bytes(topic, tag, frame);
  auto targets = b.match_subs(topic, env->meta.type_tag);
  return detail_pub_access::make(
      b.start_serial(topic, std::move(env), std::move(trig), std::move(targets)));
}

Publication ext_pub_concurrent(const std::string& topic, const std::string& tag,
                               const std::vector<std::byte>& frame, Trigger trig) {
  Broker& b = Broker::get();
  auto env = b.make_envelope_bytes(topic, tag, frame);
  auto targets = b.match_subs(topic, env->meta.type_tag);
  return detail_pub_access::make(
      b.start_concurrent(topic, std::move(env), std::move(trig), std::move(targets)));
}

int pub_id(const Publication& p) {
  if (!p.valid()) return 0;
  const std::shared_ptr<PubState> pub = detail_pub_access::state(p);
  return pub ? pub->id : 0;
}

int ext_sub(std::string pattern, int caps, std::string type_tag, int handle) {
  try {
    Broker& b = Broker::get();
    // One registration = one entry: caps must be exactly 1 (`shuttle_subscribe`) or 2
    // (`shuttle_subscribe_nb`). Any other value is undefined — otherwise `now`
    // and `serial`/`concurrent` would take different entries for the same
    // subscriber, violating invariant 4; rejected outright.
    if (caps != 1 && caps != 2)
      throw std::invalid_argument(
          "shuttle: subscription capability must be exactly one bit (blocking=1 / nonblocking=2)");
    b.register_sub(std::move(pattern), SubKind::external,
                   caps == 1 ? Capability::blocking : Capability::nonblocking, std::move(type_tag),
                   nullptr, {}, {}, handle);
    return 0;
  } catch (const std::exception& e) {
    set_last_error(e.what());
    return 1;
  }
}

void ext_unsub(int handle) {
  Broker& b = Broker::get();
  b.subs.erase(std::remove_if(b.subs.begin(), b.subs.end(),
                              [handle](const std::shared_ptr<Subscriber>& s) {
                                return s->kind == SubKind::external && s->handle == handle;
                              }),
               b.subs.end());
}

namespace {

// Fetch the publication state by id (unknown id = nullptr): the single shared
// entry point for the five facade functions below, so they need not each
// repeat find + end-check.
std::shared_ptr<PubState> pub_of(int pub_id) {
  Broker& b = Broker::get();
  const auto it = b.pubs.find(pub_id);
  return it != b.pubs.end() ? it->second : nullptr;
}

}  // namespace

bool pub_done(int pub_id) {
  const auto pub = pub_of(pub_id);
  return pub && pub->done_flag;
}

std::string pub_error(int pub_id) {
  const auto pub = pub_of(pub_id);
  return pub ? pub->first_error : std::string("shuttle: unknown pub_id");
}

void pub_wait(int pub_id, std::int64_t timeout_fs, int* status) {
  const std::shared_ptr<PubState> pub = pub_of(pub_id);
  if (!pub) {
    set_last_error("shuttle: unknown pub_id " + std::to_string(pub_id));
    *status = 2;
    return;
  }
  if (timeout_fs <= 0) {
    pub->done_ev.wait();
  } else if (!wait_pub_timed(pub, weave::duration(timeout_fs))) {
    *status = 1;
    return;
  }
  *status = pub->failures.empty() ? 0 : 2;
}

long long pub_stat(int pub_id, int kind) {
  const std::shared_ptr<PubState> pub = pub_of(pub_id);
  // The on-wire kind is a bare int: validate the range before converting to
  // the enum; out-of-range and unknown pub_id share the same answer (-1).
  if (!pub || kind < 0 || kind > static_cast<int>(PubStatKind::retired)) return -1;
  switch (static_cast<PubStatKind>(kind)) {
    case PubStatKind::target_count:
      return static_cast<long long>(pub->target_count);
    case PubStatKind::finished:
      return static_cast<long long>(pub->finished);
    case PubStatKind::skipped:
      return static_cast<long long>(pub->skipped);
    case PubStatKind::deferred:
      return static_cast<long long>(pub->deferred);
    case PubStatKind::retired:
      return static_cast<long long>(pub->retired);
  }
  return -1;  // unreachable (range validated, enum fully covered)
}

int pub_state(int pub_id) {
  const std::shared_ptr<PubState> pub = pub_of(pub_id);
  if (!pub) return -1;
  if (!pub->done_flag) return 0;
  return pub->failures.empty() ? 1 : 2;
}

void pub_on_complete(int pub_id, std::function<void()> fn) {
  const std::shared_ptr<PubState> pub = pub_of(pub_id);
  if (!pub) return;
  if (pub->done_flag) {
    fn();
    return;
  }
  pub->on_complete.push_back(std::move(fn));
}

// ── Cross-language event registry (SV-side shuttle_event / SHUTTLE_TRIG_EVENT) ──
// Events are created only on the SV side (shuttle_c_event_create); the C++
// side never holds a handle — access is by id, so the registry alone owns the
// objects and destroy frees them (SV's shuttle_event::close() goes through
// here).
namespace {
struct EventSlot {
  int id;
  weave::Event ev;
};

std::map<int, std::shared_ptr<EventSlot>> g_events;
int g_next_event_id = 1;

std::shared_ptr<EventSlot> event_lookup(int ev_id) {
  if (const auto it = g_events.find(ev_id); it != g_events.end()) return it->second;
  return nullptr;
}
}  // namespace

// The wait entry of TRIG_EVENT (SV references events by id): must be called
// in a weave task context.
void event_wait_id(int ev_id) {
  const auto slot = event_lookup(ev_id);
  if (!slot)
    throw std::invalid_argument("shuttle: unknown or destroyed event id " + std::to_string(ev_id));
  slot->ev.wait();
}

int event_registry_create() {
  auto slot = std::make_shared<EventSlot>(EventSlot{g_next_event_id++, weave::Event{}});
  const int id = slot->id;
  g_events[id] = std::move(slot);
  return id;
}

void event_registry_set(int ev_id) {
  const auto slot = event_lookup(ev_id);
  if (!slot)
    throw std::invalid_argument("shuttle: unknown or destroyed event id " + std::to_string(ev_id));
  slot->ev.set();
}

void event_registry_destroy(int ev_id) {
  g_events.erase(ev_id);
}

}  // namespace shuttle::detail
