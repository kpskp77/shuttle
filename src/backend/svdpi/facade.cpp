// svdpi backend: external facade — byte-frame publish, external-subscriber
// registration, per-pub_id queries/waits.
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
#include <memory>
#include <optional>
#include <shuttle/detail/abi.hpp>
#include <shuttle/publication.hpp>
#include <shuttle/trigger.hpp>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <weave/time.hpp>

#include "broker/broker.hpp"
#include "detail/publication.hpp"
#include <shuttle/detail/trace.hpp>
#include "svdpi_internal.hpp"

namespace shuttle::detail {

namespace {
// Per-process most-recent error message (data source for shuttle_c_last_error;
// the single-active-process guarantee makes locking unnecessary).
std::string g_last_error;

// Only external publications are addressable by pub_id (the SV side keeps the
// handle as an int), so only they enter the id → PubState registry. C++
// publications retain their state through the returned Publication handle;
// registering those too would grow this map for the whole simulation with no
// reader. The insertion happens before the handle is returned to SV, so
// pub_wait/pub_on_complete never observe a gap.
Publication track_pub(Publication pub) {
  if (auto state = detail_pub_access::state(pub)) Broker::get().pubs[state->id] = std::move(state);
  return pub;
}

// SV's registration selector (1 = `shuttle_subscribe`, 2 = `shuttle_subscribe_nb`) →
// Capability. Precisely one of the two: never a set of bits, and never "none" —
// the entry is a property of the registering call, not of the runtime
// (AGENTS.md invariant 4). nullopt = a value the protocol does not define.
std::optional<Capability> capability_of(int caps) {
  switch (caps) {
    case 1:
      return Capability::blocking;
    case 2:
      return Capability::nonblocking;
    default:
      return std::nullopt;
  }
}
}  // namespace

void set_last_error(std::string msg) {
  g_last_error = std::move(msg);
}
const std::string& last_error() {
  return g_last_error;
}

// ── External entry facade (errors reported via set_last_error + return code/exception) ──

// Frames arrive as a byte buffer owned by the caller (dpi.cpp's read_payload).
// Taking it by value lets the raw path move the buffer into the immutable
// envelope payload instead of copying it a second time; the typed path decodes
// from it before it is consumed.
Publication ext_pub_now(std::string_view topic, std::string_view tag, std::vector<std::byte> frame,
                        NowPolicy policy, std::uint32_t corr_id) {
  Broker& b = Broker::get();
  // decide before building the frame: reject consumes no seq / refreshes no retained
  b.now_preflight(topic, tag, policy);
  auto env = b.make_envelope_bytes(topic, tag, std::move(frame), corr_id);
  trace_svdpi()->trace("sv publish_now '{}' type={} seq={} pow={} corr={}", topic, tag,
                       env->meta.topic_seq, static_cast<int>(policy), corr_id);
  return track_pub(detail_pub_access::make(dispatch_now(b, std::move(env), policy)));
}

Publication ext_pub_serial(std::string_view topic, std::string_view tag,
                           std::vector<std::byte> frame, Trigger trig, std::uint32_t corr_id) {
  Broker& b = Broker::get();
  auto env = b.make_envelope_bytes(topic, tag, std::move(frame), corr_id);
  trace_svdpi()->trace("sv publish_serial '{}' type={} seq={} corr={}", topic, tag,
                       env->meta.topic_seq, corr_id);
  auto targets = b.match_subs(topic, env->meta.type_tag);
  return track_pub(detail_pub_access::make(
      b.start_serial(topic, std::move(env), std::move(trig), std::move(targets))));
}

Publication ext_pub_concurrent(std::string_view topic, std::string_view tag,
                               std::vector<std::byte> frame, Trigger trig, std::uint32_t corr_id) {
  Broker& b = Broker::get();
  auto env = b.make_envelope_bytes(topic, tag, std::move(frame), corr_id);
  trace_svdpi()->trace("sv publish_concurrent '{}' type={} seq={} corr={}", topic, tag,
                       env->meta.topic_seq, corr_id);
  auto targets = b.match_subs(topic, env->meta.type_tag);
  return track_pub(detail_pub_access::make(
      b.start_concurrent(topic, std::move(env), std::move(trig), std::move(targets))));
}

int pub_id(const Publication& p) {
  if (!p.valid()) return kNoPub;
  const std::shared_ptr<PubState> pub = detail_pub_access::state(p);
  return pub ? pub->id : kNoPub;
}

int ext_sub(std::string pattern, int caps, std::string type_tag, int handle) {
  try {
    Broker& b = Broker::get();
    // One registration = one entry: caps must be exactly 1 (`shuttle_subscribe`) or 2
    // (`shuttle_subscribe_nb`). Any other value is undefined — otherwise `now`
    // and `serial`/`concurrent` would take different entries for the same
    // subscriber, violating invariant 4; rejected outright.
    const std::optional<Capability> entry = capability_of(caps);
    if (!entry)
      throw std::invalid_argument(
          "shuttle: subscription capability must be exactly one bit (blocking=1 / nonblocking=2)");
    b.register_sub(std::move(pattern), SubKind::external, *entry, std::move(type_tag), nullptr, {},
                   {}, handle);
    return kFacadeOk;
  } catch (const std::exception& e) {
    trace_svdpi()->debug("sv subscribe failed: {}", e.what());
    set_last_error(e.what());
    return kFacadeError;
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

// Fetch the publication state by id (unknown id = nullptr): the single shared entry point for the
// five facade functions below, so they need not each repeat find + end-check.
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
    trace_svdpi()->debug("sv wait: unknown pub_id {}", pub_id);
    set_last_error("shuttle: unknown pub_id " + std::to_string(pub_id));
    *status = code_of(WaitStatus::failed);
    return;
  }
  trace_svdpi()->trace("sv wait: pub {} (timeout {} fs)", pub_id, timeout_fs);
  if (timeout_fs <= 0) {  // the protocol spells "no deadline" as 0
    pub->done_ev.wait();
  } else if (!wait_pub_timed(pub, weave::duration(timeout_fs))) {
    *status = code_of(WaitStatus::timeout);
    return;
  }
  *status = code_of(pub->failures.empty() ? WaitStatus::ok : WaitStatus::failed);
}

long long pub_stat(int pub_id, int kind) {
  const std::shared_ptr<PubState> pub = pub_of(pub_id);
  // The protocol value is a bare int: validate the range before converting to
  // the enum; out-of-range and unknown pub_id share the same answer.
  if (!pub || kind < code_of(PubStatKind::target_count) || kind > code_of(PubStatKind::retired))
    return kUnknownPub;
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
  return kUnknownPub;  // unreachable (range validated, enum fully covered)
}

int pub_state(int pub_id) {
  const std::shared_ptr<PubState> pub = pub_of(pub_id);
  if (!pub) return kUnknownPub;
  if (!pub->done_flag) return code_of(PubLifecycle::running);
  return code_of(pub->failures.empty() ? PubLifecycle::done : PubLifecycle::done_with_failures);
}

void pub_check_self_blocked(int pub_id) {
  const std::shared_ptr<PubState> pub = pub_of(pub_id);
  if (!pub) return;
  check_self_blocked(pub);
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

}  // namespace shuttle::detail
