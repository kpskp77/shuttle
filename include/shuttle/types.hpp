#pragma once

// Common foundation types: callback capability, overflow policy, `publish_now`
// policy, subscription options, and callback signature wrappers.

#include <cstddef>
#include <functional>
#include <utility>
#include <vector>

namespace shuttle {

struct Meta;

// ── Callback entry points ────────────────────────────────────────────────
// A subscriber declares which **callback entry points** it provides:
//   blocking    — blocking entry (SV export task): the callback may consume
//                 simulation time, so it can only be invoked from background
//                 scheduling
//   nonblocking — nonblocking entry (SV export function): the callback is
//                 guaranteed not to block, so it may be invoked synchronously
//                 on the publisher's stack
// These two words describe "whether the callback may consume simulation
// time", not "whether the entry blocks the caller" (`publish_now` never
// invokes blocking entries at all).
// Rule: the publish flavor only decides *scheduling* (none / serial /
// concurrent); the entry is decided by the function called at registration:
//   publish_now only invokes nonblocking entries (targets with a blocking
//   entry are handled per NowPolicy);
//   publish_serial/concurrent invoke whichever entry that registration used
//   (blocking or nonblocking) — there is no "preferred entry": one
//   registration has exactly one entry; for both, register twice (on the SV
//   side, call shuttle_subscribe and shuttle_subscribe_nb on the same object).
enum class Capability {
  blocking,
  nonblocking,
};

// What publish_now does when it meets a subscriber whose entry is blocking
// (such subscribers cannot be called on the publisher's stack). The policy
// applies only to these targets, regardless of whether the same topic also
// has nonblocking subscribers.
enum class NowPolicy {
  reject = 0,  // throw CapabilityError: the frame is never published at all
               // (type binding / seq / retained are all untouched)
  skip = 1,    // skip these subscribers: not counted in target_count, only in
               // Publication::skipped() and the per-subscriber / per-topic
               // skipped counters (the frame is still delivered to the other
               // subscribers — this is not "message loss")
  defer = 2,   // default: hand the frame to the topic's background serial
               // chain (shared with publish_serial) and return a waitable
               // handle; this keeps "background delivery order per topic ==
               // publish call order"
};

// ── nb() marker: declare a C++ callback as non-blocking ────────────────
// The contract is upheld solely by the user: C++ cannot enforce "no blocking
// primitives" at compile time, and the broker performs no runtime check.
// If the callback suspends, it suspends the publisher itself inside
// `publish_now`'s synchronous section.
template <typename F>
struct NonBlocking {
  F fn;
};
template <typename F>
NonBlocking<std::decay_t<F>> nb(F&& f) {
  return {std::forward<F>(f)};
}

// Overloads that take NonBlocking<F> are more specialized, so `nb(cb)` selects
// them by partial ordering alone — no enable_if, no trait, no runtime flag: the
// entry kind is carried by the *type* of the registration argument
// (subscribe<T>(topic, nb(cb)) vs subscribe<T>(topic, cb)).

// ── Subscription options ────────────────────────────────────────────────
// The broker keeps no per-subscriber queue: delivery executes synchronously
// inside a scheduling unit, so for a plain `subscribe` the two fields below are
// still ignored (see "Known limitations" in docs/architecture.md). A Mailbox
// (include/shuttle/mailbox.hpp) is the one consumer that honours them: it owns
// the per-subscriber queue, so `max_pending` / `overflow` describe its backlog.
enum class OverflowPolicy {
  // Not expressible for a mailbox: a delivery callback must never block (it may
  // run inside publish_now on the publisher's stack), so a bounded mailbox is
  // rejected at construction instead of stalling the deliverer.
  block,
  drop_oldest,  // drop the oldest queued item and count it
  drop_newest,  // drop the incoming item and count it
  reject,       // throw MailboxOverflowError: the delivery is recorded as a
                // subscriber Failure (visible in Publication::failures() and in
                // the failed component of dropped())
};

struct SubOptions {
  std::size_t max_pending = 0;  // per-subscriber pending-message limit; 0 = unbounded
  OverflowPolicy overflow = OverflowPolicy::block;  // used only when max_pending > 0
};

// ── Callback signatures ─────────────────────────────────────────────────
// Typed callback: void(const T&, const Meta&) — wrapped by the subscribe<T> template
// Raw-byte callback (debugging / pass-through / unregistered types):
using ByteCallback = std::function<void(const std::vector<std::byte>&, const Meta&)>;

}  // namespace shuttle
