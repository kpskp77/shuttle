#pragma once

// Mailbox: "wait for the next frame" instead of "let a callback handle each
// frame" — a subscription with a queue attached to it.
//
//   shuttle::Mailbox<Resp> replies("dev.resp");
//   auto req = shuttle::Publisher<Req>("dev.req").publish_serial(Req{...});
//   req.wait();                       // every responder's callback has returned
//   if (auto got = replies.recv(1_ms)) use(got->value, got->meta);
//
// ── What a mailbox is, and is not ───────────────────────────────────────
// A mailbox **is one subscriber** (invariant: one registration = one subscriber
// = one entry): `mailbox<T>(pattern)` expands to `subscribe<T>(pattern, nb(cb))`
// with a callback that copies the frame into the queue. It therefore joins the
// target set of every matching publication, shows up in
// Publication::subscriber_count() and in topic subscriber counts, receives
// retained replays and honours wildcard patterns exactly like any subscription.
// There is no third entry kind and no broker-side mailbox state.
//
// It registers the **nonblocking** entry because the internal callback only
// copies a value into a queue: it must never block, and cannot consume
// simulation time. The consequence is useful — `publish_now` fills a mailbox
// synchronously on the publisher's stack, so a request/response pair between
// nonblocking peers needs no background scheduling at all.
//
// A mailbox answers "a frame arrived"; Publication answers "every subscriber's
// callback has finished". Request/response usually wants both.
//
// ── Ordering ────────────────────────────────────────────────────────────
// The queue order is the order in which this subscriber's callback was invoked:
// `serial` = join order, `concurrent` = per-subscriber chain order (also publish
// order, since each subscriber's chain is FIFO), `publish_now` = registration
// order. Ordering across flavors is not promised.
//
// ── Capacity and overflow (SubOptions) ──────────────────────────────────
// max_pending == 0 (default) is unbounded. A bounded mailbox must choose
// OverflowPolicy::drop_oldest / drop_newest / reject: `block` is rejected at
// construction because the callback may run inside `publish_now`, where blocking
// the deliverer would deadlock the publisher. Overflow accounting is separate
// from Subscription::dropped(): the latter counts frames that did not reach this
// subscriber, while overflow counts frames that reached it and were then dropped
// from the queue (Mailbox::overflowed()); under `reject` the frame is instead
// recorded as a normal delivery Failure. Note that Mailbox::received() counts
// frames accepted into the queue, so under drop_oldest it includes frames that
// were later evicted — received() - size() is not the overflow count, and the two
// counters must be read independently.
//
// ── Lifetime ────────────────────────────────────────────────────────────
// Copies of a Mailbox share one queue (Subscription semantics). close() is
// idempotent: it unsubscribes and wakes waiters; already queued items stay
// readable, and only after the queue is drained do the blocking forms throw
// MailboxClosed. Destroying the last handle unsubscribes as well.
//
// ── Relationship to Requester (requester.hpp) ────────────────────────────
// They are siblings, not layers: this file is the *timing* primitive ("a frame
// arrived, take it when you want it"), Requester is the *session* primitive ("the
// answers to this request are back"). Requester deliberately does not use
// Mailbox, for four reasons that would all cost correctness:
//   · a mailbox is one FIFO: replies have to be routed by correlation id, and
//     out-of-order arrivals from several responders must not be re-sequenced;
//   · a mailbox wakes one queue, Requester wakes per call (one Event per Call),
//     so two outstanding requests are waited for independently;
//   · a reply that belongs to a cancelled or dropped call must still be
//     *counted* (Requester::stale_responses()), which needs the corr_id->call table;
//   · the reply subscription must be shared by all calls of one Requester, not
//     one queue per call and not one subscription per call.
// Mailbox is the right tool when the responder set is fixed and one queue of
// replies is enough (the example at the top of this file); reach for Requester
// when several requests are outstanding at once or the id must be correlated.

#include <cstddef>
#include <cstdint>
#include <deque>
#include <optional>
#include <shuttle/detail/abi.hpp>
#include <shuttle/exception.hpp>
#include <shuttle/meta.hpp>
#include <shuttle/subscription.hpp>
#include <shuttle/types.hpp>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>
#include <weave/event.hpp>
#include <weave/sync.hpp>
#include <weave/task.hpp>
#include <weave/time.hpp>

namespace shuttle {

// One queued frame: a copy of the payload plus the metadata of the publication
// that carried it (topic / type_tag / seq / time_fs).
template <typename T>
struct MailboxItem {
  T value;
  Meta meta;
};

namespace detail {

struct MailboxAccess;  // named construction channel, defined below

// Queue + wake event shared by all copies of one mailbox handle.
template <typename T>
struct MailboxState {
  explicit MailboxState(const SubOptions& opts)
      : capacity(opts.max_pending), overflow(opts.overflow) {}

  std::deque<MailboxItem<T>> items;
  weave::Event not_empty;    // level-triggered: "the queue is non-empty"
  std::size_t capacity = 0;  // 0 = unbounded
  OverflowPolicy overflow = OverflowPolicy::drop_oldest;
  bool closed = false;
  std::uint64_t received = 0;
  std::uint64_t overflowed = 0;
};

// A bounded mailbox cannot honour OverflowPolicy::block: the callback may run
// inside publish_now, where blocking the deliverer deadlocks the publisher.
inline void check_mailbox_options(const SubOptions& opts) {
  if (opts.max_pending == 0 || opts.overflow != OverflowPolicy::block) return;
  throw std::invalid_argument(
      "shuttle: a bounded mailbox needs OverflowPolicy::drop_oldest, drop_newest or reject — "
      "the delivery callback must not block (OverflowPolicy::block)");
}

// Called from the delivery path: push one frame, never block.
template <typename T>
void mailbox_push(MailboxState<T>& st, T value, const Meta& meta) {
  if (st.closed) return;  // closed: no new frames (queued ones stay readable)
  if (st.capacity != 0 && st.items.size() >= st.capacity) {
    switch (st.overflow) {
      case OverflowPolicy::drop_oldest:
        st.items.pop_front();
        ++st.overflowed;
        break;
      case OverflowPolicy::drop_newest:
        ++st.overflowed;
        return;
      case OverflowPolicy::reject:
        throw MailboxOverflowError("shuttle: mailbox overflowed (capacity " +
                                   std::to_string(st.capacity) + ")");
      case OverflowPolicy::block:
        break;  // unreachable: rejected by check_mailbox_options
    }
  }
  st.items.push_back(MailboxItem<T>{std::move(value), meta});
  ++st.received;
  st.not_empty.set();
}

template <typename T>
MailboxItem<T> mailbox_pop(MailboxState<T>& st) {
  MailboxItem<T> item = std::move(st.items.front());
  st.items.pop_front();
  // Clear the level flag when the queue runs dry, otherwise the next wait would
  // return immediately and spin (weave's Event keeps its flag set until clear).
  if (st.items.empty()) st.not_empty.clear();
  return item;
}

// Blocking receive: no deadline.
template <typename T>
MailboxItem<T> mailbox_recv(MailboxState<T>& st) {
  while (st.items.empty()) {
    if (st.closed) throw MailboxClosed("shuttle: receiving from a closed mailbox (queue drained)");
    st.not_empty.wait();
  }
  return mailbox_pop(st);
}

// Receive with a deadline; nullopt means "nothing arrived in time" and never
// loses a frame (a frame that arrives later stays queued).
template <typename T>
std::optional<MailboxItem<T>> mailbox_recv(const std::shared_ptr<MailboxState<T>>& st_holder,
                                           weave::duration timeout) {
  const std::int64_t deadline = now_fs() + timeout.count();
  while (st_holder->items.empty()) {
    if (st_holder->closed)
      throw MailboxClosed("shuttle: receiving from a closed mailbox (queue drained)");
    const std::int64_t remaining = deadline - now_fs();
    if (remaining <= 0) return std::nullopt;  // checked after the queue: no frame is lost
    // Race a one-shot waiter against the clock, the same shape as wait_pub_timed:
    // cancelling the waiter consumes nothing, so a late frame stays queued.
    // The state is captured by value: the waiter must not touch a dead reference
    // if the timeout wins.
    weave::Task waiter =
        weave::create_task("shuttle-mailbox-wait", [st_holder] { st_holder->not_empty.wait(); });
    try {
      weave::wait_for(waiter, weave::duration(remaining));
    } catch (const weave::TimeoutError&) {
      return std::nullopt;
    }
  }
  return mailbox_pop(*st_holder);
}

}  // namespace detail

template <typename T>
class Mailbox {
public:
  Mailbox() = default;

  [[nodiscard]] bool valid() const { return static_cast<bool>(state_) && sub_.valid(); }

  // ── Receiving (blocking primitives: they need a weave task context) ──
  // Blocks until a frame arrives; throws MailboxClosed when the mailbox is
  // closed and its queue is drained.
  MailboxItem<T> recv() { return detail::mailbox_recv(*require_state()); }
  // nullopt = timed out (or the mailbox was closed while empty? no: closed
  // throws, same as the blocking form)
  std::optional<MailboxItem<T>> recv(weave::duration timeout) {
    return detail::mailbox_recv(require_state(), timeout);
  }

  // Non-blocking form: nullopt when nothing is queued (never throws, so a
  // polling loop can use it after close() as well).
  [[nodiscard]] std::optional<MailboxItem<T>> try_recv() {
    auto st = require_state();
    if (st->items.empty()) return std::nullopt;
    return detail::mailbox_pop(*st);
  }

  // ── State and diagnostics ──
  [[nodiscard]] std::size_t size() const { return state_ ? state_->items.size() : 0; }
  [[nodiscard]] bool closed() const { return state_ && state_->closed; }
  [[nodiscard]] std::uint64_t received() const { return state_ ? state_->received : 0; }
  [[nodiscard]] std::uint64_t overflowed() const { return state_ ? state_->overflowed : 0; }
  [[nodiscard]] std::string pattern() const { return sub_.valid() ? sub_.topic() : std::string(); }

  // The underlying subscription: received() / dropped() of the subscription
  // count frames at the delivery level, complementing the queue-level counters
  // above.
  [[nodiscard]] Subscription subscription() const { return sub_; }

  // Idempotent: unsubscribe and wake every waiter. Queued frames stay readable.
  void close() {
    if (!state_) return;
    sub_.unsubscribe();
    state_->closed = true;
    state_->not_empty.set();
  }

private:
  std::shared_ptr<detail::MailboxState<T>> state_;
  Subscription sub_;

  Mailbox(std::shared_ptr<detail::MailboxState<T>> state, Subscription sub)
      : state_(std::move(state)), sub_(std::move(sub)) {}

  std::shared_ptr<detail::MailboxState<T>> require_state() const {
    if (!state_) throw std::logic_error("shuttle: empty Mailbox handle");
    return state_;
  }

  friend struct detail::MailboxAccess;
};

namespace detail {

// Named construction channel (same shape as TriggerAccess / detail_pub_access):
// the public factories below are the only way in.
struct MailboxAccess {
  template <typename T>
  static Mailbox<T> make(std::shared_ptr<MailboxState<T>> state, Subscription sub) {
    return Mailbox<T>(std::move(state), std::move(sub));
  }
};

}  // namespace detail

// Typed mailbox: one copy of T per queued frame, so T must be copy
// constructible (use mailbox_bytes for byte-only payloads).
template <typename T>
Mailbox<T> mailbox(std::string pattern, SubOptions opts = {}) {
  static_assert(std::is_copy_constructible_v<T>,
                "shuttle: mailbox<T> requires a copy-constructible T; use mailbox_bytes for "
                "non-copyable payloads");
  detail::check_mailbox_options(opts);
  auto state = std::make_shared<detail::MailboxState<T>>(opts);
  auto sub = detail::sub_typed(
      std::move(pattern), detail::type_ops<T>(),
      [state](const void* payload, const Meta& meta) {
        detail::mailbox_push(*state, T{*static_cast<const T*>(payload)}, meta);
      },
      Capability::nonblocking,
      // The queue belongs to the mailbox, not to the broker, so the broker's
      // subscription options stay at their defaults.
      SubOptions{});
  return detail::MailboxAccess::make<T>(std::move(state), std::move(sub));
}

// Byte mailbox: the raw wire bytes, no codec (unregistered types, or frames
// published from SV whose type has no C++ counterpart).
inline Mailbox<std::vector<std::byte>> mailbox_bytes(std::string pattern, SubOptions opts = {}) {
  detail::check_mailbox_options(opts);
  auto state = std::make_shared<detail::MailboxState<std::vector<std::byte>>>(opts);
  auto sub = detail::sub_raw(
      std::move(pattern),
      [state](const std::vector<std::byte>& bytes, const Meta& meta) {
        detail::mailbox_push(*state, bytes, meta);
      },
      Capability::nonblocking, SubOptions{});
  return detail::MailboxAccess::make<std::vector<std::byte>>(std::move(state), std::move(sub));
}

}  // namespace shuttle
