#pragma once

// detail::wait_wake — the one shape of "wait for a level flag, bounded by a deadline".
//
// Three things in the framework wait this way and they must not drift apart:
//   Mailbox::recv(timeout)          (mailbox.hpp)  — the queue's not_empty level
//   Call::recv/wait_for             (requester.hpp) — the slot's wake level
//   Publication::wait(timeout)      (dispatch.cpp)  — the frame's done level
// All three are *level* waits, not one-shot signals: the owner keeps the flag set
// while its condition holds and clears it when it stops (weave's Event, not Pulse).
// The waiter is therefore disposable — a timeout cancels it and consumes nothing,
// so the caller loops and re-checks its own predicate, and no frame is lost.
//
// Why this is installed: mailbox.hpp and requester.hpp are header-only public
// components, so they cannot reach into `src/`. Same rationale as
// detail/abi.hpp — installed for the public headers to share, not a user entry
// point (users call Publisher / subscribe / Mailbox / Requester).
//
// weave::timeout (not a hand-rolled create_task + wait_for) on purpose: it takes
// the waiter down when *this* task is cancelled or the deadline passes, so a
// cancelled recv cannot leave a task behind that wakes up in some later cycle.
// The one thing it does not give us is a per-call task name (it names its child
// "timeout_body"); callers are told apart by their own frame, not by this task.

#include <optional>
#include <weave/event.hpp>
#include <weave/sync.hpp>
#include <weave/time.hpp>

namespace shuttle::detail {

// now() + timeout, as an absolute deadline (the form that survives being carried
// across several waits).
inline weave::time_point deadline_after(weave::duration timeout) {
  return weave::now() + timeout;
}

// Blocks until `ev` is set. `deadline` nullopt = wait forever.
// Returns true if the event was set, false if the deadline passed first (in
// which case nothing was consumed). Must run in a weave task context.
inline bool wait_wake(weave::Event& ev, std::optional<weave::time_point> deadline) {
  if (!deadline.has_value()) {
    ev.wait();
    return true;
  }
  // `time_since_epoch() - now()` rather than `time_point - now()`: the operand
  // order matters to the type system, and the elapsed duration is what we want.
  const weave::duration limit = deadline->time_since_epoch() - weave::now();
  if (limit.count() <= 0) return false;
  try {
    weave::timeout([&ev] { ev.wait(); }, limit);
    return true;
  } catch (const weave::TimeoutError&) {
    return false;
  }
}

}  // namespace shuttle::detail
