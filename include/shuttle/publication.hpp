#pragma once

// Publication: completion handle for one publication (query semantics aligned
// with asyncio Futures / weave Tasks).
//
// Publishing does not block the publisher by default (publish_serial/
// publish_concurrent return immediately); use this handle when you need to
// wait:
//   pub.wait();             // block until all subscriber callbacks finish
//   pub.wait(1_us);         // with timeout
//   pub.result();           // wait + rethrow the first failed subscriber's exception
//                           // (aligned with weave TaskGroup semantics)
//
// ── Target set: name ∩ type ──────────────────────────────────────────────
// A subscriber is a target of this frame only if **both the name matches and
// the type tag matches**. Subscribers with disjoint types are not even
// targets: they are not in subscriber_count(), no delivery unit is created
// for them, and they produce no counters at all. Hence "a wildcard subscriber
// only receives the type it declared" is a property of the target-set
// decision, not a per-frame filter.
//
// ── Accounting semantics: every target subscriber has exactly five
//    possible outcomes, identical across all three flavors ──────────────
//   delivered  callback ran to completion (SV side = the export returned)
//   retired    unsubscribed after the snapshot but before the callback: the
//              target reached a terminal state, the content never entered the
//              callback, and nobody was at fault; counted in retired() and in
//              the retired component of subscriber-/topic-level dropped().
//              Same rule in both languages (unsubscribe takes effect
//              immediately)
//   failed     attempted but unsuccessful: callback threw / frame could not
//              be materialized / delivery was cancelled / trigger condition
//              failed (also counted in the failed component of dropped, and
//              recorded as one Failure)
//   deferred   not yet started when the handle was returned (only the defer
//              section of publish_now, taken over by the background serial
//              chain)
//   skipped    the publisher's policy chose not to deliver at all (only
//              NowPolicy::skip of publish_now): not a target and not in
//              subscriber_count()
// reject produces no frame, so it has no terminal state. The three kinds of
// "unsuccessful delivery" differ in **who decided**: retired = the
// subscriber left, skipped = the publisher's policy, failed = tried but did
// not succeed (the callback ran and threw, or the frame could not be
// materialized, or the delivery was cancelled).
// Invariants (hold for all three flavors):
//   · finished() ≤ subscriber_count(), and done() ≡ finished() == subscriber_count()
//   · subscriber_count() + skipped() == number of target subscribers of this
//     frame (name ∩ type, and not skipped by policy)
//   · finished() == the cumulative delivered + retired + failed; a deferred
//     delivery always lands in one of the first three once it completes
//   · subscriber- and topic-level dropped() is the sum of these three
//     non-delivery terminal states (skipped included):
//     dropped() == retired + skipped + failed
//
// Exception semantics: one subscriber's callback throwing does not affect the
// others; failure aggregation granularity varies by flavor: `concurrent`
// collects every subscriber's failure; `serial` keeps only the first (the
// rest are counted solely in the failed component of dropped); the
// synchronous section of `now` records every failure in failures(), but
// **finishes attempting all targets and hands the defer section to the
// background chain first**, and only then rethrows the first exception to the
// publisher (no handle is returned, but the frame's remaining targets are
// still delivered); the defer section merges into the same handle under the
// `serial` rule.

#include <cstdint>
#include <exception>
#include <memory>
#include <string>
#include <vector>
#include <weave/time.hpp>

namespace shuttle {

namespace detail {
class Broker;
}  // namespace detail

class Publication {
public:
  // Failure record for a single subscriber
  struct Failure {
    std::string subscriber;    // subscriber description: "cpp sub <id>" on the C++ side,
                               // "sv sid <id>" on the SV side; "<trigger>" / "<delivery>"
                               // when the trigger wait failed or the delivery was cancelled
    std::exception_ptr error;  // the exception thrown by the callback
  };

  Publication() = default;

  [[nodiscard]] bool valid() const { return static_cast<bool>(impl_); }

  // Block until all subscriber callbacks complete; requires a weave task
  // context (a blocking primitive).
  void wait();
  // Timed wait: true = completed, false = timed out.
  bool wait(weave::duration timeout);

  [[nodiscard]] bool done() const;  // all subscriber callbacks have finished (failures included)

  // wait() + rethrow the first failure (in completion order) if any
  void result();

  // Number of target subscribers for this publication — the broker's
  // PubState::target_count counter (a snapshot taken at publish time,
  // including the deferred part, excluding the skip-skipped part)
  [[nodiscard]] std::size_t subscriber_count() const;
  // Number of subscribers that reached a terminal state (delivered / retired / failed)
  // The completion invariant always holds: done() ≡ finished() == subscriber_count()
  // (all three flavors complete immediately when there are zero targets)
  [[nodiscard]] std::size_t finished() const;

  // ── Visibility: every "this frame failed to reach a subscriber" must be a
  //    number (same vocabulary across all three flavors) ──
  // Subscribers skipped by policy, to whom this frame will not be delivered:
  // only `skip` of `publish_now` makes this nonzero; `serial`/`concurrent`
  // always return 0 (`reject` throws directly, returns no handle, and the
  // frame was never published)
  [[nodiscard]] std::size_t skipped() const;
  // Subscribers whose delivery was deferred to the background serial chain
  // (publish_now's defer); a flag — it does not return to zero after completion
  [[nodiscard]] std::size_t deferred() const;
  // Subscribers unsubscribed after this frame's snapshot and therefore never
  // entered the callback (counted in finished and in the retired component of
  // dropped). The snapshot guarantees "who was selected", not "will certainly
  // be invoked": unsubscribe takes effect immediately, in both languages.
  // This is a terminal count, not an error detail; it does not enter failures()
  [[nodiscard]] std::size_t retired() const;

  [[nodiscard]] std::vector<Failure> failures() const;

private:
  struct Impl;
  explicit Publication(std::shared_ptr<Impl> impl) : impl_(std::move(impl)) {}

  std::shared_ptr<Impl> impl_;

  // Constructed by the broker dispatch paths (Broker is an implementation
  // detail under src/; only forward-declared here)
  friend struct detail_pub_access;
  friend class detail::Broker;
};

}  // namespace shuttle
