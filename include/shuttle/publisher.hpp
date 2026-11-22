#pragma once

// Publisher<T>: typed publish entry point for a topic, offering three
// publish flavors:
//
//   auto h = pub.publish_now(msg);         // `publish_now`: synchronous callbacks on the
//                                          // publisher's stack; never suspends
//   auto h = pub.publish_serial(msg);      // `publish_serial`: background single-chain,
//                                          // in-order callbacks; returns immediately
//   auto h = pub.publish_concurrent(msg);  // `publish_concurrent`: background, one task per
//                                          // subscriber, concurrent callbacks; returns immediately
//   auto h = pub.publish_concurrent(msg, after(ev));  // deferred trigger: starts only once the
//                                                     // condition holds h.wait(); wait until all
//                                                     // subscriber callbacks for this frame finish
//
// Naming rule: method names express **scheduling** only (now / serial /
// concurrent); the subscriber declaration decides the **entry** (blocking /
// nonblocking) — see Capability in types.hpp.
//
// One-to-many: a topic may have multiple Publisher instances publishing
// concurrently, and arbitrarily many subscribers (many-to-many); each
// Publisher instance publishes exactly one topic. Multiple Publishers may be
// bound to different types (one instance per type) — the type_tag is decided
// by the C++ codec (type name), not bound to `this`.
//
// Snapshot semantics: all three flavors copy the message and lock in the
// subscriber set at the moment of the call; later subscribe/unsubscribe calls
// do not affect this publication.
//
// Ordering guarantees:
//   · `publish_now` is strictly FIFO (executed in order on the publisher's stack);
//   · every `publish_serial` publication is queued on the per-topic serial
//     chain: repeated publications to the same topic (regardless of which
//     Publisher instance) are invoked in publish-call order;
//   · the deferred part of `publish_now` enters the same chain, so
//     "background delivery order per topic == publish call order";
//   · every `publish_concurrent` publication is queued on one chain per
//     (topic, subscriber): each subscriber therefore receives consecutive
//     frames in publish-call order, while different subscribers still run in
//     parallel (a slow subscriber never delays another one). Ordering across
//     flavors is not promised: a `concurrent` frame and a `serial` frame of the
//     same topic may overtake each other.
//
// What a chain does NOT solve: a callback that publishes to the same topic and
// `wait()`s for it. Under `serial` that deadlocks by construction, and under
// `concurrent` the inner frame queues behind the very frame that waits for it
// (per subscriber), so the same rule applies: publish without waiting, or use a
// different topic.
//
// Event source: Meta.topic_seq increases monotonically per topic,
// assigned by the broker (continuous across Publisher instances), so
// subscribers can distinguish frames from multiple publishers; publisher
// identity is not enforced (carry it inside the message if needed).

#include <memory>
#include <shuttle/detail/abi.hpp>
#include <shuttle/publication.hpp>
#include <shuttle/trigger.hpp>
#include <shuttle/types.hpp>
#include <string>
#include <utility>

namespace shuttle {

template <typename T>
class Publisher {
public:
  // Binds the topic to type T at construction (throws TypeMismatchError if a
  // conflicting binding already exists).
  explicit Publisher(std::string topic) : topic_(std::move(topic)) {
    detail::topic_declare(topic_, detail::type_ops<T>());
  }

  // publish_now: synchronously invokes subscribers with a nonblocking entry on
  // the publisher's stack; returns the frame handle. Subscribers with a
  // blocking entry are not invoked on the stack and are handled per policy:
  // defer (default) hands them to the topic's background serial chain for a
  // later invocation (covered by the handle's done()); skip drops them
  // (counted in skipped() and the skipped component of dropped); reject
  // throws CapabilityError and the frame is never published — type binding,
  // seq, and the retained snapshot are all left untouched. The two target
  // groups are partitioned by "the one entry that registration used" and do
  // not interfere with each other. If a callback throws during the
  // synchronous section: that target counts as failed, the remaining targets
  // are still attempted, the deferred section still enters the chain, and
  // only then is the first exception rethrown to the caller (no handle is
  // returned).
  // The message is taken by value on purpose: a prvalue argument (the common
  // `publish_serial(Msg{...})` form) is guaranteed-elided into the parameter by
  // C++17, and only the parameter is moved into the frame. Taking `const T&`
  // forced a full payload copy on every publish.
  Publication publish_now(T msg, NowPolicy policy = NowPolicy::defer) const {
    return detail::pub_now(topic_, std::make_shared<const T>(std::move(msg)), detail::type_ops<T>(),
                           policy);
  }

  // publish_serial: background single-chain, in-order callbacks (topic-level
  // FIFO). If trig is non-empty, starts only once the trigger condition holds.
  Publication publish_serial(T msg, Trigger trig = {}) const {
    return detail::pub_serial(topic_, std::make_shared<const T>(std::move(msg)),
                              detail::type_ops<T>(), std::move(trig));
  }

  // publish_concurrent: background, one task per subscriber, callbacks run in
  // parallel across subscribers but FIFO per (topic, subscriber). If trig is
  // non-empty, starts only once the trigger condition holds.
  Publication publish_concurrent(T msg, Trigger trig = {}) const {
    return detail::pub_concurrent(topic_, std::make_shared<const T>(std::move(msg)),
                                  detail::type_ops<T>(), std::move(trig));
  }

  // retained (latched): once enabled, the broker caches the latest frame and
  // replays it to new subscribers (the SV blocking entry is replayed
  // asynchronously via a background simulation process; all others complete
  // within the registration call)
  void set_retained(bool r) const { detail::topic_set_retained(topic_, r); }
  [[nodiscard]] bool retained() const { return detail::topic_retained(topic_); }

  [[nodiscard]] const std::string& topic() const noexcept { return topic_; }

private:
  std::string topic_;
};

}  // namespace shuttle
