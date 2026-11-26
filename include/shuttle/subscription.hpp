#pragma once

// Subscription: subscription handle (shared semantics — copies refer to the
// same subscription; the last handle's destructor unsubscribes automatically).
//
// The two overloads of subscribe<T> each declare one mutually exclusive entry
// (one registration = one entry):
//   subscribe<T>(topic, cb)      — blocking entry (SV export task, may consume
//                                  simulation time): invoked by the delivery
//                                  units of `serial`/`concurrent` and by the
//                                  deferred section of `publish_now`
//   subscribe<T>(topic, nb(cb))  — nonblocking entry (SV export function, zero
//                                  delay): invoked synchronously on the
//                                  publisher's stack by `publish_now`, and
//                                  also by `serial`/`concurrent`
// To have both entries on the same object, register twice (two independent
// subscribers, two handles, each unsubscribed separately).
//
// Topic patterns support wildcards: "uart0.*" matches a single level;
// "uart0.>" matches at least one further level (does not match "uart0"
// itself). Each level is either a plain level, a "*" occupying a whole
// level, or a ">" occupying a whole level and only allowed as the last one;
// empty levels, the empty string, "a.>.b", and "a.rx*" all throw
// TopicPatternError (names used for publish/declare/retained must not contain
// wildcards at all). A topic may have multiple subscribers (many-to-many);
// each subscriber independently receives every published frame.

#include <cstdint>
#include <memory>
#include <shuttle/detail/abi.hpp>
#include <shuttle/meta.hpp>
#include <shuttle/types.hpp>
#include <string>
#include <type_traits>
#include <utility>

namespace shuttle {

class Subscription {
public:
  Subscription() = default;
  // Shared semantics: copy/move do not duplicate the underlying subscription;
  // when the last handle is destroyed, unsubscribe() runs automatically
  ~Subscription();

  // all other member functions require this
  [[nodiscard]] bool valid() const { return static_cast<bool>(impl_); }

  // Explicit unsubscribe (idempotent); no new deliveries after unsubscribing
  // (deliveries already started / already queued from a snapshot still run to
  // completion). Note: unsubscribing does NOT invalidate the handle — valid()
  // stays true and received()/dropped() remain readable, so tests and
  // diagnostics can re-check the counters after unsubscribing.
  void unsubscribe();

  // the subscribed topic (wildcards preserved verbatim)
  [[nodiscard]] const std::string& topic() const;
  [[nodiscard]] Capability capability() const;

  // Diagnostic counters (invariant: received + dropped == number of frames
  // whose name∩type matched this subscription)
  [[nodiscard]] std::uint64_t received() const;  // frames successfully delivered
  // Frames not successfully delivered = unsubscribed-after-snapshot +
  // skipped-by-policy + failed, the sum of the three (the breakdown is not on
  // this handle; read the three fields of broker::topic_info()). The criterion
  // is "did not end with a successful callback", not "the callback never
  // ran" — the failed kind did enter the callback (and threw after receiving
  // the content). Note this **includes** skipped frames: frame-level
  // subscriber_count() does not count them, while subscriber-level dropped
  // does — "I failed to receive" must be observable.
  [[nodiscard]] std::uint64_t dropped() const;

private:
  struct Impl;
  explicit Subscription(std::shared_ptr<Impl> impl) : impl_(std::move(impl)) {}

  std::shared_ptr<Impl> impl_;

  friend Subscription detail::sub_typed(std::string, const detail::TypeOps*, detail::ErasedCallback,
                                        Capability, SubOptions);
  friend Subscription detail::sub_raw(std::string, ByteCallback, Capability, SubOptions);
};

// ── Typed subscription ──────────────────────────────────────────────────
// Callback signature: void(const T&, const Meta&)
template <typename T, typename F>
Subscription subscribe(std::string topic, F&& cb, SubOptions opts = {}) {
  static_assert(std::is_invocable_v<F&, const T&, const Meta&>,
                "shuttle: callback signature should be void(const T&, const shuttle::Meta&)");
  return detail::sub_typed(
      std::move(topic), detail::type_ops<T>(),
      [fn = std::forward<F>(cb)](const void* p, const Meta& m) mutable {
        fn(*static_cast<const T*>(p), m);
      },
      Capability::blocking, std::move(opts));
}

// Nonblocking capability (`publish_now`): the user guarantees the callback
// never blocks; see the nb() notes in types.hpp
template <typename T, typename F>
Subscription subscribe(std::string topic, NonBlocking<F> cb, SubOptions opts = {}) {
  static_assert(std::is_invocable_v<F&, const T&, const Meta&>,
                "shuttle: callback signature should be void(const T&, const shuttle::Meta&)");
  return detail::sub_typed(
      std::move(topic), detail::type_ops<T>(),
      [fn = std::move(cb.fn)](const void* p, const Meta& m) mutable {
        fn(*static_cast<const T*>(p), m);
      },
      Capability::nonblocking, std::move(opts));
}

// ── Raw-byte subscription ───────────────────────────────────────────────
// Consume the raw byte stream directly: for debugging / pass-through,
// or for topics published from SV whose type is not registered on the C++ side.
// Same two overloads as subscribe<T>: the entry follows the argument type, so
// nb(cb) is the nonblocking entry and no runtime Capability is ever passed.
template <typename F>
Subscription subscribe_bytes(std::string topic, F&& cb, SubOptions opts = {}) {
  static_assert(std::is_invocable_v<F&, const std::vector<std::byte>&, const Meta&>,
                "shuttle: byte callback signature should be void(const std::vector<std::byte>&, "
                "const shuttle::Meta&)");
  return detail::sub_raw(std::move(topic), ByteCallback(std::forward<F>(cb)), Capability::blocking,
                         std::move(opts));
}

template <typename F>
Subscription subscribe_bytes(std::string topic, NonBlocking<F> cb, SubOptions opts = {}) {
  static_assert(std::is_invocable_v<F&, const std::vector<std::byte>&, const Meta&>,
                "shuttle: byte callback signature should be void(const std::vector<std::byte>&, "
                "const shuttle::Meta&)");
  return detail::sub_raw(std::move(topic), ByteCallback(std::move(cb.fn)), Capability::nonblocking,
                         std::move(opts));
}

}  // namespace shuttle
