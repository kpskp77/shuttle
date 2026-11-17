#pragma once

// Shared data model of the broker (not installed).
//
// This header only describes frames, subscribers, topics, and publication
// state. It contains no Broker operations and depends on no backend. The
// broker core and the svdpi external facade share these types via
// broker/broker.hpp.

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <shuttle/detail/abi.hpp>
#include <shuttle/exception.hpp>
#include <shuttle/publication.hpp>
#include <string>
#include <vector>
#include <weave/event.hpp>

namespace shuttle::detail {

// ── Message envelope: immutable snapshot of one publication ─────────────
struct Envelope {
  std::shared_ptr<const void> payload;  // typed: T; raw byte stream: std::vector<std::byte>
  const TypeOps* ops;                   // &raw_ops for raw byte streams
  mutable std::shared_ptr<const std::vector<std::byte>> encoded;  // cross-language encoding cache
  Meta meta;
};

// Operation table for raw byte streams (published externally with no C++ type
// bound to the topic): encode is an identity copy, decode is missing (the
// frame cannot be materialized as a C++ type).
//
// inline: `&raw_ops` is the sole discriminator for "is this frame a pure byte
// stream"; every TU must see the same instance.
inline const TypeOps raw_ops{
    "", [](const void* p) { return *static_cast<const std::vector<std::byte>*>(p); }, nullptr};

// Return the envelope's byte-stream representation (used for cross-language
// delivery and byte subscribers), cached per envelope.
inline const std::vector<std::byte>& envelope_bytes(const Envelope& env) {
  if (!env.encoded) {
    if (!env.ops || !env.ops->encode)
      throw PublishError(std::string("shuttle: type of topic ") + env.meta.topic_name +
                         " is not encodable; cannot deliver to SV/byte subscribers");
    env.encoded =
        std::make_shared<const std::vector<std::byte>>(env.ops->encode(env.payload.get()));
  }
  return *env.encoded;
}

// ── Subscriber ───────────────────────────────────────────────────────────
// One registration has exactly one entry: cap is decided by the function
// called at registration (AGENTS.md invariant 4).
enum class SubKind { typed, bytes, external };

struct Subscriber {
  int id = 0;
  std::string pattern;  // subscription pattern (may contain wildcards)
  SubKind kind = SubKind::typed;
  Capability cap = Capability::blocking;
  std::string filter_tag;  // type filter ("" = any); the tag of T for typed subscriptions
  // kind == typed: the operation table of the subscribed type (used to bind a
  // type to topics that have none yet, based on the subscriber's type)
  const TypeOps* ops = nullptr;
  // kind == typed: type-erased callback; kind == bytes: byte callback
  ErasedCallback cb;
  ByteCallback cb_bytes;
  // kind == external: opaque backend-side handle (svdpi = SV registry id),
  // passed back verbatim at dispatch time
  int handle = 0;
  // The snapshot still holds a shared_ptr, so the object may stay alive after
  // unsubscribing; `removed` decides whether in-flight frames still enter the callback.
  bool removed = false;
  // The three kinds of "unsuccessful delivery" are accounted separately:
  // retiring is the subscriber's decision, skipping is the publisher's policy,
  // failing is a failure.
  std::uint64_t received = 0;
  std::uint64_t retired = 0;
  std::uint64_t skipped = 0;
  std::uint64_t failed = 0;
  [[nodiscard]] std::uint64_t dropped() const { return retired + skipped + failed; }
};

// Successful terminal state of a single delivery; failures are reported via
// exceptions and accounted uniformly by the delivery unit.
enum class Delivery { delivered, retired };

// ── Topic ─────────────────────────────────────────────────────────────────
struct Binding {
  std::string tag;     // type tag (raw binding = SV type_tag)
  const TypeOps* ops;  // points to raw_ops for a raw binding
};

struct Topic {
  // Unbound = byte-stream-only publications; the optional makes "bound but
  // without an operation table" inexpressible.
  std::optional<Binding> binding;
  bool retained_on = false;
  std::shared_ptr<const Envelope> retained_env;
  std::uint64_t seq = 0;
  std::uint64_t published = 0;
  std::uint64_t delivered = 0;
  std::uint64_t retired = 0;
  std::uint64_t skipped = 0;
  std::uint64_t failed = 0;
  std::uint64_t deferred = 0;
  [[nodiscard]] std::uint64_t dropped() const { return retired + skipped + failed; }
  weave::Event
      chain;  // tail of the background serial chain shared by publish_serial and publish_now(defer)
  Topic() { chain.set(); }
};

// ── Publication state ────────────────────────────────────────────────────
struct PubState {
  int id = 0;
  // All completion counters are per target subscriber, not per delivery unit.
  std::size_t target_count = 0;
  std::size_t finished = 0;
  std::size_t skipped = 0;
  std::size_t deferred = 0;
  std::size_t retired = 0;
  bool done_flag = false;
  weave::Event done_ev;
  std::vector<Publication::Failure> failures;
  std::string first_error;  // data source for the SV-side shuttle_c_pub_error
  // Completion-time callbacks (the SV-side shuttle_c_wait_pub uses them to
  // wake suspended SV processes)
  std::vector<std::function<void()>> on_complete;
};

}  // namespace shuttle::detail
