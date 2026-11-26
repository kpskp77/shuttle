#pragma once

// Broker declaration and dispatch functions shared across entry points (not
// installed).
//
// model.hpp holds only data; this header declares the operations. Definitions
// are split by responsibility:
//   broker.cpp    routing, validation, binding, delivery, subscription, retained;
//   dispatch.cpp  background delivery units, publish_now, completion accounting, waiting.
// svdpi/facade.cpp may reuse the same entry points, but the systemc backend
// still only knows the broker through backend/backend.hpp.

#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <shuttle/publication.hpp>
#include <shuttle/trigger.hpp>
#include <string>
#include <string_view>
#include <vector>
#include <weave/time.hpp>

#include "broker/model.hpp"

namespace shuttle::detail {

class Broker {
public:
  // transparent comparator: string_view can query directly
  std::map<std::string, Topic, std::less<>> topics;
  std::vector<std::shared_ptr<Subscriber>> subs;
  std::map<int, std::shared_ptr<PubState>> pubs;
  int next_sub_id = 1;
  int next_pub_id = 1;

  static Broker& get();

  // Topic-name / subscription-pattern syntax and wildcard routing.
  [[nodiscard]] static bool has_wildcard(std::string_view pattern);
  static std::size_t seg_end(std::string_view s, std::size_t pos);
  [[nodiscard]] static bool match(std::string_view pattern, std::string_view topic);
  static void check_segments(std::string_view name, const char* what);
  static void check_topic_name(std::string_view topic);
  static void check_pattern(std::string_view pattern);

  // Target-set and type-binding preflight.
  [[nodiscard]] static bool type_matches(const Subscriber& sub, std::string_view tag);
  std::vector<std::shared_ptr<Subscriber>> match_subs(std::string_view topic, std::string_view tag);
  std::size_t name_matched_count(std::string_view topic) const;
  Topic& topic_for(std::string_view name);
  void check_filter_early(const Subscriber& sub);
  void check_bind_compat(std::string_view topic, std::string_view tag);
  void now_preflight(std::string_view topic, std::string_view tag, NowPolicy policy);

  // Single-target delivery with unified failure/completion accounting. The
  // owning topic is passed in: every caller already holds it, so the retired
  // path does not need a second map lookup.
  Delivery deliver(const std::shared_ptr<Subscriber>& sub, const Envelope& env, Topic& topic);
  void record_failure(const std::shared_ptr<PubState>& pub,
                      const std::optional<Publication::Failure>& failure);
  void pub_complete(const std::shared_ptr<PubState>& pub);
  void target_finished(const std::shared_ptr<PubState>& pub);
  void unit_finished(const std::shared_ptr<PubState>& pub,
                     std::optional<Publication::Failure> failure);
  static std::string failure_error_what(const Publication::Failure& failure);
  static std::exception_ptr cancelled_exc();
  static std::exception_ptr unknown_exc();
  static Publication::Failure failure_of(std::string label, std::exception_ptr error);
  static void count_failed(Topic& topic, const std::shared_ptr<Subscriber>& sub);

  // Build the immutable frame: bind the type, assign seq, read the backend
  // time, and update the retained snapshot. `corr_id` is publisher-supplied and
  // stored as-is (0 = uncorrelated).
  std::shared_ptr<const Envelope> make_envelope(std::string_view topic,
                                                std::shared_ptr<const void> payload,
                                                const TypeOps* ops, std::uint32_t corr_id = 0);
  std::shared_ptr<const Envelope> make_envelope_bytes(std::string_view topic, std::string_view tag,
                                                      std::vector<std::byte> bytes,
                                                      std::uint32_t corr_id = 0);

  // Background scheduling. start_serial performs the per-topic chain-tail
  // handoff before returning only when the trigger is already satisfied; a
  // deferred frame joins the chain in its task, after wait_trigger, so an
  // un-fired trigger never holds the tail (see docs/architecture.md §5.2).
  static void wait_trigger(const Trigger& trig);
  std::shared_ptr<PubState> start_serial(std::string_view topic,
                                         std::shared_ptr<const Envelope> env, Trigger trig,
                                         std::vector<std::shared_ptr<Subscriber>> targets);
  std::shared_ptr<PubState> start_concurrent(std::string_view topic,
                                             std::shared_ptr<const Envelope> env, Trigger trig,
                                             std::vector<std::shared_ptr<Subscriber>> targets);
  static std::string describe(const std::shared_ptr<Subscriber>& sub);

  // Subscription registration, retained replay, and unsubscription.
  void retained_catchup(const std::shared_ptr<Subscriber>& sub);
  std::shared_ptr<Subscriber> register_sub(std::string pattern, SubKind kind, Capability cap,
                                           std::string filter_tag, const TypeOps* ops,
                                           ErasedCallback cb, ByteCallback cb_bytes, int handle);
  void remove_sub(const std::shared_ptr<Subscriber>& sub);
};

// publish_now shared by the C++ and SV sides: synchronous nonblocking section + skip/defer policy.
std::shared_ptr<PubState> dispatch_now(Broker& broker, std::shared_ptr<const Envelope> env,
                                       NowPolicy policy);

// Timeout race shared by Publication::wait(timeout) and the svdpi pub_wait.
// Must be called in a weave task / simulation-process context.
bool wait_pub_timed(const std::shared_ptr<PubState>& pub, weave::duration timeout);

// Ends the run through weave::fatal when the frame can never start because the
// task that awaits it is the task that holds its delivery chain (a subscriber
// callback published on its own topic and waited for the result). Every entry
// point that blocks on a frame calls it before suspending.
void check_self_blocked(const std::shared_ptr<PubState>& pub);

}  // namespace shuttle::detail
