#pragma once

// Broker introspection interface: debugging and statistics queries (read-only
// snapshots; they do not affect the delivery path).

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace shuttle::broker {

struct TopicInfo {
  std::string name;
  std::string type_tag;  // type tag; empty = no type binding yet (wildcard subscribers only)
  // number of subscribers whose name matches (type not considered: one topic
  // entry may serve multiple types)
  std::size_t subscriber_count;
  bool retained;            // whether retained is enabled
  std::uint64_t published;  // cumulative published frames
  std::uint64_t delivered;  // cumulative deliveries (summed over all subscribers)
  std::uint64_t dropped;    // cumulative unsuccessful deliveries (retired + skipped + failed)
  std::uint64_t retired;    // of which: unsubscribed after the snapshot
  std::uint64_t skipped;    // of which: skipped by the NowPolicy::skip policy
  std::uint64_t failed;     // of which: callback threw / delivery cancelled /
                            // delivery-unit-level failure
  std::uint64_t deferred;   // cumulative deliveries deferred to the background serial chain
};

// Topics that have published at least one frame (topics with subscribers but
// zero publications are not listed)
std::vector<std::string> topics();

// Info for a single topic; returns std::nullopt if it does not exist
std::optional<TopicInfo> topic_info(std::string_view name);

}  // namespace shuttle::broker
