// Read-only introspection interface of shuttle::broker; never enters the
// publish/delivery path.

#include <optional>
#include <shuttle/broker.hpp>
#include <string>
#include <string_view>
#include <vector>

#include "broker/broker.hpp"

namespace shuttle::broker {

std::vector<std::string> topics() {
  std::vector<std::string> out;
  for (const auto& [name, topic] : detail::Broker::get().topics)
    if (topic.published > 0) out.push_back(name);
  return out;
}

std::optional<TopicInfo> topic_info(std::string_view name) {
  detail::Broker& b = detail::Broker::get();
  const auto it = b.topics.find(name);
  if (it == b.topics.end()) return std::nullopt;
  const detail::Topic& t = it->second;
  TopicInfo info;
  info.name = std::string(name);
  info.type_tag = t.binding ? t.binding->tag : "";
  info.subscriber_count = b.name_matched_count(name);
  info.retained = t.retained_on;
  info.published = t.published;
  info.delivered = t.delivered;
  info.dropped = t.dropped();
  info.retired = t.retired;
  info.skipped = t.skipped;
  info.failed = t.failed;
  info.deferred = t.deferred;
  return info;
}

}  // namespace shuttle::broker
