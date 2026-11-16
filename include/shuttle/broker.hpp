#pragma once

// broker 内省接口：调试与统计查询（不影响投递路径，只读快照）。

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace shuttle::broker {

struct TopicInfo {
  std::string name;
  std::string type_tag;         // 类型标签；空 = 尚无类型绑定（仅有通配符订阅）
  std::size_t subscriber_count;  // 当前订阅者数量
  bool retained;                // 是否开启 retained
  std::uint64_t published;      // 累计发布帧数
  std::uint64_t delivered;      // 累计投递次数（所有订阅者合计）
  std::uint64_t dropped;        // 累计丢弃次数（按订阅者计）：类型过滤不匹配、
                                // NowPolicy::drop、回调抛异常 / 投递被取消
  std::uint64_t deferred;       // 累计被 defer 到后台串行链的投递数
};

// 已发布过至少一帧的 topic（只有订阅、从未发布过的 topic 不在此列）
std::vector<std::string> topics();

// 单个 topic 的信息；不存在返回 std::nullopt
std::optional<TopicInfo> topic_info(const std::string& name);

}  // namespace shuttle::broker
