#pragma once

// 消息元数据：随每次投递传递给订阅者回调。
// Meta 由 broker 在发布时统一生成，C++ 与 SV 两侧内容一致（经 DPI 传递）。

#include <cstdint>
#include <string>

namespace shuttle {

struct Meta {
  std::string topic;     // 实际发布的完整 topic（通配符订阅时仍为具体 topic 名）
  std::string type_tag;  // 消息类型标签（SHUTTLE_REFLECT 的名字 / typeid 名）
  std::uint64_t seq;     // topic 内单调递增序号，由 broker 分配，跨语言一致
  std::int64_t time_fs;  // 发布时的仿真时间（飞秒），取自 now_fs()
};

// 当前仿真时间（飞秒）。
// svdpi 后端：经 DPI 由 SV 侧 shuttle_sv_time_fs() 提供（基于 $realtime 换算）；
// systemc 后端：由 sc_time_stamp() 换算。
std::int64_t now_fs();

}  // namespace shuttle
