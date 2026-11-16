#pragma once

// 已安装的 ABI 头 —— 不是公开 API。
// 内容边界：本文件只含声明与轻量模板（TypeOps / type_ops<T>），实现全在 src/broker.cpp。
// 改动这里的任何一项都等于改动 ABI，需要同时重新编译 libshuttle 与使用者。

#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include <shuttle/codec.hpp>
#include <shuttle/meta.hpp>
#include <shuttle/types.hpp>

namespace shuttle {

class Publication;
class Subscription;
class Trigger;

namespace detail {

// ── TypeOps：类型的运行时操作表（type_ops<T>() 生成）─────────────────────
struct TypeOps {
  const char* tag;  // 类型标签：topic 的类型绑定身份（跨语言一致）

  // nullptr 表示该类型不可跨语言（未 SHUTTLE_REFLECT）：投递给 SV 订阅者或字节订阅者
  // （subscribe_bytes）时抛 PublishError——`publish_now` 直接抛给调用者，
  // `serial`/`concurrent` 记为该订阅者的 Failure（`Publication::result()` 时重抛）。
  std::vector<std::byte> (*encode)(const void* obj);
  // 从字节流物化对象（SV → C++ 方向）；nullptr 同上。
  std::shared_ptr<const void> (*decode)(const std::vector<std::byte>&);
};

template <typename T>
const TypeOps* type_ops() {
  static const std::string tag = type_tag_of<T>();
  static const TypeOps ops{
      tag.c_str(),
      []() -> std::vector<std::byte> (*)(const void*) {
        if constexpr (has_codec_v<T>) {
          return [](const void* p) { return encode(*static_cast<const T*>(p)); };
        }
        return nullptr;
      }(),
      []() -> std::shared_ptr<const void> (*)(const std::vector<std::byte>&) {
        if constexpr (has_codec_v<T>) {
          return [](const std::vector<std::byte>& b) -> std::shared_ptr<const void> {
            return std::make_shared<const T>(decode<T>(b));
          };
        }
        return nullptr;
      }(),
  };
  return &ops;
}

// 类型擦除后的订阅者回调：p 指向物化的 T（保证非空）
using ErasedCallback = std::function<void(const void* p, const Meta&)>;

// ── broker 交互接口（src/broker.cpp 中定义）──────────────────────────────

// 命名约定（本 namespace 全部如此）：用缩写前缀把内部入口与公开 API 分开，
//   pub_*    发布（pub_now / pub_serial / pub_concurrent，以及 pub_ 开头的完成跟踪）
//   sub_*    订阅（sub_typed / sub_raw / sub_sv）
//   topic_*  topic 级操作（绑定 / retained）
// 公开 API 一律用完整动词（Publication、Publisher、subscribe），两者在调用点即可分辨。

// 声明 topic 的类型绑定；与既有绑定冲突时抛 TypeMismatchError。
void topic_declare(const std::string& topic, const TypeOps* ops);

// pub_now（= 公开 API 的 publish_now）：在发布者栈内同步回调所有 nonblocking 入口的订阅者。
// msg 在调用点已完成拷贝（快照语义）。policy 见 NowPolicy。
// 返回值覆盖本帧的**目标**（nonblocking 部分返回时已结束，deferred 部分在后台串行链里）；
// `skip` 排除的目标不计入 `expected`，只体现在 `skipped()` / `dropped()`；`reject` 直接抛
// CapabilityError，没有返回值。
Publication pub_now(const std::string& topic, std::shared_ptr<const void> msg,
                       const TypeOps* ops, NowPolicy policy);

// pub_serial（= publish_serial）：后台单链顺序回调；
// pub_concurrent（= publish_concurrent）：后台每订阅者一任务并发回调。
// 两者都投递给**全部**匹配订阅者（每个订阅者用自己注册的那一条入口，各调用一次）。
// trig 为空触发器时立即启动；均立即返回完成句柄。
Publication pub_serial(const std::string& topic, std::shared_ptr<const void> msg,
                        const TypeOps* ops, Trigger trig);
Publication pub_concurrent(const std::string& topic, std::shared_ptr<const void> msg,
                           const TypeOps* ops, Trigger trig);

// 订阅：topic_pattern 支持通配符（"*" 匹配单级，">" 匹配其后至少一级）。
Subscription sub_typed(std::string topic_pattern, const TypeOps* ops, ErasedCallback cb,
                       Capability cap, SubOptions opts);
Subscription sub_raw(std::string topic_pattern, ByteCallback cb, Capability cap, SubOptions opts);

// retained（latched）：开启后新订阅者立即收到最新一帧
void topic_set_retained(const std::string& topic, bool on);
bool topic_retained(const std::string& topic);

}  // namespace detail
}  // namespace shuttle
