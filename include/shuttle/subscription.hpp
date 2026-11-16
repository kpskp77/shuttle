#pragma once

// Subscription：订阅句柄（共享语义，拷贝指向同一订阅，最后一份析构时自动退订）。
//
// subscribe<T> 的两个重载各声明一条互斥的入口（一次注册只有一条）：
//   subscribe<T>(topic, cb)      —— blocking 入口（SV 侧 export task，可消耗仿真时间）：
//                                  `serial`/`concurrent` 的投递单元与 `publish_now` 的 defer 段调它
//   subscribe<T>(topic, nb(cb))  —— nonblocking 入口（SV 侧 export function，零延时）：
//                                  `publish_now` 在发布者栈内同步调它，`serial`/`concurrent` 也用它
// 同一对象要两条入口就注册两次（两个独立订阅者、两个句柄各自退订）。
//
// topic 支持通配符："uart0.*" 匹配单级；"uart0.>" 匹配其后至少一级（不匹配 "uart0" 本身）。
// 同一 topic 允许多个订阅者（多对多）；多个订阅者各自独立接收每个发布帧。

#include <cstdint>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>

#include <shuttle/detail/abi.hpp>
#include <shuttle/meta.hpp>
#include <shuttle/types.hpp>

namespace shuttle {

class Subscription {
public:
  Subscription() = default;
  // 共享语义：拷贝/移动不复制底层订阅；最后一份句柄析构时自动 unsubscribe()
  ~Subscription();

  [[nodiscard]] bool valid() const { return static_cast<bool>(impl_); }  // 其余成员函数都要求它

  // 显式退订（幂等）；退订后不再收到新的投递（已按快照开始/已排队的投递仍会完成）
  // 注意：退订不使句柄失效——valid() 仍为 true，received()/dropped() 仍可读，
  // 便于测试与诊断在退订之后复核计数。
  void unsubscribe();

  [[nodiscard]] const std::string& topic() const;       // 订阅的 topic（含通配符原样）
  [[nodiscard]] Capability capability() const;

  // 诊断计数（口径：received + dropped == 它被本帧定为目标的次数）
  [[nodiscard]] std::uint64_t received() const;  // 已投递帧数
  // 没收到本帧内容的帧数，等于下面三种原因之和（拆分见 broker::TopicInfo）
  [[nodiscard]] std::uint64_t dropped() const;

private:
  struct Impl;
  explicit Subscription(std::shared_ptr<Impl> impl) : impl_(std::move(impl)) {}

  std::shared_ptr<Impl> impl_;

  friend Subscription detail::sub_typed(std::string, const detail::TypeOps*,
                                              detail::ErasedCallback, Capability, SubOptions);
  friend Subscription detail::sub_raw(std::string, ByteCallback, Capability, SubOptions);
};

// ── 类型化订阅 ───────────────────────────────────────────────────────────
// 回调签名：void(const T&, const Meta&)
template <typename T, typename F>
Subscription subscribe(std::string topic, F&& cb, SubOptions opts = {}) {
  static_assert(std::is_invocable_v<F&, const T&, const Meta&>,
                "shuttle: 回调签名应为 void(const T&, const shuttle::Meta&)");
  return detail::sub_typed(
      std::move(topic), detail::type_ops<T>(),
      [fn = std::forward<F>(cb)](const void* p, const Meta& m) mutable {
        fn(*static_cast<const T*>(p), m);
      },
      Capability::blocking, std::move(opts));
}

// 不可阻塞能力（`publish_now`）：契约由用户保证回调零阻塞，见 types.hpp nb() 的说明
template <typename T, typename F>
Subscription subscribe(std::string topic, NonBlocking<F> cb, SubOptions opts = {}) {
  static_assert(std::is_invocable_v<F&, const T&, const Meta&>,
                "shuttle: 回调签名应为 void(const T&, const shuttle::Meta&)");
  return detail::sub_typed(
      std::move(topic), detail::type_ops<T>(),
      [fn = std::move(cb.fn)](const void* p, const Meta& m) mutable {
        fn(*static_cast<const T*>(p), m);
      },
      Capability::nonblocking, std::move(opts));
}

// ── 原始字节订阅 ─────────────────────────────────────────────────────────
// 直接消费线上字节流：用于调试/透传，或订阅来自 SV 且 C++ 侧未注册类型的 topic。
inline Subscription subscribe_bytes(std::string topic, ByteCallback cb,
                                    Capability cap = Capability::blocking, SubOptions opts = {}) {
  return detail::sub_raw(std::move(topic), std::move(cb), cap, std::move(opts));
}

}  // namespace shuttle
