// Subscription 句柄与 detail::sub_typed/sub_raw。
// 句柄 Impl 只在本 TU 定义；最后一次析构与显式 unsubscribe 共用 Broker::remove_sub。

#include <cstdint>
#include <memory>
#include <shuttle/detail/abi.hpp>
#include <shuttle/subscription.hpp>
#include <string>
#include <utility>

#include "broker/broker.hpp"

namespace shuttle {

using detail::Broker;
using detail::SubKind;
using detail::Subscriber;

struct Subscription::Impl {
  ~Impl() {
    if (active) Broker::get().remove_sub(sub);
  }

  std::shared_ptr<Subscriber> sub;
  bool active = true;
};

namespace detail {

Subscription sub_typed(std::string pattern, const TypeOps* ops, ErasedCallback cb, Capability cap,
                       SubOptions opts) {
  (void)opts;
  Broker& b = Broker::get();
  // 精确 topic 的 typed 订阅同时声明类型；通配符绑定推迟到字节帧发布时。
  if (ops && !Broker::has_wildcard(pattern)) topic_declare(pattern, ops);
  const auto sub = b.register_sub(std::move(pattern), SubKind::typed, cap, ops ? ops->tag : "", ops,
                                  std::move(cb), {}, 0);
  auto impl = std::make_shared<Subscription::Impl>();
  impl->sub = sub;
  return Subscription(std::move(impl));
}

Subscription sub_raw(std::string pattern, ByteCallback cb, Capability cap, SubOptions opts) {
  (void)opts;
  Broker& b = Broker::get();
  const auto sub =
      b.register_sub(std::move(pattern), SubKind::bytes, cap, "", nullptr, {}, std::move(cb), 0);
  auto impl = std::make_shared<Subscription::Impl>();
  impl->sub = sub;
  return Subscription(std::move(impl));
}

}  // namespace detail

Subscription::~Subscription() = default;

void Subscription::unsubscribe() {
  if (impl_ && impl_->active) {
    impl_->active = false;
    Broker::get().remove_sub(impl_->sub);
  }
}

const std::string& Subscription::topic() const {
  return impl_->sub->pattern;
}
Capability Subscription::capability() const {
  return impl_->sub->cap;
}
std::uint64_t Subscription::received() const {
  return impl_->sub->received;
}
std::uint64_t Subscription::dropped() const {
  return impl_->sub->dropped();
}

}  // namespace shuttle
