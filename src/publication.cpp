// Publication 的 ABI 入口与句柄方法。
//
// 归属规则：本 TU 是 include/shuttle/publication.hpp 与 abi.hpp 中“发布侧”符号的实现——
// pub_now / pub_serial / pub_concurrent 产出 Publication，topic_* 是同一批发布前置操作，
// 因此与 Publication 的成员方法放在一起。订阅侧的 sub_typed / sub_raw 则与 Subscription
// 同处 subscription.cpp（它们必须看到只在那个 TU 定义的 Subscription::Impl）。
//
// 状态定义在 broker/model.hpp，私有 Impl 在 detail/publication.hpp。

#include "detail/publication.hpp"

#include <cstddef>
#include <cstdint>
#include <exception>
#include <memory>
#include <shuttle/detail/abi.hpp>
#include <shuttle/exception.hpp>
#include <shuttle/publication.hpp>
#include <shuttle/trigger.hpp>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>
#include <weave/time.hpp>

#include "broker/broker.hpp"

namespace shuttle::detail {

void topic_declare(const std::string& topic, const TypeOps* ops) {
  Broker::check_topic_name(topic);
  Broker& b = Broker::get();
  Topic& t = b.topic_for(topic);
  if (!t.binding) {
    t.binding = Binding{ops->tag, ops};
    return;
  }
  // SV 先发布形成的 raw 绑定，可在标签一致时升级为 typed 绑定。
  if (t.binding->tag == ops->tag && t.binding->ops == &raw_ops) {
    t.binding->ops = ops;
    return;
  }
  if (t.binding->tag != ops->tag)
    throw TypeMismatchError(std::string("shuttle: topic ") + topic + " 已绑定类型 " +
                            t.binding->tag + "，与 " + ops->tag + " 冲突");
  // 同标签重复声明放行：跨 DSO 的 type_ops<T> 实例地址不同但语义一致。
}

Publication pub_now(const std::string& topic, std::shared_ptr<const void> msg, const TypeOps* ops,
                    NowPolicy policy) {
  Broker& b = Broker::get();
  b.now_preflight(topic, ops->tag, policy);
  auto env = b.make_envelope(topic, std::move(msg), ops);
  return detail_pub_access::make(dispatch_now(b, std::move(env), policy));
}

Publication pub_serial(const std::string& topic, std::shared_ptr<const void> msg,
                       const TypeOps* ops, Trigger trig) {
  Broker& b = Broker::get();
  auto env = b.make_envelope(topic, std::move(msg), ops);
  auto targets = b.match_subs(topic, env->meta.type_tag);
  return detail_pub_access::make(
      b.start_serial(topic, std::move(env), std::move(trig), std::move(targets)));
}

Publication pub_concurrent(const std::string& topic, std::shared_ptr<const void> msg,
                           const TypeOps* ops, Trigger trig) {
  Broker& b = Broker::get();
  auto env = b.make_envelope(topic, std::move(msg), ops);
  auto targets = b.match_subs(topic, env->meta.type_tag);
  return detail_pub_access::make(
      b.start_concurrent(topic, std::move(env), std::move(trig), std::move(targets)));
}

void topic_set_retained(const std::string& topic, bool on) {
  Broker::check_topic_name(topic);
  Topic& t = Broker::get().topic_for(topic);
  t.retained_on = on;
  if (!on) t.retained_env.reset();
}

bool topic_retained(const std::string& topic) {
  const Broker& b = Broker::get();
  const auto it = b.topics.find(topic);
  return it != b.topics.end() && it->second.retained_on;
}

}  // namespace shuttle::detail

namespace shuttle {

bool Publication::done() const {
  return valid() && impl_->pub->done_flag;
}

void Publication::wait() {
  if (!valid()) throw std::logic_error("shuttle: 空 Publication 句柄");
  impl_->pub->done_ev.wait();
}

bool Publication::wait(weave::duration timeout) {
  if (!valid()) throw std::logic_error("shuttle: 空 Publication 句柄");
  return detail::wait_pub_timed(impl_->pub, timeout);
}

void Publication::result() {
  if (!valid()) throw std::logic_error("shuttle: 空 Publication 句柄");
  wait();
  if (!impl_->pub->failures.empty()) std::rethrow_exception(impl_->pub->failures.front().error);
}

std::size_t Publication::subscriber_count() const {
  return valid() ? impl_->pub->expected : 0;
}

std::size_t Publication::finished() const {
  return valid() ? impl_->pub->finished : 0;
}
std::size_t Publication::skipped() const {
  return valid() ? impl_->pub->skipped : 0;
}
std::size_t Publication::deferred() const {
  return valid() ? impl_->pub->deferred : 0;
}
std::size_t Publication::retired() const {
  return valid() ? impl_->pub->retired : 0;
}

std::vector<Publication::Failure> Publication::failures() const {
  if (!valid()) return {};
  return impl_->pub->failures;
}

}  // namespace shuttle
