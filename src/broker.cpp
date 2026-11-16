// broker 核心：topic 表、通配符路由、类型绑定、三种发布方式的投递编排、
// Publication / Subscription 句柄、retained、内省。
// 跨语言事件注册表（SV shuttle_event / SHUTTLE_TRIG_EVENT）与外部接入门面
// 同属"外部通道"，实现在 src/entry.cpp；本文件只剩纯 C++ 类型化路径。
//
// 并发模型：weave 保证任意时刻至多一个仿真进程在执行 C++ 代码
// （阻塞原语 ≡ 交出调度权），因此 broker 状态是普通数据结构，无需锁。
// 句柄的引用计数（shared_ptr）跨进程边界的读写同样受该不变性保护。

#include <algorithm>
#include <cstdio>
#include <exception>
#include <map>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <shuttle/detail/abi.hpp>
#include <shuttle/broker.hpp>
#include <shuttle/exception.hpp>
#include <shuttle/publication.hpp>
#include <shuttle/subscription.hpp>
#include <shuttle/trigger.hpp>
#include <weave/event.hpp>
#include <weave/sync.hpp>
#include <weave/task.hpp>

#include "internal/backend.hpp"
#include "internal/broker_impl.hpp"
#include "internal/broker_internal.hpp"

namespace shuttle {
using detail::Broker;
using detail::PubState;
using detail::Subscriber;
using detail::Topic;

namespace {
std::string g_last_error;

}  // namespace

// 订阅句柄 Impl：析构时自动退订（Publication 的 Impl 在 internal/broker_impl.hpp）。
struct Subscription::Impl {
  ~Impl() {
    if (active) Broker::get().remove_sub(sub);
  }
  std::shared_ptr<Subscriber> sub;
  bool active = true;
};

namespace detail {

void set_last_error(std::string msg) { g_last_error = std::move(msg); }
const std::string& last_error() { return g_last_error; }

// ── detail/abi.hpp 的实现 ─────────────────────────────────────────

void topic_declare(const std::string& topic, const TypeOps* ops) {
  Broker::check_topic_name(topic);
  Broker& b = Broker::get();
  Topic& t = b.topic_for(topic);
  if (!t.binding) {
    t.binding = Binding{ops->tag, ops};
    return;
  }
  // 既有 raw 绑定（SV 先发布）且标签一致 → 升级为 typed 绑定
  if (t.binding->tag == ops->tag && t.binding->ops == &raw_ops) {
    t.binding->ops = ops;
    return;
  }
  if (t.binding->tag != ops->tag)
    throw TypeMismatchError(std::string("shuttle: topic ") + topic + " 已绑定类型 " +
                            t.binding->tag + "，与 " + ops->tag + " 冲突");
  // 同标签重复声明：视为同一类型，放行（跨 DSO 的 type_ops<T> 实例地址不同但语义一致）
}

Publication pub_now(const std::string& topic, std::shared_ptr<const void> msg,
                       const TypeOps* ops, NowPolicy policy) {
  Broker& b = Broker::get();
  b.now_preflight(topic, ops->tag, policy);  // reject 先判：不消耗 seq / 不刷新 retained
  auto env = b.make_envelope(topic, std::move(msg), ops);
  return detail_pub_access::make(dispatch_now(b, std::move(env), policy));
}

Publication pub_serial(const std::string& topic, std::shared_ptr<const void> msg,
                       const TypeOps* ops, Trigger trig) {
  Broker& b = Broker::get();
  auto env = b.make_envelope(topic, std::move(msg), ops);
  // 全部目标订阅者（名字 ∩ 类型）：每个用自己注册的那条入口（blocking 或 nonblocking）各调用一次
  auto targets = b.match_subs(topic, env->meta.type_tag);
  return detail_pub_access::make(b.start_serial(topic, std::move(env), std::move(trig),
                                             std::move(targets)));
}

Publication pub_concurrent(const std::string& topic, std::shared_ptr<const void> msg,
                           const TypeOps* ops, Trigger trig) {
  Broker& b = Broker::get();
  auto env = b.make_envelope(topic, std::move(msg), ops);
  auto targets = b.match_subs(topic, env->meta.type_tag);
  return detail_pub_access::make(b.start_concurrent(topic, std::move(env), std::move(trig),
                                             std::move(targets)));
}

Subscription sub_typed(std::string pattern, const TypeOps* ops, ErasedCallback cb, Capability cap,
                       SubOptions opts) {
  (void)opts;
  Broker& b = Broker::get();
  // 精确 topic 的订阅即声明该 topic 的类型绑定（这样"SV 先发布、C++ 后订阅"
  // 或"C++ 只订阅不发布"的 topic 也能在发布时正确物化）。通配符订阅无法
  // 声明具体 topic 的绑定，交给 make_envelope_bytes 在发布时按订阅者类型绑定。
  if (ops && !Broker::has_wildcard(pattern)) topic_declare(pattern, ops);
  const auto sub = b.register_sub(std::move(pattern), SubKind::typed, cap,
                                  ops ? ops->tag : "", ops, std::move(cb), {}, 0);
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

// 公开入口（声明见 meta.hpp）：转发给当前后端。定义必须放在后端无关的 TU——
// 它是安装头里的承诺，缺定义会让使用者链接失败（undefined symbol）。
}  // namespace detail

std::int64_t now_fs() { return detail::backend::now_fs(); }

namespace detail {

void topic_set_retained(const std::string& topic, bool on) {
  Broker::check_topic_name(topic);
  Topic& t = Broker::get().topic_for(topic);
  t.retained_on = on;
  // 关闭时立即丢掉快照：否则快照会一直占着最新一帧，
  // 且 retained_catchup 需要靠 retained_on 判断，两者保持一致更不易错。
  if (!on) t.retained_env.reset();
}
bool topic_retained(const std::string& topic) {
  auto it = Broker::get().topics.find(topic);
  return it != Broker::get().topics.end() && it->second.retained_on;
}




}  // namespace detail

// ── Publication 句柄方法 ────────────────────────────────────────────────────
//（构造与 valid() 在 publication.hpp 内联定义）
bool Publication::done() const { return valid() && impl_->pub->done_flag; }

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

// ── Subscription 句柄方法 ───────────────────────────────────────────────────
Subscription::~Subscription() = default;

void Subscription::unsubscribe() {
  if (impl_ && impl_->active) {
    impl_->active = false;
    Broker::get().remove_sub(impl_->sub);
  }
}

const std::string& Subscription::topic() const { return impl_->sub->pattern; }
Capability Subscription::capability() const { return impl_->sub->cap; }
std::uint64_t Subscription::received() const { return impl_->sub->received; }
std::uint64_t Subscription::dropped() const { return impl_->sub->dropped(); }

// ── broker 内省 ─────────────────────────────────────────────────────────────
namespace broker {

std::vector<std::string> topics() {
  std::vector<std::string> out;
  for (const auto& [name, t] : Broker::get().topics)
    if (t.published > 0) out.push_back(name);
  return out;
}

std::optional<TopicInfo> topic_info(std::string_view name) {
  Broker& b = Broker::get();
  auto it = b.topics.find(name);
  if (it == b.topics.end()) return std::nullopt;
  const Topic& t = it->second;
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

}  // namespace broker
}  // namespace shuttle
