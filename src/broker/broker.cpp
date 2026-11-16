// Broker 的状态操作：路由与语法、类型绑定、单目标投递、订阅与 retained。
// 后台 task 与发布完成状态机见同目录 dispatch.cpp。
//
// weave 的单活跃进程契约保证这些普通数据结构无需锁；任何检查—修改区间都不能挂起。

#include "broker/broker.hpp"

#include <algorithm>
#include <cstdint>
#include <exception>
#include <memory>
#include <optional>
#include <shuttle/exception.hpp>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <weave/task.hpp>

#include "backend/backend.hpp"

namespace shuttle::detail {

Broker& Broker::get() {
  static Broker broker;
  return broker;
}

// ── 名字、模式与路由 ───────────────────────────────────────────────────────

bool Broker::has_wildcard(std::string_view pattern) {
  return pattern.find_first_of("*>") != std::string_view::npos;
}

// s[pos..] 所在段落的段尾（'.' 或串尾）。段边界只有这一处定义。
std::size_t Broker::seg_end(std::string_view s, std::size_t pos) {
  const std::size_t dot = s.find('.', pos);
  return dot == std::string_view::npos ? s.size() : dot;
}

// "*" 匹配单级，末段 ">" 匹配其后至少一级（不匹配前缀本身）。逐段比较，不做分配。
bool Broker::match(std::string_view pattern, std::string_view topic) {
  for (std::size_t pi = 0, ti = 0;;) {
    const std::size_t pe = seg_end(pattern, pi);
    const std::size_t te = seg_end(topic, ti);
    const std::string_view ps = pattern.substr(pi, pe - pi);
    if (ps == ">") return true;
    if (ps != "*" && ps != topic.substr(ti, te - ti)) return false;
    if (pe == pattern.size()) return te == topic.size();
    if (te == topic.size()) return false;
    pi = pe + 1;
    ti = te + 1;
  }
}

void Broker::check_segments(std::string_view name, const char* what) {
  for (std::size_t pi = 0;;) {
    const std::size_t pe = seg_end(name, pi);
    if (pe == pi)
      throw TopicPatternError(std::string("shuttle: ") + what + " \"" + std::string(name) +
                              "\" 含空段（多余的 '.'）");
    if (pe == name.size()) return;
    pi = pe + 1;
  }
}

// 通配符只属于订阅模式；具体 topic 的校验必须先于任何状态创建。
void Broker::check_topic_name(std::string_view topic) {
  if (topic.empty()) throw TopicPatternError("shuttle: topic 名不能为空");
  if (has_wildcard(topic))
    throw TopicPatternError(std::string("shuttle: topic \"") + std::string(topic) +
                            "\" 含通配符；* 与 > 只属于订阅模式，发布 / 类型声明 / retained "
                            "必须用具体 topic 名");
  check_segments(topic, "topic 名");
}

void Broker::check_pattern(std::string_view pattern) {
  if (pattern.empty()) throw TopicPatternError("shuttle: 订阅模式不能为空");
  check_segments(pattern, "订阅模式");
  const std::string where = std::string("shuttle: 订阅模式 \"") + std::string(pattern) + "\"";
  for (std::size_t pi = 0;;) {
    const std::size_t pe = seg_end(pattern, pi);
    const std::string_view ps = pattern.substr(pi, pe - pi);
    if (ps == ">") {
      if (pe != pattern.size()) throw TopicPatternError(where + " 中 > 必须是末段");
      return;
    }
    if (ps != "*" && has_wildcard(ps))
      throw TopicPatternError(where + " 的段 \"" + std::string(ps) +
                              "\" 把通配符混进了普通段（* 与 > 必须独占一段）");
    if (pe == pattern.size()) return;
    pi = pe + 1;
  }
}

// ── 目标集与绑定预检 ───────────────────────────────────────────────────────

bool Broker::type_matches(const Subscriber& sub, std::string_view tag) {
  return sub.filter_tag.empty() || sub.filter_tag == tag;
}

std::vector<std::shared_ptr<Subscriber>> Broker::match_subs(std::string_view topic,
                                                            std::string_view tag) {
  std::vector<std::shared_ptr<Subscriber>> out;
  for (const auto& sub : subs)
    if (type_matches(*sub, tag) && match(sub->pattern, topic)) out.push_back(sub);
  return out;
}

// broker::topic_info 的内省口径：只按名字计数，不判类型。
std::size_t Broker::name_matched_count(std::string_view topic) const {
  std::size_t n = 0;
  for (const auto& sub : subs)
    if (match(sub->pattern, topic)) ++n;
  return n;
}

Topic& Broker::topic_for(std::string_view name) {
  const auto it = topics.find(name);
  return it != topics.end() ? it->second : topics.try_emplace(std::string(name)).first->second;
}

void Broker::check_filter_early(const Subscriber& sub) {
  if (sub.filter_tag.empty()) return;
  const auto it = topics.find(sub.pattern);
  if (it == topics.end() || !it->second.binding) return;
  const std::string& bound_tag = it->second.binding->tag;
  if (bound_tag != sub.filter_tag)
    throw TypeMismatchError(std::string("shuttle: 订阅 ") + sub.pattern + " 声明类型 " +
                            sub.filter_tag + "，与 topic 绑定 " + bound_tag + " 冲突");
}

// 首次绑定的反向预检；通配符订阅横跨类型，只在目标集快照时按帧标签相交。
void Broker::check_bind_compat(std::string_view topic, std::string_view tag) {
  for (const auto& sub : subs) {
    if (sub->filter_tag.empty() || sub->filter_tag == tag) continue;
    if (has_wildcard(sub->pattern) || !match(sub->pattern, topic)) continue;
    throw TypeMismatchError(std::string("shuttle: topic ") + std::string(topic) + " 绑定类型 " +
                            std::string(tag) + "，但已有订阅者声明 " + sub->filter_tag);
  }
}

// reject 必须在类型绑定、seq、retained 等所有副作用之前判定；语法校验又先于策略。
void Broker::now_preflight(std::string_view topic, std::string_view tag, NowPolicy policy) {
  check_topic_name(topic);
  if (policy != NowPolicy::reject) return;
  const auto matched = match_subs(topic, tag);
  const std::size_t n = select(matched, Capability::blocking).size();
  if (n == 0) return;
  throw CapabilityError(std::string("shuttle: publish_now 遇到 ") + std::to_string(n) +
                        " 个入口为 blocking 的订阅者（topic " + std::string(topic) +
                        "）；它们无法在 publish_now 里投递，可用 NowPolicy::defer/skip，"
                        "或改用 publish_serial/publish_concurrent");
}

// ── 单目标投递与失败对象 ───────────────────────────────────────────────────

Delivery Broker::deliver(const std::shared_ptr<Subscriber>& sub, const Envelope& env) {
  const bool use_nb = sub->cap == Capability::nonblocking;
  // 快照持有 shared_ptr，对象在退订后仍可能存活；removed 必须在 kind 分支之前检查。
  if (sub->removed) {
    ++sub->retired;
    ++topic_for(env.meta.topic).retired;
    return Delivery::retired;
  }
  switch (sub->kind) {
    case SubKind::typed:
      // 纯字节帧绝不能按 T* 解释。
      if (env.ops == &raw_ops)
        throw PublishError(std::string("shuttle: topic ") + env.meta.topic +
                           " 的帧没有物化为 C++ 对象（类型未绑定）");
      sub->cb(env.payload.get(), env.meta);
      break;
    case SubKind::bytes:
      sub->cb_bytes(envelope_bytes(env), env.meta);
      break;
    case SubKind::external:
      // import function 的调用链不能调 export task；now 已按 cap 分拨，故这里只看注册入口。
      if (use_nb)
        backend::dispatch_nb(sub->handle, env.meta.topic, env.meta.type_tag, env.meta.seq,
                             env.meta.time_fs, envelope_bytes(env));
      else
        backend::dispatch(sub->handle, env.meta.topic, env.meta.type_tag, env.meta.seq,
                          env.meta.time_fs, envelope_bytes(env));
      // SV 注册表已删除该 sink：这次在途投递是 retired，不能记成 delivered。
      if (backend::take_retired_delivery()) {
        ++sub->retired;
        ++topic_for(env.meta.topic).retired;
        return Delivery::retired;
      }
      break;
  }
  ++sub->received;
  return Delivery::delivered;
}

std::string Broker::failure_error_what(const Publication::Failure& failure) {
  try {
    if (failure.error) std::rethrow_exception(failure.error);
  } catch (const std::exception& e) {
    return e.what();
  } catch (...) {
    return "未知异常";
  }
  return "<无异常信息>";
}

std::exception_ptr Broker::cancelled_exc() {
  return std::make_exception_ptr(std::runtime_error("投递被取消"));
}

std::exception_ptr Broker::unknown_exc() {
  return std::make_exception_ptr(std::runtime_error("未知异常"));
}

Publication::Failure Broker::failure_of(std::string label, std::exception_ptr error) {
  return Publication::Failure{std::move(label), std::move(error)};
}

void Broker::count_failed(Topic& topic, const std::shared_ptr<Subscriber>& sub) {
  ++topic.failed;
  ++sub->failed;
}

// ── 建帧与类型绑定 ─────────────────────────────────────────────────────────

std::shared_ptr<const Envelope> Broker::make_envelope(std::string_view topic,
                                                      std::shared_ptr<const void> payload,
                                                      const TypeOps* ops) {
  check_topic_name(topic);
  Topic& t = topic_for(topic);
  if (!t.binding) {
    check_bind_compat(topic, ops->tag);
    t.binding = Binding{ops->tag, ops};
  } else if (t.binding->tag != ops->tag) {
    throw TypeMismatchError(std::string("shuttle: topic ") + std::string(topic) + " 已绑定类型 " +
                            t.binding->tag + "，本次发布类型 " + ops->tag);
  }
  auto env = std::make_shared<Envelope>();
  env->payload = std::move(payload);
  env->ops = ops;
  env->meta.topic = std::string(topic);
  env->meta.type_tag = t.binding->tag;
  env->meta.seq = ++t.seq;
  env->meta.time_fs = backend::now_fs();
  ++t.published;
  if (t.retained_on) t.retained_env = env;
  return env;
}

std::shared_ptr<const Envelope> Broker::make_envelope_bytes(std::string_view topic,
                                                            std::string_view tag,
                                                            const std::vector<std::byte>& bytes) {
  check_topic_name(topic);
  Topic& t = topic_for(topic);
  if (!t.binding) {
    // 若已有匹配的 C++ typed 订阅者，就用它的类型物化；否则保持纯字节流。
    check_bind_compat(topic, tag);
    t.binding = Binding{std::string(tag), &raw_ops};
    for (const auto& sub : subs) {
      if (sub->kind != SubKind::typed || !sub->ops || !sub->ops->decode) continue;
      if (!type_matches(*sub, tag) || !match(sub->pattern, topic)) continue;
      t.binding->ops = sub->ops;
      break;
    }
  } else if (t.binding->tag != tag) {
    throw TypeMismatchError(std::string("shuttle: topic ") + std::string(topic) + " 已绑定类型 " +
                            t.binding->tag + "，本次发布类型 " + std::string(tag));
  }
  if (t.binding->ops == &raw_ops) {
    auto payload = std::make_shared<const std::vector<std::byte>>(bytes);
    auto env = std::make_shared<Envelope>();
    env->payload = std::move(payload);
    env->ops = &raw_ops;
    env->meta.topic = std::string(topic);
    env->meta.type_tag = t.binding->tag;
    env->meta.seq = ++t.seq;
    env->meta.time_fs = backend::now_fs();
    ++t.published;
    if (t.retained_on) t.retained_env = env;
    return env;
  }
  if (!t.binding->ops->decode)
    throw TypeMismatchError(std::string("shuttle: topic ") + std::string(topic) +
                            " 绑定的类型不支持从字节流物化");
  return make_envelope(topic, t.binding->ops->decode(bytes), t.binding->ops);
}

// ── retained 与订阅生命周期 ────────────────────────────────────────────────

void Broker::retained_catchup(const std::shared_ptr<Subscriber>& sub) {
  for (auto& [name, t] : topics) {
    if (!t.retained_on || !t.retained_env || !match(sub->pattern, name)) continue;
    if (!type_matches(*sub, t.retained_env->meta.type_tag)) continue;
    if (sub->kind == SubKind::external && sub->cap == Capability::blocking) {
      // SV blocking 入口是经 export task 调用，不能在可能的 function 上下文中补发。
      auto env_copy = t.retained_env;
      weave::create_task("shuttle-retained", [this, sub, env_copy]() {
        if (deliver(sub, *env_copy) == Delivery::delivered)
          ++topic_for(env_copy->meta.topic).delivered;
      });
    } else {
      if (deliver(sub, *t.retained_env) == Delivery::delivered) ++t.delivered;
    }
  }
}

std::shared_ptr<Subscriber> Broker::register_sub(std::string pattern, SubKind kind, Capability cap,
                                                 std::string filter_tag, const TypeOps* ops,
                                                 ErasedCallback cb, ByteCallback cb_bytes,
                                                 int handle) {
  check_pattern(pattern);
  auto sub = std::make_shared<Subscriber>();
  sub->id = next_sub_id++;
  sub->pattern = std::move(pattern);
  sub->kind = kind;
  sub->cap = cap;
  sub->filter_tag = std::move(filter_tag);
  sub->ops = ops;
  sub->cb = std::move(cb);
  sub->cb_bytes = std::move(cb_bytes);
  sub->handle = handle;
  check_filter_early(*sub);
  subs.push_back(sub);
  retained_catchup(sub);
  return sub;
}

void Broker::remove_sub(const std::shared_ptr<Subscriber>& sub) {
  // 先置位再摘表：已快照的投递据此记 retired，而不是继续进入回调。
  sub->removed = true;
  subs.erase(std::remove(subs.begin(), subs.end(), sub), subs.end());
}

}  // namespace shuttle::detail
