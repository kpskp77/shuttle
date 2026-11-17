// ABI entry points and handle methods of Publication.
//
// Ownership rule: this TU implements the "publish-side" symbols declared in
// include/shuttle/publication.hpp and abi.hpp — pub_now / pub_serial /
// pub_concurrent produce Publications and topic_* are the same batch of
// publish preliminaries, so they live together with Publication's member
// methods. The subscribe-side sub_typed / sub_raw live with Subscription in
// subscription.cpp (they must see Subscription::Impl, which is defined only
// in that TU).
//
// State is defined in broker/model.hpp; the private Impl is in
// detail/publication.hpp.

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
  // A raw binding formed by an earlier SV publication may be upgraded to a
  // typed binding when the tag matches.
  if (t.binding->tag == ops->tag && t.binding->ops == &raw_ops) {
    t.binding->ops = ops;
    return;
  }
  if (t.binding->tag != ops->tag)
    throw TypeMismatchError(std::string("shuttle: topic ") + topic + " is already bound to type " +
                            t.binding->tag + ", conflicting with " + ops->tag);
  // Re-declaration with the same tag is allowed: type_ops<T> instances from
  // different DSOs have different addresses but identical semantics.
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
  if (!valid()) throw std::logic_error("shuttle: empty Publication handle");
  impl_->pub->done_ev.wait();
}

bool Publication::wait(weave::duration timeout) {
  if (!valid()) throw std::logic_error("shuttle: empty Publication handle");
  return detail::wait_pub_timed(impl_->pub, timeout);
}

void Publication::result() {
  if (!valid()) throw std::logic_error("shuttle: empty Publication handle");
  wait();
  if (!impl_->pub->failures.empty()) std::rethrow_exception(impl_->pub->failures.front().error);
}

std::size_t Publication::subscriber_count() const {
  return valid() ? impl_->pub->target_count : 0;
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
