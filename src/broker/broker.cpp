// Broker state operations: routing and syntax, type binding, single-target
// delivery, subscription, and retained.
// Background tasks and the publication completion state machine live in
// dispatch.cpp in the same directory.
//
// weave's single-active-process contract lets these plain data structures go
// lock-free; no check-then-act region may suspend.

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

// ── Names, patterns, and routing ─────────────────────────────────────────

bool Broker::has_wildcard(std::string_view pattern) {
  return pattern.find_first_of("*>") != std::string_view::npos;
}

// End of the segment containing s[pos..] (at a '.' or end of string).
// Segment boundaries are defined only here.
std::size_t Broker::seg_end(std::string_view s, std::size_t pos) {
  const std::size_t dot = s.find('.', pos);
  return dot == std::string_view::npos ? s.size() : dot;
}

// "*" matches one level; a trailing ">" matches at least one further level
// (not the prefix itself). Level-by-level comparison, no allocation.
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
                              "\" contains an empty segment (a stray '.')");
    if (pe == name.size()) return;
    pi = pe + 1;
  }
}

// Wildcards belong to subscription patterns only; validating a concrete
// topic name must happen before any state is created.
void Broker::check_topic_name(std::string_view topic) {
  if (topic.empty()) throw TopicPatternError("shuttle: topic name must not be empty");
  if (has_wildcard(topic))
    throw TopicPatternError(
        std::string("shuttle: topic \"") + std::string(topic) +
        "\" contains a wildcard; * and > belong only to subscription "
        "patterns — publish / type declaration / retained require a concrete topic name");
  check_segments(topic, "topic name");
}

void Broker::check_pattern(std::string_view pattern) {
  if (pattern.empty()) throw TopicPatternError("shuttle: subscription pattern must not be empty");
  check_segments(pattern, "subscription pattern");
  const std::string where =
      std::string("shuttle: subscription pattern \"") + std::string(pattern) + "\"";
  for (std::size_t pi = 0;;) {
    const std::size_t pe = seg_end(pattern, pi);
    const std::string_view ps = pattern.substr(pi, pe - pi);
    if (ps == ">") {
      if (pe != pattern.size()) throw TopicPatternError(where + ": > must be the last segment");
      return;
    }
    if (ps != "*" && has_wildcard(ps))
      throw TopicPatternError(
          where + ": segment \"" + std::string(ps) +
          "\" embeds a wildcard inside a plain segment (* and > must each occupy a whole segment)");
    if (pe == pattern.size()) return;
    pi = pe + 1;
  }
}

// ── Target set and binding preflight ─────────────────────────────────────

bool Broker::type_matches(const Subscriber& sub, std::string_view tag) {
  return sub.filter_tag.empty() || sub.filter_tag == tag;
}

std::vector<std::shared_ptr<Subscriber>> Broker::match_subs(std::string_view topic,
                                                            std::string_view tag) {
  std::vector<std::shared_ptr<Subscriber>> out;
  out.reserve(subs.size());
  for (const auto& sub : subs) {
    if (!type_matches(*sub, tag)) continue;
    // Literal patterns skip the segment walk: one string comparison instead.
    const bool hit =
        sub->literal ? std::string_view(sub->pattern) == topic : match(sub->pattern, topic);
    if (hit) out.push_back(sub);
  }
  return out;
}

// Introspection basis for broker::topic_info: name-only counting, no type check.
std::size_t Broker::name_matched_count(std::string_view topic) const {
  std::size_t n = 0;
  for (const auto& sub : subs)
    if (match(sub->pattern, topic)) ++n;
  return n;
}

Topic& Broker::topic_for(std::string_view name) {
  const auto it = topics.find(name);
  if (it != topics.end()) return it->second;
  const auto [pos, inserted] = topics.try_emplace(std::string(name));
  (void)inserted;
  // The map node owns the key, so Meta views can point into it.
  pos->second.name_view = pos->first;
  return pos->second;
}

// ── Response channels ────────────────────────────────────────────────────

void Broker::mark_response_topic(std::string_view name) {
  check_topic_name(name);
  topic_for(name).carries_responses = true;
}

// The only enforcement point of the response-channel contract. Refusing at
// registration keeps the failure loud and early (invariant 4 style) instead of
// turning into a reply that arrives late enough to be mistaken for a dead
// responder. Patterns are matched, not compared: a wildcard blocker
// ("dev.>") would otherwise slip through and swallow the topic later.
void Broker::check_response_topic(std::string_view pattern) const {
  for (const auto& [name, topic] : topics) {
    if (!topic.carries_responses) continue;
    if (!match(pattern, name)) continue;
    throw CapabilityError(std::string("shuttle: subscription ") + std::string(pattern) +
                          " matches the response topic " + name +
                          "; register it with a nonblocking entry (nb() / "
                          "shuttle_subscribe_nb) — a blocking subscriber there holds the "
                          "topic's chain and delays every reply on it");
  }
}

void Broker::check_filter_early(const Subscriber& sub) {
  if (sub.filter_tag.empty()) return;
  const auto it = topics.find(sub.pattern);
  if (it == topics.end() || !it->second.binding) return;
  const std::string& bound_tag = it->second.binding->tag;
  if (bound_tag != sub.filter_tag)
    throw TypeMismatchError(std::string("shuttle: subscription ") + sub.pattern +
                            " declares type " + sub.filter_tag +
                            ", conflicting with the topic binding " + bound_tag);
}

// Reverse preflight for first-time binding; wildcard subscribers span types
// and are intersected per frame by tag only at target-set snapshot time.
void Broker::check_bind_compat(std::string_view topic, std::string_view tag) {
  for (const auto& sub : subs) {
    if (sub->filter_tag.empty() || sub->filter_tag == tag) continue;
    if (has_wildcard(sub->pattern) || !match(sub->pattern, topic)) continue;
    throw TypeMismatchError(std::string("shuttle: topic ") + std::string(topic) + " binds type " +
                            std::string(tag) + ", but an existing subscriber declares " +
                            sub->filter_tag);
  }
}

// reject must be decided before any side effects — type binding, seq, retained;
// syntax validation in turn precedes the policy decision. Counting in place
// keeps the preflight allocation-free (it runs on every reject-policy publish,
// and the frame is usually rejected or accepted immediately).
void Broker::now_preflight(std::string_view topic, std::string_view tag, NowPolicy policy) {
  check_topic_name(topic);
  if (policy != NowPolicy::reject) return;
  std::size_t n = 0;
  for (const auto& sub : subs) {
    if (sub->cap != Capability::blocking || !type_matches(*sub, tag)) continue;
    const bool hit =
        sub->literal ? std::string_view(sub->pattern) == topic : match(sub->pattern, topic);
    if (hit) ++n;
  }
  if (n == 0) return;
  throw CapabilityError(std::string("shuttle: publish_now met ") + std::to_string(n) +
                        " subscriber(s) with a blocking entry (topic " + std::string(topic) +
                        "); they cannot be delivered inside publish_now — use "
                        "NowPolicy::defer/skip, or publish_serial/publish_concurrent instead");
}

// ── Single-target delivery and failure objects ───────────────────────────

Delivery Broker::deliver(const std::shared_ptr<Subscriber>& sub, const Envelope& env,
                         Topic& topic) {
  const bool use_nb = sub->cap == Capability::nonblocking;
  // The snapshot holds a shared_ptr, so the object may stay alive after
  // unsubscribing; `removed` must be checked before the kind branch.
  if (sub->removed) {
    ++sub->retired;
    ++topic.retired;
    return Delivery::retired;
  }
  switch (sub->kind) {
    case SubKind::typed:
      // A pure byte-stream frame must never be reinterpreted as T*.
      if (env.ops == &raw_ops)
        throw PublishError("shuttle: frame of topic " + std::string(env.meta.topic_name) +
                           " was not materialized as a C++ object (type unbound)");
      sub->cb(env.payload.get(), env.meta);
      break;
    case SubKind::bytes:
      sub->cb_bytes(envelope_bytes(env), env.meta);
      break;
    case SubKind::external:
      // An import function's call chain cannot invoke an export task; `now`
      // has already partitioned by cap, so here we only follow the registered entry.
      if (use_nb)
        backend::dispatch_nb(sub->handle, env.meta.topic_name, env.meta.type_tag,
                             env.meta.topic_seq, env.meta.time_fs, env.meta.corr_id,
                             envelope_bytes(env));
      else
        backend::dispatch(sub->handle, env.meta.topic_name, env.meta.type_tag, env.meta.topic_seq,
                          env.meta.time_fs, env.meta.corr_id, envelope_bytes(env));
      // The SV registry has already dropped this sink: this in-flight
      // delivery is retired, not delivered.
      if (backend::take_retired_delivery()) {
        ++sub->retired;
        ++topic.retired;
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
    return "unknown exception";
  }
  return "<no exception information>";
}

std::exception_ptr Broker::cancelled_exc() {
  return std::make_exception_ptr(std::runtime_error("delivery cancelled"));
}

std::exception_ptr Broker::unknown_exc() {
  return std::make_exception_ptr(std::runtime_error("unknown exception"));
}

Publication::Failure Broker::failure_of(std::string label, std::exception_ptr error) {
  return Publication::Failure{std::move(label), std::move(error)};
}

void Broker::count_failed(Topic& topic, const std::shared_ptr<Subscriber>& sub) {
  ++topic.failed;
  ++sub->failed;
}

// ── Frame construction and type binding ──────────────────────────────────

namespace {

// Complete an already-bound frame: environment, seq, backend time, retained
// snapshot. Shared by the typed and the raw-byte entry points so the frame
// layout and counter updates exist in exactly one place.
std::shared_ptr<const Envelope> finish_envelope(Topic& t, std::shared_ptr<const void> payload,
                                                const TypeOps* ops, std::uint32_t corr_id) {
  auto env = std::make_shared<Envelope>();
  env->payload = std::move(payload);
  env->ops = ops;
  // Views into storage the broker owns: building a frame header allocates
  // nothing, and copying a Meta copies two (pointer, size) pairs. Both sources
  // outlive every Meta copy (topics is append-only; the binding's tag is
  // engaged once and never reassigned).
  env->meta.topic_name = t.name_view;
  env->meta.type_tag = t.binding->tag;
  env->meta.topic_seq = ++t.seq;
  env->meta.time_fs = backend::now_fs();
  env->meta.corr_id = corr_id;
  ++t.published;
  if (t.retained_on) t.retained_env = env;
  return env;
}

}  // namespace

std::shared_ptr<const Envelope> Broker::make_envelope(std::string_view topic,
                                                      std::shared_ptr<const void> payload,
                                                      const TypeOps* ops, std::uint32_t corr_id) {
  check_topic_name(topic);
  Topic& t = topic_for(topic);
  if (!t.binding) {
    check_bind_compat(topic, ops->tag);
    t.binding = Binding{ops->tag, ops};
  } else if (t.binding->tag != ops->tag) {
    throw TypeMismatchError(std::string("shuttle: topic ") + std::string(topic) +
                            " is already bound to type " + t.binding->tag +
                            " but this publication declares " + ops->tag);
  }
  return finish_envelope(t, std::move(payload), ops, corr_id);
}

std::shared_ptr<const Envelope> Broker::make_envelope_bytes(std::string_view topic,
                                                            std::string_view tag,
                                                            std::vector<std::byte> bytes,
                                                            std::uint32_t corr_id) {
  check_topic_name(topic);
  Topic& t = topic_for(topic);
  if (!t.binding) {
    // If a matching C++ typed subscriber already exists, use its type to
    // materialize; otherwise keep the frame a pure byte stream.
    check_bind_compat(topic, tag);
    t.binding = Binding{std::string(tag), &raw_ops};
    for (const auto& sub : subs) {
      if (sub->kind != SubKind::typed || !sub->ops || !sub->ops->decode) continue;
      if (!type_matches(*sub, tag)) continue;
      const bool hit =
          sub->literal ? std::string_view(sub->pattern) == topic : match(sub->pattern, topic);
      if (!hit) continue;
      t.binding->ops = sub->ops;
      break;
    }
  } else if (t.binding->tag != tag) {
    throw TypeMismatchError(std::string("shuttle: topic ") + std::string(topic) +
                            " is already bound to type " + t.binding->tag +
                            " but this publication declares " + std::string(tag));
  }
  if (t.binding->ops == &raw_ops) {
    // The caller hands the frame over by value: move its buffer into the
    // immutable payload instead of copying it a second time.
    return finish_envelope(t, std::make_shared<const std::vector<std::byte>>(std::move(bytes)),
                           &raw_ops, corr_id);
  }
  if (!t.binding->ops->decode)
    throw TypeMismatchError(std::string("shuttle: type bound to topic ") + std::string(topic) +
                            " does not support materialization from a byte stream");
  return finish_envelope(t, t.binding->ops->decode(bytes), t.binding->ops, corr_id);
}

// ── Retained and subscription lifecycle ──────────────────────────────────

void Broker::retained_catchup(const std::shared_ptr<Subscriber>& sub) {
  for (auto& [name, t] : topics) {
    if (!t.retained_on || !t.retained_env || !match(sub->pattern, name)) continue;
    if (!type_matches(*sub, t.retained_env->meta.type_tag)) continue;
    if (sub->kind == SubKind::external && sub->cap == Capability::blocking) {
      // The SV blocking entry is invoked via an export task and must not be
      // replayed from within a possibly-function context.
      auto env_copy = t.retained_env;
      Topic* topic_state = &t;  // map nodes are stable: `topics` is append-only
      weave::create_task("shuttle-retained", [this, sub, env_copy, topic_state]() {
        if (deliver(sub, *env_copy, *topic_state) == Delivery::delivered) ++topic_state->delivered;
      });
    } else {
      if (deliver(sub, *t.retained_env, t) == Delivery::delivered) ++t.delivered;
    }
  }
}

std::shared_ptr<Subscriber> Broker::register_sub(std::string pattern, SubKind kind, Capability cap,
                                                 std::string filter_tag, const TypeOps* ops,
                                                 ErasedCallback cb, ByteCallback cb_bytes,
                                                 int handle) {
  check_pattern(pattern);
  // A response topic may only be watched by nonblocking entries (no-op for
  // topics that are not response channels).
  if (cap == Capability::blocking) check_response_topic(pattern);
  auto sub = std::make_shared<Subscriber>();
  sub->id = next_sub_id++;
  sub->pattern = std::move(pattern);
  sub->literal = !has_wildcard(sub->pattern);
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
  // Flag first, then unlink: deliveries already snapshotted see `removed`
  // and are recorded as retired instead of entering the callback.
  sub->removed = true;
  subs.erase(std::remove(subs.begin(), subs.end(), sub), subs.end());
  // Drop this subscriber's concurrent chain tails. Subscriber ids are never
  // reused, so this is not about correctness — it keeps the tail events of a
  // long regression with churning subscribers from accumulating in `topics`
  // (and matches `removed` above: a dead registration owns no ordering state).
  for (auto& [name, topic] : topics) topic.sub_chains.erase(sub->id);
}

}  // namespace shuttle::detail
