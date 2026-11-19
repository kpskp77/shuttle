// Broker publish scheduling: completion state machine, per-topic serial
// chain, per-subscriber concurrent units, and publish_now.
// Frame creation, target matching, and deliver itself live in broker.cpp in
// the same directory; this file does not distinguish C++/SV frame origins.

#include <algorithm>
#include <cstddef>
#include <exception>
#include <iterator>
#include <memory>
#include <optional>
#include <shuttle/detail/wait.hpp>
#include <shuttle/trigger.hpp>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <weave/event.hpp>
#include <weave/sync.hpp>
#include <weave/task.hpp>
#include <weave/time.hpp>

#include "broker/broker.hpp"

namespace shuttle::detail {

// ── Unified completion and failure accounting ────────────────────────────

void Broker::record_failure(const std::shared_ptr<PubState>& pub,
                            const std::optional<Publication::Failure>& failure) {
  if (!failure) return;
  pub->failures.push_back(*failure);
  if (pub->first_error.empty()) {
    try {
      if (failure->error) pub->first_error = failure_error_what(*failure);
    } catch (...) {
      pub->first_error = "<exception>";
    }
  }
}

void Broker::pub_complete(const std::shared_ptr<PubState>& pub) {
  pub->done_flag = true;
  // Completion hooks run first: the SV wait_done relies on them to wake
  // waiting processes; a hook failure does not affect delivery results.
  auto hooks = std::exchange(pub->on_complete, {});
  for (const auto& fn : hooks) {
    try {
      fn();
    } catch (...) {
    }
  }
  pub->done_ev.set();
}

// All delivery flavors advance `finished` only here; completion counts are
// always per target subscriber.
void Broker::target_finished(const std::shared_ptr<PubState>& pub) {
  if (++pub->finished == pub->target_count && !pub->done_flag) pub_complete(pub);
}

void Broker::unit_finished(const std::shared_ptr<PubState>& pub,
                           std::optional<Publication::Failure> failure) {
  record_failure(pub, failure);
  target_finished(pub);
}

void Broker::wait_trigger(const Trigger& trig) {
  if (!trig.immediate()) trig.wait();
}

std::string Broker::describe(const std::shared_ptr<Subscriber>& sub) {
  if (sub->kind == SubKind::external) return "external handle " + std::to_string(sub->handle);
  return "cpp sub " + std::to_string(sub->id);
}

namespace {

// Tail of one ordered delivery chain, identified by (topic, key):
//   key == 0        → the topic-wide serial chain (publish_serial, publish_now(defer));
//   key == sub->id  → that subscriber's own concurrent chain (publish_concurrent).
// Both flavors share this one handoff so the tricky part — "release the tail and
// become the new tail" — exists exactly once. Reading the current tail and
// installing `mine` is a single step: no suspension point may lie between them
// (single-active-process invariant), and it must not happen at publish time for
// a frame whose trigger is not satisfied yet — an un-fired trigger holding the
// tail would stall every later frame of that chain (AGENTS.md invariant 12).
std::shared_ptr<weave::Event> chain_join(Topic& topic, int key,
                                         const std::shared_ptr<weave::Event>& mine) {
  if (key == 0) {
    // The topic-wide tail is a value on Topic; a copy of it is the wait handle
    // (already set when no frame is in flight, so waiting is a no-op).
    auto prev = std::make_shared<weave::Event>(topic.chain);
    topic.chain = *mine;
    return prev;
  }
  // A per-subscriber tail is handed off by pointer: the waiting frame keeps its
  // predecessor alive and one delivery unit costs one event instead of two.
  auto& slot = topic.sub_chains[key];
  std::shared_ptr<weave::Event> prev;
  if (slot) {
    prev = std::move(slot);
  } else {
    prev = std::make_shared<weave::Event>();
    prev->set();  // first frame of this chain: nothing to wait for
  }
  slot = mine;
  return prev;
}

}  // namespace

// ── publish_serial: one delivery unit per frame, per-topic serial chain
//    keeps publication-order FIFO ─────────────────────────────────────────

std::shared_ptr<PubState> Broker::start_serial(std::string_view topic,
                                               std::shared_ptr<const Envelope> env, Trigger trig,
                                               std::vector<std::shared_ptr<Subscriber>> targets) {
  auto pub = std::make_shared<PubState>();
  pub->id = next_pub_id++;
  pub->target_count = targets.size();
  if (targets.empty()) {
    // Zero targets complete immediately: no task, no chain-tail installed.
    pub_complete(pub);
    return pub;
  }

  Topic& frame_topic = topic_for(topic);
  // `mine` — the chain-tail event this frame must release — is always created
  // here and never re-read from the topic later (invariant 5).
  auto mine = std::make_shared<weave::Event>();
  // A frame whose trigger is already satisfied reserves the tail inside the
  // publish call, so its delivery order equals the publish order and does not
  // depend on when weave first runs the task. A deferred frame must NOT reserve
  // here: an un-fired trigger would otherwise hold the topic's chain tail and
  // stall every later frame (and deadlock whenever the trigger itself depends
  // on one of them). It joins the chain at trigger time instead.
  const bool queued = trig.immediate();
  std::shared_ptr<weave::Event> prev;
  if (queued) prev = chain_join(frame_topic, 0, mine);
  Topic* const topic_state = &frame_topic;
  weave::create_task(
      std::string("shuttle-serial-") + std::string(topic),
      [this, pub, env, trig = std::move(trig), targets = std::move(targets), prev, mine,
       topic_state]() mutable {
        std::optional<Publication::Failure> failure;
        Topic& t = *topic_state;
        std::size_t idx = 0;
        try {
          wait_trigger(trig);
          if (!prev) {
            // Deferred frame joining the chain now. chain_join reads the tail and
            // installs ours in one step: no suspension point lies between them
            // (single-active-process invariant), so no other frame can interleave.
            prev = chain_join(t, 0, mine);
          }
          prev->wait();
          for (; idx < targets.size(); ++idx) {
            const auto& sub = targets[idx];
            try {
              if (deliver(sub, *env, t) == Delivery::delivered)
                ++t.delivered;
              else
                ++pub->retired;
            } catch (const weave::CancelledError&) {
              throw;  // handled by the outer scope as a unit-level failure; also guarantees
                      // `mine` is finally set
            } catch (const std::exception&) {
              count_failed(t, sub);
              if (!failure) failure = failure_of(describe(sub), std::current_exception());
            } catch (...) {
              count_failed(t, sub);
              if (!failure) failure = failure_of(describe(sub), unknown_exc());
            }
            target_finished(pub);
          }
        } catch (const weave::CancelledError&) {
          failure = failure_of("<delivery>", cancelled_exc());
        } catch (const std::exception&) {
          failure = failure_of("<trigger>", std::current_exception());
        } catch (...) {
          failure = failure_of("<trigger>", unknown_exc());
        }
        // On trigger failure, cancellation, or unit-level failure, the
        // untried targets must also reach a terminal (failed) state.
        for (; idx < targets.size(); ++idx) {
          ++t.failed;
          ++targets[idx]->failed;
          target_finished(pub);
        }
        mine->set();  // release the next frame regardless of outcome, to prevent chain stall
                      // (harmless when a deferred frame never installed it)
        record_failure(pub, failure);
        if (pub->finished == pub->target_count && !pub->done_flag) pub_complete(pub);
      });
  return pub;
}

// ── publish_concurrent: one delivery unit per subscriber, parallel across
//    subscribers, FIFO per (topic, subscriber) ───────────────────────────────

std::shared_ptr<PubState> Broker::start_concurrent(
    std::string_view topic, std::shared_ptr<const Envelope> env, Trigger trig,
    std::vector<std::shared_ptr<Subscriber>> targets) {
  auto pub = std::make_shared<PubState>();
  pub->id = next_pub_id++;
  pub->target_count = targets.size();
  if (targets.empty()) {
    pub_complete(pub);
    return pub;
  }
  Topic& frame_topic = topic_for(topic);
  Topic* const topic_state = &frame_topic;
  // Chains are per (topic, subscriber): two frames in flight for the same
  // subscriber are serialized, and different subscribers still run in parallel
  // (a slow subscriber never delays another one's callback). Each unit hands
  // off through its own tail using the same rule as publish_serial: a frame
  // whose trigger is already satisfied reserves its tail inside the publish
  // call, so per-subscriber order equals publish order and does not depend on
  // when weave first runs the task; a deferred frame joins the chain only after
  // wait_trigger returns (invariant 12 — reserving early would let an un-fired
  // trigger hold the tail and stall that subscriber's later frames).
  const bool queued = trig.immediate();
  for (const auto& sub : targets) {
    const int key = sub->id;
    const auto mine = std::make_shared<weave::Event>();
    std::shared_ptr<weave::Event> prev;
    if (queued) prev = chain_join(frame_topic, key, mine);
    weave::create_task(std::string("shuttle-concurrent-") + std::string(topic),
                       [this, pub, env, trig, sub, topic_state, mine, prev, queued]() mutable {
                         std::optional<Publication::Failure> failure;
                         Topic& t = *topic_state;
                         try {
                           wait_trigger(trig);
                           if (!prev && !sub->removed) {
                             // Deferred frame joining this subscriber's chain now. A target
                             // that unsubscribed during the trigger wait needs no ordering
                             // (its delivery is recorded as retired anyway), and skipping the
                             // handoff keeps a dead registration from leaving a chain behind.
                             prev = chain_join(t, sub->id, mine);
                           }
                           if (prev) prev->wait();
                           if (deliver(sub, *env, t) == Delivery::delivered)
                             ++t.delivered;
                           else
                             ++pub->retired;
                         } catch (const weave::CancelledError&) {
                           count_failed(t, sub);
                           failure = failure_of(describe(sub), cancelled_exc());
                         } catch (const std::exception&) {
                           count_failed(t, sub);
                           failure = failure_of(describe(sub), std::current_exception());
                         } catch (...) {
                           count_failed(t, sub);
                           failure = failure_of(describe(sub), unknown_exc());
                         }
                         // Release the next frame of this chain regardless of outcome; an event
                         // that was never installed is harmless.
                         mine->set();
                         unit_finished(pub, failure);
                       });
  }
  return pub;
}

// ── publish_now: synchronous nonblocking section; blocking targets handled
//    per policy ────────────────────────────────────────────────────────────

namespace {

// publish_now runs every nonblocking subscriber on the caller's stack, so a
// subscriber that publishes back would recurse instead of suspending. A cycle
// (A's subscriber publishes to B, B's subscriber publishes back to A) then ends
// in a stack overflow with no diagnostic at all. The bound is a diagnostic, not
// a policy: legitimate nesting is the responder chain (request frame → handler →
// response frame), i.e. two or three levels.
constexpr int kMaxNowDepth = 8;
int now_depth = 0;

struct NowDepthGuard {
  NowDepthGuard() {
    if (++now_depth > kMaxNowDepth) {
      --now_depth;
      throw PublishError("shuttle: publish_now nested deeper than " + std::to_string(kMaxNowDepth) +
                         " frames — a subscriber is publishing back into a topic that is still "
                         "being dispatched (a publish_now cycle)");
    }
  }
  ~NowDepthGuard() { --now_depth; }
  NowDepthGuard(const NowDepthGuard&) = delete;
  NowDepthGuard& operator=(const NowDepthGuard&) = delete;
};

}  // namespace

std::shared_ptr<PubState> dispatch_now(Broker& b, std::shared_ptr<const Envelope> env,
                                       NowPolicy policy) {
  const NowDepthGuard depth_guard;
  const std::string_view topic = env->meta.topic_name;
  Topic& t = b.topic_for(topic);
  auto matched = b.match_subs(topic, env->meta.type_tag);
  // Partition in place instead of materializing two filtered copies: no extra
  // allocation for the blocking set, and the snapshot's shared_ptrs are never
  // re-incremented. Each registration has exactly one cap; the synchronous
  // section of `now` only touches nonblocking entries, structurally satisfying
  // the DPI timing constraints.
  const auto nb_end = std::partition(matched.begin(), matched.end(), [](const auto& sub) {
    return sub->cap == Capability::nonblocking;
  });
  const std::size_t nb_count = static_cast<std::size_t>(nb_end - matched.begin());
  // reject was preflighted before frame creation, so no blocking-target error
  // can occur here.
  const std::size_t blocking_count = matched.size() - nb_count;

  auto pub = std::make_shared<PubState>();
  pub->id = b.next_pub_id++;
  const bool defer = (policy == NowPolicy::defer) && blocking_count != 0;
  pub->target_count = nb_count + (defer ? blocking_count : 0);
  pub->deferred = defer ? blocking_count : 0;
  pub->skipped = defer ? 0 : blocking_count;

  // A single callback failure does not interrupt the remaining targets; the
  // first exception is rethrown to the publisher only after the defer chain
  // has started.
  std::exception_ptr first_exc;
  for (auto it = matched.begin(); it != nb_end; ++it) {
    const auto& sub = *it;
    try {
      if (b.deliver(sub, *env, t) == Delivery::delivered)
        ++t.delivered;
      else
        ++pub->retired;
    } catch (const weave::CancelledError&) {
      Broker::count_failed(t, sub);
      b.record_failure(pub, Broker::failure_of(Broker::describe(sub), Broker::cancelled_exc()));
      if (!first_exc) first_exc = pub->failures.back().error;
    } catch (const std::exception&) {
      Broker::count_failed(t, sub);
      b.record_failure(pub, Broker::failure_of(Broker::describe(sub), std::current_exception()));
      if (!first_exc) first_exc = pub->failures.back().error;
    } catch (...) {
      Broker::count_failed(t, sub);
      b.record_failure(pub, Broker::failure_of(Broker::describe(sub), Broker::unknown_exc()));
      if (!first_exc) first_exc = pub->failures.back().error;
    }
    b.target_finished(pub);
  }
  if (policy == NowPolicy::skip) {
    t.skipped += blocking_count;
    for (auto it = nb_end; it != matched.end(); ++it) ++(*it)->skipped;
  }
  if (!defer) {
    if (!pub->done_flag) b.pub_complete(pub);
    if (first_exc) std::rethrow_exception(first_exc);
    return pub;
  }

  // defer reuses publish_serial's chain; on completion the remaining targets
  // are merged back into this frame's handle.
  t.deferred += blocking_count;
  auto deferred_targets = std::vector<std::shared_ptr<Subscriber>>(
      std::make_move_iterator(nb_end), std::make_move_iterator(matched.end()));
  auto chain = b.start_serial(topic, std::move(env), Trigger{}, deferred_targets);
  const std::size_t n = chain->target_count;
  chain->on_complete.push_back([pub, chain, n]() {
    for (const auto& failure : chain->failures) pub->failures.push_back(failure);
    if (pub->first_error.empty()) pub->first_error = chain->first_error;
    pub->retired += chain->retired;
    pub->finished += n;
    if (!pub->done_flag) Broker::get().pub_complete(pub);
  });
  if (first_exc) std::rethrow_exception(first_exc);
  return pub;
}

// watcher plus timeout race; true = already completed, false = timed out.
bool wait_pub_timed(const std::shared_ptr<PubState>& pub, weave::duration timeout) {
  if (pub->done_flag) return true;
  weave::Task watcher = weave::create_task("shuttle-pub-watch", [pub]() { pub->done_ev.wait(); });
  try {
    weave::wait_for(watcher, timeout);
    return true;
  } catch (const weave::TimeoutError&) {
    return false;
  }
}

}  // namespace shuttle::detail
