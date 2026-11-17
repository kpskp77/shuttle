// Broker publish scheduling: completion state machine, per-topic serial
// chain, per-subscriber concurrent units, and publish_now.
// Frame creation, target matching, and deliver itself live in broker.cpp in
// the same directory; this file does not distinguish C++/SV frame origins.

#include <cstddef>
#include <exception>
#include <memory>
#include <optional>
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

std::vector<std::shared_ptr<Subscriber>> Broker::select(
    const std::vector<std::shared_ptr<Subscriber>>& matched, Capability want) {
  std::vector<std::shared_ptr<Subscriber>> out;
  for (const auto& sub : matched)
    if (sub->cap == want) out.push_back(sub);
  return out;
}

// ── publish_serial: one delivery unit per frame, per-topic serial chain
//    keeps publication-order FIFO ─────────────────────────────────────────

std::shared_ptr<PubState> Broker::start_serial(std::string_view topic,
                                               std::shared_ptr<const Envelope> env, Trigger trig,
                                               std::vector<std::shared_ptr<Subscriber>> targets) {
  auto pub = std::make_shared<PubState>();
  pub->id = next_pub_id++;
  pub->target_count = targets.size();
  pubs[pub->id] = pub;
  if (targets.empty()) {
    // Zero targets complete immediately: no task, no chain-tail installed.
    pub_complete(pub);
    return pub;
  }

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
  if (queued) {
    prev = std::make_shared<weave::Event>(topic_for(topic).chain);
    topic_for(topic).chain = *mine;
  }
  weave::create_task(
      std::string("shuttle-seq-") + std::string(topic),
      [this, pub, env, trig = std::move(trig), targets = std::move(targets), prev, mine]() mutable {
        std::optional<Publication::Failure> failure;
        Topic& t = topic_for(env->meta.topic_name);
        std::size_t idx =
            0;  // targets already in a terminal state; used to backfill on unit-level failure
        try {
          wait_trigger(trig);
          if (!prev) {
            // Deferred frame joining the chain now. Reading the tail and
            // installing ours is a single step: no suspension point lies
            // between them (single-active-process invariant), so no other
            // frame can interleave and steal the tail.
            prev = std::make_shared<weave::Event>(topic_for(env->meta.topic_name).chain);
            topic_for(env->meta.topic_name).chain = *mine;
          }
          prev->wait();
          for (; idx < targets.size(); ++idx) {
            const auto& sub = targets[idx];
            try {
              if (deliver(sub, *env) == Delivery::delivered)
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

// ── publish_concurrent: one independent delivery unit per subscriber ─────

std::shared_ptr<PubState> Broker::start_concurrent(
    std::string_view topic, std::shared_ptr<const Envelope> env, Trigger trig,
    std::vector<std::shared_ptr<Subscriber>> targets) {
  auto pub = std::make_shared<PubState>();
  pub->id = next_pub_id++;
  pub->target_count = targets.size();
  pubs[pub->id] = pub;
  if (targets.empty()) {
    pub_complete(pub);
    return pub;
  }
  for (const auto& sub : targets) {
    weave::create_task(std::string("shuttle-par-") + std::string(topic),
                       [this, pub, env, trig, sub]() mutable {
                         std::optional<Publication::Failure> failure;
                         Topic& t = topic_for(env->meta.topic_name);
                         try {
                           wait_trigger(trig);
                           if (deliver(sub, *env) == Delivery::delivered)
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
                         unit_finished(pub, failure);
                       });
  }
  return pub;
}

// ── publish_now: synchronous nonblocking section; blocking targets handled
//    per policy ────────────────────────────────────────────────────────────

std::shared_ptr<PubState> dispatch_now(Broker& b, std::shared_ptr<const Envelope> env,
                                       NowPolicy policy) {
  const std::string_view topic = env->meta.topic_name;
  Topic& t = b.topic_for(topic);
  const auto matched = b.match_subs(topic, env->meta.type_tag);
  // Each registration has exactly one cap; the synchronous section of `now`
  // only touches nonblocking entries, structurally satisfying the DPI timing
  // constraints.
  auto nb_subs = Broker::select(matched, Capability::nonblocking);
  auto blocking_targets = Broker::select(matched, Capability::blocking);
  // reject was preflighted before frame creation, so no blocking-target error
  // can occur here.

  auto pub = std::make_shared<PubState>();
  pub->id = b.next_pub_id++;
  const bool defer = (policy == NowPolicy::defer) && !blocking_targets.empty();
  pub->target_count = nb_subs.size() + (defer ? blocking_targets.size() : 0);
  pub->deferred = defer ? blocking_targets.size() : 0;
  pub->skipped = defer ? 0 : blocking_targets.size();
  b.pubs[pub->id] = pub;

  // A single callback failure does not interrupt the remaining targets; the
  // first exception is rethrown to the publisher only after the defer chain
  // has started.
  std::exception_ptr first_exc;
  for (const auto& sub : nb_subs) {
    try {
      if (b.deliver(sub, *env) == Delivery::delivered)
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
    t.skipped += blocking_targets.size();
    for (const auto& sub : blocking_targets) ++sub->skipped;
  }
  if (!defer) {
    if (!pub->done_flag) b.pub_complete(pub);
    if (first_exc) std::rethrow_exception(first_exc);
    return pub;
  }

  // defer reuses publish_serial's chain; on completion the remaining targets
  // are merged back into this frame's handle.
  t.deferred += blocking_targets.size();
  auto chain = b.start_serial(topic, std::move(env), Trigger{}, std::move(blocking_targets));
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
