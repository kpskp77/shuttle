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
#include <shuttle/detail/trace.hpp>
#include <shuttle/detail/wait.hpp>
#include <shuttle/exception.hpp>
#include <shuttle/trigger.hpp>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <weave/debug.hpp>
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
  std::string what = "<no exception information>";
  try {
    if (failure->error) what = failure_error_what(*failure);
  } catch (...) {
    what = "<exception>";
  }
  if (pub->first_error.empty() && failure->error) pub->first_error = what;
  // The central failure log: every delivery failure of every flavor passes
  // through here exactly once.
  trace_dispatch()->debug("pub {} on '{}': delivery to {} failed: {}", pub->id, pub->topic_view,
                          failure->subscriber, what);
}

void Broker::pub_complete(const std::shared_ptr<PubState>& pub) {
  pub->done_flag = true;
  // A zero-target frame completes before its topic entry exists (start_serial
  // and start_concurrent short-circuit before topic_for), so topic_view may
  // still be empty here.
  const std::string_view topic =
      pub->topic_view.empty() ? std::string_view("<unregistered topic>") : pub->topic_view;
  trace_dispatch()->debug("pub {} on '{}': complete — {}/{} finished, {} retired, {} failure(s)",
                          pub->id, topic, pub->finished, pub->target_count, pub->retired,
                          pub->failures.size());
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
// Returns the displaced tail (empty when nothing was in flight): the frame must
// await its task before delivering, and that same handle is what
// check_self_blocked compares itself against.
ChainLinkPtr chain_join(Topic& topic, int key, const ChainLinkPtr& mine) {
  if (key == 0) {
    // The topic-wide tail is shared by pointer: the waiter owns the same link it
    // recorded in PubState::blocked_by, so it can re-read the task later. A task
    // handle is monotonic (a terminal task awaits immediately), so sharing cannot
    // lose a wake-up.
    ChainLinkPtr prev = std::move(topic.chain);
    topic.chain = mine;
    trace_dispatch()->trace("chain join: topic '{}' serial tail handed off", topic.name_view);
    return prev;
  }
  // A per-subscriber tail is handed off by pointer: the waiting frame keeps its
  // predecessor alive and one delivery unit costs one link instead of two.
  auto& slot = topic.sub_chains[key];
  ChainLinkPtr prev = std::move(slot);  // empty = first frame of this chain
  slot = mine;
  trace_dispatch()->trace("chain join: topic '{}' sub #{} tail handed off", topic.name_view, key);
  return prev;
}

// Release condition of one chain link: the predecessor's delivery task reached a
// terminal state. That is the handoff signal — read off weave's per-task event, so
// there is no second event to keep in step with the task's own completion.
// Its outcome must not spread into this frame: a failed or cancelled predecessor
// already recorded everything on its own PubState, and this frame's delivery is
// still owed. A CancelledError raised into *this* waiter (it was cancelled while
// queued) is not the link's, and is rethrown; `cancelled()` tells the two apart.
void await_chain(const ChainLinkPtr& prev) {
  if (!prev || !prev->task.valid() || prev->task.done()) return;
  try {
    prev->task.await();
  } catch (const weave::CancelledError&) {
    if (!prev->task.cancelled()) throw;
  } catch (...) {
  }
}

}  // namespace

// A frame whose delivery must be released by the very task waiting for it can
// never complete: the chain it is queued behind is owned by its own waiter.
// Nothing else in the design can diagnose that — the frame is not failed, not
// retired and not skipped, it simply never starts — so the wait would hang
// until the simulation is killed, which is exactly the shape of bug that costs
// hours in a regression.
//
// The check runs when someone *waits*, not when a frame joins a chain: a
// callback that publishes on its own topic and does not wait for the result is
// legitimate (fire-and-forget), and its chain link is owned by that callback's
// task too.
//
// The diagnosis ends the run through weave::fatal instead of throwing: a catch
// could only swallow it, and the frame is unrecoverable — the waiter cannot be
// handed a result that nothing will ever produce. Same call the core makes for
// its own contract violations, so the report carries the wait graph.
void check_self_blocked(const std::shared_ptr<PubState>& pub) {
  if (pub->done_flag) return;
  const std::optional<weave::Task> me = weave::current_task();
  if (!me) return;
  for (const ChainLinkPtr& link : pub->blocked_by) {
    if (link && link->task.id() == me->id()) {
      const std::string why =
          "shuttle: publication " + std::to_string(pub->id) + " on topic '" +
          std::string(pub->topic_view) +
          "' can never start: its delivery chain is held by the task that is now waiting for "
          "it (a subscriber callback published on its own topic and waited for the result)";
      weave::fatal(why);
    }
  }
}

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
    trace_dispatch()->debug("pub {} on '{}': serial delivery, no targets", pub->id, topic);
    pub_complete(pub);
    return pub;
  }

  Topic& frame_topic = topic_for(topic);
  pub->topic_view = frame_topic.name_view;
  // `mine` — the chain-tail link this frame must release — is always created
  // here and never re-read from the topic later (invariant 5). Its task handle is
  // filled in as soon as the unit exists (below), because that handle is the
  // signal a waiter awaits: a link sitting in the chain with no handle yet would
  // read as "nothing to wait for".
  auto mine = std::make_shared<ChainLink>();
  // A frame whose trigger is already satisfied reserves the tail inside the
  // publish call, so its delivery order equals the publish order and does not
  // depend on when weave first runs the task. A deferred frame must NOT reserve
  // here: an un-fired trigger would otherwise hold the topic's chain tail and
  // stall every later frame (and deadlock whenever the trigger itself depends
  // on one of them). It joins the chain at trigger time instead.
  const bool queued = trig.immediate();
  ChainLinkPtr prev;
  if (queued) {
    prev = chain_join(frame_topic, 0, mine);
    if (prev) pub->blocked_by.push_back(prev);
  }
  trace_dispatch()->debug("pub {} on '{}': serial delivery, {} target(s){}", pub->id, topic,
                          targets.size(), queued ? "" : " (deferred trigger)");
  Topic* const topic_state = &frame_topic;
  mine->task = weave::create_task(
      std::string("shuttle-serial-") + std::string(topic),
      [this, pub, env, queued, trig = std::move(trig), targets = std::move(targets), prev, mine,
       topic_state]() mutable {
        std::optional<Publication::Failure> failure;
        Topic& t = *topic_state;
        std::size_t idx = 0;
        try {
          wait_trigger(trig);
          if (!queued) {
            // Deferred frame joining the chain now. chain_join reads the tail and
            // installs ours in one step: no suspension point lies between them
            // (single-active-process invariant), so no other frame can interleave.
            prev = chain_join(t, 0, mine);
            if (prev) pub->blocked_by.push_back(prev);
          }
          await_chain(prev);
          trace_dispatch()->trace("pub {}: serial unit starts delivery ({} target(s))", pub->id,
                                  targets.size());
          for (; idx < targets.size(); ++idx) {
            const auto& sub = targets[idx];
            try {
              if (deliver(sub, *env, t) == Delivery::delivered)
                ++t.delivered;
              else
                ++pub->retired;
            } catch (const weave::CancelledError&) {
              throw;  // handled by the outer scope as a unit-level failure
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
        // Release the next frame regardless of outcome, to prevent chain stall
        // (harmless when a deferred frame never installed it). The release is this
        // task's terminal state, so nothing has to be cleared first: a waiter that
        // arrives after completion awaits an already-terminal task.
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
    trace_dispatch()->debug("pub {} on '{}': concurrent delivery, no targets", pub->id, topic);
    pub_complete(pub);
    return pub;
  }
  Topic& frame_topic = topic_for(topic);
  pub->topic_view = frame_topic.name_view;
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
  trace_dispatch()->debug("pub {} on '{}': concurrent delivery, {} target(s){}", pub->id, topic,
                          targets.size(), queued ? "" : " (deferred trigger)");
  for (const auto& sub : targets) {
    const int key = sub->id;
    const auto mine = std::make_shared<ChainLink>();
    ChainLinkPtr prev;
    if (queued) {
      prev = chain_join(frame_topic, key, mine);
      if (prev) pub->blocked_by.push_back(prev);
    }
    mine->task = weave::create_task(
        std::string("shuttle-concurrent-") + std::string(topic),
        [this, pub, env, trig, sub, topic_state, mine, prev, queued]() mutable {
          std::optional<Publication::Failure> failure;
          Topic& t = *topic_state;
          try {
            wait_trigger(trig);
            if (!queued && !sub->removed) {
              // Deferred frame joining this subscriber's chain now. A target
              // that unsubscribed during the trigger wait needs no ordering
              // (its delivery is recorded as retired anyway), and skipping the
              // handoff keeps a dead registration from leaving a chain behind.
              prev = chain_join(t, sub->id, mine);
              if (prev) pub->blocked_by.push_back(prev);
            }
            await_chain(prev);
            trace_dispatch()->trace("pub {}: concurrent unit starts for sub #{}", pub->id, sub->id);
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
          // Release the next frame of this chain regardless of outcome; a link
          // that was never installed is harmless. This task's terminal state is
          // the release itself, so there is no stale id to clear first.
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
  pub->topic_view = t.name_view;
  const bool defer = (policy == NowPolicy::defer) && blocking_count != 0;
  pub->target_count = nb_count + (defer ? blocking_count : 0);
  pub->deferred = defer ? blocking_count : 0;
  pub->skipped = defer ? 0 : blocking_count;
  trace_dispatch()->debug("pub {} on '{}': publish_now — {} nonblocking, {} blocking ({}), corr={}",
                          pub->id, topic, nb_count, blocking_count, defer ? "deferred" : "skipped",
                          env->meta.corr_id);

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
  check_self_blocked(pub);
  // The frame may complete between the check above and the wait: wait_wake
  // re-reads the level, so that race costs one wake-up, never a wrong answer.
  const bool done = wait_wake(pub->done_ev, deadline_after(timeout));
  trace_dispatch()->trace("pub {} on '{}': timed wait {} ({} fs)", pub->id, pub->topic_view,
                          done ? "completed" : "timed out", timeout.count());
  return done;
}

}  // namespace shuttle::detail
