#pragma once

// Request/response sugar: one request topic, one response topic, and a correlation id that
// travels in the frame header (`Meta::corr_id`), never in the payload. Built on the same
// subscription primitives as mailbox (mailbox.hpp), which is a sibling, not a dependency:
// a mailbox is one FIFO, while responses have to be demultiplexed by corr_id, so this file
// keeps its own corr_id -> call table (see the note at the end of mailbox.hpp's header).
//
//   // responder side (any number of them, C++ or SV)
//   shuttle::serve<Req, Resp>("dev.req", "dev.resp",
//                             [](const Req& r, const shuttle::Meta&) {
//                               return Resp{business(r)};   // corr_id is filled in for you
//                             });
//
//   // requester side
//   shuttle::Requester<Req, Resp> client("dev.req", "dev.resp");
//   auto call = client.send_serial(Req{21});
//   for (const auto& resp : call.wait_all(1_ms)) use(resp);
//
// ── Correlation ─────────────────────────────────────────────────────────
// `Meta::corr_id` is the *correlation id*: allocated by the Requester, echoed by
// whoever answers, used to route each response to its Call. It is the AMQP
// `correlation_id` idea, scoped to one simulation process. (It is not the PCIe
// Requester ID — that one names a bus/device/function, this one names a call.)
//
// It lives in the frame header rather than the payload for a practical reason: the
// payload type is often not yours (a third-party struct, a generated type, a packed SV
// struct), so forcing an extra field into it — or wrapping it on every call — is
// intrusive and easy to get wrong on one side. Every frame already carries Meta in both
// languages, so the id costs one header word and changes no payload type at all.
//
// Nothing in the framework interprets corr_id: the broker copies it verbatim, and only
// Requester/serve give it meaning. A plain publish leaves it 0 (uncorrelated), and any
// subscriber may read it for logging. The public publish trio is deliberately untouched —
// `Publisher::publish_now/publish_serial/publish_concurrent` take no corr_id and never will:
// correlation is a request/response concept, so only the request/response objects
// (Requester::send_*, serve, and SV `shuttle_pub::reply`) write it.
//
// ── Why not Meta.topic_seq ──────────────────────────────────────────────
// topic_seq is assigned by the broker while the frame is built (the publisher cannot see
// it beforehand) and is a *per-topic* counter. Correlation needs a value known *before*
// publishing — the slot has to be registered before the frame leaves, because a response
// can arrive inline in publish_now — and it needs to stay unique across topics when the
// responder serves a wildcard pattern. corr_id provides both, in the header, in both
// languages.
//
// ── Counting replies ────────────────────────────────────────────────────
// Nothing in the framework knows "how many replies to expect", so the count is
// explicit: wait_for(n, timeout) waits for up to n replies, and wait_all()
// assumes the common convention "every target subscriber of the request replies
// exactly once" (n = the request frame's subscriber_count()). A responder that
// only observes would break that assumption — use wait_for/recv there.
//
// ── Lifetime and stale replies ──────────────────────────────────────────
// A Call holds the only reference to its slot; the Requester keeps a weak
// reference, so dropping the handle (or cancel()) frees the slot and any reply
// that arrives afterwards is counted in stale_responses() and discarded. This is
// also what makes retained response topics harmless (still not recommended).
//
// ── Scope ───────────────────────────────────────────────────────────────
// This is C++-side sugar; the SV side participates by subscribing to the request
// topic and answering with `pub.reply(resp, meta)`, which echoes meta.corr_id into the
// response frame (see docs/dpi_protocol.md). Request and response topics must differ: a
// responder publishes the response from inside its request callback, and using the same
// topic would put that frame behind the very callback that publishes it.

#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <shuttle/detail/abi.hpp>
#include <shuttle/exception.hpp>
#include <shuttle/meta.hpp>
#include <shuttle/publication.hpp>
#include <shuttle/publisher.hpp>
#include <shuttle/subscription.hpp>
#include <shuttle/trigger.hpp>
#include <shuttle/types.hpp>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>
#include <weave/event.hpp>
#include <weave/sync.hpp>
#include <weave/task.hpp>
#include <weave/time.hpp>

namespace shuttle {

// ── Why the handle is called Call ────────────────────────────────────────
// send_serial() returns a Call<TResp>: one *invocation in flight*, not one result. It
// exists before any response does, it can end with zero responses (no responder), one
// (the common case) or N (one per responder), and it owns the request frame
// itself as well (publication(), corr_id(), cancel()). Naming it Future<Reply> would
// promise exactly one value and invite sharing it as an awaitable; naming it
// Reply would name the outcome rather than the pending request. Call also pairs
// with serve(): a call is served — possibly by several components — and its
// responses are pulled (recv / wait_for / wait_all) rather than awaited once. The
// word is the RPC one: one method invocation, correlated by an id (cf. AMQP
// reply_to + correlation_id, Erlang gen_server:call/{Pid, Tag}, DDS-RPC
// Requester/Replier).

// The correlation-id counter is process-wide, not per-Requester.
//
// The simulation runs one active process at a time (weave's port contract, AGENTS.md
// invariant 1), so an ordinary increment needs no atomic and no lock: there is no
// suspension point between reading and storing it. It is nonetheless shared by every
// Requester, which is what makes two Requesters on one response topic safe: ids cannot
// collide, so a response can never be routed to the wrong caller. Starting at 1 and
// skipping 0 (reserved for "uncorrelated") is the whole policy.
//
// C++ and the simulator are one process, so ids allocated here are unique across both
// languages for the lifetime of the simulation. A future SV-side requester would have
// to allocate from a disjoint range of the 32-bit space, because it cannot see this
// counter without an extra DPI call.
namespace detail {

inline std::uint32_t& corr_id_counter() {
  static std::uint32_t counter = 0;
  return counter;
}

// Next id to use; 0 never comes back (0 means "this frame is not correlated").
inline std::uint32_t take_corr_id() {
  std::uint32_t& counter = corr_id_counter();
  if (++counter == 0) counter = 1;
  return counter;
}

struct CallAccess;  // named construction channel, defined below

// One outstanding request: the replies that already arrived for it.
template <typename TResp>
struct CallState {
  std::deque<TResp> items;
  weave::Event wake;
  bool cancelled = false;
};

// Shared by the Requester and its Calls: one response subscription, the demux
// table (corr_id -> weak CallState) and the stale counter. Weak entries are what make
// "drop the handle" free the slot: a response for an expired slot is stale.
template <typename TResp>
struct ResponseTable {
  std::map<std::uint32_t, std::weak_ptr<CallState<TResp>>> inflight;
  std::uint64_t stale = 0;
  Subscription sub;

  void prune() {
    for (auto it = inflight.begin(); it != inflight.end();) {
      if (it->second.expired())
        it = inflight.erase(it);
      else
        ++it;
    }
  }

  // Delivery path (nonblocking entry): route one response to its call.
  void deliver(const TResp& resp, std::uint32_t corr_id) {
    auto it = inflight.find(corr_id);
    if (it == inflight.end()) {
      ++stale;  // response for a call that was cancelled or already dropped
      return;
    }
    auto call = it->second.lock();
    if (!call) {
      ++stale;
      inflight.erase(it);
      return;
    }
    if (call->cancelled) {
      ++stale;
      return;
    }
    call->items.push_back(resp);
    call->wake.set();
  }
};

// Blocks until the call's queue is non-empty, the deadline passes, or the slot
// is cancelled. The waiter task captures the state, so a timeout never leaves a
// dangling reference behind.
template <typename TResp>
bool wait_call(const std::shared_ptr<CallState<TResp>>& call, std::int64_t deadline_fs) {
  while (call->items.empty()) {
    if (call->cancelled) return false;
    if (deadline_fs >= 0 && now_fs() >= deadline_fs) return false;
    weave::Task waiter = weave::create_task("shuttle-call-wait", [call] { call->wake.wait(); });
    try {
      if (deadline_fs < 0) {
        waiter.await();
      } else {
        const std::int64_t remaining = deadline_fs - now_fs();
        if (remaining <= 0) return false;
        weave::wait_for(waiter, weave::duration(remaining));
      }
    } catch (const weave::TimeoutError&) {
      return false;
    }
  }
  return true;
}

inline std::int64_t deadline_after(weave::duration timeout) {
  return now_fs() + timeout.count();
}

// `req_name` may be a pattern (serve() accepts one) while `resp_topic` is the
// concrete name the responder publishes to: the rule only needs the two to
// differ as strings, so one check serves both callers.
inline void check_topics(const std::string& req_topic, const std::string& resp_topic) {
  if (req_topic == resp_topic)
    throw std::invalid_argument(
        "shuttle: request and response topics must differ (the responder publishes the response "
        "from inside the request callback, so the same topic would deadlock the serial chain)");
  if (req_topic.empty() || resp_topic.empty())
    throw std::invalid_argument("shuttle: request/response needs two non-empty topic names");
}

}  // namespace detail

// Handle for one outstanding request: the replies that belong to it.
template <typename TResp>
class Call {
public:
  Call() = default;

  [[nodiscard]] bool valid() const { return static_cast<bool>(state_); }
  [[nodiscard]] std::uint32_t corr_id() const { return corr_id_; }
  // The request frame (counters: subscriber_count() == expected replies for
  // wait_all(), failures() if a responder's handler threw)
  [[nodiscard]] Publication publication() const { return pub_; }
  // Replies that already arrived and are waiting to be taken
  [[nodiscard]] std::size_t queued() const { return state_ ? state_->items.size() : 0; }
  [[nodiscard]] bool cancelled() const { return !state_ || state_->cancelled; }

  // Next reply; blocks until one arrives (use the timed form unless a responder
  // is guaranteed to answer).
  std::optional<TResp> recv() {
    if (!state_) return std::nullopt;
    if (!detail::wait_call(state_, -1)) return std::nullopt;
    return take();
  }

  // Next reply, giving up after `timeout` (nullopt = none arrived in time; the
  // slot stays alive, so a later reply is still accepted).
  std::optional<TResp> recv(weave::duration timeout) {
    if (!state_) return std::nullopt;
    if (!detail::wait_call(state_, detail::deadline_after(timeout))) return std::nullopt;
    return take();
  }

  // Up to `n` replies, giving up after `timeout`. Fewer than n means some
  // responder did not answer (or answered later, which is a stale reply).
  std::vector<TResp> wait_for(std::size_t n, weave::duration timeout) {
    std::vector<TResp> out;
    if (!state_) return out;
    const std::int64_t deadline = detail::deadline_after(timeout);
    while (out.size() < n) {
      if (state_->cancelled) break;
      if (!detail::wait_call(state_, deadline)) break;
      out.push_back(take());
    }
    return out;
  }

  // wait_for(the request frame's subscriber_count()): the convention "every
  // target replies exactly once".
  std::vector<TResp> wait_all(weave::duration timeout) {
    const std::size_t expected = pub_.valid() ? pub_.subscriber_count() : 0;
    return wait_for(expected, timeout);
  }

  // Stop accepting replies: the slot is freed, later replies become stale, and
  // a blocked recv() returns nullopt.
  void cancel() {
    if (!state_ || state_->cancelled) return;
    state_->cancelled = true;
    state_->items.clear();
    state_->wake.set();
    if (cancel_) cancel_();
  }

private:
  std::shared_ptr<detail::CallState<TResp>> state_;
  std::function<void()> cancel_;  // erases the slot from the requester's table
  std::uint32_t corr_id_ = 0;
  Publication pub_;

  TResp take() {
    TResp msg = std::move(state_->items.front());
    state_->items.pop_front();
    // Clear the level flag when the queue runs dry, otherwise the next wait
    // returns immediately and spins.
    if (state_->items.empty()) state_->wake.clear();
    return msg;
  }

  Call(std::shared_ptr<detail::CallState<TResp>> state, std::function<void()> cancel,
       std::uint32_t corr_id, Publication pub)
      : state_(std::move(state)),
        cancel_(std::move(cancel)),
        corr_id_(corr_id),
        pub_(std::move(pub)) {}

  friend struct detail::CallAccess;
};

namespace detail {

struct CallAccess {
  template <typename TResp>
  static Call<TResp> make(std::shared_ptr<CallState<TResp>> state, std::function<void()> cancel,
                          std::uint32_t corr_id, Publication pub) {
    return Call<TResp>(std::move(state), std::move(cancel), corr_id, std::move(pub));
  }
};

}  // namespace detail

// Request side: publishes on the request topic and demultiplexes the responses.
template <typename TReq, typename TResp>
class Requester {
public:
  Requester(std::string req_topic, std::string resp_topic)
      : req_topic_(std::move(req_topic)),
        resp_topic_(std::move(resp_topic)),
        table_(std::make_shared<detail::ResponseTable<TResp>>()) {
    detail::check_topics(req_topic_, resp_topic_);
    // Claim the response topic before subscribing: from here on a blocking
    // subscriber on it is refused (see topic_mark_responses).
    detail::topic_mark_responses(resp_topic_);
    state_ = std::make_shared<State>(req_topic_, resp_topic_, table_);
  }

  [[nodiscard]] bool valid() const { return static_cast<bool>(state_); }
  [[nodiscard]] const std::string& req_topic() const { return req_topic_; }
  [[nodiscard]] const std::string& resp_topic() const { return resp_topic_; }
  // The corr_id the next send_*() will use (diagnostics / cross-language assertions)
  [[nodiscard]] std::uint32_t next_corr_id() const { return detail::corr_id_counter() + 1; }
  [[nodiscard]] std::size_t inflight() const {
    std::size_t n = 0;
    for (const auto& [corr_id, slot] : table_->inflight) {
      (void)corr_id;  // the slot alone decides liveness; the id is not needed here
      if (!slot.expired()) ++n;
    }
    return n;
  }
  // Responses whose corr_id belonged to no live call (cancelled / dropped handles)
  [[nodiscard]] std::uint64_t stale_responses() const { return table_->stale; }

  // Same three flavors and parameter shapes as Publisher: send_serial /
  // send_concurrent / send_now map 1:1 onto publish_serial / publish_concurrent /
  // publish_now. The mode suffix is never dropped: `serial` is the default
  // delivery, not an unnamed special case. The request is taken by value so a
  // prvalue (`send_serial(Req{21})`) is elided into the frame rather than copied.
  Call<TResp> send_serial(TReq req, Trigger trig = {}) const {
    return send_impl(std::move(req), [trig = std::move(trig)](const std::shared_ptr<State>& st,
                                                              TReq&& msg, std::uint32_t corr_id) {
      return detail::pub_serial(st->pub.topic(), std::make_shared<const TReq>(std::move(msg)),
                                detail::type_ops<TReq>(), trig, corr_id);
    });
  }
  Call<TResp> send_concurrent(TReq req, Trigger trig = {}) const {
    return send_impl(std::move(req), [trig = std::move(trig)](const std::shared_ptr<State>& st,
                                                              TReq&& msg, std::uint32_t corr_id) {
      return detail::pub_concurrent(st->pub.topic(), std::make_shared<const TReq>(std::move(msg)),
                                    detail::type_ops<TReq>(), trig, corr_id);
    });
  }
  // Synchronous flavor: only the nonblocking-entry responders run on the
  // publisher's stack; blocking ones end up on the background chain per policy.
  Call<TResp> send_now(TReq req, NowPolicy policy = NowPolicy::defer) const {
    return send_impl(std::move(req), [policy](const std::shared_ptr<State>& st, TReq&& msg,
                                              std::uint32_t corr_id) {
      return detail::pub_now(st->pub.topic(), std::make_shared<const TReq>(std::move(msg)),
                             detail::type_ops<TReq>(), policy, corr_id);
    });
  }

private:
  struct State {
    State(std::string req_topic, std::string resp_topic,
          std::shared_ptr<detail::ResponseTable<TResp>> table)
        : pub(std::move(req_topic)), table_(std::move(table)), resp_topic(std::move(resp_topic)) {
      // Weak capture: the subscription lives inside the table (table->sub), so a
      // strong capture would be a cycle.
      std::weak_ptr<detail::ResponseTable<TResp>> weak = table_;
      // `resp_topic` (the parameter) is already moved into the member above.
      table_->sub = detail::sub_typed(
          this->resp_topic, detail::type_ops<TResp>(),
          [weak](const void* payload, const Meta& meta) {
            auto t = weak.lock();
            if (!t) return;
            const TResp& msg = *static_cast<const TResp*>(payload);
            t->deliver(msg, meta.corr_id);  // correlation comes from the frame header
          },
          Capability::nonblocking, SubOptions{});
    }

    Publisher<TReq> pub;
    std::shared_ptr<detail::ResponseTable<TResp>> table_;
    std::string resp_topic;
  };

  template <typename Publish>
  Call<TResp> send_impl(TReq req, Publish&& publish) const {
    if (!state_) throw std::logic_error("shuttle: empty Requester handle");
    std::shared_ptr<State> st = state_;
    table_->prune();
    // The id must exist before the frame does: a responder reached synchronously
    // (send_now + nonblocking entry) may publish its response inside publish().
    const std::uint32_t corr_id = detail::take_corr_id();

    auto call = std::make_shared<detail::CallState<TResp>>();
    table_->inflight[corr_id] = call;  // before publishing: a response may arrive inline
    Publication pub = publish(st, std::move(req), corr_id);
    auto table = table_;
    return detail::CallAccess::make<TResp>(
        call, [table, corr_id] { table->inflight.erase(corr_id); }, corr_id, pub);
  }

  std::string req_topic_;
  std::string resp_topic_;
  std::shared_ptr<detail::ResponseTable<TResp>> table_;
  std::shared_ptr<State> state_;
};

// Responder sugar: subscribe to the request topic and publish the handler's
// return value on the response topic, with the correlation id echoed from the
// request. The handler may throw: that is recorded as the request frame's
// delivery Failure.
//
// The entry is chosen exactly like subscribe(): a plain handler registers the
// blocking entry (a responder usually consumes simulation time), nb(handler) the
// nonblocking one. Deliberately no runtime `Capability` flag: one registration =
// one entry, and the entry must be visible at the call site (the SV API spells
// the same distinction as shuttle_subscribe vs shuttle_subscribe_nb).
//
// The response goes out through publish_now, so with nb(handler) the whole round
// trip completes on the requester's stack: no task switch, no simulation time
// spent, and the requester holds the reply as soon as its send_now() returns.
// The response topic is marked as a response channel, so a blocking subscriber
// on it is refused at registration (see detail::topic_mark_responses).
namespace detail {

template <typename TReq, typename TResp, typename F>
auto serve_body(std::string resp_topic, F&& handler) {
  static_assert(
      std::is_invocable_r_v<TResp, F&, const TReq&, const Meta&>,
      "shuttle: serve handler signature should be TResp(const TReq&, const shuttle::Meta&)");
  // The response topic is a response channel: latency-critical, nonblocking
  // watchers only. Claimed before the publisher exists so the first registration
  // on it already sees the mark.
  detail::topic_mark_responses(resp_topic);
  auto resp_pub =
      std::make_shared<Publisher<TResp>>(std::move(resp_topic));  // outlives the subscription
  return [resp_pub, fn = std::forward<F>(handler)](const TReq& req, const Meta& meta) mutable {
    TResp resp = fn(req, meta);
    // Same as resp_pub->publish_now(resp), plus the correlation id that the
    // public trio has no parameter for. `defer` keeps the frame correct even if a
    // blocking subscriber slipped in before the mark: it is handed to the chain,
    // while every nonblocking subscriber (in particular the requester) is served
    // inline, so the reply exists before the publisher returns.
    detail::pub_now(resp_pub->topic(), std::make_shared<const TResp>(std::move(resp)),
                    detail::type_ops<TResp>(), NowPolicy::defer, meta.corr_id);
  };
}

}  // namespace detail

template <typename TReq, typename TResp, typename F>
Subscription serve(std::string req_pattern, std::string resp_topic, F&& handler,
                   SubOptions opts = {}) {
  detail::check_topics(req_pattern, resp_topic);
  auto body = detail::serve_body<TReq, TResp>(std::move(resp_topic), std::forward<F>(handler));
  return subscribe<TReq>(std::move(req_pattern), std::move(body), std::move(opts));
}

template <typename TReq, typename TResp, typename F>
Subscription serve(std::string req_pattern, std::string resp_topic, NonBlocking<F> handler,
                   SubOptions opts = {}) {
  detail::check_topics(req_pattern, resp_topic);
  auto body = detail::serve_body<TReq, TResp>(std::move(resp_topic), std::move(handler.fn));
  return subscribe<TReq>(std::move(req_pattern), nb(std::move(body)), std::move(opts));
}

}  // namespace shuttle
