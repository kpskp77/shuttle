#pragma once

// Request/response sugar: one request topic, one reply topic, a correlation id
// carried in the payload. Built on the same subscription primitives as mailbox
// (mailbox.hpp), which is a sibling, not a dependency: a mailbox is one FIFO,
// while replies have to be demultiplexed by rid, so this file keeps its own
// rid -> call table (see the note at the end of mailbox.hpp's header).
//
//   // responder side (any number of them, C++ or SV)
//   shuttle::serve<Req, Resp>("dev.req", "dev.resp",
//                             [](const Req& r, const shuttle::Meta&) {
//                               return Resp{0, business(r)};   // rid is filled in for you
//                             });
//
//   // requester side
//   shuttle::Requester<Req, Resp> client("dev.req", "dev.resp");
//   auto call = client.send_serial(Req{0, 21});
//   for (const auto& reply : call.wait_all(1_ms)) use(reply);
//
// ── Correlation ─────────────────────────────────────────────────────────
// The rid lives in the payload, so it crosses languages unchanged. By default
// both payload types expose a `rid` field (shuttle::RidField); pass a different
// Traits type as the third template argument to use another field:
//
//   struct MyCorr {                       // traits: "which field correlates"
//     static std::uint32_t get_rid(const Req& r)  { return r.corr; }
//     static void set_rid(Req& r, std::uint32_t v) { r.corr = v; }
//     static std::uint32_t get_rid(const Resp& r)  { return r.corr; }
//     static void set_rid(Resp& r, std::uint32_t v) { r.corr = v; }
//   };
//
// ── Why a correlation field and not Meta.seq ────────────────────────────
// Meta.seq is assigned by the broker and is not visible to the publisher of the
// frame (Publication carries counters, not the frame's seq), so a requester
// cannot learn the seq of its own request. The rid is therefore allocated by
// the Requester and overwritten by `serve` into the reply — the same value on
// both ends, with no broker support and no protocol change.
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
// that arrives afterwards is counted in stale_replies() and discarded. This is
// also what makes retained reply topics harmless (still not recommended).
//
// ── Scope ───────────────────────────────────────────────────────────────
// This is C++-side sugar; the SV side participates by subscribing to the
// request topic and publishing a reply that echoes the rid (see
// docs/dpi_protocol.md). Request and reply topics must differ: a responder
// publishes the reply from inside its request callback, and using the same
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
// exists before any reply does, it can end with zero replies (no responder), one
// (the common case) or N (one per responder), and it owns the request frame
// itself as well (publication(), rid(), cancel()). Naming it Future<Reply> would
// promise exactly one value and invite sharing it as an awaitable; naming it
// Reply would name the outcome rather than the pending request. Call also pairs
// with serve(): a call is served — possibly by several components — and its
// replies are pulled (recv / wait_for / wait_all) rather than awaited once. The
// word is the RPC one: one method invocation, correlated by an id (cf. AMQP
// reply_to + correlation_id, Erlang gen_server:call/{Pid, Tag}, DDS-RPC
// Requester/Replier).

// Default correlation-field policy: both payload types expose a `rid` field
// assignable from std::uint32_t. Pass your own traits as the third template
// argument of Requester/serve to correlate on a different field.
//
// Why a policy type and not a framework-owned header field: the payload is the
// user's own struct, encoded field-by-field in declaration order ({>>{...}} on
// the SV side), so a base class or an extra wrapper member would change the wire
// layout in both languages. The rid must therefore be a real field of the struct,
// and only its owner knows what it is called — the traits are that knowledge,
// made a compile-time parameter so that Requesters, responders and the SV echo
// cannot silently use different rules *within one type* (Requester<Req, Resp,
// Corr> names its rule in the type). Reading and writing are separate because the
// frame payload is const and shared: a responder sees `const TReq&` and writes
// the rid into its own new reply, while the requester writes it into its own copy
// of the request (the caller's object is untouched).
//
// Two limits worth knowing: the rid space is 32 bits (next_rid is a std::uint32_t
// and wraps), so a wider field must not lose the low 32 bits on the way out and
// back; and nothing checks that the requester, the responder and the SV side
// agree on the field — a mismatch shows up as "no replies", not as an error.
struct RidField {
  template <typename T>
  static std::uint32_t get_rid(const T& msg) {
    return static_cast<std::uint32_t>(msg.rid);
  }
  template <typename T>
  static void set_rid(T& msg, std::uint32_t rid) {
    msg.rid = rid;  // implicit conversion; the field must accept std::uint32_t
  }
};

namespace detail {

struct CallAccess;  // named construction channel, defined below

// Contract for the correlation traits (RidField or a replacement): the responder
// reads the rid off the request and writes it into the reply, the requester does
// the opposite, so both sides are checked here and in one place.
template <typename Traits, typename TReq, typename TResp>
constexpr void require_rid_traits() {
  static_assert(
      std::is_convertible_v<decltype(Traits::get_rid(std::declval<const TReq&>())), std::uint32_t>,
      "shuttle: correlation traits need std::uint32_t get_rid(const TReq&)");
  static_assert(std::is_void_v<decltype(Traits::set_rid(std::declval<TReq&>(), std::uint32_t{}))>,
                "shuttle: correlation traits need void set_rid(TReq&, std::uint32_t)");
  static_assert(
      std::is_convertible_v<decltype(Traits::get_rid(std::declval<const TResp&>())), std::uint32_t>,
      "shuttle: correlation traits need std::uint32_t get_rid(const TResp&)");
  static_assert(std::is_void_v<decltype(Traits::set_rid(std::declval<TResp&>(), std::uint32_t{}))>,
                "shuttle: correlation traits need void set_rid(TResp&, std::uint32_t)");
}

// One outstanding request: the replies that already arrived for it.
template <typename TResp>
struct CallState {
  std::deque<TResp> items;
  weave::Event wake;
  bool cancelled = false;
};

// Shared by the Requester and its Calls: one reply subscription, the demux
// table (rid -> weak CallState) and the rid counter. Weak entries are what make
// "drop the handle" free the slot: a reply for an expired slot is stale.
template <typename TResp>
struct ReplyTable {
  std::map<std::uint32_t, std::weak_ptr<CallState<TResp>>> inflight;
  std::uint32_t next_rid = 1;
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

  // Delivery path (nonblocking entry): route one reply to its call.
  void deliver(const TResp& msg, std::uint32_t rid) {
    auto it = inflight.find(rid);
    if (it == inflight.end()) {
      ++stale;  // reply for a call that was cancelled or already dropped
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
    call->items.push_back(msg);
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

inline void check_topics(const std::string& request_topic, const std::string& reply_topic) {
  if (request_topic == reply_topic)
    throw std::invalid_argument(
        "shuttle: request and reply topics must differ (the responder publishes the reply from "
        "inside the request callback, so the same topic would deadlock the serial chain)");
  if (request_topic.empty() || reply_topic.empty())
    throw std::invalid_argument("shuttle: request/response needs two non-empty topic names");
}

}  // namespace detail

// Handle for one outstanding request: the replies that belong to it.
template <typename TResp>
class Call {
public:
  Call() = default;

  [[nodiscard]] bool valid() const { return static_cast<bool>(state_); }
  [[nodiscard]] std::uint32_t rid() const { return rid_; }
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
  std::uint32_t rid_ = 0;
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
       std::uint32_t rid, Publication pub)
      : state_(std::move(state)), cancel_(std::move(cancel)), rid_(rid), pub_(std::move(pub)) {}

  friend struct detail::CallAccess;
};

namespace detail {

struct CallAccess {
  template <typename TResp>
  static Call<TResp> make(std::shared_ptr<CallState<TResp>> state, std::function<void()> cancel,
                          std::uint32_t rid, Publication pub) {
    return Call<TResp>(std::move(state), std::move(cancel), rid, std::move(pub));
  }
};

}  // namespace detail

// Request side: publishes on the request topic and demultiplexes the replies.
template <typename TReq, typename TResp, typename Traits = RidField>
class Requester {
public:
  Requester(std::string request_topic, std::string reply_topic)
      : request_topic_(std::move(request_topic)),
        reply_topic_(std::move(reply_topic)),
        table_(std::make_shared<detail::ReplyTable<TResp>>()) {
    detail::require_rid_traits<Traits, TReq, TResp>();
    detail::check_topics(request_topic_, reply_topic_);
    state_ = std::make_shared<State>(request_topic_, reply_topic_, table_);
  }

  [[nodiscard]] bool valid() const { return static_cast<bool>(state_); }
  [[nodiscard]] const std::string& request_topic() const { return request_topic_; }
  [[nodiscard]] const std::string& reply_topic() const { return reply_topic_; }
  // The rid the next send_*() will use (diagnostics / cross-language assertions)
  [[nodiscard]] std::uint32_t next_rid() const { return table_->next_rid; }
  [[nodiscard]] std::size_t inflight() const {
    std::size_t n = 0;
    for (const auto& entry : table_->inflight)
      if (!entry.second.expired()) ++n;
    return n;
  }
  // Replies whose rid belonged to no live call (cancelled / dropped handles)
  [[nodiscard]] std::uint64_t stale_replies() const { return table_->stale; }

  // Same three flavors and parameter shapes as Publisher: send_serial /
  // send_concurrent / send_now map 1:1 onto publish_serial / publish_concurrent /
  // publish_now. The mode suffix is never dropped: `serial` is the default
  // delivery, not an unnamed special case.
  Call<TResp> send_serial(const TReq& req, Trigger trig = {}) const {
    return send_impl(req,
                     [trig = std::move(trig)](const std::shared_ptr<State>& st, const TReq& msg) {
                       return st->pub.publish_serial(msg, trig);
                     });
  }
  Call<TResp> send_concurrent(const TReq& req, Trigger trig = {}) const {
    return send_impl(req,
                     [trig = std::move(trig)](const std::shared_ptr<State>& st, const TReq& msg) {
                       return st->pub.publish_concurrent(msg, trig);
                     });
  }
  // Synchronous flavor: only the nonblocking-entry responders run on the
  // publisher's stack; blocking ones end up on the background chain per policy.
  Call<TResp> send_now(const TReq& req, NowPolicy policy = NowPolicy::defer) const {
    return send_impl(req, [policy](const std::shared_ptr<State>& st, const TReq& msg) {
      return st->pub.publish_now(msg, policy);
    });
  }

private:
  struct State {
    State(std::string request_topic, std::string reply_topic,
          std::shared_ptr<detail::ReplyTable<TResp>> table)
        : pub(std::move(request_topic)),
          table_(std::move(table)),
          reply_topic(std::move(reply_topic)) {
      // Weak capture: the subscription lives inside the table (table->sub), so a
      // strong capture would be a cycle.
      std::weak_ptr<detail::ReplyTable<TResp>> weak = table_;
      // `reply_topic` (the parameter) is already moved into the member above.
      table_->sub = detail::sub_typed(
          this->reply_topic, detail::type_ops<TResp>(),
          [weak](const void* payload, const Meta&) {
            auto t = weak.lock();
            if (!t) return;
            const TResp& msg = *static_cast<const TResp*>(payload);
            t->deliver(msg, Traits::get_rid(msg));
          },
          Capability::nonblocking, SubOptions{});
    }

    Publisher<TReq> pub;
    std::shared_ptr<detail::ReplyTable<TResp>> table_;
    std::string reply_topic;
  };

  template <typename Publish>
  Call<TResp> send_impl(const TReq& req, Publish&& publish) const {
    if (!state_) throw std::logic_error("shuttle: empty Requester handle");
    std::shared_ptr<State> st = state_;
    table_->prune();
    const std::uint32_t rid = table_->next_rid++;
    TReq carried = req;
    Traits::set_rid(carried, rid);

    auto call = std::make_shared<detail::CallState<TResp>>();
    table_->inflight[rid] = call;  // before publishing: a reply may arrive inline
    Publication pub = publish(st, carried);
    auto table = table_;
    return detail::CallAccess::make<TResp>(
        call, [table, rid] { table->inflight.erase(rid); }, rid, pub);
  }

  std::string request_topic_;
  std::string reply_topic_;
  std::shared_ptr<detail::ReplyTable<TResp>> table_;
  std::shared_ptr<State> state_;
};

// Responder sugar: subscribe to the request topic and publish the handler's
// return value on the reply topic, with the correlation id echoed from the
// request. The handler may throw: that is recorded as this frame's delivery
// Failure.
//
// The entry is chosen exactly like subscribe(): a plain handler registers the
// blocking entry (a responder usually consumes simulation time), nb(handler) the
// nonblocking one. Deliberately no runtime `Capability` flag: one registration =
// one entry, and the entry must be visible at the call site (the SV API spells
// the same distinction as shuttle_subscribe vs shuttle_subscribe_nb).
namespace detail {

template <typename TReq, typename TResp, typename Traits, typename F>
auto serve_body(std::string reply_topic, F&& handler) {
  static_assert(
      std::is_invocable_r_v<TResp, F&, const TReq&, const Meta&>,
      "shuttle: serve handler signature should be TResp(const TReq&, const shuttle::Meta&)");
  require_rid_traits<Traits, TReq, TResp>();
  auto reply_pub =
      std::make_shared<Publisher<TResp>>(std::move(reply_topic));  // outlives the subscription
  return [reply_pub, fn = std::forward<F>(handler)](const TReq& req, const Meta& meta) mutable {
    TResp resp = fn(req, meta);
    Traits::set_rid(resp, Traits::get_rid(req));  // correlation comes from the request
    reply_pub->publish_serial(resp);              // reply in the background
  };
}

}  // namespace detail

template <typename TReq, typename TResp, typename Traits = RidField, typename F>
Subscription serve(std::string request_pattern, std::string reply_topic, F&& handler,
                   SubOptions opts = {}) {
  detail::check_topics(request_pattern, reply_topic);
  auto body =
      detail::serve_body<TReq, TResp, Traits>(std::move(reply_topic), std::forward<F>(handler));
  return subscribe<TReq>(std::move(request_pattern), std::move(body), std::move(opts));
}

template <typename TReq, typename TResp, typename Traits = RidField, typename F>
Subscription serve(std::string request_pattern, std::string reply_topic, NonBlocking<F> handler,
                   SubOptions opts = {}) {
  detail::check_topics(request_pattern, reply_topic);
  auto body =
      detail::serve_body<TReq, TResp, Traits>(std::move(reply_topic), std::move(handler.fn));
  return subscribe<TReq>(std::move(request_pattern), nb(std::move(body)), std::move(opts));
}

}  // namespace shuttle
