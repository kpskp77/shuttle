// Broker 的发布调度：完成状态机、per-topic 串行链、逐订阅者并发单元与 publish_now。
// 帧创建、目标匹配与 deliver 本身见同目录 broker.cpp；本文件不区分 C++/SV 帧来源。

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

// ── 统一的完成与失败记账 ───────────────────────────────────────────────────

void Broker::record_failure(const std::shared_ptr<PubState>& pub,
                            const std::optional<Publication::Failure>& failure) {
  if (!failure) return;
  pub->failures.push_back(*failure);
  if (pub->first_error.empty()) {
    try {
      if (failure->error) pub->first_error = failure_error_what(*failure);
    } catch (...) {
      pub->first_error = "<异常>";
    }
  }
}

void Broker::pub_complete(const std::shared_ptr<PubState>& pub) {
  pub->done_flag = true;
  // 完成钩子先行：SV wait_done 依赖它唤醒等待进程；钩子失败不影响投递结果。
  auto hooks = std::exchange(pub->on_complete, {});
  for (const auto& fn : hooks) {
    try {
      fn();
    } catch (...) {
    }
  }
  pub->done_ev.set();
}

// 所有投递方式只有一个 finished 推进点；完成计数始终按目标订阅者计。
void Broker::target_finished(const std::shared_ptr<PubState>& pub) {
  if (++pub->finished == pub->expected && !pub->done_flag) pub_complete(pub);
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

// ── publish_serial：每帧一个投递单元，per-topic 串行链保发布序 FIFO ─────────

std::shared_ptr<PubState> Broker::start_serial(std::string_view topic,
                                               std::shared_ptr<const Envelope> env, Trigger trig,
                                               std::vector<std::shared_ptr<Subscriber>> targets) {
  auto pub = std::make_shared<PubState>();
  pub->id = next_pub_id++;
  pub->expected = targets.size();
  pubs[pub->id] = pub;
  if (targets.empty()) {
    // 零目标立即完成：不建 task，也不安装链尾。
    pub_complete(pub);
    return pub;
  }

  // 必须在发布瞬间捕获本帧要放行的 mine；task 体内再读 topic.chain 会拿到后来者的链尾。
  auto prev = std::make_shared<weave::Event>(topic_for(topic).chain);
  auto mine = std::make_shared<weave::Event>();
  topic_for(topic).chain = *mine;
  weave::create_task(
      std::string("shuttle-seq-") + std::string(topic),
      [this, pub, env, trig = std::move(trig), targets = std::move(targets), prev, mine]() mutable {
        std::optional<Publication::Failure> failure;
        Topic& t = topic_for(env->meta.topic);
        std::size_t idx = 0;  // 已到达终态的目标数；单元级失败时用它补齐
        try {
          wait_trigger(trig);
          prev->wait();
          for (; idx < targets.size(); ++idx) {
            const auto& sub = targets[idx];
            try {
              if (deliver(sub, *env) == Delivery::delivered)
                ++t.delivered;
              else
                ++pub->retired;
            } catch (const weave::CancelledError&) {
              throw;  // 交给外层统一记 failed，同时保证 mine 最终置位
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
        // 触发失败、取消或单元级失败时，未尝试的目标也必须进入 failed 终态。
        for (; idx < targets.size(); ++idx) {
          ++t.failed;
          ++targets[idx]->failed;
          target_finished(pub);
        }
        mine->set();  // 无论成败都放行下一帧，防止链 stall
        record_failure(pub, failure);
        if (pub->finished == pub->expected && !pub->done_flag) pub_complete(pub);
      });
  return pub;
}

// ── publish_concurrent：每个订阅者一个独立投递单元 ─────────────────────────

std::shared_ptr<PubState> Broker::start_concurrent(
    std::string_view topic, std::shared_ptr<const Envelope> env, Trigger trig,
    std::vector<std::shared_ptr<Subscriber>> targets) {
  auto pub = std::make_shared<PubState>();
  pub->id = next_pub_id++;
  pub->expected = targets.size();
  pubs[pub->id] = pub;
  if (targets.empty()) {
    pub_complete(pub);
    return pub;
  }
  for (const auto& sub : targets) {
    weave::create_task(std::string("shuttle-par-") + std::string(topic),
                       [this, pub, env, trig, sub]() mutable {
                         std::optional<Publication::Failure> failure;
                         Topic& t = topic_for(env->meta.topic);
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

// ── publish_now：同步 nonblocking 段，blocking 目标按策略处理 ───────────────

std::shared_ptr<PubState> dispatch_now(Broker& b, std::shared_ptr<const Envelope> env,
                                       NowPolicy policy) {
  const std::string_view topic = env->meta.topic;
  Topic& t = b.topic_for(topic);
  const auto matched = b.match_subs(topic, env->meta.type_tag);
  // 每次注册只有一条 cap；now 的同步段只碰 nonblocking，从结构上满足 DPI 时序约束。
  auto nb_subs = Broker::select(matched, Capability::nonblocking);
  auto blocking_targets = Broker::select(matched, Capability::blocking);
  // reject 已在建帧前预检，这里不会再因 blocking 目标抛错。

  auto pub = std::make_shared<PubState>();
  pub->id = b.next_pub_id++;
  const bool defer = (policy == NowPolicy::defer) && !blocking_targets.empty();
  pub->expected = nb_subs.size() + (defer ? blocking_targets.size() : 0);
  pub->deferred = defer ? blocking_targets.size() : 0;
  pub->skipped = defer ? 0 : blocking_targets.size();
  b.pubs[pub->id] = pub;

  // 单个回调失败不中断其余目标；defer 链启动后再把第一个异常抛给发布者。
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

  // defer 复用 publish_serial 的同一条链；完成时把剩余目标并回本帧句柄。
  t.deferred += blocking_targets.size();
  auto chain = b.start_serial(topic, std::move(env), Trigger{}, std::move(blocking_targets));
  const std::size_t n = chain->expected;
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

// watcher 与超时竞速；true = 已完成，false = 超时。
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
