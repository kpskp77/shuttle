// 外部接入门面：字节帧发布、外部订阅者注册、按 pub_id 的查询/等待。
//
// 这一层**与后端无关**——svdpi 只是当前唯一的调用方（Python 绑定、socket 桥会用同一组
// 入口）。所以它放在 broker 旁边而不是 `src/backend/` 里：把某个后端目录整个删掉时，
// 这里照样编译（只是没人调）。
//
// 名字的两段：
//   ext_*  = 外部调用方进来的动作（帧从外面来、订阅者在外面）；
//   pub_*  = 按 pub_id 读句柄的通用查询/等待。
//
// 记账语义（谁计入什么）由 broker 的 target_finished / pub_complete 决定，这里只做
// "句柄 ↔ id"的翻译与错误上报。

#include <algorithm>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <shuttle/detail/abi.hpp>
#include <shuttle/publication.hpp>
#include <shuttle/trigger.hpp>
#include <weave/sync.hpp>
#include <weave/task.hpp>

#include "internal/broker_impl.hpp"
#include "internal/broker_internal.hpp"

namespace shuttle {
namespace detail {

// ── 外部接入门面（错误经 set_last_error + 返回码/异常报告）────────────────

Publication ext_pub_now(const std::string& topic, const std::string& tag,
                       const std::vector<std::byte>& frame, NowPolicy policy) {
  Broker& b = Broker::get();
  b.now_preflight(topic, tag, policy);  // 先判后建帧：reject 不消耗 seq / 不刷新 retained
  auto env = b.make_envelope_bytes(topic, tag, frame);
  return detail_pub_access::make(dispatch_now(b, std::move(env), policy));
}

Publication ext_pub_serial(const std::string& topic, const std::string& tag,
                          const std::vector<std::byte>& frame, Trigger trig) {
  Broker& b = Broker::get();
  auto env = b.make_envelope_bytes(topic, tag, frame);
  auto targets = b.match_subs(topic, env->meta.type_tag);
  return detail_pub_access::make(
      b.start_serial(topic, std::move(env), std::move(trig), std::move(targets)));
}

Publication ext_pub_concurrent(const std::string& topic, const std::string& tag,
                              const std::vector<std::byte>& frame, Trigger trig) {
  Broker& b = Broker::get();
  auto env = b.make_envelope_bytes(topic, tag, frame);
  auto targets = b.match_subs(topic, env->meta.type_tag);
  return detail_pub_access::make(
      b.start_concurrent(topic, std::move(env), std::move(trig), std::move(targets)));
}

int pub_id(const Publication& p) {
  if (!p.valid()) return 0;
  const std::shared_ptr<PubState> pub = detail_pub_access::state(p);
  return pub ? pub->id : 0;
}

int ext_sub(const std::string& pattern, int caps, const std::string& type_tag, int handle) {
  try {
    Broker& b = Broker::get();
    // 一次注册 = 一条入口：caps 必须恰好是 1（`shuttle_subscribe`）或 2
    // （`shuttle_subscribe_nb`）。其它值没有定义——否则 `now` 与 `serial`/`concurrent` 会对同一个
    // 订阅者走不同入口，违背不变量 4，直接拒绝。
    if (caps != 1 && caps != 2)
      throw std::invalid_argument("shuttle: 订阅能力必须恰好一位（blocking=1 / nonblocking=2）");
    auto sub = std::make_shared<Subscriber>();
    sub->id = b.next_sub_id++;
    sub->pattern = pattern;
    sub->kind = SubKind::external;
    sub->cap = caps == 1 ? Capability::blocking : Capability::nonblocking;
    sub->handle = handle;
    sub->filter_tag = type_tag;
    b.check_filter_early(*sub);
    b.subs.push_back(sub);
    b.retained_catchup(sub);
    return 0;
  } catch (const std::exception& e) {
    set_last_error(e.what());
    return 1;
  }
}

void ext_unsub(int handle) {
  Broker& b = Broker::get();
  b.subs.erase(std::remove_if(b.subs.begin(), b.subs.end(),
                              [handle](const std::shared_ptr<Subscriber>& s) {
                                return s->kind == SubKind::external && s->handle == handle;
                              }),
               b.subs.end());
}

bool pub_done(int pub_id) {
  auto it = Broker::get().pubs.find(pub_id);
  return it != Broker::get().pubs.end() && it->second->done_flag;
}

std::string pub_error(int pub_id) {
  const auto it = Broker::get().pubs.find(pub_id);
  if (it == Broker::get().pubs.end()) return "shuttle: 未知 pub_id";
  return it->second->first_error;
}

void pub_wait(int pub_id, std::int64_t timeout_fs, int* status) {
  Broker& b = Broker::get();
  const auto it = b.pubs.find(pub_id);
  if (it == b.pubs.end()) {
    set_last_error("shuttle: 未知 pub_id " + std::to_string(pub_id));
    *status = 2;
    return;
  }
  const std::shared_ptr<PubState> pub = it->second;
  if (timeout_fs <= 0) {
    pub->done_ev.wait();
  } else {
    weave::Task watcher =
        weave::create_task("shuttle-pub-watch", [&pub]() { pub->done_ev.wait(); });
    try {
      weave::wait_for(watcher, weave::duration(timeout_fs));
    } catch (const weave::TimeoutError&) {
      *status = 1;
      return;
    }
  }
  *status = pub->failures.empty() ? 0 : 2;
}

long long pub_stat(int pub_id, int kind) {
  const auto it = Broker::get().pubs.find(pub_id);
  if (it == Broker::get().pubs.end()) return -1;
  const std::shared_ptr<PubState>& pub = it->second;
  switch (kind) {
    case 0:
      return static_cast<long long>(pub->expected);
    case 1:
      return static_cast<long long>(pub->finished);
    case 2:
      return static_cast<long long>(pub->excluded);
    case 3:
      return static_cast<long long>(pub->deferred);
    case 4:
      return static_cast<long long>(pub->retired);
    default:
      return -1;
  }
}

int pub_state(int pub_id) {
  const auto it = Broker::get().pubs.find(pub_id);
  if (it == Broker::get().pubs.end()) return -1;
  const std::shared_ptr<PubState>& pub = it->second;
  if (!pub->done_flag) return 0;
  return pub->failures.empty() ? 1 : 2;
}

void pub_on_complete(int pub_id, std::function<void()> fn) {
  const auto it = Broker::get().pubs.find(pub_id);
  if (it == Broker::get().pubs.end()) return;
  if (it->second->done_flag) {
    fn();
    return;
  }
  it->second->on_complete.push_back(std::move(fn));
}

}  // namespace detail
}  // namespace shuttle
