// 外部接入门面：字节帧发布、外部订阅者注册、按 pub_id 的查询/等待、跨语言事件注册表。
//
// 这一层**与后端无关**——svdpi 只是当前唯一的调用方（Python 绑定、socket 桥会用同一组
// 入口）。所以它放在 broker 旁边而不是 `src/backend/` 里：把某个后端目录整个删掉时，
// 这里照样编译（只是没人调）。事件注册表（SV shuttle_event / SHUTTLE_TRIG_EVENT）
// 同属"外部通道"，C++ 自己的可等待信号用 weave::Event，不进这张表。
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
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <shuttle/detail/abi.hpp>
#include <shuttle/publication.hpp>
#include <shuttle/trigger.hpp>
#include <weave/event.hpp>

#include "internal/broker_impl.hpp"
#include "internal/broker_internal.hpp"

namespace shuttle::detail {

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

int ext_sub(std::string pattern, int caps, std::string type_tag, int handle) {
  try {
    Broker& b = Broker::get();
    // 一次注册 = 一条入口：caps 必须恰好是 1（`shuttle_subscribe`）或 2
    // （`shuttle_subscribe_nb`）。其它值没有定义——否则 `now` 与 `serial`/`concurrent` 会对同一个
    // 订阅者走不同入口，违背不变量 4，直接拒绝。
    if (caps != 1 && caps != 2)
      throw std::invalid_argument("shuttle: 订阅能力必须恰好一位（blocking=1 / nonblocking=2）");
    b.register_sub(std::move(pattern), SubKind::external,
                   caps == 1 ? Capability::blocking : Capability::nonblocking,
                   std::move(type_tag), nullptr, {}, {}, handle);
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

namespace {

// 按 id 取发布状态（未知 id = nullptr）：下面五个门面函数共用的唯一入口，
// 免得每处都写一遍 find + 判 end。
std::shared_ptr<PubState> pub_of(int pub_id) {
  Broker& b = Broker::get();
  const auto it = b.pubs.find(pub_id);
  return it != b.pubs.end() ? it->second : nullptr;
}

}  // namespace

bool pub_done(int pub_id) {
  const auto pub = pub_of(pub_id);
  return pub && pub->done_flag;
}

std::string pub_error(int pub_id) {
  const auto pub = pub_of(pub_id);
  return pub ? pub->first_error : std::string("shuttle: 未知 pub_id");
}

void pub_wait(int pub_id, std::int64_t timeout_fs, int* status) {
  const std::shared_ptr<PubState> pub = pub_of(pub_id);
  if (!pub) {
    set_last_error("shuttle: 未知 pub_id " + std::to_string(pub_id));
    *status = 2;
    return;
  }
  if (timeout_fs <= 0) {
    pub->done_ev.wait();
  } else if (!wait_pub_timed(pub, weave::duration(timeout_fs))) {
    *status = 1;
    return;
  }
  *status = pub->failures.empty() ? 0 : 2;
}

long long pub_stat(int pub_id, int kind) {
  const std::shared_ptr<PubState> pub = pub_of(pub_id);
  if (!pub) return -1;
  switch (kind) {
    case 0:
      return static_cast<long long>(pub->expected);
    case 1:
      return static_cast<long long>(pub->finished);
    case 2:
      return static_cast<long long>(pub->skipped);
    case 3:
      return static_cast<long long>(pub->deferred);
    case 4:
      return static_cast<long long>(pub->retired);
    default:
      return -1;
  }
}

int pub_state(int pub_id) {
  const std::shared_ptr<PubState> pub = pub_of(pub_id);
  if (!pub) return -1;
  if (!pub->done_flag) return 0;
  return pub->failures.empty() ? 1 : 2;
}

void pub_on_complete(int pub_id, std::function<void()> fn) {
  const std::shared_ptr<PubState> pub = pub_of(pub_id);
  if (!pub) return;
  if (pub->done_flag) {
    fn();
    return;
  }
  pub->on_complete.push_back(std::move(fn));
}

// ── 跨语言事件注册表（SV 侧 shuttle_event / SHUTTLE_TRIG_EVENT）──────────────
// 事件只由 SV 侧创建（shuttle_c_event_create），C++ 侧从不持有句柄——按 id 访问，
// 所以注册表独自持有对象，destroy 即释放（SV 的 shuttle_event::close() 走这里）。
namespace {
struct EventSlot {
  int id;
  weave::Event ev;
};

std::map<int, std::shared_ptr<EventSlot>> g_events;
int g_next_event_id = 1;

std::shared_ptr<EventSlot> event_lookup(int ev_id) {
  if (const auto it = g_events.find(ev_id); it != g_events.end()) return it->second;
  return nullptr;
}
}  // namespace

// TRIG_EVENT（SV 侧按 id 引用事件）的等待入口：必须在 weave task 上下文调用。
void event_wait_id(int ev_id) {
  const auto slot = event_lookup(ev_id);
  if (!slot)
    throw std::invalid_argument("shuttle: 未知或已销毁的 event id " + std::to_string(ev_id));
  slot->ev.wait();
}

int event_registry_create() {
  auto slot = std::make_shared<EventSlot>(EventSlot{g_next_event_id++, weave::Event{}});
  const int id = slot->id;
  g_events[id] = std::move(slot);
  return id;
}

void event_registry_set(int ev_id) {
  const auto slot = event_lookup(ev_id);
  if (!slot)
    throw std::invalid_argument("shuttle: 未知或已销毁的 event id " + std::to_string(ev_id));
  slot->ev.set();
}

void event_registry_destroy(int ev_id) { g_events.erase(ev_id); }

}  // namespace shuttle::detail
