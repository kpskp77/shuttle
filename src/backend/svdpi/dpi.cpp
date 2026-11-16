// svdpi 后端：SV 侧的 import 面（`shuttle_c_*`，定义见 docs/dpi_protocol.md）。
//
// 这里只做"DPI-C 类型 ↔ C++ 类型"的翻译与错误上报，不持有 broker 状态；
// broker 侧的外部接入门面是同目录 facade.cpp 的 `ext_*` / `pub_*`（声明在
// svdpi_internal.hpp，以 detail::* 调用）。
// `detail::backend` 的实现（dispatch / now_fs / retired 通道）在 bridge.cpp。

#include <svdpi.h>

#include <cstdint>
#include <cstring>
#include <memory>
#include <shuttle/detail/abi.hpp>
#include <shuttle/exception.hpp>
#include <shuttle/trigger.hpp>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>
#include <weave/time.hpp>

#include "backend/backend.hpp"
#include "detail/trigger.hpp"
#include "svdpi_internal.hpp"

// ── weave 导出的 SV 原语（定义在 weave_pkg.sv）────────────────────────────
// shuttle 需要"挂起当前 SV 进程"的能力，而 weave::Event::wait() 只在 weave task
// 上下文中合法（普通 SV native 进程里会抛 "weave: blocking primitive outside task
// context"），因此这里直接使用 weave 的 DPI export：事件 id 由 C++ 侧申请，
// 发布完成瞬间由 C++ 触发它。
// 语义（见 ../weave/sv/weave_pkg.sv 的 weave_sv_wait）：ev_id/tick 超时/abort 三腿
// 竞速，status 0=事件命中 1=超时 2=取消；ev_id<0 表示无事件腿、timeout_ticks<0
// 表示无超时腿、abort_id<0 表示无取消腿。timeout_ticks 的单位是 $time 的 tick。
extern "C" {
int weave_sv_create_event();
void weave_sv_trigger(int ev_id);
void weave_sv_destroy_event(int ev_id);
long long weave_sv_ticks_per_sec();
void weave_sv_wait(int ev_id, long long timeout_ticks, int abort_id, int* status);
}

namespace {

using shuttle::detail::last_error;
using shuttle::detail::set_last_error;
using WeaveScopeGuard = shuttle::detail::backend::SvScopeGuard;

// DPI 常量 → NowPolicy：0 = reject / 1 = skip / 2 = defer（越界走默认 defer）。
// 用显式映射而不是 static_cast：常量表变化时不会静默错位。
shuttle::NowPolicy now_policy_of(int v) {
  switch (v) {
    case 0:
      return shuttle::NowPolicy::reject;
    case 1:
      return shuttle::NowPolicy::skip;
    default:
      return shuttle::NowPolicy::defer;
  }
}

// 从 SV open array 读取字节流（byte payload[] → 连续 8-bit 元素）。
std::vector<std::byte> read_payload(const svOpenArrayHandle h) {
  std::vector<std::byte> out;
  if (!h) return out;
  const int n = svSize(h, 1);
  out.reserve(n > 0 ? static_cast<std::size_t>(n) : 0);
  for (int i = 0; i < n; ++i) {
    // SV byte 是 2-state 8-bit，svdpi.h 以 char 表示其元素类型
    const char* p = static_cast<const char*>(svGetArrElemPtr1(h, i));
    out.push_back(static_cast<std::byte>(static_cast<unsigned char>(*p)));
  }
  return out;
}

// 按协议装配延迟触发条件（trig_kind 见 shuttle_pkg.sv 常量）。
shuttle::Trigger build_trigger(int trig_kind, long long trig_arg) {
  using namespace shuttle;
  switch (trig_kind) {
    case 0:
      return Trigger();
    case 1:
      return after(weave::duration(trig_arg));
    case 2:
      return detail::TriggerAccess::from_fn(
          [ev_id = static_cast<int>(trig_arg)]() { detail::event_wait_id(ev_id); });
    case 3:  // 等待另一次发布完成（TRIG_PUB）；其订阅者失败也视为"到达终态"
      return detail::TriggerAccess::from_fn([pub_id = static_cast<int>(trig_arg)]() {
        int status = 0;
        detail::pub_wait(pub_id, 0, &status);
      });
    default:
      throw std::invalid_argument("shuttle: 未知 trig_kind " + std::to_string(trig_kind));
  }
}

// `serial`/`concurrent` 的公共骨架：帧字节 → 串行 / 并发调度 → pub_id。
// 两种方式的差别只在调哪个 broker 入口，其余（读帧、触发器、错误上报）完全一致。
int publish_frame_bg(const char* topic, const char* type_tag, const svOpenArrayHandle payload,
                     int trig_kind, long long trig_arg, bool concurrent) {
  const WeaveScopeGuard weave_scope(SHUTTLE_WEAVE_SV_PACKAGE);
  try {
    const std::vector<std::byte> frame = read_payload(payload);
    const std::string t = topic ? topic : "";
    const std::string tag = type_tag ? type_tag : "";
    shuttle::Trigger trig = build_trigger(trig_kind, trig_arg);
    const shuttle::Publication pub =
        concurrent ? shuttle::detail::ext_pub_concurrent(t, tag, frame, std::move(trig))
                   : shuttle::detail::ext_pub_serial(t, tag, frame, std::move(trig));
    return shuttle::detail::pub_id(pub);
  } catch (const std::exception& e) {
    set_last_error(e.what());
    return 0;
  } catch (...) {
    set_last_error("shuttle: 未知异常");
    return 0;
  }
}

}  // namespace

extern "C" {

// SV 注册表在 dispatch 期间发现目标已退订（retired）时回调本函数：只置标记、不报错
// （契约：退订后的在途投递静默丢弃，不当成错误），由 dispatch/dispatch_nb 在 export
// 返回后把标记转成一次 retired 记账，避免 C++ 侧把它记成 delivered（幻影成功）。
void shuttle_c_sub_retired(int /*sv_sid*/) {
  shuttle::detail::backend::note_retired_delivery();
}

// `publish_now`：返回本帧 pub_id（>0），失败返回 0（原因见 shuttle_c_last_error）。
// nonblocking 部分在返回前已执行完；被 defer 的订阅者仍在后台串行链上，
// SV 侧可拿着 pub_id 走 shuttle_c_wait_pub 等它们。
int shuttle_c_publish_now(const char* topic, const char* type_tag, const svOpenArrayHandle payload,
                          int now_policy) {
  // 这些 import 可能间接驱动 weave（创建任务/事件/挂起等待），weave 的 export 定义在
  // weave_pkg 里；在入口处把作用域切到 weave_pkg，否则 VCS 会报
  // Error-[DPI-DXFNF] 'weave_sv_*' 不可见（入口作用域是 shuttle_pkg）。
  const WeaveScopeGuard weave_scope(SHUTTLE_WEAVE_SV_PACKAGE);
  try {
    const shuttle::Publication pub =
        shuttle::detail::ext_pub_now(topic ? topic : "", type_tag ? type_tag : "",
                                     read_payload(payload), now_policy_of(now_policy));
    return shuttle::detail::pub_id(pub);
  } catch (const std::exception& e) {
    set_last_error(e.what());
    return 0;
  } catch (...) {
    set_last_error("shuttle: 未知异常");
    return 0;
  }
}

// `publish_serial`：后台单链顺序回调。返回 pub_id（>0），失败返回 0。
int shuttle_c_publish_serial(const char* topic, const char* type_tag,
                             const svOpenArrayHandle payload, int trig_kind, long long trig_arg) {
  return publish_frame_bg(topic, type_tag, payload, trig_kind, trig_arg, false);
}

// `publish_concurrent`：后台每订阅者一任务并发回调。返回 pub_id（>0），失败返回 0。
int shuttle_c_publish_concurrent(const char* topic, const char* type_tag,
                                 const svOpenArrayHandle payload, int trig_kind,
                                 long long trig_arg) {
  return publish_frame_bg(topic, type_tag, payload, trig_kind, trig_arg, true);
}

// DPI task：阻塞等待一次发布完成。
// 挂起点是 weave 导出的 task weave_sv_wait —— 它挂起的是**调用者这个 SV 进程**，
// 因此普通 initial/always 进程也能直接等（不需要处在 weave task 里）。
// timeout_fs<=0 表示无限等待。
void shuttle_c_wait_pub(int pub_id, long long timeout_fs, int* status) {
  int sink = 0;
  if (!status) status = &sink;
  try {
    const int st = shuttle::detail::pub_state(pub_id);
    if (st < 0) {
      set_last_error("shuttle: 未知 pub_id " + std::to_string(pub_id));
      *status = 2;
      return;
    }
    if (st != 0) {  // 已完成：直接给出结论，不进入挂起
      *status = (st == 2) ? 2 : 0;
      return;
    }

    const WeaveScopeGuard weave_scope(SHUTTLE_WEAVE_SV_PACKAGE);
    const int w = weave_sv_create_event();
    if (w < 0) {
      set_last_error("shuttle: 创建 SV 唤醒事件失败");
      *status = 2;
      return;
    }
    // 超时后事件 id 会被销毁，完成钩子必须知道它已失效（weave 的约定：
    // 已销毁的 id 不能再被 trigger）。单活跃进程保证这里无需原子操作。
    struct Slot {
      int ev;
      bool active = true;
    };
    auto slot = std::make_shared<Slot>();
    slot->ev = w;
    shuttle::detail::pub_on_complete(pub_id, [slot] {
      if (!slot->active) return;
      const WeaveScopeGuard weave_scope(SHUTTLE_WEAVE_SV_PACKAGE);
      weave_sv_trigger(slot->ev);
    });

    long long ticks = -1;
    if (timeout_fs > 0) {
      const long long tps = weave_sv_ticks_per_sec();
      const unsigned long long per_sec = static_cast<unsigned long long>(tps > 0 ? tps : 1);
      ticks = static_cast<long long>(
          (static_cast<unsigned __int128>(static_cast<unsigned long long>(timeout_fs)) * per_sec) /
          1000000000000000ull);
    }
    int wst = 0;
    weave_sv_wait(w, ticks, -1, &wst);  // ← 唯一挂起点：挂起当前 SV 进程
    slot->active = false;
    weave_sv_destroy_event(w);

    if (wst == 1) {  // 超时腿胜出
      *status = 1;
      return;
    }
    *status = (shuttle::detail::pub_state(pub_id) == 2) ? 2 : 0;
  } catch (const std::exception& e) {
    set_last_error(e.what());
    *status = 2;
  } catch (...) {
    set_last_error("shuttle: 等待被取消或未知异常");
    *status = 2;
  }
}

int shuttle_c_pub_done(int pub_id) {
  return shuttle::detail::pub_done(pub_id) ? 1 : 0;
}

// 发布计数查询：kind 0=目标数 1=已完成 2=策略跳过 3=deferred 4=退订跳过；未知 pub_id 返回 -1
int shuttle_c_pub_stat(int pub_id, int kind) {
  const long long v = shuttle::detail::pub_stat(pub_id, kind);
  return v > 2147483647LL ? 2147483647 : static_cast<int>(v);
}

const char* shuttle_c_pub_error(int pub_id) {
  static std::string msg;
  msg = shuttle::detail::pub_error(pub_id);
  return msg.c_str();
}

int shuttle_c_subscribe(const char* topic_pattern, int caps, const char* type_tag, int sv_sid) {
  const WeaveScopeGuard weave_scope(SHUTTLE_WEAVE_SV_PACKAGE);
  try {
    return shuttle::detail::ext_sub(topic_pattern ? topic_pattern : "", caps,
                                    type_tag ? type_tag : "", sv_sid);
  } catch (const std::exception& e) {
    set_last_error(e.what());
    return 1;
  } catch (...) {
    set_last_error("shuttle: 未知异常");
    return 1;
  }
}

void shuttle_c_unsubscribe(int sv_sid) {
  const WeaveScopeGuard weave_scope(SHUTTLE_WEAVE_SV_PACKAGE);
  shuttle::detail::ext_unsub(sv_sid);
}

void shuttle_c_set_retained(const char* topic, int retained) {
  try {
    shuttle::detail::topic_set_retained(topic ? topic : "", retained != 0);
  } catch (const std::exception& e) {
    set_last_error(e.what());
  }
}

const char* shuttle_c_last_error() {
  return last_error().c_str();
}

// 当前帧字节流：长度与 8 字节小端分块（SV 在 shuttle_sv_dispatch 体内拉取）。
int shuttle_c_frame_len() {
  return shuttle::detail::backend::frame_len();
}

long long shuttle_c_frame_qword(int index) {
  return static_cast<long long>(shuttle::detail::backend::frame_qword(index));
}

int shuttle_c_event_create() {
  const WeaveScopeGuard weave_scope(SHUTTLE_WEAVE_SV_PACKAGE);
  try {
    return shuttle::detail::event_registry_create();
  } catch (const std::exception& e) {
    set_last_error(e.what());
    return 0;
  }
}

void shuttle_c_event_set(int ev_id) {
  const WeaveScopeGuard weave_scope(SHUTTLE_WEAVE_SV_PACKAGE);
  try {
    shuttle::detail::event_registry_set(ev_id);
  } catch (const std::exception& e) {
    set_last_error(e.what());
  }
}

// shuttle_pkg.sv 的 shuttle_event::close() 走这里（每个事件在 C++ 注册表里占一个 id，
// 不释放就会一直留着；仿真结束可不调）。
void shuttle_c_event_destroy(int ev_id) {
  const WeaveScopeGuard weave_scope(SHUTTLE_WEAVE_SV_PACKAGE);
  try {
    shuttle::detail::event_registry_destroy(ev_id);
  } catch (const std::exception& e) {
    set_last_error(e.what());
  }
}

}  // extern "C"
