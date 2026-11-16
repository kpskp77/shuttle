#pragma once

// Trigger：`serial`/`concurrent` 发布的延迟触发条件 —— "发布后等到条件满足再启动回调进程"。
//
// 语义要点：
//   · 空触发器（默认构造）= 立即启动；
//   · publish_serial / publish_concurrent 在调用瞬间即锁定订阅者集合与消息拷贝（快照语义），
//     触发器只决定后台回调进程"何时开始"，不影响"给谁发、发什么"；
//   · `publish_concurrent` 下所有订阅者进程共享同一个触发器（等待同一条件）；
//   · Trigger::wait() 是阻塞原语，由 broker 的分发 task 调用，需 weave task 上下文。

#include <functional>
#include <initializer_list>
#include <vector>

#include <weave/event.hpp>
#include <weave/task.hpp>
#include <weave/time.hpp>

namespace shuttle {

class Event;  // 前向声明：after(Event&) 工厂使用

namespace detail {
// 内部构造通道（定义在 src/internal/trigger_internal.hpp，不随包安装）。
// 公开工厂见下方 after() 系列。
struct TriggerAccess;
}  // namespace detail

class Trigger {
public:
  Trigger() = default;  // 空：立即

  // 阻塞至条件满足（空触发器立即返回）。仅 broker 内部分发路径调用。
  void wait() const {
    if (fn_) fn_();
  }
  [[nodiscard]] bool immediate() const { return !fn_; }

private:
  explicit Trigger(std::function<void()> fn) : fn_(std::move(fn)) {}

  std::function<void()> fn_;  // 可拷贝、可被多个分发进程共享等待

  friend Trigger after(weave::Event& ev);
  friend Trigger after(Event& ev);
  friend Trigger after(weave::duration d);
  friend Trigger after(const weave::Task& t);
  friend Trigger after_all(std::vector<Trigger> trigs);
  friend Trigger after_any(std::vector<Trigger> trigs);
  friend struct detail::TriggerAccess;
};

// ── shuttle::Event ────────────────────────────────────────────────────────
// 跨语言可引用的置位信号：set() 唤醒全部当前等待者并保持置位
// （之后的 wait() 立即返回；本类不提供 clear()）。
// C++ 侧直接持有；SV 侧经 shuttle_event（DPI 句柄 id）引用同一个事件，
// id 可传给 shuttle_trigger::after_event() 做延迟触发条件。
class Event {
public:
  Event();
  ~Event();
  Event(const Event&) = default;  // 共享语义：拷贝指向同一底层事件
  Event& operator=(const Event&) = default;
  Event(Event&&) noexcept = default;
  Event& operator=(Event&&) noexcept = default;

  void set();   // 唤醒所有当前等待者（level 语义：置位后新 wait 立即返回）
  void wait();  // 阻塞原语，需 weave task / 仿真进程上下文
  [[nodiscard]] bool is_set() const;
  [[nodiscard]] int id() const;  // 跨语言句柄（SV shuttle_event / TRIG_EVENT 使用）

public:
  // 内部实现类型：broker 的事件注册表需要命名/构造它，用户请勿直接操作。
  struct Impl;

private:
  std::shared_ptr<Impl> impl_;

  friend Trigger after(Event& ev);
};

// 等待一个 weave 事件被 set
Trigger after(weave::Event& ev);
// 等待一个 shuttle::Event 被 set
Trigger after(Event& ev);

// 等待一段仿真延时
Trigger after(weave::duration d);

// 等待一个 weave 任务到达终态（DONE/FAILED/CANCELLED 均视为"完成"，不重抛其异常：
// 触发器只表达"完成"这一事实，失败处理由 Publication::result() 承担）
Trigger after(const weave::Task& t);

// 全部满足 / 任一满足（空集合按立即满足处理）
Trigger after_all(std::vector<Trigger> trigs);
// 注意：`after_any` 在元素 ≥2 时于**构造点**就为每个子触发器起一个竞速 task 并开始等待
// （单元素直接返回它本身），因此必须在 weave task / 仿真进程上下文里调用，
// 不能在 nonblocking 回调里构造。
Trigger after_any(std::vector<Trigger> trigs);
inline Trigger after_all(std::initializer_list<Trigger> trigs) {
  return after_all(std::vector<Trigger>(trigs));
}
inline Trigger after_any(std::initializer_list<Trigger> trigs) {
  return after_any(std::vector<Trigger>(trigs));
}

}  // namespace shuttle
