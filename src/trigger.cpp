// Trigger 工厂实现（声明见 trigger.hpp）。
//
// after_any 在调用点（须为 weave task / 仿真进程上下文）为每个子触发器
// 创建一个 weave task 竞速；after_all 按序等待（等价于"全部满足"）。

#include <memory>
#include <utility>
#include <vector>

#include <shuttle/trigger.hpp>
#include <weave/event.hpp>
#include <weave/task.hpp>
#include <weave/time.hpp>

#include "internal/trigger_internal.hpp"

namespace shuttle {

Trigger after(weave::Event& ev) {
  // std::function 按 const 目标实例化：经 shared_ptr 转发非 const 的 wait()
  auto evp = std::make_shared<weave::Event>(ev);
  return Trigger([evp]() { evp->wait(); });
}

Trigger after(Event& ev) {
  auto evp = std::make_shared<Event>(ev);
  return Trigger([evp]() { evp->wait(); });
}

Trigger after(weave::duration d) {
  return Trigger([d]() { weave::sleep(d); });
}

Trigger after(const weave::Task& t) {
  // 只表达"到达终态"这一事实：FAILED/CANCELLED 均不重抛（语义见头文件注释）
  // 注：std::function 的调用路径按 const 目标实例化（libstdc++ 实现约束），
  // 而 await() 非 const——故经 shared_ptr 转发捕获。
  auto tp = std::make_shared<weave::Task>(t);
  return Trigger([tp]() {
    try {
      tp->await();
    } catch (...) {
    }
  });
}

Trigger after_all(std::vector<Trigger> trigs) {
  return Trigger([trigs = std::move(trigs)]() {
    for (const auto& tr : trigs) tr.wait();  // 按序等待；任一失败（非取消）向外传播
  });
}

Trigger after_any(std::vector<Trigger> trigs) {
  if (trigs.empty()) return Trigger();  // 空集：按立即满足处理
  if (trigs.size() == 1) return trigs.front();
  struct State {
    weave::Event fired;  // 首个子触发器完成即 set（level 语义天然幂等）
  };
  auto st = std::make_shared<State>();
  for (auto& tr : trigs) {
    weave::create_task("shuttle-trig-any", [st, tr]() mutable {
      try {
        tr.wait();
      } catch (...) {
        // 竞速语义：取消/失败也算"该子触发器已落定"，不影响其它子触发器
      }
      st->fired.set();
    });
  }
  return detail::TriggerAccess::from_fn([st]() { st->fired.wait(); });
}

}  // namespace shuttle
