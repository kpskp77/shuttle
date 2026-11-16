// 延迟触发（Trigger）示范。
//
// 三种发布方式都在调用瞬间完成消息拷贝并锁定订阅者集合（快照语义）；
// Trigger 只决定 `serial`/`concurrent` 的后台回调进程何时启动：
//   after(weave::Event&)    —— 事件 set 时启动
//   after(weave::duration)  —— 仿真延时后启动
//   after(weave::Task&)     —— 指定任务终态（含失败/取消）后启动
//   after_all / after_any   —— 组合多个触发器
// 等待完成（wait/result）与触发器正交：句柄在回调全部结束后才 done。

#include <cstdint>
#include <cstdio>
#include <stdexcept>

#include <shuttle/shuttle.hpp>
#include <weave/test.hpp>
#include <weave/weave.hpp>

using namespace weave::literals;

struct Alarm {
  std::uint32_t code;
};
SHUTTLE_REFLECT(Alarm, code)

WEAVE_TEST(trigger_demo) {
  shuttle::Publisher<Alarm> pub("soc.alarm");

  int received = 0;
  auto sub = shuttle::subscribe<Alarm>(
      "soc.alarm", [&](const Alarm& a, const shuttle::Meta&) {
        std::printf("[alarm] code=%u\n", a.code);
        ++received;
      });

  // ── 仿真延时触发：100ns 后才启动回调进程 ──────────────────────────────
  auto h1 = pub.publish_serial(Alarm{1}, shuttle::after(100_ns));

  // ── 事件触发：weave::Event set 时启动（SV 侧对应 TRIG_EVENT）─────────
  weave::Event ev;
  auto h2 = pub.publish_concurrent(Alarm{2}, shuttle::after(ev));
  if (h2.done()) throw std::logic_error("trigger_demo: 事件未触发前不应完成");
  ev.set();

  // ── 跟随任务：worker 到达终态后启动（成功/失败/取消均视为完成）────────
  auto worker = weave::create_task("worker", [] { weave::sleep(50_ns); });
  auto h3 = pub.publish_serial(Alarm{3}, shuttle::after(worker));

  // ── 组合触发：两个事件都 set 后才启动 ─────────────────────────────────
  weave::Event a, b;
  auto h4 = pub.publish_concurrent(Alarm{4},
                         shuttle::after_all({shuttle::after(a), shuttle::after(b)}));
  a.set();
  b.set();

  // 统一收尾：result() = wait + 重抛第一个失败；failures() 取全部明细。
  h1.result();
  h2.result();
  h3.result();
  h4.result();
  if (received != 4) throw std::logic_error("trigger_demo: 回调次数不符");
}
