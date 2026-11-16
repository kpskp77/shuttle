// Trigger 语义自检：延迟触发（duration / Event / Task / all / any）与等待语义。
//
// Trigger 的语义要点：
//   · 发布瞬间完成快照（订阅者集合 + 消息拷贝），触发器只决定"何时开始回调"；
//   · 条件满足前订阅者一帧都收不到，且 Publication 保持未完成；
//   · Trigger::wait() 在 broker 的分发 task 内执行，故回调里可以阻塞。

#include <cstdint>
#include <stdexcept>
#include <string>

#include <shuttle/shuttle.hpp>
#include <weave/test.hpp>
#include <weave/weave.hpp>

#include "case_runner.hpp"

using namespace weave::literals;
using shuttle_test::expect;
using shuttle_test::expect_eq;
using shuttle_test::report;
using shuttle_test::run_case;

// ── 消息类型（trigger_tests 专用）：SHUTTLE_REFLECT 必须在全局作用域使用；
// 类型名即跨语言 type_tag，各测试文件用互不相同的名字避免 ODR 冲突。
struct TrigMsg {
  std::uint32_t v = 0;
};
SHUTTLE_REFLECT(TrigMsg, v)

WEAVE_TEST(trigger_tests) {
  // case1：after(duration) —— 时间到之前一帧都不投递，到点后投递且可等待完成
  run_case(__func__, "case1", [] {
    shuttle::Publisher<TrigMsg> pub("trig.delay");
    std::size_t frames = 0;
    std::vector<std::uint32_t> seen;
    auto s = shuttle::subscribe<TrigMsg>("trig.delay", [&](const TrigMsg& m, const shuttle::Meta&) {
      ++frames;
      seen.push_back(m.v);
    });

    auto h = pub.publish_serial(TrigMsg{42}, shuttle::after(1_ms));
    expect_eq(static_cast<long long>(h.subscriber_count()), 1, "快照已锁定订阅者");
    expect(!h.wait(200_us), "触发前未完成");
    expect_eq(static_cast<long long>(frames), 0, "触发前不投递");

    h.wait();
    expect_eq(static_cast<long long>(frames), 1, "触发后投递一次");
    expect_eq(static_cast<long long>(seen.at(0)), 42, "消息内容不变（快照）");
  });

  // case2：after(Event&) —— 事件 set 前不投递
  run_case(__func__, "case2", [] {
    shuttle::Publisher<TrigMsg> pub("trig.event");
    std::size_t frames = 0;
    auto s = shuttle::subscribe<TrigMsg>("trig.event", [&](const TrigMsg&, const shuttle::Meta&) {
      ++frames;
    });

    shuttle::Event ev;
    auto h = pub.publish_serial(TrigMsg{1}, shuttle::after(ev));
    expect(!h.wait(200_us), "事件未 set 时未完成");
    expect_eq(static_cast<long long>(frames), 0, "事件未 set 时不投递");

    ev.set();
    h.wait();
    expect_eq(static_cast<long long>(frames), 1, "事件 set 后投递");
  });

  // case3：after(Task) —— 任务完成（而非成功）即放行，任务内抛异常不影响投递
  run_case(__func__, "case3", [] {
    shuttle::Publisher<TrigMsg> pub("trig.task");
    std::size_t frames = 0;
    auto s = shuttle::subscribe<TrigMsg>("trig.task", [&](const TrigMsg&, const shuttle::Meta&) {
      ++frames;
    });

    auto slow = weave::create_task("slow", [] {
      weave::sleep(300_us);
      throw std::runtime_error("任务失败但完成度达成");
    });
    auto h = pub.publish_serial(TrigMsg{1}, shuttle::after(slow));
    expect(!h.wait(50_us), "任务未完成时不投递");
    expect_eq(static_cast<long long>(frames), 0, "任务未完成时零投递");

    h.wait();
    expect_eq(static_cast<long long>(frames), 1, "任务完成即放行（不关心成功与否）");
    bool threw = false;
    try {
      slow.await();  // 任务自身的异常仍由调用者承担
    } catch (const std::runtime_error&) {
      threw = true;
    }
    expect(threw, "after(Task) 不影响任务本身的异常语义");
  });

  // case4：after_all —— 全部条件满足才放行
  run_case(__func__, "case4", [] {
    shuttle::Publisher<TrigMsg> pub("trig.all");
    std::size_t frames = 0;
    auto s = shuttle::subscribe<TrigMsg>("trig.all", [&](const TrigMsg&, const shuttle::Meta&) {
      ++frames;
    });

    shuttle::Event ev;
    auto h = pub.publish_serial(TrigMsg{1}, shuttle::after_all({shuttle::after(200_us),
                                                      shuttle::after(ev)}));
    weave::sleep(400_us);  // 只满足时间腿
    expect_eq(static_cast<long long>(frames), 0, "仅满足部分条件时不投递");
    ev.set();
    h.wait();
    expect_eq(static_cast<long long>(frames), 1, "全部条件满足后投递");
  });

  // case5：after_any —— 任一条件满足即放行
  run_case(__func__, "case5", [] {
    shuttle::Publisher<TrigMsg> pub("trig.any");
    std::size_t frames = 0;
    auto s = shuttle::subscribe<TrigMsg>("trig.any", [&](const TrigMsg&, const shuttle::Meta&) {
      ++frames;
    });

    shuttle::Event ev;  // 永不 set
    auto h = pub.publish_serial(TrigMsg{1}, shuttle::after_any({shuttle::after(ev),
                                                      shuttle::after(200_us)}));
    h.wait();
    expect_eq(static_cast<long long>(frames), 1, "时间腿先满足即放行");
  });

  // case6：`publish_concurrent` 多个订阅者共享同一触发器（同一条件只等一次，一起放行）
  run_case(__func__, "case6", [] {
    shuttle::Publisher<TrigMsg> pub("trig.par");
    std::size_t a = 0, b = 0;
    auto sa = shuttle::subscribe<TrigMsg>("trig.par", [&](const TrigMsg&, const shuttle::Meta&) { ++a; });
    auto sb = shuttle::subscribe<TrigMsg>("trig.par", [&](const TrigMsg&, const shuttle::Meta&) { ++b; });

    shuttle::Event ev;
    auto h = pub.publish_concurrent(TrigMsg{1}, shuttle::after(ev));
    expect_eq(static_cast<long long>(h.subscriber_count()), 2, "两个订阅者都在快照里");
    expect(!h.wait(200_us), "未触发");
    ev.set();
    h.wait();
    expect_eq(static_cast<long long>(a), 1, "订阅者 A 放行");
    expect_eq(static_cast<long long>(b), 1, "订阅者 B 放行");
  });

  // case7：触发期间退订 —— 退订即生效：快照只是"谁被选中"，放行时若已退订就不再进回调。
  // （"退订不影响在途帧"是旧契约，已废弃：SV 侧的 sink 在退订瞬间就被注册表删掉了，
  //  两种语言统一到"退订即生效"，这类投递记 retired。）
  run_case(__func__, "case7", [] {
    shuttle::Publisher<TrigMsg> pub("trig.unsub");
    std::size_t frames = 0;
    shuttle::Publication h;
    {
      auto s = shuttle::subscribe<TrigMsg>("trig.unsub", [&](const TrigMsg&, const shuttle::Meta&) {
        ++frames;
      });
      shuttle::Event ev;
      h = pub.publish_serial(TrigMsg{1}, shuttle::after(ev));
      s.unsubscribe();  // 帧已快照，但目标已退订
      ev.set();
      h.wait();
    }
    expect_eq(static_cast<long long>(frames), 0, "退订后放行的在途帧不再进回调");
    expect_eq(static_cast<long long>(h.retired()), 1, "该目标记 retired（不是 delivered）");
    expect(h.done(), "仍然到达终态");
    pub.publish_serial(TrigMsg{2}).wait();
    expect_eq(static_cast<long long>(frames), 0, "退订后不再有新帧");
  });

  report(__func__);
}
