// 基础示范：消息定义、两种回调能力订阅、三种发布方式、retained、通配符与内省。
//
// 本文件只使用 shuttle/weave 的公开头文件，以"用户链接 libshuttle"的方式构建：
//   · systemc 后端：编入 shuttle_examples_sc 可执行文件本地运行；
//   · svdpi 后端：  编入 libshuttle_examples_svdpi.so，随仿真器经 -sv_lib 加载，
//                   由 weave_pkg::weave_run 统一启动（见 examples/sv/shuttle_demo.sv）。
// SV 侧对等用法见 examples/sv/shuttle_demo.sv。

#include <cstdint>
#include <cstdio>
#include <stdexcept>
#include <string>
#include <vector>

#include <shuttle/shuttle.hpp>
#include <weave/test.hpp>
#include <weave/weave.hpp>

using namespace weave::literals;

// ── 消息类型 ───────────────────────────────────────────────────────────────
// 用户自定义 struct，成员类型须属于编码子集（见 include/shuttle/codec.hpp）。
// SHUTTLE_REFLECT 生成序列化支持（跨语言投递所需）与类型标签 "UartTx"；
// SV 侧须定义字段序同名、type_tag 一致的消息类（见 examples/sv/shuttle_demo.sv）。
struct UartTx {
  std::uint32_t addr;
  std::uint32_t data;
};
SHUTTLE_REFLECT(UartTx, addr, data)

WEAVE_TEST(basic_demo) {
  // ── 订阅：两种回调能力 ─────────────────────────────────────────────────
  // 阻塞能力（`serial`/`concurrent` 投递）：回调在 broker 的后台仿真进程中执行，可消耗仿真时间。
  auto async_mon = shuttle::subscribe<UartTx>(
      "uart0.tx", [](const UartTx& tx, const shuttle::Meta& m) {
        std::printf("[async] seq=%llu addr=0x%08x data=0x%08x\n",
                    static_cast<unsigned long long>(m.seq), tx.addr, tx.data);
        weave::sleep(10_ns);  // 阻塞能力回调：允许等待
      });

  // 不可阻塞能力（`publish_now` 投递）：回调在发布者进程内同步执行，契约上不得阻塞
  // （nb() 是能力声明，编译期无法强制，见 types.hpp 的说明）。
  UartTx last{};
  auto sync_mon = shuttle::subscribe<UartTx>(
      "uart0.tx",
      shuttle::nb([&](const UartTx& tx, const shuttle::Meta&) { last = tx; }));

  // 构造 Publisher 时完成 topic 的类型绑定；绑定冲突抛 TypeMismatchError。
  shuttle::Publisher<UartTx> pub("uart0.tx");

  // ── `publish_now`：同步回调，发布者全程不挂起 ─────────────────────────────────
  // 投递给 sync_mon；async_mon 的注册入口是 blocking，按 NowPolicy 处置：
  // 默认 defer（转后台串行链），此处演示 skip（跳过并计诊断）。
  pub.publish_now(UartTx{0x10, 0xA5}, shuttle::NowPolicy::skip);
  if (last.addr != 0x10 || last.data != 0xA5)
    throw std::logic_error("basic_demo: publish_now 未同步投递");

  // ── `publish_serial`：后台单链顺序回调 ───────────────────────────────────────────
  // 调用瞬间锁定订阅者集合与消息快照，立即返回；同一 topic 保 FIFO。
  auto h2 = pub.publish_serial(UartTx{0x11, 0x5A});
  h2.result();  // 等待全部订阅者回调结束；若有订阅者抛异常，重抛第一个失败

  // ── `publish_concurrent`：后台每订阅者一进程并发回调 ────────────────────────────────
  // 不承诺任何回调顺序；完成语义与 `publish_serial` 一致。
  auto h3 = pub.publish_concurrent(UartTx{0x12, 0x3C});
  h3.wait();
  if (!h3.done() || h3.subscriber_count() == 0)
    throw std::logic_error("basic_demo: publish_concurrent 完成状态异常");
  for (const auto& f : h3.failures())  // 逐个检查失败明细（本例应为空）
    std::printf("[warn] 订阅者 %s 失败\n", f.subscriber.c_str());

  // ── retained（latched）：新订阅者立即收到最新一帧 ─────────────────────
  pub.set_retained(true);
  pub.publish_now(UartTx{0x13, 0xC3}, shuttle::NowPolicy::skip);
  bool got_retained = false;
  auto late_joiner = shuttle::subscribe<UartTx>(
      "uart0.tx",
      shuttle::nb([&](const UartTx& tx, const shuttle::Meta&) {
        got_retained = (tx.addr == 0x13);  // 订阅瞬间同步收到 retained 帧
      }));
  if (!got_retained) throw std::logic_error("basic_demo: retained 帧未投递");

  // ── 通配符 + 原始字节订阅 ─────────────────────────────────────────────
  // "uart0.>" 匹配其后任意多级；字节订阅用于调试/透传或消费 SV 发布的消息。
  auto raw_tap = shuttle::subscribe_bytes(
      "uart0.>", [](const std::vector<std::byte>& bytes, const shuttle::Meta& m) {
        std::printf("[raw] topic=%s payload=%zu 字节\n", m.topic.c_str(), bytes.size());
      });
  pub.publish_now(UartTx{0x14, 0x44}, shuttle::NowPolicy::skip);

  // ── 内省：只读快照，不影响投递路径 ────────────────────────────────────
  if (auto info = shuttle::broker::topic_info("uart0.tx")) {
    std::printf("[broker] topic=%s type=%s subs=%zu published=%llu delivered=%llu\n",
                info->name.c_str(), info->type_tag.c_str(), info->subscriber_count,
                static_cast<unsigned long long>(info->published),
                static_cast<unsigned long long>(info->delivered));
    if (info->type_tag != "UartTx")
      throw std::logic_error("basic_demo: 类型标签与 SHUTTLE_REFLECT 不一致");
  }

  // ── 一对多：同一 topic 的多个 Publisher 实例发布到同一组订阅者 ─────────
  // 类型相同（各自的类型标签一致）时多个 Publisher 合法且常见：
  // 多个驱动组件各自实例化 Publisher 发布到同一 topic；
  // 每个订阅者独立收到每个 Publisher 的每个发布帧。
  shuttle::Publisher<UartTx> other_pub("uart0.tx");
  other_pub.publish_now(UartTx{0x15, 0x55}, shuttle::NowPolicy::skip);
  if (last.addr != 0x15) throw std::logic_error("basic_demo: 多发布者未投递");

  // ── 边界：`publish_now` 遇入口为 blocking 的订阅者 → CapabilityError；等待超时 ──────────────
  // 用一个独立 topic 隔离，避免延迟帧影响上文 topic 的 FIFO 观察。
  {
    auto blk_sub = shuttle::subscribe<UartTx>("uart1.edge", [](const UartTx&,
                                                                  const shuttle::Meta&) {
      // 入口为 blocking 的订阅者：不能接收 publish_now 的同步回调
    });
    shuttle::Publisher<UartTx> ep("uart1.edge");
    bool threw = false;
    try {
      ep.publish_now(UartTx{0x20, 0xE0}, shuttle::NowPolicy::reject);
    } catch (const shuttle::CapabilityError&) {
      threw = true;
    }
    if (!threw) throw std::logic_error("basic_demo: publish_now 未按 reject 策略报错");
    // 超时：延迟 1us 触发，只等 1ns → 尚未完成；再无限等待直至真正完成
    auto hp = ep.publish_serial(UartTx{0x21, 0xE1}, shuttle::after(1_us));
    if (hp.wait(1_ns)) throw std::logic_error("basic_demo: 等待超时判定错误");
    hp.wait();
    if (!hp.done()) throw std::logic_error("basic_demo: 超时后未等到完成");
    std::printf("[edge] CapabilityError 与等待超时行为符合契约\n");
  }

  // ── 退订：句柄共享语义，最后一份析构自动退订；也可显式退订 ─────────────
  async_mon.unsubscribe();
  // sync_mon / late_joiner / raw_tap 随作用域结束自动退订
}
