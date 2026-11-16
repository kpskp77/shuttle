#pragma once

// Publication：一次发布的完成句柄（对齐 asyncio Future / weave Task 的查询语义）。
//
// 默认发布不阻塞发布者（publish_serial/publish_concurrent 立即返回）；
// 需要等待时通过本句柄：
//   pub.wait();             // 阻塞至全部订阅者回调结束
//   pub.wait(1_us);         // 带超时
//   pub.result();           // 等待 + 重抛第一个失败的订阅者异常（对齐 weave TaskGroup 语义）
//
// ── 目标集：名字 ∩ 类型 ─────────────────────────────────────────────────────
// 一个订阅者只有在**名字匹配且类型标签匹配**时才是本帧的目标。类型不相交的订阅者连目标都
// 不是：不进 subscriber_count()、不建投递单元、不产生任何计数。因此“通配符订阅者只收自己
// 声明的类型”是目标集的判定结果，而不是逐帧过滤的结果。
//
// ── 记账语义：每个目标订阅者只有五种结局，三种方式完全一致 ─────────────────────
//   delivered  回调执行完毕（SV 侧 = export 返回）
//   retired    快照之后、回调之前被退订：目标已到终态，内容没进回调，但没有任何人出错；
//              计入 retired() 与 dropped()。两种语言同一口径（退订即生效）
//   failed     尝试了但没成功：回调抛异常 / 帧不可物化 / 投递被取消 / 触发条件失败
//              （同时计入 dropped，并留下一条 Failure）
//   deferred   返回时尚未开始（仅 publish_now 的 defer 段，后台串行链接走）
//   excluded   策略干脆不投（仅 publish_now 的 drop），不算目标、也不入 subscriber_count()
// reject 不产生帧，故没有终态。不变量（三种方式都成立）：
//   · finished() ≤ subscriber_count()，且 done() ≡ finished() == subscriber_count()
//   · subscriber_count() + excluded() == 本帧的目标订阅者数（名字 ∩ 类型，且未被策略排除）
//   · finished() == delivered + retired + failed 的累计，deferred 完成后必落入前三者之一
//
// 异常语义：单个订阅者回调抛异常不影响其余订阅者；失败聚合粒度因方式而异：`concurrent` 逐订阅者
// 全收；`serial` 只保留第一条（其余只计入 dropped）；`now` 的同步段把每个失败都记入 failures，
// 但**先把所有目标尝试完、并把 defer 段交给后台链**，最后才把第一个异常抛给发布者（句柄仍不返回，
// 但本帧的其余目标照常投递），defer 段按 `serial` 的规则并入同一句柄。

#include <cstdint>
#include <exception>
#include <memory>
#include <string>
#include <vector>

#include <weave/time.hpp>

namespace shuttle {

namespace detail {
class Broker;
}  // namespace detail

class Publication {
public:
  // 单个订阅者的失败记录
  struct Failure {
    std::string subscriber;    // 订阅者描述：C++ 侧 "cpp sub <id>"、SV 侧 "sv sid <id>"；
                               // 触发器等待失败或投递被取消时是 "<trigger>" / "<delivery>"
    std::exception_ptr error;  // 回调抛出的异常
  };

  Publication() = default;

  [[nodiscard]] bool valid() const { return static_cast<bool>(impl_); }

  // 阻塞至全部订阅者回调完成；需 weave task 上下文（阻塞原语）。
  void wait();
  // 带超时的等待：true = 完成，false = 超时。
  bool wait(weave::duration timeout);

  [[nodiscard]] bool done() const;  // 全部订阅者回调已结束（含失败）

  // wait() + 若存在失败则重抛第一个失败（完成顺序）的异常
  void result();

  // 本次发布的目标订阅者数量（发布瞬间的快照，含被 defer 的部分，不含被 drop 排除的部分）
  [[nodiscard]] std::size_t subscriber_count() const;
  // 已到达终态（delivered / retired / failed）的订阅者数量
  // 完成态恒有 done() ≡ finished() == subscriber_count()（零目标的三种方式都立即完成）
  [[nodiscard]] std::size_t finished() const;

  // ── 可见性：任何"订阅者没收到本帧"都必须是数字（三种方式同一套术语）──
  // 被策略排除、本帧不会投递给它们的订阅者数量：只有 `publish_now` 的 `drop` 会非零，
  // `serial`/`concurrent` 恒为 0（`reject` 直接抛异常、不返回句柄，且该帧从未发布过）
  [[nodiscard]] std::size_t excluded() const;
  // 被推迟到后台串行链、稍后才投递的订阅者数量（publish_now 的 defer）；是标记，完成后不回零
  [[nodiscard]] std::size_t deferred() const;
  // 本帧快照之后被退订、因而未进入回调的订阅者数量（已计入 finished 与 dropped）。
  // 快照保证"谁被选中"，不保证"一定被调用"：退订即生效，不分语言。
  // 这是终端计数，不是错误明细，不进 failures()
  [[nodiscard]] std::size_t retired() const;

  [[nodiscard]] std::vector<Failure> failures() const;

private:
  struct Impl;
  explicit Publication(std::shared_ptr<Impl> impl) : impl_(std::move(impl)) {}

  std::shared_ptr<Impl> impl_;

  // broker 分发路径构造句柄（Broker 是 src/ 下的实现细节，这里只前向声明）
  friend struct detail_pub_access;
  friend class detail::Broker;
};

}  // namespace shuttle
