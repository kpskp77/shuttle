#pragma once

// Publisher<T>：topic 的类型化发布入口，提供三种发布方式：
//
//   auto h = pub.publish_now(msg);                    // `publish_now`：发布者栈内同步回调，全程不挂起
//   auto h = pub.publish_serial(msg);                 // `publish_serial`：后台单链顺序回调，立即返回
//   auto h = pub.publish_concurrent(msg);             // `publish_concurrent`：后台每订阅者一任务并发回调，立即返回
//   auto h = pub.publish_concurrent(msg, after(ev));  // 延迟触发：条件满足后才启动
//   h.wait();                                         // 等待本帧全部订阅者回调结束
//
// 命名规则：方法名只表达**调度**（now / serial / concurrent）；
// 订阅者声明决定**入口**（blocking 入口 / nonblocking 入口）——详见 types.hpp 的 Capability。
//
// 一对多：同一 topic 允许多个 Publisher 实例同时发布，
// 也允许任意多个订阅者（多对多）；每个 Publisher 实例同时只发布一个 topic。
// 多个 Publisher 可绑定不同类型（每个类型各一个实例）——type_tag 由 C++ 侧
// SHUTTLE_REFLECT 决定，不按 this 绑定。
//
// 快照语义：三种方式都在调用瞬间完成消息拷贝并锁定订阅者集合，
// 之后的 subscribe/unsubscribe 不影响本次发布。
//
// 顺序保证：
//   · `publish_now` 严格 FIFO（发布者栈内顺序执行）；
//   · `publish_serial` 的每次发布都经 per-topic 串行链排队：同一 topic 的多次发布
//     （不论来自哪个 Publisher 实例）按发布调用顺序回调；
//   · `publish_now` 的 defer 部分也入同一条链，故"同一 topic 的后台投递顺序 = 发布调用顺序"；
//   · `publish_concurrent` 不承诺任何顺序（包括同一订阅者收到连续两帧的回调顺序），
//     需逐帧处理的订阅者应自行过一道 weave::Queue 或改用 `publish_serial`。
//
// 事件源：Meta.seq 为 broker 在 topic 内全局单调递增（跨 Publisher 实例连续），
// 订阅者可据此区分多个发布者的帧；发布者身份不做强制区分（消息内自行携带）。

#include <memory>
#include <string>
#include <utility>

#include <shuttle/detail/abi.hpp>
#include <shuttle/publication.hpp>
#include <shuttle/trigger.hpp>
#include <shuttle/types.hpp>

namespace shuttle {

template <typename T>
class Publisher {
public:
  // 构造时完成 topic 的类型绑定（与既有绑定冲突抛 TypeMismatchError）。
  explicit Publisher(std::string topic) : topic_(std::move(topic)) {
    detail::topic_declare(topic_, detail::type_ops<T>());
  }

  // publish_now：在发布者栈内同步调用入口为 nonblocking 的订阅者，返回本帧句柄。
  // 入口为 blocking 的订阅者不在栈内调用，按 policy 处置：defer（默认）交给本 topic 的后台
  // 串行链稍后调用（句柄的 done() 覆盖这部分）；drop 跳过它们（计入 excluded() 与 dropped）；
  // reject 抛 CapabilityError，且该帧从未发布过——类型绑定、seq 与 retained 快照都不推进。
  // 两拨目标按"该次注册的那一条入口"划分，互不干涉。同步段若回调抛异常：该目标计 failed，
  // 其余目标照常尝试，defer 段照常入链，最后才把第一个异常抛给调用者（句柄不返回）。
  Publication publish_now(const T& msg, NowPolicy policy = NowPolicy::defer) const {
    return detail::pub_now(topic_, std::make_shared<const T>(msg), detail::type_ops<T>(),
                              policy);
  }

  // publish_serial：后台单链顺序回调（topic 级 FIFO）。trig 非空时条件满足后才启动。
  Publication publish_serial(const T& msg, Trigger trig = {}) const {
    return detail::pub_serial(topic_, std::make_shared<const T>(msg), detail::type_ops<T>(),
                              std::move(trig));
  }

  // publish_concurrent：后台每订阅者一任务并发回调。trig 非空时条件满足后才启动。
  Publication publish_concurrent(const T& msg, Trigger trig = {}) const {
    return detail::pub_concurrent(topic_, std::make_shared<const T>(msg), detail::type_ops<T>(),
                                  std::move(trig));
  }

  // retained（latched）：开启后 broker 缓存最新一帧，新订阅者注册时补发
  // （SV 侧 blocking 入口经后台仿真进程异步补发，其余在注册调用内完成）
  void set_retained(bool r) const { detail::topic_set_retained(topic_, r); }
  [[nodiscard]] bool retained() const { return detail::topic_retained(topic_); }

  [[nodiscard]] const std::string& topic() const noexcept { return topic_; }

private:
  std::string topic_;
};

}  // namespace shuttle
