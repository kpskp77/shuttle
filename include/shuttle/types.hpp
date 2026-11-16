#pragma once

// 公共基础类型：回调能力、溢出策略、`publish_now` 策略、订阅选项、回调签名包装。

#include <cstddef>
#include <cstdint>
#include <functional>
#include <utility>
#include <vector>

#include <shuttle/meta.hpp>

namespace shuttle {

// ── 回调入口 ─────────────────────────────────────────────────────────────
// 订阅者声明自己提供哪些**回调入口**：
//   blocking    —— blocking 入口（SV 侧 export task）：回调可以消耗仿真时间，
//                  因此只能在后台调度里被调用
//   nonblocking —— nonblocking 入口（SV 侧 export function）：回调保证零阻塞，
//                  因此可以在发布者栈内被同步调用
// 这两个词描述的是"回调能否消耗仿真时间"，不是"入口会不会阻塞调用者"
// （`publish_now` 根本不调用 blocking 入口）。
// 规则：发布方式只决定"调度"（不调度 / 串行 / 并发），入口由注册时调用的函数决定：
//   publish_now 只调 nonblocking 入口（入口为 blocking 的目标按 NowPolicy 处置）；
//   publish_serial/concurrent 调该次注册的那一条入口（blocking 或 nonblocking）——
//   不存在"优先哪条"，一条注册只有一条入口，要两条入口就注册两次
//   （SV 侧对同一对象分别调 shuttle_subscribe 与 shuttle_subscribe_nb）。
enum class Capability {
  blocking,
  nonblocking,
};

// 定义 publish_now 遇到入口为 blocking 的订阅者时怎么办（这类订阅者无法在发布者的
// 调用栈内被调用）。策略只作用于这些目标，与同一 topic 上是否还有 nonblocking 订阅者无关。
enum class NowPolicy {
  reject = 0,  // 抛 CapabilityError：本帧从不发布（类型绑定 / seq / retained 都不动）
  skip = 1,    // 跳过这些订阅者：不进 expected，只进 Publication::skipped() 与该订阅者 / topic
               //            的 skipped 计数（帧本身照发，其他订阅者照收——不是"丢弃消息"）
  defer = 2,   // 默认：交给本 topic 的后台串行链（与 publish_serial 共用同一条链），
               //       返回可等待句柄；于是"同一 topic 的后台投递顺序 = 发布调用顺序"
};

// ── nb() 标记：声明一个 C++ 回调为不可阻塞 ───────────────────────────────
// 契约只由用户保证：C++ 无法在编译期强制"不调用阻塞原语"，broker 也不做运行时检查。
// 若回调里出现挂起操作，它会在 `publish_now` 的同步段里挂起发布者本身。
template <typename F>
struct NonBlocking {
  F fn;
};
template <typename F>
NonBlocking<std::decay_t<F>> nb(F&& f) {
  return {std::forward<F>(f)};
}

// ── 订阅选项（当前仅预留，未生效）────────────────────────────────────────
// broker 不维护每订阅者队列：投递在调度单元里同步执行，所以下面两项目前被忽略
// （见 docs/architecture.md 的"已知限制"）。
enum class OverflowPolicy {
  block,        // 预留：积压超限时阻塞投递方
  drop_oldest,  // 预留：积压超限时丢弃最旧消息并计数
};

struct SubOptions {
  std::size_t max_pending = 0;  // 预留：每订阅者待处理消息上限；0 = 无界
  OverflowPolicy overflow = OverflowPolicy::block;  // 预留
};

// ── 回调签名 ─────────────────────────────────────────────────────────────
// 类型化回调：void(const T&, const Meta&) —— 由 subscribe<T> 模板包装
// 原始字节回调（调试/透传/未注册类型）：
using ByteCallback = std::function<void(const std::vector<std::byte>&, const Meta&)>;

}  // namespace shuttle
