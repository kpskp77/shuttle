#pragma once

// broker 的共享数据模型（不安装）。
//
// 本头只描述帧、订阅者、topic 与发布状态，不包含 Broker 的操作，也不依赖任何后端。
// broker 核心与 svdpi 外部门面通过 broker/broker.hpp 共享这些类型。

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <shuttle/detail/abi.hpp>
#include <shuttle/exception.hpp>
#include <shuttle/publication.hpp>
#include <string>
#include <vector>
#include <weave/event.hpp>

namespace shuttle::detail {

// ── 消息信封：一次发布的不可变快照 ─────────────────────────────────────────
struct Envelope {
  std::shared_ptr<const void> payload;  // 类型化：T；原始字节流：std::vector<std::byte>
  const TypeOps* ops;                   // 原始字节流时为 &raw_ops
  mutable std::shared_ptr<const std::vector<std::byte>> encoded;  // 跨语言编码缓存
  Meta meta;
};

// 原始字节流（外部发布且 topic 未绑定 C++ 类型）的操作表：
// encode 为恒等拷贝，decode 缺失（无法物化为 C++ 类型）。
//
// inline：`&raw_ops` 是“这帧是不是纯字节流”的唯一判据，所有 TU 必须看到同一个实例。
inline const TypeOps raw_ops{
    "", [](const void* p) { return *static_cast<const std::vector<std::byte>*>(p); }, nullptr};

// 取出信封的字节流表示（跨语言投递与字节订阅使用），结果按信封缓存。
inline const std::vector<std::byte>& envelope_bytes(const Envelope& env) {
  if (!env.encoded) {
    if (!env.ops || !env.ops->encode)
      throw PublishError(std::string("shuttle: topic ") + env.meta.topic +
                         " 的类型不支持编码，无法投递到 SV/字节订阅者");
    env.encoded =
        std::make_shared<const std::vector<std::byte>>(env.ops->encode(env.payload.get()));
  }
  return *env.encoded;
}

// ── 订阅者 ────────────────────────────────────────────────────────────────
// 一次注册只有一条入口：cap 由注册时调用的函数决定（AGENTS.md 不变量 4）。
enum class SubKind { typed, bytes, external };

struct Subscriber {
  int id = 0;
  std::string pattern;  // 订阅模式（可含通配符）
  SubKind kind = SubKind::typed;
  Capability cap = Capability::blocking;
  std::string filter_tag;  // 类型过滤（"" = 任意）；typed 订阅为 T 的标签
  // kind == typed：该订阅类型的操作表（用于按订阅者类型绑定尚未绑定类型的 topic）
  const TypeOps* ops = nullptr;
  // kind == typed：类型擦除回调；kind == bytes：字节回调
  ErasedCallback cb;
  ByteCallback cb_bytes;
  // kind == external：后端侧的不透明句柄（svdpi = SV 注册表编号），dispatch 时原样带回
  int handle = 0;
  // 快照仍持有 shared_ptr，因此退订后对象可能存活；removed 决定在途帧是否还进入回调。
  bool removed = false;
  // 三种“没有成功投递”分开记账：退订是订阅者的决定，skip 是发布者策略，failed 是失败。
  std::uint64_t received = 0;
  std::uint64_t retired = 0;
  std::uint64_t skipped = 0;
  std::uint64_t failed = 0;
  [[nodiscard]] std::uint64_t dropped() const { return retired + skipped + failed; }
};

// 单次投递的成功终态；失败通过异常交给投递单元统一记账。
enum class Delivery { delivered, retired };

// ── Topic ─────────────────────────────────────────────────────────────────
struct Binding {
  std::string tag;     // 类型标签（raw 绑定 = SV type_tag）
  const TypeOps* ops;  // raw 绑定时指向 raw_ops
};

struct Topic {
  // 未绑定 = 只能发纯字节流；optional 使“已绑定但没有操作表”不可表达。
  std::optional<Binding> binding;
  bool retained_on = false;
  std::shared_ptr<const Envelope> retained_env;
  std::uint64_t seq = 0;
  std::uint64_t published = 0;
  std::uint64_t delivered = 0;
  std::uint64_t retired = 0;
  std::uint64_t skipped = 0;
  std::uint64_t failed = 0;
  std::uint64_t deferred = 0;
  [[nodiscard]] std::uint64_t dropped() const { return retired + skipped + failed; }
  weave::Event chain;  // publish_serial 与 publish_now(defer) 共用的后台串行链尾
  Topic() { chain.set(); }
};

// ── Publication 状态 ──────────────────────────────────────────────────────
struct PubState {
  int id = 0;
  // 所有完成计数都以目标订阅者为单位，而不是以投递单元为单位。
  std::size_t expected = 0;
  std::size_t finished = 0;
  std::size_t skipped = 0;
  std::size_t deferred = 0;
  std::size_t retired = 0;
  bool done_flag = false;
  weave::Event done_ev;
  std::vector<Publication::Failure> failures;
  std::string first_error;  // SV 侧 shuttle_c_pub_error 数据源
  // 完成瞬间回调（SV 侧 shuttle_c_wait_pub 用它唤醒被挂起的 SV 进程）
  std::vector<std::function<void()>> on_complete;
};

}  // namespace shuttle::detail
