#pragma once

// broker 内部实现（不安装；只给 `src/` 用）：数据模型 + 单例 + 各发布方式的公共段。
//
// 为什么在头里：broker 核心（`src/broker.cpp`，C++ 类型化路径）与外部接入门面
// （`src/entry.cpp`，字节帧 + 外部订阅者 + 按 id 的查询）必须共享同一份 `Broker`
// 定义与单例。这份头**没有任何后端依赖**——后端只经 `internal/backend.hpp` 出现。

#include <algorithm>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <shuttle/detail/abi.hpp>
#include <shuttle/broker.hpp>
#include <shuttle/exception.hpp>
#include <shuttle/publication.hpp>
#include <shuttle/subscription.hpp>
#include <shuttle/trigger.hpp>
#include <weave/event.hpp>
#include <weave/task.hpp>

#include "internal/backend.hpp"

namespace shuttle::detail {

// ── 消息信封：一次发布的不可变快照 ─────────────────────────────────────────
struct Envelope {
  std::shared_ptr<const void> payload;  // 类型化：T；原始字节流：std::vector<std::byte>
  const detail::TypeOps* ops;           // 原始字节流时为 &raw_ops
  mutable std::shared_ptr<const std::vector<std::byte>> encoded;  // 跨语言编码缓存
  Meta meta;
};

// 原始字节流（外部发布且 topic 未绑定 C++ 类型）的操作表：
// encode 为恒等拷贝，decode 缺失（无法物化为 C++ 类型）。
//
// inline：`&raw_ops` 是"这帧是不是纯字节流"的唯一判据，broker.cpp 与 entry.cpp 必须
// 看到**同一个**实例（非 inline 的 const 对象有内部链接，每个 TU 一份，比较会失效）。
inline const TypeOps raw_ops{"",
                             [](const void* p) {
                               return *static_cast<const std::vector<std::byte>*>(p);
                             },
                             nullptr};

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
// 入口：`cap` 是**该次注册唯一的那条入口**（blocking = 可经 `serial`/`concurrent` 的投递单元调用，nonblocking = 可经
// `now` 在发布者栈内同步调用）。一次注册只有一条入口（见 AGENTS.md 不变量 4），所以 deliver 不存在
// "优先哪条"的余地——不强制时永远用这条，`now` 的同步段另有一个强制 nonblocking 的开关。
enum class SubKind { typed, bytes, external };

struct Subscriber {
  int id = 0;
  std::string pattern;  // 订阅模式（可含通配符）
  SubKind kind = SubKind::typed;
  Capability cap = Capability::blocking;  // 注册时调用的那个函数决定（互斥，只有一条入口）
  std::string filter_tag;  // 类型过滤（"" = 任意）；typed 订阅为 T 的标签
  // kind == typed：该订阅类型的操作表（用于按订阅者类型绑定尚未绑定类型的 topic）
  const detail::TypeOps* ops = nullptr;
  // kind == typed：类型擦除回调；kind == bytes：字节回调
  detail::ErasedCallback cb;
  ByteCallback cb_bytes;
  // kind == external：后端侧的不透明句柄（svdpi = SV 注册表编号），dispatch 时原样带回
  int handle = 0;
  // 已退订（`remove_sub` 置位）。快照持有 shared_ptr，所以对象在退订后仍然活着——本标记
  // 是"这一帧还该不该进回调"的唯一判据：退订即生效，不问投递进程此刻在哪。
  bool removed = false;
  // 诊断计数：三种"没有成功投递"的终态各自记一份，dropped() 是它们的和。
  // 分开的理由是责任归属不同——退订是订阅者自己的决定，skip 是发布者的策略，
  // failed 是有人出错了；合成一个数就只能靠翻 Publication::failures() 猜。
  // 注意 failed 与另两个不同：回调**进去了**（拿到内容）但抛了异常、或帧没物化、
  // 或被取消——所以 dropped 的判据是"没有以成功回调收尾"，不是"没进回调"。
  std::uint64_t received = 0;
  std::uint64_t retired = 0;  // 快照之后、回调之前被退订
  std::uint64_t skipped = 0;  // 发布者策略跳过（NowPolicy::skip）
  std::uint64_t failed = 0;   // 尝试了但没成功：回调抛异常 / 投递被取消 / 单元级失败
  [[nodiscard]] std::uint64_t dropped() const { return retired + skipped + failed; }
};

// ── 单次投递的结果（记账用）───────────────────────────────────────────────
// `delivered` = 回调执行完毕；`retired` = 该订阅者在快照之后、回调之前被退订，目标已到终态
// 但内容没进回调。两个来源（都与语言无关）：C++ 侧的 `Subscriber::removed`、SV 注册表的
// 静默丢弃回告。类型标签的判定不在这里——它发生在
// **目标集快照**（`match_subs`：名字 ∩ 类型）时，不进目标集的订阅者根本没有投递单元。
// 失败不走返回值，而是抛异常，由投递单元记成 failed。
enum class Delivery { delivered, retired };

// ── Topic ─────────────────────────────────────────────────────────────────
// 类型绑定：一旦建立就不再变（标签是身份，ops 决定“能不能物化成 C++ 对象”）
struct Binding {
  std::string tag;             // 类型标签（raw 绑定 = SV type_tag）
  const detail::TypeOps* ops;  // raw 绑定时指向 raw_ops
};

struct Topic {
  // 未绑定 = 只能发纯字节流。三件事是一个东西，所以是一个 optional：
  // std::optional 让“bound 但标签为空”这类不一致从“靠纪律”变成“不可表达”。
  std::optional<Binding> binding;
  bool retained_on = false;
  std::shared_ptr<const Envelope> retained_env;
  std::uint64_t seq = 0;        // topic 内全局单调（跨发布者连续，多对多契约）
  std::uint64_t published = 0;  // 累计发布帧数
  std::uint64_t delivered = 0;  // 累计投递次数（所有订阅者合计）
  std::uint64_t retired = 0;    // 累计未成功投递：快照后被退订
  std::uint64_t skipped = 0;    // 累计未成功投递：发布者策略跳过（NowPolicy::skip）
  std::uint64_t failed = 0;     // 累计未成功投递：尝试了但没成功
  std::uint64_t deferred = 0;   // 累计被 defer 到后台串行链的投递数
  [[nodiscard]] std::uint64_t dropped() const { return retired + skipped + failed; }
  weave::Event chain;           // 后台投递的 per-topic 串行链尾（`publish_serial` 与 `now` 的 defer 共用）
  Topic() { chain.set(); }
};

// ── Publication 状态 ──────────────────────────────────────────────────────
struct PubState {
  int id = 0;
  // 本帧目标数：`serial`/`concurrent` = 匹配到的订阅者数；`now` = nonblocking 目标数
  // +（仅 defer 时）blocking 目标数（`skip` 掉的照算目标、只是不投）
  std::size_t expected = 0;
  std::size_t finished = 0;
  // 可见性计数：本帧没成功投递 / 稍后才投递的订阅者（都是标记，不是余额；
  // 见 docs/architecture.md 的"记账契约"一章）
  std::size_t skipped = 0;   // 发布者策略跳过（未尝试，不是失败）
  std::size_t deferred = 0;
  std::size_t retired = 0;   // 快照后被退订而跳过（未尝试，不是失败）
  bool done_flag = false;
  weave::Event done_ev;
  std::vector<Publication::Failure> failures;
  std::string first_error;  // 第一个失败的 what()（SV 侧 shuttle_c_pub_error 数据源）
  // 完成瞬间回调（SV 侧 shuttle_c_wait_pub 用它唤醒被挂起的 SV 进程）
  std::vector<std::function<void()>> on_complete;
};

// ── broker 单例 ───────────────────────────────────────────────────────────
class Broker {
public:
  std::map<std::string, Topic, std::less<>> topics;  // 透明比较器：string_view 直接查
  std::vector<std::shared_ptr<Subscriber>> subs;
  std::map<int, std::shared_ptr<PubState>> pubs;
  int next_sub_id = 1;
  int next_pub_id = 1;

  static Broker& get() {
    static Broker b;
    return b;
  }

  // 是否含通配符（"*" 或 ">"）
  [[nodiscard]] static bool has_wildcard(std::string_view pattern) {
    return pattern.find_first_of("*>") != std::string_view::npos;
  }

  // s[pos..] 所在段落的段尾（'.' 或串尾）。段边界只有这一处定义。
  static std::size_t seg_end(std::string_view s, std::size_t pos) {
    const std::size_t dot = s.find('.', pos);
    return dot == std::string_view::npos ? s.size() : dot;
  }

  // 通配符匹配："*" 匹配单级，">"（须为末段）匹配其后至少一级（不匹配前缀本身）。
  // 逐段比较、不做任何分配：旧实现在每次调用里 split 出两个 vector<string>
  // （每帧每订阅者 2 次 vector + N 次 substr 分配）。
  [[nodiscard]] static bool match(std::string_view pattern, std::string_view topic) {
    for (std::size_t pi = 0, ti = 0;;) {
      const std::size_t pe = seg_end(pattern, pi);
      const std::size_t te = seg_end(topic, ti);
      const std::string_view ps = pattern.substr(pi, pe - pi);
      // 走到这里就说明 ti 处还有一个（可能为空的）段落 → 至少一级
      if (ps == ">") return true;
      if (ps != "*" && ps != topic.substr(ti, te - ti)) return false;
      if (pe == pattern.size()) return te == topic.size();
      if (te == topic.size()) return false;
      pi = pe + 1;
      ti = te + 1;
    }
  }

  // ── 名字与模式的语法 ────────────────────────────────────────────────────
  // 通配符只属于**订阅模式**。发布、类型声明、retained 作用于具体 topic 名：一旦允许通配符
  // 出现在那里，`topics` 表里就会立起一个"名字里带 *"的幻影 topic——它有自己的 `seq`、
  // 类型绑定、retained 槽；而路由是字符串比对，于是 `pub("a.*")` 会投给 `a.*` / `a.>` / `>`
  // 三种订阅者，看起来像"广播"，其实只是名字撞上了模式（`a0.*` 的订阅者反而收不到）。
  static void check_segments(std::string_view name, const char* what) {
    for (std::size_t pi = 0;;) {
      const std::size_t pe = seg_end(name, pi);
      if (pe == pi)
        throw TopicPatternError(std::string("shuttle: ") + what + " \"" + std::string(name) +
                                "\" 含空段（多余的 '.'）");
      if (pe == name.size()) return;
      pi = pe + 1;
    }
  }

  // 发布侧：`topic` 必须是具体名字
  static void check_topic_name(std::string_view topic) {
    if (topic.empty()) throw TopicPatternError("shuttle: topic 名不能为空");
    if (has_wildcard(topic))
      throw TopicPatternError(std::string("shuttle: topic \"") + std::string(topic) +
                              "\" 含通配符；* 与 > 只属于订阅模式，发布 / 类型声明 / retained "
                              "必须用具体 topic 名");
    check_segments(topic, "topic 名");
  }

  // 订阅侧：每段要么是 "*"，要么是 ">"（必须是末段），要么是普通段（不含 * >）。
  // 不校验的话 `a.>.b` 会静默等价于 `a.>`（`match` 遇到 ">" 直接放行剩余模式），
  // `a*b` 则只能匹配字面量 "a*b"——两种错误都不会有任何提示。
  static void check_pattern(std::string_view pattern) {
    if (pattern.empty()) throw TopicPatternError("shuttle: 订阅模式不能为空");
    check_segments(pattern, "订阅模式");
    const std::string where = std::string("shuttle: 订阅模式 \"") + std::string(pattern) + "\"";
    for (std::size_t pi = 0;;) {
      const std::size_t pe = seg_end(pattern, pi);
      const std::string_view ps = pattern.substr(pi, pe - pi);
      if (ps == ">") {
        if (pe != pattern.size()) throw TopicPatternError(where + " 中 > 必须是末段");
        return;
      }
      if (ps != "*" && has_wildcard(ps))
        throw TopicPatternError(where + " 的段 \"" + std::string(ps) +
                                "\" 把通配符混进了普通段（* 与 > 必须独占一段）");
      if (pe == pattern.size()) return;
      pi = pe + 1;
    }
  }

  // 类型相交：订阅者的类型标签与帧的 type_tag 一致（空标签 = 任意类型）。
  // 这是**目标集**的判据之一：名字匹配但类型不相交的订阅者不是本帧的目标，不建投递单元、
  // 不产生任何计数（`subscriber_count()` 就是“名字 ∩ 类型”的大小）。
  [[nodiscard]] static bool type_matches(const Subscriber& sub, std::string_view tag) {
    return sub.filter_tag.empty() || sub.filter_tag == tag;
  }

  // 快照目标订阅者集合：名字匹配 ∩ 类型匹配
  std::vector<std::shared_ptr<Subscriber>> match_subs(std::string_view topic,
                                                      std::string_view tag) {
    std::vector<std::shared_ptr<Subscriber>> out;
    for (const auto& s : subs)
      if (type_matches(*s, tag) && match(s->pattern, topic)) out.push_back(s);
    return out;
  }

  // 只按名字计数（不判类型）：`broker::topic_info` 的内省口径
  std::size_t name_matched_count(std::string_view topic) const {
    std::size_t n = 0;
    for (const auto& s : subs)
      if (match(s->pattern, topic)) ++n;
    return n;
  }

  // 透明查找：调用方可以只给 string_view，不必先造一个 string
  Topic& topic_for(std::string_view name) {
    const auto it = topics.find(name);
    return it != topics.end() ? it->second : topics.try_emplace(std::string(name)).first->second;
  }

  // 订阅时的类型过滤预校验（仅当订阅模式恰好是具体 topic 且已绑定时可判；
  // 通配符/未绑定 topic 推迟到目标集快照时按帧标签相交）
  void check_filter_early(const Subscriber& sub) {
    if (sub.filter_tag.empty()) return;
    const auto it = topics.find(sub.pattern);
    if (it == topics.end() || !it->second.binding) return;
    const std::string& bound_tag = it->second.binding->tag;
    if (bound_tag != sub.filter_tag)
      throw TypeMismatchError(std::string("shuttle: 订阅 ") + sub.pattern + " 声明类型 " +
                              sub.filter_tag + "，与 topic 绑定 " + bound_tag + " 冲突");
  }

  // 首次绑定 topic 类型时的反向预校验（`check_filter_early` 的镜像）：确认没有"模式恰好是
  // 该 topic + 标签不同"的订阅者已经挂在那里。**通配符订阅者必须跳过**——它们本就横跨
  // 不同类型，只在目标集快照时按帧标签相交。
  // 没有这一步，同一个用户错误（订阅者标签写错）报不报错就取决于注册顺序：
  // topic 先绑定 → 订阅时 `check_filter_early` 抛；订阅先发生 → 该订阅者此后收到的每一帧
  // 都悄无声息地不属于目标集，永远不报错。
  void check_bind_compat(std::string_view topic, std::string_view tag) {
    for (const auto& sub : subs) {
      if (sub->filter_tag.empty() || sub->filter_tag == tag) continue;
      if (has_wildcard(sub->pattern) || !match(sub->pattern, topic)) continue;
      throw TypeMismatchError(std::string("shuttle: topic ") + std::string(topic) + " 绑定类型 " +
                              std::string(tag) + "，但已有订阅者声明 " + sub->filter_tag);
    }
  }

  // `publish_now` 的 reject 预检：在**任何副作用**（类型绑定、seq、retained 快照）之前判定
  // 这一帧能不能发。被拒绝的帧等于从未发布过（`published`/`seq` 都不推进）。
  // 只数**目标集**里的 blocking 订阅者：类型不相交的订阅者本帧根本没打算投给它，
  // 不能因为它存在就把一帧发不出去。
  void now_preflight(std::string_view topic, std::string_view tag, NowPolicy policy) {
    // 语法错误与策略无关：`reject` 的预检若先跑，同一份错误输入会因 policy 不同而换异常类型
    check_topic_name(topic);
    if (policy != NowPolicy::reject) return;
    const auto matched = match_subs(topic, tag);
    const std::size_t n = select(matched, Capability::blocking).size();
    if (n == 0) return;
    throw CapabilityError(std::string("shuttle: publish_now 遇到 ") + std::to_string(n) +
                          " 个入口为 blocking 的订阅者（topic " + std::string(topic) +
                          "）；它们无法在 publish_now 里投递，可用 NowPolicy::defer/skip，"
                          "或改用 publish_serial/publish_concurrent");
  }

  // 投递一帧到单个订阅者（在投递进程的上下文执行）。
  // 入口**只**由该注册的 `cap` 决定（不变量 4）：blocking 注册走 export task，nonblocking
  // 注册走 export function。没有"本次投递"级的入口覆盖——`now` 的同步段之所以不会踩到
  // `Error-[DPI-INCDIC]`，是因为 `select` 已按 `cap` 把 blocking 目标摘走了（§7）。
  // 类型标签不在这里判：调用方只会传**目标集**里的订阅者（`match_subs` 已求过名字 ∩ 类型）。
  Delivery deliver(const std::shared_ptr<Subscriber>& sub, const Envelope& env) {
    const bool use_nb = sub->cap == Capability::nonblocking;
    // 退订即生效（不分语言）：快照只保证"谁被选中"，不保证"一定被调用"。C++ 的快照持有
    // shared_ptr、对象还活着，所以这里必须显式问一次；SV 侧 sink 已被注册表删除，同一件事
    // 在 dispatch 之后由 take_retired_delivery() 报回来。两条来源互补，覆盖全部窗口。
    if (sub->removed) {
      ++sub->retired;
      ++topic_for(env.meta.topic).retired;
      return Delivery::retired;
    }
    switch (sub->kind) {
      case SubKind::typed:
        // 安全网：帧是纯字节流（raw 绑定）时绝不能按 T* 解释它
        if (env.ops == &raw_ops)
          throw PublishError(std::string("shuttle: topic ") + env.meta.topic +
                             " 的帧没有物化为 C++ 对象（类型未绑定）");
        sub->cb(env.payload.get(), env.meta);
        break;
      case SubKind::bytes:
        sub->cb_bytes(envelope_bytes(env), env.meta);
        break;
      case SubKind::external:
        // export task / export function 的选择按订阅者入口（`cap`），与调用方无关：
        if (use_nb)
          detail::backend::dispatch_nb(sub->handle, env.meta.topic, env.meta.type_tag, env.meta.seq,
                                  env.meta.time_fs, envelope_bytes(env));
        else
          detail::backend::dispatch(sub->handle, env.meta.topic, env.meta.type_tag, env.meta.seq,
                               env.meta.time_fs, envelope_bytes(env));
        // SV 注册表报告该 id 已退订：这次在途投递被丢弃。以前这里照样 ++received，
        // 于是在 C++ 侧是“幻影成功”；现在计为 retired（未进入回调，但不是错误）。
        if (detail::backend::take_retired_delivery()) {
          ++sub->retired;
          ++topic_for(env.meta.topic).retired;
          return Delivery::retired;
        }
        break;
    }
    ++sub->received;
    return Delivery::delivered;
  }

  // 记录一个订阅者的失败（不改变完成计数）
  void record_failure(const std::shared_ptr<PubState>& pub,
                      const std::optional<Publication::Failure>& failure) {
    if (!failure) return;
    pub->failures.push_back(*failure);
    if (pub->first_error.empty()) {
      try {
        if (failure->error) pub->first_error = failure_error_what(*failure);
      } catch (...) {
        pub->first_error = "<异常>";
      }
    }
  }

  // 发布完成：置位、唤醒等待者（完成钩子先行，SV 侧 wait_done 依赖它）
  void pub_complete(const std::shared_ptr<PubState>& pub) {
    pub->done_flag = true;
    auto hooks = std::exchange(pub->on_complete, {});
    for (const auto& fn : hooks) {
      try {
        fn();
      } catch (...) {
        // 唤醒钩子失败不能影响投递结果
      }
    }
    pub->done_ev.set();
  }

  // 一个目标订阅者到达终态：delivered / retired / failed 三种都算（failed 的计数由调用方
  // 自己先做，这里只管完成计数）。三种方式都走这一处，`done()` 的契约才唯一。
  void target_finished(const std::shared_ptr<PubState>& pub) {
    if (++pub->finished == pub->expected && !pub->done_flag) pub_complete(pub);
  }

  // 一个投递单元结束（`publish_concurrent`：一个订阅者 = 一个单元）
  void unit_finished(const std::shared_ptr<PubState>& pub,
                     std::optional<Publication::Failure> failure) {
    record_failure(pub, failure);
    target_finished(pub);
  }

  static std::string failure_error_what(const Publication::Failure& f) {
    try {
      if (f.error) std::rethrow_exception(f.error);
    } catch (const std::exception& e) {
      return e.what();
    } catch (...) {
      return "未知异常";
    }
    return "<无异常信息>";
  }

  // 失败对象的统一构造：文案（“投递被取消”/“未知异常”）只在这里出现一次。
  static std::exception_ptr cancelled_exc() {
    return std::make_exception_ptr(std::runtime_error("投递被取消"));
  }
  static std::exception_ptr unknown_exc() {
    return std::make_exception_ptr(std::runtime_error("未知异常"));
  }
  static Publication::Failure failure_of(std::string label, std::exception_ptr e) {
    return Publication::Failure{std::move(label), std::move(e)};
  }

  // 尝试了但没成功：该订阅者与 topic 各记一次 failed（取消与异常同一口径）
  static void count_failed(Topic& t, const std::shared_ptr<Subscriber>& sub) {
    ++t.failed;
    ++sub->failed;
  }

  // 建立类型绑定 + 打序号 + retained 快照（发布公共段）。
  std::shared_ptr<const Envelope> make_envelope(std::string_view topic,
                                                std::shared_ptr<const void> payload,
                                                const detail::TypeOps* ops) {
    check_topic_name(topic);
    Topic& t = topic_for(topic);
    if (!t.binding) {
      check_bind_compat(topic, ops->tag);
      t.binding = Binding{ops->tag, ops};
    } else if (t.binding->tag != ops->tag) {
      throw TypeMismatchError(std::string("shuttle: topic ") + std::string(topic) +
                              " 已绑定类型 " + t.binding->tag + "，本次发布类型 " + ops->tag);
    }
    auto env = std::make_shared<Envelope>();
    env->payload = std::move(payload);
    env->ops = ops;
    env->meta.topic = std::string(topic);
    env->meta.type_tag = t.binding->tag;
    env->meta.seq = ++t.seq;
    env->meta.time_fs = detail::backend::now_fs();
    ++t.published;
    if (t.retained_on) t.retained_env = env;
    return env;
  }

  // 从字节流发布（SV 侧）：raw 绑定持有字节拷贝；typed 绑定物化为 C++ 对象。
  std::shared_ptr<const Envelope> make_envelope_bytes(std::string_view topic,
                                                      std::string_view tag,
                                                      const std::vector<std::byte>& bytes) {
    check_topic_name(topic);
    Topic& t = topic_for(topic);
    if (!t.binding) {
      // 该 topic 没有 C++ 发布者（只有 SV 发布者）：若已有匹配的 C++ 类型化订阅者，
      // 就按订阅者的类型绑定，这样本帧可以物化成 C++ 对象；否则退化为纯字节流。
      check_bind_compat(topic, tag);
      t.binding = Binding{std::string(tag), &raw_ops};
      for (const auto& sub : subs) {
        if (sub->kind != SubKind::typed || !sub->ops || !sub->ops->decode) continue;
        if (!type_matches(*sub, tag) || !match(sub->pattern, topic)) continue;
        t.binding->ops = sub->ops;
        break;
      }
    } else if (t.binding->tag != tag) {
      throw TypeMismatchError(std::string("shuttle: topic ") + std::string(topic) +
                              " 已绑定类型 " + t.binding->tag + "，本次发布类型 " +
                              std::string(tag));
    }
    if (t.binding->ops == &raw_ops) {
      auto payload = std::make_shared<const std::vector<std::byte>>(bytes);
      auto env = std::make_shared<Envelope>();
      env->payload = std::move(payload);
      env->ops = &raw_ops;
      env->meta.topic = std::string(topic);
      env->meta.type_tag = t.binding->tag;
      env->meta.seq = ++t.seq;
      env->meta.time_fs = detail::backend::now_fs();
      ++t.published;
      if (t.retained_on) t.retained_env = env;
      return env;
    }
    if (!t.binding->ops->decode)
      throw TypeMismatchError(std::string("shuttle: topic ") + std::string(topic) +
                              " 绑定的类型不支持从字节流物化");
    return make_envelope(topic, t.binding->ops->decode(bytes), t.binding->ops);
  }

  static void wait_trigger(const Trigger& trig) {
    if (!trig.immediate()) trig.wait();
  }

  // ── `publish_serial`：单投递单元，per-topic 串行链保发布序 FIFO ─────────────────────
  std::shared_ptr<PubState> start_serial(std::string_view topic,
                                      std::shared_ptr<const Envelope> env, Trigger trig,
                                      std::vector<std::shared_ptr<Subscriber>> targets) {
    auto pub = std::make_shared<PubState>();
    pub->id = next_pub_id++;
    // `publish_serial` 只有一个投递单元，但它代表全部目标订阅者：expected/finished 按订阅者计数，
    // 保证 done() ≡ finished() == subscriber_count() 这一契约对三种方式统一成立。
    pub->expected = targets.size();
    pubs[pub->id] = pub;
    if (targets.empty()) {
      // 零目标：立即完成，不建投递单元、不装链尾（与 `publish_concurrent` 一致）。
      // 否则 waiting 者要多等一个调度点，且本帧的 seq 会与出队顺序错位。
      pub_complete(pub);
      return pub;
    }
    // 链式 FIFO：prev = 上一帧的"链尾"事件（初始已 set），mine = 本帧的链尾。本帧完成
    // 时必须 set **mine**（发布瞬间装进 topics[topic].chain 的那一个）；若那时再读
    // topics[topic].chain，拿到的是后来者安装的链尾，会放错帧、让后续帧永久阻塞。
    auto prev = std::make_shared<weave::Event>(topic_for(topic).chain);
    auto mine = std::make_shared<weave::Event>();
    topic_for(topic).chain = *mine;  // 安装新链尾（同步段完成，保发布序）
    weave::create_task(std::string("shuttle-seq-") + std::string(topic),
                       [this, pub, env, trig = std::move(trig), targets = std::move(targets),
                        prev, mine]() mutable {
      std::optional<Publication::Failure> failure;
      Topic& t = topic_for(env->meta.topic);
      std::size_t idx = 0;  // 已到达终态的目标数；单元级失败时用它补齐
      try {
        wait_trigger(trig);
        prev->wait();  // 排在上一帧之后
        for (; idx < targets.size(); ++idx) {
          const auto& sub = targets[idx];
          try {
            if (deliver(sub, *env) == Delivery::delivered)
              ++t.delivered;
            else
              ++pub->retired;
          } catch (const weave::CancelledError&) {
            throw;  // 取消透传到外层：本目标与其余目标统一记 failed（仍要放行链尾）
          } catch (const std::exception&) {
            count_failed(t, sub);
            if (!failure) failure = failure_of(describe(sub), std::current_exception());
          } catch (...) {
            count_failed(t, sub);
            if (!failure) failure = failure_of(describe(sub), unknown_exc());
          }
          target_finished(pub);
        }
      } catch (const weave::CancelledError&) {
        failure = failure_of("<delivery>", cancelled_exc());
      } catch (const std::exception&) {
        failure = failure_of("<trigger>", std::current_exception());
      } catch (...) {
        failure = failure_of("<trigger>", unknown_exc());
      }
      // 单元级失败（触发条件抛异常 / 被取消 / 其余目标未尝试）：本单元未到达终态的目标
      // 全部记 failed —— 没成功就必须落在计数里，不能只留一条 Failure。
      for (; idx < targets.size(); ++idx) {
        ++t.failed;
        ++targets[idx]->failed;
        target_finished(pub);
      }
      mine->set();  // 无论成败放行下一帧，防止链 stall
      record_failure(pub, failure);
      if (pub->finished == pub->expected && !pub->done_flag) pub_complete(pub);
    });
    return pub;
  }

  // ── `publish_concurrent`：每订阅者一投递单元，互不等待 ─────────────────────────────────
  std::shared_ptr<PubState> start_concurrent(std::string_view topic,
                                      std::shared_ptr<const Envelope> env, Trigger trig,
                                      std::vector<std::shared_ptr<Subscriber>> targets) {
    auto pub = std::make_shared<PubState>();
    pub->id = next_pub_id++;
    pub->expected = targets.size();
    pubs[pub->id] = pub;
    if (targets.empty()) {
      // 无投递单元：立即完成（否则 waiting 者永远等不到）
      pub_complete(pub);
      return pub;
    }
    for (const auto& sub : targets) {
      weave::create_task(std::string("shuttle-par-") + std::string(topic),
                         [this, pub, env, trig, sub]() mutable {
        std::optional<Publication::Failure> failure;
        Topic& t = topic_for(env->meta.topic);
        try {
          wait_trigger(trig);
          if (deliver(sub, *env) == Delivery::delivered)
            ++t.delivered;
          else
            ++pub->retired;
        } catch (const weave::CancelledError&) {
          // 取消与异常同一口径：没有成功收尾就是 failed（dropped 的一部分）
          count_failed(t, sub);
          failure = failure_of(describe(sub), cancelled_exc());
        } catch (const std::exception&) {
          count_failed(t, sub);
          failure = failure_of(describe(sub), std::current_exception());
        } catch (...) {
          count_failed(t, sub);
          failure = failure_of(describe(sub), unknown_exc());
        }
        unit_finished(pub, failure);
      });
    }
    return pub;
  }

  static std::string describe(const std::shared_ptr<Subscriber>& sub) {
    if (sub->kind == SubKind::external)
      return "external handle " + std::to_string(sub->handle);
    return "cpp sub " + std::to_string(sub->id);
  }

  // 按入口取匹配目标（一条注册只有一个 cap，所以天然是两拨）
  [[nodiscard]] static std::vector<std::shared_ptr<Subscriber>> select(
      const std::vector<std::shared_ptr<Subscriber>>& matched, Capability want) {
    std::vector<std::shared_ptr<Subscriber>> out;
    for (const auto& s : matched)
      if (s->cap == want) out.push_back(s);
    return out;
  }

  // 新订阅者的 retained 补发（注册后调用）。只补**本订阅者的目标帧**：名字与类型都要
  // 对上，否则它本来就不是那条 retained 帧的目标（不建 task，也不计 dropped）。
  void retained_catchup(const std::shared_ptr<Subscriber>& sub) {
    for (auto& [name, t] : topics) {
      if (!t.retained_on || !t.retained_env || !match(sub->pattern, name)) continue;
      if (!type_matches(*sub, t.retained_env->meta.type_tag)) continue;
      if (sub->kind == SubKind::external && sub->cap == Capability::blocking) {
        // SV 阻塞回调不能在当前（可能是 function）上下文中经 exported task 调用，
        // 改经后台仿真进程补发。
        auto env_copy = t.retained_env;
        weave::create_task("shuttle-retained", [this, sub, env_copy]() {
          if (deliver(sub, *env_copy) == Delivery::delivered)
            ++topic_for(env_copy->meta.topic).delivered;
        });
      } else {
        if (deliver(sub, *t.retained_env) == Delivery::delivered) ++t.delivered;
      }
    }
  }

  void remove_sub(const std::shared_ptr<Subscriber>& sub) {
    // 先置位再摘表：在途投递（快照仍持有 shared_ptr）据此记 retired 而不是 delivered。
    sub->removed = true;
    subs.erase(std::remove(subs.begin(), subs.end(), sub), subs.end());
  }
};

// 同一份"publish_now 公共段"被 C++ 类型化入口与 ext_pub_now 共用，所以在头里。
// publish_now 公共段：nonblocking 入口的订阅者同步投递 + 入口为 blocking 的目标按策略处理。
// 返回值覆盖本帧的**目标**：nonblocking 部分返回时已结束，defer 部分在后台串行链里；
// skip 掉的不计入 expected，只进 skipped()；reject 直接抛，没有返回值。
inline std::shared_ptr<PubState> dispatch_now(Broker& b, std::shared_ptr<const Envelope> env,
                                               NowPolicy policy) {
  // env 由本函数与后台链持有，视图全程稳定：少一次每帧的 string 拷贝
  const std::string_view topic = env->meta.topic;
  Topic& t = b.topic_for(topic);
  // 目标集 = 名字 ∩ 类型：类型不相交的订阅者既不进 expected，也不建任何投递单元
  const auto matched = b.match_subs(topic, env->meta.type_tag);
  // 按"该次注册的那一条入口"把目标劈成两拨：一条注册只可能有一条入口（`cap` 互斥），
  // 所以这里不是"优先哪条"。这个劈分同时是 DPI 约束的保证：下面的同步段只碰 `nb_subs`，
  // 而它们的 `cap` 已保证 `deliver` 走 export function（Error-[DPI-INCDIC]）。
  auto nb_subs = Broker::select(matched, Capability::nonblocking);
  auto blocking_targets = Broker::select(matched, Capability::blocking);
  // `NowPolicy::reject` 的可行性已在 `pub_now` / `ext_pub_now` 里**前置**检查：这里不会再抛，
  // 所以被 reject 的帧连类型绑定、seq、retained 快照都不会动（等于从未发布过）。

  auto pub = std::make_shared<PubState>();
  pub->id = b.next_pub_id++;
  const bool defer = (policy == NowPolicy::defer) && !blocking_targets.empty();
  pub->expected = nb_subs.size() + (defer ? blocking_targets.size() : 0);
  pub->deferred = defer ? blocking_targets.size() : 0;
  pub->skipped = defer ? 0 : blocking_targets.size();
  b.pubs[pub->id] = pub;

  // 同步段：每个目标都推进 finished；回调异常记入 failures 后**继续**下一个目标
  // （单个订阅者失败不影响其余订阅者），全部尝试完、defer 链启动之后再抛给调用者。
  std::exception_ptr first_exc;
  for (const auto& sub : nb_subs) {
    try {
      if (b.deliver(sub, *env) == Delivery::delivered)
        ++t.delivered;
      else
        ++pub->retired;
    } catch (const weave::CancelledError&) {
      Broker::count_failed(t, sub);
      b.record_failure(pub, Broker::failure_of(Broker::describe(sub), Broker::cancelled_exc()));
      if (!first_exc) first_exc = pub->failures.back().error;
    } catch (const std::exception&) {
      Broker::count_failed(t, sub);
      b.record_failure(pub, Broker::failure_of(Broker::describe(sub), std::current_exception()));
      if (!first_exc) first_exc = pub->failures.back().error;
    } catch (...) {
      Broker::count_failed(t, sub);
      b.record_failure(pub, Broker::failure_of(Broker::describe(sub), Broker::unknown_exc()));
      if (!first_exc) first_exc = pub->failures.back().error;
    }
    b.target_finished(pub);
  }
  if (policy == NowPolicy::skip) {
    t.skipped += blocking_targets.size();
    for (const auto& sub : blocking_targets) ++sub->skipped;
  }  if (!defer) {
    // 非 defer：`finished` 已在逐目标记账里到齐（含零目标的情况）
    if (!pub->done_flag) b.pub_complete(pub);
    if (first_exc) std::rethrow_exception(first_exc);
    return pub;
  }
  // `finished` 的剩余部分是后台链的那一拨（由下面的 on_complete 钩子并回）
  t.deferred += blocking_targets.size();
  auto chain = b.start_serial(topic, std::move(env), Trigger{}, std::move(blocking_targets));
  const std::size_t n = chain->expected;
  chain->on_complete.push_back([pub, chain, n]() {
    for (const auto& f : chain->failures) pub->failures.push_back(f);
    if (pub->first_error.empty()) pub->first_error = chain->first_error;
    pub->retired += chain->retired;
    pub->finished += n;
    if (!pub->done_flag) Broker::get().pub_complete(pub);
  });
  // defer 链已经启动：此时才把同步段的第一个异常抛给调用者（句柄不返回，但后台目标照常投递）
  if (first_exc) std::rethrow_exception(first_exc);
  return pub;
}

}  // namespace shuttle::detail

namespace shuttle {

using detail::PubState;

// 句柄 Impl：Publication 持有 PubState，两个 TU 都要能构造（detail_pub_access 在头里）。
struct Publication::Impl {
  std::shared_ptr<PubState> pub;
};

// detail_pub_access：publication.hpp 指定的句柄构造友元
struct detail_pub_access {
  static Publication make(std::shared_ptr<Publication::Impl> impl) {
    return Publication(std::move(impl));
  }
  // 便捷重载：直接包装 PubState
  static Publication make(std::shared_ptr<PubState> pub) {
    auto impl = std::make_shared<Publication::Impl>();
    impl->pub = std::move(pub);
    return Publication(std::move(impl));
  }
  // 反查：句柄背后的 PubState（detail::pub_id 用）
  static std::shared_ptr<PubState> state(const Publication& p) { return p.impl_ ? p.impl_->pub : nullptr; }
};

}  // namespace shuttle
