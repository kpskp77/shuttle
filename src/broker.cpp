// broker 核心：topic 表、通配符路由、类型绑定、三种发布方式的投递编排、
// Publication / Subscription 句柄、retained、shuttle::Event 注册表、内省。
//
// 并发模型：weave 保证任意时刻至多一个仿真进程在执行 C++ 代码
// （阻塞原语 ≡ 交出调度权），因此 broker 状态是普通数据结构，无需锁。
// 句柄的引用计数（shared_ptr）跨进程边界的读写同样受该不变性保护。

#include <algorithm>
#include <cstdio>
#include <exception>
#include <map>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <shuttle/detail/abi.hpp>
#include <shuttle/broker.hpp>
#include <shuttle/exception.hpp>
#include <shuttle/publication.hpp>
#include <shuttle/subscription.hpp>
#include <shuttle/trigger.hpp>
#include <weave/event.hpp>
#include <weave/sync.hpp>
#include <weave/task.hpp>

#include "internal/broker_internal.hpp"
#include "internal/sv_bridge.hpp"

namespace shuttle {
namespace {

// ── 消息信封：一次发布的不可变快照 ─────────────────────────────────────────
struct Envelope {
  std::shared_ptr<const void> payload;  // 类型化：T；原始字节流：std::vector<std::byte>
  const detail::TypeOps* ops;           // 原始字节流时为 &raw_ops
  mutable std::shared_ptr<const std::vector<std::byte>> encoded;  // 跨语言编码缓存
  Meta meta;
};

// 原始字节流（SV 发布且 topic 未绑定 C++ 类型）的操作表：
// encode 为恒等拷贝，decode 缺失（无法物化为 C++ 类型）。
const detail::TypeOps raw_ops{"",
                              [](const void* p) {
                                return *static_cast<const std::vector<std::byte>*>(p);
                              },
                              nullptr};

// 取出信封的字节流表示（跨语言投递与字节订阅使用），结果按信封缓存。
const std::vector<std::byte>& envelope_bytes(const Envelope& env) {
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
enum class SubKind { typed, bytes, sv };

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
  // kind == sv：SV 注册表编号
  int sv_sid = 0;
  // 诊断计数
  std::uint64_t received = 0;
  std::uint64_t dropped = 0;
};

// ── 单次投递的结果（记账用）───────────────────────────────────────────────
// `delivered` = 回调执行完毕；`retired` = 该订阅者在快照之后、回调之前被退订（SV 注册表
// 丢弃了这次在途投递），目标已到终态但内容没进回调。类型标签的判定不在这里——它发生在
// **目标集快照**（`match_subs`：名字 ∩ 类型）时，不进目标集的订阅者根本没有投递单元。
// 失败不走返回值，而是抛异常，由投递单元记成 failed。
enum class Delivery { delivered, retired };

// ── Topic ─────────────────────────────────────────────────────────────────
struct Topic {
  bool bound = false;
  std::string bound_tag;                // 类型标签（raw 绑定 = SV type_tag）
  const detail::TypeOps* ops = nullptr;  // raw 绑定时指向 raw_ops
  bool retained_on = false;
  std::shared_ptr<const Envelope> retained_env;
  std::uint64_t seq = 0;        // topic 内全局单调（跨发布者连续，多对多契约）
  std::uint64_t published = 0;  // 累计发布帧数
  std::uint64_t delivered = 0;  // 累计投递次数（所有订阅者合计）
  std::uint64_t dropped = 0;    // 累计丢弃帧数（退订丢弃/drop 策略/失败合计）
  std::uint64_t deferred = 0;   // 累计被 defer 到后台串行链的投递数
  weave::Event chain;           // 后台投递的 per-topic 串行链尾（`publish_serial` 与 `now` 的 defer 共用）
  Topic() { chain.set(); }
};

// ── Publication 状态 ──────────────────────────────────────────────────────
struct PubState {
  int id = 0;
  // 本帧目标数：`serial`/`concurrent` = 匹配到的订阅者数；`now` = nonblocking 目标数
  // +（仅 defer 时）blocking 目标数（`drop` 掉的不算目标）
  std::size_t expected = 0;
  std::size_t finished = 0;
  // 可见性计数：本帧没收到 / 稍后才收到的订阅者（都是标记，不是余额；
  // 见 docs/architecture.md 的"记账契约"一章）
  std::size_t excluded = 0;
  std::size_t deferred = 0;
  std::size_t retired = 0;  // 快照后被退订而跳过（计入 finished 与 dropped，不是失败）
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
  std::map<std::string, Topic> topics;
  std::vector<std::shared_ptr<Subscriber>> subs;
  std::map<int, std::shared_ptr<PubState>> pubs;
  int next_sub_id = 1;
  int next_pub_id = 1;

  static Broker& get() {
    static Broker b;
    return b;
  }

  // 是否含通配符（"*" 或 ">"）
  static bool has_wildcard(const std::string& pattern) {
    return pattern.find('*') != std::string::npos || pattern.find('>') != std::string::npos;
  }

  // 通配符匹配："*" 匹配单级，">"（须为末段）匹配其后至少一级（不匹配前缀本身）。
  static bool match(const std::string& pattern, const std::string& topic) {
    const std::vector<std::string> pp = split(pattern);
    const std::vector<std::string> tp = split(topic);
    return match_seg(pp, 0, tp, 0);
  }

  static std::vector<std::string> split(const std::string& s) {
    std::vector<std::string> out;
    std::size_t start = 0;
    while (true) {
      const std::size_t dot = s.find('.', start);
      if (dot == std::string::npos) {
        out.push_back(s.substr(start));
        return out;
      }
      out.push_back(s.substr(start, dot - start));
      start = dot + 1;
    }
  }

  static bool match_seg(const std::vector<std::string>& pp, std::size_t pi,
                        const std::vector<std::string>& tp, std::size_t ti) {
    if (pi == pp.size()) return ti == tp.size();
    if (pp[pi] == ">") return ti < tp.size();  // 末段通配：其后至少一级
    if (ti == tp.size()) return false;
    if (pp[pi] != "*" && pp[pi] != tp[ti]) return false;
    return match_seg(pp, pi + 1, tp, ti + 1);
  }

  // 类型相交：订阅者的类型标签与帧的 type_tag 一致（空标签 = 任意类型）。
  // 这是**目标集**的判据之一：名字匹配但类型不相交的订阅者不是本帧的目标，不建投递单元、
  // 不产生任何计数（`subscriber_count()` 就是“名字 ∩ 类型”的大小）。
  static bool type_matches(const Subscriber& sub, const std::string& tag) {
    return sub.filter_tag.empty() || sub.filter_tag == tag;
  }

  // 快照目标订阅者集合：名字匹配 ∩ 类型匹配
  std::vector<std::shared_ptr<Subscriber>> match_subs(const std::string& topic,
                                                      const std::string& tag) {
    std::vector<std::shared_ptr<Subscriber>> out;
    for (const auto& s : subs)
      if (type_matches(*s, tag) && match(s->pattern, topic)) out.push_back(s);
    return out;
  }

  // 只按名字计数（不判类型）：`broker::topic_info` 的内省口径
  std::size_t name_matched_count(const std::string& topic) const {
    std::size_t n = 0;
    for (const auto& s : subs)
      if (match(s->pattern, topic)) ++n;
    return n;
  }

  Topic& topic_for(const std::string& name) { return topics[name]; }

  // 订阅时的类型过滤预校验（仅当订阅模式恰好是具体 topic 且已绑定时可判；
  // 通配符/未绑定 topic 推迟到目标集快照时按帧标签相交）
  void check_filter_early(const Subscriber& sub) {
    if (sub.filter_tag.empty()) return;
    const auto it = topics.find(sub.pattern);
    if (it != topics.end() && it->second.bound && it->second.bound_tag != sub.filter_tag)
      throw TypeMismatchError(std::string("shuttle: 订阅 ") + sub.pattern + " 声明类型 " +
                              sub.filter_tag + "，与 topic 绑定 " + it->second.bound_tag +
                              " 冲突");
  }

  // 首次绑定 topic 类型时的反向预校验（`check_filter_early` 的镜像）：确认没有"模式恰好是
  // 该 topic + 标签不同"的订阅者已经挂在那里。**通配符订阅者必须跳过**——它们本就横跨
  // 不同类型，只在目标集快照时按帧标签相交。
  // 没有这一步，同一个用户错误（订阅者标签写错）报不报错就取决于注册顺序：
  // topic 先绑定 → 订阅时 `check_filter_early` 抛；订阅先发生 → 该订阅者此后收到的每一帧
  // 都悄无声息地不属于目标集，永远不报错。
  void check_bind_compat(const std::string& topic, const std::string& tag) {
    for (const auto& sub : subs) {
      if (sub->filter_tag.empty() || sub->filter_tag == tag) continue;
      if (has_wildcard(sub->pattern) || !match(sub->pattern, topic)) continue;
      throw TypeMismatchError(std::string("shuttle: topic ") + topic + " 绑定类型 " + tag +
                              "，但已有订阅者声明 " + sub->filter_tag);
    }
  }

  // `publish_now` 的 reject 预检：在**任何副作用**（类型绑定、seq、retained 快照）之前判定
  // 这一帧能不能发。被拒绝的帧等于从未发布过（`published`/`seq` 都不推进）。
  // 只数**目标集**里的 blocking 订阅者：类型不相交的订阅者本帧根本没打算投给它，
  // 不能因为它存在就把一帧发不出去。
  void now_preflight(const std::string& topic, const std::string& tag, NowPolicy policy) {
    if (policy != NowPolicy::reject) return;
    const auto matched = match_subs(topic, tag);
    const std::size_t n = select(matched, Capability::blocking).size();
    if (n == 0) return;
    throw CapabilityError(std::string("shuttle: publish_now 遇到 ") + std::to_string(n) +
                          " 个入口为 blocking 的订阅者（topic " + topic +
                          "）；它们无法在 publish_now 里投递，可用 NowPolicy::defer/drop，"
                          "或改用 publish_serial/publish_concurrent");
  }

  // 投递一帧到单个订阅者（在投递进程的上下文执行）。
  // 入口**只**由该注册的 `cap` 决定（不变量 4）：blocking 注册走 export task，nonblocking
  // 注册走 export function。没有"本次投递"级的入口覆盖——`now` 的同步段之所以不会踩到
  // `Error-[DPI-INCDIC]`，是因为 `select` 已按 `cap` 把 blocking 目标摘走了（§7）。
  // 类型标签不在这里判：调用方只会传**目标集**里的订阅者（`match_subs` 已求过名字 ∩ 类型）。
  Delivery deliver(const std::shared_ptr<Subscriber>& sub, const Envelope& env) {
    const bool use_nb = sub->cap == Capability::nonblocking;
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
      case SubKind::sv:
        // export task / export function 的选择按订阅者入口（`cap`），与调用方无关：
        if (use_nb)
          detail::sv::dispatch_nb(sub->sv_sid, env.meta.topic, env.meta.type_tag, env.meta.seq,
                                  env.meta.time_fs, envelope_bytes(env));
        else
          detail::sv::dispatch(sub->sv_sid, env.meta.topic, env.meta.type_tag, env.meta.seq,
                               env.meta.time_fs, envelope_bytes(env));
        // SV 注册表报告该 id 已退订：这次在途投递被丢弃。以前这里照样 ++received，
        // 于是在 C++ 侧是“幻影成功”；现在计为 retired（未进入回调，但不是错误）。
        if (detail::sv::take_retired_delivery()) {
          ++sub->dropped;
          ++topics[env.meta.topic].dropped;
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

  // 建立类型绑定 + 打序号 + retained 快照（发布公共段）。
  std::shared_ptr<const Envelope> make_envelope(const std::string& topic,
                                                std::shared_ptr<const void> payload,
                                                const detail::TypeOps* ops) {
    Topic& t = topic_for(topic);
    if (!t.bound) {
      check_bind_compat(topic, ops->tag);
      t.bound = true;
      t.ops = ops;
      t.bound_tag = ops->tag;
    } else if (t.bound_tag != std::string(ops->tag)) {
      throw TypeMismatchError(std::string("shuttle: topic ") + topic + " 已绑定类型 " +
                              t.bound_tag + "，本次发布类型 " + ops->tag);
    }
    auto env = std::make_shared<Envelope>();
    env->payload = std::move(payload);
    env->ops = ops;
    env->meta.topic = topic;
    env->meta.type_tag = t.bound_tag;
    env->meta.seq = ++t.seq;
    env->meta.time_fs = detail::sv::now_fs();
    ++t.published;
    if (t.retained_on) t.retained_env = env;
    return env;
  }

  // 从字节流发布（SV 侧）：raw 绑定持有字节拷贝；typed 绑定物化为 C++ 对象。
  std::shared_ptr<const Envelope> make_envelope_bytes(const std::string& topic,
                                                      const std::string& tag,
                                                      const std::vector<std::byte>& bytes) {
    Topic& t = topic_for(topic);
    if (!t.bound) {
      // 该 topic 没有 C++ 发布者（只有 SV 发布者）：若已有匹配的 C++ 类型化订阅者，
      // 就按订阅者的类型绑定，这样本帧可以物化成 C++ 对象；否则退化为纯字节流。
      check_bind_compat(topic, tag);
      t.bound = true;
      t.ops = &raw_ops;
      t.bound_tag = tag;
      for (const auto& sub : subs) {
        if (sub->kind != SubKind::typed || !sub->ops || !sub->ops->decode) continue;
        if (!type_matches(*sub, tag) || !match(sub->pattern, topic)) continue;
        t.ops = sub->ops;
        break;
      }
    } else if (t.bound_tag != tag) {
      throw TypeMismatchError(std::string("shuttle: topic ") + topic + " 已绑定类型 " +
                              t.bound_tag + "，本次发布类型 " + tag);
    }
    if (t.ops == &raw_ops) {
      auto payload = std::make_shared<const std::vector<std::byte>>(bytes);
      auto env = std::make_shared<Envelope>();
      env->payload = std::move(payload);
      env->ops = &raw_ops;
      env->meta.topic = topic;
      env->meta.type_tag = t.bound_tag;
      env->meta.seq = ++t.seq;
      env->meta.time_fs = detail::sv::now_fs();
      ++t.published;
      if (t.retained_on) t.retained_env = env;
      return env;
    }
    if (!t.ops->decode)
      throw TypeMismatchError(std::string("shuttle: topic ") + topic +
                              " 绑定的类型不支持从字节流物化");
    return make_envelope(topic, t.ops->decode(bytes), t.ops);
  }

  static void wait_trigger(const Trigger& trig) {
    if (!trig.immediate()) trig.wait();
  }

  // ── `publish_serial`：单投递单元，per-topic 串行链保发布序 FIFO ─────────────────────
  std::shared_ptr<PubState> start_serial(const std::string& topic,
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
    auto prev = std::make_shared<weave::Event>(topics[topic].chain);
    auto mine = std::make_shared<weave::Event>();
    topics[topic].chain = *mine;  // 安装新链尾（同步段完成，保发布序）
    weave::create_task("shuttle-seq-" + topic,
                       [this, pub, env, trig = std::move(trig), targets = std::move(targets),
                        prev, mine]() mutable {
      std::optional<Publication::Failure> failure;
      Topic& t = topics[env->meta.topic];
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
            ++t.dropped;
            ++sub->dropped;
            if (!failure)
              failure = Publication::Failure{describe(sub), std::current_exception()};
          } catch (...) {
            ++t.dropped;
            ++sub->dropped;
            if (!failure)
              failure = Publication::Failure{describe(sub),
                                             std::make_exception_ptr(std::runtime_error("未知异常"))};
          }
          target_finished(pub);
        }
      } catch (const weave::CancelledError&) {
        failure = Publication::Failure{"<delivery>",
                                       std::make_exception_ptr(std::runtime_error("投递被取消"))};
      } catch (const std::exception&) {
        failure = Publication::Failure{"<trigger>", std::current_exception()};
      } catch (...) {
        failure = Publication::Failure{
            "<trigger>", std::make_exception_ptr(std::runtime_error("未知异常"))};
      }
      // 单元级失败（触发条件抛异常 / 被取消 / 其余目标未尝试）：本单元未到达终态的目标
      // 全部记 failed —— 没收到就必须落在计数里，不能只留一条 Failure。
      for (; idx < targets.size(); ++idx) {
        ++t.dropped;
        ++targets[idx]->dropped;
        target_finished(pub);
      }
      mine->set();  // 无论成败放行下一帧，防止链 stall
      record_failure(pub, failure);
      if (pub->finished == pub->expected && !pub->done_flag) pub_complete(pub);
    });
    return pub;
  }

  // ── `publish_concurrent`：每订阅者一投递单元，互不等待 ─────────────────────────────────
  std::shared_ptr<PubState> start_concurrent(const std::string& topic,
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
      weave::create_task("shuttle-par-" + topic, [this, pub, env, trig, sub]() mutable {
        std::optional<Publication::Failure> failure;
        Topic& t = topics[env->meta.topic];
        try {
          wait_trigger(trig);
          if (deliver(sub, *env) == Delivery::delivered)
            ++t.delivered;
          else
            ++pub->retired;
        } catch (const weave::CancelledError&) {
          // 取消与异常同一口径：没收到内容就是 failed，计入 dropped
          ++t.dropped;
          ++sub->dropped;
          failure = Publication::Failure{describe(sub),
                                         std::make_exception_ptr(std::runtime_error("投递被取消"))};
        } catch (const std::exception&) {
          ++t.dropped;
          ++sub->dropped;
          failure = Publication::Failure{describe(sub), std::current_exception()};
        } catch (...) {
          ++t.dropped;
          ++sub->dropped;
          failure = Publication::Failure{describe(sub),
                                         std::make_exception_ptr(std::runtime_error("未知异常"))};
        }
        unit_finished(pub, failure);
      });
    }
    return pub;
  }

  static std::string describe(const std::shared_ptr<Subscriber>& sub) {
    if (sub->kind == SubKind::sv) return "sv sid " + std::to_string(sub->sv_sid);
    return "cpp sub " + std::to_string(sub->id);
  }

  // 按入口取匹配目标（一条注册只有一个 cap，所以天然是两拨）
  static std::vector<std::shared_ptr<Subscriber>> select(
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
      if (sub->kind == SubKind::sv && sub->cap == Capability::blocking) {
        // SV 阻塞回调不能在当前（可能是 function）上下文中经 exported task 调用，
        // 改经后台仿真进程补发。
        auto env_copy = t.retained_env;
        weave::create_task("shuttle-retained", [this, sub, env_copy]() {
          if (deliver(sub, *env_copy) == Delivery::delivered)
            ++topics[env_copy->meta.topic].delivered;
        });
      } else {
        if (deliver(sub, *t.retained_env) == Delivery::delivered) ++t.delivered;
      }
    }
  }

  void remove_sub(const std::shared_ptr<Subscriber>& sub) {
    subs.erase(std::remove(subs.begin(), subs.end(), sub), subs.end());
  }
};

thread_local std::string g_last_error;

}  // namespace

// 句柄 Impl 定义（先于 detail_pub_access / detail:: 实现出现）
struct Publication::Impl {
  std::shared_ptr<PubState> pub;
};

struct Subscription::Impl {
  ~Impl() {
    if (active) Broker::get().remove_sub(sub);
  }
  std::shared_ptr<Subscriber> sub;
  bool active = true;
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
  // 反查：句柄背后的 PubState（detail::pub_id_of 用）
  static std::shared_ptr<PubState> state(const Publication& p) { return p.impl_ ? p.impl_->pub : nullptr; }
};

namespace detail {

void set_last_error(std::string msg) { g_last_error = std::move(msg); }
const std::string& last_error() { return g_last_error; }

// ── detail/abi.hpp 的实现 ─────────────────────────────────────────

void topic_declare(const std::string& topic, const TypeOps* ops) {
  Broker& b = Broker::get();
  Topic& t = b.topics[topic];
  if (!t.bound) {
    t.bound = true;
    t.ops = ops;
    t.bound_tag = ops->tag;
    return;
  }
  // 既有 raw 绑定（SV 先发布）且标签一致 → 升级为 typed 绑定
  if (t.bound_tag == std::string(ops->tag) && t.ops == &raw_ops) {
    t.ops = ops;
    return;
  }
  if (t.bound_tag != std::string(ops->tag))
    throw TypeMismatchError(std::string("shuttle: topic ") + topic + " 已绑定类型 " +
                            t.bound_tag + "，与 " + ops->tag + " 冲突");
  // 同标签重复声明：视为同一类型，放行（跨 DSO 的 type_ops<T> 实例地址不同但语义一致）
}

// publish_now 公共段：nonblocking 入口的订阅者同步投递 + 入口为 blocking 的目标按策略处理。
// 返回值覆盖本帧的**目标**：nonblocking 部分返回时已结束，defer 部分在后台串行链里；
// drop 掉的不计入 expected，只进 excluded()；reject 直接抛，没有返回值。
static std::shared_ptr<PubState> dispatch_now(Broker& b, std::shared_ptr<const Envelope> env,
                                               NowPolicy policy) {
  const std::string topic = env->meta.topic;
  Topic& t = b.topics[topic];
  // 目标集 = 名字 ∩ 类型：类型不相交的订阅者既不进 expected，也不建任何投递单元
  const auto matched = b.match_subs(topic, env->meta.type_tag);
  // 按"该次注册的那一条入口"把目标劈成两拨：一条注册只可能有一条入口（`cap` 互斥），
  // 所以这里不是"优先哪条"。这个劈分同时是 DPI 约束的保证：下面的同步段只碰 `nb_subs`，
  // 而它们的 `cap` 已保证 `deliver` 走 export function（Error-[DPI-INCDIC]）。
  auto nb_subs = Broker::select(matched, Capability::nonblocking);
  auto blocking_targets = Broker::select(matched, Capability::blocking);
  // `NowPolicy::reject` 的可行性已在 `pub_now` / `pub_now_frame` 里**前置**检查：这里不会再抛，
  // 所以被 reject 的帧连类型绑定、seq、retained 快照都不会动（等于从未发布过）。

  auto pub = std::make_shared<PubState>();
  pub->id = b.next_pub_id++;
  const bool defer = (policy == NowPolicy::defer) && !blocking_targets.empty();
  pub->expected = nb_subs.size() + (defer ? blocking_targets.size() : 0);
  pub->deferred = defer ? blocking_targets.size() : 0;
  pub->excluded = defer ? 0 : blocking_targets.size();
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
      ++t.dropped;
      ++sub->dropped;
      pub->failures.push_back(Publication::Failure{
          Broker::describe(sub),
          std::make_exception_ptr(std::runtime_error("投递被取消"))});
      if (pub->first_error.empty()) pub->first_error = "投递被取消";
      if (!first_exc) first_exc = pub->failures.back().error;
    } catch (const std::exception& e) {
      ++t.dropped;
      ++sub->dropped;
      pub->failures.push_back(
          Publication::Failure{Broker::describe(sub), std::current_exception()});
      if (pub->first_error.empty()) pub->first_error = e.what();
      if (!first_exc) first_exc = std::current_exception();
    } catch (...) {
      ++t.dropped;
      ++sub->dropped;
      pub->failures.push_back(Publication::Failure{
          Broker::describe(sub),
          std::make_exception_ptr(std::runtime_error("未知异常"))});
      if (pub->first_error.empty()) pub->first_error = "未知异常";
      if (!first_exc) first_exc = pub->failures.back().error;
    }
    b.target_finished(pub);
  }
  if (policy == NowPolicy::drop) {
    t.dropped += blocking_targets.size();
    for (const auto& sub : blocking_targets) ++sub->dropped;
  }
  if (!defer) {
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

Publication pub_now(const std::string& topic, std::shared_ptr<const void> msg,
                       const TypeOps* ops, NowPolicy policy) {
  Broker& b = Broker::get();
  b.now_preflight(topic, ops->tag, policy);  // reject 先判：不消耗 seq / 不刷新 retained
  auto env = b.make_envelope(topic, std::move(msg), ops);
  return detail_pub_access::make(dispatch_now(b, std::move(env), policy));
}

Publication pub_serial(const std::string& topic, std::shared_ptr<const void> msg,
                       const TypeOps* ops, Trigger trig) {
  Broker& b = Broker::get();
  auto env = b.make_envelope(topic, std::move(msg), ops);
  // 全部目标订阅者（名字 ∩ 类型）：每个用自己注册的那条入口（blocking 或 nonblocking）各调用一次
  auto targets = b.match_subs(topic, env->meta.type_tag);
  return detail_pub_access::make(b.start_serial(topic, std::move(env), std::move(trig),
                                             std::move(targets)));
}

Publication pub_concurrent(const std::string& topic, std::shared_ptr<const void> msg,
                           const TypeOps* ops, Trigger trig) {
  Broker& b = Broker::get();
  auto env = b.make_envelope(topic, std::move(msg), ops);
  auto targets = b.match_subs(topic, env->meta.type_tag);
  return detail_pub_access::make(b.start_concurrent(topic, std::move(env), std::move(trig),
                                             std::move(targets)));
}

Subscription sub_typed(std::string pattern, const TypeOps* ops, ErasedCallback cb, Capability cap,
                       SubOptions opts) {
  (void)opts;
  Broker& b = Broker::get();
  // 精确 topic 的订阅即声明该 topic 的类型绑定（这样"SV 先发布、C++ 后订阅"
  // 或"C++ 只订阅不发布"的 topic 也能在发布时正确物化）。通配符订阅无法
  // 声明具体 topic 的绑定，交给 make_envelope_bytes 在发布时按订阅者类型绑定。
  if (ops && !Broker::has_wildcard(pattern)) topic_declare(pattern, ops);
  auto sub = std::make_shared<Subscriber>();
  sub->id = b.next_sub_id++;
  sub->pattern = std::move(pattern);
  sub->kind = SubKind::typed;
  sub->cap = cap;
  sub->filter_tag = ops ? ops->tag : "";
  sub->ops = ops;
  sub->cb = std::move(cb);
  b.check_filter_early(*sub);
  b.subs.push_back(sub);
  b.retained_catchup(sub);
  auto impl = std::make_shared<Subscription::Impl>();
  impl->sub = sub;
  return Subscription(std::move(impl));
}

Subscription sub_raw(std::string pattern, ByteCallback cb, Capability cap, SubOptions opts) {
  (void)opts;
  Broker& b = Broker::get();
  auto sub = std::make_shared<Subscriber>();
  sub->id = b.next_sub_id++;
  sub->pattern = std::move(pattern);
  sub->kind = SubKind::bytes;
  sub->cap = cap;
  sub->cb_bytes = std::move(cb);
  b.subs.push_back(sub);
  b.retained_catchup(sub);
  auto impl = std::make_shared<Subscription::Impl>();
  impl->sub = sub;
  return Subscription(std::move(impl));
}

void topic_set_retained(const std::string& topic, bool on) {
  Topic& t = Broker::get().topics[topic];
  t.retained_on = on;
  // 关闭时立即丢掉快照：否则快照会一直占着最新一帧，
  // 且 retained_catchup 需要靠 retained_on 判断，两者保持一致更不易错。
  if (!on) t.retained_env.reset();
}
bool topic_retained(const std::string& topic) {
  auto it = Broker::get().topics.find(topic);
  return it != Broker::get().topics.end() && it->second.retained_on;
}

// ── SV 侧入口（svdpi 桥使用；错误经 set_last_error + 返回码报告）────────────

Publication pub_now_frame(const std::string& topic, const std::string& tag,
                             const std::vector<std::byte>& frame, NowPolicy policy) {
  Broker& b = Broker::get();
  b.now_preflight(topic, tag, policy);  // 先判后建帧：reject 不消耗 seq / 不刷新 retained
  auto env = b.make_envelope_bytes(topic, tag, frame);
  return detail_pub_access::make(dispatch_now(b, std::move(env), policy));
}

Publication pub_serial_frame(const std::string& topic, const std::string& tag,
                             const std::vector<std::byte>& frame, Trigger trig) {
  Broker& b = Broker::get();
  auto env = b.make_envelope_bytes(topic, tag, frame);
  auto targets = b.match_subs(topic, env->meta.type_tag);
  return detail_pub_access::make(
      b.start_serial(topic, std::move(env), std::move(trig), std::move(targets)));
}

Publication pub_concurrent_frame(const std::string& topic, const std::string& tag,
                                 const std::vector<std::byte>& frame, Trigger trig) {
  Broker& b = Broker::get();
  auto env = b.make_envelope_bytes(topic, tag, frame);
  auto targets = b.match_subs(topic, env->meta.type_tag);
  return detail_pub_access::make(
      b.start_concurrent(topic, std::move(env), std::move(trig), std::move(targets)));
}

int pub_id_of(const Publication& p) {
  if (!p.valid()) return 0;
  const std::shared_ptr<PubState> pub = detail_pub_access::state(p);
  return pub ? pub->id : 0;
}

int sub_sv(const std::string& pattern, int caps, const std::string& type_tag, int sv_sid) {
  try {
    Broker& b = Broker::get();
    // 一次注册 = 一条入口：caps 必须恰好是 1（`shuttle_subscribe`）或 2
    // （`shuttle_subscribe_nb`）。其它值没有定义——否则 `now` 与 `serial`/`concurrent` 会对同一个
    // 订阅者走不同入口，违背不变量 4，直接拒绝。
    if (caps != 1 && caps != 2)
      throw std::invalid_argument("shuttle: 订阅能力必须恰好一位（blocking=1 / nonblocking=2）");
    auto sub = std::make_shared<Subscriber>();
    sub->id = b.next_sub_id++;
    sub->pattern = pattern;
    sub->kind = SubKind::sv;
    sub->cap = caps == 1 ? Capability::blocking : Capability::nonblocking;
    sub->sv_sid = sv_sid;
    sub->filter_tag = type_tag;
    b.check_filter_early(*sub);
    b.subs.push_back(sub);
    b.retained_catchup(sub);
    return 0;
  } catch (const std::exception& e) {
    set_last_error(e.what());
    return 1;
  }
}

void sub_sv_drop(int sv_sid) {
  Broker& b = Broker::get();
  b.subs.erase(std::remove_if(b.subs.begin(), b.subs.end(),
                              [sv_sid](const std::shared_ptr<Subscriber>& s) {
                                return s->kind == SubKind::sv && s->sv_sid == sv_sid;
                              }),
               b.subs.end());
}

bool pub_done(int pub_id) {
  auto it = Broker::get().pubs.find(pub_id);
  return it != Broker::get().pubs.end() && it->second->done_flag;
}

std::string pub_error(int pub_id) {
  const auto it = Broker::get().pubs.find(pub_id);
  if (it == Broker::get().pubs.end()) return "shuttle: 未知 pub_id";
  return it->second->first_error;
}

void pub_wait(int pub_id, std::int64_t timeout_fs, int* status) {
  Broker& b = Broker::get();
  const auto it = b.pubs.find(pub_id);
  if (it == b.pubs.end()) {
    set_last_error("shuttle: 未知 pub_id " + std::to_string(pub_id));
    *status = 2;
    return;
  }
  const std::shared_ptr<PubState> pub = it->second;
  if (timeout_fs <= 0) {
    pub->done_ev.wait();
  } else {
    weave::Task watcher =
        weave::create_task("shuttle-pub-watch", [&pub]() { pub->done_ev.wait(); });
    try {
      weave::wait_for(watcher, weave::duration(timeout_fs));
    } catch (const weave::TimeoutError&) {
      *status = 1;
      return;
    }
  }
  *status = pub->failures.empty() ? 0 : 2;
}

long long pub_stat(int pub_id, int kind) {
  const auto it = Broker::get().pubs.find(pub_id);
  if (it == Broker::get().pubs.end()) return -1;
  const std::shared_ptr<PubState>& pub = it->second;
  switch (kind) {
    case 0:
      return static_cast<long long>(pub->expected);
    case 1:
      return static_cast<long long>(pub->finished);
    case 2:
      return static_cast<long long>(pub->excluded);
    case 3:
      return static_cast<long long>(pub->deferred);
    case 4:
      return static_cast<long long>(pub->retired);
    default:
      return -1;
  }
}

int pub_state(int pub_id) {
  const auto it = Broker::get().pubs.find(pub_id);
  if (it == Broker::get().pubs.end()) return -1;
  const std::shared_ptr<PubState>& pub = it->second;
  if (!pub->done_flag) return 0;
  return pub->failures.empty() ? 1 : 2;
}

void pub_on_complete(int pub_id, std::function<void()> fn) {
  const auto it = Broker::get().pubs.find(pub_id);
  if (it == Broker::get().pubs.end()) return;
  if (it->second->done_flag) {
    fn();
    return;
  }
  it->second->on_complete.push_back(std::move(fn));
}



}  // namespace detail

// ── Publication 句柄方法 ────────────────────────────────────────────────────
//（构造与 valid() 在 publication.hpp 内联定义）
bool Publication::done() const { return valid() && impl_->pub->done_flag; }

void Publication::wait() {
  if (!valid()) throw std::logic_error("shuttle: 空 Publication 句柄");
  impl_->pub->done_ev.wait();
}

bool Publication::wait(weave::duration timeout) {
  if (!valid()) throw std::logic_error("shuttle: 空 Publication 句柄");
  if (impl_->pub->done_flag) return true;
  const std::shared_ptr<PubState> pub = impl_->pub;
  weave::Task watcher = weave::create_task("shuttle-pub-watch", [pub]() { pub->done_ev.wait(); });
  try {
    weave::wait_for(watcher, timeout);
    return true;
  } catch (const weave::TimeoutError&) {
    return false;
  }
}

void Publication::result() {
  if (!valid()) throw std::logic_error("shuttle: 空 Publication 句柄");
  wait();
  if (!impl_->pub->failures.empty()) std::rethrow_exception(impl_->pub->failures.front().error);
}

std::size_t Publication::subscriber_count() const {
  return valid() ? impl_->pub->expected : 0;
}

std::size_t Publication::finished() const {
  return valid() ? impl_->pub->finished : 0;
}

std::size_t Publication::excluded() const {
  return valid() ? impl_->pub->excluded : 0;
}

std::size_t Publication::deferred() const {
  return valid() ? impl_->pub->deferred : 0;
}

std::size_t Publication::retired() const {
  return valid() ? impl_->pub->retired : 0;
}

std::vector<Publication::Failure> Publication::failures() const {
  if (!valid()) return {};
  return impl_->pub->failures;
}

// ── Subscription 句柄方法 ───────────────────────────────────────────────────
Subscription::~Subscription() = default;

void Subscription::unsubscribe() {
  if (impl_ && impl_->active) {
    impl_->active = false;
    Broker::get().remove_sub(impl_->sub);
  }
}

const std::string& Subscription::topic() const { return impl_->sub->pattern; }
Capability Subscription::capability() const { return impl_->sub->cap; }
std::uint64_t Subscription::received() const { return impl_->sub->received; }
std::uint64_t Subscription::dropped() const { return impl_->sub->dropped; }

// ── shuttle::Event（句柄注册表）─────────────────────────────────────────────
namespace {
std::map<int, std::weak_ptr<Event::Impl>> g_event_registry;
std::map<int, std::shared_ptr<Event::Impl>> g_sv_events;  // SV 侧创建：注册表持有
int g_next_event_id = 1;

// 惰性清理已失效条目（Event 全部副本析构后 weak_ptr 过期）
void event_registry_prune() {
  for (auto it = g_event_registry.begin(); it != g_event_registry.end();) {
    if (it->second.expired())
      it = g_event_registry.erase(it);
    else
      ++it;
  }
}
std::shared_ptr<Event::Impl> event_lookup(int ev_id) {
  if (const auto it = g_sv_events.find(ev_id); it != g_sv_events.end()) return it->second;
  if (const auto it = g_event_registry.find(ev_id); it != g_event_registry.end())
    return it->second.lock();
  return nullptr;
}
}  // namespace

struct Event::Impl {
  explicit Impl(int i) : id(i) {}
  int id;
  weave::Event ev;
};

Event::Event() {
  event_registry_prune();
  auto impl = std::make_shared<Impl>(g_next_event_id++);
  g_event_registry[impl->id] = impl;
  impl_ = std::move(impl);
}

Event::~Event() = default;  // key function：析构定义在此，跨 DSO 只有一份

void Event::set() {
  if (!impl_) throw std::logic_error("shuttle: 空 Event");
  impl_->ev.set();
}
void Event::wait() {
  if (!impl_) throw std::logic_error("shuttle: 空 Event");
  impl_->ev.wait();
}
bool Event::is_set() const { return impl_ && impl_->ev.is_set(); }
int Event::id() const { return impl_ ? impl_->id : -1; }

namespace detail {
// TRIG_EVENT（SV 侧按 id 引用事件）的等待入口：必须在 weave task 上下文调用。
void event_wait_id(int ev_id) {
  const auto impl = event_lookup(ev_id);
  if (!impl)
    throw std::invalid_argument("shuttle: 未知或已销毁的 event id " + std::to_string(ev_id));
  impl->ev.wait();
}

int event_registry_create() {
  event_registry_prune();
  auto impl = std::make_shared<Event::Impl>(g_next_event_id++);
  const int id = impl->id;
  g_sv_events[id] = std::move(impl);
  return id;
}

void event_registry_set(int ev_id) {
  const auto impl = event_lookup(ev_id);
  if (!impl)
    throw std::invalid_argument("shuttle: 未知或已销毁的 event id " + std::to_string(ev_id));
  impl->ev.set();
}

void event_registry_destroy(int ev_id) {
  g_event_registry.erase(ev_id);
  g_sv_events.erase(ev_id);
}
}  // namespace detail

// ── broker 内省 ─────────────────────────────────────────────────────────────
namespace broker {

std::vector<std::string> topics() {
  std::vector<std::string> out;
  for (const auto& [name, t] : Broker::get().topics)
    if (t.published > 0) out.push_back(name);
  return out;
}

std::optional<TopicInfo> topic_info(const std::string& name) {
  Broker& b = Broker::get();
  auto it = b.topics.find(name);
  if (it == b.topics.end()) return std::nullopt;
  const Topic& t = it->second;
  TopicInfo info;
  info.name = name;
  info.type_tag = t.bound ? t.bound_tag : "";
  info.subscriber_count = b.name_matched_count(name);
  info.retained = t.retained_on;
  info.published = t.published;
  info.delivered = t.delivered;
  info.dropped = t.dropped;
  info.deferred = t.deferred;
  return info;
}

}  // namespace broker
}  // namespace shuttle
