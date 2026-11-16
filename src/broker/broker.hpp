#pragma once

// Broker 声明与跨入口共享的调度函数（不安装）。
//
// model.hpp 只含数据；本头声明操作。定义按职责分到：
//   broker.cpp    路由、校验、绑定、投递、订阅与 retained；
//   dispatch.cpp  后台投递单元、publish_now、完成计数与等待。
// svdpi/facade.cpp 可以复用同一入口，但 systemc 后端仍只经 backend/backend.hpp 认识 broker。

#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <shuttle/publication.hpp>
#include <shuttle/trigger.hpp>
#include <string>
#include <string_view>
#include <vector>
#include <weave/time.hpp>

#include "broker/model.hpp"

namespace shuttle::detail {

class Broker {
public:
  std::map<std::string, Topic, std::less<>> topics;  // 透明比较器：string_view 直接查
  std::vector<std::shared_ptr<Subscriber>> subs;
  std::map<int, std::shared_ptr<PubState>> pubs;
  int next_sub_id = 1;
  int next_pub_id = 1;

  static Broker& get();

  // topic 名 / 订阅模式语法与通配符路由。
  [[nodiscard]] static bool has_wildcard(std::string_view pattern);
  static std::size_t seg_end(std::string_view s, std::size_t pos);
  [[nodiscard]] static bool match(std::string_view pattern, std::string_view topic);
  static void check_segments(std::string_view name, const char* what);
  static void check_topic_name(std::string_view topic);
  static void check_pattern(std::string_view pattern);

  // 目标集与类型绑定预检。
  [[nodiscard]] static bool type_matches(const Subscriber& sub, std::string_view tag);
  std::vector<std::shared_ptr<Subscriber>> match_subs(std::string_view topic, std::string_view tag);
  std::size_t name_matched_count(std::string_view topic) const;
  Topic& topic_for(std::string_view name);
  void check_filter_early(const Subscriber& sub);
  void check_bind_compat(std::string_view topic, std::string_view tag);
  void now_preflight(std::string_view topic, std::string_view tag, NowPolicy policy);

  // 单目标投递与统一失败/完成记账。
  Delivery deliver(const std::shared_ptr<Subscriber>& sub, const Envelope& env);
  void record_failure(const std::shared_ptr<PubState>& pub,
                      const std::optional<Publication::Failure>& failure);
  void pub_complete(const std::shared_ptr<PubState>& pub);
  void target_finished(const std::shared_ptr<PubState>& pub);
  void unit_finished(const std::shared_ptr<PubState>& pub,
                     std::optional<Publication::Failure> failure);
  static std::string failure_error_what(const Publication::Failure& failure);
  static std::exception_ptr cancelled_exc();
  static std::exception_ptr unknown_exc();
  static Publication::Failure failure_of(std::string label, std::exception_ptr error);
  static void count_failed(Topic& topic, const std::shared_ptr<Subscriber>& sub);

  // 建立不可变帧：绑定类型、分配 seq、读取后端时间并更新 retained 快照。
  std::shared_ptr<const Envelope> make_envelope(std::string_view topic,
                                                std::shared_ptr<const void> payload,
                                                const TypeOps* ops);
  std::shared_ptr<const Envelope> make_envelope_bytes(std::string_view topic, std::string_view tag,
                                                      const std::vector<std::byte>& bytes);

  // 后台调度。start_serial 在返回前完成 per-topic 链尾交接。
  static void wait_trigger(const Trigger& trig);
  std::shared_ptr<PubState> start_serial(std::string_view topic,
                                         std::shared_ptr<const Envelope> env, Trigger trig,
                                         std::vector<std::shared_ptr<Subscriber>> targets);
  std::shared_ptr<PubState> start_concurrent(std::string_view topic,
                                             std::shared_ptr<const Envelope> env, Trigger trig,
                                             std::vector<std::shared_ptr<Subscriber>> targets);
  static std::string describe(const std::shared_ptr<Subscriber>& sub);
  [[nodiscard]] static std::vector<std::shared_ptr<Subscriber>> select(
      const std::vector<std::shared_ptr<Subscriber>>& matched, Capability want);

  // 订阅注册、retained 补发与退订。
  void retained_catchup(const std::shared_ptr<Subscriber>& sub);
  std::shared_ptr<Subscriber> register_sub(std::string pattern, SubKind kind, Capability cap,
                                           std::string filter_tag, const TypeOps* ops,
                                           ErasedCallback cb, ByteCallback cb_bytes, int handle);
  void remove_sub(const std::shared_ptr<Subscriber>& sub);
};

// C++ 与 SV 的 publish_now 共用同一实现：同步 nonblocking 段 + skip/defer 策略。
std::shared_ptr<PubState> dispatch_now(Broker& broker, std::shared_ptr<const Envelope> env,
                                       NowPolicy policy);

// Publication::wait(timeout) 与 svdpi 的 pub_wait 共用的超时竞速。
// 须在 weave task / 仿真进程上下文调用。
bool wait_pub_timed(const std::shared_ptr<PubState>& pub, weave::duration timeout);

}  // namespace shuttle::detail
