#pragma once

// 异常体系：遵循与 weave 相同的跨 DSO 纪律 ——
// 每个异常类型的析构在头文件中仅声明、在 src/ 中定义（key function），
// 保证 vtable/typeinfo 只在 libshuttle.so 中发射一次，
// 跨 DSO 的 catch(const X&) 才能按 type_info 指针比较正确匹配。

#include <stdexcept>

namespace shuttle {

// 框架所有异常的基类
class ShuttleError : public std::runtime_error {
public:
  explicit ShuttleError(const char* what_arg) : std::runtime_error(what_arg) {}
  explicit ShuttleError(const std::string& what_arg) : std::runtime_error(what_arg) {}
  ~ShuttleError() override;  // key function：src/exception.cpp 中定义
};

// 发布/投递路径失败：topic 类型绑定冲突、编码失败、帧未物化为 C++ 对象（类型未绑定）、
// 编解码长度超限
class PublishError : public ShuttleError {
public:
  using ShuttleError::ShuttleError;
  ~PublishError() override;
};

// 能力不匹配：`publish_now` 遇到入口为 blocking（可消耗仿真时间）的订阅者
// （NowPolicy::reject 下抛出）
class CapabilityError : public ShuttleError {
public:
  using ShuttleError::ShuttleError;
  ~CapabilityError() override;
};

// topic 的类型绑定冲突：同一 topic 被声明为两种不同的类型标签
class TypeMismatchError : public ShuttleError {
public:
  using ShuttleError::ShuttleError;
  ~TypeMismatchError() override;
};

// topic / 订阅模式的语法错误：发布、类型声明或 retained 用了含通配符的名字，
// 或订阅模式里 ">" 不在末段、出现空段、通配符混进了普通段
class TopicPatternError : public ShuttleError {
public:
  using ShuttleError::ShuttleError;
  ~TopicPatternError() override;
};

// 解码失败：字节流长度不足、尾部多余字节、长度前缀越界等
class DecodeError : public ShuttleError {
public:
  using ShuttleError::ShuttleError;
  ~DecodeError() override;
};

}  // namespace shuttle
