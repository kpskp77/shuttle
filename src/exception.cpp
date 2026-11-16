// exception.cpp —— 异常类型的 key-function 析构定义
//
// exception.hpp 中析构均为 out-of-line 声明（key function 惯例，与 weave 相同：
// 保证 catch 类型匹配跨越 DSO 边界按主类型分派）。此处给出定义。
#include "shuttle/exception.hpp"

namespace shuttle {

ShuttleError::~ShuttleError() = default;
PublishError::~PublishError() = default;
CapabilityError::~CapabilityError() = default;
TypeMismatchError::~TypeMismatchError() = default;
TopicPatternError::~TopicPatternError() = default;
DecodeError::~DecodeError() = default;

} // namespace shuttle
