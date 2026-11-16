#pragma once

// Trigger 的内部构造通道：仅 src/ 使用，不随包安装。
//
// 公开工厂是 shuttle::after() / after_all() / after_any()；这里的 from_fn 让桥接层
// （svdpi 装配 SHUTTLE_TRIG_EVENT / SHUTTLE_TRIG_PUB）能用任意 std::function 直接构造
// Trigger。Trigger 的构造函数是私有的，故经 detail::TriggerAccess 这个具名友元进入。

#include <functional>
#include <utility>

#include <shuttle/trigger.hpp>

namespace shuttle::detail {

struct TriggerAccess {
  static Trigger from_fn(std::function<void()> fn) { return Trigger(std::move(fn)); }
};

}  // namespace shuttle::detail
