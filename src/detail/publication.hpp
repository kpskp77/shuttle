#pragma once

// Publication 的私有实现与具名构造通道（不安装）。
//
// 公开头只持有不透明的 shared_ptr<Impl>；broker API 与 svdpi 门面需要把 PubState
// 包装成句柄或反查 pub_id，因此共享这一小块定义，而不必看到完整 Broker 实现。

#include <memory>
#include <shuttle/publication.hpp>
#include <utility>

#include "broker/model.hpp"

namespace shuttle {

struct Publication::Impl {
  std::shared_ptr<detail::PubState> pub;
};

// publication.hpp 中声明的友元：统一构造/反查，避免各入口直接触碰 impl_。
struct detail_pub_access {
  static Publication make(std::shared_ptr<Publication::Impl> impl) {
    return Publication(std::move(impl));
  }

  static Publication make(std::shared_ptr<detail::PubState> pub) {
    auto impl = std::make_shared<Publication::Impl>();
    impl->pub = std::move(pub);
    return Publication(std::move(impl));
  }

  static std::shared_ptr<detail::PubState> state(const Publication& publication) {
    return publication.impl_ ? publication.impl_->pub : nullptr;
  }
};

}  // namespace shuttle
