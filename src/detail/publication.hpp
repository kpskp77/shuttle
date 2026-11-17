#pragma once

// Private implementation of Publication and the named construction channel
// (not installed).
//
// The public header only holds the opaque shared_ptr<Impl>; the broker API
// and the svdpi facade need to wrap a PubState into a handle or look up the
// pub_id back, so this small shared definition exists without exposing the
// full Broker implementation.

#include <memory>
#include <shuttle/publication.hpp>
#include <utility>

#include "broker/model.hpp"

namespace shuttle {

struct Publication::Impl {
  std::shared_ptr<detail::PubState> pub;
};

// The friend declared in publication.hpp: unified construction / reverse
// lookup, so entry points need not touch impl_ directly.
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
