#pragma once

// shuttle —— 基于 weave 的 EDA 验证环境发布-订阅框架。
//
// 特性概览：
//   · 三种发布方式：`now` 同步回调（零挂起）/ `serial` 后台顺序回调 / `concurrent` 后台并发回调，
//     `serial`/`concurrent` 默认不阻塞发布者，提供 Publication 句柄等待完成，并支持延迟触发（Trigger）；
//   · 跨语言：C++ 与 SV（DPI）对等发布/订阅，消息按统一二进制格式编码（codec.hpp）；
//   · 入口模型：每次订阅声明一条入口（blocking / nonblocking），决定回调能否在发布者调用栈
//     内执行——nonblocking 可以，blocking 只能后台调度；`serial`/`concurrent` 对两种入口都投递，
//     `publish_now` 只用 nonblocking；
//   · topic 层级命名 + 通配符订阅 + retained（latched）；
//   · C++ 内部投递零拷贝，只有跨语言投递、字节订阅者（subscribe_bytes）或来自 SV 的帧才编解码。
//
// 后端：svdpi（跨语言全集）/ systemc（自动退化为纯 C++，API 不变）。
// DPI 协议总表见 docs/dpi_protocol.md。

#include <shuttle/broker.hpp>
#include <shuttle/codec.hpp>
#include <shuttle/exception.hpp>
#include <shuttle/meta.hpp>
#include <shuttle/publication.hpp>
#include <shuttle/publisher.hpp>
#include <shuttle/subscription.hpp>
#include <shuttle/trigger.hpp>
#include <shuttle/types.hpp>
