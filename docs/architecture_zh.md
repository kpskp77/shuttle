# shuttle 架构

写给需要理解或修改 shuttle 实现的人——broker 模型、三种发布路径，以及代码为什么长成
这样。diff 要逐条对照的操作约束清单在 [AGENTS.md](../AGENTS.md)；svdpi/systemc 后端
边界与完整 DPI 协议在 [backends.md](backends.md)；定位与用法在
[README](../README.md)。

## 一切推导所依赖的模型

**仿真进程是协程，不是 OS 进程**（继承自 weave）：任一时刻只有一个进程在执行 C++
侧代码，它不会被抢占，只会在 weave 的显式阻塞点挂起。本文档里每条保证都从这条模型推
出；抓住它的读者可以自己推出没写出来的规则。

模型的另一半是 shuttle 在之上的增量：**发布 = 快照 + 预约交接**。一次发布调用在那一刻
锁定载荷与目标集合（一个不可变的*帧*），随后每个目标的帧被交给一个入口能力在注册时
就已定死的回调入口。帧在调用之后不再有任何变化；框架唯一要回答的问题是每个目标的
回调何时、经哪个入口执行，以及这次尝试落在哪个终态。仿真时间是稀缺资源，所以真正
有意思的设计决策全都围绕"哪些回调可以消耗仿真时间、允许跑在哪里"。

下文到处用到的推论：

- broker 状态（`topics` / `subs` / `pubs`）是无锁纯数据；任何中间没有挂起点的
  check-modify 序列都是原子的。`shared_ptr` 引用计数跨语言边界是安全的，理由相同。
- 所有"后台"投递都跑在 weave 任务里；broker 不引入线程。先进先出顺序与跨订阅者并行
  是 weave 调度器的性质，不是 broker 代码的性质。
- 必须活过挂起点的状态，在挂起点*之前*就要一致——从那一刻起，别的进程可能来读它。

## 边界与契约

```
                      ┌──────────────────────────── user code ────────────────────────────┐
                      │  C++: Publisher<T> subscribe<T> Publication Subscription Trigger    │
                      │  SV : shuttle_pub#(T) shuttle_subscribe shuttle_sink(_nb) handle    │
                      └───────────────┬───────────────────────────────┬───────────────────┘
                                      │ public headers (include/shuttle/*.hpp) │ SV package (sv/shuttle_pkg.sv)
        ┌─────────────────────────────▼──────────┐        ┌───────────▼───────────────────┐
        │  broker core (src/broker/)              │        │ shuttle_pkg.sv                │
        │  Topic table / wildcards / type binding │        │  registry / sub / handle       │
        │  three publish schedulings / retained   │◀──DPI─▶│  trigger / event / pub#(T)     │
        │  C++ API and handles: src/*.cpp         │        │  frame pull + {>>{}} packing   │
        └───────┬───────────────────────▲─────────┘        └───────────────────────────────┘
                │ backend-agnostic        │ backend boundary (5-entry detail::backend facade,
     ┌──────────▼─────────┐   ┌──────────┴─────────┐          src/backend/backend.hpp)
     │ backend/svdpi/     │   │ backend/systemc/   │
     │ bridge + DPI face  │   │ no SV bridge:      │
     │ FrameContext+scope │   │ dispatch* throws   │
     └──────────┬─────────┘   └────────────────────┘
                ▼
        weave (task/event/port; both backends)  ← concurrency, single-active-process contract
```

- broker 核心不认识 DPI 也不认识 SystemC。它只通过 `src/backend/backend.hpp` 的 5 入口
  `detail::backend` 门面接触外部（`available` / `dispatch` / `dispatch_nb` /
  `note_retired_delivery` / `take_retired_delivery`）。时间**不在**这个门面里：
  `Meta::time_fs` 与所有超时算术直接读 `weave::now()`。
- `src/backend/<backend>/` 各目录互不 include；没有 `SHUTTLE_BACKEND_*` 宏。换后端 = 换
  目录（由 `xmake.lua` 的 `add_files("src/backend/<backend>/**.cpp")` 选择）。svdpi 是
  后端隔离的唯一例外：它是唯一带 FFI 通道的后端，其 `facade.cpp` 额外可见 broker 内
  部（`broker/broker.hpp`、`detail/publication.hpp`）。永远不会有第二个 FFI 调用方。
- C++ ↔ SV 边界本身——载荷传递、作用域、import/export 表、线上格式——的权威定义在
  [backends.md](backends.md)。
- `include/shuttle/detail/abi.hpp` 是第二公开层（稳定 ABI）：随包安装、随版本管理，但
  用户代码只经由 `publisher.hpp` / `subscription.hpp` 间接包含它，因为公开模板
  （`Publisher<T>` / `subscribe<T>`）必须在用户翻译单元里看到 `pub_*` / `sub_*` 的声
  明。
- `mailbox.hpp` 与 `requester.hpp` 是纯头文件组件：无 `.cpp`、无导出符号、broker 里零
  状态。它们的状态住在头文件对象自己身上。

## 术语

| 术语 | 含义 |
|---|---|
| frame | 一次发布产生的不可变快照：载荷 + `Meta{topic_name, type_tag, topic_seq, time_fs, corr_id}`。三种发布方式都先建帧 |
| target | 本帧匹配到的一个订阅者。目标集在发布调用时由注册表快照一次定完 |
| entry | 订阅者被调用时走的唯一入口：`blocking`（可消耗仿真时间）或 `nonblocking`（零延迟） |
| delivery | broker 把一帧配给一个目标的一次尝试；恰好落在一个终态（下表） |
| delivery unit | 投递的后台执行者：`serial` 每帧一个 weave 任务，`concurrent` 每订阅者一个 |
| chain | 先进先出排序结构：`serial` 与 `now` 的 defer 共用每 topic 一条（`Topic::chain`）；`concurrent` 每 `<topic, subscriber>` 一条（`Topic::sub_chains`） |
| terminal state | 一个目标对一帧的结局：delivered / retired / failed / deferred / skipped |
| bookkeeping | 上述终态的每帧 / 每订阅者 / 每 topic 计数器 |

SV 侧枚举与 C++ 镜像（`Capability::blocking` ↔ `SHUTTLE_CAP_BLOCKING`）；SV 侧 blocking
入口是 export task，nonblocking 入口是 export function。

## 关键设计决策

### 一次注册 = 一个订阅者 = 一个入口

订阅者走哪个入口，由注册时调用的函数决定：C++ `subscribe(fn)` / SV `shuttle_subscribe`
= blocking 入口；C++ `subscribe(nb(fn))` / SV `shuttle_subscribe_nb` = nonblocking 入口；
`serve(...)` 同样一分为二。公开 API 里没有任何运行时的 `Capability` 参数，也永远不用
`$cast` 探测类实现了哪些 interface class 来自动登记两个入口。

为什么：入口是*注册*的属性，不是类的属性，而且必须在调用处可见。运行时参数会让入口
在调用处隐身，并让两种语言里的 `subscriber_count()` / `publish_now` 行为错位。需要两
个入口的订阅者注册两次。被否决的备选——按类自动登记两个入口——被删掉，正是因为它把能
力变成了类级属性，相对 C++ 模型是范畴错误。

可观察后果：`Subscriber` 只存一个 `Capability cap`，`deliver` 只读它。SV 桥的 `caps`
参数只接受 1 或 2，其余拒绝。

### blocking / nonblocking 描述的是回调，不是调用

`blocking` / `nonblocking` 说的是*回调*能否消耗仿真时间——不是入口是否阻塞它的调用
方。`publish_now` 的同步段只调用 nonblocking 入口；在默认 `defer` 策略下，同一发布的
blocking 目标仍会执行，但从后台链上、在 `publish_now` 返回之后。SV 侧两个入口分别是
export task 与 export function。

`NowPolicy` 只作用于 blocking 目标：

| 策略 | 行为 | 时间后果 |
|---|---|---|
| `reject` | 抛 `CapabilityError`（C++）/ 返回 null + last_error（SV） | 该帧从未发布：`seq`、retained 与计数器都不前进 |
| `skip` | 丢弃这些目标，计入 `skipped()` | 其余目标不受影响 |
| `defer`（默认） | 把整帧交给该 topic 的串行链 | 这些回调在 `publish_now` 返回后执行 |

对 nonblocking 目标，`skip` 与 `defer` 无法区分：都在 `publish_now` 返回前完成。底下
的硬约束是 DPI：export *task* 不能运行在 import *function* 调用链里
（`Error-[DPI-INCDIC]`），所以 `dispatch_now` 先按能力把目标分组，同步循环里只有
nonblocking。

### 串行链的链尾在发布时交接——或在触发时交接

`publish_serial` 与 `publish_now(defer)` 共用每 topic 一条先进先出链，由此得到可测定
律：**同一 topic 的后台投递顺序 = 入队顺序**——立即触发帧在发布调用内入队，延迟帧在
触发时刻入队。`publish_concurrent` 在同一规则上用每 `<topic, subscriber>` 一条链：订
阅者并行执行，每个订阅者的连续帧严格先进先出。两条链互不混入：`serial` 帧永不进入
per-订阅者链，反之亦然，跨发布方式之间不保序。

交接本身不可交换，必须发生在同一个不挂起的段内。每帧捕获自己要等待的链尾事件，并装
上自己的作为新链尾：

```cpp
// 立即帧：交接发生在发布调用内
auto prev = std::make_shared<weave::Event>(topics[topic].chain);  // 本帧等待的链尾
auto mine = std::make_shared<weave::Event>();                     // 本帧释放的链尾
topics[topic].chain = *mine;                                      // 安装

// 延迟帧：先 wait_trigger，任务内做同样的三步
```

投递单元等待 `prev`，完成时 set `mine`——无论成败。最初的 bug 是在*完成*时读
`topics[topic].chain` 而不是在入队时捕获：一次并发发布装上了别帧的链尾，本帧该 set 的
事件永远等不到，之后该 topic 的每一帧都永久阻塞——表现为仿真时间停在最后一个事件
处、无报错。`mine->set()` 在 `catch` 之外，所以失败或被取消的单元照样释放链。

延迟帧不许在发布瞬间占住链尾。`after(Event)` 这类延迟是无界的；占住链尾会把同 topic
后续的每一帧（包括根本没有触发器的帧）一起扣住，`publish_serial(...).wait()` 因此永
久死锁——最糟的情况是触发器由*排在后面的同 topic 帧的回调*满足，构成真死锁。
`tests/trigger_tests.cpp` 的 case8/case9 钉住这两条定律；case10/case11 钉住
per-订阅者变体。

串行链的代价是跨订阅者的队头阻塞：一个订阅者的回调必须完全跑完，下一个才开始。这是
显式接受的代价；逃生通道是 `publish_concurrent`——只保留 per-订阅者先进先出，放弃跨
订阅者顺序。

链尾是 `shared_ptr` 持有的 `ChainLink{ev, owner}`，因为等待方必须看到**活值**——按值
拷贝会把 `owner` 冻在交接那一刻。`owner`（将要 set 事件的任务 id）由投递单元自己开
跑时写入、完成时先清成 −1 再 `ev.set()`；发布方代填不了，因为它不知道投递任务是
谁。`Broker::remove_sub` 会擦掉该订阅者的链尾槽，退订永远不会让链尾事件堆在 `topics`
里。

### 自等待在等待处诊断，不在入队处

在回调里对本 topic 发布并 `wait()` 新帧，在任何先进先出设计下都死锁——`serial` 里内
层帧排在本帧之后；`concurrent` 下只要目标集包含发布者自己的注册，同理。诊断依赖
`ChainLink::owner`：`detail::check_self_blocked` 在挂起**之前**运行于
`Publication::wait()` / `wait(timeout)` / SV `shuttle_c_wait_pub`，当 `blocked_by` 中任
何链的 owner 是当前任务时抛错。

为什么放在等待处：对本 topic 只发布不等待（中继/转发）是合法用法，而那时链尾确实由
当前回调的投递任务持有——同一个观察在入队处判定必然误报。C++ 抛
`weave::DeadlockError`（记为常规 `Failure`）；SV 返回 `SHUTTLE_WAIT_FAILED`，诊断文本
在 `shuttle_c_last_error()`。仍然诊断不到的是两个*无关*任务之间的环——链所有权只能证
明"在等自己"，证明不了任意等待图。

### 退订立即生效；在途投递记为 retired

快照只保证*谁被选中*，不保证*一定会被调用*。两个来源都必须记成 `retired` 终态（计入
`dropped`、永不计入 `delivered`、不产生 `Failure`）：

- C++ 侧：`Broker::remove_sub` 置 `Subscriber::removed`；`deliver` 在 kind 分发**之前**
  检查该标志。这个检查是必须的，因为快照持有 `shared_ptr`——漏了这一查，已退订的回调
  真的会执行。
- SV 侧：`shuttle_registry` 用 `retired_[id]` 静默丢弃（只有从未登记过的 id 才
  `$error`），丢弃时调 `shuttle_c_sub_retired`；`Broker::deliver` 在 export 返回后通过
  `take_retired_delivery()` 把这次投递转成 retired。

两个来源不相交、谁也替代不了谁：C++ 标志看不到 SV 侧 `shuttle_sub::unsubscribe()`（它
直接删注册表条目），SV 报告也覆盖不到 C++ 订阅者。早期只有 SV 来源产生 `retired`、
C++ 却真的投递给已退订回调——一种后端专属终态，属于范畴错误。反向方案（SV 侧也保住
帧）被否决：它需要跨语言在途引用计数、以及 `unsubscribe()` 返回后仍在跑回调，两者都
比丢一帧更糟。

### 目标集 = 名字 ∩ 类型，快照时定死；类型不相交即非目标

目标选择在发布调用时一次完成（`Broker::match_subs(topic, tag)`，把
`Subscriber::filter_tag` 与帧的 `meta.type_tag` 求交）。类型不相交的订阅者不是目标：
没有 `target_count`、没有投递单元、没有 `dropped`，也不出现在 `subscriber_count()`。
所以 `deliver` 从不复检标签。

为什么：`subscribe<T>("uart.*")` 订阅的是 *`uart.*` 下的 T 类事件*；别的类型的帧从来
不是它的帧。写错的标签更早暴露——订阅时由 `Broker::check_filter_early`、首次绑定时由
`Broker::check_bind_compat` 抛 `TypeMismatchError`；后者存在是因为 SV 订阅者带标签却不
声明绑定，没有它的话，写错标签会按注册顺序静默把订阅者排除在每一帧之外。`deliver`
里的最后一道防线：`raw_ops` 帧（topic 未绑定、纯字节）遇到 typed 订阅者时抛
`PublishError`，而不是把 `vector<byte>` 强转成 `T`。

### 完成推进只有一处

`Publication::finished()` 的含义是"到达终态的目标数"，唯一推进点是
`Broker::target_finished()`：

```cpp
void target_finished(const std::shared_ptr<PubState>& pub) {
  if (++pub->finished == pub->target_count && !pub->done_flag) pub_complete(pub);
}
```

每条路径——投递成功、retired 跳过、回调抛异常、串行单元的单元级失败（未尝试的目标
也要补齐计数）——都必须走它。不存在"在单元末尾一次性把 `finished` 置成
`target_count`"的分支；那种写法会让 `done()` 在真实完成之前成立。`pub_complete()` 先跑
钩子再 set `done_ev`，所以被事件唤醒的等待方永远看不到"SV 进程还没被唤醒"的中间态。
零目标的发布立即完成（`publish_serial` 也要短路）。所有"没收到"都必须可数：策略
`skip` 落 `skipped()`、快照后退订落 `retired()`、类型不相交的根本不进目标集。

五种终态：

| 终态 | 含义 | 计在 |
|---|---|---|
| delivered | 回调完整执行（SV：export 返回） | `Topic::delivered`、`Subscriber::received` |
| retired | 没进回调、无人过错：快照后、回调前被退订 | `Publication::retired()`、`Topic::retired`、`Subscriber::retired` |
| failed | 投递未干净完成：回调抛异常、帧物化失败、被取消、单元级失败 | `Topic::failed`、`Subscriber::failed`、`failures()` |
| deferred | 返回时还没启动（仅 `now` 的 defer 段）；之后落入上面三者之一 | `Publication::deferred()`（一个标记） |
| skipped | 没进回调、无人过错：被策略从帧级目标集移除（仅 `now` + `skip`） | `Publication::skipped()`、`Topic::skipped`、`Subscriber::skipped` |

记账不变量（topic 与订阅者两级都是 `dropped = retired + skipped + failed`）：

1. 三种方式下都无例外：`done()` 当且仅当 `finished() == subscriber_count()`；
2. `subscriber_count() + skipped()` = 本帧的目标订阅者数（名字 ∩ 类型，减去策略跳过）；
3. `finished()` 只计 delivered + retired + failed；故 `finished() ≤ subscriber_count()`；
4. 订阅者级 `received() + dropped()` 计到每一帧名字 ∩ 类型匹配上它的帧——注意帧级与订
   阅者级的分母差一个 `skip` 步：`dropped()` 计入策略跳过的帧，`subscriber_count()`
   不计。

### 通配符只属于订阅模式

发布、`topic_declare`、`topic_set_retained` 只收具体名字；`>` 必须独占模式的末段、
`*` 必须独占某一段；两侧都拒绝空段与空字符串（`TopicPatternError`）。校验在**建任何
状态之前**运行。

为什么发布侧必须拒绝：路由是字符串比对，允许 `publish_serial("uart.*")` 就会在
`topics` 表里立起一个名叫 `uart.*` 的幻影 topic——有自己的 `seq`、绑定与 retained
槽——并且真的会扇出给 `uart.*`、`uart.>`、`>` 三类订阅者。看着像向子树广播，其实是
名字巧合，`uart0.*` 的订阅者收不到它。更糟的是 `check_bind_compat` 会跳过通配符订阅
者，所以连标签写错都不会暴露。为什么订阅侧必须校验：`match` 遇到 `>` 就放行剩余模
式，所以 `a.>.b` 静默退化成 `a.>`；`a.rx*` 只能匹配字面量 `a.rx*`。两者都是"注册成
功、永远收不到"的静默。

只读查询（`topic_retained`）不校验：非法名字读作"从未绑定"。`now_preflight` 里名字
校验排在策略判定之前——同一个非法名字不该因 `NowPolicy` 不同而换报错。

### `topic_name` 与 `topic_seq` 是两条正交轴，发布方看不到自己的 seq

`Meta::topic_name` 回答"哪条信道"（一个 topic 的所有帧共享，所以不能标识一帧）；
`Meta::topic_seq` 回答"这条信道上的第几帧"（按 topic 从 1 编，所以 `a.1` 与 `b.1`
并存）。两者合在一起在 broker 存活期内标识一帧。哪个都缺不得：

- 单一全局序号让同一 topic 相邻帧之间的间隔失去意义，而所有计数器都是按 topic 的，
  `topic_seq` 存在的意义恰恰是能与它们对账（跳号 = 丢帧；`serial` 链上必须严格递增）；
- 光有名字无法区分帧——retained 补发帧与实时帧无法分辨（迟到的订阅者先看到*旧的*
  `topic_seq`，随后实时帧继续：这个跳跃就是"补发的历史"信号）；
- 名字本身也去不得：一个通配符注册匹配多个具体名字，回调必须知道具体来源；
  `topics` 表以它为键。帧永远携带具体名字；模式从不进入 `Meta`。

`topic_seq` 原样跨过 DPI（`longint topic_seq`），C++ 日志与 SV `$display` 以
`(topic_name, topic_seq)` 对齐。它永远不能充当请求-应答关联号，因为发布方看不到自己
帧的 `topic_seq`（见下面的关联号决策）。字段名带作用域前缀，正因为裸 `seq` 曾被误读
为全局。

### `publish_now` 有重入上限

因为回调跑在发布者栈上，订阅者再次 `publish_now` 是递归而非挂起；两个互相发布的订
阅者会在没有任何诊断的情况下爆栈。`dispatch_now` 带一个 `thread_local` 深度计数
（`kMaxNowDepth = 8`），把环变成 `PublishError`。这个上限是诊断天花板，不是策略：合
法嵌套（请求帧 → 响应者 handler → 回执帧）到 2–3 层。同步段内单个回调失败不终止该
帧——记入 `failures()` 与 `first_error`，循环继续，首个异常等所有目标处理完后才抛回调
用方。SV 侧 `shuttle_c_publish_now` 把异常转换成返回码加 `shuttle_c_last_error()`。

### 回执走 `publish_now`，且回执 topic 拒绝 blocking 旁观者

`Requester` 与 `serve` 会把回执 topic 标成响应通道（`Topic::carries_responses`）；
此后该 topic 上任何 *blocking* 入口的注册都在注册时被 `Broker::check_response_topic`
拒绝（`CapabilityError`；通配符模式按匹配判定——`dev.resp` 上注册一个 blocking 的
`dev.>` 旁观者同样失败）。理由：blocking 旁观者持有该 topic 的串行链，此后每一帧回执
都排在它后面，而请求方**无法区分"响应者死了"与"回执排在慢的旁观者后面"**——超时会撒
谎。注册期拒绝是上面入口决策的早失败对应物；投递时检查就晚了。名字本身仍然公开
（`resp_topic()`、跨语言硬编码字符串都合法），手工往回执 topic 发布也合法；只有
blocking *旁观*被禁。标记之前注册的 blocking 旁观者不做追溯拒绝——`publish_now` 路
径已经让它们无害。

回执用 `detail::pub_now`（SV `shuttle_pub::reply` 同理），绝不走 `publish_serial`：
配上 `nb(handler)` 响应者与 `send_now()`，整个往返跑在请求方栈上——`send_now()` 返
回时回执已在 `Call` 里（`call.queued() == 1`），零往返时延、不消耗仿真时间。
`NowPolicy::defer` 只是给标记之前注册的 blocking 响应者兜底。因为回执不占链尾，旁观
者再慢也拖不动请求方。

### 关联号住在帧头，不住在载荷里

`Meta::corr_id` 是请求-应答关联号：请求方分配、响应方回显、用来把回执路由回它的
`Call`。它是 `Meta` 里唯一业务可写字段（其余由 broker 填）；`0` 表示"非关联交换"。
公开发布三件套不暴露它——`Publisher::publish_*` 永远发 `corr_id = 0`；只有
`Requester::send_*`、`serve` 与 SV `shuttle_pub::reply` 会写它（经由内部
`detail::pub_*` 的尾参数）。

为什么不放进载荷——三条承重的理由：

- 发布方看不到自己的 `topic_seq`，所以源自 seq 的 id 没法在发布前写进载荷；
- 顺序要求 id 在发布*之前*就已入表：`send_now` + nonblocking 入口意味着响应者在发
  布者栈上同步回执——晚一步，回执就成了陈帧（`stale_responses()` 增长）；
- `topic_seq` 是按 topic 的记账（`a.req` 与 `b.req` 都从 1 开始，而 `serve` 支持通配
  请求 topic），暴露它会让 id 碰撞，把一个诊断字段变成 ABI 契约。

计数器是进程级普通 `static`（单活跃进程模型下成立），所以两个共享回执 topic 的
`Requester` 实例不会撞号。回执表存 `weak_ptr`；`Call` 持有唯一强引用，丢掉句柄（或
`cancel()`）即释放槽位，迟到回执落进 `stale_responses()`——可数，永不静默错投。没有这
个 id，乱序回执会错配（两个在途请求、先进先出顺序、答案互换而数值恰又相同时无法检
出——由 `requester_tests::case2` 钉住）；三目标请求无法回答"还剩几帧回执"；超时请求的
迟到回执会污染下一个请求；SV 响应方也没有整数可回显。

`wait_all()` 的期望回执数是一条约定，不是知识：框架默认"请求帧的每个目标恰好回一
帧"（`publication().subscriber_count()`）；存在不应答的旁观订阅者时用 `wait_for(n)` /
`recv`。请求 topic 与回执 topic 必须不同——响应者在请求回调内发布回执，共享 topic 会
让回执排在发布它的回调后面；构造时抛 `std::invalid_argument`。

### SV 请求者是同一套设计，只有计数器与标记跨 DPI

`shuttle_requester #(TReq, TResp)` / `shuttle_call #(TResp)` 与 `Requester` / `Call` 逐项对应：
调用槽先登记、再发帧（nonblocking 响应者在发布调用内就作答），`wait_for` 用一个截止时刻
约束整段等待，`wait_all` 从请求帧的 `subscriber_count()` 取期望回执数。有两件东西放不进
SV，于是变成 DPI 入口：`shuttle_c_take_corr_id`（计数器是进程级 C++ 状态，另起一个计数器
会让同一响应 topic 上的 SV 调用与 C++ 调用拿到同一个 id）与 `shuttle_c_mark_responses`
（`Broker::mark_response_topic` 是 broker 状态，SV 请求者必须像 `Requester`/`serve` 一样
先认领自己的响应通道）。

不同的地方是语言，不是设计：SV 没有异常，所以构造失败留在 `valid()` 为假 + `last_error()`，
`send_*` 返回空调用而不抛异常，`recv`/`wait_*` 对超时与取消都用 `ok == 0` 作答（那里没有
`TimeoutError` / `CancelledError`）。分发需要一个可持有句柄的订阅回调，而队列属于参数化类，
所以 `shuttle_reply_target` 是一个非参数化基类，请求者覆写它的 `accept_response`。请求直接
走发布 DPI 入口而不经 `shuttle_pub`——公开三件套故意没有 `corr_id` 参数，这也正是
`shuttle_pub::reply` 绕开它的原因。取消是显式的（`cancel()`）：SV 没有 `weak_ptr`，调用方
只是丢掉的槽不可见，`prune()` 只能清掉已取消的调用，它们的回执计进 `stale_responses()`。

### Mailbox = 订阅者 + 队列，不是新的投递机制

`Mailbox<T>` 组合两个既有能力：经由普通 `detail::sub_typed` / `sub_raw` 注册得到的
订阅者身份（入口**固定为 nonblocking**），外加自己的 `std::deque` + `weave::Event`。
broker 不增加状态、不增加导出符号——所以是纯头文件实现。入口必须 nonblocking，因为
邮箱回调只做一次拷贝入队，而它跑在 `publish_now` 的同步段（发布者栈）里；blocking 邮
箱回调会死锁发布者。有用的推论：`publish_now` 同步填满邮箱，所以对一个纯 nonblocking
对端的请求是零往返时延完成的。

有界邮箱必须在构造时选定 `drop_oldest` / `drop_newest` / `reject`；
`OverflowPolicy::block` 被构造时拒绝，因为阻塞投递者等于死锁发布者。`overflowed()`
（"已入队、随后被丢"）与 `Subscription::dropped()`（"从未投递到该订阅者"）是两本账；
`reject` 复用常规 `Failure` 通道，不发明新记账。close 幂等（退订 + 唤醒等待者），且
close 后队列仍可读出。SV 邮箱（`shuttle_pkg::shuttle_mailbox`）是纯 SV，基于
`shuttle_subscribe_nb` 与 weave 的 `weave_sv_wait` / `weave_sv_trigger`——没有新的 DPI
入口；SV 没有异常，所以只有 `drop_oldest` / `drop_newest`。

邮箱是时序原语，不是 RPC 原语：无关联号的单响应者交换可以在它上面搭（演示正是这么
做的），但 `Requester` 是它的*兄弟*，不是它的用户——requester 直接复用同样的订阅原
语，并不 include `mailbox.hpp`。

### 跨语言帧的载荷走反向拉取

C++→SV 投递不把载荷作为 DPI 实参传递：C++ 侧把编码后的帧停放在进程级 `FrameContext`
（一个 RAII 保存/恢复栈，可安全嵌套），SV export 函数体用 `shuttle_c_frame_len()` +
`shuttle_c_frame_qword(i)` 反向拉取。起因是 VCS 2018.09 的 `svdpi.h` 缺
`svCreateArray`——C++ 构造不了 open array——外加调用方栈上实参的生命期问题。安全前提
是单活跃进程契约：export 执行期间没有别的进程能覆盖停放中的帧。完整协议——
import/export 表、线上格式、作用域规则——是 [backends.md](backends.md) 的主题；这里
只记录一条决策：桥依赖端口契约，端口契约永远不依赖桥。

### 延迟触发只传边沿名，永不传事件 id

`SHUTTLE_TRIG_EDGE` 的 `trig_name` 是 weave 边沿**名字**；C++ 侧用
`weave::sv::Edge(name)` 在 weave 自己的名字表里 find-or-insert，两侧因此共享同一个
id。shuttle 不持有任何事件对象、也没有按 id 恢复的入口——早年的
`shuttle_event` + `shuttle_c_event_*` + `SHUTTLE_TRIG_EDGE` 影子事件系统已删、不得复
活：id 一旦跨 DPI，就得有人持有"id → 事件"表，并因此失去早失败与所有权单一。等待走
`Edge::wait()` / `Pulse::wait()` 而不是裸 `weave_sv_wait`，以继承 weave 的挂起记账
（取消注入、等待图、"挂起时无当前任务"——最后这条是上面自等待诊断成立的前提）。边沿
语义是边沿而非闩锁：`fire()` 早于等待登记即丢失（要闩锁用 `after(weave::Event&)`）。
VCS 2018.09 保留 `edge`，所以 SV 侧字段名是 `edge_name`。

### 继承逐层委托，不在叶子上摊平

`SHUTTLE_REFLECT_DERIVED(Derived, Base, 自有字段...)` 注册 `Derived` 时把基类子对象整体交给
`Codec<Base>` 编解码，不重复继承来的字段名；SV 的对偶是在派生类的三件套块里写
`` `shuttle_base(Base) ``。两种写法与在叶子摊平全部字段产生同样的字节：先基类编码，再自有
字段——因为 C++ 的基类子对象排在派生成员之前，而 codec 按声明顺序输出字段。应当写委托形式：
深度为 *n* 时摊平写法把继承名重复 *n* 次，而在第 *k* 层新增一个基类字段要改动它的全部后代。
委托下每层只写一次直接基类，链式组合，基类改动只落在基类自己的注册处。两种写法可互换正是因为
线格式相同：摊平注册的叶子与委托注册的叶子互相解码，已有帧无需迁移。

约束来自机制而非偏好：每层恰好一个公有基类（编码侧把 `Derived` 当 `Base` 传，解码侧经
`static_cast<Base&>` 赋值）；派生注册出现时 `Codec<Base>` 必须已注册；只做透传、不带自有字段
的中间层需要手写 `Codec` 特化，因为字段宏无法展开成空。SV 侧 `` `shuttle_base `` 的解包腿先把
派生句柄拷进基类型局部变量：VCS 2018.09 拒绝把派生句柄接到 `ref` 形参（`Error-[IRPC]`），
而拷贝是安全的——生成的入口在进入 `__codec` 之前已经构造好句柄所指的对象。

### `Meta` 的字符串是视图，`pubs` 只登记外部发布

`Meta::topic_name` / `Meta::type_tag` 是指向 broker 已有且永不迁移存储的
`std::string_view`（`topics` 的键与 `Binding::tag`），所以建帧零头分配，拷贝
`Meta` 只是两对（指针, 长度）。它们不以 NUL 结尾：DPI export 表需要 `const char*`，
所以 `src/backend/svdpi/bridge.cpp` 只在 SV 投递路径上物化 `std::string`；纯 C++ 用法
不受影响。用户写 `meta.topic_name == "dev.tx"` 与
`std::string s{meta.topic_name}`——拷贝初始化 `std::string s = meta.topic_name;` 编不过
（`string_view` → `string` 构造函数是 explicit）。代价是 ABI：`Meta` 不再能被用户代
码用字符串聚合初始化，旧二进制必须按新 `libshuttle.so` 重编。

`pubs` 表只登记*外部*（SV）发布，因为 SV 句柄只知道一个 `pub_id`，必须解析回
`PubState`；C++ 的 `Publication` 自己持有状态，把每帧都插进去会让 `pubs` 无限增长。
它单调增长（`pub_id` 不复用）；长时间运行的 SV 侧高频回归仍需要周期清理策略。

### 句柄生命期

`Publication` 是可拷贝的 `shared_ptr<Impl>`。`Subscription` 是 `shared_ptr<Impl>`，
由*最后一次*析构退订（`Impl::~Impl` → `Broker::remove_sub`）；`unsubscribe()` 幂等。SV
的 `shuttle_sub` 是 id 对加一个 `active_` 标志，析构不退订（SV 没有确定性析构），SV
的 `shuttle_handle` 通过 `pub_id` 镜像 `Publication` 的方法。"退订立即生效"是双向的：
之后的帧不再选中该订阅者，已快照的帧把它跳过并记为 retired。

## 公开面的分层

依赖向下指；`shuttle.hpp` 是伞头，定义什么是公开的：

```
shuttle/types.hpp        Capability / SubKind / NowPolicy / OverflowPolicy / errors
shuttle/meta.hpp         Meta{topic_name, topic_seq, type_tag, time_fs, corr_id}
shuttle/codec.hpp        Writer / Reader（大端）+ SHUTTLE_REFLECT / SHUTTLE_REFLECT_DERIVED
shuttle/trigger.hpp      Trigger + after/after_all/after_any 工厂
shuttle/publisher.hpp    Publisher<T>（此处可见 pub_* ABI；拉入 detail/abi.hpp）
shuttle/subscription.hpp subscribe<T> / subscribe_bytes / Subscription（拉入 detail/abi.hpp）
shuttle/publication.hpp  Publication（shared Impl 之上的句柄）
shuttle/mailbox.hpp      Mailbox<T> —— 纯头文件，broker 零状态
shuttle/requester.hpp    Requester / Call / serve —— 纯头文件，broker 零状态
shuttle/broker.hpp       只读 introspection（shuttle::broker::topic_info …）
shuttle.hpp              伞头
```

安装出来的 `include/shuttle/detail/` 携带 `abi.hpp`（模板需要的稳定第二层）、
`wait.hpp`（`detail::deadline_after` / `detail::wait_wake`，mailbox / requester /
`Publication::wait` 三处"等事件、可能带超时"竞速共用），以及反射支持。`xmake.lua` 的
头文件 glob 必须保持 `include/(shuttle/**.hpp)`：更窄的 glob 会把 `detail/abi.hpp` 从
安装包中静默丢掉，用户一 include `publisher.hpp` 就编译失败。

## 范围

shuttle 提供仿真进程内的类型化发布-订阅投递调度，C++、SystemVerilog 与 DPI 跨界都
算。明确不提供：

- 基础模型里的 RPC 配对（mailbox/requester 一节的请求-应答糖是组合出来的，请求/回执
  topic 是约定，不是 broker 知识）；
- 跨机器通信、持久化与回放；
- UVM 适配层；
- 普通订阅的背压（`SubOptions::max_pending` / `OverflowPolicy` 只有 `Mailbox` 遵守）；
- shuttle 自己的时钟——仿真时间是 weave 的（`weave::now()`），刻意不设
  `shuttle::now_fs()` 式转发包装（曾经有过，已删）。

已知限制：SV 侧 `$error` 不传回 C++（协议后果；严格错误通道需要扩展 `dispatch` 的返
回值）；跨语言载荷是值语义、字段子集——无 variant、指针、引用或句柄；
`TopicInfo::deferred`（topic 累计）与 `Publication::deferred()`（每帧）在两个粒度上共
用一个名字。

## 写给大模型的设计哲学

shuttle 假定 LLM 懂发布-订阅消息与 asyncio 式调度，但不懂 EDA 仿真语义；框架的职责
是把仿真特有的部分——哪些回调可以消耗仿真时间、只有一个 CPU 时"投递顺序"到底能是
什么——显式化、可计数。要推广的泛化原则：*永远*不要在"入口能力 + 队列 + topic 约定"
的组合能表达需求时引入第二种通信机制；*永远*不要让计数器里看不到的终态存在；*永远*
不要为纯头文件组件加 broker 状态。过去每次违反这三条，都长出一个影子系统（一张并行
事件表、一种后端专属终态、一只重复时钟），后来都得删掉。
