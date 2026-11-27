# shuttle 架构

写给要理解或改动 shuttle 实现的人：broker 模型、三条发布路径，以及代码为什么长成这样。
逐条对照 diff 的操作约束在 [AGENTS.md](../AGENTS.md)；svdpi/systemc 的后端边界与完整 DPI
协议在 [backends.md](backends.md)；定位与用法在 [README](../README.md)。

## 一切推导所依赖的模型

**仿真进程是协程，不是 OS 进程**（这条继承自 weave）：任一时刻只有一个进程在跑 C++ 代
码，它不被抢占，只在 weave 的显式阻塞点挂起。本文的每条保证都能从这里推出来；抓住这条
模型，没写出来的规则大致也能自己推。

模型之外，shuttle 只多了一件事：**发布 = 快照 + 预约交接**。发布调用在那一刻就把载荷与
目标集合锁死（一份不可变的*帧*），之后每个目标的帧都交给它在注册时就已定死的回调入口。帧
在调用返回后不再变化；框架要回答的只剩三个问题：每个目标的回调何时跑、从哪个入口跑、这
次尝试落在哪个终态。仿真时间是稀缺资源，所以真正有意思的决策都在同一个问题上打转：哪些
回调可以花仿真时间，允许在哪里花。

后面到处要用这几条推论：

- broker 状态（`topics` / `subs` / `pubs`）是无锁的普通数据；一段 check-modify 序列只要
  中间不挂起，就是原子的。`shared_ptr` 的引用计数能安全跨语言边界，也是同一个道理。
- 所有"后台"投递都跑在 weave 任务里，broker 不引入线程。跨订阅者并行、FIFO 顺序，都是
  weave 调度器的性质，不是 broker 代码的性质。
- 要活过挂起点的状态，必须在挂起点*之前*就一致——从那一刻起，别的进程随时可能来读。

## 边界与契约

```
                      ┌──────────────────────────── user code ────────────────────────────┐
                      │  C++: Publisher<T> subscribe<T> Publication Subscription Trigger  │
                      │  SV : shuttle_publisher #(T) shuttle_subscribe sink(_nb) handle   │
                      └───────────────┬────────────────────────────────────────┬──────────┘
                                      │ public headers (include/shuttle/*.hpp) │ SV package (sv/shuttle_pkg.sv)
        ┌─────────────────────────────▼───────────┐         ┌──────────────────▼────────────┐
        │  broker core (src/broker/)              │         │ shuttle_pkg.sv                │
        │  Topic table / wildcards / type binding │         │  registry / sub / handle      │
        │  three publish schedulings / retained   │<──DPI──>│  trigger / event / publisher  │
        │  C++ API and handles: src/*.cpp         │         │  frame pull + {>>{}} packing  │
        └───────┬────────────────────────▲────────┘         └───────────────────────────────┘
                │ backend-agnostic       │ backend boundary (5-entry detail::backend facade,
     ┌──────────▼─────────┐   ┌──────────┴─────────┐          src/backend/backend.hpp)
     │ backend/svdpi/     │   │ backend/systemc/   │
     │ bridge + DPI face  │   │ no SV bridge:      │
     │ FrameContext+scope │   │ dispatch* throws   │
     └──────────┬─────────┘   └────────────────────┘
                ▼
        weave (task/event/port; both backends)  ← concurrency, single-active-process contract
```

- broker 核心不认识 DPI，也不认识 SystemC。它与外部只经 `src/backend/backend.hpp` 里那 5
  个入口的 `detail::backend` 门面打交道（`available` / `dispatch` / `dispatch_nb` /
  `note_retired_delivery` / `take_retired_delivery`）。时间**不在**门面里：`Meta::time_fs`
  和所有超时算术直接读 `weave::now()`。
- `src/backend/<backend>/` 各目录互不 include，也没有 `SHUTTLE_BACKEND_*` 宏。换后端就是
  换目录（选谁由 `xmake.lua` 的 `add_files("src/backend/<backend>/**.cpp")` 决定）。后端
  隔离只有一处例外：svdpi 是唯一带 FFI 通道的后端，所以只有它的 `facade.cpp` 额外看得见
  broker 内部（`broker/broker.hpp`、`detail/publication.hpp`）。FFI 不会长出第二个调用方。
- C++ ↔ SV 的边界本身——载荷怎么传、作用域怎么算、import/export 表、字节布局——以
  [backends.md](backends.md) 为准。
- `include/shuttle/detail/abi.hpp` 是第二层公开接口（稳定 ABI）：随包安装，按版本管理。但
  用户代码只经 `publisher.hpp` / `subscription.hpp` 间接包含它，因为 `Publisher<T>` /
  `subscribe<T>` 这些公开模板必须在用户的翻译单元里看见 `pub_*` / `sub_*` 的声明。
- `mailbox.hpp` 与 `requester.hpp` 是纯头文件组件：没有 `.cpp`，没有导出符号，broker 里不
  留状态。状态都在头文件对象自己身上。

## 术语

| 术语 | 含义 |
|---|---|
| frame | 一次发布产生的不可变快照：载荷 + `Meta{topic_name, type_tag, topic_seq, time_fs, corr_id}`。三种发布方式都是先建帧 |
| target | 本帧匹配到的一个订阅者。目标集在发布调用时由注册表快照一次定完 |
| entry | 订阅者被调用时走的唯一入口：`blocking`（可消耗仿真时间）或 `nonblocking`（零延迟） |
| delivery | broker 把一帧配给一个目标的一次尝试；恰好落在一个终态（见下表） |
| delivery unit | 投递的后台执行者：`serial` 每帧一个 weave 任务，`concurrent` 每订阅者一个 |
| chain | FIFO 排序结构：`serial` 与 `now` 的 defer 共用每 topic 一条（`Topic::chain`）；`concurrent` 每 `<topic, subscriber>` 一条（`Topic::sub_chains`） |
| terminal state | 一个目标对一帧的结局：delivered / retired / failed / deferred / skipped |
| bookkeeping | 上面这些终态在每帧 / 每订阅者 / 每 topic 上的计数器 |

SV 侧的枚举与 C++ 一一对应（`Capability::blocking` ↔ `SHUTTLE_CAP_BLOCKING`）。SV 侧
blocking 入口是 export task，nonblocking 入口是 export function。

## 关键设计决策

### 一次注册 = 一个订阅者 = 一个入口

订阅者走哪个入口，由注册时调用的那个函数决定：C++ `subscribe(fn)` / SV
`shuttle_subscribe` 给 blocking 入口，C++ `subscribe(nb(fn))` / SV `shuttle_subscribe_nb`
给 nonblocking 入口；`serve(...)` 同样一分为二。公开 API 里没有运行时的 `Capability`
参数，也永远不用 `$cast` 探一探类实现了哪些 interface class，再顺手登记两个入口。

理由：入口是*注册*的属性，不是类的属性，而且必须在调用处一眼看见。做成运行时参数，入口
就在调用处隐形，两种语言的 `subscriber_count()` / `publish_now` 也会跟着错位。要两个入口的
订阅者注册两次。另一个备选是按类自动登记两个入口，这个方案已经删掉，正因为它把能力变成
了类级属性——在 C++ 的模型里说不通。

可观察的后果：`Subscriber` 只存一个 `Capability cap`，`deliver` 只读它。SV 桥的 `caps`
参数只收 1 或 2，别的一律拒绝。

### blocking / nonblocking 说的是回调，不是调用

`blocking` / `nonblocking` 描述*回调*能不能消耗仿真时间，不是入口是否阻塞它的调用方。
`publish_now` 的同步段只调用 nonblocking 入口；默认 `defer` 策略下，同一次发布的 blocking
目标照样执行，但从后台链上、在 `publish_now` 返回之后。SV 侧两个入口分别是 export task 与
export function。

`NowPolicy` 只对 blocking 目标生效：

| 策略 | 行为 | 时间后果 |
|---|---|---|
| `reject` | 抛 `CapabilityError`（C++）/ 返回 null + last_error（SV） | 该帧从未发布：`seq`、retained 与计数器都不前进 |
| `skip` | 丢掉这些目标，计入 `skipped()` | 其余目标不受影响 |
| `defer`（默认） | 整帧交给该 topic 的串行链 | 这些回调在 `publish_now` 返回后执行 |

对 nonblocking 目标，`skip` 与 `defer` 分辨不出来：两者都在 `publish_now` 返回前做完。真正
的约束来自 DPI：export *task* 不能跑在 import *function* 的调用链里（`Error-[DPI-INCDIC]`），
所以 `dispatch_now` 先按能力给目标分组，同步循环里只剩 nonblocking。

### 链尾在发布时交接——或在触发时交接

`publish_serial` 与 `publish_now(defer)` 共用每 topic 一条 FIFO 链，于是有了一条能验证的定
律：**同一 topic 的后台投递顺序 = 入队顺序**——立即触发的帧在发布调用内入队，延迟帧在触发
时刻入队。`publish_concurrent` 在同一规则上给每 `<topic, subscriber>` 一条链：订阅者之间并
行，每个订阅者自己的连续帧严格 FIFO。两条链互不混入：`serial` 帧永不进入 per-订阅者链，反
之亦然，跨发布方式之间不保序。

交接本身不可交换，必须发生在同一个不挂起的段里。每帧捕获自己要等的那个链尾事件，再把自己
装成新的链尾：

```cpp
// 立即帧：交接发生在发布调用内
auto prev = std::make_shared<weave::Event>(topics[topic].chain);  // 本帧等待的链尾
auto mine = std::make_shared<weave::Event>();                     // 本帧释放的链尾
topics[topic].chain = *mine;                                      // 安装

// 延迟帧：先 wait_trigger，任务内做同样的三步
```

投递单元等 `prev`，完成时 set `mine`——成不成都得 set。最初的 bug 是在*完成*时去读
`topics[topic].chain`，而不是在入队时就捕获：一次并发发布把别帧的链尾装了上去，本帧该 set
的事件永远等不到，此后该 topic 的每一帧都永久阻塞——现象是仿真时间停在最后一个事件上、一
句报错都没有。`mine->set()` 写在 `catch` 外面，所以失败或被取消的单元照样放行链。

延迟帧不许在发布那一瞬间就占住链尾。`after(Event)` 这类延迟没有上界；占住链尾会连同 topic
后续的每一帧（包括根本没有触发器的帧）一起扣下，`publish_serial(...).wait()` 因此永久死
锁——最坏的情况是触发器要由*排在后面的同 topic 帧的回调*来满足，那就成了真死锁。
`tests/shuttle_tests_trigger.cpp` 的 case8/case9 各自覆盖一条定律，case10/case11 覆盖
per-订阅者变体。

串行链的代价是跨订阅者的队头阻塞：一个订阅者的回调必须整个跑完，下一个才开始。这是明码标
价的取舍；要绕开它只有一个出口，`publish_concurrent`——只保留每订阅者 FIFO，放弃跨订阅者
顺序。

链尾是 `shared_ptr` 持有的 `ChainLink{ev, owner}`，因为等待方必须看见**活值**：按值拷贝会把
`owner` 冻在交接那一刻。`owner`（将要 set 事件的投递任务 id）由投递单元自己在开跑时写入，
完成时先清成 −1 再 `ev.set()`；发布方填不了，它不知道投递任务是谁。`Broker::remove_sub` 会
擦掉该订阅者的链尾槽，所以退订永远不会让链尾事件在 `topics` 里越堆越多。

### 自等待在等待处诊断，不在入队处

在回调里对本 topic 发布，再 `wait()` 新帧，任何 FIFO 设计都会死锁：`serial` 里内层帧排在本
帧后面；`concurrent` 下只要目标集里包含发布者自己的注册，结论一样。诊断靠
`ChainLink::owner`：`detail::check_self_blocked` 在挂起**之前**运行于
`Publication::wait()` / `wait(timeout)` / SV `shuttle_c_wait_pub`，`blocked_by` 里任何一条链
的 owner 是当前任务，就经 `weave::fatal` 结束仿真。

为什么放在等待处：对本 topic 只发布不等待（中继、转发）是合法用法，而那时链尾确实握在发起
这一帧的投递任务手里——同一个观察如果放到入队处判定，必然误报。为什么是致命而不是异常：这
一帧永远产不出一个能交回的结果，能 catch 只会把诊断变成静默挂死。报告走核心判定契约违例的
同一个 `weave::fatal`，带等待图，两个后端分别以 `$fatal` / `SC_REPORT_FATAL` 收尾。仍然诊
断不出来的是两个*无关*任务之间的环：链所有权只能证明"在等自己"，证明不了任意等待图。

### 退订立即生效；在途投递记为 retired

快照只保证*谁被选中*，不保证*一定会被调用*。两个来源都必须记成 `retired` 终态（计入
`dropped`，永不进 `delivered`，不产生 `Failure`）：

- C++ 侧：`Broker::remove_sub` 置 `Subscriber::removed`，`deliver` 在按 kind 分发**之前**查
  这个标志。这一查是必须的，因为快照握着 `shared_ptr`——漏掉它，已退订的回调真的会跑。
- SV 侧：`shuttle_registry` 用 `retired_[id]` 静默丢弃（只有从未登记过的 id 才在
  `shuttle.sub` 上写一行 `error`），丢弃时调 `shuttle_c_sub_retired`；`Broker::deliver` 在
  export 返回后通过 `take_retired_delivery()` 把这次投递转成 retired。

两个来源互不相交，谁也替代不了谁：C++ 的标志看不见 SV 侧
`shuttle_subscription::unsubscribe()`（它直接删注册表条目），SV 的报告也覆盖不到 C++ 订阅者。
早年只有 SV 来源产生 `retired`，C++ 却真的投给了已退订的回调——一种后端专属终态，在模型里
说不通。反过来的方案——SV 侧也把帧留住——代价更大：要跨语言的在途引用计数，还要
`unsubscribe()` 返回之后仍有回调在跑，两样都比丢一帧更糟。

### 目标集 = 名字 ∩ 类型，快照时定死；类型不相交即非目标

目标选择在发布调用时一次做完（`Broker::match_subs(topic, tag)`，拿 `Subscriber::filter_tag`
和帧的 `meta.type_tag` 求交）。类型不相交的订阅者不是目标：没有 `target_count`，没有投递单
元，没有 `dropped`，也不出现在 `subscriber_count()` 里。所以 `deliver` 从不复检标签。

理由：`subscribe<T>("uart.*")` 订阅的是 *`uart.*` 下的 T 类事件*；别的类型的帧从来不是它的
帧。标签写错了更早就会炸——订阅时由 `Broker::check_filter_early`、首次绑定时由
`Broker::check_bind_compat` 抛 `TypeMismatchError`。后者存在，是因为 SV 订阅者带着标签却不
声明绑定；没有它，写错的标签会按注册顺序把订阅者静默排除在每一帧之外。`deliver` 里还有最后
一道防线：`raw_ops` 帧（topic 未绑定、纯字节）碰上 typed 订阅者时抛 `PublishError`，而不是
把 `vector<byte>` 强转成 `T`。

### 完成只在一个地方推进

`Publication::finished()` 的含义是"已经到达终态的目标数"，唯一推进点是
`Broker::target_finished()`：

```cpp
void target_finished(const std::shared_ptr<PubState>& pub) {
  if (++pub->finished == pub->target_count && !pub->done_flag) pub_complete(pub);
}
```

每条路径都得走它——投递成功、retired 跳过、回调抛异常、串行单元的单元级失败（没尝试到的目
标也要补齐计数）。没有"单元末尾一次把 `finished` 置成 `target_count`"的分支；那样写会让
`done()` 在真完成之前就成立。`pub_complete()` 先跑钩子再 set `done_ev`，所以被事件唤醒的等
待方永远看不到"SV 进程还没被唤醒"这种中间态。零目标的发布立即完成（`publish_serial` 也要
短路）。所有"没收到"都必须可数：策略 `skip` 落 `skipped()`，快照后退订落 `retired()`，类型
不相交的根本进不了目标集。

五种终态：

| 终态 | 含义 | 计在 |
|---|---|---|
| delivered | 回调完整执行（SV：export 返回） | `Topic::delivered`、`Subscriber::received` |
| retired | 没进回调、无人过错：快照后、回调前被退订 | `Publication::retired()`、`Topic::retired`、`Subscriber::retired` |
| failed | 投递没干净完成：回调抛异常、帧物化失败、被取消、单元级失败 | `Topic::failed`、`Subscriber::failed`、`failures()` |
| deferred | 返回时还没启动（仅 `now` 的 defer 段）；之后落入上面三者之一 | `Publication::deferred()`（一个标记） |
| skipped | 没进回调、无人过错：被策略从帧级目标集里拿掉（仅 `now` + `skip`） | `Publication::skipped()`、`Topic::skipped`、`Subscriber::skipped` |

记账不变量（topic 与订阅者两级都是 `dropped = retired + skipped + failed`）：

1. 三种方式都无例外：`done()` 当且仅当 `finished() == subscriber_count()`；
2. `subscriber_count() + skipped()` = 本帧的目标订阅者数（名字 ∩ 类型，快照时固定）；
3. `finished()` 只算 delivered + retired + failed，故 `finished() ≤ subscriber_count()`；
4. 订阅者级的 `received() + dropped()` 统计每一帧名字 ∩ 类型匹配上它的帧——注意帧级与订阅
   者级的分母差一步 `skip`：`dropped()` 算上被策略跳过的帧，`subscriber_count()` 不算。

### 通配符只属于订阅模式

发布、`topic_declare`、`topic_set_retained` 只收具体名字；`>` 必须独占模式的末段，`*` 必须
独占某一段；两侧都拒绝空段与空字符串（`TopicPatternError`）。校验在**建任何状态之前**跑。

发布侧为什么必须拒绝：路由是字符串比对，允许 `publish_serial("uart.*")` 就会在 `topics` 表
里立起一个叫 `uart.*` 的幻影 topic——自带 `seq`、绑定与 retained 槽——而且真的会扇出给
`uart.*`、`uart.>`、`>` 三类订阅者。看着像向子树广播，实际只是名字碰巧相同，`uart0.*` 的订阅者
收不到它。更糟的是 `check_bind_compat` 会跳过通配符订阅者，连标签写错都不会露头。订阅侧为
什么必须校验：`match` 遇到 `>` 就放行剩余模式，于是 `a.>.b` 静默退化成 `a.>`；`a.rx*` 只
能匹配字面量 `a.rx*`。两种都是"注册成功、永远收不到"，一声不响。

只读查询（`topic_retained`）不校验：非法名字读作"从未绑定"。`now_preflight` 里名字校验排在
策略判定之前——同一个非法名字不该因为 `NowPolicy` 不同而换一种报错。

### `topic_name` 与 `topic_seq` 是两条正交轴，发布方看不到自己的 seq

`Meta::topic_name` 回答"哪条信道"（一个 topic 的所有帧都共享它，所以单靠它标不出一帧）；
`Meta::topic_seq` 回答"这条信道上的第几帧"（按 topic 从 1 编，所以 `a.1` 与 `b.1` 并存）。
两者合起来，在 broker 存活期内能唯一标识一帧。缺哪个都不行：

- 只留一个全局序号，同一 topic 相邻帧之间的间隔就没了意义，而所有计数器都是按 topic 的，
  `topic_seq` 存在的意义正是能和它们对上（跳号 = 丢帧；`serial` 链上必须严格递增）；
- 光有名字分不清帧——retained 补发帧与实时帧无从辨别（迟到的订阅者先看到*旧的*
  `topic_seq`，随后实时帧接着往前走：这一跳就是"补发的历史"的信号）；
- 名字也去不得：一个通配符注册能匹配多个具体名字，回调必须知道具体来源；`topics` 表还拿它
  做键。帧永远携带具体名字，模式从不进入 `Meta`。

`topic_seq` 原样跨过 DPI（`longint topic_seq`），C++ 日志与 SV `$display` 按
`(topic_name, topic_seq)` 对齐。它永远当不了请求-应答的关联号，因为发布方看不到自己帧的
`topic_seq`（见下面的关联号决策）。字段名带着作用域前缀，正是因为裸 `seq` 曾被误当成全局
序号。

### `publish_now` 有重入上限

回调跑在发布者的栈上，所以订阅者再 `publish_now` 是递归，不是挂起；两个互相发布的订阅者
会一声不响地爆栈。`dispatch_now` 带一个文件作用域的深度计数（`kMaxNowDepth = 8`），把环变
成 `PublishError`。这个上限是诊断的天花板，不是策略：合法嵌套（请求帧 → 响应者 handler →
回执帧）到 2–3 层。同步段里单个回调失败不终止该帧——记入 `failures()` 与 `first_error`，循
环继续，首个异常等所有目标处理完才抛回调用方。SV 侧 `shuttle_c_publish_now` 把异常转成返
回码加 `shuttle_c_last_error()`。

### 回执走 `publish_now`；blocking 入口永远不是回执的目标

回执用 `detail::pub_now` 加 `NowPolicy::skip` 发布（SV `shuttle_publisher::reply` 同理），
绝不走 `publish_serial`：配上 `nb(handler)` 响应者与 `send_now()`，整个往返都在请求方栈上
——`send_now()` 返回时回执已经躺在 `Call` 里（`call.queued() == 1`），零往返时延，不消耗仿
真时间。

`skip` 就是响应通道契约的全部，而且它是投递策略，不是注册规则：响应 topic 对 broker 来说
只是普通 topic。blocking 入口照样可以注册在它上面（覆盖它的通配符模式也一样），但它永远不
会成为回执的目标——帧把它算进 `skipped()` 与 `dropped()` 的 skipped 分量，回调不触发，也不
碰该 topic 的串行链，所以再慢的 blocking 旁观者也拖不动请求方、响应者或别的旁观者。回执只
是请求-应答握手的一半；排在 blocking 旁观者链后面的拷贝只是名义上的回执，所以这里用 `skip`
而不是 `defer`。

想观察回执流，用 `nb()` 注册：回执帧会投递给每个匹配的非阻塞订阅者——包括请求方自己的
`Call` 表——靠 `Meta::corr_id` 区分：非零表示一条关联回执（从请求回显而来），`0` 表示手工发
布的帧。名字本身仍然是公开的（`resp_topic()`、跨语言硬编码字符串都合法），手工往回执 topic
发布也合法；手工发布的帧按发布者自己的策略正常落到那里的 blocking 入口上。

### 关联号住在帧头，不住在载荷里

`Meta::corr_id` 是请求-应答的关联号：请求方分配，响应方回显，用来把回执路由回它的 `Call`。
它是 `Meta` 里唯一业务可写的字段（其余由 broker 填）；`0` 表示"非关联交换"。公开的发布三件
套不暴露它——`Publisher::publish_*` 永远发 `corr_id = 0`；只有 `Requester::send_*`、`serve`
与 SV `shuttle_publisher::reply` 会写它（经内部 `detail::pub_*` 的尾参数）。

为什么不放进载荷——三条理由，每一条都承重：

- 发布方看不到自己的 `topic_seq`，所以从 seq 派生的 id 没法在发布前写进载荷；
- 顺序上要求 id 在发布*之前*就入表：`send_now` 加 nonblocking 入口意味着响应者在发布者栈
  上同步回执——晚一步，回执就成了陈帧（`stale_responses()` 上涨）；
- `topic_seq` 是按 topic 的账（`a.req` 与 `b.req` 都从 1 开始，而 `serve` 支持通配请求
  topic），把它暴露出去会让 id 撞号，把一个诊断字段变成 ABI 契约。

计数器是进程级的普通 `static`（单活跃进程模型下成立），所以两个共用同一响应 topic 的
`Requester` 实例不会撞号。回执表存 `weak_ptr`；`Call` 持唯一强引用，丢掉句柄（或
`cancel()`）就释放槽位，迟到的回执落进 `stale_responses()`——可数，永不静默错投。没有这个
id，乱序回执就会错配（两个在途请求、FIFO 顺序、答案互换而数值恰好又相同时根本查不出来——
`shuttle_tests_requester::case2` 覆盖这一条）；三目标请求也回答不了"还剩几帧回执"；超时请求
的迟到回执会污染下一个请求；SV 响应方还没有整数可回显。

`wait_all()` 的期望回执数是一条约定，不是知识：框架默认"请求帧的每个目标恰好回一帧"
（`publication().subscriber_count()`）；有不该应答的旁观订阅者时用 `wait_for(n)` / `recv`。
请求 topic 与回执 topic 必须不同——响应者在请求回调里发布回执，共用 topic 会让回执排到发布
它的那个回调后面；构造时抛 `std::invalid_argument`。

### SV 请求者是同一套设计，只有计数器跨了 DPI

`shuttle_requester #(TReq, TResp)` / `shuttle_call #(TResp)` 与 `Requester` / `Call` 逐项对
应：调用槽先登记、再发帧（nonblocking 响应者在发布调用内就作答），`wait_for` 用一个截止时
刻约束整段等待，`wait_all` 从请求帧的 `subscriber_count()` 取期望回执数。只有一样东西塞不
进 SV，于是变成 DPI 入口：`shuttle_c_take_corr_id`（计数器是进程级 C++ 状态，另起一个计数器
会让同一响应 topic 上的 SV 调用与 C++ 调用拿到同一个 id）。

不同的地方是语言，不是设计：SV 没有异常，所以构造失败留在 `valid()` 为假加 `last_error()`，
`send_*` 返回空调用而不抛，`recv` / `wait_*` 对超时与取消都用 `ok == 0` 作答（那边没有
`TimeoutError` / `CancelledError`）。分发需要一个能持句柄的订阅回调，而队列属于参数化类，所
以 `shuttle_reply_target` 是个非参数化的基类，请求者覆写它的 `accept_response`。请求直接走发
布的 DPI 入口，不经 `shuttle_publisher`——公开三件套故意没有 `corr_id` 参数，
`shuttle_publisher::reply` 绕开它的原因也在这里。取消是显式的（`cancel()`）：SV 没有
`weak_ptr`，调用方丢掉的槽不可见，`prune()` 只能清掉已取消的调用，它们的回执计进
`stale_responses()`。

### Mailbox = 订阅者 + 队列，不是第二种投递机制

`Mailbox<T>` 把两样现成的东西拼在一起：经普通 `detail::sub_typed` / `sub_raw` 注册得到的订
阅者身份（入口**固定为 nonblocking**），外加自己的 `std::deque` 与 `weave::Event`。broker 不
加状态、不加导出符号——所以它能做成纯头文件。入口必须 nonblocking，因为邮箱回调只做一次拷
贝入队，而它跑在 `publish_now` 的同步段（发布者栈）里；blocking 的邮箱回调会把发布者锁死。
有用的推论：`publish_now` 能同步把邮箱填满，所以对一个纯 nonblocking 对端的请求是零往返时
延完成的。

有界邮箱必须在构造时选好 `drop_oldest` / `drop_newest` / `reject`；`OverflowPolicy::block`
在构造时就被拒绝，因为阻塞投递者等于锁死发布者。`overflowed()`（"已入队、随后被丢"）与
`Subscription::dropped()`（"从未投递到该订阅者"）是两本账；`reject` 复用常规的 `Failure`
通道，不另发明记账。close 幂等（退订 + 唤醒等待者），而且 close 之后队列仍可读出。SV 邮箱
（`shuttle_pkg::shuttle_mailbox`）是纯 SV，基于 `shuttle_subscribe_nb` 与 weave 的
`weave_sv_wait` / `weave_sv_trigger`——没有新的 DPI 入口；SV 没有异常，所以只有
`drop_oldest` / `drop_newest`。

邮箱是时序原语，不是 RPC 原语：无关联号的单响应者交换可以在它上面搭（示例正是这么做的），
但 `Requester` 是它的*兄弟*，不是它的用户——requester 直接复用同样的订阅原语，并不 include
`mailbox.hpp`。

### 跨语言帧的载荷走反向拉取

C++→SV 投递不把载荷当 DPI 实参传：C++ 侧把编码后的帧停在进程级 `FrameContext` 里（一个
RAII 保存/恢复栈，能安全嵌套），SV export 函数体再用 `shuttle_c_frame_len()` 加
`shuttle_c_frame_payload(b)` 反向拉取——数组归 SV 所有，所以每帧一次 DPI 调用、C++ 侧一次
`memcpy`。起因是 VCS 2018.09 的 `svdpi.h` 没有 `svCreateArray`，C++ 构造不了 open array，
再加上调用方栈上那些实参的生命期问题。安全前提是单活跃进程契约：export 执行期间没有别的进
程能覆盖停放中的帧。完整协议——import/export 表、字节布局、作用域规则——是
[backends.md](backends.md) 的主题；这里只记一条决策：桥依赖端口契约，端口契约永远不依赖桥。

### 延迟触发只传边沿名，永不传事件 id

`SHUTTLE_TRIG_EDGE` 的 `trig_name` 是 weave 边沿的**名字**；C++ 侧用 `weave::sv::Edge(name)`
在 weave 自己的名字表里 find-or-insert，两侧因此共享同一个 id。shuttle 不持有事件对象，也没
有按 id 恢复的入口——早年的 `shuttle_event` + `shuttle_c_event_*` + `SHUTTLE_TRIG_EDGE` 那
套影子事件系统已经删掉，不许复活：id 一旦跨 DPI，就得有人持有"id → 事件"的映射表，早失败
和所有权单一这两样也跟着丢掉。等待走 `Edge::wait()` / `Pulse::wait()`，不直接用
`weave_sv_wait`，为的是继承 weave 的挂起记账（取消注入、等待图、"挂起时没有当前任务"——最后
这条正是上面自等待诊断成立的前提）。边沿语义就是边沿，不是闩锁：`fire()` 早于等待登记就丢
了（要闩锁用 `after(weave::Event&)`）。VCS 2018.09 保留 `edge`，所以 SV 侧字段名是
`edge_name`。

### 继承逐层委托，不在叶子上摊平

`SHUTTLE_REFLECT_DERIVED(Derived, Base, 自有字段...)` 注册 `Derived` 时，把基类子对象整个交给
`Codec<Base>` 编解码，不重复写继承来的字段名；SV 的对偶写法是在派生类的三件套块里写
`` `shuttle_base(Base) ``。两种写法与"在叶子上摊平全部字段"产生同样的字节：先编基类，再编自
有字段——C++ 的基类子对象排在派生成员之前，而 codec 按声明顺序输出字段。该写委托形式：深度
为 *n* 时，摊平写法把继承名重复 *n* 次，而在第 *k* 层新增一个基类字段要改动它的全部后代。委
托下每层只写一次直接基类，链式组合，基类改动只落在基类自己的注册处。两种写法能互换，正因为
字节布局相同：摊平注册的叶子与委托注册的叶子互相解码，已有帧不用迁移。

约束是机制逼出来的，不是偏好：每层恰好一个公有基类（编码侧把 `Derived` 当 `Base` 传，解码侧
经 `static_cast<Base&>` 赋值）；派生注册出现时 `Codec<Base>` 必须已经注册；只做透传、不带自
有字段的中间层要手写 `Codec` 特化，因为字段宏展开不成空。SV 侧 `` `shuttle_base `` 的解包腿
先把派生句柄拷进基类型的局部变量：VCS 2018.09 拒绝把派生句柄接到 `ref` 形参
（`Error-[IRPC]`），而这个拷贝是安全的——生成的入口在进 `__codec` 之前已经把句柄所指的对象
构造好了。

### 对象引用以内部 id 过界

能跨边界的类型空间就是可编码的载荷：帧是带声明字段的值，所以带队列、嵌套句柄或接口的类天
生当不了载荷。放宽格式行不通——句柄会破坏值语义，`chandle` 根本进不了 `{>>{...}}`。获准的
绕法是 `sv/shuttle_ref.svh`：对象留在 SV 侧的表里，帧携带 4 字节引用，类型标签是
`$typename(T)`。broker 对此一无所知——串行、并发、now、触发器、retained 重放、跨语言路由
都照常作用在这条引用帧上，只负责转发的 C++ 对端看到的就是一个普通的大端 u32。SV 侧用
`shuttle_u32`（`pack_obj` / `unpack_into`）编这个 u32，走的是与反射过的 `logic [31:0]` 字段
同一条编解码通路——字节布局只有一份，没有第二份。

对外接口刻意照值通路已有的形式来，所以用户代码里永远看不到 id：

| 引用通路 | 对应的值通路 |
|---|---|
| `shuttle_ref_publisher #(T)::publish_now/serial/concurrent(obj, trig)` | `shuttle_publisher #(T)::publish_*` |
| `set_retained(bit)` | 同上 |
| `shuttle_ref_mailbox #(T)::new(topic[, capacity, overflow])`、`try_recv`/`recv`/`size`/`close` | `shuttle_mailbox` |
| `shuttle_ref_subscription #(T)::blocking/nonblocking(topic, sink)` | `shuttle_subscribe` / `shuttle_subscribe_nb` |
| `shuttle_ref_sink #(T)::invoke_ref(value, meta)`（task） | `shuttle_sink::invoke`（替你解包） |
| `shuttle_ref_sink_nb #(T)::invoke_ref_nb(value, meta)`（function） | `shuttle_sink_nb::invoke_nb` |
| 非参数化 sink 里的 `shuttle_ref_msg #(T)::unpack(payload)` | `T::unpack(payload)` |
| `typedef shuttle_ref_codec #(T) MyLink;` 配 `` `shuttle_field(obj, MyLink) `` | 声明字段的 codec（嵌套、向量、枚举同理） |

入口种类决定 sink 种类，与值通路一致：`::blocking` 收 `shuttle_ref_sink #(T)`，它的
`invoke_ref` 是 task，可以消耗仿真时间（跑在该主题的后台投递任务里，也就是串行链上）；
`::nonblocking` 收 `shuttle_ref_sink_nb #(T)`，它的 `invoke_ref_nb` 是 function，不得挂起
（就地调用，`publish_now` 也会调到）。两个方法名不同，所以一个类可以两个都实现，各持一个入
口。解析句柄本身从不耗时。

`_ref` 这个后缀不是装饰：直接叫 `invoke` / `invoke_nb` 的话，任何同时消费值帧与引用帧的类都
会和 `shuttle_sink` / `shuttle_sink_nb` 撞名，而 SV 没有重载能按参数类型区分。

SV 没有重载，一个类不能同时 implements `shuttle_ref_sink #(A)` 与 `shuttle_ref_sink #(B)`：
两者都声明 `invoke_ref`，编译器把第二个当重复声明拒掉（两个 `shuttle_ref_sink_nb` 同理，都声
明 `invoke_ref_nb`）。要收多种引用类型的接收方，改成实现一次非参数化的
`shuttle_sink_nb::invoke_nb(payload, meta)`（阻塞形式是 `shuttle_sink::invoke`），每个分支用
`shuttle_ref_msg #(T)::unpack(payload)` 解一行，由 `meta.type_tag` 选分支——和值通路上靠它
选消息类型是同一个动作。有继承关系时保留类型化写法：只 implements 一个
`shuttle_ref_sink #(Base)`（或它的 `_nb` 版本），在 `invoke_ref` 里 `$cast` 下转（此时编码里
的 tag 是基类名，按派生类名过滤的订阅者匹配不到）。队列式的接收方什么适配类都不用：每个类
型一个 `shuttle_ref_mailbox #(T)`，由一个服务循环统一取。

`shuttle_requester`、`shuttle_serve`、`shuttle_mailbox` 都没有引用版本，也不需要：请求/响应
本身就承载任何反射过的消息，消息可以在数据旁边捎上一个对象。机制就是 codec——用
`shuttle_ref_codec` 声明那个字段（要过一层 typedef，因为 `` `shuttle_field `` 收的是 codec
名字），请求帧、回复帧、retained 帧或 mailbox 条目里放的就是引用而不是副本：

```systemverilog
typedef shuttle_ref_codec #(FObj) FObj_link;
class AskObj;
  FObj         obj;          // 编码里 4 字节
  logic [31:0] mode;
  `shuttle_reflect_begin(AskObj)
    `shuttle_field(obj, FObj_link)
    `shuttle_field(mode, shuttle_u32)
  `shuttle_reflect_end
endclass
shuttle_requester #(AskObj, RspVal) rq = new ("svc.ask", "svc.rsp");  // 原样使用
```

两种形态共用同一张表、同一套编码，所以对象可以从引用帧进来、经值帧出去，到了对端还是同一个
句柄。

引用是表的索引，不是真引用，由此有四条约束：

- `T` 在两侧必须是同一个类类型（声明在 package 里，或放进两侧共同 include 的文件）：表与标
  签按 `#(T)` 特化隔离，也没有运行时注册这一步。标签既然是 `$typename(T)`，package 作用域的
  类发出的帧就带 `pkg::T`——C++ 对端用这个拼写反射一个只含 `std::uint32_t` 的结构体即可。
- id 单调分配、永不复用，所以过期引用会在投递处大声写一行 error，绝不静默指到更新的对象上；
  mailbox 把这类帧挡在队列外，计入 `unresolved()`。
- 表持强引用，所以发布方要等携带该引用的在途帧与 retained 帧都消费完，才能 `free(obj)` 或
  `clear()`。retained 帧在重放时才重新查表——这既让发布后的字段改动对迟到的订阅者可见，也
  让过早的 `free` 变成大声报错而不是悬空读。
- 同一对象重复发布只占一个引用，所以高频发布方不会逐帧把表撑大；表只随不同对象增长，`free`
  是它唯一的收缩途径。

机制是纯 SV——没有 DPI 入口，没有 broker 状态——因为只有持有方去解析，另一侧把引用当数据。
它不是第二条消息通路：帧照旧走 broker，带 broker 的计数、顺序与完成语义，引用只决定载荷里那
个数字是什么意思。整张对象图这样搬运，是用户自己把几样东西拼起来用，与 `Mailbox` / `Requester`
同一个思路。

### `Meta` 的字符串是视图，`pubs` 只登记外部发布

`Meta::topic_name` / `Meta::type_tag` 是指向 broker 已有存储的 `std::string_view`（`topics`
的键与 `Binding::tag`），而那块存储永不迁移，所以建帧没有额外分配，拷贝 `Meta` 只是两对
（指针, 长度）。它们都不以 NUL 结尾：DPI export 表要 `const char*`，所以
`src/backend/svdpi/bridge.cpp` 只在 SV 投递路径上物化 `std::string`，纯 C++ 用法不受影响。
用户写 `meta.topic_name == "dev.tx"`，也可以
`std::string s{meta.topic_name}`——拷贝初始化的 `std::string s = meta.topic_name;` 编不过
（`string_view` → `string` 的构造函数是 explicit）。代价在 ABI 上：`Meta` 不能再被用户代码用
字符串聚合初始化，旧二进制必须按新的 `libshuttle.so` 重编。

`pubs` 表只登记*外部*（SV）发布，因为 SV 句柄手里只有一个 `pub_id`，必须解析回 `PubState`；
C++ 的 `Publication` 自己持有状态，每帧都插进去只会让 `pubs` 无限增长。它单调增长
（`pub_id` 不复用）；长时间运行的 SV 侧高频回归仍需要一个周期性清理策略。

### 句柄生命期

`Publication` 是 `shared_ptr<Impl>` 的可拷贝句柄。`Subscription` 也是 `shared_ptr<Impl>`，
由*最后一次*析构退订（`Impl::~Impl` → `Broker::remove_sub`），`unsubscribe()` 幂等。SV 的
`shuttle_subscription` 是一对 id 加一个 `active_` 标志，析构时不退订（SV 没有确定性析构）；
SV 的 `shuttle_publication` 通过 `pub_id` 镜像 `Publication` 的方法。"退订立即生效"是双向
的：之后的帧不再选中该订阅者，已快照的帧把它跳过并记为 retired。

## 公开面的分层

依赖只向下指；`shuttle.hpp` 是伞头，它规定了什么算公开：

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

安装出来的 `include/shuttle/detail/` 里带 `abi.hpp`（模板需要的稳定第二层）、`wait.hpp`
（`detail::deadline_after` / `detail::wait_wake`——mailbox、requester 与
`Publication::wait` 三处"等事件、可能带超时"的竞速共用它），以及反射支持。`xmake.lua` 的
头文件 glob 必须保持 `include/(shuttle/**.hpp)`：glob 收窄会把 `detail/abi.hpp` 从安装包里
悄悄漏掉，用户一 include `publisher.hpp` 就编不过。

## 范围

shuttle 负责仿真进程内的类型化发布-订阅投递调度，C++、SystemVerilog 与 DPI 跨界都算在内。
明确不提供：

- 基础模型里的 RPC 配对（mailbox/requester 一节的请求-应答糖是拼出来的，请求/回执 topic 是
  约定，不是 broker 的知识）；
- 跨机器通信、持久化与回放；
- UVM 适配层；
- 普通订阅的背压（`SubOptions::max_pending` / `OverflowPolicy` 只有 `Mailbox` 遵守）；
- shuttle 自己的时钟——仿真时间是 weave 的（`weave::now()`），刻意不设
  `shuttle::now_fs()` 这类转发包装（曾经有过，已删）。

已知限制：SV 侧的错误行不传回 C++（协议的后果；严格的错误通道要扩展 `dispatch` 的返回值）；
跨语言载荷是值语义、字段子集——编码里没有 variant、指针、引用或句柄，对象图只能以内部对象
id 过界（见上），不能当载荷；`TopicInfo::deferred`（topic 累计）与 `Publication::deferred()`
（每帧）在两个粒度上共用一个名字。

## 写给大模型的设计哲学

shuttle 假定 LLM 懂发布-订阅和 asyncio 式调度，但不懂 EDA 仿真语义；框架的职责是把仿真特
有的那部分——哪些回调可以消耗仿真时间、只有一个 CPU 时"投递顺序"到底能是什么——摆到明面
上，并且可计数。要推广的泛化原则只有三条：需求能用"入口能力 + 队列 + topic 约定"表达时，
*永远*不要引入第二种通信机制；*永远*不要让计数器里看不见的终态存在；*永远*不要为纯头文件组
件加 broker 状态。过去每一次违反，都长出一个影子系统（一张并行事件表、一种后端专属终态、一
只重复时钟），最后都得删掉。
