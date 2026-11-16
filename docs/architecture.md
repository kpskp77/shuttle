# shuttle 架构

本文描述 shuttle 的**实现结构**：三条发布路径、入口模型、数据结构、记账契约与后端边界。
面向需要修改 `src/` 或排查投递行为的读者；只使用 API 请先读
[`../README.md`](../README.md)。

| 文档 | 职责 |
| --- | --- |
| [`../README.md`](../README.md) | 能力、用法、选型 |
| 本文 | 实现结构与不变量 |
| [`dpi_protocol.md`](dpi_protocol.md) | C++ ↔ SV 边界的权威定义（函数表、常量、线上格式、仿真器约束） |
| [`../AGENTS.md`](../AGENTS.md) | 改动本仓库必须守住的硬约束清单 |

## 1. 定位

shuttle 在 weave 之上提供三件事，不重复 weave 的职责：

| 层 | 内容 |
| --- | --- |
| broker（`src/broker.cpp`） | topic 表、通配符匹配、类型绑定、帧序号、三种发布编排、完成跟踪、retained |
| 类型化载荷（`include/shuttle/codec.hpp`） | 编解码与跨语言物化，类型擦除边界在 `detail/abi.hpp` |
| 外部接入门面（`src/entry.cpp`） | 字节帧发布（`ext_*`）、外部订阅者注册、按 `pub_id` 的查询/等待——与后端无关 |
| DPI 桥（`src/backend/svdpi/` / `sv/shuttle_pkg.sv`） | import/export、帧拉取、作用域修正、SV 侧分派注册表 |

并发编排（task 创建、事件、等待、超时、取消）全部由 weave 提供。后端二选一：
`svdpi`（真实仿真器，带 SV 桥）与 `systemc`（纯 C++，无 SV 桥）。后端选择只影响
`detail::backend` 的实现，broker 与公开 API 完全相同（§12）。

非目标：跨进程/跨机器通信、持久化、流量整形、UVM 兼容层。

## 2. 术语

| 术语 | 含义 |
| --- | --- |
| **帧**（frame） | 一次发布产生的不可变快照：载荷 + `Meta{topic, type_tag, seq, time_fs}`。三种方式都先建帧再生效 |
| **目标**（target） | 本帧匹配到的订阅者之一。目标集合在发布调用瞬间按当时的注册表快照确定 |
| **入口**（entry） | 订阅者被调用的那一个函数：`blocking`（可消耗仿真时间）或 `nonblocking`（零延时）。一次注册只有一条 |
| **投递**（delivery） | broker 把一个目标与一帧配对的尝试，结果是终态之一（§9） |
| **投递单元** | 承载投递的后台执行体：`serial` 为每帧一个 weave task，`concurrent` 为每订阅者一个 task（§5） |
| **串行链** | 每个 topic 一条 FIFO 链（`Topic::chain`），`serial` 与 `now` 的 `defer` 共用（§6） |
| **终态** | 目标对本帧的结局：delivered / retired / failed / deferred / excluded（§9） |
| **记账** | 上述终态在 `PubState`、`Topic`、`Subscriber` 三层的计数 |

术语与两侧枚举同名（`Capability::blocking` / `SHUTTLE_CAP_BLOCKING`）；在 SV 侧
blocking 入口即 export task，nonblocking 入口即 export function。

## 3. 分层与文件地图

```
                      ┌──────────────────────────── 用户代码 ────────────────────────────┐
                      │  C++: Publisher<T> subscribe<T> Publication Subscription Trigger    │
                      │  SV : shuttle_pub#(T) shuttle_subscribe shuttle_sink(_nb) handle    │
                      └───────────────┬───────────────────────────────┬──────────────────┘
                                      │ 公开头文件（include/shuttle/*.hpp）│ SV 包（sv/shuttle_pkg.sv）
        ┌─────────────────────────────▼──────────┐        ┌───────────▼───────────────────┐
        │  broker 核心（src/broker.cpp）           │        │ shuttle_pkg.sv                │
        │  Topic 表 / 通配符 / 类型绑定 / seq      │        │  registry / sub / handle       │
        │  三种发布编排 / Publication / retained   │◀──DPI─▶│  trigger / event / pub#(T)     │
        │  Event 注册表 / 内省                    │        │  帧拉取 + {>>{}} 打包约定       │
        └───────┬───────────────────────▲─────────┘        └───────────────────────────────┘
                │ 后端无关                │ 后端边界
     ┌──────────────────────┐   ┌──────────────────────┐  ← 同一组 detail::backend 入口
     │ backend/svdpi/       │   │ backend/systemc/     │     （src/internal/backend.hpp）
     │  bridge + DPI 面     │   │ 无 SV 桥：dispatch*  │
     │  FrameContext + 作用域│   │ 抛逻辑错误；now_fs   │
     └──────────┬───────────┘   │ 用 sc_time_stamp     │
                │               └──────────────────────┘
                ▼
        weave（task/event/port，svdpi 与 systemc 后端）  ← 并发编排、单活跃进程契约
```

| 目录 | 内容 |
| --- | --- |
| `include/shuttle/*.hpp` | 公开 API；`shuttle.hpp` 是伞头，被它暴露的才算公开 |
| `include/shuttle/detail/abi.hpp` | **已安装但非公开**：公有模板需要 `pub_*` / `sub_*` 的声明，故随包安装；只被 `publisher.hpp` / `subscription.hpp` 间接包含 |
| `src/broker.cpp` | broker 逻辑（唯一有状态的地方）：C++ 类型化入口 `pub_*` / `sub_typed` / `sub_raw`、`topic_*`、句柄方法、`Event` 注册表、`broker::` 内省 |
| `src/entry.cpp` | 外部接入门面：`ext_pub_*`（字节帧）/ `ext_sub` / `ext_unsub` / 按 `pub_id` 的 `pub_*` 查询。**与后端无关**（svdpi 只是当前调用方） |
| `src/internal/broker_impl.hpp` | `Broker` / `Subscriber` / `Topic` / `PubState` / `Envelope` 与 `dispatch_now` 的共享定义：`broker.cpp` 与 `entry.cpp` 各包含一次（不安装） |
| `src/backend/svdpi/bridge.cpp` | `detail::backend` 的 svdpi 实现：`dispatch` / `dispatch_nb` / `now_fs` / `note_retired_delivery` + 帧拉取（`frame_len` / `frame_qword`） |
| `src/backend/svdpi/dpi.cpp` | `shuttle_c_*` 的全部 DPI 导出面（import 被仿真器调用、export 反过来调仿真器） |
| `src/backend/svdpi/svdpi_internal.hpp` | 后端私有：包名宏、`SvScopeGuard`、`FrameContext`（仅 svdpi 目录内） |
| `src/backend/systemc/bridge.cpp` | systemc 后端：`detail::backend` 的退化实现 |
| `src/trigger.cpp` | `Trigger` 工厂（`after` / `after_all` / `after_any`） |
| `src/internal/backend.hpp` | broker ↔ 后端的内部边界：6 个入口（`available` / `dispatch` / `dispatch_nb` / `now_fs` / `note_retired_delivery` / `take_retired_delivery`），不安装 |
| `src/internal/trigger_internal.hpp` | `detail::TriggerAccess`：供桥接层用任意 `std::function` 构造 `Trigger`（不安装） |
| `sv/shuttle_pkg.sv` | SV 侧用户 API 与分派注册表 |
| `tests/`、`examples/` | 测试套件与示例 |

## 4. 并发模型

**不变量：任一时刻只有一个进程执行 C++ 侧记账代码；挂起等同于交出调度权。**
这条契约来自 weave 的 port 模型，shuttle 直接继承：

- broker 状态（`topics` / `subs` / `pubs`）是**普通数据结构，无锁**；
- 一切"检查—修改"只要中间不挂起就是原子的。例如 `pub_state()` 判定与
  `pub_on_complete()` 注册之间没有挂起点，因此不会漏掉完成通知；
- `shared_ptr` 的引用计数跨语言边界读写同样受此保护；
- 唯一需要显式处理的是链尾捕获（§6）：并发发布的"读链尾 + 装新链尾"必须在同一同步段内完成。

进程由 weave 创建：`weave::create_task(name, fn)` 只登记不立即执行；让出控制权
（`weave::sleep`、`Event::wait`、`Task::await`、等待 SV export task）后才轮到其他 task。
SV 的 native 进程（`initial` / `always`）在同一套调度内：阻塞在 `weave_sv_wait` 上时同样
交出控制权。

推论：所有"后台"投递都运行在 weave task 里，因此 `publish_serial` 的 FIFO 与
`publish_concurrent` 的并发都由 weave 调度器决定，broker 不额外引入线程。

## 5. 三条发布路径

三种方式的**目标集合相同**，差异只在调度、顺序与完成时机。公共前置步骤是
`make_envelope[_bytes]`：绑定类型、`++seq`、取 `time_fs`、必要时刷新 retained 快照。
三条路径最终都落到 `deliver(sub, env)`：走哪条入口见 §7，顺序保证见 §6，
计数规则见 §9。

### 5.1 `publish_now`

```
publisher 进程
  │ pub_now ──▶ now_preflight（policy==reject 先判定；拒绝则什么都不发生）
  │          ──▶ make_envelope（类型绑定 + ++seq + time_fs + retained 快照）
  │              ─▶ dispatch_now
  │                    ├─ 建 PubState（expected = nonblocking 目标数 + 待 defer 数）
  │                    ├─ 逐个 nonblocking 目标：deliver(sub, env)
  │                    │     （这些注册的 cap 已是 nonblocking → export function）
  │                    │     失败 → 记 failures / dropped，继续下一个目标
  │                    ├─ policy==drop  → 计 excluded + dropped
  │                    └─ policy==defer → start_serial(topic, env, {}, blocking_targets)
  │                                       完成钩子把失败与计数并回本帧
  └ 返回 Publication（nonblocking 部分已结束，deferred 部分未结束）
     └ 同步段出现异常时：以上全部完成后，重抛第一个异常
```

同步段不出现挂起点：`deliver(sub, env)` 的目标全部来自 nonblocking 注册（C++ 直接函数调用、
SV 侧 export function），blocking 目标根本不在这个循环里。同步段单个回调失败不中止本帧——记入 `failures()` 与
`first_error` 后继续，`defer` 段照常入链，最后才把第一个异常抛给调用者；SV 侧
`shuttle_c_publish_now` 把异常转成返回码与 `shuttle_c_last_error()`。

### 5.2 `publish_serial`

```
publish_serial ─▶ targets 为空 ? 立即 pub_complete（不建单元、不装链尾）
               ─▶ expected = targets.size()
               ─▶ prev/mine 链尾交接（§6，同步段）
               ─▶ create_task("shuttle-seq-<topic>")
                    ├─ wait_trigger(trig)              // 延迟启动条件
                    ├─ prev->wait()                    // 排在上一帧之后
                    ├─ 逐目标 deliver(sub, env)        // 用该注册的那条入口
                    │     └─ target_finished()         // ++finished
                    ├─ 单元级失败 → 未尝试的目标全部记 failed 并补齐
                    ├─ mine->set()                     // 成败都放行下一帧
                    └─ finished == expected 时 pub_complete()
返回 Publication（立即可用）
```

"单投递单元"意味着订阅者之间串行：前一个订阅者的回调彻底结束（含 SV 侧可阻塞的
`invoke` task）才开始下一个。

### 5.3 `publish_concurrent`

```
publish_concurrent ─▶ targets 为空 ? 立即 pub_complete
                   : 每个订阅者 create_task("shuttle-par-<topic>")
                          └─ wait_trigger → deliver(sub, env) → target_finished()
```

订阅者之间无顺序承诺；同一订阅者的不同帧之间也不保证 FIFO（需要 FIFO 时用
`publish_serial`）。每个 task 独立捕获自己的失败：`failures()` 按完成顺序累积，
`result()` 重抛第一个。

### 5.4 完整投递路径

```
                 ┌──────────── C++ 发布 ────────────┐        ┌──── SV 发布 ────┐
Publisher<T>::publish_now/serial/concurrent         shuttle_pub#(T)::publish_now/serial/concurrent
                 │                                        │
        pub_now / pub_serial / pub_concurrent  shuttle_c_publish_{now,serial,concurrent}
                 │                                        │
                 └───────────► make_envelope[_bytes] ◄─────┘
                                  │ 类型绑定 + ++seq + time_fs + retained 快照
                                  ▼
                      dispatch_now / start_serial / start_concurrent
                                  │ 快照目标集：名字匹配 ∩ 类型标签匹配
                                  ▼
                               deliver(sub, env)
                                  │ 入口一律按该注册的 cap（不变量 4）：
                                  │   cap==nonblocking → dispatch_nb（export function）
                                  │   cap==blocking    → dispatch（export task）
                                  │ now 的同步段只含 nonblocking 注册（select 已分拨）
                     ┌────────────┼───────────────────┐
                     ▼            ▼                   ▼
              typed: cb(T&)   bytes: cb_bytes   external: detail::backend::dispatch[_nb]
                                               └─ FrameContext + 作用域修正
                                                  → export task/function
                                                  → shuttle_registry → 用户回调
                                                  → 拉取帧 → unpack → invoke
```

## 6. 投递顺序：per-topic 串行链

`publish_serial` 与 `publish_now(defer)` 共用每个 topic 的一条 FIFO 链，由此得到可测定律：
**同一 topic 的后台投递顺序等于发布调用顺序**。

`start_serial` 在同步段（发布调用内）完成链尾交接，顺序不可交换：

```cpp
auto prev = std::make_shared<weave::Event>(topics[topic].chain);  // 本帧要等的链尾
auto mine = std::make_shared<weave::Event>();                     // 本帧要放行的链尾
topics[topic].chain = *mine;                                      // 安装新链尾
```

投递单元先 `prev->wait()`，结束（无论成败）后 `mine->set()`。若在 task 体内才读取
`topics[topic].chain`，并发发布会把其他帧的链尾 set 掉，而本帧该放行的那个永远不置位，
后续帧随之永久阻塞——表现为仿真时间停在最后一个事件处且无任何报错。
`mine->set()` 位于 `catch` 之外，保证失败与取消都不会使链停滞。

串行链的代价是跨订阅者的队头阻塞：前一个订阅者的回调彻底结束后才开始下一个。这是
显式接受的取舍；需要并发时用 `publish_concurrent`。

## 7. 入口模型与 NowPolicy

`blocking` / `nonblocking` 描述**回调能否消耗仿真时间**，不是"入口会不会阻塞调用者"：
`publish_now` 的同步段只调用 nonblocking 入口，`defer` 时 blocking 入口也在同一次发布中
被调用，只是已位于后台链上。SV 侧两者分别对应 export task 与 export function。

| 入口 | C++ 注册 | SV 注册 | 被谁调用 |
| --- | --- | --- | --- |
| blocking（`Capability::blocking`） | `subscribe<T>(topic, fn)` | `shuttle_subscribe(topic, obj)`（`SHUTTLE_CAP_BLOCKING`） | `serial` / `concurrent` 的投递单元；`now` 的后台链 |
| nonblocking（`Capability::nonblocking`） | `subscribe<T>(topic, nb(fn))` | `shuttle_subscribe_nb(topic, obj)`（`SHUTTLE_CAP_NONBLOCKING`） | `now` 的同步段；`serial` / `concurrent`（该注册只有这一条） |

一次注册只有一个订阅者、一条入口，因此 `Subscriber` 只存一个 `Capability cap`，
`deliver(sub, env)` 也只读它：既没有优先级，也没有二选一，更没有"本次投递"级的入口覆盖。
SV 桥的 `caps` 参数只接受恰好为 1 或 2，其他值直接拒绝——两位同置的订阅者不可构造。

**走哪个 export 由该注册的入口决定**：`cap == blocking` 走 `dispatch`（export task），
`cap == nonblocking` 走 `dispatch_nb`（export function）。`now` 的同步段之所以不会踩到
`Error-[DPI-INCDIC]`，靠的是 `dispatch_now` 先按 `cap` 把 blocking 目标摘走（§5.1、§11.3），
而不是靠某个投递参数临时改写入口。

`NowPolicy` 只作用于入口为 blocking 的目标，与 topic 上是否存在 nonblocking 订阅者无关：

| 策略 | 行为 | 对时序的影响 |
| --- | --- | --- |
| `reject` | 抛 `CapabilityError`（C++）/ 返回 `null` + last_error（SV） | 帧从不发布（`seq`、retained、`published` 均不推进） |
| `drop` | 跳过这些目标，计入 `excluded()` 与 `dropped()` | 其余目标不受影响 |
| `defer`（默认） | 整帧交给本 topic 的串行链（与 `serial` 同一条） | 这些回调在 `publish_now` 返回之后执行 |

对 nonblocking 目标，`drop` 与 `defer` 无差别：两者都在 `publish_now` 返回前执行完毕。

## 8. 数据结构

broker 的状态集中在三处：topic 表、订阅者表与发布表。帧是不可变快照，
完成跟踪挂在发布表上；其余对象都是这两者的视图。

### 8.1 Topic / Subscriber / Envelope

```cpp
struct Topic {
  std::optional<Binding> binding;        // 未绑定 = 只能发纯字节流（三件事是一个东西，所以是一个 optional）
  // struct Binding { std::string tag;    // C++ SHUTTLE_REFLECT 名 / SV type_tag
  //                  const detail::TypeOps* ops; };  // raw_ops = 纯字节流
  std::uint64_t seq;                     // topic 级序号（跨多个 Publisher 单调递增）
  std::uint64_t published, delivered, dropped, deferred;  // deferred = 被 defer 进链的次数
  bool retained_on; std::shared_ptr<const Envelope> retained_env;
  weave::Event chain;                    // 后台链尾（serial 与 now 的 defer 共用，初始已 set）
};

struct Subscriber {
  SubKind kind;                          // typed / bytes / external
  int id;
  int handle;                            // kind == external：后端侧不透明句柄（svdpi = SV 注册表编号）
  std::string pattern, filter_tag;       // filter_tag 空 = 任意类型（参与目标集判定）
  Capability cap;                        // 该次注册唯一的那条入口（§7）
  ErasedCallback cb; ByteCallback cb_bytes;
  const detail::TypeOps* ops;            // typed 订阅者的类型（供字节流物化）
  std::uint64_t received, dropped;
};

struct Envelope {                        // 一帧的不可变快照
  std::shared_ptr<const void> payload;   // typed: const T；raw: const vector<byte>
  const detail::TypeOps* ops;
  mutable std::shared_ptr<const std::vector<std::byte>> encoded;  // 跨语言编码缓存（懒）
  Meta meta;
};
```

`Broker` 是函数内静态单例（`Broker::get()`），持有 `topics`、`subs`、`pubs` 与两个 id 计数器。
每个进程一份；SV 侧只维护一层注册表，二者共享同一个 broker。

### 8.2 PubState（完成跟踪）

```cpp
struct PubState {
  int id;
  std::size_t expected, finished;         // 单位是"订阅者"，不是"投递单元"
  std::size_t excluded, deferred, retired;  // 见 §9
  bool done_flag; weave::Event done_ev;
  std::vector<Publication::Failure> failures; std::string first_error;
  std::vector<std::function<void()>> on_complete;   // 完成瞬间钩子（SV wait_done 用）
};
```

`pubs` 表只增不减（仅随进程结束回收），长时间高频发布需要外部周期性清理策略。

## 9. 记账契约

三种发布方式共用同一套记账规则，这是本实现最重要的契约。

### 9.1 五种终态

每个目标对本帧只有五种互斥且穷尽的结局：

| 终态 | 含义 | 计数落点 |
| --- | --- | --- |
| `delivered` | 回调执行完毕（SV 侧 = export 返回） | `Topic::delivered`、`Subscriber::received` |
| `retired` | 未进入回调，且无人的错：快照之后、回调之前被退订（C++ 的 `Subscriber::removed` 或 SV 注册表丢弃，§9.1.1） | `Publication::retired()`、`Topic::dropped`、`Subscriber::dropped` |
| `failed` | 尝试但未成功：回调抛异常 / 帧不可物化 / 投递被取消 / 触发条件失败 | `Topic::dropped`、`Subscriber::dropped`、`failures()` |
| `deferred` | 返回时尚未开始（仅 `now` 的 `defer` 段），之后必落入前三种之一 | `Publication::deferred()`（标记） |
| `excluded` | 策略不投（仅 `now` 的 `drop`），不进入目标集合 | `Publication::excluded()`、`Topic::dropped`、`Subscriber::dropped` |

`reject` 不产生终态：它在建帧之前判定（`Broker::now_preflight`，§5.1），被拒绝的帧不推进
类型绑定、`seq`、retained 快照与 `published`，等价于从未发布。

`PubState` 的字段即上表的帧级投影：

```cpp
expected;  // 目标数（不含 excluded）
finished;  // 已达终态的目标数 = delivered + retired + failed
excluded;  // 被 drop 排除的目标数（不进 expected）
deferred;  // 转到后台链、返回时尚未开始的目标数（标记，不回零）
retired;   // 快照后被退订而跳过、但计入 finished 的目标数（不分语言，§9.1.1）
```

#### 9.1.1 目标集 = 名字 ∩ 类型

**类型不匹配不是一种终态，因为它不是目标。** 目标集的判定在快照时一次做完
（`Broker::match_subs(topic, tag)`：`Subscriber::filter_tag` 与帧的 `meta.type_tag` 相交）：
类型不相交的订阅者不进 `expected`、不建投递单元、不动 `dropped`、也不出现在
`subscriber_count()` 里。`deliver` 因此不看类型标签（§5 的时序图里"快照目标集"一步就是全部）。

这一点与 `dropped` 的口径一致：`dropped` 回答"我本该收到却没收到"，而类型不相交的帧本来
就不是我的（`subscribe<T>("uart.*")` 订阅的是 `uart.*` 下**类型 T** 的事件）。标签写错这种
真错误由两条前置校验拦成 `TypeMismatchError`：订阅时 `Broker::check_filter_early`、首次
绑定时 `Broker::check_bind_compat`（§10.3）。测试：`tests/cpp/publish_tests.cpp` case15、
`tests/cpp/broker_tests.cpp` case11。

`retired` 是唯一的"非回调终态"：目标在快照之后、回调之前被退订。记 `delivered` 是幻影
成功，记 `failed` 是把合法用户动作当错——目标已到终态、回调未进入、没有失败者。它同时计入
topic 级与订阅者级 `dropped`，所以"名额给了、内容没进回调"仍然可观测。

快照只保证**谁被选中**，不保证**一定被调用**（"退订即生效"，两种语言同一口径）。两个来源，
覆盖全部窗口：

| 来源 | 机制 | 覆盖的窗口 |
| --- | --- | --- |
| C++ 侧 `Subscriber::removed` | `Broker::remove_sub` 置位；`deliver` 在入口检查（kind 分支之前），因为快照持有 `shared_ptr`、对象还活着，不查就会真的调进回调 | 快照 → 投递之间的任意时刻 |
| SV 注册表回告 | `shuttle_registry` 在退订时删除 sink 并置 `retired_[id]`，随后的在途 dispatch 静默丢弃并调 `shuttle_c_sub_retired` → `Broker::deliver` 里 `take_retired_delivery()` | 同上，但退订发生在 SV 侧，C++ 无法本地观察 |

两个来源互不重叠也互不替代：C++ 的标记看不到 SV 侧的 `shuttle_sub::unsubscribe()`（它直接
删注册表条目），SV 的回告也覆盖不到 C++ 订阅者。曾经只有 SV 一侧会产生 `retired`（C++ 侧
按"快照语义"照常回调），这与后端隔离的方向矛盾——后端不该有专属终态，故统一为两来源。

反向（让 SV 也保帧）不可行：`shuttle_sub` 退订后 sink 已不在注册表里，要保帧就得让注册表
一直持有到本帧结束，即跨语言的在途引用计数（C++ 还得回告"帧 X 完成"），并让
`unsubscribe()` 之后仍被调用——比丢一帧糟得多。

测试：`tests/cpp/broker_tests.cpp` case12（同帧内 B 退订 A → A 记 retired）、
`tests/cpp/trigger_tests.cpp` case7（触发等待期间退订）、`tests/sv/shuttle_tests.sv` 的
`case_retired`。

### 9.2 不变量

1. `done()` 当且仅当 `finished() == subscriber_count()`，三种方式无例外；
2. `subscriber_count() + excluded() == 本帧的目标订阅者数`（名字 ∩ 类型，且未被策略排除）；
3. `finished()` 只累计 delivered + retired + failed；`deferred` 完成后必落入这三者之一，
   因此 `finished() ≤ subscriber_count()`；
4. 订阅者级 `received() + dropped() == 该订阅者被定为目标的帧数`（失败、退订丢弃、策略
   排除都计入 `dropped`，回调异常亦然）；
5. 同一事件在 topic 级与订阅者级 `dropped` 中的口径一致（退订丢弃、策略排除、失败三类）。

### 9.3 唯一推进入口

`finished` 只由 `Broker::target_finished()` 递增：

```cpp
void target_finished(const std::shared_ptr<PubState>& pub) {
  if (++pub->finished == pub->expected && !pub->done_flag) pub_complete(pub);
}
```

三种方式的每个目标结束都走这里——串行链单元、并发单元、`now` 的同步段，以及
`now` 的 defer 段（由串行链的完成钩子按目标数并回）。不存在"整个单元一次性把
`finished` 置成 `expected`"的分支，否则 §9.2 的第 1 条在部分失败时会失真。

`pub_complete()` 的顺序固定：置 `done_flag` → 依次执行并清空 `on_complete` 钩子
（异常吞掉，不影响投递结果）→ `done_ev.set()`。钩子先于事件触发，因此被 `done_ev`
唤醒的等待者不会观察到"SV 进程尚未被唤醒"的中间状态。

### 9.4 三层的读取方式

| 层 | API | 语义 |
| --- | --- | --- |
| 帧 | `Publication::subscriber_count()/finished()/excluded()/deferred()/retired()` | 本帧的可见性计数 |
| 订阅者 | `Subscription::received()/dropped()` | 该注册在生命周期内的累计 |
| topic | `broker::topic_info().published/delivered/dropped/deferred` | 该 topic 的累计（`deferred` 只统计 `now` 的 defer 段） |

SV 侧同名方法经 `shuttle_c_pub_stat(pub_id, kind)` 读取，`kind` 0/1/2/3/4 分别对应
目标 / 已结束 / 排除 / deferred / 退订跳过（见 `dpi_protocol.md` 的 import 表）。

## 10. 类型系统与载荷

### 10.1 类型擦除边界

`include/shuttle/detail/abi.hpp` 是唯一不知道 `T` 的地方：

```cpp
struct TypeOps {
  const char* tag;                                                        // 类型标签
  std::vector<std::byte> (*encode)(const void*);                          // 无 codec 时为 nullptr
  std::shared_ptr<const void> (*decode)(const std::vector<std::byte>&);   // 同上
};
template <typename T> const TypeOps* type_ops();   // 每个 T 一份静态实例
```

- C++ 内部投递零拷贝：`make_shared<const T>(msg)` 作为 `Envelope::payload`，
  typed 订阅者直接 `cb(payload.get(), meta)`；
- 只有存在字节订阅者（SV 订阅者或 `subscribe_bytes`）或帧本身来自 SV 时才编码，
  结果缓存在 `Envelope::encoded`，一帧多订阅者只编一次。

### 10.2 编解码

`include/shuttle/codec.hpp` 提供 `Writer` / `Reader`（大端，越界抛 `DecodeError`）、
`Codec<T>` 及其标量/字符串/枚举/容器特化，`SHUTTLE_REFLECT(Type, 字段...)` 为聚合类型生成
特化（`tag()` 返回 `#Type`，字段按声明序编码，上限 16 个）。线上格式见
`dpi_protocol.md` 的线上格式一章；SV 侧必须用 `{>>{...}}`（声明序、每字段高位在前）。

### 10.3 类型绑定

topic 首次被声明时绑定类型标签，之后所有发布与订阅都必须一致，否则
`TypeMismatchError`。绑定来源有四个：

1. C++ `Publisher<T>` 构造 → `topic_declare(topic, ops)`；
2. C++ `subscribe<T>` 且 pattern 不含通配符 → 立即 `topic_declare`（支持"SV 先发布、
   C++ 后订阅"的时序）；
3. SV 发布 → `make_envelope_bytes` 按 `type_tag` 绑定；
4. SV 先发布、topic 尚未绑定、但已有匹配的 typed C++ 订阅者 → `make_envelope_bytes`
   用该订阅者的 `ops` 绑定，否则这一帧只能是 `raw_ops`。

前三种绑定发生时（`make_envelope` / `make_envelope_bytes` 的 `!t.bound` 分支）会跑
`Broker::check_bind_compat`：若已有“模式恰好是该 topic 且标签不同”的订阅者，直接抛
`TypeMismatchError`，**通配符订阅者跳过**（它们本就横跨类型，只在目标集快照时相交）。
这一步是 `check_filter_early` 的镜像，专门覆盖 SV 侧“只带标签、不声明绑定”的订阅者：
否则同一个标签写错，topic 先绑定则订阅时就报错，订阅先发生则此后每帧都悄无声息地不属于
目标集——错误能不能被发现取决于注册顺序。

防线：`deliver` 的 typed 分支遇到 `env.ops == &raw_ops` 直接抛 `PublishError`。
缺少这条防线时，`raw_ops` 帧会被当成 `vector<byte>` 强转成 `T`，读到堆内存垃圾。

通配符订阅无法预绑定，参与每帧的目标集判定（`Subscriber::filter_tag` ∩ 帧的
`meta.type_tag`，§9.1.1），不匹配的帧不算目标，因此不产生任何计数。

## 11. svdpi 后端

### 11.1 两个方向与载荷传递

| 方向 | 入口 | 载荷如何过边界 |
| --- | --- | --- |
| SV → C++ | `shuttle_c_publish_now` / `_serial` / `_concurrent`（import function） | `byte payload[]` open array → `svGetArrElemPtr1` 逐字节拷贝 |
| C++ → SV | `shuttle_sv_dispatch`（export task）/ `shuttle_sv_dispatch_nb`（export function） | **不走形参**：C++ 先把帧放进 thread_local `g_frame`，SV 在 export 体内用 `shuttle_c_frame_len()` + `shuttle_c_frame_qword(i)`（每块 8 字节）拉取 |

反向拉取的原因：VCS 2018.09 的 `svdpi.h` 没有 `svCreateArray`，C++ 无法构造 open array，
且以参数传递时载荷生命周期落在调用者栈帧上。拉取安全的前提是 §4 的单活跃进程不变量：
export 执行期间没有其他进程能修改 `g_frame`。`FrameContext` 是 RAII 的保存/恢复栈，
支持嵌套调用。

### 11.2 作用域修正

```cpp
class SvScopeGuard { ... };                  // 包级 export：进入前 svSetScope(<pkg>)
class WeaveScopeGuard = SvScopeGuard;        // 驱动 weave 的 import 入口切到 weave_pkg
```

- 同一 SV 进程内调用过返回 `string` 的 import（如 `shuttle_c_last_error`）之后，
  包级 export 会变得不可见，VCS 报 `Error-[DPI-DXFNF]`。因此 `dispatch` / `dispatch_nb` /
  `now_fs` 进入时切换到 `shuttle_pkg`，返回时还原调用者作用域；
- 反向：C++ 经 import 去创建 task/事件或挂起等待时，调用链的作用域是 `shuttle_pkg`，
  而 weave 的 export 位于 `weave_pkg`，因此 `shuttle_c_publish_*` / `wait_pub` /
  `subscribe` / `unsubscribe` / `event_*` 都切换到 `weave_pkg`；
- 不涉及 weave 的入口无需 guard：`shuttle_c_set_retained` 与只读查询
  （`pub_done` / `pub_stat` / `pub_error` / `detail::backend::frame_len` /
  `detail::backend::frame_qword` —— 后两件只在 svdpi 后端存在，声明在
  `src/backend/svdpi/svdpi_internal.hpp`，不进 `backend.hpp`：只有 export 体内够得着它们）。

`broker_internal.hpp` 声明、`src/entry.cpp` 实现的门面按**谁在调**分成两段，而不是按后端分：

- `ext_*`（`ext_pub_now` / `ext_pub_serial` / `ext_pub_concurrent` / `ext_sub` / `ext_unsub`）：
  外部调用方进来的动作——字节帧发布、外部订阅者注册。它描述的是「帧从外面来、订阅者在外面」，
  后端是 svdpi 还是将来的 socket / Python 绑定都一样；
- `pub_*`，首参是 `pub_id`（`pub_id` / `pub_done` / `pub_stat` / `pub_error` / `pub_wait` /
  `pub_state` / `pub_on_complete`）：按发布 id 读句柄的通用查询/等待，与后端、与语言都无关。

两组都不含后端字样。后端名字只出现在两处：`Subscriber::handle` 的注释，以及
`detail::backend` 的实现文件。

包名可用 `-DSHUTTLE_SV_PACKAGE=` / `-DSHUTTLE_WEAVE_SV_PACKAGE=` 覆盖。

### 11.3 export/import 的时序约束

DPI `function` import 的调用链内不允许出现 export **task**（`Error-[DPI-INCDIC]`）。
因此 `publish_now` 的同步段只能碰 nonblocking 入口的注册：`dispatch_now` 先按 `cap` 分拨，
同步循环里只有它们（C++ 直接函数调用、SV 侧 `shuttle_sv_dispatch_nb`）。入口为 blocking 的
目标不进同步段：`defer` 转后台（此时已脱离 import 调用链，可走 `shuttle_sv_dispatch`
export task），`drop` 直接不投递，`reject` 前置抛错。

### 11.4 SV 侧注册表与分派

DPI export 名称必须静态，无法"每订阅者一个 export 函数"，因此 SV 侧有一层间接：

```
C++ 只持有整数 handle（= SV 注册表编号 sv_sid）──▶ shuttle_sv_dispatch(task) / _nb(function)
                              └─▶ shuttle_registry::dispatch / dispatch_nb
                                    ├─ sinks_[id].invoke(payload, meta)        (blocking 入口)
                                    └─ sinks_nb_[id].invoke_nb(payload, meta)  (nonblocking 入口)
```

- `sv_sid` 由 SV 注册表分配、C++ 原样透传；`pub_id` 相反，由 C++ 分配、SV 存在句柄里；
- `shuttle_frame_bytes(output byte b[])` 在 export 体内把帧拉成动态数组
  （VCS 不接受函数返回 unpacked 数组）；
- 退订后的在途投递：`retired_[id]` 静默丢弃并调 `shuttle_c_sub_retired` 回告，
  C++ 把该次投递记为 retired（计入 `dropped`，不计 `delivered`）。未登记过的 id 仍报
  `$error`，保留真实缺陷的可见性；
- SV 侧 `$error` 不会传播回 C++，import 仍返回成功；需要严格错误通道时由回调显式返回错误。

### 11.5 `wait_done` 如何挂起普通 SV 进程

```
SV: h.wait_done() ─▶ import task shuttle_c_wait_pub(pub_id, timeout_fs, status)
      C++: pub_state(pub_id)
             ├─ 已完成 → 直接给结论
             └─ 进行中 → w = weave_sv_create_event()
                        pub_on_complete(pub_id, [w]{ weave_sv_trigger(w); })
                        weave_sv_wait(w, ticks, -1, &wst)   ← 挂起调用者这个 SV 进程
                        返回后 weave_sv_destroy_event(w)
                        status = wst==1 ? 超时 : (pub_state==2 ? 有失败 : 成功)
```

不能使用 `weave::Event::wait()`：它只在 weave task 上下文合法，普通 `initial` 进程会抛
`weave: blocking primitive outside task context`。`weave_sv_wait` 是 weave 的 export task，
挂起的是调用者本身。超时换算使用 `unsigned __int128(fs) * ticks_per_sec / 1e15`，避免
64 位溢出。

## 12. systemc 后端

`src/backend/systemc/bridge.cpp` 实现同一组 `detail::backend` 入口，但 `available() == false`：
`dispatch` / `dispatch_nb` 抛 `std::logic_error`（配置错误尽早暴露），`now_fs()` 用
`sc_time_stamp()` 换算，`take_retired_delivery()` 恒为 false。
broker 不感知后端差异，纯 C++ 用法在两种后端下行为一致；SV 相关代码（`svdpi/` 目录、`ext_*`
的调用方、跨语言用例）在 systemc 构建中**根本不参与编译**。

## 13. 句柄与生命周期

| 对象 | 语义 |
| --- | --- |
| `Publication` | `shared_ptr<Impl>`，可拷贝；`done()/wait()/wait(timeout)/finished()/subscriber_count()/excluded()/deferred()/retired()/failures()/result()` |
| `Subscription` | `shared_ptr<Impl>`，**最后一份副本析构即退订**（`Impl::~Impl` → `Broker::remove_sub`）；`unsubscribe()` 幂等 |
| `shuttle_sub`（SV） | `id_` + `active_`，`unsubscribe()` 幂等；销毁不退订（SV 无确定性析构） |
| `shuttle_handle`（SV） | 持 `pub_id`，方法名与 `Publication` 对应：`wait_done(timeout_fs)` / `done()` / `subscriber_count()` / `finished()` / `excluded()` / `deferred()` / `retired()` / `first_error()` |
| `shuttle_event`（SV） | C++ 注册表持有的 id，`set()` / `close()` |

退订即生效：它立刻把订阅者移出目标集（此后的帧不再选中它），也让**已快照**但尚未投递的帧
跳过它——这些投递记 `retired`（不是失败，也不是 `delivered`）。两种语言同一口径；实现与来源
见 §9.1.1。

## 14. 延迟触发

`Trigger` 是对"一次可等待信号"的类型擦除：`std::function<void()>` 表示"等到条件满足"，
空表示立即满足。

| 工厂 | 语义 |
| --- | --- |
| 空 `Trigger` | 立即 |
| `after(weave::duration)` | `weave::sleep(d)`，与其他触发单元互不影响 |
| `after(weave::Event&)` / `after(shuttle::Event&)` | 等待该事件置位 |
| `after(const weave::Task&)` | 等待任务到达终态（不重抛其异常） |
| `after_all(vector<Trigger>)` | 依次等待；任一子触发器抛异常（取消除外）立即中断，后续不再等待 |
| `after_any(vector<Trigger>)` | 每个子触发器各建 task 竞速，先落定者置位（取消与失败也算落定）；空集合立即满足 |

SV 侧 `shuttle_trigger` 打包为 `(trig_kind, trig_arg)` 过 DPI：`NONE` / `DELAY(fs)` /
`EVENT(ev_id)` / `PUB(pub_id)`。等待发生在投递单元内部（`wait_trigger`），因此触发等待
期间可以继续发布、订阅与退订；帧内容与目标集合在发布瞬间已快照。例外是 `after_any`：
竞速 task 在 `after_any()` 调用点即创建，它们只等子触发器、不接触帧。

## 15. 诊断与已知限制

| 手段 | 用途 |
| --- | --- |
| `broker::topics()` / `topic_info(name)` | 每个 topic 的 `type_tag`、订阅者数、retained、`published` / `delivered` / `dropped` / `deferred` |
| `Publication` 的计数 | 本帧目标 / 已结束 / 排除 / 转后台 / 退订跳过（§9） |
| `Publication::failures()` / `result()` | 失败记录 `Failure{subscriber, error}`；`subscriber` 是描述串（`cpp sub N` / `sv sid N` / `<trigger>` / `<delivery>`），文本取自 `error.what()` |
| `shuttle_c_last_error()` / `last_error()` | 立即型 import 的失败原因（thread_local，每进程独立） |
| `AGENTS.md` | 改动前必读的硬约束清单 |

已知限制与后续方向：

- `pubs` 表只增不减，长时间运行的回归需要周期性清理策略（或按 `pub_state` 惰性回收）；
- SV 侧 `$error` 不回流 C++（协议决定），需要严格错误通道时应扩展 `dispatch` 的返回值；
- 跨语言载荷只支持值语义与声明序字段子集，没有变体、指针、引用与句柄引用；
- 背压只在 `SubOptions` 中预留（`max_pending` / `OverflowPolicy`），当前实现不引入队列，
  `serial` / `concurrent` 的排队由 weave 的 task 调度承担；
- `TopicInfo::deferred`（topic 累计）与 `Publication::deferred()`（帧级）同名不同粒度，
  读取时注意区分。

## 16. 构建

```bash
just build                # 按当前后端构建
xmake f --backend=svdpi   # 默认：libshuttle.so + SV 桥（add_requires("svdpi")）
xmake f --backend=systemc # 纯 C++：add_requires("systemc")，无 SV 桥
```

后端**按目录选文件**，不靠宏：`add_files("src/*.cpp")` + `add_files("src/backend/<backend>/**.cpp")`。
换后端 = 换一个目录，两个后端目录互不相见（因此也无需 `#if` 包住整个 TU）；`SHUTTLE_BACKEND_*`
宏已删除，不再有任何东西外传给使用者。公开 API 与 broker 逻辑在两种后端下完全相同，
差异只有 `detail::backend` 的实现与「有没有 SV 桥」。
