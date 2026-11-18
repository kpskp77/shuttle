# shuttle — EDA 验证环境的发布-订阅框架

shuttle 为验证环境提供**发布-订阅**通信：C++ 组件之间、SV 组件之间，以及 C++ ↔ SV
跨语言，使用同一套 API。并发编排（task / event / 等待 / 超时 / 取消）全部复用
[weave](../weave)，shuttle 在其上提供三层：broker（路由、类型绑定、完成跟踪）、
类型化载荷（编解码与跨语言物化）、DPI 桥。

```
┌────────────────────────── 同一仿真进程 ──────────────────────────┐
│  C++ 组件                     broker                    SV 组件  │
│  Publisher<T> ──publish──▶ 匹配 / 类型绑定 / seq ──▶ shuttle_registry
│  subscribe<T>(fn|nb) ◀────────── deliver ──────────▶ shuttle_sink(_nb)
└──────────────────────────────────────────────────────────────────┘
   发布方式决定调度：publish_now（发布者栈内）· publish_serial（后台单链）· publish_concurrent（后台并发）
```

非目标：跨机器通信、消息持久化、流量整形、UVM 兼容层。后端二选一：
`svdpi`（真实仿真器，带 SV 桥）或 `systemc`（纯 C++，无 SV 桥，API 相同）。

## 文档地图

| 文档 | 职责 |
| --- | --- |
| 本文 | 能力、用法与选型：读完即可使用 |
| [`docs/architecture.md`](docs/architecture.md) | 实现结构与不变量：数据结构、记账契约、三条发布路径、后端边界 |
| [`docs/dpi_protocol.md`](docs/dpi_protocol.md) | C++ ↔ SV 边界的权威定义：import/export 表、常量、线上格式、仿真器约束 |
| [`examples/README.md`](examples/README.md) | 示例清单与预期输出 |
| [`AGENTS.md`](AGENTS.md) | 修改本仓库前必读的硬约束 |

## 快速开始

```cpp
#include <shuttle/shuttle.hpp>

struct UartTx { std::uint32_t addr, data; };
SHUTTLE_REFLECT(UartTx, addr, data)               // 生成类型标签与编解码

shuttle::Publisher<UartTx> pub("uart0.tx");       // 首次构造时绑定类型
auto mon = shuttle::subscribe<UartTx>("uart0.tx", // blocking 入口：可消耗仿真时间
    [](const UartTx& tx, const shuttle::Meta& m) { /* ... */ });

auto h = pub.publish_serial(UartTx{0x10, 0xA5});  // 立即返回句柄，回调在后台按序执行
h.result();                                      // 等全部订阅者结束，重抛第一个失败
```

```systemverilog
class Monitor implements shuttle_sink;                            // blocking 入口
  virtual task invoke(input byte payload[], input shuttle_meta meta);
    UartTx tx = UartTx::unpack(payload);
  endtask
endclass

shuttle_sub    sub = shuttle_subscribe("uart0.tx", mon, UartTx::type_tag);
shuttle_pub #(UartTx) pub = new("uart0.tx");
shuttle_handle h   = pub.publish_serial(tx);
h.wait_done();                                                    // 挂起当前 SV 进程
```

两侧共用同一个 broker，同一仿真内的 C++ 与 SV 订阅者互相可见。

## 三种发布方式

发布方式只决定**调度**；**给谁发**在三种方式下相同——都是本帧全部匹配订阅者
（唯一例外是 `publish_now` 的 `skip` 策略会跳过入口为 blocking 的目标）。

| 方式 | C++ | SV | 调度方式 | 顺序 | 返回时状态 |
| --- | --- | --- | --- | --- | --- |
| `now` | `publish_now(msg, policy)` | `publish_now(msg, SHUTTLE_NOW_*)` | 发布者进程内同步，全程不出让调度权 | 按注册顺序 | 入口为 nonblocking 的目标已执行完；入口为 blocking 的目标按策略处置（默认转后台链） |
| `serial` | `publish_serial(msg, trig)` | `publish_serial(msg, trig)` | 后台**一个** weave task（topic 级） | 严格 FIFO（按入队顺序；延迟帧在触发时刻入队） | 立即返回句柄，回调尚未开始 |
| `concurrent` | `publish_concurrent(msg, trig)` | `publish_concurrent(msg, trig)` | 后台**每订阅者一个** weave task | 每个订阅者内部 FIFO（按入队顺序）；订阅者之间并行、无相互顺序 | 立即返回句柄，回调尚未开始 |

选型：

- 默认用 `serial`。同 topic 的后台投递顺序等于入队顺序（即时帧即发布顺序），排查问题时最容易复现。
- 要求当帧生效、且回调不消耗仿真时间（例如配置下发、握手应答）用 `now`。
- 订阅者之间互不依赖、单个回调耗时可观时用 `concurrent`：订阅者之间由 weave 调度器并发推进，
  而同一个订阅者收到的连续多帧仍按发布顺序串行进入回调（每个订阅者一条链），所以有状态的
  checker 不必再自己加队列。

`serial` / `concurrent` 支持**延迟触发**：发布后不立即回调，条件满足才启动（`Trigger` /
`shuttle_trigger`，支持延时、事件、另一个发布完成、all/any 组合）。三种方式都遵循**快照
语义**：调用瞬间锁定订阅者集合与消息拷贝。延迟帧不占用串行链的队头：它在触发时刻才入队，
因此一个长期未满足的触发器不会把同 topic 的后续帧一起拖住。

## 订阅入口

订阅者声明的是**入口**，`blocking` / `nonblocking` 描述回调能否消耗仿真时间；
**登记哪条入口由注册时调用的函数决定**，两种语言一一对应：

| 入口 | C++ | SV | 可被谁调用 |
| --- | --- | --- | --- |
| blocking | `subscribe<T>(topic, fn)` | `shuttle_subscribe(topic, obj)`（`shuttle_sink::invoke`，export task） | `serial` / `concurrent` 的投递单元；`now` 的后台链 |
| nonblocking | `subscribe<T>(topic, nb(fn))` | `shuttle_subscribe_nb(topic, obj)`（`shuttle_sink_nb::invoke_nb`，export function） | `now`（发布者栈内）；`serial` / `concurrent`（该注册只有这一条入口） |

规则只有一条：**一次注册 = 一个订阅者 = 一条入口 = 每帧至多一次回调**。

- 两条入口都要 → 注册两次（C++ 两个 lambda，SV 同一对象调两个注册函数），即两个订阅者、每帧两次回调；
- 同一入口需要两种行为 → 两个对象或两个闭包（SV 无闭包，用两个对象或适配器）；
- SV 一个类可以同时 `implements shuttle_sink, shuttle_sink_nb`（interface class 不占单继承名额），
  但登记哪条入口只取决于注册函数。

`publish_now` 无法在发布者栈内调用 blocking 入口，因此这部分目标按 `NowPolicy` 处置：

| 策略 | C++ | SV | 行为 |
| --- | --- | --- | --- |
| 报错 | `NowPolicy::reject` | `SHUTTLE_NOW_REJECT` | 抛 `CapabilityError`（SV 返回 `null` + `shuttle_c_last_error()`）。**帧从不发布**：`seq`、retained 快照、`published` 均不推进 |
| 跳过 | `NowPolicy::skip` | `SHUTTLE_NOW_SKIP` | 跳过这些目标，计入 `skipped()` 与该订阅者的 `dropped()`（`skipped` 分量） |
| 转后台（默认） | `NowPolicy::defer` | `SHUTTLE_NOW_DEFER` | 整帧交给该 topic 的后台串行链（与 `serial` 同一条链），返回可等待句柄 |

## 邮箱：等下一帧

`Mailbox<T>`（C++）/ `shuttle_mailbox`（SV）把“等下一帧”做成可等待对象：它就是一个
nonblocking 订阅者 + 一个队列，因此照样进目标集、照样出现在 `subscriber_count()`、照样收 retained
回放。单响应者、无需关联 id 的问答用它就够了（下面是最小用法）；多响应者或并发在途请求需要关联号
时用 `Requester`——两者是兄弟，不是上下层。

```cpp
// C++（纯头文件，零 DPI、零 broker 改动）
shuttle::Mailbox<Resp> resps("dev.resp");
auto req = shuttle::Publisher<Req>("dev.req").publish_serial(Req{...});
req.wait();                                  // 响应者回调都已返回
if (auto got = resps.recv(1_ms)) use(got->value, got->meta);   // 超时给 nullopt，帧不丢
```

```systemverilog
// SV（纯 SV 实现，用 weave_pkg 的事件原语，不新增 DPI）
shuttle_mailbox resps = new("dev.resp");
byte payload[]; shuttle_meta meta; bit ok;
resps.recv(ok, payload, meta, 1us);
```

- 入口固定为 **nonblocking**（回调只做入队），所以 `publish_now` 也能**同步**填邮箱；
- `recv()` / `recv(timeout)` / `try_recv()` / `size()` / `received()` / `overflowed()` / `close()`；
- 容量：默认无界，`SubOptions.max_pending` 有界时必须给 `drop_oldest` / `drop_newest` / `reject`
  （`block` 构造时报错——回调里阻塞会死锁发布者）；
- 语义分工：邮箱回答"帧到了"，`Publication` 回答"所有回调都结束了"，请求-响应通常两个都用。

## 请求-响应

与邮箱**兄弟**的一层糖：`Requester`（请求方）+ `serve`（响应方）。关联号走**帧头**
（`Meta::corr_id`），载荷里不需要任何握手字段——第三方类型可以直接当请求/回执载荷。

```cpp
// 响应方：handler 返回回执，框架自动把请求的 corr_id 回填进回执帧头
shuttle::serve<Req, Resp>("dev.req", "dev.resp",
                          [](const Req& r, const shuttle::Meta&) { return Resp{business(r)}; });

// 请求方
shuttle::Requester<Req, Resp> client("dev.req", "dev.resp");
auto call = client.send_serial(Req{21});      // corr_id 自动分配，随帧头发布
auto resps = call.wait_all(1_ms);             // 每个目标订阅者回一条
if (auto first = call.recv(1_ms)) use(*first); // 或逐条取
```

- 发送方式与 `Publisher` 一一对应、同名同参：`send_serial(msg, trig)`（默认，保序）、
  `send_concurrent(msg, trig)`、`send_now(msg, NowPolicy)`；
- 多个未完成请求互不干扰：回执按 `corr_id` 分发到各自的 `Call`；计数器是进程级的，多个
  `Requester` 共用回执 topic 也不会串号；
- 超时给空结果、帧不丢；`cancel()` 或丢掉句柄会释放槽位，迟到的回执记 `stale_responses()`；
- `wait_for(n, timeout)` 显式指定期望条数，`wait_all()` 用"每个请求目标回一条"的默认约定；
- 入口和 `subscribe` 一样按参数类型选：裸 handler = blocking，`shuttle::nb(handler)` = nonblocking；
- **回执走 `publish_now`**：响应者用 `nb(handler)` 时整条往返在请求方栈上跑完——`send_now()`
  返回时回执已经在 `Call` 里（`call.queued() == 1`），不切任务、不耗仿真时间；
- **回执 topic 是响应通道**：在它上面用 blocking 入口注册会被当场拒绝（`CapabilityError`，通配符
  按匹配判定），因为 blocking 旁观者会拖慢每一帧回执、让超时说谎；看回执请用 `nb()`。名字本身
  仍然公开（`resp_topic()`），手工往回执 topic 发布依旧合法；
- 跨语言可用：SV 侧订阅请求 topic，回执用 `pub.reply(resp, meta)` 发——它把请求帧头里的
  `corr_id` 原样抄进回执（见 `tests/sv/shuttle_tests.sv` 的 `SvRqSink`）；
- 关联号对用户不可见：`publish_now` / `publish_serial` / `publish_concurrent` 不带这个参数
  （也不该带，普通发布恒为 `corr_id = 0`），写它的只有 `send_*`、`serve` 与 SV 的 `reply`。

## 完成句柄与计数

三种方式返回同一个句柄类型：C++ `Publication`，SV `shuttle_handle`。句柄同时是**可见性
凭证**——任何"本帧没能成功送到某订阅者"都能查到数字：

| 方法 | 含义 |
| --- | --- |
| `subscriber_count()` | 本帧目标数（名字 ∩ 类型，不含被 `skip` 跳过的） |
| `finished()` | 已达终态的目标数 |
| `skipped()` | 被 `skip` 跳过的目标数（仅 `now`） |
| `deferred()` | 转到后台链的目标数（仅 `now` 的 `defer`；返回时未开始，完成后不回零） |
| `retired()` | 本帧快照后被退订、回调未进入的目标数（退订即生效，两种语言同口径） |
| `failures()` / `first_error()` | 失败明细（`Failure{subscriber, error}`） |
| `wait()` / `wait(timeout)` / `wait_done(timeout_fs)` | 等待结束 |
| `done()` | 是否已全部结束 |
| `result()` | 等待并重抛第一个失败（C++） |

三条不变量对三种方式都成立：

1. `done()` 当且仅当 `finished() == subscriber_count()`；
2. `finished()` 只累计三种终态：投递、退订跳过、失败；
3. 订阅者侧 `received() + dropped() == 本帧名字∩类型命中它的次数`（含被 `skip` 跳过的），且
   `dropped() == retired + skipped + failed`；帧级 `subscriber_count()` 则不含被 `skip` 的。

失败聚合粒度不同：`concurrent` 逐订阅者记录全部失败；`serial` 只保留第一条；`now` 的同步段
只抛出第一条（其余仍记入 `failures()`），被 defer 的部分按 `serial` 规则并入同一句柄。
详见 [`docs/architecture.md`](docs/architecture.md) 的记账契约一章。

## 载荷与类型

- topic 绑定**类型标签**（C++ `SHUTTLE_REFLECT(Type, fields...)` 的类型名 ↔ SV
  `localparam` `type_tag`），不一致拒绝发布；标签冲突不会拖到投递期：具体 topic 的冲突在
  订阅时或**首次绑定时**就抛 `TypeMismatchError`（SV 只带标签登记、订阅可以早于绑定，
  绑定那一刻做反向校验）；通配符订阅者按帧参与**目标集判定**——类型标签不相同的帧不算
  目标，既不进回调也不留计数；
- C++ 内部投递**零拷贝**（`shared_ptr<const T>`）；只有存在字节订阅者（SV 订阅者或
  `subscribe_bytes`）或帧本身来自 SV 时才编解码，一帧多订阅者只编一次；
- 线上格式：字段按声明序、大端、`string`/`vector` 带 uint32 长度前缀，与 SV
  `{>>{...}}` 流式打包逐字节一致。权威定义见
  [`docs/dpi_protocol.md`](docs/dpi_protocol.md) 的线上格式一节。

需要绕过类型系统时用 `subscribe_bytes(topic, fn)`：回调收到 `const std::vector<std::byte>&`
与 `Meta`，不做物化（非阻塞入口同样写作 `subscribe_bytes(topic, nb(fn))`）。

## 延迟触发

`Trigger`（C++）/ `shuttle_trigger`（SV）只决定**回调何时开始**，不影响"给谁发、发什么"。

| 条件 | C++ | SV |
| --- | --- | --- |
| 立即（默认） | 空 `Trigger{}` | `null` |
| 固定延时 | `after(weave::duration)` | `shuttle_trigger::after_delay(fs)` |
| 事件置位 | `after(weave::Event&)`（C++ 自己的事件） | `shuttle_trigger::after_event(ev.id())`（SV `shuttle_event`） |
| 另一个发布完成 | `after(const weave::Task&)` 或经桥装配 | `shuttle_trigger::after_pub(h)` |
| 全部满足 / 任一满足 | `after_all(vector)` / `after_any(vector)` | —（组合在 C++ 侧构造） |

`after_all` 依次等待，任一子触发器抛异常（取消除外）即中断；`after_any` 为每个子触发器
各建一个 task 竞速，先落定者放行，空集合立即满足。

## API 速查

| 文件 | 内容 |
| --- | --- |
| `include/shuttle/shuttle.hpp` | 伞头：包含全部公开 API |
| `include/shuttle/publisher.hpp` | `Publisher<T>`：`publish_now` / `publish_serial` / `publish_concurrent` / `set_retained` |
| `include/shuttle/subscription.hpp` | `subscribe<T>`（普通 / `nb()`）、`subscribe_bytes`、`Subscription` |
| `include/shuttle/publication.hpp` | `Publication`：完成句柄、可见性计数、失败明细 |
| `include/shuttle/trigger.hpp` | `Trigger`（`after(weave::Event&)` 等工厂） |
| `include/shuttle/types.hpp` | `Capability`、`NowPolicy`、`SubOptions`、`nb()` |
| `include/shuttle/codec.hpp` | `Codec<T>`、`Reader`/`Writer`、`SHUTTLE_REFLECT` |
| `include/shuttle/meta.hpp` | `Meta{topic_name, type_tag, topic_seq, time_fs}`、`now_fs()` |
| `include/shuttle/broker.hpp` | 只读内省：`topics()` / `topic_info()` |
| `include/shuttle/exception.hpp` | `ShuttleError` 及其子类 |
| `include/shuttle/detail/abi.hpp` | 类型擦除边界，供 `Publisher`/`subscribe` 模板使用；不直接 include |
| `sv/shuttle_pkg.sv` | SV 侧：`shuttle_sink(_nb)`、`shuttle_pub#(T)`、`shuttle_sub`、`shuttle_handle`、`shuttle_trigger`、`shuttle_event`、注册表、DPI |

## 构建

依赖 weave（xmake-repo，`backend=svdpi|systemc`）。常用动作由 `justfile` 提供：

```bash
just build     # 按当前 xmake 后端构建库与示例
just sc        # systemc 后端本地运行示例（预期末行 "WEAVE TEST PASSED"）
just vcs       # svdpi 后端 + VCS 真仿真（预期 "SHUTTLE CPP DEMO PASSED"）
```

等价的 xmake 命令：

```bash
xmake f --backend=systemc && xmake build shuttle_examples_sc && xmake run shuttle_examples_sc
xmake f --backend=svdpi   && xmake run shuttle_examples_svdpi_vcs   # 需 xmake run 准备库搜索路径
```

`xmake run shuttle_examples_svdpi_vcs` 依次编译 `weave_pkg.sv` → `shuttle_pkg.sv` →
`shuttle_demo.sv`，以 `-sv_root`/`-sv_lib` 加载 `libshuttle_examples_svdpi.so`。
示例清单与预期输出见 [`examples/README.md`](examples/README.md)。

## 测试

`tests/` 与 `examples/` 平行：示例示范用法，测试只断言行为（失败即 `[FAIL]`，整体判失败）。

```bash
just test-sc    # systemc 后端：tests/cpp 46 个用例（跨语言用例不参与编译）
just test-vcs   # VCS 后端：tests/cpp 49 个用例 + tests/sv 79 个断言
just test-all   # 两者都跑
```

| 文件 | 覆盖内容 |
| --- | --- |
| `tests/cpp/codec_tests.cpp` | 线格式黄金字节、各宽度大端与补码、IEEE754、长度前缀、enum 底层类型、嵌套 struct、越界抛 `DecodeError`、类型标签与 `has_codec_v` |
| `tests/cpp/broker_tests.cpp` | 通配符 `*` / `>`、retained、退订幂等、句柄共享自动退订、`topic_info`、多发布者 seq 单调、类型绑定冲突、类型不相交的帧不入目标集（含绑定时的通配符豁免）、topic 名与订阅模式的语法校验（`TopicPatternError`，被拒时不留 topic 条目） |
| `tests/cpp/publish_tests.cpp` | `NowPolicy` 三策略、`wait/wait(timeout)/done/result/failures`、计数契约、失败聚合不影响其余目标、零目标立即完成、类型不相交即零目标、入口选择、`defer` 与 `serial` 共享 FIFO |
| `tests/cpp/requester_tests.cpp` | 请求-响应：单/多响应者、`corr_id` 分发、`wait_all` / `wait_for` / `recv` 超时、`cancel` 与迟到回执、响应者抛异常计入失败、非阻塞响应者 + `send_now`、帧头关联号（载荷无握手字段）、进程级计数器、同 topic 拒绝 |
| `tests/cpp/mailbox_tests.cpp` | 邮箱：`now` 同步填充、`serial`/`concurrent` 后台填充、超时不丢帧、三种溢出策略、`close` 唤醒、retained 回放、通配符、字节口味、请求-响应往返 |
| `tests/cpp/trigger_tests.cpp` | `after(duration/Event/Task)`、`after_all` / `after_any`、触发等待期间退订（在途帧记 retired）、延迟帧不阻塞后续同 topic 帧、触发器由后续帧回调放行 |
| `tests/cpp/xlang_tests.cpp` | 跨语言往返（仅 svdpi 后端）：就绪握手、双向各三帧、类型标签与 seq 单调 |
| `tests/sv/shuttle_tests.sv` | 三种发布方式、入口由注册函数决定、纯 nonblocking 订阅者也可收到 `serial`/`concurrent`、句柄计数、retained、多发布者、退订与在途投递、订阅先于绑定的标签冲突、通配符 topic 发布被拒（`publish_now` 返回 null + last_error）、快照、`shuttle_event`、跨语言往返 |

两个后端共用同一 broker，同一仿真内两侧互相可见：`tests/sv/shuttle_tests.sv` 先跑 SV 用例，
再用 `weave_run` 驱动 C++ 侧用例，两侧都通过才打印 `SHUTTLE TESTS PASSED`
（`tests/svdpi/run.sh` 以此为成功标记；无 VCS 的机器上打印 `[svdpi-test] skipped` 并以 0 退出）。

## 目录

```
include/shuttle/   公开头文件（用户只用这一层）
src/               broker 实现（后端无关）
                   + backend/svdpi/ | backend/systemc/（按目录选一个后端；
                   svdpi 目录内含外部门面 facade.cpp，见 docs/architecture.md）
sv/shuttle_pkg.sv  SV 侧包（依赖 ../weave/sv/weave_pkg.sv）
docs/              architecture.md（实现） / dpi_protocol.md（边界协议）
examples/          用户侧示范（C++ / SV）与运行说明
tests/             测试套件（cpp/ + sv/）
tests/svdpi/       VCS 运行脚本
```
