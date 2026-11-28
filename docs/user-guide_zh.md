# shuttle 用户指南

[English](user-guide.md) | 简体中文

shuttle 的概念、术语与公开 API。建议在读完 [README](../README.md) 的快速上手之后再读本
文；每条保证背后的设计理由在 [architecture](architecture_zh.md)，DPI 协议在
[backends](backends_zh.md)。

## 执行模型

shuttle 的一切都从一条前提出发：**仿真进程是协程，不是线程。** 任一时刻只有一个进程在
跑，它只在 weave 的显式阻塞点挂起，绝不抢占。日常要用的推论：

- 回调一路跑到下一个挂起点，中间不受打扰。broker 里没有锁，订阅者代码里也不需要。
- "阻塞"指的是*本进程等仿真时间*，不是*线程停摆*。回调里等待，冻住的只有自己。
- `publish_now` 的同步段跑在发布者自己的栈上：发布者不会被重入，也不会在那里挂起。

并发编排（任务、时间、事件）全部交给 weave。shuttle 没有自己的时钟和调度器——仿真时间
永远是 `weave::now()`。

## 核心概念

### topic、pattern、type tag

**topic** 是点分的名字（`uart0.tx`）。发布侧只用具体名。订阅侧可以用 NATS 风格的通配
**pattern**：`*` 恰好匹配一段，`>` 匹配任意非空尾巴且必须是最后一段（`uart0.*`、
`uart.>`）。在发布/声明名里写通配符会抛 `TopicPatternError`。

每个 topic 在首次声明时绑定一个 **type tag**；同一 topic 用不同的 tag 再声明，抛
`TypeMismatchError`。tag 在 C++ 侧是 `SHUTTLE_REFLECT` 给的那个字符串，在 SV 侧是
`localparam type_tag` / `type_tag` 实参——跨语言投递要求两边一致。

### 订阅者与入口

**订阅者**就是一次注册的回调。每次注册恰好声明一个**入口**，在注册调用那一刻定死，不是
运行时开关：

| 入口 | C++ 注册 | SV 注册 | 能否消耗仿真时间 |
|---|---|---|---|
| blocking | `subscribe<T>(topic, cb)` | `shuttle_subscribe(topic, sink, tag)` | 能 |
| nonblocking | `subscribe<T>(topic, nb(cb))` | `shuttle_subscribe_nb(topic, sink_nb, tag)` | 不能（零延迟） |

入口决定回调*能在哪里跑*：

- `publish_serial` / `publish_concurrent` 调注册时用的那个入口。
- `publish_now` **只同步调用 nonblocking 入口**（跑在发布者栈上）；blocking 目标先被分
  流出去，按 `NowPolicy` 处理。

一次注册 = 一个订阅者 = 一个入口。同一个对象想要两种行为，就注册两次（两个句柄、两套
计数器、各自退订）。

`nb(fn)` 的约定（"这个回调绝不阻塞"）由用户自己守，broker 不做运行时检查。nonblocking
回调若在 `publish_now` 里挂起，挂起的就是发布者自己。

### 三种发布方式

`Publisher<T>`（C++）与 `shuttle_publisher #(T)`（SV）提供同一组三件套。方法名只表达
**调度**；回调用哪个入口由订阅者的注册决定：

| 方式 | 回调在哪里跑 | 顺序 | 句柄 |
|---|---|---|---|
| `publish_now(msg, policy)` | 同步跑在发布者栈上（仅 nonblocking 入口） | 严格 FIFO | 同步段结束后返回 |
| `publish_serial(msg, trig)` | 后台，每 topic 一条串行链 | 每 topic FIFO，按入链顺序 | 立即返回 |
| `publish_concurrent(msg, trig)` | 后台，每订阅者一个任务 | 每 (topic, 订阅者) FIFO；订阅者之间并行 | 立即返回 |

三种方式都在调用那一刻做**快照**：报文拷贝与订阅者集合当场锁死，之后的订阅/退订不改变
本帧发给谁。退订仍然立即生效——快照里的订阅者若在回调运行前退订，这一帧落在 `retired`
终态，而不是 `delivered`。

跨方式的顺序不作承诺：同一 topic 的一个 `concurrent` 帧和一个 `serial` 帧可能互相超车。

**自等待陷阱。** 在回调里向自己的 topic 发布，再对新帧 `wait()`，按构造就是死锁（内层帧
排在那帧正在等待的帧后面）。shuttle 在等待点诊断出这种情况并经 `weave::fatal` 终止运
行。只发布不等待（转发）是合法的；换一个 topic，或者用 `Mailbox`。

### NowPolicy

`publish_now` 遇到入口为 blocking 的订阅者（无法在发布者栈上调用）时怎么办：

- `NowPolicy::defer`（默认）——交给该 topic 的后台串行链；返回的 `Publication` 覆盖它
  们的完成。
- `NowPolicy::skip`——从本帧目标集合中去掉；计入 `skipped()` 和 `dropped()` 的
  skipped 分量。
- `NowPolicy::reject`——抛 `CapabilityError`，帧根本不发布（类型绑定、seq、retained
  快照都不动）。

### Publication：每一帧都有账可查

每个目标订阅者恰好落在一个**终态**，两种语言一致：

| 终态 | 含义 |
|---|---|
| `delivered` | 回调跑到结束 |
| `failed` | 回调抛异常 / 帧无法实体化 / 投递被取消 / 触发条件失败 |
| `retired` | 快照之后、回调之前退了订 |
| `skipped` | 发布者策略决定不投递（仅 `NowPolicy::skip`） |
| `deferred` | 被 `publish_now` 移交后台串行链（`NowPolicy::defer`） |

`Publication` 句柄把这些都暴露成计数器，不变式有两条：`finished() ≤
subscriber_count()`，`done() ≡ finished() == subscriber_count()`；且
`subscriber_count() + skipped()` 等于本帧的完整目标集。一个订阅者失败不影响其他订阅者；
`result()` 等待并重抛第一个失败。`wait()` 返回只是关于计数器的陈述——相信它之前先看
`failures()`（C++）或 `first_error()`（SV）。

### 帧的标识与 Meta

每次投递都带一份 `Meta`（C++）/ `shuttle_meta`（SV），两边内容一致：

| 字段 | 含义 |
|---|---|
| `topic_name` | 实际发布的具体 topic（通配订阅拿到的也是真名，不是 pattern） |
| `type_tag` | 消息类型 tag |
| `topic_seq` | 每 topic 从 1 起的计数器，由 broker 分配，跨两种语言、跨所有 publisher 连续——`(topic_name, topic_seq)` 就是帧的身份 |
| `corr_id` | 关联 id，由 `Requester` 分配、应答方回显；0 = 无关联。放在帧头里，payload 不需要加字段 |
| `time_fs` | 发布时的仿真时间（飞秒），取自 `weave::now()` |

### retained topic

`set_retained(true)` 让 broker 锁存每个 topic 的最新一帧，新订阅者在注册时立即收到重放
（SV blocking 入口走后台仿真进程异步重放，其余都在注册调用内同步完成）。没有 retained
历史，也没有 QoS 等级。

### Mailbox

**Mailbox** 是带队列的订阅："等下一帧"，而不是"让回调处理每一帧"。它注册的是
nonblocking 入口（内部回调只把帧拷进队列），所以 `publish_now` 会同步填满 mailbox。
mailbox 是普通订阅者：计入 `subscriber_count()`，收 retained 重放，也支持通配 pattern。

有界 mailbox（`SubOptions{max_pending, overflow}`）必须选 `drop_oldest`、
`drop_newest` 或 `reject`——`block` 在构造时就被拒绝，因为投递回调绝不能阻塞。溢出记账
与订阅记账分开：`overflowed()` 数的是进队后又被丢掉的帧，`Subscription::dropped()` 数的
是根本没到订阅者的帧。

### Requester / serve

在同一组原语之上的请求/应答语法糖：一个请求 topic、一个应答 topic，关联 id 放在帧头
里。`Requester` 分配 `corr_id`、订阅应答 topic，把应答按 id 分派到每个 `Call` 的队列；
`serve` 订阅请求 topic，调用你的 handler，把返回值带上回显的 id 发出去。请求与应答
topic 必须不同（应答方在请求回调里发应答——同一个 topic 会把串行链死锁）。

`wait_all(timeout)` 采用"每个目标订阅者恰好应答一次"的约定（`n =
subscriber_count()`）；应答方可能不答时，用 `wait_for(n, timeout)` 或
`recv(timeout)`。

### Trigger

`Trigger` 推迟后台（`serial`/`concurrent`）发布的*启动*时机；快照仍在调用时做，所以
trigger 只决定*何时*，不决定*发给谁*、*发什么*。等待期间的帧不占住 topic 的链——触发时
才入链，所以延迟的 `publish_serial` 不会堵住同 topic 的后到的帧（这类帧的投递顺序是入链
顺序，不是发布顺序）。

## 术语表

| 术语 | 定义 |
|---|---|
| topic | 点分的消息名；发布侧用具体名，订阅侧可用通配 pattern |
| pattern | 带通配符的订阅名：`*` = 一段，`>` = 结尾任意段 |
| type tag | 把 topic 绑定到一个消息类型的字符串；来自 `SHUTTLE_REFLECT` / SV `type_tag` |
| frame | 一次发布的报文加它的 `Meta`；以 `(topic_name, topic_seq)` 标识 |
| subscriber | 一次注册的回调；一次注册 = 一个订阅者 |
| entry | 回调的能力类别：blocking 或 nonblocking，注册时定死 |
| target set | 按名字 ∩ 类型匹配到本帧的订阅者集合，快照时定死 |
| snapshot | 报文拷贝 + 目标集合，在发布调用时锁定 |
| chain | 每 topic（`serial`）或每订阅者（`concurrent`）的待投递 FIFO |
| terminal state | 一个目标的投递结局：delivered / failed / retired / skipped / deferred |
| publication | 一帧的完成句柄与计数器 |
| retained | 每 topic 锁存最新一帧，订阅时重放 |
| mailbox | 自带队列（有界或无界）的 nonblocking 订阅者 |
| corr_id | 帧头里的关联 id；请求/应答的分派键 |
| backend | `svdpi`（跨语言，跑在仿真器下）或 `systemc`（纯 C++） |

## 公开 API —— C++

头文件都在 `include/shuttle/` 下；包含总头 `<shuttle/shuttle.hpp>` 即可。任何阻塞调用
（`wait`、`recv`、trigger 等待）都需要 weave 任务上下文。

### 声明消息

```cpp
struct UartTx { std::uint32_t addr, data; };
SHUTTLE_REFLECT(UartTx, addr, data)          // 类型 tag "UartTx" + 编解码
SHUTTLE_REFLECT_DERIVED(Pkt, Frame, data)    // 派生类：基类经 Codec<Base> 编解码
```

codec 支持的成员类型：整数、浮点、`bool`、`std::string`、枚举（按底层宽度）、
`std::vector<T>`、`std::array<T,N>`、已注册的嵌套结构体——最多 64 个字段，按声明顺序、
大端编码。没写 `SHUTTLE_REFLECT` 的类型仍可用于纯 C++ topic（零拷贝）；跨语言投递必须有
注册。

### 发布

```cpp
shuttle::Publisher<UartTx> pub("uart0.tx");        // 构造时绑定 topic ↔ 类型

auto h0 = pub.publish_now(msg);                     // 默认 NowPolicy::defer
auto h1 = pub.publish_now(msg, shuttle::NowPolicy::skip);
auto h2 = pub.publish_serial(msg);                  // Trigger {} = 立即开始
auto h3 = pub.publish_serial(msg, shuttle::after(ev));
auto h4 = pub.publish_concurrent(msg, shuttle::after(10_us));

pub.set_retained(true);
pub.topic();                                        // topic 名
```

报文按值传入：prvalue 实参（`publish_serial(Msg{...})`）直接省略拷贝进帧，不会多拷一次。

### 订阅

```cpp
auto s1 = shuttle::subscribe<UartTx>("uart0.tx",
    [](const UartTx& tx, const shuttle::Meta& m) { /* blocking 入口 */ });

auto s2 = shuttle::subscribe<UartTx>("uart0.*",
    shuttle::nb([](const UartTx& tx, const shuttle::Meta& m) { /* nonblocking 入口 */ }));

auto s3 = shuttle::subscribe_bytes("uart0.>",       // 原始字节，无 codec
    [](const std::vector<std::byte>& b, const shuttle::Meta& m) { });
```

`Subscription`（共享语义；最后一个句柄析构时自动退订）：`unsubscribe()`（幂等，计数器仍
可读）、`valid()`、`topic()`（pattern 原样保留）、`capability()`、`received()`、
`dropped()`。

### 等待发布完成

```cpp
h2.wait();                       // 所有目标结束（需要任务上下文）
if (!h2.wait(1_ms)) { /* 超时 */ }
h2.result();                     // 等待 + 重抛第一个失败

h2.subscriber_count();           // 快照时的目标数（不含 skipped）
h2.finished();                   // 已落终态的目标数
h2.done();                       // finished() == subscriber_count()
h2.skipped(); h2.deferred(); h2.retired();
for (const auto& f : h2.failures()) { /* f.subscriber, f.error */ }
```

### 触发器

```cpp
shuttle::after(weave_event);        // 粘性的 weave::Event
shuttle::after(pulse);              // weave::Pulse（边沿语义）
shuttle::after(edge);               // weave::sv::Edge，按名字跨语言
shuttle::after(10_us);              // 仿真延迟
shuttle::after(task);               // 某个 weave 任务到达终态
shuttle::after_all({t1, t2});
shuttle::after_any({t1, t2});       // 必须在任务上下文里构造
```

### Mailbox

```cpp
shuttle::Mailbox<Resp> mb("dev.resp", {16, shuttle::OverflowPolicy::drop_oldest});
auto item = mb.recv();              // 阻塞；排空后关闭抛 MailboxClosed
auto maybe = mb.recv(1_ms);         // std::optional；nullopt = 超时
auto now   = mb.try_recv();         // 不阻塞，不抛异常
mb.size(); mb.closed(); mb.received(); mb.overflowed(); mb.subscription();
mb.close();                         // 幂等；已入队的帧仍可读
shuttle::mailbox_bytes("raw.>");    // 字节 mailbox
```

### 请求 / 应答

```cpp
// 应答方（任意多个，任意语言）
shuttle::serve<Req, Resp>("dev.req", "dev.resp",
    [](const Req& r, const shuttle::Meta&) { return Resp{...}; });      // blocking 入口
shuttle::serve<Req, Resp>("dev.req", "dev.resp", shuttle::nb(handler)); // nonblocking

// 请求方
shuttle::Requester<Req, Resp> client("dev.req", "dev.resp");
auto call = client.send_serial(Req{21});        // 也有 send_now / send_concurrent
auto rsps = call.wait_all(1_ms);                // n = 请求帧的 subscriber_count()
auto rsp  = call.recv(1_ms);                    // 等一个应答，optional
call.wait_for(2, 1_ms); call.cancel();
call.corr_id(); call.publication(); call.queued(); call.cancelled();
client.inflight(); client.stale_responses();
```

### 内省

```cpp
shuttle::broker::topics();                       // 至少发布过一帧的 topic
auto info = shuttle::broker::topic_info("uart0.tx");
// info->published / delivered / dropped / retired / skipped / failed /
//      deferred / subscriber_count / type_tag / retained
```

### 异常

都派生自 `shuttle::ShuttleError`（本身是 `std::runtime_error`）：`PublishError`（类型绑
定冲突、编码失败）、`CapabilityError`（`NowPolicy::reject`）、`TypeMismatchError`、
`TopicPatternError`（通配符误用）、`DecodeError`、`MailboxClosed`、
`MailboxOverflowError`。

## 公开 API —— SystemVerilog

编译 `sv/shuttle_pkg.sv`（在 `weave_pkg.sv` 和 `simlog_pkg.sv` 之后），然后
`import shuttle_pkg::*;`。常量与 C++ 枚举一一对应（`SHUTTLE_NOW_DEFER`、
`SHUTTLE_CAP_BLOCKING`、`SHUTTLE_TRIG_DELAY` 等）。

### 声明消息

```systemverilog
class UartTx;
  logic [31:0] addr;
  logic [31:0] data;
  `shuttle_reflect_begin(UartTx)
    `shuttle_field(addr, shuttle_u32)
    `shuttle_field(data, shuttle_u32)
  `shuttle_reflect_end
endclass
```

reflect 宏生成 `type_tag`、`pack`、`unpack` 和 codec 接口，字节布局与 C++ 逐字节一致。
codec 辅助类：`shuttle_u8/u16/u32/u64`、`shuttle_i8/i16/i32/i64`、`shuttle_f32/f64`、
`shuttle_bool`、`shuttle_string`；继承用 `` `shuttle_base(Base) ``。手写 codec 遵循同一
约定（`{>>{...}}` 流式打包，字符串带 32 位长度前缀）。

### 发布

```systemverilog
shuttle_publisher #(UartTx) pub = new("uart0.tx");

shuttle_publication h0 = pub.publish_now(tx);                    // SHUTTLE_NOW_DEFER
shuttle_publication h1 = pub.publish_now(tx, SHUTTLE_NOW_SKIP);
shuttle_publication h2 = pub.publish_serial(tx);                 // trig = null：立即开始
shuttle_publication h3 = pub.publish_serial(tx, shuttle_trigger::after_delay(10_000));
shuttle_publication h4 = pub.publish_concurrent(tx, shuttle_trigger::after_edge(clk_edge));

pub.set_retained(1);
pub.reply(resp, req_meta);        // 应答方简写：回显 req_meta.corr_id
```

`publish_*` 失败返回 `null`（原因看 `shuttle_c_last_error()`；`serial`/`concurrent`
还会打一条 error 日志）。

### 订阅

```systemverilog
class Monitor implements shuttle_sink;                 // blocking 入口
  virtual task invoke(input byte payload[], input shuttle_meta meta);
    UartTx tx = UartTx::unpack(payload);
  endtask
endclass

class FastMon implements shuttle_sink_nb;              // nonblocking 入口
  virtual function void invoke_nb(input byte payload[], input shuttle_meta meta);
  endfunction
endclass

shuttle_subscription sub  = shuttle_subscribe("uart0.tx", mon, UartTx::type_tag);
shuttle_subscription subn = shuttle_subscribe_nb("uart0.*", fast, UartTx::type_tag);
sub.unsubscribe();                // 幂等
```

### 等待与触发器

```systemverilog
h2.wait_done();                   // 阻塞本进程；timeout_fs = 0 表示一直等
h2.wait_done(1_000_000);          // 单位飞秒
h2.done(); h2.subscriber_count(); h2.finished();
h2.skipped(); h2.deferred(); h2.retired();
h2.first_error();                 // "" = 无失败（SV 没有异常）

shuttle_trigger::after_delay(fs);        // 仿真延迟
shuttle_trigger::after_pub(h);           // 另一个 Publication 完成
shuttle_trigger::after_edge(new("clk")); // weave 边沿，按名字
```

### Mailbox

```systemverilog
shuttle_mailbox mb = new("dev.resp", Resp::type_tag, 16,
                         SHUTTLE_OVERFLOW_DROP_OLDEST);
bit ok; byte payload[]; shuttle_meta meta;
mb.recv(ok, payload, meta);              // 阻塞；第三个实参给超时
if (mb.try_recv(payload, meta)) ...;     // 非阻塞
mb.size(); mb.received(); mb.overflowed(); mb.closed(); mb.close();
```

### 请求 / 应答

```systemverilog
class Doubler implements shuttle_responder #(Req, Resp);
  virtual function Resp respond(input Req req, input shuttle_meta meta);
    return ...;
  endfunction
endclass

shuttle_serve #(Req, Resp) svc = shuttle_serve #(Req, Resp)::blocking(
    "dev.req", "dev.resp", doubler);     // 另一个入口用 ::nonblocking(...)

shuttle_requester #(Req, Resp) client = new("dev.req", "dev.resp");
shuttle_call #(Resp) call = client.send_serial(req);   // 也有 send_now / send_concurrent
Resp out[$];
call.wait_all(out, 1ms);                 // 还有 wait_for(n, out, timeout)、recv(...)
call.corr_id(); call.publication(); call.cancel();
client.inflight(); client.stale_responses(); client.last_error();
```

### 对象引用

没有值语义的 SV 类用 `shuttle_ref_*` 跨边界传递：对象表示为 4 字节的内部 id（tag 为
`$typename(T)`）。值消息经 `typedef shuttle_ref_codec #(T) alias;` 加
`` `shuttle_field(f, alias) `` 携带引用；独立门面有 `shuttle_ref_publisher #(T)`、
`shuttle_ref_subscription #(T)`（`::blocking` / `::nonblocking` 工厂，配
`shuttle_ref_sink` / `shuttle_ref_sink_nb`）和 `shuttle_ref_mailbox #(T)`。生命周期动词
只有 `free(obj)` 和 `clear()`；id 单调递增，绝不复用。

## 该选哪个原语

| 你想要 | 用 |
|---|---|
| 立即投递，跑在我栈上，绝不挂起 | `publish_now` + nonblocking 订阅者 |
| 混合订阅者的有序投递 | `publish_serial` |
| 并行投递，慢订阅者不拖累别人 | `publish_concurrent` |
| 条件满足后再投递 | 任一后台方式 + `Trigger` |
| 在我进程里"等到 N 帧" | `Mailbox` |
| 带超时的问答 | `Requester` + `serve` |
| 迟到者也能拿到最新值 | retained topic |
| 不关心类型地旁观一个 topic | `subscribe_bytes` / `shuttle_mailbox` |
