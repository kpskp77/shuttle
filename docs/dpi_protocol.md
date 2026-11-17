# shuttle DPI 协议

本文是 `libshuttle.so`（C++）与 `shuttle_pkg.sv`（SV）之间 DPI 边界的**权威定义**：
函数表、常量、线上格式与仿真器约束。实现结构见
[`architecture.md`](architecture.md)；面向用户的能力与用法见 [`../README.md`](../README.md)。

方向约定与 weave 一致：

- **import（SV → C++）**：`shuttle_c_*`，C++ 实现，SV 调用；
- **export（C++ → SV）**：`shuttle_sv_*`，SV 实现，C++ 调用。

所有 import 均为 `context` import：其实现内可能回调 export（分发、取时间），
非 context import 在该场景下行为未定义。

## 1. import 表（C++ 提供）

| 函数 | 签名 | 阻塞性 | 说明 |
|---|---|---|---|
| `shuttle_c_publish_now` | `(string topic, string type_tag, byte payload[], int now_policy) → int` | 不阻塞（但同步执行回调） | `publish_now` 发布：在**调用该函数的 SV 进程**内同步回调入口为 nonblocking 的订阅者；入口为 blocking 的订阅者按 `now_policy`（`SHUTTLE_NOW_*`，默认 `defer`）处置。返回 `pub_id`（>0），失败返回 0（原因见 `shuttle_c_last_error`）。SV 侧据此构造 `shuttle_handle`，因此 `publish_now` 与 `serial` / `concurrent` 一样可 `wait_done()`、可查询 `skipped()` / `deferred()` / `retired()`。`topic` 必须是不含通配符的具体名，否则返回 0 并置 `shuttle_c_last_error` |
| `shuttle_c_publish_serial` | `(string topic, string type_tag, byte payload[], int trig_kind, longint trig_arg) → int` | 不阻塞，立即返回 | `publish_serial` 发布：后台单链顺序回调（topic 级 FIFO）。返回 `pub_id`（>0 成功，≤0 失败）。`trig_kind` / `trig_arg` 见常量表。调用瞬间锁定目标集合与消息拷贝（快照语义）。`topic` 含通配符或空段时返回 0 并置 last_error |
| `shuttle_c_publish_concurrent` | `(string topic, string type_tag, byte payload[], int trig_kind, longint trig_arg) → int` | 不阻塞，立即返回 | `publish_concurrent` 发布：后台每订阅者一个任务并发回调，无顺序承诺。其余同上（含 topic 名校验） |
| `shuttle_c_wait_pub` | `(int pub_id, longint timeout_fs, output int status)` | **阻塞**（task） | 阻塞当前 SV 进程直至该发布的全部订阅者回调结束。`timeout_fs` = 0 表示无限等待。`status` 见常量表。实现复用 weave 的挂起机制（`weave_sv_wait`），可挂起任意调用它的 SV 进程 |
| `shuttle_c_pub_done` | `(int pub_id) → int` | 不阻塞 | 轮询：非 0 表示全部订阅者回调已结束（含失败） |
| `shuttle_c_pub_error` | `(int pub_id) → string` | 不阻塞 | 第一个失败订阅者的错误信息；空串表示无失败 |
| `shuttle_c_pub_stat` | `(int pub_id, int kind) → int` | 不阻塞 | 发布句柄的可见性计数。`kind`：0 = 目标订阅者数（名字 ∩ 类型），1 = 已结束（delivered / retired / failed 三种终态都算），2 = 被策略跳过（只有 `skip` 会产生；`reject` 不返回句柄），3 = 转到后台链（defer，标记，完成后不回零），4 = 到达已退订的 SV 订阅者而跳过（已计入 1 与 `dropped` 的 `retired` 分量）。未知 `pub_id` 返回 -1 |
| `shuttle_c_subscribe` | `(string topic, int caps, string type_tag, int sv_sid) → int` | 不阻塞 | 注册 SV 订阅者。`caps` 必须恰好是 `1`（`SHUTTLE_CAP_BLOCKING`）或 `2`（`SHUTTLE_CAP_NONBLOCKING`），其余值返回非 0 并置 last_error；`type_tag` 非空时校验与 topic 类型绑定是否一致，不一致返回非 0；`sv_sid` 是 SV 注册表编号，C++ 原样记录并在分发时带回，两侧无需维护 id 映射。topic 支持通配符（`*` 匹配单级，`>` 匹配其后至少一级、不匹配前缀本身）。模式语法：每段要么是普通段，要么是独占一段的 `*`，要么是独占一段且必须为末段的 `>`；空段、空串、`a.>.b`、`a.rx*` 一律返回非 0 并置 last_error |
| `shuttle_c_unsubscribe` | `(int sv_sid)` | 不阻塞 | 退订（幂等）。退订即生效：已快照但尚未投递的帧仍会到达 SV 侧（注册表里已无 sink），由注册表丢弃并经 `shuttle_c_sub_retired` 回告，C++ 记 `retired` |
| `shuttle_c_set_retained` | `(string topic, bit retained)` | 不阻塞 | 开启或关闭 retained（latched）：开启后 broker 缓存最新一帧，新订阅者注册时补发。`topic` 不得含通配符，否则不生效并置 last_error |
| `shuttle_c_frame_len` | `() → int` | 不阻塞 | 当前帧的字节流长度。**只能在 export 体内**（`shuttle_sv_dispatch[_nb]`）调用，数据来自 C++ 侧的 `FrameContext` |
| `shuttle_c_frame_qword` | `(int index) → longint` | 不阻塞 | 取第 `index` 个 8 字节块（SV 侧按小端拼装，末块不足 8 字节的部分补零）。帧因此不经过 export 形参，绕开"C++ 无法构造 open array"的限制 |
| `shuttle_c_sub_retired` | `(int sv_sid)` | 不阻塞 | **由 SV 侧调回 C++**：`shuttle_registry::dispatch[_nb]` 发现目标已退订（`retired_`）时静默丢弃这次在途投递，同时调用本函数回告。C++ 在 export 返回后把该次投递记为 retired（计入 `dropped` 的 `retired` 分量，不计 `delivered`，不留 `Failure`） |
| `shuttle_c_event_create` | `() → int` | 不阻塞 | 创建一个由 C++ 注册表持有的事件 id（>0；0 表示失败），可作 `SHUTTLE_TRIG_EVENT` 的 `trig_arg`，也是 SV `shuttle_event` 的底层对象 |
| `shuttle_c_event_set` | `(int ev_id)` | 不阻塞 | 置位并唤醒所有等待者（C++ 侧 `detail::event_wait_id` 与 `TRIG_EVENT` 的投递单元） |
| `shuttle_c_event_destroy` | `(int ev_id)` | 不阻塞 | 释放 id（SV `shuttle_event::close()`）。仿真结束时不调用亦可随进程消亡 |
| `shuttle_c_last_error` | `() → string` | 不阻塞 | 最近一次 import 调用失败的详细原因（每进程独立） |

## 2. export 表（SV 提供）

| 函数 | 签名 | 阻塞性 | 说明 |
|---|---|---|---|
| `shuttle_sv_dispatch` | `task (int sub_id, string topic_name, string type_tag, longint topic_seq, longint time_fs)` | **可阻塞** | `serial` / `concurrent` 的投递入口：由 C++ broker 的 weave task 调用，在承载该 task 的 SV 进程中执行，用户回调（`shuttle_sink.invoke`）可消耗仿真时间 |
| `shuttle_sv_dispatch_nb` | `function void (同参)` | **不可阻塞** | `publish_now` 同步段的投递入口：在**发布者进程上下文**中执行，function 语义由编译器强制零延时；回调为 `shuttle_sink_nb.invoke_nb` |
| `shuttle_sv_time_fs` | `function longint ()` | 不阻塞 | 仿真时间源，C++ `shuttle::now_fs()` 的 svdpi 实现；返回当前仿真时间的飞秒数 |

两个 dispatch export 都**没有 payload 形参**：负载由 export 体内调用
`shuttle_c_frame_len` / `shuttle_c_frame_qword` 从 C++ 侧的 `FrameContext` 拉取（§1）。
`sub_id` 即 `shuttle_c_subscribe` 传入的 `sv_sid`；`topic_seq` 是 topic 内单调递增序号
（broker 分配，跨语言一致）；`time_fs` 是发布时刻的仿真时间。

## 3. 常量表

SV 侧坐标量一律用 `localparam int` 而非 `typedef enum`。原因有两条：这些值要直接作为
`int` 形参传给 import，形参取任意 `int` 表达式（plusarg、配置变量、计算结果）时零转换；
VCS 对"枚举变量接收 `int`"会报 `Warning-[ENUMASSIGN] Illegal assignment to enum variable`。
此外 DPI ABI 本身把形参降级为 `int`，且 IEEE 1800-2017 §35.5.5 的 DPI 可支持类型表不含
枚举，使用 `int` 是唯一可移植的写法。

`SHUTTLE_*` 前缀承担两件事：包作用域内不重名，以及与本表 C++ 侧的 `enum class` 建立一一
对应关系。

三种发布方式**不在**这张表里：它们各自对应一个 import 函数，调度方式写在函数名中，
没有 `mode` 常量。

| 名称 | C++ | SV | 值 | 含义 |
|---|---|---|---|---|
| 入口：blocking | `Capability::blocking` | `SHUTTLE_CAP_BLOCKING` | 1 | 一次注册只含此位：`shuttle_subscribe` / C++ `subscribe(t, fn)`。使用 `invoke`（export task），可消耗仿真时间 |
| 入口：nonblocking | `Capability::nonblocking` | `SHUTTLE_CAP_NONBLOCKING` | 2 | 一次注册只含此位：`shuttle_subscribe_nb` / C++ `subscribe(t, nb(fn))`。使用 `invoke_nb`（export function），零延时 |
| 策略：报错 | `NowPolicy::reject` | `SHUTTLE_NOW_REJECT` | 0 | `publish_now` 遇到入口为 blocking 的订阅者时抛 `CapabilityError` / 返回错误，帧不发布 |
| 策略：跳过 | `NowPolicy::skip` | `SHUTTLE_NOW_SKIP` | 1 | 跳过这些订阅者，计入 `skipped()` 与 `dropped()` 的 `skipped` 分量 |
| 策略：转后台（默认） | `NowPolicy::defer` | `SHUTTLE_NOW_DEFER` | 2 | 整帧交给该 topic 的后台串行链，返回可等待句柄 |
| 触发：立即 | 空 `Trigger` | `SHUTTLE_TRIG_NONE` | 0 | 立即启动回调进程 |
| 触发：延时 | `after(duration)` | `SHUTTLE_TRIG_DELAY` | 1 | `trig_arg` = 延时（飞秒） |
| 触发：事件 | `detail::event_wait_id(id)`（无公开 C++ 工厂） | `SHUTTLE_TRIG_EVENT` | 2 | `trig_arg` = 事件句柄 id（只由 SV 创建） |
| 触发：发布完成 | —（SV `after_pub(h)`；C++ 经桥装配） | `SHUTTLE_TRIG_PUB` | 3 | `trig_arg` = 另一 Publication 的 `pub_id` |
| 等待：完成 | — | `SHUTTLE_WAIT_OK` | 0 | 全部回调结束（不代表无订阅者失败） |
| 等待：超时 | — | `SHUTTLE_WAIT_TIMEOUT` | 1 | |
| 等待：有失败 | — | `SHUTTLE_WAIT_FAILED` | 2 | 订阅者失败，详情见 `shuttle_c_pub_error`；内部错误见 `shuttle_c_last_error` |

## 4. 线上格式

字段按声明顺序编码，无字段名、无填充、无对齐。整数与浮点为**大端**；长度前缀为
uint32 大端。SV 侧打包与解包都使用 `{>>{...}}`（1 位切片，由高位向低位流式），
得到与本格式一致的位流；`{<<{...}}` 会把整段比特流反向，不可用于本协议。

| C++ 类型 | SV 类型 | 编码 |
|---|---|---|
| `bool` | `bit` | u8（0/1） |
| `uint8_t` / `int8_t` | `bit [7:0]` / `byte` | u8 |
| `uint16_t` / `int16_t` | `shortint unsigned` / `shortint` | u16 大端 |
| `uint32_t` / `int32_t` | `int unsigned` / `int` | u32 大端 |
| `uint64_t` / `int64_t` | `longint unsigned` / `longint` | u64 大端 |
| `float` | `shortreal` | IEEE754 32 位大端 |
| `double` | `real` | IEEE754 64 位大端 |
| `std::string` | `string` | u32 长度前缀 + UTF-8 字节 |
| `enum`（底层类型 U） | `enum`（同位宽 base type） | 按 U 编码 |
| `std::vector<T>` | 动态数组 / 队列 | u32 个数前缀 + 元素依次 |
| `std::array<T, N>` | 定宽数组 `[N]` | N 个元素依次（无前缀） |
| 嵌套 struct | 同字段序 class | 递归展开 |

注意事项：

- **有符号性只影响解释，不影响位流**，但两侧语义必须一致（C++ `int32_t` ↔ SV `int`）。
- SV `string` 打包：先写 `s.len()`（32 位）再写 `s` 本身，即 `{>>{32'(s.len()), s}}`；
  解包先读长度，再按长度读出字节并构造 string。
- 可用的成员类型即上表；超出子集的类型不能用于跨语言 topic，`SHUTTLE_REFLECT` 会在静态
  断言与运行期两个层面拦截。
- 类型身份：`shuttle_c_publish_*` 的 `type_tag` 必须与 topic 在 broker 中的类型绑定一致
  （C++ `SHUTTLE_REFLECT` 的类型名 ↔ SV `localparam` `type_tag`），不一致拒绝发布。
- 一致性基准：`tests/cpp/codec_tests.cpp` 的黄金字节是两侧编码的唯一锚点，修改本表必须
  同步该文件与 SV 侧的 `{>>{...}}` 打包。

## 5. 语义要点

1. **多对多**：同一 topic 允许多个 Publisher 实例同时发布、任意多个订阅者，每个订阅者独立
   收到每个发布帧。`Meta.topic_seq` 由 broker 在 topic 内单调递增（跨 Publisher 实例连续），
   订阅者可据此区分多个发布者的帧。发布者身份不做强制区分，需要区分时由消息字段自行携带。
2. **DPI open array**：`byte payload[]` 在 C 侧映射为 `svOpenArrayHandle`，配合 `svSize` /
   `svGetArrElemPtr1` 访问；空数组（len = 0）合法。
3. **dispatch 的执行上下文**：
   - `serial` / `concurrent`：C++ broker 在 weave task（本身是 SV 进程）内调用
     `shuttle_sv_dispatch`，回调消耗的时间属于该后台进程，与发布者无关；
   - `publish_now`：`shuttle_sv_dispatch_nb` 在发布者进程内执行，任何延时或阻塞都会被
     function 语义在编译期拒绝。
   三种方式的目标集合都是全部匹配订阅者；走哪个 export 由该注册声明的入口决定
   （`caps` 只有 1 或 2，不是位掩码组合）：blocking 入口 → `shuttle_sv_dispatch`，
   nonblocking 入口 → `shuttle_sv_dispatch_nb`。同一对象需要两条入口时注册两次。
   `publish_now` 的同步段例外：它必须同步返回，因此只同步投递入口为 nonblocking 的注册，
   其余按 `SHUTTLE_NOW_*` 处置（默认 `defer` 转后台串行链）。
4. **SV 发起的阻塞等待**：`shuttle_c_wait_pub` 内部经 `weave_sv_wait` 挂起调用它的 SV 进程
   （不要求该进程是 weave task），因此 native SV 进程（`initial` 块、UVM phase）可直接调用。
   `shuttle_mailbox::recv` 用的是同一个挂起点，但它完全在 SV 内调用 `weave_sv_wait`，不经 DPI。
5. **id 约定**：`sv_sid` 由 SV 注册表分配，C++ 只透传；`pub_id` 由 C++ broker 分配，
   SV 只透传。两侧互不解析对方的 id 空间。
6. **错误报告**：import 返回值只携带成功或失败，详细原因经 `shuttle_c_last_error()` 取回
   （按调用进程独立保存）。
7. **C++ → SV 方向的失败**：`shuttle_sv_dispatch` 内用户回调出错时，SV 侧通过 `$error`
   上报并返回，C++ 无法捕获 SV 异常；broker 把 dispatch 正常返回视为成功，SV 侧错误计入
   仿真器的错误报告系统。
8. **顺序保证**：`publish_now` 严格 FIFO；`publish_serial` 经 per-topic 串行链保证 topic 级
   FIFO（按**入队**顺序：`SHUTTLE_TRIG_NONE` 的帧在发布调用内入队，因此顺序即发布顺序；
   带触发器的帧在触发时刻才入队，未满足的触发不会占住链头）；`publish_concurrent` 不承诺任何顺序。
   `SHUTTLE_NOW_DEFER` 的帧进入的是与 `publish_serial` **同一条**链。
9. **SV 侧邮箱不新增 DPI**：`shuttle_pkg::shuttle_mailbox` 通过 `shuttle_subscribe_nb` 注册，
   队列是 SV 队列，唤醒走 `weave_pkg` 的 `weave_sv_wait` / `weave_sv_trigger`（纯 SV，非 DPI）。
   因此 `shuttle_pkg` 需要 `import weave_pkg::*`，编译顺序必须 `weave_pkg.sv` 在前。邮箱的容量
   溢出策略只有 `SHUTTLE_OVERFLOW_DROP_OLDEST` / `_DROP_NEWEST`（`block` / `reject` 在 SV 无
   对应物：投递回调不能阻塞，SV 也没有异常可抛）。
10. **请求-响应的跨语言约定**：C++ 侧的 `Requester`/`serve`（`include/shuttle/requester.hpp`）
   把关联 id 放在载荷字段里（默认 `.rid`），SV 侧按同样字段布局**回显**即可参与，无需任何新
   DPI 入口。参考实现：`tests/sv/shuttle_tests.sv` 的 `SvRqSink`（`shuttle_subscribe` 注册
   blocking 响应者，回调里 `shuttle_pub#(T)::publish_serial` 发回执）。request/reply topic 必须
   不同：响应者是在请求回调里发布回执的，同 topic 会形成串行链自环。
11. **退订与在途投递**：`shuttle_c_unsubscribe` 立即把订阅者从 broker 移除，而发布瞬间已
   快照的投递仍可能到达 SV 侧（C++ 侧靠 `Subscriber::removed` 拦住同一类投递，两侧口径
   一致：记 `retired`）。SV 注册表把这些已退订 id 记为 retired，静默丢弃并经
   `shuttle_c_sub_retired` 回告；未登记过的 id 才报 `$error`，以保留真实缺陷的可见性。

## 6. 仿真器约束（VCS O-2018.09-SP2-2）

以下约束由实测得出，直接影响正确性，修改桥接层时必须保留。

1. **作用域（svScope）**：同一 SV 进程内调用过返回 `string` 的 import
   （`shuttle_c_last_error`）之后，再调用包级 export 会报
   `Error-[DPI-DXFNF] DPI export function not found`。因此 C++ 在每次调用 export 前显式
   切换作用域：`src/backend/svdpi/svdpi_internal.hpp` 的 `SvScopeGuard` 进入 `shuttle_pkg`
   （可用 `-DSHUTTLE_SV_PACKAGE=` 覆盖），驱动 weave 原语的 import 入口切到 `weave_pkg`
   （可用 `-DSHUTTLE_WEAVE_SV_PACKAGE=` 覆盖）。
2. **export/import 的时序约束**：DPI `function` import 的调用链内不允许调用 export
   **task**（`Error-[DPI-INCDIC]`）。因此 `publish_now` 的投递通道必须是 function
   （`shuttle_sv_dispatch_nb`），即使该订阅者注册的是 blocking 入口。
3. **SV 语法限制**：export 声明不能带返回类型；函数返回类型不支持 unpacked 数组
   （改用 `output` 形参）；包级函数不能裸 `return new(x)`（先建 automatic 变量）；
   包需声明 `timeunit` / `timeprecision`（否则与 `weave_pkg` 冲突）；
   `after_event(int)` 与 `after_event(class)` 不能构成重载。
4. **流式打包方向**：大端线序必须用 `{>>{...}}`（VCS 实测 `{>>{32'h12, 32'h3c}}` →
   `00 00 00 12 00 00 00 3c`；`{<<{...}}` 得到 `3c 00 00 00 48 00 00 00`，即整段比特流
   反向）。混合位宽同样安全：`{>>{8'hAB, 16'hCDEF, 32'h12345678, 4'h5, 4'h6}}` →
   `ab cd ef 12 34 56 78 56`。
5. **等待不得跨上下文**：`weave::Event::wait()` 只在 weave task 中合法（否则抛
   `weave: blocking primitive outside task context`）。SV 侧的 `shuttle_c_wait_pub` 因此
   直接调用 weave 导出的 `weave_sv_wait`（事件 id 由 C++ 申请，在发布完成钩子里触发）。

## 7. 修改桥接层的自检清单

- 新增 import 是否声明为 `context`；是否需要在调用前切换 `svScope`（是否触及 weave）；
- 新增 export 是否需要同步加入 `shuttle_pkg.sv` 的 `export "DPI-C"` 段与 C++ 侧声明，
  两侧签名逐字一致（`string` / `longint` / `int` 的映射）；
- 常量两侧是否同值：修改 `Capability` / `NowPolicy` / 触发种类 / 等待状态任一处，
  必须同步 `sv/shuttle_pkg.sv` 的 `localparam` 与本文表格；
- 线上格式改动是否同步 `tests/cpp/codec_tests.cpp` 的黄金字节与 SV 侧打包代码；
- 新增的失败路径是否仍在记账契约内（每个目标都必须落到五种终态之一，
  见 `architecture.md` 的"记账契约"一章）。
