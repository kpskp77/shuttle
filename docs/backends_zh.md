# 后端与 DPI 边界

本文是 layer-3 按面拆分的一份：覆盖 shuttle 的两个仿真后端（svdpi、systemc）与 C++ ↔
SystemVerilog DPI 边界，回答"边界为什么长这样"与"到底什么穿过了边界"，服务于做集成、
移植或评审后端代码的人。它是 DPI 边界的权威定义：下面的 import/export 表、协议常量
与线上格式即定义本身。改动必须满足的约束在 [AGENTS.md](../AGENTS.md)
（见 "Guarantees" 与 "Pitfalls in this environment (already hit)"），定位与首次使用在
[README.md](../README.md)，broker 内部——投递调度、终态 记账、入口选择——在
[architecture.md](architecture.md)。

## 后端选择 = 换目录，不是宏

`xmake.lua` 用目录枚举选择后端，除此之外什么都没有：

- `backend` 选项（默认 `svdpi`，另一个可选值只有 `systemc`）恰好选中一个目录：
  `add_files("src/*.cpp")` + `add_files("src/broker/*.cpp")` +
  `add_files("src/backend/<backend>/**.cpp")`。核心与后端分开枚举，保证任何递归规则都
  不会把两个后端编译进同一个库。
- 没有 `SHUTTLE_BACKEND_*` 宏，后端 TU 外面没有 `#if`：两个后端目录互不 include，
  也看不到对方的符号。换后端 = 换目录，所以后端契约的每个条目都必须有定义在编译进
  来的那个目录里（见 "Adding a backend"）。
- `weave` 依赖按 `configs = { shared = true, backend = get_config("backend") }`
  引入，shuttle 与 weave 永远按同一后端构建。下限是 weave 0.1.0——当前编号下的第一个
  发布版本（从早先的 1.x 线重排而来，代码没有改动），也是带 `weave::fatal` 的那个
  版本，自我等待诊断就经它上报；svdpi 额外依赖 `svdpi` 包，systemc 额外依赖
  `systemc` 包（均为 shared）。
- 每次构建编译什么：`src/*.cpp`（publication、subscription、introspection、
  exception、trigger——后端无关翻译单元）加 `src/broker/*.cpp`（路由、dispatch、
  model）加一个后端目录。svdpi 下该目录是 `facade.cpp` + `bridge.cpp` + `dpi.cpp`；
  systemc 下只有一个 `bridge.cpp`，`facade.cpp`、`dpi.cpp` 与
  `sv/shuttle_pkg.sv` 完全不参与构建。

用真实仿真器跑 svdpi 后端走 `tests/run-eda.sh`——唯一的 VCS 运行器。两个 phony xmake 目标
`shuttle_examples_svdpi_vcs` 与 `shuttle_tests_svdpi_vcs` 各自经 `SHUTTLE_SV_TB` 指定测试
平台、经 `SHUTTLE_PASS_MARKER` 指定通过标记（`SHUTTLE_SV_PKG` 可换包，供兼容性探针
用）。脚本经 `VCS_HOME`/`PATH` 定位 vcs，编译 `weave_pkg.sv` + `shuttle_pkg.sv` + 测试平
台，通过 `-sv_root`/`-sv_lib` 带载荷库运行 simv，以输出标记加 `Error-[`/`^Error`
行的缺席判定通过；`PATH` 上没有 vcs 时打印 `[svdpi-test] skipped` 并以 0 退出。simv
一律经 `xmake run` 启动，因为 xmake 准备好的运行时搜索路径让 simv 能找到
`libshuttle.so` → `libweave.so` 以及它们的 simlog/spdlog 依赖。

## svdpi 后端

svdpi 后端是唯一带外部订阅者通道的构建，由四块关注点严格分离的代码组成：

| 文件 | 职责 |
| --- | --- |
| `src/backend/svdpi/dpi.cpp` | DPI-C 面：`shuttle_c_*` import（仿真器调用）与 SV export 原型。只做 DPI 类型翻译与错误上报；无 broker 状态。 |
| `src/backend/svdpi/facade.cpp` | 外部门面：`ext_*`（字节帧发布、外部订阅者注册）与 `pub_*`（按 `pub_id` 的查询/等待）。只做 broker 编排；不认识 DPI。 |
| `src/backend/svdpi/bridge.cpp` | `detail::backend` 实现：`dispatch` / `dispatch_nb` / `note_retired_delivery` / `take_retired_delivery` 加帧拉取通道。 |
| `src/backend/svdpi/svdpi_internal.hpp` | 目录私有头：`SvScopeGuard`、`FrameContext`、`g_frame`、线上编码常量、`ext_*`/`pub_*` 声明。 |
| `sv/shuttle_pkg.sv` | SV 侧包：DPI import/export 声明、常量、订阅注册表与用户 API（`shuttle_publisher`、`shuttle_requester`、`shuttle_call`、`shuttle_responder`、`shuttle_serve`、`shuttle_subscription`、`shuttle_publication`、`shuttle_trigger`、`shuttle_mailbox`、`shuttle_ref_publisher` / `shuttle_ref_mailbox` / `shuttle_ref_subscription` / `shuttle_ref_codec`）。 |

声明与定义按"谁调用谁"切开：SV 调进 `dpi.cpp`，后者以 `shuttle::detail::*` 的名义调用
`facade.cpp`；broker 经 `src/backend/backend.hpp` 的 5 入口契约调用 `bridge.cpp`；
`bridge.cpp` 通过 `sv/shuttle_pkg.sv` 声明的 `shuttle_sv_*` export 回调 SV。

### 载荷的两种过界方式：SV→C++ 拷贝，C++→SV 反向拉取

两个方向刻意用不对称的机制：

| 方向 | 入口 | 载荷怎么过界 |
| --- | --- | --- |
| SV → C++ | `shuttle_c_publish_now` / `_serial` / `_concurrent`（import function） | `byte payload[]` open array；C++ 用 `svGetArrElemPtr1` 逐元素拷进 `std::vector<std::byte>`（dpi.cpp 的 `read_payload`）。 |
| C++ → SV | `shuttle_sv_dispatch`（export task）/ `shuttle_sv_dispatch_nb`（export function） | 不走实参。C++ 调 export 前把帧字节流停放在进程级全局 `g_frame`（经 RAII 的 `FrameContext`）；SV 在 export 函数体内用 `shuttle_c_frame_len()` + `shuttle_c_frame_qword(i)` 反向拉取——每个 qword 带 8 个小端字节——`shuttle_frame_bytes` 再拼回动态数组。 |

反向拉取的存在理由是 VCS 2018.09 的 `svdpi.h` 没有 `svCreateArray`：C++ 构造不了
open array 按值传递；把载荷做实参还会把它的生命期绑在调用方栈帧上。裸全局之所以安全，
只有一个理由：[../AGENTS.md](../AGENTS.md) 的单活跃进程契约（"Guarantees" 第一条）
——export 函数体执行期间没有别的仿真进程能执行 C++ 侧代码，所以拉取途中没有任何东
西能覆盖 `g_frame`。`FrameContext` 是保存/恢复栈（持有旧指针、析构时恢复），这正是嵌
套派发——一个订阅者同步发布给另一个 SV 订阅者——安全的机制。

`shuttle_frame_bytes` 经 `output byte b[]` 参数返回字节而不是作为函数返回值，是因为
VCS 不接受把非合并数组当函数返回类型。`frame_len` / `frame_qword` 是 svdpi 专属入
口：它们声明在 `svdpi_internal.hpp` 而非 `backend/backend.hpp`，只有 export 函数体
内的代码够得到它们。

### DPI 面

Import——由 libshuttle.so 提供（dpi.cpp），SV 侧调用（`shuttle_pkg.sv`）。全部是
`context` import。

| 符号 | 类别 | 签名 | 用途 |
| --- | --- | --- | --- |
| `shuttle_c_publish_now` | function | `(string topic, string type_tag, byte payload[], int now_policy, int unsigned corr_id = 0) → int` | 同步发布；返回本帧的 `pub_id`（>0）或失败时的 0（见 `shuttle_c_last_error`）。返回时 nonblocking 部分已完成。 |
| `shuttle_c_publish_serial` | function | `(string topic, string type_tag, byte payload[], int trig_kind, longint trig_arg, string trig_name, int unsigned corr_id = 0) → int` | 后台发布，走该 topic 的串行链；返回 `pub_id` 或 0。 |
| `shuttle_c_publish_concurrent` | function | 同 `_serial` | 后台发布，每订阅者一个任务；返回 `pub_id` 或 0。 |
| `shuttle_c_wait_pub` | task | `(int pub_id, longint timeout_fs, output int status)` | 挂起调用它的 SV 进程直到该发布完成、到期或等待失败；`status` 是 `SHUTTLE_WAIT_*` 码。`timeout_fs <= 0` 表示无限等。 |
| `shuttle_c_pub_done` | function | `(int pub_id) → int` | 发布到达终态时非零。 |
| `shuttle_c_pub_stat` | function | `(int pub_id, int kind) → int` | 发布计数查询；`kind` 0–4 = target / finished / skipped-by-policy / deferred / retired；未知 `pub_id` 返回 −1。 |
| `shuttle_c_pub_error` | function | `(int pub_id) → string` | 首个失败订阅者的错误信息（空 = 无失败）。 |
| `shuttle_c_sub_retired` | function | `(int sv_sid)` | SV 注册表在派发时命中已退订目标后回呼；让 C++ 把这次投递记为 retired 而非 delivered。 |
| `shuttle_c_subscribe` | function | `(string topic, int caps, string type_tag, int sv_sid) → int` | 注册外部订阅者；`caps` 为 1（`SHUTTLE_CAP_BLOCKING`）或 2（`SHUTTLE_CAP_NONBLOCKING`）；`sv_sid` 是 SV 注册表 id，不透明存储、派发时原样带回。成功返回 0。 |
| `shuttle_c_unsubscribe` | function | `(int sv_sid)` | 按句柄删除外部订阅者。 |
| `shuttle_c_set_retained` | function | `(string topic, bit retained)` | 开关某 topic 的 retained 锁存器。 |
| `shuttle_c_take_corr_id` | function | `() → int` | 从 `shuttle::Requester` 用的那个计数器里取下一个请求/响应关联 id，绝不返回 0。留在 C++ 侧，SV 请求者与 C++ 请求者才不会在同一响应 topic 上给两个调用发出同一个 id。 |
| `shuttle_c_last_error` | function | `() → string` | 本进程最近的错误信息（每个以返回码报错的入口的错误通道）。 |
| `shuttle_c_frame_len` | function | `() → int` | 帧拉取协议：停放在 `g_frame` 的帧的字节长度。 |
| `shuttle_c_frame_qword` | function | `(int index) → longint` | 帧拉取协议：第 index 个 8 字节块，qword 内小端。 |

Export——由 `shuttle_pkg.sv` 提供，C++ broker 回调（原型在 bridge.cpp）：

| 符号 | 类别 | 签名 | 用途 |
| --- | --- | --- | --- |
| `shuttle_sv_dispatch` | task | `(int sub_id, string topic_name, string type_tag, longint topic_seq, longint time_fs, int unsigned corr_id)` | 投递的 blocking 入口：跑在后台仿真进程里，可消耗仿真时间。经帧拉取协议取载荷，再经 `shuttle_registry` 分发到订阅者的 `shuttle_sink::invoke`。 |
| `shuttle_sv_dispatch_nb` | function | 同参数 | nonblocking 入口：跑在发布者进程上下文，必须零延迟，落到 `shuttle_sink_nb::invoke_nb`。 |

meta 字段作为独立 DPI 实参过界、不打包成字节，所以 C++ 日志与 SV `$display` 可以直接
按 `(topic_name, topic_seq)` 对齐。

### 常量与线上格式

数值本身就是协议的一部分：SV 把它们拼成 `localparam`，C++ 用 `wire(...)` 对同样的
数字比较（作用在 C++ 枚举上），两侧因此不会漂移。

| 族 | 常量 |
| --- | --- |
| 订阅入口（`shuttle_c_subscribe` 的 `caps`） | `SHUTTLE_CAP_BLOCKING = 1`、`SHUTTLE_CAP_NONBLOCKING = 2`——一次注册恰好是其一；其余值拒绝。 |
| `publish_now` 策略（`now_policy`） | `SHUTTLE_NOW_REJECT = 0`、`SHUTTLE_NOW_SKIP = 1`、`SHUTTLE_NOW_DEFER = 2`（默认：其余值回落到 defer）。 |
| 触发种类（`trig_kind`） | `SHUTTLE_TRIG_NONE = 0`、`SHUTTLE_TRIG_DELAY = 1`（`trig_arg` = 飞秒）、`SHUTTLE_TRIG_PUB = 2`（`trig_arg` = 另一个 `pub_id`）、`SHUTTLE_TRIG_EDGE = 3`（`trig_name` = `weave_edge` 名；其余种类为空串）。 |
| 等待状态（`shuttle_c_wait_pub` 的 `status`） | `SHUTTLE_WAIT_OK = 0`、`SHUTTLE_WAIT_TIMEOUT = 1`、`SHUTTLE_WAIT_FAILED = 2`（订阅者失败或内部错误）。 |
| 邮箱溢出 | `SHUTTLE_OVERFLOW_DROP_OLDEST = 0`、`SHUTTLE_OVERFLOW_DROP_NEWEST = 1`（SV 侧没有 `block`/`reject`）。 |
| 发布生命期（`pub_state`） | 0 = running、1 = done、2 = done with failures；−1 = 未知 `pub_id`（`kUnknownPub`）。 |
| 发布计数（`shuttle_c_pub_stat`） | 0 = target、1 = finished、2 = skipped-by-policy、3 = deferred、4 = retired。 |

帧 meta（`shuttle::Meta` / `shuttle_meta`）两侧逻辑内容同为五字段：`topic_name`、
`type_tag`、`topic_seq`、`corr_id`、`time_fs`。只有 `corr_id` 由发布方写入（0 = 非关
联交换）；其余由 broker 填，`time_fs` 来自 `weave::now()` 而非后端。

载荷线上格式（由 `include/shuttle/codec.hpp` 定义，`shuttle_pkg.sv` 头部注释中的 SV
消息类约定与之镜像）：

- 字段按声明序编码——无字段名、无填充、无对齐；嵌套 struct 递归展开。
- 整数与浮点为大端（浮点取 IEEE-754 位模式）；枚举按其底层整数类型编码，所以 SV 基类
  型必须同宽。
- `std::string` 与 `std::vector<T>` 带 32 位大端长度前缀、其后按序排元素；
  `std::array` 无前缀。
- SV 打包必须用流式操作符 `{>>{...}}`：声明序、每字段高位字节在前——恰是 C++
  `Writer` 的布局。`{<<{...}}` 会把整个比特流反转，永不兼容。字符串字段按
  `{>>{32'(s.len()), s}}` 流式传输，以匹配长度前缀。

解码对每个整数做边界检查（`Reader::take` 抛 `DecodeError`），带长度前缀的解码器在分配
前先把声明长度与剩余字节数核对，坏长度触发不了巨额预分配。

### 作用域修正只对我们自己的包级 export 需要

VCS 2018.09 上实测：SV 进程调用过返回 `string` 的 import（如 `shuttle_c_last_error`）
之后，同一进程再调用包级 export 会报 `Error-[DPI-DXFNF]`——import 调用改了当前
`svScope`，导致 export 不可见。`SvScopeGuard`（`svdpi_internal.hpp`）因此在每次调用
我们自己的 export 前切到按包名缓存的作用域（`scope_for`，单条目，单活跃进程契约下无
需加锁），退出时恢复调用方作用域。

推论：

- `dispatch` 与 `dispatch_nb`（bridge.cpp）在 export 调用外面套 `SvScopeGuard`——这些
  是可被污染的入口。
- 发布 import 可能间接驱动 weave（建任务、挂起），却不需要自己做作用域切换：weave 的
  DPI 后端在每个自己的 import 外面套了作用域守卫，所以它的 export 在调用方
  碰巧处于的任何作用域里都能解析。shuttle 因此从不直接调用 weave 的 export，也从不
  为 `weave_pkg` 切作用域；早年的 `WeaveScopeGuard` / `SHUTTLE_WEAVE_SV_PACKAGE` 方案
  被删，正因为从 dpi.cpp import weave 的 export 会绕过 weave 自己的守卫，并把 shuttle
  重新耦合到 weave 的包布局上。
- 从不调 export 的条目不需要守卫：`shuttle_c_set_retained`、`shuttle_c_take_corr_id`
  与只读查询
  （`shuttle_c_pub_done` / `_pub_stat` / `_pub_error`，以及帧拉取对——它只运行在已经
  处于守卫之下的 export 函数体内）。
- 包名本身可用 `-DSHUTTLE_SV_PACKAGE=` 覆盖；只有 shuttle 自己的包需要它。

### publish_now 的同步段只能触碰 nonblocking 入口

DPI 禁止在 import **function** 调用链里运行 export **task**
（`Error-[DPI-INCDIC]`）。发布 import 是 function，所以 `publish_now` 同步段只能调用
本身是 function 的入口——nonblocking 的那些（`shuttle_sv_dispatch_nb`）。这是结构性
强制、不是投递时的参数：`dispatch_now` 在同步循环之前按每个注册的能力把目标集分组，
blocking 入口的目标根本进不了同步段。`defer` 策略下这些目标稍后从后台 weave 任务里
到达 `shuttle_sv_dispatch`，那时 import 调用链已不存在；`skip` 下它们被计数但不投递；
`reject` 下该帧在建任何状态之前就被拒。同一个约束也是 `shuttle_sv_dispatch_nb` 必须
是 function 的原因：编译器在 SV 侧强制零延迟。

### SV 注册表把静态 export 名变成整数句柄派发

DPI export 名是静态的——不可能"每个订阅者一个 export 函数"——所以 SV 侧持有一个注
册表，C++ 侧只持有整数：

- `shuttle_registry` 把一个整数 id（`sv_sid`，从 1 向上分配）映射到回调对象，存在两
  个关联数组之一：`sinks_`（实现 `shuttle_sink` 的对象，blocking 入口）与
  `sinks_nb_`（实现 `shuttle_sink_nb` 的对象，nonblocking 入口）。一个对象可以同时实
  现两个接口，但只有注册两次两个入口才都活——一次注册 = 一个入口，与 C++ 同规则。
  `shuttle_serve_sink` 就是这样一个对象：`shuttle_serve` 只注册其中一个入口，由调用哪个
  工厂（`::blocking` 还是 `::nonblocking`）决定。
- `sv_sid` 的所有权在 SV 侧：注册表分配它、作为 `shuttle_c_subscribe` 的不透明
  `handle` 传给 C++，派发时 C++ 原样带回。两侧都不维护映射。`pub_id` 所有权是镜像：
  C++ 在发布时分配，SV 存进 `shuttle_publication`。
- 派发是两跳：export（`shuttle_sv_dispatch` / `_nb`）拉取载荷并组装 `shuttle_meta`，
  然后 `shuttle_registry::dispatch[_nb]` 按 id 路由到 `invoke` / `invoke_nb`。
- 在途退订：`shuttle_registry::remove` 删掉 sink 并置 `retired_[id]`。之后到达的派发
  在 SV 侧静默丢弃——不是错误——但会调 `shuttle_c_sub_retired`，让 C++ 把这次投递记
  为 retired（真实终态）而非幻影 delivered。从未登记过的 id 仍然 `$error`，保留真缺
  陷的可见性。
- SV 侧的 `$error` 永不传回 C++：SV 代码成功调用的 import 就成功返回。需要严格错误
  通道的接口必须显式返回（这就是为什么 `shuttle_c_subscribe` 与发布 import 用返回码
  应答，也是为什么 `shuttle_publisher::publish_now` 失败时保持静默而 `publish_serial` /
  `publish_concurrent` 立即打印——即发即弃的异步接口没有更晚的时机可以报错）。

### wait_done 通过一个叶子 weave 等待挂起普通 SV 进程

`shuttle_publication::wait_done()` 要把一个普通 `initial`/`always` 进程停住直到发布完成，
机制完全是 weave 的公开 C++ API：

1. `shuttle_c_wait_pub` 先问 `pub_state(pub_id)`；已终态的发布不挂起直接回答。
2. `pub_check_self_blocked(pub_id)` 在任何挂起之前运行：交付链被等待进程自己持有的帧
   永远等不到完成，所以这里经 `weave::fatal` 结束仿真——诊断与等待图进仿真器日志，
   wait 不再返回（改成异常只会被 catch 掉，把挂死变成静默挂死）。
3. 否则注册一个 `pub_on_complete` 钩子来 set 一个私有 `weave::Event`，并让调用进程
   在 `Event::wait()` 上挂起——weave 允许不带任务身份的叶子等待，而这正是一个普通
   SV 进程的身份。
4. 正的 `timeout_fs` 另起一个 `shuttle-pub-deadline` 任务睡到期限再 set 同一事件——
   超时是一次取消，所以它需要自己的任务——哪条腿赢，另一条就被取消。
5. 最终 `pub_state` 映射成 `SHUTTLE_WAIT_*` 状态：仍在跑 = 期限赢了（`TIMEOUT`）；
  `done_with_failures` = `FAILED`（订阅者失败文本来自 `shuttle_c_pub_error`，内部错误
   来自 `shuttle_c_last_error()`）。

完成钩子存在是因为 `pub_complete` 先跑 `on_complete` 钩子再 set done 事件，所以被唤
醒的等待方永远看不到"已完成但 SV 进程尚未收到信号"的中间态。门面里另一条
`pub_wait`（内部供 `SHUTTLE_TRIG_PUB` 触发器使用，不作为 DPI import 暴露）改为在
weave 任务上下文内经 `done_ev` / `wait_pub_timed` 回答。

### VCS 2018.09 约束

- `small` 与 `edge` 是保留字（Verilog-AMS 关键字），不能作标识符；`shuttle_trigger` 因
  此把字段命名为 `edge_name`，报错位置常指向相邻 token。
- 这个年份的 `svdpi.h` 没有 `svCreateArray`——正是它逼出了反向拉取载荷协议；VCS 还拒
  绝把非合并数组当函数返回类型（故有 `output byte b[]`），也不允许 package 里的
  local function（故 `shuttle_register` 是带内部标记名字的包函数）。
- 包内 export 在 VCS 上可用（已验证）；若目标仿真器拒绝它们，两条 export 移进一个
  module——协议不变。
- docker shim 的 vcs 容器会吞掉 simv 的 stderr：调试用 `$display` 或写进仓库 `build/`
  下的日志，不要盯着 stderr 看。
- 容器 identity-mount `~/workspace`，`/tmp` 对仿真器不可见；探针与运行目录（如
  `svdpi_work_*`）放在仓库内，`tests/run-eda.sh` 已安排好。
- docker shim 经 xmake 准备好的环境解析共享库；手工拼 `LD_LIBRARY_PATH` 跑 simv，或把
  `libweave`/`libsimlog`/`libspdlog` 拷/软链到 simv 旁边，只会弄坏解析。`xmake run`
  是受支持的入口。

## systemc 后端 = 没有外部订阅者通道的 broker

`src/backend/systemc/bridge.cpp` 把同样的 5 个 `detail::backend` 入口实现为一组契约
桩：

- `available()` 返回 `false`：该构建没有外部订阅者通道。
- `dispatch` / `dispatch_nb` 抛 `std::logic_error`——正确配置下不可达，但每个入口都
  必须有定义（换后端 = 换目录不换接口），而 throw 快速失败，缺定义只会到链接期才失
  败。
- `note_retired_delivery()` 是空操作、`take_retired_delivery()` 恒 `false`：没有 SV
  注册表来报告在途退订。
- 仿真时间从不走后端，shuttle 也不持有自己的时钟：`Meta::time_fs` 与所有超时算术直
  接读 `weave::now()`（systemc 下 weave 自己的端口读 `sc_time_stamp()`），所以任何后
  端都不会对"现在几点"产生分歧。

broker 不知道自己跑在哪个后端上：纯 C++ 用法在两个后端下完全一致。剩下的不对称只
在构建中可见——svdpi 目录（和 `shuttle_pkg.sv`）在 systemc 下不参与编译，外部订阅者
代码路径在那里完全不存在。

## 跨边界的句柄身份 = 两次单向分配

穿越 DPI 边界永远不交对象引用——只交整数，每个由一侧分配、另一侧原样带回：

| 句柄 | 分配方 | 持有方 | 经谁带回 |
| --- | --- | --- | --- |
| `sv_sid`（订阅者） | `shuttle_registry::add`（SV） | broker `Subscriber::handle`（不透明） | `shuttle_sv_dispatch[_nb]` 第一个实参 |
| `pub_id`（发布） | 发布时的 broker（`PubState::id`） | `shuttle_publication::pub_id_`（SV） | 每次 `shuttle_c_pub_*` / `shuttle_c_wait_pub` 调用 |

边界 C++ 侧唯一的真正进程级状态——`g_frame`、retired 投递标志、last-error 字符串——
作为普通全局是安全的，理由与拉取协议相同：单活跃进程契约保证任一时刻至多一个仿真
进程在 C++ 记账代码内，所以不需要锁也不需要 `thread_local`。建立在其上的链所有权与
自等待诊断属于 broker 层，记在 architecture.md，不在此处。

## 加后端 = 实现五个条目

一个新后端就是一个新目录 `src/backend/<name>/`，其 `bridge.cpp` 提供
`src/backend/backend.hpp` 声明的五个条目：

| 条目 | 契约 |
| --- | --- |
| `available()` | 该构建是否有外部（SV）订阅者通道。 |
| `dispatch(handle, topic_name, type_tag, topic_seq, time_fs, corr_id, payload)` | `serial`/`concurrent` 的 blocking 投递，在 weave 任务上下文调用；`handle` 是订阅者注册时拿到的后端侧数字，不经解释透传；meta 字段原样转发，保证 `Meta` 两种语言一致。 |
| `dispatch_nb(...)` | `publish_now` 的投递，在发布者进程上下文调用；完全不许阻塞。 |
| `note_retired_delivery()` / `take_retired_delivery()` | 在途退订的反馈通道：后端在派发命中已退订目标时置标志；broker 在调用结束后收取，把该投递记为 retired。 |

加后端时 broker 和头文件都不动——除非新后端根本无法提供投递，那说明契约本身的形
状错了。时间刻意不在契约里：仿真时间归 weave 所有，broker 直接读 `weave::now()`。

同样的目录切分适用于每一层：DPI-C 面、外部门面、`detail::backend` 实现与 SV 包源码
都留在后端自己的目录（或 `sv/`）里，永不进入 `backend.hpp` 或公开头文件。

### svdpi 是唯一带 FFI 的后端，而且永远不会有第二个

只有 svdpi 有要维护的外部函数接口：它是唯一有外部调用方的后端，所以 `dpi.cpp`（DPI
面）与 `facade.cpp`（外部门面与 FFI 错误通道）住在 svdpi 目录里，systemc 构建不编译
它们。代价是一种显式接受、永久的不对称：svdpi 目录看得见 broker 内部
（`broker/broker.hpp`），因为 `facade.cpp` 必须编排 preflight、封套构造与目标集快
照；而 systemc 后端只通过上面五入口窄口认识 broker。未来带外部订阅者的新后端会沿
用 svdpi 的安排——FFI 面住自己的目录——而 broker 看见的仍然只有那五入口契约。
