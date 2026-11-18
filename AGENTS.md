# AGENTS.md —— 给在本仓库里工作的助手/贡献者

shuttle 是 EDA 验证环境的发布-订阅框架（C++ 内、SV 内、C++ ↔ SV），并发编排全部
复用 [weave](../weave)，本仓库只做 broker + 类型化载荷 + DPI 桥。

## 命令

```bash
just build          # 按当前后端构建
just sc             # systemc 后端本地跑（预期末行 "WEAVE TEST PASSED"）
just vcs            # svdpi 后端 + 真实 VCS 仿真（预期 "SHUTTLE CPP DEMO PASSED"）
just test           # 示例：两个后端都过一遍
just test-all       # 测试套件：systemc（46 用例）+ VCS（49 用例 + 79 个 SV 断言）
```

`tests/svdpi/run.sh` 在没有 vcs 的机器上会打印 `[svdpi-test] skipped` 并以 0 退出，
所以 `just vcs` 在无 EDA 环境下会自动降级为 SKIP，不要当成失败。

## 必须守住的不变量

1. **单活跃进程**（weave 的 port 契约）：任一时刻只有一个进程执行 C++ 侧记账代码；
   挂起 ≡ 交出调度权。shuttle 的"投递时把帧字节流放在进程级上下文里让 SV 拉取"
   （`src/backend/svdpi/` 的 `FrameContext` + `shuttle_c_frame_len/qword`）就依赖这一点。
2. **作用域修正**：调用包级 export 前必须切 `svScope`（`svScope` 会被 import 调用污染，
   VCS 报 `Error-[DPI-DXFNF]`）。见 `src/backend/svdpi/svdpi_internal.hpp` 的 `SvScopeGuard`；驱动 weave 的
   import 入口切到 `weave_pkg`，其余切到 `shuttle_pkg`。
3. **import function 的调用链里不能调 export task**（`Error-[DPI-INCDIC]`）：`publish_now` 的
   投递必须走 `shuttle_sv_dispatch_nb`（function），不接受任何例外。
4. **一次注册 = 一个订阅者 = 一个入口**：入口由**注册时调用的函数**决定——C++ `subscribe(fn)`
   / SV `shuttle_subscribe` = blocking 入口（SV 侧 export task）；C++ `subscribe(nb(fn))` /
   SV `shuttle_subscribe_nb` = nonblocking 入口（SV 侧 export function）。`serve(...)` 同理（裸 handler = blocking、`nb(handler)` = nonblocking），
   **不提供运行时 `Capability` 参数**——否则入口变成运行时属性，调用处也看不出来。
   **不要**用 `$cast` 探测类实现了哪些 interface class 自动登记两个入口：那会把
   能力变成类级属性，与 C++ 的注册级属性错位（会让 `subscriber_count()` 与 `publish_now` 的行为在两种
   语言里不一致）。两入口都要 = 注册两次。三种方式的目标集合都是"全部匹配订阅者"；`publish_now`
   是唯一无法提供 blocking 入口上下文的调度，所以它是唯一需要策略的地方
   （`NowPolicy{reject,skip,defer}`，默认 `defer`）。别把"入口为 blocking 的订阅者"当成 `publish_now`
   的**错误**，也别让 `serial`/`concurrent` 静默丢帧——"没成功投递"必须体现为计数。
5. **后台投递的 per-topic 串行链**：`src/broker/dispatch.cpp` 的 `start_serial` 必须在发布瞬间捕获
   "本帧要放行的链尾事件"并在完成时 set 它；用当时的 `topics[topic].chain` 会放错帧、
   让后续帧永久阻塞（曾引发整机 hang，表现为仿真时间停在最后一个事件处、无报错）。
6. **SV 打包方向是 `{>>{...}}`**（声明序、每字段高位在前），与 C++ `Writer` 的大端逐字节一致；
   `{<<{...}}` 会整段反转比特流。string 先写 32 位长度前缀。
7. **退订即生效（在途投递记 retired）**：快照只保证"谁被选中"，不保证"一定被调用"。
   两个来源都必须记成 `Delivery::retired`（进 `dropped` 的 `retired` 分量，不进 `delivered`、
   不留 `Failure`）：
   - C++ 侧：`Broker::remove_sub` 置 `Subscriber::removed`，`deliver` 在 kind 分支**之前**检查。
     快照持有 `shared_ptr`、对象还活着，漏了这一查就会真的调进已退订的回调；
   - SV 侧：`shuttle_registry` 用 `retired_` 静默丢弃（不 `$error`，只有从未登记过的 id 才
     `$error`），并在丢弃时调 `shuttle_c_sub_retired`；`Broker::deliver` 在 export 返回后用
     `take_retired_delivery()` 把这次投递转成 retired。
   少任何一边都是"幻影成功"（`delivered`/`received` 涨了但帧没进回调），或者两边口径不一致
   （C++ 记 delivered、SV 记 retired）。别再引入"后端专属终态"。
8. **跨 DSO 的异常**：异常类要有 out-of-line 定义（key function），
   否则 `simv` 直接 `undefined symbol`。
9. **typed 订阅者只能收到已物化的对象**：SV 发布时若 topic 未绑定 C++ 类型，帧只能是纯字节流
   （`raw_ops`），此帧绝不能投给 `SubKind::typed` 订阅者（否则把 `vector<byte>` 强转成 T，
   读到堆垃圾、值每次运行都不同）。两条防线：`sub_typed` 对无通配符的 topic 立即
   `topic_declare`；`make_envelope_bytes` 在 topic 未绑定时按匹配的 typed 订阅者类型绑定；
   `deliver` 里对 `raw_ops` 帧遇 typed 订阅者直接报 `PublishError`。
10. **目标集 = 名字 ∩ 类型，完成计数按目标计，且只有一个入口在推进它**：目标集在快照时
    一次定完（`Broker::match_subs(topic, tag)` + `type_matches`）。类型不相交的订阅者**不是
    目标**——不进 `target_count`、不建投递单元、不计 `dropped`。`Publication::finished()` 对三种方式
    都等于"已到达终态的目标数"，唯一推进点是 `src/broker/dispatch.cpp` 的 `target_finished()`
    （`++finished` 后若 `finished == target_count && !done_flag` 就 `pub_complete`）。任何路径——投递成功、
    退订跳过、回调抛异常、串行单元的单元级失败（未尝试的目标要补齐计数）——都必须走它，
    不能在函数末尾"一次性把 `finished` 置成 `target_count`"（那样 `done()` 会先于真实完成成立）。
    由此得到三条不变量：`done() ≡ finished() == subscriber_count()`（无例外路径）；
    `finished() == 投递 + 退订跳过 + 失败`；`retired() ≤ finished()`（退订跳过同时计入 `dropped()`）。
    零目标的发布必须立即完成（`publish_serial` 也要短路，不能建投递单元）。所有"没收到"都必须
    可数：策略 `skip` 的落 `skipped()`、快照后被退订的落 `retired()`、类型不相交的不进目标集
    （这不是"没收到"，是"本就不是它的帧"）。别让任何一类变成无记录。
11. **通配符只属于订阅模式**：发布 / `topic_declare` / `topic_set_retained` 的名字不许含 `*`/`>`
    （`Broker::check_topic_name`），订阅模式里 `>` 必须独占末段、`*` 必须独占一段、不许空段
    （`Broker::check_pattern`），违反抛 `TopicPatternError`。两道校验都必须在**建任何状态之前**
    ——允许通配符 topic 会在 `topics` 表里立起一个名字带 `*` 的幻影 topic（自己的 seq / 绑定 /
    retained 槽），并按字符串比对真的扇出给 `a.*`、`a.>`、`>` 三类订阅者，看着像广播；而
    `check_bind_compat` 会跳过通配符订阅者，连标签写错都不会暴露。订阅侧不校验的话，`a.>.b`
    因 `match` 遇到 `>` 就放行而静默等于 `a.>`，`a.rx*` 只能匹配字面量。`now_preflight` 里的
    校验要排在策略判定之前（同一个非法名字不该因 `NowPolicy` 不同而换报错）。只读查询
    （`topic_retained`）不校验，非法名当"没绑过"。
12. **`defer` 复用 `publish_serial` 的同一条链**（不要另起并发投递）：由此得到可测定律"同一 topic 的
    后台投递顺序 = **入队顺序**"——immediate 触发帧入队于发布调用内，延迟帧入队于触发时刻。
    **延迟帧不许在发布瞬间占链尾**：`after(Event)` 这类条件的延迟是无界的，占住链尾就会把同 topic
    后续的每一帧（包括根本没有触发器的帧）一起扣住，`publish_serial(...).wait()` 因此永久卡住；
    更糟的是当触发器本身由"排在后面的同 topic 帧的回调"满足时形成真死锁（`trigger_tests.cpp`
    case8/case9 钉住这两条）。所以 `start_serial` 只在 `trig.immediate()` 时做发布时链尾交接，
    否则由 task 在 `wait_trigger` 返回后做一次读-改写入队（无挂起点，单活跃进程保证原子）。
    串行链的跨订阅者队头阻塞仍是显式接受的代价——想要并发请用 `publish_concurrent`。
    已知无解模式：回调里对同一 topic 发布串行帧并 `wait()`（内层帧排在本帧之后，本帧又要等它）——
    任何 FIFO 设计都死锁，属于禁止用法；`wait()` 在 `weave` 暴露"当前任务身份"之前无法可靠检出。

13. **邮箱的投递回调不许阻塞**：`Mailbox` 用 nonblocking 入口注册，其回调只做一次拷贝入队，
    因为 `publish_now` 的同步段（发布者栈内）也会调它。由此有两条硬规则：有界邮箱构造时
    拒绝 `OverflowPolicy::block`（阻塞投递者 = 死锁发布者）；溢出计数 `overflowed()` 与
    `Subscription::dropped()` 是两个概念（后者是"帧没投到该订阅者"，前者是"已投到、被队列
    丢掉"），`reject` 则复用正常的 `Failure` 通道，别发明新记账。SV 侧同样实现（纯 SV，
    `shuttle_subscribe_nb` + `weave_pkg` 事件原语），**不要为它加 DPI 入口**。

14. **回执 topic 是响应通道，回执照走同步段**：`Requester` 与 `serve` 注册时会调
    `detail::topic_mark_responses` 把回执 topic 标成响应通道（`Topic::carries_responses`），
    此后该 topic 上**任何 blocking 入口的注册都被 `Broker::check_response_topic` 拒绝**
    （`CapabilityError`，通配符按匹配判定，`dev.>` 也算命中）。理由：blocking 旁观者会持有该
    topic 的串行链，后续每一帧回执都排在它后面，而请求方**无法区分"响应者死了"与"回执排在后
    面"**——超时会说谎。别把它放宽成警告：这是注册期早失败，和不变 4 同一口径；也别改成投递
    时检查（那时已经晚了）。名字本身仍然公开（`resp_topic()`、跨语言硬编码都合法），手工往回执
    topic 发布也合法，只有"blocking 旁观"被禁。
    回执必须用 `detail::pub_now` 发（`serve` 与 SV `shuttle_pub::reply` 都是），因为：
    (a) `nb(handler)` 响应者 + `send_now()` 因此能给出"返回即可用"的零 RTT（`send_now` 返回时
    `call.queued() == 1`，仿真时间不推进）；(b) 回执不占链尾，旁观者再慢也咬不动请求方。
    `NowPolicy::defer` 只是兜底（标记之前就注册的 blocking 订阅者）。由此 `publish_now` 有递归
    风险：回调在发布者栈上跑，A↔B 互相发布就是无诊断的栈溢出，所以 `dispatch_now` 必须有
    重入深度上限（`kMaxNowDepth`，抛 `PublishError`）——删它等于把环变成崩溃。

## 运行环境的坑（已踩）

- 用 `xmake run` 跑 VCS：它会为 simv 准备库搜索路径。**不要**自己 export
  `LD_LIBRARY_PATH`，也不要往构建目录拷/symlink `libweave/libsimlog/libspdlog`（反而破坏解析）。
- `/home/penn/.synopsys/bin/vcs` 是 docker shim；容器只 identity-mount `~/workspace` 等目录，
  `/tmp` 对仿真器不可见——探针文件放仓库内。
- 容器内 simv 的 stderr 会被吞，排查时用 `$display` 或把日志写到 `build/` 下的文件。
- VCS 2018.09 的 SV 解析器把 `small` 当保留字（Verilog-AMS 关键字），不能作变量名；
  报错位置常指向相邻 token（`automatic shuttle_mailbox small = new(...)` 报 token is 'small'）。
- VS Code IntelliSense（`.vscode/c_cpp_properties.json`）：`compilerPath` 必须是 **`/usr/bin/g++`**
  （写 `gcc` 时只拿到 C 的系统头目录，报"找不到 `<string>`"）；`compileCommands` 要写成
  `${workspaceFolder}/.vscode/compile_commands.json`（相对路径有歧义）。注意容器内 xmake 生成的
  database 会把编译器记成 `/usr/bin/g++-11`（宿主不存在），cpptools 查系统头失败——遇到就把
  `arguments[0]` 改成宿主真实编译器。改完需 `Developer: Reload Window` 或执行
  `C/C++: Reset IntelliSense Database` 才生效。

## 头文件布局

- `include/shuttle/*.hpp`：公开 API——**判定标准是被 `shuttle.hpp` 伞头暴露**。
  `mailbox.hpp` 与 `requester.hpp` 是**纯头文件**组件（无 `.cpp`、无导出符号）：前者是
  `sub_typed`/`sub_raw` 注册的 nonblocking 订阅者 + 一个自己的队列，后者在其上做回执去复用；
  **不要在 broker 里为它们加状态**。
- `include/shuttle/detail/abi.hpp`：已安装，属**第二公开层（稳定 ABI）**——路径与内容随版本
  管理，改名 = 改 ABI；但用户代码不应直接 include（公开入口是 `publisher.hpp` /
  `subscription.hpp`，只它们间接包含这里）。公有模板（`Publisher<T>` / `subscribe<T>`）
  必须在用户 TU 里看到 `pub_*` / `sub_*` 的声明，所以它必须随包安装。
- `src/broker/`（**不安装**）：`model.hpp` 只放 `PubState` / `Subscriber` / `Topic` /
  `Envelope` 等共享数据模型，不依赖后端；`broker.hpp` 声明 `Broker` 与跨入口调度函数，
  `broker.cpp` 实现路由、校验、绑定、单目标投递和订阅生命周期，`dispatch.cpp` 实现三种
  发布调度、完成状态机与等待。头文件存在的唯一理由是让 C++ API 与 svdpi `facade.cpp`
  共享同一份声明；Broker 成员与调度函数定义不再塞进共享头。
- `src/detail/`（**不安装**）：`publication.hpp` 是 `Publication::Impl` 与具名构造通道，
  `trigger.hpp` 是 `detail::TriggerAccess`（桥接层构造 Trigger 的具名友元通道）。
- `src/*.cpp`：按公开对象划分的后端无关 TU——`publication.cpp` 同时实现 `Published` 侧
  入口（`pub_*` / `topic_*`）与 `Publication` 成员，`subscription.cpp` 同时实现
  `sub_typed` / `sub_raw` 与 `Subscription` 成员（`Subscription::Impl` 只在那里定义，
  所以 `sub_*` 不能攞）；`meta.cpp` 只做 `now_fs()` 转发，`introspection.cpp` 实现
  `shuttle::broker` 只读查询。
- `src/backend/backend.hpp`：broker ↔ 后端的 6 入口窄口（不安装）。它属于后端边界，
  所以住在 `src/backend/` 里，include 写作 `"backend/backend.hpp"`（`src/` 仍是包含根）。
- `src/backend/<backend>/`：**按目录选一个后端编译**（`xmake.lua` 的
  `add_files("src/backend/" .. 后端 .. "/**.cpp")`）。不许互相 include、不许加
  `SHUTTLE_BACKEND_*` 之类的宏——换后端 = 换目录。systemc 后端只许经
  `src/backend/backend.hpp` 的 6 入口窄口认识 broker；**svdpi 是显式的例外**：它是唯一
  带 FFI 通道的后端（且永不会有第二个 FFI 调用方），其目录额外可见 broker 内部
  （include `broker/broker.hpp` 与 `detail/publication.hpp`），外部门面就住在那里（见下条）。
- `src/backend/svdpi/facade.cpp` 是外部门面（`ext_*` / 按 `pub_id` 的 `pub_*` /
  跨语言事件注册表 `event_registry_*` / FFI 错误通道 `set_last_error`）：svdpi 专属，
  systemc 构建不编译它；与 dpi.cpp 分层（这里只做 broker 编排，不做 DPI 翻译）。
  `src/publication.cpp`、`src/subscription.cpp` 与 `src/broker/` 因此只剩纯 C++ 路径，
  不认识任何外部通道。
- `xmake.lua` 的 `add_headerfiles` 必须覆盖 `include/(shuttle/**.hpp)`——写成 `shuttle/*.hpp`
  会让安装出来的包缺 `detail/abi.hpp`，使用者一 include `publisher.hpp` 就编译失败。

## 文档

- `docs/architecture.md`：实现结构（分层、数据结构、三种方式的时序、桥接与后端边界、
  已知限制）。改 broker 核心或数据流时同步它。
- `docs/dpi_protocol.md`：DPI 边界的**权威**定义（import/export 表、常量表、线上格式、
  语义要点、以及 VCS 2018.09 实测要点）。改桥接层时同步它。
- `README.md`：面向用户的能力/用法；`examples/README.md`：示例与预期输出。
