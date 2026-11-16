# AGENTS.md —— 给在本仓库里工作的助手/贡献者

shuttle 是 EDA 验证环境的发布-订阅框架（C++ 内、SV 内、C++ ↔ SV），并发编排全部
复用 [weave](../weave)，本仓库只做 broker + 类型化载荷 + DPI 桥。

## 命令

```bash
just build          # 按当前后端构建
just sc             # systemc 后端本地跑（预期末行 "WEAVE TEST PASSED"）
just vcs            # svdpi 后端 + 真实 VCS 仿真（预期 "SHUTTLE CPP DEMO PASSED"）
just test           # 示例：两个后端都过一遍
just test-all       # 测试套件：systemc（44 用例）+ VCS（47 用例 + 76 个 SV 断言）
```

`tests/svdpi/run.sh` 在没有 vcs 的机器上会打印 `[svdpi-test] skipped` 并以 0 退出，
所以 `just vcs` 在无 EDA 环境下会自动降级为 SKIP，不要当成失败。

## 必须守住的不变量

1. **单活跃进程**（weave 的 port 契约）：任一时刻只有一个进程执行 C++ 侧记账代码；
   挂起 ≡ 交出调度权。shuttle 的"投递时把帧字节流放在 thread_local 上下文里让 SV 拉取"
   （`src/backend/svdpi/` 的 `FrameContext` + `shuttle_c_frame_len/qword`）就依赖这一点。
2. **作用域修正**：调用包级 export 前必须切 `svScope`（`svScope` 会被 import 调用污染，
   VCS 报 `Error-[DPI-DXFNF]`）。见 `src/backend/svdpi/svdpi_internal.hpp` 的 `SvScopeGuard`；驱动 weave 的
   import 入口切到 `weave_pkg`，其余切到 `shuttle_pkg`。
3. **import function 的调用链里不能调 export task**（`Error-[DPI-INCDIC]`）：`publish_now` 的
   投递必须走 `shuttle_sv_dispatch_nb`（function），不接受任何例外。
4. **一次注册 = 一个订阅者 = 一个入口**：入口由**注册时调用的函数**决定——C++ `subscribe(fn)`
   / SV `shuttle_subscribe` = blocking 入口（SV 侧 export task）；C++ `subscribe(nb(fn))` /
   SV `shuttle_subscribe_nb` = nonblocking 入口（SV 侧 export function）。
   **不要**用 `$cast` 探测类实现了哪些 interface class 自动登记两个入口：那会把
   能力变成类级属性，与 C++ 的注册级属性错位（会让 `subscriber_count()` 与 `publish_now` 的行为在两种
   语言里不一致）。两入口都要 = 注册两次。三种方式的目标集合都是"全部匹配订阅者"；`publish_now`
   是唯一无法提供 blocking 入口上下文的调度，所以它是唯一需要策略的地方
   （`NowPolicy{reject,drop,defer}`，默认 `defer`）。别把"入口为 blocking 的订阅者"当成 `publish_now`
   的**错误**，也别让 `serial`/`concurrent` 静默丢帧——"没收到"必须体现为计数。
5. **后台投递的 per-topic 串行链**：`src/broker.cpp` 的 `start_serial` 必须在发布瞬间捕获
   "本帧要放行的链尾事件"并在完成时 set 它；用当时的 `topics[topic].chain` 会放错帧、
   让后续帧永久阻塞（曾引发整机 hang，表现为仿真时间停在最后一个事件处、无报错）。
6. **SV 打包方向是 `{>>{...}}`**（声明序、每字段高位在前），与 C++ `Writer` 的大端逐字节一致；
   `{<<{...}}` 会整段反转比特流。string 先写 32 位长度前缀。
7. **退订即生效（在途投递记 retired）**：快照只保证"谁被选中"，不保证"一定被调用"。
   两个来源都必须记成 `Delivery::retired`（进 `dropped`，不进 `delivered`、不留 `Failure`）：
   - C++ 侧：`Broker::remove_sub` 置 `Subscriber::removed`，`deliver` 在 kind 分支**之前**检查。
     快照持有 `shared_ptr`、对象还活着，漏了这一查就会真的调进已退订的回调；
   - SV 侧：`shuttle_registry` 用 `retired_` 静默丢弃（不 `$error`，只有从未登记过的 id 才
     `$error`），并在丢弃时调 `shuttle_c_sub_retired`；`Broker::deliver` 在 export 返回后用
     `take_retired_delivery()` 把这次投递转成 retired。
   少任何一边都是"幻影成功"（`delivered`/`received` 涨了但帧没进回调），或者两边口径不一致
   （C++ 记 delivered、SV 记 retired）。别再引入"后端专属终态"。
8. **跨 DSO 的异常**：异常类与 `shuttle::Event` 都要有 out-of-line 定义（key function），
   否则 `simv` 直接 `undefined symbol`。
9. **typed 订阅者只能收到已物化的对象**：SV 发布时若 topic 未绑定 C++ 类型，帧只能是纯字节流
   （`raw_ops`），此帧绝不能投给 `SubKind::typed` 订阅者（否则把 `vector<byte>` 强转成 T，
   读到堆垃圾、值每次运行都不同）。两条防线：`sub_typed` 对无通配符的 topic 立即
   `topic_declare`；`make_envelope_bytes` 在 topic 未绑定时按匹配的 typed 订阅者类型绑定；
   `deliver` 里对 `raw_ops` 帧遇 typed 订阅者直接报 `PublishError`。
10. **目标集 = 名字 ∩ 类型，完成计数按目标计，且只有一个入口在推进它**：目标集在快照时
    一次定完（`Broker::match_subs(topic, tag)` + `type_matches`）。类型不相交的订阅者**不是
    目标**——不进 `expected`、不建投递单元、不计 `dropped`。`Publication::finished()` 对三种方式
    都等于"已到达终态的目标数"，唯一推进点是 `src/broker.cpp` 的 `target_finished()`
    （`++finished` 后若 `finished == expected && !done_flag` 就 `pub_complete`）。任何路径——投递成功、
    退订跳过、回调抛异常、串行单元的单元级失败（未尝试的目标要补齐计数）——都必须走它，
    不能在函数末尾"一次性把 `finished` 置成 `expected`"（那样 `done()` 会先于真实完成成立）。
    由此得到三条不变量：`done() ≡ finished() == subscriber_count()`（无例外路径）；
    `finished() == 投递 + 退订跳过 + 失败`；`retired() ≤ finished()`（退订跳过同时计入 `dropped()`）。
    零目标的发布必须立即完成（`publish_serial` 也要短路，不能建投递单元）。被排除出目标集的
    目标（策略 `drop` 的 `excluded`、类型不相交的）都必须可数，别让"没收到"变成无记录。
11. **`defer` 复用 `publish_serial` 的同一条链**（不要另起并发投递）：由此得到可测定律"同一 topic 的
    后台投递顺序 = 发布调用顺序"。串行链的跨订阅者队头阻塞是显式接受的代价——想要并发请用
    `publish_concurrent`。

## 运行环境的坑（已踩）

- 用 `xmake run` 跑 VCS：它会为 simv 准备库搜索路径。**不要**自己 export
  `LD_LIBRARY_PATH`，也不要往构建目录拷/symlink `libweave/libsimlog/libspdlog`（反而破坏解析）。
- `/home/penn/.synopsys/bin/vcs` 是 docker shim；容器只 identity-mount `~/workspace` 等目录，
  `/tmp` 对仿真器不可见——探针文件放仓库内。
- 容器内 simv 的 stderr 会被吞，排查时用 `$display` 或把日志写到 `build/` 下的文件。
- VS Code IntelliSense（`.vscode/c_cpp_properties.json`）：`compilerPath` 必须是 **`/usr/bin/g++`**
  （写 `gcc` 时只拿到 C 的系统头目录，报"找不到 `<string>`"）；`compileCommands` 要写成
  `${workspaceFolder}/.vscode/compile_commands.json`（相对路径有歧义）。注意容器内 xmake 生成的
  database 会把编译器记成 `/usr/bin/g++-11`（宿主不存在），cpptools 查系统头失败——遇到就把
  `arguments[0]` 改成宿主真实编译器。改完需 `Developer: Reload Window` 或执行
  `C/C++: Reset IntelliSense Database` 才生效。

## 头文件布局

- `include/shuttle/*.hpp`：公开 API——**判定标准是被 `shuttle.hpp` 伞头暴露**。
- `include/shuttle/detail/abi.hpp`：已安装但非公开。公有模板（`Publisher<T>` / `subscribe<T>`）
  必须在用户 TU 里看到 `pub_*` / `sub_*` 的声明，所以它必须随包安装；改名 = 改 ABI。
  只被 `publisher.hpp` / `subscription.hpp` 间接包含，用户代码不应直接 include。
- `src/internal/*.hpp`：**不安装**。只给 `src/` 用（`set_last_error`、`detail::backend` 接口、
  `broker_impl.hpp` 里的 `Broker` / `PubState` 定义——后者的存在理由是 `broker.cpp` 与
  `entry.cpp` 必须共享同一份 broker 定义与单例）。
- `src/backend/<backend>/`：**按目录选一个后端编译**（`xmake.lua` 的
  `add_files("src/backend/" .. 后端 .. "/**.cpp")`）。两个目录互不相见、不许互相 include，
  也不许再加 `SHUTTLE_BACKEND_*` 之类的宏——换后端 = 换目录。新增后端只需实现
  `src/internal/backend.hpp` 的 6 个入口。
- `src/entry.cpp` 里是外部接入门面（`ext_*` / 按 `pub_id` 的 `pub_*`）：与后端无关，
  后端目录整个删掉也能编译。
- `xmake.lua` 的 `add_headerfiles` 必须覆盖 `include/(shuttle/**.hpp)`——写成 `shuttle/*.hpp`
  会让安装出来的包缺 `detail/abi.hpp`，使用者一 include `publisher.hpp` 就编译失败。

## 文档

- `docs/architecture.md`：实现结构（分层、数据结构、三种方式的时序、桥接与后端边界、
  已知限制）。改 broker 核心或数据流时同步它。
- `docs/dpi_protocol.md`：DPI 边界的**权威**定义（import/export 表、常量表、线上格式、
  语义要点、以及 VCS 2018.09 实测要点）。改桥接层时同步它。
- `README.md`：面向用户的能力/用法；`examples/README.md`：示例与预期输出。
