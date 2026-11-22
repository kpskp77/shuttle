# 示例

[English](README.md) | 简体中文

每个示例都是一个可运行的 `WEAVE_TEST`，同时自带断言：任何错误值、丢帧或超时都会抛
错并使本次运行失败。下面的预期输出就是 `just example-all` 的对照基准——run"通过"但看着不
对劲时，拿它比对。

## 运行

```bash
just example-sc    # systemc 后端本地跑；末行 "WEAVE TEST PASSED"
just example-vcs   # svdpi 后端 + VCS；以 "SHUTTLE CPP DEMO PASSED" 为判据
```

svdpi 后端下，`shuttle_demo.sv` 与 C++ 演示在同一个仿真器、同一个 broker 上运行：
SV 监视器订阅的 `uart0.tx` 同样会收到 C++ 发布，C++ 订阅者也会收到 SV 发布。systemc
后端只跑 C++ 演示，入口是 `tests/sc_main.cpp`。

## 每个示例练什么

| 文件 | 语言 | 覆盖的 API 面 |
|---|---|---|
| `basic_demo.cpp` | C++ | publisher + typed 订阅（blocking 与 `nb`）、三种发布方式、retained topic、通配符模式、`subscribe_bytes`、introspection、多 publisher、失败/超时边界 |
| `shuttle_demo.sv` | SV | `shuttle_pub`/`shuttle_sub`/`shuttle_subscribe_nb`、一个类挂两个入口、`publish_now`/`publish_serial`/`publish_concurrent`、邮箱（纯 SV）、多 publisher、退订；并经 `weave_run` 启动 C++ 演示 |
| `mailbox_demo.cpp` | C++ | 邮箱即订阅者、`recv(timeout)` 排空、有界队列（`drop_oldest`/`drop_newest`/`reject`）、构造时拒绝 `OverflowPolicy::block`、拷贝共享同一队列、`close()`、无关联号的请求/应答 |
| `request_response_demo.cpp` | C++ | `Requester`/`serve` 与 `corr_id` 去复用、并发在途调用、旁观者与响应者之分（`wait_for(n)`）、响应方超时、抛异常的响应者落为 `Failure` |
| `trigger_demo.cpp` | C++ | `Trigger`：`after(Event)` / `after(duration)` / `after(Task)` / `after_all`，句柄完成与触发解耦 |

## 预期输出（svdpi 后端）

SV 与 C++ 回调的交错是调度出来的、不固定——每块内的 `[monitor]` 行相对顺序可能不同，
下面的确切计数也取决于跑了哪些演示。必须成立的是：

### `basic_demo` + `shuttle_demo.sv`（uart 主题）

```
[monitor] seq=2 addr=0x00000011 data=0x0000005a   ← SV 订阅者收到 SV 的 serial 发布
[monitor] seq=4 addr=0x00000011 data=0x0000005a   ← SV 订阅者收到 C++ 发布（同一 broker）
[async]   seq=4 addr=0x00000011 data=0x0000005a   ← C++ nb 订阅者收到 C++ 发布
[raw] topic=uart0.tx payload=8 bytes
[broker] topic=uart0.tx type=UartTx subs=6 published=8 delivered=20
```

`seq` 是按 topic 编号的 `topic_seq`，跨语言单调：跳号即丢帧。`subs=6` 数的是快照时刻
`uart0.tx` 上活着的每次注册，包括同一个 SV 类的两个入口（两次注册，与 C++ 一致）；
`delivered` 由这些注册与它们各自在场的帧数决定。

### `mailbox_demo`

```
[mailbox] publish_now filled it synchronously: size=3 received=3
[mailbox] drained 3 frame(s) (sum=6), then recv(1ms) returned empty
[mailbox] recv() got data=7 seq=4
[capped] drop_oldest: capacity=2 received=5 overflowed=3 survivors=4,5
[capped] drop_newest: capacity=2 received=2 overflowed=3 survivors=1,2
[reject] subscriber cpp sub N -> shuttle: mailbox overflowed (capacity 1)
[block] rejected at construction: ...
[alias] a copy reads the same queue (data=9, 0 left in the original)
[close] drains to 11, then recv() throws: ...
[rpc] request data=21 -> reply ch=3 data=42
```

`received` 数的是被队列接下的帧（所以 `drop_oldest` 会显示 `received=5` 而只剩 2 帧
存活）；`overflowed` 数的是挤出/拒收。`reject` 复用常规 `Failure` 通道——不另开记账——
句柄里的 `N` 是注册顺序的订阅者 id。

### `request_response_demo`

```
[driver] addr=0x04 corr_id=1 -> 2 resp(s), 2 target(s)
[driver]   status=0 value=0x01004
[driver]   status=1 value=0x00000
[driver] demux ok: A=0x01006 B=0x01002
[driver] observer: 2 resp(s), 3 target(s), observed=1
[driver] no responder: 0 resp(s), subscribers=0
[driver] responder cpp sub ... failed: device is not responding
```

一次请求扇出到两个寄存器块；因为 `corr_id` 全程挂在回执 topic 上，各块回自己的裁
决。旁观者场景说明为什么有非应答订阅者时不能用 `wait_all`：该用
`wait_for(<响应者数>)`。

### `trigger_demo`

```
[alarm] code=1
[alarm] code=3
[alarm] code=2
[alarm] code=4
```

顺序不固定——`code=2` 要等事件 set、`code=3` 要等 worker 任务结束——但必须恰好跑四
个回调，且四个 `result()` 都返回。

## 工作约定

每个示例文件开头都有一块注释说明场景；代码随后断言注释里写的值，注释因此不会悄悄
漂移。新增示例遵循同样的形状（注释 → 场景 → 断言），并在上面的表格里加一行；新增
SV 示例还需在本文件与 `shuttle_demo.sv` 的编译顺序说明里同步登记。
