# examples —— 用户侧 API 示范

只使用 `include/shuttle` 的公开头文件，以"链接 libshuttle"的真实用户方式构建。

| 文件 | 内容 |
|---|---|
| `cpp/basic_demo.cpp` | 消息定义（`SHUTTLE_REFLECT`）、两个入口的订阅（普通 / `nb()`）、三种发布方式（`publish_now` / `publish_serial` / `publish_concurrent`）、`NowPolicy` 的 `skip` / `reject`、`Publication` 可见性计数、retained、通配符与原始字节订阅、broker 内省 |
| `cpp/trigger_demo.cpp` | 延迟触发：`after(Event/duration/Task)`、`after_all` 组合 |
| `cpp/mailbox_demo.cpp` | 邮箱（订阅 + 队列）：`publish_now` 同步入队、在自己进程线里 `recv`/排空、有界队列的 `drop_oldest` / `drop_newest` / `reject`（`received()` 与 `overflowed()` 两个口径）、`OverflowPolicy::block` 在构造期被拒、副本共享一个队列、`close()` 后队列仍可读、无 rid 的最小 request/response |
| `cpp/request_response_demo.cpp` | 请求-响应：响应者组件（`serve`）与请求者组件（`Requester`）互不共享对象；多响应者、同 topic 上两个在途请求的 rid 分发、只看不回的旁观订阅者与 `wait_for`、无人响应时的超时、响应者抛异常计入失败 |
| `cpp/sc_main.cpp` | systemc 后端的本地运行入口 |
| `sv/shuttle_demo.sv` | SV 侧示范：消息类约定（`pack()`/`type_tag`）、`shuttle_sink`/`shuttle_sink_nb` 订阅者、`shuttle_pub#(T)` 三种发布方式与 `SHUTTLE_NOW_*` 策略、`shuttle_handle::wait_done` 与计数字、`shuttle_trigger`、`shuttle_mailbox`（纯 SV，同 C++ 邮箱契约） |

## 构建与运行

```bash
# systemc 后端：本地可执行（预期输出末行 "WEAVE TEST PASSED"）
xmake f --backend=systemc
xmake build shuttle_examples_sc && xmake run shuttle_examples_sc

# svdpi 后端：共享库，随仿真器加载
xmake f --backend=svdpi
xmake build shuttle_examples_svdpi

# 一条命令跑完真实仿真器（VCS）：编译 weave_pkg → shuttle_pkg → shuttle_demo，
# 以 -sv_root/-sv_lib 加载 libshuttle_examples_svdpi.so（间接带入
# libshuttle/libweave），按输出 "SHUTTLE CPP DEMO PASSED" 判定。
# 注意用 xmake run（它会为 simv 准备好库搜索路径）；不要自己 export
# LD_LIBRARY_PATH，也不要往构建目录里拷/symlink 依赖库。
xmake run shuttle_examples_svdpi_vcs
```

仿真编译顺序固定为：`../weave/sv/weave_pkg.sv` → `sv/shuttle_pkg.sv` →
`examples/sv/shuttle_demo.sv`。`shuttle_demo.sv` 里的 `weave_run` 会一并启动
C++ 侧注册的示例（`WEAVE_TEST`），两侧在同一 broker 上互通。

预期输出节选（左右两侧互相收帧，`seq` 在 topic 内单调递增）：

```
[monitor] seq=2 addr=0x00000011 data=0x0000005a   ← SV 订阅者收到 SV 发布
[monitor] seq=4 addr=0x00000011 data=0x0000005a   ← SV 订阅者收到 C++ 发布
[async]   seq=4 addr=0x00000011 data=0x0000005a   ← C++ 订阅者收到 C++ 发布
[raw] topic=uart0.tx payload=8 字节
[broker] topic=uart0.tx type=UartTx subs=5 published=8 delivered=15
SHUTTLE CPP DEMO PASSED
```

请求-响应示例的输出（两个 `RegisterBlock` 组件 + 一个 `TestDriver` 组件，三者不共享任何对象）：

```
[driver] addr=0x04 rid=1 -> 2 reply(ies), 2 target(s)     ← 一次请求，两个响应者各回一条
[driver]   status=0 value=0x01004 (rid=1)                  ← 命中窗口的块
[driver]   status=1 value=0x00000 (rid=1)                  ← 窗口外的块；两处 rid 相同
[driver] demux ok: A=0x01006 B=0x01002                     ← 两个在途请求的回执没有串味
[driver] observer: 2 reply(ies), 3 target(s), observed=1   ← 旁观订阅者也进目标数，故用 wait_for
[driver] no responder: 0 reply(ies), subscribers=0         ← 无人响应：超时返回空，不挂起
[driver] responder cpp sub 11 failed: device is not responding  ← 响应者抛异常 → 请求帧的 failures()
```

邮箱示例的输出（C++ 侧 `cpp/mailbox_demo.cpp`，与 SV 侧 `[mbox]` 行同义）：

```
[mailbox] publish_now filled it synchronously: size=3 received=3   ← 非阻塞入口：发布者栈内入队
[mailbox] drained 3 frame(s) (sum=6), then recv(1ms) returned empty ← 排空在自己进程线里做
[mailbox] recv() got data=7 seq=4                                  ← 阻塞式 recv()，帧已入队则不挂起
[capped] drop_oldest: capacity=2 received=5 overflowed=3 survivors=4,5
[capped] drop_newest: capacity=2 received=2 overflowed=3 survivors=1,2
                                   ↑ 两个策略下 received() 口径不同：它是"被队列接受"的帧数
[reject] subscriber cpp sub 13 -> shuttle: mailbox overflowed (capacity 1)  ← 复用正常 Failure 通道
[block] rejected at construction: shuttle: a bounded mailbox needs OverflowPolicy::drop_oldest, ...  ← 有界邮箱不许阻塞投递者
[alias] a copy reads the same queue (data=9, 0 left in the original)
[close] drains to 11, then recv() throws: shuttle: receiving from a closed mailbox (queue drained)
[rpc] request data=21 -> reply ch=3 data=42                          ← 无 rid 的单响应者最小用法
```

SV 侧（`sv/shuttle_demo.sv`，同一个邮箱契约，但没有异常通道，用输出标志报"关闭且已排空"）：

```
[mbox] addr=0x00000020 data=0x00000001 topic=mbox.demo received=1
[mbox] capacity=2 received=2 overflowed=2
```

C++ 示例注册为 `WEAVE_TEST`，与 weave 示例同一运行机制：svdpi 下由
`weave_pkg::weave_run` 经 `weave_start` 启动，systemc 下由 `weave::sc::run()` 启动。

## SV 侧打包约定（易错点）

`pack()`/`unpack()` 必须用 `{>>{...}}`（声明序、每字段高位在前），它与 C++ 侧
`Writer` 的大端写法的字节序逐字节一致；`{<<{...}}` 会整段反转比特流。string 字段先写
32 位长度前缀：`{>>{32'(s.len()), s}}`。
