# examples —— 用户侧 API 示范

只使用 `include/shuttle` 的公开头文件，以"链接 libshuttle"的真实用户方式构建。

| 文件 | 内容 |
|---|---|
| `cpp/basic_demo.cpp` | 消息定义（`SHUTTLE_REFLECT`）、两个入口的订阅（普通 / `nb()`）、三种发布方式（`publish_now` / `publish_serial` / `publish_concurrent`）、`NowPolicy` 的 `drop` / `reject`、`Publication` 可见性计数、retained、通配符与原始字节订阅、broker 内省 |
| `cpp/trigger_demo.cpp` | 延迟触发：`after(Event/duration/Task)`、`after_all` 组合 |
| `cpp/sc_main.cpp` | systemc 后端的本地运行入口 |
| `sv/shuttle_demo.sv` | SV 侧示范：消息类约定（`pack()`/`type_tag`）、`shuttle_sink`/`shuttle_sink_nb` 订阅者、`shuttle_pub#(T)` 三种发布方式与 `SHUTTLE_NOW_*` 策略、`shuttle_handle::wait_done` 与计数字、`shuttle_trigger` |

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

C++ 示例注册为 `WEAVE_TEST`，与 weave 示例同一运行机制：svdpi 下由
`weave_pkg::weave_run` 经 `weave_start` 启动，systemc 下由 `weave::sc::run()` 启动。

## SV 侧打包约定（易错点）

`pack()`/`unpack()` 必须用 `{>>{...}}`（声明序、每字段高位在前），它与 C++ 侧
`Writer` 的大端写法的字节序逐字节一致；`{<<{...}}` 会整段反转比特流。string 字段先写
32 位长度前缀：`{>>{32'(s.len()), s}}`。
